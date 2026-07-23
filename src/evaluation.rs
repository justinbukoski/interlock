use crate::domain::{RecallIntent, RetrievalMode, ScopeSelector};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationManifest {
    pub schema_version: u32,
    pub fixture_revision: String,
    pub snapshot_id: String,
    pub cases: Vec<EvaluationCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationCase {
    pub id: String,
    pub revision: u32,
    pub query: String,
    pub intent: RecallIntent,
    #[serde(default)]
    pub scope: ScopeSelector,
    pub token_budget: usize,
    pub limit: usize,
    #[serde(default)]
    pub required_text: Vec<String>,
    #[serde(default)]
    pub forbidden_text: Vec<String>,
    #[serde(default)]
    pub required_ids: Vec<String>,
    #[serde(default)]
    pub forbidden_ids: Vec<String>,
    pub adjudication: String,
    pub failure_classes: Vec<String>,
}

impl EvaluationManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1
            || self.fixture_revision.trim().is_empty()
            || self.snapshot_id.trim().is_empty()
        {
            return Err(
                "manifest requires schema_version=1, fixture_revision, and snapshot_id".into(),
            );
        }
        if self.cases.is_empty() {
            return Err("manifest must contain at least one case".into());
        }
        if self.cases.len() > 200
            || self.fixture_revision.len() > 256
            || self.snapshot_id.len() > 256
        {
            return Err("manifest exceeds fixture/case limits".into());
        }
        let mut ids = HashSet::new();
        for case in &self.cases {
            if !ids.insert(&case.id) || case.id.trim().is_empty() {
                return Err("case IDs must be unique and non-empty".into());
            }
            if case.revision == 0 || case.query.trim().is_empty() || case.query.len() > 4096 {
                return Err(format!("case {} has invalid revision/query", case.id));
            }
            case.scope
                .validate()
                .map_err(|error| format!("case {}: {error}", case.id))?;
            if !(64..=32_768).contains(&case.token_budget) || !(1..=100).contains(&case.limit) {
                return Err(format!("case {} has invalid budget/limit", case.id));
            }
            if case.adjudication.trim().is_empty() || case.failure_classes.is_empty() {
                return Err(format!("case {} lacks adjudication/tags", case.id));
            }
            validate_expectations(case)?;
            let encoded = serde_json::to_value(case).map_err(|error| error.to_string())?;
            if crate::redaction::contains_sensitive_json(&encoded) {
                return Err(format!(
                    "case {} contains prohibited secret/PII-like text",
                    case.id
                ));
            }
        }
        Ok(())
    }

    pub fn checksum(&self) -> Result<String, serde_json::Error> {
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

fn validate_expectations(case: &EvaluationCase) -> Result<(), String> {
    for (field, values) in [
        ("required_text", &case.required_text),
        ("forbidden_text", &case.forbidden_text),
        ("required_ids", &case.required_ids),
        ("forbidden_ids", &case.forbidden_ids),
    ] {
        if values.len() > 32 || values.iter().any(|value| value.len() > 1024) {
            return Err(format!("case {} exceeds {field} limits", case.id));
        }
    }
    if case.adjudication.len() > 4096
        || case.failure_classes.len() > 32
        || case
            .failure_classes
            .iter()
            .any(|value| value.len() > 128 || value.trim().is_empty())
    {
        return Err(format!("case {} exceeds adjudication/tag limits", case.id));
    }
    let required_text = normalized_set(&case.required_text, &case.id, "required_text")?;
    let forbidden_text = normalized_set(&case.forbidden_text, &case.id, "forbidden_text")?;
    let required_ids = normalized_set(&case.required_ids, &case.id, "required_ids")?;
    let forbidden_ids = normalized_set(&case.forbidden_ids, &case.id, "forbidden_ids")?;
    if !required_text.is_disjoint(&forbidden_text) || !required_ids.is_disjoint(&forbidden_ids) {
        return Err(format!(
            "case {} has contradictory required/forbidden expectations",
            case.id
        ));
    }
    Ok(())
}

fn normalized_set(
    values: &[String],
    case_id: &str,
    field: &str,
) -> Result<HashSet<String>, String> {
    let normalized = values
        .iter()
        .map(|value| value.trim().to_lowercase())
        .collect::<Vec<_>>();
    if normalized.iter().any(String::is_empty) {
        return Err(format!(
            "case {case_id} contains an empty {field} expectation"
        ));
    }
    let unique: HashSet<String> = normalized.iter().cloned().collect();
    if unique.len() != normalized.len() {
        return Err(format!(
            "case {case_id} contains duplicate {field} expectations"
        ));
    }
    Ok(unique)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedItem {
    pub id: String,
    pub lane: String,
    #[serde(skip_serializing)]
    pub rendered: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedPacket {
    pub system: String,
    pub items: Vec<NormalizedItem>,
    pub reported_token_count: Option<usize>,
    pub retrieval_mode: Option<String>,
    pub snapshot_revision: Option<i64>,
    pub response_sha256: String,
    pub execution_error: Option<String>,
}

pub fn error_packet(system: &str, code: &str) -> NormalizedPacket {
    NormalizedPacket {
        system: system.into(),
        items: Vec::new(),
        reported_token_count: None,
        retrieval_mode: None,
        snapshot_revision: None,
        response_sha256: expectation_hash(code),
        execution_error: Some(code.into()),
    }
}

pub fn normalize_v5(value: &Value) -> Result<NormalizedPacket, String> {
    let mut items = Vec::new();
    let mut recognized_lane = false;
    for lane in ["constraints", "directives", "facts", "notes", "reflections"] {
        if let Some(lane_value) = value.get(lane) {
            recognized_lane = true;
            let rows = lane_value
                .as_array()
                .ok_or_else(|| format!("v5 field {lane} must be an array"))?;
            for row in rows {
                let object = row
                    .as_object()
                    .ok_or_else(|| format!("v5 field {lane} contains a non-object"))?;
                let id = row.get("id").map(value_id).unwrap_or_default();
                let rendered = object
                    .get("content")
                    .or_else(|| object.get("rule"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("v5 field {lane} item requires string content/rule"))?
                    .to_owned();
                items.push(NormalizedItem {
                    id,
                    lane: lane.into(),
                    rendered,
                });
            }
        }
    }
    if !recognized_lane {
        return Err("v5 response contains no recognized lanes".into());
    }
    packet("v5", value, items, None, None, None)
}

pub fn normalize_v6(value: &Value) -> Result<NormalizedPacket, String> {
    for lane in ["mandatory_policy", "items"] {
        if !value.get(lane).is_some_and(Value::is_array) {
            return Err(format!("v6 response requires array field {lane}"));
        }
    }
    let token_count = value
        .get("token_count")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| "v6 response requires integer token_count".to_string())?;
    let retrieval_mode: RetrievalMode = serde_json::from_value(
        value
            .get("retrieval_mode")
            .cloned()
            .ok_or_else(|| "v6 response requires retrieval_mode".to_string())?,
    )
    .map_err(|_| "v6 response has invalid retrieval_mode".to_string())?;
    let snapshot_revision = value
        .get("snapshot_revision")
        .and_then(Value::as_i64)
        .filter(|revision| *revision >= 0)
        .ok_or_else(|| "v6 response requires non-negative snapshot_revision".to_string())?;
    let mut items = Vec::new();
    for lane in ["mandatory_policy", "items"] {
        if let Some(rows) = value.get(lane).and_then(Value::as_array) {
            for row in rows {
                let object = row
                    .as_object()
                    .ok_or_else(|| format!("v6 field {lane} contains a non-object"))?;
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| uuid::Uuid::parse_str(id).is_ok())
                    .ok_or_else(|| format!("v6 field {lane} item requires UUID string id"))?;
                let rendered = object
                    .get("rendered")
                    .and_then(Value::as_str)
                    .ok_or_else(|| format!("v6 field {lane} item requires string rendered"))?;
                items.push(NormalizedItem {
                    id: id.to_owned(),
                    lane: lane.into(),
                    rendered: rendered.to_owned(),
                });
            }
        }
    }
    packet(
        "v6",
        value,
        items,
        Some(token_count),
        Some(
            serde_json::to_value(retrieval_mode)
                .expect("retrieval mode serializes")
                .as_str()
                .expect("retrieval mode serializes as string")
                .to_owned(),
        ),
        Some(snapshot_revision),
    )
}

fn packet(
    system: &str,
    raw: &Value,
    items: Vec<NormalizedItem>,
    reported_token_count: Option<usize>,
    retrieval_mode: Option<String>,
    snapshot_revision: Option<i64>,
) -> Result<NormalizedPacket, String> {
    let bytes = serde_json::to_vec(raw).map_err(|error| error.to_string())?;
    Ok(NormalizedPacket {
        system: system.into(),
        items,
        reported_token_count,
        retrieval_mode,
        snapshot_revision,
        response_sha256: hex::encode(Sha256::digest(bytes)),
        execution_error: None,
    })
}

fn value_id(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PacketScore {
    pub required_hits: usize,
    pub required_total: usize,
    pub missing_required_sha256: Vec<String>,
    pub forbidden_hit_sha256: Vec<String>,
    pub duplicate_rate: f64,
    pub budget_ok: Option<bool>,
    pub execution_error: bool,
    pub hard_gate_pass: bool,
}

pub fn score(case: &EvaluationCase, packet: &NormalizedPacket) -> PacketScore {
    let corpus = packet
        .items
        .iter()
        .map(|item| item.rendered.to_lowercase())
        .collect::<Vec<_>>()
        .join("\n");
    let ids: HashSet<&str> = packet.items.iter().map(|item| item.id.as_str()).collect();
    let mut missing_required = case
        .required_text
        .iter()
        .filter(|needle| !corpus.contains(&needle.to_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    missing_required.extend(
        case.required_ids
            .iter()
            .filter(|id| !ids.contains(id.as_str()))
            .cloned(),
    );
    let mut forbidden_hits = case
        .forbidden_text
        .iter()
        .filter(|needle| corpus.contains(&needle.to_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    forbidden_hits.extend(
        case.forbidden_ids
            .iter()
            .filter(|id| ids.contains(id.as_str()))
            .cloned(),
    );
    let normalized = packet
        .items
        .iter()
        .map(|item| item.rendered.trim().to_lowercase())
        .collect::<Vec<_>>();
    let unique: HashSet<&str> = normalized.iter().map(String::as_str).collect();
    let duplicate_rate = if normalized.is_empty() {
        0.0
    } else {
        (normalized.len() - unique.len()) as f64 / normalized.len() as f64
    };
    let budget_ok = packet
        .reported_token_count
        .map(|count| count <= case.token_budget);
    let required_total = case.required_text.len() + case.required_ids.len();
    let execution_error = packet.execution_error.is_some();
    let hard_gate_pass = !execution_error
        && missing_required.is_empty()
        && forbidden_hits.is_empty()
        && duplicate_rate < 0.01
        && budget_ok.unwrap_or(true);
    PacketScore {
        required_hits: required_total - missing_required.len(),
        required_total,
        missing_required_sha256: missing_required
            .iter()
            .map(|value| expectation_hash(value))
            .collect(),
        forbidden_hit_sha256: forbidden_hits
            .iter()
            .map(|value| expectation_hash(value))
            .collect(),
        duplicate_rate,
        budget_ok,
        execution_error,
        hard_gate_pass,
    }
}

fn expectation_hash(value: &str) -> String {
    hex::encode(Sha256::digest(value.trim().to_lowercase().as_bytes()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaseComparison {
    pub case_id: String,
    pub revision: u32,
    pub failure_classes: Vec<String>,
    pub v5: PacketScore,
    pub v6: PacketScore,
    pub v6_regressed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateScore {
    pub cases: usize,
    pub hard_gate_failures: usize,
    pub required_hits: usize,
    pub required_total: usize,
    pub forbidden_hits: usize,
    pub execution_errors: usize,
}

pub fn aggregate(comparisons: &[CaseComparison], system: &str) -> AggregateScore {
    let scores = comparisons
        .iter()
        .map(|case| if system == "v5" { &case.v5 } else { &case.v6 });
    scores.fold(
        AggregateScore {
            cases: comparisons.len(),
            hard_gate_failures: 0,
            required_hits: 0,
            required_total: 0,
            forbidden_hits: 0,
            execution_errors: 0,
        },
        |mut total, score| {
            total.hard_gate_failures += usize::from(!score.hard_gate_pass);
            total.required_hits += score.required_hits;
            total.required_total += score.required_total;
            total.forbidden_hits += score.forbidden_hit_sha256.len();
            total.execution_errors += usize::from(score.execution_error);
            total
        },
    )
}

pub fn regressions_by_class(comparisons: &[CaseComparison]) -> BTreeMap<String, usize> {
    let mut classes = BTreeMap::new();
    for comparison in comparisons.iter().filter(|case| case.v6_regressed) {
        for class in &comparison.failure_classes {
            *classes.entry(class.clone()).or_insert(0) += 1;
        }
    }
    classes
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn case() -> EvaluationCase {
        EvaluationCase {
            id: "scope-1".into(),
            revision: 1,
            query: "state".into(),
            intent: RecallIntent::Current,
            scope: ScopeSelector::default(),
            token_budget: 128,
            limit: 10,
            required_text: vec!["new value".into()],
            forbidden_text: vec!["old value".into()],
            required_ids: vec![],
            forbidden_ids: vec![],
            adjudication: "new shadows old".into(),
            failure_classes: vec!["supersession".into()],
        }
    }

    #[test]
    fn v6_normalization_and_hard_gate_are_structural() {
        let packet = normalize_v6(&json!({"mandatory_policy":[],"items":[{"id":uuid::Uuid::nil(),"rendered":"new value"}],"token_count":100,"retrieval_mode":"hybrid","snapshot_revision":3})).unwrap();
        assert!(score(&case(), &packet).hard_gate_pass);
    }

    #[test]
    fn forbidden_and_duplicate_content_fail() {
        let packet = normalize_v5(
            &json!({"facts":[{"id":1,"content":"old value"},{"id":2,"content":"old value"}]}),
        )
        .unwrap();
        let result = score(&case(), &packet);
        assert!(!result.hard_gate_pass);
        assert_eq!(result.forbidden_hit_sha256.len(), 1);
        assert_eq!(result.duplicate_rate, 0.5);
    }

    #[test]
    fn missing_v6_budget_metadata_is_an_error() {
        let result = normalize_v6(
            &json!({"mandatory_policy":[],"items":[],"retrieval_mode":"hybrid","snapshot_revision":1}),
        );
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_empty_duplicate_and_contradictory_expectations() {
        for (required, forbidden) in [
            (vec![""], vec![]),
            (vec!["same", " SAME "], vec![]),
            (vec!["same"], vec!["SAME"]),
        ] {
            let mut candidate = case();
            candidate.required_text = required.into_iter().map(str::to_owned).collect();
            candidate.forbidden_text = forbidden.into_iter().map(str::to_owned).collect();
            let manifest = EvaluationManifest {
                schema_version: 1,
                fixture_revision: "v1".into(),
                snapshot_id: "s1".into(),
                cases: vec![candidate],
            };
            assert!(manifest.validate().is_err());
        }
    }

    #[test]
    fn execution_error_packet_fails_closed() {
        let result = score(&case(), &error_packet("v6", "v6_request_failed"));
        assert!(result.execution_error);
        assert!(!result.hard_gate_pass);
    }

    #[test]
    fn malformed_v5_and_v6_items_are_rejected() {
        assert!(normalize_v5(&json!({})).is_err());
        assert!(normalize_v5(&json!({"facts":"wrong"})).is_err());
        for item in [
            json!("new value"),
            json!({"rendered":"new value"}),
            json!({"id":uuid::Uuid::nil(),"rendered":7}),
        ] {
            assert!(normalize_v6(&json!({"mandatory_policy":[],"items":[item],"token_count":1,"retrieval_mode":"hybrid","snapshot_revision":0})).is_err());
        }
    }
}
