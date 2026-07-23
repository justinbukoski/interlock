use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScopeSelector {
    pub project_key: Option<String>,
    pub repository_key: Option<String>,
    pub thread_id: Option<String>,
    pub session_id: Option<String>,
}

impl ScopeSelector {
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("project_key", self.project_key.as_deref()),
            ("repository_key", self.repository_key.as_deref()),
            ("thread_id", self.thread_id.as_deref()),
            ("session_id", self.session_id.as_deref()),
        ] {
            if value.is_some_and(|value| value.trim().is_empty() || value.len() > 512) {
                return Err(format!("{name} must be non-empty and at most 512 bytes"));
            }
        }
        if self.session_id.is_some() && self.thread_id.is_none() {
            return Err("session_id requires thread_id".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecallIntent {
    Current,
    Why,
    History,
    Procedure,
    Explore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecallRequest {
    pub query: String,
    #[serde(default = "default_intent")]
    pub intent: RecallIntent,
    #[serde(default)]
    pub scope: ScopeSelector,
    pub token_budget: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_intent() -> RecallIntent {
    RecallIntent::Current
}
fn default_limit() -> usize {
    20
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapRequest {
    #[serde(default)]
    pub scope: ScopeSelector,
    pub token_budget: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryWriteRequest {
    pub request_id: Uuid,
    #[serde(default)]
    pub scope: ScopeSelector,
    pub subject: String,
    pub predicate: String,
    pub object: Value,
    pub authority: Authority,
    pub epistemic_status: EpistemicStatus,
    pub source_type: String,
    pub source_ref: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationWriteRequest {
    pub request_id: Uuid,
    pub source_event_id: String,
    pub event_kind: String,
    #[serde(default)]
    pub scope: ScopeSelector,
    pub observed_at: DateTime<Utc>,
    pub content: String,
    pub raw_content_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub id: Uuid,
    pub source_event_id: String,
    pub event_kind: String,
    pub scope_level: String,
    pub observed_at: DateTime<Utc>,
    pub redacted_content: String,
    pub redaction_count: usize,
    pub ingested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateWriteRequest {
    pub request_id: Uuid,
    pub derivation_key: String,
    #[serde(default)]
    pub scope: ScopeSelector,
    pub subject: String,
    pub predicate: String,
    pub object: Value,
    pub authority_claim: Authority,
    pub epistemic_status: EpistemicStatus,
    pub confidence: f32,
    pub extractor_model: String,
    pub extractor_version: String,
    pub prompt_version: String,
    pub evidence_observation_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateState {
    Pending,
    Accepted,
    Rejected,
    Quarantined,
    NeedsReview,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub id: Uuid,
    pub derivation_key: String,
    pub subject: String,
    pub predicate: String,
    pub object: Value,
    pub authority_claim: Authority,
    pub epistemic_status: EpistemicStatus,
    pub confidence: f32,
    pub state: CandidateState,
    pub canonical_proposition_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidatePromotionRequest {
    pub request_id: Uuid,
    pub candidate_id: Uuid,
    pub authority: Authority,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffWriteRequest {
    pub request_id: Uuid,
    pub project_key: String,
    pub content: String,
    pub session_id: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum Authority {
    OwnerInstruction,
    MechanicallyVerified,
    CanonicalDocumentation,
    RepositoryState,
    TrustedAgentReport,
    Inference,
    RawHistory,
}

impl Authority {
    pub fn rank(self) -> i16 {
        match self {
            Self::OwnerInstruction => 1,
            Self::MechanicallyVerified => 2,
            Self::CanonicalDocumentation => 3,
            Self::RepositoryState => 4,
            Self::TrustedAgentReport => 5,
            Self::Inference => 6,
            Self::RawHistory => 7,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EpistemicStatus {
    Verified,
    Asserted,
    Inferred,
    Uncertain,
    Disputed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryItem {
    pub id: Uuid,
    pub kind: MemoryKind,
    pub subject: String,
    pub predicate: String,
    pub object: Value,
    pub rendered: String,
    pub authority: Authority,
    pub epistemic_status: EpistemicStatus,
    pub scope_level: String,
    pub source_type: String,
    pub source_ref: String,
    pub observed_at: Option<DateTime<Utc>>,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
    pub state: String,
    pub retrieval_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemoryKind {
    Proposition,
    Observation,
    Document,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handoff {
    pub id: Uuid,
    pub project_key: String,
    pub content: String,
    pub session_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallResponse {
    pub intent: RecallIntent,
    pub retrieval_mode: RetrievalMode,
    pub embedding_model: Option<String>,
    pub degraded_reason: Option<String>,
    pub mandatory_policy: Vec<MemoryItem>,
    pub items: Vec<MemoryItem>,
    pub token_count: usize,
    pub token_budget: usize,
    pub snapshot_revision: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalMode {
    Hybrid,
    LexicalOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub directives: Vec<MemoryItem>,
    pub project_state: Vec<MemoryItem>,
    pub handoff: Option<Handoff>,
    pub token_count: usize,
    pub token_budget: usize,
    pub snapshot_revision: i64,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub id: Uuid,
    pub superseded_ids: Vec<Uuid>,
    pub snapshot_revision: i64,
}
