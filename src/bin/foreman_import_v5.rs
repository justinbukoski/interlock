use chrono::{DateTime, Utc};
use foreman_memory_v6::redaction::redact;
use reqwest::Url;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions};
use std::{fs::OpenOptions, io::Read, net::IpAddr, path::Path, time::Duration};
use uuid::Uuid;

const TENANT_ID: Uuid = Uuid::from_u128(0x11111111_1111_4111_8111_111111111111);
const USER_ID: Uuid = Uuid::from_u128(0x22222222_2222_4222_8222_222222222222);
const CODEX_CONSUMER_ID: Uuid = Uuid::from_u128(0x33333333_3333_4333_8333_333333333333);
const SOURCE_TRANSACTION_MODE: &str = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY";

struct Config {
    source_url: String,
    target_url: String,
    dry_run: bool,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let source_url = read_secret(Path::new(
            &std::env::var("FOREMAN_V5_DATABASE_URL_FILE")
                .map_err(|_| "FOREMAN_V5_DATABASE_URL_FILE is required")?,
        ))?;
        let target_url = read_secret(Path::new(
            &std::env::var("FOREMAN_V6_DATABASE_URL_FILE")
                .map_err(|_| "FOREMAN_V6_DATABASE_URL_FILE is required")?,
        ))?;
        validate_database_url(&source_url, "foreman_memory")?;
        validate_database_url(&target_url, "foreman_v6")?;
        if source_url == target_url {
            return Err("source and target databases must differ".into());
        }
        let dry_run = parse_apply_mode(
            std::env::var("FOREMAN_V5_IMPORT_DRY_RUN").ok().as_deref(),
            std::env::var("FOREMAN_V5_IMPORT_APPLY").ok().as_deref(),
        )?;
        Ok(Self {
            source_url,
            target_url,
            dry_run,
        })
    }
}

fn parse_apply_mode(legacy: Option<&str>, apply: Option<&str>) -> Result<bool, String> {
    if legacy.is_some() {
        return Err(
            "FOREMAN_V5_IMPORT_DRY_RUN is no longer accepted; omit it for dry-run or set FOREMAN_V5_IMPORT_APPLY=APPLY_V5_TO_V6"
                .into(),
        );
    }
    match apply {
        None => Ok(true),
        Some("APPLY_V5_TO_V6") => Ok(false),
        Some(_) => Err("FOREMAN_V5_IMPORT_APPLY must be absent or exactly APPLY_V5_TO_V6".into()),
    }
}

fn validate_database_url(input: &str, expected_database: &str) -> Result<(), String> {
    let url = Url::parse(input).map_err(|_| "database URL is invalid".to_string())?;
    if !matches!(url.scheme(), "postgres" | "postgresql")
        || url.host_str().is_none()
        || url.path() != format!("/{expected_database}")
    {
        return Err(format!(
            "database URL must target exact database {expected_database}"
        ));
    }
    let host = url.host_str().expect("checked host");
    let host_ip = host.trim_start_matches('[').trim_end_matches(']');
    if host != "localhost"
        && !host_ip
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
    {
        return Err("database import URLs must use loopback hosts".into());
    }
    Ok(())
}

fn read_secret(path: &Path) -> Result<String, String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    }
    let mut file = options
        .open(path)
        .map_err(|_| "cannot open database URL file".to_string())?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect database URL file".to_string())?;
    if !metadata.file_type().is_file() {
        return Err("database URL path must be a regular non-symlink file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != nix::unistd::Uid::current().as_raw()
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err("database URL file must be same-user mode 0600 or stricter".into());
        }
    }
    let mut value = String::new();
    file.by_ref()
        .take(8193)
        .read_to_string(&mut value)
        .map_err(|_| "cannot read database URL file".to_string())?;
    let value = value.trim().to_owned();
    if value.is_empty() || value.len() > 8192 {
        return Err("database URL has invalid length".into());
    }
    Ok(value)
}

fn stable_uuid(kind: &str, source_id: i64) -> Uuid {
    let digest = Sha256::digest(format!("foreman-v6:v5-import:{kind}:{source_id}"));
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

struct ImportRecord {
    source_id: i64,
    kind: &'static str,
    scope_id: Uuid,
    subject: String,
    predicate: &'static str,
    object: Value,
    authority: &'static str,
    authority_rank: i16,
    epistemic_status: String,
    source_ref: String,
    recorded_at: DateTime<Utc>,
}

fn source_record_digest(record: &ImportRecord) -> String {
    let canonical = json!({
        "authority": record.authority,
        "authority_rank": record.authority_rank,
        "epistemic_status": record.epistemic_status,
        "kind": record.kind,
        "object": record.object,
        "predicate": record.predicate,
        "recorded_at": record.recorded_at,
        "scope_id": record.scope_id,
        "source_id": record.source_id,
        "subject": record.subject,
    });
    hex::encode(Sha256::digest(
        serde_json::to_vec(&canonical).expect("canonical import record is serializable"),
    ))
}

fn expected_rendered(record: &ImportRecord) -> String {
    format!("{} {} {}", record.subject, record.predicate, record.object)
}

fn expected_outbox_payload(proposition_id: Uuid, digest: &str) -> Value {
    json!({
        "proposition_id": proposition_id.to_string(),
        "source_record_sha256": digest,
        "tenant_id": TENANT_ID.to_string(),
        "user_id": USER_ID.to_string(),
    })
}

#[derive(Default, Serialize)]
struct ImportReport {
    dry_run: bool,
    source_snapshot: String,
    constraints: u64,
    directives: u64,
    rules: u64,
    notes: u64,
    facts: u64,
    skipped_existing: u64,
    redactions: usize,
    snapshot_revision: i64,
}

async fn scope_id(
    tx: &mut Transaction<'_, Postgres>,
    consumer_id: Option<Uuid>,
) -> Result<Uuid, sqlx::Error> {
    sqlx::query_scalar(
        r#"INSERT INTO scopes(tenant_id,user_id,consumer_id,scope_level)
           VALUES($1,$2,$3,'global')
           ON CONFLICT (tenant_id,user_id,consumer_id,project_key,repository_key,thread_id,session_id)
           DO UPDATE SET scope_level=EXCLUDED.scope_level RETURNING id"#,
    )
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(consumer_id)
    .fetch_one(&mut **tx)
    .await
}

async fn insert_record(
    tx: &mut Transaction<'_, Postgres>,
    record: &ImportRecord,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mutation_id = stable_uuid(record.kind, record.source_id);
    let proposition_id = stable_uuid(&format!("{}:proposition", record.kind), record.source_id);
    let audit_id = stable_uuid(&format!("{}:audit", record.kind), record.source_id);
    let outbox_id = stable_uuid(&format!("{}:outbox", record.kind), record.source_id);
    let digest = source_record_digest(record);
    let source_ref = format!("{}#sha256={digest}", record.source_ref);
    let audit_reason = format!(
        "normalized current-state import from read-only Foreman v5; source_record_sha256={digest}"
    );
    let predicate_id: Uuid = sqlx::query_scalar("SELECT id FROM predicates WHERE key=$1")
        .bind(record.predicate)
        .fetch_one(&mut **tx)
        .await?;
    let cardinality: String = sqlx::query_scalar("SELECT cardinality FROM predicates WHERE id=$1")
        .bind(predicate_id)
        .fetch_one(&mut **tx)
        .await?;
    let rendered = expected_rendered(record);
    let outbox_payload = expected_outbox_payload(proposition_id, &digest);
    let inserted = sqlx::query(
        "INSERT INTO canonical_mutations(tenant_id,user_id,mutation_id,actor,created_at) VALUES($1,$2,$3,'v5-importer',$4) ON CONFLICT DO NOTHING",
    )
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(mutation_id)
    .bind(record.recorded_at)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if inserted == 0 {
        let valid: bool = sqlx::query_scalar(
            r#"SELECT
              EXISTS(SELECT 1 FROM canonical_mutations
                     WHERE tenant_id=$2 AND user_id=$3 AND mutation_id=$4
                       AND actor='v5-importer' AND created_at=$15)
              AND EXISTS(SELECT 1 FROM propositions p
                     WHERE p.id=$1 AND p.tenant_id=$2 AND p.user_id=$3
                       AND p.writer_consumer_id=$20 AND p.scope_id=$5
                       AND p.subject_key=$6 AND p.predicate_id=$7 AND p.cardinality=$8
                       AND p.object_value=$9 AND p.rendered=$10
                       AND p.authority=$11 AND p.authority_rank=$12 AND p.epistemic_status=$13
                       AND p.source_type='foreman-v5-import' AND p.source_ref=$14
                       AND p.last_mutation_id=$4 AND p.status='current'
                       AND p.valid_from=$15 AND p.valid_to IS NULL AND p.recorded_at=$15)
              AND EXISTS(SELECT 1 FROM audit_events
                         WHERE id=$16 AND tenant_id=$2 AND user_id=$3
                           AND actor='v5-importer' AND event_type='canonical_write'
                           AND after_id=$1 AND before_ids='{}'::uuid[]
                           AND reason=$17 AND request_id=$4 AND created_at=$15)
              AND EXISTS(SELECT 1 FROM outbox
                         WHERE id=$18 AND tenant_id=$2 AND user_id=$3
                           AND event_type='canonical_changed' AND aggregate_id=$1
                           AND payload=$19 AND created_at=$15)"#,
        )
        .bind(proposition_id)
        .bind(TENANT_ID)
        .bind(USER_ID)
        .bind(mutation_id)
        .bind(record.scope_id)
        .bind(&record.subject)
        .bind(predicate_id)
        .bind(&cardinality)
        .bind(&record.object)
        .bind(&rendered)
        .bind(record.authority)
        .bind(record.authority_rank)
        .bind(&record.epistemic_status)
        .bind(&source_ref)
        .bind(record.recorded_at)
        .bind(audit_id)
        .bind(&audit_reason)
        .bind(outbox_id)
        .bind(&outbox_payload)
        .bind(CODEX_CONSUMER_ID)
        .fetch_one(&mut **tx)
        .await?;
        if !valid {
            return Err(format!(
                "deterministic mutation conflict for {}:{} is changed, partial, or colliding",
                record.kind, record.source_id
            )
            .into());
        }
        return Ok(false);
    }
    sqlx::query(
        r#"INSERT INTO propositions(
             id,tenant_id,user_id,writer_consumer_id,scope_id,subject_key,predicate_id,
             cardinality,object_value,rendered,authority,authority_rank,epistemic_status,
             source_type,source_ref,last_mutation_id,status,valid_from,recorded_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
                  'foreman-v5-import',$14,$15,'current',$16,$16)"#,
    )
    .bind(proposition_id)
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(CODEX_CONSUMER_ID)
    .bind(record.scope_id)
    .bind(&record.subject)
    .bind(predicate_id)
    .bind(&cardinality)
    .bind(&record.object)
    .bind(&rendered)
    .bind(record.authority)
    .bind(record.authority_rank)
    .bind(&record.epistemic_status)
    .bind(&source_ref)
    .bind(mutation_id)
    .bind(record.recorded_at)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"INSERT INTO audit_events(
             id,tenant_id,user_id,actor,event_type,after_id,before_ids,reason,request_id,created_at)
           VALUES($1,$2,$3,'v5-importer','canonical_write',$4,'{}'::uuid[],
                  $5,$6,$7)"#,
    )
    .bind(audit_id)
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(proposition_id)
    .bind(&audit_reason)
    .bind(mutation_id)
    .bind(record.recorded_at)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        r#"INSERT INTO outbox(id,tenant_id,user_id,event_type,aggregate_id,payload,created_at)
           VALUES($1,$2,$3,'canonical_changed',$4,
                  $5,$6)"#,
    )
    .bind(stable_uuid(
        &format!("{}:outbox", record.kind),
        record.source_id,
    ))
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(proposition_id)
    .bind(&outbox_payload)
    .bind(record.recorded_at)
    .execute(&mut **tx)
    .await?;
    Ok(true)
}

fn normalized(input: &str, report: &mut ImportReport) -> String {
    let (value, count) = redact(input);
    report.redactions += count;
    value
}

fn mapped_epistemic(value: Option<&str>, verified: bool) -> String {
    if verified {
        return "verified".into();
    }
    match value {
        Some("asserted" | "inferred" | "uncertain" | "disputed") => value.unwrap().into(),
        _ => "asserted".into(),
    }
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("foreman-v5-import: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::from_env()?;
    let source = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&config.source_url)
        .await?;
    let target = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&config.target_url)
        .await?;
    assert_database(&source, "foreman_memory", true).await?;
    assert_database(&target, "foreman_v6", false).await?;

    let mut source_tx = source.begin().await?;
    sqlx::query(SOURCE_TRANSACTION_MODE)
        .execute(&mut *source_tx)
        .await?;
    let source_snapshot: String = sqlx::query_scalar("SELECT txid_current_snapshot()::text")
        .fetch_one(&mut *source_tx)
        .await?;
    let constraints = sqlx::query(
        "SELECT id,rule,rationale,created_at FROM constraints WHERE active ORDER BY id",
    )
    .fetch_all(&mut *source_tx)
    .await?;
    let rules = sqlx::query("SELECT id,content,updated_at FROM rules ORDER BY id")
        .fetch_all(&mut *source_tx)
        .await?;
    let notes = sqlx::query(
        "SELECT id,content,category,importance,pinned,created_at FROM notes WHERE invalid_at IS NULL ORDER BY id",
    )
    .fetch_all(&mut *source_tx)
    .await?;
    let facts = sqlx::query(
        r#"SELECT f.id,f.subject,f.predicate,f.object,f.content,f.importance,
                  f.source_kind,f.epistemic_status,f.verify_last_passed,f.created_at
           FROM facts f JOIN current_validity v ON v.fact_id=f.id
           WHERE v.invalid_at IS NULL ORDER BY f.id"#,
    )
    .fetch_all(&mut *source_tx)
    .await?;
    source_tx.commit().await?;

    let mut tx = target.begin().await?;
    let shared_scope = scope_id(&mut tx, None).await?;
    let codex_scope = scope_id(&mut tx, Some(CODEX_CONSUMER_ID)).await?;
    let mut report = ImportReport {
        dry_run: config.dry_run,
        source_snapshot,
        ..ImportReport::default()
    };

    for row in constraints {
        let id: i64 = row.get("id");
        let rule = normalized(row.get("rule"), &mut report);
        let rationale = row
            .get::<Option<String>, _>("rationale")
            .map(|value| normalized(&value, &mut report));
        let record = ImportRecord {
            source_id: id,
            kind: "constraint",
            scope_id: shared_scope,
            subject: format!("v5.constraint.{id}"),
            predicate: "system.constraint",
            object: Value::String(match rationale {
                Some(rationale) if !rationale.is_empty() => format!("{rule}\nWhy: {rationale}"),
                _ => rule,
            }),
            authority: "owner_instruction",
            authority_rank: 1,
            epistemic_status: "asserted".into(),
            source_ref: format!("foreman-v5:constraint:{id}"),
            recorded_at: row
                .get::<Option<DateTime<Utc>>, _>("created_at")
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        };
        if insert_record(&mut tx, &record).await? {
            report.constraints += 1;
        } else {
            report.skipped_existing += 1;
        }
    }

    for row in rules {
        let id: i64 = row.get("id");
        let record = ImportRecord {
            source_id: id,
            kind: "rule",
            scope_id: shared_scope,
            subject: format!("v5.rule.{id}"),
            predicate: "system.directive",
            object: Value::String(normalized(row.get("content"), &mut report)),
            authority: "owner_instruction",
            authority_rank: 1,
            epistemic_status: "asserted".into(),
            source_ref: format!("foreman-v5:rule:{id}"),
            recorded_at: row
                .get::<Option<DateTime<Utc>>, _>("updated_at")
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        };
        if insert_record(&mut tx, &record).await? {
            report.rules += 1;
        } else {
            report.skipped_existing += 1;
        }
    }

    for row in notes {
        let id: i64 = row.get("id");
        let category = normalized(
            &row.get::<Option<String>, _>("category").unwrap_or_default(),
            &mut report,
        );
        let importance = row.get::<Option<f32>, _>("importance").unwrap_or(0.5);
        let pinned = row.get::<Option<bool>, _>("pinned").unwrap_or(false);
        let directive = pinned
            && importance >= 1.0
            && matches!(category.as_str(), "directive" | "standing_directive");
        let content = normalized(row.get("content"), &mut report);
        let record = if directive {
            ImportRecord {
                source_id: id,
                kind: "directive",
                scope_id: shared_scope,
                subject: format!("v5.directive.{id}"),
                predicate: "system.directive",
                object: Value::String(content),
                authority: "owner_instruction",
                authority_rank: 1,
                epistemic_status: "asserted".into(),
                source_ref: format!("foreman-v5:note:{id}"),
                recorded_at: row
                    .get::<Option<DateTime<Utc>>, _>("created_at")
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            }
        } else {
            ImportRecord {
                source_id: id,
                kind: "note",
                scope_id: codex_scope,
                subject: format!("v5.note.{id}"),
                predicate: "legacy.note",
                object: json!({"content":content,"category":category,"importance":importance,"pinned":pinned}),
                authority: "raw_history",
                authority_rank: 7,
                epistemic_status: "asserted".into(),
                source_ref: format!("foreman-v5:note:{id}"),
                recorded_at: row
                    .get::<Option<DateTime<Utc>>, _>("created_at")
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
            }
        };
        if insert_record(&mut tx, &record).await? {
            if directive {
                report.directives += 1;
            } else {
                report.notes += 1;
            }
        } else {
            report.skipped_existing += 1;
        }
    }

    for row in facts {
        let id: i64 = row.get("id");
        let verified = row
            .get::<Option<bool>, _>("verify_last_passed")
            .unwrap_or(false);
        let v5_subject = normalized(row.get("subject"), &mut report);
        let predicate = normalized(row.get("predicate"), &mut report);
        let object = normalized(row.get("object"), &mut report);
        let content = normalized(row.get("content"), &mut report);
        let source_kind = row
            .get::<Option<String>, _>("source_kind")
            .map(|value| normalized(&value, &mut report));
        let record = ImportRecord {
            source_id: id,
            kind: "fact",
            scope_id: codex_scope,
            subject: format!("v5.fact.{id}"),
            predicate: "legacy.fact",
            object: json!({
                "v5_subject":v5_subject,
                "v5_predicate":predicate,
                "value":object,
                "content":content,
                "importance":row.get::<f32,_>("importance"),
                "source_kind":source_kind
            }),
            authority: if verified {
                "mechanically_verified"
            } else {
                "trusted_agent_report"
            },
            authority_rank: if verified { 2 } else { 5 },
            epistemic_status: mapped_epistemic(
                row.get::<Option<String>, _>("epistemic_status").as_deref(),
                verified,
            ),
            source_ref: format!("foreman-v5:fact:{id}"),
            recorded_at: row
                .get::<Option<DateTime<Utc>>, _>("created_at")
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH),
        };
        if insert_record(&mut tx, &record).await? {
            report.facts += 1;
        } else {
            report.skipped_existing += 1;
        }
    }

    let inserted =
        report.constraints + report.directives + report.rules + report.notes + report.facts;
    report.snapshot_revision = sqlx::query_scalar(
        r#"INSERT INTO snapshot_revisions(tenant_id,user_id,revision) VALUES($1,$2,$3)
           ON CONFLICT(tenant_id,user_id) DO UPDATE
           SET revision=snapshot_revisions.revision+EXCLUDED.revision RETURNING revision"#,
    )
    .bind(TENANT_ID)
    .bind(USER_ID)
    .bind(inserted as i64)
    .fetch_one(&mut *tx)
    .await?;
    if config.dry_run {
        tx.rollback().await?;
    } else {
        tx.commit().await?;
    }
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn assert_database(
    pool: &PgPool,
    expected: &str,
    source_database: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = sqlx::query("SELECT current_database() AS db, pg_is_in_recovery() AS recovery")
        .fetch_one(pool)
        .await?;
    let database: String = row.get("db");
    if database != expected || row.get::<bool, _>("recovery") {
        return Err(format!("database identity assertion failed for {expected}").into());
    }
    if !source_database {
        let has_v6: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='canonical_mutations')",
        )
        .fetch_one(pool)
        .await?;
        if !has_v6 {
            return Err("target is not a Foreman v6 database".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn database_urls_are_exact_and_loopback() {
        assert!(
            validate_database_url(
                "postgresql://u:p@127.0.0.1:5433/foreman_memory",
                "foreman_memory"
            )
            .is_ok()
        );
        assert!(
            validate_database_url(
                "postgresql://u:p@10.0.0.42:5433/foreman_memory",
                "foreman_memory"
            )
            .is_err()
        );
        assert!(
            validate_database_url(
                "postgresql://u:p@127.0.0.1:5433/foreman_v6",
                "foreman_memory"
            )
            .is_err()
        );
    }

    #[test]
    fn stable_ids_are_lane_and_source_bound() {
        assert_eq!(stable_uuid("fact", 42), stable_uuid("fact", 42));
        assert_ne!(stable_uuid("fact", 42), stable_uuid("note", 42));
        assert_ne!(stable_uuid("fact", 42), stable_uuid("fact", 43));
    }

    #[test]
    fn import_defaults_to_dry_run_and_requires_exact_apply_confirmation() {
        assert_eq!(parse_apply_mode(None, None), Ok(true));
        assert_eq!(parse_apply_mode(None, Some("APPLY_V5_TO_V6")), Ok(false));
        assert!(parse_apply_mode(None, Some("true")).is_err());
        assert!(parse_apply_mode(None, Some("apply_v5_to_v6")).is_err());
        assert!(parse_apply_mode(Some("true"), None).is_err());
        assert!(parse_apply_mode(Some("false"), Some("APPLY_V5_TO_V6")).is_err());
    }

    fn digest_record() -> ImportRecord {
        ImportRecord {
            source_id: 42,
            kind: "note",
            scope_id: Uuid::from_u128(7),
            subject: "v5.note.42".into(),
            predicate: "legacy.note",
            object: json!({"content":"normalized"}),
            authority: "raw_history",
            authority_rank: 7,
            epistemic_status: "asserted".into(),
            source_ref: "foreman-v5:note:42".into(),
            recorded_at: "2026-01-02T03:04:05Z".parse().expect("valid timestamp"),
        }
    }

    #[test]
    fn source_record_digest_is_stable_and_content_sensitive() {
        let record = digest_record();
        assert_eq!(source_record_digest(&record), source_record_digest(&record));

        let mut changed = digest_record();
        changed.object = json!({"content":"changed"});
        assert_ne!(
            source_record_digest(&record),
            source_record_digest(&changed)
        );

        changed = digest_record();
        changed.authority_rank = 6;
        assert_ne!(
            source_record_digest(&record),
            source_record_digest(&changed)
        );
    }

    #[test]
    fn expected_tuple_values_change_with_the_normalized_record() {
        let record = digest_record();
        assert_eq!(
            expected_rendered(&record),
            "v5.note.42 legacy.note {\"content\":\"normalized\"}"
        );

        let proposition_id = stable_uuid("note:proposition", 42);
        let digest = source_record_digest(&record);
        let payload = expected_outbox_payload(proposition_id, &digest);
        assert_eq!(payload["proposition_id"], proposition_id.to_string());
        assert_eq!(payload["source_record_sha256"], digest);

        let mut changed = digest_record();
        changed.object = json!({"content":"changed"});
        assert_ne!(expected_rendered(&record), expected_rendered(&changed));
        assert_ne!(
            expected_outbox_payload(proposition_id, &source_record_digest(&record)),
            expected_outbox_payload(proposition_id, &source_record_digest(&changed))
        );
    }

    #[test]
    fn source_transaction_mode_is_repeatable_read_and_read_only() {
        assert_eq!(
            SOURCE_TRANSACTION_MODE,
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        );
    }
}
