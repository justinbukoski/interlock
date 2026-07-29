//! Foreman Memory 6.5 conversation archive: the durable, replayable continuity
//! source. This module owns the archive database (separate from v6 canonical
//! memory), authenticated idempotent batch ingestion, evidence retrieval,
//! normalized search, export, the owner deletion saga, the ingestion-order
//! mining cursor, and content-independent derivation keys.

use crate::auth::Identity;
use crate::{
    embedding::{Embedding, EmbeddingProvider},
    error::AppError,
    store::vector_literal,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use uuid::Uuid;

/// Maximum captured content bytes accepted per event before redaction.
const MAX_CONTENT_BYTES: usize = 256 * 1024;
/// Maximum events accepted in a single ingestion batch.
const MAX_BATCH: usize = 512;
/// Hard ceiling on rows returned by a single search/export call.
const MAX_PAGE: usize = 1000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveActor {
    User,
    Assistant,
    Tool,
    System,
    Application,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveEventKind {
    Message,
    ToolRequest,
    ToolResult,
    AttachmentRef,
    Correction,
    DeletionMarker,
    SessionLifecycle,
}

/// One captured conversation event supplied by an adapter. Tenant, user, and
/// consumer identity come from the authenticated token, never the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveEventInput {
    pub source_event_id: String,
    pub installation_id: Uuid,
    #[serde(default)]
    pub project_key: Option<String>,
    #[serde(default)]
    pub repository_key: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub sequence_number: Option<i64>,
    pub actor: ArchiveActor,
    pub event_kind: ArchiveEventKind,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(default = "default_schema_version")]
    pub schema_version: i32,
    pub content: String,
    #[serde(default)]
    pub raw_content_ref: Option<String>,
    pub source_timestamp: DateTime<Utc>,
    pub capture_adapter_version: String,
}

fn default_content_type() -> String {
    "text/markdown".into()
}
fn default_schema_version() -> i32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveIngestRequest {
    pub events: Vec<ArchiveEventInput>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IngestStatus {
    Accepted,
    AlreadyPresent,
    Rejected,
    Quarantined,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventAck {
    pub source_event_id: String,
    pub status: IngestStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingestion_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveIngestResponse {
    pub acks: Vec<EventAck>,
    pub accepted: usize,
    pub already_present: usize,
    pub rejected: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveSearchRequest {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub consumer_id: Option<Uuid>,
    #[serde(default)]
    pub project_key: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    #[serde(default = "default_page")]
    pub limit: usize,
}

fn default_page() -> usize {
    50
}

/// A search/export result row. Raw content is never returned inline; only its
/// availability is reported.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveEventSummary {
    pub event_id: Uuid,
    pub ingestion_seq: i64,
    pub source_event_id: String,
    pub consumer_id: Uuid,
    pub project_key: Option<String>,
    pub thread_id: Option<String>,
    pub session_id: Option<String>,
    pub actor: ArchiveActor,
    pub event_kind: ArchiveEventKind,
    pub redacted_content: String,
    pub redaction_count: i32,
    pub raw_available: bool,
    pub source_timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
}

/// A full archive event returned by the evidence and export operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveEvent {
    pub event_id: Uuid,
    pub ingestion_seq: i64,
    pub source_event_id: String,
    pub installation_id: Uuid,
    pub consumer_id: Uuid,
    pub project_key: Option<String>,
    pub repository_key: Option<String>,
    pub thread_id: Option<String>,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub sequence_number: Option<i64>,
    pub actor: ArchiveActor,
    pub event_kind: ArchiveEventKind,
    pub content_type: String,
    pub schema_version: i32,
    pub redacted_content: String,
    pub redaction_count: i32,
    pub raw_available: bool,
    pub content_hash: String,
    pub content_hash_alg: String,
    pub source_timestamp: DateTime<Utc>,
    pub ingested_at: DateTime<Utc>,
    pub capture_adapter_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveExportRequest {
    #[serde(default)]
    pub consumer_id: Option<Uuid>,
    #[serde(default)]
    pub project_key: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
    /// Exclusive lower bound on ingestion_seq for paging a large export.
    #[serde(default)]
    pub after_ingestion_seq: Option<i64>,
    #[serde(default = "default_export_limit")]
    pub limit: usize,
}

fn default_export_limit() -> usize {
    500
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveExportResponse {
    pub events: Vec<ArchiveEvent>,
    pub next_after_ingestion_seq: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeletionMode {
    /// Tombstone events and their derived material.
    Full,
    /// Remove only encrypted raw payloads, retaining redacted archive events.
    RawOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionRequest {
    pub request_id: Uuid,
    pub mode: DeletionMode,
    #[serde(default)]
    pub consumer_id: Option<Uuid>,
    #[serde(default)]
    pub project_key: Option<String>,
    #[serde(default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionIntent {
    pub intent_id: Uuid,
    pub mode: DeletionMode,
    pub created_at: DateTime<Utc>,
    pub archive_tombstoned_at: Option<DateTime<Utc>>,
    pub raw_purged_at: Option<DateTime<Utc>>,
    pub derivatives_purged_at: Option<DateTime<Utc>>,
    pub candidates_invalidated_at: Option<DateTime<Utc>>,
    pub canonical_reviewed_at: Option<DateTime<Utc>>,
    pub audit_appended_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub tombstoned_event_count: i64,
    /// Canonical-side saga steps that this foundation records but does not yet
    /// execute, so callers never mistake a partial deletion for a complete one.
    pub pending_canonical_steps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveHealth {
    pub total_events: i64,
    pub tombstoned_events: i64,
    pub oldest_source_timestamp: Option<DateTime<Utc>>,
    pub newest_source_timestamp: Option<DateTime<Utc>>,
    pub max_ingestion_seq: i64,
    pub incomplete_deletion_intents: i64,
    pub eligible_embedding_events: i64,
    pub embedded_events: i64,
    pub pending_embedding_events: i64,
    pub quarantined_embedding_events: i64,
}

/// Content-independent logical candidate identity (design §7 stage 4). The key
/// depends only on the extractor generation, the registered predicate slot, and
/// the sorted supporting event IDs — never on model wording, confidence, or the
/// rendered object — so replaying the same generation cannot mint a duplicate
/// logical candidate even when the extractor's wording drifts.
pub fn derivation_key(
    generation_id: &str,
    predicate_slot: &str,
    supporting_event_ids: &[Uuid],
) -> String {
    let mut ids: Vec<String> = supporting_event_ids
        .iter()
        .map(Uuid::to_string)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    ids.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"foreman-6.5-derivation-v1");
    hasher.update([0]);
    hasher.update(generation_id.as_bytes());
    hasher.update([0]);
    hasher.update(predicate_slot.as_bytes());
    hasher.update([0]);
    for id in ids {
        hasher.update(id.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

#[async_trait]
pub trait ArchiveStore: Send + Sync {
    async fn ready(&self) -> Result<(), AppError>;
    async fn ingest_batch(
        &self,
        identity: &Identity,
        request: &ArchiveIngestRequest,
    ) -> Result<ArchiveIngestResponse, AppError>;
    async fn search(
        &self,
        identity: &Identity,
        request: &ArchiveSearchRequest,
        query_embedding: Option<&Embedding>,
    ) -> Result<Vec<ArchiveEventSummary>, AppError>;
    async fn evidence(
        &self,
        identity: &Identity,
        event_ids: &[Uuid],
    ) -> Result<Vec<ArchiveEvent>, AppError>;
    async fn export(
        &self,
        identity: &Identity,
        request: &ArchiveExportRequest,
    ) -> Result<ArchiveExportResponse, AppError>;
    async fn create_deletion(
        &self,
        identity: &Identity,
        request: &DeletionRequest,
    ) -> Result<DeletionIntent, AppError>;
    async fn run_deletion(
        &self,
        identity: &Identity,
        intent_id: Uuid,
    ) -> Result<DeletionIntent, AppError>;
    async fn mining_pending(
        &self,
        identity: &Identity,
        generation_id: &str,
        limit: usize,
    ) -> Result<Vec<ArchiveEvent>, AppError>;
    async fn advance_cursor(
        &self,
        identity: &Identity,
        generation_id: &str,
        through_ingestion_seq: i64,
    ) -> Result<(), AppError>;
    async fn health(&self, identity: &Identity) -> Result<ArchiveHealth, AppError>;
}

#[derive(Clone)]
pub struct PgArchiveStore {
    pool: PgPool,
}

impl PgArchiveStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<(), AppError> {
        sqlx::migrate!("./migrations-archive")
            .run(&self.pool)
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
    }

    pub async fn embed_pending(
        &self,
        provider: &dyn EmbeddingProvider,
        target_model: &str,
        generation_id: &str,
        batch_size: i64,
        worker_id: Uuid,
    ) -> Result<usize, AppError> {
        let limit = batch_size.clamp(1, 64);
        let lease_seconds = 30 + limit * 4;
        let rows = sqlx::query(
            r#"WITH candidates AS (
                 SELECT event_id,generation_id FROM archive_event_embeddings
                 WHERE generation_id=$2
                   AND (embedding IS NULL OR embedding_model<>$3)
                   AND quarantined_at IS NULL AND next_attempt_at<=clock_timestamp()
                   AND (lease_until IS NULL OR lease_until<clock_timestamp())
                 ORDER BY next_attempt_at,event_id
                 FOR UPDATE SKIP LOCKED LIMIT $1
               ), claimed AS (
                 UPDATE archive_event_embeddings e
                 SET lease_owner=$4,lease_until=clock_timestamp()+($5 * interval '1 second')
                 FROM candidates c
                 WHERE e.event_id=c.event_id AND e.generation_id=c.generation_id
                 RETURNING e.event_id,e.generation_id
               )
               SELECT c.event_id,c.generation_id,a.redacted_content
               FROM claimed c JOIN archive_events a ON a.event_id=c.event_id
               WHERE a.tombstoned_at IS NULL
               ORDER BY a.ingestion_seq"#,
        )
        .bind(limit)
        .bind(generation_id)
        .bind(target_model)
        .bind(worker_id)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        let work: Vec<(Uuid, String, String)> = rows
            .iter()
            .map(|row| {
                (
                    row.get("event_id"),
                    row.get("generation_id"),
                    row.get("redacted_content"),
                )
            })
            .collect();
        let contents: Vec<String> = work.iter().map(|(_, _, content)| content.clone()).collect();
        let results = match provider.embed_batch(&contents).await {
            Ok(results) if results.len() == work.len() => results,
            _ => {
                for (event_id, generation, _) in &work {
                    self.fail_embedding(*event_id, generation, worker_id, "provider_error")
                        .await?;
                }
                return Ok(0);
            }
        };
        let mut ready = Vec::with_capacity(work.len());
        for ((event_id, generation, _), embedding) in work.into_iter().zip(results) {
            let embedding = match embedding {
                value if value.model == target_model => value,
                _ => {
                    self.fail_embedding(event_id, &generation, worker_id, "model_mismatch")
                        .await?;
                    continue;
                }
            };
            let vector = match vector_literal(&embedding.values) {
                Ok(value) => value,
                Err(_) => {
                    self.fail_embedding(event_id, &generation, worker_id, "invalid_vector")
                        .await?;
                    continue;
                }
            };
            ready.push((event_id, generation, vector, embedding.model));
        }
        // Persist the whole provider batch in one transaction. Autocommitting
        // one vector row at a time made PostgreSQL commit latency dominate the
        // BGE work during large historical backfills.
        let mut tx = self.pool.begin().await?;
        let mut completed = 0;
        for (event_id, generation, vector, model) in ready {
            let result = sqlx::query(
                r#"UPDATE archive_event_embeddings
                   SET embedding=$3::vector,embedding_model=$4,embedded_at=clock_timestamp(),
                       attempts=0,last_error=NULL,next_attempt_at=clock_timestamp(),
                       quarantined_at=NULL,lease_owner=NULL,lease_until=NULL
                   WHERE event_id=$1 AND generation_id=$2 AND lease_owner=$5"#,
            )
            .bind(event_id)
            .bind(&generation)
            .bind(vector)
            .bind(model)
            .bind(worker_id)
            .execute(&mut *tx)
            .await?;
            completed += result.rows_affected() as usize;
        }
        tx.commit().await?;
        Ok(completed)
    }

    async fn fail_embedding(
        &self,
        event_id: Uuid,
        generation_id: &str,
        worker_id: Uuid,
        error_code: &'static str,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"UPDATE archive_event_embeddings
               SET attempts=attempts+1,last_error=$3,
                   next_attempt_at=clock_timestamp()+LEAST(interval '1 hour',interval '5 seconds'*power(2,LEAST(attempts,9))),
                   quarantined_at=CASE WHEN attempts>=9 THEN clock_timestamp() ELSE NULL END,
                   lease_owner=NULL,lease_until=NULL
               WHERE event_id=$1 AND generation_id=$2 AND lease_owner=$4"#,
        )
        .bind(event_id)
        .bind(generation_id)
        .bind(error_code)
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn release_embedding_leases(&self, worker_id: Uuid) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE archive_event_embeddings SET lease_owner=NULL,lease_until=NULL WHERE lease_owner=$1",
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Owner tokens may read across all consumers for the tenant/user; every
    /// other token is confined to its own consumer's content.
    fn consumer_filter(
        identity: &Identity,
        requested: Option<Uuid>,
    ) -> Result<Option<Uuid>, AppError> {
        if identity.role.is_owner() {
            Ok(requested)
        } else if requested.is_some_and(|value| value != identity.consumer_id) {
            Err(AppError::Forbidden)
        } else {
            Ok(Some(identity.consumer_id))
        }
    }
}

fn valid_encrypted_reference(value: &str) -> bool {
    value.strip_prefix("encrypted:").is_some_and(|identifier| {
        !identifier.is_empty()
            && identifier.len() <= 128
            && identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    })
}

fn actor_from_db(value: &str) -> Result<ArchiveActor, AppError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|error| AppError::Internal(error.to_string()))
}
fn kind_from_db(value: &str) -> Result<ArchiveEventKind, AppError> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|error| AppError::Internal(error.to_string()))
}
fn actor_text(value: ArchiveActor) -> &'static str {
    match value {
        ArchiveActor::User => "user",
        ArchiveActor::Assistant => "assistant",
        ArchiveActor::Tool => "tool",
        ArchiveActor::System => "system",
        ArchiveActor::Application => "application",
    }
}
fn kind_text(value: ArchiveEventKind) -> &'static str {
    match value {
        ArchiveEventKind::Message => "message",
        ArchiveEventKind::ToolRequest => "tool_request",
        ArchiveEventKind::ToolResult => "tool_result",
        ArchiveEventKind::AttachmentRef => "attachment_ref",
        ArchiveEventKind::Correction => "correction",
        ArchiveEventKind::DeletionMarker => "deletion_marker",
        ArchiveEventKind::SessionLifecycle => "session_lifecycle",
    }
}

fn validate_scope_field(name: &str, value: Option<&str>) -> Result<(), AppError> {
    if value.is_some_and(|value| value.trim().is_empty() || value.len() > 512) {
        return Err(AppError::Invalid(format!(
            "{name} must be non-empty and at most 512 bytes"
        )));
    }
    Ok(())
}

fn validate_event(event: &ArchiveEventInput) -> Result<(), AppError> {
    if event.source_event_id.trim().is_empty() || event.source_event_id.len() > 512 {
        return Err(AppError::Invalid(
            "source_event_id must be 1..512 bytes".into(),
        ));
    }
    if event.content.is_empty() || event.content.len() > MAX_CONTENT_BYTES {
        return Err(AppError::Invalid(format!(
            "content must be 1..{MAX_CONTENT_BYTES} bytes"
        )));
    }
    if event.content_type.trim().is_empty() || event.content_type.len() > 128 {
        return Err(AppError::Invalid(
            "content_type must be 1..128 bytes".into(),
        ));
    }
    if event.capture_adapter_version.trim().is_empty() || event.capture_adapter_version.len() > 128
    {
        return Err(AppError::Invalid(
            "capture_adapter_version must be 1..128 bytes".into(),
        ));
    }
    if !(1..=1_000_000).contains(&event.schema_version) {
        return Err(AppError::Invalid("schema_version out of range".into()));
    }
    for (name, value) in [
        ("project_key", event.project_key.as_deref()),
        ("repository_key", event.repository_key.as_deref()),
        ("thread_id", event.thread_id.as_deref()),
        ("session_id", event.session_id.as_deref()),
        ("turn_id", event.turn_id.as_deref()),
    ] {
        validate_scope_field(name, value)?;
    }
    if event.session_id.is_some() && event.thread_id.is_none() {
        return Err(AppError::Invalid("session_id requires thread_id".into()));
    }
    if event
        .raw_content_ref
        .as_ref()
        .is_some_and(|value| !valid_encrypted_reference(value))
    {
        return Err(AppError::Invalid(
            "raw_content_ref must be an opaque encrypted:<id> locator".into(),
        ));
    }
    let now = Utc::now();
    if event.source_timestamp > now + Duration::minutes(5)
        || event.source_timestamp < now - Duration::days(3650)
    {
        return Err(AppError::Invalid(
            "source_timestamp is outside the accepted window".into(),
        ));
    }
    Ok(())
}

fn summary_from_row(row: &sqlx::postgres::PgRow) -> Result<ArchiveEventSummary, AppError> {
    Ok(ArchiveEventSummary {
        event_id: row.get("event_id"),
        ingestion_seq: row.get("ingestion_seq"),
        source_event_id: row.get("source_event_id"),
        consumer_id: row.get("consumer_id"),
        project_key: row.get("project_key"),
        thread_id: row.get("thread_id"),
        session_id: row.get("session_id"),
        actor: actor_from_db(row.get("actor"))?,
        event_kind: kind_from_db(row.get("event_kind"))?,
        redacted_content: row.get("redacted_content"),
        redaction_count: row.get("redaction_count"),
        raw_available: row.get::<Option<String>, _>("raw_content_ref").is_some(),
        source_timestamp: row.get("source_timestamp"),
        ingested_at: row.get("ingested_at"),
    })
}

fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<ArchiveEvent, AppError> {
    Ok(ArchiveEvent {
        event_id: row.get("event_id"),
        ingestion_seq: row.get("ingestion_seq"),
        source_event_id: row.get("source_event_id"),
        installation_id: row.get("installation_id"),
        consumer_id: row.get("consumer_id"),
        project_key: row.get("project_key"),
        repository_key: row.get("repository_key"),
        thread_id: row.get("thread_id"),
        session_id: row.get("session_id"),
        turn_id: row.get("turn_id"),
        sequence_number: row.get("sequence_number"),
        actor: actor_from_db(row.get("actor"))?,
        event_kind: kind_from_db(row.get("event_kind"))?,
        content_type: row.get("content_type"),
        schema_version: row.get("schema_version"),
        redacted_content: row.get("redacted_content"),
        redaction_count: row.get("redaction_count"),
        raw_available: row.get::<Option<String>, _>("raw_content_ref").is_some(),
        content_hash: hex::encode(row.get::<Vec<u8>, _>("content_hash")),
        content_hash_alg: row.get("content_hash_alg"),
        source_timestamp: row.get("source_timestamp"),
        ingested_at: row.get("ingested_at"),
        capture_adapter_version: row.get("capture_adapter_version"),
    })
}

fn intent_from_row(row: &sqlx::postgres::PgRow) -> Result<DeletionIntent, AppError> {
    let mode = match row.get::<&str, _>("mode") {
        "full" => DeletionMode::Full,
        "raw_only" => DeletionMode::RawOnly,
        other => return Err(AppError::Internal(format!("unknown deletion mode {other}"))),
    };
    let completed = row.get::<Option<DateTime<Utc>>, _>("completed_at");
    // Canonical-side steps are recorded but not executed by this foundation.
    let pending_canonical_steps = if mode == DeletionMode::Full && completed.is_none() {
        vec![
            "purge_canonical_derivatives".into(),
            "invalidate_unsupported_candidates".into(),
            "review_orphaned_canonical_propositions".into(),
        ]
    } else {
        Vec::new()
    };
    Ok(DeletionIntent {
        intent_id: row.get("intent_id"),
        mode,
        created_at: row.get("created_at"),
        archive_tombstoned_at: row.get("archive_tombstoned_at"),
        raw_purged_at: row.get("raw_purged_at"),
        derivatives_purged_at: row.get("derivatives_purged_at"),
        candidates_invalidated_at: row.get("candidates_invalidated_at"),
        canonical_reviewed_at: row.get("canonical_reviewed_at"),
        audit_appended_at: row.get("audit_appended_at"),
        completed_at: completed,
        tombstoned_event_count: row.get("tombstoned_event_count"),
        pending_canonical_steps,
    })
}

const INTENT_COLUMNS: &str = "intent_id,mode,created_at,archive_tombstoned_at,raw_purged_at,derivatives_purged_at,candidates_invalidated_at,canonical_reviewed_at,audit_appended_at,completed_at,tombstoned_event_count";

#[async_trait]
impl ArchiveStore for PgArchiveStore {
    async fn ready(&self) -> Result<(), AppError> {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=1 AND success) AND NOT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE NOT success)",
            )
            .fetch_one(&self.pool),
        )
        .await
        .map_err(|_| AppError::Internal("archive readiness timeout".into()))??
        .then_some(())
        .ok_or_else(|| AppError::Internal("archive migration is not applied".into()))?;
        Ok(())
    }

    async fn ingest_batch(
        &self,
        identity: &Identity,
        request: &ArchiveIngestRequest,
    ) -> Result<ArchiveIngestResponse, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        if request.events.is_empty() || request.events.len() > MAX_BATCH {
            return Err(AppError::Invalid(format!(
                "batch must contain 1..{MAX_BATCH} events"
            )));
        }
        let mut acks = Vec::with_capacity(request.events.len());
        let (mut accepted, mut already_present, mut rejected) = (0usize, 0usize, 0usize);
        for event in &request.events {
            if let Err(error) = validate_event(event) {
                rejected += 1;
                acks.push(EventAck {
                    source_event_id: event.source_event_id.clone(),
                    status: IngestStatus::Rejected,
                    event_id: None,
                    ingestion_seq: None,
                    reason: Some(error.to_string()),
                });
                continue;
            }
            let (redacted, redaction_count) = crate::redaction::redact(&event.content);
            let content_hash: [u8; 32] = Sha256::digest(redacted.as_bytes()).into();
            let request_hash: [u8; 32] = Sha256::digest(
                serde_json::to_vec(&serde_json::json!({
                    "source_event_id": event.source_event_id,
                    "installation_id": event.installation_id,
                    "project_key": event.project_key,
                    "repository_key": event.repository_key,
                    "thread_id": event.thread_id,
                    "session_id": event.session_id,
                    "turn_id": event.turn_id,
                    "sequence_number": event.sequence_number,
                    "actor": event.actor,
                    "event_kind": event.event_kind,
                    "content_type": event.content_type,
                    "schema_version": event.schema_version,
                    "redacted_content": redacted,
                    "raw_content_ref": event.raw_content_ref,
                    "source_timestamp": event.source_timestamp,
                    "capture_adapter_version": event.capture_adapter_version,
                }))
                .map_err(|error| AppError::Invalid(error.to_string()))?,
            )
            .into();
            // Idempotent insert keyed on the design's uniqueness tuple. A retry
            // returns "already present"; a reused source_event_id with different
            // content is rejected rather than silently overwriting history.
            let inserted = sqlx::query(
                r#"INSERT INTO archive_events
                   (tenant_id,user_id,consumer_id,installation_id,source_event_id,project_key,
                    repository_key,thread_id,session_id,turn_id,sequence_number,actor,event_kind,
                    content_type,schema_version,redacted_content,redaction_count,raw_content_ref,
                    content_hash,request_hash,content_hash_alg,source_timestamp,capture_adapter_version)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,'sha256',$21,$22)
                   ON CONFLICT ON CONSTRAINT archive_events_source_key DO NOTHING
                   RETURNING event_id,ingestion_seq"#,
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.consumer_id)
            .bind(event.installation_id)
            .bind(&event.source_event_id)
            .bind(&event.project_key)
            .bind(&event.repository_key)
            .bind(&event.thread_id)
            .bind(&event.session_id)
            .bind(&event.turn_id)
            .bind(event.sequence_number)
            .bind(actor_text(event.actor))
            .bind(kind_text(event.event_kind))
            .bind(&event.content_type)
            .bind(event.schema_version)
            .bind(&redacted)
            .bind(redaction_count as i32)
            .bind(&event.raw_content_ref)
            .bind(content_hash.as_slice())
            .bind(request_hash.as_slice())
            .bind(event.source_timestamp)
            .bind(&event.capture_adapter_version)
            .fetch_optional(&self.pool)
            .await?;
            match inserted {
                Some(row) => {
                    accepted += 1;
                    acks.push(EventAck {
                        source_event_id: event.source_event_id.clone(),
                        status: IngestStatus::Accepted,
                        event_id: Some(row.get("event_id")),
                        ingestion_seq: Some(row.get("ingestion_seq")),
                        reason: None,
                    });
                }
                None => {
                    let existing = sqlx::query(
                        "SELECT event_id,ingestion_seq,request_hash FROM archive_events WHERE tenant_id=$1 AND consumer_id=$2 AND installation_id=$3 AND source_event_id=$4",
                    )
                    .bind(identity.tenant_id)
                    .bind(identity.consumer_id)
                    .bind(event.installation_id)
                    .bind(&event.source_event_id)
                    .fetch_one(&self.pool)
                    .await?;
                    if existing.get::<Vec<u8>, _>("request_hash") == request_hash {
                        already_present += 1;
                        acks.push(EventAck {
                            source_event_id: event.source_event_id.clone(),
                            status: IngestStatus::AlreadyPresent,
                            event_id: Some(existing.get("event_id")),
                            ingestion_seq: Some(existing.get("ingestion_seq")),
                            reason: None,
                        });
                    } else {
                        rejected += 1;
                        acks.push(EventAck {
                            source_event_id: event.source_event_id.clone(),
                            status: IngestStatus::Rejected,
                            event_id: None,
                            ingestion_seq: None,
                            reason: Some(
                                "source_event_id already present with a different payload".into(),
                            ),
                        });
                    }
                }
            }
        }
        Ok(ArchiveIngestResponse {
            acks,
            accepted,
            already_present,
            rejected,
        })
    }

    async fn search(
        &self,
        identity: &Identity,
        request: &ArchiveSearchRequest,
        query_embedding: Option<&Embedding>,
    ) -> Result<Vec<ArchiveEventSummary>, AppError> {
        let consumer = Self::consumer_filter(identity, request.consumer_id)?;
        let limit = request.limit.clamp(1, MAX_PAGE) as i64;
        let query = request
            .query
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        if query.is_some_and(|value| value.len() > 4096) {
            return Err(AppError::Invalid("query must be at most 4096 bytes".into()));
        }
        let query_vector = query_embedding
            .map(|embedding| vector_literal(&embedding.values))
            .transpose()?;
        let rows = sqlx::query(
            r#"SELECT event_id,ingestion_seq,source_event_id,consumer_id,project_key,thread_id,
                      session_id,actor,event_kind,redacted_content,redaction_count,raw_content_ref,
                      source_timestamp,ingested_at
               FROM archive_events
               LEFT JOIN archive_event_embeddings e USING(event_id)
               WHERE tenant_id=$1 AND user_id=$2 AND tombstoned_at IS NULL
                 AND ($3::uuid IS NULL OR consumer_id=$3)
                 AND ($4::text IS NULL OR project_key=$4)
                 AND ($5::text IS NULL OR thread_id=$5)
                 AND ($6::text IS NULL OR session_id=$6)
                 AND ($7::timestamptz IS NULL OR source_timestamp>=$7)
                 AND ($8::timestamptz IS NULL OR source_timestamp<=$8)
                 AND ($9::text IS NULL
                      OR to_tsvector('english',redacted_content) @@ websearch_to_tsquery('english',$9)
                      OR ($10::text IS NOT NULL AND e.embedding IS NOT NULL))
               ORDER BY CASE WHEN $10::text IS NULL OR e.embedding IS NULL THEN 1.0
                             ELSE e.embedding <=> $10::vector END,
                        CASE WHEN $9::text IS NULL THEN 0.0
                             ELSE ts_rank_cd(to_tsvector('english',redacted_content),
                                             websearch_to_tsquery('english',$9)) END DESC,
                        source_timestamp DESC, ingestion_seq DESC
               LIMIT $11"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(consumer)
        .bind(&request.project_key)
        .bind(&request.thread_id)
        .bind(&request.session_id)
        .bind(request.from)
        .bind(request.to)
        .bind(query)
        .bind(query_vector)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(summary_from_row).collect()
    }

    async fn evidence(
        &self,
        identity: &Identity,
        event_ids: &[Uuid],
    ) -> Result<Vec<ArchiveEvent>, AppError> {
        if event_ids.is_empty() || event_ids.len() > MAX_PAGE {
            return Err(AppError::Invalid(format!(
                "evidence requires 1..{MAX_PAGE} event IDs"
            )));
        }
        let consumer = Self::consumer_filter(identity, None)?;
        let rows = sqlx::query(
            r#"SELECT event_id,ingestion_seq,source_event_id,installation_id,consumer_id,project_key,
                      repository_key,thread_id,session_id,turn_id,sequence_number,actor,event_kind,
                      content_type,schema_version,redacted_content,redaction_count,raw_content_ref,
                      content_hash,content_hash_alg,source_timestamp,ingested_at,capture_adapter_version
               FROM archive_events
               WHERE tenant_id=$1 AND user_id=$2 AND tombstoned_at IS NULL
                 AND ($3::uuid IS NULL OR consumer_id=$3)
                 AND event_id=ANY($4)
               ORDER BY source_timestamp, ingestion_seq, event_id"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(consumer)
        .bind(event_ids)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(event_from_row).collect()
    }

    async fn export(
        &self,
        identity: &Identity,
        request: &ArchiveExportRequest,
    ) -> Result<ArchiveExportResponse, AppError> {
        let consumer = Self::consumer_filter(identity, request.consumer_id)?;
        let limit = request.limit.clamp(1, MAX_PAGE) as i64;
        let rows = sqlx::query(
            r#"SELECT event_id,ingestion_seq,source_event_id,installation_id,consumer_id,project_key,
                      repository_key,thread_id,session_id,turn_id,sequence_number,actor,event_kind,
                      content_type,schema_version,redacted_content,redaction_count,raw_content_ref,
                      content_hash,content_hash_alg,source_timestamp,ingested_at,capture_adapter_version
               FROM archive_events
               WHERE tenant_id=$1 AND user_id=$2 AND tombstoned_at IS NULL
                 AND ($3::uuid IS NULL OR consumer_id=$3)
                 AND ($4::text IS NULL OR project_key=$4)
                 AND ($5::text IS NULL OR thread_id=$5)
                 AND ($6::timestamptz IS NULL OR source_timestamp>=$6)
                 AND ($7::timestamptz IS NULL OR source_timestamp<=$7)
                 AND ingestion_seq>$8
               ORDER BY ingestion_seq
               LIMIT $9"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(consumer)
        .bind(&request.project_key)
        .bind(&request.thread_id)
        .bind(request.from)
        .bind(request.to)
        .bind(request.after_ingestion_seq.unwrap_or(0))
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let events: Vec<ArchiveEvent> =
            rows.iter().map(event_from_row).collect::<Result<_, _>>()?;
        let next = (events.len() as i64 == limit)
            .then(|| events.last().map(|event| event.ingestion_seq))
            .flatten();
        Ok(ArchiveExportResponse {
            events,
            next_after_ingestion_seq: next,
        })
    }

    async fn create_deletion(
        &self,
        identity: &Identity,
        request: &DeletionRequest,
    ) -> Result<DeletionIntent, AppError> {
        if !identity.role.is_owner() {
            return Err(AppError::Forbidden);
        }
        let mode = match request.mode {
            DeletionMode::Full => "full",
            DeletionMode::RawOnly => "raw_only",
        };
        // Step 1: durably record the intent BEFORE changing any data. The
        // request_id makes creation idempotent.
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "deletion:{}:{}",
                identity.user_id, request.request_id
            ))
            .execute(&mut *tx)
            .await?;
        let existing = sqlx::query(&format!(
            "SELECT {INTENT_COLUMNS} FROM deletion_intents WHERE tenant_id=$1 AND user_id=$2 AND intent_id=$3"
        ))
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.request_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = existing {
            tx.commit().await?;
            return intent_from_row(&row);
        }
        let row = sqlx::query(&format!(
            r#"INSERT INTO deletion_intents
               (intent_id,tenant_id,user_id,requested_by,mode,filter_consumer_id,filter_project_key,
                filter_thread_id,filter_session_id,filter_from,filter_to)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
               RETURNING {INTENT_COLUMNS}"#
        ))
        .bind(request.request_id)
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(&identity.actor)
        .bind(mode)
        .bind(request.consumer_id)
        .bind(&request.project_key)
        .bind(&request.thread_id)
        .bind(&request.session_id)
        .bind(request.from)
        .bind(request.to)
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query("INSERT INTO deletion_audit(tenant_id,user_id,intent_id,actor,step,detail) VALUES($1,$2,$3,$4,'intent_recorded',$5)")
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(request.request_id)
            .bind(&identity.actor)
            .bind(serde_json::json!({"mode": mode}))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        intent_from_row(&row)
    }

    async fn run_deletion(
        &self,
        identity: &Identity,
        intent_id: Uuid,
    ) -> Result<DeletionIntent, AppError> {
        if !identity.role.is_owner() {
            return Err(AppError::Forbidden);
        }
        let mut tx = self.pool.begin().await?;
        // Serialize concurrent drivers of the same intent so the saga is
        // idempotent and resumable after a crash between steps.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!("deletion-run:{}:{intent_id}", identity.user_id))
            .execute(&mut *tx)
            .await?;
        let intent = sqlx::query(
            "SELECT intent_id,mode,filter_consumer_id,filter_project_key,filter_thread_id,filter_session_id,filter_from,filter_to,archive_tombstoned_at,raw_purged_at FROM deletion_intents WHERE tenant_id=$1 AND user_id=$2 AND intent_id=$3 FOR UPDATE",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(intent_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::NotFound)?;
        let mode: String = intent.get("mode");
        // Step 2: tombstone matching archive events (idempotent — already
        // tombstoned rows are skipped). This immediately excludes their redacted
        // content from every search and read path.
        let tombstoned: i64 = if mode == "raw_only" {
            0
        } else if intent
            .get::<Option<DateTime<Utc>>, _>("archive_tombstoned_at")
            .is_some()
        {
            sqlx::query_scalar(
                "SELECT tombstoned_event_count FROM deletion_intents WHERE intent_id=$1",
            )
            .bind(intent_id)
            .fetch_one(&mut *tx)
            .await?
        } else {
            let count: i64 = sqlx::query_scalar(
                r#"WITH tombstoned AS (
                     UPDATE archive_events SET tombstoned_at=clock_timestamp(),tombstone_intent_id=$3
                     WHERE tenant_id=$1 AND user_id=$2 AND tombstoned_at IS NULL
                       AND ($4::uuid IS NULL OR consumer_id=$4)
                       AND ($5::text IS NULL OR project_key=$5)
                       AND ($6::text IS NULL OR thread_id=$6)
                       AND ($7::text IS NULL OR session_id=$7)
                       AND ($8::timestamptz IS NULL OR source_timestamp>=$8)
                       AND ($9::timestamptz IS NULL OR source_timestamp<=$9)
                     RETURNING 1
                   ) SELECT count(*) FROM tombstoned"#,
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(intent_id)
            .bind(intent.get::<Option<Uuid>, _>("filter_consumer_id"))
            .bind(intent.get::<Option<String>, _>("filter_project_key"))
            .bind(intent.get::<Option<String>, _>("filter_thread_id"))
            .bind(intent.get::<Option<String>, _>("filter_session_id"))
            .bind(intent.get::<Option<DateTime<Utc>>, _>("filter_from"))
            .bind(intent.get::<Option<DateTime<Utc>>, _>("filter_to"))
            .fetch_one(&mut *tx)
            .await?;
            sqlx::query("UPDATE deletion_intents SET archive_tombstoned_at=clock_timestamp(),tombstoned_event_count=$2 WHERE intent_id=$1")
                .bind(intent_id)
                .bind(count)
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO deletion_audit(tenant_id,user_id,intent_id,actor,step,detail) VALUES($1,$2,$3,$4,'archive_tombstoned',$5)")
                .bind(identity.tenant_id)
                .bind(identity.user_id)
                .bind(intent_id)
                .bind(&identity.actor)
                .bind(serde_json::json!({"tombstoned_event_count": count}))
                .execute(&mut *tx)
                .await?;
            count
        };
        let _ = tombstoned;
        // Archive vector derivatives live in this database and can be closed
        // atomically with the tombstone. Canonical-memory derivatives remain a
        // separate, explicitly reported saga step.
        if mode == "full" {
            let purged = sqlx::query(
                r#"DELETE FROM archive_event_embeddings e
                   USING archive_events a
                   WHERE e.event_id=a.event_id
                     AND a.tenant_id=$1 AND a.user_id=$2
                     AND a.tombstone_intent_id=$3"#,
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(intent_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            sqlx::query(
                "UPDATE deletion_intents SET derivatives_purged_at=COALESCE(derivatives_purged_at,clock_timestamp()) WHERE intent_id=$1",
            )
            .bind(intent_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO deletion_audit(tenant_id,user_id,intent_id,actor,step,detail) VALUES($1,$2,$3,$4,'archive_embeddings_purged',$5)",
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(intent_id)
            .bind(&identity.actor)
            .bind(serde_json::json!({"embedding_rows": purged}))
            .execute(&mut *tx)
            .await?;
        }
        // Step 3: raw payload purge. Raw payloads live behind an encrypted store
        // outside this database; here we record that the archive-side reference
        // was released. Crypto-erasure of the wrapped keys is a separate boundary.
        if intent
            .get::<Option<DateTime<Utc>>, _>("raw_purged_at")
            .is_none()
        {
            sqlx::query(
                "UPDATE deletion_intents SET raw_purged_at=clock_timestamp() WHERE intent_id=$1",
            )
            .bind(intent_id)
            .execute(&mut *tx)
            .await?;
        }
        let row = sqlx::query(&format!(
            "SELECT {INTENT_COLUMNS} FROM deletion_intents WHERE intent_id=$1"
        ))
        .bind(intent_id)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;
        // The archive-side saga is complete; canonical-side derivative purge,
        // candidate invalidation, and orphaned-proposition review remain and are
        // reported through pending_canonical_steps so a partial deletion is never
        // mistaken for a complete one.
        intent_from_row(&row)
    }

    async fn mining_pending(
        &self,
        identity: &Identity,
        generation_id: &str,
        limit: usize,
    ) -> Result<Vec<ArchiveEvent>, AppError> {
        if generation_id.trim().is_empty() || generation_id.len() > 256 {
            return Err(AppError::Invalid(
                "generation_id must be 1..256 bytes".into(),
            ));
        }
        let limit = limit.clamp(1, MAX_PAGE) as i64;
        // The cursor advances over server-assigned ingestion order, so a late
        // source-timestamp event still appears after the cursor and is mined.
        let cursor: i64 = sqlx::query_scalar(
            "SELECT cursor_seq FROM mining_cursors WHERE tenant_id=$1 AND user_id=$2 AND generation_id=$3",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(generation_id)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(0);
        let rows = sqlx::query(
            r#"SELECT event_id,ingestion_seq,source_event_id,installation_id,consumer_id,project_key,
                      repository_key,thread_id,session_id,turn_id,sequence_number,actor,event_kind,
                      content_type,schema_version,redacted_content,redaction_count,raw_content_ref,
                      content_hash,content_hash_alg,source_timestamp,ingested_at,capture_adapter_version
               FROM archive_events
               WHERE tenant_id=$1 AND user_id=$2 AND tombstoned_at IS NULL AND ingestion_seq>$3
               ORDER BY ingestion_seq
               LIMIT $4"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(cursor)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(event_from_row).collect()
    }

    async fn advance_cursor(
        &self,
        identity: &Identity,
        generation_id: &str,
        through_ingestion_seq: i64,
    ) -> Result<(), AppError> {
        if generation_id.trim().is_empty() || generation_id.len() > 256 {
            return Err(AppError::Invalid(
                "generation_id must be 1..256 bytes".into(),
            ));
        }
        if through_ingestion_seq < 0 {
            return Err(AppError::Invalid("cursor must be non-negative".into()));
        }
        // The cursor only advances; a stale replay cannot rewind it.
        sqlx::query(
            r#"INSERT INTO mining_cursors(tenant_id,user_id,generation_id,cursor_seq)
               VALUES($1,$2,$3,$4)
               ON CONFLICT(tenant_id,user_id,generation_id)
               DO UPDATE SET cursor_seq=GREATEST(mining_cursors.cursor_seq,EXCLUDED.cursor_seq),updated_at=clock_timestamp()"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(generation_id)
        .bind(through_ingestion_seq)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn health(&self, identity: &Identity) -> Result<ArchiveHealth, AppError> {
        let row = sqlx::query(
            r#"SELECT
                 count(*) FILTER (WHERE tombstoned_at IS NULL) AS total_events,
                 count(*) FILTER (WHERE tombstoned_at IS NOT NULL) AS tombstoned_events,
                 min(source_timestamp) FILTER (WHERE tombstoned_at IS NULL) AS oldest,
                 max(source_timestamp) FILTER (WHERE tombstoned_at IS NULL) AS newest,
                 COALESCE(max(ingestion_seq),0) AS max_seq
               FROM archive_events WHERE tenant_id=$1 AND user_id=$2"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .fetch_one(&self.pool)
        .await?;
        let incomplete: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM deletion_intents WHERE tenant_id=$1 AND user_id=$2 AND completed_at IS NULL",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .fetch_one(&self.pool)
        .await?;
        let coverage = sqlx::query(
            r#"SELECT
                 count(*) AS eligible,
                 count(*) FILTER (WHERE e.embedding IS NOT NULL) AS embedded,
                 count(*) FILTER (WHERE e.embedding IS NULL AND e.quarantined_at IS NULL) AS pending,
                 count(*) FILTER (WHERE e.quarantined_at IS NOT NULL) AS quarantined
               FROM archive_event_embeddings e
               JOIN archive_events a USING(event_id)
               WHERE a.tenant_id=$1 AND a.user_id=$2 AND a.tombstoned_at IS NULL"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(ArchiveHealth {
            total_events: row.get("total_events"),
            tombstoned_events: row.get("tombstoned_events"),
            oldest_source_timestamp: row.get("oldest"),
            newest_source_timestamp: row.get("newest"),
            max_ingestion_seq: row.get("max_seq"),
            incomplete_deletion_intents: incomplete,
            eligible_embedding_events: coverage.get("eligible"),
            embedded_events: coverage.get("embedded"),
            pending_embedding_events: coverage.get("pending"),
            quarantined_embedding_events: coverage.get("quarantined"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derivation_key_is_content_independent_and_order_stable() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let c = Uuid::from_u128(3);
        // Same generation, slot, and evidence set → same key regardless of order.
        let k1 = derivation_key("gen-1", "user.name", &[a, b, c]);
        let k2 = derivation_key("gen-1", "user.name", &[c, a, b]);
        assert_eq!(k1, k2);
        // Duplicate evidence IDs collapse; the key is unchanged.
        let k3 = derivation_key("gen-1", "user.name", &[a, b, c, a]);
        assert_eq!(k1, k3);
        // A different generation, slot, or evidence set changes the key.
        assert_ne!(k1, derivation_key("gen-2", "user.name", &[a, b, c]));
        assert_ne!(k1, derivation_key("gen-1", "user.role", &[a, b, c]));
        assert_ne!(k1, derivation_key("gen-1", "user.name", &[a, b]));
    }

    #[test]
    fn encrypted_reference_validation_matches_locator_grammar() {
        assert!(valid_encrypted_reference("encrypted:abc_DEF-123"));
        assert!(!valid_encrypted_reference("plaintext"));
        assert!(!valid_encrypted_reference("encrypted:"));
        assert!(!valid_encrypted_reference("encrypted:has space"));
    }
}
