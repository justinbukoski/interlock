//! Interlock 6.5 continuity handoff repair (design §9.1).
//!
//! Handoffs are a first-class lifecycle subsystem selected by a typed
//! `context_key`, never by an arbitrary filesystem path. Supersession is a
//! compare-and-swap on a per-context active pointer, so two agents cannot both
//! win and no items are silently lost. Handoffs never enter recall, mining, or
//! canonicalization, and a broad filesystem key can never retrieve one.

use crate::auth::Identity;
use crate::error::AppError;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::sync::LazyLock;
use uuid::Uuid;

const MAX_ITEMS_PER_KIND: usize = 200;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextKind {
    /// Normalized repository remote plus worktree identity.
    RepositoryWorktree,
    /// An explicit durable project ID registered by the client.
    DurableProject,
    /// Codex/Claude/Hermes thread identity for a projectless conversation.
    Thread,
    /// An installation-scoped projectless context for a conversation family.
    InstallationProjectless,
}

impl ContextKind {
    fn as_db(self) -> &'static str {
        match self {
            Self::RepositoryWorktree => "repository_worktree",
            Self::DurableProject => "durable_project",
            Self::Thread => "thread",
            Self::InstallationProjectless => "installation_projectless",
        }
    }
    fn from_db(value: &str) -> Result<Self, AppError> {
        match value {
            "repository_worktree" => Ok(Self::RepositoryWorktree),
            "durable_project" => Ok(Self::DurableProject),
            "thread" => Ok(Self::Thread),
            "installation_projectless" => Ok(Self::InstallationProjectless),
            other => Err(AppError::Internal(format!("unknown context type {other}"))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextRef {
    pub kind: ContextKind,
    pub key: String,
    /// Required for cross-application projectless continuity; never inferred.
    #[serde(default)]
    pub family_id: Option<String>,
}

static HOME_LIKE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)^/(home|users)/[^/]+/?$|^/root/?$").expect("static regex is valid")
});
static DRIVE_ROOT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z]:[\\/]?$").expect("static regex is valid"));

/// Returns a rejection reason when a context key is a forbidden broad location.
/// Forbidden keys can never be used to write or retrieve a handoff, so a
/// projectless task falling back to the home directory cannot surface an
/// unrelated project's handoff (design §9.1 "forbidden automatic handoff keys").
pub fn forbidden_context_reason(key: &str) -> Option<&'static str> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Some("context key is empty");
    }
    if matches!(trimmed, "/" | "\\" | "~" | "." | ".." | "*" | "-") {
        return Some("context key is a root or wildcard");
    }
    if DRIVE_ROOT.is_match(trimmed) {
        return Some("context key is a drive root");
    }
    if HOME_LIKE.is_match(trimmed) {
        return Some("context key is a home or user root directory");
    }
    None
}

fn validate_context_ref(context: &ContextRef) -> Result<(), AppError> {
    if context.key.len() > 1024 {
        return Err(AppError::Invalid(
            "context key must be 1..1024 bytes".into(),
        ));
    }
    if let Some(reason) = forbidden_context_reason(&context.key) {
        return Err(AppError::Invalid(format!(
            "unsafe handoff context key: {reason}"
        )));
    }
    // Cross-application projectless continuity requires an explicit family_id.
    if let Some(family) = &context.family_id
        && (family.trim().is_empty() || family.len() > 256)
    {
        return Err(AppError::Invalid("family_id must be 1..256 bytes".into()));
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffWriteInput {
    pub request_id: Uuid,
    pub context: ContextRef,
    pub session_id: String,
    #[serde(default)]
    pub thread_id: Option<String>,
    pub summary: String,
    pub written_by: String,
    #[serde(default)]
    pub completed: Vec<String>,
    #[serde(default)]
    pub in_progress: Vec<String>,
    #[serde(default)]
    pub next_actions: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    #[serde(default)]
    pub artifacts: Vec<String>,
    #[serde(default)]
    pub verification_state: Option<String>,
    #[serde(default)]
    pub do_not_repeat: Vec<String>,
    /// Compare-and-swap guard: the active handoff ID the writer observed. When
    /// present and stale, the write is rejected with the current active ID.
    #[serde(default)]
    pub expected_active_id: Option<Uuid>,
    #[serde(default)]
    pub source_snapshot_revision: Option<i64>,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffItem {
    pub item_id: Uuid,
    pub item_kind: String,
    pub ordinal: i32,
    pub text: String,
    pub status: String,
    pub completed_at: Option<DateTime<Utc>>,
    pub completed_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Handoff65 {
    pub handoff_id: Uuid,
    pub context_id: Uuid,
    pub context_type: ContextKind,
    pub context_key: String,
    pub producing_consumer_id: Uuid,
    pub producing_thread_id: Option<String>,
    pub producing_session_id: String,
    pub summary: String,
    pub content: serde_json::Value,
    pub status: String,
    pub predecessor_handoff_id: Option<Uuid>,
    pub source_snapshot_revision: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub items: Vec<HandoffItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffSummary {
    pub handoff_id: Uuid,
    pub status: String,
    pub summary: String,
    pub producing_consumer_id: Uuid,
    pub producing_session_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub predecessor_handoff_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextValidation {
    pub available: bool,
    pub context_type: ContextKind,
    pub normalized_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub has_active_handoff: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckRequest {
    pub handoff_id: Uuid,
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckResult {
    pub handoff_id: Uuid,
    pub newly_acknowledged: bool,
    pub first_acknowledged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteItemsRequest {
    pub handoff_id: Uuid,
    pub item_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseRequest {
    pub context: ContextRef,
    /// CAS guard: the active handoff the caller intends to close.
    pub expected_active_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloseResult {
    pub closed_handoff_id: Uuid,
    pub status: String,
}

/// A compare-and-swap conflict carrying the current active handoff so the loser
/// can reload, merge item state, and retry rather than overwriting the winner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandoffConflict {
    pub current_active_id: Option<Uuid>,
    pub reason: String,
}

#[async_trait]
pub trait ContinuityStore: Send + Sync {
    async fn ready(&self) -> Result<(), AppError>;
    async fn validate_context(
        &self,
        identity: &Identity,
        context: &ContextRef,
    ) -> Result<ContextValidation, AppError>;
    async fn write(
        &self,
        identity: &Identity,
        request: &HandoffWriteInput,
    ) -> Result<Handoff65, AppError>;
    async fn get_exact(
        &self,
        identity: &Identity,
        context: &ContextRef,
    ) -> Result<Option<Handoff65>, AppError>;
    async fn acknowledge(
        &self,
        identity: &Identity,
        request: &AckRequest,
    ) -> Result<AckResult, AppError>;
    async fn complete_items(
        &self,
        identity: &Identity,
        request: &CompleteItemsRequest,
    ) -> Result<Handoff65, AppError>;
    async fn close(
        &self,
        identity: &Identity,
        request: &CloseRequest,
    ) -> Result<CloseResult, AppError>;
    async fn history(
        &self,
        identity: &Identity,
        context: &ContextRef,
        limit: usize,
    ) -> Result<Vec<HandoffSummary>, AppError>;
}

#[derive(Clone)]
pub struct PgContinuityStore {
    pool: PgPool,
}

impl PgContinuityStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn set_write_guards(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), AppError> {
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
        ] {
            sqlx::query(statement).execute(&mut **tx).await?;
        }
        Ok(())
    }

    async fn load_handoff(
        &self,
        executor: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        identity: &Identity,
        handoff_id: Uuid,
    ) -> Result<Handoff65, AppError> {
        let row = sqlx::query(
            r#"SELECT h.handoff_id,h.context_id,c.context_type,c.context_key,h.producing_consumer_id,
                      h.producing_thread_id,h.producing_session_id,h.summary,h.content,h.status,
                      h.predecessor_handoff_id,h.source_snapshot_revision,h.created_at,h.expires_at
               FROM continuity.handoffs h JOIN continuity.contexts c ON c.context_id=h.context_id
               WHERE h.tenant_id=$1 AND h.user_id=$2 AND h.handoff_id=$3"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(handoff_id)
        .fetch_optional(&mut **executor)
        .await?
        .ok_or(AppError::NotFound)?;
        let items = sqlx::query(
            "SELECT item_id,item_kind,ordinal,text,status,completed_at,completed_by FROM continuity.handoff_items WHERE handoff_id=$1 ORDER BY item_kind,ordinal",
        )
        .bind(handoff_id)
        .fetch_all(&mut **executor)
        .await?;
        handoff_from_rows(&row, &items)
    }
}

fn handoff_from_rows(
    row: &sqlx::postgres::PgRow,
    item_rows: &[sqlx::postgres::PgRow],
) -> Result<Handoff65, AppError> {
    let items = item_rows
        .iter()
        .map(|item| HandoffItem {
            item_id: item.get("item_id"),
            item_kind: item.get("item_kind"),
            ordinal: item.get("ordinal"),
            text: item.get("text"),
            status: item.get("status"),
            completed_at: item.get("completed_at"),
            completed_by: item.get("completed_by"),
        })
        .collect();
    Ok(Handoff65 {
        handoff_id: row.get("handoff_id"),
        context_id: row.get("context_id"),
        context_type: ContextKind::from_db(row.get("context_type"))?,
        context_key: row.get("context_key"),
        producing_consumer_id: row.get("producing_consumer_id"),
        producing_thread_id: row.get("producing_thread_id"),
        producing_session_id: row.get("producing_session_id"),
        summary: row.get("summary"),
        content: row.get("content"),
        status: row.get("status"),
        predecessor_handoff_id: row.get("predecessor_handoff_id"),
        source_snapshot_revision: row.get("source_snapshot_revision"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
        items,
    })
}

fn validate_write(request: &HandoffWriteInput) -> Result<(), AppError> {
    validate_context_ref(&request.context)?;
    if request.summary.trim().is_empty() || request.summary.len() > 16 * 1024 {
        return Err(AppError::Invalid("summary must be 1..16384 bytes".into()));
    }
    if request.session_id.trim().is_empty() || request.session_id.len() > 256 {
        return Err(AppError::Invalid("session_id must be 1..256 bytes".into()));
    }
    if request.written_by.trim().is_empty() || request.written_by.len() > 256 {
        return Err(AppError::Invalid("written_by must be 1..256 bytes".into()));
    }
    if request
        .thread_id
        .as_ref()
        .is_some_and(|value| value.trim().is_empty() || value.len() > 512)
    {
        return Err(AppError::Invalid("thread_id must be 1..512 bytes".into()));
    }
    for (name, items) in [
        ("in_progress", &request.in_progress),
        ("next_actions", &request.next_actions),
        ("blockers", &request.blockers),
    ] {
        if items.len() > MAX_ITEMS_PER_KIND {
            return Err(AppError::Invalid(format!(
                "{name} exceeds {MAX_ITEMS_PER_KIND} items"
            )));
        }
        if items
            .iter()
            .any(|item| item.trim().is_empty() || item.len() > 8192)
        {
            return Err(AppError::Invalid(format!(
                "{name} items must be 1..8192 bytes"
            )));
        }
    }
    if let Some(expires_at) = request.expires_at {
        let now = Utc::now();
        if expires_at <= now || expires_at > now + Duration::days(30) {
            return Err(AppError::Invalid(
                "handoff expiry must be within the next 30 days".into(),
            ));
        }
    }
    // A handoff is continuation state, never a secret store.
    let sensitive = [request.summary.as_str()]
        .into_iter()
        .chain(request.in_progress.iter().map(String::as_str))
        .chain(request.next_actions.iter().map(String::as_str))
        .chain(request.blockers.iter().map(String::as_str))
        .chain(request.do_not_repeat.iter().map(String::as_str))
        .any(crate::redaction::contains_sensitive_text);
    if sensitive {
        return Err(AppError::Invalid(
            "handoff content cannot contain sensitive data".into(),
        ));
    }
    Ok(())
}

fn content_json(request: &HandoffWriteInput) -> serde_json::Value {
    json!({
        "summary": request.summary,
        "completed": request.completed,
        "in_progress": request.in_progress,
        "next_actions": request.next_actions,
        "blockers": request.blockers,
        "artifacts": request.artifacts,
        "verification_state": request.verification_state,
        "do_not_repeat": request.do_not_repeat,
        "written_by": request.written_by,
        "source_thread": request.thread_id,
    })
}

fn request_hash(request: &HandoffWriteInput) -> Result<[u8; 32], AppError> {
    let bytes =
        serde_json::to_vec(request).map_err(|error| AppError::Invalid(error.to_string()))?;
    Ok(Sha256::digest(bytes).into())
}

#[async_trait]
impl ContinuityStore for PgContinuityStore {
    async fn ready(&self) -> Result<(), AppError> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='continuity' AND table_name='handoffs')",
        )
        .fetch_one(&self.pool)
        .await?
        .then_some(())
        .ok_or_else(|| AppError::Internal("continuity schema is not applied".into()))
    }

    async fn validate_context(
        &self,
        identity: &Identity,
        context: &ContextRef,
    ) -> Result<ContextValidation, AppError> {
        if context.key.len() > 1024 {
            return Err(AppError::Invalid(
                "context key must be 1..1024 bytes".into(),
            ));
        }
        if let Some(reason) = forbidden_context_reason(&context.key) {
            return Ok(ContextValidation {
                available: false,
                context_type: context.kind,
                normalized_key: context.key.clone(),
                reason: Some(reason.into()),
                has_active_handoff: false,
            });
        }
        let has_active: Option<bool> = sqlx::query_scalar(
            r#"SELECT EXISTS(
                 SELECT 1 FROM continuity.contexts c JOIN continuity.handoffs h ON h.handoff_id=c.active_handoff_id
                 WHERE c.tenant_id=$1 AND c.user_id=$2 AND c.context_type=$3 AND c.context_key=$4
                   AND h.status='active' AND h.expires_at>clock_timestamp())"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(context.kind.as_db())
        .bind(&context.key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(ContextValidation {
            available: true,
            context_type: context.kind,
            normalized_key: context.key.clone(),
            reason: None,
            has_active_handoff: has_active.unwrap_or(false),
        })
    }

    async fn write(
        &self,
        identity: &Identity,
        request: &HandoffWriteInput,
    ) -> Result<Handoff65, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        validate_write(request)?;
        let request_hash = request_hash(request)?;
        let content = content_json(request);
        let content_hash: [u8; 32] = Sha256::digest(content.to_string().as_bytes()).into();
        let expires = request
            .expires_at
            .unwrap_or_else(|| Utc::now() + Duration::hours(48));
        let mut tx = self.pool.begin().await?;
        Self::set_write_guards(&mut tx).await?;
        // Serialize all writers to the same exact context so supersession is a
        // clean compare-and-swap even under concurrency.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "handoff-ctx:{}:{}:{}:{}",
                identity.tenant_id,
                identity.user_id,
                request.context.kind.as_db(),
                request.context.key
            ))
            .execute(&mut *tx)
            .await?;
        // Idempotent replay: same request_id returns the prior handoff.
        if let Some(existing) = sqlx::query(
            "SELECT handoff_id,request_hash FROM continuity.handoffs WHERE tenant_id=$1 AND user_id=$2 AND producing_consumer_id=$3 AND request_id=$4",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.request_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            if existing.get::<Vec<u8>, _>("request_hash") != request_hash {
                return Err(AppError::Conflict(
                    "handoff idempotency key reused with a different request".into(),
                ));
            }
            let handoff_id: Uuid = existing.get("handoff_id");
            let handoff = self.load_handoff(&mut tx, identity, handoff_id).await?;
            tx.commit().await?;
            return Ok(handoff);
        }
        let context = sqlx::query(
            r#"INSERT INTO continuity.contexts(tenant_id,user_id,context_type,context_key,family_id)
               VALUES($1,$2,$3,$4,$5)
               ON CONFLICT ON CONSTRAINT contexts_identity_key
               DO UPDATE SET family_id=COALESCE(continuity.contexts.family_id,EXCLUDED.family_id)
               RETURNING context_id,active_handoff_id"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.context.kind.as_db())
        .bind(&request.context.key)
        .bind(&request.context.family_id)
        .fetch_one(&mut *tx)
        .await?;
        let context_id: Uuid = context.get("context_id");
        let current_active: Option<Uuid> = context.get("active_handoff_id");
        // Compare-and-swap: reject a write whose observed active pointer is stale.
        if let Some(expected) = request.expected_active_id
            && Some(expected) != current_active
        {
            let conflict = HandoffConflict {
                current_active_id: current_active,
                reason: "active handoff changed since it was read".into(),
            };
            return Err(AppError::Conflict(
                serde_json::to_string(&conflict)
                    .unwrap_or_else(|_| "active handoff changed".into()),
            ));
        }
        // Structurally supersede exactly the prior active handoff before the new
        // one becomes active, so the partial unique index holds throughout.
        if let Some(active_id) = current_active {
            sqlx::query("UPDATE continuity.handoffs SET status='superseded' WHERE handoff_id=$1 AND status='active'")
                .bind(active_id)
                .execute(&mut *tx)
                .await?;
        }
        let inserted = sqlx::query(
            r#"INSERT INTO continuity.handoffs
               (tenant_id,user_id,context_id,producing_consumer_id,producing_thread_id,
                producing_session_id,summary,content,predecessor_handoff_id,source_snapshot_revision,
                content_hash,request_id,request_hash,expires_at)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
               RETURNING handoff_id"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(context_id)
        .bind(identity.consumer_id)
        .bind(&request.thread_id)
        .bind(&request.session_id)
        .bind(&request.summary)
        .bind(&content)
        .bind(current_active)
        .bind(request.source_snapshot_revision.unwrap_or(0))
        .bind(content_hash.as_slice())
        .bind(request.request_id)
        .bind(request_hash.as_slice())
        .bind(expires)
        .fetch_one(&mut *tx)
        .await?;
        let handoff_id: Uuid = inserted.get("handoff_id");
        sqlx::query("UPDATE continuity.contexts SET active_handoff_id=$2 WHERE context_id=$1")
            .bind(context_id)
            .bind(handoff_id)
            .execute(&mut *tx)
            .await?;
        for (kind, items) in [
            ("in_progress", &request.in_progress),
            ("next_action", &request.next_actions),
            ("blocker", &request.blockers),
        ] {
            for (ordinal, text) in items.iter().enumerate() {
                sqlx::query("INSERT INTO continuity.handoff_items(tenant_id,user_id,handoff_id,item_kind,ordinal,text) VALUES($1,$2,$3,$4,$5,$6)")
                    .bind(identity.tenant_id)
                    .bind(identity.user_id)
                    .bind(handoff_id)
                    .bind(kind)
                    .bind(ordinal as i32)
                    .bind(text)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        let handoff = self.load_handoff(&mut tx, identity, handoff_id).await?;
        tx.commit().await?;
        Ok(handoff)
    }

    async fn get_exact(
        &self,
        identity: &Identity,
        context: &ContextRef,
    ) -> Result<Option<Handoff65>, AppError> {
        if let Some(_reason) = forbidden_context_reason(&context.key) {
            // A forbidden broad key never resolves to a handoff.
            return Ok(None);
        }
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"SELECT h.handoff_id
               FROM continuity.contexts c JOIN continuity.handoffs h ON h.handoff_id=c.active_handoff_id
               WHERE c.tenant_id=$1 AND c.user_id=$2 AND c.context_type=$3 AND c.context_key=$4
                 AND h.status='active' AND h.expires_at>clock_timestamp()"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(context.kind.as_db())
        .bind(&context.key)
        .fetch_optional(&mut *tx)
        .await?;
        let result = match row {
            Some(row) => Some(
                self.load_handoff(&mut tx, identity, row.get("handoff_id"))
                    .await?,
            ),
            None => None,
        };
        tx.commit().await?;
        Ok(result)
    }

    async fn acknowledge(
        &self,
        identity: &Identity,
        request: &AckRequest,
    ) -> Result<AckResult, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        if request.session_id.trim().is_empty() || request.session_id.len() > 256 {
            return Err(AppError::Invalid("session_id must be 1..256 bytes".into()));
        }
        // The handoff must exist within this tenant/user.
        let exists: Option<bool> = sqlx::query_scalar(
            "SELECT true FROM continuity.handoffs WHERE tenant_id=$1 AND user_id=$2 AND handoff_id=$3",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.handoff_id)
        .fetch_optional(&self.pool)
        .await?;
        if exists.is_none() {
            return Err(AppError::NotFound);
        }
        // Idempotent per (handoff, consumer): repeated acknowledgement is a no-op
        // and never rewrites the first receipt time.
        let row = sqlx::query(
            r#"INSERT INTO continuity.acknowledgements(tenant_id,user_id,handoff_id,consumer_id,session_id)
               VALUES($1,$2,$3,$4,$5)
               ON CONFLICT(handoff_id,consumer_id) DO NOTHING
               RETURNING acknowledged_at"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.handoff_id)
        .bind(identity.consumer_id)
        .bind(&request.session_id)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(row) => Ok(AckResult {
                handoff_id: request.handoff_id,
                newly_acknowledged: true,
                first_acknowledged_at: row.get("acknowledged_at"),
            }),
            None => {
                let existing: DateTime<Utc> = sqlx::query_scalar(
                    "SELECT acknowledged_at FROM continuity.acknowledgements WHERE handoff_id=$1 AND consumer_id=$2",
                )
                .bind(request.handoff_id)
                .bind(identity.consumer_id)
                .fetch_one(&self.pool)
                .await?;
                Ok(AckResult {
                    handoff_id: request.handoff_id,
                    newly_acknowledged: false,
                    first_acknowledged_at: existing,
                })
            }
        }
    }

    async fn complete_items(
        &self,
        identity: &Identity,
        request: &CompleteItemsRequest,
    ) -> Result<Handoff65, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        if request.item_ids.is_empty() || request.item_ids.len() > MAX_ITEMS_PER_KIND * 3 {
            return Err(AppError::Invalid("item_ids must be 1..600".into()));
        }
        let mut tx = self.pool.begin().await?;
        Self::set_write_guards(&mut tx).await?;
        let owns: Option<bool> = sqlx::query_scalar(
            "SELECT true FROM continuity.handoffs WHERE tenant_id=$1 AND user_id=$2 AND handoff_id=$3",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.handoff_id)
        .fetch_optional(&mut *tx)
        .await?;
        if owns.is_none() {
            return Err(AppError::NotFound);
        }
        // Idempotent: only open items transition; already-completed items are
        // left untouched, preserving their first completion time.
        sqlx::query(
            "UPDATE continuity.handoff_items SET status='completed',completed_at=clock_timestamp(),completed_by=$4 WHERE handoff_id=$1 AND item_id=ANY($2) AND status='open' AND tenant_id=$3",
        )
        .bind(request.handoff_id)
        .bind(&request.item_ids)
        .bind(identity.tenant_id)
        .bind(&identity.actor)
        .execute(&mut *tx)
        .await?;
        let handoff = self
            .load_handoff(&mut tx, identity, request.handoff_id)
            .await?;
        tx.commit().await?;
        Ok(handoff)
    }

    async fn close(
        &self,
        identity: &Identity,
        request: &CloseRequest,
    ) -> Result<CloseResult, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        validate_context_ref(&request.context)?;
        let mut tx = self.pool.begin().await?;
        Self::set_write_guards(&mut tx).await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "handoff-ctx:{}:{}:{}:{}",
                identity.tenant_id,
                identity.user_id,
                request.context.kind.as_db(),
                request.context.key
            ))
            .execute(&mut *tx)
            .await?;
        let context = sqlx::query(
            "SELECT context_id,active_handoff_id FROM continuity.contexts WHERE tenant_id=$1 AND user_id=$2 AND context_type=$3 AND context_key=$4",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(request.context.kind.as_db())
        .bind(&request.context.key)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::NotFound)?;
        let active: Option<Uuid> = context.get("active_handoff_id");
        // CAS on the exact active handoff so a cleanly completed task cannot
        // accidentally close a newer active continuation.
        if active != Some(request.expected_active_id) {
            let conflict = HandoffConflict {
                current_active_id: active,
                reason: "active handoff changed before close".into(),
            };
            return Err(AppError::Conflict(
                serde_json::to_string(&conflict)
                    .unwrap_or_else(|_| "active handoff changed".into()),
            ));
        }
        let context_id: Uuid = context.get("context_id");
        sqlx::query("UPDATE continuity.handoffs SET status='completed',closed_at=clock_timestamp() WHERE handoff_id=$1 AND status='active'")
            .bind(request.expected_active_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE continuity.contexts SET active_handoff_id=NULL WHERE context_id=$1")
            .bind(context_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(CloseResult {
            closed_handoff_id: request.expected_active_id,
            status: "completed".into(),
        })
    }

    async fn history(
        &self,
        identity: &Identity,
        context: &ContextRef,
        limit: usize,
    ) -> Result<Vec<HandoffSummary>, AppError> {
        if context.key.len() > 1024 {
            return Err(AppError::Invalid(
                "context key must be 1..1024 bytes".into(),
            ));
        }
        let limit = limit.clamp(1, 200) as i64;
        let rows = sqlx::query(
            r#"SELECT h.handoff_id,h.status,h.summary,h.producing_consumer_id,h.producing_session_id,
                      h.created_at,h.expires_at,h.predecessor_handoff_id
               FROM continuity.handoffs h JOIN continuity.contexts c ON c.context_id=h.context_id
               WHERE c.tenant_id=$1 AND c.user_id=$2 AND c.context_type=$3 AND c.context_key=$4
               ORDER BY h.created_at DESC
               LIMIT $5"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(context.kind.as_db())
        .bind(&context.key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(|row| HandoffSummary {
                handoff_id: row.get("handoff_id"),
                status: row.get("status"),
                summary: row.get("summary"),
                producing_consumer_id: row.get("producing_consumer_id"),
                producing_session_id: row.get("producing_session_id"),
                created_at: row.get("created_at"),
                expires_at: row.get("expires_at"),
                predecessor_handoff_id: row.get("predecessor_handoff_id"),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forbidden_keys_are_rejected() {
        for key in [
            "/",
            "~",
            "/home/justin",
            "/Users/justin/",
            "/root",
            "C:\\",
            "",
        ] {
            assert!(
                forbidden_context_reason(key).is_some(),
                "expected {key:?} to be forbidden"
            );
        }
    }

    #[test]
    fn legitimate_typed_keys_are_allowed() {
        for key in [
            "git:github.com/justin/interlock@main",
            "durable-project:interlock-v6",
            "thread:codex-01H8X...",
            "/home/justin/Projects/interlock-v6", // deeper than a home root
        ] {
            assert!(
                forbidden_context_reason(key).is_none(),
                "expected {key:?} to be allowed"
            );
        }
    }
}
