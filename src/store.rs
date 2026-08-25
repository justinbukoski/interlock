use crate::{
    auth::{Identity, TokenRole},
    domain::{
        Authority, Candidate, CandidatePromotionRequest, CandidateState, CandidateWriteRequest,
        EpistemicStatus, Handoff, HandoffWriteRequest, MemoryItem, MemoryKind, MemoryWriteRequest,
        Observation, ObservationWriteRequest, RecallIntent, ScopeSelector, WriteResponse,
    },
    embedding::{Embedding, EmbeddingProvider},
    error::AppError,
};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use uuid::Uuid;

#[async_trait]
pub trait MemoryStore: Send + Sync {
    async fn ready(&self) -> Result<(), AppError>;
    async fn recall(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
        query: &str,
        query_embedding: Option<&Embedding>,
        intent: RecallIntent,
        limit: usize,
    ) -> Result<Vec<MemoryItem>, AppError>;
    async fn mandatory(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
    ) -> Result<Vec<MemoryItem>, AppError>;
    async fn project_state(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
    ) -> Result<Vec<MemoryItem>, AppError>;
    async fn remember(
        &self,
        identity: &Identity,
        request: &MemoryWriteRequest,
    ) -> Result<WriteResponse, AppError>;
    async fn observe(
        &self,
        identity: &Identity,
        request: &ObservationWriteRequest,
    ) -> Result<Observation, AppError>;
    async fn create_candidate(
        &self,
        identity: &Identity,
        request: &CandidateWriteRequest,
    ) -> Result<Candidate, AppError>;
    async fn promote_candidate(
        &self,
        identity: &Identity,
        request: &CandidatePromotionRequest,
    ) -> Result<WriteResponse, AppError>;
    async fn write_handoff(
        &self,
        identity: &Identity,
        request: &HandoffWriteRequest,
    ) -> Result<Handoff, AppError>;
    async fn latest_handoff(
        &self,
        identity: &Identity,
        project_key: &str,
    ) -> Result<Option<Handoff>, AppError>;
    async fn snapshot_revision(&self, identity: &Identity) -> Result<i64, AppError>;
}

#[derive(Clone)]
pub struct PgMemoryStore {
    pool: PgPool,
}

impl PgMemoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn migrate(&self) -> Result<(), AppError> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|error| AppError::Internal(error.to_string()))
    }

    /// Best-effort derived-data worker. A failed embedding leaves the row eligible
    /// for a later retry and never rolls back an acknowledged canonical write.
    pub async fn embed_pending(
        &self,
        provider: &dyn EmbeddingProvider,
        target_model: &str,
        batch_size: i64,
        worker_id: Uuid,
    ) -> Result<usize, AppError> {
        let limit = batch_size.clamp(2, 32);
        let proposition_limit = (limit + 1) / 2;
        let lease_seconds = 30 + limit * 4;
        let mut tx = self.pool.begin().await?;
        let proposition_rows = sqlx::query(
            r#"WITH candidates AS (
                 SELECT proposition_id FROM proposition_embeddings
                 WHERE (embedding IS NULL OR embedding_model<>$2)
                   AND quarantined_at IS NULL AND next_attempt_at<=clock_timestamp()
                   AND (lease_until IS NULL OR lease_until<clock_timestamp())
                 ORDER BY next_attempt_at,proposition_id FOR UPDATE SKIP LOCKED LIMIT $1
               ), claimed AS (
                 UPDATE proposition_embeddings e
                 SET lease_owner=$3,lease_until=clock_timestamp()+($4 * interval '1 second')
                 FROM candidates c WHERE e.proposition_id=c.proposition_id
                 RETURNING e.proposition_id
               )
               SELECT c.proposition_id AS id,'proposition'::text AS kind,p.rendered AS content
               FROM claimed c JOIN propositions p ON p.id=c.proposition_id"#,
        )
        .bind(proposition_limit)
        .bind(target_model)
        .bind(worker_id)
        .bind(lease_seconds)
        .fetch_all(&mut *tx)
        .await?;
        let remaining = limit - proposition_rows.len() as i64;
        let observation_rows = sqlx::query(
            r#"WITH candidates AS (
                 SELECT observation_id FROM observation_embeddings
                 WHERE (embedding IS NULL OR embedding_model<>$2)
                   AND quarantined_at IS NULL AND next_attempt_at<=clock_timestamp()
                   AND (lease_until IS NULL OR lease_until<clock_timestamp())
                 ORDER BY next_attempt_at,observation_id FOR UPDATE SKIP LOCKED LIMIT $1
               ), claimed AS (
                 UPDATE observation_embeddings e
                 SET lease_owner=$3,lease_until=clock_timestamp()+($4 * interval '1 second')
                 FROM candidates c WHERE e.observation_id=c.observation_id
                 RETURNING e.observation_id
               )
               SELECT c.observation_id AS id,'observation'::text AS kind,o.redacted_content AS content
               FROM claimed c JOIN observations o ON o.id=c.observation_id"#,
        ).bind(remaining).bind(target_model).bind(worker_id).bind(lease_seconds).fetch_all(&mut *tx).await?;
        tx.commit().await?;
        let rows = proposition_rows.into_iter().chain(observation_rows);
        let mut completed = 0;
        for row in rows {
            let id: Uuid = row.get("id");
            let kind: String = row.get("kind");
            let content: String = row.get("content");
            let embedding = match provider.embed(&content).await {
                Ok(value) => value,
                Err(_) => {
                    self.fail_embedding(&kind, id, worker_id, "provider_error")
                        .await?;
                    continue;
                }
            };
            if embedding.model != target_model {
                self.fail_embedding(&kind, id, worker_id, "model_mismatch")
                    .await?;
                continue;
            }
            let vector = match vector_literal(&embedding.values) {
                Ok(vector) => vector,
                Err(_) => {
                    self.fail_embedding(&kind, id, worker_id, "invalid_vector")
                        .await?;
                    continue;
                }
            };
            let query = match kind.as_str() {
                "proposition" => {
                    "UPDATE proposition_embeddings SET embedding=$2::vector,embedding_model=$3,embedded_at=clock_timestamp(),attempts=0,last_error=NULL,next_attempt_at=clock_timestamp(),quarantined_at=NULL,lease_owner=NULL,lease_until=NULL WHERE proposition_id=$1 AND lease_owner=$4"
                }
                "observation" => {
                    "UPDATE observation_embeddings SET embedding=$2::vector,embedding_model=$3,embedded_at=clock_timestamp(),attempts=0,last_error=NULL,next_attempt_at=clock_timestamp(),quarantined_at=NULL,lease_owner=NULL,lease_until=NULL WHERE observation_id=$1 AND lease_owner=$4"
                }
                _ => return Err(AppError::Internal("unknown embedding work kind".into())),
            };
            match sqlx::query(query)
                .bind(id)
                .bind(vector)
                .bind(embedding.model)
                .bind(worker_id)
                .execute(&self.pool)
                .await
            {
                Ok(result) => completed += result.rows_affected() as usize,
                Err(_) => {
                    self.fail_embedding(&kind, id, worker_id, "persistence_error")
                        .await?
                }
            }
        }
        Ok(completed)
    }

    async fn fail_embedding(
        &self,
        kind: &str,
        id: Uuid,
        worker_id: Uuid,
        error_code: &'static str,
    ) -> Result<(), AppError> {
        let (table, key) = match kind {
            "proposition" => ("proposition_embeddings", "proposition_id"),
            "observation" => ("observation_embeddings", "observation_id"),
            _ => return Err(AppError::Internal("unknown embedding work kind".into())),
        };
        let failure = format!(
            "UPDATE {table} SET attempts=attempts+1,last_error=$2,next_attempt_at=clock_timestamp()+LEAST(interval '1 hour',interval '5 seconds'*power(2,LEAST(attempts,9))),quarantined_at=CASE WHEN attempts>=9 THEN clock_timestamp() ELSE NULL END,lease_owner=NULL,lease_until=NULL WHERE {key}=$1 AND lease_owner=$3"
        );
        sqlx::query(&failure)
            .bind(id)
            .bind(error_code)
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn release_embedding_leases(&self, worker_id: Uuid) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("UPDATE proposition_embeddings SET lease_owner=NULL,lease_until=NULL WHERE lease_owner=$1")
            .bind(worker_id).execute(&mut *tx).await?;
        sqlx::query("UPDATE observation_embeddings SET lease_owner=NULL,lease_until=NULL WHERE lease_owner=$1")
            .bind(worker_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn scope_id(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        identity: &Identity,
        scope: &ScopeSelector,
        shared_consumer: bool,
    ) -> Result<(Uuid, String), AppError> {
        let level = if scope.session_id.is_some() {
            "session"
        } else if scope.thread_id.is_some() {
            "thread"
        } else if scope.repository_key.is_some() {
            "repository"
        } else if scope.project_key.is_some() {
            "project"
        } else {
            "user"
        };
        let applicable_consumer = (!shared_consumer).then_some(identity.consumer_id);
        let row = sqlx::query(
            r#"INSERT INTO scopes
               (tenant_id,user_id,consumer_id,project_key,repository_key,thread_id,session_id,scope_level)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT ON CONSTRAINT scopes_identity_key
               DO UPDATE SET scope_level=EXCLUDED.scope_level
               RETURNING id"#,
        )
        .bind(identity.tenant_id).bind(identity.user_id).bind(applicable_consumer)
        .bind(&scope.project_key).bind(&scope.repository_key).bind(&scope.thread_id)
        .bind(&scope.session_id).bind(level)
        .fetch_one(&mut **transaction).await?;
        Ok((row.get("id"), level.into()))
    }

    async fn configure_write_transaction(
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<(), AppError> {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut **transaction)
            .await?;
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
        ] {
            sqlx::query(statement).execute(&mut **transaction).await?;
        }
        Ok(())
    }

    async fn canonical_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        identity: &Identity,
        request: &MemoryWriteRequest,
    ) -> Result<WriteResponse, AppError> {
        validate_memory_request(request)?;
        authorize_canonical(identity, request)?;
        let request_hash = request_hash(request)?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "request:{}:{}",
                identity.consumer_id, request.request_id
            ))
            .execute(&mut **tx)
            .await?;
        if let Some(row) = sqlx::query("SELECT request_hash,response FROM canonical_requests WHERE tenant_id=$1 AND user_id=$2 AND consumer_id=$3 AND request_id=$4")
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(request.request_id)
            .fetch_optional(&mut **tx).await?
        {
            if row.get::<Vec<u8>, _>("request_hash") != request_hash {
                return Err(AppError::Conflict("idempotency key reused with a different request".into()));
            }
            return serde_json::from_value(row.get("response"))
                .map_err(|error| AppError::Internal(format!("invalid stored idempotency response: {error}")));
        }
        sqlx::query("INSERT INTO canonical_mutations(tenant_id,user_id,mutation_id,actor) VALUES($1,$2,$3,$4)")
            .bind(identity.tenant_id).bind(identity.user_id).bind(request.request_id).bind(&identity.actor)
            .execute(&mut **tx).await?;
        let shared_consumer = matches!(
            request.predicate.as_str(),
            "system.constraint" | "system.directive"
        );
        let (scope_id, _) = self
            .scope_id(tx, identity, &request.scope, shared_consumer)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "{scope_id}:{}:{}",
                request.subject, request.predicate
            ))
            .execute(&mut **tx)
            .await?;
        let predicate = sqlx::query("SELECT id,cardinality,value_type,minimum_authority_rank,owner_confirmation_required FROM predicates WHERE key=$1")
            .bind(&request.predicate).fetch_optional(&mut **tx).await?.ok_or_else(|| AppError::Invalid("unknown predicate".into()))?;
        let authority_rank = request.authority.rank();
        let minimum: i16 = predicate.get("minimum_authority_rank");
        if authority_rank > minimum {
            return Err(AppError::Conflict(
                "authority below predicate minimum".into(),
            ));
        }
        if predicate.get::<bool, _>("owner_confirmation_required")
            && request.authority != Authority::OwnerInstruction
        {
            return Err(AppError::Conflict(
                "predicate requires owner authority".into(),
            ));
        }
        let predicate_id: Uuid = predicate.get("id");
        let cardinality: String = predicate.get("cardinality");
        let value_type: String = predicate.get("value_type");
        let type_matches = match value_type.as_str() {
            "string" => request.object.is_string(),
            "number" => request.object.is_number(),
            "boolean" => request.object.is_boolean(),
            "object" => request.object.is_object(),
            "array" => request.object.is_array(),
            "any" => true,
            _ => false,
        };
        if !type_matches {
            return Err(AppError::Invalid(format!(
                "predicate requires {value_type} value"
            )));
        }
        let superseded_rows = if cardinality == "single" {
            let current = sqlx::query("SELECT id,authority_rank FROM propositions WHERE scope_id=$1 AND subject_key=$2 AND predicate_id=$3 AND status='current' FOR UPDATE")
                .bind(scope_id).bind(&request.subject).bind(predicate_id).fetch_all(&mut **tx).await?;
            if current
                .iter()
                .any(|row| row.get::<i16, _>("authority_rank") < authority_rank)
            {
                return Err(AppError::Conflict(
                    "lower authority cannot supersede current proposition".into(),
                ));
            }
            sqlx::query("UPDATE propositions SET status='superseded',valid_to=clock_timestamp(),last_mutation_id=$4 WHERE scope_id=$1 AND subject_key=$2 AND predicate_id=$3 AND status='current' RETURNING id")
                .bind(scope_id).bind(&request.subject).bind(predicate_id).bind(request.request_id).fetch_all(&mut **tx).await?
        } else {
            Vec::new()
        };
        let superseded_ids: Vec<Uuid> = superseded_rows.iter().map(|row| row.get("id")).collect();
        let authority = enum_text(request.authority)?;
        let epistemic = enum_text(request.epistemic_status)?;
        let rendered = format!(
            "{} {} {}",
            request.subject, request.predicate, request.object
        );
        let inserted = sqlx::query(r#"INSERT INTO propositions
            (tenant_id,user_id,writer_consumer_id,scope_id,subject_key,predicate_id,cardinality,object_value,rendered,authority,authority_rank,
             epistemic_status,source_type,source_ref,status,valid_from,recorded_at,last_mutation_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'current',clock_timestamp(),clock_timestamp(),$15) RETURNING id"#)
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id)
            .bind(scope_id).bind(&request.subject).bind(predicate_id).bind(&cardinality).bind(&request.object)
            .bind(&rendered).bind(&authority).bind(authority_rank).bind(&epistemic).bind(&request.source_type)
            .bind(&request.source_ref).bind(request.request_id).fetch_one(&mut **tx).await?;
        let id: Uuid = inserted.get("id");
        for old_id in &superseded_ids {
            sqlx::query("INSERT INTO proposition_edges(tenant_id,user_id,from_id,to_id,edge_type,reason,mutation_id) VALUES($1,$2,$3,$4,'supersedes',$5,$6)")
                .bind(identity.tenant_id).bind(identity.user_id).bind(id).bind(old_id).bind(&request.reason).bind(request.request_id).execute(&mut **tx).await?;
        }
        sqlx::query("INSERT INTO audit_events(tenant_id,user_id,actor,event_type,after_id,before_ids,reason,request_id) VALUES($1,$2,$3,'canonical_write',$4,$5,$6,$7)")
            .bind(identity.tenant_id).bind(identity.user_id).bind(&identity.actor).bind(id).bind(&superseded_ids).bind(&request.reason).bind(request.request_id)
            .execute(&mut **tx).await?;
        sqlx::query("INSERT INTO outbox(tenant_id,user_id,event_type,aggregate_id,payload) VALUES($1,$2,'canonical_changed',$3,jsonb_build_object('tenant_id',$1::text,'user_id',$2::text,'proposition_id',$3::text))")
            .bind(identity.tenant_id).bind(identity.user_id).bind(id).execute(&mut **tx).await?;
        let revision: i64 = sqlx::query("INSERT INTO snapshot_revisions(tenant_id,user_id,revision) VALUES($1,$2,1) ON CONFLICT(tenant_id,user_id) DO UPDATE SET revision=snapshot_revisions.revision+1 RETURNING revision")
            .bind(identity.tenant_id).bind(identity.user_id).fetch_one(&mut **tx).await?.get("revision");
        let response = WriteResponse {
            id,
            superseded_ids,
            snapshot_revision: revision,
        };
        sqlx::query("INSERT INTO canonical_requests(tenant_id,user_id,consumer_id,request_id,request_hash,actor,role,response) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(request.request_id)
            .bind(request_hash.as_slice()).bind(&identity.actor).bind(enum_text(identity.role)?)
            .bind(serde_json::to_value(&response).map_err(|error| AppError::Internal(error.to_string()))?)
            .execute(&mut **tx).await?;
        Ok(response)
    }
}

fn authority_from_db(value: &str) -> Result<Authority, AppError> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|error| AppError::Internal(error.to_string()))
}
fn epistemic_from_db(value: &str) -> Result<EpistemicStatus, AppError> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|error| AppError::Internal(error.to_string()))
}
fn enum_text<T: serde::Serialize>(value: T) -> Result<String, AppError> {
    serde_json::to_value(value)
        .map_err(|error| AppError::Internal(error.to_string()))?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| AppError::Internal("enum serialization failed".into()))
}

fn request_hash<T: serde::Serialize>(value: &T) -> Result<[u8; 32], AppError> {
    let bytes = serde_json::to_vec(value).map_err(|error| AppError::Invalid(error.to_string()))?;
    Ok(Sha256::digest(bytes).into())
}

fn authorize_canonical(identity: &Identity, request: &MemoryWriteRequest) -> Result<(), AppError> {
    if !identity.role.can_write() {
        return Err(AppError::Forbidden);
    }
    let policy = matches!(
        request.predicate.as_str(),
        "system.constraint" | "system.directive"
    );
    if (policy || request.authority == Authority::OwnerInstruction) && !identity.role.is_owner() {
        return Err(AppError::Forbidden);
    }
    if identity.role == TokenRole::Writer
        && !matches!(
            request.authority,
            Authority::TrustedAgentReport | Authority::Inference
        )
    {
        return Err(AppError::Forbidden);
    }
    if identity.role == TokenRole::Verifier && request.authority == Authority::OwnerInstruction {
        return Err(AppError::Forbidden);
    }
    Ok(())
}

fn validate_memory_request(request: &MemoryWriteRequest) -> Result<(), AppError> {
    request.scope.validate().map_err(AppError::Invalid)?;
    if request.subject.trim().is_empty()
        || request.subject.len() > 512
        || request.predicate.trim().is_empty()
        || request.predicate.len() > 128
        || request.source_type.trim().is_empty()
        || request.source_type.len() > 64
        || request.source_ref.trim().is_empty()
        || request.source_ref.len() > 2048
        || request.reason.trim().is_empty()
        || request.reason.len() > 4096
    {
        return Err(AppError::Invalid(
            "canonical memory fields are required and bounded".into(),
        ));
    }
    if serde_json::to_vec(&request.object)
        .map_err(|error| AppError::Invalid(error.to_string()))?
        .len()
        > 64 * 1024
    {
        return Err(AppError::Invalid("object exceeds 64 KiB".into()));
    }
    if crate::redaction::contains_sensitive_json(&request.object)
        || [
            request.subject.as_str(),
            request.predicate.as_str(),
            request.source_type.as_str(),
            request.source_ref.as_str(),
            request.reason.as_str(),
        ]
        .into_iter()
        .any(crate::redaction::contains_sensitive_text)
    {
        return Err(AppError::Invalid(
            "canonical object contains sensitive data and cannot enter searchable memory".into(),
        ));
    }
    Ok(())
}

fn validate_observation_request(request: &ObservationWriteRequest) -> Result<(), AppError> {
    request.scope.validate().map_err(AppError::Invalid)?;
    if request.source_event_id.trim().is_empty()
        || request.source_event_id.len() > 512
        || request.event_kind.trim().is_empty()
        || request.event_kind.len() > 128
        || request.content.trim().is_empty()
        || request.content.len() > 96 * 1024
        || request
            .raw_content_ref
            .as_ref()
            .is_some_and(|value| !valid_encrypted_reference(value))
    {
        return Err(AppError::Invalid(
            "observation identifiers and content are required and bounded".into(),
        ));
    }
    if [
        request.source_event_id.as_str(),
        request.event_kind.as_str(),
    ]
    .into_iter()
    .any(crate::redaction::contains_sensitive_text)
    {
        return Err(AppError::Invalid(
            "observation identifiers cannot contain sensitive data".into(),
        ));
    }
    let now = Utc::now();
    if request.observed_at > now + Duration::minutes(5)
        || request.observed_at < now - Duration::days(3650)
    {
        return Err(AppError::Invalid(
            "observed_at is outside the accepted time window".into(),
        ));
    }
    Ok(())
}

fn validate_candidate_request(request: &CandidateWriteRequest) -> Result<(), AppError> {
    request.scope.validate().map_err(AppError::Invalid)?;
    if request.derivation_key.trim().is_empty()
        || request.derivation_key.len() > 512
        || request.subject.trim().is_empty()
        || request.subject.len() > 512
        || request.predicate.trim().is_empty()
        || request.predicate.len() > 128
        || request.extractor_model.trim().is_empty()
        || request.extractor_model.len() > 256
        || request.extractor_version.trim().is_empty()
        || request.extractor_version.len() > 128
        || request.prompt_version.trim().is_empty()
        || request.prompt_version.len() > 128
        || !request.confidence.is_finite()
        || !(0.0..=1.0).contains(&request.confidence)
        || request.evidence_observation_ids.is_empty()
        || request.evidence_observation_ids.len() > 100
    {
        return Err(AppError::Invalid(
            "candidate fields, confidence, and 1..100 evidence IDs are required".into(),
        ));
    }
    let unique: HashSet<_> = request.evidence_observation_ids.iter().collect();
    if unique.len() != request.evidence_observation_ids.len() {
        return Err(AppError::Invalid(
            "candidate evidence IDs must be unique".into(),
        ));
    }
    if serde_json::to_vec(&request.object)
        .map_err(|error| AppError::Invalid(error.to_string()))?
        .len()
        > 64 * 1024
    {
        return Err(AppError::Invalid("candidate object exceeds 64 KiB".into()));
    }
    if crate::redaction::contains_sensitive_json(&request.object)
        || [
            request.derivation_key.as_str(),
            request.subject.as_str(),
            request.predicate.as_str(),
            request.extractor_model.as_str(),
            request.extractor_version.as_str(),
            request.prompt_version.as_str(),
        ]
        .into_iter()
        .any(crate::redaction::contains_sensitive_text)
    {
        return Err(AppError::Invalid(
            "candidate object contains sensitive data and must be quarantined outside searchable memory"
                .into(),
        ));
    }
    Ok(())
}

fn valid_encrypted_reference(value: &str) -> bool {
    let Some(identifier) = value.strip_prefix("encrypted:") else {
        return false;
    };
    !identifier.is_empty()
        && identifier.len() <= 128
        && identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn row_to_item(row: sqlx::postgres::PgRow) -> Result<MemoryItem, AppError> {
    Ok(MemoryItem {
        id: row.get("id"),
        kind: MemoryKind::Proposition,
        subject: row.get("subject_key"),
        predicate: row.get("predicate_key"),
        object: row.get("object_value"),
        rendered: row.get("rendered"),
        authority: authority_from_db(row.get("authority"))?,
        epistemic_status: epistemic_from_db(row.get("epistemic_status"))?,
        scope_level: row.get("scope_level"),
        source_type: row.get("source_type"),
        source_ref: row.get("source_ref"),
        observed_at: None,
        valid_from: row.get("valid_from"),
        valid_to: row.get("valid_to"),
        recorded_at: row.get("recorded_at"),
        state: row.get("status"),
        retrieval_reasons: vec!["current canonical proposition".into()],
    })
}

fn collapse_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<MemoryItem>, AppError> {
    let mut seen = HashSet::new();
    let mut items = Vec::new();
    for row in rows {
        let cardinality: &str = row.get("cardinality");
        let object_hash: Vec<u8> = row.get("object_hash");
        let mut key = format!(
            "{}\0{}",
            row.get::<&str, _>("subject_key"),
            row.get::<&str, _>("predicate_key")
        );
        if cardinality == "set" {
            key.push('\0');
            key.push_str(&hex::encode(object_hash));
        }
        if seen.insert(key) {
            items.push(row_to_item(row)?);
        }
    }
    Ok(items)
}

fn row_to_observation(row: &sqlx::postgres::PgRow) -> Observation {
    Observation {
        id: row.get("id"),
        source_event_id: row.get("source_event_id"),
        event_kind: row.get("event_kind"),
        scope_level: row.get("scope_level"),
        observed_at: row.get("observed_at"),
        redacted_content: row.get("redacted_content"),
        redaction_count: row.get::<i32, _>("redaction_count") as usize,
        ingested_at: row.get("ingested_at"),
    }
}

fn candidate_state_from_db(value: &str) -> Result<CandidateState, AppError> {
    serde_json::from_value(Value::String(value.to_owned()))
        .map_err(|error| AppError::Internal(error.to_string()))
}

fn row_to_candidate(row: &sqlx::postgres::PgRow) -> Result<Candidate, AppError> {
    Ok(Candidate {
        id: row.get("id"),
        derivation_key: row.get("derivation_key"),
        subject: row.get("subject_key"),
        predicate: row.get("predicate_key"),
        object: row.get("object_value"),
        authority_claim: authority_from_db(row.get("authority_claim"))?,
        epistemic_status: epistemic_from_db(row.get("epistemic_status"))?,
        confidence: row.get("confidence"),
        state: candidate_state_from_db(row.get("state"))?,
        canonical_proposition_id: row.get("canonical_proposition_id"),
        created_at: row.get("created_at"),
    })
}

const ITEM_SELECT: &str = r#"
 SELECT p.id,p.subject_key,pr.key AS predicate_key,p.cardinality,p.object_hash,p.object_value,p.rendered,
        p.authority,p.epistemic_status,s.scope_level,p.source_type,p.source_ref,
        p.valid_from,p.valid_to,p.recorded_at,p.status,s.specificity,(s.consumer_id IS NOT NULL) AS consumer_specific,
        p.authority_rank,p.search_document,pe.embedding,pe.embedding_model
 FROM propositions p JOIN predicates pr ON pr.id=p.predicate_id JOIN scopes s ON s.id=p.scope_id
 LEFT JOIN proposition_embeddings pe ON pe.proposition_id=p.id
 WHERE s.tenant_id=$1 AND s.user_id=$2 AND (s.consumer_id IS NULL OR s.consumer_id=$3)
   AND p.status='current' AND p.valid_to IS NULL
   AND (s.project_key IS NULL OR s.project_key=$4)
   AND (s.repository_key IS NULL OR s.repository_key=$5)
   AND (s.thread_id IS NULL OR s.thread_id=$6)
   AND (s.session_id IS NULL OR s.session_id=$7)
"#;

/// Query B of recall: authorization, scope, precedence, RRF fusion and late
/// payload hydration over an ANN candidate seed produced by Query A.
///
/// $1 tenant_id, $2 user_id, $3 consumer_id, $4 project_key, $5 repository_key,
/// $6 thread_id, $7 session_id, $8 query text, $9 output limit, $10 RRF lane
/// depth, $11 ANN candidate ids, $12 ANN candidate distances.
///
/// The precedence window runs over a narrow projection; `rendered`, the object
/// JSON and the source columns are joined back only for the rows that survive
/// selection. Carrying them (and the 1024-dimension vectors) through the window
/// sort is what spilled ~43MB of temp blocks per call in the previous shape.
///
/// With empty $11/$12 the semantic lane is empty and this degrades to
/// lexical-only retrieval, which is the correct behaviour when no query
/// embedding is available.
/// Whole-operation budget for the adaptive recall loop, comfortably inside
/// interlock-mcp's 20s request deadline. Individual statements keep their own
/// 5s statement_timeout; this bounds the ladder of retries as a whole.
const RECALL_DB_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

const RECALL_SQL: &str = r#"
WITH ranked_applicable AS MATERIALIZED (
  SELECT p.id,p.subject_key,pr.key AS predicate_key,p.cardinality,p.object_hash,
         p.search_document,s.specificity,(s.consumer_id IS NOT NULL) AS consumer_specific,
         p.authority_rank,p.recorded_at,
         row_number() OVER (
           PARTITION BY p.subject_key,pr.key,
             CASE WHEN p.cardinality='set' THEN encode(p.object_hash,'hex') ELSE '' END
           ORDER BY s.specificity DESC,(s.consumer_id IS NOT NULL) DESC,
                    p.authority_rank ASC,p.recorded_at DESC,p.id ASC
         ) AS precedence
  FROM propositions p
  JOIN predicates pr ON pr.id=p.predicate_id
  JOIN scopes s ON s.id=p.scope_id
  WHERE s.tenant_id=$1 AND s.user_id=$2
    AND (s.consumer_id IS NULL OR s.consumer_id=$3)
    AND p.status='current' AND p.valid_to IS NULL
    AND (s.project_key IS NULL OR s.project_key=$4)
    AND (s.repository_key IS NULL OR s.repository_key=$5)
    AND (s.thread_id IS NULL OR s.thread_id=$6)
    AND (s.session_id IS NULL OR s.session_id=$7)
), winners AS MATERIALIZED (
  SELECT * FROM ranked_applicable WHERE precedence=1
), lexical_matches AS (
  SELECT id,ts_rank_cd(search_document,websearch_to_tsquery('english',$8)) AS score
  FROM winners
  WHERE search_document @@ websearch_to_tsquery('english',$8)
     OR subject_key ILIKE '%'||$8||'%' OR predicate_key ILIKE '%'||$8||'%'
), lexical AS (
  SELECT id,row_number() OVER (ORDER BY score DESC,id) AS rank
  FROM lexical_matches ORDER BY score DESC,id LIMIT $10
), semantic_seed AS MATERIALIZED (
  SELECT seed.id,seed.distance
  FROM unnest($11::uuid[],$12::float8[]) AS seed(id,distance)
), semantic AS (
  -- Joining to winners re-applies identity, scope, status and precedence: a row
  -- the ANN seed found but the caller may not see cannot survive this join.
  SELECT s.id,row_number() OVER (ORDER BY s.distance,s.id) AS rank
  FROM semantic_seed s JOIN winners w USING (id)
  ORDER BY s.distance,s.id LIMIT $10
), fused AS (
  SELECT id,sum(score) AS relevance FROM (
    SELECT id,1.0/(60+rank) AS score FROM lexical
    UNION ALL SELECT id,1.0/(60+rank) AS score FROM semantic
  ) lanes GROUP BY id
), lane_counts AS (
  SELECT (SELECT count(*) FROM semantic) AS semantic_count
), selected AS MATERIALIZED (
  SELECT w.id,w.specificity,w.consumer_specific,w.authority_rank,w.recorded_at,f.relevance
  FROM winners w JOIN fused f USING (id)
  ORDER BY w.specificity DESC,w.consumer_specific DESC,w.authority_rank ASC,
           f.relevance DESC,w.recorded_at DESC,w.id ASC
  LIMIT $9
)
SELECT p.id,p.subject_key,pr.key AS predicate_key,p.cardinality,p.object_hash,
       p.object_value,p.rendered,p.authority,p.epistemic_status,s.scope_level,
       p.source_type,p.source_ref,p.valid_from,p.valid_to,p.recorded_at,p.status,
       lc.semantic_count
FROM selected x
JOIN propositions p ON p.id=x.id
JOIN predicates pr ON pr.id=p.predicate_id
JOIN scopes s ON s.id=p.scope_id
CROSS JOIN lane_counts lc
ORDER BY x.specificity DESC,x.consumer_specific DESC,x.authority_rank ASC,
         x.relevance DESC,x.recorded_at DESC,x.id ASC
"#;

pub(crate) fn vector_literal(values: &[f32]) -> Result<String, AppError> {
    if values.len() != 1024 || values.iter().any(|value| !value.is_finite()) {
        return Err(AppError::Invalid(
            "query embedding must contain 1024 finite values".into(),
        ));
    }
    Ok(format!(
        "[{}]",
        values
            .iter()
            .map(f32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    ))
}

#[async_trait]
impl MemoryStore for PgMemoryStore {
    async fn ready(&self) -> Result<(), AppError> {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE version=6 AND success) AND NOT EXISTS(SELECT 1 FROM _sqlx_migrations WHERE NOT success)",
            )
            .fetch_one(&self.pool),
        )
        .await
        .map_err(|_| AppError::Internal("database readiness timeout".into()))??
        .then_some(())
        .ok_or_else(|| AppError::Internal("required database migration is not applied".into()))?;
        Ok(())
    }
    async fn recall(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
        query: &str,
        query_embedding: Option<&Embedding>,
        intent: RecallIntent,
        limit: usize,
    ) -> Result<Vec<MemoryItem>, AppError> {
        if intent == RecallIntent::History {
            let rows = sqlx::query(
                r#"SELECT o.id,o.source_event_id,o.event_kind,o.observed_at,o.redacted_content,
                          o.raw_content_ref,o.ingested_at,s.scope_level
                   FROM observations o JOIN scopes s ON s.id=o.scope_id
                   WHERE o.tenant_id=$1 AND o.user_id=$2 AND o.consumer_id=$3
                     AND (s.project_key IS NULL OR s.project_key=$4)
                     AND (s.repository_key IS NULL OR s.repository_key=$5)
                     AND (s.thread_id IS NULL OR s.thread_id=$6)
                     AND (s.session_id IS NULL OR s.session_id=$7)
                     AND to_tsvector('english',o.redacted_content) @@ websearch_to_tsquery('english',$8)
                   ORDER BY s.specificity DESC,o.observed_at DESC,o.id
                   LIMIT $9"#,
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.consumer_id)
            .bind(&scope.project_key)
            .bind(&scope.repository_key)
            .bind(&scope.thread_id)
            .bind(&scope.session_id)
            .bind(query)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;
            return rows
                .into_iter()
                .map(|row| {
                    let observed_at = row.get("observed_at");
                    Ok(MemoryItem {
                        id: row.get("id"),
                        kind: MemoryKind::Observation,
                        subject: row.get("source_event_id"),
                        predicate: row.get("event_kind"),
                        object: Value::String(row.get("redacted_content")),
                        rendered: row.get("redacted_content"),
                        authority: Authority::RawHistory,
                        epistemic_status: EpistemicStatus::Uncertain,
                        scope_level: row.get("scope_level"),
                        source_type: "observation".into(),
                        source_ref: if row.get::<Option<String>, _>("raw_content_ref").is_some() {
                            "encrypted-raw-available".into()
                        } else {
                            "redacted-only".into()
                        },
                        observed_at: Some(observed_at),
                        valid_from: observed_at,
                        valid_to: None,
                        recorded_at: row.get("ingested_at"),
                        state: "history".into(),
                        retrieval_reasons: vec!["history lexical match".into()],
                    })
                })
                .collect();
        }
        let vector = query_embedding
            .map(|embedding| vector_literal(&embedding.values))
            .transpose()?;
        let embedding_model = query_embedding.map(|embedding| embedding.model.as_str());

        // Recall runs in one read-only transaction with two statements.
        //
        // Query A seeds approximate-nearest-neighbour candidates directly from
        // proposition_embeddings so the HNSW index is reachable. Query B applies
        // identity/scope/status filtering, precedence, RRF fusion and hydrates the
        // payload only for selected rows.
        //
        // Before this shape the semantic lane ranked over a materialized CTE, which
        // put the vector comparison beyond any base-table index: every recall
        // evaluated all embeddings, spilled ~43MB of temp blocks, and blew the 5s
        // statement_timeout under load. PostgreSQL reported that cancellation as
        // SQLSTATE 57014, which the error layer mislabelled as a retry-able
        // transaction conflict.
        let mut tx = self.pool.begin().await?;
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
            "SET LOCAL hnsw.iterative_scan='strict_order'",
            // Recall reads only. SET LOCAL keeps this scoped to this transaction:
            // the plain SET TRANSACTION form escaped onto pooled connections and
            // made unrelated writes fail with 25006.
            "SET LOCAL transaction_read_only=on",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }

        // RRF lane depth, unchanged from the previous query.
        let lane_target = limit.saturating_mul(8);
        // ANN over-fetch is a separate concern from lane depth: scope, consumer,
        // status and precedence rejection all happen after the seed, so no fixed
        // multiple of `limit` can guarantee a full semantic lane.
        let mut ann_seed = std::cmp::max(256, lane_target.saturating_mul(4));
        // Matches hnsw.max_scan_tuples; past this the lane is genuinely exhausted.
        const ANN_SEED_CAP: usize = 20_000;

        let mut seed_ids: Vec<Uuid> = Vec::new();
        let mut seed_distances: Vec<f64> = Vec::new();

        // Each statement has its own 5s timeout, but the retry ladder can run up
        // to eight A/B pairs. Without a deadline over the whole operation the
        // server can still be working long after interlock-mcp abandoned the call
        // at 20s, which is the failure mode this change exists to remove.
        let recall_work = async {
            let rows = loop {
                if let (Some(vector), Some(model)) = (vector.as_deref(), embedding_model) {
                    // The planner costs the sequential scan marginally cheaper than the
                    // HNSW scan on this corpus (2951 vs 2471..3141 at seed 160), so it
                    // picks the brute-force plan unless told otherwise. Scoped to this
                    // statement only: Query B still needs sequential scans.
                    sqlx::query("SET LOCAL enable_seqscan=off")
                        .execute(&mut *tx)
                        .await?;
                    let seed_rows = sqlx::query(
                        r#"SELECT pe.proposition_id AS id, pe.embedding <=> $3::vector AS distance
                       FROM proposition_embeddings pe
                       WHERE pe.embedding IS NOT NULL
                         AND pe.tenant_id=$1 AND pe.user_id=$2
                         AND pe.embedding_model=$4
                       ORDER BY pe.embedding <=> $3::vector
                       LIMIT $5"#,
                    )
                    .bind(identity.tenant_id)
                    .bind(identity.user_id)
                    .bind(vector)
                    .bind(model)
                    .bind(ann_seed as i64)
                    .fetch_all(&mut *tx)
                    .await?;
                    sqlx::query("SET LOCAL enable_seqscan=on")
                        .execute(&mut *tx)
                        .await?;

                    seed_ids = seed_rows.iter().map(|row| row.get("id")).collect();
                    seed_distances = seed_rows.iter().map(|row| row.get("distance")).collect();
                }

                let rows = sqlx::query(RECALL_SQL)
                    .bind(identity.tenant_id)
                    .bind(identity.user_id)
                    .bind(identity.consumer_id)
                    .bind(&scope.project_key)
                    .bind(&scope.repository_key)
                    .bind(&scope.thread_id)
                    .bind(&scope.session_id)
                    .bind(query)
                    .bind(limit as i64)
                    .bind(lane_target as i64)
                    .bind(&seed_ids)
                    .bind(&seed_distances)
                    .fetch_all(&mut *tx)
                    .await?;

                // Expansion must be driven by how full the *semantic lane* is, not by
                // the size of the final response: a lexical result that happens to fill
                // `limit` would otherwise mask a completely empty semantic lane and stop
                // expansion while eligible neighbours went unexamined.
                //
                // No selected rows means nothing survived either lane, so the semantic
                // lane is empty too — any surviving candidate would have produced one.
                let semantic_count: i64 = rows
                    .first()
                    .map(|row| row.get("semantic_count"))
                    .unwrap_or(0);
                // A short seed means the index ran out of neighbours, not that filtering
                // was aggressive; only an exhausted seed is worth re-running.
                let seed_exhausted = seed_ids.len() == ann_seed;
                if vector.is_some()
                    && (semantic_count as usize) < lane_target
                    && seed_exhausted
                    && ann_seed < ANN_SEED_CAP
                {
                    ann_seed = std::cmp::min(ann_seed.saturating_mul(2), ANN_SEED_CAP);
                    continue;
                }
                break rows;
            };
            Ok::<Vec<sqlx::postgres::PgRow>, AppError>(rows)
        };

        let rows = tokio::time::timeout(RECALL_DB_DEADLINE, recall_work)
            .await
            .map_err(|_| AppError::QueryTimeout)??;

        tx.commit().await?;
        rows.into_iter().map(row_to_item).collect()
    }

    async fn mandatory(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
    ) -> Result<Vec<MemoryItem>, AppError> {
        let sql = format!(
            "{ITEM_SELECT} AND pr.mandatory_bootstrap ORDER BY s.specificity DESC,(s.consumer_id IS NOT NULL) DESC,p.authority_rank,p.recorded_at,p.id LIMIT 1001"
        );
        let rows = sqlx::query(&sql)
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.consumer_id)
            .bind(&scope.project_key)
            .bind(&scope.repository_key)
            .bind(&scope.thread_id)
            .bind(&scope.session_id)
            .fetch_all(&self.pool)
            .await?;
        if rows.len() > 1000 {
            return Err(AppError::Conflict(
                "mandatory policy row cap exceeded".into(),
            ));
        }
        collapse_rows(rows)
    }

    async fn project_state(
        &self,
        identity: &Identity,
        scope: &ScopeSelector,
    ) -> Result<Vec<MemoryItem>, AppError> {
        let sql = format!(
            "{ITEM_SELECT} AND pr.key='project.state' ORDER BY s.specificity DESC,p.recorded_at DESC,p.id"
        );
        let rows = sqlx::query(&sql)
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(identity.consumer_id)
            .bind(&scope.project_key)
            .bind(&scope.repository_key)
            .bind(&scope.thread_id)
            .bind(&scope.session_id)
            .fetch_all(&self.pool)
            .await?;
        collapse_rows(rows)
    }

    async fn observe(
        &self,
        identity: &Identity,
        request: &ObservationWriteRequest,
    ) -> Result<Observation, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        validate_observation_request(request)?;
        let (redacted_content, redaction_count) = crate::redaction::redact(&request.content);
        let request_hash = request_hash(&serde_json::json!({
            "request_id": request.request_id,
            "source_event_id": request.source_event_id,
            "event_kind": request.event_kind,
            "scope": request.scope,
            "observed_at": request.observed_at,
            "redacted_content": redacted_content,
            "raw_content_ref": request.raw_content_ref,
        }))?;
        let mut tx = self.pool.begin().await?;
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "observation:{}:{}",
                identity.consumer_id, request.request_id
            ))
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "observation-source:{}:{}",
                identity.consumer_id, request.source_event_id
            ))
            .execute(&mut *tx)
            .await?;
        if let Some(row) = sqlx::query(
            r#"SELECT o.id,o.source_event_id,o.event_kind,o.observed_at,o.redacted_content,
                      o.redaction_count,o.ingested_at,o.request_hash,s.scope_level
               FROM observations o JOIN scopes s ON s.id=o.scope_id
               WHERE o.tenant_id=$1 AND o.user_id=$2 AND o.consumer_id=$3
                 AND (o.request_id=$4 OR o.source_event_id=$5)"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.request_id)
        .bind(&request.source_event_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            if row.get::<Vec<u8>, _>("request_hash") != request_hash {
                return Err(AppError::Conflict(
                    "idempotency key reused with a different request".into(),
                ));
            }
            return Ok(row_to_observation(&row));
        }
        let (scope_id, _) = self
            .scope_id(&mut tx, identity, &request.scope, false)
            .await?;
        let content_hash: [u8; 32] = Sha256::digest(redacted_content.as_bytes()).into();
        let row = sqlx::query(
            r#"INSERT INTO observations
               (tenant_id,user_id,consumer_id,request_id,request_hash,source_event_id,event_kind,
                actor,scope_id,observed_at,redacted_content,redaction_count,content_sha256,raw_content_ref)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
               RETURNING id,source_event_id,event_kind,observed_at,redacted_content,
                         redaction_count,ingested_at,(SELECT scope_level FROM scopes WHERE id=$9) AS scope_level"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.request_id)
        .bind(request_hash.as_slice())
        .bind(&request.source_event_id)
        .bind(&request.event_kind)
        .bind(&identity.actor)
        .bind(scope_id)
        .bind(request.observed_at)
        .bind(&redacted_content)
        .bind(redaction_count as i32)
        .bind(content_hash.as_slice())
        .bind(&request.raw_content_ref)
        .fetch_one(&mut *tx)
        .await?;
        let observation = row_to_observation(&row);
        sqlx::query(
            "INSERT INTO observation_outbox(tenant_id,user_id,consumer_id,observation_id,event_type,payload) VALUES($1,$2,$3,$4,'observation_ingested',jsonb_build_object('tenant_id',$1::text,'user_id',$2::text,'consumer_id',$3::text,'observation_id',$4::text))",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(observation.id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(observation)
    }

    async fn create_candidate(
        &self,
        identity: &Identity,
        request: &CandidateWriteRequest,
    ) -> Result<Candidate, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        validate_candidate_request(request)?;
        if request.authority_claim == Authority::OwnerInstruction && !identity.role.is_owner() {
            return Err(AppError::Forbidden);
        }
        let request_hash = request_hash(request)?;
        let mut tx = self.pool.begin().await?;
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "candidate-request:{}:{}",
                identity.consumer_id, request.request_id
            ))
            .execute(&mut *tx)
            .await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "candidate:{}:{}",
                identity.consumer_id, request.derivation_key
            ))
            .execute(&mut *tx)
            .await?;
        let existing = sqlx::query(
            r#"SELECT id,derivation_key,subject_key,predicate_key,object_value,authority_claim,
                      epistemic_status,confidence,state,canonical_proposition_id,created_at,request_hash
               FROM candidates WHERE tenant_id=$1 AND user_id=$2 AND consumer_id=$3
                 AND (request_id=$4 OR derivation_key=$5)"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.request_id)
        .bind(&request.derivation_key)
        .fetch_all(&mut *tx)
        .await?;
        if existing.len() > 1 {
            return Err(AppError::Conflict(
                "request and derivation keys refer to different candidates".into(),
            ));
        }
        if let Some(row) = existing.first() {
            if row.get::<Vec<u8>, _>("request_hash") != request_hash {
                return Err(AppError::Conflict(
                    "candidate idempotency or derivation key reused with different content".into(),
                ));
            }
            return row_to_candidate(row);
        }
        let evidence_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 AND user_id=$2 AND consumer_id=$3 AND id=ANY($4)",
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(&request.evidence_observation_ids)
        .fetch_one(&mut *tx)
        .await?;
        if evidence_count != request.evidence_observation_ids.len() as i64 {
            return Err(AppError::Invalid(
                "all candidate evidence must exist inside the authenticated tenant and user".into(),
            ));
        }
        let (scope_id, _) = self
            .scope_id(&mut tx, identity, &request.scope, false)
            .await?;
        let row = sqlx::query(
            r#"INSERT INTO candidates
               (tenant_id,user_id,consumer_id,request_id,request_hash,derivation_key,scope_id,
                subject_key,predicate_key,object_value,authority_claim,epistemic_status,confidence,
                extractor_model,extractor_version,prompt_version)
               VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
               RETURNING id,derivation_key,subject_key,predicate_key,object_value,authority_claim,
                         epistemic_status,confidence,state,canonical_proposition_id,created_at"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.request_id)
        .bind(request_hash.as_slice())
        .bind(&request.derivation_key)
        .bind(scope_id)
        .bind(&request.subject)
        .bind(&request.predicate)
        .bind(&request.object)
        .bind(enum_text(request.authority_claim)?)
        .bind(enum_text(request.epistemic_status)?)
        .bind(request.confidence)
        .bind(&request.extractor_model)
        .bind(&request.extractor_version)
        .bind(&request.prompt_version)
        .fetch_one(&mut *tx)
        .await?;
        let candidate_id: Uuid = row.get("id");
        for observation_id in &request.evidence_observation_ids {
            sqlx::query("INSERT INTO candidate_evidence(tenant_id,user_id,consumer_id,candidate_id,observation_id) VALUES($1,$2,$3,$4,$5)")
                .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(candidate_id).bind(observation_id)
                .execute(&mut *tx).await?;
        }
        let candidate = row_to_candidate(&row)?;
        tx.commit().await?;
        Ok(candidate)
    }

    async fn promote_candidate(
        &self,
        identity: &Identity,
        request: &CandidatePromotionRequest,
    ) -> Result<WriteResponse, AppError> {
        if !matches!(identity.role, TokenRole::Verifier | TokenRole::Owner) {
            return Err(AppError::Forbidden);
        }
        if request.reason.trim().is_empty() || request.reason.len() > 4096 {
            return Err(AppError::Invalid(
                "promotion reason must be 1..4096 bytes".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        Self::configure_write_transaction(&mut tx).await?;
        let row = sqlx::query(
            r#"SELECT c.consumer_id,c.subject_key,c.predicate_key,c.object_value,c.authority_claim,
                      c.epistemic_status,c.state,c.canonical_proposition_id,
                      s.project_key,s.repository_key,s.thread_id,s.session_id
               FROM candidates c JOIN scopes s ON s.id=c.scope_id
               WHERE c.tenant_id=$1 AND c.user_id=$2 AND c.consumer_id=$3 AND c.id=$4 FOR UPDATE OF c"#,
        )
        .bind(identity.tenant_id)
        .bind(identity.user_id)
        .bind(identity.consumer_id)
        .bind(request.candidate_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AppError::NotFound)?;
        let state: &str = row.get("state");
        if !matches!(state, "pending" | "accepted") {
            return Err(AppError::Conflict(
                "candidate has already left the pending state".into(),
            ));
        }
        let canonical_request = MemoryWriteRequest {
            request_id: request.request_id,
            scope: ScopeSelector {
                project_key: row.get("project_key"),
                repository_key: row.get("repository_key"),
                thread_id: row.get("thread_id"),
                session_id: row.get("session_id"),
            },
            subject: row.get("subject_key"),
            predicate: row.get("predicate_key"),
            object: row.get("object_value"),
            authority: request.authority,
            epistemic_status: epistemic_from_db(row.get("epistemic_status"))?,
            source_type: "candidate_promotion".into(),
            source_ref: format!("candidate:{}", request.candidate_id),
            reason: request.reason.clone(),
        };
        let response = self
            .canonical_in_tx(&mut tx, identity, &canonical_request)
            .await?;
        if state == "pending" {
            sqlx::query(
                "UPDATE candidates SET state='accepted',transition_mutation_id=$1,canonical_proposition_id=$2,reviewed_at=clock_timestamp(),reviewed_by=$3 WHERE tenant_id=$4 AND user_id=$5 AND id=$6 AND state='pending'",
            )
            .bind(request.request_id)
            .bind(response.id)
            .bind(&identity.actor)
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(request.candidate_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO candidate_events(tenant_id,user_id,candidate_id,mutation_id,actor,event_type,reason,proposition_id) VALUES($1,$2,$3,$4,$5,'accepted',$6,$7)",
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .bind(request.candidate_id)
            .bind(request.request_id)
            .bind(&identity.actor)
            .bind(&request.reason)
            .bind(response.id)
            .execute(&mut *tx)
            .await?;
        } else if row.get::<Option<Uuid>, _>("canonical_proposition_id") != Some(response.id) {
            return Err(AppError::Conflict(
                "accepted candidate does not match idempotent promotion response".into(),
            ));
        }
        tx.commit().await?;
        Ok(response)
    }

    async fn remember(
        &self,
        identity: &Identity,
        request: &MemoryWriteRequest,
    ) -> Result<WriteResponse, AppError> {
        let mut tx = self.pool.begin().await?;
        Self::configure_write_transaction(&mut tx).await?;
        let response = self.canonical_in_tx(&mut tx, identity, request).await?;
        tx.commit().await?;
        Ok(response)
    }

    async fn write_handoff(
        &self,
        identity: &Identity,
        request: &HandoffWriteRequest,
    ) -> Result<Handoff, AppError> {
        if !identity.role.can_write() {
            return Err(AppError::Forbidden);
        }
        if request.project_key.trim().is_empty()
            || request.project_key.len() > 512
            || request.content.trim().is_empty()
            || request.content.len() > 64 * 1024
            || request.session_id.trim().is_empty()
            || request.session_id.len() > 256
        {
            return Err(AppError::Invalid(
                "handoff fields are required and bounded".into(),
            ));
        }
        if crate::redaction::contains_sensitive_text(&request.content)
            || crate::redaction::contains_sensitive_text(&request.project_key)
            || crate::redaction::contains_sensitive_text(&request.session_id)
        {
            return Err(AppError::Invalid(
                "handoff fields cannot contain sensitive data".into(),
            ));
        }
        if let Some(expires_at) = request.expires_at {
            let now = Utc::now();
            if expires_at <= now || expires_at > now + Duration::days(7) {
                return Err(AppError::Invalid(
                    "handoff expiry must be within the next 7 days".into(),
                ));
            }
        }
        let request_hash = request_hash(request)?;
        let expires = request
            .expires_at
            .unwrap_or_else(|| Utc::now() + Duration::hours(48));
        let mut tx = self.pool.begin().await?;
        for statement in [
            "SET LOCAL lock_timeout='2s'",
            "SET LOCAL statement_timeout='5s'",
            "SET LOCAL idle_in_transaction_session_timeout='10s'",
        ] {
            sqlx::query(statement).execute(&mut *tx).await?;
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!(
                "handoff:{}:{}",
                identity.consumer_id, request.request_id
            ))
            .execute(&mut *tx)
            .await?;
        if let Some(row) = sqlx::query("SELECT id,project_key,content,session_id,created_at,expires_at,request_hash FROM handoffs WHERE tenant_id=$1 AND user_id=$2 AND consumer_id=$3 AND request_id=$4")
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(request.request_id).fetch_optional(&mut *tx).await?
        {
            if row.get::<Vec<u8>, _>("request_hash") != request_hash { return Err(AppError::Conflict("idempotency key reused with a different request".into())); }
            return Ok(Handoff { id: row.get("id"), project_key: row.get("project_key"), content: row.get("content"), session_id: row.get("session_id"), created_at: row.get("created_at"), expires_at: row.get("expires_at") });
        }
        let row = sqlx::query("INSERT INTO handoffs(tenant_id,user_id,consumer_id,project_key,content,session_id,expires_at,request_id,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id,created_at")
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(&request.project_key)
            .bind(&request.content).bind(&request.session_id).bind(expires).bind(request.request_id).bind(request_hash.as_slice()).fetch_one(&mut *tx).await?;
        sqlx::query("INSERT INTO snapshot_revisions(tenant_id,user_id,revision) VALUES($1,$2,1) ON CONFLICT(tenant_id,user_id) DO UPDATE SET revision=snapshot_revisions.revision+1")
            .bind(identity.tenant_id).bind(identity.user_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(Handoff {
            id: row.get("id"),
            project_key: request.project_key.clone(),
            content: request.content.clone(),
            session_id: request.session_id.clone(),
            created_at: row.get("created_at"),
            expires_at: expires,
        })
    }

    async fn latest_handoff(
        &self,
        identity: &Identity,
        project_key: &str,
    ) -> Result<Option<Handoff>, AppError> {
        let row = sqlx::query("SELECT id,project_key,content,session_id,created_at,expires_at FROM handoffs WHERE tenant_id=$1 AND user_id=$2 AND consumer_id=$3 AND project_key=$4 AND expires_at>clock_timestamp() ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(identity.tenant_id).bind(identity.user_id).bind(identity.consumer_id).bind(project_key).fetch_optional(&self.pool).await?;
        Ok(row.map(|row| Handoff {
            id: row.get("id"),
            project_key: row.get("project_key"),
            content: row.get("content"),
            session_id: row.get("session_id"),
            created_at: row.get("created_at"),
            expires_at: row.get("expires_at"),
        }))
    }

    async fn snapshot_revision(&self, identity: &Identity) -> Result<i64, AppError> {
        Ok(
            sqlx::query(
                "SELECT revision FROM snapshot_revisions WHERE tenant_id=$1 AND user_id=$2",
            )
            .bind(identity.tenant_id)
            .bind(identity.user_id)
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get("revision"))
            .unwrap_or(0),
        )
    }
}
