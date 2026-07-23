use crate::{
    auth::{AuthConfig, Identity, require_auth},
    domain::{
        BootstrapRequest, BootstrapResponse, CandidatePromotionRequest, CandidateWriteRequest,
        HandoffWriteRequest, MemoryWriteRequest, ObservationWriteRequest, RecallIntent,
        RecallRequest, RecallResponse, RetrievalMode,
    },
    embedding::EmbeddingProvider,
    error::AppError,
    store::MemoryStore,
};
use axum::{
    Extension, Json, Router,
    extract::{DefaultBodyLimit, State},
    middleware,
    routing::{get, post},
};
use chrono::Utc;
use serde::Serialize;
use std::{collections::HashSet, sync::Arc};
use tiktoken_rs::CoreBPE;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn MemoryStore>,
    pub auth: AuthConfig,
    tokenizer: Arc<CoreBPE>,
    embedder: Option<Arc<dyn EmbeddingProvider>>,
}

impl AppState {
    pub fn new(store: Arc<dyn MemoryStore>, auth: AuthConfig) -> Result<Self, AppError> {
        let tokenizer =
            tiktoken_rs::cl100k_base().map_err(|error| AppError::Internal(error.to_string()))?;
        Ok(Self {
            store,
            auth,
            tokenizer: Arc::new(tokenizer),
            embedder: None,
        })
    }

    pub fn with_embedder(mut self, embedder: Arc<dyn EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }
}

pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v6/bootstrap", post(bootstrap))
        .route("/v6/recall", post(recall))
        .route("/v6/memories", post(remember))
        .route("/v6/observations", post(observe))
        .route("/v6/candidates", post(create_candidate))
        .route("/v6/candidates/promote", post(promote_candidate))
        .route("/v6/handoffs", post(write_handoff))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .route_layer(middleware::from_fn_with_state(
            state.auth.clone(),
            require_auth,
        ));
    Router::new()
        .route("/v6/health", get(readiness))
        .route("/v6/health/live", get(liveness))
        .merge(protected)
        .with_state(state)
}

async fn liveness() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status":"ok","version":env!("CARGO_PKG_VERSION")}))
}

async fn readiness(State(state): State<AppState>) -> Result<Json<serde_json::Value>, AppError> {
    state.store.ready().await?;
    Ok(Json(
        serde_json::json!({"status":"ok","database":true,"version":env!("CARGO_PKG_VERSION")}),
    ))
}

fn validate_budget(budget: usize) -> Result<(), AppError> {
    if !(64..=32_768).contains(&budget) {
        return Err(AppError::Invalid(
            "token_budget must be between 64 and 32768".into(),
        ));
    }
    Ok(())
}

fn token_count<T: Serialize>(tokenizer: &CoreBPE, value: &T) -> Result<usize, AppError> {
    let json =
        serde_json::to_string(value).map_err(|error| AppError::Internal(error.to_string()))?;
    Ok(tokenizer.encode_with_special_tokens(&json).len())
}

fn settle_recall_count(
    tokenizer: &CoreBPE,
    response: &mut RecallResponse,
) -> Result<usize, AppError> {
    for _ in 0..8 {
        let count = token_count(tokenizer, response)?;
        if response.token_count == count {
            return Ok(count);
        }
        response.token_count = count;
    }
    token_count(tokenizer, response)
}

fn settle_bootstrap_count(
    tokenizer: &CoreBPE,
    response: &mut BootstrapResponse,
) -> Result<usize, AppError> {
    for _ in 0..8 {
        let count = token_count(tokenizer, response)?;
        if response.token_count == count {
            return Ok(count);
        }
        response.token_count = count;
    }
    token_count(tokenizer, response)
}

async fn recall(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<RecallRequest>,
) -> Result<Json<RecallResponse>, AppError> {
    validate_budget(request.token_budget)?;
    request.scope.validate().map_err(AppError::Invalid)?;
    let query = request.query.trim();
    if query.is_empty() || query.len() > 4096 {
        return Err(AppError::Invalid("query must be 1..4096 bytes".into()));
    }
    if !(1..=100).contains(&request.limit) {
        return Err(AppError::Invalid("limit must be 1..100".into()));
    }
    let mut mandatory = state.store.mandatory(&identity, &request.scope).await?;
    let mut mandatory_ids = HashSet::new();
    mandatory.retain(|item| mandatory_ids.insert(item.id));
    let (embedding, degraded_reason) = if request.intent == RecallIntent::History {
        (None, None)
    } else if let Some(embedder) = &state.embedder {
        let (safe_query, _) = crate::redaction::redact(query);
        match embedder.embed(&safe_query).await {
            Ok(embedding) => (Some(embedding), None),
            Err(error) => {
                tracing::warn!(%error, "semantic recall degraded to lexical retrieval");
                (None, Some("query_embedding_unavailable".into()))
            }
        }
    } else {
        (None, None)
    };
    let items = state
        .store
        .recall(
            &identity,
            &request.scope,
            query,
            embedding.as_ref(),
            request.intent,
            request.limit,
        )
        .await?
        .into_iter()
        .filter(|item| !mandatory_ids.contains(&item.id))
        .collect();
    let mut response = RecallResponse {
        intent: request.intent,
        retrieval_mode: if embedding.is_some() {
            RetrievalMode::Hybrid
        } else {
            RetrievalMode::LexicalOnly
        },
        embedding_model: embedding.as_ref().map(|value| value.model.clone()),
        degraded_reason,
        mandatory_policy: mandatory,
        items,
        token_count: 0,
        token_budget: request.token_budget,
        snapshot_revision: state.store.snapshot_revision(&identity).await?,
    };
    while settle_recall_count(&state.tokenizer, &mut response)? > request.token_budget {
        if response.items.is_empty() {
            let minimum = settle_recall_count(&state.tokenizer, &mut response)?;
            return Err(AppError::BudgetTooSmall { minimum });
        }
        response.items.pop();
    }
    Ok(Json(response))
}

async fn bootstrap(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<BootstrapRequest>,
) -> Result<Json<BootstrapResponse>, AppError> {
    validate_budget(request.token_budget)?;
    request.scope.validate().map_err(AppError::Invalid)?;
    let mut directives = state.store.mandatory(&identity, &request.scope).await?;
    let mut directive_ids = HashSet::new();
    directives.retain(|item| directive_ids.insert(item.id));
    let project_key = request.scope.project_key.as_deref();
    let mut response = BootstrapResponse {
        directives,
        project_state: state.store.project_state(&identity, &request.scope).await?,
        handoff: match project_key {
            Some(key) => state.store.latest_handoff(&identity, key).await?,
            None => None,
        },
        token_count: 0,
        token_budget: request.token_budget,
        snapshot_revision: state.store.snapshot_revision(&identity).await?,
        generated_at: Utc::now(),
    };
    let directives_only = BootstrapResponse {
        directives: response.directives.clone(),
        project_state: Vec::new(),
        handoff: None,
        token_count: 0,
        token_budget: request.token_budget,
        snapshot_revision: response.snapshot_revision,
        generated_at: response.generated_at,
    };
    let mut minimum_packet = directives_only;
    let minimum = settle_bootstrap_count(&state.tokenizer, &mut minimum_packet)?;
    if minimum > request.token_budget {
        return Err(AppError::BudgetTooSmall { minimum });
    }
    while settle_bootstrap_count(&state.tokenizer, &mut response)? > request.token_budget {
        if response.project_state.pop().is_none() {
            response.handoff = None;
            break;
        }
    }
    settle_bootstrap_count(&state.tokenizer, &mut response)?;
    Ok(Json(response))
}

async fn remember(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<MemoryWriteRequest>,
) -> Result<Json<crate::domain::WriteResponse>, AppError> {
    if !identity.role.can_write() {
        return Err(AppError::Forbidden);
    }
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
            "subject, predicate, and reason are required".into(),
        ));
    }
    let object_bytes = serde_json::to_vec(&request.object)
        .map_err(|error| AppError::Invalid(error.to_string()))?
        .len();
    if object_bytes > 64 * 1024 {
        return Err(AppError::Invalid("object exceeds 64 KiB".into()));
    }
    let policy_write = matches!(
        request.predicate.as_str(),
        "system.constraint" | "system.directive"
    );
    if policy_write && !identity.role.is_owner() {
        return Err(AppError::Forbidden);
    }
    if request.authority == crate::domain::Authority::OwnerInstruction && !identity.role.is_owner()
    {
        return Err(AppError::Forbidden);
    }
    if identity.role == crate::auth::TokenRole::Writer
        && !matches!(
            request.authority,
            crate::domain::Authority::TrustedAgentReport | crate::domain::Authority::Inference
        )
    {
        return Err(AppError::Forbidden);
    }
    Ok(Json(state.store.remember(&identity, &request).await?))
}

async fn observe(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<ObservationWriteRequest>,
) -> Result<Json<crate::domain::Observation>, AppError> {
    if !identity.role.can_write() {
        return Err(AppError::Forbidden);
    }
    request.scope.validate().map_err(AppError::Invalid)?;
    if request.source_event_id.trim().is_empty()
        || request.source_event_id.len() > 512
        || request.event_kind.trim().is_empty()
        || request.event_kind.len() > 128
        || request.content.trim().is_empty()
        || request.content.len() > 96 * 1024
        || request.raw_content_ref.as_ref().is_some_and(|value| {
            value.strip_prefix("encrypted:").is_none_or(|identifier| {
                identifier.is_empty()
                    || identifier.len() > 128
                    || !identifier
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            })
        })
    {
        return Err(AppError::Invalid(
            "observation identifiers and content are required and bounded".into(),
        ));
    }
    let now = Utc::now();
    if request.observed_at > now + chrono::Duration::minutes(5)
        || request.observed_at < now - chrono::Duration::days(3650)
    {
        return Err(AppError::Invalid(
            "observed_at is outside the accepted time window".into(),
        ));
    }
    Ok(Json(state.store.observe(&identity, &request).await?))
}

async fn create_candidate(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<CandidateWriteRequest>,
) -> Result<Json<crate::domain::Candidate>, AppError> {
    if !identity.role.can_write() {
        return Err(AppError::Forbidden);
    }
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
    if request
        .evidence_observation_ids
        .iter()
        .collect::<HashSet<_>>()
        .len()
        != request.evidence_observation_ids.len()
    {
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
    if request.authority_claim == crate::domain::Authority::OwnerInstruction
        && !identity.role.is_owner()
    {
        return Err(AppError::Forbidden);
    }
    Ok(Json(
        state.store.create_candidate(&identity, &request).await?,
    ))
}

async fn promote_candidate(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<CandidatePromotionRequest>,
) -> Result<Json<crate::domain::WriteResponse>, AppError> {
    if !matches!(
        identity.role,
        crate::auth::TokenRole::Verifier | crate::auth::TokenRole::Owner
    ) {
        return Err(AppError::Forbidden);
    }
    if request.reason.trim().is_empty() || request.reason.len() > 4096 {
        return Err(AppError::Invalid(
            "promotion reason must be 1..4096 bytes".into(),
        ));
    }
    Ok(Json(
        state.store.promote_candidate(&identity, &request).await?,
    ))
}

async fn write_handoff(
    State(state): State<AppState>,
    Extension(identity): Extension<Identity>,
    Json(request): Json<HandoffWriteRequest>,
) -> Result<Json<crate::domain::Handoff>, AppError> {
    if !identity.role.can_write() {
        return Err(AppError::Forbidden);
    }
    if request.project_key.trim().is_empty()
        || request.project_key.len() > 512
        || request.content.trim().is_empty()
        || request.session_id.trim().is_empty()
        || request.session_id.len() > 256
    {
        return Err(AppError::Invalid(
            "project_key, content, and session_id are required".into(),
        ));
    }
    if request.content.len() > 64 * 1024 {
        return Err(AppError::Invalid("handoff exceeds 64 KiB".into()));
    }
    if let Some(expires_at) = request.expires_at {
        let now = Utc::now();
        if expires_at <= now || expires_at > now + chrono::Duration::days(7) {
            return Err(AppError::Invalid(
                "handoff expiry must be within the next 7 days".into(),
            ));
        }
    }
    Ok(Json(state.store.write_handoff(&identity, &request).await?))
}
