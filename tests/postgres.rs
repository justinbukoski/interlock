use async_trait::async_trait;
use interlock::{
    Identity, MemoryStore, PgMemoryStore, TokenRole,
    domain::{
        Authority, CandidatePromotionRequest, CandidateState, CandidateWriteRequest,
        EpistemicStatus, HandoffWriteRequest, MemoryWriteRequest, ObservationWriteRequest,
        RecallIntent, ScopeSelector,
    },
    embedding::{Embedding, EmbeddingProvider},
    error::AppError,
};
use serde_json::json;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use uuid::Uuid;

const MIGRATION_0001: &str = include_str!("../migrations/0001_initial.sql");
const MIGRATION_0002: &str = include_str!("../migrations/0002_observation_candidates.sql");
const MIGRATION_0003: &str = include_str!("../migrations/0003_hybrid_retrieval.sql");
const MIGRATION_0004: &str = include_str!("../migrations/0004_embedding_leases.sql");
const MIGRATION_0005: &str = include_str!("../migrations/0005_legacy_import_predicates.sql");

struct StaticEmbedder;
struct FailingEmbedder;

#[async_trait]
impl EmbeddingProvider for StaticEmbedder {
    async fn embed(&self, _: &str) -> Result<Embedding, AppError> {
        let mut values = vec![0.0; 1024];
        values[0] = 1.0;
        Ok(Embedding {
            values,
            model: "test-bge".into(),
        })
    }
}

#[async_trait]
impl EmbeddingProvider for FailingEmbedder {
    async fn embed(&self, _: &str) -> Result<Embedding, AppError> {
        Err(AppError::Internal("simulated provider failure".into()))
    }
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector 0.8.2+"]
async fn derived_embeddings_enable_semantic_only_recall() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();
    let owner = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let scope = ScopeSelector {
        project_key: Some(format!("git:test/vector-{}", Uuid::new_v4())),
        ..Default::default()
    };
    store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                scope.clone(),
                "semantic needle",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    store
        .observe(
            &owner,
            &ObservationWriteRequest {
                request_id: Uuid::new_v4(),
                source_event_id: format!("mixed-backlog-{}", Uuid::new_v4()),
                event_kind: "turn".into(),
                scope: scope.clone(),
                observed_at: chrono::Utc::now(),
                content: "observation lane must make progress".into(),
                raw_content_ref: None,
            },
        )
        .await
        .unwrap();
    let first_store = store.clone();
    let second_store = store.clone();
    let (first, second) = tokio::join!(
        first_store.embed_pending(&StaticEmbedder, "test-bge", 32, Uuid::new_v4()),
        second_store.embed_pending(&StaticEmbedder, "test-bge", 32, Uuid::new_v4())
    );
    assert_eq!(
        first.unwrap() + second.unwrap(),
        2,
        "concurrent workers must exclusively claim both source kinds"
    );
    let query_embedding = StaticEmbedder.embed("unrelated vocabulary").await.unwrap();
    let recalled = store
        .recall(
            &owner,
            &scope,
            "lexically-absent-zzyzx",
            Some(&query_embedding),
            RecallIntent::Current,
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        recalled.items.len(),
        1,
        "semantic lane must retrieve without a lexical match"
    );
    assert_eq!(recalled.items[0].object, json!({"value":"semantic needle"}));
    assert_eq!(count(&pool, "SELECT count(*) FROM proposition_embeddings WHERE tenant_id=$1 AND embedding IS NOT NULL AND embedding_model='test-bge'", owner.tenant_id).await, 1);
    assert_eq!(count(&pool, "SELECT count(*) FROM observation_embeddings WHERE tenant_id=$1 AND embedding IS NOT NULL AND embedding_model='test-bge'", owner.tenant_id).await, 1);

    let failed = store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                ScopeSelector {
                    project_key: Some(format!("git:test/failure-{}", Uuid::new_v4())),
                    ..Default::default()
                },
                "poison row",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    for _ in 0..10 {
        sqlx::query("UPDATE proposition_embeddings SET next_attempt_at=clock_timestamp() WHERE proposition_id=$1")
            .bind(failed.id).execute(&pool).await.unwrap();
        store
            .embed_pending(&FailingEmbedder, "test-bge", 1, Uuid::new_v4())
            .await
            .unwrap();
    }
    let failure = sqlx::query("SELECT attempts,last_error,quarantined_at IS NOT NULL AS quarantined,lease_owner IS NULL AS released FROM proposition_embeddings WHERE proposition_id=$1")
        .bind(failed.id).fetch_one(&pool).await.unwrap();
    assert_eq!(failure.get::<i32, _>("attempts"), 10);
    assert_eq!(failure.get::<String, _>("last_error"), "provider_error");
    assert!(failure.get::<bool, _>("quarantined"));
    assert!(failure.get::<bool, _>("released"));

    let reclaimed = store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                ScopeSelector {
                    project_key: Some(format!("git:test/reclaim-{}", Uuid::new_v4())),
                    ..Default::default()
                },
                "expired lease",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    let stale_owner = Uuid::new_v4();
    sqlx::query("UPDATE proposition_embeddings SET lease_owner=$2,lease_until=clock_timestamp()-interval '1 second' WHERE proposition_id=$1")
        .bind(reclaimed.id).bind(stale_owner).execute(&pool).await.unwrap();
    assert_eq!(
        store
            .embed_pending(&StaticEmbedder, "test-bge", 1, Uuid::new_v4())
            .await
            .unwrap(),
        1
    );
    let stale_write = sqlx::query("UPDATE proposition_embeddings SET last_error='stale-writer' WHERE proposition_id=$1 AND lease_owner=$2")
        .bind(reclaimed.id).bind(stale_owner).execute(&pool).await.unwrap().rows_affected();
    assert_eq!(
        stale_write, 0,
        "expired lease owner must not persist after takeover"
    );

    let releasable = store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                ScopeSelector {
                    project_key: Some(format!("git:test/release-{}", Uuid::new_v4())),
                    ..Default::default()
                },
                "shutdown lease",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    let shutdown_worker = Uuid::new_v4();
    sqlx::query("UPDATE proposition_embeddings SET lease_owner=$2,lease_until=clock_timestamp()+interval '2 minutes' WHERE proposition_id=$1")
        .bind(releasable.id).bind(shutdown_worker).execute(&pool).await.unwrap();
    store
        .release_embedding_leases(shutdown_worker)
        .await
        .unwrap();
    let released: bool = sqlx::query_scalar("SELECT lease_owner IS NULL AND lease_until IS NULL FROM proposition_embeddings WHERE proposition_id=$1")
        .bind(releasable.id).fetch_one(&pool).await.unwrap();
    assert!(
        released,
        "shutdown cleanup must release the worker's leases"
    );
}

fn identity(tenant_id: Uuid, user_id: Uuid, consumer_id: Uuid) -> Identity {
    Identity {
        tenant_id,
        user_id,
        consumer_id,
        actor: "postgres-test".into(),
        role: TokenRole::Owner,
    }
}

fn write(
    request_id: Uuid,
    scope: ScopeSelector,
    value: &str,
    authority: Authority,
) -> MemoryWriteRequest {
    MemoryWriteRequest {
        request_id,
        scope,
        subject: "interlock-v6".into(),
        predicate: "project.state".into(),
        object: json!({"value":value}),
        authority,
        epistemic_status: EpistemicStatus::Verified,
        source_type: "integration_test".into(),
        source_ref: format!("test:{request_id}"),
        reason: "prove structural invariants".into(),
    }
}

async fn count(pool: &PgPool, sql: &str, tenant: Uuid) -> i64 {
    sqlx::query(sql)
        .bind(tenant)
        .fetch_one(pool)
        .await
        .unwrap()
        .get("count")
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL 15+"]
async fn postgres_migration_supersession_scope_and_lane_invariants() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();

    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let codex = identity(tenant, user, Uuid::new_v4());
    let lumi = identity(tenant, user, Uuid::new_v4());
    let project_scope = ScopeSelector {
        project_key: Some("git:test/repo".into()),
        ..Default::default()
    };

    let observation_scope = ScopeSelector {
        project_key: Some("git:test/observed".into()),
        thread_id: Some("thread-observe".into()),
        session_id: Some("session-observe".into()),
        ..Default::default()
    };
    let observation_request = ObservationWriteRequest {
        request_id: Uuid::new_v4(),
        source_event_id: "codex-turn-1".into(),
        event_kind: "user_turn".into(),
        scope: observation_scope.clone(),
        observed_at: chrono::Utc::now(),
        content: "deploy status password=hunter2x user@example.com".into(),
        raw_content_ref: Some("encrypted:test_turn-1".into()),
    };
    let observation = store.observe(&codex, &observation_request).await.unwrap();
    assert_eq!(observation.redaction_count, 2);
    assert!(!observation.redacted_content.contains("hunter2x"));
    assert!(!observation.redacted_content.contains("user@example.com"));
    let observation_replay = store.observe(&codex, &observation_request).await.unwrap();
    assert_eq!(observation.id, observation_replay.id);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM observation_outbox WHERE tenant_id=$1 AND event_type='observation_ingested'",
            tenant,
        )
        .await,
        1
    );
    let second_user = Identity {
        user_id: Uuid::new_v4(),
        ..codex.clone()
    };
    let mut second_user_request = observation_request.clone();
    second_user_request.request_id = Uuid::new_v4();
    assert!(
        store
            .observe(&second_user, &second_user_request)
            .await
            .is_ok(),
        "source event uniqueness must include user identity"
    );
    let mut mismatched_observation = observation_request.clone();
    mismatched_observation.content = "different content".into();
    assert!(matches!(
        store.observe(&codex, &mismatched_observation).await,
        Err(AppError::Conflict(_))
    ));
    let history = store
        .recall(
            &codex,
            &observation_scope,
            "deploy status",
            None,
            RecallIntent::History,
            10,
        )
        .await
        .unwrap();
    let history = history.items;
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].authority, Authority::RawHistory);
    assert_eq!(history[0].state, "history");
    assert!(!history[0].rendered.contains("hunter2x"));

    let extractor = Identity {
        role: TokenRole::Writer,
        ..codex.clone()
    };
    let candidate_request = CandidateWriteRequest {
        request_id: Uuid::new_v4(),
        derivation_key: "extractor-v1:codex-turn-1:project-state".into(),
        scope: observation_scope.clone(),
        subject: "observed-project".into(),
        predicate: "project.state".into(),
        object: json!({"value":"candidate-state"}),
        authority_claim: Authority::RepositoryState,
        epistemic_status: EpistemicStatus::Verified,
        confidence: 0.98,
        extractor_model: "deepseek-v4-flash".into(),
        extractor_version: "extractor-v1".into(),
        prompt_version: "prompt-v1".into(),
        evidence_observation_ids: vec![observation.id],
    };
    let candidate = store
        .create_candidate(&extractor, &candidate_request)
        .await
        .unwrap();
    assert_eq!(candidate.state, CandidateState::Pending);
    let mut cross_consumer_candidate = candidate_request.clone();
    cross_consumer_candidate.request_id = Uuid::new_v4();
    cross_consumer_candidate.derivation_key = "cross-consumer-evidence".into();
    assert!(matches!(
        store
            .create_candidate(&lumi, &cross_consumer_candidate)
            .await,
        Err(AppError::Invalid(_))
    ));
    let mut sensitive_candidate = candidate_request.clone();
    sensitive_candidate.request_id = Uuid::new_v4();
    sensitive_candidate.derivation_key = "sensitive-candidate".into();
    sensitive_candidate.object = json!({"nested":{"password":"hunter2x"}});
    assert!(matches!(
        store
            .create_candidate(&extractor, &sensitive_candidate)
            .await,
        Err(AppError::Invalid(_))
    ));
    let mut sensitive_subject_candidate = candidate_request.clone();
    sensitive_subject_candidate.request_id = Uuid::new_v4();
    sensitive_subject_candidate.derivation_key = "sensitive-subject-candidate".into();
    sensitive_subject_candidate.subject = "user@example.com".into();
    assert!(matches!(
        store
            .create_candidate(&extractor, &sensitive_subject_candidate)
            .await,
        Err(AppError::Invalid(_))
    ));
    let candidate_replay = store
        .create_candidate(&extractor, &candidate_request)
        .await
        .unwrap();
    assert_eq!(candidate.id, candidate_replay.id);
    let mut mismatched_candidate = candidate_request.clone();
    mismatched_candidate.object = json!({"value":"different"});
    assert!(matches!(
        store
            .create_candidate(&extractor, &mismatched_candidate)
            .await,
        Err(AppError::Conflict(_))
    ));
    let cross_tenant_extractor = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let mut cross_tenant_candidate = candidate_request.clone();
    cross_tenant_candidate.request_id = Uuid::new_v4();
    cross_tenant_candidate.derivation_key = "cross-tenant-evidence".into();
    assert!(matches!(
        store
            .create_candidate(&cross_tenant_extractor, &cross_tenant_candidate)
            .await,
        Err(AppError::Invalid(_))
    ));
    assert!(matches!(
        store
            .promote_candidate(
                &extractor,
                &CandidatePromotionRequest {
                    request_id: Uuid::new_v4(),
                    candidate_id: candidate.id,
                    authority: Authority::RepositoryState,
                    reason: "writers cannot promote their own extraction".into(),
                },
            )
            .await,
        Err(AppError::Forbidden)
    ));
    let verifier = Identity {
        role: TokenRole::Verifier,
        actor: "postgres-verifier".into(),
        ..codex.clone()
    };
    let promotion_request = CandidatePromotionRequest {
        request_id: Uuid::new_v4(),
        candidate_id: candidate.id,
        authority: Authority::RepositoryState,
        reason: "mechanically verified against repository state".into(),
    };
    let cross_consumer_verifier = Identity {
        role: TokenRole::Verifier,
        actor: "lumi-verifier".into(),
        ..lumi.clone()
    };
    assert!(matches!(
        store
            .promote_candidate(&cross_consumer_verifier, &promotion_request)
            .await,
        Err(AppError::NotFound)
    ));
    let promoted = store
        .promote_candidate(&verifier, &promotion_request)
        .await
        .unwrap();
    let promoted_replay = store
        .promote_candidate(&verifier, &promotion_request)
        .await
        .unwrap();
    assert_eq!(promoted.id, promoted_replay.id);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM candidates WHERE tenant_id=$1 AND state='accepted' AND canonical_proposition_id IS NOT NULL",
            tenant,
        )
        .await,
        1
    );
    assert!(
        sqlx::query("DELETE FROM observations WHERE id=$1")
            .bind(observation.id)
            .execute(&pool)
            .await
            .is_err(),
        "observations must remain immutable after ingestion"
    );
    assert!(
        sqlx::query(
            "UPDATE candidates SET object_value='{\"value\":\"tampered\"}'::jsonb WHERE id=$1"
        )
        .bind(candidate.id)
        .execute(&pool)
        .await
        .is_err(),
        "accepted candidates must remain immutable"
    );
    assert!(
        sqlx::query("DELETE FROM candidate_events WHERE candidate_id=$1")
            .bind(candidate.id)
            .execute(&pool)
            .await
            .is_err(),
        "candidate lifecycle audit events must remain immutable"
    );
    assert!(
        store
            .recall(
                &lumi,
                &observation_scope,
                "deploy status",
                None,
                RecallIntent::History,
                10,
            )
            .await
            .unwrap()
            .items
            .is_empty(),
        "history is consumer-scoped and must not leak across consumers"
    );
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM candidate_events WHERE tenant_id=$1 AND event_type='accepted'",
            tenant,
        )
        .await,
        1
    );

    let first_request = Uuid::new_v4();
    let mut sensitive_memory = write(
        Uuid::new_v4(),
        project_scope.clone(),
        "sensitive-memory",
        Authority::RepositoryState,
    );
    sensitive_memory.source_ref = "user@example.com".into();
    assert!(matches!(
        store.remember(&codex, &sensitive_memory).await,
        Err(AppError::Invalid(_))
    ));
    let first = store
        .remember(
            &codex,
            &write(
                first_request,
                project_scope.clone(),
                "project-old",
                Authority::RepositoryState,
            ),
        )
        .await
        .unwrap();
    let replay = store
        .remember(
            &codex,
            &write(
                first_request,
                project_scope.clone(),
                "project-old",
                Authority::RepositoryState,
            ),
        )
        .await
        .unwrap();
    assert_eq!(first.id, replay.id);
    assert_eq!(first.snapshot_revision, replay.snapshot_revision);
    assert!(matches!(
        store
            .remember(
                &codex,
                &write(
                    first_request,
                    project_scope.clone(),
                    "different-payload",
                    Authority::RepositoryState,
                ),
            )
            .await,
        Err(AppError::Conflict(_))
    ));

    let stronger = store
        .remember(
            &codex,
            &write(
                Uuid::new_v4(),
                project_scope.clone(),
                "project-current",
                Authority::MechanicallyVerified,
            ),
        )
        .await
        .unwrap();
    assert_eq!(stronger.superseded_ids, vec![first.id]);

    let forged_identity = Identity {
        role: TokenRole::Writer,
        ..codex.clone()
    };
    let forged_policy = MemoryWriteRequest {
        request_id: Uuid::new_v4(),
        scope: ScopeSelector::default(),
        subject: "forged-policy".into(),
        predicate: "system.directive".into(),
        object: json!("must be rejected inside the store"),
        authority: Authority::OwnerInstruction,
        epistemic_status: EpistemicStatus::Asserted,
        source_type: "integration_test".into(),
        source_ref: "test:forged".into(),
        reason: "prove storage-layer authorization".into(),
    };
    assert!(matches!(
        store.remember(&forged_identity, &forged_policy).await,
        Err(AppError::Forbidden)
    ));

    let mut unaudited = pool.begin().await.unwrap();
    let unaudited_update = sqlx::query(
        "UPDATE propositions SET status='invalid',valid_to=clock_timestamp(),last_mutation_id=$1 WHERE id=$2",
    )
    .bind(Uuid::new_v4())
    .bind(stronger.id)
    .execute(&mut *unaudited)
    .await;
    assert!(
        unaudited_update.is_err() || unaudited.commit().await.is_err(),
        "a canonical transition must be linked to its exact audit mutation"
    );

    assert!(
        sqlx::query(
            "UPDATE propositions SET object_value='{\"value\":\"tampered\"}'::jsonb WHERE id=$1"
        )
        .bind(stronger.id)
        .execute(&pool)
        .await
        .is_err(),
        "canonical proposition content must be append-only"
    );
    assert!(
        sqlx::query("DELETE FROM audit_events WHERE after_id=$1")
            .bind(stronger.id)
            .execute(&pool)
            .await
            .is_err(),
        "referenced audit records must be immutable"
    );
    assert!(
        sqlx::query("DELETE FROM proposition_edges WHERE from_id=$1")
            .bind(stronger.id)
            .execute(&pool)
            .await
            .is_err(),
        "structural supersession edges must be immutable"
    );
    assert!(
        sqlx::query("UPDATE propositions SET last_mutation_id=$1 WHERE id=$2")
            .bind(first_request)
            .bind(first.id)
            .execute(&pool)
            .await
            .is_err(),
        "a completed transition cannot be relinked to another mutation"
    );
    assert!(
        sqlx::query("UPDATE propositions SET valid_to=valid_to + interval '1 second' WHERE id=$1")
            .bind(first.id)
            .execute(&pool)
            .await
            .is_err(),
        "a completed validity interval cannot be rewritten"
    );
    assert!(matches!(
        store
            .remember(
                &codex,
                &write(
                    Uuid::new_v4(),
                    project_scope.clone(),
                    "must-not-win",
                    Authority::RepositoryState
                )
            )
            .await,
        Err(AppError::Conflict(_))
    ));
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM propositions WHERE tenant_id=$1 AND subject_key='interlock-v6' AND status='current'",
            tenant
        )
        .await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM propositions WHERE tenant_id=$1 AND status='superseded' AND valid_to IS NOT NULL", tenant).await, 1);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM proposition_edges WHERE tenant_id=$1 AND edge_type='supersedes'",
            tenant
        )
        .await,
        1
    );

    let repository_scope = ScopeSelector {
        project_key: Some("git:test/repo".into()),
        repository_key: Some("git:test/repo".into()),
        ..Default::default()
    };
    store
        .remember(
            &codex,
            &write(
                Uuid::new_v4(),
                repository_scope.clone(),
                "repository-current",
                Authority::RepositoryState,
            ),
        )
        .await
        .unwrap();
    let recalled = store
        .recall(
            &codex,
            &repository_scope,
            "interlock v6",
            None,
            RecallIntent::Current,
            20,
        )
        .await
        .unwrap();
    assert_eq!(
        recalled.items.len(),
        1,
        "broader project value must be structurally shadowed"
    );
    assert_eq!(
        recalled.items[0].object,
        json!({"value":"repository-current"})
    );

    let directive = MemoryWriteRequest {
        request_id: Uuid::new_v4(),
        scope: ScopeSelector::default(),
        subject: "policy-test".into(),
        predicate: "system.directive".into(),
        object: json!("shared across consumers"),
        authority: Authority::OwnerInstruction,
        epistemic_status: EpistemicStatus::Asserted,
        source_type: "integration_test".into(),
        source_ref: "test:shared".into(),
        reason: "prove shared policy".into(),
    };
    store.remember(&codex, &directive).await.unwrap();
    let lumi_mandatory = store
        .mandatory(&lumi, &ScopeSelector::default())
        .await
        .unwrap();
    assert!(
        lumi_mandatory
            .iter()
            .any(|item| item.subject == "policy-test")
    );

    let marker = format!("handoff-{}", Uuid::new_v4());
    store
        .write_handoff(
            &codex,
            &HandoffWriteRequest {
                request_id: Uuid::new_v4(),
                project_key: "git:test/repo".into(),
                content: marker.clone(),
                session_id: "session-1".into(),
                expires_at: None,
            },
        )
        .await
        .unwrap();
    assert!(
        store
            .latest_handoff(&codex, "git:test/repo")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .latest_handoff(&lumi, "git:test/repo")
            .await
            .unwrap()
            .is_none()
    );
    let history = store
        .recall(
            &codex,
            &repository_scope,
            &marker,
            None,
            RecallIntent::Explore,
            20,
        )
        .await
        .unwrap();
    assert!(
        history
            .items
            .iter()
            .all(|item| !item.rendered.contains(&marker))
    );

    let other_tenant = Uuid::new_v4();
    let cross_tenant = sqlx::query(
        "INSERT INTO observations(tenant_id,user_id,consumer_id,request_id,request_hash,source_event_id,event_kind,actor,scope_id,observed_at,redacted_content,content_sha256) \
         SELECT $1,s.user_id,$2,gen_random_uuid(),digest('cross-request','sha256'),'cross-tenant','test','test',s.id,clock_timestamp(),'x',digest('x','sha256') FROM scopes s WHERE s.tenant_id=$3 LIMIT 1"
    ).bind(other_tenant).bind(codex.consumer_id).bind(tenant).execute(&pool).await;
    assert!(
        cross_tenant.is_err(),
        "composite FK must reject cross-tenant provenance"
    );

    let concurrent_tenant = Uuid::new_v4();
    let concurrent_identity = identity(concurrent_tenant, Uuid::new_v4(), Uuid::new_v4());
    let concurrent_scope = ScopeSelector {
        project_key: Some("git:test/concurrent".into()),
        ..Default::default()
    };
    let mut tasks = Vec::new();
    for index in 0..8 {
        let store = store.clone();
        let identity = concurrent_identity.clone();
        let request = write(
            Uuid::new_v4(),
            concurrent_scope.clone(),
            &format!("concurrent-{index}"),
            Authority::RepositoryState,
        );
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                match store.remember(&identity, &request).await {
                    Ok(response) => return response,
                    Err(AppError::Retryable) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected concurrent write error: {error}"),
                }
            }
            panic!("concurrent write did not succeed after retry budget")
        }));
    }
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        ids.insert(task.await.unwrap().id);
    }
    assert_eq!(ids.len(), 8);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM propositions WHERE tenant_id=$1 AND status='current'",
            concurrent_tenant
        )
        .await,
        1
    );
    assert_eq!(count(&pool, "SELECT count(*) FROM propositions WHERE tenant_id=$1 AND status='superseded' AND valid_to IS NOT NULL", concurrent_tenant).await, 7);
    assert_eq!(
        count(
            &pool,
            "SELECT count(*) FROM proposition_edges WHERE tenant_id=$1 AND edge_type='supersedes'",
            concurrent_tenant
        )
        .await,
        7
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL 15+"]
async fn populated_v1_schema_upgrades_to_observation_candidate_lifecycle() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    let schema = format!("upgrade_{}", Uuid::new_v4().simple());
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(&format!("SET search_path TO {schema},public"))
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(MIGRATION_0001)
        .execute(&mut *connection)
        .await
        .unwrap();
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let consumer = Uuid::new_v4();
    let scope: Uuid = sqlx::query(
        "INSERT INTO scopes(tenant_id,user_id,consumer_id,scope_level) VALUES($1,$2,$3,'user') RETURNING id",
    )
    .bind(tenant)
    .bind(user)
    .bind(consumer)
    .fetch_one(&mut *connection)
    .await
    .unwrap()
    .get("id");
    sqlx::query(
        "INSERT INTO observations(tenant_id,user_id,consumer_id,source_event_id,event_kind,actor,scope_id,observed_at,redacted_content,content_sha256) VALUES($1,$2,$3,'legacy-event','turn','legacy',$4,clock_timestamp(),'legacy redacted',digest('legacy redacted','sha256'))",
    )
    .bind(tenant)
    .bind(user)
    .bind(consumer)
    .bind(scope)
    .execute(&mut *connection)
    .await
    .unwrap();
    sqlx::raw_sql(MIGRATION_0002)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(MIGRATION_0003)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(MIGRATION_0004)
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql(MIGRATION_0005)
        .execute(&mut *connection)
        .await
        .unwrap();
    let upgraded: bool = sqlx::query_scalar(
        "SELECT request_id IS NOT NULL AND octet_length(request_hash)=32 FROM observations WHERE source_event_id='legacy-event'",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert!(
        upgraded,
        "legacy observations must receive stable request identity"
    );
    let embedding_queued: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM observation_embeddings WHERE observation_id=(SELECT id FROM observations WHERE source_event_id='legacy-event'))",
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    assert!(
        embedding_queued,
        "migration 3 must queue populated legacy observations"
    );
    sqlx::query("SET search_path TO public")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&mut *connection)
        .await
        .unwrap();
}

/// The ANN seed must reach the HNSW index. If the vector comparison is ever moved
/// back over a materialized CTE the index becomes unreachable, every recall
/// degrades to a full scan of the embedding table, and production starts timing
/// out again — which is exactly how the 2026-08-24 outage happened.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector 0.8.2+"]
async fn ann_seed_uses_the_hnsw_index_and_not_a_sequential_scan() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();
    let owner = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let scope = ScopeSelector {
        project_key: Some(format!("git:test/plan-{}", Uuid::new_v4())),
        ..Default::default()
    };
    for index in 0..8 {
        store
            .remember(
                &owner,
                &write(
                    Uuid::new_v4(),
                    scope.clone(),
                    &format!("plan probe {index}"),
                    Authority::OwnerInstruction,
                ),
            )
            .await
            .unwrap();
    }
    store
        .embed_pending(&StaticEmbedder, "test-bge", 64, Uuid::new_v4())
        .await
        .unwrap();

    let probe = StaticEmbedder.embed("plan probe").await.unwrap();
    let literal = format!(
        "[{}]",
        probe
            .values
            .iter()
            .map(f32::to_string)
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("SET LOCAL hnsw.iterative_scan='strict_order'")
        .execute(&mut *connection)
        .await
        .ok();
    sqlx::query("SET enable_seqscan=off")
        .execute(&mut *connection)
        .await
        .unwrap();
    let plan: Vec<String> = sqlx::query(
        r#"EXPLAIN SELECT pe.proposition_id, pe.embedding <=> $3::vector AS distance
           FROM proposition_embeddings pe
           WHERE pe.embedding IS NOT NULL
             AND pe.tenant_id=$1 AND pe.user_id=$2
             AND pe.embedding_model='test-bge'
           ORDER BY pe.embedding <=> $3::vector
           LIMIT 64"#,
    )
    .bind(owner.tenant_id)
    .bind(owner.user_id)
    .bind(&literal)
    .fetch_all(&mut *connection)
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.get::<String, _>(0))
    .collect();
    let plan = plan.join("\n");

    assert!(
        plan.contains("proposition_embeddings_hnsw_idx"),
        "ANN seed must be servable by the HNSW index; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Seq Scan on proposition_embeddings"),
        "ANN seed must not fall back to a sequential scan; plan was:\n{plan}"
    );
}

/// Query A is only a hint. Everything the caller is not entitled to see, and
/// everything precedence rules out, must still be rejected by Query B — even
/// when the ANN seed ranks it first.
///
/// StaticEmbedder gives every row the same vector, so every row is an equally
/// near neighbour and the seed contains all of them. Nothing but the filtering
/// and precedence logic stands between a forbidden row and the response.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector 0.8.2+"]
async fn ann_seed_cannot_smuggle_forbidden_or_shadowed_rows_into_recall() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();

    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let owner = identity(tenant, user, Uuid::new_v4());
    let other_tenant = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let project = format!("git:test/adversarial-{}", Uuid::new_v4());
    let project_scope = ScopeSelector {
        project_key: Some(project.clone()),
        ..Default::default()
    };
    let global_scope = ScopeSelector::default();

    // Shadowed: same canonical key at global scope, beaten by the project-scoped
    // row on specificity.
    store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                global_scope.clone(),
                "shadowed-broader-value",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    store
        .remember(
            &owner,
            &write(
                Uuid::new_v4(),
                project_scope.clone(),
                "winning-specific-value",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    // Another tenant entirely: must never be a candidate.
    store
        .remember(
            &other_tenant,
            &write(
                Uuid::new_v4(),
                project_scope.clone(),
                "cross-tenant-secret",
                Authority::OwnerInstruction,
            ),
        )
        .await
        .unwrap();
    store
        .embed_pending(&StaticEmbedder, "test-bge", 64, Uuid::new_v4())
        .await
        .unwrap();

    let probe = StaticEmbedder.embed("anything").await.unwrap();
    let outcome = store
        .recall(
            &owner,
            &project_scope,
            "lexically-absent-zzyzx",
            Some(&probe),
            RecallIntent::Current,
            50,
        )
        .await
        .unwrap();

    let rendered: Vec<String> = outcome
        .items
        .iter()
        .map(|item| item.object.to_string())
        .collect();
    let joined = rendered.join(" | ");
    assert!(
        !joined.contains("cross-tenant-secret"),
        "another tenant's row reached recall through the ANN seed: {joined}"
    );
    assert!(
        !joined.contains("shadowed-broader-value"),
        "a precedence-losing row reached recall through the ANN seed: {joined}"
    );
    assert!(
        joined.contains("winning-specific-value"),
        "the entitled, precedence-winning row must still be returned: {joined}"
    );
}

/// The outbox had an INSERT and no consumer anywhere in the codebase, so it grew
/// one row per canonical write from 2026-04-09 onward and reached 36,224 rows.
/// This proves the drain claims and completes work, is idempotent across repeat
/// runs, and that retention only removes rows that were actually finished.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector 0.8.2+"]
async fn outbox_drains_completes_and_prunes_only_finished_events() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();
    let owner = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let scope = ScopeSelector {
        project_key: Some(format!("git:test/outbox-{}", Uuid::new_v4())),
        ..Default::default()
    };

    for n in 0..3 {
        store
            .remember(
                &owner,
                &write(
                    Uuid::new_v4(),
                    scope.clone(),
                    &format!("outbox seed {n}"),
                    Authority::OwnerInstruction,
                ),
            )
            .await
            .unwrap();
    }
    let pending_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox WHERE completed_at IS NULL AND tenant_id=$1",
    )
    .bind(owner.tenant_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        pending_before >= 3,
        "each canonical write should enqueue an event"
    );

    let drained = store.drain_outbox(512, Uuid::new_v4()).await.unwrap();
    assert!(drained >= 3, "drain must claim and complete the backlog");

    let pending_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox WHERE completed_at IS NULL AND tenant_id=$1",
    )
    .bind(owner.tenant_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        pending_after, 0,
        "nothing of ours should remain outstanding"
    );

    // Draining again must not resurrect completed work or double-count it.
    let second = store.drain_outbox(512, Uuid::new_v4()).await.unwrap();
    assert_eq!(second, 0, "a completed outbox must drain to nothing");

    // Retention keeps recent completions; ours were completed seconds ago.
    let deleted = store.prune_outbox(7).await.unwrap();
    let survivors: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE tenant_id=$1")
        .bind(owner.tenant_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        survivors >= 3,
        "a 7-day window must not delete work completed moments ago (deleted {deleted})"
    );

    // Age ours past the window and confirm retention then collects them.
    sqlx::query(
        "UPDATE outbox SET completed_at = clock_timestamp() - interval '30 days' WHERE tenant_id=$1",
    )
    .bind(owner.tenant_id)
    .execute(&pool)
    .await
    .unwrap();
    store.prune_outbox(7).await.unwrap();
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE tenant_id=$1")
        .bind(owner.tenant_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "aged, completed events must be collected");
}
/// An exact subject lookup is its own retrieval lane. It must survive fusion
/// even when the semantic seed is full of unrelated, higher-authority rows.
///
/// This reproduces the 2026-08-27 production symptom through a structural risk
/// in the same path: lexical generation finds the fresh row, then the final
/// authority-first LIMIT discards it behind unrelated owner-authority semantic
/// candidates. The transient production trigger was not captured.
#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector 0.8.2+"]
async fn exact_subject_lookup_survives_semantic_fusion_and_authority_ordering() {
    let database_url = std::env::var("TEST_DATABASE_URL").expect("TEST_DATABASE_URL required");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    let store = PgMemoryStore::new(pool.clone());
    store.migrate().await.unwrap();

    let owner = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let scope = ScopeSelector {
        project_key: Some(format!("git:test/exact-lane-{}", Uuid::new_v4())),
        ..Default::default()
    };
    let target_subject = format!("exact-recall-fixture-{}", Uuid::new_v4());
    let target = store
        .remember(
            &owner,
            &MemoryWriteRequest {
                request_id: Uuid::new_v4(),
                scope: scope.clone(),
                subject: target_subject.clone(),
                predicate: "project.state".into(),
                object: json!({"value":"fresh exact target"}),
                authority: Authority::MechanicallyVerified,
                epistemic_status: EpistemicStatus::Verified,
                source_type: "integration_test".into(),
                source_ref: "test:exact-lane-target".into(),
                reason: "prove exact lookup survives hybrid fusion".into(),
            },
        )
        .await
        .unwrap();

    // StaticEmbedder gives every row the same vector. More than `limit`
    // unrelated owner-authority rows therefore fill the semantic lane and
    // outrank the mechanically-verified target unless exact lookup is carried
    // independently through final selection.
    for index in 0..12 {
        store
            .remember(
                &owner,
                &MemoryWriteRequest {
                    request_id: Uuid::new_v4(),
                    scope: scope.clone(),
                    subject: format!("semantic-distractor-{index}-{}", Uuid::new_v4()),
                    predicate: "project.state".into(),
                    object: json!({"value":format!("unrelated owner row {index}")}),
                    authority: Authority::OwnerInstruction,
                    epistemic_status: EpistemicStatus::Verified,
                    source_type: "integration_test".into(),
                    source_ref: format!("test:exact-lane-distractor-{index}"),
                    reason: "fill the semantic lane with higher-authority distractors".into(),
                },
            )
            .await
            .unwrap();
    }
    store
        .embed_pending(&StaticEmbedder, "test-bge", 64, Uuid::new_v4())
        .await
        .unwrap();

    let query_embedding = StaticEmbedder.embed(&target_subject).await.unwrap();
    let outcome = store
        .recall(
            &owner,
            &scope,
            &target_subject,
            Some(&query_embedding),
            RecallIntent::Current,
            10,
        )
        .await
        .unwrap();

    assert!(
        outcome.items.iter().any(|item| item.id == target.id),
        "exact subject {target_subject} was generated lexically but discarded after fusion: {:?}",
        outcome
            .items
            .iter()
            .map(|item| (&item.subject, item.authority))
            .collect::<Vec<_>>()
    );
}

