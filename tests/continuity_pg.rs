//! Foreman 6.5 continuity-handoff integration tests. These run against the v6
//! canonical database (which carries the `continuity` schema in migration 6) and
//! are ignored by default. They prove exact-context isolation, compare-and-swap
//! supersession with a single winner, idempotent acknowledgement, clean closure,
//! and that a handoff has no path into candidates or propositions.

use foreman_memory_v6::{
    Identity, PgContinuityStore, PgMemoryStore, TokenRole,
    continuity::{
        AckRequest, CloseRequest, CompleteItemsRequest, ContextKind, ContextRef, ContinuityStore,
        HandoffWriteInput,
    },
    error::AppError,
};
use sqlx::{Row, postgres::PgPoolOptions};
use uuid::Uuid;

fn identity(tenant: Uuid, user: Uuid, consumer: Uuid) -> Identity {
    Identity {
        tenant_id: tenant,
        user_id: user,
        consumer_id: consumer,
        actor: "test-agent".into(),
        role: TokenRole::Writer,
    }
}

fn context(key: &str) -> ContextRef {
    ContextRef {
        kind: ContextKind::RepositoryWorktree,
        key: key.into(),
        family_id: None,
    }
}

fn write_input(context: ContextRef, summary: &str, expected: Option<Uuid>) -> HandoffWriteInput {
    HandoffWriteInput {
        request_id: Uuid::new_v4(),
        context,
        session_id: "session-1".into(),
        thread_id: Some("thread-1".into()),
        summary: summary.into(),
        written_by: "test-agent".into(),
        completed: vec![],
        in_progress: vec![],
        next_actions: vec!["finish the migration".into()],
        blockers: vec![],
        artifacts: vec![],
        verification_state: None,
        do_not_repeat: vec![],
        expected_active_id: expected,
        source_snapshot_revision: None,
        expires_at: None,
    }
}

async fn store() -> PgContinuityStore {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("TEST_DATABASE_URL required for continuity integration tests");
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(&url)
        .await
        .unwrap();
    PgMemoryStore::new(pool.clone()).migrate().await.unwrap();
    PgContinuityStore::new(pool)
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector"]
async fn get_exact_is_context_isolated_and_rejects_broad_keys() {
    let store = store().await;
    let id = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key_a = format!("git:test/repo-a-{}@main", Uuid::new_v4());
    let key_b = format!("git:test/repo-b-{}@main", Uuid::new_v4());
    store
        .write(&id, &write_input(context(&key_a), "work on A", None))
        .await
        .unwrap();
    // The exact context returns its handoff.
    assert!(
        store
            .get_exact(&id, &context(&key_a))
            .await
            .unwrap()
            .is_some()
    );
    // A different repository under a shared parent never leaks A's handoff.
    assert!(
        store
            .get_exact(&id, &context(&key_b))
            .await
            .unwrap()
            .is_none()
    );
    // A forbidden broad key never resolves to a handoff.
    assert!(
        store
            .get_exact(&id, &context("/home/justin"))
            .await
            .unwrap()
            .is_none()
    );
    // Writing to a forbidden key is rejected outright.
    assert!(
        store
            .write(&id, &write_input(context("/"), "broad", None))
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector"]
async fn concurrent_supersession_has_exactly_one_winner() {
    let store = store().await;
    let id = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key = format!("git:test/cas-{}@main", Uuid::new_v4());
    let first = store
        .write(&id, &write_input(context(&key), "v1", None))
        .await
        .unwrap();

    // Two writers both observed `first` as active and both try to supersede it.
    let (s1, s2) = (store.clone(), store.clone());
    let (id1, id2) = (id.clone(), id.clone());
    let (k1, k2) = (key.clone(), key.clone());
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move {
            s1.write(
                &id1,
                &write_input(context(&k1), "v2-a", Some(first.handoff_id)),
            )
            .await
        }),
        tokio::spawn(async move {
            s2.write(
                &id2,
                &write_input(context(&k2), "v2-b", Some(first.handoff_id)),
            )
            .await
        }),
    );
    let results = [r1.unwrap(), r2.unwrap()];
    let winners = results.iter().filter(|r| r.is_ok()).count();
    let conflicts = results
        .iter()
        .filter(|r| matches!(r, Err(AppError::Conflict(_))))
        .count();
    assert_eq!(winners, 1, "exactly one CAS writer may win");
    assert_eq!(
        conflicts, 1,
        "the loser receives a conflict, not a silent overwrite"
    );

    // Exactly one active handoff remains for the context.
    let active = store.get_exact(&id, &context(&key)).await.unwrap().unwrap();
    assert_ne!(active.handoff_id, first.handoff_id);
    assert_eq!(active.predecessor_handoff_id, Some(first.handoff_id));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector"]
async fn acknowledgement_is_idempotent_per_consumer() {
    let store = store().await;
    let id = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key = format!("git:test/ack-{}@main", Uuid::new_v4());
    let handoff = store
        .write(&id, &write_input(context(&key), "v1", None))
        .await
        .unwrap();
    let first = store
        .acknowledge(
            &id,
            &AckRequest {
                handoff_id: handoff.handoff_id,
                session_id: "s2".into(),
            },
        )
        .await
        .unwrap();
    assert!(first.newly_acknowledged);
    // Repeat acknowledgement is a no-op that preserves the first receipt time.
    let second = store
        .acknowledge(
            &id,
            &AckRequest {
                handoff_id: handoff.handoff_id,
                session_id: "s3".into(),
            },
        )
        .await
        .unwrap();
    assert!(!second.newly_acknowledged);
    assert_eq!(first.first_acknowledged_at, second.first_acknowledged_at);
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector"]
async fn complete_items_and_close_leave_no_active_handoff() {
    let store = store().await;
    let id = identity(Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let key = format!("git:test/close-{}@main", Uuid::new_v4());
    let handoff = store
        .write(&id, &write_input(context(&key), "v1", None))
        .await
        .unwrap();
    let item_id = handoff.items[0].item_id;
    let updated = store
        .complete_items(
            &id,
            &CompleteItemsRequest {
                handoff_id: handoff.handoff_id,
                item_ids: vec![item_id],
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.items[0].status, "completed");

    // A clean close leaves no misleading active continuation.
    store
        .close(
            &id,
            &CloseRequest {
                context: context(&key),
                expected_active_id: handoff.handoff_id,
            },
        )
        .await
        .unwrap();
    assert!(
        store
            .get_exact(&id, &context(&key))
            .await
            .unwrap()
            .is_none()
    );

    // Closing again against a stale active pointer is a conflict, not a silent
    // reclose of an unrelated handoff.
    assert!(matches!(
        store
            .close(
                &id,
                &CloseRequest {
                    context: context(&key),
                    expected_active_id: handoff.handoff_id
                }
            )
            .await,
        Err(AppError::Conflict(_) | AppError::NotFound)
    ));
}

#[tokio::test]
#[ignore = "requires TEST_DATABASE_URL pointing at disposable PostgreSQL with pgvector"]
async fn handoff_tables_have_no_path_into_canonical_memory() {
    let store = store().await;
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    // No foreign key from any continuity table references candidates or
    // propositions: a handoff can never flow into mining or canonicalization.
    let crossing: i64 = sqlx::query(
        r#"SELECT count(*) AS c FROM information_schema.table_constraints tc
             JOIN information_schema.constraint_column_usage ccu
               ON tc.constraint_name=ccu.constraint_name AND tc.constraint_schema=ccu.constraint_schema
           WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema='continuity'
             AND ccu.table_schema='public'
             AND ccu.table_name IN ('propositions','candidates','candidate_evidence','predicates')"#,
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("c");
    assert_eq!(
        crossing, 0,
        "handoffs must have no FK into canonical tables"
    );
    // Keep the store handle alive so migrations ran once.
    drop(store);
}
