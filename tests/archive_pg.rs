//! Interlock 6.5 conversation-archive integration tests. These require a
//! disposable PostgreSQL database reachable at TEST_ARCHIVE_DATABASE_URL and are
//! ignored by default, matching the existing v6 PostgreSQL test convention. They
//! prove idempotent replay/dedup, ingestion-order mining cursors, tenant/user
//! isolation, and the resumable deletion-saga foundation.

use interlock::{
    Identity, PgArchiveStore, TokenRole,
    error::AppError,
    archive::{
        ArchiveActor, ArchiveEventInput, ArchiveEventKind, ArchiveExportRequest,
        ArchiveIngestRequest, ArchiveSearchRequest, ArchiveStore, DeletionMode, DeletionRequest,
        IngestStatus,
    },
};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

fn identity(tenant: Uuid, user: Uuid, consumer: Uuid, role: TokenRole) -> Identity {
    Identity {
        tenant_id: tenant,
        user_id: user,
        consumer_id: consumer,
        actor: "test-adapter".into(),
        role,
    }
}

fn event(source_event_id: &str, content: &str, seconds_ago: i64) -> ArchiveEventInput {
    ArchiveEventInput {
        source_event_id: source_event_id.into(),
        installation_id: Uuid::from_u128(0xA11),
        project_key: Some("git:test/interlock".into()),
        repository_key: None,
        thread_id: Some("thread-1".into()),
        session_id: Some("session-1".into()),
        turn_id: None,
        sequence_number: Some(1),
        actor: ArchiveActor::User,
        event_kind: ArchiveEventKind::Message,
        content_type: "text/markdown".into(),
        schema_version: 1,
        content: content.into(),
        raw_content_ref: None,
        source_timestamp: chrono::Utc::now() - chrono::Duration::seconds(seconds_ago),
        capture_adapter_version: "test-1.0".into(),
    }
}

async fn store() -> PgArchiveStore {
    let url = std::env::var("TEST_ARCHIVE_DATABASE_URL")
        .expect("TEST_ARCHIVE_DATABASE_URL required for archive integration tests");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let store = PgArchiveStore::new(pool);
    store.migrate().await.unwrap();
    store
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn ingestion_is_idempotent_and_detects_content_drift() {
    let store = store().await;
    let writer = identity(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        TokenRole::Writer,
    );
    let sid = format!("evt-{}", Uuid::new_v4());
    let request = ArchiveIngestRequest {
        events: vec![event(&sid, "hello world", 10)],
    };
    let first = store.ingest_batch(&writer, &request).await.unwrap();
    assert_eq!(first.accepted, 1);
    assert_eq!(first.acks[0].status, IngestStatus::Accepted);

    // Replay of the identical batch is a no-op: already present, not duplicated.
    let replay = store.ingest_batch(&writer, &request).await.unwrap();
    assert_eq!(replay.already_present, 1);
    assert_eq!(replay.acks[0].status, IngestStatus::AlreadyPresent);
    assert_eq!(replay.acks[0].event_id, first.acks[0].event_id);

    // The same source_event_id with different content is rejected, never
    // silently overwriting archived history.
    let drift = ArchiveIngestRequest {
        events: vec![event(&sid, "different content", 10)],
    };
    let rejected = store.ingest_batch(&writer, &drift).await.unwrap();
    assert_eq!(rejected.rejected, 1);
    assert_eq!(rejected.acks[0].status, IngestStatus::Rejected);

    // Metadata is part of the idempotency contract too. A source adapter may
    // not reuse an event ID to silently move the same content to another turn.
    let mut metadata_drift = event(&sid, "hello world", 10);
    metadata_drift.turn_id = Some("different-turn".into());
    let rejected = store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![metadata_drift],
            },
        )
        .await
        .unwrap();
    assert_eq!(rejected.rejected, 1);
    assert_eq!(rejected.acks[0].status, IngestStatus::Rejected);
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn mining_cursor_advances_in_ingestion_order_over_late_events() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let writer = identity(tenant, user, Uuid::new_v4(), TokenRole::Writer);
    // Mining is an owner-only surface: it crosses consumers and its cursor is
    // irreversible.
    let owner = identity(tenant, user, Uuid::new_v4(), TokenRole::Owner);
    assert!(matches!(
        store.mining_pending(&writer, "gen-denied", 10).await,
        Err(AppError::Forbidden)
    ));
    assert!(matches!(
        store.advance_cursor(&writer, "gen-denied", 1).await,
        Err(AppError::Forbidden)
    ));
    let generation = format!("gen-{}", Uuid::new_v4());
    // Ingest a recent event first.
    let recent = format!("recent-{}", Uuid::new_v4());
    store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(&recent, "recent", 5)],
            },
        )
        .await
        .unwrap();
    let pending = store
        .mining_pending(&owner, &generation, 100)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    let first_seq = pending[0].ingestion_seq;
    store
        .advance_cursor(&owner, &generation, first_seq)
        .await
        .unwrap();
    assert!(
        store
            .mining_pending(&owner, &generation, 100)
            .await
            .unwrap()
            .is_empty()
    );

    // A late-arriving event whose SOURCE timestamp is far older than the already
    // mined window still gets a higher ingestion sequence, so it appears after
    // the cursor and is processed.
    let late = format!("late-{}", Uuid::new_v4());
    store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(&late, "late but old", 100000)],
            },
        )
        .await
        .unwrap();
    let after = store
        .mining_pending(&owner, &generation, 100)
        .await
        .unwrap();
    assert_eq!(
        after.len(),
        1,
        "late event below the mined source window must still be mined"
    );
    assert!(after[0].ingestion_seq > first_seq);
    assert_eq!(after[0].source_event_id, late);
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn search_evidence_and_export_are_user_isolated() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let owner_a = identity(tenant, Uuid::new_v4(), Uuid::new_v4(), TokenRole::Owner);
    let owner_b = identity(tenant, Uuid::new_v4(), Uuid::new_v4(), TokenRole::Owner);
    let sid = format!("iso-{}", Uuid::new_v4());
    let ack = store
        .ingest_batch(
            &owner_a,
            &ArchiveIngestRequest {
                events: vec![event(&sid, "zzyzx unique needle", 5)],
            },
        )
        .await
        .unwrap();
    let event_id = ack.acks[0].event_id.unwrap();

    let found = store
        .search(
            &owner_a,
            &ArchiveSearchRequest {
                query: Some("zzyzx".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    let evidence = store.evidence(&owner_a, &[event_id]).await.unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].source_event_id, sid);

    // A different user in the same tenant cannot see it.
    assert!(
        store
            .search(
                &owner_b,
                &ArchiveSearchRequest {
                    query: Some("zzyzx".into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .evidence(&owner_b, &[event_id])
            .await
            .unwrap()
            .is_empty()
    );
    let export = store
        .export(
            &owner_b,
            &ArchiveExportRequest {
                consumer_id: None,
                project_key: None,
                thread_id: None,
                from: None,
                to: None,
                after_ingestion_seq: None,
                limit: 100,
            },
        )
        .await
        .unwrap();
    assert!(export.events.iter().all(|e| e.source_event_id != sid));
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn deletion_saga_tombstones_and_is_resumable() {
    let store = store().await;
    let owner = identity(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        TokenRole::Owner,
    );
    let sid = format!("del-{}", Uuid::new_v4());
    store
        .ingest_batch(
            &owner,
            &ArchiveIngestRequest {
                events: vec![event(&sid, "delete me needle", 5)],
            },
        )
        .await
        .unwrap();
    let request = DeletionRequest {
        request_id: Uuid::new_v4(),
        mode: DeletionMode::Full,
        consumer_id: None,
        project_key: None,
        thread_id: Some("thread-1".into()),
        session_id: None,
        from: None,
        to: None,
    };
    // Step 1: intent recorded before any data changes; creation is idempotent.
    let intent = store.create_deletion(&owner, &request).await.unwrap();
    let intent_again = store.create_deletion(&owner, &request).await.unwrap();
    assert_eq!(intent.intent_id, intent_again.intent_id);
    assert!(intent.archive_tombstoned_at.is_none());
    assert!(!intent.pending_canonical_steps.is_empty());

    // Run the saga: matching events are tombstoned and excluded from search.
    let ran = store.run_deletion(&owner, intent.intent_id).await.unwrap();
    assert!(ran.archive_tombstoned_at.is_some());
    assert!(ran.tombstoned_event_count >= 1);
    assert!(
        store
            .search(
                &owner,
                &ArchiveSearchRequest {
                    query: Some("needle".into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .is_empty()
    );

    // Re-running (as a crash-recovery reconciler would) is idempotent and does
    // not re-tombstone or change the count.
    let rerun = store.run_deletion(&owner, intent.intent_id).await.unwrap();
    assert_eq!(rerun.tombstoned_event_count, ran.tombstoned_event_count);
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn ingestion_requires_write_and_delete_requires_owner() {
    let store = store().await;
    let reader = identity(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        TokenRole::Reader,
    );
    let sid = format!("perm-{}", Uuid::new_v4());
    assert!(
        store
            .ingest_batch(
                &reader,
                &ArchiveIngestRequest {
                    events: vec![event(&sid, "x", 1)]
                }
            )
            .await
            .is_err()
    );
    let writer = identity(
        reader.tenant_id,
        reader.user_id,
        reader.consumer_id,
        TokenRole::Writer,
    );
    store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(&sid, "x", 1)],
            },
        )
        .await
        .unwrap();
    // A writer cannot create a deletion intent; only an owner may.
    assert!(
        store
            .create_deletion(
                &writer,
                &DeletionRequest {
                    request_id: Uuid::new_v4(),
                    mode: DeletionMode::Full,
                    consumer_id: None,
                    project_key: None,
                    thread_id: None,
                    session_id: None,
                    from: None,
                    to: None,
                }
            )
            .await
            .is_err()
    );
}
