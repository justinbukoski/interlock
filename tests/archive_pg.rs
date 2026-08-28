//! Interlock 6.5 conversation-archive integration tests. These require a
//! disposable PostgreSQL database reachable at TEST_ARCHIVE_DATABASE_URL and are
//! ignored by default, matching the existing v6 PostgreSQL test convention. They
//! prove idempotent replay/dedup, ingestion-order mining cursors, tenant/user
//! isolation, and the resumable deletion-saga foundation.

use interlock::{
    Identity, PgArchiveStore, TokenRole,
    archive::{
        ArchiveActor, ArchiveEventInput, ArchiveEventKind, ArchiveExportRequest,
        ArchiveIngestRequest, ArchiveSearchRequest, ArchiveStore, DeletionMode, DeletionRequest,
        IngestStatus,
    },
    error::AppError,
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

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn reader_reads_redacted_archive_across_consumers() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    // Events are ingested under the capture adapter's consumer_id; a Reader
    // never ingests and carries a different consumer_id of its own. Before the
    // reader-visibility fix the consumer filter matched zero rows here and the
    // lane answered [] instead of the user's events.
    let writer = identity(tenant, user, Uuid::new_v4(), TokenRole::Writer);
    let reader = identity(tenant, user, Uuid::new_v4(), TokenRole::Reader);
    let sid = format!("reader-{}", Uuid::new_v4());
    let mut input = event(&sid, "quokka cross consumer needle", 5);
    input.raw_content_ref = Some("encrypted:raw-reader-1".into());
    let ack = store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![input],
            },
        )
        .await
        .unwrap();
    let event_id = ack.acks[0].event_id.unwrap();

    let found = store
        .search(
            &reader,
            &ArchiveSearchRequest {
                query: Some("quokka".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].consumer_id, writer.consumer_id);
    assert_eq!(found[0].redacted_content, "quokka cross consumer needle");
    assert!(found[0].raw_available);

    let evidence = store.evidence(&reader, &[event_id]).await.unwrap();
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].source_event_id, sid);
    assert!(evidence[0].raw_available);

    // Raw availability is reported; the encrypted locator itself never appears
    // in either serialized response.
    for serialized in [
        serde_json::to_string(&found).unwrap(),
        serde_json::to_string(&evidence).unwrap(),
    ] {
        assert!(!serialized.contains("encrypted:"));
        assert!(!serialized.contains("raw-reader-1"));
    }

    // Widening consumer scope did not widen tenant/user scope (§11: all
    // cross-application retrieval stays tenant- and user-scoped): a Reader of
    // a different user in the same tenant sees nothing.
    let stranger = identity(tenant, Uuid::new_v4(), Uuid::new_v4(), TokenRole::Reader);
    assert!(
        store
            .search(
                &stranger,
                &ArchiveSearchRequest {
                    query: Some("quokka".into()),
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
            .evidence(&stranger, &[event_id])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn reader_visible_redacted_content_has_secrets_scrubbed() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let writer = identity(tenant, user, Uuid::new_v4(), TokenRole::Writer);
    let reader = identity(tenant, user, Uuid::new_v4(), TokenRole::Reader);
    let sid = format!("redact-{}", Uuid::new_v4());
    store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(
                    &sid,
                    "rotate the api_key=sup3rs3cretvalue before Friday standup",
                    5,
                )],
            },
        )
        .await
        .unwrap();

    // The security claim the widened Reader lane rests on: content is scrubbed
    // at ingestion, so what crosses consumers is the redacted representation.
    let found = store
        .search(
            &reader,
            &ArchiveSearchRequest {
                query: Some("rotate".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert!(!found[0].redacted_content.contains("sup3rs3cretvalue"));
    assert!(found[0].redacted_content.contains("[REDACTED_SECRET]"));
    assert!(found[0].redaction_count >= 1);

    let evidence = store
        .evidence(&reader, &[found[0].event_id])
        .await
        .unwrap();
    assert_eq!(evidence.len(), 1);
    assert!(!evidence[0].redacted_content.contains("sup3rs3cretvalue"));
    assert!(evidence[0].redaction_count >= 1);
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn export_and_non_reader_roles_stay_consumer_confined() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let writer = identity(tenant, user, Uuid::new_v4(), TokenRole::Writer);
    let reader = identity(tenant, user, Uuid::new_v4(), TokenRole::Reader);
    let sid = format!("bound-{}", Uuid::new_v4());
    let ack = store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(&sid, "boundary needle", 5)],
            },
        )
        .await
        .unwrap();
    let event_id = ack.acks[0].event_id.unwrap();

    // Reader on export stays confined: an unfiltered export spans only the
    // reader's own (empty) consumer, and an explicit foreign consumer_id is a
    // hard 403, never a silent cross-consumer bulk read.
    let export = store
        .export(
            &reader,
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
    assert!(
        export
            .events
            .iter()
            .all(|event| event.source_event_id != sid)
    );
    assert!(
        matches!(
            store
                .export(
                    &reader,
                    &ArchiveExportRequest {
                        consumer_id: Some(writer.consumer_id),
                        project_key: None,
                        thread_id: None,
                        from: None,
                        to: None,
                        after_ingestion_seq: None,
                        limit: 100,
                    },
                )
                .await,
            Err(AppError::Forbidden)
        ),
        "reader export with an explicit foreign consumer_id must be forbidden"
    );

    // Writer on search: unfiltered reads stay its own consumer (it ingested,
    // so it finds its own event); an explicit foreign consumer_id is rejected.
    let own = store
        .search(
            &writer,
            &ArchiveSearchRequest {
                limit: 50,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert!(own.iter().any(|event| event.source_event_id == sid));
    assert!(matches!(
        store
            .search(
                &writer,
                &ArchiveSearchRequest {
                    consumer_id: Some(reader.consumer_id),
                    ..Default::default()
                },
                None,
            )
            .await,
        Err(AppError::Forbidden)
    ));

    // Verifier on search/evidence has no cross-consumer visibility at all:
    // its consumer ingested nothing, so both read paths return nothing.
    let verifier = identity(tenant, user, Uuid::new_v4(), TokenRole::Verifier);
    assert!(
        store
            .search(
                &verifier,
                &ArchiveSearchRequest {
                    limit: 50,
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
            .evidence(&verifier, &[event_id])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn reader_cannot_delete_or_advance_mining_state() {
    let store = store().await;
    let reader = identity(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        TokenRole::Reader,
    );
    // Ingestion denial is already pinned above; the widened redacted read
    // lane grants nothing on the deletion or mining-cursor paths.
    assert!(
        matches!(
            store
                .create_deletion(
                    &reader,
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
                .await,
            Err(AppError::Forbidden)
        ),
        "reader cannot create a deletion intent"
    );
    assert!(
        matches!(
            store.run_deletion(&reader, Uuid::new_v4()).await,
            Err(AppError::Forbidden)
        ),
        "reader cannot run a deletion intent"
    );
    assert!(
        matches!(
            store.mining_pending(&reader, "gen-denied", 10).await,
            Err(AppError::Forbidden)
        ),
        "reader cannot read mining windows"
    );
    assert!(
        matches!(
            store.advance_cursor(&reader, "gen-denied", 1).await,
            Err(AppError::Forbidden)
        ),
        "reader cannot advance the mining cursor"
    );
}

#[tokio::test]
#[ignore = "requires TEST_ARCHIVE_DATABASE_URL pointing at disposable PostgreSQL"]
async fn tombstoned_events_stay_invisible_to_reader_reads() {
    let store = store().await;
    let tenant = Uuid::new_v4();
    let user = Uuid::new_v4();
    let writer = identity(tenant, user, Uuid::new_v4(), TokenRole::Writer);
    let reader = identity(tenant, user, Uuid::new_v4(), TokenRole::Reader);
    let owner = identity(tenant, user, Uuid::new_v4(), TokenRole::Owner);
    let sid = format!("tomb-{}", Uuid::new_v4());
    let ack = store
        .ingest_batch(
            &writer,
            &ArchiveIngestRequest {
                events: vec![event(&sid, "expunge needle", 5)],
            },
        )
        .await
        .unwrap();
    let event_id = ack.acks[0].event_id.unwrap();

    // The event() helper files everything under thread-1, so a thread-scoped
    // full deletion covers it.
    let intent = store
        .create_deletion(
            &owner,
            &DeletionRequest {
                request_id: Uuid::new_v4(),
                mode: DeletionMode::Full,
                consumer_id: None,
                project_key: None,
                thread_id: Some("thread-1".into()),
                session_id: None,
                from: None,
                to: None,
            },
        )
        .await
        .unwrap();
    store.run_deletion(&owner, intent.intent_id).await.unwrap();

    // §10's "exclude from every search/read path" guards the widened Reader
    // audience too: tombstoned events stay invisible to reader search/evidence.
    assert!(
        store
            .search(
                &reader,
                &ArchiveSearchRequest {
                    query: Some("expunge".into()),
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
            .evidence(&reader, &[event_id])
            .await
            .unwrap()
            .is_empty()
    );
}
