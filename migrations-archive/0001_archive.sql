-- Foreman Memory 6.5 conversation archive. This schema lives in its OWN
-- PostgreSQL database, separate from the v6 canonical-memory database. It is the
-- irreplaceable continuity source: append-only, lossless, and replayable.
-- Never run this migration against the v5 or the v6 canonical database.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- The event source. Every captured conversation event from every adapter lands
-- here exactly once. Rows are append-only; deletion is an explicit owner saga
-- that tombstones rather than mutating history in place.
CREATE TABLE archive_events (
    event_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Server-assigned ingestion order. The mining cursor advances over this,
    -- never over source_timestamp or adapter sequence, so a late-arriving event
    -- always appears after the cursor.
    ingestion_seq bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    installation_id uuid NOT NULL,
    source_event_id text NOT NULL CHECK (length(source_event_id) BETWEEN 1 AND 512),
    project_key text CHECK (project_key IS NULL OR length(project_key) BETWEEN 1 AND 512),
    repository_key text CHECK (repository_key IS NULL OR length(repository_key) BETWEEN 1 AND 512),
    thread_id text CHECK (thread_id IS NULL OR length(thread_id) BETWEEN 1 AND 512),
    session_id text CHECK (session_id IS NULL OR length(session_id) BETWEEN 1 AND 512),
    turn_id text CHECK (turn_id IS NULL OR length(turn_id) BETWEEN 1 AND 512),
    -- Monotonic WITHIN a thread only, never globally (design §5.1).
    sequence_number bigint,
    actor text NOT NULL CHECK (actor IN ('user','assistant','tool','system','application')),
    event_kind text NOT NULL CHECK (event_kind IN
        ('message','tool_request','tool_result','attachment_ref','correction','deletion_marker','session_lifecycle')),
    content_type text NOT NULL CHECK (length(content_type) BETWEEN 1 AND 128),
    schema_version integer NOT NULL DEFAULT 1 CHECK (schema_version >= 1),
    redacted_content text NOT NULL,
    redaction_count integer NOT NULL DEFAULT 0 CHECK (redaction_count >= 0),
    raw_content_ref text,
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    -- Every hash records its algorithm so a later migration cannot invalidate
    -- old evidence; rolling restore comparisons group by algorithm.
    content_hash_alg text NOT NULL DEFAULT 'sha256' CHECK (content_hash_alg IN ('sha256')),
    source_timestamp timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    capture_adapter_version text NOT NULL CHECK (length(capture_adapter_version) BETWEEN 1 AND 128),
    -- Deletion tombstone. When set, the redacted content is excluded from every
    -- search and read path immediately, before downstream derivatives are purged.
    tombstoned_at timestamptz,
    tombstone_intent_id uuid,
    -- Adapters whose native IDs are thread/session-local synthesize a globally
    -- unique source_event_id; conformance tests exercise cross-session collisions.
    CONSTRAINT archive_events_source_key UNIQUE (tenant_id, consumer_id, installation_id, source_event_id)
);

-- Deterministic multi-thread assembly order is (source_timestamp,
-- ingestion order, event_id). Search excludes tombstoned rows.
CREATE INDEX archive_events_search_idx
    ON archive_events USING gin (to_tsvector('english', redacted_content))
    WHERE tombstoned_at IS NULL;
CREATE INDEX archive_events_scope_idx
    ON archive_events (tenant_id, user_id, consumer_id, source_timestamp DESC, event_id);
CREATE INDEX archive_events_thread_idx
    ON archive_events (tenant_id, user_id, thread_id, sequence_number, source_timestamp);
CREATE INDEX archive_events_ingestion_idx
    ON archive_events (tenant_id, user_id, ingestion_seq);
CREATE INDEX archive_events_project_idx
    ON archive_events (tenant_id, user_id, project_key, source_timestamp DESC)
    WHERE project_key IS NOT NULL;

-- Archive events are append-only. A row may only ever be tombstoned (soft
-- delete) or hard-deleted by the owner deletion saga; nothing else may change.
CREATE FUNCTION archive_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.event_id IS DISTINCT FROM OLD.event_id
    OR NEW.ingestion_seq IS DISTINCT FROM OLD.ingestion_seq
    OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
    OR NEW.user_id IS DISTINCT FROM OLD.user_id
    OR NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
    OR NEW.installation_id IS DISTINCT FROM OLD.installation_id
    OR NEW.source_event_id IS DISTINCT FROM OLD.source_event_id
    OR NEW.project_key IS DISTINCT FROM OLD.project_key
    OR NEW.repository_key IS DISTINCT FROM OLD.repository_key
    OR NEW.thread_id IS DISTINCT FROM OLD.thread_id
    OR NEW.session_id IS DISTINCT FROM OLD.session_id
    OR NEW.turn_id IS DISTINCT FROM OLD.turn_id
    OR NEW.sequence_number IS DISTINCT FROM OLD.sequence_number
    OR NEW.actor IS DISTINCT FROM OLD.actor
    OR NEW.event_kind IS DISTINCT FROM OLD.event_kind
    OR NEW.content_type IS DISTINCT FROM OLD.content_type
    OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
    OR NEW.content_hash IS DISTINCT FROM OLD.content_hash
    OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
    OR NEW.content_hash_alg IS DISTINCT FROM OLD.content_hash_alg
    OR NEW.redacted_content IS DISTINCT FROM OLD.redacted_content
    OR NEW.redaction_count IS DISTINCT FROM OLD.redaction_count
    OR NEW.raw_content_ref IS DISTINCT FROM OLD.raw_content_ref
    OR NEW.source_timestamp IS DISTINCT FROM OLD.source_timestamp
    OR NEW.ingested_at IS DISTINCT FROM OLD.ingested_at
    OR NEW.capture_adapter_version IS DISTINCT FROM OLD.capture_adapter_version THEN
    RAISE EXCEPTION 'archive events are append-only; only tombstoning may change a row';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER archive_events_append_only_guard BEFORE UPDATE ON archive_events
FOR EACH ROW EXECUTE FUNCTION archive_events_append_only();

-- Per-generation mining cursor. A generation pins the archive schema version,
-- redaction ruleset, extractor model/prompt, predicate registry revision, and
-- canonicalization rules revision. The cursor is a server-assigned ingestion
-- sequence, so replay is deterministic and late events are never skipped.
CREATE TABLE mining_cursors (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    generation_id text NOT NULL CHECK (length(generation_id) BETWEEN 1 AND 256),
    cursor_seq bigint NOT NULL DEFAULT 0 CHECK (cursor_seq >= 0),
    archive_schema_version integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, user_id, generation_id)
);

-- Owner deletion intents. Deletion is an idempotent, resumable cross-database
-- saga, never a cross-database transaction. This ledger records the intent and
-- the archive-side step progress; a reconciliation worker re-drives incomplete
-- intents to completion. Canonical-side closure is recorded by its own steps.
CREATE TABLE deletion_intents (
    intent_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    requested_by text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('full','raw_only')),
    -- Selection filter. NULL dimensions widen the selection within tenant/user.
    filter_consumer_id uuid,
    filter_project_key text,
    filter_thread_id text,
    filter_session_id text,
    filter_from timestamptz,
    filter_to timestamptz,
    -- Saga step progress. Each step is individually durable and idempotent.
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    archive_tombstoned_at timestamptz,
    raw_purged_at timestamptz,
    derivatives_purged_at timestamptz,
    candidates_invalidated_at timestamptz,
    canonical_reviewed_at timestamptz,
    audit_appended_at timestamptz,
    completed_at timestamptz,
    tombstoned_event_count bigint NOT NULL DEFAULT 0
);
CREATE INDEX deletion_intents_incomplete_idx
    ON deletion_intents (tenant_id, user_id, created_at)
    WHERE completed_at IS NULL;

-- Privacy audit trail. Contains no deleted content, only identifiers, counts,
-- and step transitions.
CREATE TABLE deletion_audit (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    intent_id uuid NOT NULL REFERENCES deletion_intents(intent_id),
    actor text NOT NULL,
    step text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX deletion_audit_intent_idx ON deletion_audit (intent_id, created_at);

CREATE FUNCTION deletion_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'deletion_audit rows are immutable';
END $$;
CREATE TRIGGER deletion_audit_immutable_guard BEFORE UPDATE OR DELETE ON deletion_audit
FOR EACH ROW EXECUTE FUNCTION deletion_audit_immutable();
