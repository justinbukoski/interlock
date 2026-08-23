-- The deletion saga's raw-purge step releases raw payload references by
-- setting raw_content_ref to NULL. The append-only guard from 0001 must permit
-- exactly that transition — releasing a reference is the archive-side raw
-- purge — while every other mutation stays forbidden. The transition is
-- one-way: NULL -> non-NULL (resurrecting a released reference) still raises.
CREATE OR REPLACE FUNCTION archive_events_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
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
    OR (NEW.raw_content_ref IS DISTINCT FROM OLD.raw_content_ref
        AND NOT (OLD.raw_content_ref IS NOT NULL AND NEW.raw_content_ref IS NULL))
    OR NEW.source_timestamp IS DISTINCT FROM OLD.source_timestamp
    OR NEW.ingested_at IS DISTINCT FROM OLD.ingested_at
    OR NEW.capture_adapter_version IS DISTINCT FROM OLD.capture_adapter_version THEN
    RAISE EXCEPTION 'archive events are append-only; only tombstoning or raw-reference release may change a row';
  END IF;
  RETURN NEW;
END $$;
