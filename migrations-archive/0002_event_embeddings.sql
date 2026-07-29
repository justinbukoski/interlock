-- Generation-scoped vector derivatives for every eligible conversation event.
-- The immutable archive event remains the source of truth; these rows are
-- replaceable and independently rebuildable.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE archive_event_embeddings (
    event_id uuid NOT NULL REFERENCES archive_events(event_id) ON DELETE CASCADE,
    generation_id text NOT NULL CHECK (length(generation_id) BETWEEN 1 AND 256),
    embedding vector(1024),
    embedding_model text,
    embedded_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    quarantined_at timestamptz,
    lease_owner uuid,
    lease_until timestamptz,
    PRIMARY KEY (event_id, generation_id),
    CHECK ((embedding IS NULL AND embedded_at IS NULL) OR
           (embedding IS NOT NULL AND length(embedding_model) BETWEEN 1 AND 128
            AND embedded_at IS NOT NULL)),
    CHECK ((lease_owner IS NULL AND lease_until IS NULL) OR
           (lease_owner IS NOT NULL AND lease_until IS NOT NULL))
);

CREATE FUNCTION queue_archive_event_embedding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.event_kind = 'message' AND NEW.actor IN ('user','assistant') THEN
    INSERT INTO archive_event_embeddings(event_id,generation_id)
    VALUES(NEW.event_id,'chat-bge-large-en-v1.5-v1')
    ON CONFLICT DO NOTHING;
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER archive_events_queue_embedding
AFTER INSERT ON archive_events
FOR EACH ROW EXECUTE FUNCTION queue_archive_event_embedding();

-- Queue existing events when this additive migration is applied.
INSERT INTO archive_event_embeddings(event_id,generation_id)
SELECT event_id,'chat-bge-large-en-v1.5-v1'
FROM archive_events
WHERE event_kind='message' AND actor IN ('user','assistant')
ON CONFLICT DO NOTHING;

CREATE INDEX archive_event_embeddings_hnsw_idx
  ON archive_event_embeddings USING hnsw (embedding vector_cosine_ops)
  WHERE embedding IS NOT NULL;
CREATE INDEX archive_event_embeddings_pending_idx
  ON archive_event_embeddings(next_attempt_at)
  WHERE embedding IS NULL AND quarantined_at IS NULL;
CREATE INDEX archive_event_embeddings_lease_idx
  ON archive_event_embeddings(lease_until)
  WHERE lease_owner IS NOT NULL;
