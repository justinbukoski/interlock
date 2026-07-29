-- Embeddings are rebuildable derived data, kept separate from immutable source rows.
CREATE EXTENSION IF NOT EXISTS vector;

DO $$
DECLARE installed text;
BEGIN
  SELECT extversion INTO installed FROM pg_extension WHERE extname = 'vector';
  IF installed IS NULL OR string_to_array(installed, '.')::int[] < ARRAY[0,8,2] THEN
    RAISE EXCEPTION 'Interlock requires pgvector >= 0.8.2 (installed: %)', installed;
  END IF;
END $$;

CREATE TABLE proposition_embeddings (
  proposition_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  user_id uuid NOT NULL,
  embedding vector(1024),
  embedding_model text,
  embedded_at timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_error text,
  quarantined_at timestamptz,
  FOREIGN KEY (tenant_id,user_id,proposition_id) REFERENCES propositions(tenant_id,user_id,id),
  CHECK ((embedding IS NULL AND embedded_at IS NULL) OR
         (embedding IS NOT NULL AND length(embedding_model) BETWEEN 1 AND 128 AND embedded_at IS NOT NULL))
);

CREATE TABLE observation_embeddings (
  observation_id uuid PRIMARY KEY,
  tenant_id uuid NOT NULL,
  user_id uuid NOT NULL,
  embedding vector(1024),
  embedding_model text,
  embedded_at timestamptz,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  last_error text,
  quarantined_at timestamptz,
  FOREIGN KEY (tenant_id,user_id,observation_id) REFERENCES observations(tenant_id,user_id,id),
  CHECK ((embedding IS NULL AND embedded_at IS NULL) OR
         (embedding IS NOT NULL AND length(embedding_model) BETWEEN 1 AND 128 AND embedded_at IS NOT NULL))
);

INSERT INTO proposition_embeddings(proposition_id,tenant_id,user_id)
SELECT id,tenant_id,user_id FROM propositions ON CONFLICT DO NOTHING;
INSERT INTO observation_embeddings(observation_id,tenant_id,user_id)
SELECT id,tenant_id,user_id FROM observations ON CONFLICT DO NOTHING;

CREATE FUNCTION queue_proposition_embedding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO proposition_embeddings(proposition_id,tenant_id,user_id)
  VALUES(NEW.id,NEW.tenant_id,NEW.user_id) ON CONFLICT DO NOTHING;
  RETURN NEW;
END $$;
CREATE TRIGGER propositions_queue_embedding AFTER INSERT ON propositions
FOR EACH ROW EXECUTE FUNCTION queue_proposition_embedding();

CREATE FUNCTION queue_observation_embedding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO observation_embeddings(observation_id,tenant_id,user_id)
  VALUES(NEW.id,NEW.tenant_id,NEW.user_id) ON CONFLICT DO NOTHING;
  RETURN NEW;
END $$;
CREATE TRIGGER observations_queue_embedding AFTER INSERT ON observations
FOR EACH ROW EXECUTE FUNCTION queue_observation_embedding();

CREATE INDEX proposition_embeddings_hnsw_idx
  ON proposition_embeddings USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL;
CREATE INDEX observation_embeddings_hnsw_idx
  ON observation_embeddings USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL;
CREATE INDEX proposition_embeddings_pending_idx ON proposition_embeddings(next_attempt_at)
  WHERE embedding IS NULL AND quarantined_at IS NULL;
CREATE INDEX observation_embeddings_pending_idx ON observation_embeddings(next_attempt_at)
  WHERE embedding IS NULL AND quarantined_at IS NULL;
