-- Multi-worker-safe leases for rebuildable embedding work.
ALTER TABLE proposition_embeddings
  ADD COLUMN lease_owner uuid,
  ADD COLUMN lease_until timestamptz,
  ADD CONSTRAINT proposition_embedding_lease_pair CHECK (
    (lease_owner IS NULL AND lease_until IS NULL) OR
    (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
  );

ALTER TABLE observation_embeddings
  ADD COLUMN lease_owner uuid,
  ADD COLUMN lease_until timestamptz,
  ADD CONSTRAINT observation_embedding_lease_pair CHECK (
    (lease_owner IS NULL AND lease_until IS NULL) OR
    (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
  );

CREATE INDEX proposition_embeddings_lease_idx
  ON proposition_embeddings(lease_until) WHERE lease_owner IS NOT NULL;
CREATE INDEX observation_embeddings_lease_idx
  ON observation_embeddings(lease_until) WHERE lease_owner IS NOT NULL;
