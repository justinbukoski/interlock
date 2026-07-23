-- Observation and candidate lifecycle. This migration is additive because
-- 0001 has already been published and must remain immutable.

ALTER TABLE observations
    ADD COLUMN request_id uuid,
    ADD COLUMN request_hash bytea,
    ADD COLUMN redaction_count integer NOT NULL DEFAULT 0 CHECK (redaction_count >= 0);

UPDATE observations
SET request_id = md5(tenant_id::text || user_id::text || consumer_id::text || id::text)::uuid,
    request_hash = digest(
        tenant_id::text || user_id::text || consumer_id::text || source_event_id ||
        event_kind || observed_at::text || redacted_content,
        'sha256'
    );

ALTER TABLE observations
    ALTER COLUMN request_id SET NOT NULL,
    ALTER COLUMN request_hash SET NOT NULL,
    ADD CHECK (octet_length(request_hash)=32),
    DROP CONSTRAINT observations_tenant_id_consumer_id_source_event_id_key,
    ADD CONSTRAINT observations_source_event_key UNIQUE (tenant_id,user_id,consumer_id,source_event_id),
    ADD CONSTRAINT observations_request_key UNIQUE (tenant_id,user_id,consumer_id,request_id),
    ADD CONSTRAINT observations_tenant_user_id_key UNIQUE (tenant_id,user_id,id),
    ADD CONSTRAINT observations_tenant_user_consumer_id_key UNIQUE (tenant_id,user_id,consumer_id,id);

CREATE INDEX observations_history_search_idx
    ON observations USING gin(to_tsvector('english',redacted_content));
CREATE INDEX observations_scope_time_idx
    ON observations(scope_id,observed_at DESC,id);

CREATE TABLE candidates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    derivation_key text NOT NULL CHECK (length(derivation_key) BETWEEN 1 AND 512),
    scope_id uuid NOT NULL,
    subject_key text NOT NULL CHECK (length(subject_key) BETWEEN 1 AND 512),
    predicate_key text NOT NULL CHECK (length(predicate_key) BETWEEN 1 AND 128),
    object_value jsonb NOT NULL,
    authority_claim text NOT NULL CHECK (authority_claim IN ('owner_instruction','mechanically_verified','canonical_documentation','repository_state','trusted_agent_report','inference','raw_history')),
    epistemic_status text NOT NULL CHECK (epistemic_status IN ('verified','asserted','inferred','uncertain','disputed')),
    confidence real NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    extractor_model text NOT NULL CHECK (length(extractor_model) BETWEEN 1 AND 256),
    extractor_version text NOT NULL CHECK (length(extractor_version) BETWEEN 1 AND 128),
    prompt_version text NOT NULL CHECK (length(prompt_version) BETWEEN 1 AND 128),
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','accepted','rejected','quarantined','needs_review')),
    transition_mutation_id uuid,
    canonical_proposition_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reviewed_at timestamptz,
    reviewed_by text,
    UNIQUE (tenant_id,user_id,consumer_id,request_id),
    UNIQUE (tenant_id,user_id,consumer_id,derivation_key),
    CONSTRAINT candidates_tenant_user_id_key UNIQUE (tenant_id,user_id,id),
    CONSTRAINT candidates_tenant_user_consumer_id_key UNIQUE (tenant_id,user_id,consumer_id,id),
    FOREIGN KEY (tenant_id,user_id,scope_id) REFERENCES scopes(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,canonical_proposition_id) REFERENCES propositions(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,transition_mutation_id) REFERENCES canonical_mutations(tenant_id,user_id,mutation_id),
    CHECK ((state='accepted' AND canonical_proposition_id IS NOT NULL AND reviewed_at IS NOT NULL AND reviewed_by IS NOT NULL)
        OR (state<>'accepted' AND canonical_proposition_id IS NULL)),
    CHECK ((state='pending' AND transition_mutation_id IS NULL) OR (state<>'pending' AND transition_mutation_id IS NOT NULL))
);

CREATE TABLE candidate_evidence (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    observation_id uuid NOT NULL,
    PRIMARY KEY (candidate_id,observation_id),
    FOREIGN KEY (tenant_id,user_id,consumer_id,candidate_id) REFERENCES candidates(tenant_id,user_id,consumer_id,id),
    FOREIGN KEY (tenant_id,user_id,consumer_id,observation_id) REFERENCES observations(tenant_id,user_id,consumer_id,id)
);

CREATE TABLE observation_outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    observation_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('observation_ingested')),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    completed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    UNIQUE (tenant_id,user_id,consumer_id,observation_id,event_type),
    FOREIGN KEY (tenant_id,user_id,consumer_id,observation_id)
        REFERENCES observations(tenant_id,user_id,consumer_id,id)
);

CREATE TABLE candidate_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    mutation_id uuid NOT NULL,
    actor text NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('accepted','rejected','quarantined','needs_review')),
    reason text NOT NULL,
    proposition_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,user_id,candidate_id,mutation_id),
    FOREIGN KEY (tenant_id,user_id,candidate_id) REFERENCES candidates(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,mutation_id) REFERENCES canonical_mutations(tenant_id,user_id,mutation_id),
    FOREIGN KEY (tenant_id,user_id,proposition_id) REFERENCES propositions(tenant_id,user_id,id)
);

CREATE TRIGGER observations_immutable BEFORE UPDATE OR DELETE ON observations
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
CREATE TRIGGER candidate_evidence_immutable BEFORE UPDATE OR DELETE ON candidate_evidence
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
CREATE TRIGGER candidate_events_immutable BEFORE UPDATE OR DELETE ON candidate_events
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE FUNCTION enforce_candidate_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state <> 'pending'
    OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
    OR NEW.user_id IS DISTINCT FROM OLD.user_id
    OR NEW.consumer_id IS DISTINCT FROM OLD.consumer_id
    OR NEW.request_id IS DISTINCT FROM OLD.request_id
    OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
    OR NEW.derivation_key IS DISTINCT FROM OLD.derivation_key
    OR NEW.scope_id IS DISTINCT FROM OLD.scope_id
    OR NEW.subject_key IS DISTINCT FROM OLD.subject_key
    OR NEW.predicate_key IS DISTINCT FROM OLD.predicate_key
    OR NEW.object_value IS DISTINCT FROM OLD.object_value
    OR NEW.authority_claim IS DISTINCT FROM OLD.authority_claim
    OR NEW.epistemic_status IS DISTINCT FROM OLD.epistemic_status
    OR NEW.confidence IS DISTINCT FROM OLD.confidence
    OR NEW.extractor_model IS DISTINCT FROM OLD.extractor_model
    OR NEW.extractor_version IS DISTINCT FROM OLD.extractor_version
    OR NEW.prompt_version IS DISTINCT FROM OLD.prompt_version
    OR NEW.created_at IS DISTINCT FROM OLD.created_at
    OR NEW.state = 'pending'
    OR NEW.reviewed_at IS NULL
    OR NEW.reviewed_by IS NULL THEN
    RAISE EXCEPTION 'candidate content is immutable and may transition once';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER candidates_transition_guard BEFORE UPDATE ON candidates
FOR EACH ROW EXECUTE FUNCTION enforce_candidate_transition();

CREATE FUNCTION validate_candidate_transition_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM candidate_events e
    WHERE e.tenant_id=NEW.tenant_id AND e.user_id=NEW.user_id
      AND e.candidate_id=NEW.id AND e.event_type=NEW.state
      AND e.mutation_id=NEW.transition_mutation_id
      AND (NEW.state<>'accepted' OR e.proposition_id=NEW.canonical_proposition_id)
  ) THEN
    RAISE EXCEPTION 'candidate transition % has no matching immutable event', NEW.id;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER candidates_transition_event_guard
AFTER UPDATE OF state ON candidates DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state)
EXECUTE FUNCTION validate_candidate_transition_event();
