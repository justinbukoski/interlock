-- Foreman Memory v6 owns this schema inside its dedicated database.
-- Run this migration only against a dedicated Foreman database.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE scopes (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid,
    project_key text,
    repository_key text,
    thread_id text,
    session_id text,
    scope_level text NOT NULL CHECK (scope_level IN ('global','user','project','repository','thread','session')),
    specificity smallint GENERATED ALWAYS AS (
        CASE scope_level WHEN 'session' THEN 6 WHEN 'thread' THEN 5 WHEN 'repository' THEN 4
             WHEN 'project' THEN 3 WHEN 'user' THEN 2 ELSE 1 END
    ) STORED,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT scopes_session_requires_thread CHECK (session_id IS NULL OR thread_id IS NOT NULL),
    CONSTRAINT scopes_identity_key UNIQUE NULLS NOT DISTINCT
        (tenant_id,user_id,consumer_id,project_key,repository_key,thread_id,session_id),
    CONSTRAINT scopes_tenant_user_id_key UNIQUE (tenant_id,user_id,id)
);

CREATE TABLE predicates (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    key text NOT NULL UNIQUE,
    cardinality text NOT NULL CHECK (cardinality IN ('single','set')),
    value_type text NOT NULL CHECK (value_type IN ('string','number','boolean','object','array','any')),
    minimum_authority_rank smallint NOT NULL CHECK (minimum_authority_rank BETWEEN 1 AND 7),
    owner_confirmation_required boolean NOT NULL DEFAULT false,
    mandatory_bootstrap boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO predicates(key,cardinality,value_type,minimum_authority_rank,owner_confirmation_required,mandatory_bootstrap)
VALUES
 ('system.constraint','set','string',1,true,true),
 ('system.directive','set','string',1,true,true),
 ('project.state','single','object',4,false,false)
ON CONFLICT DO NOTHING;

CREATE TABLE observations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    source_event_id text NOT NULL,
    event_kind text NOT NULL,
    actor text NOT NULL,
    scope_id uuid,
    observed_at timestamptz NOT NULL,
    redacted_content text NOT NULL,
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256)=32),
    raw_content_ref text,
    schema_version integer NOT NULL DEFAULT 1,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id,consumer_id,source_event_id),
    FOREIGN KEY (tenant_id,user_id,scope_id) REFERENCES scopes(tenant_id,user_id,id)
);

CREATE TABLE canonical_mutations (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    mutation_id uuid NOT NULL,
    actor text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id,user_id,mutation_id)
);

CREATE TABLE propositions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    writer_consumer_id uuid NOT NULL,
    scope_id uuid NOT NULL,
    subject_key text NOT NULL,
    predicate_id uuid NOT NULL REFERENCES predicates(id),
    cardinality text NOT NULL CHECK (cardinality IN ('single','set')),
    object_value jsonb NOT NULL,
    object_hash bytea GENERATED ALWAYS AS (digest(object_value::text,'sha256')) STORED,
    rendered text NOT NULL,
    search_document tsvector GENERATED ALWAYS AS (to_tsvector('english',rendered)) STORED,
    authority text NOT NULL CHECK (authority IN ('owner_instruction','mechanically_verified','canonical_documentation','repository_state','trusted_agent_report','inference','raw_history')),
    authority_rank smallint NOT NULL CHECK (authority_rank BETWEEN 1 AND 7),
    epistemic_status text NOT NULL CHECK (epistemic_status IN ('verified','asserted','inferred','uncertain','disputed')),
    source_type text NOT NULL CHECK (length(source_type) BETWEEN 1 AND 64),
    source_ref text NOT NULL CHECK (length(source_ref) BETWEEN 1 AND 2048),
    last_mutation_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('current','superseded','invalid','disputed','quarantined')),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    recorded_at timestamptz NOT NULL,
    CHECK ((status='current' AND valid_to IS NULL) OR (status<>'current' AND valid_to IS NOT NULL)),
    CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CHECK (authority_rank = CASE authority
      WHEN 'owner_instruction' THEN 1 WHEN 'mechanically_verified' THEN 2
      WHEN 'canonical_documentation' THEN 3 WHEN 'repository_state' THEN 4
      WHEN 'trusted_agent_report' THEN 5 WHEN 'inference' THEN 6 WHEN 'raw_history' THEN 7 END),
    CONSTRAINT propositions_tenant_user_id_key UNIQUE (tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,scope_id) REFERENCES scopes(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,last_mutation_id) REFERENCES canonical_mutations(tenant_id,user_id,mutation_id)
);

CREATE UNIQUE INDEX propositions_one_current_single
    ON propositions(scope_id,subject_key,predicate_id)
    WHERE status='current' AND cardinality='single';
CREATE UNIQUE INDEX propositions_one_current_set_member
    ON propositions(scope_id,subject_key,predicate_id,object_hash)
    WHERE status='current' AND cardinality='set';
CREATE INDEX propositions_search_idx ON propositions USING gin(search_document);
CREATE INDEX propositions_current_scope_idx ON propositions(scope_id,authority_rank,recorded_at DESC) WHERE status='current';

CREATE FUNCTION enforce_predicate_cardinality() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected text;
BEGIN
  SELECT cardinality INTO expected FROM predicates WHERE id=NEW.predicate_id;
  IF expected IS NULL OR expected <> NEW.cardinality THEN
    RAISE EXCEPTION 'proposition cardinality does not match predicate registry';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER propositions_cardinality_guard BEFORE INSERT OR UPDATE OF predicate_id,cardinality
ON propositions FOR EACH ROW EXECUTE FUNCTION enforce_predicate_cardinality();

CREATE TABLE proposition_edges (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    from_id uuid NOT NULL,
    to_id uuid NOT NULL,
    edge_type text NOT NULL CHECK (edge_type IN ('supersedes','corrects','supports','contradicts','narrows','derived_from')),
    mutation_id uuid NOT NULL,
    reason text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(from_id,to_id,edge_type),
    FOREIGN KEY (tenant_id,user_id,from_id) REFERENCES propositions(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,to_id) REFERENCES propositions(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,mutation_id) REFERENCES canonical_mutations(tenant_id,user_id,mutation_id)
);

CREATE TABLE handoffs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    project_key text NOT NULL,
    content text NOT NULL,
    session_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    UNIQUE(tenant_id,user_id,consumer_id,request_id)
);
CREATE INDEX handoffs_latest_idx ON handoffs(tenant_id,user_id,consumer_id,project_key,created_at DESC);

CREATE TABLE audit_events (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    actor text NOT NULL,
    event_type text NOT NULL,
    after_id uuid,
    before_ids uuid[] NOT NULL DEFAULT '{}',
    reason text NOT NULL,
    request_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (tenant_id,user_id,after_id) REFERENCES propositions(tenant_id,user_id,id),
    FOREIGN KEY (tenant_id,user_id,request_id) REFERENCES canonical_mutations(tenant_id,user_id,mutation_id),
    UNIQUE (tenant_id,user_id,request_id)
);

CREATE TABLE outbox (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    event_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_at timestamptz,
    completed_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    FOREIGN KEY (tenant_id,user_id,aggregate_id) REFERENCES propositions(tenant_id,user_id,id)
);

CREATE TABLE snapshot_revisions (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0)
    ,PRIMARY KEY(tenant_id,user_id)
);

CREATE TABLE canonical_requests (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    consumer_id uuid NOT NULL,
    request_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    response jsonb NOT NULL,
    actor text NOT NULL,
    role text NOT NULL CHECK (role IN ('reader','writer','verifier','owner')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(tenant_id,user_id,consumer_id,request_id)
);

CREATE FUNCTION reject_immutable_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% rows are immutable', TG_TABLE_NAME;
END $$;
CREATE TRIGGER canonical_mutations_immutable BEFORE UPDATE OR DELETE ON canonical_mutations
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
CREATE TRIGGER proposition_edges_immutable BEFORE UPDATE OR DELETE ON proposition_edges
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
CREATE TRIGGER audit_events_immutable BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE FUNCTION enforce_proposition_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
    OR NEW.user_id IS DISTINCT FROM OLD.user_id
    OR NEW.writer_consumer_id IS DISTINCT FROM OLD.writer_consumer_id
    OR NEW.scope_id IS DISTINCT FROM OLD.scope_id
    OR NEW.subject_key IS DISTINCT FROM OLD.subject_key
    OR NEW.predicate_id IS DISTINCT FROM OLD.predicate_id
    OR NEW.cardinality IS DISTINCT FROM OLD.cardinality
    OR NEW.object_value IS DISTINCT FROM OLD.object_value
    OR NEW.rendered IS DISTINCT FROM OLD.rendered
    OR NEW.authority IS DISTINCT FROM OLD.authority
    OR NEW.authority_rank IS DISTINCT FROM OLD.authority_rank
    OR NEW.epistemic_status IS DISTINCT FROM OLD.epistemic_status
    OR NEW.source_type IS DISTINCT FROM OLD.source_type
    OR NEW.source_ref IS DISTINCT FROM OLD.source_ref
    OR NEW.valid_from IS DISTINCT FROM OLD.valid_from
    OR NEW.recorded_at IS DISTINCT FROM OLD.recorded_at THEN
    RAISE EXCEPTION 'canonical propositions are append-only';
  END IF;
  IF OLD.status <> 'current'
    OR NEW.status = 'current'
    OR OLD.valid_to IS NOT NULL
    OR NEW.valid_to IS NULL
    OR NEW.last_mutation_id = OLD.last_mutation_id THEN
    RAISE EXCEPTION 'only one atomic current-to-noncurrent transition is permitted';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER propositions_append_only BEFORE UPDATE ON propositions
FOR EACH ROW EXECUTE FUNCTION enforce_proposition_append_only();

CREATE FUNCTION validate_proposition_insert_audit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM audit_events a
    WHERE a.tenant_id=NEW.tenant_id AND a.user_id=NEW.user_id AND a.after_id=NEW.id
      AND a.request_id=NEW.last_mutation_id AND a.event_type='canonical_write'
  ) THEN
    RAISE EXCEPTION 'canonical proposition % has no matching audit event', NEW.id;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER propositions_insert_audit_guard
AFTER INSERT ON propositions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_proposition_insert_audit();

CREATE FUNCTION validate_proposition_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.status <> 'current' THEN
    IF NOT EXISTS (
      SELECT 1 FROM audit_events a
      WHERE a.tenant_id=NEW.tenant_id AND a.user_id=NEW.user_id
        AND a.request_id=NEW.last_mutation_id AND OLD.id=ANY(a.before_ids)
        AND a.event_type IN ('canonical_write','canonical_invalidate','canonical_dispute')
    ) THEN
      RAISE EXCEPTION 'proposition transition % has no matching audit event', NEW.id;
    END IF;
  END IF;
  IF NEW.status = 'superseded' AND NOT EXISTS (
    SELECT 1 FROM proposition_edges e JOIN audit_events a
      ON a.tenant_id=e.tenant_id AND a.user_id=e.user_id
      AND a.request_id=e.mutation_id AND a.after_id=e.from_id
    WHERE e.tenant_id=NEW.tenant_id AND e.user_id=NEW.user_id
      AND e.mutation_id=NEW.last_mutation_id AND e.to_id=NEW.id
      AND e.edge_type IN ('supersedes','corrects') AND a.event_type='canonical_write'
  ) THEN
    RAISE EXCEPTION 'superseded proposition % has no structural edge', NEW.id;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER propositions_transition_guard
AFTER UPDATE OF status,valid_to ON propositions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (OLD.status IS DISTINCT FROM NEW.status OR OLD.valid_to IS DISTINCT FROM NEW.valid_to)
EXECUTE FUNCTION validate_proposition_transition();

CREATE FUNCTION validate_audit_before_ids() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE proposition_id uuid;
BEGIN
  FOREACH proposition_id IN ARRAY NEW.before_ids LOOP
    IF NOT EXISTS (
      SELECT 1 FROM propositions p
      WHERE p.tenant_id=NEW.tenant_id AND p.user_id=NEW.user_id AND p.id=proposition_id
    ) THEN
      RAISE EXCEPTION 'audit before_id % crosses tenant/user boundary or is missing', proposition_id;
    END IF;
  END LOOP;
  RETURN NEW;
END $$;
CREATE TRIGGER audit_before_ids_guard BEFORE INSERT OR UPDATE OF before_ids,tenant_id,user_id
ON audit_events FOR EACH ROW EXECUTE FUNCTION validate_audit_before_ids();

-- Structural lane separation: no FK or trigger connects handoffs to observations,
-- propositions, predicates, or outbox. Promotion requires a new explicit write.
