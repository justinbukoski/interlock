-- Interlock 6.5 continuity handoff repair (design §9.1). Handoffs become a
-- first-class lifecycle subsystem with exact typed context keys and
-- compare-and-swap supersession. They live in a DEDICATED schema so a later
-- operational split can move them to their own database without changing the
-- API. There are deliberately NO foreign keys or triggers from these tables into
-- candidates or propositions: a handoff has no database path into mining or
-- canonicalization. The original public.handoffs table remains for compatibility
-- with the existing `/v6/handoffs` route.

CREATE SCHEMA IF NOT EXISTS continuity;

CREATE FUNCTION continuity.reject_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'continuity.% rows are immutable', TG_TABLE_NAME;
END $$;

-- A typed continuation context. The key is NEVER an arbitrary filesystem path;
-- it is a normalized repository/worktree identity, a durable project ID, a
-- thread identity, or an installation-scoped projectless context. The active
-- pointer implements compare-and-swap supersession.
CREATE TABLE continuity.contexts (
    context_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    context_type text NOT NULL CHECK (context_type IN
        ('repository_worktree','durable_project','thread','installation_projectless')),
    context_key text NOT NULL CHECK (length(context_key) BETWEEN 1 AND 1024),
    -- Cross-application projectless continuity requires an explicit stable
    -- family_id declared by an owner or trusted lifecycle adapter; it is never
    -- inferred from semantic similarity.
    family_id text CHECK (family_id IS NULL OR length(family_id) BETWEEN 1 AND 256),
    active_handoff_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT contexts_identity_key UNIQUE (tenant_id, user_id, context_type, context_key)
);

CREATE TABLE continuity.handoffs (
    handoff_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    context_id uuid NOT NULL REFERENCES continuity.contexts(context_id),
    producing_consumer_id uuid NOT NULL,
    producing_thread_id text,
    producing_session_id text NOT NULL CHECK (length(producing_session_id) BETWEEN 1 AND 256),
    summary text NOT NULL,
    content jsonb NOT NULL,
    predecessor_handoff_id uuid REFERENCES continuity.handoffs(handoff_id),
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','superseded','acknowledged','completed','expired','invalid')),
    source_snapshot_revision bigint NOT NULL DEFAULT 0,
    content_hash bytea NOT NULL CHECK (octet_length(content_hash) = 32),
    request_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL CHECK (expires_at > created_at),
    closed_at timestamptz,
    CONSTRAINT handoffs_request_key UNIQUE (tenant_id, user_id, producing_consumer_id, request_id)
);

-- Structural guarantee: at most one active handoff per exact context key.
CREATE UNIQUE INDEX handoffs_one_active_per_context
    ON continuity.handoffs (context_id) WHERE status = 'active';
CREATE INDEX handoffs_context_history_idx
    ON continuity.handoffs (context_id, created_at DESC);

-- Actionable continuation items with stable IDs, so the next agent can complete
-- individual items without rewriting unrelated content.
CREATE TABLE continuity.handoff_items (
    item_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    handoff_id uuid NOT NULL REFERENCES continuity.handoffs(handoff_id),
    item_kind text NOT NULL CHECK (item_kind IN ('in_progress','next_action','blocker')),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    text text NOT NULL CHECK (length(text) BETWEEN 1 AND 8192),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','completed')),
    completed_at timestamptz,
    completed_by text,
    CHECK ((status='completed' AND completed_at IS NOT NULL) OR (status='open' AND completed_at IS NULL))
);
CREATE INDEX handoff_items_handoff_idx ON continuity.handoff_items (handoff_id, ordinal);

-- Per-consumer receipt. Acknowledgement is idempotent and survives client
-- crashes; repeated acknowledgement is a no-op and never rewrites the first
-- receipt time. It does not make the handoff canonical or delete it.
CREATE TABLE continuity.acknowledgements (
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    handoff_id uuid NOT NULL REFERENCES continuity.handoffs(handoff_id),
    consumer_id uuid NOT NULL,
    acknowledged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    session_id text NOT NULL,
    PRIMARY KEY (handoff_id, consumer_id)
);
CREATE TRIGGER acknowledgements_immutable
    BEFORE UPDATE OR DELETE ON continuity.acknowledgements
    FOR EACH ROW EXECUTE FUNCTION continuity.reject_mutation();

-- The active pointer references the current active handoff.
ALTER TABLE continuity.contexts
    ADD CONSTRAINT contexts_active_handoff_fk
    FOREIGN KEY (active_handoff_id) REFERENCES continuity.handoffs(handoff_id)
    DEFERRABLE INITIALLY DEFERRED;
