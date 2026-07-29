# Interlock Architecture

Status: design baseline, 2026-07-19. Quality takes precedence over schedule.

## Purpose

Interlock supplies compact, current, attributable context to coding agents
without flooding model context or allowing stale statements to compete with
their corrections. PostgreSQL is the durable authority. Memory-resident views
are derived accelerators and can always be rebuilt.

## Service boundary

Each installation runs on its own port, containers, PostgreSQL database, and
persistent volume. It does not share a database with another application.

## Four lanes

1. **Observations** — immutable, redacted interaction events with consumer,
   user, project, repository, thread, session, source, and timestamps.
2. **Candidates** — extractor output awaiting deterministic deduplication,
   conflict analysis, verification, or human review.
3. **Canonical memory** — versioned propositions and documents with authority,
   epistemic status, provenance, validity interval, and structural supersession.
4. **History** — raw conversational search, explicitly requested and always
   labeled low-authority; it never competes in canonical recall.

Handoffs remain a fifth, short-half-life continuation lane. They are selected by
exact project identity and never promoted automatically.

## Identity and scope

Every record has `tenant`, `user`, `consumer`, `project`, `repository`,
`thread`, and `session` dimensions where applicable. Retrieval walks a fixed
specificity chain:

`session -> thread -> repository -> project -> user -> global`

Writes default to the narrowest known scope. Global promotion is an audited
operation requiring an explicit rule or approval.

## Canonical authority

A proposition is keyed by normalized subject, predicate, scope, and authority
domain. Single-cardinality predicates have exactly one current canonical value.
Set predicates can have multiple current values. Corrections close the prior
validity interval and link `supersedes_id`; current recall excludes closed,
invalid, quarantined, and disputed rows unless the caller asks for history.

Authority order is deterministic: owner instruction, mechanically verified
observation, canonical documentation, repository state, trusted agent report,
inference, raw history. Recency breaks ties only inside the same authority tier.

## Read API

`POST /v6/bootstrap` returns a deterministic, cacheable packet containing only
applicable hard rules, current project state, and the latest handoff. It is
called once per task/session.

`POST /v6/recall` accepts query, identity, intent, per-lane limits, and a hard
token budget. Intent is one of `current`, `why`, `history`, `procedure`, or
`explore`; server classification is available but reported to the caller.

Retrieval performs lexical and vector candidate generation, scope filtering,
canonical-state filtering, temporal/authority reranking, duplicate collapse,
and maximal-marginal-relevance selection. The final serializer enforces token
budget after rendering; exceeding it is a test failure, not best effort.

Every item includes source, scope, authority, epistemic status, observed time,
validity, and stable ID. Conflicts are returned as conflicts—not silently mixed.

## Write API

`POST /v6/observe` is the only routine ingestion endpoint. Explicit durable
writes use `POST /v6/memories` with provenance and intended scope. Mutation,
promotion, verification, supersession, and invalidation are audited operations.

Raw secret-bearing content is encrypted separately with bounded retention.
Searchable content is synchronously redacted before indexing. Authentication is
required on every route, including history ingestion.

## Consolidation

Workers are idempotent and replayable. Extraction, normalization, verification,
conflict resolution, project-state synthesis, and reflection are separate jobs
with durable queues and poison-item quarantine. No LLM job can directly mutate
canonical rows; it proposes candidates consumed by deterministic transactions.

## Memory sizing and tuning

Additional RAM keeps PostgreSQL's active corpus and indexes hot and can support
rebuildable in-process snapshots of global policy, per-project canonical state,
predicate registry, and embedding centroids. Tuning should remain conservative
and measurement-driven. A separate cache database is unnecessary until
benchmarks show PostgreSQL cannot meet the intended latency target.

## Client integration

Codex receives a first-class MCP server with `bootstrap`, `recall`, `history`,
`observe`, `remember`, `correct`, and `handoff` tools. A lifecycle adapter opens
and closes sessions and observes every turn. `AGENTS.md` contains only the
fail-safe contract and tool-use policy.

Other clients use the same API and may add richer inspection interfaces for
source, freshness, scope, correction, promotion, and review queues. No client
owns memory semantics.

## The 6.5 continuity plane

Version 6.5 hardens the continuation lanes into a first-class subsystem.

**Handoff lifecycle.** Handoffs are selected by a typed `context_key`, never by
an arbitrary filesystem path. Supersession is a compare-and-swap on a
per-context active pointer, so two agents cannot both win and no handoff is
silently lost. The lifecycle API (`/v6.5/handoff/*`) covers write, exact get,
acknowledge, per-item completion, close, history, and context validation.
Handoffs still never enter recall, mining, or canonicalization.

**Conversation archive.** The archive (`/v6.5/archive/*`) is the durable,
replayable continuity source. It owns a separate database with authenticated
idempotent batch ingestion, evidence retrieval, normalized search, full export,
an owner-token deletion saga, and an ingestion-order mining cursor that feeds
candidate extraction without re-reading history.

**Capture spool.** Adapters append captured events to a local durable spool
that fsyncs the record and its ordering metadata before acknowledging, so
sudden process termination cannot lose an acknowledged event. The spool drains
into archive ingestion idempotently.
