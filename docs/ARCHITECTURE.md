# Foreman Memory v6 Architecture

Status: design baseline, 2026-07-19. Quality takes precedence over schedule.

## Purpose

Foreman v6 supplies compact, current, attributable context to Codex and Lumi
without flooding model context or allowing stale statements to compete with
their corrections. PostgreSQL is the durable authority. Memory-resident views
are derived accelerators and can always be rebuilt.

## Service boundary

v6 runs in parallel on a new port, new containers, and a dedicated ZFS-backed
PostgreSQL cluster. It reads v5 only through an export/import pipeline. It never
writes to, stops, reconfigures, or shares a database with v5.

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

Lumi uses the same API and can add the richer inspection UI: source, freshness,
scope, correction, promotion, and review queue. Neither client owns memory
semantics.

## Migration

Migration is export, normalize, import, and compare. v5 remains the production
source throughout shadow operation. v6 never assumes old priority or prose
corrections are canonical. Ambiguous conflicts enter review. Cutover, if ever
authorized, is a client configuration change with immediate rollback to v5.
