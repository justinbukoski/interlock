# Foreman Memory v6 implementation record

## Current slice

This branch implements a parallel, non-deployed v6 service in Rust. It provides:

- public liveness and migration-aware readiness checks;
- bearer-token grants scoped to tenant, user, consumer, actor, and role;
- bootstrap, recall, canonical write, and separate handoff endpoints;
- idempotent observation ingestion with synchronous secret/PII redaction;
- immutable extractor candidates linked to one or more observations and atomic,
  verifier-gated promotion into canonical memory;
- server-enforced `cl100k_base` response budgets with typed minimum-budget
  errors;
- structurally current propositions, explicit supersession edges, exact
  mutation-linked audit events, an outbox, and monotonic snapshot revisions;
- payload-bound idempotency for canonical and handoff writes;
- serializable writes with advisory locking and bounded statement, lock, pool,
  and idle-transaction waits;
- shared owner policy scopes without making ordinary consumer memory global.
- additive pgvector 0.8.2+ storage for 1024-dimensional BGE embeddings, a
  retryable derived-data worker, and deterministic lexical/vector reciprocal-
  rank fusion that cannot outrank scope or authority.
- a read-only v5/v6 shadow evaluator with frozen manifest validation,
  structural hard-gate scoring, per-failure-class regression reporting,
  private immutable reports, and an end-to-end dual-service test.

The service defaults to `127.0.0.1:8851`. A non-loopback bind is rejected unless
`FOREMAN_V6_TRUSTED_PROXY=true`, which is an explicit deployment assertion—not a
substitute for configuring TLS and proxy trust correctly.

## Required environment

- `FOREMAN_V6_DATABASE_URL`: v6's own PostgreSQL database URL.
- `FOREMAN_V6_AUTH_FILE`: JSON file containing `tokens`; on Unix it must be a
  regular, non-symlink file owned by the service user with no group/world access.
- `FOREMAN_V6_LISTEN`: optional, defaults to `127.0.0.1:8851`.
- `FOREMAN_V6_TRUSTED_PROXY=true`: required for a non-loopback listener.
- `FOREMAN_V6_EMBEDDER_URL`: optional local BGE service base URL. When absent or
  temporarily unavailable, recall explicitly reports `lexical_only`.
- `FOREMAN_V6_EMBEDDING_MODEL`: expected model identity; defaults to
  `BAAI/bge-large-en-v1.5` and rejects mismatched responses.

## Hybrid retrieval

Migration 3 requires pgvector 0.8.2 or newer and adds nullable, rebuildable
embeddings to propositions and observations. Writes remain durable when the
embedder is unavailable: the worker finds unembedded rows and retries without
coupling canonical commits to inference availability. Query text is redacted
before it leaves the process for the configured embedder.

For non-history recall, lexical and cosine-similarity candidate lanes are fused
with reciprocal-rank fusion. Scope specificity, consumer specificity, and
authority remain deterministic ordering dimensions ahead of relevance. The API
reports `hybrid` only when it obtained a valid 1024-dimensional query vector;
otherwise it reports `lexical_only` and omits the embedding model. History
remains a separate lexical observation lane in this slice.
When a configured embedder fails, `degraded_reason` is
`query_embedding_unavailable`; a deliberately unconfigured semantic lane leaves
that field empty.

Migration 4 adds expiring worker leases. Each service instance claims eligible
proposition and observation embedding rows with `FOR UPDATE SKIP LOCKED` before
calling the model, and persistence/failure transitions are conditional on that
worker's UUID. Concurrent replicas therefore do not transmit the same memory
row or accelerate its quarantine counter. The claim cap applies across both
source kinds, and lease duration is derived from the bounded HTTP timeout plus
per-row database allowance. Expired leases remain crash-recoverable.

The process uses one shared shutdown notification for Axum and the embedding
worker. On SIGINT/SIGTERM the server stops accepting work, the worker exits
between bounded batches, and the process waits up to ten seconds. If inference
is still in flight, Rust aborts the task and the store releases every lease held
by that worker before process exit. The steady-state worker batch is capped at
eight rows combined, with capacity reserved for each source kind when both have
backlogs, keeping inference and shutdown latency bounded without starving
observation embeddings.

Token files contain only SHA-256 token hashes, not plaintext bearer tokens.
Duplicate hashes are rejected. Roles are `reader`, `writer`, `verifier`, and
`owner`; only owners may write directives, constraints, or owner instructions.
The storage layer repeats this authorization so handler bypass cannot forge it.

## Observation and candidate lifecycle

`POST /v6/observations` accepts bounded interaction events. Supported-class DLP
runs inside the PostgreSQL store implementation, not only in the HTTP handler,
so a direct store caller cannot bypass it. The current detector covers provider
keys, bearer/JWT tokens, assigned secret fields, nested structured secret keys,
credential-bearing URIs, private keys, email, SSN, formatted phone numbers, and
Luhn-valid payment-card numbers. This is a maintained detection boundary, not a
claim that arbitrary future PII formats are impossible to miss. The searchable
row contains only redacted content and request identity is hashed from the
redacted representation, not the raw secret-bearing request.

Optional raw references must be opaque `encrypted:<identifier>` values where the
identifier is 1–128 ASCII alphanumeric, underscore, or hyphen characters. Recall
reports only that encrypted raw content is available; it never returns the
locator. Raw resolution, encryption, authorization, and retention are a future
separate service boundary. v6 does not accept a plaintext raw payload into
PostgreSQL.

`POST /v6/candidates` creates an immutable extractor proposal. A candidate may
name an unregistered predicate, but it must link to 1–100 observations inside
the authenticated tenant and user. Request IDs and derivation keys are both
idempotent and payload-bound. Candidates never enter ordinary recall.

`POST /v6/candidates/promote` requires a verifier or owner token and an explicit
authority attestation; it never copies the extractor's authority claim as
canonical authority. Promotion
locks the candidate and canonical key, validates the predicate, value type,
authority floor, and owner-confirmation rules, then creates the proposition,
audit, structural supersession edge, outbox event, snapshot revision, immutable
candidate event, and accepted-candidate transition in one serializable
transaction. Extractor/writer tokens cannot promote their own proposals.

History intent searches consumer-scoped redacted observations and labels every
result as `observation`, `raw_history`, `uncertain`, and `history`. Mandatory
canonical policy is returned in a distinct `mandatory_policy` response section;
history evidence is never flattened into that canonical section.

## Database and lifecycle boundary

The checked-in migration is for a fresh v6 database only. Production deployment
must provision a unique PostgreSQL cluster on its own ZFS dataset. The code does
not contain any v5 shutdown, mutation, migration, or cutover operation.

Migration 5 registers two explicit continuity predicates. The one-shot
`foreman-import-v5` utility reads v5 in a read-only transaction and writes only
to the dedicated v6 database. Active constraints and rules become mandatory
policy; only pinned, maximum-importance directive notes are promoted to that
lane. Other current notes and facts retain v5 source references under
`legacy.note` and `legacy.fact`, with lower authority. IDs are deterministic,
the target write is one transaction, and dry-run mode exercises and rolls back
the complete write path. The importer neither invalidates nor stops v5.

Handoffs remain a distinct, consumer-scoped, expiring lane. They never enter
recall or become directives. Canonical changes require an audit event carrying
the same request/mutation identifier; supersession additionally requires an
edge linked to that exact mutation and replacement proposition.

## Verification completed on 2026-07-19

- formatting check: pass;
- six HTTP contract tests: pass;
- Clippy with warnings denied: pass;
- fresh PostgreSQL 16 + pgvector 0.8.2 disposable-container integration test:
  pass (including populated migration upgrade through migration 5 and
  exclusive concurrent row claiming);
- eight-way concurrent same-key write exercise: pass;
- idempotent replay and changed-payload rejection: pass;
- direct storage-layer owner-policy forgery rejection: pass;
- unaudited canonical transition rejection at transaction commit: pass;
- tenant isolation, scope shadowing, shared-policy visibility, and handoff-lane
  isolation: pass.

The second lifecycle slice additionally proves a populated migration-1 database
upgrades through migration 2, observation replay and changed-
payload rejection, secret/email redaction, consumer-isolated history search,
candidate derivation replay, cross-tenant evidence rejection, writer promotion
denial, atomic verifier promotion and replay, and immutability of observations,
candidates, and candidate audit events. It also proves same-user cross-consumer
evidence/promotion rejection, same-consumer source IDs across different users,
nested structured-secret rejection, sensitive subject/provenance rejection, and
separate history/policy response sections.

The disposable database and tunnel are test infrastructure only and must be
removed after verification. No persistent fleet database was touched.

## Work still required before deployment or cutover

This slice is deliberately not a complete memory system. Remaining milestones
include extractor execution, semantic history/document retrieval, evaluator and
correction workflows, v5 shadow reads, backup/restore
drills, service packaging, rate limits, operational dashboards, and explicit
acceptance-gate evidence. v5 remains the production source until those gates are
met and Justin authorizes a cutover.
