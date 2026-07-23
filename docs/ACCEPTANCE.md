# Foreman Memory v6 Acceptance Gates

No date or implementation milestone can waive these gates.

## Correctness

- Zero superseded or invalid items in ordinary recall across the fixed suite.
- Zero cross-project leakage in adversarial scope tests.
- Duplicate rate below 1% in returned packets.
- Explicit conflict representation for unresolved single-value predicates.
- Owner corrections become canonical atomically and remain auditable.
- Server-rendered output never exceeds the requested token budget.

## Retrieval quality

- A frozen, human-adjudicated Justin-specific suite covers current state, why,
  temporal questions, procedures, abstention, and raw-history lookup.
- v6 must materially outperform v5 overall and on every known v5 failure class.
- Every regression is reviewed; aggregate score cannot hide a safety regression.
- Side-by-side shadow packets are inspected during real Codex work.

## Ingestion and lifecycle

- Every Codex and Lumi turn is observable with correct project/thread/session.
- Retries are idempotent and ordered; disconnects produce no silent gaps.
- Session close produces a summary and candidate set without direct canonical
  mutation.
- Ingestion lag, queue age, poison items, and dropped events are measurable.

## Security

- Every endpoint authenticated and least-privilege scoped.
- Redaction precision and recall each at least 0.95 on a maintained secret/PII
  corpus, with zero known plaintext-secret search exposure.
- Raw sensitive content encrypted, access-audited, and retention-bounded.
- Threat-model and dependency review pass before network exposure.

## Operations

- v5 health is unchanged throughout deployment and shadow testing.
- v6 uses its own ZFS dataset, PostgreSQL cluster, credentials, and backups.
- Restore into a clean parallel environment succeeds and matches checksums.
- Restart, worker crash, embedder outage, database fail/recovery, and queue replay
  tests pass without lost acknowledged writes.
- Multiple worker replicas hold exclusive active embedding claims; expired leases
  recover automatically and graceful shutdown releases owned leases.
- Load test meets p95 bootstrap <100 ms and recall <250 ms on the warm corpus;
  cold behavior is recorded rather than hidden.

## Adoption

- At least one week of real Codex shadow operation with no critical regression.
- Rollback to v5 is exercised, not merely documented.
- Justin explicitly approves any change making v6 the default. The build itself
  does not imply cutover authorization.
