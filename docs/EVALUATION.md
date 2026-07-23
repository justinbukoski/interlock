# Foreman Memory v6 Evaluation Program

Status: normative evaluation design, 2026-07-19.

## Principle

No aggregate score may hide a safety or authority failure. The suite has hard
gates, quality metrics, and operational measurements. A candidate release must
pass every hard gate and materially improve on v5 using the same frozen corpus.

## Corpus construction

The evaluation corpus has three layers:

1. **Synthetic invariants** — deliberately small fixtures proving scope,
   supersession, authority, time, conflict, budget, and lane isolation.
2. **Redacted historical failures** — frozen examples of actual Foreman errors,
   including stale model state, contradicted preferences, project drift,
   handoff promotion, credential hunting, and tenant leakage.
3. **Shadow traffic** — consented, redacted Codex and Lumi requests replayed
   against fixed snapshots. Shadow results are never used as canonical writes.

Every case has an immutable ID, fixture revision, query and identity, effective
time, allowed and forbidden IDs, expected intent, maximum tokens, adjudication,
and failure-class tags. Expected answers identify propositions and required
relationships rather than demanding one exact prose rendering.

The public repository contains synthetic fixtures and schemas. Private redacted
historical fixtures are stored in the dedicated evaluation dataset with a
manifest and checksums; secrets and raw credentials are prohibited.

## Hard-gate suites

### Structural supersession

- A corrected value is returned; its replaced value is absent from ordinary
  recall even when the old wording has higher vector similarity.
- The old value is available only through explicit history/why intent and is
  labeled superseded.
- A chain of three corrections yields one current value and an intact audit
  chain.

### Scope isolation

- Session and thread memory cannot leak to sibling tasks.
- Repository memory cannot leak to another repository in the same project.
- Project memory cannot leak to another project for the same user.
- User and tenant boundaries are tested with adversarial identical text and
  embeddings.
- Missing identity narrows results; it never broadens access.

### Authority

- An owner correction beats a newer agent inference.
- Mechanically verified repository state beats an agent report.
- Similarity and recency cannot overturn a higher-authority applicable value.
- Unresolved same-tier conflicts are represented, never silently resolved.

### Lane separation

- Handoffs appear only in exact-project bootstrap and never in recall.
- Raw chat/history appears only for history or explore intent and carries a
  low-authority label.
- Extractor candidates and quarantined rows never appear in user recall.

### Token budget

- Actual serialized output is within budget for every successful response.
- Mandatory directives that cannot fit yield `budget_too_small` with a minimum.
- No item is cut mid-record and provenance is never removed to make space.

### Security and privacy

- Cross-tenant retrieval is zero across lexical, vector, cache, snapshot, and
  document paths.
- Known plaintext secrets are absent from embeddings, logs, error bodies, and
  searchable fields.
- Redaction precision and recall each meet or exceed 0.95 on the maintained
  corpus; any known plaintext-secret exposure fails the release.
- Unauthorized raw-content access and privilege escalation attempts fail and
  emit audit records.

## Quality metrics

- Canonical answer recall and precision by failure class.
- nDCG at the configured result limit, computed only after authorization and
  canonical-state filtering.
- Required-source coverage and forbidden-item rate.
- Duplicate canonical-key rate, target below 1% per packet.
- Correct abstention when the corpus lacks an answer.
- Conflict-detection precision and recall.
- Human pairwise preference for v6 versus v5 packets, with rationale recorded.

The primary quality claim requires statistically meaningful improvement overall
and no regression on any hard historical failure class.

## Temporal evaluation

Fixtures include knowledge as observed at multiple effective times. Evaluation
proves current-state recall, historical state at a requested time, expiry,
future-dated facts, and correction timing. The runner pins database snapshot,
clock, tokenizer, embedder, reranker, and policy versions.

## Operational evaluation

- Warm p95 bootstrap below 100 ms and recall below 250 ms at production corpus
  scale; p50/p95/p99 and cold results are recorded.
- Sustained ingestion with burst traffic, retries, and out-of-order delivery.
- Database restart, worker crash, embedder outage, poison candidate, queue replay,
  snapshot rebuild, and cache-loss recovery without acknowledged-write loss.
- Backup and clean parallel restore with row counts and manifest checksums.
- One week of Codex shadow use with per-request v5/v6 comparison and no critical
  regression.

## Reproducibility

Every run records Git commit, migration revision, fixture manifest checksum,
container image digests, database snapshot ID, model and tokenizer identifiers,
retrieval-policy version, host, seed, and timestamps. Reports are immutable
artifacts linked from the release candidate.

## Implemented shadow runner

`shadow_eval` executes the same frozen case against authenticated v5 and v6
read-only recall endpoints, normalizes their different packet shapes, and
scores required/forbidden content and IDs, duplicate rate, v6 token-budget
compliance, hard-gate failures, and regressions by failure class. It never calls
a write endpoint. Redirects are disabled and endpoints are restricted to
localhost or literal private/loopback addresses.

The runner requires these environment variables:

- `FOREMAN_EVAL_MANIFEST` and `FOREMAN_EVAL_OUTPUT`;
- `FOREMAN_EVAL_V5_URL` and `FOREMAN_EVAL_V6_URL`;
- `FOREMAN_EVAL_V5_TOKEN_FILE` and `FOREMAN_EVAL_V6_TOKEN_FILE`;
- `FOREMAN_EVAL_GIT_COMMIT`.

Token files must be atomically opened without symlink following, owned by the
current user, regular files, and mode 0600 or stricter. Reports are created once
with mode 0600, synced before exit, and contain only item counts, response and
expectation hashes, closed retrieval metadata, per-system latency, aggregate
scores, and the canonicalized fixture checksum. Recalled text and item IDs are
scored in memory and deliberately omitted from the durable report. Each case's
response content is dropped before the next is retained. Existing reports are
never overwritten. Manifests reject supported secret/PII classes and enforce a
maximum of 200 cases and 32 expectations per field in addition to byte and
string limits; each endpoint body and the final report are byte-capped.

Malformed, oversized, unavailable, or structurally incompatible service
responses become fixed-code execution errors inside the completed report and
fail the gate; they do not abort without evidence. V6 normalization requires
UUID item IDs, string rendered content, a closed retrieval-mode value, token
count, and a non-negative snapshot revision. V5 lanes and their scored content
are likewise type-checked.

`eval/manifest.schema.json` is the public schema and
`eval/synthetic-v1.json` is the first public fixture manifest. Its snapshot ID
names the database fixture that must be loaded independently before execution;
the manifest does not mutate or seed either service. Exit status 2 means the
comparison completed but the synthetic continuation gate failed. Its aggregate
improvement check is not the statistically meaningful release-quality claim.
A passing synthetic run permits further shadow evaluation only—it does not
satisfy the required week of real Codex shadow use or authorize cutover.

## Adjudication

Automated scoring handles structural expectations. Ambiguous semantic cases are
blind-reviewed without revealing which system produced the packet. Owner intent
is never inferred by majority vote: when a case depends on Justin's preference,
his explicit correction is the authority and the fixture is updated through a
reviewed revision.

## Release decision

A release candidate is rejected if any hard gate fails, any known v5 safety
failure recurs, restore is unproven, shadow operation has a critical regression,
or rollback has not been exercised. Passing permits continued shadow use; it
does not authorize replacing v5.
