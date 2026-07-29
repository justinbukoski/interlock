# Interlock Retrieval Contract

Status: normative design contract, 2026-07-19.

## Inputs

Every bootstrap or recall request includes authenticated tenant, user, and
consumer identity. Recall additionally accepts query text, intent, known project
identity, effective time, lane limits, and a hard output-token budget.

The service never infers a broader scope because a client omitted a dimension.
It either resolves identity from the authenticated session or reports the
dimension as unknown and excludes narrower scoped records.

## Deterministic scope walk

Applicable memory is gathered in this order:

`session -> thread -> repository -> project -> user -> global`

More-specific current propositions override broader propositions with the same
canonical key. Broader values remain eligible only when they do not conflict or
when the response explicitly presents an unresolved conflict.

## Pipeline

1. Normalize query and validate identity, intent, time, and budget.
2. Load mandatory applicable constraints and standing directives from the
   canonical snapshot.
3. Generate candidates independently through lexical search, vector search,
   exact entity/predicate lookup, and document lookup when the intent allows it.
4. Enforce tenant, user, consumer, and scope authorization before ranking.
5. Remove invalid, superseded, quarantined, expired, and temporally inapplicable
   rows. History intent may request these with explicit state labels.
6. Collapse duplicate canonical keys and apply structural supersession.
7. Rank by scope specificity, authority tier, epistemic state, temporal fit,
   retrieval relevance, and diversity. Learned relevance cannot overturn the
   first five deterministic dimensions.
8. Render provenance-bearing items, then enforce the token budget on the actual
   serialized representation.

If mandatory directives alone exceed the budget, the server returns a typed
`budget_too_small` error with the minimum required budget. It never truncates a
rule mid-item or silently omits mandatory policy.

## Intent behavior

- `current`: only current canonical state plus applicable directives.
- `why`: canonical decisions, supporting evidence, and supersession chain.
- `procedure`: current runbooks and mechanically verified operational facts.
- `history`: observation/document search with low-authority labels; never mixed
  into current truth without a separate canonical section.
- `explore`: broader evidence set with uncertainty and conflicts preserved.

Server-side intent classification is optional. When used, the response reports
the selected intent and classifier version.

## Response

Each returned item contains stable ID, kind, rendered content, scope,
authority, epistemic state, source references, observed/recorded time, validity
interval, current state, and retrieval reasons. The packet also reports token
count, budget, snapshot revision, query time, and any excluded-lane counts.

The implemented response additionally reports `retrieval_mode` (`hybrid` or
`lexical_only`) and `embedding_model`. Embedder failure is therefore visible and
never represented as successful hybrid retrieval.

Conflicts are first-class records containing the competing proposition IDs and
the reason deterministic resolution was impossible. The server must not select
a winner by embedding similarity.

## Bootstrap

Bootstrap is a separate fixed-shape operation. It returns:

1. mandatory applicable constraints and directives;
2. current project/repository state, if identity is known;
3. the exact-project latest unexpired handoff in its own section;
4. snapshot revision and generation time.

Handoffs never enter recall, ranking, snapshots of canonical memory, or automatic
promotion. A stale handoff may be returned only when the client explicitly asks
and is visibly labeled.

## Cache semantics

Bootstrap packets are cached by identity tuple, project key, policy revision,
canonical snapshot revision, and token budget. Recall result caching additionally
includes normalized query, intent, effective-time bucket, and retrieval-policy
version. Canonical commits invalidate affected keys through the transactional
outbox. Cache absence or rebuild changes latency, never semantics.

## Evaluation invariants

- Same snapshot plus same request produces byte-equivalent ordered content,
  excluding request IDs and measured timing.
- Ordinary recall returns no structurally superseded or invalid proposition.
- Cross-project and cross-user adversarial records are never candidates after
  authorization filtering.
- The serialized packet never exceeds the requested token budget.
- A lower-authority but more similar item cannot displace an applicable owner
  correction for the same canonical key.
- History evidence is never mislabeled as canonical truth.
