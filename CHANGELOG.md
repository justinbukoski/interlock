# Changelog

## 6.5.1 — 2026-08-23

Correctness and security release. No breaking API changes; one forward
database migration (archive `0003`, applied automatically by the installer).

### Security

- **Mining routes are owner-only.** `/v6.5/mining/pending` returned every
  consumer's events to any authenticated token, and `/v6.5/mining/advance`
  let a read-only token permanently advance the irreversible mining cursor.
  Both now require the owner credential.
- **Handoffs can no longer smuggle secrets.** The sensitive-content scan and
  size bounds now cover every free-text handoff field (`completed`,
  `artifacts`, `verification_state`, `do_not_repeat` included).
- **Forbidden context keys actually cover home directories.** Bare `/home`,
  `/Users`, `~/`, Windows home trees (`C:\Users\name`), trailing-separator
  disguises, and path-traversal segments are all rejected; deeper project
  paths remain legal. Normalization is used only for the deny-list check —
  stored key identities are unchanged.
- **Error bodies no longer leak internals.** `Internal` errors are masked in
  HTTP responses like storage errors already were; full detail goes to
  server-side tracing. Archive search queries are redacted before they reach
  the embedder sidecar, matching recall.

### Correctness

- **Compare-and-swap now covers handoff creation.** Two agents that both
  observed an empty context could each write and silently supersede the
  other. `handoff_write` accepts `expect_no_active: true` — mutually
  exclusive with `expected_active_id` — so exactly one creator wins and the
  loser receives a conflict. The guard compares against the *effective*
  active handoff (active and unexpired), and the request-hash idempotency of
  legacy-shaped requests is unchanged across the upgrade. Always pass one of
  the two guards after reading state.
- **`raw_only` deletion purges what it claims.** The saga now releases
  `raw_content_ref` on the selected events (migration `0003` permits exactly
  that one-way transition under the append-only guard), records the count in
  the audit trail, and marks `raw_only` intents complete once their
  archive-side steps are durable. Readiness requires the new schema version.
- **Archive search and health are embedding-generation-safe.** Both now
  filter the embeddings join to the active generation, so introducing a
  second generation can no longer duplicate results, rank against stale
  vectors, or double-count coverage.
- **The capture spool can no longer lose acknowledged records.** A failed
  write repairs its torn tail immediately and writes land at the tracked
  logical end, so recovery can never truncate a later durable frame; new
  spool files fsync their directory entry; and mid-file corruption now fails
  closed for operator recovery instead of silently truncating everything
  after it.
- **Spool flush honors per-event acknowledgements.** A 2xx batch can carry
  individual rejections; each rejected event is durably gap-logged with the
  server's reason before the batch is acknowledged — never silently dropped,
  and never left to wedge the queue.

### Client and integrations

- The MCP adapter now honors `INTERLOCK_WRITER_TOKEN_FILE` (preferred,
  matching the documented reader+writer posture for routine agents) with
  `INTERLOCK_OWNER_TOKEN_FILE` still supported for deliberate administrative
  sessions. Previous releases read only the owner variable, so a
  configuration that set the writer variable was silently ignored, and the
  shipped templates had to grant routine agents the owner credential — both
  fixed; templates now ship the writer posture.
- `bootstrap`'s advertised default token budget is now 16000; the server
  enforces a dynamic minimum sized to the mandatory policy.
- New integration templates: ZCode (`integrations/zcode/config.json`,
  verified against ZCode 0.16.x) and the `.agents` convention
  (`integrations/agents/mcp.json`, consumed by ZCode as a fallback and by
  other tools that honor `~/.agents/mcp.json`), joining Claude Code and
  Codex. The MCP server name is now uniformly `interlock` across all
  templates and docs.
- The installer migrates 6.5.0's `v6-` prefixed token filenames to the
  canonical names by copy (the adapter rejects symlinked token files by
  design) and keeps the deprecated names in sync; the compose healthcheck
  now gates on `/v6.5/health` reporting `"status":"ok"`, so a
  schema-behind service is unhealthy instead of silently degraded; the
  fail-closed gate retries bootstrap once at the server's stated minimum
  budget instead of blocking sessions when the mandatory policy outgrows
  the default.
- New: `docs/IMPLEMENTATION.md` — the complete implementation guide, from
  install through per-agent wiring, hooks, handoff discipline, and
  verification.

## 6.5.0

Initial public 6.5 release: continuity plane (typed handoff lifecycle with
compare-and-swap supersession), conversation archive (idempotent batch
ingestion, normalized search, evidence retrieval, export, owner deletion
saga), fail-closed capture hooks, and the Rust MCP adapter.
