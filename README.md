# Foreman Memory v6

Foreman v6 is a self-hosted, agent-first durable memory service. It separates
raw history, observations, candidate facts, canonical memory, and short-lived
handoffs so an assistant can remember without treating every past sentence as
truth.

The repository contains an Axum/SQLx API,
PostgreSQL schema, authenticated v6 routes, observation/candidate lifecycle,
BGE/pgvector hybrid retrieval, a first-class Codex MCP adapter, and contract,
integration, backup, restore, and shadow tests. It does not replace v5. See:

- `docs/ARCHITECTURE.md` for system boundaries and data authority.
- `docs/ACCEPTANCE.md` for the gates required before any primary-workflow use.
- `docs/IMPLEMENTATION.md` for local operation, security boundaries, and test
  evidence.

## Friend preview

The portable package is ready for technical preview. It includes a local
PostgreSQL/pgvector stack, BGE-large embeddings, a Rust MCP adapter, automatic
user-prompt observation, and fail-closed Codex and Claude Code hooks.

The final unattended chat extractor is not yet implemented. Observations can be
stored and candidates can be reviewed/promoted, but this release does not claim
that every conversation is automatically distilled into facts. See
`docs/INSTALL_WITH_COWORK.md` for hardware, installation, and enforcement.

## Non-negotiable invariants

1. Existing memory deployments remain running and unchanged during evaluation.
2. v6 has its own PostgreSQL database and persistent volume.
3. History, observations, candidate memories, and authoritative memory are
   separate lanes; promotion between lanes is explicit and audited.
4. Superseded or invalid records are filtered structurally, never by asking the
   model to interpret correction prose.
5. Every response has a server-enforced token budget.
6. Global memory is opt-in. New records default to the current project/session.
7. Codex and Lumi are clients of the same API, not separate memory systems.

## Local verification

```sh
cargo test --offline
cargo clippy --offline --all-targets --all-features -- -D warnings
```

The ignored PostgreSQL integration test requires a disposable PostgreSQL 15+
database with pgvector 0.8.2+ through `TEST_DATABASE_URL`. Never point it at v5
or another persistent database.

## Portable install

```sh
./distribution/install.sh
```

Then follow `docs/INSTALL_WITH_COWORK.md`.

## Agent clients

`foreman_mcp` exposes `bootstrap`, `recall`, `history`, `observe`, `remember`,
`correct`, and `handoff` over MCP stdio. It accepts only a loopback HTTP URL and
mode-0600, same-user token files. Portable Codex and Claude templates live in
`integrations/`.

Codex desktop, CLI, and the IDE extension share this MCP configuration. Restart
the client after registration. The adapter's initialization instructions make
bootstrap mandatory, keep history/handoffs in their separate lanes, and require
explicit authority and provenance for canonical writes.

## v5 continuity import

`foreman-import-v5` is an optional, one-shot, idempotent continuity importer. It requires
separate mode-0600 files containing loopback PostgreSQL URLs for the exact
`foreman_memory` source and `foreman_v6` target databases. It opens v5 in an
explicitly read-only transaction, redacts supported secret classes, and writes
the normalized v6 records atomically. Always exercise the complete path first:

```sh
FOREMAN_V5_DATABASE_URL_FILE=/run/secrets/v5-url \
FOREMAN_V6_DATABASE_URL_FILE=/run/secrets/v6-url \
foreman-import-v5
```

Dry-run is the fail-safe default. Only after reviewing the JSON report should an
operator rerun with `FOREMAN_V5_IMPORT_APPLY=APPLY_V5_TO_V6`. The importer does
not stop, update, or delete v5, and deterministic IDs plus source-record digest
checks make an identical rerun a no-op while rejecting changed or partial rows.
