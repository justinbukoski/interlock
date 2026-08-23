# Interlock

[![CI](https://github.com/justinbukoski/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/justinbukoski/interlock/actions/workflows/ci.yml)

**Interlock is a self-hosted memory service that multiple AI agents share — and cannot skip.** Every conversation prompt is captured, redacted, and embedded
automatically; nobody has to say "remember this." Raw history, candidate
facts, canonical memory, and short-lived handoffs live in separate lanes, and
if the service is unreachable at session start, the agent halts instead of
proceeding on fabricated context.

What sets it apart from other agent-memory systems:

- **One memory, many models.** Every agent gets its own scoped identity and
  token, and every record carries actor and scope dimensions — so a Claude
  session, a Codex session, and three local models can work the same project
  against shared facts, directives, and handoffs. Handoffs use
  compare-and-swap supersession: two agents can never both win one, and none
  is silently lost. This is how teams of heterogeneous models collaborate on a
  codebase instead of each keeping private notes.
- **Capture is automatic and fail-closed.** Hooks record every prompt at the
  start of the turn, and a durable, crash-recoverable capture queue
  (`interlock-spool`) is available for adapters that must never silently
  discard an event.
  Durable facts are extracted from what was actually said — not from what a
  user remembered to dictate. If memory cannot be reached, the session does
  not start. Fail-closed is not a feature of this system; it is the thesis.
- **Corrections always win.** A validity-filtering invariant guarantees a
  stale fact can never out-rank its own correction in retrieval. This is the
  production failure that shaped the design — reproduce it with
  `python3 demo/stale_memory_demo.py`.

Interlock (formerly Foreman Memory) is named for the machine-safety interlock:
the device that refuses to let equipment run unless the guard is in place.

This friend release includes:

- an Axum/SQLx API and PostgreSQL with pgvector;
- BGE-large-en-v1.5 embeddings;
- a Rust MCP adapter for bootstrap, recall, history, observation, memory writes,
  corrections, and handoffs;
- fail-closed hooks for Codex, Claude Code, and ZCode, plus an `.agents`-convention template;
- a local Docker Compose installer with persistent identities and scoped tokens;
- the 6.5 continuity plane: a typed handoff lifecycle with compare-and-swap
  supersession, so two agents can never both win a handoff and none is silently
  lost; and
- the 6.5 conversation archive: a separate durable database with idempotent
  batch ingestion, normalized search, evidence retrieval, full export, and an
  owner deletion saga.

It is a technical preview. Prompt capture and retrieval work now. Fully
unattended extraction of every conversation into reviewed canonical facts is
still under development; agents can create candidates and canonical records
through the MCP tools.

## Quick start

Requirements: Docker with Compose, Git, Rust/Cargo, Python 3, and OpenSSL.

```sh
git clone https://github.com/justinbukoski/interlock.git
cd interlock
./distribution/install.sh
```

The service binds only to `127.0.0.1:8851`. Continue with
[`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) — the complete
implementation guide — to wire Claude Code, Codex, Claude Desktop/Cowork,
ZCode, or any `.agents`-convention client, enforce the fail-closed gate, and
verify the installation with positive and negative checks.
[`docs/INSTALL_WITH_COWORK.md`](docs/INSTALL_WITH_COWORK.md) remains the
guided AI-coworker variant of the same setup.

Automatic prompt capture is intentional and central to the product. Read
[`docs/PRIVACY_AND_DATA.md`](docs/PRIVACY_AND_DATA.md) before enabling hooks so
every user understands what is stored and how to export or erase it.

## See the incident this design exists to prevent

```
python3 demo/stale_memory_demo.py
```

No dependencies. It reproduces the production failure that shaped Interlock:
a stale fact out-ranking its own correction in similarity search, and the
validity-filtering invariant that makes that outcome impossible.

## How retrieval works

Agents normally retrieve memories through `interlock-mcp`, not through a remote
shell bridge. At session start, `bootstrap` loads constraints, directives, and
high-value context. During work, `recall` performs scoped hybrid retrieval and
`history` searches the observation lane. The hooks also fail closed if the
service cannot be reached.

A shell MCP is optional and useful only when Interlock runs on another machine
that the agent's normal sandbox cannot reach.

## Security model

- The API is loopback-only by default.
- Token files and generated authorization state are mode `0600`.
- Day-to-day agents receive reader and writer tokens.
- The owner token is created for administration but is not placed in the
  default agent configuration.
- Canonical owner-authority writes and system directives require explicitly
  configuring the owner token.

See [`docs/OWNER_ADMINISTRATION.md`](docs/OWNER_ADMINISTRATION.md).

## Operations

- [Complete implementation guide](docs/IMPLEMENTATION.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Data model](docs/DATA_MODEL.md)
- [Privacy and data controls](docs/PRIVACY_AND_DATA.md)
- [Backup and restore](docs/BACKUP_RESTORE.md)
- [Upgrading and uninstalling](docs/UPGRADING.md)

## Local verification

```sh
python3 -m unittest discover -s distribution/tests
cargo test --locked
cargo clippy --locked --all-targets --all-features -- -D warnings
```

The ignored PostgreSQL integration tests require disposable PostgreSQL 15+
databases with pgvector 0.8.2+ through `TEST_DATABASE_URL` and
`TEST_ARCHIVE_DATABASE_URL`. Run them with `--test-threads=1`, use each
database for a single run, and never point them at a persistent database.
