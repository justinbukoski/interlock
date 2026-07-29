# Interlock

[![CI](https://github.com/justinbukoski/interlock/actions/workflows/ci.yml/badge.svg)](https://github.com/justinbukoski/interlock/actions/workflows/ci.yml)

**Interlock** (formerly Foreman Memory) is named for the machine-safety device
that refuses to let equipment run unless the guard is in place. The same rule
applies here: if the memory service is unreachable at session start, the agent
halts instead of proceeding on fabricated context. Fail-closed is not a
feature of this system; it is the thesis.

Interlock is a self-hosted durable memory service for AI coding agents. It records
conversation prompts automatically, retrieves relevant context with hybrid
semantic search, and keeps raw history, candidate facts, canonical memory, and
short-lived handoffs in separate lanes.

This friend release includes:

- an Axum/SQLx API and PostgreSQL with pgvector;
- BGE-large-en-v1.5 embeddings;
- a Rust MCP adapter for bootstrap, recall, history, observation, memory writes,
  corrections, and handoffs;
- fail-closed Codex and Claude Code hooks;
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
[`docs/INSTALL_WITH_COWORK.md`](docs/INSTALL_WITH_COWORK.md) to connect Codex,
Claude Code, or Claude Cowork and prove that the enforcement gate works.

Automatic prompt capture is intentional and central to the product. Read
[`docs/PRIVACY_AND_DATA.md`](docs/PRIVACY_AND_DATA.md) before enabling hooks so
every user understands what is stored and how to export or erase it.

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
