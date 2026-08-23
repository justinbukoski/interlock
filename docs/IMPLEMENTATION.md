# Implementing Interlock: the complete guide

This is the end-to-end implementation guide: install the service, wire every
agent you run to it, enforce the fail-closed gate, and verify the whole thing
with the same checks used before each release. Companion references:
`docs/ARCHITECTURE.md` (why it is shaped this way), `docs/DATA_MODEL.md`,
`docs/RETRIEVAL_CONTRACT.md`, `docs/PRIVACY_AND_DATA.md` (read before
enabling capture), `docs/OWNER_ADMINISTRATION.md`, `docs/BACKUP_RESTORE.md`,
and `docs/UPGRADING.md`.

## 1. What you are implementing

Interlock is one shared memory service that every agent uses — and cannot
skip. Four properties drive every step below:

- **One memory, many models.** Every record carries actor and scope
  dimensions, and tokens are role-scoped, so a Claude session, a Codex
  session, a ZCode session, and local models work the same projects against
  shared facts, directives, and handoffs. The default install provisions one
  consumer identity shared by your agents with three role tokens; the data
  model supports per-agent identities for multi-consumer setups.
- **Separate lanes.** Raw conversation history (the archive), candidate
  facts, canonical memory, and short-lived handoffs never mix. History and
  handoffs never silently become truth.
- **Fail-closed.** If the service is unreachable at session start, the agent
  halts instead of proceeding on fabricated context. This is enforced by
  hooks, not by convention.
- **Corrections always win.** A stale fact can never out-rank its own
  correction in retrieval.

## 2. Install the service

Requirements: Docker with Compose, Git, Rust/Cargo, Python 3, OpenSSL.
Minimum 4 cores / 8 GB RAM / 15 GB disk; the first start downloads the
~1.3 GB embedding model.

```sh
git clone https://github.com/justinbukoski/interlock.git
cd interlock
./distribution/install.sh
curl -fsS http://127.0.0.1:8851/v6/health
curl -fsS http://127.0.0.1:8851/v6.5/health
```

Both health endpoints must report `"status":"ok"`; `/v6.5/health` must also
report `"archive":true` and `"continuity":true`. The installer is
re-runnable: it preserves identities and token files, rebuilds containers,
applies forward migrations, and reinstalls the two host binaries it manages —
`~/.local/bin/interlock-mcp` (the MCP adapter) and `~/.local/bin/interlock-gate`
(the fail-closed hook).

## 3. Tokens and roles

The installer provisions three token files in `~/.config/interlock/`, each
mode `0600`:

| File | Role | May call |
|---|---|---|
| `reader-token` | reader | bootstrap, recall, history, archive search/evidence, handoff reads |
| `writer-token` | writer | plus observations, normal memories, corrections, handoff writes |
| `owner-token` | owner | plus owner-authority records, system constraints/directives, archive deletion, mining |

Routine agents get **reader + writer only** — the server's role checks stop a
writer from minting owner decisions, and 6.5.1 makes mining and deletion
owner-only at the store layer as well. Reserve the owner token for deliberate
administrative sessions (`docs/OWNER_ADMINISTRATION.md`).

Installs upgraded from 6.5.0 had `v6-`prefixed token filenames; the 6.5.1
installer copies them to the names above and keeps the deprecated names in
sync as regular files (never symlinks — the adapter deliberately refuses
symlinked token files), so configurations referencing either name keep
working. Prefer the new names going forward.

## 4. Wire every agent

Templates live in `integrations/`. In each, replace `/ABSOLUTE/HOME` with
your home directory — MCP clients do not expand `~` or `$HOME`. All templates
use `INTERLOCK_READER_TOKEN_FILE` + `INTERLOCK_WRITER_TOKEN_FILE`; the
adapter prefers the writer variable and accepts
`INTERLOCK_OWNER_TOKEN_FILE` only for administrative configurations.

### Claude Code

Merge `integrations/claude/mcp.json` into `~/.claude.json` (top-level
`mcpServers`) and `integrations/claude/settings.json` into
`~/.claude/settings.json` (the `hooks` section). Copy
`integrations/claude/CLAUDE.md` into `~/.claude/CLAUDE.md` (or append it to
an existing one) — it carries the standing instructions that make the agent
treat bootstrap as mandatory. Verify with `claude mcp list` — `interlock`
must show Connected — and start a new session: the first turn should carry
an injected mandatory-policy block.

### Codex CLI

Merge `integrations/codex/config.toml` into `~/.codex/config.toml`. The
`[mcp_servers.interlock]` table wires the adapter; the `[[hooks.*]]` tables
wire the gate. Copy `integrations/codex/AGENTS.md` into `~/.codex/AGENTS.md`
(or append) for the standing instructions. Codex requires hook trust on
first run — approve it once.

### Claude Desktop / Cowork

Add the same server block to
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or
the equivalent config on your platform, under `mcpServers`, using the exact
command/env from `integrations/claude/mcp.json`. Desktop sessions pick up new
MCP servers on app restart.

Be precise about the guarantee here: **MCP connected is not fail-closed
enforced.** Desktop/Cowork exposes no hook lifecycle, so nothing mechanical
blocks a session when the service is down. The strongest available
substitute is instructional: put "call `interlock`'s `bootstrap` before
acting; if it fails, stop and say so" in the instructions the client always
loads (Claude Desktop project/global instructions, or a `CLAUDE.md` the
session reads), and treat any answer produced without a bootstrap as
suspect. If you need the hard guarantee, use a hook-capable client.

### ZCode

Merge `integrations/zcode/config.json` into `~/.zcode/cli/config.json`
(verified against ZCode 0.16.x; hook and MCP config formats are
client-version-dependent). Three ZCode specifics: hook configuration
requires `hooks.enabled: true`; hook config is snapshotted at session start,
so config edits need a new session; and the gate script emits the
Claude-style `hookSpecificOutput` JSON, which ZCode accepts natively. Keep
standing instructions in `~/.zcode/AGENTS.md` (global) or a project
`AGENTS.md` — the Codex template's instruction list works verbatim.

### Anything that follows the `.agents` convention

Drop `integrations/agents/mcp.json` into `~/.agents/mcp.json` (user scope)
or `<project>/.agents/mcp.json` (project scope). Per ZCode's documented
load rules, `.agents/mcp.json` acts as a fallback only when the `.zcode`
config of the same scope defines no MCP servers; other tools that honor the
convention can load the adapter from the same file.
Like Desktop/Cowork, convention-only clients get no fail-closed hook —
apply the instructional substitute above.

## 5. The fail-closed gate

`interlock-gate` is a stdin/stdout hook (installed by the installer) that
runs at SessionStart and UserPromptSubmit. It health-checks the service,
loads the mandatory policy and scoped project state within a server-enforced
token budget (retrying once at the server's stated minimum as your policy
grows, capped at the schema maximum of 32768 — beyond that the gate blocks,
which is the correct fail-closed outcome), records the prompt as an observation through the writer token, and
injects the result into the model's context. If the service is down, the
session does not proceed — that is the point. Do not soften the gate to
"warn and continue"; a session without memory fabricates context.

## 6. Using the tools correctly

The adapter exposes: `bootstrap`, `recall`, `history`, `observe`, `remember`,
`correct`, `handoff`, `evidence`, `archive_search`, and the 6.5 continuity
tools `handoff_get_exact`, `handoff_validate_context`, `handoff_history`,
`handoff_write`, `handoff_acknowledge`, `handoff_complete_items`,
`handoff_close`.

**Read discipline.** Call `bootstrap` before acting (budget 16000; the server
enforces a dynamic minimum sized to your mandatory policy). Use `recall`
before asking the user for facts the system may already know. `history` and
`archive_search` return raw history — treat results as evidence, never as
canonical truth.

**Write discipline.** `observe` records evidence; it does not create memory.
`remember` and `correct` require explicit subject/predicate/object,
authority, epistemic status, source, and reason — the server refuses writes
above the token's authority. Corrections are ordinary higher-authority
writes; structural supersession is the server's job.

**Handoff discipline (changed in 6.5.1).** A handoff is short-lived
continuation state for an exact typed context — never a secret store (every
field is scanned) and never a home directory or filesystem root (broad keys
are rejected; validate with `handoff_validate_context`). Writes are
compare-and-swap, and you must always pass a guard:

1. Read first: `handoff_get_exact` for your context.
2. If it returned a handoff you are superseding: pass its id as
   `expected_active_id`.
3. If it returned none: pass `expect_no_active: true`.
4. Never pass both, and never write unguarded — an unguarded write is how two
   concurrent agents silently overwrite each other.

On a 409, re-read and reconcile; the conflict body carries the current active
id. Acknowledge received handoffs, complete items by id, and `handoff_close`
(with `expected_active_id`) when work is cleanly done.

**Deletion.** Owner-only, two modes: `full` tombstones matching archive
events, purges their embeddings, and releases raw references; `raw_only`
releases raw payload references while keeping redacted content searchable.
Both are durable, idempotent, resumable sagas with an audit trail; remaining
canonical-side steps are reported in `pending_canonical_steps` so a partial
deletion is never mistaken for a complete one.

## 7. Upgrading from 6.5.0

```sh
git pull --ff-only   # or check out the v6.5.1 tag
./distribution/install.sh
curl -fsS http://127.0.0.1:8851/v6.5/health
```

The installer applies archive migration `0003` (permits exactly the
raw-reference release transition under the append-only guard) and migrates
token filenames as described in section 3. Archive readiness requires the
new schema version: a schema-behind service answers `/v6.5/health` with
HTTP 200 and `"status":"degraded"` (`"archive":false`), and the 6.5.1
compose healthcheck requires `"status":"ok"`, so the container reports
unhealthy rather than serving with a half-applied schema. No client changes are required, but update your integration blocks
to the writer-token variable if you copied pre-6.5.1 templates, and adopt the
handoff guard discipline above. See `CHANGELOG.md` for the full list of
6.5.1 fixes.

## 8. Verify the implementation

Positive checks:

```sh
curl -fsS http://127.0.0.1:8851/v6/health
curl -fsS http://127.0.0.1:8851/v6.5/health
```

Then, in a fresh agent session: the first turn shows injected mandatory
policy; `recall` returns your seeded facts; a `handoff_write` with
`expect_no_active: true` on a new project key succeeds.

Negative checks (each must fail):

- Stop the service and start a session — the gate must block it.
- `handoff_write` with a credential-looking string in any field — rejected.
- `handoff_write` with key `~/` or `/home` — rejected as a broad key.
- A second `expect_no_active` write to the same context — 409 conflict.
- Reader token on the mining surface — 403:

  ```sh
  curl -s -o /dev/null -w '%{http_code}\n' \
    -X POST http://127.0.0.1:8851/v6.5/mining/pending \
    -H "Authorization: Bearer $(cat ~/.config/interlock/reader-token)" \
    -H 'Content-Type: application/json' \
    -d '{"generation_id":"chat-bge-large-en-v1.5-v1"}'
  ```

Run `python3 demo/stale_memory_demo.py` to see the corrections-win invariant
reproduce the production failure that motivated the design.

## 9. Operations

Back up before upgrades (`docs/BACKUP_RESTORE.md`), rotate tokens per
`docs/UPGRADING.md`, keep owner-token use rare and audited
(`docs/OWNER_ADMINISTRATION.md`), and read `docs/PRIVACY_AND_DATA.md` before
enabling per-prompt capture — it records every prompt by design.
