# Install Foreman Memory with an AI coworker

## Hardware and software

- Minimum: 4 CPU cores, 8 GB RAM, and 15 GB free disk.
- Comfortable: 8 CPU cores, 16 GB RAM, and 30 GB free disk.
- GPU: optional. The supplied Docker embedder runs on CPU, including on macOS.
- Software: Docker Desktop or Docker Engine with Compose, Git, Rust/Cargo,
  Python 3, and OpenSSL.

The first start downloads the approximately 1.3 GB BGE-large model and builds
the Rust services. Later starts reuse both.

## Install and verify

```sh
./distribution/install.sh
curl -fsS http://127.0.0.1:8851/v6/health
```

Rerunning the installer is supported. It preserves the installation's stable
consumer, agent, and owner identities and retains existing token files.

Before enabling the hooks, read `docs/PRIVACY_AND_DATA.md`. The per-prompt hook
automatically records every user prompt. It does not ask for approval on each
turn.

## Prompt for GPT Cowork or Claude Cowork

Give the coworker this repository folder and the following prompt:

> Install Foreman Memory locally. Read README.md,
> docs/INSTALL_WITH_COWORK.md, and docs/PRIVACY_AND_DATA.md first. Confirm the
> user understands that every user prompt will be recorded automatically after
> hooks are enabled. Run `distribution/install.sh` without weakening its
> loopback-only networking or token permissions, and verify `/v6/health`.
>
> Merge the matching files from `integrations/` into the user's existing client
> configuration. Preserve unrelated settings and instructions. Replace `~` in
> executable and token paths with the absolute home path where the client
> requires it. Do not add the owner token to the normal agent configuration and
> never print any token contents.
>
> Restart the client and prove enforcement twice: first show that a normal
> session bootstrap succeeds; then stop the Foreman API and show that a new
> prompt is blocked. Start the API again immediately after the failure test.
> Report every file changed and provide the backup, upgrade, and erase commands.

## Client-specific configuration

### Codex

Merge `integrations/codex/config.toml` into `~/.codex/config.toml`. Merge the
SessionStart and UserPromptSubmit hooks instead of overwriting unrelated hooks.
Merge `integrations/codex/AGENTS.md` into `~/.codex/AGENTS.md`.

Codex desktop, CLI, and the IDE extension use the same MCP configuration.
Restart Codex after editing it.

### Claude Code

Merge `integrations/claude/settings.json` into
`~/.claude/settings.json`, add the MCP server from
`integrations/claude/mcp.json` at user scope, and merge
`integrations/claude/CLAUDE.md` into `~/.claude/CLAUDE.md`.

### Claude Cowork

Install the local MCP through Claude Desktop. Local enforcement and retrieval
are unavailable while the desktop app and local Foreman service are not
running. Hosted web/mobile sessions cannot independently enforce a local hook.

## What enforcement means

The setup combines:

1. a required MCP server where the client supports that setting;
2. session-start and per-prompt hooks that call health and bootstrap, returning
   a blocking result when memory is unavailable; and
3. persistent instructions requiring recall before requesting known facts and
   requiring provenance for durable writes.

The session-start hook injects the full bootstrap. Per-prompt hooks verify
bootstrap and automatically record the prompt, but inject only a short digest
to avoid repeatedly filling the context window.

## Common operations

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml ps
docker compose --env-file distribution/.env -f distribution/compose.yaml logs
docker compose --env-file distribution/.env -f distribution/compose.yaml stop
docker compose --env-file distribution/.env -f distribution/compose.yaml start
```

Next, make a tested backup using `docs/BACKUP_RESTORE.md`.
