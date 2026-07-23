# Install Foreman Memory with an AI coworker

Foreman v6 is a self-hosted memory service for coding agents. This friend
release is a **preview**: storage, scoped recall, BGE embeddings, observations,
candidate review, canonical writes, corrections, history, and handoffs work.
The unattended extractor that turns raw chat observations into reviewed fact
candidates is not yet included. User prompts are observed automatically through
hooks and agents are instructed to observe relevant outputs, but facts are not
silently promoted.

## Hardware

- Minimum: 4 CPU cores, 8 GB RAM, 15 GB free disk.
- Comfortable: 8 CPU cores, 16 GB RAM, 30 GB free disk.
- GPU: optional. CPU embedding works; an NVIDIA or Apple GPU can reduce latency,
  but this Docker preview does not require one.
- Software: Docker Desktop or Docker Engine with Compose, Git, Rust/Cargo,
  Python 3, and OpenSSL.

The first start downloads the roughly 1.3 GB BGE-large model and builds the Rust
service, so it takes longer than later starts.

## Prompt to hand to GPT Cowork or Claude Cowork

Copy the following prompt into a desktop Cowork session with this repository's
folder attached:

> Install this Foreman Memory repository locally. Read README.md and
> docs/INSTALL_WITH_COWORK.md first. Verify Docker, Compose, Rust/Cargo, Python 3,
> and OpenSSL are installed. Run `distribution/install.sh` without weakening its
> loopback-only networking or token permissions. Verify the `/v6/health` endpoint.
> Then configure the client I am using:
>
> - For Codex/GPT Cowork, merge `integrations/codex/config.toml` into my
>   `~/.codex/config.toml`, replacing every `~` in MCP command and token paths
>   with my absolute home path. Merge the SessionStart and UserPromptSubmit hooks;
>   do not overwrite unrelated settings. Put the contents of
>   `integrations/codex/AGENTS.md` in `~/.codex/AGENTS.md`, preserving existing
>   instructions.
> - For Claude Code, merge `integrations/claude/settings.json` into
>   `~/.claude/settings.json`, add the MCP server from
>   `integrations/claude/mcp.json` at user scope, and merge
>   `integrations/claude/CLAUDE.md` into `~/.claude/CLAUDE.md`. Replace `~` in
>   executable and token paths with my absolute home path.
> - For Claude Cowork, install the local MCP through Claude Desktop. Explain that
>   web/mobile Cowork cannot enforce a local hook while the desktop app is closed.
>
> Restart the client, trust the installed hooks after showing me their exact
> commands, and prove enforcement with two tests: normal bootstrap succeeds, then
> stopping the Foreman API causes a new prompt to be blocked. Restart the API
> after the failure test. Never print token contents.

## What “enforced” means

The setup uses three controls:

1. the MCP server is marked required where the client supports it;
2. a session-start and per-prompt hook calls health and bootstrap, returning
   `continue: false` and a nonzero exit when memory is unavailable;
3. persistent `AGENTS.md` or `CLAUDE.md` rules require recall and safe writes.

Project hooks only run in trusted projects. For stronger Codex enforcement,
install the hooks at user scope. Enterprise Codex administrators can place the
same hooks in managed `requirements.toml`, pin `[features].hooks = true`, and set
`allow_managed_hooks_only = true`. Prompt instructions alone are not a security
boundary, and specialized hosted tools may not traverse local hook paths.

Claude Desktop is required for local MCP access from Claude Cowork. Claude Code
supports the checked-in `.claude/settings.json` and `.mcp.json` workflow
directly.

## Operations

From the repository:

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml ps
docker compose --env-file distribution/.env -f distribution/compose.yaml logs
docker compose --env-file distribution/.env -f distribution/compose.yaml stop
docker compose --env-file distribution/.env -f distribution/compose.yaml start
```

Back up the `foreman-postgres` Docker volume. The token files live under
`~/.config/foreman`; do not copy or commit them.
