# Privacy and data controls

## Automatic capture

Foreman's value depends on continuity without requiring the user to manually
mark every useful statement. Once the UserPromptSubmit hook is enabled, every
user prompt is recorded automatically with its project, thread, session, and
timestamp. There is no per-prompt approval dialog.

Assistant output is not automatically captured by the supplied hook. Agents are
instructed to record relevant observations and durable facts through MCP, so
their behavior determines which additional material is stored.

Do not enable the hooks for someone until they have seen this disclosure.
Avoid entering passwords, private keys, recovery codes, or other secrets into
any AI prompt. Foreman's supported redaction is defense in depth, not a
guarantee that every possible secret format will be recognized.

## Where data lives

By default, the API is available only on the local loopback interface. Memory,
observations, history, audit records, and embeddings live in the Docker volume
`foreman-memory_foreman-postgres`. Model files live in
`foreman-memory_foreman-models`. Access tokens live in
`~/.config/foreman/` and generated authorization state lives under
`distribution/state/`.

Nothing in the package intentionally uploads stored memory to a hosted service.
The embedder downloads its model during the first installation.

## Retention

This preview retains captured data until the operator deletes it. The
conversation archive supports full export (`/v6.5/archive/export`) and an
owner-token deletion saga (`/v6.5/archive/delete`) for erasing archive
content. Canonical memory lanes do not yet provide age-based expiry or a
granular-delete API. Treat that as an explicit product limitation when
deciding what environments to use it in.

## Export

Use the logical PostgreSQL backup procedure in `docs/BACKUP_RESTORE.md`. That
archive is the complete portable export of stored Foreman data. Protect it as
carefully as the original database.

## Delete everything

First make an export if it may be needed later. Then, from the repository:

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml down -v
```

This permanently deletes the database and downloaded-model volumes for this
Compose project. To also remove local credentials and generated authorization
state, delete only these Foreman paths:

```sh
rm -rf ~/.config/foreman
rm -rf distribution/state
rm -f distribution/.env
rm -f ~/.local/bin/foreman-mcp ~/.local/bin/foreman-memory-gate
```

Review the paths before running them. This operation is not recoverable without
a backup.
