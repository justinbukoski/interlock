# Upgrading and uninstalling

## Upgrade

1. Create and test a backup using `docs/BACKUP_RESTORE.md`.
2. Pull the desired tagged release or reviewed commit.
3. Rerun the installer:

```sh
git pull --ff-only
./distribution/install.sh
curl -fsS http://127.0.0.1:8851/v6/health
```

The installer reuses existing tokens and stable identities. It rebuilds the
containers, applies forward database migrations, and rebuilds the MCP adapter.
Upgrading from v6.0.0 provisions the new conversation-archive database and its
generated password automatically; existing data is unaffected.
Do not downgrade across database migrations unless that release provides an
explicit downgrade procedure.

## Rotate tokens

Stop all connected agents, replace the desired token file with a new random
32-byte hex token, and rerun `distribution/install.sh` so authorization hashes
are regenerated while identities remain stable:

```sh
openssl rand -hex 32 > ~/.config/foreman/v6-writer-token
chmod 0600 ~/.config/foreman/v6-writer-token
./distribution/install.sh
```

Repeat for reader or owner only when that role must be rotated. Restart clients
after rotation.

## Uninstall

Disable the Foreman hooks and MCP entries in each agent first. Then follow
“Delete everything” in `docs/PRIVACY_AND_DATA.md`. Removing volumes erases all
stored data and cannot be undone without a backup.
