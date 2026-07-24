# Backup and restore

Run these commands from the repository root. Store backups somewhere encrypted
and access-controlled; they contain the complete conversation-derived memory.

## Create a logical backup

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres pg_dump -U foreman -d foreman -Fc > foreman-backup.dump
```

Confirm that `foreman-backup.dump` is non-empty and copy it off the machine.

## Test the backup without replacing live data

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres createdb -U foreman foreman_restore_test
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres pg_restore -U foreman -d foreman_restore_test \
  < foreman-backup.dump
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres psql -U foreman -d foreman_restore_test \
  -c 'select count(*) from memory_items;'
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres dropdb -U foreman foreman_restore_test
```

If a test database with that name already exists, investigate it rather than
dropping it blindly.

## Restore a fresh installation

Install Foreman on the destination, stop the API, replace the empty database,
restore the archive, and restart:

```sh
docker compose --env-file distribution/.env -f distribution/compose.yaml stop api
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres dropdb -U foreman --force foreman
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres createdb -U foreman foreman
docker compose --env-file distribution/.env -f distribution/compose.yaml \
  exec -T postgres pg_restore -U foreman -d foreman < foreman-backup.dump
docker compose --env-file distribution/.env -f distribution/compose.yaml start api
curl -fsS http://127.0.0.1:8851/v6/health
```

The drop step permanently replaces the destination database. Use it only on the
intended fresh destination and only after testing the archive.

The backup does not contain token files or the installation identity file.
Copy `~/.config/foreman/` and `distribution/state/identity.json` separately
through a secure channel if clients must retain the same identity and tokens.
