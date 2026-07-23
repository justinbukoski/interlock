#!/usr/bin/env bash
set -euo pipefail
umask 077

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dist="$root/distribution"
state="$dist/state"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/foreman"
mkdir -p "$state" "$config_dir" "$HOME/.local/bin"

for command in cargo docker openssl python3; do
  command -v "$command" >/dev/null || {
    echo "Missing required command: $command" >&2
    exit 1
  }
done
docker compose version >/dev/null

reader_file="$config_dir/v6-reader-token"
writer_file="$config_dir/v6-writer-token"
owner_file="$config_dir/v6-owner-token"
[[ -f "$reader_file" ]] || openssl rand -hex 32 >"$reader_file"
[[ -f "$writer_file" ]] || openssl rand -hex 32 >"$writer_file"
[[ -f "$owner_file" ]] || openssl rand -hex 32 >"$owner_file"
chmod 0600 "$reader_file" "$writer_file" "$owner_file"

env_file="$dist/.env"
if [[ ! -f "$env_file" ]]; then
  printf 'FOREMAN_DB_PASSWORD=%s\nFOREMAN_PORT=8851\nFOREMAN_UID=%s\nFOREMAN_GID=%s\n' \
    "$(openssl rand -hex 32)" "$(id -u)" "$(id -g)" >"$env_file"
fi
chmod 0600 "$env_file"

sha256() {
  if command -v sha256sum >/dev/null; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

reader_hash=$(tr -d '\r\n' <"$reader_file" | sha256)
writer_hash=$(tr -d '\r\n' <"$writer_file" | sha256)
owner_hash=$(tr -d '\r\n' <"$owner_file" | sha256)
tenant_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
user_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
consumer_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
python3 - "$state/auth.json" "$reader_hash" "$writer_hash" "$owner_hash" "$tenant_id" "$user_id" "$consumer_id" <<'PY'
import json
import os
import sys

path, reader, writer, owner, tenant, user, consumer = sys.argv[1:]
payload = {
    "tokens": [
        {
            "token_sha256": reader,
            "tenant_id": tenant,
            "user_id": user,
            "consumer_id": consumer,
            "actor": "local-reader",
            "role": "reader",
        },
        {
            "token_sha256": writer,
            "tenant_id": tenant,
            "user_id": user,
            "consumer_id": consumer,
            "actor": "local-observer",
            "role": "writer",
        },
        {
            "token_sha256": owner,
            "tenant_id": tenant,
            "user_id": user,
            "consumer_id": consumer,
            "actor": "local-owner",
            "role": "owner",
        },
    ]
}
temporary = path + ".tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(payload, handle)
    handle.write("\n")
os.chmod(temporary, 0o600)
os.replace(temporary, path)
PY

docker compose --env-file "$env_file" -f "$dist/compose.yaml" up -d --build
docker compose --env-file "$env_file" -f "$dist/compose.yaml" wait api

cargo build --locked --release --bin foreman_mcp
install -m 0755 "$root/target/release/foreman_mcp" "$HOME/.local/bin/foreman-mcp"
install -m 0755 "$dist/memory-gate.py" "$HOME/.local/bin/foreman-memory-gate"

echo
echo "Foreman Memory is running on http://127.0.0.1:8851."
echo "Next: follow docs/INSTALL_WITH_COWORK.md to connect and enforce it."
