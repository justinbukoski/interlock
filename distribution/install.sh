#!/usr/bin/env bash
set -euo pipefail
umask 077

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dist="$root/distribution"
state="$dist/state"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/foreman"
mkdir -p "$state" "$config_dir" "$HOME/.local/bin"

for command in cargo docker git openssl python3; do
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
python3 "$dist/generate-auth.py" \
  --state-dir "$state" \
  --reader-hash "$reader_hash" \
  --writer-hash "$writer_hash" \
  --owner-hash "$owner_hash"

docker compose --env-file "$env_file" -f "$dist/compose.yaml" \
  up -d --build --wait --wait-timeout 900

cargo build --locked --release --bin foreman_mcp
install -m 0755 "$root/target/release/foreman_mcp" "$HOME/.local/bin/foreman-mcp"
install -m 0755 "$dist/memory-gate.py" "$HOME/.local/bin/foreman-memory-gate"

echo
echo "Foreman Memory is running on http://127.0.0.1:8851."
echo "Next: follow docs/INSTALL_WITH_COWORK.md to connect and enforce it."
