#!/usr/bin/env bash
set -euo pipefail
umask 077

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dist="$root/distribution"
state="$dist/state"
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/interlock"
mkdir -p "$state" "$config_dir" "$HOME/.local/bin"

for command in cargo docker git openssl python3; do
  command -v "$command" >/dev/null || {
    echo "Missing required command: $command" >&2
    exit 1
  }
done
docker compose version >/dev/null

# Token files use the same names as the client defaults and the integration
# templates. Installs from 6.5.0, which provisioned v6-prefixed names, are
# migrated by COPY (not symlink — the adapter deliberately opens token files
# with O_NOFOLLOW and rejects symlinks), so configurations referencing either
# name keep working. The v6-prefixed names are deprecated; rotate via
# docs/UPGRADING.md, which refreshes both copies.
reader_file="$config_dir/reader-token"
writer_file="$config_dir/writer-token"
owner_file="$config_dir/owner-token"
for name in reader writer owner; do
  new_file="$config_dir/$name-token"
  old_file="$config_dir/v6-$name-token"
  if [[ ! -e "$new_file" && -f "$old_file" && ! -L "$old_file" ]]; then
    cp -p "$old_file" "$new_file"
  fi
  [[ -f "$new_file" ]] || openssl rand -hex 32 >"$new_file"
  # Keep a deprecated-name copy in sync so pre-6.5.1 configurations work.
  if [[ -f "$old_file" && ! -L "$old_file" ]]; then
    cp -p "$new_file" "$old_file"
    chmod 0600 "$old_file"
  fi
done
chmod 0600 "$reader_file" "$writer_file" "$owner_file"

env_file="$dist/.env"
if [[ ! -f "$env_file" ]]; then
  printf 'INTERLOCK_DB_PASSWORD=%s\nINTERLOCK_PORT=8851\nINTERLOCK_UID=%s\nINTERLOCK_GID=%s\n' \
    "$(openssl rand -hex 32)" "$(id -u)" "$(id -g)" >"$env_file"
fi
grep -q '^INTERLOCK_ARCHIVE_DB_PASSWORD=' "$env_file" || \
  printf 'INTERLOCK_ARCHIVE_DB_PASSWORD=%s\n' "$(openssl rand -hex 32)" >>"$env_file"
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

cargo build --locked --release --bin interlock_mcp
install -m 0755 "$root/target/release/interlock_mcp" "$HOME/.local/bin/interlock-mcp"
install -m 0755 "$dist/memory-gate.py" "$HOME/.local/bin/interlock-gate"

interlock_port=$(awk -F= '$1 == "INTERLOCK_PORT" { print $2; exit }' "$env_file")
echo
echo "Interlock is running on http://127.0.0.1:${interlock_port:-8851}."
echo "Next: follow docs/INSTALL_WITH_COWORK.md to connect and enforce it."
