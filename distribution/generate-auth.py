#!/usr/bin/env python3
"""Create stable Interlock identity state and regenerate token grants safely."""

import argparse
import json
import os
import pathlib
import stat
import uuid


def atomic_json(path: pathlib.Path, payload: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_regular_json(path: pathlib.Path) -> dict:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{path.name} must be a regular file")
    return json.loads(path.read_text(encoding="utf-8"))


def load_or_create_identity(identity_path: pathlib.Path, auth_path: pathlib.Path) -> dict[str, str]:
    if identity_path.exists():
        identity = read_regular_json(identity_path)
    elif auth_path.exists():
        grants = read_regular_json(auth_path).get("tokens", [])
        if not grants:
            raise ValueError("existing auth.json has no token grants")
        first = grants[0]
        identity = {
            key: first[key]
            for key in ("tenant_id", "user_id", "consumer_id")
        }
        atomic_json(identity_path, identity)
    else:
        identity = {
            "tenant_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "consumer_id": str(uuid.uuid4()),
        }
        atomic_json(identity_path, identity)

    expected = {"tenant_id", "user_id", "consumer_id"}
    if set(identity) != expected:
        raise ValueError("identity.json has unexpected or missing fields")
    for key, value in identity.items():
        if str(uuid.UUID(value)) != value:
            raise ValueError(f"identity.json contains invalid {key}")
    return identity


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", required=True, type=pathlib.Path)
    parser.add_argument("--reader-hash", required=True)
    parser.add_argument("--writer-hash", required=True)
    parser.add_argument("--owner-hash", required=True)
    args = parser.parse_args()
    args.state_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(args.state_dir, 0o700)

    auth_path = args.state_dir / "auth.json"
    identity = load_or_create_identity(args.state_dir / "identity.json", auth_path)
    common = {
        "tenant_id": identity["tenant_id"],
        "user_id": identity["user_id"],
        "consumer_id": identity["consumer_id"],
    }
    payload = {
        "tokens": [
            {
                **common,
                "token_sha256": args.reader_hash,
                "actor": "local-reader",
                "role": "reader",
            },
            {
                **common,
                "token_sha256": args.writer_hash,
                "actor": "local-agent",
                "role": "writer",
            },
            {
                **common,
                "token_sha256": args.owner_hash,
                "actor": "local-owner",
                "role": "owner",
            },
        ]
    }
    atomic_json(auth_path, payload)


if __name__ == "__main__":
    main()
