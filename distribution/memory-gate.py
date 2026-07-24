#!/usr/bin/env python3
"""Fail-closed session/prompt gate for Codex and Claude Code hooks."""

import hashlib
import json
import os
import pathlib
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone


def project_key(cwd: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", cwd, "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return "path:" + str(pathlib.Path(cwd).resolve())
    value = result.stdout.strip()
    if value:
        value = value.removesuffix(".git")
        return "git:" + value
    return "path:" + str(pathlib.Path(cwd).resolve())


def request(path: str, token: str, payload: dict | None = None) -> dict:
    base = os.environ.get("FOREMAN_V6_URL", "http://127.0.0.1:8851").rstrip("/")
    body = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base + path,
        data=body,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
        method="GET" if body is None else "POST",
    )
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def main() -> int:
    event = json.load(sys.stdin)
    cwd = event.get("cwd") or os.getcwd()
    token_path = pathlib.Path(
        os.environ.get(
            "FOREMAN_V6_READER_TOKEN_FILE",
            pathlib.Path.home() / ".config/foreman/v6-reader-token",
        )
    )
    try:
        token = token_path.read_text(encoding="utf-8").strip()
        health = request("/v6/health", token)
        if health.get("status") != "ok":
            raise RuntimeError("health response was not ok")
        scope = {"project_key": project_key(cwd)}
        session_id = event.get("session_id")
        if session_id:
            scope["thread_id"] = session_id
            scope["session_id"] = session_id
        bootstrap = request(
            "/v6/bootstrap",
            token,
            {"scope": scope, "token_budget": 4096},
        )
        prompt = event.get("prompt")
        if event.get("hook_event_name") == "UserPromptSubmit" and isinstance(prompt, str) and prompt.strip():
            writer_path = pathlib.Path(
                os.environ.get(
                    "FOREMAN_V6_WRITER_TOKEN_FILE",
                    pathlib.Path.home() / ".config/foreman/v6-writer-token",
                )
            )
            writer_token = writer_path.read_text(encoding="utf-8").strip()
            source_event_id = event.get("turn_id") or str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"{session_id}:{prompt}")
            )
            request(
                "/v6/observations",
                writer_token,
                {
                    "request_id": str(uuid.uuid4()),
                    "source_event_id": source_event_id,
                    "event_kind": "user_prompt",
                    "scope": scope,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                    "content": prompt,
                    "raw_content_ref": None,
                },
            )
    except (OSError, ValueError, RuntimeError, urllib.error.URLError) as error:
        message = f"FOREMAN MEMORY GATE FAILED: {error}. Restore memory before continuing."
        print(message, file=sys.stderr)
        print(json.dumps({"continue": False, "stopReason": message, "systemMessage": message}))
        return 2

    digest = hashlib.sha256(json.dumps(bootstrap, sort_keys=True).encode()).hexdigest()[:12]
    if event.get("hook_event_name") == "UserPromptSubmit":
        context = (
            "Foreman memory gate succeeded and this user prompt was recorded. "
            f"Bootstrap digest: {digest}. Use foreman recall before asking for known facts."
        )
    else:
        context = (
            "Foreman memory bootstrap succeeded. Treat returned constraints/directives as "
            f"mandatory. Bootstrap digest: {digest}. Bootstrap: {json.dumps(bootstrap)}"
        )
    print(
        json.dumps(
            {
                "continue": True,
                "systemMessage": context,
                "hookSpecificOutput": {
                    "hookEventName": event.get("hook_event_name", "SessionStart"),
                    "additionalContext": context,
                },
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
