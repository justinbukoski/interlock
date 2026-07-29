# Owner administration

The installer creates three credentials:

- reader: bootstrap, recall, and history;
- writer: observations, normal memories, corrections, and handoffs;
- owner: owner-authority canonical records and system constraints/directives.

The supplied Codex and Claude configurations intentionally expose only reader
and writer credentials. A routine agent therefore cannot silently elevate a
memory into an owner decision or create a system-wide directive.

For a deliberate administrative session, make a separate copy of the MCP
configuration and add:

```text
INTERLOCK_OWNER_TOKEN_FILE=/absolute/path/to/.config/interlock/owner-token
```

Restart that client, perform the narrow administrative change, review the audit
record, and remove or disable the owner-enabled MCP entry afterward. Never
commit, paste, or print the token. The owner token file must remain mode `0600`
and owned by the current user.
