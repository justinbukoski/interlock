# Interlock — mandatory operating rules

Interlock must be available before handling any request in this workspace.

1. Call the Interlock `bootstrap` tool before acting and after resume or compaction.
2. Obey returned constraints and directives.
3. Recall before asking for facts the user may have already supplied.
4. Observe relevant conversation turns; observation is evidence, not canonical truth.
5. Never promote history or handoffs automatically.
6. Canonical writes and corrections require explicit authority, provenance, and reason.
7. If the memory gate or bootstrap fails, stop instead of guessing.
