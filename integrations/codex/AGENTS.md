# Interlock — mandatory operating rules

Interlock is a hard prerequisite for every request in this workspace.

1. Call `interlock-v6.bootstrap` before acting, including after resume or compaction.
2. Read constraints and directives first and obey them as mandatory.
3. Call `interlock-v6.recall` before asking the user for previously known facts.
4. Use `history` only as evidence. History and handoffs never become canonical memory.
5. Record relevant conversation turns with `observe`. Observation is not authority.
6. Use `remember` or `correct` only with explicit authority, provenance, and reason.
7. If Interlock or bootstrap is unavailable, stop. Do not answer from guessed context.

The lifecycle gate is a second control, not a substitute for these rules.
