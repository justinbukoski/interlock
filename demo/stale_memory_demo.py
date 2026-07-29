#!/usr/bin/env python3
"""Reproduce the stale-memory incident, then kill it with a validity filter.

The incident, generalized from production: a memory store holds a fact and,
written weeks later, its correction. Similarity search ranks the *stale* fact
above its own correction — because the stale entry is keyword-rich and ranking
has no concept of "superseded" — so the agent confidently answers with
information that was explicitly corrected.

Part 1 shows a vanilla vector store doing exactly this.
Part 2 applies the invariant Interlock enforces: a superseded record is
INELIGIBLE for current-state retrieval. Not down-ranked. Filtered.
Ranking can lose a race; filtering by validity cannot.

Zero dependencies. Run: python3 demo/stale_memory_demo.py
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass, field


# ----------------------------------------------------------------------------
# A tiny deterministic embedder (hashed bag-of-words -> unit vector).
# Stands in for any real embedding model; the failure mode is identical.
# ----------------------------------------------------------------------------

DIM = 256


def embed(text: str) -> list[float]:
    vec = [0.0] * DIM
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        h = int.from_bytes(hashlib.sha256(token.encode()).digest()[:4], "big")
        vec[h % DIM] += 1.0
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [x / norm for x in vec]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


# ----------------------------------------------------------------------------
# The memory records. One stale fact, one correction, some bystanders.
# The stale entry is keyword-rich (real notes usually are); the correction is
# terse (real corrections usually are). That asymmetry is the trap.
# ----------------------------------------------------------------------------


@dataclass
class Record:
    id: int
    text: str
    written_at: str
    superseded_by: int | None = None
    vec: list[float] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.vec = embed(self.text)


MEMORY = [
    Record(
        1,
        "Staging database: the staging environment database runs on "
        "db-legacy.internal port 5432. All staging database connections, "
        "staging migrations, and staging database backups target "
        "db-legacy.internal. Staging database credentials are in the vault.",
        "2026-03-02",
    ),
    Record(
        2,
        "Production deploys happen Tuesdays after the standup.",
        "2026-03-10",
    ),
    Record(
        3,
        "CORRECTION: the staging database moved to db-next.internal on "
        "April 5. db-legacy.internal is decommissioned.",
        "2026-04-05",
        superseded_by=None,
    ),
    Record(
        4,
        "The analytics warehouse is a separate cluster; do not point app "
        "traffic at it.",
        "2026-04-11",
    ),
]

# The correction supersedes record 1. In Interlock this link is written by the
# same transaction that stores the correction — it cannot be forgotten.
MEMORY[0].superseded_by = 3

QUERY = "what is the staging database host?"


def search(records: list[Record], query: str, k: int = 2) -> list[tuple[float, Record]]:
    qv = embed(query)
    scored = sorted(((cosine(qv, r.vec), r) for r in records), key=lambda t: -t[0])
    return scored[:k]


def show(title: str, results: list[tuple[float, Record]]) -> None:
    print(f"\n=== {title}")
    print(f"    query: {QUERY!r}")
    for score, r in results:
        flag = "  <-- STALE, superseded 2026-04-05" if r.superseded_by else ""
        line = f"    {score:.3f}  [{r.written_at}] {r.text}"
        if len(r.text) > 76:
            line = f"    {score:.3f}  [{r.written_at}] {r.text[:76]}..."
        print(line + flag)
    top = results[0][1]
    hosts = re.findall(r"db-[a-z]+\.internal", top.text)
    answer = hosts[0] if hosts else "(no host found in top result)"
    verdict = (
        "WRONG — decommissioned three months ago"
        if top.superseded_by
        else "correct"
    )
    print(f"    agent answers: {answer}  ({verdict})")


def main() -> None:
    print("One memory store. One stale fact (March 2). One correction (April 5).")

    # Part 1: pure similarity ranking. The keyword-rich stale record wins
    # even though its correction sits in the same store.
    show("Part 1: vanilla vector store (ranking only)", search(MEMORY, QUERY))

    # Part 2: the invariant. Superseded records are ineligible for
    # current-state retrieval. Same store, same query, same embeddings.
    current = [r for r in MEMORY if r.superseded_by is None]
    show("Part 2: validity filter (superseded records are ineligible)",
         search(current, QUERY))

    print(
        "\nRanking treated the correction as one more document competing on"
        "\nsimilarity — and it lost to the stale note it corrects. The filter"
        "\nmakes that race unrunnable: a superseded record cannot appear in a"
        "\ncurrent-state result at any rank. Interlock enforces this as a"
        "\nstorage invariant, not a retrieval heuristic:"
        "\nhttps://github.com/justinbukoski/interlock\n"
    )


if __name__ == "__main__":
    main()
