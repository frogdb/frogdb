#!/usr/bin/env python3
"""Render the battery table from rows.py + the two result passes.

Pass 1: fullsync_results.json (+ escalation.json) — verdicts against the model as
        committed in 82a4ee22, i.e. BEFORE gap closure.
Pass 2: fullsync_results2.json (+ escalation2.json) — the same rows re-run against
        the closed model. Only rows that were MISSED in pass 1 are in pass 2.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rows import ROWS  # noqa: E402

SCRATCH = os.path.dirname(os.path.abspath(__file__))


def load(name):
    p = os.path.join(SCRATCH, name)
    return json.load(open(p)) if os.path.exists(p) else {}


res1, esc1 = load("fullsync_results.json"), load("escalation.json")
res2, esc2 = load("fullsync_results2.json"), load("escalation2.json")

# Rows that are not a well-formed single semantic edit: a sizing knob, or a value
# no guard reads. Justified in the report body.
NA = {
    "M05": "sizing/steering only — `freeSids` picks *which* slot, no guard reads the choice",
    "T06": "sizing knob — MAX_WRITES bounds the state space, it is not a semantic claim",
}

FILE_SHORT = {
    "specs/quint/replication_fullsync_logic.qnt": "logic",
    "specs/quint/replication_fullsync_machine.qnt": "machine",
    "specs/quint/replication_fullsync_types.qnt": "types",
    "specs/quint/replication_fullsync.qnt": "main",
}


def one_line(s):
    return s.replace("\n", " ⏎ ").replace("|", "\\|").strip()


def clip(s, n=110):
    s = one_line(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def verdict_of(rid, res, esc):
    if rid in esc and esc[rid]["verdict"].startswith("CAUGHT"):
        return esc[rid]["verdict"], esc[rid]["evidence"]
    if rid in res:
        v, e = res[rid]["verdict"], res[rid]["evidence"]
        return ("MISSED" if v == "green@500x20" else v), e
    return "?", ""


rows_out = []
counts = {}
changed = []
for r in ROWS:
    rid = r["id"]
    v1, e1 = verdict_of(rid, res1, esc1)
    v, ev = v1, e1
    if rid in res2 or rid in esc2:
        v2, e2 = verdict_of(rid, res2, esc2)
        if v2 != v1:
            v, ev = v2, e2
            changed.append((rid, v1, v2, e2))
    if rid in NA:
        v, ev = "N/A", NA[rid]
    shown = v if v == v1 else f"{v1} → **{v}**"
    counts[v] = counts.get(v, 0) + 1
    mut = f"`{clip(r['old'], 70)}` → `{clip(r['new'], 70)}`"
    rows_out.append(
        f"| {rid} | {FILE_SHORT[r['f']]}: {r['loc']} | {mut} | {clip(r['exp'], 90)} | {shown} | {clip(ev, 90)} |"
    )

print(
    "| Row | Target (file: the claim the edit breaks) | Mutation (old → new) | Expected catcher (pre-registered) | Verdict | Evidence |"
)
print("|---|---|---|---|---|---|")
print("\n".join(rows_out))
print()
print("Totals: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
print("Closed by gap closure: %d" % len(changed))
for rid, a, b, e in changed:
    print(f"  {rid}: {a} -> {b} ({e})")
