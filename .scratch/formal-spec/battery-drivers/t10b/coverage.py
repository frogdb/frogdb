#!/usr/bin/env python3
"""Discrimination evidence: which oracle killed which rows (final verdicts)."""

import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rows import ROWS  # noqa: E402

SCRATCH = os.path.dirname(os.path.abspath(__file__))


def load(n):
    p = os.path.join(SCRATCH, n)
    return json.load(open(p)) if os.path.exists(p) else {}


res1, esc1 = load("fullsync_results.json"), load("escalation.json")
res2, esc2 = load("fullsync_results2.json"), load("escalation2.json")


def verdict_of(rid, res, esc):
    if rid in esc and esc[rid]["verdict"].startswith("CAUGHT"):
        return esc[rid]["verdict"], esc[rid]["evidence"]
    if rid in res:
        v, e = res[rid]["verdict"], res[rid]["evidence"]
        return ("MISSED" if v == "green@500x20" else v), e
    return "?", ""


final = {}
for r in ROWS:
    rid = r["id"]
    v, e = verdict_of(rid, res1, esc1)
    if rid in res2 or rid in esc2:
        v2, e2 = verdict_of(rid, res2, esc2)
        if v2 != v:
            v, e = v2, e2
    final[rid] = (v, e)

tests = defaultdict(list)
invs = defaultdict(list)
for rid, (v, e) in final.items():
    if v == "CAUGHT-T":
        for t in e.split(","):
            t = t.strip()
            if t and t.endswith("Test"):
                tests[t].append(rid)
    elif v == "CAUGHT-P":
        body = e.split(":", 1)[1] if ":" in e else e
        for i in body.split(","):
            i = i.strip()
            if i.startswith("inv_"):
                invs[i].append(rid)

print("### Tests: rows killed\n")
print("| `run` test | rows killed | ids |")
print("|---|---:|---|")
for t, rs in sorted(tests.items(), key=lambda kv: -len(kv[1])):
    print(f"| `{t}` | {len(rs)} | {', '.join(sorted(rs))} |")

print("\n### Invariants: rows falsified\n")
print("| invariant | rows falsified | ids |")
print("|---|---:|---|")
for i, rs in sorted(invs.items(), key=lambda kv: -len(kv[1])):
    print(f"| `{i}` | {len(rs)} | {', '.join(sorted(rs))} |")

print("\nTests firing: %d ; invariants firing: %d" % (len(tests), len(invs)))
