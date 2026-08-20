#!/usr/bin/env python3
"""Prediction vs observation, against the model as committed in 82a4ee22 (pass 1)."""

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rows import ROWS  # noqa: E402

SCRATCH = os.path.dirname(os.path.abspath(__file__))


def load(n):
    p = os.path.join(SCRATCH, n)
    return json.load(open(p)) if os.path.exists(p) else {}


res1, esc1 = load("fullsync_results.json"), load("escalation.json")

pess, opt, hit, miss_pred, na = [], [], [], [], []
for r in ROWS:
    rid = r["id"]
    v = res1.get(rid, {}).get("verdict", "?")
    ev = res1.get(rid, {}).get("evidence", "")
    if rid in esc1 and esc1[rid]["verdict"].startswith("CAUGHT"):
        v, ev = esc1[rid]["verdict"], esc1[rid]["evidence"]
    if v == "green@500x20":
        v = "MISSED"
    exp = r["exp"]
    predicted_miss = "pre-registered miss" in exp or "pre-registered N/A" in exp
    names = set(re.findall(r"\b(?:inv_\w+|\w+Test)\b", exp))
    fired = set(re.findall(r"\b(?:inv_\w+|\w+Test)\b", ev))
    if predicted_miss:
        (pess if v.startswith("CAUGHT") else na).append((rid, v, ev))
    elif v == "MISSED":
        opt.append((rid, exp))
    elif names & fired:
        hit.append(rid)
    else:
        miss_pred.append((rid, exp, ev))

print("predicted-a-catcher and that exact catcher fired: %d" % len(hit))
print("predicted-a-catcher, caught by a DIFFERENT oracle: %d" % len(miss_pred))
for rid, e, ev in miss_pred:
    print(f"   {rid}: predicted {e[:60]} | actual {ev[:70]}")
print("predicted MISS/N/A, and it was: %d" % len(na))
print("predicted MISS/N/A, but CAUGHT anyway (pessimistic): %d" % len(pess))
for rid, v, ev in pess:
    print(f"   {rid}: {v} {ev[:70]}")
print("predicted a catcher, but MISSED (real gaps): %d" % len(opt))
for rid, e in opt:
    print(f"   {rid}: predicted {e[:80]}")
