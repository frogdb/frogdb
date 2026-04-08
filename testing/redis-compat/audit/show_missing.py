#!/usr/bin/env python3
"""Inspect specific missing tests from an audit JSON."""

import json
import sys

with open("/tmp/claude/audit_results.json") as f:
    data = json.load(f)["ported"]

target = sys.argv[1] if len(sys.argv) > 1 else "wait_tcl.rs"
info = data[target]
print(f"{target}: {info['missing_total']} missing out of {info['upstream_total']}")
for m in info["missing"]:
    print(f"  - {m['name']!r}")
    print(f"    line={m['line']}  tags={m['tags']}")
    print(f"    best_match={m['best_match']} ({m['best_score']})")
