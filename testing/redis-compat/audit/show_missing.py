#!/usr/bin/env python3
"""Inspect specific missing tests from an audit JSON.

Usage:
    show_missing.py <port> [--gaps|--all|--documented|--tag-excluded]

Default mode is `--gaps` (only unclassified missing tests, which are the
candidates for either porting or documenting as intentional exclusions).
"""

import json
import sys

with open("/tmp/claude/audit_results.json") as f:
    data = json.load(f)["ported"]

if len(sys.argv) < 2:
    print("usage: show_missing.py <port_file.rs> [--gaps|--all|--documented|--tag-excluded]")
    sys.exit(1)

target = sys.argv[1]
mode = sys.argv[2] if len(sys.argv) > 2 else "--gaps"

if target not in data:
    print(f"unknown port: {target!r}")
    print(f"available: {', '.join(sorted(data.keys()))}")
    sys.exit(1)

info = data[target]
upstream_total = info["upstream_total"]
matched = info["matched"]
documented = info.get("documented_count", 0)
print(
    f"{target}: {info['missing_total']} missing out of {upstream_total} "
    f"(matched={matched}, documented={documented})"
)
print(f"mode: {mode}")
print()


def passes(record: dict) -> bool:
    ex = record.get("exclusion")
    if mode == "--all":
        return True
    if mode == "--gaps":
        return ex is None
    if mode == "--documented":
        return ex == "documented"
    if mode == "--tag-excluded":
        return ex is not None and ex != "documented"
    raise SystemExit(f"unknown mode: {mode}")


count = 0
for m in info["missing"]:
    if not passes(m):
        continue
    count += 1
    print(f"  - {m['name']!r}")
    print(f"    line={m['line']}  tags={m['tags']}")
    print(f"    best_match={m['best_match']} (score={m['best_score']})")
    if m.get("exclusion"):
        print(f"    exclusion={m['exclusion']!r}: {m.get('exclusion_reason')}")
    print()

print(f"shown: {count}")
