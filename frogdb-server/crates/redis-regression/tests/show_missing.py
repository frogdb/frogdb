#!/usr/bin/env python3
"""Inspect specific missing tests from an audit JSON.

Usage:
    show_missing.py <port> [--gaps|--all|--documented|--tag-excluded|--by-category]

Default mode is `--gaps` (only unclassified missing tests, which are the
candidates for either porting or documenting as intentional exclusions).
"""

import json
import sys
from collections import Counter

with open("/tmp/claude/audit_results.json") as f:
    data = json.load(f)["ported"]

if len(sys.argv) < 2:
    print(
        "usage: show_missing.py <port_file.rs> "
        "[--gaps|--all|--documented|--tag-excluded|--by-category]"
    )
    sys.exit(1)

target = sys.argv[1]
mode = sys.argv[2] if len(sys.argv) > 2 else "--gaps"

# --by-category works across all ports or a single port
if mode == "--by-category":
    category_counts: Counter[str] = Counter()
    broken_count = 0
    ports = [target] if target != "all" else list(data.keys())
    for port in ports:
        if port not in data:
            print(f"unknown port: {port!r}")
            sys.exit(1)
        info = data[port]
        for m in info["missing"]:
            cat = m.get("exclusion_category")
            if cat:
                category_counts[cat] += 1
        broken_count += info.get("broken_count", 0)

    scope = target if target != "all" else "all ports"
    print(f"Category breakdown for {scope}:\n")
    for cat, count in category_counts.most_common():
        print(f"  {count:4d}  {cat}")
    if broken_count:
        print(f"  {broken_count:4d}  broken (#[ignore])")
    total = sum(category_counts.values()) + broken_count
    print(f"\n  {total:4d}  TOTAL")
    sys.exit(0)

if target not in data:
    print(f"unknown port: {target!r}")
    print(f"available: {', '.join(sorted(data.keys()))}")
    sys.exit(1)

info = data[target]
upstream_total = info["upstream_total"]
matched = info["matched"]
documented = info.get("documented_count", 0)
broken = info.get("broken_count", 0)
print(
    f"{target}: {info['missing_total']} missing out of {upstream_total} "
    f"(matched={matched}, documented={documented}, broken={broken})"
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
        cat = m.get("exclusion_category", "")
        cat_str = f" [{cat}]" if cat else ""
        print(f"    exclusion={m['exclusion']!r}{cat_str}: {m.get('exclusion_reason')}")
    print()

print(f"shown: {count}")
