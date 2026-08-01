#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Enforce a mutation score from a completed cargo-mutants run.

Score = caught / (caught + missed). Unviable mutants are excluded from the
denominator; timeouts are excluded but reported — a rising timeout share
means the timeout multiplier in .cargo/mutants.toml is wrong, not that the
tests are bad.

Usage: mutants-gate.py <outcomes.json> --min-score 0.90
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("outcomes", type=Path, help="outcomes.json from mutants.out")
    ap.add_argument("--min-score", type=float, required=True)
    args = ap.parse_args()

    data = json.loads(args.outcomes.read_text())
    counts = Counter(o["summary"] for o in data["outcomes"] if o.get("scenario") != "Baseline")

    caught = counts.get("CaughtMutant", 0)
    missed = counts.get("MissedMutant", 0)
    timeout = counts.get("Timeout", 0)
    unviable = counts.get("Unviable", 0)

    denom = caught + missed
    if denom == 0:
        sys.exit("no viable mutants in outcomes — wrong file or empty run?")
    score = caught / denom

    total = sum(counts.values())
    print(
        f"mutants: {total} total, {caught} caught, {missed} missed, "
        f"{unviable} unviable, {timeout} timeout"
    )
    print(f"score: {score:.1%} (gate: {args.min_score:.1%})")
    if timeout and timeout / (denom + timeout) > 0.05:
        print(
            f"warning: {timeout} timeouts (> 5% of viable) — check "
            "timeout_multiplier in .cargo/mutants.toml",
            file=sys.stderr,
        )

    if score < args.min_score:
        print("GATE: FAIL", file=sys.stderr)
        for o in data["outcomes"]:
            if o.get("summary") == "MissedMutant":
                sc = o.get("scenario", {})
                if isinstance(sc, dict):
                    print(f"  missed: {sc.get('Mutant', {}).get('name', '?')}", file=sys.stderr)
        sys.exit(1)
    print("GATE: PASS")


if __name__ == "__main__":
    main()
