#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Enforce a mutation score from a completed cargo-mutants run.

Score = caught / (caught + missed). Unviable mutants are excluded from the
denominator; timeouts are excluded but reported — a rising timeout share
means the timeout multiplier in .cargo/mutants.toml is wrong, not that the
tests are bad.

The threshold is not typed by hand: it comes from the crate's spec header via
`locked_areas` (`just locked-areas`), the one manifest of what is locked and at
what gate. A hand-typed threshold is how `just mutants-gate frogdb-cluster 0.90`
used to be accepted without complaint against an 0.80 contract. A crate no
locked spec claims is an error rather than a default — it has no contract to
enforce — unless `--min-score` is passed, which stays as the explicit override
for an experiment on an unlocked crate.

Several outcomes files may be named at once and are scored as one run: a large
locked crate is mutated in `cargo mutants --shard k/n` legs by
`mutants-weekly.yml`, and the contract the spec header states is the crate's
score, not each shard's. Summing before dividing is what makes those the same
number.

Usage: mutants-gate.py <outcomes.json>... --crate frogdb-txn [--min-score 0.90]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import locked_areas


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "outcomes",
        type=Path,
        nargs="+",
        help="one or more outcomes.json from mutants.out; counts are summed into one score",
    )
    ap.add_argument("--crate", required=True, help="the mutated crate, looked up in the manifest")
    ap.add_argument(
        "--min-score",
        type=float,
        help="override the crate's spec gate (experiments, unlocked crates)",
    )
    args = ap.parse_args()

    # Resolve the gate before reading the run: a crate outside the perimeter is
    # refused for that reason, not for a missing outcomes file.
    min_score = args.min_score
    if min_score is None:
        try:
            min_score = locked_areas.lookup_crate(args.crate).gate
        except locked_areas.ManifestError as exc:
            sys.exit(f"{exc}\npass --min-score to gate an unlocked crate anyway")

    counts: Counter[str] = Counter()
    previously_caught = 0
    missed_names: list[str] = []
    for path in args.outcomes:
        outcomes = [
            o for o in json.loads(path.read_text())["outcomes"] if o.get("scenario") != "Baseline"
        ]
        counts.update(o["summary"] for o in outcomes)
        missed_names.extend(
            o.get("scenario", {}).get("Mutant", {}).get("name", "?")
            for o in outcomes
            if o.get("summary") == "MissedMutant" and isinstance(o.get("scenario"), dict)
        )
        # An --iterate run only re-tests mutants not already caught; the caught
        # set from earlier passes lives in previously_caught.txt next to that
        # run's outcomes.json — one per file, since each shard iterates its own
        # share — and must count toward the score or an iterated run scores
        # absurdly low.
        prev_file = path.parent / "previously_caught.txt"
        if prev_file.exists():
            previously_caught += sum(
                1 for line in prev_file.read_text().splitlines() if line.strip()
            )

    caught = counts.get("CaughtMutant", 0) + previously_caught
    missed = counts.get("MissedMutant", 0)
    timeout = counts.get("Timeout", 0)
    unviable = counts.get("Unviable", 0)

    denom = caught + missed
    if denom == 0:
        sys.exit("no viable mutants in outcomes — wrong file or empty run?")
    score = caught / denom

    total = sum(counts.values()) + previously_caught
    prev_note = f" ({previously_caught} from earlier --iterate passes)" if previously_caught else ""
    print(
        f"mutants: {total} total, {caught} caught{prev_note}, {missed} missed, "
        f"{unviable} unviable, {timeout} timeout"
    )
    print(f"score: {score:.1%} (gate: {min_score:.1%})")
    if timeout and timeout / (denom + timeout) > 0.05:
        print(
            f"warning: {timeout} timeouts (> 5% of viable) — check "
            "timeout_multiplier in .cargo/mutants.toml",
            file=sys.stderr,
        )

    if score < min_score:
        print("GATE: FAIL", file=sys.stderr)
        for name in missed_names:
            print(f"  missed: {name}", file=sys.stderr)
        sys.exit(1)
    print("GATE: PASS")


if __name__ == "__main__":
    main()
