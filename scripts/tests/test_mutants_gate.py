#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for the mutation-score gate's arithmetic.

Run: ./scripts/tests/test_mutants_gate.py   (or `just test-mutants-gate`)

`mutants-weekly.yml` shards a large locked crate across several
`cargo mutants --shard k/n` legs and hands the gate every shard's
`outcomes.json` at once. The score of a sharded run is the score of the crate,
not one score per shard, so the summing is the thing worth pinning: two shards
at 3 caught + 1 missed each are one 75% run, not two 75% runs. The real run is a
four-hour weekly job that only ever scores whatever the tree happens to hold, so
each arithmetic rule is pinned here against synthetic outcomes files instead.

Every case passes `--min-score` so the fixtures do not move when a spec header's
`Gate:` does — the manifest lookup is `test_lint_locked_areas.py`'s subject.

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on the first
failure (same shape as test_lint_locked_areas.py).
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

GATE = Path(__file__).resolve().parent.parent / "mutants-gate.py"


def _outcomes(directory: Path, *, caught: int, missed: int, tag: str) -> Path:
    """Write one shard's `outcomes.json`, with a baseline entry the gate must ignore."""
    directory.mkdir(parents=True, exist_ok=True)
    entries: list[dict[str, object]] = [{"scenario": "Baseline", "summary": "Success"}]
    entries += [
        {"scenario": {"Mutant": {"name": f"{tag}-caught-{i}"}}, "summary": "CaughtMutant"}
        for i in range(caught)
    ]
    entries += [
        {"scenario": {"Mutant": {"name": f"{tag}-missed-{i}"}}, "summary": "MissedMutant"}
        for i in range(missed)
    ]
    path = directory / "outcomes.json"
    path.write_text(json.dumps({"outcomes": entries}))
    return path


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """Invoke the gate the way the weekly workflow's gate job does — as a subprocess."""
    return subprocess.run([str(GATE), *args], capture_output=True, text=True, check=False)


def test_two_shards_score_as_one_run() -> None:
    """Sharded legs are summed: 3+1 twice is one 75% run, not two 75% runs."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        a = _outcomes(root / "shard-0", caught=3, missed=1, tag="a")
        b = _outcomes(root / "shard-1", caught=3, missed=1, tag="b")
        done = _run(str(a), str(b), "--crate", "frogdb-txn", "--min-score", "0.90")
        assert done.returncode == 1, (done.returncode, done.stdout, done.stderr)
        assert "mutants: 8 total, 6 caught, 2 missed, 0 unviable, 0 timeout" in done.stdout, (
            done.stdout
        )
        assert "score: 75.0% (gate: 90.0%)" in done.stdout, done.stdout
        # Both shards' survivors are named, or half the run's evidence is lost.
        assert "missed: a-missed-0" in done.stderr, done.stderr
        assert "missed: b-missed-0" in done.stderr, done.stderr


def test_a_single_outcomes_file_still_works() -> None:
    """`just mutants-gate <crate>` passes exactly one file; it must keep scoring."""
    with tempfile.TemporaryDirectory() as tmp:
        only = _outcomes(Path(tmp) / "whole", caught=3, missed=1, tag="only")
        done = _run(str(only), "--crate", "frogdb-txn", "--min-score", "0.70")
        assert done.returncode == 0, (done.returncode, done.stdout, done.stderr)
        assert "mutants: 4 total, 3 caught, 1 missed, 0 unviable, 0 timeout" in done.stdout, (
            done.stdout
        )
        assert "score: 75.0% (gate: 70.0%)" in done.stdout, done.stdout
        assert "GATE: PASS" in done.stdout, done.stdout


def test_previously_caught_beside_one_file_counts() -> None:
    """An `--iterate` shard's earlier catches live beside *that* file, and still count."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        a = _outcomes(root / "shard-0", caught=1, missed=1, tag="a")
        (a.parent / "previously_caught.txt").write_text("a-caught-earlier\n\n")
        b = _outcomes(root / "shard-1", caught=1, missed=1, tag="b")
        done = _run(str(a), str(b), "--crate", "frogdb-txn", "--min-score", "0.60")
        assert done.returncode == 0, (done.returncode, done.stdout, done.stderr)
        assert "3 caught (1 from earlier --iterate passes)" in done.stdout, done.stdout
        assert "score: 60.0% (gate: 60.0%)" in done.stdout, done.stdout


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
