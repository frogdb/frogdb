#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Record the agent inner-loop cost for one hardening area.

Touches the area's primary source file, then times two warm incremental
steps (median of N runs each): `cargo check --all-targets` (type-check
feedback) and `cargo nextest list` (test-binary codegen + link — the real
"edit -> can run a test" latency). Appends a dated row to
.scratch/hardening/metrics/loop-cost.md.

The test count travels with the timing so a "faster" loop that silently
lost tests is visible in the same row.

Update AREAS as extraction phases land (e.g. txn -> frogdb-txn with
`--no-default-features --features core-profile`): the point of the file
is comparing rows across those transitions.
"""

from __future__ import annotations

import argparse
import os
import platform
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
METRICS = REPO / ".scratch/hardening/metrics/loop-cost.md"

# area -> (crate to check, file to touch, extra cargo args)
AREAS: dict[str, tuple[str, str, list[str]]] = {
    "txn": (
        "frogdb-txn",
        "frogdb-server/crates/txn/src/exec.rs",
        [],
    ),
    "persistence": (
        "frogdb-recovery",
        "frogdb-server/crates/recovery/src/lib.rs",
        [],
    ),
    "replication": (
        "frogdb-server",
        "frogdb-server/crates/server/src/replication/executor.rs",
        [],
    ),
    "cluster": (
        "frogdb-server",
        "frogdb-server/crates/server/src/failure_detector.rs",
        [],
    ),
}

HEADER = (
    "# Hardening campaign: inner-loop cost\n\n"
    "Warm incremental medians after touching the area's primary file: "
    "`check` = `cargo check --all-targets` (type-check feedback), "
    "`test build` = `cargo nextest list` (test-binary codegen + link, the "
    "real edit->run-a-test latency), plus the area crate's test count. "
    "Recorded by `just loop-cost <area>` (scripts/loop-cost.py).\n\n"
    "| date | rev | area | crate | check (s) | test build (s) | tests |\n"
    "|---|---|---|---|---|---|---|\n"
)


def build_env() -> dict[str, str]:
    env = os.environ.copy()
    if platform.system() == "Darwin":
        env.setdefault("LIBCLANG_PATH", "/opt/homebrew/opt/llvm/lib")
        env.setdefault("DYLD_LIBRARY_PATH", "/opt/homebrew/opt/llvm/lib")
        if env.get("FROGDB_SYSTEM_ROCKSDB", "1") != "":
            lib = env.get("FROGDB_LIB_DIR", "/opt/homebrew/lib")
            env.setdefault("ROCKSDB_LIB_DIR", lib)
            env.setdefault("SNAPPY_LIB_DIR", lib)
    return env


def run(cmd: list[str], env: dict[str, str], capture: bool = False) -> str:
    res = subprocess.run(
        cmd,
        cwd=REPO,
        env=env,
        text=True,
        capture_output=capture,
        check=False,
    )
    if res.returncode != 0:
        if capture:
            sys.stderr.write(res.stderr or "")
        sys.exit(f"command failed ({res.returncode}): {' '.join(cmd)}")
    return res.stdout if capture else ""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("area", choices=sorted(AREAS))
    ap.add_argument("--runs", type=int, default=3)
    args = ap.parse_args()

    crate, touch_file, extra = AREAS[args.area]
    touch = REPO / touch_file
    if not touch.exists():
        sys.exit(f"touch target missing (update AREAS?): {touch_file}")

    env = build_env()
    check_cmd = ["cargo", "check", "-p", crate, *extra, "--all-targets"]
    list_cmd = ["cargo", "nextest", "list", "-p", crate, *extra]

    # Warm the cache so run 1 isn't an outlier from a cold dependency graph.
    run(check_cmd, env)
    listing = run(list_cmd, env, capture=True)

    check_times: list[float] = []
    build_times: list[float] = []
    for i in range(args.runs):
        touch.touch()
        t0 = time.monotonic()
        run(check_cmd, env)
        check_times.append(time.monotonic() - t0)
        touch.touch()
        t0 = time.monotonic()
        listing = run(list_cmd, env, capture=True)
        build_times.append(time.monotonic() - t0)
        print(
            f"run {i + 1}/{args.runs}: check {check_times[-1]:.1f}s, "
            f"test build {build_times[-1]:.1f}s",
            file=sys.stderr,
        )

    # Non-tty nextest list output: one "<binary> <test path>" line per test.
    tests = sum(1 for line in listing.splitlines() if line.strip())

    rev = run(["git", "rev-parse", "--short", "HEAD"], env, capture=True).strip()
    date = time.strftime("%Y-%m-%d")
    row = (
        f"| {date} | {rev} | {args.area} | {crate} "
        f"| {statistics.median(check_times):.1f} "
        f"| {statistics.median(build_times):.1f} | {tests} |\n"
    )

    METRICS.parent.mkdir(parents=True, exist_ok=True)
    if not METRICS.exists():
        METRICS.write_text(HEADER)
    with METRICS.open("a") as f:
        f.write(row)
    print(row.strip())


if __name__ == "__main__":
    main()
