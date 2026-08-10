#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: server-crate code reads the clock through the clock seam, not the OS.

Determinism audit item R5 (`.scratch/concurrency-testing/audit/`). Every
deadline the server holds is compared against a *now*, and which clock that
`now` came from decides whether a key expired, whether a waiter timed out,
whether a node is FAILed, and what a `TTL`/`TIME`/`XINFO` reply says. Under a
paused tokio runtime — how every simulated host in the turmoil suite runs —
the timer's clock and the OS clock disagree, so a site that reads the OS
clock directly makes its decision on a different timeline from the rest of
the server and the run stops being reproducible.

The seam is `frogdb_types::clock` (re-exported as `frogdb_core::clock`):

    clock::now()         monotonic, the expiry/deadline domain
    clock::system_now()  wall clock, the stream-ID / EXPIRETIME / TIME domain

Both compile to the same reading as the OS clock when no paused runtime is
present, so converting a site is free in production.

Compliant reads, therefore, are `clock::now()`, `clock::system_now()`, and
`tokio::time::Instant::now()` (the same virtual clock, for crates with no
`frogdb-types` edge). Banned, outside the allowlist below:

    std::time::Instant::now()       explicit, or bare `Instant::now()` in a
                                    file whose `Instant` is std's
    std::time::SystemTime::now()    explicit or bare

Scope is non-test code under `frogdb-server/crates/*/src`. Test code may read
the OS clock freely: a test that wants real elapsed time is asking a question
about the machine, not about the server's timeline. `frogctl/` and
`frogdb-operator/` are out of the simulation — they are
separate binaries that never run under a paused runtime.

Exemptions are per file, carry a reason, and are count-pinned: adding a new
OS-clock read to an already-exempt file fails the gate, so an exemption
cannot quietly widen into a blanket suppression.

Usage:
    clock-seam.py           # fail on any unexempted read
    clock-seam.py --list    # print every read the gate can see, exempt or not
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, is_test_path  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CRATES = ROOT / "frogdb-server" / "crates"

# Crates that exist to test the server rather than to be it. Their whole
# source tree is test code by construction.
TEST_SUPPORT_CRATES = {
    "browser-tests",
    "redis-regression",
    "shard-harness",
    "test-harness",
    "testing",
}

# file (relative to the repo root) -> (expected number of exempt OS-clock
# reads, why they stay on the OS clock). Every entry is checked in both
# directions: a stale entry (file gone, or no longer reading the OS clock) is
# an error, and so is a count that no longer matches.
ALLOWLIST: dict[str, tuple[int, str]] = {
    "frogdb-server/crates/types/src/clock.rs": (
        1,
        "the seam itself: `system_now()` latches one real `SystemTime` as the "
        "epoch it then advances off the monotonic (virtualizable) clock",
    ),
    "frogdb-server/crates/server/src/latency_test.rs": (
        3,
        "`LATENCY TEST` measures the host's intrinsic scheduling jitter: it "
        "busy-loops until real time elapses (two `Instant::now()` reads plus "
        "the loop's own `.elapsed()` bound). On a paused clock none of the "
        "three advances and the command would hang forever",
    ),
    "frogdb-server/crates/config/src/cluster.rs": (
        1,
        "derives a node id when none is configured, so it needs a *unique* "
        "number, not a consistent one. The seam's wall clock is process-global "
        "state shared by every simulated host, which would hand them all the "
        "same id",
    ),
    "frogdb-server/crates/debug/src/bundle/generator.rs": (
        1,
        "debug-bundle id: same uniqueness argument as the node id above",
    ),
    "frogdb-server/crates/debug/src/bundle/store.rs": (
        1,
        "compares bundle age against filesystem mtimes, which are OS wall-clock "
        "values — the two have to be read off the same clock",
    ),
    "frogdb-server/crates/debug/src/bundle/collector.rs": (
        2,
        "stamps a support bundle for a human reading it later; the bundle is a "
        "forensic artifact about the machine, not a server reply",
    ),
    "frogdb-server/crates/persistence/src/rocks/checkpoint.rs": (
        1,
        "names the pre-restore backup directory. Operators correlate that name "
        "with real time, and two runs must not collide on it",
    ),
    "frogdb-server/crates/persistence/src/snapshot/rocks_coordinator.rs": (
        1,
        "fallback for a snapshot artifact's wall-clock completion time when its "
        "metadata.json carries none. The value is recorded back to disk and "
        "exported as the `SnapshotLastTimestamp` gauge, a forensic wall-clock "
        "reading Prometheus correlates against its own scrape clock — the same "
        "observability argument as the telemetry timestamps below. Newly visible "
        "once the lint learned the paren-less `unwrap_or_else(SystemTime::now)` "
        "form (hardening-2 C7)",
    ),
    "frogdb-server/crates/replication/src/split_brain_log.rs": (
        1,
        "names and stamps the split-brain forensic log — same argument as the backup directory",
    ),
    "frogdb-server/crates/telemetry/src/prometheus_recorder.rs": (
        1,
        "exposition timestamp consumed by Prometheus, which correlates it with "
        "its own scrape wall clock",
    ),
    "frogdb-server/crates/telemetry/src/status.rs": (
        1,
        "status-endpoint timestamp, rendered as ISO-8601 for external monitoring",
    ),
    "frogdb-server/crates/telemetry/src/tracing.rs": (
        1,
        "span timestamp shipped to an external tracing backend that joins it "
        "against other services' wall clocks",
    ),
    "frogdb-server/crates/tokio-coz/src/hooks.rs": (
        2,
        "causal-profiler shim around the tokio runtime, not server logic; it "
        "has no frogdb dependency and is never compiled into a simulated host. "
        "Its epoch `Instant::now()` and the `.elapsed()` that stamps each "
        "sample are deliberately real: the profiler is measuring the machine",
    ),
}

# Match both the called form `...::now()` and the paren-less value form
# `...::now` (passed to a combinator, e.g. `get_or_init(Instant::now)` /
# `unwrap_or_else(SystemTime::now)`) — both read the OS clock, the second just
# defers it. A trailing `\b` (not `\s*\(\)`) is what catches the paren-less
# case; `now\b` still rejects `now_us`, `nowhere`, etc.
STD_INSTANT_QUALIFIED = re.compile(r"\bstd::time::Instant::now\b")
STD_SYSTEM_QUALIFIED = re.compile(r"\bstd::time::SystemTime::now\b")
BARE_INSTANT = re.compile(r"(?<![:\w])Instant::now\b")
BARE_SYSTEM = re.compile(r"(?<![:\w])SystemTime::now\b")

# `x.elapsed()` is `std::time::Instant::now() - x`: it reads the OS clock
# regardless of which clock produced `x`. Seaming the *anchor* and then
# measuring its age with `.elapsed()` therefore silently un-seams the site —
# the failure this rule exists to catch, because the anchor read looks
# compliant to every other predicate here (issue 23,
# `.scratch/cluster-correctness/issues/done/`). The seam's own
# `clock::elapsed(x)` is the compliant form. `tokio::time::Instant::elapsed`
# *is* virtual, so files whose `Instant` is tokio's are exempt by the same
# import test the bare-`Instant::now` rule uses.
ELAPSED = re.compile(r"\.elapsed\(\)")

IMPORTS_STD_INSTANT = re.compile(r"^\s*use std::time::(?:\{[^}]*\bInstant\b|Instant\b)", re.M)
IMPORTS_TOKIO_INSTANT = re.compile(r"^\s*use tokio::time::(?:\{[^}]*\bInstant\b|Instant\b)", re.M)


@dataclass
class Finding:
    path: str
    line: int
    kind: str
    source: str


def scan(path: Path) -> list[Finding]:
    rel = path.relative_to(ROOT)
    text = path.read_text()
    # Fast path: `::now` (not `::now()`) so a file that only reads the clock via
    # the paren-less value form is not skipped before the real predicates run.
    squashed = text.replace(" ", "")
    if "::now" not in squashed and ".elapsed()" not in squashed:
        return []
    lines = text.splitlines()
    spans = cfg_test_spans(lines)
    # A bare `Instant::now()` is only an OS-clock read if this file's `Instant`
    # is std's. A file that imports tokio's is already on the timer's clock.
    tokio_instant = bool(IMPORTS_TOKIO_INSTANT.search(text))
    bare_instant_is_std = bool(IMPORTS_STD_INSTANT.search(text)) and not tokio_instant
    findings = []
    for idx, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("*"):
            continue
        if any(lo <= idx <= hi for lo, hi in spans):
            continue
        if STD_INSTANT_QUALIFIED.search(line):
            kind = "std::time::Instant::now()"
        elif STD_SYSTEM_QUALIFIED.search(line) or BARE_SYSTEM.search(line):
            kind = "SystemTime::now()"
        elif BARE_INSTANT.search(line) and bare_instant_is_std:
            kind = "Instant::now() (std)"
        elif ELAPSED.search(line) and not tokio_instant:
            kind = ".elapsed() (std Instant)"
        else:
            continue
        findings.append(Finding(str(rel), idx + 1, kind, stripped))
    return findings


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="print every read, exempt or not")
    args = ap.parse_args()

    by_file: dict[str, list[Finding]] = {}
    for path in sorted(CRATES.rglob("*.rs")):
        rel = path.relative_to(ROOT)
        parts = rel.parts
        if len(parts) < 4 or parts[3] != "src":
            continue  # only crate sources; tests/ and benches/ are test code
        if parts[2] in TEST_SUPPORT_CRATES:
            continue
        if is_test_path(rel):
            continue
        found = scan(path)
        if found:
            by_file[str(rel)] = found

    if args.list:
        for f, items in sorted(by_file.items()):
            mark = "exempt" if f in ALLOWLIST else "VIOLATION"
            for it in items:
                print(f"{mark:<9} {it.path}:{it.line}  {it.kind}  {it.source}")
        return 0

    status = 0
    violations = {f: v for f, v in by_file.items() if f not in ALLOWLIST}
    if violations:
        print("ERROR: OS-clock read outside the clock seam:", file=sys.stderr)
        for f in sorted(violations):
            for it in violations[f]:
                print(f"  {it.path}:{it.line}: {it.kind}", file=sys.stderr)
                print(f"      {it.source}", file=sys.stderr)
        print(file=sys.stderr)
        print("       Read through the seam instead:", file=sys.stderr)
        print("         frogdb_types::clock::now()          (monotonic)", file=sys.stderr)
        print("         frogdb_types::clock::system_now()   (wall clock)", file=sys.stderr)
        print("         frogdb_types::clock::elapsed(x)     (age of `x`)", file=sys.stderr)
        print("       (`frogdb_core::clock` re-exports both; inside", file=sys.stderr)
        print(
            "       frogdb-core/frogdb-types the style is `crate::clock::now()`.)", file=sys.stderr
        )
        print("       A crate with no frogdb-types edge may use", file=sys.stderr)
        print("       `tokio::time::Instant::now()`, which is the same clock.", file=sys.stderr)
        print("       A site that genuinely needs the OS clock goes in", file=sys.stderr)
        print("       ALLOWLIST in this script, with a reason.", file=sys.stderr)
        status = 1

    stale = []
    for f, (expected, _reason) in sorted(ALLOWLIST.items()):
        actual = len(by_file.get(f, []))
        if actual == 0:
            stale.append(f"  {f}: allowlisted, but reads no OS clock any more — drop the entry")
        elif actual != expected:
            stale.append(
                f"  {f}: allowlisted for {expected} OS-clock read(s), found {actual}. "
                "Re-justify and update the count, or move the new site onto the seam."
            )
    if stale:
        print("ERROR: the clock-seam allowlist is out of date:", file=sys.stderr)
        print("\n".join(stale), file=sys.stderr)
        status = 1

    if status == 0:
        exempt = sum(n for n, _ in ALLOWLIST.values())
        print(
            f"OK: server crates read the clock through the seam "
            f"({exempt} documented OS-clock reads in {len(ALLOWLIST)} files)"
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
