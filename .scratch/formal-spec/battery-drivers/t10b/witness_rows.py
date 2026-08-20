#!/usr/bin/env python3
"""Witness-count evidence for the step-unwiring rows (M112/M113/M114).

Unwiring an action from `step` cannot violate a safety invariant — removing
transitions only shrinks the reachable set. The observable is the witness count
collapsing to 0. This measures it, mutation by mutation, against the baseline.
"""

import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run_battery as R  # noqa: E402
from rows import ROWS  # noqa: E402

WITS = [
    "witnessFullSyncStreaming",
    "witnessWriteDuringTransfer",
    "witnessReappliedOverlap",
    "witnessTailApplied",
    "witnessPartialViaSecondary",
    "witnessEvictionForcedFullResync",
    "witnessSpliceAbandoned",
    "witnessAckIgnored",
    "witnessSuperseded",
    "witnessCorpseSession",
    "witnessSettleDiscardedFrames",
    "witnessUncleanRestartReminted",
    "witnessBacklogEvicted",
    "witnessAckRecorded",
    "witnessPromotedWithWindow",
    "witnessPartialGrant",
]
D = {r["id"]: r for r in ROWS}


def wits(seed="0x1", samples=1000, steps=25):
    r = subprocess.run(
        [
            R.QUINT,
            "run",
            R.MAIN,
            "--witnesses",
            *WITS,
            "--max-samples",
            str(samples),
            "--max-steps",
            str(steps),
            "--seed",
            seed,
            "--out-itf",
            "/dev/null",
        ],
        cwd=R.REPO,
        capture_output=True,
        text=True,
        timeout=900,
    )
    out = r.stdout + r.stderr
    got = {}
    for m in re.finditer(r"(witness\w+) was witnessed in (\d+) trace", out):
        got[m.group(1)] = int(m.group(2))
    return got


def main():
    base = wits()
    print("BASELINE", base, flush=True)
    for rid in sys.argv[1:]:
        row = D[rid]
        p = os.path.join(R.REPO, row["f"])
        t = open(p).read()
        assert t.count(row["old"]) == 1, rid
        open(p, "w").write(t.replace(row["old"], row["new"]))
        try:
            got = wits()
        finally:
            bad = R.restore()
        assert bad == "", f"{rid} dirty: {bad}"
        zeroed = [w for w in WITS if base.get(w, 0) > 0 and got.get(w, 0) == 0]
        dropped = {
            w: (base.get(w, 0), got.get(w, 0)) for w in WITS if got.get(w, 0) != base.get(w, 0)
        }
        print(f"{rid}\tZEROED={zeroed}\tDELTA={dropped}", flush=True)


if __name__ == "__main__":
    main()
