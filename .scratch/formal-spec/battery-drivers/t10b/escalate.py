#!/usr/bin/env python3
"""Escalation pass: every row green at 500x20 is re-run at 4000x40, seeds 0x1/0x2/0x3."""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rows import ROWS  # noqa: E402
import run_battery as rb  # noqa: E402

SCRATCH = os.path.dirname(os.path.abspath(__file__))
res = json.load(
    open(os.path.join(SCRATCH, os.environ.get("BATTERY_RESULTS", "fullsync_results.json")))
)
outp = os.path.join(SCRATCH, os.environ.get("BATTERY_ESC", "escalation.json"))
esc = json.load(open(outp)) if os.path.exists(outp) else {}

only = sys.argv[1:] or None
byid = {r["id"]: r for r in ROWS}
targets = [rid for rid in res if res[rid]["verdict"] == "green@500x20"]
if only:
    targets = [t for t in targets if t in only]

for rid in targets:
    if rid in esc and not only:
        continue
    row = byid[rid]
    path = os.path.join(rb.REPO, row["f"])
    text = open(path).read()
    assert text.count(row["old"]) == 1, rid
    open(path, "w").write(text.replace(row["old"], row["new"]))
    verdict, evid = "MISSED", "green at 4000x40, seeds 0x1/0x2/0x3"
    try:
        for seed in ("0x1", "0x2", "0x3"):
            rc, _ = rb.run_invs(seed, 4000, 40)
            if rc != 0:
                hits = [i for i in rb.INVS if rb.run_invs(seed, 4000, 40, [i])[0] != 0]
                verdict = "CAUGHT-P"
                evid = "4000x40 seed %s: %s" % (seed, ",".join(hits) or "unattributed")
                break
    finally:
        d = rb.restore()
    assert d == "", f"{rid}: dirty: {d}"
    esc[rid] = {"verdict": verdict, "evidence": evid}
    json.dump(esc, open(outp, "w"), indent=0)
    print(f"{rid}\t{verdict}\t{evid}", flush=True)
