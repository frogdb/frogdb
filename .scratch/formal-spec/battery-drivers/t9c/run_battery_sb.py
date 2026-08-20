#!/usr/bin/env python3
"""T9c battery driver, sandbox variant.

Identical mutation/oracle protocol to run_battery.py, but every file it touches
lives in scratchpad/t9c/sandbox_p1 (a pristine copy of the f7234770 model plus the
modules it imports). The working tree is never read or written here, so this can run
alongside the in-tree driver without racing it.
"""

import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

SCR = Path(
    "/private/tmp/claude-501/-Users-nathan-workspace-frogdb/"
    "d765cdf5-32ca-40ac-91cd-bd12d759ae64/scratchpad/t9c"
)
SPEC = SCR / "sandbox_p1"
BACKUP = SCR / "backup"
QUINT = (
    "/Users/nathan/.local/share/mise/installs/npm-informalsystems-quint/"
    "0.32.0/node_modules/.bin/quint"
)

FILES = {
    "T": "replication_feed_gate_types.qnt",
    "L": "replication_feed_gate_logic.qnt",
    "M": "replication_feed_gate_machine.qnt",
    "X": "replication_feed_gate.qnt",
}
MAIN = "replication_feed_gate.qnt"

sys.path.insert(0, str(SCR))
from rows import ROWS  # noqa: E402


def sh(cmd, timeout=1800):
    p = subprocess.run(
        ["bash", "-c", "cd %s; %s %s" % (SPEC, QUINT, cmd)],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return p.returncode, p.stdout + p.stderr


INVS = sorted(re.findall(r"^  val (inv_\w+)", (BACKUP / FILES["X"]).read_text(), re.M))
INV_ARG = " ".join(INVS)


def restore():
    for f in FILES.values():
        shutil.copyfile(BACKUP / f, SPEC / f)


def apply_row(r):
    edits = [(r["file"], r["old"], r["new"])] + [tuple(e) for e in r["extra"]]
    per_file = {}
    for key, old, new in edits:
        per_file.setdefault(key, []).append((old, new))
    for key, subs in per_file.items():
        text = (BACKUP / FILES[key]).read_text()
        for old, new in subs:
            n = text.count(old)
            if n != 1:
                raise SystemExit(
                    "row %s: pattern occurs %d times in %s:\n%r" % (r["id"], n, FILES[key], old)
                )
            text = text.replace(old, new)
        (SPEC / FILES[key]).write_text(text)


def quint_test():
    rc, out = sh("test %s" % MAIN)
    failed = re.findall(r"^\s+\d+\)\s+(\w+)", out, re.M)
    if "error:" in out and "Tests failed" not in out and rc != 0 and not failed:
        return "ERROR", out.strip().splitlines()[-3:]
    return ("FAIL" if rc != 0 else "PASS"), failed


def inv_run(invs, samples, steps, seed):
    rc, out = sh(
        "run %s --max-samples=%d --max-steps=%d --seed=%s --invariants %s"
        % (MAIN, samples, steps, seed, invs)
    )
    if "[violation]" in out:
        return "VIOLATION"
    if "[ok]" in out:
        return "OK"
    raise SystemExit("QUINT RUN ERROR (aborting battery):\n" + out[-2000:])


def attribute(samples, steps, seed):
    return [inv for inv in INVS if inv_run(inv, samples, steps, seed) == "VIOLATION"]


def main():
    only = set(sys.argv[1:])
    out_path = SCR / "results_sb.jsonl"
    fh = out_path.open("a")
    for r in ROWS:
        if r["id"] not in only:
            continue
        t0 = time.time()
        restore()
        apply_row(r)
        paired = r["expect"] == "PAIRED"
        rec = {"id": r["id"], "file": r["file"], "target": r["target"], "expect": r["expect"]}
        if paired:
            rec["test"] = "n/a"
            rec["failed_tests"] = []
        else:
            st, failed = quint_test()
            rec["test"] = st
            rec["failed_tests"] = failed
        seeds = ["0x1", "0x2", "0x3"]
        res = [inv_run(INV_ARG, 500, 20, s) for s in seeds]
        rec["run500"] = res
        rec["catchers"] = []
        if "VIOLATION" in res:
            i = res.index("VIOLATION")
            rec["catchers"] = attribute(500, 20, seeds[i])
            rec["verdict"] = "CAUGHT-P"
        else:
            deep = []
            if rec["test"] != "FAIL":
                for s in ["0x1", "0x2"]:
                    v = inv_run(INV_ARG, 4000, 40, s)
                    deep.append(v)
                    if v == "VIOLATION":
                        rec["catchers"] = attribute(4000, 40, s)
                        break
            rec["deep"] = deep
            rec["verdict"] = "CAUGHT-P" if "VIOLATION" in deep else "MISSED"
        if rec["test"] == "FAIL":
            rec["verdict"] = "CAUGHT-T" if not rec["catchers"] else "CAUGHT-T+P"
        if rec["test"] == "ERROR":
            rec["verdict"] = "ERROR"
        if paired:
            rec["verdict"] = "PAIRED-GREEN" if not rec["catchers"] else "PAIRED-STILL-CAUGHT"
        rec["secs"] = round(time.time() - t0, 1)
        restore()
        fh.write(json.dumps(rec) + "\n")
        fh.flush()
        print(
            "%-5s %-22s %-12s expect=%-9s %s"
            % (
                rec["id"],
                rec["verdict"],
                ",".join(rec["failed_tests"])[:12],
                rec["expect"],
                ",".join(c.replace("inv_", "") for c in rec["catchers"])[:70],
            ),
            flush=True,
        )
    fh.close()
    restore()
    print("SANDBOX BATTERY DONE")


if __name__ == "__main__":
    main()
