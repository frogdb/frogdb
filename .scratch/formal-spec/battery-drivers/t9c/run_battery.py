#!/usr/bin/env python3
"""T9c feed-gate mutation battery driver.

Discipline:
  - pristine copies live in BACKUP (taken from the committed tree before any run)
  - each row is applied to a pristine copy, never on top of another mutation
  - after each row every file is restored byte-for-byte from BACKUP and
    `git diff --stat -- specs/quint/replication_feed_gate*` must be empty
  - only this model is ever run or touched
"""

import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path("/Users/nathan/workspace/frogdb")
SPEC = REPO / "specs/quint"
SCR = Path(
    "/private/tmp/claude-501/-Users-nathan-workspace-frogdb/"
    "d765cdf5-32ca-40ac-91cd-bd12d759ae64/scratchpad/t9c"
)
BACKUP = SCR / "backup"

FILES = {
    "T": "replication_feed_gate_types.qnt",
    "L": "replication_feed_gate_logic.qnt",
    "M": "replication_feed_gate_machine.qnt",
    "X": "replication_feed_gate.qnt",
}
MAIN = "specs/quint/replication_feed_gate.qnt"

sys.path.insert(0, str(SCR))
from rows import ROWS  # noqa: E402


def sh(cmd, timeout=900):
    full = 'eval "$(mise activate bash)" >/dev/null 2>&1; cd %s; %s' % (REPO, cmd)
    p = subprocess.run(["bash", "-c", full], capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


INVS = sorted(re.findall(r"^  val (inv_\w+)", (BACKUP / FILES["X"]).read_text(), re.M))
INV_ARG = " ".join(INVS)


PATHS = " ".join("specs/quint/" + f for f in FILES.values())


def head_matches():
    """Guard against a concurrent agent committing a mutated copy of our files."""
    for f in FILES.values():
        rc, out = sh("git --no-optional-locks show HEAD:specs/quint/%s" % f)
        if rc != 0 or out != (BACKUP / f).read_text():
            return f
    return None


def restore(attempt=0):
    for f in FILES.values():
        shutil.copyfile(BACKUP / f, SPEC / f)
    rc, out = sh("git --no-optional-locks diff --stat -- " + PATHS)
    if out.strip():
        # A concurrent agent's broad `git add` can stage a mid-battery mutation of our
        # files; unstage ours only, never touching anyone else's paths, and retry.
        if attempt < 3:
            sh("git restore --staged " + PATHS)
            time.sleep(2)
            return restore(attempt + 1)
        bad = head_matches()
        raise SystemExit("RESTORE FAILED, tree dirty (HEAD drift: %s):\n%s" % (bad, out))


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
    rc, out = sh("quint test %s" % MAIN)
    failed = re.findall(r"^\s+\d+\)\s+(\w+)", out, re.M)
    if "error:" in out and "Tests failed" not in out and rc != 0 and not failed:
        return "ERROR", out.strip().splitlines()[-3:]
    return ("FAIL" if rc != 0 else "PASS"), failed


def inv_run(invs, samples, steps, seed):
    rc, out = sh(
        "quint run %s --max-samples=%d --max-steps=%d --seed=%s --invariants %s"
        % (MAIN, samples, steps, seed, invs)
    )
    if "[violation]" in out:
        return "VIOLATION"
    if "[ok]" in out:
        return "OK"
    raise SystemExit("QUINT RUN ERROR (aborting battery):\n" + out[-2000:])


def attribute(samples, steps, seed):
    hits = []
    for inv in INVS:
        if inv_run(inv, samples, steps, seed) == "VIOLATION":
            hits.append(inv)
    return hits


def main():
    only = set(sys.argv[1:])
    out_path = SCR / "results.jsonl"
    done = {}
    if out_path.exists():
        for line in out_path.read_text().splitlines():
            if line.strip():
                d = json.loads(line)
                done[d["id"]] = d
    fh = out_path.open("a")
    for r in ROWS:
        if only and r["id"] not in only:
            continue
        if not only and r["id"] in done:
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
    print("BATTERY DONE")


if __name__ == "__main__":
    main()
