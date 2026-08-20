#!/usr/bin/env python3
"""Full-sync model mutation battery driver (T10b).

Per row: mutate one exact string, run `quint test` then sampled invariant runs,
restore byte-for-byte from the pristine copies, verify the scoped git diff is empty.
"""

import json
import os
import subprocess
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rows import ROWS  # noqa: E402

REPO = "/Users/nathan/workspace/frogdb"
SCRATCH = os.path.dirname(os.path.abspath(__file__))
PRISTINE = os.path.join(SCRATCH, "pristine")
QUINT = "/Users/nathan/.local/share/mise/installs/npm-informalsystems-quint/0.32.0/node_modules/.bin/quint"
MAIN = "specs/quint/replication_fullsync.qnt"

INVS = [
    "inv_no_acked_write_lost_across_fullsync",
    "inv_applied_covered_by_data",
    "inv_payload_covers_grant",
    "inv_splice_continuity",
    "inv_partial_grant_sound",
    "inv_replid_offset_paired",
    "inv_restart_pairs_offset",
    "inv_identity_pair_monotone",
    "inv_failover_window_whole",
    "inv_replids_distinct",
    "inv_second_offset_not_above_live",
    "inv_offsets_ordered",
    "inv_backlog_floor_sound",
    "inv_ack_never_above_live_head",
    "inv_one_session_per_announced_identity",
    "inv_link_points_at_a_live_session",
    "inv_superseded_records_no_departure",
    "inv_primary_role_is_terminal",
    "inv_primary_applied_is_head",
    "inv_primary_holds_no_link",
    "inv_replica_holds_no_window",
    "inv_only_primaries_arm_a_floor",
    "inv_registered_session_replica_is_replica",
    "inv_prestream_session_is_linked",
    "inv_prestream_link_has_nothing_pending",
    "inv_linked_replica_recv_within_primary_data",
    "inv_replica_not_ahead_of_matching_primary",
    "inv_installed_payload_was_cut",
]


def sh(args, timeout=600):
    return subprocess.run(args, cwd=REPO, capture_output=True, text=True, timeout=timeout)


def restore():
    """Restore byte-for-byte from the pristine copies and verify against them.

    The byte comparison is the authoritative check: `git diff` compares the worktree
    against the *index*, and a concurrent agent staging files makes that momentarily
    non-empty even when this model's files are pristine.
    """
    bad = []
    for name in os.listdir(PRISTINE):
        src = os.path.join(PRISTINE, name)
        dst = os.path.join(REPO, "specs/quint", name)
        shutil.copyfile(src, dst)
        if open(src, "rb").read() != open(dst, "rb").read():
            bad.append(name)
    return ",".join(bad)


def run_tests():
    r = sh([QUINT, "test", MAIN])
    out = r.stdout + r.stderr
    return r.returncode, out


def run_invs(seed, samples=500, steps=20, invs=None):
    invs = invs or INVS
    r = sh(
        [
            QUINT,
            "run",
            MAIN,
            "--invariants",
            *invs,
            "--max-samples",
            str(samples),
            "--max-steps",
            str(steps),
            "--seed",
            str(seed),
            "--out-itf",
            "/dev/null",
        ]
    )
    out = r.stdout + r.stderr
    return r.returncode, out


import re


def failing_tests(out):
    return re.findall(r"^\s+\d+\)\s+(\w+)\s+failed", out, re.M)


def attribute(seed, samples=500, steps=20):
    hits = []
    for inv in INVS:
        rc, _ = run_invs(seed, samples, steps, [inv])
        if rc != 0:
            hits.append(inv)
    return hits


def main():
    only = sys.argv[1:] if len(sys.argv) > 1 else None
    results = {}
    respath = os.path.join(SCRATCH, os.environ.get("BATTERY_RESULTS", "fullsync_results.json"))
    if os.path.exists(respath):
        results = json.load(open(respath))

    for row in ROWS:
        rid = row["id"]
        if only and rid not in only:
            continue
        if not only and rid in results:
            continue
        path = os.path.join(REPO, row["f"])
        text = open(path).read()
        assert text.count(row["old"]) == 1, f"{rid}: old not unique"
        open(path, "w").write(text.replace(row["old"], row["new"]))

        verdict, evid = None, ""
        try:
            rc, out = run_tests()
            if "error:" in out and "syntax" in out.lower():
                verdict, evid = "INVALID", out[-400:]
            elif rc != 0:
                ft = failing_tests(out)
                verdict = "CAUGHT-T"
                evid = ",".join(ft) if ft else out[-300:]
            else:
                for seed in ("0x1", "0x2"):
                    rc2, out2 = run_invs(seed)
                    if rc2 != 0:
                        verdict = "CAUGHT-P"
                        evid = "seed %s: %s" % (seed, ",".join(attribute(seed)) or "unattributed")
                        break
                if verdict is None:
                    verdict = "green@500x20"
                    evid = ""
        finally:
            diff = restore()
        assert diff == "", f"{rid}: dirty after restore: {diff}"
        results[rid] = {"verdict": verdict, "evidence": evid}
        json.dump(results, open(respath, "w"), indent=0)
        print(f"{rid}\t{verdict}\t{evid}", flush=True)


if __name__ == "__main__":
    main()
