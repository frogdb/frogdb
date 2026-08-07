#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: a raft write acked as durable used sync write options.

Hardening-2 W1 rule C2 (`.scratch/hardening-2/PRD.md` §3.1). This is *not* a
general `rg` rule — durability here is acked by invoking a callback
(`LogFlushed::log_io_completed`) or by a method returning `Ok(())`, not by a
value a grep can see. It is a hand-crafted single-file pin on the openraft
storage impl `frogdb-server/crates/cluster/src/storage.rs`.

The invariant: every openraft storage method whose successful return (or
callback) tells the consensus layer "this is on the platter" must issue its
RocksDB write with sync options — `write_opt(batch, &opts)` where `opts` has
`set_sync(true)`, the correct form at `storage.rs:139-143` (the snapshot
`save`). A plain `db.write(batch)` or `db.flush()` returns before the WAL is
fsynced, so a power cut erases a write consensus already counted as durable.

Scoped to the three durable-ack methods:

    save        snapshot store — return means the snapshot is durable
    save_vote   raft vote-durability precondition — return means the vote is durable
    append      log entries — `callback.log_io_completed(Ok(()))` means durable

`truncate`, `purge`, `save_committed` and the `set_meta`/`delete_meta` helpers
are deliberately *out* of scope: none of them acks durability to openraft
(`save_committed` is documented write-only, re-derived from the leader on
restart; truncate/purge durability is re-established by the next append). Adding
one to the durable-ack set is a code change that this pin is meant to force a
decision on, not a silent widening.

The two known-bad sites are consensus-safety defects whose *fix* is defect wave
2, not this lint's job; they ride in the count-pinned allowlist with an issue
reference, exactly as `clock-seam.py` does. A method that is allowlisted but has
since been made sync is a stale entry and fails the gate — so the fix, when it
lands, forces its own allowlist entry out.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
STORAGE = ROOT / "frogdb-server" / "crates" / "cluster" / "src" / "storage.rs"

# The openraft storage methods whose success is a durability ack. Each must use
# sync write options; the ones that do not are listed in ALLOWLIST below.
DURABLE_ACK_METHODS = ("save", "save_vote", "append")

# method -> (why it is not sync yet, tracking issue). Count-pinned: an entry
# whose method has since become sync is stale and fails.
ALLOWLIST: dict[str, str] = {
    "append": (
        "pre-existing consensus-safety defect (round-2 issue 73): the raft log "
        "append acks via `callback.log_io_completed(Ok(()))` after a non-sync "
        "`db.write(batch)`, so a power cut can lose a committed log tail. Fix is "
        "defect wave 2, tracked by "
        ".scratch/testing-improvements-round2/issues/open/"
        "73-raft-append-acks-durability-without-fsync.md (and hardening-2 issue 03, "
        ".scratch/hardening-2/issues/open/03-six-hand-rolled-durable-writers.md)"
    ),
    "save_vote": (
        "pre-existing consensus-safety defect (hardening-2 issue 01): `save_vote` "
        "writes KEY_VOTE to the `raft_meta` CF via a non-sync `put_cf`, then calls "
        "`db.flush()` which flushes the *default* CF — so the vote is not durable, "
        "contradicting the doc at storage.rs:98-102. Fix is defect wave 2, tracked "
        "by .scratch/hardening-2/issues/open/"
        "01-save-vote-flushes-the-wrong-column-family.md"
    ),
}

# The one correct durable form: a `write_opt(...)` paired with `set_sync(true)`
# in the same method body. A method missing either half is non-sync — whether it
# used `db.write(batch)` / `db.flush()` (issue 73 / issue 01) or a `write_opt`
# with `set_sync(false)`.
SYNC_WRITE = re.compile(r"\bwrite_opt\s*\(")
SET_SYNC = re.compile(r"\bset_sync\s*\(\s*true\s*\)")


def method_body(
    lines: list[str], name: str, spans: list[tuple[int, int]]
) -> tuple[int, str] | None:
    """The (1-based defn line, joined body text) of a non-test `fn <name>`.

    Matches `fn <name>(` or `fn <name><` (generic), skipping `#[cfg(test)]`
    spans so a same-named test helper cannot be mistaken for the real method.
    Returns None if the method is absent.
    """
    fn_re = re.compile(rf"\bfn\s+{re.escape(name)}\s*[(<]")
    for i, line in enumerate(lines):
        if in_any_span(i, spans) or not fn_re.search(line):
            continue
        # Advance to the opening brace of the body, then brace-match to its close.
        j, depth, opened = i, 0, False
        while j < len(lines):
            depth += lines[j].count("{") - lines[j].count("}")
            opened = opened or "{" in lines[j]
            if opened and depth <= 0:
                break
            j += 1
        return i + 1, "\n".join(lines[i : j + 1])
    return None


def main() -> int:
    if not STORAGE.is_file():
        print(f"ERROR: {STORAGE} not found — did the raft storage impl move?", file=sys.stderr)
        return 1

    text = STORAGE.read_text()
    lines = text.splitlines()
    spans = cfg_test_spans(lines)
    rel = str(STORAGE.relative_to(ROOT))

    violations: list[tuple[str, int]] = []  # (method, defn line)
    seen: set[str] = set()
    for name in DURABLE_ACK_METHODS:
        found = method_body(lines, name, spans)
        if found is None:
            print(
                f"ERROR: durable-ack method `{name}` not found in {rel} — "
                "the pin is stale; update DURABLE_ACK_METHODS.",
                file=sys.stderr,
            )
            return 1
        defn_line, body = found
        seen.add(name)
        is_sync = bool(SYNC_WRITE.search(body)) and bool(SET_SYNC.search(body))
        if not is_sync:
            violations.append((name, defn_line))

    status = 0

    # Forward: a non-sync durable-ack method that is not allowlisted fails.
    unexpected = [(m, ln) for m, ln in violations if m not in ALLOWLIST]
    if unexpected:
        print("ERROR: raft write acked as durable without sync write options:", file=sys.stderr)
        for m, ln in unexpected:
            print(
                f"  {rel}:{ln}: `{m}` has no `write_opt(..)` with `set_sync(true)`", file=sys.stderr
            )
        print(file=sys.stderr)
        print(
            "       Acking durability before the WAL is fsynced loses the write on a",
            file=sys.stderr,
        )
        print(
            "       power cut. Use sync write options, as the snapshot `save` does:",
            file=sys.stderr,
        )
        print("           let mut opts = rocksdb::WriteOptions::default();", file=sys.stderr)
        print("           opts.set_sync(true);", file=sys.stderr)
        print("           self.db.write_opt(batch, &opts)?;", file=sys.stderr)
        print(
            "       A method whose non-sync write is a known, tracked defect goes", file=sys.stderr
        )
        print("       in ALLOWLIST in this script, with its issue reference.", file=sys.stderr)
        status = 1

    # Reverse: an allowlisted method that is now sync (fixed) is a stale entry.
    violating = {m for m, _ in violations}
    stale = [m for m in ALLOWLIST if m in seen and m not in violating]
    if stale:
        print("ERROR: the durable-ack allowlist is out of date:", file=sys.stderr)
        for m in sorted(stale):
            print(
                f"  `{m}`: allowlisted as non-sync, but now uses sync write options — "
                "drop the entry, the defect is fixed.",
                file=sys.stderr,
            )
        status = 1

    if status == 0:
        print(
            "OK: raft durable-ack writes are sync "
            f"({len(ALLOWLIST)} tracked pre-existing defect(s) allowlisted)"
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
