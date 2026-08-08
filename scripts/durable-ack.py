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
RocksDB write with sync options. A plain `db.write(batch)` or `db.flush()`
returns before the WAL is fsynced, so a power cut erases a write consensus
already counted as durable.

Two shapes satisfy it, because the impl has two kinds of durable-ack method:

*Inline* — `write_opt(batch, &opts)` where `opts` has `set_sync(true)`, the
form the snapshot `save` uses.

*Through the metadata chokepoint* — `save_vote` writes one key rather than a
batch, so it delegates to `set_meta`, which renders a per-key
`MetaDurability` class into the write options (FM-CLUSTER-098). That counts
as sync only while all three links hold: `MetaDurability::for_key` classifies
the key as `Synced`, `write_opts` turns the class into `set_sync`, and
`set_meta` hands those options to an options-carrying write. Break any link
and the method reads as non-sync again, which is the point — the chokepoint
is only a durability guarantee while it is wired end to end.

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

A known-bad site is a consensus-safety defect whose *fix* is defect wave 2, not
this lint's job; it rides in the count-pinned allowlist with an issue reference,
exactly as `clock-seam.py` does. A method that is allowlisted but has since been
made sync is a stale entry and fails the gate — so the fix, when it lands,
forces its own allowlist entry out, as `save_vote`'s did.
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
}

# The inline durable form: a `write_opt(...)` paired with `set_sync(true)` in the
# same method body. A method missing either half is non-sync — whether it used
# `db.write(batch)` / `db.flush()` (issue 73) or a `write_opt` with
# `set_sync(false)` — unless it delegates to the metadata chokepoint below.
SYNC_WRITE = re.compile(r"\bwrite_opt\s*\(")
SET_SYNC = re.compile(r"\bset_sync\s*\(\s*true\s*\)")

# The delegated durable form: `self.set_meta(KEY_X, ..)` as the method's whole
# write, durable only if the chokepoint classifies `KEY_X` as `Synced`.
DELEGATED_WRITE = re.compile(r"\bset_meta\s*\(\s*(KEY_[A-Z_]+)")
# `if key == KEY_VOTE { Self::Synced` in `MetaDurability::for_key`, whitespace
# normalized so rustfmt's line breaks do not decide whether the gate passes.
CLASSIFIED_SYNCED = "== {key} {{ Self::Synced"
# `write_opts` must derive the flag from the class, and `set_meta` must hand the
# rendered options to an options-carrying write rather than a defaulted one.
RENDERS_CLASS = re.compile(r"\bset_sync\s*\([^)]*Self::Synced")
PASSES_OPTS = re.compile(
    r"\b(?:put|delete)_cf_opt\s*\([^;]*MetaDurability::for_key\([^;]*write_opts"
)


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


def chokepoint_syncs(lines: list[str], spans: list[tuple[int, int]], key: str) -> bool:
    """Whether `set_meta(<key>, ..)` is a synced write.

    All three links of the chokepoint have to hold: the key is classified
    `Synced`, the class is rendered into `set_sync`, and the rendered options
    reach the write. Any missing link and the caller counts as non-sync.
    """
    for_key = method_body(lines, "for_key", spans)
    write_opts = method_body(lines, "write_opts", spans)
    set_meta = method_body(lines, "set_meta", spans)
    if for_key is None or write_opts is None or set_meta is None:
        return False
    classification = " ".join(for_key[1].split())
    return (
        CLASSIFIED_SYNCED.format(key=key) in classification
        and bool(RENDERS_CLASS.search(write_opts[1]))
        and bool(PASSES_OPTS.search(" ".join(set_meta[1].split())))
    )


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
        inline = bool(SYNC_WRITE.search(body)) and bool(SET_SYNC.search(body))
        delegated = any(
            chokepoint_syncs(lines, spans, key) for key in DELEGATED_WRITE.findall(body)
        )
        if not (inline or delegated):
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
            "       A single-key write may instead go through `set_meta` with the key",
            file=sys.stderr,
        )
        print(
            "       classified `MetaDurability::Synced` — but only while `for_key`,",
            file=sys.stderr,
        )
        print(
            "       `write_opts` and `set_meta` are still wired to each other.",
            file=sys.stderr,
        )
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
