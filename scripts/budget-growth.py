#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: a structure that cannot charge cannot grow.

[adr/0006](../adr/0006-memory-architecture-seams.md) §2: every non-keyspace
buffer charges a `Budget` **before** the bytes exist, and a refused charge is a
refusal the seam handles — it sheds or backpressures, and declares which. The
chokepoint is `frogdb_memory::Budget`/`Charge` (`frogdb-server/crates/memory`),
and the vocabulary is `specs/memory.md` "Invariant vocabulary".

The keyspace is out of scope by construction: its bytes are attributed by the
per-shard *arena* (ADR-0006 §3), not by a budget, so `KEYSPACE_MODULES` below
is a scope boundary, not a suppression.

## The predicate

A **growth site** is a line in non-test server-crate source that grows a field
the structure owns:

    self.<field>.<op>(...)      op ∈ GROWTH_OPS (push, insert, extend, ...)

attributed to the struct named by the enclosing `impl` block. Only fields whose
declared type is an *owned growable container* (`GROWABLE`: `Vec`, `VecDeque`,
`HashMap`, `HashSet`, `BTreeMap`, `BTreeSet`, `String`, `BytesMut`) count: a
growth call on a `usize` or an `Arc<Mutex<..>>` handle is not a buffer this
structure's memory belongs to.

A growth site is **compliant** when its struct also owns a charge — a field
whose type mentions `Charge` or `Budget`. That is the ADR's sentence made
mechanical: the structure can charge, so it may grow. It deliberately does not
try to prove the charge covers *this* call; a per-line dataflow proof is not
something a grep-shaped gate can do, and the structural pin is what stops a new
unaccounted buffer from being added next to an accounted one.

## The ratchet

Every buffer that has not been converted yet is pinned in `ALLOWLIST` by file,
with a count and a reason, checked in **both** directions: converting a buffer
must remove its entry, and adding a new unbudgeted growth site to an already
listed file fails exactly like a new file would. Entries burn down in batches as
the memory-architecture phases convert their subsystems
(`.scratch/memory-architecture/PRD.md` R8).

Usage:
    budget-growth.py            # fail on any unpinned unbudgeted growth site
    budget-growth.py --list     # every growth site the gate can see
    budget-growth.py --pins     # ALLOWLIST-shaped counts for the current tree
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span, is_test_path  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CRATES = ROOT / "frogdb-server" / "crates"

# Crates that exist to test the server rather than to be it.
TEST_SUPPORT_CRATES = {
    "browser-tests",
    "redis-regression",
    "shard-harness",
    "test-harness",
    "testing",
}

# Keyspace memory is arena-accounted, not budget-accounted (ADR-0006 §3). These
# are scope boundaries, not exemptions: a budget on the keyspace would be
# double-counting the arena figure the broker already reads.
KEYSPACE_MODULES = (
    # The store itself and the eviction machinery that sizes it.
    "frogdb-server/crates/core/src/store/",
    "frogdb-server/crates/core/src/eviction/",
    # The segmented keyspace table: the directory and the segment array are the
    # keyspace's own storage, same scope as the store above. Its structural cost
    # is reported at the allocator size class through `Table::structural_bytes`
    # for the arena to read, which is the ADR-0006 §3 path, not a budget.
    "frogdb-server/crates/table/src/",
    # The value representations a key holds: lists, sorted sets, streams, and
    # the probabilistic/time-series types. Their bytes are keyspace bytes.
    "frogdb-server/crates/types/src/types/",
    "frogdb-server/crates/types/src/timeseries/",
    "frogdb-server/crates/types/src/json/",
    "frogdb-server/crates/types/src/bloom.rs",
    "frogdb-server/crates/types/src/cuckoo.rs",
    # The block encodings those value representations are built from — a
    # listpack buffer and a blockstore block hold element bytes, so they are
    # keyspace memory too.
    "frogdb-server/crates/types/src/listpack.rs",
    "frogdb-server/crates/types/src/blockstore.rs",
    "frogdb-server/crates/types/src/skiplist.rs",
    "frogdb-server/crates/types/src/tdigest.rs",
    "frogdb-server/crates/types/src/topk.rs",
    "frogdb-server/crates/types/src/vectorset.rs",
)

# Owned containers whose growth is this structure's memory. A handle
# (`Arc<..>`, `Sender<..>`, `&mut ..`) is somebody else's bytes.
GROWABLE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:Vec|VecDeque|BinaryHeap|HashMap|HashSet"
    r"|BTreeMap|BTreeSet|IndexMap|IndexSet|String|BytesMut|SmallVec)\s*[<(]?"
)

# Calls that add caller-supplied content to a container.
GROWTH_OPS = (
    "push",
    "push_back",
    "push_front",
    "push_str",
    "insert",
    "entry",
    "extend",
    "extend_from_slice",
    "append",
    "reserve",
    "resize",
    "put_slice",
)
GROWTH = re.compile(r"\bself\.([a-z_][a-z0-9_]*)\s*\.\s*(" + "|".join(GROWTH_OPS) + r")\s*\(")

# A field that carries the structure's permission to grow.
CHARGE_FIELD = re.compile(r":\s*(?:[A-Za-z_:]*::)?(?:Charge|Budget)\b")

STRUCT_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?struct\s+([A-Za-z_][A-Za-z0-9_]*)")
IMPL_DECL = re.compile(
    r"^\s*impl(?:\s*<[^>]*>)?\s+(?:[A-Za-z_][A-Za-z0-9_:<>, ']*\s+for\s+)?"
    r"([A-Za-z_][A-Za-z0-9_]*)"
)
FIELD_DECL = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?([a-z_][a-z0-9_]*)\s*:\s*(.+?),?\s*$")

# file (relative to the repo root) -> (number of unbudgeted growth sites, why
# it has not been converted yet). Checked in both directions.
ALLOWLIST: dict[str, tuple[int, str]] = {
    # --- ACL: grows with configured users and rules, not with traffic. A
    # budget here would refuse a `ACL SETUSER`, which is an operator action.
    "frogdb-server/crates/acl/src/permissions.rs": (14, "per-user rule sets, sized by config"),
    "frogdb-server/crates/acl/src/user.rs": (1, "per-user password hashes, sized by config"),
    # --- Cluster / failure detection
    "frogdb-server/crates/cluster-runtime/src/failure_detector.rs": (
        2,
        "health row per known node; bounded by the cluster's node count",
    ),
    "frogdb-server/crates/cluster/src/test_tracing.rs": (
        2,
        "tracing field visitor for the crate's own diagnostics",
    ),
    # --- Config validation
    "frogdb-server/crates/config/src/validators/mod.rs": (
        3,
        "validation findings for one config load",
    ),
    # --- Per-connection and per-shard observability state. These are the next
    # conversion batch after the network-output class (PRD R8 phase 3): every
    # one of them grows with traffic.
    "frogdb-server/crates/core/src/client_registry/stats.rs": (
        3,
        "per-connection command counters and latency samples — traffic-driven",
    ),
    "frogdb-server/crates/core/src/hotkeys.rs": (1, "hotkey sampling session — traffic-driven"),
    "frogdb-server/crates/core/src/latency.rs": (
        3,
        "LATENCY event histories — traffic-driven, capped per event by its own constant",
    ),
    "frogdb-server/crates/core/src/slowlog.rs": (
        1,
        "SLOWLOG ring — traffic-driven, capped by `slowlog-max-len`",
    ),
    "frogdb-server/crates/core/src/observability/wal.rs": (1, "per-shard WAL lag rows"),
    "frogdb-server/crates/core/src/noop.rs": (
        4,
        "expiry indexes; keyspace-adjacent but not the store, so out of the arena's scope too",
    ),
    "frogdb-server/crates/core/src/pubsub.rs": (
        3,
        "subscription tables — traffic-driven, and the class the network-output budget covers",
    ),
    "frogdb-server/crates/core/src/registry.rs": (3, "command table, built once at startup"),
    "frogdb-server/crates/core/src/shard/search/lifecycle.rs": (
        1,
        "index registry, sized by declared indexes",
    ),
    "frogdb-server/crates/core/src/shard/wait_queue.rs": (
        12,
        "blocked-client wait queue — traffic-driven; a Budget here needs the backpressure "
        "disposition wired through the blocking commands, which is a later phase",
    ),
    "frogdb-server/crates/core/src/tracking.rs": (
        5,
        "`InvalidationRegistry` and `BroadcastTable`; the `TrackingTable` beside them is "
        "converted (issue 05) and these two follow with BCAST-mode accounting",
    ),
    "frogdb-server/crates/net/src/lib.rs": (
        2,
        "one runtime handle and one arena binding per shard, both at startup, both "
        "bounded by the shard count",
    ),
    # --- Replication
    "frogdb-server/crates/replication-runtime/src/install.rs": (
        2,
        "full-sync staging sink — the `FullsyncStaging` budget class, whose conversion is "
        "spec-first (frogdb-replication is locked)",
    ),
    "frogdb-server/crates/replication/src/feed_sequencer.rs": (
        1,
        "out-of-order feed hold queue — locked crate, spec-first conversion",
    ),
    "frogdb-server/crates/replication/src/properties.rs": (1, "property-name interner"),
    # --- Scripting
    "frogdb-server/crates/scripting/src/library.rs": (1, "FUNCTION library, sized by loaded code"),
    "frogdb-server/crates/scripting/src/registry.rs": (
        2,
        "FUNCTION registry, sized by loaded code",
    ),
    # --- Search
    "frogdb-server/crates/search/src/vector.rs": (2, "vector field id maps, sized by the index"),
    # --- Reply path. The frogdb-server accumulators on this path (search
    # merges, scatter/gather merges, INFO section text) were converted by
    # issue 18: each owns a `Charge` against the thread-local NetworkOutput
    # budget (`net_charge.rs`). What remains here is `frogdb-protocol`'s
    # reply builder, which cannot be converted in place: frogdb-protocol has
    # no frogdb-memory dependency (it is the wire-format leaf crate), and its
    # buffers are charged at feed time when they reach the connection's
    # output buffer (`connection/output_buffer.rs`).
    "frogdb-server/crates/protocol/src/reply.rs": (2, "reply builder for one command"),
    "frogdb-server/crates/server/src/connection/state.rs": (
        1,
        "per-connection latency samples — traffic-driven",
    ),
    "frogdb-server/crates/server/src/slot_migration/routing.rs": (
        3,
        "per-batch key routing scratch during a slot migration",
    ),
    # --- Telemetry
    "frogdb-server/crates/telemetry/src/node_state.rs": (1, "per-shard snapshot rows"),
    "frogdb-server/crates/telemetry/src/task_monitors.rs": (1, "one monitor per named task"),
    # --- Transactions
    "frogdb-server/crates/txn/src/state.rs": (
        2,
        "MULTI watches and queued errors — the queued commands themselves are charged "
        "to the `TxnBuffering` budget (FM-TXN-054); these two remain spec-first "
        "(frogdb-txn is locked)",
    ),
    # --- VLL
    "frogdb-server/crates/vll/src/coordinator.rs": (1, "revocation watch receivers"),
    "frogdb-server/crates/vll/src/lock_table.rs": (
        1,
        "lock table rows — locked crate, spec-first conversion",
    ),
    "frogdb-server/crates/vll/src/queue.rs": (
        1,
        "pending transaction queue — locked crate, spec-first conversion",
    ),
}


@dataclass
class Finding:
    path: str
    line: int
    struct: str
    field: str
    op: str
    source: str


def _block_end(lines: list[str], start: int) -> int:
    """Last line index of the braced item opening at or after `start`."""
    depth, opened = 0, False
    for i in range(start, len(lines)):
        depth += lines[i].count("{") - lines[i].count("}")
        opened = opened or "{" in lines[i]
        if opened and depth <= 0:
            return i
    return len(lines) - 1


def parse_structs(lines: list[str]) -> dict[str, dict[str, str]]:
    """struct name -> {field name: declared type}."""
    structs: dict[str, dict[str, str]] = {}
    for i, line in enumerate(lines):
        m = STRUCT_DECL.match(line)
        if not m or "{" not in line:
            continue  # tuple struct or unit struct: no named fields to grow
        end = _block_end(lines, i)
        fields: dict[str, str] = {}
        for body in lines[i + 1 : end]:
            stripped = body.strip()
            if not stripped or stripped.startswith(("//", "#[", "/*", "*")):
                continue
            fm = FIELD_DECL.match(body)
            if fm:
                fields[fm.group(1)] = fm.group(2)
        structs[m.group(1)] = fields
    return structs


def impl_owner(lines: list[str]) -> dict[int, str]:
    """line index -> the type name of the innermost enclosing `impl` block."""
    owner: dict[int, str] = {}
    for i, line in enumerate(lines):
        m = IMPL_DECL.match(line)
        if not m:
            continue
        end = _block_end(lines, i)
        for j in range(i, end + 1):
            owner[j] = m.group(1)
    return owner


def joined(lines: list[str], idx: int, lookahead: int = 3) -> str:
    """`lines[idx]` with any rustfmt-split method chain glued back on.

    `self.key_to_clients\\n    .entry(k)` is one growth site, and a
    line-at-a-time regex sees neither half of it.
    """
    out = lines[idx].strip()
    for nxt in lines[idx + 1 : idx + 1 + lookahead]:
        stripped = nxt.strip()
        if not stripped.startswith("."):
            break
        out += stripped
    return out


def scan(path: Path) -> tuple[list[Finding], list[Finding]]:
    """(unbudgeted growth sites, budgeted growth sites) in one file."""
    rel = path.relative_to(ROOT)
    text = path.read_text()
    if "self." not in text:
        return [], []
    lines = text.splitlines()
    spans = cfg_test_spans(lines)
    structs = parse_structs(lines)
    owner = impl_owner(lines)

    unbudgeted: list[Finding] = []
    budgeted: list[Finding] = []
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(("//", "/*", "*", "#")):
            continue
        if in_any_span(idx, spans):
            continue
        # Every growth call on the line, not just the first: two `push`es
        # separated by a semicolon are two sites, and the count pin has to see
        # both or the ratchet can be walked past.
        for m in GROWTH.finditer(joined(lines, idx)):
            field, op = m.group(1), m.group(2)
            struct = owner.get(idx)
            if struct is None or struct not in structs:
                continue  # a free function or a type declared elsewhere: unattributable
            fields = structs[struct]
            declared = fields.get(field)
            if declared is None or not GROWABLE.match(declared):
                continue  # not an owned growable container
            finding = Finding(str(rel), idx + 1, struct, field, op, stripped)
            charged = any(CHARGE_FIELD.search(f": {t}") for t in fields.values())
            (budgeted if charged else unbudgeted).append(finding)
    return unbudgeted, budgeted


def in_scope(rel: Path) -> bool:
    parts = rel.parts
    if len(parts) < 4 or parts[3] != "src":
        return False  # crate sources only; tests/ and benches/ are test code
    if parts[2] in TEST_SUPPORT_CRATES:
        return False
    if is_test_path(rel):
        return False
    return not str(rel).startswith(KEYSPACE_MODULES)


def collect() -> tuple[dict[str, list[Finding]], dict[str, list[Finding]]]:
    unbudgeted: dict[str, list[Finding]] = {}
    budgeted: dict[str, list[Finding]] = {}
    for path in sorted(CRATES.rglob("*.rs")):
        rel = path.relative_to(ROOT)
        if not in_scope(rel):
            continue
        bad, good = scan(path)
        if bad:
            unbudgeted[str(rel)] = bad
        if good:
            budgeted[str(rel)] = good
    return unbudgeted, budgeted


def stale_pins(
    unbudgeted: dict[str, list[Finding]], allowlist: dict[str, tuple[int, str]]
) -> list[str]:
    """The other direction of the ratchet: pins that no longer match the tree.

    A pinned file that stopped growing unbudgeted (converted) must leave the
    list, and a pinned file that grew a *new* unbudgeted site fails exactly like
    an unpinned file would.
    """
    stale = []
    for f, (expected, _reason) in sorted(allowlist.items()):
        actual = len(unbudgeted.get(f, []))
        if actual == 0:
            stale.append(f"  {f}: pinned, but grows nothing unbudgeted any more — drop the entry")
        elif actual != expected:
            stale.append(
                f"  {f}: pinned at {expected} unbudgeted growth site(s), found {actual}. "
                "Convert the new one, or re-justify and update the count."
            )
    return stale


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true", help="print every growth site the gate sees")
    ap.add_argument("--pins", action="store_true", help="print ALLOWLIST-shaped counts")
    args = ap.parse_args()

    unbudgeted, budgeted = collect()

    if args.list:
        for items in [v for _, v in sorted(budgeted.items())]:
            for it in items:
                print(f"charged   {it.path}:{it.line}  {it.struct}.{it.field}.{it.op}")
        for f, items in sorted(unbudgeted.items()):
            mark = "pinned" if f in ALLOWLIST else "VIOLATION"
            for it in items:
                print(f"{mark:<9} {it.path}:{it.line}  {it.struct}.{it.field}.{it.op}")
        return 0

    if args.pins:
        for f, items in sorted(unbudgeted.items()):
            print(f'    "{f}": ({len(items)}, ""),')
        return 0

    status = 0
    violations = {f: v for f, v in unbudgeted.items() if f not in ALLOWLIST}
    if violations:
        print("ERROR: a buffer grows without charging a Budget:", file=sys.stderr)
        for f in sorted(violations):
            for it in violations[f]:
                print(f"  {it.path}:{it.line}: {it.struct}.{it.field}.{it.op}(..)", file=sys.stderr)
                print(f"      {it.source}", file=sys.stderr)
        print(file=sys.stderr)
        print("       A structure that cannot charge cannot grow (adr/0006 §2).", file=sys.stderr)
        print("       Give the struct a `frogdb_memory::Charge` obtained from", file=sys.stderr)
        print("       its subsystem's `Budget`, charge before the growth", file=sys.stderr)
        print("       (`Charge::grow`), release on the shrink path, and handle", file=sys.stderr)
        print("       a `Refused` at that seam — shed or backpressure, the one", file=sys.stderr)
        print("       the Budget declares. A buffer that cannot be converted", file=sys.stderr)
        print("       yet goes in ALLOWLIST in this script, with a reason.", file=sys.stderr)
        status = 1

    stale = stale_pins(unbudgeted, ALLOWLIST)
    if stale:
        print("ERROR: the budget-growth ratchet is out of date:", file=sys.stderr)
        print("\n".join(stale), file=sys.stderr)
        status = 1

    if status == 0:
        pinned = sum(n for n, _ in ALLOWLIST.values())
        charged = sum(len(v) for v in budgeted.values())
        print(
            f"OK: {charged} budgeted growth site(s); "
            f"{pinned} unconverted site(s) pinned in {len(ALLOWLIST)} file(s)"
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
