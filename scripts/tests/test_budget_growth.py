#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for scripts/budget-growth.py's Rust scanning and ratchet.

Run: ./scripts/tests/test_budget_growth.py   (or `just test-budget-growth`)

The gate rests on three heuristics over rustfmt'd source — struct field parsing,
`impl`-block attribution of a `self.<field>` growth call, and the rejoining of a
method chain rustfmt split across lines — plus the both-directions allowlist
check that makes it a ratchet rather than a suppression list. All four are
pinned here.

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on the first
failure (same shape as test_continuation_lock_gate.py).
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

# budget-growth.py has a hyphen, so it is not importable by name.
_SCRIPT = Path(__file__).resolve().parent.parent / "budget-growth.py"
_spec = importlib.util.spec_from_file_location("budget_growth", _SCRIPT)
assert _spec and _spec.loader
bg = importlib.util.module_from_spec(_spec)
sys.modules["budget_growth"] = bg
_spec.loader.exec_module(bg)


def scan(source: str) -> tuple[list, list]:
    """(unbudgeted, budgeted) findings for a synthetic file."""
    with tempfile.TemporaryDirectory() as tmp:
        # The scanner reports paths relative to the repo root, so the fixture
        # has to live under it; its content is what is being tested.
        path = Path(bg.ROOT) / "target" / "budget_growth_fixture.rs"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source)
        try:
            return bg.scan(path)
        finally:
            path.unlink()
            del tmp


UNBUDGETED = """\
pub struct OutputBuffer {
    /// Frames not yet written to the socket.
    pending: Vec<Bytes>,
    written: u64,
}

impl OutputBuffer {
    pub fn queue(&mut self, frame: Bytes) {
        self.pending.push(frame);
        self.written += 1;
    }
}
"""

BUDGETED = """\
pub struct OutputBuffer {
    pending: Vec<Bytes>,
    charge: Charge,
}

impl OutputBuffer {
    pub fn queue(&mut self, frame: Bytes) -> Result<(), Refused> {
        self.charge.grow(frame.len() as u64)?;
        self.pending.push(frame);
        Ok(())
    }
}
"""

SPLIT_CHAIN = """\
pub struct Table {
    key_to_clients: HashMap<Bytes, HashSet<ConnId>>,
}

impl Table {
    fn record(&mut self, key: Bytes, conn: ConnId) {
        self.key_to_clients
            .entry(key)
            .or_default()
            .insert(conn);
    }
}
"""

NOT_A_BUFFER = """\
pub struct Counters {
    depth: usize,
    sink: Arc<Mutex<Vec<u8>>>,
    tx: UnboundedSender<Msg>,
}

impl Counters {
    fn bump(&mut self) {
        self.depth += 1;
        self.sink.insert(0, 7);
        self.tx.push(1);
    }
}
"""

TEST_MODULE_ONLY = """\
pub struct Scratch {
    items: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Scratch {
        fn fill(&mut self) {
            self.items.push(1);
        }
    }
}
"""

TWO_STRUCTS = """\
pub struct Charged {
    rows: Vec<u8>,
    charge: frogdb_memory::Charge,
}

pub struct Uncharged {
    rows: Vec<u8>,
}

impl Charged {
    fn add(&mut self, b: u8) {
        self.rows.push(b);
    }
}

impl Uncharged {
    fn add(&mut self, b: u8) {
        self.rows.push(b);
    }
}
"""


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        print(f"FAIL: {name}{': ' + detail if detail else ''}", file=sys.stderr)
        sys.exit(1)
    print(f"ok: {name}")


def main() -> int:
    bad, good = scan(UNBUDGETED)
    check("an unbudgeted buffer growth is a violation", len(bad) == 1 and not good, str(bad))
    check("the violation names the struct and field", bad[0].struct == "OutputBuffer", str(bad[0]))
    check("...and the field it grows", bad[0].field == "pending", str(bad[0]))

    bad, good = scan(BUDGETED)
    check("a struct owning a Charge is compliant", not bad and len(good) == 1, str((bad, good)))

    bad, _ = scan(SPLIT_CHAIN)
    check("a rustfmt-split chain is one growth site", len(bad) == 1, str(bad))

    bad, good = scan(NOT_A_BUFFER)
    check("counters and handles are not buffers", not bad and not good, str(bad))

    bad, good = scan(TEST_MODULE_ONLY)
    check("growth inside #[cfg(test)] is out of scope", not bad and not good, str(bad))

    bad, good = scan(TWO_STRUCTS)
    check(
        "attribution is per struct, not per file",
        len(bad) == 1 and bad[0].struct == "Uncharged" and len(good) == 1,
        str((bad, good)),
    )

    # --- The ratchet, both directions ---------------------------------------
    unbudgeted, _ = bg.collect()
    check(
        "the tree as landed passes",
        all(f in bg.ALLOWLIST for f in unbudgeted),
        str(sorted(set(unbudgeted) - set(bg.ALLOWLIST))),
    )
    check(
        "every pinned file still has unbudgeted growth",
        all(f in unbudgeted for f in bg.ALLOWLIST),
        str(sorted(set(bg.ALLOWLIST) - set(unbudgeted))),
    )
    check(
        "every pin's count matches the tree",
        all(len(unbudgeted[f]) == n for f, (n, _) in bg.ALLOWLIST.items()),
        str(
            {
                f: (n, len(unbudgeted.get(f, [])))
                for f, (n, _) in bg.ALLOWLIST.items()
                if len(unbudgeted.get(f, [])) != n
            }
        ),
    )
    check(
        "every pin carries a reason",
        all(reason.strip() for _, reason in bg.ALLOWLIST.values()),
        str([f for f, (_, r) in bg.ALLOWLIST.items() if not r.strip()]),
    )

    # Both directions, driven synthetically so the assertions do not depend on
    # which files happen to be pinned today.
    one = next(iter(bg.ALLOWLIST))
    site = unbudgeted[one][0]
    check(
        "a converted buffer must leave the list",
        bool(bg.stale_pins({}, {one: (1, "why")})),
        "a pin whose file no longer grows unbudgeted must be reported stale",
    )
    check(
        "a new violation in a pinned file fails",
        bool(bg.stale_pins({one: [site, site]}, {one: (1, "why")})),
        "a second site under a pin of 1 must be reported",
    )
    check(
        "a matching pin is silent",
        not bg.stale_pins({one: [site]}, {one: (1, "why")}),
    )

    bad, _ = scan(
        "pub struct Two {\n"
        "    rows: Vec<u8>,\n"
        "}\n"
        "impl Two {\n"
        "    fn add(&mut self) {\n"
        "        self.rows.push(1); self.rows.push(2);\n"
        "    }\n"
        "}\n"
    )
    check("two growth calls on one line count twice", len(bad) == 2, str(bad))

    print("\nall budget-growth scanner tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
