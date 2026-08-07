#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Regression tests for scripts/continuation-lock-gate.py's Rust scanning.

Run: ./scripts/tests/test_continuation_lock_gate.py   (or `just test-continuation-lock-gate`)

The gate's enforcement is only as good as the two scanners it rests on:
`dispatch_arms` (top-level match arms of a `dispatch_*` fn, with their bodies)
and `enum_variants` (the `*Msg` variants in `message.rs`). Both are indentation +
brace-matching heuristics over rustfmt'd source, so they are pinned here against
the shapes the real dispatch files actually contain — a nested `match` inside an
arm, an arm whose body is a single delegating call, a `#[cfg(test)]` module with
its own `match`, and a second non-dispatch `fn` in the same file.

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on the first
failure (same shape as test_coverage_depth.py).
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
from pathlib import Path

# continuation-lock-gate.py has hyphens, so it is not importable by name.
_SCRIPT = Path(__file__).resolve().parent.parent / "continuation-lock-gate.py"
_spec = importlib.util.spec_from_file_location("continuation_lock_gate", _SCRIPT)
assert _spec and _spec.loader
clg = importlib.util.module_from_spec(_spec)
sys.modules["continuation_lock_gate"] = clg
_spec.loader.exec_module(clg)


def _arms(source: str) -> list:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "dispatch_fake.rs"
        path.write_text(source)
        return clg.dispatch_arms(path)


DISPATCH_SRC = """\
use super::message::FakeMsg;

impl ShardWorker {
    /// Dispatch fake messages.
    pub(super) async fn dispatch_fake(&mut self, msg: FakeMsg) -> bool {
        match msg {
            FakeMsg::Gated { conn_id, response_tx } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(err);
                    return false;
                }
                let _ = response_tx.send(self.work().await);
            }
            FakeMsg::Nested { kind, response_tx } => {
                // A nested match must not be mistaken for a top-level arm.
                let out = match kind {
                    Kind::A => 1,
                    Kind::B => 2,
                };
                let _ = response_tx.send(out);
            }
            FakeMsg::Delegating { txid } => {
                self.handle_delegating(txid);
            }
            FakeMsg::Terse { .. } => {}
        }
        false
    }

    /// A non-dispatch fn in the same file, whose own `match` arms sit at the
    /// same column but are not message arms.
    fn describe(&self, err: &Response) -> String {
        match err {
            Response::Error(msg) => msg.clone(),
            _ => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_test_with_its_own_dispatch_match() {
        fn dispatch_fake_helper(msg: FakeMsg) {
            match msg {
                FakeMsg::TestOnly { .. } => {}
            }
        }
    }
}
"""

MESSAGE_SRC = """\
/// Fake messages.
#[derive(Debug)]
pub enum FakeMsg {
    /// Doc comment.
    Gated {
        conn_id: ConnId,
        response_tx: oneshot::Sender<Response>,
    },
    #[allow(dead_code)]
    Nested { kind: Kind, response_tx: oneshot::Sender<u8> },
    Delegating {
        txid: u64,
    },
    Terse,
}

pub enum OtherMsg {
    Only,
}
"""


def test_dispatch_arms_finds_exactly_the_top_level_arms() -> None:
    arms = _arms(DISPATCH_SRC)
    names = [a.name for a in arms]
    assert names == [
        "FakeMsg::Gated",
        "FakeMsg::Nested",
        "FakeMsg::Delegating",
        "FakeMsg::Terse",
    ], names


def test_dispatch_arms_skips_cfg_test_and_non_dispatch_fns() -> None:
    # `FakeMsg::TestOnly` lives in a `#[cfg(test)]` module; `Response::Error`
    # is a non-message arm of a sibling fn. Neither may be collected.
    names = {a.name for a in _arms(DISPATCH_SRC)}
    assert "FakeMsg::TestOnly" not in names, names
    assert not any(n.startswith("Response::") for n in names), names


def test_arm_bodies_carry_only_their_own_lines() -> None:
    arms = {a.name: a for a in _arms(DISPATCH_SRC)}
    # The gate call belongs to `Gated` and to nothing after it — an arm body
    # that leaked into the next arm would make rule 4 (unpinned gating) fire on
    # innocent arms and rule 3 (missing gate) never fire at all.
    assert arms["FakeMsg::Gated"].gates()
    assert not arms["FakeMsg::Nested"].gates()
    assert not arms["FakeMsg::Delegating"].gates()
    assert not arms["FakeMsg::Terse"].gates()
    # The nested `match kind` lines stay inside the arm that owns them.
    assert "Kind::A => 1," in arms["FakeMsg::Nested"].body
    assert "Kind::A" not in arms["FakeMsg::Gated"].body


def test_arm_line_spans_are_contiguous_and_ordered() -> None:
    arms = _arms(DISPATCH_SRC)
    for prev, nxt in zip(arms, arms[1:], strict=False):
        assert prev.start < prev.end < nxt.start, (prev.name, nxt.name)
    # The last arm's span must close before the fn does, not run to EOF (which
    # would swallow the `#[cfg(test)]` module into rule 5's allowed spans).
    assert arms[-1].end < len(DISPATCH_SRC.splitlines())


def test_enum_variants_reads_variants_not_fields() -> None:
    variants = clg.enum_variants("FakeMsg", MESSAGE_SRC.splitlines())
    assert variants == ["Gated", "Nested", "Delegating", "Terse"], variants
    # Scoped to the named enum: the next enum's variants must not leak in.
    assert clg.enum_variants("OtherMsg", MESSAGE_SRC.splitlines()) == ["Only"]
    assert clg.enum_variants("NoSuchMsg", MESSAGE_SRC.splitlines()) is None


def test_pins_are_disjoint_and_tagged() -> None:
    assert not (set(clg.GATE) & set(clg.EXEMPT))
    assert not (set(clg.GATE) & set(clg.GATE_GAP))
    assert not (set(clg.EXEMPT) & set(clg.GATE_GAP))
    assert clg.tag("CoreMsg::Execute") == "GATE"
    assert clg.tag("CoreMsg::GetVersion") == "EXEMPT"
    assert clg.tag("CoreMsg::ExecTransaction") == "GATE-GAP"
    assert clg.tag("CoreMsg::Whatever") == "-"


def test_real_dispatch_surface_matches_the_pins() -> None:
    """End-to-end: the live tree parses to exactly the pinned per-enum counts."""
    for filename, (enum, pinned) in clg.DISPATCH.items():
        arms = clg.dispatch_arms(clg.SHARD / filename)
        assert len(arms) == pinned, (filename, len(arms), pinned)
        assert all(a.name.startswith(f"{enum}::") for a in arms), filename


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
