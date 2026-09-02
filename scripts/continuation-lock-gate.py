#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: every mutating shard-dispatch arm states a continuation-lock disposition.

Hardening-2 W1 rule C3 (`.scratch/hardening-2/PRD.md` §3.1, §8.4 ruling 4). The
chokepoint is `ShardWorker::can_execute_during_lock(conn_id)`
(`core/src/shard/worker.rs:855-862`): it rejects work from any connection that is
not the continuation-lock owner while a VLL continuation lock is held. The
invariant is that every shard-message arm which mutates state either **calls that
gate** (GATE) or is **pinned EXEMPT** with a documented reason and a named forcing
test. The dispositions this script encodes were settled — with evidence and two
merged forcing tests — in `.scratch/hardening-2/c3-arm-dispositions.md`.

## Why a count pin and not a full classification

The dispatch surface is 64 arms across 11 `*Msg` enums, and the large majority
(pub/sub registration, observability counters, DEBUG probes, tracking tables)
never touch the keyspace the continuation lock protects. Classifying all 64 by
hand — the first attempt — produced a table that was mostly noise and would go
stale on every unrelated arm addition.

**The design choice recorded here: only the interesting arms are named.** Three
small pinned sets carry a per-arm disposition (GATE, EXEMPT, GATE_GAP); every
other arm is covered *purely by the pinned per-enum arm count*. There is no third
`NONMUTATING` set. The count is what makes that safe: a new or renamed arm moves
the count, and the failure message prints the enum's arms annotated with their
classification, so the unclassified newcomer is the one without a tag. That gives
the same forcing function as a full table (a new arm cannot land without a
human deciding) at 9 pinned names instead of 64.

## What is enforced

1. **Count pin.** Each `dispatch_*.rs` file's arm count must equal its pin.
2. **Enum parity.** The arms in a dispatch file must be exactly the variants of
   its `*Msg` enum in `message.rs` — in both directions. A variant handled
   somewhere else (or an arm for a variant that no longer exists) fails.
3. **GATE arms really gate.** Each pinned GATE arm's body must contain a
   `can_execute_during_lock(` call.
4. **No unpinned gating.** An arm that calls the gate but is not pinned GATE
   fails — the pin cannot silently fall behind the code.
5. **The disposition is stated at the arm.** `can_execute_during_lock` may be
   called only from a pinned GATE arm (its definition and doc-comment mentions in
   `worker.rs` / `vll.rs` / `dispatch_core.rs` aside). A gate buried inside a
   handler is invisible at the dispatch site, which is the whole point of the
   chokepoint; move it to the arm.
6. **EXEMPT entries are live.** Each EXEMPT arm must exist, must *not* have
   gained a gate call (that would make the exemption stale — promote it to GATE),
   and its named forcing test must still exist in the named file.
7. **GATE_GAP entries are live.** The two known bypasses ride the named-gap
   warn-not-fail idiom (`scripts/nested-config.py`, `scripts/spec-lint.py`):
   they WARN while their issue link resolves to a real issue file, and the moment
   the fix lands (the arm gains a gate call) the stale entry hard-fails, forcing
   its promotion to GATE.
8. **Sets are disjoint and every pinned name exists** — stale pins fail too, the
   same bidirectional ratchet the rest of the family uses.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SHARD = ROOT / "frogdb-server" / "crates" / "core" / "src" / "shard"
MESSAGE = SHARD / "message.rs"

GATE_FN = "can_execute_during_lock"
GATE_CALL = re.compile(rf"\b{GATE_FN}\s*\(")

# dispatch file -> (message enum, pinned arm count).
#
# The count is the ratchet: a new, renamed or removed arm anywhere on the
# shard-dispatch surface fails this gate and forces a GATE/EXEMPT decision.
DISPATCH: dict[str, tuple[str, int]] = {
    "dispatch_core.rs": ("CoreMsg", 4),
    "dispatch_pubsub.rs": ("PubSubMsg", 11),
    "dispatch_tracking.rs": ("TrackingMsg", 3),
    "dispatch_scripting.rs": ("ScriptingMsg", 8),
    "dispatch_blocking.rs": ("BlockingMsg", 3),
    "dispatch_observability.rs": ("ObservabilityMsg", 18),
    "dispatch_vll.rs": ("VllMsg", 5),
    "dispatch_cluster.rs": ("ClusterMsg", 3),
    "dispatch_debug_introspection.rs": ("DebugIntrospectionMsg", 8),
    "dispatch_search.rs": ("SearchMsg", 3),
    "dispatch_replication.rs": ("ReplicationMsg", 2),
}

# Arms that reach store execution under a caller-supplied `conn_id` and gate on
# it. Each must contain a `can_execute_during_lock(` call in its own arm body.
GATE: set[str] = {
    "CoreMsg::Execute",
    "CoreMsg::ScatterRequest",
    "ScriptingMsg::EvalScript",
    "ScriptingMsg::EvalScriptSha",
    "ScriptingMsg::ScriptSubCommand",
}

# Arms that mutate but are deliberately ungated, each with the reason and the
# forcing test that fails if the reason stops holding. Both were settled by
# investigation in `.scratch/hardening-2/c3-arm-dispositions.md`; do not add a
# third without the same evidence + a merged forcing test.
#
# arm -> (reason, forcing test fn, file the test lives in, relative to SHARD)
EXEMPT: dict[str, tuple[str, str, str]] = {
    "VllMsg::VllExecute": (
        "the VLL two-phase protocol is the isolation seam: a continuation lock is "
        "granted only on a drained shard and refuses every enqueue while held or "
        "pending, so `dequeue_for_execution` returns nothing and the hardcoded "
        "`conn_id = 0` drain path can never mutate under a foreign lock",
        "vll_execute_cannot_mutate_a_held_key_under_a_foreign_continuation_lock",
        "vll.rs",
    ),
    "CoreMsg::GetVersion": (
        "lazy expiry of already-dead keys with the version bump withheld: "
        "`purge_if_expired` removes only keys past their deadline (a live watched "
        "key is untouched) and `apply_lazy_purge_effects_no_version_bump` skips "
        "`bump_versions_for`, so the lock owner's WATCH set cannot be aborted by "
        "this arm",
        "get_version_purges_only_expired_keys_without_bumping_under_continuation_lock",
        "dispatch_core.rs",
    ),
}

# Known bypasses: arms that mutate under a caller-supplied `conn_id` and *should*
# gate, whose fix is tracked but has not landed. Named-gap warn-not-fail — the
# entry warns while its issue file resolves, and hard-fails the moment the arm
# gains a gate call (fix landed → promote to GATE and drop the entry).
#
# arm -> (reason, issue path relative to ROOT)
GATE_GAP: dict[str, tuple[str, str]] = {
    "CoreMsg::ExecTransaction": (
        "MULTI/EXEC runs its queued commands with no continuation-lock check, so a "
        "second connection's EXEC mutates shards a cross-shard script believes it "
        "holds exclusively",
        ".scratch/testing-improvements-round2/issues/open/"
        "50-multi-exec-bypasses-vll-continuation-gate.md",
    ),
    "ScriptingMsg::FunctionCall": (
        "FCALL runs arbitrary Lua through `handle_function_call` and applies its "
        "writes via `run_script_write_effects`, while its EvalScript/EvalScriptSha "
        "siblings in the same file gate",
        ".scratch/hardening-2/issues/open/05-functioncall-bypasses-the-vll-continuation-gate.md",
    ),
}

# `pub(super) fn dispatch_<x>(&mut self, msg: <Enum>)`, sync or async.
DISPATCH_FN = re.compile(r"\bfn\s+dispatch_[a-z_]+\s*\(")
# A top-level match arm inside a dispatch fn. rustfmt (enforced by `just
# fmt-check` in CI) puts these at exactly 12 columns: impl 0, fn 4, match 8,
# arm 12. Anything nested inside an arm is deeper.
ARM_HEAD = re.compile(r"^ {12}([A-Z][A-Za-z0-9_]*Msg)::([A-Za-z0-9_]+)\b")
# An enum variant in message.rs: rustfmt puts variants at 4 columns, their
# fields at 8. Doc comments and attributes start with `/` or `#`.
VARIANT = re.compile(r"^ {4}([A-Z][A-Za-z0-9_]*)\s*[{(,]")


class Arm:
    """One top-level match arm of a `dispatch_*` fn."""

    def __init__(self, name: str, start: int) -> None:
        self.name = name  # "CoreMsg::Execute"
        self.start = start  # 1-based line of the arm head
        self.end = start  # 1-based line of the arm's last line
        self.body = ""

    def gates(self) -> bool:
        return bool(GATE_CALL.search(self.body))


def enum_variants(name: str, lines: list[str]) -> list[str] | None:
    """Variant names of `pub enum <name>` in message.rs, in source order."""
    head = re.compile(rf"\benum\s+{re.escape(name)}\s*\{{")
    for i, line in enumerate(lines):
        if not head.search(line):
            continue
        variants: list[str] = []
        depth, opened, j = 0, False, i
        while j < len(lines):
            depth += lines[j].count("{") - lines[j].count("}")
            opened = opened or "{" in lines[j]
            if j > i and (m := VARIANT.match(lines[j])):
                variants.append(m.group(1))
            if opened and depth <= 0:
                break
            j += 1
        return variants
    return None


def dispatch_arms(path: Path) -> list[Arm]:
    """Top-level arms of every `dispatch_*` fn in `path`, outside `#[cfg(test)]`."""
    lines = path.read_text().splitlines()
    spans = cfg_test_spans(lines)
    arms: list[Arm] = []
    i, n = 0, len(lines)
    while i < n:
        if in_any_span(i, spans) or not DISPATCH_FN.search(lines[i]):
            i += 1
            continue
        # Brace-match the fn body, collecting its arm heads as we go.
        depth, opened, j = 0, False, i
        current: Arm | None = None
        while j < n:
            depth += lines[j].count("{") - lines[j].count("}")
            opened = opened or "{" in lines[j]
            if j > i and (m := ARM_HEAD.match(lines[j])):
                if current is not None:
                    current.end = j  # previous arm ended on the line above
                current = Arm(f"{m.group(1)}::{m.group(2)}", j + 1)
                arms.append(current)
            if opened and depth <= 0:
                break
            j += 1
        if current is not None:
            current.end = j
        for arm in arms:
            if arm.body == "":
                arm.body = "\n".join(lines[arm.start - 1 : arm.end])
        i = j + 1
    return arms


def tag(name: str) -> str:
    if name in GATE:
        return "GATE"
    if name in EXEMPT:
        return "EXEMPT"
    if name in GATE_GAP:
        return "GATE-GAP"
    return "-"


def report_arms(enum: str, arms: list[Arm]) -> None:
    print(f"       {enum} arms now present:", file=sys.stderr)
    for arm in arms:
        print(f"         {arm.name.split('::', 1)[1]:<28} [{tag(arm.name)}]", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "       An arm tagged `-` carries no continuation-lock disposition. If it\n"
        "       mutates state, add the `can_execute_during_lock(conn_id)` guard to\n"
        "       its body and pin it in GATE in this script. If it does not mutate\n"
        "       the keyspace, just bump the pinned count. If it mutates but must\n"
        "       stay ungated, pin it in EXEMPT with a one-line reason AND a forcing\n"
        "       test that fails when the reason stops holding.",
        file=sys.stderr,
    )


def main() -> int:  # noqa: C901 — one linear pass per rule, kept together on purpose
    status = 0
    warnings: list[str] = []

    if not MESSAGE.is_file():
        print(f"ERROR: {MESSAGE} not found — did the shard message enums move?", file=sys.stderr)
        return 1

    msg_lines = MESSAGE.read_text().splitlines()

    # Rule 8a: the three pinned sets must be disjoint.
    for a, b in (("GATE", "EXEMPT"), ("GATE", "GATE_GAP"), ("EXEMPT", "GATE_GAP")):
        overlap = set(globals()[a]) & set(globals()[b])
        if overlap:
            print(
                f"ERROR: arm(s) pinned in both {a} and {b}: {', '.join(sorted(overlap))} — "
                "an arm has exactly one disposition.",
                file=sys.stderr,
            )
            status = 1

    all_arms: dict[str, Arm] = {}

    for filename, (enum, pinned) in sorted(DISPATCH.items()):
        path = SHARD / filename
        rel = str(path.relative_to(ROOT))
        if not path.is_file():
            print(
                f"ERROR: {rel} not found — the dispatch surface moved; update DISPATCH.",
                file=sys.stderr,
            )
            status = 1
            continue

        arms = dispatch_arms(path)
        foreign = [a for a in arms if not a.name.startswith(f"{enum}::")]
        if foreign:
            print(
                f"ERROR: {rel}: expected only `{enum}` arms, found "
                f"{', '.join(a.name for a in foreign)} — update DISPATCH.",
                file=sys.stderr,
            )
            status = 1
        for arm in arms:
            all_arms[arm.name] = arm

        # Rule 1: the count pin.
        if len(arms) != pinned:
            print(
                f"ERROR: {rel}: pinned at {pinned} `{enum}` dispatch arm(s), found {len(arms)}.",
                file=sys.stderr,
            )
            report_arms(enum, arms)
            status = 1

        # Rule 2: enum parity, both directions.
        variants = enum_variants(enum, msg_lines)
        if variants is None:
            print(
                f"ERROR: `pub enum {enum}` not found in "
                f"{MESSAGE.relative_to(ROOT)} — update DISPATCH.",
                file=sys.stderr,
            )
            status = 1
        else:
            handled = {a.name.split("::", 1)[1] for a in arms}
            missing = sorted(set(variants) - handled)
            extra = sorted(handled - set(variants))
            if missing:
                print(
                    f"ERROR: {enum} variant(s) with no arm in {rel}: {', '.join(missing)} — "
                    "a variant handled outside the dispatch file escapes this gate.",
                    file=sys.stderr,
                )
                status = 1
            if extra:
                print(
                    f"ERROR: {rel}: arm(s) for non-existent {enum} variant(s): {', '.join(extra)}.",
                    file=sys.stderr,
                )
                status = 1

    # Rule 8b: every pinned name must name a real arm.
    for setname, names in (("GATE", GATE), ("EXEMPT", EXEMPT), ("GATE_GAP", GATE_GAP)):
        stale = sorted(n for n in names if n not in all_arms)
        if stale:
            print(
                f"ERROR: {setname} pins arm(s) that no longer exist: {', '.join(stale)} — "
                "drop or rename the entry.",
                file=sys.stderr,
            )
            status = 1

    # Rule 3: a pinned GATE arm must actually call the gate.
    ungated = sorted(n for n in GATE if n in all_arms and not all_arms[n].gates())
    if ungated:
        print("ERROR: arm pinned GATE but missing the continuation-lock gate:", file=sys.stderr)
        for name in ungated:
            arm = all_arms[name]
            print(f"  {name} (arm at line {arm.start})", file=sys.stderr)
        print(file=sys.stderr)
        print(
            "       A mutating arm must refuse work from a connection that does not\n"
            "       own the continuation lock, or a concurrent client writes shards a\n"
            "       cross-shard script believes it holds exclusively. Restore:\n"
            "           if let Err(err) = self.can_execute_during_lock(conn_id) {\n"
            "               let _ = response_tx.send(err);\n"
            "               return false;\n"
            "           }\n"
            "       If the arm genuinely no longer needs the gate, move it out of GATE\n"
            "       into EXEMPT with a reason and a forcing test.",
            file=sys.stderr,
        )
        status = 1

    # Rule 4: an arm that gates must be pinned GATE.
    unpinned = sorted(n for n, a in all_arms.items() if a.gates() and n not in GATE)
    if unpinned:
        print(
            "ERROR: arm calls the continuation-lock gate but is not pinned GATE:", file=sys.stderr
        )
        for name in unpinned:
            print(f"  {name} (arm at line {all_arms[name].start})", file=sys.stderr)
        print(
            "       Add it to GATE in this script (and drop any stale EXEMPT/GATE_GAP\n"
            "       entry for it) so the pin records the disposition the code now has.",
            file=sys.stderr,
        )
        status = 1

    # Rule 6: EXEMPT entries stay live — the forcing test must still exist.
    for name, (_reason, test_fn, test_file) in sorted(EXEMPT.items()):
        path = SHARD / test_file
        if not path.is_file() or not re.search(
            rf"\bfn\s+{re.escape(test_fn)}\s*\(", path.read_text()
        ):
            print(
                f"ERROR: {name}: forcing test `{test_fn}` not found in "
                f"{(SHARD / test_file).relative_to(ROOT)} — an exemption without its "
                "forcing test is an unproven claim. Restore the test or re-justify.",
                file=sys.stderr,
            )
            status = 1

    # Rule 7: GATE_GAP entries stay live — issue resolves, fix has not landed.
    for name, (reason, issue) in sorted(GATE_GAP.items()):
        if not (ROOT / issue).is_file():
            print(
                f"ERROR: {name}: gap link does not resolve to a real issue file: {issue}",
                file=sys.stderr,
            )
            status = 1
            continue
        warnings.append(f"  {name}  [gap: {issue}] {reason}")

    # Rule 5: the disposition is stated at the arm, so the gate is called nowhere
    # else. Comments (the dispositions are documented in prose) and the
    # definition itself are not calls.
    strays: list[str] = []
    for path in sorted(SHARD.rglob("*.rs")):
        lines = path.read_text().splitlines()
        spans = cfg_test_spans(lines)
        rel = str(path.relative_to(ROOT))
        gate_spans = [
            (a.start - 1, a.end - 1)
            for a in dispatch_arms(path)
            if a.name in GATE and path.name in DISPATCH
        ]
        for i, line in enumerate(lines):
            if not GATE_CALL.search(line) or line.lstrip().startswith("//"):
                continue
            if f"fn {GATE_FN}" in line or in_any_span(i, spans) or in_any_span(i, gate_spans):
                continue
            strays.append(f"  {rel}:{i + 1}")
    if strays:
        print(f"ERROR: `{GATE_FN}` called outside a pinned GATE dispatch arm:", file=sys.stderr)
        print("\n".join(strays), file=sys.stderr)
        print(
            "       The continuation-lock disposition is stated at the dispatch arm,\n"
            "       where a reader of the arm can see it; a gate buried in a handler\n"
            "       is invisible at the dispatch site. Move the call to the arm and\n"
            "       pin the arm in GATE.",
            file=sys.stderr,
        )
        status = 1

    if warnings:
        print(
            "WARNING: continuation-lock gate has open named-gap bypasses (tracked, fix not landed):"
        )
        print("\n".join(warnings))

    if status == 0:
        total = sum(count for _enum, count in DISPATCH.values())
        print(
            f"OK: shard-dispatch continuation-lock dispositions hold "
            f"({total} arms pinned: {len(GATE)} GATE, {len(EXEMPT)} EXEMPT, "
            f"{len(GATE_GAP)} tracked gap(s))"
        )
    return status


if __name__ == "__main__":
    sys.exit(main())
