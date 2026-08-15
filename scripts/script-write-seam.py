#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: the shard write seam is the only write path reachable from a script.

Spec-gaps issue 06 / `FM-TXN-051` (`specs/txn.md`). A script's `redis.call` is
the one producer of shard writes that the connection's gauntlet never saw: the
declared key set is validated at EVAL time, but the *runtime* call is invented by
Lua. `ShardWriteSeam::admit` (`core/src/write_seam.rs`) is where slot ownership,
ACL and write-admission (self-fence + `min-replicas-to-write`) are decided for
those writes, and `ScriptCommandGate::dispatch` (`core/src/scripting/gate.rs`) is
the single place a scripted sub-command is executed. This gate keeps that
structure — clippy can express neither "this call only from that function" nor
"in this order", so a source-text check is the honest tool.

## What is enforced

1. **The gate admits before it dispatches.** `dispatch` must call
   `invoker.admit(`, and that call must come *before* both `run_local` /
   `run_remote` and before `mark_write()` — a refused write must never run and
   must never leave the script write-dirty (a write-dirty script is deliberately
   unkillable).
2. **One dispatch path.** `invoker.run_local(` / `invoker.run_remote(` appear
   only inside `gate.rs`. A second executor for scripted sub-commands is a write
   path that never meets the seam.
3. **The seam is assembled at the shard.** `ShardWriteSeam::new(` may appear only
   in `write_seam.rs` (its own module) and `shard/worker.rs` (the assembler that
   reads the live cluster state, node id, quorum checker and replication
   tracker). Anywhere else means hand-built — and therefore stale or partial —
   inputs.
4. **The script context takes the seam from that assembler.** Every
   `write_seam = Some(` assignment must be fed by `self.write_seam(`.
5. **Bypasses are pinned.** `WriteAdmission::pre_authorized()` turns every gate
   off; it exists for the replica-apply / WAL-replay path, where the write was
   already admitted on the primary. `WriteAdmission::internal()` skips ACL and
   the replica floor (slot ownership and the self-fence still apply). Each is
   allowed only from a pinned file, so a new bypass has to be argued for here.
6. **Mutating shard messages carry an admission.** The seam's issuer-scoped
   inputs (ACL identity, the live `min-replicas-to-write`) cannot be read on the
   shard, so they ride the message. Each pinned variant must declare an
   `admission:` field; a new script/transaction variant that forgets it would
   compile and silently admit everything.

Test code is out of scope — `#[cfg(test)]` spans (via `scripts/_rustscan.py`),
in-`src/` test modules, integration `tests/` dirs and benches are all skipped —
so tests may build seams and admissions freely.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span, is_test_path  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
CRATES = REPO / "frogdb-server" / "crates"

GATE = CRATES / "core/src/scripting/gate.rs"
SCRIPTING_DIR = CRATES / "core/src/scripting"
WRITE_SEAM = CRATES / "core/src/write_seam.rs"
WORKER = CRATES / "core/src/shard/worker.rs"
MESSAGE = CRATES / "core/src/shard/message.rs"

# Where a seam may be assembled (production code).
SEAM_BUILDERS = {
    "frogdb-server/crates/core/src/write_seam.rs",
    "frogdb-server/crates/core/src/shard/worker.rs",
}

# Pinned bypasses: constructor -> {allowed file, ...} (production code).
BYPASS_PINS = {
    "WriteAdmission::pre_authorized": {
        "frogdb-server/crates/replication-runtime/src/executor.rs",
    },
    "WriteAdmission::internal": {
        "frogdb-server/crates/shard-harness/src/harness.rs",
    },
}

# Shard-message variants that carry writes a connection never gauntleted.
ADMISSION_VARIANTS = ("ExecTransaction", "EvalScript", "EvalScriptSha", "FunctionCall")


def production_text(path: Path) -> str:
    """File text with every `#[cfg(test)]` item blanked out (line numbers kept)."""
    lines = path.read_text().splitlines()
    spans = cfg_test_spans(lines)
    return "\n".join("" if in_any_span(index, spans) else line for index, line in enumerate(lines))


def rust_files(root: Path) -> list[Path]:
    """Production sources under `root`.

    Excluded: in-`src/` test modules (`is_test_path`), a crate's integration
    `tests/` directory, and benches. Test code may build seams and admissions
    freely — this family gates production paths.
    """
    return sorted(
        p
        for p in root.rglob("*.rs")
        if p.is_file()
        and not {"tests", "benches"} & set(p.relative_to(root).parts)
        and not is_test_path(p.relative_to(REPO))
    )


def rel(path: Path) -> str:
    return str(path.relative_to(REPO))


def find_fn(text: str, name: str) -> str | None:
    """The body of `fn <name>`, by brace matching from its opening `{`."""
    match = re.search(rf"\bfn {re.escape(name)}\b", text)
    if match is None:
        return None
    start = text.find("{", match.end())
    if start == -1:
        return None
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return None


def check_dispatch_order(errors: list[str]) -> None:
    text = production_text(GATE)
    body = find_fn(text, "dispatch")
    if body is None:
        errors.append(
            f"{rel(GATE)}: `fn dispatch` not found — the script write seam's\n"
            "       chokepoint moved. Re-point this gate (scripts/script-write-seam.py)."
        )
        return
    admit = body.find("invoker.admit(")
    if admit == -1:
        errors.append(
            f"{rel(GATE)}: `ScriptCommandGate::dispatch` does not call `invoker.admit(`.\n"
            "       Every scripted sub-command must pass the shard write seam\n"
            "       (slot ownership + ACL + write admission) before it runs — that is\n"
            "       FM-TXN-051. Without it a script's runtime write outside its declared\n"
            "       key set reaches the store unchecked."
        )
        return
    for later in ("invoker.run_local(", "invoker.run_remote(", "mark_write("):
        position = body.find(later)
        if position != -1 and position < admit:
            errors.append(
                f"{rel(GATE)}: `{later}` runs before `invoker.admit(` in `dispatch`.\n"
                "       The seam decides before the command runs, and before the script is\n"
                "       marked write-dirty: a refused write must not take effect, and must\n"
                "       not buy the unkillable-script exemption a real write buys."
            )


def check_single_dispatch_path(errors: list[str]) -> None:
    for path in rust_files(SCRIPTING_DIR):
        if path == GATE:
            continue
        for number, line in enumerate(production_text(path).splitlines(), 1):
            if "invoker.run_local(" in line or "invoker.run_remote(" in line:
                errors.append(
                    f"{rel(path)}:{number}: scripted sub-command executed outside the gate:\n"
                    f"       {line.strip()}\n"
                    "       `ScriptCommandGate::dispatch` is the only place a `redis.call`\n"
                    "       is executed, because it is the only place the write seam is\n"
                    "       consulted. Route this through the gate."
                )


def check_seam_construction(errors: list[str]) -> None:
    for path in rust_files(CRATES):
        if rel(path) in SEAM_BUILDERS:
            continue
        for number, line in enumerate(production_text(path).splitlines(), 1):
            if "ShardWriteSeam::new(" in line:
                errors.append(
                    f"{rel(path)}:{number}: shard write seam assembled outside the shard:\n"
                    f"       {line.strip()}\n"
                    "       Build it with `ShardWorker::write_seam(admission)`, which reads\n"
                    "       the live cluster state, node id, quorum checker and replication\n"
                    "       tracker. A hand-assembled seam checks a stale or partial world."
                )


def check_seam_source(errors: list[str]) -> None:
    for path in rust_files(CRATES):
        lines = production_text(path).splitlines()
        for number, line in enumerate(lines, 1):
            if "write_seam = Some(" not in line:
                continue
            window = "\n".join(lines[max(0, number - 11) : number])
            if "self.write_seam(" not in window:
                errors.append(
                    f"{rel(path)}:{number}: script context given a seam from elsewhere:\n"
                    f"       {line.strip()}\n"
                    "       The seam handed to a script must come from\n"
                    "       `self.write_seam(admission)` on the shard worker."
                )


def check_bypass_pins(errors: list[str]) -> None:
    for constructor, allowed in BYPASS_PINS.items():
        seen: set[str] = set()
        for path in rust_files(CRATES):
            if rel(path) == "frogdb-server/crates/core/src/write_seam.rs":
                continue  # its own definition and doc-comments
            for number, line in enumerate(production_text(path).splitlines(), 1):
                if f"{constructor}(" not in line:
                    continue
                if rel(path) in allowed:
                    seen.add(rel(path))
                    continue
                errors.append(
                    f"{rel(path)}:{number}: unpinned write-seam bypass `{constructor}`:\n"
                    f"       {line.strip()}\n"
                    "       Bypassing the seam needs an argued reason recorded in\n"
                    "       scripts/script-write-seam.py (and, for a behavior change, a\n"
                    "       failure-mode row). Pinned sites: " + ", ".join(sorted(allowed))
                )
        for stale in sorted(allowed - seen):
            errors.append(
                f"{stale}: stale pin — `{constructor}` is no longer used here.\n"
                "       Drop the pin from scripts/script-write-seam.py so the bypass set\n"
                "       stays exactly the set of live bypasses."
            )


def check_message_admission(errors: list[str]) -> None:
    text = MESSAGE.read_text()
    for variant in ADMISSION_VARIANTS:
        match = re.search(rf"^\s+{variant} \{{$", text, re.MULTILINE)
        if match is None:
            errors.append(
                f"{rel(MESSAGE)}: shard-message variant `{variant}` not found — the pin in\n"
                "       scripts/script-write-seam.py is stale."
            )
            continue
        body_end = text.find("\n    },", match.end())
        body = text[match.end() : body_end if body_end != -1 else match.end()]
        if "admission:" not in body:
            errors.append(
                f"{rel(MESSAGE)}: `{variant}` carries no `admission:` field.\n"
                "       The seam's issuer-scoped inputs (ACL identity, the live\n"
                "       min-replicas-to-write) cannot be read on the shard, so every\n"
                "       message that produces writes the connection never gauntleted must\n"
                "       carry a `WriteAdmission`."
            )


def main() -> int:
    errors: list[str] = []
    check_dispatch_order(errors)
    check_single_dispatch_path(errors)
    check_seam_construction(errors)
    check_seam_source(errors)
    check_bypass_pins(errors)
    check_message_admission(errors)

    if errors:
        print("ERROR: script write-seam gate failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    pins = sum(len(files) for files in BYPASS_PINS.values())
    print(
        "OK: script writes reach the store only through ShardWriteSeam::admit "
        f"({len(ADMISSION_VARIANTS)} message variants carry an admission, "
        f"{pins} pinned bypass(es))"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
