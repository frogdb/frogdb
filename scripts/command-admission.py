#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: every command execution path reaches the admission chokepoint.

redis-feel issue 13. FrogDB executes a command from three places, and before
this gate each carried its own idea of what a command must clear first:

* `ShardWorker::execute_command_body` (`core/src/shard/execution.rs`) — plain
  dispatch and EXEC-queued commands.
* `ScriptCommandGate::dispatch` (`core/src/scripting/gate.rs`) — a script's
  `redis.call`, run on the shard hosting the Lua VM.
* `ShardWorker::execute_script_sub_command` (`core/src/shard/scripting.rs`) —
  the cross-shard continuation of one of those calls.

Only the first had a `maxmemory` gate, so a Lua script could run `SET` unbounded
while the instance sat over its limit under `noeviction`. The fix was one shared
policy, `crate::command_admission::admit_command`. A shared function is only a
chokepoint while every path still calls it — and adding a fourth executor that
forgets to is a one-line change that compiles. clippy can express neither "this
call only from these functions" nor "in this order", so a source-text check is
the honest tool.

## What is enforced

1. **One policy, pinned callers.** `admit_command(` appears only in the three
   execution paths (plus its own module). A fourth caller is either a new
   execution path — which must be pinned here deliberately — or admission logic
   leaking out of the chokepoint.
2. **One executor per path.** `handler.execute(` appears only in those same
   three files. A new `handler.execute(` elsewhere is an execution path that
   never met the policy.
3. **Admission comes first.** In each path, `admit_command(` precedes the thing
   it admits — `handler.execute(` for the two shard paths; `run_local` /
   `run_remote` / `mark_write(` for the script gate, where a refused command
   must also not leave the script write-dirty (a write-dirty script is
   deliberately unkillable).
4. **`DENYOOM` has one reader.** The `maxmemory` gate keys off
   `CommandFlags::DENYOOM`; a second production site testing that flag is a
   second admission policy by another name. Declaring the flag on a command spec
   is untouched — only `.contains(CommandFlags::DENYOOM)` is pinned.
5. **The script-start pre-admission survives.** `ShardWorker::run_script` must
   still sample the OOM state (`sample_oom_state(`) and consult the script's
   declared policy (`reject_at_start`), which is what rejects a may-write
   shebang script up front the way Redis's `scriptPrepareForRun` does.

Test code is out of scope — `#[cfg(test)]` spans (via `scripts/_rustscan.py`),
in-`src/` test modules, integration `tests/` dirs and benches are all skipped —
so tests may execute handlers and read flags freely.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _rustscan import cfg_test_spans, in_any_span, is_test_path  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
CRATES = REPO / "frogdb-server" / "crates"

ADMISSION = "frogdb-server/crates/core/src/command_admission.rs"
EXECUTION = "frogdb-server/crates/core/src/shard/execution.rs"
GATE = "frogdb-server/crates/core/src/scripting/gate.rs"
SUB_COMMAND = "frogdb-server/crates/core/src/shard/scripting.rs"

# The execution paths allowed to consult the policy directly.
ADMISSION_CALLERS = {ADMISSION, EXECUTION, GATE, SUB_COMMAND}

# The execution paths allowed to run a command handler.
EXECUTORS = {EXECUTION, GATE, SUB_COMMAND}

# path -> (function, [things that must not precede admission])
ORDERING = {
    EXECUTION: ("execute_command_body", ["handler.execute("]),
    SUB_COMMAND: ("execute_script_sub_command", ["handler.execute("]),
    GATE: ("dispatch", ["invoker.run_local(", "invoker.run_remote(", "mark_write("]),
}

# The one production site that reads the flag the `maxmemory` gate keys off.
DENYOOM_READER = ADMISSION
DENYOOM_READ = ".contains(CommandFlags::DENYOOM)"

# Script-start pre-admission: what `run_script` must still do.
PRE_ADMISSION = ("sample_oom_state(", "reject_at_start")


def production_text(path: Path) -> str:
    """File text with every `#[cfg(test)]` item blanked out (line numbers kept)."""
    lines = path.read_text().splitlines()
    spans = cfg_test_spans(lines)
    return "\n".join("" if in_any_span(index, spans) else line for index, line in enumerate(lines))


def rust_files(root: Path) -> list[Path]:
    """Production sources under `root` (no test modules, `tests/` dirs or benches)."""
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


def check_pinned_sites(errors: list[str], needle: str, allowed: set[str], why: str) -> None:
    """`needle` appears only in `allowed`; every pin is still live."""
    seen: set[str] = set()
    for path in rust_files(CRATES):
        for number, line in enumerate(production_text(path).splitlines(), 1):
            if needle not in line:
                continue
            if rel(path) in allowed:
                seen.add(rel(path))
                continue
            errors.append(f"{rel(path)}:{number}: {why}\n       {line.strip()}")
    for stale in sorted(allowed - seen):
        errors.append(
            f"{stale}: stale pin — `{needle}` is no longer used here.\n"
            "       Drop the pin from scripts/command-admission.py so the pinned set stays\n"
            "       exactly the set of live sites."
        )


def check_admission_callers(errors: list[str]) -> None:
    check_pinned_sites(
        errors,
        "admit_command(",
        ADMISSION_CALLERS,
        "admission policy consulted outside a pinned execution path:",
    )


def check_executors(errors: list[str]) -> None:
    check_pinned_sites(
        errors,
        "handler.execute(",
        EXECUTORS,
        "command executed outside a pinned execution path:\n"
        "       Every execution path must reach `command_admission::admit_command`\n"
        "       first (redis-feel issue 13). Route this through one of the pinned\n"
        "       paths, or add it here together with its admission call.",
    )


def check_ordering(errors: list[str]) -> None:
    for relative, (function, laters) in ORDERING.items():
        path = REPO / relative
        body = find_fn(production_text(path), function)
        if body is None:
            errors.append(
                f"{relative}: `fn {function}` not found — an execution path moved.\n"
                "       Re-point this gate (scripts/command-admission.py)."
            )
            continue
        admit = body.find("admit_command(")
        if admit == -1:
            errors.append(
                f"{relative}: `{function}` does not call `admit_command(`.\n"
                "       This is an execution path: it must consult the one admission\n"
                "       policy (`crate::command_admission`) before it runs a command.\n"
                "       Skipping it is how a scripted `redis.call` used to run `DENYOOM`\n"
                "       commands unbounded while the instance sat over `maxmemory`."
            )
            continue
        for later in laters:
            position = body.find(later)
            if position != -1 and position < admit:
                errors.append(
                    f"{relative}: `{later}` runs before `admit_command(` in `{function}`.\n"
                    "       Admission decides before the command runs — and, in the script\n"
                    "       gate, before the script is marked write-dirty: a refused command\n"
                    "       must neither take effect nor buy the unkillable-script exemption."
                )


def check_denyoom_reader(errors: list[str]) -> None:
    check_pinned_sites(
        errors,
        DENYOOM_READ,
        {DENYOOM_READER},
        "`DENYOOM` read outside the admission chokepoint:\n"
        "       The `maxmemory` gate is decided once, in `command_admission`. A second\n"
        "       reader is a second admission policy that will drift from it.",
    )


def check_pre_admission(errors: list[str]) -> None:
    path = REPO / SUB_COMMAND
    body = find_fn(production_text(path), "run_script")
    if body is None:
        errors.append(
            f"{SUB_COMMAND}: `fn run_script` not found — the script-start admission moved.\n"
            "       Re-point this gate (scripts/command-admission.py)."
        )
        return
    for needle in PRE_ADMISSION:
        if needle not in body:
            errors.append(
                f"{SUB_COMMAND}: `run_script` no longer uses `{needle}`.\n"
                "       A script that may write is admitted once, up front, against the\n"
                "       memory state sampled at script start (Redis's `scriptPrepareForRun`\n"
                "       / `server.pre_command_oom_state`). Losing either half turns that\n"
                "       rejection off silently."
            )


def main() -> int:
    errors: list[str] = []
    check_admission_callers(errors)
    check_executors(errors)
    check_ordering(errors)
    check_denyoom_reader(errors)
    check_pre_admission(errors)

    if errors:
        print("ERROR: command admission gate failed:", file=sys.stderr)
        for error in errors:
            print(f"  {error}", file=sys.stderr)
        return 1

    print(
        "OK: every command execution path reaches command_admission::admit_command "
        f"({len(EXECUTORS)} executors, {len(ORDERING)} ordering checks, "
        "1 DENYOOM reader, script-start pre-admission intact)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
