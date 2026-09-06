#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: `CARGO_MANIFEST_DIR` is read through `frogdb_types::manifest_dir!()`.

`env!("CARGO_MANIFEST_DIR")` is baked in by `rustc` at compile time. That is
what a test wants when it needs a CWD-independent path — golden fixtures,
repro dumps, source sweeps — right up until a `target/` built in one checkout
is reused by another. FrogDB does exactly that: a worktree's `target/` is
seeded from another checkout's (`.scratch/build-cache/README.md`), so a
fingerprint-fresh test binary can be *executed* from worktree B while carrying
worktree A's manifest dir inside it — reading golden files from, and writing
repro files into, the wrong tree.

`frogdb_types::manifest_dir!()` expands to the same `env!` at the call site and
then rebases it onto the workspace root of the checkout the process is actually
running in. This gate makes that the only way in: the compile-time literal may
appear only in the helper module that defines the macro.

Two things are pinned, in both directions:

* **Forward** — any `env!`/`option_env!("CARGO_MANIFEST_DIR")` outside
  `HELPER` fails, at `file:line`.
* **Count** — the helper file itself must contain exactly `EXPECTED_IN_HELPER`
  occurrences (the macro body, and the anchor the compile-time workspace root
  is derived from). A third copy inside the helper is a stray that the forward
  rule alone would wave through; a drop below means the macro or the anchor was
  reworded out of the pattern and the gate is quietly checking nothing.

Grep only: no compile, so it runs in `lint-gates` on every commit.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The one file allowed to spell the compile-time value: the macro body (which
# uses `$crate`, so the literal sits here and expands at the caller) plus the
# anchor `resolve` derives the compile-time workspace root from.
HELPER = "frogdb-server/crates/types/src/manifest_dir.rs"
EXPECTED_IN_HELPER = 2

# `env!` and `option_env!`, however the argument is spaced or quoted.
MANIFEST_ENV = re.compile(r"\b(?:option_)?env!\s*\(\s*\"CARGO_MANIFEST_DIR\"\s*,?\s*")

HINT = "use frogdb_types::manifest_dir!() — see scripts/lint-manifest-dir.py"


def tracked_rust_files() -> list[str]:
    """Every git-tracked .rs file, repo-root-relative. Skips .gitignore'd trees
    (target/, .claude/worktrees/, ...) for free."""
    out = subprocess.run(
        ["git", "ls-files", "*.rs"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    return out.stdout.splitlines()


def main() -> int:
    violations: list[tuple[str, int, str]] = []  # (file, line, text)
    in_helper = 0

    for rel in tracked_rust_files():
        path = ROOT / rel
        try:
            text = path.read_text(errors="strict")
        except (UnicodeDecodeError, OSError):
            continue
        if "CARGO_MANIFEST_DIR" not in text:
            continue  # cheap prefilter before the per-line pass
        for lineno, line in enumerate(text.splitlines(), start=1):
            if line.lstrip().startswith("//"):
                continue  # prose about the rule is not a use of it
            if not MANIFEST_ENV.search(line):
                continue
            if rel == HELPER:
                in_helper += 1
            else:
                violations.append((rel, lineno, line.strip()))

    status = 0

    if violations:
        print(
            "ERROR: compile-time CARGO_MANIFEST_DIR read outside the helper:",
            file=sys.stderr,
        )
        for rel, lineno, text in violations:
            print(f"  {rel}:{lineno}: {text}", file=sys.stderr)
        print(file=sys.stderr)
        print(f"       {HINT}", file=sys.stderr)
        status = 1

    if in_helper != EXPECTED_IN_HELPER:
        print(
            f"ERROR: expected {EXPECTED_IN_HELPER} CARGO_MANIFEST_DIR occurrence(s) in "
            f"{HELPER}, found {in_helper}.",
            file=sys.stderr,
        )
        print(file=sys.stderr)
        print(
            "       More means a stray copy landed inside the helper; fewer means the",
            file=sys.stderr,
        )
        print(
            "       macro body or the compile-time root anchor was reworded out of the",
            file=sys.stderr,
        )
        print(
            "       gate's sight. If the change is real, move EXPECTED_IN_HELPER in",
            file=sys.stderr,
        )
        print("       scripts/lint-manifest-dir.py.", file=sys.stderr)
        status = 1

    return status


if __name__ == "__main__":
    sys.exit(main())
