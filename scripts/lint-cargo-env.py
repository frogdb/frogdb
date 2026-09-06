#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Gate: every cargo invocation in the build tooling carries the RocksDB prelude.

Without `ROCKSDB_LIB_DIR`, the `rocksdb` crate's build script does not link the
system library — it compiles the *vendored* RocksDB from source. That is ~10
minutes of CPU on a fresh worktree and ~1.5 GB of `.o` files that then sit in
`target/` forever, and it happens on the *first* cargo invocation, so a single
un-prefixed recipe poisons the whole worktree for every later command. The
same holds for `LIBCLANG_PATH` (bindgen) and, on macOS, `DYLD_LIBRARY_PATH`.
Background: `.scratch/build-cache/README.md`.

The prelude is therefore not something a caller spells out — it has exactly two
chokepoints, one per language, and this gate is what keeps a third from
appearing:

* **Justfile** — `{{rocksdb-env}}` (normally next to `{{dyld-env}}`) on the same
  line as `cargo`, or the private `_cargo` recipe invoked as a dependency call
  (`docs-gen: (_cargo "run -p docs-gen")`), which spells the prelude once.
* **`scripts/*.py`** — `cargo_env()` from `scripts/cargo_env.py`, passed as the
  subprocess `env`. A script that builds a cargo command line must import it.

Three rules, all `grep`/`ast` over source text (no compile step, whole run well
under a second):

1. A Justfile line invoking `cargo <subcmd>` needs `{{rocksdb-env}}`, unless it
   is inside `_cargo`'s own body, is a recipe header delegating to `(_cargo …)`,
   or its subcommand never compiles the workspace (`ALLOWED_SUBCOMMANDS`).
2. A `scripts/*.py` module that builds a `["cargo", …]` command line must import
   `cargo_env`; and no script other than `cargo_env.py` may set the prelude
   variables itself — a second copy of the prelude is precisely the drift being
   prevented (the copy is what goes stale when the real one moves).
3. A `scripts/*.sh` line invoking `cargo <subcmd>` fails outright: there is no
   shell prelude helper, and adding one would be that third chokepoint. A shell
   script that needs cargo should be reached through a `just` recipe.

Scope is `Justfile` plus `scripts/`, over `git ls-files` — the tooling that
actually runs cargo. Rust sources, docs and generated workflow YAML are out of
scope (the CI images build vendored RocksDB deliberately).
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path
from typing import NamedTuple

ROOT = Path(__file__).resolve().parent.parent

# `cargo [+toolchain] <subcommand>`. The toolchain token is optional; the
# subcommand is what decides whether the invocation can reach the build script.
CARGO_CALL = re.compile(r"\bcargo\s+(?:\+\S+\s+)?(\w[\w-]*)")

# Subcommands that never compile the workspace, so they can never trigger a
# vendored-RocksDB build:
#   install/binstall  — build a *tool* from crates.io, not this workspace
#   sweep/clean       — delete build artifacts
#   fmt               — rustfmt, no codegen
#   deny              — reads Cargo.lock
#   zigbuild          — the Linux cross-builds, where vendored RocksDB is the
#                       intended outcome (no system copy in the sysroot)
ALLOWED_SUBCOMMANDS = frozenset(
    {"install", "binstall", "sweep", "clean", "fmt", "deny", "zigbuild"}
)

# `cargo fuzz` *does* compile (the `fuzz` recipe carries the prelude), but
# `fuzz list` only reads testing/fuzz/Cargo.toml. Allowlisted as an exact shape,
# never as a bare `fuzz` subcommand.
FUZZ_LIST = re.compile(r"\bcargo\s+\+\S+\s+fuzz\s+list\b")

ROCKSDB_ENV = "{{rocksdb-env}}"
CARGO_DEP_CALL = re.compile(r"\(_cargo\s")
# `_cargo *args:` — the one recipe allowed to say `cargo` without the variable
# on the same line (its body spells the prelude out).
CARGO_RECIPE_HEADER = re.compile(r"^_cargo\b[^\n]*:")

# The prelude itself. Owned by scripts/cargo_env.py; a second copy anywhere else
# is the drift this gate exists to stop.
PRELUDE_VARS = frozenset(
    {"ROCKSDB_LIB_DIR", "SNAPPY_LIB_DIR", "LIBCLANG_PATH", "DYLD_LIBRARY_PATH"}
)

# repo-relative paths exempt from the rules they would otherwise trip:
CARGO_ENV_MODULE = "scripts/cargo_env.py"  # the chokepoint itself
PRELUDE_EXEMPT = frozenset({CARGO_ENV_MODULE, "scripts/tests/test_cargo_env.py"})


class Violation(NamedTuple):
    """One offending line, with the remediation category it belongs to."""

    kind: str  # "justfile" | "python-cargo" | "python-prelude" | "shell"
    file: str
    line: int
    text: str


def cargo_calls_missing_env(line: str) -> bool:
    """True if `line` invokes cargo in a way that can compile the workspace."""
    for match in CARGO_CALL.finditer(line):
        if match.group(1) in ALLOWED_SUBCOMMANDS:
            continue
        if FUZZ_LIST.match(line, match.start()):
            continue
        return True
    return False


def check_justfile(text: str, rel: str = "Justfile") -> list[Violation]:
    """Every cargo line carries `{{rocksdb-env}}` or goes through `_cargo`."""
    violations: list[Violation] = []
    in_cargo_recipe = False

    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if CARGO_RECIPE_HEADER.match(line):
            in_cargo_recipe = True
            continue
        if in_cargo_recipe and stripped and not line[0].isspace():
            in_cargo_recipe = False  # dedent ends the recipe body

        if stripped.startswith("#"):
            continue  # prose describing an invocation isn't one
        if in_cargo_recipe or ROCKSDB_ENV in line or CARGO_DEP_CALL.search(line):
            continue
        if cargo_calls_missing_env(line):
            violations.append(Violation("justfile", rel, lineno, stripped))

    return violations


def cargo_command_literals(tree: ast.Module) -> list[int]:
    """Line numbers of `["cargo", ...]` / `("cargo", ...)` command literals.

    Working off the AST rather than the raw text is what keeps the docstrings
    and comments that merely *mention* `cargo check` (mutants-gate.py,
    gen-command-metadata.py, ship-cmd-full.py) from reading as invocations.
    """
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.List | ast.Tuple)
        and node.elts
        and isinstance(node.elts[0], ast.Constant)
        and node.elts[0].value == "cargo"
    ]


def imports_cargo_env(tree: ast.Module) -> bool:
    """True if the module has `cargo_env` in scope (`from cargo_env import ...`)."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "cargo_env":
            return True
        if isinstance(node, ast.Import) and any(a.name == "cargo_env" for a in node.names):
            return True
    return False


def prelude_assignments(tree: ast.Module) -> list[tuple[int, str]]:
    """(line, var) for each place the module sets a prelude variable itself.

    Covers the three shapes a second copy would take: `env.setdefault("VAR", …)`,
    `env["VAR"] = …`, and a `{"VAR": …}` literal.
    """
    found: list[tuple[int, str]] = []

    def name_of(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and node.value in PRELUDE_VARS:
            return str(node.value)
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Attribute) and func.attr == "setdefault" and node.args:
                var = name_of(node.args[0])
                if var:
                    found.append((node.lineno, var))
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    var = name_of(target.slice)
                    if var:
                        found.append((target.lineno, var))
        elif isinstance(node, ast.Dict):
            for key in node.keys:
                var = name_of(key) if key is not None else None
                if var:
                    found.append((key.lineno, var))  # type: ignore[union-attr]

    return found


def check_python(rel: str, text: str) -> list[Violation]:
    """Cargo command lines go through `cargo_env()`; nobody re-spells the prelude."""
    violations: list[Violation] = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return violations  # not our gate's business to report

    lines = text.splitlines()

    def source(lineno: int) -> str:
        return lines[lineno - 1].strip() if 0 < lineno <= len(lines) else ""

    is_fixture = rel.startswith("scripts/tests/")
    if not is_fixture and rel != CARGO_ENV_MODULE and not imports_cargo_env(tree):
        for lineno in cargo_command_literals(tree):
            violations.append(Violation("python-cargo", rel, lineno, source(lineno)))

    if rel not in PRELUDE_EXEMPT:
        for lineno, var in prelude_assignments(tree):
            violations.append(Violation("python-prelude", rel, lineno, f"{var}: {source(lineno)}"))

    return violations


def check_shell(rel: str, text: str) -> list[Violation]:
    """No cargo in shell scripts at all — there is no shell prelude helper."""
    violations: list[Violation] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if cargo_calls_missing_env(stripped):
            violations.append(Violation("shell", rel, lineno, stripped))
    return violations


def tracked_files() -> list[str]:
    """Every git-tracked file, repo-root-relative (skips target/, worktrees, ...)."""
    out = subprocess.run(["git", "ls-files"], cwd=ROOT, capture_output=True, text=True, check=True)
    return out.stdout.splitlines()


REMEDIATION = {
    "justfile": [
        "ERROR: a Justfile cargo invocation is missing the RocksDB env prelude:",
        "       Prefix the line with `{{dyld-env}} {{rocksdb-env}}`, or route the recipe",
        "       through the `_cargo` chokepoint as a dependency call:",
        '       `my-recipe *args: (_cargo "run -p my-crate --" args)`.',
    ],
    "python-cargo": [
        "ERROR: a script builds a cargo command line without the shared env:",
        "       Add `from cargo_env import cargo_env` and pass `env=cargo_env()` to the",
        "       subprocess call (scripts/cargo_env.py is the one Python chokepoint).",
    ],
    "python-prelude": [
        "ERROR: a script sets the cargo build vars itself:",
        "       These belong to scripts/cargo_env.py alone — a second copy is what goes",
        "       stale when the real prelude moves. Call `cargo_env()` instead.",
    ],
    "shell": [
        "ERROR: a shell script invokes cargo:",
        "       There is no shell prelude helper, and a third chokepoint is not the fix:",
        "       reach cargo through a `just` recipe that carries `{{rocksdb-env}}`.",
    ],
}


def main() -> int:
    violations: list[Violation] = []

    for rel in tracked_files():
        if rel != "Justfile" and not rel.startswith("scripts/"):
            continue
        try:
            text = (ROOT / rel).read_text(errors="strict")
        except (UnicodeDecodeError, OSError):
            continue
        if rel == "Justfile":
            violations.extend(check_justfile(text, rel))
        elif rel.endswith(".py"):
            violations.extend(check_python(rel, text))
        elif rel.endswith(".sh"):
            violations.extend(check_shell(rel, text))

    if not violations:
        return 0

    for kind, message in REMEDIATION.items():
        hits = [v for v in violations if v.kind == kind]
        if not hits:
            continue
        print(message[0], file=sys.stderr)
        for v in hits:
            print(f"  {v.file}:{v.line}: {v.text}", file=sys.stderr)
        for line in message[1:]:
            print(line, file=sys.stderr)
        print(file=sys.stderr)

    print(
        "       Without ROCKSDB_LIB_DIR the first cargo call in a fresh worktree builds",
        file=sys.stderr,
    )
    print(
        "       vendored RocksDB from source: ~10 min CPU and 1.5 GB that stays in target/.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
