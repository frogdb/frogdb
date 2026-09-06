#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Fixture tests for scripts/lint-cargo-env.py, in both directions.

Run: ./scripts/tests/test_lint_cargo_env.py   (or `just test-lint-cargo-env`)

A green tree exercises only the passing direction — every Justfile recipe,
script and shell file currently carries the prelude — so the *failing*
directions (a bare `cargo build`, a script that builds a cargo command line
without `cargo_env()`, a second copy of the prelude, cargo in a shell script)
are pinned here against inline fixtures instead. Each escape hatch
(`{{rocksdb-env}}`, `(_cargo "...")`, the non-compiling subcommands, comments)
is pinned too, since an over-eager gate gets disabled rather than fixed.

No test framework: the seam-lint family is pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on failure.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# lint-cargo-env.py has a hyphen, so it is not importable by name; load by path.
_SCRIPT = Path(__file__).resolve().parent.parent / "lint-cargo-env.py"
_spec = importlib.util.spec_from_file_location("lint_cargo_env", _SCRIPT)
assert _spec and _spec.loader
sys.path.insert(0, str(_SCRIPT.parent))
gate = importlib.util.module_from_spec(_spec)
sys.modules["lint_cargo_env"] = gate
_spec.loader.exec_module(gate)

REPO = _SCRIPT.parent.parent


def kinds(violations: list) -> list[str]:
    return [v.kind for v in violations]


# --------------------------------------------------------------------------
# Justfile
# --------------------------------------------------------------------------


def test_justfile_prefixed_line_passes() -> None:
    text = "check:\n    {{dyld-env}} {{rocksdb-env}} cargo check --all-targets\n"
    assert gate.check_justfile(text) == [], gate.check_justfile(text)


def test_justfile_bare_cargo_fails() -> None:
    text = "build:\n    cargo build\n"
    found = gate.check_justfile(text)
    assert len(found) == 1, found
    assert found[0].line == 2, found
    assert found[0].kind == "justfile", found


def test_justfile_env_missing_on_only_one_line() -> None:
    """One naked line among prefixed neighbours is reported at its own line."""
    text = (
        "a:\n"
        "    {{dyld-env}} {{rocksdb-env}} cargo nextest run --all\n"
        "\n"
        "b:\n"
        "    cargo nextest run -p frogdb-core\n"
    )
    found = gate.check_justfile(text)
    assert [v.line for v in found] == [5], found


def test_justfile_cargo_dependency_call_passes() -> None:
    text = 'admin *args: (_cargo "run -p frogdb-admin --" args)\n'
    assert gate.check_justfile(text) == [], gate.check_justfile(text)


def test_justfile_cargo_recipe_body_passes() -> None:
    """`_cargo`'s own body is the one place the prelude is spelled out."""
    text = "_cargo *args:\n    {{dyld-env}} {{rocksdb-env}} cargo {{args}}\n"
    assert gate.check_justfile(text) == [], gate.check_justfile(text)
    # ... and the exemption ends at the dedent.
    text += "\nbuild:\n    cargo build\n"
    found = gate.check_justfile(text)
    assert [v.line for v in found] == [5], found


def test_justfile_non_compiling_subcommands_pass() -> None:
    for line in (
        "    cargo install cargo-zigbuild",
        "    cargo binstall cargo-nextest --secure",
        "    -cargo sweep --time 0",
        "    cargo clean",
        "    cargo fmt --all",
        "    cargo deny check --config deny.toml",
        "    cargo zigbuild --release --target x86_64-unknown-linux-gnu",
    ):
        found = gate.check_justfile(f"r:\n{line}\n")
        assert found == [], f"{line!r} should pass: {found}"


def test_justfile_fuzz_list_passes_but_fuzz_run_fails() -> None:
    listing = (
        'r:\n    targets=$(RUSTC_WRAPPER="" cargo +nightly fuzz list --fuzz-dir testing/fuzz)\n'
    )
    assert gate.check_justfile(listing) == [], gate.check_justfile(listing)
    running = "r:\n    cargo +nightly fuzz run {{target}} --fuzz-dir testing/fuzz\n"
    found = gate.check_justfile(running)
    assert len(found) == 1, found


def test_justfile_comment_passes() -> None:
    text = (
        "# Watch for changes and run tests (requires: cargo install cargo-watch)\n# cargo build\n"
    )
    assert gate.check_justfile(text) == [], gate.check_justfile(text)


def test_justfile_nested_cargo_counts_once_when_prefixed() -> None:
    """`cargo watch -s 'cargo nextest run'` — the outer call carries the env."""
    text = (
        "watch-test:\n    {{dyld-env}} {{rocksdb-env}} cargo watch -s 'cargo nextest run --all'\n"
    )
    assert gate.check_justfile(text) == [], gate.check_justfile(text)


def test_real_justfile_is_clean() -> None:
    """The shipped Justfile passes — guards the `_cargo` routing already done."""
    found = gate.check_justfile((REPO / "Justfile").read_text())
    assert found == [], found


# --------------------------------------------------------------------------
# scripts/*.py
# --------------------------------------------------------------------------

CARGO_CALL_SOURCE = """
import subprocess

def run():
    subprocess.run(["cargo", "check", "-p", "frogdb-core"], check=True)
"""


def test_python_cargo_call_without_import_fails() -> None:
    found = gate.check_python("scripts/thing.py", CARGO_CALL_SOURCE)
    assert kinds(found) == ["python-cargo"], found
    assert found[0].line == 5, found


def test_python_cargo_call_with_import_passes() -> None:
    src = "from cargo_env import cargo_env\n" + CARGO_CALL_SOURCE
    assert gate.check_python("scripts/thing.py", src) == []


def test_python_bound_cargo_list_without_import_fails() -> None:
    src = 'check_cmd = ["cargo", "check", "-p", "x", "--all-targets"]\n'
    assert kinds(gate.check_python("scripts/thing.py", src)) == ["python-cargo"]


def test_python_docstring_mention_passes() -> None:
    src = '''"""Times `cargo check --all-targets` and `cargo nextest list`."""

# also mentioned in a comment: cargo build --release
NOTE = "run cargo check first"
'''
    assert gate.check_python("scripts/thing.py", src) == []


def test_python_string_containment_check_passes() -> None:
    """ship-cmd-full.py's `if "cargo" not in text` is not a command line."""
    src = 'def f(text):\n    if "cargo" not in text:\n        return None\n'
    assert gate.check_python("scripts/ship-cmd-full.py", src) == []


def test_python_prelude_copy_fails() -> None:
    for src in (
        'env.setdefault("ROCKSDB_LIB_DIR", "/opt/homebrew/lib")\n',
        'env["LIBCLANG_PATH"] = "/opt/homebrew/opt/llvm/lib"\n',
        'env = {"DYLD_LIBRARY_PATH": "/opt/homebrew/opt/llvm/lib"}\n',
        'env.setdefault("SNAPPY_LIB_DIR", lib_dir)\n',
    ):
        found = gate.check_python("scripts/thing.py", src)
        assert kinds(found) == ["python-prelude"], f"{src!r}: {found}"


def test_python_prelude_allowed_in_chokepoint_and_its_test() -> None:
    src = 'env.setdefault("ROCKSDB_LIB_DIR", lib_dir)\n'
    assert gate.check_python("scripts/cargo_env.py", src) == []
    assert gate.check_python("scripts/tests/test_cargo_env.py", src) == []


def test_python_fixture_dir_skips_cargo_rule_but_not_prelude() -> None:
    """scripts/tests/ builds fixtures, not real cargo calls — but no prelude copies."""
    assert gate.check_python("scripts/tests/test_x.py", CARGO_CALL_SOURCE) == []
    src = 'os.environ["ROCKSDB_LIB_DIR"] = "/tmp/lib"\n'
    assert kinds(gate.check_python("scripts/tests/test_x.py", src)) == ["python-prelude"]


def test_python_syntax_error_is_not_this_gates_business() -> None:
    assert gate.check_python("scripts/thing.py", "def broken(:\n") == []


# --------------------------------------------------------------------------
# scripts/*.sh
# --------------------------------------------------------------------------


def test_shell_cargo_fails() -> None:
    found = gate.check_shell("scripts/x.sh", "#!/bin/bash\ncargo build --release\n")
    assert kinds(found) == ["shell"], found
    assert found[0].line == 2, found


def test_shell_non_compiling_subcommand_and_comments_pass() -> None:
    text = "#!/bin/bash\n# Invoked by cargo/nextest as: cargo nextest run\ncargo sweep --time 7\n"
    assert gate.check_shell("scripts/x.sh", text) == [], gate.check_shell("scripts/x.sh", text)


def test_real_cov_runner_is_clean() -> None:
    """cov-runner.sh is a nextest *runner*, not a cargo caller."""
    path = REPO / "scripts" / "cov-runner.sh"
    found = gate.check_shell("scripts/cov-runner.sh", path.read_text())
    assert found == [], found


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failures = 0
    for t in tests:
        try:
            t()
            print(f"  PASS {t.__name__}")
        except AssertionError as e:
            failures += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(tests) - failures}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
