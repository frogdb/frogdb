#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Fixture tests for scripts/lint-manifest-dir.py.

Run: ./scripts/tests/test_lint_manifest_dir.py  (or `just test-lint-manifest-dir`)

The gate is a grep over `git ls-files '*.rs'`, so each case builds a throwaway
git repo containing a helper file and (sometimes) a violating file, points the
script's ROOT at it, and checks the verdict. Covered: a clean tree passes, a
bare `env!("CARGO_MANIFEST_DIR")` outside the helper fails, and a count that
does not match `EXPECTED_IN_HELPER` fails even when no file violates the
forward rule.

No test framework: the seam-lint scripts are pure-stdlib `uv run --script`, so
this stays a dependency-free assert script that exits nonzero on first failure.
"""

from __future__ import annotations

import importlib.util
import io
import subprocess
import sys
import tempfile
from contextlib import redirect_stderr
from pathlib import Path

# lint-manifest-dir.py has hyphens, so it is not importable by name; load by path.
_SCRIPT = Path(__file__).resolve().parent.parent / "lint-manifest-dir.py"
_spec = importlib.util.spec_from_file_location("lint_manifest_dir", _SCRIPT)
assert _spec and _spec.loader
lmd = importlib.util.module_from_spec(_spec)
sys.modules["lint_manifest_dir"] = lmd
_spec.loader.exec_module(lmd)

ENV = 'env!("CARGO_MANIFEST_DIR")'

# A helper file with exactly EXPECTED_IN_HELPER occurrences, shaped like the real one.
HELPER_BODY = f"""\
//! Prose may mention {ENV} freely — comments are not uses.
const TYPES_MANIFEST_DIR: &str = {ENV};

#[macro_export]
macro_rules! manifest_dir {{
    () => {{
        $crate::manifest_dir::resolve({ENV})
    }};
}}
"""


def run_gate(root: Path) -> tuple[int, str]:
    """Run the gate with ROOT pointed at `root`; return (exit code, stderr)."""
    original = lmd.ROOT
    lmd.ROOT = root
    err = io.StringIO()
    try:
        with redirect_stderr(err):
            status = lmd.main()
    finally:
        lmd.ROOT = original
    return status, err.getvalue()


def make_tree(files: dict[str, str]) -> Path:
    """A git repo with `files` tracked (the gate sweeps `git ls-files`)."""
    root = Path(tempfile.mkdtemp(prefix="lint-manifest-dir-"))
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    for rel, body in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    return root


def test_clean_tree_passes() -> None:
    root = make_tree(
        {
            lmd.HELPER: HELPER_BODY,
            "frogdb-server/crates/cluster/src/golden.rs": (
                'fn dir() -> PathBuf { frogdb_types::manifest_dir!().join("testdata") }\n'
            ),
        }
    )
    status, err = run_gate(root)
    assert status == 0, f"clean tree should pass, got {status}: {err}"
    assert err == "", f"clean tree should be silent, got: {err}"


def test_violating_file_fails() -> None:
    root = make_tree(
        {
            lmd.HELPER: HELPER_BODY,
            "frogdb-server/crates/cluster/src/golden.rs": (
                "// a comment mentioning " + ENV + " is fine\n"
                f'fn dir() -> PathBuf {{ Path::new({ENV}).join("testdata") }}\n'
            ),
        }
    )
    status, err = run_gate(root)
    assert status == 1, f"violation should fail, got {status}: {err}"
    assert "frogdb-server/crates/cluster/src/golden.rs:2:" in err, err
    assert "use frogdb_types::manifest_dir!()" in err, err
    # The comment on line 1 must not be reported.
    assert "golden.rs:1:" not in err, err


def test_option_env_also_fails() -> None:
    root = make_tree(
        {
            lmd.HELPER: HELPER_BODY,
            "frogdb-server/crates/cluster/src/golden.rs": (
                'fn dir() -> Option<&\'static str> { option_env!("CARGO_MANIFEST_DIR") }\n'
            ),
        }
    )
    status, err = run_gate(root)
    assert status == 1, f"option_env! should fail, got {status}: {err}"
    assert "golden.rs:1:" in err, err


def test_count_pin_mismatch_fails() -> None:
    # A stray third copy inside the helper: no file violates the forward rule,
    # so only the count pin can catch it.
    root = make_tree({lmd.HELPER: HELPER_BODY + f"const STRAY: &str = {ENV};\n"})
    status, err = run_gate(root)
    assert status == 1, f"count mismatch should fail, got {status}: {err}"
    assert f"expected {lmd.EXPECTED_IN_HELPER} CARGO_MANIFEST_DIR occurrence(s)" in err, err
    assert "found 3" in err, err


def test_count_pin_underrun_fails() -> None:
    # The macro reworded out of the pattern's sight.
    root = make_tree({lmd.HELPER: f"const TYPES_MANIFEST_DIR: &str = {ENV};\n"})
    status, err = run_gate(root)
    assert status == 1, f"count underrun should fail, got {status}: {err}"
    assert "found 1" in err, err


def test_real_tree_is_clean() -> None:
    # The gate must pass on the repo it ships in.
    status, err = run_gate(lmd.ROOT)
    assert status == 0, f"the repo itself should pass the gate: {err}"


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"ok  {test.__name__}")
    print(f"\n{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
