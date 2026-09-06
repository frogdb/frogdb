#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Tests for scripts/cargo_env.py, the shared cargo build environment.

Run: ./scripts/tests/test_cargo_env.py   (or `just test-cargo-env`)

The stakes are a ~10-minute, 1.5 GB vendored RocksDB build: if `ROCKSDB_LIB_DIR`
goes missing from a script's cargo environment, `librocksdb-sys` silently
compiles RocksDB from source instead of linking the system copy. These pin the
four vars the Justfile prelude sets, the `FROGDB_SYSTEM_ROCKSDB=""` (vendored)
opt-out, and the two pass-through rules: an explicit caller setting wins, and
`RUSTC_WRAPPER` (sccache) is never touched.

No test framework: the scripts here are pure-stdlib `uv run --script`, so this
stays a dependency-free assert script that exits nonzero on the first failure.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from cargo_env import cargo_env  # noqa: E402

LLVM = "/opt/homebrew/opt/llvm/lib"
LIBS = "/opt/homebrew/lib"


def test_darwin_defaults() -> None:
    env = cargo_env({}, platform="darwin")
    assert env["LIBCLANG_PATH"] == LLVM, env
    assert env["DYLD_LIBRARY_PATH"] == LLVM, env
    assert env["ROCKSDB_LIB_DIR"] == LIBS, env
    assert env["SNAPPY_LIB_DIR"] == LIBS, env


def test_linux_defaults() -> None:
    """Linux: platform libclang, no DYLD_*, and vendored RocksDB by default."""
    env = cargo_env({}, platform="linux")
    assert env["LIBCLANG_PATH"] == "/usr/lib/llvm-18/lib", env
    assert "DYLD_LIBRARY_PATH" not in env, env
    assert "ROCKSDB_LIB_DIR" not in env, env
    assert "SNAPPY_LIB_DIR" not in env, env


def test_system_rocksdb_opt_out() -> None:
    """An empty FROGDB_SYSTEM_ROCKSDB means vendored, even on macOS."""
    env = cargo_env({"FROGDB_SYSTEM_ROCKSDB": ""}, platform="darwin")
    assert "ROCKSDB_LIB_DIR" not in env, env
    assert "SNAPPY_LIB_DIR" not in env, env
    # The libclang vars are unconditional, so bindgen still works.
    assert env["LIBCLANG_PATH"] == LLVM, env


def test_system_rocksdb_opt_in_on_linux() -> None:
    env = cargo_env({"FROGDB_SYSTEM_ROCKSDB": "1"}, platform="linux")
    assert env["ROCKSDB_LIB_DIR"] == LIBS, env
    assert env["SNAPPY_LIB_DIR"] == LIBS, env


def test_lib_dir_override() -> None:
    env = cargo_env({"FROGDB_LIB_DIR": "/usr/local/lib"}, platform="darwin")
    assert env["ROCKSDB_LIB_DIR"] == "/usr/local/lib", env
    assert env["SNAPPY_LIB_DIR"] == "/usr/local/lib", env


def test_existing_values_win() -> None:
    """setdefault, not assignment: an explicit caller/user setting is kept."""
    base = {
        "LIBCLANG_PATH": "/opt/llvm-19/lib",
        "DYLD_LIBRARY_PATH": "/opt/llvm-19/lib",
        "ROCKSDB_LIB_DIR": "/opt/rocksdb/lib",
        "SNAPPY_LIB_DIR": "/opt/snappy/lib",
    }
    env = cargo_env(base, platform="darwin")
    for key, value in base.items():
        assert env[key] == value, (key, env)


def test_rustc_wrapper_passes_through() -> None:
    """sccache is inherited, never forced off: a script shares `just`'s cache."""
    env = cargo_env({"RUSTC_WRAPPER": "/opt/homebrew/bin/sccache"}, platform="darwin")
    assert env["RUSTC_WRAPPER"] == "/opt/homebrew/bin/sccache", env
    # ...and it is not invented where the caller had none.
    assert "RUSTC_WRAPPER" not in cargo_env({}, platform="darwin")


def test_base_is_not_mutated() -> None:
    base: dict[str, str] = {}
    cargo_env(base, platform="darwin")
    assert base == {}, base


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
