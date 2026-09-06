# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Cargo build environment for Python scripts, mirroring the Justfile prelude.

This is the single Python entry point for the environment cargo needs in this
repo: it reproduces the Justfile's `LIBCLANG_PATH`, `dyld-env`, and
`rocksdb-env` prelude (libclang for bindgen, `DYLD_LIBRARY_PATH` on macOS, and
`ROCKSDB_LIB_DIR`/`SNAPPY_LIB_DIR` pointing at `FROGDB_LIB_DIR` whenever
`FROGDB_SYSTEM_ROCKSDB` is non-empty) so a script's cargo invocation links the
system RocksDB instead of spending ~10 minutes of CPU and 1.5 GB of disk
building the vendored copy from source. Every value is applied with
`setdefault`, so an explicit caller or user setting always wins, and
`RUSTC_WRAPPER` (sccache) is passed through untouched — a script must never
force it off, or it thrashes the build cache it shares with `just`. The
`lint-cargo-env` gate (see `agents/seam-lints.md`) requires every cargo
subprocess launched from `scripts/` to build its environment through this
module.

The module carries no PyPI dependencies, so a `uv run --script` consumer that
imports it needs nothing declared in its own PEP-723 header.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping

MACOS_LIBCLANG = "/opt/homebrew/opt/llvm/lib"
LINUX_LIBCLANG = "/usr/lib/llvm-18/lib"
DEFAULT_LIB_DIR = "/opt/homebrew/lib"


def cargo_env(
    base: Mapping[str, str] | None = None,
    *,
    platform: str | None = None,
) -> dict[str, str]:
    """Return a copy of `base` (default `os.environ`) with cargo's build vars set.

    Mirrors the Justfile exactly: libclang defaults per platform,
    `DYLD_LIBRARY_PATH` on macOS only, and the system-RocksDB vars only when
    `FROGDB_SYSTEM_ROCKSDB` is non-empty (default `1` on macOS, empty — i.e.
    vendored — elsewhere). Existing values are never overwritten.
    """
    env = dict(os.environ if base is None else base)
    is_macos = (sys.platform if platform is None else platform) == "darwin"

    env.setdefault("LIBCLANG_PATH", MACOS_LIBCLANG if is_macos else LINUX_LIBCLANG)
    if is_macos:
        env.setdefault("DYLD_LIBRARY_PATH", MACOS_LIBCLANG)

    if env.get("FROGDB_SYSTEM_ROCKSDB", "1" if is_macos else ""):
        lib_dir = env.get("FROGDB_LIB_DIR", DEFAULT_LIB_DIR)
        env.setdefault("ROCKSDB_LIB_DIR", lib_dir)
        env.setdefault("SNAPPY_LIB_DIR", lib_dir)

    return env
