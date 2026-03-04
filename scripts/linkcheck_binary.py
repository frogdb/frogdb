#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Check that a built binary dynamically links system libraries (post-build).

Verifies RocksDB, Snappy, and optionally zstd appear in dynamic library deps.

Usage:
    uv run scripts/linkcheck_binary.py                                  # Not using system RocksDB
    uv run scripts/linkcheck_binary.py --system-rocksdb                 # Check RocksDB + Snappy
    uv run scripts/linkcheck_binary.py --system-rocksdb --system-zstd   # Also check zstd
"""

import argparse
import platform
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Check binary links system libraries")
    parser.add_argument(
        "--system-rocksdb",
        action="store_true",
        help="Check for dynamic RocksDB and Snappy linking",
    )
    parser.add_argument(
        "--system-zstd",
        action="store_true",
        help="Also check for dynamic zstd linking",
    )
    args = parser.parse_args()

    if not args.system_rocksdb and not args.system_zstd:
        print("No system library flags set — skipping binary link check.")
        sys.exit(0)

    # Find the most recently built binary (debug or release)
    candidates = [
        Path("target/release/frogdb-server"),
        Path("target/debug/frogdb-server"),
    ]

    binary = None
    for candidate in candidates:
        if candidate.is_file():
            if binary is None or candidate.stat().st_mtime > binary.stat().st_mtime:
                binary = candidate

    if binary is None:
        print("No built binary found. Run 'just build' or 'just release' first.")
        sys.exit(1)

    print(f"Checking binary: {binary}")

    if platform.system() == "Darwin":
        result = subprocess.run(
            ["otool", "-L", str(binary)], capture_output=True, text=True, check=True
        )
    else:
        result = subprocess.run(["ldd", str(binary)], capture_output=True, text=True, check=True)

    deps = result.stdout

    # Build list of libraries to check
    checks: list[tuple[str, str, bool]] = []
    if args.system_rocksdb:
        checks.append(("librocksdb", "RocksDB", True))
        checks.append(("libsnappy", "Snappy", True))
    if args.system_zstd:
        checks.append(("libzstd", "zstd", True))

    ok = True
    for lib_name, label, required in checks:
        if lib_name in deps:
            print(f"  \u2713 Dynamically links {lib_name} (system {label})")
        else:
            print(
                f"  \u2717 No dynamic {lib_name} link found — {label} is likely statically compiled"
            )
            if required:
                ok = False

    if not ok:
        sys.exit(1)

    print()
    print("All binary link checks passed.")


if __name__ == "__main__":
    main()
