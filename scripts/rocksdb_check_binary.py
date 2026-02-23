#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Check that a built binary dynamically links system RocksDB (post-build).

Usage:
    uv run scripts/rocksdb_check_binary.py                   # Not using system RocksDB
    uv run scripts/rocksdb_check_binary.py --system-rocksdb  # Check binary linking
"""

import argparse
import platform
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Check binary links system RocksDB")
    parser.add_argument(
        "--system-rocksdb",
        action="store_true",
        help="Enable system RocksDB binary check",
    )
    args = parser.parse_args()

    if not args.system_rocksdb:
        print("FROGDB_SYSTEM_ROCKSDB is not set — skipping binary link check.")
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
        result = subprocess.run(["otool", "-L", str(binary)], capture_output=True, text=True, check=True)
    else:
        result = subprocess.run(["ldd", str(binary)], capture_output=True, text=True, check=True)

    deps = result.stdout

    if "librocksdb" in deps:
        print("  ✓ Dynamically links librocksdb (system RocksDB)")
    else:
        print("  ✗ No dynamic librocksdb link found — RocksDB is likely vendored (statically compiled)")
        sys.exit(1)


if __name__ == "__main__":
    main()
