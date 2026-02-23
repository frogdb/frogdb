#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Check that system RocksDB libraries exist at the configured path.

Usage:
    uv run scripts/linkcheck_libs.py                              # Not using system RocksDB
    uv run scripts/linkcheck_libs.py --system-rocksdb             # Check default path
    uv run scripts/linkcheck_libs.py --system-rocksdb --lib-dir /usr/lib
"""

import argparse
import platform
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Check for system RocksDB libraries")
    parser.add_argument(
        "--system-rocksdb",
        action="store_true",
        help="Enable system RocksDB check",
    )
    parser.add_argument(
        "--lib-dir",
        default="/opt/homebrew/lib",
        help="Library directory to search (default: /opt/homebrew/lib)",
    )
    args = parser.parse_args()

    if not args.system_rocksdb:
        print("FROGDB_SYSTEM_ROCKSDB is not set — using vendored (compiled-from-source) RocksDB.")
        print("Set FROGDB_SYSTEM_ROCKSDB=1 to use system-installed RocksDB for faster builds.")
        sys.exit(0)

    lib_dir = Path(args.lib_dir)
    print(f"Checking for system RocksDB libraries in: {lib_dir}")

    ext = "dylib" if platform.system() == "Darwin" else "so"

    ok = True
    for lib in ("rocksdb", "snappy"):
        matches = list(lib_dir.glob(f"lib{lib}.*{ext}*"))
        if matches:
            print(f"  ✓ lib{lib} found")
        else:
            print(f"  ✗ lib{lib} NOT found")
            ok = False

    if not ok:
        print()
        if platform.system() == "Darwin":
            print("Install with: brew install rocksdb snappy")
        else:
            print("Install with: apt install librocksdb-dev libsnappy-dev  (or your distro's equivalent)")
        sys.exit(1)

    print("All required libraries found.")


if __name__ == "__main__":
    main()
