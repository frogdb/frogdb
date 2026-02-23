#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Post-build verification: inspect cargo build output files to confirm linking mode.

Checks target/<profile>/build/<crate>-*/output for each dependency to determine
whether it was linked from system libraries or compiled from source.

Usage:
    uv run scripts/linkcheck_build.py --profile debug --system-rocksdb
    uv run scripts/linkcheck_build.py --profile release --system-rocksdb --system-zstd
"""

import argparse
import glob
import sys
from pathlib import Path

# Maps crate name to (system indicator pattern, source indicator pattern, description)
CRATE_CHECKS = {
    "librocksdb-sys": {
        "system": "cargo:rustc-link-lib=dylib=rocksdb",
        "source": "cargo:rustc-link-lib=static=rocksdb",
        "label": "RocksDB",
    },
    "snappy-sys": {
        "system": "cargo:rustc-link-lib=dylib=snappy",
        "source": "cargo:rustc-link-lib=static=snappy",
        "label": "Snappy",
        # snappy linking info is in librocksdb-sys output, not a separate crate
        "alt_crate": "librocksdb-sys",
    },
    "zstd-sys": {
        "system": "cargo:rustc-link-lib=dylib=zstd",
        "source": "cargo:rustc-link-lib=static=zstd",
        "label": "zstd",
    },
    "lz4-sys": {
        "system": None,  # lz4-sys always compiles from source
        "source": "cargo:rustc-link-lib=static=lz4",
        "label": "lz4",
    },
}


def find_output_file(build_dir: Path, crate_name: str) -> Path | None:
    """Find the cargo build output file for a crate."""
    pattern = str(build_dir / f"{crate_name}-*" / "output")
    matches = glob.glob(pattern)
    if not matches:
        return None
    # Return the most recently modified one
    return max((Path(m) for m in matches), key=lambda p: p.stat().st_mtime)


def check_crate(
    build_dir: Path,
    crate_name: str,
    info: dict,
    expect_system: bool,
) -> bool:
    """Check a single crate's linking mode. Returns True if check passed."""
    label = info["label"]
    search_crate = info.get("alt_crate", crate_name)

    output_file = find_output_file(build_dir, search_crate)
    if output_file is None:
        print(f"  ? {label}: no build output found for {search_crate} (stale or clean build)")
        return True  # not a failure, just no data

    content = output_file.read_text()

    system_pattern = info["system"]
    source_pattern = info["source"]

    is_system = system_pattern is not None and system_pattern in content
    is_source = source_pattern is not None and source_pattern in content

    if system_pattern is None:
        # Crate always compiles from source (e.g. lz4-sys)
        print(f"  - {label}: always compiled from source (4 small C files, unavoidable)")
        return True

    if expect_system:
        if is_system:
            print(f"  \u2713 {label}: linked from system library")
            return True
        elif is_source:
            print(f"  \u2717 {label}: compiled from source (expected system library)")
            return False
        else:
            print(f"  ? {label}: could not determine linking mode from {output_file}")
            return False
    else:
        if is_source:
            print(f"  - {label}: compiled from source (vendored)")
            return True
        elif is_system:
            print(f"  - {label}: linked from system library")
            return True
        else:
            print(f"  ? {label}: could not determine linking mode from {output_file}")
            return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify build outputs confirm system library linking"
    )
    parser.add_argument(
        "--profile",
        default="debug",
        choices=["debug", "release"],
        help="Build profile to check (default: debug)",
    )
    parser.add_argument(
        "--system-rocksdb",
        action="store_true",
        help="Expect RocksDB and Snappy to be linked from system libraries",
    )
    parser.add_argument(
        "--system-zstd",
        action="store_true",
        help="Expect zstd to be linked from system library (via ZSTD_SYS_USE_PKG_CONFIG=1)",
    )
    args = parser.parse_args()

    build_dir = Path("target") / args.profile / "build"
    if not build_dir.is_dir():
        print(f"Build directory not found: {build_dir}")
        print("Run a build first (e.g. 'just build' or 'just release').")
        sys.exit(1)

    print(f"Checking build outputs in: {build_dir}")
    print()

    ok = True

    # RocksDB
    if not check_crate(build_dir, "librocksdb-sys", CRATE_CHECKS["librocksdb-sys"], args.system_rocksdb):
        ok = False

    # Snappy (output lives in librocksdb-sys)
    if not check_crate(build_dir, "snappy-sys", CRATE_CHECKS["snappy-sys"], args.system_rocksdb):
        ok = False

    # zstd
    if not check_crate(build_dir, "zstd-sys", CRATE_CHECKS["zstd-sys"], args.system_zstd):
        ok = False

    # lz4 (always source, informational only)
    check_crate(build_dir, "lz4-sys", CRATE_CHECKS["lz4-sys"], False)

    print()
    if ok:
        print("All checks passed.")
    else:
        print("Some checks failed — see above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
