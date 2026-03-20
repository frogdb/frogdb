#!/usr/bin/env python3
"""Analyze a samply profile JSON (Firefox Profiler format) for frogdb-server.

Resolves addresses to function names via macOS `atos` and prints:
1. Self-time breakdown by library (kernel vs malloc vs frogdb vs ...)
2. Top frogdb-server functions by self-time
3. Top frogdb-server functions by total (inclusive) time
4. What frogdb-server code is driving kernel syscalls

Usage:
    python analyze_profile.py /tmp/frogdb-profiles/frogdb-profile.json.gz
    python analyze_profile.py /tmp/frogdb-profiles/frogdb-profile.json.gz --binary ./target/profiling/frogdb-server
    python analyze_profile.py /tmp/frogdb-profiles/frogdb-profile.json.gz --top 50
"""

from __future__ import annotations

import argparse
import gzip
import json
import subprocess
from collections import Counter
from pathlib import Path


def load_profile(path: str) -> dict:
    p = Path(path)
    if p.suffix == ".gz" or p.suffixes[-2:] == [".json", ".gz"]:
        with gzip.open(p, "rt") as f:
            return json.load(f)
    with open(p) as f:
        return json.load(f)


def resolve_addresses(binary: str, addrs: list[int], base: int = 0x100000000) -> dict[int, str]:
    """Resolve relative addresses to function names using atos."""
    if not addrs:
        return {}
    abs_addrs = [f"0x{base + a:x}" for a in sorted(addrs)]
    try:
        result = subprocess.run(
            ["atos", "-o", binary] + abs_addrs,
            capture_output=True,
            text=True,
            timeout=60,
        )
        resolved = result.stdout.strip().split("\n")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {a: f"0x{a:x}" for a in addrs}

    mapping = {}
    for i, addr in enumerate(sorted(addrs)):
        if i < len(resolved):
            name = resolved[i].strip()
            name = name.split(" (in frogdb-server)")[0]
            name = name.split(" (in ")[0]
            # Demangle Rust symbols
            for old, new in [
                ("_$LT$", "<"),
                ("$GT$", ">"),
                ("$u20$", " "),
                ("$RF$", "&"),
                ("$C$", ","),
                ("$LP$", "("),
                ("$RP$", ")"),
                ("$u7b$", "{"),
                ("$u7d$", "}"),
            ]:
                name = name.replace(old, new)
            mapping[addr] = name
        else:
            mapping[addr] = f"0x{addr:x}"
    return mapping


def get_lib_name(libs, lib_idx):
    if lib_idx is not None and 0 <= lib_idx < len(libs):
        return libs[lib_idx]["name"]
    return "unknown"


def get_frame_lib_idx(thread, frame_idx, libs):
    """Get the library index for a given frame."""
    func_idx = thread["frameTable"]["func"][frame_idx]
    func_res = thread["funcTable"].get("resource", [])
    res_idx = func_res[func_idx] if func_idx < len(func_res) else -1
    rl = thread["resourceTable"].get("lib", [])
    lib_idx = rl[res_idx] if 0 <= res_idx < len(rl) else -1
    return lib_idx


def analyze(profile: dict, binary: str, top_n: int = 40):
    libs = profile["libs"]
    tokio_threads = [t for t in profile["threads"] if "tokio-runtime-worker" in t.get("name", "")]

    if not tokio_threads:
        print("No tokio-runtime-worker threads found in profile.")
        return

    # --- Phase 1: Collect per-library self-time and per-address self/total time ---
    lib_self_time = Counter()
    frogdb_self_time = Counter()
    frogdb_total_time = Counter()
    kernel_callers = Counter()  # deepest frogdb frame above kernel leaf
    total_samples = 0
    frogdb_addrs = set()

    for thr in tokio_threads:
        samples = thr["samples"]
        st = thr["stackTable"]
        ft = thr["frameTable"]

        sample_stacks = samples["stack"]
        stack_prefixes = st["prefix"]
        stack_frames = st["frame"]
        frame_addrs = ft["address"]

        for stack_idx in sample_stacks:
            if stack_idx is None:
                continue
            total_samples += 1

            # Self-time: leaf frame
            leaf_frame = stack_frames[stack_idx]
            leaf_lib = get_frame_lib_idx(thr, leaf_frame, libs)
            lib_name = get_lib_name(libs, leaf_lib)
            lib_self_time[lib_name] += 1

            addr = frame_addrs[leaf_frame]
            if lib_name == "frogdb-server":
                frogdb_self_time[addr] += 1
                frogdb_addrs.add(addr)

            # Total-time: walk stack for frogdb frames
            seen = set()
            idx = stack_idx
            while idx is not None:
                fi = stack_frames[idx]
                li = get_frame_lib_idx(thr, fi, libs)
                a = frame_addrs[fi]
                if get_lib_name(libs, li) == "frogdb-server" and a not in seen:
                    frogdb_total_time[a] += 1
                    frogdb_addrs.add(a)
                    seen.add(a)
                idx = stack_prefixes[idx]

            # Kernel caller analysis
            is_kernel = lib_name in ("libsystem_kernel.dylib",)
            if is_kernel:
                idx = stack_prefixes[stack_idx]
                while idx is not None:
                    fi = stack_frames[idx]
                    li = get_frame_lib_idx(thr, fi, libs)
                    if get_lib_name(libs, li) == "frogdb-server":
                        a = frame_addrs[fi]
                        kernel_callers[a] += 1
                        frogdb_addrs.add(a)
                        break
                    idx = stack_prefixes[idx]

    # --- Phase 2: Resolve addresses ---
    print(f"Resolving {len(frogdb_addrs)} unique frogdb-server addresses...")
    addr_names = resolve_addresses(binary, list(frogdb_addrs))

    # --- Phase 3: Print results ---
    W = 130

    print(
        f"\nTotal samples across {len(tokio_threads)} tokio-runtime-worker threads: {total_samples:,}"
    )

    # Library self-time breakdown
    print(f"\n{'=' * W}")
    print("SELF-TIME BREAKDOWN BY LIBRARY")
    print(f"{'=' * W}")
    print(f"{'%':>7}  {'Samples':>9}  Library")
    print(f"{'-' * 7}  {'-' * 9}  {'-' * 50}")
    for lib, count in lib_self_time.most_common():
        pct = count / total_samples * 100
        print(f"{pct:7.2f}  {count:9,}  {lib}")

    # Top frogdb self-time
    print(f"\n{'=' * W}")
    print(f"TOP {top_n} FROGDB-SERVER FUNCTIONS BY SELF-TIME (userspace CPU work)")
    print(f"{'=' * W}")
    print(f"{'Self%':>7}  {'Samples':>9}  Function")
    print(f"{'-' * 7}  {'-' * 9}  {'-' * (W - 20)}")
    for addr, count in frogdb_self_time.most_common(top_n):
        pct = count / total_samples * 100
        name = addr_names.get(addr, f"0x{addr:x}")
        print(f"{pct:7.2f}  {count:9,}  {name[: W - 20]}")

    # Top frogdb total-time
    print(f"\n{'=' * W}")
    print(f"TOP {top_n} FROGDB-SERVER FUNCTIONS BY TOTAL-TIME (inclusive of callees)")
    print(f"{'=' * W}")
    print(f"{'Total%':>7}  {'Self%':>7}  Function")
    print(f"{'-' * 7}  {'-' * 7}  {'-' * (W - 18)}")
    for addr, count in frogdb_total_time.most_common(top_n):
        total_pct = count / total_samples * 100
        self_count = frogdb_self_time.get(addr, 0)
        self_pct = self_count / total_samples * 100
        name = addr_names.get(addr, f"0x{addr:x}")
        print(f"{total_pct:7.2f}  {self_pct:7.2f}  {name[: W - 18]}")

    # Kernel caller analysis
    print(f"\n{'=' * W}")
    print("KERNEL SYSCALL CALLERS (deepest frogdb-server frame above kernel leaf)")
    print(f"{'=' * W}")
    print(f"{'%':>7}  {'Samples':>9}  Caller Function")
    print(f"{'-' * 7}  {'-' * 9}  {'-' * (W - 20)}")
    for addr, count in kernel_callers.most_common(top_n):
        pct = count / total_samples * 100
        name = addr_names.get(addr, f"0x{addr:x}")
        print(f"{pct:7.2f}  {count:9,}  {name[: W - 20]}")


def main():
    parser = argparse.ArgumentParser(description="Analyze samply profile for frogdb-server")
    parser.add_argument("profile", help="Path to profile .json or .json.gz file")
    parser.add_argument(
        "--binary",
        default="./target/profiling/frogdb-server",
        help="Path to frogdb-server binary with debug symbols (default: ./target/profiling/frogdb-server)",
    )
    parser.add_argument(
        "--top", type=int, default=40, help="Number of top functions to show (default: 40)"
    )
    args = parser.parse_args()

    profile = load_profile(args.profile)
    analyze(profile, args.binary, args.top)


if __name__ == "__main__":
    main()
