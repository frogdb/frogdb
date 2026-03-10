#!/usr/bin/env python3
"""
Profile FrogDB under load using samply and memtier_benchmark.

Usage:
    python profile_load.py --workload mixed --requests 50000 --open
    python profile_load.py -w read-heavy -n 100000 --save-only
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

PROFILE_DIR = Path("/tmp/claude")
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SERVER_PORT = 6379
STARTUP_TIMEOUT = 30  # seconds


def wait_for_port(port: int, timeout: float = STARTUP_TIMEOUT) -> bool:
    """Wait for a port to become available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def build_profiling_binary() -> None:
    """Build FrogDB with profiling symbols via just."""
    print("Building profiling binary...")
    subprocess.run(
        ["just", "build-profile"],
        cwd=REPO_ROOT,
        check=True,
    )


def run_profile(
    workload: str,
    requests: int,
    auto_open: bool,
    threads: int = 4,
    clients: int = 25,
    shards: int | None = None,
) -> Path:
    """Run the full profiling workflow."""
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    profile_path = PROFILE_DIR / f"frogdb-profile-{timestamp}.json"

    # Build first
    build_profiling_binary()

    # Server environment: disable persistence, configure shards
    server_env = {
        **os.environ,
        "FROGDB_PERSISTENCE__ENABLED": "false",
    }
    if shards is not None:
        server_env["FROGDB_SERVER__NUM_SHARDS"] = str(shards)

    # Start samply with FrogDB
    shard_info = f" ({shards} shards)" if shards else ""
    print(f"Starting FrogDB under samply profiler{shard_info}...")
    samply_proc = subprocess.Popen(
        [
            "samply",
            "record",
            "--save-only",
            "-o",
            str(profile_path),
            str(REPO_ROOT / "target" / "profiling" / "frogdb-server"),
        ],
        env=server_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for server to be ready
        print("Waiting for FrogDB to start...")
        if not wait_for_port(SERVER_PORT):
            print("Error: FrogDB failed to start within timeout", file=sys.stderr)
            samply_proc.terminate()
            sys.exit(1)
        print(f"FrogDB ready on port {SERVER_PORT}")

        # Run load test using run_memtier.py
        print(f"Running {workload} workload ({requests} requests)...")
        memtier_result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).parent / "run_memtier.py"),
                "-w",
                workload,
                "-n",
                str(requests),
                "-t",
                str(threads),
                "-c",
                str(clients),
            ],
        )

        if memtier_result.returncode != 0:
            print("Warning: Load test had non-zero exit code", file=sys.stderr)

    finally:
        # Stop server gracefully (triggers profile save)
        print("Stopping server...")
        samply_proc.send_signal(signal.SIGINT)
        samply_proc.wait(timeout=30)

    print(f"Profile saved to: {profile_path}")

    # Optionally open in Firefox Profiler
    if auto_open and profile_path.exists():
        print("Opening in Firefox Profiler...")
        subprocess.run(["samply", "load", str(profile_path)])

    return profile_path


def main():
    parser = argparse.ArgumentParser(
        description="Profile FrogDB under load using samply",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-w",
        "--workload",
        choices=["read-heavy", "write-heavy", "mixed"],
        default="mixed",
        help="Workload preset",
    )
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=10000,
        help="Number of requests per client",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=4,
        help="Number of memtier threads",
    )
    parser.add_argument(
        "-c",
        "--clients",
        type=int,
        default=25,
        help="Clients per thread",
    )
    parser.add_argument(
        "--shards",
        type=int,
        default=None,
        help="Number of FrogDB shards (default: server default)",
    )

    open_group = parser.add_mutually_exclusive_group()
    open_group.add_argument(
        "--open",
        action="store_true",
        dest="auto_open",
        default=True,
        help="Auto-open Firefox Profiler after profiling (default)",
    )
    open_group.add_argument(
        "--save-only",
        action="store_false",
        dest="auto_open",
        help="Save profile without opening viewer",
    )

    args = parser.parse_args()

    run_profile(
        workload=args.workload,
        requests=args.requests,
        auto_open=args.auto_open,
        threads=args.threads,
        clients=args.clients,
        shards=args.shards,
    )


if __name__ == "__main__":
    main()
