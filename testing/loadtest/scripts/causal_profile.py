#!/usr/bin/env python3
"""
Causal-profile FrogDB under load using tokio-coz and memtier_benchmark.

Builds with causal profiling support, runs the server with the profiler active,
generates load, then captures the causal profiling report.

Usage:
    python causal_profile.py --workload mixed --duration 120
    python causal_profile.py -w read-heavy --duration 90 --shards 4
    python causal_profile.py --profile release --duration 120
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

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


def build_causal_binary(profile: str = "debug") -> None:
    """Build FrogDB with causal profiling support via just."""
    print(f"Building with causal profiling support ({profile})...")
    subprocess.run(
        ["just", "build-causal", profile],
        cwd=REPO_ROOT,
        check=True,
    )


def run_causal_profile(
    workload: str,
    duration: int,
    threads: int = 4,
    clients: int = 25,
    shards: int | None = None,
    profile: str = "debug",
) -> None:
    """Run the full causal profiling workflow."""
    # Build first
    build_causal_binary(profile)

    # Server environment: disable persistence, enable causal profiling
    # Filter out FROGDB_SYSTEM_ROCKSDB — it's a build flag, not a server config key,
    # and figment rejects unknown FROGDB_* env vars.
    server_env = {k: v for k, v in os.environ.items() if k != "FROGDB_SYSTEM_ROCKSDB"}
    server_env["FROGDB_PERSISTENCE__ENABLED"] = "false"
    server_env["COZ_PROFILE"] = "1"
    if shards is not None:
        server_env["FROGDB_SERVER__NUM_SHARDS"] = str(shards)

    target_dir = "release" if profile == "release" else "debug"
    binary = REPO_ROOT / "target" / target_dir / "frogdb-server"
    if not binary.exists():
        print(f"Error: binary not found at {binary}", file=sys.stderr)
        sys.exit(1)

    shard_info = f" ({shards} shards)" if shards else ""
    print(f"Starting FrogDB with causal profiling{shard_info}...")
    server_proc = subprocess.Popen(
        [str(binary)],
        env=server_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        # Wait for server to be ready
        print("Waiting for FrogDB to start...")
        if not wait_for_port(SERVER_PORT):
            print("Error: FrogDB failed to start within timeout", file=sys.stderr)
            server_proc.terminate()
            sys.exit(1)
        print(f"FrogDB ready on port {SERVER_PORT}")

        # Run load test using run_memtier.py (duration-based for profiling)
        print(f"Running {workload} workload for {duration} seconds...")
        memtier_result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).parent / "run_memtier.py"),
                "-w",
                workload,
                "--test-time",
                str(duration),
                "-t",
                str(threads),
                "-c",
                str(clients),
            ],
        )

        if memtier_result.returncode != 0:
            print("Warning: Load test had non-zero exit code", file=sys.stderr)

    finally:
        # Stop server gracefully (triggers profiler report + JSON write)
        print("Stopping server (writing causal profile report)...")
        server_proc.send_signal(signal.SIGINT)
        try:
            stdout, stderr = server_proc.communicate(timeout=30)
            # Print profiler output (report goes to stdout/stderr)
            if stdout:
                print(stdout.decode(errors="replace"))
            if stderr:
                print(stderr.decode(errors="replace"), file=sys.stderr)
        except subprocess.TimeoutExpired:
            print("Warning: server did not exit within timeout, killing", file=sys.stderr)
            server_proc.kill()

    report_path = Path("causal-profile.json")
    if report_path.exists():
        print(f"\nCausal profile report written to: {report_path}")
    else:
        print("\nWarning: causal-profile.json not found (profiling may not have run)")


def main():
    parser = argparse.ArgumentParser(
        description="Causal-profile FrogDB under load using tokio-coz",
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
        "--duration",
        type=int,
        default=90,
        help="Load test duration in seconds (default: 90, enough for ~5 profiling spans)",
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
        default=4,
        help="Number of FrogDB shards (default: 4)",
    )
    parser.add_argument(
        "--profile",
        choices=["debug", "release"],
        default="debug",
        help="Build profile (default: debug)",
    )

    args = parser.parse_args()

    run_causal_profile(
        workload=args.workload,
        duration=args.duration,
        threads=args.threads,
        clients=args.clients,
        shards=args.shards,
        profile=args.profile,
    )


if __name__ == "__main__":
    main()
