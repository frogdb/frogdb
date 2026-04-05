#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
dev_server.py - Start FrogDB with continuous low-volume traffic for development.

Builds and runs the server, waits for it to be ready, then generates gentle
background traffic so metrics/debug pages/logs stay populated.

Usage:
    uv run dev_server.py                        # mixed workload, 500 ops/sec
    uv run dev_server.py -w read-heavy          # read-heavy workload
    uv run dev_server.py --rate 200             # slower rate
    uv run dev_server.py --release              # use release build

Press Ctrl-C to stop both the load generator and the server.
"""

import argparse
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SERVER_PORT = 6379
HTTP_PORT = 9090
STARTUP_TIMEOUT = 60  # generous for debug builds

WORKLOAD_RATIOS = {
    "read-heavy": "19:1",
    "write-heavy": "1:19",
    "mixed": "9:1",
}


def wait_for_port(port: int, timeout: float = STARTUP_TIMEOUT) -> bool:
    """Wait for a port to accept connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.2)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Start FrogDB with continuous low-volume traffic",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-w",
        "--workload",
        choices=list(WORKLOAD_RATIOS.keys()),
        default="mixed",
        help="Workload preset",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=500,
        help="Target ops/sec (across all clients)",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Use release build instead of debug",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=SERVER_PORT,
        help="Server port",
    )
    args, extra = parser.parse_known_args()

    if not shutil.which("memtier_benchmark"):
        print(
            "Error: memtier_benchmark not found. Install with: brew install memtier_benchmark",
            file=sys.stderr,
        )
        return 1

    ratio = WORKLOAD_RATIOS[args.workload]
    run_recipe = "run-release" if args.release else "run"

    # Start the server via just (inherits DYLD/ROCKSDB env from justfile)
    server_args = ["just", run_recipe] + extra
    build_type = "release" if args.release else "debug"
    print(f"Building and starting FrogDB ({build_type})...")

    server_proc = subprocess.Popen(
        server_args,
        cwd=REPO_ROOT,
        # Let server stdout/stderr pass through so logs are visible
    )

    memtier_proc = None

    def cleanup(signum=None, frame=None):
        """Shut down memtier then server."""
        if memtier_proc and memtier_proc.poll() is None:
            memtier_proc.terminate()
            try:
                memtier_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                memtier_proc.kill()

        if server_proc.poll() is None:
            server_proc.terminate()
            try:
                server_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server_proc.kill()

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        # Wait for server to be ready
        print(f"Waiting for FrogDB on port {args.port}...")
        if not wait_for_port(args.port):
            print("Error: FrogDB failed to start within timeout", file=sys.stderr)
            cleanup()
            return 1

        print()
        print(f"  FrogDB ready on port {args.port}")
        print(f"  Debug UI:  http://127.0.0.1:{HTTP_PORT}/debug")
        print(f"  Metrics:   http://127.0.0.1:{HTTP_PORT}/metrics")
        print(f"  Status:    http://127.0.0.1:{HTTP_PORT}/status/json")
        print()
        print(f"  Workload:  {args.workload} (ratio {ratio})")
        print(f"  Rate:      ~{args.rate} ops/sec")
        print()
        print("  Press Ctrl-C to stop")
        print()

        # Start low-volume memtier in the background
        memtier_cmd = [
            "memtier_benchmark",
            "--server",
            "127.0.0.1",
            "--port",
            str(args.port),
            "--threads",
            "1",
            "--clients",
            "5",
            "--ratio",
            ratio,
            "--key-pattern",
            "G:G",
            "--data-size",
            "128",
            "--test-time",
            "999999",
            "--rate-limiting",
            str(args.rate),
            "--print-interval",
            "10",
            "--hide-histogram",
        ]
        memtier_proc = subprocess.Popen(
            memtier_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for either process to exit
        while True:
            if server_proc.poll() is not None:
                print(f"\nServer exited (code {server_proc.returncode})")
                break
            if memtier_proc.poll() is not None:
                print(f"\nLoad generator exited (code {memtier_proc.returncode})")
                break
            time.sleep(0.5)

    finally:
        cleanup()

    return server_proc.returncode or 0


if __name__ == "__main__":
    sys.exit(main())
