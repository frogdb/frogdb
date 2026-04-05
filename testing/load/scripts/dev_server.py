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
    uv run dev_server.py                        # random port, mixed workload, 500 ops/sec
    uv run dev_server.py -w read-heavy          # read-heavy workload
    uv run dev_server.py --rate 200             # slower rate
    uv run dev_server.py --release              # use release build
    uv run dev_server.py --port 6379            # fixed port instead of random

Press Ctrl-C to stop both the load generator and the server.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
STATE_FILE = REPO_ROOT / ".dev-server.json"
STARTUP_TIMEOUT = 60  # generous for debug builds

WORKLOAD_RATIOS = {
    "read-heavy": "19:1",
    "write-heavy": "1:19",
    "mixed": "9:1",
}


def find_free_port() -> int:
    """Find a free TCP port by binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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


def kill_existing() -> None:
    """Kill any existing dev server found via the state file."""
    if not STATE_FILE.exists():
        return

    try:
        state = json.loads(STATE_FILE.read_text())
        pid = state.get("pid")
        if pid:
            try:
                os.kill(pid, signal.SIGTERM)
                # Wait briefly for process to exit
                for _ in range(20):
                    try:
                        os.kill(pid, 0)  # Check if still alive
                        time.sleep(0.25)
                    except OSError:
                        break
                print(f"Killed previous dev server (PID {pid})")
            except OSError:
                pass  # Already dead
    except (json.JSONDecodeError, KeyError):
        pass

    STATE_FILE.unlink(missing_ok=True)


def write_state(pid: int, port: int, http_port: int) -> None:
    """Write the dev server state file."""
    STATE_FILE.write_text(
        json.dumps(
            {"pid": pid, "port": port, "http_port": http_port},
            indent=2,
        )
        + "\n"
    )


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
        default=0,
        help="Server port (0 = random free port)",
    )
    args, extra = parser.parse_known_args()

    if not shutil.which("memtier_benchmark"):
        print(
            "Error: memtier_benchmark not found. Install with: brew install memtier_benchmark",
            file=sys.stderr,
        )
        return 1

    # Kill any existing dev server
    kill_existing()

    # Resolve ports
    server_port = args.port if args.port != 0 else find_free_port()
    http_port = find_free_port() if args.port == 0 else 9090

    ratio = WORKLOAD_RATIOS[args.workload]
    run_recipe = "run-release" if args.release else "run"

    # Start the server via just (inherits DYLD/ROCKSDB env from justfile)
    server_env = {
        **os.environ,
        "FROGDB_SERVER__PORT": str(server_port),
        "FROGDB_HTTP__PORT": str(http_port),
    }
    server_args = ["just", run_recipe] + extra
    build_type = "release" if args.release else "debug"
    print(f"Building and starting FrogDB ({build_type})...")

    server_proc = subprocess.Popen(
        server_args,
        cwd=REPO_ROOT,
        env=server_env,
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

        STATE_FILE.unlink(missing_ok=True)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    try:
        # Wait for server to be ready
        print(f"Waiting for FrogDB on port {server_port}...")
        if not wait_for_port(server_port):
            print("Error: FrogDB failed to start within timeout", file=sys.stderr)
            cleanup()
            return 1

        # Write state file for skill/tooling discovery
        write_state(server_proc.pid, server_port, http_port)

        print()
        print(f"  FrogDB ready on port {server_port}")
        print(f"  Debug UI:  http://127.0.0.1:{http_port}/debug")
        print(f"  Metrics:   http://127.0.0.1:{http_port}/metrics")
        print(f"  Status:    http://127.0.0.1:{http_port}/status/json")
        print()
        print(f"  Workload:  {args.workload} (ratio {ratio})")
        print(f"  Rate:      ~{args.rate} ops/sec")
        print(f"  State:     {STATE_FILE}")
        print()
        print("  Press Ctrl-C to stop")
        print()

        # Start low-volume memtier in the background
        memtier_cmd = [
            "memtier_benchmark",
            "--server",
            "127.0.0.1",
            "--port",
            str(server_port),
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
