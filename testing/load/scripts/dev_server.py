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
MEMTIER_LOG_FILE = REPO_ROOT / ".dev-server-memtier.log"
# Readiness-only: the build phase (see build_server()) runs first and unbounded, so by the time
# this clock starts the binary has already been compiled and spawned. This is just "how long may
# the process take to open its listening socket."
STARTUP_TIMEOUT = 60

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


def build_server(build_type: str) -> bool:
    """Build the server binary as its own phase, output visible, no deadline.

    Runs before the server is spawned so a cold/throttled compile never eats into the
    readiness-wait timeout (see wait_for_port).
    """
    print(f"==> Building FrogDB ({build_type})...")
    result = subprocess.run(["just", "build-server", build_type], cwd=REPO_ROOT)
    if result.returncode != 0:
        print(f"Error: build failed (exit {result.returncode})", file=sys.stderr)
        return False
    print("==> Build complete.")
    return True


def print_log_tail(path: Path, lines: int = 40) -> None:
    """Print the last N lines of a log file to stderr, for post-mortem on a nonzero exit."""
    try:
        content = path.read_text()
    except OSError as exc:
        print(f"  (could not read {path}: {exc})", file=sys.stderr)
        return
    tail = content.splitlines()[-lines:]
    print(f"--- last {len(tail)} line(s) of {path} ---", file=sys.stderr)
    for line in tail:
        print(f"  {line}", file=sys.stderr)
    print("--- end of log ---", file=sys.stderr)


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
    # Without this, stdout is fully block-buffered whenever it isn't a TTY (redirected to a
    # file, piped, or captured by a harness) — our phase-transition prints would then sit in
    # Python's buffer and only appear all at once at process exit, interleaved wrong relative
    # to the subprocess output (cargo/server/memtier write directly, unbuffered) they're meant
    # to bracket. Line-buffering keeps them appearing in real time.
    sys.stdout.reconfigure(line_buffering=True)

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
    build_type = "release" if args.release else "debug"

    # Build first, as its own unbounded phase (see build_server()) — the binary is fully
    # compiled before we spawn it or start the readiness clock.
    if not build_server(build_type):
        return 1

    # Start the server via just (inherits DYLD/ROCKSDB env from justfile). The build above
    # already finished, so this just launches the (already up to date) binary.
    server_env = {
        **os.environ,
        "FROGDB_SERVER__PORT": str(server_port),
        "FROGDB_HTTP__PORT": str(http_port),
    }
    server_args = ["just", run_recipe] + extra
    print(f"==> Starting FrogDB ({build_type})...")

    server_proc = subprocess.Popen(
        server_args,
        cwd=REPO_ROOT,
        env=server_env,
        # Let server stdout/stderr pass through so logs are visible
    )

    memtier_proc = None
    memtier_log = None

    def cleanup(signum=None, frame=None):
        """Shut down memtier then server."""
        if memtier_proc and memtier_proc.poll() is None:
            memtier_proc.terminate()
            try:
                memtier_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                memtier_proc.kill()
        if memtier_log and not memtier_log.closed:
            memtier_log.close()

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
        # Wait for server to be ready. The build already finished (build_server(), above) and
        # the binary is already spawned (server_proc, above), so this clock times only the
        # process's own startup (config load, storage open, listener bind) — not compile time.
        print(f"==> Waiting for FrogDB to be ready on port {server_port}...")
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
            "--hide-histogram",
        ]
        print(f"==> Starting memtier load generator (log: {MEMTIER_LOG_FILE})...")
        memtier_log = open(MEMTIER_LOG_FILE, "w")
        memtier_proc = subprocess.Popen(
            memtier_cmd,
            stdout=memtier_log,
            stderr=subprocess.STDOUT,
        )

        # Wait for either process to exit
        while True:
            if server_proc.poll() is not None:
                print(f"\nServer exited (code {server_proc.returncode})")
                break
            if memtier_proc.poll() is not None:
                print(f"\nLoad generator exited (code {memtier_proc.returncode})")
                if memtier_proc.returncode != 0:
                    memtier_log.flush()
                    print_log_tail(MEMTIER_LOG_FILE)
                break
            time.sleep(0.5)

    finally:
        cleanup()

    return server_proc.returncode or 0


if __name__ == "__main__":
    sys.exit(main())
