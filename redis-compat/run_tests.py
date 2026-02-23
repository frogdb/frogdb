#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Redis Compatibility Test Runner for FrogDB

This script downloads the Redis 7.2.4 source code, builds FrogDB in release mode,
and runs the official Redis Tcl test suite against FrogDB in external server mode.
"""

import argparse
import atexit
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

# Configuration
REDIS_VERSION = "7.2.4"
REDIS_URL = f"https://github.com/redis/redis/archive/refs/tags/{REDIS_VERSION}.tar.gz"
CACHE_DIR = Path(".redis-tests")
DEFAULT_PORT = 6399
FROGDB_STARTUP_TIMEOUT = 10  # seconds


def get_script_dir() -> Path:
    """Get the directory containing this script."""
    return Path(__file__).parent.resolve()


def get_project_root() -> Path:
    """Get the FrogDB project root directory."""
    return get_script_dir().parent


def get_skipfile_path() -> Path:
    """Get the path to the combined skipfile."""
    return CACHE_DIR / "combined_skiplist.txt"


def download_redis(cache_dir: Path) -> Path:
    """Download and extract Redis source to cache directory."""
    redis_dir = cache_dir / f"redis-{REDIS_VERSION}"

    if redis_dir.exists():
        print(f"Using cached Redis source at {redis_dir}")
        return redis_dir

    cache_dir.mkdir(parents=True, exist_ok=True)
    tarball = cache_dir / f"redis-{REDIS_VERSION}.tar.gz"

    print(f"Downloading Redis {REDIS_VERSION}...")
    urllib.request.urlretrieve(REDIS_URL, tarball)

    print(f"Extracting to {cache_dir}...")
    with tarfile.open(tarball, "r:gz") as tar:
        tar.extractall(cache_dir)

    tarball.unlink()
    print(f"Redis source ready at {redis_dir}")
    return redis_dir


def build_frogdb(project_root: Path) -> Path:
    """Build FrogDB in release mode and return path to binary."""
    print("Building FrogDB in release mode...")
    result = subprocess.run(
        ["cargo", "build", "--release", "-p", "frogdb-server"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("Build failed:")
        print(result.stderr)
        sys.exit(1)

    binary = project_root / "target" / "release" / "frogdb-server"
    if not binary.exists():
        print(f"Error: Expected binary not found at {binary}")
        sys.exit(1)

    print(f"FrogDB built successfully: {binary}")
    return binary


def combine_skiplists(script_dir: Path, cache_dir: Path) -> Path:
    """Combine all skiplist files into one for the test runner."""
    skipfiles = [
        script_dir / "skiplist-intentional.txt",
        script_dir / "skiplist-not-implemented.txt",
        script_dir / "skiplist-flaky.txt",
    ]

    combined = cache_dir / "combined_skiplist.txt"
    cache_dir.mkdir(parents=True, exist_ok=True)

    with open(combined, "w") as outfile:
        for skipfile in skipfiles:
            if skipfile.exists():
                outfile.write(f"# From {skipfile.name}\n")
                with open(skipfile) as infile:
                    for line in infile:
                        # Skip empty lines and comments for deduplication
                        stripped = line.strip()
                        if stripped and not stripped.startswith("#"):
                            outfile.write(f"{stripped}\n")
                outfile.write("\n")

    print(f"Combined skiplist written to {combined}")
    return combined


class FrogDBServer:
    """Manages the FrogDB server process."""

    def __init__(self, binary: Path, port: int, data_dir: Path):
        self.binary = binary
        self.port = port
        self.data_dir = data_dir
        self.process: subprocess.Popen | None = None

    def start(self) -> None:
        """Start the FrogDB server."""
        self.data_dir.mkdir(parents=True, exist_ok=True)

        print(f"Starting FrogDB on port {self.port}...")
        self.process = subprocess.Popen(
            [
                str(self.binary),
                "--port",
                str(self.port),
                "--data-dir",
                str(self.data_dir),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Wait for server to be ready
        start_time = time.time()
        while time.time() - start_time < FROGDB_STARTUP_TIMEOUT:
            try:
                import socket

                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    print(f"FrogDB is ready on port {self.port}")
                    return
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(0.1)

        # Check if process died
        if self.process.poll() is not None:
            stdout, stderr = self.process.communicate()
            print("FrogDB failed to start:")
            print(stderr.decode())
            sys.exit(1)

        print(f"Warning: FrogDB may not be ready after {FROGDB_STARTUP_TIMEOUT}s")

    def stop(self) -> None:
        """Stop the FrogDB server."""
        if self.process is None:
            return

        print("Stopping FrogDB...")
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            print("FrogDB did not terminate gracefully, killing...")
            self.process.kill()
            self.process.wait()

        self.process = None
        print("FrogDB stopped")

    def cleanup_data(self) -> None:
        """Remove the data directory."""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)


def run_redis_tests(
    redis_dir: Path,
    port: int,
    skipfile: Path,
    single: str | None = None,
    tags: list[str] | None = None,
    verbose: bool = False,
) -> int:
    """Run the Redis test suite against FrogDB."""
    tests_dir = redis_dir / "tests"
    runtest = tests_dir / "integration" / "run.tcl"

    if not runtest.exists():
        # Fallback to older Redis test layout
        runtest = tests_dir / "support" / "run.tcl"

    # Build the command
    cmd = [
        "tclsh",
        str(redis_dir / "tests" / "test_helper.tcl"),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--singledb",
        "--ignore-encoding",
        "--ignore-digest",
    ]

    # Add skipfile if it exists and has content
    if skipfile.exists() and skipfile.stat().st_size > 0:
        cmd.extend(["--skipfile", str(skipfile)])

    # Add default exclusion tags for features FrogDB doesn't support
    default_exclude_tags = [
        "needs:repl",
        "needs:debug",
        "needs:save",
        "needs:reset",
        "needs:config-maxmemory",
        "external:skip",
    ]

    for tag in default_exclude_tags:
        cmd.extend(["--tags", f"-{tag}"])

    # Add user-specified tags
    if tags:
        for tag in tags:
            cmd.extend(["--tags", tag])

    # Add single test if specified
    if single:
        cmd.extend(["--single", single])

    if verbose:
        cmd.append("--verbose")

    print(f"Running: {' '.join(cmd)}")
    print("-" * 60)

    # Run the tests
    result = subprocess.run(
        cmd,
        cwd=redis_dir / "tests",
        env={**os.environ, "TERM": "dumb"},
    )

    return result.returncode


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run Redis compatibility tests against FrogDB"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("FROGDB_PORT", DEFAULT_PORT)),
        help=f"Port to run FrogDB on (default: {DEFAULT_PORT}, env: FROGDB_PORT)",
    )
    parser.add_argument(
        "--single",
        type=str,
        help="Run a single test file (e.g., unit/type/string)",
    )
    parser.add_argument(
        "--tags",
        type=str,
        action="append",
        help="Additional tags to filter tests (can be specified multiple times)",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip building FrogDB (use existing binary)",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading Redis (use cached version)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose test output",
    )
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep FrogDB data directory after tests",
    )

    args = parser.parse_args()

    script_dir = get_script_dir()
    project_root = get_project_root()
    cache_dir = project_root / CACHE_DIR

    # Download Redis if needed
    if args.skip_download and not (cache_dir / f"redis-{REDIS_VERSION}").exists():
        print("Error: --skip-download specified but Redis source not cached")
        sys.exit(1)

    redis_dir = download_redis(cache_dir)

    # Build FrogDB
    if args.skip_build:
        binary = project_root / "target" / "release" / "frogdb-server"
        if not binary.exists():
            print("Error: --skip-build specified but binary not found")
            sys.exit(1)
    else:
        binary = build_frogdb(project_root)

    # Combine skiplists
    skipfile = combine_skiplists(script_dir, cache_dir)

    # Setup FrogDB server
    data_dir = cache_dir / "frogdb-test-data"
    server = FrogDBServer(binary, args.port, data_dir)

    # Register cleanup handlers
    def cleanup() -> None:
        server.stop()
        if not args.keep_data:
            server.cleanup_data()

    atexit.register(cleanup)
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(130))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(143))

    # Start server and run tests
    server.start()

    exit_code = run_redis_tests(
        redis_dir=redis_dir,
        port=args.port,
        skipfile=skipfile,
        single=args.single,
        tags=args.tags,
        verbose=args.verbose,
    )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
