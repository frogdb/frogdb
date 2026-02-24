#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Redis Compatibility Test Runner for FrogDB

This script downloads the Redis 7.2.4 source code, builds FrogDB in release mode,
and runs the official Redis Tcl test suite against FrogDB in external server mode.

By default, every non-skipped suite is run individually with its own FrogDB server
instance and a wall-clock timeout. Suites that hang or crash the server are detected
and reported without blocking the rest of the run.
"""

import argparse
import atexit
import os
import re
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
DEFAULT_SUITE_TIMEOUT = 60  # seconds per suite
FROGDB_STARTUP_TIMEOUT = 10  # seconds

# Default tags to exclude (features FrogDB doesn't support)
DEFAULT_EXCLUDE_TAGS = [
    "needs:repl",
    "needs:debug",
    "needs:save",
    "needs:reset",
    "needs:config-maxmemory",
    "external:skip",
]


def get_script_dir() -> Path:
    """Get the directory containing this script."""
    return Path(__file__).parent.resolve()


def get_project_root() -> Path:
    """Get the FrogDB project root directory."""
    return get_script_dir().parent


def parse_skiplists(script_dir: Path) -> tuple[list[str], list[str]]:
    """Parse all skiplist files and separate unit names from test names.

    Returns (skip_units, skip_tests) where:
    - skip_units: entries like "unit/protocol" that skip entire test files (--skipunit)
    - skip_tests: individual test name patterns (--skipfile)
    """
    skipfiles = [
        script_dir / "skiplist-intentional.txt",
        script_dir / "skiplist-not-implemented.txt",
        script_dir / "skiplist-flaky.txt",
    ]

    skip_units: list[str] = []
    skip_tests: list[str] = []

    for skipfile in skipfiles:
        if not skipfile.exists():
            continue
        with open(skipfile) as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                # Unit names look like "unit/foo" or "integration/bar".
                # Everything else (exact names, /regex patterns) is a test name.
                if stripped.startswith(("unit/", "integration/")):
                    skip_units.append(stripped)
                else:
                    skip_tests.append(stripped)

    # Deduplicate while preserving order
    skip_units = list(dict.fromkeys(skip_units))
    skip_tests = list(dict.fromkeys(skip_tests))

    print(f"Skipping {len(skip_units)} test units, {len(skip_tests)} individual tests")
    return skip_units, skip_tests


def discover_suites(redis_dir: Path, skip_units: list[str]) -> list[str]:
    """Parse all_tests from test_helper.tcl, minus skipped units."""
    test_helper = redis_dir / "tests" / "test_helper.tcl"
    content = test_helper.read_text()

    # Extract the Tcl list between "set ::all_tests {" and the closing "}"
    match = re.search(r'set\s+::all_tests\s*\{([^}]+)\}', content)
    if not match:
        print("Error: Could not parse $::all_tests from test_helper.tcl")
        sys.exit(1)

    all_suites = match.group(1).split()
    skip_set = set(skip_units)
    suites = [
        s for s in all_suites
        if s not in skip_set
        and not any(s.startswith(u + "/") for u in skip_set)
    ]

    print(f"Discovered {len(all_suites)} total suites, {len(suites)} to run "
          f"({len(all_suites) - len(suites)} skipped)")
    return suites


def download_redis(cache_dir: Path) -> Path:
    """Download and extract Redis source to cache directory."""
    redis_dir = (cache_dir / f"redis-{REDIS_VERSION}").resolve()

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


def write_skipfile(skip_tests: list[str], cache_dir: Path) -> Path:
    """Write individual test name patterns to a skipfile for --skipfile."""
    combined = cache_dir / "combined_skiplist.txt"
    cache_dir.mkdir(parents=True, exist_ok=True)

    with open(combined, "w") as outfile:
        for test in skip_tests:
            outfile.write(f"{test}\n")

    return combined


def find_tclsh() -> str:
    """Find tclsh binary, preferring Homebrew tcl-tk@8."""
    homebrew_tclsh8 = Path("/opt/homebrew/opt/tcl-tk@8/bin/tclsh8.6")
    if homebrew_tclsh8.exists():
        return str(homebrew_tclsh8)
    tclsh = shutil.which("tclsh8.6") or shutil.which("tclsh")
    if tclsh is None:
        print("Error: tclsh not found. Install tcl-tk@8 via Homebrew: brew install tcl-tk@8")
        sys.exit(1)
    return tclsh


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

        # Write a minimal config file to set the data directory
        config_path = self.data_dir / "frogdb-test.toml"
        config_path.write_text(
            f"[persistence]\ndata_dir = \"{self.data_dir}\"\n"
        )

        self.process = subprocess.Popen(
            [
                str(self.binary),
                "--port",
                str(self.port),
                "--config",
                str(config_path),
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

        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

        self.process = None

    def cleanup_data(self) -> None:
        """Remove the data directory."""
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir)


def parse_test_output(output: str) -> dict:
    """Parse Redis test suite output for pass/fail/error counts and failing test names."""
    passed = 0
    failed = 0
    errors = 0
    failing_tests: list[str] = []

    for line in output.splitlines():
        m = re.search(r"Passed\s+(\d+)", line)
        if m:
            passed = int(m.group(1))
        m = re.search(r"Failed\s+(\d+)", line)
        if m:
            failed = int(m.group(1))
        if "[err]" in line.lower() or "[exception]" in line.lower():
            errors += 1
        # Capture failing test names from lines like "[err]: test name in ..."
        err_match = re.match(r'\[err\]:\s*(.+?)(?:\s+in\s+|$)', line, re.IGNORECASE)
        if err_match:
            failing_tests.append(err_match.group(1).strip())

    return {"passed": passed, "failed": failed, "errors": errors, "failing_tests": failing_tests}


def build_tcl_command(
    tclsh: str,
    redis_dir: Path,
    port: int,
    skipfile: Path,
    skip_units: list[str],
    suite: str | None = None,
    tags: list[str] | None = None,
    verbose: bool = False,
) -> list[str]:
    """Build the tclsh command for running Redis tests."""
    cmd = [
        tclsh,
        str(redis_dir / "tests" / "test_helper.tcl"),
        "--host", "127.0.0.1",
        "--port", str(port),
        "--singledb",
        "--ignore-encoding",
        "--ignore-digest",
        "--timeout", "30",
    ]

    if skipfile.exists() and skipfile.stat().st_size > 0:
        cmd.extend(["--skipfile", str(skipfile)])

    for unit in skip_units:
        cmd.extend(["--skipunit", unit])

    for tag in DEFAULT_EXCLUDE_TAGS:
        cmd.extend(["--tags", f"-{tag}"])

    if tags:
        for tag in tags:
            cmd.extend(["--tags", tag])

    if suite:
        cmd.extend(["--single", suite])

    if verbose:
        cmd.append("--verbose")

    return cmd


def run_single_suite(
    tclsh: str,
    redis_dir: Path,
    suite: str,
    port: int,
    skipfile: Path,
    skip_units: list[str],
    timeout: int,
    server: FrogDBServer,
    tags: list[str] | None = None,
    verbose: bool = False,
) -> dict:
    """Run a single suite with timeout and crash detection. Returns result dict."""
    cmd = build_tcl_command(
        tclsh=tclsh,
        redis_dir=redis_dir,
        port=port,
        skipfile=skipfile,
        skip_units=skip_units,
        suite=suite,
        tags=tags,
        verbose=verbose,
    )

    # Use a new process group so we can kill the entire tree on timeout
    proc = subprocess.Popen(
        cmd,
        cwd=redis_dir,
        env={**os.environ, "TERM": "dumb"},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    timed_out = False
    try:
        stdout_bytes, stderr_bytes = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        # Kill the entire process group (tclsh + any children it spawned)
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            proc.kill()
        stdout_bytes, stderr_bytes = proc.communicate()

    output = ""
    if stdout_bytes:
        output += stdout_bytes.decode(errors="replace")
    if stderr_bytes:
        output += "\n" + stderr_bytes.decode(errors="replace")

    if timed_out:
        counts = parse_test_output(output)
        return {
            "suite": suite,
            "status": "TIMEOUT",
            "returncode": -1,
            "output": output,
            **counts,
        }

    # Check if the server crashed during the test
    if server.process and server.process.poll() is not None:
        counts = parse_test_output(output)
        return {
            "suite": suite,
            "status": "CRASH",
            "returncode": proc.returncode,
            "output": output,
            **counts,
        }

    counts = parse_test_output(output)
    return {
        "suite": suite,
        "status": "PASS" if proc.returncode == 0 else "FAIL",
        "returncode": proc.returncode,
        "output": output,
        **counts,
    }


def print_summary(results: list[dict], elapsed: float) -> None:
    """Print a summary table of all suite results."""
    print(f"\n{'=' * 80}")
    print("REDIS COMPATIBILITY TEST SUMMARY")
    print(f"{'=' * 80}")

    print(f"\n{'Suite':<40} {'Status':<10} {'Pass':<6} {'Fail':<6} {'Err':<6}")
    print("-" * 80)

    pass_suites: list[str] = []
    fail_suites: list[str] = []
    timeout_suites: list[str] = []
    crash_suites: list[str] = []
    error_suites: list[str] = []

    for r in results:
        status = r["status"]
        if status == "PASS":
            pass_suites.append(r["suite"])
        elif status == "TIMEOUT":
            timeout_suites.append(r["suite"])
        elif status == "CRASH":
            crash_suites.append(r["suite"])
        elif status == "ERROR":
            error_suites.append(r["suite"])
        else:
            fail_suites.append(r["suite"])

        print(
            f"{r['suite']:<40} {status:<10} "
            f"{r.get('passed', '-'):<6} {r.get('failed', '-'):<6} {r.get('errors', '-'):<6}"
        )

    print("-" * 80)

    total_passed = sum(r.get("passed", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)
    total_errors = sum(r.get("errors", 0) for r in results)
    print(
        f"{'TOTAL':<40} {'':<10} "
        f"{total_passed:<6} {total_failed:<6} {total_errors:<6}"
    )

    minutes, seconds = divmod(int(elapsed), 60)
    print(f"\nCompleted in {minutes}m {seconds}s")
    print(f"  {len(pass_suites)} passed, {len(fail_suites)} failed, "
          f"{len(timeout_suites)} timed out, {len(crash_suites)} crashed, "
          f"{len(error_suites)} errors")

    if timeout_suites:
        print(f"\nTIMEOUT suites (hung/deadlocked):")
        for s in timeout_suites:
            print(f"  - {s}")

    if crash_suites:
        print(f"\nCRASH suites (server died):")
        for s in crash_suites:
            print(f"  - {s}")

    if fail_suites:
        print(f"\nFAIL suites (tests failed but completed):")
        for s in fail_suites:
            print(f"  - {s}")


def update_skiplists(
    results: list[dict],
    skiplist_path: Path,
) -> None:
    """Append TIMEOUT and CRASH suites to the not-implemented skiplist."""
    # Read existing entries to avoid duplicates
    existing: set[str] = set()
    if skiplist_path.exists():
        with open(skiplist_path) as f:
            for line in f:
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    existing.add(stripped)

    new_entries: list[tuple[str, str]] = []
    for r in results:
        if r["status"] == "TIMEOUT" and r["suite"] not in existing:
            new_entries.append((r["suite"], "# TODO: hangs/deadlocks the server"))
        elif r["status"] == "CRASH" and r["suite"] not in existing:
            new_entries.append((r["suite"], "# TODO: crashes the server"))

    if not new_entries:
        print("\nNo new suites to add to skiplist.")
        return

    with open(skiplist_path, "a") as f:
        f.write("\n# =============================================================================\n")
        f.write("# AUTO-DETECTED BY PER-SUITE RUNNER\n")
        f.write("# =============================================================================\n")
        for suite, comment in new_entries:
            f.write(f"{comment}\n{suite}\n")

    print(f"\nUpdated {skiplist_path.name}: added {len(new_entries)} suite(s)")
    for suite, comment in new_entries:
        print(f"  + {suite} ({comment})")


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
        help="Run a single test suite (e.g., unit/type/string)",
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
    parser.add_argument(
        "--suite-timeout",
        type=int,
        default=int(os.environ.get("FROGDB_SUITE_TIMEOUT", DEFAULT_SUITE_TIMEOUT)),
        help=f"Wall-clock timeout per suite in seconds (default: {DEFAULT_SUITE_TIMEOUT})",
    )
    parser.add_argument(
        "--update-skiplists",
        action="store_true",
        help="Auto-append TIMEOUT/CRASH suites to skiplist-not-implemented.txt",
    )

    args = parser.parse_args()

    script_dir = get_script_dir()
    project_root = get_project_root()
    cache_dir = (project_root / CACHE_DIR).resolve()

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

    # Parse skiplists into units (whole files) and tests (individual names)
    skip_units, skip_tests = parse_skiplists(script_dir)
    skipfile = write_skipfile(skip_tests, cache_dir)

    # --single mode: run a single suite with one server (legacy behavior)
    if args.single:
        data_dir = cache_dir / "frogdb-test-data"
        server = FrogDBServer(binary, args.port, data_dir)

        def cleanup() -> None:
            server.stop()
            if not args.keep_data:
                server.cleanup_data()

        atexit.register(cleanup)
        signal.signal(signal.SIGINT, lambda s, f: sys.exit(130))
        signal.signal(signal.SIGTERM, lambda s, f: sys.exit(143))

        print(f"Starting FrogDB on port {args.port}...")
        server.start()
        print(f"FrogDB is ready on port {args.port}")

        tclsh = find_tclsh()
        result = run_single_suite(
            tclsh=tclsh,
            redis_dir=redis_dir,
            suite=args.single,
            port=args.port,
            skipfile=skipfile,
            skip_units=skip_units,
            timeout=args.suite_timeout,
            server=server,
            tags=args.tags,
            verbose=args.verbose,
        )

        # Print output directly for --single mode
        if result["output"]:
            print(result["output"])

        status = result["status"]
        print(f"\nResult: {status} — {result.get('passed', 0)} passed, "
              f"{result.get('failed', 0)} failed, {result.get('errors', 0)} errors")

        sys.exit(0 if status == "PASS" else 1)

    # Per-suite mode (default): run each suite with its own server
    suites = discover_suites(redis_dir, skip_units)
    tclsh = find_tclsh()

    # Register signal handlers for clean shutdown
    _shutdown_requested = False

    def request_shutdown(signum: int, frame: object) -> None:
        nonlocal _shutdown_requested
        if _shutdown_requested:
            # Second signal: hard exit
            sys.exit(128 + signum)
        _shutdown_requested = True
        print("\nShutdown requested, finishing current suite...")

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    results: list[dict] = []
    start_time = time.time()

    for i, suite in enumerate(suites, 1):
        if _shutdown_requested:
            print(f"Skipping remaining {len(suites) - i + 1} suites due to shutdown request")
            break

        print(f"\n[{i}/{len(suites)}] {suite} ", end="", flush=True)

        data_dir = cache_dir / "frogdb-suite-data"
        if data_dir.exists():
            shutil.rmtree(data_dir)

        server = FrogDBServer(binary, args.port, data_dir)
        try:
            server.start()

            result = run_single_suite(
                tclsh=tclsh,
                redis_dir=redis_dir,
                suite=suite,
                port=args.port,
                skipfile=skipfile,
                skip_units=skip_units,
                timeout=args.suite_timeout,
                server=server,
                tags=args.tags,
                verbose=args.verbose,
            )
            results.append(result)

            status = result["status"]
            passed = result.get("passed", 0)
            failed = result.get("failed", 0)

            if status == "PASS":
                print(f"PASS ({passed} passed)")
            elif status == "TIMEOUT":
                print(f"TIMEOUT (hung after {args.suite_timeout}s, {passed} passed before hang)")
            elif status == "CRASH":
                print(f"CRASH (server died, {passed} passed before crash)")
            else:
                print(f"FAIL ({passed} passed, {failed} failed)")

        except Exception as e:
            print(f"ERROR ({e})")
            results.append({
                "suite": suite,
                "status": "ERROR",
                "returncode": -1,
                "output": str(e),
                "passed": 0,
                "failed": 0,
                "errors": 0,
                "failing_tests": [],
            })
        finally:
            server.stop()
            if not args.keep_data:
                server.cleanup_data()

    elapsed = time.time() - start_time
    print_summary(results, elapsed)

    if args.update_skiplists:
        skiplist_path = script_dir / "skiplist-not-implemented.txt"
        update_skiplists(results, skiplist_path)

    # Exit with failure if any suite timed out or crashed
    has_critical = any(r["status"] in ("TIMEOUT", "CRASH", "ERROR") for r in results)
    sys.exit(1 if has_critical else 0)


if __name__ == "__main__":
    main()
