#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Jepsen Test Orchestration Script for FrogDB

Manages Docker Compose lifecycle, test execution, and result reporting
across single-node, replication, and Raft cluster topologies.
"""

import argparse
import difflib
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


# =============================================================================
# Data Model
# =============================================================================


class Topology(Enum):
    SINGLE = "single"
    REPLICATION = "replication"
    RAFT = "raft"


@dataclass(frozen=True)
class TopologyConfig:
    compose_file: str  # Relative to project root
    services: tuple[str, ...]  # Empty = start all services


TOPOLOGY_CONFIGS: dict[Topology, TopologyConfig] = {
    Topology.SINGLE: TopologyConfig("jepsen/docker-compose.yml", ("n1",)),
    Topology.REPLICATION: TopologyConfig(
        "jepsen/frogdb/docker-compose.replication.yml", ()
    ),
    Topology.RAFT: TopologyConfig(
        "jepsen/frogdb/docker-compose.raft-cluster.yml", ()
    ),
}


@dataclass(frozen=True)
class TestDefinition:
    name: str
    workload: str
    nemesis: str
    time_limit: int
    topology: Topology
    cluster_flag: bool = False
    suites: tuple[str, ...] = ()


TESTS: tuple[TestDefinition, ...] = (
    # Single-node basic workloads
    TestDefinition("register", "register", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("counter", "counter", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("append", "append", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("transaction", "transaction", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("queue", "queue", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("set", "set", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("hash", "hash", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("sortedset", "sortedset", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("expiry", "expiry", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("blocking", "blocking", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    # Single-node crash workloads
    TestDefinition("crash", "register", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("counter-crash", "counter", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("append-crash", "append", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("append-rapid", "append", "rapid-kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("transaction-crash", "transaction", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("sortedset-crash", "sortedset", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("expiry-crash", "expiry", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("expiry-rapid", "expiry", "rapid-kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition("blocking-crash", "blocking", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    # Single-node nemesis (standalone)
    TestDefinition("nemesis-pause", "register", "pause", 60, Topology.SINGLE),
    # Replication workloads
    TestDefinition("replication", "replication", "none", 30, Topology.REPLICATION, suites=("replication", "all")),
    TestDefinition("lag", "lag", "none", 30, Topology.REPLICATION, suites=("replication", "all")),
    TestDefinition("split-brain", "split-brain", "partition", 60, Topology.REPLICATION, suites=("replication", "all")),
    TestDefinition("zombie", "zombie", "partition", 60, Topology.REPLICATION, suites=("replication", "all")),
    TestDefinition("replication-chaos", "replication", "all-replication", 120, Topology.REPLICATION, suites=("replication", "all")),
    # Raft cluster core workloads
    TestDefinition("cluster-formation", "cluster-formation", "none", 30, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("leader-election", "leader-election", "none", 30, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("slot-migration", "slot-migration", "none", 60, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("cross-slot", "cross-slot", "none", 30, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("key-routing", "key-routing", "none", 30, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("leader-election-partition", "leader-election", "partition", 60, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("key-routing-kill", "key-routing", "kill", 60, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("slot-migration-partition", "slot-migration", "partition", 90, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    TestDefinition("raft-chaos", "key-routing", "raft-cluster", 120, Topology.RAFT, cluster_flag=True, suites=("raft", "all")),
    # Raft extended nemesis tests
    TestDefinition("clock-skew", "register", "clock-skew", 60, Topology.RAFT, cluster_flag=True, suites=("raft-extended",)),
    TestDefinition("disk-failure", "register", "disk-failure", 60, Topology.RAFT, cluster_flag=True, suites=("raft-extended",)),
    TestDefinition("slow-network", "register", "slow-network", 60, Topology.RAFT, cluster_flag=True, suites=("raft-extended",)),
    TestDefinition("memory-pressure", "register", "memory-pressure", 60, Topology.RAFT, cluster_flag=True, suites=("raft-extended",)),
)

TESTS_BY_NAME: dict[str, TestDefinition] = {t.name: t for t in TESTS}
SUITE_NAMES: tuple[str, ...] = tuple(sorted({s for t in TESTS for s in t.suites}))


# =============================================================================
# Output
# =============================================================================


class Color:
    """ANSI color codes, auto-disabled when not a TTY."""

    def __init__(self, *, enabled: bool | None = None):
        if enabled is None:
            enabled = sys.stdout.isatty()
        self.enabled = enabled

    def _wrap(self, code: str, text: str) -> str:
        if not self.enabled:
            return text
        return f"\033[{code}m{text}\033[0m"

    def green(self, text: str) -> str:
        return self._wrap("32", text)

    def red(self, text: str) -> str:
        return self._wrap("31", text)

    def yellow(self, text: str) -> str:
        return self._wrap("33", text)

    def bold(self, text: str) -> str:
        return self._wrap("1", text)

    def dim(self, text: str) -> str:
        return self._wrap("2", text)


# =============================================================================
# Helpers
# =============================================================================


def get_project_root() -> Path:
    """Get the FrogDB project root (parent of jepsen/)."""
    return Path(__file__).resolve().parent.parent


def preflight(c: Color) -> None:
    """Validate prerequisites (Docker running, lein on PATH)."""
    ok = True
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        if result.returncode != 0:
            raise RuntimeError
    except (FileNotFoundError, RuntimeError, subprocess.TimeoutExpired):
        print(c.red("Error: Docker is not running."))
        print("  Start Docker Desktop or the Docker daemon and try again.")
        ok = False

    if not shutil.which("lein"):
        print(c.red("Error: lein (Leiningen) not found on PATH."))
        print("  Install: https://leiningen.org/#install")
        ok = False

    if not ok:
        sys.exit(1)


# =============================================================================
# Docker Compose
# =============================================================================


COMPOSE_PROJECT = "jepsen-frogdb"


def compose_cmd(root: Path, topology: Topology) -> list[str]:
    """Base docker compose command for a topology."""
    cfg = TOPOLOGY_CONFIGS[topology]
    return [
        "docker", "compose",
        "-p", COMPOSE_PROJECT,
        "-f", str(root / cfg.compose_file),
    ]


def compose_is_up(root: Path, topology: Topology) -> bool:
    """Check if a topology's containers are already running."""
    cmd = compose_cmd(root, topology) + ["ps", "--status=running", "-q"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0 and bool(result.stdout.strip())


def compose_up(root: Path, topology: Topology, c: Color) -> None:
    """Start containers for a topology and wait for healthy."""
    if compose_is_up(root, topology):
        print(c.dim(f"{topology.value} topology is already running."))
        return
    cfg = TOPOLOGY_CONFIGS[topology]
    cmd = compose_cmd(root, topology) + ["up", "-d", "--wait"]
    if cfg.services:
        cmd.extend(cfg.services)
    print(c.dim(f"Starting {topology.value} topology..."))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(c.red(f"Failed to start {topology.value} topology:"))
        if result.stderr:
            print(result.stderr)
        sys.exit(1)
    print(c.green(f"{topology.value} topology is up and healthy."))


def compose_down(root: Path, topology: Topology, c: Color) -> None:
    """Tear down containers for a topology."""
    cmd = compose_cmd(root, topology) + ["down", "-v"]
    print(c.dim(f"Tearing down {topology.value} topology..."))
    subprocess.run(cmd, capture_output=True)


# =============================================================================
# Test Execution
# =============================================================================


@dataclass
class TestResult:
    test: TestDefinition
    passed: bool | None  # None = not run / ambiguous
    elapsed: float
    error: str | None = None


def run_test(
    root: Path,
    test: TestDefinition,
    time_limit: int | None,
    extra_args: list[str],
    c: Color,
) -> TestResult:
    """Run a single Jepsen test and return the result with verdict."""
    tl = time_limit if time_limit is not None else test.time_limit
    cmd = [
        "lein", "run", "test",
        "--docker",
        "--workload", test.workload,
        "--nemesis", test.nemesis,
        "--time-limit", str(tl),
    ]
    if test.cluster_flag:
        cmd.append("--cluster")
    cmd.extend(extra_args)

    cwd = root / "jepsen" / "frogdb"

    print()
    print(c.bold(f"  Test: {test.name}"))
    print(c.dim(f"  workload={test.workload}  nemesis={test.nemesis}  time-limit={tl}"))
    print(c.dim(f"  {' '.join(cmd)}"))
    print()

    start = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        tail_lines: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            tail_lines.append(line)
            if len(tail_lines) > 50:
                tail_lines.pop(0)

        proc.wait()
        elapsed = time.monotonic() - start

        tail_text = "".join(tail_lines)
        if "Everything looks good!" in tail_text:
            return TestResult(test, True, elapsed)
        elif "Analysis invalid!" in tail_text:
            return TestResult(test, False, elapsed)
        elif proc.returncode != 0:
            return TestResult(test, False, elapsed, error=f"exit code {proc.returncode}")
        else:
            return TestResult(test, None, elapsed, error="no verdict detected")

    except FileNotFoundError:
        return TestResult(test, None, time.monotonic() - start, error="lein not found")


def generate_batch_edn(tests: list[TestDefinition], time_limit: int | None) -> str:
    """Generate an EDN batch file for test-all command. Returns the temp file path."""
    configs = []
    for t in tests:
        tl = time_limit if time_limit is not None else t.time_limit
        pairs = [
            f':workload "{t.workload}"',
            f':nemesis "{t.nemesis}"',
            f":time-limit {tl}",
        ]
        if t.cluster_flag:
            pairs.append(":cluster true")
        configs.append("{" + " ".join(pairs) + "}")

    edn = "[" + "\n ".join(configs) + "]"

    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".edn", prefix="jepsen-batch-", delete=False,
    )
    f.write(edn)
    f.close()
    return f.name


def run_test_batch(
    root: Path,
    tests: list[TestDefinition],
    time_limit: int | None,
    extra_args: list[str],
    c: Color,
    *,
    stop_on_failure: bool = False,
) -> list[TestResult]:
    """Run multiple Jepsen tests in a single JVM via the test-all command."""
    batch_path = generate_batch_edn(tests, time_limit)

    cmd = [
        "lein", "run", "test-all",
        "--docker",
        "--batch-file", batch_path,
    ]
    cmd.extend(extra_args)

    cwd = root / "jepsen" / "frogdb"

    print()
    print(c.bold(f"  Batch: {len(tests)} tests in single JVM"))
    for t in tests:
        tl = time_limit if time_limit is not None else t.time_limit
        print(c.dim(f"    {t.name}: workload={t.workload} nemesis={t.nemesis} time-limit={tl}"))
    print(c.dim(f"  {' '.join(cmd)}"))
    print()

    results: list[TestResult] = []
    verdict_idx = 0
    start = time.monotonic()
    test_start = start

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()

            if verdict_idx < len(tests):
                if "Everything looks good!" in line:
                    now = time.monotonic()
                    results.append(TestResult(tests[verdict_idx], True, now - test_start))
                    test_start = now
                    verdict_idx += 1
                elif "Analysis invalid!" in line:
                    now = time.monotonic()
                    results.append(TestResult(tests[verdict_idx], False, now - test_start))
                    test_start = now
                    verdict_idx += 1
                    if stop_on_failure:
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break

        proc.wait()

        # Mark any tests without verdicts as unknown
        now = time.monotonic()
        while verdict_idx < len(tests):
            results.append(
                TestResult(tests[verdict_idx], None, now - test_start, error="no verdict detected")
            )
            test_start = now
            verdict_idx += 1

    except FileNotFoundError:
        for t in tests[verdict_idx:]:
            results.append(TestResult(t, None, time.monotonic() - start, error="lein not found"))
    finally:
        try:
            Path(batch_path).unlink(missing_ok=True)
        except OSError:
            pass

    return results


def resolve_tests(
    test_name: str | None,
    suite_name: str | None,
    c: Color,
) -> list[TestDefinition]:
    """Resolve a test name or suite name to a list of TestDefinitions."""
    if suite_name:
        if suite_name not in SUITE_NAMES:
            close = difflib.get_close_matches(suite_name, SUITE_NAMES, n=3, cutoff=0.4)
            print(c.red(f"Unknown suite: {suite_name}"))
            if close:
                print(f"  Did you mean: {', '.join(close)}?")
            print(f"  Available suites: {', '.join(SUITE_NAMES)}")
            sys.exit(1)
        return [t for t in TESTS if suite_name in t.suites]

    if test_name:
        if test_name in TESTS_BY_NAME:
            return [TESTS_BY_NAME[test_name]]
        close = difflib.get_close_matches(test_name, TESTS_BY_NAME.keys(), n=3, cutoff=0.4)
        print(c.red(f"Unknown test: {test_name}"))
        if close:
            print(f"  Did you mean: {', '.join(close)}?")
        print("  Run 'jepsen/run.py list' to see all tests.")
        sys.exit(1)

    print(c.red("Specify a test name or --suite."))
    sys.exit(1)


def format_elapsed(seconds: float) -> str:
    """Format elapsed seconds as mm:ss."""
    m, s = divmod(int(seconds), 60)
    return f"{m}:{s:02d}"


def print_summary(results: list[TestResult], c: Color) -> None:
    """Print a summary table of test results."""
    if not results:
        return

    print()
    print(c.bold("=" * 72))
    print(c.bold("  Summary"))
    print(c.bold("=" * 72))

    name_w = max(len(r.test.name) for r in results)
    topo_w = max(len(r.test.topology.value) for r in results)

    for r in results:
        if r.passed is True:
            verdict = c.green("PASS")
        elif r.passed is False:
            verdict = c.red("FAIL")
        else:
            verdict = c.yellow("UNKNOWN")

        elapsed = format_elapsed(r.elapsed)
        err = f"  ({r.error})" if r.error else ""
        print(
            f"  {r.test.name:<{name_w}}  {r.test.topology.value:<{topo_w}}  "
            f"{elapsed}  {verdict}{err}"
        )

    passed = sum(1 for r in results if r.passed is True)
    failed = sum(1 for r in results if r.passed is False)
    unknown = sum(1 for r in results if r.passed is None)
    total = len(results)
    total_time = format_elapsed(sum(r.elapsed for r in results))

    print()
    parts = [f"{total} tests"]
    if passed:
        parts.append(c.green(f"{passed} passed"))
    if failed:
        parts.append(c.red(f"{failed} failed"))
    if unknown:
        parts.append(c.yellow(f"{unknown} unknown"))
    parts.append(f"in {total_time}")
    print(f"  {', '.join(parts)}")
    print()


# =============================================================================
# Subcommands
# =============================================================================


def cmd_run(args: argparse.Namespace, extra_args: list[str]) -> None:
    c = Color(enabled=False if args.no_color else None)
    root = get_project_root()
    preflight(c)

    if args.build:
        print(c.bold("Building FrogDB for Jepsen..."))
        result = subprocess.run(["just", "cross-build"], cwd=root)
        if result.returncode != 0:
            print(c.red("Cross-build failed."))
            sys.exit(1)
        result = subprocess.run(
            ["docker", "build", "-t", "frogdb:latest", "."], cwd=root
        )
        if result.returncode != 0:
            print(c.red("Docker build failed."))
            sys.exit(1)

    tests = resolve_tests(args.test, args.suite, c)

    # Group by topology, process in order: single -> replication -> raft
    topo_order = [Topology.SINGLE, Topology.REPLICATION, Topology.RAFT]
    groups: dict[Topology, list[TestDefinition]] = {}
    for t in tests:
        groups.setdefault(t.topology, []).append(t)

    results: list[TestResult] = []
    total = len(tests)
    idx = 0
    active_topology: Topology | None = None

    try:
        for topo in topo_order:
            if topo not in groups:
                continue

            # Tear down previous topology before switching (port conflicts)
            if active_topology is not None and active_topology != topo:
                compose_down(root, active_topology, c)
                active_topology = None

            if active_topology != topo:
                compose_up(root, topo, c)
                active_topology = topo

            group_tests = groups[topo]
            if len(group_tests) == 1:
                test = group_tests[0]
                idx += 1
                print(c.bold(f"\n{'=' * 72}"))
                print(c.bold(f"  [{idx}/{total}] {test.name}"))
                print(c.bold(f"{'=' * 72}"))

                result = run_test(root, test, args.time_limit, extra_args, c)
                results.append(result)

                if result.passed is True:
                    print(c.green(f"\n  {test.name}: PASS ({format_elapsed(result.elapsed)})"))
                elif result.passed is False:
                    print(c.red(f"\n  {test.name}: FAIL ({format_elapsed(result.elapsed)})"))
                else:
                    print(c.yellow(f"\n  {test.name}: UNKNOWN ({format_elapsed(result.elapsed)})"))
            else:
                print(c.bold(f"\n{'=' * 72}"))
                print(c.bold(
                    f"  [{idx + 1}-{idx + len(group_tests)}/{total}]"
                    f" {topo.value} batch ({len(group_tests)} tests)"
                ))
                print(c.bold(f"{'=' * 72}"))

                batch_results = run_test_batch(
                    root, group_tests, args.time_limit, extra_args, c,
                    stop_on_failure=args.stop_on_failure,
                )

                for result in batch_results:
                    idx += 1
                    if result.passed is True:
                        print(c.green(f"\n  {result.test.name}: PASS ({format_elapsed(result.elapsed)})"))
                    elif result.passed is False:
                        print(c.red(f"\n  {result.test.name}: FAIL ({format_elapsed(result.elapsed)})"))
                    else:
                        print(c.yellow(f"\n  {result.test.name}: UNKNOWN ({format_elapsed(result.elapsed)})"))
                results.extend(batch_results)

            if args.stop_on_failure and any(r.passed is False for r in results):
                break

    except KeyboardInterrupt:
        print(c.yellow("\n\nInterrupted."))
    finally:
        if args.teardown and active_topology is not None:
            compose_down(root, active_topology, c)

    print_summary(results, c)

    if any(r.passed is False for r in results):
        sys.exit(1)
    if any(r.passed is None for r in results):
        sys.exit(2)


def cmd_list(args: argparse.Namespace) -> None:
    c = Color()
    topology_filter = args.topology

    print(c.bold("Suites:"))
    for suite in SUITE_NAMES:
        count = sum(1 for t in TESTS if suite in t.suites)
        print(f"  {suite:<16} ({count} tests)")
    print()

    print(c.bold("Tests:"))
    name_w = max(len(t.name) for t in TESTS)
    wl_w = max(len(t.workload) for t in TESTS)
    nem_w = max(len(t.nemesis) for t in TESTS)

    for t in TESTS:
        if topology_filter and t.topology.value != topology_filter:
            continue
        cluster = " --cluster" if t.cluster_flag else ""
        print(
            f"  {t.name:<{name_w}}  {t.topology.value:<12}  "
            f"{t.workload:<{wl_w}}  {t.nemesis:<{nem_w}}  "
            f"{t.time_limit:>3}s{cluster}"
        )


def cmd_build(args: argparse.Namespace) -> None:
    root = get_project_root()
    c = Color()

    print(c.bold("Cross-compiling FrogDB..."))
    result = subprocess.run(["just", "cross-build"], cwd=root)
    if result.returncode != 0:
        print(c.red("Cross-build failed."))
        sys.exit(1)

    print(c.bold("Building Docker image..."))
    result = subprocess.run(
        ["docker", "build", "-t", "frogdb:latest", "."], cwd=root
    )
    if result.returncode != 0:
        print(c.red("Docker build failed."))
        sys.exit(1)

    print(c.green("Build complete."))


def cmd_up(args: argparse.Namespace) -> None:
    root = get_project_root()
    c = Color()
    topo = Topology(args.topology)
    compose_up(root, topo, c)


def cmd_down(args: argparse.Namespace) -> None:
    root = get_project_root()
    c = Color()
    if args.topology:
        compose_down(root, Topology(args.topology), c)
    else:
        for topo in Topology:
            compose_down(root, topo, c)


def cmd_clean(args: argparse.Namespace) -> None:
    root = get_project_root()
    store = root / "jepsen" / "frogdb" / "store"
    if store.exists():
        shutil.rmtree(store)
        print(f"Removed {store}")
    else:
        print("Nothing to clean.")


def cmd_results(args: argparse.Namespace) -> None:
    root = get_project_root()
    latest = root / "jepsen" / "frogdb" / "store" / "latest"
    if not latest.exists():
        print("No results found. Run a test first.")
        sys.exit(1)
    if platform.system() == "Darwin":
        subprocess.run(["open", str(latest)])
    elif platform.system() == "Linux":
        subprocess.run(["xdg-open", str(latest)])
    else:
        print(f"Results at: {latest}")


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Jepsen test orchestration for FrogDB",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- run ---
    p_run = sub.add_parser("run", help="Run Jepsen tests")
    p_run.add_argument("test", nargs="?", help="Test name to run")
    p_run.add_argument("--suite", help="Run a predefined test suite")
    p_run.add_argument(
        "--build", action="store_true", default=False,
        help="Build before running",
    )
    p_run.add_argument("--no-build", dest="build", action="store_false")
    p_run.add_argument(
        "--teardown", action="store_true", default=False,
        help="Tear down compose after running",
    )
    p_run.add_argument("--no-teardown", dest="teardown", action="store_false")
    p_run.add_argument("--time-limit", type=int, help="Override default time-limit")
    p_run.add_argument(
        "--stop-on-failure", action="store_true",
        help="Abort suite on first failure",
    )
    p_run.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    p_run.add_argument("-v", "--verbose", action="store_true", help="Reserved")

    # --- list ---
    p_list = sub.add_parser("list", help="List available tests and suites")
    p_list.add_argument(
        "--topology", choices=[t.value for t in Topology],
        help="Filter by topology",
    )

    # --- build ---
    sub.add_parser("build", help="Cross-compile and build Docker image")

    # --- up ---
    p_up = sub.add_parser("up", help="Start Docker Compose topology")
    p_up.add_argument("topology", choices=[t.value for t in Topology])

    # --- down ---
    p_down = sub.add_parser("down", help="Tear down Docker Compose topology")
    p_down.add_argument(
        "topology", nargs="?", choices=[t.value for t in Topology],
        help="Omit to tear down all",
    )

    # --- clean ---
    sub.add_parser("clean", help="Remove Jepsen test results")

    # --- results ---
    sub.add_parser("results", help="Open latest results in browser")

    # Use parse_known_args so extra flags pass through to lein
    args, extra = parser.parse_known_args()

    handlers = {
        "run": lambda: cmd_run(args, extra),
        "list": lambda: cmd_list(args),
        "build": lambda: cmd_build(args),
        "up": lambda: cmd_up(args),
        "down": lambda: cmd_down(args),
        "clean": lambda: cmd_clean(args),
        "results": lambda: cmd_results(args),
    }
    handlers[args.command]()


if __name__ == "__main__":
    main()
