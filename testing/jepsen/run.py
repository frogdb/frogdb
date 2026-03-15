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
import hashlib
import os
import platform
import re
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
    node_count: int  # Number of nodes in the topology
    has_bus_ports: bool = False  # Whether nodes expose cluster bus ports


DEFAULT_BASE_PORT = 16379

# Default base ports for parallel mode (non-conflicting ranges)
PARALLEL_BASE_PORTS: dict["Topology", int] = {
    Topology.SINGLE: 16379,
    Topology.REPLICATION: 17379,
    Topology.RAFT: 18379,
}

TOPOLOGY_CONFIGS: dict[Topology, TopologyConfig] = {
    Topology.SINGLE: TopologyConfig("testing/jepsen/docker-compose.yml", ("n1",), node_count=1),
    Topology.REPLICATION: TopologyConfig(
        "testing/jepsen/frogdb/docker-compose.replication.yml", (), node_count=3
    ),
    Topology.RAFT: TopologyConfig(
        "testing/jepsen/frogdb/docker-compose.raft-cluster.yml",
        (),
        node_count=5,
        has_bus_ports=True,
    ),
}


def port_env_vars(topology: Topology, base_port: int) -> dict[str, str]:
    """Compute per-node port environment variables for Docker Compose."""
    cfg = TOPOLOGY_CONFIGS[topology]
    env: dict[str, str] = {"FROGDB_BASE_PORT": str(base_port)}
    for i in range(cfg.node_count):
        env[f"FROGDB_PORT_N{i + 1}"] = str(base_port + i)
        if cfg.has_bus_ports:
            env[f"FROGDB_BUS_PORT_N{i + 1}"] = str(base_port + 10000 + i)
    return env


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
    TestDefinition(
        "register", "register", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "counter", "counter", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "append", "append", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "transaction", "transaction", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "queue", "queue", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition("set", "set", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition("hash", "hash", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")),
    TestDefinition(
        "sortedset", "sortedset", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "expiry", "expiry", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    TestDefinition(
        "blocking", "blocking", "none", 30, Topology.SINGLE, suites=("single", "crash", "all")
    ),
    # Single-node crash workloads
    TestDefinition("crash", "register", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition(
        "counter-crash", "counter", "kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    TestDefinition("append-crash", "append", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition(
        "append-rapid", "append", "rapid-kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    TestDefinition(
        "transaction-crash", "transaction", "kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    TestDefinition(
        "sortedset-crash", "sortedset", "kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    TestDefinition("expiry-crash", "expiry", "kill", 60, Topology.SINGLE, suites=("crash", "all")),
    TestDefinition(
        "expiry-rapid", "expiry", "rapid-kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    TestDefinition(
        "blocking-crash", "blocking", "kill", 60, Topology.SINGLE, suites=("crash", "all")
    ),
    # Single-node nemesis (standalone)
    TestDefinition("nemesis-pause", "register", "pause", 60, Topology.SINGLE),
    # Replication workloads
    TestDefinition(
        "replication",
        "replication",
        "none",
        30,
        Topology.REPLICATION,
        suites=("replication", "all"),
    ),
    TestDefinition("lag", "lag", "none", 30, Topology.REPLICATION, suites=("replication", "all")),
    TestDefinition(
        "split-brain",
        "split-brain",
        "partition",
        60,
        Topology.REPLICATION,
        suites=("replication", "all"),
    ),
    TestDefinition(
        "zombie", "zombie", "partition", 60, Topology.REPLICATION, suites=("replication", "all")
    ),
    TestDefinition(
        "replication-chaos",
        "replication",
        "all-replication",
        120,
        Topology.REPLICATION,
        suites=("replication", "all"),
    ),
    # Raft cluster core workloads
    TestDefinition(
        "cluster-formation",
        "cluster-formation",
        "none",
        30,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "leader-election",
        "leader-election",
        "none",
        30,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "slot-migration",
        "slot-migration",
        "none",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "cross-slot",
        "cross-slot",
        "none",
        30,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "key-routing",
        "key-routing",
        "none",
        30,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "leader-election-partition",
        "leader-election",
        "partition",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "key-routing-kill",
        "key-routing",
        "kill",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "slot-migration-partition",
        "slot-migration",
        "partition",
        90,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    TestDefinition(
        "raft-chaos",
        "key-routing",
        "raft-cluster",
        120,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft", "all"),
    ),
    # Raft extended nemesis tests
    TestDefinition(
        "clock-skew",
        "register",
        "clock-skew",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft-extended",),
    ),
    TestDefinition(
        "disk-failure",
        "register",
        "disk-failure",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft-extended",),
    ),
    TestDefinition(
        "slow-network",
        "register",
        "slow-network",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft-extended",),
    ),
    TestDefinition(
        "memory-pressure",
        "register",
        "memory-pressure",
        60,
        Topology.RAFT,
        cluster_flag=True,
        suites=("raft-extended",),
    ),
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
    """Get the FrogDB project root (grandparent of testing/jepsen/)."""
    return Path(__file__).resolve().parent.parent.parent


STAMP_FILE = Path("/tmp/frogdb-jepsen-build-stamp")


def source_hash(root: Path) -> str:
    """Hash all Rust source files + Cargo manifests to detect changes."""
    h = hashlib.sha256()
    for pattern in [
        "frogdb-server/crates/**/*.rs",
        "frogdb-server/crates/**/Cargo.toml",
        "Cargo.toml",
        "Cargo.lock",
    ]:
        for f in sorted(root.glob(pattern)):
            h.update(f.read_bytes())
    return h.hexdigest()[:16]


def needs_rebuild(root: Path) -> bool:
    """Check if Rust source has changed since last Docker build."""
    current = source_hash(root)
    return not (STAMP_FILE.exists() and STAMP_FILE.read_text().strip() == current)


def record_build(root: Path) -> None:
    """Record the current source hash after a successful build."""
    STAMP_FILE.write_text(source_hash(root))


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


def compose_cmd(root: Path, topology: Topology, base_port: int = DEFAULT_BASE_PORT) -> list[str]:
    """Base docker compose command for a topology."""
    cfg = TOPOLOGY_CONFIGS[topology]
    # Use a per-port project name so parallel topologies don't clash
    project = (
        f"{COMPOSE_PROJECT}-{base_port}" if base_port != DEFAULT_BASE_PORT else COMPOSE_PROJECT
    )
    return [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(root / cfg.compose_file),
    ]


def compose_env(topology: Topology, base_port: int = DEFAULT_BASE_PORT) -> dict[str, str]:
    """Build the full environment for a compose subprocess."""
    env = dict(os.environ)
    env.update(port_env_vars(topology, base_port))
    return env


def compose_is_up(root: Path, topology: Topology, base_port: int = DEFAULT_BASE_PORT) -> bool:
    """Check if a topology's containers are already running."""
    cmd = compose_cmd(root, topology, base_port) + ["ps", "--status=running", "-q"]
    result = subprocess.run(
        cmd, capture_output=True, text=True, env=compose_env(topology, base_port)
    )
    return result.returncode == 0 and bool(result.stdout.strip())


def compose_up(
    root: Path, topology: Topology, c: Color, base_port: int = DEFAULT_BASE_PORT
) -> None:
    """Start containers for a topology and wait for healthy.

    Always uses --force-recreate so containers start with clean state
    (no leftover iptables rules, crashed processes, etc. from prior runs).
    """
    cfg = TOPOLOGY_CONFIGS[topology]
    cmd = compose_cmd(root, topology, base_port) + ["up", "-d", "--force-recreate", "--wait"]
    if cfg.services:
        cmd.extend(cfg.services)
    env = compose_env(topology, base_port)
    print(c.dim(f"Starting {topology.value} topology (base port {base_port})..."))
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print(c.red(f"Failed to start {topology.value} topology:"))
        if result.stderr:
            print(result.stderr)
        sys.exit(1)
    print(c.green(f"{topology.value} topology is up and healthy."))


def compose_down(
    root: Path, topology: Topology, c: Color, base_port: int = DEFAULT_BASE_PORT
) -> None:
    """Tear down containers for a topology."""
    cmd = compose_cmd(root, topology, base_port) + ["down", "-v"]
    env = compose_env(topology, base_port)
    print(c.dim(f"Tearing down {topology.value} topology..."))
    subprocess.run(cmd, capture_output=True, env=env)


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
    base_port: int = DEFAULT_BASE_PORT,
) -> TestResult:
    """Run a single Jepsen test and return the result with verdict."""
    tl = time_limit if time_limit is not None else test.time_limit
    cmd = [
        "lein",
        "run",
        "test",
        "--docker",
        "--workload",
        test.workload,
        "--nemesis",
        test.nemesis,
        "--time-limit",
        str(tl),
        "--base-port",
        str(base_port),
    ]
    if test.cluster_flag:
        cmd.append("--cluster")
    cmd.extend(extra_args)

    cwd = root / "testing" / "jepsen" / "frogdb"

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


def generate_batch_edn(
    tests: list[TestDefinition], time_limit: int | None, base_port: int = DEFAULT_BASE_PORT
) -> str:
    """Generate an EDN batch file for test-all command. Returns the temp file path."""
    configs = []
    for t in tests:
        tl = time_limit if time_limit is not None else t.time_limit
        pairs = [
            f':workload "{t.workload}"',
            f':nemesis "{t.nemesis}"',
            f":time-limit {tl}",
            f":base-port {base_port}",
        ]
        if t.cluster_flag:
            pairs.append(":cluster true")
        configs.append("{" + " ".join(pairs) + "}")

    edn = "[" + "\n ".join(configs) + "]"

    f = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".edn",
        prefix="jepsen-batch-",
        delete=False,
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
    base_port: int = DEFAULT_BASE_PORT,
) -> list[TestResult]:
    """Run multiple Jepsen tests in a single JVM via the test-all command."""
    batch_path = generate_batch_edn(tests, time_limit, base_port)

    cmd = [
        "lein",
        "run",
        "test-all",
        "--docker",
        "--batch-file",
        batch_path,
        "--base-port",
        str(base_port),
    ]
    cmd.extend(extra_args)

    cwd = root / "testing" / "jepsen" / "frogdb"

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


def git_short_hash(root: Path) -> str:
    """Get short git hash for labeling Docker images."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=root, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def build_frogdb(root: Path, c: Color, mode: str = "cross") -> None:
    """Build FrogDB Docker image using the specified build mode."""
    git_hash = git_short_hash(root)
    label = f"--label=frogdb.git-hash={git_hash}"

    if mode == "docker":
        print(c.bold("Building FrogDB in Docker (Dockerfile.builder)..."))
        result = subprocess.run(
            [
                "docker",
                "build",
                "-f",
                "frogdb-server/docker/Dockerfile.builder",
                "--build-arg",
                "BUILD_TARGET=debug",
                label,
                "-t",
                "frogdb:latest",
                ".",
            ],
            cwd=root,
        )
        if result.returncode != 0:
            print(c.red("Docker build failed."))
            sys.exit(1)
    else:
        print(c.bold("Cross-compiling FrogDB..."))
        result = subprocess.run(["just", "cross-build"], cwd=root)
        if result.returncode != 0:
            print(c.red("Cross-build failed."))
            sys.exit(1)
        print(c.bold("Building Docker image..."))
        result = subprocess.run(
            [
                "docker",
                "build",
                "-f",
                "frogdb-server/docker/Dockerfile",
                label,
                "-t",
                "frogdb:latest",
                ".",
            ],
            cwd=root,
        )
        if result.returncode != 0:
            print(c.red("Docker build failed."))
            sys.exit(1)

    record_build(root)


def cmd_run(args: argparse.Namespace, extra_args: list[str]) -> None:
    c = Color(enabled=False if args.no_color else None)
    root = get_project_root()
    preflight(c)

    if args.build is True:
        build_frogdb(root, c, args.build_mode)
    elif args.build is None:
        # Auto-detect: rebuild only when source changed since last build
        if needs_rebuild(root):
            print(c.yellow("Source changed since last build — rebuilding..."))
            build_frogdb(root, c, args.build_mode)

    tests = resolve_tests(args.test, args.suite, c)
    parallel = args.parallel
    explicit_base_port: int | None = args.base_port

    # Compute base port per topology
    def base_port_for(topo: Topology) -> int:
        if explicit_base_port is not None:
            return explicit_base_port
        if parallel:
            return PARALLEL_BASE_PORTS[topo]
        return DEFAULT_BASE_PORT

    # Group by topology, process in order: single -> replication -> raft
    topo_order = [Topology.SINGLE, Topology.REPLICATION, Topology.RAFT]
    groups: dict[Topology, list[TestDefinition]] = {}
    for t in tests:
        groups.setdefault(t.topology, []).append(t)

    results: list[TestResult] = []
    total = len(tests)
    idx = 0
    active_topologies: set[Topology] = set()

    try:
        if parallel:
            # Start all topologies upfront with non-conflicting ports
            for topo in topo_order:
                if topo in groups:
                    compose_up(root, topo, c, base_port_for(topo))
                    active_topologies.add(topo)
        active_topology: Topology | None = None

        for topo in topo_order:
            if topo not in groups:
                continue

            bp = base_port_for(topo)

            if not parallel:
                # Sequential mode: tear down previous topology before switching
                if active_topology is not None and active_topology != topo:
                    compose_down(root, active_topology, c, base_port_for(active_topology))
                    active_topologies.discard(active_topology)
                    active_topology = None

                if active_topology != topo:
                    compose_up(root, topo, c, bp)
                    active_topologies.add(topo)
                    active_topology = topo

            group_tests = groups[topo]
            if len(group_tests) == 1:
                test = group_tests[0]
                idx += 1
                print(c.bold(f"\n{'=' * 72}"))
                print(c.bold(f"  [{idx}/{total}] {test.name}"))
                print(c.bold(f"{'=' * 72}"))

                result = run_test(root, test, args.time_limit, extra_args, c, base_port=bp)
                results.append(result)

                if result.passed is True:
                    print(c.green(f"\n  {test.name}: PASS ({format_elapsed(result.elapsed)})"))
                elif result.passed is False:
                    print(c.red(f"\n  {test.name}: FAIL ({format_elapsed(result.elapsed)})"))
                else:
                    print(c.yellow(f"\n  {test.name}: UNKNOWN ({format_elapsed(result.elapsed)})"))
            else:
                print(c.bold(f"\n{'=' * 72}"))
                print(
                    c.bold(
                        f"  [{idx + 1}-{idx + len(group_tests)}/{total}]"
                        f" {topo.value} batch ({len(group_tests)} tests)"
                    )
                )
                print(c.bold(f"{'=' * 72}"))

                batch_results = run_test_batch(
                    root,
                    group_tests,
                    args.time_limit,
                    extra_args,
                    c,
                    stop_on_failure=args.stop_on_failure,
                    base_port=bp,
                )

                for result in batch_results:
                    idx += 1
                    if result.passed is True:
                        print(
                            c.green(
                                f"\n  {result.test.name}: PASS ({format_elapsed(result.elapsed)})"
                            )
                        )
                    elif result.passed is False:
                        print(
                            c.red(
                                f"\n  {result.test.name}: FAIL ({format_elapsed(result.elapsed)})"
                            )
                        )
                    else:
                        print(
                            c.yellow(
                                f"\n  {result.test.name}: UNKNOWN ({format_elapsed(result.elapsed)})"
                            )
                        )
                results.extend(batch_results)

            if args.stop_on_failure and any(r.passed is False for r in results):
                break

    except KeyboardInterrupt:
        print(c.yellow("\n\nInterrupted."))
    finally:
        if args.teardown:
            for topo in active_topologies:
                compose_down(root, topo, c, base_port_for(topo))

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
    build_frogdb(root, c, args.build_mode)
    print(c.green("Build complete."))


def cmd_up(args: argparse.Namespace) -> None:
    root = get_project_root()
    c = Color()
    topo = Topology(args.topology)
    bp = getattr(args, "base_port", None) or DEFAULT_BASE_PORT
    compose_up(root, topo, c, bp)


def cmd_down(args: argparse.Namespace) -> None:
    root = get_project_root()
    c = Color()
    bp = getattr(args, "base_port", None) or DEFAULT_BASE_PORT
    if args.topology:
        compose_down(root, Topology(args.topology), c, bp)
    else:
        for topo in Topology:
            compose_down(root, topo, c, bp)


def cmd_clean(args: argparse.Namespace) -> None:
    root = get_project_root()
    store = root / "testing" / "jepsen" / "frogdb" / "store"
    if store.exists():
        shutil.rmtree(store)
        print(f"Removed {store}")
    else:
        print("Nothing to clean.")


def cmd_results(args: argparse.Namespace) -> None:
    root = get_project_root()
    latest = root / "testing" / "jepsen" / "frogdb" / "store" / "latest"
    if not latest.exists():
        print("No results found. Run a test first.")
        sys.exit(1)
    if platform.system() == "Darwin":
        subprocess.run(["open", str(latest)])
    elif platform.system() == "Linux":
        subprocess.run(["xdg-open", str(latest)])
    else:
        print(f"Results at: {latest}")


def cmd_summary(args: argparse.Namespace) -> None:
    """Print pass/fail summary from latest test results."""
    root = get_project_root()
    c = Color()
    store = root / "testing" / "jepsen" / "frogdb" / "store"

    if not store.exists():
        print("No test results found. Run a test first.")
        sys.exit(1)

    # Each test writes results under store/<test-name>/<timestamp>/
    # with a 'latest' symlink. Also store/latest points to the most recent test.
    results: list[tuple[str, str, str]] = []  # (name, verdict, timestamp)

    for entry in sorted(store.iterdir()):
        if entry.name in ("latest",) or not entry.is_dir():
            continue

        latest = entry / "latest"
        if not latest.exists():
            continue

        # Resolve the symlink to get the timestamp directory name
        try:
            ts_dir = latest.resolve()
            timestamp = ts_dir.name
        except OSError:
            timestamp = "?"

        results_edn = latest / "results.edn"
        if not results_edn.exists():
            results.append((entry.name, "NO RESULTS", timestamp))
            continue

        # Parse :valid? from EDN (simple regex — avoids needing an EDN parser)
        try:
            text = results_edn.read_text()
            if re.search(r":valid\?\s+true", text):
                verdict = "PASS"
            elif re.search(r":valid\?\s+false", text):
                verdict = "FAIL"
            else:
                verdict = "UNKNOWN"
        except OSError:
            verdict = "READ ERROR"

        results.append((entry.name, verdict, timestamp))

    if not results:
        print("No test results found in store.")
        sys.exit(1)

    name_w = max(len(r[0]) for r in results)

    print()
    print(c.bold("=" * 72))
    print(c.bold("  Jepsen Test Results Summary"))
    print(c.bold("=" * 72))

    passed = 0
    failed = 0
    for name, verdict, timestamp in results:
        if verdict == "PASS":
            v = c.green(verdict)
            passed += 1
        elif verdict == "FAIL":
            v = c.red(verdict)
            failed += 1
        else:
            v = c.yellow(verdict)
        print(f"  {name:<{name_w}}  {v:<14}  {c.dim(timestamp)}")

    print()
    total = len(results)
    parts = [f"{total} tests"]
    if passed:
        parts.append(c.green(f"{passed} passed"))
    if failed:
        parts.append(c.red(f"{failed} failed"))
    other = total - passed - failed
    if other:
        parts.append(c.yellow(f"{other} other"))
    print(f"  {', '.join(parts)}")
    print()


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
        "--build",
        action="store_true",
        default=None,
        help="Force build before running",
    )
    p_run.add_argument(
        "--no-build",
        dest="build",
        action="store_false",
        help="Skip build even if source changed",
    )
    p_run.add_argument(
        "--build-mode",
        choices=["cross", "docker"],
        default="cross",
        help="Build mode: cross (zigbuild + Dockerfile) or docker (Dockerfile.builder)",
    )
    p_run.add_argument(
        "--teardown",
        action="store_true",
        default=False,
        help="Tear down compose after running",
    )
    p_run.add_argument("--no-teardown", dest="teardown", action="store_false")
    p_run.add_argument("--time-limit", type=int, help="Override default time-limit")
    p_run.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Abort suite on first failure",
    )
    p_run.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    p_run.add_argument(
        "--base-port",
        type=int,
        default=None,
        help=f"Base host port for Docker containers (default: {DEFAULT_BASE_PORT})",
    )
    p_run.add_argument(
        "--parallel",
        action="store_true",
        help="Run topologies in parallel with non-conflicting port ranges",
    )
    p_run.add_argument("-v", "--verbose", action="store_true", help="Reserved")

    # --- list ---
    p_list = sub.add_parser("list", help="List available tests and suites")
    p_list.add_argument(
        "--topology",
        choices=[t.value for t in Topology],
        help="Filter by topology",
    )

    # --- build ---
    p_build = sub.add_parser("build", help="Build FrogDB Docker image")
    p_build.add_argument(
        "--build-mode",
        choices=["cross", "docker"],
        default="cross",
        help="Build mode: cross (zigbuild + Dockerfile) or docker (Dockerfile.builder)",
    )

    # --- up ---
    p_up = sub.add_parser("up", help="Start Docker Compose topology")
    p_up.add_argument("topology", choices=[t.value for t in Topology])
    p_up.add_argument(
        "--base-port", type=int, default=None, help=f"Base host port (default: {DEFAULT_BASE_PORT})"
    )

    # --- down ---
    p_down = sub.add_parser("down", help="Tear down Docker Compose topology")
    p_down.add_argument(
        "topology",
        nargs="?",
        choices=[t.value for t in Topology],
        help="Omit to tear down all",
    )
    p_down.add_argument(
        "--base-port", type=int, default=None, help=f"Base host port (default: {DEFAULT_BASE_PORT})"
    )

    # --- clean ---
    sub.add_parser("clean", help="Remove Jepsen test results")

    # --- results ---
    sub.add_parser("results", help="Open latest results in browser")

    # --- summary ---
    sub.add_parser("summary", help="Print pass/fail summary from latest test results")

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
        "summary": lambda: cmd_summary(args),
    }
    handlers[args.command]()


if __name__ == "__main__":
    main()
