#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""
compare_all.py - Unified benchmark comparison against Redis, Valkey, and Dragonfly

Runs identical memtier_benchmark workloads against FrogDB and selected backends,
then generates comprehensive comparison reports.

Prerequisites:
    - Docker installed (for --start-docker)
    - memtier_benchmark installed
    - FrogDB running (if not using --start-docker)

Usage:
    uv run compare_all.py [OPTIONS]

Options:
    --backends LIST       Comma-separated backends: redis,valkey,dragonfly
    --all                 Run against all backends
    --start-docker        Auto-start Docker containers before benchmarks (includes FrogDB)
    --workload NAME       Workload preset: read-heavy, write-heavy, mixed (default: mixed)
    --yaml-workload NAME  Run a YAML workload from workloads/ (e.g., ycsb-a, leaderboard)
    --extended            Include extended command workloads (hash, list, zset)
    -n, --requests N      Requests per client (default: 10000)
    -o, --output DIR      Output directory for results
    --frogdb-port PORT    FrogDB port when running natively (default: 16379)
    --frogdb-native       Run FrogDB natively instead of in Docker (with --start-docker)
    --cpus N              CPUs per server (default: 1)
    --isolate             Pin each server to dedicated CPU cores via cpuset
    --scaling             Run scaling test: 1, 2, 4 cores
    --start-core N        Begin core assignment from core N (default: 0)

Examples:
    # Full comparison - all servers in Docker (fair comparison)
    uv run compare_all.py --all --start-docker

    # Quick comparison against Valkey only (all in Docker)
    uv run compare_all.py --backends valkey --start-docker

    # Extended workloads with all backends (Docker)
    uv run compare_all.py --all --extended --start-docker

    # Run YCSB-A workload comparison
    uv run compare_all.py --all --start-docker --yaml-workload ycsb-a

    # Run leaderboard workload (sorted sets)
    uv run compare_all.py --all --start-docker --yaml-workload leaderboard

    # Compare against native FrogDB (not in Docker)
    uv run compare_all.py --backends redis --start-docker --frogdb-native

    # Fair single-core comparison (1 CPU each, isolated)
    uv run compare_all.py --all --start-docker --cpus 1 --isolate

    # 2-core comparison (8 cores needed)
    uv run compare_all.py --all --start-docker --cpus 2 --isolate

    # CPU quota mode (no isolation, simpler for macOS)
    uv run compare_all.py --all --start-docker --cpus 2

    # Full scaling test (16+ cores recommended)
    uv run compare_all.py --all --start-docker --scaling
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class BackendConfig:
    name: str
    port: int
    docker_service: str
    image: str | None = None
    base_cmd: list[str] = field(default_factory=list)
    thread_flag: str | None = None  # For multi-threaded servers (Dragonfly)
    shard_flag: str | None = None  # For sharded servers (FrogDB)


# Backend configurations
# When --start-docker is used, all servers run in containers for fair comparison
BACKENDS = {
    "frogdb": BackendConfig(
        name="FrogDB",
        port=16379,
        docker_service="frogdb",
        image=None,  # Built locally
        base_cmd=["frogdb-server", "--bind", "0.0.0.0"],
        shard_flag="--shards",  # FrogDB uses shards for multi-threading
    ),
    "redis": BackendConfig(
        name="Redis",
        port=16380,
        docker_service="redis",
        image="redis:7-alpine",
        base_cmd=["redis-server", "--save", "", "--appendonly", "no"],
    ),
    "valkey": BackendConfig(
        name="Valkey",
        port=16381,
        docker_service="valkey",
        image="valkey/valkey:8-alpine",
        base_cmd=["valkey-server", "--save", "", "--appendonly", "no"],
    ),
    "dragonfly": BackendConfig(
        name="Dragonfly",
        port=16382,
        docker_service="dragonfly",
        image="docker.dragonflydb.io/dragonflydb/dragonfly",
        base_cmd=["dragonfly"],
        thread_flag="--proactor_threads",  # Dragonfly uses proactor threads
    ),
}


@dataclass
class CpuAssignment:
    """CPU assignment for a backend container."""

    cpus: int
    cpuset: str | None = None  # e.g., "0,1" for cores 0 and 1


def extract_memtier_metrics(data: dict) -> dict[str, Any]:
    """Extract key metrics from memtier JSON data."""
    metrics: dict[str, Any] = {
        "ops_per_sec": 0,
        "get_ops_per_sec": 0,
        "set_ops_per_sec": 0,
    }

    # Try to extract from 'ALL STATS' section
    if "ALL STATS" in data:
        stats = data["ALL STATS"]
        if "Gets" in stats:
            metrics["get_ops_per_sec"] = stats["Gets"].get("Ops/sec", 0)
        if "Sets" in stats:
            metrics["set_ops_per_sec"] = stats["Sets"].get("Ops/sec", 0)
        if "Totals" in stats:
            metrics["ops_per_sec"] = stats["Totals"].get("Ops/sec", 0)

    # Try alternative format
    if "totals" in data:
        totals = data["totals"]
        metrics["ops_per_sec"] = totals.get("ops_per_sec", 0) or totals.get("Ops/sec", 0)

    return metrics


def format_ops(n: float) -> str:
    """Format operations per second."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    else:
        return f"{n:.0f}"


def get_available_cpus() -> int:
    """Get the number of available CPUs for Docker containers."""
    # Try to get Docker's available CPUs first
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.NCPU}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return int(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError):
        pass

    # Fall back to system CPU count
    return os.cpu_count() or 4


def calculate_cpu_assignments(
    backends: list[str],
    cpus_per_server: int,
    isolate: bool,
    start_core: int = 0,
) -> dict[str, CpuAssignment]:
    """Calculate CPU assignments for each backend.

    Args:
        backends: List of backend names to assign CPUs to
        cpus_per_server: Number of CPUs per server
        isolate: If True, pin each server to dedicated cores via cpuset
        start_core: Starting core number for assignments

    Returns:
        Dictionary mapping backend name to CpuAssignment
    """
    total_cores = get_available_cpus()
    needed_cores = len(backends) * cpus_per_server

    if isolate and needed_cores + start_core > total_cores:
        raise ValueError(
            f"Need {needed_cores} cores starting from core {start_core} for isolation, "
            f"but only {total_cores} cores available. "
            f"Either reduce --cpus, remove --isolate, or add more CPUs to Docker."
        )

    assignments = {}
    current_core = start_core

    for backend in backends:
        if isolate:
            # Pin to specific cores
            cores = list(range(current_core, current_core + cpus_per_server))
            assignments[backend] = CpuAssignment(
                cpus=cpus_per_server,
                cpuset=",".join(map(str, cores)),
            )
            current_core += cpus_per_server
        else:
            # CPU quota only (shared cores)
            assignments[backend] = CpuAssignment(
                cpus=cpus_per_server,
                cpuset=None,
            )

    return assignments


@dataclass
class WorkloadConfig:
    ratio: str
    key_pattern: str


WORKLOAD_CONFIGS = {
    "read-heavy": WorkloadConfig(ratio="19:1", key_pattern="G:G"),
    "write-heavy": WorkloadConfig(ratio="1:19", key_pattern="S:S"),
    "mixed": WorkloadConfig(ratio="9:1", key_pattern="G:G"),
}


def check_connectivity(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a server is reachable on the given host:port."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
            return True
    except (TimeoutError, OSError):
        return False


def wait_for_connectivity(host: str, port: int, max_wait: int = 30) -> bool:
    """Wait for a server to become available."""
    start = time.time()
    while time.time() - start < max_wait:
        if check_connectivity(host, port):
            return True
        time.sleep(0.5)
    return False


def start_container_with_cpu_isolation(
    backend: str,
    assignment: CpuAssignment,
    project_root: Path,
) -> bool:
    """Start a single container with CPU isolation using docker run.

    Args:
        backend: Backend name (frogdb, redis, valkey, dragonfly)
        assignment: CPU assignment for this backend
        project_root: Path to project root (for FrogDB image building)

    Returns:
        True if container started successfully
    """
    config = BACKENDS[backend]
    container_name = f"benchmark-{backend}"

    # First, remove any existing container with the same name
    subprocess.run(
        ["docker", "rm", "-f", container_name],
        capture_output=True,
    )

    # Build command
    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{config.port}:6379",
    ]

    # CPU isolation
    if assignment.cpuset:
        cmd.extend(["--cpuset-cpus", assignment.cpuset])
    else:
        cmd.extend(["--cpus", str(assignment.cpus)])

    # Memory lock for Dragonfly
    if backend == "dragonfly":
        cmd.extend(["--ulimit", "memlock=-1"])

    # Image
    if config.image:
        cmd.append(config.image)
    else:
        # FrogDB - use pre-built image
        cmd.append("frogdb:latest")

    # Server command with thread/shard config based on CPUs
    server_cmd = config.base_cmd.copy()
    if config.shard_flag and assignment.cpus > 1:
        server_cmd.extend([config.shard_flag, str(assignment.cpus)])
    if config.thread_flag:
        server_cmd.extend([config.thread_flag, str(assignment.cpus)])

    cmd.extend(server_cmd)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error starting {backend}: {result.stderr}", file=sys.stderr)
        return False

    return True


def build_frogdb_image(project_root: Path) -> bool:
    """Build FrogDB Docker image if not already present."""
    # Check if image exists
    result = subprocess.run(
        ["docker", "images", "-q", "frogdb:latest"],
        capture_output=True,
        text=True,
    )
    if result.stdout.strip():
        print("  FrogDB image already exists")
        return True

    print("  Building FrogDB Docker image (this may take a while)...")
    result = subprocess.run(
        ["docker", "build", "-t", "frogdb:latest", str(project_root)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error building FrogDB image: {result.stderr}", file=sys.stderr)
        return False

    return True


def start_containers_with_cpu_isolation(
    backends: list[str],
    assignments: dict[str, CpuAssignment],
    project_root: Path,
    include_frogdb: bool = True,
) -> bool:
    """Start all containers with CPU isolation using docker run.

    Args:
        backends: List of backend names to start
        assignments: CPU assignments for each backend
        project_root: Path to project root
        include_frogdb: If True, also start FrogDB container

    Returns:
        True if all containers started successfully
    """
    host = "127.0.0.1"

    # Build FrogDB image if needed
    if include_frogdb and "frogdb" in assignments:
        if not build_frogdb_image(project_root):
            return False

    # Start containers
    all_backends = (
        ["frogdb"] + [b for b in backends if b != "frogdb"] if include_frogdb else backends
    )

    for backend in all_backends:
        if backend not in assignments:
            continue

        assignment = assignments[backend]
        config = BACKENDS[backend]

        cpu_info = f"cpuset={assignment.cpuset}" if assignment.cpuset else f"cpus={assignment.cpus}"
        print(f"  Starting {config.name} ({cpu_info})...")

        if not start_container_with_cpu_isolation(backend, assignment, project_root):
            return False

    # Wait for containers to be ready
    print("  Waiting for containers to be ready...")
    for backend in all_backends:
        if backend not in assignments:
            continue
        config = BACKENDS[backend]
        if not wait_for_connectivity(host, config.port):
            print(f"  Warning: {config.name} not responding after 30s", file=sys.stderr)

    return True


def stop_containers_with_cpu_isolation(backends: list[str], include_frogdb: bool = True) -> None:
    """Stop containers started with docker run."""
    all_backends = (
        ["frogdb"] + [b for b in backends if b != "frogdb"] if include_frogdb else backends
    )

    print("Stopping containers...")
    for backend in all_backends:
        container_name = f"benchmark-{backend}"
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
        )


def start_docker_containers(
    backends: list[str], compose_file: Path, include_frogdb: bool = False
) -> bool:
    """Start Docker containers for specified backends using docker-compose.

    This is the legacy method without CPU isolation. Use start_containers_with_cpu_isolation
    for CPU-isolated benchmarks.

    Args:
        backends: List of backend names to start (redis, valkey, dragonfly)
        compose_file: Path to docker-compose file
        include_frogdb: If True, also start FrogDB container for fair comparison
    """
    if not compose_file.exists():
        print(f"Error: Docker compose file not found: {compose_file}", file=sys.stderr)
        return False

    services = [BACKENDS[b].docker_service for b in backends if b in BACKENDS and b != "frogdb"]

    # Include FrogDB if requested (for fair containerized comparison)
    if include_frogdb:
        services.insert(0, "frogdb")

    if not services:
        return True

    print(f"Starting Docker containers: {', '.join(services)}")
    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "up",
        "-d",
        "--build",
    ] + services

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error starting Docker containers: {result.stderr}", file=sys.stderr)
        return False

    # Wait for containers to be ready
    host = "127.0.0.1"

    # Wait for FrogDB if included
    if include_frogdb:
        frogdb = BACKENDS["frogdb"]
        print(f"  Waiting for {frogdb.name} on port {frogdb.port}...")
        if not wait_for_connectivity(host, frogdb.port):
            print(f"  Warning: {frogdb.name} not responding after 30s", file=sys.stderr)

    for backend_name in backends:
        if backend_name not in BACKENDS or backend_name == "frogdb":
            continue
        backend = BACKENDS[backend_name]
        print(f"  Waiting for {backend.name} on port {backend.port}...")
        if not wait_for_connectivity(host, backend.port):
            print(f"  Warning: {backend.name} not responding after 30s", file=sys.stderr)

    return True


def stop_docker_containers(compose_file: Path) -> None:
    """Stop all Docker containers started with docker-compose."""
    if not compose_file.exists():
        return

    print("Stopping Docker containers...")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down"],
        capture_output=True,
    )


def run_memtier(
    host: str,
    port: int,
    json_file: Path,
    txt_file: Path,
    ratio: str,
    key_pattern: str,
    requests: int,
    name: str = "",
) -> int:
    """Run memtier_benchmark and capture output."""
    cmd = [
        "memtier_benchmark",
        "-s",
        host,
        "-p",
        str(port),
        "--threads",
        "4",
        "--clients",
        "25",
        "--requests",
        str(requests),
        "--data-size",
        "128",
        "--ratio",
        ratio,
        "--key-pattern",
        key_pattern,
        "--key-maximum",
        "10000000",
        "--pipeline",
        "1",
        "--hide-histogram",
        "--json-out-file",
        str(json_file),
    ]

    with open(txt_file, "w") as f:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        f.write(result.stdout)
        # Print a summary line instead of full output
        if name:
            lines = result.stdout.strip().split("\n")
            # Find the totals line
            for line in lines:
                if "Totals" in line:
                    print(f"  {name}: {line.strip()}")
                    break

    return result.returncode


def run_extended_benchmark(
    host: str,
    port: int,
    output_file: Path,
    name: str,
) -> dict:
    """Run extended benchmarks for non-GET/SET commands using unified runners.

    Uses the new benchmark system with redis-py based runners for:
    - Hash operations (HSET, HGET, HGETALL)
    - List operations (LPUSH, RPOP, LRANGE)
    - Sorted Set operations (ZADD, ZRANGE, ZINCRBY)
    """
    script_dir = Path(__file__).parent

    # Try to use the new unified benchmark system
    try:
        sys.path.insert(0, str(script_dir))
        from runners.hash_runner import HashRunner
        from runners.list_runner import ListRunner
        from runners.zset import ZSetRunner
        from workload_loader import (
            ConcurrencyConfig,
            DataConfig,
            HashConfig,
            KeysConfig,
            ListConfig,
            WorkloadConfig,
            ZSetConfig,
        )

        results = {}

        # Hash workload
        print("    Running hash operations...")
        hash_workload = WorkloadConfig(
            name="extended-hash",
            commands={"HSET": 30, "HGET": 50, "HGETALL": 20},
            keys=KeysConfig(space_size=10000),
            data=DataConfig(size_bytes=128),
            hash=HashConfig(fields_per_hash=10),
            concurrency=ConcurrencyConfig(clients=50, threads=4, pipeline=1),
        )
        hash_runner = HashRunner(host, port)
        if hash_runner.is_available():
            hash_output = output_file.parent / f"{output_file.stem}_hash"
            hash_output.mkdir(parents=True, exist_ok=True)
            hash_result = hash_runner.run(hash_workload, hash_output, requests=10000, warmup=True)
            results["hash"] = {
                "ops_per_sec": hash_result.total_ops_per_sec,
                "p99_ms": hash_result.p99_latency_ms,
                "commands": {
                    cmd: {"ops_per_sec": r.ops_per_sec, "p99_ms": r.p99_latency_ms}
                    for cmd, r in hash_result.commands.items()
                },
            }

        # List workload
        print("    Running list operations...")
        list_workload = WorkloadConfig(
            name="extended-list",
            commands={"LPUSH": 50, "RPOP": 30, "LRANGE": 20},
            keys=KeysConfig(space_size=1000),
            data=DataConfig(size_bytes=128),
            list=ListConfig(max_length=1000),
            concurrency=ConcurrencyConfig(clients=50, threads=4, pipeline=1),
        )
        list_runner = ListRunner(host, port)
        if list_runner.is_available():
            list_output = output_file.parent / f"{output_file.stem}_list"
            list_output.mkdir(parents=True, exist_ok=True)
            list_result = list_runner.run(list_workload, list_output, requests=10000, warmup=True)
            results["list"] = {
                "ops_per_sec": list_result.total_ops_per_sec,
                "p99_ms": list_result.p99_latency_ms,
                "commands": {
                    cmd: {"ops_per_sec": r.ops_per_sec, "p99_ms": r.p99_latency_ms}
                    for cmd, r in list_result.commands.items()
                },
            }

        # ZSet workload
        print("    Running sorted set operations...")
        zset_workload = WorkloadConfig(
            name="extended-zset",
            commands={"ZADD": 30, "ZINCRBY": 30, "ZRANGE": 25, "ZRANK": 15},
            keys=KeysConfig(space_size=100),
            data=DataConfig(size_bytes=128),
            zset=ZSetConfig(members_per_set=1000),
            concurrency=ConcurrencyConfig(clients=50, threads=4, pipeline=1),
        )
        zset_runner = ZSetRunner(host, port)
        if zset_runner.is_available():
            zset_output = output_file.parent / f"{output_file.stem}_zset"
            zset_output.mkdir(parents=True, exist_ok=True)
            zset_result = zset_runner.run(zset_workload, zset_output, requests=10000, warmup=True)
            results["zset"] = {
                "ops_per_sec": zset_result.total_ops_per_sec,
                "p99_ms": zset_result.p99_latency_ms,
                "commands": {
                    cmd: {"ops_per_sec": r.ops_per_sec, "p99_ms": r.p99_latency_ms}
                    for cmd, r in zset_result.commands.items()
                },
            }

        # Save results
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        return results

    except ImportError as e:
        # Fall back to legacy extended_benchmark.py if new system not available
        print(f"  Warning: New benchmark system not available ({e}), trying legacy...")
        extended_script = script_dir / "extended_benchmark.py"

        if not extended_script.exists():
            print("  Warning: extended_benchmark.py not found, skipping extended workloads")
            return {}

        result = subprocess.run(
            [sys.executable, str(extended_script), "--host", host, "--port", str(port), "--json"],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            print(f"  Warning: Extended benchmark failed for {name}: {result.stderr}")
            return {}

        try:
            data = json.loads(result.stdout)
            with open(output_file, "w") as f:
                json.dump(data, f, indent=2)
            return data
        except json.JSONDecodeError:
            print(f"  Warning: Failed to parse extended benchmark output for {name}")
            return {}
    except Exception as e:
        print(f"  Warning: Extended benchmark failed: {e}")
        return {}


def collect_results(
    report_dir: Path,
    backends_run: list[str],
    cpu_config: dict[str, Any] | None = None,
) -> dict:
    """Collect all benchmark results into a single structure.

    Args:
        report_dir: Directory containing result files
        backends_run: List of backend names that were benchmarked
        cpu_config: Optional CPU configuration info to include in results
    """
    results: dict[str, Any] = {
        "timestamp": datetime.now().isoformat(),
        "backends": {},
    }

    if cpu_config:
        results["cpu_config"] = cpu_config

    # FrogDB results
    frogdb_json = report_dir / "frogdb.json"
    if frogdb_json.exists():
        with open(frogdb_json) as f:
            results["backends"]["frogdb"] = {
                "name": "FrogDB",
                "memtier": json.load(f),
            }
        # Check for extended results
        frogdb_extended = report_dir / "frogdb_extended.json"
        if frogdb_extended.exists():
            with open(frogdb_extended) as f:
                results["backends"]["frogdb"]["extended"] = json.load(f)

    # Backend results
    for backend_name in backends_run:
        json_file = report_dir / f"{backend_name}.json"
        if json_file.exists():
            with open(json_file) as f:
                results["backends"][backend_name] = {
                    "name": BACKENDS[backend_name].name,
                    "memtier": json.load(f),
                }
            # Check for extended results
            extended_file = report_dir / f"{backend_name}_extended.json"
            if extended_file.exists():
                with open(extended_file) as f:
                    results["backends"][backend_name]["extended"] = json.load(f)

    return results


def main() -> None:
    script_dir = Path(__file__).parent.resolve()
    loadtest_dir = script_dir.parent
    default_output_dir = loadtest_dir / "reports"
    compose_file = loadtest_dir / "docker-compose.benchmarks.yml"

    parser = argparse.ArgumentParser(
        description="Compare FrogDB performance against Redis, Valkey, and Dragonfly",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--backends", type=str, help="Comma-separated list of backends: redis,valkey,dragonfly"
    )
    parser.add_argument("--all", action="store_true", help="Run against all backends")
    parser.add_argument(
        "--start-docker", action="store_true", help="Auto-start Docker containers before benchmarks"
    )
    parser.add_argument(
        "-w",
        "--workload",
        default="mixed",
        choices=["read-heavy", "write-heavy", "mixed"],
        help="Workload preset (default: mixed)",
    )
    parser.add_argument(
        "--yaml-workload",
        type=str,
        help="Run a YAML workload from workloads/ (e.g., ycsb-a, leaderboard)",
    )
    parser.add_argument(
        "--extended",
        action="store_true",
        help="Include extended command workloads (hash, list, zset)",
    )
    parser.add_argument(
        "-n", "--requests", type=int, default=10000, help="Requests per client (default: 10000)"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=default_output_dir,
        help=f"Output directory for results (default: {default_output_dir})",
    )
    parser.add_argument(
        "--frogdb-port",
        type=int,
        default=16379,
        help="FrogDB port when running natively (default: 16379). Ignored with --start-docker.",
    )
    parser.add_argument(
        "--frogdb-native",
        action="store_true",
        help="Run FrogDB natively instead of in Docker (even with --start-docker)",
    )

    # CPU isolation options
    parser.add_argument("--cpus", type=int, default=1, help="CPUs per server (default: 1)")
    parser.add_argument(
        "--isolate", action="store_true", help="Pin each server to dedicated CPU cores via cpuset"
    )
    parser.add_argument("--scaling", action="store_true", help="Run scaling test: 1, 2, 4 cores")
    parser.add_argument(
        "--start-core", type=int, default=0, help="Begin core assignment from core N (default: 0)"
    )

    args = parser.parse_args()

    # Determine which backends to run
    if args.all:
        backends_to_run = list(BACKENDS.keys())
    elif args.backends:
        backends_to_run = [b.strip().lower() for b in args.backends.split(",")]
        invalid = [b for b in backends_to_run if b not in BACKENDS]
        if invalid:
            print(f"Error: Invalid backends: {invalid}", file=sys.stderr)
            print(f"Valid backends: {list(BACKENDS.keys())}", file=sys.stderr)
            sys.exit(1)
    else:
        print("Error: Specify --backends or --all", file=sys.stderr)
        sys.exit(1)

    # Handle YAML workload mode - delegate to benchmark.py
    if args.yaml_workload:
        benchmark_script = script_dir / "benchmark.py"
        if not benchmark_script.exists():
            print(f"Error: benchmark.py not found at {benchmark_script}", file=sys.stderr)
            sys.exit(1)

        # Build command for benchmark.py
        cmd = [
            sys.executable,
            str(benchmark_script),
            "--workload",
            args.yaml_workload,
            "--backends",
            ",".join(backends_to_run),
            "-n",
            str(args.requests * 100 * 4),  # Convert per-client to total
            "-o",
            str(args.output),
        ]

        if args.start_docker:
            cmd.append("--start-docker")

        print(f"Delegating to benchmark.py for YAML workload: {args.yaml_workload}")
        print(f"Command: {' '.join(cmd)}")
        print()

        result = subprocess.run(cmd)
        sys.exit(result.returncode)

    # Check if memtier_benchmark is available
    if not shutil.which("memtier_benchmark"):
        print("Error: memtier_benchmark not found", file=sys.stderr)
        print("Install it with: brew install memtier_benchmark (macOS)", file=sys.stderr)
        sys.exit(1)

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = args.output / f"compare_{args.workload}_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Get workload configuration
    config = WORKLOAD_CONFIGS[args.workload]
    host = "127.0.0.1"

    # Determine FrogDB mode: Docker or native
    frogdb_in_docker = args.start_docker and not args.frogdb_native
    frogdb_port = BACKENDS["frogdb"].port if frogdb_in_docker else args.frogdb_port

    # Filter out frogdb from backends_to_run if present (it's handled separately)
    comparison_backends = [b for b in backends_to_run if b != "frogdb"]

    # Handle scaling test mode
    if args.scaling:
        print("=" * 70)
        print("FrogDB Scaling Test Suite")
        print("=" * 70)
        print("CPU configurations: 1, 2, 4 cores")
        print(f"Workload:           {args.workload}")
        print(f"Backends:           {', '.join(BACKENDS[b].name for b in backends_to_run)}")
        print("=" * 70)
        print()

        scaling_results = {}
        for cpu_count in [1, 2, 4]:
            print(f"\n{'=' * 70}")
            print(f"Running with {cpu_count} CPU(s) per server")
            print("=" * 70)

            try:
                assignments = calculate_cpu_assignments(
                    backends_to_run, cpu_count, isolate=True, start_core=args.start_core
                )
            except ValueError as e:
                print(f"Warning: {e}")
                print(f"Skipping {cpu_count}-CPU test")
                continue

            # Start containers
            if args.start_docker:
                if not start_containers_with_cpu_isolation(
                    comparison_backends,
                    assignments,
                    script_dir.parent.parent,
                    include_frogdb=frogdb_in_docker,
                ):
                    print(f"Warning: Failed to start containers for {cpu_count}-CPU test")
                    continue

            # Run benchmarks
            scaling_report_dir = args.output / f"scaling_{cpu_count}cpu_{timestamp}"
            scaling_report_dir.mkdir(parents=True, exist_ok=True)

            # Run FrogDB benchmark
            frogdb_json = scaling_report_dir / "frogdb.json"
            frogdb_txt = scaling_report_dir / "frogdb.txt"
            run_memtier(
                host=host,
                port=frogdb_port,
                json_file=frogdb_json,
                txt_file=frogdb_txt,
                ratio=config.ratio,
                key_pattern=config.key_pattern,
                requests=args.requests,
                name="FrogDB",
            )

            # Run other backends
            for backend_name in comparison_backends:
                backend = BACKENDS[backend_name]
                if not check_connectivity(host, backend.port):
                    continue
                backend_json = scaling_report_dir / f"{backend_name}.json"
                backend_txt = scaling_report_dir / f"{backend_name}.txt"
                run_memtier(
                    host=host,
                    port=backend.port,
                    json_file=backend_json,
                    txt_file=backend_txt,
                    ratio=config.ratio,
                    key_pattern=config.key_pattern,
                    requests=args.requests,
                    name=backend.name,
                )

            # Collect results
            cpu_config = {
                "cpus_per_server": cpu_count,
                "isolated": True,
            }
            results = collect_results(scaling_report_dir, comparison_backends, cpu_config)
            scaling_results[cpu_count] = results

            # Save results
            combined_json = scaling_report_dir / "combined_results.json"
            with open(combined_json, "w") as f:
                json.dump(results, f, indent=2)

            # Stop containers
            if args.start_docker:
                stop_containers_with_cpu_isolation(
                    comparison_backends, include_frogdb=frogdb_in_docker
                )

        # Generate scaling summary
        print("\n" + "=" * 70)
        print("Scaling Test Summary")
        print("=" * 70)
        print(f"\n{'Backend':<12} {'1 CPU':<15} {'2 CPUs':<15} {'4 CPUs':<15}")
        print("-" * 57)

        for backend in backends_to_run:
            ops_values = []
            for cpu_count in [1, 2, 4]:
                if cpu_count in scaling_results:
                    backend_data = scaling_results[cpu_count].get("backends", {}).get(backend, {})
                    if "memtier" in backend_data:
                        metrics = extract_memtier_metrics(backend_data["memtier"])
                        ops = metrics.get("ops_per_sec", 0)
                        ops_values.append(format_ops(ops))
                    else:
                        ops_values.append("-")
                else:
                    ops_values.append("-")

            print(
                f"{BACKENDS[backend].name:<12} {ops_values[0]:<15} {ops_values[1]:<15} {ops_values[2]:<15}"
            )

        print("\nResults saved to:")
        for cpu_count in [1, 2, 4]:
            if cpu_count in scaling_results:
                print(f"  - {args.output / f'scaling_{cpu_count}cpu_{timestamp}'}")

        return

    # Determine CPU configuration for non-scaling mode
    use_cpu_isolation = args.cpus > 1 or args.isolate
    cpu_assignments: dict[str, CpuAssignment] | None = None

    if use_cpu_isolation and args.start_docker:
        try:
            # Include frogdb in CPU assignments when running in Docker
            assignment_backends = (
                (["frogdb"] + comparison_backends) if frogdb_in_docker else comparison_backends
            )
            cpu_assignments = calculate_cpu_assignments(
                assignment_backends, args.cpus, args.isolate, args.start_core
            )
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    print("=" * 70)
    print("FrogDB Benchmark Comparison Suite")
    print("=" * 70)
    print(f"Workload:       {args.workload}")
    print(f"Ratio (R:W):    {config.ratio}")
    print(f"Key pattern:    {config.key_pattern}")
    print(f"Requests:       {args.requests} per client")
    if comparison_backends:
        print(f"Backends:       FrogDB, {', '.join(BACKENDS[b].name for b in comparison_backends)}")
    else:
        print("Backends:       FrogDB only")
    print(f"FrogDB mode:    {'Docker' if frogdb_in_docker else 'Native'}")
    if use_cpu_isolation and args.start_docker:
        print(f"CPUs/server:    {args.cpus}")
        print(f"CPU isolation:  {'Pinned (cpuset)' if args.isolate else 'Quota (--cpus)'}")
        if args.isolate:
            print(f"Start core:     {args.start_core}")
    print(f"Extended:       {'Yes' if args.extended else 'No'}")
    print(f"Output:         {report_dir}")
    print("=" * 70)
    print()

    # Start Docker containers if requested
    if args.start_docker:
        if use_cpu_isolation and cpu_assignments:
            print("Starting containers with CPU isolation...")
            if not start_containers_with_cpu_isolation(
                comparison_backends,
                cpu_assignments,
                script_dir.parent.parent,
                include_frogdb=frogdb_in_docker,
            ):
                print("Warning: Some containers may not have started correctly", file=sys.stderr)
        else:
            if not start_docker_containers(
                comparison_backends, compose_file, include_frogdb=frogdb_in_docker
            ):
                print("Warning: Some containers may not have started correctly", file=sys.stderr)
        print()

    # Check FrogDB connectivity
    print("Checking server connectivity...")
    if not check_connectivity(host, frogdb_port):
        print(f"Warning: FrogDB not responding on {host}:{frogdb_port}")
        if frogdb_in_docker:
            print(
                "Check Docker container status: docker compose -f loadtest/docker-compose.benchmarks.yml ps"
            )
        else:
            print("Start it with: cargo run --release")
    else:
        print(f"  FrogDB: OK ({host}:{frogdb_port})")

    # Check backend connectivity
    for backend_name in comparison_backends:
        backend = BACKENDS[backend_name]
        if check_connectivity(host, backend.port):
            print(f"  {backend.name}: OK ({host}:{backend.port})")
        else:
            print(f"  {backend.name}: NOT RESPONDING ({host}:{backend.port})")

    print()

    # Run FrogDB benchmark
    print("Running benchmarks...")
    print("-" * 70)

    print("FrogDB (memtier):")
    frogdb_json = report_dir / "frogdb.json"
    frogdb_txt = report_dir / "frogdb.txt"
    run_memtier(
        host=host,
        port=frogdb_port,
        json_file=frogdb_json,
        txt_file=frogdb_txt,
        ratio=config.ratio,
        key_pattern=config.key_pattern,
        requests=args.requests,
        name="FrogDB",
    )

    # Run backend benchmarks
    for backend_name in comparison_backends:
        backend = BACKENDS[backend_name]
        if not check_connectivity(host, backend.port):
            print(f"\n{backend.name}: SKIPPED (not running)")
            continue

        print(f"\n{backend.name} (memtier):")
        backend_json = report_dir / f"{backend_name}.json"
        backend_txt = report_dir / f"{backend_name}.txt"
        run_memtier(
            host=host,
            port=backend.port,
            json_file=backend_json,
            txt_file=backend_txt,
            ratio=config.ratio,
            key_pattern=config.key_pattern,
            requests=args.requests,
            name=backend.name,
        )

    # Run extended workloads if requested
    if args.extended:
        print("\n" + "-" * 70)
        print("Running extended benchmarks (hash, list, zset)...")

        print("\nFrogDB (extended):")
        frogdb_extended = report_dir / "frogdb_extended.json"
        run_extended_benchmark(host, frogdb_port, frogdb_extended, "FrogDB")

        for backend_name in comparison_backends:
            backend = BACKENDS[backend_name]
            if not check_connectivity(host, backend.port):
                continue

            print(f"\n{backend.name} (extended):")
            backend_extended = report_dir / f"{backend_name}_extended.json"
            run_extended_benchmark(host, backend.port, backend_extended, backend.name)

    print()

    # Build CPU config for results
    cpu_config: dict[str, Any] | None = None
    if use_cpu_isolation and args.start_docker and cpu_assignments:
        cpu_config = {
            "cpus_per_server": args.cpus,
            "isolated": args.isolate,
            "start_core": args.start_core if args.isolate else None,
        }

    # Collect results
    results = collect_results(report_dir, comparison_backends, cpu_config)

    # Save combined results
    combined_json = report_dir / "combined_results.json"
    with open(combined_json, "w") as f:
        json.dump(results, f, indent=2)

    # Generate report
    print("=" * 70)
    print("Generating Report")
    print("=" * 70)

    generate_script = script_dir / "generate_report.py"
    if generate_script.exists():
        report_file = report_dir / "benchmark_report.md"
        cmd = [
            sys.executable,
            str(generate_script),
            "--input",
            str(combined_json),
            "--output",
            str(report_file),
        ]
        # Pass CPU config to report generator
        if cpu_config:
            cmd.extend(["--cpus", str(args.cpus)])
            if args.isolate:
                cmd.append("--isolated")

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Report generated: {report_file}")
            # Print summary from report
            if report_file.exists():
                lines = report_file.read_text().split("\n")
                # Print the summary section
                in_summary = False
                for line in lines:
                    if line.startswith("## Summary"):
                        in_summary = True
                    elif line.startswith("## ") and in_summary:
                        break
                    elif in_summary:
                        print(line)
        else:
            print(f"Warning: Report generation failed: {result.stderr}")
    else:
        # Fallback to parse_results.py for basic comparison
        parse_script = script_dir / "parse_results.py"
        if parse_script.exists() and comparison_backends:
            first_backend = comparison_backends[0]
            backend_json = report_dir / f"{first_backend}.json"
            if frogdb_json.exists() and backend_json.exists():
                subprocess.run(
                    [
                        sys.executable,
                        str(parse_script),
                        "--frogdb",
                        str(frogdb_json),
                        "--redis",
                        str(backend_json),
                    ],
                )

    print()
    print(f"Results saved to: {report_dir}")
    print("=" * 70)

    # Clean up: stop containers if we started them with CPU isolation
    if args.start_docker and use_cpu_isolation and cpu_assignments:
        print()
        stop_containers_with_cpu_isolation(comparison_backends, include_frogdb=frogdb_in_docker)


if __name__ == "__main__":
    main()
