#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml", "redis"]
# ///
"""
benchmark.py - Unified benchmark runner for FrogDB

Run any workload against any backend using the appropriate benchmark tool.

Usage:
    # Run a workload against local FrogDB
    uv run benchmark.py --workload session-store

    # Run against multiple backends
    uv run benchmark.py --workload ycsb-a --backends frogdb,redis,valkey

    # Run with Docker-managed backends
    uv run benchmark.py --workload ycsb-b --backends all --start-docker

    # List available workloads
    uv run benchmark.py --list

    # Validate all workload files
    uv run benchmark.py --validate

Examples:
    # YCSB-style benchmark
    uv run benchmark.py --workload ycsb-a --backends all --start-docker

    # Leaderboard workload (sorted sets)
    uv run benchmark.py --workload leaderboard --backends frogdb

    # Session store with rate limiting
    uv run benchmark.py --workload session-store -n 100000

    # Compare all backends with extended workloads
    uv run benchmark.py --workload mixed-realistic --backends all --start-docker
"""

import argparse
import json
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
script_dir = Path(__file__).parent.resolve()
sys.path.insert(0, str(script_dir))

import runners.geo  # noqa: E402, F401 — register GEO runner
import runners.hash_runner  # noqa: E402, F401 — register HASH runner
import runners.list_runner  # noqa: E402, F401 — register LIST runner
import runners.memtier  # noqa: E402, F401 — register STRING runner
import runners.pubsub  # noqa: E402, F401 — register PUBSUB runner
import runners.set_runner  # noqa: E402, F401 — register SET runner
import runners.stream  # noqa: E402, F401 — register STREAM runner
import runners.zset  # noqa: E402, F401 — register ZSET runner
from runners.base import (  # noqa: E402
    BenchmarkResult,
    RunnerRegistry,
)
from workload_loader import (  # noqa: E402
    CommandCategory,
    WorkloadConfig,
    WorkloadValidationError,
    list_workloads,
    load_workload,
)


@dataclass
class BackendConfig:
    """Backend server configuration."""

    name: str
    display_name: str
    port: int
    docker_image: str | None = None
    docker_service: str | None = None


# Backend configurations
BACKENDS = {
    "frogdb": BackendConfig(
        name="frogdb",
        display_name="FrogDB",
        port=16379,
        docker_service="frogdb",
    ),
    "redis": BackendConfig(
        name="redis",
        display_name="Redis",
        port=16380,
        docker_image="redis:7-alpine",
        docker_service="redis",
    ),
    "valkey": BackendConfig(
        name="valkey",
        display_name="Valkey",
        port=16381,
        docker_image="valkey/valkey:8-alpine",
        docker_service="valkey",
    ),
    "dragonfly": BackendConfig(
        name="dragonfly",
        display_name="Dragonfly",
        port=16382,
        docker_image="docker.dragonflydb.io/dragonflydb/dragonfly",
        docker_service="dragonfly",
    ),
}


def check_connectivity(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a server is reachable."""
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


def check_port_conflicts(backends: list[str]) -> list[tuple[str, int, str]]:
    """Check if benchmark ports are already in use by other containers.

    Returns a list of (backend_name, port, conflicting_container) tuples.
    """
    conflicts = []
    for name in backends:
        if name not in BACKENDS:
            continue
        port = BACKENDS[name].port
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"publish={port}", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
            )
            for container in result.stdout.strip().splitlines():
                # Ignore our own benchmark containers (they'll be torn down)
                if not container.startswith("benchmark-"):
                    conflicts.append((name, port, container))
        except Exception:
            pass
    return conflicts


def start_docker_containers(
    backends: list[str],
    compose_file: Path,
    include_frogdb: bool = True,
) -> bool:
    """Start Docker containers for specified backends.

    Tears down any existing benchmark containers first to avoid port conflicts,
    then starts the requested services and waits for them to become healthy.
    """
    if not compose_file.exists():
        print(f"Error: Docker compose file not found: {compose_file}", file=sys.stderr)
        return False

    services = []
    if include_frogdb:
        services.append("frogdb")
    services.extend(
        [BACKENDS[b].docker_service for b in backends if b in BACKENDS and b != "frogdb"]
    )

    if not services:
        return True

    # Tear down any stale benchmark containers first
    stop_docker_containers(compose_file, quiet=True)

    # Check for port conflicts from non-benchmark containers
    all_backends = (["frogdb"] + backends) if include_frogdb else backends
    conflicts = check_port_conflicts(all_backends)
    if conflicts:
        print("Error: Benchmark ports are in use by other containers:", file=sys.stderr)
        for name, port, container in conflicts:
            print(f"  Port {port} ({name}) is used by container '{container}'", file=sys.stderr)
        print(
            "\nStop the conflicting containers first, e.g.:",
            file=sys.stderr,
        )
        containers = sorted({c for _, _, c in conflicts})
        print(f"  docker stop {' '.join(containers)}", file=sys.stderr)
        return False

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
        print(f"Warning: docker compose up exited with code {result.returncode}", file=sys.stderr)

    # Wait for containers to be ready
    host = "127.0.0.1"

    ready = True
    for backend_name in all_backends:
        if backend_name not in BACKENDS:
            continue
        backend = BACKENDS[backend_name]
        print(f"  Waiting for {backend.display_name} on port {backend.port}...")
        if not wait_for_connectivity(host, backend.port):
            print(f"  Warning: {backend.display_name} not responding after 30s")
            ready = False
        else:
            print(f"  {backend.display_name} ready")

    return ready


def stop_docker_containers(compose_file: Path, quiet: bool = False) -> None:
    """Stop and remove benchmark Docker containers."""
    if not compose_file.exists():
        return

    if not quiet:
        print("Stopping Docker containers...")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "--remove-orphans"],
        capture_output=True,
    )


def get_runner_for_workload(
    workload: WorkloadConfig,
    host: str,
    port: int,
) -> Any:
    """Get the appropriate runner for a workload.

    Returns the runner for the primary command category in the workload.
    """
    primary_category = workload.get_primary_category()
    runner = RunnerRegistry.get(primary_category, host, port)

    if runner is None:
        raise RuntimeError(f"No runner registered for category: {primary_category.value}")

    if not runner.is_available():
        raise RuntimeError(f"Runner {runner.name} is not available (missing dependencies)")

    return runner


def run_benchmark(
    workload: WorkloadConfig,
    backend: BackendConfig,
    output_dir: Path,
    requests: int,
    host: str = "127.0.0.1",
) -> BenchmarkResult | None:
    """Run a benchmark for a workload against a backend.

    Returns:
        BenchmarkResult or None if benchmark failed
    """
    # Check connectivity
    if not check_connectivity(host, backend.port):
        print(f"  {backend.display_name}: NOT RESPONDING on port {backend.port}")
        return None

    # Get appropriate runner
    try:
        runner = get_runner_for_workload(workload, host, backend.port)
    except RuntimeError as e:
        print(f"  {backend.display_name}: {e}")
        return None

    # Create backend-specific output directory
    backend_output_dir = output_dir / backend.name
    backend_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"  {backend.display_name} ({runner.name})...")

    try:
        result = runner.run(
            workload=workload,
            output_dir=backend_output_dir,
            requests=requests,
            warmup=True,
        )
        result.backend_name = backend.name

        # Print summary
        print(
            f"    {result.total_ops_per_sec:,.0f} ops/sec, "
            f"p50={result.p50_latency_ms:.2f}ms, "
            f"p99={result.p99_latency_ms:.2f}ms"
        )

        if not result.targets_met:
            print(f"    Target failures: {', '.join(result.target_failures)}")

        return result

    except Exception as e:
        print(f"    Error: {e}")
        return None


def generate_report(
    results: dict[str, BenchmarkResult],
    workload: WorkloadConfig,
    output_dir: Path,
) -> None:
    """Generate a Markdown report from benchmark results."""
    report_file = output_dir / "benchmark_report.md"

    with open(report_file, "w") as f:
        f.write(f"# Benchmark Report: {workload.name}\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Description:** {workload.description}\n\n")

        # Commands in workload
        f.write("## Workload\n\n")
        f.write("| Command | Percentage |\n")
        f.write("|---------|------------|\n")
        for cmd, pct in workload.commands.items():
            f.write(f"| {cmd} | {pct}% |\n")
        f.write("\n")

        # Configuration
        f.write("## Configuration\n\n")
        f.write(f"- **Key Pattern:** {workload.keys.pattern.value}\n")
        f.write(f"- **Key Space:** {workload.keys.space_size:,}\n")
        f.write(f"- **Data Size:** {workload.data.size_bytes} bytes\n")
        f.write(f"- **Clients:** {workload.concurrency.clients}\n")
        f.write(f"- **Pipeline:** {workload.concurrency.pipeline}\n")
        f.write("\n")

        # Results summary
        f.write("## Summary\n\n")
        f.write("| Backend | Ops/sec | p50 (ms) | p95 (ms) | p99 (ms) | Targets Met |\n")
        f.write("|---------|---------|----------|----------|----------|-------------|\n")

        for backend_name, result in sorted(results.items()):
            targets = "Yes" if result.targets_met else "No"
            f.write(
                f"| {BACKENDS.get(backend_name, BackendConfig(backend_name, backend_name, 0)).display_name} "
                f"| {result.total_ops_per_sec:,.0f} "
                f"| {result.p50_latency_ms:.2f} "
                f"| {result.p95_latency_ms:.2f} "
                f"| {result.p99_latency_ms:.2f} "
                f"| {targets} |\n"
            )
        f.write("\n")

        # Per-command results
        if any(result.commands for result in results.values()):
            f.write("## Per-Command Results\n\n")

            for backend_name, result in sorted(results.items()):
                if not result.commands:
                    continue

                backend_display = BACKENDS.get(
                    backend_name, BackendConfig(backend_name, backend_name, 0)
                ).display_name
                f.write(f"### {backend_display}\n\n")
                f.write("| Command | Ops/sec | p50 (ms) | p99 (ms) | Errors |\n")
                f.write("|---------|---------|----------|----------|--------|\n")

                for cmd, cmd_result in result.commands.items():
                    f.write(
                        f"| {cmd} "
                        f"| {cmd_result.ops_per_sec:,.0f} "
                        f"| {cmd_result.p50_latency_ms:.2f} "
                        f"| {cmd_result.p99_latency_ms:.2f} "
                        f"| {cmd_result.errors} |\n"
                    )
                f.write("\n")

        # Target failures
        failures = [
            (name, result.target_failures)
            for name, result in results.items()
            if result.target_failures
        ]

        if failures:
            f.write("## Target Failures\n\n")
            for backend_name, target_failures in failures:
                backend_display = BACKENDS.get(
                    backend_name, BackendConfig(backend_name, backend_name, 0)
                ).display_name
                f.write(f"**{backend_display}:**\n")
                for failure in target_failures:
                    f.write(f"- {failure}\n")
                f.write("\n")

    print(f"\nReport generated: {report_file}")


def main() -> None:
    loadtest_dir = script_dir.parent
    default_output_dir = loadtest_dir / "reports"
    compose_file = loadtest_dir / "docker-compose.benchmarks.yml"

    parser = argparse.ArgumentParser(
        description="Unified benchmark runner for FrogDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-w", "--workload", type=str, help="Workload name (from workloads/ directory)"
    )
    parser.add_argument(
        "--backends", type=str, help="Comma-separated backends: frogdb,redis,valkey,dragonfly"
    )
    parser.add_argument("--all", action="store_true", help="Run against all backends")
    parser.add_argument("--start-docker", action="store_true", help="Auto-start Docker containers (tears down stale containers first, cleans up after)")
    parser.add_argument("--stop-docker", action="store_true", help="Stop benchmark Docker containers and exit")
    parser.add_argument(
        "-n", "--requests", type=int, default=100000, help="Total requests to run (default: 100000)"
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=default_output_dir,
        help=f"Output directory (default: {default_output_dir})",
    )
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="Host for backends (default: 127.0.0.1)"
    )
    parser.add_argument("--list", action="store_true", help="List available workloads")
    parser.add_argument("--validate", action="store_true", help="Validate all workload files")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")

    parser.add_argument(
        "--keep-docker", action="store_true", help="Keep Docker containers running after benchmarks"
    )

    args = parser.parse_args()

    # Stop Docker containers and exit
    if args.stop_docker:
        stop_docker_containers(compose_file)
        return

    # List workloads
    if args.list:
        workloads = list_workloads()
        if workloads:
            print("Available workloads:")
            for w in workloads:
                try:
                    config = load_workload(w)
                    print(f"  {w:20} - {config.description}")
                except Exception as e:
                    print(f"  {w:20} - (error: {e})")
        else:
            print("No workloads found in workloads/ directory")
        return

    # Validate workloads
    if args.validate:
        workloads = list_workloads()
        errors = []
        for w in workloads:
            try:
                load_workload(w)
                print(f"  OK: {w}")
            except Exception as e:
                errors.append((w, str(e)))
                print(f"  FAIL: {w} - {e}")

        if errors:
            print(f"\n{len(errors)} workload(s) failed validation")
            sys.exit(1)
        else:
            print(f"\nAll {len(workloads)} workload(s) valid")
        return

    # Require workload for benchmarking
    if not args.workload:
        parser.print_help()
        print("\nError: --workload is required for benchmarking")
        sys.exit(1)

    # Load workload
    try:
        workload = load_workload(args.workload)
    except FileNotFoundError:
        print(f"Error: Workload not found: {args.workload}")
        print("Use --list to see available workloads")
        sys.exit(1)
    except WorkloadValidationError as e:
        print(f"Error: Invalid workload: {e}")
        sys.exit(1)

    # Determine backends
    if args.all:
        backends_to_run = list(BACKENDS.keys())
    elif args.backends:
        backends_to_run = [b.strip().lower() for b in args.backends.split(",")]
        invalid = [b for b in backends_to_run if b not in BACKENDS]
        if invalid:
            print(f"Error: Invalid backends: {invalid}")
            print(f"Valid backends: {list(BACKENDS.keys())}")
            sys.exit(1)
    else:
        # Default to FrogDB only
        backends_to_run = ["frogdb"]

    # Check for required tools
    primary_category = workload.get_primary_category()
    if primary_category == CommandCategory.STRING:
        if not shutil.which("memtier_benchmark"):
            print("Error: memtier_benchmark not found")
            print("Install with: brew install memtier_benchmark")
            sys.exit(1)

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = args.output / f"{args.workload}_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Print header
    print("=" * 70)
    print(f"FrogDB Benchmark: {workload.name}")
    print("=" * 70)
    print(f"Description:    {workload.description}")
    print(f"Commands:       {workload.commands}")
    print(f"Key Pattern:    {workload.keys.pattern.value}")
    print(f"Key Space:      {workload.keys.space_size:,}")
    print(f"Requests:       {args.requests:,}")
    print(f"Backends:       {', '.join(BACKENDS[b].display_name for b in backends_to_run)}")
    print(f"Output:         {report_dir}")
    print("=" * 70)
    print()

    # Start Docker containers if requested
    docker_started = False
    if args.start_docker:
        comparison_backends = [b for b in backends_to_run if b != "frogdb"]
        if not start_docker_containers(comparison_backends, compose_file, include_frogdb=True):
            print("Error: Failed to start Docker containers. Aborting.")
            sys.exit(1)
        docker_started = True
        print()

    try:
        # Run benchmarks
        print("Running benchmarks...")
        print("-" * 70)

        results: dict[str, BenchmarkResult] = {}

        for backend_name in backends_to_run:
            backend = BACKENDS[backend_name]
            result = run_benchmark(
                workload=workload,
                backend=backend,
                output_dir=report_dir,
                requests=args.requests,
                host=args.host,
            )
            if result:
                results[backend_name] = result

        print()

        # Save combined results
        combined_results = {
            "workload": workload.name,
            "timestamp": datetime.now().isoformat(),
            "requests": args.requests,
            "backends": {name: result.to_dict() for name, result in results.items()},
        }

        combined_json = report_dir / "combined_results.json"
        with open(combined_json, "w") as f:
            json.dump(combined_results, f, indent=2)

        # Generate report
        if not args.json:
            generate_report(results, workload, report_dir)

        # Print JSON output if requested
        if args.json:
            print(json.dumps(combined_results, indent=2))
        else:
            print(f"\nResults saved to: {report_dir}")

        print("=" * 70)
    finally:
        # Always clean up Docker containers unless --keep-docker is specified
        if docker_started and not args.keep_docker:
            stop_docker_containers(compose_file)


if __name__ == "__main__":
    main()
