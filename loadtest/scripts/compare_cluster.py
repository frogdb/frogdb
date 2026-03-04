#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""
compare_cluster.py - Multi-threaded/multi-process cluster benchmark comparison

Compares FrogDB (3 internal shards) vs Redis Cluster (3 masters) vs
Valkey Cluster (3 masters) vs Dragonfly (3 proactor threads).

All services and the benchmark client (memtier_benchmark) run inside Docker
on a shared bridge network. This avoids macOS Docker Desktop host networking
issues while allowing Redis/Valkey cluster gossip and MOVED redirects to
work correctly.

Prerequisites:
    - Docker installed (Docker Desktop on macOS)
    - FrogDB cross-compiled binary: just cross-build-arm

Usage:
    cd loadtest/scripts
    uv run compare_cluster.py [OPTIONS]

Options:
    --workload NAME       Workload preset: read-heavy, write-heavy, mixed (default: mixed)
    -n, --requests N      Requests per client (default: 10000)
    --backends LIST       Comma-separated: redis,valkey,dragonfly or "all" (default: all)
    --keep                Don't stop containers after benchmarks
    -o, --output DIR      Output directory for results

Examples:
    uv run compare_cluster.py --workload mixed -n 10000
    uv run compare_cluster.py --backends redis,valkey -n 5000
    uv run compare_cluster.py --workload read-heavy --keep
"""

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

# --- Configuration ---

COMPOSE_FILE = "docker-compose.cluster-benchmarks.yml"
COMPOSE_PROJECT = "cluster-bench"
DOCKER_NETWORK = "cluster-benchnet"

# Docker network IPs (must match docker-compose.cluster-benchmarks.yml)
FROGDB_IP = "172.30.0.10"
REDIS_CLUSTER_IPS = ["172.30.0.11", "172.30.0.12", "172.30.0.13"]
VALKEY_CLUSTER_IPS = ["172.30.0.21", "172.30.0.22", "172.30.0.23"]
DRAGONFLY_IP = "172.30.0.30"
CLUSTER_PORT = 6379

# Container names
REDIS_CONTAINERS = ["cluster-bench-redis-1", "cluster-bench-redis-2", "cluster-bench-redis-3"]
VALKEY_CONTAINERS = ["cluster-bench-valkey-1", "cluster-bench-valkey-2", "cluster-bench-valkey-3"]

# Services in docker-compose for each backend
BACKEND_SERVICES = {
    "frogdb": ["frogdb"],
    "redis": ["redis-1", "redis-2", "redis-3"],
    "valkey": ["valkey-1", "valkey-2", "valkey-3"],
    "dragonfly": ["dragonfly"],
}

BACKEND_DISPLAY_NAMES = {
    "frogdb": "FrogDB (3 shards)",
    "redis": "Redis Cluster (3 masters)",
    "valkey": "Valkey Cluster (3 masters)",
    "dragonfly": "Dragonfly (3 threads)",
}


@dataclass
class WorkloadConfig:
    ratio: str
    key_pattern: str


WORKLOAD_CONFIGS = {
    "read-heavy": WorkloadConfig(ratio="19:1", key_pattern="G:G"),
    "write-heavy": WorkloadConfig(ratio="1:19", key_pattern="S:S"),
    "mixed": WorkloadConfig(ratio="9:1", key_pattern="G:G"),
}


# --- Utility Functions ---


def compose_cmd() -> list[str]:
    """Base docker compose command with project name and file."""
    compose_file = get_compose_file()
    return ["docker", "compose", "-f", str(compose_file), "-p", COMPOSE_PROJECT]


def docker_exec(container: str, cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a command inside a running container."""
    return subprocess.run(
        ["docker", "exec", container] + cmd,
        capture_output=True,
        text=True,
        **kwargs,
    )


def wait_for_container_port(container: str, port: int, max_wait: int = 60) -> bool:
    """Wait for a port to be reachable inside a container using redis-cli PING."""
    start = time.time()
    while time.time() - start < max_wait:
        result = docker_exec(container, ["redis-cli", "-p", str(port), "PING"])
        if result.returncode == 0 and "PONG" in result.stdout:
            return True
        time.sleep(0.5)
    return False


def extract_memtier_metrics(data: dict) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "ops_per_sec": 0,
        "get_ops_per_sec": 0,
        "set_ops_per_sec": 0,
        "get_latency": {},
        "set_latency": {},
        "total_latency": {},
    }

    if "ALL STATS" in data:
        stats = data["ALL STATS"]
        if "Gets" in stats:
            gets = stats["Gets"]
            metrics["get_ops_per_sec"] = gets.get("Ops/sec", 0)
            if "Percentile Latencies" in gets:
                pcts = gets["Percentile Latencies"]
                metrics["get_latency"] = {
                    "p50_ms": pcts.get("p50.00", 0),
                    "p99_ms": pcts.get("p99.00", 0),
                }
        if "Sets" in stats:
            sets = stats["Sets"]
            metrics["set_ops_per_sec"] = sets.get("Ops/sec", 0)
            if "Percentile Latencies" in sets:
                pcts = sets["Percentile Latencies"]
                metrics["set_latency"] = {
                    "p50_ms": pcts.get("p50.00", 0),
                    "p99_ms": pcts.get("p99.00", 0),
                }
        if "Totals" in stats:
            totals = stats["Totals"]
            metrics["ops_per_sec"] = totals.get("Ops/sec", 0)
            if "Percentile Latencies" in totals:
                pcts = totals["Percentile Latencies"]
                metrics["total_latency"] = {
                    "p50_ms": pcts.get("p50.00", 0),
                    "p90_ms": pcts.get("p90.00", 0),
                    "p95_ms": pcts.get("p95.00", 0),
                    "p99_ms": pcts.get("p99.00", 0),
                    "p99.9_ms": pcts.get("p99.90", 0),
                }

    return metrics


def format_ops(n: float) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    else:
        return f"{n:.0f}"


def format_latency(ms: float) -> str:
    if ms == 0:
        return "-"
    elif ms < 1:
        return f"{ms * 1000:.0f}us"
    else:
        return f"{ms:.3f}ms"


# --- Docker Compose Management ---


def get_compose_file() -> Path:
    script_dir = Path(__file__).parent.resolve()
    return script_dir.parent / COMPOSE_FILE


def build_memtier_image() -> bool:
    """Build the memtier_benchmark Docker image (skips if already cached)."""
    # Check if image already exists
    check = subprocess.run(
        ["docker", "images", "-q", f"{COMPOSE_PROJECT}-memtier"],
        capture_output=True,
        text=True,
    )
    if check.returncode == 0 and check.stdout.strip():
        print("memtier_benchmark image: cached")
        return True

    print("Building memtier_benchmark image (first run may take a few minutes)...")
    result = subprocess.run(
        compose_cmd() + ["build", "memtier"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error building memtier image: {result.stderr}", file=sys.stderr)
        return False
    print("  memtier image: OK")
    return True


def start_services(backends: list[str]) -> bool:
    compose_file = get_compose_file()
    if not compose_file.exists():
        print(f"Error: {compose_file} not found", file=sys.stderr)
        return False

    # Collect all services to start
    services = ["frogdb"]  # Always start FrogDB
    for backend in backends:
        services.extend(BACKEND_SERVICES.get(backend, []))

    print(f"Starting services: {', '.join(services)}")
    result = subprocess.run(
        compose_cmd() + ["up", "-d"] + services,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error starting containers: {result.stderr}", file=sys.stderr)
        return False

    return True


def stop_services() -> None:
    compose_file = get_compose_file()
    if not compose_file.exists():
        return
    print("Stopping containers...")
    subprocess.run(
        compose_cmd() + ["down"],
        capture_output=True,
    )


# --- Wait for All Nodes ---


def wait_for_backends(backends: list[str]) -> bool:
    all_ok = True

    # FrogDB - check via docker exec
    print(f"  Waiting for FrogDB ({FROGDB_IP}:{CLUSTER_PORT})...")
    if not wait_for_container_port("cluster-bench-frogdb", CLUSTER_PORT):
        print("  ERROR: FrogDB not responding", file=sys.stderr)
        all_ok = False
    else:
        print("  FrogDB: OK")

    for backend in backends:
        if backend == "redis":
            for container in REDIS_CONTAINERS:
                print(f"  Waiting for {container}...")
                if not wait_for_container_port(container, CLUSTER_PORT):
                    print(f"  ERROR: {container} not responding", file=sys.stderr)
                    all_ok = False
                else:
                    print(f"  {container}: OK")
        elif backend == "valkey":
            for container in VALKEY_CONTAINERS:
                print(f"  Waiting for {container}...")
                if not wait_for_container_port(container, CLUSTER_PORT):
                    print(f"  ERROR: {container} not responding", file=sys.stderr)
                    all_ok = False
                else:
                    print(f"  {container}: OK")
        elif backend == "dragonfly":
            print(f"  Waiting for Dragonfly ({DRAGONFLY_IP}:{CLUSTER_PORT})...")
            if not wait_for_container_port("cluster-bench-dragonfly", CLUSTER_PORT):
                print("  ERROR: Dragonfly not responding", file=sys.stderr)
                all_ok = False
            else:
                print("  Dragonfly: OK")

    return all_ok


# --- Cluster Initialization ---


def init_redis_cluster() -> bool:
    """Create Redis Cluster from the 3 master nodes via docker exec."""
    print("Initializing Redis Cluster...")
    endpoints = [f"{ip}:{CLUSTER_PORT}" for ip in REDIS_CLUSTER_IPS]
    cmd = ["redis-cli", "--cluster", "create"] + endpoints + ["--cluster-yes"]

    result = docker_exec(REDIS_CONTAINERS[0], cmd)
    if result.returncode != 0:
        print(f"Error creating Redis Cluster: {result.stderr}", file=sys.stderr)
        print(f"stdout: {result.stdout}", file=sys.stderr)
        return False

    return wait_for_cluster_ok(REDIS_CONTAINERS[0], "Redis")


def init_valkey_cluster() -> bool:
    """Create Valkey Cluster from the 3 master nodes via docker exec."""
    print("Initializing Valkey Cluster...")
    endpoints = [f"{ip}:{CLUSTER_PORT}" for ip in VALKEY_CLUSTER_IPS]

    # Valkey containers have valkey-cli
    cmd = ["valkey-cli", "--cluster", "create"] + endpoints + ["--cluster-yes"]
    result = docker_exec(VALKEY_CONTAINERS[0], cmd)

    if result.returncode != 0:
        # Fall back to redis-cli if valkey-cli not found
        cmd = ["redis-cli", "--cluster", "create"] + endpoints + ["--cluster-yes"]
        result = docker_exec(VALKEY_CONTAINERS[0], cmd)

    if result.returncode != 0:
        print(f"Error creating Valkey Cluster: {result.stderr}", file=sys.stderr)
        print(f"stdout: {result.stdout}", file=sys.stderr)
        return False

    return wait_for_cluster_ok(VALKEY_CONTAINERS[0], "Valkey")


def wait_for_cluster_ok(container: str, name: str, max_wait: int = 30) -> bool:
    """Wait for CLUSTER INFO to show cluster_state:ok via docker exec."""
    start = time.time()
    while time.time() - start < max_wait:
        result = docker_exec(container, ["redis-cli", "-p", str(CLUSTER_PORT), "CLUSTER", "INFO"])
        if result.returncode == 0 and "cluster_state:ok" in result.stdout:
            for line in result.stdout.split("\n"):
                if line.startswith("cluster_known_nodes:"):
                    nodes = int(line.split(":")[1].strip())
                    if nodes >= 3:
                        print(f"  {name} Cluster: OK ({nodes} nodes, state=ok)")
                        return True
        time.sleep(1)

    print(f"  ERROR: {name} Cluster did not stabilize within {max_wait}s", file=sys.stderr)
    return False


# --- Benchmark Execution ---


def run_memtier(
    host: str,
    port: int,
    json_file: Path,
    txt_file: Path,
    ratio: str,
    key_pattern: str,
    requests: int,
    name: str,
    cluster_mode: bool = False,
    threads: int = 4,
    clients: int = 25,
) -> bool:
    """Run memtier_benchmark inside Docker on the cluster network."""
    # Mount the report directory as /output in the container
    host_dir = json_file.parent.resolve()
    container_json = f"/output/{json_file.name}"

    cmd = compose_cmd() + [
        "run",
        "--rm",
        "-T",
        "-v",
        f"{host_dir}:/output",
        "memtier",
        "-s",
        host,
        "-p",
        str(port),
        "--threads",
        str(threads),
        "--clients",
        str(clients),
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
        container_json,
    ]

    if cluster_mode:
        cmd.append("--cluster-mode")

    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    # Save text output
    with open(txt_file, "w") as f:
        f.write(result.stdout)

    # Print summary line
    for line in result.stdout.strip().split("\n"):
        if "Totals" in line:
            print(f"  {name}: {line.strip()}")
            break

    if result.returncode != 0:
        print(f"  {name}: memtier exited with code {result.returncode}", file=sys.stderr)
        # Print last few lines for debugging
        lines = result.stdout.strip().split("\n")
        for line in lines[-5:]:
            print(f"    {line}", file=sys.stderr)
        return False

    return True


def run_benchmarks(
    backends: list[str],
    workload: WorkloadConfig,
    requests: int,
    report_dir: Path,
    threads: int = 4,
    clients: int = 25,
) -> dict[str, Any]:
    """Run memtier against all backends and collect results."""
    results: dict[str, Any] = {}

    # FrogDB (single endpoint, internal routing)
    print(f"\n  FrogDB (3 shards, {FROGDB_IP}:{CLUSTER_PORT}):")
    frogdb_json = report_dir / "frogdb.json"
    frogdb_txt = report_dir / "frogdb.txt"
    if run_memtier(
        FROGDB_IP,
        CLUSTER_PORT,
        frogdb_json,
        frogdb_txt,
        workload.ratio,
        workload.key_pattern,
        requests,
        name="FrogDB",
        threads=threads,
        clients=clients,
    ):
        if frogdb_json.exists():
            with open(frogdb_json) as f:
                results["frogdb"] = {
                    "name": BACKEND_DISPLAY_NAMES["frogdb"],
                    "memtier": json.load(f),
                }

    for backend in backends:
        if backend == "redis":
            print(f"\n  Redis Cluster (3 masters, {REDIS_CLUSTER_IPS[0]}:{CLUSTER_PORT}):")
            redis_json = report_dir / "redis.json"
            redis_txt = report_dir / "redis.txt"
            if run_memtier(
                REDIS_CLUSTER_IPS[0],
                CLUSTER_PORT,
                redis_json,
                redis_txt,
                workload.ratio,
                workload.key_pattern,
                requests,
                name="Redis Cluster",
                cluster_mode=True,
                threads=threads,
                clients=clients,
            ):
                if redis_json.exists():
                    with open(redis_json) as f:
                        results["redis"] = {
                            "name": BACKEND_DISPLAY_NAMES["redis"],
                            "memtier": json.load(f),
                        }

        elif backend == "valkey":
            print(f"\n  Valkey Cluster (3 masters, {VALKEY_CLUSTER_IPS[0]}:{CLUSTER_PORT}):")
            valkey_json = report_dir / "valkey.json"
            valkey_txt = report_dir / "valkey.txt"
            if run_memtier(
                VALKEY_CLUSTER_IPS[0],
                CLUSTER_PORT,
                valkey_json,
                valkey_txt,
                workload.ratio,
                workload.key_pattern,
                requests,
                name="Valkey Cluster",
                cluster_mode=True,
                threads=threads,
                clients=clients,
            ):
                if valkey_json.exists():
                    with open(valkey_json) as f:
                        results["valkey"] = {
                            "name": BACKEND_DISPLAY_NAMES["valkey"],
                            "memtier": json.load(f),
                        }

        elif backend == "dragonfly":
            print(f"\n  Dragonfly (3 threads, {DRAGONFLY_IP}:{CLUSTER_PORT}):")
            df_json = report_dir / "dragonfly.json"
            df_txt = report_dir / "dragonfly.txt"
            if run_memtier(
                DRAGONFLY_IP,
                CLUSTER_PORT,
                df_json,
                df_txt,
                workload.ratio,
                workload.key_pattern,
                requests,
                name="Dragonfly",
                threads=threads,
                clients=clients,
            ):
                if df_json.exists():
                    with open(df_json) as f:
                        results["dragonfly"] = {
                            "name": BACKEND_DISPLAY_NAMES["dragonfly"],
                            "memtier": json.load(f),
                        }

    return results


# --- Report Generation ---


def generate_cluster_report(
    results: dict[str, Any],
    workload_name: str,
    requests: int,
    report_dir: Path,
) -> Path:
    """Generate a Markdown report for cluster benchmark results."""
    lines = []

    lines.append("# FrogDB Cluster Benchmark Report")
    lines.append("")
    lines.append(f"**Generated:** {datetime.now().isoformat()}")
    lines.append(f"**Workload:** {workload_name}")
    lines.append(f"**Requests per client:** {requests}")
    lines.append("")

    # Architecture overview
    lines.append("## Architecture")
    lines.append("")
    lines.append("| Backend | Configuration | Endpoint |")
    lines.append("|---------|--------------|----------|")
    lines.append(f"| FrogDB | 1 process, 3 internal shards | `{FROGDB_IP}:{CLUSTER_PORT}` |")
    lines.append(
        f"| Redis Cluster | 3 master processes | `{REDIS_CLUSTER_IPS[0]}-{REDIS_CLUSTER_IPS[-1]}:{CLUSTER_PORT}` |"
    )
    lines.append(
        f"| Valkey Cluster | 3 master processes | `{VALKEY_CLUSTER_IPS[0]}-{VALKEY_CLUSTER_IPS[-1]}:{CLUSTER_PORT}` |"
    )
    lines.append(f"| Dragonfly | 1 process, 3 proactor threads | `{DRAGONFLY_IP}:{CLUSTER_PORT}` |")
    lines.append("")

    # Summary table
    backend_names = list(results.keys())
    if backend_names:
        lines.append("## Throughput Summary")
        lines.append("")

        header = "| Metric |"
        separator = "|--------|"
        for name in backend_names:
            display = results[name].get("name", name)
            header += f" {display} |"
            separator += "--------|"
        lines.append(header)
        lines.append(separator)

        # Extract metrics
        all_metrics = {}
        for name in backend_names:
            if "memtier" in results[name]:
                all_metrics[name] = extract_memtier_metrics(results[name]["memtier"])
            else:
                all_metrics[name] = {}

        # Total ops/sec
        row = "| Total ops/sec |"
        for name in backend_names:
            ops = all_metrics.get(name, {}).get("ops_per_sec", 0)
            row += f" {format_ops(ops)} |"
        lines.append(row)

        # GET ops/sec
        row = "| GET ops/sec |"
        for name in backend_names:
            ops = all_metrics.get(name, {}).get("get_ops_per_sec", 0)
            row += f" {format_ops(ops)} |"
        lines.append(row)

        # SET ops/sec
        row = "| SET ops/sec |"
        for name in backend_names:
            ops = all_metrics.get(name, {}).get("set_ops_per_sec", 0)
            row += f" {format_ops(ops)} |"
        lines.append(row)

        # p99 latency
        row = "| p99 latency |"
        for name in backend_names:
            lat = all_metrics.get(name, {}).get("total_latency", {}).get("p99_ms", 0)
            row += f" {format_latency(lat)} |"
        lines.append(row)

        lines.append("")

        # Performance comparison vs FrogDB
        if "frogdb" in all_metrics:
            frogdb_ops = all_metrics["frogdb"].get("ops_per_sec", 0)
            if frogdb_ops > 0:
                lines.append("### Performance Comparison (vs FrogDB)")
                lines.append("")
                for name in backend_names:
                    if name == "frogdb":
                        continue
                    other_ops = all_metrics[name].get("ops_per_sec", 0)
                    if other_ops > 0:
                        display = results[name].get("name", name)
                        ratio = frogdb_ops / other_ops
                        if ratio >= 1.0:
                            lines.append(f"- FrogDB is **{ratio:.2f}x** faster than {display}")
                        else:
                            lines.append(f"- FrogDB is **{1 / ratio:.2f}x** slower than {display}")
                lines.append("")

    # Methodology
    lines.append("## Methodology")
    lines.append("")
    lines.append("- **Benchmark tool:** memtier_benchmark (inside Docker)")
    lines.append("- **Threads:** 4")
    lines.append("- **Clients per thread:** 25")
    lines.append("- **Total concurrent clients:** 100")
    lines.append("- **Data size:** 128 bytes")
    lines.append("- **Key space:** 10M keys")
    lines.append("- **Pipeline depth:** 1 (no pipelining)")
    lines.append("")
    lines.append("All services and the benchmark client run on a shared Docker bridge network")
    lines.append("(`cluster-benchnet`). Redis/Valkey clusters use `memtier --cluster-mode` which")
    lines.append("handles MOVED redirects automatically. FrogDB and Dragonfly use standard")
    lines.append("single-endpoint mode (internal routing).")
    lines.append("")

    report_path = report_dir / "cluster_benchmark_report.md"
    with open(report_path, "w") as f:
        f.write("\n".join(lines))

    return report_path


def print_summary(results: dict[str, Any]) -> None:
    """Print a compact summary to stdout."""
    print()
    print("=" * 70)
    print("Results Summary")
    print("=" * 70)
    print()
    print(f"{'Backend':<30} {'ops/sec':>12} {'GET ops/s':>12} {'SET ops/s':>12} {'p99':>10}")
    print("-" * 76)

    for name, data in results.items():
        if "memtier" not in data:
            continue
        metrics = extract_memtier_metrics(data["memtier"])
        display = data.get("name", name)
        ops = format_ops(metrics.get("ops_per_sec", 0))
        get_ops = format_ops(metrics.get("get_ops_per_sec", 0))
        set_ops = format_ops(metrics.get("set_ops_per_sec", 0))
        p99 = format_latency(metrics.get("total_latency", {}).get("p99_ms", 0))
        print(f"{display:<30} {ops:>12} {get_ops:>12} {set_ops:>12} {p99:>10}")

    print()


# --- Main ---


def main() -> None:
    script_dir = Path(__file__).parent.resolve()
    loadtest_dir = script_dir.parent
    default_output_dir = loadtest_dir / "reports"

    parser = argparse.ArgumentParser(
        description="Cluster benchmark: FrogDB vs Redis Cluster vs Valkey Cluster vs Dragonfly",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-w",
        "--workload",
        default="mixed",
        choices=["read-heavy", "write-heavy", "mixed"],
        help="Workload preset (default: mixed)",
    )
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=10000,
        help="Requests per client (default: 10000)",
    )
    parser.add_argument(
        "--backends",
        type=str,
        default="all",
        help="Comma-separated: redis,valkey,dragonfly or 'all' (default: all)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="memtier threads (default: 4)",
    )
    parser.add_argument(
        "--clients",
        type=int,
        default=25,
        help="memtier clients per thread (default: 25)",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Don't stop containers after benchmarks",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=default_output_dir,
        help=f"Output directory for results (default: {default_output_dir})",
    )

    args = parser.parse_args()

    # Parse backends
    if args.backends == "all":
        backends = ["redis", "valkey", "dragonfly"]
    else:
        backends = [b.strip().lower() for b in args.backends.split(",")]
        valid = {"redis", "valkey", "dragonfly"}
        invalid = [b for b in backends if b not in valid]
        if invalid:
            print(f"Error: Invalid backends: {invalid}. Valid: {sorted(valid)}", file=sys.stderr)
            sys.exit(1)

    workload = WORKLOAD_CONFIGS[args.workload]

    # Create report directory
    args.output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = args.output / f"cluster_{args.workload}_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("FrogDB Cluster Benchmark Suite")
    print("=" * 70)
    print(f"Workload:     {args.workload} (ratio {workload.ratio})")
    print(f"Requests:     {args.requests} per client")
    print(f"Threads:      {args.threads}")
    print(f"Clients:      {args.clients} per thread ({args.threads * args.clients} total)")
    print(f"Backends:     FrogDB, {', '.join(BACKEND_DISPLAY_NAMES[b] for b in backends)}")
    print(f"Output:       {report_dir}")
    print("=" * 70)
    print()

    # Build memtier image first
    if not build_memtier_image():
        sys.exit(1)

    # Start containers
    print("\nStarting containers...")
    if not start_services(backends):
        sys.exit(1)

    # Wait for all nodes
    print("\nWaiting for nodes to be ready...")
    if not wait_for_backends(backends):
        print(
            "Some backends failed to start. Continuing with available backends...", file=sys.stderr
        )

    # Initialize clusters
    print()
    if "redis" in backends:
        if not init_redis_cluster():
            print(
                "Warning: Redis Cluster initialization failed, removing from benchmarks",
                file=sys.stderr,
            )
            backends.remove("redis")

    if "valkey" in backends:
        if not init_valkey_cluster():
            print(
                "Warning: Valkey Cluster initialization failed, removing from benchmarks",
                file=sys.stderr,
            )
            backends.remove("valkey")

    # Run benchmarks
    print()
    print("-" * 70)
    print("Running benchmarks...")
    print("-" * 70)

    results = run_benchmarks(
        backends, workload, args.requests, report_dir, args.threads, args.clients
    )

    # Save combined results
    combined = {
        "timestamp": datetime.now().isoformat(),
        "workload": args.workload,
        "requests_per_client": args.requests,
        "mode": "cluster",
        "backends": results,
    }
    combined_json = report_dir / "combined_results.json"
    with open(combined_json, "w") as f:
        json.dump(combined, f, indent=2)

    # Try to use generate_report.py if available
    generate_script = script_dir / "generate_report.py"
    if generate_script.exists():
        report_file = report_dir / "benchmark_report.md"
        gen_result = subprocess.run(
            [
                sys.executable,
                str(generate_script),
                "--input",
                str(combined_json),
                "--output",
                str(report_file),
            ],
            capture_output=True,
            text=True,
        )
        if gen_result.returncode == 0:
            print(f"\nReport (generic): {report_file}")

    # Generate cluster-specific report
    cluster_report = generate_cluster_report(results, args.workload, args.requests, report_dir)
    print(f"Report (cluster): {cluster_report}")

    # Print summary
    print_summary(results)

    print(f"Results saved to: {report_dir}")
    print("=" * 70)

    # Teardown
    if not args.keep:
        print()
        stop_services()
    else:
        print("\n--keep specified, containers left running.")
        print(f"To stop: docker compose -f loadtest/{COMPOSE_FILE} -p {COMPOSE_PROJECT} down")


if __name__ == "__main__":
    main()
