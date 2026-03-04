#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
compare_redis.py - Compare FrogDB performance against real Redis

Runs identical memtier_benchmark workloads against both FrogDB and Redis,
then generates a side-by-side comparison report.

Prerequisites:
    - FrogDB running on port 6379 (default)
    - Redis running on port 6380
    - memtier_benchmark installed

Usage:
    uv run compare_redis.py [OPTIONS]

Options:
    --frogdb-port PORT    FrogDB port (default: 6379)
    --redis-port PORT     Redis port (default: 6380)
    -w, --workload NAME   Workload: read-heavy, write-heavy, mixed (default: mixed)
    -n, --requests N      Requests per client (default: 10000)
    -o, --output DIR      Output directory for results (default: ./reports)
    --help                Show this help message

Examples:
    uv run compare_redis.py                          # Default comparison
    uv run compare_redis.py -w read-heavy            # Read-heavy workload
    uv run compare_redis.py -n 100000 -o /tmp/bench  # More requests, custom output
"""

import argparse
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class WorkloadConfig:
    ratio: str
    key_pattern: str


WORKLOAD_CONFIGS = {
    "read-heavy": WorkloadConfig(ratio="19:1", key_pattern="G:G"),
    "write-heavy": WorkloadConfig(ratio="1:19", key_pattern="S:S"),
    "mixed": WorkloadConfig(ratio="9:1", key_pattern="G:G"),
}


def check_connectivity(host: str, port: int) -> bool:
    """Check if a server is reachable on the given host:port."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect((host, port))
            return True
    except (TimeoutError, OSError):
        return False


def run_memtier(
    host: str,
    port: int,
    json_file: Path,
    txt_file: Path,
    ratio: str,
    key_pattern: str,
    requests: int,
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
        "--print-percentiles",
        "--json-out-file",
        str(json_file),
    ]

    with open(txt_file, "w") as f:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        f.write(result.stdout)
        print(result.stdout)

    return result.returncode


def main() -> None:
    script_dir = Path(__file__).parent.resolve()
    default_output_dir = script_dir.parent / "reports"

    parser = argparse.ArgumentParser(
        description="Compare FrogDB performance against real Redis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--frogdb-port", type=int, default=6379, help="FrogDB port (default: 6379)")
    parser.add_argument("--redis-port", type=int, default=6380, help="Redis port (default: 6380)")
    parser.add_argument(
        "-w",
        "--workload",
        default="mixed",
        choices=["read-heavy", "write-heavy", "mixed"],
        help="Workload preset (default: mixed)",
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

    args = parser.parse_args()

    # Check if memtier_benchmark is available
    if not shutil.which("memtier_benchmark"):
        print("Error: memtier_benchmark not found", file=sys.stderr)
        sys.exit(1)

    # Create output directory
    args.output.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = args.output / f"compare_{args.workload}_{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Get workload configuration
    config = WORKLOAD_CONFIGS[args.workload]
    host = "127.0.0.1"

    print("=" * 60)
    print("FrogDB vs Redis Comparison Test")
    print("=" * 60)
    print(f"Workload:     {args.workload}")
    print(f"Ratio (R:W):  {config.ratio}")
    print(f"Key pattern:  {config.key_pattern}")
    print(f"Requests:     {args.requests} per client")
    print(f"FrogDB:       {host}:{args.frogdb_port}")
    print(f"Redis:        {host}:{args.redis_port}")
    print(f"Output:       {report_dir}")
    print("=" * 60)
    print()

    # Check connectivity
    print("Checking FrogDB connectivity...")
    if not check_connectivity(host, args.frogdb_port):
        print(f"Warning: FrogDB does not appear to be running on {host}:{args.frogdb_port}")
        print("Start it with: cargo run --release")

    print("Checking Redis connectivity...")
    if not check_connectivity(host, args.redis_port):
        print(f"Warning: Redis does not appear to be running on {host}:{args.redis_port}")
        print(f"Start it with: redis-server --port {args.redis_port}")

    print()

    # Run FrogDB benchmark
    print("Running FrogDB benchmark...")
    print("-" * 60)
    frogdb_json = report_dir / "frogdb.json"
    frogdb_txt = report_dir / "frogdb.txt"
    run_memtier(
        host=host,
        port=args.frogdb_port,
        json_file=frogdb_json,
        txt_file=frogdb_txt,
        ratio=config.ratio,
        key_pattern=config.key_pattern,
        requests=args.requests,
    )

    print()

    # Run Redis benchmark
    print("Running Redis benchmark...")
    print("-" * 60)
    redis_json = report_dir / "redis.json"
    redis_txt = report_dir / "redis.txt"
    run_memtier(
        host=host,
        port=args.redis_port,
        json_file=redis_json,
        txt_file=redis_txt,
        ratio=config.ratio,
        key_pattern=config.key_pattern,
        requests=args.requests,
    )

    print()
    print("=" * 60)
    print("Generating Comparison Report")
    print("=" * 60)

    # Generate comparison using the Python script
    parse_script = script_dir / "parse_results.py"
    comparison_file = report_dir / "comparison.txt"

    if parse_script.exists():
        result = subprocess.run(
            [
                sys.executable,
                str(parse_script),
                "--frogdb",
                str(frogdb_json),
                "--redis",
                str(redis_json),
                "--output",
                str(comparison_file),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            print(comparison_file.read_text())
        else:
            print(f"Error running parse_results.py: {result.stderr}", file=sys.stderr)
    else:
        print("Note: parse_results.py not found, showing raw results")
        print()
        print("FrogDB Results:")
        print("-" * 60)
        frogdb_lines = frogdb_txt.read_text().splitlines()
        for line in frogdb_lines[-20:]:
            print(line)
        print()
        print("Redis Results:")
        print("-" * 60)
        redis_lines = redis_txt.read_text().splitlines()
        for line in redis_lines[-20:]:
            print(line)

    print()
    print(f"Results saved to: {report_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
