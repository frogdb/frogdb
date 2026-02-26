#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
run_memtier.py - Wrapper for memtier_benchmark against FrogDB

memtier_benchmark is more powerful than redis-benchmark, offering:
- Zipfian key distribution for realistic workloads
- HDR histograms for accurate percentile measurement
- JSON output for automated processing

Usage:
    uv run run_memtier.py [OPTIONS]

Options:
    -h, --host HOST       Server hostname (default: 127.0.0.1)
    -p, --port PORT       Server port (default: 6379)
    -t, --threads N       Number of threads (default: 4)
    -c, --clients N       Clients per thread (default: 25)
    -n, --requests N      Requests per client (default: 10000)
    -d, --datasize N      Data size in bytes (default: 128)
    --ratio R:W           Read:Write ratio (default: 1:1)
    --key-pattern P       Key pattern: P=parallel, R=random, S=sequential, G=gaussian (default: R:R)
    --key-maximum N       Maximum key number (default: 10000000)
    --pipeline N          Pipeline N requests (default: 1)
    -w, --workload NAME   Predefined workload: read-heavy, write-heavy, mixed (default: mixed)
    --json FILE           Output JSON results to file
    --hdr FILE            Output HDR histogram to file
    --help                Show this help message

Examples:
    uv run run_memtier.py                                    # Default settings
    uv run run_memtier.py -t 8 -c 50 --ratio 10:1           # Read-heavy workload
    uv run run_memtier.py -w read-heavy --json results.json # Predefined workload with JSON output
    uv run run_memtier.py --key-pattern G:G --pipeline 10   # Gaussian distribution, pipelining
"""

import argparse
import os
import shutil
import sys
from dataclasses import dataclass


@dataclass
class WorkloadPreset:
    ratio: str
    key_pattern: str


WORKLOAD_PRESETS = {
    # 95% reads - session store scenario
    "read-heavy": WorkloadPreset(ratio="19:1", key_pattern="G:G"),
    # 95% writes - logging scenario
    "write-heavy": WorkloadPreset(ratio="1:19", key_pattern="S:S"),
    # Typical application - 90% reads, 10% writes
    "mixed": WorkloadPreset(ratio="9:1", key_pattern="G:G"),
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wrapper for memtier_benchmark against FrogDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    parser.add_argument("-h", "--host", default="127.0.0.1",
                        help="Server hostname (default: 127.0.0.1)")
    parser.add_argument("-p", "--port", type=int, default=6379,
                        help="Server port (default: 6379)")
    parser.add_argument("-t", "--threads", type=int, default=4,
                        help="Number of threads (default: 4)")
    parser.add_argument("-c", "--clients", type=int, default=25,
                        help="Clients per thread (default: 25)")
    parser.add_argument("-n", "--requests", type=int, default=10000,
                        help="Requests per client (default: 10000)")
    parser.add_argument("-d", "--datasize", type=int, default=128,
                        help="Data size in bytes (default: 128)")
    parser.add_argument("--ratio", default="1:1",
                        help="Read:Write ratio (default: 1:1)")
    parser.add_argument("--key-pattern", default="R:R",
                        help="Key pattern: P=parallel, R=random, S=sequential, G=gaussian (default: R:R)")
    parser.add_argument("--key-maximum", type=int, default=10000000,
                        help="Maximum key number (default: 10000000)")
    parser.add_argument("--pipeline", type=int, default=1,
                        help="Pipeline N requests (default: 1)")
    parser.add_argument("-w", "--workload", choices=["read-heavy", "write-heavy", "mixed"],
                        help="Predefined workload preset")
    parser.add_argument("--json", dest="json_file", metavar="FILE",
                        help="Output JSON results to file")
    parser.add_argument("--hdr", dest="hdr_file", metavar="FILE",
                        help="Output HDR histogram to file")

    args = parser.parse_args()

    # Check if memtier_benchmark is available
    if not shutil.which("memtier_benchmark"):
        print("Error: memtier_benchmark not found", file=sys.stderr)
        print("Install it with: brew install memtier_benchmark (macOS)", file=sys.stderr)
        print("Or build from source: https://github.com/RedisLabs/memtier_benchmark", file=sys.stderr)
        sys.exit(1)

    # Apply workload presets
    ratio = args.ratio
    key_pattern = args.key_pattern
    if args.workload and args.workload in WORKLOAD_PRESETS:
        preset = WORKLOAD_PRESETS[args.workload]
        ratio = preset.ratio
        key_pattern = preset.key_pattern

    # Build command arguments
    cmd_args = [
        "memtier_benchmark",
        "-s", args.host,
        "-p", str(args.port),
        "--threads", str(args.threads),
        "--clients", str(args.clients),
        "--requests", str(args.requests),
        "--data-size", str(args.datasize),
        "--ratio", ratio,
        "--key-pattern", key_pattern,
        "--key-maximum", str(args.key_maximum),
        "--pipeline", str(args.pipeline),
        "--hide-histogram",
        "--print-percentiles", "50,99,99.9",
    ]

    if args.json_file:
        cmd_args.extend(["--json-out-file", args.json_file])

    if args.hdr_file:
        cmd_args.extend(["--hdr-file-prefix", args.hdr_file])

    # Print configuration
    total_clients = args.threads * args.clients
    total_requests = total_clients * args.requests

    print("=" * 60)
    print("FrogDB memtier_benchmark Test")
    print("=" * 60)
    print(f"Host:           {args.host}:{args.port}")
    print(f"Threads:        {args.threads}")
    print(f"Clients/Thread: {args.clients}")
    print(f"Total Clients:  {total_clients}")
    print(f"Requests/Client:{args.requests}")
    print(f"Total Requests: {total_requests}")
    print(f"Data size:      {args.datasize} bytes")
    print(f"Ratio (R:W):    {ratio}")
    print(f"Key pattern:    {key_pattern}")
    print(f"Key maximum:    {args.key_maximum}")
    print(f"Pipeline:       {args.pipeline}")
    if args.workload:
        print(f"Workload:       {args.workload}")
    if args.json_file:
        print(f"JSON output:    {args.json_file}")
    print("=" * 60)
    print()

    # Run benchmark (replace current process)
    os.execvp("memtier_benchmark", cmd_args)


if __name__ == "__main__":
    main()
