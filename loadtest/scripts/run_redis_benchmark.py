#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
run_redis_benchmark.py - Wrapper for redis-benchmark against FrogDB

Usage:
    uv run run_redis_benchmark.py [OPTIONS]

Options:
    -h, --host HOST       Server hostname (default: 127.0.0.1)
    -p, --port PORT       Server port (default: 6379)
    -c, --clients N       Number of parallel connections (default: 50)
    -n, --requests N      Total number of requests (default: 100000)
    -d, --datasize N      Data size in bytes for SET/GET (default: 128)
    -P, --pipeline N      Pipeline N requests (default: 1)
    -t, --tests TESTS     Comma-separated list of tests to run (default: all)
    -w, --workload NAME   Predefined workload: read-heavy, write-heavy, mixed, all
    --csv                 Output in CSV format
    --help                Show this help message

Examples:
    uv run run_redis_benchmark.py                           # Default settings
    uv run run_redis_benchmark.py -c 100 -n 1000000        # 100 clients, 1M requests
    uv run run_redis_benchmark.py -P 16 -w read-heavy      # Pipeline 16, read-heavy workload
    uv run run_redis_benchmark.py --csv > results.csv      # CSV output
"""

import argparse
import os
import shutil
import sys


def get_workload_tests(workload: str) -> str | None:
    """Get the test string for a predefined workload."""
    workloads = {
        # 95% reads - simulate session store / cache
        "read-heavy": "get,get,get,get,get,get,get,get,get,get,get,get,get,get,get,get,get,get,get,set",
        # 95% writes - simulate logging / queue
        "write-heavy": "set,set,set,set,set,set,set,set,set,set,set,set,set,set,set,set,set,set,set,get",
        # 50/50 - balanced workload
        "mixed": "get,set",
        # All supported commands
        "all": None,
    }
    return workloads.get(workload)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wrapper for redis-benchmark against FrogDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument("--help", action="help", help="Show this help message and exit")
    parser.add_argument(
        "-h", "--host", default="127.0.0.1", help="Server hostname (default: 127.0.0.1)"
    )
    parser.add_argument("-p", "--port", type=int, default=6379, help="Server port (default: 6379)")
    parser.add_argument(
        "-c", "--clients", type=int, default=50, help="Number of parallel connections (default: 50)"
    )
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=100000,
        help="Total number of requests (default: 100000)",
    )
    parser.add_argument(
        "-d",
        "--datasize",
        type=int,
        default=128,
        help="Data size in bytes for SET/GET (default: 128)",
    )
    parser.add_argument(
        "-P", "--pipeline", type=int, default=1, help="Pipeline N requests (default: 1)"
    )
    parser.add_argument("-t", "--tests", default="", help="Comma-separated list of tests to run")
    parser.add_argument(
        "-w",
        "--workload",
        default="all",
        choices=["read-heavy", "write-heavy", "mixed", "all"],
        help="Predefined workload (default: all)",
    )
    parser.add_argument("--csv", action="store_true", help="Output in CSV format")

    args = parser.parse_args()

    # Check if redis-benchmark is available
    if not shutil.which("redis-benchmark"):
        print("Error: redis-benchmark not found", file=sys.stderr)
        print(
            "Install it with: brew install redis (macOS) or apt install redis-tools (Ubuntu)",
            file=sys.stderr,
        )
        sys.exit(1)

    # Determine tests to run
    tests = args.tests
    if not tests and args.workload:
        tests = get_workload_tests(args.workload) or ""

    # Build command arguments
    cmd_args = [
        "redis-benchmark",
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-c",
        str(args.clients),
        "-n",
        str(args.requests),
        "-d",
        str(args.datasize),
        "-P",
        str(args.pipeline),
    ]

    if tests:
        cmd_args.extend(["-t", tests])

    if args.csv:
        cmd_args.append("--csv")

    # Print configuration (unless CSV output)
    if not args.csv:
        print("=" * 60)
        print("FrogDB redis-benchmark Test")
        print("=" * 60)
        print(f"Host:       {args.host}:{args.port}")
        print(f"Clients:    {args.clients}")
        print(f"Requests:   {args.requests}")
        print(f"Data size:  {args.datasize} bytes")
        print(f"Pipeline:   {args.pipeline}")
        print(f"Workload:   {args.workload}")
        if tests:
            print(f"Tests:      {tests}")
        print("=" * 60)
        print()

    # Run benchmark (replace current process)
    os.execvp("redis-benchmark", cmd_args)


if __name__ == "__main__":
    main()
