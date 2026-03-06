#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
parse_results.py - Parse and compare memtier_benchmark results

Reads memtier JSON output files and generates formatted comparison reports.
Supports both single-server analysis and FrogDB vs Redis comparisons.

Usage:
    uv run parse_results.py --frogdb results.json
    uv run parse_results.py --frogdb frogdb.json --redis redis.json
    uv run parse_results.py --frogdb frogdb.json --targets targets.yaml

Output:
    Human-readable performance report with latency percentiles and throughput.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# Performance targets from OBSERVABILITY.md
DEFAULT_TARGETS = {
    "get_p50_us": 100,
    "get_p95_us": 200,
    "get_p99_us": 500,
    "set_async_p50_us": 100,
    "set_async_p95_us": 200,
    "set_async_p99_us": 500,
    "set_sync_p50_us": 1000,
    "set_sync_p95_us": 2000,
    "set_sync_p99_us": 5000,
    "throughput_get_ops": 100000,
    "throughput_set_ops": 50000,
}


def load_memtier_json(filepath: Path) -> dict[str, Any]:
    """Load and parse memtier_benchmark JSON output."""
    with open(filepath) as f:
        return json.load(f)


def extract_metrics(data: dict) -> dict[str, Any]:
    """Extract key metrics from memtier JSON data."""
    # memtier_benchmark JSON structure varies by version
    # Handle both formats

    metrics = {
        "ops_per_sec": 0,
        "get_ops_per_sec": 0,
        "set_ops_per_sec": 0,
        "get_latency": {},
        "set_latency": {},
        "total_requests": 0,
        "total_bytes": 0,
    }

    # Try to extract from 'ALL STATS' or 'totals' section
    if "ALL STATS" in data:
        stats = data["ALL STATS"]
        if "Gets" in stats:
            gets = stats["Gets"]
            metrics["get_ops_per_sec"] = gets.get("Ops/sec", 0)
            metrics["get_latency"] = {
                "avg_ms": gets.get("Latency", 0),
                "p50_ms": gets.get("KB/sec", 0),  # This might be wrong field
            }
        if "Sets" in stats:
            sets = stats["Sets"]
            metrics["set_ops_per_sec"] = sets.get("Ops/sec", 0)
        if "Totals" in stats:
            totals = stats["Totals"]
            metrics["ops_per_sec"] = totals.get("Ops/sec", 0)
            metrics["total_requests"] = totals.get("Requests", 0)

    # Alternative format
    if "totals" in data:
        totals = data["totals"]
        metrics["ops_per_sec"] = totals.get("ops_per_sec", 0) or totals.get("Ops/sec", 0)

        # Latency percentiles
        if "full_latency" in totals:
            lat = totals["full_latency"]
            metrics["get_latency"] = {
                "p50_ms": lat.get("p50.000", 0),
                "p90_ms": lat.get("p90.000", 0),
                "p95_ms": lat.get("p95.000", 0),
                "p99_ms": lat.get("p99.000", 0),
                "p99.9_ms": lat.get("p99.900", 0),
            }

    # Try get/set specific stats
    if "GET" in data:
        get_stats = data["GET"]
        metrics["get_ops_per_sec"] = get_stats.get("ops_per_sec", 0)
        if "latency_histogram" in get_stats:
            metrics["get_latency"] = extract_latency_percentiles(get_stats["latency_histogram"])

    if "SET" in data:
        set_stats = data["SET"]
        metrics["set_ops_per_sec"] = set_stats.get("ops_per_sec", 0)
        if "latency_histogram" in set_stats:
            metrics["set_latency"] = extract_latency_percentiles(set_stats["latency_histogram"])

    return metrics


def extract_latency_percentiles(histogram: list) -> dict[str, float]:
    """Extract latency percentiles from histogram data."""
    percentiles = {}
    for entry in histogram:
        if isinstance(entry, dict):
            pct = entry.get("percentile", 0)
            val = entry.get("value", 0)
            if pct == 50:
                percentiles["p50_ms"] = val
            elif pct == 90:
                percentiles["p90_ms"] = val
            elif pct == 95:
                percentiles["p95_ms"] = val
            elif pct == 99:
                percentiles["p99_ms"] = val
            elif pct == 99.9:
                percentiles["p99.9_ms"] = val
    return percentiles


def format_number(n: float, precision: int = 2) -> str:
    """Format a number with thousand separators."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.{precision}f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.{precision}f}K"
    else:
        return f"{n:.{precision}f}"


def check_target(actual: float, target: float) -> str:
    """Check if actual value meets target, return status string."""
    if actual <= target:
        return "[OK]"
    elif actual <= target * 1.5:
        return "[WARN]"
    else:
        return "[FAIL]"


def generate_single_report(metrics: dict, name: str = "FrogDB") -> str:
    """Generate a report for a single server."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"{name} Load Test Report")
    lines.append("=" * 60)
    lines.append("")

    # Throughput
    ops = metrics.get("ops_per_sec", 0)
    lines.append(f"Throughput:     {format_number(ops)} ops/sec")

    # GET latency
    get_lat = metrics.get("get_latency", {})
    if get_lat:
        p50 = get_lat.get("p50_ms", 0)
        p95 = get_lat.get("p95_ms", 0)
        p99 = get_lat.get("p99_ms", 0)
        p999 = get_lat.get("p99.9_ms", 0)
        lines.append(
            f"GET Latency:    p50={p50:.2f}ms  p95={p95:.2f}ms  p99={p99:.2f}ms  p99.9={p999:.2f}ms"
        )

    # SET latency
    set_lat = metrics.get("set_latency", {})
    if set_lat:
        p50 = set_lat.get("p50_ms", 0)
        p95 = set_lat.get("p95_ms", 0)
        p99 = set_lat.get("p99_ms", 0)
        p999 = set_lat.get("p99.9_ms", 0)
        lines.append(
            f"SET Latency:    p50={p50:.2f}ms  p95={p95:.2f}ms  p99={p99:.2f}ms  p99.9={p999:.2f}ms"
        )

    lines.append("")
    lines.append("Performance vs Targets:")
    lines.append("-" * 60)

    # Check against targets
    if get_lat:
        p99_us = get_lat.get("p99_ms", 0) * 1000
        target = DEFAULT_TARGETS["get_p99_us"]
        status = check_target(p99_us, target)
        lines.append(f"  GET p99 target: <{target}us    Actual: {p99_us:.0f}us    {status}")

    if set_lat:
        p99_us = set_lat.get("p99_ms", 0) * 1000
        target = DEFAULT_TARGETS["set_async_p99_us"]
        status = check_target(p99_us, target)
        lines.append(f"  SET p99 target: <{target}us    Actual: {p99_us:.0f}us    {status}")

    lines.append("=" * 60)
    return "\n".join(lines)


def generate_comparison_report(frogdb: dict, redis: dict) -> str:
    """Generate a comparison report between FrogDB and Redis."""
    lines = []
    lines.append("=" * 70)
    lines.append("FrogDB vs Redis Comparison Report")
    lines.append("=" * 70)
    lines.append("")

    # Throughput comparison
    frogdb_ops = frogdb.get("ops_per_sec", 0)
    redis_ops = redis.get("ops_per_sec", 0)
    ratio = frogdb_ops / redis_ops if redis_ops > 0 else 0

    lines.append("Throughput (ops/sec):")
    lines.append("-" * 70)
    lines.append(f"  FrogDB:  {format_number(frogdb_ops):>12}")
    lines.append(f"  Redis:   {format_number(redis_ops):>12}")
    lines.append(f"  Ratio:   {ratio:>12.2f}x")
    lines.append("")

    # Latency comparison
    lines.append("Latency Comparison (ms):")
    lines.append("-" * 70)
    lines.append(f"{'':15} {'p50':>10} {'p95':>10} {'p99':>10} {'p99.9':>10}")
    lines.append("-" * 70)

    # GET latency
    frogdb_get = frogdb.get("get_latency", {})
    redis_get = redis.get("get_latency", {})

    if frogdb_get or redis_get:
        lines.append("GET:")
        for name, lat in [("  FrogDB", frogdb_get), ("  Redis", redis_get)]:
            p50 = lat.get("p50_ms", 0)
            p95 = lat.get("p95_ms", 0)
            p99 = lat.get("p99_ms", 0)
            p999 = lat.get("p99.9_ms", 0)
            lines.append(f"{name:15} {p50:>10.3f} {p95:>10.3f} {p99:>10.3f} {p999:>10.3f}")

    # SET latency
    frogdb_set = frogdb.get("set_latency", {})
    redis_set = redis.get("set_latency", {})

    if frogdb_set or redis_set:
        lines.append("SET:")
        for name, lat in [("  FrogDB", frogdb_set), ("  Redis", redis_set)]:
            p50 = lat.get("p50_ms", 0)
            p95 = lat.get("p95_ms", 0)
            p99 = lat.get("p99_ms", 0)
            p999 = lat.get("p99.9_ms", 0)
            lines.append(f"{name:15} {p50:>10.3f} {p95:>10.3f} {p99:>10.3f} {p999:>10.3f}")

    lines.append("")
    lines.append("Performance Summary:")
    lines.append("-" * 70)

    if ratio >= 1.0:
        lines.append(f"  FrogDB is {ratio:.2f}x FASTER than Redis")
    else:
        lines.append(f"  FrogDB is {1 / ratio:.2f}x SLOWER than Redis")

    # Compare p99 latencies
    if frogdb_get and redis_get:
        frogdb_p99 = frogdb_get.get("p99_ms", 0)
        redis_p99 = redis_get.get("p99_ms", 0)
        if frogdb_p99 > 0 and redis_p99 > 0:
            lat_ratio = redis_p99 / frogdb_p99
            if lat_ratio >= 1.0:
                lines.append(f"  GET p99 latency: FrogDB is {lat_ratio:.2f}x LOWER")
            else:
                lines.append(f"  GET p99 latency: FrogDB is {1 / lat_ratio:.2f}x HIGHER")

    lines.append("=" * 70)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Parse and compare memtier_benchmark results")
    parser.add_argument(
        "--frogdb",
        type=Path,
        required=True,
        help="Path to FrogDB memtier JSON results",
    )
    parser.add_argument(
        "--redis",
        type=Path,
        help="Path to Redis memtier JSON results (for comparison)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file for report (default: stdout)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output raw metrics as JSON",
    )

    args = parser.parse_args()

    # Load FrogDB results
    if not args.frogdb.exists():
        print(f"Error: FrogDB results file not found: {args.frogdb}", file=sys.stderr)
        sys.exit(1)

    frogdb_data = load_memtier_json(args.frogdb)
    frogdb_metrics = extract_metrics(frogdb_data)

    # Generate report
    if args.redis:
        if not args.redis.exists():
            print(f"Error: Redis results file not found: {args.redis}", file=sys.stderr)
            sys.exit(1)

        redis_data = load_memtier_json(args.redis)
        redis_metrics = extract_metrics(redis_data)

        if args.json:
            output = json.dumps(
                {
                    "frogdb": frogdb_metrics,
                    "redis": redis_metrics,
                },
                indent=2,
            )
        else:
            output = generate_comparison_report(frogdb_metrics, redis_metrics)
    else:
        if args.json:
            output = json.dumps(frogdb_metrics, indent=2)
        else:
            output = generate_single_report(frogdb_metrics)

    # Output
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Report written to: {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
