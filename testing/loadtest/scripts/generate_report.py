#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
generate_report.py - Generate Markdown benchmark comparison reports

Reads combined benchmark results JSON and generates a comprehensive
Markdown report with tables, latency distributions, and methodology notes.

Usage:
    uv run generate_report.py --input combined_results.json --output report.md

Options:
    --input FILE          Combined results JSON file
    --output FILE         Output Markdown file (default: benchmark_report.md)

Examples:
    uv run generate_report.py --input reports/compare_mixed_20240115/combined_results.json
"""

import argparse
import json
import platform
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def extract_memtier_metrics(data: dict) -> dict[str, Any]:
    """Extract key metrics from memtier JSON data."""
    metrics = {
        "ops_per_sec": 0,
        "get_ops_per_sec": 0,
        "set_ops_per_sec": 0,
        "get_latency": {},
        "set_latency": {},
        "total_latency": {},
    }

    # Try to extract from 'ALL STATS' section
    if "ALL STATS" in data:
        stats = data["ALL STATS"]
        if "Gets" in stats:
            gets = stats["Gets"]
            metrics["get_ops_per_sec"] = gets.get("Ops/sec", 0)
            if "Percentile Latencies" in gets:
                pcts = gets["Percentile Latencies"]
                metrics["get_latency"] = {
                    "p50_ms": pcts.get("p50.00", 0),
                    "p90_ms": pcts.get("p90.00", 0),
                    "p95_ms": pcts.get("p95.00", 0),
                    "p99_ms": pcts.get("p99.00", 0),
                    "p99.9_ms": pcts.get("p99.90", 0),
                }
        if "Sets" in stats:
            sets = stats["Sets"]
            metrics["set_ops_per_sec"] = sets.get("Ops/sec", 0)
            if "Percentile Latencies" in sets:
                pcts = sets["Percentile Latencies"]
                metrics["set_latency"] = {
                    "p50_ms": pcts.get("p50.00", 0),
                    "p90_ms": pcts.get("p90.00", 0),
                    "p95_ms": pcts.get("p95.00", 0),
                    "p99_ms": pcts.get("p99.00", 0),
                    "p99.9_ms": pcts.get("p99.90", 0),
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

    # Try alternative format
    if "totals" in data:
        totals = data["totals"]
        metrics["ops_per_sec"] = totals.get("ops_per_sec", 0) or totals.get("Ops/sec", 0)

        if "full_latency" in totals:
            lat = totals["full_latency"]
            metrics["total_latency"] = {
                "p50_ms": lat.get("p50.000", 0),
                "p90_ms": lat.get("p90.000", 0),
                "p95_ms": lat.get("p95.000", 0),
                "p99_ms": lat.get("p99.000", 0),
                "p99.9_ms": lat.get("p99.900", 0),
            }

    return metrics


def format_ops(n: float) -> str:
    """Format operations per second."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    else:
        return f"{n:.0f}"


def format_latency(ms: float) -> str:
    """Format latency in milliseconds."""
    if ms == 0:
        return "-"
    elif ms < 1:
        return f"{ms * 1000:.2f}μs"
    else:
        return f"{ms:.3f}ms"


def generate_summary_table(backends: dict[str, Any]) -> str:
    """Generate the summary comparison table."""
    lines = []

    # Collect all backend names
    backend_names = list(backends.keys())
    if not backend_names:
        return ""

    # Header
    header = "| Metric |"
    separator = "|--------|"
    for name in backend_names:
        display_name = backends[name].get("name", name.capitalize())
        header += f" {display_name} |"
        separator += "--------|"

    lines.append(header)
    lines.append(separator)

    # Extract metrics for each backend
    metrics_list = {}
    for name in backend_names:
        backend_data = backends[name]
        if "memtier" in backend_data:
            metrics_list[name] = extract_memtier_metrics(backend_data["memtier"])
        else:
            metrics_list[name] = {}

    # Total ops/sec
    row = "| Total ops/sec |"
    for name in backend_names:
        ops = metrics_list.get(name, {}).get("ops_per_sec", 0)
        row += f" {format_ops(ops)} |"
    lines.append(row)

    # GET ops/sec
    row = "| GET ops/sec |"
    for name in backend_names:
        ops = metrics_list.get(name, {}).get("get_ops_per_sec", 0)
        row += f" {format_ops(ops)} |"
    lines.append(row)

    # SET ops/sec
    row = "| SET ops/sec |"
    for name in backend_names:
        ops = metrics_list.get(name, {}).get("set_ops_per_sec", 0)
        row += f" {format_ops(ops)} |"
    lines.append(row)

    # GET p99 latency
    row = "| GET p99 latency |"
    for name in backend_names:
        lat = metrics_list.get(name, {}).get("get_latency", {}).get("p99_ms", 0)
        row += f" {format_latency(lat)} |"
    lines.append(row)

    # SET p99 latency
    row = "| SET p99 latency |"
    for name in backend_names:
        lat = metrics_list.get(name, {}).get("set_latency", {}).get("p99_ms", 0)
        row += f" {format_latency(lat)} |"
    lines.append(row)

    return "\n".join(lines)


def generate_latency_table(backends: dict[str, Any], operation: str) -> str:
    """Generate detailed latency distribution table."""
    lines = []

    backend_names = list(backends.keys())
    if not backend_names:
        return ""

    # Header
    header = "| Backend | p50 | p90 | p95 | p99 | p99.9 |"
    separator = "|---------|-----|-----|-----|-----|-------|"

    lines.append(header)
    lines.append(separator)

    for name in backend_names:
        backend_data = backends[name]
        display_name = backend_data.get("name", name.capitalize())

        if "memtier" in backend_data:
            metrics = extract_memtier_metrics(backend_data["memtier"])
            lat_key = f"{operation.lower()}_latency"
            lat = metrics.get(lat_key, {})

            if not lat:
                lat = metrics.get("total_latency", {})

            row = f"| {display_name} |"
            row += f" {format_latency(lat.get('p50_ms', 0))} |"
            row += f" {format_latency(lat.get('p90_ms', 0))} |"
            row += f" {format_latency(lat.get('p95_ms', 0))} |"
            row += f" {format_latency(lat.get('p99_ms', 0))} |"
            row += f" {format_latency(lat.get('p99.9_ms', 0))} |"
            lines.append(row)
        else:
            lines.append(f"| {display_name} | - | - | - | - | - |")

    return "\n".join(lines)


def generate_extended_table(backends: dict[str, Any], workload_name: str) -> str:
    """Generate table for extended workload results."""
    lines = []

    backend_names = list(backends.keys())
    if not backend_names:
        return ""

    # Find commands in this workload
    commands = set()
    for name in backend_names:
        backend_data = backends[name]
        if "extended" in backend_data:
            for workload in backend_data["extended"].get("workloads", []):
                if workload.get("name") == workload_name:
                    for result in workload.get("results", []):
                        commands.add(result.get("command", ""))

    if not commands:
        return ""

    # Header
    header = "| Command |"
    separator = "|---------|"
    for name in backend_names:
        display_name = backends[name].get("name", name.capitalize())
        header += f" {display_name} ops/s |"
        separator += "---------|"

    lines.append(header)
    lines.append(separator)

    for cmd in sorted(commands):
        row = f"| {cmd} |"
        for name in backend_names:
            backend_data = backends[name]
            ops = 0
            if "extended" in backend_data:
                for workload in backend_data["extended"].get("workloads", []):
                    if workload.get("name") == workload_name:
                        for result in workload.get("results", []):
                            if result.get("command") == cmd:
                                ops = result.get("ops_per_sec", 0)
                                break
            row += f" {format_ops(ops)} |"
        lines.append(row)

    return "\n".join(lines)


def generate_report(results: dict, cpus: int | None = None, isolated: bool = False) -> str:
    """Generate the full Markdown report.

    Args:
        results: Combined benchmark results dictionary
        cpus: Number of CPUs per server (if CPU isolation was used)
        isolated: Whether CPU pinning (cpuset) was used
    """
    lines = []

    timestamp = results.get("timestamp", datetime.now().isoformat())
    backends = results.get("backends", {})

    # Get CPU config from results if available
    cpu_config = results.get("cpu_config", {})
    if cpu_config:
        cpus = cpu_config.get("cpus_per_server", cpus)
        isolated = cpu_config.get("isolated", isolated)

    # Title
    lines.append("# FrogDB Performance Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {timestamp}")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    summary_table = generate_summary_table(backends)
    if summary_table:
        lines.append(summary_table)
    lines.append("")

    # Performance comparison
    if len(backends) > 1 and "frogdb" in backends:
        frogdb_metrics = extract_memtier_metrics(backends["frogdb"].get("memtier", {}))
        frogdb_ops = frogdb_metrics.get("ops_per_sec", 0)

        comparisons = []
        for name, data in backends.items():
            if name == "frogdb":
                continue
            other_metrics = extract_memtier_metrics(data.get("memtier", {}))
            other_ops = other_metrics.get("ops_per_sec", 0)
            if other_ops > 0 and frogdb_ops > 0:
                ratio = frogdb_ops / other_ops
                display_name = data.get("name", name.capitalize())
                if ratio >= 1.0:
                    comparisons.append(f"- FrogDB is **{ratio:.2f}x** faster than {display_name}")
                else:
                    comparisons.append(
                        f"- FrogDB is **{1 / ratio:.2f}x** slower than {display_name}"
                    )
            elif other_ops > 0 and frogdb_ops == 0:
                display_name = data.get("name", name.capitalize())
                comparisons.append(
                    f"- FrogDB: no data available (comparison with {display_name} not possible)"
                )

        if comparisons:
            lines.append("### Performance Comparison")
            lines.append("")
            for comp in comparisons:
                lines.append(comp)
            lines.append("")

    # String Operations
    lines.append("## String Operations (GET/SET)")
    lines.append("")
    lines.append("Standard GET/SET operations benchmarked with memtier_benchmark.")
    lines.append("")

    # GET Latency Distribution
    lines.append("### GET Latency Distribution")
    lines.append("")
    get_lat_table = generate_latency_table(backends, "GET")
    if get_lat_table:
        lines.append(get_lat_table)
    lines.append("")

    # SET Latency Distribution
    lines.append("### SET Latency Distribution")
    lines.append("")
    set_lat_table = generate_latency_table(backends, "SET")
    if set_lat_table:
        lines.append(set_lat_table)
    lines.append("")

    # Extended workloads
    has_extended = any("extended" in data for data in backends.values())
    if has_extended:
        lines.append("## Extended Operations")
        lines.append("")
        lines.append("Complex data structure operations benchmarked with redis-py client.")
        lines.append("")

        # Hash Operations
        lines.append("### Hash Operations")
        lines.append("")
        hash_table = generate_extended_table(backends, "hash")
        if hash_table:
            lines.append(hash_table)
        else:
            lines.append("_No hash benchmark data available_")
        lines.append("")

        # List Operations
        lines.append("### List Operations")
        lines.append("")
        list_table = generate_extended_table(backends, "list")
        if list_table:
            lines.append(list_table)
        else:
            lines.append("_No list benchmark data available_")
        lines.append("")

        # Sorted Set Operations
        lines.append("### Sorted Set Operations")
        lines.append("")
        zset_table = generate_extended_table(backends, "zset")
        if zset_table:
            lines.append(zset_table)
        else:
            lines.append("_No sorted set benchmark data available_")
        lines.append("")

        # Pub/Sub Operations
        lines.append("### Pub/Sub Operations")
        lines.append("")
        pubsub_table = generate_extended_table(backends, "pubsub")
        if pubsub_table:
            lines.append(pubsub_table)
        else:
            lines.append("_No pub/sub benchmark data available_")
        lines.append("")

    # Methodology
    lines.append("## Methodology")
    lines.append("")
    lines.append(f"- **Platform:** {platform.system()} {platform.release()}")
    lines.append(f"- **Architecture:** {platform.machine()}")
    lines.append("- **Benchmark tool:** memtier_benchmark (GET/SET), redis-py (extended)")
    lines.append("- **Threads:** 4")
    lines.append("- **Clients per thread:** 25")
    lines.append("- **Total concurrent clients:** 100")
    lines.append("- **Data size:** 128 bytes")
    lines.append("- **Key space:** 10M keys")
    lines.append("- **Pipeline depth:** 1 (no pipelining)")

    # CPU isolation info
    if cpus:
        lines.append("")
        lines.append("### CPU Configuration")
        lines.append("")
        lines.append(f"- **CPUs per server:** {cpus}")
        if isolated:
            lines.append("- **Isolation mode:** Pinned (cpuset)")
            lines.append("- Each server runs on dedicated CPU cores with no contention")
        else:
            lines.append("- **Isolation mode:** Quota (--cpus)")
            lines.append("- Each server limited to CPU quota but may share physical cores")

        # Multi-threaded server notes
        lines.append("")
        lines.append("**Server thread configuration:**")
        lines.append(
            f"- FrogDB: {cpus} shard(s)" if cpus > 1 else "- FrogDB: single shard (default)"
        )
        lines.append(f"- Dragonfly: {cpus} proactor thread(s)")
        lines.append("- Redis: single-threaded (no multi-threading support)")
        lines.append("- Valkey: single-threaded (no multi-threading support)")

    lines.append("")

    # Port mappings
    lines.append("### Backend Configuration")
    lines.append("")
    lines.append("| Backend | Port |")
    lines.append("|---------|------|")
    lines.append("| FrogDB | 6379 |")
    lines.append("| Redis | 6380 |")
    lines.append("| Valkey | 6381 |")
    lines.append("| Dragonfly | 6382 |")
    lines.append("")

    # Raw data location
    lines.append("## Raw Data")
    lines.append("")
    lines.append("JSON result files are located in the same directory as this report:")
    lines.append("")
    lines.append("- `combined_results.json` - All benchmark data")
    lines.append("- `frogdb.json` - FrogDB memtier results")
    lines.append("- `redis.json` - Redis memtier results (if run)")
    lines.append("- `valkey.json` - Valkey memtier results (if run)")
    lines.append("- `dragonfly.json` - Dragonfly memtier results (if run)")
    lines.append("- `*_extended.json` - Extended workload results (if run)")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate Markdown benchmark comparison reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Combined results JSON file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output Markdown file (default: benchmark_report.md in same dir)",
    )
    parser.add_argument(
        "--cpus",
        type=int,
        help="Number of CPUs per server (for CPU isolation info in methodology)",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="CPU pinning (cpuset) was used",
    )

    args = parser.parse_args()

    # Load results
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    with open(args.input) as f:
        results = json.load(f)

    # Generate report
    report = generate_report(results, cpus=args.cpus, isolated=args.isolated)

    # Output
    if args.output:
        output_path = args.output
    else:
        output_path = args.input.parent / "benchmark_report.md"

    with open(output_path, "w") as f:
        f.write(report)

    print(f"Report written to: {output_path}")


if __name__ == "__main__":
    main()
