"""
memtier.py - String operations benchmark runner using memtier_benchmark

Handles: GET, SET, MGET, MSET, INCR, DECR, APPEND
Tool: memtier_benchmark (Redis Labs)

Memtier is the most accurate tool for measuring GET/SET throughput and latency.
It supports various key patterns and operation ratios.
"""

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import CommandCategory, WorkloadConfig

from runners.base import (
    BenchmarkResult,
    BenchmarkRunner,
    CommandResult,
    register_runner,
)


@register_runner(CommandCategory.STRING)
class MemtierRunner(BenchmarkRunner):
    """String operations runner using memtier_benchmark.

    Supports:
    - GET/SET with configurable ratios
    - Various key distribution patterns (random, gaussian, sequential)
    - Pipelining for throughput testing
    - JSON output for detailed metrics
    """

    name = "memtier"
    version = "1.0.0"
    category = CommandCategory.STRING

    def is_available(self) -> bool:
        """Check if memtier_benchmark is installed."""
        return shutil.which("memtier_benchmark") is not None

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Run memtier_benchmark with workload configuration.

        Args:
            workload: Workload configuration
            output_dir: Directory to store result files
            requests: Total requests (distributed across threads * clients)
            warmup: Whether to run warmup (memtier has built-in warmup)

        Returns:
            BenchmarkResult with metrics
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        # Calculate requests per client
        threads = workload.concurrency.threads
        clients = workload.concurrency.clients // workload.concurrency.threads
        if clients < 1:
            clients = 1
        requests_per_client = max(1, requests // (threads * clients))

        # Build memtier command
        cmd = self._build_command(workload, output_dir, requests_per_client)

        # Run warmup if requested (separate shorter run)
        if warmup:
            warmup_requests = min(1000, requests_per_client // 10)
            warmup_cmd = cmd.copy()
            # Update requests for warmup
            for i, arg in enumerate(warmup_cmd):
                if arg == "--requests":
                    warmup_cmd[i + 1] = str(warmup_requests)
                elif arg == "--json-out-file":
                    warmup_cmd[i + 1] = str(output_dir / "warmup.json")

            subprocess.run(warmup_cmd, capture_output=True, text=True)
            time.sleep(0.5)  # Brief pause after warmup

        # Run main benchmark
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        duration = time.time() - start_time

        # Save raw output
        txt_file = output_dir / "memtier_output.txt"
        with open(txt_file, "w") as f:
            f.write(result.stdout)
            if result.stderr:
                f.write("\n--- STDERR ---\n")
                f.write(result.stderr)

        # Parse results
        json_file = output_dir / "memtier_results.json"
        return self._parse_results(
            workload=workload,
            json_file=json_file,
            duration=duration,
            return_code=result.returncode,
        )

    def _build_command(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests_per_client: int,
    ) -> list[str]:
        """Build memtier_benchmark command line."""
        json_file = output_dir / "memtier_results.json"

        # Get memtier-compatible settings
        ratio = workload.get_memtier_ratio()
        if ratio is None:
            # Default to balanced if not pure GET/SET
            ratio = "1:1"

        key_pattern = workload.get_memtier_key_pattern()

        # Calculate clients per thread
        threads = workload.concurrency.threads
        clients_per_thread = workload.concurrency.clients // threads
        if clients_per_thread < 1:
            clients_per_thread = 1

        cmd = [
            "memtier_benchmark",
            "-s",
            self.host,
            "-p",
            str(self.port),
            "--threads",
            str(threads),
            "--clients",
            str(clients_per_thread),
            "--requests",
            str(requests_per_client),
            "--data-size",
            str(workload.data.size_bytes),
            "--ratio",
            ratio,
            "--key-pattern",
            key_pattern,
            "--key-maximum",
            str(workload.keys.space_size),
            "--pipeline",
            str(workload.concurrency.pipeline),
            "--hide-histogram",
            "--json-out-file",
            str(json_file),
        ]

        # Add key prefix if specified
        if workload.keys.prefix:
            cmd.extend(["--key-prefix", workload.keys.prefix])

        # Add rate limiting if specified
        if workload.concurrency.rate_limit_rps > 0:
            # memtier uses --rate-limiting in ops/sec per connection
            ops_per_connection = workload.concurrency.rate_limit_rps // (
                threads * clients_per_thread
            )
            if ops_per_connection > 0:
                cmd.extend(["--rate-limiting", str(ops_per_connection)])

        return cmd

    def _parse_results(
        self,
        workload: WorkloadConfig,
        json_file: Path,
        duration: float,
        return_code: int,
    ) -> BenchmarkResult:
        """Parse memtier JSON output into BenchmarkResult."""
        result = BenchmarkResult(
            workload_name=workload.name,
            backend_name="",  # Set by caller
            runner_type=self.name,
            runner_version=self.version,
            duration_seconds=duration,
        )

        if not json_file.exists():
            result.target_failures = ["memtier_benchmark failed to produce output"]
            result.targets_met = False
            return result

        try:
            with open(json_file) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            result.target_failures = [f"Failed to parse memtier JSON: {e}"]
            result.targets_met = False
            return result

        result.raw_output = data

        # Extract metrics from "ALL STATS" section
        if "ALL STATS" in data:
            stats = data["ALL STATS"]
            totals = stats.get("Totals", {})

            result.total_ops_per_sec = totals.get("Ops/sec", 0)
            result.total_ops = totals.get("Ops", 0)

            # Overall latency (from Totals)
            result.p50_latency_ms = totals.get("Latency", {}).get("p50.00", 0)
            result.p95_latency_ms = totals.get("Latency", {}).get("p95.00", 0)
            result.p99_latency_ms = totals.get("Latency", {}).get("p99.00", 0)
            result.p999_latency_ms = totals.get("Latency", {}).get("p99.90", 0)

            # Per-command results
            if "Gets" in stats:
                gets = stats["Gets"]
                result.commands["GET"] = CommandResult(
                    command="GET",
                    ops_per_sec=gets.get("Ops/sec", 0),
                    p50_latency_ms=gets.get("Latency", {}).get("p50.00", 0),
                    p95_latency_ms=gets.get("Latency", {}).get("p95.00", 0),
                    p99_latency_ms=gets.get("Latency", {}).get("p99.00", 0),
                    p999_latency_ms=gets.get("Latency", {}).get("p99.90", 0),
                    total_ops=gets.get("Ops", 0),
                )

            if "Sets" in stats:
                sets = stats["Sets"]
                result.commands["SET"] = CommandResult(
                    command="SET",
                    ops_per_sec=sets.get("Ops/sec", 0),
                    p50_latency_ms=sets.get("Latency", {}).get("p50.00", 0),
                    p95_latency_ms=sets.get("Latency", {}).get("p95.00", 0),
                    p99_latency_ms=sets.get("Latency", {}).get("p99.00", 0),
                    p999_latency_ms=sets.get("Latency", {}).get("p99.90", 0),
                    total_ops=sets.get("Ops", 0),
                )

        # Check targets
        self.check_targets(result, workload)

        return result


def main():
    """Test the memtier runner."""
    runner = MemtierRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if not runner.is_available():
        print("memtier_benchmark not found. Install with: brew install memtier_benchmark")
        return

    # Test with a simple workload
    from workload_loader import ConcurrencyConfig, DataConfig, KeysConfig, WorkloadConfig

    workload = WorkloadConfig(
        name="test-string",
        description="Test string workload",
        commands={"GET": 90, "SET": 10},
        keys=KeysConfig(space_size=10000),
        data=DataConfig(size_bytes=128),
        concurrency=ConcurrencyConfig(clients=10, threads=2, pipeline=1),
    )

    print(f"\nWorkload: {workload.name}")
    print(f"Commands: {workload.commands}")
    print(f"Memtier ratio: {workload.get_memtier_ratio()}")
    print(f"Memtier key pattern: {workload.get_memtier_key_pattern()}")


if __name__ == "__main__":
    main()
