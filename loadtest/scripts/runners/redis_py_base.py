"""
redis_py_base.py - Base class for redis-py based benchmark runners

Provides common functionality for runners that use redis-py to benchmark
data structures not well supported by memtier (hashes, lists, sorted sets, etc.).
"""

import json
import random
import statistics
import string
import time
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import BenchmarkRunner, BenchmarkResult, CommandResult


@dataclass
class OperationStats:
    """Statistics for a single operation type."""
    command: str
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0

    @property
    def count(self) -> int:
        return len(self.latencies_ms)

    def add_latency(self, latency_ms: float) -> None:
        self.latencies_ms.append(latency_ms)

    def percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        return sorted_latencies[idx]


class RedisPyRunner(BenchmarkRunner):
    """Base class for redis-py based runners.

    Subclasses implement specific command operations (hash, list, zset, etc.).
    This base class handles:
    - Connection pooling
    - Thread-based concurrent execution
    - Latency measurement and statistics
    - Result aggregation
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        super().__init__(host, port)
        self._pool: Any = None

    def is_available(self) -> bool:
        """Check if redis-py is available."""
        return REDIS_AVAILABLE

    def _get_connection_pool(self) -> Any:
        """Get or create connection pool."""
        if not REDIS_AVAILABLE:
            raise RuntimeError("redis-py not installed")

        if self._pool is None:
            self._pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                max_connections=200,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
        return self._pool

    def _get_client(self) -> Any:
        """Get a Redis client from the pool."""
        if not REDIS_AVAILABLE:
            raise RuntimeError("redis-py not installed")
        return redis.Redis(connection_pool=self._get_connection_pool())

    def _generate_key(self, workload: WorkloadConfig, index: int) -> str:
        """Generate a key based on workload configuration."""
        prefix = workload.keys.prefix or ""
        return f"{prefix}key:{index % workload.keys.space_size}"

    def _generate_value(self, size_bytes: int) -> str:
        """Generate a random value of specified size."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size_bytes))

    @abstractmethod
    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate data for the benchmark.

        Args:
            client: Redis client
            workload: Workload configuration
            count: Number of keys to create
        """
        pass

    @abstractmethod
    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get operation functions for this runner.

        Returns:
            Dictionary mapping command names to operation functions.
            Each function takes (client, key, index) and performs the operation.
        """
        pass

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Run the benchmark.

        Args:
            workload: Workload configuration
            output_dir: Directory to store result files
            requests: Total number of requests
            warmup: Whether to run warmup phase

        Returns:
            BenchmarkResult with metrics
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        client = self._get_client()

        # Get operations weighted by command distribution
        operations = self.get_operations(workload)
        weighted_ops: list[tuple[str, Callable]] = []

        for cmd, pct in workload.commands.items():
            cmd_upper = cmd.upper()
            if cmd_upper in operations:
                # Add operation multiple times based on percentage
                for _ in range(pct):
                    weighted_ops.append((cmd_upper, operations[cmd_upper]))

        if not weighted_ops:
            return BenchmarkResult(
                workload_name=workload.name,
                backend_name="",
                runner_type=self.name,
                runner_version=self.version,
                targets_met=False,
                target_failures=["No supported operations in workload"],
            )

        # Setup data
        setup_count = min(workload.keys.space_size, 10000)
        self.setup_data(client, workload, setup_count)

        # Initialize stats
        stats: dict[str, OperationStats] = {}
        for cmd in workload.commands:
            stats[cmd.upper()] = OperationStats(command=cmd.upper())

        # Warmup
        if warmup:
            warmup_count = min(1000, requests // 10)
            self._run_operations(
                client, weighted_ops, workload, warmup_count, stats={}, warmup=True
            )
            time.sleep(0.1)

        # Main benchmark
        start_time = time.time()

        if workload.concurrency.threads > 1:
            # Multi-threaded execution
            self._run_threaded(
                workload, weighted_ops, requests, stats
            )
        else:
            # Single-threaded execution
            self._run_operations(
                client, weighted_ops, workload, requests, stats, warmup=False
            )

        duration = time.time() - start_time

        # Build result
        result = self._build_result(workload, stats, duration)

        # Save results
        json_file = output_dir / f"{self.name}_results.json"
        with open(json_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        # Check targets
        self.check_targets(result, workload)

        return result

    def _run_operations(
        self,
        client: Any,
        weighted_ops: list[tuple[str, Callable]],
        workload: WorkloadConfig,
        count: int,
        stats: dict[str, OperationStats],
        warmup: bool = False,
    ) -> None:
        """Run operations sequentially."""
        pipeline_depth = workload.concurrency.pipeline

        if pipeline_depth > 1:
            self._run_pipelined(client, weighted_ops, workload, count, stats, warmup)
        else:
            self._run_single(client, weighted_ops, workload, count, stats, warmup)

    def _run_single(
        self,
        client: Any,
        weighted_ops: list[tuple[str, Callable]],
        workload: WorkloadConfig,
        count: int,
        stats: dict[str, OperationStats],
        warmup: bool = False,
    ) -> None:
        """Run operations one at a time."""
        for i in range(count):
            cmd, op_func = random.choice(weighted_ops)
            key = self._generate_key(workload, i)

            start = time.perf_counter()
            try:
                op_func(client, key, i)
            except Exception:
                if not warmup and cmd in stats:
                    stats[cmd].errors += 1
                continue

            latency_ms = (time.perf_counter() - start) * 1000
            if not warmup and cmd in stats:
                stats[cmd].add_latency(latency_ms)

    def _run_pipelined(
        self,
        client: Any,
        weighted_ops: list[tuple[str, Callable]],
        workload: WorkloadConfig,
        count: int,
        stats: dict[str, OperationStats],
        warmup: bool = False,
    ) -> None:
        """Run operations in pipelines."""
        pipeline_depth = workload.concurrency.pipeline
        batches = (count + pipeline_depth - 1) // pipeline_depth

        for batch in range(batches):
            pipe = client.pipeline(transaction=False)
            batch_ops: list[str] = []

            for j in range(pipeline_depth):
                idx = batch * pipeline_depth + j
                if idx >= count:
                    break

                cmd, op_func = random.choice(weighted_ops)
                key = self._generate_key(workload, idx)
                batch_ops.append(cmd)

                # Queue operation in pipeline
                try:
                    op_func(pipe, key, idx)
                except Exception:
                    if not warmup and cmd in stats:
                        stats[cmd].errors += 1

            if batch_ops:
                start = time.perf_counter()
                try:
                    pipe.execute()
                except Exception:
                    for cmd in batch_ops:
                        if not warmup and cmd in stats:
                            stats[cmd].errors += 1
                    continue

                # Distribute latency across all operations in batch
                total_latency_ms = (time.perf_counter() - start) * 1000
                per_op_latency = total_latency_ms / len(batch_ops)
                if not warmup:
                    for cmd in batch_ops:
                        if cmd in stats:
                            stats[cmd].add_latency(per_op_latency)

    def _run_threaded(
        self,
        workload: WorkloadConfig,
        weighted_ops: list[tuple[str, Callable]],
        total_requests: int,
        stats: dict[str, OperationStats],
    ) -> None:
        """Run operations across multiple threads."""
        num_threads = workload.concurrency.threads
        requests_per_thread = total_requests // num_threads

        # Per-thread stats
        thread_stats: list[dict[str, OperationStats]] = []
        for _ in range(num_threads):
            ts = {}
            for cmd in workload.commands:
                ts[cmd.upper()] = OperationStats(command=cmd.upper())
            thread_stats.append(ts)

        def worker(thread_id: int) -> None:
            client = self._get_client()
            self._run_operations(
                client,
                weighted_ops,
                workload,
                requests_per_thread,
                thread_stats[thread_id],
                warmup=False,
            )

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for future in as_completed(futures):
                future.result()  # Raise any exceptions

        # Merge stats
        for ts in thread_stats:
            for cmd, op_stats in ts.items():
                if cmd in stats:
                    stats[cmd].latencies_ms.extend(op_stats.latencies_ms)
                    stats[cmd].errors += op_stats.errors

    def _build_result(
        self,
        workload: WorkloadConfig,
        stats: dict[str, OperationStats],
        duration: float,
    ) -> BenchmarkResult:
        """Build BenchmarkResult from collected stats."""
        result = BenchmarkResult(
            workload_name=workload.name,
            backend_name="",
            runner_type=self.name,
            runner_version=self.version,
            duration_seconds=duration,
        )

        all_latencies: list[float] = []

        for cmd, op_stats in stats.items():
            if op_stats.count == 0:
                continue

            all_latencies.extend(op_stats.latencies_ms)

            result.commands[cmd] = CommandResult(
                command=cmd,
                ops_per_sec=op_stats.count / duration if duration > 0 else 0,
                p50_latency_ms=op_stats.percentile(50),
                p95_latency_ms=op_stats.percentile(95),
                p99_latency_ms=op_stats.percentile(99),
                p999_latency_ms=op_stats.percentile(99.9),
                total_ops=op_stats.count,
                errors=op_stats.errors,
            )

        # Overall stats
        result.total_ops = len(all_latencies)
        result.total_ops_per_sec = result.total_ops / duration if duration > 0 else 0

        if all_latencies:
            sorted_latencies = sorted(all_latencies)
            result.p50_latency_ms = sorted_latencies[int(len(sorted_latencies) * 0.50)]
            result.p95_latency_ms = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            result.p99_latency_ms = sorted_latencies[int(len(sorted_latencies) * 0.99)]
            idx_999 = min(int(len(sorted_latencies) * 0.999), len(sorted_latencies) - 1)
            result.p999_latency_ms = sorted_latencies[idx_999]

        return result
