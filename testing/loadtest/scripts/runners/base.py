"""
base.py - Abstract base class for benchmark runners

Defines the interface all runners must implement and provides common utilities.
"""

import sys
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import CommandCategory, WorkloadConfig


@dataclass
class CommandResult:
    """Results for a single command type."""

    command: str
    ops_per_sec: float = 0.0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    p999_latency_ms: float = 0.0
    total_ops: int = 0
    errors: int = 0


@dataclass
class BenchmarkResult:
    """Complete benchmark result for a workload run."""

    workload_name: str
    backend_name: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    # Overall metrics
    total_ops_per_sec: float = 0.0
    total_ops: int = 0

    # Latency percentiles (overall)
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    p999_latency_ms: float = 0.0

    # Per-command results
    commands: dict[str, CommandResult] = field(default_factory=dict)

    # Whether targets were met
    targets_met: bool = True
    target_failures: list[str] = field(default_factory=list)

    # Runner metadata
    runner_type: str = ""
    runner_version: str = ""
    raw_output: dict[str, Any] = field(default_factory=dict)

    # Duration
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "workload": self.workload_name,
            "backend": self.backend_name,
            "timestamp": self.timestamp,
            "ops_per_sec": self.total_ops_per_sec,
            "total_ops": self.total_ops,
            "latency": {
                "p50_ms": self.p50_latency_ms,
                "p95_ms": self.p95_latency_ms,
                "p99_ms": self.p99_latency_ms,
                "p99.9_ms": self.p999_latency_ms,
            },
            "commands": {
                cmd: {
                    "ops_per_sec": result.ops_per_sec,
                    "p50_ms": result.p50_latency_ms,
                    "p99_ms": result.p99_latency_ms,
                    "total_ops": result.total_ops,
                    "errors": result.errors,
                }
                for cmd, result in self.commands.items()
            },
            "targets_met": self.targets_met,
            "target_failures": self.target_failures,
            "duration_seconds": self.duration_seconds,
            "runner": {
                "type": self.runner_type,
                "version": self.runner_version,
            },
        }


class BenchmarkRunner(ABC):
    """Abstract base class for benchmark runners.

    Each runner implements benchmarking for a specific command category
    using an appropriate tool (memtier, redis-py, custom).
    """

    # Category this runner handles
    category: CommandCategory

    # Human-readable name
    name: str = "base"

    # Version
    version: str = "1.0.0"

    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        """Initialize runner with connection info.

        Args:
            host: Redis-compatible server hostname
            port: Server port
        """
        self.host = host
        self.port = port

    @abstractmethod
    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Execute the benchmark.

        Args:
            workload: Workload configuration
            output_dir: Directory to store result files
            requests: Total number of requests to execute
            warmup: Whether to run warmup phase before benchmark

        Returns:
            BenchmarkResult with metrics
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        """Check if this runner's dependencies are available.

        Returns:
            True if runner can be used
        """
        pass

    def supports_workload(self, workload: WorkloadConfig) -> bool:
        """Check if this runner supports the given workload.

        Default implementation checks if workload uses commands from this
        runner's category.

        Args:
            workload: Workload configuration to check

        Returns:
            True if this runner can handle the workload
        """
        categories = workload.get_command_categories()
        return self.category in categories

    def check_targets(self, result: BenchmarkResult, workload: WorkloadConfig) -> None:
        """Check if benchmark result meets workload targets.

        Updates result.targets_met and result.target_failures.

        Args:
            result: Benchmark result to check
            workload: Workload with target definitions
        """
        targets = workload.targets
        failures = []

        if targets.ops_per_sec > 0:
            if result.total_ops_per_sec < targets.ops_per_sec:
                failures.append(f"ops/sec: {result.total_ops_per_sec:.0f} < {targets.ops_per_sec}")

        if targets.p50_latency_ms > 0:
            if result.p50_latency_ms > targets.p50_latency_ms:
                failures.append(
                    f"p50 latency: {result.p50_latency_ms:.2f}ms > {targets.p50_latency_ms}ms"
                )

        if targets.p95_latency_ms > 0:
            if result.p95_latency_ms > targets.p95_latency_ms:
                failures.append(
                    f"p95 latency: {result.p95_latency_ms:.2f}ms > {targets.p95_latency_ms}ms"
                )

        if targets.p99_latency_ms > 0:
            if result.p99_latency_ms > targets.p99_latency_ms:
                failures.append(
                    f"p99 latency: {result.p99_latency_ms:.2f}ms > {targets.p99_latency_ms}ms"
                )

        result.target_failures = failures
        result.targets_met = len(failures) == 0


class RunnerRegistry:
    """Registry for benchmark runners.

    Allows registering runners for command categories and retrieving
    appropriate runners for workloads.
    """

    _runners: dict[CommandCategory, type[BenchmarkRunner]] = {}
    _instances: dict[tuple[CommandCategory, str, int], BenchmarkRunner] = {}

    @classmethod
    def register(cls, category: CommandCategory, runner_class: type[BenchmarkRunner]) -> None:
        """Register a runner for a command category.

        Args:
            category: Command category this runner handles
            runner_class: Runner class to register
        """
        cls._runners[category] = runner_class

    @classmethod
    def get(
        cls,
        category: CommandCategory,
        host: str = "127.0.0.1",
        port: int = 6379,
    ) -> BenchmarkRunner | None:
        """Get a runner instance for a category.

        Args:
            category: Command category
            host: Server hostname
            port: Server port

        Returns:
            Runner instance or None if not registered
        """
        if category not in cls._runners:
            return None

        # Cache instances by (category, host, port)
        key = (category, host, port)
        if key not in cls._instances:
            cls._instances[key] = cls._runners[category](host, port)

        return cls._instances[key]

    @classmethod
    def get_for_workload(
        cls,
        workload: WorkloadConfig,
        host: str = "127.0.0.1",
        port: int = 6379,
    ) -> list[BenchmarkRunner]:
        """Get all runners needed for a workload.

        Args:
            workload: Workload configuration
            host: Server hostname
            port: Server port

        Returns:
            List of runner instances that can handle the workload's commands
        """
        categories = workload.get_command_categories()
        runners = []

        for cat in categories:
            runner = cls.get(cat, host, port)
            if runner is not None:
                runners.append(runner)

        return runners

    @classmethod
    def list_registered(cls) -> list[CommandCategory]:
        """List all registered command categories.

        Returns:
            List of registered categories
        """
        return list(cls._runners.keys())

    @classmethod
    def clear(cls) -> None:
        """Clear all registrations (for testing)."""
        cls._runners.clear()
        cls._instances.clear()


def register_runner(
    category: CommandCategory,
) -> Callable[[type[BenchmarkRunner]], type[BenchmarkRunner]]:
    """Decorator to register a runner class.

    Usage:
        @register_runner(CommandCategory.STRING)
        class MemtierRunner(BenchmarkRunner):
            ...
    """

    def decorator(cls: type[BenchmarkRunner]) -> type[BenchmarkRunner]:
        cls.category = category
        RunnerRegistry.register(category, cls)
        return cls

    return decorator


def get_runner(
    category: CommandCategory,
    host: str = "127.0.0.1",
    port: int = 6379,
) -> BenchmarkRunner | None:
    """Convenience function to get a runner for a category.

    Args:
        category: Command category
        host: Server hostname
        port: Server port

    Returns:
        Runner instance or None
    """
    return RunnerRegistry.get(category, host, port)
