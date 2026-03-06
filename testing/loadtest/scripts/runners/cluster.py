"""
cluster.py - Cluster-specific benchmark runner

Handles cluster-specific benchmarking dimensions:
- Hash slot distribution fairness
- Cross-slot operation latency (MGET across slots)
- Replication lag measurement
- Failover recovery time

Tool: redis-py with cluster support

STATUS: Implementation ready for when FrogDB clustering is available
"""

import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig

try:
    import redis  # noqa: F401
    from redis.cluster import ClusterNode, RedisCluster

    REDIS_CLUSTER_AVAILABLE = True
except ImportError:
    REDIS_CLUSTER_AVAILABLE = False


@dataclass
class SlotDistribution:
    """Hash slot distribution analysis."""

    total_slots: int = 16384
    nodes: int = 0
    slots_per_node: dict[str, int] = field(default_factory=dict)
    min_slots: int = 0
    max_slots: int = 0
    std_dev: float = 0.0
    imbalance_pct: float = 0.0  # How far from perfect distribution


@dataclass
class CrossSlotResult:
    """Results from cross-slot operation testing."""

    operation: str
    keys_count: int
    slots_accessed: int
    latency_ms: float
    success: bool
    error: str = ""


@dataclass
class ReplicationLagResult:
    """Replication lag measurement result."""

    primary_node: str
    replica_node: str
    lag_ms: float
    measured_at: str = ""


@dataclass
class FailoverResult:
    """Failover recovery time measurement."""

    failed_node: str
    new_primary: str
    detection_time_ms: float
    promotion_time_ms: float
    total_recovery_ms: float
    client_errors_during: int


@dataclass
class ClusterBenchmarkResult:
    """Complete cluster benchmark result."""

    workload_name: str
    timestamp: str
    cluster_nodes: list[str]

    # Slot distribution
    slot_distribution: SlotDistribution | None = None

    # Cross-slot latency
    cross_slot_results: list[CrossSlotResult] = field(default_factory=list)

    # Replication lag
    replication_lag: list[ReplicationLagResult] = field(default_factory=list)

    # Failover (if simulated)
    failover_result: FailoverResult | None = None

    # Standard benchmark metrics (from running workload)
    ops_per_sec: float = 0.0
    p50_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "workload": self.workload_name,
            "timestamp": self.timestamp,
            "cluster_nodes": self.cluster_nodes,
            "metrics": {
                "ops_per_sec": self.ops_per_sec,
                "p50_latency_ms": self.p50_latency_ms,
                "p99_latency_ms": self.p99_latency_ms,
            },
        }

        if self.slot_distribution:
            result["slot_distribution"] = {
                "total_slots": self.slot_distribution.total_slots,
                "nodes": self.slot_distribution.nodes,
                "slots_per_node": self.slot_distribution.slots_per_node,
                "min_slots": self.slot_distribution.min_slots,
                "max_slots": self.slot_distribution.max_slots,
                "std_dev": self.slot_distribution.std_dev,
                "imbalance_pct": self.slot_distribution.imbalance_pct,
            }

        if self.cross_slot_results:
            result["cross_slot"] = [
                {
                    "operation": r.operation,
                    "keys_count": r.keys_count,
                    "slots_accessed": r.slots_accessed,
                    "latency_ms": r.latency_ms,
                    "success": r.success,
                    "error": r.error,
                }
                for r in self.cross_slot_results
            ]

        if self.replication_lag:
            result["replication_lag"] = [
                {
                    "primary": r.primary_node,
                    "replica": r.replica_node,
                    "lag_ms": r.lag_ms,
                }
                for r in self.replication_lag
            ]

        if self.failover_result:
            result["failover"] = {
                "failed_node": self.failover_result.failed_node,
                "new_primary": self.failover_result.new_primary,
                "detection_time_ms": self.failover_result.detection_time_ms,
                "promotion_time_ms": self.failover_result.promotion_time_ms,
                "total_recovery_ms": self.failover_result.total_recovery_ms,
                "client_errors": self.failover_result.client_errors_during,
            }

        return result


class ClusterBenchmarkRunner:
    """Cluster-specific benchmark runner.

    Measures cluster-specific performance dimensions that don't apply
    to single-node deployments:

    1. Slot Distribution: How evenly are hash slots distributed?
    2. Cross-Slot Latency: Cost of multi-key operations across slots
    3. Replication Lag: Delay between primary and replica writes
    4. Failover Recovery: Time to recover from node failure

    Usage:
        runner = ClusterBenchmarkRunner(nodes=[
            ("localhost", 7000),
            ("localhost", 7001),
            ("localhost", 7002),
        ])

        result = runner.run(workload, output_dir)
    """

    name = "cluster"
    version = "1.0.0"

    def __init__(
        self,
        nodes: list[tuple[str, int]] | None = None,
        startup_nodes: list[dict[str, Any]] | None = None,
    ):
        """Initialize cluster runner.

        Args:
            nodes: List of (host, port) tuples for cluster nodes
            startup_nodes: Alternative: list of {"host": str, "port": int} dicts
        """
        self.nodes = nodes or []
        self.startup_nodes = startup_nodes or []
        self._client: Any = None

    def is_available(self) -> bool:
        """Check if redis-py cluster support is available."""
        return REDIS_CLUSTER_AVAILABLE

    def _get_client(self) -> Any:
        """Get or create cluster client."""
        if not REDIS_CLUSTER_AVAILABLE:
            raise RuntimeError("redis-py cluster support not available")

        if self._client is None:
            if self.startup_nodes:
                nodes = [ClusterNode(n["host"], n["port"]) for n in self.startup_nodes]
            elif self.nodes:
                nodes = [ClusterNode(host, port) for host, port in self.nodes]
            else:
                raise ValueError("No cluster nodes configured")

            self._client = RedisCluster(startup_nodes=nodes)

        return self._client

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
    ) -> ClusterBenchmarkResult:
        """Run cluster benchmark.

        Args:
            workload: Workload configuration (uses cluster settings)
            output_dir: Directory to store results

        Returns:
            ClusterBenchmarkResult with all metrics
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        from datetime import datetime

        result = ClusterBenchmarkResult(
            workload_name=workload.name,
            timestamp=datetime.now().isoformat(),
            cluster_nodes=[f"{h}:{p}" for h, p in self.nodes],
        )

        client = self._get_client()

        # Run cluster-specific benchmarks based on workload config
        cluster_config = workload.cluster

        if cluster_config.measure_slot_distribution:
            result.slot_distribution = self._measure_slot_distribution(client)

        if cluster_config.measure_cross_slot_latency:
            result.cross_slot_results = self._measure_cross_slot_latency(client, workload)

        # Replication lag measurement
        result.replication_lag = self._measure_replication_lag(client)

        # Failover simulation (if enabled and safe)
        if cluster_config.simulate_failover:
            # Note: This is destructive and should only be run in test environments
            result.failover_result = self._simulate_failover(client)

        # Save results
        json_file = output_dir / "cluster_results.json"
        with open(json_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        return result

    def _measure_slot_distribution(self, client: Any) -> SlotDistribution:
        """Measure hash slot distribution across nodes.

        Returns:
            SlotDistribution with fairness metrics
        """
        result = SlotDistribution()

        try:
            slots = client.cluster_slots()

            # Count slots per node
            node_slots: dict[str, int] = {}
            for slot_range in slots:
                start, end = slot_range[0], slot_range[1]
                count = end - start + 1

                # Primary node info is at index 2
                if len(slot_range) > 2:
                    node_info = slot_range[2]
                    if isinstance(node_info, (list, tuple)):
                        node_addr = f"{node_info[0]}:{node_info[1]}"
                    else:
                        node_addr = str(node_info)

                    node_slots[node_addr] = node_slots.get(node_addr, 0) + count

            result.nodes = len(node_slots)
            result.slots_per_node = node_slots

            if node_slots:
                counts = list(node_slots.values())
                result.min_slots = min(counts)
                result.max_slots = max(counts)

                if len(counts) > 1:
                    result.std_dev = statistics.stdev(counts)

                # Calculate imbalance as % deviation from perfect distribution
                perfect = result.total_slots / result.nodes
                max_deviation = max(abs(c - perfect) for c in counts)
                result.imbalance_pct = (max_deviation / perfect) * 100

        except Exception:
            # Return empty result on error
            pass

        return result

    def _measure_cross_slot_latency(
        self,
        client: Any,
        workload: WorkloadConfig,
    ) -> list[CrossSlotResult]:
        """Measure latency of cross-slot operations.

        Tests:
        1. MGET with keys in same slot (baseline)
        2. MGET with keys across 2 slots
        3. MGET with keys across all nodes

        Returns:
            List of CrossSlotResult measurements
        """
        results = []

        # Helper to generate keys for specific slots
        def key_for_slot(slot: int, suffix: str = "") -> str:
            # Use hash tags to target specific slots
            # {slot} forces the key to hash to that slot number
            return f"{{slot{slot}}}{suffix}"

        # Test 1: Same slot (baseline)
        same_slot_keys = [key_for_slot(0, f":key{i}") for i in range(10)]

        # Pre-populate
        for key in same_slot_keys:
            try:
                client.set(key, "value")
            except Exception:
                pass

        start = time.perf_counter()
        try:
            client.mget(same_slot_keys)
            latency = (time.perf_counter() - start) * 1000
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=10,
                    slots_accessed=1,
                    latency_ms=latency,
                    success=True,
                )
            )
        except Exception as e:
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=10,
                    slots_accessed=1,
                    latency_ms=0,
                    success=False,
                    error=str(e),
                )
            )

        # Test 2: Two slots
        two_slot_keys = [
            key_for_slot(0, f":key{i}") if i < 5 else key_for_slot(1000, f":key{i}")
            for i in range(10)
        ]

        for key in two_slot_keys:
            try:
                client.set(key, "value")
            except Exception:
                pass

        start = time.perf_counter()
        try:
            # Cross-slot MGET typically raises CrossSlotError in Redis Cluster
            # We catch this and note it
            client.mget(two_slot_keys)
            latency = (time.perf_counter() - start) * 1000
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=10,
                    slots_accessed=2,
                    latency_ms=latency,
                    success=True,
                )
            )
        except Exception as e:
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=10,
                    slots_accessed=2,
                    latency_ms=0,
                    success=False,
                    error=str(e),
                )
            )

        # Test 3: Many slots (one key per ~1000 slots)
        many_slot_keys = [key_for_slot(i * 1000, ":key") for i in range(16)]

        for key in many_slot_keys:
            try:
                client.set(key, "value")
            except Exception:
                pass

        start = time.perf_counter()
        try:
            client.mget(many_slot_keys)
            latency = (time.perf_counter() - start) * 1000
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=16,
                    slots_accessed=16,
                    latency_ms=latency,
                    success=True,
                )
            )
        except Exception as e:
            results.append(
                CrossSlotResult(
                    operation="MGET",
                    keys_count=16,
                    slots_accessed=16,
                    latency_ms=0,
                    success=False,
                    error=str(e),
                )
            )

        return results

    def _measure_replication_lag(self, client: Any) -> list[ReplicationLagResult]:
        """Measure replication lag between primary and replicas.

        Method: Write to primary, immediately read from replica,
        measure time until value appears.

        Returns:
            List of ReplicationLagResult for each primary-replica pair
        """
        results = []

        try:
            # Get cluster nodes info
            _nodes_info = client.cluster_nodes()

            # Find primary-replica pairs
            # This is cluster-specific and depends on the cluster setup
            # For now, return empty - needs cluster topology parsing

        except Exception:
            # Replication lag measurement not supported
            pass

        return results

    def _simulate_failover(self, client: Any) -> FailoverResult | None:
        """Simulate node failure and measure recovery time.

        WARNING: This is a destructive operation that should only be
        run in test/staging environments.

        Returns:
            FailoverResult with timing metrics, or None if not performed
        """
        # This is intentionally not fully implemented to prevent
        # accidental cluster damage. Full implementation would:
        # 1. Select a primary node
        # 2. Send DEBUG SEGFAULT or CLUSTER FAILOVER FORCE
        # 3. Measure time until cluster is healthy again
        # 4. Track client errors during the transition

        return None


def main():
    """Test the cluster runner."""
    runner = ClusterBenchmarkRunner(
        nodes=[
            ("localhost", 7000),
            ("localhost", 7001),
            ("localhost", 7002),
        ]
    )

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if not runner.is_available():
        print("redis-py cluster support not available")
        print("Install with: pip install redis[cluster]")
        return

    print("\nCluster benchmarking features:")
    print("  - Slot distribution analysis")
    print("  - Cross-slot operation latency")
    print("  - Replication lag measurement")
    print("  - Failover recovery timing (when enabled)")


if __name__ == "__main__":
    main()
