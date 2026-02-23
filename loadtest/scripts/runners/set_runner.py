"""
set.py - Set operations benchmark runner

Handles: SADD, SMEMBERS, SISMEMBER, SREM, SCARD, SINTER, SUNION, SDIFF
Tool: redis-py

Use cases: Tags, relationships, unique items, membership testing
"""

from pathlib import Path
from typing import Any, Callable

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import register_runner, BenchmarkResult
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.SET)
class SetRunner(RedisPyRunner):
    """Set operations runner using redis-py.

    Supports:
    - SADD: Add members
    - SMEMBERS: Get all members
    - SISMEMBER: Check membership
    - SREM: Remove members
    - SCARD: Get set cardinality
    - SINTER: Intersection of sets
    - SUNION: Union of sets
    - SDIFF: Difference of sets
    """

    name = "set"
    version = "1.0.0"
    category = CommandCategory.SET

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate set keys with members.

        Creates sets with overlapping members to ensure meaningful results
        for SINTER, SUNION, and SDIFF operations.
        """
        for i in range(min(count, 100)):
            key = self._generate_key(workload, i)
            # Create overlapping member sets:
            # - Common members (0-4) exist in all sets
            # - Unique members based on set index
            common_members = [f"member:{j}" for j in range(5)]
            unique_members = [f"member:{i * 10 + j}" for j in range(5, 15)]
            client.sadd(key, *common_members, *unique_members)

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get set operation functions."""
        space_size = workload.keys.space_size

        def sadd(client: Any, key: str, index: int) -> None:
            member = f"member:{index}"
            client.sadd(key, member)

        def smembers(client: Any, key: str, index: int) -> None:
            client.smembers(key)

        def sismember(client: Any, key: str, index: int) -> None:
            member = f"member:{index % 100}"
            client.sismember(key, member)

        def srem(client: Any, key: str, index: int) -> None:
            member = f"member:{index % 100}"
            client.srem(key, member)

        def scard(client: Any, key: str, index: int) -> None:
            client.scard(key)

        def sinter(client: Any, key: str, index: int) -> None:
            # Get a second key for intersection
            key2 = self._generate_key(workload, (index + 1) % space_size)
            client.sinter(key, key2)

        def sunion(client: Any, key: str, index: int) -> None:
            # Get a second key for union
            key2 = self._generate_key(workload, (index + 1) % space_size)
            client.sunion(key, key2)

        def sdiff(client: Any, key: str, index: int) -> None:
            # Get a second key for difference
            key2 = self._generate_key(workload, (index + 1) % space_size)
            client.sdiff(key, key2)

        return {
            "SADD": sadd,
            "SMEMBERS": smembers,
            "SISMEMBER": sismember,
            "SREM": srem,
            "SCARD": scard,
            "SINTER": sinter,
            "SUNION": sunion,
            "SDIFF": sdiff,
        }

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Run the benchmark."""
        return super().run(workload, output_dir, requests, warmup)


def main():
    """Test the set runner."""
    runner = SetRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if runner.is_available():
        from workload_loader import WorkloadConfig, KeysConfig

        workload = WorkloadConfig(
            name="test-set",
            commands={"SADD": 30, "SISMEMBER": 30, "SINTER": 20, "SUNION": 20},
            keys=KeysConfig(space_size=100),
        )

        ops = runner.get_operations(workload)
        print(f"Implemented operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
