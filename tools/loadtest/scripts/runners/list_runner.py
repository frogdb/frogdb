"""
list.py - List operations benchmark runner using redis-py

Handles: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LTRIM
Tool: redis-py

Use cases: Message queues, task queues, activity feeds, timelines
"""

import random
from pathlib import Path
from typing import Any, Callable

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import register_runner
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.LIST)
class ListRunner(RedisPyRunner):
    """List operations runner using redis-py.

    Supports:
    - LPUSH, RPUSH: Add elements
    - LPOP, RPOP: Remove and return elements
    - LRANGE: Get range of elements
    - LLEN: Get list length
    - LINDEX: Get element by index
    - LSET: Set element by index
    - LTRIM: Trim list to range
    """

    name = "list"
    version = "1.0.0"
    category = CommandCategory.LIST

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate list keys with elements.

        Args:
            client: Redis client
            workload: Workload configuration
            count: Number of list keys to create
        """
        max_length = workload.list.max_length
        initial_length = min(100, max_length)  # Start with 100 elements
        value_size = workload.data.size_bytes

        pipe = client.pipeline(transaction=False)

        for i in range(count):
            key = self._generate_key(workload, i)

            # Add initial elements
            values = [self._generate_value(value_size) for _ in range(initial_length)]
            pipe.rpush(key, *values)

            # Execute in batches
            if (i + 1) % 50 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)

        pipe.execute()

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get list operation functions.

        Returns:
            Dictionary of command names to operation functions.
        """
        value_size = workload.data.size_bytes
        max_length = workload.list.max_length

        def lpush(client: Any, key: str, index: int) -> None:
            value = self._generate_value(value_size)
            client.lpush(key, value)

        def rpush(client: Any, key: str, index: int) -> None:
            value = self._generate_value(value_size)
            client.rpush(key, value)

        def lpop(client: Any, key: str, index: int) -> None:
            client.lpop(key)

        def rpop(client: Any, key: str, index: int) -> None:
            client.rpop(key)

        def lrange(client: Any, key: str, index: int) -> None:
            # Get first 10 elements (typical pagination)
            client.lrange(key, 0, 9)

        def llen(client: Any, key: str, index: int) -> None:
            client.llen(key)

        def lindex(client: Any, key: str, index: int) -> None:
            # Get element at random position
            pos = random.randint(0, min(99, max_length - 1))
            client.lindex(key, pos)

        def lset(client: Any, key: str, index: int) -> None:
            # Set element at position 0 (always exists)
            value = self._generate_value(value_size)
            client.lset(key, 0, value)

        def ltrim(client: Any, key: str, index: int) -> None:
            # Keep last max_length elements
            client.ltrim(key, -max_length, -1)

        return {
            "LPUSH": lpush,
            "RPUSH": rpush,
            "LPOP": lpop,
            "RPOP": rpop,
            "LRANGE": lrange,
            "LLEN": llen,
            "LINDEX": lindex,
            "LSET": lset,
            "LTRIM": ltrim,
        }


def main():
    """Test the list runner."""
    runner = ListRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if not runner.is_available():
        print("redis-py not installed. Install with: pip install redis")
        return

    # Test with a simple workload
    from workload_loader import (
        WorkloadConfig, KeysConfig, ConcurrencyConfig, DataConfig, ListConfig
    )

    workload = WorkloadConfig(
        name="test-list",
        description="Test list workload",
        commands={"LPUSH": 50, "RPOP": 50},
        keys=KeysConfig(space_size=100, prefix="queue:"),
        data=DataConfig(size_bytes=256),
        list=ListConfig(max_length=1000),
        concurrency=ConcurrencyConfig(clients=10, threads=2, pipeline=1),
    )

    print(f"\nWorkload: {workload.name}")
    print(f"Commands: {workload.commands}")
    print(f"Max list length: {workload.list.max_length}")

    ops = runner.get_operations(workload)
    print(f"Supported operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
