"""
hash.py - Hash operations benchmark runner using redis-py

Handles: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HINCRBY, HLEN, etc.
Tool: redis-py

Use cases: User profiles, object storage, session data
"""

import random
import string
from pathlib import Path
from typing import Any, Callable

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import register_runner
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.HASH)
class HashRunner(RedisPyRunner):
    """Hash operations runner using redis-py.

    Supports:
    - HSET, HGET: Single field operations
    - HMSET, HMGET: Multi-field operations
    - HGETALL: Full hash retrieval
    - HDEL: Field deletion
    - HINCRBY: Field increment
    - HLEN, HEXISTS, HKEYS, HVALS: Utility commands
    """

    name = "hash"
    version = "1.0.0"
    category = CommandCategory.HASH

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate hash keys with fields.

        Args:
            client: Redis client
            workload: Workload configuration
            count: Number of hash keys to create
        """
        fields_per_hash = workload.hash.fields_per_hash
        value_size = workload.data.size_bytes // fields_per_hash
        value_size = max(8, value_size)  # Minimum 8 bytes per field

        pipe = client.pipeline(transaction=False)

        for i in range(count):
            key = self._generate_key(workload, i)
            field_data = {}
            for j in range(fields_per_hash):
                field_name = f"field:{j}"
                field_value = self._generate_value(value_size)
                field_data[field_name] = field_value

            pipe.hset(key, mapping=field_data)

            # Execute in batches to avoid memory issues
            if (i + 1) % 100 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)

        # Execute remaining
        pipe.execute()

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get hash operation functions.

        Returns:
            Dictionary of command names to operation functions.
        """
        fields_per_hash = workload.hash.fields_per_hash
        value_size = workload.data.size_bytes // fields_per_hash
        value_size = max(8, value_size)

        def hset(client: Any, key: str, index: int) -> None:
            field = f"field:{random.randint(0, fields_per_hash - 1)}"
            value = self._generate_value(value_size)
            client.hset(key, field, value)

        def hget(client: Any, key: str, index: int) -> None:
            field = f"field:{random.randint(0, fields_per_hash - 1)}"
            client.hget(key, field)

        def hmset(client: Any, key: str, index: int) -> None:
            # Set multiple fields at once
            num_fields = min(3, fields_per_hash)
            mapping = {}
            for i in range(num_fields):
                field = f"field:{random.randint(0, fields_per_hash - 1)}"
                mapping[field] = self._generate_value(value_size)
            client.hset(key, mapping=mapping)

        def hmget(client: Any, key: str, index: int) -> None:
            # Get multiple fields at once
            num_fields = min(3, fields_per_hash)
            fields = [f"field:{random.randint(0, fields_per_hash - 1)}"
                      for _ in range(num_fields)]
            client.hmget(key, fields)

        def hgetall(client: Any, key: str, index: int) -> None:
            client.hgetall(key)

        def hdel(client: Any, key: str, index: int) -> None:
            field = f"field:{random.randint(0, fields_per_hash - 1)}"
            client.hdel(key, field)

        def hincrby(client: Any, key: str, index: int) -> None:
            field = f"counter:{random.randint(0, 2)}"
            client.hincrby(key, field, 1)

        def hlen(client: Any, key: str, index: int) -> None:
            client.hlen(key)

        def hexists(client: Any, key: str, index: int) -> None:
            field = f"field:{random.randint(0, fields_per_hash - 1)}"
            client.hexists(key, field)

        def hkeys(client: Any, key: str, index: int) -> None:
            client.hkeys(key)

        def hvals(client: Any, key: str, index: int) -> None:
            client.hvals(key)

        return {
            "HSET": hset,
            "HGET": hget,
            "HMSET": hmset,
            "HMGET": hmget,
            "HGETALL": hgetall,
            "HDEL": hdel,
            "HINCRBY": hincrby,
            "HLEN": hlen,
            "HEXISTS": hexists,
            "HKEYS": hkeys,
            "HVALS": hvals,
        }


def main():
    """Test the hash runner."""
    runner = HashRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if not runner.is_available():
        print("redis-py not installed. Install with: pip install redis")
        return

    # Test with a simple workload
    from workload_loader import (
        WorkloadConfig, KeysConfig, ConcurrencyConfig, DataConfig, HashConfig
    )

    workload = WorkloadConfig(
        name="test-hash",
        description="Test hash workload",
        commands={"HSET": 30, "HGET": 50, "HGETALL": 20},
        keys=KeysConfig(space_size=1000),
        data=DataConfig(size_bytes=128),
        hash=HashConfig(fields_per_hash=10),
        concurrency=ConcurrencyConfig(clients=10, threads=2, pipeline=1),
    )

    print(f"\nWorkload: {workload.name}")
    print(f"Commands: {workload.commands}")
    print(f"Fields per hash: {workload.hash.fields_per_hash}")

    ops = runner.get_operations(workload)
    print(f"Supported operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
