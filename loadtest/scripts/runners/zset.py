"""
zset.py - Sorted Set operations benchmark runner using redis-py

Handles: ZADD, ZRANGE, ZRANK, ZSCORE, ZINCRBY, ZREM, ZCARD, ZCOUNT, etc.
Tool: redis-py

Use cases: Leaderboards, rankings, priority queues, time-based indexes
"""

import random
from pathlib import Path
from typing import Any, Callable

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import register_runner
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.ZSET)
class ZSetRunner(RedisPyRunner):
    """Sorted Set operations runner using redis-py.

    Supports:
    - ZADD: Add members with scores
    - ZRANGE, ZREVRANGE: Get members by rank
    - ZRANGEBYSCORE: Get members by score range
    - ZRANK, ZREVRANK: Get rank of member
    - ZSCORE: Get score of member
    - ZINCRBY: Increment member score
    - ZREM: Remove members
    - ZCARD: Get set cardinality
    - ZCOUNT: Count members in score range
    """

    name = "zset"
    version = "1.0.0"
    category = CommandCategory.ZSET

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate sorted set keys with members.

        Args:
            client: Redis client
            workload: Workload configuration
            count: Number of sorted set keys to create
        """
        members_per_set = workload.zset.members_per_set
        initial_members = min(1000, members_per_set)  # Start with up to 1000 members

        pipe = client.pipeline(transaction=False)

        for i in range(count):
            key = self._generate_key(workload, i)

            # Add initial members with random scores
            members = {}
            for j in range(initial_members):
                member = f"member:{j}"
                score = random.uniform(0, 1000000)  # Random score
                members[member] = score

            pipe.zadd(key, members)

            # Execute in batches
            if (i + 1) % 20 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)

        pipe.execute()

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get sorted set operation functions.

        Returns:
            Dictionary of command names to operation functions.
        """
        members_per_set = workload.zset.members_per_set

        def zadd(client: Any, key: str, index: int) -> None:
            # Add or update a member
            member = f"member:{random.randint(0, members_per_set - 1)}"
            score = random.uniform(0, 1000000)
            client.zadd(key, {member: score})

        def zrange(client: Any, key: str, index: int) -> None:
            # Get top 10 members (typical leaderboard view)
            client.zrange(key, 0, 9, withscores=True)

        def zrevrange(client: Any, key: str, index: int) -> None:
            # Get top 10 members in reverse order (high to low)
            client.zrevrange(key, 0, 9, withscores=True)

        def zrangebyscore(client: Any, key: str, index: int) -> None:
            # Get members in score range
            min_score = random.uniform(0, 500000)
            max_score = min_score + 100000
            client.zrangebyscore(key, min_score, max_score, start=0, num=10)

        def zrank(client: Any, key: str, index: int) -> None:
            member = f"member:{random.randint(0, members_per_set - 1)}"
            client.zrank(key, member)

        def zrevrank(client: Any, key: str, index: int) -> None:
            member = f"member:{random.randint(0, members_per_set - 1)}"
            client.zrevrank(key, member)

        def zscore(client: Any, key: str, index: int) -> None:
            member = f"member:{random.randint(0, members_per_set - 1)}"
            client.zscore(key, member)

        def zincrby(client: Any, key: str, index: int) -> None:
            # Increment a member's score (typical for leaderboards)
            member = f"member:{random.randint(0, members_per_set - 1)}"
            increment = random.randint(1, 100)
            client.zincrby(key, increment, member)

        def zrem(client: Any, key: str, index: int) -> None:
            member = f"member:{random.randint(0, members_per_set - 1)}"
            client.zrem(key, member)

        def zcard(client: Any, key: str, index: int) -> None:
            client.zcard(key)

        def zcount(client: Any, key: str, index: int) -> None:
            # Count members in score range
            min_score = random.uniform(0, 500000)
            max_score = min_score + 100000
            client.zcount(key, min_score, max_score)

        return {
            "ZADD": zadd,
            "ZRANGE": zrange,
            "ZREVRANGE": zrevrange,
            "ZRANGEBYSCORE": zrangebyscore,
            "ZRANK": zrank,
            "ZREVRANK": zrevrank,
            "ZSCORE": zscore,
            "ZINCRBY": zincrby,
            "ZREM": zrem,
            "ZCARD": zcard,
            "ZCOUNT": zcount,
        }


def main():
    """Test the zset runner."""
    runner = ZSetRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if not runner.is_available():
        print("redis-py not installed. Install with: pip install redis")
        return

    # Test with a simple workload
    from workload_loader import (
        WorkloadConfig, KeysConfig, ConcurrencyConfig, DataConfig, ZSetConfig
    )

    workload = WorkloadConfig(
        name="test-zset",
        description="Test sorted set workload",
        commands={"ZADD": 30, "ZINCRBY": 30, "ZRANGE": 20, "ZRANK": 15, "ZSCORE": 5},
        keys=KeysConfig(space_size=100, prefix="lb:"),
        data=DataConfig(size_bytes=128),
        zset=ZSetConfig(members_per_set=10000),
        concurrency=ConcurrencyConfig(clients=10, threads=2, pipeline=1),
    )

    print(f"\nWorkload: {workload.name}")
    print(f"Commands: {workload.commands}")
    print(f"Members per set: {workload.zset.members_per_set}")

    ops = runner.get_operations(workload)
    print(f"Supported operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
