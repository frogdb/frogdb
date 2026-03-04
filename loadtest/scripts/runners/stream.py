"""
stream.py - Stream operations benchmark runner

Handles: XADD, XREAD, XRANGE, XLEN, XINFO, XTRIM, XGROUP, XREADGROUP, XACK, XPENDING
Tool: redis-py

Use cases: Event sourcing, audit logs, message queues, time-series data
"""

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import CommandCategory, WorkloadConfig

from runners.base import BenchmarkResult, register_runner
from runners.redis_py_base import RedisPyRunner


@register_runner(CommandCategory.STREAM)
class StreamRunner(RedisPyRunner):
    """Stream operations runner using redis-py.

    Supports:
    - XADD: Append entries to stream
    - XREAD: Read entries (blocking and non-blocking)
    - XRANGE: Get entries by ID range
    - XLEN: Get stream length
    - XINFO: Get stream info
    - XTRIM: Trim stream to max length
    - XGROUP: Create consumer groups
    - XREADGROUP: Read as consumer from group
    - XACK: Acknowledge processed messages
    - XPENDING: Check pending messages
    """

    name = "stream"
    version = "1.0.0"
    category = CommandCategory.STREAM

    # Consumer group name used for benchmarking
    CONSUMER_GROUP = "benchmark_group"

    def setup_data(self, client: Any, workload: WorkloadConfig, count: int) -> None:
        """Pre-populate streams with entries and create consumer groups."""
        fields_per_entry = workload.stream.fields_per_entry

        # Check if workload uses consumer group operations
        uses_consumer_groups = any(
            cmd.upper() in ("XGROUP", "XREADGROUP", "XACK", "XPENDING") for cmd in workload.commands
        )

        for i in range(min(count, 100)):
            key = self._generate_key(workload, i)
            # Add initial entries
            for _j in range(10):
                fields = {f"field{k}": f"value{k}" for k in range(fields_per_entry)}
                client.xadd(key, fields)

            # Create consumer group if needed
            if uses_consumer_groups:
                try:
                    client.xgroup_create(key, self.CONSUMER_GROUP, id="0", mkstream=True)
                except Exception:
                    # Group may already exist
                    pass

    def get_operations(
        self,
        workload: WorkloadConfig,
    ) -> dict[str, Callable[[Any, str, int], None]]:
        """Get stream operation functions."""
        fields_per_entry = workload.stream.fields_per_entry
        max_len = workload.stream.max_len
        consumer_group = self.CONSUMER_GROUP

        def xadd(client: Any, key: str, index: int) -> None:
            fields = {f"field{i}": f"value{index}_{i}" for i in range(fields_per_entry)}
            if max_len > 0:
                client.xadd(key, fields, maxlen=max_len, approximate=True)
            else:
                client.xadd(key, fields)

        def xread(client: Any, key: str, index: int) -> None:
            # Non-blocking read of latest entries
            client.xread({key: "0-0"}, count=10, block=None)

        def xrange(client: Any, key: str, index: int) -> None:
            # Get first 10 entries
            client.xrange(key, min="-", max="+", count=10)

        def xlen(client: Any, key: str, index: int) -> None:
            client.xlen(key)

        def xinfo(client: Any, key: str, index: int) -> None:
            try:
                client.xinfo_stream(key)
            except Exception:
                pass  # Stream may not exist

        def xtrim(client: Any, key: str, index: int) -> None:
            if max_len > 0:
                client.xtrim(key, maxlen=max_len, approximate=True)

        def xgroup(client: Any, key: str, index: int) -> None:
            # Create a new consumer group (or handle existing)
            group_name = f"{consumer_group}_{index % 10}"
            try:
                client.xgroup_create(key, group_name, id="0", mkstream=True)
            except Exception:
                # Group already exists - this is expected in benchmarks
                pass

        def xreadgroup(client: Any, key: str, index: int) -> None:
            # Read as consumer from group
            consumer_name = f"consumer_{index % 10}"
            try:
                client.xreadgroup(
                    consumer_group,
                    consumer_name,
                    {key: ">"},
                    count=1,
                    block=None,
                )
            except Exception:
                # Group may not exist or no new messages
                pass

        def xack(client: Any, key: str, index: int) -> None:
            # Acknowledge messages - in benchmark we ack message ID 0-0
            # In real usage, you'd track actual message IDs
            try:
                # Get pending messages and ack them
                pending = client.xpending_range(key, consumer_group, "-", "+", 1)
                if pending:
                    msg_id = pending[0]["message_id"]
                    client.xack(key, consumer_group, msg_id)
            except Exception:
                # No pending messages or group doesn't exist
                pass

        def xpending(client: Any, key: str, index: int) -> None:
            # Check pending messages in consumer group
            try:
                client.xpending(key, consumer_group)
            except Exception:
                # Group may not exist
                pass

        return {
            "XADD": xadd,
            "XREAD": xread,
            "XRANGE": xrange,
            "XLEN": xlen,
            "XINFO": xinfo,
            "XTRIM": xtrim,
            "XGROUP": xgroup,
            "XREADGROUP": xreadgroup,
            "XACK": xack,
            "XPENDING": xpending,
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
    """Test the stream runner."""
    runner = StreamRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if runner.is_available():
        from workload_loader import KeysConfig, StreamConfig, WorkloadConfig

        workload = WorkloadConfig(
            name="test-stream",
            commands={"XADD": 60, "XREADGROUP": 25, "XACK": 10, "XPENDING": 5},
            keys=KeysConfig(space_size=100, prefix="stream:"),
            stream=StreamConfig(fields_per_entry=5, max_len=10000),
        )

        ops = runner.get_operations(workload)
        print(f"Implemented operations: {list(ops.keys())}")


if __name__ == "__main__":
    main()
