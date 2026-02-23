"""
pubsub.py - Pub/Sub operations benchmark runner

Handles: PUBLISH, SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE
Tool: Custom redis-py implementation

Use cases: Real-time messaging, notifications, event broadcasting

Features:
- Publisher throughput measurement
- Subscriber latency measurement (end-to-end)
- Fan-out efficiency tracking
- Multi-threaded subscriber infrastructure
"""

import json
import queue
import struct
import threading
import time
from pathlib import Path
from typing import Any

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from workload_loader import WorkloadConfig, CommandCategory
from runners.base import (
    BenchmarkRunner,
    BenchmarkResult,
    CommandResult,
    register_runner,
)

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


@register_runner(CommandCategory.PUBSUB)
class PubSubRunner(BenchmarkRunner):
    """Pub/Sub operations runner with subscriber latency measurement.

    Architecture:
    - Subscriber threads listen on channels via pubsub.subscribe()
    - Publishers embed timestamp in message payload
    - Subscribers extract timestamp and compute end-to-end latency
    - Results aggregate both publish and subscriber-side metrics

    Metrics:
    - Messages published per second
    - Messages received per second
    - Publish-to-receive latency (p50, p95, p99)
    - Subscriber fan-out efficiency
    """

    name = "pubsub"
    version = "1.0.0"
    category = CommandCategory.PUBSUB

    def is_available(self) -> bool:
        """Check if redis-py is available."""
        return REDIS_AVAILABLE

    def run(
        self,
        workload: WorkloadConfig,
        output_dir: Path,
        requests: int = 100000,
        warmup: bool = True,
    ) -> BenchmarkResult:
        """Run the pub/sub benchmark with subscriber infrastructure.

        1. Start subscriber threads
        2. Run publishers (measure publish latency)
        3. Stop subscribers and collect end-to-end latencies
        4. Merge results
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        result = BenchmarkResult(
            workload_name=workload.name,
            backend_name="",
            runner_type=self.name,
            runner_version=self.version,
        )

        if not REDIS_AVAILABLE:
            result.targets_met = False
            result.target_failures = ["redis-py not installed"]
            return result

        num_channels = workload.keys.space_size
        message_size = workload.pubsub.message_size_bytes
        subscribers_per_channel = workload.pubsub.subscribers_per_channel

        # Queue for subscriber latencies
        subscriber_latencies: queue.Queue[float] = queue.Queue()
        stop_event = threading.Event()

        # 1. Start subscriber threads
        subscribers = self._start_subscribers(
            workload,
            num_channels,
            subscribers_per_channel,
            subscriber_latencies,
            stop_event,
        )

        # Give subscribers time to connect
        time.sleep(0.5)

        # 2. Run publishers
        publish_result = self._run_publishers(
            workload,
            num_channels,
            message_size,
            requests,
        )

        # 3. Wait briefly for messages to propagate, then stop subscribers
        time.sleep(0.5)
        stop_event.set()

        # Wait for subscribers to finish
        for sub in subscribers:
            sub.join(timeout=2.0)

        # 4. Collect subscriber latencies
        e2e_latencies: list[float] = []
        while not subscriber_latencies.empty():
            try:
                e2e_latencies.append(subscriber_latencies.get_nowait())
            except queue.Empty:
                break

        # Build final result
        result = self._build_result(publish_result, e2e_latencies, workload)

        # Save results
        json_file = output_dir / "pubsub_results.json"
        with open(json_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)

        self.check_targets(result, workload)
        return result

    def _start_subscribers(
        self,
        workload: WorkloadConfig,
        num_channels: int,
        subscribers_per_channel: int,
        latency_queue: queue.Queue[float],
        stop_event: threading.Event,
    ) -> list[threading.Thread]:
        """Start subscriber threads.

        Each thread subscribes to a subset of channels and records
        end-to-end latency for received messages.
        """
        subscribers: list[threading.Thread] = []

        # Distribute channels across subscriber threads
        # Each subscriber handles a portion of channels
        total_subscriptions = min(num_channels * subscribers_per_channel, 100)
        channels_per_thread = max(1, num_channels // max(1, total_subscriptions // subscribers_per_channel))

        for i in range(min(total_subscriptions, 10)):  # Cap at 10 subscriber threads
            start_channel = (i * channels_per_thread) % num_channels
            end_channel = min(start_channel + channels_per_thread, num_channels)

            channel_names = [
                f"{workload.keys.prefix}channel:{j}"
                for j in range(start_channel, end_channel)
            ]

            thread = threading.Thread(
                target=self._subscriber_worker,
                args=(channel_names, latency_queue, stop_event),
                daemon=True,
            )
            thread.start()
            subscribers.append(thread)

        return subscribers

    def _subscriber_worker(
        self,
        channels: list[str],
        latency_queue: queue.Queue[float],
        stop_event: threading.Event,
    ) -> None:
        """Worker thread that subscribes to channels and measures latency."""
        try:
            client = redis.Redis(host=self.host, port=self.port)
            pubsub = client.pubsub()
            pubsub.subscribe(*channels)

            while not stop_event.is_set():
                message = pubsub.get_message(timeout=0.1)
                if message and message["type"] == "message":
                    receive_time = time.perf_counter()
                    try:
                        # Extract timestamp from message payload
                        data = message["data"]
                        if isinstance(data, bytes):
                            # First 8 bytes are the timestamp (double)
                            if len(data) >= 8:
                                publish_time = struct.unpack("d", data[:8])[0]
                                latency_ms = (receive_time - publish_time) * 1000
                                if latency_ms >= 0:  # Sanity check
                                    latency_queue.put(latency_ms)
                    except Exception:
                        pass  # Skip malformed messages

            pubsub.unsubscribe()
            pubsub.close()
            client.close()
        except Exception:
            pass  # Subscriber errors are non-fatal

    def _run_publishers(
        self,
        workload: WorkloadConfig,
        num_channels: int,
        message_size: int,
        requests: int,
    ) -> dict[str, Any]:
        """Run publisher operations and measure throughput/latency."""
        client = redis.Redis(host=self.host, port=self.port)

        # Payload template (timestamp + padding)
        padding_size = max(0, message_size - 8)  # 8 bytes for timestamp
        padding = b"x" * padding_size

        published = 0
        errors = 0
        latencies: list[float] = []
        start_time = time.time()

        for i in range(requests):
            channel = f"{workload.keys.prefix}channel:{i % num_channels}"

            # Create message with embedded timestamp
            publish_time = time.perf_counter()
            message = struct.pack("d", publish_time) + padding

            op_start = time.perf_counter()
            try:
                client.publish(channel, message)
                published += 1
            except Exception:
                errors += 1
                continue

            latency_ms = (time.perf_counter() - op_start) * 1000
            latencies.append(latency_ms)

        duration = time.time() - start_time
        client.close()

        return {
            "published": published,
            "errors": errors,
            "latencies": latencies,
            "duration": duration,
        }

    def _build_result(
        self,
        publish_result: dict[str, Any],
        e2e_latencies: list[float],
        workload: WorkloadConfig,
    ) -> BenchmarkResult:
        """Build combined result from publish and subscriber metrics."""
        result = BenchmarkResult(
            workload_name=workload.name,
            backend_name="",
            runner_type=self.name,
            runner_version=self.version,
        )

        duration = publish_result["duration"]
        published = publish_result["published"]
        pub_latencies = publish_result["latencies"]

        # PUBLISH metrics
        if pub_latencies:
            sorted_pub = sorted(pub_latencies)
            result.commands["PUBLISH"] = CommandResult(
                command="PUBLISH",
                ops_per_sec=published / duration if duration > 0 else 0,
                p50_latency_ms=sorted_pub[int(len(sorted_pub) * 0.50)],
                p95_latency_ms=sorted_pub[int(len(sorted_pub) * 0.95)],
                p99_latency_ms=sorted_pub[int(len(sorted_pub) * 0.99)],
                total_ops=published,
                errors=publish_result["errors"],
            )

        # End-to-end (subscriber) metrics
        if e2e_latencies:
            sorted_e2e = sorted(e2e_latencies)
            result.commands["SUBSCRIBE_E2E"] = CommandResult(
                command="SUBSCRIBE_E2E",
                ops_per_sec=len(e2e_latencies) / duration if duration > 0 else 0,
                p50_latency_ms=sorted_e2e[int(len(sorted_e2e) * 0.50)],
                p95_latency_ms=sorted_e2e[int(len(sorted_e2e) * 0.95)],
                p99_latency_ms=sorted_e2e[int(len(sorted_e2e) * 0.99)],
                total_ops=len(e2e_latencies),
            )

            # Fan-out efficiency: messages received / messages published
            fan_out_ratio = len(e2e_latencies) / published if published > 0 else 0
            result.raw_output["fan_out_ratio"] = fan_out_ratio

        # Overall stats (use publish metrics as primary)
        result.total_ops = published
        result.total_ops_per_sec = published / duration if duration > 0 else 0
        result.duration_seconds = duration

        if pub_latencies:
            result.p50_latency_ms = result.commands["PUBLISH"].p50_latency_ms
            result.p99_latency_ms = result.commands["PUBLISH"].p99_latency_ms

        # Add e2e summary if available
        if e2e_latencies:
            result.raw_output["e2e_messages_received"] = len(e2e_latencies)
            result.raw_output["e2e_p50_latency_ms"] = result.commands["SUBSCRIBE_E2E"].p50_latency_ms
            result.raw_output["e2e_p99_latency_ms"] = result.commands["SUBSCRIBE_E2E"].p99_latency_ms

        return result


def main():
    """Test the pubsub runner."""
    runner = PubSubRunner()

    print(f"Runner: {runner.name} v{runner.version}")
    print(f"Available: {runner.is_available()}")

    if runner.is_available():
        from workload_loader import WorkloadConfig, KeysConfig, PubSubConfig

        workload = WorkloadConfig(
            name="test-pubsub",
            commands={"PUBLISH": 100},
            keys=KeysConfig(space_size=10, prefix="channel:"),
            pubsub=PubSubConfig(
                subscribers_per_channel=5,
                message_size_bytes=128,
            ),
        )

        print(f"\nWorkload: {workload.name}")
        print(f"Channels: {workload.keys.space_size}")
        print(f"Subscribers per channel: {workload.pubsub.subscribers_per_channel}")
        print(f"Message size: {workload.pubsub.message_size_bytes} bytes")
        print("\nFeatures:")
        print("  - Publisher throughput measurement")
        print("  - End-to-end subscriber latency measurement")
        print("  - Fan-out efficiency tracking")


if __name__ == "__main__":
    main()
