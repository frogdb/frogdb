#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml"]
# ///
"""
workload_loader.py - YAML workload parsing and validation

Loads and validates workload configuration files for the benchmark system.

Schema supports:
- Command categories (string, hash, list, zset, set, stream, pubsub, geo)
- Access patterns (uniform, gaussian, zipfian, sequential)
- Contention levels via keyspace sizing
- Persistence mode overrides
- Performance targets

Usage:
    from workload_loader import load_workload, WorkloadConfig

    config = load_workload("session-store")
    print(config.commands)
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml


class KeyPattern(Enum):
    """Key distribution patterns."""

    UNIFORM = "uniform"  # Equal probability
    GAUSSIAN = "gaussian"  # Bell curve (hot spot)
    ZIPFIAN = "zipfian"  # Power law (80/20)
    SEQUENTIAL = "sequential"  # Linear ordering


class PersistenceMode(Enum):
    """Persistence modes."""

    ASYNC = "async"  # No sync (max throughput)
    PERIODIC = "periodic"  # Periodic sync (balanced)
    SYNC = "sync"  # Every write synced


class CommandCategory(Enum):
    """Command categories for routing to runners."""

    STRING = "string"  # GET, SET, INCR, MGET, MSET, APPEND
    HASH = "hash"  # HSET, HGET, HMSET, HMGET, HGETALL, HDEL
    LIST = "list"  # LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN
    SET = "set"  # SADD, SMEMBERS, SINTER, SUNION, SISMEMBER
    ZSET = "zset"  # ZADD, ZRANGE, ZRANK, ZSCORE, ZINCRBY
    STREAM = "stream"  # XADD, XREAD, XRANGE, XLEN
    PUBSUB = "pubsub"  # PUBLISH, SUBSCRIBE
    GEO = "geo"  # GEOADD, GEORADIUS, GEODIST


# Mapping from command names to categories
COMMAND_CATEGORIES: dict[str, CommandCategory] = {
    # String/Key commands
    "GET": CommandCategory.STRING,
    "SET": CommandCategory.STRING,
    "DEL": CommandCategory.STRING,
    "MGET": CommandCategory.STRING,
    "MSET": CommandCategory.STRING,
    "INCR": CommandCategory.STRING,
    "DECR": CommandCategory.STRING,
    "APPEND": CommandCategory.STRING,
    "GETSET": CommandCategory.STRING,
    "SETNX": CommandCategory.STRING,
    "SETEX": CommandCategory.STRING,
    "EXISTS": CommandCategory.STRING,
    "EXPIRE": CommandCategory.STRING,
    "TTL": CommandCategory.STRING,
    # Hash commands
    "HSET": CommandCategory.HASH,
    "HGET": CommandCategory.HASH,
    "HMSET": CommandCategory.HASH,
    "HMGET": CommandCategory.HASH,
    "HGETALL": CommandCategory.HASH,
    "HDEL": CommandCategory.HASH,
    "HINCRBY": CommandCategory.HASH,
    "HLEN": CommandCategory.HASH,
    "HEXISTS": CommandCategory.HASH,
    "HKEYS": CommandCategory.HASH,
    "HVALS": CommandCategory.HASH,
    # List commands
    "LPUSH": CommandCategory.LIST,
    "RPUSH": CommandCategory.LIST,
    "LPOP": CommandCategory.LIST,
    "RPOP": CommandCategory.LIST,
    "LRANGE": CommandCategory.LIST,
    "LLEN": CommandCategory.LIST,
    "LINDEX": CommandCategory.LIST,
    "LSET": CommandCategory.LIST,
    "LTRIM": CommandCategory.LIST,
    # Set commands
    "SADD": CommandCategory.SET,
    "SMEMBERS": CommandCategory.SET,
    "SISMEMBER": CommandCategory.SET,
    "SREM": CommandCategory.SET,
    "SCARD": CommandCategory.SET,
    "SINTER": CommandCategory.SET,
    "SUNION": CommandCategory.SET,
    "SDIFF": CommandCategory.SET,
    # Sorted Set commands
    "ZADD": CommandCategory.ZSET,
    "ZRANGE": CommandCategory.ZSET,
    "ZRANK": CommandCategory.ZSET,
    "ZSCORE": CommandCategory.ZSET,
    "ZINCRBY": CommandCategory.ZSET,
    "ZREM": CommandCategory.ZSET,
    "ZCARD": CommandCategory.ZSET,
    "ZCOUNT": CommandCategory.ZSET,
    "ZRANGEBYSCORE": CommandCategory.ZSET,
    "ZREVRANGE": CommandCategory.ZSET,
    # Stream commands
    "XADD": CommandCategory.STREAM,
    "XREAD": CommandCategory.STREAM,
    "XRANGE": CommandCategory.STREAM,
    "XLEN": CommandCategory.STREAM,
    "XINFO": CommandCategory.STREAM,
    "XTRIM": CommandCategory.STREAM,
    "XGROUP": CommandCategory.STREAM,
    "XREADGROUP": CommandCategory.STREAM,
    "XACK": CommandCategory.STREAM,
    "XPENDING": CommandCategory.STREAM,
    # Pub/Sub commands
    "PUBLISH": CommandCategory.PUBSUB,
    "SUBSCRIBE": CommandCategory.PUBSUB,
    "PSUBSCRIBE": CommandCategory.PUBSUB,
    "UNSUBSCRIBE": CommandCategory.PUBSUB,
    # Geo commands
    "GEOADD": CommandCategory.GEO,
    "GEORADIUS": CommandCategory.GEO,
    "GEODIST": CommandCategory.GEO,
    "GEOPOS": CommandCategory.GEO,
    "GEOHASH": CommandCategory.GEO,
    "GEOSEARCH": CommandCategory.GEO,
}


@dataclass
class KeysConfig:
    """Key configuration."""

    pattern: KeyPattern = KeyPattern.UNIFORM
    space_size: int = 1_000_000
    prefix: str = ""


@dataclass
class DataConfig:
    """Data configuration."""

    size_bytes: int = 128
    size_distribution: str = "fixed"  # fixed or random
    ttl_seconds: int = 0  # 0 = no TTL


@dataclass
class ConcurrencyConfig:
    """Concurrency configuration."""

    clients: int = 100
    threads: int = 4
    pipeline: int = 1
    rate_limit_rps: int = 0  # 0 = unlimited


@dataclass
class PersistenceConfig:
    """Persistence configuration."""

    mode: PersistenceMode = PersistenceMode.ASYNC


@dataclass
class TargetsConfig:
    """Performance target configuration."""

    ops_per_sec: int = 0
    p50_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0


@dataclass
class HashConfig:
    """Hash-specific configuration."""

    fields_per_hash: int = 10


@dataclass
class ListConfig:
    """List-specific configuration."""

    max_length: int = 1000


@dataclass
class ZSetConfig:
    """Sorted Set-specific configuration."""

    members_per_set: int = 1000


@dataclass
class StreamConfig:
    """Stream-specific configuration."""

    fields_per_entry: int = 5
    max_len: int = 0  # 0 = unlimited


@dataclass
class GeoConfig:
    """Geo-specific configuration."""

    points_per_index: int = 10000
    radius_km: float = 10.0


@dataclass
class PubSubConfig:
    """Pub/Sub-specific configuration."""

    subscribers_per_channel: int = 10
    message_size_bytes: int = 128


@dataclass
class ClusterConfig:
    """Cluster-specific configuration."""

    enabled: bool = False
    nodes: int = 3
    hash_slots: int = 16384
    measure_slot_distribution: bool = True
    measure_cross_slot_latency: bool = True
    simulate_failover: bool = False


@dataclass
class WorkloadConfig:
    """Complete workload configuration."""

    name: str
    description: str = ""
    scenario: str = ""

    # Command distribution: {"GET": 95, "SET": 5}
    commands: dict[str, int] = field(default_factory=dict)

    # Configurations
    keys: KeysConfig = field(default_factory=KeysConfig)
    data: DataConfig = field(default_factory=DataConfig)
    concurrency: ConcurrencyConfig = field(default_factory=ConcurrencyConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    targets: TargetsConfig = field(default_factory=TargetsConfig)

    # Data structure specific configs
    hash: HashConfig = field(default_factory=HashConfig)
    list: ListConfig = field(default_factory=ListConfig)
    zset: ZSetConfig = field(default_factory=ZSetConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)
    geo: GeoConfig = field(default_factory=GeoConfig)
    pubsub: PubSubConfig = field(default_factory=PubSubConfig)

    # Cluster config
    cluster: ClusterConfig = field(default_factory=ClusterConfig)

    def get_command_categories(self) -> dict[CommandCategory, dict[str, int]]:
        """Group commands by category with their percentages."""
        categories: dict[CommandCategory, dict[str, int]] = {}
        for cmd, pct in self.commands.items():
            cmd_upper = cmd.upper()
            if cmd_upper not in COMMAND_CATEGORIES:
                raise ValueError(f"Unknown command: {cmd}")
            cat = COMMAND_CATEGORIES[cmd_upper]
            if cat not in categories:
                categories[cat] = {}
            categories[cat][cmd_upper] = pct
        return categories

    def get_primary_category(self) -> CommandCategory:
        """Return the category with the most commands."""
        categories = self.get_command_categories()
        if not categories:
            return CommandCategory.STRING
        return max(categories.keys(), key=lambda c: sum(categories[c].values()))

    def get_memtier_ratio(self) -> str | None:
        """Get memtier-compatible ratio string (only works for GET/SET workloads)."""
        get_pct = self.commands.get("GET", 0) + self.commands.get("get", 0)
        set_pct = self.commands.get("SET", 0) + self.commands.get("set", 0)

        if get_pct + set_pct != 100:
            return None  # Not a pure GET/SET workload

        # memtier uses SET:GET ratio
        from math import gcd

        if get_pct == 0:
            return "1:0"
        if set_pct == 0:
            return "0:1"

        divisor = gcd(set_pct, get_pct)
        return f"{set_pct // divisor}:{get_pct // divisor}"

    def get_memtier_key_pattern(self) -> str:
        """Convert key pattern to memtier format."""
        pattern_map = {
            KeyPattern.UNIFORM: "R",  # Random
            KeyPattern.GAUSSIAN: "G",  # Gaussian
            KeyPattern.ZIPFIAN: "R",  # memtier doesn't have Zipfian, use Random
            KeyPattern.SEQUENTIAL: "S",  # Sequential
        }
        p = pattern_map.get(self.keys.pattern, "R")
        return f"{p}:{p}"


class WorkloadValidationError(Exception):
    """Raised when workload validation fails."""

    pass


def _parse_key_pattern(value: str) -> KeyPattern:
    """Parse key pattern from string."""
    try:
        return KeyPattern(value.lower())
    except ValueError as err:
        valid = [p.value for p in KeyPattern]
        raise WorkloadValidationError(f"Invalid key pattern: {value}. Valid: {valid}") from err


def _parse_persistence_mode(value: str) -> PersistenceMode:
    """Parse persistence mode from string."""
    try:
        return PersistenceMode(value.lower())
    except ValueError as err:
        valid = [m.value for m in PersistenceMode]
        raise WorkloadValidationError(f"Invalid persistence mode: {value}. Valid: {valid}") from err


def _parse_commands(commands: list[dict[str, Any]] | dict[str, Any]) -> dict[str, int]:
    """Parse command distribution from YAML."""
    result: dict[str, int] = {}

    if isinstance(commands, dict):
        # Simple format: {GET: 95, SET: 5}
        for cmd, pct in commands.items():
            if isinstance(pct, str) and pct.endswith("%"):
                pct = int(pct[:-1])
            result[cmd.upper()] = int(pct)
    elif isinstance(commands, list):
        # List format: [{GET: 95%}, {SET: 5%}]
        for item in commands:
            for cmd, pct in item.items():
                if isinstance(pct, str) and pct.endswith("%"):
                    pct = int(pct[:-1])
                result[cmd.upper()] = int(pct)
    else:
        raise WorkloadValidationError(f"Invalid commands format: {type(commands)}")

    return result


def _validate_commands(commands: dict[str, int]) -> None:
    """Validate command distribution."""
    if not commands:
        raise WorkloadValidationError("No commands specified")

    total = sum(commands.values())
    if total != 100:
        raise WorkloadValidationError(f"Command percentages must sum to 100, got {total}")

    for cmd in commands:
        if cmd not in COMMAND_CATEGORIES:
            raise WorkloadValidationError(f"Unknown command: {cmd}")


def load_workload_from_dict(data: dict[str, Any]) -> WorkloadConfig:
    """Load workload configuration from a dictionary."""
    if "name" not in data:
        raise WorkloadValidationError("Workload must have a 'name' field")

    config = WorkloadConfig(
        name=data["name"],
        description=data.get("description", ""),
        scenario=data.get("scenario", ""),
    )

    # Parse commands
    if "commands" in data:
        config.commands = _parse_commands(data["commands"])
        _validate_commands(config.commands)

    # Parse keys config
    if "keys" in data:
        keys_data = data["keys"]
        config.keys = KeysConfig(
            pattern=_parse_key_pattern(keys_data.get("pattern", "uniform")),
            space_size=keys_data.get("space_size", 1_000_000),
            prefix=keys_data.get("prefix", ""),
        )

    # Parse data config
    if "data" in data:
        data_cfg = data["data"]
        config.data = DataConfig(
            size_bytes=data_cfg.get("size_bytes", 128),
            size_distribution=data_cfg.get("size_distribution", "fixed"),
            ttl_seconds=data_cfg.get("ttl_seconds", 0),
        )

    # Parse concurrency config
    if "concurrency" in data:
        conc_data = data["concurrency"]
        config.concurrency = ConcurrencyConfig(
            clients=conc_data.get("clients", 100),
            threads=conc_data.get("threads", 4),
            pipeline=conc_data.get("pipeline", 1),
            rate_limit_rps=conc_data.get("rate_limit_rps", 0),
        )

    # Parse persistence config
    if "persistence" in data:
        pers_data = data["persistence"]
        config.persistence = PersistenceConfig(
            mode=_parse_persistence_mode(pers_data.get("mode", "async")),
        )

    # Parse targets config
    if "targets" in data:
        targets_data = data["targets"]
        config.targets = TargetsConfig(
            ops_per_sec=targets_data.get("ops_per_sec", 0),
            p50_latency_ms=targets_data.get("p50_latency_ms", 0.0),
            p95_latency_ms=targets_data.get("p95_latency_ms", 0.0),
            p99_latency_ms=targets_data.get("p99_latency_ms", 0.0),
        )

    # Parse data structure specific configs
    if "hash" in data:
        config.hash = HashConfig(
            fields_per_hash=data["hash"].get("fields_per_hash", 10),
        )

    if "list" in data:
        config.list = ListConfig(
            max_length=data["list"].get("max_length", 1000),
        )

    if "zset" in data:
        config.zset = ZSetConfig(
            members_per_set=data["zset"].get("members_per_set", 1000),
        )

    if "stream" in data:
        config.stream = StreamConfig(
            fields_per_entry=data["stream"].get("fields_per_entry", 5),
            max_len=data["stream"].get("max_len", 0),
        )

    if "geo" in data:
        config.geo = GeoConfig(
            points_per_index=data["geo"].get("points_per_index", 10000),
            radius_km=data["geo"].get("radius_km", 10.0),
        )

    if "pubsub" in data:
        config.pubsub = PubSubConfig(
            subscribers_per_channel=data["pubsub"].get("subscribers_per_channel", 10),
            message_size_bytes=data["pubsub"].get("message_size_bytes", 128),
        )

    # Parse cluster config
    if "cluster" in data:
        cluster_data = data["cluster"]
        config.cluster = ClusterConfig(
            enabled=cluster_data.get("enabled", False),
            nodes=cluster_data.get("nodes", 3),
            hash_slots=cluster_data.get("hash_slots", 16384),
            measure_slot_distribution=cluster_data.get("measure_slot_distribution", True),
            measure_cross_slot_latency=cluster_data.get("measure_cross_slot_latency", True),
            simulate_failover=cluster_data.get("simulate_failover", False),
        )

    return config


def load_workload(name: str, workloads_dir: Path | None = None) -> WorkloadConfig:
    """Load a workload configuration by name.

    Args:
        name: Workload name (without .yaml extension)
        workloads_dir: Directory containing workload YAML files.
                       Defaults to testing/load/workloads/

    Returns:
        WorkloadConfig instance

    Raises:
        FileNotFoundError: If workload file doesn't exist
        WorkloadValidationError: If workload is invalid
    """
    if workloads_dir is None:
        script_dir = Path(__file__).parent.resolve()
        workloads_dir = script_dir.parent / "workloads"

    workload_file = workloads_dir / f"{name}.yaml"
    if not workload_file.exists():
        # Try without extension (in case name includes .yaml)
        if name.endswith(".yaml"):
            workload_file = workloads_dir / name
        else:
            raise FileNotFoundError(f"Workload not found: {workload_file}")

    with open(workload_file) as f:
        data = yaml.safe_load(f)

    return load_workload_from_dict(data)


def list_workloads(workloads_dir: Path | None = None) -> list[str]:
    """List available workload names.

    Args:
        workloads_dir: Directory containing workload YAML files.
                       Defaults to testing/load/workloads/

    Returns:
        List of workload names (without .yaml extension)
    """
    if workloads_dir is None:
        script_dir = Path(__file__).parent.resolve()
        workloads_dir = script_dir.parent / "workloads"

    if not workloads_dir.exists():
        return []

    return sorted(
        [
            f.stem
            for f in workloads_dir.glob("*.yaml")
            if not f.name.startswith("_")  # Skip private files
        ]
    )


def main() -> None:
    """CLI entry point for testing workload loading."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Load and display workload configurations")
    parser.add_argument("workload", nargs="?", help="Workload name to load")
    parser.add_argument("--list", action="store_true", help="List available workloads")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--validate", action="store_true", help="Validate all workloads")
    args = parser.parse_args()

    if args.list:
        workloads = list_workloads()
        if workloads:
            print("Available workloads:")
            for w in workloads:
                print(f"  - {w}")
        else:
            print("No workloads found")
        return

    if args.validate:
        workloads = list_workloads()
        errors = []
        for w in workloads:
            try:
                load_workload(w)
                print(f"  OK: {w}")
            except Exception as e:
                errors.append((w, str(e)))
                print(f"  FAIL: {w} - {e}")

        if errors:
            print(f"\n{len(errors)} workload(s) failed validation")
            exit(1)
        else:
            print(f"\nAll {len(workloads)} workload(s) valid")
        return

    if not args.workload:
        parser.print_help()
        return

    try:
        config = load_workload(args.workload)

        if args.json:
            # Convert dataclass to dict for JSON output
            from dataclasses import asdict

            output = asdict(config)
            # Convert enums to strings
            output["keys"]["pattern"] = config.keys.pattern.value
            output["persistence"]["mode"] = config.persistence.mode.value
            print(json.dumps(output, indent=2))
        else:
            print(f"Workload: {config.name}")
            print(f"Description: {config.description}")
            print(f"Scenario: {config.scenario}")
            print(f"Commands: {config.commands}")
            print(f"Key Pattern: {config.keys.pattern.value}")
            print(f"Key Space: {config.keys.space_size:,}")
            print(f"Data Size: {config.data.size_bytes} bytes")
            print(f"Clients: {config.concurrency.clients}")
            print(f"Pipeline: {config.concurrency.pipeline}")

            if config.targets.ops_per_sec > 0:
                print(f"Target OPS: {config.targets.ops_per_sec:,}")
            if config.targets.p99_latency_ms > 0:
                print(f"Target p99: {config.targets.p99_latency_ms}ms")

            # Show memtier config if applicable
            ratio = config.get_memtier_ratio()
            if ratio:
                print("\nMemtier Config:")
                print(f"  Ratio: {ratio}")
                print(f"  Key Pattern: {config.get_memtier_key_pattern()}")

    except FileNotFoundError as e:
        print(f"Error: {e}")
        exit(1)
    except WorkloadValidationError as e:
        print(f"Validation error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
