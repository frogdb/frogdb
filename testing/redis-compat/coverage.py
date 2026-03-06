#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""
Redis Command Coverage Analyzer for FrogDB

Analyzes the Redis test suite to determine which commands are tested
and compares against FrogDB's implemented commands.
"""

import argparse
import re
import sys
from pathlib import Path

# Commands implemented in FrogDB (based on spec/COMMANDS.md and implementation)
# This list should be updated as new commands are implemented
IMPLEMENTED_COMMANDS = {
    # Connection
    "PING",
    "ECHO",
    "QUIT",
    "AUTH",
    "SELECT",
    # Server
    "DBSIZE",
    "FLUSHDB",
    "FLUSHALL",
    "INFO",
    "TIME",
    "COMMAND",
    "CONFIG",
    "BGSAVE",
    "LASTSAVE",
    "DEBUG",
    # Keys
    "DEL",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "KEYS",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PTTL",
    "RANDOMKEY",
    "RENAME",
    "RENAMENX",
    "SCAN",
    "TOUCH",
    "TTL",
    "TYPE",
    "UNLINK",
    "COPY",
    "OBJECT",
    # Strings
    "APPEND",
    "DECR",
    "DECRBY",
    "GET",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "MGET",
    "MSET",
    "MSETNX",
    "PSETEX",
    "SET",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "STRLEN",
    "SUBSTR",
    # Lists
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "LMPOP",
    # Sets
    "SADD",
    "SCARD",
    "SDIFF",
    "SDIFFSTORE",
    "SINTER",
    "SINTERCARD",
    "SINTERSTORE",
    "SISMEMBER",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "SUNION",
    "SUNIONSTORE",
    # Hashes
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "HVALS",
    # Sorted Sets
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTER",
    "ZINTERCARD",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
    "ZUNIONSTORE",
    # HyperLogLog
    "PFADD",
    "PFCOUNT",
    "PFMERGE",
    # Geo
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUSBYMEMBER",
    "GEOSEARCH",
    "GEOSEARCHSTORE",
    # Bitmap
    "BITCOUNT",
    "BITFIELD",
    "BITOP",
    "BITPOS",
    "GETBIT",
    "SETBIT",
    # Pub/Sub
    "PUBLISH",
    "SUBSCRIBE",
    "UNSUBSCRIBE",
    "PSUBSCRIBE",
    "PUNSUBSCRIBE",
    "PUBSUB",
    # Transactions
    "MULTI",
    "EXEC",
    "DISCARD",
    "WATCH",
    "UNWATCH",
    # Scripting
    "EVAL",
    "EVALSHA",
    "SCRIPT",
    # Client
    "CLIENT",
}

# Commands that are intentionally not supported
INTENTIONALLY_UNSUPPORTED = {
    "SELECT",  # Single database model
    "MIGRATE",  # Different cluster model
    "WAIT",  # Different replication model
    "OBJECT FREQ",  # LFU not persisted
    "OBJECT IDLETIME",  # LRU not persisted
}

# Commands planned but not yet implemented
NOT_YET_IMPLEMENTED = {
    # Blocking commands (Phase 11)
    "BLPOP",
    "BRPOP",
    "BLMOVE",
    "BRPOPLPUSH",
    "BLMPOP",
    "BZPOPMIN",
    "BZPOPMAX",
    "BZMPOP",
    # Streams (Phase 13)
    "XADD",
    "XREAD",
    "XREADGROUP",
    "XRANGE",
    "XREVRANGE",
    "XLEN",
    "XINFO",
    "XACK",
    "XCLAIM",
    "XAUTOCLAIM",
    "XPENDING",
    "XTRIM",
    "XDEL",
    "XGROUP",
    "XSETID",
    # Cluster (Phase 14)
    "CLUSTER",
    "READONLY",
    "READWRITE",
    "ASKING",
    # RESP3 (Phase 12)
    "HELLO",
    # Other deferred
    "MEMORY",
    "LATENCY",
    "MODULE",
    "ACL",
    "SLOWLOG",
    "DEBUG",
    "MONITOR",
    "FUNCTION",
    "FCALL",
    "FCALL_RO",
}


def get_script_dir() -> Path:
    """Get the directory containing this script."""
    return Path(__file__).parent.resolve()


def get_project_root() -> Path:
    """Get the FrogDB project root directory."""
    return get_script_dir().parent.parent


def find_redis_tests(cache_dir: Path) -> Path | None:
    """Find the Redis test directory."""
    for item in cache_dir.iterdir():
        if item.is_dir() and item.name.startswith("redis-"):
            tests_dir = item / "tests"
            if tests_dir.exists():
                return tests_dir
    return None


def extract_commands_from_tests(tests_dir: Path) -> dict[str, set[str]]:
    """Extract Redis commands used in test files."""
    commands_by_file: dict[str, set[str]] = {}

    # Common Redis command pattern in Tcl tests
    # Matches: r command, $r command, {*}$r command, assert_equal [$r command]
    command_pattern = re.compile(
        r"(?:\$?r\s+|assert_\w+\s+\[(?:\$?r|\{\*\}\$r)\s+)(\w+)", re.IGNORECASE
    )

    for tcl_file in tests_dir.rglob("*.tcl"):
        relative_path = str(tcl_file.relative_to(tests_dir))
        commands = set()

        try:
            content = tcl_file.read_text(encoding="utf-8", errors="ignore")

            # Find commands in test assertions and calls
            for match in command_pattern.finditer(content):
                cmd = match.group(1).upper()
                if cmd not in {
                    "SET",
                    "GET",
                    "IF",
                    "ELSE",
                    "PROC",
                    "RETURN",
                    "EXPR",
                    "PUTS",
                    "CATCH",
                }:
                    # Filter out Tcl keywords that might match
                    commands.add(cmd)

            if commands:
                commands_by_file[relative_path] = commands

        except Exception as e:
            print(f"Warning: Could not read {tcl_file}: {e}", file=sys.stderr)

    return commands_by_file


def load_skiplists(script_dir: Path) -> set[str]:
    """Load all skiplist files and return skipped test patterns."""
    skipped = set()

    skipfiles = [
        script_dir / "skiplist-intentional.txt",
        script_dir / "skiplist-not-implemented.txt",
        script_dir / "skiplist-flaky.txt",
    ]

    for skipfile in skipfiles:
        if skipfile.exists():
            with open(skipfile) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        skipped.add(line)

    return skipped


def analyze_coverage(
    commands_by_file: dict[str, set[str]],
    skipped_tests: set[str],
) -> dict:
    """Analyze command coverage."""
    # Collect all commands from non-skipped tests
    tested_commands: set[str] = set()
    skipped_commands: set[str] = set()

    for test_file, commands in commands_by_file.items():
        # Check if this test file is skipped
        is_skipped = any(test_file.startswith(skip) or skip in test_file for skip in skipped_tests)

        if is_skipped:
            skipped_commands.update(commands)
        else:
            tested_commands.update(commands)

    # Calculate coverage
    implemented_and_tested = IMPLEMENTED_COMMANDS & tested_commands
    implemented_not_tested = IMPLEMENTED_COMMANDS - tested_commands
    tested_not_implemented = tested_commands - IMPLEMENTED_COMMANDS - NOT_YET_IMPLEMENTED

    return {
        "implemented_count": len(IMPLEMENTED_COMMANDS),
        "tested_count": len(tested_commands),
        "implemented_and_tested": implemented_and_tested,
        "implemented_not_tested": implemented_not_tested,
        "tested_not_implemented": tested_not_implemented,
        "skipped_commands": skipped_commands,
        "coverage_percent": (
            len(implemented_and_tested) / len(IMPLEMENTED_COMMANDS) * 100
            if IMPLEMENTED_COMMANDS
            else 0
        ),
    }


def print_report(analysis: dict, verbose: bool = False) -> None:
    """Print the coverage report."""
    print("=" * 60)
    print("FrogDB Redis Command Coverage Report")
    print("=" * 60)
    print()

    print(f"Implemented commands:     {analysis['implemented_count']}")
    print(f"Commands tested by suite: {analysis['tested_count']}")
    print(f"Coverage:                 {analysis['coverage_percent']:.1f}%")
    print()

    if analysis["implemented_and_tested"]:
        print(f"✓ Implemented and tested: {len(analysis['implemented_and_tested'])} commands")
        if verbose:
            for cmd in sorted(analysis["implemented_and_tested"]):
                print(f"    {cmd}")
        print()

    if analysis["implemented_not_tested"]:
        print(f"⚠ Implemented but not tested: {len(analysis['implemented_not_tested'])} commands")
        for cmd in sorted(analysis["implemented_not_tested"]):
            print(f"    {cmd}")
        print()

    if analysis["tested_not_implemented"]:
        print(f"✗ Tested but not implemented: {len(analysis['tested_not_implemented'])} commands")
        for cmd in sorted(analysis["tested_not_implemented"]):
            status = "(planned)" if cmd in NOT_YET_IMPLEMENTED else "(unknown)"
            print(f"    {cmd} {status}")
        print()

    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze Redis command coverage for FrogDB")
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show all tested commands",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )

    args = parser.parse_args()

    script_dir = get_script_dir()
    project_root = get_project_root()
    cache_dir = project_root / ".redis-tests"

    # Find Redis tests
    tests_dir = find_redis_tests(cache_dir)
    if tests_dir is None:
        print("Error: Redis test suite not found. Run 'just redis-compat' first to download.")
        print(f"Expected location: {cache_dir}/redis-*/tests/")
        sys.exit(1)

    print(f"Analyzing tests in: {tests_dir}")
    print()

    # Extract commands and analyze
    commands_by_file = extract_commands_from_tests(tests_dir)
    skipped_tests = load_skiplists(script_dir)
    analysis = analyze_coverage(commands_by_file, skipped_tests)

    if args.json:
        import json

        # Convert sets to lists for JSON serialization
        output = {k: list(v) if isinstance(v, set) else v for k, v in analysis.items()}
        print(json.dumps(output, indent=2))
    else:
        print_report(analysis, verbose=args.verbose)


if __name__ == "__main__":
    main()
