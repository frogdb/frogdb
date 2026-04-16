#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Generate compatibility exclusions JSON from regression test metadata.

Parses the `## Intentional exclusions` doc-comment sections from the Rust
port files in `frogdb-server/crates/redis-regression/tests/` and produces
`website/src/data/compat-exclusions.json` consumed by the Astro docs site.

Usage:
    uv run website/scripts/compat-gen.py            # generate
    uv run website/scripts/compat-gen.py --check     # verify (CI)
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
TESTS_DIR = REPO_ROOT / "frogdb-server" / "crates" / "redis-regression" / "tests"
DEFAULT_OUTPUT = REPO_ROOT / "website" / "src" / "data" / "compat-exclusions.json"

# ---------------------------------------------------------------------------
# Regexes (from audit_tcl.py)
# ---------------------------------------------------------------------------

INTENTIONAL_HEADER_RE = re.compile(r"//!\s*##\s*Intentional exclusions", re.IGNORECASE)
# New format: `name` -- category -- prose
INTENTIONAL_BULLET_NEW_RE = re.compile(
    r"//!\s*-\s*`([^`]+)`\s*\u2014\s*([a-z][a-z0-9:-]*)\s*\u2014\s*(.*)$"
)
# Legacy format: `name` -- reason (no structured category)
INTENTIONAL_BULLET_RE = re.compile(r"//!\s*-\s*`([^`]+)`\s*\u2014\s*(.*)$")
# Continuation lines for multi-line reasons
INTENTIONAL_CONT_RE = re.compile(r"^//!\s{2,}(.+)$")
# Count #[ignore = "..."] tests (broken tests)
IGNORE_RE = re.compile(r'#\[ignore\s*=\s*"([^"]+)"\]')
# Extract test function names
TEST_FN_RE = re.compile(
    r"#\[tokio::test\b[^\]]*\][^\n]*\n(?:\s*#\[[^\]]*\][^\n]*\n)*\s*async\s+fn\s+([a-z_][a-z0-9_]*)"
)

# ---------------------------------------------------------------------------
# Static metadata
# ---------------------------------------------------------------------------

# Maps test file names to the primary Redis commands they exercise.
FILE_TO_COMMANDS: dict[str, list[str]] = {
    "acl_tcl.rs": ["ACL"],
    "acl_regression.rs": ["ACL"],
    "acl_v2_regression.rs": ["ACL"],
    "auth_tcl.rs": ["AUTH"],
    "auth_regression.rs": ["AUTH"],
    "bitfield_tcl.rs": ["BITFIELD"],
    "bitops_tcl.rs": ["BITOP", "BITCOUNT", "BITPOS"],
    "bitops_regression.rs": ["BITOP", "BITCOUNT", "BITPOS"],
    "bloom_regression.rs": ["BF.ADD", "BF.EXISTS"],
    "client_eviction_tcl.rs": ["CLIENT"],
    "cluster_scripting_tcl.rs": ["EVAL", "EVALSHA"],
    "cluster_scripting_regression.rs": ["EVAL", "EVALSHA"],
    "cluster_sharded_pubsub_tcl.rs": ["SSUBSCRIBE", "SPUBLISH"],
    "cms_topk_regression.rs": ["CMS.INCRBY", "TOPK.ADD"],
    "dump_tcl.rs": ["DUMP", "RESTORE"],
    "dump_regression.rs": ["DUMP", "RESTORE"],
    "event_sourcing_regression.rs": ["XADD", "XREAD"],
    "expire_tcl.rs": ["EXPIRE", "TTL", "PEXPIRE", "PERSIST"],
    "expire_regression.rs": ["EXPIRE", "TTL"],
    "functions_tcl.rs": ["FUNCTION"],
    "functions_regression.rs": ["FUNCTION"],
    "geo_tcl.rs": ["GEOADD", "GEODIST", "GEOSEARCH"],
    "hash_tcl.rs": ["HSET", "HGET", "HDEL", "HGETALL"],
    "hash_field_expire_tcl.rs": ["HEXPIRE", "HTTL", "HPERSIST"],
    "hotkeys_tcl.rs": ["HOTKEYS"],
    "hyperloglog_tcl.rs": ["PFADD", "PFCOUNT", "PFMERGE"],
    "incr_tcl.rs": ["INCR", "DECR", "INCRBY", "DECRBY"],
    "info_command_tcl.rs": ["COMMAND"],
    "info_keysizes_tcl.rs": ["INFO"],
    "info_regression.rs": ["INFO"],
    "info_tcl.rs": ["INFO"],
    "maxmemory_tcl.rs": ["MAXMEMORY"],
    "introspection_tcl.rs": ["CONFIG", "CLIENT", "DEBUG"],
    "introspection2_tcl.rs": ["CONFIG", "CLIENT"],
    "keyspace_tcl.rs": ["SELECT", "DBSIZE", "KEYS"],
    "latency_monitor_tcl.rs": ["LATENCY"],
    "lazyfree_tcl.rs": ["UNLINK", "FLUSHDB"],
    "list_tcl.rs": ["LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE"],
    "maxmemory_regression.rs": ["MAXMEMORY"],
    "memefficiency_tcl.rs": ["MEMORY"],
    "multi_tcl.rs": ["MULTI", "EXEC", "DISCARD", "WATCH"],
    "networking_tcl.rs": ["CLIENT"],
    "other_tcl.rs": ["OBJECT", "TYPE"],
    "pause_tcl.rs": ["CLIENT PAUSE"],
    "protocol_tcl.rs": ["PING", "ECHO"],
    "pubsub_tcl.rs": ["SUBSCRIBE", "PUBLISH"],
    "pubsub_regression.rs": ["SUBSCRIBE", "PUBLISH"],
    "pubsubshard_tcl.rs": ["SSUBSCRIBE", "SPUBLISH"],
    "querybuf_tcl.rs": ["CLIENT"],
    "quit_regression.rs": ["QUIT"],
    "replybufsize_tcl.rs": ["CLIENT"],
    "scan_tcl.rs": ["SCAN", "SSCAN", "HSCAN", "ZSCAN"],
    "scripting_tcl.rs": ["EVAL", "EVALSHA", "SCRIPT"],
    "scripting_regression.rs": ["EVAL", "EVALSHA", "SCRIPT"],
    "set_tcl.rs": ["SADD", "SREM", "SMEMBERS", "SCARD"],
    "slowlog_tcl.rs": ["SLOWLOG"],
    "sort_tcl.rs": ["SORT"],
    "stream_tcl.rs": ["XADD", "XLEN", "XRANGE", "XREAD", "XTRIM"],
    "stream_cgroups_tcl.rs": ["XGROUP", "XREADGROUP", "XACK", "XCLAIM"],
    "stream_regression.rs": ["XADD", "XLEN", "XRANGE"],
    "string_tcl.rs": ["SET", "GET", "SETNX", "SETEX", "MSET", "MGET"],
    "tracking_tcl.rs": ["CLIENT TRACKING"],
    "violations_tcl.rs": ["ACL"],
    "wait_tcl.rs": ["WAIT"],
    "zset_tcl.rs": ["ZADD", "ZREM", "ZRANGE", "ZSCORE", "ZRANK"],
}

# Human-readable labels and descriptions for each exclusion category.
CATEGORY_META: dict[str, tuple[str, str]] = {
    "redis-specific": (
        "Redis-Specific Internals",
        "Tests that exercise Redis-internal implementation details (allocator, object encoding, event loop) that don't apply to FrogDB's architecture.",
    ),
    "intentional-incompatibility:observability": (
        "Observability Differences",
        "Tests for Redis-specific INFO fields, stats, or monitoring behavior that FrogDB reports differently or doesn't expose.",
    ),
    "intentional-incompatibility:encoding": (
        "Encoding Differences",
        "Tests that assert specific internal encoding types (ziplist, listpack, intset) which FrogDB does not use.",
    ),
    "intentional-incompatibility:config": (
        "Configuration Differences",
        "Tests relying on Redis-specific configuration mechanisms (redis.conf, ACL files, CONFIG REWRITE) that FrogDB replaces with TOML config.",
    ),
    "tested-elsewhere": (
        "Tested Elsewhere",
        "Functionality covered by other FrogDB tests (fuzz tests, integration tests, or different regression suites).",
    ),
    "intentional-incompatibility:replication": (
        "Replication Differences",
        "Tests for Redis replication internals (SLAVEOF, replica propagation) that differ in FrogDB's Raft-based replication.",
    ),
    "intentional-incompatibility:protocol": (
        "Protocol Differences",
        "Tests for RESP3/HELLO protocol negotiation or protocol-level behaviors that FrogDB handles differently.",
    ),
    "intentional-incompatibility:debug": (
        "DEBUG Command",
        "Tests requiring the Redis DEBUG command, which is not exposed in FrogDB.",
    ),
    "intentional-incompatibility:memory": (
        "Memory Management",
        "Tests for Redis-specific memory management (MEMORY USAGE internals, jemalloc stats, maxmemory policies).",
    ),
    "intentional-incompatibility:persistence": (
        "Persistence Differences",
        "Tests for RDB/AOF persistence mechanisms that FrogDB replaces with RocksDB-backed storage.",
    ),
    "intentional-incompatibility:cluster": (
        "Cluster Differences",
        "Tests for Redis Cluster gossip-protocol internals that differ in FrogDB's Raft-based clustering.",
    ),
    "intentional-incompatibility:single-db": (
        "Single Database",
        "Tests requiring SELECT/multi-DB support, which FrogDB intentionally limits to a single database.",
    ),
    "intentional-incompatibility:scripting": (
        "Scripting Differences",
        "Tests for Redis Lua 5.1 scripting quirks or redis.* API details that differ in FrogDB's Lua 5.4 environment.",
    ),
    "intentional-incompatibility:cli": (
        "CLI Differences",
        "Tests requiring the redis-cli binary or CLI-specific features.",
    ),
    "uncategorized": (
        "Uncategorized",
        "Exclusions documented in legacy format without a structured category.",
    ),
}

# ---------------------------------------------------------------------------
# Parsing functions (adapted from audit_tcl.py)
# ---------------------------------------------------------------------------


def extract_rust_tests(rs_path: Path) -> list[str]:
    """Extract `#[tokio::test] async fn ...` names from a Rust port file."""
    if not rs_path.exists():
        return []
    content = rs_path.read_text()
    return TEST_FN_RE.findall(content)


def extract_documented_exclusions(
    rs_path: Path,
) -> list[dict[str, str]]:
    """Parse `## Intentional exclusions` from a Rust file's doc-comment header.

    Returns a list of dicts with keys: test_name, category, reason.
    """
    if not rs_path.exists():
        return []

    lines: list[str] = []
    with open(rs_path) as f:
        for line in f:
            if not line.startswith("//!"):
                break
            lines.append(line.rstrip())

    exclusions: list[dict[str, str]] = []
    in_section = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if INTENTIONAL_HEADER_RE.search(line):
            in_section = True
            i += 1
            continue
        if not in_section:
            i += 1
            continue

        # Try new categorised format first
        m_new = INTENTIONAL_BULLET_NEW_RE.match(line)
        if m_new:
            name = m_new.group(1).strip()
            category = m_new.group(2).strip()
            prose = m_new.group(3).strip()
            i += 1
            while i < len(lines):
                cm = INTENTIONAL_CONT_RE.match(lines[i])
                if cm:
                    prose = prose + " " + cm.group(1)
                    i += 1
                else:
                    break
            exclusions.append({"test_name": name, "category": category, "reason": prose.strip()})
            continue

        # Fall back to legacy format
        m_old = INTENTIONAL_BULLET_RE.match(line)
        if m_old:
            name = m_old.group(1).strip()
            reason = m_old.group(2).strip()
            i += 1
            while i < len(lines):
                cm = INTENTIONAL_CONT_RE.match(lines[i])
                if cm:
                    reason = reason + " " + cm.group(1)
                    i += 1
                else:
                    break
            exclusions.append(
                {"test_name": name, "category": "uncategorized", "reason": reason.strip()}
            )
            continue

        i += 1

    return exclusions


def count_broken_tests(rs_path: Path) -> int:
    """Count #[ignore = "..."] tests in a Rust file."""
    if not rs_path.exists():
        return 0
    content = rs_path.read_text()
    return len(IGNORE_RE.findall(content))


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------


def generate(tests_dir: Path) -> dict:
    """Walk test files and produce the compat-exclusions data structure."""
    rs_files = sorted(tests_dir.glob("*.rs"))

    total_tests = 0
    total_exclusions = 0
    total_broken = 0
    category_counts: Counter[str] = Counter()
    suites: list[dict] = []
    # command -> {suites, total_tests, total_excluded, categories}
    command_data: dict[str, dict] = {}

    for rs_path in rs_files:
        fname = rs_path.name
        test_names = extract_rust_tests(rs_path)
        test_count = len(test_names)
        exclusions = extract_documented_exclusions(rs_path)
        exclusion_count = len(exclusions)

        # Skip files with no tests and no exclusions
        if test_count == 0 and exclusion_count == 0:
            continue

        total_tests += test_count
        broken = count_broken_tests(rs_path)
        total_broken += broken
        total_exclusions += exclusion_count

        for exc in exclusions:
            category_counts[exc["category"]] += 1

        commands = FILE_TO_COMMANDS.get(fname, [])

        # Only include suites with exclusions
        if exclusion_count > 0:
            suites.append(
                {
                    "file": fname,
                    "test_count": test_count,
                    "exclusion_count": exclusion_count,
                    "commands": commands,
                    "exclusions": exclusions,
                }
            )

        # Build command impact data
        for cmd in commands:
            if cmd not in command_data:
                command_data[cmd] = {
                    "suites": [],
                    "total_tests": 0,
                    "total_excluded": 0,
                    "categories": Counter(),
                }
            cd = command_data[cmd]
            cd["suites"].append(fname)
            cd["total_tests"] += test_count
            cd["total_excluded"] += exclusion_count
            for exc in exclusions:
                cd["categories"][exc["category"]] += 1

    # Build categories dict
    categories: dict[str, dict] = {}
    for cat, count in category_counts.most_common():
        meta = CATEGORY_META.get(cat, (cat.replace("-", " ").title(), ""))
        categories[cat] = {
            "label": meta[0],
            "description": meta[1],
            "count": count,
        }

    # Build command_impact (only commands with exclusions, sorted alpha)
    command_impact: dict[str, dict] = {}
    for cmd in sorted(command_data):
        cd = command_data[cmd]
        if cd["total_excluded"] > 0:
            command_impact[cmd] = {
                "suites": sorted(set(cd["suites"])),
                "total_tests": cd["total_tests"],
                "total_excluded": cd["total_excluded"],
                "categories": dict(cd["categories"]),
            }

    return {
        "_generated": "DO NOT EDIT \u2014 auto-generated by: just compat-gen",
        "summary": {
            "total_tests": total_tests,
            "total_exclusions": total_exclusions,
            "broken_tests": total_broken,
            "upstream_version": "Redis 8.6.0",
        },
        "categories": categories,
        "suites": suites,
        "command_impact": command_impact,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate compatibility exclusions data from regression test metadata."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output path for the generated JSON file",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit 1 if generated JSON differs from committed file",
    )
    args = parser.parse_args()

    data = generate(TESTS_DIR)
    generated_json = json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    if args.check:
        output_path: Path = args.output
        if not output_path.exists():
            print(
                f"Missing: {output_path}. Run 'just compat-gen' to generate.",
                file=sys.stderr,
            )
            return 1
        existing = output_path.read_text()
        if existing != generated_json:
            print(
                f"Differs: {output_path}. Run 'just compat-gen' to regenerate.",
                file=sys.stderr,
            )
            return 1
        print("compat-exclusions.json is up to date.")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(generated_json)
    print(f"Generated: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
