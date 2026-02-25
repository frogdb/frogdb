# Redis Compatibility Status

Tracking compatibility issues found by running the Redis 7.2.4 test suite against FrogDB.
Last run: 2026-02-25.

> **Failure details:** See [`failures/`](failures/) for per-suite analysis and fix recommendations.

## Running Tests

```bash
# Run all suites
just redis-compat

# Run a single suite
just redis-compat --single unit/type/zset

# Verbose output
just redis-compat --verbose

# Skip rebuilding FrogDB (reuse existing binary)
just redis-compat --skip-build

# Show pass/fail coverage summary
just redis-compat-coverage

# Clean cached Redis source and test data
just redis-compat-clean
```

See [`README.md`](README.md) for full options, prerequisites, and troubleshooting.

## Updating This Document

When a failing suite is fixed (0 errors):
1. Move its row from **Failed** to **Passed** (add a "Fixed: ..." note)
2. Delete its file from `failures/`
3. Re-run `just redis-compat` to confirm

---

## Full Suite Results

### Passed

| Suite | Notes |
|-------|-------|
| `unit/printver` | |
| `unit/dump` | DUMP/RESTORE working |
| `unit/auth` | AUTH working |
| `unit/info` | |
| `unit/type/incr` | |
| `unit/type/list-2` | |
| `unit/type/list-3` | |
| `unit/type/string` | Fixed: SET GET NX/WRONGTYPE, SUBSTR alias |
| `unit/quit` | |
| `unit/acl` | ACL working |
| `unit/acl-v2` | ACL v2 working |
| `unit/pubsub` | Fixed: RESP3 PING format; skips for CLIENT REPLY OFF, keyspace notif |
| `unit/pubsubshard` | |
| `unit/scan` | Fixed: hash-based cursors for SSCAN/HSCAN/ZSCAN |
| `unit/keyspace` | Fixed: COPY DB validation, glob backtracking |
| `unit/hyperloglog` | Fixed: arity (PFCOUNT/PFMERGE/PFDEBUG), TODENSE; skips for corruption/sparse |
| `unit/other` | Fixed: HELP commands, StringValue leading zeros; skips for SELECT/PIPELINING |
| `unit/limits` | |
| `unit/obuf-limits` | |
| `unit/memefficiency` | |
| `unit/wait` | |
| `unit/networking` | |
| `unit/client-eviction` | |
| `unit/violations` | |
| `unit/replybufsize` | |
| `unit/type/hash` | |
| `unit/type/set` | |
| `unit/type/list` | |
| `unit/type/stream` | |
| `unit/type/stream-cgroups` | |
| `unit/tracking` | |
| `unit/protocol` | |
| `unit/info-command` | Fixed: COMMAND INFO metadata |
| `unit/bitops` | Fixed: BITCOUNT range normalization, SETBIT/BITFIELD dirty tracking |

### Failed

| Suite | Errors | Category |
|-------|--------|----------|
| `unit/sort` | 8 | SORT edge cases |
| `unit/expire` | 22 | Expiry edge cases |
| `unit/geo` | 21 | GEO command edge cases |
| `unit/type/zset` | 41 | Sorted set operations |
| `unit/pause` | 6 | CLIENT PAUSE behavior |
| `unit/multi` | 1 | MULTI/EXEC edge cases |
| `unit/functions` | 17 | FCALL error messages, FUNCTION DUMP/RESTORE |

### External Mode

All tests in these suites are skipped when running against an external server.

| Suite |
|-------|
| `unit/aofrw` |
| `unit/maxmemory` |
| `unit/tls` |
| `unit/oom-score-adj` |
| `unit/shutdown` |
| `unit/cluster/misc` |
| `unit/cluster/cli` |
| `unit/cluster/scripting` |
| `unit/cluster/hostnames` |
| `unit/cluster/human-announced-nodename` |
| `unit/cluster/multi-slot-operations` |
| `unit/cluster/slot-ownership` |
| `unit/cluster/links` |
| `unit/cluster/cluster-response-tls` |
| `integration/block-repl` |
| `integration/replication` |
| `integration/replication-2` |
| `integration/replication-3` |
| `integration/replication-4` |
| `integration/replication-psync` |
| `integration/replication-buffer` |
| `integration/shutdown` |
| `integration/aof` |
| `integration/aof-race` |
| `integration/aof-multi-part` |
| `integration/rdb` |
| `integration/corrupt-dump` |
| `integration/corrupt-dump-fuzzer` |
| `integration/convert-zipmap-hash-on-load` |
| `integration/convert-ziplist-hash-on-load` |
| `integration/convert-ziplist-zset-on-load` |
| `integration/logging` |
| `integration/psync2` |
| `integration/psync2-reg` |
| `integration/psync2-pingoff` |
| `integration/psync2-master-restart` |
| `integration/failover` |
| `integration/redis-benchmark` |
| `integration/dismiss-mem` |

### Skipped

#### Intentional incompatibilities

| Suite | Reason |
|-------|--------|
| `unit/introspection` | Single-DB model; SELECT not supported |
| `unit/introspection-2` | CONFIG REWRITE not supported |
| `unit/bitfield` | BITFIELD u64 intentionally limited to u63 |
| `unit/scripting` | Strict key validation (all keys must be in KEYS array) |
| `unit/debug` | DEBUG commands not supported (not planned) |
| `unit/monitor` | MONITOR command not supported (not planned) |
| `unit/moduleapi` | Module API not supported (not planned) |
| `unit/slowlog` | SLOWLOG output formatting (introspection/metadata) |
| `unit/querybuf` | CLIENT LIST qbuf fields (introspection/metadata) |
| `unit/lazyfree` | INFO memory used_memory, CONFIG RESETSTAT (introspection/metadata) |
| `unit/latency-monitor` | LATENCY HISTOGRAM output (introspection/metadata) |
| `integration/redis-cli` | Requires redis-cli binary |

---

### Known Cross-Suite Issues

#### Blocking Client Tracking

FrogDB does not update `blocked_clients` in CLIENT LIST / INFO, causing tests
that poll for blocked state to fail. Affects: `unit/type/zset`, `unit/pause`,
`unit/multi`, `unit/slowlog`.

#### CONFIG RESETSTAT Not Implemented

CONFIG RESETSTAT returns OK but is a no-op, causing cascading failures in:
`unit/lazyfree`, `unit/latency-monitor`.

#### ZSCORE/ZMSCORE Response Format

Fixed: ZSCORE and ZMSCORE now return bulk string format consistently (matching Redis).
Remaining `unit/type/zset` failures are from ZMPOP/ZPOPMIN response format edge cases
and ZUNIONSTORE/ZINTERSTORE with mixed set types.
