# Redis Compatibility Status

Tracking compatibility issues found by running the Redis 7.2.4 test suite against FrogDB.
Last run: 2026-02-24.

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

### Failed

| Suite | Errors | Category |
|-------|--------|----------|
| `unit/sort` | 24 | Missing SORT features |
| `unit/expire` | 15 | Expiry edge cases |
| `unit/bitops` | 12 | BITOP/BITCOUNT/BITPOS edge cases |
| `unit/geo` | 11 | GEO command edge cases |
| `unit/type/zset` | 45 | Sorted set operations (cascading from ZSCORE/ZMSCORE format) |
| `unit/slowlog` | 14 | SLOWLOG entry limits, argument trimming, format |
| `unit/info-command` | 6 | COMMAND INFO metadata |
| `unit/pause` | 6 | CLIENT PAUSE behavior |
| `unit/multi` | 5 | MULTI/EXEC edge cases |
| `unit/lazyfree` | 5 | UNLINK / async deletion |
| `unit/querybuf` | 4 | Query buffer management |
| `unit/functions` | 1 | FUNCTION engine name case-sensitivity |
| `unit/latency-monitor` | 1 | CONFIG RESETSTAT not implemented |

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

ZSCORE and ZMSCORE return bulk string format instead of integer format for whole-number
scores, causing cascading Tcl evaluation failures in `unit/type/zset`.
