# Redis Compatibility TODO

Tracking compatibility issues found by running the Redis 7.2.4 test suite against FrogDB.
Run `just redis-compat` to reproduce. Last run: 2026-02-24.

## Test Run Summary

**Total suites:** 89 (from `$::all_tests` in test_helper.tcl)
**Skipped:** 9 (intentional incompatibilities + not-yet-implemented features)
**Run:** 80 suites individually with per-suite FrogDB server instances
**Results:** 64 passed, 16 failed, 0 timed out, 0 crashed
**Wall-clock time:** 2m 35s (60s timeout per suite)

---

## Full Suite Results

### Passed (64 suites)

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
| `unit/aofrw` | All tests skipped (external mode) |
| `unit/acl` | ACL working |
| `unit/acl-v2` | ACL v2 working |
| `unit/pubsub` | Fixed: RESP3 PING format; skips for CLIENT REPLY OFF, keyspace notif |
| `unit/pubsubshard` | |
| `unit/scan` | Fixed: hash-based cursors for SSCAN/HSCAN/ZSCAN |
| `unit/keyspace` | Fixed: COPY DB validation, glob backtracking |
| `unit/hyperloglog` | Fixed: arity (PFCOUNT/PFMERGE/PFDEBUG), TODENSE; skips for corruption/sparse |
| `unit/other` | Fixed: HELP commands, StringValue leading zeros; skips for SELECT/PIPELINING |
| `unit/maxmemory` | All tests skipped (external mode) |
| `unit/limits` | |
| `unit/obuf-limits` | |
| `unit/memefficiency` | |
| `unit/wait` | |
| `unit/tls` | All tests skipped (external mode) |
| `unit/oom-score-adj` | All tests skipped (external mode) |
| `unit/shutdown` | All tests skipped (external mode) |
| `unit/networking` | |
| `unit/client-eviction` | |
| `unit/violations` | |
| `unit/replybufsize` | |
| `unit/cluster/misc` | All tests skipped (external mode) |
| `unit/cluster/cli` | All tests skipped (external mode) |
| `unit/cluster/scripting` | All tests skipped (external mode) |
| `unit/cluster/hostnames` | All tests skipped (external mode) |
| `unit/cluster/human-announced-nodename` | All tests skipped (external mode) |
| `unit/cluster/multi-slot-operations` | All tests skipped (external mode) |
| `unit/cluster/slot-ownership` | All tests skipped (external mode) |
| `unit/cluster/links` | All tests skipped (external mode) |
| `unit/cluster/cluster-response-tls` | All tests skipped (external mode) |
| `integration/block-repl` | All tests skipped (external mode) |
| `integration/replication` | All tests skipped (external mode) |
| `integration/replication-2` | All tests skipped (external mode) |
| `integration/replication-3` | All tests skipped (external mode) |
| `integration/replication-4` | All tests skipped (external mode) |
| `integration/replication-psync` | All tests skipped (external mode) |
| `integration/replication-buffer` | All tests skipped (external mode) |
| `integration/shutdown` | All tests skipped (external mode) |
| `integration/aof` | All tests skipped (external mode) |
| `integration/aof-race` | All tests skipped (external mode) |
| `integration/aof-multi-part` | All tests skipped (external mode) |
| `integration/rdb` | All tests skipped (external mode) |
| `integration/corrupt-dump` | All tests skipped (external mode) |
| `integration/corrupt-dump-fuzzer` | All tests skipped (external mode) |
| `integration/convert-zipmap-hash-on-load` | All tests skipped (external mode) |
| `integration/convert-ziplist-hash-on-load` | All tests skipped (external mode) |
| `integration/convert-ziplist-zset-on-load` | All tests skipped (external mode) |
| `integration/logging` | All tests skipped (external mode) |
| `integration/psync2` | All tests skipped (external mode) |
| `integration/psync2-reg` | All tests skipped (external mode) |
| `integration/psync2-pingoff` | All tests skipped (external mode) |
| `integration/psync2-master-restart` | All tests skipped (external mode) |
| `integration/failover` | All tests skipped (external mode) |
| `integration/redis-benchmark` | All tests skipped (external mode) |
| `integration/dismiss-mem` | All tests skipped (external mode) |

### Failed (16 suites)

| Suite | Errors | Category |
|-------|--------|----------|
| `unit/sort` | 24 | Missing SORT features |
| `unit/expire` | 17 | Expiry edge cases |
| `unit/type/hash` | 18 | Hash encoding/config |
| `unit/bitops` | 12 | BITOP/BITCOUNT/BITPOS edge cases |
| `unit/type/set` | 11 | Set operations |
| `unit/geo` | 11 | GEO command edge cases |
| `unit/type/zset` | 9 | Sorted set operations |
| `unit/slowlog` | 7 | SLOWLOG entry limits, argument trimming, format |
| `unit/type/list` | 7 | Blocking list ops + client tracking |
| `unit/info-command` | 6 | COMMAND INFO metadata |
| `unit/pause` | 6 | CLIENT PAUSE behavior |
| `unit/multi` | 5 | MULTI/EXEC edge cases |
| `unit/lazyfree` | 5 | UNLINK / async deletion |
| `unit/querybuf` | 4 | Query buffer management |
| `unit/functions` | 1 | FUNCTION subsystem |
| `unit/latency-monitor` | 1 | LATENCY HISTORY |

### Skipped (9 suites)

#### Intentional incompatibilities (5 suites)

| Suite | Reason |
|-------|--------|
| `unit/introspection` | Single-DB model; SELECT not supported |
| `unit/introspection-2` | CONFIG REWRITE not supported |
| `unit/bitfield` | BITFIELD u64 intentionally limited to u63 |
| `unit/scripting` | Strict key validation (all keys must be in KEYS array) |
| `integration/redis-cli` | Requires redis-cli binary |

#### Not yet implemented (4 suites)

| Suite | Reason |
|-------|--------|
| `unit/type/stream` | Streams not implemented (5 errors) |
| `unit/type/stream-cgroups` | Consumer groups not implemented (3 errors) |
| `unit/tracking` | Client-side caching not implemented (1 error) |
| `unit/protocol` | Empty query (\r\n) handling causes decode error |

#### Previously skipped, now enabled (13 suites)

These were in `skiplist-not-implemented.txt` but pass in external server mode:

| Suite | Old Reason |
|-------|------------|
| `unit/cluster/*` (9 suites) | All tests skipped in external mode |
| `unit/maxmemory` | All tests skipped in external mode |
| `unit/oom-score-adj` | All tests skipped in external mode |
| `unit/shutdown` | All tests skipped in external mode |
| `unit/aofrw` | All tests skipped in external mode |

---

## Failure Analysis

### High Error Count Suites

#### `unit/sort` (24 errors)

SORT command has significant gaps. Many tests exercise SORT with BY, GET, STORE,
and ALPHA modifiers that likely have edge cases or missing features.

#### `unit/expire` (17 errors)

Expiry-related edge cases. Tests exercise:
- Subkey expiry (HEXPIRE, etc.) -- may not be implemented
- EXPIRETIME/PEXPIRETIME precision
- Expire propagation and notification behavior

#### `unit/type/hash` (18 errors)

Hash encoding transitions and CONFIG-driven behavior:
- Encoding type assertion failures (ziplist vs hashtable transitions)
- HRANDFIELD with RESP3, count overflow, count variants
- HINCRBYFLOAT edge cases (32-bit values, NaN/Infinity rejection)
- Hash ziplist encoding tests
- CONFIG aliases for `hash-max-ziplist-entries` / `hash-max-listpack-entries` added

#### `unit/type/set` (11 errors)

Set operation edge cases:
- SRANDMEMBER count overflow (skipped via flaky list -- causes shard panic)
- SDIFF fuzzing timeout (skipped via flaky list)
- SDIFF/SINTER/SDIFFSTORE/SINTERSTORE against non-set error handling
- SMOVE notification behavior
- Set encoding transitions

#### `unit/bitops` (12 errors)

BITOP, BITCOUNT, BITPOS edge cases. May include:
- BITOP with empty keys or mismatched lengths
- BITPOS with byte/bit range arguments
- BITCOUNT with non-existent keys

#### `unit/geo` (11 errors)

GEO command edge cases:
- GEOSEARCH with various shape/sort options
- GEODIST edge cases
- Likely encoding or precision issues

#### `unit/type/list` (7 errors)

Blocking list operation tests and edge cases:
- LPOS RANK i64::MIN out-of-range not detected (2 errors, both encodings)
- BLPOP timeout error message mismatch (1 error)
- RPOPLPUSH stub not implemented (1 exception, cascades to remaining errors)
- BLPOP/BRPOP/BLMOVE client state reporting

#### `unit/type/zset` (9 errors)

Sorted set operations:
- BZPOPMIN/BZPOPMAX blocking behavior
- ZRANGESTORE edge cases
- Encoding transitions (ziplist vs skiplist)

#### `unit/slowlog` (7 errors)

SLOWLOG implementation gaps:
- Entry count limiting (SLOWLOG GET with count)
- Argument trimming (long arguments should be truncated)
- CONFIG SET slowlog-log-slower-than / slowlog-max-len
- Entry format details

### Known Issues (from previous analysis)

#### Blocking Client Tracking

FrogDB does not update `blocked_clients` in CLIENT LIST / INFO, causing tests
that poll for blocked state to fail. Affects: `unit/type/list`, `unit/type/zset`,
`unit/pause`, `unit/multi`.

#### Error Message Mismatches

No outstanding error message mismatches.

---

## Action Items

### Medium Effort
- [ ] Fix COMMAND INFO metadata (fixes `unit/info-command` -- 6 errors)
- [ ] Investigate SORT edge cases (`unit/sort` -- 24 errors)
- [ ] Fix CLIENT PAUSE behavior (`unit/pause` -- 6 errors)
- [ ] Allow blocking commands inside MULTI/EXEC (execute immediately)
- [ ] Fix SLOWLOG implementation (`unit/slowlog` -- 7 errors)
- [ ] Fix LATENCY HISTOGRAM (`unit/latency-monitor` -- 1 error)

### Major Work
- [ ] Fix blocked client tracking (CLIENT LIST blocked state, blocked_clients count)
- [ ] Investigate and fix expire edge cases (`unit/expire` -- 17 errors)
- [ ] Investigate and fix BITOP/BITCOUNT/BITPOS edge cases (`unit/bitops` -- 12 errors)
- [ ] Investigate and fix GEO command edge cases (`unit/geo` -- 11 errors)
- [ ] Investigate UNLINK/async deletion (`unit/lazyfree` -- 5 errors)
- [ ] Investigate query buffer management (`unit/querybuf` -- 4 errors)
