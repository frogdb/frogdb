# Redis 7.2.4 Compatibility Test Status

Last updated: 2026-02-23

## Summary

| Category | Count |
|----------|-------|
| Test files in suite | 95 |
| Intentionally skipped | 6 |
| Skipped (not yet implemented) | 32 |
| Executed | 57 |
| Passed (clean) | 1 |
| Passed (all tests ignored/N/A) | 30 |
| Partial pass (some failures) | 6 |
| Failed (exception at start) | 1 |
| Failed (cascading shard crash) | 10 |
| Individual tests passed | ~142 |
| Individual tests failed | 20 |
| Individual test exceptions | 16 |

## Critical Issue: Shard Crash Cascade

`HRANDFIELD` with a count that overflows `i64` (e.g. `HRANDFIELD key 10000000000`) causes the
shard to crash with "ERR shard dropped request". Once a shard crashes, all subsequent test files
that call `FLUSHALL` fail immediately with "ERR shard unavailable". This blocked **10 test files**
from executing.

**Affected files:** `unit/expire`, `unit/quit`, `unit/latency-monitor`, `unit/pubsub`,
`unit/bitops`, `unit/geo`, `unit/hyperloglog`, `unit/tls`, `unit/violations`, `unit/replybufsize`

## Test File Results

### Passed Clean

| Test File | Tests Passed | Notes |
|-----------|-------------|-------|
| `unit/type/incr` | 29 | 1 ignored (needs:debug) |

### Mostly Passed (minor failures)

| Test File | Passed | Failed | Exception | Root Cause |
|-----------|--------|--------|-----------|------------|
| `unit/keyspace` | 31 | 1 | 1 | COPY to non-integer DB; missing `zset-max-ziplist-entries` CONFIG |
| `unit/scan` | 16 | 2 | 0 | ZSCAN score precision; SSCAN iteration regression |
| `unit/type/string` | ~44 | 2 | 1 | MSET error case mismatch; SETRANGE empty key; SUBSTR not implemented |

### Partially Passed (significant failures)

| Test File | Passed | Failed | Exception | Root Cause |
|-----------|--------|--------|-----------|------------|
| `unit/type/list` | 15 | 11 | 1 | LPOS COUNT/RANK bugs; LPOP/RPOP arity; RESP3→RESP2 downgrade blocked |
| `unit/type/hash` | 5 | 2 | 1 | HRANDFIELD RESP3 format; HRANDFIELD count overflow crashes shard |
| `unit/info-command` | 0 | 2 | 1 | INFO section filtering issues; multi-section INFO not supported |

### Blocked by Missing CONFIG Parameters

| Test File | Missing Parameter |
|-----------|-------------------|
| `unit/type/set` | `set-max-listpack-value` |

### Blocked by Shard Crash (cascading)

All 10 files below failed with "ERR shard unavailable" on `FLUSHALL` due to the shard crash
triggered by `HRANDFIELD count overflow` in `unit/type/hash`:

`unit/expire`, `unit/quit`, `unit/latency-monitor`, `unit/pubsub`, `unit/bitops`,
`unit/geo`, `unit/hyperloglog`, `unit/tls`, `unit/violations`, `unit/replybufsize`

### All Tests Ignored (external:skip / not applicable)

These test files executed but all individual tests were skipped via tag filtering:

`unit/info`, `unit/acl-v2`, `unit/pubsubshard`, `unit/networking`, `unit/client-eviction`,
`integration/block-repl`, `integration/replication` (1-4, psync, buffer),
`integration/shutdown`, `integration/aof` (+ aof-race, aof-multi-part),
`integration/rdb`, `integration/corrupt-dump` (+ fuzzer),
`integration/convert-*` (3 files), `integration/logging`,
`integration/psync2` (+ reg, pingoff, master-restart),
`integration/failover`, `integration/redis-benchmark`, `integration/dismiss-mem`,
`unit/cluster/*` (8 files)

Exception: `integration/replication-buffer` had 2 passing tests.

## Intentionally Skipped Test Files

| Test File | Reason |
|-----------|--------|
| `unit/introspection` | Single-database model; SELECT not supported |
| `unit/introspection-2` | CONFIG REWRITE not supported |
| `unit/multi` | Single-database model affects transaction semantics |
| `unit/bitfield` | BITFIELD u64 not supported (limited to u63) |
| `unit/scripting` | FrogDB enforces strict key validation in Lua |
| `integration/redis-cli` | Requires redis-cli binary (not applicable) |

## Individual Test Skips

| Test Name | Reason |
|-----------|--------|
| `Commands pipelining` | FrogDB only supports RESP protocol, not inline commands |

## Failure Root Causes

### P0: Shard Crash on HRANDFIELD Count Overflow

`HRANDFIELD key <large_number>` causes shard panic when count overflows. Returns
"ERR shard dropped request" instead of "value is out of range". Cascades to crash
all subsequent test files.

### P1: Missing CONFIG Parameters

| Parameter | Default | Blocks |
|-----------|---------|--------|
| `set-max-listpack-value` | `"64"` | `unit/type/set` (entire file) |
| `zset-max-ziplist-entries` | `"128"` | COPY sorted set test in `unit/keyspace` |
| `zset-max-ziplist-value` | `"64"` | Likely needed for sorted set tests |

### P2: LPOS Command Bugs

- `LPOS ... COUNT 0` returns wrong results (should return all matches)
- `LPOS ... RANK <n>` error message doesn't match Redis format
- `LPOS ... MAXLEN` with negative RANK returns wrong order

### P3: RESP3 Issues

- `HRANDFIELD ... WITHVALUES` in RESP3 returns flat array instead of map (6 elements vs 3 pairs)
- `LPOP/RPOP key 0` in RESP3 returns `*0` instead of `_` (null vs empty array)
- RESP3→RESP2 protocol downgrade via `HELLO 2` is rejected

### P4: INFO Command Limitations

- `INFO` with no args returns `rejected_calls` in global output (test expects it only in `stats` section)
- `INFO <section>` returns empty string for `stats` section (test expects `rejected_calls` in it)
- `INFO cpu sentinel` (multiple sections) returns wrong-number-of-arguments error

### P5: Error Message Mismatches

- `MSET` arity error uses uppercase command name (`'MSET'` vs `'mset'`)
- `LPOP/RPOP key 1 1` should return arity error but returns empty

### P6: SUBSTR Not Implemented

`SUBSTR` (deprecated alias for `GETRANGE`) is not implemented, causing exception in
`unit/type/string`.

### P7: SETRANGE Empty Key Behavior

`SETRANGE` on a non-existing key with offset 0 and empty string should not create the key,
but FrogDB creates it.

### P8: ZSCAN Score Precision

`ZSCAN` returns `0` for very small float scores instead of preserving the original
string representation.

### P9: SSCAN Iteration Completeness

`SSCAN` with pattern matching may miss elements during full iteration (regression test #4906).

## Deferred Issues

- **PEXPIRETIME 1ms precision**: Inherent Instant/SystemTime conversion issue
- **Large integer expire overflow**: Complex bounds-checking across conversion functions
- **PUNSUBSCRIBE pattern cleanup**: Needs investigation of shard-side pattern handling
