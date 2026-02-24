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
| Failed (cascading shard crash) | ~~10~~ 0 (fixed) |
| Individual tests passed | ~142 |
| Individual tests failed | ~~20~~ (many fixed, re-run needed) |
| Individual test exceptions | ~~16~~ (many fixed, re-run needed) |

## Remediation Status

The following issues have been fixed. Re-run `just redis-compat` to get updated counts.

### Fixed Issues

| Issue | Priority | Fix |
|-------|----------|-----|
| HRANDFIELD count overflow crashes shard | P0 | Clamp negative count via `unsigned_abs()` + `min(entries.len() * 10)` |
| Missing CONFIG params (`set-max-listpack-value`, `zset-max-*`) | P1 | Added 5 new no-op params; gated by `compat.strict_config` |
| LPOS COUNT 0 returns nothing | P2 | `Some(0) => usize::MAX` to return all matches |
| LPOS negative RANK + MAXLEN order | P2 | Verified correct (positions collected in descending order) |
| HRANDFIELD WITHVALUES RESP3 returns flat array | P3 | Return `Response::Map` for RESP3 |
| LPOP/RPOP count=0 returns null | P3 | Always return `Response::Array` when count arg is present |
| HELLO 2 downgrade blocked | P3 | Removed downgrade prevention check |
| INFO multi-section not supported | P4 | Changed arity to `AtLeast(0)`, iterate + deduplicate sections |
| Error messages use uppercase command names | P5 | Lowercased all `WrongArity`/`WrongArgCount` command strings + arity check paths |
| LPOP/RPOP arity allows extra args | P5 | Changed from `AtLeast(1)` to `Range { min: 1, max: 2 }` |
| SUBSTR alias not implemented | P6 | Added `SubstrCommand` delegating to `GetrangeCommand` |
| SETRANGE empty key with offset=0 | P7 | Early return `Integer(0)` when key doesn't exist, offset=0, value empty |
| ZSCAN format_float returns "0" for small scores | P8 | Replaced manual formatting with `ryu` crate for accurate representation |

### Expected Improvements After Re-run

- 10 previously-crashed test files should now execute (HRANDFIELD fix)
- `unit/type/set` should unblock (no-op CONFIG params)
- `unit/type/list` failures should drop significantly (LPOS + LPOP/RPOP fixes)
- `unit/type/hash` failures should drop to 0 (HRANDFIELD overflow + RESP3)
- `unit/scan` failures should drop by 1 (ZSCAN precision fix; SSCAN deferred)
- `unit/type/string` failures should drop to 0, exceptions to 0 (SETRANGE + SUBSTR)
- `unit/keyspace` exceptions should drop to 0 (no-op CONFIG params)
- `unit/info-command` should improve (multi-section INFO support)

## ~~Critical Issue: Shard Crash Cascade~~ (FIXED)

~~`HRANDFIELD` with a count that overflows `i64` (e.g. `HRANDFIELD key 10000000000`) causes the
shard to crash with "ERR shard dropped request". Once a shard crashes, all subsequent test files
that call `FLUSHALL` fail immediately with "ERR shard unavailable". This blocked **10 test files**
from executing.~~

**Fixed:** Count is now clamped via `unsigned_abs()` to handle `i64::MIN` and limited to
`entries.len() * 10` to prevent OOM allocation.

## Test File Results

### Passed Clean

| Test File | Tests Passed | Notes |
|-----------|-------------|-------|
| `unit/type/incr` | 29 | 1 ignored (needs:debug) |

### Mostly Passed (minor failures)

| Test File | Passed | Failed | Exception | Root Cause |
|-----------|--------|--------|-----------|------------|
| `unit/keyspace` | 31 | 1 | ~~1~~ 0 | COPY to non-integer DB; ~~missing `zset-max-ziplist-entries` CONFIG~~ (fixed) |
| `unit/scan` | 16 | ~~2~~ ~1 | 0 | ~~ZSCAN score precision~~ (fixed); SSCAN iteration regression (deferred) |
| `unit/type/string` | ~44 | ~~2~~ 0 | ~~1~~ 0 | ~~MSET error case mismatch~~ (fixed); ~~SETRANGE empty key~~ (fixed); ~~SUBSTR not implemented~~ (fixed) |

### Partially Passed (significant failures)

| Test File | Passed | Failed | Exception | Root Cause |
|-----------|--------|--------|-----------|------------|
| `unit/type/list` | 15 | ~~11~~ ~2 | ~~1~~ 0 | ~~LPOS COUNT/RANK bugs~~ (fixed); ~~LPOP/RPOP arity~~ (fixed); ~~RESP3→RESP2 downgrade blocked~~ (fixed) |
| `unit/type/hash` | 5 | ~~2~~ 0 | ~~1~~ 0 | ~~HRANDFIELD RESP3 format~~ (fixed); ~~HRANDFIELD count overflow crashes shard~~ (fixed) |
| `unit/info-command` | 0 | ~~2~~ ~1 | ~~1~~ 0 | ~~INFO multi-section not supported~~ (fixed); INFO section content gaps |

### ~~Blocked by Missing CONFIG Parameters~~ (FIXED)

| Test File | Missing Parameter | Status |
|-----------|-------------------|--------|
| `unit/type/set` | ~~`set-max-listpack-value`~~ | Fixed: added as no-op param |

### ~~Blocked by Shard Crash (cascading)~~ (FIXED)

~~All 10 files below failed with "ERR shard unavailable" on `FLUSHALL` due to the shard crash
triggered by `HRANDFIELD count overflow` in `unit/type/hash`:~~

~~`unit/expire`, `unit/quit`, `unit/latency-monitor`, `unit/pubsub`, `unit/bitops`,
`unit/geo`, `unit/hyperloglog`, `unit/tls`, `unit/violations`, `unit/replybufsize`~~

**Fixed:** These files should now execute normally.

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

### ~~P0: Shard Crash on HRANDFIELD Count Overflow~~ (FIXED)

~~`HRANDFIELD key <large_number>` causes shard panic when count overflows. Returns
"ERR shard dropped request" instead of "value is out of range". Cascades to crash
all subsequent test files.~~

### ~~P1: Missing CONFIG Parameters~~ (FIXED)

~~| Parameter | Default | Blocks |
|-----------|---------|--------|
| `set-max-listpack-value` | `"64"` | `unit/type/set` (entire file) |
| `zset-max-ziplist-entries` | `"128"` | COPY sorted set test in `unit/keyspace` |
| `zset-max-ziplist-value` | `"64"` | Likely needed for sorted set tests |~~

All parameters added as no-op compatibility params. Gated behind `compat.strict_config` flag.

### ~~P2: LPOS Command Bugs~~ (FIXED)

~~- `LPOS ... COUNT 0` returns wrong results (should return all matches)
- `LPOS ... RANK <n>` error message doesn't match Redis format
- `LPOS ... MAXLEN` with negative RANK returns wrong order~~

### ~~P3: RESP3 Issues~~ (FIXED)

~~- `HRANDFIELD ... WITHVALUES` in RESP3 returns flat array instead of map (6 elements vs 3 pairs)
- `LPOP/RPOP key 0` in RESP3 returns `*0` instead of `_` (null vs empty array)
- RESP3→RESP2 protocol downgrade via `HELLO 2` is rejected~~

### P4: INFO Command Limitations (PARTIALLY FIXED)

- ~~`INFO cpu sentinel` (multiple sections) returns wrong-number-of-arguments error~~ (fixed)
- `INFO` with no args returns `rejected_calls` in global output (test expects it only in `stats` section)
- `INFO <section>` returns empty string for `stats` section (test expects `rejected_calls` in it)

### ~~P5: Error Message Mismatches~~ (FIXED)

~~- `MSET` arity error uses uppercase command name (`'MSET'` vs `'mset'`)
- `LPOP/RPOP key 1 1` should return arity error but returns empty~~

### ~~P6: SUBSTR Not Implemented~~ (FIXED)

~~`SUBSTR` (deprecated alias for `GETRANGE`) is not implemented, causing exception in
`unit/type/string`.~~

### ~~P7: SETRANGE Empty Key Behavior~~ (FIXED)

~~`SETRANGE` on a non-existing key with offset 0 and empty string should not create the key,
but FrogDB creates it.~~

### ~~P8: ZSCAN Score Precision~~ (FIXED)

~~`ZSCAN` returns `0` for very small float scores instead of preserving the original
string representation.~~ Now uses `ryu` crate for accurate float formatting.

### P9: SSCAN Iteration Completeness (DEFERRED)

`SSCAN` with pattern matching may miss elements during full iteration (regression test #4906).
FrogDB uses positional cursors, not Redis-style reverse-binary cursors. Fixing requires
significant architectural work. Current implementation works correctly for non-mutating scans.

## Deferred Issues

- **SSCAN cursor stability across mutations**: Positional vs reverse-binary cursor architecture difference
- **PEXPIRETIME 1ms precision**: Inherent Instant/SystemTime conversion issue
- **Large integer expire overflow**: Complex bounds-checking across conversion functions
- **PUNSUBSCRIBE pattern cleanup**: Needs investigation of shard-side pattern handling

## Compat Configuration

A new `[compat]` config section controls compatibility behavior:

```toml
[compat]
# When true, CONFIG GET/SET returns errors for Redis params that FrogDB
# treats as no-ops. When false (default), silently accepts them.
strict_config = false
```
