# Redis 7.2.4 Compatibility Test Status

This document tracks FrogDB's compatibility with the Redis 7.2.4 test suite.

## Summary

| Category | Count |
|----------|-------|
| Test files total | 58 |
| Intentionally skipped | 5 |
| Passed | — |
| Failed | — |
| Not yet run | — |

> Results will be updated after a full `just redis-compat` run.

## Intentionally Skipped Tests

These tests are skipped because FrogDB intentionally diverges from Redis:

| Test File | Reason |
|-----------|--------|
| `unit/introspection` | FrogDB uses single-database model; SELECT not supported |
| `unit/introspection-2` | CONFIG REWRITE not supported |
| `unit/multi` | Single-database model affects transaction semantics |
| `unit/bitfield` | BITFIELD u64 not supported (limited to u63) |
| `unit/scripting` | FrogDB enforces strict key validation in Lua |

## Skiplist: Individual Tests

| Test Name | Reason |
|-----------|--------|
| `integration/redis-cli` | Requires redis-cli binary (not applicable) |
| `Commands pipelining` | FrogDB only supports RESP protocol, not inline commands |

## Known Failure Categories

### 1. CONFIG SET Unknown Parameters (4 test files blocked)

Tests abort early because `CONFIG SET` fails on Redis encoding-threshold parameters
that FrogDB doesn't recognize. Fixed by adding no-op CONFIG parameters.

**Affected files:** `unit/type/list`, `unit/type/set`, `unit/type/hash`, `unit/latency-monitor`

### 2. INFO Command Errors

`INFO <unknown-section>` returns an error instead of an empty string.
`INFO stats` missing `rejected_calls` field.

**Affected file:** `unit/info-command`

### 3. Error Message Case Mismatch

FrogDB uses uppercase command names in arity error messages; Redis uses lowercase.

### 4. GETSET Not Implemented

Returns `NotImplemented` error. GETSET is deprecated but still tested.

### 5. DECRBY Overflow on i64::MIN

`DECRBY key -9223372036854775808` wraps instead of returning an error due to
negation overflow of `i64::MIN`.

### 6. INCRBYFLOAT NaN/Infinity Error Message

Returns "value is not a valid float" but Redis returns
"increment would produce NaN or Infinity".

### 7. SCAN Pattern Filtering Not Applied

SSCAN/HSCAN parse the MATCH option but don't filter results.

### 8. Expire/TTL Edge Cases

- PEXPIREAT/EXPIREAT don't accept negative timestamps (should treat as already-expired)
- TTL truncates instead of using ceiling division
- SETEX/PSETEX don't reject negative/zero timeouts correctly

### 9. PubSub PING Response Format

PING in subscribe mode returns simple `PONG` instead of the required
array format `["pong", ""]`.

## Deferred Issues

These are known incompatibilities that require deeper changes:

- **PEXPIRETIME 1ms precision**: Inherent Instant/SystemTime conversion issue
- **Large integer expire overflow**: Complex bounds-checking across conversion functions
- **PUNSUBSCRIBE pattern cleanup**: Needs investigation of shard-side pattern handling
