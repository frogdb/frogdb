# Regression Test Batches 7-9 + Permanent Skips

Deferred batches from the ignored regression test reduction effort.
Starting point: ~168 ignored tests (after batches 4-6).

## Batch 7: Blocking + PAUSE + Stream Deep Fixes (L, ~31 tests)

Hardest batch — blocking atomicity and chained wake are architectural.

| Fix | Tests | File(s) |
|-----|-------|---------|
| CLIENT PAUSE write mode blocking | 7 | client/pause handler, `pause_tcl.rs` |
| CLIENT PAUSE + MULTI/EXEC, expires, syntax errors | 5 | pause handler, `pause_tcl.rs` |
| CLIENT PAUSE + UNBLOCK | 3 | pause handler, `pause_tcl.rs` |
| Stream deleted PEL entries as empty-field placeholders | 1 | `commands/src/stream/`, `stream_cgroups_tcl.rs` |
| Blocking XREADGROUP unblock on key delete/type change/group destroy | 3 | shard blocking, `stream_cgroups_tcl.rs` |
| XINFO STREAM FULL format | 1 | `commands/src/stream/`, `stream_cgroups_tcl.rs` |
| Blocking XREADGROUP empty array | 1 | stream read, `stream_cgroups_tcl.rs` |
| Blocking list atomicity (LPUSH+DEL, MULTI isolation) | 4 | `core/src/shard/blocking.rs`, `list_tcl.rs` |
| BLMOVE wake ordering, BRPOPLPUSH cascading/circular | 4 | shard blocking, `list_tcl.rs` |
| Blocking inside MULTI | 2 | shard blocking, `list_tcl.rs`, `multi_tcl.rs` |

**Dependencies:** After Batches 4-6. Blocking atomicity and chained wake are architectural challenges.

## Batch 8: ACL Formatting + FUNCTION Engine (L, ~32 tests)

| Fix | Tests | File(s) |
|-----|-------|---------|
| ACL command string formatting | 8 | `acl/src/`, `acl_tcl.rs` |
| ACL username validation, GENPASS, DRYRUN | 12 | `acl/src/`, `acl_tcl.rs` |
| ACL misc behavior | 2 | `acl_tcl.rs` |
| FUNCTION engine fixes (kill, globals, registration, timeout, version) | 10 | `scripting/src/`, `functions_tcl.rs` |

**Dependencies:** ACL DRYRUN needs solid ACL checker. FUNCTION fixes depend on scripting crate.

## Batch 9: ACL Selectors v2 + Lua Scripting Deep (XL, ~80 tests)

| Fix | Tests | File(s) |
|-----|-------|---------|
| ACL Selectors v2 | 20 | `acl/src/` (major new feature — ~1000+ new lines) |
| ACL v2 BITFIELD/COPY/SORT related | 8 | `acl/src/`, `acl_v2_tcl.rs` |
| Lua _G behavior, metatables, sandbox | ~3 | `core/src/scripting/lua_vm.rs` |
| Lua cjson/bit/cmsgpack libraries | ~19 | `core/src/scripting/` |
| Lua shebang flags (#!lua flags=no-writes) | ~8 | scripting handlers |
| Lua redis.set_repl, redis.replicate_commands | 2 | scripting bindings |
| Remaining Lua edge cases (error format, types, pcall) | ~20 | scripting crate |

**Dependencies:** Largest batch. ACL selectors is major new feature work. Lua fixes vary in difficulty.

## Permanent Skips (~8 tests)

These tests will remain ignored due to fundamental architectural differences.

| Test | File | Reason |
|------|------|--------|
| HLL corruption detection (4 tests) | `hyperloglog_tcl.rs` | FrogDB uses separate HLL type, not raw string bytes |
| HLL cache invalidation (1 test) | `hyperloglog_tcl.rs` | Same — opaque HLL type, no byte-level access |
| SCAN guarantees under write load (1 test) | `scan_tcl.rs` | Sharded architecture provides different iteration guarantees |
| KEYS very long nested patterns (1 test) | `keyspace_tcl.rs` | Performance regression test, architecture-dependent |
| SORT STORE in MULTI (1 test) | `multi_tcl.rs` | SORT STORE intentionally unsupported |
| FLUSHDB invalidation ordering (1 test) | `tracking_tcl.rs` | Sharded invalidation delivery order differs |

## Deferred from Batches 4-6

These items were too complex or risky for their respective batches:

| Item | Tests | Reason |
|------|-------|--------|
| LCS LEN precision (227 vs 222) | 1 | Algorithm correctness investigation needed |
| Protocol edge cases (empty query, negative/OOR multibulk, large args) | 5 | Deep parser changes in `protocol/src/` |
| Unauthenticated client size limits | 2 | Protocol-level limits pre-AUTH |
| Protected mode | 1 | New feature (non-loopback client restrictions) |
| CLIENT LIST `tot-cmds` for blocked commands | 1 | Requires blocked-client command tracking |

## Execution Order

```
Batch 7 (blocking/pause)  — can start after Batch 6
Batch 8 (ACL + FUNCTION)  — can parallel with Batch 7
Batch 9 (Selectors + Lua) — after Batch 8 (depends on ACL core)
```
