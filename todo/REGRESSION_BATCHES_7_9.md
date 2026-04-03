# Regression Test Batches 7-9 + Permanent Skips

Deferred batches from the ignored regression test reduction effort.
Starting point: ~168 ignored tests (after batches 4-6).
Current: ~109 ignored tests (after batch 8 + selector removal).

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

## Batch 8: ACL Formatting + FUNCTION Engine — DONE (31 of 32 tests)

Completed 2026-04-03. Commits: `add6f7ee`, `07ebe313`.

| Fix | Tests | Status |
|-----|-------|--------|
| ACL GETUSER command string formatting (ordered rule_log) | 8 | ✅ Done |
| ACL username validation, multi-pipe rejection, unknown subcommand validation | 5 | ✅ Done |
| ACL GENPASS bit-length fix + bounds validation | 2 | ✅ Done |
| ACL DRYRUN subcommand | 2 | ✅ Done |
| ACL misc (DELUSER count, default-user-off, requirepass, SELECT subcommand) | 4 | ✅ Done |
| ACL PUBLISH in MULTI channel permission check | 1 | ⏳ Needs PUBLISH in command registry |
| FUNCTION registration validation (too-many-args, bad description, unknown keys) | 3 | ✅ Done |
| FUNCTION KILL error format | 1 | ✅ Done |
| FUNCTION LOAD timeout (instruction hook in sandbox) | 1 | ✅ Done |
| FUNCTION redis.REDIS_VERSION + bit library | 1 | ✅ Done |
| FUNCTION no-writes flag enforcement on FCALL | 1 | ✅ Done |
| FUNCTION sandbox security (phase-split execution, redis table protection) | 3 | ✅ Done |

**Key changes:**
- `CommandPermissions` now has `rule_log: Vec<AclCommandRule>` for ordered serialization
- PING is no longer auth-exempt (matches Redis behavior)
- `execute_function` split into load/lock/invoke phases for sandbox security
- `bit` compatibility library added for Lua 5.4 (Redis uses LuaJIT bit ops)

## Batch 9: Lua Scripting Deep (XL, ~52 tests)

| Fix | Tests | File(s) |
|-----|-------|---------|
| Lua _G behavior, metatables, sandbox | ~3 | `core/src/scripting/lua_vm.rs` |
| Lua cjson/cmsgpack libraries (bit done in Batch 8) | ~19 | `core/src/scripting/` |
| Lua shebang flags (#!lua flags=no-writes) | ~8 | scripting handlers |
| Lua redis.set_repl, redis.replicate_commands | 2 | scripting bindings |
| Remaining Lua edge cases (error format, types, pcall) | ~20 | scripting crate |

**Dependencies:** Lua fixes vary in difficulty.

## Permanently Not Implemented

### ACL Selectors v2 (28 tests removed)

ACL selectors (`(...)` syntax, `clearselectors`) are intentionally not supported.
Research shows near-zero real-world adoption — Dragonfly and KeyDB also ship without
this feature. Use separate users instead of selectors to grant different permission sets.

Removed 2026-04-03. The `acl_v2_tcl.rs` test file was deleted; 5 non-selector tests
(R/W permission validation, DRYRUN basics) were moved to `acl_tcl.rs`.

### Architectural Differences (~8 tests)

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
Batch 8 (ACL + FUNCTION)  — DONE (31/32 tests, 2026-04-03)
Batch 9 (Lua scripting)   — ready to start
```
