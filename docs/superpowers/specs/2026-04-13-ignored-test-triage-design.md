# Ignored Test Triage: Fixing 120 Disabled Redis Compatibility Tests

**Date:** 2026-04-13
**Status:** Draft

## Context

FrogDB has 120 `#[ignore]` tests in `frogdb-server/crates/redis-regression/tests/*_tcl.rs`. These
are ported Redis 8.6.0 TCL tests that exercise behavior FrogDB doesn't yet match. The
COMPATIBILITY.md audit (Phase 1-5) documented 648 intentional exclusions but left these 120 as
"broken" — tests that exist but are disabled and should eventually be fixed.

This spec triages all 120 into fix/skip buckets and defines 7 root-cause-grouped work items to
address them.

## Triage Decision

| Decision | Count | Action |
|----------|------:|--------|
| **Fix** | 115 | Implement across 7 work items |
| **Reclassify** | 5 | HLL corruption tests → `intentional-incompatibility:encoding` |

### Reclassified (5 tests → skip)

These 5 HyperLogLog tests in `hyperloglog_tcl.rs` manipulate HLL binary internals via
SETRANGE/APPEND/GETRANGE. FrogDB's HLL encoding may differ from Redis internals, and no real client
manipulates HLL bytes directly. Reclassify to `intentional-incompatibility:encoding`:

- `tcl_pfadd_pfcount_cache_invalidation_works` — relies on GETRANGE of internal HLL binary
- `tcl_corrupted_sparse_hll_detected_additional_at_tail` — relies on APPEND to corrupt HLL
- `tcl_corrupted_sparse_hll_detected_broken_magic` — relies on SETRANGE to corrupt HLL
- `tcl_corrupted_sparse_hll_detected_invalid_encoding` — relies on SETRANGE to corrupt HLL
- `tcl_corrupted_dense_hll_detected_wrong_length` — relies on SETRANGE to corrupt HLL

---

## Work Items

Ordered by root-cause impact (tests unlocked per effort). Each work item is a coherent subsystem
fix.

### WI-1: Lua VM Completeness (52 tests)

**File:** `frogdb-server/crates/core/src/scripting/lua_vm.rs`
**Goal:** Make EVAL/EVALSHA fully Redis-compatible.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Add cjson library | ~8 | Register `cjson.encode`/`cjson.decode`/`cjson.decode_array_with_array_mt` |
| Add cmsgpack library | ~2 | Register `cmsgpack.pack`/`cmsgpack.unpack` |
| Fix pcall/error handling | ~6 | Error table format, WRONGTYPE propagation, status/error reply APIs |
| Shebang flag parsing | ~4 | Parse `#!lua flags=no-writes,no-cluster` in EVAL scripts |
| Global protection edge cases | ~8 | Fix `_G` access, metatable readonly, undeclared variable error format |
| Blocking commands in scripts | ~7 | Return nil immediately (Redis behavior) instead of erroring |
| Helper functions | ~3 | `redis.sha1hex`, `redis.set_repl`, `redis.replicate_commands` |
| Type conversion edge cases | ~6 | Float-to-int truncation, table unpack, massive argument unpack |
| SCRIPT FLUSH | ~1 | Clear the SHA cache |
| Remaining edge cases | ~7 | Arity validation, bit operations, CLUSTER RESET denial |

### WI-2: Transaction Post-Execution Refactor (~12 tests unlocked)

**File:** `frogdb-server/crates/core/src/shard/pipeline.rs`
**Goal:** Defer waiter satisfaction until after ALL commands in MULTI/EXEC complete.

Currently `run_transaction_post_execution()` calls `satisfy_waiters_for_command()` per-command in a
loop. Waiters see intermediate transaction state.

**Fix:** Collect all written keys across all commands, then call waiter satisfaction once with the
final state.

Unlocks:
- `tcl_blpop_lpush_del_should_not_awake_blocked_client` (list)
- `tcl_blpop_lpush_del_set_should_not_awake_blocked_client` (list)
- `tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop` (list)
- `tcl_pause_starts_at_end_of_transaction` (pause)
- Several CLIENT PAUSE + MULTI interaction tests

### WI-3: Command Registry for MULTI (7 tests)

**Files:** Command registry, `router.rs`
**Goal:** Register SPUBLISH and PUBLISH as queueable commands in MULTI.

Currently metadata-only registrations that don't support MULTI queueing.

Unlocks:
- 4 sharded pubsub MULTI tests (`cluster_sharded_pubsub_tcl.rs`)
- 1 ACL PUBLISH-in-MULTI test (`acl_tcl.rs`)
- 2 related cross-slot tests

### WI-4: CLIENT PAUSE Write-Blocking (15 tests)

**File:** `frogdb-server/crates/core/src/client_registry/mod.rs`, `lifecycle.rs`
**Goal:** Complete the CLIENT PAUSE implementation.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Write-command detection | ~4 | PAUSE WRITE must actually block SET, PFCOUNT, PUBLISH, etc. |
| Timeout preservation | 1 | Longer timeout wins when re-pausing |
| Script interaction | ~3 | EVAL blocked by WRITE, EVAL_RO passes, shebang `flags=no-writes` |
| Syntax error bypass | 1 | Wrong-arity errors get immediate response during pause |
| Expire suppression | 1 | Skip active+passive expires during PAUSE WRITE |
| CLIENT UNBLOCK rejection | 1 | UNBLOCK must fail for pause-blocked clients |
| RANDOMKEY handling | 1 | Avoid infinite loop with expiring keys during PAUSE WRITE |
| Multiple client unblock | 1 | All queued clients unblock when pause ends |
| Scripts in MULTI | 2 | Write scripts in MULTI blocked, RO scripts pass |

### WI-5: Blocking List Chaining & Edge Cases (~7 tests after WI-2)

**File:** `frogdb-server/crates/core/src/shard/blocking.rs`
**Goal:** Fix cascading wake, wrong-type handling, and ordering.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Chained wakes | 4 | BLMOVE/BRPOPLPUSH destination triggers next blocker's wake |
| Wrong-type destination | 2 | Validate dest type before pushing in BLMove handler |
| Multi-client wake ordering | 1 | FIFO ordering for multiple clients blocked on same key |

Note: blocking-in-MULTI test (`tcl_brpoplpush_inside_a_transaction`) is partially addressed by WI-2.

### WI-6: Stream XREADGROUP Unblock (8 tests)

**Files:** Stream command handlers, shard blocking infrastructure
**Goal:** Unblock XREADGROUP waiters on key/group lifecycle events.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| XGROUP DESTROY unblock | 1 | Unblock with NOGROUP error |
| Key deletion unblock | 1 | Unblock on DEL/UNLINK with error |
| Key type change unblock | 1 | Unblock with WRONGTYPE |
| Deleted entry representation | 1 | Return `[id, []]` for deleted entries in PEL history |
| Blocking timeout behavior | 1 | Return nil on timeout, not empty array |
| XINFO STREAM FULL format | 1 | Match Redis response format |
| Flaky timing tests | 2 | Increase sleep or use retry/polling loops |

### WI-7: Protocol Edge Cases & Misc (10 tests)

**Files:** RESP codec, various command handlers
**Goal:** Graceful error handling for malformed RESP + remaining one-offs.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Empty query handling | 1 | Ignore `\r\n` alone, process next command |
| Negative multibulk | 1 | Ignore and continue |
| Out-of-range multibulk/bulk | 2 | Send `-ERR` instead of closing connection |
| Large argument count | 1 | Handle 10k+ args gracefully |
| KEYS nested pattern | 1 | Fix glob matching for deeply nested patterns |
| SCAN guarantees | 1 | Iteration guarantees under concurrent writes |
| CLIENT LIST tot-cmds | 1 | Track tot-cmds for blocked commands |
| FLUSHDB tracking invalidation | 1 | Fix invalidation ordering during FLUSHDB |
| TRACKING REDIRECT | 1 | Fix RESP3 tracking redirection behavior |

---

## Verification

After each work item:
1. Remove `#[ignore]` from the targeted tests
2. Run `just test frogdb-redis-regression <test_name>` for each un-ignored test
3. Run `just test frogdb-redis-regression` for the full regression suite (no regressions)
4. For reclassified tests (WI-0): move from `#[ignore]` to `## Intentional exclusions` with
   `intentional-incompatibility:encoding` category

After all work items:
1. Run the audit script to verify gap count drops to 0 unclassified gaps
2. Update `todo/COMPATIBILITY.md` broken count from 120 to 0
3. Verify the `broken` row in the exclusion categories table is removed or zeroed

## Dependencies & Ordering

```
WI-1 (Lua)          ─── independent, can start immediately
WI-2 (Tx refactor)  ─── blocks WI-4 MULTI tests and WI-5 atomicity tests
WI-3 (Cmd registry) ─── independent, quick win
WI-4 (CLIENT PAUSE) ─── MULTI interaction tests (pause_starts_at_end_of_transaction,
                        write_scripts_in_multi_exec) need WI-2; rest independent
WI-5 (Blocking lists)── atomicity tests (lpush_del, multi_isolation) need WI-2;
                        chained wakes and wrong-type are independent
WI-6 (Streams)      ─── independent
WI-7 (Protocol/misc)─── independent
```

Parallelizable: WI-1, WI-3, WI-6, WI-7 can all proceed independently.
Sequential: WI-2 should land before the MULTI-dependent tests in WI-4 and WI-5.
Non-MULTI tests in WI-4 and WI-5 can proceed in parallel with WI-2.
