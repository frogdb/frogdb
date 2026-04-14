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
| **Fix** | 102 | Implement across 6 remaining work items (WI-1,3,4,5,6,7) |
| **Reclassify** | 5 | HLL corruption tests → `intentional-incompatibility:encoding` |
| **Already done (removed)** | 13 | Stale `#[ignore]` removed — tests already passing |

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

### WI-1: Lua VM Completeness (58 tests)

**Files:** `scripting_tcl.rs` (52) + `cluster_scripting_tcl.rs` (6)
**Goal:** Make EVAL/EVALSHA fully Redis-compatible, including cluster-mode scripting behavior.

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
| Cluster scripting | ~6 | Shebang flags, strict key validation, no-cluster flag enforcement |
| Remaining edge cases | ~7 | Arity validation, bit operations, CLUSTER RESET denial |

### WI-2: Transaction Post-Execution Refactor (~12 tests unlocked) — COMPLETE

**Status:** Complete (2026-04-13). No code refactor needed — the 3 core atomicity tests were already
passing. The `#[ignore]` attributes were stale (the underlying fix landed in commit `e63b8bf4`
on 2026-03-31). The per-command satisfaction loop is functionally correct because all commands
execute before `run_transaction_post_execution()` runs, so the store reflects final state.

**Fixed (stale ignores removed):**
- `tcl_blpop_lpush_del_should_not_awake_blocked_client` (list)
- `tcl_blpop_lpush_del_set_should_not_awake_blocked_client` (list)
- `tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop` (list)

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

### WI-5: Blocking List & MULTI Edge Cases (6 tests remaining)

**Files:** `list_tcl.rs` (4) + `multi_tcl.rs` (2)
**Goal:** Fix wrong-type handling, ordering, blocking-in-MULTI, and SORT STORE.

Chained wakes (BLMOVE cascade, circular BRPOPLPUSH, linked LMOVEs) are now passing — removed 3
stale `#[ignore]` attributes.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Wrong-type destination | 2 | Validate dest type before pushing in BLMove handler |
| Multi-client wake ordering | 1 | FIFO ordering for multiple clients blocked on same key |
| Blocking in MULTI | 2 | `tcl_brpoplpush_inside_a_transaction` + `tcl_blocking_commands_ignore_timeout_in_multi` |
| SORT STORE | 1 | `tcl_exec_fail_on_watched_key_modified_by_sort_store_empty` |

### WI-6: Stream XREADGROUP Unblock (6 tests remaining)

**Files:** Stream command handlers, shard blocking infrastructure
**Goal:** Unblock XREADGROUP waiters on key/group lifecycle events.

`tcl_xinfo_full_output` and `tcl_xautoclaim_claim_from_another` now pass — removed stale ignores.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| XGROUP DESTROY unblock | 1 | Unblock with NOGROUP error |
| Key deletion unblock | 1 | Unblock on DEL/UNLINK with error |
| Key type change unblock | 1 | Unblock with WRONGTYPE |
| Deleted entry representation | 1 | Return `[id, []]` for deleted entries in PEL history |
| Blocking timeout behavior | 1 | Return nil on timeout, not empty array |
| Flaky timing test | 1 | `tcl_xautoclaim_as_iterator` — increase sleep or use retry loops |

### WI-7: Protocol Edge Cases & Misc (10 tests remaining)

**Files:** `protocol_tcl.rs` (5) + `keyspace_tcl.rs` (1) + `introspection_tcl.rs` (1) +
`tracking_tcl.rs` (2) + `auth_tcl.rs` (1)
**Goal:** Graceful error handling for malformed RESP + remaining one-offs.

Previously passing tests removed: `tcl_scan_guarantees_under_write_load`,
`tcl_protected_mode_works_as_expected`, `tcl_unauthenticated_clients_*` (3),
`tcl_pexpiretime_returns_absolute_expiration_time_in_milliseconds`.

| Sub-task | Tests | Description |
|----------|------:|-------------|
| Empty query handling | 1 | Ignore `\r\n` alone, process next command |
| Negative multibulk | 1 | Ignore and continue |
| Out-of-range multibulk/bulk | 2 | Send `-ERR` instead of closing connection |
| Large argument count | 1 | Handle 10k+ args gracefully |
| KEYS nested pattern | 1 | Fix glob matching for deeply nested patterns |
| CLIENT LIST tot-cmds | 1 | Track tot-cmds for blocked commands |
| FLUSHDB tracking invalidation | 1 | Fix invalidation ordering during FLUSHDB |
| TRACKING REDIRECT | 1 | Fix RESP3 tracking redirection behavior |
| AUTH binary password | 1 | `tcl_auth_fails_when_binary_password_is_wrong` |

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

## Priority Order

All remaining WIs are independent — ordered by impact (tests unlocked per effort):

| Priority | WI | Tests | Rationale |
|---------:|:---|------:|-----------|
| 1 | WI-1: Lua VM | 58 | Largest group, first-class feature commitment |
| 2 | WI-3: Command registry | 7 | Quick win, low effort |
| 3 | WI-4: CLIENT PAUSE | 15 | Medium effort, needed for rolling upgrades |
| 4 | WI-6: Streams | 6 | Medium effort, clean subsystem boundary |
| 5 | WI-7: Protocol/misc | 10 | Low-medium effort, various one-offs |
| 6 | WI-5: Blocking/MULTI | 6 | Small scope, edge cases |
| 7 | HLL reclassify | 5 | Documentation only, no code changes |
| -- | ~~WI-2: Tx refactor~~ | 0 | COMPLETE (stale ignores removed) |

Remaining `#[ignore]`: 107 (120 original - 13 stale ignores removed).
WI totals: 58 + 7 + 15 + 6 + 6 + 10 + 5 = 107.
