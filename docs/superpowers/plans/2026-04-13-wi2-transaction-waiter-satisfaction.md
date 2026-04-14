# WI-2: Transaction Post-Execution Waiter Satisfaction Refactor

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Defer blocking waiter satisfaction in MULTI/EXEC transactions until after all commands complete, so blocked clients see final transaction state instead of intermediate per-command state.

**Architecture:** Replace the per-command `satisfy_waiters_for_command()` loop in `run_transaction_post_execution()` (and its `_after_wal` variant) with a single pass that collects all waiter-relevant keys from the entire transaction, deduplicates them, then calls satisfaction once per unique key based on the final store state. This matches Redis behavior where blockers only wake after EXEC completes.

**Tech Stack:** Rust, tokio, bytes crate. Tests use the `redis-regression` integration test harness (`TestServer`, `TestClient`).

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `frogdb-server/crates/core/src/command.rs:232` | Modify | Add `Hash` derive to `WaiterKind` enum (needed for dedup HashSet) |
| `frogdb-server/crates/core/src/shard/pipeline.rs` | Modify | Replace per-command waiter loop with deduped post-transaction satisfaction |
| `frogdb-server/crates/redis-regression/tests/list_tcl.rs` | Modify | Remove `#[ignore]` from 3 atomicity tests |

---

### Task 1: Write a focused unit test for the fix

We'll un-ignore the 3 existing tests that directly validate this behavior. First, verify they fail without the fix.

**Files:**
- Modify: `frogdb-server/crates/redis-regression/tests/list_tcl.rs:1701-1794`

- [ ] **Step 1: Remove `#[ignore]` from the 3 atomicity tests**

In `frogdb-server/crates/redis-regression/tests/list_tcl.rs`, remove the `#[ignore = "..."]` attribute from these 3 tests:

Line 1702: Remove `#[ignore = "FrogDB LPUSH+DEL atomicity gap: shard notifies waiter before DEL completes"]`
Line 1733: Remove `#[ignore = "FrogDB LPUSH+DEL+SET atomicity gap: shard notifies waiter before DEL completes"]`
Line 1769: Remove `#[ignore = "FrogDB blocking client sees intermediate MULTI state"]`

- [ ] **Step 2: Run the tests to confirm they fail**

Run:
```bash
just test frogdb-redis-regression tcl_blpop_lpush_del_should_not_awake_blocked_client
just test frogdb-redis-regression tcl_blpop_lpush_del_set_should_not_awake_blocked_client
just test frogdb-redis-regression tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop
```

Expected: All 3 FAIL. The first two will assert `val == b"b"` but get `b"a"` (blocker wakes from intermediate LPUSH). The third will assert `val == b"c"` but get `b"a"` (blocker wakes from first LPUSH instead of seeing final state).

- [ ] **Step 3: Commit the test changes**

```bash
git add frogdb-server/crates/redis-regression/tests/list_tcl.rs
git commit -m "test(compat): un-ignore 3 MULTI/EXEC atomicity tests for blocking waiters"
```

---

### Task 2: Refactor `run_transaction_post_execution()` to defer waiter satisfaction

**Files:**
- Modify: `frogdb-server/crates/core/src/shard/pipeline.rs:171-248`

- [ ] **Step 1: Add `Hash` derive to `WaiterKind`**

In `frogdb-server/crates/core/src/command.rs`, line 232, change:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaiterKind {
```

to:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WaiterKind {
```

This is needed because the new `satisfy_waiters_for_transaction()` method uses a `HashSet<(WaiterKind, Bytes)>` for deduplication.

- [ ] **Step 2: Add a new method `satisfy_waiters_for_transaction()`**

Add this method to the `impl ShardWorker` block in `pipeline.rs`, directly after the existing `satisfy_waiters()` method (after line 196):

```rust
    /// Satisfy blocking waiters based on the **final** state of all keys written
    /// by a transaction.
    ///
    /// Unlike `satisfy_waiters_for_command()` (which is called per-command and
    /// may wake blockers on intermediate state), this collects all waiter-relevant
    /// keys from the entire transaction, deduplicates them, and satisfies waiters
    /// once per unique (key, kind) pair. This ensures blockers only see the
    /// post-EXEC state — matching Redis MULTI/EXEC semantics.
    fn satisfy_waiters_for_transaction(&mut self, write_infos: &[(&dyn Command, &[Bytes])]) {
        // Collect all (kind, key) pairs that might wake waiters, deduped.
        let mut wake_keys: Vec<(WaiterKind, Bytes)> = Vec::new();
        let mut seen: std::collections::HashSet<(WaiterKind, Bytes)> =
            std::collections::HashSet::new();

        for &(handler, args) in write_infos {
            match handler.wakes_waiters() {
                WaiterWake::None => {}
                WaiterWake::Kind(kind) => {
                    for key in handler.keys(args) {
                        let key_bytes = Bytes::copy_from_slice(key);
                        if seen.insert((kind, key_bytes.clone())) {
                            wake_keys.push((kind, key_bytes));
                        }
                    }
                }
                WaiterWake::All => {
                    for key in handler.keys(args) {
                        let key_bytes = Bytes::copy_from_slice(key);
                        for kind in
                            [WaiterKind::List, WaiterKind::SortedSet, WaiterKind::Stream]
                        {
                            if seen.insert((kind, key_bytes.clone())) {
                                wake_keys.push((kind, key_bytes.clone()));
                            }
                        }
                    }
                }
            }
        }

        // Now satisfy waiters once per unique (kind, key) — the store already
        // reflects the final transaction state, so try_satisfy_*_waiters will
        // check whether the key actually has data before waking anyone.
        for (kind, key) in wake_keys {
            match kind {
                WaiterKind::List => self.try_satisfy_list_waiters(&key),
                WaiterKind::SortedSet => self.try_satisfy_zset_waiters(&key),
                WaiterKind::Stream => self.try_satisfy_stream_waiters(&key),
            }
        }
    }
```

- [ ] **Step 3: Replace the per-command loop in `run_transaction_post_execution()`**

In `run_transaction_post_execution()` (line 209), replace lines 245-248:

```rust
        // 4. Satisfy blocking waiters for all relevant keys
        for &(handler, args) in write_infos {
            self.satisfy_waiters_for_command(handler, args);
        }
```

with:

```rust
        // 4. Satisfy blocking waiters based on final transaction state
        self.satisfy_waiters_for_transaction(write_infos);
```

- [ ] **Step 4: Apply the same fix to `run_transaction_post_execution_after_wal()`**

In `run_transaction_post_execution_after_wal()` (line 276), replace lines 312-315:

```rust
        // 4. Waiter satisfaction
        for &(handler, args) in write_infos {
            self.satisfy_waiters_for_command(handler, args);
        }
```

with:

```rust
        // 4. Satisfy blocking waiters based on final transaction state
        self.satisfy_waiters_for_transaction(write_infos);
```

- [ ] **Step 5: Run the 3 un-ignored tests**

```bash
just test frogdb-redis-regression tcl_blpop_lpush_del_should_not_awake_blocked_client
just test frogdb-redis-regression tcl_blpop_lpush_del_set_should_not_awake_blocked_client
just test frogdb-redis-regression tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop
```

Expected: All 3 PASS.

- [ ] **Step 6: Run the full regression suite to check for regressions**

```bash
just test frogdb-redis-regression
```

Expected: No new failures. Existing passing tests (especially blocking list tests like `tcl_blpop_*`, `tcl_brpoplpush_*`, `tcl_blmove_*`) must still pass.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/core/src/command.rs frogdb-server/crates/core/src/shard/pipeline.rs
git commit -m "fix(shard): defer waiter satisfaction until after MULTI/EXEC completes

Replace per-command satisfy_waiters_for_command() loop in
run_transaction_post_execution() with a single deduped pass that
collects all waiter-relevant keys, then satisfies based on final
transaction state. Blocked clients now see post-EXEC state instead
of intermediate per-command state, matching Redis MULTI/EXEC semantics.

Fixes: tcl_blpop_lpush_del_should_not_awake_blocked_client
Fixes: tcl_blpop_lpush_del_set_should_not_awake_blocked_client
Fixes: tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop"
```

---

### Task 3: Verify no regressions in broader test suite

**Files:** None modified — verification only.

- [ ] **Step 1: Run the full server test suite**

```bash
just test frogdb-server
```

Expected: PASS (no regressions in unit or integration tests).

- [ ] **Step 2: Run clippy on the modified crate**

```bash
just lint frogdb-core
```

Expected: No new warnings. The `std::collections::HashSet` import is used; if clippy suggests using a different collection, follow its suggestion.

- [ ] **Step 3: Run fmt**

```bash
just fmt frogdb-core
```

Expected: No formatting changes needed (or auto-fixed).

---

### Task 4: Update documentation

**Files:**
- Modify: `docs/superpowers/specs/2026-04-13-ignored-test-triage-design.md`

- [ ] **Step 1: Update the spec to reflect completed work**

In the WI-2 section of `docs/superpowers/specs/2026-04-13-ignored-test-triage-design.md`, add a note at the top:

```markdown
**Status:** Complete (3 tests fixed: tcl_blpop_lpush_del_should_not_awake_blocked_client, tcl_blpop_lpush_del_set_should_not_awake_blocked_client, tcl_multi_exec_is_isolated_from_the_point_of_view_of_blpop)
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-04-13-ignored-test-triage-design.md
git commit -m "docs: mark WI-2 transaction waiter satisfaction as complete"
```
