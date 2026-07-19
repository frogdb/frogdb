# Concurrency Phase 2 — DEBUG Introspection Commands + Quiescence Checkers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 2 of the concurrency-invariant-testing design
(`docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`): four always-available,
Redis-style `DEBUG` introspection subcommands — `DEBUG LOCKTABLE`, `DEBUG WAITQUEUE`,
`DEBUG MEMORY-CHECK`, `DEBUG EXPIRY-INDEX-CHECK` — each a single `ShardMessage` handled inside the
shard event loop (a naturally consistent per-shard snapshot) returning a structured RESP reply, plus
the four tier-4 **quiescence checkers** in `frogdb-testing`
(`check_locktable_empty`, `check_waitqueue_empty`, `check_memory_accounting`,
`check_expiry_index_consistent`) that consume the parsed replies.

**Architecture:** Every command follows the **exact `DEBUG VLL` template** already in the tree:

- a `ShardMessage::Get*Info { response_tx: oneshot::Sender<*Info> }` variant
  (`frogdb-server/crates/core/src/shard/message.rs`, alongside `GetVllQueueInfo` at line ~600) with a
  matching `probe_type_str` arm;
- a per-shard info struct in `frogdb-server/crates/core/src/shard/types.rs` (alongside `VllQueueInfo`
  at line ~819), re-exported from `shard/mod.rs`;
- a `collect_*`/`*_check` collector on `ShardWorker` in
  `frogdb-server/crates/core/src/shard/diagnostics.rs` (mirrors `collect_vll_queue_info`, line ~161);
- event-loop dispatch (`frogdb-server/crates/core/src/shard/event_loop.rs`, the `GetVllQueueInfo` arm
  at line ~281) routing to a new `dispatch_debug_introspection` handler;
- a `DebugProvider` trait method (`frogdb-server/crates/core/src/conn_command.rs`, `gather_vll` at
  line ~523) whose `ConnectionHandler` impl
  (`frogdb-server/crates/server/src/connection/handlers/debug.rs`, `gather_vll` at line ~72) does the
  per-shard oneshot round trip with a 5s per-shard timeout;
- an executor match arm + a `format_*_response` formatter + a `debug_help()` line + a `StubDebug`
  update (`frogdb-server/crates/server/src/connection/debug_conn_command.rs`; the `b"VLL"` arm is at
  line ~141, `debug_help` at ~235, `StubDebug` at ~760).

Two commands need new **privileged read-only accessors** the existing types do not expose: the wait
queue's per-key/per-conn detail (`waiters_by_key`, `conn_entries`) is private, so Task 1 adds a
read-only `dump()` carrying per-waiter registration order (enables the exact FIFO checking the
phase-1 ledger note calls for); and `HashMapStore` needs a live-size recompute (Task 2) and an
expiry-index audit (Task 3). `LOCKTABLE` reuses the VLL lock table's existing `iter_keys()` /
`lock_state_string()` (`frogdb-server/crates/vll/src/lock_table.rs`) via
`ShardVll::intent_snapshots()` / `continuation_lock_snapshot()` — no new VLL accessor is needed.

The quiescence checkers live in a **new** `frogdb-server/crates/testing/src/quiescence.rs` and consume
**transport-agnostic parsed structs** (`LockTableSnapshot`, `WaitQueueSnapshot`,
`MemoryCheckSnapshot`, `ExpiryIndexSnapshot`) — NOT `frogdb_protocol` RESP types and NOT the
`frogdb-core` info structs. **Decision (explicit):** quiescence violations are a **parallel**
`QuiescenceViolation` enum, not an extension of `conservation::ConservationViolation` — the two
checker families have disjoint inputs (`ConservationViolation` scans a `History`; quiescence checks
parsed DEBUG replies) and coupling them would force `History`-shaped variants into a reply checker.

**Tech Stack:** Rust, `bytes::Bytes`, `tokio::sync::oneshot`, `thiserror`, `frogdb_protocol::Response`
(RESP `Map`/`Array`/`Bulk`/`Integer`), `just` build system, `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You may be in a git worktree, not the main checkout. Only edit files under the
  directory `pwd` reports; use absolute paths.
- **Affected crates and their build commands** (package names verified in each `Cargo.toml`):
  - `frogdb-core` (`crates/core`), `frogdb-server` (`crates/server`), `frogdb-testing`
    (`crates/testing`).
  - Type-check: `just check <crate>`. Test (optionally filtered): `just test <crate> <pattern>` (the
    `test` recipe expands `<pattern>` to `-E 'test(/<pattern>/)'`). Format: `just fmt <crate>`. Lint:
    `just lint <crate>`.
  - Target the owning crate when running one test to avoid rebuilding the workspace.
  - If `sccache` errors, rerun the command prefixed with `RUSTC_WRAPPER=""`.
- **TDD:** every task writes a failing test first, watches it fail, then writes the minimal
  implementation. Never skip the "watch it fail" step.
- **Commit per task** with the exact `git` command shown in the task's final step.
- **No `Co-Authored-By` lines** in any commit message.
- **Existing DEBUG subcommands must keep working.** Adding `ShardMessage` variants forces updating the
  exhaustive `probe_type_str` match (`message.rs`) and the exhaustive event-loop dispatch match
  (`event_loop.rs`); adding a `DebugProvider` method forces updating both the `ConnectionHandler` impl
  (`handlers/debug.rs`) and the `StubDebug` test fixture (`debug_conn_command.rs`). Each of these is
  done **within the same task** so the tree compiles at every commit. After each server-side task run
  `just check frogdb-server` to confirm `StubDebug` still satisfies `DebugProvider`.
- **Production code, always available.** These are Redis-style `DEBUG` subcommands with no
  `enable-debug-command` gate (unlike `DEBUG SLEEP`) and no test-only `cfg`. The quiescence design
  deliberately probes through them so the same probes work against real processes in later
  Jepsen/replication phases.
- **Structured replies only via `Response::Simple`/`Bulk`/`Integer`/`Array`/`Map`.** Do not use
  `Response::Boolean`/`Double` in these formatters (keep RESP2 wire output unambiguous for tooling);
  encode flags as `Integer` 0/1.
- **YAGNI:** no optional `shard_id` filter argument on these four (unlike `DEBUG VLL`) — every command
  gathers all shards, because quiescence checks the whole fleet. No field-level expiry audit in
  `EXPIRY-INDEX-CHECK` (key-level `expiry_index` only — that is the spec's "persistent or deleted key"
  bullet and proposal 30's bug class); field expiry is a documented follow-up.

---

### Task 1: Read-only `ShardWaitQueue::dump()` with per-waiter registration order

`DEBUG WAITQUEUE` needs waiters "by key / kind / connection" in **registration order** so the FIFO
wake-fairness invariant can be checked exactly. The queue's `waiters_by_key`, `entries`, and
`conn_entries` are private and the per-key `VecDeque<usize>` already preserves FIFO order
(`push_back` on register), but nothing exposes it read-only and there is no cross-key monotonic
registration ordinal. This task adds a queue-owned monotonic sequence (assigned in `register`, no
change to `WaitEntry`'s three construction sites in `blocking.rs`/`execution.rs`) and a `dump()`
returning per-key waiters in FIFO order, each carrying its registration ordinal.

**Files:**
- Modify: `frogdb-server/crates/core/src/shard/wait_queue.rs`
- Modify: `frogdb-server/crates/core/src/shard/mod.rs` (re-export the new `WaiterDump` type)

**Interfaces:**
- Produces: `pub struct WaiterDump { pub conn_id: u64, pub op: &'static str, pub registration_seq: u64, pub has_deadline: bool }`;
  `ShardWaitQueue::dump(&self) -> Vec<(Bytes, Vec<WaiterDump>)>` — keys sorted lexicographically,
  waiters within a key in FIFO (registration) order.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block at the bottom of
`frogdb-server/crates/core/src/shard/wait_queue.rs`:

```rust
    #[test]
    fn dump_reports_fifo_order_and_registration_seq() {
        let mut queue = ShardWaitQueue::new();
        // Two waiters on "k" (conn 1 then conn 2), one on "j" (conn 3).
        queue.register(make_entry(1, vec!["k"])).unwrap();
        queue.register(make_entry(2, vec!["k"])).unwrap();
        queue.register(make_entry(3, vec!["j"])).unwrap();

        let dump = queue.dump();
        // Keys sorted lexicographically: "j" before "k".
        let keys: Vec<Bytes> = dump.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![Bytes::from("j"), Bytes::from("k")]);

        let k_waiters = &dump.iter().find(|(k, _)| k == "k").unwrap().1;
        // FIFO within the key: conn 1 registered before conn 2.
        assert_eq!(k_waiters[0].conn_id, 1);
        assert_eq!(k_waiters[1].conn_id, 2);
        assert!(k_waiters[0].registration_seq < k_waiters[1].registration_seq);
        assert_eq!(k_waiters[0].op, "BLPOP");
        assert!(!k_waiters[0].has_deadline);
    }

    #[test]
    fn dump_empty_when_no_waiters() {
        let queue = ShardWaitQueue::new();
        assert!(queue.dump().is_empty());
    }

    #[test]
    fn dump_reflects_registration_seq_across_keys() {
        let mut queue = ShardWaitQueue::new();
        queue.register(make_entry(1, vec!["k"])).unwrap();
        queue.register(make_entry(2, vec!["j"])).unwrap();
        let dump = queue.dump();
        let k_seq = dump.iter().find(|(k, _)| k == "k").unwrap().1[0].registration_seq;
        let j_seq = dump.iter().find(|(k, _)| k == "j").unwrap().1[0].registration_seq;
        // "k" was registered before "j" — its ordinal is strictly smaller even
        // though "j" sorts first in the dump output.
        assert!(k_seq < j_seq);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core dump_reports_fifo_order`
Expected: FAIL — compile error `no method named dump` / `cannot find type WaiterDump`.

- [ ] **Step 3: Write minimal implementation**

In `frogdb-server/crates/core/src/shard/wait_queue.rs`, add two fields to `ShardWaitQueue` (after
`max_blocked_connections`):

```rust
    /// Monotonic registration counter; each `register` stamps the next value.
    next_seq: u64,
    /// Registration ordinal per entry slot (parallel to `entries`). Only the
    /// slots of live entries are ever read (via `dump`); reused slots are
    /// overwritten on the next `register`, so stale ordinals are never observed.
    seq_by_slot: Vec<u64>,
```

Initialize both in `with_limits` (the `new` constructor delegates to it):

```rust
        Self {
            waiters_by_key: HashMap::new(),
            entries: Vec::new(),
            free_slots: Vec::new(),
            conn_entries: HashMap::new(),
            waiter_count: 0,
            max_waiters_per_key,
            max_blocked_connections,
            next_seq: 0,
            seq_by_slot: Vec::new(),
        }
```

In `register`, stamp the ordinal at slot allocation. Replace the slot-allocation block:

```rust
        // Allocate a slot for the entry
        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            self.entries[idx] = Some(entry);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            idx
        };
```

with:

```rust
        let seq = self.next_seq;
        self.next_seq += 1;

        // Allocate a slot for the entry, keeping `seq_by_slot` in lock-step
        // with `entries` so `seq_by_slot[slot_idx]` is always valid.
        let slot_idx = if let Some(idx) = self.free_slots.pop() {
            self.entries[idx] = Some(entry);
            self.seq_by_slot[idx] = seq;
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            self.seq_by_slot.push(seq);
            idx
        };
```

Add the `WaiterDump` type just above the `#[cfg(test)]` module:

```rust
/// One waiter's read-only diagnostic view (DEBUG WAITQUEUE). `op` is the
/// blocking-command name; `registration_seq` is the queue-wide monotonic
/// registration ordinal (smaller = registered earlier), enabling exact FIFO
/// wake-fairness checking.
#[derive(Debug, Clone)]
pub struct WaiterDump {
    pub conn_id: u64,
    pub op: &'static str,
    pub registration_seq: u64,
    pub has_deadline: bool,
}

/// Static name for a blocking op (DEBUG WAITQUEUE display).
fn blocking_op_name(op: &BlockingOp) -> &'static str {
    match op {
        BlockingOp::BLPop => "BLPOP",
        BlockingOp::BRPop => "BRPOP",
        BlockingOp::BLMove { .. } => "BLMOVE",
        BlockingOp::BLMPop { .. } => "BLMPOP",
        BlockingOp::BZPopMin => "BZPOPMIN",
        BlockingOp::BZPopMax => "BZPOPMAX",
        BlockingOp::BZMPop { .. } => "BZMPOP",
        BlockingOp::XRead { .. } => "XREAD",
        BlockingOp::XReadGroup { .. } => "XREADGROUP",
    }
}
```

Add the `dump` method inside `impl ShardWaitQueue` (place it right after `blocked_keys_count`):

```rust
    /// Read-only per-key dump of all waiters, in registration (FIFO) order
    /// within each key. Keys are returned sorted lexicographically for a
    /// deterministic snapshot. Used by DEBUG WAITQUEUE.
    pub fn dump(&self) -> Vec<(Bytes, Vec<WaiterDump>)> {
        let mut keys: Vec<&Bytes> = self.waiters_by_key.keys().collect();
        keys.sort();
        keys.into_iter()
            .map(|key| {
                let waiters = self
                    .waiters_by_key
                    .get(key)
                    .map(|deque| {
                        deque
                            .iter()
                            .filter_map(|&idx| {
                                let entry = self.entries[idx].as_ref()?;
                                Some(WaiterDump {
                                    conn_id: entry.conn_id,
                                    op: blocking_op_name(&entry.op),
                                    registration_seq: self.seq_by_slot[idx],
                                    has_deadline: entry.deadline.is_some(),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                (key.clone(), waiters)
            })
            .collect()
    }
```

- [ ] **Step 4: Re-export `WaiterDump`**

In `frogdb-server/crates/core/src/shard/mod.rs`, extend the wait-queue re-export line:

```rust
pub use wait_queue::{ShardWaitQueue, WaitEntry, WaiterDump};
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-core wait_queue`
Expected: PASS — the three new tests plus every pre-existing wait-queue test (register/unregister/
pop/drain counts are unchanged; `seq_by_slot` only grows alongside `entries`).

- [ ] **Step 6: Type-check the crate**

Run: `just check frogdb-core`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/core/src/shard/wait_queue.rs frogdb-server/crates/core/src/shard/mod.rs
git commit -m "feat(core): read-only wait-queue dump with per-waiter registration order"
```

---

### Task 2: `HashMapStore` live-size recompute for `DEBUG MEMORY-CHECK`

`memory_used` is a running counter (`crates/core/src/store/hashmap.rs:107`) charged on
insert/overwrite/delete and reconciled after in-place `get_mut` mutations via
`flush_keysizes_refreshes`. At quiesce (after the worker has flushed each command's deferred
refreshes) it must equal the sum of every live entry's size. `DEBUG MEMORY-CHECK` recomputes that live
sum and reports both, so drift (a leaked or double-counted charge) is observable. This task adds the
recompute on `HashMapStore` and a defaulted `Store`-trait method.

**Files:**
- Modify: `frogdb-server/crates/core/src/store/hashmap.rs`
- Modify: `frogdb-server/crates/core/src/store/mod.rs`

**Interfaces:**
- Produces: `Store::recompute_memory_used(&self) -> usize` (trait, default returns `self.memory_used()`);
  `HashMapStore::recompute_memory_used` override summing each entry's live `memory_size(key)`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/core/src/store/hashmap.rs` (the
module already has `use super::*;`, `Value`, and `Bytes` in scope — mirror the neighbouring tests for
exact imports):

```rust
    #[test]
    fn recompute_matches_tracked_after_inserts() {
        let mut store = HashMapStore::new();
        assert_eq!(store.recompute_memory_used(), store.memory_used());
        store.set(Bytes::from("a"), Value::string(Bytes::from("hello")));
        store.set(Bytes::from("b"), Value::string(Bytes::from("world")));
        assert_eq!(store.recompute_memory_used(), store.memory_used());
        store.delete(b"a");
        assert_eq!(store.recompute_memory_used(), store.memory_used());
    }

    #[test]
    fn recompute_reveals_unflushed_inplace_growth() {
        // In-place growth via get_mut is invisible to the tracked counter until
        // flush_keysizes_refreshes runs; recompute (live) reveals the drift, and
        // flushing reconciles it. This is exactly what DEBUG MEMORY-CHECK detects.
        let mut store = HashMapStore::new();
        store.set(Bytes::from("l"), Value::list());
        let before = store.recompute_memory_used();
        assert_eq!(before, store.memory_used());

        if let Some(v) = store.get_mut(b"l")
            && let Some(list) = v.as_list_mut()
        {
            for i in 0..100u32 {
                list.push_back(Bytes::from(format!("element-{i}")));
            }
        }
        // Live recompute now exceeds the still-unreconciled tracked counter.
        assert!(store.recompute_memory_used() > store.memory_used());

        store.flush_keysizes_refreshes();
        assert_eq!(store.recompute_memory_used(), store.memory_used());
    }
```

(If `ListValue::push_back` has a different name, use the constructor the neighbouring list tests in
this module already use — grep `Value::list` usages in the file. The assertion that matters is
`recompute > memory_used` before flush and `==` after.)

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core recompute_matches_tracked`
Expected: FAIL — compile error `no method named recompute_memory_used`.

- [ ] **Step 3: Add the trait method (defaulted)**

In `frogdb-server/crates/core/src/store/mod.rs`, add to the `Store` trait, immediately after the
`fn memory_used(&self) -> usize;` declaration:

```rust
    /// Recompute the live memory footprint by summing every entry's current
    /// size, independent of the running [`Store::memory_used`] counter.
    ///
    /// Used by `DEBUG MEMORY-CHECK`: at quiesce (deferred size refreshes
    /// flushed) this must equal `memory_used()`; a difference is an accounting
    /// leak. The default returns `memory_used()` (trivially consistent) for
    /// stores that do not track per-entry sizes.
    fn recompute_memory_used(&self) -> usize {
        self.memory_used()
    }
```

- [ ] **Step 4: Override on `HashMapStore`**

In `frogdb-server/crates/core/src/store/hashmap.rs`, inside the `impl Store for HashMapStore` block,
add next to the existing `fn memory_used`:

```rust
    fn recompute_memory_used(&self) -> usize {
        // `Entry::memory_size` returns the live size (key + value + metadata +
        // Entry overhead; warm values contribute zero value bytes). Summing it
        // over every entry is the ground truth the running counter tracks.
        self.data.iter().map(|(k, e)| e.memory_size(k)).sum()
    }
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-core recompute`
Expected: PASS (both new tests).

- [ ] **Step 6: Type-check the crate**

Run: `just check frogdb-core`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/core/src/store/hashmap.rs frogdb-server/crates/core/src/store/mod.rs
git commit -m "feat(core): recompute_memory_used for DEBUG MEMORY-CHECK"
```

---

### Task 3: `HashMapStore` expiry-index audit for `DEBUG EXPIRY-INDEX-CHECK`

The key-level `expiry_index` (`crates/core/src/store/hashmap.rs:103`, an `ExpiryIndex` from
`crates/core/src/noop.rs`) must, at all times, contain exactly the keys whose entry has an
`expires_at` deadline, with matching deadlines. Proposal 30's confirmed bug was a stale index entry
pointing at a key that had been made persistent — deleting a key that should have survived. This task
adds a read-only cross-check that walks the index against actual entry deadlines (both directions) and
returns the anomalies.

**Files:**
- Modify: `frogdb-server/crates/core/src/noop.rs` (`ExpiryIndex::iter`)
- Modify: `frogdb-server/crates/core/src/store/mod.rs` (anomaly types + defaulted trait method)
- Modify: `frogdb-server/crates/core/src/store/hashmap.rs` (`audit_expiry_index` override)

**Interfaces:**
- Produces: `ExpiryIndex::iter(&self) -> impl Iterator<Item = (&Bytes, Instant)>`;
  `pub struct ExpiryIndexAnomaly { pub key: String, pub kind: ExpiryIndexAnomalyKind }`;
  `pub enum ExpiryIndexAnomalyKind { KeyMissing, KeyPersistent, DeadlineMismatch, IndexMissing }`;
  `Store::audit_expiry_index(&self) -> Vec<ExpiryIndexAnomaly>` (default returns empty).

- [ ] **Step 1: Add `ExpiryIndex::iter` (no test yet — exercised via the store audit test)**

In `frogdb-server/crates/core/src/noop.rs`, add to `impl ExpiryIndex`, right after `get`:

```rust
    /// Read-only iterator over `(key, deadline)` pairs (unordered).
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, Instant)> {
        self.by_key.iter().map(|(k, t)| (k, *t))
    }
```

- [ ] **Step 2: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/core/src/store/hashmap.rs`:

```rust
    #[test]
    fn expiry_index_audit_clean_when_consistent() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("persistent"), Value::string(Bytes::from("v")));
        store.set(Bytes::from("ttl"), Value::string(Bytes::from("v")));
        store.set_expiry(b"ttl", Instant::now() + Duration::from_secs(3600));
        // Persistent key not in index, ttl key in index with a matching deadline.
        assert!(store.audit_expiry_index().is_empty());
    }

    #[test]
    fn expiry_index_audit_flags_stale_entry_after_persist() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        assert!(store.audit_expiry_index().is_empty());

        // Directly corrupt: leave a stale index entry for a now-persistent key.
        // (Simulates proposal-30's class of bug: a write path that forgot to
        // clear the index on overwrite/persist.)
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));
        store.persist(b"k"); // makes the entry persistent AND clears the index...
        // ...so re-corrupt after persist to model the leak:
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));

        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "k");
        assert!(matches!(anomalies[0].kind, ExpiryIndexAnomalyKind::KeyPersistent));
    }

    #[test]
    fn expiry_index_audit_flags_index_missing() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        // Drop only the index entry, leaving the entry's deadline in place.
        store.force_drop_index_entry_for_test(b"k");
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert!(matches!(anomalies[0].kind, ExpiryIndexAnomalyKind::IndexMissing));
    }
```

The two `*_for_test` helpers are `#[cfg(test)]`-only shims so the test can corrupt the otherwise
private index. Add them to `impl HashMapStore` (guarded, in the same file, above the `impl Store`
block):

```rust
    #[cfg(test)]
    fn force_index_entry_for_test(&mut self, key: Bytes, deadline: std::time::Instant) {
        self.expiry_index.set(key, deadline);
    }

    #[cfg(test)]
    fn force_drop_index_entry_for_test(&mut self, key: &[u8]) {
        self.expiry_index.remove(key);
    }
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-core expiry_index_audit`
Expected: FAIL — `no method named audit_expiry_index` / `cannot find type ExpiryIndexAnomalyKind`.

- [ ] **Step 4: Add anomaly types + defaulted trait method**

In `frogdb-server/crates/core/src/store/mod.rs`, add near the `Store` trait (after the imports, before
the trait), the anomaly types:

```rust
/// One inconsistency found by [`Store::audit_expiry_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiryIndexAnomaly {
    /// The offending key (lossy UTF-8 for display/transport).
    pub key: String,
    /// What is wrong.
    pub kind: ExpiryIndexAnomalyKind,
}

/// Classes of expiry-index inconsistency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryIndexAnomalyKind {
    /// Index references a key that no longer exists in the store.
    KeyMissing,
    /// Index references a key whose entry has no deadline (persistent).
    KeyPersistent,
    /// Index deadline disagrees with the entry's `expires_at`.
    DeadlineMismatch,
    /// Entry has a deadline but is absent from the index.
    IndexMissing,
}
```

Add to the `Store` trait (after `recompute_memory_used`):

```rust
    /// Cross-check the key-level expiry index against actual entry deadlines,
    /// returning every inconsistency. Used by `DEBUG EXPIRY-INDEX-CHECK`; at
    /// quiesce this must be empty. The default returns empty for stores without
    /// an expiry index.
    fn audit_expiry_index(&self) -> Vec<ExpiryIndexAnomaly> {
        Vec::new()
    }
```

- [ ] **Step 5: Override on `HashMapStore`**

In `frogdb-server/crates/core/src/store/hashmap.rs`, add to `impl Store for HashMapStore`. Use the
in-scope import path for the anomaly types (`crate::store::{ExpiryIndexAnomaly, ExpiryIndexAnomalyKind}` —
add to the file's `use` block if not already imported):

```rust
    fn audit_expiry_index(&self) -> Vec<ExpiryIndexAnomaly> {
        let mut anomalies = Vec::new();

        // Direction 1: every index entry must point at a key with a matching
        // deadline.
        for (key, index_deadline) in self.expiry_index.iter() {
            match self.data.get(key) {
                None => anomalies.push(ExpiryIndexAnomaly {
                    key: String::from_utf8_lossy(key).into_owned(),
                    kind: ExpiryIndexAnomalyKind::KeyMissing,
                }),
                Some(entry) => match entry.metadata.expires_at {
                    None => anomalies.push(ExpiryIndexAnomaly {
                        key: String::from_utf8_lossy(key).into_owned(),
                        kind: ExpiryIndexAnomalyKind::KeyPersistent,
                    }),
                    Some(actual) if actual != index_deadline => {
                        anomalies.push(ExpiryIndexAnomaly {
                            key: String::from_utf8_lossy(key).into_owned(),
                            kind: ExpiryIndexAnomalyKind::DeadlineMismatch,
                        })
                    }
                    Some(_) => {}
                },
            }
        }

        // Direction 2: every entry with a deadline must be in the index.
        for (key, entry) in self.data.iter() {
            if entry.metadata.expires_at.is_some() && self.expiry_index.get(key).is_none() {
                anomalies.push(ExpiryIndexAnomaly {
                    key: String::from_utf8_lossy(key).into_owned(),
                    kind: ExpiryIndexAnomalyKind::IndexMissing,
                });
            }
        }

        anomalies
    }
```

- [ ] **Step 6: Re-export the anomaly types**

In `frogdb-server/crates/core/src/store/mod.rs`, extend the `Store` re-export (line ~148) so the server
crate can name them, e.g.:

```rust
pub use store::{
    ExpiryIndexAnomaly, ExpiryIndexAnomalyKind, HashMapStore, Store, StoreTypedExt,
    StoreTypedFamilyExt, TypedArc, WrongTypeError,
};
```

Wait — that re-export lives in `lib.rs`. Edit `frogdb-server/crates/core/src/lib.rs` line ~148 to add
`ExpiryIndexAnomaly, ExpiryIndexAnomalyKind` to the `pub use store::{ … }` list.

- [ ] **Step 7: Run tests to verify they pass**

Run: `just test frogdb-core expiry_index_audit`
Expected: PASS (all three).

- [ ] **Step 8: Type-check the crate**

Run: `just check frogdb-core`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add frogdb-server/crates/core/src/noop.rs frogdb-server/crates/core/src/store/mod.rs \
        frogdb-server/crates/core/src/store/hashmap.rs frogdb-server/crates/core/src/lib.rs
git commit -m "feat(core): audit_expiry_index for DEBUG EXPIRY-INDEX-CHECK"
```

---

### Task 4: `DEBUG LOCKTABLE` — VLL intents, grants, continuation locks

The full vertical for the first command, establishing the shared scaffolding (`ShardMessage` variant,
info type, collector, the new `dispatch_debug_introspection` handler, `DebugProvider` method, executor
arm, help, `StubDebug`). Reuses `ShardVll::intent_snapshots()` (built on the VLL lock table's
`iter_keys()` + `lock_state_string()`) and `continuation_lock_snapshot()` — no new VLL accessor.

**Files:**
- Modify: `frogdb-server/crates/core/src/shard/message.rs`
- Modify: `frogdb-server/crates/core/src/shard/types.rs`
- Modify: `frogdb-server/crates/core/src/shard/mod.rs`
- Modify: `frogdb-server/crates/core/src/shard/diagnostics.rs`
- Modify: `frogdb-server/crates/core/src/shard/event_loop.rs`
- Create: `frogdb-server/crates/core/src/shard/dispatch_debug_introspection.rs`
- Modify: `frogdb-server/crates/core/src/conn_command.rs`
- Modify: `frogdb-server/crates/server/src/connection/handlers/debug.rs`
- Modify: `frogdb-server/crates/server/src/connection/debug_conn_command.rs`
- Modify: `frogdb-server/crates/server/tests/main.rs`
- Create: `frogdb-server/crates/server/tests/integration_debug_introspection.rs`

**Interfaces:**
- Produces: `ShardMessage::GetLockTableInfo { response_tx: oneshot::Sender<LockTableInfo> }`;
  `pub struct LockTableInfo { pub shard_id: usize, pub intents: Vec<VllKeyIntentInfo>, pub continuation_lock: Option<VllContinuationLockInfo> }`;
  `ShardWorker::collect_lock_table_info(&self) -> LockTableInfo`;
  `ShardWorker::dispatch_debug_introspection(&self, msg: ShardMessage)`;
  `DebugProvider::gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>>`.

- [ ] **Step 1: Write the failing collector test**

Add a `#[cfg(test)] mod introspection_tests` block at the bottom of
`frogdb-server/crates/core/src/shard/diagnostics.rs`. It builds a minimal worker (mirroring the
`make_test_worker` helper in `shard/rollback.rs`) and asserts the collector on an idle worker:

```rust
#[cfg(test)]
mod introspection_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use tokio::sync::mpsc;

    use crate::eviction::EvictionConfig;
    use crate::noop::NoopMetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{ShardReceiver, ShardSender};

    fn test_worker() -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_, conn_rx) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    #[test]
    fn lock_table_info_empty_on_idle_worker() {
        let worker = test_worker();
        let info = worker.collect_lock_table_info();
        assert_eq!(info.shard_id, 0);
        assert!(info.intents.is_empty());
        assert!(info.continuation_lock.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core lock_table_info_empty`
Expected: FAIL — `no method named collect_lock_table_info`.

- [ ] **Step 3: Add the info type + re-export**

In `frogdb-server/crates/core/src/shard/types.rs`, after `VllQueueInfo` (line ~832), add:

```rust
/// Response for `DEBUG LOCKTABLE` — a per-shard VLL lock-table snapshot.
#[derive(Debug, Clone, Default)]
pub struct LockTableInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Per-key intents (txids + grant state), reusing the VLL intent view.
    pub intents: Vec<VllKeyIntentInfo>,
    /// The continuation lock, if one is held.
    pub continuation_lock: Option<VllContinuationLockInfo>,
}
```

In `frogdb-server/crates/core/src/shard/mod.rs`, add `LockTableInfo` to the `pub use types::{ … }`
list.

- [ ] **Step 4: Add the collector**

In `frogdb-server/crates/core/src/shard/diagnostics.rs`, add to `impl ShardWorker` (next to
`collect_vll_queue_info`), and add `LockTableInfo` to the `use super::types::{ … }` import at the top:

```rust
    /// Collect the VLL lock-table snapshot for `DEBUG LOCKTABLE`.
    pub(crate) fn collect_lock_table_info(&self) -> LockTableInfo {
        let intents = self
            .vll
            .intent_snapshots()
            .into_iter()
            .map(|snap| VllKeyIntentInfo {
                key: Self::format_key_for_display(&snap.key),
                txids: snap.txids,
                lock_state: snap.lock_state,
            })
            .collect();
        let continuation_lock = self.vll.continuation_lock_snapshot().map(|lock| {
            VllContinuationLockInfo {
                txid: lock.txid,
                conn_id: lock.conn_id,
                age_ms: lock.age_ms,
            }
        });
        LockTableInfo {
            shard_id: self.shard_id(),
            intents,
            continuation_lock,
        }
    }
```

- [ ] **Step 5: Add the `ShardMessage` variant + probe string**

In `frogdb-server/crates/core/src/shard/message.rs`, add after `GetVllQueueInfo` (line ~603):

```rust
    /// Get the VLL lock-table snapshot from this shard (DEBUG LOCKTABLE).
    GetLockTableInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::LockTableInfo>,
    },
```

Add the matching `probe_type_str` arm next to `GetVllQueueInfo` (line ~676):

```rust
            ShardMessage::GetLockTableInfo { .. } => "GetLockTableInfo",
```

- [ ] **Step 6: Create the introspection dispatcher + wire the event loop**

Create `frogdb-server/crates/core/src/shard/dispatch_debug_introspection.rs`:

```rust
//! Dispatch for the always-available DEBUG introspection messages
//! (LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK). Each is a
//! read-only per-shard snapshot handled inside the shard event loop — the
//! probe surface the concurrency-invariant quiescence checkers consult. All
//! collectors are `&self`; none await.

use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch a DEBUG introspection message: build the per-shard snapshot and
    /// reply on its oneshot.
    pub(super) fn dispatch_debug_introspection(&self, msg: ShardMessage) {
        match msg {
            ShardMessage::GetLockTableInfo { response_tx } => {
                let _ = response_tx.send(self.collect_lock_table_info());
            }
            _ => unreachable!("dispatch_debug_introspection got a non-introspection message"),
        }
    }
}
```

Register the module in `frogdb-server/crates/core/src/shard/mod.rs` (next to the other `mod
dispatch_*;` lines): `mod dispatch_debug_introspection;`.

In `frogdb-server/crates/core/src/shard/event_loop.rs`, add a dispatch arm. Place it right after the
VLL group (the `GetVllQueueInfo { .. } => self.dispatch_vll(msg).await,` arm, line ~281):

```rust
            GetLockTableInfo { .. } => {
                self.dispatch_debug_introspection(msg);
                false
            }
```

- [ ] **Step 7: Run the collector test to verify it passes**

Run: `just test frogdb-core lock_table_info_empty && just check frogdb-core`
Expected: PASS.

- [ ] **Step 8: Add the `DebugProvider` method + `ConnectionHandler` impl**

In `frogdb-server/crates/core/src/conn_command.rs`, add to the `DebugProvider` trait (after
`gather_vll`, line ~526):

```rust
    /// DEBUG LOCKTABLE — the per-shard VLL lock-table snapshots (all shards).
    fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::LockTableInfo>>;
```

In `frogdb-server/crates/server/src/connection/handlers/debug.rs`, add the impl (mirror `gather_vll`'s
per-shard oneshot round trip with a 5s timeout), and add `LockTableInfo` to the
`use frogdb_core::shard::{ … }` import:

```rust
    /// DEBUG LOCKTABLE — gather the VLL lock-table snapshot from every shard.
    fn gather_lock_table<'a>(&'a self) -> BoxFuture<'a, Vec<LockTableInfo>> {
        Box::pin(async move {
            use tokio::sync::oneshot;

            let mut results = Vec::new();
            let timeout = std::time::Duration::from_secs(5);

            for shard_id in 0..self.core.shard_senders.len() {
                let (response_tx, response_rx) = oneshot::channel();
                if self.core.shard_senders[shard_id]
                    .send(frogdb_core::shard::ShardMessage::GetLockTableInfo { response_tx })
                    .await
                    .is_err()
                {
                    tracing::warn!(shard_id, "Failed to send GetLockTableInfo message");
                    continue;
                }
                match tokio::time::timeout(timeout, response_rx).await {
                    Ok(Ok(info)) => results.push(info),
                    Ok(Err(_)) => {
                        tracing::warn!(shard_id, "Channel closed while waiting for lock-table info")
                    }
                    Err(_) => tracing::warn!(shard_id, "Timeout waiting for lock-table info"),
                }
            }
            results
        })
    }
```

- [ ] **Step 9: Add the executor arm, formatter, help line, and `StubDebug` method**

In `frogdb-server/crates/server/src/connection/debug_conn_command.rs`:

Add a match arm in `execute` (next to `b"VLL"`, line ~141):

```rust
                b"LOCKTABLE" => format_locktable_response(debug.gather_lock_table().await),
```

Add the formatter (next to `format_vll_response`). `frogdb_core::shard::LockTableInfo` /
`VllKeyIntentInfo` / `VllContinuationLockInfo` are already reachable via the `frogdb_core` import:

```rust
/// Format `DEBUG LOCKTABLE` — a RESP map of `shard:<id>` → per-shard detail.
/// Empty across all shards returns a recognizable sentinel bulk string.
fn format_locktable_response(infos: Vec<frogdb_core::shard::LockTableInfo>) -> Response {
    let all_empty = infos
        .iter()
        .all(|i| i.intents.is_empty() && i.continuation_lock.is_none());
    if all_empty {
        return Response::Bulk(Some(Bytes::from("# lock table is empty")));
    }

    let mut shards = Vec::new();
    for info in infos {
        let intents = Response::Array(
            info.intents
                .iter()
                .map(|intent| {
                    Response::Map(vec![
                        (
                            Response::bulk("key"),
                            Response::bulk(intent.key.clone()),
                        ),
                        (
                            Response::bulk("txids"),
                            Response::Array(
                                intent.txids.iter().map(|t| Response::Integer(*t as i64)).collect(),
                            ),
                        ),
                        (
                            Response::bulk("lock_state"),
                            Response::bulk(intent.lock_state.clone()),
                        ),
                    ])
                })
                .collect(),
        );
        let continuation_lock = match &info.continuation_lock {
            Some(l) => Response::bulk(format!(
                "txid:{} conn_id:{} age_ms:{}",
                l.txid, l.conn_id, l.age_ms
            )),
            None => Response::Bulk(None),
        };
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (Response::bulk("continuation_lock"), continuation_lock),
                (Response::bulk("intents"), intents),
            ]),
        ));
    }
    Response::Map(shards)
}
```

(`Response::bulk` accepts `impl Into<Bytes>`; if a call site needs an owned `String`, `Response::bulk`
already handles it — mirror the existing `debug_help` usage.)

Add a `debug_help` entry (in the `help` vector, next to the VLL lines):

```rust
        "DEBUG LOCKTABLE",
        "    Show the per-shard VLL lock table (intents, grants, continuation locks).",
```

Add the `StubDebug` method (in the `#[cfg(test)] mod tests` `impl DebugProvider for StubDebug` block):

```rust
        fn gather_lock_table<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::LockTableInfo>> {
            Box::pin(async { Vec::new() })
        }
```

- [ ] **Step 10: Run the unit tests + verify the stub still satisfies the trait**

Run: `just test frogdb-server debug_conn_command && just check frogdb-server`
Expected: PASS — the existing `StubDebug`-driven executor tests still pass; the stub compiles against
the extended trait.

- [ ] **Step 11: Write the integration test**

Register the module in `frogdb-server/crates/server/tests/main.rs` (add near the other
`mod integration_*;` lines): `mod integration_debug_introspection;`.

Create `frogdb-server/crates/server/tests/integration_debug_introspection.rs`:

```rust
//! Integration tests for the DEBUG introspection commands (Phase 2 concurrency
//! quiescence probes): LOCKTABLE, WAITQUEUE, MEMORY-CHECK, EXPIRY-INDEX-CHECK.

use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

#[tokio::test]
async fn debug_locktable_empty_on_idle_server() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DEBUG", "LOCKTABLE"]).await;
    // Idle server: no intents, no continuation locks -> sentinel bulk string.
    match resp {
        Response::Bulk(Some(b)) => {
            assert_eq!(&b[..], b"# lock table is empty");
        }
        other => panic!("expected empty-sentinel bulk, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn debug_locktable_unknown_still_errors_are_isolated() {
    // Regression guard: adding LOCKTABLE must not break the unknown-subcommand path.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "NOPE-NOT-A-CMD"]).await;
    assert!(matches!(resp, Response::Error(_)));
    server.shutdown().await;
}
```

- [ ] **Step 12: Run the integration test + full affected-crate gates**

Run:
```
just test frogdb-server debug_locktable
just check frogdb-core && just check frogdb-server
just lint frogdb-core && just lint frogdb-server
just fmt frogdb-core && just fmt frogdb-server
```
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
git add frogdb-server/crates/core/src/shard frogdb-server/crates/core/src/conn_command.rs \
        frogdb-server/crates/server/src/connection/handlers/debug.rs \
        frogdb-server/crates/server/src/connection/debug_conn_command.rs \
        frogdb-server/crates/server/tests/main.rs \
        frogdb-server/crates/server/tests/integration_debug_introspection.rs
git commit -m "feat(server): DEBUG LOCKTABLE introspection command"
```

---

### Task 5: `DEBUG WAITQUEUE` — waiters by key / kind / connection

Second vertical, built on Task 1's `dump()`. Same scaffolding shape as Task 4; the introspection
dispatcher, event-loop match, `DebugProvider`, executor, help, and `StubDebug` all gain one more
member. The reply preserves per-waiter registration order (exact FIFO checking).

**Files:** same set as Task 4 (no new files; extend
`integration_debug_introspection.rs`), plus the wait-queue dump from Task 1.

**Interfaces:**
- Produces: `ShardMessage::GetWaitQueueInfo { response_tx: oneshot::Sender<WaitQueueInfo> }`;
  `pub struct WaitQueueInfo { pub shard_id: usize, pub total_waiters: usize, pub keys: Vec<WaitQueueKeyInfo> }`;
  `pub struct WaitQueueKeyInfo { pub key: String, pub waiters: Vec<WaitQueueWaiterInfo> }`;
  `pub struct WaitQueueWaiterInfo { pub conn_id: u64, pub op: String, pub registration_seq: u64, pub has_deadline: bool }`;
  `ShardWorker::collect_wait_queue_info(&self) -> WaitQueueInfo`;
  `DebugProvider::gather_wait_queue<'a>(&'a self) -> BoxFuture<'a, Vec<WaitQueueInfo>>`.

- [ ] **Step 1: Write the failing collector test**

Add to the `introspection_tests` module in `shard/diagnostics.rs` (reuse `test_worker`). Register a
waiter directly on `worker.wait_queue` (it is `pub(crate)`), constructing a `WaitEntry` the way
`wait_queue.rs`'s own tests do:

```rust
    #[test]
    fn wait_queue_info_reports_registered_waiter() {
        use crate::shard::WaitEntry;
        use crate::types::BlockingOp;
        use bytes::Bytes;
        use frogdb_protocol::ProtocolVersion;
        use tokio::sync::oneshot;

        let mut worker = test_worker();
        let (tx, _rx) = oneshot::channel();
        worker
            .wait_queue
            .register(WaitEntry {
                conn_id: 7,
                keys: vec![Bytes::from("k")],
                op: BlockingOp::BLPop,
                response_tx: tx,
                deadline: None,
                protocol_version: ProtocolVersion::default(),
            })
            .unwrap();

        let info = worker.collect_wait_queue_info();
        assert_eq!(info.shard_id, 0);
        assert_eq!(info.total_waiters, 1);
        assert_eq!(info.keys.len(), 1);
        assert_eq!(info.keys[0].key, "k");
        assert_eq!(info.keys[0].waiters[0].conn_id, 7);
        assert_eq!(info.keys[0].waiters[0].op, "BLPOP");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core wait_queue_info_reports`
Expected: FAIL — `no method named collect_wait_queue_info`.

- [ ] **Step 3: Add the info types + re-export**

In `frogdb-server/crates/core/src/shard/types.rs`, after `LockTableInfo`:

```rust
/// Response for `DEBUG WAITQUEUE` — a per-shard blocking-waiter snapshot.
#[derive(Debug, Clone, Default)]
pub struct WaitQueueInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Total active waiters on this shard.
    pub total_waiters: usize,
    /// Waiters grouped by key (keys sorted; waiters in registration order).
    pub keys: Vec<WaitQueueKeyInfo>,
}

/// Waiters blocked on one key.
#[derive(Debug, Clone)]
pub struct WaitQueueKeyInfo {
    /// The key (lossy UTF-8 for display).
    pub key: String,
    /// Waiters in registration (FIFO) order.
    pub waiters: Vec<WaitQueueWaiterInfo>,
}

/// One blocked waiter's view.
#[derive(Debug, Clone)]
pub struct WaitQueueWaiterInfo {
    /// Connection id of the blocked client.
    pub conn_id: u64,
    /// Blocking command name (e.g. "BLPOP").
    pub op: String,
    /// Queue-wide monotonic registration ordinal (smaller = earlier).
    pub registration_seq: u64,
    /// Whether the waiter has a finite deadline.
    pub has_deadline: bool,
}
```

In `shard/mod.rs`, add `WaitQueueInfo, WaitQueueKeyInfo, WaitQueueWaiterInfo` to the
`pub use types::{ … }` list.

- [ ] **Step 4: Add the collector**

In `shard/diagnostics.rs` (`impl ShardWorker`, add the new types to the `use super::types::{ … }`
import):

```rust
    /// Collect the blocking-waiter snapshot for `DEBUG WAITQUEUE`.
    pub(crate) fn collect_wait_queue_info(&self) -> WaitQueueInfo {
        let keys = self
            .wait_queue
            .dump()
            .into_iter()
            .map(|(key, waiters)| WaitQueueKeyInfo {
                key: Self::format_key_for_display(&key),
                waiters: waiters
                    .into_iter()
                    .map(|w| WaitQueueWaiterInfo {
                        conn_id: w.conn_id,
                        op: w.op.to_string(),
                        registration_seq: w.registration_seq,
                        has_deadline: w.has_deadline,
                    })
                    .collect(),
            })
            .collect();
        WaitQueueInfo {
            shard_id: self.shard_id(),
            total_waiters: self.wait_queue.waiter_count(),
            keys,
        }
    }
```

- [ ] **Step 5: Add the `ShardMessage` variant + probe + dispatch + event-loop arm**

In `shard/message.rs`, after `GetLockTableInfo`:

```rust
    /// Get the blocking-waiter snapshot from this shard (DEBUG WAITQUEUE).
    GetWaitQueueInfo {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::WaitQueueInfo>,
    },
```

Add the `probe_type_str` arm: `ShardMessage::GetWaitQueueInfo { .. } => "GetWaitQueueInfo",`.

In `shard/dispatch_debug_introspection.rs`, add a match arm before the `_ => unreachable!`:

```rust
            ShardMessage::GetWaitQueueInfo { response_tx } => {
                let _ = response_tx.send(self.collect_wait_queue_info());
            }
```

In `shard/event_loop.rs`, extend the introspection dispatch arm to cover the new variant:

```rust
            GetLockTableInfo { .. } | GetWaitQueueInfo { .. } => {
                self.dispatch_debug_introspection(msg);
                false
            }
```

- [ ] **Step 6: Run collector test + type-check**

Run: `just test frogdb-core wait_queue_info_reports && just check frogdb-core`
Expected: PASS.

- [ ] **Step 7: Add `DebugProvider` method + `ConnectionHandler` impl**

In `conn_command.rs` (`DebugProvider`, after `gather_lock_table`):

```rust
    /// DEBUG WAITQUEUE — the per-shard blocking-waiter snapshots (all shards).
    fn gather_wait_queue<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::WaitQueueInfo>>;
```

In `handlers/debug.rs`, add the impl mirroring `gather_lock_table` exactly but with
`ShardMessage::GetWaitQueueInfo` and `WaitQueueInfo` (add `WaitQueueInfo` to the
`use frogdb_core::shard::{ … }` import). Copy the 5s-timeout per-shard loop verbatim, substituting the
message and log strings.

- [ ] **Step 8: Add executor arm, formatter, help, `StubDebug`**

In `debug_conn_command.rs`:

Executor arm (next to `b"LOCKTABLE"`):

```rust
                b"WAITQUEUE" => format_waitqueue_response(debug.gather_wait_queue().await),
```

Formatter:

```rust
/// Format `DEBUG WAITQUEUE` — a RESP map of `shard:<id>` → detail, preserving
/// per-waiter registration order. Empty across all shards -> sentinel bulk.
fn format_waitqueue_response(infos: Vec<frogdb_core::shard::WaitQueueInfo>) -> Response {
    if infos.iter().all(|i| i.total_waiters == 0) {
        return Response::Bulk(Some(Bytes::from("# wait queue is empty")));
    }
    let mut shards = Vec::new();
    for info in infos {
        let keys = Response::Array(
            info.keys
                .iter()
                .map(|k| {
                    let waiters = Response::Array(
                        k.waiters
                            .iter()
                            .map(|w| {
                                Response::Map(vec![
                                    (Response::bulk("conn_id"), Response::Integer(w.conn_id as i64)),
                                    (Response::bulk("op"), Response::bulk(w.op.clone())),
                                    (
                                        Response::bulk("registration_seq"),
                                        Response::Integer(w.registration_seq as i64),
                                    ),
                                    (
                                        Response::bulk("has_deadline"),
                                        Response::Integer(i64::from(w.has_deadline)),
                                    ),
                                ])
                            })
                            .collect(),
                    );
                    Response::Map(vec![
                        (Response::bulk("key"), Response::bulk(k.key.clone())),
                        (Response::bulk("waiters"), waiters),
                    ])
                })
                .collect(),
        );
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("total_waiters"),
                    Response::Integer(info.total_waiters as i64),
                ),
                (Response::bulk("keys"), keys),
            ]),
        ));
    }
    Response::Map(shards)
}
```

Help line:

```rust
        "DEBUG WAITQUEUE",
        "    Show blocked waiters by key/connection, in registration order.",
```

`StubDebug` method:

```rust
        fn gather_wait_queue<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::WaitQueueInfo>> {
            Box::pin(async { Vec::new() })
        }
```

- [ ] **Step 9: Run unit tests + check**

Run: `just test frogdb-server debug_conn_command && just check frogdb-server`
Expected: PASS.

- [ ] **Step 10: Extend the integration test (idle + populated)**

Append to `frogdb-server/crates/server/tests/integration_debug_introspection.rs`:

```rust
#[tokio::test]
async fn debug_waitqueue_empty_on_idle_server() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "WAITQUEUE"]).await;
    match resp {
        Response::Bulk(Some(b)) => assert_eq!(&b[..], b"# wait queue is empty"),
        other => panic!("expected empty-sentinel bulk, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test]
async fn debug_waitqueue_reports_blocked_client() {
    let server = TestServer::start_standalone().await;

    // A second connection blocks on BLPOP with an infinite timeout.
    let mut blocker = server.connect().await;
    let handle = tokio::spawn(async move {
        // Never resolves within the test; the task is dropped at shutdown.
        blocker.command(&["BLPOP", "waitq-key", "0"]).await
    });

    // Poll DEBUG WAITQUEUE until the waiter appears (bounded).
    let mut probe = server.connect().await;
    let mut seen = false;
    for _ in 0..50 {
        let resp = probe.command(&["DEBUG", "WAITQUEUE"]).await;
        if matches!(resp, Response::Map(_)) {
            seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(seen, "DEBUG WAITQUEUE never reported the blocked BLPOP");

    handle.abort();
    server.shutdown().await;
}
```

- [ ] **Step 11: Run integration + gates**

Run:
```
just test frogdb-server debug_waitqueue
just check frogdb-core && just check frogdb-server
just lint frogdb-core && just lint frogdb-server
just fmt frogdb-core && just fmt frogdb-server
```
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add frogdb-server/crates/core/src/shard frogdb-server/crates/core/src/conn_command.rs \
        frogdb-server/crates/server/src/connection/handlers/debug.rs \
        frogdb-server/crates/server/src/connection/debug_conn_command.rs \
        frogdb-server/crates/server/tests/integration_debug_introspection.rs
git commit -m "feat(server): DEBUG WAITQUEUE introspection command"
```

---

### Task 6: `DEBUG MEMORY-CHECK` — recomputed live size vs tracked `memory_used`

Third vertical, built on Task 2's `recompute_memory_used`. Same scaffolding shape.

**Files:** same set as Task 5 (extend `integration_debug_introspection.rs`).

**Interfaces:**
- Produces: `ShardMessage::MemoryCheck { response_tx: oneshot::Sender<MemoryCheckInfo> }`;
  `pub struct MemoryCheckInfo { pub shard_id: usize, pub tracked_bytes: usize, pub recomputed_bytes: usize }`;
  `ShardWorker::collect_memory_check(&self) -> MemoryCheckInfo`;
  `DebugProvider::memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<MemoryCheckInfo>>`.

- [ ] **Step 1: Write the failing collector test**

Add to `introspection_tests` in `shard/diagnostics.rs`:

```rust
    #[test]
    fn memory_check_consistent_after_writes() {
        use crate::store::Store;
        use crate::types::Value;
        use bytes::Bytes;

        let mut worker = test_worker();
        worker.store.set(Bytes::from("a"), Value::string(Bytes::from("hello")));
        worker.store.set(Bytes::from("b"), Value::string(Bytes::from("world")));

        let info = worker.collect_memory_check();
        assert_eq!(info.shard_id, 0);
        assert_eq!(info.tracked_bytes, info.recomputed_bytes);
        assert!(info.tracked_bytes > 0);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core memory_check_consistent`
Expected: FAIL — `no method named collect_memory_check`.

- [ ] **Step 3: Add the info type + re-export**

In `shard/types.rs`, after the wait-queue types:

```rust
/// Response for `DEBUG MEMORY-CHECK` — tracked vs recomputed live footprint.
#[derive(Debug, Clone, Default)]
pub struct MemoryCheckInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// The running `memory_used` counter.
    pub tracked_bytes: usize,
    /// Recomputed live sum over all entries.
    pub recomputed_bytes: usize,
}
```

Add `MemoryCheckInfo` to the `pub use types::{ … }` list in `shard/mod.rs`.

- [ ] **Step 4: Add the collector**

In `shard/diagnostics.rs` (add `MemoryCheckInfo` to the `use super::types::{ … }` import, and add
`use crate::store::Store;` if the `Store` trait methods are not already in scope in this file):

```rust
    /// Collect the memory-accounting cross-check for `DEBUG MEMORY-CHECK`.
    pub(crate) fn collect_memory_check(&self) -> MemoryCheckInfo {
        MemoryCheckInfo {
            shard_id: self.shard_id(),
            tracked_bytes: self.store.memory_used(),
            recomputed_bytes: self.store.recompute_memory_used(),
        }
    }
```

- [ ] **Step 5: Add the `ShardMessage` variant + probe + dispatch + event-loop arm**

`shard/message.rs`, after `GetWaitQueueInfo`:

```rust
    /// Recompute live memory and report tracked vs recomputed (DEBUG MEMORY-CHECK).
    MemoryCheck {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::MemoryCheckInfo>,
    },
```

`probe_type_str` arm: `ShardMessage::MemoryCheck { .. } => "MemoryCheck",`.

`shard/dispatch_debug_introspection.rs`, add arm:

```rust
            ShardMessage::MemoryCheck { response_tx } => {
                let _ = response_tx.send(self.collect_memory_check());
            }
```

`shard/event_loop.rs`, extend the introspection arm:

```rust
            GetLockTableInfo { .. } | GetWaitQueueInfo { .. } | MemoryCheck { .. } => {
                self.dispatch_debug_introspection(msg);
                false
            }
```

- [ ] **Step 6: Run collector test + check**

Run: `just test frogdb-core memory_check_consistent && just check frogdb-core`
Expected: PASS.

- [ ] **Step 7: `DebugProvider` + `ConnectionHandler` impl**

`conn_command.rs` (`DebugProvider`):

```rust
    /// DEBUG MEMORY-CHECK — per-shard tracked-vs-recomputed memory (all shards).
    fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::MemoryCheckInfo>>;
```

`handlers/debug.rs`: add the impl mirroring `gather_lock_table` but with
`ShardMessage::MemoryCheck` and `MemoryCheckInfo` (add to the import). Copy the 5s-timeout loop.

- [ ] **Step 8: Executor arm, formatter, help, `StubDebug`**

Executor arm (next to `b"WAITQUEUE"`):

```rust
                b"MEMORY-CHECK" => format_memory_check_response(debug.memory_check().await),
```

Formatter (always a map — a diff of zero is still worth reporting; `consistent` encoded as 0/1):

```rust
/// Format `DEBUG MEMORY-CHECK` — RESP map of `shard:<id>` → {tracked, recomputed,
/// diff, consistent}. `diff` is recomputed − tracked (may be negative).
fn format_memory_check_response(infos: Vec<frogdb_core::shard::MemoryCheckInfo>) -> Response {
    let mut shards = Vec::new();
    for info in infos {
        let diff = info.recomputed_bytes as i64 - info.tracked_bytes as i64;
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("tracked_bytes"),
                    Response::Integer(info.tracked_bytes as i64),
                ),
                (
                    Response::bulk("recomputed_bytes"),
                    Response::Integer(info.recomputed_bytes as i64),
                ),
                (Response::bulk("diff"), Response::Integer(diff)),
                (
                    Response::bulk("consistent"),
                    Response::Integer(i64::from(diff == 0)),
                ),
            ]),
        ));
    }
    Response::Map(shards)
}
```

Help line:

```rust
        "DEBUG MEMORY-CHECK",
        "    Recompute live memory and report the diff vs the tracked counter.",
```

`StubDebug` method:

```rust
        fn memory_check<'a>(&'a self) -> BoxFuture<'a, Vec<frogdb_core::shard::MemoryCheckInfo>> {
            Box::pin(async { Vec::new() })
        }
```

- [ ] **Step 9: Run unit tests + check**

Run: `just test frogdb-server debug_conn_command && just check frogdb-server`
Expected: PASS.

- [ ] **Step 10: Extend the integration test**

Append to `integration_debug_introspection.rs`:

```rust
#[tokio::test]
async fn debug_memory_check_consistent_after_writes() {
    use crate::common::response_helpers::unwrap_integer;

    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    for i in 0..64 {
        let key = format!("mc-{i}");
        let resp = client.command(&["SET", &key, "some-value"]).await;
        assert!(matches!(resp, Response::Simple(_)));
    }

    let resp = client.command(&["DEBUG", "MEMORY-CHECK"]).await;
    let Response::Map(shards) = resp else {
        panic!("expected map, got {resp:?}");
    };
    assert!(!shards.is_empty());
    for (_shard, detail) in &shards {
        let Response::Map(fields) = detail else {
            panic!("expected per-shard map");
        };
        let consistent = fields
            .iter()
            .find(|(k, _)| matches!(k, Response::Bulk(Some(b)) if &b[..] == b"consistent"))
            .map(|(_, v)| unwrap_integer(v))
            .expect("consistent field present");
        assert_eq!(consistent, 1, "memory accounting drifted at quiesce");
    }

    server.shutdown().await;
}
```

- [ ] **Step 11: Run integration + gates**

Run:
```
just test frogdb-server debug_memory_check
just check frogdb-core && just check frogdb-server
just lint frogdb-core && just lint frogdb-server
just fmt frogdb-core && just fmt frogdb-server
```
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add frogdb-server/crates/core/src/shard frogdb-server/crates/core/src/conn_command.rs \
        frogdb-server/crates/server/src/connection/handlers/debug.rs \
        frogdb-server/crates/server/src/connection/debug_conn_command.rs \
        frogdb-server/crates/server/tests/integration_debug_introspection.rs
git commit -m "feat(server): DEBUG MEMORY-CHECK introspection command"
```

---

### Task 7: `DEBUG EXPIRY-INDEX-CHECK` — index entries vs entry deadlines

Fourth vertical, built on Task 3's `audit_expiry_index`. Same scaffolding shape; the reply carries the
per-shard anomaly list.

**Files:** same set as Task 6 (extend `integration_debug_introspection.rs`).

**Interfaces:**
- Produces: `ShardMessage::ExpiryIndexCheck { response_tx: oneshot::Sender<ExpiryIndexCheckInfo> }`;
  `pub struct ExpiryIndexCheckInfo { pub shard_id: usize, pub total_entries: usize, pub anomalies: Vec<ExpiryIndexAnomaly> }`
  (reusing `frogdb_core::ExpiryIndexAnomaly` from Task 3);
  `ShardWorker::collect_expiry_index_check(&self) -> ExpiryIndexCheckInfo`;
  `DebugProvider::expiry_index_check<'a>(&'a self) -> BoxFuture<'a, Vec<ExpiryIndexCheckInfo>>`.

- [ ] **Step 1: Write the failing collector test**

Add to `introspection_tests` in `shard/diagnostics.rs`:

```rust
    #[test]
    fn expiry_index_check_clean_after_expire() {
        use crate::store::Store;
        use crate::types::Value;
        use bytes::Bytes;
        use std::time::{Duration, Instant};

        let mut worker = test_worker();
        worker.store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        worker
            .store
            .set_expiry(b"k", Instant::now() + Duration::from_secs(3600));

        let info = worker.collect_expiry_index_check();
        assert_eq!(info.shard_id, 0);
        assert_eq!(info.total_entries, 1);
        assert!(info.anomalies.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-core expiry_index_check_clean`
Expected: FAIL — `no method named collect_expiry_index_check`.

- [ ] **Step 3: Add the info type + re-export**

In `shard/types.rs`, after `MemoryCheckInfo` (import the anomaly type at the top of the file with
`use crate::store::ExpiryIndexAnomaly;`):

```rust
/// Response for `DEBUG EXPIRY-INDEX-CHECK` — index-vs-entry inconsistencies.
#[derive(Debug, Clone, Default)]
pub struct ExpiryIndexCheckInfo {
    /// Shard identifier.
    pub shard_id: usize,
    /// Number of key-level expiry-index entries examined.
    pub total_entries: usize,
    /// Inconsistencies found (empty = consistent).
    pub anomalies: Vec<ExpiryIndexAnomaly>,
}
```

Add `ExpiryIndexCheckInfo` to the `pub use types::{ … }` list in `shard/mod.rs`.

- [ ] **Step 4: Add the collector**

The collector needs the index-entry count. `Store` does not expose it directly, but
`keys_with_expiry_count()` does (it returns the key-level expiry count). In `shard/diagnostics.rs`
(add `ExpiryIndexCheckInfo` to the `use super::types::{ … }` import; `crate::store::Store` already in
scope from Task 6):

```rust
    /// Collect the expiry-index cross-check for `DEBUG EXPIRY-INDEX-CHECK`.
    pub(crate) fn collect_expiry_index_check(&self) -> ExpiryIndexCheckInfo {
        ExpiryIndexCheckInfo {
            shard_id: self.shard_id(),
            total_entries: self.store.keys_with_expiry_count(),
            anomalies: self.store.audit_expiry_index(),
        }
    }
```

- [ ] **Step 5: Add the `ShardMessage` variant + probe + dispatch + event-loop arm**

`shard/message.rs`, after `MemoryCheck`:

```rust
    /// Cross-check the expiry index against entry deadlines (DEBUG EXPIRY-INDEX-CHECK).
    ExpiryIndexCheck {
        /// Channel to send the response.
        response_tx: oneshot::Sender<super::types::ExpiryIndexCheckInfo>,
    },
```

`probe_type_str` arm: `ShardMessage::ExpiryIndexCheck { .. } => "ExpiryIndexCheck",`.

`shard/dispatch_debug_introspection.rs`, add arm:

```rust
            ShardMessage::ExpiryIndexCheck { response_tx } => {
                let _ = response_tx.send(self.collect_expiry_index_check());
            }
```

`shard/event_loop.rs`, extend the introspection arm to the final four-way form:

```rust
            GetLockTableInfo { .. }
            | GetWaitQueueInfo { .. }
            | MemoryCheck { .. }
            | ExpiryIndexCheck { .. } => {
                self.dispatch_debug_introspection(msg);
                false
            }
```

- [ ] **Step 6: Run collector test + check**

Run: `just test frogdb-core expiry_index_check_clean && just check frogdb-core`
Expected: PASS.

- [ ] **Step 7: `DebugProvider` + `ConnectionHandler` impl**

`conn_command.rs` (`DebugProvider`):

```rust
    /// DEBUG EXPIRY-INDEX-CHECK — per-shard index-vs-deadline audit (all shards).
    fn expiry_index_check<'a>(&'a self) -> BoxFuture<'a, Vec<crate::shard::ExpiryIndexCheckInfo>>;
```

`handlers/debug.rs`: add the impl mirroring `gather_lock_table` with `ShardMessage::ExpiryIndexCheck`
and `ExpiryIndexCheckInfo` (add to the import). Copy the 5s-timeout loop.

- [ ] **Step 8: Executor arm, formatter, help, `StubDebug`**

Executor arm (next to `b"MEMORY-CHECK"`):

```rust
                b"EXPIRY-INDEX-CHECK" => {
                    format_expiry_index_check_response(debug.expiry_index_check().await)
                }
```

Formatter (empty across all shards -> sentinel; otherwise a map of per-shard detail with the anomaly
list). Anomaly kind rendered via its `Debug` (import path `frogdb_core::ExpiryIndexAnomalyKind` not
needed — format the kind with `format!("{:?}", …)`):

```rust
/// Format `DEBUG EXPIRY-INDEX-CHECK` — sentinel when every shard is clean, else
/// a RESP map of `shard:<id>` → {total_entries, anomalies:[{key, kind}]}.
fn format_expiry_index_check_response(
    infos: Vec<frogdb_core::shard::ExpiryIndexCheckInfo>,
) -> Response {
    if infos.iter().all(|i| i.anomalies.is_empty()) {
        return Response::Bulk(Some(Bytes::from("# expiry index is consistent")));
    }
    let mut shards = Vec::new();
    for info in infos {
        let anomalies = Response::Array(
            info.anomalies
                .iter()
                .map(|a| {
                    Response::Map(vec![
                        (Response::bulk("key"), Response::bulk(a.key.clone())),
                        (Response::bulk("kind"), Response::bulk(format!("{:?}", a.kind))),
                    ])
                })
                .collect(),
        );
        shards.push((
            Response::bulk(format!("shard:{}", info.shard_id)),
            Response::Map(vec![
                (
                    Response::bulk("total_entries"),
                    Response::Integer(info.total_entries as i64),
                ),
                (Response::bulk("anomalies"), anomalies),
            ]),
        ));
    }
    Response::Map(shards)
}
```

Help line:

```rust
        "DEBUG EXPIRY-INDEX-CHECK",
        "    Cross-check the expiry index against entry deadlines.",
```

`StubDebug` method:

```rust
        fn expiry_index_check<'a>(
            &'a self,
        ) -> BoxFuture<'a, Vec<frogdb_core::shard::ExpiryIndexCheckInfo>> {
            Box::pin(async { Vec::new() })
        }
```

- [ ] **Step 9: Run unit tests + check**

Run: `just test frogdb-server debug_conn_command && just check frogdb-server`
Expected: PASS.

- [ ] **Step 10: Extend the integration test**

Append to `integration_debug_introspection.rs`:

```rust
#[tokio::test]
async fn debug_expiry_index_check_consistent_after_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // A persistent key and a key with a long TTL: the index must be consistent.
    assert!(matches!(client.command(&["SET", "persistent", "v"]).await, Response::Simple(_)));
    assert!(matches!(client.command(&["SET", "ttl", "v", "EX", "3600"]).await, Response::Simple(_)));

    let resp = client.command(&["DEBUG", "EXPIRY-INDEX-CHECK"]).await;
    match resp {
        Response::Bulk(Some(b)) => assert_eq!(&b[..], b"# expiry index is consistent"),
        other => panic!("expected consistent-sentinel bulk, got {other:?}"),
    }

    server.shutdown().await;
}
```

- [ ] **Step 11: Run integration + gates + DEBUG HELP regression**

Run:
```
just test frogdb-server debug_expiry_index
just test frogdb-server debug_conn_command
just check frogdb-core && just check frogdb-server
just lint frogdb-core && just lint frogdb-server
just fmt frogdb-core && just fmt frogdb-server
```
Expected: PASS — including the existing `help_is_nonempty_array` test (the four new help lines are
additive).

- [ ] **Step 12: Commit**

```bash
git add frogdb-server/crates/core/src/shard frogdb-server/crates/core/src/conn_command.rs \
        frogdb-server/crates/server/src/connection/handlers/debug.rs \
        frogdb-server/crates/server/src/connection/debug_conn_command.rs \
        frogdb-server/crates/server/tests/integration_debug_introspection.rs
git commit -m "feat(server): DEBUG EXPIRY-INDEX-CHECK introspection command"
```

---

### Task 8: Quiescence checkers in `frogdb-testing`

The tier-4 quiescence checkers consuming the parsed DEBUG replies. Transport-agnostic parsed structs
(no RESP types, no `frogdb-core` info types) plus a parallel `QuiescenceViolation` enum and four pure
checker functions. Wired into `lib.rs`. The turmoil harness (phase 3) will decode DEBUG replies into
these structs; this task delivers the checkers and their unit tests.

**Files:**
- Create: `frogdb-server/crates/testing/src/quiescence.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Produces: `pub struct LockTableSnapshot { pub shard_id: usize, pub intent_key_count: usize, pub continuation_lock_held: bool }`;
  `pub struct WaitQueueSnapshot { pub shard_id: usize, pub total_waiters: usize }`;
  `pub struct MemoryCheckSnapshot { pub shard_id: usize, pub tracked_bytes: u64, pub recomputed_bytes: u64 }`;
  `pub struct ExpiryIndexSnapshot { pub shard_id: usize, pub anomaly_count: usize }`;
  `pub enum QuiescenceViolation { … }`;
  `check_locktable_empty(&[LockTableSnapshot]) -> Result<(), QuiescenceViolation>`;
  `check_waitqueue_empty(&[WaitQueueSnapshot]) -> Result<(), QuiescenceViolation>`;
  `check_memory_accounting(&[MemoryCheckSnapshot]) -> Result<(), QuiescenceViolation>`;
  `check_expiry_index_consistent(&[ExpiryIndexSnapshot]) -> Result<(), QuiescenceViolation>`.

- [ ] **Step 1: Write the failing test**

Create `frogdb-server/crates/testing/src/quiescence.rs` with only the test module to start:

```rust
//! Tier-4 quiescence checkers (concurrency-invariant-testing design).
//!
//! These consume **parsed DEBUG-reply snapshots** — transport-agnostic structs,
//! not RESP types and not `frogdb-core` info structs — so the same checkers
//! ingest snapshots from turmoil, real-network, and later Jepsen/replication
//! runs. Run after a workload drains and expiry settles: every check must hold.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locktable_empty_passes_and_fails() {
        let clean = [
            LockTableSnapshot { shard_id: 0, intent_key_count: 0, continuation_lock_held: false },
            LockTableSnapshot { shard_id: 1, intent_key_count: 0, continuation_lock_held: false },
        ];
        assert!(check_locktable_empty(&clean).is_ok());

        let leaked_intent = [LockTableSnapshot {
            shard_id: 3,
            intent_key_count: 2,
            continuation_lock_held: false,
        }];
        assert!(matches!(
            check_locktable_empty(&leaked_intent),
            Err(QuiescenceViolation::LockTableNotEmpty { shard_id: 3, intent_keys: 2, .. })
        ));

        let leaked_lock = [LockTableSnapshot {
            shard_id: 1,
            intent_key_count: 0,
            continuation_lock_held: true,
        }];
        assert!(matches!(
            check_locktable_empty(&leaked_lock),
            Err(QuiescenceViolation::LockTableNotEmpty { continuation_lock: true, .. })
        ));
    }

    #[test]
    fn waitqueue_empty_passes_and_fails() {
        assert!(check_waitqueue_empty(&[WaitQueueSnapshot { shard_id: 0, total_waiters: 0 }]).is_ok());
        assert!(matches!(
            check_waitqueue_empty(&[WaitQueueSnapshot { shard_id: 2, total_waiters: 1 }]),
            Err(QuiescenceViolation::WaitQueueNotEmpty { shard_id: 2, waiters: 1 })
        ));
    }

    #[test]
    fn memory_accounting_passes_and_fails() {
        let ok = [MemoryCheckSnapshot { shard_id: 0, tracked_bytes: 4096, recomputed_bytes: 4096 }];
        assert!(check_memory_accounting(&ok).is_ok());

        let drift = [MemoryCheckSnapshot { shard_id: 1, tracked_bytes: 4096, recomputed_bytes: 5000 }];
        assert!(matches!(
            check_memory_accounting(&drift),
            Err(QuiescenceViolation::MemoryAccountingDrift {
                shard_id: 1,
                tracked: 4096,
                recomputed: 5000,
            })
        ));
    }

    #[test]
    fn expiry_index_consistent_passes_and_fails() {
        assert!(check_expiry_index_consistent(&[ExpiryIndexSnapshot { shard_id: 0, anomaly_count: 0 }]).is_ok());
        assert!(matches!(
            check_expiry_index_consistent(&[ExpiryIndexSnapshot { shard_id: 4, anomaly_count: 3 }]),
            Err(QuiescenceViolation::ExpiryIndexInconsistent { shard_id: 4, anomalies: 3 })
        ));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-testing quiescence`
Expected: FAIL — `cannot find type LockTableSnapshot` / `cannot find function check_locktable_empty`.

- [ ] **Step 3: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/quiescence.rs` (above the `#[cfg(test)]` module):

```rust
/// Parsed `DEBUG LOCKTABLE` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct LockTableSnapshot {
    pub shard_id: usize,
    pub intent_key_count: usize,
    pub continuation_lock_held: bool,
}

/// Parsed `DEBUG WAITQUEUE` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct WaitQueueSnapshot {
    pub shard_id: usize,
    pub total_waiters: usize,
}

/// Parsed `DEBUG MEMORY-CHECK` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct MemoryCheckSnapshot {
    pub shard_id: usize,
    pub tracked_bytes: u64,
    pub recomputed_bytes: u64,
}

/// Parsed `DEBUG EXPIRY-INDEX-CHECK` snapshot for one shard.
#[derive(Debug, Clone)]
pub struct ExpiryIndexSnapshot {
    pub shard_id: usize,
    pub anomaly_count: usize,
}

/// A quiescence-invariant violation. Parallel to
/// [`crate::conservation::ConservationViolation`]: disjoint input (parsed DEBUG
/// replies vs a `History`), so it is its own enum rather than a shared one.
#[derive(Debug, thiserror::Error)]
pub enum QuiescenceViolation {
    /// The VLL lock table still holds intents or a continuation lock at quiesce.
    #[error(
        "shard {shard_id}: lock table not empty at quiesce ({intent_keys} intent key(s), continuation_lock={continuation_lock})"
    )]
    LockTableNotEmpty {
        shard_id: usize,
        intent_keys: usize,
        continuation_lock: bool,
    },
    /// The wait queue still holds blocked waiters at quiesce.
    #[error("shard {shard_id}: wait queue not empty at quiesce ({waiters} waiter(s))")]
    WaitQueueNotEmpty { shard_id: usize, waiters: usize },
    /// The tracked memory counter disagrees with the recomputed live size.
    #[error(
        "shard {shard_id}: memory accounting drift (tracked {tracked}, recomputed {recomputed})"
    )]
    MemoryAccountingDrift {
        shard_id: usize,
        tracked: u64,
        recomputed: u64,
    },
    /// The expiry index disagrees with entry deadlines.
    #[error("shard {shard_id}: expiry index inconsistent ({anomalies} anomaly/anomalies)")]
    ExpiryIndexInconsistent { shard_id: usize, anomalies: usize },
}

/// VLL lock table empty on every shard (no leaked intents or continuation locks).
pub fn check_locktable_empty(
    snapshots: &[LockTableSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.intent_key_count > 0 || s.continuation_lock_held {
            return Err(QuiescenceViolation::LockTableNotEmpty {
                shard_id: s.shard_id,
                intent_keys: s.intent_key_count,
                continuation_lock: s.continuation_lock_held,
            });
        }
    }
    Ok(())
}

/// Wait queue empty on every shard (no orphaned waiters after disconnects/timeouts).
pub fn check_waitqueue_empty(
    snapshots: &[WaitQueueSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.total_waiters > 0 {
            return Err(QuiescenceViolation::WaitQueueNotEmpty {
                shard_id: s.shard_id,
                waiters: s.total_waiters,
            });
        }
    }
    Ok(())
}

/// Tracked `memory_used` matches the recomputed live size on every shard.
pub fn check_memory_accounting(
    snapshots: &[MemoryCheckSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.tracked_bytes != s.recomputed_bytes {
            return Err(QuiescenceViolation::MemoryAccountingDrift {
                shard_id: s.shard_id,
                tracked: s.tracked_bytes,
                recomputed: s.recomputed_bytes,
            });
        }
    }
    Ok(())
}

/// Expiry index has no entry pointing at a persistent or deleted key on any shard.
pub fn check_expiry_index_consistent(
    snapshots: &[ExpiryIndexSnapshot],
) -> Result<(), QuiescenceViolation> {
    for s in snapshots {
        if s.anomaly_count > 0 {
            return Err(QuiescenceViolation::ExpiryIndexInconsistent {
                shard_id: s.shard_id,
                anomalies: s.anomaly_count,
            });
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Wire into `lib.rs`**

In `frogdb-server/crates/testing/src/lib.rs`, add `pub mod quiescence;` next to `pub mod conservation;`
and a re-export block:

```rust
pub use quiescence::{
    ExpiryIndexSnapshot, LockTableSnapshot, MemoryCheckSnapshot, QuiescenceViolation,
    WaitQueueSnapshot, check_expiry_index_consistent, check_locktable_empty,
    check_memory_accounting, check_waitqueue_empty,
};
```

Also add a bullet to the crate-level doc comment's feature list (mirroring the conservation bullet):
`//! - Quiescence checkers (`quiescence`): lock-table-empty, wait-queue-empty, memory-accounting,`
`//!   expiry-index-consistent — consuming parsed DEBUG-reply snapshots`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing quiescence`
Expected: PASS (all four checker tests).

- [ ] **Step 6: Full affected-crate gates**

Run:
```
just check frogdb-testing
just lint frogdb-testing
just fmt frogdb-testing
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/testing/src/quiescence.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): tier-4 quiescence checkers over parsed DEBUG replies"
```

---

## Final verification

After Task 8, run the full affected-crate suites once more to confirm nothing regressed:

```
just test frogdb-core
just test frogdb-server debug
just test frogdb-testing
just check frogdb-core && just check frogdb-server && just check frogdb-testing
just lint frogdb-core && just lint frogdb-server && just lint frogdb-testing
just fmt frogdb-core && just fmt frogdb-server && just fmt frogdb-testing
```

Spec coverage self-check — every Phase-2 item maps to a task:

| Spec item (design §"DEBUG introspection commands" / Invariants tier 4 / Architecture pt 3) | Task |
|---|---|
| `DEBUG LOCKTABLE` (intents, grants, continuation locks) | 4 |
| `DEBUG WAITQUEUE` (waiters by key/kind/connection, registration order) | 1, 5 |
| `DEBUG MEMORY-CHECK` (recompute live size, diff vs `memory_used`) | 2, 6 |
| `DEBUG EXPIRY-INDEX-CHECK` (index vs entry deadlines) | 3, 7 |
| Each = ShardMessage variant + info struct + dispatch + DebugProvider method + match arm + help + StubDebug | 4–7 |
| Structured RESP map replies, follow `DEBUG VLL` pattern | 4–7 |
| Quiescence checkers in `frogdb-testing` (4 functions, parsed structs, parallel violation enum) | 8 |
| Unit tests per shard handler + StubDebug executor tests + integration tests + checker unit tests | 4–8 |
