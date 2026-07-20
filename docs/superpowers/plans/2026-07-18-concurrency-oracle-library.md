# Concurrency Oracle Library (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the `frogdb-testing` crate with per-type sequential models (list/hash/zset/stream), an inconclusive-aware linearizability result, a node-aware history format, per-key history partitioning, whole-history conservation checkers, and fault-injection self-tests — the Phase-1 "Oracle library" from the concurrency-invariant-testing design.

**Architecture:** All work lands in `frogdb-server/crates/testing` (crate name `frogdb-testing`). Models split from the single 678-line `models.rs` into a `models/` module directory. New STRICT models reject unknown ops (`None`) unlike `KVModel`'s permissive fallthrough. Blocking ops (BLPOP/BZPOPMIN/…) are modeled as ordinary invoke-window ops — the existing WGL checker already tries every linearization point inside a `[invoke, return]` window, so a blocking hit is legal iff some point in the window leaves the element at the list head, and a timeout-nil is legal iff some point leaves the list empty. Conservation checkers are pure history scans (not WGL). Everything is transport-agnostic (no turmoil types).

**Tech Stack:** Rust, `bytes::Bytes`, `serde`/`serde_json`, `thiserror`, `proptest` (dev-dependency). WGL linearizability checker (existing). `just` build system, `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You may be in a git worktree, not the main checkout. Only edit files under the directory `pwd` reports; use absolute paths.
- **Crate name is `frogdb-testing`** (verified in `frogdb-server/crates/testing/Cargo.toml`). Build commands:
  - Type-check: `just check frogdb-testing`
  - Test (optionally filtered): `just test frogdb-testing <pattern>` (the `test` recipe expands `<pattern>` to `-E 'test(/<pattern>/)'`).
  - Format: `just fmt frogdb-testing`
  - Lint: `just lint frogdb-testing`
- **TDD:** every task writes a failing test first, watches it fail, then writes the minimal implementation.
- **Commit per task** with the exact `git` command shown in the task's final step.
- **No `Co-Authored-By` lines** in any commit message.
- **Do not break existing consumers.** ~50 tests in `frogdb-server/crates/server/tests/simulation.rs` read `result.is_linearizable` and use `check_linearizability::<KVModel>`, plus `KVModel`, `KVState`, `Model`, `History`. After Tasks 1 and 3, run `just check frogdb-server` to confirm they still compile. Keep the existing public names (`History`, `OpKind`, `Operation`, `KVModel`, `KVState`, `Model`, `RegisterModel`, `check_linearizability`, `LinearizabilityResult`).
- **Do NOT change `KVModel`'s permissive `_ => Some(state.clone())` fallthrough** in this phase. New models are STRICT (`_ => None`).
- **Result encodings follow existing conventions:** pipe (`|`) delimited multi-value results, the literal `nil` sentinel for a missing element inside a delimited list, `OK` for simple-string, integers as their decimal string. Match `KVModel`'s `mget`/`exec` encoding exactly.
- **YAGNI:** implement only the ops listed per task. No counts on LPOP/RPOP, no XREADGROUP (deferred), no ZRANGEBYSCORE, etc.

---

### Task 1: Add `inconclusive` field to `LinearizabilityResult`

Callers must distinguish "proven non-linearizable" from "checker gave up at the state bound". Add a `pub inconclusive: bool` (false on every normal path); `check_linearizability_bounded` sets `inconclusive: true` together with `is_linearizable: false` when the state bound is hit — never a silent pass.

**Files:**
- Modify: `frogdb-server/crates/testing/src/checker.rs`

**Interfaces:**
- Produces: `LinearizabilityResult { pub is_linearizable: bool, pub linearization: Option<Vec<u64>>, pub problematic_ops: Vec<u64>, pub states_explored: u64, pub inconclusive: bool }`. `check_linearizability_bounded::<M>(history: &History, max_states: u64) -> LinearizabilityResult`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block at the bottom of `frogdb-server/crates/testing/src/checker.rs`:

```rust
    #[test]
    fn test_bounded_inconclusive_when_state_bound_hit() {
        let mut history = History::new();
        let op1 = history.invoke(1, "set", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        // max_states = 1 forces the search to abort before it can finish.
        let result = check_linearizability_bounded::<KVModel>(&history, 1);
        assert!(!result.is_linearizable, "bounded-out run must not report linearizable");
        assert!(result.inconclusive, "hitting the state bound must set inconclusive");
        assert!(result.problematic_ops.is_empty(), "inconclusive run cannot blame specific ops");
    }

    #[test]
    fn test_normal_run_is_not_inconclusive() {
        let mut history = History::new();
        let op1 = history.invoke(1, "set", vec![Bytes::from("x"), Bytes::from("1")]);
        history.respond(op1, Some(Bytes::from("OK")));

        let result = check_linearizability_bounded::<KVModel>(&history, 1000);
        assert!(result.is_linearizable);
        assert!(!result.inconclusive, "a completed search is conclusive");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-testing test_bounded_inconclusive`
Expected: FAIL — compile error `struct LinearizabilityResult has no field named inconclusive` / `no field inconclusive`.

- [ ] **Step 3: Write minimal implementation**

In `frogdb-server/crates/testing/src/checker.rs`, add the field to the struct definition:

```rust
/// Result of linearizability checking.
#[derive(Debug, Clone)]
pub struct LinearizabilityResult {
    /// Whether the history is linearizable.
    pub is_linearizable: bool,
    /// A valid linearization order (if linearizable).
    pub linearization: Option<Vec<u64>>,
    /// Operations that could not be linearized (if not linearizable).
    pub problematic_ops: Vec<u64>,
    /// Number of states explored during checking.
    pub states_explored: u64,
    /// True when the checker gave up (state bound reached) rather than
    /// proving (non-)linearizability. When true, `is_linearizable` is false
    /// but the history was NOT proven non-linearizable — never a silent pass.
    pub inconclusive: bool,
}
```

Then add `inconclusive: false,` to **every** `LinearizabilityResult { .. }` literal in the file (there are eight normal ones: the incomplete-history and empty-history branches in both `check_linearizability` and `check_linearizability_bounded`, plus the success/failure branches in `Checker::check` and the success/failure branches in `BoundedChecker::check`). Then change the single limit-reached branch in `BoundedChecker::check` to set `inconclusive: true`. After the edit that branch reads exactly:

```rust
        } else if self.limit_reached {
            // Inconclusive - limit reached. Reported as non-linearizable to be
            // conservative, but flagged inconclusive so callers never treat a
            // gave-up run as a pass.
            LinearizabilityResult {
                is_linearizable: false,
                linearization: None,
                problematic_ops: vec![],
                states_explored: self.inner.states_explored,
                inconclusive: true,
            }
        } else {
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `just test frogdb-testing checker`
Expected: PASS (all checker tests, including the two new ones).

- [ ] **Step 5: Verify existing consumers still compile**

Run: `just check frogdb-server`
Expected: PASS — the ~50 simulation tests read `.is_linearizable` only; adding a field does not break them (no external code constructs `LinearizabilityResult`).

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/checker.rs
git commit -m "feat(testing): add inconclusive flag to LinearizabilityResult"
```

---

### Task 2: Add `node` field to `Operation` + `invoke_on_node`

A future replication phase needs per-node history. Add `pub node: Option<String>` with `#[serde(default)]` (so old JSON deserializes to `None`). `History::invoke` keeps its signature and records `node: None`; add an `invoke_on_node` variant.

**Files:**
- Modify: `frogdb-server/crates/testing/src/history.rs`

**Interfaces:**
- Produces: `Operation { .., pub node: Option<String> }`. `History::invoke_on_node(&mut self, client_id: u64, node: impl Into<String>, function: impl Into<String>, args: Vec<Bytes>) -> u64`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/testing/src/history.rs`:

```rust
    #[test]
    fn test_invoke_on_node_records_node() {
        let mut history = History::new();
        let op = history.invoke_on_node(1, "node-a", "write", vec![Bytes::from("x")]);
        history.respond(op, Some(Bytes::from("OK")));

        let invoke = history
            .operations()
            .iter()
            .find(|o| o.kind == OpKind::Invoke)
            .unwrap();
        assert_eq!(invoke.node.as_deref(), Some("node-a"));
    }

    #[test]
    fn test_plain_invoke_has_no_node() {
        let mut history = History::new();
        let op = history.invoke(1, "write", vec![Bytes::from("x")]);
        history.respond(op, None);
        let invoke = history
            .operations()
            .iter()
            .find(|o| o.kind == OpKind::Invoke)
            .unwrap();
        assert_eq!(invoke.node, None);
    }

    #[test]
    fn test_node_defaults_when_absent_in_json() {
        // JSON produced before the `node` field existed must still deserialize.
        let json = r#"{"operations":[{"id":1,"client_id":1,"kind":"Invoke","function":"read","args":["x"],"result":null,"timestamp":0}]}"#;
        let restored = History::from_json(json).unwrap();
        assert_eq!(restored.operations()[0].node, None);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-testing test_invoke_on_node`
Expected: FAIL — compile error `no method named invoke_on_node` / `no field node`.

- [ ] **Step 3: Write minimal implementation**

In `frogdb-server/crates/testing/src/history.rs`, add the field to `Operation` (place it after `timestamp`):

```rust
    /// Logical timestamp for ordering.
    pub timestamp: u64,
    /// Node/replica that served the operation (future replication phase).
    #[serde(default)]
    pub node: Option<String>,
```

Update the `Operation` literal in `History::invoke` to add `node: None,`, and the literal in `History::respond` to carry the node forward. In `respond`, capture the node alongside the other invoke fields:

```rust
        let invoke = &self.operations[invoke_idx];
        let client_id = invoke.client_id;
        let function = invoke.function.clone();
        let args = invoke.args.clone();
        let node = invoke.node.clone();

        let timestamp = self.next_timestamp();

        let op = Operation {
            id: op_id,
            client_id,
            kind: OpKind::Return,
            function,
            args,
            result,
            timestamp,
            node,
        };
```

Add the new method to the `impl History` block, right after `invoke`:

```rust
    /// Record an operation invocation attributed to a specific node/replica.
    ///
    /// Returns the operation ID to be used when recording the response.
    pub fn invoke_on_node(
        &mut self,
        client_id: u64,
        node: impl Into<String>,
        function: impl Into<String>,
        args: Vec<Bytes>,
    ) -> u64 {
        let id = next_op_id();
        let timestamp = self.next_timestamp();

        let op = Operation {
            id,
            client_id,
            kind: OpKind::Invoke,
            function: function.into(),
            args,
            result: None,
            timestamp,
            node: Some(node.into()),
        };

        let idx = self.operations.len();
        self.operations.push(op);
        self.pending.insert(id, idx);

        id
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `just test frogdb-testing history`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frogdb-server/crates/testing/src/history.rs
git commit -m "feat(testing): add node field and invoke_on_node to history"
```

---

### Task 3: Split `models.rs` into a `models/` module directory

Mechanical move, **no behavior change**. `models.rs` (678 lines) becomes `models/mod.rs` (the `Model` trait + re-exports) with `models/register.rs` (`RegisterModel`/`RegisterState`) and `models/kv.rs` (`KVModel`/`KVState`). Their `#[cfg(test)] mod tests` blocks move with them.

**Files:**
- Delete: `frogdb-server/crates/testing/src/models.rs`
- Create: `frogdb-server/crates/testing/src/models/mod.rs`
- Create: `frogdb-server/crates/testing/src/models/register.rs`
- Create: `frogdb-server/crates/testing/src/models/kv.rs`

**Interfaces:**
- Produces (unchanged public surface): trait `Model` (assoc `type State`; `init`, `step`, `check_result`), `RegisterModel`/`RegisterState`, `KVModel`/`KVState`, all still re-exported from `frogdb_testing::models` and (via lib.rs) from `frogdb_testing`.

- [ ] **Step 1: Create `models/mod.rs` with the trait and re-exports**

Create `frogdb-server/crates/testing/src/models/mod.rs`:

```rust
//! Sequential specification models for linearizability checking.
//!
//! A model defines the sequential behavior of a data structure.
//! The linearizability checker uses these models to verify that
//! a concurrent execution could be explained by some sequential ordering.

use bytes::Bytes;

mod kv;
mod register;

pub use kv::{KVModel, KVState};
pub use register::{RegisterModel, RegisterState};

/// A sequential specification model.
///
/// Models define the expected behavior of a data structure when operations
/// are executed sequentially. The linearizability checker uses this to verify
/// that a concurrent history can be linearized to a valid sequential execution.
pub trait Model: Clone + Default {
    /// The state type for this model.
    type State: Clone + Default;

    /// Initial state for the model.
    fn init() -> Self::State {
        Self::State::default()
    }

    /// Execute an operation on the current state.
    ///
    /// Returns `Some(new_state)` if the operation (and its recorded result)
    /// is valid in this state, or `None` if it cannot be applied.
    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State>;

    /// Check if a result matches the expected result for an operation.
    fn check_result(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> bool {
        Self::step(state, function, args, result).is_some()
    }
}
```

- [ ] **Step 2: Move `RegisterModel` into `models/register.rs`**

Create `frogdb-server/crates/testing/src/models/register.rs`. Header:

```rust
//! Single-register sequential model.

use super::Model;
use bytes::Bytes;
```

Then paste, unchanged, the existing items from the old `models.rs`: the `RegisterModel` struct, the `RegisterState` struct, and the entire `impl Model for RegisterModel { .. }` block. Move the register-related tests (`test_register_read_write`, `test_register_cas`) into a `#[cfg(test)] mod tests { use super::*; use bytes::Bytes; .. }` block in this file.

- [ ] **Step 3: Move `KVModel` into `models/kv.rs`**

Create `frogdb-server/crates/testing/src/models/kv.rs`. Header:

```rust
//! Key-value store sequential model (strings + MULTI/EXEC).

use super::Model;
use bytes::Bytes;
use std::collections::HashMap;
```

Then paste, unchanged, the existing `KVModel` struct, `KVState` struct, and the entire `impl Model for KVModel { .. }` block from the old `models.rs` — **including the permissive `_ => Some(state.clone())` fallthrough (do not touch it)**. Move the KV tests (`test_kv_read_write`, `test_kv_mset`, `test_kv_mget_validates_result`, `test_kv_incr`, `test_kv_exec_transaction`, `test_kv_exec_read_own_writes`) into a `#[cfg(test)] mod tests { use super::*; use bytes::Bytes; .. }` block in this file.

- [ ] **Step 4: Delete the old file**

```bash
git rm frogdb-server/crates/testing/src/models.rs
```

`lib.rs` already has `pub mod models;` and `pub use models::{KVModel, KVState, Model, RegisterModel};` — these resolve to `models/mod.rs` unchanged, so no lib.rs edit is needed in this task.

- [ ] **Step 5: Run tests to verify no behavior change**

Run: `just test frogdb-testing models`
Expected: PASS — all moved tests pass (identical logic).

- [ ] **Step 6: Verify the crate and consumers compile**

Run: `just check frogdb-testing && just check frogdb-server`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/testing/src/models frogdb-server/crates/testing/src/lib.rs
git rm --cached frogdb-server/crates/testing/src/models.rs 2>/dev/null; git add -A frogdb-server/crates/testing/src
git commit -m "refactor(testing): split models.rs into models/ module directory"
```

---

### Task 4: `ListModel`

STRICT model for lists: `LPUSH RPUSH LPOP RPOP LMOVE LLEN LRANGE` and blocking `BLPOP BRPOP BLMOVE`. Blocking ops need no special mechanism — WGL tries every linearization point in the `[invoke, return]` window; a hit is legal iff the returned element is the head/tail at that point, a timeout-nil iff the list is empty at that point.

**Files:**
- Create: `frogdb-server/crates/testing/src/models/list.rs`
- Modify: `frogdb-server/crates/testing/src/models/mod.rs`

**Interfaces:**
- Consumes: `Model` trait (Task 3); `check_linearizability` (existing).
- Produces: `ListModel`, `ListState { pub lists: HashMap<Bytes, VecDeque<Bytes>> }`; shared `pub(crate) fn expect_int(result: Option<&Bytes>, expected: i64) -> bool` in `models/mod.rs`.
- Result encodings: length/`LLEN` → decimal int string; `LPOP`/`RPOP`/`LMOVE`/`BLMOVE` → the element bytes, or `None` for empty/timeout; `LRANGE` → `|`-joined elements (`""`/`None` for empty); `BLPOP`/`BRPOP` hit → `"key|elem"`, timeout → `None`.

- [ ] **Step 1: Add the shared `expect_int` helper to `models/mod.rs`**

Append to `frogdb-server/crates/testing/src/models/mod.rs`:

```rust
/// Shared helper: true iff `result` is an integer reply equal to `expected`.
pub(crate) fn expect_int(result: Option<&Bytes>, expected: i64) -> bool {
    result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(expected))
}
```

And add the module wiring near the other `mod`/`pub use` lines:

```rust
mod list;
pub use list::{ListModel, ListState};
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/models/list.rs` with only a test module to start (implementation added in Step 4):

```rust
//! List sequential model (LPUSH/RPUSH/LPOP/RPOP/LMOVE/LLEN/LRANGE + blocking).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn list_happy_path() {
        let s = ListState::default();
        // RPUSH k a  -> len 1
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // RPUSH k b  -> len 2
        let s = ListModel::step(&s, "rpush", &[b("k"), b("b")], Some(&b("2"))).unwrap();
        // LLEN k -> 2
        assert!(ListModel::step(&s, "llen", &[b("k")], Some(&b("2"))).is_some());
        // LRANGE k 0 -1 -> a|b
        assert!(ListModel::step(&s, "lrange", &[b("k"), b("0"), b("-1")], Some(&b("a|b"))).is_some());
        // LPOP k -> a
        let s = ListModel::step(&s, "lpop", &[b("k")], Some(&b("a"))).unwrap();
        // RPOP k -> b
        let s = ListModel::step(&s, "rpop", &[b("k")], Some(&b("b"))).unwrap();
        // LPOP empty -> nil
        assert!(ListModel::step(&s, "lpop", &[b("k")], None).is_some());
    }

    #[test]
    fn list_wrong_pop_rejected() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // Popping a value that isn't the head is illegal.
        assert!(ListModel::step(&s, "lpop", &[b("k")], Some(&b("z"))).is_none());
    }

    #[test]
    fn list_lmove() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("src"), b("x")], Some(&b("1"))).unwrap();
        // LMOVE src dst left right -> x
        let s = ListModel::step(&s, "lmove", &[b("src"), b("dst"), b("left"), b("right")], Some(&b("x"))).unwrap();
        assert!(ListModel::step(&s, "llen", &[b("src")], Some(&b("0"))).is_some());
        assert!(ListModel::step(&s, "lpop", &[b("dst")], Some(&b("x"))).is_some());
    }

    #[test]
    fn list_strict_rejects_unknown() {
        let s = ListState::default();
        assert!(ListModel::step(&s, "sadd", &[b("k"), b("v")], Some(&b("1"))).is_none());
    }

    #[test]
    fn blpop_timeout_legal_when_empty() {
        let s = ListState::default();
        // Empty list -> BLPOP may time out (nil).
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], None).is_some());
        // Empty list -> BLPOP cannot return an element.
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], Some(&b("k|a"))).is_none());
    }

    #[test]
    fn blpop_hit_legal_when_head_matches() {
        let s = ListState::default();
        let s = ListModel::step(&s, "rpush", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        // Non-empty list -> BLPOP must serve the head, and cannot time out here.
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], Some(&b("k|a"))).is_some());
        assert!(ListModel::step(&s, "blpop", &[b("k"), b("0")], None).is_none());
    }

    #[test]
    fn blpop_window_linearizable() {
        // A BLPOP whose window overlaps the RPUSH that supplies its element
        // is linearizable (linearization point after the push).
        let mut h = History::new();
        let blk = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        let push = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(push, Some(b("1")));
        h.respond(blk, Some(b("k|a")));
        let r = check_linearizability::<ListModel>(&h);
        assert!(r.is_linearizable);
    }

    #[test]
    fn blpop_timeout_with_element_present_not_linearizable() {
        // Element pushed and never removed, yet a fully-concurrent-after BLPOP
        // times out: no linearization point leaves the list empty for it.
        let mut h = History::new();
        let push = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(push, Some(b("1")));
        let blk = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(blk, None); // timeout despite a present element
        let r = check_linearizability::<ListModel>(&h);
        assert!(!r.is_linearizable);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing list_`
Expected: FAIL — compile error `cannot find type ListModel` / `cannot find value ListState`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/models/list.rs` (above the `#[cfg(test)]` module):

```rust
use super::{expect_int, Model};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

/// Sequential model for Redis lists.
#[derive(Debug, Clone, Default)]
pub struct ListModel;

/// State for the list model.
#[derive(Debug, Clone, Default)]
pub struct ListState {
    /// key -> ordered elements (front = head).
    pub lists: HashMap<Bytes, VecDeque<Bytes>>,
}

impl ListState {
    fn len(&self, key: &Bytes) -> usize {
        self.lists.get(key).map_or(0, VecDeque::len)
    }

    fn is_empty_list(&self, key: &Bytes) -> bool {
        self.len(key) == 0
    }

    /// Drop the key if its list became empty (keeps state canonical).
    fn prune(&mut self, key: &Bytes) {
        if self.lists.get(key).is_some_and(VecDeque::is_empty) {
            self.lists.remove(key);
        }
    }
}

fn side(arg: &Bytes) -> Option<bool> {
    // true = left/head, false = right/tail.
    match String::from_utf8_lossy(arg).to_lowercase().as_str() {
        "left" => Some(true),
        "right" => Some(false),
        _ => None,
    }
}

impl Model for ListModel {
    type State = ListState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "lpush" | "rpush" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let list = new.lists.entry(key.clone()).or_default();
                for v in &args[1..] {
                    if function == "lpush" {
                        list.push_front(v.clone());
                    } else {
                        list.push_back(v.clone());
                    }
                }
                let len = list.len() as i64;
                if expect_int(result, len) {
                    Some(new)
                } else {
                    None
                }
            }
            "lpop" | "rpop" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let popped = new.lists.get_mut(key).and_then(|l| {
                    if function == "lpop" {
                        l.pop_front()
                    } else {
                        l.pop_back()
                    }
                });
                match popped {
                    Some(elem) => {
                        new.prune(key);
                        if result == Some(&elem) {
                            Some(new)
                        } else {
                            None
                        }
                    }
                    None => {
                        if result.is_none() {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            "llen" => {
                if args.is_empty() {
                    return None;
                }
                if expect_int(result, state.len(&args[0]) as i64) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "lrange" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let start: i64 = String::from_utf8_lossy(&args[1]).parse().ok()?;
                let stop: i64 = String::from_utf8_lossy(&args[2]).parse().ok()?;
                let empty = VecDeque::new();
                let list = state.lists.get(key).unwrap_or(&empty);
                let len = list.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start };
                let e_raw = if stop < 0 { len + stop } else { stop };
                let e = e_raw.min(len - 1);
                let mut elems: Vec<String> = Vec::new();
                if s <= e && s < len {
                    for idx in s..=e {
                        elems.push(String::from_utf8_lossy(&list[idx as usize]).to_string());
                    }
                }
                let expected = elems.join("|");
                match result {
                    Some(r) if String::from_utf8_lossy(r) == expected => Some(state.clone()),
                    None if expected.is_empty() => Some(state.clone()),
                    _ => None,
                }
            }
            "lmove" => {
                if args.len() < 4 {
                    return None;
                }
                let (src, dst) = (&args[0], &args[1]);
                let from = side(&args[2])?;
                let to = side(&args[3])?;
                let mut new = state.clone();
                let elem = new.lists.get_mut(src).and_then(|l| {
                    if from {
                        l.pop_front()
                    } else {
                        l.pop_back()
                    }
                });
                match elem {
                    Some(e) => {
                        new.prune(src);
                        let d = new.lists.entry(dst.clone()).or_default();
                        if to {
                            d.push_front(e.clone());
                        } else {
                            d.push_back(e.clone());
                        }
                        if result == Some(&e) {
                            Some(new)
                        } else {
                            None
                        }
                    }
                    None => {
                        if result.is_none() {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                }
            }
            "blpop" | "brpop" => {
                if args.len() < 2 {
                    return None;
                }
                let keys = &args[..args.len() - 1];
                let first = keys.iter().find(|k| !state.is_empty_list(k));
                match (first, result) {
                    (None, None) => Some(state.clone()),
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(k), Some(r)) => {
                        let mut new = state.clone();
                        let elem = new.lists.get_mut(k).and_then(|l| {
                            if function == "blpop" {
                                l.pop_front()
                            } else {
                                l.pop_back()
                            }
                        })?;
                        new.prune(k);
                        let expected = format!(
                            "{}|{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(&elem)
                        );
                        if String::from_utf8_lossy(r) == expected {
                            Some(new)
                        } else {
                            None
                        }
                    }
                }
            }
            "blmove" => {
                if args.len() < 5 {
                    return None;
                }
                let (src, dst) = (&args[0], &args[1]);
                let from = side(&args[2])?;
                let to = side(&args[3])?;
                if state.is_empty_list(src) {
                    return if result.is_none() {
                        Some(state.clone())
                    } else {
                        None
                    };
                }
                let mut new = state.clone();
                let elem = new
                    .lists
                    .get_mut(src)
                    .and_then(|l| if from { l.pop_front() } else { l.pop_back() })?;
                new.prune(src);
                let d = new.lists.entry(dst.clone()).or_default();
                if to {
                    d.push_front(elem.clone());
                } else {
                    d.push_back(elem.clone());
                }
                if result == Some(&elem) {
                    Some(new)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing list_ && just test frogdb-testing blpop`
Expected: PASS.

- [ ] **Step 6: Add a proptest round-trip property**

Append this proptest to the `#[cfg(test)] mod tests` block in `list.rs` (proptest is already a dev-dependency):

```rust
    use proptest::prelude::*;

    proptest! {
        /// RPUSH-ing then LPOP-ing every element in FIFO order always stays
        /// consistent with a VecDeque reference implementation.
        #[test]
        fn rpush_then_lpop_matches_reference(vals in proptest::collection::vec("[a-z]{1,3}", 0..12)) {
            let mut state = ListState::default();
            let mut reference: std::collections::VecDeque<Bytes> = std::collections::VecDeque::new();
            for (i, v) in vals.iter().enumerate() {
                let vb = Bytes::from(v.clone());
                reference.push_back(vb.clone());
                let len = (i + 1) as i64;
                state = ListModel::step(&state, "rpush", &[b("k"), vb], Some(&Bytes::from(len.to_string())))
                    .expect("rpush must apply");
            }
            while let Some(expected) = reference.pop_front() {
                state = ListModel::step(&state, "lpop", &[b("k")], Some(&expected))
                    .expect("lpop must match FIFO head");
            }
            prop_assert!(ListModel::step(&state, "lpop", &[b("k")], None).is_some());
        }
    }
```

Run: `just test frogdb-testing rpush_then_lpop`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/testing/src/models
git commit -m "feat(testing): add strict ListModel with blocking-op window semantics"
```

---

### Task 5: `HashModel`

STRICT model for hashes: `HSET HDEL HGET HINCRBY HGETALL HLEN`.

**Files:**
- Create: `frogdb-server/crates/testing/src/models/hash.rs`
- Modify: `frogdb-server/crates/testing/src/models/mod.rs`

**Interfaces:**
- Consumes: `Model`, `expect_int` (Task 4).
- Produces: `HashModel`, `HashState { pub hashes: HashMap<Bytes, HashMap<Bytes, Bytes>> }`.
- Encodings: `HSET`/`HDEL` → count of new/removed fields (int); `HGET` → value or `None`; `HINCRBY` → new int; `HGETALL` → fields sorted by name, `|`-joined `f|v|f|v` (`""`/`None` empty); `HLEN` → int.

- [ ] **Step 1: Add module wiring**

Add to `frogdb-server/crates/testing/src/models/mod.rs` near the other `mod`/`pub use` lines:

```rust
mod hash;
pub use hash::{HashModel, HashState};
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/models/hash.rs`:

```rust
//! Hash sequential model (HSET/HDEL/HGET/HINCRBY/HGETALL/HLEN).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn hash_happy_path() {
        let s = HashState::default();
        // HSET h f1 v1 f2 v2 -> 2 new fields
        let s = HashModel::step(&s, "hset", &[b("h"), b("f1"), b("v1"), b("f2"), b("v2")], Some(&b("2"))).unwrap();
        // HGET h f1 -> v1
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], Some(&b("v1"))).is_some());
        // HLEN -> 2
        assert!(HashModel::step(&s, "hlen", &[b("h")], Some(&b("2"))).is_some());
        // HGETALL sorted -> f1|v1|f2|v2
        assert!(HashModel::step(&s, "hgetall", &[b("h")], Some(&b("f1|v1|f2|v2"))).is_some());
        // HSET existing field -> 0 new
        let s = HashModel::step(&s, "hset", &[b("h"), b("f1"), b("v9")], Some(&b("0"))).unwrap();
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], Some(&b("v9"))).is_some());
        // HDEL h f1 -> 1 removed
        let s = HashModel::step(&s, "hdel", &[b("h"), b("f1")], Some(&b("1"))).unwrap();
        assert!(HashModel::step(&s, "hget", &[b("h"), b("f1")], None).is_some());
    }

    #[test]
    fn hash_hincrby() {
        let s = HashState::default();
        let s = HashModel::step(&s, "hincrby", &[b("h"), b("n"), b("5")], Some(&b("5"))).unwrap();
        assert!(HashModel::step(&s, "hincrby", &[b("h"), b("n"), b("-2")], Some(&b("3"))).is_some());
    }

    #[test]
    fn hash_strict_rejects_unknown() {
        let s = HashState::default();
        assert!(HashModel::step(&s, "get", &[b("h")], Some(&b("v"))).is_none());
    }

    #[test]
    fn hash_wrong_result_rejected() {
        let s = HashState::default();
        assert!(HashModel::step(&s, "hset", &[b("h"), b("f"), b("v")], Some(&b("0"))).is_none());
    }

    #[test]
    fn hash_linearizable() {
        let mut h = History::new();
        let w = h.invoke(1, "hset", vec![b("h"), b("f"), b("v")]);
        h.respond(w, Some(b("1")));
        let r = h.invoke(2, "hget", vec![b("h"), b("f")]);
        h.respond(r, Some(b("v")));
        assert!(check_linearizability::<HashModel>(&h).is_linearizable);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing hash_`
Expected: FAIL — `cannot find type HashModel`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/models/hash.rs`:

```rust
use super::{expect_int, Model};
use bytes::Bytes;
use std::collections::HashMap;

/// Sequential model for Redis hashes.
#[derive(Debug, Clone, Default)]
pub struct HashModel;

/// State for the hash model.
#[derive(Debug, Clone, Default)]
pub struct HashState {
    /// key -> (field -> value).
    pub hashes: HashMap<Bytes, HashMap<Bytes, Bytes>>,
}

impl Model for HashModel {
    type State = HashState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "hset" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let h = new.hashes.entry(key.clone()).or_default();
                let mut added = 0i64;
                let mut i = 1;
                while i + 1 < args.len() {
                    if h.insert(args[i].clone(), args[i + 1].clone()).is_none() {
                        added += 1;
                    }
                    i += 2;
                }
                if expect_int(result, added) {
                    Some(new)
                } else {
                    None
                }
            }
            "hdel" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(h) = new.hashes.get_mut(key) {
                    for f in &args[1..] {
                        if h.remove(f).is_some() {
                            removed += 1;
                        }
                    }
                    if h.is_empty() {
                        new.hashes.remove(key);
                    }
                }
                if expect_int(result, removed) {
                    Some(new)
                } else {
                    None
                }
            }
            "hget" => {
                if args.len() < 2 {
                    return None;
                }
                let val = state.hashes.get(&args[0]).and_then(|h| h.get(&args[1]));
                if result == val {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "hincrby" => {
                if args.len() < 3 {
                    return None;
                }
                let (key, field) = (&args[0], &args[1]);
                let delta: i64 = String::from_utf8_lossy(&args[2]).parse().ok()?;
                let cur = state
                    .hashes
                    .get(key)
                    .and_then(|h| h.get(field))
                    .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                    .unwrap_or(0);
                let nv = cur + delta;
                let mut new = state.clone();
                new.hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), Bytes::from(nv.to_string()));
                if expect_int(result, nv) {
                    Some(new)
                } else {
                    None
                }
            }
            "hgetall" => {
                if args.is_empty() {
                    return None;
                }
                let mut pairs: Vec<(String, String)> = state
                    .hashes
                    .get(&args[0])
                    .map(|h| {
                        h.iter()
                            .map(|(f, v)| {
                                (
                                    String::from_utf8_lossy(f).to_string(),
                                    String::from_utf8_lossy(v).to_string(),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                pairs.sort();
                let expected = pairs
                    .into_iter()
                    .flat_map(|(f, v)| [f, v])
                    .collect::<Vec<_>>()
                    .join("|");
                match result {
                    Some(r) if String::from_utf8_lossy(r) == expected => Some(state.clone()),
                    None if expected.is_empty() => Some(state.clone()),
                    _ => None,
                }
            }
            "hlen" => {
                if args.is_empty() {
                    return None;
                }
                let len = state.hashes.get(&args[0]).map_or(0, HashMap::len) as i64;
                if expect_int(result, len) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing hash_`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/models
git commit -m "feat(testing): add strict HashModel"
```

---

### Task 6: `ZSetModel`

STRICT model for sorted sets: `ZADD ZREM ZSCORE ZCARD` and blocking `BZPOPMIN BZPOPMAX` (same invoke-window treatment as list blocking ops).

**Files:**
- Create: `frogdb-server/crates/testing/src/models/zset.rs`
- Modify: `frogdb-server/crates/testing/src/models/mod.rs`

**Interfaces:**
- Consumes: `Model`, `expect_int`.
- Produces: `ZSetModel`, `ZSetState { pub zsets: HashMap<Bytes, HashMap<Bytes, f64>> }`; `pub(crate) fn fmt_score(s: f64) -> String` in `models/mod.rs`.
- Encodings: `ZADD` → count of new members (int); `ZREM` → count removed (int); `ZSCORE` → `fmt_score` string or `None`; `ZCARD` → int; `BZPOPMIN`/`BZPOPMAX` hit → `"key|member|score"`, timeout → `None`. On score ties the lexicographically smallest member is popped for both min and max (`// DIVERGENCE:` documented simplification; tests avoid ties).

- [ ] **Step 1: Add `fmt_score` helper and module wiring to `models/mod.rs`**

Append the helper:

```rust
/// Format a zset score the way integer-valued scores render (no trailing `.0`).
pub(crate) fn fmt_score(s: f64) -> String {
    if s.is_finite() && s.fract() == 0.0 {
        format!("{}", s as i64)
    } else {
        format!("{s}")
    }
}
```

Add wiring:

```rust
mod zset;
pub use zset::{ZSetModel, ZSetState};
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/models/zset.rs`:

```rust
//! Sorted-set sequential model (ZADD/ZREM/ZSCORE/ZCARD + blocking BZPOP).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn zset_happy_path() {
        let s = ZSetState::default();
        // ZADD z 1 a 2 b -> 2 new
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("1"), b("a"), b("2"), b("b")], Some(&b("2"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], Some(&b("1"))).is_some());
        assert!(ZSetModel::step(&s, "zcard", &[b("z")], Some(&b("2"))).is_some());
        // Re-add existing member with new score -> 0 new
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("5"), b("a")], Some(&b("0"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], Some(&b("5"))).is_some());
        // ZREM -> 1
        let s = ZSetModel::step(&s, "zrem", &[b("z"), b("a")], Some(&b("1"))).unwrap();
        assert!(ZSetModel::step(&s, "zscore", &[b("z"), b("a")], None).is_some());
    }

    #[test]
    fn bzpopmin_selects_lowest_score() {
        let s = ZSetState::default();
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("2"), b("b"), b("1"), b("a")], Some(&b("2"))).unwrap();
        // BZPOPMIN z 0 -> z|a|1
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
        // Popping the wrong (non-min) member is illegal.
        assert!(ZSetModel::step(&s, "bzpopmin", &[b("z"), b("0")], Some(&b("z|b|2"))).is_none());
    }

    #[test]
    fn bzpopmax_timeout_only_when_empty() {
        let s = ZSetState::default();
        // Empty -> timeout legal.
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], None).is_some());
        let s = ZSetModel::step(&s, "zadd", &[b("z"), b("1"), b("a")], Some(&b("1"))).unwrap();
        // Non-empty -> cannot time out.
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], None).is_none());
        assert!(ZSetModel::step(&s, "bzpopmax", &[b("z"), b("0")], Some(&b("z|a|1"))).is_some());
    }

    #[test]
    fn zset_strict_rejects_unknown() {
        let s = ZSetState::default();
        assert!(ZSetModel::step(&s, "sadd", &[b("z"), b("a")], Some(&b("1"))).is_none());
    }

    #[test]
    fn bzpopmin_window_linearizable() {
        let mut h = History::new();
        let blk = h.invoke(2, "bzpopmin", vec![b("z"), b("0")]);
        let add = h.invoke(1, "zadd", vec![b("z"), b("1"), b("a")]);
        h.respond(add, Some(b("1")));
        h.respond(blk, Some(b("z|a|1")));
        assert!(check_linearizability::<ZSetModel>(&h).is_linearizable);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing zset_ && just test frogdb-testing bzpop`
Expected: FAIL — `cannot find type ZSetModel`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/models/zset.rs`:

```rust
use super::{expect_int, fmt_score, Model};
use bytes::Bytes;
use std::collections::HashMap;

/// Sequential model for Redis sorted sets.
#[derive(Debug, Clone, Default)]
pub struct ZSetModel;

/// State for the sorted-set model.
#[derive(Debug, Clone, Default)]
pub struct ZSetState {
    /// key -> (member -> score).
    pub zsets: HashMap<Bytes, HashMap<Bytes, f64>>,
}

impl Model for ZSetModel {
    type State = ZSetState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "zadd" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let z = new.zsets.entry(key.clone()).or_default();
                let mut added = 0i64;
                let mut i = 1;
                while i + 1 < args.len() {
                    let score: f64 = String::from_utf8_lossy(&args[i]).parse().ok()?;
                    let member = args[i + 1].clone();
                    if z.insert(member, score).is_none() {
                        added += 1;
                    }
                    i += 2;
                }
                if expect_int(result, added) {
                    Some(new)
                } else {
                    None
                }
            }
            "zrem" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(z) = new.zsets.get_mut(key) {
                    for m in &args[1..] {
                        if z.remove(m).is_some() {
                            removed += 1;
                        }
                    }
                    if z.is_empty() {
                        new.zsets.remove(key);
                    }
                }
                if expect_int(result, removed) {
                    Some(new)
                } else {
                    None
                }
            }
            "zscore" => {
                if args.len() < 2 {
                    return None;
                }
                let score = state.zsets.get(&args[0]).and_then(|z| z.get(&args[1]));
                match (score, result) {
                    (Some(s), Some(r)) if String::from_utf8_lossy(r) == fmt_score(*s) => {
                        Some(state.clone())
                    }
                    (None, None) => Some(state.clone()),
                    _ => None,
                }
            }
            "zcard" => {
                if args.is_empty() {
                    return None;
                }
                let card = state.zsets.get(&args[0]).map_or(0, HashMap::len) as i64;
                if expect_int(result, card) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "bzpopmin" | "bzpopmax" => {
                if args.len() < 2 {
                    return None;
                }
                let keys = &args[..args.len() - 1];
                let want_min = function == "bzpopmin";
                let first = keys
                    .iter()
                    .find(|k| state.zsets.get(*k).is_some_and(|z| !z.is_empty()));
                match (first, result) {
                    (None, None) => Some(state.clone()),
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(k), Some(r)) => {
                        let z = state.zsets.get(k)?;
                        // DIVERGENCE: on score ties we pop the lexicographically
                        // smallest member for both MIN and MAX (Redis' exact tie
                        // rule is not modeled in phase 1; workloads avoid ties).
                        let (member, score) = z
                            .iter()
                            .min_by(|(am, asc), (bm, bsc)| {
                                let ord = asc
                                    .partial_cmp(bsc)
                                    .unwrap_or(std::cmp::Ordering::Equal);
                                let ord = if want_min { ord } else { ord.reverse() };
                                ord.then_with(|| am.cmp(bm))
                            })
                            .map(|(m, s)| (m.clone(), *s))?;
                        let mut new = state.clone();
                        let zz = new.zsets.get_mut(k)?;
                        zz.remove(&member);
                        if zz.is_empty() {
                            new.zsets.remove(k);
                        }
                        let expected = format!(
                            "{}|{}|{}",
                            String::from_utf8_lossy(k),
                            String::from_utf8_lossy(&member),
                            fmt_score(score)
                        );
                        if String::from_utf8_lossy(r) == expected {
                            Some(new)
                        } else {
                            None
                        }
                    }
                }
            }
            _ => None,
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing zset_ && just test frogdb-testing bzpop`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/models
git commit -m "feat(testing): add strict ZSetModel with blocking BZPOP window semantics"
```

---

### Task 7: `StreamModel`

STRICT model for streams: `XADD` (auto `*` and explicit `ms-seq` ids), `XLEN`, `XREAD` (non-group; `XREADGROUP` is deferred to a later phase). `XREAD BLOCK` needs no special case — the same `xread` step handles a blocking hit (non-empty result) and a blocking timeout (`None`) because the checker chooses the linearization point.

**Files:**
- Create: `frogdb-server/crates/testing/src/models/stream.rs`
- Modify: `frogdb-server/crates/testing/src/models/mod.rs`

**Interfaces:**
- Consumes: `Model`.
- Produces: `StreamModel`, `StreamState { pub streams: HashMap<Bytes, StreamData> }`, `StreamData { pub entries: Vec<(StreamId, Vec<Bytes>)>, pub last_id: StreamId }`, `StreamId { pub ms: u64, pub seq: u64 }`.
- Encodings: `XADD` result is the id string (`"ms-seq"`), authoritative — the model validates it is `> last_id` and (for explicit ids) equals the requested id. `XLEN` → int. `XREAD key after_id` → entries with id `> after_id`, each as `id,field,value,field,value`, entries `|`-joined; empty → `None`.

- [ ] **Step 1: Add module wiring to `models/mod.rs`**

```rust
mod stream;
pub use stream::{StreamData, StreamId, StreamModel, StreamState};
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/models/stream.rs`:

```rust
//! Stream sequential model (XADD/XLEN/XREAD; XREADGROUP deferred).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn stream_explicit_id_happy_path() {
        let s = StreamState::default();
        // XADD st 1-1 f v -> "1-1"
        let s = StreamModel::step(&s, "xadd", &[b("st"), b("1-1"), b("f"), b("v")], Some(&b("1-1"))).unwrap();
        assert!(StreamModel::step(&s, "xlen", &[b("st")], Some(&b("1"))).is_some());
        // XREAD st 0 -> "1-1,f,v"
        assert!(StreamModel::step(&s, "xread", &[b("st"), b("0")], Some(&b("1-1,f,v"))).is_some());
        // XREAD after the only entry -> nil
        assert!(StreamModel::step(&s, "xread", &[b("st"), b("1-1")], None).is_some());
    }

    #[test]
    fn stream_auto_id_must_increase() {
        let s = StreamState::default();
        let s = StreamModel::step(&s, "xadd", &[b("st"), b("*"), b("f"), b("v")], Some(&b("5-0"))).unwrap();
        // A returned id <= last is illegal.
        assert!(StreamModel::step(&s, "xadd", &[b("st"), b("*"), b("f"), b("v")], Some(&b("5-0"))).is_none());
        // A strictly larger id is fine.
        assert!(StreamModel::step(&s, "xadd", &[b("st"), b("*"), b("f"), b("v")], Some(&b("6-0"))).is_some());
    }

    #[test]
    fn stream_explicit_id_must_match_result() {
        let s = StreamState::default();
        assert!(StreamModel::step(&s, "xadd", &[b("st"), b("2-2"), b("f"), b("v")], Some(&b("3-3"))).is_none());
    }

    #[test]
    fn stream_strict_rejects_unknown() {
        let s = StreamState::default();
        assert!(StreamModel::step(&s, "xrange", &[b("st")], Some(&b("x"))).is_none());
    }

    #[test]
    fn stream_linearizable() {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let r = h.invoke(2, "xread", vec![b("st"), b("0")]);
        h.respond(r, Some(b("1-1,f,v")));
        assert!(check_linearizability::<StreamModel>(&h).is_linearizable);
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing stream_`
Expected: FAIL — `cannot find type StreamModel`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/models/stream.rs`:

```rust
use super::Model;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;

/// A stream entry id (`ms-seq`). Ordering is (ms, seq) lexicographically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct StreamId {
    /// Milliseconds component.
    pub ms: u64,
    /// Sequence component.
    pub seq: u64,
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Parse an explicit `ms` or `ms-seq` id. `*` and unparsable ids return None.
fn parse_id(bytes: &[u8]) -> Option<StreamId> {
    let s = String::from_utf8_lossy(bytes);
    if s == "*" {
        return None;
    }
    let mut it = s.splitn(2, '-');
    let ms = it.next()?.parse::<u64>().ok()?;
    let seq = match it.next() {
        Some(x) => x.parse::<u64>().ok()?,
        None => 0,
    };
    Some(StreamId { ms, seq })
}

/// Per-key stream contents.
#[derive(Debug, Clone, Default)]
pub struct StreamData {
    /// Entries in id order.
    pub entries: Vec<(StreamId, Vec<Bytes>)>,
    /// Highest id assigned so far.
    pub last_id: StreamId,
}

/// Sequential model for Redis streams.
#[derive(Debug, Clone, Default)]
pub struct StreamModel;

/// State for the stream model.
#[derive(Debug, Clone, Default)]
pub struct StreamState {
    /// key -> stream data.
    pub streams: HashMap<Bytes, StreamData>,
}

impl Model for StreamModel {
    type State = StreamState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "xadd" => {
                // args: key id field value [field value...]
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let id_arg = &args[1];
                let fields: Vec<Bytes> = args[2..].to_vec();
                // XADD always returns the assigned id; it is authoritative.
                let assigned = parse_id(result?.as_ref())?;
                let last = state
                    .streams
                    .get(key)
                    .map(|s| s.last_id)
                    .unwrap_or_default();
                if assigned <= last {
                    return None;
                }
                if id_arg.as_ref() != b"*" {
                    let requested = parse_id(id_arg)?;
                    if requested != assigned {
                        return None;
                    }
                }
                let mut new = state.clone();
                let sd = new.streams.entry(key.clone()).or_default();
                sd.entries.push((assigned, fields));
                sd.last_id = assigned;
                Some(new)
            }
            "xlen" => {
                if args.is_empty() {
                    return None;
                }
                let len = state.streams.get(&args[0]).map_or(0, |s| s.entries.len()) as i64;
                if result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(len))
                {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xread" => {
                // args: key after_id  (non-group; blocking hit or timeout)
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let after = parse_id(&args[1])?;
                let entries: Vec<String> = state
                    .streams
                    .get(key)
                    .map(|s| {
                        s.entries
                            .iter()
                            .filter(|(id, _)| *id > after)
                            .map(|(id, fields)| {
                                let mut parts = vec![id.to_string()];
                                parts.extend(
                                    fields.iter().map(|f| String::from_utf8_lossy(f).to_string()),
                                );
                                parts.join(",")
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                match result {
                    None if entries.is_empty() => Some(state.clone()),
                    Some(r) if !entries.is_empty()
                        && String::from_utf8_lossy(r) == entries.join("|") =>
                    {
                        Some(state.clone())
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing stream_`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/models
git commit -m "feat(testing): add strict StreamModel (XADD/XLEN/XREAD non-group)"
```

---

### Task 8: `partition` module — `partition_by_key` + `default_keys_of` + multi-key explode

Split a whole history into per-key sub-histories so each key can be checked independently with WGL against its type model. Multi-key atomic ops (MSET, MGET, EXEC) are exploded into per-key atomic sub-ops that share the parent op's invoke/complete window (Tier-2 invariant). Aborted EXECs (nil result) produce no per-key sub-ops.

**Files:**
- Create: `frogdb-server/crates/testing/src/partition.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Consumes: `History`, `OpKind` (history.rs).
- Produces: `pub fn partition_by_key(history: &History, keys_of: impl Fn(&str, &[Bytes]) -> Vec<Bytes>) -> HashMap<Bytes, History>`; `pub fn default_keys_of(function: &str, args: &[Bytes]) -> Vec<Bytes>`; `pub(crate) fn parse_exec_commands(args: &[Bytes]) -> Option<Vec<(String, Vec<Bytes>)>>` (reused by Task 9).

- [ ] **Step 1: Declare the module in lib.rs**

Add to `frogdb-server/crates/testing/src/lib.rs`, next to the other `pub mod` lines:

```rust
pub mod partition;
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/partition.rs`:

```rust
//! Per-key history partitioning for scalable linearizability checking.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use crate::models::KVModel;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    #[test]
    fn default_keys_of_single_and_multi() {
        assert_eq!(default_keys_of("set", &[b("k"), b("v")]), vec![b("k")]);
        assert_eq!(default_keys_of("mget", &[b("a"), b("c")]), vec![b("a"), b("c")]);
        assert_eq!(default_keys_of("mset", &[b("a"), b("1"), b("c"), b("2")]), vec![b("a"), b("c")]);
        assert_eq!(default_keys_of("blpop", &[b("k1"), b("k2"), b("0")]), vec![b("k1"), b("k2")]);
        assert_eq!(default_keys_of("lmove", &[b("s"), b("d"), b("left"), b("right")]), vec![b("s"), b("d")]);
    }

    #[test]
    fn partition_splits_independent_keys() {
        let mut h = History::new();
        let s1 = h.invoke(1, "set", vec![b("a"), b("1")]);
        h.respond(s1, Some(b("OK")));
        let s2 = h.invoke(1, "set", vec![b("c"), b("2")]);
        h.respond(s2, Some(b("OK")));
        let g = h.invoke(2, "get", vec![b("a")]);
        h.respond(g, Some(b("1")));

        let parts = partition_by_key(&h, default_keys_of);
        assert_eq!(parts.len(), 2);
        for sub in parts.values() {
            assert!(check_linearizability::<KVModel>(sub).is_linearizable);
        }
        // key "a" got 2 ops (set + get), key "c" got 1 (set).
        assert_eq!(parts[&b("a")].completed_operations().len(), 2);
        assert_eq!(parts[&b("c")].completed_operations().len(), 1);
    }

    #[test]
    fn partition_explodes_mset() {
        let mut h = History::new();
        let m = h.invoke(1, "mset", vec![b("a"), b("1"), b("c"), b("2")]);
        h.respond(m, Some(b("OK")));

        let parts = partition_by_key(&h, default_keys_of);
        // Each key sees an atomic `set key val` sub-op returning OK.
        let a = &parts[&b("a")];
        let ops = a.completed_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].function, "set");
        assert_eq!(ops[0].args, vec![b("a"), b("1")]);
        assert!(check_linearizability::<KVModel>(a).is_linearizable);
    }

    #[test]
    fn partition_explodes_mget() {
        let mut h = History::new();
        let s = h.invoke(1, "set", vec![b("a"), b("1")]);
        h.respond(s, Some(b("OK")));
        let m = h.invoke(2, "mget", vec![b("a"), b("c")]);
        h.respond(m, Some(b("1|nil")));

        let parts = partition_by_key(&h, default_keys_of);
        assert!(check_linearizability::<KVModel>(&parts[&b("a")]).is_linearizable);
        // key "c" sub-op is `get c -> nil`.
        let c_ops = parts[&b("c")].completed_operations();
        assert_eq!(c_ops[0].function, "get");
        assert_eq!(c_ops[0].result, None);
    }

    #[test]
    fn partition_explodes_committed_exec_per_key() {
        // EXEC: SET a 1, SET c 2  -> "OK|OK"
        let mut h = History::new();
        let e = h.invoke(1, "exec", vec![
            b("2"), b("set"), b("2"), b("a"), b("1"),
            b("set"), b("2"), b("c"), b("2"),
        ]);
        h.respond(e, Some(b("OK|OK")));

        let parts = partition_by_key(&h, default_keys_of);
        let a_ops = parts[&b("a")].completed_operations();
        assert_eq!(a_ops[0].function, "exec");
        // Sub-exec re-encoded with only key "a"'s command, result "OK".
        assert_eq!(a_ops[0].result, Some(b("OK")));
        assert!(check_linearizability::<KVModel>(&parts[&b("a")]).is_linearizable);
    }

    #[test]
    fn partition_skips_aborted_exec() {
        let mut h = History::new();
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("a"), b("1")]);
        h.respond(e, None); // aborted (nil)
        let parts = partition_by_key(&h, default_keys_of);
        assert!(parts.get(&b("a")).is_none() || parts[&b("a")].completed_operations().is_empty());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing partition`
Expected: FAIL — `cannot find function partition_by_key`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/partition.rs`:

```rust
use crate::history::{History, OpKind};
use bytes::Bytes;
use std::collections::HashMap;

/// Parse the `exec` argument encoding into `(command_name, command_args)` pairs.
///
/// Encoding: `[num_cmds, name1, num_args1, args1..., name2, ...]`.
pub(crate) fn parse_exec_commands(args: &[Bytes]) -> Option<Vec<(String, Vec<Bytes>)>> {
    let num: usize = String::from_utf8_lossy(args.first()?).parse().ok()?;
    let mut idx = 1;
    let mut cmds = Vec::with_capacity(num);
    for _ in 0..num {
        let name = String::from_utf8_lossy(args.get(idx)?).to_lowercase();
        idx += 1;
        let na: usize = String::from_utf8_lossy(args.get(idx)?).parse().ok()?;
        idx += 1;
        let mut cargs = Vec::with_capacity(na);
        for _ in 0..na {
            cargs.push(args.get(idx)?.clone());
            idx += 1;
        }
        cmds.push((name, cargs));
    }
    Some(cmds)
}

/// Distinct keys (first-seen order) touched by an `exec` op's sub-commands.
fn exec_keys(args: &[Bytes]) -> Vec<Bytes> {
    let mut keys = Vec::new();
    if let Some(cmds) = parse_exec_commands(args) {
        for (_name, cargs) in cmds {
            if let Some(k) = cargs.first() {
                if !keys.contains(k) {
                    keys.push(k.clone());
                }
            }
        }
    }
    keys
}

/// Default key-extraction covering the phase-1 op vocabulary.
pub fn default_keys_of(function: &str, args: &[Bytes]) -> Vec<Bytes> {
    match function {
        "mget" => args.to_vec(),
        "mset" => args.iter().step_by(2).cloned().collect(),
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            if args.len() >= 2 {
                args[..args.len() - 1].to_vec()
            } else {
                Vec::new()
            }
        }
        "lmove" | "blmove" => args.iter().take(2).cloned().collect(),
        "exec" => exec_keys(args),
        "get" | "set" | "read" | "write" | "cas" | "del" | "delete" | "incr" | "lpush"
        | "rpush" | "lpop" | "rpop" | "llen" | "lrange" | "hset" | "hdel" | "hget"
        | "hincrby" | "hgetall" | "hlen" | "zadd" | "zrem" | "zscore" | "zcard" | "xadd"
        | "xlen" | "xread" | "watch" => args.first().cloned().into_iter().collect(),
        _ => Vec::new(),
    }
}

/// Project an op onto a single key, returning the per-key sub-op
/// `(function, args, result)` or `None` if the op has no effect for that key.
fn project_for_key(
    function: &str,
    args: &[Bytes],
    result: Option<&Bytes>,
    key: &Bytes,
) -> Option<(String, Vec<Bytes>, Option<Bytes>)> {
    match function {
        "mset" => {
            let mut i = 0;
            while i + 1 < args.len() {
                if &args[i] == key {
                    return Some((
                        "set".to_string(),
                        vec![args[i].clone(), args[i + 1].clone()],
                        Some(Bytes::from_static(b"OK")),
                    ));
                }
                i += 2;
            }
            None
        }
        "mget" => {
            let idx = args.iter().position(|a| a == key)?;
            let joined = result
                .map(|r| String::from_utf8_lossy(r).to_string())
                .unwrap_or_default();
            let fields: Vec<&str> = if joined.is_empty() {
                Vec::new()
            } else {
                joined.split('|').collect()
            };
            let val = fields.get(idx).copied().unwrap_or("nil");
            let sub_result = if val == "nil" {
                None
            } else {
                Some(Bytes::from(val.to_string()))
            };
            Some(("get".to_string(), vec![key.clone()], sub_result))
        }
        "exec" => explode_exec_for_key(args, result, key),
        _ => Some((function.to_string(), args.to_vec(), result.cloned())),
    }
}

/// Re-encode a committed exec restricted to the commands touching `key`.
fn explode_exec_for_key(
    args: &[Bytes],
    result: Option<&Bytes>,
    key: &Bytes,
) -> Option<(String, Vec<Bytes>, Option<Bytes>)> {
    let cmds = parse_exec_commands(args)?;
    // Aborted exec (nil) has no committed per-key effect.
    let results: Vec<String> = match result {
        None => return None,
        Some(r) => {
            let s = String::from_utf8_lossy(r);
            if s.is_empty() {
                Vec::new()
            } else {
                s.split('|').map(str::to_string).collect()
            }
        }
    };
    let mut sub_cmds: Vec<(String, Vec<Bytes>)> = Vec::new();
    let mut sub_results: Vec<String> = Vec::new();
    for (i, (name, cargs)) in cmds.iter().enumerate() {
        if cargs.first() == Some(key) {
            sub_cmds.push((name.clone(), cargs.clone()));
            if let Some(res) = results.get(i) {
                sub_results.push(res.clone());
            }
        }
    }
    if sub_cmds.is_empty() {
        return None;
    }
    let mut new_args = vec![Bytes::from(sub_cmds.len().to_string())];
    for (name, cargs) in &sub_cmds {
        new_args.push(Bytes::from(name.clone()));
        new_args.push(Bytes::from(cargs.len().to_string()));
        new_args.extend(cargs.clone());
    }
    Some((
        "exec".to_string(),
        new_args,
        Some(Bytes::from(sub_results.join("|"))),
    ))
}

/// Split `history` into per-key sub-histories. Multi-key atomic ops are
/// exploded into per-key atomic sub-ops that share the parent op's window.
pub fn partition_by_key(
    history: &History,
    keys_of: impl Fn(&str, &[Bytes]) -> Vec<Bytes>,
) -> HashMap<Bytes, History> {
    // Result-by-id for completed ops only (checker needs complete histories).
    let mut results: HashMap<u64, Option<Bytes>> = HashMap::new();
    for c in history.completed_operations() {
        results.insert(c.id, c.result.clone());
    }

    let mut subs: HashMap<Bytes, History> = HashMap::new();
    let mut sub_ids: HashMap<(u64, Bytes), u64> = HashMap::new();

    // Replay raw records in timestamp order so sub-op windows preserve overlap.
    for op in history.operations() {
        let Some(result) = results.get(&op.id) else {
            continue;
        };
        for key in keys_of(&op.function, &op.args) {
            let Some((f, a, r)) = project_for_key(&op.function, &op.args, result.as_ref(), &key)
            else {
                continue;
            };
            match op.kind {
                OpKind::Invoke => {
                    let sub = subs.entry(key.clone()).or_default();
                    let sid = sub.invoke(op.client_id, f, a);
                    sub_ids.insert((op.id, key.clone()), sid);
                }
                OpKind::Return => {
                    if let Some(sid) = sub_ids.get(&(op.id, key.clone())) {
                        if let Some(sub) = subs.get_mut(&key) {
                            sub.respond(*sid, r);
                        }
                    }
                }
            }
        }
    }
    subs
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing partition`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/partition.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): add per-key history partitioning with multi-key explode"
```

---

### Task 9a: Conservation checkers — delivery + FIFO wake order

Whole-history scans (not WGL). `ConservationViolation` is a `thiserror` enum. This task adds the enum plus the two list-oriented checkers.

**Files:**
- Create: `frogdb-server/crates/testing/src/conservation.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Consumes: `History` (history.rs).
- Produces: `pub enum ConservationViolation` (variants `MultipleDelivery`, `LostElement`, `PhantomDelivery`, `SumMismatch`, `WatchFalseNegative`, `FifoViolation` — all carry op ids + a human description via `#[error(..)]`); `pub fn check_exactly_once_delivery(history: &History, final_elements: &HashMap<Bytes, Vec<Bytes>>) -> Result<(), ConservationViolation>`; `pub fn check_fifo_wake_order(history: &History) -> Result<(), ConservationViolation>`.

- [ ] **Step 1: Declare the module in lib.rs**

Add to `frogdb-server/crates/testing/src/lib.rs`:

```rust
pub mod conservation;
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/conservation.rs`:

```rust
//! Whole-history conservation checkers (pure scans, not WGL-based).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::History;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn push_pop_history() -> History {
        let mut h = History::new();
        let p1 = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p1, Some(b("1")));
        let p2 = h.invoke(1, "rpush", vec![b("k"), b("b")]);
        h.respond(p2, Some(b("2")));
        let q1 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q1, Some(b("a")));
        let q2 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q2, Some(b("b")));
        h
    }

    #[test]
    fn delivery_ok_when_all_consumed() {
        let h = push_pop_history();
        assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    }

    #[test]
    fn delivery_ok_with_leftover_in_final_state() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("x")]);
        h.respond(p, Some(b("1")));
        let mut final_state = HashMap::new();
        final_state.insert(b("k"), vec![b("x")]);
        assert!(check_exactly_once_delivery(&h, &final_state).is_ok());
    }

    #[test]
    fn delivery_detects_double_pop() {
        let mut h = push_pop_history();
        // A second, illegal delivery of "a".
        let q = h.invoke(3, "lpop", vec![b("k")]);
        h.respond(q, Some(b("a")));
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::MultipleDelivery { .. })
        ));
    }

    #[test]
    fn delivery_detects_lost_element() {
        let mut h = History::new();
        let p = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p, Some(b("1")));
        // Never delivered, not in final state -> lost.
        assert!(matches!(
            check_exactly_once_delivery(&h, &HashMap::new()),
            Err(ConservationViolation::LostElement { .. })
        ));
    }

    #[test]
    fn fifo_ok_when_served_in_invoke_order() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w1, Some(b("k|a")));
        h.respond(w2, Some(b("k|b")));
        assert!(check_fifo_wake_order(&h).is_ok());
    }

    #[test]
    fn fifo_detects_out_of_order_wake() {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        // w2 (later waiter) served before w1 (earlier waiter).
        h.respond(w2, Some(b("k|b")));
        h.respond(w1, Some(b("k|a")));
        assert!(matches!(
            check_fifo_wake_order(&h),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing delivery_ && just test frogdb-testing fifo_`
Expected: FAIL — `cannot find type ConservationViolation`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/conservation.rs`:

```rust
use crate::history::History;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

/// A conservation-invariant violation. Carries the offending op id(s) and a
/// human-readable description (via the `Display`/`Error` message).
#[derive(Debug, thiserror::Error)]
pub enum ConservationViolation {
    /// An element was delivered (or left in final state) more times than pushed.
    #[error("element {element:?} (pushed by op {pushed_by}) delivered {times} times, over-consumed")]
    MultipleDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of a push that introduced it.
        pushed_by: u64,
        /// Observed delivery count.
        times: usize,
    },
    /// An element was pushed but neither delivered nor present at quiesce.
    #[error("element {element:?} (pushed by op {pushed_by}) was neither delivered nor in final state")]
    LostElement {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the push that introduced it.
        pushed_by: u64,
    },
    /// An element was delivered that was never pushed.
    #[error("element {element:?} delivered by op {delivered_by} was never pushed")]
    PhantomDelivery {
        /// The element bytes.
        element: Vec<u8>,
        /// Op id of the delivering pop.
        delivered_by: u64,
    },
    /// A transfer workload failed to conserve the sum over the tracked keys.
    #[error("transaction sum not conserved on {keys:?}: expected {expected}, computed {computed}")]
    SumMismatch {
        /// The tracked keys.
        keys: Vec<String>,
        /// The invariant target sum.
        expected: i64,
        /// The sum implied by the recorded history.
        computed: i64,
    },
    /// A committed EXEC ignored a concurrent write to a watched key.
    #[error("watch false-negative: exec op {exec_op} committed though op {writer_op} wrote watched key {key:?} after watch op {watch_op}")]
    WatchFalseNegative {
        /// The committed exec op id.
        exec_op: u64,
        /// The watch op id.
        watch_op: u64,
        /// The interfering writer op id.
        writer_op: u64,
        /// The watched key.
        key: Vec<u8>,
    },
    /// Blocked poppers on a key were not served in registration (invoke) order.
    #[error("FIFO wake order violated on key {key:?}: op {served} (later waiter) served before op {waiter}")]
    FifoViolation {
        /// The key.
        key: Vec<u8>,
        /// The op served out of order.
        served: u64,
        /// The earlier waiter it jumped ahead of.
        waiter: u64,
    },
}

/// Every pushed element is delivered to exactly one popper XOR present at
/// quiesce; no element delivered twice or lost. `final_elements` maps each key
/// to the elements remaining in its list after the workload drains.
pub fn check_exactly_once_delivery(
    history: &History,
    final_elements: &HashMap<Bytes, Vec<Bytes>>,
) -> Result<(), ConservationViolation> {
    fn record_push(
        value: Bytes,
        op: u64,
        pushed: &mut HashMap<Bytes, i64>,
        push_op: &mut HashMap<Bytes, u64>,
    ) {
        *pushed.entry(value.clone()).or_default() += 1;
        push_op.entry(value).or_insert(op);
    }

    let mut pushed: HashMap<Bytes, i64> = HashMap::new();
    let mut push_op: HashMap<Bytes, u64> = HashMap::new();
    let mut delivered: HashMap<Bytes, (i64, u64)> = HashMap::new(); // count, last op id

    for op in history.completed_operations() {
        match op.function.as_str() {
            "lpush" | "rpush" => {
                for v in op.args.iter().skip(1) {
                    record_push(v.clone(), op.id, &mut pushed, &mut push_op);
                }
            }
            "lpop" | "rpop" => {
                if let Some(r) = &op.result {
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                }
            }
            "blpop" | "brpop" => {
                if let Some(r) = &op.result {
                    if let Some((_, elem)) = String::from_utf8_lossy(r).split_once('|') {
                        let key = Bytes::from(elem.to_string());
                        let e = delivered.entry(key).or_insert((0, op.id));
                        e.0 += 1;
                        e.1 = op.id;
                    }
                }
            }
            "lmove" | "blmove" => {
                if let Some(r) = &op.result {
                    // Counts as both a delivery (from src) and a push (to dst).
                    let e = delivered.entry(r.clone()).or_insert((0, op.id));
                    e.0 += 1;
                    e.1 = op.id;
                    record_push(r.clone(), op.id, &mut pushed, &mut push_op);
                }
            }
            _ => {}
        }
    }

    let mut final_counts: HashMap<Bytes, i64> = HashMap::new();
    for elems in final_elements.values() {
        for e in elems {
            *final_counts.entry(e.clone()).or_default() += 1;
        }
    }

    let mut values: HashSet<Bytes> = HashSet::new();
    values.extend(pushed.keys().cloned());
    values.extend(delivered.keys().cloned());
    values.extend(final_counts.keys().cloned());

    for v in values {
        let p = pushed.get(&v).copied().unwrap_or(0);
        let (d, last_op) = delivered.get(&v).copied().unwrap_or((0, 0));
        let f = final_counts.get(&v).copied().unwrap_or(0);
        if p == 0 && d > 0 {
            return Err(ConservationViolation::PhantomDelivery {
                element: v.to_vec(),
                delivered_by: last_op,
            });
        }
        if d + f > p {
            return Err(ConservationViolation::MultipleDelivery {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
                times: d as usize,
            });
        }
        if d + f < p {
            return Err(ConservationViolation::LostElement {
                element: v.to_vec(),
                pushed_by: push_op.get(&v).copied().unwrap_or(0),
            });
        }
    }
    Ok(())
}

/// Blocked poppers (BLPOP/BRPOP hits) on a key are served in invoke order.
pub fn check_fifo_wake_order(history: &History) -> Result<(), ConservationViolation> {
    // key -> [(invoke_time, return_time, op_id)] for served blocking pops.
    let mut by_key: HashMap<Bytes, Vec<(u64, u64, u64)>> = HashMap::new();
    for op in history.completed_operations() {
        if matches!(op.function.as_str(), "blpop" | "brpop") && op.result.is_some() {
            if let Some(key) = op.args.first() {
                by_key
                    .entry(key.clone())
                    .or_default()
                    .push((op.invoke_time, op.return_time, op.id));
            }
        }
    }
    for (key, mut served) in by_key {
        served.sort_by_key(|x| x.1); // by serve (return) order
        for w in served.windows(2) {
            if w[0].0 > w[1].0 {
                // Served earlier but invoked later -> jumped an earlier waiter.
                return Err(ConservationViolation::FifoViolation {
                    key: key.to_vec(),
                    served: w[0].2,
                    waiter: w[1].2,
                });
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing delivery_ && just test frogdb-testing fifo_`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/conservation.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): add delivery + FIFO wake-order conservation checkers"
```

---

### Task 9b: Conservation checkers — transaction sum + WATCH no-false-negative

Adds the two transaction-oriented checkers to `conservation.rs`. Watch metadata: `watch` ops carry the key in `args[0]`; a committed EXEC returns a non-nil result, an aborted one returns nil (`None`).

**Files:**
- Modify: `frogdb-server/crates/testing/src/conservation.rs`

**Interfaces:**
- Consumes: `ConservationViolation` (Task 9a), `parse_exec_commands` and `default_keys_of` (Task 8), `CompletedOperation` (history.rs).
- Produces: `pub fn check_tx_sum_conservation(history: &History, keys: &[Bytes], expected_sum: i64) -> Result<(), ConservationViolation>`; `pub fn check_watch_no_false_negative(history: &History) -> Result<(), ConservationViolation>`.

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)] mod tests` block in `conservation.rs`:

```rust
    fn transfer(h: &mut History, client: u64, from: &str, to: &str, amt: i64) {
        // EXEC: DECRBY from amt, INCRBY to amt -> two integer replies.
        let op = h.invoke(
            client,
            "exec",
            vec![
                b("2"),
                b("decrby"), b("2"), b(from), Bytes::from(amt.to_string()),
                b("incrby"), b("2"), b(to), Bytes::from(amt.to_string()),
            ],
        );
        h.respond(op, Some(b("0|0")));
    }

    #[test]
    fn tx_sum_conserved_under_transfers() {
        let mut h = History::new();
        transfer(&mut h, 1, "a", "b", 5);
        transfer(&mut h, 2, "b", "a", 3);
        let keys = vec![b("a"), b("b")];
        assert!(check_tx_sum_conservation(&h, &keys, 100).is_ok());
    }

    #[test]
    fn tx_sum_detects_leak() {
        let mut h = History::new();
        // Only credit b, never debit a -> +5 net, not conserved.
        let op = h.invoke(1, "exec", vec![b("1"), b("incrby"), b("2"), b("b"), b("5")]);
        h.respond(op, Some(b("0")));
        let keys = vec![b("a"), b("b")];
        assert!(matches!(
            check_tx_sum_conservation(&h, &keys, 100),
            Err(ConservationViolation::SumMismatch { .. })
        ));
    }

    #[test]
    fn watch_ok_when_no_interfering_write() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn watch_detects_false_negative() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        // Another client writes the watched key after the WATCH...
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        // ...yet this client's EXEC commits -> false negative.
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(matches!(
            check_watch_no_false_negative(&h),
            Err(ConservationViolation::WatchFalseNegative { .. })
        ));
    }

    #[test]
    fn watch_aborted_exec_is_fine() {
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, None); // aborted -> correct behavior
        assert!(check_watch_no_false_negative(&h).is_ok());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-testing tx_sum && just test frogdb-testing watch_`
Expected: FAIL — `cannot find function check_tx_sum_conservation`.

- [ ] **Step 3: Write minimal implementation**

Add to the top-of-file `use` block in `conservation.rs`:

```rust
use crate::history::CompletedOperation;
use crate::partition::{default_keys_of, parse_exec_commands};
```

Append these functions to `conservation.rs` (after `check_fifo_wake_order`):

```rust
/// Net integer delta a single command applies to keys in `keyset`.
fn cmd_delta(name: &str, args: &[Bytes], keyset: &HashSet<Bytes>) -> i64 {
    if args.is_empty() || !keyset.contains(&args[0]) {
        return 0;
    }
    let by = || {
        args.get(1)
            .and_then(|a| String::from_utf8_lossy(a).parse::<i64>().ok())
            .unwrap_or(0)
    };
    match name {
        "incr" => 1,
        "decr" => -1,
        "incrby" => by(),
        "decrby" => -by(),
        _ => 0,
    }
}

/// Bank-transfer conservation: the total over `keys` must not change, so the
/// final sum equals `expected_sum`. Sums INCR/DECR(BY) deltas from committed
/// EXECs and standalone counter ops; a nonzero net delta is a violation.
pub fn check_tx_sum_conservation(
    history: &History,
    keys: &[Bytes],
    expected_sum: i64,
) -> Result<(), ConservationViolation> {
    let keyset: HashSet<Bytes> = keys.iter().cloned().collect();
    let mut delta: i64 = 0;
    for op in history.completed_operations() {
        match op.function.as_str() {
            "exec" => {
                if op.result.is_none() {
                    continue; // aborted
                }
                for (name, cargs) in parse_exec_commands(&op.args).unwrap_or_default() {
                    delta += cmd_delta(&name, &cargs, &keyset);
                }
            }
            "incr" | "decr" | "incrby" | "decrby" => {
                delta += cmd_delta(&op.function, &op.args, &keyset);
            }
            _ => {}
        }
    }
    if delta != 0 {
        return Err(ConservationViolation::SumMismatch {
            keys: keys
                .iter()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .collect(),
            expected: expected_sum,
            computed: expected_sum + delta,
        });
    }
    Ok(())
}

fn is_write(function: &str) -> bool {
    matches!(
        function,
        "set" | "write" | "del" | "delete" | "incr" | "incrby" | "decr" | "decrby" | "lpush"
            | "rpush" | "lpop" | "rpop" | "hset" | "hdel" | "hincrby" | "zadd" | "zrem" | "mset"
    )
}

/// Find a completed write to `key` by a client other than `exclude_client`
/// whose return time falls strictly within `(lo, hi)`.
fn writer_between(
    ops: &[CompletedOperation],
    key: &Bytes,
    lo: u64,
    hi: u64,
    exclude_client: u64,
) -> Option<u64> {
    for op in ops {
        if op.client_id == exclude_client || !is_write(&op.function) {
            continue;
        }
        if !default_keys_of(&op.function, &op.args).iter().any(|k| k == key) {
            continue;
        }
        if op.return_time > lo && op.return_time < hi {
            return Some(op.id);
        }
    }
    None
}

/// WATCH no-false-negative: a committed EXEC must not have ignored another
/// client's write to a watched key that completed between the WATCH and the
/// EXEC invoke. (Over-abort is legal and not checked here.)
pub fn check_watch_no_false_negative(history: &History) -> Result<(), ConservationViolation> {
    let ops = history.completed_operations();
    let mut by_client: HashMap<u64, Vec<&CompletedOperation>> = HashMap::new();
    for op in &ops {
        by_client.entry(op.client_id).or_default().push(op);
    }
    for (_client, mut cops) in by_client {
        cops.sort_by_key(|o| o.invoke_time);
        // (key, watch_time, watch_op_id)
        let mut watched: Vec<(Bytes, u64, u64)> = Vec::new();
        for op in cops {
            match op.function.as_str() {
                "watch" => {
                    if let Some(k) = op.args.first() {
                        watched.push((k.clone(), op.invoke_time, op.id));
                    }
                }
                "exec" => {
                    if op.result.is_some() {
                        for (k, wt, wid) in &watched {
                            if let Some(writer) =
                                writer_between(&ops, k, *wt, op.invoke_time, op.client_id)
                            {
                                return Err(ConservationViolation::WatchFalseNegative {
                                    exec_op: op.id,
                                    watch_op: *wid,
                                    writer_op: writer,
                                    key: k.to_vec(),
                                });
                            }
                        }
                    }
                    watched.clear();
                }
                "discard" | "reset" | "unwatch" => watched.clear(),
                _ => {}
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `just test frogdb-testing tx_sum && just test frogdb-testing watch_`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add frogdb-server/crates/testing/src/conservation.rs
git commit -m "feat(testing): add tx-sum and WATCH no-false-negative conservation checkers"
```

---

### Task 10: Fault-injection self-tests

Guards against silent-green checker bugs. `fault_injection` provides history-corruption helpers; tests assert each corruption is CAUGHT and the uncorrupted original PASSES.

**Files:**
- Create: `frogdb-server/crates/testing/src/fault_injection.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Consumes: `History`, `OpKind`, `Operation` (history.rs); `check_exactly_once_delivery`, `check_fifo_wake_order` (Task 9a).
- Produces: `pub fn drop_delivery(history: &History) -> History`; `pub fn duplicate_delivery(history: &History) -> History`; `pub fn reorder_completions(history: &History) -> History`; `pub fn lose_element(history: &History) -> History`.

- [ ] **Step 1: Declare the module in lib.rs**

Add to `frogdb-server/crates/testing/src/lib.rs`:

```rust
pub mod fault_injection;
```

- [ ] **Step 2: Write the failing test**

Create `frogdb-server/crates/testing/src/fault_injection.rs`:

```rust
//! History-corruption helpers used by checker self-tests.
//!
//! A correct checker must reject each corruption and accept the original.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conservation::{check_exactly_once_delivery, check_fifo_wake_order};
    use crate::history::History;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn valid_delivery_history() -> History {
        let mut h = History::new();
        let p1 = h.invoke(1, "rpush", vec![b("k"), b("a")]);
        h.respond(p1, Some(b("1")));
        let p2 = h.invoke(1, "rpush", vec![b("k"), b("b")]);
        h.respond(p2, Some(b("2")));
        let q1 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q1, Some(b("a")));
        let q2 = h.invoke(2, "lpop", vec![b("k")]);
        h.respond(q2, Some(b("b")));
        h
    }

    fn valid_fifo_history() -> History {
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w1, Some(b("k|a")));
        h.respond(w2, Some(b("k|b")));
        h
    }

    #[test]
    fn original_delivery_history_passes() {
        assert!(check_exactly_once_delivery(&valid_delivery_history(), &HashMap::new()).is_ok());
    }

    #[test]
    fn drop_delivery_is_caught() {
        let corrupt = drop_delivery(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn duplicate_delivery_is_caught() {
        let corrupt = duplicate_delivery(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn lose_element_is_caught() {
        let corrupt = lose_element(&valid_delivery_history());
        assert!(check_exactly_once_delivery(&corrupt, &HashMap::new()).is_err());
    }

    #[test]
    fn original_fifo_history_passes() {
        assert!(check_fifo_wake_order(&valid_fifo_history()).is_ok());
    }

    #[test]
    fn reorder_completions_is_caught() {
        let corrupt = reorder_completions(&valid_fifo_history());
        assert!(check_fifo_wake_order(&corrupt).is_err());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `just test frogdb-testing fault_injection`
Expected: FAIL — `cannot find function drop_delivery`.

- [ ] **Step 4: Write minimal implementation**

Prepend to `frogdb-server/crates/testing/src/fault_injection.rs`:

```rust
use crate::history::{History, OpKind, Operation};
use std::collections::HashMap;

fn is_pop(function: &str) -> bool {
    matches!(function, "lpop" | "rpop" | "blpop" | "brpop")
}

/// Rebuild a history from raw records, replaying invoke/return in list order so
/// timestamps follow the (possibly reordered) record sequence.
fn rebuild(records: &[Operation]) -> History {
    let mut h = History::new();
    let mut idmap: HashMap<u64, u64> = HashMap::new();
    for r in records {
        match r.kind {
            OpKind::Invoke => {
                let nid = h.invoke(r.client_id, r.function.clone(), r.args.clone());
                idmap.insert(r.id, nid);
            }
            OpKind::Return => {
                if let Some(nid) = idmap.get(&r.id) {
                    h.respond(*nid, r.result.clone());
                }
            }
        }
    }
    h
}

/// Corruption: drop the first successful pop entirely (an element that was
/// delivered now vanishes). Trips `check_exactly_once_delivery` (LostElement).
pub fn drop_delivery(history: &History) -> History {
    let target = history
        .completed_operations()
        .into_iter()
        .find(|op| is_pop(&op.function) && op.result.is_some())
        .map(|op| op.id);
    let mut recs: Vec<Operation> = history.operations().to_vec();
    if let Some(id) = target {
        recs.retain(|o| o.id != id);
    }
    rebuild(&recs)
}

/// Corruption: duplicate the first successful pop (double delivery). Trips
/// `check_exactly_once_delivery` (MultipleDelivery / PhantomDelivery).
pub fn duplicate_delivery(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let inv = recs
        .iter()
        .find(|o| matches!(o.kind, OpKind::Invoke) && is_pop(&o.function))
        .cloned();
    if let Some(inv) = inv {
        let ret = recs
            .iter()
            .find(|o| o.id == inv.id && matches!(o.kind, OpKind::Return) && o.result.is_some())
            .cloned();
        if let Some(ret) = ret {
            let mut dinv = inv.clone();
            dinv.id = u64::MAX;
            let mut dret = ret.clone();
            dret.id = u64::MAX;
            recs.push(dinv);
            recs.push(dret);
        }
    }
    rebuild(&recs)
}

/// Corruption: remove the first push entirely (a lost write). The element it
/// introduced either never existed for its later delivery (PhantomDelivery) or
/// unbalances the counts. Trips `check_exactly_once_delivery`.
pub fn lose_element(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let target = recs
        .iter()
        .find(|o| matches!(o.kind, OpKind::Invoke) && matches!(o.function.as_str(), "lpush" | "rpush"))
        .map(|o| o.id);
    if let Some(id) = target {
        recs.retain(|o| o.id != id);
    }
    rebuild(&recs)
}

/// Corruption: swap the serve order of the two earliest blocking-pop hits so a
/// later waiter returns first. Trips `check_fifo_wake_order` (FifoViolation).
pub fn reorder_completions(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    let idxs: Vec<usize> = recs
        .iter()
        .enumerate()
        .filter(|(_, o)| {
            matches!(o.kind, OpKind::Return)
                && matches!(o.function.as_str(), "blpop" | "brpop")
                && o.result.is_some()
        })
        .map(|(i, _)| i)
        .take(2)
        .collect();
    if idxs.len() == 2 {
        recs.swap(idxs[0], idxs[1]);
    }
    rebuild(&recs)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `just test frogdb-testing fault_injection`
Expected: PASS (original histories accepted, all four corruptions rejected).

- [ ] **Step 6: Commit**

```bash
git add frogdb-server/crates/testing/src/fault_injection.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "test(testing): add fault-injection self-tests for conservation checkers"
```

---

### Task 11: Final wiring — lib.rs re-exports, doc comments, full test + lint

Surface the new public API from the crate root, add crate-level doc comments, and run the whole crate's tests plus clippy.

**Files:**
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Produces (crate-root re-exports): the model types, `check_linearizability_bounded`, partition functions, conservation checkers + `ConservationViolation`.

- [ ] **Step 1: Write the failing test**

Create `frogdb-server/crates/testing/tests/public_api.rs` (an integration test that only compiles if the crate root re-exports everything):

```rust
//! Smoke test that the phase-1 oracle API is reachable from the crate root.

use bytes::Bytes;
use frogdb_testing::{
    check_exactly_once_delivery, check_fifo_wake_order, check_linearizability,
    check_linearizability_bounded, check_tx_sum_conservation, check_watch_no_false_negative,
    default_keys_of, partition_by_key, ConservationViolation, HashModel, History, ListModel,
    StreamModel, ZSetModel,
};
use std::collections::HashMap;

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

#[test]
fn oracle_api_is_reachable_from_root() {
    let mut h = History::new();
    let p = h.invoke(1, "rpush", vec![b("k"), b("a")]);
    h.respond(p, Some(b("1")));
    let q = h.invoke(2, "lpop", vec![b("k")]);
    h.respond(q, Some(b("a")));

    // Models reachable and usable via the checker.
    assert!(check_linearizability::<ListModel>(&h).is_linearizable);
    // Bounded checker + inconclusive flag reachable from the root.
    assert!(check_linearizability_bounded::<ListModel>(&h, 1).inconclusive);

    // Partitioning + conservation reachable.
    let _parts = partition_by_key(&h, default_keys_of);
    assert!(check_exactly_once_delivery(&h, &HashMap::new()).is_ok());
    assert!(check_fifo_wake_order(&h).is_ok());
    assert!(check_tx_sum_conservation(&h, &[b("k")], 0).is_ok());
    assert!(check_watch_no_false_negative(&h).is_ok());

    // Remaining models are in scope.
    let _ = (HashModel, ZSetModel, StreamModel);
    let _err_ty: fn() -> Option<ConservationViolation> = || None;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `just test frogdb-testing oracle_api`
Expected: FAIL — unresolved imports (e.g. `ListModel`, `partition_by_key`, `ConservationViolation` not found in crate root).

- [ ] **Step 3: Update the crate-root re-exports and docs**

Replace the re-export block at the bottom of `frogdb-server/crates/testing/src/lib.rs` with:

```rust
pub mod checker;
pub mod conservation;
pub mod fault_injection;
pub mod history;
pub mod models;
pub mod partition;

pub use checker::{
    check_linearizability, check_linearizability_bounded, LinearizabilityResult,
};
pub use conservation::{
    check_exactly_once_delivery, check_fifo_wake_order, check_tx_sum_conservation,
    check_watch_no_false_negative, ConservationViolation,
};
pub use history::{History, OpKind, Operation};
pub use models::{
    HashModel, HashState, KVModel, KVState, ListModel, ListState, Model, RegisterModel,
    RegisterState, StreamData, StreamId, StreamModel, StreamState, ZSetModel, ZSetState,
};
pub use partition::{default_keys_of, partition_by_key};
```

Then extend the crate-level `//!` doc comment near the top of `lib.rs` (after the existing "Sequential specification models" bullet) to mention the phase-1 additions:

```rust
//! - Per-key history partitioning (`partition`) for scalable checking
//! - Conservation checkers (`conservation`): exactly-once delivery, FIFO wake
//!   order, transaction-sum conservation, WATCH no-false-negative
//! - Fault-injection self-tests (`fault_injection`) guarding against
//!   silent-green checker bugs
//! - Strict per-type models: lists, hashes, sorted sets, streams
```

- [ ] **Step 4: Run the new test to verify it passes**

Run: `just test frogdb-testing oracle_api`
Expected: PASS.

- [ ] **Step 5: Run the whole crate's tests**

Run: `just test frogdb-testing`
Expected: PASS (all unit + integration tests across models, checker, history, partition, conservation, fault_injection).

- [ ] **Step 6: Format, lint, and confirm consumers still build**

Run: `just fmt frogdb-testing && just lint frogdb-testing && just check frogdb-server`
Expected: PASS — no clippy warnings (lint uses `-D warnings`); the ~50 simulation tests still compile.

- [ ] **Step 7: Commit**

```bash
git add frogdb-server/crates/testing/src/lib.rs frogdb-server/crates/testing/tests/public_api.rs
git commit -m "feat(testing): re-export phase-1 oracle API from crate root"
```
