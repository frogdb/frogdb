# Concurrency Phase 4a — Workload Vocabulary Expansion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 4a of the concurrency-invariant-testing design (`docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`, "Phasing" item 4a): expand the proven Phase-3 seeded-workload turmoil harness — **no new harness** — to close the largest concurrency-coverage holes. Five components: (1) un-ignore the TxHeavy transaction sweep by teaching the KV model + conservation checkers to accept an aborted/errored EXEC, biasing the generator toward same-slot transaction groups, and researching VLL-EXEC reachability; (2) consumer groups via a new strict `StreamGroupModel` + PEL conservation checker + `Family::StreamGroup` generator vocabulary; (3) scripting via a fixed pool of single-key Lua scripts translated to existing model ops + `Family::Script`; (4) exact multi-waiter FIFO using the Phase-2 `DEBUG WAITQUEUE` per-waiter registration ordinals; (5) three bundled small items (connections-responsive quiescence, `audit_expiry_index` anomaly tests, binary-safe history serde).

**Architecture:** All model/checker work lands in `frogdb-testing` (fast, transport-agnostic, unit-tested) and must be green before the generator changes (`frogdb-testing/src/workload.rs`), which must be green before the server-test harness wiring (`frogdb-server/crates/server/tests/common/*` + `concurrency_workload.rs`) and the sweeps. The one production-crate change is two additional unit tests for the existing `frogdb-core` `Store::audit_expiry_index` (no behavior change). New strict models reject unknown ops (`None`), matching the Phase-1 convention. Conservation checkers are pure whole-history scans. Everything routes through the existing Phase-3 pipeline: response legality → per-key bounded WGL with inconclusive-downgrade → conservation → tier-4 quiescence.

**Tech Stack:** Rust; `bytes::Bytes`; `serde`/`serde_json`/`serde_bytes`; `rand` (`StdRng`, `RngExt`, `SeedableRng`, `seed_from_u64`); `thiserror`; `proptest` (dev-dep); `turmoil` 0.7.1 (`--features turmoil`); the Phase-1 oracle in `frogdb-testing`; `just` + `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You may be in a git worktree, not the main checkout. Only edit files under the directory `pwd` reports; use absolute paths.
- **Crate names / build commands** (verified in `Cargo.toml`s and the `Justfile`):
  - `frogdb-testing` — `just check frogdb-testing`, `just test frogdb-testing <pattern>` (expands `<pattern>` to `-E 'test(/<pattern>/)'`), `just lint frogdb-testing`, `just fmt frogdb-testing`.
  - `frogdb-core` — `just check frogdb-core`, `just test frogdb-core <pattern>`.
  - `frogdb-server` — `just check frogdb-server`; the simulation/workload tests need the `turmoil` feature and run via `cargo nextest run -p frogdb-server --features turmoil -E 'test(/<pattern>/)'` (also `just concurrency-turmoil <PATTERN>`).
  - If `sccache` errors, re-run prefixed with `RUSTC_WRAPPER=""`.
- **Turmoil tests need `--features turmoil`.** The `common/` harness modules are gated `#[cfg(feature = "turmoil")]`; a plain `just test frogdb-server` will not compile or run them.
- **Watchdog rules apply (CLAUDE.md).** Any build/test expected to take >2 min runs in the background with **raw output redirected to a log file** (`cmd > /path/to/log 2>&1`), with a liveness check every few minutes (log growing via `wc -c`, or CPU accumulating via `ps`). Never pipe a long run through `grep`/`tail`. **Commit before launching a long run.** The `_dyld_start` / `syspolicyd` exec-validation-wedge signatures in CLAUDE.md apply to the turmoil sweeps.
- **TDD:** every task writes a failing test first, watches it fail with the predicted error, then writes the minimal implementation.
- **Commit per task** with the exact `git` command shown in the task's final step. **No `Co-Authored-By` lines** in any commit message.
- **Task ordering is a hard dependency chain:** model/checker (`frogdb-testing`, fast) → generator (`frogdb-testing`) → harness wiring (`frogdb-server` tests) → sweeps. Each task is independently committable and must leave the workspace green.
- **Do not weaken a checker to make a sweep pass.** A sweep failure is signal: triage harness/model gap vs. genuine server bug per `docs/superpowers/plans/concurrency-phase3-bug-workflow.md`. A genuine server bug gets a pinned named regression test.
- **Encoding constraints carried over from Phase-1/3** (each already has a generator invariant test in `workload.rs`): no `|` in any generated key/value; HGETALL/stream replies canonicalized by the recorder; stream ids only `*` or full `ms-seq`; single-type keys so a per-key sub-history is single-model. New vocabulary must honor all of these.
- **The reserved result delimiter is `|`.** The recorder's error encoding is the literal prefix `ERR:` (produced by `OperationHistory::encode_array_result` for an `OperationResult::Error`). This plan makes `ERR:` a **cross-crate contract**: an EXEC/transaction result string beginning with `ERR:` denotes an aborted/rejected transaction. Document it at both ends.

### Discrepancies found while reading the code (resolved in-plan)

These correct the audit brief against the current tree; they change some task shapes:

1. **WATCH-abort is already handled.** `KVModel::exec` (`models/kv.rs:294-300`) already accepts a nil EXEC result (`None`) as a legal no-op. The real gap is a **CROSSSLOT/EXECABORT error** EXEC, which the runner records as `Some("ERR:…")` (via `record_exec_reply`'s `other =>` arm → `record_exec_return` → `encode_array_result`), not as `None`. Task 4 targets that.
2. **The sim server runs with `allow_cross_slot_standalone: true`** (`common/sim_helpers.rs:146,187,230`) and `cluster_enabled` defaults false (standalone). This **contradicts** the `seed_sweep_txheavy` root-cause comment ("the multi-shard server correctly rejects those EXECs with -CROSSSLOT"). Cross-slot multi-key ops may now be *accepted* and routed via VLL rather than rejected. Task 3 re-verifies the actual failure before Task 4/6 implement against it, and folds the VLL-reachability research into the same empirical pass.
3. **`audit_expiry_index` lives in `frogdb-core`** (`store/hashmap.rs:778`, `store/mod.rs` trait), **not** in `frogdb-testing`. It already has `KeyPersistent` and `IndexMissing` tests (`hashmap.rs:1405,1430`); the missing ones are `KeyMissing` and `DeadlineMismatch` (Task 2).
4. **`DEBUG WAITQUEUE` already emits per-waiter registration ordinals** (`debug_conn_command.rs:554-604`: each waiter carries `conn_id`, `op`, `registration_seq`, `has_deadline`), but the testing-side `WaitQueueSnapshot` (`quiescence.rs:16-21`) and the probe adapter (`quiescence_probe.rs::parse_waitqueue`) **drop everything except `total_waiters`**. Also, the quiescence probe runs post-drain (queue empty), so registration data must be captured **mid-run** — Task 18 adds a prober. Correlating a served pop (`client_id` in `History`) to a waiter (`conn_id` in the dump) needs a `client_id→conn_id` map; FrogDB supports `CLIENT ID` (`client_conn_command.rs:76,193`), used for that map.

---

## Component 1 — Un-ignore the TxHeavy transaction sweep

### Task 1: Binary-safe history serde (bundled small item 5c)

`history.rs` round-trips `Bytes` through `String::from_utf8_lossy` in three serde adapters (`bytes_vec_serde`, `bytes_vec_serde_pub`, `bytes_option_serde`), silently corrupting non-UTF-8 bytes. Make it **lossless** while keeping readable JSON for the common UTF-8 case: encode a valid-UTF-8 value as a plain JSON string, and a non-UTF-8 value as a tagged object `{"b64": "<base64>"}`.

**Files:**
- Modify: `frogdb-server/crates/testing/Cargo.toml` — add `base64` to `[dependencies]`.
- Modify: `frogdb-server/crates/testing/src/history.rs`

**Interfaces:**
- Unchanged public types; the three serde modules gain a shared lossless codec. JSON stays human-readable for UTF-8 values.

- [ ] **Step 1: Add the dep.** In `frogdb-server/crates/testing/Cargo.toml`, under `[dependencies]`, add:

```toml
base64 = { workspace = true }
```

If `base64` is not in `[workspace.dependencies]`, add `base64 = "0.22"` there and reference it with `base64 = { workspace = true }`.

- [ ] **Step 2: Write the failing test.** Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/testing/src/history.rs`:

```rust
    #[test]
    fn non_utf8_bytes_round_trip_losslessly() {
        let mut history = History::new();
        // 0xff 0xfe is not valid UTF-8; lossy encoding would corrupt it.
        let op = history.invoke(1, "set", vec![Bytes::from(vec![0xff, 0xfe]), Bytes::from("v")]);
        history.respond(op, Some(Bytes::from(vec![0x80, 0x00, 0x81])));

        let json = history.to_json();
        let restored = History::from_json(&json).unwrap();

        let inv = restored
            .operations()
            .iter()
            .find(|o| o.kind == OpKind::Invoke)
            .unwrap();
        assert_eq!(inv.args[0].as_ref(), &[0xff, 0xfe][..], "non-UTF8 arg corrupted");
        let ret = restored
            .operations()
            .iter()
            .find(|o| o.kind == OpKind::Return)
            .unwrap();
        assert_eq!(ret.result.as_deref(), Some(&[0x80, 0x00, 0x81][..]));
    }

    #[test]
    fn utf8_values_stay_readable_in_json() {
        let mut history = History::new();
        let op = history.invoke(1, "set", vec![Bytes::from("key"), Bytes::from("val")]);
        history.respond(op, Some(Bytes::from("OK")));
        let json = history.to_json();
        // Readable case must remain a plain JSON string, not a base64 object.
        assert!(json.contains("\"key\""));
        assert!(json.contains("\"OK\""));
        assert!(!json.contains("b64"));
    }
```

- [ ] **Step 3: Run test to verify it fails.**

Run: `just test frogdb-testing round_trip_losslessly`
Expected: FAIL — the lossy path decodes `0xff 0xfe` to the UTF-8 replacement char, so the assertion mismatches.

- [ ] **Step 4: Write minimal implementation.** In `frogdb-server/crates/testing/src/history.rs`, add a private shared codec and rewrite the three modules to use it. Add near the top (after the `use` lines):

```rust
/// Lossless `Bytes` <-> JSON scalar: a UTF-8 value serializes as a plain string
/// (human-readable); a non-UTF-8 value serializes as `{"b64": "<base64>"}`.
mod bytes_codec {
    use base64::Engine;
    use bytes::Bytes;
    use serde::de::{self, MapAccess, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(b: &Bytes, s: S) -> Result<S::Ok, S::Error> {
        match std::str::from_utf8(b) {
            Ok(text) => s.serialize_str(text),
            Err(_) => {
                use serde::ser::SerializeMap;
                let mut m = s.serialize_map(Some(1))?;
                let enc = base64::engine::general_purpose::STANDARD.encode(b);
                m.serialize_entry("b64", &enc)?;
                m.end()
            }
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Bytes, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Bytes;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a UTF-8 string or a {\"b64\": ...} object")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Bytes, E> {
                Ok(Bytes::from(v.to_owned()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Bytes, A::Error> {
                let mut out: Option<Bytes> = None;
                while let Some(k) = map.next_key::<String>()? {
                    if k == "b64" {
                        let enc: String = map.next_value()?;
                        let raw = base64::engine::general_purpose::STANDARD
                            .decode(enc.as_bytes())
                            .map_err(de::Error::custom)?;
                        out = Some(Bytes::from(raw));
                    } else {
                        let _: serde::de::IgnoredAny = map.next_value()?;
                    }
                }
                out.ok_or_else(|| de::Error::missing_field("b64"))
            }
        }
        d.deserialize_any(V)
    }
}
```

Rewrite `bytes_vec_serde`, `bytes_vec_serde_pub`, and `bytes_option_serde` to delegate element-wise to `bytes_codec` instead of `from_utf8_lossy`. For the two `Vec<Bytes>` modules:

```rust
mod bytes_vec_serde {
    use super::bytes_codec;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Wrap(#[serde(with = "bytes_codec")] Bytes);

    pub fn serialize<S: Serializer>(bytes: &[Bytes], s: S) -> Result<S::Ok, S::Error> {
        let wrapped: Vec<Wrap> = bytes.iter().cloned().map(Wrap).collect();
        wrapped.serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Bytes>, D::Error> {
        let wrapped: Vec<Wrap> = Vec::deserialize(d)?;
        Ok(wrapped.into_iter().map(|w| w.0).collect())
    }
}
```

Apply the identical body to `pub mod bytes_vec_serde_pub` (keeping it `pub`). For `bytes_option_serde`:

```rust
mod bytes_option_serde {
    use super::bytes_codec;
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Wrap(#[serde(with = "bytes_codec")] Bytes);

    pub fn serialize<S: Serializer>(b: &Option<Bytes>, s: S) -> Result<S::Ok, S::Error> {
        b.as_ref().cloned().map(Wrap).serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Bytes>, D::Error> {
        let w: Option<Wrap> = Option::deserialize(d)?;
        Ok(w.map(|w| w.0))
    }
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `just test frogdb-testing history`
Expected: PASS — including the pre-existing `test_json_serialization` and `test_node_defaults_when_absent_in_json` (the old UTF-8 JSON shape is preserved for readable values).

- [ ] **Step 6: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/Cargo.toml frogdb-server/crates/testing/src/history.rs Cargo.toml
git commit -m "fix(testing): lossless binary-safe history serde (base64 for non-UTF8, readable JSON otherwise)"
```

---

### Task 2: `audit_expiry_index` KeyMissing + DeadlineMismatch tests (bundled small item 5b)

Two missing unit tests for the existing `frogdb-core` `HashMapStore::audit_expiry_index` (`store/hashmap.rs:778`). Direction-1 of the audit already emits `KeyMissing` (index entry pointing at a deleted key) and `DeadlineMismatch` (index deadline ≠ entry deadline) — verified in the impl at `:785` and `:794` — but only `KeyPersistent`/`IndexMissing` are covered by tests. No production change; tests only.

**Files:**
- Modify: `frogdb-server/crates/core/src/store/hashmap.rs` (test module only)

**Interfaces:**
- Consumes existing test helpers `force_index_entry_for_test(&mut self, key: Bytes, deadline: Instant)` (`:675`) and `set_expiry(&mut self, key: &[u8], expires_at: Instant)`.

- [ ] **Step 1: Write the failing tests.** Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/core/src/store/hashmap.rs`, next to `expiry_index_audit_flags_index_missing`:

```rust
    #[test]
    fn expiry_index_audit_flags_key_missing() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        // An index entry for a key that was never stored: direction-1 KeyMissing.
        store.force_index_entry_for_test(Bytes::from("ghost"), Instant::now() + Duration::from_secs(60));
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "ghost");
        assert!(matches!(anomalies[0].kind, ExpiryIndexAnomalyKind::KeyMissing));
    }

    #[test]
    fn expiry_index_audit_flags_deadline_mismatch() {
        use std::time::{Duration, Instant};
        let mut store = HashMapStore::new();
        store.set(Bytes::from("k"), Value::string(Bytes::from("v")));
        store.set_expiry(b"k", Instant::now() + Duration::from_secs(3600));
        assert!(store.audit_expiry_index().is_empty());
        // Overwrite the index entry's deadline so it disagrees with the entry's.
        store.force_index_entry_for_test(Bytes::from("k"), Instant::now() + Duration::from_secs(1));
        let anomalies = store.audit_expiry_index();
        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].key, "k");
        assert!(matches!(anomalies[0].kind, ExpiryIndexAnomalyKind::DeadlineMismatch));
    }
```

- [ ] **Step 2: Run tests to verify they fail-then-pass.** These assert existing behavior, so they should pass on first run *if* the helper does what the impl expects. Run:

Run: `just test frogdb-core expiry_index_audit`
Expected: PASS for all four (two pre-existing + two new). If either new test fails, that is a **genuine bug in `audit_expiry_index`** (direction-1 not emitting the documented anomaly): stop, follow `superpowers:systematic-debugging`, fix the audit, and keep the test as the regression guard. (The impl at `:785`/`:794` shows both anomalies are emitted, so a green run is expected.)

- [ ] **Step 3: Commit.**

```bash
git add frogdb-server/crates/core/src/store/hashmap.rs
git commit -m "test(core): cover audit_expiry_index KeyMissing and DeadlineMismatch anomalies"
```

---

### Task 3: RESEARCH — characterize the current TxHeavy failure + VLL/CROSSSLOT reachability

**No production/test code lands in this task.** It is an empirical investigation whose findings drive Tasks 4, 6, and 7. The `seed_sweep_txheavy` root-cause comment predates the `allow_cross_slot_standalone: true` config (Discrepancy #2) and must be re-validated before implementing a fix against it.

**Files:** none (produce findings in the task's final commit message / hand notes; do not write a report file).

**Research procedure:**

- [ ] **Step 1: Reproduce the current failure.** Run the ignored sweep in the foreground (or background per the watchdog rules) and capture per-seed output:

```bash
cargo nextest run -p frogdb-server --features turmoil --run-ignored all \
  -E 'test(/seed_sweep_txheavy/)' > /tmp/txheavy.log 2>&1
```

Record: does it pass now? If not, which seeds fail and with which `report.violations` strings (non-linearizable key vs. WATCH false-negative vs. tx-sum vs. quiescence)?

- [ ] **Step 2: Capture the raw EXEC replies.** Add a throwaway `eprintln!` in `common/workload_runner.rs::record_exec_reply` dumping `reply` for `exec` ops (remove before any commit), and re-run a single failing seed via a one-off `run_and_check(seed, Profile::TxHeavy, 4, 30, 2)`. Classify each EXEC outcome into: **committed** (`Array([...])`), **WATCH-abort** (`Nil`), **error** (`Error("CROSSSLOT …")` / `Error("EXECABORT …")`). This resolves whether the cross-slot EXEC is *rejected* (error) or *accepted via VLL* (committed) under `allow_cross_slot_standalone: true`.

- [ ] **Step 3: Resolve VLL-EXEC reachability.** Read the routing + transaction driver to determine whether a *standalone same-slot* multi-key EXEC can span internal shards (VLL), and whether cross-slot EXEC is rejected or VLL-routed:
  - `frogdb-server/crates/server/tests/common/sim_harness.rs::shard_for_key` (`:389`): `shard = hash_slot(key) * num_shards / HASH_SLOTS`. **Same slot ⟹ same internal shard** (shard is a pure function of slot). So same-hash-tag keys never span internal shards.
  - `frogdb-server/crates/core/src/shard/keyspace_coordinator.rs`, `shard/dispatch_vll.rs`, `shard/vll.rs`, and `conn_command.rs` (search `participants`, `unique shard`, `requires_same_slot`, `CROSSSLOT`, `allow_cross_slot_standalone`): determine what triggers the VLL multi-shard path for a transaction, and whether cross-slot EXEC in standalone mode is (a) VLL-routed and committed, or (b) CROSSSLOT/EXECABORT-rejected.
  - `frogdb-server/crates/config/src/server.rs` (`allow_cross_slot_standalone`, default `false`, sim sets `true`).

- [ ] **Step 4: Record the two-outcome decision.** Write the conclusion into the Task-8 commit message and use it to select the response-legality check in Task 7:
  - **Outcome A — cross-slot EXEC is VLL-routed and *committed* in standalone mode.** Then the generator's cross-slot transactions already exercise the VLL-EXEC path (previously "zero coverage"). Tasks 6/7 keep a deliberate minority of cross-slot EXECs and assert they *commit consistently* (partition + linearize per key). Same-slot-but-cross-internal-shard EXEC is **structurally unreachable** (same slot ⟹ same shard) — note this and defer any single-shard-vs-VLL distinction to Phase-4b's shard-driver harness (spec scenario 3, sparse participants).
  - **Outcome B — cross-slot EXEC is *rejected* (CROSSSLOT/EXECABORT).** Then the sweep failure is exactly the documented model gap; Task 4 makes the model accept the errored EXEC as a no-op, Task 6 biases toward same-slot (which commit), and Task 7 asserts cross-slot EXECs *are rejected* (pin CROSSSLOT). VLL-EXEC then has no generator-reachable coverage under this sharding scheme → follow-up note for Phase-4b.

- [ ] **Step 5: Remove any throwaway instrumentation.** Confirm `git status` is clean (this task commits nothing). If you added debug prints, delete them.

---

### Task 4: Errored/aborted EXEC encoding in `KVModel` (component 1a)

Teach the model that an aborted/rejected transaction (WATCH-abort **or** CROSSSLOT/EXECABORT error) is a legal **no-op** (state unchanged), so a per-key Kv sub-history containing such an EXEC is no longer poisoned. WATCH-abort (`None`) is already accepted (`kv.rs:294-300`); this adds the error case. The shared predicate `is_errored_exec_result` is the `ERR:` cross-crate contract.

**Known edge (accept, document):** a *committed* EXEC whose FIRST sub-command errored (e.g. INCR on a non-integer) would also encode as `"ERR:…|…"` and be misread as rejected. The generator's vocabulary (set/get/incr/del on numeric values) cannot produce sub-command type errors, and a misread would surface as a noisy WGL failure (state advanced but model treated it as no-op), never as a silent pass — so the prefix contract is safe here. Note it in the predicate's doc comment for the Phase-4b triage reader.

**Files:**
- Modify: `frogdb-server/crates/testing/src/partition.rs` — add `pub fn is_errored_exec_result`.
- Modify: `frogdb-server/crates/testing/src/models/kv.rs` — accept the error result in the `exec` arm.
- Modify: `frogdb-server/crates/testing/src/lib.rs` — re-export `is_errored_exec_result`.

**Interfaces:**
- Produces: `pub fn is_errored_exec_result(result: &bytes::Bytes) -> bool` (true iff the recorded EXEC result denotes an aborted/rejected transaction — i.e. begins with `"ERR:"`).

- [ ] **Step 1: Write the failing test.** Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/testing/src/models/kv.rs`:

```rust
    #[test]
    fn test_kv_exec_errored_is_legal_and_unchanged() {
        // A CROSSSLOT/EXECABORT EXEC lands in the history as Some("ERR:...").
        // It is a rejected transaction: legal, state unchanged (like a nil abort).
        let mut state = KVState::default();
        state.store.insert(Bytes::from("k"), Bytes::from("orig"));
        let new_state = KVModel::step(
            &state,
            "exec",
            &[
                Bytes::from("1"),
                Bytes::from("set"),
                Bytes::from("2"),
                Bytes::from("k"),
                Bytes::from("new"),
            ],
            Some(&Bytes::from("ERR:EXECABORT Transaction discarded because of previous errors.")),
        )
        .expect("errored EXEC must be accepted as a no-op");
        assert_eq!(new_state.store.get(&Bytes::from("k")), Some(&Bytes::from("orig")));
    }
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `just test frogdb-testing test_kv_exec_errored`
Expected: FAIL — the current `Some(result_bytes)` arm compares `"ERR:EXECABORT …"` to the computed `expected` (`"OK"`) and returns `None`.

- [ ] **Step 3: Add the predicate.** Append to `frogdb-server/crates/testing/src/partition.rs`:

```rust
/// True iff a recorded EXEC/transaction result denotes an aborted or rejected
/// transaction rather than a committed one. The turmoil recorder encodes a
/// server error reply (CROSSSLOT / EXECABORT / …) as the pipe-safe marker
/// `"ERR:<message>"` (see `OperationHistory::encode_array_result`); a
/// WATCH-abort is instead recorded as `None`. Callers treat both as no-ops.
pub fn is_errored_exec_result(result: &Bytes) -> bool {
    result.starts_with(b"ERR:")
}
```

- [ ] **Step 4: Accept the error in `KVModel::exec`.** In `frogdb-server/crates/testing/src/models/kv.rs`, in the `"exec"` arm, change the final `match result { … }` so the `Some(result_bytes)` branch short-circuits on an errored result before comparing to `expected`:

```rust
                // Verify result matches expected
                match result {
                    // A nil EXEC is a WATCH-aborted transaction: legal, no change.
                    None => Some(state.clone()),
                    Some(result_bytes) => {
                        // A CROSSSLOT/EXECABORT error EXEC (recorder marker
                        // "ERR:...") is a rejected transaction: also a legal
                        // no-op that leaves state unchanged.
                        if crate::partition::is_errored_exec_result(result_bytes) {
                            return Some(state.clone());
                        }
                        let result_str = String::from_utf8_lossy(result_bytes);
                        if result_str == expected {
                            Some(current_state)
                        } else {
                            None
                        }
                    }
                }
```

(The empty-transaction early-return at `num_cmds == 0` is unchanged.)

- [ ] **Step 5: Re-export.** In `frogdb-server/crates/testing/src/lib.rs`, extend the `pub use partition::{…}` line:

```rust
pub use partition::{default_keys_of, is_errored_exec_result, partition_by_key};
```

- [ ] **Step 6: Run tests to verify they pass.**

Run: `just test frogdb-testing test_kv_exec && just test frogdb-testing partition`
Expected: PASS.

- [ ] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/models/kv.rs frogdb-server/crates/testing/src/partition.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): KVModel treats errored/CROSSSLOT EXEC as a legal no-op"
```

---

### Task 5: Conservation checkers honor aborted/errored EXECs (component 1a)

`check_watch_no_false_negative` treats any `exec` with `result.is_some()` as **committed** (`conservation.rs:472`), and `check_tx_sum_conservation` treats only `result.is_none()` as aborted (`:281`). Both now mis-classify an errored EXEC (`Some("ERR:…")`) as committed — the former would count its sub-commands as interfering writes, the latter would apply its deltas. Add a shared `exec_committed` helper.

**Files:**
- Modify: `frogdb-server/crates/testing/src/conservation.rs`

**Interfaces:**
- Adds a private `fn exec_committed(result: Option<&Bytes>) -> bool` = `result.is_some_and(|r| !is_errored_exec_result(r))`. Used by both checkers and by `writer_between`'s exec branch.

- [ ] **Step 1: Write the failing tests.** Add to the `#[cfg(test)] mod tests` block in `frogdb-server/crates/testing/src/conservation.rs`:

```rust
    #[test]
    fn watch_errored_exec_is_not_a_commit() {
        // Watcher's EXEC is CROSSSLOT-rejected (ERR:) despite an interfering
        // write: a rejected transaction did not commit, so it is NOT a false
        // negative.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "set", vec![b("k"), b("z")]);
        h.respond(other, Some(b("OK")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("ERR:EXECABORT Transaction discarded because of previous errors.")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }

    #[test]
    fn tx_sum_ignores_errored_exec() {
        // An errored EXEC applied no deltas; counting them would falsely leak.
        let mut h = History::new();
        let op = h.invoke(1, "exec", vec![b("1"), b("incrby"), b("2"), b("b"), b("5")]);
        h.respond(op, Some(b("ERR:CROSSSLOT Keys in request don't hash to the same slot")));
        let keys = vec![b("a"), b("b")];
        assert!(check_tx_sum_conservation(&h, &keys, 100).is_ok());
    }

    #[test]
    fn watch_errored_other_exec_not_a_writer() {
        // Another client's EXEC that was CROSSSLOT-rejected is not an
        // interfering write, so the watcher's committed EXEC is fine.
        let mut h = History::new();
        let w = h.invoke(1, "watch", vec![b("k")]);
        h.respond(w, Some(b("OK")));
        let other = h.invoke(2, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("z")]);
        h.respond(other, Some(b("ERR:CROSSSLOT Keys in request don't hash to the same slot")));
        let e = h.invoke(1, "exec", vec![b("1"), b("set"), b("2"), b("k"), b("v")]);
        h.respond(e, Some(b("OK")));
        assert!(check_watch_no_false_negative(&h).is_ok());
    }
```

- [ ] **Step 2: Run tests to verify they fail.**

Run: `just test frogdb-testing errored_exec && just test frogdb-testing tx_sum_ignores`
Expected: FAIL — `watch_errored_exec_is_not_a_commit` and `watch_errored_other_exec_not_a_writer` report a `WatchFalseNegative`; `tx_sum_ignores_errored_exec` reports a `SumMismatch`.

- [ ] **Step 3: Write the implementation.** In `frogdb-server/crates/testing/src/conservation.rs`, add to the top-of-file `use`:

```rust
use crate::partition::{default_keys_of, is_errored_exec_result, parse_exec_commands};
```

Add the helper (place it near `is_write`):

```rust
/// True iff an `exec` op's recorded result denotes a committed transaction (a
/// non-nil, non-errored result). A `None` (WATCH-abort) or an `"ERR:…"`
/// (CROSSSLOT/EXECABORT) result is NOT a commit.
fn exec_committed(result: Option<&Bytes>) -> bool {
    result.is_some_and(|r| !is_errored_exec_result(r))
}
```

In `check_tx_sum_conservation`, change the `"exec"` arm's guard from `if op.result.is_none() { continue; }` to:

```rust
            "exec" => {
                if !exec_committed(op.result.as_ref()) {
                    continue; // aborted or CROSSSLOT-rejected: applied no deltas
                }
                for (name, cargs) in parse_exec_commands(&op.args).unwrap_or_default() {
                    delta += cmd_delta(&name, &cargs, &keyset);
                }
            }
```

In `check_watch_no_false_negative`, change the committed-EXEC guard from `if op.result.is_some() {` to `if exec_committed(op.result.as_ref()) {`.

In `writer_between`, change the other-client exec branch guard from `op.function == "exec" && op.result.is_some()` to `op.function == "exec" && exec_committed(op.result.as_ref())`.

- [ ] **Step 4: Run tests to verify they pass.**

Run: `just test frogdb-testing conservation && just test frogdb-testing watch_ && just test frogdb-testing tx_sum`
Expected: PASS (new tests + all pre-existing conservation tests).

- [ ] **Step 5: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/conservation.rs
git commit -m "feat(testing): conservation checkers treat errored/CROSSSLOT EXEC as non-committed"
```

---

### Task 6: Generator — same-slot transaction grouping + minority cross-slot (component 1b)

Bias `TxHeavy` so most WATCH/EXEC transactions draw all their keys from **one hash-tag group** (same slot ⇒ same internal shard ⇒ a fast-path single-shard EXEC that commits), while keeping a deliberate minority of cross-slot combos to pin the rejection/VLL behavior Task 3 characterized. Today `gen_kv`/`gen_exec` pick each sub-command key independently from `families.kv` (whose keys have *distinct* tags `{t0}kv0`, `{t1}kv1`, …), so a multi-key EXEC almost always spans slots.

**Files:**
- Modify: `frogdb-server/crates/testing/src/workload.rs`

**Interfaces:**
- `gen_exec` gains a `same_slot: bool` parameter. `gen_kv` in `TxHeavy` chooses `same_slot = true` ~85% of the time. A `same_slot` transaction confines every sub-command key **and** any preceding WATCH to a single kv key-group sharing one tag; a cross-slot transaction draws keys freely.

- [ ] **Step 1: Extend the deterministic key-group helper.** The current `build_key_families` gives each kv key a distinct tag. Add a helper that groups the kv keys by shared tag so a "same-slot group" exists. Since a single kv key trivially shares its own slot, the simplest sound construction is: a same-slot transaction picks **one** kv key and reuses it for every sub-command (all identical key ⇒ trivially same slot), while WATCH targets that same key. Add to `frogdb-server/crates/testing/src/workload.rs`:

```rust
/// A same-slot transaction confines all keys to a single group sharing one
/// hash tag. With the current per-key distinct-tag key space, the guaranteed
/// same-slot group is a single key reused across sub-commands; document that
/// widening the key space to multi-key same-tag groups is a follow-up.
fn same_slot_group<'a>(keys: &'a [Bytes], rng: &mut StdRng) -> &'a Bytes {
    pick(keys, rng)
}
```

- [ ] **Step 2: Write the failing test.** Add to the `#[cfg(test)] mod tests` block in `workload.rs`:

```rust
    #[test]
    fn txheavy_mostly_same_slot_exec() {
        // In TxHeavy, the strong majority of EXECs must be single-slot: every
        // sub-command key hashes to the same slot as the first (so the server
        // commits on the fast path rather than rejecting/needing VLL). A
        // deliberate minority may still be cross-slot.
        use crate::partition::default_keys_of;

        fn slot(k: &[u8]) -> u16 {
            // CRC16-XMODEM over the hash tag, mirroring the server's hash_slot.
            let tagged = {
                let s = k.iter().position(|&b| b == b'{');
                match s {
                    Some(si) => {
                        let rest = &k[si + 1..];
                        match rest.iter().position(|&b| b == b'}') {
                            Some(ei) if ei > 0 => &rest[..ei],
                            _ => k,
                        }
                    }
                    None => k,
                }
            };
            let mut crc: u16 = 0;
            for &byte in tagged {
                crc ^= (byte as u16) << 8;
                for _ in 0..8 {
                    crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
                }
            }
            crc % 16384
        }

        let mut same = 0u32;
        let mut cross = 0u32;
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::TxHeavy, 4, 40);
            for op in w.clients.iter().flat_map(|c| &c.ops).filter(|o| o.command == "exec") {
                // Classify by SUB-COMMAND count, not key count: default_keys_of
                // DEDUPS (exec_keys, partition.rs), so a same-slot EXEC that
                // reuses one key yields keys.len() == 1 and must count as same.
                let num_cmds = op
                    .args
                    .first()
                    .and_then(|n| String::from_utf8_lossy(n).parse::<usize>().ok())
                    .unwrap_or(0);
                if num_cmds < 2 {
                    continue;
                }
                let keys = default_keys_of("exec", &op.args);
                let s0 = slot(&keys[0]);
                if keys.len() == 1 || keys.iter().all(|k| slot(k) == s0) {
                    same += 1;
                } else {
                    cross += 1;
                }
            }
        }
        assert!(same > cross * 3, "expected same-slot EXECs to dominate: same={same} cross={cross}");
        assert!(cross > 0, "keep a deliberate minority of cross-slot EXECs: cross={cross}");
    }
```

- [ ] **Step 3: Run test to verify it fails.**

Run: `just test frogdb-testing txheavy_mostly_same_slot`
Expected: FAIL — the current generator makes nearly all multi-key EXECs cross-slot (`same` ≈ 0).

- [ ] **Step 4: Implement the biasing.** In `workload.rs`, change `gen_exec`'s signature to accept `same_slot: bool` and thread the key choice; change `gen_kv`'s two `gen_exec(...)` call sites to pass a same-slot decision:

```rust
fn gen_kv(profile: Profile, families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let keys = &families.kv;
    let tx_bias = matches!(profile, Profile::TxHeavy);
    let r = rng.random_range(0..100);
    let t = think(rng);
    if tx_bias && r < 35 {
        // ~85% of transactions are single-slot (commit on the fast path).
        let same_slot = rng.random_range(0..100) < 85;
        gen_exec(keys, rng, ops, t, same_slot);
        return;
    }
    if tx_bias && r < 45 {
        let k = pick(keys, rng).clone();
        push_op(ops, "watch", vec![k], t);
        return;
    }
    if tx_bias && r < 50 {
        push_op(ops, "discard", vec![], t);
        return;
    }
    let k = pick(keys, rng).clone();
    match rng.random_range(0..4) {
        0 => push_op(ops, "set", vec![k, num_value(rng)], t),
        1 => push_op(ops, "get", vec![k], t),
        2 => push_op(ops, "incr", vec![k], t),
        _ => push_op(ops, "del", vec![k], t),
    }
}

fn gen_exec(keys: &[Bytes], rng: &mut StdRng, ops: &mut Vec<ScriptedOp>, think_ms: u64, same_slot: bool) {
    let n = rng.random_range(1..4);
    // Same-slot: reuse ONE group key for every sub-command; cross-slot: pick
    // each sub-command's key independently.
    let group = same_slot_group(keys, rng).clone();
    let mut args: Vec<Bytes> = vec![Bytes::from(n.to_string())];
    for _ in 0..n {
        let k = if same_slot { group.clone() } else { pick(keys, rng).clone() };
        match rng.random_range(0..4) {
            0 => {
                args.push(Bytes::from("set"));
                args.push(Bytes::from("2"));
                args.push(k);
                args.push(num_value(rng));
            }
            1 => {
                args.push(Bytes::from("get"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
            2 => {
                args.push(Bytes::from("incr"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
            _ => {
                args.push(Bytes::from("del"));
                args.push(Bytes::from("1"));
                args.push(k);
            }
        }
    }
    push_op(ops, "exec", args, think_ms);
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `just test frogdb-testing txheavy && just test frogdb-testing generate_is_deterministic && just test frogdb-testing only_phase1_vocabulary`
Expected: PASS. (Determinism and the vocabulary allowlist are unchanged — no new command names introduced.)

- [ ] **Step 6: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/workload.rs
git commit -m "feat(testing): bias TxHeavy toward same-slot EXEC groups, keep minority cross-slot"
```

---

### Task 7: Response-legality — pin the CROSSSLOT/commit outcome (component 1b)

Add a stage-1 (response-legality) check in the pipeline that pins the cross-slot EXEC behavior Task 3 established, so a regression in the server's slot enforcement is caught. This lives in the server-test crate (it needs `hash_slot`, which is not in `frogdb-testing`). **Select the check body per the Task-3 outcome.**

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs`

**Interfaces:**
- Adds `fn check_exec_slot_discipline(history: &History, report: &mut InvariantReport)`, called from `check_all_with` in stage 1 alongside `check_response_legality`.

- [ ] **Step 1: Write the failing test.** Add to the `#[cfg(test)] mod tests` block in `invariants.rs` (uses `frogdb_testing::is_errored_exec_result` from Task 4 and `super::super::sim_harness::hash_slot`):

```rust
    #[test]
    fn cross_slot_exec_discipline_flagged() {
        use crate::common::sim_harness::hash_slot;
        // Two keys in different slots inside one EXEC. Outcome B (reject):
        // committing it is illegal. Outcome A (VLL commit): committing it is
        // legal. The test pins whichever Task 3 established; shown for Outcome B.
        let a = Bytes::from("{t0}kv0");
        let b = Bytes::from("{t1}kv1");
        assert_ne!(hash_slot(&a), hash_slot(&b), "fixture keys must differ in slot");

        let mut h = History::new();
        let e = h.invoke(
            1,
            "exec",
            vec![
                Bytes::from("2"),
                Bytes::from("set"), Bytes::from("2"), a.clone(), Bytes::from("1"),
                Bytes::from("set"), Bytes::from("2"), b.clone(), Bytes::from("2"),
            ],
        );
        // A committed cross-slot EXEC (array reply -> "OK|OK").
        h.respond(e, Some(Bytes::from("OK|OK")));

        let mut report = InvariantReport::default();
        check_exec_slot_discipline(&h, &mut report);
        // For Outcome B this MUST flag; for Outcome A, invert the assertion.
        assert!(!report.violations.is_empty(), "cross-slot commit must be flagged under Outcome B");
    }
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/cross_slot_exec_discipline/)'`
Expected: FAIL — `cannot find function check_exec_slot_discipline`.

- [ ] **Step 3: Implement the check (Outcome B shown; invert for Outcome A).** In `invariants.rs`, add:

```rust
use frogdb_testing::is_errored_exec_result;
use super::sim_harness::hash_slot;

/// Pin the server's slot discipline for transactions. Per Task 3:
/// - Outcome B (cross-slot EXEC is rejected): a *committed* EXEC whose
///   sub-command keys span >1 hash slot is a CROSSSLOT-enforcement regression.
/// - Outcome A (cross-slot EXEC is VLL-committed): flip the condition to flag a
///   cross-slot EXEC that was *rejected* (`ERR:`) instead of committing.
fn check_exec_slot_discipline(history: &History, report: &mut InvariantReport) {
    for op in history.completed_operations() {
        if op.function != "exec" {
            continue;
        }
        let keys = default_keys_of("exec", &op.args);
        if keys.len() < 2 {
            continue;
        }
        let s0 = hash_slot(&keys[0]);
        let cross_slot = keys.iter().any(|k| hash_slot(k) != s0);
        if !cross_slot {
            continue;
        }
        let committed = op
            .result
            .as_ref()
            .is_some_and(|r| !is_errored_exec_result(r));
        // Outcome B:
        if committed {
            report.violations.push(format!(
                "op {} (exec) committed across {} hash slots (CROSSSLOT not enforced)",
                op.id,
                keys.iter().map(|k| hash_slot(k)).collect::<std::collections::BTreeSet<_>>().len()
            ));
        }
    }
}
```

Call it from `check_all_with`'s stage 1, right after `check_response_legality(history, &mut report);`:

```rust
    check_exec_slot_discipline(history, &mut report);
```

- [ ] **Step 4: Run tests to verify they pass.**

Run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/cross_slot_exec_discipline|clean_history_passes/)'`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/invariants.rs
git commit -m "test(sim): pin cross-slot EXEC slot discipline in the response-legality stage"
```

---

### Task 8: Un-ignore `seed_sweep_txheavy` and wire it into `just concurrency`

With Tasks 4–7 in place, the TxHeavy sweep should be green. Remove the `#[ignore]`, refresh the stale comment, and add it to the CI tier.

**Files:**
- Modify: `frogdb-server/crates/server/tests/concurrency_workload.rs`
- Modify: `Justfile`

- [ ] **Step 1: Un-ignore and refresh the comment.** In `concurrency_workload.rs`, replace the `#[ignore = "…"]` attribute and the stale root-cause comment block (`:119-153`) with a short note that the model/checker now accept aborted/errored EXECs and the generator biases toward same-slot transactions. Keep the `#[test] fn seed_sweep_txheavy` body (the `for seed in 0..20u64` loop). The refreshed header:

```rust
// TxHeavy seed sweep (CI per-PR tier). Transactions are biased toward
// single-slot key groups (which commit); a deliberate minority are cross-slot
// to pin the server's slot discipline (see check_exec_slot_discipline). KVModel
// and the conservation checkers accept an aborted (nil) or errored ("ERR:…",
// CROSSSLOT/EXECABORT) EXEC as a legal no-op, so a rejected transaction no
// longer poisons its per-key Kv sub-history.
#[test]
fn seed_sweep_txheavy() {
    for seed in 0..20u64 {
        let report = run_and_check(seed, Profile::TxHeavy, 4, 30, 2);
        if !report.passed() {
            let path = write_repro_for(seed, Profile::TxHeavy, 4, 30, 2);
            panic!(
                "seed {seed} (TxHeavy) violated invariants: {:?}\nrepro: {}",
                report.violations,
                path.display()
            );
        }
    }
}
```

- [ ] **Step 2: Run the sweep (watchdog protocol; commit Tasks 1–7 first).**

```bash
cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_txheavy/)' \
  > /tmp/txheavy_sweep.log 2>&1
```

Verify liveness (`wc -c /tmp/txheavy_sweep.log` growing, or CPU accumulating). Expected: PASS. **If a seed fails**, triage per the bug workflow: harness/model gap → fix the harness (do not weaken a checker); genuine server bug → capture the repro, add a pinned named regression in the `regressions` module, and fix in-plan.

- [ ] **Step 3: Wire into `just concurrency`.** In the `Justfile` `concurrency:` recipe (`:74-81`), add the TxHeavy sweep line after the existing short-workload sweep:

```make
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/simulation/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_short_workloads/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_txheavy/)'
```

- [ ] **Step 4: Commit.**

```bash
git add frogdb-server/crates/server/tests/concurrency_workload.rs Justfile
git commit -m "test(sim): un-ignore TxHeavy seed sweep and wire it into just concurrency"
```

---

## Component 2 — Consumer groups (`Family::StreamGroup`)

**Model routing decision (documented here, referenced by Tasks 9/11/12):** a stream key receives *both* plain stream ops (`xadd`/`xlen`/`xread`) *and* group ops (`xgroup`/`xreadgroup`/`xack`/`xclaim`/`xautoclaim`/`xpending`). Per-key WGL needs **one** model per key, so `StreamGroupModel` is a **superset of `StreamModel`**: it embeds the stream contents (entries + `last_id`) *and* per-group state (last-delivered id + PEL), and its `step` handles the full vocabulary. The pipeline routes the **entire Stream family** to `StreamGroupModel`; the existing `StreamModel` stays for back-compat but is no longer the pipeline's stream model. The generator emits both plain and group ops on stream keys.

**Bounded option set (documented simplification, strict within it):** to keep the sequential semantics unambiguous under turmoil (where wall-clock idle time is nondeterministic), the generator emits only: `XGROUP CREATE key group $|0 MKSTREAM`; `XREADGROUP GROUP g c COUNT n STREAMS key >` (new) and `… key 0` (own-pending history re-read); `XACK`; `XPENDING key group` (summary form); `XCLAIM key group consumer 0 id…` (min-idle-time 0, no JUSTID → returns entries, +1 delivery count); `XAUTOCLAIM key group consumer 0 0 COUNT n JUSTID` (min-idle-time 0, JUSTID → returns ids, no count change). `min-idle-time` is always `0` (claims are unconditional; idle timing is not modeled). No blocking `XREADGROUP` (Phase-4b). No `NOACK`. The generator never `XDEL`s, so the deleted-entry-in-PEL edge cases (returned-as-nil / GC on claim) do not arise; the model documents but does not exercise them. Redis-7/Valkey semantics are identical for all six commands.

### Task 9: `StreamGroupModel`

**Files:**
- Create: `frogdb-server/crates/testing/src/models/stream_group.rs`
- Modify: `frogdb-server/crates/testing/src/models/mod.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Produces: `StreamGroupModel`, `StreamGroupState { streams: HashMap<Bytes, StreamGroupData> }`, `StreamGroupData { entries: Vec<(StreamId, Vec<Bytes>)>, last_id: StreamId, groups: HashMap<Bytes, Group> }`, `Group { last_delivered: StreamId, pel: BTreeMap<StreamId, PelEntry> }`, `PelEntry { consumer: Bytes, delivery_count: u64 }`. Reuses `StreamId`/`parse_id` from `stream.rs` (make them `pub(crate)`).
- **Result encodings** (produced by the recorder canonicalizer in Task 12):
  - `xadd` → `"ms-seq"`; `xlen` → int; `xread`/`xreadgroup` entries → `"id,f,v,…"` per entry, entries `|`-joined; empty read → `None`.
  - `xgroup` (CREATE) → `"OK"`; a BUSYGROUP/other error → `None`.
  - `xack` → int (entries removed from the group PEL).
  - `xpending` (summary) → `"total|min|max|consumer:count,…"` with consumers sorted; empty PEL → `"0"`.
  - `xclaim` (no JUSTID) → claimed entries `"id,f,v,…"` `|`-joined (ownership → consumer, `delivery_count += 1`); nothing claimable → `None`.
  - `xautoclaim` (JUSTID) → `"cursor;id,id,…"` (cursor `ms-seq`, then claimed ids comma-joined; no count change); nothing → `"0-0;"`.

- [ ] **Step 1: Make `StreamId`/`parse_id` reusable.** In `frogdb-server/crates/testing/src/models/stream.rs`, change `fn parse_id` to `pub(crate) fn parse_id`. `StreamId` is already `pub`. Add module wiring to `frogdb-server/crates/testing/src/models/mod.rs`:

```rust
mod stream_group;
pub use stream_group::{Group, PelEntry, StreamGroupData, StreamGroupModel, StreamGroupState};
```

- [ ] **Step 2: Write the failing test.** Create `frogdb-server/crates/testing/src/models/stream_group.rs` with a test module first:

```rust
//! Consumer-group stream model (superset of StreamModel): XADD/XLEN/XREAD plus
//! XGROUP CREATE, non-blocking XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM.
//! PEL: entry id -> (owning consumer, delivery count). Bounded option set:
//! min-idle-time is always 0, no NOACK, no blocking, no XDEL edge cases.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checker::check_linearizability;
    use crate::history::History;
    use bytes::Bytes;

    fn b(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    fn step(s: &StreamGroupState, f: &str, args: &[&str], r: Option<&str>) -> Option<StreamGroupState> {
        let a: Vec<Bytes> = args.iter().map(|x| b(x)).collect();
        StreamGroupModel::step(s, f, &a, r.map(b).as_ref())
    }

    #[test]
    fn group_happy_path() {
        let s = StreamGroupState::default();
        // XADD st 1-1 f v ; XADD st 2-1 f w
        let s = step(&s, "xadd", &["st", "1-1", "f", "v"], Some("1-1")).unwrap();
        let s = step(&s, "xadd", &["st", "2-1", "f", "w"], Some("2-1")).unwrap();
        // XGROUP CREATE st g1 0 MKSTREAM
        let s = step(&s, "xgroup", &["CREATE", "st", "g1", "0", "MKSTREAM"], Some("OK")).unwrap();
        // XREADGROUP GROUP g1 c1 COUNT 10 STREAMS st '>' -> both entries, PEL={1-1,2-1}->c1
        let s = step(&s, "xreadgroup", &["GROUP", "g1", "c1", "COUNT", "10", "STREAMS", "st", ">"],
            Some("1-1,f,v|2-1,f,w")).unwrap();
        // XPENDING st g1 (summary) -> total|min|max|c1:2
        assert!(step(&s, "xpending", &["st", "g1"], Some("2|1-1|2-1|c1:2")).is_some());
        // Re-read own pending from 0 -> both again, no count change.
        assert!(step(&s, "xreadgroup", &["GROUP", "g1", "c1", "STREAMS", "st", "0"],
            Some("1-1,f,v|2-1,f,w")).is_some());
        // XACK st g1 1-1 -> 1 removed.
        let s = step(&s, "xack", &["st", "g1", "1-1"], Some("1")).unwrap();
        // XACK again -> 0 (already acked).
        assert!(step(&s, "xack", &["st", "g1", "1-1"], Some("0")).is_some());
        // XPENDING now -> only 2-1 pending for c1.
        assert!(step(&s, "xpending", &["st", "g1"], Some("1|2-1|2-1|c1:1")).is_some());
    }

    #[test]
    fn readgroup_gt_delivers_only_new() {
        let s = StreamGroupState::default();
        let s = step(&s, "xadd", &["st", "1-1", "f", "v"], Some("1-1")).unwrap();
        let s = step(&s, "xgroup", &["CREATE", "st", "g", "0"], Some("OK")).unwrap();
        // First '>' delivers 1-1.
        let s = step(&s, "xreadgroup", &["GROUP", "g", "c", "STREAMS", "st", ">"], Some("1-1,f,v")).unwrap();
        // Second '>' with nothing new -> nil (None).
        assert!(step(&s, "xreadgroup", &["GROUP", "g", "c", "STREAMS", "st", ">"], None).is_some());
        // Claiming a nonexistent-in-PEL is a no-op; wrong delivery result rejected.
        assert!(step(&s, "xreadgroup", &["GROUP", "g", "c", "STREAMS", "st", ">"], Some("1-1,f,v")).is_none());
    }

    #[test]
    fn xclaim_transfers_and_increments() {
        let s = StreamGroupState::default();
        let s = step(&s, "xadd", &["st", "5-0", "f", "v"], Some("5-0")).unwrap();
        let s = step(&s, "xgroup", &["CREATE", "st", "g", "0"], Some("OK")).unwrap();
        let s = step(&s, "xreadgroup", &["GROUP", "g", "c1", "STREAMS", "st", ">"], Some("5-0,f,v")).unwrap();
        // XCLAIM to c2 (min-idle 0) -> returns the entry, delivery_count now 2.
        let s = step(&s, "xclaim", &["st", "g", "c2", "0", "5-0"], Some("5-0,f,v")).unwrap();
        assert!(step(&s, "xpending", &["st", "g"], Some("1|5-0|5-0|c2:1")).is_some());
        // XAUTOCLAIM JUSTID from 0 to c1 -> cursor 0-0, id 5-0, no count change.
        assert!(step(&s, "xautoclaim", &["st", "g", "c1", "0", "0", "COUNT", "10", "JUSTID"],
            Some("0-0;5-0")).is_some());
    }

    #[test]
    fn strict_rejects_unknown() {
        let s = StreamGroupState::default();
        assert!(step(&s, "sadd", &["st", "x"], Some("1")).is_none());
    }

    #[test]
    fn group_linearizable_end_to_end() {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(2, "xreadgroup", vec![b("GROUP"), b("g"), b("c"), b("STREAMS"), b("st"), b(">")]);
        h.respond(r, Some(b("1-1,f,v")));
        assert!(check_linearizability::<StreamGroupModel>(&h).is_linearizable);
    }
}
```

- [ ] **Step 3: Run test to verify it fails.**

Run: `just test frogdb-testing group_happy_path`
Expected: FAIL — `cannot find type StreamGroupModel`.

- [ ] **Step 4: Write the implementation.** Prepend to `frogdb-server/crates/testing/src/models/stream_group.rs`:

```rust
use super::Model;
use crate::models::stream::{parse_id, StreamId};
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};

/// One pending-entry-list record: which consumer owns it and how many times it
/// has been delivered.
#[derive(Debug, Clone)]
pub struct PelEntry {
    pub consumer: Bytes,
    pub delivery_count: u64,
}

/// One consumer group: the high-water delivered id and the group PEL.
#[derive(Debug, Clone, Default)]
pub struct Group {
    pub last_delivered: StreamId,
    /// entry id -> PEL record. BTreeMap so id order is free.
    pub pel: BTreeMap<StreamId, PelEntry>,
}

/// Per-key stream contents plus its consumer groups.
#[derive(Debug, Clone, Default)]
pub struct StreamGroupData {
    pub entries: Vec<(StreamId, Vec<Bytes>)>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, Group>,
}

impl StreamGroupData {
    fn entry_fields(&self, id: StreamId) -> Option<&Vec<Bytes>> {
        self.entries.iter().find(|(i, _)| *i == id).map(|(_, f)| f)
    }
}

/// Sequential model for streams with consumer groups (superset of StreamModel).
#[derive(Debug, Clone, Default)]
pub struct StreamGroupModel;

/// State for the consumer-group stream model.
#[derive(Debug, Clone, Default)]
pub struct StreamGroupState {
    pub streams: HashMap<Bytes, StreamGroupData>,
}

/// Encode an ordered set of `(id, fields)` as `"id,f,v,…"` entries `|`-joined.
fn encode_entries(entries: &[(StreamId, Vec<Bytes>)]) -> Option<String> {
    if entries.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for (id, fields) in entries {
        let mut one = vec![id.to_string()];
        one.extend(fields.iter().map(|f| String::from_utf8_lossy(f).to_string()));
        parts.push(one.join(","));
    }
    Some(parts.join("|"))
}

/// Read the trailing `STREAMS key id` triple from an xreadgroup arg list.
fn streams_key_id(args: &[Bytes]) -> Option<(Bytes, Bytes)> {
    let pos = args
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))?;
    let key = args.get(pos + 1)?.clone();
    let id = args.get(pos + 2)?.clone();
    Some((key, id))
}

impl Model for StreamGroupModel {
    type State = StreamGroupState;

    fn step(
        state: &Self::State,
        function: &str,
        args: &[Bytes],
        result: Option<&Bytes>,
    ) -> Option<Self::State> {
        match function {
            "xadd" => {
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return None;
                }
                let key = &args[0];
                let id_arg = &args[1];
                let fields: Vec<Bytes> = args[2..].to_vec();
                let assigned = parse_id(result?.as_ref())?;
                let last = state.streams.get(key).map(|s| s.last_id).unwrap_or_default();
                if assigned <= last {
                    return None;
                }
                if id_arg.as_ref() != b"*" && parse_id(id_arg)? != assigned {
                    return None;
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
                if result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(len)) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xread" => {
                if args.len() < 2 {
                    return None;
                }
                let after = parse_id(&args[1])?;
                let entries: Vec<(StreamId, Vec<Bytes>)> = state
                    .streams
                    .get(&args[0])
                    .map(|s| s.entries.iter().filter(|(id, _)| *id > after).cloned().collect())
                    .unwrap_or_default();
                let expected = encode_entries(&entries);
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xgroup" => {
                // Bounded: only `CREATE key group id [MKSTREAM]`.
                if args.len() < 4 || !args[0].eq_ignore_ascii_case(b"CREATE") {
                    return None;
                }
                let key = &args[1];
                let group = &args[2];
                let id_arg = &args[3];
                let exists = state
                    .streams
                    .get(key)
                    .is_some_and(|s| s.groups.contains_key(group));
                match result {
                    // BUSYGROUP or other error -> None: legal only if the group
                    // already existed (idempotent re-create attempt).
                    None => {
                        if exists {
                            Some(state.clone())
                        } else {
                            None
                        }
                    }
                    Some(r) if r.as_ref() == b"OK" => {
                        if exists {
                            return None; // OK requires the group did not exist
                        }
                        let mut new = state.clone();
                        let sd = new.streams.entry(key.clone()).or_default();
                        let last_delivered = if id_arg.as_ref() == b"$" {
                            sd.last_id
                        } else {
                            parse_id(id_arg)?
                        };
                        sd.groups.insert(group.clone(), Group { last_delivered, pel: BTreeMap::new() });
                        Some(new)
                    }
                    Some(_) => None,
                }
            }
            "xreadgroup" => {
                // GROUP g c [COUNT n] STREAMS key id
                let gi = args.iter().position(|a| a.eq_ignore_ascii_case(b"GROUP"))?;
                let group = args.get(gi + 1)?.clone();
                let consumer = args.get(gi + 2)?.clone();
                let (key, id) = streams_key_id(args)?;
                let count = args
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
                    .and_then(|ci| args.get(ci + 1))
                    .and_then(|c| String::from_utf8_lossy(c).parse::<usize>().ok())
                    .unwrap_or(usize::MAX);

                let mut new = state.clone();
                let sd = new.streams.entry(key.clone()).or_default();
                let Some(g) = sd.groups.get_mut(&group) else {
                    // NOGROUP -> recorded as None; accept only as None.
                    return if result.is_none() { Some(state.clone()) } else { None };
                };
                if id.as_ref() == b">" {
                    // New messages after last_delivered, up to count.
                    let last = g.last_delivered;
                    let to_deliver: Vec<(StreamId, Vec<Bytes>)> = sd
                        .entries
                        .iter()
                        .filter(|(eid, _)| *eid > last)
                        .take(count)
                        .cloned()
                        .collect();
                    let expected = encode_entries(&to_deliver);
                    if result.map(|r| String::from_utf8_lossy(r).to_string()) != expected {
                        return None;
                    }
                    for (eid, _) in &to_deliver {
                        g.last_delivered = g.last_delivered.max(*eid);
                        g.pel.insert(*eid, PelEntry { consumer: consumer.clone(), delivery_count: 1 });
                    }
                    Some(new)
                } else {
                    // History re-read of the consumer's own pending with id > `id`.
                    let after = parse_id(&id)?;
                    let own: Vec<(StreamId, Vec<Bytes>)> = g
                        .pel
                        .iter()
                        .filter(|(eid, p)| **eid > after && p.consumer == consumer)
                        .filter_map(|(eid, _)| sd.entry_fields(*eid).map(|f| (*eid, f.clone())))
                        .collect();
                    // Empty own-pending re-read is `[[key, []]]` -> None encoding.
                    let expected = encode_entries(&own);
                    if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                        // Re-read refreshes idle but does NOT change delivery_count.
                        Some(new)
                    } else {
                        None
                    }
                }
            }
            "xack" => {
                if args.len() < 3 {
                    return None;
                }
                let key = &args[0];
                let group = &args[1];
                let mut new = state.clone();
                let mut removed = 0i64;
                if let Some(sd) = new.streams.get_mut(key)
                    && let Some(g) = sd.groups.get_mut(group)
                {
                    for id_arg in &args[2..] {
                        if let Some(id) = parse_id(id_arg)
                            && g.pel.remove(&id).is_some()
                        {
                            removed += 1;
                        }
                    }
                }
                if result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(removed)) {
                    Some(new)
                } else {
                    None
                }
            }
            "xpending" => {
                // Summary form: key group -> "total|min|max|c:n,c:n" (sorted); "0" if empty.
                if args.len() < 2 {
                    return None;
                }
                let g = state.streams.get(&args[0]).and_then(|s| s.groups.get(&args[1]));
                let expected = match g {
                    None => "0".to_string(),
                    Some(g) if g.pel.is_empty() => "0".to_string(),
                    Some(g) => {
                        let total = g.pel.len();
                        let min = g.pel.keys().next().unwrap().to_string();
                        let max = g.pel.keys().next_back().unwrap().to_string();
                        let mut per: BTreeMap<String, u64> = BTreeMap::new();
                        for p in g.pel.values() {
                            *per.entry(String::from_utf8_lossy(&p.consumer).to_string()).or_default() += 1;
                        }
                        let consumers = per
                            .into_iter()
                            .map(|(c, n)| format!("{c}:{n}"))
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("{total}|{min}|{max}|{consumers}")
                    }
                };
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == Some(expected) {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "xclaim" => {
                // key group consumer min-idle-time id... (no JUSTID): returns
                // claimed entries, transfers ownership, delivery_count += 1.
                if args.len() < 5 {
                    return None;
                }
                let key = &args[0];
                let group = &args[1];
                let consumer = &args[2];
                let mut new = state.clone();
                let Some(sd) = new.streams.get_mut(key) else { return None };
                let entries_snapshot = sd.entries.clone();
                let Some(g) = sd.groups.get_mut(group) else { return None };
                let mut claimed: Vec<(StreamId, Vec<Bytes>)> = Vec::new();
                for id_arg in &args[4..] {
                    let Some(id) = parse_id(id_arg) else { continue };
                    if let Some(p) = g.pel.get_mut(&id) {
                        p.consumer = consumer.clone();
                        p.delivery_count += 1;
                        if let Some((_, f)) = entries_snapshot.iter().find(|(i, _)| *i == id) {
                            claimed.push((id, f.clone()));
                        }
                    }
                }
                claimed.sort_by_key(|(i, _)| *i);
                let expected = encode_entries(&claimed);
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == expected {
                    Some(new)
                } else {
                    None
                }
            }
            "xautoclaim" => {
                // key group consumer min-idle start [COUNT n] JUSTID: returns
                // "cursor;id,id" (ids only), transfers ownership, no count change.
                let justid = args.iter().any(|a| a.eq_ignore_ascii_case(b"JUSTID"));
                if args.len() < 5 || !justid {
                    return None; // bounded set uses JUSTID only
                }
                let key = &args[0];
                let group = &args[1];
                let consumer = &args[2];
                let start = parse_id(&args[4])?;
                let count = args
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
                    .and_then(|ci| args.get(ci + 1))
                    .and_then(|c| String::from_utf8_lossy(c).parse::<usize>().ok())
                    .unwrap_or(100);
                let mut new = state.clone();
                let Some(sd) = new.streams.get_mut(key) else { return None };
                let Some(g) = sd.groups.get_mut(group) else { return None };
                let ids: Vec<StreamId> = g.pel.range(start..).map(|(id, _)| *id).take(count).collect();
                for id in &ids {
                    if let Some(p) = g.pel.get_mut(id) {
                        p.consumer = consumer.clone(); // JUSTID: no delivery_count change
                    }
                }
                // Cursor 0-0 = scan complete (bounded workloads claim all at once).
                let cursor = StreamId::default();
                let expected = format!(
                    "{};{}",
                    cursor,
                    ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(",")
                );
                if result.map(|r| String::from_utf8_lossy(r).to_string()) == Some(expected) {
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

- [ ] **Step 5: Run tests to verify they pass.**

Run: `just test frogdb-testing group_ && just test frogdb-testing xclaim && just test frogdb-testing readgroup`
Expected: PASS.

- [ ] **Step 6: Re-export from the crate root.** In `frogdb-server/crates/testing/src/lib.rs`, extend the `pub use models::{…}` line to add `Group, PelEntry, StreamGroupData, StreamGroupModel, StreamGroupState`.

- [ ] **Step 7: Format, lint, verify consumers, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing && just check frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/models
git commit -m "feat(testing): strict StreamGroupModel (XGROUP/XREADGROUP/XACK/XPENDING/XCLAIM/XAUTOCLAIM + PEL)"
```

---

### Task 10: PEL conservation checker + fault-injection self-tests

Whole-history scan (not WGL) enforcing the four PEL invariants from the spec: (i) every delivered-unacked entry is in exactly one consumer's PEL; (ii) no entry both acked and still pending; (iii) delivery counts are monotonic non-decreasing per entry; (iv) nothing lost — every XADD'd entry is either readable via `>`, PEL'd, or acked. Follows the existing `ConservationViolation` + fault-injection pattern.

**Files:**
- Modify: `frogdb-server/crates/testing/src/conservation.rs`
- Modify: `frogdb-server/crates/testing/src/fault_injection.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Adds `ConservationViolation` variants `PelDoubleOwned`, `PelAckedButPending`, `StreamEntryLost` (each carrying ids + a `#[error(..)]` message).
- Adds `pub fn check_pel_conservation(history: &History) -> Result<(), ConservationViolation>`.
- Adds fault-injection helpers `pub fn double_own_pel(history: &History) -> History` and `pub fn lose_stream_entry(history: &History) -> History`.

- [ ] **Step 1: Write the failing checker test.** Add to the `#[cfg(test)] mod tests` block in `conservation.rs`:

```rust
    fn group_history() -> History {
        // XADD 1-1 ; XGROUP CREATE ; XREADGROUP > (c1 owns 1-1, dc=1) ; XACK 1-1
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(2, "xreadgroup", vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")]);
        h.respond(r, Some(b("1-1,f,v")));
        h
    }

    #[test]
    fn pel_ok_when_delivered_then_acked() {
        let mut h = group_history();
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        assert!(check_pel_conservation(&h).is_ok());
    }

    #[test]
    fn pel_detects_acked_but_still_pending() {
        let mut h = group_history();
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        // A later re-read reports 1-1 still pending for c1 -> both acked & pending.
        let rr = h.invoke(2, "xreadgroup", vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b("0")]);
        h.respond(rr, Some(b("1-1,f,v")));
        assert!(matches!(
            check_pel_conservation(&h),
            Err(ConservationViolation::PelAckedButPending { .. })
        ));
    }

    #[test]
    fn pel_detects_double_owned() {
        // 1-1 claimed to two consumers concurrently reported as pending for both.
        let mut h = group_history();
        let c1 = h.invoke(2, "xpending", vec![b("st"), b("g")]);
        h.respond(c1, Some(b("1|1-1|1-1|c1:1")));
        // A claim moves 1-1 to c2, but a stale reader still sees c1 owning it too.
        let cl = h.invoke(3, "xclaim", vec![b("st"), b("g"), b("c2"), b("0"), b("1-1")]);
        h.respond(cl, Some(b("1-1,f,v")));
        let p2 = h.invoke(3, "xpending", vec![b("st"), b("g")]);
        h.respond(p2, Some(b("2|1-1|1-1|c1:1,c2:1"))); // two owners of the same id
        assert!(matches!(
            check_pel_conservation(&h),
            Err(ConservationViolation::PelDoubleOwned { .. })
        ));
    }
```

- [ ] **Step 2: Run test to verify it fails.**

Run: `just test frogdb-testing pel_`
Expected: FAIL — `cannot find function check_pel_conservation`.

- [ ] **Step 3: Write the checker.** Add the four variants to the `ConservationViolation` enum in `conservation.rs`:

```rust
    /// A stream entry is reported pending for two different consumers at once.
    #[error("PEL entry {id:?} double-owned: consumers {a:?} and {b:?} (op {op})")]
    PelDoubleOwned { id: String, a: String, b: String, op: u64 },
    /// A stream entry was acked yet later reported still pending.
    #[error("PEL entry {id:?} acked by op {ack_op} but reported pending by op {pending_op}")]
    PelAckedButPending { id: String, ack_op: u64, pending_op: u64 },
    /// An XADD'd stream entry was skipped over: a later `>` read delivered a
    /// higher id while this entry was never delivered, PEL'd, or acked.
    #[error("stream entry {id:?} (added by op {added_by}) was lost (skipped by a later '>' read)")]
    StreamEntryLost { id: String, added_by: u64 },
```

Append the checker (after `check_watch_no_false_negative`). It parses the same canonical encodings the recorder produces (Task 12):

```rust
/// PEL conservation for consumer-group streams. Scans the whole history:
/// (i) no entry pending for two consumers at once (partial — see note below);
/// (ii) no entry both acked and later reported pending; (iii) no entry skipped:
/// a `>` read that starts after an entry's XADD completes and delivers a
/// HIGHER id must have delivered (or previously delivered/acked) that entry —
/// `>` delivers in id order, so a higher id with the entry absent everywhere
/// means the server lost it. Entries added after the last read are
/// legitimately undelivered and are NOT flagged. Delivery-count monotonicity
/// is unobservable with summary-form XPENDING; deferred to Phase-4b's
/// extended-form vocabulary.
pub fn check_pel_conservation(history: &History) -> Result<(), ConservationViolation> {
    use std::collections::{HashMap, HashSet};

    // Parse "id,f,v|..." into the entry ids it delivered.
    fn delivered_ids(r: &Bytes) -> Vec<String> {
        String::from_utf8_lossy(r)
            .split('|')
            .filter(|e| !e.is_empty())
            .filter_map(|e| e.split(',').next().map(str::to_string))
            .collect()
    }

    // "ms-seq" -> (ms, seq) for order comparison.
    fn id_tuple(s: &str) -> Option<(u64, u64)> {
        let (ms, seq) = s.split_once('-')?;
        Some((ms.parse().ok()?, seq.parse().ok()?))
    }

    let mut added: HashMap<String, u64> = HashMap::new(); // id -> add op
    let mut ever_pending: HashSet<String> = HashSet::new();
    let mut acked: HashMap<String, u64> = HashMap::new(); // id -> ack op

    for op in history.completed_operations() {
        match op.function.as_str() {
            "xadd" => {
                if let Some(r) = &op.result {
                    added.entry(String::from_utf8_lossy(r).to_string()).or_insert(op.id);
                }
            }
            "xreadgroup" => {
                if let Some(r) = &op.result {
                    for id in delivered_ids(r) {
                        ever_pending.insert(id);
                    }
                }
            }
            "xclaim" => {
                if let Some(r) = &op.result {
                    for id in delivered_ids(r) {
                        ever_pending.insert(id);
                    }
                }
            }
            "xack" => {
                for id_arg in op.args.iter().skip(2) {
                    acked.entry(String::from_utf8_lossy(id_arg).to_string()).or_insert(op.id);
                }
            }
            "xpending" => {
                if let Some(r) = &op.result {
                    // "total|min|max|c:n,c:n"; a per-consumer count is a summary,
                    // not per-id ownership, so the double-owned check keys off a
                    // total that exceeds the distinct pending ids observed.
                    let s = String::from_utf8_lossy(r);
                    if s != "0"
                        && let Some(total) = s.split('|').next().and_then(|t| t.parse::<usize>().ok())
                    {
                        // Count consumer:count pairs; if summed per-consumer
                        // counts exceed `total`, an id is owned twice.
                        if let Some(consumers) = s.split('|').nth(3) {
                            let summed: usize = consumers
                                .split(',')
                                .filter_map(|c| c.rsplit(':').next())
                                .filter_map(|n| n.parse::<usize>().ok())
                                .sum();
                            if summed > total {
                                let (a, b) = consumers
                                    .split_once(',')
                                    .unwrap_or((consumers, consumers));
                                return Err(ConservationViolation::PelDoubleOwned {
                                    id: s.split('|').nth(1).unwrap_or("?").to_string(),
                                    a: a.to_string(),
                                    b: b.to_string(),
                                    op: op.id,
                                });
                            }
                        }
                        for id in s.split('|').nth(1).into_iter().chain(s.split('|').nth(2)) {
                            ever_pending.insert(id.to_string());
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // (ii) acked-but-pending: an id present in a re-read AFTER its ack.
    for op in history.completed_operations() {
        if op.function == "xreadgroup"
            && let Some(r) = &op.result
        {
            for id in delivered_ids(r) {
                if let Some(&ack_op) = acked.get(&id)
                    && op.invoke_time > history.completed_operations()
                        .iter()
                        .find(|o| o.id == ack_op)
                        .map_or(u64::MAX, |o| o.return_time)
                {
                    return Err(ConservationViolation::PelAckedButPending {
                        id,
                        ack_op,
                        pending_op: op.id,
                    });
                }
            }
        }
    }

    // (iii) nothing skipped: an entry absent from every delivery/PEL/ack while
    // a `>` read that STARTED after its add COMPLETED delivered a HIGHER id.
    // Restricted to `>` reads: a "0" re-read shows old PEL entries whose
    // delivery may predate this add, which implies nothing about a skip.
    for (id, add_op) in &added {
        if ever_pending.contains(id) || acked.contains_key(id) {
            continue;
        }
        let Some(eid) = id_tuple(id) else { continue };
        let add_return = history
            .completed_operations()
            .iter()
            .find(|o| o.id == *add_op)
            .map_or(u64::MAX, |o| o.return_time);
        let skipped = history.completed_operations().iter().any(|o| {
            o.function == "xreadgroup"
                && o.args.last().is_some_and(|a| a.as_ref() == b">")
                && o.invoke_time > add_return
                && o.result.as_ref().is_some_and(|r| {
                    delivered_ids(r)
                        .iter()
                        .any(|d| id_tuple(d).is_some_and(|dt| dt > eid))
                })
        });
        if skipped {
            return Err(ConservationViolation::StreamEntryLost {
                id: id.clone(),
                added_by: *add_op,
            });
        }
    }

    Ok(())
}
```

Note: a `>` read with COUNT truncates the *tail* of the in-order delivery, never the middle, so COUNT cannot cause a false skip. Entries added after the last `>` read are never flagged (no later read exists).

- [ ] **Step 4: Run the checker tests.**

Run: `just test frogdb-testing pel_`
Expected: PASS.

- [ ] **Step 5: Add fault-injection self-tests.** In `frogdb-server/crates/testing/src/fault_injection.rs`, add the two corruption helpers and their self-tests (mirroring the existing `drop_delivery` pattern). Prepend helpers after the existing ones:

```rust
/// Corruption: rewrite an XPENDING summary so its per-consumer counts sum to
/// more than its total (an id owned by two consumers). Trips
/// `check_pel_conservation` (PelDoubleOwned).
pub fn double_own_pel(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    for o in recs.iter_mut() {
        if o.kind == OpKind::Return
            && o.function == "xpending"
            && let Some(r) = &o.result
        {
            let s = String::from_utf8_lossy(r).to_string();
            if s != "0" {
                let mut f: Vec<&str> = s.split('|').collect();
                if f.len() >= 4 {
                    // total=2 but only one id, owned by c1:1 AND c2:1.
                    f[0] = "2";
                    let joined = format!("{}|{}|{}|c1:1,c2:1", f[0], f[1], f[2]);
                    o.result = Some(bytes::Bytes::from(joined));
                    break;
                }
            }
        }
    }
    rebuild(&recs)
}

/// Corruption: remove the FIRST delivered entry from the first `>` xreadgroup
/// result that delivered two or more entries. The dropped id is then never
/// delivered/PEL'd/acked while the same read shows a higher id — trips
/// `check_pel_conservation` (StreamEntryLost).
pub fn lose_stream_entry(history: &History) -> History {
    let mut recs: Vec<Operation> = history.operations().to_vec();
    for o in recs.iter_mut() {
        if o.kind == OpKind::Return
            && o.function == "xreadgroup"
            && let Some(r) = &o.result
        {
            let s = String::from_utf8_lossy(r).to_string();
            if let Some((_, rest)) = s.split_once('|') {
                o.result = Some(bytes::Bytes::from(rest.to_string()));
                break;
            }
        }
    }
    rebuild(&recs)
}
```

Add to the `#[cfg(test)] mod tests` block in `fault_injection.rs`:

```rust
    fn valid_group_history() -> History {
        let mut h = History::new();
        let a = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a, Some(b("1-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(2, "xreadgroup", vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")]);
        h.respond(r, Some(b("1-1,f,v")));
        let ack = h.invoke(2, "xack", vec![b("st"), b("g"), b("1-1")]);
        h.respond(ack, Some(b("1")));
        h
    }

    #[test]
    fn original_group_history_passes() {
        use crate::conservation::check_pel_conservation;
        assert!(check_pel_conservation(&valid_group_history()).is_ok());
    }

    #[test]
    fn double_own_pel_is_caught() {
        use crate::conservation::check_pel_conservation;
        // Add an xpending observation to corrupt.
        let mut h = valid_group_history();
        let p = h.invoke(2, "xpending", vec![b("st"), b("g")]);
        h.respond(p, Some(b("1|1-1|1-1|c1:1")));
        let corrupt = double_own_pel(&h);
        assert!(check_pel_conservation(&corrupt).is_err());
    }

    #[test]
    fn lose_stream_entry_is_caught() {
        use crate::conservation::{check_pel_conservation, ConservationViolation};
        // Two entries delivered in one '>' read; dropping the first from the
        // reply makes it a skipped (lost) entry.
        let mut h = History::new();
        let a1 = h.invoke(1, "xadd", vec![b("st"), b("1-1"), b("f"), b("v")]);
        h.respond(a1, Some(b("1-1")));
        let a2 = h.invoke(1, "xadd", vec![b("st"), b("2-1"), b("f"), b("w")]);
        h.respond(a2, Some(b("2-1")));
        let g = h.invoke(1, "xgroup", vec![b("CREATE"), b("st"), b("g"), b("0")]);
        h.respond(g, Some(b("OK")));
        let r = h.invoke(2, "xreadgroup", vec![b("GROUP"), b("g"), b("c1"), b("STREAMS"), b("st"), b(">")]);
        h.respond(r, Some(b("1-1,f,v|2-1,f,w")));
        assert!(check_pel_conservation(&h).is_ok());
        let corrupt = lose_stream_entry(&h);
        assert!(matches!(
            check_pel_conservation(&corrupt),
            Err(ConservationViolation::StreamEntryLost { .. })
        ));
    }
```

- [ ] **Step 6: Run the self-tests.**

Run: `just test frogdb-testing pel_ && just test frogdb-testing double_own && just test frogdb-testing lose_stream && just test frogdb-testing original_group`
Expected: PASS.

- [ ] **Step 7: Re-export + commit.** In `frogdb-server/crates/testing/src/lib.rs`, add `check_pel_conservation` to the `pub use conservation::{…}` list.

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/conservation.rs frogdb-server/crates/testing/src/fault_injection.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): PEL conservation checker + fault-injection self-tests for consumer groups"
```

---

### Task 11: Generator — `Family::StreamGroup`

Add a sixth internal family whose keys are streams used with consumer groups. A `StreamGroup` client script sets up a group once, then interleaves XADD (producer) and XREADGROUP `>`/XACK/XCLAIM/XAUTOCLAIM/XPENDING (consumers) on the same key, honoring the bounded option set.

**Files:**
- Modify: `frogdb-server/crates/testing/src/workload.rs`

**Interfaces:**
- Adds `Family::StreamGroup` with its own key bucket `{tN}sg`. Adds `gen_stream_group`. Extends the vocabulary allowlist test with the six group commands.

- [ ] **Step 1: Write the failing allowlist test.** Extend `only_phase1_vocabulary_emitted` in `workload.rs` to include the group commands, and add a routability guard:

```rust
    #[test]
    fn stream_group_vocabulary_emitted_and_routable() {
        use crate::partition::default_keys_of;
        const GROUP_OPS: &[&str] = &["xgroup", "xreadgroup", "xack", "xpending", "xclaim", "xautoclaim"];
        let mut seen: std::collections::HashSet<&str> = Default::default();
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                if GROUP_OPS.contains(&op.command.as_str()) {
                    seen.insert(GROUP_OPS.iter().find(|g| **g == op.command).unwrap());
                    // Every group op must route to a non-empty key set.
                    assert!(!default_keys_of(&op.command, &op.args).is_empty(),
                        "group op {} not routable", op.command);
                }
            }
        }
        assert!(seen.contains("xreadgroup") && seen.contains("xack"),
            "generator must emit consumer-group ops; saw {seen:?}");
    }
```

Also add the six commands to the `ALLOWED` array in `only_phase1_vocabulary_emitted`.

- [ ] **Step 2: Run test to verify it fails.**

Run: `just test frogdb-testing stream_group_vocabulary`
Expected: FAIL — no group ops are emitted; `seen` is empty.

- [ ] **Step 3: Implement the family.** In `workload.rs`:
  - Add `StreamGroup` to `enum Family` and to `const FAMILIES` (now length 6; update the `[Family; 5]`/`5` literals to `6`).
  - Add a `stream_group: Vec<Bytes>` bucket to `KeyFamilies`, a `"sg"` kind row in `build_key_families` (the loop already tags each family; extend the `[(&mut …, kind)]` array), and a `KeyFamilies::get` arm.
  - Extend `pick_family` so `Mixed` and a modest `TxHeavy`/`BlockingHeavy` slice can pick `StreamGroup`.
  - Add `gen_stream_group`, dispatched from `gen_op`. Because a group must exist before XREADGROUP, the generator emits a `xgroup CREATE … MKSTREAM` as the client's *first* touch of each stream-group key (tracked per client), then interleaves the rest:

```rust
fn gen_stream_group(
    families: &KeyFamilies,
    rng: &mut StdRng,
    created: &mut std::collections::HashSet<Vec<u8>>,
    ops: &mut Vec<ScriptedOp>,
) {
    let keys = &families.stream_group;
    let k = pick(keys, rng).clone();
    let group = Bytes::from("g0");
    let t = think(rng);
    // Ensure the group exists (MKSTREAM makes the stream too). Idempotent
    // BUSYGROUP re-creates are accepted by the model as no-ops.
    if created.insert(k.to_vec()) {
        push_op(ops, "xgroup", vec![Bytes::from("CREATE"), k.clone(), group.clone(),
            Bytes::from("0"), Bytes::from("MKSTREAM")], t);
        return;
    }
    let consumer = Bytes::from(format!("c{}", rng.random_range(0..3)));
    match rng.random_range(0..7) {
        0 => push_op(ops, "xadd", vec![k, Bytes::from("*"),
            Bytes::from(format!("f{}", rng.random_range(0..3))), alnum_value(rng)], t),
        1 => push_op(ops, "xreadgroup", vec![Bytes::from("GROUP"), group, consumer,
            Bytes::from("COUNT"), Bytes::from("10"), Bytes::from("STREAMS"), k, Bytes::from(">")], t),
        2 => push_op(ops, "xreadgroup", vec![Bytes::from("GROUP"), group, consumer,
            Bytes::from("STREAMS"), k, Bytes::from("0")], t),
        3 => push_op(ops, "xpending", vec![k, group], t),
        4 => {
            // XAUTOCLAIM JUSTID from 0 (claims all eligible to `consumer`).
            push_op(ops, "xautoclaim", vec![k, group, consumer, Bytes::from("0"),
                Bytes::from("0"), Bytes::from("COUNT"), Bytes::from("10"), Bytes::from("JUSTID")], t);
        }
        5 => {
            // XCLAIM wire no-op (id 0-0 is never in a PEL): exercises the
            // command/recorder/model path; live-id claims happen via XAUTOCLAIM.
            push_op(ops, "xclaim", vec![k, group, consumer, Bytes::from("0"), Bytes::from("0-0")], t);
        }
        _ => {
            // XACK the group PEL's low id space is unknown to the generator, so
            // ack a plausible recent id space via a wildcard-free explicit id is
            // not possible; instead ack nothing-harmful by acking id "0-0"
            // (count 0). Real acks happen via the model observing prior reads.
            push_op(ops, "xack", vec![k, group, Bytes::from("0-0")], t);
        }
    }
}
```

  Thread a per-client `created: HashSet<Vec<u8>>` through `generate`/`gen_op` alongside `block_offset` (add the parameter to `gen_op` and construct the set once per client, like `block_offset`). Update `gen_op`'s `match fam` to dispatch `Family::StreamGroup => gen_stream_group(families, rng, created, ops)`.

  **Note (documented gap):** XACK/XCLAIM need concrete pending ids the generator cannot know statically (auto `*` ids are server-assigned). The bounded generator therefore emits XACK/XCLAIM with id `0-0` (harmless count-0 / no-op) so the *command vocabulary* is exercised and the model/recorder path is covered, while real ack/claim *state transitions* are driven by XREADGROUP `>` + `0` re-reads whose ids the model tracks. Exercising XACK/XCLAIM against live server-assigned ids requires the runner to thread back the ids returned by a prior XREADGROUP — a follow-up (Phase-4b) noted here.

- [ ] **Step 4: Run tests to verify they pass.**

Run: `just test frogdb-testing stream_group_vocabulary && just test frogdb-testing only_phase1_vocabulary && just test frogdb-testing generate_is_deterministic`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/workload.rs
git commit -m "feat(testing): generator Family::StreamGroup consumer-group vocabulary"
```

---

### Task 12: Harness routing for consumer groups

Route the Stream family to `StreamGroupModel`, teach `default_keys_of` the group commands, and canonicalize the group RESP replies into the model encodings.

**Files:**
- Modify: `frogdb-server/crates/testing/src/partition.rs` — `default_keys_of` group arms.
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs` — `family_of` + `run_bounded` route Stream → `StreamGroupModel`; wire `check_pel_conservation`.
- Modify: `frogdb-server/crates/server/tests/common/sim_harness.rs` — `canonicalize_result` arms for the group commands.

- [ ] **Step 1: `default_keys_of` group arms.** In `partition.rs::default_keys_of`, add arms so every group command routes to its stream key:

```rust
        // Consumer-group ops route to their single stream key.
        "xgroup" => args.get(1).cloned().into_iter().collect(), // CREATE key group ...
        "xack" | "xpending" | "xclaim" | "xautoclaim" => args.first().cloned().into_iter().collect(),
        "xreadgroup" => args
            .iter()
            .position(|a| a.eq_ignore_ascii_case(b"STREAMS"))
            .and_then(|p| args.get(p + 1))
            .cloned()
            .into_iter()
            .collect(),
```

Add a unit test in `partition.rs` asserting each routes to `["st"]`/`["k"]` as appropriate; run `just test frogdb-testing default_keys_of`.

- [ ] **Step 2: Pipeline routing.** In `invariants.rs`:
  - Import `StreamGroupModel` and `check_pel_conservation` from `frogdb_testing`.
  - In `family_of`, add the group commands to the `Stream` arm:

```rust
        "xadd" | "xlen" | "xread" | "xgroup" | "xreadgroup" | "xack" | "xpending" | "xclaim"
        | "xautoclaim" => Some(Family::Stream),
```

  - In `run_bounded`, change `Family::Stream => check_linearizability_bounded::<StreamModel>(sub, max_states)` to use `StreamGroupModel` (the superset covers plain + group ops).
  - In stage 3 of `check_all_with`, add: `if let Err(e) = check_pel_conservation(history) { report.violations.push(format!("PEL conservation: {e}")); }`

- [ ] **Step 3: Recorder canonicalization.** In `sim_harness.rs::canonicalize_result`, add arms translating the group RESP replies to the model encodings (mirroring the existing `xread` arm which already yields `"id,f,v|…"`):
  - `xreadgroup` → reuse `canonicalize_xread` (same nested shape).
  - `xpending` (summary array `[total, min, max, [[consumer, count], …]]`) → `"total|min|max|c:n,c:n"` (consumers sorted), or `"0"` when total is 0.
  - `xclaim` → same as `xread` entries `"id,f,v|…"`.
  - `xautoclaim` (`[cursor, [entries…], [deleted…]]`, JUSTID makes entries just ids) → `"cursor;id,id"`.
  - `xgroup` → **error replies canonicalize to `None`** (BUSYGROUP from racing CREATEs is expected and the model's encoding table requires `None`; the default error path would record `"ERR:…"` and poison the key). Same for `xreadgroup`: a NOGROUP error reply → `None`. `+OK`/`:N` (xgroup OK, xack) already convert.

  Write a failing recorder test first (like the existing `hgetall_is_canonicalized_sorted`), e.g. an XPENDING summary array canonicalizes to `"2|1-1|2-1|c1:2"`; run it under `--features turmoil`, implement the arm, confirm green.

- [ ] **Step 4: Regression gate.**

Run: `just check frogdb-server && cargo nextest run -p frogdb-server --features turmoil -E 'test(/canonical|clean_history_passes/)'`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/testing/src/partition.rs frogdb-server/crates/server/tests/common/invariants.rs frogdb-server/crates/server/tests/common/sim_harness.rs
git commit -m "test(sim): route Stream family to StreamGroupModel, wire PEL checker, canonicalize group replies"
```

---

## Component 3 — Scripting (`Family::Script`)

**Script pool + translation decision (documented here):** a script invocation is recorded as a **composite pseudo-op** whose `function` is a stable script name and whose effect is expressible by the existing `KVModel`/`ListModel`, so per-key WGL routes it to the target key's normal family (no separate model). The runner sends `EVAL`/`EVALSHA` on the wire but records the pseudo-op + the script's return value. The fixed pool (all **single-key**, `KEYS[1]` only):

| Pseudo-op (`command`) | Lua (single key `KEYS[1]`, args `ARGV`) | Model effect | Result encoding |
|---|---|---|---|
| `script_getset` | `local o=redis.call('GET',KEYS[1]); redis.call('SET',KEYS[1],ARGV[1]); return o` | get old, set new | old value or `nil` (KVModel) |
| `script_cincr` | `if redis.call('EXISTS',KEYS[1])==1 then return redis.call('INCR',KEYS[1]) else return -1 end` | INCR iff key exists, else -1 | new int, or `-1` (KVModel) |
| `script_setnx_get` | `if redis.call('EXISTS',KEYS[1])==0 then redis.call('SET',KEYS[1],ARGV[1]) end; return redis.call('GET',KEYS[1])` | set iff absent, return current | current value (KVModel) |
| `script_lpush_llen` | `redis.call('LPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])` | lpush then llen | new length int (ListModel) |
| `script_rpush_llen` | `redis.call('RPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])` | rpush then llen | new length int (ListModel) |

`SCRIPT LOAD` populates the SHA cache; the generator mixes `EVAL` and `EVALSHA` of the same scripts across clients (the SHA-cache population race). **EVALSHA NOSCRIPT handling:** the runner tries `EVALSHA <sha>`; if the reply is `-NOSCRIPT`, it transparently falls back to `EVAL <src>` and records the single logical pseudo-op with the fallback's result (documented; the fallback is invisible to the model). No FUNCTION, no cross-shard scripts (all pool scripts are single-key so `hash_slot` is trivially satisfied even with cluster enforcement).

### Task 13: Script pseudo-ops in `KVModel` / `ListModel`

**Files:**
- Modify: `frogdb-server/crates/testing/src/models/kv.rs` — `script_getset`, `script_cincr`, `script_setnx_get` arms.
- Modify: `frogdb-server/crates/testing/src/models/list.rs` — `script_lpush_llen`, `script_rpush_llen` arms.
- Modify: `frogdb-server/crates/testing/src/partition.rs` — `default_keys_of` routes `script_*` by `args[0]`.

**Interfaces:**
- Each `script_*` pseudo-op has args `[key, argv0?]` and a result matching the table. `default_keys_of("script_*", args) = [args[0]]`.

- [ ] **Step 1: Write the failing model tests.** Add to `kv.rs` tests:

```rust
    #[test]
    fn script_getset_returns_old_sets_new() {
        let mut s = KVState::default();
        s.store.insert(Bytes::from("k"), Bytes::from("old"));
        let s = KVModel::step(&s, "script_getset", &[Bytes::from("k"), Bytes::from("new")], Some(&Bytes::from("old"))).unwrap();
        assert_eq!(s.store.get(&Bytes::from("k")), Some(&Bytes::from("new")));
        // Absent key -> old is nil.
        let s2 = KVState::default();
        assert!(KVModel::step(&s2, "script_getset", &[Bytes::from("x"), Bytes::from("v")], None).is_some());
    }

    #[test]
    fn script_cincr_only_when_present() {
        let mut s = KVState::default();
        s.store.insert(Bytes::from("k"), Bytes::from("5"));
        assert!(KVModel::step(&s, "script_cincr", &[Bytes::from("k")], Some(&Bytes::from("6"))).is_some());
        // Absent -> -1, unchanged.
        let s2 = KVState::default();
        let s2 = KVModel::step(&s2, "script_cincr", &[Bytes::from("x")], Some(&Bytes::from("-1"))).unwrap();
        assert!(!s2.store.contains_key(&Bytes::from("x")));
    }
```

Add to `list.rs` tests:

```rust
    #[test]
    fn script_lpush_llen_returns_new_length() {
        let s = ListState::default();
        let s = ListModel::step(&s, "script_lpush_llen", &[b("k"), b("a")], Some(&b("1"))).unwrap();
        assert!(ListModel::step(&s, "script_rpush_llen", &[b("k"), b("b")], Some(&b("2"))).is_some());
    }
```

- [ ] **Step 2: Run to verify failure.**

Run: `just test frogdb-testing script_getset && just test frogdb-testing script_lpush`
Expected: FAIL — the permissive `_ => Some(state.clone())` (KVModel) would *accept* any result, so `script_getset` with a wrong old value would NOT be rejected; assert the *rejection* path in the test too, then implement the strict arms. (ListModel is strict `_ => None`, so its test fails to compile-match until the arm exists.)

- [ ] **Step 3: Implement the arms.** In `kv.rs`, add before the `_ =>` fallthrough:

```rust
            "script_getset" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let old = state.store.get(key).cloned();
                if result == old.as_ref() {
                    let mut new = state.clone();
                    new.store.insert(key.clone(), args[1].clone());
                    Some(new)
                } else {
                    None
                }
            }
            "script_cincr" => {
                if args.is_empty() {
                    return None;
                }
                let key = &args[0];
                if let Some(cur) = state.store.get(key) {
                    let n = String::from_utf8_lossy(cur).parse::<i64>().ok()? + 1;
                    if result.is_some_and(|r| String::from_utf8_lossy(r).parse::<i64>().ok() == Some(n)) {
                        let mut new = state.clone();
                        new.store.insert(key.clone(), Bytes::from(n.to_string()));
                        Some(new)
                    } else {
                        None
                    }
                } else if result.is_some_and(|r| r.as_ref() == b"-1") {
                    Some(state.clone())
                } else {
                    None
                }
            }
            "script_setnx_get" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                if !new.store.contains_key(key) {
                    new.store.insert(key.clone(), args[1].clone());
                }
                let cur = new.store.get(key).cloned();
                if result == cur.as_ref() {
                    Some(new)
                } else {
                    None
                }
            }
```

In `list.rs`, add before the `_ => None`:

```rust
            "script_lpush_llen" | "script_rpush_llen" => {
                if args.len() < 2 {
                    return None;
                }
                let key = &args[0];
                let mut new = state.clone();
                let list = new.lists.entry(key.clone()).or_default();
                if function == "script_lpush_llen" {
                    list.push_front(args[1].clone());
                } else {
                    list.push_back(args[1].clone());
                }
                let len = list.len() as i64;
                if super::expect_int(result, len) {
                    Some(new)
                } else {
                    None
                }
            }
```

In `partition.rs::default_keys_of`, add `"script_getset" | "script_cincr" | "script_setnx_get" | "script_lpush_llen" | "script_rpush_llen"` to the single-key `args.first()` arm.

- [ ] **Step 4: Run tests to verify they pass.**

Run: `just test frogdb-testing script_ && just test frogdb-testing default_keys_of`
Expected: PASS.

- [ ] **Step 5: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/models frogdb-server/crates/testing/src/partition.rs
git commit -m "feat(testing): model script pseudo-ops (getset/cincr/setnx_get/lpush_llen/rpush_llen)"
```

---

### Task 14: Generator `Family::Script` + runner EVAL/EVALSHA wiring

Add a Script family that emits `script_*` pseudo-ops on KV/List keys, and teach the runner to execute them via `SCRIPT LOAD` + `EVAL`/`EVALSHA` with a NOSCRIPT→EVAL fallback, recording the pseudo-op + result.

**Files:**
- Modify: `frogdb-server/crates/testing/src/workload.rs` — `Family::Script`, `gen_script`, allowlist extension.
- Modify: `frogdb-server/crates/server/tests/common/workload_runner.rs` — script execution + NOSCRIPT fallback.
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs` — `family_of` routes `script_*` to Kv/List.

**Interfaces:**
- Generator: `Family::Script` picks a pool script + a target key from the matching family (`kv` for KV-effect scripts, `list` for list-effect scripts). A shared const `SCRIPT_POOL: &[(&str, &str)]` maps pseudo-op name → Lua source (single source of truth, imported by the runner via `frogdb_testing::workload::SCRIPT_POOL`).

- [ ] **Step 1: Publish the pool + failing allowlist test.** In `workload.rs`, add:

```rust
/// The fixed Lua script pool: (pseudo-op name, single-key Lua source). Shared
/// with the turmoil runner, which SCRIPT LOADs / EVALs / EVALSHAs these.
pub const SCRIPT_POOL: &[(&str, &str)] = &[
    ("script_getset", "local o=redis.call('GET',KEYS[1]); redis.call('SET',KEYS[1],ARGV[1]); return o"),
    ("script_cincr", "if redis.call('EXISTS',KEYS[1])==1 then return redis.call('INCR',KEYS[1]) else return -1 end"),
    ("script_setnx_get", "if redis.call('EXISTS',KEYS[1])==0 then redis.call('SET',KEYS[1],ARGV[1]) end; return redis.call('GET',KEYS[1])"),
    ("script_lpush_llen", "redis.call('LPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])"),
    ("script_rpush_llen", "redis.call('RPUSH',KEYS[1],ARGV[1]); return redis.call('LLEN',KEYS[1])"),
];
```

Add a failing test:

```rust
    #[test]
    fn script_vocabulary_emitted_and_routable() {
        use crate::partition::default_keys_of;
        let names: Vec<&str> = SCRIPT_POOL.iter().map(|(n, _)| *n).collect();
        let mut seen = false;
        for seed in 0..60 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                if names.contains(&op.command.as_str()) {
                    seen = true;
                    assert!(!default_keys_of(&op.command, &op.args).is_empty());
                }
            }
        }
        assert!(seen, "generator must emit script pseudo-ops");
    }
```

- [ ] **Step 2: Run to verify failure.** Run: `just test frogdb-testing script_vocabulary_emitted`. Expected: FAIL — no script ops emitted.

- [ ] **Step 3: Implement the family.** In `workload.rs`: add `Family::Script` to `enum Family` and `FAMILIES` (now 7); extend `pick_family` (`Mixed` and slices of the others) to select it; add `gen_script`:

```rust
fn gen_script(families: &KeyFamilies, rng: &mut StdRng, ops: &mut Vec<ScriptedOp>) {
    let (name, _) = SCRIPT_POOL[rng.random_range(0..SCRIPT_POOL.len())];
    let list_effect = name.contains("push_llen");
    let key = if list_effect { pick(&families.list, rng).clone() } else { pick(&families.kv, rng).clone() };
    let t = think(rng);
    // KV-effect scripts that write take an ARGV[1]; cincr takes none.
    let args = if name == "script_cincr" {
        vec![key]
    } else {
        vec![key, if list_effect { alnum_value(rng) } else { num_value(rng) }]
    };
    push_op(ops, name, args, t);
}
```

Dispatch it from `gen_op` (`Family::Script => gen_script(families, rng, ops)`). Add the five names to the `ALLOWED` allowlist.

- [ ] **Step 4: Runner execution + NOSCRIPT fallback.** In `workload_runner.rs::run_op`, add a `script_*` arm that maps the pseudo-op to a pool script, SCRIPT-LOADs it (idempotently, tracked per connection so the SHA cache race is exercised), and EVAL/EVALSHAs it:

```rust
        cmd if cmd.starts_with("script_") => run_script(history, client_id, op, stream, buf, acc).await,
```

Add:

```rust
/// Execute a pool script pseudo-op via EVAL or EVALSHA (alternating to race the
/// SHA cache), falling back to EVAL on NOSCRIPT. Records the pseudo-op + result.
async fn run_script(
    history: &Arc<Mutex<OperationHistory>>,
    client_id: u64,
    op: &frogdb_testing::ScriptedOp,
    stream: &mut TcpStream,
    buf: &mut [u8],
    acc: &mut Vec<u8>,
) -> Result<(), BoxError> {
    use tokio::io::AsyncWriteExt;
    let src = frogdb_testing::workload::SCRIPT_POOL
        .iter()
        .find(|(n, _)| *n == op.command)
        .map(|(_, s)| *s)
        .expect("unknown script pseudo-op");
    let key = op.args[0].clone();
    let argv: Vec<Bytes> = op.args[1..].to_vec();
    let op_id = {
        let mut h = history.lock().unwrap();
        h.record_invoke(client_id, op.command.to_uppercase(), op.args.clone())
    };
    // sha1(src) hex.
    let sha = hex_sha1(src.as_bytes());
    // Build EVALSHA sha numkeys key argv...
    let numkeys = Bytes::from("1");
    let mut evalsha: Vec<&[u8]> = vec![b"EVALSHA", sha.as_bytes(), numkeys.as_ref(), key.as_ref()];
    evalsha.extend(argv.iter().map(|a| a.as_ref()));
    stream.write_all(&encode_command(&evalsha)).await?;
    let mut reply = read_reply(stream, buf, acc).await?;
    if matches!(&reply, OperationResult::Error(e) if e.starts_with("NOSCRIPT")) {
        // Fallback: EVAL <src> numkeys key argv...
        let mut eval: Vec<&[u8]> = vec![b"EVAL", src.as_bytes(), numkeys.as_ref(), key.as_ref()];
        eval.extend(argv.iter().map(|a| a.as_ref()));
        stream.write_all(&encode_command(&eval)).await?;
        reply = read_reply(stream, buf, acc).await?;
    }
    {
        let mut h = history.lock().unwrap();
        h.record_return_canonical(op_id, client_id, &op.command, reply);
    }
    Ok(())
}
```

Provide a small local `hex_sha1` (use the `sha1` crate already in the workspace, or a vendored impl) computing the lowercase-hex SHA-1 of the script source — the same digest FrogDB assigns. **Verify** the exact digest scheme FrogDB uses for EVALSHA (`grep -rn "sha1\|Sha1" frogdb-server/crates/core/src/scripting`) and match it; if FrogDB uses a nonstandard digest, prefer always-EVAL plus a separate `SCRIPT LOAD` step that captures the server-returned SHA to use for EVALSHA (this also makes the SHA-cache race explicit and removes the digest-matching risk).

- [ ] **Step 5: Route the pseudo-ops in the pipeline.** In `invariants.rs::family_of`, add `script_getset|script_cincr|script_setnx_get` to the `Kv` arm and `script_lpush_llen|script_rpush_llen` to the `List` arm.

- [ ] **Step 6: Failing runner smoke test → green.** Add a `--features turmoil` test that a tiny Script-heavy workload runs and records complete script ops:

```rust
    #[test]
    fn tiny_script_workload_runs() {
        let w = Workload::generate(2, Profile::Mixed, 2, 8);
        let history = run_workload(&w, 1, true);
        assert!(history.is_complete());
    }
```

Run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/tiny_script_workload_runs/)'`
Expected: PASS.

- [ ] **Step 7: Format, lint, commit.**

Run: `just fmt frogdb-testing && just lint frogdb-testing && just check frogdb-server`

```bash
git add frogdb-server/crates/testing/src/workload.rs frogdb-server/crates/server/tests/common/workload_runner.rs frogdb-server/crates/server/tests/common/invariants.rs
git commit -m "feat(testing): generator Family::Script + runner EVAL/EVALSHA with NOSCRIPT fallback"
```

---

## Component 4 — Exact multi-waiter FIFO via `DEBUG WAITQUEUE` ordinals

**Mechanism decision (documented here, honest about feasibility):** the current FIFO checker uses invoke-time as a *proxy* for registration order and false-positives when two waiters' invokes overlap (`conservation.rs:204-211`), which is why the generator pins one blocking owner per key. `DEBUG WAITQUEUE` emits the true per-waiter `registration_seq` (`debug_conn_command.rs:574`), but the post-drain quiescence probe sees an empty queue. So registration order must be captured **mid-run** by a dedicated *prober* client that polls `DEBUG WAITQUEUE` on a tight sim-time cadence while waiters are parked, accumulating `(shard, key, conn_id) → registration_seq`. Correlation to the recorded pops uses `client_id → conn_id`, captured by having each client issue `CLIENT ID` right after connecting. The upgraded checker uses `registration_seq` order **authoritatively** for keys where ≥2 served waiters have known ordinals, and **falls back to the invoke-time proxy** where ordinals are missing — a strict improvement (removes false positives, keeps existing coverage). **Reliability enabler:** the multi-waiter path uses **long** blocking timeouts (several sim-seconds) and delayed producers so waiters stay parked long enough for the prober's cadence to observe them; this makes ordinal capture reliable rather than racy. Because ordinals are authoritative, the multi-waiter path **drops** the single-owner stagger.

### Task 15: Carry per-waiter registration ordinals into the testing snapshot

**Files:**
- Modify: `frogdb-server/crates/testing/src/quiescence.rs` — enrich `WaitQueueSnapshot` with per-waiter detail.
- Modify: `frogdb-server/crates/testing/src/lib.rs` — export the new struct.

**Interfaces:**
- Add `pub struct WaiterOrdinal { pub key: Vec<u8>, pub conn_id: u64, pub registration_seq: u64 }`. Extend `WaitQueueSnapshot` with `pub waiters: Vec<WaiterOrdinal>` (default empty for back-compat; `total_waiters` unchanged so `check_waitqueue_empty` is unaffected).

- [ ] **Step 1: Write the failing test.** Add to `quiescence.rs` tests:

```rust
    #[test]
    fn waitqueue_snapshot_carries_ordinals() {
        let s = WaitQueueSnapshot {
            shard_id: 0,
            total_waiters: 2,
            waiters: vec![
                WaiterOrdinal { key: b"k".to_vec(), conn_id: 7, registration_seq: 3 },
                WaiterOrdinal { key: b"k".to_vec(), conn_id: 9, registration_seq: 5 },
            ],
        };
        assert_eq!(s.waiters.len(), 2);
        assert!(s.waiters[0].registration_seq < s.waiters[1].registration_seq);
        // Non-empty still trips the emptiness checker.
        assert!(check_waitqueue_empty(&[s]).is_err());
    }
```

- [ ] **Step 2: Run to verify failure.** Run: `just test frogdb-testing waitqueue_snapshot_carries_ordinals`. Expected: FAIL — `WaitQueueSnapshot` has no `waiters` field; `WaiterOrdinal` undefined.

- [ ] **Step 3: Implement.** In `quiescence.rs`:

```rust
/// One parked waiter's registration ordinal, from a mid-run DEBUG WAITQUEUE
/// observation. `registration_seq` is the queue-wide monotonic ordinal (smaller
/// = registered earlier), enabling exact FIFO wake-order checking.
#[derive(Debug, Clone)]
pub struct WaiterOrdinal {
    pub key: Vec<u8>,
    pub conn_id: u64,
    pub registration_seq: u64,
}
```

Add `pub waiters: Vec<WaiterOrdinal>` to `WaitQueueSnapshot`. Update the struct's two constructions in this file's tests and `parse_waitqueue` in `quiescence_probe.rs` (next task) accordingly. `check_waitqueue_empty` still keys off `total_waiters`, unchanged.

- [ ] **Step 4: Run + export + commit.** Add `WaiterOrdinal` to the `pub use quiescence::{…}` line in `lib.rs`.

Run: `just test frogdb-testing quiescence && just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/quiescence.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): carry per-waiter registration ordinals in WaitQueueSnapshot"
```

---

### Task 16: Exact FIFO checker consuming registration ordinals

**Files:**
- Modify: `frogdb-server/crates/testing/src/conservation.rs`
- Modify: `frogdb-server/crates/testing/src/fault_injection.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs`

**Interfaces:**
- Adds `pub fn check_fifo_wake_order_exact(history: &History, order: &WaiterRegistrationOrder) -> Result<(), ConservationViolation>` where `WaiterRegistrationOrder` maps `(served_key, client_id) -> registration_seq`. Where a key has ordinals for its served waiters, serve order (return-time) must match ascending `registration_seq`; otherwise the function defers to the invoke-time proxy for that key.

- [ ] **Step 1: Write the failing test.** Add to `conservation.rs` tests:

```rust
    #[test]
    fn exact_fifo_uses_registration_order_not_invoke_order() {
        use crate::conservation::WaiterRegistrationOrder;
        // Two waiters whose INVOKE order is w1 (client 1) then w2 (client 2),
        // but whose true REGISTRATION order (per DEBUG WAITQUEUE) is REVERSED.
        // Serving in registration order (w2 first) is legal exactly; the
        // invoke-proxy would wrongly flag it.
        let mut h = History::new();
        let w1 = h.invoke(1, "blpop", vec![b("k"), b("0")]);
        let w2 = h.invoke(2, "blpop", vec![b("k"), b("0")]);
        h.respond(w2, Some(b("k|b"))); // served first
        h.respond(w1, Some(b("k|a"))); // served second
        let mut order = WaiterRegistrationOrder::default();
        order.insert(b("k"), 2, 3); // client 2 registered first (seq 3)
        order.insert(b("k"), 1, 8); // client 1 registered later (seq 8)
        assert!(check_fifo_wake_order_exact(&h, &order).is_ok(),
            "serving in true registration order must be legal");

        // Now flip: serve w1 (registered later) before w2 -> violation.
        let mut h2 = History::new();
        let a = h2.invoke(1, "blpop", vec![b("k"), b("0")]);
        let bb = h2.invoke(2, "blpop", vec![b("k"), b("0")]);
        h2.respond(a, Some(b("k|a")));
        h2.respond(bb, Some(b("k|b")));
        assert!(matches!(
            check_fifo_wake_order_exact(&h2, &order),
            Err(ConservationViolation::FifoViolation { .. })
        ));
    }
```

- [ ] **Step 2: Run to verify failure.** Run: `just test frogdb-testing exact_fifo`. Expected: FAIL — undefined symbols.

- [ ] **Step 3: Implement.** In `conservation.rs`:

```rust
/// Observed true registration order: (served_key, client_id) -> registration_seq.
#[derive(Debug, Default, Clone)]
pub struct WaiterRegistrationOrder {
    map: std::collections::HashMap<(Bytes, u64), u64>,
}

impl WaiterRegistrationOrder {
    pub fn insert(&mut self, key: Bytes, client_id: u64, registration_seq: u64) {
        // Keep the smallest observed ordinal per (key, client).
        self.map
            .entry((key, client_id))
            .and_modify(|e| *e = (*e).min(registration_seq))
            .or_insert(registration_seq);
    }
    fn get(&self, key: &Bytes, client_id: u64) -> Option<u64> {
        self.map.get(&(key.clone(), client_id)).copied()
    }
}

/// Exact FIFO wake-order: for each key whose served blocking pops all have a
/// known registration ordinal, serve (return-time) order must equal ascending
/// registration_seq. Keys missing ordinals fall back to
/// [`check_fifo_wake_order`]'s invoke-time proxy.
pub fn check_fifo_wake_order_exact(
    history: &History,
    order: &WaiterRegistrationOrder,
) -> Result<(), ConservationViolation> {
    // Group served blocking pops by served key: (return_time, client_id, op_id).
    let mut by_key: HashMap<Bytes, Vec<(u64, u64, u64)>> = HashMap::new();
    for op in history.completed_operations() {
        if !matches!(op.function.as_str(), "blpop" | "brpop") {
            continue;
        }
        let Some(r) = &op.result else { continue };
        let rs = String::from_utf8_lossy(r);
        let Some((k, _)) = rs.split_once('|') else { continue };
        by_key
            .entry(Bytes::from(k.to_string()))
            .or_default()
            .push((op.return_time, op.client_id, op.id));
    }
    for (key, mut served) in by_key {
        let all_known = served.iter().all(|(_, c, _)| order.get(&key, *c).is_some());
        if !all_known {
            // Fall back to the invoke-time proxy for this key.
            continue;
        }
        served.sort_by_key(|x| x.0); // serve order
        for w in served.windows(2) {
            let seq0 = order.get(&key, w[0].1).unwrap();
            let seq1 = order.get(&key, w[1].1).unwrap();
            if seq0 > seq1 {
                return Err(ConservationViolation::FifoViolation {
                    key: key.to_vec(),
                    served: w[0].2,
                    waiter: w[1].2,
                });
            }
        }
    }
    // For keys without ordinals, the invoke-time proxy still applies.
    check_fifo_wake_order(history)
}
```

- [ ] **Step 4: Fault-injection self-test.** Add a corruption helper (or reuse `reorder_completions`) and a self-test in `fault_injection.rs` asserting a swapped serve order with a fixed registration order is caught by `check_fifo_wake_order_exact`. Run: `just test frogdb-testing exact_fifo`. Expected: PASS.

- [ ] **Step 5: Export + commit.** Add `check_fifo_wake_order_exact, WaiterRegistrationOrder` to the `pub use conservation::{…}` line.

Run: `just fmt frogdb-testing && just lint frogdb-testing`

```bash
git add frogdb-server/crates/testing/src/conservation.rs frogdb-server/crates/testing/src/fault_injection.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): exact FIFO wake-order checker consuming DEBUG WAITQUEUE ordinals"
```

---

### Task 17: Harness — CLIENT ID map, mid-run WAITQUEUE prober, multi-waiter path, wire the exact checker

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/quiescence_probe.rs` — parse per-waiter ordinals; build `WaiterRegistrationOrder`.
- Modify: `frogdb-server/crates/server/tests/common/workload_runner.rs` — `CLIENT ID` map; mid-run prober; expose `WaiterRegistrationOrder` in `CapturedRun`.
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs` — `check_all` accepts the order and calls `check_fifo_wake_order_exact`.
- Modify: `frogdb-server/crates/testing/src/workload.rs` — a multi-waiter generator path (long timeouts, multiple clients per key).

**Interfaces:**
- `parse_waitqueue` reads each shard's `keys[].waiters[]` (`conn_id`, `registration_seq`) into `WaiterOrdinal`s.
- `run_workload_capturing` returns `registration_order: WaiterRegistrationOrder` built from mid-run prober observations correlated via `client_id → conn_id`.
- `check_all` gains a `registration_order: Option<&WaiterRegistrationOrder>` param; stage 3 uses `check_fifo_wake_order_exact` when present, else `check_fifo_wake_order`.

- [ ] **Step 1: Parse per-waiter ordinals.** In `quiescence_probe.rs::parse_waitqueue`, read the nested `keys → waiters` arrays the dump emits (`key`, then each waiter's `conn_id`/`registration_seq`), populating `WaiterOrdinal`s on each `WaitQueueSnapshot`. Add a parser unit test mirroring `waitqueue_reads_total_waiters` but with a `keys`/`waiters` payload; run `cargo nextest run -p frogdb-server --features turmoil -E 'test(/waitqueue/)'`.

- [ ] **Step 2: CLIENT ID map + prober.** In `workload_runner.rs`:
  - After each client connects, issue `CLIENT ID`, parse the integer reply, and store `client_id → conn_id` in a shared `Arc<Mutex<HashMap<u64,u64>>>`. **Verify first** (read `client_conn_command.rs` and `debug_conn_command.rs`) that `CLIENT ID` returns the SAME id space as the `conn_id` in the WAITQUEUE dump; if they differ, find the dump-side id's source and surface that instead (a mismatched join silently disables the exact checker via the all-known guard — assert at least one join succeeds in the smoke test so a mismatch is loud).
  - Add a `prober` sim client that loops until `DRAIN_SETTLE_MS`, every ~50 sim-ms issuing `DEBUG WAITQUEUE`, parsing the reply via `parse_waitqueue`, and folding every `WaiterOrdinal` into a shared accumulator `Arc<Mutex<Vec<(u16 /*shard*/, WaiterOrdinal)>>>`.
  - After `sim.run()`, build a `WaiterRegistrationOrder`: for each observed `WaiterOrdinal`, look up the `client_id` for its `conn_id` (invert the CLIENT ID map) and `order.insert(key, client_id, registration_seq)`. Put it in `CapturedRun`.
  - Represent the parked key in the dump the same way the recorder stores served keys (`format_key_for_display` / raw bytes) so the `(served_key, client_id)` join matches; verify the key-display encoding round-trips (add a targeted assertion).

- [ ] **Step 3: Multi-waiter generator path.** In `workload.rs`, add a `Profile`-gated multi-waiter list/zset path (either a new `Profile::MultiWaiter` or a `BlockingHeavy` sub-path) that, on selected keys, lets **every** client issue a *long-timeout* blocking pop (drop the `blocking_owner` restriction for that path) with a delayed producer, so multiple waiters park concurrently on one key. Use a long timeout (e.g. `"5"` seconds) so the parked window comfortably exceeds the prober cadence. **Soundness constraint: each client issues AT MOST ONE blocking pop per multi-waiter key across its whole script** — `WaiterRegistrationOrder` joins on `(key, client_id)`, so a client that parks twice on the same key would inherit its first registration's ordinal and produce false FIFO violations. Enforce with a per-client `HashSet<key>` in the generator, add a generator test asserting it, and note per-op ordinal matching (time-window correlation of prober observations to a pop's `[invoke, return]` interval) as the Phase-4b lift that removes the constraint. Add a generator test asserting that on the multi-waiter path a key can receive blocking pops from >1 client (the inverse of the existing `blocking_ops_single_owner_per_key`, scoped to the new path).

- [ ] **Step 4: Wire the exact checker.** In `invariants.rs`, change `check_all`/`check_all_with` to accept `registration_order: Option<&WaiterRegistrationOrder>`; in stage 3, call `check_fifo_wake_order_exact(history, order)` when `Some`, else `check_fifo_wake_order(history)`. Update `concurrency_workload.rs::run_and_check` to pass `Some(&run.registration_order)`. Update the existing `check_all(...)` call sites and unit tests to pass `None`.

- [ ] **Step 5: Smoke test + regression gate.** Add a `--features turmoil` test running a small multi-waiter workload and asserting `check_all` passes (a correct server serves in registration order). Run:

```bash
cargo nextest run -p frogdb-server --features turmoil -E 'test(/multi_waiter|waitqueue/)' > /tmp/mw.log 2>&1
```

Verify liveness; expected PASS. If a served-order violation appears, triage harness-vs-server per the bug workflow (a genuine FIFO-fairness bug earns a pinned regression).

- [ ] **Step 6: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/quiescence_probe.rs frogdb-server/crates/server/tests/common/workload_runner.rs frogdb-server/crates/server/tests/common/invariants.rs frogdb-server/crates/testing/src/workload.rs
git commit -m "test(sim): mid-run WAITQUEUE prober + CLIENT ID map for exact multi-waiter FIFO checking"
```

---

## Component 5 — Connections-responsive quiescence (remaining bundled item)

### Task 18: Post-workload PING sweep over all sim clients

After the workload drains, every sim client connection must still be responsive (a leaked/wedged connection or a shard that stopped servicing a connection is a bug even if the wait queue looks empty). Add a PING sweep to the drainer and surface an unresponsive connection as a quiescence violation.

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/workload_runner.rs` — PING sweep in the drainer, captured into `CapturedRun`.
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs` — a connections-responsive violation in the quiescence stage.

**Interfaces:**
- `CapturedRun` gains `pub connections_responsive: bool` (all client connections replied `+PONG` post-drain). `check_all` treats `false` as a `quiescence:` violation.

- [ ] **Step 1: Write the failing test.** Add to `invariants.rs` tests a case where `connections_responsive = false` yields a violation. Since `check_all` gathers responsiveness from `CapturedRun`, thread it as a parameter or a field on `QuiescenceSnapshots`. Simplest: add `pub connections_responsive: bool` (default `true`) to `QuiescenceSnapshots`, and in `check_quiescence` push `"quiescence: N sim client connection(s) unresponsive post-drain"` when `false`:

```rust
    #[test]
    fn unresponsive_connection_is_a_quiescence_violation() {
        let h = History::new();
        let snap = QuiescenceSnapshots { connections_responsive: false, ..Default::default() };
        let report = check_all(&h, &Default::default(), Some(&snap), None);
        assert!(report.quiescence_checked);
        assert!(report.violations.iter().any(|v| v.contains("unresponsive")),
            "expected an unresponsive-connection violation, got {:?}", report.violations);
    }
```

(Adjust the `check_all` arity to the `registration_order` param added in Task 17.)

- [ ] **Step 2: Run to verify failure.** Run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/unresponsive_connection/)'`. Expected: FAIL — no `connections_responsive` field / no violation emitted.

- [ ] **Step 3: Implement.**
  - Add `pub connections_responsive: bool` to `QuiescenceSnapshots` (`quiescence_probe.rs`), defaulting `true`, and in `check_quiescence` push the violation string when `false`.
  - In `workload_runner.rs`, have each client, as its final scripted action (after its ops), send `PING` and record whether it got `+PONG`; fold the AND across clients into a shared `Arc<Mutex<bool>>`, and set `QuiescenceSnapshots::connections_responsive` before returning `CapturedRun`. (Each client already owns its `stream`; a trailing `PING` is a one-line addition to the per-client async block.)

- [ ] **Step 4: Run + regression gate.** Run: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/unresponsive_connection|quiescence/)'`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/workload_runner.rs frogdb-server/crates/server/tests/common/quiescence_probe.rs frogdb-server/crates/server/tests/common/invariants.rs
git commit -m "test(sim): connections-responsive quiescence via post-workload PING sweep"
```

---

## Task 19: Final verification + triage protocol

Run the full concurrency tier and per-crate suites, then the sweeps at N=20 seeds, and triage any failure per the phase-3 bug workflow.

**Files:** none (verification), unless a failure requires a pinned regression (then `concurrency_workload.rs`).

- [ ] **Step 1: Fast per-crate suites (foreground).**

```bash
just check frogdb-testing && just test frogdb-testing && just lint frogdb-testing && just fmt frogdb-testing
just test frogdb-core expiry_index_audit
just check frogdb-server
```

Expected: all green. The `frogdb-testing` suite now includes the new StreamGroup model, PEL checker + self-tests, exact-FIFO checker, script pseudo-ops, binary-safe serde, and the errored-EXEC handling.

- [ ] **Step 2: Full concurrency tier (watchdog protocol; commit first).**

```bash
just concurrency > /tmp/concurrency.log 2>&1
```

Verify liveness every few minutes (`wc -c /tmp/concurrency.log` growing, or `ps -Ao pid,pcpu,etime,command` shows the nextest processes accumulating CPU). Expected: PASS — shuttle concurrency, the `simulation` tests, `seed_sweep_short_workloads`, and `seed_sweep_txheavy` all green.

- [ ] **Step 3: The generated sweeps at N=20 (each profile).**

```bash
cargo nextest run -p frogdb-server --features turmoil \
  -E 'test(/seed_sweep_short_workloads|seed_sweep_txheavy/)' > /tmp/sweeps.log 2>&1
```

The short-workload sweep already exercises `Mixed`/`BlockingHeavy` (now including StreamGroup, Script, and multi-waiter FIFO vocabulary); TxHeavy runs its 20 seeds. Verify liveness; expected PASS.

- [ ] **Step 4: Triage protocol for any failure.** Per `docs/superpowers/plans/concurrency-phase3-bug-workflow.md`:
  1. The sweep auto-writes `target/concurrency-repros/<seed>.json`; reproduce with `just concurrency-repro <file>`.
  2. **Triage harness/model gap vs. server bug:**
     - Harness/model gap (a reply the recorder mis-encodes, a model that mis-steps a new op, a checker false-positive) → fix the harness/model. **Never weaken a checker to make the sweep pass.**
     - Genuine server bug → follow `superpowers:systematic-debugging`, shrink the repro, land the server fix with a **named pinned regression** in the `regressions` module of `concurrency_workload.rs` (hardcoded seed/profile/config; fails before, passes after). Reference the repro JSON in the commit message; do not commit `target/`.
  3. Candidate day-1 finds for the new vocabulary: PEL double-delivery under concurrent XREADGROUP/XCLAIM; a script's composite op observed non-atomically (interleaved sub-effects); a genuine multi-waiter FIFO-fairness violation now that the exact checker can see registration order.

- [ ] **Step 5: Final commit (only if triage produced fixes/regressions).**

```bash
git add -A
git commit -m "test(sim): phase-4a workload vocabulary expansion — final verification + pinned regressions"
```

---

## Self-check (every Phase-4a spec item has a home)

| Spec 4a item | Task(s) |
|---|---|
| Un-ignore TxHeavy: KVModel accepts aborted/errored EXEC | 4 |
| Conservation checkers accept aborted/errored EXEC | 5 |
| Generator biases toward same-slot transaction groups + minority cross-slot | 6 |
| CROSSSLOT rejection pinned (response legality) | 7 |
| Research: standalone same-slot cross-internal-shard VLL-EXEC reachability | 3 |
| Un-ignore `seed_sweep_txheavy` + wire into `just concurrency` | 8 |
| Consumer groups: strict `StreamGroupModel` (PEL) | 9 |
| PEL conservation checker + fault-injection self-tests | 10 |
| Generator `Family::StreamGroup` vocabulary | 11 |
| Harness routing (family/model/keys/canonicalization) for groups | 12 |
| Scripting: fixed pool, model-steppable via KVModel/ListModel | 13 |
| Generator `Family::Script` + EVAL/EVALSHA + NOSCRIPT fallback | 14 |
| Exact multi-waiter FIFO via DEBUG WAITQUEUE ordinals | 15, 16, 17 |
| Connections-responsive quiescence (PING sweep) | 18 |
| `audit_expiry_index` KeyMissing + DeadlineMismatch tests | 2 |
| Binary-safe history serde | 1 |
| Final verification + triage | 19 |

## Honest scoping notes

- **Discrepancy #2 is load-bearing:** the sim server runs with `allow_cross_slot_standalone: true`, so the TxHeavy root-cause comment ("correctly rejects with -CROSSSLOT") may be stale. Task 3 re-verifies the actual outcome empirically before Tasks 4/6/7 commit to it, and Task 7's response-legality assertion is written to branch on that outcome. The errored-EXEC-encoding fix (Task 4/5) is correct regardless of which outcome holds — an `"ERR:…"` EXEC result must be a no-op wherever it occurs.
- **Same slot ⟹ same internal shard** under `shard_for_key = hash_slot * num_shards / HASH_SLOTS`, so a *standalone same-slot cross-internal-shard* EXEC is structurally unreachable from the generator; the VLL-EXEC path's remaining coverage (if any) is a Phase-4b shard-driver-harness item (spec scenario 3). Task 3 records the definitive finding.
- **Consumer-group vocabulary is deliberately bounded** (min-idle-time always 0, XPENDING summary form only, XCLAIM no-JUSTID / XAUTOCLAIM JUSTID, no NOACK, no blocking XREADGROUP, no XDEL) so the sequential semantics are deterministic under turmoil. Live-id XACK/XCLAIM against server-assigned `*` ids, extended XPENDING, and deleted-entry-in-PEL edge cases are noted follow-ups for Phase-4b.
- **PEL double-ownership detection is partial:** summary-form XPENDING exposes per-consumer counts but not per-id ownership, so the conservation checker can only catch a double-own that inflates the summed counts past the total; the `StreamGroupModel` WGL check is the primary guard (it pins every XPENDING summary against sequential PEL state). Extended-form XPENDING (per-id consumer) is the Phase-4b tightening. Delivery-count monotonicity is likewise unobservable in 4a and deferred.
- **Exact multi-waiter FIFO depends on mid-run polling**, which is inherently best-effort: the checker uses registration ordinals *authoritatively where captured* and falls back to the invoke-time proxy otherwise, so it can only *remove* false positives and *add* true-positive detection — never regress soundness. Long parked windows (multi-waiter path uses multi-second timeouts) make capture reliable.
- **Script atomicity is checked only at the composite-op granularity:** a pool script records as one linearization-checked pseudo-op with its return value; the model does not observe intra-script sub-effect ordering (that is the point of atomic scripts). FUNCTION and cross-shard scripts are out of scope.
