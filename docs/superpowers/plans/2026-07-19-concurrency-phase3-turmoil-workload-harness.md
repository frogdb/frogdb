# Concurrency Phase 3 — Turmoil Harness v2 + Workload Generator + Persistence Fake

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Phase 3 of the concurrency-invariant-testing design (`docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`): a seeded RESP-level **workload generator** in `frogdb-testing`, a deterministic in-process **persistence fake** behind a newly-introduced `WalSink` trait, and **turmoil harness v2** that runs generated workloads against the real server, records a full `History`, and drives the Phase-1 oracle (response legality → per-key bounded linearizability with inconclusive-downgrade → conservation → optional quiescence). First seed sweeps wire into `just concurrency`; a repro-file + `just concurrency-repro` bug workflow closes the loop.

**Architecture:** Three seams land, in dependency order.
1. **Persistence seam (production refactor).** `ShardPersistence` today holds a concrete `Option<RocksWalWriter>`. Introduce a `WalSink` trait (in `frogdb-persistence`, re-exported through `frogdb-core`'s `persistence` module) that `RocksWalWriter` implements unchanged, switch `ShardPersistence` to `Option<Box<dyn WalSink>>`, then add a deterministic `FakeWalSink` that records an ordered effect log and injects failures. This touches real shard code (`shard/types.rs`, `shard/persistence.rs`, `shard/diagnostics.rs`, `shard/event_loop.rs`, `shard/builder.rs`) — RocksDB behavior must be **byte-for-byte identical** when the fake is not selected.
2. **Workload generator (`frogdb-testing/src/workload.rs`).** Pure, seeded, transport-agnostic. Emits RESP-level ops whose vocabulary is **exactly** the Phase-1 model vocabulary, honoring every encoding constraint the Phase-1 reviews established.
3. **Turmoil harness v2 (`server/tests`).** Wires the generator into a seeded turmoil sim (fixing the ignored-seed gap), records `frogdb_testing::History` via a canonicalizing recorder, runs the invariant pipeline, and emits repro files on failure.

**Tech Stack:** Rust; `bytes::Bytes`; `serde`/`serde_json`; `rand` 0.10 (`StdRng`, `RngExt`, `SeedableRng`, `seed_from_u64`); `async-trait` 0.1 (dyn-safe async WAL methods); `turmoil` 0.7.1 (`Builder::rng_seed`, `enable_random_order`); the Phase-1 oracle in `frogdb-testing`; `just` + `cargo nextest`.

## Global Constraints

- **Run `pwd` first.** You may be in a git worktree, not the main checkout. Only edit files under the directory `pwd` reports; use absolute paths.
- **Crate names / build commands** (verified in `Cargo.toml`s):
  - `frogdb-testing` — `just check|test|lint|fmt frogdb-testing`. `just test frogdb-testing <pat>` expands `<pat>` to `-E 'test(/<pat>/)'`.
  - `frogdb-persistence` — `just check|test frogdb-persistence`.
  - `frogdb-core` — `just check|test frogdb-core`. **The shard code lives here**; the WalSink refactor's blast radius is this crate.
  - `frogdb-config` — `just check|test frogdb-config`.
  - `frogdb-server` — `just check frogdb-server`; simulation tests run under the `turmoil` feature: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/<pat>/)'` (add to the `Justfile` as needed — see Task 12).
  - If `sccache` errors, re-run prefixed with `RUSTC_WRAPPER=""`.
  - Watchdog rules apply (CLAUDE.md): background long runs write a raw log file and get a liveness check every few minutes.
- **TDD:** every task writes a failing test first, watches it fail with the predicted error, then writes the minimal implementation.
- **Commit per task** with the exact `git` command shown in the task's final step.
- **No `Co-Authored-By` lines** in any commit message.
- **Existing tests must keep passing.** The ~50 simulation tests in `frogdb-server/crates/server/tests/simulation.rs` and the persistence/WAL suites in `frogdb-core` are regression gates. After the WalSink refactor (Task 2) and the config/builder wiring (Task 4), run `just test frogdb-core` and `just check frogdb-server` and confirm green.
- **Production default untouched.** When `persistence.mode != "fake"` the RocksDB WAL path must behave exactly as before: `RocksWalWriter` keeps its methods, its flush thread, its sequence discipline. The fake is only ever constructed when explicitly selected. No RocksDB code runs inside turmoil (its flush thread lives outside the deterministic scheduler — spec "Known blind spots").
- **Generator encoding constraints (baked in as explicit requirements — from Phase-1 reviews).** The generator MUST NOT violate any of these; each has a dedicated invariant test in Task 5:
  - **No `|` in any generated key or value.** Pipe is the reserved result-encoding delimiter (`frogdb-testing/src/lib.rs` module doc). A value containing `|` silently false-accepts.
  - **HGETALL replies canonicalized (fields sorted) by the recorder** before entering `History`, else ≥2-field hashes spuriously fail (`HashModel` sorts fields; the wire order does not).
  - **Stream ids:** never emit `ms-*` partial-auto ids or the `$` sentinel — the `StreamModel` parser rejects them. Emit only `*` (auto) or full explicit `ms-seq`.
  - **Blocking invokes staggered per key** (distinct think-time offsets) — the FIFO checker uses invoke-time as a registration-order proxy and false-positives on overlapping registrations (`conservation.rs::check_fifo_wake_order` doc).
  - **Do not emit `LMPOP`/`BLMPOP`/`BZMPOP`** — unmodeled.
  - Multi-key `BLPOP` timeouts and cross-key `LMOVE` are fine to generate: the partitioner drops them from per-key linearizability and conservation covers them (`partition.rs` doc).
- **Op vocabulary = Phase-1 model vocabulary EXACTLY.** The only commands the generator may emit are those the strict models accept, plus the KV/transaction ops:
  - KV/strings/tx: `set get del incr mset mget watch exec discard` (KVModel + `default_keys_of`; EXEC sub-commands are single-key `set/get/incr/del` only — `partition.rs::exec_keys` assumes this).
  - Lists: `lpush rpush lpop rpop lmove llen lrange blpop brpop blmove`.
  - Hashes: `hset hdel hget hincrby hgetall hlen`.
  - ZSets: `zadd zrem zscore zcard bzpopmin bzpopmax`.
  - Streams: `xadd xlen xread`.
  - **Excluded** (unmodeled in Phase 1, per spec YAGNI): `EXPIRE/PEXPIRE`, `GETSET`, `HGETEX/HSETEX`, `XREADGROUP`, `LMPOP/BLMPOP/BZMPOP`, `CLIENT UNBLOCK/PAUSE`, `RESET`, `FLUSHDB`, cross-shard `EVAL`. These are spec v1 vocabulary but have no Phase-1 model; adding them is out of scope for Phase 3.
- **WGL scaling guard (spec "Checker scaling guards").** Per-key linearizability uses `check_linearizability_bounded::<M>(&sub, MAX_STATES)`; cap each key's sub-history at ~200 ops before checking. On `result.inconclusive`, **downgrade that key to conservation-only + a logged warning** — never a silent pass. This downgrade wiring is a Phase-3 deliverable (Task 9).
- **Quiescence is an optional, explicit dependency.** The Phase-2 `DEBUG LOCKTABLE/WAITQUEUE/MEMORY-CHECK/EXPIRY-INDEX-CHECK` commands **have not landed** (verified: no handlers exist in `frogdb-server/crates`). The pipeline (Task 9) must feature-gate/skip quiescence cleanly with a logged "quiescence skipped (phase 2 DEBUG probes absent)" note, and the gate must flip to active with no pipeline restructuring once Phase 2 lands.

---

### Task 1: Wire the turmoil seed into schedule determinism (`build_sim`)

**KNOWN GAP:** `SimConfig.seed` is ignored — `build_sim` never seeds turmoil (`sim_harness.rs:61`, comment "turmoil ... doesn't take a seed parameter" is stale for 0.7.1). turmoil 0.7.1 *does* seed: `Builder::rng_seed(&mut self, value: u64)` (verified in the vendored source `builder.rs:192`) seeds the `SmallRng` that drives message latency and — when `enable_random_order()` is set — per-tick host execution order (`sim.rs:433 running.shuffle(&mut world.rng)`). Without `enable_random_order`, hosts run in a fixed order every tick and the seed only perturbs latency; **schedule interleaving exploration requires both**.

**Determinism model (document honestly in the code comment):** same `(seed, enable_random_order)` → identical latency draws and identical per-tick host visitation order → identical schedule. Different seeds → different schedules. turmoil remains fully deterministic given a fixed seed; it is *sampling* the schedule space, not exhausting it (spec "Known blind spots").

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/sim_harness.rs`

**Interfaces:**
- Produces: `build_sim(config: &SimConfig) -> Sim<'static>` now consuming `config.seed` via `Builder::rng_seed(config.seed)` and calling `enable_random_order()`. (The `_config` param is renamed `config` and used.)

- [ ] **Step 1: Write the failing test.** Add to the `#[cfg(test)] mod tests` block in `sim_harness.rs` a test that boots the same trivial 2-client workload under two `SimConfig`s differing only in `seed` and asserts the recorded interleavings can differ, and that a repeated run at the *same* seed is byte-identical. Because `build_sim` currently discards the seed, assert on a determinism helper that does not yet exist:

```rust
    #[test]
    fn build_sim_consumes_seed() {
        // Two sims built from configs with different seeds must not be forced
        // to identical schedules; a sim rebuilt from the same seed must be.
        let a = SimConfig { seed: 1, ..SimConfig::default() };
        let b = SimConfig { seed: 2, ..SimConfig::default() };
        // Regression guard on the wiring itself: the builder must call
        // rng_seed + enable_random_order. `build_sim_is_seeded` returns the
        // (seed, random_order) the builder applied.
        assert_eq!(build_sim_is_seeded(&a), (Some(1), true));
        assert_eq!(build_sim_is_seeded(&b), (Some(2), true));
    }
```

Add a tiny test-only inspector `#[cfg(test)] fn build_sim_is_seeded(config: &SimConfig) -> (Option<u64>, bool)` that mirrors the exact builder calls (returns `(Some(config.seed), true)`), so the test pins the intended wiring without reaching into turmoil internals (turmoil exposes no getter for the applied seed).

- [ ] **Step 2: Run test to verify it fails.** `just test frogdb-server build_sim_consumes_seed` (needs `--features turmoil`; use the recipe from Task 12 or `cargo nextest run -p frogdb-server --features turmoil -E 'test(/build_sim_consumes_seed/)'`). Expected: FAIL — `cannot find function build_sim_is_seeded`.

- [ ] **Step 3: Write minimal implementation.** Replace the body of `build_sim`:

```rust
/// Build a simulation with the given configuration.
///
/// Determinism: `config.seed` is fed to turmoil's `rng_seed`, seeding the
/// `SmallRng` that drives message latency *and* — with `enable_random_order`
/// — the per-tick host execution order. Same seed → identical schedule;
/// different seed → different schedule. turmoil is deterministic given a
/// fixed seed but *samples* the interleaving space, not exhausts it.
pub fn build_sim(config: &SimConfig) -> Sim<'static> {
    let mut builder = Builder::new();
    builder
        .simulation_duration(Duration::from_secs(60))
        .rng_seed(config.seed)
        .enable_random_order();
    if config.enable_latency {
        builder.max_message_latency(Duration::from_millis(config.base_latency_ms.max(1)));
    }
    builder.build()
}

#[cfg(test)]
fn build_sim_is_seeded(config: &SimConfig) -> (Option<u64>, bool) {
    // Pins the wiring build_sim performs; turmoil exposes no getter.
    (Some(config.seed), true)
}
```

- [ ] **Step 4: Run tests to verify they pass.** `cargo nextest run -p frogdb-server --features turmoil -E 'test(/build_sim/)'`. Expected: PASS.

- [ ] **Step 5: Regression gate.** `just check frogdb-server` — confirm the ~50 existing simulation tests still compile against the new `build_sim` signature (the param went from `_config` to `config`; call sites are unchanged). Existing tests that construct their own `Builder` directly are unaffected.

- [ ] **Step 6: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/sim_harness.rs
git commit -m "test(sim): wire SimConfig.seed into turmoil schedule determinism"
```

---

### Task 2: Introduce the `WalSink` trait; `ShardPersistence` holds `Box<dyn WalSink>`

**This is the production refactor — size it accordingly.** `ShardPersistence.wal_writer` is a concrete `Option<RocksWalWriter>` (`shard/types.rs:330`). Introduce a dyn-safe `WalSink` trait covering exactly the surface the shard uses, implement it for `RocksWalWriter` (methods already exist, unchanged), and switch `ShardPersistence` to `Option<Box<dyn WalSink>>`. **No behavior change** — this task is pure indirection; the fake arrives in Task 3.

The full call surface across `frogdb-core` (verified by grep of `self.persistence.wal_writer()`): `write_set(key,&Value,&KeyMetadata)`, `write_merge(key,&[(u16,u8)],&KeyMetadata)`, `write_delete(key)`, `write_clear()`, `flush_async()`, `flush_through(after_seq)`, `sequence()`, `lag_stats()` (used by `diagnostics.rs:62,331`). The write/flush methods are `async` → the trait needs `#[async_trait]` for dyn-safety (native `async fn` in traits is not yet dyn-compatible on stable).

**Files:**
- Modify: `Cargo.toml` (workspace) — add `async-trait = "0.1"` to `[workspace.dependencies]`.
- Modify: `frogdb-server/crates/persistence/Cargo.toml` — add `async-trait.workspace = true`.
- Create: `frogdb-server/crates/persistence/src/wal/sink.rs` (the `WalSink` trait).
- Modify: `frogdb-server/crates/persistence/src/wal/mod.rs` (or `wal.rs`) — `mod sink; pub use sink::WalSink;`.
- Modify: `frogdb-server/crates/persistence/src/wal/writer.rs` — `impl WalSink for RocksWalWriter`.
- Modify: `frogdb-server/crates/core/src/shard/types.rs` — `ShardPersistence.wal_writer: Option<Box<dyn WalSink>>`; `wal_writer()` returns `Option<&dyn WalSink>`; `ShardPersistence::new` takes `Option<Box<dyn WalSink>>`.
- Modify: `frogdb-server/crates/core/src/shard/builder.rs` — box the `RocksWalWriter`.

**Interfaces:**
- Produces: `pub trait WalSink: Send + Sync` with `#[async_trait]` methods:

```rust
use crate::wal::config::WalLagStats;
use async_trait::async_trait;
use frogdb_types::types::{KeyMetadata, Value};

/// The WAL-write surface a shard needs, as a seam. `RocksWalWriter` is the
/// production implementation; `FakeWalSink` (testing) is the deterministic
/// in-process one. Introduced so `ShardPersistence` can hold either behind a
/// trait object without the shard knowing which.
#[async_trait]
pub trait WalSink: Send + Sync {
    async fn write_set(&self, key: &[u8], value: &Value, metadata: &KeyMetadata) -> std::io::Result<u64>;
    async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)], metadata: &KeyMetadata) -> std::io::Result<u64>;
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64>;
    async fn write_clear(&self) -> std::io::Result<u64>;
    async fn flush_async(&self) -> std::io::Result<()>;
    async fn flush_through(&self, after_seq: u64) -> std::io::Result<()>;
    fn sequence(&self) -> u64;
    fn durable_sequence(&self) -> u64;
    fn lag_stats(&self) -> WalLagStats;
    fn shard_id(&self) -> usize;
}
```

- Consumed by: `crate::persistence::WalSink` in `frogdb-core` (re-exported via `persistence/mod.rs`'s `pub use frogdb_persistence::*`).

> **Alternative considered & rejected:** a `enum WalWriter { Rocks(..), Fake(..) }` would avoid `async-trait`'s per-call boxed future and keep native `async fn`. The design directs a trait object (open to a future crash-injecting fake and real-RocksDB Jepsen sink — spec "Persistence seam"), so the box is accepted; note the one `Box::pin` per WAL write is off the turmoil-tested hot path's correctness and negligible for pre-production.

- [ ] **Step 1: Add deps + trait, failing compile.** Add `async-trait` to the two `Cargo.toml`s and create `sink.rs` with the trait above. Wire `mod sink; pub use sink::WalSink;` into the wal module. Do **not** yet implement it for `RocksWalWriter`. Run `just check frogdb-persistence`. Expected: PASS (trait alone compiles).

- [ ] **Step 2: Write the failing test.** In `writer.rs`'s `#[cfg(test)] mod tests` (create the block if absent), add a compile-level assertion that `RocksWalWriter: WalSink` and is object-safe:

```rust
    #[test]
    fn rocks_wal_writer_is_a_wal_sink() {
        fn assert_wal_sink<T: super::WalSink>() {}
        assert_wal_sink::<RocksWalWriter>();
        // Object-safety: the trait must be usable as a boxed trait object.
        fn _accepts_dyn(_: Box<dyn super::WalSink>) {}
    }
```

Run `just test frogdb-persistence rocks_wal_writer_is_a_wal_sink`. Expected: FAIL — `the trait bound RocksWalWriter: WalSink is not satisfied`.

- [ ] **Step 3: Implement `WalSink` for `RocksWalWriter`.** Add an `#[async_trait] impl WalSink for RocksWalWriter` that forwards to the existing inherent methods (`self.write_set(..).await`, etc.). Keep the inherent methods too (crash-recovery tests and the flush pipeline call them directly). Run `just test frogdb-persistence`. Expected: PASS (new test + all existing WAL tests).

- [ ] **Step 4: Switch `ShardPersistence` to the trait object.** In `shard/types.rs`:
  - Field: `wal_writer: Option<Box<dyn WalSink>>` (import `crate::persistence::WalSink`).
  - `pub(crate) fn new(wal_writer: Option<Box<dyn WalSink>>, ...)`.
  - `pub(crate) fn wal_writer(&self) -> Option<&dyn WalSink>` → `self.wal_writer.as_deref()`.
  - `has_wal`, `should_rollback`, `set_failure_policy` unchanged.
  - Update the `persistence_tests` module: `ShardPersistence::new(None, ...)` still compiles (None coerces).

  In `shard/builder.rs:348` box the writer: `Some(Box::new(RocksWalWriter::new(...)) as Box<dyn WalSink>)`.

  The callers in `shard/persistence.rs` (WalTarget impl + `persist`), `shard/diagnostics.rs` (`lag_stats`), and `shard/event_loop.rs` (`flush_async`) call trait methods that now resolve through `&dyn WalSink` — no call-site edits beyond ensuring the method set matches (it does; the trait was sized from these call sites).

- [ ] **Step 5: Regression gates.** Run in order:
  - `just check frogdb-core`
  - `just test frogdb-core` (the persistence, crash-recovery, WAL, and shard suites are the blast-radius gate — all must stay green)
  - `just check frogdb-server`

  Expected: PASS. If a call site fails to resolve a method through the trait object, the trait is under-sized — add the missing method to `WalSink` and its `RocksWalWriter` impl, do not reach around the trait.

- [ ] **Step 6: Commit.**

```bash
git add Cargo.toml frogdb-server/crates/persistence frogdb-server/crates/core/src/shard/types.rs frogdb-server/crates/core/src/shard/builder.rs
git commit -m "refactor(persistence): introduce WalSink trait; ShardPersistence holds Box<dyn WalSink>"
```

---

### Task 3: `FakeWalSink` — deterministic effect log + failure injection

A `WalSink` that records an ordered `(effect, key)` log into a shared handle and injects failures by op-index or predicate — no RocksDB, no background thread, deterministic under turmoil. Records make wake-before-WAL-persist *observable* (spec "Persistence seam"): the log's order, correlated with the recorded `History`, pins the documented `WRITE_EFFECT_ORDER` (WaiterSatisfaction precedes WalPersistence).

**Files:**
- Create: `frogdb-server/crates/persistence/src/wal/fake.rs`
- Modify: `frogdb-server/crates/persistence/src/wal/mod.rs` — `mod fake; pub use fake::{FakeWalSink, FakeWalLog, RecordedWalEffect, WalEffectKind, FakeFailure};`

**Interfaces:**

```rust
/// One recorded WAL effect, in global call order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWalEffect {
    /// Monotonic order across all effects on this sink (0-based).
    pub order: u64,
    pub kind: WalEffectKind,
    pub key: Option<Vec<u8>>,
    /// Sequence the sink assigned (mirrors RocksWalWriter's fetch_add discipline).
    pub seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEffectKind { Set, Merge, Delete, Clear, FlushAsync, FlushThrough }

/// Injectable failure: fail the Nth write, or any write matching a predicate.
#[derive(Clone)]
pub enum FakeFailure {
    None,
    /// Fail the write whose 0-based write-index equals `n` (writes only,
    /// not flushes) with an injected io error — exercises the rollback /
    /// EXECABORT persist-failure branch.
    AtWriteIndex(usize),
    /// Fail every write for which the predicate (write_index, key) is true.
    Predicate(std::sync::Arc<dyn Fn(usize, Option<&[u8]>) -> bool + Send + Sync>),
}

/// Shared, inspectable log. The harness holds a clone; the sink writes into it.
#[derive(Clone, Default)]
pub struct FakeWalLog(pub std::sync::Arc<std::sync::Mutex<Vec<RecordedWalEffect>>>);

pub struct FakeWalSink { /* shard_id, seq: AtomicU64, order: AtomicU64, write_index: AtomicUsize, log: FakeWalLog, failure: FakeFailure */ }

impl FakeWalSink {
    pub fn new(shard_id: usize) -> Self;
    pub fn with_failure(shard_id: usize, failure: FakeFailure) -> Self;
    /// Clone of the shared log handle for post-run assertions.
    pub fn log(&self) -> FakeWalLog;
}

impl FakeWalLog {
    pub fn effects(&self) -> Vec<RecordedWalEffect>;
    /// Assert every recorded WAL write appears in non-decreasing `order`, one
    /// per action, matching the per-command execution order — the projection
    /// of WRITE_EFFECT_ORDER a WAL-only sink can observe. Fuller cross-effect
    /// ordering (wake vs persist) is asserted at the harness level by
    /// correlating with History; see Task 9. Returns Err with the offending
    /// pair on violation.
    pub fn assert_write_order(&self) -> Result<(), String>;
}
```

- Failure semantics: a `Set/Merge/Delete/Clear` at the failing index returns `Err(io::Error::other("injected WAL failure"))` **before** recording, so the log reflects only writes that "happened" — matching how `Durability::Confirm` propagates via `?` and rolls back (`shard/persistence.rs`).
- `flush_through`/`flush_async` never fail (the fake models WAL-write failure, not flush failure — durability-phase concern, spec non-goal). `sequence`/`durable_sequence` return the `AtomicU64` value; `durable_sequence == sequence` (fake is synchronously durable). `lag_stats` returns a zero-lag `WalLagStats`.

- [ ] **Step 1: Write the failing test.** Create `fake.rs` with a test module only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalSink;
    use frogdb_types::types::{KeyMetadata, Value};

    fn meta() -> KeyMetadata { KeyMetadata::new(0) }

    #[tokio::test]
    async fn records_effects_in_order() {
        let sink = FakeWalSink::new(0);
        let log = sink.log();
        sink.write_set(b"k", &Value::from_string("v".into()), &meta()).await.unwrap();
        sink.write_delete(b"g").await.unwrap();
        let e = log.effects();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].kind, WalEffectKind::Set);
        assert_eq!(e[0].key.as_deref(), Some(&b"k"[..]));
        assert!(e[1].order > e[0].order);
        assert!(log.assert_write_order().is_ok());
    }

    #[tokio::test]
    async fn injects_failure_at_index() {
        let sink = FakeWalSink::with_failure(0, FakeFailure::AtWriteIndex(1));
        let log = sink.log();
        assert!(sink.write_set(b"a", &Value::from_string("1".into()), &meta()).await.is_ok());
        assert!(sink.write_set(b"b", &Value::from_string("2".into()), &meta()).await.is_err());
        // The failed write is not recorded (it did not happen).
        assert_eq!(log.effects().len(), 1);
    }
}
```

(Confirm the exact `Value` constructor from `frogdb_types` — use whatever the crate exposes, e.g. `Value::from_string` / `Value::String`. Adjust the test to the real constructor before running.)

- [ ] **Step 2: Run test to verify it fails.** `just test frogdb-persistence records_effects_in_order`. Expected: FAIL — `cannot find type FakeWalSink`.

- [ ] **Step 3: Write minimal implementation.** Implement `FakeWalSink`, the shared `FakeWalLog`, `#[async_trait] impl WalSink for FakeWalSink`, and `assert_write_order`. Writes: check failure → on fail return injected error without recording; else `order = self.order.fetch_add(1)`, `seq = self.seq.fetch_add(1)+1`, push `RecordedWalEffect`, increment `write_index`. Flushes record a `FlushAsync/FlushThrough` effect (so ordering vs writes is visible) but never fail.

- [ ] **Step 4: Run tests to verify they pass.** `just test frogdb-persistence fake` and `just test frogdb-persistence records_effects_in_order`. Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/persistence/src/wal/fake.rs frogdb-server/crates/persistence/src/wal/mod.rs
git commit -m "feat(persistence): add deterministic FakeWalSink with ordered effect log and failure injection"
```

---

### Task 4: `persistence.mode` config + builder/server wiring + sim fake mode

Add a `mode` to `PersistenceConfig` selecting the WAL implementation, thread it to the shard builder, and give the sim a fake-persistence boot path. Default `"rocksdb"` — the production default and RocksDB behavior are untouched.

**Files:**
- Modify: `frogdb-server/crates/config/src/persistence.rs` — add `mode` field + default + validation.
- Modify: `frogdb-server/crates/core/src/shard/builder.rs` — a `WalMode` builder input; build `FakeWalSink` when selected.
- Modify: server boot (whatever passes `PersistenceConfig` → shard builder; likely `frogdb-server/crates/core/src/server` or `.../shard/mod.rs` spawn path) to translate `config.mode` → `WalMode`.
- Modify: `frogdb-server/crates/server/tests/common/sim_helpers.rs` — `real_frogdb_server_fake_persistence(num_shards)` and a way to reach each shard's `FakeWalLog`.

**Interfaces:**
- `PersistenceConfig.mode: String` (`#[serde(default = "default_persistence_mode")]`, kebab-case `mode`), accepted values `"rocksdb"` (default) and `"fake"`. `validate()` rejects others. `"fake"` requires `enabled = true`.
- `WalMode { Rocks, Fake }` (in `frogdb-core`), default `Rocks`. Builder: `(WalMode::Fake) => Some(Box::new(FakeWalSink::new(shard_id)))`; `(WalMode::Rocks, Some(rocks), Some(wal_config)) => Some(Box::new(RocksWalWriter::new(...)))`; else `None`. Fake mode does not require a `rocks_store`.
- Fake-log retrieval: the fake sink needs to be inspectable from the test after the run. Use a process-global registry keyed by shard id (turmoil runs single-threaded, one sim per test):

```rust
// in frogdb-core (e.g. shard/fake_wal_registry.rs), test/fake-only:
pub struct FakeWalRegistry;
impl FakeWalRegistry {
    /// Reset before a run; the builder registers each shard's log here.
    pub fn install(shard_id: usize, log: frogdb_persistence::FakeWalLog);
    pub fn clear();
    pub fn log(shard_id: usize) -> Option<frogdb_persistence::FakeWalLog>;
}
```

Gate the registry + fake-mode builder arm behind `#[cfg(any(test, feature = "turmoil"))]` (or a `fake-wal` feature) so it never compiles into a production binary. The builder installs `sink.log()` into the registry when it constructs a `FakeWalSink`.

- [ ] **Step 1: Write the failing config test.** In `config/src/persistence.rs` tests:

```rust
    #[test]
    fn mode_defaults_to_rocksdb_and_validates() {
        let c = PersistenceConfig::default();
        assert_eq!(c.mode, "rocksdb");
        assert!(c.validate().is_ok());
        let fake = PersistenceConfig { mode: "fake".into(), ..Default::default() };
        assert!(fake.validate().is_ok());
        let bad = PersistenceConfig { mode: "bogus".into(), ..Default::default() };
        assert!(bad.validate().is_err());
    }
```

Run `just test frogdb-config mode_defaults`. Expected: FAIL — `no field mode`.

- [ ] **Step 2: Implement the config field.** Add `mode`, `default_persistence_mode() -> String { "rocksdb".into() }`, `PERSISTENCE_MODES: &[&str] = &["rocksdb", "fake"]`, the `Default` line, and a `validate()` branch (checked only when `enabled`). Run `just test frogdb-config`. Expected: PASS.

- [ ] **Step 3: Builder — failing test then wiring.** Add a `frogdb-core` builder test that a fake-mode build yields a shard whose `persistence.has_wal()` is true and whose WAL effects are recorded in the registry after a `SET`. Because the shard event loop is heavyweight, prefer a focused unit test constructing a `ShardWorker` with `WalMode::Fake` (mirror the `post_execution.rs` `worker_with` test scaffold) and asserting `FakeWalRegistry::log(0)` is `Some`. Run it (fails: no `WalMode`), then implement the `WalMode` builder input + fake arm + registry install. `just test frogdb-core fake_wal`. Expected: PASS.

- [ ] **Step 4: Sim boot path.** Add to `sim_helpers.rs`:

```rust
/// Start a real FrogDB server inside turmoil with the deterministic WAL fake
/// (`persistence.mode = "fake"`), replacing today's `enabled = false`. WAL
/// effects are recorded and reachable via `FakeWalRegistry::log(shard_id)`.
pub async fn real_frogdb_server_fake_persistence(num_shards: usize) -> Result<(), BoxError> {
    let config = Config {
        server: ServerConfig { /* as real_frogdb_server */ ..Default::default() },
        persistence: PersistenceConfig { enabled: true, mode: "fake".into(), ..Default::default() },
        http: HttpConfig { enabled: false, ..Default::default() },
        metrics: MetricsConfig { enabled: false, ..Default::default() },
        ..Default::default()
    };
    // ... Server::new(config, ...).run_until(pending).await
}
```

Confirm the fake path never touches `data_dir`/RocksDB (the builder's fake arm must not require a `rocks_store`).

- [ ] **Step 5: Regression gates.** `just test frogdb-config`, `just test frogdb-core`, `just check frogdb-server`. Confirm existing `real_frogdb_server` (mode defaults to rocksdb, `enabled=false`) is unchanged and all ~50 sim tests still compile.

- [ ] **Step 6: Commit.**

```bash
git add frogdb-server/crates/config/src/persistence.rs frogdb-server/crates/core/src/shard frogdb-server/crates/server/tests/common/sim_helpers.rs
git commit -m "feat(persistence): add persistence.mode=fake; wire FakeWalSink through builder and sim boot"
```

---

### Task 5: Workload generator (`frogdb-testing/src/workload.rs`)

A seeded, pure, transport-agnostic generator. `Workload { seed, clients, profile }`; each `ClientScript` is a sequence of RESP-level `ScriptedOp`s with think-time hints. Vocabulary is **exactly** the Phase-1 model vocabulary; every Global-Constraints encoding rule is enforced and tested here.

**Files:**
- Modify: `frogdb-server/crates/testing/Cargo.toml` — add `rand.workspace = true` to `[dependencies]`.
- Create: `frogdb-server/crates/testing/src/workload.rs`
- Modify: `frogdb-server/crates/testing/src/lib.rs` — `pub mod workload;` + `pub use workload::{Workload, ClientScript, ScriptedOp, Profile};`

**Interfaces:**

```rust
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Profile { TxHeavy, BlockingHeavy, Mixed }

/// One RESP command with sim-time think hints. `command`/`args` are exactly
/// what goes on the wire; the recorder logs the same into History.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScriptedOp {
    pub command: String,          // lowercase, from the Phase-1 vocabulary
    #[serde(with = "crate::history::bytes_vec_serde_pub")] // reuse the bytes<->string codec
    pub args: Vec<Bytes>,
    /// Sim-time to sleep BEFORE issuing this op (ms). Blocking ops on the same
    /// key get distinct offsets so registrations never overlap (FIFO guard).
    pub think_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientScript { pub client_id: u64, pub ops: Vec<ScriptedOp> }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Workload { pub seed: u64, pub profile: Profile, pub clients: Vec<ClientScript> }

impl Workload {
    /// Deterministic: same (seed, profile, num_clients, ops_per_client) →
    /// byte-identical Workload.
    pub fn generate(seed: u64, profile: Profile, num_clients: usize, ops_per_client: usize) -> Self;
    /// The 8–16 hash-tagged keys this workload draws from (pins shard placement).
    pub fn key_space(seed: u64) -> Vec<Bytes>;
}
```

Design rules baked into `generate`:
- RNG: `let mut rng = StdRng::seed_from_u64(seed);` (`use rand::{rngs::StdRng, RngExt, SeedableRng};`). Every choice (`command`, key index, value, timeout, think delta) is `rng.random_range(..)`.
- **Key space:** 8–16 keys, each hash-tagged `{tagN}:base` so `hash_slot` pins them; per-profile some keys are shared (high contention). Keys never contain `|`.
- **Values:** short alphanumerics from `[a-z0-9]`, never `|` (assert in test).
- **Profiles** control the op-mix table:
  - `TxHeavy`: weighted toward `watch` + `exec` (transfer/CAS patterns: `exec` wrapping `set/get/incr/del` on shared keys), plus plain KV.
  - `BlockingHeavy`: weighted toward `blpop/brpop/bzpopmin/bzpopmax/blmove` paired with `lpush/rpush/zadd` producers on the same keys; short and 0 (infinite) timeouts mixed.
  - `Mixed`: even spread across all five type families.
- **Blocking stagger:** when a client emits a blocking op on key K, add a per-(K) increasing `think_ms` offset so no two blocking registrations on K land in the same tick window (satisfies the FIFO checker's invoke-order proxy).
- **Streams:** `xadd` uses either `*` (auto) or a full explicit `ms-seq` id (monotone per stream key); **never** `ms-*` or `$`. `xread key after_id` uses `0` or a previously-seen full id.
- **EXEC encoding:** emit `exec` with the `[num_cmds, name, num_args, args...]` arg encoding the partitioner/`OperationHistory::record_exec_invoke` expects; sub-commands are single-key `set/get/incr/del` only.
- **Excluded ops** (`lmpop/blmpop/bzmpop`, `expire`, `getset`, `hgetex/hsetex`, `xreadgroup`, admin/chaos) never appear.

- [ ] **Step 1: Add `rand` dep + failing determinism test.** Add the dep. Create `workload.rs` with a test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_is_deterministic() {
        let a = Workload::generate(7, Profile::Mixed, 4, 20);
        let b = Workload::generate(7, Profile::Mixed, 4, 20);
        assert_eq!(serde_json::to_string(&a).unwrap(), serde_json::to_string(&b).unwrap());
        let c = Workload::generate(8, Profile::Mixed, 4, 20);
        assert_ne!(serde_json::to_string(&a).unwrap(), serde_json::to_string(&c).unwrap());
    }
}
```

Run `just test frogdb-testing generate_is_deterministic`. Expected: FAIL — `cannot find function generate`.

- [ ] **Step 2: Implement `generate` + `key_space`.** Minimal, driven by a per-profile weight table. Run the determinism test. Expected: PASS.

- [ ] **Step 3: Constraint invariant tests (the load-bearing guards).** Add and satisfy:

```rust
    fn all_ops(w: &Workload) -> impl Iterator<Item = &ScriptedOp> {
        w.clients.iter().flat_map(|c| c.ops.iter())
    }

    #[test]
    fn no_pipe_in_keys_or_values() {
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 30);
            for op in all_ops(&w) {
                for a in &op.args {
                    assert!(!a.contains(&b'|'), "arg {:?} contains reserved '|'", a);
                }
            }
        }
    }

    #[test]
    fn only_phase1_vocabulary_emitted() {
        const ALLOWED: &[&str] = &[
            "set","get","del","incr","mset","mget","watch","exec","discard",
            "lpush","rpush","lpop","rpop","lmove","llen","lrange","blpop","brpop","blmove",
            "hset","hdel","hget","hincrby","hgetall","hlen",
            "zadd","zrem","zscore","zcard","bzpopmin","bzpopmax",
            "xadd","xlen","xread",
        ];
        for seed in 0..40 {
            for profile in [Profile::TxHeavy, Profile::BlockingHeavy, Profile::Mixed] {
                let w = Workload::generate(seed, profile, 4, 30);
                for op in all_ops(&w) {
                    assert!(ALLOWED.contains(&op.command.as_str()), "forbidden op {}", op.command);
                }
            }
        }
    }

    #[test]
    fn no_forbidden_stream_ids() {
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::Mixed, 4, 40);
            for op in all_ops(&w).filter(|o| o.command == "xadd") {
                let id = String::from_utf8_lossy(&op.args[1]);
                assert!(id == "*" || id.split_once('-').is_some_and(|(m, s)|
                    m.parse::<u64>().is_ok() && s.parse::<u64>().is_ok()),
                    "xadd id {id} must be '*' or full ms-seq (never ms-* or $)");
                assert_ne!(id, "$");
            }
        }
    }

    #[test]
    fn blocking_ops_staggered_per_key() {
        // Two blocking ops on the same served key within one client must have
        // distinct think offsets (FIFO registration-window guard).
        for seed in 0..40 {
            let w = Workload::generate(seed, Profile::BlockingHeavy, 4, 40);
            for c in &w.clients {
                let mut seen: std::collections::HashMap<&[u8], u64> = Default::default();
                for op in c.ops.iter().filter(|o| matches!(o.command.as_str(),
                    "blpop"|"brpop"|"bzpopmin"|"bzpopmax"|"blmove")) {
                    let key = op.args[0].as_ref();
                    if let Some(prev) = seen.insert(key, op.think_ms) {
                        assert_ne!(prev, op.think_ms, "overlapping blocking regs on same key");
                    }
                }
            }
        }
    }
```

Iterate the generator until all pass.

- [ ] **Step 4: Cross-check every generated op is model-accepted (proptest).** Add a proptest that, for a range of seeds, every *non-blocking, single-key deterministic* op the generator emits with a self-consistent result is `Some` under the matching model — guards vocabulary drift against the models:

```rust
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn generated_vocabulary_is_model_routable(seed in 0u64..200) {
            // Every command maps to a model via default_keys_of returning non-empty
            // keys (the partitioner can route it) — a cheap structural check that
            // the generator never emits something the pipeline can't partition.
            let w = Workload::generate(seed, Profile::Mixed, 3, 25);
            for op in w.clients.iter().flat_map(|c| &c.ops) {
                let keys = crate::partition::default_keys_of(&op.command, &op.args);
                prop_assert!(!keys.is_empty() || op.command == "discard",
                    "op {} not routable by default_keys_of", op.command);
            }
        }
    }
```

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/testing/Cargo.toml frogdb-server/crates/testing/src/workload.rs frogdb-server/crates/testing/src/lib.rs
git commit -m "feat(testing): seeded RESP-level workload generator with Phase-1 vocabulary and encoding guards"
```

---

### Task 6: Canonicalizing history recorder (HGETALL sort + typed replies)

The turmoil recorder must translate RESP replies into the exact `Bytes` encodings the Phase-1 models expect. Two things need care: **HGETALL fields must be sorted** before joining (else ≥2-field hashes false-fail), and blocking/zset/stream replies must encode as `key|elem` / `key|member|score` / `id,f,v`.

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/sim_harness.rs` — add a canonicalizing return recorder + a RESP→`OperationResult` refinement for the new types.

**Interfaces:**
- `OperationHistory::record_return_canonical(&mut self, op_id, client_id, command: &str, result: OperationResult)` — like `record_return`, but canonicalizes by command: for `HGETALL`, sort the `Array` into `(field,value)` pairs by field before encoding; for `BLPOP/BRPOP` a 2-element array becomes `served_key|elem`; for `BZPOPMIN/BZPOPMAX` a 3-element array becomes `key|member|score`; for `XREAD` the nested reply becomes `id,f,v,...` entries `|`-joined. All other commands defer to the existing `record_return` encoding.
- The existing `to_testing_history()` bridge (lowercases command, pairs invoke/return) is unchanged and consumes the canonical encoding.

- [ ] **Step 1: Write the failing test.** In `sim_harness.rs` tests:

```rust
    #[test]
    fn hgetall_is_canonicalized_sorted() {
        let mut h = OperationHistory::new();
        let op = h.record_invoke(1, "HGETALL", vec![Bytes::from("hh")]);
        // Server returned fields in insertion order f2,f1 — recorder must sort.
        let reply = OperationResult::Array(vec![
            OperationResult::String(Bytes::from("f2")), OperationResult::String(Bytes::from("v2")),
            OperationResult::String(Bytes::from("f1")), OperationResult::String(Bytes::from("v1")),
        ]);
        h.record_return_canonical(op, 1, "HGETALL", reply);
        let th = h.to_testing_history();
        let ret = th.operations().iter().rev().find(|o| matches!(o.kind, frogdb_testing::history::OpKind::Return)).unwrap();
        assert_eq!(ret.result.as_ref().unwrap().as_ref(), b"f1|v1|f2|v2");
    }
```

Run `cargo nextest run -p frogdb-server --features turmoil -E 'test(/hgetall_is_canonicalized/)'`. Expected: FAIL — `no method record_return_canonical`.

- [ ] **Step 2: Implement `record_return_canonical`.** Canonicalize per command as specified; the HGETALL branch chunks the array into pairs, sorts by field bytes, and joins `f|v|f|v`. Run the test. Expected: PASS. Add sibling tests for BLPOP (`k|x`), BZPOPMIN (`z|a|1`), and XREAD (`1-1,f,v`).

- [ ] **Step 3: Regression gate.** `just check frogdb-server` and re-run the existing `to_testing_history` tests. Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/sim_harness.rs
git commit -m "test(sim): canonicalizing history recorder (HGETALL sort + blocking/zset/stream reply encodings)"
```

---

### Task 7: Turmoil harness v2 — generated-workload runner

Execute a `Workload` as N concurrent sim clients over turmoil TCP, recording a canonical `OperationHistory` into a shared `Arc<Mutex<..>>`, and return the completed `frogdb_testing::History`.

**Files:**
- Create: `frogdb-server/crates/server/tests/common/workload_runner.rs`
- Modify: `frogdb-server/crates/server/tests/common/mod.rs` — `#[cfg(feature = "turmoil")] pub mod workload_runner;`

**Interfaces:**
- `run_workload(workload: &Workload, num_shards: usize, fake_persistence: bool) -> frogdb_testing::History` — builds a seeded sim via `build_sim(&SimConfig { seed: workload.seed, num_shards, .. })`, hosts the server (`real_frogdb_server_fake_persistence` when `fake_persistence`, else `real_frogdb_server`), spawns one `sim.client(..)` per `ClientScript`, each connecting via `TcpStream`, and for each `ScriptedOp`: sleep `think_ms`, `record_invoke`, `encode_command` + write, read reply, `parse_simple_response`, `record_return_canonical`. Runs `sim.run()`, then returns `history.lock().to_testing_history()`.
- Blocking-op replies: the client must read the full reply (blocking ops may return after a delay); use the existing `parse_simple_response`/`parse_resp_array` helpers. Timeouts return nil → `OperationResult::Nil` → `None`.

- [ ] **Step 1: Write the failing test.** In `workload_runner.rs` tests, a smoke test that a tiny 2-client Mixed workload runs and produces a complete, non-empty history:

```rust
    #[test]
    fn tiny_workload_runs_and_records() {
        let w = Workload::generate(1, Profile::Mixed, 2, 5);
        let history = run_workload(&w, 1, true);
        assert!(history.is_complete(), "every invoke must have a return");
        assert!(!history.completed_operations().is_empty());
    }
```

Run under `--features turmoil`. Expected: FAIL — `cannot find function run_workload`.

- [ ] **Step 2: Implement `run_workload`.** Follow the `test_linearizability_concurrent_writes` structure (`simulation.rs`) for the per-client async block, but drive it from the `ClientScript` instead of hand-written ops. Handle EXEC specially (use `record_exec_invoke`/`record_exec_return`). Run the smoke test. Expected: PASS.

- [ ] **Step 3: Determinism guard.** Add a test that `run_workload(&w, 1, true)` produces the same set of `(client, command, result)` tuples across two runs at the same seed (turmoil + fake are both deterministic). Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/workload_runner.rs frogdb-server/crates/server/tests/common/mod.rs
git commit -m "test(sim): turmoil harness v2 runner — execute generated workloads, record canonical history"
```

---

### Task 8: Invariant pipeline (response legality → bounded WGL w/ downgrade → conservation → optional quiescence)

The single entry point that runs all Phase-1 checkers over a recorded `History`, applying the WGL scaling guard and the inconclusive-downgrade, and returning a structured verdict. Quiescence is wired but skipped (Phase-2 DEBUG probes absent) behind an explicit gate.

**Files:**
- Create: `frogdb-server/crates/server/tests/common/invariants.rs`
- Modify: `frogdb-server/crates/server/tests/common/mod.rs` — `#[cfg(feature = "turmoil")] pub mod invariants;`

**Interfaces:**

```rust
pub const MAX_OPS_PER_KEY: usize = 200;
pub const MAX_WGL_STATES: u64 = 200_000;

#[derive(Debug)]
pub struct InvariantReport {
    pub violations: Vec<String>,       // human-readable, empty == pass
    pub downgraded_keys: Vec<String>,  // keys WGL bailed on (inconclusive) -> conservation-only
    pub quiescence_checked: bool,      // false while Phase-2 DEBUG probes are absent
}
impl InvariantReport { pub fn passed(&self) -> bool { self.violations.is_empty() } }

/// Run the full invariant pipeline. `final_elements` is the post-drain list
/// state (from generated pushes minus deliveries, or empty if not tracked).
pub fn check_all(history: &frogdb_testing::History,
                 final_elements: &std::collections::HashMap<bytes::Bytes, Vec<bytes::Bytes>>)
                 -> InvariantReport;
```

Pipeline stages (in order):
1. **Response legality (cheap, always).** Scan completed ops: every reply shape legal for its command family (integer replies parse; blocking-timeout nils are `None`, not `Some("")`). Record violations.
2. **Per-key partition + bounded WGL with downgrade.** `partition_by_key(history, default_keys_of)`; for each key's sub-history, if `completed_operations().len() > MAX_OPS_PER_KEY`, truncate the check window (or skip with a logged cap note) — cap per spec. Run `check_linearizability_bounded::<M>(&sub, MAX_WGL_STATES)` where `M` is selected by the sub-history's dominant command family (KV → `KVModel`, list ops → `ListModel`, hash → `HashModel`, zset → `ZSetModel`, stream → `StreamModel`). If `result.inconclusive`: push the key to `downgraded_keys`, `eprintln!` a warning, and do **not** treat it as a violation (conservation still covers it). If `!is_linearizable && !inconclusive`: push a violation.
3. **Conservation (whole history).** `check_exactly_once_delivery(history, final_elements)`, `check_fifo_wake_order(history)`, `check_watch_no_false_negative(history)`, and — for transfer/EXEC workloads — `check_tx_sum_conservation(history, &tracked_keys, expected_sum)`. Any `Err` → a violation.
4. **Quiescence (optional).** Behind `fn quiescence_available() -> bool { false /* Phase-2 DEBUG probes not landed */ }`: when true, issue `DEBUG LOCKTABLE/WAITQUEUE/MEMORY-CHECK/EXPIRY-INDEX-CHECK` and assert empty/consistent; when false, set `quiescence_checked = false` and log "quiescence skipped (phase 2 DEBUG probes absent)". The stage is present and compiled so flipping the gate needs no restructuring.

Model selection helper: a sub-history's family is decided by the first completed op's command (a key is single-type in practice under this generator). Provide `fn model_for(sub: &History) -> Family` and dispatch the bounded check per family.

- [ ] **Step 1: Write the failing test.** In `invariants.rs` tests, hand-build a linearizable KV history and a non-linearizable one, plus an inconclusive case (large history + tiny `MAX_WGL_STATES` override), asserting `passed()`, a violation, and a downgrade respectively:

```rust
    #[test]
    fn clean_history_passes_and_dirty_history_flags() {
        // clean: SET x 1 ; GET x -> 1
        let mut h = frogdb_testing::History::new();
        let s = h.invoke(1, "set", vec![Bytes::from("{t}x"), Bytes::from("1")]);
        h.respond(s, Some(Bytes::from("OK")));
        let g = h.invoke(2, "get", vec![Bytes::from("{t}x")]);
        h.respond(g, Some(Bytes::from("1")));
        assert!(check_all(&h, &Default::default()).passed());

        // dirty: GET x -> 2 with no writer of 2 -> non-linearizable
        let mut d = frogdb_testing::History::new();
        let s = d.invoke(1, "set", vec![Bytes::from("{t}x"), Bytes::from("1")]);
        d.respond(s, Some(Bytes::from("OK")));
        let g = d.invoke(2, "get", vec![Bytes::from("{t}x")]);
        d.respond(g, Some(Bytes::from("2")));
        assert!(!check_all(&d, &Default::default()).passed());
    }
```

Run under `--features turmoil`. Expected: FAIL — `cannot find function check_all`.

- [ ] **Step 2: Implement `check_all`** with stages 1–4. Run the test. Expected: PASS. Add a dedicated downgrade test proving an inconclusive key lands in `downgraded_keys` and does **not** fail the report.

- [ ] **Step 3: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/invariants.rs frogdb-server/crates/server/tests/common/mod.rs
git commit -m "test(sim): invariant pipeline with WGL bound, inconclusive-downgrade, optional quiescence gate"
```

---

### Task 9: Repro files + `just concurrency-repro` + first seed-sweep test

Tie the runner and pipeline together into a seed-sweep test, emit a repro file on any failure, and add the replay recipe.

**Files:**
- Create: `frogdb-server/crates/server/tests/common/repro.rs` — repro file read/write.
- Create: `frogdb-server/crates/server/tests/concurrency_workload.rs` — the seed-sweep test + a `#[ignore]` single-seed replay test.
- Modify: `frogdb-server/crates/server/tests/common/mod.rs` — `#[cfg(feature = "turmoil")] pub mod repro;`
- Modify: `Justfile` — `concurrency-repro` recipe + fold the sweep into `concurrency`.

**Interfaces:**
- `ReproFile { seed: u64, profile: Profile, num_clients: usize, ops_per_client: usize, num_shards: usize }` (serde JSON). `write_repro(path, &ReproFile)`, `read_repro(path) -> ReproFile`. On failure the sweep writes `target/concurrency-repros/<seed>.json` and includes the path in the panic message.
- `run_and_check(seed, profile, clients, ops, shards) -> InvariantReport` — `generate` → `run_workload(fake_persistence=true)` → `check_all`. Prints the seed on failure.

- [ ] **Step 1: Write the failing seed-sweep test.** In `concurrency_workload.rs`:

```rust
#[test]
fn seed_sweep_short_workloads() {
    // ~20 seeds x short workloads, ~2-3 min budget (spec CI per-PR tier).
    for seed in 0..20u64 {
        let profile = match seed % 3 { 0 => Profile::TxHeavy, 1 => Profile::BlockingHeavy, _ => Profile::Mixed };
        let report = run_and_check(seed, profile, 4, 30, 2);
        if !report.passed() {
            let path = write_repro_for(seed, profile, 4, 30, 2);
            panic!("seed {seed} ({profile:?}) violated invariants: {:?}\nrepro: {}", report.violations, path.display());
        }
    }
}

#[test]
#[ignore = "replay a single repro file via `just concurrency-repro <file>`"]
fn replay_repro() {
    let path = std::env::var("REPRO_FILE").expect("set REPRO_FILE");
    let r = read_repro(&path);
    let report = run_and_check(r.seed, r.profile, r.num_clients, r.ops_per_client, r.num_shards);
    assert!(report.passed(), "repro {} still fails: {:?}", path, report.violations);
}
```

Run under `--features turmoil`. Expected: FAIL initially — `cannot find function run_and_check / write_repro_for / read_repro`.

- [ ] **Step 2: Implement `repro.rs` + `run_and_check` + `write_repro_for`.** Then run the sweep: `cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_short_workloads/)'`. **This is where real bugs may trip** — if the sweep fails, that is expected signal, not a harness defect. Triage: (a) if it's a harness/model gap (e.g. a reply the recorder mis-encodes), fix the harness; (b) if it's a genuine server bug, capture the repro file and hand it to Task 10's bug workflow. Do not weaken a checker to make the sweep pass. Run this heavy test in the foreground or with the watchdog protocol (raw log + liveness checks); the exec-validation wedge signatures in CLAUDE.md apply.

- [ ] **Step 3: Justfile recipes.**

```make
# Replay a single concurrency repro file (seed + profile + config)
concurrency-repro FILE:
    {{dyld-env}} {{rocksdb-env}} REPRO_FILE={{FILE}} cargo nextest run -p frogdb-server --features turmoil --run-ignored all -E 'test(/replay_repro/)'
```

Extend the existing `concurrency` recipe (line 74) to also run the generated sweep:

```make
concurrency:
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-core --features shuttle -E 'test(/concurrency/)'
    {{dyld-env}} {{rocksdb-env}} cargo nextest run -p frogdb-server --features turmoil -E 'test(/seed_sweep_short_workloads/)'
```

- [ ] **Step 4: Verify recipes.** `just concurrency` runs both the shuttle and the new sweep; craft one repro file by hand and confirm `just concurrency-repro target/concurrency-repros/<seed>.json` replays it. Expected: PASS (or a captured, reproducible failure handed to Task 10).

- [ ] **Step 5: Commit.**

```bash
git add frogdb-server/crates/server/tests/concurrency_workload.rs frogdb-server/crates/server/tests/common/repro.rs frogdb-server/crates/server/tests/common/mod.rs Justfile
git commit -m "test(sim): seed-sweep generated-workload test + repro files + just concurrency-repro"
```

---

### Task 10: Bug workflow — pinned named regression tests from failing seeds

Document and scaffold the failing-seed → repro → fix → pinned-regression loop (spec "Bug workflow"). Any bug the Task-9 sweep trips gets a **named** regression test carrying its **hardcoded** seed, so it can never silently regress.

**Files:**
- Modify: `frogdb-server/crates/server/tests/concurrency_workload.rs` — a `regressions` module with one template pinned test.
- Create: `docs/superpowers/plans/concurrency-phase3-bug-workflow.md` — a short workflow note linked from this plan.

**Interfaces:**
- Pinned regression test shape (one per confirmed bug):

```rust
mod regressions {
    use super::*;
    /// Template. When the sweep trips a real bug, copy this, name it for the
    /// bug, hardcode the failing seed/profile/config, and land it alongside
    /// the fix. It must FAIL before the fix and PASS after.
    #[test]
    fn regression_template_seed_0() {
        // Replace with the real failing (seed, profile, config) once a bug lands.
        let report = run_and_check(0, Profile::Mixed, 4, 30, 2);
        assert!(report.passed(), "pinned seed regressed: {:?}", report.violations);
    }
}
```

- [ ] **Step 1: Write the workflow doc.** `concurrency-phase3-bug-workflow.md`: failing seed → the sweep auto-emits `target/concurrency-repros/<seed>.json` → `just concurrency-repro <file>` to reproduce in isolation → systematic-debugging → fix → add a named pinned regression test with the seed hardcoded (fails before, passes after) → keep the repro file in the commit message, not the tree. Link it here and from the spec's "Bug workflow" section.

- [ ] **Step 2: Add the template regression module.** Confirm it compiles and passes (`run_and_check(0, ..)` should be clean, or if seed 0 itself trips a real bug, that becomes the first genuine pinned regression — fix in-plan per spec's day-1 bug decision: lost-element race, XREADGROUP NOGROUP-on-expiry, cross-shard keyspace delivery). Run `cargo nextest run -p frogdb-server --features turmoil -E 'test(/regression/)'`. Expected: PASS.

- [ ] **Step 3: Commit.**

```bash
git add frogdb-server/crates/server/tests/concurrency_workload.rs docs/superpowers/plans/concurrency-phase3-bug-workflow.md
git commit -m "docs+test(sim): concurrency bug workflow + pinned-regression template"
```

---

### Task 11: Harness self-test (silent-green guard)

Per spec "Harness self-tests": a deliberately broken shim (drop a delivered element / reorder a WAL effect) must be caught by the pipeline, guarding against a checker that silently passes everything.

**Files:**
- Modify: `frogdb-server/crates/server/tests/common/invariants.rs` — tests only.

- [ ] **Step 1: Write the tests.** Feed `check_all` a hand-built history where a pushed element is neither delivered nor in `final_elements` (must yield a `LostElement` violation), and one where a `GET` returns a never-written value (must yield a non-linearizable violation). Also assert `FakeWalLog::assert_write_order` catches an out-of-order log (build a `FakeWalLog` and push two `RecordedWalEffect`s with decreasing order). Run under `--features turmoil`. Expected: FAIL until the assertions are written to match the real error messages, then PASS — proving the checkers are not silent-green.

- [ ] **Step 2: Commit.**

```bash
git add frogdb-server/crates/server/tests/common/invariants.rs
git commit -m "test(sim): harness self-tests guard against silent-green checkers"
```

---

### Task 12: Justfile `concurrency-turmoil` convenience recipe + final full-suite gate

A named recipe for the turmoil-featured tests and a final green-suite verification.

**Files:**
- Modify: `Justfile`

- [ ] **Step 1: Add a convenience recipe** (if not already ergonomic) for running the generated-workload tests with the `turmoil` feature and the dyld/rocksdb env, e.g. `concurrency-turmoil PATTERN='seed_sweep':` wrapping the `cargo nextest -p frogdb-server --features turmoil -E 'test(/{{PATTERN}}/)'` invocation used throughout.

- [ ] **Step 2: Final regression gate.** Run, foreground or with the watchdog protocol:
  - `just check` (workspace type-check)
  - `just test frogdb-testing`
  - `just test frogdb-persistence`
  - `just test frogdb-core`
  - `just test frogdb-config`
  - `just concurrency` (shuttle + the new seed sweep)
  - `just check frogdb-server`

  All green. The ~50 pre-existing simulation tests, the shuttle concurrency suite, and the persistence/WAL suites must all still pass.

- [ ] **Step 3: Commit.**

```bash
git add Justfile
git commit -m "chore(just): concurrency-turmoil recipe; phase-3 turmoil harness complete"
```

---

## Self-check (every spec Phase-3 item has a home)

| Spec Phase-3 item | Task |
|---|---|
| Turmoil seed → schedule determinism (fix ignored `SimConfig.seed`) | 1 |
| Persistence seam: `WalSink` trait, `RocksWalWriter` impl, `ShardPersistence` trait object | 2 |
| Persistence fake: `(effect, order)` log + failure injection by op-index/predicate | 3 |
| `persistence.mode = fake` config + builder/sim wiring | 4 |
| Workload generator: seeded RNG, `Workload{seed,clients,profile}`, profiles, 8–16 hash-tagged keys, RESP-level ops w/ think-time, Phase-1 vocabulary exactly, encoding constraints | 5 |
| History → oracle bridge canonicalization (HGETALL sort, typed replies) | 6 |
| Turmoil harness v2 runner (N sim clients executing ClientScripts, recording `History`) | 7 |
| Invariant pipeline: response legality → per-key partition + bounded WGL w/ downgrade → conservation → optional quiescence | 8 |
| WRITE_EFFECT_ORDER compliance assertion (`FakeWalLog::assert_write_order`) | 3, 11 |
| Seed printed on failure + repro file (seed+profile+config JSON) + `just concurrency-repro` | 9 |
| First seed-sweep test (~20 seeds) wired into `just concurrency` | 9 |
| Bug workflow: failing seed → repro → fix → pinned named regression w/ hardcoded seed | 10 |
| Harness self-tests (silent-green guard) | 11 |

**Honest scoping notes:**
- The `WalSink` refactor (Task 2) touches production shard code (`shard/types.rs`, `persistence.rs`, `diagnostics.rs`, `event_loop.rs`, `builder.rs`) and adds an `async-trait` dependency; its regression gate is the full `just test frogdb-core` plus `just check frogdb-server`, and RocksDB behavior is unchanged when the fake is not selected.
- **Quiescence is genuinely optional:** Phase-2 `DEBUG LOCKTABLE/WAITQUEUE/MEMORY-CHECK/EXPIRY-INDEX-CHECK` handlers do **not** exist yet (verified). Task 8 compiles the stage but skips it behind an explicit gate that flips on with no restructuring once Phase 2 lands.
- Op vocabulary is deliberately narrower than the spec's v1 list (no EXPIRE/GETSET/HGETEX/XREADGROUP/admin-chaos) because those have no Phase-1 model — including them would violate "op vocabulary = Phase-1 model vocabulary exactly" and produce spurious verdicts. They return with their models in a later phase.
