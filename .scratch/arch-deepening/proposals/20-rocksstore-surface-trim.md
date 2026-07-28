# Proposal 20 — Trim the `RocksStore` public surface: delete dead methods, demote crate-internal ones, gate the cross-crate test seam behind a `test-support` feature

## Summary

`RocksStore` (`persistence/src/rocks/mod.rs`) exposes **24 `pub fn`** in one file. Three are
outright dead (zero callers), a further group is reached only from same-crate tests, the single-key
CRUD accessors (`put`/`get`/`delete`/`merge`/`put_opt`) turn out to have **no production callers at
all** (reached only from tests, some cross-crate), and the
`WriteBatch`-mutating primitives that the candidate exploration flagged as "RocksSink-only" are in
fact a two-consumer seam: the production **FrogDB WAL** flush path (`RocksSink`, same crate) **and**
the `core` crate's `#[cfg(test)]` crash-recovery tests. That second consumer is the finding that
governs the design — it makes the candidate's proposed `#[cfg(test)]` gating **incorrect**, because
`cfg(test)` is per-crate and `persistence` compiles as a plain (non-test) dependency when `core`'s
tests build. Naively `cfg(test)`-gating or `pub(crate)`-demoting those methods would break `core`'s
crash-recovery suite.

This proposal keeps the verified core premise (a wide, shallow surface mixing dead / test-only /
crate-internal methods into one undifferentiated `pub` band) but corrects the mechanics: **delete**
the dead methods, **demote to `pub(crate)`** the methods with only same-crate callers, and introduce a
`test-support` cargo **feature** to carry the genuinely cross-crate test seam (single-key
`put`/`get`/`put_opt`, `sync_wal`, and the raw-batch primitives the `core` crash tests drive),
instead of the visibility-breaking `cfg(test)`.

**Correction from adversarial review (crucial):** a full caller audit shows the single-key
`put`/`get`/`delete`/`merge`/`put_opt`/`delete_opt` accessors have **zero production callers** — every
one is reached only from `#[cfg(test)]` code (in `persistence`, in `core`, and in `server`). Production
writes flow through the WAL batch path (`RocksSink` → `batch_put`/`batch_merge`/`batch_clear_shard` →
`write_batch_opt`); production reads flow through `iter_cf` (`recovery.rs:96`). So the single-key CRUD
methods are **not** "the real interface" — they are test-only conveniences in the same category as
`put_opt`. The genuinely load-bearing production surface is `open`/`open_with_warm`, `iter_cf`,
`flush`, `has_data`, `num_shards`, `warm_enabled`, `clear_tier_shard`, `set_metrics_recorder`. This
proposal therefore re-bands `put`/`get` into the cross-crate test seam (Band C) and `delete`/`merge`
into the same-crate/dead bands, so the discipline is applied consistently rather than gating `put_opt`
while leaving its identical sibling `put` ungated. Every remaining member passes a deletion test.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current design |
| --- | --- | --- |
| `frogdb-server/crates/persistence/src/rocks/mod.rs` | 479 | `RocksStore` (L24-44) + the 24 `pub fn` (L47-479); dead trio at L238/265/473; test/internal methods interleaved |
| `frogdb-server/crates/persistence/src/rocks/columns.rs` | — | Tier resolvers: `put_tier`/`get_tier`/`delete_tier`/`iter_tier` (L79-113), warm/search-meta accessors, `tier_cf_handle` (`pub(crate)`, L55) — the deep layer the `mod.rs` shims delegate to |
| `frogdb-server/crates/persistence/src/rocks/checkpoint.rs` | — | `create_checkpoint`/`latest_sequence_number`/`path`/`load_staged_checkpoint` — the **Checkpoint** seam; out of scope for this proposal (already narrow, all production) |
| `frogdb-server/crates/persistence/src/wal/flush.rs` | — | `RocksSink` (`pub(super)`, L130-208) — the production consumer of the batch primitives; `impl WriteSink for RocksSink` stages via `batch_put`/`batch_delete`/`batch_merge`/`batch_clear_shard` and commits via `write_batch_opt` (L145-198) |
| `frogdb-server/crates/persistence/src/rocks/tests.rs` | 820+ | Same-crate unit tests; sole caller of `write_batch`, plus `batch_merge`/`batch_clear_shard` |
| `frogdb-server/crates/core/src/persistence/test_harness.rs` | — | `#[cfg(test)]` in `core`; calls `RocksStore::put_opt` (L145) and `sync_wal` (L176) cross-crate |
| `frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs` | — | `#[cfg(test)]` in `core`; imports `rocksdb::WriteBatch` (L28), drives `batch_put`/`batch_delete`/`write_batch_opt` cross-crate |
| `frogdb-server/crates/core/src/persistence/mod.rs` | — | Gates the two test modules `#[cfg(test)]` (L19-26); re-exports `frogdb_persistence::{rocks, …}` (L15) |
| `frogdb-server/crates/persistence/Cargo.toml` | — | **No `[features]` section today** — the `test-support` feature must be added here |
| `frogdb-server/crates/core/Cargo.toml` | — | `frogdb-persistence.workspace = true` (L33); would enable `features = ["test-support"]` in `[dev-dependencies]` |
| `frogdb-server/docs/adr/0002-single-database.md` | — | Single logical DB; storage stays behind `RocksStore`. Untouched. |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | — | Data path never through Raft; the cluster crate's own `WriteBatch` use (`cluster/src/storage.rs`) is the **Raft Metadata Plane** DB, not `RocksStore`. Untouched. |

## Problem (concrete verified evidence)

Fresh workspace greps (excluding `target/` and the definitions in `rocks/mod.rs`) give exact caller
sets. The 24 methods fall into four bands, not the one flat `pub` band they occupy today.

### Band A — dead, zero callers (verified)

- **`create_batch_for_shard` (`mod.rs:265`)** — 0 callers workspace-wide.
- **`delete_opt` (`mod.rs:238`)** — 0 callers. (Its non-`_opt` sibling `delete` → `delete_tier` is
  itself test-only — a single same-crate test caller — see Band B; the `_opt` variant has none at all.)
- **`open_shared` (`mod.rs:473`)** — 0 callers, and already carries `#[allow(dead_code)]`
  (`mod.rs:472`). A free `pub fn`, not even a method — a wrapper nobody calls.

### Band B — only same-crate callers (production and/or `persistence` tests)

- **`write_batch` (`mod.rs:251`)** — the non-`_opt` commit. 0 production callers; only
  `persistence/src/rocks/tests.rs:33, 141, 707`. The production commit path uses `write_batch_opt`
  (below), never this. Same-crate only → `pub(crate)`-able (or `cfg(test)` same-crate).
- **`batch_merge` (`mod.rs:285`)** — `wal/flush.rs:159` (production `RocksSink`, same crate) +
  `rocks/tests.rs:140`. No cross-crate caller.
- **`batch_clear_shard` (`mod.rs:325`)** — `wal/flush.rs:166` (production, same crate) +
  `rocks/tests.rs:706`. No cross-crate caller.
- **`spawn_clear_reclamation` (`mod.rs:385`)** — production `RocksSink` at `wal/flush.rs:190` (same
  crate) **plus** same-crate tests `rocks/tests.rs:829, 893`. No cross-crate caller. The free helper
  it forwards to lives at `reclaim.rs:105`. This one is `pub` but wholly `persistence`-internal, an
  over-broad export the candidate did not flag.
- **`delete` (single-key, `mod.rs:235`)** — 0 production callers; only one same-crate test,
  `rocks/tests.rs:22`. Its `_opt` sibling `delete_opt` is Band A (dead). Same-crate only →
  `pub(crate)` or `cfg(test)`.
- **`merge` (single-key, `mod.rs:224`)** — 0 production callers; only one same-crate test,
  `rocks/tests.rs:95`. Same-crate only → `pub(crate)` or `cfg(test)`. (Production merges go through
  `batch_merge` on the WAL batch path, not this single-key shim.)

All Band-B methods have their sole production consumer (`RocksSink`, declared `pub(super)` at
`flush.rs:130`) inside `persistence`, so `pub(crate)` fully satisfies production; only same-crate
tests otherwise touch them.

### Band C — cross-crate, test-only, **no production caller**

- **`put` (single-key, `mod.rs:204`)** — 0 production callers. Reached only from tests, and those
  tests span crates: `core/…/persistence/tests.rs:50, 93, 117`, `core/…/crash_recovery_tests.rs:369,
  773, 1226`, `core/…/store_recovery.rs` (`#[cfg(test)]` unit_tests, L154+), `core/…/test_harness.rs`
  (L134/155/165), `server/…/recovery/tests.rs:49`, plus same-crate `persistence/…/rocks/tests.rs:94,
  138` and `wal/tests.rs:46`. Cross-crate ⇒ Band C, not the "real interface."
- **`get` (single-key, `mod.rs:232`)** — 0 production callers. Reached only from tests, cross-crate:
  `core/…/persistence/tests.rs` (many), `core/…/crash_recovery_tests.rs:597`, same-crate
  `persistence/…/wal/tests.rs` and `rocks/tests.rs`. Production reads use `iter_cf` (`recovery.rs:96`),
  never this. Cross-crate ⇒ Band C.
- **`put_opt` (`mod.rs:207`)** — 0 production callers. Only `core/…/test_harness.rs:145` and
  `core/…/crash_recovery_tests.rs:739, 902, 1015`.
- **`sync_wal` (`mod.rs:433`)** — 0 production callers. Only `core/…/test_harness.rs:176` (the
  `TestHarness::sync_wal` wrapper the crash tests call at `crash_recovery_tests.rs:208`).

All are consumed **only** from other crates' (`core`, `server`) `#[cfg(test)]` modules and
`persistence`'s own tests — never from production code. Because the `put`/`get` consumers reach across
crate boundaries (into `core` and `server` test builds), they need the same `test-support` cargo
feature as `put_opt`/`sync_wal`, not `cfg(test)`.

### Band D — cross-crate: production consumer **and** cross-crate test consumer

This is the band the candidate got wrong. It claimed the batch primitives and `write_batch_opt` are
"consumed ONLY by RocksSink in wal/flush.rs … zero `rocksdb::WriteBatch` refs outside persistence."
Both halves are false:

- **`batch_put` (`mod.rs:274`)** — `wal/flush.rs:147` (production) **and**
  `core/…/crash_recovery_tests.rs:277, 325` (cross-crate test).
- **`batch_delete` (`mod.rs:296`)** — `wal/flush.rs:153` (production) **and**
  `core/…/crash_recovery_tests.rs:377` (cross-crate test).
- **`write_batch_opt` (`mod.rs:258`)** — `wal/flush.rs:179` (production) **and**
  `core/…/crash_recovery_tests.rs:284, 332, 383` (cross-crate test).
- **`rocksdb::WriteBatch` outside `persistence`:** `core/…/crash_recovery_tests.rs:28` imports it and
  constructs it at L269, 318, 374 to drive those primitives (Test 2.2 "WriteBatch operations are
  atomic"). (`cluster/src/storage.rs:318, 345, 372` also build `WriteBatch`, but against the crate's
  own `DB::open_cf_descriptors` at `storage.rs:53` — the **Raft Metadata Plane** store, unrelated to
  `RocksStore`; `cluster` does not even depend on `persistence`.)

**Why Band D defeats the candidate's mechanic.** The `core` crash-recovery tests are `#[cfg(test)]`
modules **in `core`** (`core/…/persistence/mod.rs:19-26`). `#[cfg(test)]` is evaluated per-crate:
when `cargo test -p frogdb-core` builds, `frogdb-persistence` is compiled as an ordinary dependency
**without** `--test`, so any `#[cfg(test)]`-gated `RocksStore` method simply does not exist in the
`persistence` rlib that `core`'s tests link against. Likewise `pub(crate)` in `persistence` is
invisible to `core`. So the candidate's "gate test helpers behind `#[cfg(test)]`" would **fail to
compile `core`'s crash-recovery suite** for `put_opt`, `sync_wal`, `batch_put`, `batch_delete`, and
`write_batch_opt`. The cross-crate test seam needs a **cargo feature**, not `cfg(test)`.

### Why it is shallow / wide (architecture vocabulary)

`RocksStore` is meant to be a **deep Module**: a small **Interface** over the substantial
implementation of column-family-per-**Internal-Shard** storage, tiering, merge operators, and
reclamation. Instead the `mod.rs` face is **wide and shallow** — 24 `pub fn` in one undifferentiated
`pub` band where deletion-test failures (Band A), same-crate helpers (Band B), and a test-only
cross-crate seam (Bands C/D) all wear the same maximal visibility as the real interface. **Locality**
suffers: a reader cannot tell from the signature whether `write_batch` is a production entry point
(it is not), whether the single-key `put`/`get`/`delete`/`merge` are load-bearing (they are not — all
test-only), or whether `delete_opt` mirrors a production `delete` (neither is production; `delete_opt`
is fully dead). **Leverage** is low for the
dead trio — surface area a caller must scan past, and for `open_shared` a whole extra construction
path shadowing `open`. The `WriteBatch`-in/`WriteBatch`-out primitives additionally **leak a
`rocksdb` type across the `mod.rs` Interface** where the deep tier layer (`columns.rs`) otherwise
keeps RocksDB handles crate-private behind `tier_cf_handle` (`pub(crate)`). The batch **Seam** is
real and worth keeping — `RocksSink` is the production adapter and the crash tests are a legitimate
second consumer — but it should be a *named, gated* seam, not an accident of everything being `pub`.

## Proposed design (Rust interface sketch — signatures only)

Four coordinated moves, all compiler-guided; no behavior change.

### 1. Delete Band A

Remove `create_batch_for_shard` (`mod.rs:265-273`), `delete_opt` (`mod.rs:238-250`), and the free
`open_shared` (`mod.rs:472-479`) outright. Nothing references them.

### 2. Demote Band B to `pub(crate)`

```rust
impl RocksStore {
    pub(crate) fn write_batch(&self, batch: WriteBatch) -> Result<(), RocksError>;
    pub(crate) fn batch_merge(&self, batch: &mut WriteBatch, shard_id: usize, key: &[u8], operand: &[u8]) -> Result<(), RocksError>;
    pub(crate) fn batch_clear_shard(&self, batch: &mut WriteBatch, shard_id: usize) -> Result<Option<Vec<u8>>, RocksError>;
    pub(crate) fn spawn_clear_reclamation(self: &Arc<Self>, tier: CfTier, shard_id: usize, upper_bound: Vec<u8>);
    // Single-key shims with only a same-crate test caller each:
    pub(crate) fn delete(&self, shard_id: usize, key: &[u8]) -> Result<(), RocksError>;
    pub(crate) fn merge(&self, shard_id: usize, key: &[u8], operand: &[u8]) -> Result<(), RocksError>;
}
```

`RocksSink` (same crate) and `rocks/tests.rs` (same crate) keep full access; the `mod.rs` public
face loses six members. (`delete`/`merge` could equally be `#[cfg(test)]` since their only callers are
same-crate tests — `pub(crate)` is chosen for symmetry with the other Band-B methods.)

### 3. Introduce a `test-support` feature for the cross-crate seam (Bands C + D)

Add to `persistence/Cargo.toml`:

```toml
[features]
# Exposes RocksStore's raw write-batch / opt / WAL-sync primitives to
# dependent crates' *test* builds (core's crash-recovery suite). Never
# enabled in a production build.
test-support = []
```

Both `core/Cargo.toml` **and** `server/Cargo.toml` enable it only for tests (both crates' test suites
call `put`/`get` cross-crate — `server/…/recovery/tests.rs:49`):

```toml
[dev-dependencies]
frogdb-persistence = { workspace = true, features = ["test-support"] }
```

Gate the Band-C methods (no production caller) so they exist in `persistence`'s own tests **and** any
dependent's `test-support` build, and are absent from production:

```rust
impl RocksStore {
    #[cfg(any(test, feature = "test-support"))]
    pub fn put(&self, shard_id: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError>;
    #[cfg(any(test, feature = "test-support"))]
    pub fn get(&self, shard_id: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError>;
    #[cfg(any(test, feature = "test-support"))]
    pub fn put_opt(&self, shard_id: usize, key: &[u8], value: &[u8], wo: &WriteOptions) -> Result<(), RocksError>;
    #[cfg(any(test, feature = "test-support"))]
    pub fn sync_wal(&self) -> Result<(), RocksError>;
}
```

Band D is the one part that needs care: each method has a **production** consumer (`RocksSink`) *and*
a cross-crate test consumer. Recommended end-state — make the production primitive `pub(crate)` and
expose the cross-crate test need through a single **named, feature-gated batch seam** rather than
re-widening three methods:

```rust
// Production primitives: crate-internal, consumed by RocksSink.
impl RocksStore {
    pub(crate) fn batch_put(&self, batch: &mut WriteBatch, shard_id: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError>;
    pub(crate) fn batch_delete(&self, batch: &mut WriteBatch, shard_id: usize, key: &[u8]) -> Result<(), RocksError>;
    pub(crate) fn write_batch_opt(&self, batch: WriteBatch, wo: &WriteOptions) -> Result<(), RocksError>;
}

// Cross-crate test seam: one explicit, feature-gated surface the crash tests drive,
// instead of three re-widened primitives. Lives in a `#[cfg(...)] mod test_support`.
#[cfg(any(test, feature = "test-support"))]
impl RocksStore {
    /// Stage a batch of raw put/delete ops — each carrying its own shard — and commit
    /// them atomically with the given WriteOptions, bypassing the FrogDB WAL. For
    /// crash-atomicity tests only.
    pub fn commit_raw_batch(&self, ops: &[RawBatchOp<'_>], wo: &WriteOptions) -> Result<(), RocksError>;
}

// NOTE: shard is per-op, NOT a single batch-level parameter. Test 2.3
// (`test_cross_shard_batch`, crash_recovery_tests.rs:313) stages puts to FOUR different
// shards into ONE WriteBatch and commits atomically to verify cross-shard all-or-nothing.
// A batch-level `shard_id` would force one batch per shard and destroy that property.
#[cfg(any(test, feature = "test-support"))]
pub enum RawBatchOp<'a> {
    Put { shard: usize, key: &'a [u8], value: &'a [u8] },
    Delete { shard: usize, key: &'a [u8] },
}
```

The `core` crash tests call `commit_raw_batch` (they currently open-code
`WriteBatch::default()`/`batch_put`/`write_batch_opt`); `RocksSink` keeps the `pub(crate)`
primitives. This removes the `rocksdb::WriteBatch` type from the cross-crate surface entirely, and the
per-op `shard` field lets a single call span shards exactly as `test_cross_shard_batch` requires.

*Lighter alternative (see Risks):* if rewriting the crash tests is undesirable, keep `batch_put`,
`batch_delete`, `write_batch_opt` gated by cfg-split visibility — `pub(crate)` normally,
`pub` under `test-support` — via `#[cfg_attr]`-free duplication:

```rust
#[cfg(not(any(test, feature = "test-support")))] pub(crate) fn batch_put(/* … */) -> Result<(), RocksError>;
#[cfg(any(test, feature = "test-support"))]        pub        fn batch_put(/* … */) -> Result<(), RocksError>;
```

Uglier (signature duplicated per method) but zero test rewrites.

### 4. Resulting `mod.rs` public face (the genuinely load-bearing production surface)

Only the methods with **verified production callers** stay unconditionally `pub`:

- **`open` / `open_with_warm`** — construction (`store_recovery.rs`, `server` startup).
- **`iter_cf`** (`persistence/src/recovery.rs:96`) — the production **read** path (recovery streams
  every key from a CF; there is no production single-key `get`).
- **`flush`** (`server/…/subsystems.rs:659`).
- **`has_data`** (`server/…/recovery/shards.rs:59`).
- **`num_shards`** (`core/…/store_recovery.rs:84, 86, 108`, production recovery loop).
- **`warm_enabled`** (`core/…/store_recovery.rs`, production warm-recovery branch).
- **`clear_tier_shard`** (`core/…/store/warm_tier.rs:133`, the **Warm Tier** FLUSHDB path).
- **`set_metrics_recorder`** (`server/…/server/mod.rs:244`).

`cf_handle` and `metrics_recorder` are already `pub(crate)`.

**Correction (major review finding).** The previous draft listed `put`/`get`/`delete`/`merge` here as
"the real interface" and cited `put` as having a production caller in `core/…/store_recovery.rs`. That
citation was **false**: the `.put` calls at `store_recovery.rs:154+` are inside the `#[cfg(test)] mod
unit_tests` (gated at `store_recovery.rs:122`); the real production adapters `recover_shard` /
`recover_all_shards` (L66/L79) never write — they read via `iter_cf` and populate an in-memory
`HashMapStore`. A full workspace grep returns **zero** production callers for any of
`put`/`get`/`delete`/`merge`/`put_opt`/`delete_opt`. Production writes go through the WAL batch
(`RocksSink`: `batch_put`/`batch_merge`/`batch_clear_shard`/`write_batch_opt`); production reads go
through `iter_cf`. The single-key CRUD accessors are therefore test-only, and this proposal bands them
accordingly: `put`/`get` → Band C (cross-crate test seam, `test-support`-gated), `delete`/`merge` →
Band B (`pub(crate)`, same-crate test only), `put_opt` → Band C, `delete_opt` → Band A (deleted).
Applying the proposal's own separate-real-from-test discipline consistently is the point; leaving `put`
`pub` while gating `put_opt` would have been the exact inconsistency the review flagged.

## Migration plan (ordered)

1. **Phase 1 — delete Band A (S).** Remove `create_batch_for_shard`, `delete_opt`, `open_shared`.
   `just check frogdb-persistence` (compiler confirms zero references). Standalone, zero-risk PR.
2. **Phase 2 — demote Band B to `pub(crate)` (S).** Change `write_batch`, `batch_merge`,
   `batch_clear_shard`, `spawn_clear_reclamation`, and the single-key `delete`/`merge` shims to
   `pub(crate)`. `just check frogdb-persistence` then `just check frogdb-server` — only same-crate
   callers exist, so this must compile untouched.
3. **Phase 3 — add the `test-support` feature (M).**
   a. Add `[features] test-support = []` to `persistence/Cargo.toml`.
   b. Add `features = ["test-support"]` to `frogdb-persistence` under **both** `core`'s **and**
      `server`'s `[dev-dependencies]` (server's recovery tests call `put` cross-crate too).
   c. Gate `put`, `get`, `put_opt`, `sync_wal` with `#[cfg(any(test, feature = "test-support"))]`.
   d. Band D: either (recommended) add the `commit_raw_batch` / `RawBatchOp` test seam behind the
      same cfg, demote `batch_put`/`batch_delete`/`write_batch_opt` to `pub(crate)`, and rewrite the
      three crash-test call sites (`crash_recovery_tests.rs:277-284, 325-332, 374-383`) to use it; or
      (lighter) apply the cfg-split visibility to the trio and leave the tests as-is.
   e. `just test frogdb-persistence`, `just test frogdb-core`, **and** `just test frogdb-server` must
      all pass (the latter two prove the cross-crate `test-support` wiring works for every dependent
      that calls the gated methods). Run the full suite on a Blacksmith testbox
      (`just tb-run "just test"`) since feature-unification touches multiple crates.

Phases 1 and 2 are independently landable and carry the bulk of the surface reduction with near-zero
risk; Phase 3 is the only part touching build config and cross-crate tests.

## Test plan

- **No new production behavior** — this is a visibility/deletion refactor, so the guarantee is "the
  same tests still pass," enforced by the compiler for Bands A/B.
- **Band A:** deletion is proven correct by `cargo check` (any stray reference fails the build);
  no test changes.
- **Band B:** `rocks/tests.rs` already exercises `write_batch`/`batch_merge`/`batch_clear_shard`;
  they stay green because tests are same-crate. Add no tests.
- **Band C/D — the load-bearing check is that `core`'s crash-recovery suite still compiles and
  passes with `test-support` enabled.** `just test frogdb-core` (running `test_harness` +
  `crash_recovery_tests`) is the regression guard that the feature seam replaces the previous `pub`
  reachability exactly. If Band D takes the `commit_raw_batch` route, port **all** the raw-batch tests
  to the new seam and assert identical recovered-key sets:
  - Test 2.2 `test_writebatch_atomic` (`crash_recovery_tests.rs:264`) — single-shard batch atomicity.
  - **Test 2.3 `test_cross_shard_batch` (`crash_recovery_tests.rs:313`) — stages puts to four
    different shards into one batch; this is why `RawBatchOp` carries a per-op `shard` (a batch-level
    `shard_id` cannot express it). Do not omit this one.**
  - Test 2.4 `test_batch_delete_atomic` (`crash_recovery_tests.rs:359`) — batch-delete durability.
  - The `write_batch_opt` durability paths exercised by the above.
  (The *lighter* cfg-split alternative keeps `batch_put`/`batch_delete`/`write_batch_opt` verbatim and
  handles all three tests, including the cross-shard case, with no test rewrites.)
- **Guard against accidental production exposure:** the guard must be **external** to `persistence` —
  a same-crate unit test compiles under `cfg(test)`, where the gated methods *are* present, so it can
  never observe their absence. Use CI that builds `persistence` with default features (no
  `test-support`) — a leaked production reference to `put`/`get`/`put_opt`/`sync_wal`/the raw-batch
  seam fails that build — and/or a `cargo public-api` snapshot asserting the default-feature public
  surface excludes them.
- **Full-suite gate:** `just tb-run "just test"` on the testbox after Phase 3 (feature unification
  can re-trigger builds across `server`/`core`).

## Risks & alternatives

- **The `#[cfg(test)]` trap is the whole point — do not regress to it.** The single largest risk is
  "fixing" Band C/D with `#[cfg(test)]` instead of the feature; that compiles `persistence` fine and
  only breaks when `core`'s tests link, which a `persistence`-only `just check` will not catch. The
  migration must run `just test frogdb-core` explicitly.
- **Band D rewrite vs cfg-split.** The `commit_raw_batch` seam is cleaner (no `WriteBatch` on the
  cross-crate surface, one named test entry point) but rewrites the batch test bodies that
  deliberately test raw RocksDB batch atomicity; the cfg-split keeps the tests verbatim at the cost of
  duplicated signatures. **`commit_raw_batch` must take per-op `shard` (via `RawBatchOp`), not a
  batch-level `shard_id`** — otherwise `test_cross_shard_batch` (Test 2.3), which spans four shards in
  one atomic batch, cannot be expressed and its all-or-nothing property is lost. With the per-op shard
  field it maps cleanly. Recommend `commit_raw_batch` for the deeper module boundary; cfg-split is an
  acceptable MVP (and sidesteps the seam-design question entirely by keeping `batch_put` verbatim).
  Either way the production surface ends up identical (`pub(crate)` primitives).
- **`test-support` feature hygiene.** A feature is additive and could be accidentally enabled by a
  non-test dependent, re-exposing the seam. Mitigate by enabling it *only* under `[dev-dependencies]`
  (never `[dependencies]`) and never referencing it from `server`'s runtime deps. Document the intent
  in the `Cargo.toml` comment (shown above).
- **`write_batch` (Band B) could alternatively be `#[cfg(test)]`.** Since its only callers are
  same-crate tests, plain `#[cfg(test)]` works here (unlike Band C/D) and is even tighter than
  `pub(crate)`. Minor; either is fine.
- **Re-banding `put`/`get`/`delete`/`merge` widens Phase 2/3 (accepted).** The review correction moves
  four methods previously assumed "real interface" into the test bands, so this proposal now touches
  the single-key CRUD shims too, and `server`'s `[dev-dependencies]` must also enable `test-support`.
  This is deliberate: gating `put_opt` while leaving `put` `pub` would be internally inconsistent. If
  a reviewer prefers a smaller blast radius, an acceptable *scoping* fallback is to leave `put`/`get`/
  `delete`/`merge` `pub` for now but relabel them honestly as **test-only conveniences pending
  removal** (not "the real interface"), and gate them in a follow-up — a scope decision, not a factual
  claim about production callers.
- **Scope discipline.** The `checkpoint.rs` and `columns.rs` surfaces (the deep tier layer) are
  intentionally out of scope — a separate deepening. This proposal only re-bands the `mod.rs` methods.
- **ADRs untouched.** ADR-0002 (single database) and ADR-0001 (data path never through Raft) are
  unaffected — no storage topology or Raft change; the cluster crate's separate `WriteBatch` use is
  its own Raft Metadata Plane DB.

## Effort

**M overall**, cleanly separable: **S** for Phase 1 (delete three dead methods) + **S** for Phase 2
(`pub(crate)` demotions) — both pure, compiler-verified, independently landable — and **M** for Phase
3, whose weight is entirely the `test-support` feature wiring and the Band-D test seam, not
algorithmic risk. No async, disk, ordering, or behavioral change anywhere.

## Related

- **Proposal 12** (`12-snapshot-coordinator-surface-trim.md`) — the direct precedent: same
  deletion-test discipline applied to the `SnapshotCoordinator` trait (delete vestigial methods,
  demote scheduler-owned ones). This proposal is the `RocksStore` analogue, with the added wrinkle
  that `RocksStore`'s test seam is genuinely cross-crate, so a cargo feature replaces `#[cfg(test)]`.
- **Proposal 06** (`06-snapshot-scheduler.md`) — extracting the deep core beneath a trimmed surface;
  same "push depth down, narrow the face" shape.
- **`frogdb-server/CONTEXT.md`** — canonical terms used here: **Internal Shard**, **Store**, **FrogDB
  WAL** vs **RocksDB WAL** (the batch primitives feed the RocksDB write batch → RocksDB WAL step),
  **Checkpoint**, **Warm Tier**, spill/unspill.

## Adversarial review

**Verdict: AMEND** (premise verified SOUND, not refuted). Two majors + two minors, all addressed in
place. Caller sets independently re-verified against the tree at revision time.

- **Major 1 — false "production caller: put" / mis-banded "real interface" (RESOLVED).** The review
  proved `put`/`get`/`delete`/`merge`/`put_opt`/`delete_opt` all have **zero** production callers:
  the `.put` at `store_recovery.rs:154+` is inside `#[cfg(test)] mod unit_tests` (gate at
  `store_recovery.rs:122`); production `recover_shard`/`recover_all_shards` (L66/L79) read via
  `iter_cf` and write to an in-memory `HashMapStore`. Confirmed by grep: only test files call these
  methods (`delete` = 1 same-crate test caller, `merge` = 1, `put`/`get` = cross-crate `core`/`server`
  tests + same-crate). Fix: rewrote Section 4 to list only the genuinely production-called surface
  (`open*`, `iter_cf`, `flush`, `has_data`, `num_shards`, `warm_enabled`, `clear_tier_shard`,
  `set_metrics_recorder`), added an explicit correction paragraph, and re-banded `put`/`get` into
  Band C (test-support-gated), `delete`/`merge` into Band B (`pub(crate)`). Summary, Bands A/B/C, the
  shallow/wide section, migration Phase 2/3, and the risks all updated to apply the discipline
  consistently. Noted `server/Cargo.toml` also needs `test-support` in `[dev-dependencies]`.

- **Major 2 — `commit_raw_batch(shard_id)` cannot express Test 2.3 cross-shard batch (RESOLVED).**
  Confirmed `test_cross_shard_batch` (`crash_recovery_tests.rs:313`) stages `batch_put` to shards
  0..4 into one `WriteBatch` and commits atomically. Fix: changed `RawBatchOp` to carry a per-op
  `shard` field (`Put { shard, key, value }` / `Delete { shard, key }`) and dropped the batch-level
  `shard_id` parameter from `commit_raw_batch`, so one seam call spans shards. Added Test 2.3 (and 2.4)
  to the test-plan enumeration (previously omitted) with an explicit "do not omit" note, and updated
  the Band-D risk bullet. The cfg-split alternative already handles the cross-shard case verbatim.

- **Minor 3 — `spawn_clear_reclamation` "only wal/flush.rs:190" incomplete (RESOLVED).** Confirmed
  additional same-crate callers `rocks/tests.rs:829, 893`. Band B bullet corrected; the `pub(crate)`
  conclusion is unchanged (all same-crate).

- **Minor 4 — in-`persistence` unit-test guard is unworkable (RESOLVED).** Correct: a same-crate test
  compiles under `cfg(test)` where the gated methods exist, so it cannot observe their absence.
  Replaced with the external guard only — CI building `persistence` with default features (no
  `test-support`) and/or a `cargo public-api` snapshot; dropped the unit-test phrasing.

Phases 1 (delete Band A) and 2 (`pub(crate)` demote) remain correct and low-risk. The core mechanic
(cross-crate seam needs a cargo **feature**, not per-crate `#[cfg(test)]`; resolver `"2"` keeps
`test-support` out of production) is unchanged and was independently confirmed by the reviewer.
