# Proposal 22 — Give `RocksConfig` a `from_persistence` constructor so the operator-vs-invariant knob split lives next to the defaults

## Summary

Building the `RocksConfig` that opens the on-disk **Store** is done by hand in the server's recovery
path (`recovery/shards.rs`): `open_rocks` transcribes `PersistenceConfig` field-by-field, wires seven
operator-driven knobs to config fields, and then hardcodes the remaining knobs as literals — three of
which (`level0_file_num_compaction_trigger: 8`, `target_file_size_base: 128 MB`,
`max_bytes_for_level_base: 512 MB`) are byte-for-byte copies of `RocksConfig::default()`. The
knowledge "*which* RocksDB knobs are operator-tunable and which are fixed FrogDB invariants" is thus
split across two crates: the defaults live in `frogdb-persistence` (`rocks/config.rs`), the
partition-and-transcription lives in `frogdb-server` (`recovery/shards.rs`), and they are kept
consistent by hand. This proposal moves the mapping into a single `RocksConfig::from_persistence`
constructor owned by the persistence crate, so the invariant lives next to the defaults it depends on
and the duplicated literals disappear via `..Self::default()`.

The premise verified cleanly, with two corrections to the candidate's field accounting (below):
the duplicated-literal set is *larger* than claimed (five hardcoded knobs match `default()`, not
three), and `compression` is a seventh operator-driven knob the candidate omitted — both of which
strengthen the case rather than weaken it.

## Problem

`open_rocks` (`recovery/shards.rs:19-49`) is the **single production construction site** of a
non-default `RocksConfig`. Every other construction in the tree is `RocksConfig::default()` (tests
only — 60+ call sites across `core/src/persistence/*`, `rocks/tests.rs`, `store/hashmap.rs`,
`shard/search/lifecycle.rs`). So the whole "translate operator config → RocksDB knobs" seam is this
one struct literal, and it does three separable jobs mashed together:

1. **Operator passthrough** — seven fields read from `PersistenceConfig` (unit conversions, the
   `> 0 ? Some : None` rate-limit encoding, the compression string→enum parse).
2. **Fixed FrogDB invariants** — five fields set without consulting config, of which three are
   literal duplicates of `RocksConfig::default()` and two (`max_background_jobs`,
   `create_if_missing`) happen to re-derive the default expression.
3. **Compression parsing** — delegated to `parse_compression` in `server/src/server/util.rs`, which
   `unreachable!()`s on an invalid string, silently depending on `PersistenceConfig::validate` having
   run first (`config/src/persistence.rs:221-228`).

The fragmentation is a **locality** failure: to answer "is `target_file_size_base` operator-tunable?"
you must read `shards.rs` (it is hardcoded there) and cross-check `config.rs` (it is *not* a
`PersistenceConfig` field) and notice the literal equals `default()`. The decision has no single
owner. Its failure mode is silent drift: change `RocksConfig::default()`'s
`target_file_size_base` and every test keeps passing (they use `default()`), but the production
Store still opens with the stale `shards.rs` literal — the default and the real deployment diverge
with no compiler complaint. This is the exact honesty gap [proposal 19](19-config-defaults-honesty.md)
flags for config-vs-runtime skew, localized to the RocksDB tuning surface.

## Evidence (verified file:line)

- `open_rocks` builds the literal at **`frogdb-server/crates/server/src/recovery/shards.rs:22-39`**;
  it is the only non-default construction and is called once, from `recovery/mod.rs:172`.
- The three hardcoded knobs the candidate cites are verified identical between the two sites:
  - `level0_file_num_compaction_trigger: 8` — `shards.rs:30` vs `config.rs:61`.
  - `target_file_size_base: 128 * 1024 * 1024` — `shards.rs:31` vs `config.rs:62`.
  - `max_bytes_for_level_base: 512 * 1024 * 1024` — `shards.rs:32` vs `config.rs:63`.
- **Correction 1 (strengthens the case):** two *more* fields are hardcoded in `shards.rs` and equal
  to `default()`, so the duplicated-invariant set is **five, not three**: `max_background_jobs:
  num_cpus::get() as i32` (`shards.rs:25` vs `config.rs:56`) and `create_if_missing: true`
  (`shards.rs:24` vs `config.rs:55`). `..Self::default()` subsumes all five.
- **Correction 2 (strengthens the case):** `compression` is a **seventh** operator-driven knob the
  candidate's list omits — `compression: parse_compression(&config.compression)` (`shards.rs:24`),
  driven by the `compression` config field (`config/src/persistence.rs:47-50`). So the operator set is
  `{write_buffer_size, compression, block_cache_size, bloom_filter_bits, max_write_buffer_number,
  compaction_rate_limit_mb, flush_compact_range}` (7), and the fixed-invariant set is
  `{max_background_jobs, create_if_missing, level0_file_num_compaction_trigger, target_file_size_base,
  max_bytes_for_level_base}` (5) — a clean 7/5 partition over the 12-field struct.
- `RocksConfig` and its `Default` impl live in **`frogdb-server/crates/persistence/src/rocks/config.rs:31-68`**
  (crate `frogdb-persistence`), re-exported by `frogdb_core::persistence` (`core/src/persistence/mod.rs:13-15`,
  `core/src/lib.rs:112-114`). `CompressionType` is also defined there (`config.rs:69-87`).
- `PersistenceConfig` lives in **`frogdb-server/crates/config/src/persistence.rs:13-94`** (crate
  `frogdb-config`) — confirming the candidate's `frogdb-config?` guess. `RecoveryInputs.persistence`
  is `&'a PersistenceConfig` (`recovery/mod.rs:52`).
- **Crate dependency direction (the key design constraint), verified:**
  - `frogdb-persistence` depends only on `frogdb-types` (`persistence/Cargo.toml`).
  - `frogdb-config` depends only on `frogdb-config-derive` + `serde` (`config/Cargo.toml`); it does
    **not** depend on `frogdb-persistence` (grep for `frogdb_persistence` in `config/` is empty).
  - Therefore adding a `frogdb-persistence → frogdb-config` edge is **acyclic** — no path back from
    config to persistence exists.
  - **`frogdb-config` is currently a top-of-graph crate.** Its only consumers are `crates/server`,
    `frogdb-operator`, and the `ops/{docs,deb,helm}-gen` generators — all leaves. Critically,
    **`frogdb-core` does *not* depend on `frogdb-config`**, neither directly (its `Cargo.toml` lists
    no `frogdb-config`) nor transitively (none of its ten `frogdb-*` deps —
    types/protocol/vll/replication/cluster/persistence/acl/scripting/search/commands — pull it in;
    each grep = 0). This is the load-bearing fact for the design decision below: *any* placement of
    the mapping (persistence, or core, or config itself) introduces a **new** crate edge, because no
    crate at or below `frogdb-core` sees `frogdb-config` today.
- `parse_compression` (`server/src/server/util.rs:40-52`) `unreachable!()`s on an unknown string,
  relying on `PersistenceConfig::validate` (`config/src/persistence.rs:221-228`) having rejected it
  first. No test pins `open_rocks` itself; the persistence-layer tests all use `RocksConfig::default()`.

## Proposed design (Rust interface sketch)

Move the operator/invariant partition into the persistence crate as a constructor on `RocksConfig`,
so it sits directly beside the `Default` impl whose literals it must agree with. Add one
`frogdb-persistence → frogdb-config` dependency edge (verified acyclic). **This is a deliberate
downward relocation of `frogdb-config` in the DAG** — see the risk accounting below; the proposal
commits to this direction rather than treating it as a fallback.

```rust
// frogdb-persistence: rocks/config.rs — next to `impl Default for RocksConfig`
use frogdb_config::PersistenceConfig;

impl RocksConfig {
    /// Build the RocksDB knob set from operator-facing persistence config.
    ///
    /// Only the operator-tunable knobs are read from `cfg`; the fixed FrogDB
    /// tuning invariants (`max_background_jobs`, `create_if_missing`,
    /// `level0_file_num_compaction_trigger`, `target_file_size_base`,
    /// `max_bytes_for_level_base`) fall through to `Default`, so this is the
    /// single place the operator/invariant partition is decided.
    pub fn from_persistence(cfg: &PersistenceConfig) -> Self;
}

impl CompressionType {
    /// Parse a validated compression string (`none`/`snappy`/`lz4`/`zstd`).
    /// Contract: the caller validated via `PersistenceConfig::validate`; an
    /// unknown value falls back to the default rather than panicking.
    pub fn from_config_str(s: &str) -> Self;
}
```

The body wires exactly the seven operator knobs and lets `..Self::default()` supply the five
invariants — so the three duplicated literals (and the two re-derived expressions) vanish from the
codebase, existing in exactly one place (`Default`):

```rust
Self {
    write_buffer_size: cfg.write_buffer_size_mb * 1024 * 1024,
    compression: CompressionType::from_config_str(&cfg.compression),
    block_cache_size: cfg.block_cache_size_mb * 1024 * 1024,
    bloom_filter_bits: cfg.bloom_filter_bits,
    max_write_buffer_number: cfg.max_write_buffer_number,
    compaction_rate_limit_mb: (cfg.compaction_rate_limit_mb > 0)
        .then_some(cfg.compaction_rate_limit_mb),
    flush_compact_range: cfg.flush_compact_range,
    ..Self::default()   // the 5 fixed invariants, defined once
}
```

`open_rocks` collapses to a call plus the two arguments that legitimately come from
`RecoveryInputs`, not from `PersistenceConfig` — `num_shards` and `warm_enabled`:

```rust
let rocks_config = RocksConfig::from_persistence(config);
let rocks = Arc::new(RocksStore::open_with_warm(
    &config.data_dir, inputs.num_shards, &rocks_config, inputs.warm_enabled,
)?);
```

`parse_compression` in `server/util.rs` is deleted; `CompressionType::from_config_str` replaces it,
living beside the enum it produces (the string vocabulary `none/snappy/lz4/zstd` already exists in
three places — the enum, `parse_compression`, and `PersistenceConfig::validate`; this removes one).

**Interface shape.** `from_persistence` is a *deeper* seam than the status-quo struct literal: the
server hands over one whole config value and gets back a ready Store config, with the
partition-invariant hidden behind the call rather than transcribed at it. The five fixed knobs are no
longer even *nameable* at the call site, so they cannot drift from `Default`.

## Migration plan (ordered steps)

1. **Add the dependency edge.** `frogdb-persistence/Cargo.toml`: add `frogdb-config.workspace =
   true`. Confirm `just check frogdb-persistence` still resolves (acyclic — verified above).
2. **Add `CompressionType::from_config_str`** in `rocks/config.rs`, mapping the four validated
   strings and falling back to the default on the unreachable arm (a total function, no `unreachable!`
   in the storage crate). Port the `parse_compression` mapping verbatim.
3. **Add `RocksConfig::from_persistence`** in `rocks/config.rs` directly below `impl Default`, using
   `..Self::default()` for the five invariants.
4. **Rewrite `open_rocks`** (`recovery/shards.rs`) to call `from_persistence`; drop the `parse_compression`
   import and the `num_cpus` use if now unused.
5. **Delete `parse_compression`** from `server/src/server/util.rs` and fix its imports; `just check
   frogdb-server` drives out any stragglers.
6. **Run the suite** (`just test` on a Blacksmith testbox — full workspace) to confirm recovery and
   RocksDB open paths are green.

Each step compiles independently; the change is small and compiler-guided.

## Test plan

- **New unit test in `frogdb-persistence`** (`rocks/config.rs` tests), now possible because the
  mapping is a pure function in the persistence crate — no server boot, no disk:
  - `from_persistence` on `PersistenceConfig::default()` yields a `RocksConfig` **equal to**
    `RocksConfig::default()`. **The meaningful part of this assertion is the seven operator fields**:
    it pins that `PersistenceConfig`'s defaults still map onto `RocksConfig`'s defaults, so if someone
    changes one crate's default but not the other, the test fails. This is the **drift guard** and the
    honesty invariant proposal 19 wants. (The five *invariant* fields are equal to `default()`
    tautologically — `from_persistence` builds them via `..Self::default()`, so that half of the
    assertion can never fail and carries no guard value; it is included only for completeness /
    documentation of the full-struct equality.)
  - Non-default operator values map through correctly: MB→bytes conversions, `compaction_rate_limit_mb
    = 0 → None` vs `> 0 → Some`, each compression string → the right `CompressionType`,
    `flush_compact_range`/`bloom_filter_bits`/`max_write_buffer_number` passthrough.
- **`CompressionType::from_config_str`**: one assertion per valid string + the default-fallback arm.
  Fold in the parity check that it matches the deleted `parse_compression` for all four inputs.
- **Regression**: existing recovery/crash-recovery tests (`persistence/src/recovery.rs`,
  `core/src/persistence/crash_recovery_tests.rs`) stay green unchanged — they use `default()`, which is
  untouched.

## Risks & alternatives

- **Dependency direction (main risk — a real DAG change, committed to deliberately).** The design
  adds `frogdb-persistence → frogdb-config`. Verified acyclic: `frogdb-config` depends only on
  `frogdb-config-derive` + `serde`, never on persistence, so no cycle is possible. But this is **not**
  a marginal "storage crate reads a config struct" tweak — it is a **downward relocation of
  `frogdb-config` in the crate graph**, and the proposal owns that explicitly:
  - Today `frogdb-config` is *top-of-graph*: consumed only by `crates/server`, `frogdb-operator`, and
    the `ops/*-gen` leaves. Nothing at or below `frogdb-core` depends on it (verified — see Evidence).
  - After this edge, `frogdb-config` sits **beneath `frogdb-persistence`**. Because `frogdb-core`
    depends on persistence, and ~everything above core depends on core, the config crate (and its two
    deps, `frogdb-config-derive` + `serde`) becomes a transitive dependency of `frogdb-persistence`,
    `frogdb-core`, and every crate above core. That is a genuine widening of the low-level dependency
    surface, not free.
  - **Why we accept it anyway.** The relocation is cheap in practice — `frogdb-config` is a leaf-light
    crate (config-derive + serde, both already ubiquitous in the workspace via serde), so no heavy
    build cost or new external toolchain is pulled down. And it buys the design's core property:
    `RocksConfig::from_persistence` lives *next to* the `Default` impl whose literals it must agree
    with, which is exactly the locality the status quo lacks. Every candidate placement adds a *new*
    edge regardless (nothing at/below core sees config today — Evidence), so "avoid the edge entirely"
    is not on the table; the only choice is *which* crate gains it. We choose persistence because it
    is the one crate that owns `RocksConfig::default()`.
- **Alternative A — put `from_persistence` on `PersistenceConfig` (in `frogdb-config`), returning
  `RocksConfig`.** Rejected: it forces `frogdb-config → frogdb-persistence` (the opposite edge), and
  worse, puts the partition-invariant in the crate that does *not* own `RocksConfig::default()` —
  reintroducing the same split this proposal closes, just relocated. It also drags all of persistence
  (RocksDB/librocksdb-sys) *beneath* the config crate, which the ops/*-gen leaves consume — a far
  heavier downward pull than the chosen edge.
- **Alternative B — a free function in `frogdb-core::persistence`.** Correcting the earlier draft:
  `frogdb-core` does **not** depend on `frogdb-config` today (neither directly nor transitively —
  verified, Evidence), so this placement **also introduces a new edge** (`frogdb-core → frogdb-config`)
  — it is *not* dependency-free as previously claimed. Its only real advantage over the chosen design
  is that it keeps the edge out of the very-bottom storage crate (`frogdb-persistence` stays
  config-unaware); the cost is weaker locality — the mapping sits one crate away from the `Default`
  literals it must track, reopening a smaller version of the split this proposal closes. Given both
  options add an edge and the chosen one gives strictly better locality, B is a fallback only if a
  reviewer specifically wants `frogdb-persistence` to stay config-free.
- **Compression fallback vs `unreachable!` (deliberate behavior change, accepted).** Replacing
  `parse_compression`'s `unreachable!()` with a default-fallback arm is an **intentional** change to
  the failure mode, not an incidental refactor. Today an invalid compression string that bypassed
  `validate()` aborts loudly (panic in recovery); after this change it silently opens the Store with
  the default (LZ4). We accept this trade deliberately: crashing a storage-open/recovery path on a
  mis-tuned-but-openable knob is worse than opening with a safe default. The cost we are knowingly
  paying is the loss of a loud invariant tripwire — any *future* `PersistenceConfig` construction path
  that forgets to call `validate()` would now silently mis-tune compression instead of failing fast.
  Because every current construction path validates at config load, the arm remains unreachable in
  practice; the `from_config_str` doc comment states the contract and this fail-soft behavior
  explicitly so the tripwire's removal is discoverable.
- **No ADR interaction.** RocksDB tuning is pure **Store**-open (data-path) configuration. ADR-0001
  (Raft owns cluster metadata; the data path never goes through Raft) is untouched — this config never
  reaches the **Raft Metadata Plane**. `num_shards`/`warm_enabled` stay caller-supplied because they
  come from `RecoveryInputs`, not `PersistenceConfig`, and govern column-family layout, not tuning.
- **`snapshot`-config sibling.** `SnapshotConfig` (`config/src/persistence.rs:269`) has an analogous
  but separate shape; out of scope here — this proposal is scoped to the `RocksConfig` seam.

## Effort

**S.** One new dependency line, two new functions in one file (`rocks/config.rs`), a ~15-line
collapse of `open_rocks`, one deletion (`parse_compression`), and ~2 new unit tests. Confined to
`frogdb-persistence` plus one caller in `frogdb-server`; compiler-driven, no async/disk/ordering
changes, and the load-bearing `Default` impl is untouched.

## Related

- [Proposal 19 — config defaults honesty](19-config-defaults-honesty.md): the drift-guard test here is
  a concrete instance of the config-vs-runtime honesty invariant, localized to RocksDB tuning.
- [Proposal 13 — single config-param registry](13-config-param-single-registry.md): both attack
  "the same configuration fact is written in N places, kept consistent by hand"; 13 targets the
  CONFIG GET/SET param registry, 22 targets the persistence-config → RocksDB-knob translation.

## Adversarial review

**Verdict: AMEND** — premise CONFIRMED accurate (the `open_rocks` site, the 7/5 field partition, the
acyclic `persistence → config` edge, and the `parse_compression`/`validate` coupling all checked out).
The amendment was required because the Risks & Alternatives section carried the load-bearing
crate-direction decision on inaccurate cost data. All four issues resolved:

- **Major — "Alternative B keeps persistence dependency-free / core already depends on both crates
  transitively" is factually wrong.** *Confirmed and fixed.* Independently verified: `frogdb-core`
  lists no `frogdb-config` in its `Cargo.toml` and none of its ten `frogdb-*` deps pull it in (each
  grep = 0). Alternative B therefore *also* introduces a new edge (`frogdb-core → frogdb-config`) and
  is not dependency-free. Rewrote the Alt B bullet to state this, and added an Evidence bullet
  recording that nothing at/below `frogdb-core` sees `frogdb-config` today — so every candidate
  placement adds a new edge, narrowing B's real advantage to "keeps the bottom storage crate
  config-unaware."
- **Major — the `persistence → config` edge is understated as "common layering," when it is a
  downward relocation of a top-of-graph crate.** *Confirmed and fixed.* Verified `frogdb-config` is
  consumed only by `crates/server`, `frogdb-operator`, and the `ops/*-gen` leaves today. Rewrote the
  main risk bullet to own the relocation explicitly: config moves *beneath* persistence and becomes a
  transitive dep of persistence, core, and everything above core; the proposal now commits to this
  direction (config is leaf-light: config-derive + serde) rather than deferring to "fall back to B if
  a reviewer objects," and the design section flags it as deliberate.
- **Minor — the drift-guard test's invariant-field half is tautological.** *Confirmed and fixed.*
  Since `from_persistence` builds the five invariants via `..Self::default()`, their equality to
  `default()` cannot fail. Reworded the test-plan bullet so the guard value is attributed only to the
  seven operator fields (that `PersistenceConfig` defaults still map onto `RocksConfig` defaults); the
  invariant half is documented as completeness-only.
- **Minor — compression fail-soft is an intentional behavior change, not a footnote.** *Confirmed and
  fixed.* Promoted the bullet from a caveat to an explicit, deliberate acceptance: it documents the
  change from loud panic to silent LZ4 default, names the cost (loss of an invariant tripwire for any
  future path that skips `validate()`), and states the `from_config_str` doc comment must record the
  contract.

Reviewer's harmless note (a second `RocksConfig` struct-literal exists at `rocks/tests.rs:883`, a test
using `..default()`) is consistent with the proposal's "tests use `default()`" characterization; no
change needed. Proposal is sound to proceed.
