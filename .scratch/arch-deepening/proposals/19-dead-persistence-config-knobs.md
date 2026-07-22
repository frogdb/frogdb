# Proposal 19 — Two persistence config knobs accepted then discarded: honor `compression`, delete the vestigial `WalConfig.failure_policy` seed

## Summary (2-3 sentences)

Two persistence configuration values are parsed, validated, and stored into persistence-crate
config structs but never reach the RocksDB / shard-worker behavior they name. `persistence.compression`
is validated, parsed into `RocksConfig.compression`, exposed through `CONFIG GET compression`, and
registered as **not** a no-op — yet `RocksStore::open` ignores the field entirely and hard-codes a
fixed per-level compression schedule; `CompressionType::to_rocksdb` is `#[allow(dead_code)]`. The
second half of the candidate's claim is **partly wrong and corrected here**: the `wal-failure-policy`
CONFIG param is fully wired and effective through a `ConfigManager` `Arc<AtomicU8>`; what is dead is
the *duplicate* `WalConfig.failure_policy` field, which is read only as a builder fallback seed that
production overwrites and every test leaves at the default. Decision per knob: **honor** compression
by mapping the enum to a curated per-level **preset table** (each variant → an explicit 7-level
schedule, the default `Lz4` variant reproducing the historical array exactly), and **delete** the
redundant `WalConfig.failure_policy` field (the effective policy already has a single owner elsewhere).
Honoring is the recommended path but the clean **delete-alternative** (§ Knob A) is an equally sound
fallback if per-CF compression tuning is out of scope — a scalar codec enum is an imperfect fit for a
tiered schedule, so removing the false knob is defensible.

## Problem (verified)

Both knobs present a **seam that promises something the implementation does not deliver**. A config
parameter is a public interface: a user who sets `compression = "zstd"` or reads
`CONFIG GET compression` is entitled to believe it governs on-disk compression. Today it does not.
Separately, `WalConfig` carries a `failure_policy` field that reads as "this WAL config determines the
failure policy," but the field's value is discarded in the production path — a duplicate source of
truth that can silently diverge from the real one.

These are *distinct from* proposal 14's "dead config, wire-or-delete" class (a param never threaded to
any consumer). Here the values **are** threaded into a config struct and then dropped at the last hop —
accepted-then-discarded, not merely unwired. That makes them more dangerous: `CONFIG GET` reflects them,
and the registry marks compression `noop: false`, so the observability surface actively asserts an
effect that does not exist. This violates the project's observability-accuracy rule (misleading data is
not acceptable).

## Evidence (verified file:line)

### Knob A — `compression`: genuinely dead end-to-end (candidate CONFIRMED)

- `RocksConfig.compression: CompressionType` field —
  `frogdb-server/crates/persistence/src/rocks/config.rs:34`.
- Populated from the validated config string via `parse_compression` —
  `frogdb-server/crates/server/src/recovery/shards.rs:24`
  (`compression: parse_compression(&config.compression)`), parser at
  `frogdb-server/crates/server/src/server/util.rs:40-51`.
- The open path **never reads `config.compression`**. `open_with_cf_lister` hard-codes a fixed
  per-level schedule — `frogdb-server/crates/persistence/src/rocks/mod.rs:99-107`:

  ```rust
  cf_opts.set_compression_per_level(&[
      DBCompressionType::None, DBCompressionType::None,
      DBCompressionType::Lz4,  DBCompressionType::Lz4,
      DBCompressionType::Zstd, DBCompressionType::Zstd, DBCompressionType::Zstd,
  ]);
  ```

- `CompressionType::to_rocksdb` is the only translator from the enum to a `DBCompressionType`, and it
  is `#[allow(dead_code)]` with **zero callers** —
  `frogdb-server/crates/persistence/src/rocks/config.rs:78-86` (workspace grep for `to_rocksdb`: only
  the definition). Every `DBCompressionType` reference in the crate is the hard-coded array above.
- The param is exposed at runtime. It is a read-only (`mutable: false`) `ParamMeta` whose getter
  returns the boot string — `frogdb-server/crates/server/src/runtime_config.rs:1014-1018`
  (`getter: |mgr| mgr.static_config.compression.clone()`), so `CONFIG GET compression` returns e.g.
  `"zstd"` while RocksDB uses the fixed schedule regardless.
- **Additional lie not in the candidate:** the config registry marks it `noop: false` —
  `frogdb-server/crates/config/src/params.rs:883-889` (`name: "compression", mutable: false,
  noop: false`). The codebase asserts it has effect; it does not. Validated at
  `frogdb-server/crates/config/src/persistence.rs:221-228` against `["none","snappy","lz4","zstd"]`.

### Knob B — `wal-failure-policy`: the KNOB works; only `WalConfig.failure_policy` is dead (candidate CORRECTED)

- `WalConfig.failure_policy: WalFailurePolicy` field —
  `frogdb-server/crates/persistence/src/wal/config.rs:78`; computed and stored by `build_wal_config`
  — `frogdb-server/crates/server/src/server/util.rs:67-93`.
- `RocksWalWriter::new` reads only `mode`, `channel_capacity`, `batch_size_threshold`,
  `batch_timeout_ms` — `frogdb-server/crates/persistence/src/wal/writer.rs:32-63`. It does **not**
  read `failure_policy`. (Candidate CONFIRMED.)
- **Correction 1 — the field is not unread.** `WalConfig.failure_policy` *is* read, as a fallback
  seed in the shard builder — `frogdb-server/crates/core/src/shard/builder.rs:363-376`:

  ```rust
  let failure_policy = self.wal_failure_policy.clone()
      .unwrap_or_else(|| Arc::new(AtomicU8::new(WalFailurePolicy::default().as_u8())));
  ... if self.wal_failure_policy.is_none() {
      failure_policy.store(wal_config.failure_policy.as_u8(), Ordering::Relaxed);  // :373
  }
  ```

- **Correction 2 — production overwrites the seed.** `with_persistence` builds the worker *without*
  `with_wal_failure_policy` (`frogdb-server/crates/core/src/shard/worker.rs:316-345`), so the seed at
  builder.rs:373 runs — then `frogdb-server/crates/server/src/server/shards.rs:217` calls
  `worker.set_wal_failure_policy_flag(ctx.config_manager.wal_failure_policy_flag())`, **replacing** the
  atomic wholesale (`set_wal_failure_policy_flag` at
  `frogdb-server/crates/core/src/shard/worker.rs:170-172`). The seed is dead on arrival in production.
- **Correction 3 — the effective policy has a separate, complete owner.** `ConfigManager` derives its
  own `Arc<AtomicU8>` *directly from the raw config string*, not from `WalConfig.failure_policy` —
  `frogdb-server/crates/server/src/runtime_config.rs:603` (field), `704-705`
  (`WalFailurePolicy::from_config_str(&config.persistence.wal_failure_policy).as_u8()`), `727`
  (construction), accessor `wal_failure_policy_flag()` at `748-749`. `CONFIG SET wal-failure-policy`
  stores into the same atomic — `runtime_config.rs:1556-1560`. The atomic is consumed by the **Internal
  Shard** worker's rollback path, not the WAL flush thread — the `should_rollback()` accessor is defined
  at `frogdb-server/crates/core/src/shard/types.rs:404-408` and read by the two **production** execution
  sites `frogdb-server/crates/core/src/shard/execution.rs:349` (single-command) and `execution.rs:466`
  (MULTI/EXEC). (`rollback.rs:424` is inside the unit test `test_rollback_mode_flag_toggle`, not a
  runtime consumer — the prior draft mis-cited it as the consumer; corrected here.)
- **Correction 4 — the honor-direction in the candidate is wrong.** The candidate suggested "the
  writer owns the failure-policy atomic." The failure policy governs shard-worker rollback (whether a
  failed FrogDB WAL flush rolls the in-memory mutation back), *not* WAL flushing; the WAL writer is the
  wrong owner. `wal-failure-policy` is registered `mutable: true, noop: false` and is genuinely live —
  `frogdb-server/crates/config/src/params.rs:488-494`. So there is nothing to "honor": the knob already
  works. The only defect is the duplicate field.
- No test sets `WalConfig.failure_policy` to a non-default and relies on the builder seed: every
  `WalConfig` test literal uses `..Default::default()` (→ `Continue`), and every rollback test wires
  the atomic explicitly via `set_wal_failure_policy_flag`
  (`frogdb-server/crates/core/tests/shard_driver/scenario_s6.rs:57`, `rollback.rs:424`). The seed at
  builder.rs:373 therefore only ever stores `Continue` in tests and a value-that-gets-overwritten in
  production.

### Net verified picture

| Knob | Threaded into | Reaches its behavior? | Registry says | Verdict |
| --- | --- | --- | --- | --- |
| `compression` | `RocksConfig.compression` | **No** — schedule hard-coded (`mod.rs:99-107`) | `noop: false` (claims effect) | Honor or delete |
| `wal-failure-policy` (param) | `ConfigManager` `Arc<AtomicU8>` | **Yes** — shard rollback reads it | `noop: false` (true) | Keep as-is |
| `WalConfig.failure_policy` (field) | `WalConfig` | **No** — seed overwritten (`shards.rs:217`) | n/a (struct field) | Delete |

## Why it is shallow/fragmented (architecture vocabulary)

**`compression` is a broken Interface promise.** A config parameter is the widest public Interface a
storage engine has. `RocksConfig` presents `compression` as an input to `open`; `open` silently ignores
it. The **seam** promises a knob and drops it at the last hop, so **Locality** is inverted: to learn
that compression is actually fixed you must read `open_with_cf_lister`, not the config type that names
it. `to_rocksdb` being `#[allow(dead_code)]` is the smell made explicit — the adapter from the config
enum to the engine type exists, compiles, and is wired to nothing.

**`WalConfig.failure_policy` is a duplicated source of truth.** The effective policy is owned by one
deep module — the `ConfigManager` atomic, read live by the shard worker and mutated by `CONFIG SET`.
`WalConfig.failure_policy` is a *second* derivation of the same config string that flows through a
different path (`build_wal_config` → `WalConfig` → builder seed) and is discarded before it can matter.
Two derivations of one fact, kept equal only by coincidence (both parse the same string). This is low
**Leverage**: the field widens `WalConfig`'s surface and the `build_wal_config` body without gating any
production behavior. It fails the **deletion test** — remove it and the only caller (builder.rs:373)
loses a seed that production already overwrites and tests never exercise off-default.

**The registry makes both worse.** `params.rs` marks `compression` `noop: false`, so the honesty
surface proposal 13 is trying to make authoritative currently *lies* about this parameter. Whatever we
decide, the registry entry must end up telling the truth.

## Proposed design (Rust interface sketch — signatures only)

### Knob A — honor `compression` via a curated preset table (recommended over delete)

**Design constraint the prior draft got wrong.** RocksDB's fixed schedule
(`[None,None,Lz4,Lz4,Zstd,Zstd,Zstd]`) is a deliberate *two-codec* tiered strategy: hot shallow levels
(L0/L1) uncompressed for write/read latency, warm levels Lz4, deep levels Zstd for ratio. The earlier
sketch tried to honor the knob as "apply the configured codec to the compressed tail" **and** keep a
mandatory guard that the default `Lz4` reproduce the historical array exactly. Those two requirements
are mutually exclusive: a single-codec-tail mapping makes `Lz4` produce `[None,None,Lz4,Lz4,Lz4,Lz4,Lz4]`,
not the historical Lz4-then-Zstd array. No "codec applied uniformly to the tail" rule can emit a
two-codec array for one enum value. The prior design was internally contradictory; it is reworked here.

**Coherent design: the enum names a curated compression *preset*, not a single tail codec.**
`per_level_schedule` is an explicit lookup table — each variant maps to a fully-specified 7-level
schedule that is a deliberate engineering choice, not a mechanical `to_rocksdb()` fill. This resolves
the contradiction: the default `Lz4` preset *is* the historical mixed Lz4/Zstd array (guard passes with
no special-casing — it is simply that variant's table row), while the other variants are coherent
uniform-tail presets.

```rust
impl CompressionType {
    /// Curated per-level schedule for a 7-level CF. Each variant is an explicit
    /// operator-facing *preset*, not a uniform codec fill:
    ///   None  => [None,  None,  None,  None,  None,  None,  None ]  // compression off
    ///   Lz4   => [None,  None,  Lz4,   Lz4,   Zstd,  Zstd,  Zstd ]  // balanced default (historical)
    ///   Zstd  => [None,  None,  Zstd,  Zstd,  Zstd,  Zstd,  Zstd ]  // max ratio
    ///   Snappy=> [None,  None,  Snappy,Snappy,Snappy,Snappy,Snappy] // Snappy tail
    /// Shallow L0/L1 stay uncompressed in every non-`None` preset to protect
    /// write/read latency; only `None` compresses nothing.
    pub(crate) fn per_level_schedule(self) -> [DBCompressionType; 7];
}
```

`to_rocksdb` stays only as the per-cell helper the table is built from (drop its `#[allow(dead_code)]`,
since `per_level_schedule` now calls it); it is no longer claimed as a standalone knob-honoring path.

`open_with_cf_lister` reads the config instead of a literal:

```rust
cf_opts.set_compression_per_level(&config.compression.per_level_schedule());
```

`RocksConfig.compression` stays; the registry keeps `noop: false` (now true); `compression` stays
`mutable: false` because RocksDB compression is a CF-open property — it cannot change at runtime without
reopening (this is why it lives in `static_config`, correctly).

**Documented semantic caveat (the honest cost of this design):** because the default must reproduce the
historical two-codec array, the `Lz4` *preset* is a balanced mixed Lz4/Zstd schedule, not "Lz4 at every
compressed level." An operator reading `compression = "lz4"` and expecting pure Lz4 gets Zstd at the
deepest tiers. This asymmetry (`Lz4` = balanced, `Zstd`/`Snappy` = uniform tail) is the curated-preset
model's price and **must be spelled out in the config reference**. The alternative — making `Lz4`
uniform for consistency — drifts the default on-disk format (see Risks) and is rejected. If the
asymmetry is judged unacceptable, prefer the delete-alternative below over shipping a surprising knob.

*Delete alternative (equally sound; pick if per-CF tuning is out of scope):* a scalar codec enum is an
imperfect fit for a per-level tiered schedule, so removing the false knob is a legitimate — arguably
cleaner — resolution. Remove `RocksConfig.compression`, `to_rocksdb`, the `CompressionType` enum's dead
arms, the `parse_compression` call at `recovery/shards.rs:24`, and either drop the `compression` param
entirely (documenting the fixed schedule) or, if the param string must stay for compatibility, flip the
registry entry to `noop: true` so the honesty surface stops asserting an effect. This is pure
subtraction with none of the preset-asymmetry or Snappy-build risk (see Risks) that honoring carries.

### Knob B — delete the redundant `WalConfig.failure_policy` field

The effective policy already has a single owner (`ConfigManager` atomic → shard worker). Remove the
duplicate:

```rust
// wal/config.rs — WalConfig loses one field
pub struct WalConfig {
    pub mode: DurabilityMode,
    pub batch_size_threshold: usize,
    pub batch_timeout_ms: u64,
    pub channel_capacity: usize,
    // failure_policy: DELETED
}

// server/util.rs — build_wal_config stops computing/storing it
pub fn build_wal_config(config: &PersistenceConfig) -> WalConfig; // body drops the failure_policy match
```

The builder seed at `builder.rs:363-376` collapses to: if no `wal_failure_policy` atomic was supplied,
default to `WalFailurePolicy::default()` (already the fallback) — dropping the
`wal_config.failure_policy.as_u8()` store. The one warning that `build_wal_config` emits (rollback +
non-sync mode, `util.rs:75-81`) is worth **preserving**; relocate it to the config-validation/startup
path that already reads `config.persistence.wal_failure_policy` + `config.persistence.durability_mode`
(it is a config-consistency diagnostic, not a `WalConfig` concern). `WalFailurePolicy` the type stays —
it is the shared encoding for the live atomic.

*Honor alternative (rejected):* make `WalConfig.failure_policy` the single source and delete the
`ConfigManager` re-derivation. Rejected because `CONFIG SET wal-failure-policy` needs a mutable
`Arc<AtomicU8>` the shard worker observes live; `WalConfig` is a plain owned value cloned per shard and
cannot carry runtime mutations. The atomic is the correct owner; the field is the redundant copy.

## Migration plan (ordered steps)

**Knob A (honor compression):**
1. Add `CompressionType::per_level_schedule`; drop `#[allow(dead_code)]` from `to_rocksdb`
   (`rocks/config.rs`).
2. Replace the literal array at `rocks/mod.rs:99-107` with `config.compression.per_level_schedule()`.
3. Add a persistence unit test asserting each `CompressionType` yields the intended schedule; add a
   RocksDB open test that a non-default `compression` produces a store whose CF options reflect it (or,
   pragmatically, an integration assertion via `CONFIG GET` + engine property if exposed).
4. Confirm the registry entry `compression` (`params.rs:883-889`) is now truthfully `noop: false`; no
   change needed, but note it in the proposal-13 truth audit.

**Knob B (delete field), independent of A:**
1. Delete `failure_policy` from `WalConfig` (`wal/config.rs:78`, `:87`).
2. Remove the `failure_policy` computation from `build_wal_config`; relocate the rollback-vs-non-sync
   warning to the startup/validation path (`server/util.rs`).
3. Drop the `wal_config.failure_policy.as_u8()` seed store at `builder.rs:371-376`; keep the
   default-fallback branch.
4. Fix compilation fallout in `WalConfig` literals (test harnesses, `wal/tests.rs`,
   `crash_recovery_tests.rs`, `core/persistence/tests.rs`) — all use `..Default::default()` today, so
   deletion is mechanical.
5. `cargo check` / `just check frogdb-persistence frogdb-core frogdb-server`; run the rollback suites
   (`scenario_s6`, `shard/rollback`) to confirm the live atomic path is untouched.

The two knobs are independent; land B first (pure deletion, lowest risk), then A.

## Test plan

- **Compression honored (new):** unit test `per_level_schedule` against the full curated table for all
  four variants — `None` → all-`None`; `Lz4` → the historical `[None,None,Lz4,Lz4,Zstd,Zstd,Zstd]`;
  `Zstd` → `[None,None,Zstd,Zstd,Zstd,Zstd,Zstd]`; `Snappy` → `[None,None,Snappy×5]`. Pin every cell so
  the presets cannot drift silently. Open-time test that `RocksConfig { compression: Zstd, .. }` opens a
  store whose CF compression differs from `compression: None` (assert via a written+reopened store, or a
  RocksDB CF option readback if the binding exposes it).
- **Compression regression guard:** the `Lz4`-variant row above *is* the guard — it asserts the default
  reproduces the exact historical `[None,None,Lz4,Lz4,Zstd,Zstd,Zstd]` schedule, so honoring the knob
  does not silently change the default on-disk format. With the preset-table design this is a plain
  table-row assertion, not a special-case carve-out.
- **Snappy build-support probe (new):** an open-time test that `RocksConfig { compression: Snappy, .. }`
  opens successfully on the target build (see Risks) — this is the first value that routes to
  `DBCompressionType::Snappy`, so it must be exercised on the aarch64-Linux vendored RocksDB before the
  knob is honored.
- **WAL policy unchanged (existing must stay green):** `scenario_s6` and `shard/rollback` tests prove
  the live atomic drives rollback; they do not touch `WalConfig.failure_policy` and must pass
  unchanged after deletion.
- **CONFIG round-trip:** `CONFIG GET compression` still reflects boot config (unchanged);
  `CONFIG GET/SET wal-failure-policy` unchanged (registry entry already correct).
- **No off-default seed reliance (deletion safety):** confirm via grep that no `WalConfig` literal sets
  `failure_policy` to a non-`Continue` value (verified: none exists today).

## Risks & alternatives

- **Compression default-format drift.** The `Lz4`-variant table row is the mandatory regression guard:
  honoring the knob must keep the default mapping to the exact historical per-level array, or existing
  data directories would open with a different compression profile. Low risk with the guard; high if
  skipped. The curated-preset design makes the guard a natural table entry rather than a special case.
- **Preset asymmetry (`Lz4` = balanced, others = uniform tail).** Because the default must reproduce the
  historical two-codec array, the `Lz4` preset is *not* pure Lz4 — it uses Zstd at the deepest tiers,
  while `Zstd`/`Snappy` presets are uniform. An operator setting `compression = "lz4"` expecting Lz4
  everywhere gets a surprise. This is the honest cost of preserving the default format; it **must** be
  documented in the config reference. Making `Lz4` uniform for consistency is rejected because it drifts
  the default on-disk format (previous bullet). If the asymmetry is unacceptable, take the
  delete-alternative — do not ship a knob whose default value's name misdescribes its effect.
- **Snappy build support (latent, honor-only).** `compression = "snappy"` is validated and accepted
  today but silently ignored (the hard-coded Lz4/Zstd schedule runs), so it has never actually reached
  RocksDB. Honoring the knob routes it to `DBCompressionType::Snappy` for the *first time*. The target
  box builds RocksDB from **vendored source on aarch64 Linux** (not Homebrew); if that build is
  configured without Snappy support, a previously-inert value would newly **fail at CF open**. Mitigate
  with the Snappy open-time probe test (see Test plan) run on the actual build before honoring, and/or
  confirm `librocksdb-sys` is compiled with the `snappy` feature. The delete-alternative eliminates this
  risk entirely (Snappy never becomes reachable). `None`/`Lz4`/`Zstd` are already exercised by the
  historical hard-coded schedule, so only Snappy is newly at risk.
- **Compression stays immutable.** RocksDB compression is fixed at CF open; `mutable: false` is correct
  and must not change. Honoring the knob does not make it runtime-settable — a reopen (restart) is
  required. This matches Redis/Valkey (`rdbcompression`/`rdb-key-save-delay` are load-time) and Kvrocks
  (`rocksdb.compression` applies at open); no engine hot-swaps SST compression.
- **WAL field deletion is pure subtraction** — the effective path is untouched, so behavioral risk is
  near zero; the only fallout is compile errors in `WalConfig` literals, which the compiler enumerates.
- **Alternative: mark compression `noop: true` and keep the dead field.** Cheapest honesty fix, but
  ships a permanently-inert operator knob and forgoes real per-CF tuning. Recommended only if compression
  tuning is deliberately out of scope; the honor path is preferred because the machinery
  (`to_rocksdb`) already exists.
- **ADR / crate-direction:** neither change crosses a crate boundary in the wrong direction —
  compression stays inside `persistence` (`rocks`), the WAL field deletion is within `persistence` +
  its `core`/`server` consumers, and nothing touches the Raft Metadata Plane or the data path (data
  path never goes through Raft — ADR-0001). No `core`-internals leakage into `persistence`.

## Effort

**S–M.** Knob B is **S**: delete one field, drop one computation, relocate one warning, fix
`..Default::default()`-based literals (compiler-guided). Knob A is **M**: add `per_level_schedule`,
rewire one call site, and — the real cost — write the open-time / default-format-guard tests that pin
the schedule so the default on-disk format cannot drift. Both confined to `persistence` plus thin
`core`/`server` call-site edits; no async, ordering, or cross-plane changes.

## Related

- **Proposal 14 — dead config, wire-or-delete.** Sibling but *distinct*: proposal 14 covers parameters
  never threaded to any consumer. These two are **accepted-then-discarded** — parsed, validated, stored
  in a config struct, surfaced through `CONFIG GET`, and dropped at the final hop. The
  observability-accuracy stakes are higher here because `CONFIG GET` and the `noop: false` registry flag
  actively assert an effect that does not exist.
- **Proposal 13 — single config-param registry.** The `compression` `noop: false` entry
  (`params.rs:883-889`) is exactly the kind of registry claim proposal 13 wants to make authoritative;
  honoring compression makes that entry true rather than requiring it be flipped to `noop: true`.

## Adversarial review

**Verdict: AMEND** (core dead-config finding confirmed and valuable; the Knob-A honor design needed
rework). All revisions below applied in place.

- **[major] Knob-A honor design was internally contradictory** — "apply the configured codec to the
  compressed tail" *and* a guard requiring the default `Lz4` to reproduce the two-codec historical array
  `[None,None,Lz4,Lz4,Zstd,Zstd,Zstd]` cannot both hold, since no single-codec-tail rule emits a
  two-codec array for one enum value. **Resolved:** reworked the honor design from a "tail codec" fill to
  an explicit **curated preset table** where each variant maps to a deliberate, fully-specified 7-level
  schedule. The default `Lz4` preset *is* the historical mixed array (guard becomes a plain table row, no
  special-casing); `Zstd`/`Snappy` are uniform-tail presets. The resulting `Lz4`-preset asymmetry (not
  pure Lz4) is now called out as a mandatory documented caveat and added to Risks, and the
  delete-alternative is elevated to an equally-sound fallback (it dissolves the contradiction entirely).
  Verified against `mod.rs:99-107` (two codecs) and `config.rs:69-86` (`to_rocksdb`, single-codec).
- **[minor] Correction-3 cited `rollback.rs:424` as the production consumer** — that line is inside the
  unit test `test_rollback_mode_flag_toggle`, not runtime code. **Resolved:** corrected the citation to
  the accessor definition `types.rs:404-408` and the two real production reads `execution.rs:349`
  (single-command) and `execution.rs:466` (MULTI/EXEC); noted the mis-citation. Verified via grep of
  `should_rollback` across the shard module.
- **[minor] Snappy build-configuration risk not covered** — honoring routes `compression=snappy` to
  `DBCompressionType::Snappy` for the first time (today it is validated-but-ignored); the vendored
  aarch64-Linux RocksDB build could lack Snappy and newly fail at CF open. **Resolved:** added a
  Snappy build-support Risk entry and a Snappy open-time probe test; noted the delete-alternative
  eliminates the risk. `None`/`Lz4`/`Zstd` are already reachable via the historical schedule, so only
  Snappy is newly exposed.
- **Unchallenged and retained:** compression is genuinely dead end-to-end (`to_rocksdb` has zero real
  callers; schedule hard-coded at `mod.rs:99-107`; registry `noop:false` is a real lie); the WAL
  correction is accurate (the `wal-failure-policy` param is fully live via the `ConfigManager`
  `Arc<AtomicU8>`, while `WalConfig.failure_policy` is a dead seed overwritten at `shards.rs:217`); Knob
  B (delete the field) stands as pure subtraction. Premise and both decisions retained.
</content>
</invoke>
