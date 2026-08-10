# Proposal 44 — One `RocksStore` open interface (`RocksOpenOptions`) + a `TestKv` home for the shadow single-key API

## Summary

`RocksStore` publishes **five** open entry points — four public
(`open`, `open_with_metrics`, `open_with_warm`, `open_with_warm_metrics`) and one
private (`open_with_cf_lister`) — over exactly **one** implementation. Four of the
five are pure argument-forwarding shims; the fifth carries the entire ~160-line
open path. The four public names are the 2×2 product of two optional axes
(`warm_enabled: bool` × `metrics: Arc<dyn MetricsRecorder>`), so the interface a
caller must learn grows as **2^N in the number of open options**, while the
behaviour behind it stays one function. A third axis already exists — the
injectable `list_cf` enumerator — and it is parked in the private fifth fn
precisely because a sixth public name was unpalatable.

The sharpest form of the problem: of **174** call expressions across the
workspace, exactly **two are production** (`recovery/src/shards.rs:29`,
`replication-runtime/src/install.rs:243`). The whole metrics × warm matrix exists
to spell defaults for tests — `open` (137 call expressions) and
`open_with_metrics` (4) have **zero** production callers.

Alongside it, `rocks/mod.rs` interleaves a **shadow single-key key/value API**
(`put`, `put_opt`, `get`, `delete`, `merge`, `write_batch`, `sync_wal`,
`commit_raw_batch` + `RawBatchOp`) into the same `impl RocksStore` block as the
production methods, each carrying its own `#[cfg(any(test, feature =
"test-support"))]` or `#[cfg(test)]`. Nine gated items are threaded through four
downstream crates' dev-dependency feature flags. A reader of `rocks/mod.rs` must
decide *per method* whether it is real.

This proposal (a) collapses the open matrix to **one public options type plus the
zero-option `open` spelling**, leaving `open_with_cf_lister` as a single
crate-internal seam, and (b) moves the shadow API behind one gate — a
`test_support` module, optionally a `TestKv<'_>` wrapper — so the production
interface of `RocksStore` is what it says it is. **The body of the open path is
not touched**: the statement order inside `open_with_cf_lister` is
FM-tagged and is preserved verbatim.

## Files involved (verified paths + counts)

| Path | Lines | Role |
|------|-------|------|
| `frogdb-server/crates/persistence/src/rocks/mod.rs` | 692 | The four public opens (78, 90, 100, 119), the private one (145), and the nine gated shadow-API items (364–435, 608–620, 630–666) |
| `frogdb-server/crates/persistence/src/rocks/tests.rs` | 1697 | 36 FM-PERSISTENCE tags; the only caller of `open_with_cf_lister` (374); the shim-intent test `noop_default_shim_reclaims_without_panicking` (991) |
| `frogdb-server/crates/recovery/src/shards.rs` | 183 | **Production caller #1** — `open_with_warm_metrics` at 29 (recovery phase 2, `RecoveryPhase::OpenRocks`) |
| `frogdb-server/crates/replication-runtime/src/install.rs` | ~830 | **Production caller #2** — `open_with_warm` at 243 (`read_snapshot` over a staged checkpoint); also the 4 cross-crate `put_tier` callers (605, 609, 658, 780), all inside its `#[cfg(test)] mod tests` (365) |
| `frogdb-server/crates/persistence/src/rocks/columns.rs` | 147 | The ungated tier ops `put_tier`/`get_tier`/`delete_tier` (79–111) and `iter_tier` (113) |
| `frogdb-server/crates/persistence/src/rocks/manifest.rs` | 364 | `ColumnFamilyManifest::reconcile` — the guard the open path transcribes; doc-links `open_with_warm` at 24 |
| `frogdb-server/crates/persistence/src/rocks/wal_watermark.rs` | 250 | `detect_and_reset` / `write`, called at the tail of the open path (mod.rs:293–297) |
| `frogdb-server/crates/persistence/Cargo.toml` | — | `test-support = []` (17) |
| `frogdb-server/crates/{core,recovery,replication,server}/Cargo.toml`, `frogdb-server/benchmarks/Cargo.toml` | — | Five dev-dependency declarations of `features = ["test-support"]` (core:79, recovery:23, server:200, replication:31, benchmarks:24) |
| `.scratch/hardening/specs/persistence-failure-modes.md` | 732 | LOCKED. Rows 024/027/029/030/031/032/034/035 pin this path; `RocksStore::open` appears in prose twice (378 under FM-024, 696 under FM-041) |

Call-site census (`grep -o 'RocksStore::<name>('`, whole workspace, excluding
`target/`):

| Entry point | Call expressions | Production callers |
|---|---:|---:|
| `RocksStore::open` | 137 | **0** |
| `RocksStore::open_with_warm` | 30 | **1** (install.rs:243) |
| `RocksStore::open_with_metrics` | 4 | **0** |
| `RocksStore::open_with_warm_metrics` | 2 | **1** (shards.rs:29; the other is tests.rs:922) |
| `RocksStore::open_with_cf_lister` (private) | 1 | 0 (FM-032 forcing test, tests.rs:374) |
| **Total** | **174** | **2** |

The **non-default migration set is 36** (30 + 4 + 2): every site that passes an
explicit `warm_enabled` and/or `metrics`. The 137 all-defaults `open` sites do not
move.

## Problem

### 1. Combinatorial constructor axes

`mod.rs:73–152` is, in order:

- **78** `open(path, num_shards, config)` → forwards with `warm = false`, `metrics = Noop`
- **90** `open_with_metrics(path, num_shards, config, metrics)` → forwards with `warm = false`
- **100** `open_with_warm(path, num_shards, config, warm_enabled)` → forwards with `metrics = Noop`
- **119** `open_with_warm_metrics(path, num_shards, config, warm_enabled, metrics)` → forwards with the real `DB::list_cf`
- **145** `open_with_cf_lister(path, num_shards, config, warm_enabled, metrics, list_cf)` → the implementation

Three of the four public fns have a body that is one call. The doc comments are
themselves a symptom: 74–77, 87–89 and 98–99 exist only to explain *which
defaults this spelling picks*, and mod.rs:59–65 (a field comment on `metrics`)
has to enumerate the shims to explain the injection policy. Sixty-five lines of
interface for one function.

The axis count is not stable. `open_with_metrics` was added by round-10 proposal
24 (metrics injection) purely so one Main-tier reclamation test could pass a real
recorder without enabling the warm tier — an entire public name minted to fill one
cell of the matrix, with four call sites and no production caller. The `list_cf`
axis (added for FM-PERSISTENCE-032) was *not* given a public name, and the comment
at 126–129 explains the asymmetry.

### 2. Interface facts a caller must reconstruct

Everything a caller must know to pick an entry point is encoded in the *name*, not
in the types: which of the five defaults it silently supplies, that the
`NoopMetricsRecorder` is deliberate rather than a placeholder to be overwritten
later (mod.rs:59–65 spends seven lines saying so), and that
`open_with_warm_metrics` is "the production entry point" (114). Adding an
argument to the open path today means touching four signatures and 174 call
sites, most of which do not care about the argument.

### 3. The shadow single-key API shares the production `impl` block

Nine items in `mod.rs` are test-only and interleaved with production methods:

| Item | Line | Gate (line) | Production callers |
|---|---:|---|---:|
| `put` | 365 | `any(test, feature = "test-support")` (364) | 0 |
| `put_opt` | 372 | `any(test, feature = "test-support")` (371) | 0 |
| `merge` | 394 | `test` (393), `pub(crate)` | 0 |
| `get` | 414 | `any(test, feature = "test-support")` (413) | 0 |
| `delete` | 422 | `any(test, feature = "test-support")` (421) | 0 |
| `write_batch` | 429 | `test` (428), `pub(crate)` | 0 |
| `sync_wal` | 609 | `any(test, feature = "test-support")` (608) | 0 |
| `RawBatchOp` | 631 | `any(test, feature = "test-support")` (630) | 0 |
| `commit_raw_batch` | 650 | `any(test, feature = "test-support")` on the `impl` block (643) | 0 |

Each carries a 4–8 line doc comment restating "test-only convenience with zero
production callers" and naming the production path the reader should have looked
at instead — nine near-identical disclaimers. Five downstream `Cargo.toml`s carry
a dev-dependency feature declaration (three of them with a comment explaining why
it is safe), and an estimated ~226 call expressions of the shape
`.put|get|delete(<shard>, …)` across ~13 files depend on them.

### 4. Verified caveat: the gate hides the *name*, not the capability

The tier ops in `columns.rs` — `put_tier` (79), `get_tier` (94), `delete_tier`
(104) and `iter_tier` (113) — are all `pub` and **ungated**. So the doc claim
"absent from production builds" (mod.rs:363, 412, 420) is true of the spelling
only: `RocksStore::put_tier(CfTier::Main, shard, k, v)` compiles in a production
build today. `iter_tier` is the fourth member of that ungated set; unlike the
other three it has no gated twin at all — its production caller `iter_cf`
(mod.rs:577) is itself ungated, so `iter_tier` leaks no capability the production
interface does not already offer.

**Why the hole is not closed here.** The mechanical fix is `pub(crate)` on
`put_tier`/`get_tier`/`delete_tier` (`iter_tier` can follow — it has no
cross-crate caller either). The concrete blocker is `put_tier`: it has **4
cross-crate callers**, all in `replication-runtime/src/install.rs` (605, 609,
658, 780), inside that crate's `#[cfg(test)] mod tests` (365).
`frogdb-replication-runtime` is **not** one of the five crates that declare
`frogdb-persistence = { features = ["test-support"] }`. So the real fix is
`pub(crate)` ×3 + a **sixth** `test-support` dev-dependency (on
`frogdb-replication-runtime`) + repointing those 4 sites onto the gated
`put`/`put_warm` spelling. That is a coherent change, but it is a different
change from this one: it edits a locked crate's feature graph rather than its
open interface. Recorded as a finding; `columns.rs` is an explicit non-goal
(below).

## Why it is shallow (architecture vocabulary)

- **The interface is nearly as complex as the implementation.** Five entry
  points, one body. Depth is leverage at the interface — behaviour per unit of
  interface a caller must learn. Here four of the five names buy zero behaviour;
  they buy argument defaults. The interface is shallow by construction.
- **The seam is in the wrong place.** `open_with_cf_lister` is a legitimate
  **internal seam** — private to the implementation, used by the module's own
  tests (LANGUAGE.md: a module may have internal seams as well as the external
  one). The four public opens are not seams at all: nothing varies across them.
  *One adapter means a hypothetical seam; two means a real one* — the metrics axis
  has two real adapters (`NoopMetricsRecorder`, the server recorder) and the warm
  axis two real values, but neither needs a *name* per combination.
- **The deletion test.** Delete `open_with_metrics` and `open_with_warm`:
  complexity does not vanish, it moves to 34 call sites as an extra default
  argument each — that is a pass-through, not a module earning its keep. Delete
  `open` (137 sites, all-defaults) and real noise appears at every site; `open`
  earns its name, the other three do not.
- **The interface is the test surface.** Today it is the *test* surface that
  dictates the interface: the axis matrix exists for tests, and nine test-only
  methods sit in the production `impl`. Callers and tests cross the same seam;
  when the tests need a different shape, that shape belongs behind its own name
  (`TestKv`), not spliced into the production one.

## Proposed change

### A. One options type; `open` stays as the zero-option spelling

```rust
/// Everything the open path needs, with the optional axes defaulted.
/// Adding an axis costs one field and one setter — never a new entry point.
pub struct RocksOpenOptions<'a> {
    path: &'a Path,
    num_shards: usize,
    config: &'a RocksConfig,
    warm_enabled: bool,                                  // default false
    metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>, // default Noop, documented
}

impl<'a> RocksOpenOptions<'a> {
    pub fn new(path: &'a Path, num_shards: usize, config: &'a RocksConfig) -> Self { … }
    pub fn warm(mut self, enabled: bool) -> Self { self.warm_enabled = enabled; self }
    pub fn metrics(mut self, m: Arc<dyn MetricsRecorder>) -> Self { self.metrics = m; self }
    pub fn open(self) -> Result<RocksStore, RocksError> {
        RocksStore::open_with_cf_lister(self, |opts, p| DB::list_cf(opts, p).map_err(RocksError::from))
    }
}

impl RocksStore {
    /// Open with every option at its default (non-warm, no-op metrics).
    pub fn open(path: &Path, num_shards: usize, config: &RocksConfig) -> Result<Self, RocksError> {
        RocksOpenOptions::new(path, num_shards, config).open()
    }

    /// Unchanged body. **This is the production open path** — every open,
    /// production or test, runs it; the only thing private about it is the name.
    /// The `list_cf` parameter is the one internal seam (FM-032).
    fn open_with_cf_lister(
        opts: RocksOpenOptions<'_>,
        list_cf: impl FnOnce(&Options, &Path) -> Result<Vec<String>, RocksError>,
    ) -> Result<Self, RocksError> { /* mod.rs:153–311 verbatim */ }
}
```

`open_with_metrics`, `open_with_warm`, `open_with_warm_metrics` are deleted.

**The win is the growth rate, not the name count.** A builder trades three entry
points for `new` + two setters, so counting public names proves nothing. What
changes is how the surface grows: today each new optional axis **doubles** the
entry-point count (O(2^N) — and the `list_cf` axis was already forced private to
avoid paying it); after the change each axis costs exactly one setter (O(N)), and
there is exactly one place the open body is entered.

The two production call sites become:

```rust
// recovery/src/shards.rs:29
let rocks = Arc::new(
    RocksOpenOptions::new(&config.data_dir, inputs.num_shards, &rocks_config)
        .warm(inputs.warm_enabled)
        .metrics(inputs.metrics_recorder.clone())
        .open()?,
);

// replication-runtime/src/install.rs:243
let rocks = RocksOpenOptions::new(staged_dir, num_shards, rocks_config)
    .warm(warm_enabled)
    .open()
    .map_err(|e| classify_open_failure(e, num_shards, warm_enabled))?;
```

The 137 `open(path, n, cfg)` sites are **untouched**. Only the 36 non-default
sites migrate, mechanically (`sed`-able per former entry point).

Why keep the name `open` rather than a single `RocksOpenOptions::open`:
FM-PERSISTENCE-024's Invariant text names `RocksStore::open` verbatim ("the same
marker `RocksStore::open` trusts", spec:378), FM-PERSISTENCE-041's Invariant names
it again (spec:696), as do `staged.rs:36`, `checkpoint.rs:61`,
`recovery/src/lib.rs:230`, `server/src/server/shards.rs:65` and
`config/src/persistence.rs:105`. Keeping the name means **no LOCKED-spec prose
edit**, which keeps this a pure-refactor change rather than a spec-first one.

### B. One gate for the shadow API

Move the gated items out of the production `impl` block into
`rocks/test_support.rs`, declared once:

```rust
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
```

Two shapes, in increasing order of separation:

- **B1 (recommended first).** Keep the methods on `RocksStore` and only relocate
  them to the new file behind the single `mod` gate. **Zero call-site churn**;
  `mod.rs` becomes production-only and its `metrics` field comment (59–65) shrinks
  with the shims it names. Three mechanical details, all verified:
  - **Seven of the nine `cfg` attributes collapse; two must stay.** The seven at
    364, 371, 413, 421, 608, 630 and 643 carry the `any(test, feature =
    "test-support")` gate and are subsumed by the `mod` gate. `merge` (393) and
    `write_batch` (428) are `#[cfg(test)] pub(crate)` — narrower than the module
    gate. They **keep their nested `#[cfg(test)]`**: under a `--features
    test-support` build the module compiles but nothing in the workspace calls
    either (both have same-crate test callers only), so dropping the inner gate
    produces `dead_code`, which is a hard error under the repo's clippy settings.
  - **`RawBatchOp` needs a re-export or one import edit.** It is a type, not a
    method: after the move it lives at `crate::rocks::test_support::RawBatchOp`,
    while `core/src/persistence/crash_recovery_tests.rs:19` imports it as `use
    super::rocks::{RawBatchOp, RocksConfig, RocksStore}`. Either add `#[cfg(any(test,
    feature = "test-support"))] pub use test_support::RawBatchOp;` to `rocks/mod.rs`
    (zero call-site churn, recommended) or edit that one import.
  - The in-crate uses (`tests.rs:1449–1459`) are covered by either choice.
- **B2 (optional follow-on).** Introduce the wrapper, so the cross-crate shadow
  API stops living on `RocksStore` at all:

```rust
pub struct TestKv<'a>(&'a RocksStore);
impl RocksStore { pub fn test_kv(&self) -> TestKv<'_> { TestKv(self) } }
impl TestKv<'_> {
    pub fn put(&self, shard: usize, k: &[u8], v: &[u8]) -> Result<(), RocksError> { … }
    pub fn put_opt(…) …  pub fn get(…) …  pub fn delete(…) …
    pub fn sync_wal(…) …  pub fn commit_raw_batch(…) …
}
```

Where each item lands under B2:

- **On `TestKv`**: the six cross-crate names — `put`, `put_opt`, `get`, `delete`,
  `sync_wal`, `commit_raw_batch`. These are the ones whose presence on
  `RocksStore` is the actual problem.
- **Not on `TestKv`**: `merge` and `write_batch` stay inherent `#[cfg(test)]
  pub(crate)` methods inside `test_support.rs`. They are invisible outside the
  crate and their only callers are the crate's own tests, so wrapping them buys
  nothing and would widen a `pub(crate)` item to `pub`.
- **`RawBatchOp`** stays a free item in `test_support.rs` (it is an argument type
  for `commit_raw_batch`, not a method), re-exported under the same gate as in B1.

`RocksStore`'s production interface then carries exactly one gated name
(`test_kv`). Cost: an estimated **~226** call expressions gain `.test_kv()` across
~13 files. **That figure is an estimate, not a measured count** — a workspace grep
for `.put(`/`.get(`/`.delete(` cannot distinguish these from `HashMap::get` and
friends, so it over-counts badly (1766 raw hits). The migration is
**compiler-driven, not `sed`-driven**: delete the inherent methods, then fix every
resulting `E0599` until the workspace builds. Real diff volume, no design risk,
and worth its own commit. B2 does **not** close the `put_tier(CfTier::Main, …)`
hole from finding 4; that lives in `columns.rs` and is out of scope.

## Testability improvement

- **Adding an open axis stops costing an interface change.** Whatever the next
  axis turns out to be, it is one field plus one setter rather than a doubling of
  the entry-point count — and the FM-032 lister seam gets a precedent for staying
  private.
- **The `list_cf` seam becomes uniform with the rest.**
  `test_cf_enumeration_failure_propagates_and_preserves_data` (tests.rs:360)
  currently spells out five positional arguments including an explicit
  `Arc::new(NoopMetricsRecorder)` just to reach the sixth; after the change it
  reads `RocksStore::open_with_cf_lister(RocksOpenOptions::new(t.path(), 2, &cfg),
  |_, _| Err(…))`. Same seam, same assertions, less ceremony.
- **`rocks/mod.rs` becomes readable as a production interface.** Today nine of
  ~30 methods are test-only and each needs its own disclaimer paragraph. After
  B1 the file answers "what can production do to a `RocksStore`" without the
  reader adjudicating `cfg` attributes; after B2 the answer is enforced by the
  type.
- **New mutation targets are pre-covered.** The setters `warm()`/`metrics()` are
  new mutable code:
  - `warm()` is forced by the FM-PERSISTENCE-031 tests (`test_warm_cf_disabled`
    tests.rs:167, `test_warm_toggle_on_then_off_fails` 230,
    `test_warm_toggle_off_then_on_succeeds` 251, and the rest of that row's
    ten-test Forced-by list) — a mutant that ignores the argument flips those from
    pass to fail.
  - `metrics()` is forced by `warm_clear_tier_shard_reclaims_and_counts`
    (tests.rs:918), which asserts
    `frogdb_flush_compact_{started,completed}_total == Some(1)`, and by the helper
    `reopen_and_measure` (tests.rs:1191), whose three FM-034/FM-035 callers (1238,
    1272, 1294) assert an exact non-zero
    `frogdb_wal_recovery_dropped_records_total`. Both need a real recorder to
    reach the store, so a mutant that drops the injected recorder fails them.
  - **Correction to an earlier draft:** `reclamation_disabled_by_config_knob`
    (tests.rs:954) does **not** force `metrics()`. Its metric assertion is
    `counter_value(...) == None` — it asserts the recorder receives *nothing*, so a
    mutant that substitutes a `Noop` recorder still passes.

## Risks / scope boundaries

### Spec rows this path is pinned by (LOCKED — `Status: LOCKED (2026-08-02)`, gate 0.85)

| Row | What it pins in this path |
|---|---|
| FM-PERSISTENCE-024 | `is_complete_db()` checked **before** the first install rename. Its three forcing tests (`test_load_staged_checkpoint_incomplete_dir_refuses_and_preserves_data`, `incomplete_staged_checkpoint_is_refused_without_touching_live_db`, `is_complete_db_requires_the_rocksdb_manifest_pointer`) all exercise `staged.rs::is_complete_db`; the row's mention of `RocksStore::open` (spec:378) is descriptive prose about which marker RocksDB trusts. It is therefore a **naming argument for keeping the `open` spelling, not a behavioural constraint on this move** |
| FM-PERSISTENCE-027 | Recovery phase order …→ install staged checkpoint → **open RocksDB** → restore shards →… ; `RecoveryPhase::OpenRocks` |
| FM-PERSISTENCE-029 | `has_data()` is a probe run **after** the open, so RocksDB has already replayed its WAL |
| FM-PERSISTENCE-030 | `reconcile` compares persisted vs configured CF layout **before any store is built** (`ShardCountMismatch`) |
| FM-PERSISTENCE-031 | Warm-tier reopen guard; adding CFs safe, dropping them from the open set not (`WarmTierMismatch`) |
| FM-PERSISTENCE-032 | A failed `list_cf` **propagates**; never coerced into an empty list / fresh-open path |
| FM-PERSISTENCE-034 | `DBRecoveryMode::PointInTime` set explicitly at open, "pinned by a test" — see the acceptance criterion below, which is where this row is not what it looks like |
| FM-PERSISTENCE-035 | `detect_and_reset` compares the watermark against `latest_sequence_number()` **at open**; fresh open seeds instead |
| FM-PERSISTENCE-041 | Names `RocksStore::open` in prose (spec:696) — the second occurrence, same naming argument as 024 |

**Acceptance criterion.** The statement order inside the open body is preserved
exactly — `db_opts` → `set_wal_recovery_mode(PointInTime)` → ratelimiter →
block/CF options → `db_exists` probe → `list_cf` (propagating) →
`ColumnFamilyManifest::reconcile` → `open_cf_descriptors` → create-missing-CF loop
→ `wal_watermark::detect_and_reset` (existing) / `write` (fresh) → struct
construction. This is a **signature-only** change: `mod.rs:153–311` moves
character-for-character, with `path`/`num_shards`/`config`/`warm_enabled`/`metrics`
read off `opts` instead of positional parameters. No FM row is edited and **no
forcing test *assertion* changes** — two forcing tests do change, but only in how
they spell the constructor:

- `test_cf_enumeration_failure_propagates_and_preserves_data` (FM-032,
  tests.rs:374) changes **signature**: six positional arguments become
  `(RocksOpenOptions, lister)`. Its three assertions are byte-identical.
- The FM-031 tests change **spelling**: `RocksStore::open_with_warm(p, n, &cfg,
  true)` becomes `RocksOpenOptions::new(p, n, &cfg).warm(true).open()`. Assertions
  unchanged.

`just lint-failure-modes` must pass unchanged (no tag moves, no row edits), and
`just mutants-diff frogdb-persistence` is required before push (gate 0.85 for the
persistence pair; `frogdb-persistence` sat at 99.1% at the lock).

**The FM-034 pin is not actually forced today, and this move must not inherit
that.** `wal_recovery_mode_is_pinned_to_point_in_time` (tests.rs:1312–1318) is
listed in FM-034's Forced-by and its Invariant says the mode is "pinned by a
test". The test body is:

```rust
let pinned = rocksdb::DBRecoveryMode::PointInTime;
assert!(matches!(pinned, rocksdb::DBRecoveryMode::PointInTime));
```

That asserts a local constant against itself. Deleting
`db_opts.set_wal_recovery_mode(DBRecoveryMode::PointInTime)` at mod.rs:188 fails
**no test in the workspace** — and cannot be caught behaviourally either, because
`PointInTime` is also RocksDB's library default, which is precisely the silent
drift the row exists to prevent. So the criterion "the pin survives the move" is
currently unverifiable by the suite.

Requirement on this proposal: the pin must end up with a **real forcing
assertion**, not the tautology. The recommended shape falls out of part A — make
the recovery mode a `RocksOpenOptions` field defaulted to `PointInTime` and assert
the default on the constructed options
(`assert_eq!(RocksOpenOptions::new(p, 1, &cfg).recovery_mode(), DBRecoveryMode::PointInTime)`),
which is an assertion against real, mutable code that a mutant can flip. The
alternative is to accept that the row is behaviourally unforceable and say so *at
the code*, per the locked-area rule on unkillable mutants.

**Sequencing consequence:** either remedy edits FM-034's `Forced by` line (and,
for the second option, its Invariant prose) in a LOCKED spec. That makes it a
**spec-first change (row → failing test → fix), landed separately from and before
this refactor** — 44 itself stays a pure refactor with no spec edit. Do not fold
the two into one commit.

- **One test's intent is about the shims themselves.**
  `noop_default_shim_reclaims_without_panicking` (tests.rs:991) is named and
  documented for "the `open`/`open_with_warm` shims default to an *explicit*
  `NoopMetricsRecorder`". It is **not** FM-tagged, so no spec churn, but its name
  and doc must be updated to say "the default `RocksOpenOptions` metrics recorder"
  — the property it guards (a Store opened without a recorder still reclaims
  soundly) survives verbatim.
- **What the metrics policy actually is, and what must not regress.** The policy
  round-10 proposal 24 established is *no post-construction setter*: the recorder
  is injected at construction and stored immutably, so a `RocksStore` cannot exist
  in a "no-metrics window" that a later install closes (24 deleted exactly such a
  `set_metrics_recorder` slot). `open` today already supplies a `Noop` sink
  internally (mod.rs:84) — it is not caller-supplied — so a defaulted builder
  field is the *same* policy, not a weakening of it. What this proposal must
  preserve is only that: `RocksOpenOptions::open` always hands the constructor a
  recorder, the `RocksStore::metrics` field stays private and immutable, and **no
  post-construction setter is reintroduced**. Document the default on
  `RocksOpenOptions::new` in the same terms the current `open` doc uses.
- **`RocksOpenOptions` must not become a second config type.** It carries the
  *arguments the open path cannot read off `RocksConfig`* — nothing that belongs
  in `RocksConfig::from_persistence` (round-10 proposal 22) may migrate into it.
- **ADR-0003 friction: none.** The ADR pins the recovery seam, `SnapshotFs`, write
  groups and the WAL watermark; it says nothing about `RocksStore`'s constructor
  shape, and the watermark call stays where it is in the open body.
- **B2 diff size.** The ~226 call-expression estimate is the whole cost of
  `TestKv`. Land B1 first; treat B2 as a separate commit that can be dropped
  without losing the readability win.

### Explicit non-goal: `columns.rs` tier shims

`columns.rs:120–146` (`put_warm`/`get_warm`/`delete_warm`/`iter_warm_cf`,
`put_search_meta`/`get_search_meta`/`delete_search_meta`/`iter_search_meta`) are
per-tier shims over the same `tier_cf_handle` + `put_tier`/`get_tier`/`delete_tier`
/`iter_tier` set. They were examined and are **deliberately kept** (see the module
doc, columns.rs:1–9: "kept because callers are numerous"). This proposal does not
touch `columns.rs`, does not gate the tier ops, and does not fold the warm/search
shims — including the finding in §4, which is recorded for the record only. Any
change there is a separate proposal with its own justification.

### Boundaries vs sibling proposals (file-level)

| Sibling | Owns | Overlap rule |
|---|---|---|
| 38 snapshot-layout-fs-read | `snapshot/rocks_coordinator.rs`, `snapshot/stager.rs`, `snapshot/fs_seam.rs` | None. 44 touches no `snapshot/` file. |
| 40 frame-codec-unify | `wal/mod.rs` (frame header), `probabilistic.rs`, `dataset.rs` | None. |
| 41 persistence-small-dedups | `wal/writer.rs` (WalSink delegations, `committed_sequence`), `wal/flush.rs:559–678`, the deletion-test kills | None in `rocks/`. Note `committed_sequence` on `DurableSyncTarget` (`rocks/mod.rs:37`) is the **trait** method, not the `WalWriter` inherent one 41 deletes — 44 must not touch the trait, 41 must not touch `rocks/mod.rs`. |
| 42 rockssink-commit-effects | `wal/flush.rs:181–211` (`CommitEffects`) | Reads `RocksStore::spawn_clear_reclamation` / `record_wal_watermark`; 44 changes neither signature. Sequence 42 last per lane note. |
| 39 recovery-replay-driver | `core/src/persistence/store_recovery.rs`, `persistence/src/recovery.rs`, `core/src/persistence/mod.rs`, core's recovery tests — **and one line of `recovery/src/shards.rs`** | Two contacts. (a) Both files 39 owns hold `RocksStore::open*` **test** call sites (store_recovery.rs 143–280, recovery.rs 306/567/606); all are default-`open` or `open_with_warm`, and 44's migration list takes only the three `open_with_warm` sites in `recovery.rs`. (b) **`recovery/src/shards.rs` — see the note below.** |
| 43 recovery-wrappers-staged-checkpoint | `recovery/src/{functions,tests}.rs`, `persistence/src/rocks/{staged,tests}.rs`, `scripting/src/persistence.rs`, `server/src/function_store.rs`, `types/src/metrics/definitions.rs` (Option A) | **No file overlap with 44.** 43 explicitly does **not** edit `recovery/src/shards.rs` — its files table (43:41) marks the file "Owned by proposal 39's boundary; not edited here", and its Part 1 ruling ("do not fold" the phase wrappers) is what keeps it out of the diff (43:541–543). 43 also does not touch `rocks/mod.rs`. The two *do* share the LOCKED spec file — 43 amends FM-037, 44 edits no row — and both re-gate `frogdb-persistence`, so whichever lands second re-runs `just mutants-diff`. |

**Coordination note — `recovery/src/shards.rs` has two editors, not one.** An
earlier draft called 44 the sole editor of this file; that is no longer true.
Reading 39's current on-disk text: its files table (39:47) states that
`shards.rs` **line 56 changes** — the `duration_ms = stats.duration_ms` binding in
the "Recovery complete" `info!` is replaced by a local `Instant` elapsed as a
required part of 39's `duration_ms` deletion (39:269, 39:483), and 39 budgets a
`just mutants-diff frogdb-recovery` for it (39:444). 43's current text agrees
(43:538 lists that line under 39's ownership; 43:541–543 confirms 43 itself leaves
the file untouched).

44 edits lines **29–35** of the same file (the `open_with_warm_metrics` call in
`open_rocks`); 39 edits line **56** (inside `restore`). Different functions, ~20
lines apart, no shared token — git merges them in either order. **Order is free**;
if 39 lands first, 44 rebases with no textual conflict. The one rule: neither
proposal may reformat the other's function while both are in flight, and both
must re-run `just mutants-diff frogdb-recovery` (the crate carries its own 0.85
gate) since each touches it.

## Effort estimate

**M.** Part A is a signature-only refactor of one function plus a small options
type: the 160-line body moves verbatim, 36 non-default call sites migrate
mechanically, 137 default sites are untouched, and one test's name/doc is
corrected. Part B1 is a file move behind a single `cfg` gate, with two nested
`#[cfg(test)]` attributes retained and one gated re-export added (S). What makes
the whole M rather than S is the LOCKED-crate discipline rather than the code:
nine failure-mode rows pin or name this path, so the change needs the ordering
diff read line-by-line, `just lint-failure-modes` clean, and a `just mutants-diff
frogdb-persistence` run (gate 0.85) before push — plus the separately-sequenced
spec-first fix for FM-034's vacuous pin test. Part B2 (`TestKv`) adds an estimated
~226 compiler-driven call-site edits across ~13 files and 5 crates — real diff
volume, no design risk; land it separately or defer it.

**No live bug found; no independently-landable hotfix.** The nearest
independently-landable slice is **B1** (relocate the gated items behind one
`mod` gate): zero call-site churn, no behaviour change, no FM row touched, and it
delivers most of the `rocks/mod.rs` readability win on its own. The **FM-034
vacuous-pin fix** is separately landable too, and should go first since it is
spec-first work this refactor would otherwise silently inherit.
