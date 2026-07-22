# Proposal 24 — Inject the metrics recorder into `RocksStore` at construction; delete the mutable install slot

## Summary

`RocksStore` is born holding a `Mutex<Arc<dyn MetricsRecorder>>` that defaults to
`NoopMetricsRecorder`; the real recorder is bolted on later via a separate `set_metrics_recorder`
call at server startup. The doc comment on the field asserts the real recorder is installed "before
any client command can run," but *nothing* enforces that ordering — it is an unenforced two-phase-init
invariant kept alive by the position of one line in `Server::with_listeners`. Store-initiated
background work (post-clear space reclamation) reads the recorder on every pass; any Store constructed
without the install (every test, every future caller, any startup reorder) silently counts into the
noop. This proposal replaces the install seam with **constructor injection**: the recorder becomes a
required, immutable field supplied at `open`, the mutable slot and `set_metrics_recorder` are deleted,
and the "no-metrics window" cannot exist by construction.

The one verified correction to the exploration note: **checkpoint-restore is not a `RocksStore` open
site.** `load_staged_checkpoint` (`rocks/checkpoint.rs:28`) is a pair of filesystem renames; the
restored directory is picked up by the *normal* `open_with_warm` on the next boot, so there is exactly
**one** production open site to thread the recorder through, not several.

## Problem

`RocksStore` uses two-phase initialization for its metrics recorder:

1. **Phase 1 — construction with a noop default.** Every `open*` path ends at
   `open_with_cf_lister`, which builds the struct with
   `metrics: std::sync::Mutex::new(Arc::new(NoopMetricsRecorder))` (`rocks/mod.rs:193`).
2. **Phase 2 — late install.** Server startup calls `rocks.set_metrics_recorder(...)` once
   (`server/mod.rs:244`), overwriting the slot.

The field's own doc comment states the contract (`rocks/mod.rs:39-42`):

> Starts as a no-op recorder; server startup installs the real one via
> `RocksStore::set_metrics_recorder` before any client command can run.

That is an **invariant expressed in prose, enforced by nothing**. It holds today only because of where
the install line sits (`server/mod.rs:240-245`: "Installed before shard workers spawn, so no client
command can race it"). The compiler cannot hold any caller to it. A Store opened anywhere else —
today that is every test and benchmark, tomorrow any new production open path — is a fully-functional
Store whose reclamation counters are silently wired to `/dev/null`.

The consumer that reads through this slot is the post-clear reclamation pass (`rocks/reclaim.rs:160`):

```rust
let metrics = rocks.metrics_recorder();
FlushCompactStarted::inc(&*metrics, &shard_label);   // reclaim.rs:161
// ... DeleteFilesInRange + forced bottommost CompactRange ...
FlushCompactCompleted::inc(&*metrics, &shard_label); // reclaim.rs:187
```

The reclamation pass is triggered by exactly two data-path sites, both FLUSHDB/FLUSHALL-class clears:

- `wal/flush.rs:166` + `wal/flush.rs:190` — the FrogDB WAL flush engine applying a Main-tier clear.
- `frogdb-server/crates/core/src/store/warm_tier.rs:133` — a Warm Tier clear.

**Honest scope.** In production the window is currently *not reachable*: both triggers are data-path
operations that only run while serving client commands, which is after shard workers spawn, which is
after the install line at `server/mod.rs:244`. So this is not a live counter-loss bug — it is a
**latent structural fragility**: a robustness/clarity deepening, not a fix. The value in closing it is
that (a) the invariant currently survives on line ordering that any refactor of `with_listeners` can
break with no compiler signal, and (b) the two-phase seam is itself the defect — a mutable
`Mutex`-guarded slot that is written exactly once and only exists so the recorder can arrive late.

Two secondary smells fall out of the same seam:

- **A `Mutex` guarding a write-once value.** After the single `set_metrics_recorder`, the slot never
  changes, yet every reclamation pass takes `self.metrics.lock().unwrap()` (`rocks/mod.rs:403`) to
  read it. The lock protects a value that is immutable in practice.
- **Tests mirror the fragile ordering.** `rocks/tests.rs:857` and `rocks/tests.rs:889` each hand-call
  `set_metrics_recorder` immediately after `open`, reproducing the exact two-step dance the production
  code uses — the test scaffolding has to know the invariant the type refuses to enforce.

## Evidence (verified file:line)

| Claim | Location | Status |
| --- | --- | --- |
| Field is `Mutex<Arc<dyn MetricsRecorder>>` | `frogdb-server/crates/persistence/src/rocks/mod.rs:43` | verified |
| Doc comment asserts unenforced "before any client command" contract | `frogdb-server/crates/persistence/src/rocks/mod.rs:39-42` | verified |
| Constructed defaulting to `NoopMetricsRecorder` | `frogdb-server/crates/persistence/src/rocks/mod.rs:193` | verified |
| `set_metrics_recorder` (the install seam) | `frogdb-server/crates/persistence/src/rocks/mod.rs:396-398` | verified |
| `metrics_recorder()` reader locks the mutex | `frogdb-server/crates/persistence/src/rocks/mod.rs:402-404` | verified |
| Sole production install, positioned before shard workers | `frogdb-server/crates/server/src/server/mod.rs:243-244` | verified |
| Counters read the recorder per pass | `frogdb-server/crates/persistence/src/rocks/reclaim.rs:160,161,187` | verified |
| Trigger: Main-tier clear via FrogDB WAL flush | `frogdb-server/crates/persistence/src/wal/flush.rs:166,190` | verified |
| Trigger: Warm Tier clear | `frogdb-server/crates/core/src/store/warm_tier.rs:133` | verified |
| Tests hand-call `set_metrics_recorder` after open | `frogdb-server/crates/persistence/src/rocks/tests.rs:857,889` | verified |
| **Only one production `open*` site** | `frogdb-server/crates/server/src/recovery/shards.rs:41` (`open_with_warm`) | verified |
| Checkpoint-restore is a rename, **not** an open | `frogdb-server/crates/persistence/src/rocks/checkpoint.rs:28-101` | verified — corrects candidate |
| Recorder exists *before* the Store opens in init | recorder `frogdb-server/crates/server/src/server/init.rs:109`; recovery/open `init.rs:238` | verified — injection is feasible |
| `MetricsRecorder` lives in `frogdb-types`; persistence already depends on it | `frogdb-server/crates/types/src/traits/metrics.rs:10,80`; `frogdb-server/crates/persistence/Cargo.toml:10` | verified — no new crate coupling |
| All other `open*` calls are tests/benches (incl. `replica_session.rs:1356,1451`, both `#[tokio::test]`) | workspace grep | verified |

### Discrepancies from the exploration candidate

- **"checkpoint-restore" listed as an open site to enumerate.** False. `load_staged_checkpoint`
  performs `std::fs::rename` only (`checkpoint.rs:84,87`); it never constructs a `RocksStore`. The
  restored dir is opened by the ordinary `open_with_warm` on the following boot. There is one
  production open path, `recovery/shards.rs:41`.
- **"silently hit the noop if reclamation races install or startup reorders."** Directionally correct
  but overstated as a live hazard: the two reclamation triggers are gated behind shard-worker spawn,
  which is after the install line, so the race is not presently reachable in production. The proposal
  is framed as closing an unenforced invariant, not fixing an active counter-loss.

## Proposed design (Rust interface sketch)

Make the recorder a construction-time dependency of the Store, stored immutably. The `open*` surface
splits into an explicit-recorder production constructor and Noop-defaulting shims for tooling/tests, so
a Store can never exist in a "recorder not yet installed" state.

```rust
use frogdb_types::traits::{MetricsRecorder, NoopMetricsRecorder};
use std::sync::Arc;

pub struct RocksStore {
    // ... unchanged fields ...
    // was: metrics: std::sync::Mutex<Arc<dyn MetricsRecorder>>
    metrics: Arc<dyn MetricsRecorder>,   // immutable; set once at construction
}

impl RocksStore {
    /// Production entry point: caller supplies the recorder up front.
    pub fn open_with_warm_metrics(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
        metrics: Arc<dyn MetricsRecorder>,
    ) -> Result<Self, RocksError>;

    /// Convenience shims — an *explicit* Noop, not a hidden default that
    /// "should" be overwritten later. Used by tests, benches, and tools that
    /// legitimately want no metrics.
    pub fn open(path: &Path, num_shards: usize, config: &RocksConfig) -> Result<Self, RocksError> {
        Self::open_with_warm_metrics(path, num_shards, config, false, Arc::new(NoopMetricsRecorder))
    }
    pub fn open_with_warm(
        path: &Path, num_shards: usize, config: &RocksConfig, warm_enabled: bool,
    ) -> Result<Self, RocksError> {
        Self::open_with_warm_metrics(path, num_shards, config, warm_enabled, Arc::new(NoopMetricsRecorder))
    }

    // The canonical seam gains the recorder and stores it directly.
    fn open_with_cf_lister(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
        metrics: Arc<dyn MetricsRecorder>,
        list_cf: impl FnOnce(&Options, &Path) -> Result<Vec<String>, RocksError>,
    ) -> Result<Self, RocksError>;

    // Reader loses the lock; the field is already an Arc.
    pub(crate) fn metrics_recorder(&self) -> Arc<dyn MetricsRecorder> {
        Arc::clone(&self.metrics)
    }

    // DELETED: pub fn set_metrics_recorder(&self, recorder: Arc<dyn MetricsRecorder>)
}
```

Production wiring threads the already-constructed recorder through the recovery seam:

```rust
// frogdb-server/crates/server/src/recovery/mod.rs — RecoveryInputs gains one field:
pub struct RecoveryInputs<'a> {
    // ...
    pub metrics_recorder: Arc<dyn MetricsRecorder>,
}

// frogdb-server/crates/server/src/recovery/shards.rs::open_rocks — pass it through:
let rocks = Arc::new(RocksStore::open_with_warm_metrics(
    &config.data_dir, inputs.num_shards, &rocks_config, inputs.warm_enabled,
    inputs.metrics_recorder.clone(),
)?);
```

`server/mod.rs:243-245` (the `set_metrics_recorder` install block) is deleted — the recorder is
injected at open, and `init.rs:238` already constructs the recorder (`init.rs:109`) before recovery
runs, so nothing needs reordering.

**Locality / depth.** The recorder becomes part of the Store's construction contract — the one place
that already owns "what does this Store need to exist." The mutable slot, the lock on the read path,
and the separate install call all disappear; the interface narrows (one fewer public method) while the
guarantee strengthens (a Store always has a recorder). This is the classic deep-module move: push the
dependency into the constructor so no caller can observe a half-initialized object.

### Design alternative considered — mandatory recorder on every constructor

Drop the Noop shims and force *every* `open*` caller (~100 test/bench sites) to pass an explicit
recorder. This is the purist "no default can drift" form, but it is a large mechanical churn for zero
production benefit over the shim approach (tests genuinely want Noop), and the shims already make the
Noop choice *explicit at the call site* rather than a hidden field default. Recommended: keep the
shims. If the team prefers zero defaults, the churn is `sed`-able but should be a separate pass.

## Migration plan (ordered)

1. **Field + canonical constructor.** Change `metrics` to `Arc<dyn MetricsRecorder>`; add the
   `metrics` parameter to `open_with_cf_lister` and store it directly (drop the `Mutex`).
2. **Public surface.** Add `open_with_warm_metrics`; rewrite `open`/`open_with_warm` as shims passing
   `Arc::new(NoopMetricsRecorder)`; delete `set_metrics_recorder`; drop the `.lock()` in
   `metrics_recorder()`.
3. **Production wiring.** Add `metrics_recorder: Arc<dyn MetricsRecorder>` to `RecoveryInputs`
   (`frogdb-server/crates/server/src/recovery/mod.rs`). Because it is a required (non-defaulted)
   field, this forces two changes the field addition triggers:
   - **`RecoveryInputs::from_config` signature grows a param.** `from_config(config, num_shards)`
     (`recovery/mod.rs:65`) currently derives *every* field from `config`, but the recorder is
     constructed separately at `init.rs:109` — so `from_config` must become
     `from_config(config, num_shards, metrics_recorder)` and store the passed `Arc`. Update its sole
     production call site at `init.rs:238` to thread `metrics_recorder.clone()` in.
   - **11 test struct-literal sites in `recovery/tests.rs`** (lines 61, 94, 123, 157, 180, 206, 243,
     273, 305, 333, 383 — each `let inputs = RecoveryInputs { … }`). Rust struct literals cannot omit
     a non-defaulted field, so every one must add `metrics_recorder: Arc::new(NoopMetricsRecorder…)`
     or the crate won't compile after this step. These are the largest single block of churn.

   Then pass the field through in `open_rocks` (`recovery/shards.rs:41`) and delete the install block
   at `server/mod.rs:243-245`.
4. **Tests that install a recorder in `persistence`.** `rocks/tests.rs:855-857` and
   `rocks/tests.rs:887-889` switch from `open* ...; s.set_metrics_recorder(recorder)` to a single
   `open_with_warm_metrics(... , recorder)` (the 887 site uses `open` with a custom `config`, so it
   needs the non-warm `open_with_cf_lister` path or an added `open_with_metrics` non-warm shim — see
   Risks). No behavioral change to the assertions.
5. **`cargo check` drives the rest.** Every other `open`/`open_with_warm` caller is unchanged (they
   keep the Noop shim). Confirm no remaining reference to `set_metrics_recorder`.

## Test plan

- **Existing counter tests stay green, adapted to injection.**
  `warm_clear_tier_shard_reclaims_and_counts` (`rocks/tests.rs:852`) and the Main-tier counter test at
  `rocks/tests.rs:887` construct the Store *with* the `CountingRecorder` and assert
  `frogdb_flush_compact_{started,completed}_total == 1` exactly as today — proving the injected
  recorder reaches `run_reclamation`.
- **New: the shims default to Noop, verifiably.** A unit test that opens via `open`/`open_with_warm`,
  runs `run_reclamation`, and asserts it does not panic and increments nothing observable — pinning
  that the explicit-Noop default is intentional and safe.
- **Regression guard by construction, not by test.** The deleted `set_metrics_recorder` means "forgot
  to install the recorder" is no longer expressible — the type requires a recorder at `open`. No test
  is needed to guard an unreachable state; the compiler does it.
- **Reclaim guard/coalesce tests unaffected** (`reclaim.rs:200-240`): they never touch the recorder.
- Run `just test frogdb-persistence` (rocks + reclaim + warm-tier counter tests) and
  `just test frogdb-server` (startup/recovery wiring). Full-suite build on the Blacksmith testbox per
  repo policy.

## Risks & alternatives

- **Non-warm custom-config test site (`rocks/tests.rs:887`).** It uses `open` with a bespoke
  `RocksConfig` and then installs a recorder. The warm-only `open_with_warm_metrics` doesn't fit
  directly; either route it through a `warm_enabled: false` call to `open_with_warm_metrics`, or add a
  symmetric non-warm `open_with_metrics(path, num_shards, config, metrics)` shim. Prefer the latter for
  symmetry with `open`. Minor.
- **`RecoveryInputs` signature growth.** Adds one required field carried through the recovery
  orchestrator. Because the field is non-defaulted, it forces `from_config` to grow a
  `metrics_recorder` param (it can no longer derive every field from `config`) and forces all 11
  `RecoveryInputs { … }` construction sites in `recovery/tests.rs` to add the field — all
  compiler-guided, mechanical churn, not a design change. It is already the struct that assembles
  everything the open needs, so the field itself is additive, not a new seam.
- **Crate dependency direction is respected.** `MetricsRecorder`/`NoopMetricsRecorder` live in
  `frogdb-types` (`frogdb-server/crates/types/src/traits/metrics.rs:10,80`); `persistence` already depends on `frogdb-types`
  (`frogdb-server/crates/persistence/Cargo.toml:10`) and already names these types today. `server` already holds the
  recorder. No crate gains a new dependency; nothing reaches into `core` internals.
- **`Mutex` removal safety.** The slot's only writer was `set_metrics_recorder` (deleted); the only
  reader is `reclaim.rs:160`. Making the field an immutable `Arc` is sound and removes a lock from the
  reclamation path (a negligible but free win). Confirmed no other `self.metrics` mutation exists.
- **Off the Raft path.** Reclamation and its counters are store-initiated background work, not part of
  the data path and not routed through the Raft Metadata Plane; ADR-0001 (Raft owns metadata, data
  path never through Raft) is untouched. No persistence ADR governs the recorder install.
- **Alternative: keep the slot, add a debug-assert.** One could keep two-phase init and add a
  `debug_assert!` that the recorder was installed before the first reclamation. Rejected: it detects
  the violation at runtime in debug builds only, still permits the shallow mutable slot and the lock,
  and does nothing for the test scaffolding smell. Constructor injection removes the failure mode
  instead of instrumenting it.

## Effort

**M.** One field/constructor change plus deletions in `persistence`, and one required field added to
`RecoveryInputs` in `server`. The `RecoveryInputs` change is compiler-guided but wider than a single
edit: it forces the `from_config` signature to grow a `metrics_recorder` param (updated at its lone
production call `init.rs:238`) and touches all 11 `RecoveryInputs { … }` construction sites in
`recovery/tests.rs`, on top of the ~2 `set_metrics_recorder` test adaptations in `rocks/tests.rs` — so
roughly **15 test/wiring sites**, not 2. Every one is mechanical (add one field / one arg); none
change assertions or design. The shim approach keeps all ~100 other `RocksStore::open*` callers
untouched, so the persistence-side blast radius stays small. The bulk of the effort is the mechanical
`RecoveryInputs` field/param threading, which is why this is M rather than S.

## Related

None.

## Adversarial review

**Verdict: AMEND** — premise sound and core claims unrefuted; two minor defects corrected.

- **Issue 1 (minor) — migration/effort undercount of `RecoveryInputs` churn.** *Confirmed and
  fixed.* Making `metrics_recorder` a required field on `RecoveryInputs` cannot be absorbed by the
  "~2 test adaptations" the plan claimed. Verified in-repo: 11 `let inputs = RecoveryInputs { … }`
  struct-literal sites in `frogdb-server/crates/server/src/recovery/tests.rs` (lines 61, 94, 123,
  157, 180, 206, 243, 273, 305, 333, 383 — `grep -c` = 11), each of which must add the field or the
  crate won't compile (Rust struct literals can't omit a non-defaulted field). Additionally
  `RecoveryInputs::from_config(config, num_shards)` (`recovery/mod.rs:65`) derives every field from
  `config`, but the recorder is built separately at `init.rs:109`, so its signature must grow a
  `metrics_recorder` param (call site `init.rs:238`). Resolution: migration-plan step 3 now spells
  out the `from_config` signature change and the 11 test literals; the `RecoveryInputs` risk bullet
  and the Effort section were rewritten (S–M → M, "~2 sites" → "roughly 15 test/wiring sites"). The
  constructor-injection design is unaffected — the added churn is entirely mechanical and
  compiler-guided.
- **Issue 2 (minor) — citation path prefixes.** *Confirmed and fixed.* The crates live under
  `frogdb-server/crates/`, so evidence-table paths like `persistence/src/rocks/mod.rs:43` were
  missing that prefix. Crate-relative portions and all line numbers were correct. Resolution: every
  full-path citation now carries the `frogdb-server/crates/<crate>/src/...` prefix (evidence table,
  code-comment references, risk bullets). Inline crate-relative shorthands (e.g. `rocks/mod.rs:193`,
  `init.rs:238`) were left as-is by design.

Reviewer's substantive notes (single production open site at `recovery/shards.rs:41`; write-once
`Mutex` safe to remove; recorder built at `init.rs:109` before recovery at `init.rs:238`; checkpoint
restore is `fs::rename` only, not an open; no ADR/round-1–9 conflict; honest latent-fragility framing)
were all confirmed accurate and required no change. The injection design itself passed review
unchanged.
