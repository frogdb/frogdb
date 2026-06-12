# Proposal: Recovery Orchestrator

Status: proposed
Date: 2026-06-12

## Problem

The startup/crash-recovery sequence has no owning module. It is smeared across four files in three
crates, plus a fourth recovery site that runs at shard-spawn time:

- `frogdb-server/crates/server/src/server/startup.rs:47-139` — `init_persistence()` installs staged
  checkpoints, opens RocksDB, and triggers shard restore
- `frogdb-server/crates/core/src/persistence/recovery.rs:53-211` — RocksDB iteration,
  deserialization, expiry filtering; rebuilds per-shard hash tables and expiry indexes
- `frogdb-server/crates/persistence/src/rocks/checkpoint.rs:26-55` — staged checkpoint
  install (filesystem rename surgery on the data dir, *before* the DB can be opened)
- `frogdb-server/crates/server/src/server/shards.rs:191-283` — search index recovery, inlined into
  worker spawning

No module's interface states the recovery-order invariant: **staged checkpoint install → open
RocksDB → restore shards → restore replication state → restore cluster state**. The ordering exists
only as the incidental top-to-bottom layout of `Server::with_listeners()`
(`frogdb-server/crates/server/src/server/mod.rs:209-271`) and the helpers it calls. The friction
this causes:

- **A new recovery step has no obvious home.** Evidence: function-library restore
  (`functions.fdb`) ended up inlined in the middle of infrastructure init
  (`frogdb-server/crates/server/src/server/init.rs:280-307`), between slowlog counter creation and
  TLS setup, because nothing said "recovery steps go here."
- **The interface is shallow.** `init_persistence()` returns a bare 3-tuple
  (`PersistenceInitResult`, `frogdb-server/crates/server/src/server/util.rs:33-37`); every caller
  re-learns what the tuple positions mean and what must happen before/after.
- **No test seam.** The full sequence is only exercised through a complete server boot
  (`Server::with_listeners`). `checkpoint.rs` — the code that renames a live database directory out
  of the way — has **zero tests**. Existing crash-recovery tests
  (`frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs`) cover WAL/RocksDB
  durability per shard, but nothing covers staged-checkpoint install ordering, replication-state
  restore, or cluster-state restore. The highest-stakes path in a database has no seam.
- **Recovery outputs leak silently.** `shards.rs:60` does
  `recovered_iter.next().unwrap_or_default()` — a shard-count mismatch (e.g. resharding a data dir)
  silently drops recovered data instead of failing recovery.

Related correctness flag (separate fix, out of scope here): the replication offset is never
persisted after startup. `ReplicationTrackerImpl` only updates an in-memory `AtomicU64`
(`frogdb-server/crates/replication/src/tracker.rs:128-140`); `ReplicationState::save()`
(`frogdb-server/crates/replication/src/state.rs:119`) has no callers after `load_or_create()`.
Relatedly, the replica full-sync path stages `replication_metadata.json` inside the checkpoint
(`frogdb-server/crates/replication/src/replica/connection.rs:302`) but **no boot code ever reads
it**. The orchestrator must leave room for "restore replication offset from staged metadata" as a
phase; the persistence bug itself is not fixed by this proposal.

## Current state

The actual end-to-end recovery sequence today, mapped step by step:

| # | Step | Where it lives | Crate |
|---|------|----------------|-------|
| 1 | Ensure data dir exists | `server/src/server/startup.rs:63` | frogdb-server |
| 2 | Install staged full-sync checkpoint (`checkpoint_ready/` → rename old db aside → rename checkpoint in) | `server/src/server/startup.rs:66-77` calling `persistence/src/rocks/checkpoint.rs:26-55`; staged by `replication/src/replica/connection.rs:288-307` | frogdb-server → frogdb-persistence (staged by frogdb-replication) |
| 3 | Open RocksDB (+ optional warm-tier CFs) | `server/src/server/startup.rs:80-104` | frogdb-server → frogdb-persistence |
| 4 | Restore per-shard hash tables, expiry indexes, warm-tier entries | `server/src/server/startup.rs:107-121` calling `core/src/persistence/recovery.rs:173-211` (`recover_all_shards` → `recover_shard:53`, `recover_warm_shard:120`) | frogdb-core |
| 5 | Spawn periodic WAL sync task (runtime concern, interleaved) | `server/src/server/startup.rs:124-136` | frogdb-server |
| 6 | Create snapshot coordinator (runtime concern, interleaved) | `server/src/server/init.rs:188-224` | frogdb-server |
| 7 | Restore function libraries from `functions.fdb` | `server/src/server/init.rs:280-307` | frogdb-server → frogdb-core |
| 8 | Restore replication state (id + offset) from state file; seed tracker offset | `server/src/server/mod.rs:218` → `server/src/server/replication_init.rs:50-60` (primary) / `:98-117` (replica), calling `replication/src/state.rs:75-116` | frogdb-server → frogdb-replication |
| 9 | Open Raft storage; replay Raft log into `ClusterStateMachine`; bootstrap/slot-assign | `server/src/server/mod.rs:228` → `server/src/server/cluster_init.rs:63-65` (storage), `:178-186` (Raft init/replay), `:191-284` (bootstrap) | frogdb-server → frogdb-cluster (via core re-export) |
| 10 | Hand recovered stores to shard workers (`unwrap_or_default()` on mismatch) | `server/src/server/mod.rs:243-271` → `server/src/server/shards.rs:52-60` | frogdb-server |
| 11 | Restore search indexes, aliases, dictionaries from RocksDB search-meta CF | `server/src/server/shards.rs:172-177`, `:191-283` | frogdb-server → frogdb-search |

Two real excerpts showing the cross-crate coordination. First, the staged-checkpoint install: a
path-level invariant (must run before step 3 opens the same directory) enforced only by statement
order inside a helper whose name says nothing about checkpoints:

```rust
// frogdb-server/crates/server/src/server/startup.rs:65-77 (inside init_persistence)
    // Check for and load staged checkpoint from replica full sync
    match RocksStore::load_staged_checkpoint(&config.data_dir) {
        Ok(true) => {
            info!("Loaded staged checkpoint from replica full sync");
        }
        Ok(false) => {
            // No checkpoint to load, continue normally
        }
        Err(e) => {
            error!(error = %e, "Failed to load staged checkpoint");
            return Err(anyhow::anyhow!("Failed to load staged checkpoint: {}", e));
        }
    }
```

The `Ok(true)` result is discarded after logging — the replication phase never learns that a staged
checkpoint (with its own `replication_metadata.json`) was just installed.

Second, replication-state restore buried inside handler wiring — a recovery step (read persisted
state, seed in-memory offset) fused with component construction:

```rust
// frogdb-server/crates/server/src/server/replication_init.rs:45-60 (inside init_replication)
        // Initialize PrimaryReplicationHandler for primary role
        let state_path = config
            .persistence
            .data_dir
            .join(&config.replication.state_file);
        let repl_state = frogdb_core::ReplicationState::load_or_create(&state_path)
            .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;
        ...
        let tracker = Arc::new(frogdb_core::ReplicationTrackerImpl::new());
        tracker.set_offset(repl_state.replication_offset);
```

The same `load_or_create` call is duplicated for the replica role at `replication_init.rs:102-103`.

Today the recovery sequence **fails the deletion test**: there is nothing you could delete to
remove "crash recovery" — it is interleaved with metrics setup, TLS, channel creation, and task
spawning across `init.rs`, `mod.rs`, `replication_init.rs`, `cluster_init.rs`, and `shards.rs`.

## Proposed design

One recovery orchestration module in the server crate — the only crate that already depends on
core, persistence, replication, and cluster, so it is the natural place to state the cross-crate
ordering invariant. The module consumes the existing core/persistence/replication interfaces; those
crates do not change.

```text
frogdb-server/crates/server/src/recovery/
├── mod.rs        # public seam: RecoveryInputs → recover() → RecoveredState
├── checkpoint.rs # phase 1: install staged checkpoint
├── shards.rs     # phases 2-3: open RocksDB, restore shard stores
├── functions.rs  # phase 4: functions.fdb
├── replication.rs# phase 5: replication state file
└── cluster.rs    # phase 6: Raft storage open
```

The interface is deliberately deep: a narrow seam (one function, plain-data inputs and outputs)
over six ordered phases of internals. Inputs are config + data dir; outputs are recovered state.
No live components, no listeners, no spawned tasks — wiring stays in `init.rs`/`mod.rs`.

```rust
// frogdb-server/crates/server/src/recovery/mod.rs

/// What recovery reads. Pure data — no sockets, channels, or running components.
pub struct RecoveryInputs<'a> {
    pub data_dir: &'a Path,
    pub persistence: &'a PersistenceConfig,
    pub replication: &'a ReplicationConfig,
    pub cluster: &'a ClusterConfig,
    pub num_shards: usize,
    pub warm_enabled: bool,
}

/// What recovery produces. Opened handles + plain data; component wiring happens later.
pub struct RecoveredState {
    /// Open store; `None` when persistence is disabled.
    pub rocks: Option<Arc<RocksStore>>,
    /// One entry per shard, in shard order. Always exactly `num_shards` long —
    /// a length mismatch is a recovery error, not a silent default.
    pub shards: Vec<(HashMapStore, ExpiryIndex)>,
    /// Parsed function libraries from `functions.fdb` (name, source).
    pub functions: Vec<(String, String)>,
    /// Replication identity + offset from the state file.
    pub replication: ReplicationState,
    /// Open Raft storage; `None` in standalone mode.
    pub raft_storage: Option<ClusterStorage>,
    /// True iff a staged full-sync checkpoint was installed this boot.
    /// (Future phase: surface the staged `replication_metadata.json` here.)
    pub installed_staged_checkpoint: bool,
    pub stats: RecoveryStats,
}

/// Phases, in execution order. Errors carry the failing phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    InstallStagedCheckpoint,
    OpenRocks,
    RestoreShards,
    RestoreFunctions,
    RestoreReplicationState,
    OpenClusterStorage,
}

#[derive(Debug, thiserror::Error)]
#[error("recovery failed during {phase:?}: {source}")]
pub struct RecoveryError {
    pub phase: RecoveryPhase,
    #[source]
    pub source: anyhow::Error,
}

/// The one seam. The ordering invariant is this function body, readable top to bottom.
pub fn recover(inputs: &RecoveryInputs<'_>) -> Result<RecoveredState, RecoveryError> {
    let installed = checkpoint::install_staged(inputs)?;            // 1
    let rocks = shards::open_rocks(inputs)?;                        // 2
    let (shards, stats) = shards::restore(inputs, rocks.as_ref())?; // 3
    let functions = functions::restore(inputs)?;                    // 4
    let replication = replication::restore_state(inputs)?;          // 5
    let raft_storage = cluster::open_storage(inputs)?;              // 6
    Ok(RecoveredState { rocks, shards, functions, replication, raft_storage,
                        installed_staged_checkpoint: installed, stats })
}
```

Each phase is a plain function over plain data, so each is individually testable through the
module's seam or directly. The seam is synchronous (recovery is filesystem + RocksDB iteration);
the caller decides whether to `spawn_blocking` it (see open questions).

After: `init.rs` collapses its persistence/recovery sections to one call through the seam, and the
downstream init phases consume `RecoveredState` fields instead of re-doing recovery themselves:

```rust
// frogdb-server/crates/server/src/server/init.rs (after)
    let recovered = crate::recovery::recover(&RecoveryInputs::from_config(config, num_shards))?;

    // Runtime concerns start *after* recovery, explicitly:
    let periodic_sync_handle =
        startup::spawn_wal_sync_if_periodic(&config.persistence, &recovered.rocks, wal_sync_monitor);
```

```rust
// frogdb-server/crates/server/src/server/replication_init.rs (after) — recovery step removed;
// the handler is constructed from already-recovered state:
pub(super) fn init_replication(
    config: &Config,
    recovered: &RecoveredState,
    ...
) -> Result<ReplicationInitResult> {
    let repl_state = recovered.replication.clone();
    let tracker = Arc::new(ReplicationTrackerImpl::new());
    tracker.set_offset(repl_state.replication_offset);
    ...
```

`startup.rs:init_persistence` and the `PersistenceInitResult` tuple alias are deleted.

Why this is the right shape:

- **Locality.** Every recovery step lives in one module; the order invariant is six lines of
  `recover()` instead of the incidental layout of four files. "Where do I add a recovery step?"
  has exactly one answer: a new phase function and a line in `recover()`. The module passes the
  deletion test: remove `src/recovery/` and you lose exactly crash recovery — nothing else.
- **Leverage.** One seam pays for: crash-recovery tests without a server boot (today impossible),
  an obvious landing spot for the offset-persistence fix (consume the staged
  `replication_metadata.json` in `RestoreReplicationState`), future phases (cluster validation,
  any new persisted subsystem), and phase-tagged error reporting for operators
  ("recovery failed during InstallStagedCheckpoint" beats a bare anyhow chain).
- **Depth.** The current `init_persistence` is shallow — a tuple-returning helper whose callers
  must know the protocol around it. `recover()` is deep: one call, complex internals hidden, and
  the output type documents what recovery produces.

## Migration plan

Extract phases behind the orchestrator one at a time; every step is behavior-preserving and lands
green on `just check && just test`.

1. **Create the module + move `init_persistence`.** Add `server/src/recovery/`, move the body of
   `startup.rs:init_persistence` into phases 1-3 minus the WAL-sync spawn (step 5 above), which
   becomes `startup::spawn_wal_sync_if_periodic` called from `init.rs` after recovery. Delete
   `PersistenceInitResult` (`util.rs:33-37`). `RecoveredState` starts with `rocks`, `shards`,
   `installed_staged_checkpoint`, `stats`.
2. **Move `functions.fdb` load** out of `init.rs:280-307` into phase 4. The phase returns parsed
   `(name, code)` pairs; `init.rs` keeps the registry wiring (load into `SharedFunctionRegistry`).
3. **Hoist `ReplicationState::load_or_create`** out of `replication_init.rs:50-51` and `:102-103`
   into phase 5 (one call instead of two role-specific copies). `init_replication` takes
   `&RecoveredState` and constructs handlers from it.
4. **Hoist `ClusterStorage::open`** out of `cluster_init.rs:63-65` into phase 6. Raft instance
   construction, log replay, and bootstrap stay in `cluster_init` (they are async and entangled
   with networking) but consume `recovered.raft_storage`.
5. **Make the shard handoff strict.** Replace `shards.rs:60` `unwrap_or_default()` with a
   guarantee from the orchestrator that `shards.len() == num_shards`, erroring at recovery time on
   mismatch.
6. **Tests.** Add seam-level and phase-level tests (below). Follow-up (separate change): the
   offset-persistence fix adds "read staged `replication_metadata.json`" to phase 5 — a one-phase
   diff instead of a three-crate hunt.

Search-index recovery (step 11) stays in `shards.rs` for now — it is per-worker and produces
non-`Send`-friendly open index handles; see open questions.

## Testing impact

Crash recovery becomes testable through one seam: synthesize a data dir state on disk, call
`recover()`, assert on `RecoveredState`. No listeners, no shard workers, no full boot.

Seam-level tests (in `server/src/recovery/`):

- **Staged checkpoint install:** create `checkpoint_ready/` next to a populated db dir → recover →
  assert checkpoint data won, old db renamed to `*_backup_*`, `installed_staged_checkpoint == true`.
  (`checkpoint.rs` currently has zero tests.)
- **Fresh boot:** empty data dir → `num_shards` empty stores, fresh replication id, offset 0.
- **Restart with data:** write via `RocksStore::put` + `serialize` → recover → keys present,
  expired keys skipped, expiry index populated (mirrors existing `recovery.rs` unit tests but now
  covering the full sequence including ordering).
- **Corrupt/missing replication state file:** recover → fresh state generated and saved (existing
  `state.rs` behavior, now asserted at the seam).
- **Shard-count mismatch:** data dir created with N shards, recover with M → explicit
  `RecoveryError`, not silent data loss.
- **Persistence disabled:** `rocks == None`, empty shards, no filesystem touched beyond state file.

Phase-level tests: each phase function takes plain inputs, so edge cases (e.g.
`install_staged` with a data dir that has no parent, `restore_functions` with a corrupt
`functions.fdb`) get direct unit tests without constructing the rest of the world.

Concurrency/integration: existing turmoil and full-boot tests keep covering the wiring; the new
tests subtract the boot dependency from recovery-correctness coverage rather than duplicating it.

## Risks / open questions

- **Async/blocking boundary.** `recover()` is blocking I/O but is called from async
  `Server::with_listeners`. Today `init_persistence` already blocks the runtime at startup, so a
  synchronous seam is behavior-preserving; wrapping the call in `spawn_blocking` is a follow-up.
  Counterpoint: `ClusterStorage::open` is sync today, but if Raft storage ever needs async open,
  phase 6 forces a decision (async seam vs. keeping cluster open out of the orchestrator).
- **Ordering between replication and cluster restore.** Phase 5 (replication state) runs before
  phase 6 (Raft storage), matching today's `mod.rs:218` → `mod.rs:228` order. But Raft log replay
  can imply a role change (this node was demoted while down) that contradicts the replication
  state file. Today that reconciliation is implicit/absent; the orchestrator makes both restored
  values visible in one struct, but deciding who wins is wiring-layer policy and stays out of
  scope. The spec for that decision should reference Redis/Valkey behavior (replicas trust the
  cluster config epoch over local replication state).
- **Cluster-mode vs standalone divergence.** `RecoveredState` uses `Option` fields for cluster
  artifacts, mirroring today's shape. If divergence grows, a
  `RecoveredTopology::{Standalone, Cluster{..}}` enum would state the invariant more strongly —
  deferred until a third mode-dependent field appears.
- **Search-index recovery placement.** Step 11 opens per-shard index handles inside worker
  construction. Moving it into the orchestrator means shipping open handles through
  `RecoveredState` (ownership/`Send` questions) vs. leaving a documented second recovery site.
  Proposed: leave in `shards.rs` with a comment pointing at the orchestrator, revisit when search
  metadata grows another consumer.
- **Staged replication metadata.** `replication_metadata.json` lands inside the installed db dir
  and is never read (`replica/connection.rs:302`). Phase 5 is the obvious consumer, but the format
  has no reader today — the offset-persistence fix (out of scope) should define it.
- **Turmoil builds.** The orchestrator returns data and spawns nothing, so it avoids the
  `crate::net` / `cfg(turmoil)` abstractions entirely — recovery tests run as plain unit tests in
  both build flavors. Anything that spawns stays in init code.
