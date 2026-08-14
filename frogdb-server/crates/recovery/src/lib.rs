//! Crash-recovery orchestrator.
//!
//! Startup recovery used to be smeared across the server crate's
//! `server/startup.rs`, `server/init.rs`, `server/replication_init.rs`,
//! `server/cluster_init.rs`, and `server/shards.rs`, with the recovery-order
//! invariant existing only as the incidental top-to-bottom layout of
//! `Server::with_listeners()`. This crate owns that invariant: a single seam
//! [`recover`] over a set of ordered phases.
//!
//! The seam is deliberately deep — one function, plain-data inputs and outputs —
//! over phases of filesystem + RocksDB internals. Inputs are config + data dir;
//! outputs are recovered handles and plain data. No live components, no
//! listeners, no spawned tasks: wiring stays in the server's `init.rs`/`mod.rs`.
//! That is also why recovery needs no host trait: nothing here reaches into
//! `Server`/`Subsystems` state, so there are no server-coupled effects to invert
//! (contrast `frogdb_txn`'s `TxnHost`, ADR 0002). Because the orchestrator
//! spawns nothing and returns data, it sidesteps the server's `net` /
//! `cfg(turmoil)` abstractions entirely and its tests run as plain unit tests in
//! both build flavors — and, since this crate depends on neither the server nor
//! any async-network crate, they build without the server's 130K-LOC test binary.
//!
//! The recovery-order invariant, readable top to bottom in [`recover`]:
//! **verify the data directory → install staged checkpoint → open RocksDB →
//! restore shard stores → restore functions → restore replication state → open
//! cluster storage**.
//!
//! One recovery step deliberately stays out of the seam: per-shard search-index
//! recovery, owned by `frogdb_core::IndexLifecycleManager::recover` and invoked
//! from the server's `server/shards.rs` at worker-spawn time. It opens non-`Send`
//! tantivy + usearch handles directly into each worker as it is constructed, so
//! shipping them through [`RecoveredState`] is awkward; the lifecycle seam
//! (proposal 15) gives that site a real, testable home while keeping the `Send`
//! boundary clean (proposal 06, "Search-index recovery placement").

use std::path::Path;

use frogdb_config::{
    ClusterConfigSection, Config, PersistenceConfig, RecoveryConfig, ReplicationConfigSection,
};
use frogdb_core::persistence::{RecoveryStats, RocksStore};
use frogdb_core::sync::Arc;
use frogdb_core::{ClusterStorage, ExpiryIndex, HashMapStore, MetricsRecorder, ReplicationState};
use tracing::info;

mod checkpoint;
mod cluster;
mod data_dir;
mod functions;
mod replication;
mod shards;

#[cfg(test)]
mod tests;

/// What recovery reads. Pure data — no sockets, channels, or running components.
pub struct RecoveryInputs<'a> {
    /// Data directory root (equal to `persistence.data_dir`).
    pub data_dir: &'a Path,
    /// Persistence configuration.
    pub persistence: &'a PersistenceConfig,
    /// Replication configuration (role + state file name).
    pub replication: &'a ReplicationConfigSection,
    /// Cluster configuration (whether cluster mode is enabled).
    pub cluster: &'a ClusterConfigSection,
    /// Recovery policy: what to do about state that will not decode.
    /// Separate from [`Self::persistence`] because it is
    /// policy rather than plumbing — the same data dir boots or refuses
    /// depending only on this.
    pub recovery: &'a RecoveryConfig,
    /// Number of shards the server is configured for.
    pub num_shards: usize,
    /// Whether the warm tier (tiered storage) column families are enabled.
    pub warm_enabled: bool,
    /// Metrics recorder injected into the RocksDB store at open, so
    /// store-initiated background work (post-clear reclamation counters) is
    /// wired to the real recorder from construction. Built separately at server
    /// startup (`init.rs`) before recovery runs, so it cannot be derived from
    /// `config` and is threaded in explicitly.
    pub metrics_recorder: Arc<dyn MetricsRecorder>,
}

impl<'a> RecoveryInputs<'a> {
    /// Build recovery inputs from the server config, the resolved shard count,
    /// and the already-constructed metrics recorder.
    pub fn from_config(
        config: &'a Config,
        num_shards: usize,
        metrics_recorder: Arc<dyn MetricsRecorder>,
    ) -> Self {
        Self {
            data_dir: &config.persistence.data_dir,
            persistence: &config.persistence,
            replication: &config.replication,
            cluster: &config.cluster,
            recovery: &config.recovery,
            num_shards,
            warm_enabled: config.tiered_storage.enabled,
            metrics_recorder,
        }
    }
}

/// What recovery produces. Opened handles + plain data; component wiring happens
/// later in the init phases.
pub struct RecoveredState {
    /// Open store; `None` when persistence is disabled.
    pub rocks: Option<Arc<RocksStore>>,
    /// One entry per shard, in shard order. Always exactly `num_shards` long — a
    /// length mismatch is a recovery error, not a silent default.
    pub shards: Vec<(HashMapStore, ExpiryIndex)>,
    /// Persisted function libraries read from `functions.fdb` as raw
    /// `(name, source code)` pairs. The wiring layer parses and registers them.
    pub functions: Vec<(String, String)>,
    /// Replication identity + offset, reconciled with any staged full-sync
    /// metadata installed this boot. Seeding the in-memory tracker from it stays
    /// in the wiring layer.
    pub replication: ReplicationState,
    /// Open Raft storage; `None` in standalone (non-cluster) mode. Raft instance
    /// construction, log replay, and bootstrap consume it in the wiring layer.
    pub raft_storage: Option<ClusterStorage>,
    /// True iff a staged full-sync checkpoint was installed this boot.
    ///
    /// Part of the recovery output contract and asserted by the seam tests. The
    /// wiring layer does not consume it yet; a later phase will surface the
    /// staged `replication_metadata.json` alongside it (see proposal 06).
    pub installed_staged_checkpoint: bool,
    /// Aggregate recovery statistics (keys loaded, expired, bytes, duration).
    ///
    /// Logged during the restore phase and asserted by the seam tests.
    pub stats: RecoveryStats,
}

/// Recovery phases, in execution order. Errors carry the failing phase so
/// operators get "recovery failed during OpenRocks" instead of a bare anyhow
/// chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPhase {
    /// Decide whether the data directory is one FrogDB may initialize, before
    /// anything writes to it: marker present, genuinely empty, or refuse.
    VerifyDataDir,
    /// Install a staged full-sync checkpoint (filesystem rename surgery on the
    /// data dir, before the DB can be opened).
    InstallStagedCheckpoint,
    /// Open RocksDB (and optional warm-tier column families).
    OpenRocks,
    /// Restore per-shard hash tables, expiry indexes, and warm-tier entries.
    RestoreShards,
    /// Read persisted function libraries from `functions.fdb`.
    RestoreFunctions,
    /// Restore replication state, reconciled with staged full-sync metadata.
    RestoreReplicationState,
    /// Open the Raft cluster storage (cluster mode only).
    OpenClusterStorage,
}

/// Error from a recovery phase, tagged with the phase that failed.
#[derive(Debug, thiserror::Error)]
#[error("recovery failed during {phase:?}: {source}")]
pub struct RecoveryError {
    /// The phase that failed.
    pub phase: RecoveryPhase,
    /// The underlying cause.
    #[source]
    pub source: anyhow::Error,
}

impl RecoveryError {
    fn new(phase: RecoveryPhase, source: impl Into<anyhow::Error>) -> Self {
        Self {
            phase,
            source: source.into(),
        }
    }
}

/// The one recovery seam. The ordering invariant is this function body, readable
/// top to bottom.
///
/// Synchronous: recovery is filesystem + RocksDB iteration. Today's startup
/// already blocks the runtime here, so a synchronous seam is behavior-preserving;
/// wrapping the call in `spawn_blocking` is a follow-up.
pub fn recover(inputs: &RecoveryInputs<'_>) -> Result<RecoveredState, RecoveryError> {
    // Persistence phases (1-4) only run when persistence is enabled AND backed
    // by RocksDB; otherwise the on-disk store does not exist and we start with
    // fresh per-shard stores. The `fake` WAL mode is enabled-but-RocksDB-less:
    // it records into an in-process fake sink (simulation tests), so it takes
    // the fresh-stores path here and never opens RocksDB.
    let rocks_backed =
        inputs.persistence.enabled && !inputs.persistence.mode.eq_ignore_ascii_case("fake");
    let (rocks, shards, functions, installed, stats) = if rocks_backed {
        info!(
            data_dir = %inputs.persistence.data_dir.display(),
            durability_mode = %inputs.persistence.durability_mode,
            "Initializing persistence"
        );

        // Phase 0 is a *gate*, not a step: it decides whether this directory is
        // FrogDB's before the install below renames anything into it, before
        // RocksDB creates a database in it, and — because recovery as a whole
        // finishes before `init_replication` dials anyone — before a full
        // resync could repopulate it and hide the mistake behind data that
        // looks plausible.
        let marker = data_dir::verify(inputs)
            .map_err(|e| RecoveryError::new(RecoveryPhase::VerifyDataDir, e))?;
        let installed = checkpoint::install_staged(inputs)
            .map_err(|e| RecoveryError::new(RecoveryPhase::InstallStagedCheckpoint, e))?;
        // The install phase is what guarantees the data directory exists, and a
        // marker cannot be published into a directory that is not there; this
        // is the same phase's second half, so it reports as one.
        data_dir::stamp(inputs.data_dir, &marker)
            .map_err(|e| RecoveryError::new(RecoveryPhase::VerifyDataDir, e))?;
        let rocks = shards::open_rocks(inputs)
            .map_err(|e| RecoveryError::new(RecoveryPhase::OpenRocks, e))?;
        let (shards, mut stats) = shards::restore(inputs, &rocks)
            .map_err(|e| RecoveryError::new(RecoveryPhase::RestoreShards, e))?;
        let (functions, functions_failed) = functions::restore(inputs)
            .map_err(|e| RecoveryError::new(RecoveryPhase::RestoreFunctions, e))?;
        stats.functions_failed = functions_failed;
        (Some(rocks), shards, functions, installed, stats)
    } else {
        info!("Persistence disabled");
        (
            None,
            fresh_shards(inputs.num_shards),
            Vec::new(),
            false,
            RecoveryStats::default(),
        )
    };

    // Orchestrator invariant: exactly one recovered store per configured shard.
    // The persistence open (`RocksStore::open`) already aborts loudly on a
    // shard-count mismatch, and `spawn_shard_workers` guards the handoff; this
    // states the guarantee at the seam so the doc contract on `shards` holds by
    // construction and any future restore path that breaks it fails here rather
    // than silently dropping or misrouting data.
    if shards.len() != inputs.num_shards {
        return Err(RecoveryError::new(
            RecoveryPhase::RestoreShards,
            anyhow::anyhow!(
                "recovered {} shard store(s) but the server is configured for {} shard(s)",
                shards.len(),
                inputs.num_shards
            ),
        ));
    }

    // Replication state (phase 5) is role-gated, not persistence-gated:
    // replication runs without RocksDB persistence, so this phase runs in both
    // cases (it is a no-op for standalone nodes).
    let replication = replication::restore_state(inputs)
        .map_err(|e| RecoveryError::new(RecoveryPhase::RestoreReplicationState, e))?;

    // Raft storage (phase 6) is cluster-gated, also independent of persistence.
    let raft_storage = cluster::open_storage(inputs)
        .map_err(|e| RecoveryError::new(RecoveryPhase::OpenClusterStorage, e))?;

    Ok(RecoveredState {
        rocks,
        shards,
        functions,
        replication,
        raft_storage,
        installed_staged_checkpoint: installed,
        stats,
    })
}

/// Build `num_shards` empty per-shard stores for a fresh boot.
fn fresh_shards(num_shards: usize) -> Vec<(HashMapStore, ExpiryIndex)> {
    (0..num_shards).map(|_| Default::default()).collect()
}
