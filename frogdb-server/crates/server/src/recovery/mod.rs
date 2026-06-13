//! Crash-recovery orchestrator.
//!
//! Startup recovery used to be smeared across `server/startup.rs`,
//! `server/init.rs`, `server/replication_init.rs`, `server/cluster_init.rs`, and
//! `server/shards.rs`, with the recovery-order invariant existing only as the
//! incidental top-to-bottom layout of `Server::with_listeners()`. This module
//! owns that invariant: a single seam [`recover`] over a set of ordered phases.
//!
//! The seam is deliberately deep — one function, plain-data inputs and outputs —
//! over phases of filesystem + RocksDB internals. Inputs are config + data dir;
//! outputs are recovered handles and plain data. No live components, no
//! listeners, no spawned tasks: wiring stays in `init.rs`/`mod.rs`. Because the
//! orchestrator spawns nothing and returns data, it sidesteps the `crate::net` /
//! `cfg(turmoil)` abstractions entirely and its tests run as plain unit tests in
//! both build flavors.
//!
//! The recovery-order invariant, readable top to bottom in [`recover`]:
//! **install staged checkpoint → open RocksDB → restore shard stores**. Later
//! phases (functions, replication state, cluster storage) are added behind the
//! same seam as they are extracted from the wiring layer.

use std::path::Path;

use frogdb_core::persistence::{RecoveryStats, RocksStore};
use frogdb_core::sync::Arc;
use frogdb_core::{ExpiryIndex, HashMapStore};
use tracing::info;

use crate::config::{Config, PersistenceConfig};

mod checkpoint;
mod shards;

#[cfg(test)]
mod tests;

/// What recovery reads. Pure data — no sockets, channels, or running components.
pub(crate) struct RecoveryInputs<'a> {
    /// Data directory root (equal to `persistence.data_dir`).
    pub data_dir: &'a Path,
    /// Persistence configuration.
    pub persistence: &'a PersistenceConfig,
    /// Number of shards the server is configured for.
    pub num_shards: usize,
    /// Whether the warm tier (tiered storage) column families are enabled.
    pub warm_enabled: bool,
}

impl<'a> RecoveryInputs<'a> {
    /// Build recovery inputs from the server config and the resolved shard count.
    pub fn from_config(config: &'a Config, num_shards: usize) -> Self {
        Self {
            data_dir: &config.persistence.data_dir,
            persistence: &config.persistence,
            num_shards,
            warm_enabled: config.tiered_storage.enabled,
        }
    }
}

/// What recovery produces. Opened handles + plain data; component wiring happens
/// later in the init phases.
pub(crate) struct RecoveredState {
    /// Open store; `None` when persistence is disabled.
    pub rocks: Option<Arc<RocksStore>>,
    /// One entry per shard, in shard order. Always exactly `num_shards` long — a
    /// length mismatch is a recovery error, not a silent default.
    pub shards: Vec<(HashMapStore, ExpiryIndex)>,
    /// True iff a staged full-sync checkpoint was installed this boot.
    ///
    /// Part of the recovery output contract and asserted by the seam tests. The
    /// wiring layer does not consume it yet; a later phase will surface the
    /// staged `replication_metadata.json` alongside it (see proposal 06).
    #[allow(dead_code)]
    pub installed_staged_checkpoint: bool,
    /// Aggregate recovery statistics (keys loaded, expired, bytes, duration).
    ///
    /// Logged during the restore phase and asserted by the seam tests.
    #[allow(dead_code)]
    pub stats: RecoveryStats,
}

/// Recovery phases, in execution order. Errors carry the failing phase so
/// operators get "recovery failed during OpenRocks" instead of a bare anyhow
/// chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryPhase {
    /// Install a staged full-sync checkpoint (filesystem rename surgery on the
    /// data dir, before the DB can be opened).
    InstallStagedCheckpoint,
    /// Open RocksDB (and optional warm-tier column families).
    OpenRocks,
    /// Restore per-shard hash tables, expiry indexes, and warm-tier entries.
    RestoreShards,
}

/// Error from a recovery phase, tagged with the phase that failed.
#[derive(Debug, thiserror::Error)]
#[error("recovery failed during {phase:?}: {source}")]
pub(crate) struct RecoveryError {
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
pub(crate) fn recover(inputs: &RecoveryInputs<'_>) -> Result<RecoveredState, RecoveryError> {
    // Persistence disabled: nothing on disk, fresh per-shard stores. No
    // filesystem is touched.
    if !inputs.persistence.enabled {
        info!("Persistence disabled");
        return Ok(RecoveredState {
            rocks: None,
            shards: fresh_shards(inputs.num_shards),
            installed_staged_checkpoint: false,
            stats: RecoveryStats::default(),
        });
    }

    info!(
        data_dir = %inputs.persistence.data_dir.display(),
        durability_mode = %inputs.persistence.durability_mode,
        "Initializing persistence"
    );

    let installed = checkpoint::install_staged(inputs)
        .map_err(|e| RecoveryError::new(RecoveryPhase::InstallStagedCheckpoint, e))?;
    let rocks =
        shards::open_rocks(inputs).map_err(|e| RecoveryError::new(RecoveryPhase::OpenRocks, e))?;
    let (shards, stats) = shards::restore(inputs, &rocks)
        .map_err(|e| RecoveryError::new(RecoveryPhase::RestoreShards, e))?;

    Ok(RecoveredState {
        rocks: Some(rocks),
        shards,
        installed_staged_checkpoint: installed,
        stats,
    })
}

/// Build `num_shards` empty per-shard stores for a fresh boot.
fn fresh_shards(num_shards: usize) -> Vec<(HashMapStore, ExpiryIndex)> {
    (0..num_shards).map(|_| Default::default()).collect()
}
