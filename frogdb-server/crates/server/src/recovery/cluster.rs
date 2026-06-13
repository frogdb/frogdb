//! Phase 6: open the Raft cluster storage.
//!
//! This phase opens (or creates) the Raft log/metadata store at `<data_dir>/raft`
//! when cluster mode is enabled. Everything else about cluster bring-up — Raft
//! instance construction, log replay, and bootstrap — is async and entangled
//! with networking, so it stays in `cluster_init.rs` and consumes the opened
//! storage from `RecoveredState.raft_storage`.
//!
//! Like the replication phase, this is gated on cluster mode rather than on
//! persistence: the Raft store is independent of the RocksDB data store.

use anyhow::Result;
use frogdb_core::ClusterStorage;

use super::RecoveryInputs;

/// Open the Raft cluster storage when cluster mode is enabled, else `None`.
pub(super) fn open_storage(inputs: &RecoveryInputs<'_>) -> Result<Option<ClusterStorage>> {
    if !inputs.cluster.enabled {
        return Ok(None);
    }

    let raft_path = inputs.data_dir.join("raft");
    let storage = ClusterStorage::open(&raft_path)
        .map_err(|e| anyhow::anyhow!("Failed to open Raft storage: {}", e))?;
    Ok(Some(storage))
}
