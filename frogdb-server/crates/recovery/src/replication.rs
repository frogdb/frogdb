//! Phase 5: restore replication state, reconciled with staged full-sync metadata.
//!
//! This phase runs after phase 1 has installed any staged checkpoint, so the
//! `replication_metadata.json` that a replica stages inside a full-sync
//! checkpoint has already been carried into the data dir. It loads the persisted
//! replication state (id + offset), adopts the staged metadata over it when
//! present (the metadata describes the offset matching the freshly installed
//! snapshot), persists the reconciled state, and consumes the staging file.
//!
//! The returned `ReplicationState` is the *reconciled* state. Seeding the
//! in-memory replication tracker from it is deliberately left to the wiring
//! layer (`replication_init.rs`): the synchronous recovery seam produces plain
//! data and never touches live components.
//!
//! Standalone nodes never persisted replication state, so this phase returns a
//! fresh in-memory state without touching disk for them — preserving that
//! behavior. Replication does not require persistence to be enabled, so this
//! phase runs in both the persistence-enabled and persistence-disabled cases.

use anyhow::Result;
use frogdb_core::ReplicationState;
use tracing::{info, warn};

use crate::RecoveryInputs;

/// Load and reconcile the persisted replication state.
pub(crate) fn restore_state(inputs: &RecoveryInputs<'_>) -> Result<ReplicationState> {
    // Standalone mode never read or wrote a replication state file; return a
    // fresh in-memory state and touch no disk.
    if !(inputs.replication.is_primary() || inputs.replication.is_replica()) {
        return Ok(ReplicationState::new());
    }

    let state_path = inputs.data_dir.join(&inputs.replication.state_file);
    let mut repl_state = ReplicationState::load_or_create(&state_path)
        .map_err(|e| anyhow::anyhow!("Failed to load replication state: {}", e))?;

    // Reconcile with any staged full-sync metadata installed this boot. Corrupt
    // or absent metadata falls through to the loaded/fresh state — a fresh state
    // has offset 0, which forces a full resync rather than serving a
    // silently-zeroed or mismatched offset.
    match frogdb_core::read_staged_replication_metadata(inputs.data_dir) {
        Ok(Some(meta)) => {
            info!(
                replication_id = %meta.replication_id,
                offset = meta.replication_offset,
                "Adopting replication offset from staged full-sync checkpoint"
            );
            repl_state.apply_staged_metadata(&meta);
            match repl_state.save(&state_path) {
                // The state file now carries the snapshot's position, so the
                // staging file has nothing left to say: consume it and later
                // restarts read the state file.
                Ok(()) => frogdb_core::consume_staged_replication_metadata(inputs.data_dir),
                // The write did not land, so the staging file is still the only
                // durable copy of the offset that matches the installed
                // snapshot. Keeping it costs one idempotent re-adoption on the
                // next boot (the metadata describes data that is already
                // there); consuming it would leave that boot resuming from the
                // pre-full-sync offset against post-full-sync data.
                Err(e) => warn!(
                    error = %e,
                    "Failed to persist reconciled replication state; keeping the staged \
                     metadata so the next boot re-adopts it"
                ),
            }
        }
        Ok(None) => {}
        Err(e) => {
            warn!(error = %e, "Failed to read staged replication metadata");
        }
    }

    Ok(repl_state)
}
