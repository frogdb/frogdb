//! Phase 1: install a staged full-sync checkpoint.
//!
//! A replica that receives a checkpoint full sync stages the new database under
//! `checkpoint_ready/` next to the live data dir (the typed contract lives in
//! `frogdb_persistence::rocks::staged`). This phase ensures the data dir
//! exists, then installs the staged checkpoint (filesystem rename surgery) *before*
//! the DB is opened. The install validates `checkpoint_ready/CURRENT` before
//! touching the live dir, so an incomplete checkpoint is refused without moving
//! the live database aside, and prunes displaced-database backups beyond the
//! retention limit (see [`RocksStore::load_staged_checkpoint`]).

use std::fs;

use anyhow::Result;
use frogdb_core::persistence::RocksStore;
use tracing::{error, info};

use super::RecoveryInputs;

/// Ensure the data directory exists, then install a staged full-sync checkpoint
/// if one is present. Returns `true` iff a checkpoint was installed this boot.
pub(super) fn install_staged(inputs: &RecoveryInputs<'_>) -> Result<bool> {
    // Ensure data directory exists.
    fs::create_dir_all(inputs.data_dir)?;

    // Check for and load a staged checkpoint from a replica full sync.
    match RocksStore::load_staged_checkpoint(inputs.data_dir) {
        Ok(true) => {
            info!("Loaded staged checkpoint from replica full sync");
            Ok(true)
        }
        // No checkpoint to load, continue normally.
        Ok(false) => Ok(false),
        Err(e) => {
            error!(error = %e, "Failed to load staged checkpoint");
            Err(anyhow::anyhow!("Failed to load staged checkpoint: {}", e))
        }
    }
}
