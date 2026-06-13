//! Phase 4: restore persisted function libraries from `functions.fdb`.
//!
//! This phase only reads from disk and returns the raw `(library name, source
//! code)` pairs. Parsing the source and loading it into the function registry is
//! wiring-layer work that stays in `init.rs`, because the registry is a live
//! component the recovery seam deliberately does not touch.

use anyhow::Result;
use tracing::warn;

use super::RecoveryInputs;

/// Read persisted function libraries from `functions.fdb`.
///
/// A missing or empty file yields an empty vector (the common fresh-boot case).
/// An unreadable or corrupt file is *not* a recovery failure: it is logged and
/// treated as "no functions", matching the prior inline behavior — a corrupt
/// function library should not block the database from starting.
pub(super) fn restore(inputs: &RecoveryInputs<'_>) -> Result<Vec<(String, String)>> {
    let functions_path = inputs.data_dir.join("functions.fdb");
    match frogdb_core::load_from_file(&functions_path) {
        Ok(libraries) => Ok(libraries),
        Err(e) => {
            warn!(error = %e, "Failed to load persisted functions");
            Ok(Vec::new())
        }
    }
}
