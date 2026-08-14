//! Phase 4: restore persisted function libraries from `functions.fdb`.
//!
//! This phase only reads from disk and returns the raw `(library name, source
//! code)` pairs. Parsing the source and loading it into the function registry is
//! wiring-layer work that stays in `init.rs`, because the registry is a live
//! component the recovery seam deliberately does not touch.

use anyhow::Result;
use frogdb_types::metrics::definitions::RecoveryFunctionsFailed;
use tracing::warn;

use crate::RecoveryInputs;

/// Read persisted function libraries from `functions.fdb`.
///
/// A missing or empty file yields an empty vector (the common fresh-boot case).
/// An unreadable or corrupt file is *not* a recovery failure: it is logged and
/// treated as "no functions", matching the prior inline behavior — a corrupt
/// function library should not block the database from starting.
///
/// The tolerance is counted, not silent: a downgraded file returns a failure
/// count of `1` and increments `frogdb_recovery_functions_failed_total`, so a
/// boot that came back with a smaller `FUNCTION LIST` is distinguishable from
/// one that never had the libraries (FM-PERSISTENCE-037). One, not the number
/// of libraries in the file — the file did not parse, so how many it held is
/// exactly what is unknown.
pub(crate) fn restore(inputs: &RecoveryInputs<'_>) -> Result<(Vec<(String, String)>, u64)> {
    let functions_path = inputs.data_dir.join("functions.fdb");
    match frogdb_core::load_from_file(&functions_path) {
        Ok(libraries) => Ok((libraries, 0)),
        Err(e) => {
            warn!(error = %e, "Failed to load persisted functions");
            RecoveryFunctionsFailed::inc(inputs.metrics_recorder.as_ref());
            Ok((Vec::new(), 1))
        }
    }
}
