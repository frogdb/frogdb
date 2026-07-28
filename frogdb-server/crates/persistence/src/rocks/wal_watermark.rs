//! Durable WAL sequence high-watermark: a signal for silent point-in-time
//! recovery truncation.
//!
//! FrogDB pins the RocksDB WAL recovery mode to
//! [`PointInTime`](rocksdb::DBRecoveryMode::PointInTime) (see the open path in
//! [`super`]). On a mid-log checksum failure, point-in-time recovery truncates
//! the WAL at the *first* corrupt record and silently discards **every** valid
//! record after it, then returns a perfectly healthy `open`. RocksDB surfaces
//! no programmatic signal that committed data vanished — an operator would be
//! missing acknowledged writes with nothing in the logs to explain it.
//!
//! To give recovery a signal, we persist the highest RocksDB sequence number
//! known to be durably synced into a small side-file next to the database
//! ([`FILE_NAME`]). On the next open we compare that watermark against the
//! sequence number RocksDB *actually* recovered to. A recovered sequence
//! **below** the watermark means WAL replay stopped early — records were
//! dropped — so we emit the
//! [`WalRecoveryDroppedRecords`](frogdb_types::metrics::definitions::WalRecoveryDroppedRecords)
//! counter and a WARN log carrying the exact count.
//!
//! ## Guarantee direction (why this never false-alarms)
//!
//! The watermark is only ever advanced *after* a durable sync
//! ([`RocksStore::record_wal_watermark`] is called from the sync flush path and
//! at graceful shutdown), and it is written best-effort with an atomic
//! temp-write + rename — it is never fsync'd on its own. If the watermark write
//! is lost in a crash it can only *lag* the true durable sequence, never lead
//! it. So the open-time comparison can under-report (miss a truncation whose
//! surviving suffix is still above a stale watermark) but can never fire on a
//! clean recovery. Under-reporting is the safe failure mode; a false "you lost
//! data" alarm is not.
//!
//! The file holds a single decimal `u64`. A torn or garbage file parses to
//! `None` and is treated as "no watermark" — again, a miss rather than a false
//! alarm.

use std::path::{Path, PathBuf};

use frogdb_types::metrics::definitions::WalRecoveryDroppedRecords;
use frogdb_types::traits::MetricsRecorder;
use tracing::{debug, warn};

/// Side-file, inside the RocksDB directory, holding the durable-sync sequence
/// watermark. The name is deliberately outside every RocksDB file pattern
/// (`*.sst`, `*.log`, `MANIFEST-*`, `CURRENT`, …) so RocksDB's obsolete-file
/// cleanup never touches it.
pub(crate) const FILE_NAME: &str = "frogdb_wal_watermark";

/// Absolute path of the watermark file for a RocksDB directory.
fn watermark_path(db_dir: &Path) -> PathBuf {
    db_dir.join(FILE_NAME)
}

/// Read the persisted watermark, or `None` if absent/unreadable/garbage.
///
/// Every failure mode collapses to `None`: a missing file (fresh database), an
/// unreadable file, or a non-numeric body all mean "no trustworthy prior
/// watermark", which suppresses detection rather than risking a false alarm.
pub(crate) fn read(db_dir: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(watermark_path(db_dir)).ok()?;
    raw.trim().parse::<u64>().ok()
}

/// Persist `seq` as the new watermark via an atomic temp-write + rename.
///
/// Best-effort: the caller treats an `Err` as non-fatal (a lost watermark only
/// costs a future detection, never correctness). Not fsync'd — see the module
/// docs on why a watermark that lags after a crash is the safe direction.
pub(crate) fn write(db_dir: &Path, seq: u64) -> std::io::Result<()> {
    let final_path = watermark_path(db_dir);
    // Same-directory temp so the rename is a single-filesystem atomic swap; a
    // crash mid-write leaves either the old file or the new one, never a torn
    // body that could parse to a bogus (possibly huge) sequence.
    let tmp_path = db_dir.join(format!("{FILE_NAME}.tmp"));
    std::fs::write(&tmp_path, seq.to_string())?;
    std::fs::rename(&tmp_path, &final_path)
}

/// Compare the persisted watermark against `recovered_seq` after a WAL replay,
/// emit the drop metric + a WARN log on a shortfall, then re-baseline the
/// watermark to `recovered_seq` so the next open compares against reality.
///
/// Returns the number of records point-in-time recovery dropped (0 when there
/// was no prior watermark or recovery reached/exceeded it). Only meaningful on
/// an *existing* database — callers skip it for a freshly created one, where
/// there is nothing to have recovered.
pub(crate) fn detect_and_reset(
    db_dir: &Path,
    recovered_seq: u64,
    metrics: &dyn MetricsRecorder,
) -> u64 {
    let dropped = match read(db_dir) {
        Some(watermark) if recovered_seq < watermark => {
            let dropped = watermark - recovered_seq;
            WalRecoveryDroppedRecords::inc_by(metrics, dropped);
            warn!(
                db_dir = %db_dir.display(),
                watermark,
                recovered_seq,
                dropped,
                "RocksDB point-in-time WAL recovery dropped committed records: the durable-sync \
                 sequence watermark was {watermark} but recovery reached only {recovered_seq}. \
                 A corrupt mid-log WAL record truncated the durable suffix ({dropped} sequence \
                 numbers of acknowledged writes were lost)."
            );
            dropped
        }
        Some(watermark) => {
            debug!(
                db_dir = %db_dir.display(),
                watermark,
                recovered_seq,
                "WAL recovery reached the durable-sync watermark; no corruption truncation detected"
            );
            0
        }
        None => 0,
    };
    // Re-baseline to what actually recovered. Without this, a database that
    // suffered a truncation (recovered_seq now below the old watermark) would
    // re-alarm on every subsequent open until it wrote past the stale mark.
    if let Err(e) = write(db_dir, recovered_seq) {
        debug!(db_dir = %db_dir.display(), error = %e, "Failed to re-baseline WAL watermark after recovery");
    }
    dropped
}
