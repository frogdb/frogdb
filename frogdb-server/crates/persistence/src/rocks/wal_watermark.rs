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

/// Persist `candidate` as the new watermark, but only if it is higher than
/// what is already recorded — a `fetch_max`, not a blind overwrite.
///
/// [`RocksStore::record_wal_watermark`](super::RocksStore::record_wal_watermark)
/// is called independently and concurrently by every shard's own sync commit
/// plus the periodic `durable_sync` tick; each reports its own covered
/// sequence out of order relative to the others. A lower candidate arriving
/// after a higher one must not regress the mark — see the module docs above
/// on why lagging, never leading, is the safe direction. A missing/unreadable
/// current watermark (fresh database, torn file) is treated as `0` here so
/// the candidate always wins.
pub(crate) fn fetch_max(db_dir: &Path, candidate: u64) -> std::io::Result<()> {
    if let Some(current) = read(db_dir)
        && candidate <= current
    {
        return Ok(());
    }
    write(db_dir, candidate)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

    /// Records every counter call, including one carrying zero — the
    /// difference between "reported nothing" and "reported a loss of zero
    /// records", which is exactly what the never-false-alarm guarantee is
    /// about.
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<Vec<(String, u64)>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
            self.counters
                .lock()
                .unwrap()
                .push((name.to_string(), value));
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
    }

    impl RecordingRecorder {
        fn calls(&self) -> Vec<(String, u64)> {
            self.counters.lock().unwrap().clone()
        }
    }

    const COUNTER: &str = "frogdb_wal_recovery_dropped_records_total";

    // FM-PERSISTENCE-035
    /// The watermark is a plain decimal `u64` in a file whose name no RocksDB
    /// cleanup pattern matches, and every unreadable form of it means "no
    /// watermark" rather than a bogus one.
    #[test]
    fn the_watermark_file_round_trips_and_degrades_to_none() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(read(tmp.path()), None, "a fresh database has no watermark");

        write(tmp.path(), 4_294_967_296).unwrap();
        assert_eq!(
            std::fs::read_to_string(tmp.path().join(FILE_NAME)).unwrap(),
            "4294967296",
            "the body is the decimal sequence, nothing else"
        );
        assert_eq!(read(tmp.path()), Some(4_294_967_296));
        assert!(
            !tmp.path().join(format!("{FILE_NAME}.tmp")).exists(),
            "the temp file is renamed away, not left behind"
        );

        std::fs::write(tmp.path().join(FILE_NAME), "not a number").unwrap();
        assert_eq!(read(tmp.path()), None, "garbage parses as absent");
        std::fs::write(tmp.path().join(FILE_NAME), "-5").unwrap();
        assert_eq!(read(tmp.path()), None, "so does a negative");
    }

    // FM-PERSISTENCE-035
    /// A recovery that reached the watermark — or exactly matched it — lost
    /// nothing, and must raise *nothing at all*. Reporting a loss of zero
    /// records is still an alarm on a dashboard that counts events.
    #[test]
    fn a_recovery_that_reaches_the_watermark_reports_nothing() {
        let tmp = TempDir::new().unwrap();
        let metrics = RecordingRecorder::default();

        // No prior watermark: nothing to compare against.
        assert_eq!(detect_and_reset(tmp.path(), 100, &metrics), 0);
        assert!(metrics.calls().is_empty());
        assert_eq!(
            read(tmp.path()),
            Some(100),
            "and the first open baselines the watermark"
        );

        // Recovery landed exactly on the watermark.
        assert_eq!(detect_and_reset(tmp.path(), 100, &metrics), 0);
        assert!(
            metrics.calls().is_empty(),
            "an exact match is a clean recovery, not a zero-record loss"
        );

        // Recovery ran past it (normal: writes since the last sync).
        assert_eq!(detect_and_reset(tmp.path(), 150, &metrics), 0);
        assert!(metrics.calls().is_empty());
        assert_eq!(read(tmp.path()), Some(150));
    }

    // FM-PERSISTENCE-035
    /// A recovery that stopped *below* the watermark dropped exactly the
    /// difference, reports it once, and then re-baselines so the next open does
    /// not re-report the same loss forever.
    #[test]
    fn a_short_recovery_reports_the_gap_once_and_re_baselines() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), 500).unwrap();
        let metrics = RecordingRecorder::default();

        assert_eq!(
            detect_and_reset(tmp.path(), 480, &metrics),
            20,
            "20 sequence numbers of acknowledged writes vanished"
        );
        assert_eq!(metrics.calls(), vec![(COUNTER.to_string(), 20)]);
        assert_eq!(
            read(tmp.path()),
            Some(480),
            "the watermark is re-baselined to what actually recovered"
        );

        // Same database, next boot: the loss is history, not news.
        assert_eq!(detect_and_reset(tmp.path(), 480, &metrics), 0);
        assert_eq!(
            metrics.calls().len(),
            1,
            "the same loss must not be re-reported on every subsequent open"
        );
    }
}
