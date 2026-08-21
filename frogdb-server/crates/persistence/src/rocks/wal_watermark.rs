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
//! ## Body integrity (why this holds on any filesystem)
//!
//! The temp-write + rename keeps the *directory entry* atomic, but that alone
//! does not make the body atomic: rename ordering versus the data write is a
//! filesystem policy, not a POSIX guarantee. ext4's `auto_da_alloc` heuristic
//! flushes the replaced file's data before the rename commits; XFS, btrfs and
//! ext4 mounted `data=writeback` make no such promise, and FrogDB's
//! Kubernetes-primary deployment means the operator, not FrogDB, picks the
//! filesystem. A crash can therefore leave the renamed file holding a torn
//! prefix or a run of zeroes.
//!
//! So the body carries its own integrity check rather than depending on a
//! named mount option: [`FORMAT_TAG`], the decimal sequence, and an xxh3-64 of
//! the sequence text. [`read`] recomputes the digest and yields `None` on any
//! mismatch, so a torn, zero-filled, truncated or otherwise corrupt body — and
//! equally a body written by an older, unchecksummed build — is treated as *no
//! watermark* rather than as a bogus sequence. That keeps the failure mode
//! pointed the safe way on every filesystem: a missed detection, never a false
//! "you lost data" alarm.

use std::path::{Path, PathBuf};

use frogdb_types::metrics::definitions::WalRecoveryDroppedRecords;
use frogdb_types::traits::MetricsRecorder;
use tracing::{debug, warn};

/// Side-file, inside the RocksDB directory, holding the durable-sync sequence
/// watermark. The name is deliberately outside every RocksDB file pattern
/// (`*.sst`, `*.log`, `MANIFEST-*`, `CURRENT`, …) so RocksDB's obsolete-file
/// cleanup never touches it.
pub(crate) const FILE_NAME: &str = "frogdb_wal_watermark";

/// Leading token of the watermark body. Bumping it retires every previously
/// written file the safe way: an unrecognised tag reads back as `None`.
const FORMAT_TAG: &str = "frogdb-wal-watermark-v1";

/// Absolute path of the watermark file for a RocksDB directory.
fn watermark_path(db_dir: &Path) -> PathBuf {
    db_dir.join(FILE_NAME)
}

/// Digest of the decimal sequence text, as it appears in the body.
///
/// Not a cryptographic MAC: nothing here defends against a *forged* watermark
/// (an attacker with write access to the database directory owns the data
/// itself). It defends against the accident — a torn tail, a zero-filled
/// block, a truncated write — that a rename alone does not exclude.
fn digest(seq_text: &str) -> u64 {
    xxhash_rust::xxh3::xxh3_64(seq_text.as_bytes())
}

/// Serialise the on-disk body for `seq`: tag, decimal sequence, digest.
fn encode(seq: u64) -> String {
    let seq_text = seq.to_string();
    format!("{FORMAT_TAG} {seq_text} {:016x}\n", digest(&seq_text))
}

/// Parse a body written by [`encode`], or `None` if it is not intact.
fn decode(raw: &str) -> Option<u64> {
    let mut fields = raw.split_whitespace();
    let (tag, seq_text, checksum) = (fields.next()?, fields.next()?, fields.next()?);
    if fields.next().is_some() || tag != FORMAT_TAG {
        return None;
    }
    // Verify before parsing: a corrupt body must never reach the comparison in
    // `detect_and_reset`, whatever it happens to spell.
    if u64::from_str_radix(checksum, 16).ok()? != digest(seq_text) {
        return None;
    }
    seq_text.parse::<u64>().ok()
}

/// Read the persisted watermark, or `None` if absent/unreadable/corrupt.
///
/// Every failure mode collapses to `None`: a missing file (fresh database), an
/// unreadable file, an unrecognised format tag, or a body whose digest does not
/// match all mean "no trustworthy prior watermark", which suppresses detection
/// rather than risking a false alarm.
pub(crate) fn read(db_dir: &Path) -> Option<u64> {
    decode(&std::fs::read_to_string(watermark_path(db_dir)).ok()?)
}

/// Persist `seq` as the new watermark via an atomic temp-write + rename.
///
/// Best-effort: the caller treats an `Err` as non-fatal (a lost watermark only
/// costs a future detection, never correctness). Not fsync'd — see the module
/// docs on why a watermark that lags after a crash is the safe direction.
pub(crate) fn write(db_dir: &Path, seq: u64) -> std::io::Result<()> {
    let final_path = watermark_path(db_dir);
    // Same-directory temp so the rename is a single-filesystem atomic swap: the
    // directory entry flips whole. The *body* is guarded by its own checksum,
    // because whether the data lands before the rename is a filesystem policy
    // and not something FrogDB gets to assume (see the module docs).
    let tmp_path = db_dir.join(format!("{FILE_NAME}.tmp"));
    std::fs::write(&tmp_path, encode(seq))?;
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
    /// The watermark is a tagged, checksummed decimal `u64` in a file whose
    /// name no RocksDB cleanup pattern matches, and every unreadable form of it
    /// means "no watermark" rather than a bogus one.
    #[test]
    fn the_watermark_file_round_trips_and_degrades_to_none() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(read(tmp.path()), None, "a fresh database has no watermark");

        write(tmp.path(), 4_294_967_296).unwrap();
        assert_eq!(
            std::fs::read_to_string(tmp.path().join(FILE_NAME)).unwrap(),
            format!("{FORMAT_TAG} 4294967296 {:016x}\n", digest("4294967296")),
            "the body is the format tag, the decimal sequence, and its digest"
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
    /// A watermark body that did not survive the crash intact must read back as
    /// *absent*, never as a sequence.
    ///
    /// The temp-write + rename makes the directory entry atomic, but not the
    /// body: only ext4's `auto_da_alloc` heuristic orders the data write ahead
    /// of the rename, and XFS, btrfs and ext4 `data=writeback` do not. Since
    /// operators pick the filesystem, the body carries a digest instead of
    /// inheriting a mount option's promise. Every shape a half-landed write can
    /// leave behind is exercised here directly, because no filesystem this test
    /// can run on would produce them on demand.
    ///
    /// The direction matters more than the detection: a corrupt body that
    /// happened to parse as a large sequence would make `detect_and_reset`
    /// report a data loss that never occurred — the false alarm the whole
    /// watermark design exists to avoid.
    #[test]
    fn a_torn_or_corrupt_watermark_body_reads_as_absent_not_as_a_sequence() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(FILE_NAME);
        write(tmp.path(), 900).unwrap();
        let intact = std::fs::read_to_string(&path).unwrap();

        // A tail lost to a partial write: every truncation of a good body. The
        // bound stops at the trailing newline, which carries no content — a
        // body missing only that is not torn.
        for cut in 0..intact.trim_end().len() {
            std::fs::write(&path, &intact[..cut]).unwrap();
            assert_eq!(
                read(tmp.path()),
                None,
                "a body truncated to {cut} bytes must not read as a watermark"
            );
        }

        // A block the filesystem allocated but never filled.
        std::fs::write(&path, vec![0u8; intact.len()]).unwrap();
        assert_eq!(
            read(tmp.path()),
            None,
            "a zero-filled body is not a sequence"
        );

        // A digit flipped in place: same length, same shape, wrong content.
        // Without the checksum this is the false alarm — it parses cleanly as a
        // watermark 8100 sequences above the truth.
        let flipped = intact.replacen("900", "9000000", 1);
        std::fs::write(&path, &flipped).unwrap();
        assert_eq!(
            flipped.split_whitespace().nth(1),
            Some("9000000"),
            "the corruption really did land in the sequence field"
        );
        assert_eq!(
            read(tmp.path()),
            None,
            "a sequence the digest does not cover must never reach the comparison"
        );

        // A file from an older build, or a future format: unrecognised, absent.
        std::fs::write(&path, "900").unwrap();
        assert_eq!(read(tmp.path()), None, "an untagged legacy body is absent");
        std::fs::write(
            &path,
            format!("frogdb-wal-watermark-v2 900 {:016x}\n", digest("900")),
        )
        .unwrap();
        assert_eq!(read(tmp.path()), None, "an unknown format tag is absent");

        // And the intact body still reads — the guard rejects damage, not data.
        std::fs::write(&path, &intact).unwrap();
        assert_eq!(read(tmp.path()), Some(900));
    }

    // FM-PERSISTENCE-035
    /// A corrupt watermark suppresses the alarm rather than raising a false
    /// one, and the very next recovery re-baselines the file back to a body
    /// that reads.
    #[test]
    fn a_corrupt_watermark_suppresses_detection_and_re_baselines() {
        let tmp = TempDir::new().unwrap();
        let metrics = RecordingRecorder::default();
        write(tmp.path(), 500).unwrap();
        // Damage the digest, leaving a sequence far above what recovery reached.
        let corrupt = std::fs::read_to_string(tmp.path().join(FILE_NAME))
            .unwrap()
            .replacen("500", "50000", 1);
        std::fs::write(tmp.path().join(FILE_NAME), corrupt).unwrap();

        assert_eq!(
            detect_and_reset(tmp.path(), 480, &metrics),
            0,
            "an unreadable watermark is 'no watermark', not a 49520-record loss"
        );
        assert!(metrics.calls().is_empty(), "and it raises nothing at all");
        assert_eq!(
            read(tmp.path()),
            Some(480),
            "the corrupt body is replaced by a readable baseline"
        );
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
