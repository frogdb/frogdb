//! Checkpoint creation and loading for RocksDB.
use super::RocksStore;
use super::config::RocksError;
use std::path::Path;
use tracing::info;
impl RocksStore {
    pub fn create_checkpoint(&self, path: &Path) -> Result<(), RocksError> {
        let ps = path.display().to_string();
        info!(path = %ps, "Creating RocksDB checkpoint");
        let cp = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(|e| {
            tracing::error!(path = %ps, error = %e, "Checkpoint creation failed");
            RocksError::from(e)
        })?;
        cp.create_checkpoint(path).map_err(|e| {
            tracing::error!(path = %ps, error = %e, "Checkpoint creation failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn latest_sequence_number(&self) -> u64 {
        self.db.latest_sequence_number()
    }
    /// Persist `covered_seq` as the durable-sync WAL watermark (see
    /// [`super::wal_watermark`]) if it is higher than what is already
    /// recorded.
    ///
    /// `covered_seq` must be the sequence the caller snapshotted *before*
    /// starting the write or flush it is now reporting complete — never a
    /// fresh `latest_sequence_number()` read taken after the fact. A concurrent
    /// shard's still-unsynced write can land in the gap between a sync
    /// finishing and a post-hoc read, inflating the claimed durable point past
    /// what is actually on disk (FM-PERSISTENCE-035). A `fetch_max`, not a
    /// blind store: independent concurrent callers (each shard's own sync
    /// commit, and the periodic `durable_sync` tick) report out of order, and
    /// a lower snapshot arriving after a higher one must not regress the mark.
    /// Best-effort: a failed write only costs a future corruption-detection,
    /// never correctness, so it is logged at debug level and swallowed rather
    /// than propagated.
    pub fn record_wal_watermark(&self, covered_seq: u64) {
        if let Err(e) = super::wal_watermark::fetch_max(self.db.path(), covered_seq) {
            tracing::debug!(covered_seq, error = %e, "Failed to record WAL watermark");
        }
    }
    pub fn path(&self) -> &Path {
        self.db.path()
    }
    /// Open `dir` read-only through RocksDB and close it again: does this
    /// directory hold a database, all of it?
    ///
    /// The structural check in [`super::payload`] proves `CURRENT` resolves to a
    /// MANIFEST; only RocksDB can resolve that MANIFEST to the SST and blob
    /// files the current version references. Running its own reader is the
    /// difference between "the files I know to look for are there" and "the
    /// database opens", and it is deliberately *not* reimplemented here: the
    /// MANIFEST is a version-coupled `VersionEdit` log, and a hand-rolled
    /// parser's mis-reads would refuse good checkpoints.
    ///
    /// `max_open_files = -1` is load-bearing: it makes the open load a table
    /// reader for every file the version references, so a missing or truncated
    /// SST fails *here* rather than on some later read of that key range.
    ///
    /// Read-only, so no `LOCK` is taken and the payload is not mutated beyond
    /// RocksDB's own `LOG`. The merge operator is registered because the
    /// payload's CFs were written with it; the trial open reads no values, but
    /// a CF descriptor that disagrees with what wrote the data is not the thing
    /// under test here.
    pub(crate) fn trial_open_payload(dir: &Path) -> Result<(), RocksError> {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(false);
        db_opts.create_missing_column_families(false);
        db_opts.set_paranoid_checks(true);
        db_opts.set_max_open_files(-1);
        let cf_names = rocksdb::DB::list_cf(&db_opts, dir)?;
        let mut cf_opts = rocksdb::Options::default();
        cf_opts.set_merge_operator(
            "frogdb-value-merge",
            super::full_value_merge,
            super::partial_value_merge,
        );
        let descriptors = cf_names
            .into_iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(name, cf_opts.clone()));
        let db =
            rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::open_cf_descriptors_read_only(
                &db_opts,
                dir,
                descriptors,
                // A staged checkpoint legitimately carries WAL files (RocksDB's
                // checkpoint includes the live log): their presence is not an error,
                // and replaying them read-only is part of proving the payload opens.
                false,
            )?;
        drop(db);
        Ok(())
    }

    /// Install a staged full-sync checkpoint (see [`super::staged`] for the
    /// three-party contract), if one is present in `data_dir`.
    ///
    /// `data_dir` is the configured `persistence.data-dir` — the layout root,
    /// not the RocksDB directory. Everything this touches lives inside it
    /// (FM-PERSISTENCE-057).
    pub fn load_staged_checkpoint(data_dir: &Path) -> std::io::Result<bool> {
        Self::load_staged_checkpoint_with(data_dir, &crate::fs_seam::RealFs)
    }

    /// [`load_staged_checkpoint`](Self::load_staged_checkpoint) against an
    /// injectable filesystem, so the sync/rename ordering the install depends on
    /// can be asserted by a recording fake. See [`crate::fs_seam`] for the rule.
    pub(crate) fn load_staged_checkpoint_with(
        data_dir: &Path,
        fs: &dyn crate::fs_seam::SnapshotFs,
    ) -> std::io::Result<bool> {
        let layout = crate::data_dir::DataDirLayout::new(data_dir);
        let staged = super::staged::StagedCheckpoint::in_data_dir(data_dir);
        if !staged.exists() {
            return Ok(false);
        }
        info!(checkpoint_dir = %staged.dir().display(), "Found staged checkpoint, loading...");
        // Refuse to install a staged directory that is not a database this node
        // can actually open. Installing an unopenable directory moves the live
        // database aside (into `*_backup_*`) and then fails every subsequent
        // boot with the only good copy sitting beside it — and with
        // `BACKUP_RETENTION = 1` a retried sync overwrites that copy. So the
        // whole verdict is reached *before* the first rename, while nothing has
        // moved:
        //
        //   1. structure + payload manifest (`StagedCheckpoint::verify`): does
        //      `CURRENT` resolve to a real MANIFEST, and is every file the
        //      payload's own manifest lists present at its recorded size;
        //   2. trial open (`trial_open_payload`): RocksDB opens the staged
        //      directory read-only, which is what resolves the MANIFEST to the
        //      SST/blob set it references. That check belongs to the party that
        //      owns the format — a hand-rolled MANIFEST reader would be
        //      version-coupled, and its mis-parses would refuse *good*
        //      checkpoints.
        //
        // A failure here is fatal to the boot rather than a fallback to the live
        // database: the operator staged this checkpoint deliberately, and
        // quietly ignoring it would be the "restored server that isn't
        // restored" failure. The live data is intact, so the recovery is to
        // remove or re-copy `<data-dir>/staging`.
        let report = staged.verify().map_err(|e| {
            tracing::error!(checkpoint_dir = %staged.dir().display(), error = %e, "Staged checkpoint failed verification; refusing to install");
            std::io::Error::from(e)
        })?;
        if let Err(e) = Self::trial_open_payload(staged.dir()) {
            tracing::error!(checkpoint_dir = %staged.dir().display(), error = %e, "RocksDB cannot open the staged checkpoint; refusing to install");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "staged checkpoint at {} cannot be opened by RocksDB ({e}); \
                     refusing to install to avoid moving the live database aside",
                    staged.dir().display()
                ),
            ));
        }
        info!(
            checkpoint_dir = %staged.dir().display(),
            manifest = %report.manifest,
            payload_manifest = report.payload_manifest_present,
            files_checked = report.files_checked,
            "Staged checkpoint verified"
        );

        let db_dir = layout.db_dir();
        let backup_root = layout.backup_root();
        let root = layout.root();

        // The install is two renames, sequenced so that no crash point loses
        // data. Each is a single same-filesystem rename (atomic on POSIX), so
        // there is never a half-database at the live path:
        //
        //   rename 1: <data-dir>/db      -> <data-dir>/backup/db_backup_<seq>
        //                                   (only if a live db exists)
        //   rename 2: <data-dir>/staging -> <data-dir>/db
        //
        // Every operand is *inside* the data directory, which is what makes
        // both renames same-filesystem on the deployment that matters: in
        // Kubernetes the PVC is mounted at the data dir, so a rename with the
        // data dir itself as an operand fails EBUSY and a staged download
        // outside it lands on the container's ephemeral root
        // (FM-PERSISTENCE-057).
        //
        // Crash between 1 and 2: the next boot sees {no live db, staging still
        // present} and re-runs this install to completion — the previous data
        // survives in the backup and the new data in staging. Crash after 2:
        // rename 2 atomically consumed the staging directory, so the next boot
        // is a no-op; rename 2 is the commit point, and the staged replication
        // metadata rides inside the installed dir. (Both windows are pinned by
        // the crash-window tests in `rocks/tests.rs`.)
        //
        // That reasoning only holds if each rename is *durable* when the next
        // one runs, so every directory whose entries change is fsynced after
        // the change (see [`crate::fs_seam`]). Without the first sync a power
        // loss could surface rename 2 without rename 1 — the live dir replaced
        // while the backup that was supposed to hold the previous database
        // never appeared.
        if db_dir.exists() {
            // The backup root's *own* name must be durable before a database
            // is renamed into it, or a power loss can leave the backup
            // reachable from nowhere.
            std::fs::create_dir_all(&backup_root)?;
            fs.sync_dir(root)?;
            // The name comes from the persisted monotone counter, durably
            // advanced before it is used, never from the clock: a clock stepped
            // backwards would invert retention order and delete the generation
            // worth keeping, and two installs inside one second would collide
            // on the name and fail the second rename with `ENOTEMPTY`
            // (FM-PERSISTENCE-058).
            let seq = super::staged::BackupCounter::reserve_next(
                &backup_root,
                crate::data_dir::DB_DIR_NAME,
                fs,
            )?;
            let bd = backup_root.join(super::staged::backup_dir_name(
                crate::data_dir::DB_DIR_NAME,
                seq,
            ));
            info!(from = %db_dir.display(), to = %bd.display(), "Backing up existing database");
            fs.rename(&db_dir, &bd)?;
            fs.sync_dir(&backup_root)?;
            fs.sync_dir(root)?;
        }
        info!(from = %staged.dir().display(), to = %db_dir.display(), "Installing checkpoint as new database");
        fs.rename(staged.dir(), &db_dir)?;
        fs.sync_dir(root)?;

        // Post-commit, best-effort retention: keep the newest backup, delete
        // older ones. Without this, every replica full sync leaked a complete
        // database copy. Failure is logged, never propagated — retention is
        // hygiene, not worth failing a successful install over.
        match super::staged::prune_backups(
            &backup_root,
            crate::data_dir::DB_DIR_NAME,
            super::staged::BACKUP_RETENTION,
        ) {
            Ok(outcome) if outcome == Default::default() => {}
            Ok(outcome) => info!(
                removed = outcome.removed,
                refused = outcome.refused,
                "Pruned old database backups"
            ),
            Err(e) => tracing::warn!(error = %e, "Failed to prune old database backups"),
        }

        info!("Checkpoint loaded successfully");
        Ok(true)
    }
}
