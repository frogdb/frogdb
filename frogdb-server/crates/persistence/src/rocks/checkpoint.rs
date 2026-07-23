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
    /// Persist the current RocksDB sequence number as the durable-sync WAL
    /// watermark (see [`super::wal_watermark`]). Call this *after* a durable
    /// sync — the sync flush path and graceful shutdown — so the recorded
    /// sequence reflects data that survived to disk. Best-effort: a failed write
    /// only costs a future corruption-detection, never correctness, so it is
    /// logged at debug level and swallowed rather than propagated.
    pub fn record_wal_watermark(&self) {
        let seq = self.db.latest_sequence_number();
        if let Err(e) = super::wal_watermark::write(self.db.path(), seq) {
            tracing::debug!(seq, error = %e, "Failed to record WAL watermark");
        }
    }
    pub fn path(&self) -> &Path {
        self.db.path()
    }
    /// Install a staged full-sync checkpoint (see [`super::staged`] for the
    /// three-party contract), if one is present next to `rocksdb_dir`.
    pub fn load_staged_checkpoint(rocksdb_dir: &Path) -> std::io::Result<bool> {
        let staged = match super::staged::StagedCheckpoint::for_db_dir(rocksdb_dir) {
            Some(s) => s,
            None => return Ok(false),
        };
        if !staged.exists() {
            return Ok(false);
        }
        info!(checkpoint_dir = %staged.dir().display(), "Found staged checkpoint, loading...");
        // Refuse to install a staged directory that is not a complete RocksDB
        // database. Every valid checkpoint carries a `CURRENT` manifest pointer —
        // the same marker `RocksStore::open` uses to detect an existing db.
        // Installing an incomplete directory would move the live database aside
        // (into `*_backup_*`) and then open a fresh empty db in its place, which
        // is silent data loss. Validate *before* touching the live dir so the
        // original data is left untouched on refusal.
        if !staged.is_complete_db() {
            tracing::error!(checkpoint_dir = %staged.dir().display(), "Staged checkpoint is missing its CURRENT manifest; refusing to install");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "staged checkpoint at {} is incomplete (missing CURRENT manifest); \
                     refusing to install to avoid moving the live database aside",
                    staged.dir().display()
                ),
            ));
        }

        let db_name = rocksdb_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("db");
        let parent = rocksdb_dir.parent().expect("for_db_dir returned Some");

        // The install is two renames, sequenced so that no crash point loses
        // data. Each is a single same-filesystem rename (atomic on POSIX), so
        // there is never a half-database at the live path:
        //
        //   rename 1: <db>              -> <db>_backup_<ts>  (only if a live db exists)
        //   rename 2: checkpoint_ready/ -> <db>
        //
        // Crash between 1 and 2: the next boot sees {no live db,
        // checkpoint_ready still present} and re-runs this install to
        // completion — the previous data survives in the backup and the new
        // data in checkpoint_ready. Crash after 2: rename 2 atomically
        // consumed the staging marker, so the next boot is a no-op; rename 2
        // is the commit point, and the staged replication metadata rides
        // inside the installed dir. (Both windows are pinned by the
        // crash-window tests in `rocks/tests.rs`.)
        if rocksdb_dir.exists() {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let bd = parent.join(super::staged::backup_dir_name(db_name, ts));
            info!(from = %rocksdb_dir.display(), to = %bd.display(), "Backing up existing database");
            std::fs::rename(rocksdb_dir, &bd)?;
        }
        info!(from = %staged.dir().display(), to = %rocksdb_dir.display(), "Installing checkpoint as new database");
        std::fs::rename(staged.dir(), rocksdb_dir)?;

        // Post-commit, best-effort retention: keep the newest backup, delete
        // older ones. Without this, every replica full sync leaked a complete
        // database copy. Failure is logged, never propagated — retention is
        // hygiene, not worth failing a successful install over.
        match super::staged::prune_backups(parent, db_name, super::staged::BACKUP_RETENTION) {
            Ok(0) => {}
            Ok(n) => info!(removed = n, "Pruned old database backups"),
            Err(e) => tracing::warn!(error = %e, "Failed to prune old database backups"),
        }

        info!("Checkpoint loaded successfully");
        Ok(true)
    }
}
