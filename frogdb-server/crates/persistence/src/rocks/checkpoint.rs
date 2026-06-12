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
    pub fn path(&self) -> &Path {
        self.db.path()
    }
    pub fn load_staged_checkpoint(rocksdb_dir: &Path) -> std::io::Result<bool> {
        let pd = match rocksdb_dir.parent() {
            Some(p) => p,
            None => return Ok(false),
        };
        let crd = pd.join("checkpoint_ready");
        if !crd.exists() {
            return Ok(false);
        }
        info!(checkpoint_dir = %crd.display(), "Found staged checkpoint, loading...");
        // Refuse to install a staged directory that is not a complete RocksDB
        // database. Every valid checkpoint carries a `CURRENT` manifest pointer —
        // the same marker `RocksStore::open` uses to detect an existing db.
        // Installing an incomplete directory would move the live database aside
        // (into `*_backup_*`) and then open a fresh empty db in its place, which
        // is silent data loss. Validate *before* touching the live dir so the
        // original data is left untouched on refusal.
        if !crd.join("CURRENT").exists() {
            tracing::error!(checkpoint_dir = %crd.display(), "Staged checkpoint is missing its CURRENT manifest; refusing to install");
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "staged checkpoint at {} is incomplete (missing CURRENT manifest); \
                     refusing to install to avoid moving the live database aside",
                    crd.display()
                ),
            ));
        }
        if rocksdb_dir.exists() {
            let bd = pd.join(format!(
                "{}_backup_{}",
                rocksdb_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("db"),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            ));
            info!(from = %rocksdb_dir.display(), to = %bd.display(), "Backing up existing database");
            std::fs::rename(rocksdb_dir, &bd)?;
        }
        info!(from = %crd.display(), to = %rocksdb_dir.display(), "Installing checkpoint as new database");
        std::fs::rename(&crd, rocksdb_dir)?;
        info!("Checkpoint loaded successfully");
        Ok(true)
    }
}
