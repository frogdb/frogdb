//! RocksDB wrapper with column family per shard.
//!
//! Each shard gets its own column family to allow independent compaction
//! and potential future optimizations like shard-specific tuning.

use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBCompressionType, DBWithThreadMode, MultiThreaded,
    Options, WriteBatch, WriteOptions, DB,
};
use std::path::Path;
use std::sync::Arc as StdArc;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};

/// Errors that can occur with RocksDB operations.
#[derive(Debug, Error)]
pub enum RocksError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),

    #[error("Invalid shard ID: {0}")]
    InvalidShardId(usize),
}

/// Configuration for RocksDB.
#[derive(Debug, Clone)]
pub struct RocksConfig {
    /// Write buffer size in bytes (default: 64MB).
    pub write_buffer_size: usize,

    /// Compression type (default: LZ4).
    pub compression: CompressionType,

    /// Maximum background jobs (default: num_cpus).
    pub max_background_jobs: i32,

    /// Whether to create the database if it doesn't exist (default: true).
    pub create_if_missing: bool,
}

impl Default for RocksConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            compression: CompressionType::Lz4,
            max_background_jobs: num_cpus::get() as i32,
            create_if_missing: true,
        }
    }
}

/// Compression type for RocksDB.
#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionType {
    /// No compression.
    None,
    /// Snappy compression (fast, moderate compression).
    Snappy,
    /// LZ4 compression (fast, good compression).
    #[default]
    Lz4,
    /// Zstd compression (slower, best compression).
    Zstd,
}

impl CompressionType {
    fn to_rocksdb(self) -> DBCompressionType {
        match self {
            CompressionType::None => DBCompressionType::None,
            CompressionType::Snappy => DBCompressionType::Snappy,
            CompressionType::Lz4 => DBCompressionType::Lz4,
            CompressionType::Zstd => DBCompressionType::Zstd,
        }
    }
}

/// RocksDB store with column family per shard.
pub struct RocksStore {
    db: DBWithThreadMode<MultiThreaded>,
    num_shards: usize,
}

impl RocksStore {
    /// Open or create a RocksDB database at the given path.
    ///
    /// Creates one column family per shard named "shard_0", "shard_1", etc.
    pub fn open(path: &Path, num_shards: usize, config: &RocksConfig) -> Result<Self, RocksError> {
        let path_str = path.display().to_string();
        info!(
            path = %path_str,
            num_shards,
            write_buffer_size = config.write_buffer_size,
            "Opening RocksDB"
        );

        let mut db_opts = Options::default();
        db_opts.create_if_missing(config.create_if_missing);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(config.max_background_jobs);

        // Configure options for each column family
        let mut cf_opts = Options::default();
        cf_opts.set_write_buffer_size(config.write_buffer_size);
        cf_opts.set_compression_type(config.compression.to_rocksdb());

        // Build column family descriptors
        let cf_names: Vec<String> = (0..num_shards).map(|i| format!("shard_{}", i)).collect();

        // Check if database exists
        let db_exists = path.exists() && path.join("CURRENT").exists();

        let db = if db_exists {
            // Get existing column families
            let existing_cfs = DB::list_cf(&db_opts, path).unwrap_or_default();

            // Build descriptors for all column families
            let mut cf_descriptors: Vec<ColumnFamilyDescriptor> = Vec::new();

            // Always include the default CF
            if existing_cfs.contains(&"default".to_string()) {
                cf_descriptors.push(ColumnFamilyDescriptor::new("default", Options::default()));
            }

            // Add shard CFs
            for cf_name in &cf_names {
                if existing_cfs.contains(cf_name) {
                    cf_descriptors.push(ColumnFamilyDescriptor::new(cf_name, cf_opts.clone()));
                }
            }

            // Open with existing CFs
            let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
                &db_opts,
                path,
                cf_descriptors,
            )
            .map_err(|e| {
                error!(path = %path_str, error = %e, "Failed to open RocksDB");
                RocksError::from(e)
            })?;

            // Create any missing shard CFs
            // Note: We need to iterate and create each missing CF
            // This is a workaround since open_cf_descriptors doesn't create missing CFs
            for cf_name in &cf_names {
                if !existing_cfs.contains(cf_name) {
                    debug!(cf_name = %cf_name, "Creating column family");
                    db.create_cf(cf_name, &cf_opts).map_err(|e| {
                        error!(path = %path_str, error = %e, "Failed to open RocksDB");
                        RocksError::from(e)
                    })?;
                }
            }

            db
        } else {
            // Create new database with all column families
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = cf_names
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name, cf_opts.clone()))
                .collect();

            DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, path, cf_descriptors)
                .map_err(|e| {
                error!(path = %path_str, error = %e, "Failed to open RocksDB");
                RocksError::from(e)
            })?
        };

        info!(path = %path_str, num_shards, "RocksDB opened");
        Ok(Self { db, num_shards })
    }

    /// Get the column family handle for a shard.
    fn cf_handle(&self, shard_id: usize) -> Result<StdArc<BoundColumnFamily<'_>>, RocksError> {
        if shard_id >= self.num_shards {
            return Err(RocksError::InvalidShardId(shard_id));
        }

        let cf_name = format!("shard_{}", shard_id);
        self.db
            .cf_handle(&cf_name)
            .ok_or(RocksError::ColumnFamilyNotFound(cf_name))
    }

    /// Put a key-value pair into a shard.
    pub fn put(&self, shard_id: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.put_cf(&cf, key, value).map_err(|e| {
            error!(shard_id, key_len = key.len(), error = %e, "RocksDB put failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Put a key-value pair with specific write options.
    pub fn put_opt(
        &self,
        shard_id: usize,
        key: &[u8],
        value: &[u8],
        write_opts: &WriteOptions,
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db
            .put_cf_opt(&cf, key, value, write_opts)
            .map_err(|e| {
                error!(shard_id, key_len = key.len(), error = %e, "RocksDB put failed");
                RocksError::from(e)
            })?;
        Ok(())
    }

    /// Get a value from a shard.
    pub fn get(&self, shard_id: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let cf = self.cf_handle(shard_id)?;
        Ok(self.db.get_cf(&cf, key)?)
    }

    /// Delete a key from a shard.
    pub fn delete(&self, shard_id: usize, key: &[u8]) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.delete_cf(&cf, key).map_err(|e| {
            error!(shard_id, key_len = key.len(), error = %e, "RocksDB delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Delete a key with specific write options.
    pub fn delete_opt(
        &self,
        shard_id: usize,
        key: &[u8],
        write_opts: &WriteOptions,
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.delete_cf_opt(&cf, key, write_opts).map_err(|e| {
            error!(shard_id, key_len = key.len(), error = %e, "RocksDB delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Write a batch of operations to a shard.
    pub fn write_batch(&self, batch: WriteBatch) -> Result<(), RocksError> {
        self.db.write(batch).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Write a batch with specific write options.
    pub fn write_batch_opt(
        &self,
        batch: WriteBatch,
        write_opts: &WriteOptions,
    ) -> Result<(), RocksError> {
        self.db.write_opt(batch, write_opts).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Create a write batch for a shard.
    ///
    /// Returns a WriteBatch and the column family name for use with batch operations.
    pub fn create_batch_for_shard(
        &self,
        shard_id: usize,
    ) -> Result<(WriteBatch, String), RocksError> {
        if shard_id >= self.num_shards {
            return Err(RocksError::InvalidShardId(shard_id));
        }
        Ok((WriteBatch::default(), format!("shard_{}", shard_id)))
    }

    /// Put to a write batch.
    pub fn batch_put(
        &self,
        batch: &mut WriteBatch,
        shard_id: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        batch.put_cf(&cf, key, value);
        Ok(())
    }

    /// Delete from a write batch.
    pub fn batch_delete(
        &self,
        batch: &mut WriteBatch,
        shard_id: usize,
        key: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        batch.delete_cf(&cf, key);
        Ok(())
    }

    /// Iterate over all key-value pairs in a shard.
    pub fn iter_cf(&self, shard_id: usize) -> Result<RocksIterator<'_>, RocksError> {
        let cf = self.cf_handle(shard_id)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(RocksIterator { inner: iter })
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) -> Result<(), RocksError> {
        debug!(num_shards = self.num_shards, "RocksDB flush initiated");
        for shard_id in 0..self.num_shards {
            let cf = self.cf_handle(shard_id)?;
            self.db.flush_cf(&cf).map_err(|e| {
                error!(shard_id, error = %e, "RocksDB flush failed");
                RocksError::from(e)
            })?;
        }
        Ok(())
    }

    /// Check if the database has any data.
    pub fn has_data(&self) -> bool {
        for shard_id in 0..self.num_shards {
            if let Ok(cf) = self.cf_handle(shard_id) {
                let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                if iter.next().is_some() {
                    return true;
                }
            }
        }
        false
    }

    /// Get the number of shards.
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }

    /// Sync WAL to disk.
    pub fn sync_wal(&self) -> Result<(), RocksError> {
        // RocksDB doesn't have a direct sync_wal method in the Rust bindings,
        // but flushing with sync=true achieves the same effect.
        // For more granular control, we use FlushOptions.
        let mut flush_opts = rocksdb::FlushOptions::default();
        flush_opts.set_wait(true);

        for shard_id in 0..self.num_shards {
            let cf = self.cf_handle(shard_id)?;
            self.db.flush_cf_opt(&cf, &flush_opts).map_err(|e| {
                error!(shard_id, error = %e, "RocksDB flush failed");
                RocksError::from(e)
            })?;
        }
        Ok(())
    }

    /// Create a checkpoint (point-in-time snapshot) at the given path.
    ///
    /// NOTE: This uses RocksDB's Checkpoint API which creates hard links to
    /// immutable SST files, making it very fast and space-efficient.
    /// Must be called from a spawn_blocking context as Checkpoint is !Send.
    pub fn create_checkpoint(&self, path: &Path) -> Result<(), RocksError> {
        let path_str = path.display().to_string();
        info!(path = %path_str, "Creating RocksDB checkpoint");
        let checkpoint = rocksdb::checkpoint::Checkpoint::new(&self.db).map_err(|e| {
            error!(path = %path_str, error = %e, "Checkpoint creation failed");
            RocksError::from(e)
        })?;
        checkpoint.create_checkpoint(path).map_err(|e| {
            error!(path = %path_str, error = %e, "Checkpoint creation failed");
            RocksError::from(e)
        })?;
        Ok(())
    }

    /// Get the latest RocksDB sequence number.
    ///
    /// This number increases monotonically with each write operation and can
    /// be used to track the exact point-in-time of a snapshot.
    pub fn latest_sequence_number(&self) -> u64 {
        self.db.latest_sequence_number()
    }

    /// Get the path to the RocksDB database directory.
    pub fn path(&self) -> &Path {
        self.db.path()
    }

    /// Load a staged checkpoint if one exists.
    ///
    /// This is called during server startup to check for a replica checkpoint
    /// that was received during full sync. If found, it replaces the current
    /// database with the checkpoint data.
    ///
    /// The checkpoint is expected at `{rocksdb_dir}/../checkpoint_ready` (sibling directory).
    ///
    /// # Arguments
    ///
    /// * `rocksdb_dir` - The RocksDB database directory
    ///
    /// # Returns
    ///
    /// Returns `true` if a checkpoint was loaded, `false` otherwise.
    pub fn load_staged_checkpoint(rocksdb_dir: &Path) -> std::io::Result<bool> {
        let parent_dir = match rocksdb_dir.parent() {
            Some(p) => p,
            None => return Ok(false),
        };

        let checkpoint_ready_dir = parent_dir.join("checkpoint_ready");

        if !checkpoint_ready_dir.exists() {
            return Ok(false);
        }

        info!(
            checkpoint_dir = %checkpoint_ready_dir.display(),
            "Found staged checkpoint, loading..."
        );

        // Back up existing database if it exists
        if rocksdb_dir.exists() {
            let backup_dir = parent_dir.join(format!(
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
            info!(
                from = %rocksdb_dir.display(),
                to = %backup_dir.display(),
                "Backing up existing database"
            );
            std::fs::rename(rocksdb_dir, &backup_dir)?;
        }

        // Move checkpoint to the RocksDB directory
        info!(
            from = %checkpoint_ready_dir.display(),
            to = %rocksdb_dir.display(),
            "Installing checkpoint as new database"
        );
        std::fs::rename(&checkpoint_ready_dir, rocksdb_dir)?;

        info!("Checkpoint loaded successfully");
        Ok(true)
    }
}

/// Iterator over RocksDB key-value pairs.
pub struct RocksIterator<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>,
}

impl<'a> Iterator for RocksIterator<'a> {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(|result| result.ok())
    }
}

/// Helper function to get number of CPUs.
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }
}

/// Create a RocksStore wrapped in Arc for sharing across threads.
#[allow(dead_code)]
pub fn open_shared(
    path: &Path,
    num_shards: usize,
    config: &RocksConfig,
) -> Result<Arc<RocksStore>, RocksError> {
    Ok(Arc::new(RocksStore::open(path, num_shards, config)?))
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_open_and_write() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 4, &RocksConfig::default()).unwrap();

        store.put(0, b"key1", b"value1").unwrap();
        assert_eq!(store.get(0, b"key1").unwrap(), Some(b"value1".to_vec()));

        store.put(3, b"key2", b"value2").unwrap();
        assert_eq!(store.get(3, b"key2").unwrap(), Some(b"value2".to_vec()));

        // Key doesn't exist in different shard
        assert_eq!(store.get(1, b"key1").unwrap(), None);
    }

    #[test]
    fn test_delete() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        store.put(0, b"key", b"value").unwrap();
        assert!(store.get(0, b"key").unwrap().is_some());

        store.delete(0, b"key").unwrap();
        assert!(store.get(0, b"key").unwrap().is_none());
    }

    #[test]
    fn test_write_batch() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        let mut batch = WriteBatch::default();
        store.batch_put(&mut batch, 0, b"k1", b"v1").unwrap();
        store.batch_put(&mut batch, 0, b"k2", b"v2").unwrap();
        store.batch_put(&mut batch, 1, b"k3", b"v3").unwrap();
        store.write_batch(batch).unwrap();

        assert_eq!(store.get(0, b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get(0, b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get(1, b"k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_iterate() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        store.put(0, b"a", b"1").unwrap();
        store.put(0, b"b", b"2").unwrap();
        store.put(0, b"c", b"3").unwrap();

        let items: Vec<_> = store.iter_cf(0).unwrap().collect();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_has_data() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        assert!(!store.has_data());

        store.put(0, b"key", b"value").unwrap();
        assert!(store.has_data());
    }

    #[test]
    fn test_reopen() {
        let tmp = TempDir::new().unwrap();

        {
            let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();
            store.put(0, b"persist", b"data").unwrap();
        }

        // Reopen
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();
        assert_eq!(store.get(0, b"persist").unwrap(), Some(b"data".to_vec()));
    }

    #[test]
    fn test_invalid_shard() {
        let tmp = TempDir::new().unwrap();
        let store = RocksStore::open(tmp.path(), 2, &RocksConfig::default()).unwrap();

        let result = store.put(5, b"key", b"value");
        assert!(matches!(result, Err(RocksError::InvalidShardId(5))));
    }
}
