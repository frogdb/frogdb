//! RocksDB wrapper with column family per shard.
mod checkpoint;
pub mod columns;
pub mod config;
mod manifest;
#[cfg(test)]
mod tests;

pub use columns::{CfTier, RocksIterator};
pub use config::{CompressionType, RocksConfig, RocksError};
use manifest::ColumnFamilyManifest;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    DBWithThreadMode, MultiThreaded, Options, WriteBatch, WriteOptions,
};
use std::path::Path;
use std::sync::Arc;
use std::sync::Arc as StdArc;
use tracing::{debug, error, info};

pub struct RocksStore {
    pub(crate) db: DBWithThreadMode<MultiThreaded>,
    pub(crate) num_shards: usize,
    pub(crate) cf_names: Vec<String>,
    pub(crate) warm_enabled: bool,
    pub(crate) warm_cf_names: Vec<String>,
    pub(crate) search_meta_cf_names: Vec<String>,
}

impl RocksStore {
    pub fn open(path: &Path, num_shards: usize, config: &RocksConfig) -> Result<Self, RocksError> {
        Self::open_with_warm(path, num_shards, config, false)
    }
    pub fn open_with_warm(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
    ) -> Result<Self, RocksError> {
        let path_str = path.display().to_string();
        info!(path = %path_str, num_shards, write_buffer_size = config.write_buffer_size, "Opening RocksDB");
        let mut db_opts = Options::default();
        db_opts.create_if_missing(config.create_if_missing);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(config.max_background_jobs);
        if let Some(rate_mb) = config.compaction_rate_limit_mb {
            db_opts.set_ratelimiter(rate_mb as i64 * 1024 * 1024, 100_000, 10);
        }
        let mut block_opts = BlockBasedOptions::default();
        if config.bloom_filter_bits > 0 {
            block_opts.set_bloom_filter(config.bloom_filter_bits as f64, false);
        }
        if config.block_cache_size > 0 {
            let cache = rocksdb::Cache::new_lru_cache(config.block_cache_size);
            block_opts.set_block_cache(&cache);
        }
        block_opts.set_format_version(5);
        let mut cf_opts = Options::default();
        cf_opts.set_write_buffer_size(config.write_buffer_size);
        cf_opts.set_max_write_buffer_number(config.max_write_buffer_number);
        cf_opts.set_block_based_table_factory(&block_opts);
        cf_opts.set_compression_per_level(&[
            DBCompressionType::None,
            DBCompressionType::None,
            DBCompressionType::Lz4,
            DBCompressionType::Lz4,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
            DBCompressionType::Zstd,
        ]);
        cf_opts
            .set_level_zero_file_num_compaction_trigger(config.level0_file_num_compaction_trigger);
        cf_opts.set_target_file_size_base(config.target_file_size_base);
        cf_opts.set_max_bytes_for_level_base(config.max_bytes_for_level_base);
        let db_exists = path.exists() && path.join("CURRENT").exists();
        // A failed enumeration must not be silently coerced into an empty CF
        // list: that would bypass every reconcile invariant (the shard-count
        // and warm-tier guards both depend on a trustworthy `existing_cfs`) and
        // then ask RocksDB to open a non-empty database with an empty descriptor
        // set, surfacing as a confusing open failure. Propagate the error.
        let existing_cfs = if db_exists {
            DB::list_cf(&db_opts, path).map_err(RocksError::from)?
        } else {
            Vec::new()
        };

        // One reconciled decision: which CFs must this store open, given what is
        // persisted and what config asks for? `reconcile` owns the shard-count
        // and warm-tier invariants and returns the required set or a hard error;
        // the open path below is a transcription of `required()`.
        let manifest =
            ColumnFamilyManifest::reconcile(&path_str, &existing_cfs, num_shards, warm_enabled)?;

        let db = if db_exists {
            // Open exactly the persisted subset of the required CFs.
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = manifest
                .required()
                .filter(|cf| existing_cfs.iter().any(|e| e.as_str() == *cf))
                .map(|cf| {
                    let opts = if cf == "default" {
                        Options::default()
                    } else {
                        cf_opts.clone()
                    };
                    ColumnFamilyDescriptor::new(cf, opts)
                })
                .collect();
            let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
                &db_opts,
                path,
                cf_descriptors,
            )
            .map_err(|e| {
                error!(path = %path_str, error = %e, "Failed to open RocksDB");
                RocksError::from(e)
            })?;
            // Create any required CF not yet persisted (e.g. a first-enable of
            // the warm tier). `default` is created implicitly by RocksDB.
            for cf in manifest.required() {
                if cf != "default" && !existing_cfs.iter().any(|e| e.as_str() == cf) {
                    debug!(cf_name = %cf, "Creating column family");
                    db.create_cf(cf, &cf_opts).map_err(|e| {
                        error!(path = %path_str, error = %e, "Failed to create column family");
                        RocksError::from(e)
                    })?;
                }
            }
            db
        } else {
            let cf_descriptors: Vec<ColumnFamilyDescriptor> = manifest
                .required()
                .map(|cf| ColumnFamilyDescriptor::new(cf, cf_opts.clone()))
                .collect();
            DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, path, cf_descriptors)
                .map_err(|e| {
                error!(path = %path_str, error = %e, "Failed to open RocksDB");
                RocksError::from(e)
            })?
        };
        info!(path = %path_str, num_shards, warm_enabled, "RocksDB opened");
        Ok(Self {
            db,
            num_shards,
            cf_names: manifest.shard_names().to_vec(),
            warm_enabled,
            warm_cf_names: manifest.warm_names().to_vec(),
            search_meta_cf_names: manifest.search_meta_names().to_vec(),
        })
    }
    /// Main-tier resolver shim; see [`RocksStore::tier_cf_handle`] for the
    /// single resolver shared by all tiers.
    pub(crate) fn cf_handle(
        &self,
        shard_id: usize,
    ) -> Result<StdArc<BoundColumnFamily<'_>>, RocksError> {
        self.tier_cf_handle(CfTier::Main, shard_id)
    }
    pub fn put(&self, shard_id: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.put_tier(CfTier::Main, shard_id, key, value)
    }
    pub fn put_opt(
        &self,
        shard_id: usize,
        key: &[u8],
        value: &[u8],
        wo: &WriteOptions,
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.put_cf_opt(&cf, key, value, wo).map_err(|e| {
            error!(shard_id, error = %e, "RocksDB put failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn get(&self, shard_id: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.get_tier(CfTier::Main, shard_id, key)
    }
    pub fn delete(&self, shard_id: usize, key: &[u8]) -> Result<(), RocksError> {
        self.delete_tier(CfTier::Main, shard_id, key)
    }
    pub fn delete_opt(
        &self,
        shard_id: usize,
        key: &[u8],
        wo: &WriteOptions,
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.delete_cf_opt(&cf, key, wo).map_err(|e| {
            error!(shard_id, error = %e, "RocksDB delete failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn write_batch(&self, batch: WriteBatch) -> Result<(), RocksError> {
        self.db.write(batch).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn write_batch_opt(&self, batch: WriteBatch, wo: &WriteOptions) -> Result<(), RocksError> {
        self.db.write_opt(batch, wo).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub fn create_batch_for_shard(
        &self,
        shard_id: usize,
    ) -> Result<(WriteBatch, String), RocksError> {
        if shard_id >= self.num_shards {
            return Err(RocksError::InvalidShardId(shard_id));
        }
        Ok((WriteBatch::default(), self.cf_names[shard_id].clone()))
    }
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
    pub fn iter_cf(&self, shard_id: usize) -> Result<RocksIterator<'_>, RocksError> {
        self.iter_tier(CfTier::Main, shard_id)
    }
    pub fn flush(&self) -> Result<(), RocksError> {
        debug!(num_shards = self.num_shards, "RocksDB flush initiated");
        for sid in 0..self.num_shards {
            let cf = self.cf_handle(sid)?;
            self.db.flush_cf(&cf).map_err(|e| {
                error!(shard_id = sid, error = %e, "RocksDB flush failed");
                RocksError::from(e)
            })?;
        }
        Ok(())
    }
    pub fn has_data(&self) -> bool {
        for sid in 0..self.num_shards {
            if let Ok(cf) = self.cf_handle(sid) {
                let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                if iter.next().is_some() {
                    return true;
                }
            }
        }
        false
    }
    pub fn num_shards(&self) -> usize {
        self.num_shards
    }
    pub fn sync_wal(&self) -> Result<(), RocksError> {
        let mut fo = rocksdb::FlushOptions::default();
        fo.set_wait(true);
        for sid in 0..self.num_shards {
            let cf = self.cf_handle(sid)?;
            self.db.flush_cf_opt(&cf, &fo).map_err(|e| {
                error!(shard_id = sid, error = %e, "RocksDB flush failed");
                RocksError::from(e)
            })?;
        }
        Ok(())
    }
}
#[allow(dead_code)]
pub fn open_shared(
    path: &Path,
    num_shards: usize,
    config: &RocksConfig,
) -> Result<Arc<RocksStore>, RocksError> {
    Ok(Arc::new(RocksStore::open(path, num_shards, config)?))
}
