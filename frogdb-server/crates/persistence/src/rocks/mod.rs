//! RocksDB wrapper with column family per shard.
mod checkpoint;
pub mod columns;
pub mod config;
mod manifest;
mod reclaim;
pub mod staged;
#[cfg(test)]
mod tests;

pub use columns::{CfTier, RocksIterator};
pub use config::{CompressionType, RocksConfig, RocksError};
use manifest::ColumnFamilyManifest;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DB, DBWithThreadMode,
    MergeOperands, MultiThreaded, Options, WriteBatch, WriteOptions,
};
pub use staged::StagedCheckpoint;
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
    /// Whether a FLUSHDB/FLUSHALL range tombstone is followed by an eager
    /// DeleteFilesInRange + CompactRange to reclaim disk (proposal 48). Baked
    /// from [`RocksConfig::flush_compact_range`] at open time.
    pub(crate) flush_compact_range: bool,
    /// Coalescing guard: at most one post-clear reclamation pass runs per
    /// `(tier, shard)` at a time. Shared across the flush thread (primary CF)
    /// and any other trigger site via the `Arc<RocksStore>`.
    pub(crate) reclaim_guard: reclaim::ReclaimGuard,
    /// Metrics recorder for store-initiated background work (post-clear
    /// reclamation counters). Injected at construction and immutable: a Store
    /// can never exist without a recorder, so the "no-metrics window" cannot
    /// occur by construction. Callers that legitimately want no metrics (tests,
    /// benches, tooling) pass an explicit
    /// [`NoopMetricsRecorder`](frogdb_types::traits::NoopMetricsRecorder) via the
    /// [`open`](RocksStore::open)/[`open_with_warm`](RocksStore::open_with_warm)
    /// shims; production threads the real recorder through
    /// [`open_with_warm_metrics`](RocksStore::open_with_warm_metrics).
    metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
}

impl RocksStore {
    /// Noop-metrics shim over [`open_with_warm_metrics`](Self::open_with_warm_metrics)
    /// (non-warm). The `NoopMetricsRecorder` is *explicit at the call site* — not a
    /// hidden default a later install "should" overwrite — so tests, benches, and
    /// tools opt into no metrics deliberately.
    pub fn open(path: &Path, num_shards: usize, config: &RocksConfig) -> Result<Self, RocksError> {
        Self::open_with_warm_metrics(
            path,
            num_shards,
            config,
            false,
            Arc::new(frogdb_types::traits::NoopMetricsRecorder),
        )
    }
    /// Non-warm open with an explicit metrics recorder. Symmetric to
    /// [`open`](Self::open) for callers that want the real recorder without the
    /// warm tier (e.g. the Main-tier reclamation counter test).
    pub fn open_with_metrics(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Result<Self, RocksError> {
        Self::open_with_warm_metrics(path, num_shards, config, false, metrics)
    }
    /// Noop-metrics shim over [`open_with_warm_metrics`](Self::open_with_warm_metrics).
    /// See [`open`](Self::open) on why the Noop is explicit rather than a default.
    pub fn open_with_warm(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
    ) -> Result<Self, RocksError> {
        Self::open_with_warm_metrics(
            path,
            num_shards,
            config,
            warm_enabled,
            Arc::new(frogdb_types::traits::NoopMetricsRecorder),
        )
    }
    /// Production entry point: the caller supplies the metrics recorder up front,
    /// so store-initiated background work (post-clear reclamation counters) is
    /// wired to the real recorder from the moment the Store exists. The recorder
    /// is stored immutably — there is no separate late-install step and thus no
    /// window in which reclamation counts into a no-op.
    pub fn open_with_warm_metrics(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
        metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Result<Self, RocksError> {
        // Production path: enumerate the persisted column families with the real
        // `DB::list_cf`. The enumeration is threaded through an injectable seam
        // ([`open_with_cf_lister`]) so tests can force it to fail and assert the
        // error propagates rather than being coerced into an empty CF list.
        Self::open_with_cf_lister(
            path,
            num_shards,
            config,
            warm_enabled,
            metrics,
            |opts, p| DB::list_cf(opts, p).map_err(RocksError::from),
        )
    }

    /// Reopen seam parameterised over the column-family enumerator. The public
    /// [`open_with_warm`](Self::open_with_warm) supplies the real `DB::list_cf`;
    /// tests supply a lister that fails, exercising the branch where a failed
    /// enumeration must be propagated (not swallowed into a fresh-open that
    /// bypasses the shard-count and warm-tier reopen guards).
    fn open_with_cf_lister(
        path: &Path,
        num_shards: usize,
        config: &RocksConfig,
        warm_enabled: bool,
        metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
        list_cf: impl FnOnce(&Options, &Path) -> Result<Vec<String>, RocksError>,
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
        // Honor the configured compression preset (proposal 19). Each
        // `CompressionType` maps to a curated 7-level schedule; the default
        // `Lz4` preset reproduces the historical `[None,None,Lz4,Lz4,Zstd,Zstd,Zstd]`
        // array exactly, so existing data directories keep their on-disk format.
        cf_opts.set_compression_per_level(&config.compression.per_level_schedule());
        // Value-merge operator: folds type-tagged merge operands into the base
        // value. Registered on every data CF (the bare `default` CF holds no
        // values). Currently only HyperLogLog emits merges; the operator
        // returns `None` (surfaced by RocksDB as a merge failure) on any other
        // marker or undecodable operand — see [`full_value_merge`].
        cf_opts.set_merge_operator("frogdb-value-merge", full_value_merge, partial_value_merge);
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
            list_cf(&db_opts, path)?
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
            flush_compact_range: config.flush_compact_range,
            reclaim_guard: reclaim::ReclaimGuard::new(),
            metrics,
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
    /// Single-key write into a shard's main-tier column family.
    ///
    /// Test-only convenience with **zero production callers**: production writes
    /// flow through the FrogDB WAL batch path (`RocksSink` →
    /// [`batch_put`](Self::batch_put) → [`write_batch_opt`](Self::write_batch_opt)).
    /// Exposed to dependent crates' test builds via the `test-support` feature
    /// (mirroring its sibling [`put_opt`](Self::put_opt)), absent from production
    /// builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn put(&self, shard_id: usize, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.put_tier(CfTier::Main, shard_id, key, value)
    }
    /// Single-key write with explicit [`WriteOptions`]. Test-only (no production
    /// caller); exposed to dependent crates' test builds via the `test-support`
    /// feature, absent from production builds.
    #[cfg(any(test, feature = "test-support"))]
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
    /// Apply a single merge operand to `key` via the registered value-merge
    /// operator (mirrors [`RocksStore::put`]). RocksDB folds the operand into
    /// the base value at read/compaction time.
    ///
    /// Same-crate test-only: production merges flow through the WAL batch path
    /// ([`RocksStore::batch_merge`]), never this single-key shim. Compiled only
    /// under `cfg(test)`.
    #[cfg(test)]
    pub(crate) fn merge(
        &self,
        shard_id: usize,
        key: &[u8],
        operand: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        self.db.merge_cf(&cf, key, operand).map_err(|e| {
            error!(shard_id, error = %e, "RocksDB merge failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    /// Single-key read from a shard's main-tier column family.
    ///
    /// Test-only convenience with **zero production callers**: production reads
    /// stream every key via [`iter_cf`](Self::iter_cf) during recovery. Exposed
    /// to dependent crates' test builds via the `test-support` feature, absent
    /// from production builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn get(&self, shard_id: usize, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        self.get_tier(CfTier::Main, shard_id, key)
    }
    /// Single-key delete from a shard's main-tier column family. Test-only (no
    /// production caller; production deletes flow through the WAL batch path).
    /// Exposed to dependent crates' test builds via the `test-support` feature,
    /// absent from production builds.
    #[cfg(any(test, feature = "test-support"))]
    pub fn delete(&self, shard_id: usize, key: &[u8]) -> Result<(), RocksError> {
        self.delete_tier(CfTier::Main, shard_id, key)
    }
    /// Commit a batch with default write options. Same-crate test-only: the
    /// production commit path uses [`RocksStore::write_batch_opt`]. Compiled
    /// only under `cfg(test)`.
    #[cfg(test)]
    pub(crate) fn write_batch(&self, batch: WriteBatch) -> Result<(), RocksError> {
        self.db.write(batch).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    /// Commit a staged batch with explicit [`WriteOptions`] — the production
    /// commit primitive driven by `RocksSink`. Crate-internal; the cross-crate
    /// crash tests reach the same durability path through the feature-gated
    /// [`commit_raw_batch`](Self::commit_raw_batch) seam.
    pub(crate) fn write_batch_opt(
        &self,
        batch: WriteBatch,
        wo: &WriteOptions,
    ) -> Result<(), RocksError> {
        self.db.write_opt(batch, wo).map_err(|e| {
            error!(error = %e, "RocksDB batch write failed");
            RocksError::from(e)
        })?;
        Ok(())
    }
    pub(crate) fn batch_put(
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
    pub(crate) fn batch_merge(
        &self,
        batch: &mut WriteBatch,
        shard_id: usize,
        key: &[u8],
        operand: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        batch.merge_cf(&cf, key, operand);
        Ok(())
    }
    pub(crate) fn batch_delete(
        &self,
        batch: &mut WriteBatch,
        shard_id: usize,
        key: &[u8],
    ) -> Result<(), RocksError> {
        let cf = self.cf_handle(shard_id)?;
        batch.delete_cf(&cf, key);
        Ok(())
    }

    /// Stage a full-range delete over the shard's primary column family into
    /// `batch` (FLUSHDB / FLUSHALL). One range tombstone, not O(keys) point
    /// deletes.
    ///
    /// RocksDB's `delete_range(from, to)` is `[from, to)` — the end bound is
    /// exclusive and keys are unbounded-length byte strings, so no fixed
    /// sentinel covers "every key". We instead read the current maximum key and
    /// use `max ++ [0x00]` as the exclusive upper bound: it is strictly greater
    /// than `max` (a proper prefix compares less), and nothing exceeds `max`, so
    /// `[[], max ++ [0]]` covers the whole CF in a single op. An empty CF stages
    /// nothing. Callers that need the tombstone to cover in-flight writes must
    /// ensure those writes are committed first (see
    /// [`super::wal::flush::FlushEngine::apply_clear`]).
    ///
    /// Returns the staged tombstone's exclusive upper bound, or `None` when the
    /// CF was empty (nothing staged). The caller uses the bound to trigger
    /// post-commit space reclamation ([`RocksStore::spawn_clear_reclamation`])
    /// once — and only if — the batch containing the tombstone commits.
    pub(crate) fn batch_clear_shard(
        &self,
        batch: &mut WriteBatch,
        shard_id: usize,
    ) -> Result<Option<Vec<u8>>, RocksError> {
        let cf = self.cf_handle(shard_id)?;
        let upper = self.range_delete_upper_bound(&cf);
        if let Some(upper) = &upper {
            batch.delete_range_cf(&cf, [].as_slice(), upper.as_slice());
        }
        Ok(upper)
    }

    /// Compute the exclusive upper bound for a full-CF range delete: the largest
    /// key with a `0x00` byte appended, or `None` if the CF is empty. See
    /// [`RocksStore::batch_clear_shard`] for why this covers every key.
    fn range_delete_upper_bound(&self, cf: &impl rocksdb::AsColumnFamilyRef) -> Option<Vec<u8>> {
        let mut iter = self.db.raw_iterator_cf(cf);
        iter.seek_to_last();
        iter.key().map(|max| {
            let mut upper = Vec::with_capacity(max.len() + 1);
            upper.extend_from_slice(max);
            upper.push(0);
            upper
        })
    }

    /// Delete every key in a tier's shard column family via a single range
    /// tombstone, committed directly (not batched). Used to clear the warm CF on
    /// FLUSHDB, where the shard thread is the sole writer, so no flush-pipeline
    /// ordering is required. A no-op when the CF is empty.
    ///
    /// Once the tombstone commits, an asynchronous space-reclamation pass
    /// (DeleteFilesInRange + forced-bottommost CompactRange, proposal 48) is
    /// spawned so the cleared SST bytes are returned eagerly rather than
    /// whenever compaction happens to cover the range.
    pub fn clear_tier_shard(
        self: &Arc<Self>,
        tier: CfTier,
        shard_id: usize,
    ) -> Result<(), RocksError> {
        let cf = self.tier_cf_handle(tier, shard_id)?;
        if let Some(upper) = self.range_delete_upper_bound(&cf) {
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&cf, [].as_slice(), upper.as_slice());
            self.db.write(batch).map_err(|e| {
                error!(shard_id, tier = ?tier, error = %e, "RocksDB range clear failed");
                RocksError::from(e)
            })?;
            drop(cf);
            self.spawn_clear_reclamation(tier, shard_id, upper);
        }
        Ok(())
    }

    /// Trigger an asynchronous post-clear space-reclamation pass over one
    /// column family (see [`reclaim`]). `upper_bound` is the committed range
    /// tombstone's exclusive upper bound as returned by
    /// [`RocksStore::batch_clear_shard`]. Coalesced: a second call for the same
    /// `(tier, shard)` while a pass is in flight is dropped, not queued.
    pub(crate) fn spawn_clear_reclamation(
        self: &Arc<Self>,
        tier: CfTier,
        shard_id: usize,
        upper_bound: Vec<u8>,
    ) {
        reclaim::spawn_clear_reclamation(Arc::clone(self), tier, shard_id, upper_bound);
    }

    /// The metrics recorder injected at construction, used by store-initiated
    /// background work (post-clear reclamation counters). Immutable — cloned per
    /// reclamation pass without any lock.
    pub(crate) fn metrics_recorder(&self) -> Arc<dyn frogdb_types::traits::MetricsRecorder> {
        Arc::clone(&self.metrics)
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
    /// Flush every shard's memtable with `wait = true`. Test-only (the
    /// crash-recovery harness forces a durable point); exposed to dependent
    /// crates' test builds via the `test-support` feature.
    #[cfg(any(test, feature = "test-support"))]
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

/// A single put/delete operation for [`RocksStore::commit_raw_batch`], each
/// carrying its own target shard so one atomic batch can span multiple shards.
///
/// The per-op `shard` (rather than a batch-level parameter) is load-bearing:
/// the crash-recovery `test_cross_shard_batch` stages writes to several shards
/// into one `WriteBatch` to verify cross-shard all-or-nothing atomicity, which
/// a single batch-level shard could not express.
#[cfg(any(test, feature = "test-support"))]
pub enum RawBatchOp<'a> {
    Put {
        shard: usize,
        key: &'a [u8],
        value: &'a [u8],
    },
    Delete {
        shard: usize,
        key: &'a [u8],
    },
}

#[cfg(any(test, feature = "test-support"))]
impl RocksStore {
    /// Stage a batch of raw put/delete ops — each carrying its own shard — and
    /// commit them atomically with the given [`WriteOptions`], bypassing the
    /// FrogDB WAL. The one named cross-crate seam the crash-atomicity tests
    /// drive, replacing open-coded `WriteBatch`/`batch_put`/`write_batch_opt`
    /// so no `rocksdb::WriteBatch` type crosses the `mod.rs` interface.
    pub fn commit_raw_batch(
        &self,
        ops: &[RawBatchOp<'_>],
        wo: &WriteOptions,
    ) -> Result<(), RocksError> {
        let mut batch = WriteBatch::default();
        for op in ops {
            match op {
                RawBatchOp::Put { shard, key, value } => {
                    self.batch_put(&mut batch, *shard, key, value)?
                }
                RawBatchOp::Delete { shard, key } => self.batch_delete(&mut batch, *shard, key)?,
            }
        }
        self.write_batch_opt(batch, wo)
    }
}
/// Full RocksDB merge callback: fold every operand onto `existing` in order.
/// Delegates to [`crate::merge_hll_serialized`], which returns `None` (a merge
/// failure to RocksDB) only on a non-HLL marker or undecodable input — never on
/// a well-formed empty fold.
fn full_value_merge(
    _key: &[u8],
    existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let ops: Vec<&[u8]> = operands.iter().collect();
    crate::merge_hll_serialized(existing, &ops)
}

/// Partial RocksDB merge callback: combine several operands (no base) into one.
/// Delegates to [`crate::partial_merge_hll_deltas`], which returns `None` — so
/// RocksDB falls back to the full merge — when any operand is a full value or
/// undecodable.
fn partial_value_merge(
    _key: &[u8],
    _existing: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let ops: Vec<&[u8]> = operands.iter().collect();
    crate::partial_merge_hll_deltas(&ops)
}
