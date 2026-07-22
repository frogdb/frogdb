//! RocksDB configuration types and error definitions.
use rocksdb::DBCompressionType;
use thiserror::Error;
#[derive(Debug, Error)]
pub enum RocksError {
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),
    #[error("Column family not found: {0}")]
    ColumnFamilyNotFound(String),
    #[error("Invalid shard ID: {0}")]
    InvalidShardId(usize),
    #[error(
        "shard count mismatch: data directory {path} was written with {persisted} shard(s) but \
         the server is configured for {configured} shard(s); refusing to start to avoid silently \
         dropping or misrouting persisted data (restart with num_shards = {persisted}, or migrate \
         the data directory to the new shard count)"
    )]
    ShardCountMismatch {
        path: String,
        persisted: usize,
        configured: usize,
    },
    #[error(
        "warm-tier mismatch: data directory {path} was written with the warm tier (tiered \
         storage) enabled, but the server is configured with it disabled; refusing to start \
         because the persisted tiered_warm_* column families would be left unopened (re-enable \
         tiered-storage.enabled, or migrate the warm tier out before disabling it)"
    )]
    WarmTierMismatch { path: String },
}
#[derive(Debug, Clone)]
pub struct RocksConfig {
    pub write_buffer_size: usize,
    pub compression: CompressionType,
    pub max_background_jobs: i32,
    pub create_if_missing: bool,
    pub block_cache_size: usize,
    pub bloom_filter_bits: i32,
    pub max_write_buffer_number: i32,
    pub level0_file_num_compaction_trigger: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub compaction_rate_limit_mb: Option<u64>,
    /// Follow a FLUSHDB/FLUSHALL range tombstone with an eager, asynchronous
    /// DeleteFilesInRange + CompactRange over the cleared column family so SST
    /// bytes are reclaimed immediately instead of waiting for a compaction to
    /// happen to cover the range (proposal 48). Default on — this is what
    /// Kvrocks does unconditionally after its FLUSHDB DeleteRange.
    pub flush_compact_range: bool,
}
impl Default for RocksConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024 * 1024,
            compression: CompressionType::Lz4,
            max_background_jobs: num_cpus::get() as i32,
            create_if_missing: true,
            block_cache_size: 256 * 1024 * 1024,
            bloom_filter_bits: 10,
            max_write_buffer_number: 4,
            level0_file_num_compaction_trigger: 8,
            target_file_size_base: 128 * 1024 * 1024,
            max_bytes_for_level_base: 512 * 1024 * 1024,
            compaction_rate_limit_mb: None,
            flush_compact_range: true,
        }
    }
}
#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionType {
    None,
    Snappy,
    #[default]
    Lz4,
    Zstd,
}
impl CompressionType {
    /// Translate a single codec variant to its RocksDB type. This is the
    /// per-cell helper the curated [`per_level_schedule`](Self::per_level_schedule)
    /// table is built from; it is not a standalone knob-honoring path on its own.
    pub(crate) fn to_rocksdb(self) -> DBCompressionType {
        match self {
            Self::None => DBCompressionType::None,
            Self::Snappy => DBCompressionType::Snappy,
            Self::Lz4 => DBCompressionType::Lz4,
            Self::Zstd => DBCompressionType::Zstd,
        }
    }

    /// Curated per-level schedule for a 7-level column family. Each variant is an
    /// explicit operator-facing *preset*, not a uniform single-codec fill:
    ///
    /// ```text
    ///   None   => [None,  None,  None,  None,  None,  None,  None ]  // compression off
    ///   Lz4    => [None,  None,  Lz4,   Lz4,   Zstd,  Zstd,  Zstd ]  // balanced default (historical)
    ///   Zstd   => [None,  None,  Zstd,  Zstd,  Zstd,  Zstd,  Zstd ]  // max ratio
    ///   Snappy => [None,  None,  Snappy,Snappy,Snappy,Snappy,Snappy] // Snappy tail
    /// ```
    ///
    /// Shallow L0/L1 stay uncompressed in every non-`None` preset to protect
    /// write/read latency; only `None` compresses nothing.
    ///
    /// Note the deliberate asymmetry: because the default `Lz4` preset must
    /// reproduce the historical two-codec array exactly (no on-disk format
    /// drift), it is a *balanced* mixed Lz4/Zstd schedule — not pure Lz4 — while
    /// `Zstd`/`Snappy` are uniform tails. This is documented as the curated-preset
    /// model's cost in the config reference.
    pub(crate) fn per_level_schedule(self) -> [DBCompressionType; 7] {
        use DBCompressionType as D;
        let tail = self.to_rocksdb();
        match self {
            // Balanced default: warm levels Lz4, deep levels Zstd. This row *is*
            // the historical hard-coded schedule; keeping it here means the
            // default on-disk format is pinned by the preset table itself.
            Self::Lz4 => [D::None, D::None, D::Lz4, D::Lz4, D::Zstd, D::Zstd, D::Zstd],
            // Uniform-tail presets: shallow L0/L1 uncompressed, everything below
            // the configured codec (or nothing for `None`).
            Self::None | Self::Snappy | Self::Zstd => {
                [D::None, D::None, tail, tail, tail, tail, tail]
            }
        }
    }
}
pub(crate) mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }
}
