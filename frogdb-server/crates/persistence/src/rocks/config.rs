//! RocksDB configuration types and error definitions.
use frogdb_config::PersistenceConfig;
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
impl RocksConfig {
    /// Build the RocksDB knob set from operator-facing persistence config.
    ///
    /// Only the seven operator-tunable knobs are read from `cfg`; the five fixed
    /// FrogDB tuning invariants (`max_background_jobs`, `create_if_missing`,
    /// `level0_file_num_compaction_trigger`, `target_file_size_base`,
    /// `max_bytes_for_level_base`) fall through to [`Default`] via
    /// `..Self::default()`. This constructor is therefore the single place the
    /// operator/invariant partition is decided, and it lives next to the
    /// `Default` impl whose literals those invariants must agree with — so the
    /// two can never silently drift.
    pub fn from_persistence(cfg: &PersistenceConfig) -> Self {
        Self {
            write_buffer_size: cfg.write_buffer_size_mb * 1024 * 1024,
            compression: CompressionType::from_config_str(&cfg.compression),
            block_cache_size: cfg.block_cache_size_mb * 1024 * 1024,
            bloom_filter_bits: cfg.bloom_filter_bits,
            max_write_buffer_number: cfg.max_write_buffer_number,
            compaction_rate_limit_mb: (cfg.compaction_rate_limit_mb > 0)
                .then_some(cfg.compaction_rate_limit_mb),
            flush_compact_range: cfg.flush_compact_range,
            ..Self::default()
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    #[default]
    Lz4,
    Zstd,
}
impl CompressionType {
    /// Parse a validated compression string (`none`/`snappy`/`lz4`/`zstd`).
    ///
    /// Contract: the caller has validated the string via
    /// `PersistenceConfig::validate`. An unknown value **falls back to the
    /// default codec** ([`CompressionType::Lz4`]) rather than panicking — a
    /// deliberate fail-soft so a mis-tuned-but-openable knob never aborts a
    /// Store-open/recovery path. The trade-off is the loss of a loud tripwire:
    /// any future construction path that skips `validate()` will silently
    /// mis-tune compression instead of failing fast (see proposal 22).
    pub fn from_config_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "none" => Self::None,
            "snappy" => Self::Snappy,
            "lz4" => Self::Lz4,
            "zstd" => Self::Zstd,
            _ => Self::default(),
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_config::PersistenceConfig;

    /// Drift guard (proposal 22): `from_persistence` on the *default*
    /// `PersistenceConfig` must reproduce `RocksConfig::default()`. The
    /// load-bearing half is the seven operator fields — if someone changes one
    /// crate's default without the other, this fails. (The five invariant fields
    /// are equal to `default()` tautologically, since `from_persistence` builds
    /// them via `..Self::default()`; they carry no guard value here.)
    #[test]
    fn from_persistence_default_equals_rocks_default() {
        let cfg = PersistenceConfig::default();
        assert_eq!(RocksConfig::from_persistence(&cfg), RocksConfig::default());
    }

    /// Non-default operator values map through correctly: MB→bytes conversions,
    /// the `compaction_rate_limit_mb > 0 ? Some : None` encoding, compression
    /// string parse, and the raw passthroughs.
    #[test]
    fn from_persistence_maps_operator_knobs() {
        let cfg = PersistenceConfig {
            write_buffer_size_mb: 32,
            compression: "zstd".to_string(),
            block_cache_size_mb: 128,
            bloom_filter_bits: 16,
            max_write_buffer_number: 6,
            compaction_rate_limit_mb: 50,
            flush_compact_range: false,
            ..PersistenceConfig::default()
        };
        let rocks = RocksConfig::from_persistence(&cfg);
        assert_eq!(rocks.write_buffer_size, 32 * 1024 * 1024);
        assert_eq!(rocks.compression, CompressionType::Zstd);
        assert_eq!(rocks.block_cache_size, 128 * 1024 * 1024);
        assert_eq!(rocks.bloom_filter_bits, 16);
        assert_eq!(rocks.max_write_buffer_number, 6);
        assert_eq!(rocks.compaction_rate_limit_mb, Some(50));
        assert!(!rocks.flush_compact_range);
        // The five fixed invariants fall through to Default.
        assert_eq!(
            rocks.level0_file_num_compaction_trigger,
            RocksConfig::default().level0_file_num_compaction_trigger
        );
        assert_eq!(rocks.create_if_missing, true);
    }

    /// `compaction_rate_limit_mb == 0` encodes to `None` (unlimited).
    #[test]
    fn from_persistence_zero_rate_limit_is_none() {
        let cfg = PersistenceConfig {
            compaction_rate_limit_mb: 0,
            ..PersistenceConfig::default()
        };
        assert_eq!(
            RocksConfig::from_persistence(&cfg).compaction_rate_limit_mb,
            None
        );
    }

    /// Every valid compression string maps to the right variant, and the
    /// fail-soft arm falls back to the default codec instead of panicking.
    #[test]
    fn from_config_str_maps_and_falls_back() {
        assert_eq!(
            CompressionType::from_config_str("none"),
            CompressionType::None
        );
        assert_eq!(
            CompressionType::from_config_str("snappy"),
            CompressionType::Snappy
        );
        assert_eq!(
            CompressionType::from_config_str("lz4"),
            CompressionType::Lz4
        );
        assert_eq!(
            CompressionType::from_config_str("zstd"),
            CompressionType::Zstd
        );
        // Case-insensitive, matching the deleted `parse_compression`.
        assert_eq!(
            CompressionType::from_config_str("ZSTD"),
            CompressionType::Zstd
        );
        // Fail-soft: an unvalidated/unknown value opens with the default codec.
        assert_eq!(
            CompressionType::from_config_str("bogus"),
            CompressionType::default()
        );
    }
}
