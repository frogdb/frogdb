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

    /// The compaction rate limit as RocksDB wants it: bytes per second, or
    /// `None` for unlimited.
    ///
    /// The MB→bytes conversion lives here rather than inline at the `Options`
    /// call site because `rocksdb::Options` is write-only — nothing can read
    /// back the rate it was handed, so a wrong conversion at the call site is
    /// unobservable. As a named function it is asserted directly.
    pub(crate) fn compaction_rate_limit_bytes_per_sec(&self) -> Option<i64> {
        self.compaction_rate_limit_mb
            .map(|mb| mb as i64 * 1024 * 1024)
    }

    /// The process-wide memtable ceiling: what the single `WriteBufferManager`
    /// is created with (FM-PERSISTENCE-060).
    ///
    /// `write_buffer_size` is the per-column-family *flush trigger* — how large
    /// one memtable grows before it rolls — and `max_write_buffer_number` is how
    /// many of those may be live at once before writes wait. Their product used
    /// to be paid *per column family*, i.e. multiplied by shards and by tiers;
    /// as a manager budget it is paid once for the whole process.
    ///
    /// Saturating, because the two knobs are operator input and a 64-bit
    /// overflow here would wrap to a tiny budget — the one arithmetic mistake
    /// that turns a memory ceiling into a permanent write stall.
    pub fn memtable_budget_bytes(&self) -> usize {
        self.write_buffer_size
            .saturating_mul(self.max_write_buffer_number.max(1) as usize)
    }

    /// The single number the persistence `Budget` is opened with, and the
    /// capacity of the shared block cache (FM-PERSISTENCE-061): the *sum* of the
    /// two operator knobs.
    ///
    /// It is a sum rather than `block_cache_size` alone because memtable bytes
    /// are costed to the same cache (`new_write_buffer_manager_with_cache`), so
    /// a capacity of `block_cache_size` would quietly shrink the read cache by
    /// the memtable budget as writes arrived.
    pub fn memory_budget_bytes(&self) -> usize {
        self.block_cache_size
            .saturating_add(self.memtable_budget_bytes())
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
            // Redundant with the fallback arm — `Self::default()` is `Lz4` — but
            // kept explicit so the default can move without silently retiring the
            // name operators actually write in their config files.
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

    /// The memtable ceiling is the write-buffer *product*: one column family may
    /// hold `max_write_buffer_number` buffers of `write_buffer_size` before it
    /// stalls, and that product — not the per-CF size alone, and not anything
    /// scaled by the column-family count — is what the process-wide
    /// `WriteBufferManager` is built with.
    // FM-PERSISTENCE-060
    #[test]
    fn the_memtable_budget_is_the_write_buffer_product() {
        let cfg = RocksConfig {
            write_buffer_size: 8 * 1024 * 1024,
            max_write_buffer_number: 3,
            ..RocksConfig::default()
        };
        assert_eq!(cfg.memtable_budget_bytes(), 24 * 1024 * 1024);

        // `max_write_buffer_number: 0` is not a RocksDB-default escape hatch the
        // way `block_cache_size: 0` is — it would zero the whole budget and stall
        // every write. Floor it at one buffer.
        let none = RocksConfig {
            write_buffer_size: 8 * 1024 * 1024,
            max_write_buffer_number: 0,
            ..RocksConfig::default()
        };
        assert_eq!(none.memtable_budget_bytes(), 8 * 1024 * 1024);
    }

    /// The process memory budget is the *sum* of the two operator knobs — the
    /// block cache plus the memtable product — so that one number bounds both
    /// halves of the engine's footprint and can be charged to one `Budget`.
    // FM-PERSISTENCE-061
    #[test]
    fn the_memory_budget_is_the_sum_of_the_two_operator_knobs() {
        let cfg = RocksConfig {
            block_cache_size: 64 * 1024 * 1024,
            write_buffer_size: 8 * 1024 * 1024,
            max_write_buffer_number: 2,
            ..RocksConfig::default()
        };
        assert_eq!(cfg.memory_budget_bytes(), 80 * 1024 * 1024);

        // `block_cache_size: 0` keeps its "leave the RocksDB default alone"
        // meaning: no cache is installed, so the budget is the memtable half.
        let no_cache = RocksConfig {
            block_cache_size: 0,
            ..cfg.clone()
        };
        assert_eq!(no_cache.memory_budget_bytes(), 16 * 1024 * 1024);
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
        assert!(rocks.create_if_missing);
    }

    /// The MB/s knob reaches RocksDB as bytes/s. `Options` has no getter for
    /// the rate limiter, so this conversion is only ever checked here.
    #[test]
    fn compaction_rate_limit_converts_megabytes_to_bytes_per_second() {
        let limited = RocksConfig {
            compaction_rate_limit_mb: Some(50),
            ..RocksConfig::default()
        };
        assert_eq!(
            limited.compaction_rate_limit_bytes_per_sec(),
            Some(52_428_800)
        );
        let unlimited = RocksConfig {
            compaction_rate_limit_mb: None,
            ..RocksConfig::default()
        };
        assert_eq!(unlimited.compaction_rate_limit_bytes_per_sec(), None);
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

    /// Background compaction/flush parallelism is sized from the machine, not
    /// pinned to a constant: a fixed `1` would serialize compaction on every
    /// host regardless of how many cores it has.
    #[test]
    fn background_jobs_default_is_sized_from_the_machine() {
        let cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        assert!(cores >= 1, "parallelism is never zero");
        assert_eq!(RocksConfig::default().max_background_jobs, cores as i32);
    }
}
