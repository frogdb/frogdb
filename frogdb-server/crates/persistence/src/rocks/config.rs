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
    #[allow(dead_code)]
    pub(crate) fn to_rocksdb(self) -> DBCompressionType {
        match self {
            Self::None => DBCompressionType::None,
            Self::Snappy => DBCompressionType::Snappy,
            Self::Lz4 => DBCompressionType::Lz4,
            Self::Zstd => DBCompressionType::Zstd,
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
