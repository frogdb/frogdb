//! Persistence and snapshot configuration.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "persistence")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct PersistenceConfig {
    /// Whether persistence is enabled.
    #[serde(default = "default_persistence_enabled")]
    #[param(name = "persistence-enabled")]
    pub enabled: bool,

    /// WAL implementation to use: `"rocksdb"` (default, production) or `"fake"`
    /// (deterministic in-process sink for simulation tests). `"fake"` requires
    /// `enabled = true`.
    #[serde(default = "default_persistence_mode")]
    #[param(skip)]
    // skip: startup-only WAL backend selector (rocksdb/fake); 'fake' is a simulation-test sink, no runtime meaning; no Redis analogue
    pub mode: String,

    /// Directory for data files.
    #[serde(default = "default_data_dir")]
    #[param(name = "dir")]
    pub data_dir: PathBuf,

    /// Durability mode: "async", "periodic", or "sync".
    #[serde(default = "default_durability_mode")]
    #[param(mutable)]
    pub durability_mode: String,

    /// Sync interval in milliseconds (for periodic mode).
    #[serde(default = "default_sync_interval_ms")]
    #[param(mutable)]
    pub sync_interval_ms: u64,

    /// RocksDB write buffer size in MB.
    #[serde(default = "default_write_buffer_size_mb")]
    #[param]
    pub write_buffer_size_mb: usize,

    /// Compression type: "none", "snappy", "lz4", "zstd".
    #[serde(default = "default_compression")]
    #[param]
    pub compression: String,

    /// RocksDB block cache size in MB.
    #[serde(default = "default_block_cache_size_mb")]
    #[param]
    pub block_cache_size_mb: usize,

    /// RocksDB bloom filter bits per key. Set to 0 to disable.
    #[serde(default = "default_bloom_filter_bits")]
    #[param]
    pub bloom_filter_bits: i32,

    /// Maximum number of RocksDB write buffers.
    #[serde(default = "default_max_write_buffer_number")]
    #[param]
    pub max_write_buffer_number: i32,

    /// RocksDB compaction rate limit in MB/s. 0 means unlimited.
    #[serde(default = "default_compaction_rate_limit_mb")]
    #[param(skip)]
    pub compaction_rate_limit_mb: u64,

    /// Reclaim disk eagerly after FLUSHDB/FLUSHALL: follow the range tombstone
    /// with an asynchronous DeleteFilesInRange + CompactRange over the cleared
    /// column families. Without it, flushed SST bytes are only reclaimed when
    /// a compaction happens to cover the range.
    #[serde(default = "default_flush_compact_range")]
    #[param]
    pub flush_compact_range: bool,

    /// Batch size threshold in KB before flushing.
    #[serde(default = "default_batch_size_threshold_kb")]
    #[param(skip)]
    pub batch_size_threshold_kb: usize,

    /// Batch timeout in milliseconds before flushing.
    #[serde(default = "default_batch_timeout_ms")]
    #[param(mutable)]
    pub batch_timeout_ms: u64,

    /// WAL failure policy: "continue" or "rollback".
    #[serde(default = "default_wal_failure_policy")]
    #[param(mutable)]
    pub wal_failure_policy: String,
}

fn default_persistence_enabled() -> bool {
    true
}

fn default_persistence_mode() -> String {
    "rocksdb".to_string()
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./frogdb-data")
}

fn default_durability_mode() -> String {
    "periodic".to_string()
}

/// Valid durability modes accepted by the `durability-mode` parameter.
///
/// Single source of truth shared by [`PersistenceConfig::validate`] and the
/// runtime `durability-mode` CONFIG SET setter, so the two cannot drift apart.
pub const DURABILITY_MODES: &[&str] = &["async", "periodic", "sync"];

/// Valid WAL failure policies accepted by the `wal-failure-policy` parameter.
///
/// Single source of truth shared by [`PersistenceConfig::validate`] and the
/// runtime `wal-failure-policy` CONFIG SET setter, so the two cannot drift apart.
pub const WAL_FAILURE_POLICIES: &[&str] = &["continue", "rollback"];

/// Valid WAL implementations accepted by the `mode` parameter.
///
/// `"rocksdb"` is the production default; `"fake"` selects the deterministic
/// in-process WAL sink used by simulation tests.
pub const PERSISTENCE_MODES: &[&str] = &["rocksdb", "fake"];

pub const DEFAULT_SYNC_INTERVAL_MS: u64 = 1000;
pub const DEFAULT_WRITE_BUFFER_SIZE_MB: usize = 64;
pub const DEFAULT_BLOCK_CACHE_SIZE_MB: usize = 256;
pub const DEFAULT_BLOOM_FILTER_BITS: i32 = 10;
pub const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 4;
pub const DEFAULT_BATCH_SIZE_THRESHOLD_KB: usize = 4096;
pub const DEFAULT_BATCH_TIMEOUT_MS: u64 = 10;
pub const DEFAULT_SNAPSHOT_INTERVAL_SECS: u64 = 3600;
pub const DEFAULT_MAX_SNAPSHOTS: usize = 5;

fn default_sync_interval_ms() -> u64 {
    DEFAULT_SYNC_INTERVAL_MS
}

fn default_write_buffer_size_mb() -> usize {
    DEFAULT_WRITE_BUFFER_SIZE_MB
}

fn default_compression() -> String {
    "lz4".to_string()
}

fn default_block_cache_size_mb() -> usize {
    DEFAULT_BLOCK_CACHE_SIZE_MB
}

fn default_bloom_filter_bits() -> i32 {
    DEFAULT_BLOOM_FILTER_BITS
}

fn default_max_write_buffer_number() -> i32 {
    DEFAULT_MAX_WRITE_BUFFER_NUMBER
}

fn default_compaction_rate_limit_mb() -> u64 {
    0 // unlimited
}

fn default_flush_compact_range() -> bool {
    true
}

fn default_batch_size_threshold_kb() -> usize {
    DEFAULT_BATCH_SIZE_THRESHOLD_KB
}

fn default_batch_timeout_ms() -> u64 {
    DEFAULT_BATCH_TIMEOUT_MS
}

fn default_wal_failure_policy() -> String {
    "continue".to_string()
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: default_persistence_enabled(),
            mode: default_persistence_mode(),
            data_dir: default_data_dir(),
            durability_mode: default_durability_mode(),
            sync_interval_ms: default_sync_interval_ms(),
            write_buffer_size_mb: default_write_buffer_size_mb(),
            compression: default_compression(),
            block_cache_size_mb: default_block_cache_size_mb(),
            bloom_filter_bits: default_bloom_filter_bits(),
            max_write_buffer_number: default_max_write_buffer_number(),
            compaction_rate_limit_mb: default_compaction_rate_limit_mb(),
            flush_compact_range: default_flush_compact_range(),
            batch_size_threshold_kb: default_batch_size_threshold_kb(),
            batch_timeout_ms: default_batch_timeout_ms(),
            wal_failure_policy: default_wal_failure_policy(),
        }
    }
}

impl PersistenceConfig {
    /// Validate the persistence configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if !PERSISTENCE_MODES.contains(&self.mode.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid persistence mode '{}', expected one of: {}",
                self.mode,
                PERSISTENCE_MODES.join(", ")
            );
        }

        let valid_compressions = ["none", "snappy", "lz4", "zstd"];
        if !valid_compressions.contains(&self.compression.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid compression type '{}', expected one of: {}",
                self.compression,
                valid_compressions.join(", ")
            );
        }

        if !DURABILITY_MODES.contains(&self.durability_mode.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid durability_mode '{}', expected one of: {}",
                self.durability_mode,
                DURABILITY_MODES.join(", ")
            );
        }

        if !WAL_FAILURE_POLICIES.contains(&self.wal_failure_policy.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid wal_failure_policy '{}', expected one of: {}",
                self.wal_failure_policy,
                WAL_FAILURE_POLICIES.join(", ")
            );
        }

        if self.write_buffer_size_mb == 0 {
            anyhow::bail!("persistence.write_buffer_size_mb must be > 0");
        }

        if self.max_write_buffer_number <= 0 {
            anyhow::bail!("persistence.max_write_buffer_number must be > 0");
        }

        if self.bloom_filter_bits < 0 {
            anyhow::bail!("persistence.bloom_filter_bits must be >= 0 (0 = disabled, typical: 10)");
        }

        Ok(())
    }
}

/// Snapshot configuration.
//
// No fields are exposed as CONFIG GET/SET parameters today; each still carries
// an explicit `#[param(skip)]` so the per-field coverage guarantee holds.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "snapshot")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SnapshotConfig {
    /// Directory for storing snapshots.
    #[serde(default = "default_snapshot_dir")]
    #[param]
    pub snapshot_dir: PathBuf,

    /// Interval between automatic snapshots in seconds (0 = disabled).
    #[serde(default = "default_snapshot_interval_secs")]
    #[param(skip)]
    pub snapshot_interval_secs: u64,

    /// Maximum number of snapshots to retain (0 = unlimited).
    #[serde(default = "default_max_snapshots")]
    #[param(skip)]
    // skip: borderline: snapshot retention count, no Redis CONFIG analogue; snapshot subsystem liveness unverified
    pub max_snapshots: usize,
}

fn default_snapshot_dir() -> PathBuf {
    PathBuf::from("./snapshots")
}

fn default_snapshot_interval_secs() -> u64 {
    DEFAULT_SNAPSHOT_INTERVAL_SECS
}

fn default_max_snapshots() -> usize {
    DEFAULT_MAX_SNAPSHOTS
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: default_snapshot_dir(),
            snapshot_interval_secs: default_snapshot_interval_secs(),
            max_snapshots: default_max_snapshots(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_invalid_compression() {
        let config = PersistenceConfig {
            compression: "invalid".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid compression type")
        );
    }

    #[test]
    fn test_validate_valid_compression_types() {
        for compression in [
            "none", "snappy", "lz4", "zstd", "NONE", "Snappy", "LZ4", "ZSTD",
        ] {
            let config = PersistenceConfig {
                compression: compression.to_string(),
                ..Default::default()
            };
            assert!(
                config.validate().is_ok(),
                "Compression {} should be valid",
                compression
            );
        }
    }

    #[test]
    fn test_validate_invalid_durability_mode() {
        let config = PersistenceConfig {
            durability_mode: "invalid".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid durability_mode")
        );
    }

    #[test]
    fn test_validate_valid_durability_modes() {
        for mode in ["async", "periodic", "sync", "ASYNC", "Periodic", "SYNC"] {
            let config = PersistenceConfig {
                durability_mode: mode.to_string(),
                ..Default::default()
            };
            assert!(
                config.validate().is_ok(),
                "Durability mode {} should be valid",
                mode
            );
        }
    }

    #[test]
    fn test_validate_persistence_disabled_skips_validation() {
        let config = PersistenceConfig {
            enabled: false,
            compression: "invalid".to_string(),
            durability_mode: "invalid".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_wal_failure_policy() {
        let config = PersistenceConfig {
            wal_failure_policy: "invalid".to_string(),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid wal_failure_policy")
        );
    }

    #[test]
    fn test_validate_valid_wal_failure_policies() {
        for policy in ["continue", "rollback", "CONTINUE", "Rollback", "ROLLBACK"] {
            let config = PersistenceConfig {
                wal_failure_policy: policy.to_string(),
                ..Default::default()
            };
            assert!(
                config.validate().is_ok(),
                "WAL failure policy {} should be valid",
                policy
            );
        }
    }

    #[test]
    fn test_default_wal_failure_policy() {
        let config = PersistenceConfig::default();
        assert_eq!(config.wal_failure_policy, "continue");
    }

    #[test]
    fn mode_defaults_to_rocksdb_and_validates() {
        let c = PersistenceConfig::default();
        assert_eq!(c.mode, "rocksdb");
        assert!(c.validate().is_ok());
        let fake = PersistenceConfig {
            mode: "fake".into(),
            ..Default::default()
        };
        assert!(fake.validate().is_ok());
        let bad = PersistenceConfig {
            mode: "bogus".into(),
            ..Default::default()
        };
        assert!(bad.validate().is_err());
    }
}
