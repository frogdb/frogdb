//! Persistence and snapshot configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Whether persistence is enabled.
    #[serde(default = "default_persistence_enabled")]
    pub enabled: bool,

    /// Directory for data files.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Durability mode: "async", "periodic", or "sync".
    #[serde(default = "default_durability_mode")]
    pub durability_mode: String,

    /// Sync interval in milliseconds (for periodic mode).
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,

    /// RocksDB write buffer size in MB.
    #[serde(default = "default_write_buffer_size_mb")]
    pub write_buffer_size_mb: usize,

    /// Compression type: "none", "snappy", "lz4", "zstd".
    #[serde(default = "default_compression")]
    pub compression: String,

    /// RocksDB block cache size in MB (default: 256).
    #[serde(default = "default_block_cache_size_mb")]
    pub block_cache_size_mb: usize,

    /// RocksDB bloom filter bits per key (default: 10). Set to 0 to disable.
    #[serde(default = "default_bloom_filter_bits")]
    pub bloom_filter_bits: i32,

    /// Maximum number of RocksDB write buffers (default: 4).
    #[serde(default = "default_max_write_buffer_number")]
    pub max_write_buffer_number: i32,

    /// RocksDB compaction rate limit in MB/s (default: 0 = unlimited).
    #[serde(default = "default_compaction_rate_limit_mb")]
    pub compaction_rate_limit_mb: u64,

    /// Batch size threshold in KB before flushing.
    #[serde(default = "default_batch_size_threshold_kb")]
    pub batch_size_threshold_kb: usize,

    /// Batch timeout in milliseconds before flushing.
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,

    /// WAL failure policy: "continue" (default) or "rollback".
    #[serde(default = "default_wal_failure_policy")]
    pub wal_failure_policy: String,
}

fn default_persistence_enabled() -> bool {
    true
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./frogdb-data")
}

fn default_durability_mode() -> String {
    "periodic".to_string()
}

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
            data_dir: default_data_dir(),
            durability_mode: default_durability_mode(),
            sync_interval_ms: default_sync_interval_ms(),
            write_buffer_size_mb: default_write_buffer_size_mb(),
            compression: default_compression(),
            block_cache_size_mb: default_block_cache_size_mb(),
            bloom_filter_bits: default_bloom_filter_bits(),
            max_write_buffer_number: default_max_write_buffer_number(),
            compaction_rate_limit_mb: default_compaction_rate_limit_mb(),
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

        let valid_compressions = ["none", "snappy", "lz4", "zstd"];
        if !valid_compressions.contains(&self.compression.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid compression type '{}', expected one of: {}",
                self.compression,
                valid_compressions.join(", ")
            );
        }

        let valid_modes = ["async", "periodic", "sync"];
        if !valid_modes.contains(&self.durability_mode.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid durability_mode '{}', expected one of: {}",
                self.durability_mode,
                valid_modes.join(", ")
            );
        }

        let valid_policies = ["continue", "rollback"];
        if !valid_policies.contains(&self.wal_failure_policy.to_lowercase().as_str()) {
            anyhow::bail!(
                "invalid wal_failure_policy '{}', expected one of: {}",
                self.wal_failure_policy,
                valid_policies.join(", ")
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
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SnapshotConfig {
    /// Directory for storing snapshots.
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: PathBuf,

    /// Interval between automatic snapshots in seconds (0 = disabled).
    #[serde(default = "default_snapshot_interval_secs")]
    pub snapshot_interval_secs: u64,

    /// Maximum number of snapshots to retain (0 = unlimited).
    #[serde(default = "default_max_snapshots")]
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
}
