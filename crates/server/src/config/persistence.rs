//! Persistence and snapshot configuration.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Persistence configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

    /// Batch size threshold in KB before flushing.
    #[serde(default = "default_batch_size_threshold_kb")]
    pub batch_size_threshold_kb: usize,

    /// Batch timeout in milliseconds before flushing.
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
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

fn default_sync_interval_ms() -> u64 {
    1000
}

fn default_write_buffer_size_mb() -> usize {
    64
}

fn default_compression() -> String {
    "lz4".to_string()
}

fn default_batch_size_threshold_kb() -> usize {
    4096 // 4MB
}

fn default_batch_timeout_ms() -> u64 {
    10
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
            batch_size_threshold_kb: default_batch_size_threshold_kb(),
            batch_timeout_ms: default_batch_timeout_ms(),
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

        Ok(())
    }
}

/// Snapshot configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    3600 // 1 hour
}

fn default_max_snapshots() -> usize {
    5
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

impl SnapshotConfig {
    /// Convert to the core SnapshotConfig type.
    pub fn to_core_config(&self) -> frogdb_core::persistence::SnapshotConfig {
        frogdb_core::persistence::SnapshotConfig {
            snapshot_dir: self.snapshot_dir.clone(),
            snapshot_interval_secs: self.snapshot_interval_secs,
            max_snapshots: self.max_snapshots,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_invalid_compression() {
        let mut config = PersistenceConfig::default();
        config.compression = "invalid".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid compression type"));
    }

    #[test]
    fn test_validate_valid_compression_types() {
        for compression in ["none", "snappy", "lz4", "zstd", "NONE", "Snappy", "LZ4", "ZSTD"] {
            let mut config = PersistenceConfig::default();
            config.compression = compression.to_string();
            assert!(
                config.validate().is_ok(),
                "Compression {} should be valid",
                compression
            );
        }
    }

    #[test]
    fn test_validate_invalid_durability_mode() {
        let mut config = PersistenceConfig::default();
        config.durability_mode = "invalid".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid durability_mode"));
    }

    #[test]
    fn test_validate_valid_durability_modes() {
        for mode in ["async", "periodic", "sync", "ASYNC", "Periodic", "SYNC"] {
            let mut config = PersistenceConfig::default();
            config.durability_mode = mode.to_string();
            assert!(
                config.validate().is_ok(),
                "Durability mode {} should be valid",
                mode
            );
        }
    }

    #[test]
    fn test_validate_persistence_disabled_skips_validation() {
        let mut config = PersistenceConfig::default();
        config.enabled = false;
        config.compression = "invalid".to_string();
        config.durability_mode = "invalid".to_string();
        // Should pass because persistence is disabled
        assert!(config.validate().is_ok());
    }
}
