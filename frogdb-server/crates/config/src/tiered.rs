//! Tiered storage configuration.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Tiered storage configuration for two-tier (hot/warm) storage.
///
/// When enabled, eviction policies `tiered-lru` and `tiered-lfu` spill values
/// to a RocksDB warm tier instead of deleting them.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "tiered-storage")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct TieredStorageConfig {
    /// Enable tiered storage. Requires persistence to be enabled.
    #[serde(default)]
    #[param(skip)]
    // skip: startup-only: tiered-storage subsystem initialized at boot, requires persistence; not live-toggleable
    pub enabled: bool,
}

impl TieredStorageConfig {
    /// Validate the tiered storage configuration.
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_tiered_config() {
        let config = TieredStorageConfig::default();
        assert!(!config.enabled);
    }
}
