//! Tiered storage configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Tiered storage configuration for two-tier (hot/warm) storage.
///
/// When enabled, eviction policies `tiered-lru` and `tiered-lfu` demote values
/// to a RocksDB warm tier instead of deleting them.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TieredStorageConfig {
    /// Enable tiered storage. Requires persistence to be enabled.
    #[serde(default)]
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
