//! JSON configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default maximum nesting depth for JSON documents.
pub const DEFAULT_JSON_MAX_DEPTH: usize = 128;

/// Default maximum size in bytes for JSON documents (64MB).
pub const DEFAULT_JSON_MAX_SIZE: usize = 64 * 1024 * 1024;

/// JSON configuration.
//
// Both fields are consumed at runtime via `CommandContext.json_limits`; issue-14
// promoted them to immutable CONFIG GET/SET params (GET reports the honest
// startup value, SET is rejected).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "json")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct JsonConfig {
    /// Maximum nesting depth for JSON documents.
    #[serde(default = "default_json_max_depth")]
    #[param(name = "json-max-depth")]
    // issue-14: consumed via CommandContext.json_limits; immutable
    pub max_depth: usize,

    /// Maximum size in bytes for JSON documents.
    #[serde(default = "default_json_max_size")]
    #[param(name = "json-max-size")]
    // issue-14: consumed via CommandContext.json_limits; immutable
    pub max_size: usize,
}

fn default_json_max_depth() -> usize {
    DEFAULT_JSON_MAX_DEPTH
}

fn default_json_max_size() -> usize {
    DEFAULT_JSON_MAX_SIZE
}

impl Default for JsonConfig {
    fn default() -> Self {
        Self {
            max_depth: default_json_max_depth(),
            max_size: default_json_max_size(),
        }
    }
}

impl JsonConfig {
    /// Validate the JSON configuration.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.max_depth == 0 {
            anyhow::bail!("json.max_depth must be > 0");
        }
        if self.max_size == 0 {
            anyhow::bail!("json.max_size must be > 0");
        }
        Ok(())
    }
}
