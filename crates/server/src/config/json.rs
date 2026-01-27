//! JSON configuration.

use serde::{Deserialize, Serialize};

/// JSON configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonConfig {
    /// Maximum nesting depth for JSON documents.
    #[serde(default = "default_json_max_depth")]
    pub max_depth: usize,

    /// Maximum size in bytes for JSON documents.
    #[serde(default = "default_json_max_size")]
    pub max_size: usize,
}

fn default_json_max_depth() -> usize {
    frogdb_core::DEFAULT_JSON_MAX_DEPTH
}

fn default_json_max_size() -> usize {
    frogdb_core::DEFAULT_JSON_MAX_SIZE
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
    /// Convert to JsonLimits.
    pub fn to_limits(&self) -> frogdb_core::JsonLimits {
        frogdb_core::JsonLimits {
            max_depth: self.max_depth,
            max_size: self.max_size,
        }
    }
}
