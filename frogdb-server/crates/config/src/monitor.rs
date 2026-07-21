//! MONITOR command configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// MONITOR command configuration.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "monitor")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct MonitorConfig {
    /// Bounded broadcast channel capacity (ring buffer size).
    /// Slow subscribers skip ahead rather than blocking the server.
    #[serde(default = "default_channel_capacity")]
    #[param(skip)] // skip: internal MONITOR broadcast ring-buffer capacity; no operator story
    pub channel_capacity: usize,
}

fn default_channel_capacity() -> usize {
    4096
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            channel_capacity: default_channel_capacity(),
        }
    }
}
