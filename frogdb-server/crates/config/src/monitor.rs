//! MONITOR command configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// MONITOR command configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct MonitorConfig {
    /// Bounded broadcast channel capacity (ring buffer size).
    /// Slow subscribers skip ahead rather than blocking the server.
    #[serde(default = "default_channel_capacity")]
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
