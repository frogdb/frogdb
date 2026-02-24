//! Compatibility configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Compatibility configuration for Redis protocol behavior.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompatConfig {
    /// When true, CONFIG GET/SET returns errors for Redis params that FrogDB
    /// treats as no-ops. When false (default), silently accepts them for
    /// compatibility with Redis clients/tools that set these params.
    #[serde(default)]
    pub strict_config: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compat_config_defaults() {
        let config = CompatConfig::default();
        assert!(!config.strict_config);
    }
}
