//! Compatibility configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Compatibility configuration for Redis protocol behavior.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "compat")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct CompatConfig {
    /// When true, CONFIG GET/SET returns errors for Redis params that FrogDB
    /// treats as no-ops. When false, silently accepts them for
    /// compatibility with Redis clients/tools that set these params.
    #[serde(default)]
    #[param(skip)]
    // skip: borderline: FrogDB-specific CONFIG-strictness meta-toggle; no Redis analogue
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
