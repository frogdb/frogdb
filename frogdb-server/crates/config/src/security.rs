//! Security and ACL configuration.

use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Security configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "security")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SecurityConfig {
    /// Legacy password for the default user (like Redis requirepass).
    /// If set, clients must AUTH with this password before running commands.
    #[serde(default)]
    #[param(mutable)]
    pub requirepass: String,
}

/// ACL configuration.
//
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "acl")]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct AclFileConfig {
    /// Path to the ACL file for SAVE/LOAD operations.
    /// If empty, ACL SAVE/LOAD will return an error.
    #[serde(default)]
    #[param]
    pub aclfile: String,

    /// Maximum number of entries in the ACL LOG.
    #[serde(default = "default_acl_log_max_len")]
    #[param(skip)]
    pub log_max_len: usize,
}

pub const DEFAULT_ACL_LOG_MAX_LEN: usize = 128;

fn default_acl_log_max_len() -> usize {
    DEFAULT_ACL_LOG_MAX_LEN
}

impl Default for AclFileConfig {
    fn default() -> Self {
        Self {
            aclfile: String::new(),
            log_max_len: default_acl_log_max_len(),
        }
    }
}
