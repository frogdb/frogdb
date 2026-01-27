//! Admin API configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Admin API configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct AdminConfig {
    /// Whether the admin API is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Port for the admin HTTP server.
    #[serde(default = "default_admin_port")]
    pub port: u16,

    /// Bind address for the admin HTTP server.
    #[serde(default = "default_admin_bind")]
    pub bind: String,
}

fn default_admin_port() -> u16 {
    6380
}

fn default_admin_bind() -> String {
    "127.0.0.1".to_string()
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_admin_port(),
            bind: default_admin_bind(),
        }
    }
}

impl AdminConfig {
    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Validate the admin configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.port == 0 {
            anyhow::bail!("admin.port cannot be 0");
        }

        Ok(())
    }
}
