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

    /// Port for the admin RESP protocol listener.
    #[serde(default = "default_admin_port")]
    pub port: u16,

    /// Bind address for the admin RESP protocol listener.
    #[serde(default = "default_admin_bind")]
    pub bind: String,

    /// Port for the admin HTTP API (health, cluster, role, nodes endpoints).
    /// Defaults to admin port + 1 (6381).
    #[serde(default = "default_admin_http_port")]
    pub http_port: u16,

    /// Bind address for the admin HTTP API. Defaults to the same as `bind`.
    #[serde(default)]
    pub http_bind: Option<String>,
}

pub const DEFAULT_ADMIN_PORT: u16 = 6380;
pub const DEFAULT_ADMIN_HTTP_PORT: u16 = 6381;
pub const DEFAULT_ADMIN_BIND: &str = "127.0.0.1";

fn default_admin_port() -> u16 {
    DEFAULT_ADMIN_PORT
}

fn default_admin_http_port() -> u16 {
    DEFAULT_ADMIN_HTTP_PORT
}

fn default_admin_bind() -> String {
    DEFAULT_ADMIN_BIND.to_string()
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_admin_port(),
            bind: default_admin_bind(),
            http_port: default_admin_http_port(),
            http_bind: None,
        }
    }
}

impl AdminConfig {
    /// Get the full bind address for the admin RESP listener.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Get the full bind address for the admin HTTP API listener.
    pub fn http_bind_addr(&self) -> String {
        let host = self.http_bind.as_deref().unwrap_or(&self.bind);
        format!("{}:{}", host, self.http_port)
    }

    /// Validate the admin configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.port == 0 {
            anyhow::bail!("admin.port cannot be 0");
        }

        if self.http_port == 0 {
            anyhow::bail!("admin.http_port cannot be 0");
        }

        if self.port == self.http_port
            && self.http_bind.as_deref().unwrap_or(&self.bind) == self.bind
        {
            anyhow::bail!(
                "admin.port and admin.http_port must be different (both are {})",
                self.port
            );
        }

        Ok(())
    }
}
