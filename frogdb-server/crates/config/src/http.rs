//! HTTP server configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// HTTP server configuration for the unified observability/admin endpoint.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    /// Whether the HTTP server is enabled.
    #[serde(default = "default_http_enabled")]
    pub enabled: bool,

    /// Bind address for the HTTP server.
    #[serde(default = "default_http_bind")]
    pub bind: String,

    /// Port for the HTTP server.
    #[serde(default = "default_http_port")]
    pub port: u16,

    /// Optional bearer token for protected endpoints (/admin/*, /debug/*).
    /// When set, requests to these paths must include `Authorization: Bearer <token>`.
    #[serde(default)]
    pub token: Option<String>,
}

fn default_http_enabled() -> bool {
    true
}

fn default_http_bind() -> String {
    "127.0.0.1".to_string()
}

pub const DEFAULT_HTTP_PORT: u16 = 9090;

fn default_http_port() -> u16 {
    DEFAULT_HTTP_PORT
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: default_http_enabled(),
            bind: default_http_bind(),
            port: default_http_port(),
            token: None,
        }
    }
}

impl HttpConfig {
    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.bind, self.port)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.port == 0 {
            anyhow::bail!("http.port cannot be 0");
        }

        if self.token.as_ref().is_some_and(|t| t.is_empty()) {
            anyhow::bail!("http.token must not be empty when set");
        }

        if self.token.is_some() && self.bind == "0.0.0.0" {
            tracing::warn!(
                "http.token is set but http.bind is 0.0.0.0 — bearer token is sent in \
                 plaintext over HTTP. Consider binding to 127.0.0.1 or using a reverse proxy \
                 with TLS."
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_http_config() {
        let config = HttpConfig::default();
        assert!(config.enabled);
        assert_eq!(config.bind, "127.0.0.1");
        assert_eq!(config.port, DEFAULT_HTTP_PORT);
        assert!(config.token.is_none());
    }

    #[test]
    fn test_http_bind_addr() {
        let config = HttpConfig::default();
        assert_eq!(config.bind_addr(), "127.0.0.1:9090");
    }

    #[test]
    fn test_validate_zero_port() {
        let config = HttpConfig {
            port: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_empty_token() {
        let config = HttpConfig {
            token: Some(String::new()),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_disabled_skips_checks() {
        let config = HttpConfig {
            enabled: false,
            port: 0,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
