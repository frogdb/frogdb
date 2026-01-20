//! Configuration handling via Figment.

use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Main configuration struct.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Server configuration.
    #[serde(default)]
    pub server: ServerConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,
}

/// Server-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ServerConfig {
    /// Bind address.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Listen port.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Number of shards (0 = auto-detect CPU cores).
    #[serde(default = "default_num_shards")]
    pub num_shards: usize,
}

/// Logging configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format (pretty, json).
    #[serde(default = "default_log_format")]
    pub format: String,
}

fn default_bind() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    6379
}

fn default_num_shards() -> usize {
    1 // Start with 1 shard as per the plan
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "pretty".to_string()
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            port: default_port(),
            num_shards: default_num_shards(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from multiple sources.
    ///
    /// Priority (highest to lowest):
    /// 1. CLI arguments
    /// 2. Environment variables (FROGDB_ prefix)
    /// 3. TOML config file
    /// 4. Built-in defaults
    pub fn load(
        config_path: Option<&Path>,
        bind: Option<String>,
        port: Option<u16>,
        shards: Option<String>,
        log_level: Option<String>,
        log_format: Option<String>,
    ) -> Result<Self> {
        let mut figment = Figment::new()
            .merge(Serialized::defaults(Config::default()));

        // Merge config file if provided
        if let Some(path) = config_path {
            figment = figment.merge(Toml::file(path));
        } else {
            // Try default config file
            figment = figment.merge(Toml::file("frogdb.toml").nested());
        }

        // Merge environment variables
        figment = figment.merge(Env::prefixed("FROGDB_").split("__"));

        // Build CLI overrides
        let mut cli_overrides = Config::default();

        if let Some(bind) = bind {
            cli_overrides.server.bind = bind;
        }

        if let Some(port) = port {
            cli_overrides.server.port = port;
        }

        if let Some(shards) = shards {
            cli_overrides.server.num_shards = if shards == "auto" {
                std::thread::available_parallelism()
                    .map(|p| p.get())
                    .unwrap_or(1)
            } else {
                shards.parse().context("Invalid shard count")?
            };
        }

        if let Some(level) = log_level {
            cli_overrides.logging.level = level;
        }

        if let Some(format) = log_format {
            cli_overrides.logging.format = format;
        }

        // Merge CLI overrides (only non-default values)
        // For simplicity, we'll re-merge specific values
        let mut config: Config = figment.extract().context("Failed to load configuration")?;

        // Apply CLI overrides explicitly
        if bind.is_some() {
            config.server.bind = cli_overrides.server.bind;
        }
        if port.is_some() {
            config.server.port = cli_overrides.server.port;
        }
        if shards.is_some() {
            config.server.num_shards = cli_overrides.server.num_shards;
        }
        if log_level.is_some() {
            config.logging.level = cli_overrides.logging.level;
        }
        if log_format.is_some() {
            config.logging.format = cli_overrides.logging.format;
        }

        // Validate
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration values.
    fn validate(&self) -> Result<()> {
        // Validate port
        if self.server.port == 0 {
            anyhow::bail!("Port cannot be 0");
        }

        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log level '{}', expected one of: {:?}",
                self.logging.level,
                valid_levels
            );
        }

        // Validate log format
        let valid_formats = ["pretty", "json"];
        if !valid_formats.contains(&self.logging.format.to_lowercase().as_str()) {
            anyhow::bail!(
                "Invalid log format '{}', expected one of: {:?}",
                self.logging.format,
                valid_formats
            );
        }

        Ok(())
    }

    /// Initialize logging based on configuration.
    pub fn init_logging(&self) -> Result<()> {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new(&self.logging.level));

        match self.logging.format.to_lowercase().as_str() {
            "json" => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt::layer().json())
                    .init();
            }
            _ => {
                tracing_subscriber::registry()
                    .with(filter)
                    .with(fmt::layer())
                    .init();
            }
        }

        Ok(())
    }

    /// Generate default TOML configuration.
    pub fn default_toml() -> String {
        r#"# FrogDB Configuration File

[server]
# Bind address
bind = "127.0.0.1"

# Listen port
port = 6379

# Number of shards (0 = auto-detect CPU cores)
num_shards = 1

[logging]
# Log level (trace, debug, info, warn, error)
level = "info"

# Log format (pretty, json)
format = "pretty"
"#
        .to_string()
    }

    /// Get the full bind address.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.server.bind, self.server.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.num_shards, 1);
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "pretty");
    }

    #[test]
    fn test_validate_invalid_log_level() {
        let mut config = Config::default();
        config.logging.level = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_log_format() {
        let mut config = Config::default();
        config.logging.format = "xml".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_bind_addr() {
        let config = Config::default();
        assert_eq!(config.bind_addr(), "127.0.0.1:6379");
    }
}
