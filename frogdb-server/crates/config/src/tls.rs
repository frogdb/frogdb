//! TLS configuration.

use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// TLS protocol version.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum TlsProtocol {
    /// TLS 1.2
    #[serde(rename = "1.2")]
    Tls12,
    /// TLS 1.3
    #[serde(rename = "1.3")]
    Tls13,
}

/// Client certificate authentication mode.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ClientCertMode {
    /// No client certificate required.
    #[default]
    None,
    /// Client certificate requested but not required.
    Optional,
    /// Client certificate required (mutual TLS).
    Required,
}

/// TLS configuration section.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Path to the server certificate file (PEM format).
    #[serde(default)]
    pub cert_file: PathBuf,

    /// Path to the server private key file (PEM format).
    #[serde(default)]
    pub key_file: PathBuf,

    /// Path to the CA certificate file for client certificate verification (PEM format).
    /// Required when `require_client_cert` is not `none`.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,

    /// Port for TLS connections.
    #[serde(default = "default_tls_port")]
    pub tls_port: u16,

    /// Client certificate authentication mode.
    #[serde(default)]
    pub require_client_cert: ClientCertMode,

    /// Allowed TLS protocol versions.
    #[serde(default = "default_protocols")]
    pub protocols: Vec<TlsProtocol>,

    /// Allowed ciphersuites. Empty means use rustls defaults.
    #[serde(default)]
    pub ciphersuites: Vec<String>,

    /// Whether to encrypt replication connections.
    #[serde(default)]
    pub tls_replication: bool,

    /// Whether to encrypt cluster bus connections.
    #[serde(default)]
    pub tls_cluster: bool,

    /// Whether to enable dual-accept mode for rolling TLS cluster migration.
    #[serde(default)]
    pub tls_cluster_migration: bool,

    /// Whether to keep the admin port as plaintext even when TLS is enabled.
    #[serde(default = "default_true")]
    pub no_tls_on_admin_port: bool,

    /// Whether to keep the HTTP server as plaintext even when TLS is enabled.
    #[serde(default = "default_true")]
    pub no_tls_on_http: bool,

    /// Path to client certificate for outgoing replication/cluster connections.
    #[serde(default)]
    pub client_cert_file: Option<PathBuf>,

    /// Path to client private key for outgoing replication/cluster connections.
    #[serde(default)]
    pub client_key_file: Option<PathBuf>,

    /// Whether to watch certificate files for changes and auto-reload.
    #[serde(default = "default_true")]
    pub watch_certs: bool,

    /// Debounce interval in milliseconds for certificate file watcher.
    #[serde(default = "default_watch_debounce_ms")]
    pub watch_debounce_ms: u64,

    /// TLS handshake timeout in milliseconds.
    #[serde(default = "default_handshake_timeout_ms")]
    pub handshake_timeout_ms: u64,
}

pub const DEFAULT_TLS_PORT: u16 = 6380;

fn default_tls_port() -> u16 {
    DEFAULT_TLS_PORT
}

fn default_protocols() -> Vec<TlsProtocol> {
    vec![TlsProtocol::Tls13, TlsProtocol::Tls12]
}

fn default_true() -> bool {
    true
}

fn default_watch_debounce_ms() -> u64 {
    500
}

fn default_handshake_timeout_ms() -> u64 {
    10000
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: PathBuf::new(),
            key_file: PathBuf::new(),
            ca_file: None,
            tls_port: default_tls_port(),
            require_client_cert: ClientCertMode::default(),
            protocols: default_protocols(),
            ciphersuites: Vec::new(),
            tls_replication: false,
            tls_cluster: false,
            tls_cluster_migration: false,
            no_tls_on_admin_port: true,
            no_tls_on_http: true,
            client_cert_file: None,
            client_key_file: None,
            watch_certs: true,
            watch_debounce_ms: 500,
            handshake_timeout_ms: 10000,
        }
    }
}

impl TlsConfig {
    /// Validate the TLS configuration.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // cert_file and key_file are required when TLS is enabled
        if self.cert_file.as_os_str().is_empty() {
            anyhow::bail!("tls.cert_file is required when tls.enabled = true");
        }
        if self.key_file.as_os_str().is_empty() {
            anyhow::bail!("tls.key_file is required when tls.enabled = true");
        }

        // ca_file is required for mTLS
        if self.require_client_cert != ClientCertMode::None && self.ca_file.is_none() {
            anyhow::bail!(
                "tls.ca_file is required when tls.require_client_cert is '{}'",
                match self.require_client_cert {
                    ClientCertMode::Optional => "optional",
                    ClientCertMode::Required => "required",
                    ClientCertMode::None => unreachable!(),
                }
            );
        }

        // tls_replication requires TLS to be enabled
        if self.tls_replication {
            // Already checked enabled above, but be explicit
            // This is for clarity in case someone sets tls_replication without enabled
        }

        // tls_cluster requires TLS to be enabled
        if self.tls_cluster {
            // Already checked enabled above
        }

        // tls_cluster_migration requires tls_cluster
        if self.tls_cluster_migration && !self.tls_cluster {
            anyhow::bail!("tls.tls_cluster_migration = true requires tls.tls_cluster = true");
        }

        // At least one protocol must be specified
        if self.protocols.is_empty() {
            anyhow::bail!("tls.protocols must contain at least one protocol version");
        }

        if self.tls_port == 0 {
            anyhow::bail!("tls.tls_port cannot be 0");
        }

        if self.handshake_timeout_ms == 0 {
            anyhow::bail!("tls.handshake_timeout_ms must be > 0");
        }

        // Check that cert/key files exist
        if !self.cert_file.exists() {
            anyhow::bail!(
                "tls.cert_file '{}' does not exist",
                self.cert_file.display()
            );
        }
        if !self.key_file.exists() {
            anyhow::bail!("tls.key_file '{}' does not exist", self.key_file.display());
        }
        if let Some(ref ca_file) = self.ca_file
            && !ca_file.exists()
        {
            anyhow::bail!("tls.ca_file '{}' does not exist", ca_file.display());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_valid() {
        let config = TlsConfig::default();
        // Disabled TLS should always validate OK
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_enabled_missing_cert() {
        let config = TlsConfig {
            enabled: true,
            key_file: PathBuf::from("/some/key.pem"),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("cert_file is required"));
    }

    #[test]
    fn test_enabled_missing_key() {
        let config = TlsConfig {
            enabled: true,
            cert_file: PathBuf::from("/some/cert.pem"),
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("key_file is required"));
    }

    #[test]
    fn test_mtls_missing_ca() {
        let config = TlsConfig {
            enabled: true,
            cert_file: PathBuf::from("/some/cert.pem"),
            key_file: PathBuf::from("/some/key.pem"),
            require_client_cert: ClientCertMode::Required,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("ca_file is required"));
    }

    #[test]
    fn test_cluster_migration_without_cluster() {
        let config = TlsConfig {
            enabled: true,
            cert_file: PathBuf::from("/some/cert.pem"),
            key_file: PathBuf::from("/some/key.pem"),
            tls_cluster_migration: true,
            tls_cluster: false,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tls_cluster_migration"));
    }

    #[test]
    fn test_empty_protocols() {
        let config = TlsConfig {
            enabled: true,
            cert_file: PathBuf::from("/some/cert.pem"),
            key_file: PathBuf::from("/some/key.pem"),
            protocols: vec![],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("at least one protocol"));
    }

    #[test]
    fn test_zero_port() {
        let config = TlsConfig {
            enabled: true,
            cert_file: PathBuf::from("/some/cert.pem"),
            key_file: PathBuf::from("/some/key.pem"),
            tls_port: 0,
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("cannot be 0"));
    }

    #[test]
    fn test_serde_defaults() {
        let json = r#"{"enabled": false}"#;
        let config: TlsConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.tls_port, 6380);
        assert_eq!(config.protocols.len(), 2);
        assert!(config.no_tls_on_admin_port);
        assert!(config.watch_certs);
        assert_eq!(config.handshake_timeout_ms, 10000);
    }

    #[test]
    fn test_protocol_serde() {
        let json = r#"["1.3", "1.2"]"#;
        let protocols: Vec<TlsProtocol> = serde_json::from_str(json).unwrap();
        assert_eq!(protocols, vec![TlsProtocol::Tls13, TlsProtocol::Tls12]);
    }

    #[test]
    fn test_client_cert_mode_serde() {
        let json = r#""required""#;
        let mode: ClientCertMode = serde_json::from_str(json).unwrap();
        assert_eq!(mode, ClientCertMode::Required);

        let json = r#""optional""#;
        let mode: ClientCertMode = serde_json::from_str(json).unwrap();
        assert_eq!(mode, ClientCertMode::Optional);

        let json = r#""none""#;
        let mode: ClientCertMode = serde_json::from_str(json).unwrap();
        assert_eq!(mode, ClientCertMode::None);
    }
}
