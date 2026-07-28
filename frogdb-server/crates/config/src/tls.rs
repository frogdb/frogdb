//! TLS configuration.

use anyhow::Result;
use frogdb_config_derive::ConfigParams;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Deserialize an optional path, mapping the empty string to `None`.
///
/// CONFIG GET/SET can only spell "unset" as the empty string, and CONFIG
/// REWRITE removes the key rather than writing `""` for exactly that reason.
/// This is the belt-and-braces other end: a hand-edited (or older) file
/// carrying `ca-file = ""` is read as *unset* instead of as a file named `""`,
/// which would otherwise fail validation with a baffling "does not exist".
fn deserialize_optional_path<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Option<PathBuf> = Option::deserialize(deserializer)?;
    Ok(raw.filter(|p| !p.as_os_str().is_empty()))
}

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

/// An extra server identity (certificate + key) offered alongside the primary
/// `cert-file`/`key-file` pair.
///
/// Configured as a TOML array-of-tables:
///
/// ```toml
/// [[tls.additional-certs]]
/// cert-file = "/etc/frogdb/tls/ecdsa.crt"
/// key-file  = "/etc/frogdb/tls/ecdsa.key"
/// ```
///
/// The server builds one rustls `CertifiedKey` per pair and picks, per
/// ClientHello, the first identity whose key can sign one of the signature
/// schemes the client advertised. The primary pair is always tried first and is
/// also the fallback when no identity matches, so a single-cert deployment
/// behaves exactly as before.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub struct AdditionalCert {
    /// Path to the certificate file (PEM format).
    pub cert_file: PathBuf,
    /// Path to the matching private key file (PEM format).
    pub key_file: PathBuf,
}

/// TLS configuration section.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, ConfigParams)]
#[params(section = "tls")]
#[serde(rename_all = "kebab-case")]
pub struct TlsConfig {
    /// Whether TLS is enabled.
    #[serde(default)]
    #[param(name = "tls-enabled")]
    pub enabled: bool,

    /// Path to the server certificate file (PEM format).
    #[serde(default)]
    #[param(mutable, name = "tls-cert-file")]
    pub cert_file: PathBuf,

    /// Path to the server private key file (PEM format).
    #[serde(default)]
    #[param(mutable, name = "tls-key-file")]
    pub key_file: PathBuf,

    /// Extra server identities offered alongside `cert-file`/`key-file`
    /// (e.g. an ECDSA pair next to an RSA pair for dual-cert deployments).
    ///
    /// See [`AdditionalCert`] for the TOML shape and the selection rule. Every
    /// pair must parse and its key must match its certificate, or TLS setup
    /// fails loudly at startup / on reload.
    #[serde(default)]
    // skip: array-of-tables with no flat CONFIG representation — a single
    // `tls-additional-certs` string cannot express N (cert, key) pairs without
    // inventing an encoding CONFIG SET would then have to parse, and CONFIG
    // REWRITE would have to round-trip back into `[[tls.additional-certs]]`.
    // The live seam exists regardless (`TlsRuntimeHandle::set_additional_certs`,
    // used by the cert watcher); it is reachable by editing the file and
    // reloading, not by CONFIG.
    #[param(skip)]
    pub additional_certs: Vec<AdditionalCert>,

    /// Path to the CA certificate file for client certificate verification (PEM format).
    /// Required when `require_client_cert` is not `none`.
    #[serde(default, deserialize_with = "deserialize_optional_path")]
    #[param(mutable, name = "tls-ca-cert-file")]
    pub ca_file: Option<PathBuf>,

    /// Port for TLS connections.
    #[serde(default = "default_tls_port")]
    #[param]
    pub tls_port: u16,

    /// Client certificate authentication mode.
    #[serde(default)]
    #[param(name = "tls-auth-clients")]
    pub require_client_cert: ClientCertMode,

    /// Allowed TLS protocol versions.
    #[serde(default = "default_protocols")]
    #[param(name = "tls-protocols")]
    pub protocols: Vec<TlsProtocol>,

    /// Allowed ciphersuites, by rustls IANA name (case-insensitive), e.g.
    /// `TLS13_AES_256_GCM_SHA384` or `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`.
    /// Empty means use rustls defaults. Applied by the server crate when
    /// building the rustls Server/Client config; an unknown name or a set that
    /// excludes every enabled protocol version fails loudly at startup.
    #[serde(default)]
    // Live-mutable: `CONFIG SET tls-ciphersuites` rebuilds the rustls configs
    // through `TlsRuntimeHandle::set_ciphersuites`, which validates the names
    // as part of the reload and leaves the previous configs serving if the
    // reload fails. Redis-compat name `tls-ciphersuites`.
    #[param(mutable, name = "tls-ciphersuites")]
    pub ciphersuites: Vec<String>,

    /// Whether to encrypt replication connections.
    #[serde(default)]
    #[param]
    pub tls_replication: bool,

    /// Whether to encrypt cluster bus connections.
    #[serde(default)]
    #[param]
    pub tls_cluster: bool,

    /// Whether to enable dual-accept mode for rolling TLS cluster migration.
    #[serde(default)]
    #[param(mutable)]
    pub tls_cluster_migration: bool,

    /// Whether to keep the admin port as plaintext even when TLS is enabled.
    #[serde(default = "default_true")]
    #[param(skip)]
    // skip: startup listener wiring; plaintext/TLS on a bound port is fixed at bind time
    pub no_tls_on_admin_port: bool,

    /// Whether to keep the HTTP server as plaintext even when TLS is enabled.
    #[serde(default = "default_true")]
    #[param(skip)]
    // skip: startup listener wiring; plaintext/TLS on a bound port is fixed at bind time
    pub no_tls_on_http: bool,

    /// Path to client certificate for outgoing replication/cluster connections.
    #[serde(default, deserialize_with = "deserialize_optional_path")]
    #[param(mutable, name = "tls-client-cert-file")]
    pub client_cert_file: Option<PathBuf>,

    /// Path to client private key for outgoing replication/cluster connections.
    #[serde(default, deserialize_with = "deserialize_optional_path")]
    #[param(mutable, name = "tls-client-key-file")]
    pub client_key_file: Option<PathBuf>,

    /// Whether to watch certificate files for changes and auto-reload.
    ///
    /// When enabled, the server polls every file this section references
    /// (`cert-file`, `key-file`, `ca-file`, the client pair and every
    /// `[[tls.additional-certs]]` pair) and re-reads them once they stop
    /// changing, so rotating certificates in place needs no restart. A reload
    /// that fails (mismatched pair, unparseable PEM) is logged and the
    /// previously loaded certificates keep serving.
    #[serde(default = "default_true")]
    // Immutable: the watcher task is spawned (or not) once at startup; there is
    // no seam to start or stop it live. GET reports the honest startup value.
    #[param(name = "tls-watch-certs")]
    pub watch_certs: bool,

    /// Poll/debounce interval in milliseconds for the certificate file watcher.
    ///
    /// A rotation writes the certificate and key as two separate files, so the
    /// watcher waits for one full quiet interval before reloading; reaction
    /// latency is between one and two intervals. Values below 10ms are clamped.
    #[serde(default = "default_watch_debounce_ms")]
    // Immutable: read once when the watcher task starts and captured by its
    // poll loop. GET reports the honest startup value.
    #[param(name = "tls-watch-debounce-ms")]
    pub watch_debounce_ms: u64,

    /// TLS handshake timeout in milliseconds.
    #[serde(default = "default_handshake_timeout_ms")]
    #[param(mutable, name = "tls-handshake-timeout-ms")]
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
            additional_certs: Vec::new(),
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

        // A half-configured client identity would silently fall back to the
        // server certificate, presenting the wrong identity to peers.
        match (&self.client_cert_file, &self.client_key_file) {
            (Some(_), None) => {
                anyhow::bail!("tls.client_cert_file set without tls.client_key_file")
            }
            (None, Some(_)) => {
                anyhow::bail!("tls.client_key_file set without tls.client_cert_file")
            }
            _ => {}
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

        // Every additional identity must be a complete, existing pair. A
        // half-configured extra cert is an operator error, not a reason to
        // silently serve only the primary identity.
        for (i, extra) in self.additional_certs.iter().enumerate() {
            if extra.cert_file.as_os_str().is_empty() {
                anyhow::bail!("tls.additional_certs[{i}].cert_file is required");
            }
            if extra.key_file.as_os_str().is_empty() {
                anyhow::bail!("tls.additional_certs[{i}].key_file is required");
            }
            if !extra.cert_file.exists() {
                anyhow::bail!(
                    "tls.additional_certs[{i}].cert_file '{}' does not exist",
                    extra.cert_file.display()
                );
            }
            if !extra.key_file.exists() {
                anyhow::bail!(
                    "tls.additional_certs[{i}].key_file '{}' does not exist",
                    extra.key_file.display()
                );
            }
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
    fn test_additional_certs_toml_array_of_tables() {
        let toml_src = r#"
enabled = false
cert-file = "/tmp/a.crt"
key-file = "/tmp/a.key"

[[additional-certs]]
cert-file = "/tmp/ec.crt"
key-file = "/tmp/ec.key"

[[additional-certs]]
cert-file = "/tmp/rsa.crt"
key-file = "/tmp/rsa.key"
"#;
        let config: TlsConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(config.additional_certs.len(), 2);
        assert_eq!(
            config.additional_certs[0].cert_file,
            PathBuf::from("/tmp/ec.crt")
        );
        assert_eq!(
            config.additional_certs[1].key_file,
            PathBuf::from("/tmp/rsa.key")
        );
    }

    #[test]
    fn test_additional_certs_default_empty() {
        let json = r#"{"enabled": false}"#;
        let config: TlsConfig = serde_json::from_str(json).unwrap();
        assert!(config.additional_certs.is_empty());
    }

    #[test]
    fn test_additional_cert_missing_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("primary.crt");
        let key = dir.path().join("primary.key");
        std::fs::write(&cert, b"x").unwrap();
        std::fs::write(&key, b"x").unwrap();

        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            additional_certs: vec![AdditionalCert {
                cert_file: dir.path().join("nope.crt"),
                key_file: dir.path().join("nope.key"),
            }],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("additional_certs[0].cert_file"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_additional_cert_empty_key_path_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cert = dir.path().join("primary.crt");
        let key = dir.path().join("primary.key");
        std::fs::write(&cert, b"x").unwrap();
        std::fs::write(&key, b"x").unwrap();
        let extra = dir.path().join("extra.crt");
        std::fs::write(&extra, b"x").unwrap();

        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            additional_certs: vec![AdditionalCert {
                cert_file: extra,
                key_file: PathBuf::new(),
            }],
            ..Default::default()
        };
        let err = config.validate().unwrap_err();
        assert!(
            err.to_string().contains("additional_certs[0].key_file"),
            "unexpected error: {err}"
        );
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
