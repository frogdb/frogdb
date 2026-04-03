//! TLS test certificate generation helpers.
//!
//! Uses `rcgen` to generate ephemeral certificates for integration tests.
//! All certificates are written to a temporary directory that is cleaned
//! up when the `TlsFixture` is dropped.

use rcgen::{CertificateParams, Issuer, KeyPair};
use std::path::{Path, PathBuf};

/// A collection of PEM files for TLS testing.
///
/// The temporary directory is cleaned up on drop.
#[derive(Debug)]
pub struct TlsFixture {
    /// Temporary directory containing all PEM files.
    pub dir: tempfile::TempDir,
    /// Path to the CA certificate.
    pub ca_cert: PathBuf,
    /// Path to the server certificate.
    pub server_cert: PathBuf,
    /// Path to the server private key.
    pub server_key: PathBuf,
    /// Path to a client certificate (for mTLS).
    pub client_cert: PathBuf,
    /// Path to the client private key.
    pub client_key: PathBuf,
    /// CA certificate DER bytes (for building TLS client configs in tests).
    pub ca_cert_der: Vec<u8>,
    /// Client certificate DER bytes.
    pub client_cert_der: Vec<u8>,
    /// Client key DER bytes.
    pub client_key_der: Vec<u8>,
}

impl TlsFixture {
    /// Generate a complete set of test certificates.
    ///
    /// Creates a self-signed CA, server cert (with SAN=localhost,127.0.0.1),
    /// and client cert for mTLS testing.
    pub fn generate() -> Self {
        let dir = tempfile::tempdir().expect("failed to create temp dir");

        // Generate CA
        let mut ca_params = CertificateParams::new(vec!["FrogDB Test CA".to_string()]).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_issuer = Issuer::from_params(&ca_params, &ca_key);

        // Generate server cert signed by CA
        let server_params =
            CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()]).unwrap();
        let server_key = KeyPair::generate().unwrap();
        let server_cert_signed = server_params.signed_by(&server_key, &ca_issuer).unwrap();

        // Generate client cert signed by CA
        let client_params = CertificateParams::new(vec!["frogdb-test-client".to_string()]).unwrap();
        let client_key = KeyPair::generate().unwrap();
        let client_cert_signed = client_params.signed_by(&client_key, &ca_issuer).unwrap();

        // Write PEM files
        let ca_cert_path = dir.path().join("ca.crt");
        let server_cert_path = dir.path().join("server.crt");
        let server_key_path = dir.path().join("server.key");
        let client_cert_path = dir.path().join("client.crt");
        let client_key_path = dir.path().join("client.key");

        write_pem(&ca_cert_path, &ca_cert.pem());
        write_pem(&server_cert_path, &server_cert_signed.pem());
        write_pem(&server_key_path, &server_key.serialize_pem());
        write_pem(&client_cert_path, &client_cert_signed.pem());
        write_pem(&client_key_path, &client_key.serialize_pem());

        Self {
            ca_cert_der: ca_cert.der().to_vec(),
            client_cert_der: client_cert_signed.der().to_vec(),
            client_key_der: client_key.serialize_der().to_vec(),
            dir,
            ca_cert: ca_cert_path,
            server_cert: server_cert_path,
            server_key: server_key_path,
            client_cert: client_cert_path,
            client_key: client_key_path,
        }
    }
}

fn write_pem(path: &Path, contents: &str) {
    std::fs::write(path, contents)
        .unwrap_or_else(|e| panic!("failed to write {}: {}", path.display(), e));
}
