//! Runtime handle for live TLS reconfiguration.
//!
//! [`TlsManager`] can already swap its rustls configs behind an `ArcSwap`, but it
//! is stateless with respect to *what* the current TLS configuration is: every
//! `reload` needs a fully-populated [`TlsConfig`]. [`TlsRuntimeHandle`] closes
//! that gap. It owns the manager plus the currently-effective `TlsConfig`, so a
//! CONFIG SET that changes a single field (`tls-cert-file`, `tls-ciphersuites`,
//! ...) can be expressed as a mutation of the stored config followed by a
//! rebuild.
//!
//! Every mutation is **build-then-swap**: the candidate config is a *clone* of
//! the stored one, the rustls configs are rebuilt from the candidate, and only a
//! successful rebuild commits the candidate as the new effective config. A bad
//! path, an unparseable PEM or an unknown ciphersuite therefore leaves the old
//! certificates serving, and the caller gets the error.
//!
//! This module is only compiled in non-turmoil builds (see [`crate::tls`]).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use frogdb_config::TlsConfig;

use crate::tls::TlsManager;

/// Owns the live [`TlsManager`] and the TLS configuration it was last built
/// from, and applies configuration changes to both atomically.
///
/// Cheap to clone behind an `Arc`; all mutation goes through `&self`.
pub struct TlsRuntimeHandle {
    /// The manager whose `ArcSwap`ped rustls configs back every acceptor and
    /// connector handed out to connection code.
    manager: Arc<TlsManager>,
    /// The configuration `manager`'s current rustls configs were built from.
    ///
    /// Held under a `Mutex` rather than an `ArcSwap` because mutations are
    /// read-modify-write: two concurrent CONFIG SETs to different TLS fields
    /// must not lose one of the writes.
    config: Mutex<TlsConfig>,
}

impl TlsRuntimeHandle {
    /// Build the manager from `config` and wrap both in a handle.
    pub fn new(config: &TlsConfig) -> anyhow::Result<Self> {
        let manager = Arc::new(TlsManager::new(config)?);
        Ok(Self::from_parts(manager, config.clone()))
    }

    /// Wrap an already-constructed manager together with the config it was
    /// built from.
    pub fn from_parts(manager: Arc<TlsManager>, config: TlsConfig) -> Self {
        Self {
            manager,
            config: Mutex::new(config),
        }
    }

    /// The live TLS manager. Call `acceptor()`/`connector()` on it *per
    /// connection* — a cached acceptor pins the certificates it was created
    /// with and would not see reloads.
    pub fn manager(&self) -> &Arc<TlsManager> {
        &self.manager
    }

    /// A snapshot of the currently-effective TLS configuration.
    pub fn current_config(&self) -> TlsConfig {
        self.config.lock().unwrap().clone()
    }

    /// Apply `mutate` to a clone of the effective config, rebuild the rustls
    /// configs from it, and commit the clone only if the rebuild succeeded.
    ///
    /// On error nothing is committed: the manager keeps serving the previous
    /// certificates and the stored config is unchanged.
    fn apply(&self, mutate: impl FnOnce(&mut TlsConfig)) -> anyhow::Result<()> {
        let mut guard = self.config.lock().unwrap();
        let mut candidate = guard.clone();
        mutate(&mut candidate);
        // `TlsManager::reload` is itself build-then-swap: it only stores the new
        // rustls configs after both have been built successfully.
        self.manager.reload(&candidate)?;
        *guard = candidate;
        Ok(())
    }

    /// Re-read every certificate file from its current path.
    ///
    /// This is the rotation-in-place idiom: the operator overwrites the PEM
    /// files under the configured paths and asks the server to pick them up
    /// without changing any configuration value.
    pub fn reload_current(&self) -> anyhow::Result<()> {
        self.apply(|_| {})
    }

    /// Point the primary server identity at a new certificate file.
    pub fn set_cert_file(&self, path: PathBuf) -> anyhow::Result<()> {
        self.apply(|c| c.cert_file = path)
    }

    /// Point the primary server identity at a new private key file.
    pub fn set_key_file(&self, path: PathBuf) -> anyhow::Result<()> {
        self.apply(|c| c.key_file = path)
    }

    /// Set (or clear, with `None`) the client certificate used for outgoing
    /// replication/cluster connections.
    pub fn set_client_cert_file(&self, path: Option<PathBuf>) -> anyhow::Result<()> {
        self.apply(|c| c.client_cert_file = path)
    }

    /// Set (or clear, with `None`) the client key used for outgoing
    /// replication/cluster connections.
    pub fn set_client_key_file(&self, path: Option<PathBuf>) -> anyhow::Result<()> {
        self.apply(|c| c.client_key_file = path)
    }

    /// Set the CA bundle used for client-certificate verification and for
    /// verifying peers on outgoing connections.
    pub fn set_ca_file(&self, path: Option<PathBuf>) -> anyhow::Result<()> {
        self.apply(|c| c.ca_file = path)
    }

    /// Restrict the offered ciphersuites (empty = rustls defaults).
    pub fn set_ciphersuites(&self, suites: Vec<String>) -> anyhow::Result<()> {
        self.apply(|c| c.ciphersuites = suites)
    }

    /// Replace the extra server identities offered alongside the primary pair.
    pub fn set_additional_certs(
        &self,
        certs: Vec<frogdb_config::AdditionalCert>,
    ) -> anyhow::Result<()> {
        self.apply(|c| c.additional_certs = certs)
    }
}

impl std::fmt::Debug for TlsRuntimeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsRuntimeHandle")
            .field("cert_file", &self.current_config().cert_file)
            .finish_non_exhaustive()
    }
}

/// Test support shared by this module's tests and the certificate-watcher
/// tests: certificate fixtures plus a real loopback TLS handshake, so
/// assertions are about the certificate a client is actually served rather
/// than about internal state.
#[cfg(test)]
pub(crate) mod test_support {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, SignatureScheme};

    use super::TlsRuntimeHandle;

    fn self_signed() -> (rcgen::KeyPair, rcgen::Certificate) {
        let key = rcgen::KeyPair::generate().unwrap();
        let params =
            rcgen::CertificateParams::new(vec!["localhost".to_string(), "127.0.0.1".to_string()])
                .unwrap();
        let cert = params.self_signed(&key).unwrap();
        (key, cert)
    }

    /// Write a fresh self-signed identity to `dir/<stem>.crt` + `dir/<stem>.key`.
    pub(crate) fn write_identity(dir: &Path, stem: &str) -> (PathBuf, PathBuf) {
        let (key, cert) = self_signed();
        let cert_path = dir.join(format!("{stem}.crt"));
        let key_path = dir.join(format!("{stem}.key"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    /// Overwrite `cert`/`key` in place with brand-new material, returning the
    /// new leaf DER. Simulates an operator rotating certificates under the
    /// configured paths.
    pub(crate) fn rotate_in_place(cert: &Path, key: &Path) -> Vec<u8> {
        let (new_key, new_cert) = self_signed();
        // Key first: a watcher firing between the two writes sees a mismatched
        // pair, fails that reload, and succeeds on the next event.
        std::fs::write(key, new_key.serialize_pem()).unwrap();
        std::fs::write(cert, new_cert.pem()).unwrap();
        new_cert.der().to_vec()
    }

    /// The DER of the first certificate in a PEM file.
    pub(crate) fn leaf_der(pem_path: &Path) -> Vec<u8> {
        let pem = std::fs::read(pem_path).unwrap();
        rustls_pemfile::certs(&mut pem.as_slice())
            .next()
            .unwrap()
            .unwrap()
            .to_vec()
    }

    /// A client-side verifier that accepts any server certificate.
    ///
    /// Test-only: these tests are about *which* certificate the server
    /// presents, not about chain validation.
    #[derive(Debug)]
    struct AcceptAnyServerCert(Arc<rustls::crypto::CryptoProvider>);

    impl ServerCertVerifier for AcceptAnyServerCert {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    /// Complete one real TLS handshake against `handle`'s current acceptor and
    /// return the DER of the leaf certificate the server presented.
    ///
    /// The acceptor is fetched *inside* this helper, per connection — the same
    /// discipline production accept loops must follow for reloads to take
    /// effect.
    pub(crate) async fn handshake_leaf(handle: &TlsRuntimeHandle) -> Vec<u8> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = handle.manager().acceptor();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(stream).await;
        });

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client_config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(provider)))
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let tls = connector
            .connect(ServerName::from(addr.ip()), tcp)
            .await
            .expect("loopback handshake succeeds");
        let leaf = tls
            .get_ref()
            .1
            .peer_certificates()
            .expect("server presented a certificate")[0]
            .to_vec();
        server.await.unwrap();
        leaf
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    fn handle_with(cert: PathBuf, key: PathBuf) -> TlsRuntimeHandle {
        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            ..Default::default()
        };
        TlsRuntimeHandle::new(&config).unwrap()
    }

    #[tokio::test]
    async fn reload_with_bad_cert_path_errors_and_old_config_still_serves() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "good");
        let handle = handle_with(cert.clone(), key);
        let before = handshake_leaf(&handle).await;

        let err = handle
            .set_cert_file(dir.path().join("does-not-exist.crt"))
            .unwrap_err();
        assert!(
            err.to_string().contains("failed to open cert file"),
            "unexpected error: {err}"
        );

        // Stored config untouched ...
        assert_eq!(handle.current_config().cert_file, cert);
        // ... and clients are still served the original certificate.
        assert_eq!(before, handshake_leaf(&handle).await);
    }

    #[tokio::test]
    async fn reload_with_unknown_ciphersuite_errors_and_old_config_still_serves() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "good");
        let handle = handle_with(cert, key);
        let before = handshake_leaf(&handle).await;

        let err = handle
            .set_ciphersuites(vec!["TLS_NOPE".to_string()])
            .unwrap_err();
        assert!(err.to_string().contains("unknown tls.ciphersuites"));
        assert!(handle.current_config().ciphersuites.is_empty());
        assert_eq!(before, handshake_leaf(&handle).await);
    }

    #[tokio::test]
    async fn mismatched_intermediate_pair_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_a, key_a) = write_identity(dir.path(), "a");
        let (_cert_b, key_b) = write_identity(dir.path(), "b");
        let handle = handle_with(cert_a, key_a);
        let before = handshake_leaf(&handle).await;

        // Swapping only the key leaves cert A paired with key B.
        let err = handle.set_key_file(key_b).unwrap_err();
        assert!(
            err.to_string().contains("failed to load TLS identity"),
            "unexpected error: {err}"
        );
        assert_eq!(before, handshake_leaf(&handle).await);
    }

    #[tokio::test]
    async fn new_cert_is_served_by_the_next_acceptor() {
        let dir = tempfile::tempdir().unwrap();
        let (cert_a, key_a) = write_identity(dir.path(), "a");
        let (cert_b, key_b) = write_identity(dir.path(), "b");
        let handle = handle_with(cert_a, key_a);
        let before = handshake_leaf(&handle).await;

        handle
            .apply(|c| {
                c.cert_file = cert_b.clone();
                c.key_file = key_b.clone();
            })
            .unwrap();

        let after = handshake_leaf(&handle).await;
        assert_ne!(before, after, "acceptor() must serve the new certificate");
        assert_eq!(after, leaf_der(&cert_b));
        assert_eq!(handle.current_config().cert_file, cert_b);
    }

    #[tokio::test]
    async fn reload_current_picks_up_rotation_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "rotating");
        let handle = handle_with(cert.clone(), key.clone());
        let before = handshake_leaf(&handle).await;

        let rotated = rotate_in_place(&cert, &key);
        handle.reload_current().unwrap();

        let after = handshake_leaf(&handle).await;
        assert_ne!(before, after);
        assert_eq!(after, rotated);
        // Rotation in place never changes the configured paths.
        assert_eq!(handle.current_config().cert_file, cert);
    }

    #[tokio::test]
    async fn additional_certs_can_be_added_at_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "primary");
        let (extra_cert, extra_key) = write_identity(dir.path(), "extra");
        let handle = handle_with(cert.clone(), key);
        assert!(handle.current_config().additional_certs.is_empty());

        handle
            .set_additional_certs(vec![frogdb_config::AdditionalCert {
                cert_file: extra_cert.clone(),
                key_file: extra_key,
            }])
            .unwrap();
        assert_eq!(handle.current_config().additional_certs.len(), 1);
        // The primary identity stays primary for a default client hello.
        assert_eq!(handshake_leaf(&handle).await, leaf_der(&cert));

        // A bad extra pair is rejected and leaves the good list in place.
        let err = handle
            .set_additional_certs(vec![frogdb_config::AdditionalCert {
                cert_file: dir.path().join("missing.crt"),
                key_file: dir.path().join("missing.key"),
            }])
            .unwrap_err();
        assert!(
            err.to_string().contains("failed to open cert file"),
            "{err}"
        );
        assert_eq!(
            handle.current_config().additional_certs[0].cert_file,
            extra_cert
        );
    }

    #[tokio::test]
    async fn client_identity_setters_rebuild_the_connector() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "server");
        let (client_cert, client_key) = write_identity(dir.path(), "client");
        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            tls_replication: true,
            ..Default::default()
        };
        let handle = TlsRuntimeHandle::new(&config).unwrap();
        assert!(handle.manager().connector().is_some());

        // A client cert without its matching key must not silently fall back to
        // the server identity.
        let err = handle
            .set_client_cert_file(Some(client_cert.clone()))
            .unwrap_err();
        assert!(
            err.to_string().contains("must be set together"),
            "unexpected error: {err}"
        );
        assert!(handle.current_config().client_cert_file.is_none());

        handle
            .apply(|c| {
                c.client_cert_file = Some(client_cert.clone());
                c.client_key_file = Some(client_key.clone());
            })
            .unwrap();
        assert_eq!(handle.current_config().client_cert_file, Some(client_cert));
        assert!(handle.manager().connector().is_some());
    }
}
