//! TLS stream abstraction and TLS manager.
//!
//! This module is only compiled in non-turmoil builds. Turmoil simulation
//! uses plain TCP streams and does not support TLS.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arc_swap::ArcSwap;
use pin_project_lite::pin_project;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::warn;

use frogdb_config::{ClientCertMode, TlsConfig, TlsProtocol};
use frogdb_replication::BoxedStream;

pin_project! {
    /// A stream that is either plaintext TCP or TLS-wrapped TCP.
    ///
    /// This enum allows the server to handle both plain and TLS connections
    /// through a single code path using `AsyncRead + AsyncWrite`.
    #[project = MaybeTlsStreamProj]
    pub enum MaybeTlsStream {
        /// Plaintext TCP connection.
        Plain { #[pin] inner: tokio::net::TcpStream },
        /// TLS-encrypted TCP connection.
        Tls { #[pin] inner: tokio_rustls::server::TlsStream<tokio::net::TcpStream> },
    }
}

impl MaybeTlsStream {
    /// Get the peer address of the underlying TCP stream.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            MaybeTlsStream::Plain { inner } => inner.peer_addr(),
            MaybeTlsStream::Tls { inner } => inner.get_ref().0.peer_addr(),
        }
    }

    /// Get the local address of the underlying TCP stream.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            MaybeTlsStream::Plain { inner } => inner.local_addr(),
            MaybeTlsStream::Tls { inner } => inner.get_ref().0.local_addr(),
        }
    }

    /// Consume self and return the underlying TCP stream.
    ///
    /// For TLS connections, this drops the TLS session and returns the raw TCP stream.
    pub fn into_tcp_stream(self) -> tokio::net::TcpStream {
        match self {
            MaybeTlsStream::Plain { inner } => inner,
            MaybeTlsStream::Tls { inner } => inner.into_inner().0,
        }
    }

    /// Consume self and return a type-erased boxed async I/O stream.
    ///
    /// Unlike `into_tcp_stream()`, this preserves the TLS session so the
    /// connection remains encrypted end-to-end (e.g. during PSYNC handoff).
    pub fn into_boxed(self) -> BoxedStream {
        match self {
            MaybeTlsStream::Plain { inner } => Box::new(inner),
            MaybeTlsStream::Tls { inner } => Box::new(inner),
        }
    }
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            MaybeTlsStreamProj::Plain { inner } => inner.poll_read(cx, buf),
            MaybeTlsStreamProj::Tls { inner } => inner.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            MaybeTlsStreamProj::Plain { inner } => inner.poll_write(cx, buf),
            MaybeTlsStreamProj::Tls { inner } => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            MaybeTlsStreamProj::Plain { inner } => inner.poll_flush(cx),
            MaybeTlsStreamProj::Tls { inner } => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            MaybeTlsStreamProj::Plain { inner } => inner.poll_shutdown(cx),
            MaybeTlsStreamProj::Tls { inner } => inner.poll_shutdown(cx),
        }
    }
}

// ---------------------------------------------------------------------------
// TlsManager — loads certificates and builds rustls ServerConfig
// ---------------------------------------------------------------------------

/// Manages TLS configuration and provides TLS acceptors and connectors.
///
/// The configs are stored behind `ArcSwap` to support hot-reloading
/// without disrupting existing connections.
pub struct TlsManager {
    server_config: ArcSwap<ServerConfig>,
    /// Client config for outgoing TLS connections (cluster bus, replication).
    /// `None` when neither `tls_cluster` nor `tls_replication` is enabled.
    client_config: Option<ArcSwap<ClientConfig>>,
}

impl TlsManager {
    /// Create a new TLS manager from the TLS configuration.
    ///
    /// Loads certificates and private key from disk, builds the rustls ServerConfig,
    /// and optionally configures client certificate verification for mTLS.
    /// When `tls_cluster` or `tls_replication` is enabled, also builds a ClientConfig
    /// for outgoing connections.
    pub fn new(config: &TlsConfig) -> anyhow::Result<Self> {
        // Ensure a crypto provider is installed (idempotent if already set)
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let server_config = build_server_config(config)?;
        validate_client_identity(config)?;

        let client_config = if config.tls_cluster || config.tls_replication {
            Some(ArcSwap::from_pointee(build_client_config(config)?))
        } else {
            None
        };

        Ok(Self {
            server_config: ArcSwap::from_pointee(server_config),
            client_config,
        })
    }

    /// Get a TLS acceptor using the current server configuration.
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config.load_full())
    }

    /// Get a TLS connector for outgoing connections, if client TLS is configured.
    pub fn connector(&self) -> Option<TlsConnector> {
        self.client_config
            .as_ref()
            .map(|c| TlsConnector::from(c.load_full()))
    }

    /// Get the current server configuration.
    pub fn current_server_config(&self) -> Arc<ServerConfig> {
        self.server_config.load_full()
    }

    /// Reload certificates from disk, updating both server and client configs.
    ///
    /// Both configs are built *before* either is published, so a failure leaves
    /// the manager entirely on the previous generation. Publishing the server
    /// config first and then failing to build the client config would leave
    /// incoming connections on the new certificates while outgoing ones kept
    /// presenting the old identity — a divergence no later reload undoes,
    /// because the next attempt fails at the same place.
    pub fn reload(&self, config: &TlsConfig) -> anyhow::Result<()> {
        let new_server = Arc::new(build_server_config(config)?);
        validate_client_identity(config)?;
        let new_client = match self.client_config {
            Some(_) => Some(Arc::new(build_client_config(config)?)),
            None => None,
        };

        self.server_config.store(new_server);
        if let (Some(client_swap), Some(new_client)) = (&self.client_config, new_client) {
            client_swap.store(new_client);
        }

        Ok(())
    }
}

/// Check that whatever half of the client identity is configured is loadable.
///
/// Runs even when no client config is built: the pair is settable while
/// `tls_cluster`/`tls_replication` are off, and a value CONFIG SET accepted must
/// not turn out to be unreadable on the day TLS peering is switched on. It also
/// covers the half-set pair, which `build_client_config` deliberately does not
/// read (see [`crate::tls_runtime`] for the contract).
fn validate_client_identity(config: &TlsConfig) -> anyhow::Result<()> {
    if let Some(path) = &config.client_cert_file {
        load_certs(path)?;
    }
    if let Some(path) = &config.client_key_file {
        load_private_key(path)?;
    }
    Ok(())
}

/// Load PEM-encoded certificates from a file.
fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("failed to open cert file '{}': {}", path.display(), e))?;
    let mut reader = io::BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse certs from '{}': {}", path.display(), e))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in '{}'", path.display());
    }
    Ok(certs)
}

/// Load a PEM-encoded private key from a file.
fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("failed to open key file '{}': {}", path.display(), e))?;
    let mut reader = io::BufReader::new(file);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| anyhow::anyhow!("failed to parse key from '{}': {}", path.display(), e))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in '{}'", path.display()))?;
    Ok(key)
}

/// Load PEM-encoded CA certificates into a root cert store.
fn load_ca_certs(path: &Path) -> anyhow::Result<RootCertStore> {
    let file = std::fs::File::open(path)
        .map_err(|e| anyhow::anyhow!("failed to open CA file '{}': {}", path.display(), e))?;
    let mut reader = io::BufReader::new(file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            anyhow::anyhow!("failed to parse CA certs from '{}': {}", path.display(), e)
        })?;
    if certs.is_empty() {
        // A truncated or wrong-format CA bundle parses cleanly as zero certs.
        // Accepting it would build an empty root store, i.e. silently verify
        // nothing — refuse it exactly like `load_certs` does.
        anyhow::bail!("no CA certificates found in '{}'", path.display());
    }
    let mut store = RootCertStore::empty();
    for cert in certs {
        store.add(cert)?;
    }
    Ok(store)
}

/// Build a crypto provider restricted to the named `ciphersuites`.
///
/// Returns `Ok(None)` when the list is empty, signalling that the caller should
/// use rustls' default provider and full suite list unchanged. Otherwise returns
/// a provider whose `cipher_suites` are exactly the named suites, in the order
/// requested.
///
/// Suite names are matched case-insensitively against rustls' IANA names
/// (e.g. `TLS13_AES_256_GCM_SHA384`, `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`).
/// An unknown name is a hard error listing the valid names. If none of the named
/// suites are usable with the enabled `versions` (e.g. only TLS 1.2 suites named
/// while only TLS 1.3 is enabled), that too is a hard error rather than a silent
/// fallback to the defaults.
fn ciphersuite_provider(
    ciphersuites: &[String],
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> anyhow::Result<Option<Arc<rustls::crypto::CryptoProvider>>> {
    if ciphersuites.is_empty() {
        return Ok(None);
    }

    let base = rustls::crypto::aws_lc_rs::default_provider();

    let mut selected: Vec<rustls::SupportedCipherSuite> = Vec::with_capacity(ciphersuites.len());
    let mut unknown: Vec<&str> = Vec::new();
    for name in ciphersuites {
        match base.cipher_suites.iter().find(|s| {
            s.suite()
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(name))
        }) {
            // De-dupe: a repeated name should not add the suite twice.
            Some(suite) if !selected.iter().any(|s| s.suite() == suite.suite()) => {
                selected.push(*suite);
            }
            Some(_) => {}
            None => unknown.push(name.as_str()),
        }
    }

    if !unknown.is_empty() {
        anyhow::bail!(
            "unknown tls.ciphersuites {:?}; valid names are: {}",
            unknown,
            valid_ciphersuite_names(&base).join(", ")
        );
    }

    // Guard against a named set that excludes every suite compatible with the
    // enabled protocol versions. rustls would also reject this in
    // `with_protocol_versions`, but a targeted message is clearer.
    if !selected.iter().any(|s| versions.contains(&s.version())) {
        anyhow::bail!(
            "none of the configured tls.ciphersuites {:?} are usable with the enabled \
             tls.protocols; name suites matching an enabled protocol version",
            ciphersuites
        );
    }

    let provider = rustls::crypto::CryptoProvider {
        cipher_suites: selected,
        ..base
    };
    Ok(Some(Arc::new(provider)))
}

/// The IANA names of all cipher suites offered by the default aws-lc-rs provider.
fn valid_ciphersuite_names(provider: &rustls::crypto::CryptoProvider) -> Vec<&'static str> {
    provider
        .cipher_suites
        .iter()
        .filter_map(|s| s.suite().as_str())
        .collect()
}

// ---------------------------------------------------------------------------
// Multi-certificate resolver
// ---------------------------------------------------------------------------

/// Serves one of several configured server identities, chosen per ClientHello.
///
/// Holds every configured `CertifiedKey` with the primary
/// (`tls.cert-file`/`tls.key-file`) pair first, followed by
/// `tls.additional-certs` in configuration order. On each handshake the first
/// identity whose signing key can produce a signature in one of the schemes the
/// client advertised wins; if none can, the primary identity is served anyway so
/// the client sees a normal handshake failure (or succeeds, for a client whose
/// advertised list was merely incomplete) rather than a `no certificate`
/// alert — this matches rustls' single-cert behaviour, which never consults the
/// client's scheme list at all.
///
/// This is what makes RSA + ECDSA dual-cert deployments work: an ECDSA-only
/// client gets the ECDSA identity, an RSA-only client gets the RSA one.
#[derive(Debug)]
pub struct MultiCertResolver {
    /// Configured identities, primary first. Never empty.
    keys: Vec<Arc<CertifiedKey>>,
}

impl MultiCertResolver {
    /// Build a resolver over `keys`, which must be non-empty and ordered with
    /// the primary identity first.
    fn new(keys: Vec<Arc<CertifiedKey>>) -> anyhow::Result<Self> {
        if keys.is_empty() {
            anyhow::bail!("TLS certificate resolver requires at least one certificate/key pair");
        }
        Ok(Self { keys })
    }

    /// Number of configured identities (primary + additional).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Always false — a resolver is never built without a primary identity.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The selection rule, factored out of [`ResolvesServerCert::resolve`] so it
    /// is directly testable: `rustls::server::ClientHello` has no public
    /// constructor, so tests drive the scheme list instead.
    fn select(&self, offered: &[rustls::SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        self.keys
            .iter()
            .find(|ck| ck.key.choose_scheme(offered).is_some())
            .or_else(|| self.keys.first())
            .cloned()
    }
}

impl ResolvesServerCert for MultiCertResolver {
    fn resolve(&self, client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.select(client_hello.signature_schemes())
    }
}

/// Load one certificate/key pair into a rustls `CertifiedKey`.
///
/// `CertifiedKey::from_der` verifies that the key matches the leaf certificate
/// when the key's public key is recoverable, so a mismatched pair fails here
/// rather than at handshake time.
fn load_certified_key(
    cert_path: &Path,
    key_path: &Path,
    provider: &rustls::crypto::CryptoProvider,
) -> anyhow::Result<Arc<CertifiedKey>> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let certified = CertifiedKey::from_der(certs, key, provider).map_err(|e| {
        anyhow::anyhow!(
            "failed to load TLS identity from cert '{}' + key '{}': {}",
            cert_path.display(),
            key_path.display(),
            e
        )
    })?;
    Ok(Arc::new(certified))
}

/// Build the [`MultiCertResolver`] for `config`: the primary pair first, then
/// every `additional_certs` entry in order.
fn build_cert_resolver(
    config: &TlsConfig,
    provider: &rustls::crypto::CryptoProvider,
) -> anyhow::Result<Arc<MultiCertResolver>> {
    let mut keys = Vec::with_capacity(1 + config.additional_certs.len());
    keys.push(load_certified_key(
        &config.cert_file,
        &config.key_file,
        provider,
    )?);
    for extra in &config.additional_certs {
        keys.push(load_certified_key(
            &extra.cert_file,
            &extra.key_file,
            provider,
        )?);
    }
    Ok(Arc::new(MultiCertResolver::new(keys)?))
}

/// Build a rustls ServerConfig from the TLS configuration.
fn build_server_config(config: &TlsConfig) -> anyhow::Result<ServerConfig> {
    // Determine protocol versions
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = config
        .protocols
        .iter()
        .map(|p| match p {
            TlsProtocol::Tls12 => &rustls::version::TLS12,
            TlsProtocol::Tls13 => &rustls::version::TLS13,
        })
        .collect();

    // Build the server config with appropriate client cert verification.
    // A non-empty `ciphersuites` list restricts the crypto provider's suites;
    // an empty list keeps rustls' default full suite list.
    let restricted = ciphersuite_provider(&config.ciphersuites, &versions)?;

    // Private keys are loaded through the same provider the config is built
    // with, so a restricted suite list and the key provider stay consistent.
    // Naming the provider explicitly (rather than letting rustls reach for the
    // process-global default) keeps key loading, suite selection and client-cert
    // verification on one provider, and makes config building independent of
    // whether `install_default` has run yet.
    let provider =
        restricted.unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let resolver = build_cert_resolver(config, &provider)?;

    let builder =
        ServerConfig::builder_with_provider(provider.clone()).with_protocol_versions(&versions)?;

    let server_config = match config.require_client_cert {
        ClientCertMode::None => builder.with_no_client_auth().with_cert_resolver(resolver),
        ClientCertMode::Optional | ClientCertMode::Required => {
            let ca_file = config
                .ca_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("ca_file required for client cert verification"))?;
            let ca_store = load_ca_certs(ca_file)?;

            // Build the verifier against the *same* provider the rest of the
            // config uses, rather than the process-global default: the
            // ciphersuite-restricted provider must apply to client-cert
            // verification too, and a config built before any global provider
            // is installed must not panic.
            let verifier_builder =
                WebPkiClientVerifier::builder_with_provider(Arc::new(ca_store), provider);
            let verifier = if config.require_client_cert == ClientCertMode::Required {
                verifier_builder.build()?
            } else {
                verifier_builder.allow_unauthenticated().build()?
            };

            builder
                .with_client_cert_verifier(verifier)
                .with_cert_resolver(resolver)
        }
    };

    Ok(server_config)
}

/// Build a rustls ClientConfig for outgoing TLS connections (cluster bus, replication).
///
/// Uses `client_cert_file`/`client_key_file` for client identity when *both* are
/// set, and the server `cert_file`/`key_file` otherwise — including when only
/// one half of the pair is set, which is warned about loudly. See
/// [`crate::tls_runtime`] for why a half-set pair must remain a legal,
/// boot-valid state rather than a hard error.
///
/// Uses `ca_file` for server verification if set, otherwise uses system/webpki roots.
fn build_client_config(config: &TlsConfig) -> anyhow::Result<ClientConfig> {
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = config
        .protocols
        .iter()
        .map(|p| match p {
            TlsProtocol::Tls12 => &rustls::version::TLS12,
            TlsProtocol::Tls13 => &rustls::version::TLS13,
        })
        .collect();

    // Build root cert store for verifying the remote server's certificate
    let root_store = if let Some(ref ca_file) = config.ca_file {
        load_ca_certs(ca_file)?
    } else {
        let mut store = RootCertStore::empty();
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        store
    };

    // Mirror the server path: restrict the provider's suites when named, and
    // always name the provider explicitly.
    let provider = ciphersuite_provider(&config.ciphersuites, &versions)?
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()));
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&versions)?
        .with_root_certificates(root_store);

    // Use client cert/key if available (for mTLS to peers), fall back to server cert/key
    let (cert_path, key_path) = match (&config.client_cert_file, &config.client_key_file) {
        (Some(cert), Some(key)) => (cert.as_path(), key.as_path()),
        // Half-set: the pair is *mid-rotation*, not misconfigured. CONFIG SET
        // applies one parameter at a time, so every complete pair is reached
        // through this state; failing here would make the pair unsettable.
        (client_cert, client_key) => {
            if let Some(configured) = client_cert.as_ref().or(client_key.as_ref()) {
                warn!(
                    path = %configured.display(),
                    "TLS client identity is incomplete (only one of \
                     tls-client-cert-file/tls-client-key-file is set); outgoing \
                     connections present the server certificate until both are set"
                );
            }
            (config.cert_file.as_path(), config.key_file.as_path())
        }
    };

    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let client_config = builder.with_client_auth_cert(certs, key)?;
    Ok(client_config)
}

/// Connect to a remote address over TLS, returning a type-erased stream.
///
/// This is used by the server crate to provide TLS-wrapped connection factories
/// to the cluster and replication crates without leaking TLS types.
pub async fn tls_connect(
    connector: &TlsConnector,
    addr: std::net::SocketAddr,
    timeout_duration: std::time::Duration,
) -> io::Result<BoxedStream> {
    let tcp = tokio::time::timeout(timeout_duration, tokio::net::TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS connect timeout"))??;

    let server_name = ServerName::from(addr.ip());
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;

    Ok(Box::new(tls_stream))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TLS13: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS13];
    const TLS12: &[&rustls::SupportedProtocolVersion] = &[&rustls::version::TLS12];
    const BOTH: &[&rustls::SupportedProtocolVersion] =
        &[&rustls::version::TLS13, &rustls::version::TLS12];

    /// The IANA names of the suites carried by a provider, uppercased.
    fn suite_names(provider: &rustls::crypto::CryptoProvider) -> Vec<String> {
        provider
            .cipher_suites
            .iter()
            .filter_map(|s| s.suite().as_str().map(|n| n.to_ascii_uppercase()))
            .collect()
    }

    #[test]
    fn empty_ciphersuites_uses_defaults() {
        // Empty list => None, i.e. caller keeps rustls' full default suite list.
        let provider = ciphersuite_provider(&[], BOTH).unwrap();
        assert!(provider.is_none());
    }

    #[test]
    fn named_subset_restricts_provider_suites() {
        let names = vec![
            "TLS13_AES_256_GCM_SHA384".to_string(),
            "TLS13_AES_128_GCM_SHA256".to_string(),
        ];
        let provider = ciphersuite_provider(&names, TLS13)
            .unwrap()
            .expect("named suites yield a filtered provider");
        // The provider carries exactly the two named suites, nothing else.
        assert_eq!(
            suite_names(&provider),
            vec![
                "TLS13_AES_256_GCM_SHA384".to_string(),
                "TLS13_AES_128_GCM_SHA256".to_string(),
            ]
        );
    }

    #[test]
    fn matching_is_case_insensitive() {
        let names = vec!["tls13_aes_256_gcm_sha384".to_string()];
        let provider = ciphersuite_provider(&names, TLS13).unwrap().unwrap();
        assert_eq!(
            suite_names(&provider),
            vec!["TLS13_AES_256_GCM_SHA384".to_string()]
        );
    }

    #[test]
    fn repeated_name_is_deduped() {
        let names = vec![
            "TLS13_AES_128_GCM_SHA256".to_string(),
            "TLS13_AES_128_GCM_SHA256".to_string(),
        ];
        let provider = ciphersuite_provider(&names, TLS13).unwrap().unwrap();
        assert_eq!(provider.cipher_suites.len(), 1);
    }

    #[test]
    fn unknown_name_is_a_hard_error_listing_valid_names() {
        let names = vec!["TLS_NOT_A_REAL_SUITE".to_string()];
        let err = ciphersuite_provider(&names, TLS13).unwrap_err();
        let msg = err.to_string();
        // Error names the offending suite ...
        assert!(
            msg.contains("TLS_NOT_A_REAL_SUITE"),
            "error should name the unknown suite: {msg}"
        );
        // ... and lists at least one valid suite name to guide the operator.
        assert!(
            msg.contains("TLS13_AES_256_GCM_SHA384"),
            "error should list valid names: {msg}"
        );
    }

    #[test]
    fn named_suites_incompatible_with_protocol_fail_loudly() {
        // Only a TLS 1.2 suite is named, but only TLS 1.3 is enabled: must be a
        // hard error, not a silent fallback to the default suite list.
        let names = vec!["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()];
        let err = ciphersuite_provider(&names, TLS13).unwrap_err();
        assert!(
            err.to_string().contains("usable with the enabled"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn tls12_suite_ok_when_tls12_enabled() {
        let names = vec!["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()];
        let provider = ciphersuite_provider(&names, TLS12).unwrap().unwrap();
        assert_eq!(
            suite_names(&provider),
            vec!["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()]
        );
    }

    // -----------------------------------------------------------------------
    // Multi-cert resolver
    // -----------------------------------------------------------------------

    use rustls::SignatureScheme;

    /// Self-signed identity written to `dir` as `<stem>.crt` / `<stem>.key`.
    fn write_identity(
        dir: &Path,
        stem: &str,
        key: rcgen::KeyPair,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.self_signed(&key).unwrap();
        let cert_path = dir.join(format!("{stem}.crt"));
        let key_path = dir.join(format!("{stem}.key"));
        std::fs::write(&cert_path, cert.pem()).unwrap();
        std::fs::write(&key_path, key.serialize_pem()).unwrap();
        (cert_path, key_path)
    }

    fn ecdsa_key() -> rcgen::KeyPair {
        rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap()
    }

    fn rsa_key() -> rcgen::KeyPair {
        rcgen::KeyPair::generate_rsa_for(&rcgen::PKCS_RSA_SHA256, rcgen::RsaKeySize::_2048).unwrap()
    }

    fn provider() -> Arc<rustls::crypto::CryptoProvider> {
        Arc::new(rustls::crypto::aws_lc_rs::default_provider())
    }

    /// A resolver whose primary identity is RSA and whose additional identity
    /// is ECDSA, plus the DER of each leaf cert for identity assertions.
    struct DualCertFixture {
        _dir: tempfile::TempDir,
        resolver: Arc<MultiCertResolver>,
        rsa_leaf: Vec<u8>,
        ecdsa_leaf: Vec<u8>,
    }

    fn dual_cert_fixture() -> DualCertFixture {
        let dir = tempfile::tempdir().unwrap();
        let (rsa_cert, rsa_key_path) = write_identity(dir.path(), "rsa", rsa_key());
        let (ec_cert, ec_key_path) = write_identity(dir.path(), "ecdsa", ecdsa_key());

        let config = TlsConfig {
            enabled: true,
            cert_file: rsa_cert,
            key_file: rsa_key_path,
            additional_certs: vec![frogdb_config::AdditionalCert {
                cert_file: ec_cert,
                key_file: ec_key_path,
            }],
            ..Default::default()
        };
        let p = provider();
        let resolver = build_cert_resolver(&config, &p).unwrap();
        let rsa_leaf = resolver.keys[0].cert[0].to_vec();
        let ecdsa_leaf = resolver.keys[1].cert[0].to_vec();
        DualCertFixture {
            _dir: dir,
            resolver,
            rsa_leaf,
            ecdsa_leaf,
        }
    }

    #[test]
    fn resolver_holds_primary_first_then_additional() {
        let fx = dual_cert_fixture();
        assert_eq!(fx.resolver.len(), 2);
        assert!(!fx.resolver.is_empty());
        // Primary (tls.cert-file) is index 0, additional-certs follow in order.
        assert_eq!(fx.resolver.keys[0].cert[0].to_vec(), fx.rsa_leaf);
        assert_eq!(fx.resolver.keys[1].cert[0].to_vec(), fx.ecdsa_leaf);
    }

    #[test]
    fn resolver_picks_ecdsa_for_ecdsa_only_client_hello() {
        let fx = dual_cert_fixture();
        let chosen = fx
            .resolver
            .select(&[SignatureScheme::ECDSA_NISTP256_SHA256])
            .expect("an identity is always chosen");
        assert_eq!(
            chosen.cert[0].to_vec(),
            fx.ecdsa_leaf,
            "ECDSA-only client must be served the ECDSA identity, not the RSA primary"
        );
    }

    #[test]
    fn resolver_picks_rsa_primary_for_rsa_only_client_hello() {
        let fx = dual_cert_fixture();
        let chosen = fx
            .resolver
            .select(&[SignatureScheme::RSA_PSS_SHA256])
            .expect("an identity is always chosen");
        assert_eq!(chosen.cert[0].to_vec(), fx.rsa_leaf);
    }

    #[test]
    fn resolver_falls_back_to_primary_when_nothing_matches() {
        let fx = dual_cert_fixture();
        // ED25519 matches neither the RSA nor the ECDSA key: the primary is
        // served so the client sees an ordinary handshake failure rather than a
        // `no certificate` alert.
        let chosen = fx
            .resolver
            .select(&[SignatureScheme::ED25519])
            .expect("primary is always the fallback");
        assert_eq!(chosen.cert[0].to_vec(), fx.rsa_leaf);
    }

    #[test]
    fn resolver_with_only_primary_always_serves_it() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "only", ecdsa_key());
        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            ..Default::default()
        };
        let resolver = build_cert_resolver(&config, &provider()).unwrap();
        assert_eq!(resolver.len(), 1);
        let leaf = resolver.keys[0].cert[0].to_vec();
        for schemes in [
            vec![SignatureScheme::ECDSA_NISTP256_SHA256],
            vec![SignatureScheme::RSA_PSS_SHA256],
            vec![],
        ] {
            assert_eq!(resolver.select(&schemes).unwrap().cert[0].to_vec(), leaf);
        }
    }

    #[test]
    fn mismatched_additional_pair_fails_to_load() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "primary", ecdsa_key());
        // Point the additional identity at cert A but key B.
        let (extra_cert, _) = write_identity(dir.path(), "extra", ecdsa_key());
        let (_, other_key) = write_identity(dir.path(), "other", ecdsa_key());

        let config = TlsConfig {
            enabled: true,
            cert_file: cert,
            key_file: key,
            additional_certs: vec![frogdb_config::AdditionalCert {
                cert_file: extra_cert,
                key_file: other_key,
            }],
            ..Default::default()
        };
        let err = build_cert_resolver(&config, &provider()).unwrap_err();
        assert!(
            err.to_string().contains("failed to load TLS identity"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn server_config_builds_with_additional_certs_under_mtls() {
        // The client-cert-verifier branch must also route through the resolver.
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = write_identity(dir.path(), "primary", rsa_key());
        let (ec_cert, ec_key) = write_identity(dir.path(), "ecdsa", ecdsa_key());
        // Reuse the primary self-signed cert as the client CA: it only has to
        // parse into the root store for the verifier to build.
        let config = TlsConfig {
            enabled: true,
            cert_file: cert.clone(),
            key_file: key,
            ca_file: Some(cert),
            require_client_cert: ClientCertMode::Required,
            additional_certs: vec![frogdb_config::AdditionalCert {
                cert_file: ec_cert,
                key_file: ec_key,
            }],
            ..Default::default()
        };
        build_server_config(&config).expect("mTLS server config builds with a multi-cert resolver");
    }
}
