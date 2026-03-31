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
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::{TlsAcceptor, TlsConnector};

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
    pub fn reload(&self, config: &TlsConfig) -> anyhow::Result<()> {
        let new_server = build_server_config(config)?;
        self.server_config.store(Arc::new(new_server));

        if let Some(ref client_swap) = self.client_config {
            let new_client = build_client_config(config)?;
            client_swap.store(Arc::new(new_client));
        }

        Ok(())
    }
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
    let mut store = RootCertStore::empty();
    for cert in certs {
        store.add(cert)?;
    }
    Ok(store)
}

/// Build a rustls ServerConfig from the TLS configuration.
fn build_server_config(config: &TlsConfig) -> anyhow::Result<ServerConfig> {
    let certs = load_certs(&config.cert_file)?;
    let key = load_private_key(&config.key_file)?;

    // Determine protocol versions
    let versions: Vec<&'static rustls::SupportedProtocolVersion> = config
        .protocols
        .iter()
        .map(|p| match p {
            TlsProtocol::Tls12 => &rustls::version::TLS12,
            TlsProtocol::Tls13 => &rustls::version::TLS13,
        })
        .collect();

    // Build the server config with appropriate client cert verification
    let builder = ServerConfig::builder_with_protocol_versions(&versions);

    let server_config = match config.require_client_cert {
        ClientCertMode::None => builder.with_no_client_auth().with_single_cert(certs, key)?,
        ClientCertMode::Optional | ClientCertMode::Required => {
            let ca_file = config
                .ca_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("ca_file required for client cert verification"))?;
            let ca_store = load_ca_certs(ca_file)?;

            let verifier = if config.require_client_cert == ClientCertMode::Required {
                WebPkiClientVerifier::builder(Arc::new(ca_store)).build()?
            } else {
                WebPkiClientVerifier::builder(Arc::new(ca_store))
                    .allow_unauthenticated()
                    .build()?
            };

            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)?
        }
    };

    Ok(server_config)
}

/// Build a rustls ClientConfig for outgoing TLS connections (cluster bus, replication).
///
/// Uses `client_cert_file`/`client_key_file` for client identity if set,
/// otherwise falls back to the server `cert_file`/`key_file`.
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

    let builder =
        ClientConfig::builder_with_protocol_versions(&versions).with_root_certificates(root_store);

    // Use client cert/key if available (for mTLS to peers), fall back to server cert/key
    let (cert_path, key_path) = match (&config.client_cert_file, &config.client_key_file) {
        (Some(cert), Some(key)) => (cert.as_path(), key.as_path()),
        _ => (config.cert_file.as_path(), config.key_file.as_path()),
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
