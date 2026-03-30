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
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsAcceptor;

use frogdb_config::{ClientCertMode, TlsConfig, TlsProtocol};

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
    /// Used for PSYNC handoff where the replication handler takes over the connection.
    pub fn into_tcp_stream(self) -> tokio::net::TcpStream {
        match self {
            MaybeTlsStream::Plain { inner } => inner,
            MaybeTlsStream::Tls { inner } => inner.into_inner().0,
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

/// Manages TLS configuration and provides TLS acceptors for incoming connections.
///
/// The server config is stored behind `ArcSwap` to support future hot-reloading
/// without disrupting existing connections.
pub struct TlsManager {
    server_config: ArcSwap<ServerConfig>,
}

impl TlsManager {
    /// Create a new TLS manager from the TLS configuration.
    ///
    /// Loads certificates and private key from disk, builds the rustls ServerConfig,
    /// and optionally configures client certificate verification for mTLS.
    pub fn new(config: &TlsConfig) -> anyhow::Result<Self> {
        // Ensure a crypto provider is installed (idempotent if already set)
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let server_config = build_server_config(config)?;
        Ok(Self {
            server_config: ArcSwap::from_pointee(server_config),
        })
    }

    /// Get a TLS acceptor using the current server configuration.
    pub fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(self.server_config.load_full())
    }

    /// Get the current server configuration.
    pub fn current_server_config(&self) -> Arc<ServerConfig> {
        self.server_config.load_full()
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
