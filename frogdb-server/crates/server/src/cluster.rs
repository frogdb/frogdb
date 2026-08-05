//! Server-side cluster facade.
//!
//! The metadata plane (Raft, replicated topology, RPC envelopes) lives in
//! `frogdb-cluster`, and the runtime that turns it into a running node — the
//! cluster bus, cross-node pub/sub, the live `[cluster]` flags, the failure
//! detector — lives in `frogdb-cluster-runtime`. This module re-exports the
//! runtime so the server's call sites keep a single `crate::cluster` facade,
//! and contributes the one piece that cannot leave: [`ClusterBusTls`], the TLS
//! seam the bus accepts inbound connections through.
//!
//! Slot migration (`crate::slot_migration`) and the `CLUSTER` command handlers
//! (`crate::commands::cluster`) stay in the server: both are coupled to a live
//! connection's routing and dispatch.

pub use frogdb_cluster_runtime::bus::{self, ClusterBusContext};
pub use frogdb_cluster_runtime::failure_detector::{
    self, FailureDetector, FailureDetectorConfig, spawn_failure_detector_task,
};
pub use frogdb_cluster_runtime::flags::{self, ClusterRuntimeFlags, SelfFenceGate};
pub use frogdb_cluster_runtime::pubsub::{self, ClusterPubSubForwarder};

/// The server's [`BusTlsAcceptor`](frogdb_cluster_runtime::bus::BusTlsAcceptor):
/// inbound cluster-bus TLS, served from the live [`TlsRuntimeHandle`].
///
/// Holds the handle rather than a snapshot of it, because all three answers are
/// read per connection: the acceptor is rebuilt from the manager on every
/// handshake so a certificate reload reaches the next peer, and the dual-accept
/// flag and handshake timeout are shared atomics a `CONFIG SET` writes through.
///
/// [`TlsRuntimeHandle`]: crate::tls_runtime::TlsRuntimeHandle
#[cfg(not(feature = "turmoil"))]
pub struct ClusterBusTls {
    tls_runtime: std::sync::Arc<crate::tls_runtime::TlsRuntimeHandle>,
}

#[cfg(not(feature = "turmoil"))]
impl ClusterBusTls {
    /// Wrap the live TLS runtime handle as the bus's TLS seam.
    pub fn new(tls_runtime: std::sync::Arc<crate::tls_runtime::TlsRuntimeHandle>) -> Self {
        Self { tls_runtime }
    }
}

#[cfg(not(feature = "turmoil"))]
impl frogdb_cluster_runtime::bus::BusTlsAcceptor for ClusterBusTls {
    fn accept(
        &self,
        stream: frogdb_net::TcpStream,
    ) -> frogdb_cluster_runtime::bus::BusTlsHandshake {
        // A fresh acceptor per handshake: a cached one pins the certificates it
        // was created with and would not see a reload.
        let acceptor = self.tls_runtime.manager().acceptor();
        Box::pin(async move {
            let stream = acceptor
                .accept(stream)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;
            Ok(Box::new(stream) as frogdb_core::cluster::BoxedStream)
        })
    }

    fn dual_accept(&self) -> bool {
        self.tls_runtime.cluster_migration()
    }

    fn handshake_timeout(&self) -> std::time::Duration {
        self.tls_runtime.handshake_timeout().get()
    }
}
