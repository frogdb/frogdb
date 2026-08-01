//! Conditional network primitives.
//!
//! The tokio/turmoil-swappable surface (`TcpListener`, `TcpStream`, `spawn`,
//! `JoinHandle`, `tcp_listener_reusable`) lives in the [`frogdb_net`] crate so
//! the cluster/replication runtime can depend on it without dragging the server
//! along. It is re-exported here verbatim, so server code keeps writing
//! `crate::net::…`.
//!
//! What stays behind is the one alias that cannot leave: [`ConnectionStream`],
//! whose production arm is `crate::tls::MaybeTlsStream` and is therefore
//! TLS-coupled.
//!
//! See [`frogdb_net`]'s crate docs for why the swap is a cargo feature rather
//! than `#[cfg(test)]`, and for the two guards that keep the feature wiring
//! honest.

pub use frogdb_net::*;

// ConnectionStream — the stream type used by ConnectionHandler.
// In production, this is MaybeTlsStream (plain or TLS).
// Under turmoil simulation, this is turmoil's TcpStream (no TLS support).
#[cfg(feature = "turmoil")]
pub type ConnectionStream = turmoil::net::TcpStream;

#[cfg(not(feature = "turmoil"))]
pub type ConnectionStream = crate::tls::MaybeTlsStream;

/// Compile-time proof that `frogdb-server/turmoil` actually forwards
/// `frogdb-net/turmoil`. Without the forward, `frogdb_net::TcpStream` would
/// still be tokio's while this crate's own `turmoil` arm is active — the
/// simulation would silently run on the production network stack instead of
/// failing loudly. This identity function stops type-checking in that case.
/// (`just lint-turmoil-features` catches the same breakage in the manifests.)
#[cfg(feature = "turmoil")]
const _: () = {
    fn _net_crate_is_simulated(s: turmoil::net::TcpStream) -> TcpStream {
        s
    }
};
