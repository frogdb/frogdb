//! Conditional network primitives.
//!
//! This crate provides network types that can be swapped between Tokio and
//! Turmoil implementations for network simulation testing.
//!
//! When the `turmoil` feature is enabled, these types come from the Turmoil
//! library, enabling deterministic network simulation with fault injection.
//! Otherwise, they come from Tokio for production use.
//!
//! The feature flag approach (rather than `#[cfg(test)]`) is used because
//! integration tests compile the library in normal mode, not test mode.
//! Using just `feature = "turmoil"` ensures the simulation networking is
//! active when running `cargo test --features turmoil --test simulation`.
//!
//! Because the swap is a *feature*, every crate in the dependency chain has to
//! forward it: `frogdb-server`'s `turmoil` feature enables `frogdb-net/turmoil`.
//! A broken forward would silently compile the production tokio stack into the
//! simulation, so it is guarded twice — `just lint-turmoil-features` checks the
//! manifest wiring, and `frogdb-server`'s `net` module carries a compile-time
//! type-identity assertion against `turmoil::net`.
//!
//! Note: Turmoil intercepts tokio's runtime, so `spawn` and `JoinHandle` use
//! tokio's types directly - turmoil will handle them correctly when running
//! inside a simulation.
//!
//! TLS-coupled aliases (notably `ConnectionStream`, whose production arm is the
//! server's `MaybeTlsStream`) deliberately stay in `frogdb-server`: this crate
//! owns only the tokio/turmoil swap.

// TcpListener
#[cfg(feature = "turmoil")]
pub use turmoil::net::TcpListener;

#[cfg(not(feature = "turmoil"))]
pub use tokio::net::TcpListener;

// TcpStream
#[cfg(feature = "turmoil")]
pub use turmoil::net::TcpStream;

#[cfg(not(feature = "turmoil"))]
pub use tokio::net::TcpStream;

// spawn - tokio's spawn works inside turmoil simulations
pub use tokio::spawn;

// JoinHandle - tokio's JoinHandle works inside turmoil simulations
pub use tokio::task::JoinHandle;

use std::net::SocketAddr;

/// Creates a TcpListener with SO_REUSEADDR enabled.
/// This allows rebinding to ports in TIME_WAIT state, which is essential
/// for rapid server restarts in tests and production deployments.
///
/// SO_REUSEPORT is only enabled in release builds — it allows multiple
/// processes to bind to the same port for hot-restart / rolling upgrades.
/// In debug builds (including tests), SO_REUSEPORT is disabled to prevent
/// the OS from assigning the same ephemeral port to concurrent test servers.
#[cfg(not(feature = "turmoil"))]
pub async fn tcp_listener_reusable(addr: SocketAddr) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};

    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    #[cfg(not(debug_assertions))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
}

/// Turmoil doesn't need SO_REUSEADDR - use regular bind.
#[cfg(feature = "turmoil")]
pub async fn tcp_listener_reusable(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// Compile-time proof that the exported primitives really are turmoil's when
/// the `turmoil` feature is on. If the swap ever regressed to tokio's types
/// (e.g. a `cfg` typo), these identity functions would stop type-checking.
#[cfg(feature = "turmoil")]
const _: () = {
    fn _listener_is_turmoil(l: turmoil::net::TcpListener) -> TcpListener {
        l
    }
    fn _stream_is_turmoil(s: turmoil::net::TcpStream) -> TcpStream {
        s
    }
};

/// Compile-time proof of the mirror-image invariant: without the feature the
/// primitives are tokio's, so a stray `turmoil` enable in a production build
/// cannot go unnoticed.
#[cfg(not(feature = "turmoil"))]
const _: () = {
    fn _listener_is_tokio(l: tokio::net::TcpListener) -> TcpListener {
        l
    }
    fn _stream_is_tokio(s: tokio::net::TcpStream) -> TcpStream {
        s
    }
};
