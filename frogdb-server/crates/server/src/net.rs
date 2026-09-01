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

/// The jemalloc-backed [`ShardArenaSource`] production shards bind through.
///
/// This is where the two halves meet: [`frogdb_net`] owns the shard thread and
/// declares *what* it needs (create an arena, bind this thread to it), and
/// `frogdb_telemetry::jemalloc` owns the `mallctl` chokepoint that can actually
/// do it. Neither may depend on the other — telemetry sits above net — so the
/// adapter lives here, in the one crate that already has both.
#[derive(Debug, Default, Clone, Copy)]
pub struct JemallocShardArenas;

impl ShardArenaSource for JemallocShardArenas {
    fn arenas_available(&self) -> bool {
        // False on a target without jemalloc, where every call below would fail
        // once per shard for a facility the build was never going to have.
        frogdb_telemetry::jemalloc::narenas().is_some()
    }

    fn create_arena(&self) -> std::io::Result<u32> {
        frogdb_telemetry::jemalloc::create_arena()
    }

    fn bind_current_thread(&self, arena: u32) -> std::io::Result<()> {
        frogdb_telemetry::jemalloc::bind_current_thread_to_arena(arena)
    }
}

/// The arena source for this build's shards.
pub fn shard_arena_source() -> std::sync::Arc<dyn ShardArenaSource> {
    std::sync::Arc::new(JemallocShardArenas)
}

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
