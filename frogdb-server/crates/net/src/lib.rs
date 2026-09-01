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
//!
//! The swap is not only over *types*. [`ShardExecutor`] widens the same seam to
//! cover shard *placement*: production and simulation get different answers to
//! "what does a shard run on", chosen by the same `turmoil` feature (ADR-0006
//! §1).

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

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

// ---------------------------------------------------------- shard placement ---

/// The body of one shard worker, type-erased so [`ShardExecutor`] can be an
/// object.
///
/// This is exactly what the server hands to [`spawn`] today (the instrumented
/// `worker.run()` future), boxed. It is `Send + 'static` because the future has
/// to be movable to whatever the executor decides to run it on — today the
/// ambient runtime, and (issue 02) a dedicated shard thread.
pub type ShardBody = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A launched shard's handle: what the supervisor joins (and, on failure,
/// classifies) to notice a dead shard.
///
/// It is a tokio [`JoinHandle`] because that is what a shard is today — a task.
/// When shards become OS threads (issue 02) this alias becomes the place where
/// a thread join is bridged back into something awaitable; keeping it named
/// here means the supervisor's signature does not have to move again.
pub type ShardHandle = JoinHandle<()>;

/// Where a shard's core-local state lives and how it is driven.
///
/// This is the placement seam ruled in
/// [`adr/0006`](../../../../adr/0006-memory-architecture-seams.md) §1: a shard
/// is launched through an executor rather than a bare [`spawn`], so the
/// production shape (thread-per-core with a bound jemalloc arena) and the
/// simulation shape (tasks on the sim host's single thread, arena binding
/// elided) can diverge *behind* one call site instead of at it.
///
/// The trait is deliberately object-safe: the server holds one
/// `Box<dyn ShardExecutor>` chosen once at boot.
///
/// In this issue both implementations do the same thing — `spawn` on the
/// ambient runtime — because the seam lands as a no-op over today's behavior.
pub trait ShardExecutor: Send {
    /// Launch shard `shard_id` running `worker`, and return its handle.
    fn launch(&mut self, shard_id: usize, worker: ShardBody) -> ShardHandle;

    /// The jemalloc arena bound to shard `shard_id`, or `None` when arena
    /// binding is not modelled by this executor.
    ///
    /// Every implementation returns `None` today; the real one grows a `Some`
    /// when arenas arrive. [`SimShardExecutor`] returns `None` *permanently* —
    /// a simulation host is one thread hosting every shard, so `thread.arena`
    /// cannot express per-shard ownership and allocator behavior is explicitly
    /// outside the simulation's fidelity envelope (ADR-0006 §1, §3).
    fn arena_of(&self, shard_id: usize) -> Option<u32>;

    /// Short, stable identifier for the wired implementation — for logs and for
    /// tests that assert which one is in play.
    fn kind(&self) -> &'static str;
}

/// Production shard placement.
///
/// Today: `frogdb_net::spawn` on the ambient runtime, which is byte-for-byte
/// what `server::shards` did before the seam existed. No threads, no
/// current-thread runtimes, no arenas — those arrive in issues 02 and 03.
#[derive(Debug, Default)]
pub struct RealShardExecutor {
    _private: (),
}

impl RealShardExecutor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ShardExecutor for RealShardExecutor {
    fn launch(&mut self, _shard_id: usize, worker: ShardBody) -> ShardHandle {
        spawn(worker)
    }

    fn arena_of(&self, _shard_id: usize) -> Option<u32> {
        // No arenas yet (issue 03).
        None
    }

    fn kind(&self) -> &'static str {
        "real"
    }
}

/// Simulation shard placement.
///
/// Shards run as tokio tasks on the *caller's* runtime — under turmoil that is
/// the single thread the sim host owns, so every shard stays scheduled by the
/// simulation and keeps seeing simulated time. Arena binding is a no-op and
/// [`ShardExecutor::arena_of`] reports no arena rather than a fake one.
///
/// Defined unconditionally (it needs nothing from the `turmoil` crate) so both
/// implementations type-check under both feature configurations; only the
/// *selection* below is `cfg`-gated.
#[derive(Debug, Default)]
pub struct SimShardExecutor {
    _private: (),
}

impl SimShardExecutor {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ShardExecutor for SimShardExecutor {
    fn launch(&mut self, _shard_id: usize, worker: ShardBody) -> ShardHandle {
        tokio::spawn(worker)
    }

    fn arena_of(&self, _shard_id: usize) -> Option<u32> {
        // Permanent: arena binding is not modelled under simulation.
        None
    }

    fn kind(&self) -> &'static str {
        "sim"
    }
}

/// The shard executor for this build: [`SimShardExecutor`] under the `turmoil`
/// feature, [`RealShardExecutor`] otherwise.
///
/// Selection is a compile-time `cfg`, following the type swaps above rather
/// than adding a runtime switch.
#[cfg(feature = "turmoil")]
pub fn shard_executor() -> Box<dyn ShardExecutor> {
    Box::new(SimShardExecutor::new())
}

/// The shard executor for this build — see the `turmoil` arm above.
#[cfg(not(feature = "turmoil"))]
pub fn shard_executor() -> Box<dyn ShardExecutor> {
    Box::new(RealShardExecutor::new())
}

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

/// Compile-time proof that [`ShardExecutor`] stays object-safe and that both
/// implementations can be held behind the same object. The server holds one
/// `Box<dyn ShardExecutor>`, so a `Self`-typed or generic method added to the
/// trait has to fail here rather than at the call site.
const _: () = {
    fn _real_is_object_safe(e: RealShardExecutor) -> Box<dyn ShardExecutor> {
        Box::new(e)
    }
    fn _sim_is_object_safe(e: SimShardExecutor) -> Box<dyn ShardExecutor> {
        Box::new(e)
    }
};

#[cfg(test)]
mod shard_executor_tests {
    use super::*;

    const SHARDS: usize = 4;

    /// Launch `SHARDS` trivial shard bodies through `exec` and join them all,
    /// so `arena_of` is interrogated about shards that really were launched.
    async fn launch_all(exec: &mut dyn ShardExecutor) {
        let mut handles = Vec::with_capacity(SHARDS);
        for shard_id in 0..SHARDS {
            handles.push(exec.launch(
                shard_id,
                Box::pin(async move {
                    let _ = shard_id;
                }),
            ));
        }
        for h in handles {
            h.await.expect("shard body must not panic");
        }
    }

    /// Permanent: arena binding is deliberately not modelled under simulation,
    /// so the sim executor reports no arena for any shard. This must still hold
    /// after the real executor grows real arenas (issue 03) — a `Some` here
    /// would mean the simulation is claiming a per-shard arena that its single
    /// host thread cannot actually own (ADR-0006 §1).
    #[tokio::test]
    async fn sim_executor_reports_no_arena_for_any_shard() {
        let mut exec = SimShardExecutor::new();
        launch_all(&mut exec).await;
        for shard_id in 0..SHARDS {
            assert_eq!(
                exec.arena_of(shard_id),
                None,
                "sim executor must never report an arena (shard {shard_id})"
            );
        }
    }

    /// This issue lands the seam over today's behavior, so the real executor
    /// has no arenas either yet. Issue 03 replaces this expectation.
    #[tokio::test]
    async fn real_executor_reports_no_arena_yet() {
        let mut exec = RealShardExecutor::new();
        launch_all(&mut exec).await;
        for shard_id in 0..SHARDS {
            assert_eq!(exec.arena_of(shard_id), None);
        }
    }

    /// The two implementations are distinguishable by name, which is what the
    /// wiring assertions in `frogdb-server` rely on.
    #[test]
    fn kinds_are_distinct() {
        assert_eq!(RealShardExecutor::new().kind(), "real");
        assert_eq!(SimShardExecutor::new().kind(), "sim");
    }

    /// The `cfg` selection: `turmoil` picks the sim executor, its absence picks
    /// the real one.
    #[test]
    fn selected_executor_matches_the_feature() {
        let expected = if cfg!(feature = "turmoil") {
            "sim"
        } else {
            "real"
        };
        assert_eq!(shard_executor().kind(), expected);
    }
}
