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

use std::any::Any;
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
/// It stays a tokio [`JoinHandle`] even though a production shard is now an OS
/// thread ([`RealShardExecutor`]). The bridge is deliberate rather than
/// incidental: the supervisor distinguishes "returned early" from "panicked,
/// with this payload" by matching on [`tokio::task::JoinError`], and a
/// `JoinError` cannot be constructed outside tokio — so a bespoke handle type
/// could not carry a shard thread's panic to the fail-stop handler at all.
/// [`RealShardExecutor::launch`] therefore catches the shard thread's unwind,
/// ships the payload over a oneshot, and re-raises it inside a tiny bridging
/// task whose `JoinHandle` *is* this handle. Panic payload, cancellation and
/// early-return all keep their existing meanings, and `select_all` in the
/// supervisor keeps working untouched.
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

    /// The jemalloc arena bound to shard `shard_id`, or `None` when that shard
    /// has no arena of its own.
    ///
    /// [`RealShardExecutor`] answers `Some` for every shard it launched with a
    /// working [`ShardArenaSource`], and `None` for a shard whose bind failed —
    /// which is a degraded, still-correct state (see
    /// [`RealShardExecutor::launch`]), not an impossible one. Callers must treat
    /// `None` as "this shard's memory is not separately attributable", never as
    /// "this shard allocated nothing".
    ///
    /// [`SimShardExecutor`] returns `None` *permanently* — a simulation host is
    /// one thread hosting every shard, so `thread.arena` cannot express
    /// per-shard ownership and allocator behavior is explicitly outside the
    /// simulation's fidelity envelope (ADR-0006 §1, §3).
    fn arena_of(&self, shard_id: usize) -> Option<u32>;

    /// The runtime a connection assigned to shard `shard_id` should run on, or
    /// `None` when this executor has no per-shard runtime to pin it to.
    ///
    /// This is the other half of PRD R3/R4: current-thread runtimes per shard
    /// *without* connection→core pinning measured 3.7× worse than today's
    /// work-stealing runtime, because every request then pays two cross-thread
    /// wakeups (spike-report §(b)). Returning a handle here is what lets the
    /// acceptor put a connection's task on the same thread as the shard that
    /// owns its keys, making a same-shard command a zero-hop, same-thread call.
    ///
    /// [`SimShardExecutor`] returns `None`: there is one thread under
    /// simulation, so "pinning" is already true and the ambient runtime is the
    /// shard's runtime. `None` therefore means "spawn where you always did".
    fn connection_runtime(&self, shard_id: usize) -> Option<tokio::runtime::Handle>;

    /// Short, stable identifier for the wired implementation — for logs and for
    /// tests that assert which one is in play.
    fn kind(&self) -> &'static str;
}

/// A snapshot of [`ShardExecutor::connection_runtime`] for every shard, taken
/// once after the shards are launched and cloned to each acceptor.
///
/// The acceptor cannot hold the executor (it is `&mut`-consumed by the launch
/// loop and lives on the startup path), and asking a trait object per accepted
/// connection would put a virtual call on the accept path for an answer that
/// never changes. So the answers are collected once.
///
/// The default value — the one a build with no per-shard runtimes gets — pins
/// nothing, which is exactly today's behavior.
#[derive(Clone, Debug, Default)]
pub struct ShardPlacement {
    runtimes: std::sync::Arc<Vec<Option<tokio::runtime::Handle>>>,
}

impl ShardPlacement {
    /// Ask `executor` where each of `num_shards` shards' connections belong.
    pub fn collect(executor: &dyn ShardExecutor, num_shards: usize) -> Self {
        Self {
            runtimes: std::sync::Arc::new(
                (0..num_shards)
                    .map(|shard_id| executor.connection_runtime(shard_id))
                    .collect(),
            ),
        }
    }

    /// Placement that pins nothing: every connection runs on the ambient
    /// runtime, as it did before connection→core pinning existed.
    pub fn unpinned() -> Self {
        Self::default()
    }

    /// The runtime a connection assigned to `shard_id` must run on, or `None`
    /// to run it on the ambient runtime.
    pub fn runtime_for(&self, shard_id: usize) -> Option<&tokio::runtime::Handle> {
        self.runtimes.get(shard_id).and_then(Option::as_ref)
    }

    /// Whether any shard has a runtime of its own — i.e. whether connections
    /// are pinned at all in this build.
    pub fn is_pinned(&self) -> bool {
        self.runtimes.iter().any(Option::is_some)
    }
}

// ------------------------------------------------------------ shard arenas ---

/// The allocator operations [`RealShardExecutor`] needs to give a shard thread
/// an arena of its own.
///
/// The two calls are `arenas.create` and `thread.arena` (ADR-0006 §3). They live
/// behind a trait rather than being made here because the jemalloc `mallctl`
/// chokepoint is `frogdb_telemetry::jemalloc` — the one module allowed to call
/// `tikv_jemalloc_*` — and this crate sits *below* telemetry in the dependency
/// graph. Inverting the dependency keeps the chokepoint intact and keeps this
/// crate's four-dependency footprint (and its arena tests' determinism: the
/// tests drive a counting fake, not the real allocator).
///
/// Implementations must be callable from the shard thread itself: `thread.arena`
/// binds *the calling thread*, so [`Self::bind_current_thread`] is only
/// meaningful when invoked on the thread being bound.
pub trait ShardArenaSource: Send + Sync + std::fmt::Debug {
    /// Whether this build can bind arenas at all. `false` short-circuits the
    /// whole setup, so a build without an arena-capable allocator does not log a
    /// failure per shard for a facility it was never going to have.
    fn arenas_available(&self) -> bool;

    /// Create a fresh arena and return its index (`arenas.create`).
    fn create_arena(&self) -> std::io::Result<u32>;

    /// Bind the *calling* thread to `arena` (`thread.arena`).
    fn bind_current_thread(&self, arena: u32) -> std::io::Result<()>;
}

/// A source that provides no arenas: what a build with no arena-capable
/// allocator (and every test that does not care about arenas) gets.
///
/// [`ShardExecutor::arena_of`] then reports `None` for every shard, which is the
/// same answer as before arenas existed.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoShardArenas;

impl ShardArenaSource for NoShardArenas {
    fn arenas_available(&self) -> bool {
        false
    }

    fn create_arena(&self) -> std::io::Result<u32> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this build binds no per-shard arenas",
        ))
    }

    fn bind_current_thread(&self, _arena: u32) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "this build binds no per-shard arenas",
        ))
    }
}

/// Production shard placement: one OS thread per shard, each running its own
/// `tokio::runtime::Builder::new_current_thread()` runtime, pinned to a CPU
/// where the platform supports it (PRD R4).
///
/// A shard is no longer a task on the shared work-stealing runtime. The runtime
/// is built *here*, on the launching thread, so its [`tokio::runtime::Handle`]
/// is available synchronously to [`ShardExecutor::connection_runtime`] — that
/// handle is what the acceptor uses to put a connection on the core that owns
/// its keys, and it is not optional: the spike measured per-shard current-thread
/// runtimes *without* connection pinning at 0.27× today's throughput
/// (spike-report §(b)). The `Runtime` value itself is moved onto the shard
/// thread, which drives it with `block_on(worker)`.
///
/// Each shard thread also binds a jemalloc arena of its own, as its first act,
/// before it runs a line of the worker (ADR-0006 §3). See
/// [`RealShardExecutor::launch`].
#[derive(Debug)]
pub struct RealShardExecutor {
    /// Per-shard runtime handles, indexed by launch order and keyed by shard id
    /// so a non-contiguous or out-of-order launch cannot silently misroute
    /// connections.
    runtimes: Vec<(usize, tokio::runtime::Handle)>,
    /// Where arenas come from. Defaults to [`NoShardArenas`]; the server hands
    /// in the jemalloc-backed one.
    arenas: std::sync::Arc<dyn ShardArenaSource>,
    /// `(shard_id, arena)` for every shard whose bind succeeded. A shard is
    /// absent when it has no arena — never present with a placeholder.
    bound_arenas: Vec<(usize, u32)>,
}

impl Default for RealShardExecutor {
    fn default() -> Self {
        Self {
            runtimes: Vec::new(),
            arenas: std::sync::Arc::new(NoShardArenas),
            bound_arenas: Vec::new(),
        }
    }
}

impl RealShardExecutor {
    /// An executor that binds no arenas.
    pub fn new() -> Self {
        Self::default()
    }

    /// An executor that gives each shard thread an arena from `arenas`.
    pub fn with_arenas(arenas: std::sync::Arc<dyn ShardArenaSource>) -> Self {
        Self {
            arenas,
            ..Self::default()
        }
    }

    /// Every `(shard_id, arena)` binding made so far, in launch order — what the
    /// server feeds to `frogdb_telemetry::ShardArenaRegistry`.
    pub fn bound_arenas(&self) -> &[(usize, u32)] {
        &self.bound_arenas
    }
}

/// Create and bind this thread's arena, on the thread being bound.
///
/// Returns the arena, or `None` when this build has no arenas or the allocator
/// refused. A refusal is **degraded, not fatal** — see [`RealShardExecutor::launch`].
fn bind_shard_arena(arenas: &dyn ShardArenaSource, shard_id: usize) -> Option<u32> {
    if !arenas.arenas_available() {
        return None;
    }
    let arena = match arenas.create_arena() {
        Ok(arena) => arena,
        Err(error) => {
            tracing::error!(
                shard_id,
                %error,
                "Shard arena creation failed; shard will allocate on the automatic \
                 arena and its memory will not be separately attributable"
            );
            return None;
        }
    };
    if let Err(error) = arenas.bind_current_thread(arena) {
        tracing::error!(
            shard_id,
            arena,
            %error,
            "Shard arena bind failed; shard will allocate on the automatic arena \
             and its memory will not be separately attributable"
        );
        return None;
    }
    Some(arena)
}

/// The CPU shard `shard_id` intends to run on: shards are laid out over the
/// available CPUs in order and wrap when there are more shards than CPUs.
///
/// Pure and total so the intended half of the placement report is assertable on
/// every platform, including the ones where the *achieved* half does not exist.
pub fn intended_cpu(shard_id: usize, cpus: usize) -> usize {
    if cpus == 0 { 0 } else { shard_id % cpus }
}

/// Number of CPUs shard threads are laid out over.
fn available_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Whether shard threads should be pinned at all.
///
/// Pinning is on by default (it is the ruling), and `FROGDB_SHARD_PIN=0` turns
/// it off for the case the ruling does not cover: a machine running many
/// FrogDB processes at once — CI, a laptop running the test suite — where every
/// process pinning its shards to CPUs 0..N piles them onto the same few cores
/// while the rest of the machine idles. Correctness never depends on pinning,
/// so this is a scheduling knob, not a behavior switch.
fn pinning_enabled() -> bool {
    !matches!(
        std::env::var("FROGDB_SHARD_PIN").as_deref(),
        Ok("0") | Ok("off") | Ok("false")
    )
}

/// Why pinning is or is not available on this platform, reported alongside every
/// shard's placement so an operator can tell "unpinned" from "pinning failed".
#[cfg(target_os = "linux")]
const PINNING_MECHANISM: &str = "sched_setaffinity";

/// macOS exposes no strict CPU affinity API — `thread_policy_set`'s affinity
/// tags are advisory and Apple silicon ignores them outright — so shard threads
/// run wherever the scheduler puts them. This is a performance property, not a
/// correctness one: nothing in the shard body depends on which CPU it is on.
#[cfg(not(target_os = "linux"))]
const PINNING_MECHANISM: &str = "unsupported on this platform (macOS has no strict CPU affinity \
                                 API; Apple silicon ignores thread_policy_set affinity tags)";

/// Pin the calling thread to `cpu` and report the CPU it actually ended up
/// restricted to, or `None` when the platform cannot pin.
///
/// The return value is read back from the OS rather than echoed from the
/// request, so the startup report distinguishes "asked for CPU 3 and got it"
/// from "asked for CPU 3 and the call was ignored".
#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: usize) -> Option<usize> {
    // SAFETY: `set` is a zeroed `cpu_set_t` of exactly the size passed to both
    // calls, and thread id 0 means "the calling thread".
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return None;
        }
        let mut got: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut got);
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut got) != 0 {
            return None;
        }
        (0..libc::CPU_SETSIZE as usize).find(|&c| libc::CPU_ISSET(c, &got))
    }
}

/// See [`PINNING_MECHANISM`]: no strict affinity API, so nothing is pinned and
/// the achieved CPU is unknown rather than wrong.
#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_cpu: usize) -> Option<usize> {
    None
}

impl ShardExecutor for RealShardExecutor {
    /// Launch shard `shard_id` on its own thread, with its own arena.
    ///
    /// # Arena binding
    ///
    /// The shard thread's *first* act is `arenas.create` + `thread.arena`,
    /// before it runs any of the worker: jemalloc charges an allocation to
    /// whatever arena the thread is bound to at the time, so bytes allocated
    /// before the bind are charged to the automatic arena permanently, and
    /// rebinding later without flushing the thread cache bleeds allocations
    /// across arenas (spike-report §(a) E4). Hence bind-once, first, and no
    /// rebinding machinery anywhere.
    ///
    /// `launch` then blocks until the thread reports what it bound. This costs
    /// one thread-spawn plus one `mallctl` at startup, and buys an `arena_of`
    /// that is exact the instant `launch` returns — the alternative, publishing
    /// the arena asynchronously, would make the registry the server builds
    /// straight afterwards racy for no gain on a path that runs once per boot.
    ///
    /// # A failed bind is degraded, not fatal
    ///
    /// If the allocator refuses, the shard runs on the automatic arena and
    /// `arena_of` reports `None`. It does **not** fail-stop, because:
    ///
    /// * Arenas are an *accounting* mechanism under ADR-0006 §3. No data-path
    ///   behavior depends on which arena serves a shard: allocation, freeing,
    ///   and every correctness property are identical either way. Fail-stop is
    ///   for states where continuing produces wrong answers, and this is not one.
    /// * Every consumer already handles an absent arena. Simulation builds bind
    ///   none at all and `msvc` builds cannot, so "no arena for this shard" is a
    ///   state the registry and the broker must handle regardless; making it
    ///   fatal here would add a fail-stop trigger that buys nothing.
    /// * The failure is loud without being fatal: an ERROR names the shard and
    ///   the allocator's own message, the shard's startup line carries
    ///   `arena=None`, and the shard is simply missing from the per-shard memory
    ///   gauges. Refusing to start a database because a statistics arena could
    ///   not be created trades a monitoring gap for a total outage.
    fn launch(&mut self, shard_id: usize, worker: ShardBody) -> ShardHandle {
        // Built here, not on the shard thread: `Runtime` is `Send`, and building
        // it eagerly makes the handle available to `connection_runtime` the
        // moment `launch` returns — the acceptor is wired long before any shard
        // thread has had a chance to publish anything.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .thread_name(format!("frogdb-shard-{shard_id}-blocking"))
            .build()
            .expect("shard current-thread runtime");
        self.runtimes.push((shard_id, runtime.handle().clone()));

        let intended = intended_cpu(shard_id, available_cpus());
        let pin = pinning_enabled();
        let arenas = self.arenas.clone();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Option<Box<dyn Any + Send>>>();
        let (arena_tx, arena_rx) = std::sync::mpsc::channel::<Option<u32>>();

        std::thread::Builder::new()
            .name(format!("frogdb-shard-{shard_id}"))
            .spawn(move || {
                // First, before anything on this thread allocates into the
                // shard's steady state.
                let arena = bind_shard_arena(&*arenas, shard_id);
                let _ = arena_tx.send(arena);

                let achieved = if pin {
                    pin_current_thread(intended)
                } else {
                    None
                };
                tracing::info!(
                    shard_id,
                    intended_cpu = intended,
                    achieved_cpu = ?achieved,
                    pinning_requested = pin,
                    mechanism = PINNING_MECHANISM,
                    arena = ?arena,
                    "Shard thread started"
                );
                // The supervisor's fail-stop policy is written against a
                // panicking *task*; a panicking thread would just unwind into
                // nothing. Catch it here and re-raise it in the bridging task
                // below so the panic reaches `FailStopHandler` with its payload
                // intact.
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    runtime.block_on(worker);
                }));
                let _ = done_tx.send(outcome.err());
            })
            .expect("spawn shard thread");

        // A dropped sender means the thread died before it could report — the
        // same "no arena for this shard" answer, arrived at differently.
        if let Some(arena) = arena_rx.recv().unwrap_or(None) {
            self.bound_arenas.push((shard_id, arena));
        }

        spawn(async move {
            match done_rx.await {
                // Worker returned. The supervisor decides whether that is
                // benign (shutdown) or fatal (live node).
                Ok(None) => {}
                // Worker panicked: re-raise so this task's `JoinHandle` yields
                // the same `JoinError` a panicking shard task used to.
                Ok(Some(payload)) => std::panic::resume_unwind(payload),
                // Sender dropped without reporting — the thread died in a way
                // that skipped its own epilogue. Indistinguishable from an
                // early return from the supervisor's point of view, which is
                // the correct (fatal, while live) verdict either way.
                Err(_) => {}
            }
        })
    }

    fn arena_of(&self, shard_id: usize) -> Option<u32> {
        self.bound_arenas
            .iter()
            .find(|(id, _)| *id == shard_id)
            .map(|(_, arena)| *arena)
    }

    fn connection_runtime(&self, shard_id: usize) -> Option<tokio::runtime::Handle> {
        self.runtimes
            .iter()
            .find(|(id, _)| *id == shard_id)
            .map(|(_, h)| h.clone())
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

    fn connection_runtime(&self, _shard_id: usize) -> Option<tokio::runtime::Handle> {
        // Permanent: a sim host is one thread, so the connection and the shard
        // are already colocated and there is nothing to pin them to. `None`
        // keeps the accept path on `spawn`, exactly as before.
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
///
/// `arenas` is where shard arenas come from; the simulation arm drops it, since
/// a sim host is one thread and per-shard arenas cannot exist there.
#[cfg(feature = "turmoil")]
pub fn shard_executor(_arenas: std::sync::Arc<dyn ShardArenaSource>) -> Box<dyn ShardExecutor> {
    Box::new(SimShardExecutor::new())
}

/// The shard executor for this build — see the `turmoil` arm above.
#[cfg(not(feature = "turmoil"))]
pub fn shard_executor(arenas: std::sync::Arc<dyn ShardArenaSource>) -> Box<dyn ShardExecutor> {
    Box::new(RealShardExecutor::with_arenas(arenas))
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
    use std::sync::Arc;

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

    /// A [`ShardArenaSource`] that hands out arenas from a counter and records
    /// which thread each bind ran on.
    ///
    /// A fake, not the real allocator: the properties under test here are the
    /// executor's (one arena per shard, bound on the shard's own thread, before
    /// the worker runs, degrading rather than dying), and the allocator's own
    /// behavior is pinned separately by `frogdb-telemetry`'s jemalloc tests.
    #[derive(Debug, Default)]
    struct FakeArenas {
        next: std::sync::atomic::AtomicU32,
        binds: std::sync::Mutex<Vec<(u32, std::thread::ThreadId)>>,
        fail_create: bool,
        fail_bind: bool,
        unavailable: bool,
    }

    impl FakeArenas {
        fn working() -> Arc<Self> {
            Arc::new(Self {
                // Start above 0: the automatic arena is 0, so a bug that
                // reported "unbound" as an arena would look like arena 0.
                next: std::sync::atomic::AtomicU32::new(1),
                ..Self::default()
            })
        }

        fn binds(&self) -> Vec<(u32, std::thread::ThreadId)> {
            self.binds.lock().unwrap().clone()
        }
    }

    impl ShardArenaSource for FakeArenas {
        fn arenas_available(&self) -> bool {
            !self.unavailable
        }

        fn create_arena(&self) -> std::io::Result<u32> {
            if self.fail_create {
                return Err(std::io::Error::other("arenas.create refused"));
            }
            Ok(self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
        }

        fn bind_current_thread(&self, arena: u32) -> std::io::Result<()> {
            if self.fail_bind {
                return Err(std::io::Error::other("thread.arena refused"));
            }
            self.binds
                .lock()
                .unwrap()
                .push((arena, std::thread::current().id()));
            Ok(())
        }
    }

    /// The real executor gives every shard an arena of its own — the mapping
    /// issue 05's broker reads to charge memory to the shard that allocated it.
    /// Two shards sharing an arena would make every per-shard figure a sum of
    /// both.
    #[tokio::test]
    async fn real_executor_reports_a_distinct_arena_per_shard() {
        let arenas = FakeArenas::working();
        let mut exec = RealShardExecutor::with_arenas(arenas.clone());
        launch_all(&mut exec).await;

        let mut seen = Vec::new();
        for shard_id in 0..SHARDS {
            let arena = exec
                .arena_of(shard_id)
                .unwrap_or_else(|| panic!("shard {shard_id} must report an arena"));
            assert!(
                !seen.contains(&arena),
                "arena {arena} was handed to two shards; per-shard accounting \
                 would double-count both"
            );
            seen.push(arena);
        }
        assert_eq!(
            exec.bound_arenas().len(),
            SHARDS,
            "every binding must be visible to the registry the server builds"
        );
    }

    /// `thread.arena` binds *the calling thread*, so a bind performed anywhere
    /// but on the shard's own thread would charge the launching thread's
    /// allocations to the shard's arena and leave the shard on the automatic
    /// one — the exact inversion of what the arena is for.
    #[tokio::test]
    async fn each_arena_is_bound_on_the_shard_thread_that_owns_it() {
        let arenas = FakeArenas::working();
        let launcher = std::thread::current().id();
        let mut exec = RealShardExecutor::with_arenas(arenas.clone());
        launch_all(&mut exec).await;

        let binds = arenas.binds();
        assert_eq!(binds.len(), SHARDS, "one bind per shard");
        let mut threads = Vec::new();
        for (arena, thread) in binds {
            assert_ne!(thread, launcher, "arena {arena} was bound on the launcher");
            assert!(
                !threads.contains(&thread),
                "two shards bound arenas from the same thread"
            );
            threads.push(thread);
        }
    }

    /// The bind must precede the worker: jemalloc charges an allocation to
    /// whatever arena the thread is bound to at the time, so anything the worker
    /// allocates before the bind lands on the automatic arena permanently.
    #[tokio::test]
    async fn the_arena_is_bound_before_the_worker_runs() {
        let arenas = FakeArenas::working();
        let mut exec = RealShardExecutor::with_arenas(arenas.clone());
        let (tx, rx) = tokio::sync::oneshot::channel();
        let observer = arenas.clone();
        let handle = exec.launch(
            0,
            Box::pin(async move {
                let _ = tx.send(observer.binds().len());
            }),
        );
        let binds_when_the_worker_started = rx.await.expect("worker reported");
        handle.await.expect("shard body must not panic");

        assert_eq!(
            binds_when_the_worker_started, 1,
            "the shard's arena must already be bound when its worker gets \
             control, or the worker's first allocations escape it"
        );
    }

    /// A refused bind is degraded, not fatal: the shard still runs, and reports
    /// no arena rather than a wrong one. See `RealShardExecutor::launch` for why
    /// this is not a fail-stop.
    #[tokio::test]
    async fn a_failed_arena_bind_leaves_the_shard_running_and_unattributed() {
        for arenas in [
            Arc::new(FakeArenas {
                fail_create: true,
                ..FakeArenas::default()
            }),
            Arc::new(FakeArenas {
                fail_bind: true,
                ..FakeArenas::default()
            }),
        ] {
            let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let mut exec = RealShardExecutor::with_arenas(arenas);
            let mut handles = Vec::new();
            for shard_id in 0..SHARDS {
                let ran = ran.clone();
                handles.push(exec.launch(
                    shard_id,
                    Box::pin(async move {
                        ran.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }),
                ));
            }
            for h in handles {
                h.await.expect("a shard whose arena failed must still run");
            }
            assert_eq!(ran.load(std::sync::atomic::Ordering::Relaxed), SHARDS);
            for shard_id in 0..SHARDS {
                assert_eq!(
                    exec.arena_of(shard_id),
                    None,
                    "a shard that failed to bind must report no arena, not a \
                     wrong one"
                );
            }
            assert!(exec.bound_arenas().is_empty());
        }
    }

    /// Without a source — the default, and every build with no arena-capable
    /// allocator — the executor binds nothing and reports nothing.
    #[tokio::test]
    async fn real_executor_without_a_source_reports_no_arena() {
        let mut exec = RealShardExecutor::new();
        launch_all(&mut exec).await;
        for shard_id in 0..SHARDS {
            assert_eq!(exec.arena_of(shard_id), None);
        }
    }

    /// The load-bearing half of R3/R4: the real executor hands out a runtime
    /// per shard for connections to be pinned to. Without this the accept path
    /// has nothing to pin *to* and the shape degrades to the spike's 3.7×
    /// regression.
    #[tokio::test]
    async fn real_executor_offers_a_distinct_runtime_per_shard() {
        let mut exec = RealShardExecutor::new();
        let mut handles = Vec::new();
        for shard_id in 0..SHARDS {
            handles.push(exec.launch(shard_id, Box::pin(std::future::ready(()))));
        }
        let ids: Vec<_> = (0..SHARDS)
            .map(|s| {
                exec.connection_runtime(s)
                    .unwrap_or_else(|| panic!("shard {s} must offer a runtime"))
                    .id()
            })
            .collect();
        for (a, id) in ids.iter().enumerate() {
            for (b, other) in ids.iter().enumerate() {
                assert!(
                    a == b || id != other,
                    "shards {a} and {b} share a runtime; connections would not be core-local"
                );
            }
        }
        for h in handles {
            h.await.expect("shard body must not panic");
        }
    }

    /// Structural, not timed: a body launched by the real executor observes a
    /// thread that is neither the caller's nor any sibling shard's. This is the
    /// executable form of "every shard runs on a dedicated OS thread".
    #[tokio::test]
    async fn real_executor_runs_each_shard_on_its_own_thread() {
        use std::sync::Mutex;
        let seen: Arc<Mutex<Vec<std::thread::ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
        let caller = std::thread::current().id();

        let mut exec = RealShardExecutor::new();
        let mut handles = Vec::new();
        for shard_id in 0..SHARDS {
            let seen = seen.clone();
            handles.push(exec.launch(
                shard_id,
                Box::pin(async move {
                    seen.lock().unwrap().push(std::thread::current().id());
                }),
            ));
        }
        for h in handles {
            h.await.expect("shard body must not panic");
        }

        let ids = seen.lock().unwrap().clone();
        assert_eq!(ids.len(), SHARDS);
        for id in &ids {
            assert_ne!(*id, caller, "a shard ran on the launching thread");
        }
        for (a, id) in ids.iter().enumerate() {
            for (b, other) in ids.iter().enumerate() {
                assert!(a == b || id != other, "shards shared a thread: {ids:?}");
            }
        }
    }

    /// A connection spawned onto a shard's runtime runs on that shard's thread —
    /// the same thread the shard body runs on. This is the zero-hop property
    /// stated structurally: no timing, just thread identity.
    #[tokio::test]
    async fn a_task_spawned_on_a_shards_runtime_shares_the_shards_thread() {
        let (body_tx, body_rx) = tokio::sync::oneshot::channel();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let mut exec = RealShardExecutor::new();
        let handle = exec.launch(
            0,
            Box::pin(async move {
                let _ = body_tx.send(std::thread::current().id());
                let _ = stop_rx.await;
            }),
        );
        let shard_thread = body_rx.await.expect("shard body reported its thread");

        let rt = exec.connection_runtime(0).expect("shard 0 has a runtime");
        let conn_thread = rt
            .spawn(async { std::thread::current().id() })
            .await
            .expect("pinned task must not panic");

        assert_eq!(
            conn_thread, shard_thread,
            "a connection pinned to shard 0 must run on shard 0's thread"
        );

        let _ = stop_tx.send(());
        handle.await.expect("shard body must not panic");
    }

    /// The bridge from an OS-thread panic back to the supervisor's
    /// `JoinError`-shaped fail-stop policy. A shard thread that panics must
    /// still surface as a panicking `ShardHandle` carrying the payload, or the
    /// supervisor would classify a crash as a benign early return.
    #[tokio::test]
    async fn a_panicking_shard_thread_surfaces_as_a_panicking_handle() {
        let mut exec = RealShardExecutor::new();
        let handle = exec.launch(0, Box::pin(async { panic!("shard 0 boom") }));
        let err = handle.await.expect_err("handle must report the panic");
        let payload = err.try_into_panic().expect("must be a panic, not a cancel");
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("shard 0 boom")
        );
    }

    /// Intended placement is a total function of shard id and CPU count, and it
    /// is the half of the placement report that exists on every platform — the
    /// achieved half is `None` wherever there is no affinity API.
    #[test]
    fn intended_cpu_lays_shards_over_the_cpus_in_order_and_wraps() {
        assert_eq!(intended_cpu(0, 4), 0);
        assert_eq!(intended_cpu(3, 4), 3);
        assert_eq!(intended_cpu(4, 4), 0);
        assert_eq!(intended_cpu(9, 4), 1);
        // Never divides by zero if `available_parallelism` is unavailable.
        assert_eq!(intended_cpu(7, 0), 0);
    }

    /// Linux only: the achieved CPU is read back from the kernel, so this
    /// asserts the pin actually took rather than that the call was made. On
    /// macOS there is no strict affinity API and `pin_current_thread` reports
    /// `None` by construction — see [`PINNING_MECHANISM`].
    #[cfg(target_os = "linux")]
    #[test]
    fn a_pinned_thread_reports_the_cpu_it_was_pinned_to() {
        let cpus = available_cpus();
        for shard_id in 0..cpus.min(4) {
            let intended = intended_cpu(shard_id, cpus);
            let achieved = std::thread::spawn(move || pin_current_thread(intended))
                .join()
                .unwrap();
            assert_eq!(
                achieved,
                Some(intended),
                "shard {shard_id} asked for CPU {intended} and the kernel disagreed"
            );
        }
    }

    /// Placement collection: the sim executor pins nothing, the real one pins
    /// every shard. `ShardPlacement::is_pinned` is what the accept path logs.
    #[tokio::test]
    async fn placement_reflects_the_executor_that_produced_it() {
        let mut sim = SimShardExecutor::new();
        launch_all(&mut sim).await;
        let sim_placement = ShardPlacement::collect(&sim, SHARDS);
        assert!(!sim_placement.is_pinned());
        assert!(sim_placement.runtime_for(0).is_none());

        let mut real = RealShardExecutor::new();
        let mut handles = Vec::new();
        for shard_id in 0..SHARDS {
            handles.push(real.launch(shard_id, Box::pin(std::future::ready(()))));
        }
        let real_placement = ShardPlacement::collect(&real, SHARDS);
        assert!(real_placement.is_pinned());
        for shard_id in 0..SHARDS {
            assert!(real_placement.runtime_for(shard_id).is_some());
        }
        // Out of range is not pinned rather than a panic.
        assert!(real_placement.runtime_for(SHARDS + 1).is_none());
        for h in handles {
            h.await.expect("shard body must not panic");
        }
    }

    /// The unpinned default is what a caller with no executor gets, and it must
    /// mean "spawn where you always did".
    #[test]
    fn unpinned_placement_pins_nothing() {
        let p = ShardPlacement::unpinned();
        assert!(!p.is_pinned());
        assert!(p.runtime_for(0).is_none());
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
        assert_eq!(shard_executor(Arc::new(NoShardArenas)).kind(), expected);
    }
}
