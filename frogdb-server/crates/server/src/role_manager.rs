//! The `RoleManager` deep module: the single owner of this node's replication
//! *role* and the lifecycle a role change implies.
//!
//! Before this module, "what role is this node" was smeared across three
//! disconnected representations with no owner — a hand-threaded
//! `Arc<AtomicBool>`, the Raft `NodeRole` enum, and a boot-only replica
//! streaming task — and there was *no runtime implementation of demotion at
//! all*: `REPLICAOF host port` parsed, logged, returned `+OK`, and flipped
//! nothing.
//!
//! [`RoleManager`] consolidates the data-path role flag, the current primary
//! target, and the live streaming handle behind a narrow interface with two
//! real operations: [`RoleManager::promote`] (become a writable primary) and
//! [`RoleManager::demote`] (become a replica of an address, opening the
//! stream). [`RoleManagerHandle`] implements [`frogdb_core::RoleController`] so
//! a `REPLICAOF` command handler — or the Raft failover event consumer — can
//! request a transition without touching the streaming machinery directly.
//!
//! ## Boundary with the Raft Metadata Plane (ADR-0001)
//!
//! `RoleManager` *reflects* role onto the local data path; it does not own
//! cluster-topology consensus. In cluster mode the flow is one-directional:
//! Raft decides the topology change and emits a demotion/promotion event, and
//! the event consumer calls [`RoleManagerHandle::request_demote`] to make the
//! local data path match the committed decision. `RoleManager` never writes
//! `NodeRole` and never initiates a topology change. In standalone replication
//! mode `REPLICAOF` is the sole driver and `RoleManager` is the role authority.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::replication::ReplicaReplicationHandler;

/// A running inbound replication stream (connection task + frame consumer).
///
/// The stream is owned by the [`RoleManager`]; **dropping the handle tears the
/// stream down** (aborts its tasks and signals the connection loop). This is
/// the sole stop mechanism, so `promote()`/re-`demote()` need only replace the
/// stored handle.
pub trait ReplicaStream: Send {
    /// Whether this stream's connection to the primary is currently up
    /// (connected, past PSYNC, streaming WAL frames). See
    /// [`crate::replication::ReplicaReplicationHandler::link_up`].
    fn link_up(&self) -> bool;
}

/// Opens inbound replication streams to a primary.
///
/// Injected into the [`RoleManager`] so unit tests can substitute a fake that
/// records `start` calls without touching a socket, while production wires the
/// real streamer that reuses the boot-time replication machinery.
pub trait ReplicaStreamer: Send + Sync {
    /// Open a replica stream to `primary`, returning a handle whose `Drop`
    /// stops it.
    fn start(&self, primary: SocketAddr) -> Box<dyn ReplicaStream>;
}

/// Owns this node's replication role and the streaming lifecycle a role change
/// implies. The single writer of the data-path role flag.
pub struct RoleManager {
    /// The one data-path role flag. Read by the write guard, `ROLE`/`INFO`, and
    /// the frame consumer through clones handed out by [`RoleManager::role_flag`].
    is_replica: Arc<AtomicBool>,
    /// Current primary we replicate from (`None` when primary/standalone).
    primary_target: Option<SocketAddr>,
    /// Live inbound stream, or `None`.
    stream: Option<Box<dyn ReplicaStream>>,
    /// Factory that opens a replica stream to a primary address.
    streamer: Arc<dyn ReplicaStreamer>,
    /// The boot-spawned replica handler, when this node started as a
    /// config-file Replica (`replicaof` in config). `init_replication` and
    /// `Server::start_subsystems` construct and spawn it directly, before
    /// this `RoleManager` exists, so it is wired in after the fact via
    /// [`RoleManager::register_boot_replica_handler`]. `promote()` (and a
    /// superseding `demote()`) stop it through its own shutdown watch so a
    /// Role Promotion of a boot-spawned Replica actually halts the boot
    /// reconnect loop instead of leaving it dialing the old primary forever.
    boot_replica_handler: Option<Arc<ReplicaReplicationHandler>>,
}

impl RoleManager {
    /// Build a manager over the shared role flag, using `streamer` to open
    /// inbound streams. The flag's current value is the boot role.
    ///
    /// `boot_target` seeds [`RoleManager::primary_target`] with the
    /// `replicaof`-configured primary when this node boots as a replica
    /// (`None` for a primary/standalone boot). The boot replication stream
    /// itself is opened separately by `init_replication`/`subsystems` — this
    /// only records the *address* so `ROLE` and INFO agree with the runtime
    /// demotion path from process start, instead of only after the first
    /// `REPLICAOF host port`. The manager is the one place that owns this
    /// value for the lifetime of the process.
    pub fn new(
        is_replica: Arc<AtomicBool>,
        streamer: Arc<dyn ReplicaStreamer>,
        boot_target: Option<SocketAddr>,
    ) -> Self {
        Self {
            is_replica,
            primary_target: boot_target,
            stream: None,
            streamer,
            boot_replica_handler: None,
        }
    }

    /// Register the boot-spawned replica handler so a later `promote()` /
    /// `demote()` also stops its reconnect loop. Called once, by
    /// `Server::start_subsystems`, immediately after spawning the boot
    /// replica's connection task and before the acceptor starts serving
    /// client connections — so no `REPLICAOF` can race ahead of this call.
    pub fn register_boot_replica_handler(
        &mut self,
        handler: Arc<ReplicaReplicationHandler>,
        primary: SocketAddr,
    ) {
        self.boot_replica_handler = Some(handler);
        self.primary_target = Some(primary);
    }

    /// A clone of the data-path role flag, for the write guard / `ROLE` / `INFO`
    /// / frame consumer. They keep reading an `Arc<AtomicBool>`; only the
    /// *writer* moved here.
    pub fn role_flag(&self) -> Arc<AtomicBool> {
        self.is_replica.clone()
    }

    /// The primary this node is currently a replica of, if any.
    pub fn primary_target(&self) -> Option<SocketAddr> {
        self.primary_target
    }

    /// Whether this node is currently a replica.
    pub fn is_replica(&self) -> bool {
        self.is_replica.load(Ordering::Acquire)
    }

    /// Whether the current inbound replication stream is up: connected to
    /// `primary_target` and streaming, not merely dialing/handshaking/full-
    /// syncing. `false` when not a replica, or when a replica but neither the
    /// boot-spawned handler nor a runtime-demotion stream is currently
    /// connected. At most one of `boot_replica_handler`/`stream` is ever the
    /// active connection (`demote()` always retires the boot handler before
    /// installing a new `stream`), so checking both in sequence never double
    /// counts.
    pub fn link_up(&self) -> bool {
        if !self.is_replica() {
            return false;
        }
        if let Some(handler) = &self.boot_replica_handler {
            return handler.link_up();
        }
        if let Some(stream) = &self.stream {
            return stream.link_up();
        }
        false
    }

    /// Role Promotion: become a writable primary. Stops any inbound stream
    /// (runtime-demotion or boot-spawned) and clears the flag. Idempotent.
    pub fn promote(&mut self) {
        // Dropping the handle stops a runtime-demotion stream.
        self.stream = None;
        // Stop the boot-spawned reconnect loop too: it was started by
        // `Server::start_subsystems` before this manager existed, so it
        // lives outside `stream` and needs its own teardown.
        if let Some(handler) = self.boot_replica_handler.take() {
            handler.stop();
        }
        self.primary_target = None;
        self.is_replica.store(false, Ordering::Release);
        tracing::info!("Role Promotion complete: node is now a primary");
    }

    /// Role Demotion: become a replica of `primary`. Sets the read-only flag
    /// (fenced *before* the stream starts so no client write races the
    /// transition), tears down any existing stream, and opens a new one.
    /// Idempotent per target: re-demoting to the same primary is a no-op.
    pub fn demote(&mut self, primary: SocketAddr) {
        if self.is_replica() && self.primary_target == Some(primary) {
            tracing::debug!(primary = %primary, "Role Demotion no-op: already replicating");
            return;
        }
        // Fence: flip to read-only before opening the stream.
        self.is_replica.store(true, Ordering::Release);
        // Stop any prior stream before starting the new one.
        self.stream = None;
        // A boot-spawned handler being superseded by a fresh runtime target
        // must stop too, or it would keep dialing its original primary
        // alongside the new stream.
        if let Some(handler) = self.boot_replica_handler.take() {
            handler.stop();
        }
        self.stream = Some(self.streamer.start(primary));
        self.primary_target = Some(primary);
        tracing::info!(primary = %primary, "Role Demotion complete: node is now a replica");
    }
}

/// Cloneable, thread-safe handle to a [`RoleManager`].
///
/// Implements [`frogdb_core::RoleController`] so command handlers and the Raft
/// failover consumer can request transitions. Cloned into every shard's
/// `CommandContext` and the demotion-event consumer.
#[derive(Clone)]
pub struct RoleManagerHandle {
    inner: Arc<Mutex<RoleManager>>,
}

impl RoleManagerHandle {
    /// Wrap a manager for shared, synchronized access.
    pub fn new(manager: RoleManager) -> Self {
        Self {
            inner: Arc::new(Mutex::new(manager)),
        }
    }

    fn with_lock<R>(&self, f: impl FnOnce(&mut RoleManager) -> R) -> R {
        f(&mut self.inner.lock().expect("role manager poisoned"))
    }

    /// The data-path role flag owned by the wrapped manager.
    pub fn role_flag(&self) -> Arc<AtomicBool> {
        self.with_lock(|manager| manager.role_flag())
    }

    /// The primary this node is currently a replica of, if any.
    pub fn primary_target(&self) -> Option<SocketAddr> {
        self.with_lock(|manager| manager.primary_target())
    }

    /// Whether the current inbound replication stream is up. See
    /// [`RoleManager::link_up`].
    pub fn link_up(&self) -> bool {
        self.inner.lock().expect("role manager poisoned").link_up()
    }

    /// Adopt a boot-spawned replica handler so a later `promote()`/`demote()`
    /// also stops its reconnect loop. See
    /// [`RoleManager::register_boot_replica_handler`].
    pub fn register_boot_replica_handler(
        &self,
        handler: Arc<ReplicaReplicationHandler>,
        primary: SocketAddr,
    ) {
        self.with_lock(|manager| manager.register_boot_replica_handler(handler, primary));
    }
}

impl frogdb_core::RoleController for RoleManagerHandle {
    fn request_promote(&self) {
        self.with_lock(RoleManager::promote);
    }

    fn request_demote(&self, primary: SocketAddr) {
        self.with_lock(|manager| manager.demote(primary));
    }

    fn primary_target(&self) -> Option<SocketAddr> {
        RoleManagerHandle::primary_target(self)
    }

    fn master_link_up(&self) -> bool {
        self.link_up()
    }
}

// ============================================================================
// Real streamer: reuses the boot-time replication machinery
// ============================================================================

/// The production [`ReplicaStreamer`]. Builds a
/// [`ReplicaReplicationHandler`](crate::replication::ReplicaReplicationHandler)
/// and frame consumer — the exact pair `init_replication` and `subsystems`
/// spawn at boot — for a runtime `REPLICAOF`/failover-driven demotion.
///
/// A runtime demotion always targets a *new* primary (a fresh replication
/// identity), so it starts from a fresh [`ReplicationState`] and full-resyncs,
/// which is the correct behaviour for adopting a new primary.
pub struct RealReplicaStreamer {
    shard_senders: Arc<Vec<frogdb_core::ShardSender>>,
    num_shards: usize,
    listening_port: u16,
    data_dir: std::path::PathBuf,
    state_path: std::path::PathBuf,
    is_replica_flag: Arc<AtomicBool>,
    /// The cluster-bus HealthProbe offset atomic. When set, every runtime replica
    /// stream publishes its applied offset here so the failure detector observes a
    /// runtime-demoted Replica's offset exactly like a boot-configured one. `None`
    /// outside cluster mode.
    shared_offset: Option<Arc<std::sync::atomic::AtomicU64>>,
    /// Spontaneous replica→primary ACK cadence (`replication.ack-interval-ms`),
    /// stamped onto every runtime-demoted replica handler in `build_handler`.
    ack_interval_ms: u64,
    #[cfg(not(feature = "turmoil"))]
    tls: Option<ReplicaTlsConfig>,
}

#[cfg(not(feature = "turmoil"))]
struct ReplicaTlsConfig {
    manager: Arc<crate::tls::TlsManager>,
    handshake_timeout: std::time::Duration,
}

impl RealReplicaStreamer {
    /// Assemble the streamer from server configuration and shared collaborators.
    pub fn new(
        config: &crate::config::Config,
        shard_senders: Arc<Vec<frogdb_core::ShardSender>>,
        num_shards: usize,
        is_replica_flag: Arc<AtomicBool>,
        shared_offset: Option<Arc<std::sync::atomic::AtomicU64>>,
        #[cfg(not(feature = "turmoil"))] tls_manager: &Option<Arc<crate::tls::TlsManager>>,
    ) -> Self {
        let data_dir = config.persistence.data_dir.clone();
        let state_path = data_dir.join(&config.replication.state_file);

        #[cfg(not(feature = "turmoil"))]
        let tls = if config.tls.enabled
            && config.tls.tls_replication
            && let Some(mgr) = tls_manager
        {
            Some(ReplicaTlsConfig {
                manager: mgr.clone(),
                handshake_timeout: std::time::Duration::from_millis(
                    config.tls.handshake_timeout_ms,
                ),
            })
        } else {
            None
        };

        Self {
            shard_senders,
            num_shards,
            listening_port: config.server.port,
            data_dir,
            state_path,
            is_replica_flag,
            shared_offset,
            ack_interval_ms: config.replication.ack_interval_ms,
            #[cfg(not(feature = "turmoil"))]
            tls,
        }
    }
}

impl RealReplicaStreamer {
    /// Build (but do not spawn) the replica handler for `primary`: fresh
    /// replication identity, HealthProbe offset wiring, and TLS connect factory.
    /// Extracted from [`ReplicaStreamer::start`] so the offset-probe wiring can be
    /// asserted at a seam without opening a socket.
    fn build_handler(
        &self,
        primary: SocketAddr,
    ) -> (
        crate::replication::ReplicaReplicationHandler,
        tokio::sync::mpsc::Receiver<frogdb_core::ReplicationFrame>,
    ) {
        use crate::replication::ReplicaReplicationHandler;

        let (handler, frame_rx) = ReplicaReplicationHandler::new(
            primary,
            self.listening_port,
            frogdb_replication::ReplicationState::new(),
            self.state_path.clone(),
            self.data_dir.clone(),
        );

        let mut handler = handler;
        handler.set_ack_interval(self.ack_interval_ms);

        // Publish this stream's applied offset into the cluster-bus HealthProbe
        // atomic, mirroring the boot-time replica path in `init_replication`, so
        // the failure detector sees a runtime-demoted Replica's offset the same
        // as a boot-configured one.
        if let Some(offset) = &self.shared_offset {
            handler.set_shared_offset(offset.clone());
        }

        // Under turmoil the runtime-demoted replica must dial its new primary
        // through the simulated network, exactly like the boot-time replica path
        // in `replication_init.rs`. Without this the default factory's
        // `tokio::net::TcpStream::connect` panics ("IO is disabled") inside a
        // turmoil run, so a healed/demoted old primary could never re-attach.
        #[cfg(feature = "turmoil")]
        {
            let factory: frogdb_replication::replica::ConnectFactory =
                Arc::new(|addr: SocketAddr| {
                    Box::pin(async move {
                        let stream = turmoil::net::TcpStream::connect(addr).await?;
                        Ok(Box::new(stream) as frogdb_replication::BoxedStream)
                    })
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = std::io::Result<frogdb_replication::BoxedStream>,
                                    > + Send,
                            >,
                        >
                });
            handler.set_connect_factory(factory);
        }

        // Wire TLS for the outgoing connection, mirroring `init_replication`.
        #[cfg(not(feature = "turmoil"))]
        if let Some(tls) = &self.tls {
            let mgr = tls.manager.clone();
            let handshake_timeout = tls.handshake_timeout;
            let factory: frogdb_replication::replica::ConnectFactory =
                Arc::new(move |addr: SocketAddr| {
                    let mgr = mgr.clone();
                    Box::pin(async move {
                        let connector = mgr.connector().ok_or_else(|| {
                            std::io::Error::other("TLS client connector not configured")
                        })?;
                        crate::tls::tls_connect(&connector, addr, handshake_timeout).await
                    })
                        as std::pin::Pin<
                            Box<
                                dyn std::future::Future<
                                        Output = std::io::Result<frogdb_replication::BoxedStream>,
                                    > + Send,
                            >,
                        >
                });
            handler.set_connect_factory(factory);
        }

        (handler, frame_rx)
    }
}

impl ReplicaStreamer for RealReplicaStreamer {
    fn start(&self, primary: SocketAddr) -> Box<dyn ReplicaStream> {
        use crate::replication::{ReplicaCommandExecutor, consume_frames};

        let (handler, frame_rx) = self.build_handler(primary);
        let handler = Arc::new(handler);
        let replication_state = Some(handler.shared_state());

        // Connection task: connects to the primary and receives frames.
        let handler_clone = handler.clone();
        let conn = crate::net::spawn(async move {
            if let Err(e) = handler_clone.start().await {
                tracing::error!(error = %e, "Replica replication connection error");
            }
        });

        // Frame consumer task: applies replicated commands to the shards. Stops
        // itself when the role flag flips back to primary.
        let executor = ReplicaCommandExecutor::new(self.shard_senders.clone(), self.num_shards);
        let flag = self.is_replica_flag.clone();
        let consumer = crate::net::spawn(async move {
            consume_frames(frame_rx, executor, flag, replication_state).await;
        });

        tracing::info!(primary = %primary, "Runtime replica stream started");

        Box::new(RealReplicaStream {
            conn: Some(conn),
            consumer: Some(consumer),
            handler,
        })
    }
}

/// Handle to a running real replica stream. `Drop` stops it.
struct RealReplicaStream {
    conn: Option<crate::net::JoinHandle<()>>,
    consumer: Option<crate::net::JoinHandle<()>>,
    handler: Arc<crate::replication::ReplicaReplicationHandler>,
}

impl ReplicaStream for RealReplicaStream {
    fn link_up(&self) -> bool {
        self.handler.link_up()
    }
}

impl Drop for RealReplicaStream {
    fn drop(&mut self) {
        // Signal the connection loop, then abort both tasks so the stream stops
        // promptly on promotion / re-demotion.
        self.handler.stop();
        if let Some(h) = self.conn.take() {
            h.abort();
        }
        if let Some(h) = self.consumer.take() {
            h.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// A fake stream whose `Drop` records that it was stopped.
    struct FakeStream {
        stops: Arc<AtomicUsize>,
    }
    impl ReplicaStream for FakeStream {
        fn link_up(&self) -> bool {
            false
        }
    }
    impl Drop for FakeStream {
        fn drop(&mut self) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A fake streamer that records every `start(addr)` call and hands back a
    /// stream that records its stop.
    #[derive(Default)]
    struct FakeStreamer {
        started: Mutex<Vec<SocketAddr>>,
        stops: Arc<AtomicUsize>,
    }
    impl ReplicaStreamer for FakeStreamer {
        fn start(&self, primary: SocketAddr) -> Box<dyn ReplicaStream> {
            self.started.lock().unwrap().push(primary);
            Box::new(FakeStream {
                stops: self.stops.clone(),
            })
        }
    }

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn manager(replica_at_boot: bool) -> (RoleManager, Arc<FakeStreamer>) {
        let streamer = Arc::new(FakeStreamer::default());
        let flag = Arc::new(AtomicBool::new(replica_at_boot));
        (RoleManager::new(flag, streamer.clone(), None), streamer)
    }

    #[test]
    fn demote_sets_flag_records_target_and_starts_stream() {
        let (mut mgr, streamer) = manager(false);
        let a = addr("127.0.0.1:7000");

        mgr.demote(a);

        // Data-path flag flips to replica, target recorded, stream opened.
        assert!(
            mgr.is_replica(),
            "demote must flip the role flag to replica"
        );
        assert!(mgr.role_flag().load(Ordering::Acquire));
        assert_eq!(mgr.primary_target(), Some(a));
        assert_eq!(*streamer.started.lock().unwrap(), vec![a]);
    }

    /// Regression test for the old no-op stub: `REPLICAOF host port` returned
    /// `+OK` while flipping nothing and opening no stream. This asserts the
    /// exact conditions that stub violated, so it FAILS against the old code.
    #[test]
    fn regression_demote_is_not_a_no_op() {
        let (mut mgr, streamer) = manager(false);
        let a = addr("10.0.0.5:6380");

        // Precondition matching the old stub's world: a writable primary.
        assert!(!mgr.is_replica());

        mgr.demote(a);

        // The old stub left is_replica false and never called the streamer.
        assert!(
            mgr.is_replica(),
            "old stub left the node writable after REPLICAOF host port"
        );
        assert_eq!(
            streamer.started.lock().unwrap().len(),
            1,
            "old stub opened no connection to the new primary"
        );
        assert_eq!(mgr.primary_target(), Some(a));
    }

    #[test]
    fn promote_clears_flag_and_stops_stream() {
        let (mut mgr, streamer) = manager(false);
        mgr.demote(addr("127.0.0.1:7000"));
        assert_eq!(streamer.stops.load(Ordering::SeqCst), 0);

        mgr.promote();

        assert!(!mgr.is_replica(), "promote must clear the role flag");
        assert_eq!(mgr.primary_target(), None);
        assert_eq!(
            streamer.stops.load(Ordering::SeqCst),
            1,
            "promote must stop the inbound stream"
        );
    }

    #[test]
    fn demote_promote_round_trip() {
        let (mut mgr, streamer) = manager(false);
        let a = addr("127.0.0.1:7000");

        mgr.demote(a);
        assert!(mgr.is_replica());
        mgr.promote();
        assert!(!mgr.is_replica());
        mgr.demote(a);
        assert!(mgr.is_replica());
        assert_eq!(mgr.primary_target(), Some(a));

        // demote, promote, demote => two starts, one stop (from the promote).
        assert_eq!(streamer.started.lock().unwrap().len(), 2);
        assert_eq!(streamer.stops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn demote_is_idempotent_per_target_but_switches_primaries() {
        let (mut mgr, streamer) = manager(false);
        let a = addr("127.0.0.1:7000");
        let b = addr("127.0.0.1:7001");

        mgr.demote(a);
        mgr.demote(a); // idempotent: no new stream, no stop
        assert_eq!(streamer.started.lock().unwrap().len(), 1);
        assert_eq!(streamer.stops.load(Ordering::SeqCst), 0);

        mgr.demote(b); // different primary: stop old, start new
        assert_eq!(mgr.primary_target(), Some(b));
        assert_eq!(streamer.started.lock().unwrap().len(), 2);
        assert_eq!(streamer.stops.load(Ordering::SeqCst), 1);
    }

    /// Build a bare `RealReplicaStreamer` for the offset-wiring seam tests. The
    /// shard/flag collaborators are unused by `build_handler`, so they can be
    /// empty; only `shared_offset` (and path/port config) matter here.
    fn streamer_with_offset(
        shared_offset: Option<Arc<std::sync::atomic::AtomicU64>>,
    ) -> RealReplicaStreamer {
        RealReplicaStreamer {
            shard_senders: Arc::new(Vec::new()),
            num_shards: 1,
            listening_port: 0,
            data_dir: std::env::temp_dir(),
            state_path: std::env::temp_dir().join("replication_state.json"),
            is_replica_flag: Arc::new(AtomicBool::new(false)),
            shared_offset,
            ack_interval_ms: 1000,
            #[cfg(not(feature = "turmoil"))]
            tls: None,
        }
    }

    /// Probe seam (issue 07, criterion 3): a runtime-started replica stream must
    /// wire the cluster-bus HealthProbe offset atomic, so a runtime-demoted
    /// Replica's replication offset is visible to the failure detector exactly
    /// like a boot-configured one. `build_handler` (the non-spawning half of
    /// `start`) must hand the handler the SAME atomic the cluster bus reads.
    #[test]
    fn runtime_stream_wires_shared_offset_to_healthprobe_atomic() {
        // The atomic the cluster-bus HealthProbe answers with.
        let probe_offset = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let streamer = streamer_with_offset(Some(probe_offset.clone()));

        let (handler, _frame_rx) = streamer.build_handler(addr("127.0.0.1:7000"));
        let wired = handler
            .shared_offset()
            .expect("runtime stream must wire the HealthProbe offset");

        // Same atomic instance the failure detector reads via the cluster bus.
        assert!(
            Arc::ptr_eq(&wired, &probe_offset),
            "runtime replica must publish into the cluster-bus HealthProbe atomic"
        );

        // An offset the stream publishes is visible through the probe atomic,
        // exactly as for a boot-configured replica.
        wired.store(4242, Ordering::Release);
        assert_eq!(probe_offset.load(Ordering::Acquire), 4242);
    }

    /// Outside cluster mode there is no HealthProbe atomic; a runtime stream then
    /// leaves the handler's offset unwired (the boot path behaves the same).
    #[test]
    fn runtime_stream_without_cluster_leaves_offset_unwired() {
        let streamer = streamer_with_offset(None);
        let (handler, _frame_rx) = streamer.build_handler(addr("127.0.0.1:7000"));
        assert!(handler.shared_offset().is_none());
    }

    #[test]
    fn handle_request_demote_flips_flag_through_role_controller() {
        use frogdb_core::RoleController;

        let (mgr, streamer) = manager(false);
        let flag = mgr.role_flag();
        let handle = RoleManagerHandle::new(mgr);
        let a = addr("127.0.0.1:7000");

        <RoleManagerHandle as RoleController>::request_demote(&handle, a);

        // The shared flag every write guard / ROLE reads is now true.
        assert!(flag.load(Ordering::Acquire));
        assert_eq!(handle.primary_target(), Some(a));
        assert_eq!(*streamer.started.lock().unwrap(), vec![a]);

        <RoleManagerHandle as RoleController>::request_promote(&handle);
        assert!(!flag.load(Ordering::Acquire));
        assert_eq!(handle.primary_target(), None);
    }

    /// A `replicaof`-configured boot target is recorded into the manager at
    /// construction, so `ROLE`/INFO report the real primary immediately at
    /// startup — not only after the first runtime `REPLICAOF host port`.
    #[test]
    fn boot_target_seeds_primary_target() {
        let streamer = Arc::new(FakeStreamer::default());
        let flag = Arc::new(AtomicBool::new(true));
        let a = addr("127.0.0.1:7000");

        let mgr = RoleManager::new(flag, streamer.clone(), Some(a));

        assert_eq!(mgr.primary_target(), Some(a));
        assert!(mgr.is_replica());
        // The boot stream is opened separately by `init_replication`, not by
        // the manager: seeding the target must not start a duplicate stream.
        assert!(streamer.started.lock().unwrap().is_empty());
    }

    /// A primary/standalone boot passes no target, and `primary_target()`
    /// stays `None` until (if ever) a runtime Role Demotion sets one.
    #[test]
    fn no_boot_target_on_primary_boot() {
        let (mgr, _streamer) = manager(false);
        assert_eq!(mgr.primary_target(), None);
    }

    /// A fake stream whose `link_up()` answer is fixed at construction, for
    /// asserting `RoleManager::link_up` propagates whatever the active stream
    /// reports without touching real sockets.
    struct FixedLinkStream(bool);
    impl ReplicaStream for FixedLinkStream {
        fn link_up(&self) -> bool {
            self.0
        }
    }
    struct FixedLinkStreamer(bool);
    impl ReplicaStreamer for FixedLinkStreamer {
        fn start(&self, _primary: SocketAddr) -> Box<dyn ReplicaStream> {
            Box::new(FixedLinkStream(self.0))
        }
    }

    #[test]
    fn link_up_is_false_before_any_replication_role() {
        let (mgr, _streamer) = manager(false);
        assert!(
            !mgr.link_up(),
            "a primary/standalone node has no inbound link to report"
        );
    }

    #[test]
    fn link_up_reflects_the_active_runtime_stream() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut mgr = RoleManager::new(flag, Arc::new(FixedLinkStreamer(true)), None);
        mgr.demote(addr("127.0.0.1:7000"));
        assert!(
            mgr.link_up(),
            "demote() must surface the new stream's link_up() answer"
        );
    }

    #[test]
    fn link_up_is_false_when_the_active_stream_reports_down() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut mgr = RoleManager::new(flag, Arc::new(FixedLinkStreamer(false)), None);
        mgr.demote(addr("127.0.0.1:7000"));
        assert!(
            !mgr.link_up(),
            "a replica mid-handshake/full-sync must not report up"
        );
    }

    #[test]
    fn link_up_is_false_after_promote_even_though_still_flagged_replica_briefly() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut mgr = RoleManager::new(flag, Arc::new(FixedLinkStreamer(true)), None);
        mgr.demote(addr("127.0.0.1:7000"));
        assert!(mgr.link_up());
        mgr.promote();
        assert!(
            !mgr.link_up(),
            "promote() drops the stream, so link_up must fall back to false"
        );
    }

    /// A registered boot-spawned handler (never connected in this test) must
    /// report `link_up() == false` — the same handler `INFO` would read from,
    /// exercised through the real `ReplicaReplicationHandler` rather than a
    /// fake, so this also guards the wiring between `RoleManager` and the
    /// production handler type.
    #[test]
    fn link_up_reads_through_boot_handler_before_any_connection() {
        let primary = addr("127.0.0.1:7000");
        let data_dir = std::env::temp_dir();
        let state_path = data_dir.join(format!(
            "frogdb-test-role-manager-link-up-{}-{}.json",
            std::process::id(),
            line!()
        ));
        let (handler, _rx) = ReplicaReplicationHandler::new(
            primary,
            6380,
            frogdb_replication::ReplicationState::new(),
            state_path,
            data_dir,
        );
        let handler = Arc::new(handler);

        let (mut mgr, _streamer) = manager(true);
        mgr.register_boot_replica_handler(handler, primary);

        assert!(
            !mgr.link_up(),
            "a boot handler that never connected must report down, not up"
        );
    }

    /// `RoleController::primary_target` (the trait method command handlers
    /// and INFO actually call through) must agree with the inherent method,
    /// including the boot-seeded value.
    #[test]
    fn role_controller_primary_target_matches_boot_seed() {
        use frogdb_core::RoleController;

        let streamer = Arc::new(FakeStreamer::default());
        let flag = Arc::new(AtomicBool::new(true));
        let a = addr("127.0.0.1:7000");
        let handle = RoleManagerHandle::new(RoleManager::new(flag, streamer, Some(a)));

        assert_eq!(
            <RoleManagerHandle as RoleController>::primary_target(&handle),
            Some(a)
        );
    }

    /// Regression test for the bug this issue fixes: a boot-spawned
    /// `ReplicaReplicationHandler` (built by `init_replication`/
    /// `start_subsystems`, entirely outside the `RoleManager`'s own `stream`
    /// field) used to keep dialing the old primary forever after a Role
    /// Promotion, because `RoleManager` had no idea it existed. This
    /// registers a boot handler pointed at a primary that always refuses the
    /// connection, drives some reconnect attempts, promotes, and asserts both
    /// that the reconnect task terminates (via `stop()`, no `abort()`) and
    /// that no further connection attempts happen afterward.
    #[tokio::test]
    async fn promote_stops_registered_boot_replica_handler() {
        use frogdb_replication::replica::ConnectFactory;
        use std::sync::atomic::AtomicUsize;
        use std::time::Duration;

        let attempts = Arc::new(AtomicUsize::new(0));
        let counter = attempts.clone();
        let factory: ConnectFactory = Arc::new(move |_addr| {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "test: primary unreachable",
                ))
            })
        });

        let primary = addr("127.0.0.1:7000");
        let data_dir = std::env::temp_dir();
        let state_path = data_dir.join(format!(
            "frogdb-test-role-manager-boot-{}-{}.json",
            std::process::id(),
            line!()
        ));
        let (mut handler, _rx) = ReplicaReplicationHandler::new(
            primary,
            6380,
            frogdb_replication::ReplicationState::new(),
            state_path,
            data_dir,
        );
        handler.set_connect_factory(factory);
        let handler = Arc::new(handler);

        let handler_clone = handler.clone();
        let task = tokio::spawn(async move { handler_clone.start().await });

        // Let a few reconnect attempts happen (first backoff is 100ms).
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(
            attempts.load(Ordering::SeqCst) >= 1,
            "boot handler should have attempted to connect at least once"
        );

        let (mut mgr, _streamer) = manager(true);
        mgr.register_boot_replica_handler(handler, primary);
        assert_eq!(mgr.primary_target(), Some(primary));

        mgr.promote();

        assert!(
            !mgr.is_replica(),
            "promote must clear the role flag even for a boot-spawned replica"
        );
        assert_eq!(mgr.primary_target(), None);

        // The reconnect loop must terminate via the shutdown watch, no
        // `task.abort()` needed.
        let result = tokio::time::timeout(Duration::from_secs(5), task).await;
        assert!(
            result.is_ok(),
            "promote() must stop the boot handler's reconnect loop without abort"
        );
        assert!(result.unwrap().unwrap().is_ok());

        // No further connection attempts to the old primary after promotion.
        let attempts_at_promote = attempts.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            attempts_at_promote,
            "no further connection attempts to the old primary after promote()"
        );
    }
}
