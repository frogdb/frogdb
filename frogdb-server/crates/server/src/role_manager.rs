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

/// A running inbound replication stream (connection task + frame consumer).
///
/// The stream is owned by the [`RoleManager`]; **dropping the handle tears the
/// stream down** (aborts its tasks and signals the connection loop). This is
/// the sole stop mechanism, so `promote()`/re-`demote()` need only replace the
/// stored handle.
pub trait ReplicaStream: Send {}

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
}

impl RoleManager {
    /// Build a manager over the shared role flag, using `streamer` to open
    /// inbound streams. The flag's current value is the boot role.
    pub fn new(is_replica: Arc<AtomicBool>, streamer: Arc<dyn ReplicaStreamer>) -> Self {
        Self {
            is_replica,
            primary_target: None,
            stream: None,
            streamer,
        }
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

    /// Role Promotion: become a writable primary. Stops any inbound stream and
    /// clears the flag. Idempotent.
    pub fn promote(&mut self) {
        // Dropping the handle stops the stream.
        self.stream = None;
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

    /// The data-path role flag owned by the wrapped manager.
    pub fn role_flag(&self) -> Arc<AtomicBool> {
        self.inner
            .lock()
            .expect("role manager poisoned")
            .role_flag()
    }

    /// The primary this node is currently a replica of, if any.
    pub fn primary_target(&self) -> Option<SocketAddr> {
        self.inner
            .lock()
            .expect("role manager poisoned")
            .primary_target()
    }
}

impl frogdb_core::RoleController for RoleManagerHandle {
    fn request_promote(&self) {
        self.inner.lock().expect("role manager poisoned").promote();
    }

    fn request_demote(&self, primary: SocketAddr) {
        self.inner
            .lock()
            .expect("role manager poisoned")
            .demote(primary);
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
            #[cfg(not(feature = "turmoil"))]
            tls,
        }
    }
}

impl ReplicaStreamer for RealReplicaStreamer {
    fn start(&self, primary: SocketAddr) -> Box<dyn ReplicaStream> {
        use crate::replication::{
            ReplicaCommandExecutor, ReplicaReplicationHandler, consume_frames,
        };

        let (handler, frame_rx) = ReplicaReplicationHandler::new(
            primary,
            self.listening_port,
            frogdb_replication::ReplicationState::new(),
            self.state_path.clone(),
            self.data_dir.clone(),
        );

        #[allow(unused_mut)]
        let mut handler = handler;

        // Publish this stream's applied offset into the cluster-bus HealthProbe
        // atomic, mirroring the boot-time replica path in `init_replication`, so
        // the failure detector sees a runtime-demoted Replica's offset the same
        // as a boot-configured one.
        if let Some(offset) = &self.shared_offset {
            handler.set_shared_offset(offset.clone());
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

impl ReplicaStream for RealReplicaStream {}

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
    impl ReplicaStream for FakeStream {}
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
        (RoleManager::new(flag, streamer.clone()), streamer)
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
}
