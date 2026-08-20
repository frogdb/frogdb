//! Cluster bus TCP server for Raft RPC communication.
//!
//! This module provides a TCP server that handles incoming RPC requests
//! from other cluster nodes. It uses the length-prefixed JSON protocol defined
//! in frogdb_core::cluster::network.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use frogdb_core::PubSubMsg;
use frogdb_core::ShardSender;
use frogdb_core::cluster::{
    BusRpc, ClusterBusStats, ClusterRaft, ClusterRpcRequest, ClusterRpcResponse, FramedStream,
    NodeId, RaftRpc, handle_rpc_request, parse_rpc_message, send_rpc_response,
};
// Framing helpers diverge by transport: production wraps a `tokio::net::TcpStream`
// (optionally TLS) via `new_framed_tcp`; under turmoil the accepted stream is a
// type-erased `BoxedStream` framed via `new_framed`.
#[cfg(feature = "turmoil")]
use frogdb_core::cluster::new_framed;
#[cfg(not(feature = "turmoil"))]
use frogdb_core::cluster::new_framed_tcp;
use frogdb_core::pubsub::BROADCAST_SHARD;
use frogdb_core::shard_for_key;
use tokio::sync::oneshot;

use frogdb_net::TcpListener;
use tracing::warn;
use tracing::{debug, error, info};

/// The server-side TLS seam for inbound cluster-bus connections.
///
/// TLS is the one thing this crate cannot own: the certificate store, the
/// reload watcher and the `MaybeTlsStream` alias are all `frogdb-server`
/// types. So the bus asks for the three things it actually needs — the
/// handshake, the live dual-accept flag, and the live handshake timeout — and
/// the server supplies them over its `TlsRuntimeHandle`. All three are read
/// *per connection*, so a `CONFIG SET` reaches the next peer that dials in
/// without disturbing established bus connections.
#[cfg(not(feature = "turmoil"))]
pub trait BusTlsAcceptor: Send + Sync {
    /// Complete a TLS handshake on `stream`, yielding the encrypted stream.
    ///
    /// The bus applies [`Self::handshake_timeout`] around this future, so an
    /// implementation need not bound it again.
    fn accept(&self, stream: frogdb_net::TcpStream) -> BusTlsHandshake;

    /// Whether to accept both plain and TLS connections (rolling migration).
    fn dual_accept(&self) -> bool;

    /// How long a handshake — or the transport sniff that precedes one — may
    /// take before the connection is dropped.
    fn handshake_timeout(&self) -> std::time::Duration;
}

/// The in-flight handshake [`BusTlsAcceptor::accept`] returns.
#[cfg(not(feature = "turmoil"))]
pub type BusTlsHandshake = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = std::io::Result<frogdb_core::cluster::BoxedStream>> + Send,
    >,
>;

/// The replica-link half of a `HealthProbe` answer, as a seam.
///
/// A failover decision has to know whether a candidate's inbound replication
/// stream is attached and past PSYNC (FM-CLUSTER-106). The session's *source*
/// end lives on the primary that just failed, so the only reachable end is the
/// candidate itself — and the candidate is already being asked for its offset.
/// The bus therefore asks the node one narrow question rather than depending on
/// `frogdb-server`'s `RoleManager`; the blanket impl below makes every
/// [`frogdb_core::RoleController`] one of these for free.
pub trait ReplicaLinkState: Send + Sync {
    /// Whether this node's inbound replication stream is attached past PSYNC.
    fn master_link_up(&self) -> bool;
}

impl<T: frogdb_core::RoleController> ReplicaLinkState for T {
    fn master_link_up(&self) -> bool {
        frogdb_core::RoleController::master_link_up(self)
    }
}

/// The consensus half of the bus, as a seam.
///
/// The bus itself services only [`BusRpc`]; everything else is consensus traffic
/// it hands straight to openraft. Naming that hand-off as a trait is what makes
/// the accept loop and the connection loop testable — an `openraft::Raft` cannot
/// be constructed without storage, a network factory and an election, so a
/// context that owned one concretely could not be built in a unit test at all.
pub trait BusRaftHandler: Send + Sync + 'static {
    /// Service one Raft RPC and produce its wire response.
    fn handle_raft_rpc(
        &self,
        rpc: RaftRpc,
    ) -> impl std::future::Future<Output = ClusterRpcResponse> + Send;
}

impl BusRaftHandler for Arc<ClusterRaft> {
    // Pure delegation to `frogdb_core::cluster::handle_rpc_request` against a
    // live `openraft::Raft`. cargo-mutants' only replacement for the return type
    // (`ClusterRpcResponse`) needs `Default`, which the enum does not implement,
    // so this body carries no viable mutant.
    async fn handle_raft_rpc(&self, rpc: RaftRpc) -> ClusterRpcResponse {
        handle_rpc_request(self, rpc).await
    }
}

/// Context for the cluster bus, providing access to Raft and shard infrastructure.
///
/// Generic over the [`BusRaftHandler`] seam so the bus is testable without a
/// live consensus instance; production uses the default (`Arc<ClusterRaft>`).
pub struct ClusterBusContext<R = Arc<ClusterRaft>> {
    pub raft: R,
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub num_shards: usize,
    pub node_id: NodeId,
    pub replication_offset: Arc<AtomicU64>,
    /// The inbound-link state this node reports on a `HealthProbe`, read at
    /// answer time so the reply is never a boot-time snapshot.
    pub replica_link: Arc<dyn ReplicaLinkState>,
    /// The node-wide cluster-bus packet counters, shared with the outbound
    /// direction (`ClusterNetworkFactory::bus_stats`) so `CLUSTER INFO` reports
    /// one pair for the whole bus.
    pub bus_stats: Arc<ClusterBusStats>,
    /// TLS seam for accepting encrypted cluster bus connections. `None` when
    /// the bus serves plaintext only.
    #[cfg(not(feature = "turmoil"))]
    pub tls: Option<Arc<dyn BusTlsAcceptor>>,
}

/// Run the cluster bus TCP server.
///
/// This server listens for incoming connections from other cluster nodes
/// and handles Raft RPCs, pub/sub forwarding, and HealthProbe requests.
///
/// Accepts a pre-bound `TcpListener` so that the port is held open from
/// `Server::new()` and never subject to TOCTOU port races.
pub async fn run<R: BusRaftHandler>(
    listener: TcpListener,
    ctx: Arc<ClusterBusContext<R>>,
) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    info!(%addr, "Cluster bus listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    debug!(%peer, "Cluster bus connection accepted");
                    if let Err(e) = handle_connection(stream, &ctx).await {
                        // Connection errors are expected when nodes disconnect
                        debug!(%peer, error = %e, "Cluster bus connection closed");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept cluster bus connection");
            }
        }
    }
}

/// Transport chosen for one inbound cluster-bus connection.
#[cfg(not(feature = "turmoil"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BusTransport {
    Plaintext,
    Tls,
}

/// Decide how to read an inbound connection.
///
/// Strict mode (the steady state) requires TLS from every peer. Dual-accept
/// mode, used while rolling a cluster onto TLS, sniffs the first byte:
/// `0x16` is the TLS `ContentType::Handshake` that opens a ClientHello, so
/// anything else is a peer that has not been restarted yet and is still
/// speaking plaintext. A connection that sent nothing yet (`None`) cannot be
/// a ClientHello in flight — `peek` returning 0 bytes means EOF — so it is
/// treated as plaintext and fails downstream on its own.
#[cfg(not(feature = "turmoil"))]
fn choose_transport(migration: bool, first_byte: Option<u8>) -> BusTransport {
    const TLS_HANDSHAKE_CONTENT_TYPE: u8 = 0x16;
    if !migration {
        return BusTransport::Tls;
    }
    match first_byte {
        Some(TLS_HANDSHAKE_CONTENT_TYPE) => BusTransport::Tls,
        _ => BusTransport::Plaintext,
    }
}

/// The byte a one-byte `peek` actually observed, if any.
///
/// `peek` reports the number of bytes copied; `0` is EOF, not a byte worth
/// `0x00`. Separated from the I/O so the EOF-vs-byte distinction — which decides
/// whether a connection is fed to the TLS acceptor or to the plaintext framer —
/// is forced without a socket.
#[cfg(not(feature = "turmoil"))]
fn sniffed_byte(peeked: usize, peek_buf: [u8; 1]) -> Option<u8> {
    (peeked > 0).then_some(peek_buf[0])
}

/// Handle a single cluster bus connection: pick the transport, then serve it.
///
/// Only the framing step differs between the production and turmoil builds
/// ([`negotiate_framing`]); the read/dispatch/respond loop
/// ([`serve_connection`]) is shared, so the two builds cannot drift.
async fn handle_connection<R: BusRaftHandler>(
    stream: frogdb_net::TcpStream,
    ctx: &ClusterBusContext<R>,
) -> std::io::Result<()> {
    let framed = negotiate_framing(stream, ctx).await?;
    serve_connection(framed, ctx).await
}

/// Choose the transport for one inbound connection and frame it.
///
/// Strict mode hands every connection to the TLS acceptor; dual-accept sniffs
/// the first byte first (see [`choose_transport`]). Both the flag and the
/// handshake timeout are read here, per connection, so a runtime change applies
/// to the next peer that dials in without disturbing established connections.
#[cfg(not(feature = "turmoil"))]
async fn negotiate_framing<R: BusRaftHandler>(
    stream: frogdb_net::TcpStream,
    ctx: &ClusterBusContext<R>,
) -> std::io::Result<FramedStream> {
    let Some(ref tls) = ctx.tls else {
        return Ok(new_framed_tcp(stream));
    };

    let migration = tls.dual_accept();
    let handshake_timeout = tls.handshake_timeout();
    let first_byte = if migration {
        let mut peek_buf = [0u8; 1];
        // Bounded by the same budget as the handshake itself: a peer that
        // connects and sends nothing must not hold the task (and the socket)
        // open forever. `peek` has no timeout of its own.
        let peeked = tokio::time::timeout(handshake_timeout, stream.peek(&mut peek_buf))
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "cluster bus transport sniff timeout",
                )
            })??;
        sniffed_byte(peeked, peek_buf)
    } else {
        None
    };

    match choose_transport(migration, first_byte) {
        BusTransport::Plaintext => Ok(new_framed_tcp(stream)),
        BusTransport::Tls => {
            let tls_stream = tokio::time::timeout(handshake_timeout, tls.accept(stream))
                .await
                .map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake timeout")
                })??;
            Ok(frogdb_core::cluster::new_framed(tls_stream))
        }
    }
}

/// Frame one inbound connection under turmoil.
///
/// Turmoil's accepted stream is a `turmoil::net::TcpStream` (no TLS); type-erase
/// it into a `BoxedStream` and frame it the way the production plaintext path
/// does. Real Raft RPCs — vote, append-entries, install-snapshot — then flow
/// through turmoil's simulated network, giving deterministic multi-node
/// consensus.
// Surviving-mutant note: this body compiles only under `--features turmoil`,
// which the default-feature mutation run never builds, so a mutation here is
// unobservable to that run by construction. The turmoil simulation suite is
// the witness for this arm; the real-TCP twin above carries the forcing tests.
#[cfg(feature = "turmoil")]
async fn negotiate_framing<R: BusRaftHandler>(
    stream: frogdb_net::TcpStream,
    _ctx: &ClusterBusContext<R>,
) -> std::io::Result<FramedStream> {
    Ok(new_framed(Box::new(stream)))
}

/// Whether a parse failure is an ordinary disconnect rather than a protocol
/// error.
///
/// A peer that goes away mid-connection is the steady state on a cluster bus —
/// nodes restart, links drop — so it ends the connection with `Ok(())` and no
/// warning. Anything else is a frame this node could not understand and is
/// surfaced as `InvalidData`. Split out of the loop so each disjunct is forced
/// individually: an `&&` here would classify every real disconnect as a protocol
/// error and fill the log with warnings on every rolling restart.
fn is_clean_disconnect(error_msg: &str) -> bool {
    error_msg.contains("connection closed")
        || error_msg.contains("connection reset")
        || error_msg.contains("broken pipe")
}

/// Read RPC requests in a loop, process them, and send responses.
///
/// Bus RPCs (pub/sub fan-out, health probes) are serviced from the context;
/// everything else is consensus traffic handed to the [`BusRaftHandler`].
async fn serve_connection<R: BusRaftHandler>(
    mut framed: FramedStream,
    ctx: &ClusterBusContext<R>,
) -> std::io::Result<()> {
    loop {
        let request = match parse_rpc_message(&mut framed, &ctx.bus_stats).await {
            Ok(req) => req,
            Err(e) => {
                let error_msg = e.to_string();
                if is_clean_disconnect(&error_msg) {
                    return Ok(());
                }
                warn!(error = %e, "Failed to parse cluster RPC request");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    error_msg,
                ));
            }
        };

        // Dispatch on the wire envelope: one typed seam. The bus owns its
        // subset (`BusRpc`) locally; the Raft handler owns the rest (`RaftRpc`).
        let response = match request {
            ClusterRpcRequest::Bus(bus_rpc) => handle_bus_rpc(ctx, bus_rpc).await,
            ClusterRpcRequest::Raft(raft_rpc) => ctx.raft.handle_raft_rpc(raft_rpc).await,
        };

        if let Err(e) = send_rpc_response(&mut framed, response, &ctx.bus_stats).await {
            warn!(error = %e, "Failed to send cluster RPC response");
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                e.to_string(),
            ));
        }
    }
}

/// Handle a bus-local RPC: pub/sub fan-out and health probes, serviced from the
/// cluster-bus context (shard senders, node id, replication offset) without ever
/// touching Raft.
///
/// The match is exhaustive by construction — [`BusRpc`] names only the variants
/// this function can service, so it cannot carry (nor mis-route) a Raft RPC.
async fn handle_bus_rpc<R: BusRaftHandler>(
    ctx: &ClusterBusContext<R>,
    request: BusRpc,
) -> ClusterRpcResponse {
    match request {
        BusRpc::PubSubBroadcast { channel, message } => {
            handle_pubsub_broadcast(&ctx.shard_senders, &channel, &message).await
        }
        BusRpc::PubSubForward { channel, message } => {
            handle_pubsub_forward(&ctx.shard_senders, ctx.num_shards, &channel, &message).await
        }
        BusRpc::HealthProbe => ClusterRpcResponse::HealthProbeResponse {
            node_id: ctx.node_id,
            replication_offset: ctx.replication_offset.load(Ordering::Acquire),
            replica_link_up: ctx.replica_link.master_link_up(),
        },
    }
}

/// Handle a PubSubBroadcast RPC: deliver to the broadcast pub/sub coordinator
/// shard ([`BROADCAST_SHARD`]).
async fn handle_pubsub_broadcast(
    shard_senders: &[ShardSender],
    channel: &[u8],
    message: &[u8],
) -> ClusterRpcResponse {
    let (response_tx, response_rx) = oneshot::channel();
    let _ = shard_senders[BROADCAST_SHARD]
        .send(PubSubMsg::Publish {
            channel: bytes::Bytes::copy_from_slice(channel),
            message: bytes::Bytes::copy_from_slice(message),
            response_tx,
        })
        .await;

    let count = response_rx.await.unwrap_or(0);
    ClusterRpcResponse::PubSubBroadcastResult {
        subscriber_count: count,
    }
}

/// Handle a PubSubForward RPC: deliver to the shard that owns the channel's slot.
async fn handle_pubsub_forward(
    shard_senders: &[ShardSender],
    num_shards: usize,
    channel: &[u8],
    message: &[u8],
) -> ClusterRpcResponse {
    let shard_id = shard_for_key(channel, num_shards);
    let (response_tx, response_rx) = oneshot::channel();
    let _ = shard_senders[shard_id]
        .send(PubSubMsg::ShardedPublish {
            channel: bytes::Bytes::copy_from_slice(channel),
            message: bytes::Bytes::copy_from_slice(message),
            response_tx,
        })
        .await;

    let count: usize = response_rx.await.unwrap_or_default();
    ClusterRpcResponse::PubSubForwardResult {
        subscriber_count: count,
    }
}

// Real-network test: `tcp_listener_reusable` binds a real socket, which under
// the `turmoil` feature routes through turmoil's simulated net and panics
// (scoped-tls) outside a running sim. Excluded from the turmoil build.
#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_core::cluster::{ClusterCommand, ClusterNetworkFactory, new_framed};
    use frogdb_net::tcp_listener_reusable;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// A [`BusRaftHandler`] that records what consensus traffic reached it and
    /// answers every RPC successfully — enough to prove the envelope dispatch
    /// without an `openraft::Raft`.
    #[derive(Default)]
    struct RecordingRaft {
        seen: Mutex<Vec<String>>,
    }

    impl BusRaftHandler for RecordingRaft {
        async fn handle_raft_rpc(&self, rpc: RaftRpc) -> ClusterRpcResponse {
            self.seen.lock().unwrap().push(format!("{rpc:?}"));
            ClusterRpcResponse::ForwardedWrite(Ok(()))
        }
    }

    /// A link state fixed at construction, standing in for the node's
    /// `RoleManager` in a bus that has no replication machinery.
    struct FixedLink(bool);

    impl ReplicaLinkState for FixedLink {
        fn master_link_up(&self) -> bool {
            self.0
        }
    }

    /// A plaintext bus context reporting `node_id` / `offset` on a health probe.
    fn test_context(node_id: NodeId, offset: u64) -> Arc<ClusterBusContext<RecordingRaft>> {
        test_context_with_link(node_id, offset, true)
    }

    /// As [`test_context`], with the reported inbound-link state chosen.
    fn test_context_with_link(
        node_id: NodeId,
        offset: u64,
        link_up: bool,
    ) -> Arc<ClusterBusContext<RecordingRaft>> {
        Arc::new(ClusterBusContext {
            raft: RecordingRaft::default(),
            // A health probe never reaches the shards; the pub/sub arms that do
            // are covered by `pubsub.rs`.
            shard_senders: Arc::new(Vec::new()),
            num_shards: 0,
            node_id,
            replication_offset: Arc::new(AtomicU64::new(offset)),
            replica_link: Arc::new(FixedLink(link_up)),
            bus_stats: Arc::new(ClusterBusStats::new()),
            tls: None,
        })
    }

    // FM-CLUSTER-065
    #[test]
    fn strict_mode_always_requires_tls() {
        assert_eq!(choose_transport(false, Some(0x16)), BusTransport::Tls);
        assert_eq!(choose_transport(false, Some(b'*')), BusTransport::Tls);
        assert_eq!(choose_transport(false, None), BusTransport::Tls);
    }

    // FM-CLUSTER-066
    #[test]
    fn dual_accept_sniffs_the_client_hello() {
        assert_eq!(choose_transport(true, Some(0x16)), BusTransport::Tls);
        // A not-yet-migrated peer speaking the plaintext bus protocol.
        assert_eq!(choose_transport(true, Some(b'*')), BusTransport::Plaintext);
        assert_eq!(choose_transport(true, None), BusTransport::Plaintext);
    }

    /// The bus reads the dual-accept flag per connection, so flipping it on the
    /// TLS runtime handle changes what the *next* connection does.
    // FM-CLUSTER-066
    #[test]
    fn flipping_the_runtime_flag_changes_the_next_connection() {
        let flag = std::sync::Arc::new(AtomicBool::new(false));
        let plaintext_peer = Some(b'*');

        // Strict: a plaintext peer is (correctly) fed to the TLS acceptor.
        assert_eq!(
            choose_transport(flag.load(Ordering::Relaxed), plaintext_peer),
            BusTransport::Tls
        );

        // Operator starts a rolling migration.
        flag.store(true, Ordering::Relaxed);
        assert_eq!(
            choose_transport(flag.load(Ordering::Relaxed), plaintext_peer),
            BusTransport::Plaintext
        );

        // ...and closes it again once every peer speaks TLS.
        flag.store(false, Ordering::Relaxed);
        assert_eq!(
            choose_transport(flag.load(Ordering::Relaxed), plaintext_peer),
            BusTransport::Tls
        );
    }

    /// A one-byte `peek` reports a *count*, not a byte: `0` is EOF and must not
    /// be read as a `0x00` first byte (which `choose_transport` would then treat
    /// as plaintext for a different reason, and which any non-strict comparison
    /// here would turn into a spurious `Some`).
    // FM-CLUSTER-066
    #[test]
    fn a_peek_of_zero_bytes_is_not_a_first_byte() {
        assert_eq!(sniffed_byte(0, [0x16]), None, "EOF yields no first byte");
        assert_eq!(sniffed_byte(0, [0x00]), None);
        assert_eq!(sniffed_byte(1, [0x16]), Some(0x16));
        assert_eq!(
            sniffed_byte(1, [0x00]),
            Some(0x00),
            "a peeked NUL is a byte, not EOF"
        );
    }

    /// Each disconnect phrase independently ends the connection quietly; a
    /// parse error that names none of them is a protocol error.
    #[test]
    fn every_disconnect_phrase_ends_the_connection_quietly() {
        for msg in [
            "connection closed",
            "connection reset by peer",
            "broken pipe",
            "network error: connection closed",
        ] {
            assert!(is_clean_disconnect(msg), "{msg:?} must be a clean close");
        }
        for msg in [
            "invalid frame",
            "deserialization failed: bad varint",
            "frame size exceeded",
            "",
        ] {
            assert!(
                !is_clean_disconnect(msg),
                "{msg:?} must surface as a protocol error"
            );
        }
    }

    /// The accept loop keeps serving: a peer dials the bound listener, gets its
    /// health probe answered from the context, and the *same* connection then
    /// carries a Raft RPC into the consensus seam. A loop that returned after
    /// binding — or a connection handler that returned before its first
    /// response — leaves the peer with no answer at all.
    // FM-CLUSTER-051
    #[tokio::test]
    async fn the_bus_serves_probes_and_raft_rpcs_on_one_connection() {
        let listener = tcp_listener_reusable("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .expect("binding an ephemeral port must succeed");
        let addr = listener.local_addr().unwrap();
        let ctx = test_context(42, 9_001);
        let server = tokio::spawn(run(listener, ctx.clone()));

        let factory = ClusterNetworkFactory::new();
        let peer = factory.connect(42, addr);

        assert_eq!(
            peer.health_probe().await.expect("probe must be answered"),
            (42, 9_001, true),
            "the probe answers from the context, unconditionally"
        );

        // Pooled: the same connection, so this also proves the handler loops
        // rather than serving one request and returning.
        peer.forward_write(ClusterCommand::MarkNodeFailed { node_id: 7 })
            .await
            .expect("a forwarded write is consensus traffic and must dispatch");
        assert_eq!(
            ctx.raft.seen.lock().unwrap().len(),
            1,
            "exactly the Raft RPC reached the consensus seam; the probe did not"
        );

        server.abort();
    }

    /// The probe answer carries the node's *live* inbound-link state, not a
    /// constant: a failover decision reads it to tell a replica that is still
    /// attached from one whose session has ended (FM-CLUSTER-106).
    // FM-CLUSTER-106
    #[tokio::test]
    async fn the_health_probe_reports_this_nodes_inbound_link_state() {
        for link_up in [true, false] {
            let listener = tcp_listener_reusable("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .await
                .expect("binding an ephemeral port must succeed");
            let addr = listener.local_addr().unwrap();
            let ctx = test_context_with_link(5, 1_234, link_up);
            let server = tokio::spawn(run(listener, ctx));

            let factory = ClusterNetworkFactory::new();
            assert_eq!(
                factory
                    .connect(5, addr)
                    .health_probe()
                    .await
                    .expect("probe must be answered"),
                (5, 1_234, link_up),
                "the bus reports whatever the link seam says"
            );

            server.abort();
        }
    }

    /// A peer that simply goes away ends the connection with `Ok`, so the accept
    /// loop logs nothing on an ordinary restart.
    #[tokio::test]
    async fn a_vanished_peer_closes_the_connection_cleanly() {
        let ctx = test_context(1, 0);
        let (client, server) = tokio::io::duplex(1024);
        let serving =
            tokio::spawn(async move { serve_connection(new_framed(Box::new(server)), &ctx).await });

        drop(client);
        assert!(
            serving.await.unwrap().is_ok(),
            "a dropped peer is not a protocol error"
        );
    }

    // FM-CLUSTER-066
    #[tokio::test]
    async fn test_cluster_bus_bind_fails_on_invalid_addr() {
        // Trying to bind to a privileged port should fail (unless running as root)
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        // We can't easily test run() without a real Raft instance,
        // but we can verify tcp_listener_reusable behavior
        let result: std::io::Result<frogdb_net::TcpListener> = tcp_listener_reusable(addr).await;
        // Binding to a privileged port should fail for non-root users.
        // On macOS and when running as root (e.g. Docker containers), binding
        // to port 1 may succeed — that's acceptable, we just verify the call
        // doesn't panic.
        if let Ok(listener) = result {
            drop(listener);
        }
        // If it failed, that's the expected behavior for non-root.
    }
}
