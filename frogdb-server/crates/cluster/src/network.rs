//! Raft network layer for inter-node TCP communication.
//!
//! This module provides the network implementation for openraft, enabling
//! cluster nodes to communicate via TCP for Raft consensus operations.
//!
//! ## Wire protocol
//!
//! Messages are length-delimited frames (4-byte big-endian length prefix)
//! containing postcard-serialized `ClusterRpcRequest`/`ClusterRpcResponse`
//! enums. The framing is handled by `tokio_util::codec::LengthDelimitedCodec`.
//!
//! ## Connection pooling
//!
//! `ClusterNetworkFactory` maintains one persistent TCP connection per peer
//! (via `ConnectionPool`). Connections are lazily established on first RPC
//! and automatically reconnected on I/O errors.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::bytes::Bytes;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::stats::ClusterBusStats;
use crate::types::{ClusterCommand, ClusterError, ClusterResponse, NodeId, TypeConfig};
use openraft::ChangeMembers;

/// Supertrait combining `AsyncRead + AsyncWrite` for use in trait objects.
pub trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}

/// A type-erased async I/O stream.
pub type BoxedStream = Box<dyn AsyncReadWrite + Unpin + Send>;

/// A framed stream using length-delimited encoding over a type-erased I/O stream.
pub type FramedStream = Framed<BoxedStream, LengthDelimitedCodec>;

/// Factory for creating connections to cluster peers.
///
/// The server crate injects either a plain TCP or TLS-wrapped factory.
pub type ConnectFactory = Arc<
    dyn Fn(SocketAddr) -> Pin<Box<dyn Future<Output = io::Result<BoxedStream>> + Send>>
        + Send
        + Sync,
>;

/// Default connection factory: plain TCP with 5-second timeout.
pub fn plain_tcp_connect_factory(connect_timeout_ms: u64) -> ConnectFactory {
    Arc::new(move |addr| {
        let timeout_dur = std::time::Duration::from_millis(connect_timeout_ms);
        Box::pin(async move {
            let stream = tokio::time::timeout(timeout_dur, TcpStream::connect(addr))
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection timeout"))??;
            Ok(Box::new(stream) as BoxedStream)
        })
    })
}

/// Maximum frame size for cluster RPC messages.
///
/// Not an independent number: the bus carries user payloads as well as
/// consensus traffic — [`BusRpc::PubSubBroadcast`] and [`BusRpc::PubSubForward`]
/// ship the channel and message a client published — so a ceiling below what
/// the connection layer accepts is a message that is taken and then cannot be
/// delivered. It is therefore the shared internal ceiling
/// ([`frogdb_protocol::MAX_INTERNAL_FRAME_LEN`]), the same one the replication
/// frame codec derives from (round-2 issue 69).
const MAX_FRAME_SIZE: usize = frogdb_protocol::MAX_INTERNAL_FRAME_LEN;

/// Simple error wrapper for network errors that implements std::error::Error.
#[derive(Debug)]
struct NetworkErrorWrapper(String);

impl std::fmt::Display for NetworkErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for NetworkErrorWrapper {}

/// RPCs serviced through the [`ClusterRaft`](crate::ClusterRaft) handle: the
/// openraft consensus trio plus the application write-forward (which reaches the
/// leader via `client_write`). `ForwardedWrite` lives here — with the consensus
/// RPCs — because it needs the Raft handle, not because it is consensus traffic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpc {
    /// AppendEntries RPC (leader to followers).
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// Vote RPC (candidate to all nodes).
    Vote(VoteRequest<NodeId>),
    /// InstallSnapshot RPC (leader to lagging followers).
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Forwarded client write (follower to leader).
    ForwardedWrite(ClusterCommand),
}

/// RPCs serviced locally from the cluster-bus context (shard senders, node id,
/// replication offset) — these never touch Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BusRpc {
    /// Broadcast pub/sub message to all nodes.
    PubSubBroadcast { channel: Vec<u8>, message: Vec<u8> },
    /// Forward sharded pub/sub message to the slot-owning node.
    PubSubForward { channel: Vec<u8>, message: Vec<u8> },
    /// Lightweight health probe for failover scoring.
    HealthProbe,
}

/// Wire envelope for cluster RPC requests.
///
/// One outer discriminant selects the owning handler; the payload is that
/// handler's exhaustive subset. Serialized/parsed as a single type per frame,
/// exactly as before — the nested design keeps the "one frame decodes to one
/// type" wire constraint while making the Raft-vs-bus split compiler-enforced.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterRpcRequest {
    /// An RPC for the Raft handler (consensus + write-forward).
    Raft(RaftRpc),
    /// An RPC serviced locally by the cluster bus.
    Bus(BusRpc),
}

impl From<RaftRpc> for ClusterRpcRequest {
    fn from(rpc: RaftRpc) -> Self {
        ClusterRpcRequest::Raft(rpc)
    }
}

impl From<BusRpc> for ClusterRpcRequest {
    fn from(rpc: BusRpc) -> Self {
        ClusterRpcRequest::Bus(rpc)
    }
}

/// RPC response types for cluster communication.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClusterRpcResponse {
    /// Response to AppendEntries.
    AppendEntries(AppendEntriesResponse<NodeId>),
    /// Response to Vote.
    Vote(VoteResponse<NodeId>),
    /// Response to InstallSnapshot.
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
    /// Response to ForwardedWrite.
    ForwardedWrite(Result<(), String>),
    /// Response to PubSubBroadcast.
    PubSubBroadcastResult { subscriber_count: usize },
    /// Response to PubSubForward.
    PubSubForwardResult { subscriber_count: usize },
    /// Response to HealthProbe.
    HealthProbeResponse {
        node_id: NodeId,
        replication_offset: u64,
    },
    /// Error response.
    Error(String),
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/// Per-peer connection slot: an async mutex guarding an optional framed stream.
type PeerConnection = tokio::sync::Mutex<Option<FramedStream>>;

/// Pool of persistent TCP connections to cluster peers.
///
/// Each peer gets at most one connection. Connections are lazily established
/// and automatically cleared on I/O errors so the next RPC reconnects.
#[derive(Default)]
struct ConnectionPool {
    connections: RwLock<BTreeMap<NodeId, Arc<PeerConnection>>>,
}

impl ConnectionPool {
    /// Get or create the connection slot for a peer.
    fn slot(&self, node_id: NodeId) -> Arc<PeerConnection> {
        // Fast path: read lock
        {
            let conns = self.connections.read();
            if let Some(slot) = conns.get(&node_id) {
                return Arc::clone(slot);
            }
        }
        // Slow path: write lock to insert
        let mut conns = self.connections.write();
        Arc::clone(
            conns
                .entry(node_id)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None))),
        )
    }

    /// Remove a peer's connection slot (e.g. when a node is removed).
    fn remove(&self, node_id: NodeId) {
        self.connections.write().remove(&node_id);
    }
}

// ---------------------------------------------------------------------------
// Network factory
// ---------------------------------------------------------------------------

/// Factory for creating network connections to cluster nodes.
#[derive(Clone)]
pub struct ClusterNetworkFactory {
    /// Known node addresses.
    node_addrs: Arc<RwLock<BTreeMap<NodeId, SocketAddr>>>,
    /// Persistent connection pool.
    pool: Arc<ConnectionPool>,
    /// Connection timeout in milliseconds.
    connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    request_timeout_ms: u64,
    /// Factory for creating connections to peers.
    connect_factory: ConnectFactory,
    /// Bus packet counters shared with every peer handle this factory makes,
    /// and with the inbound bus loop (see [`ClusterNetworkFactory::bus_stats`]).
    bus_stats: Arc<ClusterBusStats>,
}

impl std::fmt::Debug for ClusterNetworkFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterNetworkFactory")
            .field("node_addrs", &self.node_addrs)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .finish()
    }
}

impl ClusterNetworkFactory {
    /// Create a new network factory.
    pub fn new() -> Self {
        Self::with_timeouts(5000, 10000)
    }

    /// Create a network factory with custom timeouts.
    pub fn with_timeouts(connect_timeout_ms: u64, request_timeout_ms: u64) -> Self {
        Self {
            node_addrs: Arc::new(RwLock::new(BTreeMap::new())),
            pool: Arc::new(ConnectionPool::default()),
            connect_timeout_ms,
            request_timeout_ms,
            connect_factory: plain_tcp_connect_factory(connect_timeout_ms),
            bus_stats: Arc::new(ClusterBusStats::new()),
        }
    }

    /// The node-wide cluster-bus packet counters.
    ///
    /// Handed to the inbound bus loop at startup so both directions accumulate
    /// into the one pair `CLUSTER INFO` reports.
    pub fn bus_stats(&self) -> &Arc<ClusterBusStats> {
        &self.bus_stats
    }

    /// Set a custom connection factory (e.g. for TLS connections).
    pub fn set_connect_factory(&mut self, factory: ConnectFactory) {
        self.connect_factory = factory;
    }

    /// Register a node's address.
    pub fn register_node(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addrs.write().insert(node_id, addr);
    }

    /// Remove a node's address.
    pub fn remove_node(&self, node_id: NodeId) {
        self.node_addrs.write().remove(&node_id);
        self.pool.remove(node_id);
    }

    /// Get a node's address.
    pub fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.node_addrs.read().get(&node_id).copied()
    }

    /// Get all known node addresses.
    pub fn get_all_nodes(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.node_addrs.read().clone()
    }

    /// Create a pool-aware `ClusterNetwork` handle for a peer.
    pub fn connect(&self, target: NodeId, addr: SocketAddr) -> ClusterNetwork {
        ClusterNetwork {
            _target: target,
            addr,
            pool: Some(Arc::clone(&self.pool)),
            _connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            connect_factory: Arc::clone(&self.connect_factory),
            bus_stats: Arc::clone(&self.bus_stats),
        }
    }
}

impl Default for ClusterNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<TypeConfig> for ClusterNetworkFactory {
    type Network = ClusterNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        // Try to get address from our registry first, fall back to node.addr
        let addr = self.get_node_addr(target).unwrap_or_else(|| {
            node.addr
                .parse()
                .unwrap_or_else(|_| "127.0.0.1:16379".parse().unwrap())
        });

        // Raft RPCs use one-shot connections for now. OpenRaft manages its
        // own retry/reconnect logic and the interaction between pooled
        // connections and Raft's client lifecycle is subtle. Non-Raft paths
        // (pub/sub, health probes, forwarding) use the pool via connect().
        ClusterNetwork {
            _target: target,
            addr,
            pool: None,
            _connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            connect_factory: Arc::clone(&self.connect_factory),
            bus_stats: Arc::clone(&self.bus_stats),
        }
    }
}

// ---------------------------------------------------------------------------
// Network client
// ---------------------------------------------------------------------------

/// Network connection to a specific cluster node.
#[derive(Clone)]
pub struct ClusterNetwork {
    /// Target node ID.
    _target: NodeId,
    /// Target node address.
    addr: SocketAddr,
    /// Connection pool (None for pool-less bootstrap connections).
    pool: Option<Arc<ConnectionPool>>,
    /// Connection timeout in milliseconds.
    _connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    request_timeout_ms: u64,
    /// Factory for creating connections.
    connect_factory: ConnectFactory,
    /// The node-wide bus packet counters this handle reports into.
    bus_stats: Arc<ClusterBusStats>,
}

impl std::fmt::Debug for ClusterNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterNetwork")
            .field("target", &self._target)
            .field("addr", &self.addr)
            .field("pooled", &self.pool.is_some())
            .finish()
    }
}

impl ClusterNetwork {
    /// Create a new network connection without connection pooling.
    ///
    /// Use `ClusterNetworkFactory::connect()` for pooled connections.
    /// This constructor is for early bootstrap, before the factory is
    /// fully initialized.
    pub fn new(target: NodeId, addr: SocketAddr) -> Self {
        Self {
            _target: target,
            addr,
            pool: None,
            _connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            connect_factory: plain_tcp_connect_factory(5000),
            // A bootstrap handle predates the factory, so it counts into its
            // own pair rather than the node's. Reachable through
            // [`Self::bus_stats`] so the counting itself stays observable.
            bus_stats: Arc::new(ClusterBusStats::new()),
        }
    }

    /// The bus packet counters this handle reports into.
    pub fn bus_stats(&self) -> &Arc<ClusterBusStats> {
        &self.bus_stats
    }

    /// Send a lightweight health probe to query a node's replication offset.
    pub async fn health_probe(&self) -> Result<(NodeId, u64), ClusterError> {
        let request = BusRpc::HealthProbe.into();
        match self.send_rpc(request).await? {
            ClusterRpcResponse::HealthProbeResponse {
                node_id,
                replication_offset,
            } => Ok((node_id, replication_offset)),
            _ => Err(ClusterError::NetworkError(
                "unexpected response type for health probe".to_string(),
            )),
        }
    }

    /// Forward a write command to a remote node (typically the Raft leader).
    pub async fn forward_write(&self, cmd: ClusterCommand) -> Result<(), ClusterError> {
        let request = RaftRpc::ForwardedWrite(cmd).into();
        match self.send_rpc(request).await? {
            ClusterRpcResponse::ForwardedWrite(Ok(())) => Ok(()),
            ClusterRpcResponse::ForwardedWrite(Err(msg)) => Err(ClusterError::NetworkError(
                format!("forwarded write failed: {}", msg),
            )),
            _ => Err(ClusterError::NetworkError(
                "unexpected response type for forwarded write".to_string(),
            )),
        }
    }

    /// Send an RPC request and receive the response.
    pub async fn send_rpc(
        &self,
        request: ClusterRpcRequest,
    ) -> Result<ClusterRpcResponse, ClusterError> {
        let request_bytes = postcard::to_allocvec(&request)
            .map_err(|e| ClusterError::NetworkError(format!("serialization failed: {}", e)))?;

        let request_timeout = std::time::Duration::from_millis(self.request_timeout_ms);

        if let Some(pool) = &self.pool {
            self.send_rpc_pooled(pool, request_bytes, request_timeout)
                .await
        } else {
            self.send_rpc_oneshot(request_bytes, request_timeout).await
        }
    }

    /// Send RPC over a pooled connection, reconnecting on failure.
    ///
    /// Takes the connection out of the pool before I/O so the mutex is not
    /// held during potentially slow network operations. If the cached
    /// connection is stale (peer restarted), the first attempt fails fast
    /// (500ms cap) and we reconnect automatically.
    async fn send_rpc_pooled(
        &self,
        pool: &ConnectionPool,
        request_bytes: Vec<u8>,
        timeout: std::time::Duration,
    ) -> Result<ClusterRpcResponse, ClusterError> {
        let slot = pool.slot(self._target);

        // Take the cached connection (if any) without holding the lock during I/O.
        let cached = { slot.lock().await.take() };

        if let Some(mut framed) = cached {
            // Cap timeout on cached connections to detect stale ones quickly.
            let stale_timeout = timeout.min(std::time::Duration::from_millis(500));
            match Self::try_send_on_framed(
                &mut framed,
                &request_bytes,
                stale_timeout,
                &self.bus_stats,
            )
            .await
            {
                Ok(response) => {
                    *slot.lock().await = Some(framed);
                    return Ok(response);
                }
                Err(_) => {
                    // Stale connection — drop it, fall through to reconnect
                }
            }
        }

        // Open a fresh connection
        let mut framed = self.open_framed_connection().await?;

        match Self::try_send_on_framed(&mut framed, &request_bytes, timeout, &self.bus_stats).await
        {
            Ok(response) => {
                *slot.lock().await = Some(framed);
                Ok(response)
            }
            Err(e) => Err(e),
        }
    }

    /// Send RPC over a fresh one-shot connection (no pooling).
    async fn send_rpc_oneshot(
        &self,
        request_bytes: Vec<u8>,
        timeout: std::time::Duration,
    ) -> Result<ClusterRpcResponse, ClusterError> {
        let mut framed = self.open_framed_connection().await?;

        let result =
            Self::try_send_on_framed(&mut framed, &request_bytes, timeout, &self.bus_stats).await;

        match result {
            Ok(r) => Ok(r),
            Err(e) => Err(e),
        }
    }

    /// Attempt to send a serialized request and read the response on a framed stream.
    ///
    /// Both directions are counted here, where the frame actually crosses the
    /// wire: a request the peer never accepted, and a response that never
    /// arrived, are not bus traffic and must not be reported as any.
    async fn try_send_on_framed(
        framed: &mut FramedStream,
        request_bytes: &[u8],
        timeout: std::time::Duration,
        bus_stats: &ClusterBusStats,
    ) -> Result<ClusterRpcResponse, ClusterError> {
        let result = tokio::time::timeout(timeout, async {
            framed
                .send(Bytes::copy_from_slice(request_bytes))
                .await
                .map_err(|e| {
                    ClusterError::NetworkError(format!("failed to send request: {}", e))
                })?;
            bus_stats.record_sent();

            let response_frame = framed
                .next()
                .await
                .ok_or_else(|| ClusterError::NetworkError("connection closed".to_string()))?
                .map_err(|e| {
                    ClusterError::NetworkError(format!("failed to read response: {}", e))
                })?;
            bus_stats.record_received();

            postcard::from_bytes(&response_frame)
                .map_err(|e| ClusterError::NetworkError(format!("deserialization failed: {}", e)))
        })
        .await;

        match result {
            Ok(r) => r,
            Err(_) => Err(ClusterError::NetworkError("request timeout".to_string())),
        }
    }

    /// Open a new connection and wrap it in a length-delimited frame codec.
    async fn open_framed_connection(&self) -> Result<FramedStream, ClusterError> {
        let stream = (self.connect_factory)(self.addr)
            .await
            .map_err(|e| ClusterError::NetworkError(format!("connection failed: {}", e)))?;

        Ok(new_framed(stream))
    }
}

impl RaftNetwork<TypeConfig> for ClusterNetwork {
    fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            AppendEntriesResponse<NodeId>,
            RPCError<NodeId, BasicNode, RaftError<NodeId>>,
        >,
    > + Send {
        let request = RaftRpc::AppendEntries(req).into();
        let this = self.clone();

        async move {
            match this.send_rpc(request).await {
                Ok(ClusterRpcResponse::AppendEntries(resp)) => Ok(resp),
                Ok(ClusterRpcResponse::Error(msg)) => Err(RPCError::Network(NetworkError::new(
                    &Unreachable::new(&NetworkErrorWrapper(msg)),
                ))),
                Ok(_) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper("unexpected response type".to_string()),
                )))),
                Err(e) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper(e.to_string()),
                )))),
            }
        }
    }

    fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>,
    > + Send {
        let request = RaftRpc::Vote(req).into();
        let this = self.clone();

        async move {
            match this.send_rpc(request).await {
                Ok(ClusterRpcResponse::Vote(resp)) => Ok(resp),
                Ok(ClusterRpcResponse::Error(msg)) => Err(RPCError::Network(NetworkError::new(
                    &Unreachable::new(&NetworkErrorWrapper(msg)),
                ))),
                Ok(_) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper("unexpected response type".to_string()),
                )))),
                Err(e) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper(e.to_string()),
                )))),
            }
        }
    }

    fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            InstallSnapshotResponse<NodeId>,
            RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
        >,
    > + Send {
        let request = RaftRpc::InstallSnapshot(req).into();
        let this = self.clone();

        async move {
            match this.send_rpc(request).await {
                Ok(ClusterRpcResponse::InstallSnapshot(resp)) => Ok(resp),
                Ok(ClusterRpcResponse::Error(msg)) => Err(RPCError::Network(NetworkError::new(
                    &Unreachable::new(&NetworkErrorWrapper(msg)),
                ))),
                Ok(_) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper("unexpected response type".to_string()),
                )))),
                Err(e) => Err(RPCError::Network(NetworkError::new(&Unreachable::new(
                    &NetworkErrorWrapper(e.to_string()),
                )))),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Server-side helpers (used by cluster_bus)
// ---------------------------------------------------------------------------

/// Add a node to the Raft voter set (learner first, then promote).
///
/// Must be called on the leader. Spawns a background task so the caller
/// (client response path) is not blocked. Skips nodes that are already
/// Raft voters (e.g. initial bootstrap members whose self-registration
/// AddNode arrives via ForwardedWrite).
///
/// The learner-add and voter-promotion steps are retried with backoff:
/// a node that is visible in `ClusterState` (via the committed `AddNode`
/// DATA command) but missing from the Raft voter set silently weakens the
/// cluster's fault tolerance, so a transient error here must not be
/// terminal. Failures after all retries are logged at `error` level.
/// A full reconciliation loop (ClusterState nodes vs. Raft voters) is a
/// known follow-up, not yet implemented.
/// How long [`spawn_add_raft_voter`] waits before retrying the attempt that just
/// failed, or `None` when that attempt was the last one.
///
/// Split out of the retry loop so the schedule is checkable without a live Raft
/// and without sleeping: `attempt` is 1-based, so attempt `max_attempts` is
/// terminal, and the backoff grows linearly with the attempt number.
fn voter_retry_delay(attempt: u32, max_attempts: u32) -> Option<std::time::Duration> {
    (attempt < max_attempts).then(|| std::time::Duration::from_millis(500 * attempt as u64))
}

pub fn spawn_add_raft_voter(raft: crate::ClusterRaft, node_id: NodeId, addr: std::net::SocketAddr) {
    const MAX_ATTEMPTS: u32 = 5;

    tokio::spawn(async move {
        for attempt in 1..=MAX_ATTEMPTS {
            // Skip if the node is already a Raft voter (initial bootstrap
            // members, or a prior attempt that succeeded). Re-adding one is not
            // free: `add_learner` proposes a fresh membership entry and then
            // blocks until the peer is caught up. Re-checked on every attempt.
            {
                let membership = raft.metrics().borrow().membership_config.clone();
                if membership.membership().voter_ids().any(|id| id == node_id) {
                    return;
                }
            }

            let node = BasicNode {
                addr: addr.to_string(),
            };
            let result = match raft.add_learner(node_id, node, true).await {
                Ok(_) => {
                    let members =
                        ChangeMembers::AddVoterIds(std::collections::BTreeSet::from([node_id]));
                    raft.change_membership(members, false)
                        .await
                        .map(|_| ())
                        .map_err(|e| ("promote Raft learner to voter", e.to_string()))
                }
                Err(e) => Err(("add Raft learner", e.to_string())),
            };

            match result {
                Ok(()) => {
                    tracing::info!(node_id, %addr, "Added node to Raft voter set");
                    return;
                }
                Err((step, e)) => match voter_retry_delay(attempt, MAX_ATTEMPTS) {
                    Some(delay) => {
                        tracing::warn!(node_id, attempt, error = %e, "Failed to {step}; retrying");
                        tokio::time::sleep(delay).await;
                    }
                    None => {
                        tracing::error!(
                            node_id,
                            %addr,
                            error = %e,
                            "Failed to {step} after {MAX_ATTEMPTS} attempts; \
                             node is in cluster state but NOT a Raft voter"
                        );
                    }
                },
            }
        }
    });
}

/// Handle incoming Raft-handle RPC requests from other cluster nodes.
///
/// The parameter is [`RaftRpc`] — the subset serviced through the
/// [`ClusterRaft`](crate::ClusterRaft) handle — so this match is exhaustive by
/// construction: it cannot name (nor mis-route) a bus-local RPC.
pub async fn handle_rpc_request(raft: &crate::ClusterRaft, request: RaftRpc) -> ClusterRpcResponse {
    match request {
        RaftRpc::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => ClusterRpcResponse::AppendEntries(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        RaftRpc::Vote(req) => match raft.vote(req).await {
            Ok(resp) => ClusterRpcResponse::Vote(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        RaftRpc::InstallSnapshot(req) => match raft.install_snapshot(req).await {
            Ok(resp) => ClusterRpcResponse::InstallSnapshot(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        RaftRpc::ForwardedWrite(cmd) => {
            // Extract AddNode info before the command is consumed by client_write
            let add_node_info = if let ClusterCommand::AddNode { ref node } = cmd {
                Some((node.id, node.cluster_addr))
            } else {
                None
            };

            match raft.client_write(cmd).await {
                // The Raft write can commit while the state machine rejects the
                // command; surface that as an error instead of a silent OK.
                Ok(resp) => {
                    if let ClusterResponse::Error(e) = resp.data {
                        // ForwardedWrite is a cross-node RPC contract that stays
                        // Result<(), String> (out of scope for proposal 32), so
                        // the typed error is re-flattened to its display string
                        // here — the only site that intentionally does so.
                        return ClusterRpcResponse::ForwardedWrite(Err(e.to_string()));
                    }
                    if let Some((node_id, cluster_addr)) = add_node_info {
                        spawn_add_raft_voter(raft.clone(), node_id, cluster_addr);
                    }
                    ClusterRpcResponse::ForwardedWrite(Ok(()))
                }
                Err(e) => ClusterRpcResponse::ForwardedWrite(Err(e.to_string())),
            }
        }
    }
}

/// Create a new `FramedStream` from a type-erased I/O stream.
pub fn new_framed(stream: BoxedStream) -> FramedStream {
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_SIZE)
        .new_codec();
    Framed::new(stream, codec)
}

/// Create a new `FramedStream` from a raw `TcpStream`.
pub fn new_framed_tcp(stream: TcpStream) -> FramedStream {
    new_framed(Box::new(stream))
}

/// Parse an incoming message from a cluster bus connection.
pub async fn parse_rpc_message(
    stream: &mut FramedStream,
    bus_stats: &ClusterBusStats,
) -> Result<ClusterRpcRequest, ClusterError> {
    let frame = stream
        .next()
        .await
        .ok_or_else(|| ClusterError::NetworkError("connection closed".to_string()))?
        .map_err(|e| ClusterError::NetworkError(format!("failed to read message: {}", e)))?;
    // A frame arrived, so it is bus traffic whether or not it decodes.
    bus_stats.record_received();

    postcard::from_bytes(&frame)
        .map_err(|e| ClusterError::NetworkError(format!("deserialization failed: {}", e)))
}

/// Send an RPC response over a cluster bus connection.
pub async fn send_rpc_response(
    stream: &mut FramedStream,
    response: ClusterRpcResponse,
    bus_stats: &ClusterBusStats,
) -> Result<(), ClusterError> {
    let response_bytes = postcard::to_allocvec(&response)
        .map_err(|e| ClusterError::NetworkError(format!("serialization failed: {}", e)))?;

    stream
        .send(Bytes::from(response_bytes))
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to send response: {}", e)))?;
    bus_stats.record_sent();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::ClusterBusStatsSnapshot;

    // FM-CLUSTER-051
    #[test]
    fn test_network_factory_node_registration() {
        let factory = ClusterNetworkFactory::new();

        let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        factory.register_node(1, addr);

        assert_eq!(factory.get_node_addr(1), Some(addr));
        assert_eq!(factory.get_node_addr(2), None);

        factory.remove_node(1);
        assert_eq!(factory.get_node_addr(1), None);
    }

    // FM-CLUSTER-051
    #[test]
    fn test_rpc_request_serialization() {
        // Test that our RPC types can be serialized/deserialized with postcard
        let request: ClusterRpcRequest = RaftRpc::Vote(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        })
        .into();

        let bytes = postcard::to_allocvec(&request).unwrap();
        let _: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
    }

    // FM-CLUSTER-051
    #[test]
    fn test_from_shims_wrap_correct_arm() {
        // The `From` shims must place each subset on the matching envelope arm.
        let raft: ClusterRpcRequest = RaftRpc::Vote(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        })
        .into();
        assert!(matches!(raft, ClusterRpcRequest::Raft(RaftRpc::Vote(_))));

        let bus: ClusterRpcRequest = BusRpc::HealthProbe.into();
        assert!(matches!(bus, ClusterRpcRequest::Bus(BusRpc::HealthProbe)));
    }

    // FM-CLUSTER-051
    #[test]
    fn test_all_rpc_variants_roundtrip() {
        use crate::types::NodeInfo;

        // AppendEntries (empty)
        let req: ClusterRpcRequest = RaftRpc::AppendEntries(AppendEntriesRequest {
            vote: openraft::Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        })
        .into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Raft(RaftRpc::AppendEntries(_))
        ));

        // InstallSnapshot
        let req: ClusterRpcRequest = RaftRpc::InstallSnapshot(InstallSnapshotRequest {
            vote: openraft::Vote::new(1, 1),
            meta: openraft::SnapshotMeta {
                last_log_id: None,
                last_membership: openraft::StoredMembership::new(
                    None,
                    openraft::Membership::new(vec![std::collections::BTreeSet::new()], None),
                ),
                snapshot_id: "snap-1".to_string(),
            },
            offset: 0,
            data: vec![1, 2, 3],
            done: true,
        })
        .into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Raft(RaftRpc::InstallSnapshot(_))
        ));

        // ForwardedWrite
        let node = NodeInfo::new_primary(
            1,
            "127.0.0.1:6379".parse().unwrap(),
            "127.0.0.1:16379".parse().unwrap(),
        );
        let req: ClusterRpcRequest =
            RaftRpc::ForwardedWrite(ClusterCommand::AddNode { node }).into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Raft(RaftRpc::ForwardedWrite(_))
        ));

        // HealthProbe
        let req: ClusterRpcRequest = BusRpc::HealthProbe.into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Bus(BusRpc::HealthProbe)
        ));

        // PubSubBroadcast
        let req: ClusterRpcRequest = BusRpc::PubSubBroadcast {
            channel: b"test".to_vec(),
            message: b"hello".to_vec(),
        }
        .into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Bus(BusRpc::PubSubBroadcast { .. })
        ));

        // PubSubForward
        let req: ClusterRpcRequest = BusRpc::PubSubForward {
            channel: b"test".to_vec(),
            message: b"hello".to_vec(),
        }
        .into();
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcRequest::Bus(BusRpc::PubSubForward { .. })
        ));

        // Responses
        let resp = ClusterRpcResponse::HealthProbeResponse {
            node_id: 42,
            replication_offset: 1000,
        };
        let bytes = postcard::to_allocvec(&resp).unwrap();
        let decoded: ClusterRpcResponse = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(
            decoded,
            ClusterRpcResponse::HealthProbeResponse { .. }
        ));
    }

    // ---- Cluster-bus packet counters (CLUSTER INFO) ------------------------

    /// A connect factory that hands out one in-memory stream, so a peer can be
    /// played by a task instead of a socket.
    fn duplex_connect_factory(client_end: tokio::io::DuplexStream) -> ConnectFactory {
        let slot = Arc::new(parking_lot::Mutex::new(Some(client_end)));
        Arc::new(move |_addr| {
            let taken = slot.lock().take();
            Box::pin(async move {
                match taken {
                    Some(stream) => Ok(Box::new(stream) as BoxedStream),
                    None => Err(io::Error::other("connect factory exhausted")),
                }
            })
        })
    }

    /// The peer half of a bus connection: read one request, answer it.
    async fn serve_one_health_probe(server_end: tokio::io::DuplexStream) {
        let mut framed = new_framed(Box::new(server_end));
        let stats = ClusterBusStats::new();
        let request = parse_rpc_message(&mut framed, &stats).await.unwrap();
        assert!(matches!(
            request,
            ClusterRpcRequest::Bus(BusRpc::HealthProbe)
        ));
        send_rpc_response(
            &mut framed,
            ClusterRpcResponse::HealthProbeResponse {
                node_id: 7,
                replication_offset: 99,
            },
            &stats,
        )
        .await
        .unwrap();
    }

    // FM-CLUSTER-077
    #[tokio::test]
    async fn cluster_stats_messages_sent_grows_with_bus_traffic() {
        let (client_end, server_end) = tokio::io::duplex(64 * 1024);
        let peer = tokio::spawn(serve_one_health_probe(server_end));

        let mut factory = ClusterNetworkFactory::new();
        factory.set_connect_factory(duplex_connect_factory(client_end));
        assert_eq!(
            factory.bus_stats().snapshot(),
            ClusterBusStatsSnapshot::default(),
            "a node that has not spoken to anyone reports no traffic"
        );

        let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        let probe = factory.connect(1, addr).health_probe().await.unwrap();
        assert_eq!(probe, (7, 99));
        peer.await.unwrap();

        // One request out, one response back — and the peer handle counts into
        // the same node-wide pair the factory exposes.
        assert_eq!(
            factory.bus_stats().snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 1,
                messages_received: 1
            }
        );
    }

    /// The receiving side counts the request it reads and the response it
    /// writes, using the same counter pair as the outbound direction.
    // FM-CLUSTER-077
    #[tokio::test]
    async fn cluster_stats_messages_received_grows_on_the_receiving_node() {
        let (client_end, server_end) = tokio::io::duplex(64 * 1024);
        let stats = Arc::new(ClusterBusStats::new());

        let client = tokio::spawn(async move {
            let mut framed = new_framed(Box::new(client_end));
            let request: ClusterRpcRequest = BusRpc::HealthProbe.into();
            framed
                .send(Bytes::from(postcard::to_allocvec(&request).unwrap()))
                .await
                .unwrap();
            framed.next().await.unwrap().unwrap();
        });

        let mut framed = new_framed(Box::new(server_end));
        parse_rpc_message(&mut framed, &stats).await.unwrap();
        assert_eq!(
            stats.snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 0,
                messages_received: 1
            },
            "reading a request is inbound traffic only"
        );

        send_rpc_response(
            &mut framed,
            ClusterRpcResponse::HealthProbeResponse {
                node_id: 7,
                replication_offset: 99,
            },
            &stats,
        )
        .await
        .unwrap();
        assert_eq!(
            stats.snapshot(),
            ClusterBusStatsSnapshot {
                messages_sent: 1,
                messages_received: 1
            }
        );

        client.await.unwrap();
    }

    // ---- Connection pool ---------------------------------------------------

    /// The pool's whole purpose is that a peer's slot is *the same* slot every
    /// time — a fresh slot per call would mean a fresh connection per RPC — and
    /// that removing a node drops the slot it cached, so a node that comes back
    /// on the same id is not answered over the dead peer's socket.
    // FM-CLUSTER-051
    #[tokio::test]
    async fn the_pool_keeps_one_slot_per_peer_until_the_peer_is_removed() {
        let pool = ConnectionPool::default();

        let first = pool.slot(1);
        assert!(
            Arc::ptr_eq(&first, &pool.slot(1)),
            "the second caller must get the slot the first one created"
        );
        assert!(
            !Arc::ptr_eq(&first, &pool.slot(2)),
            "different peers never share a slot"
        );

        // A slot carries state: what is cached in it must still be there.
        *first.lock().await = None;
        assert_eq!(pool.connections.read().len(), 2);

        pool.remove(1);
        assert_eq!(pool.connections.read().len(), 1, "the slot is dropped");
        assert!(
            !Arc::ptr_eq(&first, &pool.slot(1)),
            "a re-registered peer gets a fresh slot, not the dead one"
        );
    }

    /// The factory's address book is what `CLUSTER MEET`/`FORGET` and the Raft
    /// network both read; reporting an empty one would strand every peer.
    // FM-CLUSTER-051
    #[test]
    fn the_factory_reports_every_registered_address() {
        let factory = ClusterNetworkFactory::new();
        assert!(factory.get_all_nodes().is_empty());

        let one: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        let two: SocketAddr = "127.0.0.1:16380".parse().unwrap();
        factory.register_node(1, one);
        factory.register_node(2, two);
        assert_eq!(
            factory.get_all_nodes(),
            BTreeMap::from([(1, one), (2, two)])
        );

        factory.remove_node(1);
        assert_eq!(factory.get_all_nodes(), BTreeMap::from([(2, two)]));
    }

    /// Every peer handle a factory makes reports into the factory's counters,
    /// not into a private pair of its own — `CLUSTER INFO` reports one node-wide
    /// number.
    // FM-CLUSTER-077
    #[test]
    fn a_peer_handle_shares_the_factorys_counters() {
        let factory = ClusterNetworkFactory::new();
        let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        let peer = factory.connect(1, addr);
        assert!(
            Arc::ptr_eq(peer.bus_stats(), factory.bus_stats()),
            "the handle must count into the node-wide pair"
        );

        // A bootstrap handle predates the factory and owns its own pair, which
        // is still reachable rather than fabricated per call.
        let bootstrap = ClusterNetwork::new(1, addr);
        assert!(Arc::ptr_eq(bootstrap.bus_stats(), bootstrap.bus_stats()));
        assert!(!Arc::ptr_eq(bootstrap.bus_stats(), factory.bus_stats()));
    }

    // ---- Forwarded writes --------------------------------------------------

    /// The peer half of a forwarded write: read the request, answer with the
    /// caller's chosen response.
    async fn serve_one_forwarded_write(
        server_end: tokio::io::DuplexStream,
        response: ClusterRpcResponse,
    ) {
        let mut framed = new_framed(Box::new(server_end));
        let stats = ClusterBusStats::new();
        let request = parse_rpc_message(&mut framed, &stats).await.unwrap();
        assert!(matches!(
            request,
            ClusterRpcRequest::Raft(RaftRpc::ForwardedWrite(_))
        ));
        send_rpc_response(&mut framed, response, &stats)
            .await
            .unwrap();
    }

    /// Forward one write to a peer that answers with `response`.
    async fn forward_write_answered_with(response: ClusterRpcResponse) -> Result<(), ClusterError> {
        let (client_end, server_end) = tokio::io::duplex(64 * 1024);
        let peer = tokio::spawn(serve_one_forwarded_write(server_end, response));

        let mut factory = ClusterNetworkFactory::new();
        factory.set_connect_factory(duplex_connect_factory(client_end));
        let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        let result = factory
            .connect(1, addr)
            .forward_write(ClusterCommand::IncrementEpoch)
            .await;
        peer.await.unwrap();
        result
    }

    /// A forwarded write reports the leader's verdict verbatim: a remote commit
    /// is a local success, a remote rejection carries its reason, and an answer
    /// to some other question is an error rather than a silent success — the
    /// follower answers its client on the strength of this result.
    // FM-CLUSTER-048
    #[tokio::test]
    async fn a_forwarded_write_reports_the_leaders_verdict() {
        forward_write_answered_with(ClusterRpcResponse::ForwardedWrite(Ok(())))
            .await
            .expect("a remote commit is a local success");

        let rejected = forward_write_answered_with(ClusterRpcResponse::ForwardedWrite(Err(
            "slot not owned".to_string(),
        )))
        .await
        .expect_err("a remote rejection is not a success");
        let rendered = rejected.to_string();
        assert!(
            rendered.contains("forwarded write failed") && rendered.contains("slot not owned"),
            "the leader's reason must survive the trip back: {rendered}"
        );

        let confused = forward_write_answered_with(ClusterRpcResponse::HealthProbeResponse {
            node_id: 7,
            replication_offset: 99,
        })
        .await
        .expect_err("an answer to another question is not a commit");
        assert!(
            confused.to_string().contains("unexpected response type"),
            "got {confused}"
        );
    }

    // ---- Raft voter promotion ----------------------------------------------

    /// The retry schedule: every attempt but the last earns a linearly growing
    /// backoff. Retrying past the last attempt would loop forever on a peer that
    /// is never coming back; stopping before it turns one transient error into a
    /// node that is in the cluster state but not in the voter set.
    // FM-CLUSTER-051
    #[test]
    fn the_voter_retry_schedule_backs_off_and_then_stops() {
        use std::time::Duration;

        assert_eq!(voter_retry_delay(1, 5), Some(Duration::from_millis(500)));
        assert_eq!(voter_retry_delay(2, 5), Some(Duration::from_millis(1000)));
        assert_eq!(voter_retry_delay(4, 5), Some(Duration::from_millis(2000)));
        assert_eq!(
            voter_retry_delay(5, 5),
            None,
            "the last attempt is terminal, not another retry"
        );
        assert_eq!(voter_retry_delay(6, 5), None);
    }

    /// A node that is in the cluster state but not in the Raft voter set weakens
    /// fault tolerance silently, so the promotion really has to run — and it
    /// must skip nodes that already *are* voters, because re-adding one costs a
    /// redundant membership entry and a blocking catch-up wait.
    // FM-CLUSTER-051
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member() {
        use std::collections::BTreeMap as Map;

        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::ClusterStorage::open(dir.path()).unwrap();
        let raft = openraft::Raft::new(
            1,
            Arc::new(openraft::Config {
                election_timeout_min: 100,
                election_timeout_max: 200,
                heartbeat_interval: 50,
                ..Default::default()
            }),
            ClusterNetworkFactory::with_timeouts(50, 50),
            storage,
            crate::state::ClusterStateMachine::new(),
        )
        .await
        .expect("a single-node Raft must start");

        let self_addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        raft.initialize(Map::from([(
            1u64,
            BasicNode {
                addr: self_addr.to_string(),
            },
        )]))
        .await
        .expect("bootstrapping one voter must succeed");

        let voters = |raft: &crate::ClusterRaft| {
            raft.metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .collect::<Vec<_>>()
        };
        let knows = |raft: &crate::ClusterRaft, id: NodeId| {
            raft.metrics()
                .borrow()
                .membership_config
                .membership()
                .nodes()
                .any(|(known, _)| *known == id)
        };
        // The metrics watch is updated asynchronously, so settle on the
        // bootstrap membership before judging what the promotion did to it.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while voters(&raft) != vec![1] {
            assert!(
                std::time::Instant::now() < deadline,
                "the bootstrap voter never appeared: {:?}",
                raft.metrics().borrow().membership_config
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // An existing voter: nothing is proposed at all, so the membership entry
        // that is in force does not move and node 1 stays a voter. This phase
        // runs first, while no other add-voter saga is in flight — the stranger
        // phase below spawns a task whose learner→voter promotion can land a
        // second membership entry at any later point, which would move the log
        // id out from under this assertion for reasons unrelated to the re-add.
        let settled = *raft.metrics().borrow().membership_config.log_id();
        spawn_add_raft_voter(raft.clone(), 1, self_addr);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            raft.metrics().borrow().membership_config.log_id(),
            &settled,
            "re-adding an existing voter must propose nothing"
        );
        assert!(voters(&raft).contains(&1));

        // A stranger: the promotion has to reach Raft. `add_learner` commits the
        // membership entry before it blocks waiting for the (unreachable) peer
        // to catch up, so the node appearing in the membership is the signal.
        spawn_add_raft_voter(raft.clone(), 2, "127.0.0.1:16380".parse().unwrap());
        while !knows(&raft, 2) {
            assert!(
                std::time::Instant::now() < deadline,
                "node 2 was never proposed to Raft: {:?}",
                raft.metrics().borrow().membership_config
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        raft.shutdown().await.unwrap();
    }

    /// A frame that never crossed the wire is not traffic. Reporting the
    /// attempt would tell an operator the bus is working when it is not.
    // FM-CLUSTER-077
    #[tokio::test]
    async fn a_connection_that_never_opens_counts_nothing() {
        let mut factory = ClusterNetworkFactory::new();
        factory.set_connect_factory(Arc::new(|_addr| {
            Box::pin(async { Err(io::Error::other("peer is down")) })
        }));

        let addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        assert!(factory.connect(1, addr).health_probe().await.is_err());

        assert_eq!(
            factory.bus_stats().snapshot(),
            ClusterBusStatsSnapshot::default()
        );
    }
}
