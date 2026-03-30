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

use crate::types::{ClusterCommand, ClusterError, NodeId, TypeConfig};

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

/// Maximum frame size for cluster RPC messages (64 MiB).
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Simple error wrapper for network errors that implements std::error::Error.
#[derive(Debug)]
struct NetworkErrorWrapper(String);

impl std::fmt::Display for NetworkErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for NetworkErrorWrapper {}

/// RPC request types for cluster communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterRpcRequest {
    /// AppendEntries RPC (leader to followers).
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// Vote RPC (candidate to all nodes).
    Vote(VoteRequest<NodeId>),
    /// InstallSnapshot RPC (leader to lagging followers).
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Forwarded client write (follower to leader).
    ForwardedWrite(ClusterCommand),
    /// Broadcast pub/sub message to all nodes.
    PubSubBroadcast { channel: Vec<u8>, message: Vec<u8> },
    /// Forward sharded pub/sub message to the slot-owning node.
    PubSubForward { channel: Vec<u8>, message: Vec<u8> },
    /// Lightweight health probe for failover scoring.
    HealthProbe,
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
        let connect_timeout_ms = 5000;
        Self {
            node_addrs: Arc::new(RwLock::new(BTreeMap::new())),
            pool: Arc::new(ConnectionPool::default()),
            connect_timeout_ms,
            request_timeout_ms: 10000,
            connect_factory: plain_tcp_connect_factory(connect_timeout_ms),
        }
    }

    /// Create a network factory with custom timeouts.
    pub fn with_timeouts(connect_timeout_ms: u64, request_timeout_ms: u64) -> Self {
        Self {
            node_addrs: Arc::new(RwLock::new(BTreeMap::new())),
            pool: Arc::new(ConnectionPool::default()),
            connect_timeout_ms,
            request_timeout_ms,
            connect_factory: plain_tcp_connect_factory(connect_timeout_ms),
        }
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
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            connect_factory: Arc::clone(&self.connect_factory),
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
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            connect_factory: Arc::clone(&self.connect_factory),
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
    connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    request_timeout_ms: u64,
    /// Factory for creating connections.
    connect_factory: ConnectFactory,
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
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
            connect_factory: plain_tcp_connect_factory(5000),
        }
    }

    /// Send a lightweight health probe to query a node's replication offset.
    pub async fn health_probe(&self) -> Result<(NodeId, u64), ClusterError> {
        let request = ClusterRpcRequest::HealthProbe;
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
        let request = ClusterRpcRequest::ForwardedWrite(cmd);
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
            match Self::try_send_on_framed(&mut framed, &request_bytes, stale_timeout).await {
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

        match Self::try_send_on_framed(&mut framed, &request_bytes, timeout).await {
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

        let result = Self::try_send_on_framed(&mut framed, &request_bytes, timeout).await;

        match result {
            Ok(r) => Ok(r),
            Err(e) => Err(e),
        }
    }

    /// Attempt to send a serialized request and read the response on a framed stream.
    async fn try_send_on_framed(
        framed: &mut FramedStream,
        request_bytes: &[u8],
        timeout: std::time::Duration,
    ) -> Result<ClusterRpcResponse, ClusterError> {
        let result = tokio::time::timeout(timeout, async {
            framed
                .send(Bytes::copy_from_slice(request_bytes))
                .await
                .map_err(|e| {
                    ClusterError::NetworkError(format!("failed to send request: {}", e))
                })?;

            let response_frame = framed
                .next()
                .await
                .ok_or_else(|| ClusterError::NetworkError("connection closed".to_string()))?
                .map_err(|e| {
                    ClusterError::NetworkError(format!("failed to read response: {}", e))
                })?;

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
        let request = ClusterRpcRequest::AppendEntries(req);
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
        let request = ClusterRpcRequest::Vote(req);
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
        let request = ClusterRpcRequest::InstallSnapshot(req);
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

/// Handle incoming RPC requests from other cluster nodes.
pub async fn handle_rpc_request(
    raft: &crate::ClusterRaft,
    request: ClusterRpcRequest,
) -> ClusterRpcResponse {
    match request {
        ClusterRpcRequest::AppendEntries(req) => match raft.append_entries(req).await {
            Ok(resp) => ClusterRpcResponse::AppendEntries(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        ClusterRpcRequest::Vote(req) => match raft.vote(req).await {
            Ok(resp) => ClusterRpcResponse::Vote(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        ClusterRpcRequest::InstallSnapshot(req) => match raft.install_snapshot(req).await {
            Ok(resp) => ClusterRpcResponse::InstallSnapshot(resp),
            Err(e) => ClusterRpcResponse::Error(e.to_string()),
        },
        ClusterRpcRequest::ForwardedWrite(cmd) => match raft.client_write(cmd).await {
            Ok(_) => ClusterRpcResponse::ForwardedWrite(Ok(())),
            Err(e) => ClusterRpcResponse::ForwardedWrite(Err(e.to_string())),
        },
        ClusterRpcRequest::PubSubBroadcast { .. } | ClusterRpcRequest::PubSubForward { .. } => {
            ClusterRpcResponse::Error(
                "PubSub RPCs must be handled by the cluster bus, not the Raft handler".to_string(),
            )
        }
        ClusterRpcRequest::HealthProbe => {
            ClusterRpcResponse::Error("HealthProbe must be handled by cluster bus".to_string())
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
) -> Result<ClusterRpcRequest, ClusterError> {
    let frame = stream
        .next()
        .await
        .ok_or_else(|| ClusterError::NetworkError("connection closed".to_string()))?
        .map_err(|e| ClusterError::NetworkError(format!("failed to read message: {}", e)))?;

    postcard::from_bytes(&frame)
        .map_err(|e| ClusterError::NetworkError(format!("deserialization failed: {}", e)))
}

/// Send an RPC response over a cluster bus connection.
pub async fn send_rpc_response(
    stream: &mut FramedStream,
    response: ClusterRpcResponse,
) -> Result<(), ClusterError> {
    let response_bytes = postcard::to_allocvec(&response)
        .map_err(|e| ClusterError::NetworkError(format!("serialization failed: {}", e)))?;

    stream
        .send(Bytes::from(response_bytes))
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to send response: {}", e)))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_rpc_request_serialization() {
        // Test that our RPC types can be serialized/deserialized with postcard
        let request = ClusterRpcRequest::Vote(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });

        let bytes = postcard::to_allocvec(&request).unwrap();
        let _: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
    }

    #[test]
    fn test_all_rpc_variants_roundtrip() {
        use crate::types::NodeInfo;

        // AppendEntries (empty)
        let req = ClusterRpcRequest::AppendEntries(AppendEntriesRequest {
            vote: openraft::Vote::new(1, 1),
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        });
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, ClusterRpcRequest::AppendEntries(_)));

        // InstallSnapshot
        let req = ClusterRpcRequest::InstallSnapshot(InstallSnapshotRequest {
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
        });
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, ClusterRpcRequest::InstallSnapshot(_)));

        // ForwardedWrite
        let node = NodeInfo::new_primary(
            1,
            "127.0.0.1:6379".parse().unwrap(),
            "127.0.0.1:16379".parse().unwrap(),
        );
        let req = ClusterRpcRequest::ForwardedWrite(ClusterCommand::AddNode { node });
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, ClusterRpcRequest::ForwardedWrite(_)));

        // HealthProbe
        let req = ClusterRpcRequest::HealthProbe;
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, ClusterRpcRequest::HealthProbe));

        // PubSubBroadcast
        let req = ClusterRpcRequest::PubSubBroadcast {
            channel: b"test".to_vec(),
            message: b"hello".to_vec(),
        };
        let bytes = postcard::to_allocvec(&req).unwrap();
        let decoded: ClusterRpcRequest = postcard::from_bytes(&bytes).unwrap();
        assert!(matches!(decoded, ClusterRpcRequest::PubSubBroadcast { .. }));

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
}
