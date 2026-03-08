//! Raft network layer for inter-node TCP communication.
//!
//! This module provides the network implementation for openraft, enabling
//! cluster nodes to communicate via TCP for Raft consensus operations.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::types::{ClusterCommand, ClusterError, NodeId, TypeConfig};

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
    PubSubBroadcast {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    /// Forward sharded pub/sub message to the slot-owning node.
    PubSubForward {
        channel: Vec<u8>,
        message: Vec<u8>,
    },
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
    /// Error response.
    Error(String),
}

/// Factory for creating network connections to cluster nodes.
#[derive(Debug, Clone)]
pub struct ClusterNetworkFactory {
    /// Known node addresses.
    node_addrs: Arc<RwLock<BTreeMap<NodeId, SocketAddr>>>,
    /// Connection timeout in milliseconds.
    connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    request_timeout_ms: u64,
}

impl ClusterNetworkFactory {
    /// Create a new network factory.
    pub fn new() -> Self {
        Self {
            node_addrs: Arc::new(RwLock::new(BTreeMap::new())),
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
        }
    }

    /// Create a network factory with custom timeouts.
    pub fn with_timeouts(connect_timeout_ms: u64, request_timeout_ms: u64) -> Self {
        Self {
            node_addrs: Arc::new(RwLock::new(BTreeMap::new())),
            connect_timeout_ms,
            request_timeout_ms,
        }
    }

    /// Register a node's address.
    pub fn register_node(&self, node_id: NodeId, addr: SocketAddr) {
        self.node_addrs.write().insert(node_id, addr);
    }

    /// Remove a node's address.
    pub fn remove_node(&self, node_id: NodeId) {
        self.node_addrs.write().remove(&node_id);
    }

    /// Get a node's address.
    pub fn get_node_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        self.node_addrs.read().get(&node_id).copied()
    }

    /// Get all known node addresses.
    pub fn get_all_nodes(&self) -> BTreeMap<NodeId, SocketAddr> {
        self.node_addrs.read().clone()
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

        ClusterNetwork {
            _target: target,
            addr,
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
        }
    }
}

/// Network connection to a specific cluster node.
#[derive(Debug, Clone)]
pub struct ClusterNetwork {
    /// Target node ID.
    _target: NodeId,
    /// Target node address.
    addr: SocketAddr,
    /// Connection timeout in milliseconds.
    connect_timeout_ms: u64,
    /// Request timeout in milliseconds.
    request_timeout_ms: u64,
}

impl ClusterNetwork {
    /// Create a new network connection.
    pub fn new(target: NodeId, addr: SocketAddr) -> Self {
        Self {
            _target: target,
            addr,
            connect_timeout_ms: 5000,
            request_timeout_ms: 10000,
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
        // Connect to target node
        let connect_timeout = std::time::Duration::from_millis(self.connect_timeout_ms);
        let mut stream = tokio::time::timeout(connect_timeout, TcpStream::connect(self.addr))
            .await
            .map_err(|_| ClusterError::NetworkError("connection timeout".to_string()))?
            .map_err(|e| ClusterError::NetworkError(format!("connection failed: {}", e)))?;

        // Serialize request
        let request_bytes = serde_json::to_vec(&request)
            .map_err(|e| ClusterError::NetworkError(format!("serialization failed: {}", e)))?;

        // Send length-prefixed message
        let len = request_bytes.len() as u32;
        stream
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| ClusterError::NetworkError(format!("failed to write length: {}", e)))?;
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| ClusterError::NetworkError(format!("failed to write request: {}", e)))?;
        stream
            .flush()
            .await
            .map_err(|e| ClusterError::NetworkError(format!("failed to flush: {}", e)))?;

        // Receive response with timeout
        let request_timeout = std::time::Duration::from_millis(self.request_timeout_ms);
        let response = tokio::time::timeout(request_timeout, async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.map_err(|e| {
                ClusterError::NetworkError(format!("failed to read response length: {}", e))
            })?;
            let len = u32::from_be_bytes(len_buf) as usize;

            // Sanity check on length
            if len > 64 * 1024 * 1024 {
                return Err(ClusterError::NetworkError("response too large".to_string()));
            }

            // Read response body
            let mut response_bytes = vec![0u8; len];
            stream.read_exact(&mut response_bytes).await.map_err(|e| {
                ClusterError::NetworkError(format!("failed to read response: {}", e))
            })?;

            // Deserialize response
            serde_json::from_slice(&response_bytes)
                .map_err(|e| ClusterError::NetworkError(format!("deserialization failed: {}", e)))
        })
        .await
        .map_err(|_| ClusterError::NetworkError("request timeout".to_string()))??;

        Ok(response)
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

/// Handle incoming RPC requests from other cluster nodes.
///
/// This function processes a single RPC request and returns the response.
/// It should be called by the cluster bus TCP server.
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
        // PubSub RPCs are handled by the server's cluster_bus module (which has
        // access to shard_senders). This function only handles Raft-level RPCs.
        ClusterRpcRequest::PubSubBroadcast { .. } | ClusterRpcRequest::PubSubForward { .. } => {
            ClusterRpcResponse::Error(
                "PubSub RPCs must be handled by the cluster bus, not the Raft handler".to_string(),
            )
        }
    }
}

/// Parse an incoming message from a cluster bus connection.
///
/// Returns the parsed request if successful.
pub async fn parse_rpc_message(stream: &mut TcpStream) -> Result<ClusterRpcRequest, ClusterError> {
    // Read length prefix
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to read message length: {}", e)))?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check on length
    if len > 64 * 1024 * 1024 {
        return Err(ClusterError::NetworkError("message too large".to_string()));
    }

    // Read message body
    let mut message_bytes = vec![0u8; len];
    stream
        .read_exact(&mut message_bytes)
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to read message: {}", e)))?;

    // Deserialize request
    serde_json::from_slice(&message_bytes)
        .map_err(|e| ClusterError::NetworkError(format!("deserialization failed: {}", e)))
}

/// Send an RPC response over a cluster bus connection.
pub async fn send_rpc_response(
    stream: &mut TcpStream,
    response: ClusterRpcResponse,
) -> Result<(), ClusterError> {
    // Serialize response
    let response_bytes = serde_json::to_vec(&response)
        .map_err(|e| ClusterError::NetworkError(format!("serialization failed: {}", e)))?;

    // Send length-prefixed message
    let len = response_bytes.len() as u32;
    stream
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to write length: {}", e)))?;
    stream
        .write_all(&response_bytes)
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to write response: {}", e)))?;
    stream
        .flush()
        .await
        .map_err(|e| ClusterError::NetworkError(format!("failed to flush: {}", e)))?;

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
        // Test that our RPC types can be serialized/deserialized
        let request = ClusterRpcRequest::Vote(VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });

        let bytes = serde_json::to_vec(&request).unwrap();
        let _: ClusterRpcRequest = serde_json::from_slice(&bytes).unwrap();
    }
}
