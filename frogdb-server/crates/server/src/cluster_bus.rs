//! Cluster bus TCP server for Raft RPC communication.
//!
//! This module provides a TCP server that handles incoming RPC requests
//! from other cluster nodes. It uses the length-prefixed JSON protocol defined
//! in frogdb_core::cluster::network.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
#[cfg(not(feature = "turmoil"))]
use std::sync::atomic::Ordering;

use frogdb_core::cluster::{ClusterRaft, NodeId};
#[cfg(not(feature = "turmoil"))]
use frogdb_core::cluster::{
    ClusterRpcRequest, ClusterRpcResponse, handle_rpc_request, new_framed, parse_rpc_message,
    send_rpc_response,
};
#[cfg(not(feature = "turmoil"))]
use frogdb_core::shard_for_key;
use frogdb_core::{ShardMessage, ShardSender};
#[cfg(not(feature = "turmoil"))]
use tokio::sync::oneshot;

use crate::net::TcpListener;
#[cfg(not(feature = "turmoil"))]
use tracing::warn;
use tracing::{debug, error, info};

/// Context for the cluster bus, providing access to Raft and shard infrastructure.
pub struct ClusterBusContext {
    pub raft: Arc<ClusterRaft>,
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub num_shards: usize,
    pub node_id: NodeId,
    pub replication_offset: Arc<AtomicU64>,
}

/// Run the cluster bus TCP server.
///
/// This server listens for incoming connections from other cluster nodes
/// and handles Raft RPCs, pub/sub forwarding, and HealthProbe requests.
///
/// Accepts a pre-bound `TcpListener` so that the port is held open from
/// `Server::new()` and never subject to TOCTOU port races.
pub async fn run(listener: TcpListener, ctx: Arc<ClusterBusContext>) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    info!(%addr, "Cluster bus listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    debug!(%peer, "Cluster bus connection accepted");
                    #[cfg(not(feature = "turmoil"))]
                    if let Err(e) = handle_connection(stream, &ctx).await {
                        // Connection errors are expected when nodes disconnect
                        debug!(%peer, error = %e, "Cluster bus connection closed");
                    }
                    #[cfg(feature = "turmoil")]
                    drop((stream, ctx));
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept cluster bus connection");
            }
        }
    }
}

/// Handle a single cluster bus connection.
///
/// Reads RPC requests in a loop, processes them, and sends responses.
/// Raft RPCs are delegated to the Raft instance; pub/sub and HealthProbe
/// RPCs are handled locally.
#[cfg(not(feature = "turmoil"))]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    ctx: &ClusterBusContext,
) -> std::io::Result<()> {
    let mut framed = new_framed(stream);

    loop {
        // Parse the incoming RPC request
        let request = match parse_rpc_message(&mut framed).await {
            Ok(req) => req,
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("connection closed")
                    || error_msg.contains("connection reset")
                    || error_msg.contains("broken pipe")
                {
                    // Clean disconnect or peer went away
                    return Ok(());
                }
                warn!(error = %e, "Failed to parse cluster RPC request");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ));
            }
        };

        // Dispatch based on request type
        let response = match request {
            ClusterRpcRequest::PubSubBroadcast { channel, message } => {
                handle_pubsub_broadcast(&ctx.shard_senders, &channel, &message).await
            }
            ClusterRpcRequest::PubSubForward { channel, message } => {
                handle_pubsub_forward(&ctx.shard_senders, ctx.num_shards, &channel, &message).await
            }
            ClusterRpcRequest::HealthProbe => ClusterRpcResponse::HealthProbeResponse {
                node_id: ctx.node_id,
                replication_offset: ctx.replication_offset.load(Ordering::Acquire),
            },
            // All Raft RPCs (AppendEntries, Vote, InstallSnapshot, ForwardedWrite)
            raft_request => handle_rpc_request(&ctx.raft, raft_request).await,
        };

        // Send the response
        if let Err(e) = send_rpc_response(&mut framed, response).await {
            warn!(error = %e, "Failed to send cluster RPC response");
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                e.to_string(),
            ));
        }
    }
}

/// Handle a PubSubBroadcast RPC: deliver to shard 0 (broadcast pub/sub coordinator).
#[cfg(not(feature = "turmoil"))]
async fn handle_pubsub_broadcast(
    shard_senders: &[ShardSender],
    channel: &[u8],
    message: &[u8],
) -> ClusterRpcResponse {
    let (response_tx, response_rx) = oneshot::channel();
    let _ = shard_senders[0]
        .send(ShardMessage::Publish {
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
#[cfg(not(feature = "turmoil"))]
async fn handle_pubsub_forward(
    shard_senders: &[ShardSender],
    num_shards: usize,
    channel: &[u8],
    message: &[u8],
) -> ClusterRpcResponse {
    let shard_id = shard_for_key(channel, num_shards);
    let (response_tx, response_rx) = oneshot::channel();
    let _ = shard_senders[shard_id]
        .send(ShardMessage::ShardedPublish {
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

#[cfg(test)]
mod tests {
    use crate::net::tcp_listener_reusable;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_cluster_bus_bind_fails_on_invalid_addr() {
        // Trying to bind to a privileged port should fail (unless running as root)
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        // We can't easily test run() without a real Raft instance,
        // but we can verify tcp_listener_reusable behavior
        let result: std::io::Result<crate::net::TcpListener> = tcp_listener_reusable(addr).await;
        // This should fail due to permission denied or address in use
        assert!(result.is_err() || cfg!(target_os = "macos")); // macOS sometimes allows this
    }
}
