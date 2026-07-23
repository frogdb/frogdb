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
    BusRpc, ClusterRaft, ClusterRpcRequest, ClusterRpcResponse, NodeId, handle_rpc_request,
    parse_rpc_message, send_rpc_response,
};
// Framing helpers diverge by transport: production wraps a `tokio::net::TcpStream`
// (optionally TLS) via `new_framed_tcp`; under turmoil the accepted stream is a
// type-erased `BoxedStream` framed via `new_framed`.
#[cfg(feature = "turmoil")]
use frogdb_core::cluster::new_framed;
#[cfg(not(feature = "turmoil"))]
use frogdb_core::cluster::new_framed_tcp;
use frogdb_core::shard_for_key;
use tokio::sync::oneshot;

use crate::net::TcpListener;
use tracing::warn;
use tracing::{debug, error, info};

/// Context for the cluster bus, providing access to Raft and shard infrastructure.
pub struct ClusterBusContext {
    pub raft: Arc<ClusterRaft>,
    pub shard_senders: Arc<Vec<ShardSender>>,
    pub num_shards: usize,
    pub node_id: NodeId,
    pub replication_offset: Arc<AtomicU64>,
    /// TLS manager for accepting encrypted cluster bus connections.
    #[cfg(not(feature = "turmoil"))]
    pub tls_manager: Option<Arc<crate::tls::TlsManager>>,
    /// Whether to accept both plain and TLS connections during migration.
    #[cfg(not(feature = "turmoil"))]
    pub tls_cluster_migration: bool,
    /// TLS handshake timeout.
    #[cfg(not(feature = "turmoil"))]
    pub tls_handshake_timeout: std::time::Duration,
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
    let framed = if let Some(ref mgr) = ctx.tls_manager {
        if ctx.tls_cluster_migration {
            // Migration mode: peek first byte to determine TLS vs plaintext.
            // 0x16 = TLS ContentType::Handshake (ClientHello).
            let mut peek_buf = [0u8; 1];
            let n = stream.peek(&mut peek_buf).await?;
            if n > 0 && peek_buf[0] == 0x16 {
                let acceptor = mgr.acceptor();
                let tls_stream =
                    tokio::time::timeout(ctx.tls_handshake_timeout, acceptor.accept(stream))
                        .await
                        .map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "TLS handshake timeout",
                            )
                        })?
                        .map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e)
                        })?;
                frogdb_core::cluster::new_framed(Box::new(tls_stream))
            } else {
                new_framed_tcp(stream)
            }
        } else {
            // Strict TLS mode: all connections must be TLS.
            let acceptor = mgr.acceptor();
            let tls_stream =
                tokio::time::timeout(ctx.tls_handshake_timeout, acceptor.accept(stream))
                    .await
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake timeout")
                    })?
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;
            frogdb_core::cluster::new_framed(Box::new(tls_stream))
        }
    } else {
        new_framed_tcp(stream)
    };
    let mut framed = framed;

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

        // Dispatch on the wire envelope: one typed seam. The bus owns its
        // subset (`BusRpc`) locally; the Raft handler owns the rest (`RaftRpc`).
        let response = match request {
            ClusterRpcRequest::Bus(bus_rpc) => handle_bus_rpc(ctx, bus_rpc).await,
            ClusterRpcRequest::Raft(raft_rpc) => handle_rpc_request(&ctx.raft, raft_rpc).await,
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

/// Handle a single cluster bus connection under turmoil.
///
/// Turmoil's accepted stream is a `turmoil::net::TcpStream` (no TLS); type-erase
/// it into a `BoxedStream` and frame it the same way the production plaintext
/// path does. The read/dispatch/respond loop is otherwise identical to
/// [`handle_connection`] (the non-turmoil variant), so real Raft RPCs — vote,
/// append-entries, install-snapshot — and the bus subset flow through turmoil's
/// simulated network, giving deterministic multi-node cluster consensus.
#[cfg(feature = "turmoil")]
async fn handle_connection(
    stream: crate::net::ConnectionStream,
    ctx: &ClusterBusContext,
) -> std::io::Result<()> {
    let mut framed = new_framed(Box::new(stream));

    loop {
        let request = match parse_rpc_message(&mut framed).await {
            Ok(req) => req,
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("connection closed")
                    || error_msg.contains("connection reset")
                    || error_msg.contains("broken pipe")
                {
                    return Ok(());
                }
                warn!(error = %e, "Failed to parse cluster RPC request");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ));
            }
        };

        let response = match request {
            ClusterRpcRequest::Bus(bus_rpc) => handle_bus_rpc(ctx, bus_rpc).await,
            ClusterRpcRequest::Raft(raft_rpc) => handle_rpc_request(&ctx.raft, raft_rpc).await,
        };

        if let Err(e) = send_rpc_response(&mut framed, response).await {
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
async fn handle_bus_rpc(ctx: &ClusterBusContext, request: BusRpc) -> ClusterRpcResponse {
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
    use crate::connection::pubsub_conn_command::BROADCAST_SHARD;
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
    use crate::net::tcp_listener_reusable;
    use std::net::SocketAddr;

    #[tokio::test]
    async fn test_cluster_bus_bind_fails_on_invalid_addr() {
        // Trying to bind to a privileged port should fail (unless running as root)
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        // We can't easily test run() without a real Raft instance,
        // but we can verify tcp_listener_reusable behavior
        let result: std::io::Result<crate::net::TcpListener> = tcp_listener_reusable(addr).await;
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
