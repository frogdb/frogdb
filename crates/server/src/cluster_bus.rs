//! Cluster bus TCP server for Raft RPC communication.
//!
//! This module provides a TCP server that handles incoming Raft RPC requests
//! from other cluster nodes. It uses the length-prefixed JSON protocol defined
//! in frogdb_core::cluster::network.

use std::sync::Arc;

use frogdb_core::cluster::ClusterRaft;
#[cfg(not(feature = "turmoil"))]
use frogdb_core::cluster::{handle_rpc_request, parse_rpc_message, send_rpc_response};

use crate::net::TcpListener;
#[cfg(not(feature = "turmoil"))]
use tracing::warn;
use tracing::{debug, error, info};

/// Run the cluster bus TCP server.
///
/// This server listens for incoming connections from other cluster nodes
/// and handles Raft RPC requests (AppendEntries, Vote, InstallSnapshot).
///
/// Accepts a pre-bound `TcpListener` so that the port is held open from
/// `Server::new()` and never subject to TOCTOU port races.
///
/// # Arguments
///
/// * `listener` - Pre-bound TCP listener for cluster bus connections
/// * `raft` - The Raft instance to handle requests
///
/// # Returns
///
/// This function runs indefinitely and only returns on error.
pub async fn run(listener: TcpListener, raft: Arc<ClusterRaft>) -> std::io::Result<()> {
    let addr = listener.local_addr()?;
    info!(%addr, "Cluster bus listening");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                let raft = raft.clone();
                tokio::spawn(async move {
                    debug!(%peer, "Cluster bus connection accepted");
                    #[cfg(not(feature = "turmoil"))]
                    if let Err(e) = handle_connection(stream, &raft).await {
                        // Connection errors are expected when nodes disconnect
                        debug!(%peer, error = %e, "Cluster bus connection closed");
                    }
                    #[cfg(feature = "turmoil")]
                    drop((stream, raft));
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
/// Reads RPC requests in a loop, processes them via Raft, and sends responses.
/// The connection is kept open for multiple requests (connection reuse).
#[cfg(not(feature = "turmoil"))]
async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    raft: &ClusterRaft,
) -> std::io::Result<()> {
    loop {
        // Parse the incoming RPC request
        let request = match parse_rpc_message(&mut stream).await {
            Ok(req) => req,
            Err(e) => {
                // Check if this is a clean disconnect (EOF)
                let error_msg = e.to_string();
                if error_msg.contains("failed to read message length")
                    || error_msg.contains("unexpected end of file")
                    || error_msg.contains("connection reset")
                {
                    // Clean disconnect, not an error
                    return Ok(());
                }
                warn!(error = %e, "Failed to parse cluster RPC request");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e.to_string(),
                ));
            }
        };

        // Handle the request via Raft
        let response = handle_rpc_request(raft, request).await;

        // Send the response
        if let Err(e) = send_rpc_response(&mut stream, response).await {
            warn!(error = %e, "Failed to send cluster RPC response");
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                e.to_string(),
            ));
        }
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
