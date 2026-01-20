//! Connection handling.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use frogdb_core::{shard_for_key, CommandRegistry, ShardMessage};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use futures::{SinkExt, StreamExt};
use redis_protocol::resp2::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, trace};

/// Connection state.
#[allow(dead_code)]
pub struct ConnectionState {
    /// Unique connection ID.
    pub id: u64,

    /// Client address.
    pub addr: SocketAddr,

    /// Connection creation time.
    pub created_at: std::time::Instant,

    /// Protocol version.
    pub protocol_version: ProtocolVersion,

    /// Client name (from CLIENT SETNAME).
    pub name: Option<Bytes>,
}

impl ConnectionState {
    fn new(id: u64, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            created_at: std::time::Instant::now(),
            protocol_version: ProtocolVersion::default(),
            name: None,
        }
    }
}

/// Connection handler that processes client commands.
pub struct ConnectionHandler {
    /// Framed socket with RESP2 codec.
    framed: Framed<TcpStream, Resp2>,

    /// Connection state.
    state: ConnectionState,

    /// Assigned shard ID.
    shard_id: usize,

    /// Total number of shards.
    num_shards: usize,

    /// Command registry.
    registry: Arc<CommandRegistry>,

    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
}

impl ConnectionHandler {
    /// Create a new connection handler.
    pub fn new(
        socket: TcpStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        num_shards: usize,
        registry: Arc<CommandRegistry>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    ) -> Self {
        let framed = Framed::new(socket, Resp2::default());
        let state = ConnectionState::new(conn_id, addr);

        Self {
            framed,
            state,
            shard_id,
            num_shards,
            registry,
            shard_senders,
        }
    }

    /// Run the connection handling loop.
    pub async fn run(mut self) -> Result<()> {
        debug!(conn_id = self.state.id, "Connection handler started");

        loop {
            // Read next frame from client
            let frame = match self.framed.next().await {
                Some(Ok(frame)) => frame,
                Some(Err(e)) => {
                    debug!(conn_id = self.state.id, error = %e, "Frame error");
                    let error_frame = BytesFrame::SimpleError(Bytes::from(format!("ERR {}", e)));
                    let _ = self.framed.send(error_frame).await;
                    continue;
                }
                None => {
                    debug!(conn_id = self.state.id, "Client disconnected");
                    break;
                }
            };

            // Parse frame into command
            let cmd = match ParsedCommand::try_from(frame) {
                Ok(cmd) => cmd,
                Err(e) => {
                    let error_frame = BytesFrame::SimpleError(Bytes::from(format!("ERR {}", e)));
                    let _ = self.framed.send(error_frame).await;
                    continue;
                }
            };

            trace!(
                conn_id = self.state.id,
                cmd = %String::from_utf8_lossy(&cmd.name),
                args = cmd.args.len(),
                "Received command"
            );

            // Handle QUIT specially
            if cmd.name.eq_ignore_ascii_case(b"QUIT") {
                let response_frame: BytesFrame = Response::ok().into();
                let _ = self.framed.send(response_frame).await;
                break;
            }

            // Route and execute
            let response = self.route_and_execute(&cmd).await;

            // Send response
            let response_frame: BytesFrame = response.into();
            if self.framed.send(response_frame).await.is_err() {
                debug!(conn_id = self.state.id, "Failed to send response");
                break;
            }
        }

        debug!(conn_id = self.state.id, "Connection handler finished");
        Ok(())
    }

    /// Route command to appropriate shard and execute.
    async fn route_and_execute(&self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Lookup command
        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ))
            }
        };

        // Validate arity
        if !handler.arity().check(cmd.args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
        }

        // Extract keys for routing
        let keys = handler.keys(&cmd.args);

        // Keyless commands: execute on local shard
        if keys.is_empty() {
            return self.execute_on_shard(self.shard_id, cmd).await;
        }

        // Single-key command: route to owner shard
        if keys.len() == 1 {
            let target_shard = shard_for_key(keys[0], self.num_shards);
            return self.execute_on_shard(target_shard, cmd).await;
        }

        // Multi-key command: validate same shard or return CROSSSLOT
        let first_shard = shard_for_key(keys[0], self.num_shards);
        for key in &keys[1..] {
            if shard_for_key(key, self.num_shards) != first_shard {
                return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
            }
        }

        // All keys on same shard
        self.execute_on_shard(first_shard, cmd).await
    }

    /// Execute command on a specific shard.
    async fn execute_on_shard(&self, shard_id: usize, cmd: &ParsedCommand) -> Response {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ShardMessage::Execute {
            command: cmd.clone(),
            conn_id: self.state.id,
            response_tx,
        };

        // Send to shard
        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        // Await response
        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }
}
