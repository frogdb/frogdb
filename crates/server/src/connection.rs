//! Connection handling.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use frogdb_core::{shard_for_key, CommandRegistry, PartialResult, ScatterOp, ShardMessage, TransactionResult};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use futures::{SinkExt, StreamExt};
use redis_protocol::bytes_utils::Str;
use redis_protocol::codec::Resp2;
use redis_protocol::resp2::types::BytesFrame;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::server::next_txid;

/// Target shard(s) for a transaction (prepared for future multi-shard support).
#[derive(Debug, Clone, Default)]
pub enum TransactionTarget {
    /// No keys yet - target undetermined.
    #[default]
    None,
    /// Single shard - execute directly.
    Single(usize),
    /// Multiple shards detected - error in Phase 7.1, VLL in future.
    Multi(Vec<usize>),
}

/// State for MULTI/EXEC transactions.
#[derive(Debug, Default)]
pub struct TransactionState {
    /// Queue of commands to execute at EXEC time (None = not in transaction).
    pub queue: Option<Vec<ParsedCommand>>,
    /// Watched keys: key -> (shard_id, version_at_watch_time).
    pub watches: HashMap<Bytes, (usize, u64)>,
    /// Target shard(s) for this transaction.
    pub target: TransactionTarget,
    /// Whether any queued command had an error (abort at EXEC).
    pub exec_abort: bool,
    /// Error messages for commands that had syntax errors during queuing.
    pub queued_errors: Vec<String>,
}

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

    /// Transaction state for MULTI/EXEC.
    pub transaction: TransactionState,
}

impl ConnectionState {
    fn new(id: u64, addr: SocketAddr) -> Self {
        Self {
            id,
            addr,
            created_at: std::time::Instant::now(),
            protocol_version: ProtocolVersion::default(),
            name: None,
            transaction: TransactionState::default(),
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

    /// Allow cross-slot operations (scatter-gather).
    allow_cross_slot: bool,

    /// Timeout for scatter-gather operations.
    scatter_gather_timeout: Duration,
}

impl ConnectionHandler {
    /// Create a new connection handler.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket: TcpStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        num_shards: usize,
        registry: Arc<CommandRegistry>,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        allow_cross_slot: bool,
        scatter_gather_timeout_ms: u64,
    ) -> Self {
        let framed = Framed::new(socket, Resp2);
        let state = ConnectionState::new(conn_id, addr);

        Self {
            framed,
            state,
            shard_id,
            num_shards,
            registry,
            shard_senders,
            allow_cross_slot,
            scatter_gather_timeout: Duration::from_millis(scatter_gather_timeout_ms),
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
                    let error_frame = BytesFrame::Error(
                        Str::from_inner(Bytes::from(format!("ERR {}", e)))
                            .expect("error message must be valid UTF-8"),
                    );
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
                    let error_frame = BytesFrame::Error(
                        Str::from_inner(Bytes::from(format!("ERR {}", e)))
                            .expect("error message must be valid UTF-8"),
                    );
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

            // Handle QUIT specially (also clears transaction state)
            if cmd.name.eq_ignore_ascii_case(b"QUIT") {
                // Clear transaction state before quitting
                self.state.transaction = TransactionState::default();
                let response_frame: BytesFrame = Response::ok().into();
                let _ = self.framed.send(response_frame).await;
                break;
            }

            // Route and execute (with transaction handling)
            let response = self.route_and_execute_with_transaction(&cmd).await;

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

    /// Route and execute a command, handling transaction mode.
    async fn route_and_execute_with_transaction(&mut self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Handle transaction commands specially
        match cmd_name_str.as_ref() {
            "MULTI" => return self.handle_multi(),
            "EXEC" => return self.handle_exec().await,
            "DISCARD" => return self.handle_discard(),
            "WATCH" => return self.handle_watch(&cmd.args).await,
            "UNWATCH" => return self.handle_unwatch(),
            _ => {}
        }

        // If in transaction mode, queue the command instead of executing
        if self.state.transaction.queue.is_some() {
            return self.queue_command(cmd);
        }

        // Normal execution
        self.route_and_execute(cmd).await
    }

    /// Handle MULTI command - start a transaction.
    fn handle_multi(&mut self) -> Response {
        if self.state.transaction.queue.is_some() {
            return Response::error("ERR MULTI calls can not be nested");
        }

        self.state.transaction.queue = Some(Vec::new());
        self.state.transaction.target = TransactionTarget::None;
        self.state.transaction.exec_abort = false;
        self.state.transaction.queued_errors.clear();

        Response::ok()
    }

    /// Handle EXEC command - execute the queued transaction.
    async fn handle_exec(&mut self) -> Response {
        // Check if in transaction mode
        let queue = match self.state.transaction.queue.take() {
            Some(q) => q,
            None => return Response::error("ERR EXEC without MULTI"),
        };

        // Get watches and clear them
        let watches: Vec<(Bytes, u64)> = self.state.transaction.watches
            .drain()
            .map(|(key, (_, ver))| (key, ver))
            .collect();

        // Check if we should abort due to queuing errors
        if self.state.transaction.exec_abort {
            self.clear_transaction_state();
            return Response::error("EXECABORT Transaction discarded because of previous errors.");
        }

        // Handle empty transaction
        if queue.is_empty() {
            self.clear_transaction_state();
            return Response::Array(vec![]);
        }

        // Get target shard
        let target_shard = match &self.state.transaction.target {
            TransactionTarget::None => {
                // No keys in any command - execute on local shard
                self.shard_id
            }
            TransactionTarget::Single(shard) => *shard,
            TransactionTarget::Multi(_) => {
                self.clear_transaction_state();
                return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
            }
        };

        // Clear transaction state before executing
        self.clear_transaction_state();

        // Send ExecTransaction to the target shard
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ShardMessage::ExecTransaction {
            commands: queue,
            watches,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[target_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        // Wait for result
        match response_rx.await {
            Ok(TransactionResult::Success(results)) => Response::Array(results),
            Ok(TransactionResult::WatchAborted) => Response::null(),
            Ok(TransactionResult::Error(e)) => Response::error(e),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle DISCARD command - abort the transaction.
    fn handle_discard(&mut self) -> Response {
        if self.state.transaction.queue.is_none() {
            return Response::error("ERR DISCARD without MULTI");
        }

        // Clear all transaction state including watches (Redis behavior)
        self.state.transaction = TransactionState::default();
        Response::ok()
    }

    /// Handle WATCH command - watch keys for modifications.
    async fn handle_watch(&mut self, args: &[Bytes]) -> Response {
        // WATCH is not allowed inside MULTI
        if self.state.transaction.queue.is_some() {
            return Response::error("ERR WATCH inside MULTI is not allowed");
        }

        // Validate arity
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'watch' command");
        }

        // Check same-slot requirement for watched keys
        let mut target_shard: Option<usize> = None;
        for key in args {
            let shard = shard_for_key(key, self.num_shards);
            match target_shard {
                None => target_shard = Some(shard),
                Some(s) if s != shard => {
                    return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
                }
                _ => {}
            }
        }

        let shard = target_shard.unwrap();

        // Get version from the shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::GetVersion { response_tx };

        if self.shard_senders[shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        let version = match response_rx.await {
            Ok(v) => v,
            Err(_) => return Response::error("ERR shard dropped request"),
        };

        // Store watched keys with their versions
        for key in args {
            self.state.transaction.watches.insert(key.clone(), (shard, version));
        }

        // Update transaction target based on watched keys
        self.update_target(shard);

        Response::ok()
    }

    /// Handle UNWATCH command - forget all watched keys.
    fn handle_unwatch(&mut self) -> Response {
        self.state.transaction.watches.clear();
        Response::ok()
    }

    /// Queue a command during transaction mode.
    fn queue_command(&mut self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Look up command for validation
        let handler = match self.registry.get(&cmd_name_str) {
            Some(h) => h,
            None => {
                self.state.transaction.exec_abort = true;
                self.state.transaction.queued_errors.push(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ));
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                ));
            }
        };

        // Validate arity
        if !handler.arity().check(cmd.args.len()) {
            self.state.transaction.exec_abort = true;
            self.state.transaction.queued_errors.push(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name()
            ));
        }

        // Extract keys for same-slot validation
        let keys = handler.keys(&cmd.args);

        // Check same-slot requirement
        for key in &keys {
            let shard = shard_for_key(key, self.num_shards);
            self.update_target(shard);

            // Check if we've crossed into multi-shard territory
            if let TransactionTarget::Multi(_) = &self.state.transaction.target {
                // Don't abort immediately, just mark for error at EXEC time
                // This allows continuing to queue commands (Redis behavior)
            }
        }

        // Queue the command
        if let Some(ref mut queue) = self.state.transaction.queue {
            queue.push(cmd.clone());
        }

        Response::Simple(Bytes::from_static(b"QUEUED"))
    }

    /// Update the transaction target shard.
    fn update_target(&mut self, shard_id: usize) {
        self.state.transaction.target = match &self.state.transaction.target {
            TransactionTarget::None => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) if *s == shard_id => TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) => TransactionTarget::Multi(vec![*s, shard_id]),
            TransactionTarget::Multi(shards) => {
                let mut shards = shards.clone();
                if !shards.contains(&shard_id) {
                    shards.push(shard_id);
                }
                TransactionTarget::Multi(shards)
            }
        };
    }

    /// Clear transaction state.
    fn clear_transaction_state(&mut self) {
        self.state.transaction.queue = None;
        self.state.transaction.target = TransactionTarget::None;
        self.state.transaction.exec_abort = false;
        self.state.transaction.queued_errors.clear();
        // Note: watches are cleared separately, not here (they're consumed by EXEC)
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

        // Multi-key command: check if all keys are on the same shard
        let first_shard = shard_for_key(keys[0], self.num_shards);
        let all_same_shard = keys[1..].iter().all(|key| {
            shard_for_key(key, self.num_shards) == first_shard
        });

        if all_same_shard {
            // All keys on same shard - execute directly
            return self.execute_on_shard(first_shard, cmd).await;
        }

        // Keys span multiple shards
        // Check if command requires same slot (like MSETNX)
        if handler.requires_same_slot() {
            return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
        }

        // Check if cross-slot is allowed
        if !self.allow_cross_slot {
            return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
        }

        // Determine the scatter operation based on command name
        let scatter_op = match cmd_name_str.as_ref() {
            "MGET" => Some(ScatterOp::MGet),
            "MSET" => {
                // Build pairs from args
                let pairs: Vec<(Bytes, Bytes)> = cmd.args
                    .chunks(2)
                    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                    .collect();
                Some(ScatterOp::MSet { pairs })
            }
            "DEL" => Some(ScatterOp::Del),
            "EXISTS" => Some(ScatterOp::Exists),
            "TOUCH" => Some(ScatterOp::Touch),
            "UNLINK" => Some(ScatterOp::Unlink),
            _ => None,
        };

        match scatter_op {
            Some(op) => self.execute_scatter_gather(&keys, &cmd.args, op).await,
            None => {
                // Command doesn't support scatter-gather
                Response::error("CROSSSLOT Keys in request don't hash to the same slot")
            }
        }
    }

    /// Execute a scatter-gather operation across multiple shards.
    async fn execute_scatter_gather(
        &self,
        keys: &[&[u8]],
        _args: &[Bytes],
        operation: ScatterOp,
    ) -> Response {
        // Group keys by shard
        let mut shard_keys: HashMap<usize, Vec<Bytes>> = HashMap::new();
        let mut key_order: Vec<(usize, Bytes)> = Vec::new(); // (shard_id, key)

        for key in keys {
            let shard_id = shard_for_key(key, self.num_shards);
            let key_bytes = Bytes::copy_from_slice(key);
            shard_keys.entry(shard_id).or_default().push(key_bytes.clone());
            key_order.push((shard_id, key_bytes));
        }

        // For MSET, we need to distribute pairs to shards
        let shard_operations: HashMap<usize, ScatterOp> = match &operation {
            ScatterOp::MSet { pairs } => {
                let mut shard_pairs: HashMap<usize, Vec<(Bytes, Bytes)>> = HashMap::new();
                for (key, value) in pairs {
                    let shard_id = shard_for_key(key, self.num_shards);
                    shard_pairs.entry(shard_id).or_default().push((key.clone(), value.clone()));
                }
                shard_pairs.into_iter()
                    .map(|(shard_id, pairs)| (shard_id, ScatterOp::MSet { pairs }))
                    .collect()
            }
            _ => {
                // For other operations, use the same operation for all shards
                shard_keys.keys().map(|&shard_id| (shard_id, operation.clone())).collect()
            }
        };

        let txid = next_txid();

        // Send ScatterRequest to each shard
        let mut handles: Vec<(usize, oneshot::Receiver<PartialResult>)> = Vec::new();

        for (shard_id, keys) in &shard_keys {
            let (tx, rx) = oneshot::channel();
            let shard_op = shard_operations.get(shard_id).cloned().unwrap_or_else(|| operation.clone());

            let msg = ShardMessage::ScatterRequest {
                request_id: txid,
                keys: keys.clone(),
                operation: shard_op,
                response_tx: tx,
            };

            if self.shard_senders[*shard_id].send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((*shard_id, rx));
        }

        // Await all responses with timeout
        let mut shard_results: HashMap<usize, HashMap<Bytes, Response>> = HashMap::new();

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    let results: HashMap<Bytes, Response> = partial.results.into_iter().collect();
                    shard_results.insert(shard_id, results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped scatter-gather request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "Scatter-gather timeout");
                    return Response::error("ERR scatter-gather timeout");
                }
            }
        }

        // Merge results based on operation type
        match &operation {
            ScatterOp::MGet => {
                // Return array in original key order
                let results: Vec<Response> = key_order
                    .iter()
                    .map(|(shard_id, key)| {
                        shard_results
                            .get(shard_id)
                            .and_then(|m| m.get(key))
                            .cloned()
                            .unwrap_or(Response::null())
                    })
                    .collect();
                Response::Array(results)
            }
            ScatterOp::MSet { .. } => {
                // MSET always returns OK
                Response::ok()
            }
            ScatterOp::Del | ScatterOp::Unlink | ScatterOp::Exists | ScatterOp::Touch => {
                // Sum the counts
                let total: i64 = shard_results
                    .values()
                    .flat_map(|m| m.values())
                    .filter_map(|r| {
                        if let Response::Integer(n) = r {
                            Some(*n)
                        } else {
                            None
                        }
                    })
                    .sum();
                Response::Integer(total)
            }
        }
    }

    /// Execute command on a specific shard.
    async fn execute_on_shard(&self, shard_id: usize, cmd: &ParsedCommand) -> Response {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ShardMessage::Execute {
            command: cmd.clone(),
            conn_id: self.state.id,
            txid: None, // Single-shard operations don't need txid
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
