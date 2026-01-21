//! Connection handling.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use frogdb_core::{
    shard_for_key, CommandRegistry, GlobPattern, IntrospectionRequest, IntrospectionResponse,
    PartialResult, PubSubMessage, PubSubSender, ScatterOp, ShardMessage, TransactionResult,
    MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
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

/// Pub/Sub state for a connection.
#[derive(Debug, Default)]
pub struct PubSubState {
    /// Broadcast channel subscriptions.
    pub subscriptions: HashSet<Bytes>,
    /// Pattern subscriptions.
    pub patterns: HashSet<Bytes>,
    /// Sharded channel subscriptions.
    pub sharded_subscriptions: HashSet<Bytes>,
}

impl PubSubState {
    /// Check if connection is in pub/sub mode.
    pub fn in_pubsub_mode(&self) -> bool {
        !self.subscriptions.is_empty()
            || !self.patterns.is_empty()
            || !self.sharded_subscriptions.is_empty()
    }

    /// Get total subscription count.
    pub fn total_count(&self) -> usize {
        self.subscriptions.len() + self.patterns.len() + self.sharded_subscriptions.len()
    }

    /// Get subscription count (channels + patterns, not sharded).
    pub fn sub_count(&self) -> usize {
        self.subscriptions.len() + self.patterns.len()
    }
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

    /// Pub/Sub state.
    pub pubsub: PubSubState,
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
            pubsub: PubSubState::default(),
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

    /// Sender for pub/sub messages (cloned to shards when subscribing).
    pubsub_tx: PubSubSender,

    /// Receiver for pub/sub messages from shards.
    pubsub_rx: mpsc::UnboundedReceiver<PubSubMessage>,
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

        // Create pub/sub channel
        let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

        Self {
            framed,
            state,
            shard_id,
            num_shards,
            registry,
            shard_senders,
            allow_cross_slot,
            scatter_gather_timeout: Duration::from_millis(scatter_gather_timeout_ms),
            pubsub_tx,
            pubsub_rx,
        }
    }

    /// Run the connection handling loop.
    pub async fn run(mut self) -> Result<()> {
        debug!(conn_id = self.state.id, "Connection handler started");

        loop {
            tokio::select! {
                // Handle pub/sub messages from shards
                Some(pubsub_msg) = self.pubsub_rx.recv() => {
                    let response = pubsub_msg.to_response();
                    let response_frame: BytesFrame = response.into();
                    if self.framed.send(response_frame).await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to send pub/sub message");
                        break;
                    }
                }

                // Handle client commands
                frame_result = self.framed.next() => {
                    let frame = match frame_result {
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

                    // Route and execute (with transaction and pub/sub handling)
                    let responses = self.route_and_execute_with_transaction(&cmd).await;

                    // Send response(s)
                    for response in responses {
                        let response_frame: BytesFrame = response.into();
                        if self.framed.send(response_frame).await.is_err() {
                            debug!(conn_id = self.state.id, "Failed to send response");
                            // Break out of the loop on send failure
                            break;
                        }
                    }
                }
            }
        }

        // Cleanup: notify all shards that this connection is closed
        self.notify_connection_closed().await;

        debug!(conn_id = self.state.id, "Connection handler finished");
        Ok(())
    }

    /// Notify all shards that this connection is closed.
    async fn notify_connection_closed(&self) {
        // Only notify if we had any subscriptions
        if self.state.pubsub.in_pubsub_mode() {
            for sender in self.shard_senders.iter() {
                let _ = sender.send(ShardMessage::ConnectionClosed {
                    conn_id: self.state.id,
                }).await;
            }
        }
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    async fn route_and_execute_with_transaction(&mut self, cmd: &ParsedCommand) -> Vec<Response> {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Check pub/sub mode restrictions
        if self.state.pubsub.in_pubsub_mode() && !Self::is_allowed_in_pubsub_mode(&cmd_name_str) {
            return vec![Response::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd_name_str
            ))];
        }

        // Handle pub/sub commands specially (they return multiple responses)
        match cmd_name_str.as_ref() {
            "SUBSCRIBE" => return self.handle_subscribe(&cmd.args).await,
            "UNSUBSCRIBE" => return self.handle_unsubscribe(&cmd.args).await,
            "PSUBSCRIBE" => return self.handle_psubscribe(&cmd.args).await,
            "PUNSUBSCRIBE" => return self.handle_punsubscribe(&cmd.args).await,
            "PUBLISH" => return vec![self.handle_publish(&cmd.args).await],
            "SSUBSCRIBE" => return self.handle_ssubscribe(&cmd.args).await,
            "SUNSUBSCRIBE" => return self.handle_sunsubscribe(&cmd.args).await,
            "SPUBLISH" => return vec![self.handle_spublish(&cmd.args).await],
            "PUBSUB" => return vec![self.handle_pubsub_command(&cmd.args).await],
            _ => {}
        }

        // Handle transaction commands specially
        match cmd_name_str.as_ref() {
            "MULTI" => return vec![self.handle_multi()],
            "EXEC" => return vec![self.handle_exec().await],
            "DISCARD" => return vec![self.handle_discard()],
            "WATCH" => return vec![self.handle_watch(&cmd.args).await],
            "UNWATCH" => return vec![self.handle_unwatch()],
            _ => {}
        }

        // Handle scripting commands specially (routing needs special handling)
        match cmd_name_str.as_ref() {
            "EVAL" => return vec![self.handle_eval(&cmd.args).await],
            "EVALSHA" => return vec![self.handle_evalsha(&cmd.args).await],
            "SCRIPT" => return vec![self.handle_script(&cmd.args).await],
            _ => {}
        }

        // If in transaction mode, queue the command instead of executing
        if self.state.transaction.queue.is_some() {
            return vec![self.queue_command(cmd)];
        }

        // Normal execution
        vec![self.route_and_execute(cmd).await]
    }

    /// Check if a command is allowed in pub/sub mode.
    fn is_allowed_in_pubsub_mode(cmd: &str) -> bool {
        matches!(
            cmd,
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
                | "SSUBSCRIBE" | "SUNSUBSCRIBE" | "PING" | "QUIT" | "RESET"
        )
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

    // =========================================================================
    // Pub/Sub command handlers
    // =========================================================================

    /// Handle SUBSCRIBE command.
    async fn handle_subscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error("ERR wrong number of arguments for 'subscribe' command")];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.subscriptions.len() + args.len();
        if new_count > MAX_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max subscriptions reached")];
        }

        let mut responses = Vec::with_capacity(args.len());

        // Fan out to all shards for broadcast subscriptions
        for channel in args {
            // Add to local tracking
            self.state.pubsub.subscriptions.insert(channel.clone());

            // Send to all shards
            for sender in self.shard_senders.iter() {
                let (response_tx, _response_rx) = oneshot::channel();
                let _ = sender.send(ShardMessage::Subscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    sender: self.pubsub_tx.clone(),
                    response_tx,
                }).await;
            }

            // Build subscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"subscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle UNSUBSCRIBE command.
    async fn handle_unsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.pubsub.subscriptions.iter().cloned().collect()
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"unsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            self.state.pubsub.subscriptions.remove(&channel);

            // Send to all shards
            for sender in self.shard_senders.iter() {
                let (response_tx, _response_rx) = oneshot::channel();
                let _ = sender.send(ShardMessage::Unsubscribe {
                    channels: vec![channel.clone()],
                    conn_id: self.state.id,
                    response_tx,
                }).await;
            }

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"unsubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle PSUBSCRIBE command.
    async fn handle_psubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error("ERR wrong number of arguments for 'psubscribe' command")];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.patterns.len() + args.len();
        if new_count > MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max pattern subscriptions reached")];
        }

        let mut responses = Vec::with_capacity(args.len());

        for pattern in args {
            // Add to local tracking
            self.state.pubsub.patterns.insert(pattern.clone());

            // Send to all shards
            for sender in self.shard_senders.iter() {
                let (response_tx, _response_rx) = oneshot::channel();
                let _ = sender.send(ShardMessage::PSubscribe {
                    patterns: vec![pattern.clone()],
                    conn_id: self.state.id,
                    sender: self.pubsub_tx.clone(),
                    response_tx,
                }).await;
            }

            // Build subscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"psubscribe")),
                Response::bulk(pattern.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle PUNSUBSCRIBE command.
    async fn handle_punsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all patterns
        let patterns: Vec<Bytes> = if args.is_empty() {
            self.state.pubsub.patterns.iter().cloned().collect()
        } else {
            args.to_vec()
        };

        // Handle case where no patterns to unsubscribe from
        if patterns.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"punsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(patterns.len());

        for pattern in patterns {
            // Remove from local tracking
            self.state.pubsub.patterns.remove(&pattern);

            // Send to all shards
            for sender in self.shard_senders.iter() {
                let (response_tx, _response_rx) = oneshot::channel();
                let _ = sender.send(ShardMessage::PUnsubscribe {
                    patterns: vec![pattern.clone()],
                    conn_id: self.state.id,
                    response_tx,
                }).await;
            }

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sub_count();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"punsubscribe")),
                Response::bulk(pattern.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle PUBLISH command.
    async fn handle_publish(&self, args: &[Bytes]) -> Response {
        if args.len() != 2 {
            return Response::error("ERR wrong number of arguments for 'publish' command");
        }

        let channel = &args[0];
        let message = &args[1];

        // Scatter to all shards and sum subscriber counts
        let mut total_count = 0usize;
        let mut handles = Vec::with_capacity(self.num_shards);

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::Publish {
                channel: channel.clone(),
                message: message.clone(),
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Gather results
        for rx in handles {
            if let Ok(count) = rx.await {
                total_count += count;
            }
        }

        Response::Integer(total_count as i64)
    }

    /// Handle SSUBSCRIBE command (sharded subscriptions).
    async fn handle_ssubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        if args.is_empty() {
            return vec![Response::error("ERR wrong number of arguments for 'ssubscribe' command")];
        }

        // Check subscription limits
        let new_count = self.state.pubsub.sharded_subscriptions.len() + args.len();
        if new_count > MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION {
            return vec![Response::error("ERR max sharded subscriptions reached")];
        }

        let mut responses = Vec::with_capacity(args.len());

        for channel in args {
            // Add to local tracking
            self.state.pubsub.sharded_subscriptions.insert(channel.clone());

            // Route to the owning shard only
            let shard_id = shard_for_key(channel, self.num_shards);
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.shard_senders[shard_id].send(ShardMessage::ShardedSubscribe {
                channels: vec![channel.clone()],
                conn_id: self.state.id,
                sender: self.pubsub_tx.clone(),
                response_tx,
            }).await;

            // Build subscription confirmation response
            let count = self.state.pubsub.sharded_subscriptions.len();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"ssubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle SUNSUBSCRIBE command (sharded subscriptions).
    async fn handle_sunsubscribe(&mut self, args: &[Bytes]) -> Vec<Response> {
        // If no args, unsubscribe from all sharded channels
        let channels: Vec<Bytes> = if args.is_empty() {
            self.state.pubsub.sharded_subscriptions.iter().cloned().collect()
        } else {
            args.to_vec()
        };

        // Handle case where no channels to unsubscribe from
        if channels.is_empty() {
            return vec![Response::Array(vec![
                Response::bulk(Bytes::from_static(b"sunsubscribe")),
                Response::null(),
                Response::Integer(0),
            ])];
        }

        let mut responses = Vec::with_capacity(channels.len());

        for channel in channels {
            // Remove from local tracking
            self.state.pubsub.sharded_subscriptions.remove(&channel);

            // Route to the owning shard only
            let shard_id = shard_for_key(&channel, self.num_shards);
            let (response_tx, _response_rx) = oneshot::channel();
            let _ = self.shard_senders[shard_id].send(ShardMessage::ShardedUnsubscribe {
                channels: vec![channel.clone()],
                conn_id: self.state.id,
                response_tx,
            }).await;

            // Build unsubscription confirmation response
            let count = self.state.pubsub.sharded_subscriptions.len();
            responses.push(Response::Array(vec![
                Response::bulk(Bytes::from_static(b"sunsubscribe")),
                Response::bulk(channel.clone()),
                Response::Integer(count as i64),
            ]));
        }

        responses
    }

    /// Handle SPUBLISH command (sharded publish).
    async fn handle_spublish(&self, args: &[Bytes]) -> Response {
        if args.len() != 2 {
            return Response::error("ERR wrong number of arguments for 'spublish' command");
        }

        let channel = &args[0];
        let message = &args[1];

        // Route to the owning shard only
        let shard_id = shard_for_key(channel, self.num_shards);
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self.shard_senders[shard_id].send(ShardMessage::ShardedPublish {
            channel: channel.clone(),
            message: message.clone(),
            response_tx,
        }).await;

        match response_rx.await {
            Ok(count) => Response::Integer(count as i64),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle PUBSUB subcommands.
    async fn handle_pubsub_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'pubsub' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "CHANNELS" => self.handle_pubsub_channels(&args[1..]).await,
            "NUMSUB" => self.handle_pubsub_numsub(&args[1..]).await,
            "NUMPAT" => self.handle_pubsub_numpat().await,
            "SHARDCHANNELS" => self.handle_pubsub_shardchannels(&args[1..]).await,
            "SHARDNUMSUB" => self.handle_pubsub_shardnumsub(&args[1..]).await,
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try PUBSUB HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle PUBSUB CHANNELS [pattern].
    async fn handle_pubsub_channels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::PubSubIntrospection {
                request: IntrospectionRequest::Channels { pattern: pattern.clone() },
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Gather and deduplicate
        let mut all_channels = HashSet::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::Channels(channels)) = rx.await {
                all_channels.extend(channels);
            }
        }

        let mut channels: Vec<_> = all_channels.into_iter().collect();
        channels.sort();
        Response::Array(channels.into_iter().map(Response::bulk).collect())
    }

    /// Handle PUBSUB NUMSUB [channel ...].
    async fn handle_pubsub_numsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::PubSubIntrospection {
                request: IntrospectionRequest::NumSub { channels: channels.clone() },
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Gather and sum counts per channel
        let mut channel_counts: HashMap<Bytes, usize> = HashMap::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::NumSub(counts)) = rx.await {
                for (channel, count) in counts {
                    *channel_counts.entry(channel).or_insert(0) += count;
                }
            }
        }

        // Build response: [channel1, count1, channel2, count2, ...]
        let mut result = Vec::with_capacity(channels.len() * 2);
        for channel in channels {
            let count = channel_counts.get(&channel).copied().unwrap_or(0);
            result.push(Response::bulk(channel));
            result.push(Response::Integer(count as i64));
        }

        Response::Array(result)
    }

    /// Handle PUBSUB NUMPAT.
    async fn handle_pubsub_numpat(&self) -> Response {
        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::PubSubIntrospection {
                request: IntrospectionRequest::NumPat,
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Sum all pattern counts
        let mut total = 0usize;
        for rx in handles {
            if let Ok(IntrospectionResponse::NumPat(count)) = rx.await {
                total += count;
            }
        }

        Response::Integer(total as i64)
    }

    /// Handle PUBSUB SHARDCHANNELS [pattern].
    async fn handle_pubsub_shardchannels(&self, args: &[Bytes]) -> Response {
        let pattern = if args.is_empty() {
            None
        } else {
            Some(GlobPattern::new(args[0].clone()))
        };

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::PubSubIntrospection {
                request: IntrospectionRequest::ShardChannels { pattern: pattern.clone() },
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Gather and deduplicate
        let mut all_channels = HashSet::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::Channels(channels)) = rx.await {
                all_channels.extend(channels);
            }
        }

        let mut channels: Vec<_> = all_channels.into_iter().collect();
        channels.sort();
        Response::Array(channels.into_iter().map(Response::bulk).collect())
    }

    /// Handle PUBSUB SHARDNUMSUB [channel ...].
    async fn handle_pubsub_shardnumsub(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::Array(vec![]);
        }

        let channels: Vec<Bytes> = args.to_vec();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender.send(ShardMessage::PubSubIntrospection {
                request: IntrospectionRequest::ShardNumSub { channels: channels.clone() },
                response_tx,
            }).await;
            handles.push(response_rx);
        }

        // Gather and sum counts per channel
        let mut channel_counts: HashMap<Bytes, usize> = HashMap::new();
        for rx in handles {
            if let Ok(IntrospectionResponse::NumSub(counts)) = rx.await {
                for (channel, count) in counts {
                    *channel_counts.entry(channel).or_insert(0) += count;
                }
            }
        }

        // Build response: [channel1, count1, channel2, count2, ...]
        let mut result = Vec::with_capacity(channels.len() * 2);
        for channel in channels {
            let count = channel_counts.get(&channel).copied().unwrap_or(0);
            result.push(Response::bulk(channel));
            result.push(Response::Integer(count as i64));
        }

        Response::Array(result)
    }

    // =========================================================================
    // Scripting command handlers
    // =========================================================================

    /// Handle EVAL command.
    async fn handle_eval(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'eval' command");
        }

        // Parse arguments
        let script_source = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine target shard
        let target_shard = if keys.is_empty() {
            0 // No keys -> shard 0
        } else {
            let first_shard = shard_for_key(&keys[0], self.num_shards);
            // Check all keys hash to same shard
            for key in &keys[1..] {
                if shard_for_key(key, self.num_shards) != first_shard {
                    return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
                }
            }
            first_shard
        };

        // Send to shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[target_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle EVALSHA command.
    async fn handle_evalsha(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'evalsha' command");
        }

        // Parse arguments
        let script_sha = args[0].clone();
        let numkeys = match std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => return Response::error("ERR value is not an integer or out of range"),
        };

        // Validate we have enough args
        if args.len() < 2 + numkeys {
            return Response::error("ERR Number of keys can't be greater than number of args");
        }

        // Extract keys and argv
        let keys: Vec<Bytes> = args[2..2 + numkeys].to_vec();
        let argv: Vec<Bytes> = args[2 + numkeys..].to_vec();

        // Determine target shard
        let target_shard = if keys.is_empty() {
            0 // No keys -> shard 0
        } else {
            let first_shard = shard_for_key(&keys[0], self.num_shards);
            // Check all keys hash to same shard
            for key in &keys[1..] {
                if shard_for_key(key, self.num_shards) != first_shard {
                    return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
                }
            }
            first_shard
        };

        // Send to shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[target_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT command.
    async fn handle_script(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "LOAD" => self.handle_script_load(&args[1..]).await,
            "EXISTS" => self.handle_script_exists(&args[1..]).await,
            "FLUSH" => self.handle_script_flush(&args[1..]).await,
            "KILL" => self.handle_script_kill().await,
            "HELP" => self.handle_script_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try SCRIPT HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle SCRIPT LOAD.
    async fn handle_script_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|load' command");
        }

        let script_source = args[0].clone();

        // Send to shard 0 (scripts are loaded per-shard, but we return the SHA)
        // In a production system, we'd broadcast to all shards
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScriptLoad {
            script_source,
            response_tx,
        };

        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(sha) => Response::bulk(Bytes::from(sha)),
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT EXISTS.
    async fn handle_script_exists(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'script|exists' command");
        }

        let shas = args.to_vec();

        // Query shard 0 (in production, would need to check the target shard)
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScriptExists {
            shas,
            response_tx,
        };

        if self.shard_senders[0].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(results) => {
                let arr: Vec<Response> = results
                    .into_iter()
                    .map(|exists| Response::Integer(if exists { 1 } else { 0 }))
                    .collect();
                Response::Array(arr)
            }
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Handle SCRIPT FLUSH.
    async fn handle_script_flush(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC|SYNC argument (we ignore it for now)
        let _async_mode = if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            match mode.as_slice() {
                b"ASYNC" => true,
                b"SYNC" => false,
                _ => {
                    return Response::error(
                        "ERR SCRIPT FLUSH only supports ASYNC and SYNC options",
                    )
                }
            }
        } else {
            false
        };

        // Broadcast to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::ScriptFlush { response_tx })
                .await;
            handles.push(response_rx);
        }

        // Wait for all to complete
        for rx in handles {
            let _ = rx.await;
        }

        Response::ok()
    }

    /// Handle SCRIPT KILL.
    async fn handle_script_kill(&self) -> Response {
        // Try to kill on all shards, return first error or success
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let _ = sender
                .send(ShardMessage::ScriptKill { response_tx })
                .await;

            if let Ok(result) = response_rx.await {
                match result {
                    Ok(()) => return Response::ok(),
                    Err(e) if e.contains("NOTBUSY") => continue, // Try next shard
                    Err(e) => return Response::error(e),
                }
            }
        }

        Response::error("NOTBUSY No scripts in execution right now.")
    }

    /// Handle SCRIPT HELP.
    fn handle_script_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(b"SCRIPT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")),
            Response::bulk(Bytes::from_static(b"EXISTS <sha1> [<sha1> ...]")),
            Response::bulk(Bytes::from_static(b"    Return information about the existence of the scripts in the script cache.")),
            Response::bulk(Bytes::from_static(b"FLUSH [ASYNC|SYNC]")),
            Response::bulk(Bytes::from_static(b"    Flush the Lua scripts cache. Defaults to SYNC.")),
            Response::bulk(Bytes::from_static(b"KILL")),
            Response::bulk(Bytes::from_static(b"    Kill the currently executing Lua script.")),
            Response::bulk(Bytes::from_static(b"LOAD <script>")),
            Response::bulk(Bytes::from_static(b"    Load a script into the scripts cache without executing it.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }
}
