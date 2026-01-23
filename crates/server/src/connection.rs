//! Connection handling.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use frogdb_core::{
    generate_latency_graph, persistence::SnapshotCoordinator, shard_for_key, AclManager,
    AuthenticatedUser, ClientHandle, ClientRegistry, CommandCategory, CommandFlags, CommandRegistry,
    GlobPattern, IntrospectionRequest, IntrospectionResponse, KeyAccessType, LatencyEvent,
    MetricsRecorder, PartialResult, PauseMode, PubSubMessage, PubSubSender, ScatterOp,
    SharedFunctionRegistry, ShardMemoryStats, ShardMessage, StreamId, TransactionResult,
    MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION, MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, trace, warn};

use crate::migrate::{MigrateArgs, MigrateClient, MigrateError};
use crate::net::TcpStream;
use crate::runtime_config::ConfigManager;
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

/// Authentication state for a connection.
#[derive(Debug, Clone)]
pub enum AuthState {
    /// Not authenticated yet (default when requirepass is set).
    NotAuthenticated,
    /// Authenticated with a specific user.
    Authenticated(AuthenticatedUser),
}

impl Default for AuthState {
    fn default() -> Self {
        // By default, use the default user with full permissions
        AuthState::Authenticated(AuthenticatedUser::default_user())
    }
}

impl AuthState {
    /// Check if the connection is authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, AuthState::Authenticated(_))
    }

    /// Get the authenticated user, if any.
    pub fn user(&self) -> Option<&AuthenticatedUser> {
        match self {
            AuthState::Authenticated(user) => Some(user),
            AuthState::NotAuthenticated => None,
        }
    }

    /// Get the username.
    pub fn username(&self) -> &str {
        match self {
            AuthState::Authenticated(user) => &user.username,
            AuthState::NotAuthenticated => "(not authenticated)",
        }
    }
}

/// Blocked state for connections waiting on blocking commands.
#[derive(Debug, Clone)]
pub struct BlockedState {
    /// Shard ID where the wait is registered.
    pub shard_id: usize,
    /// Keys the client is waiting on.
    pub keys: Vec<Bytes>,
}

/// Reply mode for CLIENT REPLY command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplyMode {
    /// Normal reply mode (default).
    #[default]
    On,
    /// No replies to client commands.
    Off,
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

    /// Whether HELLO has been received on this connection.
    pub hello_received: bool,

    /// When HELLO was received (for debugging/monitoring).
    pub hello_at: Option<std::time::Instant>,

    /// Client name (from CLIENT SETNAME).
    pub name: Option<Bytes>,

    /// Transaction state for MULTI/EXEC.
    pub transaction: TransactionState,

    /// Pub/Sub state.
    pub pubsub: PubSubState,

    /// Authentication state.
    pub auth: AuthState,

    /// Blocked state for blocking commands (None = not blocked).
    pub blocked: Option<BlockedState>,

    /// Reply mode (from CLIENT REPLY).
    pub reply_mode: ReplyMode,

    /// Skip the next reply (for CLIENT REPLY SKIP).
    pub skip_next_reply: bool,
}

impl ConnectionState {
    fn new(id: u64, addr: SocketAddr, requires_auth: bool) -> Self {
        Self {
            id,
            addr,
            created_at: std::time::Instant::now(),
            protocol_version: ProtocolVersion::default(),
            hello_received: false,
            hello_at: None,
            name: None,
            transaction: TransactionState::default(),
            pubsub: PubSubState::default(),
            auth: if requires_auth {
                AuthState::NotAuthenticated
            } else {
                AuthState::default()
            },
            blocked: None,
            reply_mode: ReplyMode::default(),
            skip_next_reply: false,
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

    /// Client registry for CLIENT commands.
    client_registry: Arc<ClientRegistry>,

    /// Configuration manager for CONFIG commands.
    config_manager: Arc<ConfigManager>,

    /// Client handle (auto-unregisters on drop).
    client_handle: ClientHandle,

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

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// ACL manager for authentication and authorization.
    acl_manager: Arc<AclManager>,

    /// Snapshot coordinator for BGSAVE/LASTSAVE commands.
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Function registry for FUNCTION/FCALL commands.
    function_registry: SharedFunctionRegistry,
}

/// Determine key access type from command flags.
fn key_access_type_for_flags(flags: CommandFlags) -> KeyAccessType {
    if flags.contains(CommandFlags::READONLY) {
        KeyAccessType::Read
    } else if flags.contains(CommandFlags::WRITE) {
        KeyAccessType::Write
    } else {
        // Commands with neither flag (admin commands, etc.) - check both
        KeyAccessType::ReadWrite
    }
}

/// Convert protocol BlockingOp to core BlockingOp.
fn convert_blocking_op(op: frogdb_protocol::BlockingOp) -> frogdb_core::BlockingOp {
    match op {
        frogdb_protocol::BlockingOp::BLPop => frogdb_core::BlockingOp::BLPop,
        frogdb_protocol::BlockingOp::BRPop => frogdb_core::BlockingOp::BRPop,
        frogdb_protocol::BlockingOp::BLMove { dest, src_dir, dest_dir } => {
            frogdb_core::BlockingOp::BLMove {
                dest,
                src_dir: convert_direction(src_dir),
                dest_dir: convert_direction(dest_dir),
            }
        }
        frogdb_protocol::BlockingOp::BLMPop { direction, count } => {
            frogdb_core::BlockingOp::BLMPop {
                direction: convert_direction(direction),
                count,
            }
        }
        frogdb_protocol::BlockingOp::BZPopMin => frogdb_core::BlockingOp::BZPopMin,
        frogdb_protocol::BlockingOp::BZPopMax => frogdb_core::BlockingOp::BZPopMax,
        frogdb_protocol::BlockingOp::BZMPop { min, count } => {
            frogdb_core::BlockingOp::BZMPop { min, count }
        }
        frogdb_protocol::BlockingOp::XRead { after_ids, count } => {
            frogdb_core::BlockingOp::XRead {
                after_ids: after_ids
                    .into_iter()
                    .map(|(ms, seq)| StreamId::new(ms, seq))
                    .collect(),
                count,
            }
        }
        frogdb_protocol::BlockingOp::XReadGroup { group, consumer, noack, count } => {
            frogdb_core::BlockingOp::XReadGroup {
                group,
                consumer,
                noack,
                count,
            }
        }
    }
}

/// Convert protocol Direction to core Direction.
fn convert_direction(dir: frogdb_protocol::Direction) -> frogdb_core::Direction {
    match dir {
        frogdb_protocol::Direction::Left => frogdb_core::Direction::Left,
        frogdb_protocol::Direction::Right => frogdb_core::Direction::Right,
    }
}

/// Commands that have subcommands (container commands in Redis terminology).
const CONTAINER_COMMANDS: &[&str] = &[
    "ACL", "CLIENT", "CONFIG", "CLUSTER", "DEBUG", "MEMORY", "MODULE",
    "OBJECT", "SCRIPT", "SLOWLOG", "XGROUP", "XINFO", "COMMAND", "PUBSUB",
    "FUNCTION", "LATENCY",
];

/// Extract subcommand from args for container commands.
fn extract_subcommand(command: &str, args: &[Bytes]) -> Option<String> {
    if CONTAINER_COMMANDS.iter().any(|c| c.eq_ignore_ascii_case(command)) {
        args.first()
            .map(|a| String::from_utf8_lossy(a).to_uppercase())
    } else {
        None
    }
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
        client_registry: Arc<ClientRegistry>,
        config_manager: Arc<ConfigManager>,
        client_handle: ClientHandle,
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        allow_cross_slot: bool,
        scatter_gather_timeout_ms: u64,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        acl_manager: Arc<AclManager>,
        snapshot_coordinator: Arc<dyn SnapshotCoordinator>,
        function_registry: SharedFunctionRegistry,
    ) -> Self {
        let framed = Framed::new(socket, Resp2);
        let requires_auth = acl_manager.requires_auth();
        let state = ConnectionState::new(conn_id, addr, requires_auth);

        // Create pub/sub channel
        let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

        Self {
            framed,
            state,
            shard_id,
            num_shards,
            registry,
            client_registry,
            config_manager,
            client_handle,
            shard_senders,
            allow_cross_slot,
            scatter_gather_timeout: Duration::from_millis(scatter_gather_timeout_ms),
            pubsub_tx,
            pubsub_rx,
            metrics_recorder,
            acl_manager,
            snapshot_coordinator,
            function_registry,
        }
    }

    /// Send a response to the client, using appropriate encoding based on protocol version.
    ///
    /// For RESP2 connections, uses the standard Framed codec.
    /// For RESP3 connections, manually encodes and writes to the socket.
    async fn send_response(&mut self, response: Response) -> std::io::Result<()> {
        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Use RESP2 encoding via the Framed codec
                let frame = response.to_resp2_frame();
                self.framed
                    .send(frame)
                    .await
                    .map_err(std::io::Error::other)
            }
            ProtocolVersion::Resp3 => {
                // Manually encode RESP3 and write to socket
                let frame = response.to_resp3_frame();
                let mut buf = BytesMut::new();
                redis_protocol::resp3::encode::complete::extend_encode(&mut buf, &frame)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                self.framed.get_mut().write_all(&buf).await?;
                self.framed.get_mut().flush().await
            }
        }
    }

    /// Run the connection handling loop.
    pub async fn run(mut self) -> Result<()> {
        debug!(conn_id = self.state.id, "Connection handler started");

        loop {
            tokio::select! {
                // Check for CLIENT KILL
                _ = self.client_handle.killed() => {
                    debug!(conn_id = self.state.id, "Killed by CLIENT KILL");
                    break;
                }

                // Handle pub/sub messages from shards
                Some(pubsub_msg) = self.pubsub_rx.recv() => {
                    let response = pubsub_msg.to_response_with_protocol(self.state.protocol_version);
                    if self.send_response(response).await.is_err() {
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
                            let _ = self.send_response(Response::error(format!("ERR {}", e))).await;
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
                            let _ = self.send_response(Response::error(format!("ERR {}", e))).await;
                            continue;
                        }
                    };

                    trace!(
                        conn_id = self.state.id,
                        cmd = %String::from_utf8_lossy(&cmd.name),
                        args = cmd.args.len(),
                        "Received command"
                    );

                    // Update last command time for idle tracking
                    self.client_registry.update_last_command(self.state.id);

                    // Handle QUIT specially (also clears transaction state)
                    if cmd.name.eq_ignore_ascii_case(b"QUIT") {
                        // Clear transaction state before quitting
                        self.state.transaction = TransactionState::default();
                        let _ = self.send_response(Response::ok()).await;
                        break;
                    }

                    // Wait if server is paused (for non-exempt commands)
                    self.wait_if_paused(&cmd).await;

                    // Start timing for both metrics and slowlog
                    let start_time = std::time::Instant::now();
                    let cmd_name_for_metrics = String::from_utf8_lossy(&cmd.name).to_uppercase();
                    let timer = frogdb_metrics::CommandTimer::new(
                        cmd_name_for_metrics.clone(),
                        self.metrics_recorder.clone(),
                    );

                    // Route and execute (with transaction and pub/sub handling)
                    let responses = self.route_and_execute_with_transaction(&cmd).await;

                    // Calculate elapsed time in microseconds for slowlog
                    let elapsed_us = start_time.elapsed().as_micros() as u64;

                    // Record metrics - check for errors in responses
                    let has_error = responses.iter().any(|r| matches!(r, Response::Error(_)));
                    if has_error {
                        timer.finish_with_error("command_error");
                    } else {
                        timer.finish();
                    }

                    // Log to slowlog if threshold exceeded and command not exempt
                    self.maybe_log_slow_query(&cmd, elapsed_us).await;

                    // Send response(s) based on reply mode
                    match self.state.reply_mode {
                        ReplyMode::On => {
                            // Check for SKIP mode
                            if self.state.skip_next_reply {
                                self.state.skip_next_reply = false;
                                // Skip sending this response
                            } else {
                                for response in responses {
                                    if self.send_response(response).await.is_err() {
                                        debug!(conn_id = self.state.id, "Failed to send response");
                                        // Break out of the loop on send failure
                                        break;
                                    }
                                }
                            }
                        }
                        ReplyMode::Off => {
                            // Don't send any replies
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
        // Notify if we had any subscriptions
        if self.state.pubsub.in_pubsub_mode() {
            for sender in self.shard_senders.iter() {
                let _ = sender.send(ShardMessage::ConnectionClosed {
                    conn_id: self.state.id,
                }).await;
            }
        }

        // Unregister any blocking waits
        if let Some(ref blocked) = self.state.blocked {
            if let Some(sender) = self.shard_senders.get(blocked.shard_id) {
                let _ = sender.send(ShardMessage::UnregisterWait {
                    conn_id: self.state.id,
                }).await;
            }
        }
    }

    /// Handle a blocking command wait.
    ///
    /// This sends a BlockWait message to the shard and waits for a response.
    async fn handle_blocking_wait(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
        proto_op: frogdb_protocol::BlockingOp,
    ) -> Response {
        use std::time::{Duration, Instant};
        use tokio::sync::oneshot;

        // Convert protocol BlockingOp to core BlockingOp
        let op = convert_blocking_op(proto_op);

        // Determine the target shard - all keys must be on the same shard
        // (this was already validated in the command execute method)
        if keys.is_empty() {
            return Response::error("ERR No keys provided for blocking command");
        }

        let target_shard = shard_for_key(&keys[0], self.num_shards);

        // Calculate deadline
        let deadline = if timeout > 0.0 {
            Some(Instant::now() + Duration::from_secs_f64(timeout))
        } else {
            None // Block forever (until data or disconnect)
        };

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Send BlockWait message to shard
        let sender = match self.shard_senders.get(target_shard) {
            Some(s) => s,
            None => return Response::error("ERR Internal error: invalid shard"),
        };

        if sender.send(ShardMessage::BlockWait {
            conn_id: self.state.id,
            keys: keys.clone(),
            op,
            response_tx,
            deadline,
        }).await.is_err() {
            return Response::error("ERR Internal error: shard unreachable");
        }

        // Update blocked state
        self.state.blocked = Some(BlockedState {
            shard_id: target_shard,
            keys: keys.clone(),
        });

        // Wait for response with timeout
        let result = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::timeout(remaining, response_rx).await
        } else {
            // No timeout - wait indefinitely
            // Use a very long timeout to avoid blocking forever
            tokio::time::timeout(Duration::from_secs(86400), response_rx).await
        };

        // Clear blocked state
        self.state.blocked = None;

        match result {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                // Channel was dropped (shard shutdown or error)
                Response::Null
            }
            Err(_) => {
                // Timeout - send unregister and return null
                if let Some(sender) = self.shard_senders.get(target_shard) {
                    let _ = sender.send(ShardMessage::UnregisterWait {
                        conn_id: self.state.id,
                    }).await;
                }
                Response::Null
            }
        }
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    async fn route_and_execute_with_transaction(&mut self, cmd: &ParsedCommand) -> Vec<Response> {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Handle AUTH command (always allowed, even without authentication)
        if cmd_name_str == "AUTH" {
            return vec![self.handle_auth(&cmd.args).await];
        }

        // Handle HELLO command (always allowed, even without authentication)
        if cmd_name_str == "HELLO" {
            return vec![self.handle_hello(&cmd.args).await];
        }

        // Handle ACL command
        if cmd_name_str == "ACL" {
            return vec![self.handle_acl_command(&cmd.args).await];
        }

        // Check authentication before processing other commands
        // AUTH, QUIT, HELLO, and PING are allowed without authentication
        if !self.state.auth.is_authenticated() && !Self::is_auth_exempt(&cmd_name_str) {
            return vec![Response::error("NOAUTH Authentication required.")];
        }

        // Check command ACL permission (for commands not routed to route_and_execute)
        // Note: ACL command is exempt (users need ACL WHOAMI to check their identity)
        if let Some(user) = self.state.auth.user() {
            let subcommand = extract_subcommand(&cmd_name_str, &cmd.args);
            if cmd_name_str != "ACL" && !user.check_command(&cmd_name_str, subcommand.as_deref()) {
                let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                // Log with subcommand if present
                let log_cmd = if let Some(ref sub) = subcommand {
                    format!("{}|{}", cmd_name_str.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name_str.to_lowercase()
                };
                self.acl_manager.log().log_command_denied(&user.username, &client_info, &log_cmd);
                // Return error with subcommand in message if present
                let err_cmd = if let Some(ref sub) = subcommand {
                    format!("{} {}", cmd_name_str.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name_str.to_lowercase()
                };
                return vec![Response::error(format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    err_cmd
                ))];
            }
        }

        // Check pub/sub mode restrictions
        if self.state.pubsub.in_pubsub_mode() && !Self::is_allowed_in_pubsub_mode(&cmd_name_str) {
            return vec![Response::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd_name_str
            ))];
        }

        // Handle pub/sub commands specially (they return multiple responses)
        match cmd_name_str.as_ref() {
            "SUBSCRIBE" => {
                // Check channel permissions
                if let Some(user) = self.state.auth.user() {
                    for channel in &cmd.args {
                        if !user.check_channel_access(channel) {
                            let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                            let channel_str = String::from_utf8_lossy(channel);
                            self.acl_manager.log().log_channel_denied(
                                &user.username,
                                &client_info,
                                &channel_str,
                            );
                            return vec![Response::error(
                                "NOPERM this user has no permissions to access one of the channels used as arguments"
                            )];
                        }
                    }
                }
                return self.handle_subscribe(&cmd.args).await;
            }
            "UNSUBSCRIBE" => return self.handle_unsubscribe(&cmd.args).await,
            "PSUBSCRIBE" => {
                // Check channel permissions for patterns
                if let Some(user) = self.state.auth.user() {
                    for pattern in &cmd.args {
                        if !user.check_channel_access(pattern) {
                            let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                            let pattern_str = String::from_utf8_lossy(pattern);
                            self.acl_manager.log().log_channel_denied(
                                &user.username,
                                &client_info,
                                &pattern_str,
                            );
                            return vec![Response::error(
                                "NOPERM this user has no permissions to access one of the channels used as arguments"
                            )];
                        }
                    }
                }
                return self.handle_psubscribe(&cmd.args).await;
            }
            "PUNSUBSCRIBE" => return self.handle_punsubscribe(&cmd.args).await,
            "PUBLISH" => {
                // Check channel permission for the target channel
                if let Some(user) = self.state.auth.user() {
                    if !cmd.args.is_empty() {
                        let channel = &cmd.args[0];
                        if !user.check_channel_access(channel) {
                            let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                            let channel_str = String::from_utf8_lossy(channel);
                            self.acl_manager.log().log_channel_denied(
                                &user.username,
                                &client_info,
                                &channel_str,
                            );
                            return vec![Response::error(
                                "NOPERM this user has no permissions to access one of the channels used as arguments"
                            )];
                        }
                    }
                }
                return vec![self.handle_publish(&cmd.args).await];
            }
            "SSUBSCRIBE" => {
                // Check channel permissions for sharded channels
                if let Some(user) = self.state.auth.user() {
                    for channel in &cmd.args {
                        if !user.check_channel_access(channel) {
                            let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                            let channel_str = String::from_utf8_lossy(channel);
                            self.acl_manager.log().log_channel_denied(
                                &user.username,
                                &client_info,
                                &channel_str,
                            );
                            return vec![Response::error(
                                "NOPERM this user has no permissions to access one of the channels used as arguments"
                            )];
                        }
                    }
                }
                return self.handle_ssubscribe(&cmd.args).await;
            }
            "SUNSUBSCRIBE" => return self.handle_sunsubscribe(&cmd.args).await,
            "SPUBLISH" => {
                // Check channel permission for the target sharded channel
                if let Some(user) = self.state.auth.user() {
                    if !cmd.args.is_empty() {
                        let channel = &cmd.args[0];
                        if !user.check_channel_access(channel) {
                            let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                            let channel_str = String::from_utf8_lossy(channel);
                            self.acl_manager.log().log_channel_denied(
                                &user.username,
                                &client_info,
                                &channel_str,
                            );
                            return vec![Response::error(
                                "NOPERM this user has no permissions to access one of the channels used as arguments"
                            )];
                        }
                    }
                }
                return vec![self.handle_spublish(&cmd.args).await];
            }
            "PUBSUB" => return vec![self.handle_pubsub_command(&cmd.args).await],
            "RESET" => return vec![self.handle_reset().await],
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

        // Handle function commands specially
        match cmd_name_str.as_ref() {
            "FCALL" => return vec![self.handle_fcall(&cmd.args, false).await],
            "FCALL_RO" => return vec![self.handle_fcall(&cmd.args, true).await],
            "FUNCTION" => return vec![self.handle_function(&cmd.args).await],
            _ => {}
        }

        // Handle CLIENT commands specially (need access to client registry)
        if cmd_name_str == "CLIENT" {
            return vec![self.handle_client_command(&cmd.args).await];
        }

        // Handle CONFIG commands specially (need access to config manager)
        if cmd_name_str == "CONFIG" {
            return vec![self.handle_config_command(&cmd.args).await];
        }

        // Handle SLOWLOG commands specially (need scatter-gather across shards)
        if cmd_name_str == "SLOWLOG" {
            return vec![self.handle_slowlog_command(&cmd.args).await];
        }

        // Handle MEMORY commands specially (need scatter-gather or key-based routing)
        if cmd_name_str == "MEMORY" {
            return vec![self.handle_memory_command(&cmd.args).await];
        }

        // Handle LATENCY commands specially (need scatter-gather across shards)
        if cmd_name_str == "LATENCY" {
            return vec![self.handle_latency_command(&cmd.args).await];
        }

        // Handle BGSAVE/LASTSAVE commands specially (need snapshot coordinator)
        if cmd_name_str == "BGSAVE" {
            return vec![self.handle_bgsave(&cmd.args)];
        }
        if cmd_name_str == "LASTSAVE" {
            return vec![self.handle_lastsave()];
        }

        // Handle server commands that need special routing
        match cmd_name_str.as_ref() {
            "SCAN" => return vec![self.handle_scan(&cmd.args).await],
            "KEYS" => return vec![self.handle_keys(&cmd.args).await],
            "DBSIZE" => return vec![self.handle_dbsize().await],
            "RANDOMKEY" => return vec![self.handle_randomkey().await],
            "FLUSHDB" => return vec![self.handle_flushdb(&cmd.args).await],
            "FLUSHALL" => return vec![self.handle_flushall(&cmd.args).await],
            "MIGRATE" => return vec![self.handle_migrate(&cmd.args).await],
            "DEBUG" => {
                // Check for DEBUG SLEEP subcommand
                if !cmd.args.is_empty() && cmd.args[0].eq_ignore_ascii_case(b"SLEEP") {
                    return vec![self.handle_debug_sleep(&cmd.args).await];
                }
                // Fall through for other DEBUG subcommands
            }
            "SHUTDOWN" => return vec![self.handle_shutdown(&cmd.args).await],
            "INFO" => return vec![self.handle_info(&cmd.args).await],
            _ => {}
        }

        // If in transaction mode, queue the command instead of executing
        if self.state.transaction.queue.is_some() {
            // Check if it's a blocking command - not allowed in MULTI
            if Self::is_blocking_command(&cmd_name_str) {
                return vec![Response::error(
                    "ERR Blocking commands are not allowed inside a transaction",
                )];
            }
            return vec![self.queue_command(cmd)];
        }

        // Normal execution
        let response = self.route_and_execute(cmd).await;

        // Check if this is a blocking command that needs to wait
        if let Response::BlockingNeeded { keys, timeout, op } = response {
            return vec![self.handle_blocking_wait(keys, timeout, op).await];
        }

        vec![response]
    }

    /// Check if a command is a blocking command.
    fn is_blocking_command(cmd: &str) -> bool {
        matches!(
            cmd,
            "BLPOP" | "BRPOP" | "BLMOVE" | "BLMPOP" | "BZPOPMIN" | "BZPOPMAX" | "BZMPOP" | "BRPOPLPUSH"
        )
    }

    /// Check if a command is allowed in pub/sub mode.
    fn is_allowed_in_pubsub_mode(cmd: &str) -> bool {
        matches!(
            cmd,
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
                | "SSUBSCRIBE" | "SUNSUBSCRIBE" | "PING" | "QUIT" | "RESET"
        )
    }

    /// Check if a command is exempt from authentication requirements.
    fn is_auth_exempt(cmd: &str) -> bool {
        matches!(cmd, "AUTH" | "QUIT" | "HELLO" | "PING")
    }

    /// Get client info string for ACL logging.
    fn client_info_string(&self) -> String {
        format!(
            "id={} addr={} name={}",
            self.state.id,
            self.state.addr,
            self.state.name.as_ref().map(|b| String::from_utf8_lossy(b)).unwrap_or_default()
        )
    }

    /// Handle AUTH command.
    async fn handle_auth(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'auth' command");
        }

        let client_info = self.client_info_string();

        let result = if args.len() == 1 {
            // AUTH <password> - authenticate as default user
            let password = String::from_utf8_lossy(&args[0]);
            self.acl_manager.authenticate_default(&password, &client_info)
        } else {
            // AUTH <username> <password>
            let username = String::from_utf8_lossy(&args[0]);
            let password = String::from_utf8_lossy(&args[1]);
            self.acl_manager.authenticate(&username, &password, &client_info)
        };

        match result {
            Ok(user) => {
                self.state.auth = AuthState::Authenticated(user);
                Response::ok()
            }
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// Handle HELLO command for protocol negotiation.
    ///
    /// Format: HELLO [protover [AUTH username password] [SETNAME clientname]]
    async fn handle_hello(&mut self, args: &[Bytes]) -> Response {
        let mut requested_version: Option<u32> = None;
        let mut auth_username: Option<&Bytes> = None;
        let mut auth_password: Option<&Bytes> = None;
        let mut setname: Option<&Bytes> = None;

        // Parse arguments
        let mut i = 0;
        while i < args.len() {
            if i == 0 {
                // First argument is protocol version
                match std::str::from_utf8(&args[i]).ok().and_then(|s| s.parse::<u32>().ok()) {
                    Some(v) => requested_version = Some(v),
                    None => return Response::error("ERR Protocol version is not an integer or out of range"),
                }
            } else {
                // Check for AUTH or SETNAME
                let arg = args[i].to_ascii_uppercase();
                if arg == b"AUTH".as_slice() {
                    // AUTH username password
                    if i + 2 >= args.len() {
                        return Response::error("ERR Syntax error in HELLO option 'AUTH'");
                    }
                    auth_username = Some(&args[i + 1]);
                    auth_password = Some(&args[i + 2]);
                    i += 2;
                } else if arg == b"SETNAME".as_slice() {
                    // SETNAME clientname
                    if i + 1 >= args.len() {
                        return Response::error("ERR Syntax error in HELLO option 'SETNAME'");
                    }
                    setname = Some(&args[i + 1]);
                    i += 1;
                } else {
                    return Response::error(format!("ERR Syntax error in HELLO option '{}'", String::from_utf8_lossy(&args[i])));
                }
            }
            i += 1;
        }

        // Handle protocol version
        if let Some(version) = requested_version {
            if !(2..=3).contains(&version) {
                // Return NOPROTO error for unsupported versions
                return Response::error("NOPROTO sorry, this protocol version is not supported");
            }

            let new_version = if version == 3 {
                ProtocolVersion::Resp3
            } else {
                ProtocolVersion::Resp2
            };

            // Check for downgrade after HELLO 3
            if self.state.hello_received && self.state.protocol_version.is_resp3() && !new_version.is_resp3() {
                return Response::error("ERR protocol downgrade from RESP3 to RESP2 is not allowed");
            }

            self.state.protocol_version = new_version;
        }

        // Handle AUTH
        if let (Some(username), Some(password)) = (auth_username, auth_password) {
            let client_info = self.client_info_string();
            let username_str = String::from_utf8_lossy(username);
            let password_str = String::from_utf8_lossy(password);

            match self.acl_manager.authenticate(&username_str, &password_str, &client_info) {
                Ok(user) => {
                    self.state.auth = AuthState::Authenticated(user);
                }
                Err(_) => {
                    return Response::error("WRONGPASS invalid username-password pair or user is disabled");
                }
            }
        }

        // Handle SETNAME
        if let Some(name) = setname {
            if name.is_empty() {
                self.state.name = None;
                self.client_registry.update_name(self.state.id, None);
            } else {
                self.state.name = Some(name.clone());
                self.client_registry.update_name(self.state.id, Some(name.clone()));
            }
        }

        // Mark HELLO as received
        self.state.hello_received = true;
        self.state.hello_at = Some(std::time::Instant::now());

        // Build server info response
        self.build_hello_response()
    }

    /// Build the HELLO response with server info.
    fn build_hello_response(&self) -> Response {
        // Server info fields
        let server = Response::bulk(Bytes::from_static(b"server"));
        let server_val = Response::bulk(Bytes::from_static(b"frogdb"));

        let version = Response::bulk(Bytes::from_static(b"version"));
        let version_val = Response::bulk(Bytes::from(env!("CARGO_PKG_VERSION")));

        let proto = Response::bulk(Bytes::from_static(b"proto"));
        let proto_val = Response::Integer(if self.state.protocol_version.is_resp3() { 3 } else { 2 });

        let id = Response::bulk(Bytes::from_static(b"id"));
        let id_val = Response::Integer(self.state.id as i64);

        let mode = Response::bulk(Bytes::from_static(b"mode"));
        let mode_val = Response::bulk(Bytes::from_static(b"standalone"));

        let role = Response::bulk(Bytes::from_static(b"role"));
        let role_val = Response::bulk(Bytes::from_static(b"master"));

        let modules = Response::bulk(Bytes::from_static(b"modules"));
        let modules_val = Response::Array(vec![]);

        if self.state.protocol_version.is_resp3() {
            // Return as Map for RESP3
            Response::Map(vec![
                (server, server_val),
                (version, version_val),
                (proto, proto_val),
                (id, id_val),
                (mode, mode_val),
                (role, role_val),
                (modules, modules_val),
            ])
        } else {
            // Return as flat array for RESP2
            Response::Array(vec![
                server, server_val,
                version, version_val,
                proto, proto_val,
                id, id_val,
                mode, mode_val,
                role, role_val,
                modules, modules_val,
            ])
        }
    }

    /// Handle ACL command and subcommands.
    async fn handle_acl_command(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl' command");
        }

        let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
        let subcmd_args = &args[1..];

        match subcmd.as_str() {
            "WHOAMI" => self.handle_acl_whoami(),
            "LIST" => self.handle_acl_list(),
            "USERS" => self.handle_acl_users(),
            "GETUSER" => self.handle_acl_getuser(subcmd_args),
            "SETUSER" => self.handle_acl_setuser(subcmd_args),
            "DELUSER" => self.handle_acl_deluser(subcmd_args),
            "CAT" => self.handle_acl_cat(subcmd_args),
            "GENPASS" => self.handle_acl_genpass(subcmd_args),
            "LOG" => self.handle_acl_log(subcmd_args),
            "SAVE" => self.handle_acl_save(),
            "LOAD" => self.handle_acl_load(),
            "HELP" => self.handle_acl_help(),
            _ => Response::error(format!("ERR Unknown subcommand or wrong number of arguments for '{}'", subcmd)),
        }
    }

    /// ACL WHOAMI - return current username.
    fn handle_acl_whoami(&self) -> Response {
        Response::bulk(Bytes::from(self.state.auth.username().to_string()))
    }

    /// ACL LIST - list all users with their ACL rules.
    fn handle_acl_list(&self) -> Response {
        let users = self.acl_manager.list_users_detailed();
        Response::Array(users.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// ACL USERS - list all usernames.
    fn handle_acl_users(&self) -> Response {
        let users = self.acl_manager.list_users();
        Response::Array(users.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
    }

    /// ACL GETUSER <username> - get user info.
    fn handle_acl_getuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|getuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        match self.acl_manager.get_user(&username) {
            Some(user) => {
                let info = user.to_getuser_info();
                let mut result = Vec::new();
                for (key, value) in info {
                    result.push(Response::bulk(Bytes::from(key)));
                    match value {
                        frogdb_core::acl::user::UserInfoValue::String(s) => {
                            result.push(Response::bulk(Bytes::from(s)));
                        }
                        frogdb_core::acl::user::UserInfoValue::StringArray(arr) => {
                            result.push(Response::Array(
                                arr.into_iter()
                                    .map(|s| Response::bulk(Bytes::from(s)))
                                    .collect(),
                            ));
                        }
                    }
                }
                Response::Array(result)
            }
            None => Response::null(),
        }
    }

    /// ACL SETUSER <username> [rules...] - create or modify user.
    fn handle_acl_setuser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|setuser' command");
        }

        let username = String::from_utf8_lossy(&args[0]);
        let rules: Vec<&str> = args[1..]
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.set_user(&username, &rules) {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL DELUSER <username> [...] - delete users.
    fn handle_acl_deluser(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'acl|deluser' command");
        }

        let usernames: Vec<&str> = args
            .iter()
            .map(|b| std::str::from_utf8(b).unwrap_or(""))
            .collect();

        match self.acl_manager.delete_users(&usernames) {
            Ok(count) => Response::Integer(count as i64),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL CAT [category] - list categories or commands in category.
    fn handle_acl_cat(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            // List all categories
            let categories: Vec<Response> = CommandCategory::all()
                .iter()
                .map(|c| Response::bulk(Bytes::from(c.name())))
                .collect();
            Response::Array(categories)
        } else {
            // List commands in category
            let category_name = String::from_utf8_lossy(&args[0]);
            match CommandCategory::parse(&category_name) {
                Some(category) => {
                    let commands: Vec<Response> = category
                        .commands()
                        .iter()
                        .map(|c| Response::bulk(Bytes::from(*c)))
                        .collect();
                    Response::Array(commands)
                }
                None => Response::error(format!("ERR Unknown ACL category '{}'", category_name)),
            }
        }
    }

    /// ACL GENPASS [bits] - generate secure random password.
    fn handle_acl_genpass(&self, args: &[Bytes]) -> Response {
        let bits = if args.is_empty() {
            256
        } else {
            match String::from_utf8_lossy(&args[0]).parse::<u32>() {
                Ok(b) if b > 0 => b,
                _ => return Response::error("ERR ACL GENPASS argument must be a positive integer"),
            }
        };

        let password = frogdb_core::generate_password(bits);
        Response::bulk(Bytes::from(password))
    }

    /// ACL LOG [count|RESET] - view or reset security log.
    fn handle_acl_log(&self, args: &[Bytes]) -> Response {
        if !args.is_empty() {
            let arg = String::from_utf8_lossy(&args[0]).to_uppercase();
            if arg == "RESET" {
                self.acl_manager.log().reset();
                return Response::ok();
            }
        }

        let count = if args.is_empty() {
            None
        } else {
            String::from_utf8_lossy(&args[0]).parse::<usize>().ok()
        };

        let entries = self.acl_manager.log().get(count);
        let result: Vec<Response> = entries
            .into_iter()
            .map(|entry| {
                let fields = entry.to_resp_fields();
                let mut arr = Vec::new();
                for (key, value) in fields {
                    arr.push(Response::bulk(Bytes::from(key)));
                    match value {
                        frogdb_core::acl::log::AclLogValue::String(s) => {
                            arr.push(Response::bulk(Bytes::from(s)));
                        }
                        frogdb_core::acl::log::AclLogValue::Integer(i) => {
                            arr.push(Response::Integer(i));
                        }
                        frogdb_core::acl::log::AclLogValue::Float(f) => {
                            arr.push(Response::bulk(Bytes::from(format!("{:.6}", f))));
                        }
                    }
                }
                Response::Array(arr)
            })
            .collect();

        Response::Array(result)
    }

    /// ACL SAVE - save ACL to file.
    fn handle_acl_save(&self) -> Response {
        match self.acl_manager.save() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL LOAD - load ACL from file.
    fn handle_acl_load(&self) -> Response {
        match self.acl_manager.load() {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// ACL HELP - show help.
    fn handle_acl_help(&self) -> Response {
        let help = vec![
            "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "CAT [<category>]",
            "    List all commands that belong to <category>, or all command categories",
            "    when no category is specified.",
            "DELUSER <username> [<username> ...]",
            "    Delete a list of users.",
            "GENPASS [<bits>]",
            "    Generate a secure random password. The optional `bits` argument specifies",
            "    the amount of bits for the password; default is 256.",
            "GETUSER <username>",
            "    Get the user details.",
            "LIST",
            "    List all users in ACL format.",
            "LOAD",
            "    Reload users from the ACL file.",
            "LOG [<count>|RESET]",
            "    List latest ACL security events, or RESET to clear log.",
            "SAVE",
            "    Save the current ACL to file.",
            "SETUSER <username> [<property> [<property> ...]]",
            "    Create or modify a user with the specified properties.",
            "USERS",
            "    List all usernames.",
            "WHOAMI",
            "    Return the current connection username.",
            "HELP",
            "    Print this help.",
        ];
        Response::Array(help.into_iter().map(|s| Response::bulk(Bytes::from(s))).collect())
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
            protocol_version: self.state.protocol_version,
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

    /// Handle RESET command - reset connection to initial state.
    /// This exits pub/sub mode, clears transaction state, and resets protocol to RESP2.
    async fn handle_reset(&mut self) -> Response {
        // 1. Exit pub/sub mode - unsubscribe from all channels
        if self.state.pubsub.in_pubsub_mode() {
            // Clear local subscription tracking
            self.state.pubsub.subscriptions.clear();
            self.state.pubsub.patterns.clear();
            self.state.pubsub.sharded_subscriptions.clear();

            // Notify all shards to remove this connection's subscriptions
            for sender in self.shard_senders.iter() {
                let _ = sender.send(ShardMessage::ConnectionClosed {
                    conn_id: self.state.id,
                }).await;
            }
        }

        // 2. Clear transaction state (abort any MULTI in progress)
        self.state.transaction = TransactionState::default();

        // 3. Reset protocol to RESP2 (per Redis behavior)
        self.state.protocol_version = ProtocolVersion::Resp2;

        // 4. Clear client name
        self.state.name = None;

        // Return RESET acknowledgment
        Response::Simple(Bytes::from_static(b"RESET"))
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

        // Check key permissions with command context
        // For selectors to work correctly, we must check that BOTH the command
        // AND the key are allowed within the same permission context
        if let Some(user) = self.state.auth.user() {
            if !keys.is_empty() {
                let access_type = key_access_type_for_flags(handler.flags());
                let cmd_name = handler.name();
                let subcommand = extract_subcommand(cmd_name, &cmd.args);
                for key in &keys {
                    if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
                        return Response::error(
                            "NOPERM this user has no permissions to access one of the keys used as arguments"
                        );
                    }
                }
            }
        }

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

        // Check key permissions with command context
        // For selectors to work correctly, we must check that BOTH the command
        // AND the key are allowed within the same permission context
        if let Some(user) = self.state.auth.user() {
            if !keys.is_empty() {
                let access_type = key_access_type_for_flags(handler.flags());
                let subcommand = extract_subcommand(&cmd_name_str, &cmd.args);
                for key in &keys {
                    if !user.check_command_with_key(&cmd_name_str, subcommand.as_deref(), key, access_type) {
                        let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                        let key_str = String::from_utf8_lossy(key);
                        self.acl_manager.log().log_key_denied(
                            &user.username,
                            &client_info,
                            &key_str,
                        );
                        return Response::error(
                            "NOPERM this user has no permissions to access one of the keys used as arguments"
                        );
                    }
                }
            }
        }

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

        // Special handling for COPY - it's a two-phase operation (read + write)
        if cmd_name_str == "COPY" {
            return self.execute_cross_shard_copy(&cmd.args).await;
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
            ScatterOp::Del | ScatterOp::Unlink | ScatterOp::Exists | ScatterOp::Touch | ScatterOp::DbSize => {
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
            ScatterOp::Keys { .. } => {
                // Collect all keys from all shards
                let mut all_keys: Vec<Bytes> = shard_results
                    .values()
                    .flat_map(|m| m.keys().cloned())
                    .collect();
                all_keys.sort();
                Response::Array(all_keys.into_iter().map(Response::bulk).collect())
            }
            ScatterOp::FlushDb => {
                // FlushDb returns OK
                Response::ok()
            }
            ScatterOp::Scan { .. } => {
                // Scan results are handled specially in handle_scan
                Response::error("ERR SCAN scatter-gather not supported through this path")
            }
            ScatterOp::Copy { .. } | ScatterOp::CopySet { .. } => {
                // These are handled specially in execute_cross_shard_copy
                Response::error("ERR COPY scatter-gather not supported through this path")
            }
            ScatterOp::RandomKey => {
                // This is handled specially in handle_randomkey
                Response::error("ERR RANDOMKEY scatter-gather not supported through this path")
            }
            ScatterOp::Dump => {
                // This is handled specially in handle_migrate
                Response::error("ERR DUMP scatter-gather not supported through this path")
            }
        }
    }

    /// Execute a cross-shard COPY operation.
    /// This is a two-phase operation: read from source shard, write to destination shard.
    async fn execute_cross_shard_copy(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'copy' command");
        }

        let source = &args[0];
        let dest = &args[1];

        // Parse optional arguments
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"REPLACE" => {
                    replace = true;
                    i += 1;
                }
                b"DB" => {
                    // DB option is accepted but ignored
                    if i + 1 >= args.len() {
                        return Response::error("ERR DB requires an argument");
                    }
                    tracing::warn!("COPY DB option not supported, ignoring");
                    i += 2; // Skip DB and its argument
                }
                _ => {
                    return Response::error(format!(
                        "ERR Unknown option: {}",
                        String::from_utf8_lossy(&arg)
                    ));
                }
            }
        }

        let source_shard = shard_for_key(source, self.num_shards);
        let dest_shard = shard_for_key(dest, self.num_shards);

        // Phase 1: Read from source shard using ScatterOp::Copy
        let (tx1, rx1) = oneshot::channel();
        let copy_request = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![source.clone()],
            operation: ScatterOp::Copy {
                source_key: source.clone(),
            },
            response_tx: tx1,
        };

        if self.shard_senders[source_shard].send(copy_request).await.is_err() {
            return Response::error("ERR source shard unavailable");
        }

        // Await response from source shard
        let source_result = match tokio::time::timeout(self.scatter_gather_timeout, rx1).await {
            Ok(Ok(partial)) => partial,
            Ok(Err(_)) => return Response::error("ERR source shard dropped request"),
            Err(_) => return Response::error("ERR scatter-gather timeout"),
        };

        // Parse the source shard response
        let source_data = source_result.results.into_iter().next();
        let (value_type, value_data, expiry_ms) = match source_data {
            Some((_, Response::Array(arr))) if arr.len() == 3 => {
                // Extract type, data, and expiry from the response
                let type_bytes = match &arr[0] {
                    Response::Bulk(Some(b)) => b.clone(),
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                let data_bytes = match &arr[1] {
                    Response::Bulk(Some(b)) => b.clone(),
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                let expiry = match &arr[2] {
                    Response::Integer(ms) => Some(*ms),
                    Response::Null | Response::Bulk(None) => None,
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                (type_bytes, data_bytes, expiry)
            }
            Some((_, Response::Null)) | Some((_, Response::Bulk(None))) => {
                // Source key doesn't exist
                return Response::Integer(0);
            }
            _ => return Response::error("ERR invalid response from source shard"),
        };

        // Phase 2: Write to destination shard using ScatterOp::CopySet
        let (tx2, rx2) = oneshot::channel();
        let copy_set_request = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![dest.clone()],
            operation: ScatterOp::CopySet {
                dest_key: dest.clone(),
                value_type,
                value_data,
                expiry_ms,
                replace,
            },
            response_tx: tx2,
        };

        if self.shard_senders[dest_shard].send(copy_set_request).await.is_err() {
            return Response::error("ERR destination shard unavailable");
        }

        // Await response from destination shard
        let dest_result = match tokio::time::timeout(self.scatter_gather_timeout, rx2).await {
            Ok(Ok(partial)) => partial,
            Ok(Err(_)) => return Response::error("ERR destination shard dropped request"),
            Err(_) => return Response::error("ERR scatter-gather timeout"),
        };

        // Return the response from the destination shard
        match dest_result.results.into_iter().next() {
            Some((_, response)) => response,
            None => Response::error("ERR no response from destination shard"),
        }
    }

    /// Handle MIGRATE command - migrate keys to another Redis-compatible server.
    async fn handle_migrate(&self, args: &[Bytes]) -> Response {
        // Parse arguments
        let parsed = match MigrateArgs::parse(args) {
            Ok(p) => p,
            Err(e) => return Response::error(e),
        };

        // Check if we have any keys to migrate
        if parsed.keys.is_empty() {
            return Response::Simple(Bytes::from_static(b"NOKEY"));
        }

        // Group keys by shard
        let mut shard_keys: HashMap<usize, Vec<Bytes>> = HashMap::new();
        for key in &parsed.keys {
            let shard_id = shard_for_key(key, self.num_shards);
            shard_keys.entry(shard_id).or_default().push(key.clone());
        }

        let txid = next_txid();
        let timeout_dur = Duration::from_millis(parsed.timeout_ms);

        // Scatter-gather DUMP from all shards
        let mut handles: Vec<(usize, oneshot::Receiver<PartialResult>)> = Vec::new();

        for (shard_id, keys) in &shard_keys {
            let (tx, rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: txid,
                keys: keys.clone(),
                operation: ScatterOp::Dump,
                response_tx: tx,
            };

            if self.shard_senders[*shard_id].send(msg).await.is_err() {
                return Response::error("IOERR error accessing local shard");
            }
            handles.push((*shard_id, rx));
        }

        // Collect serialized dumps from all shards
        let mut dumps: Vec<(Bytes, Vec<u8>)> = Vec::new();

        for (_shard_id, rx) in handles {
            let partial = match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => return Response::error("IOERR error accessing local shard"),
                Err(_) => return Response::error("IOERR timeout reading local keys"),
            };

            for (key, resp) in partial.results {
                if let Response::Bulk(Some(data)) = resp {
                    dumps.push((key, data.to_vec()));
                }
                // Skip keys that don't exist (Response::Null)
            }
        }

        // If no keys actually exist, return NOKEY
        if dumps.is_empty() {
            return Response::Simple(Bytes::from_static(b"NOKEY"));
        }

        // Connect to target server
        let mut client = match MigrateClient::connect(&parsed.host, parsed.port, timeout_dur).await {
            Ok(c) => c,
            Err(MigrateError::Timeout) => return Response::error("IOERR timeout connecting to target"),
            Err(e) => return Response::error(format!("IOERR error connecting to target: {}", e)),
        };

        // Authenticate if needed
        if let Some(ref auth) = parsed.auth {
            if let Err(e) = client.auth(&auth.password, auth.username.as_deref()).await {
                return Response::error(format!("IOERR authentication failed: {}", e));
            }
        }

        // Select destination database
        if parsed.dest_db != 0 {
            if let Err(e) = client.select_db(parsed.dest_db).await {
                return Response::error(format!("IOERR error selecting database: {}", e));
            }
        }

        // RESTORE each key on target
        for (key, data) in &dumps {
            // Extract TTL from serialized data (stored in header bytes 2-10 as i64 milliseconds)
            let ttl = if data.len() >= 10 {
                let expires_ms = i64::from_le_bytes(data[2..10].try_into().unwrap_or([0; 8]));
                if expires_ms > 0 {
                    // Convert absolute timestamp to relative TTL
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    let remaining = expires_ms - now_ms;
                    if remaining > 0 { remaining } else { 0 }
                } else {
                    0 // No expiry
                }
            } else {
                0
            };

            if let Err(e) = client.restore(key, ttl, data, parsed.replace).await {
                return Response::error(format!("IOERR error restoring key on target: {}", e));
            }
        }

        // Delete source keys (unless COPY option was specified)
        if !parsed.copy {
            // Group keys by shard for deletion
            let mut delete_shard_keys: HashMap<usize, Vec<Bytes>> = HashMap::new();
            for (key, _) in &dumps {
                let shard_id = shard_for_key(key, self.num_shards);
                delete_shard_keys.entry(shard_id).or_default().push(key.clone());
            }

            let delete_txid = next_txid();
            let mut delete_handles: Vec<oneshot::Receiver<PartialResult>> = Vec::new();

            for (shard_id, keys) in delete_shard_keys {
                let (tx, rx) = oneshot::channel();
                let msg = ShardMessage::ScatterRequest {
                    request_id: delete_txid,
                    keys,
                    operation: ScatterOp::Del,
                    response_tx: tx,
                };

                if self.shard_senders[shard_id].send(msg).await.is_err() {
                    // Log but don't fail - keys already migrated
                    tracing::warn!("Failed to delete source key after MIGRATE");
                }
                delete_handles.push(rx);
            }

            // Wait for deletes to complete (but don't fail if they time out)
            for rx in delete_handles {
                let _ = tokio::time::timeout(self.scatter_gather_timeout, rx).await;
            }
        }

        Response::ok()
    }

    /// Execute command on a specific shard.
    async fn execute_on_shard(&self, shard_id: usize, cmd: &ParsedCommand) -> Response {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ShardMessage::Execute {
            command: cmd.clone(),
            conn_id: self.state.id,
            txid: None, // Single-shard operations don't need txid
            protocol_version: self.state.protocol_version,
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
            protocol_version: self.state.protocol_version,
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
            protocol_version: self.state.protocol_version,
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

    // =========================================================================
    // Function command handlers
    // =========================================================================

    /// Handle FCALL and FCALL_RO commands.
    async fn handle_fcall(&self, args: &[Bytes], read_only: bool) -> Response {
        let cmd_name = if read_only { "fcall_ro" } else { "fcall" };

        if args.len() < 2 {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd_name
            ));
        }

        // Parse arguments: function numkeys [key ...] [arg ...]
        let function_name = args[0].clone();
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
        let msg = ShardMessage::FunctionCall {
            function_name,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            read_only,
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

    /// Handle FUNCTION command with subcommands.
    async fn handle_function(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "LOAD" => self.handle_function_load(&args[1..]).await,
            "LIST" => self.handle_function_list(&args[1..]),
            "DELETE" => self.handle_function_delete(&args[1..]),
            "FLUSH" => self.handle_function_flush(&args[1..]),
            "STATS" => self.handle_function_stats(),
            "DUMP" => self.handle_function_dump(),
            "RESTORE" => self.handle_function_restore(&args[1..]).await,
            "KILL" => self.handle_function_kill().await,
            "HELP" => self.handle_function_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle FUNCTION LOAD [REPLACE] code.
    async fn handle_function_load(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|load' command");
        }

        // Check for REPLACE option
        let (replace, code) = if args.len() >= 2 && args[0].to_ascii_uppercase() == b"REPLACE".as_slice() {
            (true, &args[1])
        } else {
            (false, &args[0])
        };

        let code_str = match std::str::from_utf8(code) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library code must be valid UTF-8"),
        };

        // Load the library
        let library = match frogdb_core::load_library(code_str) {
            Ok(lib) => lib,
            Err(e) => return Response::error(e.to_string()),
        };

        let library_name = library.name.clone();

        // Register in the global registry
        {
            let mut registry = self.function_registry.write().unwrap();
            match registry.load_library(library, replace) {
                Ok(_) => {}
                Err(e) => return Response::error(e.to_string()),
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::bulk(Bytes::from(library_name))
    }

    /// Handle FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE].
    fn handle_function_list(&self, args: &[Bytes]) -> Response {
        let mut pattern: Option<&str> = None;
        let mut with_code = false;

        let mut i = 0;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"LIBRARYNAME" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    pattern = std::str::from_utf8(&args[i]).ok();
                }
                b"WITHCODE" => {
                    with_code = true;
                }
                _ => {
                    return Response::error(format!(
                        "ERR unknown option '{}'",
                        String::from_utf8_lossy(&args[i])
                    ));
                }
            }
            i += 1;
        }

        let registry = self.function_registry.read().unwrap();
        let libraries = registry.list_libraries(pattern);

        let mut result = Vec::new();
        for lib in libraries {
            let mut lib_info = Vec::new();

            // library_name
            lib_info.push(Response::bulk(Bytes::from_static(b"library_name")));
            lib_info.push(Response::bulk(Bytes::from(lib.name.clone())));

            // engine
            lib_info.push(Response::bulk(Bytes::from_static(b"engine")));
            lib_info.push(Response::bulk(Bytes::from(lib.engine.clone())));

            // functions
            lib_info.push(Response::bulk(Bytes::from_static(b"functions")));
            let mut funcs = Vec::new();
            for func in lib.functions.values() {
                let mut func_info = Vec::new();

                func_info.push(Response::bulk(Bytes::from_static(b"name")));
                func_info.push(Response::bulk(Bytes::from(func.name.clone())));

                func_info.push(Response::bulk(Bytes::from_static(b"flags")));
                let flags: Vec<Response> = func
                    .flags
                    .to_strings()
                    .into_iter()
                    .map(|f| Response::bulk(Bytes::from(f)))
                    .collect();
                func_info.push(Response::Array(flags));

                if let Some(ref desc) = func.description {
                    func_info.push(Response::bulk(Bytes::from_static(b"description")));
                    func_info.push(Response::bulk(Bytes::from(desc.clone())));
                }

                funcs.push(Response::Array(func_info));
            }
            lib_info.push(Response::Array(funcs));

            // code (if requested)
            if with_code {
                lib_info.push(Response::bulk(Bytes::from_static(b"library_code")));
                lib_info.push(Response::bulk(Bytes::from(lib.code.clone())));
            }

            result.push(Response::Array(lib_info));
        }

        Response::Array(result)
    }

    /// Handle FUNCTION DELETE library-name.
    fn handle_function_delete(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|delete' command");
        }

        let library_name = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR library name must be valid UTF-8"),
        };

        {
            let mut registry = self.function_registry.write().unwrap();
            match registry.delete_library(library_name) {
                Ok(()) => {}
                Err(e) => return Response::error(e.to_string()),
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::ok()
    }

    /// Handle FUNCTION FLUSH [ASYNC|SYNC].
    fn handle_function_flush(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC|SYNC argument (we ignore it for now)
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error(
                    "ERR FUNCTION FLUSH only supports ASYNC and SYNC options",
                );
            }
        }

        {
            let mut registry = self.function_registry.write().unwrap();
            registry.flush();
        }

        // Persist to disk (empty state)
        self.persist_functions();

        Response::ok()
    }

    /// Handle FUNCTION STATS.
    fn handle_function_stats(&self) -> Response {
        let registry = self.function_registry.read().unwrap();
        let stats = registry.stats();

        let mut result = Vec::new();

        // running_script
        result.push(Response::bulk(Bytes::from_static(b"running_script")));
        if let Some(ref running) = stats.running_function {
            let mut script_info = Vec::new();
            script_info.push(Response::bulk(Bytes::from_static(b"name")));
            script_info.push(Response::bulk(Bytes::from(running.name.clone())));
            script_info.push(Response::bulk(Bytes::from_static(b"command")));
            script_info.push(Response::bulk(Bytes::from_static(b"fcall")));
            script_info.push(Response::bulk(Bytes::from_static(b"duration_ms")));
            script_info.push(Response::Integer(running.duration_ms as i64));
            result.push(Response::Array(script_info));
        } else {
            result.push(Response::Null);
        }

        // engines
        result.push(Response::bulk(Bytes::from_static(b"engines")));
        let mut engines = Vec::new();
        let mut lua_info = Vec::new();
        lua_info.push(Response::bulk(Bytes::from_static(b"libraries_count")));
        lua_info.push(Response::Integer(stats.library_count as i64));
        lua_info.push(Response::bulk(Bytes::from_static(b"functions_count")));
        lua_info.push(Response::Integer(stats.function_count as i64));
        engines.push(Response::bulk(Bytes::from_static(b"LUA")));
        engines.push(Response::Array(lua_info));
        result.push(Response::Array(engines));

        Response::Array(result)
    }

    /// Handle FUNCTION DUMP.
    fn handle_function_dump(&self) -> Response {
        let registry = self.function_registry.read().unwrap();
        let dump = frogdb_core::dump_libraries(&registry);
        Response::bulk(Bytes::from(dump))
    }

    /// Handle FUNCTION RESTORE payload [APPEND|REPLACE|FLUSH].
    async fn handle_function_restore(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'function|restore' command");
        }

        let payload = &args[0];
        let policy = if args.len() > 1 {
            match frogdb_core::RestorePolicy::from_str(&String::from_utf8_lossy(&args[1])) {
                Ok(p) => p,
                Err(e) => return Response::error(e.to_string()),
            }
        } else {
            frogdb_core::RestorePolicy::Append
        };

        // Parse the dump
        let libraries = match frogdb_core::restore_libraries(payload) {
            Ok(libs) => libs,
            Err(e) => return Response::error(e.to_string()),
        };

        // Apply based on policy
        {
            let mut registry = self.function_registry.write().unwrap();

            if policy == frogdb_core::RestorePolicy::Flush {
                registry.flush();
            }

            let replace = policy == frogdb_core::RestorePolicy::Replace;

            for (name, code) in libraries {
                let library = match frogdb_core::load_library(&code) {
                    Ok(lib) => lib,
                    Err(e) => {
                        return Response::error(format!(
                            "ERR Failed to load library '{}': {}",
                            name, e
                        ))
                    }
                };

                if let Err(e) = registry.load_library(library, replace) {
                    return Response::error(format!(
                        "ERR Failed to restore library '{}': {}",
                        name, e
                    ));
                }
            }
        }

        // Persist to disk
        self.persist_functions();

        Response::ok()
    }

    /// Handle FUNCTION HELP.
    fn handle_function_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(b"FUNCTION <subcommand> [<arg> [value] ...]. Subcommands are:")),
            Response::bulk(Bytes::from_static(b"DELETE <library-name>")),
            Response::bulk(Bytes::from_static(b"    Delete a library and all its functions.")),
            Response::bulk(Bytes::from_static(b"DUMP")),
            Response::bulk(Bytes::from_static(b"    Return a serialized payload of loaded libraries.")),
            Response::bulk(Bytes::from_static(b"FLUSH [ASYNC|SYNC]")),
            Response::bulk(Bytes::from_static(b"    Delete all libraries.")),
            Response::bulk(Bytes::from_static(b"KILL")),
            Response::bulk(Bytes::from_static(b"    Kill a currently running read-only function.")),
            Response::bulk(Bytes::from_static(b"LIST [LIBRARYNAME pattern] [WITHCODE]")),
            Response::bulk(Bytes::from_static(b"    List all libraries, optionally filtered by name pattern.")),
            Response::bulk(Bytes::from_static(b"LOAD [REPLACE] <library-code>")),
            Response::bulk(Bytes::from_static(b"    Create a new library with the given code.")),
            Response::bulk(Bytes::from_static(b"RESTORE <serialized-payload> [APPEND|REPLACE|FLUSH]")),
            Response::bulk(Bytes::from_static(b"    Restore libraries from the serialized payload.")),
            Response::bulk(Bytes::from_static(b"STATS")),
            Response::bulk(Bytes::from_static(b"    Return information about running scripts and engines.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }

    /// Persist functions to disk if persistence is enabled.
    fn persist_functions(&self) {
        if !self.config_manager.persistence_enabled() {
            return;
        }

        let path = PathBuf::from(self.config_manager.data_dir()).join("functions.fdb");
        let registry = self.function_registry.read().unwrap();

        if let Err(e) = frogdb_core::save_to_file(&registry, &path) {
            warn!(error = %e, "Failed to persist functions to disk");
        }
    }

    /// Handle FUNCTION KILL - terminate a running read-only function.
    ///
    /// FUNCTION KILL uses the same mechanism as SCRIPT KILL since functions
    /// execute using the same Lua script executor. It will only kill functions
    /// that were called via FCALL_RO (read-only execution).
    async fn handle_function_kill(&self) -> Response {
        // Send ScriptKill to all shards (only one can be running a script at a time per shard)
        // We check all shards since we don't track which shard is running the function
        let mut responses = Vec::with_capacity(self.num_shards);

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScriptKill { response_tx };

            if sender.send(msg).await.is_err() {
                continue;
            }

            if let Ok(result) = response_rx.await {
                responses.push(result);
            }
        }

        // Check if any shard was running a script
        for response in responses {
            match response {
                Ok(()) => return Response::ok(),
                Err(e) if e.contains("UNKILLABLE") => {
                    return Response::error("UNKILLABLE The busy script was not running in read-only mode.")
                }
                Err(_) => {} // NOTBUSY - continue checking other shards
            }
        }

        // No shard had a running script
        Response::error("NOTBUSY No scripts in execution right now.")
    }

    // =========================================================================
    // Key iteration and server command handlers
    // =========================================================================

    /// Handle SCAN command with cursor-based iteration across shards.
    ///
    /// Cursor format: shard_id (16 bits) | position (48 bits)
    async fn handle_scan(&self, args: &[Bytes]) -> Response {
        use crate::commands::scan::cursor;
        use frogdb_core::KeyType;

        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'scan' command");
        }

        // Parse cursor
        let cursor_str = match std::str::from_utf8(&args[0]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid cursor"),
        };
        let encoded_cursor: u64 = match cursor_str.parse() {
            Ok(c) => c,
            Err(_) => return Response::error("ERR invalid cursor"),
        };

        // Decode cursor to get shard_id and position
        let (shard_id, position) = cursor::decode(encoded_cursor);

        // Parse optional arguments
        let mut pattern: Option<Bytes> = None;
        let mut count: usize = 10;
        let mut key_type: Option<KeyType> = None;

        let mut i = 1;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"MATCH" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    pattern = Some(args[i].clone());
                }
                b"COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    count = match std::str::from_utf8(&args[i])
                        .ok()
                        .and_then(|s| s.parse().ok())
                    {
                        Some(c) => c,
                        None => return Response::error("ERR value is not an integer or out of range"),
                    };
                }
                b"TYPE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let type_str = args[i].to_ascii_lowercase();
                    key_type = match type_str.as_slice() {
                        b"string" => Some(KeyType::String),
                        b"list" => Some(KeyType::List),
                        b"set" => Some(KeyType::Set),
                        b"zset" => Some(KeyType::SortedSet),
                        b"hash" => Some(KeyType::Hash),
                        b"stream" => Some(KeyType::Stream),
                        _ => return Response::error(format!(
                            "ERR unknown type: {}",
                            String::from_utf8_lossy(&type_str)
                        )),
                    };
                }
                _ => return Response::error("ERR syntax error"),
            }
            i += 1;
        }

        // If shard_id is beyond our shards, we're done
        if shard_id as usize >= self.num_shards {
            return Response::Array(vec![
                Response::bulk(Bytes::from_static(b"0")),
                Response::Array(vec![]),
            ]);
        }

        // Iterate through shards, collecting keys
        let mut all_keys = Vec::new();
        let mut next_shard = shard_id as usize;
        let mut next_position = position;

        while all_keys.len() < count && next_shard < self.num_shards {
            // Send scan request to current shard
            let (response_tx, response_rx) = oneshot::channel();
            let remaining = count - all_keys.len();

            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::Scan {
                    cursor: next_position,
                    count: remaining,
                    pattern: pattern.clone(),
                    key_type,
                },
                response_tx,
            };

            if self.shard_senders[next_shard].send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }

            match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
                Ok(Ok(partial)) => {
                    // Extract cursor and keys from response
                    let mut shard_next_cursor = 0u64;
                    for (key, response) in partial.results {
                        if key.as_ref() == b"__cursor__" {
                            if let Response::Integer(c) = response {
                                shard_next_cursor = c as u64;
                            }
                        } else {
                            all_keys.push(key);
                        }
                    }

                    if shard_next_cursor == 0 {
                        // Shard exhausted, move to next shard
                        next_shard += 1;
                        next_position = 0;
                    } else {
                        // More keys in this shard
                        next_position = shard_next_cursor;
                        break; // We have a valid cursor, stop
                    }
                }
                Ok(Err(_)) => return Response::error("ERR shard dropped request"),
                Err(_) => return Response::error("ERR scan timeout"),
            }
        }

        // Encode next cursor
        let final_cursor = if next_shard >= self.num_shards {
            0 // Done
        } else {
            cursor::encode(next_shard as u16, next_position)
        };

        // Build response
        let key_responses: Vec<Response> = all_keys.into_iter().map(Response::bulk).collect();

        Response::Array(vec![
            Response::bulk(Bytes::from(final_cursor.to_string())),
            Response::Array(key_responses),
        ])
    }

    /// Handle KEYS command - scatter-gather across all shards.
    async fn handle_keys(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'keys' command");
        }

        let pattern = args[0].clone();

        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::Keys { pattern: pattern.clone() },
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Gather all keys
        let mut all_keys = Vec::new();
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (key, _) in partial.results {
                        all_keys.push(key);
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped KEYS request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "KEYS timeout");
                    return Response::error("ERR keys timeout");
                }
            }
        }

        // Sort keys for consistency
        all_keys.sort();

        Response::Array(all_keys.into_iter().map(Response::bulk).collect())
    }

    /// Handle DBSIZE command - sum key counts from all shards.
    async fn handle_dbsize(&self) -> Response {
        // Scatter to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Sum counts
        let mut total: i64 = 0;
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, response) in partial.results {
                        if let Response::Integer(count) = response {
                            total += count;
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped DBSIZE request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "DBSIZE timeout");
                    return Response::error("ERR dbsize timeout");
                }
            }
        }

        Response::Integer(total)
    }

    /// Handle RANDOMKEY command - return a random key using weighted shard selection.
    async fn handle_randomkey(&self) -> Response {
        use rand::Rng;

        // Phase 1: Get key counts from all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::DbSize,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Collect key counts per shard
        let mut shard_counts: Vec<(usize, i64)> = Vec::with_capacity(self.num_shards);
        let mut total_keys: i64 = 0;

        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(partial)) => {
                    for (_, response) in partial.results {
                        if let Response::Integer(count) = response {
                            shard_counts.push((shard_id, count));
                            total_keys += count;
                        }
                    }
                }
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped DBSIZE request for RANDOMKEY");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "DBSIZE timeout for RANDOMKEY");
                    return Response::error("ERR timeout");
                }
            }
        }

        // If database is empty, return nil
        if total_keys == 0 {
            return Response::null();
        }

        // Phase 2: Select shard probabilistically (weighted by key count)
        let selected_shard = {
            let mut rng = rand::thread_rng();
            let selection = rng.gen_range(0..total_keys);
            let mut cumulative: i64 = 0;
            let mut selected: usize = 0;

            for (shard_id, count) in &shard_counts {
                cumulative += count;
                if selection < cumulative {
                    selected = *shard_id;
                    break;
                }
            }
            selected
        };

        // Phase 3: Request random key from selected shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![],
            operation: ScatterOp::RandomKey,
            response_tx,
        };

        if self.shard_senders[selected_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                // Return the random key (or null if shard is now empty)
                for (_, response) in partial.results {
                    return response;
                }
                Response::null()
            }
            Ok(Err(_)) => {
                warn!(selected_shard, "Shard dropped RANDOMKEY request");
                Response::error("ERR shard dropped request")
            }
            Err(_) => {
                warn!(selected_shard, "RANDOMKEY timeout");
                Response::error("ERR timeout")
            }
        }
    }

    /// Handle FLUSHDB command - clear all shards.
    async fn handle_flushdb(&self, args: &[Bytes]) -> Response {
        // Parse optional ASYNC/SYNC argument (we only support SYNC for now)
        if !args.is_empty() {
            let mode = args[0].to_ascii_uppercase();
            if mode.as_slice() != b"ASYNC" && mode.as_slice() != b"SYNC" {
                return Response::error("ERR syntax error");
            }
        }

        // Broadcast to all shards
        let mut handles = Vec::with_capacity(self.num_shards);
        for (shard_id, sender) in self.shard_senders.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::ScatterRequest {
                request_id: next_txid(),
                keys: vec![],
                operation: ScatterOp::FlushDb,
                response_tx,
            };
            if sender.send(msg).await.is_err() {
                return Response::error("ERR shard unavailable");
            }
            handles.push((shard_id, response_rx));
        }

        // Wait for all to complete
        for (shard_id, rx) in handles {
            match tokio::time::timeout(self.scatter_gather_timeout, rx).await {
                Ok(Ok(_)) => {}
                Ok(Err(_)) => {
                    warn!(shard_id, "Shard dropped FLUSHDB request");
                    return Response::error("ERR shard dropped request");
                }
                Err(_) => {
                    warn!(shard_id, "FLUSHDB timeout");
                    return Response::error("ERR flushdb timeout");
                }
            }
        }

        Response::ok()
    }

    /// Handle FLUSHALL command - same as FLUSHDB (single database).
    async fn handle_flushall(&self, args: &[Bytes]) -> Response {
        self.handle_flushdb(args).await
    }

    /// Handle DEBUG SLEEP command - sleep without blocking the shard.
    async fn handle_debug_sleep(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'debug|sleep' command");
        }

        // args[0] is "SLEEP", args[1] is the duration
        let duration_str = match std::str::from_utf8(&args[1]) {
            Ok(s) => s,
            Err(_) => return Response::error("ERR invalid duration"),
        };

        let duration: f64 = match duration_str.parse() {
            Ok(d) => d,
            Err(_) => return Response::error("ERR invalid duration"),
        };

        if duration < 0.0 {
            return Response::error("ERR invalid duration");
        }

        // Sleep in the connection handler (not the shard worker)
        let duration_ms = (duration * 1000.0) as u64;
        tokio::time::sleep(Duration::from_millis(duration_ms)).await;

        Response::ok()
    }

    /// Handle SHUTDOWN command.
    async fn handle_shutdown(&self, _args: &[Bytes]) -> Response {
        // Note: Actual shutdown requires signaling the main server
        // For now, we just return an error suggesting manual shutdown
        Response::error("ERR SHUTDOWN is not supported in this mode. Use Ctrl+C to stop the server.")
    }

    /// Handle INFO command - gather info from all shards.
    async fn handle_info(&self, args: &[Bytes]) -> Response {
        // Execute on local shard (INFO mostly returns static data or aggregate stats)
        let cmd = frogdb_protocol::ParsedCommand {
            name: Bytes::from_static(b"INFO"),
            args: args.to_vec(),
        };
        self.execute_on_shard(self.shard_id, &cmd).await
    }

    // =========================================================================
    // CLIENT command handlers
    // =========================================================================

    /// Handle CLIENT command and dispatch to subcommands.
    async fn handle_client_command(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "ID" => self.handle_client_id(),
            "SETNAME" => self.handle_client_setname(&args[1..]),
            "GETNAME" => self.handle_client_getname(),
            "LIST" => self.handle_client_list(&args[1..]),
            "INFO" => self.handle_client_info(),
            "KILL" => self.handle_client_kill(&args[1..]),
            "PAUSE" => self.handle_client_pause(&args[1..]),
            "UNPAUSE" => self.handle_client_unpause(),
            "SETINFO" => self.handle_client_setinfo(&args[1..]),
            "NO-EVICT" => self.handle_client_no_evict(&args[1..]),
            "NO-TOUCH" => self.handle_client_no_touch(&args[1..]),
            "TRACKINGINFO" => self.handle_client_trackinginfo(),
            "GETREDIR" => self.handle_client_getredir(),
            "CACHING" => self.handle_client_caching(&args[1..]),
            "REPLY" => self.handle_client_reply(&args[1..]),
            "UNBLOCK" => self.handle_client_unblock(&args[1..]),
            "HELP" => self.handle_client_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try CLIENT HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle CLIENT ID - return connection ID.
    fn handle_client_id(&self) -> Response {
        Response::Integer(self.state.id as i64)
    }

    /// Handle CLIENT SETNAME - set connection name.
    fn handle_client_setname(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|setname' command");
        }

        let name = &args[0];

        // Validate name: no spaces allowed
        if name.contains(&b' ') {
            return Response::error("ERR Client names cannot contain spaces");
        }

        // Empty name clears the name
        let name_opt = if name.is_empty() {
            None
        } else {
            Some(name.clone())
        };

        // Update local state
        self.state.name = name_opt.clone();

        // Update in registry
        self.client_registry.update_name(self.state.id, name_opt);

        Response::ok()
    }

    /// Handle CLIENT GETNAME - get connection name.
    fn handle_client_getname(&self) -> Response {
        match &self.state.name {
            Some(name) => Response::bulk(name.clone()),
            None => Response::null(),
        }
    }

    /// Handle CLIENT LIST - list all connections.
    fn handle_client_list(&self, args: &[Bytes]) -> Response {
        // Parse optional TYPE filter
        let mut filter_type: Option<&str> = None;

        let mut i = 0;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"TYPE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let type_str = String::from_utf8_lossy(&args[i]).to_lowercase();
                    match type_str.as_str() {
                        "normal" | "master" | "replica" | "pubsub" => {
                            filter_type = match type_str.as_str() {
                                "normal" => Some("normal"),
                                "master" => Some("master"),
                                "replica" => Some("replica"),
                                "pubsub" => Some("pubsub"),
                                _ => None,
                            };
                        }
                        _ => {
                            return Response::error(format!(
                                "ERR Unknown client type '{}'",
                                type_str
                            ));
                        }
                    }
                }
                b"ID" => {
                    // CLIENT LIST ID id1 id2 ... - not implemented yet
                    return Response::error("ERR CLIENT LIST ID not implemented");
                }
                _ => {
                    return Response::error(format!(
                        "ERR syntax error, expected 'TYPE' but got '{}'",
                        String::from_utf8_lossy(&opt)
                    ));
                }
            }
            i += 1;
        }

        // Get all clients from registry
        let clients = self.client_registry.list();

        // Build output
        let mut output = String::new();
        for info in clients {
            // Apply type filter
            if let Some(ft) = filter_type {
                if info.client_type() != ft {
                    continue;
                }
            }

            output.push_str(&info.to_client_list_entry());
            output.push('\n');
        }

        Response::bulk(Bytes::from(output))
    }

    /// Handle CLIENT INFO - get current connection info.
    fn handle_client_info(&self) -> Response {
        match self.client_registry.get(self.state.id) {
            Some(info) => {
                let entry = info.to_client_list_entry();
                Response::bulk(Bytes::from(entry + "\n"))
            }
            None => Response::error("ERR client not found"),
        }
    }

    /// Handle CLIENT KILL - terminate connections.
    fn handle_client_kill(&self, args: &[Bytes]) -> Response {
        use frogdb_core::KillFilter;
        use std::net::SocketAddr;

        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|kill' command");
        }

        // Old-style syntax: CLIENT KILL addr:port
        if args.len() == 1 && !args[0].contains(&b' ') {
            let addr_str = String::from_utf8_lossy(&args[0]);
            let addr: SocketAddr = match addr_str.parse() {
                Ok(a) => a,
                Err(_) => return Response::error("ERR Invalid IP:port pair"),
            };

            let filter = KillFilter {
                addr: Some(addr),
                skip_me: true,
                current_conn_id: Some(self.state.id),
                ..Default::default()
            };

            let killed = self.client_registry.kill_by_filter(&filter);
            if killed > 0 {
                return Response::ok();
            } else {
                return Response::error("ERR No such client");
            }
        }

        // New-style syntax: CLIENT KILL [ID id] [ADDR ip:port] [LADDR ip:port] [TYPE type] [USER username] [SKIPME yes|no]
        let mut filter = KillFilter {
            skip_me: true, // Default is to skip ourselves
            current_conn_id: Some(self.state.id),
            ..Default::default()
        };

        let mut i = 0;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"ID" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let id_str = String::from_utf8_lossy(&args[i]);
                    filter.id = match id_str.parse() {
                        Ok(id) => Some(id),
                        Err(_) => return Response::error("ERR client-id is not an integer"),
                    };
                }
                b"ADDR" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let addr_str = String::from_utf8_lossy(&args[i]);
                    filter.addr = match addr_str.parse() {
                        Ok(a) => Some(a),
                        Err(_) => return Response::error("ERR Invalid IP:port pair"),
                    };
                }
                b"LADDR" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let addr_str = String::from_utf8_lossy(&args[i]);
                    filter.laddr = match addr_str.parse() {
                        Ok(a) => Some(a),
                        Err(_) => return Response::error("ERR Invalid IP:port pair"),
                    };
                }
                b"TYPE" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let type_str = String::from_utf8_lossy(&args[i]).to_lowercase();
                    match type_str.as_str() {
                        "normal" | "master" | "replica" | "pubsub" => {
                            filter.client_type = Some(type_str);
                        }
                        _ => {
                            return Response::error(format!(
                                "ERR Unknown client type '{}'",
                                type_str
                            ));
                        }
                    }
                }
                b"USER" => {
                    // USER filter - noop for now (ACL not implemented)
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    // TODO: Implement in Phase 10.5 (ACL)
                }
                b"SKIPME" => {
                    i += 1;
                    if i >= args.len() {
                        return Response::error("ERR syntax error");
                    }
                    let val = args[i].to_ascii_lowercase();
                    match val.as_slice() {
                        b"yes" => filter.skip_me = true,
                        b"no" => filter.skip_me = false,
                        _ => return Response::error("ERR syntax error"),
                    }
                }
                _ => {
                    return Response::error(format!(
                        "ERR syntax error, expected filter but got '{}'",
                        String::from_utf8_lossy(&opt)
                    ));
                }
            }
            i += 1;
        }

        // Must have at least one filter
        if filter.id.is_none()
            && filter.addr.is_none()
            && filter.laddr.is_none()
            && filter.client_type.is_none()
        {
            return Response::error("ERR syntax error");
        }

        let killed = self.client_registry.kill_by_filter(&filter);
        Response::Integer(killed as i64)
    }

    /// Handle CLIENT PAUSE - pause command execution.
    fn handle_client_pause(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|pause' command");
        }

        // Parse timeout in milliseconds
        let timeout_str = String::from_utf8_lossy(&args[0]);
        let timeout_ms: u64 = match timeout_str.parse() {
            Ok(t) => t,
            Err(_) => return Response::error("ERR timeout is not an integer or out of range"),
        };

        // Parse optional mode (WRITE or ALL)
        let mode = if args.len() > 1 {
            let mode_str = args[1].to_ascii_uppercase();
            match mode_str.as_slice() {
                b"WRITE" => PauseMode::Write,
                b"ALL" => PauseMode::All,
                _ => {
                    return Response::error(
                        "ERR pause mode must be either WRITE or ALL",
                    );
                }
            }
        } else {
            PauseMode::Write // Default mode
        };

        self.client_registry.pause(mode, timeout_ms);
        Response::ok()
    }

    /// Handle CLIENT UNPAUSE - resume command execution.
    fn handle_client_unpause(&self) -> Response {
        self.client_registry.unpause();
        Response::ok()
    }

    /// Handle CLIENT SETINFO - set client library info.
    fn handle_client_setinfo(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'client|setinfo' command");
        }

        let attr = args[0].to_ascii_uppercase();
        let value = &args[1];

        match attr.as_slice() {
            b"LIB-NAME" => {
                // Validate: no spaces or newlines allowed
                if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                    return Response::error("ERR lib-name cannot contain spaces, newlines or special characters");
                }
                self.client_registry.update_lib_info(self.state.id, Some(value.clone()), None);
                Response::ok()
            }
            b"LIB-VER" => {
                // Validate: no spaces or newlines allowed
                if value.iter().any(|&b| b == b' ' || b == b'\n' || b == b'\r') {
                    return Response::error("ERR lib-ver cannot contain spaces, newlines or special characters");
                }
                self.client_registry.update_lib_info(self.state.id, None, Some(value.clone()));
                Response::ok()
            }
            _ => Response::error(format!(
                "ERR unknown attribute '{}'",
                String::from_utf8_lossy(&attr)
            )),
        }
    }

    /// Handle CLIENT NO-EVICT - protect client from eviction.
    fn handle_client_no_evict(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|no-evict' command");
        }

        let mode = args[0].to_ascii_uppercase();
        let info = match self.client_registry.get(self.state.id) {
            Some(info) => info,
            None => return Response::error("ERR client not found"),
        };

        let mut flags = info.flags;

        match mode.as_slice() {
            b"ON" => {
                flags |= frogdb_core::ClientFlags::NO_EVICT;
            }
            b"OFF" => {
                flags.remove(frogdb_core::ClientFlags::NO_EVICT);
            }
            _ => {
                return Response::error("ERR argument must be 'ON' or 'OFF'");
            }
        }

        self.client_registry.update_flags(self.state.id, flags);
        Response::ok()
    }

    /// Handle CLIENT NO-TOUCH - don't update LRU time on access.
    fn handle_client_no_touch(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|no-touch' command");
        }

        let mode = args[0].to_ascii_uppercase();
        let info = match self.client_registry.get(self.state.id) {
            Some(info) => info,
            None => return Response::error("ERR client not found"),
        };

        let mut flags = info.flags;

        match mode.as_slice() {
            b"ON" => {
                flags |= frogdb_core::ClientFlags::NO_TOUCH;
            }
            b"OFF" => {
                flags.remove(frogdb_core::ClientFlags::NO_TOUCH);
            }
            _ => {
                return Response::error("ERR argument must be 'ON' or 'OFF'");
            }
        }

        self.client_registry.update_flags(self.state.id, flags);
        Response::ok()
    }

    /// Handle CLIENT TRACKINGINFO - return tracking state (stub).
    fn handle_client_trackinginfo(&self) -> Response {
        // Tracking not yet implemented, return "off" state
        Response::Array(vec![
            Response::bulk(Bytes::from_static(b"flags")),
            Response::Array(vec![Response::bulk(Bytes::from_static(b"off"))]),
            Response::bulk(Bytes::from_static(b"redirect")),
            Response::Integer(-1),
            Response::bulk(Bytes::from_static(b"prefixes")),
            Response::Array(vec![]),
        ])
    }

    /// Handle CLIENT GETREDIR - return tracking redirect ID (stub).
    fn handle_client_getredir(&self) -> Response {
        // Tracking not yet implemented, return -1 (not tracking)
        Response::Integer(-1)
    }

    /// Handle CLIENT CACHING - control client-side caching (stub).
    fn handle_client_caching(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|caching' command");
        }

        let mode = args[0].to_ascii_uppercase();
        match mode.as_slice() {
            b"YES" | b"NO" => {
                // Tracking not yet implemented, accept but ignore
                Response::ok()
            }
            _ => {
                Response::error("ERR argument must be 'YES' or 'NO'")
            }
        }
    }

    /// Handle CLIENT REPLY - control reply mode.
    fn handle_client_reply(&mut self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|reply' command");
        }

        let mode = args[0].to_ascii_uppercase();
        match mode.as_slice() {
            b"ON" => {
                self.state.reply_mode = ReplyMode::On;
                Response::ok()
            }
            b"OFF" => {
                self.state.reply_mode = ReplyMode::Off;
                // Note: This command itself should still return OK
                Response::ok()
            }
            b"SKIP" => {
                self.state.skip_next_reply = true;
                Response::ok()
            }
            _ => {
                Response::error("ERR argument must be 'ON', 'OFF' or 'SKIP'")
            }
        }
    }

    /// Handle CLIENT UNBLOCK - unblock a blocked client.
    fn handle_client_unblock(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'client|unblock' command");
        }

        // Parse client ID
        let id_str = String::from_utf8_lossy(&args[0]);
        let client_id: u64 = match id_str.parse() {
            Ok(id) => id,
            Err(_) => return Response::error("ERR client ID is not an integer or out of range"),
        };

        // Parse optional mode (TIMEOUT or ERROR)
        let mode = if args.len() > 1 {
            let mode_str = args[1].to_ascii_uppercase();
            match mode_str.as_slice() {
                b"TIMEOUT" => frogdb_core::UnblockMode::Timeout,
                b"ERROR" => frogdb_core::UnblockMode::Error,
                _ => {
                    return Response::error(
                        "ERR unblock mode must be either TIMEOUT or ERROR",
                    );
                }
            }
        } else {
            frogdb_core::UnblockMode::Timeout // Default mode
        };

        // Try to unblock the client
        if self.client_registry.unblock(client_id, mode) {
            Response::Integer(1)
        } else {
            Response::Integer(0)
        }
    }

    /// Handle CLIENT HELP - show help.
    fn handle_client_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")),
            Response::bulk(Bytes::from_static(b"CACHING <YES|NO>")),
            Response::bulk(Bytes::from_static(b"    Control client-side caching for the current connection.")),
            Response::bulk(Bytes::from_static(b"GETNAME")),
            Response::bulk(Bytes::from_static(b"    Return the name of the current connection.")),
            Response::bulk(Bytes::from_static(b"GETREDIR")),
            Response::bulk(Bytes::from_static(b"    Return the client tracking redirection ID (-1 if not tracking).")),
            Response::bulk(Bytes::from_static(b"ID")),
            Response::bulk(Bytes::from_static(b"    Return the ID of the current connection.")),
            Response::bulk(Bytes::from_static(b"INFO")),
            Response::bulk(Bytes::from_static(b"    Return information about the current connection.")),
            Response::bulk(Bytes::from_static(b"KILL <ip:port>|<filter> [value] ... [<filter> [value] ...]")),
            Response::bulk(Bytes::from_static(b"    Kill connection(s).")),
            Response::bulk(Bytes::from_static(b"LIST [TYPE <normal|master|replica|pubsub>]")),
            Response::bulk(Bytes::from_static(b"    Return information about client connections.")),
            Response::bulk(Bytes::from_static(b"NO-EVICT <ON|OFF>")),
            Response::bulk(Bytes::from_static(b"    Protect client from eviction.")),
            Response::bulk(Bytes::from_static(b"NO-TOUCH <ON|OFF>")),
            Response::bulk(Bytes::from_static(b"    Don't update LRU time on key access.")),
            Response::bulk(Bytes::from_static(b"PAUSE <timeout> [WRITE|ALL]")),
            Response::bulk(Bytes::from_static(b"    Suspend clients for specified time.")),
            Response::bulk(Bytes::from_static(b"REPLY <ON|OFF|SKIP>")),
            Response::bulk(Bytes::from_static(b"    Control server replies.")),
            Response::bulk(Bytes::from_static(b"SETINFO <LIB-NAME|LIB-VER> <value>")),
            Response::bulk(Bytes::from_static(b"    Set client library info.")),
            Response::bulk(Bytes::from_static(b"SETNAME <name>")),
            Response::bulk(Bytes::from_static(b"    Set the name of the current connection.")),
            Response::bulk(Bytes::from_static(b"TRACKINGINFO")),
            Response::bulk(Bytes::from_static(b"    Return tracking state for the current connection.")),
            Response::bulk(Bytes::from_static(b"UNBLOCK <client-id> [TIMEOUT|ERROR]")),
            Response::bulk(Bytes::from_static(b"    Unblock a blocked client.")),
            Response::bulk(Bytes::from_static(b"UNPAUSE")),
            Response::bulk(Bytes::from_static(b"    Resume processing commands.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }

    // =========================================================================
    // CONFIG command handlers
    // =========================================================================

    /// Handle CONFIG command and dispatch to subcommands.
    async fn handle_config_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "GET" => self.handle_config_get(&args[1..]),
            "SET" => self.handle_config_set(&args[1..]).await,
            "HELP" => self.handle_config_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try CONFIG HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle CONFIG GET <pattern> - return parameters matching pattern.
    fn handle_config_get(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config|get' command");
        }

        let pattern = String::from_utf8_lossy(&args[0]);
        let results = self.config_manager.get(&pattern);

        // Return as array of [name, value, name, value, ...]
        let mut response = Vec::with_capacity(results.len() * 2);
        for (name, value) in results {
            response.push(Response::bulk(Bytes::from(name)));
            response.push(Response::bulk(Bytes::from(value)));
        }

        Response::Array(response)
    }

    /// Handle CONFIG SET <param> <value> - set a mutable configuration parameter.
    ///
    /// This is async because it may need to propagate eviction config changes
    /// to all shard workers and wait for acknowledgment.
    async fn handle_config_set(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'config|set' command");
        }

        let param = String::from_utf8_lossy(&args[0]);
        let value = String::from_utf8_lossy(&args[1]);

        match self.config_manager.set_async(&param, &value).await {
            Ok(()) => Response::ok(),
            Err(e) => Response::error(e.to_string()),
        }
    }

    /// Handle CONFIG HELP - return help text.
    fn handle_config_help(&self) -> Response {
        let help = ConfigManager::help_text();
        let response: Vec<Response> = help
            .into_iter()
            .map(|s| Response::bulk(Bytes::from(s)))
            .collect();
        Response::Array(response)
    }

    // =========================================================================
    // SLOWLOG command handlers
    // =========================================================================

    /// Log a slow query to the appropriate shard if threshold is exceeded.
    async fn maybe_log_slow_query(&self, cmd: &ParsedCommand, elapsed_us: u64) {
        // Check threshold setting
        let threshold = self.config_manager.slowlog_log_slower_than();

        // -1 means disabled
        if threshold < 0 {
            return;
        }

        // Check if elapsed time exceeds threshold (0 means log all)
        if threshold > 0 && elapsed_us < threshold as u64 {
            return;
        }

        // Check if command has SKIP_SLOWLOG flag
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);
        if let Some(handler) = self.registry.get(&cmd_name_str) {
            if handler.flags().contains(CommandFlags::SKIP_SLOWLOG) {
                return;
            }
        }

        // Prepare command args for logging (including command name)
        let mut command_args = vec![cmd.name.clone()];
        command_args.extend(cmd.args.iter().cloned());

        // Truncate args according to max_arg_len setting
        let max_arg_len = self.config_manager.slowlog_max_arg_len();
        let truncated_args = frogdb_core::SlowLog::truncate_args(&command_args, max_arg_len);

        // Get client info
        let client_addr = self.state.addr.to_string();
        let client_name = self
            .state
            .name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();

        // Send to shard 0 (or we could distribute based on some logic)
        // Using shard 0 is simplest and matches Redis behavior
        if let Some(sender) = self.shard_senders.first() {
            let _ = sender
                .send(ShardMessage::SlowlogAdd {
                    duration_us: elapsed_us,
                    command: truncated_args,
                    client_addr,
                    client_name,
                })
                .await;
        }
    }

    /// Handle SLOWLOG command and dispatch to subcommands.
    async fn handle_slowlog_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'slowlog' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "GET" => self.handle_slowlog_get(&args[1..]).await,
            "LEN" => self.handle_slowlog_len().await,
            "RESET" => self.handle_slowlog_reset().await,
            "HELP" => self.handle_slowlog_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try SLOWLOG HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle SLOWLOG GET [count] - get recent slow queries.
    async fn handle_slowlog_get(&self, args: &[Bytes]) -> Response {
        // Default count is 10, like Redis
        let count = if args.is_empty() {
            10
        } else {
            match String::from_utf8_lossy(&args[0]).parse::<usize>() {
                Ok(n) => n,
                Err(_) => return Response::error("ERR value is not an integer or out of range"),
            }
        };

        // Scatter-gather: collect from all shards
        let mut all_entries = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogGet { count, response_tx })
                .await
                .is_ok()
            {
                if let Ok(entries) = response_rx.await {
                    all_entries.extend(entries);
                }
            }
        }

        // Sort by ID descending (newest first) and limit to count
        all_entries.sort_by(|a, b| b.id.cmp(&a.id));
        all_entries.truncate(count);

        // Convert to Redis response format
        let entries: Vec<Response> = all_entries
            .into_iter()
            .map(|entry| {
                let args: Vec<Response> = entry
                    .command
                    .into_iter()
                    .map(Response::bulk)
                    .collect();

                Response::Array(vec![
                    Response::Integer(entry.id as i64),
                    Response::Integer(entry.timestamp),
                    Response::Integer(entry.duration_us as i64),
                    Response::Array(args),
                    Response::bulk(Bytes::from(entry.client_addr)),
                    Response::bulk(Bytes::from(entry.client_name)),
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle SLOWLOG LEN - get total number of entries across all shards.
    async fn handle_slowlog_len(&self) -> Response {
        let mut total_len = 0usize;

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogLen { response_tx })
                .await
                .is_ok()
            {
                if let Ok(len) = response_rx.await {
                    total_len += len;
                }
            }
        }

        Response::Integer(total_len as i64)
    }

    /// Handle SLOWLOG RESET - clear all slow query logs.
    async fn handle_slowlog_reset(&self) -> Response {
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::SlowlogReset { response_tx })
                .await
                .is_ok()
            {
                let _ = response_rx.await;
            }
        }

        Response::ok()
    }

    /// Handle SLOWLOG HELP - show help text.
    fn handle_slowlog_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"SLOWLOG <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"GET [<count>]")),
            Response::bulk(Bytes::from_static(
                b"    Return top <count> entries from the slowlog (default 10).",
            )),
            Response::bulk(Bytes::from_static(
                b"    Entries are made of:",
            )),
            Response::bulk(Bytes::from_static(
                b"    id, timestamp, time in microseconds, arguments array, client address, client name",
            )),
            Response::bulk(Bytes::from_static(b"LEN")),
            Response::bulk(Bytes::from_static(
                b"    Return the number of entries in the slowlog.",
            )),
            Response::bulk(Bytes::from_static(b"RESET")),
            Response::bulk(Bytes::from_static(b"    Reset the slowlog.")),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
        ];
        Response::Array(help)
    }

    // =========================================================================
    // MEMORY command handlers
    // =========================================================================

    /// Handle MEMORY command and dispatch to subcommands.
    async fn handle_memory_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "DOCTOR" => self.handle_memory_doctor().await,
            "HELP" => self.handle_memory_help(),
            "MALLOC-SIZE" => self.handle_memory_malloc_size(&args[1..]),
            "PURGE" => self.handle_memory_purge(),
            "STATS" => self.handle_memory_stats().await,
            "USAGE" => self.handle_memory_usage(&args[1..]).await,
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try MEMORY HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle MEMORY DOCTOR - diagnose memory issues.
    async fn handle_memory_doctor(&self) -> Response {
        let stats = self.gather_memory_stats().await;

        let mut report = Vec::new();

        // Analyze memory usage
        let total_data_memory: usize = stats.iter().map(|s| s.data_memory).sum();
        let total_keys: usize = stats.iter().map(|s| s.keys).sum();
        let total_peak: u64 = stats.iter().map(|s| s.peak_memory).max().unwrap_or(0);
        let total_limit: u64 = stats.iter().map(|s| s.memory_limit).sum();

        report.push(format!("Sam, I have a few things to report:"));

        // Check memory usage ratio
        if total_limit > 0 {
            let usage_ratio = total_data_memory as f64 / total_limit as f64;
            if usage_ratio > 0.9 {
                report.push(format!(
                    "* High memory usage: {:.1}% of {} bytes limit",
                    usage_ratio * 100.0,
                    total_limit
                ));
            } else if usage_ratio > 0.75 {
                report.push(format!(
                    "* Moderate memory usage: {:.1}% of {} bytes limit",
                    usage_ratio * 100.0,
                    total_limit
                ));
            }
        }

        // Check for memory fragmentation
        let total_overhead: usize = stats.iter().map(|s| s.overhead_estimate).sum();
        let overhead_ratio = if total_data_memory > 0 {
            total_overhead as f64 / total_data_memory as f64
        } else {
            0.0
        };

        if overhead_ratio > 0.5 {
            report.push(format!(
                "* High overhead ratio: {:.1}% overhead detected",
                overhead_ratio * 100.0
            ));
        }

        // Report peak memory if significantly higher than current
        if total_peak > 0 && total_data_memory > 0 {
            let peak_ratio = total_peak as f64 / total_data_memory as f64;
            if peak_ratio > 1.5 {
                report.push(format!(
                    "* Peak memory was {:.1}x higher than current usage",
                    peak_ratio
                ));
            }
        }

        if report.len() == 1 {
            report.push("* No memory issues detected.".to_string());
        }

        report.push(format!(
            "\nSummary: {} keys, {} bytes data, {} shards",
            total_keys,
            total_data_memory,
            stats.len()
        ));

        Response::bulk(Bytes::from(report.join("\n")))
    }

    /// Handle MEMORY HELP - show help text.
    fn handle_memory_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"MEMORY <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"DOCTOR")),
            Response::bulk(Bytes::from_static(
                b"    Return memory problems reports.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
            Response::bulk(Bytes::from_static(b"MALLOC-SIZE <size>")),
            Response::bulk(Bytes::from_static(
                b"    Return the allocator usable size for the given input size.",
            )),
            Response::bulk(Bytes::from_static(b"PURGE")),
            Response::bulk(Bytes::from_static(
                b"    Attempt to release memory back to the OS.",
            )),
            Response::bulk(Bytes::from_static(b"STATS")),
            Response::bulk(Bytes::from_static(
                b"    Return information about memory usage.",
            )),
            Response::bulk(Bytes::from_static(b"USAGE <key> [SAMPLES <count>]")),
            Response::bulk(Bytes::from_static(
                b"    Return memory used by a key and its value.",
            )),
        ];
        Response::Array(help)
    }

    /// Handle MEMORY MALLOC-SIZE <size> - get allocator usable size (stub).
    fn handle_memory_malloc_size(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory|malloc-size' command");
        }

        // Parse the size argument
        match String::from_utf8_lossy(&args[0]).parse::<i64>() {
            Ok(size) => {
                // Without jemalloc, just return the input size
                // In a real implementation this would query the allocator
                Response::Integer(size)
            }
            Err(_) => Response::error("ERR value is not an integer or out of range"),
        }
    }

    /// Handle MEMORY PURGE - force memory release (stub).
    fn handle_memory_purge(&self) -> Response {
        // Without jemalloc, this is a no-op
        // In a real implementation this would call jemalloc_purge_arena or similar
        Response::ok()
    }

    /// Handle MEMORY STATS - get detailed memory statistics.
    async fn handle_memory_stats(&self) -> Response {
        let stats = self.gather_memory_stats().await;

        let total_data_memory: usize = stats.iter().map(|s| s.data_memory).sum();
        let total_keys: usize = stats.iter().map(|s| s.keys).sum();
        let total_overhead: usize = stats.iter().map(|s| s.overhead_estimate).sum();
        let peak_memory: u64 = stats.iter().map(|s| s.peak_memory).max().unwrap_or(0);
        let total_limit: u64 = stats.iter().map(|s| s.memory_limit).sum();

        // Build a flat array of key-value pairs (Redis MEMORY STATS format)
        let mut result = vec![
            Response::bulk(Bytes::from_static(b"peak.allocated")),
            Response::Integer(peak_memory as i64),
            Response::bulk(Bytes::from_static(b"total.allocated")),
            Response::Integer(total_data_memory as i64),
            Response::bulk(Bytes::from_static(b"startup.allocated")),
            Response::Integer(0), // We don't track startup memory separately
            Response::bulk(Bytes::from_static(b"replication.backlog")),
            Response::Integer(0), // No replication backlog yet
            Response::bulk(Bytes::from_static(b"clients.slaves")),
            Response::Integer(0), // No replica clients yet
            Response::bulk(Bytes::from_static(b"clients.normal")),
            Response::Integer(0), // Would need client tracking
            Response::bulk(Bytes::from_static(b"aof.buffer")),
            Response::Integer(0), // No AOF buffer
            Response::bulk(Bytes::from_static(b"overhead.total")),
            Response::Integer(total_overhead as i64),
            Response::bulk(Bytes::from_static(b"keys.count")),
            Response::Integer(total_keys as i64),
            Response::bulk(Bytes::from_static(b"keys.bytes-per-key")),
            Response::Integer(if total_keys > 0 {
                (total_data_memory / total_keys) as i64
            } else {
                0
            }),
            Response::bulk(Bytes::from_static(b"dataset.bytes")),
            Response::Integer(total_data_memory as i64),
            Response::bulk(Bytes::from_static(b"dataset.percentage")),
            Response::bulk(Bytes::from(if total_data_memory > 0 && total_limit > 0 {
                format!("{:.2}", (total_data_memory as f64 / total_limit as f64) * 100.0)
            } else {
                "0.00".to_string()
            })),
            Response::bulk(Bytes::from_static(b"peak.percentage")),
            Response::bulk(Bytes::from(if peak_memory > 0 && total_limit > 0 {
                format!("{:.2}", (peak_memory as f64 / total_limit as f64) * 100.0)
            } else {
                "0.00".to_string()
            })),
        ];

        // Add per-shard breakdown
        result.push(Response::bulk(Bytes::from_static(b"db.0")));
        let db_stats = vec![
            Response::bulk(Bytes::from_static(b"overhead.hashtable.main")),
            Response::Integer(total_overhead as i64),
            Response::bulk(Bytes::from_static(b"overhead.hashtable.expires")),
            Response::Integer(0),
        ];
        result.push(Response::Array(db_stats));

        Response::Array(result)
    }

    /// Handle MEMORY USAGE <key> [SAMPLES count] - get memory for a specific key.
    async fn handle_memory_usage(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'memory|usage' command");
        }

        let key = &args[0];
        let _samples = if args.len() >= 3
            && args[1].eq_ignore_ascii_case(b"SAMPLES")
        {
            match String::from_utf8_lossy(&args[2]).parse::<usize>() {
                Ok(n) => Some(n),
                Err(_) => return Response::error("ERR value is not an integer or out of range"),
            }
        } else {
            None
        };

        // Route to the shard that owns this key
        let shard_id = shard_for_key(key, self.shard_senders.len());
        let sender = &self.shard_senders[shard_id];

        let (response_tx, response_rx) = oneshot::channel();
        if sender
            .send(ShardMessage::MemoryUsage {
                key: key.clone(),
                samples: _samples,
                response_tx,
            })
            .await
            .is_err()
        {
            return Response::error("ERR shard communication error");
        }

        match response_rx.await {
            Ok(Some(usage)) => Response::Integer(usage as i64),
            Ok(None) => Response::Null,
            Err(_) => Response::error("ERR shard response error"),
        }
    }

    /// Gather memory stats from all shards.
    async fn gather_memory_stats(&self) -> Vec<ShardMemoryStats> {
        let mut stats = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::MemoryStats { response_tx })
                .await
                .is_ok()
            {
                if let Ok(shard_stats) = response_rx.await {
                    stats.push(shard_stats);
                }
            }
        }

        stats
    }

    // =========================================================================
    // LATENCY command handlers
    // =========================================================================

    /// Handle LATENCY command and dispatch to subcommands.
    async fn handle_latency_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "DOCTOR" => self.handle_latency_doctor().await,
            "GRAPH" => self.handle_latency_graph(&args[1..]).await,
            "HELP" => self.handle_latency_help(),
            "HISTOGRAM" => self.handle_latency_histogram(&args[1..]).await,
            "HISTORY" => self.handle_latency_history(&args[1..]).await,
            "LATEST" => self.handle_latency_latest().await,
            "RESET" => self.handle_latency_reset(&args[1..]).await,
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try LATENCY HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle LATENCY DOCTOR - diagnose latency issues.
    async fn handle_latency_doctor(&self) -> Response {
        let latest = self.gather_latency_latest().await;

        let mut report = Vec::new();
        report.push("I have a few latency reports to share:".to_string());

        if latest.is_empty() {
            report.push("* No latency events recorded yet.".to_string());
        } else {
            for (event, sample) in &latest {
                if sample.latency_ms > 100 {
                    report.push(format!(
                        "* {} event at {} had HIGH latency of {}ms",
                        event.as_str(),
                        sample.timestamp,
                        sample.latency_ms
                    ));
                } else if sample.latency_ms > 10 {
                    report.push(format!(
                        "* {} event at {} had moderate latency of {}ms",
                        event.as_str(),
                        sample.timestamp,
                        sample.latency_ms
                    ));
                }
            }

            if report.len() == 1 {
                report.push("* All recorded events have acceptable latency.".to_string());
            }
        }

        Response::bulk(Bytes::from(report.join("\n")))
    }

    /// Handle LATENCY GRAPH <event> - show ASCII latency graph.
    async fn handle_latency_graph(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency|graph' command");
        }

        let event_str = String::from_utf8_lossy(&args[0]);
        let event = match LatencyEvent::from_str(&event_str) {
            Some(e) => e,
            None => {
                return Response::error(format!(
                    "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                    event_str
                ));
            }
        };

        let history = self.gather_latency_history(event).await;
        let graph = generate_latency_graph(event, &history);

        Response::bulk(Bytes::from(graph))
    }

    /// Handle LATENCY HELP - show help text.
    fn handle_latency_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"LATENCY <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"DOCTOR")),
            Response::bulk(Bytes::from_static(
                b"    Return latency diagnostic report.",
            )),
            Response::bulk(Bytes::from_static(b"GRAPH <event>")),
            Response::bulk(Bytes::from_static(
                b"    Return an ASCII art graph of latency for the event.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Print this help.")),
            Response::bulk(Bytes::from_static(b"HISTOGRAM [<command> ...]")),
            Response::bulk(Bytes::from_static(
                b"    Return a cumulative distribution of command latencies.",
            )),
            Response::bulk(Bytes::from_static(b"HISTORY <event>")),
            Response::bulk(Bytes::from_static(
                b"    Return timestamp-latency pairs for the event.",
            )),
            Response::bulk(Bytes::from_static(b"LATEST")),
            Response::bulk(Bytes::from_static(
                b"    Return the latest latency samples for all events.",
            )),
            Response::bulk(Bytes::from_static(b"RESET [<event> ...]")),
            Response::bulk(Bytes::from_static(
                b"    Reset latency data for specified events, or all if none given.",
            )),
        ];
        Response::Array(help)
    }

    /// Handle LATENCY HISTOGRAM [command...] - show command latency histogram.
    async fn handle_latency_histogram(&self, _args: &[Bytes]) -> Response {
        // This would require command-level latency tracking which is not yet implemented
        // Return an empty response for now
        Response::Array(vec![])
    }

    /// Handle LATENCY HISTORY <event> - get historical latency data.
    async fn handle_latency_history(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'latency|history' command");
        }

        let event_str = String::from_utf8_lossy(&args[0]);
        let event = match LatencyEvent::from_str(&event_str) {
            Some(e) => e,
            None => {
                return Response::error(format!(
                    "ERR Unknown event type: {}. Valid events: command, fork, aof-fsync, expire-cycle, eviction-cycle, snapshot-io",
                    event_str
                ));
            }
        };

        let history = self.gather_latency_history(event).await;

        // Return as array of [timestamp, latency] pairs
        let entries: Vec<Response> = history
            .into_iter()
            .map(|sample| {
                Response::Array(vec![
                    Response::Integer(sample.timestamp),
                    Response::Integer(sample.latency_ms as i64),
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle LATENCY LATEST - get latest latency samples.
    async fn handle_latency_latest(&self) -> Response {
        let latest = self.gather_latency_latest().await;

        let entries: Vec<Response> = latest
            .into_iter()
            .map(|(event, sample)| {
                Response::Array(vec![
                    Response::bulk(Bytes::from(event.as_str())),
                    Response::Integer(sample.timestamp),
                    Response::Integer(sample.latency_ms as i64),
                    Response::Integer(sample.latency_ms as i64), // max_latency (same as latest in our impl)
                ])
            })
            .collect();

        Response::Array(entries)
    }

    /// Handle LATENCY RESET [event...] - clear latency data.
    async fn handle_latency_reset(&self, args: &[Bytes]) -> Response {
        // Parse event names
        let events: Vec<LatencyEvent> = args
            .iter()
            .filter_map(|arg| {
                let s = String::from_utf8_lossy(arg);
                LatencyEvent::from_str(&s)
            })
            .collect();

        // Broadcast reset to all shards
        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyReset {
                    events: events.clone(),
                    response_tx,
                })
                .await
                .is_ok()
            {
                let _ = response_rx.await;
            }
        }

        Response::ok()
    }

    /// Gather latest latency samples from all shards.
    async fn gather_latency_latest(&self) -> Vec<(LatencyEvent, frogdb_core::LatencySample)> {
        use std::collections::HashMap;

        let mut latest_by_event: HashMap<LatencyEvent, frogdb_core::LatencySample> = HashMap::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyLatest { response_tx })
                .await
                .is_ok()
            {
                if let Ok(samples) = response_rx.await {
                    for (event, sample) in samples {
                        // Keep the most recent sample for each event
                        latest_by_event
                            .entry(event)
                            .and_modify(|existing| {
                                if sample.timestamp > existing.timestamp {
                                    *existing = sample;
                                }
                            })
                            .or_insert(sample);
                    }
                }
            }
        }

        latest_by_event.into_iter().collect()
    }

    /// Gather latency history for a specific event from all shards.
    async fn gather_latency_history(&self, event: LatencyEvent) -> Vec<frogdb_core::LatencySample> {
        let mut all_samples = Vec::new();

        for sender in self.shard_senders.iter() {
            let (response_tx, response_rx) = oneshot::channel();
            if sender
                .send(ShardMessage::LatencyHistory { event, response_tx })
                .await
                .is_ok()
            {
                if let Ok(samples) = response_rx.await {
                    all_samples.extend(samples);
                }
            }
        }

        // Sort by timestamp (newest first)
        all_samples.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        all_samples
    }

    // =========================================================================
    // BGSAVE/LASTSAVE command handlers
    // =========================================================================

    /// Handle BGSAVE command - trigger a background snapshot.
    fn handle_bgsave(&self, args: &[Bytes]) -> Response {
        // Check for SCHEDULE option
        if !args.is_empty() {
            let opt = args[0].to_ascii_uppercase();
            if opt.as_slice() == b"SCHEDULE" {
                // BGSAVE SCHEDULE - schedule a save if one is already running,
                // otherwise start immediately
                if self.snapshot_coordinator.in_progress() {
                    self.snapshot_coordinator.schedule_snapshot();
                    return Response::Simple(Bytes::from_static(
                        b"Background saving scheduled",
                    ));
                }
                // No save in progress, fall through to start one immediately
            }
        }

        match self.snapshot_coordinator.start_snapshot() {
            Ok(handle) => {
                tracing::info!(epoch = handle.epoch(), "BGSAVE started");
                Response::Simple(Bytes::from_static(b"Background saving started"))
            }
            Err(frogdb_core::persistence::SnapshotError::AlreadyInProgress) => {
                // Return a simple status like Redis does
                Response::Simple(Bytes::from_static(
                    b"Background save already in progress",
                ))
            }
            Err(e) => Response::error(format!("ERR {}", e)),
        }
    }

    /// Handle LASTSAVE command - return Unix timestamp of last successful save.
    fn handle_lastsave(&self) -> Response {
        use std::time::{SystemTime, UNIX_EPOCH};

        match self.snapshot_coordinator.last_save_time() {
            Some(instant) => {
                // Convert Instant to Unix timestamp
                // We calculate how long ago the save was and subtract from current time
                let elapsed = instant.elapsed();
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                let save_time = now.as_secs().saturating_sub(elapsed.as_secs());
                Response::Integer(save_time as i64)
            }
            None => {
                // No snapshot has been taken yet
                Response::Integer(0)
            }
        }
    }

    /// Wait if the server is paused (CLIENT PAUSE).
    /// This queues commands (not drops them) by blocking until pause ends.
    async fn wait_if_paused(&self, cmd: &ParsedCommand) {
        // Get command flags to determine if this is a write command
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        let is_write_command = match self.registry.get(&cmd_name_str) {
            Some(handler) => handler.flags().contains(CommandFlags::WRITE),
            None => false, // Unknown commands treated as non-write
        };

        // Certain commands are always exempt from pause
        let is_exempt = matches!(
            cmd_name_str.as_ref(),
            "CLIENT" | "PING" | "QUIT" | "RESET" | "INFO" | "CONFIG" | "DEBUG" | "SLOWLOG"
        );

        if is_exempt {
            return;
        }

        // Check pause state and wait if necessary
        loop {
            match self.client_registry.check_pause() {
                Some(PauseMode::All) => {
                    // All commands are paused
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Some(PauseMode::Write) if is_write_command => {
                    // Write commands are paused
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                _ => {
                    // Not paused or this command is not affected
                    return;
                }
            }
        }
    }
}
