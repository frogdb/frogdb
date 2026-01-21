//! Connection handling.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use frogdb_core::{
    shard_for_key, AclManager, AuthenticatedUser, ClientHandle, ClientRegistry,
    CommandCategory, CommandFlags, CommandRegistry, GlobPattern,
    IntrospectionRequest, IntrospectionResponse, MetricsRecorder, PartialResult,
    PauseMode, PubSubMessage, PubSubSender, ScatterOp, ShardMessage,
    TransactionResult, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
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

    /// Authentication state.
    pub auth: AuthState,

    /// Blocked state for blocking commands (None = not blocked).
    pub blocked: Option<BlockedState>,
}

impl ConnectionState {
    fn new(id: u64, addr: SocketAddr, requires_auth: bool) -> Self {
        Self {
            id,
            addr,
            created_at: std::time::Instant::now(),
            protocol_version: ProtocolVersion::default(),
            name: None,
            transaction: TransactionState::default(),
            pubsub: PubSubState::default(),
            auth: if requires_auth {
                AuthState::NotAuthenticated
            } else {
                AuthState::default()
            },
            blocked: None,
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
    }
}

/// Convert protocol Direction to core Direction.
fn convert_direction(dir: frogdb_protocol::Direction) -> frogdb_core::Direction {
    match dir {
        frogdb_protocol::Direction::Left => frogdb_core::Direction::Left,
        frogdb_protocol::Direction::Right => frogdb_core::Direction::Right,
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

                    // Update last command time for idle tracking
                    self.client_registry.update_last_command(self.state.id);

                    // Handle QUIT specially (also clears transaction state)
                    if cmd.name.eq_ignore_ascii_case(b"QUIT") {
                        // Clear transaction state before quitting
                        self.state.transaction = TransactionState::default();
                        let response_frame: BytesFrame = Response::ok().into();
                        let _ = self.framed.send(response_frame).await;
                        break;
                    }

                    // Wait if server is paused (for non-exempt commands)
                    self.wait_if_paused(&cmd).await;

                    // Start timing for metrics
                    let cmd_name_for_metrics = String::from_utf8_lossy(&cmd.name).to_uppercase();
                    let timer = frogdb_metrics::CommandTimer::new(
                        cmd_name_for_metrics,
                        self.metrics_recorder.clone(),
                    );

                    // Route and execute (with transaction and pub/sub handling)
                    let responses = self.route_and_execute_with_transaction(&cmd).await;

                    // Record metrics - check for errors in responses
                    let has_error = responses.iter().any(|r| matches!(r, Response::Error(_)));
                    if has_error {
                        timer.finish_with_error("command_error");
                    } else {
                        timer.finish();
                    }

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

        // Handle ACL command
        if cmd_name_str == "ACL" {
            return vec![self.handle_acl_command(&cmd.args).await];
        }

        // Check authentication before processing other commands
        // AUTH, QUIT, HELLO, and PING are allowed without authentication
        if !self.state.auth.is_authenticated() && !Self::is_auth_exempt(&cmd_name_str) {
            return vec![Response::error("NOAUTH Authentication required.")];
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

        // Handle CLIENT commands specially (need access to client registry)
        if cmd_name_str == "CLIENT" {
            return vec![self.handle_client_command(&cmd.args).await];
        }

        // Handle CONFIG commands specially (need access to config manager)
        if cmd_name_str == "CONFIG" {
            return vec![self.handle_config_command(&cmd.args)];
        }

        // Handle server commands that need special routing
        match cmd_name_str.as_ref() {
            "SCAN" => return vec![self.handle_scan(&cmd.args).await],
            "KEYS" => return vec![self.handle_keys(&cmd.args).await],
            "DBSIZE" => return vec![self.handle_dbsize().await],
            "FLUSHDB" => return vec![self.handle_flushdb(&cmd.args).await],
            "FLUSHALL" => return vec![self.handle_flushall(&cmd.args).await],
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

    /// Handle CLIENT HELP - show help.
    fn handle_client_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:")),
            Response::bulk(Bytes::from_static(b"GETNAME")),
            Response::bulk(Bytes::from_static(b"    Return the name of the current connection.")),
            Response::bulk(Bytes::from_static(b"ID")),
            Response::bulk(Bytes::from_static(b"    Return the ID of the current connection.")),
            Response::bulk(Bytes::from_static(b"INFO")),
            Response::bulk(Bytes::from_static(b"    Return information about the current connection.")),
            Response::bulk(Bytes::from_static(b"KILL <ip:port>|<filter> [value] ... [<filter> [value] ...]")),
            Response::bulk(Bytes::from_static(b"    Kill connection(s).")),
            Response::bulk(Bytes::from_static(b"LIST [TYPE <normal|master|replica|pubsub>]")),
            Response::bulk(Bytes::from_static(b"    Return information about client connections.")),
            Response::bulk(Bytes::from_static(b"PAUSE <timeout> [WRITE|ALL]")),
            Response::bulk(Bytes::from_static(b"    Suspend clients for specified time.")),
            Response::bulk(Bytes::from_static(b"SETNAME <name>")),
            Response::bulk(Bytes::from_static(b"    Set the name of the current connection.")),
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
    fn handle_config_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            return Response::error("ERR wrong number of arguments for 'config' command");
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "GET" => self.handle_config_get(&args[1..]),
            "SET" => self.handle_config_set(&args[1..]),
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
    fn handle_config_set(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'config|set' command");
        }

        let param = String::from_utf8_lossy(&args[0]);
        let value = String::from_utf8_lossy(&args[1]);

        match self.config_manager.set(&param, &value) {
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
