//! Connection handling.
//!
//! This module provides the [`ConnectionHandler`] which processes client commands.
//! The handler can be created using either the legacy `new()` method with many
//! individual parameters, or the more organized `from_deps()` method with grouped
//! dependencies, or the [`ConnectionHandlerBuilder`] for a fluent API.
//!
//! # Dependency Groups
//!
//! Dependencies are organized into logical groups:
//! - [`CoreDeps`] - Essential dependencies for command execution
//! - [`AdminDeps`] - Dependencies for administrative commands
//! - [`ClusterDeps`] - Dependencies for cluster mode (optional)
//! - [`ObservabilityDeps`] - Dependencies for tracing and monitoring
//! - [`ConnectionConfig`] - Configuration options

// Submodules
mod builder;
pub mod deps;
pub mod handlers;
pub mod router;
pub mod state;

// Re-export dependency groups
pub use deps::{
    AdminDeps, ClusterDeps, ConnectionConfig, ConnectionDeps, CoreDeps, ObservabilityDeps,
};

// Re-export state types
pub use state::{
    AuthState, BlockedState, ConnectionState, LocalClientStats, PubSubState, ReplyMode,
    TransactionState, TransactionTarget, STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS,
};

// Re-export builder
pub use builder::{ConnectionHandlerBuilder, connection_builder, standalone_config};

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use frogdb_core::{
    cluster::{ClusterCommand, NodeInfo, NodeRole, SlotRange},
    persistence::SnapshotCoordinator, shard_for_key, slot_for_key,
    AclManager, ClientHandle, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    CommandCategory, CommandFlags, CommandRegistry, ConnectionLevelOp, ExecutionStrategy,
    GlobPattern, IntrospectionRequest, IntrospectionResponse, KeyAccessType, MetricsRecorder,
    PartialResult, PauseMode, PubSubMessage, PubSubSender, ReplicationTracker, ReplicationTrackerImpl,
    RwLockExt, ScatterOp, ShardReadyResult, SharedFunctionRegistry, ShardMessage, StreamId,
    TransactionResult, MAX_PATTERN_SUBSCRIPTIONS_PER_CONNECTION,
    MAX_SHARDED_SUBSCRIPTIONS_PER_CONNECTION, MAX_SUBSCRIPTIONS_PER_CONNECTION,
};
use openraft::error::{ClientWriteError, RaftError};
use frogdb_metrics::SharedTracer;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, RaftClusterOp, Response};
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use serde_json;
use tracing::{debug, info, trace, warn};

use crate::config::TracingConfig;
use crate::migrate::{MigrateArgs, MigrateClient, MigrateError};
use crate::net::TcpStream;
use crate::replication::PrimaryReplicationHandler;
use crate::runtime_config::ConfigManager;
use crate::scatter::{strategy_for_op, ScatterGatherExecutor};
use crate::server::next_txid;

use router::ConnectionLevelHandler;

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

    /// Optional shared tracer for distributed tracing.
    shared_tracer: Option<SharedTracer>,

    /// Tracing configuration.
    tracing_config: TracingConfig,

    /// Optional replication tracker for WAIT command.
    replication_tracker: Option<Arc<ReplicationTrackerImpl>>,

    /// Optional cluster state (only when cluster mode is enabled).
    cluster_state: Option<Arc<ClusterState>>,

    /// This node's ID (for cluster mode).
    node_id: Option<u64>,

    /// Whether this is an admin connection (from admin port).
    is_admin: bool,

    /// Whether admin port separation is enabled.
    admin_enabled: bool,

    /// Hot shard detection configuration.
    hotshards_config: frogdb_metrics::HotShardConfig,

    /// Memory diagnostics configuration.
    memory_diag_config: frogdb_metrics::MemoryDiagConfig,

    /// Optional latency band tracker for SLO monitoring.
    band_tracker: Option<Arc<frogdb_metrics::LatencyBandTracker>>,

    /// Optional Raft instance (only when cluster mode is enabled).
    raft: Option<Arc<ClusterRaft>>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional primary replication handler for PSYNC connection handoff.
    primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,

    /// Pending PSYNC handoff parameters (replication_id, offset).
    /// Set when PSYNC command returns PSYNC_HANDOFF, processed after the loop.
    pending_psync_handoff: Option<(String, i64)>,
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
        frogdb_protocol::BlockingOp::Wait { num_replicas, timeout_ms } => {
            frogdb_core::BlockingOp::Wait { num_replicas, timeout_ms }
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

/// Estimate the size of a RESP2 frame in bytes.
/// This is an approximation based on the frame structure.
fn estimate_resp2_frame_size(frame: &redis_protocol::resp2::types::BytesFrame) -> usize {
    use redis_protocol::resp2::types::BytesFrame;
    match frame {
        BytesFrame::SimpleString(s) => 1 + s.len() + 2, // +, string, CRLF
        BytesFrame::Error(e) => 1 + e.as_bytes().len() + 2, // -, message, CRLF
        BytesFrame::Integer(i) => 1 + format!("{}", i).len() + 2, // :, number, CRLF
        BytesFrame::BulkString(bs) => {
            1 + format!("{}", bs.len()).len() + 2 + bs.len() + 2 // $, len, CRLF, data, CRLF
        }
        BytesFrame::Array(arr) => {
            let header = 1 + format!("{}", arr.len()).len() + 2; // *, count, CRLF
            let elements: usize = arr.iter().map(estimate_resp2_frame_size).sum();
            header + elements
        }
        BytesFrame::Null => 5, // $-1\r\n
    }
}

/// Estimate the size of a command in bytes (received from client).
fn estimate_command_size(cmd: &ParsedCommand) -> usize {
    // Account for RESP array header + name + all args
    // Format: *<n>\r\n$<len>\r\n<name>\r\n$<len>\r\n<arg>\r\n...
    let n = 1 + cmd.args.len(); // command name + args
    let header = 1 + format!("{}", n).len() + 2; // *<n>\r\n

    let name_size = 1 + format!("{}", cmd.name.len()).len() + 2 + cmd.name.len() + 2;
    let args_size: usize = cmd
        .args
        .iter()
        .map(|a| 1 + format!("{}", a.len()).len() + 2 + a.len() + 2)
        .sum();

    header + name_size + args_size
}
/// Format a Unix timestamp as ISO 8601 string.
fn format_timestamp_iso(secs: u64) -> String {
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (1970-01-01)
    let mut year = 1970i32;
    let mut remaining_days = days_since_epoch as i32;

    loop {
        let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        let days_in_year = if is_leap { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        year += 1;
    }

    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let days_in_months: [i32; 12] = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &days_in_months {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }
    let day = remaining_days + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}

/// Convert protocol RaftClusterOp to core ClusterCommand.
/// Returns None for operations that require special handling (e.g., Failover).
fn convert_raft_cluster_op(op: &RaftClusterOp) -> Option<ClusterCommand> {
    match op {
        RaftClusterOp::AddNode {
            node_id,
            addr,
            cluster_addr,
        } => Some(ClusterCommand::AddNode {
            node: NodeInfo::new_primary(*node_id, *addr, *cluster_addr),
        }),
        RaftClusterOp::RemoveNode { node_id } => {
            Some(ClusterCommand::RemoveNode { node_id: *node_id })
        }
        RaftClusterOp::AssignSlots { node_id, slots } => Some(ClusterCommand::AssignSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        }),
        RaftClusterOp::RemoveSlots { node_id, slots } => Some(ClusterCommand::RemoveSlots {
            node_id: *node_id,
            slots: slots.iter().map(|&s| SlotRange::single(s)).collect(),
        }),
        RaftClusterOp::SetRole {
            node_id,
            is_replica,
            primary_id,
        } => Some(ClusterCommand::SetRole {
            node_id: *node_id,
            role: if *is_replica {
                NodeRole::Replica
            } else {
                NodeRole::Primary
            },
            primary_id: *primary_id,
        }),
        RaftClusterOp::BeginSlotMigration {
            slot,
            source_node,
            target_node,
        } => Some(ClusterCommand::BeginSlotMigration {
            slot: *slot,
            source_node: *source_node,
            target_node: *target_node,
        }),
        RaftClusterOp::CompleteSlotMigration {
            slot,
            source_node,
            target_node,
        } => Some(ClusterCommand::CompleteSlotMigration {
            slot: *slot,
            source_node: *source_node,
            target_node: *target_node,
        }),
        RaftClusterOp::CancelSlotMigration { slot } => {
            Some(ClusterCommand::CancelSlotMigration { slot: *slot })
        }
        RaftClusterOp::IncrementEpoch => Some(ClusterCommand::IncrementEpoch),
        RaftClusterOp::MarkNodeFailed { node_id } => {
            Some(ClusterCommand::MarkNodeFailed { node_id: *node_id })
        }
        RaftClusterOp::MarkNodeRecovered { node_id } => {
            Some(ClusterCommand::MarkNodeRecovered { node_id: *node_id })
        }
        // Failover requires special handling - multiple Raft commands
        RaftClusterOp::Failover { .. } => None,
    }
}

/// Commands that have subcommands (container commands in Redis terminology).
const CONTAINER_COMMANDS: &[&str] = &[
    "ACL", "CLIENT", "CONFIG", "CLUSTER", "DEBUG", "MEMORY", "MODULE",
    "OBJECT", "SCRIPT", "SLOWLOG", "XGROUP", "XINFO", "COMMAND", "PUBSUB",
    "FUNCTION", "LATENCY", "STATUS",
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
    /// Create a new connection handler using grouped dependencies.
    ///
    /// This is the preferred way to create a ConnectionHandler as it uses
    /// logical dependency groups for better organization.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let handler = ConnectionHandler::from_deps(
    ///     socket,
    ///     addr,
    ///     conn_id,
    ///     shard_id,
    ///     client_handle,
    ///     core_deps,
    ///     admin_deps,
    ///     cluster_deps,
    ///     config,
    ///     observability_deps,
    /// );
    /// ```
    pub fn from_deps(
        socket: TcpStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        client_handle: ClientHandle,
        core: CoreDeps,
        admin: AdminDeps,
        cluster: ClusterDeps,
        config: ConnectionConfig,
        observability: ObservabilityDeps,
    ) -> Self {
        let framed = Framed::new(socket, Resp2);
        let requires_auth = core.acl_manager.requires_auth();
        let state = ConnectionState::new(conn_id, addr, requires_auth);

        // Create pub/sub channel
        let (pubsub_tx, pubsub_rx) = mpsc::unbounded_channel();

        debug!(conn_id = conn_id, addr = %addr, "Connection established");

        Self {
            framed,
            state,
            shard_id,
            num_shards: config.num_shards,
            registry: core.registry,
            client_registry: admin.client_registry,
            config_manager: admin.config_manager,
            client_handle,
            shard_senders: core.shard_senders,
            allow_cross_slot: config.allow_cross_slot,
            scatter_gather_timeout: config.scatter_gather_timeout,
            pubsub_tx,
            pubsub_rx,
            metrics_recorder: core.metrics_recorder,
            acl_manager: core.acl_manager,
            snapshot_coordinator: admin.snapshot_coordinator,
            function_registry: admin.function_registry,
            shared_tracer: observability.shared_tracer,
            tracing_config: observability.tracing_config,
            replication_tracker: cluster.replication_tracker,
            cluster_state: cluster.cluster_state,
            node_id: cluster.node_id,
            is_admin: config.is_admin,
            admin_enabled: config.admin_enabled,
            hotshards_config: config.hotshards_config,
            memory_diag_config: config.memory_diag_config,
            band_tracker: observability.band_tracker,
            raft: cluster.raft,
            network_factory: cluster.network_factory,
            primary_replication_handler: cluster.primary_replication_handler,
            pending_psync_handoff: None,
        }
    }

    /// Create a new connection handler (legacy interface with individual parameters).
    ///
    /// # Deprecated
    ///
    /// This constructor takes many individual parameters and is hard to maintain.
    /// Use [`from_deps`](Self::from_deps) or [`ConnectionHandlerBuilder`] instead.
    #[deprecated(
        since = "0.2.0",
        note = "Use from_deps() or ConnectionHandlerBuilder for better organization"
    )]
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
        shared_tracer: Option<SharedTracer>,
        tracing_config: TracingConfig,
        replication_tracker: Option<Arc<ReplicationTrackerImpl>>,
        cluster_state: Option<Arc<ClusterState>>,
        node_id: Option<u64>,
        is_admin: bool,
        admin_enabled: bool,
        hotshards_config: frogdb_metrics::HotShardConfig,
        memory_diag_config: frogdb_metrics::MemoryDiagConfig,
        band_tracker: Option<Arc<frogdb_metrics::LatencyBandTracker>>,
        raft: Option<Arc<ClusterRaft>>,
        network_factory: Option<Arc<ClusterNetworkFactory>>,
        primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    ) -> Self {
        // Convert to grouped dependencies and delegate
        let core = CoreDeps {
            registry,
            shard_senders,
            metrics_recorder,
            acl_manager,
        };
        let admin = AdminDeps {
            client_registry,
            config_manager,
            snapshot_coordinator,
            function_registry,
        };
        let cluster = ClusterDeps {
            cluster_state,
            node_id,
            raft,
            network_factory,
            replication_tracker,
            primary_replication_handler,
        };
        let config = ConnectionConfig {
            num_shards,
            allow_cross_slot,
            scatter_gather_timeout: Duration::from_millis(scatter_gather_timeout_ms),
            is_admin,
            admin_enabled,
            hotshards_config,
            memory_diag_config,
        };
        let observability = ObservabilityDeps {
            shared_tracer,
            tracing_config,
            band_tracker,
        };

        Self::from_deps(
            socket,
            addr,
            conn_id,
            shard_id,
            client_handle,
            core,
            admin,
            cluster,
            config,
            observability,
        )
    }

    /// Send a response to the client, using appropriate encoding based on protocol version.
    ///
    /// For RESP2 connections, uses the standard Framed codec.
    /// For RESP3 connections, manually encodes and writes to the socket.
    ///
    /// # Panics
    ///
    /// Panics if the response is an internal action type (BlockingNeeded, RaftNeeded,
    /// MigrateNeeded). These should be intercepted by `route_and_execute_with_transaction`
    /// before reaching this method.
    async fn send_response(&mut self, response: Response) -> std::io::Result<()> {
        // Convert to WireResponse, panicking if it's an internal action
        // (which would indicate a bug in the command routing logic)
        let wire_response = response.into_wire().expect(
            "Internal action reached send_response - should be intercepted by route_and_execute_with_transaction"
        );
        self.send_wire_response(wire_response).await
    }

    /// Send a wire response to the client.
    ///
    /// This is the type-safe version that only accepts wire-serializable responses.
    /// Use this when you have already extracted a WireResponse from a Response.
    async fn send_wire_response(&mut self, response: frogdb_protocol::WireResponse) -> std::io::Result<()> {
        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Use RESP2 encoding via the Framed codec
                let frame = response.to_resp2_frame();
                // Estimate frame size for stats tracking
                let frame_size = estimate_resp2_frame_size(&frame);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
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
                // Track actual encoded size
                self.state.local_stats.add_bytes_sent(buf.len() as u64);
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
                    info!(conn_id = self.state.id, addr = %self.state.addr, "Connection killed");
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
                            debug!(
                                conn_id = self.state.id,
                                addr = %self.state.addr,
                                session_duration_ms = self.state.created_at.elapsed().as_millis() as u64,
                                "Client disconnected"
                            );
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

                    // Track bytes received for this command
                    let cmd_bytes = estimate_command_size(&cmd);
                    self.state.local_stats.add_bytes_recv(cmd_bytes as u64);

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
                    let timer = frogdb_metrics::CommandTimer::with_band_tracker(
                        cmd_name_for_metrics.clone(),
                        self.metrics_recorder.clone(),
                        self.band_tracker.clone(),
                    );

                    // Start request span for distributed tracing (if enabled)
                    let request_span = self.shared_tracer.as_ref()
                        .map(|t| t.start_request_span(&cmd_name_for_metrics, self.state.id));

                    // Route and execute (with transaction and pub/sub handling)
                    let responses = self.route_and_execute_with_transaction(&cmd).await;

                    // Check for PSYNC_HANDOFF signal - this requires special handling
                    // because we need to hand off the TCP connection to the replication handler
                    if let Some(handoff_params) = Self::extract_psync_handoff(&responses) {
                        info!(
                            conn_id = self.state.id,
                            addr = %self.state.addr,
                            replication_id = %handoff_params.0,
                            offset = handoff_params.1,
                            "PSYNC handoff requested - will transfer connection to replication handler"
                        );
                        // Store params for handoff after the loop (where we have ownership of self)
                        self.pending_psync_handoff = Some(handoff_params);
                        break;
                    }

                    // Calculate elapsed time in microseconds for slowlog
                    let elapsed_us = start_time.elapsed().as_micros() as u64;

                    // Record per-client command statistics
                    self.state.local_stats.record_command(&cmd_name_for_metrics, elapsed_us);

                    // Record metrics - check for errors in responses
                    let has_error = responses.iter().any(|r| matches!(r, Response::Error(_)));
                    if has_error {
                        timer.finish_with_error("command_error");
                        // Mark span as error
                        if let Some(ref span) = request_span {
                            span.set_error("command_error");
                        }
                    } else {
                        timer.finish();
                        // Mark span as success
                        if let Some(ref span) = request_span {
                            span.set_ok();
                        }
                    }

                    // End the request span
                    if let Some(span) = request_span {
                        span.end();
                    }

                    // Log to slowlog if threshold exceeded and command not exempt
                    self.maybe_log_slow_query(&cmd, elapsed_us).await;

                    // Periodically sync local stats to the registry
                    self.maybe_sync_stats();

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

        // Check if we need to do PSYNC handoff
        if let Some((replication_id, offset)) = self.pending_psync_handoff.take() {
            info!(
                conn_id = self.state.id,
                addr = %self.state.addr,
                replication_id = %replication_id,
                offset = offset,
                "Performing PSYNC handoff"
            );

            // Get the primary replication handler
            if let Some(ref handler) = self.primary_replication_handler {
                // Extract the raw TcpStream from the Framed codec.
                // into_inner() consumes the Framed and returns the underlying stream.
                // crate::net::TcpStream is tokio::net::TcpStream (or turmoil::net::TcpStream in tests)
                let stream = self.framed.into_inner();

                // Call the replication handler - this takes over the connection
                // Note: PrimaryReplicationHandler uses tokio::net::TcpStream directly.
                // In production (non-turmoil), crate::net::TcpStream IS tokio::net::TcpStream.
                // In turmoil mode, we'd need to handle this differently.
                #[cfg(not(feature = "turmoil"))]
                {
                    if let Err(e) = handler.handle_psync(
                        stream,
                        self.state.addr,
                        &replication_id,
                        offset,
                    ).await {
                        warn!(
                            conn_id = self.state.id,
                            error = %e,
                            "PSYNC handoff failed"
                        );
                    }
                }

                #[cfg(feature = "turmoil")]
                {
                    // In turmoil mode, we can't directly pass the turmoil TcpStream
                    // to the handler which expects tokio TcpStream.
                    // For now, log an error. A proper solution would use a trait.
                    warn!(
                        conn_id = self.state.id,
                        "PSYNC handoff not supported in turmoil simulation mode"
                    );
                    let _ = stream; // Suppress unused warning
                }
            } else {
                warn!(
                    conn_id = self.state.id,
                    "PSYNC handoff requested but no primary replication handler available"
                );
            }

            // Don't run normal cleanup - replication handler has the connection
            debug!(conn_id = self.state.id, "Connection handler finished (PSYNC handoff)");
            return Ok(());
        }

        // Cleanup: notify all shards that this connection is closed
        self.notify_connection_closed().await;

        debug!(conn_id = self.state.id, "Connection handler finished");
        Ok(())
    }

    /// Notify all shards that this connection is closed.
    async fn notify_connection_closed(&mut self) {
        // Final stats sync before closing
        self.sync_stats_to_registry();

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

    /// Extract PSYNC_HANDOFF parameters from responses.
    ///
    /// The PSYNC command returns a special response array to signal that
    /// the connection should be handed off to the replication handler:
    /// `[PSYNC_HANDOFF, replication_id, offset]`
    ///
    /// Returns `Some((replication_id, offset))` if handoff is needed.
    fn extract_psync_handoff(responses: &[Response]) -> Option<(String, i64)> {
        if responses.len() != 1 {
            return None;
        }

        if let Response::Array(items) = &responses[0] {
            if items.len() >= 3 {
                // Check for PSYNC_HANDOFF marker
                if let Response::Simple(marker) = &items[0] {
                    if marker.as_ref() == b"PSYNC_HANDOFF" {
                        // Extract replication_id
                        let replication_id = match &items[1] {
                            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
                            _ => return None,
                        };

                        // Extract offset
                        let offset = match &items[2] {
                            Response::Bulk(Some(b)) => {
                                String::from_utf8_lossy(b).parse::<i64>().ok()?
                            }
                            _ => return None,
                        };

                        return Some((replication_id, offset));
                    }
                }
            }
        }

        None
    }


    /// Periodically sync local stats to the registry.
    /// Syncs every STATS_SYNC_INTERVAL_COMMANDS commands or STATS_SYNC_INTERVAL_MS milliseconds.
    fn maybe_sync_stats(&mut self) {
        let should_sync = self.state.local_stats.commands_total >= STATS_SYNC_INTERVAL_COMMANDS
            || self.state.last_stats_sync.elapsed().as_millis() as u64 >= STATS_SYNC_INTERVAL_MS;

        if should_sync && self.state.local_stats.has_data() {
            self.sync_stats_to_registry();
        }
    }

    /// Force sync local stats to the registry.
    fn sync_stats_to_registry(&mut self) {
        if self.state.local_stats.has_data() {
            let delta = self.state.local_stats.to_delta();
            self.client_registry.update_stats(self.state.id, &delta);
            self.state.local_stats.clear();
            self.state.last_stats_sync = std::time::Instant::now();
        }
    }

    /// Handle a blocking command wait.
    ///
    /// This sends a BlockWait message to the shard and waits for a response.
    /// For WAIT command, uses the replication tracker instead of shard routing.
    async fn handle_blocking_wait(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
        proto_op: frogdb_protocol::BlockingOp,
    ) -> Response {
        use std::time::{Duration, Instant};
        use tokio::sync::oneshot;

        // Handle WAIT command specially - it uses the replication tracker, not shard routing
        if let frogdb_protocol::BlockingOp::Wait { num_replicas, timeout_ms } = proto_op {
            return self.handle_wait_command(num_replicas, timeout_ms).await;
        }

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

    /// Handle WAIT command using the replication tracker.
    ///
    /// WAIT blocks until the specified number of replicas have acknowledged
    /// all writes up to this point, or until the timeout expires.
    async fn handle_wait_command(&self, num_replicas: u32, timeout_ms: u64) -> Response {
        use std::time::Duration;

        // If no replication tracker is available, return 0 replicas
        let tracker = match &self.replication_tracker {
            Some(t) => t,
            None => {
                // No replication configured - return 0 replicas immediately
                return Response::Integer(0);
            }
        };

        // Get the current replication offset that replicas need to acknowledge
        let current_offset = tracker.current_offset();

        // If num_replicas is 0 or timeout_ms is 0, return immediately with current replica count
        // Redis behavior: timeout=0 means "check and return immediately, don't wait"
        if num_replicas == 0 || timeout_ms == 0 {
            let count = tracker.count_acked(current_offset);
            return Response::Integer(count as i64);
        }

        // Wait for replicas with timeout
        let timeout_duration = Duration::from_millis(timeout_ms);

        let wait_future = tracker.wait_for_acks(current_offset, num_replicas);
        let result = tokio::time::timeout(timeout_duration, wait_future).await;

        match result {
            Ok(count) => Response::Integer(count as i64),
            Err(_) => {
                // Timeout - return the count of replicas that did ACK
                let count = tracker.count_acked(current_offset);
                Response::Integer(count as i64)
            }
        }
    }

    /// Handle a Raft cluster command asynchronously.
    ///
    /// This method is called when a cluster command (MEET, FORGET, ADDSLOTS, etc.)
    /// returns `Response::RaftNeeded`. It executes the Raft operation asynchronously
    /// and updates the NetworkFactory after successful commit.
    async fn handle_raft_command(
        &self,
        op: RaftClusterOp,
        register_node: Option<(u64, std::net::SocketAddr)>,
        unregister_node: Option<u64>,
    ) -> Response {
        // Get the Raft instance
        let raft = match &self.raft {
            Some(r) => r,
            None => return Response::error("ERR Cluster mode not enabled"),
        };

        // Handle Failover specially - it requires multiple Raft commands
        if let RaftClusterOp::Failover {
            replica_id,
            primary_id,
            force: _,
        } = &op
        {
            return self
                .handle_failover_command(raft, *replica_id, *primary_id)
                .await;
        }

        // Convert protocol RaftClusterOp to core ClusterCommand
        let cmd = match convert_raft_cluster_op(&op) {
            Some(cmd) => cmd,
            None => return Response::error("ERR Unsupported cluster operation"),
        };

        // Execute the Raft command
        match raft.client_write(cmd).await {
            Ok(_) => {
                // Update NetworkFactory after successful Raft commit
                if let Some((node_id, addr)) = register_node {
                    if let Some(ref factory) = self.network_factory {
                        factory.register_node(node_id, addr);
                    }
                }
                if let Some(node_id) = unregister_node {
                    if let Some(ref factory) = self.network_factory {
                        factory.remove_node(node_id);
                    }
                }
                Response::ok()
            }
            Err(e) => {
                // Check if this is a ForwardToLeader error
                if let RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) = &e {
                    // Try to get the leader's client address from ClusterState
                    if let Some(leader_id) = forward.leader_id {
                        if let Some(ref cluster_state) = self.cluster_state {
                            if let Some(leader_info) = cluster_state.get_node(leader_id) {
                                // Return redirect error with leader's client address
                                return Response::error(format!(
                                    "REDIRECT {} {}",
                                    leader_id, leader_info.addr
                                ));
                            }
                        }
                    }
                    // Leader unknown - return error with whatever info we have
                    return Response::error(format!(
                        "CLUSTERDOWN No leader available: {:?}",
                        forward.leader_id
                    ));
                }
                // Other errors
                Response::error(format!("ERR Raft error: {}", e))
            }
        }
    }

    /// Handle a CLUSTER FAILOVER command.
    ///
    /// This requires multiple Raft commands:
    /// 1. SetRole - promote replica to primary
    /// 2. AssignSlots - transfer slots from old primary to new primary
    async fn handle_failover_command(
        &self,
        raft: &ClusterRaft,
        replica_id: u64,
        primary_id: u64,
    ) -> Response {
        // Get cluster state to find slots to transfer
        let cluster_state = match &self.cluster_state {
            Some(cs) => cs,
            None => return Response::error("ERR Cluster state not available"),
        };

        // 1. Promote replica: change role to Primary
        let role_cmd = ClusterCommand::SetRole {
            node_id: replica_id,
            role: NodeRole::Primary,
            primary_id: None,
        };

        if let Err(e) = raft.client_write(role_cmd).await {
            return Response::error(format!("ERR Failover failed to promote replica: {}", e));
        }

        // 2. Transfer slot ownership from old primary to new primary
        let snapshot = cluster_state.snapshot();
        let slots = snapshot.get_node_slots(primary_id);

        for range in slots {
            let slot_cmd = ClusterCommand::AssignSlots {
                node_id: replica_id,
                slots: vec![range],
            };
            if let Err(e) = raft.client_write(slot_cmd).await {
                // Log warning but continue - partial failover is better than none
                tracing::warn!(
                    error = %e,
                    slot_range = ?range,
                    "Failed to transfer slots during failover"
                );
            }
        }

        tracing::info!(
            new_primary = replica_id,
            old_primary = primary_id,
            "Manual failover completed"
        );

        Response::ok()
    }

    /// Handle a MIGRATE command asynchronously.
    ///
    /// This method performs the actual key migration:
    /// 1. Serialize the key(s) with DUMP format
    /// 2. Connect to the target server
    /// 3. Authenticate if needed
    /// 4. Send RESTORE command(s)
    /// 5. Delete local key(s) if not COPY
    async fn handle_migrate_command(&mut self, args: Vec<Bytes>) -> Response {
        use crate::migrate::{MigrateArgs, MigrateClient};
        use std::time::Duration;

        // Parse arguments
        let migrate_args = match MigrateArgs::parse(&args) {
            Ok(args) => args,
            Err(e) => return Response::error(e),
        };

        // Must have at least one key to migrate
        if migrate_args.keys.is_empty() {
            return Response::error("ERR No keys to migrate");
        }

        let timeout = Duration::from_millis(migrate_args.timeout_ms);

        // Connect to target server
        let mut client =
            match MigrateClient::connect(&migrate_args.host, migrate_args.port, timeout).await {
                Ok(c) => c,
                Err(e) => return Response::error(format!("IOERR error or timeout {}", e)),
            };

        // Authenticate if needed
        if let Some(ref auth) = migrate_args.auth {
            if let Err(e) = client.auth(&auth.password, auth.username.as_deref()).await {
                return Response::error(format!("ERR Target authentication error: {}", e));
            }
        }

        // Select database if not 0
        if migrate_args.dest_db != 0 {
            if let Err(e) = client.select_db(migrate_args.dest_db).await {
                return Response::error(format!("ERR Target database error: {}", e));
            }
        }

        // Process each key
        let mut migrated_count = 0;
        for key in &migrate_args.keys {
            // Get the key's serialized data using DUMP format
            // We need to send a command to the shard to serialize the key
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let target_shard = shard_for_key(key, self.num_shards);

            if let Some(sender) = self.shard_senders.get(target_shard) {
                let dump_cmd = ParsedCommand::new(Bytes::from("DUMP"), vec![key.clone()]);
                if sender
                    .send(ShardMessage::Execute {
                        command: dump_cmd,
                        conn_id: self.state.id,
                        txid: None,
                        protocol_version: self.state.protocol_version,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    return Response::error("ERR Internal error: shard unreachable");
                }
            } else {
                return Response::error("ERR Internal error: invalid shard");
            }

            let dump_response = match response_rx.await {
                Ok(r) => r,
                Err(_) => return Response::error("ERR Internal error: no response from shard"),
            };

            // Check if key exists (DUMP returns null for missing keys)
            let serialized = match &dump_response {
                Response::Bulk(Some(data)) => data.clone(),
                Response::Bulk(None) | Response::Null => {
                    // Key doesn't exist, skip it (Redis behavior)
                    continue;
                }
                Response::Error(e) => {
                    return Response::error(format!(
                        "ERR Error dumping key: {}",
                        String::from_utf8_lossy(e)
                    ));
                }
                _ => return Response::error("ERR Unexpected DUMP response"),
            };

            // Get TTL for the key
            let (ttl_tx, ttl_rx) = tokio::sync::oneshot::channel();
            if let Some(sender) = self.shard_senders.get(target_shard) {
                let pttl_cmd = ParsedCommand::new(Bytes::from("PTTL"), vec![key.clone()]);
                if sender
                    .send(ShardMessage::Execute {
                        command: pttl_cmd,
                        conn_id: self.state.id,
                        txid: None,
                        protocol_version: self.state.protocol_version,
                        response_tx: ttl_tx,
                    })
                    .await
                    .is_err()
                {
                    return Response::error("ERR Internal error: shard unreachable");
                }
            }

            let ttl = match ttl_rx.await {
                Ok(Response::Integer(t)) => {
                    if t < 0 {
                        0 // No TTL or key doesn't exist
                    } else {
                        t
                    }
                }
                _ => 0,
            };

            // RESTORE the key on target
            if let Err(e) = client
                .restore(key, ttl, &serialized, migrate_args.replace)
                .await
            {
                return Response::error(format!("ERR Target error: {}", e));
            }

            // Delete local key if not COPY
            if !migrate_args.copy {
                let (del_tx, _del_rx) = tokio::sync::oneshot::channel();
                if let Some(sender) = self.shard_senders.get(target_shard) {
                    let del_cmd = ParsedCommand::new(Bytes::from("DEL"), vec![key.clone()]);
                    let _ = sender
                        .send(ShardMessage::Execute {
                            command: del_cmd,
                            conn_id: self.state.id,
                            txid: None,
                            protocol_version: self.state.protocol_version,
                            response_tx: del_tx,
                        })
                        .await;
                }
            }

            migrated_count += 1;
        }

        if migrated_count == 0 && !migrate_args.keys.is_empty() {
            Response::Simple(Bytes::from("NOKEY"))
        } else {
            Response::ok()
        }
    }

    /// Dispatch a command to its connection-level handler.
    ///
    /// This method routes commands to their appropriate handlers based on the
    /// `ConnectionLevelHandler` category. Returns `Some(responses)` if the command
    /// was handled, or `None` if it should fall through to standard routing.
    async fn dispatch_connection_level(
        &mut self,
        handler: ConnectionLevelHandler,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        match handler {
            // Auth handlers - these are handled early in route_and_execute_with_transaction
            // so they shouldn't reach here, but we handle them for completeness
            ConnectionLevelHandler::Auth => Some(vec![self.handle_auth(args).await]),
            ConnectionLevelHandler::Hello => Some(vec![self.handle_hello(args).await]),
            ConnectionLevelHandler::Acl => Some(vec![self.handle_acl_command(args).await]),

            // Pub/Sub handlers
            ConnectionLevelHandler::PubSub => self.dispatch_pubsub(cmd_name, args).await,

            // Sharded Pub/Sub handlers
            ConnectionLevelHandler::ShardedPubSub => self.dispatch_sharded_pubsub(cmd_name, args).await,

            // Transaction handlers
            ConnectionLevelHandler::Transaction => self.dispatch_transaction(cmd_name, args).await,

            // Scripting handlers
            ConnectionLevelHandler::Scripting => self.dispatch_scripting(cmd_name, args).await,

            // Function handlers
            ConnectionLevelHandler::Function => self.dispatch_function(cmd_name, args).await,

            // Admin handlers
            ConnectionLevelHandler::Client => Some(vec![self.handle_client_command(args).await]),
            ConnectionLevelHandler::Config => Some(vec![self.handle_config_command(args).await]),
            ConnectionLevelHandler::Info => Some(vec![self.handle_info(args).await]),
            ConnectionLevelHandler::Debug => self.dispatch_debug(args).await,
            ConnectionLevelHandler::Slowlog => Some(vec![self.handle_slowlog_command(args).await]),
            ConnectionLevelHandler::Memory => Some(vec![self.handle_memory_command(args).await]),
            ConnectionLevelHandler::Latency => Some(vec![self.handle_latency_command(args).await]),
            ConnectionLevelHandler::Status => Some(vec![self.handle_status_command(args).await]),

            // Connection state handlers
            ConnectionLevelHandler::ConnectionState => self.dispatch_connection_state(cmd_name, args).await,

            // Cluster handlers - fall through to standard routing
            ConnectionLevelHandler::Cluster => None,

            // Replication handlers - fall through to standard routing
            ConnectionLevelHandler::Replication => None,
        }
    }

    /// Dispatch pub/sub commands.
    async fn dispatch_pubsub(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "SUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_subscribe(args).await)
            }
            "UNSUBSCRIBE" => Some(self.handle_unsubscribe(args).await),
            "PSUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_psubscribe(args).await)
            }
            "PUNSUBSCRIBE" => Some(self.handle_punsubscribe(args).await),
            "PUBLISH" => {
                if !args.is_empty() {
                    if let Err(err) = self.validate_channel_access(&args[..1]) {
                        return Some(vec![err]);
                    }
                }
                Some(vec![self.handle_publish(args).await])
            }
            "PUBSUB" => Some(vec![self.handle_pubsub_command(args).await]),
            "RESET" => Some(vec![self.handle_reset().await]),
            _ => None,
        }
    }

    /// Dispatch sharded pub/sub commands.
    async fn dispatch_sharded_pubsub(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "SSUBSCRIBE" => {
                if let Err(err) = self.validate_channel_access(args) {
                    return Some(vec![err]);
                }
                Some(self.handle_ssubscribe(args).await)
            }
            "SUNSUBSCRIBE" => Some(self.handle_sunsubscribe(args).await),
            "SPUBLISH" => {
                if !args.is_empty() {
                    if let Err(err) = self.validate_channel_access(&args[..1]) {
                        return Some(vec![err]);
                    }
                }
                Some(vec![self.handle_spublish(args).await])
            }
            _ => None,
        }
    }

    /// Dispatch transaction commands.
    async fn dispatch_transaction(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "MULTI" => Some(vec![self.handle_multi()]),
            "EXEC" => Some(vec![self.handle_exec().await]),
            "DISCARD" => Some(vec![self.handle_discard()]),
            "WATCH" => Some(vec![self.handle_watch(args).await]),
            "UNWATCH" => Some(vec![self.handle_unwatch()]),
            _ => None,
        }
    }

    /// Dispatch scripting commands.
    async fn dispatch_scripting(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "EVAL" => Some(vec![self.handle_eval(args).await]),
            "EVALSHA" => Some(vec![self.handle_evalsha(args).await]),
            "SCRIPT" => Some(vec![self.handle_script(args).await]),
            _ => None,
        }
    }

    /// Dispatch function commands.
    async fn dispatch_function(&mut self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "FCALL" => Some(vec![self.handle_fcall(args, false).await]),
            "FCALL_RO" => Some(vec![self.handle_fcall(args, true).await]),
            "FUNCTION" => Some(vec![self.handle_function(args).await]),
            _ => None,
        }
    }

    /// Dispatch debug commands.
    async fn dispatch_debug(&mut self, args: &[Bytes]) -> Option<Vec<Response>> {
        if args.is_empty() {
            return None; // Fall through to standard routing
        }

        let subcommand = args[0].to_ascii_uppercase();
        match subcommand.as_slice() {
            b"SLEEP" => Some(vec![self.handle_debug_sleep(args).await]),
            b"TRACING" => {
                if args.len() > 1 && args[1].eq_ignore_ascii_case(b"STATUS") {
                    Some(vec![self.handle_debug_tracing_status()])
                } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"RECENT") {
                    Some(vec![self.handle_debug_tracing_recent(args)])
                } else {
                    Some(vec![Response::error(
                        "ERR Unknown DEBUG TRACING subcommand. Use STATUS or RECENT [count].",
                    )])
                }
            }
            b"VLL" => Some(vec![self.handle_debug_vll(args).await]),
            b"BUNDLE" => {
                if args.len() > 1 && args[1].eq_ignore_ascii_case(b"GENERATE") {
                    Some(vec![self.handle_debug_bundle_generate(args).await])
                } else if args.len() > 1 && args[1].eq_ignore_ascii_case(b"LIST") {
                    Some(vec![self.handle_debug_bundle_list()])
                } else {
                    Some(vec![Response::error(
                        "ERR Unknown DEBUG BUNDLE subcommand. Use GENERATE [DURATION <seconds>] or LIST.",
                    )])
                }
            }
            _ => None, // Fall through for other DEBUG subcommands
        }
    }

    /// Dispatch connection state commands.
    async fn dispatch_connection_state(&mut self, cmd_name: &str, _args: &[Bytes]) -> Option<Vec<Response>> {
        match cmd_name {
            "RESET" => Some(vec![self.handle_reset().await]),
            // Note: SELECT, QUIT, PING, ECHO, COMMAND are handled via standard shard routing
            _ => None,
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

        // Run pre-execution checks (auth, admin port, ACL, pub/sub mode)
        if let Some(error_response) = self.run_pre_checks(&cmd_name_str, &cmd.args) {
            return vec![error_response];
        }

        // Category-based dispatch using the command router
        // This handles: pub/sub, transactions, scripting, functions, admin commands
        if let Some(handler) = router::CommandRouter::handler_for_command(&cmd_name_str) {
            if let Some(responses) = self.dispatch_connection_level(handler, &cmd_name_str, &cmd.args).await {
                return responses;
            }
        }

        // Handle persistence commands (need snapshot coordinator)
        if cmd_name_str == "BGSAVE" {
            return vec![self.handle_bgsave(&cmd.args)];
        }
        if cmd_name_str == "LASTSAVE" {
            return vec![self.handle_lastsave()];
        }

        // Handle PSYNC command - validates args and returns handoff signal
        // The actual handoff happens in the run() loop when it detects PSYNC_HANDOFF
        if cmd_name_str == "PSYNC" {
            // Check if we have a primary replication handler (we're running as primary)
            if self.primary_replication_handler.is_none() {
                return vec![Response::error("ERR PSYNC not supported - server is not running as primary")];
            }
            // Execute PSYNC command which will return PSYNC_HANDOFF signal
            return vec![self.route_and_execute(cmd).await];
        }

        // Handle server commands that need scatter-gather routing
        match cmd_name_str.as_ref() {
            "SCAN" => return vec![self.handle_scan(&cmd.args).await],
            "KEYS" => return vec![self.handle_keys(&cmd.args).await],
            "DBSIZE" => return vec![self.handle_dbsize().await],
            "RANDOMKEY" => return vec![self.handle_randomkey().await],
            "FLUSHDB" => return vec![self.handle_flushdb(&cmd.args).await],
            "FLUSHALL" => return vec![self.handle_flushall(&cmd.args).await],
            "MIGRATE" => return vec![self.handle_migrate(&cmd.args).await],
            "SHUTDOWN" => return vec![self.handle_shutdown(&cmd.args).await],
            _ => {}
        }

        // If in transaction mode, queue the command instead of executing
        if self.state.transaction.queue.is_some() {
            // Check if it's a blocking command - not allowed in MULTI
            // Use execution_strategy() for type-safe blocking detection
            if self.is_blocking_command(&cmd_name_str) {
                return vec![Response::error(
                    "ERR Blocking commands are not allowed inside a transaction",
                )];
            }
            return vec![self.queue_command(cmd)];
        }

        // Handle ASKING command (sets connection flag)
        if cmd_name_str == "ASKING" {
            self.state.asking = true;
            return vec![Response::ok()];
        }

        // Handle READONLY command (sets connection flag)
        if cmd_name_str == "READONLY" {
            self.state.readonly = true;
            return vec![Response::ok()];
        }

        // Handle READWRITE command (clears readonly flag)
        if cmd_name_str == "READWRITE" {
            self.state.readonly = false;
            return vec![Response::ok()];
        }

        // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK errors)
        if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
            return vec![cluster_error];
        }

        // Normal execution
        let response = self.route_and_execute(cmd).await;

        // Check if this is a blocking command that needs to wait
        if let Response::BlockingNeeded { keys, timeout, op } = response {
            return vec![self.handle_blocking_wait(keys, timeout, op).await];
        }

        // Check if this is a Raft cluster command that needs async execution
        if let Response::RaftNeeded {
            op,
            register_node,
            unregister_node,
        } = response
        {
            return vec![
                self.handle_raft_command(op, register_node, unregister_node)
                    .await,
            ];
        }

        // Check if this is a MIGRATE command that needs async execution
        if let Response::MigrateNeeded { args } = response {
            return vec![self.handle_migrate_command(args).await];
        }

        vec![response]
    }

    /// Check if a command is a blocking command.
    fn is_blocking_command(&self, cmd_name: &str) -> bool {
        self.registry
            .get_entry(cmd_name)
            .is_some_and(|entry| matches!(entry.execution_strategy(), ExecutionStrategy::Blocking { .. }))
    }

    /// Check if a command is allowed in pub/sub mode.
    fn is_allowed_in_pubsub_mode(&self, cmd_name: &str) -> bool {
        // PING and QUIT are special cases - always allowed
        if matches!(cmd_name, "PING" | "QUIT") {
            return true;
        }
        // Commands with PubSub or ConnectionState strategy are allowed
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
                    | ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
            )
        })
    }

    /// Check if a command is exempt from authentication requirements.
    fn is_auth_exempt(&self, cmd_name: &str) -> bool {
        // PING and QUIT are always allowed
        if matches!(cmd_name, "PING" | "QUIT") {
            return true;
        }
        // Commands with Auth strategy are exempt (they handle their own auth)
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
            )
        })
    }

    /// Validate that the current user has permission to access all specified channels.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Response)` with NOPERM error
    /// if any channel is denied.
    fn validate_channel_access(&self, channels: &[Bytes]) -> Result<(), Response> {
        let Some(user) = self.state.auth.user() else {
            return Ok(());
        };

        for channel in channels {
            if !user.check_channel_access(channel) {
                let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                let channel_str = String::from_utf8_lossy(channel);
                self.acl_manager.log().log_channel_denied(
                    &user.username,
                    &client_info,
                    &channel_str,
                );
                return Err(Response::error(
                    "NOPERM this user has no permissions to access one of the channels used as arguments"
                ));
            }
        }
        Ok(())
    }

    /// Run pre-execution checks for a command.
    ///
    /// This validates:
    /// - Authentication (if required)
    /// - Admin port restrictions
    /// - ACL command permissions
    /// - Pub/sub mode restrictions
    ///
    /// Returns `Some(Response)` with an error if the command should be rejected,
    /// or `None` if the command can proceed.
    fn run_pre_checks(&self, cmd_name: &str, args: &[Bytes]) -> Option<Response> {
        // Check authentication
        if !self.state.auth.is_authenticated() && !self.is_auth_exempt(cmd_name) {
            return Some(Response::error("NOAUTH Authentication required."));
        }

        // Block admin commands on regular port when admin port is enabled
        if self.admin_enabled && !self.is_admin {
            if let Some(cmd_info) = self.registry.get(cmd_name) {
                if cmd_info.flags().contains(CommandFlags::ADMIN) {
                    return Some(Response::error(
                        "NOADMIN Admin commands are disabled on this port. Use the admin port."
                    ));
                }
            }
        }

        // Check command ACL permission
        // Note: ACL command is exempt (users need ACL WHOAMI to check their identity)
        if let Some(user) = self.state.auth.user() {
            let subcommand = extract_subcommand(cmd_name, args);
            if cmd_name != "ACL" && !user.check_command(cmd_name, subcommand.as_deref()) {
                let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                // Log with subcommand if present
                let log_cmd = if let Some(ref sub) = subcommand {
                    format!("{}|{}", cmd_name.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name.to_lowercase()
                };
                self.acl_manager.log().log_command_denied(&user.username, &client_info, &log_cmd);
                warn!(
                    conn_id = self.state.id,
                    username = %user.username,
                    command = %log_cmd,
                    "ACL denied command"
                );
                // Return error with subcommand in message if present
                let err_cmd = if let Some(ref sub) = subcommand {
                    format!("{} {}", cmd_name.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name.to_lowercase()
                };
                return Some(Response::error(format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    err_cmd
                )));
            }
        }

        // Check pub/sub mode restrictions
        if self.state.pubsub.in_pubsub_mode() && !self.is_allowed_in_pubsub_mode(cmd_name) {
            return Some(Response::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd_name
            )));
        }

        None
    }

    /// Check if a command is exempt from slot validation in cluster mode.
    /// Connection-level and admin commands that don't operate on specific keys are exempt.
    fn is_cluster_exempt(&self, cmd_name: &str) -> bool {
        // Certain commands are always exempt
        if matches!(
            cmd_name,
            "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG"
        ) {
            return true;
        }
        // Connection-level commands and scatter-gather commands are exempt
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(_) | ExecutionStrategy::ScatterGather { .. }
            )
        })
    }

    /// Validate slot ownership for keys in cluster mode.
    /// Returns Some(Response) if validation fails, None if OK.
    fn validate_cluster_slots(&mut self, cmd: &ParsedCommand) -> Option<Response> {
        // Only validate if cluster mode is enabled
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;

        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);

        // Skip cluster-exempt commands (using execution strategy for type-safe check)
        if self.is_cluster_exempt(&cmd_name) {
            return None;
        }

        // Get keys from command using the registry
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None; // Unknown command, let execute handle it
        };

        // No keys = no slot validation needed
        if keys.is_empty() {
            return None;
        }

        // Calculate slot for first key
        let first_slot = slot_for_key(keys[0]);

        // CROSSSLOT check - all keys must hash to same slot
        for key in &keys[1..] {
            let slot = slot_for_key(key);
            if slot != first_slot {
                return Some(Response::error(
                    "CROSSSLOT Keys in request don't hash to the same slot",
                ));
            }
        }

        // Check slot ownership
        let snapshot = cluster_state.snapshot();

        match snapshot.slot_assignment.get(&first_slot) {
            Some(&owner) if owner == node_id => {
                // We own this slot - check for migration
                if let Some(migration) = snapshot.migrations.get(&first_slot) {
                    // Slot is being migrated
                    if !self.state.asking {
                        // Client needs to follow the migration
                        if let Some(target_node) = snapshot.nodes.get(&migration.target_node) {
                            return Some(Response::error(format!(
                                "ASK {} {}:{}",
                                first_slot,
                                target_node.addr.ip(),
                                target_node.addr.port()
                            )));
                        }
                    }
                    // Clear ASKING flag after use
                    self.state.asking = false;
                }
                None // We own it and no migration issues
            }
            Some(&owner) => {
                // Another node owns this slot
                if let Some(owner_node) = snapshot.nodes.get(&owner) {
                    Some(Response::error(format!(
                        "MOVED {} {}:{}",
                        first_slot,
                        owner_node.addr.ip(),
                        owner_node.addr.port()
                    )))
                } else {
                    Some(Response::error(format!(
                        "CLUSTERDOWN Hash slot {} not served",
                        first_slot
                    )))
                }
            }
            None => {
                // Slot not assigned
                Some(Response::error(format!(
                    "CLUSTERDOWN Hash slot {} not served",
                    first_slot
                )))
            }
        }
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
                info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                self.state.auth = AuthState::Authenticated(user);
                Response::ok()
            }
            Err(e) => {
                let username = if args.len() > 1 {
                    String::from_utf8_lossy(&args[0]).to_string()
                } else {
                    "default".to_string()
                };
                warn!(
                    conn_id = self.state.id,
                    addr = %self.state.addr,
                    username = %username,
                    reason = %e,
                    "Authentication failed"
                );
                Response::error(e.to_string())
            }
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
                    info!(conn_id = self.state.id, username = %user.username, "Client authenticated");
                    self.state.auth = AuthState::Authenticated(user);
                }
                Err(_) => {
                    warn!(
                        conn_id = self.state.id,
                        addr = %self.state.addr,
                        username = %username_str,
                        reason = "invalid username-password pair or user is disabled",
                        "Authentication failed"
                    );
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

        debug!(conn_id = self.state.id, "Transaction started");
        self.state.transaction.queue = Some(Vec::new());
        self.state.transaction.target = TransactionTarget::None;
        self.state.transaction.exec_abort = false;
        self.state.transaction.queued_errors.clear();
        self.state.transaction.start_time = Some(std::time::Instant::now());

        Response::ok()
    }

    /// Handle EXEC command - execute the queued transaction.
    async fn handle_exec(&mut self) -> Response {
        // Capture start time for duration metric
        let start_time = self.state.transaction.start_time;

        // Helper to record transaction metrics
        let record_transaction_metrics = |recorder: &Arc<dyn MetricsRecorder>,
                                          outcome: &str,
                                          queued_count: usize,
                                          start_time: Option<std::time::Instant>| {
            recorder.increment_counter(
                "frogdb_transactions_total",
                1,
                &[("outcome", outcome)],
            );
            recorder.record_histogram(
                "frogdb_transactions_queued_commands",
                queued_count as f64,
                &[("outcome", outcome)],
            );
            if let Some(start) = start_time {
                recorder.record_histogram(
                    "frogdb_transactions_duration_seconds",
                    start.elapsed().as_secs_f64(),
                    &[("outcome", outcome)],
                );
            }
        };

        // Check if in transaction mode
        let queue = match self.state.transaction.queue.take() {
            Some(q) => q,
            None => return Response::error("ERR EXEC without MULTI"),
        };

        let queued_count = queue.len();

        // Get watches and clear them
        let watches: Vec<(Bytes, u64)> = self.state.transaction.watches
            .drain()
            .map(|(key, (_, ver))| (key, ver))
            .collect();

        // Check if we should abort due to queuing errors
        if self.state.transaction.exec_abort {
            record_transaction_metrics(&self.metrics_recorder, "execabort", queued_count, start_time);
            self.clear_transaction_state();
            return Response::error("EXECABORT Transaction discarded because of previous errors.");
        }

        // Handle empty transaction
        if queue.is_empty() {
            record_transaction_metrics(&self.metrics_recorder, "committed", 0, start_time);
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
                record_transaction_metrics(&self.metrics_recorder, "crossslot", queued_count, start_time);
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
            record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
            return Response::error("ERR shard unavailable");
        }

        // Wait for result
        match response_rx.await {
            Ok(TransactionResult::Success(results)) => {
                let duration_ms = start_time.map(|s| s.elapsed().as_millis() as u64).unwrap_or(0);
                debug!(
                    conn_id = self.state.id,
                    commands_count = queued_count,
                    duration_ms,
                    "Transaction executed"
                );
                record_transaction_metrics(&self.metrics_recorder, "committed", queued_count, start_time);
                Response::Array(results)
            }
            Ok(TransactionResult::WatchAborted) => {
                debug!(conn_id = self.state.id, "Transaction aborted due to WATCH conflict");
                record_transaction_metrics(&self.metrics_recorder, "watch_aborted", queued_count, start_time);
                Response::null()
            }
            Ok(TransactionResult::Error(e)) => {
                record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                Response::error(e)
            }
            Err(_) => {
                record_transaction_metrics(&self.metrics_recorder, "error", queued_count, start_time);
                Response::error("ERR shard dropped request")
            }
        }
    }

    /// Handle DISCARD command - abort the transaction.
    fn handle_discard(&mut self) -> Response {
        if self.state.transaction.queue.is_none() {
            return Response::error("ERR DISCARD without MULTI");
        }

        // Record transaction metrics
        let queued_count = self.state.transaction.queue.as_ref().map_or(0, |q| q.len());
        self.metrics_recorder.increment_counter(
            "frogdb_transactions_total",
            1,
            &[("outcome", "discarded")],
        );
        self.metrics_recorder.record_histogram(
            "frogdb_transactions_queued_commands",
            queued_count as f64,
            &[("outcome", "discarded")],
        );
        if let Some(start) = self.state.transaction.start_time {
            self.metrics_recorder.record_histogram(
                "frogdb_transactions_duration_seconds",
                start.elapsed().as_secs_f64(),
                &[("outcome", "discarded")],
            );
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

        match scatter_op.and_then(|op| strategy_for_op(&op)) {
            Some(strategy) => {
                let executor = ScatterGatherExecutor::new(
                    self.shard_senders.clone(),
                    self.scatter_gather_timeout,
                    self.metrics_recorder.clone(),
                    self.state.id,
                );
                executor.execute(strategy.as_ref(), &cmd.args).await
            }
            None => {
                // Command doesn't support scatter-gather
                Response::error("CROSSSLOT Keys in request don't hash to the same slot")
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
            conn_id: self.state.id,
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
            conn_id: self.state.id,
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
                conn_id: self.state.id,
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
                    conn_id: self.state.id,
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

            debug!(
                conn_id = self.state.id,
                channel = %String::from_utf8_lossy(channel),
                "Subscribed to channel"
            );

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

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self.execute_single_shard_script(script_source, keys, argv, 0).await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys.iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script(script_source, keys, argv, shards[0]).await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script(script_source, keys, argv, shards).await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a Lua script on a single shard.
    async fn execute_single_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a Lua script across multiple shards using VLL continuation locks.
    ///
    /// This acquires continuation locks on all involved shards (in sorted order to
    /// prevent deadlocks), executes the script on the primary shard, then releases
    /// all locks.
    async fn execute_cross_shard_script(
        &self,
        script_source: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0]; // Execute on first shard

        // Phase 1: Acquire continuation locks on all shards (in sorted order)
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> = Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.shard_senders[shard_id].send(msg).await.is_err() {
                // Abort: release already-locked shards
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {
                    // Shard is ready
                }
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    // Lock acquisition failed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    // Channel closed - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    // Timeout - release all locks
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScript {
            script_source,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        let response = if self.shard_senders[primary_shard].send(msg).await.is_ok() {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
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

        // Determine which shards are involved
        if keys.is_empty() {
            // No keys -> single shard (shard 0)
            return self.execute_single_shard_script_sha(script_sha, keys, argv, 0).await;
        }

        // Collect unique shards in sorted order
        let mut shards: Vec<usize> = keys.iter()
            .map(|k| shard_for_key(k, self.num_shards))
            .collect();
        shards.sort();
        shards.dedup();

        if shards.len() == 1 {
            // Single shard - use simple path
            self.execute_single_shard_script_sha(script_sha, keys, argv, shards[0]).await
        } else if self.allow_cross_slot {
            // Cross-shard script - use VLL continuation locks
            self.execute_cross_shard_script_sha(script_sha, keys, argv, shards).await
        } else {
            // Cross-slot not allowed
            Response::error("CROSSSLOT Keys in request don't hash to the same slot")
        }
    }

    /// Execute a cached Lua script (by SHA) on a single shard.
    async fn execute_single_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shard_id: usize,
    ) -> Response {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }

    /// Execute a cached Lua script (by SHA) across multiple shards using VLL continuation locks.
    async fn execute_cross_shard_script_sha(
        &self,
        script_sha: Bytes,
        keys: Vec<Bytes>,
        argv: Vec<Bytes>,
        shards: Vec<usize>, // Already sorted and deduplicated
    ) -> Response {
        use std::time::Duration;

        let txid = next_txid();
        let primary_shard = shards[0];

        // Phase 1: Acquire continuation locks on all shards
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<oneshot::Receiver<ShardReadyResult>> = Vec::with_capacity(shards.len());

        for &shard_id in &shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            let msg = ShardMessage::VllContinuationLock {
                txid,
                conn_id: self.state.id,
                ready_tx,
                release_rx,
            };

            if self.shard_senders[shard_id].send(msg).await.is_err() {
                for tx in release_txs {
                    let _ = tx.send(());
                }
                return Response::error("ERR shard unavailable");
            }

            release_txs.push(release_tx);
            ready_rxs.push(ready_rx);
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        for (i, ready_rx) in ready_rxs.into_iter().enumerate() {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!("ERR lock acquisition failed: {}", e));
                }
                Ok(Err(_)) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error("ERR shard dropped lock request");
                }
                Err(_) => {
                    for tx in release_txs {
                        let _ = tx.send(());
                    }
                    return Response::error(format!(
                        "ERR lock acquisition timeout on shard {}",
                        shards[i]
                    ));
                }
            }
        }

        // Phase 3: Execute script on primary shard
        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::EvalScriptSha {
            script_sha,
            keys,
            argv,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            response_tx,
        };

        let response = if self.shard_senders[primary_shard].send(msg).await.is_ok() {
            match response_rx.await {
                Ok(resp) => resp,
                Err(_) => Response::error("ERR script execution failed"),
            }
        } else {
            Response::error("ERR shard unavailable")
        };

        // Phase 4: Release all locks
        for tx in release_txs {
            let _ = tx.send(());
        }

        response
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
            let mut registry = match self.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
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

        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
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
            let mut registry = match self.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
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
            let mut registry = match self.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            registry.flush();
        }

        // Persist to disk (empty state)
        self.persist_functions();

        Response::ok()
    }

    /// Handle FUNCTION STATS.
    fn handle_function_stats(&self) -> Response {
        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
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
        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(_) => return Response::error("ERR internal lock contention"),
        };
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
            let mut registry = match self.function_registry.try_write_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };

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
        let registry = match self.function_registry.try_read_err() {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to acquire function registry lock for persistence");
                return;
            }
        };

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
                conn_id: self.state.id,
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
                conn_id: self.state.id,
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
                conn_id: self.state.id,
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
                conn_id: self.state.id,
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
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[selected_shard].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => {
                // Return the random key (or null if shard is now empty)
                if let Some((_, response)) = partial.results.into_iter().next() {
                    response
                } else {
                    Response::null()
                }
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
                conn_id: self.state.id,
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

    /// Handle DEBUG TRACING STATUS command.
    fn handle_debug_tracing_status(&self) -> Response {
        match &self.shared_tracer {
            Some(tracer) => {
                let status = tracer.get_status();
                let lines = vec![
                    format!("enabled:{}", if status.enabled { "yes" } else { "no" }),
                    format!("sampling_rate:{}", status.sampling_rate),
                    format!("otlp_endpoint:{}", status.otlp_endpoint),
                    format!("service_name:{}", status.service_name),
                    format!("recent_traces_count:{}", status.recent_traces_count),
                    format!("scatter_gather_spans:{}", status.scatter_gather_spans),
                    format!("shard_spans:{}", status.shard_spans),
                    format!("persistence_spans:{}", status.persistence_spans),
                ];
                Response::Bulk(Some(Bytes::from(lines.join("\r\n"))))
            }
            None => Response::Bulk(Some(Bytes::from("enabled:no\r\nreason:tracer not configured"))),
        }
    }

    /// Handle DEBUG TRACING RECENT [count] command.
    fn handle_debug_tracing_recent(&self, args: &[Bytes]) -> Response {
        // args[0] = "TRACING", args[1] = "RECENT", args[2] = optional count
        let count = args
            .get(2)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(10);

        match &self.shared_tracer {
            Some(tracer) => {
                let traces = tracer.get_recent_traces(count);
                let entries: Vec<Response> = traces
                    .iter()
                    .map(|t| {
                        Response::Array(vec![
                            Response::Bulk(Some(Bytes::from(t.trace_id.clone()))),
                            Response::Integer(t.timestamp_ms as i64),
                            Response::Bulk(Some(Bytes::from(t.command.clone()))),
                            Response::Integer(if t.sampled { 1 } else { 0 }),
                        ])
                    })
                    .collect();
                Response::Array(entries)
            }
            None => Response::Array(vec![]),
        }
    }

/// Handle DEBUG VLL [shard_id] command.
    async fn handle_debug_vll(&self, args: &[Bytes]) -> Response {
        // args[0] = "VLL", args[1] = optional shard_id
        let shard_filter: Option<usize> = if args.len() > 1 {
            match std::str::from_utf8(&args[1]) {
                Ok(s) => match s.parse::<usize>() {
                    Ok(id) => {
                        if id >= self.shard_senders.len() {
                            return Response::error(format!(
                                "ERR invalid shard_id: {} (num_shards: {})",
                                id,
                                self.shard_senders.len()
                            ));
                        }
                        Some(id)
                    }
                    Err(_) => {
                        return Response::error("ERR invalid shard_id: must be a number");
                    }
                },
                Err(_) => {
                    return Response::error("ERR invalid shard_id: must be valid UTF-8");
                }
            }
        } else {
            None
        };

        let infos = self.gather_vll_queue_info(shard_filter).await;
        self.format_vll_response(infos)
    }

    /// Gather VLL queue info from shards.
    async fn gather_vll_queue_info(
        &self,
        shard_filter: Option<usize>,
    ) -> Vec<frogdb_core::shard::VllQueueInfo> {
        use tokio::sync::oneshot;

        let mut results = Vec::new();
        let timeout = std::time::Duration::from_secs(5);

        let shard_ids: Vec<usize> = match shard_filter {
            Some(id) => vec![id],
            None => (0..self.shard_senders.len()).collect(),
        };

        for shard_id in shard_ids {
            let (response_tx, response_rx) = oneshot::channel();

            let send_result = self.shard_senders[shard_id]
                .send(frogdb_core::shard::ShardMessage::GetVllQueueInfo { response_tx })
                .await;

            if send_result.is_err() {
                tracing::warn!(shard_id, "Failed to send GetVllQueueInfo message");
                continue;
            }

            match tokio::time::timeout(timeout, response_rx).await {
                Ok(Ok(info)) => {
                    results.push(info);
                }
                Ok(Err(_)) => {
                    tracing::warn!(shard_id, "Channel closed while waiting for VLL info");
                }
                Err(_) => {
                    tracing::warn!(shard_id, "Timeout waiting for VLL info");
                }
            }
        }

        results
    }

    /// Format VLL queue info as a response.
    fn format_vll_response(&self, infos: Vec<frogdb_core::shard::VllQueueInfo>) -> Response {
        // Check if all queues are empty
        let all_empty = infos.iter().all(|i| {
            i.queue_depth == 0
                && i.continuation_lock.is_none()
                && i.intent_table.is_empty()
        });

        if all_empty {
            return Response::Bulk(Some(Bytes::from("# VLL queues are empty")));
        }

        let mut lines = Vec::new();

        for info in infos {
            // Shard header
            let mut header = format!("shard:{} queue_depth:{}", info.shard_id, info.queue_depth);
            if let Some(txid) = info.executing_txid {
                header.push_str(&format!(" executing_txid:{}", txid));
            }
            lines.push(header);

            // Continuation lock
            if let Some(ref lock) = info.continuation_lock {
                lines.push(format!(
                    "continuation_lock: txid:{} conn_id:{} age_ms:{}",
                    lock.txid, lock.conn_id, lock.age_ms
                ));
            }

            // Pending operations
            if !info.pending_ops.is_empty() {
                lines.push("pending:".to_string());
                for op in &info.pending_ops {
                    lines.push(format!(
                        "  txid:{} operation:{} keys:{} state:{} age_ms:{}",
                        op.txid, op.operation, op.key_count, op.state, op.age_ms
                    ));
                }
            }

            // Intent table
            if !info.intent_table.is_empty() {
                lines.push("intents:".to_string());
                for intent in &info.intent_table {
                    let txids_str: Vec<String> =
                        intent.txids.iter().map(|t| t.to_string()).collect();
                    lines.push(format!(
                        "  key:{} txids:[{}] lock:{}",
                        intent.key,
                        txids_str.join(","),
                        intent.lock_state
                    ));
                }
            }

            // Empty line between shards
            lines.push(String::new());
        }

        // Remove trailing empty line
        if lines.last().map(|s| s.is_empty()).unwrap_or(false) {
            lines.pop();
        }

        Response::Bulk(Some(Bytes::from(lines.join("\n"))))
    }

    /// Handle DEBUG BUNDLE GENERATE [DURATION <seconds>] command.
    ///
    /// Generates a diagnostic bundle and returns the bundle ID.
    /// The bundle can be downloaded via HTTP: GET /debug/api/bundle/<id>
    async fn handle_debug_bundle_generate(&self, args: &[Bytes]) -> Response {
        // args[0] = "BUNDLE", args[1] = "GENERATE", args[2..] = optional DURATION <seconds>
        let mut duration_secs: u64 = 0;

        // Parse optional DURATION argument
        let mut i = 2;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"DURATION") {
                if i + 1 >= args.len() {
                    return Response::error("ERR DURATION requires a value in seconds");
                }
                match std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(d) => duration_secs = d,
                    None => return Response::error("ERR DURATION must be a positive integer"),
                }
                i += 2;
            } else {
                return Response::error(format!(
                    "ERR Unknown argument '{}' for DEBUG BUNDLE GENERATE",
                    String::from_utf8_lossy(&args[i])
                ));
            }
        }

        // Create bundle config and collector
        let config = frogdb_metrics::BundleConfig::default();
        // Convert Option<Arc<OtelTracer>> to Option<Arc<dyn TraceProvider>>
        let trace_provider: Option<std::sync::Arc<dyn frogdb_metrics::TraceProvider>> =
            self.shared_tracer.clone().map(|t| t as std::sync::Arc<dyn frogdb_metrics::TraceProvider>);
        let collector = frogdb_metrics::DiagnosticCollector::new(
            self.shard_senders.clone(),
            trace_provider,
            config.clone(),
        );

        // Collect diagnostic data
        let data = if duration_secs == 0 {
            collector.collect_instant().await
        } else {
            collector.collect_with_duration(duration_secs).await
        };

        // Generate the bundle
        let generator = frogdb_metrics::BundleGenerator::new(config.clone());
        let id = frogdb_metrics::BundleGenerator::generate_id();

        match generator.create_zip(&id, &data, duration_secs) {
            Ok(zip_data) => {
                // Try to store the bundle for later HTTP download
                let store = frogdb_metrics::BundleStore::new(config);
                if let Err(e) = store.store(&id, &zip_data) {
                    tracing::warn!(error = %e, "Failed to store bundle (HTTP download may not work)");
                }

                // Return the bundle ID
                Response::Bulk(Some(Bytes::from(id)))
            }
            Err(e) => Response::error(format!("ERR Failed to generate bundle: {}", e)),
        }
    }

    /// Handle DEBUG BUNDLE LIST command.
    ///
    /// Lists all available bundles with their ID, timestamp, and size.
    fn handle_debug_bundle_list(&self) -> Response {
        let config = frogdb_metrics::BundleConfig::default();
        let store = frogdb_metrics::BundleStore::new(config);
        let bundles = store.list();

        let entries: Vec<Response> = bundles
            .into_iter()
            .map(|b| {
                Response::Array(vec![
                    Response::Bulk(Some(Bytes::from(b.id))),
                    Response::Bulk(Some(Bytes::from(b.created_at))),
                    Response::Integer(b.size_bytes as i64),
                ])
            })
            .collect();

        Response::Array(entries)
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
    // SLOWLOG helpers (command handlers in handlers/slowlog.rs)
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

    // =========================================================================
    // STATUS command handlers
    // =========================================================================

    /// Handle STATUS command and dispatch to subcommands.
    async fn handle_status_command(&self, args: &[Bytes]) -> Response {
        if args.is_empty() {
            // STATUS without subcommand shows help (like FrogDB-specific behavior)
            return self.handle_status_help();
        }

        let subcommand = args[0].to_ascii_uppercase();
        let subcommand_str = String::from_utf8_lossy(&subcommand);

        match subcommand_str.as_ref() {
            "JSON" => self.handle_status_json().await,
            "HELP" => self.handle_status_help(),
            _ => Response::error(format!(
                "ERR unknown subcommand '{}'. Try STATUS HELP.",
                subcommand_str
            )),
        }
    }

    /// Handle STATUS JSON - return machine-readable server status.
    async fn handle_status_json(&self) -> Response {
        // Gather shard stats
        let shard_stats = self.gather_memory_stats().await;

        // Build status response
        let now = std::time::SystemTime::now();
        let timestamp = now
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Calculate ISO 8601 timestamp
        let timestamp_iso = format_timestamp_iso(timestamp);

        // Get client info
        let clients = self.client_registry.list();
        let blocked_clients = clients
            .iter()
            .filter(|c| c.flags.contains(frogdb_core::ClientFlags::BLOCKED))
            .count();

        // Calculate totals from shard stats
        let total_keys: usize = shard_stats.iter().map(|s| s.keys).sum();
        let used_bytes: u64 = shard_stats.iter().map(|s| s.data_memory as u64).sum();
        let peak_bytes: u64 = shard_stats.iter().map(|s| s.peak_memory as u64).sum();

        // Build shards array
        let shards: Vec<serde_json::Value> = shard_stats
            .iter()
            .enumerate()
            .map(|(id, stats)| {
                serde_json::json!({
                    "id": id,
                    "keys": stats.keys,
                    "memory_bytes": stats.data_memory,
                    "peak_memory_bytes": stats.peak_memory
                })
            })
            .collect();

        // Build the status JSON
        let status = serde_json::json!({
            "frogdb": {
                "version": env!("CARGO_PKG_VERSION"),
                "uptime_secs": 0, // Would need start_time tracking
                "process_id": std::process::id(),
                "timestamp": timestamp,
                "timestamp_iso": timestamp_iso
            },
            "cluster": {
                "database_available": true,
                "mode": "standalone",
                "num_shards": self.num_shards
            },
            "health": {
                "status": "healthy",
                "issues": []
            },
            "clients": {
                "connected": clients.len(),
                "max_clients": 0, // Would need config access
                "blocked": blocked_clients
            },
            "memory": {
                "used_bytes": used_bytes,
                "peak_bytes": peak_bytes,
                "limit_bytes": 0, // Would need config access
                "fragmentation_ratio": 1.0
            },
            "persistence": {
                "enabled": false
            },
            "shards": shards,
            "keyspace": {
                "total_keys": total_keys,
                "expired_keys_total": 0
            },
            "commands": {
                "total_processed": 0,
                "ops_per_sec": 0.0
            }
        });

        // Pretty-print the JSON
        let json_str = serde_json::to_string_pretty(&status).unwrap_or_else(|_| "{}".to_string());
        Response::bulk(Bytes::from(json_str))
    }

    /// Handle STATUS HELP - show subcommand help.
    fn handle_status_help(&self) -> Response {
        let help = vec![
            Response::bulk(Bytes::from_static(
                b"STATUS <subcommand> [<arg> ...]. Subcommands are:",
            )),
            Response::bulk(Bytes::from_static(b"JSON")),
            Response::bulk(Bytes::from_static(
                b"    Return machine-readable server status as JSON.",
            )),
            Response::bulk(Bytes::from_static(b"HELP")),
            Response::bulk(Bytes::from_static(b"    Show this help.")),
        ];
        Response::Array(help)
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
