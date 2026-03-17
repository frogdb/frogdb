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
pub(crate) mod dispatch;
pub(crate) mod guards;
pub mod handlers;
pub mod router;
pub(crate) mod routing;
pub mod state;
pub(crate) mod util;

// Re-export dependency groups
pub use deps::{
    AdminDeps, ClusterDeps, ConnectionConfig, ConnectionDeps, CoreDeps, ObservabilityDeps,
};

// Re-export state types
pub use state::{
    AuthState, BlockedState, ConnectionState, LocalClientStats, PubSubState, ReplyMode,
    STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS, TrackingMode, TrackingState,
    TransactionState, TransactionTarget,
};

// Re-export builder
pub use builder::{ConnectionHandlerBuilder, connection_builder, standalone_config};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use frogdb_core::{
    AclManager, ClientHandle, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    CommandFlags, CommandRegistry, InvalidationMessage, InvalidationSender, MetricsRecorder,
    PauseMode, PubSubMessage, PubSubSender, ReplicationTrackerImpl, ShardMessage,
    SharedFunctionRegistry, command::QuorumChecker, persistence::SnapshotCoordinator,
};
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use frogdb_telemetry::SharedTracer;
use futures::{SinkExt, StreamExt};
use redis_protocol::codec::Resp2;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug, info, trace, warn};

use crate::cluster_pubsub::ClusterPubSubForwarder;
use crate::config::TracingConfig;
use crate::net::TcpStream;
use crate::replication::PrimaryReplicationHandler;
use crate::runtime_config::ConfigManager;
// Re-export next_txid for handler modules
pub use crate::server::next_txid;

// Re-export utility functions used by handler submodules and internally
pub(crate) use util::{
    estimate_command_size, estimate_resp2_frame_size, extract_subcommand, key_access_type_for_flags,
};

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
    /// Lazily initialized on first pub/sub command (~1% of connections use pub/sub).
    pubsub_tx: Option<PubSubSender>,

    /// Receiver for pub/sub messages from shards.
    /// Lazily initialized on first pub/sub command.
    pubsub_rx: Option<mpsc::UnboundedReceiver<PubSubMessage>>,

    /// Sender for invalidation messages (cloned to shards when tracking enabled).
    /// Lazily initialized on CLIENT TRACKING ON.
    invalidation_tx: Option<InvalidationSender>,

    /// Receiver for invalidation messages from shards.
    /// Lazily initialized on CLIENT TRACKING ON.
    invalidation_rx: Option<mpsc::UnboundedReceiver<InvalidationMessage>>,

    /// Whether the next command's reads should be tracked (computed before dispatch).
    pending_track_reads: bool,

    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,

    /// ACL manager for authentication and authorization.
    acl_manager: Arc<AclManager>,

    /// Snapshot coordinator for BGSAVE/LASTSAVE commands.
    snapshot_coordinator: Arc<dyn SnapshotCoordinator>,

    /// Function registry for FUNCTION/FCALL commands.
    function_registry: SharedFunctionRegistry,

    /// Cursor store for FT.AGGREGATE WITHCURSOR / FT.CURSOR.
    pub(crate) cursor_store: Option<Arc<crate::cursor_store::AggregateCursorStore>>,

    /// Optional shared tracer for distributed tracing.
    shared_tracer: Option<SharedTracer>,

    /// Tracing configuration.
    _tracing_config: TracingConfig,

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
    _hotshards_config: frogdb_debug::HotShardConfig,

    /// Memory diagnostics configuration.
    memory_diag_config: frogdb_debug::MemoryDiagConfig,

    /// Optional latency band tracker for SLO monitoring.
    band_tracker: Option<Arc<frogdb_telemetry::LatencyBandTracker>>,

    /// Optional Raft instance (only when cluster mode is enabled).
    raft: Option<Arc<ClusterRaft>>,

    /// Optional network factory for cluster node management.
    network_factory: Option<Arc<ClusterNetworkFactory>>,

    /// Optional primary replication handler for PSYNC connection handoff.
    primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,

    /// Pending PSYNC handoff parameters (replication_id, offset).
    /// Set when PSYNC command returns PSYNC_HANDOFF, processed after the loop.
    pending_psync_handoff: Option<(String, i64)>,

    /// Reusable buffer for RESP3 encoding to avoid per-response allocation.
    resp3_buf: BytesMut,

    /// Whether per-request tracing spans are enabled (shared AtomicBool).
    per_request_spans: Arc<std::sync::atomic::AtomicBool>,

    /// Whether this server is a replica (rejects write commands from clients).
    /// Shared across all connections so REPLICAOF NO ONE takes effect immediately.
    is_replica: Arc<std::sync::atomic::AtomicBool>,

    /// Optional quorum checker for self-fencing (write rejection on quorum loss).
    quorum_checker: Option<Arc<dyn QuorumChecker>>,

    /// Optional pub/sub forwarder for cross-node message delivery in cluster mode.
    cluster_pubsub_forwarder: Option<Arc<ClusterPubSubForwarder>>,

    /// MONITOR command broadcaster (shared across all connections).
    monitor_broadcaster: Arc<crate::monitor::MonitorBroadcaster>,

    /// MONITOR subscription receiver (set when MONITOR command is executed).
    monitor_rx: Option<tokio::sync::broadcast::Receiver<Arc<crate::monitor::MonitorEvent>>>,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    chaos_config: Arc<crate::config::ChaosConfig>,
}

/// Result of processing a single command frame.
enum FrameAction {
    /// Command processed normally, keep going.
    Continue,
    /// Connection should close (QUIT, PSYNC handoff, disconnect).
    Break,
    /// Response was skipped (ReplyMode::Off or skip_next_reply).
    SkipResponse,
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
    #[allow(clippy::too_many_arguments)]
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
            pubsub_tx: None,
            pubsub_rx: None,
            invalidation_tx: None,
            invalidation_rx: None,
            pending_track_reads: false,
            metrics_recorder: core.metrics_recorder,
            acl_manager: core.acl_manager,
            snapshot_coordinator: admin.snapshot_coordinator,
            function_registry: admin.function_registry,
            cursor_store: Some(admin.cursor_store),
            shared_tracer: observability.shared_tracer,
            _tracing_config: observability.tracing_config,
            replication_tracker: cluster.replication_tracker,
            cluster_state: cluster.cluster_state,
            node_id: cluster.node_id,
            is_admin: config.is_admin,
            admin_enabled: config.admin_enabled,
            _hotshards_config: config.hotshards_config,
            memory_diag_config: config.memory_diag_config,
            band_tracker: observability.band_tracker,
            raft: cluster.raft,
            network_factory: cluster.network_factory,
            primary_replication_handler: cluster.primary_replication_handler,
            pending_psync_handoff: None,
            resp3_buf: BytesMut::with_capacity(4096),
            per_request_spans: config.per_request_spans,
            is_replica: config.is_replica,
            quorum_checker: cluster.quorum_checker,
            #[cfg(feature = "turmoil")]
            chaos_config: config.chaos_config.clone(),
            cluster_pubsub_forwarder: cluster.pubsub_forwarder,
            monitor_broadcaster: observability.monitor_broadcaster,
            monitor_rx: None,
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
        hotshards_config: frogdb_debug::HotShardConfig,
        memory_diag_config: frogdb_debug::MemoryDiagConfig,
        band_tracker: Option<Arc<frogdb_telemetry::LatencyBandTracker>>,
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
            cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
        };
        let cluster = ClusterDeps {
            cluster_state,
            node_id,
            raft,
            network_factory,
            replication_tracker,
            primary_replication_handler,
            quorum_checker: None,
            pubsub_forwarder: None,
        };
        let config = ConnectionConfig {
            num_shards,
            allow_cross_slot,
            scatter_gather_timeout: Duration::from_millis(scatter_gather_timeout_ms),
            is_admin,
            admin_enabled,
            hotshards_config,
            memory_diag_config,
            per_request_spans: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            is_replica: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "turmoil")]
            chaos_config: Arc::new(crate::config::ChaosConfig::default()),
        };
        let observability = ObservabilityDeps {
            shared_tracer,
            tracing_config,
            band_tracker,
            monitor_broadcaster: Arc::new(crate::monitor::MonitorBroadcaster::new(4096)),
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
    async fn send_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        // NullArray requires raw bytes (*-1\r\n) in RESP2 which the redis-protocol
        // crate cannot produce (its Null is always $-1\r\n). In RESP3, null is just _\r\n.
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            match self.state.protocol_version {
                ProtocolVersion::Resp2 => {
                    const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
                    self.state
                        .local_stats
                        .add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
                    self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await?;
                    return self.framed.get_mut().flush().await;
                }
                ProtocolVersion::Resp3 => {
                    let frame = response.to_resp3_frame();
                    self.resp3_buf.clear();
                    redis_protocol::resp3::encode::complete::extend_encode(
                        &mut self.resp3_buf,
                        &frame,
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                    self.state
                        .local_stats
                        .add_bytes_sent(self.resp3_buf.len() as u64);
                    self.framed.get_mut().write_all(&self.resp3_buf).await?;
                    return self.framed.get_mut().flush().await;
                }
            }
        }

        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Use RESP2 encoding via the Framed codec
                let frame = response.to_resp2_frame();
                // Estimate frame size for stats tracking
                let frame_size = estimate_resp2_frame_size(&frame);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed.send(frame).await.map_err(std::io::Error::other)
            }
            ProtocolVersion::Resp3 => {
                // Manually encode RESP3 and write to socket
                let frame = response.to_resp3_frame();
                self.resp3_buf.clear();
                redis_protocol::resp3::encode::complete::extend_encode(&mut self.resp3_buf, &frame)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                // Track actual encoded size
                self.state
                    .local_stats
                    .add_bytes_sent(self.resp3_buf.len() as u64);
                self.framed.get_mut().write_all(&self.resp3_buf).await?;
                self.framed.get_mut().flush().await
            }
        }
    }

    /// Buffer a response without flushing (for write coalescing).
    async fn feed_response(&mut self, response: Response) -> std::io::Result<()> {
        let wire_response = response.into_wire().expect(
            "Internal action reached feed_response - should be intercepted by route_and_execute_with_transaction"
        );
        self.feed_wire_response(wire_response).await
    }

    /// Buffer a wire response without flushing.
    async fn feed_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            match self.state.protocol_version {
                ProtocolVersion::Resp2 => {
                    const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
                    self.state
                        .local_stats
                        .add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
                    return self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await;
                }
                ProtocolVersion::Resp3 => {
                    let frame = response.to_resp3_frame();
                    self.resp3_buf.clear();
                    redis_protocol::resp3::encode::complete::extend_encode(
                        &mut self.resp3_buf,
                        &frame,
                    )
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                    self.state
                        .local_stats
                        .add_bytes_sent(self.resp3_buf.len() as u64);
                    self.framed.get_mut().write_all(&self.resp3_buf).await?;
                    self.resp3_buf.clear();
                    return Ok(());
                }
            }
        }

        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                let frame = response.to_resp2_frame();
                let frame_size = estimate_resp2_frame_size(&frame);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed.feed(frame).await.map_err(std::io::Error::other)
            }
            ProtocolVersion::Resp3 => {
                let frame = response.to_resp3_frame();
                // Don't clear resp3_buf here — accumulate across multiple feeds
                redis_protocol::resp3::encode::complete::extend_encode(&mut self.resp3_buf, &frame)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                let encoded_len = self.resp3_buf.len() as u64;
                self.state.local_stats.add_bytes_sent(encoded_len);
                self.framed.get_mut().write_all(&self.resp3_buf).await?;
                self.resp3_buf.clear();
                Ok(())
            }
        }
    }

    /// Flush all buffered responses to the client.
    async fn flush_responses(&mut self) -> std::io::Result<()> {
        // Flush the RESP2 codec buffer and then the underlying stream
        self.framed.flush().await.map_err(std::io::Error::other)?;
        self.framed.get_mut().flush().await
    }

    /// Try to decode the next frame from the codec's internal read buffer
    /// without issuing a read syscall. Returns `None` if no complete frame
    /// is buffered.
    fn try_next_frame(
        &mut self,
    ) -> Option<
        Result<
            redis_protocol::resp2::types::BytesFrame,
            <Resp2 as tokio_util::codec::Decoder>::Error,
        >,
    > {
        use futures::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self.framed).poll_next(&mut cx) {
            Poll::Ready(item) => item,
            Poll::Pending => None,
        }
    }

    /// Process a single command frame: parse, execute, record metrics, and buffer the response.
    ///
    /// Uses `feed_response` instead of `send_response` so the caller can batch
    /// multiple commands before a single `flush_responses()`.
    async fn process_one_command(
        &mut self,
        frame: redis_protocol::resp2::types::BytesFrame,
    ) -> FrameAction {
        // Parse frame into command and wrap in Arc
        let cmd = match ParsedCommand::try_from(frame) {
            Ok(cmd) => Arc::new(cmd),
            Err(e) => {
                let _ = self
                    .feed_response(Response::error(format!("ERR {}", e)))
                    .await;
                return FrameAction::Continue;
            }
        };

        trace!(
            conn_id = self.state.id,
            cmd = %String::from_utf8_lossy(&cmd.name),
            args = cmd.args.len(),
            "Received command"
        );

        // Capture a single timestamp for timing, metrics, and idle tracking
        let now = std::time::Instant::now();

        // Update last command time for idle tracking
        self.client_registry
            .update_last_command_at(self.state.id, now);

        // Track bytes received for this command
        let cmd_bytes = estimate_command_size(&cmd);
        self.state.local_stats.add_bytes_recv(cmd_bytes as u64);

        // Chaos injection: simulate connection reset before processing command.
        #[cfg(feature = "turmoil")]
        if self.chaos_config.should_simulate_connection_reset() {
            trace!(conn_id = self.state.id, "Chaos: simulating connection reset");
            return FrameAction::Break;
        }

        // Handle QUIT specially (also clears transaction state)
        if cmd.name.eq_ignore_ascii_case(b"QUIT") {
            self.state.transaction = TransactionState::default();
            let _ = self.feed_response(Response::ok()).await;
            return FrameAction::Break;
        }

        // Compute the uppercase command name once for the entire pipeline
        let cmd_name = cmd.name_uppercase_string();

        // Fire USDT probe: command-start
        let first_key = cmd
            .args
            .first()
            .map(|k| std::str::from_utf8(k).unwrap_or("<binary>"))
            .unwrap_or("");
        frogdb_core::probes::fire_command_start(&cmd_name, first_key, self.state.id);

        // Broadcast to MONITOR subscribers (skip MONITOR itself)
        if cmd_name != "MONITOR" && self.monitor_broadcaster.has_subscribers() {
            self.monitor_broadcaster
                .send(crate::monitor::MonitorEvent::new(
                    self.state.addr,
                    &cmd_name,
                    &cmd.args,
                ));
        }

        // Start timing for both metrics and slowlog (reuse captured timestamp)
        let timer = frogdb_telemetry::CommandTimer::with_start_time(
            now,
            cmd_name.clone(),
            self.metrics_recorder.clone(),
            self.band_tracker.clone(),
        );

        // Start request span for distributed tracing (if enabled)
        let request_span = self
            .shared_tracer
            .as_ref()
            .map(|t| t.start_request_span(&cmd_name, self.state.id));

        // Route and execute (with transaction and pub/sub handling)
        let responses = if self
            .per_request_spans
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.route_and_execute_with_transaction(&cmd, &cmd_name)
                .instrument(tracing::info_span!("cmd_execute", cmd = %cmd_name))
                .await
        } else {
            self.route_and_execute_with_transaction(&cmd, &cmd_name)
                .await
        };

        // Check for PSYNC_HANDOFF signal
        if let Some(handoff_params) = Self::extract_psync_handoff(&responses) {
            info!(
                conn_id = self.state.id,
                addr = %self.state.addr,
                replication_id = %handoff_params.0,
                offset = handoff_params.1,
                "PSYNC handoff requested - will transfer connection to replication handler"
            );
            self.pending_psync_handoff = Some(handoff_params);
            return FrameAction::Break;
        }

        // Calculate elapsed time in microseconds for slowlog
        let elapsed_us = now.elapsed().as_micros() as u64;

        // Check for errors in responses (reused by probe + metrics)
        let has_error = responses.iter().any(|r| matches!(r, Response::Error(_)));

        // Fire USDT probe: command-done
        frogdb_core::probes::fire_command_done(
            &cmd_name,
            elapsed_us,
            if has_error { "error" } else { "ok" },
        );

        // Record per-client command statistics
        self.state.local_stats.record_command(&cmd_name, elapsed_us);

        // Record metrics
        if has_error {
            timer.finish_with_error("command_error");
            if let Some(ref span) = request_span {
                span.set_error("command_error");
            }
        } else {
            timer.finish();
            if let Some(ref span) = request_span {
                span.set_ok();
            }
        }

        // End the request span
        if let Some(span) = request_span {
            span.end();
        }

        // Record causal profiling throughput progress point
        #[cfg(feature = "causal-profile")]
        tokio_coz::progress!("commands_processed");

        // Log to slowlog if threshold exceeded and command not exempt
        self.maybe_log_slow_query(&cmd, elapsed_us).await;

        // Periodically sync local stats to the registry
        self.maybe_sync_stats();

        // Buffer response(s) based on reply mode
        match self.state.reply_mode {
            ReplyMode::On => {
                if self.state.skip_next_reply {
                    self.state.skip_next_reply = false;
                    return FrameAction::SkipResponse;
                }
                // Feed responses into the write buffer without flushing
                for response in responses {
                    if self.feed_response(response).await.is_err() {
                        return FrameAction::Break;
                    }
                }
            }
            ReplyMode::Off => {
                return FrameAction::SkipResponse;
            }
        }

        FrameAction::Continue
    }

    /// Ensure the pub/sub channel is initialized, returning a clone of the sender.
    /// Called lazily on the first pub/sub command to avoid allocating channels
    /// for the ~99% of connections that never use pub/sub.
    fn ensure_pubsub_channel(&mut self) -> PubSubSender {
        if let Some(ref tx) = self.pubsub_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.pubsub_tx = Some(tx.clone());
        self.pubsub_rx = Some(rx);
        tx
    }

    /// Convert an invalidation message to a RESP3 Push response.
    fn invalidation_to_response(msg: &InvalidationMessage) -> Response {
        match msg {
            InvalidationMessage::Keys(keys) => Response::Push(vec![
                Response::bulk(bytes::Bytes::from_static(b"invalidate")),
                Response::Array(keys.iter().map(|k| Response::bulk(k.clone())).collect()),
            ]),
            InvalidationMessage::FlushAll => Response::Push(vec![
                Response::bulk(bytes::Bytes::from_static(b"invalidate")),
                Response::Null,
            ]),
        }
    }

    /// Ensure the invalidation channel is initialized, returning a clone of the sender.
    /// Called lazily on CLIENT TRACKING ON.
    pub(crate) fn ensure_invalidation_channel(&mut self) -> InvalidationSender {
        if let Some(ref tx) = self.invalidation_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.invalidation_tx = Some(tx.clone());
        self.invalidation_rx = Some(rx);
        tx
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
                Some(pubsub_msg) = async {
                    match self.pubsub_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Buffer the first pub/sub message
                    let response = pubsub_msg.to_response_with_protocol(self.state.protocol_version);
                    if self.feed_response(response).await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to send pub/sub message");
                        break;
                    }
                    // Drain additional pub/sub messages from the channel
                    if let Some(ref mut rx) = self.pubsub_rx {
                        let mut extra = Vec::new();
                        while let Ok(msg) = rx.try_recv() {
                            extra.push(msg);
                        }
                        for msg in extra {
                            let response = msg.to_response_with_protocol(self.state.protocol_version);
                            if self.feed_response(response).await.is_err() {
                                break;
                            }
                        }
                    }
                    // Single flush for all pub/sub messages
                    if self.flush_responses().await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to flush pub/sub responses");
                        break;
                    }
                }

                // Handle invalidation messages (CLIENT TRACKING)
                Some(inv_msg) = async {
                    match self.invalidation_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let response = Self::invalidation_to_response(&inv_msg);
                    if self.feed_response(response).await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to send invalidation");
                        break;
                    }
                    // Drain additional invalidation messages (collect first to release borrow)
                    if let Some(ref mut rx) = self.invalidation_rx {
                        let mut extra = Vec::new();
                        while let Ok(msg) = rx.try_recv() {
                            extra.push(msg);
                        }
                        for msg in extra {
                            let response = Self::invalidation_to_response(&msg);
                            if self.feed_response(response).await.is_err() {
                                break;
                            }
                        }
                    }
                    if self.flush_responses().await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to flush invalidation responses");
                        break;
                    }
                }

                // Handle MONITOR events
                result = async {
                    match self.monitor_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match result {
                        Ok(event) => {
                            // Collect first event + drain buffered events
                            let mut events = vec![event];
                            if let Some(ref mut rx) = self.monitor_rx {
                                while let Ok(event) = rx.try_recv() {
                                    events.push(event);
                                }
                            }
                            // Feed all events
                            let mut write_err = false;
                            for event in &events {
                                let formatted = crate::monitor::MonitorBroadcaster::format_event(event);
                                if self.feed_response(Response::Simple(bytes::Bytes::from(formatted))).await.is_err() {
                                    write_err = true;
                                    break;
                                }
                            }
                            if write_err {
                                break;
                            }
                            if self.flush_responses().await.is_err() {
                                debug!(conn_id = self.state.id, "Failed to flush MONITOR responses");
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            debug!(conn_id = self.state.id, skipped = n, "MONITOR subscriber lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            self.monitor_rx = None;
                        }
                    }
                }

                // Handle client commands
                frame_result = async {
                    if self.per_request_spans.load(std::sync::atomic::Ordering::Relaxed) {
                        self.framed.next().instrument(tracing::info_span!("cmd_read")).await
                    } else {
                        self.framed.next().await
                    }
                } => {
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

                    // Process first command (buffers response, no flush yet)
                    let mut should_break = false;
                    match self.process_one_command(frame).await {
                        FrameAction::Break => should_break = true,
                        FrameAction::Continue | FrameAction::SkipResponse => {}
                    }

                    // Drain loop: process all complete frames already in the read buffer
                    if !should_break {
                        while let Some(frame_result) = self.try_next_frame() {
                            let frame = match frame_result {
                                Ok(frame) => frame,
                                Err(e) => {
                                    debug!(conn_id = self.state.id, error = %e, "Frame error in drain");
                                    let _ = self.feed_response(
                                        Response::error(format!("ERR {}", e))
                                    ).await;
                                    continue;
                                }
                            };
                            match self.process_one_command(frame).await {
                                FrameAction::Break => {
                                    should_break = true;
                                    break;
                                }
                                FrameAction::Continue | FrameAction::SkipResponse => {}
                            }
                        }
                    }

                    // Single flush for all buffered responses
                    if self.flush_responses().await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to flush responses");
                        break;
                    }

                    if should_break {
                        break;
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
            if let Some(handler) = &self.primary_replication_handler {
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
                    if let Err(e) = handler
                        .handle_psync(stream, self.state.addr, &replication_id, offset)
                        .await
                    {
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
                    let _ = (handler, stream);
                }
            } else {
                warn!(
                    conn_id = self.state.id,
                    "PSYNC handoff requested but no primary replication handler available"
                );
            }

            // Don't run normal cleanup - replication handler has the connection
            debug!(
                conn_id = self.state.id,
                "Connection handler finished (PSYNC handoff)"
            );
            return Ok(());
        }

        // Cleanup: notify all shards that this connection is closed
        self.notify_connection_closed().await;

        debug!(conn_id = self.state.id, "Connection handler finished");
        Ok(())
    }

    /// Notify all shards that this connection is closed.
    async fn notify_connection_closed(&mut self) {
        // Drop MONITOR subscription (auto-decrements broadcast receiver count)
        self.monitor_rx = None;

        // Final stats sync before closing
        self.sync_stats_to_registry();

        // Notify shards if we had subscriptions or tracking enabled
        if self.state.pubsub.in_pubsub_mode() || self.state.tracking.enabled {
            for sender in self.shard_senders.iter() {
                let _ = sender
                    .send(ShardMessage::ConnectionClosed {
                        conn_id: self.state.id,
                    })
                    .await;
            }
        }

        // Unregister any blocking waits
        if let Some(ref blocked) = self.state.blocked
            && let Some(sender) = self.shard_senders.get(blocked.shard_id)
        {
            let _ = sender
                .send(ShardMessage::UnregisterWait {
                    conn_id: self.state.id,
                })
                .await;
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

        if let Response::Array(items) = &responses[0]
            && items.len() >= 3
        {
            // Check for PSYNC_HANDOFF marker
            if let Response::Simple(marker) = &items[0]
                && marker.as_ref() == b"PSYNC_HANDOFF"
            {
                // Extract replication_id
                let replication_id = match &items[1] {
                    Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => return None,
                };

                // Extract offset
                let offset = match &items[2] {
                    Response::Bulk(Some(b)) => String::from_utf8_lossy(b).parse::<i64>().ok()?,
                    _ => return None,
                };

                return Some((replication_id, offset));
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

    /// Wait if the server is paused (CLIENT PAUSE).
    /// This queues commands (not drops them) by blocking until pause ends.
    ///
    /// Called from `route_and_execute_with_transaction` after transaction-control
    /// dispatch and transaction queuing, so it only blocks commands outside MULTI.
    pub(crate) async fn wait_if_paused(&self, cmd_name: &str) {
        // Get command flags to determine if this is a write/script command
        let flags = self
            .registry
            .get(cmd_name)
            .map(|h| h.flags())
            .unwrap_or(CommandFlags::empty());

        let is_write_command = flags.contains(CommandFlags::WRITE);
        let is_script_command = flags.contains(CommandFlags::SCRIPT);
        let is_readonly_script = is_script_command && flags.contains(CommandFlags::READONLY);

        // Certain commands are always exempt from pause
        let is_exempt = matches!(
            cmd_name,
            "CLIENT" | "PING" | "QUIT" | "RESET" | "INFO" | "CONFIG" | "DEBUG" | "SLOWLOG"
        );

        if is_exempt {
            return;
        }

        // For PAUSE WRITE: block writes, scripts (conservatively), and special
        // commands that replicate or have write side-effects. Read-only script
        // variants (EVAL_RO, EVALSHA_RO, FCALL_RO) are exempt.
        let is_write_for_pause = is_write_command
            || (is_script_command && !is_readonly_script)
            || matches!(cmd_name, "PFCOUNT" | "PUBLISH" | "SPUBLISH");

        // Check pause state and wait if necessary
        loop {
            match self.client_registry.check_pause() {
                Some(PauseMode::All) => {
                    // All commands are paused
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Some(PauseMode::Write) if is_write_for_pause => {
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
