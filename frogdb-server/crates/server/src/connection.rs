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
pub(crate) mod codec;
pub mod deps;
pub(crate) mod dispatch;
mod frame_io;
pub(crate) mod guards;
pub mod handlers;
mod lifecycle;
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
use codec::FrogDbResp2;
use frogdb_core::{
    AclManager, ClientHandle, ClientRegistry, ClusterNetworkFactory, ClusterRaft, ClusterState,
    CommandRegistry, InvalidationMessage, InvalidationSender, MetricsRecorder, PubSubMessage,
    PubSubSender, ReplicationTrackerImpl, ShardMessage, ShardSender, SharedFunctionRegistry,
    persistence::SnapshotCoordinator,
};
use frogdb_protocol::{ParsedCommand, Response};
use frogdb_telemetry::SharedTracer;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug, info, trace, warn};

#[cfg(feature = "turmoil")]
use crate::config::ChaosConfigExt;
use crate::config::TracingConfig;
use crate::net::ConnectionStream;
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
    // -- Connection I/O --
    /// Framed socket with RESP2 codec.
    framed: Framed<ConnectionStream, FrogDbResp2>,

    /// Connection state.
    state: ConnectionState,

    // -- Identity --
    /// Assigned shard ID.
    shard_id: usize,

    /// Total number of shards.
    num_shards: usize,

    // -- Dependency groups --
    /// Core dependencies (registry, shard senders, metrics, ACL).
    core: CoreDeps,

    /// Admin dependencies (client registry, config manager, snapshots, functions, cursors).
    admin: AdminDeps,

    /// Cluster dependencies (cluster state, node ID, raft, network, replication).
    cluster: ClusterDeps,

    /// Observability dependencies (tracer, tracing config, band tracker, monitor).
    observability: ObservabilityDeps,

    // -- Connection-local state --
    /// Client handle (auto-unregisters on drop).
    client_handle: ClientHandle,

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

    /// Whether the next command should suppress touch() (CLIENT NO-TOUCH mode).
    pending_no_touch: bool,

    /// Whether this is an admin connection (from admin port).
    is_admin: bool,

    /// Whether admin port separation is enabled.
    admin_enabled: bool,

    /// Whether unsafe DEBUG subcommands (DEBUG SLEEP) are enabled.
    /// Default false in production; test harness sets to true.
    enable_debug_command: bool,

    /// Hot shard detection configuration.
    _hotshards_config: frogdb_debug::HotShardConfig,

    /// Memory diagnostics configuration.
    memory_diag_config: frogdb_debug::MemoryDiagConfig,

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

    /// MONITOR subscription receiver (set when MONITOR command is executed).
    monitor_rx: Option<tokio::sync::broadcast::Receiver<Arc<crate::monitor::MonitorEvent>>>,

    /// REDIRECT forwarding task handle (aborted on TRACKING OFF or disconnect).
    redirect_task: Option<tokio::task::JoinHandle<()>>,

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
        socket: ConnectionStream,
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
        let framed = Framed::new(socket, FrogDbResp2::default());
        // Dynamic auth check: also require auth if the default user is disabled
        let requires_auth = core.acl_manager.requires_auth()
            || core
                .acl_manager
                .get_user("default")
                .is_some_and(|u| !u.enabled);
        let state = ConnectionState::new(conn_id, addr, requires_auth);

        debug!(conn_id = conn_id, addr = %addr, "Connection established");

        Self {
            framed,
            state,
            shard_id,
            num_shards: config.num_shards,
            core,
            admin,
            cluster,
            observability,
            client_handle,
            allow_cross_slot: config.allow_cross_slot,
            scatter_gather_timeout: config.scatter_gather_timeout,
            pubsub_tx: None,
            pubsub_rx: None,
            invalidation_tx: None,
            invalidation_rx: None,
            pending_track_reads: false,
            pending_no_touch: false,
            is_admin: config.is_admin,
            admin_enabled: config.admin_enabled,
            enable_debug_command: config.enable_debug_command,
            _hotshards_config: config.hotshards_config,
            memory_diag_config: config.memory_diag_config,
            pending_psync_handoff: None,
            resp3_buf: BytesMut::with_capacity(4096),
            per_request_spans: config.per_request_spans,
            is_replica: config.is_replica,
            #[cfg(feature = "turmoil")]
            chaos_config: config.chaos_config.clone(),
            monitor_rx: None,
            redirect_task: None,
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
        socket: ConnectionStream,
        addr: SocketAddr,
        conn_id: u64,
        shard_id: usize,
        num_shards: usize,
        registry: Arc<CommandRegistry>,
        client_registry: Arc<ClientRegistry>,
        config_manager: Arc<ConfigManager>,
        client_handle: ClientHandle,
        shard_senders: Arc<Vec<ShardSender>>,
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
        raft: Option<Arc<ClusterRaft>>,
        network_factory: Option<Arc<ClusterNetworkFactory>>,
        primary_replication_handler: Option<Arc<PrimaryReplicationHandler>>,
    ) -> Self {
        // Convert to grouped dependencies and delegate
        let core = CoreDeps {
            registry,
            shard_senders,
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
            enable_debug_command: false,
            #[cfg(feature = "turmoil")]
            chaos_config: Arc::new(crate::config::ChaosConfig::default()),
        };
        let observability = ObservabilityDeps {
            metrics_recorder,
            shared_tracer,
            tracing_config,
            monitor_broadcaster: Arc::new(crate::monitor::MonitorBroadcaster::new(4096)),
            latency_histograms: Arc::new(frogdb_core::CommandLatencyHistograms::new(true)),
            hotkey_session: frogdb_core::new_shared_hotkey_session(),
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
        self.admin
            .client_registry
            .update_last_command_at(self.state.id, now);

        // Track the currently executing command for CLIENT LIST/INFO
        {
            let cmd_name = String::from_utf8_lossy(&cmd.name).to_lowercase();
            let cmd_str = if !cmd.args.is_empty() {
                // For commands with subcommands, format as "cmd|sub"
                let sub = String::from_utf8_lossy(&cmd.args[0]).to_lowercase();
                match cmd_name.as_str() {
                    "client" | "config" | "command" | "object" | "debug" | "hotkeys" | "memory"
                    | "cluster" | "acl" | "xinfo" | "xgroup" | "script" | "function"
                    | "slowlog" | "latency" | "module" | "pfdebug" | "srandmember" => {
                        format!("{cmd_name}|{sub}")
                    }
                    _ => cmd_name.clone(),
                }
            } else {
                cmd_name.clone()
            };
            self.admin
                .client_registry
                .update_current_cmd(self.state.id, Some(cmd_str));
        }

        // Track bytes received for this command
        let cmd_bytes = estimate_command_size(&cmd);
        self.state.local_stats.add_bytes_recv(cmd_bytes as u64);

        // Chaos injection: simulate connection reset before processing command.
        #[cfg(feature = "turmoil")]
        if self.chaos_config.should_simulate_connection_reset() {
            trace!(
                conn_id = self.state.id,
                "Chaos: simulating connection reset"
            );
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

        // Rate limit check (after QUIT handled above, before dispatch)
        if let Some(err_resp) = self.check_rate_limit(&cmd_name, cmd_bytes as u64) {
            // Record as rejected (pre-execution)
            if let Response::Error(ref bytes) = err_resp {
                let prefix = frogdb_core::extract_error_prefix(bytes);
                self.admin
                    .client_registry
                    .error_stats
                    .record_rejected(prefix);
                self.admin
                    .client_registry
                    .record_command_rejected(&cmd_name);
            }
            let _ = self.feed_response(err_resp).await;
            return FrameAction::Continue;
        }

        // Fire USDT probe: command-start
        let first_key = cmd
            .args
            .first()
            .map(|k| std::str::from_utf8(k).unwrap_or("<binary>"))
            .unwrap_or("");
        frogdb_core::probes::fire_command_start(&cmd_name, first_key, self.state.id);

        // Broadcast to MONITOR subscribers (skip MONITOR itself)
        if cmd_name != "MONITOR" && self.observability.monitor_broadcaster.has_subscribers() {
            self.observability
                .monitor_broadcaster
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
            self.observability.metrics_recorder.clone(),
        );

        // Start request span for distributed tracing (if enabled)
        let request_span = self
            .observability
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

        // Record into server-wide latency histograms (for INFO latencystats)
        self.observability
            .latency_histograms
            .record(&cmd_name, elapsed_us);

        // Blocking commands should immediately surface in INFO commandstats
        // without waiting for the periodic sync threshold (every 100 commands
        // or 1000 ms). Force-sync here so a follow-up INFO sees `calls=1`.
        if is_blocking_command_name(&cmd_name) {
            self.sync_stats_to_registry();
        }

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

        // Record hotkey accesses if a session is active
        self.maybe_record_hotkeys(&cmd, &cmd_name, elapsed_us, cmd_bytes);

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
            if let Some(handler) = &self.cluster.primary_replication_handler {
                // Extract the ConnectionStream from the Framed codec, then get the raw TcpStream.
                let connection_stream = self.framed.into_inner();

                #[cfg(not(feature = "turmoil"))]
                {
                    // Pass the stream as a boxed trait object, preserving TLS if active.
                    let boxed_stream = connection_stream.into_boxed();
                    if let Err(e) = handler
                        .handle_psync(boxed_stream, self.state.addr, &replication_id, offset)
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
                    warn!(
                        conn_id = self.state.id,
                        "PSYNC handoff not supported in turmoil simulation mode"
                    );
                    let _ = (handler, connection_stream);
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
}

/// Returns true if the command name (any case) is a data-blocking command.
///
/// Used to force-sync per-client stats to the registry immediately after the
/// command completes, so `INFO commandstats` reflects the call without
/// waiting for the periodic sync threshold. WAIT is excluded because it's a
/// replication control command and is not exercised by the commandstats
/// regression tests.
fn is_blocking_command_name(name: &str) -> bool {
    matches!(
        name.to_ascii_uppercase().as_str(),
        "BLPOP"
            | "BRPOP"
            | "BLMPOP"
            | "BLMOVE"
            | "BRPOPLPUSH"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "BZMPOP"
            | "XREAD"
            | "XREADGROUP"
    )
}
