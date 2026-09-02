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
pub(crate) mod acl_conn_command;
pub(crate) mod auth_conn_command;
mod blocking;
mod builder;
pub(crate) mod client_conn_command;
pub(crate) mod cluster;
pub(crate) mod codec;
pub(crate) mod conn_command;
pub(crate) mod connection_state_conn_command;
pub(crate) mod debug_conn_command;
mod debug_handler;
pub mod deps;
pub(crate) mod dispatch;
mod frame_io;
pub(crate) mod guards;
pub(crate) mod hotkeys;
pub(crate) mod hotshards_conn_command;
pub(crate) mod info_conn_command;
mod info_handler;
mod lifecycle;
pub(crate) mod monitor_conn_command;
pub(crate) mod observability_conn_command;
pub mod output_buffer;
pub(crate) mod pause_gate;
pub(crate) mod permission_guard;
pub(crate) mod persistence_conn_command;
pub(crate) mod persistence_handler;
pub(crate) mod pubsub_conn_command;
pub(crate) mod routing;
pub(crate) mod scatter;
pub(crate) mod scripting;
pub(crate) mod scripting_conn_command;
pub(crate) mod search;
mod slowlog;
pub mod state;
pub(crate) mod status_handler;
pub(crate) mod timeseries_scatter;
pub(crate) mod transaction;
pub(crate) mod transaction_conn_command;
pub(crate) mod util;

// Re-export dependency groups
pub use deps::{
    AdminDeps, ClusterDeps, ConnectionConfig, ConnectionDeps, CoreDeps, ObservabilityDeps,
};

// Re-export state types
pub use state::{
    AuthState, BlockedState, ConnectionState, LocalClientStats, PubSubState, ReplyDisposition,
    ReplyMode, STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS, TrackingEnableError,
    TrackingEnableRequest, TrackingMode, TrackingState, TransactionState, TransactionTarget,
};

// Re-export builder
pub use builder::{ConnectionHandlerBuilder, connection_builder, standalone_config};

use frogdb_core::clock;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::BytesMut;
use codec::FrogDbResp2;
use frogdb_core::{ClientHandle, PubSubMsg, PubSubReceiver, PubSubSender};
use frogdb_protocol::{ParsedCommand, Response, WireResponse};
use frogdb_replication::ReplicaAnnouncement;
use futures::StreamExt;
use lifecycle::TrackingIo;
use redis_protocol::error::RedisProtocolError;
use redis_protocol::resp2::types::BytesFrame;
use tokio_util::codec::Framed;
use tracing::{Instrument, debug, info, trace, warn};

use crate::commands::replication::PsyncHandoff;
#[cfg(feature = "turmoil")]
use crate::config::ChaosConfigExt;
use crate::connection::dispatch::Dispatched;
use crate::net::ConnectionStream;
// Re-export next_txid for the command-execution submodules
pub use crate::server::next_txid;

// Re-export utility functions used by connection submodules and internally
pub(crate) use util::{estimate_command_size, estimate_resp2_frame_size};

/// How often an otherwise-idle connection wakes to judge its output buffer and
/// return buffers it no longer needs.
///
/// One second, matching Redis's `serverCron` cadence for the same job and the
/// granularity of `client-output-buffer-limit`'s `soft-seconds` — a finer tick
/// would buy no accuracy the configuration can express, and every open
/// connection pays for this timer.
const IDLE_TICK: Duration = Duration::from_secs(1);

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
    pubsub_rx: Option<PubSubReceiver>,

    /// Client-tracking IO plumbing (invalidation channel + REDIRECT forwarding
    /// task). Grouped so the CLIENT executor can borrow it mutably as a
    /// [`frogdb_core::ClientTrackingProvider`] disjointly from `self.state`.
    tracking_io: TrackingIo,

    /// Whether the next command's reads should be tracked (computed before dispatch).
    pending_track_reads: bool,

    /// The slot-ownership generation this command's slot validation was taken
    /// against, when this node owns the slot
    /// ([`SlotFence`](crate::slot_migration::SlotFence)).
    ///
    /// Stamped by `ClusterSlotValidation` (and by the EXEC-time batch
    /// validator), consumed once by the dispatch driver after the command has
    /// run and before its reply reaches the client. Lives on the handler rather
    /// than being threaded through every executor because the commands that
    /// need it terminate at three different dispatch stages — `Execute`
    /// (shard commands), `ConnectionCommand` (scripts), and
    /// `TransactionControl` (`EXEC`) — and all three funnel through the one
    /// driver loop.
    pending_slot_fence: Option<crate::slot_migration::SlotFence>,

    /// Whether the next command should suppress touch() (CLIENT NO-TOUCH mode).
    pending_no_touch: bool,

    /// Whether this is an admin connection (from admin port).
    is_admin: bool,

    /// Whether admin port separation is enabled.
    admin_enabled: bool,

    /// Whether unsafe DEBUG subcommands (DEBUG SLEEP) are enabled.
    /// Default false in production; test harness sets to true.
    enable_debug_command: bool,

    /// Memory diagnostics provider (MEMORY DOCTOR), wrapping the configured
    /// [`frogdb_debug::MemoryDiagConfig`] behind the core `MemoryDiagProvider`
    /// seam so it can be exposed through [`ConnCtx::memory_diag`].
    memory_diag: crate::connection::observability_conn_command::MemoryDiag,

    /// Pending PSYNC connection takeover. Set when `ReplicationHandshake` yields a
    /// typed [`Dispatched::Handoff`], carried out after the run loop (so any
    /// buffered pipelined replies flush over the wire before the socket is
    /// handed to the `PrimaryReplicationHandler`).
    pending_psync_handoff: Option<PsyncHandoff>,

    /// What this connection has announced about itself over `REPLCONF`, if it
    /// is a replica mid-handshake.
    ///
    /// Accumulated by `DispatchStage::ReplicationHandshake` and handed to the
    /// primary at the `PSYNC` takeover below, because `PSYNC` is what creates
    /// the replica's session — there is no session to write it into when the
    /// `REPLCONF` arrives. A connection that never sends `REPLCONF` keeps the
    /// default, which is what a `PSYNC` from a bare client would register.
    replica_announcement: ReplicaAnnouncement,

    /// Reusable buffer for RESP3 encoding to avoid per-response allocation.
    ///
    /// Leased from this core's pool ([`frogdb_net::buffers`]) and re-leased
    /// small when the connection goes idle, so a client that once received a
    /// megabyte reply does not hold a megabyte for the rest of its session.
    resp3_buf: BytesMut,

    /// Buffered-output accounting and `client-output-buffer-limit` enforcement.
    /// See [`crate::connection::output_buffer`] — every buffered out-byte on
    /// this connection is charged here, and this is the only thing that decides
    /// a connection is too far behind to keep.
    output_buffer: output_buffer::OutputBufferAccount,

    /// Whether per-request tracing spans are enabled (shared AtomicBool).
    per_request_spans: Arc<std::sync::atomic::AtomicBool>,

    /// Whether this server is a replica (rejects write commands from clients).
    /// Shared across all connections so REPLICAOF NO ONE takes effect immediately.
    is_replica: Arc<std::sync::atomic::AtomicBool>,

    /// MONITOR subscription receiver (set when MONITOR command is executed).
    monitor_rx: Option<tokio::sync::broadcast::Receiver<Arc<crate::monitor::MonitorEvent>>>,

    /// Frames that arrived while a blocking wait was parked.
    ///
    /// A parked wait owns the socket (`specs/blocking.md`, "Parked-wait
    /// supervision"): it must keep reading to notice the peer leaving, but the
    /// frames it reads must *not* run ahead of the blocking command they were
    /// pipelined behind. They are buffered here and drained by
    /// [`Self::try_next_frame`] ahead of the socket, preserving arrival order.
    /// Bounded by [`MAX_PARKED_PIPELINE_FRAMES`]; at the cap the watch stops
    /// reading and TCP backpressure takes over.
    parked_frames: VecDeque<Result<BytesFrame, RedisProtocolError>>,

    /// Set when a parked blocking wait ended because the connection itself
    /// ended (peer EOF or `CLIENT KILL`). Consumed by `process_one_command`,
    /// which suppresses the reply and terminates the run loop, leaving the
    /// blocked-flag set so `notify_connection_closed` unregisters the waiter
    /// (`specs/blocking.md` TR-BLOCKING-013, TR-BLOCKING-021).
    parked_wait_exit: Option<blocking::coordinator::ParkedExit>,

    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    chaos_config: Arc<crate::config::ChaosConfig>,
}

/// How many frames a parked wait will buffer before it stops reading the socket.
///
/// Bounded because the frames are held in memory by a connection that is,
/// by definition, not making progress. Deep enough that ordinary pipelining
/// behind a blocking command still works; shallow enough that a client cannot
/// use a parked wait as an unbounded server-side buffer.
pub(crate) const MAX_PARKED_PIPELINE_FRAMES: usize = 64;

/// Result of processing a single command frame.
enum FrameAction {
    /// Command processed normally, keep going.
    Continue,
    /// Connection should close (QUIT, disconnect).
    Break,
    /// Response was skipped (ReplyMode::Off or skip_next_reply).
    SkipResponse,
    /// PSYNC requested a raw-socket takeover. Carries the typed handoff out of
    /// `process_one_command`; the run loop stashes it and breaks *after* the
    /// shared flush so buffered pipelined replies still reach the wire.
    Handoff(PsyncHandoff),
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
            tracking_io: TrackingIo::default(),
            pending_track_reads: false,
            pending_slot_fence: None,
            pending_no_touch: false,
            is_admin: config.is_admin,
            admin_enabled: config.admin_enabled,
            enable_debug_command: config.enable_debug_command,
            memory_diag: crate::connection::observability_conn_command::MemoryDiag(
                config.memory_diag_config,
            ),
            pending_psync_handoff: None,
            parked_frames: VecDeque::new(),
            parked_wait_exit: None,
            replica_announcement: ReplicaAnnouncement::default(),
            resp3_buf: frogdb_net::buffers::lease(frogdb_net::buffers::MIN_CLASS_BYTES)
                .into_inner(),
            output_buffer: output_buffer::OutputBufferAccount::new(
                // Every connection starts normal; SUBSCRIBE and PSYNC move it,
                // and `account_buffered_output` re-derives the class on every
                // write so a class change cannot be missed.
                output_buffer::OutputBufferClass::Normal,
                config.output_buffer_limits,
                &frogdb_memory::network_output::current(),
            ),
            per_request_spans: config.per_request_spans,
            is_replica: config.is_replica,
            #[cfg(feature = "turmoil")]
            chaos_config: config.chaos_config.clone(),
            monitor_rx: None,
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
                    .feed_response(WireResponse::error(format!("ERR {}", e)))
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
        let now = clock::now();

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
            self.state.clear_transaction();
            let _ = self.feed_response(WireResponse::ok()).await;
            return FrameAction::Break;
        }

        // Compute the uppercase command name once for the entire pipeline
        let cmd_name = cmd.name_uppercase_string();

        // Rate limit check (after QUIT handled above, before dispatch)
        if let Some(err_resp) = self.check_rate_limit(&cmd_name, cmd_bytes as u64) {
            // Rate limiting refuses the command outright, so it is recorded as
            // a rejection — through the same accounting seam the pre-dispatch
            // gauntlet uses, so the cmdstat rules (registered names only) hold
            // here too.
            self.record_error_response(&err_resp, true, &cmd_name);
            let _ = self.feed_response(Self::narrow_to_wire(err_resp)).await;
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
        let dispatched = if self
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

        // A PSYNC takeover is a typed control outcome, not a data reply: carry it
        // out of this frame so the run loop can flush any buffered pipelined
        // replies before handing the raw socket to the replication handler.
        let responses = match dispatched {
            Dispatched::Responses(responses) => responses,
            Dispatched::Handoff(handoff) => {
                info!(
                    conn_id = self.state.id,
                    addr = %self.state.addr,
                    replication_id = %handoff.replication_id,
                    offset = handoff.offset,
                    "PSYNC handoff requested - will transfer connection to replication handler"
                );
                return FrameAction::Handoff(handoff);
            }
        };

        // A blocking wait that ended because the *connection* ended (peer EOF or
        // CLIENT KILL) has no reader for its reply. Break out before the reply
        // is buffered, leaving the blocked-flag set so `notify_connection_closed`
        // unregisters the waiter (`specs/blocking.md` TR-BLOCKING-013,
        // TR-BLOCKING-021). Buffered pipelined replies from earlier commands
        // still reach the wire: the run loop flushes before it breaks.
        if let Some(exit) = self.parked_wait_exit.take() {
            debug!(
                conn_id = self.state.id,
                addr = %self.state.addr,
                reason = exit.reason(),
                "terminating connection that ended while parked in a blocking command"
            );
            return FrameAction::Break;
        }

        // Calculate elapsed time in microseconds for slowlog
        let elapsed_us = clock::elapsed(now).as_micros() as u64;

        // Check for errors in responses (reused by probe + metrics)
        let has_error = responses.iter().any(|r| matches!(r, Response::Error(_)));

        // Fire USDT probe: command-done
        frogdb_core::probes::fire_command_done(
            &cmd_name,
            elapsed_us,
            if has_error { "error" } else { "ok" },
        );

        // Record per-client command statistics. Only *registered* command names
        // may key a per-command map (`cmdstat_<name>`, the latency histograms):
        // an unrecognized name is raw client input, and keying stats by it is an
        // unbounded-cardinality growth vector. The connection's own totals still
        // count the round trip.
        let known_command = self.records_command_stats(&cmd_name);
        self.state
            .local_stats
            .record_command(known_command.then_some(cmd_name.as_str()), elapsed_us);

        // Record into server-wide latency histograms (for INFO latencystats)
        if known_command {
            self.observability
                .latency_histograms
                .record(&cmd_name, elapsed_us);
        }

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

        // Buffer response(s) based on the connection's reply disposition.
        match self.state.consume_reply_disposition() {
            ReplyDisposition::Send => {
                // Feed responses into the write buffer without flushing.
                // Internal actions were already resolved by the dispatch layer;
                // narrow_to_wire collapses each to its wire form for the encoder.
                for response in responses {
                    if self
                        .feed_response(Self::narrow_to_wire(response))
                        .await
                        .is_err()
                    {
                        return FrameAction::Break;
                    }
                }
            }
            ReplyDisposition::Suppress => {
                return FrameAction::SkipResponse;
            }
        }

        FrameAction::Continue
    }

    /// Which `client-output-buffer-limit` class this connection is in *now*.
    ///
    /// Derived rather than stored: a connection changes class mid-session
    /// (`SUBSCRIBE` makes it a subscriber, `REPLCONF listening-port` marks a
    /// replica link), and a stored copy is a copy someone forgets to update at
    /// one of those transitions. `account_buffered_output` re-derives it on
    /// every write, so the class a connection is judged against is always the
    /// class it is actually in.
    fn output_class(&self) -> output_buffer::OutputBufferClass {
        if self.replica_announcement.listening_port != 0 {
            output_buffer::OutputBufferClass::Replica
        } else if self.pubsub_rx.is_some() {
            output_buffer::OutputBufferClass::PubSub
        } else {
            output_buffer::OutputBufferClass::Normal
        }
    }

    /// Log and record the teardown of a subscriber whose pub/sub delivery queue
    /// exceeded the hard limit. The caller `break`s the delivery loop, dropping
    /// the socket — matching Redis's `client-output-buffer-limit pubsub`.
    ///
    /// The queue bound is that same pubsub hard limit reached one step earlier —
    /// on messages the subscriber was too slow for the server to even buffer —
    /// so the verdict is logged and counted through the one output-buffer seam,
    /// exactly as a hard-limit kill on any other class. Only the dropped-message
    /// count, which is peculiar to the queue, is recorded separately.
    fn disconnect_overflowed_subscriber(&mut self) {
        let dropped = self.pubsub_rx.as_ref().map(|rx| rx.dropped()).unwrap_or(0);
        debug!(
            conn_id = self.state.id,
            dropped, "pub/sub delivery queue overflowed"
        );
        frogdb_telemetry::definitions::PubsubOutputBufferDisconnects::inc(
            &*self.observability.metrics_recorder,
        );
        // Measure now rather than reading the account: the account was zeroed by
        // the flush that preceded this check, so it would report `buffered=0`
        // for a kill caused by megabytes of undelivered messages.
        let buffered = self.buffered_output_bytes();
        let _ = self.shed_output(output_buffer::ShedReason::HardLimit, buffered);
    }

    /// Run the connection handling loop.
    pub async fn run(mut self) -> Result<()> {
        debug!(conn_id = self.state.id, "Connection handler started");

        // The connection's own housekeeping tick. Everything a connection must
        // do *when nothing is happening to it* hangs off this one timer: judge
        // the output-buffer limits (the soft limit is a clock, not a threshold,
        // so without a tick it can only ever fire on a write), and hand buffers
        // grown by a past burst back to the core's pool. A connection that
        // buffered a megabyte and then went quiet is precisely the case neither
        // the command path nor the write path can reach.
        let mut idle_tick =
            tokio::time::interval_at(tokio::time::Instant::now() + IDLE_TICK, IDLE_TICK);
        // Delay, not Burst: a connection descheduled for a while must not then
        // run a backlog of ticks it gained nothing from.
        idle_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            // `biased;` (determinism audit R7/A52): fixes the poll order so a
            // seeded turmoil run replays identically instead of depending on
            // tokio's random tie-break. Order chosen for production fairness,
            // not just determinism:
            //   1. CLIENT KILL — rare, terminal; must win any tie so a killed
            //      connection can't be perpetually re-armed by traffic on the
            //      other arms (the audit explicitly calls out "CLIENT KILL
            //      beats an already-buffered command").
            //   2. Next command frame — the data plane for *this* connection.
            //      Kept ahead of the three push channels so a subscriber /
            //      tracked / monitored client can still read its own next
            //      command (e.g. UNSUBSCRIBE, RESET) under a sustained push
            //      firehose, rather than being starved by messages meant to
            //      be delivered *to* it.
            //   3-5. Pub/sub, invalidation, MONITOR — each already has its
            //      own bounded-backlog escape hatch (`recv_or_overflow`
            //      disconnects a too-slow subscriber; MONITOR reports
            //      `Lagged`), so a delay here degrades gracefully into that
            //      existing, designed backstop rather than an unbounded
            //      correctness problem. Relative order among these three is
            //      untouched (pub/sub, invalidation, MONITOR) — no
            //      asymmetry between them is called out by the audit.
            //   6. Housekeeping tick — output-buffer judgment and idle buffer
            //      trimming. Last because it serves the server, not the client,
            //      and losing a tick to real traffic costs nothing: the next
            //      one is a second away, and a busy connection is judged on
            //      every write anyway.
            tokio::select! {
                biased;

                // 1. Check for CLIENT KILL
                _ = self.client_handle.killed() => {
                    info!(conn_id = self.state.id, addr = %self.state.addr, "Connection killed");
                    break;
                }

                // 2. Handle client commands
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
                            // Use `details()` (not `Display`) so the wire form is
                            // Redis's `-ERR Protocol error: ...` without the
                            // upstream `Decode Error: ` kind prefix.
                            let _ = self.send_response(WireResponse::error(format!("ERR {}", e.details()))).await;
                            continue;
                        }
                        None => {
                            debug!(
                                conn_id = self.state.id,
                                addr = %self.state.addr,
                                session_duration_ms = clock::elapsed(self.state.created_at).as_millis() as u64,
                                "Client disconnected"
                            );
                            break;
                        }
                    };

                    // Process first command (buffers response, no flush yet)
                    let mut should_break = false;
                    match self.process_one_command(frame).await {
                        FrameAction::Break => should_break = true,
                        // Stash the handoff and break *after* the shared flush
                        // below, so buffered pipelined replies reach the wire
                        // before the socket is taken over.
                        FrameAction::Handoff(handoff) => {
                            self.pending_psync_handoff = Some(handoff);
                            should_break = true;
                        }
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
                                        WireResponse::error(format!("ERR {}", e.details()))
                                    ).await;
                                    continue;
                                }
                            };
                            match self.process_one_command(frame).await {
                                FrameAction::Break => {
                                    should_break = true;
                                    break;
                                }
                                FrameAction::Handoff(handoff) => {
                                    self.pending_psync_handoff = Some(handoff);
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

                // 3. Handle pub/sub messages from shards
                Some(drained) = async {
                    match self.pubsub_rx.as_mut() {
                        Some(rx) => rx.recv_or_overflow().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // An overflow with an empty channel surfaces here directly:
                    // the flood was dropped to keep memory bounded, so there is
                    // no message to drain — tear the slow subscriber down.
                    let pubsub_msg = match drained {
                        frogdb_core::Drained::Message(msg) => msg,
                        frogdb_core::Drained::Overflowed => {
                            self.disconnect_overflowed_subscriber();
                            break;
                        }
                    };
                    // Buffer the first pub/sub message
                    let response = pubsub_msg.to_response_with_protocol(self.state.protocol_version);
                    if self.feed_response(Self::narrow_to_wire(response)).await.is_err() {
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
                            if self.feed_response(Self::narrow_to_wire(response)).await.is_err() {
                                break;
                            }
                        }
                    }
                    // Single flush for all pub/sub messages
                    if self.flush_responses().await.is_err() {
                        debug!(conn_id = self.state.id, "Failed to flush pub/sub responses");
                        break;
                    }
                    // If the per-connection pub/sub output buffer overflowed while
                    // this subscriber was too slow to drain it, the server dropped
                    // messages to keep memory bounded. Redis disconnects such a
                    // client (client-output-buffer-limit pubsub). We do the same,
                    // best-effort, on the next drain after a successful flush.
                    if self
                        .pubsub_rx
                        .as_ref()
                        .is_some_and(|rx| rx.has_overflowed())
                    {
                        self.disconnect_overflowed_subscriber();
                        break;
                    }
                }

                // Handle invalidation messages (CLIENT TRACKING)
                Some(inv_msg) = async {
                    match self.tracking_io.invalidation_rx.as_mut() {
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
                    if let Some(ref mut rx) = self.tracking_io.invalidation_rx {
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
                                if self.feed_response(WireResponse::Simple(frogdb_protocol::SafeStatus::sanitized(formatted))).await.is_err() {
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

                // 6. Housekeeping tick. Last on purpose: it is the only arm that
                // does no work for the client, so any real traffic outranks it.
                _ = idle_tick.tick() => {
                    if self.on_idle_tick().is_err() {
                        // The output-buffer seam condemned this connection while
                        // it sat idle (the soft window ran out). `on_idle_tick`
                        // has already released the buffer, logged and counted.
                        break;
                    }
                }
            }
        }

        // Check if we need to do PSYNC handoff
        if let Some(PsyncHandoff {
            replication_id,
            offset,
        }) = self.pending_psync_handoff.take()
        {
            info!(
                conn_id = self.state.id,
                addr = %self.state.addr,
                replication_id = %replication_id,
                offset = offset,
                "Performing PSYNC handoff"
            );

            // A handoff is only ever stashed after `ReplicationHandshake` passed the
            // `primary_replication_handler.is_none()` gate, so the handler is
            // present here by construction. The former no-handler `else` (a
            // silent warn) is thus dead; this `expect` documents the invariant
            // and would surface a dispatch-order regression loudly rather than
            // silently dropping the replica.
            let handler =
                self.cluster.primary_replication_handler.as_ref().expect(
                    "ReplicationHandshake gates handler presence before yielding a handoff",
                );

            // ACCOUNTING GAP: the socket leaves output-buffer accounting here.
            // `OutputBufferAccount`'s `Charge` is dropped with `self` at the end
            // of this branch, which correctly releases the bytes this connection
            // still held, but nothing takes over: from this point the
            // replication feed's buffering is charged to no `NetworkOutput`
            // budget and judged against no `client-output-buffer-limit` class,
            // so the `replica` class governs only the pre-handoff connection.
            // Closing this means charging inside the replication crates, which
            // is spec-first work under `specs/replication.md`; it is filed
            // separately and recorded in `specs/memory.md` FM-MEMORY-001's
            // "NOT observable" and in the `client-output-buffer-limit` docs.
            //
            // Extract the ConnectionStream from the Framed codec and type-erase
            // it for the replication handler (`handle_psync` takes a
            // `BoxedStream`). Non-turmoil: `into_boxed` preserves TLS if
            // active; turmoil: the simulated `TcpStream` implements
            // `AsyncRead`/`AsyncWrite` and boxes directly, so primary+replica
            // pairs work under simulation too.
            let connection_stream = self.framed.into_inner();
            #[cfg(not(feature = "turmoil"))]
            let boxed_stream = connection_stream.into_boxed();
            #[cfg(feature = "turmoil")]
            let boxed_stream: frogdb_replication::BoxedStream = Box::new(connection_stream);

            if let Err(e) = handler
                .handle_psync(
                    boxed_stream,
                    self.state.addr,
                    &replication_id,
                    offset,
                    self.replica_announcement,
                )
                .await
            {
                warn!(
                    conn_id = self.state.id,
                    error = %e,
                    "PSYNC handoff failed"
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

        // Return this connection's buffers to the core's pool instead of freeing
        // them. A closing connection is exactly when the next one is likely to
        // be accepted on this core, and its read buffer is the right size for
        // that connection's first command. Both buffers are dead here, so the
        // usual "only trim what is empty" guard does not apply — anything still
        // in them is bytes we are never going to write.
        self.release_buffers_to_pool();

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
