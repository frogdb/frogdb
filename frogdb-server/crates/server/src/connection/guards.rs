//! Pre-execution checks and validation guards.
//!
//! This module owns the *guard* half of the pre-dispatch gauntlet (see
//! [`crate::connection::dispatch`] for the ordering and the driver). Guard
//! stages are pure decisions over a [`PreDispatchView`] — a borrowed view of
//! everything the guards read or mutate (connection-mode state, the registry,
//! cluster/admin dep handles) and *nothing on the socket*. That is what makes
//! them unit-testable with no loopback TCP pair: contrast the historical
//! `make_test_handler`, which bound `127.0.0.1:0` purely to construct a
//! `ConnectionHandler` for `run_pre_checks`.
//!
//! Guard predicates living here:
//! - [`PreDispatchView::run_pre_checks`] — auth / replica-readonly / quorum-fence
//!   / admin-port / ACL / pub-sub-mode gate
//! - [`PreDispatchView::validate_cluster_slots`] — MOVED/ASK/CROSSSLOT routing
//! - [`PreDispatchView::check_migrating_multikey`] — TRYAGAIN during slot rehash
//! - [`PreDispatchView::pubsub_mode_ping`] — RESP2 `["pong", msg]` framing
//! - [`PreDispatchView::arity_check`] — wrong-arg-count rejection
//! - [`PreDispatchView::try_queue_in_transaction`] — MULTI queuing (+ slot
//!   pre-validation) and [`PreDispatchView::queue_command`]
//!
//! Handler-side helpers that legitimately need more than the view (post-execution
//! ASK conversion, rate limiting) stay on [`ConnectionHandler`].

use bytes::Bytes;
use frogdb_core::{
    AclManager, CommandFlags, CommandRegistry, ConnectionLevelOp, CoreMsg, ExecutionStrategy,
    RateLimitExceeded, ScatterOp, ShardSender, shard_for_key, slot_for_key,
};
use frogdb_protocol::{ParsedCommand, Response};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::connection::deps::ClusterDeps;
use crate::connection::next_txid;
use crate::connection::permission_guard::{PermissionGuard, build_permission_guard};
use crate::connection::state::ConnectionState;
use crate::connection::util::{extract_subcommand, key_access_type_for_flags};
use crate::slot_migration::{RouteDecision, RouteOutcome, SlotValidator, redirect};

/// Everything the pre-dispatch gauntlet's *guard* stages read or mutate —
/// connection-mode state, the registry, cluster/admin dep handles, per-command
/// scratch — and nothing on the socket. Constructible in a unit test with no
/// TCP pair (contrast the old `make_test_handler`). The `PreChecks`, `Arity`,
/// `PubSubPing`, `TransactionQueue`, `ClusterSlotValidation`, and
/// `MigratingTryAgain` stages are pure functions over this view.
///
/// The view holds `&mut` borrows only for the synchronous guard body (or, for
/// the one async guard, across a single shard round-trip); it is built and
/// dropped inside each guard arm of `run_stage`, before any dispatch executor
/// re-borrows `self`. That is the borrow discipline the pre-dispatch driver
/// depends on.
pub(crate) struct PreDispatchView<'a> {
    /// Connection-mode state: `in_pubsub_mode` / `in_transaction` / `take_asking`
    /// / `is_readonly` / auth. Mutated by the guards (mode state) but never the
    /// socket.
    pub(crate) state: &'a mut ConnectionState,
    /// Command registry: arity, flags, execution strategy, key extraction.
    pub(crate) registry: &'a CommandRegistry,
    /// Cluster dependency handles (slot migration coordinator, node id, quorum
    /// checker, cluster state).
    pub(crate) cluster: &'a ClusterDeps,
    /// ACL manager, for building the per-connection [`PermissionGuard`].
    pub(crate) acl_manager: &'a AclManager,
    /// Runtime config, read live on the write path for the
    /// `min-replicas-to-write` gate (so `CONFIG SET` takes effect at once).
    pub(crate) config_manager: &'a crate::runtime_config::ConfigManager,
    /// Shard senders, for the multi-key MIGRATING presence scatter.
    pub(crate) shard_senders: &'a [ShardSender],
    /// Replica flag: writes are rejected on a read-only replica.
    pub(crate) is_replica: &'a AtomicBool,
    /// Whether this is an admin-port connection.
    pub(crate) is_admin: bool,
    /// Whether admin-port separation is enabled.
    pub(crate) admin_enabled: bool,
    /// Number of shards, for slot→shard routing.
    pub(crate) num_shards: usize,
    /// Scatter-gather timeout for the MIGRATING presence check.
    pub(crate) scatter_gather_timeout: Duration,
}

impl ConnectionHandler {
    /// Build the socketless [`PreDispatchView`] over this handler's fields.
    ///
    /// Borrows the exact fields the guard stages name (disjoint from `framed`),
    /// so the returned view can mutate connection-mode state while the guard body
    /// runs, then is dropped before any dispatch executor re-borrows `self`.
    pub(crate) fn pre_dispatch_view(&mut self) -> PreDispatchView<'_> {
        PreDispatchView {
            state: &mut self.state,
            registry: &self.core.registry,
            cluster: &self.cluster,
            acl_manager: &self.core.acl_manager,
            config_manager: &self.admin.config_manager,
            shard_senders: &self.core.shard_senders,
            is_replica: &self.is_replica,
            is_admin: self.is_admin,
            admin_enabled: self.admin_enabled,
            num_shards: self.num_shards,
            scatter_gather_timeout: self.scatter_gather_timeout,
        }
    }

    /// Check if a command is exempt from rate limiting.
    /// AUTH, HELLO, PING, QUIT, and RESET are always exempt.
    pub(crate) fn is_rate_limit_exempt(cmd_name: &str) -> bool {
        matches!(cmd_name, "AUTH" | "HELLO" | "PING" | "QUIT" | "RESET")
    }

    /// Check rate limit for the current command.
    /// Returns `Some(Response)` if the command should be rejected, `None` if allowed.
    #[allow(clippy::result_large_err)]
    pub(crate) fn check_rate_limit(&self, cmd_name: &str, cmd_bytes: u64) -> Option<Response> {
        if self.is_admin {
            return None;
        }
        let user = self.state.authenticated_user()?;
        let rl = user.rate_limit.as_ref()?;
        if Self::is_rate_limit_exempt(cmd_name) {
            return None;
        }
        match rl.try_acquire(cmd_bytes) {
            Ok(()) => None,
            Err(RateLimitExceeded::Commands) => Some(Response::error(
                "ERR rate limit exceeded: commands per second",
            )),
            Err(RateLimitExceeded::Bytes) => {
                Some(Response::error("ERR rate limit exceeded: bytes per second"))
            }
        }
    }

    /// For a MIGRATING slot (we are the source), check if the command's response
    /// indicates the key doesn't exist locally. If so, return an ASK redirect
    /// so the client retries on the importing target.
    ///
    /// Redis behavior: during MIGRATING, existing keys are served locally;
    /// missing keys (already migrated away) get `-ASK` redirect.
    ///
    /// This runs *after* execution (`Execute` terminal), so it stays on the
    /// handler rather than the pre-dispatch view.
    pub(crate) fn migrating_ask_for_nil(
        &self,
        cmd: &ParsedCommand,
        response: &Response,
    ) -> Option<Response> {
        // Only relevant in cluster mode
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;

        // Check if response indicates "key not found"
        if !is_nil_response(response) {
            return None;
        }

        // Get the first key's slot
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.core.registry.get_entry(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None;
        };
        if keys.is_empty() {
            return None;
        }
        let slot = slot_for_key(keys[0]);

        // Check if we own this slot and it's in MIGRATING state
        let snapshot = cluster_state.snapshot();
        if let Some(owner) = snapshot.get_slot_owner(slot)
            && owner == node_id
            && let Some(migration) = snapshot.migrations.get(&slot)
            && let Some(target_node) = snapshot.nodes.get(&migration.target_node)
        {
            return Some(ask_response(slot, target_node.addr));
        }

        None
    }
}

impl PreDispatchView<'_> {
    /// Build an ACL [`PermissionGuard`] for the connection's current user, or
    /// `None` when unauthenticated (ACL not enforced). Shares its construction
    /// with [`ConnectionHandler::permission_guard`].
    pub(crate) fn permission_guard(&self) -> Option<PermissionGuard<'_>> {
        build_permission_guard(self.acl_manager, self.state)
    }

    /// Check if a command is allowed in pub/sub mode.
    ///
    /// In RESP3, all commands are allowed while subscribed -- responses come
    /// back inline and pub/sub messages are delivered as out-of-band Push
    /// frames. In RESP2, only (P|S)SUBSCRIBE, (P|S)UNSUBSCRIBE, PING, QUIT,
    /// and RESET are allowed.
    pub(crate) fn is_allowed_in_pubsub_mode(&self, cmd_name: &str) -> bool {
        // RESP3: all commands are allowed in subscribed mode.
        if self.state.protocol_version.is_resp3() {
            return true;
        }
        // RESP2: PING and QUIT are special cases - always allowed
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
    pub(crate) fn is_auth_exempt(&self, cmd_name: &str) -> bool {
        // These commands are always allowed without authentication (matches Redis 7+ behavior):
        // - QUIT: client disconnection
        // - PING: health check / keepalive
        // - HELLO: protocol negotiation (can also carry AUTH inline)
        if matches!(cmd_name, "QUIT" | "PING" | "HELLO") {
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

    /// Run pre-execution checks for a command (`PreChecks` stage).
    ///
    /// This validates:
    /// - Authentication (if required)
    /// - Replica read-only rejection
    /// - Self-fence (quorum-loss write rejection)
    /// - Admin port restrictions
    /// - ACL command permissions
    /// - Pub/sub mode restrictions
    ///
    /// Returns `Some(Response)` with an error if the command should be rejected,
    /// or `None` if the command can proceed.
    pub(crate) fn run_pre_checks(&self, cmd_name: &str, args: &[Bytes]) -> Option<Response> {
        // Check authentication
        if !self.state.is_authenticated() && !self.is_auth_exempt(cmd_name) {
            return Some(Response::error("NOAUTH Authentication required."));
        }

        // Block write commands on replicas — but a cluster slot redirect takes
        // precedence over the read-only-replica rejection.
        //
        // Redis `processCommand` runs the cluster redirection (`getNodeByQuery`,
        // which yields `-MOVED`/`-CROSSSLOT`/`-ASK`) *before* the read-only
        // replica check (`server.masterhost && server.repl_slave_ro`). So a
        // keyed write targeting a slot committed to another node must be
        // answered with `-MOVED` (the slot's primary), never `-READONLY` —
        // [`Self::validate_cluster_slots`] issues that redirect at the
        // `ClusterSlotValidation` stage, and at queue time inside
        // [`Self::try_queue_in_transaction`]. Deferring here (rather than
        // short-circuiting with `-READONLY`) makes the reply deterministic
        // regardless of whether the async replica-role flag has been applied
        // yet; without it, a keyed write races the flag and intermittently
        // leaks `-READONLY` where `-MOVED` is required. The deferral is
        // ownership-aware — see [`Self::write_defers_to_cluster_redirect`] for
        // why (a slot-owning replica is reachable in FrogDB and must keep the
        // `-READONLY` rejection). Keyless writes (FLUSHALL, …) and
        // standalone-replication writes are not slot-redirectable and still
        // get `-READONLY` here.
        //
        // Flag checks read `get_entry` (all registered commands), not `get`
        // (shard commands only), so a connection-level command like CONFIG —
        // now a `CommandImpl::Connection` entry with no shard executor — is
        // still visible to these gates.
        if self.is_replica.load(Ordering::Relaxed)
            && let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && !self.write_defers_to_cluster_redirect(cmd_name, args)
        {
            return Some(Response::error(
                "READONLY You can't write against a read only replica.",
            ));
        }

        // Self-fence: reject writes when quorum is lost in cluster mode
        if let Some(ref qc) = self.cluster.quorum_checker
            && let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && !qc.has_quorum()
        {
            return Some(Response::error(
                "CLUSTERDOWN The cluster is down (quorum lost, writes rejected)",
            ));
        }

        // min-replicas-to-write: reject writes when fewer than the configured
        // number of "good" (recently-ACKing streaming) replicas are connected —
        // Redis's `NOREPLICAS` write-safety gate. Unlike the self-fence checker
        // above this does not "arm": with `min-replicas-to-write N` and zero
        // replicas the primary refuses writes from boot, exactly as Redis does.
        // The config is read live so `CONFIG SET` applies immediately; the read
        // only happens for WRITE-flagged commands.
        //
        // NOTE: like the self-fence gate this fires only in `run_pre_checks`,
        // which covers direct writes and MULTI *queue* time. Writes issued from
        // inside a Lua script (`redis.call`, EVAL lacks the WRITE flag) and the
        // narrow window where a MULTI is queued while replicas are healthy and
        // then EXEC'd after they drop are NOT gated here — a bound shared with
        // self-fence, tracked as a follow-up (uniform enforcement belongs at the
        // shard/script-gate write seam).
        if let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
        {
            let min_replicas = self.config_manager.min_replicas_to_write();
            if min_replicas > 0 {
                let max_lag = Duration::from_millis(self.config_manager.min_replicas_timeout_ms());
                let good = self
                    .cluster
                    .replication_tracker
                    .as_ref()
                    .map(|t| t.count_good_replicas(max_lag))
                    .unwrap_or(0);
                if good < min_replicas {
                    return Some(Response::error(
                        "NOREPLICAS Not enough good replicas to write.",
                    ));
                }
            }
        }

        // Block admin commands on regular port when admin port is enabled
        if self.admin_enabled
            && !self.is_admin
            && let Some(cmd_info) = self.registry.get_entry(cmd_name)
            && cmd_info.flags().contains(CommandFlags::ADMIN)
        {
            return Some(Response::error(
                "NOADMIN Admin commands are disabled on this port. Use the admin port.",
            ));
        }

        // Check command ACL permission through the unified enforcement seam.
        // Note: ACL command is exempt (users need ACL WHOAMI to check their identity).
        if cmd_name != "ACL"
            && let Some(guard) = self.permission_guard()
        {
            let subcommand = extract_subcommand(cmd_name, args);
            if let Err(err) = guard.check_command(cmd_name, subcommand.as_deref()) {
                return Some(err);
            }
        }

        // Check pub/sub mode restrictions
        if self.state.in_pubsub_mode() && !self.is_allowed_in_pubsub_mode(cmd_name) {
            return Some(Response::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd_name
            )));
        }

        None
    }

    /// Whether a keyed write on this (replica) connection targets a slot owned
    /// by *another* node — i.e. it will be answered by a cluster slot redirect
    /// (`-MOVED`), which must take precedence over the read-only-replica
    /// rejection (`-READONLY`).
    ///
    /// Redis `processCommand` runs the cluster redirect (`getNodeByQuery`)
    /// before the `repl_slave_ro` check, so a keyed write to a slot this node
    /// does not serve must be `-MOVED`, never `-READONLY`. [`Self::run_pre_checks`]
    /// consults this to defer the read-only rejection in exactly that case.
    ///
    /// SAFETY — this check is deliberately *ownership-aware* rather than assuming
    /// the invariant "a replica never owns the slot for its keys". FrogDB
    /// auto-assigns slots to bootstrapping nodes, so a node that later becomes a
    /// replica (CLUSTER REPLICATE flips only role/primary_id, not slot ownership)
    /// can still own slots — a slot-owning replica is reachable. If we deferred
    /// purely on "is a keyed write", such a replica would route `LocalServe` and
    /// *execute* the write locally (silent divergence). By deferring only when
    /// the target slot is committed to a *different* node, the dangerous case
    /// (this replica owns the slot, or the slot is unassigned) falls through to
    /// the `-READONLY` rejection — the safe answer — with no dependency on any
    /// topology-level invariant. The common case (replica does not own the
    /// slot; its primary does) defers to `-MOVED` as required.
    ///
    /// Keyless writes (FLUSHALL, …), cluster-exempt commands, and
    /// standalone-replication writes return `false` and stay `-READONLY`.
    fn write_defers_to_cluster_redirect(&self, cmd_name: &str, args: &[Bytes]) -> bool {
        // Cluster mode is gated by the same handles `validate_cluster_slots`
        // requires; without them no redirect is produced and READONLY must win.
        let (Some(node_id), Some(cluster_state)) =
            (self.cluster.node_id, self.cluster.cluster_state.as_ref())
        else {
            return false;
        };
        if self.cluster.slot_migration.is_none() {
            return false;
        }
        // Connection-level / scatter-gather / server-wide (and CLUSTER/PING/…)
        // commands are not slot-routed, so they are never redirected.
        if self.is_cluster_exempt(cmd_name) {
            return false;
        }
        // Only keyed commands are slot-routed; a keyless write is not
        // redirectable and stays under the READONLY rejection.
        let Some(entry) = self.registry.get_entry(cmd_name) else {
            return false;
        };
        let keys = entry.keys(args);
        if keys.is_empty() {
            return false;
        }
        // Defer to `-MOVED` only when the target slot is committed to a *different*
        // node. If this replica owns the slot, or it is unassigned, keep
        // `-READONLY` (never let a replica execute a keyed write locally).
        // Cluster requires all keys in one slot, so the first key's slot is
        // representative (a genuine cross-slot command is caught later as
        // CROSSSLOT).
        let slot = slot_for_key(keys[0]);
        matches!(
            cluster_state.snapshot().get_slot_owner(slot),
            Some(owner) if owner != node_id
        )
    }

    /// PING has bespoke framing while subscribed (`PubSubPing` stage), so it
    /// cannot use the standard shard PING path: in RESP2 a subscribed PING
    /// replies with an array `["pong", <message>]`; in RESP3 it replies with the
    /// simple `PONG` (or the message argument). Returns `Some` only when the
    /// connection is in pub/sub mode *and* the command is PING; every other case
    /// returns `None` and continues down the normal dispatch flow.
    pub(crate) fn pubsub_mode_ping(&self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        if !self.state.in_pubsub_mode() || cmd_name != "PING" {
            return None;
        }
        let response = if self.state.protocol_version.is_resp3() {
            if args.is_empty() {
                Response::pong()
            } else {
                Response::bulk(args[0].clone())
            }
        } else {
            let message = if args.is_empty() {
                Bytes::from_static(b"")
            } else {
                args[0].clone()
            };
            Response::Array(vec![
                Response::bulk(Bytes::from_static(b"pong")),
                Response::bulk(message),
            ])
        };
        Some(vec![response])
    }

    /// Validate arity (`Arity` stage). Returns `Some(Response)` with the
    /// wrong-arg-count error if the command's argument count is invalid, `None`
    /// otherwise. Ordered *before* the pause gate so syntax errors bypass pause.
    pub(crate) fn arity_check(&self, cmd_name: &str, args: &[Bytes]) -> Option<Response> {
        let entry = self.registry.get_entry(cmd_name)?;
        if entry.arity().check(args.len()) {
            return None;
        }
        Some(Response::error(format!(
            "ERR wrong number of arguments for '{}' command",
            entry.name().to_ascii_lowercase()
        )))
    }

    /// If in transaction mode, queue the command instead of executing it
    /// (`TransactionQueue` stage). Returns `Some(responses)` to short-circuit
    /// (the QUEUED reply, or a cluster-slot abort), or `None` to continue.
    ///
    /// Cluster slot ownership is validated *before* queuing — commands that would
    /// get MOVED fail immediately rather than succeeding at EXEC time. Runs
    /// before connection-level dispatch so commands like CLIENT PAUSE, EVAL, etc.
    /// are queued during MULTI (not executed immediately).
    pub(crate) fn try_queue_in_transaction(
        &mut self,
        cmd: &ParsedCommand,
    ) -> Option<Vec<Response>> {
        if !self.state.in_transaction() {
            return None;
        }
        // Validate cluster slot ownership before queuing — commands that would
        // get MOVED should fail immediately rather than succeeding at EXEC time.
        if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
            let error_msg = match &cluster_error {
                Response::Error(e) => Some(String::from_utf8_lossy(e).to_string()),
                _ => None,
            };
            self.state.abort_transaction(error_msg);
            return Some(vec![cluster_error]);
        }
        Some(vec![self.queue_command(cmd)])
    }

    /// Queue a command for later EXEC. Validates the command (unknown/arity),
    /// enforces key + channel ACL permissions (so queue-time denials log to ACL
    /// LOG exactly like the live paths), folds its keys into the transaction
    /// target, and pushes it. Returns the `QUEUED` reply, or an error response
    /// that also aborts the transaction.
    pub(crate) fn queue_command(&mut self, cmd: &ParsedCommand) -> Response {
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);

        // Look up command for validation (get_entry covers both full and metadata-only
        // commands, so connection-level commands like PUBLISH/SPUBLISH can be queued).
        let entry = match self.registry.get_entry(&cmd_name_str) {
            Some(e) => e,
            None => {
                let msg = format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name_str
                );
                self.state.abort_transaction(Some(msg.clone()));
                return Response::error(msg);
            }
        };

        // Validate arity
        if !entry.arity().check(cmd.args.len()) {
            let msg = format!(
                "ERR wrong number of arguments for '{}' command",
                entry.name()
            );
            self.state.abort_transaction(Some(msg.clone()));
            return Response::error(msg);
        }

        // Extract keys for same-slot validation
        let keys = entry.keys(&cmd.args);

        // Check key + channel permissions through the unified enforcement seam, so
        // queue-time denials are logged to ACL LOG exactly like the live paths.
        // (The command itself is already validated upstream by run_pre_checks.)
        if let Some(guard) = self.permission_guard() {
            if !keys.is_empty() {
                // Per-key access (STORE-family: write dest, read sources), so a
                // MULTI-queued command's denial matches direct dispatch exactly.
                let keyed_flags = entry.keys_with_flags(&cmd.args);
                let fallback = key_access_type_for_flags(entry.flags());
                if let Err(err) = guard.check_keys_with_flags(&keyed_flags, fallback) {
                    return err;
                }
            }
            match cmd_name_str.as_ref() {
                // First arg is the channel.
                "PUBLISH" | "SPUBLISH" => {
                    if let Some(channel) = cmd.args.first()
                        && let Err(err) = guard.check_channels(std::slice::from_ref(channel))
                    {
                        return err;
                    }
                }
                // All args are channels.
                "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                    if let Err(err) = guard.check_channels(&cmd.args) {
                        return err;
                    }
                }
                _ => {}
            }
        }

        // Fold this command's keys into the transaction target. In cluster mode
        // the accumulator uses slot-level detection (Redis requires all keys in
        // one slot); in standalone mode, shard-level detection.
        let is_cluster = self.cluster.cluster_state.is_some();
        self.state
            .fold_transaction_keys(&keys, self.num_shards, is_cluster);

        // Queue the command
        self.state.push_queued_command(cmd.clone());

        Response::Simple(Bytes::from_static(b"QUEUED"))
    }

    /// Check if a command is exempt from slot validation in cluster mode.
    /// Connection-level and admin commands that don't operate on specific keys are exempt.
    pub(crate) fn is_cluster_exempt(&self, cmd_name: &str) -> bool {
        // Certain commands are always exempt
        if matches!(cmd_name, "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG") {
            return true;
        }
        // Connection-level, scatter-gather, and server-wide commands are exempt
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(_)
                    | ExecutionStrategy::ScatterGather(_)
                    | ExecutionStrategy::ServerWide(_)
            )
        })
    }

    /// Validate slot ownership for keys in cluster mode (`ClusterSlotValidation`
    /// stage). Returns Some(Response) if validation fails (CROSSSLOT/MOVED/ASK),
    /// None if OK. Consumes `take_asking` exactly once.
    pub(crate) fn validate_cluster_slots(&mut self, cmd: &ParsedCommand) -> Option<Response> {
        // Only validate if cluster mode is enabled
        let coordinator = self.cluster.slot_migration.as_ref()?;
        let node_id = self.cluster.node_id?;

        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);

        // Skip cluster-exempt commands (using execution strategy for type-safe check)
        if self.is_cluster_exempt(&cmd_name) {
            return None;
        }

        // Get keys from command using the registry
        let keys = if let Some(cmd_impl) = self.registry.get_entry(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None; // Unknown command, let execute handle it
        };

        // No keys = no slot validation needed
        if keys.is_empty() {
            return None;
        }

        // CROSSSLOT check — all keys must hash to one slot (the strict cluster
        // notion). The empty case returned above, so `Ok(None)` is unreachable.
        let first_slot = match SlotValidator::same_slot(&keys) {
            Ok(Some(slot)) => slot,
            Ok(None) => return None,
            Err(crossslot) => return Some(crossslot),
        };

        // ASKING is a one-shot flag consumed by routing. Read-and-clear up front;
        // the LocalServe arm restores it, preserving the historical quirk that a
        // command routed to a slot we fully own does not consume ASKING.
        let asking = self.state.take_asking();
        let decision = coordinator.route(first_slot, &cmd_name, asking, node_id);

        // LocalServe historically preserves ASKING when we fully own the slot.
        if matches!(decision, RouteDecision::LocalServe) && asking {
            self.state.set_asking();
        }

        // READONLY mode: allow read-only commands to execute locally even though
        // we don't own the slot (replica reads). Only consulted by the `Moved`
        // arm inside `to_response`; harmless to compute for the others.
        let readonly_eligible = self.state.is_readonly()
            && self
                .registry
                .get_entry(&cmd_name)
                .is_some_and(|c| c.flags().contains(CommandFlags::READONLY));

        match decision.to_response(readonly_eligible) {
            RouteOutcome::ServeLocal => None,
            RouteOutcome::Reply(resp) => Some(resp),
        }
    }

    /// For multi-key commands targeting a MIGRATING slot, check key presence
    /// and return TRYAGAIN if keys are split between source and target
    /// (`MigratingTryAgain` stage).
    ///
    /// Redis semantics:
    /// - All keys present locally → serve locally (None)
    /// - All keys absent → ASK redirect
    /// - Mixed presence → TRYAGAIN
    pub(crate) async fn check_migrating_multikey(&self, cmd: &ParsedCommand) -> Option<Response> {
        // Only relevant in cluster mode
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;

        // Get keys from command
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.registry.get_entry(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None;
        };

        // Single-key commands are handled by existing migrating_ask_for_nil
        if keys.len() < 2 {
            return None;
        }

        let slot = slot_for_key(keys[0]);

        // Check if we own the slot and it's in MIGRATING state
        let snapshot = cluster_state.snapshot();
        let owner = snapshot.get_slot_owner(slot)?;
        if owner != node_id {
            return None;
        }
        let migration = snapshot.migrations.get(&slot)?;
        let target_addr = snapshot.nodes.get(&migration.target_node)?.addr;

        // Send EXISTS scatter to the owning shard to check key presence
        let shard_id = shard_for_key(keys[0], self.num_shards);
        let keys_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::copy_from_slice(k)).collect();

        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ScatterRequest {
            request_id: next_txid(),
            keys: keys_bytes,
            operation: ScatterOp::Exists,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Some(Response::error("ERR shard unavailable"));
        }

        let partial = match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => partial,
            _ => return Some(Response::error("ERR shard unavailable")),
        };

        let mut any_present = false;
        let mut any_absent = false;
        for (_, response) in partial.keyed_slice() {
            match response {
                Response::Integer(1) => any_present = true,
                Response::Integer(0) => any_absent = true,
                _ => {}
            }
            if any_present && any_absent {
                return Some(Response::error(
                    "TRYAGAIN Multiple keys request during rehashing of slot",
                ));
            }
        }

        if any_absent && !any_present {
            // All keys migrated away — ASK redirect
            return Some(ask_response(slot, target_addr));
        }

        // All keys present locally — serve normally
        None
    }
}

fn is_nil_response(response: &Response) -> bool {
    matches!(response, Response::Null | Response::Bulk(None))
}

fn ask_response(slot: u16, addr: SocketAddr) -> Response {
    redirect::ask(slot, addr)
}

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_core::command::QuorumChecker;
    use std::sync::Arc;

    struct MockQuorumChecker {
        has_quorum: bool,
    }

    impl QuorumChecker for MockQuorumChecker {
        fn has_quorum(&self) -> bool {
            self.has_quorum
        }
        fn count_reachable_nodes(&self) -> usize {
            if self.has_quorum { 2 } else { 1 }
        }
    }

    /// Socketless fixtures for exercising [`PreDispatchView`] guard predicates.
    ///
    /// Replaces the historical `make_test_handler`, which bound `127.0.0.1:0`,
    /// accepted, and connected purely to construct a `ConnectionHandler` before
    /// calling `run_pre_checks`. The view owns no socket, so these fixtures hold
    /// the borrowed pieces (state, registry, cluster deps, ...) and hand out a
    /// `PreDispatchView` on demand — no tokio TCP pair.
    struct ViewFixture {
        state: ConnectionState,
        registry: Arc<CommandRegistry>,
        cluster: ClusterDeps,
        acl_manager: Arc<AclManager>,
        shard_senders: Vec<ShardSender>,
        is_replica: AtomicBool,
        is_admin: bool,
        admin_enabled: bool,
        config_manager: Arc<crate::runtime_config::ConfigManager>,
    }

    impl ViewFixture {
        fn new(quorum_checker: Option<Arc<dyn QuorumChecker>>) -> Self {
            let mut registry = CommandRegistry::new();
            crate::register_commands(&mut registry);
            let cluster = ClusterDeps {
                quorum_checker,
                ..ClusterDeps::default()
            };
            Self {
                state: ConnectionState::new(1, "127.0.0.1:9999".parse().unwrap(), false),
                registry: Arc::new(registry),
                cluster,
                acl_manager: AclManager::new(Default::default()),
                shard_senders: Vec::new(),
                is_replica: AtomicBool::new(false),
                is_admin: false,
                admin_enabled: false,
                config_manager: Arc::new(crate::runtime_config::ConfigManager::new(
                    &crate::config::Config::default(),
                )),
            }
        }

        fn view(&mut self) -> PreDispatchView<'_> {
            PreDispatchView {
                state: &mut self.state,
                registry: &self.registry,
                cluster: &self.cluster,
                acl_manager: self.acl_manager.as_ref(),
                config_manager: &self.config_manager,
                shard_senders: &self.shard_senders,
                is_replica: &self.is_replica,
                is_admin: self.is_admin,
                admin_enabled: self.admin_enabled,
                num_shards: 1,
                scatter_gather_timeout: Duration::from_millis(5000),
            }
        }
    }

    #[test]
    fn test_self_fence_write_rejected_when_quorum_lost() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: false });
        let mut fx = ViewFixture::new(Some(qc));

        let result = fx.view().run_pre_checks("SET", &[]);
        assert!(result.is_some());
        match result.unwrap() {
            Response::Error(msg) => {
                assert!(
                    msg.starts_with(b"CLUSTERDOWN"),
                    "expected CLUSTERDOWN error, got: {}",
                    String::from_utf8_lossy(&msg)
                );
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }

    #[test]
    fn test_self_fence_read_allowed_when_quorum_lost() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: false });
        let mut fx = ViewFixture::new(Some(qc));

        let result = fx.view().run_pre_checks("GET", &[]);
        assert!(
            result.is_none(),
            "GET should be allowed when quorum is lost"
        );
    }

    #[test]
    fn test_self_fence_write_allowed_when_quorum_present() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: true });
        let mut fx = ViewFixture::new(Some(qc));

        let result = fx.view().run_pre_checks("SET", &[]);
        assert!(
            result.is_none(),
            "SET should be allowed when quorum is present"
        );
    }

    #[test]
    fn test_self_fence_no_quorum_checker_standalone() {
        let mut fx = ViewFixture::new(None);

        let result = fx.view().run_pre_checks("SET", &[]);
        assert!(
            result.is_none(),
            "SET should be allowed in standalone mode (no quorum checker)"
        );
    }

    #[test]
    fn test_replica_rejects_write_allows_read() {
        let mut fx = ViewFixture::new(None);
        fx.is_replica.store(true, Ordering::Relaxed);

        match fx.view().run_pre_checks("SET", &[]) {
            Some(Response::Error(msg)) => assert!(
                msg.starts_with(b"READONLY"),
                "expected READONLY, got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected READONLY error, got: {:?}", other),
        }
        assert!(
            fx.view().run_pre_checks("GET", &[]).is_none(),
            "GET should be allowed on a replica"
        );
    }

    #[test]
    fn test_admin_port_gate_rejects_admin_command_on_regular_port() {
        let mut fx = ViewFixture::new(None);
        fx.admin_enabled = true;
        fx.is_admin = false;

        // DEBUG carries the ADMIN flag; on the regular port with admin separation
        // enabled it is rejected with NOADMIN.
        match fx.view().run_pre_checks("DEBUG", &[]) {
            Some(Response::Error(msg)) => assert!(
                msg.starts_with(b"NOADMIN"),
                "expected NOADMIN, got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected NOADMIN error, got: {:?}", other),
        }
    }

    #[test]
    fn test_arity_check_rejects_wrong_argument_count() {
        let mut fx = ViewFixture::new(None);
        // GET takes exactly one argument; zero args is an arity error.
        match fx.view().arity_check("GET", &[]) {
            Some(Response::Error(msg)) => assert!(
                msg.starts_with(b"ERR wrong number of arguments"),
                "got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected arity error, got: {:?}", other),
        }
        // One argument is valid.
        assert!(
            fx.view()
                .arity_check("GET", &[Bytes::from_static(b"k")])
                .is_none()
        );
    }

    #[test]
    fn test_pubsub_mode_ping_resp2_framing() {
        let mut fx = ViewFixture::new(None);
        // Not in pub/sub mode: PING returns None (falls through to normal path).
        assert!(fx.view().pubsub_mode_ping("PING", &[]).is_none());

        // Enter pub/sub mode (RESP2 default) by subscribing to a channel.
        fx.state.add_subscription(
            crate::connection::state::SubKind::Channel,
            Bytes::from_static(b"c1"),
        );
        let responses = fx
            .view()
            .pubsub_mode_ping("PING", &[Bytes::from_static(b"hello")])
            .expect("PING in pub/sub mode returns bespoke framing");
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            Response::Array(items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Response::Bulk(Some(b)) if b.as_ref() == b"pong"));
                assert!(matches!(&items[1], Response::Bulk(Some(b)) if b.as_ref() == b"hello"));
            }
            other => panic!("expected [pong, msg] array, got: {:?}", other),
        }

        // Non-PING command returns None even in pub/sub mode.
        assert!(fx.view().pubsub_mode_ping("GET", &[]).is_none());
    }

    /// `is_allowed_in_pubsub_mode` unit coverage (no unit test existed prior to
    /// this task — see issue 28). Pins the exact RESP2 allow-set boundary
    /// against Redis 8.6's `processCommand` subscribe-context gate: SUBSCRIBE,
    /// UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING,
    /// QUIT, RESET — exactly 9 commands — plus a representative disallowed
    /// data command.
    #[test]
    fn test_is_allowed_in_pubsub_mode_resp2_allow_set_boundary() {
        let mut fx = ViewFixture::new(None);
        // Default protocol version is RESP2.
        assert!(!fx.view().state.protocol_version.is_resp3());

        for allowed in [
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
            "PING",
            "QUIT",
            "RESET",
        ] {
            assert!(
                fx.view().is_allowed_in_pubsub_mode(allowed),
                "{allowed} should be allowed while subscribed under RESP2"
            );
        }

        // Representative disallowed data commands.
        for disallowed in ["GET", "SET", "DEL"] {
            assert!(
                !fx.view().is_allowed_in_pubsub_mode(disallowed),
                "{disallowed} should be rejected while subscribed under RESP2"
            );
        }
    }

    /// RESP3 lifts the restriction entirely: every command, including plain
    /// data commands and even unknown ones, is allowed while subscribed —
    /// `is_allowed_in_pubsub_mode` short-circuits to `true` before consulting
    /// the registry (`guards.rs:202-204`).
    #[test]
    fn test_is_allowed_in_pubsub_mode_resp3_allows_everything() {
        let mut fx = ViewFixture::new(None);
        fx.state.protocol_version = frogdb_protocol::ProtocolVersion::Resp3;

        for cmd in ["GET", "SET", "DEL", "SUBSCRIBE", "PING", "NOSUCHCOMMAND"] {
            assert!(
                fx.view().is_allowed_in_pubsub_mode(cmd),
                "{cmd} should be allowed while subscribed under RESP3"
            );
        }
    }

    /// KNOWN DIVERGENCE from Redis: `is_allowed_in_pubsub_mode` permits any
    /// command sharing RESET's `ConnectionLevel(ConnectionState)` execution
    /// strategy, not just RESET itself. ASKING/READONLY/READWRITE share that
    /// strategy (`connection_state_conn_command.rs`), so they are — contrary
    /// to Redis 8.6, which allows only the 9 commands pinned above — also
    /// permitted while subscribed under RESP2. Pinned here so a future
    /// strategy split (or intentional accept) is a deliberate test change,
    /// not a silent behavior drift.
    #[test]
    fn test_is_allowed_in_pubsub_mode_resp2_connection_state_siblings_diverge_from_redis() {
        let mut fx = ViewFixture::new(None);
        for sibling in ["ASKING", "READONLY", "READWRITE"] {
            assert!(
                fx.view().is_allowed_in_pubsub_mode(sibling),
                "{sibling} shares RESET's ConnectionState strategy, so the \
                 current gate allows it too (divergence from Redis's 9-command \
                 allow-set — Redis rejects ASKING/READONLY/READWRITE while \
                 subscribed)"
            );
        }
    }

    #[test]
    fn test_noauth_rejected_when_auth_required() {
        // requires_auth = true, unauthenticated connection.
        let mut fx = ViewFixture::new(None);
        fx.state = ConnectionState::new(1, "127.0.0.1:9999".parse().unwrap(), true);

        // GET requires auth → NOAUTH.
        match fx.view().run_pre_checks("GET", &[]) {
            Some(Response::Error(msg)) => assert!(
                msg.starts_with(b"NOAUTH"),
                "expected NOAUTH, got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected NOAUTH error, got: {:?}", other),
        }
        // PING is auth-exempt.
        assert!(fx.view().run_pre_checks("PING", &[]).is_none());
    }
}
