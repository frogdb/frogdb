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
//! - [`PreDispatchView::check_migrating_source`] — the MIGRATING-source presence
//!   probe: serve / `ASK` / `TRYAGAIN` while a slot is being handed over
//! - [`PreDispatchView::validate_queued_batch`] — the EXEC-time twin of the two
//!   above: whole-queue MOVED/ASK/TRYAGAIN/CROSSSLOT re-validation against one
//!   cluster snapshot (called from `transaction.rs`, not from the gauntlet)
//! - [`PreDispatchView::validate_watch_slots`] and
//!   [`PreDispatchView::watched_slots_still_local`] — the same two decisions for
//!   the *watch set*, which the gauntlet structurally cannot reach: `WATCH`
//!   short-circuits at the `TransactionControl` stage, long before
//!   `ClusterSlotValidation` runs
//! - [`PreDispatchView::pubsub_mode_ping`] — RESP2 `["pong", msg]` framing
//! - [`PreDispatchView::command_lookup_check`] — unknown-command and
//!   wrong-arg-count rejection
//! - [`PreDispatchView::try_queue_in_transaction`] — MULTI queuing (+ slot
//!   pre-validation) and [`PreDispatchView::queue_command`]
//!
//! Handler-side helpers that legitimately need more than the view (post-execution
//! ASK conversion, rate limiting) stay on [`ConnectionHandler`].

use bytes::Bytes;
use frogdb_core::{
    AclManager, CommandFlags, CommandRegistry, ConnectionLevelOp, CoreMsg, ExecutionStrategy,
    RateLimitExceeded, ScatterOp, ShardSender, WatchEntry, admin_surface, shard_for_key,
    slot_for_key,
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
use crate::slot_migration::{
    BatchKeys, BatchRoute, RouteDecision, RouteOutcome, SlotValidator, SlotVerdict, redirect,
    route_migrating_source, route_queued_batch, route_watched_keys, route_with_snapshot,
    stamp_fence, watch_slot_is_locally_served,
};

/// The `-MISCONF` reply sent while `snapshot.stop-writes-on-save-error` is on
/// and the last background save failed.
///
/// The prefix is Redis' verbatim, because that is what clients and operator
/// runbooks match on. The rest is not: Redis' text says the dataset is at risk,
/// which would be a lie here. FrogDB acknowledges a write only once the WAL has
/// it, so a failed snapshot costs backup freshness, not acknowledged data — and
/// the remediation names the fields FrogDB actually publishes.
pub(crate) const SAVE_ERROR_REFUSAL: &str = "MISCONF Errors writing snapshots. \
     Refusing writes because snapshot.stop-writes-on-save-error is on. \
     Acknowledged writes are still durable in the WAL; backups are not. \
     Check rdb_last_bgsave_error in INFO persistence and disk space, then BGSAVE.";

/// Everything the pre-dispatch gauntlet's *guard* stages read or mutate —
/// connection-mode state, the registry, cluster/admin dep handles, per-command
/// scratch — and nothing on the socket. Constructible in a unit test with no
/// TCP pair (contrast the old `make_test_handler`). The `PreChecks`,
/// `CommandLookup`, `PubSubPing`, `TransactionQueue`, `ClusterSlotValidation`,
/// and
/// `MigratingSourceProbe` stages are pure functions over this view.
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
    /// Shard senders, for the MIGRATING-source presence scatter.
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
        if self.is_replica.load(Ordering::Acquire)
            && let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && !self.write_defers_to_cluster_redirect(cmd_name, args)
        {
            return Some(Response::error(
                "READONLY You can't write against a read only replica.",
            ));
        }

        // MISCONF: reject writes while the last background save failed, but only
        // when the operator asked for it (`snapshot.stop-writes-on-save-error`,
        // off by default). Redis runs the equivalent check
        // (`writeCommandsDeniedByDiskError`) immediately before its
        // `NOREPLICAS` gate and after its OOM gate, which is exactly here:
        // FrogDB's OOM refusal is shard-side (`check_memory_for_write`), so
        // everything in this function already precedes it. It sits *after* the
        // read-only-replica check rather than before it — FrogDB's ladder puts
        // READONLY first so a slot redirect can take precedence — which is also
        // what keeps a replica answering `-READONLY` (a client-actionable
        // reply) instead of `-MISCONF` about a backup it does not own.
        //
        // Replica *apply* traffic cannot reach this: the replication executor
        // sends `CoreMsg::Execute` straight to the shards under
        // `REPLICA_INTERNAL_CONN_ID` and never builds a `PreDispatchView`. A
        // primary whose saves are failing therefore refuses its clients without
        // stalling the replica it is feeding — the same carve-out Redis makes
        // with its "unless coming from our master" clause.
        //
        // Shares the two bounds documented on the `NOREPLICAS` gate below:
        // writes issued from inside a Lua script, and a MULTI queued while
        // healthy then EXEC'd after a save failed, are not gated here.
        if let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && self.config_manager.refuse_writes_on_save_error()
        {
            return Some(Response::error(SAVE_ERROR_REFUSAL));
        }

        // Self-fence: reject writes when quorum is lost. Two checkers reach this
        // rung — the cluster's Raft quorum (via `SelfFenceGate`) and the
        // replication replica-loss fence — so the *wording* comes from the
        // checker that refused (`quorum_lost_error`), not from here: a
        // standalone primary must not be told its cluster is down.
        if let Some(ref qc) = self.cluster.quorum_checker
            && let Some(cmd_impl) = self.registry.get_entry(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && !qc.has_quorum()
        {
            return Some(Response::error(qc.quorum_lost_error()));
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
        // which covers direct writes and MULTI *queue* time. The two producers
        // it cannot see — a Lua `redis.call` (EVAL lacks the WRITE flag) and a
        // MULTI queued while replicas are healthy then EXEC'd after they drop —
        // are gated on the shard instead, by `ShardWriteSeam::admit`
        // (`specs/txn.md` FM-TXN-051), against the `WriteAdmission` this
        // connection built at dispatch time. So both fences run twice for a
        // script or a transaction: here, and again where the write happens.
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

        // Block admin commands on regular port when admin port is enabled.
        // Container commands are gated per subcommand — `CLUSTER SLOTS` is a
        // client command, `CLUSTER SETSLOT` is not — so the surface is resolved
        // from the declarative table rather than the whole-command flag alone.
        if self.admin_enabled
            && !self.is_admin
            && let Some(cmd_info) = self.registry.get_entry(cmd_name)
            && admin_surface(cmd_name, cmd_info.flags())
                .requires_admin(extract_subcommand(cmd_name, args).as_deref())
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
        // Node-scoped commands (server-wide, non-scripting connection-level,
        // and CLUSTER/PING/…) are not slot-routed, so they are never
        // redirected. Scatter-gather and scripting commands fold their keys
        // and are slot-validated like any other keyed command.
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

    /// Resolve the command in the registry and validate its arity
    /// (`CommandLookup` stage). Returns `Some(Response)` with the
    /// unknown-command or wrong-arg-count error, `None` if the command exists
    /// and its argument count is valid.
    ///
    /// Both rejections are decided here — before any executor runs — so they
    /// are accounted as `rejected_calls`, matching Redis's `processCommand`,
    /// which rejects an unknown command (and a wrong arity) ahead of `call()`.
    /// Ordered *before* the pause gate so syntax errors bypass `CLIENT PAUSE`,
    /// and *after* the transaction-queuing stage so an unknown command inside a
    /// `MULTI` still aborts the transaction at queue time.
    pub(crate) fn command_lookup_check(
        &self,
        cmd_name: &str,
        original_name: &[u8],
        args: &[Bytes],
    ) -> Option<Response> {
        let Some(entry) = self.registry.get_entry(cmd_name) else {
            return Some(Response::error(format!(
                "ERR {}",
                frogdb_protocol::format_unknown_command_error(original_name, args)
            )));
        };
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
        //
        // The fence is discarded here on purpose: queuing executes nothing, so
        // there is no acknowledgement to fence. The batch is fenced as a unit at
        // `EXEC` (`validate_queued_batch`), against the topology that exists
        // then — which is the only generation a committed batch can be judged
        // against anyway.
        if let SlotVerdict::Reply(cluster_error) = self.validate_cluster_slots(cmd) {
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
                    "ERR {}",
                    frogdb_protocol::format_unknown_command_error(&cmd.name, &cmd.args)
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
        //
        // A denial here poisons the transaction, exactly like the unknown-command
        // and arity rejections above and like the `PreChecks` command-level
        // denial: Redis's `rejectCommand` flags *every* queue-time refusal with
        // `CLIENT_DIRTY_EXEC`, so EXEC answers `-EXECABORT` rather than
        // returning a short array (testing-gap issue 33).
        // (Scoped so the `PermissionGuard`'s borrow of `self` ends before the
        // abort below re-borrows it mutably.)
        let denial = match self.permission_guard() {
            None => None,
            Some(guard) => {
                let key_denial = if keys.is_empty() {
                    None
                } else {
                    // Per-key access (STORE-family: write dest, read sources), so
                    // a MULTI-queued command's denial matches direct dispatch
                    // exactly.
                    let keyed_flags = entry.keys_with_flags(&cmd.args);
                    let fallback = key_access_type_for_flags(entry.flags());
                    guard.check_keys_with_flags(&keyed_flags, fallback).err()
                };
                key_denial.or_else(|| match cmd_name_str.as_ref() {
                    // First arg is the channel.
                    "PUBLISH" | "SPUBLISH" => cmd.args.first().and_then(|channel| {
                        guard.check_channels(std::slice::from_ref(channel)).err()
                    }),
                    // All args are channels.
                    "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
                        guard.check_channels(&cmd.args).err()
                    }
                    _ => None,
                })
            }
        };
        if let Some(err) = denial {
            let msg = match &err {
                Response::Error(e) => Some(String::from_utf8_lossy(e).to_string()),
                _ => None,
            };
            self.state.abort_transaction(msg);
            return err;
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
    ///
    /// Exempt means **node-scoped**: the command's effect is not addressed by
    /// key, so no slot owns it and no redirect could name a node to send the
    /// client to. That is strictly narrower than "does not route to a single
    /// shard":
    ///
    /// - **Scatter-gather** commands (`MSET`/`MGET`/`DEL`/`EXISTS`/`TOUCH`/
    ///   `UNLINK`) *are* keyed. Every key hashes to a slot some node owns; the
    ///   fan-out is how they are served once they are ours to serve, not a
    ///   reason to serve them. Exempting them let a former slot owner answer
    ///   `MSET {t}a 1 {t}b 2` locally instead of `-MOVED`, which is the same
    ///   orphan-write shape the EXEC batch re-validation exists to prevent.
    /// - The **scripting** family (`EVAL`/`EVALSHA`/`EVAL_RO`/`FCALL`/…)
    ///   declares its keys through `numkeys`, surfaced by the registry's
    ///   `dynamic_keys` hook. A script with declared `KEYS` is slot-routed;
    ///   `SCRIPT LOAD` and friends declare none and fall out on the empty-key
    ///   check instead. Scripts are *dispatched* at the `ConnectionCommand`
    ///   stage, so this refusal only bites because `ClusterSlotValidation`
    ///   runs ahead of that stage — pinned structurally by the `MUST_PRECEDE`
    ///   pair `(ClusterSlotValidation, ConnectionCommand)` (FM-CLUSTER-030;
    ///   before that pair existed a bare `EVAL` ran to completion on a
    ///   non-owner and acked its writes there).
    ///
    /// What remains genuinely node-scoped: server-wide fan-outs (`SCAN`,
    /// `KEYS`, `DBSIZE`, `FLUSHDB`, `FT.*`, `MIGRATE`, …), the non-scripting
    /// connection-level families (pub/sub, transactions, admin, auth,
    /// connection state, replication, persistence), and the explicit name list.
    pub(crate) fn is_cluster_exempt(&self, cmd_name: &str) -> bool {
        // Certain commands are always exempt
        if matches!(cmd_name, "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG") {
            return true;
        }
        self.registry
            .get_entry(cmd_name)
            .is_some_and(|entry| match entry.execution_strategy() {
                ExecutionStrategy::ServerWide(_) => true,
                ExecutionStrategy::ConnectionLevel(op) => op != ConnectionLevelOp::Scripting,
                _ => false,
            })
    }

    /// Validate slot ownership for keys in cluster mode (`ClusterSlotValidation`
    /// stage). Consumes `take_asking` exactly once.
    ///
    /// [`SlotVerdict::Reply`] is the refusal (CROSSSLOT/MOVED/ASK/CLUSTERDOWN).
    /// [`SlotVerdict::Serve`] carries the [`SlotFence`] the execute seam must
    /// re-check before acknowledging — `Some` exactly when this node is the
    /// slot's *owner*, which is the only side of a handoff that can acknowledge
    /// a write it is about to stop owning. Everything else (standalone, keyless
    /// commands, cluster-exempt commands, the importing target, a READONLY
    /// replica read) answers `Serve(None)`.
    ///
    /// The routing decision and the fence are derived from **one** snapshot: two
    /// reads could straddle the very `PrepareSlotHandoff` the fence exists to
    /// catch, and would stamp a generation the verdict was never taken against.
    pub(crate) fn validate_cluster_slots(&mut self, cmd: &ParsedCommand) -> SlotVerdict {
        match self.validate_cluster_slots_inner(cmd) {
            Some(verdict) => verdict,
            // Not in cluster mode, or nothing slot-routable about this command.
            None => SlotVerdict::Serve(None),
        }
    }

    /// The cluster-mode body of [`Self::validate_cluster_slots`]; `None` for
    /// every "no decision to make" exit, which the caller renders as
    /// `Serve(None)`.
    fn validate_cluster_slots_inner(&mut self, cmd: &ParsedCommand) -> Option<SlotVerdict> {
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
            Err(crossslot) => return Some(SlotVerdict::Reply(crossslot)),
        };

        // ASKING is a one-shot flag consumed by routing. Read-and-clear up front;
        // the LocalServe arm restores it, preserving the historical quirk that a
        // command routed to a slot we fully own does not consume ASKING.
        let asking = self.state.take_asking();
        let snapshot = coordinator.snapshot();
        let decision = route_with_snapshot(&snapshot, first_slot, &cmd_name, asking, node_id);

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

        Some(match decision.to_response(readonly_eligible) {
            RouteOutcome::ServeLocal => {
                SlotVerdict::Serve(stamp_fence(&snapshot, first_slot, node_id))
            }
            RouteOutcome::Reply(resp) => SlotVerdict::Reply(resp),
        })
    }

    /// Slot-validate the keys a `WATCH` names, in cluster mode.
    ///
    /// `WATCH` is a keyed command with a `KeySpec::All` spec, but it is
    /// `ConnectionLevel(Transaction)` and so short-circuits at the
    /// `TransactionControl` dispatch stage — long before `ClusterSlotValidation`
    /// runs, which `is_cluster_exempt` would exempt it from anyway. Without this
    /// call a node that does not own the key's slot answers `+OK` and registers
    /// a CAS no writer on the real owner can ever dirty.
    ///
    /// Returns `Some(reply)` — the bare `-MOVED` / `-CLUSTERDOWN` /
    /// `-CROSSSLOT` — when the watch must be refused and nothing recorded.
    ///
    /// ASKING is **peeked, not consumed**: the one-shot flag belongs to the
    /// `MULTI`/`EXEC` block the client is setting up, which still needs it to
    /// reach the importing-target routing arm.
    pub(crate) fn validate_watch_slots(&self, keys: &[Bytes]) -> Option<Response> {
        // Gated on the same handles as the per-command and EXEC-time seams, so
        // no configuration can leave one validator live and another off.
        self.cluster.slot_migration.as_ref()?;
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;

        let mut batch = BatchKeys::default();
        for key in keys {
            batch.add_key(key);
        }
        route_watched_keys(
            &cluster_state.snapshot(),
            &batch,
            self.state.is_asking(),
            node_id,
        )
    }

    /// Whether every watched key's slot is still served by this node
    /// (EXEC-time, cluster mode only).
    ///
    /// `false` means the CAS precondition can no longer be evaluated here: the
    /// version the watch recorded is frozen on a slot whose real owner is now
    /// taking writes, so EXEC must fail the watch rather than commit against a
    /// stale local copy. The queue's own verdict
    /// ([`Self::validate_queued_batch`]) is taken first and outranks this.
    ///
    /// One snapshot backs the whole answer. `asking` is the block-scoped flag
    /// `take_transaction` captured for this EXEC — the connection's own copy has
    /// already been consumed by then, so it cannot be read here.
    pub(crate) fn watched_slots_still_local(&self, watches: &[WatchEntry], asking: bool) -> bool {
        // Standalone has no slot ownership to lose: nothing to check.
        self.check_watched_slots(watches, asking).unwrap_or(true)
    }

    /// The cluster-mode body of [`Self::watched_slots_still_local`]; `None` when
    /// this server is not in cluster mode. Gated on the same three handles as
    /// the per-command and EXEC-time batch seams.
    fn check_watched_slots(&self, watches: &[WatchEntry], asking: bool) -> Option<bool> {
        self.cluster.slot_migration.as_ref()?;
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;
        let snapshot = cluster_state.snapshot();
        Some(watches.iter().all(|watch| {
            watch_slot_is_locally_served(&snapshot, slot_for_key(&watch.key), asking, node_id)
        }))
    }

    /// FM-CLUSTER-028: on the MIGRATING source, decide by key *presence* before
    /// the command runs (`MigratingSourceProbe` stage).
    ///
    /// `MIGRATE` deletes each key as it hands it over, so owning the slot and
    /// holding the key are different questions for the whole migration window.
    /// Redis answers the second one in `getNodeByQuery` for every arity:
    ///
    /// - All keys present locally → serve locally (`None`)
    /// - All keys absent → `-ASK` at the importing node
    /// - Mixed presence → `-TRYAGAIN`
    ///
    /// Arity is **not** a gate. The former `keys.len() >= 2` gate left every
    /// single-key command to a post-execution nil-reply conversion, which
    /// answered `+OK` to a `SET` on an already-migrated key and re-created it
    /// behind the migration; `CLUSTER SETSLOT <slot> NODE` then destroyed the
    /// acknowledged write (issue 40).
    ///
    /// Cluster-exempt commands are skipped for the same reason
    /// [`Self::validate_cluster_slots`] skips them: no slot owns a node-scoped
    /// command, so no migration can redirect it.
    ///
    /// The routing half lives in
    /// [`route_migrating_source`](crate::slot_migration::route_migrating_source)
    /// and the probe itself in [`Self::probe_key_presence`], both shared with
    /// the EXEC-time batch validation ([`Self::validate_queued_batch`]) so the
    /// two paths cannot drift. The slot-state read comes first, so a command on
    /// a slot with no open migration costs one snapshot read and no keyspace
    /// lookup.
    pub(crate) async fn check_migrating_source(&self, cmd: &ParsedCommand) -> Option<Response> {
        // Only relevant in cluster mode
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;

        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);

        // Node-scoped commands are not addressed by key; no migration redirects
        // them (same exemption as the slot-validation stage).
        if self.is_cluster_exempt(&cmd_name) {
            return None;
        }

        // Get keys from command
        let keys = self.registry.get_entry(&cmd_name)?.keys(&cmd.args);
        // Keyless commands are never slot-routed, so nothing here can be
        // migrating away from us.
        let first = keys.first()?;
        // Cross-slot key sets were already refused by `ClusterSlotValidation`,
        // so the first key's slot is the whole command's slot.
        let slot = slot_for_key(first);

        // Are we the source of an open migration off this slot, with a target we
        // can name in an ASK? If not, there is nothing to probe.
        let snapshot = cluster_state.snapshot();
        let target_addr = route_migrating_source(&snapshot, slot, node_id)?;

        let keys_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::copy_from_slice(k)).collect();
        self.probe_key_presence(&keys_bytes)
            .await
            .migrating_source_reply(slot, target_addr)
    }

    /// Probe whether `keys` currently exist on this node, via one
    /// [`ScatterOp::Exists`] round-trip to the owning shard.
    ///
    /// All keys of a migrating slot hash to that slot and therefore to one
    /// shard, so a single shard is addressed. The caller decides what the
    /// verdict *means*: the source of a migration turns `AllAbsent` into `ASK`,
    /// the importing target never does.
    ///
    /// `keys` must be non-empty; an empty probe reports [`KeyPresence::AllPresent`]
    /// (nothing is missing), which is the "serve locally" answer on every caller.
    ///
    /// The probe **fails closed**: a shard that answers with anything other than
    /// `Integer(0|1)` per key, or answers about fewer keys than were asked
    /// about, yields [`KeyPresence::Unavailable`] rather than a presence
    /// verdict. Failing open here would mean "serve the batch on the migration
    /// source", which is exactly the orphan write this seam exists to prevent.
    pub(crate) async fn probe_key_presence(&self, keys: &[Bytes]) -> KeyPresence {
        let Some(first) = keys.first() else {
            return KeyPresence::AllPresent;
        };
        let shard_id = shard_for_key(first, self.num_shards);

        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ScatterRequest {
            request_id: next_txid(),
            keys: keys.to_vec(),
            operation: ScatterOp::Exists,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return KeyPresence::Unavailable;
        }

        let partial = match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => partial,
            _ => return KeyPresence::Unavailable,
        };

        let mut any_present = false;
        let mut any_absent = false;
        let mut answered = 0usize;
        for (_, response) in partial.keyed_slice() {
            answered += 1;
            match response {
                Response::Integer(1) => any_present = true,
                Response::Integer(0) => any_absent = true,
                // An EXISTS probe answers 0 or 1 per key. Anything else is a
                // shard error or a protocol change; either way we do not know
                // whether the key is here.
                _ => return KeyPresence::Unavailable,
            }
        }

        // A short answer means some key's verdict is missing.
        if answered < keys.len() {
            return KeyPresence::Unavailable;
        }

        match (any_present, any_absent) {
            (true, true) => KeyPresence::Mixed,
            (_, true) => KeyPresence::AllAbsent,
            _ => KeyPresence::AllPresent,
        }
    }

    /// Fold an open MULTI's queued commands into the keyed footprint that
    /// EXEC-time slot validation routes over.
    ///
    /// Mirrors the per-command seam exactly: cluster-exempt commands
    /// ([`Self::is_cluster_exempt`]) contribute nothing, every other command's
    /// keys come from the registry. The result is deliberately *not* the union
    /// of per-command verdicts — a whole-batch decision against one snapshot is
    /// the only way to avoid a torn verdict (see the EXEC re-validation PRD).
    pub(crate) fn fold_queued_batch(&self, queue: &[ParsedCommand]) -> QueuedBatch {
        let mut batch = BatchKeys::default();
        // Batch-level READONLY eligibility: the connection must be in READONLY
        // mode and no slot-routed command in the batch may be a write. Redis
        // folds transaction write-ness the same way
        // (`c->mstate.cmd_flags & CMD_WRITE` in `getNodeByQuery`), so a batch
        // containing a single write is never rescued onto a replica.
        let mut all_readonly = self.state.is_readonly();

        for cmd in queue {
            let name_bytes = cmd.name_uppercase();
            let name = String::from_utf8_lossy(&name_bytes);
            // Unknown commands never reach a queue that EXEC runs (queuing
            // rejects and aborts them); ignore rather than guess a slot.
            let Some(entry) = self.registry.get_entry(&name) else {
                continue;
            };
            let flags = entry.flags();

            // A write disqualifies the batch from replica service however it is
            // routed, so this is evaluated *before* any exemption. Structural
            // on purpose: an over-broad exemption then costs a redirect, never
            // a write served locally on a node that does not own the slot.
            if flags.contains(CommandFlags::WRITE) {
                all_readonly = false;
            }

            if self.is_cluster_exempt(&name) {
                continue;
            }

            // A slot-routed command that is not declared READONLY is treated as
            // a write for eligibility purposes (conservative; only reached for
            // non-exempt commands, so `PING` in a READONLY batch is unaffected).
            if !flags.contains(CommandFlags::READONLY) {
                all_readonly = false;
            }
            for key in entry.keys(&cmd.args) {
                batch.add_key(key);
            }
        }

        QueuedBatch {
            keys: batch,
            readonly_eligible: all_readonly,
        }
    }

    /// EXEC-time whole-batch slot re-validation.
    ///
    /// [`SlotVerdict::Reply`] is the bare `-MOVED` / `-ASK` / `-TRYAGAIN` /
    /// `-CROSSSLOT` / `-CLUSTERDOWN` that becomes EXEC's answer, with the queue
    /// already discarded by `take_transaction` (Redis: `discardTransaction`
    /// then `clusterRedirectClient`). [`SlotVerdict::Serve`] means "run the
    /// batch here", carrying the [`SlotFence`] the execute seam re-checks
    /// before the EXEC array reaches the client.
    ///
    /// The fence is stamped only for a batch that pins to exactly one slot this
    /// node owns. A batch straddling slots cannot be reduced to one generation,
    /// and the importing target is never fenced for the same reason a single
    /// command is not (see [`Self::validate_cluster_slots`]).
    ///
    /// Exactly one [`ClusterState::snapshot`] backs the whole decision, so a
    /// migration applying mid-validation cannot produce an internally
    /// inconsistent verdict. The residual window is Raft apply latency, which
    /// the non-transaction path shares — and which is what the fence closes.
    pub(crate) async fn validate_queued_batch(
        &self,
        queue: &[ParsedCommand],
        asking: bool,
    ) -> SlotVerdict {
        match self.validate_queued_batch_inner(queue, asking).await {
            Some(verdict) => verdict,
            None => SlotVerdict::Serve(None),
        }
    }

    /// The cluster-mode body of [`Self::validate_queued_batch`]; `None` when
    /// this server is not in cluster mode.
    async fn validate_queued_batch_inner(
        &self,
        queue: &[ParsedCommand],
        asking: bool,
    ) -> Option<SlotVerdict> {
        // Cluster mode only; standalone has no slot ownership to re-validate.
        //
        // Gated on the *same* handles as the per-command seam
        // ([`Self::validate_cluster_slots`] needs `slot_migration` + `node_id`)
        // plus the `cluster_state` this path snapshots. Deliberately identical
        // so no configuration can leave one of the two validators live and the
        // other silently off — the queue-time check passing while the EXEC-time
        // check is disabled is precisely the hole this seam closes.
        self.cluster.slot_migration.as_ref()?;
        let cluster_state = self.cluster.cluster_state.as_ref()?;
        let node_id = self.cluster.node_id?;

        let QueuedBatch {
            keys,
            readonly_eligible,
        } = self.fold_queued_batch(queue);

        let snapshot = cluster_state.snapshot();
        // The batch's fence, for whichever arm ends up serving locally. Derived
        // from the same snapshot as the routing verdict, exactly as the
        // per-command seam does.
        let serve_local = || {
            SlotVerdict::Serve(
                keys.single_slot()
                    .and_then(|slot| stamp_fence(&snapshot, slot, node_id)),
            )
        };
        Some(
            match route_queued_batch(&snapshot, &keys, asking, node_id, readonly_eligible) {
                BatchRoute::ServeLocal => serve_local(),
                BatchRoute::Redirect(reply) => SlotVerdict::Reply(reply),
                // We are the migration source: the presence of the batch's keys
                // decides. All present → the batch is still ours; all gone → ASK
                // the target; split → TRYAGAIN.
                BatchRoute::ProbeMigratingSource { slot, target } => {
                    match self
                        .probe_key_presence(keys.keys())
                        .await
                        .migrating_source_reply(slot, target)
                    {
                        Some(reply) => SlotVerdict::Reply(reply),
                        None => serve_local(),
                    }
                }
                // We are the importing target with ASKING set. Redis serves the
                // batch here unless it is multi-key with something still missing,
                // in which case neither side can satisfy it yet → TRYAGAIN. It
                // never ASKs back at the source (that would be a redirect loop).
                BatchRoute::ProbeImporting { .. } => {
                    if keys.keys().len() < 2 {
                        return Some(serve_local());
                    }
                    match self.probe_key_presence(keys.keys()).await {
                        KeyPresence::AllPresent => serve_local(),
                        KeyPresence::AllAbsent | KeyPresence::Mixed => {
                            SlotVerdict::Reply(redirect::tryagain())
                        }
                        KeyPresence::Unavailable => {
                            SlotVerdict::Reply(Response::error("ERR shard unavailable"))
                        }
                    }
                }
            },
        )
    }
}

/// Verdict of the [`ScatterOp::Exists`] presence probe over a migrating slot's
/// keys. The probe reports *facts*; each caller owns the redirect policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyPresence {
    /// Every probed key exists on this node.
    AllPresent,
    /// No probed key exists on this node (all already migrated away).
    AllAbsent,
    /// Some exist and some do not — the request straddles the open slot.
    Mixed,
    /// The shard did not answer (send failure or scatter timeout).
    Unavailable,
}

impl KeyPresence {
    /// FM-CLUSTER-028: the MIGRATING source's reply for this verdict — the one
    /// place the presence→redirect policy is written.
    ///
    /// `None` means "serve it here". Both callers route through this: the
    /// per-command [`PreDispatchView::check_migrating_source`] stage and the
    /// EXEC-time [`PreDispatchView::validate_queued_batch`], so a single-key
    /// `SET`, a `MSET`, and the same commands inside a `MULTI` cannot answer
    /// the same key set differently.
    ///
    /// The importing *target* deliberately does not use this: it never `ASK`s
    /// back at the source (that is a redirect loop), so its arm keeps its own
    /// mapping.
    fn migrating_source_reply(self, slot: u16, target: SocketAddr) -> Option<Response> {
        match self {
            KeyPresence::AllPresent => None,
            // Every key has already been handed over — send the client after it.
            KeyPresence::AllAbsent => Some(redirect::ask(slot, target)),
            // The request straddles the open slot; neither side can serve it.
            KeyPresence::Mixed => Some(redirect::tryagain()),
            // Fail closed: not knowing means not serving. Serving here is the
            // orphan write this seam exists to prevent.
            KeyPresence::Unavailable => Some(Response::error("ERR shard unavailable")),
        }
    }
}

/// The keyed footprint of a queued MULTI, plus the batch-level READONLY
/// eligibility, as folded by [`PreDispatchView::fold_queued_batch`].
pub(crate) struct QueuedBatch {
    /// Distinct slots and the union of keys the batch touches.
    pub(crate) keys: BatchKeys,
    /// `true` iff the connection is READONLY *and* no slot-routed command in
    /// the batch is a write, so a foreign-owned slot may be served locally.
    pub(crate) readonly_eligible: bool,
}

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_core::command::QuorumChecker;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    struct MockQuorumChecker {
        has_quorum: bool,
    }

    impl QuorumChecker for MockQuorumChecker {
        fn has_quorum(&self) -> bool {
            self.has_quorum
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

    // FM-REPLICATION-046 FM-REPLICATION-042
    /// The `min-replicas-to-write` gate rejects writes once its only replica
    /// stops ACKing, and keeps rejecting them after the freshness window has
    /// been round-tripped through CONFIG.
    ///
    /// Two things are load-bearing here and neither had a test. First, the gate
    /// counts *good* replicas, not attached ones: a session that is still in
    /// `Streaming` but silent past the window must not satisfy the quorum.
    /// Second, `CONFIG GET min-replicas-max-lag` followed by `CONFIG SET` of the
    /// reported value — what any config-management tool does on every
    /// reconciliation — must leave that filter armed. The seconds view used to
    /// truncate a sub-second window to `0`, and `0` is the disable sentinel, so
    /// the round trip silently degraded the gate into a mere "is anything still
    /// attached" count and the silent replica was counted as good again.
    #[test]
    fn noreplicas_still_fires_after_a_replica_goes_silent() {
        let mut fx = ViewFixture::new(None);
        let tracker = Arc::new(frogdb_core::ReplicationTrackerImpl::new());
        let session = tracker.register_replica("127.0.0.1:6380".parse().unwrap());
        session.force_phase_for_test(frogdb_core::replication::Phase::Streaming);
        fx.cluster.replication_tracker = Some(Arc::clone(&tracker));

        fx.config_manager.set("min-replicas-to-write", "1").unwrap();
        fx.config_manager
            .set("min-replicas-max-lag-ms", "500")
            .unwrap();

        // A freshly ACKing replica satisfies the gate.
        assert!(
            fx.view().run_pre_checks("SET", &[]).is_none(),
            "a fresh streaming replica must satisfy min-replicas-to-write"
        );

        // The replica goes silent for an hour without disconnecting.
        session.backdate_last_ack_for_test(Duration::from_secs(3600));
        let expect_noreplicas =
            |fx: &mut ViewFixture, when: &str| match fx.view().run_pre_checks("SET", &[]) {
                Some(Response::Error(msg)) => assert!(
                    msg.starts_with(b"NOREPLICAS"),
                    "expected NOREPLICAS {when}, got: {}",
                    String::from_utf8_lossy(&msg)
                ),
                other => panic!("expected NOREPLICAS {when}, got: {other:?}"),
            };
        expect_noreplicas(&mut fx, "once the replica goes silent");

        // Reads are unaffected — the gate is write-only.
        assert!(fx.view().run_pre_checks("GET", &[]).is_none());

        // The CONFIG round trip on Redis's seconds-valued spelling.
        let reported = fx
            .config_manager
            .get("min-replicas-max-lag")
            .into_iter()
            .find(|(name, _)| name == "min-replicas-max-lag")
            .expect("min-replicas-max-lag must be a live CONFIG parameter")
            .1;
        assert_ne!(reported, "0", "a 500ms window must not report as disabled");
        fx.config_manager
            .set("min-replicas-max-lag", &reported)
            .unwrap();
        expect_noreplicas(&mut fx, "after a CONFIG GET/SET round trip");

        // And the disable is still reachable when an operator asks for it.
        fx.config_manager.set("min-replicas-max-lag", "0").unwrap();
        assert!(
            fx.view().run_pre_checks("SET", &[]).is_none(),
            "`min-replicas-max-lag 0` disables the freshness filter, so the \
             silent replica counts as good again"
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
    fn test_command_lookup_check_rejects_wrong_argument_count() {
        let mut fx = ViewFixture::new(None);
        // GET takes exactly one argument; zero args is an arity error.
        match fx.view().command_lookup_check("GET", b"GET", &[]) {
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
                .command_lookup_check("GET", b"GET", &[Bytes::from_static(b"k")])
                .is_none()
        );
    }

    /// An unrecognized command name is rejected by the same guard, so the
    /// error is decided pre-dispatch (a `rejected_call`) rather than falling
    /// through to the terminal `Execute` stage (a `failed_call`).
    #[test]
    fn test_command_lookup_check_rejects_unknown_command() {
        let mut fx = ViewFixture::new(None);
        match fx
            .view()
            .command_lookup_check("ASDFNOTACOMMAND", b"ASDFNOTACOMMAND", &[])
        {
            Some(Response::Error(msg)) => assert!(
                msg.starts_with(b"ERR unknown command 'ASDFNOTACOMMAND'"),
                "got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected unknown-command error, got: {other:?}"),
        }
    }

    /// The unknown-command error echoes the client's original-case spelling
    /// (not the uppercase lookup key) and lists the offending args, matching
    /// Redis's `commandCheckExistence` byte-for-byte (verified against a
    /// locally built Redis 8.6.1).
    #[test]
    fn test_command_lookup_check_unknown_command_preserves_case_and_lists_args() {
        let mut fx = ViewFixture::new(None);
        let args = [Bytes::from_static(b"bar"), Bytes::from_static(b"baz")];
        match fx
            .view()
            .command_lookup_check("NOTACOMMAND", b"notacommand", &args)
        {
            Some(Response::Error(msg)) => assert_eq!(
                &msg[..],
                b"ERR unknown command 'notacommand', with args beginning with: 'bar' 'baz' "
                    as &[u8],
                "got: {}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("expected unknown-command error, got: {other:?}"),
        }
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

    // ------------------------------------------------------------------
    // FM-CLUSTER-028 — the MIGRATING-source presence probe
    // ------------------------------------------------------------------

    const SOURCE_NODE: u64 = 1;
    const TARGET_NODE: u64 = 2;

    /// Two keys sharing one hash tag, so both land in the same (migrating) slot
    /// and `ClusterSlotValidation` would never have refused them as CROSSSLOT.
    const MIG_KEY_A: &[u8] = b"{mig}a";
    const MIG_KEY_B: &[u8] = b"{mig}b";

    fn importing_addr() -> SocketAddr {
        "127.0.0.1:7002".parse().unwrap()
    }

    fn cmd(name: &'static str, args: &[&[u8]]) -> ParsedCommand {
        ParsedCommand::new(
            Bytes::from_static(name.as_bytes()),
            args.iter().map(|a| Bytes::copy_from_slice(a)).collect(),
        )
    }

    /// The wire text of a probe verdict, for exact-shape assertions.
    fn reply_text(reply: &Option<Response>) -> String {
        match reply {
            None => "<serve locally>".to_string(),
            Some(Response::Error(msg)) => String::from_utf8_lossy(msg).to_string(),
            Some(other) => format!("{other:?}"),
        }
    }

    impl ViewFixture {
        /// Make this node the owner of `MIG_KEY_A`'s slot. `migrating` also
        /// opens a migration off that slot towards [`importing_addr`].
        fn owns_migrating_slot(&mut self, migrating: bool) -> u16 {
            let slot = slot_for_key(MIG_KEY_A);
            let mut snap = frogdb_cluster::types::ClusterSnapshot::new();
            snap.nodes.insert(
                SOURCE_NODE,
                frogdb_cluster::types::NodeInfo::new_primary(
                    SOURCE_NODE,
                    "127.0.0.1:7001".parse().unwrap(),
                    "127.0.0.1:17001".parse().unwrap(),
                ),
            );
            snap.nodes.insert(
                TARGET_NODE,
                frogdb_cluster::types::NodeInfo::new_primary(
                    TARGET_NODE,
                    importing_addr(),
                    "127.0.0.1:17002".parse().unwrap(),
                ),
            );
            snap.slot_assignment.insert(slot, SOURCE_NODE);
            if migrating {
                snap.migrations.insert(
                    slot,
                    frogdb_cluster::types::SlotMigration::new(slot, SOURCE_NODE, TARGET_NODE),
                );
            }
            self.cluster.cluster_state = Some(Arc::new(frogdb_core::ClusterState::from_snapshot(
                snap,
                Arc::new(AtomicU64::new(SOURCE_NODE)),
            )));
            self.cluster.node_id = Some(SOURCE_NODE);
            slot
        }

        /// Install a fake shard that answers the `EXISTS` presence probe:
        /// `present` keys report `1`, every other probed key reports `0`.
        ///
        /// The returned counter is how many probes actually reached a shard, so
        /// `0` is a positive proof that no keyspace lookup happened — the
        /// property that keeps this stage off the non-migrating hot path.
        fn probe_shard(&mut self, present: &[&'static [u8]]) -> Arc<AtomicUsize> {
            let probes = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&probes);
            let present: Vec<Bytes> = present.iter().map(|k| Bytes::from_static(k)).collect();
            let (tx, mut rx) = tokio::sync::mpsc::channel::<frogdb_core::Envelope>(16);
            tokio::spawn(async move {
                while let Some(envelope) = rx.recv().await {
                    let frogdb_core::ShardMessage::Core(CoreMsg::ScatterRequest {
                        keys,
                        operation,
                        response_tx,
                        ..
                    }) = envelope.message
                    else {
                        continue;
                    };
                    assert!(
                        matches!(operation, ScatterOp::Exists),
                        "the migrating-source probe must ask EXISTS, got {operation:?}"
                    );
                    counter.fetch_add(1, Ordering::Relaxed);
                    let results = keys
                        .into_iter()
                        .map(|k| {
                            let here = present.contains(&k);
                            (k, Response::Integer(i64::from(here)))
                        })
                        .collect();
                    let _ = response_tx.send(frogdb_core::PartialResult::keyed(results));
                }
            });
            self.shard_senders = vec![ShardSender::new(tx)];
            probes
        }
    }

    // FM-CLUSTER-028
    /// The bug issue 40 was filed for: a **single-key write** whose key the
    /// migration has already handed over must be `-ASK`ed, not acked here.
    ///
    /// Answering `+OK` re-created the key behind the migration and
    /// `CLUSTER SETSLOT <slot> NODE` then destroyed the acknowledged write. The
    /// arity gate that caused it is gone, so `SET`/`INCR`/`DEL`/`EXPIRE` — none
    /// of which ever reply nil, so none of which the old post-execution
    /// nil-to-ASK conversion could ever have caught — all take the probe.
    #[tokio::test]
    async fn migrating_source_asks_a_single_key_write_whose_key_moved() {
        let mut fx = ViewFixture::new(None);
        let slot = fx.owns_migrating_slot(true);
        // Nothing is present: the whole slot has been handed over.
        let probes = fx.probe_shard(&[]);

        let expected = format!("ASK {slot} {}", importing_addr());
        for command in [
            cmd("SET", &[MIG_KEY_A, b"v"]),
            cmd("INCR", &[MIG_KEY_A]),
            cmd("DEL", &[MIG_KEY_A]),
            cmd("EXPIRE", &[MIG_KEY_A, b"10"]),
            // …and the read the old hack did catch, still redirected.
            cmd("GET", &[MIG_KEY_A]),
        ] {
            let reply = fx.view().check_migrating_source(&command).await;
            assert_eq!(
                reply_text(&reply),
                expected,
                "{} on an already-migrated key must ASK the importing node",
                String::from_utf8_lossy(&command.name)
            );
        }
        assert_eq!(probes.load(Ordering::Relaxed), 5);
    }

    // FM-CLUSTER-028
    /// The other half of the contract: a key the migration has not reached yet
    /// is served here, whatever the command would reply.
    ///
    /// `HGET` on a missing field, `LPOS` on a value that is not in the list and
    /// `GET` on a key that exists all pass through. The first two are why the
    /// retired reply-side hack was unsound in the *other* direction: they reply
    /// nil while the key is very much still here, so it converted them into
    /// spurious `ASK`s that sent the client to a node holding nothing.
    #[tokio::test]
    async fn migrating_source_serves_a_single_key_command_still_held_here() {
        let mut fx = ViewFixture::new(None);
        fx.owns_migrating_slot(true);
        let probes = fx.probe_shard(&[MIG_KEY_A]);

        for command in [
            cmd("GET", &[MIG_KEY_A]),
            cmd("SET", &[MIG_KEY_A, b"v"]),
            cmd("HGET", &[MIG_KEY_A, b"nosuchfield"]),
            cmd("LPOS", &[MIG_KEY_A, b"nosuchvalue"]),
        ] {
            let reply = fx.view().check_migrating_source(&command).await;
            assert_eq!(
                reply_text(&reply),
                "<serve locally>",
                "{} must be served where the key still lives",
                String::from_utf8_lossy(&command.name)
            );
        }
        assert_eq!(probes.load(Ordering::Relaxed), 4);
    }

    // FM-CLUSTER-028
    /// A multi-key command straddling the open slot — one key handed over, one
    /// still here — is `-TRYAGAIN`: neither node can satisfy it yet. Unchanged
    /// by this fix, pinned so dropping the arity gate did not disturb it.
    #[tokio::test]
    async fn migrating_source_tryagain_when_the_keys_straddle_the_slot() {
        let mut fx = ViewFixture::new(None);
        fx.owns_migrating_slot(true);
        let probes = fx.probe_shard(&[MIG_KEY_A]);

        for command in [
            cmd("MGET", &[MIG_KEY_A, MIG_KEY_B]),
            cmd("MSET", &[MIG_KEY_A, b"1", MIG_KEY_B, b"2"]),
        ] {
            let reply = fx.view().check_migrating_source(&command).await;
            assert!(
                reply_text(&reply).starts_with("TRYAGAIN"),
                "{} across a half-migrated slot must TRYAGAIN, got {}",
                String::from_utf8_lossy(&command.name),
                reply_text(&reply)
            );
        }
        assert_eq!(probes.load(Ordering::Relaxed), 2);
    }

    // FM-CLUSTER-028
    /// The scripting family is probed too. `EVAL` declares its keys through
    /// `numkeys` and short-circuits at the `ConnectionCommand` stage, so it
    /// only reaches the probe because `MigratingSourceProbe` is ordered ahead
    /// of that stage (the `MUST_PRECEDE` pair) and `is_cluster_exempt` refuses
    /// to exempt `Scripting` — the same structural fix FM-CLUSTER-030 made for
    /// slot validation.
    #[tokio::test]
    async fn migrating_source_probes_the_scripting_family() {
        let mut fx = ViewFixture::new(None);
        let slot = fx.owns_migrating_slot(true);
        let probes = fx.probe_shard(&[]);

        let command = cmd(
            "EVAL",
            &[b"return redis.call('set', KEYS[1], 1)", b"1", MIG_KEY_A],
        );
        let reply = fx.view().check_migrating_source(&command).await;
        assert_eq!(
            reply_text(&reply),
            format!("ASK {slot} {}", importing_addr()),
            "a script writing an already-migrated key must be redirected, not run here"
        );
        assert_eq!(probes.load(Ordering::Relaxed), 1);
    }

    // FM-CLUSTER-028
    /// A slot with no open migration costs one snapshot read and **no** keyspace
    /// lookup: the probe reads slot state first and returns before it would
    /// address a shard. `0` probes is the assertion that keeps this stage off
    /// the hot path.
    #[tokio::test]
    async fn migrating_source_probe_is_skipped_when_the_slot_is_not_migrating() {
        let mut fx = ViewFixture::new(None);
        fx.owns_migrating_slot(false);
        let probes = fx.probe_shard(&[]);

        assert!(
            fx.view()
                .check_migrating_source(&cmd("SET", &[MIG_KEY_A, b"v"]))
                .await
                .is_none()
        );
        assert_eq!(
            probes.load(Ordering::Relaxed),
            0,
            "a slot with no open migration must not cost a keyspace lookup"
        );

        // Standalone mode (no cluster state at all) is the same no-op.
        let mut standalone = ViewFixture::new(None);
        let probes = standalone.probe_shard(&[]);
        assert!(
            standalone
                .view()
                .check_migrating_source(&cmd("SET", &[MIG_KEY_A, b"v"]))
                .await
                .is_none()
        );
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    // FM-CLUSTER-028
    /// Node-scoped commands are never probed — no slot owns them, so no
    /// migration can redirect them. `WATCH` is the interesting one: it is a
    /// keyed command whose keys hash into the migrating slot, and exempting it
    /// here is deliberate (its own slot check is
    /// [`PreDispatchView::validate_watch_slots`], run at the
    /// `TransactionControl` stage).
    #[tokio::test]
    async fn migrating_source_probe_is_skipped_for_node_scoped_commands() {
        let mut fx = ViewFixture::new(None);
        fx.owns_migrating_slot(true);
        let probes = fx.probe_shard(&[]);

        for command in [
            cmd("WATCH", &[MIG_KEY_A]),
            cmd("CLUSTER", &[b"INFO"]),
            cmd("SCAN", &[b"0"]),
            cmd("DBSIZE", &[]),
            cmd("PING", &[]),
        ] {
            let reply = fx.view().check_migrating_source(&command).await;
            assert!(
                reply.is_none(),
                "{} is node-scoped and must not be redirected by a migration",
                String::from_utf8_lossy(&command.name)
            );
        }
        assert_eq!(
            probes.load(Ordering::Relaxed),
            0,
            "node-scoped commands must not cost a keyspace lookup"
        );
    }

    // FM-CLUSTER-028
    /// The probe fails closed. A shard that cannot answer leaves us unable to
    /// tell whether the key is still here, and serving it in that state is
    /// exactly the orphan write this seam exists to prevent — so the command is
    /// refused instead.
    #[tokio::test]
    async fn migrating_source_refuses_when_the_shard_does_not_answer() {
        let mut fx = ViewFixture::new(None);
        fx.owns_migrating_slot(true);
        // A sender whose receiver is already gone: the probe's send fails.
        let (tx, rx) = tokio::sync::mpsc::channel::<frogdb_core::Envelope>(1);
        drop(rx);
        fx.shard_senders = vec![ShardSender::new(tx)];

        let reply = fx
            .view()
            .check_migrating_source(&cmd("SET", &[MIG_KEY_A, b"v"]))
            .await;
        assert_eq!(reply_text(&reply), "ERR shard unavailable");
    }
}
