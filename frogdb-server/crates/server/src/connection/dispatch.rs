//! Command dispatch and pipeline logic.
//!
//! This module handles routing parsed commands to their connection-level handlers
//! and executing the main command pipeline (transaction handling, pub/sub mode,
//! cluster validation, etc.).

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{ConnectionLevelOp, ExecutionStrategy, ServerWideOp};
use frogdb_protocol::Response;
use tracing::Instrument;

use crate::connection::ConnectionHandler;
use crate::connection::conn_command::ConnectionCommand;

impl ConnectionHandler {
    /// Dispatch a command registered as [`frogdb_core::CommandImpl::Connection`]
    /// through its connection-level executor, returning `Some(responses)` if it
    /// was handled. Returns `None` for any command that is not a migrated
    /// connection command, so unmigrated groups fall through to the legacy
    /// router→handler path.
    ///
    /// The `Connection` variant holds a `&'static dyn ConnectionCommand`, so the
    /// executor reference outlives the transient registry borrow and does not
    /// conflict with borrowing `self` again to build the [`ConnCtx`].
    async fn dispatch_connection_command(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        let command = self.core.registry.get_entry(cmd_name)?.as_connection()?;
        // Pub/sub (SUBSCRIBE/…/PUBSUB) emits one reply per channel and needs the
        // connection-local pub/sub machinery (subscription set + lazy channel +
        // cluster routing + scatter-gather introspection), so it dispatches
        // through the multi-response seam (`execute_multi`) over a dedicated
        // `ConnCtx::pubsub` view. `command` is `'static`, so reading its spec
        // does not conflict with re-borrowing `self` to build that view.
        if matches!(
            command.spec().strategy,
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
        ) {
            return Some(self.execute_pubsub(command, args).await);
        }
        // CLIENT mutates per-connection state (name/reply/tracking/caching) and
        // drives tracking IO, so it dispatches through the mutable builder that
        // populates `conn_state` and `tracking`. All other connection commands
        // are pure reads and use the shared `conn_ctx`. `as_connection()` yields
        // a `'static` reference, so it does not conflict with re-borrowing `self`.
        if cmd_name == "CLIENT" {
            return Some(vec![
                command.execute(&mut self.conn_ctx_clientmut(), args).await,
            ]);
        }
        // MONITOR registers the connection as a monitor: it mutates the
        // connection-local monitor receiver and reads the broadcaster, so it
        // dispatches through the dedicated builder that populates
        // `ConnCtx::monitor`. `command` is `'static`, so it does not conflict
        // with re-borrowing `self` to build that view.
        if cmd_name == "MONITOR" {
            return Some(vec![self.execute_monitor(command, args).await]);
        }
        Some(vec![command.execute(&mut self.conn_ctx(), args).await])
    }

    /// Execute a server-wide command by its typed op. The exhaustive match is
    /// the single dispatch point.
    ///
    /// Adding a new server-wide command is two steps: (1) add a [`ServerWideOp`]
    /// variant, and (2) declare `ExecutionStrategy::ServerWide(ServerWideOp::…)`
    /// on the command spec. The compiler then forces a match arm here (a missing
    /// arm is a compile error), so the name-keyed table plus drift tests this
    /// replaced are gone.
    async fn dispatch_server_wide(&mut self, op: ServerWideOp, args: &[Bytes]) -> Response {
        match op {
            ServerWideOp::Scan => self.handle_scan(args).await,
            ServerWideOp::Keys => self.handle_keys(args).await,
            ServerWideOp::DbSize => self.handle_dbsize().await,
            ServerWideOp::RandomKey => self.handle_randomkey().await,
            ServerWideOp::FlushDb => self.handle_flushdb(args).await,
            ServerWideOp::FlushAll => self.handle_flushall(args).await,
            ServerWideOp::Migrate => self.handle_migrate(args).await,
            ServerWideOp::Shutdown => self.handle_shutdown(args).await,
            ServerWideOp::TsQueryIndex => self.handle_ts_queryindex(args).await,
            ServerWideOp::TsMGet => self.handle_ts_mget(args).await,
            ServerWideOp::TsMRange => self.handle_ts_mrange(args, false).await,
            ServerWideOp::TsMRevRange => self.handle_ts_mrange(args, true).await,
            ServerWideOp::FtCreate => self.handle_ft_create(args).await,
            ServerWideOp::FtSearch => self.handle_ft_search(args).await,
            ServerWideOp::FtDropIndex => self.handle_ft_dropindex(args).await,
            ServerWideOp::FtInfo => self.handle_ft_info(args).await,
            ServerWideOp::FtList => self.handle_ft_list(args).await,
            ServerWideOp::FtAlter => self.handle_ft_alter(args).await,
            ServerWideOp::FtSynUpdate => self.handle_ft_synupdate(args).await,
            ServerWideOp::FtSynDump => self.handle_ft_syndump(args).await,
            ServerWideOp::FtAggregate => self.handle_ft_aggregate(args).await,
            ServerWideOp::FtHybrid => self.handle_ft_hybrid(args).await,
            ServerWideOp::FtAliasAdd => self.handle_ft_aliasadd(args).await,
            ServerWideOp::FtAliasDel => self.handle_ft_aliasdel(args).await,
            ServerWideOp::FtAliasUpdate => self.handle_ft_aliasupdate(args).await,
            ServerWideOp::FtTagVals => self.handle_ft_tagvals(args).await,
            ServerWideOp::FtDictAdd => self.handle_ft_dictadd(args).await,
            ServerWideOp::FtDictDel => self.handle_ft_dictdel(args).await,
            ServerWideOp::FtDictDump => self.handle_ft_dictdump(args).await,
            ServerWideOp::FtConfig => self.handle_ft_config(args).await,
            ServerWideOp::FtSpellCheck => self.handle_ft_spellcheck(args).await,
            ServerWideOp::FtExplain => self.handle_ft_explain(args, false).await,
            ServerWideOp::FtExplainCli => self.handle_ft_explain(args, true).await,
            ServerWideOp::FtProfile => self.handle_ft_profile(args).await,
            ServerWideOp::EsAll => self.handle_es_all(args).await,
        }
    }

    /// Handle internal action signals returned by commands.
    ///
    /// Some commands return special Response variants that signal the connection
    /// handler to perform async operations (blocking waits, Raft consensus, migration).
    async fn handle_internal_action(&mut self, response: Response) -> Response {
        match response {
            Response::BlockingNeeded { keys, timeout, op } => {
                self.handle_blocking_wait(keys, timeout, op).await
            }
            Response::RaftNeeded {
                op,
                register_node,
                unregister_node,
            } => {
                self.handle_raft_command(op, register_node, unregister_node)
                    .await
            }
            Response::MigrateNeeded { args } => self.handle_migrate_command(args).await,
            Response::SlotMigrationNeeded { kind } => self.handle_slot_migration(kind).await,
            other => other,
        }
    }

    /// PING has bespoke framing while subscribed, so it cannot use the standard
    /// shard PING path: in RESP2 a subscribed PING replies with an array
    /// `["pong", <message>]`; in RESP3 it replies with the simple `PONG` (or the
    /// message argument). Returns `Some` only for PING; every other command
    /// (including RESET/ASKING/READONLY/READWRITE, now migrated behind the
    /// ConnCtx seam) returns `None` and continues down the normal dispatch flow.
    fn pubsub_mode_ping(&self, cmd_name: &str, args: &[Bytes]) -> Option<Vec<Response>> {
        if cmd_name != "PING" {
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

    /// Dispatch a connection-state command (ASKING/READONLY/READWRITE) migrated
    /// behind the ConnCtx seam. Unlike the read-only registry-union
    /// [`dispatch_connection_command`](Self::dispatch_connection_command), these
    /// **mutate** per-connection state, so they dispatch through the *mutable*
    /// [`conn_ctx_authmut`](Self::conn_ctx_authmut) view (`conn_state = Some`).
    ///
    /// Scoped to the `ConnectionState` strategy so it only claims these commands
    /// and leaves the read-only migrated connection commands (CONFIG, INFO, ...)
    /// to the read-only union below. RESET is *not* handled here — it needs a
    /// handler-local teardown the seam cannot reach and is intercepted earlier
    /// via [`execute_reset`](Self::execute_reset).
    async fn dispatch_connection_state_command(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> Option<Vec<Response>> {
        let entry = self.core.registry.get_entry(cmd_name)?;
        if !matches!(
            entry.execution_strategy(),
            ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
        ) {
            return None;
        }
        let command = entry.as_connection()?;
        Some(vec![
            command.execute(&mut self.conn_ctx_authmut(), args).await,
        ])
    }

    /// Execute RESET (migrated behind the ConnCtx seam) plus the handler-local
    /// teardown the seam cannot reach.
    ///
    /// The [`RESET_CONN_COMMAND`](crate::connection::connection_state_conn_command)
    /// executor resets the connection state (via `ConnStateMut::reset`), fans out
    /// the shard `ConnectionClosed` notification, and clears the registry name.
    /// The two remaining steps — dropping the MONITOR subscription and tearing
    /// down the tracking session's local plumbing (invalidation channels +
    /// redirect forwarder) — touch `ConnectionHandler` fields absent from the
    /// `ConnCtx`, so they are done here, matching the old `handle_reset`.
    pub(crate) async fn execute_reset(&mut self, args: &[Bytes]) -> Response {
        let response = crate::connection::connection_state_conn_command::RESET_CONN_COMMAND
            .execute(&mut self.conn_ctx_authmut(), args)
            .await;
        self.tracking_session_teardown_local();
        self.monitor_rx = None;
        response
    }

    /// Route and execute a command, handling transaction and pub/sub modes.
    /// Returns a Vec of responses since pub/sub commands can return multiple messages.
    ///
    /// `cmd_name` is the precomputed uppercase command name to avoid redundant allocations.
    pub(crate) async fn route_and_execute_with_transaction(
        &mut self,
        cmd: &Arc<frogdb_protocol::ParsedCommand>,
        cmd_name: &str,
    ) -> Vec<Response> {
        // AUTH and HELLO run *before* authentication is enforced: a
        // not-yet-authenticated client must be able to authenticate / negotiate
        // the protocol. They are migrated behind the ConnCtx seam (registered as
        // CommandImpl::Connection executors) but intercepted here — before the
        // NOAUTH pre-check and before transaction queuing — and dispatched
        // through the *mutable* `conn_ctx_authmut` view (they change auth /
        // protocol state). This preserves the historical pre-auth ordering.
        if cmd_name == "AUTH" || cmd_name == "HELLO" {
            let command = self
                .core
                .registry
                .get_entry(cmd_name)
                .and_then(|entry| entry.as_connection())
                .expect("AUTH/HELLO are registered as connection commands");
            return vec![
                command
                    .execute(&mut self.conn_ctx_authmut(), &cmd.args)
                    .await,
            ];
        }

        // ACL is migrated behind the ConnCtx seam: after pre-checks it dispatches
        // through the registry union (`dispatch_connection_command`) like CONFIG,
        // rather than being intercepted early here.

        // Run pre-execution checks (auth, admin port, ACL, pub/sub mode)
        if let Some(error_response) = self.run_pre_checks(cmd_name, &cmd.args) {
            self.record_error_response(&error_response, true, cmd_name);
            return vec![error_response];
        }

        // RESET is migrated behind the ConnCtx seam. It performs a full reset of
        // the connection's server-side context and must dispatch directly —
        // never queued in MULTI, never blocked by pause — in both normal and
        // pub/sub mode. It is intercepted here (after pre-checks, like the old
        // direct-dispatch path) and dispatched through the mutable seam plus the
        // handler-local teardown the seam cannot reach.
        if cmd_name == "RESET" {
            return vec![self.execute_reset(&cmd.args).await];
        }

        // In pub/sub mode, PING needs bespoke framing (RESP2 array ["pong",
        // <message>]); it is registered as a Standard (shard) command so it
        // cannot use the normal shard PING path. Every other command allowed in
        // pub/sub mode (SUBSCRIBE/…, and the migrated ASKING/READONLY/READWRITE)
        // continues down the normal dispatch flow below.
        if self.state.in_pubsub_mode()
            && let Some(responses) = self.pubsub_mode_ping(cmd_name, &cmd.args)
        {
            return responses;
        }

        // Transaction control commands (MULTI/EXEC/DISCARD/WATCH/UNWATCH),
        // migrated behind the ConnCtx seam, are always dispatched directly, never
        // queued or blocked by pause. (RESET was in this set but is intercepted
        // above.) MULTI/DISCARD/WATCH/UNWATCH run their `CommandImpl::Connection`
        // executors over the mutable ConnCtx; EXEC's orchestration stays in
        // `handle_exec` (see `dispatch_transaction_command`).
        if let Some(responses) = self.dispatch_transaction_command(cmd_name, &cmd.args).await {
            return responses;
        }

        // If in transaction mode, queue the command instead of executing.
        // This must happen BEFORE connection-level dispatch so that commands like
        // CLIENT PAUSE, EVAL, etc. are queued during MULTI (not executed immediately).
        // Blocking commands (BLPOP, BRPOP, etc.) are also queued — at EXEC time they
        // execute with timeout=0 (non-blocking), matching Redis semantics.
        if self.state.in_transaction() {
            // Validate cluster slot ownership before queuing — commands that would
            // get MOVED should fail immediately rather than succeeding at EXEC time.
            if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
                let error_msg = match &cluster_error {
                    Response::Error(e) => Some(String::from_utf8_lossy(e).to_string()),
                    _ => None,
                };
                self.state.abort_transaction(error_msg);
                return vec![cluster_error];
            }
            return vec![self.queue_command(cmd)];
        }

        // Validate arity BEFORE the pause check so that commands with wrong
        // argument counts return an immediate error even when paused (test:
        // syntax errors bypass pause).
        if let Some(entry) = self.core.registry.get_entry(cmd_name)
            && !entry.arity().check(cmd.args.len())
        {
            let err = Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                entry.name().to_ascii_lowercase()
            ));
            self.record_error_response(&err, true, cmd_name);
            return vec![err];
        }

        // Wait if server is paused (CLIENT PAUSE). This is checked AFTER transaction
        // queuing so commands inside MULTI are queued without blocking.
        self.wait_if_paused(cmd_name, &cmd.args).await;

        // Connection-state commands (ASKING/READONLY/READWRITE) migrated behind
        // the ConnCtx seam MUTATE per-connection state, so they dispatch through
        // the *mutable* `conn_ctx_authmut` view. This must run before the
        // read-only registry union below, which would otherwise claim them and
        // dispatch with `conn_state = None` (a no-op). (RESET, the fourth
        // connection-state command, is intercepted earlier via `execute_reset`.)
        if let Some(responses) = self
            .dispatch_connection_state_command(cmd_name, &cmd.args)
            .await
        {
            return responses;
        }

        // Registry-union dispatch: a command registered as
        // `CommandImpl::Connection` (CONFIG, BGSAVE/LASTSAVE, CLIENT, DEBUG,
        // MONITOR, ACL, INFO, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY, LATENCY,
        // STATUS, the pub/sub family, and the scripting family) executes through
        // its `ConnCtx` executor. This is the sole connection-command dispatch
        // path: the legacy `ConnectionLevelHandler` router→handler machinery was
        // removed once every connection group had migrated behind the seam.
        if let Some(responses) = self.dispatch_connection_command(cmd_name, &cmd.args).await {
            return responses;
        }

        // Handle PSYNC command - validates args and returns handoff signal
        // The actual handoff happens in the run() loop when it detects PSYNC_HANDOFF
        if cmd_name == "PSYNC" {
            // Check if we have a primary replication handler (we're running as primary)
            if self.cluster.primary_replication_handler.is_none() {
                return vec![Response::error(
                    "ERR PSYNC not supported - server is not running as primary",
                )];
            }
            // Execute PSYNC command which will return PSYNC_HANDOFF signal
            return vec![self.route_and_execute(cmd, cmd_name).await];
        }

        // Handle WAIT at the connection level: it blocks on the replication
        // WaitCoordinator (offset snapshot, GETACK solicitation, quorum wait),
        // not on shard routing. Reached only outside MULTI — a queued WAIT
        // executes on the shard at EXEC time, where `WaitCommand::execute`
        // returns the acked count without blocking (Redis deny-blocking
        // semantics). Same dispatch shape as PSYNC above.
        if cmd_name == "WAIT" {
            return vec![self.handle_wait_command(&cmd.args).await];
        }

        // Server-wide commands (registry-driven: SCAN, KEYS, DBSIZE, RANDOMKEY,
        // FLUSHDB, FLUSHALL, MIGRATE, SHUTDOWN, TS.*, FT.*, ES.ALL). The typed
        // op is extracted from the registry entry's strategy before re-borrowing
        // `self` mutably: `execution_strategy()` returns an owned
        // `ExecutionStrategy`, so copying the `Copy` `ServerWideOp` out ends the
        // registry borrow before `dispatch_server_wide` takes `&mut self`.
        let server_wide_op = self.core.registry.get_entry(cmd_name).and_then(|entry| {
            match entry.execution_strategy() {
                ExecutionStrategy::ServerWide(op) => Some(op),
                _ => None,
            }
        });
        if let Some(op) = server_wide_op {
            return vec![self.dispatch_server_wide(op, &cmd.args).await];
        }

        // Route CLUSTER GETKEYSINSLOT / COUNTKEYSINSLOT to the correct shard.
        // These subcommands query a specific slot's keys, but the CLUSTER command
        // is keyless and would otherwise execute on the connection's assigned shard.
        // Since all keys for a given slot live on shard (slot % num_shards), we
        // must forward to that shard.
        if cmd_name == "CLUSTER" && !cmd.args.is_empty() {
            let sub = cmd.args[0].to_ascii_uppercase();
            if (sub.as_slice() == b"GETKEYSINSLOT" || sub.as_slice() == b"COUNTKEYSINSLOT")
                && cmd.args.len() >= 2
                && let Ok(slot_str) = std::str::from_utf8(&cmd.args[1])
                && let Ok(slot) = slot_str.parse::<u16>()
            {
                let target_shard = slot as usize % self.num_shards;
                return vec![self.execute_on_shard(target_shard, Arc::clone(cmd)).await];
            }
        }

        // Validate cluster slot ownership (returns CROSSSLOT/MOVED/ASK errors)
        if let Some(cluster_error) = self.validate_cluster_slots(cmd) {
            return vec![cluster_error];
        }

        // Check for TRYAGAIN during slot migration for multi-key commands
        if let Some(tryagain) = self.check_migrating_multikey(cmd).await {
            return vec![tryagain];
        }

        // Client tracking: compute whether this command's reads should be tracked
        self.pending_track_reads = self.state.should_track_read();

        // NO-TOUCH: check if this connection has NO_TOUCH flag set
        self.pending_no_touch = self
            .admin
            .client_registry
            .get(self.state.id)
            .map(|info| info.flags.contains(frogdb_core::ClientFlags::NO_TOUCH))
            .unwrap_or(false);

        // Normal execution
        let response = if self
            .per_request_spans
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.route_and_execute(cmd, cmd_name)
                .instrument(tracing::info_span!("cmd_route"))
                .await
        } else {
            self.route_and_execute(cmd, cmd_name).await
        };

        // For MIGRATING slots: if the key doesn't exist locally (nil response),
        // convert to ASK redirect so the client retries on the importing target.
        if let Some(ask) = self.migrating_ask_for_nil(cmd, &response) {
            return vec![ask];
        }

        // Handle internal action signals (blocking, raft, migrate)
        let final_response = self.handle_internal_action(response).await;

        // Record failed call if execution returned an error
        self.record_error_response(&final_response, false, cmd_name);

        vec![final_response]
    }

    /// Classify and record an error response for error statistics.
    ///
    /// `is_rejected` = true means the command was rejected before execution (pre-checks, arity).
    /// `is_rejected` = false means the command failed during execution.
    fn record_error_response(&self, response: &Response, is_rejected: bool, cmd_name: &str) {
        if let Response::Error(bytes) = response {
            let prefix = frogdb_core::extract_error_prefix(bytes);
            let error_stats = &self.admin.client_registry.error_stats;
            if is_rejected {
                error_stats.record_rejected(prefix);
                self.admin.client_registry.record_command_rejected(cmd_name);
            } else {
                error_stats.record_failed(prefix);
                self.admin.client_registry.record_command_failed(cmd_name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use frogdb_core::{CommandRegistry, ExecutionStrategy, ServerWideOp};

    /// Each `ServerWideOp` must be declared by at most one command spec.
    /// The exhaustive match in `dispatch_server_wide` already guarantees every
    /// op has a handler (adding a variant without an arm is a compile error);
    /// this catches the opposite mistake — a wrong-op copy-paste that points two
    /// different commands at the same op.
    #[test]
    fn server_wide_ops_are_unique_per_command() {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);

        let mut seen: Vec<(ServerWideOp, String)> = Vec::new();
        for (name, entry) in registry.iter() {
            if let ExecutionStrategy::ServerWide(op) = entry.execution_strategy() {
                if let Some((_, prev)) = seen.iter().find(|(seen_op, _)| *seen_op == op) {
                    panic!(
                        "ServerWideOp::{:?} declared by two commands: {} and {}",
                        op, prev, name
                    );
                }
                seen.push((op, name.to_string()));
            }
        }
    }
}
