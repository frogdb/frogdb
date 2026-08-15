//! The connection's side of the EXEC seam.
//!
//! The EXEC *algorithm* — abort/rate-limit gates, EXEC-time slot
//! re-validation, the pause barrier, the deferred/shard partition, target
//! resolution, the shard round-trip and the deferred-command merge — lives in
//! the [`frogdb_txn`] crate, where it can be driven by a test host without a
//! server. What lives here is the other half of that seam:
//!
//! - [`ConnectionHandler::handle_exec`] — takes the transaction off the
//!   connection state and hands it to [`frogdb_txn::handle_exec`].
//! - `impl TxnHost for ConnectionHandler` — every effect the algorithm needs,
//!   expressed against the real handler: the registry lookup, the rate limiter,
//!   the cluster redirect seam, the shard channels, and the re-entry into the
//!   `ConnCtx` dispatch machinery for deferred connection-level commands (the
//!   meta-circularity) and into `dispatch_server_wide` for server-wide ones.
//!
//! The transaction command *state machine* (MULTI/DISCARD/WATCH/UNWATCH) is
//! migrated behind the [`ConnectionCommand`](frogdb_core::ConnectionCommand)
//! seam in
//! [`transaction_conn_command`](crate::connection::transaction_conn_command).
//! Command *queuing* itself — validating and queuing a command while a MULTI is
//! open — lives in
//! [`PreDispatchView`](crate::connection::guards::PreDispatchView).

use async_trait::async_trait;
use bytes::Bytes;
use frogdb_core::{CoreMsg, MetricsRecorder, RateLimitExceeded, ServerWideOp, WatchEntry};
use frogdb_protocol::{ParsedCommand, Response};
use frogdb_txn::{Deferral, ShardTxnReply, TxnHost};
use tokio::sync::oneshot;

use crate::connection::ConnectionHandler;
use crate::slot_migration::SlotVerdict;

impl ConnectionHandler {
    /// Handle EXEC command - execute the queued transaction.
    pub(crate) async fn handle_exec(&mut self) -> Vec<Response> {
        // Take the queue and watches atomically, leaving the transaction state
        // clean. EXEC's exit paths therefore never need to clear fields by hand.
        let Some(summary) = self.state.take_transaction() else {
            return vec![Response::error("ERR EXEC without MULTI")];
        };
        frogdb_txn::handle_exec(self, summary).await
    }

    /// Execute a connection-level command that was deferred from a transaction.
    ///
    /// This dispatches directly through the command's `CommandImpl::Connection`
    /// executor (the registry union), rather than re-entering the main
    /// `route_and_execute_with_transaction` flow — which would create a recursive
    /// async cycle via `dispatch_transaction_command` -> `handle_exec`.
    ///
    /// Returns `(exec_slot_response, push_confirmations)`. The first element goes
    /// into the EXEC array; the second contains any out-of-band Push frames to
    /// send after the EXEC response (e.g., RESP3 subscribe/unsubscribe confirmations).
    async fn execute_connection_level_in_transaction(
        &mut self,
        cmd_name: &str,
        args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        // Registry-union dispatch: every deferred connection-level command is a
        // migrated `CommandImpl::Connection` executor (CONFIG, BGSAVE/LASTSAVE,
        // CLIENT, DEBUG, MONITOR, ACL, INFO, HOTKEYS, FT.CURSOR, SLOWLOG, MEMORY,
        // LATENCY, STATUS, the pub/sub family, and the scripting family), so it
        // executes through its executor exactly as on the main dispatch path
        // (`dispatch_connection_command`). `as_connection()` yields a `'static`
        // reference, so it does not conflict with re-borrowing `self` to build
        // the `ConnCtx`.
        let migrated = self
            .core
            .registry
            .get_entry(cmd_name)
            .and_then(|entry| entry.as_connection());
        if let Some(command) = migrated {
            // The deferred connection command selects its dispatch shape from its
            // declared `mutation` capability, exactly as on the main path
            // (`dispatch_connection_command`) — never from its string name.
            return match command.spec().mutation {
                // Pub/sub deferred to EXEC: multi-response with bespoke MULTI
                // framing (PUBLISH/SPUBLISH/PUBSUB single response;
                // SUBSCRIBE-family incl. SUNSUBSCRIBE one confirmation per
                // channel; SSUBSCRIBE alone rejected inside MULTI — verified
                // Redis-parity policy, see `exec_pubsub_in_transaction`). This
                // is a distinct framing path from the main `execute_pubsub`.
                frogdb_core::ConnMutation::PubSub => {
                    self.exec_pubsub_in_transaction(cmd_name, command, args)
                        .await
                }
                // MONITOR owns its `MonitorIo` at the call site; route it through
                // the dedicated builder so a deferred MONITOR wires
                // `ConnCtx::monitor` rather than hitting the read-only view (which
                // has `monitor = None`).
                frogdb_core::ConnMutation::Monitor => {
                    (self.execute_monitor(command, args).await, vec![])
                }
                // Read-only (CONFIG/…), AUTH-class, and CLIENT views build in
                // place from the same declared capability.
                mutation @ (frogdb_core::ConnMutation::None
                | frogdb_core::ConnMutation::Auth
                | frogdb_core::ConnMutation::Client) => (
                    command
                        .execute(&mut self.conn_ctx_for(mutation), args)
                        .await,
                    vec![],
                ),
            };
        }

        // The only connection-level command not registered as a
        // `CommandImpl::Connection` executor is PSYNC (`ConnectionLevel(
        // Replication)`, registered as a shard `Command`). It has no meaningful
        // behavior inside MULTI/EXEC and never had a real arm on the legacy path
        // either, so — like any other non-migrated deferred command — it replies
        // `+OK`.
        (Response::ok(), vec![])
    }
}

#[async_trait]
impl TxnHost for ConnectionHandler {
    fn shard_id(&self) -> usize {
        self.shard_id
    }

    fn conn_id(&self) -> u64 {
        self.state.id
    }

    fn metrics_recorder(&self) -> &dyn MetricsRecorder {
        &*self.observability.metrics_recorder
    }

    fn deferral_of(&self, name: &str) -> Option<Deferral> {
        self.core
            .registry
            .get_entry(name)
            .and_then(|entry| match entry.execution_strategy() {
                frogdb_core::ExecutionStrategy::ConnectionLevel(_) => {
                    Some(Deferral::ConnectionLevel)
                }
                frogdb_core::ExecutionStrategy::ServerWide(op) => Some(Deferral::ServerWide(op)),
                _ => None,
            })
    }

    fn queue_has_writes(&self, queue: &[ParsedCommand]) -> bool {
        self.transaction_has_writes(queue)
    }

    fn try_acquire_batch(&self, queue: &[ParsedCommand]) -> Result<(), RateLimitExceeded> {
        if self.is_admin {
            return Ok(());
        }
        let Some(user) = self.state.authenticated_user() else {
            return Ok(());
        };
        let Some(ref rl) = user.rate_limit else {
            return Ok(());
        };
        let total_bytes: u64 = queue
            .iter()
            .map(|c| crate::connection::estimate_command_size(c) as u64)
            .sum();
        rl.try_acquire_batch(queue.len() as u64, total_bytes)
    }

    async fn validate_queued_batch(
        &mut self,
        queue: &[ParsedCommand],
        asking: bool,
    ) -> Option<Response> {
        // A validated batch stamps the fence the dispatch driver spends after
        // `EXEC` finishes (see `ConnectionHandler::spend_slot_fence`), so a
        // handoff prepared while the batch executes cannot be acknowledged.
        // Re-validation after a pause re-stamps, replacing the stamp taken
        // before the wait with one cut from the post-wait topology.
        match self
            .pre_dispatch_view()
            .validate_queued_batch(queue, asking)
            .await
        {
            SlotVerdict::Reply(redirect) => Some(redirect),
            SlotVerdict::Serve(fence) => {
                self.pending_slot_fence = fence;
                None
            }
        }
    }

    fn watched_slots_still_local(&mut self, watches: &[WatchEntry], asking: bool) -> bool {
        self.pre_dispatch_view()
            .watched_slots_still_local(watches, asking)
    }

    async fn wait_if_paused(&mut self, queue: &[ParsedCommand]) -> bool {
        // Resolve the batch to a slot here, on the host, where the registry
        // lives — the EXEC algorithm never learns what a hash slot is.
        let slot = crate::connection::pause_gate::queue_pause_slot(&self.core.registry, queue);
        self.wait_if_paused_for_transaction(slot).await
    }

    async fn send_shard_transaction(
        &mut self,
        target_shard: usize,
        commands: Vec<ParsedCommand>,
        watches: Vec<WatchEntry>,
    ) -> ShardTxnReply {
        let (response_tx, response_rx) = oneshot::channel();
        let msg = CoreMsg::ExecTransaction {
            commands,
            watches,
            conn_id: self.state.id,
            protocol_version: self.state.protocol_version,
            // Re-admitted at the shard before any queued command runs: the
            // queue-time checks are as old as the MULTI window
            // (`specs/txn.md` FM-TXN-051).
            admission: self.write_admission(),
            response_tx,
        };

        if self.core.shard_senders[target_shard]
            .send(msg)
            .await
            .is_err()
        {
            return ShardTxnReply::Unavailable;
        }

        match response_rx.await {
            Ok(result) => ShardTxnReply::Replied(result),
            Err(_) => ShardTxnReply::Dropped,
        }
    }

    async fn run_connection_level(
        &mut self,
        name: &str,
        args: &[Bytes],
    ) -> (Response, Vec<Response>) {
        self.execute_connection_level_in_transaction(name, args)
            .await
    }

    async fn run_server_wide(&mut self, op: ServerWideOp, args: &[Bytes]) -> Response {
        self.dispatch_server_wide(op, args).await
    }
}
