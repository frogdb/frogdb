//! Connection setup, teardown, and utility methods.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{
    BoxFuture, ClientTrackingProvider, CommandFlags, FunctionFlags, InvalidationMessage,
    InvalidationSender, PauseMode, ShardMessage, ShardSender,
};
use frogdb_protocol::Response;
use tokio::sync::mpsc;

use frogdb_core::ClientMemoryUsage;

use super::ConnectionHandler;
use super::state::{STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS};

/// Client-tracking IO plumbing owned by the connection handler: the
/// invalidation delivery channel and the optional REDIRECT forwarding task.
/// Grouped into one value so the CLIENT executor can hold it as a
/// [`ClientTrackingProvider`] `&mut` borrow disjoint from the `ConnectionState`
/// borrow (`ConnCtx::conn_state`).
///
/// The tracking session is one unit with three coupled halves: the
/// `ConnectionState` transition (owned by
/// [`ConnStateMut`](frogdb_core::ConnStateMut)), this local delivery plumbing,
/// and the per-shard registration. [`ClientTrackingProvider::enable`] /
/// [`disable`](ClientTrackingProvider::disable) own the latter two so no call
/// site re-implements a subset of the invariant.
///
/// Shard message choice: CLIENT TRACKING OFF sends `TrackingUnregister`
/// (tracking-only — the connection keeps its pub/sub subscriptions), while RESET
/// and connection close send `ConnectionClosed`, whose tracking half is
/// identical to `TrackingUnregister` and additionally drops pub/sub state — one
/// message covers both teardowns on those paths, which then only call
/// [`TrackingIo::teardown_local`].
#[derive(Default)]
pub(crate) struct TrackingIo {
    /// Sender for invalidation messages (cloned to shards when tracking enabled).
    /// Lazily initialized on CLIENT TRACKING ON.
    invalidation_tx: Option<InvalidationSender>,
    /// Receiver for invalidation messages from shards.
    /// Lazily initialized on CLIENT TRACKING ON.
    pub(crate) invalidation_rx: Option<mpsc::UnboundedReceiver<InvalidationMessage>>,
    /// REDIRECT forwarding task handle (aborted on TRACKING OFF or disconnect).
    redirect_task: Option<tokio::task::JoinHandle<()>>,
}

impl TrackingIo {
    /// Ensure the invalidation channel is initialized, returning a clone of the
    /// sender. Called lazily on CLIENT TRACKING ON (non-REDIRECT modes).
    fn ensure_invalidation_channel(&mut self) -> InvalidationSender {
        if let Some(ref tx) = self.invalidation_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.invalidation_tx = Some(tx.clone());
        self.invalidation_rx = Some(rx);
        tx
    }

    /// Local half of the tracking teardown: drop the invalidation channels and
    /// abort the redirect forwarding task. Idempotent.
    pub(crate) fn teardown_local(&mut self) {
        self.invalidation_tx = None;
        self.invalidation_rx = None;
        if let Some(task) = self.redirect_task.take() {
            task.abort();
        }
    }
}

impl ClientTrackingProvider for TrackingIo {
    /// Enable client tracking IO (CLIENT TRACKING ON): wire the invalidation
    /// delivery path and register with every shard. The `ConnectionState`
    /// transition ([`ConnStateMut::enable_tracking`](frogdb_core::ConnStateMut::enable_tracking))
    /// has already run and produced `prefixes`; the REDIRECT target was
    /// validated by the caller.
    fn enable<'a>(
        &'a mut self,
        conn_id: u64,
        redirect: u64,
        bcast: bool,
        noloop: bool,
        prefixes: Vec<Bytes>,
        shard_senders: &'a [ShardSender],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            // Re-enabling may change REDIRECT: abort any stale forwarding task so
            // repeated CLIENT TRACKING ON calls don't leak one task per call.
            if let Some(task) = self.redirect_task.take() {
                task.abort();
            }

            // Invalidation delivery path: either a forwarding task that publishes
            // to __redis__:invalidate (REDIRECT mode) or the connection's own
            // invalidation channel.
            let sender = if redirect > 0 {
                let (fwd_tx, mut fwd_rx) = mpsc::unbounded_channel::<InvalidationMessage>();
                let broadcast_shard =
                    shard_senders[crate::connection::pubsub_conn_command::BROADCAST_SHARD].clone();
                let task = tokio::spawn(async move {
                    while let Some(msg) = fwd_rx.recv().await {
                        let payload = match &msg {
                            InvalidationMessage::Keys(keys) => {
                                // Encode as space-separated key names for pub/sub
                                let key_strs: Vec<&[u8]> =
                                    keys.iter().map(|k| k.as_ref()).collect();
                                Bytes::copy_from_slice(&key_strs.join(&b' '))
                            }
                            InvalidationMessage::FlushAll => Bytes::from_static(b""),
                        };
                        let (resp_tx, _) = tokio::sync::oneshot::channel();
                        let _ = broadcast_shard
                            .send(ShardMessage::Publish {
                                channel: Bytes::from_static(b"__redis__:invalidate"),
                                message: payload,
                                response_tx: resp_tx,
                            })
                            .await;
                    }
                });
                self.redirect_task = Some(task);
                fwd_tx
            } else {
                self.ensure_invalidation_channel()
            };

            // Register with all shards. Broadcast registration is additive
            // shard-side, so each call sends only its own (new) prefix batch.
            if bcast {
                for shard_sender in shard_senders.iter() {
                    let _ = shard_sender
                        .send(ShardMessage::TrackingBroadcastRegister {
                            conn_id,
                            sender: sender.clone(),
                            noloop,
                            prefixes: prefixes.clone(),
                        })
                        .await;
                }
            } else {
                for shard_sender in shard_senders.iter() {
                    let _ = shard_sender
                        .send(ShardMessage::TrackingRegister {
                            conn_id,
                            sender: sender.clone(),
                            noloop,
                        })
                        .await;
                }
            }
        })
    }

    /// Disable client tracking IO (CLIENT TRACKING OFF): unregister from every
    /// shard and tear down local plumbing. The caller has already applied the
    /// `ConnectionState` transition and confirmed tracking had been enabled.
    fn disable<'a>(
        &'a mut self,
        conn_id: u64,
        shard_senders: &'a [ShardSender],
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            for shard_sender in shard_senders.iter() {
                let _ = shard_sender
                    .send(ShardMessage::TrackingUnregister { conn_id })
                    .await;
            }
            self.teardown_local();
        })
    }
}

impl ConnectionHandler {
    /// Local half of the tracking teardown: drop the invalidation channels and
    /// abort the redirect forwarding task. Idempotent. Used directly by RESET
    /// and connection close, where shard-side tracking state is removed by the
    /// broader `ConnectionClosed` fan-out instead of `TrackingUnregister`.
    pub(crate) fn tracking_session_teardown_local(&mut self) {
        self.tracking_io.teardown_local();
    }

    /// Notify all shards that this connection is closed.
    pub(super) async fn notify_connection_closed(&mut self) {
        // Drop MONITOR subscription (auto-decrements broadcast receiver count)
        self.monitor_rx = None;

        // Tear down the tracking session's local plumbing; the shard-side
        // half rides on the ConnectionClosed fan-out below.
        self.tracking_session_teardown_local();

        // Final stats sync before closing
        self.sync_stats_to_registry();

        // Notify shards if we had subscriptions or tracking enabled
        if self.state.in_pubsub_mode() || self.state.tracking().enabled {
            for sender in self.core.shard_senders.iter() {
                let _ = sender
                    .send(ShardMessage::ConnectionClosed {
                        conn_id: self.state.id,
                    })
                    .await;
            }
        }

        // Unregister any blocking waits
        if let Some(shard_id) = self.state.blocked_shard()
            && let Some(sender) = self.core.shard_senders.get(shard_id)
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
    pub(super) fn extract_psync_handoff(responses: &[Response]) -> Option<(String, i64)> {
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

    /// Periodically sync local stats and memory usage to the registry.
    /// Syncs every STATS_SYNC_INTERVAL_COMMANDS commands or STATS_SYNC_INTERVAL_MS milliseconds.
    pub(super) fn maybe_sync_stats(&mut self) {
        let should_sync = self.state.local_stats.commands_total >= STATS_SYNC_INTERVAL_COMMANDS
            || self.state.last_stats_sync.elapsed().as_millis() as u64 >= STATS_SYNC_INTERVAL_MS;

        if should_sync {
            if self.state.local_stats.has_data() {
                self.sync_stats_to_registry();
            }
            // Always sync memory on the same schedule
            self.sync_memory_to_registry();
            // Check if client eviction is needed
            self.maybe_evict_clients();
        }
    }

    /// Compute the current memory usage of this connection.
    pub(crate) fn compute_client_memory(&self) -> ClientMemoryUsage {
        // Query buffer: access inner BytesMut length from Framed codec
        let query_buf_size = self.framed.read_buffer().len();

        // Argv: 0 between commands (transient during execution)
        let argv_mem = 0;

        // Multi buffer: sum of estimated memory of queued commands
        let multi_mem = self
            .state
            .queued_commands()
            .map(|q| {
                q.iter()
                    .map(|cmd| {
                        // Estimate: name + args bytes + Vec overhead
                        cmd.name.len() + cmd.args.iter().map(|a| a.len() + 24).sum::<usize>() + 64 // ParsedCommand struct overhead
                    })
                    .sum()
            })
            .unwrap_or(0);

        // Output buffer: resp3_buf
        let output_buf_len = self.resp3_buf.len();

        // Output list (pub/sub + invalidation channel pending messages)
        // We can't directly read the channel length, but we track it via
        // subscription counts as a proxy
        let output_list_len = 0;
        let output_list_mem = 0;

        // Watched keys
        let watched_keys_mem: usize = self
            .state
            .watched_key_iter()
            .map(|k| k.len() + 48) // key bytes + HashMap entry overhead
            .sum();

        // Subscriptions (channels + patterns + sharded)
        let subscriptions_mem: usize = self
            .state
            .subscription_name_iter()
            .map(|b| b.len() + 48) // bytes + HashSet entry overhead
            .sum();

        // Tracking prefixes
        let tracking_prefixes_mem: usize = self
            .state
            .tracking()
            .prefixes
            .iter()
            .map(|b| b.len() + 24) // bytes + Vec element overhead
            .sum();

        ClientMemoryUsage {
            query_buf_size,
            argv_mem,
            multi_mem,
            output_buf_len,
            output_list_len,
            output_list_mem,
            watched_keys_mem,
            subscriptions_mem,
            tracking_prefixes_mem,
        }
    }

    /// Sync memory usage to the client registry.
    pub(crate) fn sync_memory_to_registry(&mut self) {
        let mem = self.compute_client_memory();
        self.admin.client_registry.update_memory(self.state.id, mem);
    }

    /// Check if client eviction is needed and trigger it.
    /// Called after syncing memory.
    pub(crate) fn maybe_evict_clients(&self) {
        let limit = self.admin.config_manager.resolve_maxmemory_clients();
        if limit == 0 {
            return;
        }
        let total = self.admin.client_registry.total_client_memory();
        if total > limit {
            let evicted = self.admin.client_registry.try_evict_clients(limit);
            if evicted > 0 {
                tracing::info!(
                    evicted,
                    total_memory = total,
                    limit,
                    "Client eviction: evicted {} client(s)",
                    evicted
                );
            }
        }
    }

    /// Force sync local stats to the registry.
    pub(crate) fn sync_stats_to_registry(&mut self) {
        self.state
            .sync_stats_to_registry(&self.admin.client_registry);
    }

    /// Check whether a command should be blocked by the current pause state.
    ///
    /// Returns `true` if the command must wait, `false` if it's exempt or no
    /// pause is active.
    ///
    /// `cmd_args` is the raw argument list for the command (used to inspect
    /// EVAL/EVALSHA script bodies for `#!lua flags=no-writes` shebangs).
    pub(crate) fn should_pause_command(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) -> bool {
        // Certain commands are always exempt from pause
        let is_exempt = matches!(
            cmd_name,
            "CLIENT" | "PING" | "QUIT" | "RESET" | "INFO" | "CONFIG" | "DEBUG" | "SLOWLOG"
        );

        if is_exempt {
            return false;
        }

        match self.admin.client_registry.check_pause() {
            Some(PauseMode::All) => true,
            Some(PauseMode::Write) => {
                // Get command flags to determine if this is a write/script
                // command. Resolve through the registry *union* (`get_entry`) so
                // keyed connection commands (EVAL/EVALSHA/FCALL) — which carry the
                // SCRIPT flag but have no shard executor — are still recognized.
                let flags = self
                    .core
                    .registry
                    .get_entry(cmd_name)
                    .map(|e| e.flags())
                    .unwrap_or(CommandFlags::empty());

                let is_write_command = flags.contains(CommandFlags::WRITE);
                let is_script_command = flags.contains(CommandFlags::SCRIPT);
                let is_readonly_script =
                    is_script_command && flags.contains(CommandFlags::READONLY);

                // Read-only script variants (EVAL_RO, EVALSHA_RO, FCALL_RO) are
                // always exempt from PAUSE WRITE.
                if is_readonly_script {
                    return false;
                }

                // For EVAL/EVALSHA: check if the script body has a
                // `#!lua flags=no-writes` shebang. If so, exempt it.
                if is_script_command
                    && !is_readonly_script
                    && matches!(cmd_name, "EVAL" | "EVALSHA" | "FCALL")
                    && self.script_has_no_writes_flag(cmd_name, cmd_args)
                {
                    return false;
                }

                // Block writes, scripts (conservatively), and special commands
                // that replicate or have write side-effects.
                is_write_command
                    || is_script_command
                    || matches!(cmd_name, "PFCOUNT" | "PUBLISH" | "SPUBLISH")
            }
            None => false,
        }
    }

    /// Check whether a script command has a `no-writes` flag via shebang
    /// or function registration.
    ///
    /// For EVAL: first arg is the script body — check for `#!lua flags=...no-writes...`
    /// For EVALSHA: the script is cached; we can't inspect it here, so be conservative.
    /// For FCALL: look up the function in the registry and check its flags.
    fn script_has_no_writes_flag(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) -> bool {
        match cmd_name {
            "EVAL" => {
                // First arg is the script body
                if let Some(script_body) = cmd_args.first() {
                    return Self::shebang_has_no_writes(script_body);
                }
                false
            }
            "FCALL" => {
                // First arg is the function name
                if let Some(func_name) = cmd_args.first() {
                    let name = std::str::from_utf8(func_name).unwrap_or("");
                    let registry = self.admin.function_registry.read().unwrap();
                    if let Some((func, _)) = registry.get_function(name) {
                        return func.flags.contains(FunctionFlags::NO_WRITES);
                    }
                }
                false
            }
            // EVALSHA: we can't cheaply inspect the cached script from the
            // connection handler, so be conservative and block.
            _ => false,
        }
    }

    /// Lightweight check for `#!lua flags=...no-writes...` in a script body.
    fn shebang_has_no_writes(source: &[u8]) -> bool {
        let s = match std::str::from_utf8(source) {
            Ok(s) => s,
            Err(_) => return false,
        };
        if !s.starts_with("#!") {
            return false;
        }
        let first_line = s.lines().next().unwrap_or("");
        // Parse "flags=..." from the shebang line
        for part in first_line.split_whitespace() {
            if let Some(("flags", v)) = part.split_once('=') {
                for f in v.split(',') {
                    if f.trim() == "no-writes" {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Wait if the server is paused (CLIENT PAUSE).
    /// This queues commands (not drops them) by blocking until pause ends.
    /// Marks the client as PAUSED so `wait_for_blocked_clients` can observe it
    /// and CLIENT UNBLOCK correctly rejects unblocking.
    ///
    /// Called from `route_and_execute_with_transaction` after transaction-control
    /// dispatch and transaction queuing, so it only blocks commands outside MULTI.
    pub(crate) async fn wait_if_paused(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) {
        if !self.should_pause_command(cmd_name, cmd_args) {
            return;
        }

        // Mark client as paused/blocked
        self.admin
            .client_registry
            .update_paused_state(self.state.id, true);

        // Wait until pause ends or this command is no longer affected
        loop {
            if !self.should_pause_command(cmd_name, cmd_args) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clear paused state
        self.admin
            .client_registry
            .update_paused_state(self.state.id, false);
    }

    /// Wait if the server is paused, for a write-containing transaction (EXEC).
    /// Both PAUSE ALL and PAUSE WRITE block write transactions.
    pub(crate) async fn wait_if_paused_for_transaction(&self) {
        if self.admin.client_registry.check_pause().is_none() {
            return;
        }

        // Mark client as paused/blocked
        self.admin
            .client_registry
            .update_paused_state(self.state.id, true);

        // Wait until pause ends
        loop {
            if self.admin.client_registry.check_pause().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clear paused state
        self.admin
            .client_registry
            .update_paused_state(self.state.id, false);
    }

    /// Check whether a MULTI/EXEC transaction contains write commands that
    /// should be blocked by PAUSE WRITE.
    pub(crate) fn transaction_has_writes(&self, queue: &[frogdb_protocol::ParsedCommand]) -> bool {
        for cmd in queue {
            let name = cmd.name_uppercase();
            let name_str = std::str::from_utf8(&name).unwrap_or("");
            // Resolve flags through the registry *union* (`get_entry`) so keyed
            // connection commands (EVAL/EVALSHA/FCALL) — which carry the SCRIPT
            // flag but have no shard executor — are still classified as writes
            // for pause purposes.
            let flags = self
                .core
                .registry
                .get_entry(name_str)
                .map(|e| e.flags())
                .unwrap_or(CommandFlags::empty());

            let is_write = flags.contains(CommandFlags::WRITE);
            let is_script = flags.contains(CommandFlags::SCRIPT);
            let is_readonly_script = is_script && flags.contains(CommandFlags::READONLY);

            // Read-only script variants are never writes
            if is_readonly_script {
                continue;
            }

            // For EVAL with no-writes shebang, skip
            if is_script
                && matches!(name_str, "EVAL" | "EVALSHA" | "FCALL")
                && self.script_has_no_writes_flag(name_str, &cmd.args)
            {
                continue;
            }

            if is_write || is_script || matches!(name_str, "PFCOUNT" | "PUBLISH" | "SPUBLISH") {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tracking_redirect_tests {
    //! Regression coverage for the REDIRECT re-enable task leak (proposal 34).
    //!
    //! Re-enabling `CLIENT TRACKING ON REDIRECT <id>` previously spawned a fresh
    //! invalidation-forwarding task on every call without aborting the prior one,
    //! leaking one task per re-enable. The fix in
    //! [`TrackingIo::enable`](super::TrackingIo) aborts the stale task before
    //! respawning (`self.redirect_task.take() -> abort()`).
    //!
    //! Why this is a unit test rather than an integration/behavioral one: the
    //! shard-side invalidation registry stores exactly one sender per connection
    //! (`InvalidationRegistry::register` inserts, replacing any prior sender), so a
    //! re-enable drops the old task's sender and an un-aborted task exits on its
    //! own once the drop propagates. There is therefore no steady-state
    //! double-delivery to observe from a client. The genuine, deterministic
    //! observable is the forwarding task's lifecycle, reached here via the
    //! `JoinHandle` on `TrackingIo` and its `AbortHandle::is_finished()`.
    //!
    //! The retained shard receivers below keep every sender cloned into a queued
    //! `TrackingRegister` message alive, so an un-aborted forwarding task blocks on
    //! `recv()` forever instead of exiting — isolating the abort logic as the sole
    //! thing under test. With the abort removed, the first handle never finishes
    //! and these tests fail on the bounded-timeout assertion.

    use std::time::{Duration, Instant};

    use frogdb_core::ClientTrackingProvider;
    use frogdb_core::shard::Envelope;
    use tokio::sync::mpsc;

    use super::TrackingIo;

    /// Build `count` [`ShardSender`](frogdb_core::ShardSender)s, returning the
    /// receivers so the caller can keep them (and thus any queued senders) alive.
    fn make_shard_senders(
        count: usize,
    ) -> (Vec<frogdb_core::ShardSender>, Vec<mpsc::Receiver<Envelope>>) {
        let mut senders = Vec::with_capacity(count);
        let mut receivers = Vec::with_capacity(count);
        for _ in 0..count {
            let (tx, rx) = mpsc::channel::<Envelope>(64);
            senders.push(frogdb_core::ShardSender::new(tx));
            receivers.push(rx);
        }
        (senders, receivers)
    }

    /// Poll `cond` until it holds or `timeout` elapses. Used instead of a fixed
    /// sleep because task cancellation is observed by the runtime asynchronously.
    async fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cond()
    }

    /// Re-enabling REDIRECT to a *different* target id must abort the previous
    /// forwarding task before spawning the replacement.
    #[tokio::test]
    async fn reenable_redirect_different_target_aborts_previous_task() {
        let mut io = TrackingIo::default();
        // Keep the receivers alive so queued senders stay live for the test.
        let (senders, _receivers) = make_shard_senders(4);

        // First enable: REDIRECT to connection id 2.
        io.enable(1, 2, false, false, Vec::new(), &senders).await;
        let first = io
            .redirect_task
            .as_ref()
            .expect("first enable should spawn a forwarding task")
            .abort_handle();
        assert!(
            !first.is_finished(),
            "first forwarding task should be running after the first enable"
        );

        // Re-enable: REDIRECT to a different connection id 3.
        io.enable(1, 3, false, false, Vec::new(), &senders).await;
        let second = io
            .redirect_task
            .as_ref()
            .expect("re-enable should spawn a new forwarding task")
            .abort_handle();
        assert!(
            !second.is_finished(),
            "second forwarding task should be running after re-enable"
        );

        let aborted = wait_until(Duration::from_secs(5), || first.is_finished()).await;
        assert!(
            aborted,
            "re-enabling REDIRECT must abort the previous forwarding task (task-leak regression)"
        );
        // The replacement task must still be alive.
        assert!(
            !second.is_finished(),
            "replacement task must remain running after the old one is aborted"
        );
    }

    /// Re-enabling REDIRECT to the *same* target id twice must likewise abort and
    /// respawn — a leaked task double-forwarding to one target is the classic
    /// symptom this guards against.
    #[tokio::test]
    async fn reenable_redirect_same_target_aborts_previous_task() {
        let mut io = TrackingIo::default();
        let (senders, _receivers) = make_shard_senders(4);

        io.enable(1, 2, false, false, Vec::new(), &senders).await;
        let first = io
            .redirect_task
            .as_ref()
            .expect("first enable should spawn a forwarding task")
            .abort_handle();
        assert!(!first.is_finished());

        io.enable(1, 2, false, false, Vec::new(), &senders).await;

        let aborted = wait_until(Duration::from_secs(5), || first.is_finished()).await;
        assert!(
            aborted,
            "re-enabling REDIRECT to the same id must abort the previous forwarding task"
        );
    }
}
