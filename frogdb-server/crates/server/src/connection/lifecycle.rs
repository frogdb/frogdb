//! Connection setup, teardown, and utility methods.

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{
    BlockingMsg, BoxFuture, ClientTrackingProvider, CommandFlags, ExecutionStrategy, FunctionFlags,
    InvalidationMessage, InvalidationSender, PauseMode, PubSubMsg, ShardSender, TrackingMsg,
};
use tokio::sync::mpsc;

use frogdb_core::ClientMemoryUsage;

use super::ConnectionHandler;
use super::pause_gate;
use super::state::{STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS};
use crate::scatter::ScatterGather;
use frogdb_core::clock;

/// The read buffer's resting size: what a connection is seeded with from the
/// core's pool and what an idle one is trimmed back down to. Matches
/// tokio-util's own `Framed` initial capacity (`INITIAL_CAPACITY`, 8 KiB) —
/// trimming below it would hand the codec less than it would otherwise start
/// with, so the very next command would grow it straight back.
pub(super) const READ_IDLE_TARGET: usize = 8 * 1024;

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
                            .send(PubSubMsg::Publish {
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
                ScatterGather::broadcast(shard_senders)
                    .broadcast_all(|_shard| TrackingMsg::TrackingBroadcastRegister {
                        conn_id,
                        sender: sender.clone(),
                        noloop,
                        prefixes: prefixes.clone(),
                    })
                    .await;
            } else {
                ScatterGather::broadcast(shard_senders)
                    .broadcast_all(|_shard| TrackingMsg::TrackingRegister {
                        conn_id,
                        sender: sender.clone(),
                        noloop,
                    })
                    .await;
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
            ScatterGather::broadcast(shard_senders)
                .broadcast_all(|_shard| TrackingMsg::TrackingUnregister { conn_id })
                .await;
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
            ScatterGather::broadcast(self.core.shard_senders.as_slice())
                .broadcast_all(|_shard| PubSubMsg::ConnectionClosed {
                    conn_id: self.state.id,
                })
                .await;
        }

        // Unregister any blocking waits. The connection is closing, so there is
        // no client to hand a raced serve back to — the ack is discarded (a
        // serve that raced this teardown is instead made whole by the shard's
        // restore-on-send-failure path). See `BlockingMsg::UnregisterWait`.
        if let Some(shard_id) = self.state.blocked_shard()
            && let Some(sender) = self.core.shard_senders.get(shard_id)
        {
            let (ack_tx, _ack_rx) = tokio::sync::oneshot::channel();
            let _ = sender
                .send(BlockingMsg::UnregisterWait {
                    conn_id: self.state.id,
                    ack: ack_tx,
                })
                .await;
        }
    }

    /// Periodically sync local stats and memory usage to the registry.
    /// Syncs every STATS_SYNC_INTERVAL_COMMANDS commands or STATS_SYNC_INTERVAL_MS milliseconds.
    pub(super) fn maybe_sync_stats(&mut self) {
        let should_sync = self.state.local_stats.commands_total >= STATS_SYNC_INTERVAL_COMMANDS
            || clock::elapsed(self.state.last_stats_sync).as_millis() as u64
                >= STATS_SYNC_INTERVAL_MS;

        if should_sync {
            let busy = self.state.local_stats.has_data();
            if busy {
                self.sync_stats_to_registry();
            }
            // Same schedule, because "shrink when idle" needs a tick and this is
            // the one the connection already has. A connection that did nothing
            // since the last sync is the definition of idle, and is where a
            // buffer grown by one big reply gets handed back.
            self.trim_idle_buffers(busy);
            // Always sync memory on the same schedule
            self.sync_memory_to_registry();
            // WATCH count on the same schedule — real per-connection state,
            // not otherwise visible to the registry between commands.
            self.admin
                .client_registry
                .update_watch_count(self.state.id, self.state.watched_key_iter().count());
            // Check if client eviction is needed
            self.maybe_evict_clients();
        }
    }

    /// The connection's housekeeping tick, driven by the `select!` loop's timer
    /// arm.
    ///
    /// Two jobs, both of which only exist because a connection can sit still:
    ///
    /// 1. **Judge the output buffer.** `client-output-buffer-limit`'s soft limit
    ///    is a stopwatch — "above the soft mark continuously for N seconds" — so
    ///    something has to look while nothing is being written. This is that
    ///    something, and it goes through the same
    ///    [`account_buffered_output`](Self::account_buffered_output) seam the
    ///    write path uses, judging freshly measured bytes rather than a stale
    ///    figure.
    /// 2. **Give buffers back.** A connection that served one huge reply and
    ///    then went quiet keeps that capacity for the rest of its session
    ///    otherwise; the command path's trim never runs again for it.
    ///
    /// An `Err` means the seam condemned the connection (it has already released
    /// the buffer, logged and counted); the caller drops the connection.
    pub(super) fn on_idle_tick(&mut self) -> std::io::Result<()> {
        self.account_buffered_output()?;
        // A connection that produced nothing since the last stats sync is idle
        // by the same definition `maybe_sync_stats` uses.
        let busy = self.state.local_stats.has_data();
        self.trim_idle_buffers(busy);
        Ok(())
    }

    /// Hand oversized read/write buffers back to this core's pool.
    ///
    /// A connection that once served a megabyte reply otherwise keeps a
    /// megabyte for the rest of its session — times every idle connection, which
    /// is the retention this pool exists to stop. Only *empty* buffers are
    /// swapped: a buffer with bytes in it is mid-conversation, and there is
    /// nothing to reclaim from it.
    ///
    /// `busy` says whether *this connection* did anything since the last tick,
    /// and the core-wide sweep rides an idle tick only. That is a proxy for the
    /// core being idle, not a guarantee — a busy neighbour on the same core can
    /// still have its free lists trimmed; see the comment at the `sweep()` call
    /// for why that is accepted.
    pub(super) fn trim_idle_buffers(&mut self, busy: bool) {
        use frogdb_net::buffers;

        /// What an idle connection's *reply* buffers are re-leased down to — the
        /// pool's smallest class, which is also a sensible size for the next
        /// reply.
        const IDLE_TARGET: usize = buffers::MIN_CLASS_BYTES;

        fn shrink(buf: &mut bytes::BytesMut, target: usize) {
            if buf.is_empty() && buf.capacity() > target {
                buffers::recycle(buf, target);
            }
        }

        shrink(self.framed.read_buffer_mut(), READ_IDLE_TARGET);
        shrink(self.framed.write_buffer_mut(), IDLE_TARGET);
        shrink(&mut self.resp3_buf, IDLE_TARGET);

        if !busy {
            // Per-connection idleness is a proxy for the core's: on a mixed
            // core, one idle connection's tick trims free lists a busy
            // neighbour is about to lease from. Accepted deliberately: `sweep`
            // only trims down to `CLASS_LOW_WATER` — it never empties a class —
            // so the neighbour still leases warm buffers, and the cost is a
            // bounded low-to-high-water refill, cheaper than plumbing a
            // core-wide busy signal across connections.
            buffers::sweep();
        }
    }

    /// Hand this connection's buffers to the core's pool on the way out.
    ///
    /// The pool's whole point is that a core's buffers outlive the connections
    /// that used them: without this, every close frees allocations the next
    /// accept immediately asks for again. The pool decides what it can actually
    /// park — an odd capacity, or one still shared with an outstanding slice, is
    /// freed there rather than mislabelled.
    pub(super) fn release_buffers_to_pool(&mut self) {
        use frogdb_net::buffers;

        buffers::release(self.framed.read_buffer_mut());
        buffers::release(self.framed.write_buffer_mut());
        buffers::release(&mut self.resp3_buf);
    }

    /// Compute the current memory usage of this connection.
    pub(crate) fn compute_client_memory(&mut self) -> ClientMemoryUsage {
        // Query buffer: access inner BytesMut length from Framed codec
        let query_buf_size = self.framed.read_buffer().len();
        // High-water mark over sampled memory syncs — real peak of what we've
        // actually observed, not a fabricated capacity figure. Backs CLIENT
        // INFO/LIST's `rbp` field.
        self.state.query_buf_peak = self.state.query_buf_peak.max(query_buf_size);
        let query_buf_peak = self.state.query_buf_peak;

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

        // Output buffer — `obl`. Both protocol versions buffer (RESP2 in the
        // codec's write buffer, RESP3 in `resp3_buf`), so this is the whole of
        // what is queued for the client right now.
        let output_buf_len = self.framed.write_buffer().len() + self.resp3_buf.len();

        // Output list — `oll` / `omem`. FrogDB has no Redis-style reply *list*:
        // buffered output is one contiguous buffer, so there are no list nodes
        // to count and `oll` stays 0. `omem` is the figure operators actually
        // use — "how much memory is this client costing me in queued output" —
        // and it is the same number the `NetworkOutput` budget is charged and
        // the `client-output-buffer-limit` decision is made on. Reporting it
        // from the account rather than recomputing it is what keeps `CLIENT
        // INFO` honest about the limit that will kill the connection.
        let output_list_len = 0;
        let output_list_mem = self.output_buffer.buffered_bytes() as usize;

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
            query_buf_peak,
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
    /// `cmd_args` is the raw argument list for the command. It feeds two
    /// decisions: which hash slot the command is pinned to (for slot-scoped
    /// pauses) and whether an EVAL/EVALSHA script body carries a
    /// `#!lua flags=no-writes` shebang.
    ///
    /// Two pause dimensions compose here. The node-global `CLIENT PAUSE` applies
    /// to every command, exactly as it always has. A slot-scoped pause — what the
    /// slot-migration finalization barrier arms — applies only to commands that
    /// can reach its slot, and never to the handover's own control and copy
    /// traffic (see
    /// [`exempt_from_slot_pause`](crate::connection::pause_gate::exempt_from_slot_pause)).
    /// When both cover a command, the stronger mode wins.
    pub(crate) fn should_pause_command(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) -> bool {
        // Certain commands are always exempt from pause
        let is_exempt = matches!(
            cmd_name,
            "CLIENT" | "PING" | "QUIT" | "RESET" | "INFO" | "CONFIG" | "DEBUG" | "SLOWLOG"
        );

        if is_exempt {
            return false;
        }

        // One lock for the common "nothing is paused" answer; the command's key
        // set is only resolved to a slot when a slot-scoped pause actually exists.
        let overview = self.admin.client_registry.pause_overview();
        if !overview.is_active() {
            return false;
        }
        let slot_mode = self.slot_pause_mode(cmd_name, cmd_args, &overview);

        match PauseMode::strongest(overview.node, slot_mode) {
            Some(mode) => self.command_matches_pause_mode(cmd_name, cmd_args, mode),
            None => false,
        }
    }

    /// Resolves the slot-scoped pause mode covering `cmd_name`, if any —
    /// the migration finalization barrier's half of [`should_pause_command`].
    /// Split out so [`slot_pause_blocks_command`](Self::slot_pause_blocks_command)
    /// can ask the slot-scoped question alone, without the node-global
    /// `CLIENT PAUSE` dimension that dilutes `should_pause_command`'s answer.
    fn slot_pause_mode(
        &self,
        cmd_name: &str,
        cmd_args: &[bytes::Bytes],
        overview: &frogdb_core::PauseOverview,
    ) -> Option<PauseMode> {
        if overview.slot_scoped && !pause_gate::exempt_from_slot_pause(cmd_name) {
            let slot = pause_gate::command_pause_slot(&self.core.registry, cmd_name, cmd_args);
            self.admin.client_registry.slot_pause(slot)
        } else {
            None
        }
    }

    /// Whether the *slot-scoped* pause alone — the migration finalization
    /// barrier, never a node-global `CLIENT PAUSE` — currently holds
    /// `cmd_name` back.
    ///
    /// Blocking-capable commands bypass `CLIENT PAUSE` (spec-gaps issue 17 /
    /// distsys-review MAJ-14: pause gates execution, not parking), but that
    /// ruling is scoped to the admin-facing pause command, not the
    /// migration barrier a slot-scoped pause implements — letting a
    /// blocking command's synchronous immediate-pop path write straight
    /// through an armed handoff barrier would reopen the acknowledged-write
    /// hazard `ReplicaFeedGate`/FM-CLUSTER-097 close. So the bypass in
    /// [`wait_if_paused`](Self::wait_if_paused) only fires when this
    /// returns `false`.
    fn slot_pause_blocks_command(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) -> bool {
        let overview = self.admin.client_registry.pause_overview();
        if !overview.slot_scoped {
            return false;
        }
        match self.slot_pause_mode(cmd_name, cmd_args, &overview) {
            Some(mode) => self.command_matches_pause_mode(cmd_name, cmd_args, mode),
            None => false,
        }
    }

    /// Whether `cmd_name` is subject to `mode` (`PauseMode::All` covers every
    /// command; `PauseMode::Write` covers writes, scripts conservatively, and
    /// a short list of commands with write-adjacent side effects). Shared by
    /// [`should_pause_command`](Self::should_pause_command) (node-global +
    /// slot-scoped combined) and
    /// [`slot_pause_blocks_command`](Self::slot_pause_blocks_command)
    /// (slot-scoped alone).
    fn command_matches_pause_mode(
        &self,
        cmd_name: &str,
        cmd_args: &[bytes::Bytes],
        mode: PauseMode,
    ) -> bool {
        match mode {
            PauseMode::All => true,
            PauseMode::Write => {
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
        }
    }

    /// Whether a pause covering a batch pinned to `slot` is in force.
    ///
    /// EXEC's gate. Like [`should_pause_command`](Self::should_pause_command) it
    /// composes the two pause dimensions, and like it, it resolves the slot one
    /// only when a slot-scoped pause actually exists — the common answer costs
    /// one lock.
    ///
    /// `slot` is `None` when the batch cannot be pinned to a single hash slot,
    /// and [`ClientRegistry::slot_pause`](frogdb_core::ClientRegistry::slot_pause)
    /// answers that fail-closed with the strongest pause armed on *any* slot: an
    /// unpinnable write batch may reach the barriered slot, and a write that runs
    /// while a barrier is up is precisely the acknowledged-then-orphaned write
    /// the barrier exists to prevent.
    ///
    /// Coarser than `should_pause_command` in one respect that is deliberate:
    /// mode is not consulted. EXEC only asks once it knows the batch contains
    /// writes, and both `PauseMode::All` and `PauseMode::Write` park a write.
    fn pause_active_for_batch(&self, slot: Option<u16>) -> bool {
        let overview = self.admin.client_registry.pause_overview();
        if overview.node.is_some() {
            return true;
        }
        if !overview.slot_scoped {
            return false;
        }
        self.admin.client_registry.slot_pause(slot).is_some()
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

    /// Whether `cmd_name` is a blocking-capable command (BLPOP, BRPOP,
    /// BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, BLMPOP, BZMPOP, XREAD,
    /// XREADGROUP) per its registered [`ExecutionStrategy`]. This is the
    /// canonical, complete predicate — unlike `CommandFlags::BLOCKING`
    /// (XREAD/XREADGROUP never carry it) or a hardcoded name list.
    fn is_blocking_capable_command(&self, cmd_name: &str) -> bool {
        self.core
            .registry
            .get_entry(cmd_name)
            .is_some_and(|e| matches!(e.execution_strategy(), ExecutionStrategy::Blocking { .. }))
    }

    /// Wait if the server is paused (CLIENT PAUSE).
    /// This queues commands (not drops them) by blocking until pause ends.
    /// Marks the client as PAUSED so `wait_for_blocked_clients` can observe it
    /// and CLIENT UNBLOCK correctly rejects unblocking.
    ///
    /// Called from `route_and_execute_with_transaction` after transaction-control
    /// dispatch and transaction queuing, so it only blocks commands outside MULTI.
    ///
    /// Blocking-capable commands bypass the node-global `CLIENT PAUSE`
    /// dimension of this gate: `CLIENT PAUSE` gates *execution*, not
    /// *parking* (spec-gaps issue 17 / distsys-review MAJ-14). Such a command
    /// registers a real wait-queue entry (`handle_blocking_wait`) whose
    /// deadline starts immediately and whose BLOCKED flag makes it visible in
    /// `blocked_clients`/`CLIENT LIST`, independent of any pause window
    /// (Redis's "Blocking timeout following PAUSE should honor the timeout").
    /// The write that would satisfy such a waiter (e.g. LPUSH) still goes
    /// through this same gate on its own connection, so PAUSE WRITE continues
    /// to hold back new data during the pause — only the *waiter's* deadline
    /// is pause-independent.
    ///
    /// What the bypass buys is parking, never popping. A write-flagged
    /// blocking command that finds its data already available does **not**
    /// take the immediate pop while a node-global pause covers it: the shard
    /// refuses the pop at its own decision point and the command parks like
    /// any other waiter (`specs/blocking.md` TR-BLOCKING-026, spec-gaps issue
    /// 30 / ruling R9). That check cannot live here — this side of the seam
    /// does not know whether there is data to pop, and could only find out by
    /// racing the very writes the pause is draining.
    ///
    /// The bypass does *not* extend to the slot-scoped pause the migration
    /// finalization barrier arms (`slot_pause_blocks_command`), which is a
    /// per-slot dimension the shard-side node-global gate above deliberately
    /// does not see: holding the command here keeps a blocking command's pop
    /// from crossing an armed handoff barrier and reopening the
    /// acknowledged-write hazard `ReplicaFeedGate` closes.
    pub(crate) async fn wait_if_paused(&self, cmd_name: &str, cmd_args: &[bytes::Bytes]) {
        if self.is_blocking_capable_command(cmd_name)
            && !self.slot_pause_blocks_command(cmd_name, cmd_args)
        {
            return;
        }
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

    /// Wait if a pause covering this batch is in force, for a write-containing
    /// transaction (EXEC). Both PAUSE ALL and PAUSE WRITE block write
    /// transactions.
    ///
    /// `slot` is the hash slot the whole batch pins to
    /// ([`queue_pause_slot`](crate::connection::pause_gate::queue_pause_slot)),
    /// or `None` when it cannot be pinned — see
    /// [`pause_active_for_batch`](Self::pause_active_for_batch) for what that
    /// means. A slot-scoped migration barrier therefore parks only the write
    /// transactions that can reach its slot, instead of every write transaction
    /// on the node.
    ///
    /// Returns `true` if the call actually blocked. EXEC uses that to decide
    /// whether its pre-pause cluster-slot verdict is still fresh: nothing else
    /// in the EXEC path can take unbounded wall-clock time, so an unblocked
    /// call means the snapshot cannot have gone stale in between.
    pub(crate) async fn wait_if_paused_for_transaction(&self, slot: Option<u16>) -> bool {
        if !self.pause_active_for_batch(slot) {
            return false;
        }

        // Mark client as paused/blocked
        self.admin
            .client_registry
            .update_paused_state(self.state.id, true);

        // Wait until the pause covering this batch ends. Re-asked against the
        // same slot each poll, so a barrier released on our slot frees us even
        // while barriers on other slots stay armed.
        loop {
            if !self.pause_active_for_batch(slot) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clear paused state
        self.admin
            .client_registry
            .update_paused_state(self.state.id, false);
        true
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

/// The housekeeping tick, over a real connection.
///
/// What the unit tests in [`output_buffer`](super::output_buffer) cannot show is
/// that anything *calls* the seam when a connection is doing nothing — which is
/// exactly when a client that has stopped reading must be judged. These build a
/// real `ConnectionHandler` over a loopback socket and drive `on_idle_tick`
/// directly: the function the `select!` loop's timer arm calls.
///
/// # Why the soft limit is not forced end to end
///
/// It cannot be, over a socket. The reply path applies backpressure at the
/// codec's boundary instead of accumulating, so the only way to hold a
/// connection above a soft mark is a client that has stopped reading — and such
/// a connection is parked inside `write_all`, where no `select!` arm runs at
/// all, this tick included. When the client does read, the buffers drain in the
/// same breath. Forcing the window end to end would need a non-blocking
/// feed-and-poll write path, which is a different change. So it is forced here,
/// at the seam the timer actually drives.
#[cfg(all(test, not(feature = "turmoil")))]
mod idle_tick_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use frogdb_core::{
        AclManager, ClientRegistry, CommandRegistry, NoopSnapshotCoordinator, ShardSender,
        SharedFunctionRegistry, persistence::SnapshotCoordinator,
    };
    use frogdb_net::buffers;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    use crate::connection::ConnectionHandler;
    use crate::connection::builder::standalone_config;
    use crate::connection::deps::{AdminDeps, ClusterDeps, CoreDeps, ObservabilityDeps};
    use crate::connection::output_buffer::{OutputBufferLimit, OutputBufferLimits};
    use crate::runtime_config::ConfigManager;

    /// A connection handler over a real loopback socket, with `limits` in force.
    ///
    /// The peer end and the shard receiver come back with it: dropping either
    /// would make the connection fail for a reason that has nothing to do with
    /// what is under test.
    async fn handler_with_limits(
        limits: OutputBufferLimits,
    ) -> (
        ConnectionHandler,
        TcpStream,
        mpsc::Receiver<frogdb_core::shard::Envelope>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let peer = TcpStream::connect(addr).await.expect("connect");
        let (server_side, client_addr) = listener.accept().await.expect("accept");

        let (shard_tx, shard_rx) = mpsc::channel(1);
        let client_registry = Arc::new(ClientRegistry::new());
        let client_handle = client_registry.register(1, client_addr, None);
        let snapshot_coordinator: Arc<dyn SnapshotCoordinator> =
            Arc::new(NoopSnapshotCoordinator::new());

        let mut config = standalone_config(1);
        config.output_buffer_limits = limits;

        let handler = ConnectionHandler::from_deps(
            crate::tls::MaybeTlsStream::Plain { inner: server_side },
            client_addr,
            1,
            0,
            client_handle,
            CoreDeps {
                registry: Arc::new(CommandRegistry::new()),
                shard_senders: Arc::new(vec![ShardSender::new(shard_tx)]),
                acl_manager: AclManager::new(Default::default()),
                txn_budgets: crate::connection::deps::unbounded_txn_budgets(1),
            },
            AdminDeps {
                client_registry,
                config_manager: Arc::new(ConfigManager::new(&crate::config::Config::default())),
                snapshot_coordinator,
                function_registry: SharedFunctionRegistry::default(),
                cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
                recovery_stats: Default::default(),
            },
            ClusterDeps::default(),
            config,
            ObservabilityDeps::default(),
        );
        (handler, peer, shard_rx)
    }

    /// Output that stays above the soft mark across ticks is shed once the
    /// window has elapsed — and not before.
    ///
    /// This is the finding the tick exists for: with nothing calling the seam,
    /// the soft limit was unreachable however long a client stayed behind.
    // FM-MEMORY-001
    #[tokio::test]
    async fn the_idle_tick_sheds_a_connection_that_stays_above_the_soft_mark() {
        let limits = OutputBufferLimits {
            normal: OutputBufferLimit {
                hard_bytes: 0,
                soft_bytes: 1024,
                soft_seconds: 1,
            },
            ..Default::default()
        };
        let (mut handler, _peer, _shard_rx) = handler_with_limits(limits).await;

        // Buffered output a flush would not clear: bytes staged for a client
        // that is not reading, in the RESP3 staging buffer the seam measures.
        handler.resp3_buf.extend_from_slice(&vec![b'x'; 4096]);

        // The first tick opens the window rather than shedding: a burst that
        // drains promptly must not be a kill.
        handler
            .on_idle_tick()
            .expect("a connection inside its window is kept");

        // The window is judged against the clock, so wait it out rather than
        // reaching behind the seam.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert!(
            handler.on_idle_tick().is_err(),
            "output above the soft mark for longer than soft-seconds must be shed"
        );
        assert_eq!(
            handler.resp3_buf.len(),
            0,
            "a shed connection's buffered output is discarded, not written"
        );
    }

    /// A connection whose buffers grew for one big reply gives that capacity
    /// back once it goes idle, rather than holding it for the whole session.
    #[tokio::test]
    async fn an_idle_connection_decays_its_buffers_to_the_pool() {
        let (mut handler, _peer, _shard_rx) =
            handler_with_limits(OutputBufferLimits::default()).await;

        // A megabyte of reply staging, as a large GET would leave behind, and
        // then drained: empty, but still holding its capacity.
        handler.resp3_buf.reserve(buffers::MAX_CLASS_BYTES);
        let grown = handler.resp3_buf.capacity();
        assert!(grown >= buffers::MAX_CLASS_BYTES);

        handler.on_idle_tick().expect("an empty buffer is not shed");

        assert_eq!(
            handler.resp3_buf.capacity(),
            buffers::MIN_CLASS_BYTES,
            "an idle connection must decay to the pool's smallest class; still holding {}",
            handler.resp3_buf.capacity()
        );
    }

    /// Closing hands the buffers to this core's pool, where the next accept can
    /// lease them, instead of freeing them back to the allocator.
    #[tokio::test]
    async fn a_closing_connection_returns_its_buffers_to_the_pool() {
        let (mut handler, _peer, _shard_rx) =
            handler_with_limits(OutputBufferLimits::default()).await;
        handler.resp3_buf.reserve(buffers::MIN_CLASS_BYTES);

        let before = buffers::with_pool(|pool| pool.parked_bytes()).expect("pool");
        handler.release_buffers_to_pool();
        let after = buffers::with_pool(|pool| pool.parked_bytes()).expect("pool");

        assert!(
            after > before,
            "a closing connection's buffers must land in the pool ({before} -> {after})"
        );
        assert_eq!(
            handler.resp3_buf.capacity(),
            0,
            "and it must not lease a replacement on its way out"
        );
    }

    /// A connection shed at the tick leaves nothing charged behind it.
    // FM-MEMORY-002
    #[tokio::test]
    async fn a_shed_connection_releases_its_charge() {
        let limits = OutputBufferLimits {
            normal: OutputBufferLimit {
                hard_bytes: 1024,
                soft_bytes: 0,
                soft_seconds: 0,
            },
            ..Default::default()
        };
        let (mut handler, _peer, _shard_rx) = handler_with_limits(limits).await;

        let budget = frogdb_memory::network_output::current();
        let before = budget.charged();

        handler.resp3_buf.extend_from_slice(&vec![b'x'; 4096]);
        assert!(
            handler.on_idle_tick().is_err(),
            "output past the hard limit is shed at the tick too"
        );
        assert_eq!(
            budget.charged(),
            before,
            "the shed connection's bytes are released from the budget"
        );
    }
}
