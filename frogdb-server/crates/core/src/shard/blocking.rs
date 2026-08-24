use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use tokio::sync::oneshot;
use tokio::time::Instant;

use frogdb_types::metrics::definitions::{
    BlockedClients, BlockedMigrationMoved, BlockedSatisfiedTotal, BlockedTimeoutTotal,
};

use crate::command::{SynthesizedCommand, WaiterKind};
use crate::keyspace_event::KeyspaceEventFlags;
use crate::store::{HashMapStore, Store};
use crate::types::{BlockingOp, Direction, StreamEntry, Value};

use super::helpers::format_xread_response;
use super::message::UnregisterAck;
use super::wait_queue::WaitEntry;
use super::worker::ShardWorker;

/// Maximum depth for recursive BLMove/BRPOPLPUSH wake chains.
///
/// Each hop consumes one list element, so a chain naturally terminates when
/// the source list becomes empty. This cap is a safety net against pathological
/// graph topologies (e.g. long fan-out chains). Waiters beyond the cap will
/// be woken on the next write to the chain head.
const MAX_BLMOVE_FANOUT_DEPTH: usize = 16;

/// The write a post-pause satisfaction pass performed on one key, as a record
/// the canonical effect pipeline can run (`specs/blocking.md`
/// TR-BLOCKING-026).
///
/// `resume_pops_deferred_by_pause` serves pops that no enclosing write drove,
/// so there is no real command to hang their effects off — this stands in for
/// one, the way the synthetic `DEL` does for an engine removal
/// (`ShardWorker::run_internal_removal_effects`). Unlike that one it cannot be
/// resolved from the registry: no registered command has this combination of
/// declarations, and it must never be dispatchable from the wire.
///
/// Every field is load-bearing:
///
/// - [`CommandFlags::WRITE`] plus [`CommandFlags::NO_PROPAGATE`]: the pop is a
///   write and owes the full local effect set, but it must not replicate
///   *itself* — the satisfaction driver already recorded the deterministic pop
///   commands in `pending_serve_propagations`, and the broadcast effect ships
///   those.
/// - [`KeySpec::All`]: the record carries exactly the one served key, which is
///   what the version bump and the tracking invalidation address.
/// - [`WalStrategy::PersistOrDeleteFirstKey`]: a pop either leaves the
///   collection smaller or empties and deletes it — the same strategy `LPOP`
///   and `ZPOPMIN` declare, and type-agnostic, so one record serves list, zset
///   and stream keys alike.
/// - [`WaiterWake::None`]: the satisfaction pass already ran; re-entering it
///   from inside the pipeline would drive the same queue twice.
/// - [`EventSpec::Suppressed`]: the driver publishes the pop's keyspace events
///   itself, at the moment of the pop, exactly as on the write-woken path.
/// - `reindex: None`: lists, sorted sets and streams are never search-indexed.
struct PausedPopServe;

impl crate::command::Command for PausedPopServe {
    fn spec(&self) -> &'static crate::CommandSpec {
        static SPEC: crate::CommandSpec = crate::CommandSpec {
            name: "BLOCKING-SERVE",
            docs: crate::CommandDocs {
                summary: "Internal: the store mutation a post-pause blocking-waiter wake performed.",
                since: "0.0.0",
                group: "generic",
                complexity: Some("O(1)"),
            },
            arity: crate::Arity::Fixed(1),
            flags: crate::CommandFlags::WRITE
                .union(crate::CommandFlags::NO_PROPAGATE),
            keys: crate::KeySpec::All,
            access: crate::AccessSpec::UniformRW,
            wal: crate::WalStrategy::PersistOrDeleteFirstKey,
            wakes: crate::WaiterWake::None,
            event: crate::EventSpec::Suppressed,
            requires_same_slot: false,
            reindex: crate::ReindexSpec::None,
            lookup: crate::LookupSpec::None,
            mutation: crate::ConnMutation::None,
            strategy: crate::ExecutionStrategy::Standard,
        };
        &SPEC
    }

    fn execute(
        &self,
        _ctx: &mut crate::command::CommandContext,
        _args: &[Bytes],
    ) -> Result<Response, frogdb_types::CommandError> {
        // Effect-only: this command is never registered and never dispatched.
        // The mutation it stands for was performed by the satisfaction driver
        // before the effect pipeline ran.
        unreachable!("PausedPopServe is an effect record, never an executable command")
    }
}

impl ShardWorker {
    /// Handle a blocking wait request.
    pub(crate) fn handle_block_wait(
        &mut self,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        response_tx: oneshot::Sender<Response>,
        deadline: Option<Instant>,
        protocol_version: ProtocolVersion,
    ) {
        let keys_count = keys.len();
        let entry = WaitEntry {
            conn_id,
            keys,
            op,
            response_tx,
            deadline,
            protocol_version,
        };

        if let Err(refused) = self.wait_queue.register(entry) {
            tracing::warn!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                error = refused.message,
                "Refused blocking wait registration at the admission limit"
            );
            // The queue hands the entry back precisely so the refusal reaches
            // the client. Dropping `response_tx` here would surface as
            // `-ERR shard unavailable`, which is reserved for shard death
            // (`specs/blocking.md` FM-BLOCKING-006, FM-BLOCKING-004).
            let _ = refused
                .entry
                .response_tx
                .send(Response::error(refused.message));
        } else {
            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id,
                keys_count,
                "Client blocked on keys"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id().to_string();
            BlockedClients::set(
                self.observability.metrics(),
                self.wait_queue.waiter_count() as f64,
                &shard_label,
            );
        }
    }

    /// Handle unregistering a blocking wait (timeout, CLIENT UNBLOCK, or
    /// disconnect), acknowledging the serve-vs-timeout race.
    ///
    /// Runs on the shard's serial timeline, so whether the waiter is still
    /// registered here is authoritative. If it is (`removed` non-empty) the
    /// timeout won: remove it and report [`UnregisterAck::Unregistered`]. If it
    /// is already gone a serve or the GC tick beat the timeout and has sent a
    /// response on the client's channel: report [`UnregisterAck::AlreadyServed`]
    /// so the client drains that value instead of discarding it. See
    /// [`BlockingMsg::UnregisterWait`].
    pub(crate) fn handle_unregister_wait(
        &mut self,
        conn_id: u64,
        ack: oneshot::Sender<UnregisterAck>,
    ) {
        let removed = self.wait_queue.unregister(conn_id);
        let reply = if removed.is_empty() {
            UnregisterAck::AlreadyServed
        } else {
            tracing::trace!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                count = removed.len(),
                "Unregistered blocking waits"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id().to_string();
            BlockedClients::set(
                self.observability.metrics(),
                self.wait_queue.waiter_count() as f64,
                &shard_label,
            );
            UnregisterAck::Unregistered
        };
        let _ = ack.send(reply);
    }

    /// Handle a slot migration completion by sending `-MOVED` to all blocked clients
    /// waiting on keys in the migrated slot — or `-CLUSTERDOWN` when the notifier
    /// could not name the new owner (`target_addr: None`), which is the same
    /// rendering routing uses for "owner known, address unknown".
    pub(crate) fn handle_slot_migrated(
        &mut self,
        slot: u16,
        target_addr: Option<std::net::SocketAddr>,
    ) {
        let drained = self.wait_queue.drain_waiters_for_slot(slot);

        if drained.is_empty() {
            return;
        }

        let shard_label = self.shard_id().to_string();
        let moved_count = drained.len();

        for entry in drained {
            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id = entry.conn_id,
                slot,
                target_addr = ?target_addr,
                "Waking blocked client after slot migration"
            );

            // Route through the shared redirect seam so the address is rendered
            // once, bracketing IPv6 (`MOVED <slot> [<v6>]:<port>`). The inline
            // `ip():port()` form joined with a bare colon was unparseable for
            // IPv6 targets.
            let reply = match target_addr {
                Some(addr) => frogdb_types::redirect::moved(slot, addr),
                None => frogdb_types::redirect::clusterdown_slot(slot),
            };
            let _ = entry.response_tx.send(reply);
        }

        // The counter names the `-MOVED` redirect, so only those count: a
        // `-CLUSTERDOWN` wake-up is under-reported rather than reported as
        // something it is not (it is loud in the dispatcher's logs instead).
        if target_addr.is_some() {
            BlockedMigrationMoved::inc_by(
                self.observability.metrics(),
                moved_count as u64,
                &shard_label,
            );
        }

        BlockedClients::set(
            self.observability.metrics(),
            self.wait_queue.waiter_count() as f64,
            &shard_label,
        );
    }

    /// Release every parked waiter because this node is no longer a primary.
    ///
    /// Sent by `RoleManager::demote` through the blocked-waiter fence, ahead of
    /// the inbound replication stream being started. A waiter served after the
    /// demotion would be a local write on a replica, diverging its store from
    /// the stream it is applying; a waiter left parked would sit until its own
    /// timeout waiting for a push that can only arrive as replicated data. Both
    /// are answered by draining the queue here — `specs/blocking.md`
    /// FM-BLOCKING-007.
    pub(crate) fn handle_release_all_waiters(&mut self) {
        let drained = self.wait_queue.drain_all();

        if drained.is_empty() {
            return;
        }

        tracing::debug!(
            shard_id = self.shard_id(),
            count = drained.len(),
            "Releasing blocked clients after demotion"
        );

        for entry in drained {
            let _ = entry
                .response_tx
                .send(Response::error(crate::ROLE_CHANGED_UNBLOCK_ERR));
        }

        let shard_label = self.shard_id().to_string();
        BlockedClients::set(
            self.observability.metrics(),
            self.wait_queue.waiter_count() as f64,
            &shard_label,
        );
    }

    /// Coarse safety-net for expired blocking waits.
    ///
    /// The server-side `BlockingWaitCoordinator` is the *canonical* timeout
    /// authority: it fires precisely at the deadline, replies to the client, and
    /// sends `UnregisterWait`. This shard-side tick (every ~100ms) only garbage-
    /// collects entries the server has not yet unregistered. By the time it runs
    /// the server has already replied, so the response channel is dropped and
    /// the send below is a no-op; it carries the op-aware nil purely so the two
    /// authorities can never disagree on the wire shape. Crucially this tick
    /// never *consumes* store data, so it cannot lose an element — the
    /// lost-element race is closed in the satisfaction path, which re-validates a
    /// waiter's deadline before popping (see `drive_satisfaction`).
    pub(crate) fn check_waiter_timeouts(&mut self) {
        let now = Instant::now();
        let expired = self.wait_queue.collect_expired(now);

        if !expired.is_empty() {
            let shard_label = self.shard_id().to_string();

            for entry in expired {
                tracing::trace!(
                    shard_id = self.shard_id(),
                    conn_id = entry.conn_id,
                    "Blocking wait timed out"
                );

                // Send the op-aware nil for timeout (no-op if the server already
                // replied and dropped the receiver).
                let _ = entry.response_tx.send(entry.op.timeout_reply());

                // Increment timeout counter
                BlockedTimeoutTotal::inc(self.observability.metrics(), &shard_label);
            }

            // Update blocked clients gauge
            BlockedClients::set(
                self.observability.metrics(),
                self.wait_queue.waiter_count() as f64,
                &shard_label,
            );
        }
    }

    /// Try to satisfy list waiters after a list write operation.
    ///
    /// Called after LPUSH, RPUSH, LPUSHX, RPUSHX, BLMOVE, BRPOPLPUSH operations.
    pub fn try_satisfy_list_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut ListSatisfaction, key);
    }

    /// Try to satisfy sorted set waiters after a sorted set write operation.
    ///
    /// Called after ZADD operations.
    pub fn try_satisfy_zset_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut ZsetSatisfaction, key);
    }

    /// Try to satisfy stream waiters after a stream write operation.
    ///
    /// Called after XADD, DEL, UNLINK, SET, XGROUP DESTROY, and RENAME operations.
    pub fn try_satisfy_stream_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut StreamSatisfaction, key);
    }

    /// Drive waiter satisfaction for `key`, then apply the effects of any lazy
    /// purge the strategies triggered.
    ///
    /// Each [`WaiterSatisfaction::check_key`] impl calls
    /// [`crate::store::Store::purge_if_expired`] so a blocker woken by a write
    /// never observes a stale just-expired value; that populates the store's
    /// `lazily_purged` buffer. This wrapper drains it through
    /// [`Self::apply_lazy_purge_effects`], so a key that dies on the blocking
    /// wake path gets the same externally observable effects (a shard-version
    /// bump + an XREADGROUP → NOGROUP drain) as any other lazy purge — the
    /// blocking-path counterpart of the [`Self::execute_scatter_part`] and
    /// `execute_command_inner` seams. Without it the report would survive into
    /// the *next* message and be applied at the wrong seam (issue 08).
    ///
    /// The drain runs **after** [`Self::drive_satisfaction_body`] and its BLMove
    /// wake-cascade have fully unwound — one drain point covering all three
    /// `check_key` impls — so it never reenters the wait queue while the driver
    /// is still iterating it (`apply_lazy_purge_effects` →
    /// [`Self::drain_stream_waiters_with_error`] pops the same queue). This is
    /// why the cascade recurses through the body, not this wrapper: effects
    /// drain exactly once for the whole wake chain.
    fn drive_satisfaction(&mut self, strat: &mut dyn WaiterSatisfaction, key: &Bytes) {
        self.drive_satisfaction_body(strat, key, 0);
        self.apply_lazy_purge_effects();
    }

    /// Generic satisfaction driver shared by every waiter kind (the body proper;
    /// see [`Self::drive_satisfaction`], which wraps this to drain lazy-purge
    /// effects afterward).
    ///
    /// Owns everything that is the *same* across the families — the FIFO loop,
    /// the BLMove wake-cascade recursion, the depth cap, the version bump, the
    /// timeout re-validation, and the completion/metrics — while the per-op "is
    /// this key satisfiable / what reply does this op produce / where does the
    /// wake cascade" logic lives behind the [`WaiterSatisfaction`] seam. The
    /// strategy sees only the store; this driver is the sole mutator of the wait
    /// queue.
    fn drive_satisfaction_body(
        &mut self,
        strat: &mut dyn WaiterSatisfaction,
        key: &Bytes,
        depth: usize,
    ) {
        if depth >= MAX_BLMOVE_FANOUT_DEPTH {
            tracing::warn!(
                shard_id = self.shard_id(),
                key = %String::from_utf8_lossy(key),
                depth,
                "BLMove fan-out depth cap hit; remaining blockers will wake on next write"
            );
            return;
        }

        let kind = strat.kind();
        // One clock reading for the whole satisfaction pass. Re-sampling per iteration made
        // the deadline fast-path below depend on how long the *earlier* iterations took, so
        // two runs that differ only in scheduling could skip a different set of waiters.
        let now = Instant::now();
        // Waiters this pass popped but could not satisfy (TR-BLOCKING-023).
        // They cannot go back into the queue inside the loop — the key still
        // looks ready, so the loop would pop the same waiter forever — and they
        // must go back *before* any drain below, or a drain would leave them
        // parked on a key it just declared unusable. Hence: collect here, requeue
        // at the single exit, drain after that.
        let mut retried: Vec<(WaitEntry, u64)> = Vec::new();
        let mut drain: Option<KeyReady> = None;
        while self.wait_queue.has_waiters_for_kind(key, kind) {
            match strat.check_key(&mut self.store, key) {
                KeyReady::No => break,
                ready @ (KeyReady::DrainNoGroup | KeyReady::DrainWrongType) => {
                    drain = Some(ready);
                    break;
                }
                KeyReady::Yes => {}
            }

            let Some((entry, seq)) = self
                .wait_queue
                .pop_oldest_waiter_of_kind_with_seq(key, kind)
            else {
                break;
            };

            // Deadline fast-path. The server is the canonical timeout authority;
            // if a popped waiter's deadline has already elapsed the server has
            // (or is about to) return a timeout nil, so answer with that same
            // op-aware nil without consuming and try the next one. This is a
            // cheap optimization, not the correctness backstop: a receiver can
            // still be dropped in the window *after* this check and *before* the
            // `send` below (the server fires precisely at the deadline). That
            // residual race is closed by restoring the consumed data on send
            // failure — see the `Err` arm and [`Restore`] — so no element is
            // ever popped and delivered to nobody.
            //
            // The reply is *sent*, never signalled by dropping `response_tx`:
            // the coordinator reads a closed channel as shard death
            // (`specs/blocking.md` FM-BLOCKING-004), so a dropped sender here
            // would turn an ordinary timeout into `-ERR shard unavailable`. The
            // send is a no-op when the coordinator's own deadline branch already
            // fired, which is the common case.
            if entry.deadline.is_some_and(|d| d <= now) {
                let shard_label = self.shard_id().to_string();
                let _ = entry.response_tx.send(entry.op.timeout_reply());
                BlockedTimeoutTotal::inc(self.observability.metrics(), &shard_label);
                BlockedClients::set(
                    self.observability.metrics(),
                    self.wait_queue.waiter_count() as f64,
                    &shard_label,
                );
                continue;
            }

            match strat.satisfy(&mut self.store, key, &entry) {
                // The key was ready for the *kind*, but not for this particular
                // waiter's op — a stream waiter whose `after_id` the new entries
                // do not reach, or an XREADGROUP whose new entry an earlier
                // waiter in this same pass already consumed. Nothing was
                // consumed and nothing about the client changed, so it goes on
                // waiting with its original deadline (TR-BLOCKING-023). It must
                // never be answered here: a nil would be a timeout the client
                // never asked for, and dropping the entry would close the
                // channel, which the coordinator reads as shard death
                // (FM-BLOCKING-004).
                Satisfaction::Retry => {
                    retried.push((entry, seq));
                    continue;
                }
                Satisfaction::Reject(reply) => self.complete_blocked_waiter(entry, reply),
                Satisfaction::Done {
                    reply,
                    cascade,
                    events,
                    restore,
                    propagate,
                } => {
                    // Deliver first: `satisfy` already consumed the store data,
                    // so the externally observable effects (version bump,
                    // keyspace events, replication propagation, wake cascade)
                    // must be committed only if the reply actually reaches the
                    // client. If the receiver was dropped in the pop→send race,
                    // restore the consumed data and commit nothing — the element
                    // is neither lost nor double-delivered.
                    match entry.response_tx.send(reply) {
                        Ok(()) => {
                            self.record_blocked_waiter_satisfied();
                            // The one place a satisfaction pass commits store
                            // data to a client; `resume_pops_deferred_by_pause`
                            // reads the delta to decide whether the pass it
                            // just drove produced a write whose effects still
                            // owe the canonical pipeline a run.
                            self.waiters_served_total += 1;
                            if strat.bumps_version() {
                                // The wake mutated `key` (e.g. an element popped
                                // for a BLPOP), so bump only its slot — a watch on
                                // a different-slot key survives.
                                self.bump_version_for_key(key);
                            }
                            // Record the deterministic pop for replication (issue
                            // 02). Only a committed delivery propagates — the
                            // restore arm below undoes the pop, so nothing must
                            // ship there. Pushed *before* the BLMove cascade
                            // recurses so a wake chain replicates in apply order
                            // (each hop's `LMOVE` lands ahead of the next):
                            // replicas must apply push-then-pop. Flushed at the
                            // terminal `ReplicationBroadcast` effect, after the
                            // waking write's own broadcast.
                            if let Some(cmd) = propagate {
                                self.pending_serve_propagations.push(cmd);
                            }
                            // Publish the same keyspace events the immediate
                            // command path deposits. Routed through the
                            // coordinator seam (`emit_keyspace_notification`),
                            // which honours the notify-keyspace-events config
                            // gate; nothing is published when notifications are
                            // disabled.
                            for (key, name, class) in &events {
                                self.emit_keyspace_notification(key, name, *class);
                            }
                            // A BLMove/BRPOPLPUSH pushes to its destination; wake
                            // any blockers on that key so wake chains propagate.
                            if let Some(dest) = cascade {
                                self.drive_satisfaction_body(strat, &dest, depth + 1);
                            }
                        }
                        Err(_) => {
                            self.apply_restore(restore);
                            let shard_label = self.shard_id().to_string();
                            BlockedClients::set(
                                self.observability.metrics(),
                                self.wait_queue.waiter_count() as f64,
                                &shard_label,
                            );
                        }
                    }
                }
            }
        }

        self.requeue_retried_waiters(retried);

        match drain {
            Some(KeyReady::DrainNoGroup) => self.drain_stream_waiters_with_error(key),
            Some(KeyReady::DrainWrongType) => self.drain_stream_waiters_wrongtype(key),
            _ => {}
        }
    }

    /// Put every waiter this satisfaction pass could not satisfy back into the
    /// queue, unchanged (`specs/blocking.md` TR-BLOCKING-023).
    ///
    /// A refusal can only come from an admission bound, and the queue is at most
    /// as full as it was when these waiters were admitted, so it is not expected
    /// — but the entry owns the client's channel, so it is answered with the
    /// refusal text (FM-BLOCKING-006) rather than dropped.
    fn requeue_retried_waiters(&mut self, retried: Vec<(WaitEntry, u64)>) {
        if retried.is_empty() {
            return;
        }
        // Oldest first, so the head-insertions leave the deque in its original
        // order rather than reversing this pass's retries.
        for (entry, seq) in retried.into_iter().rev() {
            if let Err(refused) = self.wait_queue.requeue_retry(entry, seq) {
                self.complete_blocked_waiter(refused.entry, Response::error(refused.message));
            }
        }
        BlockedClients::set(
            self.observability.metrics(),
            self.wait_queue.waiter_count() as f64,
            &self.shard_id().to_string(),
        );
    }

    /// Send a response to a blocked client and record metrics.
    ///
    /// Used by the terminal-reply paths that consume *no* store data (a
    /// `Reject` reply — WRONGTYPE/NOGROUP — and the XREADGROUP drains): a
    /// dropped receiver there loses nothing, so the send result is ignored. The
    /// data-consuming `Done` path does not route through here; it sends inline
    /// so it can restore the consumed element on delivery failure.
    fn complete_blocked_waiter(&self, entry: WaitEntry, response: Response) {
        tracing::debug!(
            shard_id = self.shard_id(),
            conn_id = entry.conn_id,
            "Blocked client unblocked"
        );

        let _ = entry.response_tx.send(response);
        self.record_blocked_waiter_satisfied();
    }

    /// Serve blocking pops that parked only because a node-global `CLIENT
    /// PAUSE` was armed, now that it has lapsed (`specs/blocking.md`
    /// TR-BLOCKING-026).
    ///
    /// A waiter parked by the pause gate is the one kind with no wake coming.
    /// Every other blocked client is woken by the write that makes its key
    /// ready; this one found its key *already* ready and was parked anyway, so
    /// unless some unrelated write happens to land on the same key it would sit
    /// there until its deadline and answer nil with the data still in front of
    /// it. The 100 ms blocking sweep therefore drives the satisfaction pass
    /// itself once the gate lapses. A waiter whose deadline elapsed *during*
    /// the pause still times out here — the deadline runs through the pause,
    /// which is exactly issue 17's ruled deviation.
    ///
    /// The pass runs *before* the effect pipeline rather than inside it,
    /// mirroring active expiry (`run_active_expiry` deletes, then
    /// `run_internal_removal_effects` runs the canonical effects for what it
    /// deleted). Driving the pipeline first with a `WaiterWake::All` record
    /// would run a WATCH version bump, a dirty increment and a WAL persist for
    /// every parked key on every sweep — including the keys nothing was served
    /// from, which is an observable write that never happened. So: satisfy
    /// first, then run the canonical effects for exactly the keys a waiter was
    /// actually served from. The satisfaction driver publishes its own version
    /// bump and keyspace notifications at the moment of the pop, exactly as it
    /// does when a write wakes it; what the pipeline adds is everything the
    /// *enclosing* write would otherwise have contributed — tracking
    /// invalidation, the dirty counter, WAL persistence and the flush of
    /// `pending_serve_propagations` to replicas. Without that last part a pop
    /// served here would be delivered to the client, never persisted, and
    /// never replicated: acknowledged-write loss on the next restart.
    pub(crate) async fn resume_pops_deferred_by_pause(&mut self) {
        if !self.pops_deferred_by_pause || self.node_write_pause.active() {
            return;
        }
        // One shot per pause window. Whatever is still parked after this pass
        // is parked for an ordinary reason — its key is not ready — and is back
        // under the ordinary rule that a write wakes it.
        self.pops_deferred_by_pause = false;

        // Keys a waiter was actually served store data from, in the order the
        // passes ran (sorted — see `ShardWaitQueue::waiting_keys`).
        let mut served: Vec<Vec<Bytes>> = Vec::new();
        for key in self.wait_queue.waiting_keys() {
            let before = self.waiters_served_total;
            // A key's parked waiters are not indexed by kind here, and one key
            // can carry waiters of several kinds; each driver is a no-op when
            // the key has none of its own.
            self.try_satisfy_list_waiters(&key);
            self.try_satisfy_zset_waiters(&key);
            self.try_satisfy_stream_waiters(&key);
            if self.waiters_served_total > before {
                served.push(vec![key]);
            }
        }
        if served.is_empty() {
            return;
        }

        // One synthetic write record per served key: `PersistOrDeleteFirstKey`
        // addresses exactly one key, and the keys are independent writes rather
        // than one transaction, so the scatter scope (which replicates each
        // record on its own, with no MULTI/EXEC wrap) is the right framing.
        let dirty_delta = served.len() as i64;
        let handler = &PausedPopServe as &dyn crate::command::Command;
        let write_refs: Vec<crate::command::WriteRecord<'_>> = served
            .iter()
            .map(|args| crate::command::WriteRecord::new(handler, args.as_slice()))
            .collect();
        self.run_write_effects(
            super::post_execution::WriteSummary {
                writes: &write_refs,
                dirty_delta,
                // Not attributable to any one client — several blocked clients
                // may have been served in one pass — so the engine identity,
                // which invalidates every tracking client (none of them wrote
                // this) and leaves the replication broadcast enabled.
                conn_id: super::post_execution::ENGINE_INTERNAL_CONN_ID,
                removal_reasons: &[],
            },
            super::post_execution::WalPhase::Persist,
            super::post_execution::EffectScope::ScatterPart,
        )
        .await;
    }

    /// Record the metrics for one satisfied waiter (satisfied counter + blocked
    /// gauge).
    fn record_blocked_waiter_satisfied(&self) {
        let shard_label = self.shard_id().to_string();
        BlockedSatisfiedTotal::inc(self.observability.metrics(), &shard_label);
        BlockedClients::set(
            self.observability.metrics(),
            self.wait_queue.waiter_count() as f64,
            &shard_label,
        );
    }

    /// Put back store data a wake consumed when its reply could not be delivered
    /// (the receiver was dropped in the pop→send race). Restores exact ordering,
    /// recreating the key if the wake had emptied and deleted it. No version bump
    /// or keyspace event fires — from the outside, the wake never happened.
    fn apply_restore(&mut self, restore: Restore) {
        match restore {
            Restore::None => {}
            Restore::List { key, dir, elems } => {
                if self.store.get(&key).is_none() {
                    self.store.set(key.clone(), Value::list());
                }
                if let Some(list) = self.store.get_mut(&key).and_then(|v| v.as_list_mut()) {
                    // `elems` is in pop order; re-insert at the same end in
                    // reverse to reconstruct the original sequence.
                    for e in elems.into_iter().rev() {
                        match dir {
                            Direction::Left => list.push_front(e),
                            Direction::Right => list.push_back(e),
                        }
                    }
                }
            }
            Restore::Zset { key, members } => {
                if self.store.get(&key).is_none() {
                    self.store.set(key.clone(), Value::sorted_set());
                }
                if let Some(zset) = self.store.get_mut(&key).and_then(|v| v.as_sorted_set_mut()) {
                    for (member, score) in members {
                        zset.add(member, score);
                    }
                }
            }
            Restore::Move {
                src,
                src_dir,
                dest,
                dest_dir,
                value,
            } => {
                // Undo the push onto the destination.
                if let Some(list) = self.store.get_mut(&dest).and_then(|v| v.as_list_mut()) {
                    match dest_dir {
                        Direction::Left => {
                            list.pop_front();
                        }
                        Direction::Right => {
                            list.pop_back();
                        }
                    }
                }
                cleanup_empty_list(&mut self.store, &dest);
                // Undo the pop from the source (recreating it if emptied).
                if self.store.get(&src).is_none() {
                    self.store.set(src.clone(), Value::list());
                }
                if let Some(list) = self.store.get_mut(&src).and_then(|v| v.as_list_mut()) {
                    match src_dir {
                        Direction::Left => list.push_front(value),
                        Direction::Right => list.push_back(value),
                    }
                }
            }
        }
    }

    /// Drain XREADGROUP waiters for a key and send NOGROUP error
    /// (`specs/blocking.md` TR-BLOCKING-019).
    ///
    /// Only XREADGROUP waiters are drained — XREAD waiters remain blocked,
    /// matching Redis behaviour where a plain XREAD client stays blocked when
    /// the stream key is deleted or expires. It will either time-out or be
    /// woken when a new stream is created under the same key. That is a real
    /// asymmetry with the wrong-type drain, not an oversight: a missing key is
    /// still satisfiable by a later XADD, a wrong-typed one is not.
    pub(crate) fn drain_stream_waiters_with_error(&mut self, key: &Bytes) {
        while let Some(entry) = self.wait_queue.pop_oldest_xreadgroup_waiter(key) {
            let response = match &entry.op {
                BlockingOp::XReadGroup { group, .. } => Response::error(format!(
                    "NOGROUP No such consumer group '{}' for key name '{}'",
                    String::from_utf8_lossy(group),
                    String::from_utf8_lossy(key),
                )),
                _ => unreachable!("pop_oldest_xreadgroup_waiter only returns XReadGroup"),
            };
            self.complete_blocked_waiter(entry, response);
        }
    }

    /// Drain *every* stream waiter for a key and send the WRONGTYPE error
    /// (`specs/blocking.md` TR-BLOCKING-022).
    ///
    /// Called when the key's type has changed (e.g. SET overwrote a stream).
    /// Both XREAD and XREADGROUP waiters go: a wrong-typed key makes every
    /// stream wait on it unsatisfiable, and a plain `XREAD BLOCK 0` has no
    /// deadline and nothing that could ever re-signal the key as a stream, so
    /// leaving those parked is an unleavable blocked state. This is the one
    /// place the two drains differ — `drain_stream_waiters_with_error`'s
    /// missing-key condition leaves plain XREAD waiters parked *because* a
    /// later XADD still satisfies them.
    fn drain_stream_waiters_wrongtype(&mut self, key: &Bytes) {
        while let Some(entry) = self.wait_queue.pop_oldest_stream_waiter(key) {
            let response = Response::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
            self.complete_blocked_waiter(entry, response);
        }
    }
}

// ===========================================================================
// Satisfaction strategy seam
//
// The two real per-family decisions — *is the key satisfiable* and *what reply
// does this op produce (and where does the wake cascade)* — live behind this
// seam. Each strategy sees only the store; the wait queue, recursion, depth
// cap, metrics, and timeout re-validation are the driver's job. Deliberately do
// not hand a strategy the wait queue, or the seam dissolves.
// ===========================================================================

/// Keyspace events a woken serve must publish: `(key, event_name, class)`.
///
/// The immediate (non-blocking) command paths deposit the very same
/// Redis-verified events via `CommandContext::notify_event`; the satisfaction
/// path re-emits them here because it pops/moves directly on the store instead
/// of re-executing the command (unlike Redis, which re-runs the command on the
/// serve path — see blocked.c handleClientsBlockedOnKeys). At most two events
/// fire (a pop plus a BLMOVE/BRPOPLPUSH push).
type WokenEvents = Vec<(Bytes, &'static str, KeyspaceEventFlags)>;

/// What a satisfaction attempt produced for one popped waiter.
#[derive(Debug)]
enum Satisfaction {
    /// A reply was produced. `cascade` is a follow-up key whose waiters must
    /// also be woken (the BLMove/BRPOPLPUSH destination), or `None`.
    Done {
        /// The reply to deliver to the woken client.
        reply: Response,
        /// Destination key whose waiters should be woken next, if any.
        cascade: Option<Bytes>,
        /// Keyspace notifications to publish for this serve (pop, and push for
        /// a move). Empty for stream reads, which emit nothing.
        events: WokenEvents,
        /// How to put the consumed store data back if delivery fails (the
        /// receiver was dropped in the pop→send race). See [`Restore`].
        restore: Restore,
        /// The deterministic command replicas must apply to reproduce this
        /// served mutation (issue 02). A served blocking pop mutates the store
        /// directly here; the *waking* write (e.g. `LPUSH`) is the only thing
        /// broadcast otherwise, so a replica that re-executes it keeps the
        /// element the primary's blocked client consumed. Naming the exact
        /// deterministic pop (`LPOP`/`RPOP`/`LMOVE`/`ZPOPMIN` …) and shipping it
        /// after the waking write closes that divergence — the blocking-serve
        /// counterpart of SPOP's `SREM`/`DEL` rewrite. `None` for a pure read
        /// (blocking `XREAD`), which mutates nothing replicas do not already
        /// derive from the broadcast `XADD`. Applied only when delivery
        /// commits — a restored (undelivered) pop ships nothing.
        propagate: Option<SynthesizedCommand>,
    },
    /// The key was ready for the waiter's *kind* but produced nothing for this
    /// particular waiter, so it stays parked: the driver puts it back where it
    /// came from, with its deadline and registration ordinal intact, and moves
    /// on to the next one (`specs/blocking.md` TR-BLOCKING-023).
    ///
    /// Reachable only from [`StreamSatisfaction`], whose `check_key` answers a
    /// question (does the key exist and hold a stream?) weaker than the one
    /// `satisfy` asks (does it hold entries *this* waiter has not read?): a
    /// waiter parked on an `after_id` beyond the stream's tail, or an
    /// XREADGROUP whose single new entry an earlier waiter in the same pass has
    /// already taken. The list/zset arms return it only where `check_key`'s
    /// non-emptiness answer would have to be stale, which cannot happen on the
    /// shard's serial thread — they are re-park-safe rather than load-bearing.
    Retry,
    /// A terminal reply that consumed nothing (WRONGTYPE, NOGROUP); deliver it
    /// and drop the waiter without touching the stored value.
    Reject(Response),
}

/// How to undo the store mutation a [`Satisfaction::Done`] performed, applied
/// only when delivery to the woken client fails.
///
/// The wake path pops/moves data out of the store *before* it can know whether
/// the reply reaches the client: the server (the canonical timeout authority)
/// can drop the response receiver in the narrow window between the shard's
/// deadline re-check and its `send`. When that happens the popped element would
/// otherwise be lost — removed from the store and delivered to nobody (the
/// serve-vs-timeout race; guarded by testing-improvements issue 07). Restoring it
/// keeps every element in exactly one place: delivered, or back in the store.
#[derive(Debug)]
enum Restore {
    /// Nothing was consumed (stream reads: entries stay in the stream), so
    /// there is nothing to put back.
    None,
    /// List elements popped from `dir` end of `key`, in pop order. Re-inserted
    /// at the same end in reverse so the original ordering is exactly restored.
    List {
        key: Bytes,
        dir: Direction,
        elems: Vec<Bytes>,
    },
    /// Sorted-set members popped from `key`; re-added with their scores.
    Zset {
        key: Bytes,
        members: Vec<(Bytes, f64)>,
    },
    /// A BLMOVE/BRPOPLPUSH `value` popped from `src` (`src_dir`) and pushed to
    /// `dest` (`dest_dir`); undone by popping it off `dest` and pushing it back
    /// onto `src`.
    Move {
        src: Bytes,
        src_dir: Direction,
        dest: Bytes,
        dest_dir: Direction,
        value: Bytes,
    },
}

/// Outcome of validating a key before the driver pops a waiter of the
/// strategy's kind.
enum KeyReady {
    /// The key holds data a waiter of this kind could consume.
    Yes,
    /// No data right now; stop the satisfaction loop for this key.
    No,
    /// Stream-only: the key was deleted/expired — drain XREADGROUP waiters with
    /// NOGROUP, leave XREAD waiters blocked, and stop.
    DrainNoGroup,
    /// Stream-only: the key's type changed — drain XREADGROUP waiters with
    /// WRONGTYPE, leave XREAD waiters blocked, and stop.
    DrainWrongType,
}

/// Strategy for satisfying waiters of one [`WaiterKind`].
///
/// The store is the only collaborator a strategy sees. It runs against the
/// concrete [`HashMapStore`] (the only store the satisfaction path ever drives)
/// so it can use the hot-only, no-unspill read (`get_hot`) the satisfiability
/// check needs.
trait WaiterSatisfaction {
    /// Which waiter kind this strategy drives.
    fn kind(&self) -> WaiterKind;

    /// Whether a `Done` outcome should bump the shard version. List/zset writes
    /// pop elements and do; stream reads/group-deliveries do not (matching the
    /// pre-seam behaviour).
    fn bumps_version(&self) -> bool;

    /// Validate `key` before the driver pops a waiter.
    fn check_key(&mut self, store: &mut HashMapStore, key: &Bytes) -> KeyReady;

    /// Execute `entry.op` against the store for `key`.
    fn satisfy(&mut self, store: &mut HashMapStore, key: &Bytes, entry: &WaitEntry)
    -> Satisfaction;
}

/// Satisfaction strategy for BLPOP / BRPOP / BLMOVE / BRPOPLPUSH / BLMPOP.
struct ListSatisfaction;

impl WaiterSatisfaction for ListSatisfaction {
    fn kind(&self) -> WaiterKind {
        WaiterKind::List
    }

    fn bumps_version(&self) -> bool {
        true
    }

    fn check_key(&mut self, store: &mut HashMapStore, key: &Bytes) -> KeyReady {
        // Lazily purge an expired key so a blocker woken by a write doesn't
        // observe a stale value. Load-bearing for reblock-after-expire.
        if store.purge_if_expired(key) {
            return KeyReady::No;
        }
        let non_empty = store
            .get_hot(key)
            .and_then(|v| v.as_list().map(|l| !l.is_empty()))
            .unwrap_or(false);
        if non_empty {
            KeyReady::Yes
        } else {
            KeyReady::No
        }
    }

    fn satisfy(
        &mut self,
        store: &mut HashMapStore,
        key: &Bytes,
        entry: &WaitEntry,
    ) -> Satisfaction {
        match &entry.op {
            BlockingOp::BLPop => {
                match store
                    .get_mut(key)
                    .and_then(|v| v.as_list_mut())
                    .and_then(|l| l.pop_front())
                {
                    Some(value) => {
                        cleanup_empty_list(store, key);
                        Satisfaction::Done {
                            reply: Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(value.clone()),
                            ]),
                            cascade: None,
                            events: vec![(key.clone(), "lpop", KeyspaceEventFlags::LIST)],
                            restore: Restore::List {
                                key: key.clone(),
                                dir: Direction::Left,
                                elems: vec![value],
                            },
                            // The served BLPOP popped one element off the front:
                            // replicas reproduce it with `LPOP key`.
                            propagate: Some(SynthesizedCommand {
                                name: "LPOP",
                                args: vec![key.clone()],
                            }),
                        }
                    }
                    None => Satisfaction::Retry,
                }
            }
            BlockingOp::BRPop => {
                match store
                    .get_mut(key)
                    .and_then(|v| v.as_list_mut())
                    .and_then(|l| l.pop_back())
                {
                    Some(value) => {
                        cleanup_empty_list(store, key);
                        Satisfaction::Done {
                            reply: Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(value.clone()),
                            ]),
                            cascade: None,
                            events: vec![(key.clone(), "rpop", KeyspaceEventFlags::LIST)],
                            restore: Restore::List {
                                key: key.clone(),
                                dir: Direction::Right,
                                elems: vec![value],
                            },
                            // The served BRPOP popped one element off the back:
                            // replicas reproduce it with `RPOP key`.
                            propagate: Some(SynthesizedCommand {
                                name: "RPOP",
                                args: vec![key.clone()],
                            }),
                        }
                    }
                    None => Satisfaction::Retry,
                }
            }
            BlockingOp::BLMove {
                dest,
                src_dir,
                dest_dir,
            } => {
                // Check destination type BEFORE popping from source. If dest
                // exists and is not a list, return WRONGTYPE without consuming
                // the source element so the next waiter can attempt it.
                let dest_is_wrong_type = store
                    .get(dest)
                    .map(|v| v.as_list().is_none())
                    .unwrap_or(false);
                if dest_is_wrong_type {
                    return Satisfaction::Reject(Response::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }

                let value = match src_dir {
                    Direction::Left => store
                        .get_mut(key)
                        .and_then(|v| v.as_list_mut())
                        .and_then(|l| l.pop_front()),
                    Direction::Right => store
                        .get_mut(key)
                        .and_then(|v| v.as_list_mut())
                        .and_then(|l| l.pop_back()),
                };

                let Some(value) = value else {
                    return Satisfaction::Retry;
                };

                cleanup_empty_list(store, key);

                // Push to destination — get or create the dest list.
                if store.get(dest).is_none() {
                    store.set(dest.clone(), Value::list());
                }
                if let Some(dest_list) = store.get_mut(dest).and_then(|v| v.as_list_mut()) {
                    match dest_dir {
                        Direction::Left => dest_list.push_front(value.clone()),
                        Direction::Right => dest_list.push_back(value.clone()),
                    }
                }

                // Direction-resolved pop on the source, push on the destination
                // (mirrors the immediate BLMOVE/BRPOPLPUSH deposits).
                let pop_event = match src_dir {
                    Direction::Left => "lpop",
                    Direction::Right => "rpop",
                };
                let push_event = match dest_dir {
                    Direction::Left => "lpush",
                    Direction::Right => "rpush",
                };
                Satisfaction::Done {
                    reply: Response::bulk(value.clone()),
                    cascade: Some(dest.clone()),
                    events: vec![
                        (key.clone(), pop_event, KeyspaceEventFlags::LIST),
                        (dest.clone(), push_event, KeyspaceEventFlags::LIST),
                    ],
                    restore: Restore::Move {
                        src: key.clone(),
                        src_dir: *src_dir,
                        dest: dest.clone(),
                        dest_dir: *dest_dir,
                        value,
                    },
                    // A served BLMOVE/BRPOPLPUSH both pops the source and pushes
                    // the destination. `LMOVE` reproduces both ends
                    // deterministically in one command, so replicas match the
                    // primary on *both* keys (BRPOPLPUSH is `LMOVE src dst RIGHT
                    // LEFT`). A wake cascade appends the next hop's `LMOVE`
                    // after this one, preserving apply order.
                    propagate: Some(SynthesizedCommand {
                        name: "LMOVE",
                        args: vec![
                            key.clone(),
                            dest.clone(),
                            direction_arg(*src_dir),
                            direction_arg(*dest_dir),
                        ],
                    }),
                }
            }
            BlockingOp::BLMPop { direction, count } => {
                let mut popped: Vec<Bytes> = Vec::new();
                if let Some(list) = store.get_mut(key).and_then(|v| v.as_list_mut()) {
                    for _ in 0..*count {
                        let elem = match direction {
                            Direction::Left => list.pop_front(),
                            Direction::Right => list.pop_back(),
                        };
                        match elem {
                            Some(e) => popped.push(e),
                            None => break,
                        }
                    }
                }

                if popped.is_empty() {
                    return Satisfaction::Retry;
                }

                cleanup_empty_list(store, key);
                let popped_count = popped.len();
                let (pop_event, pop_cmd) = match direction {
                    Direction::Left => ("lpop", "LPOP"),
                    Direction::Right => ("rpop", "RPOP"),
                };
                let elements = popped.iter().cloned().map(Response::bulk).collect();
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ]),
                    cascade: None,
                    events: vec![(key.clone(), pop_event, KeyspaceEventFlags::LIST)],
                    restore: Restore::List {
                        key: key.clone(),
                        dir: *direction,
                        elems: popped,
                    },
                    // The served BLMPOP popped exactly `popped_count` elements
                    // off one end. `LPOP key N` / `RPOP key N` reproduces that
                    // count deterministically (the count is the *actual* number
                    // popped, so a partial drain replicates exactly what the
                    // primary removed).
                    propagate: Some(SynthesizedCommand {
                        name: pop_cmd,
                        args: vec![key.clone(), Bytes::from(popped_count.to_string())],
                    }),
                }
            }
            // Unreachable: `pop_oldest_waiter_of_kind(List)` filters on
            // `entry_matches_kind`, which is the exact op set matched above, so
            // this strategy is never handed another family's op. Fail-stop
            // rather than fall back — a silent reply here would answer the
            // wrong client with the wrong shape.
            other => unreachable!("pop_oldest_waiter_of_kind(List) returned {other:?}"),
        }
    }
}

/// Satisfaction strategy for BZPOPMIN / BZPOPMAX / BZMPOP.
struct ZsetSatisfaction;

impl WaiterSatisfaction for ZsetSatisfaction {
    fn kind(&self) -> WaiterKind {
        WaiterKind::SortedSet
    }

    fn bumps_version(&self) -> bool {
        true
    }

    fn check_key(&mut self, store: &mut HashMapStore, key: &Bytes) -> KeyReady {
        if store.purge_if_expired(key) {
            return KeyReady::No;
        }
        let non_empty = store
            .get_hot(key)
            .and_then(|v| v.as_sorted_set().map(|z| !z.is_empty()))
            .unwrap_or(false);
        if non_empty {
            KeyReady::Yes
        } else {
            KeyReady::No
        }
    }

    fn satisfy(
        &mut self,
        store: &mut HashMapStore,
        key: &Bytes,
        entry: &WaitEntry,
    ) -> Satisfaction {
        let is_resp3 = entry.protocol_version.is_resp3();
        match &entry.op {
            BlockingOp::BZPopMin => {
                let Some(zset) = store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) else {
                    return Satisfaction::Retry;
                };
                let popped = zset.pop_min(1);
                let is_empty = zset.is_empty();
                let Some((member, score)) = popped.into_iter().next() else {
                    return Satisfaction::Retry;
                };
                if is_empty {
                    store.delete(key);
                }
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::bulk(member.clone()),
                        zset_score_reply(score, is_resp3),
                    ]),
                    cascade: None,
                    events: vec![(key.clone(), "zpopmin", KeyspaceEventFlags::ZSET)],
                    restore: Restore::Zset {
                        key: key.clone(),
                        members: vec![(member, score)],
                    },
                    // The served BZPOPMIN popped the single lowest-scoring
                    // member: `ZPOPMIN key` reproduces it deterministically.
                    propagate: Some(SynthesizedCommand {
                        name: "ZPOPMIN",
                        args: vec![key.clone()],
                    }),
                }
            }
            BlockingOp::BZPopMax => {
                let Some(zset) = store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) else {
                    return Satisfaction::Retry;
                };
                let popped = zset.pop_max(1);
                let is_empty = zset.is_empty();
                let Some((member, score)) = popped.into_iter().next() else {
                    return Satisfaction::Retry;
                };
                if is_empty {
                    store.delete(key);
                }
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::bulk(member.clone()),
                        zset_score_reply(score, is_resp3),
                    ]),
                    cascade: None,
                    events: vec![(key.clone(), "zpopmax", KeyspaceEventFlags::ZSET)],
                    restore: Restore::Zset {
                        key: key.clone(),
                        members: vec![(member, score)],
                    },
                    // The served BZPOPMAX popped the single highest-scoring
                    // member: `ZPOPMAX key` reproduces it deterministically.
                    propagate: Some(SynthesizedCommand {
                        name: "ZPOPMAX",
                        args: vec![key.clone()],
                    }),
                }
            }
            BlockingOp::BZMPop { min, count } => {
                let popped =
                    if let Some(zset) = store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) {
                        if *min {
                            zset.pop_min(*count)
                        } else {
                            zset.pop_max(*count)
                        }
                    } else {
                        Vec::new()
                    };

                if popped.is_empty() {
                    return Satisfaction::Retry;
                }

                let elements = popped
                    .iter()
                    .map(|(member, score)| {
                        Response::Array(vec![
                            Response::bulk(member.clone()),
                            zset_score_reply(*score, is_resp3),
                        ])
                    })
                    .collect();

                cleanup_empty_zset(store, key);
                let popped_count = popped.len();
                let (pop_event, pop_cmd) = if *min {
                    ("zpopmin", "ZPOPMIN")
                } else {
                    ("zpopmax", "ZPOPMAX")
                };
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ]),
                    cascade: None,
                    events: vec![(key.clone(), pop_event, KeyspaceEventFlags::ZSET)],
                    restore: Restore::Zset {
                        key: key.clone(),
                        members: popped,
                    },
                    // The served BZMPOP popped `popped_count` members off one
                    // end: `ZPOPMIN key N` / `ZPOPMAX key N` reproduces exactly
                    // the count removed (partial drain included).
                    propagate: Some(SynthesizedCommand {
                        name: pop_cmd,
                        args: vec![key.clone(), Bytes::from(popped_count.to_string())],
                    }),
                }
            }
            // Unreachable: `pop_oldest_waiter_of_kind(SortedSet)` filters on
            // `entry_matches_kind`, which is the exact op set matched above, so
            // this strategy is never handed another family's op. Fail-stop
            // rather than fall back — a silent reply here would answer the
            // wrong client with the wrong shape.
            other => unreachable!("pop_oldest_waiter_of_kind(SortedSet) returned {other:?}"),
        }
    }
}

/// Satisfaction strategy for XREAD / XREADGROUP BLOCK.
struct StreamSatisfaction;

impl WaiterSatisfaction for StreamSatisfaction {
    fn kind(&self) -> WaiterKind {
        WaiterKind::Stream
    }

    fn bumps_version(&self) -> bool {
        false
    }

    fn check_key(&mut self, store: &mut HashMapStore, key: &Bytes) -> KeyReady {
        // Lazily purge an expired stream so blockers don't observe a stale
        // just-expired key; treat it as deleted.
        if store.purge_if_expired(key) {
            return KeyReady::DrainNoGroup;
        }
        match store.get(key) {
            None => KeyReady::DrainNoGroup,
            Some(value) if value.as_stream().is_none() => KeyReady::DrainWrongType,
            Some(_) => KeyReady::Yes,
        }
    }

    fn satisfy(
        &mut self,
        store: &mut HashMapStore,
        key: &Bytes,
        entry: &WaitEntry,
    ) -> Satisfaction {
        match &entry.op {
            BlockingOp::XRead { after_ids, count } => {
                let key_idx = entry.keys.iter().position(|k| k == key).unwrap_or(0);
                let after_id = &after_ids[key_idx];

                let entries: Vec<StreamEntry> = match store.get(key) {
                    Some(value) => value
                        .as_stream()
                        .map(|s| s.read_after(after_id, *count))
                        .unwrap_or_default(),
                    None => Vec::new(),
                };

                if entries.is_empty() {
                    return Satisfaction::Retry;
                }
                Satisfaction::Done {
                    reply: format_xread_response(key, &entries),
                    cascade: None,
                    // A blocking stream read emits no keyspace event (reads never
                    // do; XADD already notified when the entry was written).
                    events: Vec::new(),
                    // XREAD does not remove entries from the stream, so a failed
                    // delivery leaves nothing to restore.
                    restore: Restore::None,
                    // A plain blocking `XREAD` mutates nothing — the replica
                    // already holds the entries from the broadcast `XADD`, so
                    // there is nothing to reproduce.
                    propagate: None,
                }
            }
            BlockingOp::XReadGroup {
                group,
                consumer,
                noack,
                count,
            } => {
                // After RENAME the destination stream may lack the group, in
                // which case Redis returns NOGROUP.
                let group_exists = match store.get(key) {
                    Some(v) => v
                        .as_stream()
                        .map(|s| s.get_group(group).is_some())
                        .unwrap_or(false),
                    None => false,
                };

                if !group_exists {
                    return Satisfaction::Reject(Response::error(format!(
                        "NOGROUP No such consumer group '{}' for key name '{}'",
                        String::from_utf8_lossy(group),
                        String::from_utf8_lossy(key),
                    )));
                }

                match read_group_entries(store, key, group, consumer, *noack, *count) {
                    Some(entries) if !entries.is_empty() => Satisfaction::Done {
                        reply: format_xread_response(key, &entries),
                        cascade: None,
                        events: Vec::new(),
                        // XREADGROUP advances the group's last-delivered id and
                        // PEL but leaves the entries in the stream (reclaimable
                        // via XPENDING/XAUTOCLAIM), so it consumes no store data
                        // to restore here.
                        restore: Restore::None,
                        // KNOWN GAP (issue 02 follow-up): a served blocking
                        // XREADGROUP advances consumer-group state
                        // (last-delivered-id, PEL) that is NOT reproduced on the
                        // replica — the waking XADD is broadcast but the group
                        // advancement is not. Unlike the list/zset pops handled
                        // above, reproducing it means synthesizing an
                        // XREADGROUP/XCLAIM against the replica's group, a
                        // distinct mechanism (Redis propagates XCLAIM). Deferred
                        // to a dedicated stream-consumer-group replication task;
                        // `None` here preserves today's behaviour rather than
                        // shipping an untested stream path.
                        propagate: None,
                    },
                    _ => Satisfaction::Retry,
                }
            }
            // Unreachable: `pop_oldest_waiter_of_kind(Stream)` filters on
            // `entry_matches_kind`, which is the exact op set matched above, so
            // this strategy is never handed another family's op. Fail-stop
            // rather than fall back — a silent reply here would answer the
            // wrong client with the wrong shape.
            other => unreachable!("pop_oldest_waiter_of_kind(Stream) returned {other:?}"),
        }
    }
}

/// The `LEFT`/`RIGHT` keyword a synthesized `LMOVE` replication command uses
/// for a [`Direction`] end.
fn direction_arg(dir: Direction) -> Bytes {
    match dir {
        Direction::Left => Bytes::from_static(b"LEFT"),
        Direction::Right => Bytes::from_static(b"RIGHT"),
    }
}

/// Format a sorted-set score for the reply: a RESP3 double, or a RESP2 bulk
/// string.
fn zset_score_reply(score: f64, is_resp3: bool) -> Response {
    if is_resp3 {
        Response::Double(score)
    } else {
        Response::bulk(Bytes::from(score.to_string()))
    }
}

/// Delete `key` if it now holds an empty list.
fn cleanup_empty_list(store: &mut HashMapStore, key: &Bytes) {
    if let Some(value) = store.get(key)
        && let Some(list) = value.as_list()
        && list.is_empty()
    {
        store.delete(key);
    }
}

/// Delete `key` if it now holds an empty sorted set.
fn cleanup_empty_zset(store: &mut HashMapStore, key: &Bytes) {
    if let Some(value) = store.get(key)
        && let Some(zset) = value.as_sorted_set()
        && zset.is_empty()
    {
        store.delete(key);
    }
}

/// Read new entries for an XREADGROUP waiter and update group state (PEL,
/// last-delivered id, consumer timestamps). Returns `None` when there is
/// nothing new to deliver.
fn read_group_entries(
    store: &mut HashMapStore,
    key: &Bytes,
    group_name: &Bytes,
    consumer_name: &Bytes,
    noack: bool,
    count: Option<usize>,
) -> Option<Vec<StreamEntry>> {
    let stream = store.get_mut(key)?.as_stream_mut()?;
    let group = stream.get_group_mut(group_name)?;

    let last_delivered = group.last_delivered_id();
    let new_entries = stream.read_after(&last_delivered, count);

    if new_entries.is_empty() {
        return None;
    }

    stream.record_group_delivery(group_name, consumer_name, &new_entries, noack);

    Some(new_entries)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use tokio::sync::mpsc;

    use super::*;
    use crate::eviction::EvictionConfig;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{Envelope, ShardReceiver};

    // ---- Store-only strategy tests (no wait queue, no worker) -------------

    fn make_entry(op: BlockingOp, keys: Vec<Bytes>) -> (WaitEntry, oneshot::Receiver<Response>) {
        let (tx, rx) = oneshot::channel();
        let entry = WaitEntry {
            conn_id: 1,
            keys,
            op,
            response_tx: tx,
            deadline: None,
            protocol_version: ProtocolVersion::default(),
        };
        (entry, rx)
    }

    fn list_with(key: &Bytes, elems: &[&str]) -> HashMapStore {
        let mut store = HashMapStore::new();
        let mut v = Value::list();
        let list = v.as_list_mut().unwrap();
        for e in elems {
            list.push_back(Bytes::from(e.to_string()));
        }
        store.set(key.clone(), v);
        store
    }

    #[test]
    fn blpop_satisfy_pops_and_replies() {
        let key = Bytes::from_static(b"k");
        let mut store = list_with(&key, &["a", "b"]);
        let (entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);

        match ListSatisfaction.satisfy(&mut store, &key, &entry) {
            Satisfaction::Done { reply, cascade, .. } => {
                assert!(cascade.is_none());
                assert!(matches!(reply, Response::Array(_)));
            }
            other => panic!("expected Done, got {other:?}"),
        }
        // The front element was consumed; "b" remains.
        let remaining = store.get_hot(&key).unwrap();
        let list = remaining.as_list().unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn blpop_satisfy_empty_is_retry() {
        let key = Bytes::from_static(b"k");
        // Empty list present but no elements: pop_front yields None -> Retry.
        let mut store = list_with(&key, &[]);
        let (entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        assert!(matches!(
            ListSatisfaction.satisfy(&mut store, &key, &entry),
            Satisfaction::Retry
        ));
    }

    #[test]
    fn blmove_wrong_type_dest_is_reject() {
        let src = Bytes::from_static(b"s");
        let dst = Bytes::from_static(b"d");
        let mut store = list_with(&src, &["a"]);
        store.set(dst.clone(), Value::string("notalist"));

        let (entry, _rx) = make_entry(
            BlockingOp::BLMove {
                dest: dst.clone(),
                src_dir: Direction::Left,
                dest_dir: Direction::Right,
            },
            vec![src.clone()],
        );
        match ListSatisfaction.satisfy(&mut store, &src, &entry) {
            Satisfaction::Reject(Response::Error(_)) => {}
            other => panic!("expected Reject(WRONGTYPE), got {other:?}"),
        }
        // Source element is untouched on a WRONGTYPE reject.
        assert_eq!(store.get_hot(&src).unwrap().as_list().unwrap().len(), 1);
    }

    #[test]
    fn blmove_success_moves_and_cascades() {
        let src = Bytes::from_static(b"s");
        let dst = Bytes::from_static(b"d");
        let mut store = list_with(&src, &["a"]);

        let (entry, _rx) = make_entry(
            BlockingOp::BLMove {
                dest: dst.clone(),
                src_dir: Direction::Left,
                dest_dir: Direction::Right,
            },
            vec![src.clone()],
        );
        match ListSatisfaction.satisfy(&mut store, &src, &entry) {
            Satisfaction::Done { reply, cascade, .. } => {
                assert!(matches!(reply, Response::Bulk(Some(_))));
                assert_eq!(cascade, Some(dst.clone()), "BLMOVE cascades to its dest");
            }
            other => panic!("expected Done, got {other:?}"),
        }
        // Source emptied (and deleted), element now lives in dest.
        assert!(store.get_hot(&src).is_none());
        assert_eq!(store.get_hot(&dst).unwrap().as_list().unwrap().len(), 1);
    }

    #[test]
    fn bzpopmin_satisfy_pops_min() {
        let key = Bytes::from_static(b"z");
        let mut store = HashMapStore::new();
        let mut v = Value::sorted_set();
        let z = v.as_sorted_set_mut().unwrap();
        z.add(Bytes::from_static(b"a"), 1.0);
        z.add(Bytes::from_static(b"b"), 2.0);
        store.set(key.clone(), v);

        let (entry, _rx) = make_entry(BlockingOp::BZPopMin, vec![key.clone()]);
        match ZsetSatisfaction.satisfy(&mut store, &key, &entry) {
            Satisfaction::Done { reply, cascade, .. } => {
                assert!(cascade.is_none());
                assert!(matches!(reply, Response::Array(_)));
            }
            other => panic!("expected Done, got {other:?}"),
        }
        // Min element ("a") consumed; "b" remains.
        assert_eq!(
            store.get_hot(&key).unwrap().as_sorted_set().unwrap().len(),
            1
        );
    }

    #[test]
    fn xreadgroup_missing_group_is_reject() {
        let key = Bytes::from_static(b"st");
        let mut store = HashMapStore::new();
        store.set(key.clone(), Value::stream()); // stream exists, no groups

        let (entry, _rx) = make_entry(
            BlockingOp::XReadGroup {
                group: Bytes::from_static(b"g"),
                consumer: Bytes::from_static(b"c"),
                noack: false,
                count: None,
            },
            vec![key.clone()],
        );
        match StreamSatisfaction.satisfy(&mut store, &key, &entry) {
            Satisfaction::Reject(Response::Error(e)) => {
                assert!(e.starts_with(b"NOGROUP"), "expected NOGROUP, got {e:?}");
            }
            other => panic!("expected Reject(NOGROUP), got {other:?}"),
        }
    }

    // ---- Served-pop replication synthesis (issue 02) ----------------------
    //
    // Each served blocking pop must name the deterministic command a replica
    // re-applies to reproduce the primary's mutation. Verbatim propagation of
    // the *waking* write alone (LPUSH/ZADD/…) leaves the consumed element on the
    // replica; these pins assert the synthesized pop for every op family.

    /// Extract the synthesized replication command from a `Done` outcome.
    fn propagate_of(s: Satisfaction) -> Option<SynthesizedCommand> {
        match s {
            Satisfaction::Done { propagate, .. } => propagate,
            other => panic!("expected Done, got {other:?}"),
        }
    }

    #[test]
    fn blpop_propagates_as_lpop() {
        let key = Bytes::from_static(b"k");
        let mut store = list_with(&key, &["a", "b"]);
        let (entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        let cmd = propagate_of(ListSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BLPOP serve must synthesize a pop");
        assert_eq!(cmd.name, "LPOP");
        assert_eq!(cmd.args, vec![key]);
    }

    #[test]
    fn brpop_propagates_as_rpop() {
        let key = Bytes::from_static(b"k");
        let mut store = list_with(&key, &["a", "b"]);
        let (entry, _rx) = make_entry(BlockingOp::BRPop, vec![key.clone()]);
        let cmd = propagate_of(ListSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BRPOP serve must synthesize a pop");
        assert_eq!(cmd.name, "RPOP");
        assert_eq!(cmd.args, vec![key]);
    }

    #[test]
    fn blmove_propagates_as_lmove_with_directions() {
        let src = Bytes::from_static(b"s");
        let dst = Bytes::from_static(b"d");
        let mut store = list_with(&src, &["a"]);
        let (entry, _rx) = make_entry(
            BlockingOp::BLMove {
                dest: dst.clone(),
                src_dir: Direction::Right,
                dest_dir: Direction::Left,
            },
            vec![src.clone()],
        );
        // BRPOPLPUSH is exactly `LMOVE src dst RIGHT LEFT`; the synthesized
        // command must carry both keys and both resolved directions so the
        // replica reproduces the pop AND the push.
        let cmd = propagate_of(ListSatisfaction.satisfy(&mut store, &src, &entry))
            .expect("BLMOVE serve must synthesize a move");
        assert_eq!(cmd.name, "LMOVE");
        assert_eq!(
            cmd.args,
            vec![
                src,
                dst,
                Bytes::from_static(b"RIGHT"),
                Bytes::from_static(b"LEFT"),
            ]
        );
    }

    #[test]
    fn blmpop_propagates_as_lpop_with_actual_count() {
        let key = Bytes::from_static(b"k");
        // Only two elements present but COUNT 5 requested: the propagated count
        // must be the *actual* number popped (2), not the requested 5, so a
        // partial drain replicates exactly what the primary removed.
        let mut store = list_with(&key, &["a", "b"]);
        let (entry, _rx) = make_entry(
            BlockingOp::BLMPop {
                direction: Direction::Left,
                count: 5,
            },
            vec![key.clone()],
        );
        let cmd = propagate_of(ListSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BLMPOP serve must synthesize a pop");
        assert_eq!(cmd.name, "LPOP");
        assert_eq!(cmd.args, vec![key, Bytes::from_static(b"2")]);
    }

    #[test]
    fn bzpopmin_propagates_as_zpopmin() {
        let key = Bytes::from_static(b"z");
        let mut store = HashMapStore::new();
        let mut v = Value::sorted_set();
        let z = v.as_sorted_set_mut().unwrap();
        z.add(Bytes::from_static(b"a"), 1.0);
        z.add(Bytes::from_static(b"b"), 2.0);
        store.set(key.clone(), v);
        let (entry, _rx) = make_entry(BlockingOp::BZPopMin, vec![key.clone()]);
        let cmd = propagate_of(ZsetSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BZPOPMIN serve must synthesize a pop");
        assert_eq!(cmd.name, "ZPOPMIN");
        assert_eq!(cmd.args, vec![key]);
    }

    #[test]
    fn bzpopmax_propagates_as_zpopmax() {
        let key = Bytes::from_static(b"z");
        let mut store = HashMapStore::new();
        let mut v = Value::sorted_set();
        let z = v.as_sorted_set_mut().unwrap();
        z.add(Bytes::from_static(b"a"), 1.0);
        z.add(Bytes::from_static(b"b"), 2.0);
        store.set(key.clone(), v);
        let (entry, _rx) = make_entry(BlockingOp::BZPopMax, vec![key.clone()]);
        let cmd = propagate_of(ZsetSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BZPOPMAX serve must synthesize a pop");
        assert_eq!(cmd.name, "ZPOPMAX");
        assert_eq!(cmd.args, vec![key]);
    }

    #[test]
    fn bzmpop_propagates_as_zpop_with_actual_count() {
        let key = Bytes::from_static(b"z");
        let mut store = HashMapStore::new();
        let mut v = Value::sorted_set();
        let z = v.as_sorted_set_mut().unwrap();
        z.add(Bytes::from_static(b"a"), 1.0);
        z.add(Bytes::from_static(b"b"), 2.0);
        z.add(Bytes::from_static(b"c"), 3.0);
        store.set(key.clone(), v);
        let (entry, _rx) = make_entry(
            BlockingOp::BZMPop {
                min: false,
                count: 2,
            },
            vec![key.clone()],
        );
        let cmd = propagate_of(ZsetSatisfaction.satisfy(&mut store, &key, &entry))
            .expect("BZMPOP serve must synthesize a pop");
        assert_eq!(cmd.name, "ZPOPMAX");
        assert_eq!(cmd.args, vec![key, Bytes::from_static(b"2")]);
    }

    #[test]
    fn blocking_xread_propagates_nothing() {
        // A plain blocking XREAD is a pure read: the replica already holds the
        // entries from the broadcast XADD, so there is nothing to reproduce.
        let key = Bytes::from_static(b"st");
        let mut store = HashMapStore::new();
        let mut v = Value::stream();
        v.as_stream_mut()
            .unwrap()
            .add(
                crate::types::StreamIdSpec::Explicit(crate::types::StreamId::new(1, 0)),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"1"))],
            )
            .unwrap();
        store.set(key.clone(), v);
        let (entry, _rx) = make_entry(
            BlockingOp::XRead {
                after_ids: vec![crate::types::StreamId::new(0, 0)],
                count: None,
            },
            vec![key.clone()],
        );
        assert!(
            propagate_of(StreamSatisfaction.satisfy(&mut store, &key, &entry)).is_none(),
            "a blocking XREAD serve replicates nothing"
        );
    }

    // ---- Driver test: BLMOVE fan-out depth cap (needs a worker) -----------

    fn build_worker() -> (
        ShardWorker,
        mpsc::Sender<Envelope>,
        mpsc::Sender<crate::shard::NewConnection>,
    ) {
        let (msg_tx, msg_rx) = mpsc::channel::<Envelope>(8);
        let (conn_tx, conn_rx) = mpsc::channel::<crate::shard::NewConnection>(8);
        let worker = ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            Arc::new(vec![]),
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            Arc::new(crate::noop::NoopMetricsRecorder),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        );
        (worker, msg_tx, conn_tx)
    }

    #[test]
    fn blmove_fanout_stops_at_depth_cap() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();

        // Build a BLMove chain longer than the cap: a waiter on k{i} that moves
        // into k{i+1}. Keep every receiver alive so the satisfaction path treats
        // the waiters as live.
        let chain_len = MAX_BLMOVE_FANOUT_DEPTH + 3;
        let key = |i: usize| Bytes::from(format!("k{i}"));
        let mut receivers = Vec::new();
        for i in 0..chain_len {
            let (entry, rx) = make_entry(
                BlockingOp::BLMove {
                    dest: key(i + 1),
                    src_dir: Direction::Left,
                    dest_dir: Direction::Right,
                },
                vec![key(i)],
            );
            worker.wait_queue.register(entry).unwrap();
            receivers.push(rx);
        }

        // Seed one element at the chain head and drive the cascade.
        let mut head = Value::list();
        head.as_list_mut()
            .unwrap()
            .push_back(Bytes::from_static(b"x"));
        worker.store.set(key(0), head);

        worker.try_satisfy_list_waiters(&key(0));

        // The cascade stops at the cap: the element lands at k{CAP} and the
        // waiter there (and beyond) is left unwoken — no element is lost.
        let landed = worker.store.get_hot(&key(MAX_BLMOVE_FANOUT_DEPTH));
        assert!(
            landed.is_some_and(|v| v.as_list().is_some_and(|l| l.len() == 1)),
            "element should rest at the depth-cap key"
        );
        assert!(
            worker
                .wait_queue
                .has_waiters_for_kind(&key(MAX_BLMOVE_FANOUT_DEPTH), WaiterKind::List),
            "the waiter at the depth cap should remain blocked"
        );
    }

    // ---- Stream drains: wrong-type vs missing-key scope (issue 15) --------
    //
    // The two drain arms have deliberately different scope, and the difference
    // is the whole point of TR-BLOCKING-019 vs TR-BLOCKING-022: a *missing* key
    // is still satisfiable by a later XADD (plain XREAD waiters stay parked), a
    // *wrong-typed* key is not satisfiable by anything (every stream waiter is
    // drained, XREAD included — otherwise the wait is unleavable).

    /// Register a stream waiter of the given op on `key`.
    fn park_stream_waiter(
        worker: &mut ShardWorker,
        key: &Bytes,
        op: BlockingOp,
    ) -> oneshot::Receiver<Response> {
        let (entry, rx) = make_entry(op, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();
        rx
    }

    fn xread_from_zero() -> BlockingOp {
        BlockingOp::XRead {
            after_ids: vec![crate::types::StreamId::new(0, 0)],
            count: None,
        }
    }

    fn xreadgroup_op() -> BlockingOp {
        BlockingOp::XReadGroup {
            group: Bytes::from_static(b"g"),
            consumer: Bytes::from_static(b"c"),
            noack: false,
            count: None,
        }
    }

    /// The forcing test for the fix: a plain `XREAD BLOCK 0` waiter on a key
    /// that gets overwritten with another type must be drained, not left
    /// parked. Pre-fix the drain popped only XREADGROUP waiters, so this waiter
    /// stayed in the queue forever — no deadline, and nothing that could ever
    /// re-signal the key as a stream.
    #[test]
    fn a_wrong_typed_key_drains_plain_xread_waiters() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");
        worker.store.set(key.clone(), Value::stream());

        let mut rx = park_stream_waiter(&mut worker, &key, xread_from_zero());

        // SET overwrites the stream, then signals the stream waiters.
        worker.store.set(key.clone(), Value::string("notastream"));
        worker.try_satisfy_stream_waiters(&key);

        match rx.try_recv().expect("the XREAD waiter must be drained") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
                "the drain must use the pinned WRONGTYPE text"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::Stream),
            "no stream waiter may survive a wrong-typed key"
        );
    }

    /// The same drain still covers XREADGROUP waiters (pins the pre-existing
    /// half of the arm, which had no forcing test at all).
    #[test]
    fn a_wrong_typed_key_drains_xreadgroup_waiters() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");
        worker.store.set(key.clone(), Value::stream());

        let mut rx = park_stream_waiter(&mut worker, &key, xreadgroup_op());

        worker.store.set(key.clone(), Value::string("notastream"));
        worker.try_satisfy_stream_waiters(&key);

        match rx
            .try_recv()
            .expect("the XREADGROUP waiter must be drained")
        {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::Stream),
            "no stream waiter may survive a wrong-typed key"
        );
    }

    /// The asymmetry: a *deleted* stream drains only the XREADGROUP waiter. The
    /// plain XREAD waiter stays parked because its wait is still satisfiable —
    /// and a later XADD recreating the key does satisfy it.
    #[test]
    fn a_deleted_stream_drains_xreadgroup_but_leaves_xread_parked() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");
        worker.store.set(key.clone(), Value::stream());

        let mut group_rx = park_stream_waiter(&mut worker, &key, xreadgroup_op());
        let mut read_rx = park_stream_waiter(&mut worker, &key, xread_from_zero());

        worker.store.delete(&key);
        worker.try_satisfy_stream_waiters(&key);

        match group_rx
            .try_recv()
            .expect("the XREADGROUP waiter must be drained")
        {
            Response::Error(bytes) => assert!(
                bytes.starts_with(b"NOGROUP No such consumer group 'g' for key name 'st'"),
                "expected the pinned NOGROUP text, got {bytes:?}"
            ),
            other => panic!("expected NOGROUP, got {other:?}"),
        }
        assert!(
            read_rx.try_recv().is_err(),
            "a plain XREAD waiter stays parked when the key merely disappears"
        );

        // ... and the surviving waiter is served by a later XADD under the same
        // key, which is why leaving it parked is sound.
        let mut recreated = Value::stream();
        recreated
            .as_stream_mut()
            .unwrap()
            .add(
                crate::types::StreamIdSpec::Explicit(crate::types::StreamId::new(1, 0)),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"1"))],
            )
            .unwrap();
        worker.store.set(key.clone(), recreated);
        worker.try_satisfy_stream_waiters(&key);

        assert!(
            matches!(
                read_rx.try_recv().expect("the XREAD waiter must be served"),
                Response::Array(_)
            ),
            "the surviving XREAD waiter is served by the recreating XADD"
        );
    }

    // ---- Retried waiters (TR-BLOCKING-023 / FM-BLOCKING-013) --------------
    //
    // `check_key` for streams asks a weaker question than `satisfy` does: the
    // key holds a stream, but not necessarily entries *this* waiter has not
    // read. The waiter is popped and then produces nothing; it must go back to
    // waiting rather than be answered.

    /// Park an XREAD waiter for `conn_id` reading everything after `after_ms-0`.
    fn park_xread_after(
        worker: &mut ShardWorker,
        key: &Bytes,
        conn_id: u64,
        after_ms: u64,
    ) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        worker
            .wait_queue
            .register(WaitEntry {
                conn_id,
                keys: vec![key.clone()],
                op: BlockingOp::XRead {
                    after_ids: vec![crate::types::StreamId::new(after_ms, 0)],
                    count: None,
                },
                response_tx: tx,
                deadline: None,
                protocol_version: ProtocolVersion::default(),
            })
            .unwrap();
        rx
    }

    /// Append `<ms>-0` to the stream at `key`, creating it if needed, then run
    /// the satisfaction pass the real XADD path would run.
    fn xadd_and_wake(worker: &mut ShardWorker, key: &Bytes, ms: u64) {
        if worker.store.get_hot(key).is_none() {
            worker.store.set(key.clone(), Value::stream());
        }
        worker
            .store
            .get_mut(key)
            .and_then(|v| v.as_stream_mut())
            .expect("key must hold a stream")
            .add(
                crate::types::StreamIdSpec::Explicit(crate::types::StreamId::new(ms, 0)),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"1"))],
            )
            .unwrap();
        worker.try_satisfy_stream_waiters(key);
    }

    // TR-BLOCKING-023
    // FM-BLOCKING-013
    /// The forcing test: an unrelated write makes the key "ready" for stream
    /// waiters, but its entry is older than what this waiter asked for. Before
    /// the fix the popped waiter was answered with an op-aware nil — a timeout
    /// the client never asked for; it must simply stay parked.
    #[test]
    fn a_stream_waiter_the_new_entry_does_not_reach_stays_parked() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");

        let mut rx = park_xread_after(&mut worker, &key, 1, 5);
        xadd_and_wake(&mut worker, &key, 2);

        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "a waiter the new entry does not reach must be neither answered nor \
             have its channel closed"
        );
        assert!(
            worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::Stream),
            "the unsatisfied waiter must be back in the queue"
        );
    }

    // TR-BLOCKING-023
    // FM-BLOCKING-013
    /// A retried waiter is a full queue member: the next write that *does*
    /// reach its `after_id` serves it.
    #[test]
    fn a_retried_stream_waiter_is_served_by_a_later_write() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");

        let mut rx = park_xread_after(&mut worker, &key, 1, 5);
        xadd_and_wake(&mut worker, &key, 2);
        xadd_and_wake(&mut worker, &key, 6);

        assert!(
            matches!(
                rx.try_recv()
                    .expect("the retried waiter must now be served"),
                Response::Array(_)
            ),
            "the write past the waiter's after_id serves it"
        );
    }

    // TR-BLOCKING-023
    // FM-BLOCKING-013
    /// A retry is invisible to ordering: the waiter returns to the head of the
    /// key's deque with the ordinal it registered under, so neither per-key
    /// FIFO wake order nor the slot-drain order (TR-BLOCKING-018) sees it move.
    #[test]
    fn a_retried_waiter_keeps_its_deque_position_and_ordinal() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");

        // conns 1 and 3 ask for more than the write will deliver; conn 2 is
        // served by it and leaves.
        let _first = park_xread_after(&mut worker, &key, 1, 5);
        let mut served = park_xread_after(&mut worker, &key, 2, 0);
        let _third = park_xread_after(&mut worker, &key, 3, 5);

        xadd_and_wake(&mut worker, &key, 2);
        assert!(served.try_recv().is_ok(), "conn 2's read is satisfied");

        let dump = worker.wait_queue.dump();
        let (_, waiters) = dump
            .iter()
            .find(|(k, _)| k == &key)
            .expect("the retried waiters are still parked on the key");
        let seen: Vec<(u64, u64)> = waiters
            .iter()
            .map(|w| (w.conn_id, w.registration_seq))
            .collect();
        assert_eq!(
            seen,
            vec![(1, 0), (3, 2)],
            "retried waiters keep their registration order, ordinals and position"
        );
    }

    // TR-BLOCKING-023
    // FM-BLOCKING-013
    /// A requeued waiter is covered by everything a freshly registered one is —
    /// in particular the wrong-type drain (TR-BLOCKING-022), so a retry can
    /// never be the way a waiter ends up stranded on an unusable key.
    #[test]
    fn a_retried_waiter_is_still_covered_by_a_wrong_type_drain() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"st");

        let mut rx = park_xread_after(&mut worker, &key, 1, 5);
        xadd_and_wake(&mut worker, &key, 2);

        worker.store.set(key.clone(), Value::string("notastream"));
        worker.try_satisfy_stream_waiters(&key);

        match rx.try_recv().expect("the requeued waiter must be drained") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                b"WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
    }

    // ---- Lost-element timeout race (the scoped correctness flag) ----------

    /// A push that reaches the shard *after* the server already returned a
    /// timeout (its receiver dropped) must not lose the element. `satisfy` pops
    /// it, the inline `send` fails, and `apply_restore` puts it back — so it is
    /// neither delivered to nobody nor silently dropped (the serve-vs-timeout
    /// pop→send race; guarded by testing-improvements issue 07).
    #[test]
    fn push_after_receiver_dropped_does_not_lose_element() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        // Register a BLPOP waiter, then drop its receiver to simulate the server
        // having already returned a timeout nil and torn down its side.
        let (entry, rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();
        drop(rx);

        // A push lands and the shard tries to satisfy the (doomed) waiter.
        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.try_satisfy_list_waiters(&key);

        // The element must still be in the store — restored after the failed
        // delivery rather than popped into the void.
        let list = worker.store.get_hot(&key).expect("key must survive");
        assert_eq!(
            list.as_list().unwrap().len(),
            1,
            "element must not be lost to an abandoned waiter"
        );
        assert_eq!(
            list.as_list().unwrap().get(0).cloned(),
            Some(Bytes::from_static(b"x")),
            "the restored element keeps its value"
        );
        // The doomed waiter was consumed off the queue (not left blocking).
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::List),
            "the abandoned waiter is removed from the queue"
        );
    }

    /// Multi-element / multi-key restore: a BLMPOP waiter whose receiver is gone
    /// must have *every* popped element restored, in original order, to the
    /// correct end — the multi-key exactly-once conservation property (issue 07).
    #[test]
    fn blmpop_restore_preserves_all_elements_in_order() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let ka = Bytes::from_static(b"a");
        let kb = Bytes::from_static(b"b");

        // A BLMPOP LEFT waiter across overlapping keys [a, b], receiver dropped.
        let (entry, rx) = make_entry(
            BlockingOp::BLMPop {
                direction: Direction::Left,
                count: 10,
            },
            vec![ka.clone(), kb.clone()],
        );
        worker.wait_queue.register(entry).unwrap();
        drop(rx);

        // Seed key `a` with three ordered elements.
        let mut v = Value::list();
        {
            let l = v.as_list_mut().unwrap();
            l.push_back(Bytes::from_static(b"1"));
            l.push_back(Bytes::from_static(b"2"));
            l.push_back(Bytes::from_static(b"3"));
        }
        worker.store.set(ka.clone(), v);

        worker.try_satisfy_list_waiters(&ka);

        // All three elements are back, in their original order.
        let list = worker.store.get_hot(&ka).expect("key a must survive");
        let elems: Vec<Bytes> = list.as_list().unwrap().iter().cloned().collect();
        assert_eq!(
            elems,
            vec![
                Bytes::from_static(b"1"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"3"),
            ],
            "every popped element is restored in original order"
        );
        // The waiter is gone from BOTH overlapping keys (multi-key removal).
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&ka, WaiterKind::List)
        );
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&kb, WaiterKind::List)
        );
    }

    /// Zset restore: a BZPOPMIN waiter whose receiver is gone must have its
    /// popped member+score put back so the sorted set is whole.
    #[test]
    fn bzpopmin_restore_preserves_member_and_score() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"z");

        let (entry, rx) = make_entry(BlockingOp::BZPopMin, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();
        drop(rx);

        let mut v = Value::sorted_set();
        {
            let z = v.as_sorted_set_mut().unwrap();
            z.add(Bytes::from_static(b"a"), 1.0);
            z.add(Bytes::from_static(b"b"), 2.0);
        }
        worker.store.set(key.clone(), v);

        worker.try_satisfy_zset_waiters(&key);

        let zset = worker.store.get_hot(&key).expect("zset must survive");
        assert_eq!(
            zset.as_sorted_set().unwrap().len(),
            2,
            "the popped member must be restored to the sorted set"
        );
    }

    /// A push racing a waiter whose deadline already elapsed (the server fires
    /// at the precise deadline) must likewise leave the element in the store.
    #[test]
    fn push_after_deadline_elapsed_does_not_consume_element() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        // Receiver kept alive, but the deadline is already in the past — the
        // server is the timeout authority and has effectively already returned.
        let (mut entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        entry.deadline = Some(Instant::now() - std::time::Duration::from_secs(1));
        worker.wait_queue.register(entry).unwrap();

        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.try_satisfy_list_waiters(&key);

        let list = worker.store.get_hot(&key).expect("key must survive");
        assert_eq!(
            list.as_list().unwrap().len(),
            1,
            "an expired waiter must not consume the pushed element"
        );
    }

    /// A live waiter (deadline in the future, receiver open) is still satisfied
    /// normally — the re-validation only drops doomed waiters.
    #[test]
    fn push_to_live_waiter_still_consumes_element() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        let (mut entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        entry.deadline = Some(Instant::now() + std::time::Duration::from_secs(60));
        worker.wait_queue.register(entry).unwrap();

        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.try_satisfy_list_waiters(&key);

        // The element was delivered, so the list is now empty and the key removed.
        assert!(
            worker.store.get_hot(&key).is_none(),
            "a live waiter consumes the pushed element"
        );
        assert!(
            !worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::List),
            "the satisfied waiter is removed from the queue"
        );
    }

    // ---- Served-pop propagation buffer accumulation (issue 02) ------------

    /// Driving satisfaction for a live BLPOP waiter records the equivalent
    /// `LPOP` in the worker's `pending_serve_propagations` buffer, which the
    /// `ReplicationBroadcast` effect later flushes to replicas.
    #[test]
    fn served_blpop_records_pending_propagation() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        let (mut entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        entry.deadline = Some(Instant::now() + std::time::Duration::from_secs(60));
        worker.wait_queue.register(entry).unwrap();

        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.try_satisfy_list_waiters(&key);

        assert_eq!(worker.pending_serve_propagations.len(), 1);
        assert_eq!(worker.pending_serve_propagations[0].name, "LPOP");
        assert_eq!(worker.pending_serve_propagations[0].args, vec![key]);
    }

    /// A doomed waiter (receiver dropped) records NO propagation: nothing was
    /// consumed, so nothing must ship to replicas.
    #[test]
    fn doomed_waiter_records_no_propagation() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        let (entry, rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();
        drop(rx);

        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.try_satisfy_list_waiters(&key);

        assert!(
            worker.pending_serve_propagations.is_empty(),
            "a served element that reached no client must not replicate a pop"
        );
    }

    /// A BLMOVE wake chain records one `LMOVE` per served hop, in apply order:
    /// the parent hop's move lands ahead of the cascade's so a replica applies
    /// them push-then-pop and converges.
    #[test]
    fn blmove_cascade_records_ordered_propagations() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let a = Bytes::from_static(b"a");
        let b = Bytes::from_static(b"b");
        let c = Bytes::from_static(b"c");

        // Waiter 1: BLMOVE a -> b (LEFT, RIGHT). Waiter 2: BLMOVE b -> c.
        for (src, dest) in [(a.clone(), b.clone()), (b.clone(), c.clone())] {
            let (mut entry, rx) = make_entry(
                BlockingOp::BLMove {
                    dest,
                    src_dir: Direction::Left,
                    dest_dir: Direction::Right,
                },
                vec![src],
            );
            entry.deadline = Some(Instant::now() + std::time::Duration::from_secs(60));
            worker.wait_queue.register(entry).unwrap();
            std::mem::forget(rx); // keep the receiver open for the whole cascade
        }

        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(a.clone(), v);
        worker.try_satisfy_list_waiters(&a);

        // Element cascaded a -> b -> c; both hops recorded, parent first.
        let names: Vec<&str> = worker
            .pending_serve_propagations
            .iter()
            .map(|c| c.name)
            .collect();
        assert_eq!(names, ["LMOVE", "LMOVE"]);
        assert_eq!(worker.pending_serve_propagations[0].args[0], a);
        assert_eq!(worker.pending_serve_propagations[0].args[1], b);
        assert_eq!(worker.pending_serve_propagations[1].args[0], b);
        assert_eq!(worker.pending_serve_propagations[1].args[1], c);
        assert_eq!(
            worker.store.get_hot(&c).unwrap().as_list().unwrap().len(),
            1,
            "the element rests at the end of the chain"
        );
    }

    // ---- Lazy-purge drain at the satisfaction seam (issue 08) -------------

    /// A blocking wake whose `check_key` lazily purges an expired key must drain
    /// the store's `lazily_purged` report at the `drive_satisfaction` seam —
    /// mirroring `scatter_mget_drains_lazy_purge_report`. `try_satisfy_*` never
    /// routes through `execute_command_inner`, so without the wrapper drain the
    /// report would leak into the NEXT, unrelated message and its effects (a
    /// version bump + XREADGROUP drain) apply at the wrong seam. Pins all three:
    /// the purge physically fired, the report was drained here, and the parity
    /// version bump landed at this seam rather than being deferred.
    #[test]
    fn waiter_satisfaction_drains_lazy_purge_report() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"k");

        // A live BLPOP waiter (future deadline, receiver kept open) is parked on
        // the key so the driver enters its loop and calls `check_key`.
        let (mut entry, _rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        entry.deadline = Some(Instant::now() + std::time::Duration::from_secs(60));
        worker.wait_queue.register(entry).unwrap();

        // Seed the key as an already-expired list: a value is physically present
        // but its TTL has elapsed, so `check_key`'s `purge_if_expired` removes it.
        let mut v = Value::list();
        v.as_list_mut().unwrap().push_back(Bytes::from_static(b"x"));
        worker.store.set(key.clone(), v);
        worker.store.set_expiry(
            b"k",
            std::time::Instant::now() - std::time::Duration::from_secs(60),
        );
        assert!(worker.store.contains(b"k"));

        let version_before = worker.get_key_version(b"k");

        worker.try_satisfy_list_waiters(&key);

        // (a) The purge physically fired: the expired key is gone and the waiter
        // stayed blocked (an expired key is never a satisfiable wake).
        assert!(
            !worker.store.contains(b"k"),
            "the blocking-wake path must have lazily purged the expired key"
        );
        assert!(
            worker
                .wait_queue
                .has_waiters_for_kind(&key, WaiterKind::List),
            "the waiter stays blocked when its key expired instead of holding data"
        );
        // (b) The report was drained at the satisfaction seam — nothing leaks to
        // the next message.
        assert!(
            worker.store.take_lazily_purged().is_empty(),
            "drive_satisfaction must drain the lazy-purge report (no leak to the next message)"
        );
        // (c) The parity version bump landed *here*, not deferred: the only bump
        // possible on this call is `apply_lazy_purge_effects` (the waiter never
        // reached a `Done`, so `bumps_version` did not fire).
        assert_eq!(
            worker.get_key_version(b"k"),
            version_before.wrapping_add(1),
            "the lazy purge must bump the shard version at the satisfaction seam"
        );
    }

    // ---- Migration MOVED formatting (the folded-in IPv6 flag) -------------

    /// The migration-MOVED sent to blocked clients routes through the shared
    /// redirect seam, so an IPv6 target is bracketed
    /// (`MOVED <slot> [<v6>]:<port>`). The pre-fix inline `ip():port()` join
    /// produced the unparseable `MOVED <slot> 2001:db8::1:6379`.
    // FM-BLOCKING-008
    #[test]
    fn slot_migrated_moved_brackets_ipv6() {
        use std::net::SocketAddr;

        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"blocked-key");
        let slot = crate::shard::partition::slot_for_key(&key);

        let (entry, mut rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();

        let addr: SocketAddr = "[2001:db8::1]:6379".parse().unwrap();
        worker.handle_slot_migrated(slot, Some(addr));

        // handle_slot_migrated sends synchronously, so the reply is ready.
        match rx.try_recv().expect("waiter received the MOVED reply") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                format!("MOVED {slot} [2001:db8::1]:6379").as_bytes(),
                "IPv6 MOVED target must be bracketed and unambiguous"
            ),
            other => panic!("expected MOVED error, got {other:?}"),
        }
    }

    /// IPv4 targets keep the plain `host:port` rendering.
    // FM-BLOCKING-008
    #[test]
    fn slot_migrated_moved_ipv4_plain() {
        use std::net::SocketAddr;

        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"blocked-key-v4");
        let slot = crate::shard::partition::slot_for_key(&key);

        let (entry, mut rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();

        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        worker.handle_slot_migrated(slot, Some(addr));

        match rx.try_recv().expect("waiter received the MOVED reply") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                format!("MOVED {slot} 127.0.0.1:6380").as_bytes(),
            ),
            other => panic!("expected MOVED error, got {other:?}"),
        }
    }

    /// A migration notice whose new owner this node cannot name still wakes the
    /// blocked client — with `-CLUSTERDOWN`, the same rendering routing uses for
    /// "owner known, address unknown". Dropping the notice would park a
    /// zero-timeout `BLPOP` forever on a slot this node no longer serves.
    // FM-CLUSTER-038, FM-BLOCKING-008
    #[test]
    fn slot_migrated_without_a_known_target_replies_clusterdown() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"blocked-key-unknown-target");
        let slot = crate::shard::partition::slot_for_key(&key);

        let (entry, mut rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();

        worker.handle_slot_migrated(slot, None);

        match rx.try_recv().expect("waiter is woken, not left parked") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                format!("CLUSTERDOWN Hash slot {slot} not served").as_bytes(),
            ),
            other => panic!("expected CLUSTERDOWN error, got {other:?}"),
        }
        assert_eq!(
            worker.wait_queue.waiter_count(),
            0,
            "the waiter is drained, not merely replied to"
        );
    }

    // ---- Drop-elimination: every shard-side resolution *sends* ------------

    fn park(
        worker: &mut ShardWorker,
        conn_id: u64,
        keys: Vec<Bytes>,
        deadline: Option<Instant>,
    ) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        worker
            .wait_queue
            .register(WaitEntry {
                conn_id,
                keys,
                op: BlockingOp::BLPop,
                response_tx: tx,
                deadline,
                protocol_version: ProtocolVersion::default(),
            })
            .expect("registration under test is within the queue's bounds");
        rx
    }

    /// A registration refused by the global bound replies with that bound's own
    /// error and parks nothing. The queue hands the entry back rather than
    /// dropping it precisely so the refusal is not read as shard death.
    // FM-BLOCKING-006
    #[test]
    fn admission_refusal_at_the_global_limit_replies_and_registers_nothing() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        // One waiter node-wide, per-key bound disabled, so the second
        // registration can only be refused by the global bound.
        worker.set_wait_queue_limits(0, 1);

        let (tx_a, _rx_a) = oneshot::channel();
        worker.handle_block_wait(
            1,
            vec![Bytes::from_static(b"ka")],
            BlockingOp::BLPop,
            tx_a,
            None,
            ProtocolVersion::default(),
        );
        assert_eq!(worker.wait_queue.waiter_count(), 1);

        let (tx_b, mut rx_b) = oneshot::channel();
        worker.handle_block_wait(
            2,
            vec![Bytes::from_static(b"kb")],
            BlockingOp::BLPop,
            tx_b,
            None,
            ProtocolVersion::default(),
        );

        assert_eq!(
            rx_b.try_recv()
                .expect("the refusal is sent, not signalled by dropping the sender"),
            Response::error(crate::shard::wait_queue::MAX_BLOCKED_CONNECTIONS_ERR),
        );
        assert_eq!(
            worker.wait_queue.waiter_count(),
            1,
            "the refused waiter is not parked"
        );
        assert!(!worker.wait_queue.has_waiters(&Bytes::from_static(b"kb")));
    }

    /// The per-key bound refuses with its own distinct error, and — because the
    /// bound is checked across every requested key before any insertion — a
    /// multi-key wait refused on its last key leaves no entry under the earlier
    /// ones.
    // FM-BLOCKING-006
    #[test]
    fn admission_refusal_at_the_per_key_limit_replies_and_registers_nothing() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        // One waiter per key, global bound disabled.
        worker.set_wait_queue_limits(1, 0);

        let hot = Bytes::from_static(b"hot");
        let cold = Bytes::from_static(b"cold");
        let _parked = park(&mut worker, 1, vec![hot.clone()], None);

        let (tx, mut rx) = oneshot::channel();
        worker.handle_block_wait(
            2,
            vec![cold.clone(), hot.clone()],
            BlockingOp::BLPop,
            tx,
            None,
            ProtocolVersion::default(),
        );

        assert_eq!(
            rx.try_recv()
                .expect("the refusal is sent, not signalled by dropping the sender"),
            Response::error(crate::shard::wait_queue::MAX_WAITERS_PER_KEY_ERR),
        );
        assert_eq!(
            worker.wait_queue.waiter_count(),
            1,
            "only the first waiter is parked"
        );
        assert!(
            !worker.wait_queue.has_waiters(&cold),
            "a refusal on a later key leaves nothing under the earlier ones"
        );
    }

    /// Data arriving for a waiter whose deadline has already elapsed answers it
    /// with the op-aware timeout nil and consumes nothing. Before the fix the
    /// fast-path dropped `response_tx`, which the coordinator now reads as
    /// `-ERR shard unavailable` — an ordinary timeout reported as shard death.
    // FM-BLOCKING-002
    #[test]
    fn push_after_deadline_elapsed_replies_with_the_op_aware_nil() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"expired-waiter-key");
        let mut rx = park(
            &mut worker,
            7,
            vec![key.clone()],
            Some(Instant::now() - std::time::Duration::from_millis(50)),
        );

        let mut list = Value::list();
        list.as_list_mut()
            .unwrap()
            .push_back(Bytes::from_static(b"a"));
        worker.store.set(key.clone(), list);

        worker.try_satisfy_list_waiters(&key);

        assert_eq!(
            rx.try_recv()
                .expect("the timeout nil is sent, not signalled by dropping the sender"),
            Response::NullArray,
            "BLPOP's timeout shape, and never the shard-death error"
        );
        assert!(
            worker
                .store
                .get_hot(&key)
                .is_some_and(|v| v.as_list().is_some_and(|l| l.len() == 1)),
            "an expired waiter consumes nothing"
        );
        assert_eq!(worker.wait_queue.waiter_count(), 0);
    }

    /// Demotion drains every parked waiter with the role-change error, and the
    /// release is one-shot: a wait registered *after* it (the shard mailbox is
    /// serial, so ordering here is exact) stays parked.
    // FM-BLOCKING-007
    #[test]
    fn demotion_release_answers_every_waiter_and_empties_the_queue() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let mut parked: Vec<oneshot::Receiver<Response>> = (0..3u64)
            .map(|i| park(&mut worker, i, vec![Bytes::from(format!("k{i}"))], None))
            .collect();
        assert_eq!(worker.wait_queue.waiter_count(), 3);

        worker.handle_release_all_waiters();

        for (i, rx) in parked.iter_mut().enumerate() {
            assert_eq!(
                rx.try_recv()
                    .unwrap_or_else(|e| panic!("waiter {i} must be answered, got {e:?}")),
                Response::error(crate::ROLE_CHANGED_UNBLOCK_ERR),
            );
        }
        assert_eq!(worker.wait_queue.waiter_count(), 0);
        assert!(!worker.wait_queue.has_waiters(&Bytes::from_static(b"k0")));

        // XREAD BLOCK is legal on a replica, so the release must not become a
        // standing policy that drains everything registered afterwards.
        let later = Bytes::from_static(b"after-demotion");
        let mut after = park(&mut worker, 99, vec![later.clone()], None);
        assert_eq!(worker.wait_queue.waiter_count(), 1);
        assert!(
            after.try_recv().is_err(),
            "a wait registered after the release is not retroactively drained"
        );
    }

    /// A disconnect unregisters the connection out of *every* key of a
    /// multi-key wait, and a second unregister (the disconnect racing the
    /// timeout path) reports `AlreadyServed` rather than removing twice.
    // FM-BLOCKING-009
    #[test]
    fn unregister_after_disconnect_clears_every_key_of_a_multi_key_wait() {
        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let keys: Vec<Bytes> = ["ka", "kb", "kc"].iter().map(|k| Bytes::from(*k)).collect();
        let _rx = park(&mut worker, 9, keys.clone(), None);
        for key in &keys {
            assert!(worker.wait_queue.has_waiters(key));
        }

        let (ack_tx, mut ack_rx) = oneshot::channel();
        worker.handle_unregister_wait(9, ack_tx);
        assert!(matches!(
            ack_rx.try_recv().expect("the ack is always sent"),
            UnregisterAck::Unregistered
        ));

        assert_eq!(worker.wait_queue.waiter_count(), 0);
        for key in &keys {
            assert!(
                !worker.wait_queue.has_waiters(key),
                "no key keeps a dangling index entry for the gone connection"
            );
        }

        let (ack2_tx, mut ack2_rx) = oneshot::channel();
        worker.handle_unregister_wait(9, ack2_tx);
        assert!(matches!(
            ack2_rx.try_recv().expect("the ack is always sent"),
            UnregisterAck::AlreadyServed
        ));
    }
}
