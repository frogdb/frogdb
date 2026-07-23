use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use tokio::sync::oneshot;

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

        if let Err(e) = self.wait_queue.register(entry) {
            tracing::warn!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                error = %e,
                "Failed to register blocking wait"
            );
            // The response_tx was moved into entry, so we can't send an error back here.
            // The client will timeout.
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
    /// waiting on keys in the migrated slot.
    pub(crate) fn handle_slot_migrated(&mut self, slot: u16, target_addr: std::net::SocketAddr) {
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
                "Sending MOVED to blocked client after slot migration"
            );

            // Route through the shared redirect seam so the address is rendered
            // once, bracketing IPv6 (`MOVED <slot> [<v6>]:<port>`). The inline
            // `ip():port()` form joined with a bare colon was unparseable for
            // IPv6 targets.
            let _ = entry
                .response_tx
                .send(frogdb_types::redirect::moved(slot, target_addr));
        }

        BlockedMigrationMoved::inc_by(
            self.observability.metrics(),
            moved_count as u64,
            &shard_label,
        );

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
        while self.wait_queue.has_waiters_for_kind(key, kind) {
            match strat.check_key(&mut self.store, key) {
                KeyReady::No => break,
                KeyReady::DrainNoGroup => {
                    self.drain_stream_waiters_with_error(key);
                    return;
                }
                KeyReady::DrainWrongType => {
                    self.drain_stream_waiters_wrongtype(key);
                    return;
                }
                KeyReady::Yes => {}
            }

            let Some(entry) = self.wait_queue.pop_oldest_waiter_of_kind(key, kind) else {
                break;
            };

            // Deadline fast-path. The server is the canonical timeout authority;
            // if a popped waiter's deadline has already elapsed the server has
            // (or is about to) return a timeout nil, so skip it without
            // consuming and try the next one. This is a cheap optimization, not
            // the correctness backstop: a receiver can still be dropped in the
            // window *after* this check and *before* the `send` below (the
            // server fires precisely at the deadline). That residual race is
            // closed by restoring the consumed data on send failure — see the
            // `Err` arm and [`Restore`] — so no element is ever popped and
            // delivered to nobody.
            if entry.deadline.is_some_and(|d| d <= Instant::now()) {
                continue;
            }

            match strat.satisfy(&mut self.store, key, &entry) {
                Satisfaction::Retry => continue,
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

    /// Drain XREADGROUP waiters for a key and send NOGROUP error.
    ///
    /// Only XREADGROUP waiters are drained — XREAD waiters remain blocked,
    /// matching Redis behaviour where a plain XREAD client stays blocked when
    /// the stream key is deleted or expires. It will either time-out or be
    /// woken when a new stream is created under the same key.
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

    /// Drain XREADGROUP waiters for a key and send WRONGTYPE error.
    ///
    /// Called when the key's type has changed (e.g., SET overwrote a stream).
    /// Only XREADGROUP waiters are drained — XREAD waiters stay blocked (see
    /// `drain_stream_waiters_with_error` for rationale).
    fn drain_stream_waiters_wrongtype(&mut self, key: &Bytes) {
        while let Some(entry) = self.wait_queue.pop_oldest_xreadgroup_waiter(key) {
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
    /// The key is no longer satisfiable for this waiter (it lost a race to an
    /// earlier waiter that emptied the key); drop the waiter and re-loop.
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
            _ => {
                debug_assert!(
                    false,
                    "pop_oldest_waiter_of_kind(List) returned non-list op"
                );
                Satisfaction::Retry
            }
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
            _ => {
                debug_assert!(
                    false,
                    "pop_oldest_waiter_of_kind(SortedSet) returned non-zset op"
                );
                Satisfaction::Retry
            }
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
            _ => {
                debug_assert!(
                    false,
                    "pop_oldest_waiter_of_kind(Stream) returned non-stream op"
                );
                Satisfaction::Retry
            }
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
        worker
            .store
            .set_expiry(b"k", Instant::now() - std::time::Duration::from_secs(60));
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
    #[test]
    fn slot_migrated_moved_brackets_ipv6() {
        use std::net::SocketAddr;

        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"blocked-key");
        let slot = crate::shard::partition::slot_for_key(&key);

        let (entry, mut rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();

        let addr: SocketAddr = "[2001:db8::1]:6379".parse().unwrap();
        worker.handle_slot_migrated(slot, addr);

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
    #[test]
    fn slot_migrated_moved_ipv4_plain() {
        use std::net::SocketAddr;

        let (mut worker, _msg_tx, _conn_tx) = build_worker();
        let key = Bytes::from_static(b"blocked-key-v4");
        let slot = crate::shard::partition::slot_for_key(&key);

        let (entry, mut rx) = make_entry(BlockingOp::BLPop, vec![key.clone()]);
        worker.wait_queue.register(entry).unwrap();

        let addr: SocketAddr = "127.0.0.1:6380".parse().unwrap();
        worker.handle_slot_migrated(slot, addr);

        match rx.try_recv().expect("waiter received the MOVED reply") {
            Response::Error(bytes) => assert_eq!(
                &bytes[..],
                format!("MOVED {slot} 127.0.0.1:6380").as_bytes(),
            ),
            other => panic!("expected MOVED error, got {other:?}"),
        }
    }
}
