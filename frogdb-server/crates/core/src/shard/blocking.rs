use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use tokio::sync::oneshot;

use crate::command::WaiterKind;
use crate::store::{HashMapStore, Store};
use crate::types::{BlockingOp, Direction, StreamEntry, Value};

use super::helpers::format_xread_response;
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
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Handle unregistering a blocking wait (disconnect or explicit cancel).
    pub(crate) fn handle_unregister_wait(&mut self, conn_id: u64) {
        let removed = self.wait_queue.unregister(conn_id);
        if !removed.is_empty() {
            tracing::trace!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                count = removed.len(),
                "Unregistered blocking waits on disconnect"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
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

        self.observability.metrics_recorder.increment_counter(
            "frogdb_blocked_migration_moved_total",
            moved_count as u64,
            &[("shard", &shard_label)],
        );

        self.observability.metrics_recorder.record_gauge(
            "frogdb_blocked_clients",
            self.wait_queue.waiter_count() as f64,
            &[("shard", &shard_label)],
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
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_blocked_timeout_total",
                    1,
                    &[("shard", &shard_label)],
                );
            }

            // Update blocked clients gauge
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Try to satisfy list waiters after a list write operation.
    ///
    /// Called after LPUSH, RPUSH, LPUSHX, RPUSHX, BLMOVE, BRPOPLPUSH operations.
    pub fn try_satisfy_list_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut ListSatisfaction, key, 0);
    }

    /// Try to satisfy sorted set waiters after a sorted set write operation.
    ///
    /// Called after ZADD operations.
    pub fn try_satisfy_zset_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut ZsetSatisfaction, key, 0);
    }

    /// Try to satisfy stream waiters after a stream write operation.
    ///
    /// Called after XADD, DEL, UNLINK, SET, XGROUP DESTROY, and RENAME operations.
    pub fn try_satisfy_stream_waiters(&mut self, key: &Bytes) {
        self.drive_satisfaction(&mut StreamSatisfaction, key, 0);
    }

    /// Generic satisfaction driver shared by every waiter kind.
    ///
    /// Owns everything that is the *same* across the families — the FIFO loop,
    /// the BLMove wake-cascade recursion, the depth cap, the version bump, the
    /// timeout re-validation, and the completion/metrics — while the per-op "is
    /// this key satisfiable / what reply does this op produce / where does the
    /// wake cascade" logic lives behind the [`WaiterSatisfaction`] seam. The
    /// strategy sees only the store; this driver is the sole mutator of the wait
    /// queue.
    fn drive_satisfaction(
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

            // Lost-element race fix. The server is the canonical timeout
            // authority; this shard only completes waiters on a genuine wake. If
            // a popped waiter's deadline has already elapsed, or its receiver is
            // gone, the server has (or is about to) return a timeout nil — so
            // consuming store data for it would pop an element and deliver it to
            // nobody. Re-validate before touching the value: drop the doomed
            // waiter without consuming, and try the next one. Because the shard
            // reads `now` strictly before it pops (synchronously, no await), and
            // the server's `biased` select favours a delivered response over a
            // simultaneous deadline, every element the shard does pop is
            // guaranteed to reach a still-waiting receiver.
            if entry.deadline.is_some_and(|d| d <= Instant::now()) || entry.response_tx.is_closed()
            {
                continue;
            }

            match strat.satisfy(&mut self.store, key, &entry) {
                Satisfaction::Retry => continue,
                Satisfaction::Reject(reply) => self.complete_blocked_waiter(entry, reply),
                Satisfaction::Done { reply, cascade } => {
                    if strat.bumps_version() {
                        self.increment_version();
                    }
                    self.complete_blocked_waiter(entry, reply);
                    // A BLMove/BRPOPLPUSH pushes to its destination; wake any
                    // blockers on that key so wake chains propagate.
                    if let Some(dest) = cascade {
                        self.drive_satisfaction(strat, &dest, depth + 1);
                    }
                }
            }
        }
    }

    /// Send a response to a satisfied blocked client and record metrics.
    fn complete_blocked_waiter(&self, entry: WaitEntry, response: Response) {
        tracing::debug!(
            shard_id = self.shard_id(),
            conn_id = entry.conn_id,
            "Blocked client unblocked"
        );

        let _ = entry.response_tx.send(response);

        let shard_label = self.shard_id().to_string();
        self.observability.metrics_recorder.increment_counter(
            "frogdb_blocked_satisfied_total",
            1,
            &[("shard", &shard_label)],
        );

        self.observability.metrics_recorder.record_gauge(
            "frogdb_blocked_clients",
            self.wait_queue.waiter_count() as f64,
            &[("shard", &shard_label)],
        );
    }

    /// Drain XREADGROUP waiters for a key and send NOGROUP error.
    ///
    /// Only XREADGROUP waiters are drained — XREAD waiters remain blocked,
    /// matching Redis behaviour where a plain XREAD client stays blocked when
    /// the stream key is deleted or expires. It will either time-out or be
    /// woken when a new stream is created under the same key.
    fn drain_stream_waiters_with_error(&mut self, key: &Bytes) {
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
    },
    /// The key is no longer satisfiable for this waiter (it lost a race to an
    /// earlier waiter that emptied the key); drop the waiter and re-loop.
    Retry,
    /// A terminal reply that consumed nothing (WRONGTYPE, NOGROUP); deliver it
    /// and drop the waiter without touching the stored value.
    Reject(Response),
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
/// so it can use the hot-only, no-promote read (`get_hot`) the satisfiability
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
                                Response::bulk(value),
                            ]),
                            cascade: None,
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
                                Response::bulk(value),
                            ]),
                            cascade: None,
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

                Satisfaction::Done {
                    reply: Response::bulk(value),
                    cascade: Some(dest.clone()),
                }
            }
            BlockingOp::BLMPop { direction, count } => {
                let mut elements = Vec::new();
                if let Some(list) = store.get_mut(key).and_then(|v| v.as_list_mut()) {
                    for _ in 0..*count {
                        let elem = match direction {
                            Direction::Left => list.pop_front(),
                            Direction::Right => list.pop_back(),
                        };
                        match elem {
                            Some(e) => elements.push(Response::bulk(e)),
                            None => break,
                        }
                    }
                }

                if elements.is_empty() {
                    return Satisfaction::Retry;
                }

                cleanup_empty_list(store, key);
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ]),
                    cascade: None,
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
                        Response::bulk(member),
                        zset_score_reply(score, is_resp3),
                    ]),
                    cascade: None,
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
                        Response::bulk(member),
                        zset_score_reply(score, is_resp3),
                    ]),
                    cascade: None,
                }
            }
            BlockingOp::BZMPop { min, count } => {
                let mut elements = Vec::new();
                if let Some(zset) = store.get_mut(key).and_then(|v| v.as_sorted_set_mut()) {
                    let popped = if *min {
                        zset.pop_min(*count)
                    } else {
                        zset.pop_max(*count)
                    };
                    for (member, score) in popped {
                        elements.push(Response::Array(vec![
                            Response::bulk(member),
                            zset_score_reply(score, is_resp3),
                        ]));
                    }
                }

                if elements.is_empty() {
                    return Satisfaction::Retry;
                }

                cleanup_empty_zset(store, key);
                Satisfaction::Done {
                    reply: Response::Array(vec![
                        Response::bulk(key.clone()),
                        Response::Array(elements),
                    ]),
                    cascade: None,
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

    let last_delivered = group.last_delivered_id;
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
            Satisfaction::Done { reply, cascade } => {
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
            Satisfaction::Done { reply, cascade } => {
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
            Satisfaction::Done { reply, cascade } => {
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
    /// timeout (its receiver dropped) must not pop the element into the
    /// abandoned channel — the element would be removed from the store and
    /// delivered to nobody.
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

        // The element must still be in the store — not popped into the void.
        let list = worker.store.get_hot(&key).expect("key must survive");
        assert_eq!(
            list.as_list().unwrap().len(),
            1,
            "element must not be lost to an abandoned waiter"
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
        let slot = crate::shard::helpers::slot_for_key(&key);

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
        let slot = crate::shard::helpers::slot_for_key(&key);

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
