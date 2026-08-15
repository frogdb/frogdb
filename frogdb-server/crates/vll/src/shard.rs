//! Per-shard VLL state machine.
//!
//! [`VllShardState`] owns the lock table, transaction queue, and
//! continuation lock for a single shard, and exposes a small API that
//! callers use instead of reaching into those primitives directly.
//!
//! The state machine doesn't run scatter operations itself — that work lives
//! on the host worker because it touches per-shard storage. Execution is
//! split into [`Self::dequeue_for_execution`] (caller takes the op out of
//! the queue) and [`Self::release_after_execution`] (caller signals the op
//! has finished, releasing locks and unblocking waiters).

use std::fmt::Debug;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::Instant;

use super::lock_table::{GrantOutcome, LockTable};
use super::queue::{ContinuationLock, TransactionQueue, VllPendingOp};
use super::types::{LockMode, PendingOpState, ShardReadyResult, VllError};

/// Default queue capacity used when no explicit limit is provided.
pub const DEFAULT_MAX_QUEUE_DEPTH: usize = 10000;

/// Threshold at which [`EnqueueOutcome::queue_depth_warning`] is set.
pub const QUEUE_DEPTH_WARN_THRESHOLD: usize = 8000;

/// How long a continuation-lock request stays parked waiting for the shard
/// to drain before it is failed with `LockTimeout`.
///
/// Derived from — always strictly below — the coordinator's
/// [`DEFAULT_LOCK_ACQUISITION_TIMEOUT`](crate::DEFAULT_LOCK_ACQUISITION_TIMEOUT),
/// so the shard resolves (and cleans up after) its own parked request before
/// the coordinator gives up on it, by construction rather than by two
/// hand-tuned constants that could drift apart in a later edit. The margin
/// itself (currently half) is a tuning knob, not the safety property: what
/// actually makes a lost race harmless is `grant_continuation` installing
/// neither the lock nor the release receiver when the requester has already
/// given up (`ready_tx` closed) — see [FM-VLL-003](../../../../specs/vll.md#fm-vll-003--continuation-lock-requested-while-the-shard-queue-has-not-drained).
pub const CONTINUATION_DRAIN_TIMEOUT: Duration = Duration::from_millis(
    (crate::coordinator::DEFAULT_LOCK_ACQUISITION_TIMEOUT.as_millis() / 2) as u64,
);

/// Hard cap on how long one continuation lock may be held.
///
/// A continuation lock takes the shard exclusively: while it is held every
/// foreign SCA request is refused ([`VllError::ShardBusy`]), so an unbounded
/// hold is a node-wide availability event with no escape. The cap turns that
/// into a bounded one — past it the lock is *revoked*: the holder's
/// coordinator is told to abandon the work and drop its guard. FoundationDB
/// caps a transaction's life at 5 s and CockroachDB expires a lock whose
/// owner stops heartbeating for the same reason.
///
/// This is a liveness backstop, not a correctness clock: nothing about the
/// *outcome* of the holder's work is decided here — the revocation is
/// delivered to the holder, which resolves its own transaction.
pub const CONTINUATION_MAX_HOLD: Duration = Duration::from_millis(5000);

/// A continuation-lock request parked until the shard drains.
///
/// Holds the requester's channels so the state machine can answer it later:
/// `Ready` from a drain point, or `Failed(LockTimeout)` once `deadline` passes.
#[derive(Debug)]
struct PendingContinuation {
    txid: u64,
    conn_id: u64,
    ready_tx: oneshot::Sender<ShardReadyResult>,
    release_rx: oneshot::Receiver<()>,
    revoke_tx: oneshot::Sender<VllError>,
    deadline: Instant,
}

/// What [`VllShardState::next_continuation_event`] observed and applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationEvent {
    /// The held lock's release signal fired; the lock has been cleared.
    Released,
    /// A parked request's drain deadline passed; it has been failed with
    /// [`VllError::LockTimeout`] and the drain barrier is lifted.
    DrainTimedOut,
    /// The held lock outlived [`CONTINUATION_MAX_HOLD`]; its holder has been
    /// sent a [`VllError::Revoked`] notice. The lock itself stays installed
    /// until the holder's release signal arrives — only the holder knows when
    /// its shard-side work has actually stopped.
    HoldCapExpired,
}

/// Per-shard VLL state machine.
///
/// Owns the lock table, transaction queue, and continuation lock for a
/// single shard. Generic over the operation payload type `O` (e.g.,
/// `ScatterOp` in `frogdb-core`).
#[derive(Debug)]
pub struct VllShardState<O: Debug> {
    lock_table: Option<LockTable>,
    tx_queue: Option<TransactionQueue<O>>,
    continuation_lock: Option<ContinuationLock>,
    pending_continuation_release: Option<oneshot::Receiver<()>>,
    /// Back-channel to the current holder's coordinator: the shard's only way
    /// to make a holder let go. Installed with the lock, fired at most once
    /// (by a wound, a `SCRIPT KILL`, or the hold cap), and dropped with the
    /// lock.
    continuation_revoke_tx: Option<oneshot::Sender<VllError>>,
    /// When the held lock's [`CONTINUATION_MAX_HOLD`] cap expires. Cleared
    /// once the cap has fired so a revoked-but-not-yet-released lock does not
    /// spin the host's event loop.
    continuation_hold_deadline: Option<Instant>,
    pending_continuation: Option<PendingContinuation>,
    /// Ops handed to the host by [`VllShardState::dequeue_for_execution`] that
    /// have not yet reported back through
    /// [`VllShardState::release_after_execution`]. They have left the queue but
    /// still hold their locks, so the shard is not drained while any is
    /// outstanding.
    executing_ops: usize,
    max_queue_depth: usize,
}

impl<O: Debug> Default for VllShardState<O> {
    fn default() -> Self {
        Self::with_max_queue_depth(DEFAULT_MAX_QUEUE_DEPTH)
    }
}

impl<O: Debug> VllShardState<O> {
    /// Construct a new state machine with the given queue capacity.
    pub fn with_max_queue_depth(max_queue_depth: usize) -> Self {
        Self {
            lock_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            continuation_revoke_tx: None,
            continuation_hold_deadline: None,
            pending_continuation: None,
            executing_ops: 0,
            max_queue_depth,
        }
    }

    fn ensure_initialized(&mut self) -> (&mut LockTable, &mut TransactionQueue<O>) {
        if self.lock_table.is_none() {
            self.lock_table = Some(LockTable::new());
        }
        if self.tx_queue.is_none() {
            self.tx_queue = Some(TransactionQueue::new(self.max_queue_depth));
        }
        (
            self.lock_table.as_mut().unwrap(),
            self.tx_queue.as_mut().unwrap(),
        )
    }

    /// Enqueue an SCA lock request and try to acquire locks immediately.
    ///
    /// The `ready_tx` is signaled `Ready` if locks are acquired right away,
    /// `Failed(QueueFull)` if the queue is full, `Failed(ShardBusy)` if a
    /// continuation lock currently holds the shard exclusively, or remains
    /// pending if the request must wait behind earlier conflicting txids.
    /// Pending requests progress on later calls to
    /// [`Self::try_advance_pending_locks`] or [`Self::release_after_execution`].
    ///
    /// **Continuation invariant.** A continuation lock takes the shard
    /// exclusive — its owner (a cross-shard Lua script today) routes
    /// sub-commands through `ScriptSubCommand`, not SCA. Any SCA request
    /// arriving while a continuation lock is held is therefore from a
    /// different connection and would interleave with the lock owner's
    /// sub-commands, breaking isolation. We reject such requests with
    /// `ShardBusy` so the caller can retry after the lock releases.
    ///
    /// The same refusal applies while a continuation request is *parked*
    /// waiting for the queue to drain ([`Self::request_continuation_lock`]):
    /// admitting new work would keep the queue non-empty and starve the drain
    /// until its deadline.
    ///
    /// `wound_tx` is this request's back-channel for the *other* direction:
    /// once its locks are granted, an older transaction that needs one of them
    /// wounds this op here (see [`VllPendingOp::wound`] and
    /// [`GrantOutcome::WoundYounger`]).
    pub fn enqueue_lock_request(
        &mut self,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: O,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        wound_tx: oneshot::Sender<VllError>,
    ) -> EnqueueOutcome {
        if self.continuation_held_or_pending() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
            return EnqueueOutcome {
                queue_depth_warning: None,
                enqueue_failed: true,
            };
        }

        let (lock_table, tx_queue) = self.ensure_initialized();
        let queue_depth = tx_queue.len();
        let queue_depth_warning =
            (queue_depth >= QUEUE_DEPTH_WARN_THRESHOLD).then_some(queue_depth);

        if !tx_queue.has_capacity() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::QueueFull));
            return EnqueueOutcome {
                queue_depth_warning,
                enqueue_failed: true,
            };
        }

        lock_table.declare(&keys, txid, mode);

        let pending_op = VllPendingOp::new(txid, keys.clone(), operation, ready_tx, wound_tx);
        if let Err(_e) = tx_queue.enqueue(pending_op) {
            lock_table.release(&keys, txid);
            return EnqueueOutcome {
                queue_depth_warning,
                enqueue_failed: true,
            };
        }

        Self::try_acquire_for(lock_table, tx_queue, txid);

        EnqueueOutcome {
            queue_depth_warning,
            enqueue_failed: false,
        }
    }

    /// Advance one pending op: grant if it can be granted, wound the younger
    /// holders standing in its way if it cannot.
    ///
    /// **Wound-wait on the SCA path.** A grant refused by a *younger* holder
    /// (`GrantOutcome::WoundYounger`) is the one refusal that inverts the txid
    /// order, and inverted edges are what let two multi-shard transactions
    /// deadlock when they win shards in opposite orders. So the older
    /// requester stays pending and the younger holders are told to give way.
    /// The notice is advisory — see [`VllPendingOp::wound`] for why the shard
    /// must not release a victim's locks itself — so this call grants nothing;
    /// the requester is granted from the drain point the victim's own abort
    /// produces. A victim already dequeued for execution is beyond wounding
    /// and is simply waited for: it holds no cross-shard wait edge of its own,
    /// so it always finishes.
    fn try_acquire_for(lock_table: &mut LockTable, tx_queue: &mut TransactionQueue<O>, txid: u64) {
        // The `Pending` guard states the caller's contract; no test can force
        // it false, so mutating it to `true` survives. Both callers already
        // pass pending txids only (enqueue has just enqueued, and
        // `try_advance_pending_locks` filters), and a `Ready` op holds every
        // key it asked for, so re-granting one takes the `Granted` arm and
        // finds `ready_tx` already spent. Kept so a future caller's mistake is
        // a no-op rather than a second ready notification.
        let keys = match tx_queue.get_mut(txid) {
            Some(op) if op.state == PendingOpState::Pending => op.keys.clone(),
            _ => return,
        };

        match lock_table.try_grant(&keys, txid) {
            GrantOutcome::Granted => {
                if let Some(op) = tx_queue.get_mut(txid)
                    && let Some(ready_tx) = op.mark_ready()
                {
                    let _ = ready_tx.send(ShardReadyResult::Ready);
                }
            }
            GrantOutcome::Blocked => {}
            GrantOutcome::WoundYounger(victims) => {
                for victim in victims {
                    if let Some(op) = tx_queue.get_mut(victim) {
                        op.wound();
                    }
                }
            }
        }
    }

    /// Try to advance lock acquisition for every pending op in the queue.
    ///
    /// Called after an op completes (releasing locks) so that newly-unblocked
    /// pending ops can transition to Ready.
    pub fn try_advance_pending_locks(&mut self) {
        let Some(tx_queue) = self.tx_queue.as_ref() else {
            return;
        };
        let pending_txids: Vec<u64> = tx_queue
            .iter()
            .filter(|(_, op)| op.state == PendingOpState::Pending)
            .map(|(&txid, _)| txid)
            .collect();
        for txid in pending_txids {
            if let (Some(lock_table), Some(tx_queue)) =
                (self.lock_table.as_mut(), self.tx_queue.as_mut())
            {
                Self::try_acquire_for(lock_table, tx_queue, txid);
            }
        }
    }

    /// Take an operation out of the queue for execution.
    ///
    /// The caller is expected to execute the operation and then call
    /// [`Self::release_after_execution`] to release locks and advance
    /// remaining pending ops.
    pub fn dequeue_for_execution(&mut self, txid: u64) -> Option<DequeuedOp<O>> {
        let tx_queue = self.tx_queue.as_mut()?;
        let mut op = tx_queue.dequeue(txid)?;
        op.state = PendingOpState::Executing;
        // The op has left the queue but still holds its locks: the shard is
        // not drained until the host reports back.
        self.executing_ops += 1;
        Some(DequeuedOp {
            txid: op.txid,
            keys: op.keys,
            operation: op.operation,
        })
    }

    /// Release a transaction's locks and intents after a dequeued op
    /// finishes executing.
    ///
    /// Triggers a pass over remaining pending ops to advance newly-unblocked
    /// locks, and — as a drain point — may grant a parked continuation
    /// request.
    ///
    /// Pairs 1:1 with [`Self::dequeue_for_execution`]; `saturating_sub` keeps
    /// an unpaired call from wrapping the outstanding-op count into a state
    /// where the shard could never drain again.
    pub fn release_after_execution(&mut self, txid: u64, keys: &[Bytes]) {
        self.executing_ops = self.executing_ops.saturating_sub(1);
        if let Some(lock_table) = self.lock_table.as_mut() {
            lock_table.release(keys, txid);
        }
        self.try_advance_pending_locks();
        self.try_grant_pending_continuation();
    }

    /// Abort a pending or ready operation, releasing any held locks and
    /// advancing waiters whose locks may now be acquirable.
    ///
    /// `LockTable::release` is a single transition covering granted and
    /// still-pending intents alike, so no state inspection is needed here.
    pub fn abort(&mut self, txid: u64) {
        let Some(tx_queue) = self.tx_queue.as_mut() else {
            return;
        };
        let Some(op) = tx_queue.dequeue(txid) else {
            return;
        };
        if let Some(lock_table) = self.lock_table.as_mut() {
            lock_table.release(&op.keys, txid);
        }
        // Advance waiters that may have been blocked behind the aborted op.
        self.try_advance_pending_locks();
        // Aborting the last queued op drains the shard — the other way a
        // parked continuation request gets its lock.
        self.try_grant_pending_continuation();
    }

    /// Request a continuation (drain + shard-exclusive) lock.
    ///
    /// The shard must be drained before it can be taken exclusively — an op
    /// still queued, or one already dequeued and still executing, would
    /// release its own locks while the continuation owner believes it holds
    /// the shard alone.
    ///
    /// **This call never waits.** It runs on the host's shard event loop, and
    /// that loop is exactly what drains the queue (by processing execute /
    /// abort messages), so waiting here would deadlock the drain against
    /// itself and stall the shard for the whole timeout. Instead an
    /// undrained shard *parks* the request: the caller's channels are stored,
    /// new SCA work is refused meanwhile (see [`Self::enqueue_lock_request`]),
    /// and the lock is granted from the next drain point. If the shard has
    /// still not drained [`CONTINUATION_DRAIN_TIMEOUT`] later, the host's
    /// [`Self::next_continuation_event`] arm fails the request with
    /// [`VllError::LockTimeout`] and takes no lock.
    ///
    /// The host is responsible for driving [`Self::next_continuation_event`]
    /// from its event loop; without it a parked request never times out and a
    /// granted lock is never cleared.
    ///
    /// **Wound-wait.** A collision with an existing claim is not resolved by
    /// refusing whoever arrives second: that rule has no priority order, so
    /// two cross-shard scripts over overlapping shard sets can each win a
    /// different shard, refuse each other, release, and retry into the same
    /// interleaving forever. Instead the *older* transaction (lower `txid`)
    /// wins: it wounds the younger claim — the younger holder is told to
    /// abandon its work through `revoke_tx`, the younger parked request is
    /// failed with [`VllError::Wounded`] — and the older request takes or
    /// parks for the lock. A younger requester colliding with an older claim
    /// still gets [`VllError::ShardBusy`]; that refusal is order-respecting
    /// and cannot cycle. Because the total order on `txid` is a global one,
    /// the same transaction wins on *every* shard, so progress is guaranteed.
    pub fn request_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
        revoke_tx: oneshot::Sender<VllError>,
    ) {
        // The parking slot holds at most one claim, so it is resolved first:
        // whichever of the two is older keeps it. Doing this before the held
        // check matters — a request that parks behind a wounded holder must
        // not silently overwrite an even older claim already waiting there.
        if let Some(parked) = self.pending_continuation.as_ref() {
            if txid >= parked.txid {
                let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
                return;
            }
            // A parked claim has no shard-side work to unwind — failing its
            // requester is the whole wound.
            if let Some(victim) = self.pending_continuation.take() {
                let _ = victim
                    .ready_tx
                    .send(ShardReadyResult::Failed(VllError::Wounded));
            }
        }

        if let Some(held) = self.continuation_lock.as_ref() {
            if txid >= held.txid {
                let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
                return;
            }
            // Wound the younger holder and park behind it: its release is the
            // drain point that hands this request the lock. Parking (rather
            // than refusing) is what makes the wound worth anything — the
            // older transaction must end up holding what it wounded for.
            self.revoke_continuation(VllError::Wounded);
            self.park_continuation(txid, conn_id, ready_tx, release_rx, revoke_tx);
            return;
        }

        if self.is_drained() {
            self.grant_continuation(txid, conn_id, ready_tx, release_rx, revoke_tx);
            return;
        }

        self.park_continuation(txid, conn_id, ready_tx, release_rx, revoke_tx);
    }

    /// Park a continuation request until the shard drains (or, when it was
    /// parked behind a wounded holder, until that holder releases).
    fn park_continuation(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
        revoke_tx: oneshot::Sender<VllError>,
    ) {
        self.pending_continuation = Some(PendingContinuation {
            txid,
            conn_id,
            ready_tx,
            release_rx,
            revoke_tx,
            deadline: Instant::now() + CONTINUATION_DRAIN_TIMEOUT,
        });
    }

    /// Tell the current holder's coordinator to abandon its work.
    ///
    /// Fires at most once per grant — the sender is taken, so a wound followed
    /// by a `SCRIPT KILL` (or by the hold cap) does not double-signal. The
    /// lock stays installed: only the holder knows when its shard-side work
    /// has stopped, and it says so by dropping its guard, which is the release
    /// signal this shard already waits on.
    fn revoke_continuation(&mut self, reason: VllError) -> bool {
        match self.continuation_revoke_tx.take() {
            Some(tx) => tx.send(reason).is_ok(),
            None => false,
        }
    }

    /// Revoke the continuation lock this shard holds, if any.
    ///
    /// The `SCRIPT KILL` / `FUNCTION KILL` entry point: killing a cross-shard
    /// script has to take its continuation locks away too, or the shard stays
    /// exclusively held — refusing every other connection's work — until a
    /// future nobody is waiting on happens to finish. Returns whether a holder
    /// was notified.
    pub fn revoke_held_continuation(&mut self) -> bool {
        self.revoke_continuation(VllError::Revoked)
    }

    /// Whether this shard already has a continuation claim — held or parked.
    fn continuation_held_or_pending(&self) -> bool {
        self.continuation_lock.is_some() || self.pending_continuation.is_some()
    }

    /// Whether the shard holds no SCA work: nothing queued, nothing executing.
    fn is_drained(&self) -> bool {
        self.executing_ops == 0 && self.tx_queue.as_ref().is_none_or(|q| q.is_empty())
    }

    /// Install the continuation lock and answer its requester.
    ///
    /// The lock and the release receiver go in together — a lock without its
    /// receiver could never be released. If the requester has already given up
    /// (its `ready_tx` is closed, e.g. the coordinator timed out first) neither
    /// is installed: taking a lock nobody will release would refuse every later
    /// SCA request on this shard.
    fn grant_continuation(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
        revoke_tx: oneshot::Sender<VllError>,
    ) {
        if ready_tx.send(ShardReadyResult::Ready).is_err() {
            return;
        }
        self.continuation_lock = Some(ContinuationLock::new(txid, conn_id));
        self.pending_continuation_release = Some(release_rx);
        self.continuation_revoke_tx = Some(revoke_tx);
        self.continuation_hold_deadline = Some(Instant::now() + CONTINUATION_MAX_HOLD);
    }

    /// Grant a parked continuation request if the shard has drained.
    ///
    /// Called from every drain point ([`Self::release_after_execution`],
    /// [`Self::abort`]); a no-op when nothing is parked.
    fn try_grant_pending_continuation(&mut self) {
        if !self.is_drained() {
            return;
        }
        let Some(pending) = self.pending_continuation.take() else {
            return;
        };
        self.grant_continuation(
            pending.txid,
            pending.conn_id,
            pending.ready_tx,
            pending.release_rx,
            pending.revoke_tx,
        );
    }

    /// Wait for the next continuation-lock event and apply it.
    ///
    /// One host-loop arm serves both halves of the continuation lifecycle —
    /// they are mutually exclusive, since a request is only parked when no
    /// lock is held:
    ///
    /// - lock held → the release signal, after which the lock is cleared —
    ///   raced against [`CONTINUATION_MAX_HOLD`], which revokes a lock held
    ///   past the cap instead of clearing it;
    /// - request parked → its drain deadline, after which the requester is
    ///   failed with [`VllError::LockTimeout`] and the drain barrier lifts.
    ///
    /// The two halves are no longer mutually exclusive: wound-wait parks the
    /// older requester *behind* the younger holder it wounded, so both can be
    /// set at once. The held half wins while it lasts, and clearing the lock
    /// runs the same drain-point grant every other release path runs, so the
    /// parked request is answered as soon as the holder lets go.
    ///
    /// With neither, the future never completes, so it is safe to drive from a
    /// `select!` arm that recreates it every iteration. Cancel-safe: a losing
    /// `select!` iteration leaves the stored receiver and deadline untouched,
    /// and the state transition happens with no await between it and the event
    /// it applies.
    pub async fn next_continuation_event(&mut self) -> ContinuationEvent {
        if self.pending_continuation_release.is_some() {
            let hold_deadline = self.continuation_hold_deadline;
            let release_rx = self
                .pending_continuation_release
                .as_mut()
                .expect("checked above");
            let released = match hold_deadline {
                Some(deadline) => tokio::select! {
                    biased;
                    _ = release_rx => true,
                    _ = tokio::time::sleep_until(deadline) => false,
                },
                None => {
                    let _ = release_rx.await;
                    true
                }
            };

            if !released {
                // Hold cap expired. Tell the holder to let go and disarm the
                // cap: the lock clears when the holder's release arrives, not
                // on this shard's say-so.
                self.continuation_hold_deadline = None;
                self.revoke_continuation(VllError::Revoked);
                return ContinuationEvent::HoldCapExpired;
            }

            self.continuation_lock = None;
            self.pending_continuation_release = None;
            self.continuation_revoke_tx = None;
            self.continuation_hold_deadline = None;
            // Releasing is a drain point like any other: a request parked
            // behind this holder (wound-wait) gets its lock here.
            self.try_grant_pending_continuation();
            return ContinuationEvent::Released;
        }

        if let Some(deadline) = self.pending_continuation.as_ref().map(|p| p.deadline) {
            tokio::time::sleep_until(deadline).await;
            if let Some(pending) = self.pending_continuation.take() {
                let _ = pending
                    .ready_tx
                    .send(ShardReadyResult::Failed(VllError::LockTimeout));
            }
            return ContinuationEvent::DrainTimedOut;
        }

        std::future::pending().await
    }

    /// Connection id of the current continuation-lock owner, if any.
    pub fn continuation_lock_owner(&self) -> Option<u64> {
        self.continuation_lock.as_ref().map(|l| l.conn_id)
    }

    /// Number of pending operations in the queue.
    pub fn queue_depth(&self) -> usize {
        self.tx_queue.as_ref().map_or(0, |q| q.len())
    }

    /// Iterate over the queue's pending ops in txid order.
    pub fn iter_pending_ops(&self) -> impl Iterator<Item = PendingOpSnapshot<'_, O>> {
        self.tx_queue
            .as_ref()
            .into_iter()
            .flat_map(|q| q.iter())
            .map(|(_, op)| PendingOpSnapshot {
                txid: op.txid,
                state: op.state,
                key_count: op.keys.len(),
                age_ms: op.age().as_millis() as u64,
                operation: &op.operation,
            })
    }

    /// Snapshot the current continuation lock for diagnostics.
    pub fn continuation_lock_snapshot(&self) -> Option<ContinuationLockSnapshot> {
        self.continuation_lock
            .as_ref()
            .map(|l| ContinuationLockSnapshot {
                txid: l.txid,
                conn_id: l.conn_id,
                age_ms: l.age().as_millis() as u64,
            })
    }

    /// Snapshot the lock table for diagnostics.
    pub fn intent_snapshots(&self) -> Vec<IntentSnapshot> {
        let Some(lock_table) = self.lock_table.as_ref() else {
            return Vec::new();
        };
        lock_table
            .iter_keys()
            .map(|(key, txids)| IntentSnapshot {
                key: key.clone(),
                txids,
                lock_state: lock_table.lock_state_string(key),
            })
            .collect()
    }
}

/// Outcome of [`VllShardState::enqueue_lock_request`].
///
/// `queue_depth_warning` is `Some(depth)` when the depth exceeded
/// [`QUEUE_DEPTH_WARN_THRESHOLD`] at enqueue time. The caller may log this
/// using its own shard identity.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnqueueOutcome {
    pub queue_depth_warning: Option<usize>,
    pub enqueue_failed: bool,
}

/// An operation that has been removed from the queue and is awaiting the
/// host's executor.
pub struct DequeuedOp<O> {
    pub txid: u64,
    pub keys: Vec<Bytes>,
    pub operation: O,
}

/// Snapshot of a pending op for diagnostics output.
pub struct PendingOpSnapshot<'a, O> {
    pub txid: u64,
    pub state: PendingOpState,
    pub key_count: usize,
    pub age_ms: u64,
    pub operation: &'a O,
}

/// Snapshot of the continuation lock for diagnostics output.
#[derive(Debug, Clone)]
pub struct ContinuationLockSnapshot {
    pub txid: u64,
    pub conn_id: u64,
    pub age_ms: u64,
}

/// Snapshot of a single key's intent state for diagnostics output.
#[derive(Debug, Clone)]
pub struct IntentSnapshot {
    pub key: Bytes,
    pub txids: Vec<u64>,
    pub lock_state: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn channels() -> (
        oneshot::Sender<ShardReadyResult>,
        oneshot::Receiver<ShardReadyResult>,
    ) {
        oneshot::channel()
    }

    /// A revocation sender whose receiver is already gone — for the tests that
    /// do not exercise revocation. Sending on it fails harmlessly, which is
    /// the same shape as a coordinator that has already given up.
    fn dead_revoke() -> oneshot::Sender<VllError> {
        oneshot::channel().0
    }

    /// The SCA path's equivalent: a wound sender nobody is listening on, for
    /// the tests that are not about wound-wait.
    fn dead_wound() -> oneshot::Sender<VllError> {
        oneshot::channel().0
    }

    /// Pins the concrete derivation, not just the ratio: every other test
    /// touching `CONTINUATION_DRAIN_TIMEOUT` measures elapsed time relative
    /// to the constant itself (e.g. `CONTINUATION_DRAIN_TIMEOUT / 2`), so
    /// they'd pass unchanged even if the derivation below flipped from
    /// halving the coordinator's timeout to doubling it — silently breaking
    /// the "always strictly below" invariant the doc comment promises. This
    /// test would catch that: it fails the moment the shard's parked-request
    /// deadline stops being strictly under the coordinator's own timeout.
    // FM-VLL-003
    #[test]
    fn continuation_drain_timeout_stays_below_the_coordinators_acquisition_timeout() {
        assert!(CONTINUATION_DRAIN_TIMEOUT < crate::coordinator::DEFAULT_LOCK_ACQUISITION_TIMEOUT);
        assert_eq!(
            CONTINUATION_DRAIN_TIMEOUT,
            crate::coordinator::DEFAULT_LOCK_ACQUISITION_TIMEOUT / 2
        );
    }

    #[tokio::test]
    async fn enqueue_acquires_when_no_contention() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        let outcome = state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn second_writer_blocks_until_release() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt1, rr1) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt1,
            dead_wound(),
        );
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, mut rr2) = channels();
        state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        // Second writer must wait — the channel should not yet have a value.
        assert!(rr2.try_recv().is_err());

        // Execute and release op #1; #2 should advance to Ready.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn abort_releases_intents_and_advances_waiters() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt1, rr1) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt1,
            dead_wound(),
        );
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, mut rr2) = channels();
        state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        assert!(rr2.try_recv().is_err());

        state.abort(1);
        // Aborting #1 should release locks and advance #2 to Ready.
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn abort_of_pending_op_removes_it_from_sca_ordering() {
        let mut state: VllShardState<()> = VllShardState::default();

        // #1 holds the lock; #2 and #3 queue behind it.
        let (rt1, rr1) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt1,
            dead_wound(),
        );
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, mut rr2) = channels();
        state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        let (rt3, mut rr3) = channels();
        state.enqueue_lock_request(
            3,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt3,
            dead_wound(),
        );
        assert!(rr2.try_recv().is_err());
        assert!(rr3.try_recv().is_err());

        // Abort #2 while it is still Pending (holds no locks, only intents).
        state.abort(2);
        // #3 is still blocked by #1's granted lock.
        assert!(rr3.try_recv().is_err());

        // Releasing #1 must advance #3 — the aborted #2 no longer blocks
        // SCA ordering.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(rr3.await, Ok(ShardReadyResult::Ready)));
    }

    // FM-VLL-001
    #[tokio::test]
    async fn sca_lock_request_rejected_while_continuation_held() {
        let mut state: VllShardState<()> = VllShardState::default();

        // Acquire a continuation lock first.
        let (cont_rt, cont_rr) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));

        // SCA request from a *different* connection arrives. It must be
        // rejected with ShardBusy, not silently enqueued (which would let
        // it interleave with the continuation owner's commands).
        let (rt, rr) = channels();
        let outcome = state.enqueue_lock_request(
            51,
            vec![Bytes::from_static(b"k")],
            LockMode::Read,
            (),
            rt,
            dead_wound(),
        );
        assert!(outcome.enqueue_failed);
        assert!(matches!(
            rr.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
        assert_eq!(state.queue_depth(), 0);
    }

    // FM-VLL-002
    #[tokio::test]
    async fn continuation_lock_blocks_second_acquire() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt1, rr1) = oneshot::channel();
        let (_release_tx1, release_rx1) = oneshot::channel();
        state.request_continuation_lock(100, 7, rt1, release_rx1, dead_revoke());
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, rr2) = oneshot::channel();
        let (_release_tx2, release_rx2) = oneshot::channel();
        state.request_continuation_lock(101, 8, rt2, release_rx2, dead_revoke());
        assert!(matches!(
            rr2.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
    }

    /// One continuation claim per shard, parked included: a second request
    /// arriving while one is parked must be refused outright. Queueing it
    /// would leave two requests racing for the same drain, and the loser's
    /// `release_rx` would sit in a state nobody owns.
    // FM-VLL-002
    #[tokio::test(start_paused = true)]
    async fn second_continuation_request_refused_while_one_is_parked() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt1, mut cont_rr1) = channels();
        let (_release_tx1, release_rx1) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt1, release_rx1, dead_revoke());
        assert!(cont_rr1.try_recv().is_err(), "first request is parked");

        let (cont_rt2, cont_rr2) = channels();
        let (_release_tx2, release_rx2) = oneshot::channel();
        state.request_continuation_lock(51, 8, cont_rt2, release_rx2, dead_revoke());
        assert!(matches!(
            cont_rr2.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));

        // The first request still owns the claim and is granted on drain.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(cont_rr1.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(7));
    }

    /// A continuation lock takes the shard exclusively, so it may only be
    /// granted once the shard has drained. An op still sitting in the queue
    /// would execute — and release its own locks — while the continuation
    /// owner believed it had the shard alone. The request is therefore parked,
    /// *without* waiting (the shard's own loop is what drains the queue), and
    /// granted from the drain point.
    ///
    /// Time is paused: no virtual time may pass while the request is parked,
    /// and none is needed to grant it.
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn continuation_lock_parks_then_grants_when_the_queue_drains() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.queue_depth(), 1, "op stays queued until it executes");

        let (cont_rt, mut cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        let before = Instant::now();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());

        assert_eq!(Instant::now(), before, "parking must not wait");
        assert!(
            cont_rr.try_recv().is_err(),
            "no answer while the queue holds an op"
        );
        assert_eq!(
            state.continuation_lock_owner(),
            None,
            "the lock is not taken over a queued op"
        );

        // The host drains the queue — which it could not do if the request
        // had waited inline.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);

        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(7));
        assert_eq!(Instant::now(), before, "the grant costs no time either");
    }

    /// A shard that never drains fails its parked request after
    /// `CONTINUATION_DRAIN_TIMEOUT` — and leaves nothing behind: no lock, no
    /// stored release receiver, and no drain barrier (SCA work is accepted
    /// again).
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn continuation_lock_times_out_when_the_queue_never_drains() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt, cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());

        // The host's event-loop arm: nothing drains, so the deadline fires.
        let before = Instant::now();
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::DrainTimedOut
        );
        assert_eq!(
            Instant::now() - before,
            CONTINUATION_DRAIN_TIMEOUT,
            "the request waits the full drain timeout before failing"
        );

        assert!(matches!(
            cont_rr.await,
            Ok(ShardReadyResult::Failed(VllError::LockTimeout))
        ));
        assert_eq!(
            state.continuation_lock_owner(),
            None,
            "a drain timeout must not leave a lock behind"
        );
        assert!(state.continuation_lock_snapshot().is_none());

        // The barrier is lifted: this shard takes SCA work again.
        let (rt2, rr2) = channels();
        let outcome = state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"j")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    /// The drain gate is not "the queue map is empty" — an op that has been
    /// dequeued for execution has left the queue but still holds its locks, so
    /// the shard is not yet exclusive-able. Granting there would hand the
    /// continuation owner a shard whose in-flight op is about to release locks
    /// under it.
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn continuation_lock_parks_while_a_dequeued_op_is_still_executing() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        // Op is out of the queue and executing on the host.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        assert_eq!(state.queue_depth(), 0, "an executing op has left the queue");

        let (cont_rt, mut cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());
        assert!(
            cont_rr.try_recv().is_err(),
            "an executing op must keep the request parked"
        );
        assert_eq!(state.continuation_lock_owner(), None);

        // Execution finishes: now the shard is drained.
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(7));
    }

    /// Aborting the last queued op is the other way a shard drains — a
    /// coordinator that gives up on its scatter unblocks the parked request.
    #[tokio::test(start_paused = true)]
    async fn parked_continuation_granted_when_the_last_queued_op_aborts() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt, mut cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());
        assert!(cont_rr.try_recv().is_err());

        state.abort(1);
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(7));
    }

    /// If the requester gave up before the shard drained, the grant must take
    /// no lock: nobody holds the paired release sender any more, so the shard
    /// would refuse SCA work with `ShardBusy` until the host noticed.
    #[tokio::test(start_paused = true)]
    async fn continuation_grant_skipped_when_the_requester_gave_up() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt, cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());

        // The coordinator times out and drops its half of both channels.
        drop(cont_rr);

        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);

        assert_eq!(
            state.continuation_lock_owner(),
            None,
            "no lock is installed for a requester that has gone away"
        );
        assert!(state.continuation_lock_snapshot().is_none());

        // And the shard is immediately usable again.
        let (rt2, rr2) = channels();
        let outcome = state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"j")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    /// The drain barrier: while a request is parked the queue may only shrink.
    /// Admitting new SCA work would keep the shard undrained and starve the
    /// parked request into a guaranteed `LockTimeout`.
    // FM-VLL-004
    #[tokio::test(start_paused = true)]
    async fn sca_lock_request_refused_while_a_continuation_request_is_parked() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt, mut cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());
        assert!(cont_rr.try_recv().is_err(), "request is parked");

        // A second connection's SCA request arrives while the drain is pending.
        let (rt2, rr2) = channels();
        let outcome = state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"j")],
            LockMode::Read,
            (),
            rt2,
            dead_wound(),
        );
        assert!(outcome.enqueue_failed);
        assert_eq!(outcome.queue_depth_warning, None);
        assert!(matches!(
            rr2.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
        assert_eq!(
            state.queue_depth(),
            1,
            "the refused request declares no intent and joins no queue"
        );

        // With no new arrivals the queue drains and the parked request wins.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));
    }

    /// The drain gate keys off the shard being drained, not off the queue
    /// having never been used: once the queued op has executed and released,
    /// the same request is granted synchronously, with no parking.
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn continuation_lock_acquires_once_queue_drains() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert_eq!(state.queue_depth(), 0);

        let (cont_rt, mut cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());

        assert!(
            matches!(cont_rr.try_recv(), Ok(ShardReadyResult::Ready)),
            "a drained shard answers without parking"
        );
        assert_eq!(state.continuation_lock_owner(), Some(7));
    }

    /// Owner/snapshot accessors must track the held lock, and the release
    /// event must actually drop it — the host event loop drives
    /// `next_continuation_event` after the coordinator's guard is dropped, and
    /// a shard that kept a stale lock would refuse every later SCA request
    /// with `ShardBusy`.
    #[tokio::test]
    async fn continuation_lock_owner_and_snapshot_track_the_held_lock() {
        let mut state: VllShardState<()> = VllShardState::default();
        assert_eq!(state.continuation_lock_owner(), None);
        assert!(state.continuation_lock_snapshot().is_none());

        let (rt, rr) = channels();
        let (release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt, release_rx, dead_revoke());
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        assert_eq!(state.continuation_lock_owner(), Some(7));
        let snap = state
            .continuation_lock_snapshot()
            .expect("snapshot while the lock is held");
        assert_eq!(snap.txid, 50);
        assert_eq!(snap.conn_id, 7);

        // What the host event loop does once the coordinator's guard drops.
        release_tx
            .send(())
            .expect("release receiver is held by the state");
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::Released
        );

        assert_eq!(state.continuation_lock_owner(), None);
        assert!(state.continuation_lock_snapshot().is_none());

        // The shard accepts SCA work again.
        let (rt2, rr2) = channels();
        let outcome = state.enqueue_lock_request(
            60,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    /// With nothing held and nothing parked the event future must never
    /// resolve: the host drives it from a `select!` arm that is recreated
    /// every iteration, so a future that resolved immediately would spin the
    /// shard event loop.
    #[tokio::test(start_paused = true)]
    async fn continuation_event_pends_forever_without_a_lock() {
        let mut state: VllShardState<()> = VllShardState::default();
        assert!(
            tokio::time::timeout(Duration::from_millis(500), state.next_continuation_event())
                .await
                .is_err(),
            "event future must stay pending while nothing is held or parked"
        );
    }

    /// The event future is cancel-safe: a `select!` iteration that loses to
    /// another arm must not consume the stored receiver, and the signal must
    /// still be observed on a later poll.
    #[tokio::test(start_paused = true)]
    async fn continuation_release_survives_cancellation_then_fires() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        let (release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt, release_rx, dead_revoke());
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        // Lock still held: the future does not resolve, and this cancelled
        // poll must not eat the receiver.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), state.next_continuation_event())
                .await
                .is_err()
        );

        release_tx.send(()).expect("release receiver preserved");
        let event =
            tokio::time::timeout(Duration::from_millis(500), state.next_continuation_event())
                .await
                .expect("release must be observed after cancellation");
        assert_eq!(event, ContinuationEvent::Released);
        assert_eq!(state.continuation_lock_owner(), None);
    }

    /// A parked request's deadline is cancel-safe too: losing a `select!`
    /// iteration must not restart the drain timer, and the parked request must
    /// not be dropped on the floor.
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn parked_continuation_deadline_survives_cancellation() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (cont_rt, cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());

        let before = Instant::now();
        // Half the drain budget elapses in a losing `select!` arm.
        assert!(
            tokio::time::timeout(
                CONTINUATION_DRAIN_TIMEOUT / 2,
                state.next_continuation_event()
            )
            .await
            .is_err()
        );
        // The deadline is absolute, so the remaining budget is what is left,
        // not a fresh full timeout.
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::DrainTimedOut
        );
        assert_eq!(Instant::now() - before, CONTINUATION_DRAIN_TIMEOUT);
        assert!(matches!(
            cont_rr.await,
            Ok(ShardReadyResult::Failed(VllError::LockTimeout))
        ));
    }

    /// `queue_depth_warning` is a boundary: it fires at exactly
    /// [`QUEUE_DEPTH_WARN_THRESHOLD`] queued ops, not one short of it.
    #[tokio::test]
    async fn queue_depth_warning_fires_exactly_at_threshold() {
        let mut state: VllShardState<()> = VllShardState::default();

        // Fill to one below the threshold. Distinct keys, so every op
        // acquires immediately and stays queued in the Ready state.
        for txid in 0..(QUEUE_DEPTH_WARN_THRESHOLD as u64 - 1) {
            let (rt, _rr) = channels();
            let outcome = state.enqueue_lock_request(
                txid,
                vec![Bytes::from(format!("k{txid}"))],
                LockMode::Write,
                (),
                rt,
                dead_wound(),
            );
            assert!(!outcome.enqueue_failed);
        }
        assert_eq!(state.queue_depth(), QUEUE_DEPTH_WARN_THRESHOLD - 1);

        // Observed depth is one below the threshold: no warning.
        let (rt, _rr) = channels();
        let outcome = state.enqueue_lock_request(
            u64::MAX - 1,
            vec![Bytes::from_static(b"below")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert_eq!(outcome.queue_depth_warning, None);

        // Observed depth is exactly the threshold: warning, carrying the depth.
        let (rt, _rr) = channels();
        let outcome = state.enqueue_lock_request(
            u64::MAX,
            vec![Bytes::from_static(b"at")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert_eq!(
            outcome.queue_depth_warning,
            Some(QUEUE_DEPTH_WARN_THRESHOLD)
        );
    }

    /// The threshold governs the error paths too. A shallow queue rejects with
    /// `QueueFull` at a depth far below [`QUEUE_DEPTH_WARN_THRESHOLD`], and the
    /// outcome must not claim a depth warning the depth does not justify —
    /// otherwise every rejection logs "queue depth high" at a depth of 1.
    #[tokio::test]
    async fn queue_full_rejection_below_threshold_carries_no_depth_warning() {
        let mut state: VllShardState<()> = VllShardState::with_max_queue_depth(1);

        let (rt, rr) = channels();
        let outcome = state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"a")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (rt2, rr2) = channels();
        let outcome = state.enqueue_lock_request(
            2,
            vec![Bytes::from_static(b"b")],
            LockMode::Write,
            (),
            rt2,
            dead_wound(),
        );
        assert!(outcome.enqueue_failed);
        assert!(matches!(
            rr2.await,
            Ok(ShardReadyResult::Failed(VllError::QueueFull))
        ));
        assert_eq!(
            outcome.queue_depth_warning, None,
            "a depth of 1 is not a high-depth warning"
        );
    }

    /// Same rule on the `ShardBusy` path: a refusal while the continuation
    /// lock is held says nothing about queue depth.
    #[tokio::test]
    async fn shard_busy_rejection_carries_no_depth_warning() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (cont_rt, cont_rr) = channels();
        let (_release_tx, release_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, cont_rt, release_rx, dead_revoke());
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));

        let (rt, rr) = channels();
        let outcome = state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"a")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(outcome.enqueue_failed);
        assert!(matches!(
            rr.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
        assert_eq!(outcome.queue_depth_warning, None);
    }

    /// Wound-wait's core: an older (lower-txid) request colliding with a
    /// younger *holder* is not refused. The holder is told to let go, the older
    /// request parks behind it, and the holder's release hands the lock over.
    ///
    /// Refusing here instead (the pre-wound-wait rule) has no priority order,
    /// so two cross-shard scripts over overlapping shard sets can each win a
    /// different shard and refuse each other forever.
    ///
    /// Time is paused: none of this needs a clock, and the assertions below
    /// would be met by a drain/hold *timeout* if one were allowed to fire.
    // FM-VLL-006
    #[tokio::test(start_paused = true)]
    async fn older_continuation_request_wounds_the_younger_holder() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt_young, rr_young) = channels();
        let (release_tx_young, release_rx_young) = oneshot::channel();
        let (revoke_tx_young, revoke_rx_young) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt_young, release_rx_young, revoke_tx_young);
        assert!(matches!(rr_young.await, Ok(ShardReadyResult::Ready)));

        let (rt_old, mut rr_old) = channels();
        let (_release_tx_old, release_rx_old) = oneshot::channel();
        state.request_continuation_lock(20, 8, rt_old, release_rx_old, dead_revoke());

        assert_eq!(
            revoke_rx_young.await,
            Ok(VllError::Wounded),
            "the younger holder is told to abandon its work"
        );
        assert!(
            rr_old.try_recv().is_err(),
            "the older request parks rather than being refused"
        );
        assert_eq!(
            state.continuation_lock_owner(),
            Some(7),
            "the wound does not clear the lock — only the holder knows when its work stopped"
        );

        // The wounded holder lets go; its release is the drain point that hands
        // the lock to the request that wounded it.
        let _ = release_tx_young.send(());
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::Released
        );
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(8));
    }

    /// The parking slot holds one claim, so an older request colliding with a
    /// *parked* younger one takes the slot: the younger requester is failed
    /// with a retryable `Wounded`, never left holding a channel nobody answers.
    // FM-VLL-006
    #[tokio::test(start_paused = true)]
    async fn older_continuation_request_wounds_a_parked_younger_claim() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt, rr) = channels();
        state.enqueue_lock_request(
            1,
            vec![Bytes::from_static(b"k")],
            LockMode::Write,
            (),
            rt,
            dead_wound(),
        );
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let (rt_young, rr_young) = channels();
        let (_release_tx_young, release_rx_young) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt_young, release_rx_young, dead_revoke());

        let (rt_old, mut rr_old) = channels();
        let (_release_tx_old, release_rx_old) = oneshot::channel();
        state.request_continuation_lock(20, 8, rt_old, release_rx_old, dead_revoke());

        assert!(matches!(
            rr_young.await,
            Ok(ShardReadyResult::Failed(VllError::Wounded))
        ));
        assert!(
            rr_old.try_recv().is_err(),
            "the older claim now owns the slot"
        );

        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(state.continuation_lock_owner(), Some(8));
    }

    /// A younger request colliding with an older claim is still refused — that
    /// refusal respects the txid order and so cannot take part in a cycle.
    // FM-VLL-006
    #[tokio::test(start_paused = true)]
    async fn younger_continuation_request_is_refused_without_wounding() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt_old, rr_old) = channels();
        let (_release_tx_old, release_rx_old) = oneshot::channel();
        let (revoke_tx_old, mut revoke_rx_old) = oneshot::channel();
        state.request_continuation_lock(20, 7, rt_old, release_rx_old, revoke_tx_old);
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));

        let (rt_young, rr_young) = channels();
        let (_release_tx_young, release_rx_young) = oneshot::channel();
        state.request_continuation_lock(50, 8, rt_young, release_rx_young, dead_revoke());

        assert!(matches!(
            rr_young.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
        assert!(
            revoke_rx_old.try_recv().is_err(),
            "the older holder is never wounded by a younger arrival"
        );
        assert_eq!(state.continuation_lock_owner(), Some(7));
    }

    /// `SCRIPT KILL` / `FUNCTION KILL` has to reach the continuation lock: a
    /// killed cross-shard script that keeps its lock leaves the shard refusing
    /// every other connection's work until a future nobody awaits happens to
    /// finish.
    // FM-VLL-007
    #[tokio::test(start_paused = true)]
    async fn revoking_a_held_continuation_notifies_its_holder() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        let (release_tx, release_rx) = oneshot::channel();
        let (revoke_tx, revoke_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt, release_rx, revoke_tx);
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        assert!(state.revoke_held_continuation(), "a holder was notified");
        assert_eq!(revoke_rx.await, Ok(VllError::Revoked));
        assert_eq!(
            state.continuation_lock_owner(),
            Some(7),
            "revocation asks; the holder's release is what clears the lock"
        );
        assert!(
            !state.revoke_held_continuation(),
            "the notice fires at most once per grant"
        );

        let _ = release_tx.send(());
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::Released
        );
        assert_eq!(state.continuation_lock_owner(), None);
    }

    /// Revoking with no lock held is a no-op, not a panic: `SCRIPT KILL` runs
    /// against every shard, most of which hold nothing.
    // FM-VLL-007
    #[tokio::test]
    async fn revoking_an_unheld_continuation_reports_no_holder() {
        let mut state: VllShardState<()> = VllShardState::default();
        assert!(!state.revoke_held_continuation());
    }

    /// The hold cap is the backstop for a holder nobody kills: past
    /// `CONTINUATION_MAX_HOLD` the shard revokes on its own, so an exclusive
    /// hold cannot be unbounded. It disarms afterwards — a revoked-but-not-yet-
    /// released lock must not re-fire the cap on every loop iteration.
    // FM-VLL-007
    #[tokio::test(start_paused = true)]
    async fn continuation_hold_cap_revokes_the_holder_once() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        let (release_tx, release_rx) = oneshot::channel();
        let (revoke_tx, revoke_rx) = oneshot::channel();
        state.request_continuation_lock(50, 7, rt, release_rx, revoke_tx);
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));

        let start = Instant::now();
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::HoldCapExpired
        );
        assert_eq!(start.elapsed(), CONTINUATION_MAX_HOLD);
        assert_eq!(revoke_rx.await, Ok(VllError::Revoked));

        // Disarmed: the next event is the holder's release, not a second cap
        // expiry, and it costs no further virtual time.
        let _ = release_tx.send(());
        assert_eq!(
            state.next_continuation_event().await,
            ContinuationEvent::Released
        );
        assert_eq!(start.elapsed(), CONTINUATION_MAX_HOLD);
        assert_eq!(state.continuation_lock_owner(), None);
    }

    /// Wound-wait on the SCA path. An older request that finds a younger
    /// holder in its way notifies that holder instead of parking behind it —
    /// parking is what closes the wait-for cycle when two multi-shard
    /// transactions win shards in opposite orders.
    ///
    /// The wound is advisory: the shard releases nothing on its own, because
    /// the victim may already have a phase-3 execute in flight. The victim's
    /// coordinator unwinds through the ordinary abort path, and only that
    /// abort hands the lock over.
    // FM-VLL-010
    #[tokio::test]
    async fn an_older_sca_request_wounds_the_younger_holder_in_its_way() {
        let mut state: VllShardState<()> = VllShardState::default();
        let key = Bytes::from_static(b"k");

        let (rt_young, rr_young) = channels();
        let (wound_tx_young, wound_rx_young) = oneshot::channel();
        state.enqueue_lock_request(
            50,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_young,
            wound_tx_young,
        );
        assert!(matches!(rr_young.await, Ok(ShardReadyResult::Ready)));

        let (rt_old, mut rr_old) = channels();
        state.enqueue_lock_request(
            20,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_old,
            dead_wound(),
        );

        assert_eq!(
            wound_rx_young.await,
            Ok(VllError::Wounded),
            "the younger holder is told to give way"
        );
        assert!(
            rr_old.try_recv().is_err(),
            "wounding is advisory: the shard does not hand the lock over itself"
        );

        // What the victim's coordinator does with the notice.
        state.abort(50);
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));
    }

    /// The mirror case: a younger arrival parks behind an older holder without
    /// wounding it. That edge points from younger to older, which is the txid
    /// order, so it cannot take part in a cycle.
    // FM-VLL-010
    #[tokio::test]
    async fn a_younger_sca_request_waits_without_wounding_the_older_holder() {
        let mut state: VllShardState<()> = VllShardState::default();
        let key = Bytes::from_static(b"k");

        let (rt_old, rr_old) = channels();
        let (wound_tx_old, mut wound_rx_old) = oneshot::channel();
        state.enqueue_lock_request(
            20,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_old,
            wound_tx_old,
        );
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));

        let (rt_young, mut rr_young) = channels();
        state.enqueue_lock_request(
            50,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_young,
            dead_wound(),
        );

        assert!(rr_young.try_recv().is_err(), "the younger request parks");
        assert!(
            wound_rx_old.try_recv().is_err(),
            "seniority is never wounded"
        );
    }

    /// A victim past the point of no return is not wounded. Once the op has
    /// been dequeued for execution its writes may already be landing, and a
    /// wound the coordinator acted on would ask it to abort work it can no
    /// longer take back. The older request waits for the release instead.
    // FM-VLL-010
    #[tokio::test]
    async fn an_executing_op_is_not_wounded() {
        let mut state: VllShardState<()> = VllShardState::default();
        let key = Bytes::from_static(b"k");

        let (rt_young, rr_young) = channels();
        let (wound_tx_young, mut wound_rx_young) = oneshot::channel();
        state.enqueue_lock_request(
            50,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_young,
            wound_tx_young,
        );
        assert!(matches!(rr_young.await, Ok(ShardReadyResult::Ready)));
        let dequeued = state.dequeue_for_execution(50).expect("op 50 ready");

        let (rt_old, mut rr_old) = channels();
        state.enqueue_lock_request(
            20,
            vec![key.clone()],
            LockMode::Write,
            (),
            rt_old,
            dead_wound(),
        );
        assert!(
            wound_rx_young.try_recv().is_err(),
            "an op already executing is beyond wounding"
        );
        assert!(rr_old.try_recv().is_err());

        state.release_after_execution(dequeued.txid, &dequeued.keys);
        assert!(matches!(rr_old.await, Ok(ShardReadyResult::Ready)));
    }

    #[test]
    fn diagnostic_snapshots_reflect_state() {
        let mut state: VllShardState<()> = VllShardState::default();
        assert_eq!(state.queue_depth(), 0);
        assert!(state.continuation_lock_snapshot().is_none());
        assert!(state.intent_snapshots().is_empty());

        let (rt, _rr) = channels();
        state.enqueue_lock_request(
            5,
            vec![Bytes::from_static(b"k")],
            LockMode::Read,
            (),
            rt,
            dead_wound(),
        );
        assert_eq!(state.queue_depth(), 1);
        let snaps: Vec<_> = state.iter_pending_ops().collect();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].txid, 5);
        // Intent has been declared and lock acquired (no contention).
        assert!(!state.intent_snapshots().is_empty());
    }
}
