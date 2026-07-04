//! Per-shard VLL state machine.
//!
//! [`VllShardState`] owns the intent table, transaction queue, and
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

use super::intent_table::IntentTable;
use super::queue::{ContinuationLock, TransactionQueue, VllPendingOp};
use super::types::{LockMode, PendingOpState, ShardReadyResult, VllError};

/// Default queue capacity used when no explicit limit is provided.
pub const DEFAULT_MAX_QUEUE_DEPTH: usize = 10000;

/// Threshold at which [`EnqueueOutcome::queue_depth_warning`] is set.
pub const QUEUE_DEPTH_WARN_THRESHOLD: usize = 8000;

/// Maximum time the state machine waits for the queue to drain before
/// reporting `LockTimeout` for a continuation lock request.
pub const CONTINUATION_DRAIN_TIMEOUT: Duration = Duration::from_millis(2000);

/// Polling interval used while waiting for the queue to drain during
/// continuation-lock acquisition.
const CONTINUATION_DRAIN_POLL: Duration = Duration::from_millis(10);

/// Per-shard VLL state machine.
///
/// Owns the intent table, transaction queue, and continuation lock for a
/// single shard. Generic over the operation payload type `O` (e.g.,
/// `ScatterOp` in `frogdb-core`).
#[derive(Debug)]
pub struct VllShardState<O: Debug> {
    intent_table: Option<IntentTable>,
    tx_queue: Option<TransactionQueue<O>>,
    continuation_lock: Option<ContinuationLock>,
    pending_continuation_release: Option<oneshot::Receiver<()>>,
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
            intent_table: None,
            tx_queue: None,
            continuation_lock: None,
            pending_continuation_release: None,
            max_queue_depth,
        }
    }

    fn ensure_initialized(&mut self) -> (&mut IntentTable, &mut TransactionQueue<O>) {
        if self.intent_table.is_none() {
            self.intent_table = Some(IntentTable::new());
        }
        if self.tx_queue.is_none() {
            self.tx_queue = Some(TransactionQueue::new(self.max_queue_depth));
        }
        (
            self.intent_table.as_mut().unwrap(),
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
    pub fn enqueue_lock_request(
        &mut self,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: O,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> EnqueueOutcome {
        if self.continuation_lock.is_some() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
            return EnqueueOutcome {
                queue_depth_warning: None,
                enqueue_failed: true,
            };
        }

        let (intent_table, tx_queue) = self.ensure_initialized();
        let queue_depth = tx_queue.len();
        let queue_depth_warning = queue_depth >= QUEUE_DEPTH_WARN_THRESHOLD;

        if !tx_queue.has_capacity() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::QueueFull));
            return EnqueueOutcome {
                queue_depth_warning: Some(queue_depth),
                enqueue_failed: true,
            };
        }

        intent_table.declare_intents(&keys, txid, mode);

        let pending_op = VllPendingOp::new(txid, keys.clone(), mode, operation, ready_tx);
        if let Err(_e) = tx_queue.enqueue(pending_op) {
            intent_table.remove_all_intents(&keys, txid);
            return EnqueueOutcome {
                queue_depth_warning: Some(queue_depth),
                enqueue_failed: true,
            };
        }

        Self::try_acquire_for(intent_table, tx_queue, txid);

        EnqueueOutcome {
            queue_depth_warning: queue_depth_warning.then_some(queue_depth),
            enqueue_failed: false,
        }
    }

    fn try_acquire_for(
        intent_table: &mut IntentTable,
        tx_queue: &mut TransactionQueue<O>,
        txid: u64,
    ) {
        let Some(op) = tx_queue.get_mut(txid) else {
            return;
        };
        if op.state != PendingOpState::Pending {
            return;
        }
        if !intent_table.can_proceed(&op.keys, txid, op.mode) {
            return;
        }
        if intent_table.try_acquire_locks(&op.keys, op.mode)
            && let Some(ready_tx) = op.mark_ready()
        {
            let _ = ready_tx.send(ShardReadyResult::Ready);
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
            if let (Some(intent_table), Some(tx_queue)) =
                (self.intent_table.as_mut(), self.tx_queue.as_mut())
            {
                Self::try_acquire_for(intent_table, tx_queue, txid);
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
        Some(DequeuedOp {
            txid: op.txid,
            keys: op.keys,
            mode: op.mode,
            operation: op.operation,
        })
    }

    /// Release locks and remove intents after a dequeued op finishes executing.
    ///
    /// Triggers a pass over remaining pending ops to advance newly-unblocked
    /// locks.
    pub fn release_after_execution(&mut self, txid: u64, keys: &[Bytes], mode: LockMode) {
        if let Some(intent_table) = self.intent_table.as_mut() {
            intent_table.release_locks(keys, mode);
            intent_table.remove_all_intents(keys, txid);
        }
        self.try_advance_pending_locks();
    }

    /// Abort a pending or ready operation, releasing any held locks and
    /// advancing waiters whose locks may now be acquirable.
    pub fn abort(&mut self, txid: u64) {
        let Some(tx_queue) = self.tx_queue.as_mut() else {
            return;
        };
        let Some(op) = tx_queue.dequeue(txid) else {
            return;
        };
        if let Some(intent_table) = self.intent_table.as_mut() {
            if op.state == PendingOpState::Ready {
                intent_table.release_locks(&op.keys, op.mode);
            }
            intent_table.remove_all_intents(&op.keys, txid);
        }
        // Advance waiters that may have been blocked behind the aborted op.
        self.try_advance_pending_locks();
    }

    /// Acquire a continuation (drain + shard-exclusive) lock.
    ///
    /// Drains the queue first (with a [`CONTINUATION_DRAIN_TIMEOUT`]) and
    /// then takes the lock. The caller is responsible for polling
    /// [`Self::take_pending_continuation_release`] in their event loop and
    /// calling [`Self::clear_continuation_lock`] when the release signal
    /// fires.
    pub async fn acquire_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) {
        if self.continuation_lock.is_some() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
            return;
        }

        let start = std::time::Instant::now();
        loop {
            let pending = self
                .tx_queue
                .as_ref()
                .map(|q| !q.is_empty())
                .unwrap_or(false);
            if !pending {
                break;
            }
            if start.elapsed() > CONTINUATION_DRAIN_TIMEOUT {
                let _ = ready_tx.send(ShardReadyResult::Failed(VllError::LockTimeout));
                return;
            }
            tokio::time::sleep(CONTINUATION_DRAIN_POLL).await;
        }

        self.continuation_lock = Some(ContinuationLock::new(txid, conn_id));
        self.pending_continuation_release = Some(release_rx);
        let _ = ready_tx.send(ShardReadyResult::Ready);
    }

    /// Wait for the continuation-release signal.
    ///
    /// If no continuation lock is held this future never completes, so it is
    /// safe to drive from a `select!` arm that recreates the future every
    /// iteration. Cancel-safe: if the surrounding `select!` fires another
    /// arm first, the release receiver is preserved for the next call.
    pub async fn await_continuation_release(&mut self) {
        match &mut self.pending_continuation_release {
            Some(rx) => {
                let _ = rx.await;
            }
            None => std::future::pending().await,
        }
    }

    /// Clear the continuation lock and any pending release receiver.
    /// Called by the host event loop after [`Self::await_continuation_release`]
    /// returns.
    pub fn clear_continuation_lock(&mut self) {
        self.continuation_lock = None;
        self.pending_continuation_release = None;
    }

    /// Returns true if a continuation lock is currently held.
    pub fn has_continuation_lock(&self) -> bool {
        self.continuation_lock.is_some()
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

    /// Snapshot the intent table for diagnostics.
    pub fn intent_snapshots(&self) -> Vec<IntentSnapshot> {
        let Some(intent_table) = self.intent_table.as_ref() else {
            return Vec::new();
        };
        intent_table
            .iter_keys()
            .map(|(key, txids)| IntentSnapshot {
                key: key.clone(),
                txids,
                lock_state: intent_table.get_lock_state_string(key),
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
    pub mode: LockMode,
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

    #[tokio::test]
    async fn enqueue_acquires_when_no_contention() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt, rr) = channels();
        let outcome =
            state.enqueue_lock_request(1, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt);
        assert!(!outcome.enqueue_failed);
        assert!(matches!(rr.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn second_writer_blocks_until_release() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt1, rr1) = channels();
        state.enqueue_lock_request(1, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt1);
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, mut rr2) = channels();
        state.enqueue_lock_request(2, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt2);
        // Second writer must wait — the channel should not yet have a value.
        assert!(rr2.try_recv().is_err());

        // Execute and release op #1; #2 should advance to Ready.
        let dequeued = state.dequeue_for_execution(1).expect("op 1 ready");
        state.release_after_execution(dequeued.txid, &dequeued.keys, dequeued.mode);
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn abort_releases_intents_and_advances_waiters() {
        let mut state: VllShardState<()> = VllShardState::default();

        let (rt1, rr1) = channels();
        state.enqueue_lock_request(1, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt1);
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, mut rr2) = channels();
        state.enqueue_lock_request(2, vec![Bytes::from_static(b"k")], LockMode::Write, (), rt2);
        assert!(rr2.try_recv().is_err());

        state.abort(1);
        // Aborting #1 should release locks and advance #2 to Ready.
        assert!(matches!(rr2.await, Ok(ShardReadyResult::Ready)));
    }

    #[tokio::test]
    async fn sca_lock_request_rejected_while_continuation_held() {
        let mut state: VllShardState<()> = VllShardState::default();

        // Acquire a continuation lock first.
        let (cont_rt, cont_rr) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel();
        state
            .acquire_continuation_lock(50, 7, cont_rt, release_rx)
            .await;
        assert!(matches!(cont_rr.await, Ok(ShardReadyResult::Ready)));

        // SCA request from a *different* connection arrives. It must be
        // rejected with ShardBusy, not silently enqueued (which would let
        // it interleave with the continuation owner's commands).
        let (rt, rr) = channels();
        let outcome =
            state.enqueue_lock_request(51, vec![Bytes::from_static(b"k")], LockMode::Read, (), rt);
        assert!(outcome.enqueue_failed);
        assert!(matches!(
            rr.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
        assert_eq!(state.queue_depth(), 0);
    }

    #[tokio::test]
    async fn continuation_lock_blocks_second_acquire() {
        let mut state: VllShardState<()> = VllShardState::default();
        let (rt1, rr1) = oneshot::channel();
        let (_release_tx1, release_rx1) = oneshot::channel();
        state
            .acquire_continuation_lock(100, 7, rt1, release_rx1)
            .await;
        assert!(matches!(rr1.await, Ok(ShardReadyResult::Ready)));

        let (rt2, rr2) = oneshot::channel();
        let (_release_tx2, release_rx2) = oneshot::channel();
        state
            .acquire_continuation_lock(101, 8, rt2, release_rx2)
            .await;
        assert!(matches!(
            rr2.await,
            Ok(ShardReadyResult::Failed(VllError::ShardBusy))
        ));
    }

    #[test]
    fn diagnostic_snapshots_reflect_state() {
        let mut state: VllShardState<()> = VllShardState::default();
        assert_eq!(state.queue_depth(), 0);
        assert!(state.continuation_lock_snapshot().is_none());
        assert!(state.intent_snapshots().is_empty());

        let (rt, _rr) = channels();
        state.enqueue_lock_request(5, vec![Bytes::from_static(b"k")], LockMode::Read, (), rt);
        assert_eq!(state.queue_depth(), 1);
        let snaps: Vec<_> = state.iter_pending_ops().collect();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].txid, 5);
        // Intent has been declared and lock acquired (no contention).
        assert!(!state.intent_snapshots().is_empty());
    }
}
