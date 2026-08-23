//! VLL transaction queue for ordering and tracking pending operations.

use std::collections::BTreeMap;
use std::fmt::Debug;

use bytes::Bytes;
use tokio::sync::oneshot;
// The timer's clock, not the OS clock: under a paused runtime (the turmoil
// simulation) the two disagree, and every age this file reports is measured
// against a deadline the timer owns. See `frogdb_types::clock`.
use tokio::time::Instant;

use super::{LockMode, PendingOpState, ShardReadyResult, VllError};

/// A pending VLL operation in the queue.
///
/// Generic over `O`, the operation payload (e.g., `ScatterOp` in frogdb-core).
/// The VLL module never inspects the operation — it just stores and passes it through.
#[derive(Debug)]
pub struct VllPendingOp<O: Debug = ()> {
    /// Transaction ID (global ordering).
    pub txid: u64,
    /// Keys involved in this operation.
    pub keys: Vec<Bytes>,
    /// How this op declared its keys. Kept past the grant because it is what
    /// tells a *write* path from a *read* path if the op panics mid-execution
    /// (see [`VllShardState::release_after_panic`](crate::VllShardState::release_after_panic)).
    pub mode: LockMode,
    /// The operation to execute.
    pub operation: O,
    /// Current state.
    pub state: PendingOpState,
    /// When this operation was enqueued.
    pub enqueued_at: Instant,
    /// Channel to notify coordinator when ready.
    pub ready_tx: Option<oneshot::Sender<ShardReadyResult>>,
    /// Back-channel to this op's coordinator, used once the op has been told
    /// `Ready` and `ready_tx` is spent: an older transaction that finds this
    /// op holding a lock it needs *wounds* it here. Taken when it fires, so a
    /// repeated wound (every `try_advance_pending_locks` pass retries the
    /// grant) notifies once.
    pub wound_tx: Option<oneshot::Sender<VllError>>,
}

impl<O: Debug> VllPendingOp<O> {
    /// Create a new pending operation.
    pub fn new(
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: O,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        wound_tx: oneshot::Sender<VllError>,
    ) -> Self {
        Self {
            txid,
            keys,
            mode,
            operation,
            state: PendingOpState::Pending,
            enqueued_at: Instant::now(),
            ready_tx: Some(ready_tx),
            wound_tx: Some(wound_tx),
        }
    }

    /// Mark as ready and take the ready channel.
    pub fn mark_ready(&mut self) -> Option<oneshot::Sender<ShardReadyResult>> {
        self.state = PendingOpState::Ready;
        self.ready_tx.take()
    }

    /// Tell this op's coordinator an older transaction wants what it holds.
    ///
    /// The notice is **advisory**: nothing is released here. The shard cannot
    /// free a granted op's locks on its own — the coordinator may already have
    /// dispatched `VllExecute` to sibling shards, and a shard-side release
    /// would let this shard refuse work the siblings are applying, i.e. a
    /// partial write with nothing to roll it back. So the victim's own
    /// coordinator unwinds through the ordinary abort path, which is what
    /// actually releases the locks. A notice that arrives too late (the
    /// coordinator has stopped listening) is simply dropped and the victim
    /// finishes normally. Returns whether anyone was listening.
    pub fn wound(&mut self) -> bool {
        match self.wound_tx.take() {
            Some(tx) => tx.send(VllError::Wounded).is_ok(),
            None => false,
        }
    }

    /// Get the age of this operation.
    pub fn age(&self) -> std::time::Duration {
        self.enqueued_at.elapsed()
    }
}

/// VLL transaction queue for a single shard.
///
/// Maintains operations ordered by txid using a BTreeMap.
/// Lower txid = higher priority.
///
/// Generic over `O`, the operation payload stored in each `VllPendingOp`.
#[derive(Debug)]
pub struct TransactionQueue<O: Debug = ()> {
    /// Pending operations indexed by transaction ID (BTreeMap for ordering).
    pending: BTreeMap<u64, VllPendingOp<O>>,
    /// Maximum queue depth.
    max_depth: usize,
}

impl<O: Debug> Default for TransactionQueue<O> {
    fn default() -> Self {
        Self::new(10000)
    }
}

impl<O: Debug> TransactionQueue<O> {
    /// Create a new transaction queue with the specified max depth.
    pub fn new(max_depth: usize) -> Self {
        Self {
            pending: BTreeMap::new(),
            max_depth,
        }
    }

    /// Check if the queue has capacity for a new transaction.
    pub fn has_capacity(&self) -> bool {
        self.pending.len() < self.max_depth
    }

    /// Enqueue a new operation.
    ///
    /// Returns an error if the queue is full.
    pub fn enqueue(&mut self, op: VllPendingOp<O>) -> Result<(), VllError> {
        if !self.has_capacity() {
            return Err(VllError::QueueFull);
        }
        self.pending.insert(op.txid, op);
        Ok(())
    }

    /// Get a mutable reference to an operation by txid.
    pub fn get_mut(&mut self, txid: u64) -> Option<&mut VllPendingOp<O>> {
        self.pending.get_mut(&txid)
    }

    /// Get a reference to an operation by txid.
    #[cfg(test)]
    pub fn get(&self, txid: u64) -> Option<&VllPendingOp<O>> {
        self.pending.get(&txid)
    }

    /// Remove a completed operation from the queue.
    pub fn dequeue(&mut self, txid: u64) -> Option<VllPendingOp<O>> {
        self.pending.remove(&txid)
    }

    /// Get the number of pending operations.
    pub fn len(&self) -> usize {
        self.pending.len()
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Get the lowest txid in the queue (highest priority).
    #[cfg(test)]
    pub fn lowest_txid(&self) -> Option<u64> {
        self.pending.keys().next().copied()
    }

    /// Check if a txid is at the front of the queue (has priority).
    #[cfg(test)]
    pub fn is_front(&self, txid: u64) -> bool {
        self.lowest_txid() == Some(txid)
    }

    /// Get all operations in txid order.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &VllPendingOp<O>)> {
        self.pending.iter()
    }
}

/// State for a continuation lock (used for MULTI/EXEC and Lua scripts).
#[derive(Debug)]
pub struct ContinuationLock {
    /// Transaction ID holding the lock.
    pub txid: u64,
    /// Connection ID that owns this lock.
    pub conn_id: u64,
    /// When the lock was acquired.
    pub acquired_at: Instant,
}

impl ContinuationLock {
    /// Create a new continuation lock.
    pub fn new(txid: u64, conn_id: u64) -> Self {
        Self {
            txid,
            conn_id,
            acquired_at: Instant::now(),
        }
    }

    /// Get the age of this lock.
    pub fn age(&self) -> std::time::Duration {
        self.acquired_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_op(txid: u64) -> VllPendingOp {
        let (ready_tx, _ready_rx) = oneshot::channel();
        let (wound_tx, _wound_rx) = oneshot::channel();
        VllPendingOp::new(
            txid,
            vec![Bytes::from_static(b"key1")],
            LockMode::Write,
            (),
            ready_tx,
            wound_tx,
        )
    }

    #[test]
    fn test_queue_ordering() {
        let mut queue = TransactionQueue::new(100);

        // Enqueue out of order
        queue.enqueue(make_test_op(5)).unwrap();
        queue.enqueue(make_test_op(2)).unwrap();
        queue.enqueue(make_test_op(8)).unwrap();
        queue.enqueue(make_test_op(1)).unwrap();

        // Should be ordered by txid
        let txids: Vec<u64> = queue.iter().map(|(&txid, _)| txid).collect();
        assert_eq!(txids, vec![1, 2, 5, 8]);

        // Lowest should be 1
        assert_eq!(queue.lowest_txid(), Some(1));
        assert!(queue.is_front(1));
        assert!(!queue.is_front(2));
    }

    #[test]
    fn test_queue_capacity() {
        let mut queue = TransactionQueue::new(3);

        assert!(queue.has_capacity());
        queue.enqueue(make_test_op(1)).unwrap();
        queue.enqueue(make_test_op(2)).unwrap();
        queue.enqueue(make_test_op(3)).unwrap();

        assert!(!queue.has_capacity());
        assert!(matches!(
            queue.enqueue(make_test_op(4)),
            Err(VllError::QueueFull)
        ));
    }

    #[test]
    fn test_dequeue() {
        let mut queue = TransactionQueue::new(100);

        queue.enqueue(make_test_op(1)).unwrap();
        queue.enqueue(make_test_op(2)).unwrap();

        assert_eq!(queue.len(), 2);

        let op = queue.dequeue(1).unwrap();
        assert_eq!(op.txid, 1);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.lowest_txid(), Some(2));
    }

    /// `is_empty` gates the continuation-lock drain check
    /// ([`crate::VllShardState::request_continuation_lock`]): it must track
    /// the queue's contents, not a fixed answer.
    #[test]
    fn empty_reflects_queued_ops() {
        let mut queue = TransactionQueue::new(100);
        assert!(queue.is_empty());

        queue.enqueue(make_test_op(1)).unwrap();
        assert!(!queue.is_empty());

        queue.dequeue(1).unwrap();
        assert!(queue.is_empty(), "queue is empty again once the op leaves");
    }

    /// Ages feed the `VLL` diagnostics output; a stuck-at-zero age would
    /// silently hide a wedged op.
    #[test]
    fn pending_op_age_advances() {
        let op = make_test_op(1);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(
            op.age() >= std::time::Duration::from_millis(1),
            "age must measure elapsed time since enqueue, got {:?}",
            op.age()
        );
    }

    /// The wound back-channel is the shard's only way to tell a granted op's
    /// coordinator to give way, and every `try_advance_pending_locks` pass
    /// re-runs the grant that fires it — so it has to be take-once.
    // FM-VLL-010
    #[test]
    fn wounding_an_op_notifies_its_coordinator_exactly_once() {
        let (ready_tx, _ready_rx) = oneshot::channel();
        let (wound_tx, mut wound_rx) = oneshot::channel();
        let mut op = VllPendingOp::new(
            7,
            vec![Bytes::from_static(b"key1")],
            LockMode::Write,
            (),
            ready_tx,
            wound_tx,
        );

        assert!(op.wound(), "the coordinator is listening");
        assert!(matches!(wound_rx.try_recv(), Ok(VllError::Wounded)));
        assert!(
            !op.wound(),
            "a second wound must not re-signal a coordinator already told to give way"
        );
    }

    #[test]
    fn test_get_mut() {
        let mut queue = TransactionQueue::new(100);
        queue.enqueue(make_test_op(1)).unwrap();

        {
            let op = queue.get_mut(1).unwrap();
            assert_eq!(op.state, PendingOpState::Pending);
            op.mark_ready();
        }

        let op = queue.get(1).unwrap();
        assert_eq!(op.state, PendingOpState::Ready);
    }

    #[test]
    fn test_continuation_lock() {
        let lock = ContinuationLock::new(42, 123);
        assert_eq!(lock.txid, 42);
        assert_eq!(lock.conn_id, 123);
        std::thread::sleep(std::time::Duration::from_millis(2));
        // Bounded on both sides: a lock age that never advances would hide a
        // long-held continuation lock in the diagnostics output.
        assert!(
            lock.age() >= std::time::Duration::from_millis(1),
            "age must measure elapsed time since acquisition, got {:?}",
            lock.age()
        );
        assert!(lock.age() < std::time::Duration::from_secs(10));
    }
}
