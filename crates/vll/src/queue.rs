//! VLL transaction queue for ordering and tracking pending operations.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Instant;

use bytes::Bytes;
use tokio::sync::oneshot;

use super::{ExecuteSignal, LockMode, PendingOpState, ShardReadyResult, VllError};

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
    /// Lock mode required.
    pub mode: LockMode,
    /// The operation to execute.
    pub operation: O,
    /// Current state.
    pub state: PendingOpState,
    /// When this operation was enqueued.
    pub enqueued_at: Instant,
    /// Channel to notify coordinator when ready.
    pub ready_tx: Option<oneshot::Sender<ShardReadyResult>>,
    /// Channel to receive execute signal from coordinator.
    pub execute_rx: Option<oneshot::Receiver<ExecuteSignal>>,
}

impl<O: Debug> VllPendingOp<O> {
    /// Create a new pending operation.
    pub fn new(
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: O,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    ) -> Self {
        Self {
            txid,
            keys,
            mode,
            operation,
            state: PendingOpState::Pending,
            enqueued_at: Instant::now(),
            ready_tx: Some(ready_tx),
            execute_rx: Some(execute_rx),
        }
    }

    /// Mark as ready and take the ready channel.
    pub fn mark_ready(&mut self) -> Option<oneshot::Sender<ShardReadyResult>> {
        self.state = PendingOpState::Ready;
        self.ready_tx.take()
    }

    /// Mark as executing and take the execute channel.
    pub fn mark_executing(&mut self) -> Option<oneshot::Receiver<ExecuteSignal>> {
        self.state = PendingOpState::Executing;
        self.execute_rx.take()
    }

    /// Mark as done.
    pub fn mark_done(&mut self) {
        self.state = PendingOpState::Done;
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
    pub fn lowest_txid(&self) -> Option<u64> {
        self.pending.keys().next().copied()
    }

    /// Get operations that have exceeded the timeout.
    ///
    /// Returns txids of operations older than the timeout.
    pub fn get_expired(&self, timeout: std::time::Duration) -> Vec<u64> {
        self.pending
            .iter()
            .filter(|(_, op)| op.age() > timeout)
            .map(|(&txid, _)| txid)
            .collect()
    }

    /// Remove expired operations and return them.
    pub fn cleanup_expired(&mut self, timeout: std::time::Duration) -> Vec<VllPendingOp<O>> {
        let expired_txids = self.get_expired(timeout);
        expired_txids
            .into_iter()
            .filter_map(|txid| self.pending.remove(&txid))
            .collect()
    }

    /// Check if a txid is at the front of the queue (has priority).
    pub fn is_front(&self, txid: u64) -> bool {
        self.lowest_txid() == Some(txid)
    }

    /// Get all operations in txid order.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &VllPendingOp<O>)> {
        self.pending.iter()
    }

    /// Get all txids in the queue.
    pub fn txids(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }

    /// Check if a specific txid is in the queue.
    pub fn contains(&self, txid: u64) -> bool {
        self.pending.contains_key(&txid)
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
    /// Channel to receive commands while locked.
    pub command_rx: Option<oneshot::Receiver<ExecuteSignal>>,
}

impl ContinuationLock {
    /// Create a new continuation lock.
    pub fn new(txid: u64, conn_id: u64) -> Self {
        Self {
            txid,
            conn_id,
            acquired_at: Instant::now(),
            command_rx: None,
        }
    }

    /// Get the age of this lock.
    pub fn age(&self) -> std::time::Duration {
        self.acquired_at.elapsed()
    }

    /// Check if this lock has exceeded the given timeout.
    pub fn is_expired(&self, timeout: std::time::Duration) -> bool {
        self.age() > timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_op(txid: u64) -> VllPendingOp {
        let (ready_tx, _ready_rx) = oneshot::channel();
        let (_execute_tx, execute_rx) = oneshot::channel();
        VllPendingOp::new(
            txid,
            vec![Bytes::from_static(b"key1")],
            LockMode::Write,
            (),
            ready_tx,
            execute_rx,
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
        assert!(!lock.is_expired(std::time::Duration::from_secs(10)));
    }
}
