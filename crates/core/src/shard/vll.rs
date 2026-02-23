use bytes::Bytes;
use tokio::sync::oneshot;

use crate::vll::{ExecuteSignal, IntentTable, LockMode, ShardReadyResult, VllError};
use crate::{TransactionQueue, VllPendingOp};

use super::message::ScatterOp;
use super::types::PartialResult;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle VLL lock request - declare intents and try to acquire locks.
    pub(crate) async fn handle_vll_lock_request(
        &mut self,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: ScatterOp,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        execute_rx: oneshot::Receiver<ExecuteSignal>,
    ) {
        // Ensure we have the VLL infrastructure
        if self.intent_table.is_none() {
            self.intent_table = Some(IntentTable::new());
        }
        if self.tx_queue.is_none() {
            self.tx_queue = Some(TransactionQueue::new(10000));
        }

        let intent_table = self.intent_table.as_mut().unwrap();
        let tx_queue = self.tx_queue.as_mut().unwrap();

        // Check queue capacity - warn when queue depth is high
        let queue_depth = tx_queue.len();
        // Warn when queue has 8000+ transactions (80% of default 10000)
        if queue_depth >= 8000 {
            tracing::warn!(
                shard_id = self.shard_id,
                queue_depth,
                "Shard message queue depth high"
            );
        }

        if !tx_queue.has_capacity() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::QueueFull));
            return;
        }

        // Declare intents
        intent_table.declare_intents(&keys, txid, mode);

        // Create pending operation
        let pending_op =
            VllPendingOp::new(txid, keys.clone(), mode, operation, ready_tx, execute_rx);
        if let Err(e) = tx_queue.enqueue(pending_op) {
            intent_table.remove_all_intents(&keys, txid);
            tracing::warn!(shard_id = self.shard_id, txid, error = %e, "Failed to enqueue VLL operation");
            return;
        }

        // Try to acquire locks using SCA
        self.try_acquire_vll_locks(txid).await;
    }

    /// Try to acquire locks for a VLL operation using SCA.
    async fn try_acquire_vll_locks(&mut self, txid: u64) {
        let intent_table = match self.intent_table.as_mut() {
            Some(t) => t,
            None => return,
        };
        let tx_queue = match self.tx_queue.as_mut() {
            Some(q) => q,
            None => return,
        };

        let op = match tx_queue.get_mut(txid) {
            Some(op) => op,
            None => return,
        };

        // Check if we can proceed using SCA
        if !intent_table.can_proceed(&op.keys, txid, op.mode) {
            // Cannot proceed yet - will be retried when earlier operations complete
            return;
        }

        // Try to acquire locks
        if intent_table.try_acquire_locks(&op.keys, op.mode) {
            // Success! Notify coordinator
            if let Some(ready_tx) = op.mark_ready() {
                let _ = ready_tx.send(ShardReadyResult::Ready);
            }
        }
        // If lock acquisition fails, we'll retry later
    }

    /// Handle VLL execute - execute a ready operation.
    pub(crate) async fn handle_vll_execute(
        &mut self,
        txid: u64,
        response_tx: oneshot::Sender<PartialResult>,
    ) {
        let tx_queue = match self.tx_queue.as_mut() {
            Some(q) => q,
            None => {
                // No queue means no operation to execute
                let _ = response_tx.send(PartialResult { results: vec![] });
                return;
            }
        };

        // Get and remove the operation
        let op = match tx_queue.dequeue(txid) {
            Some(op) => op,
            None => {
                let _ = response_tx.send(PartialResult { results: vec![] });
                return;
            }
        };

        // Execute the operation
        let result = self.execute_scatter_part(&op.keys, &op.operation).await;

        // Release locks
        if let Some(intent_table) = self.intent_table.as_mut() {
            intent_table.release_locks(&op.keys, op.mode);
            intent_table.remove_all_intents(&op.keys, txid);
        }

        // Send result
        let _ = response_tx.send(result);

        // Try to unblock waiting operations
        self.process_waiting_vll_ops().await;
    }

    /// Handle VLL abort - cleanup a failed operation.
    pub(crate) fn handle_vll_abort(&mut self, txid: u64) {
        // Remove from queue
        if let Some(tx_queue) = self.tx_queue.as_mut()
            && let Some(op) = tx_queue.dequeue(txid)
        {
            // Release any held locks and remove intents
            if let Some(intent_table) = self.intent_table.as_mut() {
                if op.state == crate::vll::PendingOpState::Ready {
                    // Was holding locks
                    intent_table.release_locks(&op.keys, op.mode);
                }
                intent_table.remove_all_intents(&op.keys, txid);
            }
        }
    }

    /// Handle VLL continuation lock - acquire full shard lock for MULTI/Lua.
    ///
    /// This is non-blocking: we store the release receiver and poll it in the main loop,
    /// allowing the shard to continue processing messages from the lock owner while locked.
    pub(crate) async fn handle_vll_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) {
        // Check if continuation lock is already held
        if self.continuation_lock.is_some() {
            let _ = ready_tx.send(ShardReadyResult::Failed(VllError::ShardBusy));
            return;
        }

        // Wait for queue to drain
        let drain_timeout = std::time::Duration::from_millis(2000);
        let start = std::time::Instant::now();

        while let Some(tx_queue) = self.tx_queue.as_ref() {
            if tx_queue.is_empty() {
                break;
            }
            if start.elapsed() > drain_timeout {
                let _ = ready_tx.send(ShardReadyResult::Failed(VllError::LockTimeout));
                return;
            }
            // Yield to allow pending ops to complete
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Acquire continuation lock
        self.continuation_lock = Some(crate::vll::ContinuationLock::new(txid, conn_id));

        // Store release receiver for polling in main loop (non-blocking!)
        self.pending_continuation_release = Some(release_rx);

        // Notify ready - shard continues processing messages from lock owner
        let _ = ready_tx.send(ShardReadyResult::Ready);
    }

    /// Process waiting VLL operations after one completes.
    async fn process_waiting_vll_ops(&mut self) {
        let tx_queue = match self.tx_queue.as_ref() {
            Some(q) => q,
            None => return,
        };

        // Get all pending txids
        let pending_txids: Vec<u64> = tx_queue
            .iter()
            .filter(|(_, op)| op.state == crate::vll::PendingOpState::Pending)
            .map(|(&txid, _)| txid)
            .collect();

        // Try to acquire locks for each
        for txid in pending_txids {
            self.try_acquire_vll_locks(txid).await;
        }
    }
}
