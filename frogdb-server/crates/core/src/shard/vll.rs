use bytes::Bytes;
use tokio::sync::oneshot;

use crate::vll::{LockMode, ShardReadyResult};

use super::message::ScatterOp;
use super::types::PartialResult;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle a VLL lock request — declare intents, enqueue, and try-acquire.
    pub(crate) async fn handle_vll_lock_request(
        &mut self,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: ScatterOp,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) {
        let outcome = self
            .vll
            .enqueue_lock_request(txid, keys, mode, operation, ready_tx);
        if let Some(depth) = outcome.queue_depth_warning {
            tracing::warn!(
                shard_id = self.identity.shard_id,
                queue_depth = depth,
                "Shard message queue depth high"
            );
        }
        if outcome.enqueue_failed {
            tracing::warn!(
                shard_id = self.identity.shard_id,
                txid,
                "Failed to enqueue VLL operation"
            );
        }
    }

    /// Handle VLL execute — run the dequeued op and release locks afterward.
    pub(crate) async fn handle_vll_execute(
        &mut self,
        txid: u64,
        response_tx: oneshot::Sender<PartialResult>,
    ) {
        let Some(op) = self.vll.dequeue_for_execution(txid) else {
            let _ = response_tx.send(PartialResult::default());
            return;
        };

        let result = self.execute_scatter_part(&op.keys, &op.operation, 0).await;

        self.vll.release_after_execution(op.txid, &op.keys);

        let _ = response_tx.send(result);
    }

    /// Handle VLL abort — discard the pending op and advance waiters.
    pub(crate) fn handle_vll_abort(&mut self, txid: u64) {
        self.vll.abort(txid);
    }

    /// Handle VLL continuation lock — drain queue, then take shard-exclusive.
    ///
    /// The release receiver is stored on the state machine and polled in the
    /// main event loop so the shard can keep processing messages from the
    /// lock owner while the lock is held.
    pub(crate) async fn handle_vll_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) {
        self.vll
            .acquire_continuation_lock(txid, conn_id, ready_tx, release_rx)
            .await;
    }
}
