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
                shard_id = self.identity.shard_id(),
                queue_depth = depth,
                "Shard message queue depth high"
            );
        }
        if outcome.enqueue_failed {
            tracing::warn!(
                shard_id = self.identity.shard_id(),
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

        // Panic isolation (c2-07). The release below is the whole reason this
        // site needs its own guard rather than relying on the outer net: an
        // unwind past it leaks the op's key locks *and* leaves `executing_ops`
        // incremented, which permanently blocks every later request on those
        // keys and any parked continuation lock. So the panic path releases
        // exactly as the success path does — the lock never stays owned by a
        // command that no longer exists.
        let outcome =
            super::panic_guard::caught(self.execute_scatter_part(&op.keys, &op.operation, 0)).await;

        let result = match outcome {
            Ok(result) => result,
            Err(panic_message) => {
                let err = self.recover_from_panic(
                    super::panic_guard::PanicSite::VllExecute,
                    op.operation.name(),
                    &panic_message,
                );
                Self::scatter_error_reply(&op.operation, &op.keys, err)
            }
        };

        self.vll.release_after_execution(op.txid, &op.keys);

        let _ = response_tx.send(result);
    }

    /// Handle VLL abort — discard the pending op and advance waiters.
    pub(crate) fn handle_vll_abort(&mut self, txid: u64) {
        self.vll.abort(txid);
    }

    /// Handle VLL continuation lock — take the shard exclusively, or park the
    /// request until it drains.
    ///
    /// Never waits: this runs on the shard's own event loop, and that loop is
    /// what drains the queue the lock is waiting for. A parked request is
    /// answered from a later drain point, and its deadline plus the release
    /// signal are both served by the loop's continuation-event arm
    /// (`event_loop.rs`), so the shard keeps processing messages throughout.
    pub(crate) fn handle_vll_continuation_lock(
        &mut self,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) {
        self.vll
            .request_continuation_lock(txid, conn_id, ready_tx, release_rx);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    use crate::eviction::EvictionConfig;
    use crate::noop::NoopMetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{ScatterOp, ShardReceiver, ShardSender};
    use crate::vll::{LockMode, ShardReadyResult};

    fn test_worker() -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_, conn_rx) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    /// The continuation-lock handler runs on the shard's own event loop, and
    /// the queue it waits on only drains when that loop processes a
    /// `VllExecute`. So the handler must never block on the drain: it parks
    /// the request, returns immediately, and the lock is granted from the
    /// drain point once the queued op has executed.
    ///
    /// Time is paused, so any wait inside the handler shows up as virtual
    /// clock advance — the assertion that no time passed is what fails if the
    /// handler goes back to waiting inline.
    // FM-VLL-003
    #[tokio::test(start_paused = true)]
    async fn continuation_request_does_not_stall_the_shard_event_loop() {
        let mut worker = test_worker();
        let key = Bytes::from_static(b"k");

        // An SCA op is queued on this shard, holding its lock until the
        // coordinator sends `VllExecute`.
        let (sca_ready_tx, sca_ready_rx) = oneshot::channel();
        worker
            .handle_vll_lock_request(
                1,
                vec![key.clone()],
                LockMode::Write,
                ScatterOp::MGet,
                sca_ready_tx,
            )
            .await;
        assert!(matches!(sca_ready_rx.await, Ok(ShardReadyResult::Ready)));

        // A cross-shard script now asks for the continuation lock.
        let (cont_ready_tx, mut cont_ready_rx) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel();
        let before = tokio::time::Instant::now();
        worker.handle_vll_continuation_lock(2, 99, cont_ready_tx, release_rx);
        assert_eq!(
            tokio::time::Instant::now(),
            before,
            "the continuation request must not stall the shard event loop"
        );
        assert!(
            cont_ready_rx.try_recv().is_err(),
            "the lock must not be granted while an op is still queued"
        );

        // The shard is still able to process messages: the queued op executes
        // and releases its locks.
        let (exec_tx, exec_rx) = oneshot::channel();
        worker.handle_vll_execute(1, exec_tx).await;
        assert!(exec_rx.await.is_ok());

        // Draining the queue grants the parked request.
        assert!(matches!(cont_ready_rx.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(worker.vll.continuation_lock_owner(), Some(99));
    }
}
