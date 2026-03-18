use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch VLL (Very Lightweight Locking) messages.
    pub(super) async fn dispatch_vll(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::VllLockRequest {
                txid,
                keys,
                mode,
                operation,
                ready_tx,
                execute_rx,
            } => {
                self.handle_vll_lock_request(txid, keys, mode, operation, ready_tx, execute_rx)
                    .await;
            }
            ShardMessage::VllExecute { txid, response_tx } => {
                self.handle_vll_execute(txid, response_tx).await;
            }
            ShardMessage::VllAbort { txid } => {
                self.handle_vll_abort(txid);
            }
            ShardMessage::VllContinuationLock {
                txid,
                conn_id,
                ready_tx,
                release_rx,
            } => {
                self.handle_vll_continuation_lock(txid, conn_id, ready_tx, release_rx)
                    .await;
            }
            ShardMessage::GetVllQueueInfo { response_tx } => {
                let info = self.collect_vll_queue_info();
                let _ = response_tx.send(info);
            }
            _ => unreachable!(),
        }
        false
    }
}
