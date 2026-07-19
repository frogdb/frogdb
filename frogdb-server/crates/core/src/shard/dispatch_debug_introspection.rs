//! Dispatch for the always-available DEBUG introspection messages
//! (LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK). Each is a
//! read-only per-shard snapshot handled inside the shard event loop — the
//! probe surface the concurrency-invariant quiescence checkers consult. All
//! collectors are `&self`; none await.

use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch a DEBUG introspection message: build the per-shard snapshot and
    /// reply on its oneshot.
    pub(super) fn dispatch_debug_introspection(&self, msg: ShardMessage) {
        match msg {
            ShardMessage::GetLockTableInfo { response_tx } => {
                let _ = response_tx.send(self.collect_lock_table_info());
            }
            ShardMessage::GetWaitQueueInfo { response_tx } => {
                let _ = response_tx.send(self.collect_wait_queue_info());
            }
            ShardMessage::MemoryCheck { response_tx } => {
                let _ = response_tx.send(self.collect_memory_check());
            }
            _ => unreachable!("dispatch_debug_introspection got a non-introspection message"),
        }
    }
}
