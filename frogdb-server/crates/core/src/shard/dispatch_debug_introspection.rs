//! Dispatch for the always-available DEBUG introspection messages
//! (LOCKTABLE / WAITQUEUE / MEMORY-CHECK / EXPIRY-INDEX-CHECK / EXPIRE-BACKDATE /
//! RE-ENCODE / OBJECT).
//! The snapshot probes are read-only per-shard collectors handled inside the
//! shard event loop — the probe surface the concurrency-invariant quiescence
//! checkers consult. EXPIRE-BACKDATE is the one mutator: it rewrites a single
//! key's expiry deadline (only), leaving the actual expiry to the normal
//! read/sweep seams. RE-ENCODE is the other: it rebuilds one key's value
//! through its own encoding, compacting it without changing what it holds.

use super::message::DebugIntrospectionMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch a DEBUG introspection message: build the per-shard snapshot (or,
    /// for EXPIRE-BACKDATE and RE-ENCODE, rewrite the key's deadline or its
    /// value's representation) and reply on its oneshot.
    pub(super) fn dispatch_debug_introspection(&mut self, msg: DebugIntrospectionMsg) {
        match msg {
            DebugIntrospectionMsg::GetLockTableInfo { response_tx } => {
                let _ = response_tx.send(self.collect_lock_table_info());
            }
            DebugIntrospectionMsg::GetWaitQueueInfo { response_tx } => {
                let _ = response_tx.send(self.collect_wait_queue_info());
            }
            DebugIntrospectionMsg::GetWaitQueueLog { response_tx } => {
                let _ = response_tx.send(self.collect_wait_queue_log());
            }
            DebugIntrospectionMsg::MemoryCheck { response_tx } => {
                let _ = response_tx.send(self.collect_memory_check());
            }
            DebugIntrospectionMsg::ExpiryIndexCheck { response_tx } => {
                let _ = response_tx.send(self.collect_expiry_index_check());
            }
            DebugIntrospectionMsg::ExpireBackdate {
                key,
                ms,
                response_tx,
            } => {
                let result = self.store.backdate_expiry(&key, ms);
                let _ = response_tx.send(result);
            }
            DebugIntrospectionMsg::ReEncode { key, response_tx } => {
                let _ = response_tx.send(self.store.re_encode(&key));
            }
            DebugIntrospectionMsg::ObjectInfo { key, response_tx } => {
                let _ = response_tx.send(self.collect_object_info(&key));
            }
        }
    }
}
