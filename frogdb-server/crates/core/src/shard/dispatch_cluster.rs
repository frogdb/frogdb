use super::message::ClusterMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch cluster/raft messages (SlotMigrated, RaftCommand, DrainSlot).
    pub(super) async fn dispatch_cluster(&mut self, msg: ClusterMsg) -> bool {
        match msg {
            ClusterMsg::SlotMigrated { slot, target_addr } => {
                self.handle_slot_migrated(slot, target_addr);
                self.handle_slot_migrated_pubsub(slot);
            }
            ClusterMsg::RaftCommand { cmd, response_tx } => {
                let result = if let Some(raft) = self.cluster.raft() {
                    raft.client_write(cmd)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                } else {
                    Err("Raft not initialized".to_string())
                };
                let _ = response_tx.send(result);
            }
            ClusterMsg::DrainSlot { slot, ack } => {
                // C3 continuation-lock classification: **no lock required**.
                // Reaching this arm is the entire semantics — every command
                // enqueued ahead of it has already run to completion in this
                // same single-threaded loop. The arm acquires no keys, holds no
                // cross-message state, and contains no await point, so there is
                // no continuation for a lock to protect.
                tracing::trace!(slot, "Shard drained for slot handoff");
                let _ = ack.send(());
            }
        }
        false
    }
}
