use super::message::ClusterMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch cluster/raft messages (SlotMigrated, RaftCommand).
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
        }
        false
    }
}
