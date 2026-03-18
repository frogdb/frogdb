use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch cluster/raft messages (SlotMigrated, RaftCommand).
    pub(super) async fn dispatch_cluster(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::SlotMigrated { slot, target_addr } => {
                self.handle_slot_migrated(slot, target_addr);
                self.handle_slot_migrated_pubsub(slot);
            }
            ShardMessage::RaftCommand { cmd, response_tx } => {
                let result = if let Some(ref raft) = self.cluster.raft {
                    raft.client_write(cmd)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                } else {
                    Err("Raft not initialized".to_string())
                };
                let _ = response_tx.send(result);
            }
            _ => unreachable!(),
        }
        false
    }
}
