use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch blocking command messages (BlockWait, UnregisterWait).
    pub(super) fn dispatch_blocking(&mut self, msg: ShardMessage) {
        match msg {
            ShardMessage::BlockWait {
                conn_id,
                keys,
                op,
                response_tx,
                deadline,
                protocol_version,
            } => {
                self.handle_block_wait(conn_id, keys, op, response_tx, deadline, protocol_version);
            }
            ShardMessage::UnregisterWait { conn_id } => {
                self.handle_unregister_wait(conn_id);
            }
            _ => unreachable!(),
        }
    }
}
