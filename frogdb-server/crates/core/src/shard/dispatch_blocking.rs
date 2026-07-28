use super::message::BlockingMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch blocking command messages (BlockWait, UnregisterWait).
    pub(super) fn dispatch_blocking(&mut self, msg: BlockingMsg) {
        match msg {
            BlockingMsg::BlockWait {
                conn_id,
                keys,
                op,
                response_tx,
                deadline,
                protocol_version,
            } => {
                self.handle_block_wait(conn_id, keys, op, response_tx, deadline, protocol_version);
            }
            BlockingMsg::UnregisterWait { conn_id, ack } => {
                self.handle_unregister_wait(conn_id, ack);
            }
        }
    }
}
