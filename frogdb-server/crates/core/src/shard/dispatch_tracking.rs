use super::message::ShardMessage;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch client tracking messages.
    pub(super) fn dispatch_tracking(&mut self, msg: ShardMessage) {
        match msg {
            ShardMessage::TrackingRegister {
                conn_id,
                sender,
                noloop,
            } => {
                self.tracking.register(
                    conn_id,
                    crate::tracking::TrackedConnection { sender, noloop },
                );
            }
            ShardMessage::TrackingUnregister { conn_id } => {
                self.tracking.unregister(conn_id);
            }
            ShardMessage::TrackingBroadcastRegister {
                conn_id,
                sender,
                noloop,
                prefixes,
            } => {
                self.tracking.register_broadcast(
                    conn_id,
                    crate::tracking::TrackedConnection { sender, noloop },
                    &prefixes,
                );
            }
            _ => unreachable!(),
        }
    }
}
