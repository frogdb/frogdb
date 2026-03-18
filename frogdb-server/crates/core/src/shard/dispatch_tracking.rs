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
                self.tracking.invalidation_registry.register(
                    conn_id,
                    crate::tracking::TrackedConnection { sender, noloop },
                );
            }
            ShardMessage::TrackingUnregister { conn_id } => {
                self.tracking.tracking_table.remove_connection(conn_id);
                self.tracking.broadcast_table.remove_connection(conn_id);
                self.tracking.invalidation_registry.unregister(conn_id);
            }
            ShardMessage::TrackingBroadcastRegister {
                conn_id,
                sender,
                noloop,
                prefixes,
            } => {
                self.tracking.invalidation_registry.register(
                    conn_id,
                    crate::tracking::TrackedConnection { sender, noloop },
                );
                self.tracking.broadcast_table.register(conn_id, &prefixes);
            }
            _ => unreachable!(),
        }
    }
}
