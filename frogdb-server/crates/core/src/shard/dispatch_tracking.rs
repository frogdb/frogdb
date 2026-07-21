use super::message::TrackingMsg;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Dispatch client tracking messages.
    pub(super) fn dispatch_tracking(&mut self, msg: TrackingMsg) {
        match msg {
            TrackingMsg::TrackingRegister {
                conn_id,
                sender,
                noloop,
            } => {
                self.tracking.register(
                    conn_id,
                    crate::tracking::TrackedConnection { sender, noloop },
                );
            }
            TrackingMsg::TrackingUnregister { conn_id } => {
                self.tracking.unregister(conn_id);
            }
            TrackingMsg::TrackingBroadcastRegister {
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
        }
    }
}
