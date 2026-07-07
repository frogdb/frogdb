//! Connection-state reset handler.
//!
//! AUTH and HELLO were migrated behind the [`ConnectionCommand`] seam (see
//! [`crate::connection::auth_conn_command`]); what remains here is RESET, which
//! is not yet migrated.
//!
//! [`ConnectionCommand`]: frogdb_core::ConnectionCommand

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::{ConnectionHandler, ShardMessage};

impl ConnectionHandler {
    /// Handle RESET command - reset connection to initial state.
    /// This exits pub/sub mode, clears transaction and tracking state,
    /// and resets protocol to RESP2.
    pub(crate) async fn handle_reset(&mut self) -> Response {
        // State half: exit pub/sub mode, clear tracking + transaction state,
        // reset the protocol to RESP2, and clear the client name. The returned
        // effects drive the I/O half below.
        let effects = self.state.reset();

        // 1. Notify shards to remove subscriptions and/or tracking state. The
        //    original code sent ConnectionClosed once if either was active.
        if effects.was_in_pubsub || effects.tracking_was_enabled {
            for sender in self.core.shard_senders.iter() {
                let _ = sender
                    .send(ShardMessage::ConnectionClosed {
                        conn_id: self.state.id,
                    })
                    .await;
            }
        }

        // 1.5. Tear down the tracking session's local plumbing (invalidation
        //      channels + redirect forwarder). Shard-side tracking state was
        //      removed by the ConnectionClosed fan-out above — its tracking
        //      half is identical to the TrackingUnregister that CLIENT
        //      TRACKING OFF sends. Idempotent, so no tracking_was_enabled gate.
        self.tracking_session_teardown_local();

        // 4. Exit MONITOR mode
        self.monitor_rx = None;

        // 5. Clear client name in the registry (local state cleared by reset()).
        self.admin.client_registry.update_name(self.state.id, None);

        // Note: lib_name/lib_ver are NOT cleared by RESET (per Redis semantics).
        // These persist across RESET so client libraries retain their identity.

        // Return RESET acknowledgment
        Response::Simple(Bytes::from_static(b"RESET"))
    }
}
