//! Administrative command handlers.

use bytes::Bytes;
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle SHUTDOWN command.
    pub(crate) async fn handle_shutdown(&self, _args: &[Bytes]) -> Response {
        // Note: Actual shutdown requires signaling the main server
        // For now, we just return an error suggesting manual shutdown
        Response::error(
            "ERR SHUTDOWN is not supported in this mode. Use Ctrl+C to stop the server.",
        )
    }
}
