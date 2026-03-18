//! MONITOR command handler.

use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Handle the MONITOR command — subscribe this connection to the monitor event stream.
    pub(crate) async fn handle_monitor(&mut self) -> Response {
        let rx = self.observability.monitor_broadcaster.subscribe();
        self.monitor_rx = Some(rx);
        Response::ok()
    }
}
