//! STATUS JSON command handler.
//!
//! Renders the machine-readable server status from the *same*
//! [`frogdb_telemetry::StatusCollector`] the HTTP `/status` endpoint uses, so the
//! two observability surfaces can never disagree. The STATUS JSON executor (see
//! [`crate::connection::observability_conn_command`]) reads only
//! [`frogdb_core::ConnCtx::status`] and delegates here — there is no second
//! hand-assembled scatter or field list.

use std::sync::Arc;

use bytes::Bytes;
use frogdb_core::{BoxFuture, StatusProvider};
use frogdb_protocol::Response;
use frogdb_telemetry::StatusCollector;

use crate::connection::ConnectionHandler;

/// Collect and render the current server status as the JSON bulk-string reply.
/// The one place STATUS JSON is assembled: it drives the shared collector's
/// single-scatter [`StatusCollector::collect`] and its `to_json`, identical to
/// the HTTP `/status` path.
fn render_status_json(collector: Arc<StatusCollector>) -> BoxFuture<'static, Response> {
    Box::pin(async move {
        let status = collector.collect().await;
        Response::bulk(Bytes::from(collector.to_json(&status)))
    })
}

/// STATUS JSON is dispatched through the [`frogdb_core::ConnectionCommand`] seam:
/// its executor reads [`frogdb_core::ConnCtx::status`] and delegates here.
impl StatusProvider for ConnectionHandler {
    fn status_json<'a>(&'a self) -> BoxFuture<'a, Response> {
        match &self.observability.status_collector {
            Some(collector) => render_status_json(collector.clone()),
            // Only the test-default deps lack a collector; production always
            // wires one (built unconditionally in `start_subsystems`).
            None => Box::pin(async { Response::error("ERR status collector unavailable") }),
        }
    }
}

/// A [`StatusProvider`] over an owned collector, so a fixture without a full
/// [`ConnectionHandler`] can render STATUS JSON from a real collector through the
/// same [`render_status_json`] path the handler uses.
#[cfg(test)]
pub(crate) struct StatusCollectorProvider(pub(crate) Arc<StatusCollector>);

#[cfg(test)]
impl StatusProvider for StatusCollectorProvider {
    fn status_json<'a>(&'a self) -> BoxFuture<'a, Response> {
        render_status_json(self.0.clone())
    }
}
