//! Debug web UI for FrogDB.
//!
//! Provides a simple HTML interface for viewing server state and metrics.

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use std::sync::Arc;
use std::time::Instant;

use crate::prometheus_recorder::PrometheusRecorder;

/// Server information for the debug UI.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    /// Server version.
    pub version: String,
    /// Server start time.
    pub start_time: Instant,
    /// Number of shards.
    pub num_shards: usize,
    /// Bind address.
    pub bind_addr: String,
    /// Port number.
    pub port: u16,
}

/// Debug state for the debug web UI.
#[derive(Debug, Clone)]
pub struct DebugState {
    /// Server information.
    pub info: ServerInfo,
}

impl DebugState {
    /// Create a new debug state.
    pub fn new(info: ServerInfo) -> Self {
        Self { info }
    }
}

/// Handle a debug request.
pub async fn handle_debug_request(
    path: &str,
    state: &Arc<DebugState>,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    match path {
        "/debug" | "/debug/" => render_index(state),
        "/debug/metrics" => render_metrics_page(recorder),
        _ => not_found(),
    }
}

/// Render the debug index page.
fn render_index(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let uptime = state.info.start_time.elapsed();
    let uptime_secs = uptime.as_secs();
    let hours = uptime_secs / 3600;
    let minutes = (uptime_secs % 3600) / 60;
    let seconds = uptime_secs % 60;

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>FrogDB Debug</title>
    <style>
        body {{ font-family: sans-serif; margin: 40px; }}
        h1 {{ color: #2e7d32; }}
        .info {{ background: #f5f5f5; padding: 20px; border-radius: 8px; }}
        .info dt {{ font-weight: bold; margin-top: 10px; }}
        .info dd {{ margin-left: 20px; }}
        a {{ color: #1976d2; }}
    </style>
</head>
<body>
    <h1>🐸 FrogDB Debug</h1>
    <div class="info">
        <dl>
            <dt>Version</dt>
            <dd>{}</dd>
            <dt>Uptime</dt>
            <dd>{}h {}m {}s</dd>
            <dt>Shards</dt>
            <dd>{}</dd>
            <dt>Bind Address</dt>
            <dd>{}:{}</dd>
        </dl>
    </div>
    <h2>Links</h2>
    <ul>
        <li><a href="/metrics">Prometheus Metrics</a></li>
        <li><a href="/health/live">Liveness Check</a></li>
        <li><a href="/health/ready">Readiness Check</a></li>
        <li><a href="/status/json">Status JSON</a></li>
    </ul>
</body>
</html>"#,
        state.info.version,
        hours,
        minutes,
        seconds,
        state.info.num_shards,
        state.info.bind_addr,
        state.info.port,
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(html)))
        .unwrap()
}

/// Render a simple metrics page.
fn render_metrics_page(recorder: &Arc<PrometheusRecorder>) -> Response<Full<Bytes>> {
    let metrics = recorder.encode();
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>FrogDB Metrics</title>
    <style>
        body {{ font-family: monospace; margin: 40px; }}
        pre {{ background: #f5f5f5; padding: 20px; overflow-x: auto; }}
    </style>
</head>
<body>
    <h1>FrogDB Metrics</h1>
    <pre>{}</pre>
</body>
</html>"#,
        metrics
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(html)))
        .unwrap()
}

/// Return a 404 Not Found response.
fn not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}
