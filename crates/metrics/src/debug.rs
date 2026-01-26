//! Debug web UI for FrogDB.
//!
//! Provides a simple web interface for debugging and monitoring the server.

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

/// Debug state shared across requests.
#[derive(Debug, Clone)]
pub struct DebugState {
    /// Server info.
    pub info: ServerInfo,
}

impl DebugState {
    /// Create a new debug state.
    pub fn new(info: ServerInfo) -> Self {
        Self { info }
    }
}

/// Handle debug web UI requests.
pub async fn handle_debug_request(
    path: &str,
    state: &Arc<DebugState>,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    match path {
        "/debug" | "/debug/" => render_debug_index(state, recorder),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "text/plain")
            .body(Full::new(Bytes::from("Debug page not found")))
            .unwrap(),
    }
}

/// Render the main debug index page.
fn render_debug_index(
    state: &Arc<DebugState>,
    _recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let uptime = state.info.start_time.elapsed();
    let uptime_str = format!(
        "{}h {}m {}s",
        uptime.as_secs() / 3600,
        (uptime.as_secs() % 3600) / 60,
        uptime.as_secs() % 60
    );

    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>FrogDB Debug</title>
    <style>
        body {{ font-family: system-ui, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; }}
        .info {{ margin: 10px 0; }}
        .label {{ font-weight: bold; color: #7f8c8d; }}
        .value {{ color: #2c3e50; }}
        a {{ color: #3498db; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>FrogDB Debug</h1>
        <div class="info"><span class="label">Version:</span> <span class="value">{}</span></div>
        <div class="info"><span class="label">Uptime:</span> <span class="value">{}</span></div>
        <div class="info"><span class="label">Shards:</span> <span class="value">{}</span></div>
        <div class="info"><span class="label">Bind:</span> <span class="value">{}:{}</span></div>
        <p><a href="/metrics">Prometheus Metrics</a></p>
        <p><a href="/health/live">Health Check (Liveness)</a></p>
        <p><a href="/health/ready">Health Check (Readiness)</a></p>
    </div>
</body>
</html>"#,
        state.info.version,
        uptime_str,
        state.info.num_shards,
        state.info.bind_addr,
        state.info.port
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(html)))
        .unwrap()
}
