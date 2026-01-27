//! Debug web UI for FrogDB.
//!
//! This module provides a web-based debug interface for inspecting server state,
//! metrics, and diagnostics.

use bytes::Bytes;
use http_body_util::Full;
use hyper::http::{Uri, StatusCode};
use hyper::Response;
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
    /// Server port.
    pub port: u16,
}

/// Debug state for the web UI.
#[derive(Debug)]
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
///
/// Routes to different debug endpoints based on the URI path.
pub async fn handle_debug_request(
    uri: &Uri,
    state: &Arc<DebugState>,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let path = uri.path();

    match path {
        "/debug" | "/debug/" => handle_debug_index(state),
        "/debug/info" => handle_debug_info(state),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Debug endpoint not found")))
            .unwrap(),
    }
}

/// Handle the debug index page.
fn handle_debug_index(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let uptime = state.info.start_time.elapsed();
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <title>FrogDB Debug</title>
    <style>
        body {{ font-family: sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        .info {{ margin: 10px 0; }}
        .label {{ font-weight: bold; }}
    </style>
</head>
<body>
    <h1>FrogDB Debug Interface</h1>
    <div class="info"><span class="label">Version:</span> {}</div>
    <div class="info"><span class="label">Uptime:</span> {:.1}s</div>
    <div class="info"><span class="label">Shards:</span> {}</div>
    <div class="info"><span class="label">Address:</span> {}:{}</div>
    <h2>Endpoints</h2>
    <ul>
        <li><a href="/debug/info">/debug/info</a> - Server information (JSON)</li>
        <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
        <li><a href="/health/live">/health/live</a> - Liveness check</li>
        <li><a href="/health/ready">/health/ready</a> - Readiness check</li>
    </ul>
</body>
</html>"#,
        state.info.version,
        uptime.as_secs_f64(),
        state.info.num_shards,
        state.info.bind_addr,
        state.info.port
    );

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html")
        .body(Full::new(Bytes::from(html)))
        .unwrap()
}

/// Handle the debug info endpoint.
fn handle_debug_info(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let uptime = state.info.start_time.elapsed();
    let json = serde_json::json!({
        "version": state.info.version,
        "uptime_seconds": uptime.as_secs_f64(),
        "num_shards": state.info.num_shards,
        "bind_addr": state.info.bind_addr,
        "port": state.info.port,
    });

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(json.to_string())))
        .unwrap()
}
