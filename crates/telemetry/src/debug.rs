//! Debug web UI for FrogDB.
//!
//! This module provides a web-based debug interface for inspecting server state,
//! metrics, and diagnostics.

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode, Uri};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::bundle::{BundleConfig, BundleGenerator, BundleStore, DiagnosticData};
use crate::prometheus_recorder::PrometheusRecorder;

#[derive(RustEmbed)]
#[folder = "assets/"]
struct Assets;

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

impl Default for ServerInfo {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            start_time: Instant::now(),
            num_shards: 1,
            bind_addr: "127.0.0.1".to_string(),
            port: 6379,
        }
    }
}

/// A configuration entry for display in the debug UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigEntry {
    pub name: String,
    pub value: String,
}

/// Debug state for the web UI.
#[derive(Debug)]
pub struct DebugState {
    /// Server information.
    pub info: ServerInfo,
    /// Configuration entries.
    pub config_entries: Vec<ConfigEntry>,
}

impl DebugState {
    /// Create a new debug state.
    pub fn new(info: ServerInfo, config_entries: Vec<ConfigEntry>) -> Self {
        Self {
            info,
            config_entries,
        }
    }

    /// Get uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        self.info.start_time.elapsed().as_secs()
    }
}

/// Serializable version of ServerInfo for JSON endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServerInfoJson {
    version: String,
    uptime_secs: u64,
    num_shards: usize,
    bind_addr: String,
    port: u16,
}

impl From<&ServerInfo> for ServerInfoJson {
    fn from(info: &ServerInfo) -> Self {
        Self {
            version: info.version.clone(),
            uptime_secs: info.start_time.elapsed().as_secs(),
            num_shards: info.num_shards,
            bind_addr: info.bind_addr.clone(),
            port: info.port,
        }
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
        "/debug" | "/debug/" => handle_debug_index(),
        "/debug/info" => handle_debug_info(state),

        // Static assets
        p if p.starts_with("/debug/assets/") => handle_debug_asset(p),

        // JSON APIs
        "/debug/api/cluster" => handle_api_cluster(state),
        "/debug/api/config" => handle_api_config(state),
        "/debug/api/metrics" => handle_api_metrics(recorder),
        "/debug/api/slowlog" => handle_api_slowlog(),
        "/debug/api/latency" => handle_api_latency(),
        "/debug/api/bundle/list" => handle_api_bundle_list(),
        "/debug/api/bundle/generate" => handle_api_bundle_generate(uri),

        // Bundle download by ID
        p if p.starts_with("/debug/api/bundle/") => {
            let id = &p["/debug/api/bundle/".len()..];
            handle_api_bundle_download(id)
        }

        // HTML partials
        "/debug/partials/cluster" => handle_partial_cluster(state),
        "/debug/partials/config" => handle_partial_config(state),
        "/debug/partials/metrics" => handle_partial_metrics(),
        "/debug/partials/slowlog" => handle_partial_slowlog(),
        "/debug/partials/latency" => handle_partial_latency(),

        _ => not_found(),
    }
}

// ---------------------------------------------------------------------------
// Index page (embedded HTML asset)
// ---------------------------------------------------------------------------

fn handle_debug_index() -> Response<Full<Bytes>> {
    match Assets::get("index.html") {
        Some(content) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "text/html; charset=utf-8")
            .body(Full::new(Bytes::from(content.data.to_vec())))
            .unwrap(),
        None => not_found(),
    }
}

// ---------------------------------------------------------------------------
// Static asset serving via RustEmbed
// ---------------------------------------------------------------------------

fn handle_debug_asset(path: &str) -> Response<Full<Bytes>> {
    // Strip the "/debug/assets/" prefix to get the asset path
    let asset_path = path.strip_prefix("/debug/assets/").unwrap_or("");

    match Assets::get(asset_path) {
        Some(content) => {
            let mime = mime_guess::from_path(asset_path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", mime.as_ref())
                .body(Full::new(Bytes::from(content.data.to_vec())))
                .unwrap()
        }
        None => not_found(),
    }
}

// ---------------------------------------------------------------------------
// JSON API handlers
// ---------------------------------------------------------------------------

fn handle_debug_info(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let info_json = ServerInfoJson::from(&state.info);
    let body = serde_json::to_string(&info_json).unwrap_or_default();

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn handle_api_cluster(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let json = serde_json::json!({
        "role": "standalone",
        "uptime_seconds": state.uptime_secs(),
        "num_shards": state.info.num_shards,
        "version": state.info.version,
        "bind_addr": state.info.bind_addr,
        "port": state.info.port,
    });
    json_response(&json)
}

fn handle_api_config(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let json = serde_json::json!({
        "entries": state.config_entries,
    });
    json_response(&json)
}

fn handle_api_metrics(recorder: &Arc<PrometheusRecorder>) -> Response<Full<Bytes>> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let metrics_text = recorder.encode();

    let throughput = extract_metric_value(&metrics_text, "frogdb_commands_total");
    let connections_current = extract_metric_value(&metrics_text, "frogdb_connections_current");
    let keys_total = extract_metric_value(&metrics_text, "frogdb_keys_total");

    let json = serde_json::json!({
        "timestamp": timestamp,
        "throughput": throughput,
        "connections_current": connections_current,
        "keys_total": keys_total,
    });
    json_response(&json)
}

fn handle_api_slowlog() -> Response<Full<Bytes>> {
    let json = serde_json::json!({ "entries": [] });
    json_response(&json)
}

fn handle_api_latency() -> Response<Full<Bytes>> {
    let json = serde_json::json!({ "data": [] });
    json_response(&json)
}

// ---------------------------------------------------------------------------
// Bundle API handlers
// ---------------------------------------------------------------------------

fn handle_api_bundle_list() -> Response<Full<Bytes>> {
    match BundleStore::new(BundleConfig::default()).list() {
        bundles => {
            let json = serde_json::json!({ "bundles": bundles });
            json_response(&json)
        }
    }
}

fn handle_api_bundle_generate(uri: &Uri) -> Response<Full<Bytes>> {
    // Parse optional ?duration=N query param
    let _duration: u64 = uri
        .query()
        .and_then(|q| q.split('&').find_map(|pair| pair.strip_prefix("duration=")))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // Generate a minimal bundle without shard data (DiagnosticCollector requires
    // shard_senders which aren't available in the metrics crate's debug handler).
    let config = BundleConfig::default();
    let generator = BundleGenerator::new(config);
    let id = BundleGenerator::generate_id();

    let data = DiagnosticData::default();

    match generator.create_zip(&id, &data, _duration) {
        Ok(zip_data) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/zip")
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"frogdb-bundle-{}.zip\"", id),
            )
            .body(Full::new(Bytes::from(zip_data)))
            .unwrap(),
        Err(_) => service_unavailable("Bundle generation failed"),
    }
}

fn handle_api_bundle_download(id: &str) -> Response<Full<Bytes>> {
    let config = BundleConfig::default();
    if !config.directory.exists() {
        return service_unavailable("Bundle storage not available");
    }
    let store = BundleStore::new(config);
    match store.get(id) {
        Some(data) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/zip")
            .header(
                "Content-Disposition",
                format!("attachment; filename=\"frogdb-bundle-{}.zip\"", id),
            )
            .body(Full::new(Bytes::from(data)))
            .unwrap(),
        None => not_found(),
    }
}

// ---------------------------------------------------------------------------
// HTML partial handlers
// ---------------------------------------------------------------------------

fn handle_partial_cluster(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let uptime = state.uptime_secs();
    let html = format!(
        r#"<div class="cluster-info">
  <h3>Cluster Overview</h3>
  <dl>
    <dt>Role</dt><dd>standalone</dd>
    <dt>Uptime</dt><dd>{}s</dd>
    <dt>Version</dt><dd>{}</dd>
    <dt>Shards</dt><dd>{}</dd>
    <dt>Address</dt><dd>{}:{}</dd>
  </dl>
</div>"#,
        uptime, state.info.version, state.info.num_shards, state.info.bind_addr, state.info.port,
    );
    html_response(&html)
}

fn handle_partial_config(state: &Arc<DebugState>) -> Response<Full<Bytes>> {
    let mut rows = String::new();
    for entry in &state.config_entries {
        rows.push_str(&format!(
            "    <tr><td>{}</td><td>{}</td></tr>\n",
            entry.name, entry.value
        ));
    }
    let html = format!(
        r#"<div class="config-info">
  <h3>Configuration</h3>
  <table>
    <thead><tr><th>Name</th><th>Value</th></tr></thead>
    <tbody>
{}    </tbody>
  </table>
</div>"#,
        rows
    );
    html_response(&html)
}

fn handle_partial_metrics() -> Response<Full<Bytes>> {
    let html = r#"<div class="metrics-overview" data-metrics="true">
  <h3>Metrics</h3>
  <p>Metrics overview — charts will auto-populate via API polling.</p>
</div>"#;
    html_response(html)
}

fn handle_partial_slowlog() -> Response<Full<Bytes>> {
    let html = r#"<div class="slowlog-info">
  <h3>Slow Log</h3>
  <p>No slow log entries available.</p>
</div>"#;
    html_response(html)
}

fn handle_partial_latency() -> Response<Full<Bytes>> {
    let html = r#"<div class="latency-info">
  <h3>Latency</h3>
  <p>No latency data available.</p>
</div>"#;
    html_response(html)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn json_response(value: &serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_string(value).unwrap_or_default();
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn html_response(html: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(html.to_string())))
        .unwrap()
}

fn not_found() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from("Not found")))
        .unwrap()
}

fn service_unavailable(msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header("Content-Type", "text/plain")
        .body(Full::new(Bytes::from(msg.to_string())))
        .unwrap()
}

/// Extract a summed metric value from Prometheus text output.
fn extract_metric_value(text: &str, metric_name: &str) -> f64 {
    let mut total = 0.0;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Match lines like "metric_name{labels} value" or "metric_name value"
        if line.starts_with(metric_name) {
            let rest = &line[metric_name.len()..];
            // Value is after the space following labels (or directly after the name)
            let value_str = if rest.starts_with('{') {
                // Has labels: find closing brace then space
                rest.find('}')
                    .and_then(|i| rest[i + 1..].trim().split_whitespace().next())
            } else {
                rest.trim().split_whitespace().next()
            };
            if let Some(v) = value_str.and_then(|s| s.parse::<f64>().ok()) {
                total += v;
            }
        }
    }
    total
}
