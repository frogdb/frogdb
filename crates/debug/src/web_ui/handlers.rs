//! HTTP handlers for the debug web UI.
//!
//! This module provides handlers for JSON API endpoints and HTML partials.

use super::state::DebugState;
use bytes::Bytes;
use frogdb_telemetry::PrometheusRecorder;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use serde::Serialize;
use std::sync::Arc;

/// Response helper to create JSON responses.
fn json_response<T: Serialize>(data: &T) -> Response<Full<Bytes>> {
    match serde_json::to_string(data) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .unwrap(),
        Err(_) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"serialization failed"}"#,
            )))
            .unwrap(),
    }
}

/// Response helper to create HTML responses.
fn html_response(html: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/html; charset=utf-8")
        .body(Full::new(Bytes::from(html)))
        .unwrap()
}

// ============================================================================
// JSON API Handlers
// ============================================================================

/// Cluster overview response.
#[derive(Serialize)]
pub struct ClusterResponse {
    pub version: String,
    pub role: String,
    pub uptime_seconds: u64,
    pub num_shards: usize,
    pub connected_replicas: usize,
    pub master_host: Option<String>,
    pub master_port: Option<u16>,
    pub replication_offset: u64,
    pub bind_addr: String,
    pub port: u16,
}

/// Handle GET /debug/api/cluster
pub fn handle_api_cluster(state: &DebugState) -> Response<Full<Bytes>> {
    let (connected_replicas, master_host, master_port, replication_offset) =
        if let Some(ref repl) = state.replication_info {
            (
                repl.connected_replicas(),
                repl.master_host(),
                repl.master_port(),
                repl.replication_offset(),
            )
        } else {
            (0, None, None, 0)
        };

    let response = ClusterResponse {
        version: state.server_info.version.clone(),
        role: state.role().to_string(),
        uptime_seconds: state.uptime_seconds(),
        num_shards: state.server_info.num_shards,
        connected_replicas,
        master_host,
        master_port,
        replication_offset,
        bind_addr: state.server_info.bind_addr.clone(),
        port: state.server_info.port,
    };

    json_response(&response)
}

/// Config entry for API response (includes source and mutability).
#[derive(Serialize)]
struct ApiConfigEntry {
    name: String,
    value: String,
    source: String,
    mutable: bool,
}

/// Config API response.
#[derive(Serialize)]
struct ConfigResponse {
    entries: Vec<ApiConfigEntry>,
}

/// Handle GET /debug/api/config
pub fn handle_api_config(state: &DebugState) -> Response<Full<Bytes>> {
    let entries: Vec<ApiConfigEntry> = if state.config_entries.is_empty() {
        // Fallback to basic config info from server_info
        vec![
            ApiConfigEntry {
                name: "bind".to_string(),
                value: state.server_info.bind_addr.clone(),
                source: "config".to_string(),
                mutable: false,
            },
            ApiConfigEntry {
                name: "port".to_string(),
                value: state.server_info.port.to_string(),
                source: "config".to_string(),
                mutable: false,
            },
            ApiConfigEntry {
                name: "shards".to_string(),
                value: state.server_info.num_shards.to_string(),
                source: "config".to_string(),
                mutable: false,
            },
        ]
    } else {
        state
            .config_entries
            .iter()
            .map(|e| ApiConfigEntry {
                name: e.name.clone(),
                value: e.value.clone(),
                source: "config".to_string(),
                mutable: false,
            })
            .collect()
    };

    json_response(&ConfigResponse { entries })
}

/// Metrics snapshot for charts.
#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub timestamp: u64,
    pub throughput: f64,
    pub latency_p50: f64,
    pub latency_p95: f64,
    pub latency_p99: f64,
    pub memory_used: u64,
    pub memory_peak: u64,
    pub connections_current: u64,
    pub keys_total: u64,
}

/// Handle GET /debug/api/metrics
pub fn handle_api_metrics(
    _state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let snapshot = MetricsSnapshot {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        throughput: recorder
            .get_counter_value("frogdb_commands_total")
            .unwrap_or(0.0),
        latency_p50: 0.0,
        latency_p95: 0.0,
        latency_p99: 0.0,
        memory_used: 0,
        memory_peak: 0,
        connections_current: recorder
            .get_gauge_value("frogdb_connections_current")
            .unwrap_or(0.0) as u64,
        keys_total: recorder.get_gauge_value("frogdb_keys_total").unwrap_or(0.0) as u64,
    };

    json_response(&snapshot)
}

/// Slowlog API response.
#[derive(Serialize)]
pub struct SlowlogResponse {
    pub entries: Vec<super::state::SlowlogEntry>,
}

/// Handle GET /debug/api/slowlog
pub async fn handle_api_slowlog(state: &DebugState) -> Response<Full<Bytes>> {
    let entries = state.get_slowlog(128).await;
    json_response(&SlowlogResponse { entries })
}

/// Latency API response.
#[derive(Serialize)]
pub struct LatencyResponse {
    pub data: Vec<super::state::LatencyData>,
}

/// Handle GET /debug/api/latency
pub async fn handle_api_latency(state: &DebugState) -> Response<Full<Bytes>> {
    let data = state.get_latency().await;
    json_response(&LatencyResponse { data })
}

// ============================================================================
// HTML Partial Handlers
// ============================================================================

/// Handle GET /debug/partials/cluster
pub fn handle_partial_cluster(state: &DebugState) -> Response<Full<Bytes>> {
    let uptime = state.uptime_seconds();
    let uptime_str = format_duration(uptime);
    let role = state.role();
    let role_badge_class = if role == "primary" || role == "standalone" {
        "badge-success"
    } else {
        "badge-info"
    };

    let (replicas_html, master_html) = if let Some(ref repl) = state.replication_info {
        let replicas = format!(
            r#"<div class="stat-item">
                <div class="stat-label">Connected Replicas</div>
                <div class="stat-value">{}</div>
            </div>"#,
            repl.connected_replicas()
        );

        let master = if let (Some(host), Some(port)) = (repl.master_host(), repl.master_port()) {
            format!(
                r#"<div class="stat-item">
                    <div class="stat-label">Master</div>
                    <div class="stat-value">{}:{}</div>
                </div>"#,
                host, port
            )
        } else {
            String::new()
        };

        (replicas, master)
    } else {
        (String::new(), String::new())
    };

    let html = format!(
        r#"<div class="card">
            <div class="card-header">
                <h3>Server Status</h3>
                <span class="badge {role_badge_class}">{role}</span>
            </div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-label">Uptime</div>
                    <div class="stat-value highlight">{uptime_str}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Shards</div>
                    <div class="stat-value">{}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Listen Address</div>
                    <div class="stat-value">{}:{}</div>
                </div>
                {replicas_html}
                {master_html}
            </div>
        </div>"#,
        state.server_info.num_shards, state.server_info.bind_addr, state.server_info.port
    );

    html_response(html)
}

/// Handle GET /debug/partials/config
pub fn handle_partial_config(state: &DebugState) -> Response<Full<Bytes>> {
    let entries: Vec<(&str, String, &str, bool)> = if state.config_entries.is_empty() {
        vec![
            ("bind", state.server_info.bind_addr.clone(), "config", false),
            ("port", state.server_info.port.to_string(), "config", false),
            (
                "shards",
                state.server_info.num_shards.to_string(),
                "config",
                false,
            ),
        ]
    } else {
        state
            .config_entries
            .iter()
            .map(|e| (e.name.as_str(), e.value.clone(), "config", false))
            .collect()
    };

    let rows: String = entries
        .iter()
        .map(|(name, value, source, mutable)| {
            let mutable_badge = if *mutable {
                r#"<span class="badge badge-success">mutable</span>"#
            } else {
                r#"<span class="badge badge-info">immutable</span>"#
            };
            format!(
                r#"<tr>
                    <td><code>{}</code></td>
                    <td class="config-value">{}</td>
                    <td class="config-source">{}</td>
                    <td>{}</td>
                </tr>"#,
                name, value, source, mutable_badge
            )
        })
        .collect();

    let html = format!(
        r#"<div class="card">
            <div class="card-header">
                <h3>Configuration</h3>
            </div>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Value</th>
                            <th>Source</th>
                            <th>Mutability</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>"#
    );

    html_response(html)
}

/// Handle GET /debug/partials/metrics
pub fn handle_partial_metrics(
    state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let commands_total = recorder
        .get_counter_value("frogdb_commands_total")
        .unwrap_or(0.0);
    let connections = recorder
        .get_gauge_value("frogdb_connections_current")
        .unwrap_or(0.0);
    let keys = recorder.get_gauge_value("frogdb_keys_total").unwrap_or(0.0);

    let snapshot = MetricsSnapshot {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        throughput: commands_total,
        latency_p50: 0.0,
        latency_p95: 0.0,
        latency_p99: 0.0,
        memory_used: 0,
        memory_peak: 0,
        connections_current: connections as u64,
        keys_total: keys as u64,
    };

    let metrics_json = serde_json::to_string(&snapshot).unwrap_or_default();

    let html = format!(
        r#"<div data-metrics='{metrics_json}'></div>
        <div class="card">
            <div class="card-header">
                <h3>Current Metrics</h3>
            </div>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-label">Total Commands</div>
                    <div class="stat-value highlight">{}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Current Connections</div>
                    <div class="stat-value">{}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Total Keys</div>
                    <div class="stat-value">{}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">Shards</div>
                    <div class="stat-value">{}</div>
                </div>
            </div>
        </div>"#,
        format_number(commands_total as u64),
        connections as u64,
        format_number(keys as u64),
        state.server_info.num_shards
    );

    html_response(html)
}

/// Handle GET /debug/partials/slowlog
pub async fn handle_partial_slowlog(state: &DebugState) -> Response<Full<Bytes>> {
    let entries = state.get_slowlog(50).await;

    let rows: String = if entries.is_empty() {
        r#"<tr><td colspan="5" style="text-align: center; color: var(--text-secondary);">No slow queries recorded</td></tr>"#.to_string()
    } else {
        entries
            .iter()
            .map(|entry| {
                format!(
                    r#"<tr>
                        <td>{}</td>
                        <td class="slowlog-duration">{}</td>
                        <td class="slowlog-command" title="{}">{}</td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>"#,
                    entry.id,
                    format_duration_us(entry.duration_us),
                    html_escape(&entry.command),
                    html_escape(&truncate(&entry.command, 60)),
                    entry.client_addr.as_deref().unwrap_or("-"),
                    entry.client_name.as_deref().unwrap_or("-")
                )
            })
            .collect()
    };

    let html = format!(
        r#"<div class="card">
            <div class="card-header">
                <h3>Slow Query Log</h3>
            </div>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Duration</th>
                            <th>Command</th>
                            <th>Client</th>
                            <th>Name</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>"#
    );

    html_response(html)
}

/// Handle GET /debug/partials/latency
pub async fn handle_partial_latency(state: &DebugState) -> Response<Full<Bytes>> {
    let data = state.get_latency().await;

    let content = if data.is_empty() {
        r#"<div class="card">
            <div class="card-header">
                <h3>Latency Monitoring</h3>
            </div>
            <p style="color: var(--text-secondary); text-align: center; padding: 2rem;">
                No latency data available. Enable latency monitoring with LATENCY DOCTOR.
            </p>
        </div>"#
            .to_string()
    } else {
        let bars: String = data
            .iter()
            .map(|d| {
                let max_latency = data.iter().map(|x| x.max_us).max().unwrap_or(1);
                let pct = (d.max_us as f64 / max_latency as f64 * 100.0).min(100.0);
                format!(
                    r#"<div class="bar-row">
                        <div class="bar-label">{}</div>
                        <div class="bar-track">
                            <div class="bar-fill" style="width: {}%"></div>
                        </div>
                        <div class="bar-value">{}</div>
                    </div>"#,
                    d.event,
                    pct,
                    format_duration_us(d.max_us)
                )
            })
            .collect();

        format!(
            r#"<div class="card">
                <div class="card-header">
                    <h3>Latency Monitoring</h3>
                </div>
                <div class="bar-chart">
                    {bars}
                </div>
            </div>"#
        )
    };

    html_response(content)
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Format a duration in seconds to human readable string.
fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        format!("{}d {}h", days, hours)
    }
}

/// Format duration in microseconds.
fn format_duration_us(us: u64) -> String {
    if us < 1000 {
        format!("{}us", us)
    } else if us < 1_000_000 {
        format!("{:.2}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

/// Format a large number with commas.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Truncate a string with ellipsis.
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Escape HTML special characters.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

// ============================================================================
// Bundle API Handlers
// ============================================================================

/// Bundle list response.
#[derive(Serialize)]
pub struct BundleListResponse {
    pub bundles: Vec<crate::bundle::BundleInfo>,
}

/// Handle GET /debug/api/bundle/list - list all stored bundles.
pub fn handle_api_bundle_list(state: &DebugState) -> Response<Full<Bytes>> {
    if !state.bundle_enabled() {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"Bundle support not enabled"}"#,
            )))
            .unwrap();
    }

    let bundles = state.list_bundles();
    let response = BundleListResponse { bundles };
    json_response(&response)
}

/// Handle GET /debug/api/bundle/generate - generate and download a bundle.
///
/// Query parameters:
/// - `duration`: Collection duration in seconds (default: 0 = instant snapshot)
pub async fn handle_api_bundle_generate(
    state: &DebugState,
    query: Option<&str>,
) -> Response<Full<Bytes>> {
    if !state.bundle_enabled() {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"Bundle support not enabled"}"#,
            )))
            .unwrap();
    }

    // Parse duration from query string
    let duration_secs = parse_query_param(query, "duration")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Generate bundle
    match state.generate_bundle_streaming(duration_secs).await {
        Ok((id, zip_data)) => {
            let filename = format!("frogdb-bundle-{}.zip", id);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/zip")
                .header(
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename),
                )
                .header("X-Bundle-Id", &id)
                .body(Full::new(Bytes::from(zip_data)))
                .unwrap()
        }
        Err(e) => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(format!(r#"{{"error":"{}"}}"#, e))))
            .unwrap(),
    }
}

/// Handle GET /debug/api/bundle/:id - download a stored bundle.
pub fn handle_api_bundle_download(state: &DebugState, id: &str) -> Response<Full<Bytes>> {
    if !state.bundle_enabled() {
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"Bundle support not enabled"}"#,
            )))
            .unwrap();
    }

    match state.get_bundle(id) {
        Some(zip_data) => {
            let filename = format!("frogdb-bundle-{}.zip", id);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/zip")
                .header(
                    "Content-Disposition",
                    format!("attachment; filename=\"{}\"", filename),
                )
                .body(Full::new(Bytes::from(zip_data)))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(format!(
                r#"{{"error":"Bundle '{}' not found"}}"#,
                id
            ))))
            .unwrap(),
    }
}

/// Parse a query parameter from a query string.
fn parse_query_param<'a>(query: Option<&'a str>, key: &str) -> Option<&'a str> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        if k == key { Some(v) } else { None }
    })
}

// ============================================================================
// Bundle Partial Handler
// ============================================================================

/// Handle GET /debug/partials/bundles - HTML partial for bundle list.
pub fn handle_partial_bundles(state: &DebugState) -> Response<Full<Bytes>> {
    if !state.bundle_enabled() {
        let html = r#"
            <div class="card">
                <div class="card-header">
                    <h3>Diagnostic Bundles</h3>
                </div>
                <div class="empty-state">
                    <p>Bundle support is not enabled on this server.</p>
                    <p class="text-secondary">Configure bundle support to generate diagnostic bundles.</p>
                </div>
            </div>
        "#;
        return html_response(html.to_string());
    }

    let bundles = state.list_bundles();

    let rows = if bundles.is_empty() {
        "<tr><td colspan=\"4\" class=\"empty-state\">No bundles generated yet</td></tr>".to_string()
    } else {
        bundles
            .iter()
            .map(|b| {
                format!(
                    r#"<tr>
                    <td><code>{}</code></td>
                    <td>{}</td>
                    <td>{}</td>
                    <td><a class="download-link" href="/debug/api/bundle/{}" download>Download</a></td>
                </tr>"#,
                    &b.id[..8.min(b.id.len())], // Show first 8 chars of ID
                    format_timestamp(b.created_at),
                    format_bytes(b.size_bytes),
                    b.id
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let html = format!(
        r#"
        <div class="card">
            <div class="card-header">
                <h3>Diagnostic Bundles</h3>
            </div>
            <div class="bundle-actions">
                <select id="bundle-duration">
                    <option value="0">Instant</option>
                    <option value="5">5 seconds</option>
                    <option value="10">10 seconds</option>
                    <option value="30">30 seconds</option>
                    <option value="60">60 seconds</option>
                </select>
                <button class="btn-primary" onclick="window.dispatchEvent(new CustomEvent('generate-bundle', {{detail: document.getElementById('bundle-duration').value}}))">
                    Generate Bundle
                </button>
            </div>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Bundle ID</th>
                            <th>Created</th>
                            <th>Size</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {}
                    </tbody>
                </table>
            </div>
        </div>
        "#,
        rows
    );

    html_response(html)
}

/// Format a Unix timestamp as a human-readable string.
fn format_timestamp(secs: u64) -> String {
    // Simple UTC timestamp formatting (no external deps)
    let remaining = secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;
    format!("{:02}:{:02}:{:02} UTC", hours, minutes, seconds)
}

/// Format bytes as human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;

    if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
