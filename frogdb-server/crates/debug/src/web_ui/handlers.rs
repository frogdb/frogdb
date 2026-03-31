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

/// Handle GET /debug/api/metrics — returns a rich metrics snapshot.
pub fn handle_api_metrics(
    _state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let snapshot = recorder.dashboard_snapshot();
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
// HTML Render Helpers
// ============================================================================

/// Render cluster status HTML fragment (no card wrapper).
fn render_cluster_html(state: &DebugState) -> String {
    let uptime = state.uptime_seconds();
    let uptime_str = format_duration(uptime);
    let role = state.role();
    let role_badge_class = if role == "primary" || role == "standalone" {
        "tag-success"
    } else {
        "tag-info"
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

    format!(
        r#"<div class="section-header">
            <h3>Server Status</h3>
            <span class="tag {role_badge_class}">{role}</span>
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
        </div>"#,
        state.server_info.num_shards, state.server_info.bind_addr, state.server_info.port
    )
}

/// Render metrics HTML fragment (no card wrapper).
fn render_metrics_html(state: &DebugState, recorder: &Arc<PrometheusRecorder>) -> String {
    let m = recorder.dashboard_snapshot();

    format!(
        r#"<div class="section-header">
            <h3>Current Metrics</h3>
        </div>
        <div class="stats-grid">
            <div class="stat-item">
                <div class="stat-label">Total Commands</div>
                <div class="stat-value highlight">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Connections</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Total Keys</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Memory Used</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Memory RSS</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Evicted Keys</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Hit Rate</div>
                <div class="stat-value">{}</div>
            </div>
            <div class="stat-item">
                <div class="stat-label">Shards</div>
                <div class="stat-value">{}</div>
            </div>
        </div>"#,
        format_number(m.commands_total as u64),
        m.connections_current as u64,
        format_number(m.keys_total as u64),
        format_bytes(m.memory_used_bytes as u64),
        format_bytes(m.memory_rss_bytes as u64),
        format_number(m.eviction_keys_total as u64),
        format_hit_rate(m.keyspace_hits_total, m.keyspace_misses_total),
        state.server_info.num_shards
    )
}

/// Render slowlog HTML fragment (no card wrapper).
async fn render_slowlog_html(state: &DebugState) -> String {
    let entries = state.get_slowlog(50).await;

    let rows: String = if entries.is_empty() {
        r#"<tr><td colspan="5" style="text-align: center; color: var(--text-light);">No slow queries recorded</td></tr>"#.to_string()
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

    format!(
        r#"<div class="section-header">
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
        </div>"#
    )
}

/// Render latency HTML fragment (no card wrapper).
async fn render_latency_html(state: &DebugState) -> String {
    let data = state.get_latency().await;

    if data.is_empty() {
        r#"<div class="section-header">
            <h3>Latency Monitoring</h3>
        </div>
        <p style="color: var(--text-light); text-align: center; padding: 2rem;">
            No latency data available. Enable latency monitoring with LATENCY DOCTOR.
        </p>"#
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
            r#"<div class="section-header">
                <h3>Latency Monitoring</h3>
            </div>
            <div class="bar-chart">
                {bars}
            </div>"#
        )
    }
}

// ============================================================================
// HTML Partial Handlers
// ============================================================================

/// Handle GET /debug/partials/cluster
pub fn handle_partial_cluster(state: &DebugState) -> Response<Full<Bytes>> {
    html_response(render_cluster_html(state))
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
                r#"<span class="tag tag-success">mutable</span>"#
            } else {
                r#"<span class="tag tag-info">immutable</span>"#
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
        r#"<div class="section-header">
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
        </div>"#
    );

    html_response(html)
}

/// Handle GET /debug/partials/metrics
pub fn handle_partial_metrics(
    state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    html_response(render_metrics_html(state, recorder))
}

/// Handle GET /debug/partials/slowlog
pub async fn handle_partial_slowlog(state: &DebugState) -> Response<Full<Bytes>> {
    html_response(render_slowlog_html(state).await)
}

/// Handle GET /debug/partials/latency
pub async fn handle_partial_latency(state: &DebugState) -> Response<Full<Bytes>> {
    html_response(render_latency_html(state).await)
}

/// Handle GET /debug/partials/overview - combined cluster + metrics + endpoints.
pub fn handle_partial_overview(
    state: &DebugState,
    recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let cluster = render_cluster_html(state);
    let metrics = render_metrics_html(state, recorder);
    let endpoints = render_endpoints_html();
    html_response(format!("{cluster}\n{metrics}\n{endpoints}"))
}

/// Handle GET /debug/partials/performance - combined latency + slowlog.
pub async fn handle_partial_performance(state: &DebugState) -> Response<Full<Bytes>> {
    let latency = render_latency_html(state).await;
    let slowlog = render_slowlog_html(state).await;
    html_response(format!("{latency}\n{slowlog}"))
}

// ============================================================================
// Client Handlers
// ============================================================================

/// Client list API response.
#[derive(Serialize)]
pub struct ClientListResponse {
    pub clients: Vec<super::state::ClientSnapshot>,
}

/// Handle GET /debug/api/clients — JSON list of connected clients.
pub fn handle_api_clients(state: &DebugState) -> Response<Full<Bytes>> {
    let clients = state.get_clients();
    json_response(&ClientListResponse { clients })
}

/// Handle GET /debug/partials/clients — HTML table of connected clients.
pub fn handle_partial_clients(state: &DebugState) -> Response<Full<Bytes>> {
    let clients = state.get_clients();

    let rows = if clients.is_empty() {
        r#"<tr><td colspan="9" style="text-align: center; color: var(--text-light);">No connected clients</td></tr>"#.to_string()
    } else {
        clients
            .iter()
            .map(|c| {
                let lib_info = if c.lib_name.is_empty() {
                    "-".to_string()
                } else if c.lib_ver.is_empty() {
                    html_escape(&c.lib_name)
                } else {
                    format!("{} <small>{}</small>", html_escape(&c.lib_name), html_escape(&c.lib_ver))
                };

                format!(
                    r#"<tr>
                        <td><code>{}</code></td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td>{}</td>
                        <td><code>{}</code></td>
                        <td>{}</td>
                        <td>{}</td>
                    </tr>"#,
                    c.id,
                    html_escape(&c.addr),
                    if c.name.is_empty() { "-" } else { &c.name },
                    lib_info,
                    format_duration(c.age_secs),
                    format_duration(c.idle_secs),
                    c.flags,
                    format_number(c.commands_total),
                    format_bytes(c.bytes_recv + c.bytes_sent),
                )
            })
            .collect()
    };

    let html = format!(
        r#"<div class="section-header">
            <h3>Connected Clients</h3>
            <span class="tag tag-info">{} connected</span>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Address</th>
                        <th>Name</th>
                        <th>Library</th>
                        <th>Age</th>
                        <th>Idle</th>
                        <th>Flags</th>
                        <th>Commands</th>
                        <th>Traffic</th>
                    </tr>
                </thead>
                <tbody>
                    {rows}
                </tbody>
            </table>
        </div>"#,
        clients.len()
    );

    html_response(html)
}

// ============================================================================
// Metrics Charts Handler
// ============================================================================

/// Handle GET /debug/partials/metrics-charts — chart containers with init JS.
pub fn handle_partial_metrics_charts(
    _state: &DebugState,
    _recorder: &Arc<PrometheusRecorder>,
) -> Response<Full<Bytes>> {
    let html = r#"<div class="section-header">
            <h3>Metrics Charts</h3>
            <small style="color: var(--text-light);">Data accumulates over time (10 min window)</small>
        </div>
        <div class="chart-grid">
            <div class="chart-container" id="chart-commands">
                <div class="chart-title">Commands/s</div>
                <div class="chart-area" id="chart-commands-area"></div>
            </div>
            <div class="chart-container" id="chart-memory">
                <div class="chart-title">Memory</div>
                <div class="chart-area" id="chart-memory-area"></div>
            </div>
            <div class="chart-container" id="chart-connections">
                <div class="chart-title">Connections</div>
                <div class="chart-area" id="chart-connections-area"></div>
            </div>
            <div class="chart-container" id="chart-keys">
                <div class="chart-title">Keyspace</div>
                <div class="chart-area" id="chart-keys-area"></div>
            </div>
            <div class="chart-container" id="chart-network">
                <div class="chart-title">Network I/O</div>
                <div class="chart-area" id="chart-network-area"></div>
            </div>
            <div class="chart-container" id="chart-evictions">
                <div class="chart-title">Evictions/s</div>
                <div class="chart-area" id="chart-evictions-area"></div>
            </div>
        </div>"#;

    html_response(html.to_string())
}

// ============================================================================
// Endpoints Render Helper
// ============================================================================

/// Render the external endpoints links section.
fn render_endpoints_html() -> String {
    r#"<div class="section-header">
            <h3>Endpoints</h3>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Path</th>
                        <th>Description</th>
                        <th>Link</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><code>/metrics</code></td>
                        <td>Prometheus metrics (text format)</td>
                        <td><a href="/metrics" target="_blank">Open</a></td>
                    </tr>
                    <tr>
                        <td><code>/status/json</code></td>
                        <td>Server status snapshot (JSON)</td>
                        <td><a href="/status/json" target="_blank">Open</a></td>
                    </tr>
                    <tr>
                        <td><code>/health/ready</code></td>
                        <td>Readiness probe</td>
                        <td><a href="/health/ready" target="_blank">Open</a></td>
                    </tr>
                    <tr>
                        <td><code>/health/live</code></td>
                        <td>Liveness probe</td>
                        <td><a href="/health/live" target="_blank">Open</a></td>
                    </tr>
                </tbody>
            </table>
        </div>"#
        .to_string()
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

/// Format a hit rate as percentage.
fn format_hit_rate(hits: f64, misses: f64) -> String {
    let total = hits + misses;
    if total == 0.0 {
        "-".to_string()
    } else {
        format!("{:.1}%", hits / total * 100.0)
    }
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
            <div class="section-header">
                <h3>Diagnostic Bundles</h3>
            </div>
            <p style="text-align: center; color: var(--text-light);">
                Bundle support is not enabled on this server.<br>
                Configure bundle support to generate diagnostic bundles.
            </p>
        "#;
        return html_response(html.to_string());
    }

    let bundles = state.list_bundles();

    let rows = if bundles.is_empty() {
        "<tr><td colspan=\"4\" style=\"text-align: center; color: var(--text-light);\">No bundles generated yet</td></tr>".to_string()
    } else {
        bundles
            .iter()
            .map(|b| {
                format!(
                    r#"<tr>
                    <td><code>{}</code></td>
                    <td>{}</td>
                    <td>{}</td>
                    <td><a href="/debug/api/bundle/{}" download>Download</a></td>
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
        <div class="section-header">
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
            <button onclick="window.dispatchEvent(new CustomEvent('generate-bundle', {{detail: document.getElementById('bundle-duration').value}}))">
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
