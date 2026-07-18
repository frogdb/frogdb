//! Health checking for FrogDB pods.

use anyhow::Result;
use tracing::debug;

/// Check if a FrogDB pod is healthy by querying its health endpoint.
#[allow(dead_code)]
pub async fn check_pod_health(pod_ip: &str, metrics_port: u16) -> Result<bool> {
    let url = format!("http://{}:{}/health/ready", pod_ip, metrics_port);
    debug!(%url, "Checking pod health");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    match client.get(&url).send().await {
        Ok(resp) => Ok(resp.status().is_success()),
        Err(_) => Ok(false),
    }
}

/// Probe a pod's replication role via the FrogDB admin endpoint on the metrics port.
///
/// Queries `GET /admin/role`, which reports `{"role": "master" | "slave", ...}`.
/// Returns `Some(true)` when the pod reports itself the Primary (`role: master`),
/// `Some(false)` for a replica role, and `None` when the role could not be
/// determined — endpoint unreachable, non-success status, or an unparseable body.
/// Callers treat `None` as "unknown" and must not fail reconciliation on it.
pub async fn probe_pod_is_primary(pod_ip: &str, metrics_port: u16) -> Option<bool> {
    let url = format!("http://{}:{}/admin/role", pod_ip, metrics_port);
    debug!(%url, "Probing pod replication role");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .ok()?;

    let resp = client.get(&url).send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }

    let body: serde_json::Value = resp.json().await.ok()?;
    match body.get("role").and_then(|r| r.as_str()) {
        Some("master") => Some(true),
        Some(_) => Some(false),
        None => None,
    }
}
