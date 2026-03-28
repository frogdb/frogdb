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
