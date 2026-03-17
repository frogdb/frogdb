use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Metadata about a stored bundle (mirrors the server's `BundleInfo`).
#[derive(Debug, Clone, Deserialize)]
pub struct BundleInfo {
    pub id: String,
    pub created_at: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Deserialize)]
pub struct BundleListResponse {
    pub bundles: Vec<BundleInfo>,
}

/// HTTP client for the FrogDB diagnostic bundle API.
pub struct BundleClient {
    base_url: String,
    client: reqwest::Client,
}

impl BundleClient {
    pub fn new(host: &str, port: u16) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self {
            base_url: format!("http://{}:{}", host, port),
            client,
        })
    }

    /// List all stored bundles on the server.
    pub async fn list_bundles(&self) -> Result<Vec<BundleInfo>> {
        let url = format!("{}/debug/api/bundle/list", self.base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| classify_request_error(&e, &self.base_url))?;

        match resp.status().as_u16() {
            200 => {
                let body: BundleListResponse = resp
                    .json()
                    .await
                    .context("Failed to parse bundle list response")?;
                Ok(body.bundles)
            }
            503 => bail!("Bundle support not enabled on the server."),
            status => {
                let body = resp.text().await.unwrap_or_default();
                bail!("Server error (HTTP {}): {}", status, body);
            }
        }
    }

    /// Generate a new bundle and return `(bundle_id, zip_bytes)`.
    pub async fn generate_bundle(&self, duration_secs: u64) -> Result<(String, Vec<u8>)> {
        let url = format!(
            "{}/debug/api/bundle/generate?duration={}",
            self.base_url, duration_secs
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| classify_request_error(&e, &self.base_url))?;

        match resp.status().as_u16() {
            200 => {
                let id = resp
                    .headers()
                    .get("X-Bundle-Id")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from)
                    .unwrap_or_else(|| "unknown".into());
                let bytes = resp.bytes().await.context("Failed to read bundle data")?;
                Ok((id, bytes.to_vec()))
            }
            503 => bail!("Bundle support not enabled on the server."),
            status => {
                let body = resp.text().await.unwrap_or_default();
                bail!("Server error (HTTP {}): {}", status, body);
            }
        }
    }

    /// Download a previously stored bundle by ID.
    pub async fn download_bundle(&self, id: &str) -> Result<Vec<u8>> {
        let url = format!("{}/debug/api/bundle/{}", self.base_url, id);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| classify_request_error(&e, &self.base_url))?;

        match resp.status().as_u16() {
            200 => {
                let bytes = resp.bytes().await.context("Failed to read bundle data")?;
                Ok(bytes.to_vec())
            }
            404 => bail!("Bundle '{}' not found.", id),
            503 => bail!("Bundle support not enabled on the server."),
            status => {
                let body = resp.text().await.unwrap_or_default();
                bail!("Server error (HTTP {}): {}", status, body);
            }
        }
    }
}

/// Classify a reqwest error into a user-friendly message.
fn classify_request_error(err: &reqwest::Error, base_url: &str) -> anyhow::Error {
    if err.is_connect() {
        anyhow::anyhow!(
            "Could not connect to FrogDB at {}. Is the server running?",
            base_url
        )
    } else if err.is_timeout() {
        anyhow::anyhow!("Request timed out.")
    } else {
        anyhow::anyhow!("HTTP request failed: {}", err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_bundle_info() {
        let json = r#"{"id":"abc-123","created_at":1773855002,"size_bytes":12800}"#;
        let info: BundleInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.id, "abc-123");
        assert_eq!(info.created_at, 1_773_855_002);
        assert_eq!(info.size_bytes, 12_800);
    }

    #[test]
    fn test_deserialize_bundle_list_response() {
        let json = r#"{
            "bundles": [
                {"id":"a","created_at":100,"size_bytes":200},
                {"id":"b","created_at":300,"size_bytes":400}
            ]
        }"#;
        let resp: BundleListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.bundles.len(), 2);
        assert_eq!(resp.bundles[0].id, "a");
        assert_eq!(resp.bundles[1].id, "b");
    }

    #[test]
    fn test_deserialize_empty_bundle_list() {
        let json = r#"{"bundles":[]}"#;
        let resp: BundleListResponse = serde_json::from_str(json).unwrap();
        assert!(resp.bundles.is_empty());
    }
}
