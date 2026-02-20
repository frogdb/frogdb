//! Integration tests for HTTP metrics and health endpoints.

mod common;

use common::test_server::TestServer;
use std::time::Duration;

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus_format() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/metrics", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let content_type = response.headers().get("content-type").unwrap();
    assert!(content_type.to_str().unwrap().contains("text/plain"));

    let body = response.text().await.unwrap();
    // Metrics endpoint should return valid content (may be empty or have metrics)
    // Prometheus format uses # for comments/metadata
    assert!(body.is_empty() || body.contains("frogdb_") || body.starts_with("#"));

    server.shutdown().await;
}

#[tokio::test]
async fn test_metrics_include_command_counters() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Execute commands to generate metrics
    client.command(&["SET", "key1", "value1"]).await;
    client.command(&["GET", "key1"]).await;
    client.command(&["GET", "nonexistent"]).await;

    // Give the server a moment to record metrics
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fetch metrics
    let http_client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = http_client
        .get(format!("http://{}/metrics", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    let body = response.text().await.unwrap();
    // Should contain command-related metrics
    assert!(
        body.contains("frogdb_commands_total") || body.contains("commands"),
        "Metrics should contain command counters: {}",
        body
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_health_live_endpoint() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/health/live", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_health_ready_endpoint() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/health/ready", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_healthz_alias_endpoint() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/healthz", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_readyz_alias_endpoint() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/readyz", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "ok");

    server.shutdown().await;
}

#[tokio::test]
async fn test_metrics_404_on_unknown_path() {
    let server = TestServer::start_standalone().await;

    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let response = client
        .get(format!("http://{}/unknown", server.metrics_addr()))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);

    server.shutdown().await;
}
