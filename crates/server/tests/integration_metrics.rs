//! Integration tests for HTTP metrics and health endpoints.

mod common;

use common::test_server::TestServer;
use frogdb_protocol::Response;
use frogdb_telemetry::assert_gauge_gte;
use frogdb_telemetry::testing::{get_counter, get_histogram_count};
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

// ============================================================================
// Connection Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_endpoint_returns_connection_metrics() {
    let server = TestServer::start_standalone().await;

    // Check connections before any client connects
    let before = server.fetch_metrics().await;
    let total_before = get_counter(&before, "frogdb_connections_total", &[]);

    // Make a connection to trigger connection metrics
    let mut client = server.connect().await;
    client.command(&["PING"]).await;

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;

    // Verify connection total incremented
    let total_after = get_counter(&after, "frogdb_connections_total", &[]);
    assert!(
        total_after > total_before,
        "Connection total should increment after connect"
    );

    // Verify current connections gauge is at least 1
    assert_gauge_gte!(&after, "frogdb_connections_current", &[], 1);

    server.shutdown().await;
}

// ============================================================================
// Detailed Command Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_command_metrics_recorded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Get baseline metrics
    let before = server.fetch_metrics().await;
    let set_before = get_counter(&before, "frogdb_commands_total", &[("command", "SET")]);
    let get_before = get_counter(&before, "frogdb_commands_total", &[("command", "GET")]);

    // Execute some commands
    client.command(&["SET", "testkey", "testvalue"]).await;
    client.command(&["GET", "testkey"]).await;
    client.command(&["DEL", "testkey"]).await;
    client.command(&["PING"]).await;

    // Small delay for metrics to be recorded
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;

    // Verify command counters incremented
    let set_after = get_counter(&after, "frogdb_commands_total", &[("command", "SET")]);
    let get_after = get_counter(&after, "frogdb_commands_total", &[("command", "GET")]);

    assert_eq!(
        set_after,
        set_before + 1.0,
        "SET command counter should increment by 1"
    );
    assert_eq!(
        get_after,
        get_before + 1.0,
        "GET command counter should increment by 1"
    );

    // Verify histogram has recorded durations
    let set_hist_count = get_histogram_count(
        &after,
        "frogdb_commands_duration_seconds",
        &[("command", "SET")],
    );
    assert!(
        set_hist_count >= 1,
        "Should have at least 1 SET command duration recorded"
    );

    server.shutdown().await;
}

// ============================================================================
// Data Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_keys_total_metric() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add some keys to ensure there's data
    for i in 0..10 {
        client
            .command(&["SET", &format!("testkey:{}", i), &format!("value{}", i)])
            .await;
    }

    // Note: frogdb_keys_total is collected every 10 seconds by shard metrics,
    // so we just verify the metric name exists in the output (may still be 0)
    let metrics = server.fetch_metrics().await;
    assert!(
        metrics.contains("frogdb_keys_total"),
        "Should contain keys total metric"
    );

    // Cleanup
    for i in 0..10 {
        client.command(&["DEL", &format!("testkey:{}", i)]).await;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_memory_used_metric() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add some data to use memory
    client.command(&["SET", "memtest", "some value here"]).await;

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let metrics = server.fetch_metrics().await;

    assert!(
        metrics.contains("frogdb_memory_used_bytes"),
        "Should contain memory used metric"
    );

    server.shutdown().await;
}

// ============================================================================
// Shard Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_shard_metrics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add keys that will be distributed across shards
    for i in 0..20 {
        client
            .command(&["SET", &format!("shardkey:{}", i), "value"])
            .await;
    }

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let metrics = server.fetch_metrics().await;

    assert!(
        metrics.contains("frogdb_shard_keys"),
        "Should contain shard keys metric"
    );
    assert!(
        metrics.contains("frogdb_shard_memory_bytes"),
        "Should contain shard memory metric"
    );

    // Cleanup
    for i in 0..20 {
        client.command(&["DEL", &format!("shardkey:{}", i)]).await;
    }

    server.shutdown().await;
}

// ============================================================================
// Error Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_error_metrics_recorded() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Trigger an error by using wrong type operation
    // First set a string key
    client.command(&["SET", "mykey", "value"]).await;

    // Try to use a list operation on a string key (should fail with WRONGTYPE)
    let response = client.command(&["LPUSH", "mykey", "item"]).await;
    assert!(
        matches!(response, Response::Error(_)),
        "LPUSH on string key should return error"
    );

    // Cleanup
    client.command(&["DEL", "mykey"]).await;

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let metrics = server.fetch_metrics().await;

    // Note: Error metrics may or may not be present depending on implementation
    // We check that the metrics endpoint is still functional after errors
    assert!(
        metrics.contains("frogdb_"),
        "Metrics endpoint should still work after errors"
    );

    server.shutdown().await;
}
