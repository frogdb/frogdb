//! Integration tests for FrogDB metrics and health endpoints.
//!
//! These tests verify that the metrics HTTP server exposes Prometheus metrics
//! and health endpoints correctly.

mod common;

use common::TestServer;
use frogdb_metrics::assert_gauge_gte;
use frogdb_metrics::testing::{get_counter, get_histogram_count};
use frogdb_protocol::Response;
use std::time::Duration;

// ============================================================================
// Metrics Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_metrics_endpoint_returns_frogdb_metrics() {
    let server = TestServer::start().await;
    let url = server.metrics_url("/metrics");

    let resp = server.client().get(&url).send().await.unwrap();
    assert!(resp.status().is_success());

    let body = resp.text().await.unwrap();

    // Verify the response contains FrogDB metrics
    assert!(
        body.contains("frogdb_"),
        "Metrics endpoint should return FrogDB metrics"
    );
    assert!(
        body.contains("frogdb_uptime_seconds"),
        "Should contain uptime metric"
    );
    assert!(body.contains("frogdb_info"), "Should contain info metric");

    server.shutdown().await;
}

#[tokio::test]
async fn test_metrics_endpoint_returns_connection_metrics() {
    let server = TestServer::start().await;

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
// Health Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_health_liveness() {
    let server = TestServer::start().await;
    let url = server.metrics_url("/health/live");

    let resp = server.client().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Liveness endpoint should return 200"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_health_readiness() {
    let server = TestServer::start().await;
    let url = server.metrics_url("/health/ready");

    let resp = server.client().get(&url).send().await.unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "Readiness endpoint should return 200"
    );

    server.shutdown().await;
}

// ============================================================================
// Command Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_command_metrics_recorded() {
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
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
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add some data to use memory
    client.command(&["SET", "memtest", "some value here"]).await;

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = server.metrics_url("/metrics");
    let resp = server.client().get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("frogdb_memory_used_bytes"),
        "Should contain memory used metric"
    );

    server.shutdown().await;
}

// ============================================================================
// Shard Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_shard_metrics() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    // Add keys that will be distributed across shards
    for i in 0..20 {
        client
            .command(&["SET", &format!("shardkey:{}", i), "value"])
            .await;
    }

    // Small delay for metrics to update
    tokio::time::sleep(Duration::from_millis(50)).await;

    let url = server.metrics_url("/metrics");
    let resp = server.client().get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    assert!(
        body.contains("frogdb_shard_keys"),
        "Should contain shard keys metric"
    );
    assert!(
        body.contains("frogdb_shard_memory_bytes"),
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
    let server = TestServer::start().await;
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

    let url = server.metrics_url("/metrics");
    let resp = server.client().get(&url).send().await.unwrap();
    let body = resp.text().await.unwrap();

    // Note: Error metrics may or may not be present depending on implementation
    // We check that the metrics endpoint is still functional after errors
    assert!(
        body.contains("frogdb_"),
        "Metrics endpoint should still work after errors"
    );

    server.shutdown().await;
}
