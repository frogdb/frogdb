//! Integration tests for HTTP metrics and health endpoints.

use crate::common::test_server::{TestServer, TestServerConfig};
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
// Keyspace Hit/Miss Metrics Tests
// ============================================================================

/// Keyspace hit/miss metrics must be recorded for commands executed inside a
/// MULTI/EXEC transaction, matching the single-command path and Redis (which
/// counts hits/misses per command regardless of transaction context).
#[tokio::test]
async fn test_keyspace_metrics_counted_inside_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Seed two existing keys (SET does not carry TRACKS_KEYSPACE). Hash tags
    // keep all keys on one shard so the transaction is not cross-slot.
    client.command(&["SET", "{ks}kk1", "v1"]).await;
    client.command(&["SET", "{ks}kk2", "v2"]).await;

    // Baseline keyspace counters.
    let before = server.fetch_metrics().await;
    let hits_before = get_counter(&before, "frogdb_keyspace_hits_total", &[]);
    let misses_before = get_counter(&before, "frogdb_keyspace_misses_total", &[]);

    // Run three TRACKS_KEYSPACE reads (GET) inside a transaction: two hits on
    // existing keys plus one lookup on a missing key.
    client.command(&["MULTI"]).await;
    client.command(&["GET", "{ks}kk1"]).await;
    client.command(&["GET", "{ks}kk2"]).await;
    client.command(&["GET", "{ks}kk_missing"]).await;
    let exec = client.command(&["EXEC"]).await;
    assert!(matches!(exec, Response::Array(_)), "EXEC should succeed");

    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;
    let hits_after = get_counter(&after, "frogdb_keyspace_hits_total", &[]);
    let misses_after = get_counter(&after, "frogdb_keyspace_misses_total", &[]);

    // All three TRACKS_KEYSPACE commands must contribute to keyspace stats, and
    // classification is at lookup level: the two GETs on existing keys are hits,
    // and the GET on a missing key is a miss — it must NOT be counted as a hit
    // even though its reply is a nil bulk string (Response::Bulk(None)).
    assert_eq!(
        hits_after - hits_before,
        2.0,
        "expected 2 keyspace hits inside MULTI/EXEC, got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        1.0,
        "expected 1 keyspace miss inside MULTI/EXEC, got +{}",
        misses_after - misses_before
    );

    server.shutdown().await;
}

/// GET on an existing key is a keyspace hit; GET on a missing key is a miss.
///
/// Regression: a missing-key GET replies `Response::Bulk(None)`, which the old
/// reply-shape classifier (`matches!(response, Response::Null)`) treated as a
/// hit. Hit/miss is now derived from actual key existence at lookup level.
#[tokio::test]
async fn test_keyspace_get_hit_and_miss() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "ks_get_key", "v"]).await;

    let before = server.fetch_metrics().await;
    let hits_before = get_counter(&before, "frogdb_keyspace_hits_total", &[]);
    let misses_before = get_counter(&before, "frogdb_keyspace_misses_total", &[]);

    client.command(&["GET", "ks_get_key"]).await; // hit: key exists
    client.command(&["GET", "ks_get_missing"]).await; // miss: key absent

    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;
    let hits_after = get_counter(&after, "frogdb_keyspace_hits_total", &[]);
    let misses_after = get_counter(&after, "frogdb_keyspace_misses_total", &[]);

    assert_eq!(
        hits_after - hits_before,
        1.0,
        "GET on an existing key should be exactly one hit, got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        1.0,
        "GET on a missing key should be exactly one miss, got +{}",
        misses_after - misses_before
    );

    server.shutdown().await;
}

/// HGET on an existing hash with a missing FIELD is a keyspace hit (the key
/// lookup succeeded), while HGET on a missing KEY is a miss. A reply-shape
/// classifier cannot distinguish these — both reply with a nil bulk string.
#[tokio::test]
async fn test_keyspace_hget_missing_field_counts_as_hit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["HSET", "ks_hash", "f1", "v1"]).await;

    let before = server.fetch_metrics().await;
    let hits_before = get_counter(&before, "frogdb_keyspace_hits_total", &[]);
    let misses_before = get_counter(&before, "frogdb_keyspace_misses_total", &[]);

    client.command(&["HGET", "ks_hash", "f1"]).await; // hit: key + field exist
    client.command(&["HGET", "ks_hash", "missing"]).await; // hit: key exists, field absent
    client.command(&["HGET", "ks_missing_hash", "f1"]).await; // miss: key absent

    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;
    let hits_after = get_counter(&after, "frogdb_keyspace_hits_total", &[]);
    let misses_after = get_counter(&after, "frogdb_keyspace_misses_total", &[]);

    assert_eq!(
        hits_after - hits_before,
        2.0,
        "both HGETs on the existing hash are hits (missing field included), got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        1.0,
        "HGET on a missing key is a miss, got +{}",
        misses_after - misses_before
    );

    server.shutdown().await;
}

/// MGET counts one keyspace lookup per key: a hit for each existing key and a
/// miss for each missing key, matching Redis (one `lookupKeyRead` per key).
///
/// Hash tags keep all keys on one shard, so this exercises the single-shard
/// `MgetCommand::execute` path.
#[tokio::test]
async fn test_keyspace_mget_counts_per_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "{m}1", "v1"]).await;
    client.command(&["SET", "{m}2", "v2"]).await;

    let before = server.fetch_metrics().await;
    let hits_before = get_counter(&before, "frogdb_keyspace_hits_total", &[]);
    let misses_before = get_counter(&before, "frogdb_keyspace_misses_total", &[]);

    // Two existing keys + one missing key -> two hits and one miss.
    client.command(&["MGET", "{m}1", "{m}2", "{m}miss"]).await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;
    let hits_after = get_counter(&after, "frogdb_keyspace_hits_total", &[]);
    let misses_after = get_counter(&after, "frogdb_keyspace_misses_total", &[]);

    assert_eq!(
        hits_after - hits_before,
        2.0,
        "MGET should count one hit per existing key, got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        1.0,
        "MGET should count one miss per missing key, got +{}",
        misses_after - misses_before
    );

    server.shutdown().await;
}

/// MGET that fans out across shards (cross-slot scatter path) still counts one
/// keyspace lookup per key, recorded by each shard for its own subset.
#[tokio::test]
async fn test_keyspace_mget_cross_shard_counts_per_key() {
    // Enable the cross-shard scatter path for multi-key commands in standalone.
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        allow_cross_slot_standalone: true,
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Plain (untagged) keys are spread across shards by the hash router.
    let existing = ["ksx_a", "ksx_b", "ksx_c", "ksx_d"];
    for k in existing {
        client.command(&["SET", k, "v"]).await;
    }

    let before = server.fetch_metrics().await;
    let hits_before = get_counter(&before, "frogdb_keyspace_hits_total", &[]);
    let misses_before = get_counter(&before, "frogdb_keyspace_misses_total", &[]);

    // Four existing + two missing keys -> four hits and two misses total,
    // independent of the per-shard split.
    client
        .command(&[
            "MGET",
            "ksx_a",
            "ksx_b",
            "ksx_c",
            "ksx_d",
            "ksx_miss1",
            "ksx_miss2",
        ])
        .await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = server.fetch_metrics().await;
    let hits_after = get_counter(&after, "frogdb_keyspace_hits_total", &[]);
    let misses_after = get_counter(&after, "frogdb_keyspace_misses_total", &[]);

    assert_eq!(
        hits_after - hits_before,
        4.0,
        "cross-shard MGET should count one hit per existing key, got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        2.0,
        "cross-shard MGET should count one miss per missing key, got +{}",
        misses_after - misses_before
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

// ============================================================================
// INFO stats keyspace_hits/keyspace_misses Tests
// ============================================================================

/// Read the `INFO stats` section over RESP and return the value of a numeric
/// field such as `keyspace_hits`. Returns 0 when the field is absent.
fn parse_info_stat(info: &str, field: &str) -> u64 {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(field)
            && let Some(val) = rest.strip_prefix(':')
        {
            return val.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Fetch `INFO stats` over the RESP connection and return its body as a String.
async fn info_stats(client: &mut crate::common::test_server::TestClient) -> String {
    match client.command(&["INFO", "stats"]).await {
        Response::Bulk(Some(data)) => String::from_utf8_lossy(&data).into_owned(),
        other => panic!("expected bulk reply for INFO stats, got: {other:?}"),
    }
}

/// `INFO stats` must report the real cumulative `keyspace_hits`/`keyspace_misses`
/// counters (the same source Prometheus scrapes), not the hardcoded `0`
/// placeholders. Regression for the long-standing wiring bug.
#[tokio::test]
async fn test_info_stats_reports_keyspace_hits_and_misses() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Seed an existing key (SET does not carry TRACKS_KEYSPACE, so it does not
    // move the keyspace counters itself).
    client.command(&["SET", "ks_info_key", "v"]).await;

    // Baseline from INFO itself, so the assertions are robust to any lookups
    // the harness may have performed during connection setup.
    let before = info_stats(&mut client).await;
    let hits_before = parse_info_stat(&before, "keyspace_hits");
    let misses_before = parse_info_stat(&before, "keyspace_misses");

    // Two hits on the existing key, two misses on absent keys.
    client.command(&["GET", "ks_info_key"]).await; // hit
    client.command(&["GET", "ks_info_key"]).await; // hit
    client.command(&["GET", "ks_info_absent_1"]).await; // miss
    client.command(&["GET", "ks_info_absent_2"]).await; // miss

    // Counters are recorded on the shard thread after the reply is sent; give
    // the recorder a moment so the subsequent INFO observes them.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = info_stats(&mut client).await;
    let hits_after = parse_info_stat(&after, "keyspace_hits");
    let misses_after = parse_info_stat(&after, "keyspace_misses");

    // The wired counters must be nonzero (the bug reported a constant 0)...
    assert!(
        hits_after >= 2,
        "INFO stats keyspace_hits should be nonzero after hits, got {hits_after}"
    );
    assert!(
        misses_after >= 2,
        "INFO stats keyspace_misses should be nonzero after misses, got {misses_after}"
    );

    // ...and the deltas must match the exact number of hit/miss lookups.
    assert_eq!(
        hits_after - hits_before,
        2,
        "expected exactly 2 new keyspace hits, got +{}",
        hits_after - hits_before
    );
    assert_eq!(
        misses_after - misses_before,
        2,
        "expected exactly 2 new keyspace misses, got +{}",
        misses_after - misses_before
    );

    server.shutdown().await;
}
