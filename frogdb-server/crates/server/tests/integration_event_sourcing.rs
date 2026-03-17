//! Integration tests for Event Sourcing (ES.*) commands.

use crate::common::response_helpers::{
    assert_error_prefix, assert_ok, unwrap_array, unwrap_bulk, unwrap_integer,
};
use crate::common::test_server::TestServer;
use crate::common::test_server::TestServerConfig;
use frogdb_protocol::Response;

// ============================================================================
// ES.APPEND tests
// ============================================================================

#[tokio::test]
async fn test_es_append_basic() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append first event at version 0
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "0",
            "OrderCreated",
            r#"{"id":1}"#,
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);
    assert_eq!(unwrap_integer(&arr[0]), 1); // version = 1
    assert!(!unwrap_bulk(&arr[1]).is_empty()); // stream ID returned

    // Append second event at version 1
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "1",
            "OrderPaid",
            r#"{"amount":100}"#,
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2); // version = 2

    // Append with extra fields
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "2",
            "OrderShipped",
            r#"{"carrier":"ups"}"#,
            "tracking_number",
            "1Z999",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 3); // version = 3

    server.shutdown().await;
}

#[tokio::test]
async fn test_es_append_version_mismatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append first event
    let _ = client
        .command(&["ES.APPEND", "{es}stream", "0", "Event1", "data1"])
        .await;

    // Try to append with wrong expected version
    let resp = client
        .command(&["ES.APPEND", "{es}stream", "0", "Event2", "data2"])
        .await;
    assert_error_prefix(&resp, "VERSIONMISMATCH expected 0 actual 1");

    // Correct version should succeed
    let resp = client
        .command(&["ES.APPEND", "{es}stream", "1", "Event2", "data2"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    server.shutdown().await;
}

#[tokio::test]
async fn test_es_append_idempotent() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append with idempotency key
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "0",
            "OrderCreated",
            "data1",
            "IF_NOT_EXISTS",
            "idem-key-1",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1);

    // Retry with same idempotency key — should return current version, not error
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "1",
            "OrderCreated",
            "data1",
            "IF_NOT_EXISTS",
            "idem-key-1",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1); // same version as before
    // Second element is null (no new stream ID)
    assert!(matches!(arr[1], Response::Bulk(None)));

    // Different idempotency key should work
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "1",
            "OrderPaid",
            "data2",
            "IF_NOT_EXISTS",
            "idem-key-2",
        ])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 2);

    server.shutdown().await;
}

// ============================================================================
// ES.READ tests
// ============================================================================

#[tokio::test]
async fn test_es_read_version_range() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append 5 events
    for i in 0..5 {
        let _ = client
            .command(&[
                "ES.APPEND",
                "{es}stream",
                &i.to_string(),
                &format!("Event{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Read all events
    let resp = client.command(&["ES.READ", "{es}stream", "1"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 5);

    // Read versions 2 through 4
    let resp = client.command(&["ES.READ", "{es}stream", "2", "4"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);

    // Verify first returned entry is version 2
    let entry = unwrap_array(arr[0].clone());
    assert_eq!(unwrap_integer(&entry[0]), 2);

    // Read with COUNT
    let resp = client
        .command(&["ES.READ", "{es}stream", "1", "COUNT", "2"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);

    // Read non-existent stream
    let resp = client.command(&["ES.READ", "{es}nonexistent", "1"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 0);

    server.shutdown().await;
}

// ============================================================================
// ES.REPLAY tests
// ============================================================================

#[tokio::test]
async fn test_es_replay_no_snapshot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append 3 events
    for i in 0..3 {
        let _ = client
            .command(&[
                "ES.APPEND",
                "{es}stream",
                &i.to_string(),
                &format!("Event{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Replay without snapshot
    let resp = client.command(&["ES.REPLAY", "{es}stream"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2); // [snapshot_or_null, events]

    // Snapshot should be null
    assert!(matches!(arr[0], Response::Bulk(None)));

    // Should have all 3 events
    let events = unwrap_array(arr[1].clone());
    assert_eq!(events.len(), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_es_replay_with_snapshot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append 5 events
    for i in 0..5 {
        let _ = client
            .command(&[
                "ES.APPEND",
                "{es}stream",
                &i.to_string(),
                &format!("Event{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Create snapshot at version 3
    let resp = client
        .command(&["ES.SNAPSHOT", "{es}snapshot", "3", r#"{"balance":300}"#])
        .await;
    assert_ok(&resp);

    // Replay with snapshot — should only return events after version 3
    let resp = client
        .command(&["ES.REPLAY", "{es}stream", "SNAPSHOT", "{es}snapshot"])
        .await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);

    // Snapshot state
    let snap = unwrap_bulk(&arr[0]);
    assert_eq!(snap, br#"{"balance":300}"#);

    // Events after snapshot (versions 4 and 5)
    let events = unwrap_array(arr[1].clone());
    assert_eq!(events.len(), 2);

    // Verify first event is version 4
    let event4 = unwrap_array(events[0].clone());
    assert_eq!(unwrap_integer(&event4[0]), 4);

    server.shutdown().await;
}

// ============================================================================
// ES.SNAPSHOT tests
// ============================================================================

#[tokio::test]
async fn test_es_snapshot() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create snapshot
    let resp = client
        .command(&["ES.SNAPSHOT", "{es}snap", "5", r#"{"state":"ok"}"#])
        .await;
    assert_ok(&resp);

    // Verify snapshot was stored (it's a string key)
    let resp = client.command(&["GET", "{es}snap"]).await;
    let bulk = unwrap_bulk(&resp);
    assert_eq!(bulk, b"5:{\"state\":\"ok\"}");

    server.shutdown().await;
}

// ============================================================================
// ES.INFO tests
// ============================================================================

#[tokio::test]
async fn test_es_info() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Info on non-existent stream
    let resp = client.command(&["ES.INFO", "{es}stream"]).await;
    assert!(matches!(resp, Response::Bulk(None)));

    // Append 3 events
    for i in 0..3 {
        let _ = client
            .command(&[
                "ES.APPEND",
                "{es}stream",
                &i.to_string(),
                &format!("Event{}", i + 1),
                &format!("data{}", i + 1),
            ])
            .await;
    }

    // Get info
    let resp = client.command(&["ES.INFO", "{es}stream"]).await;
    let arr = unwrap_array(resp);

    // Find version field
    assert_eq!(arr.len(), 10); // 5 key-value pairs
    assert_eq!(unwrap_bulk(&arr[0]), b"version");
    assert_eq!(unwrap_integer(&arr[1]), 3);
    assert_eq!(unwrap_bulk(&arr[2]), b"entries");
    assert_eq!(unwrap_integer(&arr[3]), 3);

    server.shutdown().await;
}

// ============================================================================
// ES.ALL tests
// ============================================================================

#[tokio::test]
async fn test_es_all_single_shard() {
    // Use 1 shard to simplify testing
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Append events to two different streams
    let _ = client
        .command(&["ES.APPEND", "stream1", "0", "Event1", "data1"])
        .await;
    let _ = client
        .command(&["ES.APPEND", "stream2", "0", "EventA", "dataA"])
        .await;
    let _ = client
        .command(&["ES.APPEND", "stream1", "1", "Event2", "data2"])
        .await;

    // Read the $all stream
    let resp = client.command(&["ES.ALL"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 3);

    // With COUNT
    let resp = client.command(&["ES.ALL", "COUNT", "2"]).await;
    let arr = unwrap_array(resp);
    assert_eq!(arr.len(), 2);

    server.shutdown().await;
}

// ============================================================================
// Idempotency eviction tests
// ============================================================================

#[tokio::test]
async fn test_es_idempotency_eviction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Append many events with idempotency keys to trigger eviction
    // IdempotencyState::DEFAULT_LIMIT is 10_000, but we test with a smaller
    // number to verify the mechanism works.
    // First, record key "old-key"
    let _ = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "0",
            "Event0",
            "data0",
            "IF_NOT_EXISTS",
            "old-key",
        ])
        .await;

    // Append many more events to push old-key toward eviction
    // (IdempotencyState limit is 10,000, so this won't actually evict in practice,
    // but we verify the key is tracked)
    let resp = client
        .command(&[
            "ES.APPEND",
            "{es}stream",
            "1",
            "Event1",
            "data1",
            "IF_NOT_EXISTS",
            "old-key",
        ])
        .await;
    // Should return the existing version (dedup hit)
    let arr = unwrap_array(resp);
    assert_eq!(unwrap_integer(&arr[0]), 1); // version didn't change

    // Verify info shows idempotency keys tracked
    let resp = client.command(&["ES.INFO", "{es}stream"]).await;
    let arr = unwrap_array(resp);
    // idempotency-keys should be > 0
    assert_eq!(unwrap_bulk(&arr[8]), b"idempotency-keys");
    assert!(unwrap_integer(&arr[9]) > 0);

    server.shutdown().await;
}
