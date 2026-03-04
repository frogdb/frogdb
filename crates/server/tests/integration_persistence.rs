//! Integration tests for persistence: write → shutdown → restart → read cycles.

mod common;

use bytes::Bytes;
use common::response_helpers::{assert_ok, unwrap_array, unwrap_bulk, unwrap_integer};
use common::test_server::{TestServer, TestServerConfig};
use frogdb_protocol::Response;
use std::time::Duration;

/// Helper: build a persistence-enabled config pointing at the given directory.
fn persistence_config(data_dir: &std::path::Path) -> TestServerConfig {
    TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir.to_path_buf()),
        num_shards: Some(1),
        ..Default::default()
    }
}

// ============================================================================
// Test 1: Data survives a restart
// ============================================================================

#[tokio::test]
async fn test_data_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    // --- First boot: write data ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "value1"]).await);
    assert_eq!(
        client.command(&["LPUSH", "list1", "a", "b", "c"]).await,
        Response::Integer(3)
    );
    assert_eq!(
        client.command(&["HSET", "hash1", "f1", "v1"]).await,
        Response::Integer(1)
    );
    assert_eq!(
        client.command(&["SADD", "set1", "m1", "m2"]).await,
        Response::Integer(2)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Second boot: verify data ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // String
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("value1")))
    );

    // List (LPUSH a b c → stored as [c, b, a])
    let list = unwrap_array(client.command(&["LRANGE", "list1", "0", "-1"]).await);
    let vals: Vec<&[u8]> = list.iter().map(|r| unwrap_bulk(r)).collect();
    assert_eq!(vals, vec![b"c", b"b", b"a"]);

    // Hash
    assert_eq!(
        client.command(&["HGET", "hash1", "f1"]).await,
        Response::Bulk(Some(Bytes::from("v1")))
    );

    // Set
    let members = unwrap_array(client.command(&["SMEMBERS", "set1"]).await);
    let mut strs: Vec<String> = members
        .iter()
        .map(|r| String::from_utf8(unwrap_bulk(r).to_vec()).unwrap())
        .collect();
    strs.sort();
    assert_eq!(strs, vec!["m1", "m2"]);

    server.shutdown().await;
}

// ============================================================================
// Test 2: Expiry persists across restart
// ============================================================================

#[tokio::test]
async fn test_expiry_persists_across_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "value1", "EX", "3600"]).await);
    let ttl = unwrap_integer(&client.command(&["TTL", "key1"]).await);
    assert!(ttl > 3500, "TTL should be ~3600, got {ttl}");

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("value1")))
    );
    let ttl = unwrap_integer(&client.command(&["TTL", "key1"]).await);
    assert!(ttl > 0, "TTL should still be positive after restart, got {ttl}");
    assert!(ttl <= 3600, "TTL should not exceed original, got {ttl}");

    server.shutdown().await;
}

// ============================================================================
// Test 3: Deleted keys stay deleted after restart
// ============================================================================

#[tokio::test]
async fn test_deleted_keys_stay_deleted_after_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "key1", "val1"]).await);
    assert_ok(&client.command(&["SET", "key2", "val2"]).await);
    assert_ok(&client.command(&["SET", "key3", "val3"]).await);
    assert_eq!(
        client.command(&["DEL", "key2"]).await,
        Response::Integer(1)
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_eq!(
        client.command(&["GET", "key2"]).await,
        Response::Bulk(None) // deleted
    );
    assert_eq!(
        client.command(&["GET", "key3"]).await,
        Response::Bulk(Some(Bytes::from("val3")))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 4: BGSAVE snapshot + WAL replay both survive restart
// ============================================================================

#[tokio::test]
async fn test_bgsave_snapshot_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Write data and snapshot it
    assert_ok(&client.command(&["SET", "snap_key", "snap_val"]).await);
    let bgsave_resp = client.command(&["BGSAVE"]).await;
    match &bgsave_resp {
        Response::Simple(msg) => {
            let msg_str = String::from_utf8_lossy(msg);
            assert!(
                msg_str.contains("Background saving started") || msg_str.contains("already in progress"),
                "Unexpected BGSAVE response: {msg_str}"
            );
        }
        _ => panic!("Expected simple string response for BGSAVE, got: {bgsave_resp:?}"),
    }

    // Wait for snapshot to complete
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Write more data AFTER the snapshot — this should be in the WAL only
    assert_ok(&client.command(&["SET", "wal_key", "wal_val"]).await);

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    // Snapshot data
    assert_eq!(
        client.command(&["GET", "snap_key"]).await,
        Response::Bulk(Some(Bytes::from("snap_val")))
    );
    // WAL-replayed data
    assert_eq!(
        client.command(&["GET", "wal_key"]).await,
        Response::Bulk(Some(Bytes::from("wal_val")))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 5: Empty restart — no data, server healthy
// ============================================================================

#[tokio::test]
async fn test_empty_restart_no_data() {
    let tmp = tempfile::tempdir().unwrap();

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(0));
    assert_eq!(
        client.command(&["PING"]).await,
        Response::Simple(Bytes::from_static(b"PONG"))
    );

    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;

    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(0));
    assert_eq!(
        client.command(&["PING"]).await,
        Response::Simple(Bytes::from_static(b"PONG"))
    );

    server.shutdown().await;
}

// ============================================================================
// Test 6: Multiple restarts accumulate data
// ============================================================================

#[tokio::test]
async fn test_multiple_restarts_accumulate_data() {
    let tmp = tempfile::tempdir().unwrap();

    // --- Cycle 1: write key1 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_ok(&client.command(&["SET", "key1", "val1"]).await);
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Cycle 2: write key2, verify key1 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_ok(&client.command(&["SET", "key2", "val2"]).await);
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // --- Cycle 3: write key3, verify key1 + key2 ---
    let server = TestServer::start_standalone_with_config(persistence_config(tmp.path())).await;
    let mut client = server.connect().await;
    assert_eq!(
        client.command(&["GET", "key1"]).await,
        Response::Bulk(Some(Bytes::from("val1")))
    );
    assert_eq!(
        client.command(&["GET", "key2"]).await,
        Response::Bulk(Some(Bytes::from("val2")))
    );
    assert_ok(&client.command(&["SET", "key3", "val3"]).await);
    assert_eq!(client.command(&["DBSIZE"]).await, Response::Integer(3));

    server.shutdown().await;
}
