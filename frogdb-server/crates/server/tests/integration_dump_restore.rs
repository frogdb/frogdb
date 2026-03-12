//! Integration tests for DUMP / RESTORE command round-trips.
//!
//! Verifies that every value type can be serialized with DUMP and restored with
//! RESTORE, preserving the original data.  Types whose serialization is still
//! stubbed out (Stream, BloomFilter, TimeSeries) are documented with `#[ignore]`
//! tests that assert the *correct* behaviour so they can be enabled once
//! serialization is implemented.

use crate::common::response_helpers::{unwrap_bulk, unwrap_integer};
use crate::common::test_server::{TestServer, is_error};
use bytes::Bytes;
use frogdb_protocol::Response;

// ============================================================================
// Helper
// ============================================================================

/// DUMP a key, DELETE it, RESTORE it under the same name, then return the
/// original DUMP payload so callers can do type-specific assertions.
async fn dump_delete_restore(server: &TestServer, key: &str) -> Bytes {
    // DUMP
    let dump_resp = server.send("DUMP", &[key]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data for key '{key}', got: {other:?}"),
    };

    // DELETE
    let del_resp = server.send("DEL", &[key]).await;
    assert!(
        matches!(del_resp, Response::Integer(1)),
        "DEL should return 1, got: {del_resp:?}"
    );

    // RESTORE (ttl=0 means no expiry)
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from(key.to_string());
    let ttl = Bytes::from("0");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE should return OK, got: {resp:?}"
    );

    payload
}

// ============================================================================
// Round-trip tests for implemented types
// ============================================================================

#[tokio::test]
async fn test_dump_restore_string_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("SET", &["{dr}str", "hello world"]).await;
    dump_delete_restore(&server, "{dr}str").await;

    let resp = server.send("GET", &["{dr}str"]).await;
    assert_eq!(
        unwrap_bulk(&resp),
        b"hello world",
        "String value should survive DUMP/RESTORE"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_hash_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("HSET", &["{dr}hash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;
    dump_delete_restore(&server, "{dr}hash").await;

    let resp = server.send("HGET", &["{dr}hash", "f1"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v1");
    let resp = server.send("HGET", &["{dr}hash", "f2"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v2");
    let resp = server.send("HGET", &["{dr}hash", "f3"]).await;
    assert_eq!(unwrap_bulk(&resp), b"v3");
    assert_eq!(unwrap_integer(&server.send("HLEN", &["{dr}hash"]).await), 3);

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_list_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("RPUSH", &["{dr}list", "a", "b", "c", "d", "e"])
        .await;
    dump_delete_restore(&server, "{dr}list").await;

    assert_eq!(unwrap_integer(&server.send("LLEN", &["{dr}list"]).await), 5);
    assert_eq!(
        unwrap_bulk(&server.send("LINDEX", &["{dr}list", "0"]).await),
        b"a"
    );
    assert_eq!(
        unwrap_bulk(&server.send("LINDEX", &["{dr}list", "4"]).await),
        b"e"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_set_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("SADD", &["{dr}set", "x", "y", "z", "w"]).await;
    dump_delete_restore(&server, "{dr}set").await;

    assert_eq!(unwrap_integer(&server.send("SCARD", &["{dr}set"]).await), 4);
    assert_eq!(
        unwrap_integer(&server.send("SISMEMBER", &["{dr}set", "x"]).await),
        1
    );
    assert_eq!(
        unwrap_integer(&server.send("SISMEMBER", &["{dr}set", "w"]).await),
        1
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_sorted_set_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send(
            "ZADD",
            &["{dr}zset", "1.5", "alpha", "2.5", "beta", "3.5", "gamma"],
        )
        .await;
    dump_delete_restore(&server, "{dr}zset").await;

    assert_eq!(
        unwrap_integer(&server.send("ZCARD", &["{dr}zset"]).await),
        3
    );
    let score = server.send("ZSCORE", &["{dr}zset", "beta"]).await;
    let score_str = std::str::from_utf8(unwrap_bulk(&score)).unwrap();
    let score_val: f64 = score_str.parse().unwrap();
    assert!(
        (score_val - 2.5).abs() < f64::EPSILON,
        "score should be 2.5"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_hll_round_trip() {
    let server = TestServer::start_standalone().await;

    server
        .send("PFADD", &["{dr}hll", "elem1", "elem2", "elem3"])
        .await;
    let count_before = unwrap_integer(&server.send("PFCOUNT", &["{dr}hll"]).await);
    assert!(count_before >= 3, "HLL should count at least 3");

    dump_delete_restore(&server, "{dr}hll").await;

    let count_after = unwrap_integer(&server.send("PFCOUNT", &["{dr}hll"]).await);
    assert_eq!(
        count_before, count_after,
        "HLL cardinality should be preserved after DUMP/RESTORE"
    );

    server.shutdown().await;
}

// ============================================================================
// Known-gap documentation tests (stubbed serialization)
// ============================================================================

#[tokio::test]
async fn test_dump_restore_stream_round_trip() {
    let server = TestServer::start_standalone().await;

    // Add entries to a stream
    server
        .send("XADD", &["{dr}stream", "*", "field1", "value1"])
        .await;
    server
        .send("XADD", &["{dr}stream", "*", "field2", "value2"])
        .await;

    dump_delete_restore(&server, "{dr}stream").await;

    // After proper serialization, stream should have 2 entries
    let len = unwrap_integer(&server.send("XLEN", &["{dr}stream"]).await);
    assert_eq!(len, 2, "Stream should have 2 entries after DUMP/RESTORE");

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_bloom_filter_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("BF.ADD", &["{dr}bloom", "item1"]).await;
    server.send("BF.ADD", &["{dr}bloom", "item2"]).await;

    dump_delete_restore(&server, "{dr}bloom").await;

    // After proper serialization, bloom filter should contain items
    let resp = server.send("BF.EXISTS", &["{dr}bloom", "item1"]).await;
    assert_eq!(
        unwrap_integer(&resp),
        1,
        "BloomFilter should contain item1 after DUMP/RESTORE"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_dump_restore_timeseries_round_trip() {
    let server = TestServer::start_standalone().await;

    server.send("TS.ADD", &["{dr}ts", "1000", "42.0"]).await;
    server.send("TS.ADD", &["{dr}ts", "2000", "43.0"]).await;

    dump_delete_restore(&server, "{dr}ts").await;

    // After proper serialization, the latest sample should be accessible
    let resp = server.send("TS.GET", &["{dr}ts"]).await;
    assert!(
        !is_error(&resp),
        "TS.GET should succeed after DUMP/RESTORE, got: {resp:?}"
    );

    server.shutdown().await;
}

// ============================================================================
// Additional DUMP/RESTORE tests
// ============================================================================

/// Tests that RESTORE with REPLACE flag overwrites an existing key.
#[tokio::test]
async fn test_dump_restore_replace_overwrites_existing_key() {
    let server = TestServer::start_standalone().await;

    // Set original value and DUMP it
    server.send("SET", &["{dr}replace", "original"]).await;
    let dump_resp = server.send("DUMP", &["{dr}replace"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // Overwrite with a different value
    server.send("SET", &["{dr}replace", "overwritten"]).await;

    // RESTORE with REPLACE should succeed even though key exists
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}replace");
    let ttl = Bytes::from("0");
    let replace = Bytes::from("REPLACE");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload, &replace])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE with REPLACE should return OK, got: {resp:?}"
    );

    // Value should be restored to "original"
    let get_resp = server.send("GET", &["{dr}replace"]).await;
    assert_eq!(
        unwrap_bulk(&get_resp),
        b"original",
        "RESTORE REPLACE should overwrite with original value"
    );

    server.shutdown().await;
}

/// Tests that RESTORE without REPLACE fails when key already exists (BUSYKEY).
#[tokio::test]
async fn test_dump_restore_without_replace_fails_on_existing_key() {
    let server = TestServer::start_standalone().await;

    // Set and DUMP a value
    server.send("SET", &["{dr}busykey", "value1"]).await;
    let dump_resp = server.send("DUMP", &["{dr}busykey"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // Key still exists — RESTORE without REPLACE should fail
    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}busykey");
    let ttl = Bytes::from("0");
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        is_error(&resp),
        "RESTORE without REPLACE on existing key should error, got: {resp:?}"
    );

    server.shutdown().await;
}

/// Tests that RESTORE with a TTL argument sets expiry on the restored key.
#[tokio::test]
async fn test_dump_restore_preserves_ttl() {
    let server = TestServer::start_standalone().await;

    // Set and DUMP a value
    server.send("SET", &["{dr}ttlkey", "ttl_value"]).await;
    let dump_resp = server.send("DUMP", &["{dr}ttlkey"]).await;
    let payload = match &dump_resp {
        Response::Bulk(Some(data)) => data.clone(),
        other => panic!("DUMP should return bulk data, got: {other:?}"),
    };

    // DELETE and RESTORE with TTL of 10000ms
    server.send("DEL", &["{dr}ttlkey"]).await;

    let restore_cmd = Bytes::from("RESTORE");
    let key_bytes = Bytes::from("{dr}ttlkey");
    let ttl = Bytes::from("10000"); // 10 seconds
    let resp = server
        .connect()
        .await
        .command_raw(&[&restore_cmd, &key_bytes, &ttl, &payload])
        .await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s.as_ref() == b"OK"),
        "RESTORE with TTL should return OK, got: {resp:?}"
    );

    // Verify TTL is set
    let pttl_resp = server.send("PTTL", &["{dr}ttlkey"]).await;
    let pttl = unwrap_integer(&pttl_resp);
    assert!(
        pttl > 0 && pttl <= 10000,
        "PTTL should be between 0 and 10000, got: {}",
        pttl
    );

    // Verify value is correct
    let get_resp = server.send("GET", &["{dr}ttlkey"]).await;
    assert_eq!(unwrap_bulk(&get_resp), b"ttl_value");

    server.shutdown().await;
}

/// Tests that DUMP on a non-existent key returns nil.
#[tokio::test]
async fn test_dump_nil_for_nonexistent_key() {
    let server = TestServer::start_standalone().await;

    let resp = server.send("DUMP", &["{dr}nonexistent"]).await;
    assert!(
        matches!(resp, Response::Bulk(None)),
        "DUMP on non-existent key should return nil, got: {resp:?}"
    );

    server.shutdown().await;
}

/// Tests that integer-encoded strings round-trip correctly through DUMP/RESTORE.
#[tokio::test]
async fn test_dump_restore_integer_encoded_string() {
    let server = TestServer::start_standalone().await;

    // Set an integer-like string (Redis may encode this as an integer internally)
    server.send("SET", &["{dr}intstr", "12345"]).await;
    dump_delete_restore(&server, "{dr}intstr").await;

    let resp = server.send("GET", &["{dr}intstr"]).await;
    assert_eq!(
        unwrap_bulk(&resp),
        b"12345",
        "Integer-encoded string should survive DUMP/RESTORE"
    );

    // Also verify it can be used as an integer
    let incr_resp = server.send("INCR", &["{dr}intstr"]).await;
    assert_eq!(
        unwrap_integer(&incr_resp),
        12346,
        "Restored integer string should be incrementable"
    );

    server.shutdown().await;
}
