//! Rust port of Redis 8.6.0 `unit/dump.tcl` test suite.
//!
//! Exclusions:
//! - All MIGRATE tests (require two separate server instances communicating)
//! - Tests tagged `needs:debug` (RESTORE with ABSTTL in the past)
//! - Tests tagged `needs:repl` (RESTORE expired key propagation)
//! - Tests tagged `external:skip`
//! - Tests requiring CONFIG SET (RESTORE with LRU, RESTORE with LFU)
//! - Encoding loops
//!
//! ## Intentional exclusions
//!
//! MIGRATE (requires two separate server instances; FrogDB testing model is single-instance):
//! - `MIGRATE is caching connections` — Redis-internal feature
//! - `MIGRATE cached connections are released after some time` — Redis-internal feature
//! - `MIGRATE is able to migrate a key between two instances` — Redis-internal feature
//! - `MIGRATE is able to copy a key between two instances` — Redis-internal feature
//! - `MIGRATE will not overwrite existing keys, unless REPLACE is used` — Redis-internal feature
//! - `MIGRATE propagates TTL correctly` — Redis-internal feature
//! - `MIGRATE can correctly transfer large values` — Redis-internal feature
//! - `MIGRATE can correctly transfer hashes` — Redis-internal feature
//! - `MIGRATE timeout actually works` — Redis-internal feature
//! - `MIGRATE can migrate multiple keys at once` — Redis-internal feature
//! - `MIGRATE with multiple keys must have empty key arg` — Redis-internal feature
//! - `MIGRATE with multiple keys migrate just existing ones` — Redis-internal feature
//! - `MIGRATE with multiple keys: stress command rewriting` — Redis-internal feature
//! - `MIGRATE with multiple keys: delete just ack keys` — Redis-internal feature
//! - `MIGRATE AUTH: correct and wrong password cases` — Redis-internal feature

use bytes::Bytes;
use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::{SystemTime, UNIX_EPOCH};

/// Helper: DUMP a key and return the serialized bytes.
async fn dump_key(client: &mut frogdb_test_harness::server::TestClient, key: &str) -> Vec<u8> {
    let resp = client.command(&["DUMP", key]).await;
    unwrap_bulk(&resp).to_vec()
}

/// Helper: RESTORE a key from serialized bytes with optional extra arguments.
async fn restore_key(
    client: &mut frogdb_test_harness::server::TestClient,
    key: &str,
    ttl: &str,
    serialized: Vec<u8>,
    extra: &[&str],
) -> Response {
    let mut args: Vec<Bytes> = vec![
        Bytes::from("RESTORE"),
        Bytes::from(key.to_string()),
        Bytes::from(ttl.to_string()),
        Bytes::from(serialized),
    ];
    for e in extra {
        args.push(Bytes::from(e.to_string()));
    }
    let refs: Vec<&Bytes> = args.iter().collect();
    client.command_raw(&refs).await
}

// -----------------------------------------------------------------------
// DUMP / RESTORE are able to serialize / unserialize a simple key
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_dump_restore_simple_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;
    client.command(&["DEL", "foo"]).await;

    assert_integer_eq(&client.command(&["EXISTS", "foo"]).await, 0);
    let resp = restore_key(&mut client, "foo", "0", encoded, &[]).await;
    assert_ok(&resp);
    assert_integer_eq(&client.command(&["TTL", "foo"]).await, -1);
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

// -----------------------------------------------------------------------
// RESTORE can set an arbitrary expire to the materialized key
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_with_ttl() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;
    client.command(&["DEL", "foo"]).await;

    let resp = restore_key(&mut client, "foo", "5000", encoded, &[]).await;
    assert_ok(&resp);

    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(
        (3000..=5000).contains(&pttl),
        "PTTL {pttl} should be in 3000..=5000"
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

// -----------------------------------------------------------------------
// RESTORE can set an expire that overflows a 32-bit integer
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_can_set_an_expire_that_overflows_a_32_bit_integer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;
    client.command(&["DEL", "foo"]).await;

    let resp = restore_key(&mut client, "foo", "2569591501", encoded, &[]).await;
    assert_ok(&resp);

    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(
        (2_569_591_501 - 3000..=2_569_591_501).contains(&pttl),
        "PTTL {pttl} should be near 2569591501"
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

// -----------------------------------------------------------------------
// RESTORE can set an absolute expire
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_can_set_an_absolute_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;
    client.command(&["DEL", "foo"]).await;

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let abs_ttl = (now_ms + 3000).to_string();

    let resp = restore_key(&mut client, "foo", &abs_ttl, encoded, &["ABSTTL"]).await;
    assert_ok(&resp);

    let pttl = unwrap_integer(&client.command(&["PTTL", "foo"]).await);
    assert!(
        (2000..=3100).contains(&pttl),
        "PTTL {pttl} should be in 2000..=3100"
    );
    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
}

// -----------------------------------------------------------------------
// RESTORE with TTL maintain valid object (iterate and verify consistency)
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_with_ttl_maintains_valid_object() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;

    for _ in 0..100 {
        client.command(&["DEL", "foo"]).await;
        let resp = restore_key(
            &mut client,
            "foo",
            "1000",
            encoded.clone(),
            &["IDLETIME", "500"],
        )
        .await;
        assert_ok(&resp);
        assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar");
    }
}

// -----------------------------------------------------------------------
// RESTORE returns an error if the key already exists
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_error_on_existing_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let encoded = dump_key(&mut client, "foo").await;

    // Key still exists, RESTORE without REPLACE should fail with BUSYKEY
    let resp = restore_key(&mut client, "foo", "0", encoded, &[]).await;
    assert_error_prefix(&resp, "BUSYKEY");
}

// -----------------------------------------------------------------------
// RESTORE can overwrite an existing key with REPLACE
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_replace_overwrites() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar1"]).await;
    let encoded1 = dump_key(&mut client, "foo").await;

    client.command(&["SET", "foo", "bar2"]).await;
    let encoded2 = dump_key(&mut client, "foo").await;

    client.command(&["DEL", "foo"]).await;

    let resp = restore_key(&mut client, "foo", "0", encoded1, &[]).await;
    assert_ok(&resp);

    let resp = restore_key(&mut client, "foo", "0", encoded2, &["REPLACE"]).await;
    assert_ok(&resp);

    assert_bulk_eq(&client.command(&["GET", "foo"]).await, b"bar2");
}

// -----------------------------------------------------------------------
// RESTORE can detect a syntax error for unrecognized options
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_restore_syntax_error() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = restore_key(
        &mut client,
        "foo",
        "0",
        b"...".to_vec(),
        &["invalid-option"],
    )
    .await;
    // Redis returns ERR ... syntax ...
    match &resp {
        Response::Error(msg) => {
            let lower = String::from_utf8_lossy(msg).to_lowercase();
            assert!(
                lower.contains("syntax"),
                "expected syntax error, got: {lower}"
            );
        }
        other => panic!("expected error response, got: {other:?}"),
    }
}

// -----------------------------------------------------------------------
// DUMP of non-existing key returns nil
// -----------------------------------------------------------------------
#[tokio::test]
async fn tcl_dump_nonexisting_key_returns_nil() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DUMP", "nonexisting_key"]).await;
    assert_nil(&resp);
}
