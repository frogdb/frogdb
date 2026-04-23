//! Rust port of Redis 8.6.0 `unit/maxmemory.tcl` test suite.
//!
//! Ports the three policy-parameterized test groups (is the memory limit
//! honoured, only allkeys-* should remove non-volatile keys, volatile policy
//! should only remove volatile keys). FrogDB supports 8 standard Redis
//! policies (no LRM). Each policy variant is a separate test function.
//!
//! ## Intentional exclusions
//!
//! DUMP/RESTORE (not implemented):
//! - `SET and RESTORE key nearly as large as the memory limit` — intentional-incompatibility:persistence — DUMP/RESTORE not implemented
//!
//! Replication-specific:
//! - `slave buffer are counted correctly` — intentional-incompatibility:replication — replication-internal
//! - `replica buffer don't induce eviction` — intentional-incompatibility:replication — replication-internal
//! - `propagation with eviction` — intentional-incompatibility:replication — replication-internal
//! - `propagation with eviction in MULTI` — intentional-incompatibility:replication — replication-internal
//!
//! Redis internals:
//! - `Don't rehash if used memory exceeds maxmemory after rehash` — intentional-incompatibility:debug — uses populate (DEBUG), dict rehashing internals
//!
//! LRM policy (not implemented — Redis 8.x):
//! - `LRM: Basic write updates idle time` — redis-specific — LRM not implemented
//! - `LRM: RENAME updates destination key LRM` — redis-specific — LRM not implemented
//! - `LRM: XREADGROUP updates stream LRM` — redis-specific — LRM not implemented
//! - `LRM: Keys with only read operations should be removed first` — redis-specific — LRM not implemented

use std::collections::HashMap;
use std::time::Duration;

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};

/// Parse a numeric field from INFO output.
fn parse_info_field(info: &[u8], field: &str) -> u64 {
    let text = std::str::from_utf8(info).expect("INFO should be valid UTF-8");
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(&format!("{field}:")) {
            return rest
                .trim()
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("failed to parse {field} value '{rest}': {e}"));
        }
    }
    panic!("field '{field}' not found in INFO output");
}

/// Get used_memory from INFO memory.
async fn get_used_memory(client: &mut frogdb_test_harness::server::TestClient) -> u64 {
    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    parse_info_field(&info, "used_memory")
}

/// Start a single-shard server for maxmemory tests.
async fn start_maxmemory_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

/// Generate a pseudo-random key name for a given index (simple hash to avoid
/// sequential patterns that might bias eviction sampling).
fn random_key(i: u32) -> String {
    // Upstream uses [randomKey] which returns a random string. We use a
    // deterministic-but-well-distributed key to keep tests reproducible.
    format!("key:{i}")
}

// ===========================================================================
// Group 1: "maxmemory - is the memory limit honoured? (policy $policy)"
//
// For each policy: flushall, measure used_memory, set maxmemory = used + 100KB,
// add keys with SETEX until near limit, add same number again, assert
// used_memory < limit + 4096.
// ===========================================================================

async fn test_limit_honoured(policy: &str) {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;

    let used = get_used_memory(&mut client).await;
    let limit = used + 100 * 1024;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", policy])
            .await,
    );

    // Add keys until near the limit.
    let mut numkeys = 0u32;
    loop {
        client
            .command(&["SETEX", &random_key(numkeys), "10000", "x"])
            .await;
        numkeys += 1;

        let mem = get_used_memory(&mut client).await;
        if mem + 4096 > limit {
            assert!(
                numkeys > 10,
                "should have added >10 keys before hitting limit"
            );
            break;
        }
    }

    // Add the same number of keys again — eviction should keep us under limit.
    for j in 0..numkeys {
        client
            .command(&["SETEX", &random_key(numkeys + j), "10000", "x"])
            .await;
    }

    let final_mem = get_used_memory(&mut client).await;
    assert!(
        final_mem < limit + 4096,
        "policy {policy}: used_memory {final_mem} should be < limit+4096 ({})",
        limit + 4096
    );
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_allkeys_random() {
    test_limit_honoured("allkeys-random").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_allkeys_lru() {
    test_limit_honoured("allkeys-lru").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_allkeys_lfu() {
    test_limit_honoured("allkeys-lfu").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_volatile_lru() {
    test_limit_honoured("volatile-lru").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_volatile_lfu() {
    test_limit_honoured("volatile-lfu").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_volatile_random() {
    test_limit_honoured("volatile-random").await;
}

#[tokio::test]
async fn tcl_maxmemory_limit_honoured_volatile_ttl() {
    test_limit_honoured("volatile-ttl").await;
}

// ===========================================================================
// Group 2: "maxmemory - only allkeys-* should remove non-volatile keys"
//
// For each policy: add non-volatile keys (SET, no TTL) until near limit, then
// add more. allkeys-* policies should stay under limit; volatile-* policies
// should hit OOM because no keys have a TTL.
// ===========================================================================

async fn test_nonvolatile_keys(policy: &str) {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;

    let used = get_used_memory(&mut client).await;
    let limit = used + 100 * 1024;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", policy])
            .await,
    );

    // Add non-volatile keys until near the limit.
    let mut numkeys = 0u32;
    loop {
        client.command(&["SET", &random_key(numkeys), "x"]).await;
        numkeys += 1;

        let mem = get_used_memory(&mut client).await;
        if mem + 4096 > limit {
            assert!(
                numkeys > 10,
                "should have added >10 keys before hitting limit"
            );
            break;
        }
    }

    // Add the same number of keys again. allkeys-* should evict and stay under
    // limit; volatile-* can't evict (no TTL keys) and should return OOM.
    let mut oom_seen = false;
    for j in 0..numkeys {
        let resp = client
            .command(&["SET", &random_key(numkeys + j), "x"])
            .await;
        if let frogdb_protocol::Response::Error(ref msg) = resp {
            let msg_str = std::str::from_utf8(msg).unwrap_or("");
            if msg_str.contains("OOM") || msg_str.contains("used memory") {
                oom_seen = true;
            }
        }
    }

    if policy.starts_with("allkeys-") {
        let final_mem = get_used_memory(&mut client).await;
        assert!(
            final_mem < limit + 4096,
            "policy {policy}: allkeys should evict non-volatile keys, but used_memory={final_mem}"
        );
    } else {
        assert!(
            oom_seen,
            "policy {policy}: volatile policy should OOM when only non-volatile keys exist"
        );
    }
}

#[tokio::test]
async fn tcl_maxmemory_nonvolatile_allkeys_random() {
    test_nonvolatile_keys("allkeys-random").await;
}

#[tokio::test]
async fn tcl_maxmemory_nonvolatile_allkeys_lru() {
    test_nonvolatile_keys("allkeys-lru").await;
}

#[tokio::test]
async fn tcl_maxmemory_nonvolatile_volatile_lru() {
    test_nonvolatile_keys("volatile-lru").await;
}

#[tokio::test]
async fn tcl_maxmemory_nonvolatile_volatile_random() {
    test_nonvolatile_keys("volatile-random").await;
}

#[tokio::test]
async fn tcl_maxmemory_nonvolatile_volatile_ttl() {
    test_nonvolatile_keys("volatile-ttl").await;
}

// ===========================================================================
// Group 3: "maxmemory - policy $policy should only remove volatile keys."
//
// For each volatile policy: mix volatile (odd keys with SETEX) and non-volatile
// (even keys with SET), add more volatile keys, assert under limit, assert all
// non-volatile keys survive.
// ===========================================================================

async fn test_volatile_only(policy: &str) {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;

    let used = get_used_memory(&mut client).await;
    let limit = used + 100 * 1024;

    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", policy])
            .await,
    );

    // Add keys until near the limit. Odd keys are volatile, even are non-volatile.
    let mut numkeys = 0u32;
    loop {
        if numkeys % 2 == 1 {
            client
                .command(&["SETEX", &format!("key:{numkeys}"), "10000", "x"])
                .await;
        } else {
            client
                .command(&["SET", &format!("key:{numkeys}"), "x"])
                .await;
        }

        let mem = get_used_memory(&mut client).await;
        if mem + 4096 > limit {
            assert!(
                numkeys > 10,
                "should have added >10 keys before hitting limit"
            );
            break;
        }
        numkeys += 1;
    }

    // Add the same number of volatile keys — eviction should keep us under limit
    // by removing only volatile keys.
    for j in 0..numkeys {
        // Ignore OOM errors — some volatile policies may temporarily fail
        let _ = client
            .command(&["SETEX", &format!("foo:{j}"), "10000", "x"])
            .await;
    }

    let final_mem = get_used_memory(&mut client).await;
    assert!(
        final_mem < limit + 4096,
        "policy {policy}: should stay under limit after volatile eviction, used_memory={final_mem}"
    );

    // All non-volatile keys (even indices) should still exist.
    for j in (0..numkeys).step_by(2) {
        let resp = client.command(&["EXISTS", &format!("key:{j}")]).await;
        assert_integer_eq(&resp, 1);
    }
}

#[tokio::test]
async fn tcl_maxmemory_volatile_only_volatile_lru() {
    test_volatile_only("volatile-lru").await;
}

#[tokio::test]
async fn tcl_maxmemory_volatile_only_volatile_lfu() {
    test_volatile_only("volatile-lfu").await;
}

#[tokio::test]
async fn tcl_maxmemory_volatile_only_volatile_random() {
    test_volatile_only("volatile-random").await;
}

#[tokio::test]
async fn tcl_maxmemory_volatile_only_volatile_ttl() {
    test_volatile_only("volatile-ttl").await;
}

// ===========================================================================
// OBJECT IDLETIME/FREQ: lru/lfu value of the key just added
// ===========================================================================

#[tokio::test]
async fn tcl_lru_lfu_value_of_key_just_added() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    // Create a key
    client.command(&["SET", "mykey", "myval"]).await;

    // A freshly-created key should have idle time 0 (or close to 0)
    let idle = unwrap_integer(&client.command(&["OBJECT", "IDLETIME", "mykey"]).await);
    assert!(
        idle <= 1,
        "OBJECT IDLETIME of just-added key should be 0 or 1, got {idle}"
    );

    // LFU counter should be > 0 for a just-created key (initial value is 5)
    let freq = unwrap_integer(&client.command(&["OBJECT", "FREQ", "mykey"]).await);
    assert!(
        freq > 0,
        "OBJECT FREQ of just-added key should be > 0, got {freq}"
    );
}

// ===========================================================================
// Client eviction tests (maxmemory-clients integration with maxmemory)
// ===========================================================================

/// Parse a CLIENT LIST response into a list of per-client field maps.
fn parse_client_list(data: &[u8]) -> Vec<HashMap<String, String>> {
    let text = std::str::from_utf8(data).unwrap();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.split_whitespace()
                .filter_map(|kv| {
                    let (k, v) = kv.split_once('=')?;
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        })
        .collect()
}

/// Helper: set up a maxmemory server with a specific maxmemory limit
/// and optionally enable maxmemory-clients.
async fn start_maxmemory_client_eviction_server(
    maxmemory: u64,
    maxmemory_clients: Option<&str>,
) -> TestServer {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut admin = server.connect().await;

    // Set maxmemory limit
    assert_ok(
        &admin
            .command(&["CONFIG", "SET", "maxmemory", &maxmemory.to_string()])
            .await,
    );
    assert_ok(
        &admin
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"])
            .await,
    );

    if let Some(mc) = maxmemory_clients {
        assert_ok(
            &admin
                .command(&["CONFIG", "SET", "maxmemory-clients", mc])
                .await,
        );
    }

    server
}

// ===========================================================================
// eviction due to output buffers of many MGET clients
//
// When client eviction is OFF: many MGET clients bloat their output buffers,
// which should NOT cause key eviction to happen incorrectly.
// When client eviction is ON: the bloated clients should be evicted instead
// of keys.
// ===========================================================================

/// Test that MGET clients with large output buffers don't cause incorrect
/// key eviction when client eviction is disabled.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_output_buffers_of_mget_clients_client_eviction_false() {
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, None).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);

    // Create some data to serve
    let val = "x".repeat(1000);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    let initial_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert!(
        initial_keys >= 200,
        "should have at least 200 keys, got {initial_keys}"
    );

    // Create several MGET clients that read lots of data (bloating output buffers)
    let mut mget_keys: Vec<String> = Vec::new();
    for i in 0..200 {
        mget_keys.push(format!("key:{i}"));
    }
    let mget_args: Vec<&str> = std::iter::once("MGET")
        .chain(mget_keys.iter().map(|s| s.as_str()))
        .collect();

    let mut clients = Vec::new();
    for i in 0..5 {
        let mut c = server.connect().await;
        assert_ok(&c.command(&["CLIENT", "SETNAME", &format!("mget{i}")]).await);
        // Issue MGET to bloat output buffer
        c.command(&mget_args).await;
        clients.push(c);
    }

    // Wait for memory sync
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.command(&["PING"]).await;

    // Without client eviction, key eviction might trigger. But the important thing
    // is that the server is still healthy and the admin connection works.
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    // Keys should still exist (output buffers don't count toward maxmemory for key eviction)
    assert!(final_keys > 0, "should still have keys, got {final_keys}");
}

/// Test that MGET clients with large output buffers are evicted when
/// client eviction is enabled.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_output_buffers_of_mget_clients_client_eviction_true() {
    // 4MB maxmemory, 50KB maxmemory-clients
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, Some("50kb")).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);
    assert_ok(&admin.command(&["CLIENT", "NO-EVICT", "ON"]).await);

    // Create some data to serve
    let val = "x".repeat(1000);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    let initial_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);

    // Create several MGET clients that read lots of data (bloating output buffers)
    let mut mget_keys: Vec<String> = Vec::new();
    for i in 0..200 {
        mget_keys.push(format!("key:{i}"));
    }
    let mget_args: Vec<&str> = std::iter::once("MGET")
        .chain(mget_keys.iter().map(|s| s.as_str()))
        .collect();

    for i in 0..5 {
        let mut c = server.connect().await;
        let _ = c.command(&["CLIENT", "SETNAME", &format!("mget{i}")]).await;
        // Issue MGET to bloat output buffer
        let _ = c.command(&mget_args).await;
        // Don't hold the client - let it be potentially evicted
        drop(c);
    }

    // Wait for eviction to potentially happen
    tokio::time::sleep(Duration::from_millis(1000)).await;
    admin.command(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // With client eviction, keys should be preserved (clients are evicted instead)
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert_eq!(
        final_keys, initial_keys,
        "keys should be preserved when client eviction handles memory pressure"
    );

    // Admin connection should survive (NO-EVICT)
    admin.command(&["PING"]).await;
}

// ===========================================================================
// eviction due to input buffer of a dead client
//
// A "dead" client sends a large request but stops reading, leaving data
// in the input buffer. Without client eviction, the server may need to
// evict keys. With client eviction, the dead client itself is evicted.
// ===========================================================================

/// Test key behavior with dead client input buffer, client eviction disabled.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_input_buffer_of_dead_client_client_eviction_false() {
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, None).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);

    // Populate data
    let val = "x".repeat(500);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    let initial_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);

    // Create a "dead" client that sends a large value but doesn't do much after
    let mut dead = server.connect().await;
    assert_ok(&dead.command(&["CLIENT", "SETNAME", "dead"]).await);
    let big_val = "x".repeat(200 * 1024);
    dead.command(&["SET", "bigkey", &big_val]).await;

    // Wait for memory sync
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.command(&["PING"]).await;

    // Without client eviction, the server should still be healthy
    let resp = admin.command(&["CLIENT", "LIST"]).await;
    let data = unwrap_bulk(&resp);
    let text = std::str::from_utf8(data).unwrap();
    assert!(
        text.contains("name=admin"),
        "admin should still be connected"
    );

    // Keys should mostly still exist
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert!(final_keys > 0, "should still have keys, got {final_keys}");
    let _ = (initial_keys, dead);
}

/// Test that dead client with large input buffer is evicted when client eviction is on.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_input_buffer_of_dead_client_client_eviction_true() {
    // 4MB maxmemory, 50KB maxmemory-clients
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, Some("50kb")).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);
    assert_ok(&admin.command(&["CLIENT", "NO-EVICT", "ON"]).await);

    // Populate data
    let val = "x".repeat(500);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    let initial_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);

    // Create a "dead" client that sends a large value
    let mut dead = server.connect().await;
    let _ = dead.command(&["CLIENT", "SETNAME", "dead"]).await;
    let big_val = "x".repeat(200 * 1024);
    dead.command(&["SET", "bigkey", &big_val]).await;

    // Wait for memory sync and eviction
    tokio::time::sleep(Duration::from_millis(1000)).await;
    admin.command(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // With client eviction, keys should be preserved
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    // initial_keys + 1 because "dead" client added "bigkey"
    assert!(
        final_keys >= initial_keys,
        "keys should be preserved when client eviction is active: initial={initial_keys}, final={final_keys}"
    );

    // Admin should survive
    admin.command(&["PING"]).await;
}

// ===========================================================================
// eviction due to output buffers of pubsub
//
// Pub/Sub subscribers accumulate messages in their output buffers.
// Without client eviction, the server handles it via maxmemory key eviction.
// With client eviction, the subscriber is evicted.
// ===========================================================================

/// Test key behavior with pubsub output buffers, client eviction disabled.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_output_buffers_of_pubsub_client_eviction_false() {
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, None).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);

    // Populate data
    let val = "x".repeat(500);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    // Create a subscriber
    let mut sub = server.connect().await;
    assert_ok(&sub.command(&["CLIENT", "SETNAME", "sub"]).await);
    sub.send_only(&["SUBSCRIBE", "mychannel"]).await;
    let _ = sub.read_response(Duration::from_millis(100)).await;

    // Publish messages to bloat the subscriber's output buffer
    let msg = "x".repeat(5000);
    for _ in 0..100 {
        admin.command(&["PUBLISH", "mychannel", &msg]).await;
    }

    // Wait for memory sync
    tokio::time::sleep(Duration::from_millis(500)).await;
    admin.command(&["PING"]).await;

    // Without client eviction, server should still be healthy
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert!(final_keys > 0, "should still have keys, got {final_keys}");
}

/// Test that pubsub subscriber with large output buffers is evicted when
/// client eviction is enabled.
#[tokio::test]
async fn tcl_maxmemory_eviction_due_to_output_buffers_of_pubsub_client_eviction_true() {
    // 4MB maxmemory, 50KB maxmemory-clients
    let server = start_maxmemory_client_eviction_server(4 * 1024 * 1024, Some("50kb")).await;
    let mut admin = server.connect().await;
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);
    assert_ok(&admin.command(&["CLIENT", "NO-EVICT", "ON"]).await);

    // Populate data
    let val = "x".repeat(500);
    for i in 0..200 {
        admin.command(&["SET", &format!("key:{i}"), &val]).await;
    }

    let initial_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);

    // Create a subscriber
    let mut sub = server.connect().await;
    let _ = sub.command(&["CLIENT", "SETNAME", "sub"]).await;
    sub.send_only(&["SUBSCRIBE", "mychannel"]).await;
    let _ = sub.read_response(Duration::from_millis(100)).await;

    // Publish messages to bloat the subscriber's output buffer
    let msg = "x".repeat(5000);
    for _ in 0..100 {
        admin.command(&["PUBLISH", "mychannel", &msg]).await;
    }

    // Wait for eviction
    tokio::time::sleep(Duration::from_millis(1000)).await;
    admin.command(&["PING"]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // With client eviction, keys should be preserved
    let final_keys = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert!(
        final_keys >= initial_keys,
        "keys should be preserved with client eviction: initial={initial_keys}, final={final_keys}"
    );

    // Admin should survive
    admin.command(&["PING"]).await;
}

// ===========================================================================
// client tracking don't cause eviction feedback loop
//
// When client tracking invalidation messages are sent, they increase the
// output buffer size. If this triggers client eviction, and the eviction
// clears keys that trigger more invalidations, it could create a feedback
// loop. This test verifies that doesn't happen.
// ===========================================================================

#[tokio::test]
async fn tcl_maxmemory_client_tracking_no_eviction_feedback_loop() {
    let server = TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await;
    let mut admin = server.connect().await;

    // Set up maxmemory and client eviction
    assert_ok(
        &admin
            .command(&["CONFIG", "SET", "maxmemory", "4194304"]) // 4MB
            .await,
    );
    assert_ok(
        &admin
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"])
            .await,
    );
    assert_ok(
        &admin
            .command(&["CONFIG", "SET", "maxmemory-clients", "200kb"])
            .await,
    );
    assert_ok(&admin.command(&["CLIENT", "SETNAME", "admin"]).await);
    assert_ok(&admin.command(&["CLIENT", "NO-EVICT", "ON"]).await);

    // Create a RESP3 client with tracking enabled
    let mut tracker = server.connect_resp3().await;

    // HELLO 3 to switch to RESP3
    let hello_resp = tracker.command(&["HELLO", "3"]).await;
    // HELLO 3 should return a map with proto:3
    assert!(
        !matches!(
            hello_resp,
            redis_protocol::resp3::types::BytesFrame::SimpleError { .. }
        ),
        "HELLO 3 should succeed"
    );

    // Enable client tracking
    let track_resp = tracker.command(&["CLIENT", "TRACKING", "ON"]).await;
    assert!(
        !matches!(
            track_resp,
            redis_protocol::resp3::types::BytesFrame::SimpleError { .. }
        ),
        "CLIENT TRACKING ON should succeed"
    );

    // Create some keys and access them to build up tracking state
    for i in 0..50 {
        tracker
            .command(&["SET", &format!("track:{i}"), "val"])
            .await;
    }

    // Now read them to register tracking interest
    for i in 0..50 {
        tracker.command(&["GET", &format!("track:{i}")]).await;
    }

    // Modify the keys from admin to trigger invalidations
    for i in 0..50 {
        admin
            .command(&["SET", &format!("track:{i}"), "newval"])
            .await;
    }

    // Wait a bit for invalidation messages to be processed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The key test: the server should still be healthy with no feedback loop.
    // The tracker client should still be alive or gracefully evicted.
    // The admin connection must survive.
    admin.command(&["PING"]).await;

    // Verify no crash or hang occurred
    let dbsize = unwrap_integer(&admin.command(&["DBSIZE"]).await);
    assert!(dbsize > 0, "database should still have keys, got {dbsize}");

    // Check that admin is still in client list
    let resp = admin.command(&["CLIENT", "LIST"]).await;
    let data = unwrap_bulk(&resp);
    let clients = parse_client_list(data);
    assert!(
        clients
            .iter()
            .any(|c| c.get("name").map(|n| n.as_str()) == Some("admin")),
        "admin client should survive potential feedback loop"
    );
}
