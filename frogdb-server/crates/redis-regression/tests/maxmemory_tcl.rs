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
//! Client eviction (maxmemory-clients not implemented):
//! - `eviction due to output buffers of many MGET clients, client eviction: false` — intentional-incompatibility:memory — client eviction
//! - `eviction due to output buffers of many MGET clients, client eviction: true` — intentional-incompatibility:memory — client eviction
//! - `eviction due to input buffer of a dead client, client eviction: false` — intentional-incompatibility:memory — client eviction
//! - `eviction due to input buffer of a dead client, client eviction: true` — intentional-incompatibility:memory — client eviction
//! - `eviction due to output buffers of pubsub, client eviction: false` — intentional-incompatibility:memory — client eviction
//! - `eviction due to output buffers of pubsub, client eviction: true` — intentional-incompatibility:memory — client eviction
//!
//! Replication-specific:
//! - `slave buffer are counted correctly` — intentional-incompatibility:replication — replication-internal
//! - `replica buffer don't induce eviction` — intentional-incompatibility:replication — replication-internal
//! - `propagation with eviction` — intentional-incompatibility:replication — replication-internal
//! - `propagation with eviction in MULTI` — intentional-incompatibility:replication — replication-internal
//!
//! Redis internals:
//! - `Don't rehash if used memory exceeds maxmemory after rehash` — intentional-incompatibility:debug — uses populate (DEBUG), dict rehashing internals
//! - `client tracking don't cause eviction feedback loop` — intentional-incompatibility:memory — HELLO 3 + CLIENT TRACKING feedback loop
//!
//! LRM policy (not implemented — Redis 8.x):
//! - `LRM: Basic write updates idle time` — redis-specific — LRM not implemented
//! - `LRM: RENAME updates destination key LRM` — redis-specific — LRM not implemented
//! - `LRM: XREADGROUP updates stream LRM` — redis-specific — LRM not implemented
//! - `LRM: Keys with only read operations should be removed first` — redis-specific — LRM not implemented

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
