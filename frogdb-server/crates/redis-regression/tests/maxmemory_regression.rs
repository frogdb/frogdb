//! Regression tests for the Redis `unit/maxmemory` test suite.
//!
//! FrogDB has a full eviction implementation: CONFIG SET maxmemory/maxmemory-policy,
//! all 8 policies, eviction pool with sampling, LFU log factor + decay time.
//! OBJECT IDLETIME and OBJECT FREQ are fully functional.

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::{TestServer, TestServerConfig};

/// Parse a numeric field from INFO output.
///
/// INFO returns lines like "used_memory:12345\r\n". This extracts the u64
/// value for a given field name.
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

/// Start a single-shard server for maxmemory tests.
///
/// Single shard avoids cross-slot routing complexity and makes memory
/// accounting more predictable.
async fn start_maxmemory_server() -> TestServer {
    TestServer::start_standalone_with_config(TestServerConfig {
        num_shards: Some(1),
        ..Default::default()
    })
    .await
}

// ---------------------------------------------------------------------------
// CONFIG SET/GET roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn config_set_maxmemory_and_policy() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    // Set maxmemory
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", "1048576"])
            .await,
    );
    let resp = client.command(&["CONFIG", "GET", "maxmemory"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[1], b"1048576");

    // Set maxmemory-policy
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
    );
    let resp = client.command(&["CONFIG", "GET", "maxmemory-policy"]).await;
    let items = unwrap_array(resp);
    assert_eq!(items.len(), 2);
    assert_bulk_eq(&items[1], b"allkeys-lru");

    // Verify all policies are accepted
    for policy in &[
        "noeviction",
        "volatile-lru",
        "allkeys-lru",
        "volatile-lfu",
        "allkeys-lfu",
        "volatile-random",
        "allkeys-random",
        "volatile-ttl",
    ] {
        assert_ok(
            &client
                .command(&["CONFIG", "SET", "maxmemory-policy", policy])
                .await,
        );
    }
}

// ---------------------------------------------------------------------------
// noeviction policy — OOM rejection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn noeviction_rejects_writes_over_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    // Get baseline memory
    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    // Set tight limit: current usage + 50KB headroom
    let limit = used + 50_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "noeviction"])
            .await,
    );

    // Fill until we get OOM
    let mut oom_seen = false;
    for i in 0..500 {
        let resp = client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
        if let frogdb_protocol::Response::Error(ref msg) = resp {
            let msg_str = std::str::from_utf8(msg).unwrap_or("");
            if msg_str.contains("OOM") {
                oom_seen = true;
                break;
            }
        }
    }

    assert!(oom_seen, "expected OOM error with noeviction policy");
}

// ---------------------------------------------------------------------------
// allkeys-* eviction policies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn allkeys_lru_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
    );

    // Write 1000 keys (~1MB) — well over the 100KB headroom
    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

#[tokio::test]
async fn allkeys_random_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-random"])
            .await,
    );

    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

#[tokio::test]
async fn allkeys_lfu_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"])
            .await,
    );

    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

// ---------------------------------------------------------------------------
// volatile-* eviction policies (only evict keys with TTL)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn volatile_lru_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "volatile-lru"])
            .await,
    );

    // Write keys WITH TTL so volatile policies can evict them
    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000), "EX", "3600"])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "volatile-lru eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

#[tokio::test]
async fn volatile_random_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "volatile-random"])
            .await,
    );

    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000), "EX", "3600"])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "volatile-random eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

#[tokio::test]
async fn volatile_ttl_honors_memory_limit() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "volatile-ttl"])
            .await,
    );

    // Use varying TTLs — volatile-ttl evicts keys with shortest remaining TTL first
    for i in 0..1000 {
        let ttl = 100 + i; // 100..1099 seconds
        client
            .command(&[
                "SET",
                &format!("key:{i}"),
                &"x".repeat(1000),
                "EX",
                &ttl.to_string(),
            ])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "volatile-ttl eviction should have removed some keys, but DBSIZE={dbsize}"
    );
}

// ---------------------------------------------------------------------------
// volatile policies only evict keys with TTL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn volatile_policies_only_evict_keys_with_ttl() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 200_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "volatile-lru"])
            .await,
    );

    // Write 50 keys WITHOUT TTL — these should survive volatile eviction
    for i in 0..50 {
        client
            .command(&["SET", &format!("persist:{i}"), &"x".repeat(1000)])
            .await;
    }

    // Write 500 keys WITH TTL — these are eviction candidates
    for i in 0..500 {
        client
            .command(&[
                "SET",
                &format!("volatile:{i}"),
                &"x".repeat(1000),
                "EX",
                "3600",
            ])
            .await;
    }

    // All persistent keys should still exist
    let mut persist_count = 0;
    for i in 0..50 {
        let resp = client.command(&["EXISTS", &format!("persist:{i}")]).await;
        if unwrap_integer(&resp) == 1 {
            persist_count += 1;
        }
    }
    assert_eq!(
        persist_count, 50,
        "all persistent (no-TTL) keys should survive volatile eviction"
    );
}

// ---------------------------------------------------------------------------
// allkeys-* policies evict non-volatile keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn allkeys_policies_evict_non_volatile_keys() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
    );

    // Write keys WITHOUT TTL
    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        dbsize < 1000,
        "allkeys-lru should evict non-volatile keys too, but DBSIZE={dbsize}"
    );
}

// ---------------------------------------------------------------------------
// maxmemory 0 means unlimited
// ---------------------------------------------------------------------------

#[tokio::test]
async fn maxmemory_zero_means_unlimited() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    // Set maxmemory to 0 (unlimited)
    assert_ok(&client.command(&["CONFIG", "SET", "maxmemory", "0"]).await);
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
    );

    // Write keys — none should be evicted
    for i in 0..200 {
        assert_ok(
            &client
                .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
                .await,
        );
    }

    let dbsize = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert_eq!(dbsize, 200, "with maxmemory 0, no keys should be evicted");
}

#[tokio::test]
async fn evicted_keys_stat_tracked() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    let info = unwrap_bulk(&client.command(&["INFO", "memory"]).await).to_vec();
    let used = parse_info_field(&info, "used_memory");

    let limit = used + 100_000;
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory", &limit.to_string()])
            .await,
    );
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lru"])
            .await,
    );

    for i in 0..1000 {
        client
            .command(&["SET", &format!("key:{i}"), &"x".repeat(1000)])
            .await;
    }

    let info = unwrap_bulk(&client.command(&["INFO", "stats"]).await).to_vec();
    let evicted = parse_info_field(&info, "evicted_keys");
    assert!(
        evicted > 0,
        "evicted_keys should be > 0 after eviction, got {evicted}"
    );
}

#[tokio::test]
async fn object_idletime_tracks_lru() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);

    // Wait a moment so idle time accumulates
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = client.command(&["OBJECT", "IDLETIME", "mykey"]).await;
    let idle = unwrap_integer(&resp);
    assert!(
        idle >= 1,
        "OBJECT IDLETIME should reflect idle time, got {idle}"
    );
}

#[tokio::test]
async fn object_freq_tracks_lfu() {
    let server = start_maxmemory_server().await;
    let mut client = server.connect().await;

    // Switch to LFU policy so frequency tracking is active
    assert_ok(
        &client
            .command(&["CONFIG", "SET", "maxmemory-policy", "allkeys-lfu"])
            .await,
    );

    assert_ok(&client.command(&["SET", "mykey", "hello"]).await);

    // Access the key multiple times to bump frequency
    for _ in 0..10 {
        client.command(&["GET", "mykey"]).await;
    }

    let resp = client.command(&["OBJECT", "FREQ", "mykey"]).await;
    let freq = unwrap_integer(&resp);
    assert!(
        freq > 0,
        "OBJECT FREQ should reflect access frequency, got {freq}"
    );
}
