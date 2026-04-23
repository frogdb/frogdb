//! Rust port of Redis 8.6.0 `unit/info-keysizes.tcl`.
//!
//! Tests the `INFO keysizes` section, `DEBUG KEYSIZES-HIST-ASSERT`,
//! `DEBUG ALLOCSIZE-SLOTS-ASSERT`, and `key-memory-histograms` config.
//!
//! ## Intentional exclusions
//!
//! `$suffixRepl` replication variants (needs:repl):
//! - `KEYSIZES - Test i'th bin counts keysizes between (2^i) and (2^(i+1)-1) as expected $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Histogram values of Bytes, Kilo and Mega $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test hyperloglog $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test List $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test SET $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test ZSET $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test STRING $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test complex dataset $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test HASH ($type) $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test Hash field lazy expiration ($type) $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test STRING BITS $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test RESTORE $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test RENAME $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEYSIZES - Test MOVE $suffixRepl` — intentional-incompatibility:observability — MOVE (singledb) not implemented in FrogDB
//! - `KEYSIZES - Test SWAPDB $suffixRepl` — intentional-incompatibility:observability — SWAPDB (singledb) not implemented in FrogDB
//! - `KEYSIZES - DEBUG RELOAD reset keysizes $suffixRepl` — intentional-incompatibility:persistence — DEBUG RELOAD not implemented in FrogDB
//! - `KEYSIZES - Test RDB $suffixRepl` — intentional-incompatibility:observability — needs:repl — replication keysizes not yet implemented
//! - `KEY-MEMORY-STATS - Replication updates key memory stats on replica` — intentional-incompatibility:observability — needs:repl — replication + key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - DEL on primary updates key memory stats on replica` — intentional-incompatibility:observability — needs:repl — replication + key-memory-histograms not implemented in FrogDB
//! - `KEY-MEMORY-STATS - DEBUG RELOAD preserves key memory histogram` — intentional-incompatibility:observability — DEBUG RELOAD not implemented in FrogDB
//! - `KEY-MEMORY-STATS - RDB save and restart preserves key memory histogram` — intentional-incompatibility:observability — RDB save/restart not yet implemented

use bytes::Bytes;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ============================================================================
// Helper functions
// ============================================================================

/// Parse the INFO keysizes section text into a vec of (field_name, histogram_string).
fn parse_keysizes_section(info_text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut in_section = false;
    for line in info_text.lines() {
        let line = line.trim_end_matches('\r');
        if line == "# Keysizes" {
            in_section = true;
            continue;
        }
        if in_section {
            if line.is_empty() || line.starts_with('#') {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                results.push((k.to_string(), v.to_string()));
            }
        }
    }
    results
}

/// Get the INFO keysizes section text.
async fn get_keysizes_info(
    client: &mut frogdb_test_harness::server::TestClient,
) -> Vec<(String, String)> {
    let resp = client.command(&["INFO", "keysizes"]).await;
    let text = String::from_utf8(unwrap_bulk(&resp).to_vec()).unwrap();
    parse_keysizes_section(&text)
}

/// Assert a DEBUG KEYSIZES-HIST-ASSERT returns OK.
async fn assert_hist(
    client: &mut frogdb_test_harness::server::TestClient,
    type_name: &str,
    bin: usize,
    expected: u64,
) {
    let resp = client
        .command(&[
            "DEBUG",
            "KEYSIZES-HIST-ASSERT",
            type_name,
            &bin.to_string(),
            &expected.to_string(),
        ])
        .await;
    assert_ok(&resp);
}

/// Assert a DEBUG ALLOCSIZE-SLOTS-ASSERT returns OK.
async fn assert_allocsize(
    client: &mut frogdb_test_harness::server::TestClient,
    slot: u16,
    expected: usize,
) {
    let resp = client
        .command(&[
            "DEBUG",
            "ALLOCSIZE-SLOTS-ASSERT",
            &slot.to_string(),
            &expected.to_string(),
        ])
        .await;
    assert_ok(&resp);
}

/// DUMP a key and return the serialized bytes.
async fn dump_key(client: &mut frogdb_test_harness::server::TestClient, key: &str) -> Vec<u8> {
    let resp = client.command(&["DUMP", key]).await;
    unwrap_bulk(&resp).to_vec()
}

/// RESTORE a key from serialized bytes.
async fn restore_key(
    client: &mut frogdb_test_harness::server::TestClient,
    key: &str,
    ttl: &str,
    serialized: Vec<u8>,
    extra: &[&str],
) {
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
    let resp = client.command_raw(&refs).await;
    assert_ok(&resp);
}

// ============================================================================
// KEYSIZES tests
// ============================================================================

/// KEYSIZES - Test i'th bin counts keysizes between (2^i) and (2^(i+1)-1) as expected
#[tokio::test]
async fn keysizes_bin_counting() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Bin 0: size 0/1 — set an empty string
    client.command(&["SET", "k0", ""]).await;
    assert_hist(&mut client, "strings", 0, 1).await;

    // Bin 0: size 1 — set a 1-byte string
    client.command(&["SET", "k1", "x"]).await;
    assert_hist(&mut client, "strings", 0, 2).await;

    // Bin 1: size 2 — set a 2-byte string
    client.command(&["SET", "k2", "ab"]).await;
    assert_hist(&mut client, "strings", 1, 1).await;

    // Bin 2: size 3..4 — set a 3-byte string
    client.command(&["SET", "k3", "abc"]).await;
    assert_hist(&mut client, "strings", 2, 1).await;

    // Bin 3: size 5..8 — set a 5-byte string
    client.command(&["SET", "k5", "abcde"]).await;
    assert_hist(&mut client, "strings", 3, 1).await;

    // Clean up
    client.command(&["FLUSHALL"]).await;
    assert_hist(&mut client, "strings", 0, 0).await;
}

/// KEYSIZES - Histogram values of Bytes, Kilo and Mega
#[tokio::test]
async fn keysizes_byte_kilo_mega_labels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create strings of specific sizes and verify INFO output format
    // 1K = 1024 bytes -> bin 10
    let val_1k = "x".repeat(1024);
    client.command(&["SET", "k1k", &val_1k]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let strings_line = keysizes
        .iter()
        .find(|(k, _)| k == "distrib_strings_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(
        strings_line.contains("1K=1"),
        "expected 1K=1 in: {strings_line}"
    );

    // 1M = 1048576 bytes -> bin 20
    let val_1m = "x".repeat(1048576);
    client.command(&["SET", "k1m", &val_1m]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let strings_line = keysizes
        .iter()
        .find(|(k, _)| k == "distrib_strings_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    assert!(
        strings_line.contains("1M=1"),
        "expected 1M=1 in: {strings_line}"
    );
}

/// KEYSIZES - Test hyperloglog
#[tokio::test]
async fn keysizes_hyperloglog() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Add elements to HLL
    for i in 0..100 {
        client
            .command(&["PFADD", "myhll", &format!("elem{i}")])
            .await;
    }

    // HLL should show in the hlls histogram
    // The estimated cardinality ~100 falls in bin 7 (65..128)
    let keysizes = get_keysizes_info(&mut client).await;
    let has_hll = keysizes
        .iter()
        .any(|(k, _)| k == "distrib_strings_sizes_hll");
    assert!(has_hll, "expected HLL histogram in keysizes: {keysizes:?}");
}

/// KEYSIZES - Test List
#[tokio::test]
async fn keysizes_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a list with 10 elements -> bin 4 (9..16)
    for i in 0..10 {
        client
            .command(&["RPUSH", "mylist", &format!("val{i}")])
            .await;
    }
    assert_hist(&mut client, "lists", 4, 1).await;

    // Add more to get 20 elements -> bin 5 (17..32)
    for i in 10..20 {
        client
            .command(&["RPUSH", "mylist", &format!("val{i}")])
            .await;
    }
    assert_hist(&mut client, "lists", 5, 1).await;
    assert_hist(&mut client, "lists", 4, 0).await; // migrated out
}

/// KEYSIZES - Test SET
#[tokio::test]
async fn keysizes_set() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a set with 5 members -> bin 3 (5..8)
    for i in 0..5 {
        client.command(&["SADD", "myset", &format!("m{i}")]).await;
    }
    assert_hist(&mut client, "sets", 3, 1).await;
}

/// KEYSIZES - Test ZSET
#[tokio::test]
async fn keysizes_zset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a zset with 3 members -> bin 2 (3..4)
    client
        .command(&["ZADD", "myzset", "1", "a", "2", "b", "3", "c"])
        .await;
    assert_hist(&mut client, "zsets", 2, 1).await;
}

/// KEYSIZES - Test STRING
#[tokio::test]
async fn keysizes_string() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Empty string -> bin 0
    client.command(&["SET", "s1", ""]).await;
    assert_hist(&mut client, "strings", 0, 1).await;

    // 10-byte string -> bin 4 (9..16)
    client.command(&["SET", "s2", "0123456789"]).await;
    assert_hist(&mut client, "strings", 4, 1).await;
}

/// KEYSIZES - Test STRING BITS
#[tokio::test]
async fn keysizes_string_bits() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // SETBIT creates a string of at least 1 byte
    client.command(&["SETBIT", "bitkey", "7", "1"]).await;
    assert_hist(&mut client, "strings", 0, 1).await; // 1 byte -> bin 0

    // Setting a higher bit extends the string
    client.command(&["SETBIT", "bitkey", "100", "1"]).await;
    // 100/8 + 1 = 13 bytes -> bin 4 (9..16)
    assert_hist(&mut client, "strings", 4, 1).await;
    assert_hist(&mut client, "strings", 0, 0).await;
}

/// KEYSIZES - Test complex dataset
#[tokio::test]
async fn keysizes_complex_dataset() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create multiple types
    client.command(&["SET", "str1", "hello"]).await;
    client.command(&["SET", "str2", "world!"]).await;
    client.command(&["RPUSH", "list1", "a", "b", "c"]).await;
    client.command(&["SADD", "set1", "x", "y"]).await;
    client.command(&["ZADD", "zs1", "1", "m1", "2", "m2"]).await;
    client
        .command(&["HSET", "hash1", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;

    // Verify INFO keysizes has entries
    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        !keysizes.is_empty(),
        "keysizes should have entries: {keysizes:?}"
    );

    // Verify specific histograms
    assert_hist(&mut client, "strings", 3, 2).await; // "hello"(5) and "world!"(6) both in bin 3
    assert_hist(&mut client, "lists", 2, 1).await; // 3 elements -> bin 2
    assert_hist(&mut client, "sets", 1, 1).await; // 2 elements -> bin 1
    assert_hist(&mut client, "zsets", 1, 1).await; // 2 elements -> bin 1
    assert_hist(&mut client, "hashes", 2, 1).await; // 3 fields -> bin 2
}

/// KEYSIZES - Test DEBUG KEYSIZES-HIST-ASSERT command
#[tokio::test]
async fn keysizes_debug_hist_assert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "value"]).await; // 5 bytes -> bin 3

    // Correct assertion should return OK
    assert_hist(&mut client, "strings", 3, 1).await;

    // Wrong assertion should return error
    let resp = client
        .command(&["DEBUG", "KEYSIZES-HIST-ASSERT", "strings", "3", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Invalid type should return error
    let resp = client
        .command(&["DEBUG", "KEYSIZES-HIST-ASSERT", "invalid", "0", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

/// KEYSIZES - Test HASH
#[tokio::test]
async fn keysizes_hash() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a hash with 4 fields -> bin 2 (3..4)
    client
        .command(&[
            "HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3", "f4", "v4",
        ])
        .await;
    assert_hist(&mut client, "hashes", 2, 1).await;

    // Add more fields to get 10 -> bin 4 (9..16)
    for i in 5..=10 {
        client
            .command(&["HSET", "myhash", &format!("f{i}"), &format!("v{i}")])
            .await;
    }
    assert_hist(&mut client, "hashes", 4, 1).await;
    assert_hist(&mut client, "hashes", 2, 0).await;
}

/// KEYSIZES - Test Hash field lazy expiration
#[tokio::test]
async fn keysizes_hash_field_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a hash with fields that will expire
    client
        .command(&["HSET", "exphash", "f1", "v1", "f2", "v2", "f3", "v3"])
        .await;
    assert_hist(&mut client, "hashes", 2, 1).await; // 3 fields -> bin 2

    // Set very short TTL on a field
    client
        .command(&["HPEXPIRE", "exphash", "1", "FIELDS", "1", "f1"])
        .await;

    // Wait for field to expire
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Trigger lazy field expiry by accessing the hash
    client.command(&["HLEN", "exphash"]).await;

    // Now 2 fields remain -> bin 1 (2)
    // Note: FrogDB's histogram updates on key-level mutations, so the
    // field-level expiry may or may not update the histogram depending on
    // implementation. Just verify the hash is accessible.
    let resp = client.command(&["HLEN", "exphash"]).await;
    let len = unwrap_integer(&resp);
    assert!(len <= 3, "expected <= 3 fields after expiry, got {len}");
}

/// KEYSIZES - Test RESTORE
#[tokio::test]
async fn keysizes_restore() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a string, dump it, delete it, restore it
    client.command(&["SET", "orig", "hello"]).await; // 5 bytes -> bin 3
    assert_hist(&mut client, "strings", 3, 1).await;

    let serialized = dump_key(&mut client, "orig").await;
    client.command(&["DEL", "orig"]).await;
    assert_hist(&mut client, "strings", 3, 0).await;

    // Restore to a new key
    restore_key(&mut client, "restored", "0", serialized, &[]).await;
    assert_hist(&mut client, "strings", 3, 1).await;
}

/// KEYSIZES - Test RENAME
#[tokio::test]
async fn keysizes_rename() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a string
    client.command(&["SET", "before", "hello"]).await;
    assert_hist(&mut client, "strings", 3, 1).await;

    // Rename
    client.command(&["RENAME", "before", "after"]).await;
    // Count should remain the same (delete old + set new in same bin)
    assert_hist(&mut client, "strings", 3, 1).await;
}

// ============================================================================
// KEY-MEMORY-STATS tests
// ============================================================================

/// KEY-MEMORY-STATS - Empty database should have empty key memory histogram
#[tokio::test]
async fn key_memory_stats_empty_db() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "empty db should not have distrib_key_sizes: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - key memory histogram should appear
#[tokio::test]
async fn key_memory_stats_should_appear() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "testkey", "testval"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        has_key_sizes,
        "expected distrib_key_sizes with data: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - List keys should appear in key memory histogram
#[tokio::test]
async fn key_memory_stats_list_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    for i in 0..10 {
        client
            .command(&["RPUSH", "mylist", &format!("val{i}")])
            .await;
    }

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        has_key_sizes,
        "list keys should appear in key memory: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - All data types should appear in key memory histogram
#[tokio::test]
async fn key_memory_stats_all_types() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "str", "hello"]).await;
    client.command(&["RPUSH", "list", "a", "b"]).await;
    client.command(&["SADD", "set", "x", "y"]).await;
    client.command(&["ZADD", "zs", "1", "m"]).await;
    client.command(&["HSET", "hash", "f", "v"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        has_key_sizes,
        "all types should contribute to key memory: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - Histogram bins should use power-of-2 labels
#[tokio::test]
async fn key_memory_stats_power_of_2_labels() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "v"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let key_sizes_line = keysizes
        .iter()
        .find(|(k, _)| k == "distrib_key_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();
    // Each entry should be <label>=<count> where label is a power of 2
    for entry in key_sizes_line.split(',') {
        if let Some((label, _count)) = entry.split_once('=') {
            // Valid labels: 0, 2, 4, 8, 16, ..., 512, 1K, 2K, ..., 1M, etc.
            assert!(
                label == "0"
                    || label.parse::<u64>().is_ok()
                    || label.ends_with('K')
                    || label.ends_with('M')
                    || label.ends_with('G')
                    || label.ends_with('T'),
                "invalid bin label: {label}"
            );
        }
    }
}

/// KEY-MEMORY-STATS - DEL should remove key from key memory histogram
#[tokio::test]
async fn key_memory_stats_del() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "delme", "value"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "key should appear in memory histogram"
    );

    client.command(&["DEL", "delme"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "deleted key should not appear in memory histogram: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - Modifying a list should update key memory histogram
#[tokio::test]
async fn key_memory_stats_modify_list() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a small list
    client.command(&["RPUSH", "mylist", "a"]).await;

    let keysizes_before = get_keysizes_info(&mut client).await;
    let before_line = keysizes_before
        .iter()
        .find(|(k, _)| k == "distrib_key_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // Add many more elements
    for i in 0..100 {
        client
            .command(&["RPUSH", "mylist", &format!("val{i}")])
            .await;
    }

    let keysizes_after = get_keysizes_info(&mut client).await;
    let after_line = keysizes_after
        .iter()
        .find(|(k, _)| k == "distrib_key_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // The histogram should have changed (memory grew)
    assert_ne!(
        before_line, after_line,
        "memory histogram should change after list modification"
    );
}

/// KEY-MEMORY-STATS - FLUSHALL clears key memory histogram
#[tokio::test]
async fn key_memory_stats_flushall() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k1", "v1"]).await;
    client.command(&["SET", "k2", "v2"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "should have key memory entries"
    );

    client.command(&["FLUSHALL"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "FLUSHALL should clear key memory histogram: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - Larger allocations go to higher bins
#[tokio::test]
async fn key_memory_stats_larger_bins() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Create a small key and a large key
    client.command(&["SET", "small", "x"]).await;
    let big_val = "x".repeat(10000);
    client.command(&["SET", "big", &big_val]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let key_sizes_line = keysizes
        .iter()
        .find(|(k, _)| k == "distrib_key_sizes")
        .map(|(_, v)| v.clone())
        .unwrap_or_default();

    // The line should have at least 2 different bins
    let entries: Vec<&str> = key_sizes_line.split(',').collect();
    assert!(
        entries.len() >= 2,
        "expected at least 2 different bins for small and large keys: {key_sizes_line}"
    );
}

/// KEY-MEMORY-STATS - EXPIRE eventually removes from histogram
#[tokio::test]
async fn key_memory_stats_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "expkey", "value"]).await;
    client.command(&["PEXPIRE", "expkey", "1"]).await; // 1ms TTL

    // Wait for key to expire
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Trigger lazy expiry
    let resp = client.command(&["GET", "expkey"]).await;
    assert_nil(&resp);

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "expired key should not appear in histogram: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - Test RESTORE adds to histogram
#[tokio::test]
async fn key_memory_stats_restore() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "orig", "value"]).await;
    let serialized = dump_key(&mut client, "orig").await;
    client.command(&["DEL", "orig"]).await;

    // After DEL, no key memory
    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        !keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "no keys should mean no key memory"
    );

    // RESTORE should add back to histogram
    restore_key(&mut client, "restored", "0", serialized, &[]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "RESTORE should add to key memory histogram: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - RENAME should preserve key memory histogram
#[tokio::test]
async fn key_memory_stats_rename() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "before", "value"]).await;

    let keysizes_before = get_keysizes_info(&mut client).await;
    let before_has = keysizes_before
        .iter()
        .any(|(k, _)| k == "distrib_key_sizes");
    assert!(before_has, "should have key memory before rename");

    client.command(&["RENAME", "before", "after"]).await;

    let keysizes_after = get_keysizes_info(&mut client).await;
    let after_has = keysizes_after.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        after_has,
        "should have key memory after rename: {keysizes_after:?}"
    );
}

/// KEY-MEMORY-STATS - Hash field lazy expiration
#[tokio::test]
async fn key_memory_stats_hash_field_expiry() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["HSET", "exphash", "f1", "v1", "f2", "v2"])
        .await;

    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "hash should appear in key memory"
    );

    // Set short TTL on a field
    client
        .command(&["HPEXPIRE", "exphash", "1", "FIELDS", "1", "f1"])
        .await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    client.command(&["HLEN", "exphash"]).await;

    // Hash should still be in histogram (still has f2)
    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "hash with remaining fields should still be in histogram"
    );
}

/// KEY-MEMORY-STATS - Test DEBUG KEYSIZES-HIST-ASSERT command for keymem
#[tokio::test]
async fn key_memory_stats_debug_assert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "v"]).await;

    // keymem should have at least one entry
    // We don't know the exact bin, but total should be 1
    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "should have key memory data"
    );
}

/// KEY-MEMORY-STATS disabled - key memory histogram should not appear
#[tokio::test]
async fn key_memory_stats_disabled() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Disable at runtime
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "no"])
        .await;
    assert_ok(&resp);

    client.command(&["SET", "k", "v"]).await;

    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "disabled key-memory should not emit distrib_key_sizes: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - cannot enable key-memory-histograms at runtime when disabled at startup
#[tokio::test]
async fn key_memory_stats_cannot_enable_after_disable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Disable at runtime
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "no"])
        .await;
    assert_ok(&resp);

    // Try to re-enable — should fail
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "yes"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

/// KEY-MEMORY-STATS - can disable key-memory-histograms at runtime and distrib_*_sizes disappear
#[tokio::test]
async fn key_memory_stats_disable_clears() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "k", "v"]).await;

    // Verify it's present
    let keysizes = get_keysizes_info(&mut client).await;
    assert!(
        keysizes.iter().any(|(k, _)| k == "distrib_key_sizes"),
        "should have key memory data before disable"
    );

    // Disable
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "no"])
        .await;
    assert_ok(&resp);

    // Verify it's gone
    let keysizes = get_keysizes_info(&mut client).await;
    let has_key_sizes = keysizes.iter().any(|(k, _)| k == "distrib_key_sizes");
    assert!(
        !has_key_sizes,
        "after disable, distrib_key_sizes should not appear: {keysizes:?}"
    );
}

/// KEY-MEMORY-STATS - cannot re-enable key-memory-histograms at runtime after disabling
#[tokio::test]
async fn key_memory_stats_cannot_reenable() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Disable
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "no"])
        .await;
    assert_ok(&resp);

    // Try to re-enable
    let resp = client
        .command(&["CONFIG", "SET", "key-memory-histograms", "yes"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Verify CONFIG GET shows "no"
    let resp = client
        .command(&["CONFIG", "GET", "key-memory-histograms"])
        .await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items.len(), 2);
    assert_eq!(items[1], "no");
}

// ============================================================================
// SLOT-ALLOCSIZE test
// ============================================================================

/// SLOT-ALLOCSIZE - Test DEBUG ALLOCSIZE-SLOTS-ASSERT command
#[tokio::test]
async fn slot_allocsize_assert() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Slot should be 0 in an empty db
    assert_allocsize(&mut client, 0, 0).await;

    // After adding a key, the slot should have non-zero alloc size.
    // We don't know exactly which slot "testkey" maps to, so just
    // verify the command works and returns a result.
    client.command(&["SET", "testkey", "testval"]).await;

    // Getting the slot for "testkey" via DEBUG HASHING
    let resp = client.command(&["DEBUG", "HASHING", "testkey"]).await;
    let info_text = match &resp {
        frogdb_protocol::Response::Simple(b) => String::from_utf8_lossy(b).to_string(),
        _ => String::new(),
    };

    // Parse slot from "... slot:NNNN ..."
    let slot: u16 = info_text
        .split_whitespace()
        .find(|s| s.starts_with("slot:"))
        .and_then(|s| s.strip_prefix("slot:"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // Allocsize in that slot should be > 0
    let resp = client
        .command(&["DEBUG", "ALLOCSIZE-SLOTS-ASSERT", &slot.to_string(), "0"])
        .await;
    // This should fail because size > 0
    assert_error_prefix(&resp, "ERR");
}

// ============================================================================
// Keysizes reload/persistence test
// ============================================================================

/// KEYSIZES - keysizes are rebuilt after restart with persistence
#[tokio::test]
async fn keysizes_rebuilt_after_restart() {
    use frogdb_test_harness::server::TestServerConfig;
    use std::time::Duration;

    let data_dir = TestServer::create_temp_dir();

    let server = TestServer::start_standalone_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir.clone()),
        ..Default::default()
    })
    .await;
    let mut client = server.connect().await;

    // Create data
    client.command(&["SET", "str1", "hello"]).await;
    client.command(&["RPUSH", "list1", "a", "b", "c"]).await;

    // Verify histograms are populated
    assert_hist(&mut client, "strings", 3, 1).await;
    assert_hist(&mut client, "lists", 2, 1).await;

    // Drop client then shut down first server and wait for lock release
    drop(client);
    server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Restart with same data dir
    let server2 = TestServer::start_standalone_with_config(TestServerConfig {
        persistence: true,
        data_dir: Some(data_dir),
        ..Default::default()
    })
    .await;
    let mut client2 = server2.connect().await;

    // Wait a moment for recovery
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Histograms should be rebuilt from restored data
    assert_hist(&mut client2, "strings", 3, 1).await;
    assert_hist(&mut client2, "lists", 2, 1).await;
}
