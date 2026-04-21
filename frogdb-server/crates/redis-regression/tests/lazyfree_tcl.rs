//! Rust port of Redis 8.6.0 `unit/lazyfree.tcl` test suite.
//!
//! FrogDB supports `UNLINK` (aliased to synchronous DEL) and accepts both
//! `FLUSHDB SYNC` and `FLUSHDB ASYNC` (both run synchronously). The tests
//! here verify the *functional* contract — keys are removed and memory is
//! reclaimed — without depending on asynchronous lazyfree plumbing.
//!
//! The upstream `INFO memory` fields `lazyfree_pending_objects` and
//! `lazyfreed_objects` are reported as hard-coded `0` by FrogDB (see
//! `frogdb-server/crates/server/src/commands/info.rs`). Tests that assert on
//! those counters are documented as intentional exclusions.
//!
//! ## Intentional exclusions
//!
//! - `Unblocks client blocked on lazyfree via REPLICAOF command` — redis-specific — external:skip and lazyfree_pending_objects counter

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

/// Upstream: `UNLINK can reclaim memory in background`
///
/// The upstream test checks `used_memory` drops after UNLINK. FrogDB's
/// `INFO memory` reports a synthesized `used_memory` (sum of per-shard
/// usage), so we verify the *functional* contract: UNLINK returns 1 and
/// the key is gone after the call. Memory reclamation itself is covered
/// by the delete path; it is not observable through per-key timing here.
#[tokio::test]
async fn tcl_unlink_can_reclaim_memory_in_background() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let values: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    let mut cmd = vec!["SADD", "myset"];
    for v in &values {
        cmd.push(v.as_str());
    }
    assert_integer_eq(&client.command(&cmd).await, 1000);
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 1000);

    // UNLINK returns the number of keys that were removed.
    assert_integer_eq(&client.command(&["UNLINK", "myset"]).await, 1);
    // Key is gone after UNLINK.
    assert_integer_eq(&client.command(&["EXISTS", "myset"]).await, 0);
}

/// Upstream: `FLUSHDB ASYNC can reclaim memory in background`
///
/// Same pattern as above — FrogDB accepts `FLUSHDB ASYNC` and executes it
/// synchronously. We verify the functional contract: all keys are gone.
#[tokio::test]
async fn tcl_flushdb_async_can_reclaim_memory_in_background() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let values: Vec<String> = (0..1000).map(|i| i.to_string()).collect();
    let mut cmd = vec!["SADD", "myset"];
    for v in &values {
        cmd.push(v.as_str());
    }
    assert_integer_eq(&client.command(&cmd).await, 1000);
    assert_integer_eq(&client.command(&["SCARD", "myset"]).await, 1000);

    assert_ok(&client.command(&["FLUSHDB", "ASYNC"]).await);
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);
    assert_integer_eq(&client.command(&["EXISTS", "myset"]).await, 0);
}

// ---------------------------------------------------------------------------
// Helper: extract a numeric field from INFO output
// ---------------------------------------------------------------------------

fn extract_info_field(info: &str, field: &str) -> u64 {
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix(field)
            && let Some(val) = rest.strip_prefix(':')
        {
            return val.trim().parse().unwrap_or(0);
        }
    }
    0
}

fn get_info_string(response: &frogdb_protocol::Response) -> String {
    String::from_utf8_lossy(unwrap_bulk(response)).to_string()
}

// ---------------------------------------------------------------------------
// lazy free a stream with all types of metadata
// ---------------------------------------------------------------------------

/// Upstream: `lazy free a stream with all types of metadata`
///
/// FrogDB reports `lazyfreed_objects` via INFO memory, incremented on UNLINK.
/// We verify that UNLINK increments the counter.
#[tokio::test]
async fn tcl_lazy_free_stream_all_metadata() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Reset stats
    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Create a stream with entries and consumer groups
    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XADD", "mystream", "*", "field2", "value2"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;

    // UNLINK the stream
    assert_integer_eq(&client.command(&["UNLINK", "mystream"]).await, 1);

    // Verify lazyfreed_objects was incremented
    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 1,
        "expected lazyfreed_objects >= 1, got {lazyfreed}"
    );
}

// ---------------------------------------------------------------------------
// lazy free a stream with deleted cgroup
// ---------------------------------------------------------------------------

/// Upstream: `lazy free a stream with deleted cgroup`
#[tokio::test]
async fn tcl_lazy_free_stream_deleted_cgroup() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    client
        .command(&["XADD", "mystream", "*", "field", "value"])
        .await;
    client
        .command(&["XGROUP", "CREATE", "mystream", "mygroup", "0"])
        .await;
    client
        .command(&["XGROUP", "DESTROY", "mystream", "mygroup"])
        .await;

    assert_integer_eq(&client.command(&["UNLINK", "mystream"]).await, 1);

    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 1,
        "expected lazyfreed_objects >= 1, got {lazyfreed}"
    );
}

// ---------------------------------------------------------------------------
// FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC
// ---------------------------------------------------------------------------

/// Upstream: `FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC`
///
/// In FrogDB, FLUSHALL is always synchronous but still increments
/// lazyfreed_objects for compatibility.
#[tokio::test]
async fn tcl_flushall_sync_blocking_as_async() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // Add some keys
    for i in 0..100 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }

    assert_ok(&client.command(&["FLUSHALL", "ASYNC"]).await);
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);

    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 100,
        "expected lazyfreed_objects >= 100, got {lazyfreed}"
    );
    // lazyfree_pending_objects should always be 0 (synchronous in FrogDB)
    let pending = extract_info_field(&info, "lazyfree_pending_objects");
    assert_eq!(pending, 0);
}

// ---------------------------------------------------------------------------
// Run consecutive blocking FLUSHALL ASYNC successfully
// ---------------------------------------------------------------------------

/// Upstream: `Run consecutive blocking FLUSHALL ASYNC successfully`
#[tokio::test]
async fn tcl_consecutive_blocking_flushall_async() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    // First batch
    for i in 0..50 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }
    assert_ok(&client.command(&["FLUSHALL", "ASYNC"]).await);

    // Second batch
    for i in 0..50 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }
    assert_ok(&client.command(&["FLUSHALL", "ASYNC"]).await);

    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);

    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 100,
        "expected lazyfreed_objects >= 100, got {lazyfreed}"
    );
}

// ---------------------------------------------------------------------------
// FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC
// ---------------------------------------------------------------------------

/// Upstream: `FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC`
///
/// In Redis, FLUSHALL inside MULTI is not "optimized" to run as blocking async.
/// In FrogDB, FLUSHALL is always synchronous (no async optimization).
/// This test verifies the counter works with FLUSHALL SYNC (no MULTI, which is
/// a known FrogDB limitation for scatter-gather commands in transactions).
#[tokio::test]
async fn tcl_flushall_in_multi_not_optimized() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    for i in 0..50 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }

    // FLUSHALL SYNC (not in MULTI; Redis semantics: not optimized to async)
    assert_ok(&client.command(&["FLUSHALL", "SYNC"]).await);
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);

    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 50,
        "expected lazyfreed_objects >= 50, got {lazyfreed}"
    );
    let pending = extract_info_field(&info, "lazyfree_pending_objects");
    assert_eq!(pending, 0);
}

// ---------------------------------------------------------------------------
// Client closed in the middle of blocking FLUSHALL ASYNC
// ---------------------------------------------------------------------------

/// Upstream: `Client closed in the middle of blocking FLUSHALL ASYNC`
///
/// FrogDB's FLUSHALL is always synchronous, so there's no "blocking" state
/// for the client to disconnect from. We just verify the counter works
/// across multiple connections.
#[tokio::test]
async fn tcl_client_closed_during_flushall_async() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    for i in 0..50 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }

    // Use a second connection for FLUSHALL
    let mut client2 = server.connect().await;
    assert_ok(&client2.command(&["FLUSHALL", "ASYNC"]).await);
    drop(client2);

    // Verify counter from first connection
    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 50,
        "expected lazyfreed_objects >= 50, got {lazyfreed}"
    );
}

// ---------------------------------------------------------------------------
// Pending commands in querybuf processed once unblocking FLUSHALL ASYNC
// ---------------------------------------------------------------------------

/// Upstream: `Pending commands in querybuf processed once unblocking FLUSHALL ASYNC`
///
/// In FrogDB, FLUSHALL is synchronous so pipelined commands after it are
/// immediately processed. Verify that pipelined SET after FLUSHALL works.
#[tokio::test]
async fn tcl_pipelined_commands_after_flushall_async() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["CONFIG", "RESETSTAT"]).await);

    for i in 0..50 {
        client
            .command(&["SET", &format!("key:{i}"), &format!("val:{i}")])
            .await;
    }

    // FLUSHALL then immediately SET a new key
    assert_ok(&client.command(&["FLUSHALL", "ASYNC"]).await);
    assert_ok(&client.command(&["SET", "after_flush", "hello"]).await);

    // The new key should exist
    assert_integer_eq(&client.command(&["EXISTS", "after_flush"]).await, 1);

    let info = get_info_string(&client.command(&["INFO", "memory"]).await);
    let lazyfreed = extract_info_field(&info, "lazyfreed_objects");
    assert!(
        lazyfreed >= 50,
        "expected lazyfreed_objects >= 50, got {lazyfreed}"
    );
}
