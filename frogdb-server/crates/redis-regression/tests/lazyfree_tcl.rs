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
//! Tests that require the `lazyfreed_objects` / `lazyfree_pending_objects`
//! counters in `INFO memory` (FrogDB reports these as constant 0):
//! - `lazy free a stream with all types of metadata` — needs:config-resetstat
//!   and lazyfreed_objects counter
//! - `lazy free a stream with deleted cgroup` — needs:config-resetstat and
//!   lazyfreed_objects counter
//! - `FLUSHALL SYNC optimized to run in bg as blocking FLUSHALL ASYNC` —
//!   lazyfreed_objects counter
//! - `Run consecutive blocking FLUSHALL ASYNC successfully` — lazyfreed_objects
//!   counter
//! - `FLUSHALL SYNC in MULTI not optimized to run as blocking FLUSHALL ASYNC` —
//!   lazyfreed_objects / lazyfree_pending_objects counters
//! - `Client closed in the middle of blocking FLUSHALL ASYNC` —
//!   lazyfreed_objects counter
//! - `Pending commands in querybuf processed once unblocking FLUSHALL ASYNC` —
//!   lazyfreed_objects counter
//! - `Unblocks client blocked on lazyfree via REPLICAOF command` — external:skip
//!   and lazyfree_pending_objects counter

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
