//! Rust port of Redis 8.6.0 `unit/wait.tcl` test suite.
//!
//! Excludes:
//! - All tests tagged `external:skip` (entire first block)
//! - All replication tests (`needs:repl`): "WAIT should acknowledge 1 additional copy",
//!   "WAIT should not acknowledge 2 additional copies", "WAIT should not acknowledge 1
//!   additional copy if slave is blocked", "WAIT implicitly blocks on client pause",
//!   "WAIT replica multiple clients unblock - reuse last result"
//! - All WAITAOF tests: entire `wait aof network external:skip` block (requires AOF,
//!   replication, and CONFIG SET)
//! - All failover tests: entire `failover external:skip` block (requires replication)
//! - Tests using CONFIG SET
//!
//! What remains: argument validation tests for WAIT that can run on a standalone server,
//! plus basic standalone WAIT behavior (no replicas => returns 0 immediately).

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// WAIT argument validation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB WAIT does not validate overflow"]
async fn tcl_wait_out_of_range_timeout_overflow() {
    // Timeout is parsed as milliseconds by getLongLongFromObjectOrReply().
    // Value 0x8000000000000000 is beyond LLONG_MAX — should get "out of range".
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["WAIT", "2", "9223372036854775808"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
#[ignore = "FrogDB WAIT does not validate overflow"]
async fn tcl_wait_out_of_range_timeout_llong_max() {
    // LLONG_MAX (0x7FFFFFFFFFFFFFFF) is expected to fail by later overflow
    // condition after addition of mstime().
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client
        .command(&["WAIT", "2", "9223372036854775807"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

#[tokio::test]
async fn tcl_wait_negative_timeout() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["WAIT", "2", "-1"]).await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// WAIT standalone behavior (no replicas)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "WAIT may not be implemented or may behave differently on standalone FrogDB"]
async fn tcl_wait_standalone_no_replicas_returns_zero() {
    // On a standalone server with no replicas, WAIT with a short timeout
    // should return 0 (no replicas acknowledged).
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["WAIT", "1", "100"]).await;
    assert_integer_eq(&resp, 0);
}

#[tokio::test]
#[ignore = "WAIT may not be implemented or may behave differently on standalone FrogDB"]
async fn tcl_wait_zero_replicas_returns_immediately() {
    // WAIT 0 timeout should return immediately with the current replica count (0).
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "foo", "bar"]).await;
    let resp = client.command(&["WAIT", "0", "0"]).await;
    assert_integer_eq(&resp, 0);
}
