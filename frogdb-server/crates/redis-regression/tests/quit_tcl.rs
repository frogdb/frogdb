//! Rust port of Redis 8.6.0 `unit/quit.tcl` test suite.
//!
//! All 3 upstream tests are ported (no exclusions).

use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

#[tokio::test]
async fn tcl_quit_returns_ok() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["QUIT"]).await);

    // After QUIT the connection is closed — subsequent commands should fail.
    let resp = client.read_response(Duration::from_millis(500)).await;
    assert!(resp.is_none(), "connection should be closed after QUIT");
}

#[tokio::test]
async fn tcl_pipelined_commands_after_quit_must_not_be_executed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Pipeline: QUIT followed by SET foo bar
    client.send_only(&["QUIT"]).await;
    client.send_only(&["SET", "foo", "bar"]).await;

    // Should get OK for QUIT
    let resp = client.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "should get QUIT response");
    assert_ok(&resp.unwrap());

    // Connection should be closed — no response for SET
    let resp2 = client.read_response(Duration::from_millis(500)).await;
    assert!(resp2.is_none(), "connection should be closed after QUIT");

    // Reconnect and verify SET was not executed
    let mut client2 = server.connect().await;
    assert_nil(&client2.command(&["GET", "foo"]).await);
}

#[tokio::test]
async fn tcl_pipelined_commands_after_quit_exceed_read_buffer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Pipeline: QUIT followed by SET with a 1024-byte value
    client.send_only(&["QUIT"]).await;
    let big_value = "x".repeat(1024);
    client.send_only(&["SET", "foo", &big_value]).await;

    // Should get OK for QUIT
    let resp = client.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "should get QUIT response");
    assert_ok(&resp.unwrap());

    // Connection should be closed — SET should not execute
    let resp2 = client.read_response(Duration::from_millis(500)).await;
    assert!(resp2.is_none(), "connection should be closed after QUIT");

    // Reconnect and verify SET was not executed
    let mut client2 = server.connect().await;
    assert_nil(&client2.command(&["GET", "foo"]).await);
}
