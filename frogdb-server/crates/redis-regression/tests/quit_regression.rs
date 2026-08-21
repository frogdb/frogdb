use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

#[tokio::test]
async fn quit_returns_ok() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["QUIT"]).await;
    assert_ok(&resp);
}

#[tokio::test]
async fn connection_closes_after_quit() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["QUIT"]).await);

    // After QUIT, reading another response should time out or return None
    let resp = client.read_response(Duration::from_millis(200)).await;
    assert!(resp.is_none(), "connection should be closed after QUIT");
}

#[tokio::test]
async fn quit_pipelined_commands_after_quit_exceed_read_buffer() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // Pipeline QUIT followed by a SET with a large value (exceeds typical read buffer)
    client.send_only(&["QUIT"]).await;
    let big_value = "x".repeat(1024);
    client.send_only(&["SET", "foo", &big_value]).await;

    // Should get OK for QUIT
    let resp = client.read_response(Duration::from_secs(2)).await;
    assert!(resp.is_some(), "should get QUIT response");
    assert_ok(&resp.unwrap());

    // Connection should be closed — SET should not execute
    let resp2 = client.read_response(Duration::from_millis(200)).await;
    assert!(resp2.is_none(), "connection should be closed after QUIT");

    // Verify SET was not executed
    let mut client2 = server.connect().await;
    assert_nil(&client2.command(&["GET", "foo"]).await);
}

/// Regression: upstream's `QUIT` arity is `-1` — `quitCommand` ignores whatever
/// follows the command name — but FrogDB registered `Fixed(0)` and answered a
/// wrong-arity error, so a client sending trailing junk never got its +OK or a
/// clean close. Found by the vendored-metadata arity cross-check.
#[tokio::test]
async fn quit_tolerates_trailing_arguments() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["QUIT", "ignored"]).await);

    let resp = client.read_response(Duration::from_millis(200)).await;
    assert!(resp.is_none(), "connection should be closed after QUIT");
}
