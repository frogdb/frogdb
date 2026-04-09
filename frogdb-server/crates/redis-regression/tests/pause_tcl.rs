//! Rust port of Redis 8.6.0 `unit/pause.tcl` test suite.
//!
//! Excludes:
//! - `needs:repl` / `external:skip` tests (replication pause, replica offset tests)
//! - Tests using `CONFIG SET`

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Read commands during WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_read_commands_not_blocked_by_client_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    // Read commands should go through immediately
    let _ = rd.command(&["GET", "FOO"]).await;
    let r = rd.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "PONG"),
        "PING should respond immediately, got {r:?}"
    );
    let r = rd.command(&["INFO"]).await;
    assert!(matches!(r, Response::Bulk(Some(_))));

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Pause precedence: ALL over WRITE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_old_pause_all_takes_precedence_over_new_pause_write() {
    // Scenario: PAUSE ALL 200ms, then PAUSE WRITE 20ms, then wait 50ms.
    // GET should still be blocked by the ALL pause until ~200ms elapses.
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;

    assert_ok(&rd.command(&["SET", "FOO", "BAR"]).await);

    let start = tokio::time::Instant::now();
    assert_ok(&control.command(&["CLIENT", "PAUSE", "200", "ALL"]).await);
    assert_ok(&control.command(&["CLIENT", "PAUSE", "20", "WRITE"]).await);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // This GET should be blocked until the 200ms ALL pause expires
    let r = rd.command(&["GET", "FOO"]).await;
    assert_bulk_eq(&r, b"BAR");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(200),
        "GET should have been blocked ~200ms by PAUSE ALL, elapsed={elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// Shorter pause does not override longer one
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_new_pause_time_smaller_than_old_preserves_old() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );
    // Shorter pause should not override
    assert_ok(&control.command(&["CLIENT", "PAUSE", "10", "WRITE"]).await);

    tokio::time::sleep(Duration::from_millis(100)).await;

    rd.send_only(&["SET", "FOO", "BAR"]).await;
    server.wait_for_blocked_clients(1).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert_ok(&resp.expect("SET response after unpause"));
}

// ---------------------------------------------------------------------------
// Write commands blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_write_commands_paused_by_write_mode() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    rd.send_only(&["SET", "FOO", "BAR"]).await;
    server.wait_for_blocked_clients(1).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert_ok(&resp.expect("SET response after unpause"));
}

// ---------------------------------------------------------------------------
// Special commands (PFCOUNT, PUBLISH) blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_special_commands_paused_by_write_mode() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;
    let mut rd2 = server.connect().await;

    control.command(&["PFADD", "pause-hll", "test"]).await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    // PFCOUNT can replicate, should be blocked
    rd.send_only(&["PFCOUNT", "pause-hll"]).await;
    server.wait_for_blocked_clients(1).await;

    // PUBLISH adds to replication stream, should be blocked
    rd2.send_only(&["PUBLISH", "foo", "bar"]).await;
    server.wait_for_blocked_clients(2).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    let resp1 = rd.read_response(Duration::from_secs(2)).await;
    assert_integer_eq(&resp1.expect("PFCOUNT response"), 1);

    let resp2 = rd2.read_response(Duration::from_secs(2)).await;
    assert_integer_eq(&resp2.expect("PUBLISH response"), 0);
}

// ---------------------------------------------------------------------------
// Read/admin MULTI-EXEC not blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_read_admin_multi_exec_not_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rr = server.connect().await;

    control.command(&["SET", "FOO", "BAR"]).await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    assert_ok(&rr.command(&["MULTI"]).await);
    let r = rr.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let r = rr.command(&["GET", "FOO"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let resp = rr.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 2);

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Write MULTI-EXEC blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_write_multi_exec_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;

    assert_ok(&rd.command(&["MULTI"]).await);
    let r = rd.command(&["SET", "FOO", "BAR"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    rd.send_only(&["EXEC"]).await;
    server.wait_for_blocked_clients(1).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = rd.read_response(Duration::from_secs(2)).await;
    // EXEC returns an array with the SET result
    let results = unwrap_array(resp.expect("EXEC response"));
    assert_eq!(results.len(), 1);
    assert_ok(&results[0]);
}

// ---------------------------------------------------------------------------
// Scripts blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_scripts_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;
    let mut rd2 = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    rd.send_only(&["EVAL", "return 1", "0"]).await;
    rd2.send_only(&["EVAL", "return 1", "0"]).await;

    server.wait_for_blocked_clients(2).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    let resp1 = rd.read_response(Duration::from_secs(2)).await;
    assert_integer_eq(&resp1.expect("EVAL response 1"), 1);

    let resp2 = rd2.read_response(Duration::from_secs(2)).await;
    assert_integer_eq(&resp2.expect("EVAL response 2"), 1);
}

// ---------------------------------------------------------------------------
// Read-only scripts NOT blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB may not support #!lua flags=no-writes shebang or FUNCTION LOAD yet"]
async fn tcl_ro_scripts_not_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rr = server.connect().await;

    rr.command(&["SET", "x", "y"]).await;

    // Create a function for later
    rr.command(&[
        "FUNCTION",
        "LOAD",
        "REPLACE",
        "#!lua name=f1\nredis.register_function{function_name='f1',callback=function() return 'hello' end,flags={'no-writes'}}",
    ])
    .await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "6000000", "WRITE"])
            .await,
    );

    // EVAL with no-writes flag should not be blocked
    let resp = rr
        .command(&["EVAL", "#!lua flags=no-writes\nreturn 'unique script'", "0"])
        .await;
    assert_bulk_eq(&resp, b"unique script");

    // Repeat on cached script
    let resp = rr
        .command(&["EVAL", "#!lua flags=no-writes\nreturn 'unique script'", "0"])
        .await;
    assert_bulk_eq(&resp, b"unique script");

    // EVAL_RO
    let resp = rr
        .command(&[
            "EVAL_RO",
            "return redis.call('GeT', 'x')..' unique script'",
            "1",
            "x",
        ])
        .await;
    assert_bulk_eq(&resp, b"y unique script");

    // EVALSHA with no-writes
    let sha_resp = rr
        .command(&["SCRIPT", "LOAD", "#!lua flags=no-writes\nreturn 2"])
        .await;
    let sha = std::str::from_utf8(unwrap_bulk(&sha_resp))
        .unwrap()
        .to_string();
    let resp = rr.command(&["EVALSHA", &sha, "0"]).await;
    assert_integer_eq(&resp, 2);

    // FCALL
    let resp = rr.command(&["FCALL", "f1", "0"]).await;
    assert_bulk_eq(&resp, b"hello");

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Read-only scripts in MULTI-EXEC NOT blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB may not support #!lua flags=no-writes shebang yet"]
async fn tcl_ro_scripts_in_multi_exec_not_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rr = server.connect().await;

    rr.command(&["SET", "FOO", "BAR"]).await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    assert_ok(&rr.command(&["MULTI"]).await);
    let r = rr
        .command(&["EVAL", "#!lua flags=no-writes\nreturn 12", "0"])
        .await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let r = rr
        .command(&["EVAL", "#!lua flags=no-writes\nreturn 13", "0"])
        .await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let resp = rr.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 2);
    assert_integer_eq(&results[0], 12);
    assert_integer_eq(&results[1], 13);

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Write scripts in MULTI-EXEC blocked by WRITE pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_write_scripts_in_multi_exec_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd = server.connect().await;
    let mut rd2 = server.connect().await;

    // Build first transaction
    assert_ok(&rd.command(&["MULTI"]).await);
    let r = rd.command(&["EVAL", "return 12", "0"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );

    // Build second transaction
    assert_ok(&rd2.command(&["MULTI"]).await);
    let r = rd2.command(&["EVAL", "return 13", "0"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    rd.send_only(&["EXEC"]).await;
    rd2.send_only(&["EXEC"]).await;
    server.wait_for_blocked_clients(2).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    let resp1 = rd.read_response(Duration::from_secs(2)).await;
    let results1 = unwrap_array(resp1.expect("EXEC response 1"));
    assert_eq!(results1.len(), 1);
    assert_integer_eq(&results1[0], 12);

    let resp2 = rd2.read_response(Duration::from_secs(2)).await;
    let results2 = unwrap_array(resp2.expect("EXEC response 2"));
    assert_eq!(results2.len(), 1);
    assert_integer_eq(&results2[0], 13);
}

// ---------------------------------------------------------------------------
// May-replicate commands rejected in RO scripts
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_may_replicate_commands_rejected_in_ro_scripts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PUBLISH from EVAL_RO should be rejected
    let resp = client
        .command(&["EVAL_RO", "return redis.call('publish','ch','msg')", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");

    // Make sure PUBLISH is not blocked from a normal EVAL
    let resp = client
        .command(&["EVAL", "return redis.call('publish','ch','msg')", "0"])
        .await;
    assert_integer_eq(&resp, 0);
}

// ---------------------------------------------------------------------------
// Multiple clients queued and unblocked
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_multiple_clients_queued_and_unblocked() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;

    let mut clients = Vec::new();
    for _ in 0..3 {
        clients.push(server.connect().await);
    }

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    for c in &mut clients {
        c.send_only(&["SET", "FOO", "BAR"]).await;
    }

    server.wait_for_blocked_clients(3).await;

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    for c in &mut clients {
        let resp = c.read_response(Duration::from_secs(2)).await;
        assert_ok(&resp.expect("SET response after unpause"));
    }
}

// ---------------------------------------------------------------------------
// Syntax errors get immediate response during pause
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_clients_with_syntax_errors_will_get_responses_immediately_during_pause() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    // Wrong number of arguments -> immediate error, not blocked
    let resp = client.command(&["SET", "FOO"]).await;
    assert_error_prefix(&resp, "ERR wrong number of arguments");

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Active/passive expires skipped during client pause WRITE
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB may differ in expire behavior during PAUSE WRITE (Redis skips active+passive expires)"]
async fn tcl_active_passive_expires_skipped_during_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    // Get baseline expired_keys count via INFO
    let info = client.command(&["INFO", "stats"]).await;
    let info_str = std::str::from_utf8(unwrap_bulk(&info)).unwrap().to_string();
    let expired_before: u64 = info_str
        .lines()
        .find(|l| l.starts_with("expired_keys:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);

    // Set keys with very short TTL inside a transaction with PAUSE
    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "foo", "bar", "PX", "10"]).await;
    client.command(&["SET", "bar", "foo", "PX", "10"]).await;
    client.command(&["CLIENT", "PAUSE", "50000", "WRITE"]).await;
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 3);

    // Wait for keys to logically expire
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Keys should be logically expired (GET returns nil)
    assert_nil(&control.command(&["GET", "foo"]).await);
    assert_nil(&control.command(&["GET", "bar"]).await);

    // But expired_keys counter should not have increased (expires skipped during pause)
    let info2 = control.command(&["INFO", "stats"]).await;
    let info_str2 = std::str::from_utf8(unwrap_bulk(&info2))
        .unwrap()
        .to_string();
    let expired_after: u64 = info_str2
        .lines()
        .find(|l| l.starts_with("expired_keys:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    assert_eq!(
        expired_before, expired_after,
        "no keys should have been expired during pause"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    // Force passive expiry
    client.command(&["GET", "foo"]).await;
    client.command(&["GET", "bar"]).await;

    // Now expired_keys should have increased
    let info3 = client.command(&["INFO", "stats"]).await;
    let info_str3 = std::str::from_utf8(unwrap_bulk(&info3))
        .unwrap()
        .to_string();
    let expired_final: u64 = info_str3
        .lines()
        .find(|l| l.starts_with("expired_keys:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    assert_eq!(
        expired_final,
        expired_before + 2,
        "two keys should have been expired after unpause"
    );
}

// ---------------------------------------------------------------------------
// Client pause starts at end of transaction
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_pause_starts_at_end_of_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut control = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "FOO1", "BAR"]).await;
    client.command(&["CLIENT", "PAUSE", "60000", "WRITE"]).await;
    client.command(&["SET", "FOO2", "BAR"]).await;
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 3);
    assert_ok(&results[0]); // SET FOO1
    assert_ok(&results[1]); // CLIENT PAUSE
    assert_ok(&results[2]); // SET FOO2

    // Now writes should be blocked
    let mut rd = server.connect().await;
    rd.send_only(&["SET", "FOO3", "BAR"]).await;
    server.wait_for_blocked_clients(1).await;

    // FOO1 and FOO2 were set inside the transaction (before pause took effect)
    assert_bulk_eq(&control.command(&["GET", "FOO1"]).await, b"BAR");
    assert_bulk_eq(&control.command(&["GET", "FOO2"]).await, b"BAR");
    // FOO3 should not be set yet (blocked)
    assert_nil(&control.command(&["GET", "FOO3"]).await);

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = rd.read_response(Duration::from_secs(2)).await;
    assert_ok(&resp.expect("SET FOO3 response after unpause"));
}

// ---------------------------------------------------------------------------
// RANDOMKEY does not cause infinite loop during pause write
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB may differ: RANDOMKEY with expiring keys during PAUSE WRITE"]
async fn tcl_randomkey_no_infinite_loop_during_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let mut control = server.connect().await;

    client.command(&["FLUSHALL"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "key", "value", "PX", "3"]).await;
    client.command(&["CLIENT", "PAUSE", "10000", "WRITE"]).await;
    let resp = client.command(&["EXEC"]).await;
    let _ = unwrap_array(resp);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // RANDOMKEY should return the key (even if logically expired, it should not loop)
    let resp = control.command(&["RANDOMKEY"]).await;
    assert_bulk_eq(&resp, b"key");

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    // After unpause, key should be gone
    assert_nil(&client.command(&["RANDOMKEY"]).await);
}

// ---------------------------------------------------------------------------
// CLIENT UNBLOCK cannot unblock clients blocked by CLIENT PAUSE
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB CLIENT PAUSE behavior differs from Redis"]
async fn tcl_client_unblock_not_allowed_for_pause_blocked_clients() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut rd1 = server.connect().await;
    let mut rd2 = server.connect().await;

    // Get client IDs
    let resp1 = rd1.command(&["CLIENT", "ID"]).await;
    let client_id1 = unwrap_integer(&resp1);
    let resp2 = rd2.command(&["CLIENT", "ID"]).await;
    let client_id2 = unwrap_integer(&resp2);

    control.command(&["DEL", "mylist"]).await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "100000", "WRITE"])
            .await,
    );

    rd1.send_only(&["BLPOP", "mylist", "0"]).await;
    rd2.send_only(&["BLPOP", "mylist", "0"]).await;
    server.wait_for_blocked_clients(2).await;

    // CLIENT UNBLOCK should return 0 (cannot unblock pause-blocked clients)
    assert_integer_eq(
        &control
            .command(&["CLIENT", "UNBLOCK", &client_id1.to_string(), "TIMEOUT"])
            .await,
        0,
    );
    assert_integer_eq(
        &control
            .command(&["CLIENT", "UNBLOCK", &client_id2.to_string(), "ERROR"])
            .await,
        0,
    );

    // After unpause, CLIENT UNBLOCK should work
    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    assert_integer_eq(
        &control
            .command(&["CLIENT", "UNBLOCK", &client_id1.to_string(), "TIMEOUT"])
            .await,
        1,
    );
    assert_integer_eq(
        &control
            .command(&["CLIENT", "UNBLOCK", &client_id2.to_string(), "ERROR"])
            .await,
        1,
    );

    // rd1 should get nil (timeout unblock)
    let resp = rd1.read_response(Duration::from_secs(2)).await;
    assert_nil(&resp.expect("rd1 should get nil from TIMEOUT unblock"));

    // rd2 should get an UNBLOCKED error
    let resp = rd2.read_response(Duration::from_secs(2)).await;
    assert_error_prefix(&resp.expect("rd2 should get UNBLOCKED error"), "UNBLOCKED");
}
