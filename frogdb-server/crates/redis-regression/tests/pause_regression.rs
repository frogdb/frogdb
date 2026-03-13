use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Tests that do NOT depend on buggy CLIENT PAUSE behavior
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_commands_not_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut reader = server.connect().await;

    // Pause for write operations only
    assert_ok(&control.command(&["CLIENT", "PAUSE", "200", "WRITE"]).await);

    // Read commands should go through immediately
    assert!(matches!(
        reader.command(&["GET", "nonexistent"]).await,
        Response::Bulk(None)
    ));

    // PING should also go through
    let r = reader.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "PONG"),
        "got {r:?}"
    );

    // INFO should go through
    let r = reader.command(&["INFO", "server"]).await;
    assert!(matches!(r, Response::Bulk(Some(_))));

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

#[tokio::test]
async fn read_admin_multi_exec_not_blocked_by_pause_ro() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    assert_ok(&control.command(&["CLIENT", "PAUSE", "500", "WRITE"]).await);

    // MULTI with read-only commands should not be blocked
    assert_ok(&client.command(&["MULTI"]).await);
    let r = client.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "PING in MULTI should queue, got {r:?}"
    );
    let r = client.command(&["GET", "somekey"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "GET in MULTI should queue, got {r:?}"
    );
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 2);

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

#[tokio::test]
async fn multiple_clients_queued_and_unblocked() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;

    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;
    let mut c3 = server.connect().await;

    // Pause writes
    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    // Queue 3 write commands (they will block)
    c1.send_only(&["SET", "k1", "v1"]).await;
    c2.send_only(&["SET", "k2", "v2"]).await;
    c3.send_only(&["SET", "k3", "v3"]).await;

    // Small sleep to allow the commands to arrive at the server
    tokio::time::sleep(Duration::from_millis(100)).await;

    // None should have responded yet
    assert!(
        c1.read_response(Duration::from_millis(50)).await.is_none(),
        "c1 should be blocked"
    );
    assert!(
        c2.read_response(Duration::from_millis(50)).await.is_none(),
        "c2 should be blocked"
    );
    assert!(
        c3.read_response(Duration::from_millis(50)).await.is_none(),
        "c3 should be blocked"
    );

    // Unpause → all three should complete
    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    let r1 = c1.read_response(Duration::from_secs(2)).await;
    let r2 = c2.read_response(Duration::from_secs(2)).await;
    let r3 = c3.read_response(Duration::from_secs(2)).await;

    assert_ok(&r1.expect("c1 response"));
    assert_ok(&r2.expect("c2 response"));
    assert_ok(&r3.expect("c3 response"));
}

#[tokio::test]
async fn clients_with_syntax_errors_get_immediate_response_during_pause() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    // Wrong arg count → immediate ERR, not blocked
    let resp = client.command(&["GET"]).await; // missing key argument
    assert_error_prefix(&resp, "ERR");

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

#[tokio::test]
async fn pause_starts_at_end_of_transaction() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT PAUSE inside a MULTI/EXEC — the pause should apply after the
    // transaction completes (i.e., EXEC itself should not be blocked)
    assert_ok(&client.command(&["MULTI"]).await);
    let r = client.command(&["CLIENT", "PAUSE", "500", "WRITE"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "CLIENT PAUSE in MULTI should return QUEUED, got {r:?}"
    );
    // Execute the transaction — should succeed with OK for CLIENT PAUSE
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert_ok(&results[0]);

    // Clean up
    client.command(&["CLIENT", "UNPAUSE"]).await;
}

#[tokio::test]
async fn unpause_with_no_pause_active_is_ok() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // CLIENT UNPAUSE when nothing is paused should succeed
    assert_ok(&client.command(&["CLIENT", "UNPAUSE"]).await);
}

// ---------------------------------------------------------------------------
// Tests for scripts being blocked by pause
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scripts_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    // EVAL with write commands should be blocked
    client
        .send_only(&["EVAL", "return redis.call('set',KEYS[1],'bar')", "1", "foo"])
        .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        client
            .read_response(Duration::from_millis(50))
            .await
            .is_none(),
        "EVAL with write should be blocked"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = client.read_response(Duration::from_secs(2)).await;
    assert_ok(&resp.expect("EVAL response after unpause"));
}

#[tokio::test]
async fn ro_scripts_not_blocked_by_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    assert_ok(&control.command(&["CLIENT", "PAUSE", "200", "WRITE"]).await);

    // EVAL_RO with no writes should not be blocked
    let resp = client.command(&["EVAL_RO", "return 'hello'", "0"]).await;
    assert_bulk_eq(&resp, b"hello");

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}

#[tokio::test]
async fn may_replicate_commands_rejected_in_ro_scripts() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // PUBLISH from EVAL_RO should return an error (not allowed in read-only scripts)
    let resp = client
        .command(&["EVAL_RO", "return redis.call('publish','ch','msg')", "0"])
        .await;
    assert_error_prefix(&resp, "ERR");
}

// ---------------------------------------------------------------------------
// Known-failing tests (CLIENT PAUSE bugs in FrogDB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn old_pause_all_over_new_pause_write() {
    // Redis semantics: PAUSE ALL always takes precedence over PAUSE WRITE.
    // If PAUSE ALL is active and you issue PAUSE WRITE with a shorter timeout,
    // the PAUSE ALL should still govern behavior.
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut reader = server.connect().await;

    // First pause ALL for a long time
    assert_ok(&control.command(&["CLIENT", "PAUSE", "60000"]).await);

    // Now issue PAUSE WRITE for shorter time — should NOT override the ALL pause
    assert_ok(&control.command(&["CLIENT", "PAUSE", "100", "WRITE"]).await);

    // Read should still be blocked (because ALL pause is active)
    reader.send_only(&["GET", "somekey"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        reader
            .read_response(Duration::from_millis(50))
            .await
            .is_none(),
        "GET should be blocked by PAUSE ALL"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let _ = reader.read_response(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn new_pause_time_preserved_over_smaller() {
    // A PAUSE WRITE 60000 followed by PAUSE WRITE 100 should keep the 60000 timeout.
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut writer = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    // Issue a shorter pause — should NOT shorten the existing pause
    assert_ok(&control.command(&["CLIENT", "PAUSE", "100", "WRITE"]).await);

    // After 200ms (> 100ms), writes should still be blocked if 60s is preserved
    writer.send_only(&["SET", "foo", "bar"]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        writer
            .read_response(Duration::from_millis(50))
            .await
            .is_none(),
        "SET should still be blocked"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let _ = writer.read_response(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn write_commands_paused_by_write_mode() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut writer = server.connect().await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    writer.send_only(&["SET", "foo", "bar"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        writer
            .read_response(Duration::from_millis(50))
            .await
            .is_none(),
        "SET should be blocked by PAUSE WRITE"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let resp = writer.read_response(Duration::from_secs(2)).await;
    assert_ok(&resp.expect("SET response after unpause"));
}

#[tokio::test]
async fn special_commands_paused_by_write_pfcount_publish() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    // PFCOUNT may write (merges HLL on first call) and PUBLISH is considered
    // a write-replicating command. Both should be blocked by PAUSE WRITE.
    client.command(&["PFADD", "myhll", "a", "b", "c"]).await;

    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    client.send_only(&["PFCOUNT", "myhll"]).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        client
            .read_response(Duration::from_millis(50))
            .await
            .is_none(),
        "PFCOUNT should be blocked by PAUSE WRITE"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
    let _ = client.read_response(Duration::from_secs(2)).await;
}

#[tokio::test]
async fn active_passive_expires_skipped_during_pause() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    // SET a key with a short TTL
    assert_ok(&client.command(&["SET", "mykey", "myval", "PX", "500"]).await);

    // Pause ALL — this should suppress active expiry
    assert_ok(&control.command(&["CLIENT", "PAUSE", "60000"]).await);

    // Sleep well past the TTL and several active expiry cycles (100ms each)
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Unpause
    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);

    // Key should still exist: active expiry was suppressed during PAUSE ALL.
    // Use DBSIZE instead of GET to avoid triggering passive/lazy expiry.
    let count = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert!(
        count >= 1,
        "key should survive PAUSE ALL (active expiry suppressed), DBSIZE={count}"
    );

    // Wait for active expiry to clean up now that pause is lifted
    tokio::time::sleep(Duration::from_millis(300)).await;

    let count = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert_eq!(count, 0, "key should be expired after active expiry resumes");
}

#[tokio::test]
async fn active_expiry_continues_during_pause_write() {
    let server = TestServer::start_standalone().await;
    let mut control = server.connect().await;
    let mut client = server.connect().await;

    // SET a key with a short TTL
    assert_ok(&client.command(&["SET", "mykey", "myval", "PX", "500"]).await);

    // Pause WRITE only — active expiry should continue
    assert_ok(
        &control
            .command(&["CLIENT", "PAUSE", "60000", "WRITE"])
            .await,
    );

    // Sleep past the TTL + several active expiry cycles
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // During PAUSE WRITE, reads are allowed — use DBSIZE to check that
    // active expiry deleted the key (DBSIZE is a read, not blocked)
    let count = unwrap_integer(&client.command(&["DBSIZE"]).await);
    assert_eq!(
        count, 0,
        "key should be expired by active expiry during PAUSE WRITE"
    );

    assert_ok(&control.command(&["CLIENT", "UNPAUSE"]).await);
}
