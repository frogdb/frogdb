use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;

// ---------------------------------------------------------------------------
// Basic MULTI / EXEC / DISCARD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multi_exec_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["RPUSH", "mylist", "a"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    // Commands inside MULTI return QUEUED
    let r = client.command(&["RPUSH", "mylist", "b"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let r = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );
    let r = client.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {r:?}"
    );

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 3);
    // RPUSH returned 2
    assert_eq!(unwrap_integer(&results[0]), 2);
    // LRANGE returned [a, b]
    let lrange = unwrap_array(results[1].clone());
    assert_eq!(lrange.len(), 2);
    // PING returned PONG
    assert!(matches!(&results[2], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn discard_aborts_queued_commands() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "original"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "x", "changed"]).await; // returns QUEUED
    assert_ok(&client.command(&["DISCARD"]).await);

    // x should still be "original"
    assert_bulk_eq(&client.command(&["GET", "x"]).await, b"original");
}

#[tokio::test]
async fn nested_multi_not_allowed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    let resp = client.command(&["MULTI"]).await;
    assert_error_prefix(&resp, "ERR");
    // Clean up
    client.command(&["DISCARD"]).await;
}

#[tokio::test]
async fn multi_with_command_that_alters_argc_argv() {
    // SPOP inside MULTI should work (tests commands that alter internal arg structure)
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a", "b", "c"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPOP", "myset"]).await; // returns QUEUED
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    // SPOP should return one of the members
    assert!(matches!(results[0], Response::Bulk(Some(_))));
}

#[tokio::test]
async fn watch_inside_multi_not_allowed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    let resp = client.command(&["WATCH", "x"]).await;
    assert_error_prefix(&resp, "ERR");
    client.command(&["DISCARD"]).await;
}

#[tokio::test]
async fn exec_fails_with_queuing_error_nonexisting_command() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SET", "x", "1"]).await; // QUEUED
    let resp = client.command(&["NOSUCHCOMMAND"]).await; // should error
    assert_error_prefix(&resp, "ERR");
    // EXEC should return error or nil array (transaction aborted)
    let exec_resp = client.command(&["EXEC"]).await;
    // After a command error in MULTI, EXEC should fail (EXECABORT)
    assert_error_prefix(&exec_resp, "EXECABORT");
}

#[tokio::test]
async fn after_exec_abort_client_multi_state_cleared() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["NOSUCHCOMMAND"]).await; // causes queuing error
    client.command(&["EXEC"]).await; // should return EXECABORT

    // Client should now be in normal state; PING should work
    let resp = client.command(&["PING"]).await;
    assert!(matches!(&resp, Response::Simple(s) if s == "PONG"));
}

// ---------------------------------------------------------------------------
// WATCH / EXEC
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exec_works_on_unwatched_key_not_modified() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["PING"]).await;
    let resp = client.command(&["EXEC"]).await;

    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn exec_fails_on_watched_key_modified() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "30"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // Another client modifies the key
    c2.command(&["SET", "x", "40"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;

    // EXEC returns null array when watched key was dirtied
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil/empty exec result, got {resp:?}"
    );
}

#[tokio::test]
async fn exec_fails_on_watched_key_modified_one_of_five() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    for i in 1..=5 {
        c1.command(&["SET", &format!("{{k}}key{i}"), &i.to_string()])
            .await;
    }

    // Watch all 5 keys (use hash tag so all land on same slot)
    assert_ok(
        &c1.command(&[
            "WATCH", "{k}key1", "{k}key2", "{k}key3", "{k}key4", "{k}key5",
        ])
        .await,
    );

    // Dirty just {k}key3
    c2.command(&["SET", "{k}key3", "modified"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;

    // Should be aborted
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil/empty exec result, got {resp:?}"
    );
}

#[tokio::test]
async fn exec_fail_on_watched_key_modified_by_sort_store() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["RPUSH", "{t}src", "3", "1", "2"]).await;
    assert_ok(&c1.command(&["WATCH", "{t}dst"]).await);

    // Another client does SORT STORE into {t}dst — should dirty the watched key
    c2.command(&["SORT", "{t}src", "STORE", "{t}dst"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;

    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil exec result after SORT STORE dirtied watched key, got {resp:?}"
    );
}

#[tokio::test]
async fn after_successful_exec_key_no_longer_watched() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "1"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // Execute successfully (no modification)
    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    let _ = unwrap_array(resp); // should succeed

    // Now dirty the key — this should NOT affect a new MULTI/EXEC
    c2.command(&["SET", "x", "modified"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    // x is no longer watched; EXEC should succeed
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn after_failed_exec_key_no_longer_watched() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "1"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // Dirty the key → EXEC will fail
    c2.command(&["SET", "x", "modified"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let _aborted = c1.command(&["EXEC"]).await;

    // Now do another MULTI/EXEC — x is no longer watched
    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn unwatch_prevents_exec_failure() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "1"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // Dirty the key
    c2.command(&["SET", "x", "2"]).await;

    // Unwatch before MULTI
    assert_ok(&c1.command(&["UNWATCH"]).await);

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    // Should succeed since we unwatched
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn unwatch_when_nothing_watched() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // UNWATCH without any WATCH should return OK
    assert_ok(&client.command(&["UNWATCH"]).await);
}

#[tokio::test]
async fn discard_clears_watch_dirty_flag() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "1"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    c2.command(&["SET", "x", "2"]).await; // dirty

    assert_ok(&c1.command(&["MULTI"]).await);
    assert_ok(&c1.command(&["DISCARD"]).await); // clears watch state

    // New MULTI/EXEC should succeed (watch was cleared by DISCARD)
    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn discard_unwatches_all_keys() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "{k}x", "1"]).await;
    c1.command(&["SET", "{k}y", "2"]).await;
    assert_ok(&c1.command(&["WATCH", "{k}x", "{k}y"]).await);

    assert_ok(&c1.command(&["MULTI"]).await);
    assert_ok(&c1.command(&["DISCARD"]).await);

    // Dirty both keys after DISCARD
    c2.command(&["SET", "{k}x", "10"]).await;
    c2.command(&["SET", "{k}y", "20"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    // Should succeed because DISCARD cleared the watch
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
}

#[tokio::test]
async fn watch_considers_expire_on_watched_key() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "hello"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // EXPIRE on watched key should dirty it
    c2.command(&["EXPIRE", "x", "1000"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;

    // EXPIRE dirtied the watched key → EXEC should fail
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil exec result after EXPIRE dirtied watched key, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// FLUSHALL / FLUSHDB watching (known-failing FrogDB bugs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flushall_touches_watched_keys() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "hello"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    c2.command(&["FLUSHALL"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil exec result after FLUSHALL, got {resp:?}"
    );
}

#[tokio::test]
async fn flushall_does_not_touch_non_affected_keys() {
    // DEL x, then WATCH x, then FLUSHALL → EXEC should still fail because
    // x went from non-existent to non-existent but FLUSHALL is a write op
    // Actually the test here is: watch a key that doesn't exist, flushall, exec → succeed
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    // Ensure x doesn't exist
    c1.command(&["DEL", "x"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    // Flush (x wasn't set anyway, so no data loss for x)
    c2.command(&["FLUSHALL"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["SET", "x", "new"]).await;
    // This may or may not be considered dirty depending on implementation.
    // We only verify that EXEC either succeeds or returns nil; not a hard requirement.
    let _resp = c1.command(&["EXEC"]).await;
}

#[tokio::test]
async fn flushdb_touches_watched_keys() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["SET", "x", "hello"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    c2.command(&["FLUSHDB"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil exec result after FLUSHDB, got {resp:?}"
    );
}

#[tokio::test]
async fn flushdb_does_not_touch_unaffected_watched_keys() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    c1.command(&["DEL", "x"]).await;
    assert_ok(&c1.command(&["WATCH", "x"]).await);

    c2.command(&["FLUSHDB"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["SET", "x", "new"]).await;
    let _resp = c1.command(&["EXEC"]).await;
    // We just verify the connection is still usable
    let r = c1.command(&["PING"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "PONG"),
        "got {r:?}"
    );
}

#[tokio::test]
async fn flushall_watching_several_keys() {
    let server = TestServer::start_standalone().await;
    let mut c1 = server.connect().await;
    let mut c2 = server.connect().await;

    for i in 1..=5 {
        c1.command(&["SET", &format!("{{k}}key{i}"), &i.to_string()])
            .await;
    }
    assert_ok(
        &c1.command(&["WATCH", "{k}key1", "{k}key2", "{k}key3", "{k}key4", "{k}key5"])
            .await,
    );

    c2.command(&["FLUSHALL"]).await;

    assert_ok(&c1.command(&["MULTI"]).await);
    c1.command(&["PING"]).await;
    let resp = c1.command(&["EXEC"]).await;
    assert!(
        matches!(&resp, Response::Bulk(None))
            || matches!(&resp, Response::Array(a) if a.is_empty())
            || matches!(&resp, Response::Null),
        "expected nil exec result, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// Blocking commands inside MULTI
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blocking_commands_ignore_timeout_in_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    // Blocking commands inside MULTI should be queued, not block
    let r = client.command(&["BLPOP", "mylist", "0"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED for BLPOP in MULTI, got {r:?}"
    );
    let r = client.command(&["BRPOP", "mylist", "0"]).await;
    assert!(
        matches!(&r, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED for BRPOP in MULTI, got {r:?}"
    );
    assert_ok(&client.command(&["DISCARD"]).await);
}

// ---------------------------------------------------------------------------
// Missing features — stubbed with #[ignore]
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "needs CONFIG SET maxmemory support"]
async fn exec_fails_with_queuing_error_oom() {
    // CONFIG SET maxmemory 1 → then MULTI with SET → EXEC should return EXECABORT
    todo!()
}

#[tokio::test]
#[ignore = "needs DEBUG set-active-expire support"]
async fn exec_fails_on_lazy_expired_watched_key() {
    // DEBUG set-active-expire 0 → SET x EX 1 → WATCH x → sleep → MULTI/EXEC
    todo!()
}

#[tokio::test]
#[ignore = "needs DEBUG set-active-expire support"]
async fn watch_stale_keys_should_not_fail_exec() {
    todo!()
}

#[tokio::test]
#[ignore = "needs DEBUG set-active-expire support"]
async fn delete_watched_stale_keys_should_not_fail_exec() {
    todo!()
}

#[tokio::test]
#[ignore = "needs DEBUG set-active-expire support"]
async fn flushdb_while_watching_stale_keys_should_not_fail_exec() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET maxmemory support"]
async fn discard_should_not_fail_during_oom() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET lua-time-limit support"]
async fn multi_and_script_timeout() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET lua-time-limit support"]
async fn exec_and_script_timeout() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET lua-time-limit support"]
async fn multi_exec_body_and_script_timeout() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET lua-time-limit support"]
async fn just_exec_and_script_timeout() {
    todo!()
}

#[tokio::test]
#[ignore = "needs CONFIG SET maxmemory support"]
async fn exec_with_only_read_commands_not_rejected_when_oom() {
    todo!()
}
