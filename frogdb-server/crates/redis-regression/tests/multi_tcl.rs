//! Rust port of Redis 8.6.0 `unit/multi.tcl` test suite.
//!
//! Excludes:
//! - `needs:debug` tests (DEBUG SET-ACTIVE-EXPIRE, lazy expiry watch tests)
//! - `needs:repl` tests (replication stream propagation assertions)
//! - `needs:config-maxmemory` tests (CONFIG SET maxmemory from second client)
//! - `external:skip` tests (BGREWRITEAOF, AOF config, AOF propagation)
//! - `needs:save` / `needs:reset` tests (SAVE, SHUTDOWN inside MULTI)
//! - `singledb:skip` tests (SWAPDB, SELECT / cross-DB WATCH)
//! - Script timeout tests (EVAL busy scripts with concurrent MULTI from another client)
//!
//! ## Intentional exclusions
//!
//! SWAPDB / cross-DB WATCH (FrogDB has a single database):
//! - `SWAPDB does not touch non-existing key replaced with stale key` — intentional-incompatibility:single-db — single-DB
//! - `SWAPDB does not touch stale key replaced with another stale key` — intentional-incompatibility:single-db — single-DB
//! - `WATCH is able to remember the DB a key belongs to` — intentional-incompatibility:single-db — single-DB
//!
//! Script timeout interactions (different script-execution model):
//! - `MULTI and script timeout` — redis-specific — Redis-internal feature (script timeout)
//! - `EXEC and script timeout` — redis-specific — Redis-internal feature (script timeout)
//! - `just EXEC and script timeout` — redis-specific — Redis-internal feature (script timeout)
//!
//! Replica/replication-state interactions:
//! - `exec with write commands and state change` — intentional-incompatibility:replication — replication-internal
//! - `exec with read commands and stale replica state change` — intentional-incompatibility:replication — replication-internal
//!
//! OOM / maxmemory inside EXEC (different eviction model):
//! - `EXEC with only read commands should not be rejected when OOM` — intentional-incompatibility:config — needs:config-maxmemory
//! - `EXEC with at least one use-memory command should fail` — intentional-incompatibility:config — needs:config-maxmemory
//!
//! Replication-propagation tests:
//! - `MULTI propagation of PUBLISH` — intentional-incompatibility:replication — replication-internal
//! - `MULTI propagation of SCRIPT LOAD` — intentional-incompatibility:replication — replication-internal
//! - `MULTI propagation of EVAL` — intentional-incompatibility:replication — replication-internal
//! - `MULTI propagation of SCRIPT FLUSH` — intentional-incompatibility:replication — replication-internal
//! - `MULTI propagation of XREADGROUP` — intentional-incompatibility:replication — replication-internal
//! - `MULTI with $cmd` — intentional-incompatibility:replication — replication-internal (inner-command propagation matrix)
//!
//! Stale-key WATCH (needs:debug — requires DEBUG SET-ACTIVE-EXPIRE):
//! - `WATCH stale keys should not fail EXEC` — intentional-incompatibility:debug — needs:debug
//! - `Delete WATCHed stale keys should not fail EXEC` — intentional-incompatibility:debug — needs:debug
//! - `FLUSHDB while watching stale keys should not fail EXEC` — intentional-incompatibility:debug — needs:debug
//! - `SWAPDB does not touch watched stale keys` — intentional-incompatibility:debug — needs:debug + singledb:skip
//!
//! AOF / config-rewrite (FrogDB does not support these):
//! - `MULTI with BGREWRITEAOF` — intentional-incompatibility:persistence — aof
//! - `MULTI with config set appendonly` — intentional-incompatibility:persistence — aof
//! - `MULTI with config error` — redis-specific — Redis-internal CONFIG behavior
//! - `exec with write commands and state change` (needs:repl, min-replicas-to-write)
//! - `exec with read commands and stale replica state change` (needs:repl)
//! - `MULTI with config error` (raw RESP parsing of mixed array responses)
//! - Encoding loops

use frogdb_protocol::Response;
use frogdb_test_harness::response::*;
use frogdb_test_harness::server::TestServer;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helper: assert EXEC returned nil/empty (transaction aborted due to WATCH).
// ---------------------------------------------------------------------------

fn assert_exec_aborted(resp: &Response) {
    assert!(
        matches!(resp, Response::Bulk(None))
            || matches!(resp, Response::Array(a) if a.is_empty())
            || matches!(resp, Response::Null),
        "expected nil/empty exec result (aborted), got {resp:?}"
    );
}

fn assert_queued(resp: &Response) {
    assert!(
        matches!(resp, Response::Simple(s) if s == "QUEUED"),
        "expected QUEUED, got {resp:?}"
    );
}

// ---------------------------------------------------------------------------
// MULTI / EXEC / DISCARD basics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_multi_exec_basics() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a"]).await;
    client.command(&["RPUSH", "mylist", "b"]).await;
    client.command(&["RPUSH", "mylist", "c"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["LRANGE", "mylist", "0", "-1"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 2);

    // LRANGE returned [a, b, c]
    let items = extract_bulk_strings(&results[0]);
    assert_eq!(items, vec!["a", "b", "c"]);

    // PING returned PONG
    assert!(matches!(&results[1], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_discard() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "mylist"]).await;
    client.command(&["RPUSH", "mylist", "a"]).await;
    client.command(&["RPUSH", "mylist", "b"]).await;
    client.command(&["RPUSH", "mylist", "c"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["DEL", "mylist"]).await);
    assert_ok(&client.command(&["DISCARD"]).await);

    // mylist should still have its elements
    let resp = client.command(&["LRANGE", "mylist", "0", "-1"]).await;
    let items = extract_bulk_strings(&resp);
    assert_eq!(items, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn tcl_nested_multi_not_allowed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    let resp = client.command(&["MULTI"]).await;
    assert_error_prefix(&resp, "ERR");
    client.command(&["EXEC"]).await;
}

#[tokio::test]
async fn tcl_multi_where_commands_alter_argc_argv() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SADD", "myset", "a"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["SPOP", "myset"]).await);

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert_bulk_eq(&results[0], b"a");

    // After SPOP the set should be empty
    assert_integer_eq(&client.command(&["EXISTS", "myset"]).await, 0);
}

#[tokio::test]
async fn tcl_watch_inside_multi_not_allowed() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    let resp = client.command(&["WATCH", "x"]).await;
    assert_error_prefix(&resp, "ERR");
    client.command(&["EXEC"]).await;
}

// ---------------------------------------------------------------------------
// EXEC fails with queuing errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_exec_fails_with_queuing_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo1{t}", "foo2{t}"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["SET", "foo1{t}", "bar1"]).await);
    // Send an invalid command to trigger a queuing error
    let resp = client.command(&["NON-EXISTING-COMMAND"]).await;
    assert_error_prefix(&resp, "ERR");
    assert_queued(&client.command(&["SET", "foo2{t}", "bar2"]).await);

    let exec_resp = client.command(&["EXEC"]).await;
    assert_error_prefix(&exec_resp, "EXECABORT");

    // Neither key should have been set
    assert_integer_eq(&client.command(&["EXISTS", "foo1{t}"]).await, 0);
    assert_integer_eq(&client.command(&["EXISTS", "foo2{t}"]).await, 0);
}

#[tokio::test]
async fn tcl_if_exec_aborts_client_multi_state_cleared() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "foo1{t}", "foo2{t}"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["SET", "foo1{t}", "bar1"]).await);
    let _ = client.command(&["NON-EXISTING-COMMAND"]).await;
    assert_queued(&client.command(&["SET", "foo2{t}", "bar2"]).await);
    let exec_resp = client.command(&["EXEC"]).await;
    assert_error_prefix(&exec_resp, "EXECABORT");

    // Client should be back to normal state
    let resp = client.command(&["PING"]).await;
    assert!(matches!(&resp, Response::Simple(s) if s == "PONG"));
}

// ---------------------------------------------------------------------------
// WATCH / EXEC interactions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_exec_works_on_watched_key_not_modified() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["WATCH", "x{t}", "y{t}", "z{t}"]).await);
    assert_ok(&client.command(&["WATCH", "k{t}"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_exec_fail_on_watched_key_modified_1_of_1() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    // Modify the watched key (same client, before MULTI)
    client.command(&["SET", "x", "40"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
async fn tcl_exec_fail_on_watched_key_modified_1_of_5() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x{t}", "30"]).await;
    assert_ok(
        &client
            .command(&["WATCH", "a{t}", "b{t}", "x{t}", "k{t}", "z{t}"])
            .await,
    );
    client.command(&["SET", "x{t}", "40"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
#[ignore = "FrogDB SORT STORE not implemented"]
async fn tcl_exec_fail_on_watched_key_modified_by_sort_store_empty() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHDB"]).await;
    client.command(&["LPUSH", "foo", "bar"]).await;
    assert_ok(&client.command(&["WATCH", "foo"]).await);
    // SORT an empty list with STORE into the watched key
    client.command(&["SORT", "emptylist", "STORE", "foo"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
async fn tcl_after_successful_exec_key_no_longer_watched() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));

    // Now modify the key -- it should no longer be watched
    client.command(&["SET", "x", "40"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_after_failed_exec_key_no_longer_watched() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["SET", "x", "40"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);
    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);

    // Key is no longer watched after failed EXEC
    client.command(&["SET", "x", "40"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_it_is_possible_to_unwatch() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["SET", "x", "40"]).await;
    assert_ok(&client.command(&["UNWATCH"]).await);

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_unwatch_when_nothing_watched() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["UNWATCH"]).await);
}

// ---------------------------------------------------------------------------
// FLUSHALL / FLUSHDB and WATCH
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_flushall_touches_watched_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["FLUSHALL"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
async fn tcl_flushall_does_not_touch_non_affected_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["FLUSHALL"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

#[tokio::test]
async fn tcl_flushdb_touches_watched_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["SET", "x", "30"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["FLUSHDB"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
async fn tcl_flushdb_does_not_touch_non_affected_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["FLUSHDB"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert!(matches!(&results[0], Response::Simple(s) if s == "PONG"));
}

// ---------------------------------------------------------------------------
// WATCH with EXPIRE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_watch_considers_expire_on_watched_key() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["DEL", "x"]).await;
    client.command(&["SET", "x", "foo"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["EXPIRE", "x", "10"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

#[tokio::test]
async fn tcl_watch_considers_touched_expired_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;
    client.command(&["DEL", "x"]).await;
    client.command(&["SET", "x", "foo"]).await;
    client.command(&["EXPIRE", "x", "1"]).await;
    assert_ok(&client.command(&["WATCH", "x"]).await);

    // Wait for the key to expire
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify the key has expired
    assert_integer_eq(&client.command(&["DBSIZE"]).await, 0);

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["PING"]).await);

    let resp = client.command(&["EXEC"]).await;
    assert_exec_aborted(&resp);
}

// ---------------------------------------------------------------------------
// DISCARD clears WATCH state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_discard_clears_watch_dirty_flag() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["SET", "x", "10"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_ok(&client.command(&["DISCARD"]).await);

    // DISCARD should clear the dirty flag, so new MULTI/EXEC should work
    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["INCR", "x"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert_integer_eq(&results[0], 11);
}

#[tokio::test]
async fn tcl_discard_unwatches_all_keys() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    assert_ok(&client.command(&["WATCH", "x"]).await);
    client.command(&["SET", "x", "10"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_ok(&client.command(&["DISCARD"]).await);

    // After DISCARD, further modifications should not affect EXEC
    client.command(&["SET", "x", "10"]).await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["INCR", "x"]).await);
    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 1);
    assert_integer_eq(&results[0], 11);
}

// ---------------------------------------------------------------------------
// Blocking commands inside MULTI ignore the timeout
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB blocking commands in MULTI behavior differs"]
async fn tcl_blocking_commands_ignore_timeout_in_multi() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client
        .command(&["XGROUP", "CREATE", "s{t}", "g", "$", "MKSTREAM"])
        .await;

    assert_ok(&client.command(&["MULTI"]).await);
    assert_queued(&client.command(&["BLPOP", "empty_list{t}", "0"]).await);
    assert_queued(&client.command(&["BRPOP", "empty_list{t}", "0"]).await);
    assert_queued(
        &client
            .command(&["BRPOPLPUSH", "empty_list1{t}", "empty_list2{t}", "0"])
            .await,
    );
    assert_queued(
        &client
            .command(&[
                "BLMOVE",
                "empty_list1{t}",
                "empty_list2{t}",
                "LEFT",
                "LEFT",
                "0",
            ])
            .await,
    );
    assert_queued(&client.command(&["BZPOPMIN", "empty_zset{t}", "0"]).await);
    assert_queued(&client.command(&["BZPOPMAX", "empty_zset{t}", "0"]).await);
    assert_queued(
        &client
            .command(&["XREAD", "BLOCK", "0", "STREAMS", "s{t}", "$"])
            .await,
    );
    assert_queued(
        &client
            .command(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                "s{t}",
                ">",
            ])
            .await,
    );

    let resp = client.command(&["EXEC"]).await;
    let results = unwrap_array(resp);
    assert_eq!(results.len(), 8);

    // All blocking commands on empty keys inside MULTI return nil
    for (i, r) in results.iter().enumerate() {
        assert!(
            matches!(r, Response::Bulk(None) | Response::Null),
            "expected nil for blocking command {i}, got {r:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Flushall while watching several keys by one client
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_flushall_while_watching_several_keys_one_client() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    client.command(&["FLUSHALL"]).await;
    client.command(&["MSET", "a{t}", "a", "b{t}", "b"]).await;
    assert_ok(&client.command(&["WATCH", "b{t}", "a{t}"]).await);
    client.command(&["FLUSHALL"]).await;

    // After FLUSHALL the watched keys are dirty; verify server is still responsive
    let resp = client.command(&["PING"]).await;
    assert!(matches!(&resp, Response::Simple(s) if s == "PONG"));
}
