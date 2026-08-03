//! Function libraries across a replication link (issue 48).
//!
//! The function registry is process-wide state that lives *beside* the keyspace,
//! not in it: it is one `SharedFunctionRegistry` per node, persisted to its own
//! `functions.fdb`. Before issue 48 nothing carried it across the link at all —
//! neither the steady-state stream nor the full-sync payload — so a replica
//! answered `FCALL_RO` with "function not found" for every library its primary
//! had, and a promoted replica lost them permanently.
//!
//! Both halves are tested here, because they are separate mechanisms: the
//! steady-state half is a `CONTROL_SHARD`-tagged `FUNCTION` frame, the full-sync
//! half is a whole-registry `FUNCTION RESTORE <dump> FLUSH` broadcast inside the
//! window the syncing replica replays.

use std::time::Duration;

use crate::common::replication_helpers::{start_primary_replica_pair, wait_for_replication};
use crate::common::test_server::{TestServer, TestServerConfig, get_error_message, is_error};
use bytes::Bytes;
use frogdb_protocol::Response;

/// A library with one read-only function, so it can be called with `FCALL_RO`
/// on a replica (a plain `FCALL` is a write and is refused there).
const GREETER: &str = r#"#!lua name=greeter
redis.register_function{
    function_name='greet',
    callback=function(keys, args) return 'hello ' .. (args[1] or 'world') end,
    flags={'no-writes'}
}
"#;

/// A second library, to prove `FUNCTION DELETE` is not confused with a flush.
const COUNTER: &str = r#"#!lua name=counter
redis.register_function{
    function_name='answer',
    callback=function(keys, args) return 42 end,
    flags={'no-writes'}
}
"#;

/// Poll `FCALL_RO <function>` on `server` until it stops erroring, or give up.
///
/// Replication is asynchronous and `WAIT` only covers frames the primary has
/// broadcast, so a poll (rather than a single read) is what keeps this from
/// being a race. Returns the successful response.
async fn wait_for_function(server: &TestServer, function: &str, arg: &str) -> Response {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut last = Response::Null;
    while std::time::Instant::now() < deadline {
        let response = server.send("FCALL_RO", &[function, "0", arg]).await;
        if !is_error(&response) {
            return response;
        }
        last = response;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{function} never became callable on this node; last reply: {last:?}");
}

/// Poll until `FCALL_RO <function>` *fails* on `server` — the deletion half.
async fn wait_for_function_gone(server: &TestServer, function: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let response = server.send("FCALL_RO", &[function, "0"]).await;
        if is_error(&response) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{function} is still callable on this node after it was deleted upstream");
}

/// **Issue 48, the steady-state half.** A library loaded on a primary that
/// already has a replica attached reaches that replica, and is callable there.
///
/// FM-REPLICATION-054
#[tokio::test]
async fn a_function_loaded_on_the_primary_reaches_an_attached_replica() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    let loaded = primary.send("FUNCTION", &["LOAD", GREETER]).await;
    assert_eq!(loaded, Response::Bulk(Some(Bytes::from("greeter"))));

    let _ = wait_for_replication(&primary, 5000).await;

    let called = wait_for_function(&replica, "greet", "replica").await;
    assert_eq!(called, Response::Bulk(Some(Bytes::from("hello replica"))));

    let listed = replica.send("FUNCTION", &["LIST"]).await;
    match listed {
        Response::Array(entries) => assert!(
            !entries.is_empty(),
            "FUNCTION LIST on the replica must show the replicated library",
        ),
        other => panic!("unexpected FUNCTION LIST reply on the replica: {other:?}"),
    }

    replica.shutdown().await;
    primary.shutdown().await;
}

/// **Issue 48, the full-sync half.** A library loaded *before* any replica
/// exists still reaches the replica that later attaches — the steady-state
/// stream never carried it, so only the full-sync snapshot can.
///
/// FM-REPLICATION-055
#[tokio::test]
async fn a_replica_that_full_syncs_receives_the_primarys_existing_libraries() {
    let primary = TestServer::start_primary_with_config(TestServerConfig::default()).await;

    let loaded = primary.send("FUNCTION", &["LOAD", GREETER]).await;
    assert_eq!(loaded, Response::Bulk(Some(Bytes::from("greeter"))));

    // Only now does a replica exist: everything above is invisible to the
    // replication stream and can only arrive with the full resync.
    let replica =
        TestServer::start_replica_with_config(&primary, TestServerConfig::default()).await;

    let called = wait_for_function(&replica, "greet", "latecomer").await;
    assert_eq!(called, Response::Bulk(Some(Bytes::from("hello latecomer"))));

    replica.shutdown().await;
    primary.shutdown().await;
}

/// A promoted replica can still serve the libraries it replicated: they are in
/// its own registry, not borrowed from the link. This is the failure that made
/// the missing replication dangerous rather than merely incomplete — a failover
/// silently dropped every library.
///
/// FM-REPLICATION-056
#[tokio::test]
async fn a_promoted_replica_keeps_the_libraries_it_replicated() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    primary.send("FUNCTION", &["LOAD", GREETER]).await;
    let _ = wait_for_replication(&primary, 5000).await;
    wait_for_function(&replica, "greet", "before").await;

    let promoted = replica.send("REPLICAOF", &["NO", "ONE"]).await;
    assert!(!is_error(&promoted), "promotion failed: {promoted:?}");

    // FCALL (not FCALL_RO) — the node is a primary now, so the write-capable
    // entry point must work too.
    let called = replica.send("FCALL", &["greet", "0", "promoted"]).await;
    assert_eq!(called, Response::Bulk(Some(Bytes::from("hello promoted"))));

    replica.shutdown().await;
    primary.shutdown().await;
}

/// `FUNCTION DELETE` replicates as itself: the replica loses exactly that
/// library and keeps the others. A flush-and-reload would pass a
/// "library is gone" assertion while being wrong about the rest.
///
/// FM-REPLICATION-054
#[tokio::test]
async fn function_delete_removes_one_library_on_the_replica_and_keeps_the_rest() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    primary.send("FUNCTION", &["LOAD", GREETER]).await;
    primary.send("FUNCTION", &["LOAD", COUNTER]).await;
    let _ = wait_for_replication(&primary, 5000).await;
    wait_for_function(&replica, "greet", "x").await;
    wait_for_function(&replica, "answer", "").await;

    let deleted = primary.send("FUNCTION", &["DELETE", "greeter"]).await;
    assert!(!is_error(&deleted), "FUNCTION DELETE failed: {deleted:?}");
    let _ = wait_for_replication(&primary, 5000).await;

    wait_for_function_gone(&replica, "greet").await;
    assert_eq!(
        replica.send("FCALL_RO", &["answer", "0"]).await,
        Response::Integer(42),
        "deleting one library must not take the other with it",
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// A client may not mutate a replica's registry directly. The replica's
/// libraries are its primary's, applied off the stream; a locally loaded one
/// would be invisible to the primary and would survive until the next
/// `FUNCTION` frame overwrote it — exactly the divergence propagation exists to
/// close. Redis flags `function|load` as a write and answers `-READONLY` here.
///
/// FM-REPLICATION-057
#[tokio::test]
async fn a_client_can_not_load_a_function_on_a_replica() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;

    for args in [
        vec!["LOAD", GREETER],
        vec!["DELETE", "greeter"],
        vec!["FLUSH"],
    ] {
        let response = replica.send("FUNCTION", &args).await;
        let message = get_error_message(&response).unwrap_or_default();
        assert!(
            message.starts_with("READONLY"),
            "FUNCTION {} on a replica must be refused with -READONLY, got {response:?}",
            args[0],
        );
    }

    // Reads are still served: only the mutating subcommands are gated.
    let listed = replica.send("FUNCTION", &["LIST"]).await;
    assert!(
        !is_error(&listed),
        "FUNCTION LIST is a read and must still work on a replica: {listed:?}",
    );

    replica.shutdown().await;
    primary.shutdown().await;
}
