//! Integration tests for the DEBUG introspection commands (Phase 2 concurrency
//! quiescence probes): LOCKTABLE, WAITQUEUE, MEMORY-CHECK, EXPIRY-INDEX-CHECK.

use crate::common::test_server::TestServer;
use frogdb_protocol::Response;

#[tokio::test]
async fn debug_locktable_empty_on_idle_server() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    let resp = client.command(&["DEBUG", "LOCKTABLE"]).await;
    // Idle server: no intents, no continuation locks -> sentinel bulk string.
    match resp {
        Response::Bulk(Some(b)) => {
            assert_eq!(&b[..], b"# lock table is empty");
        }
        other => panic!("expected empty-sentinel bulk, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test]
async fn debug_locktable_unknown_still_errors_are_isolated() {
    // Regression guard: adding LOCKTABLE must not break the unknown-subcommand path.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "NOPE-NOT-A-CMD"]).await;
    assert!(matches!(resp, Response::Error(_)));
    server.shutdown().await;
}

#[tokio::test]
async fn debug_waitqueue_empty_on_idle_server() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "WAITQUEUE"]).await;
    match resp {
        Response::Bulk(Some(b)) => assert_eq!(&b[..], b"# wait queue is empty"),
        other => panic!("expected empty-sentinel bulk, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test]
async fn debug_waitqueue_reports_blocked_client() {
    let server = TestServer::start_standalone().await;

    // A second connection blocks on BLPOP with an infinite timeout.
    let mut blocker = server.connect().await;
    let handle = tokio::spawn(async move {
        // Never resolves within the test; the task is dropped at shutdown.
        blocker.command(&["BLPOP", "waitq-key", "0"]).await
    });

    // Poll DEBUG WAITQUEUE until the waiter appears (bounded). When waiters are
    // present the server replies with a structured RESP map; over the default
    // RESP2 client that arrives as a (non-empty) Array. The empty case is the
    // `# wait queue is empty` bulk sentinel, so an Array reply means "seen".
    let mut probe = server.connect().await;
    let mut seen = false;
    for _ in 0..50 {
        let resp = probe.command(&["DEBUG", "WAITQUEUE"]).await;
        if matches!(resp, Response::Array(ref items) if !items.is_empty()) {
            seen = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(seen, "DEBUG WAITQUEUE never reported the blocked BLPOP");

    handle.abort();
    server.shutdown().await;
}
