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
