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

#[tokio::test]
async fn debug_memory_check_consistent_after_writes() {
    use crate::common::response_helpers::unwrap_integer;

    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    for i in 0..64 {
        let key = format!("mc-{i}");
        let resp = client.command(&["SET", &key, "some-value"]).await;
        assert!(matches!(resp, Response::Simple(_)));
    }

    // MEMORY-CHECK always replies with a per-shard map. Over the default RESP2
    // client a server map arrives as a flat array of alternating key/detail
    // entries; each detail is itself a flat array of alternating field/value.
    let resp = client.command(&["DEBUG", "MEMORY-CHECK"]).await;
    let Response::Array(entries) = resp else {
        panic!("expected array (RESP2-flattened map), got {resp:?}");
    };
    assert!(!entries.is_empty());
    // Details are the odd-indexed entries (values of the outer map).
    let details: Vec<&Response> = entries.iter().skip(1).step_by(2).collect();
    assert!(!details.is_empty(), "no per-shard detail entries");
    for detail in details {
        let Response::Array(fields) = detail else {
            panic!("expected per-shard detail array, got {detail:?}");
        };
        let consistent = fields
            .chunks_exact(2)
            .find(|pair| matches!(&pair[0], Response::Bulk(Some(b)) if &b[..] == b"consistent"))
            .map(|pair| unwrap_integer(&pair[1]))
            .expect("consistent field present");
        assert_eq!(consistent, 1, "memory accounting drifted at quiesce");
    }

    server.shutdown().await;
}
