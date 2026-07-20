use crate::replica::connection::ConnectionState;
use crate::replica::{ConnectFactory, ReplicaReplicationHandler};
use crate::state::ReplicationState;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[test]
fn test_connection_state_display() {
    assert_eq!(format!("{}", ConnectionState::Disconnected), "disconnected");
    assert_eq!(format!("{}", ConnectionState::Streaming), "streaming");
}

#[tokio::test]
async fn test_replica_handler_creation() {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = PathBuf::from("/tmp/frogdb-test");
    let state_path = data_dir.join("replication_state.json");
    let (handler, _rx) = ReplicaReplicationHandler::new(addr, 6380, state, state_path, data_dir);
    let current_state = handler.state().await;
    assert!(!current_state.replication_id.is_empty());
}

/// A connect factory that counts every dial attempt and always fails
/// immediately, keeping `start()` in its retry/backoff loop forever unless
/// something breaks it out.
fn counting_failing_factory() -> (ConnectFactory, Arc<AtomicUsize>) {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let factory: ConnectFactory = Arc::new(move |_addr| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "test: primary unreachable",
            ))
        })
    });
    (factory, attempts)
}

/// Regression test for the bug this issue fixes: `start()` used to ignore its
/// own `shutdown` watch entirely, so `stop()` could never break the
/// reconnect loop — only `task.abort()` could. This asserts `stop()` alone
/// (no abort) terminates a `start()` task that is actively retrying, and that
/// no further connection attempts happen afterward.
#[tokio::test]
async fn test_stop_terminates_reconnect_loop_without_abort() {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = std::env::temp_dir();
    let state_path = data_dir.join(format!(
        "frogdb-test-stop-{}-{}.json",
        std::process::id(),
        line!()
    ));
    let (mut handler, _rx) =
        ReplicaReplicationHandler::new(addr, 6380, state, state_path, data_dir);
    let (factory, attempts) = counting_failing_factory();
    handler.set_connect_factory(factory);
    let handler = Arc::new(handler);

    let handler_clone = handler.clone();
    let task = tokio::spawn(async move { handler_clone.start().await });

    // Let a few reconnect attempts happen (first backoff is 100ms).
    tokio::time::sleep(Duration::from_millis(250)).await;
    let attempts_before_stop = attempts.load(Ordering::SeqCst);
    assert!(
        attempts_before_stop >= 1,
        "handler should have attempted to connect at least once before stop()"
    );

    handler.stop();

    // The loop must observe the shutdown watch and return `Ok(())` on its
    // own — no `task.abort()` involved. A generous timeout distinguishes
    // "terminated via the watch" from "would have hung forever without
    // abort" (the pre-fix behavior).
    let result = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(
        result.is_ok(),
        "start() must terminate via stop() without needing task.abort()"
    );
    assert!(
        result.unwrap().unwrap().is_ok(),
        "start() should return Ok(()) after a clean stop()"
    );

    // No further connection attempts after stop().
    let attempts_at_stop = attempts.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        attempts_at_stop,
        "no further connection attempts after stop()"
    );
}

/// `stop()` called before `start()` is ever polled must make `start()`
/// return immediately rather than attempting one more connection.
#[tokio::test]
async fn test_stop_before_start_prevents_any_connection_attempt() {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = std::env::temp_dir();
    let state_path = data_dir.join(format!(
        "frogdb-test-stop-before-start-{}-{}.json",
        std::process::id(),
        line!()
    ));
    let (mut handler, _rx) =
        ReplicaReplicationHandler::new(addr, 6380, state, state_path, data_dir);
    let (factory, attempts) = counting_failing_factory();
    handler.set_connect_factory(factory);

    handler.stop();

    let result = tokio::time::timeout(Duration::from_secs(2), handler.start()).await;
    assert!(
        result.is_ok(),
        "start() must return immediately when already stopped"
    );
    assert!(result.unwrap().is_ok());
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        0,
        "no connection attempt should occur once stop() was already requested"
    );
}
