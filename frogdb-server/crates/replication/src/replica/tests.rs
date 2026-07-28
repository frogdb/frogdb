use crate::replica::connection::ConnectionState;
use crate::replica::offset::ReplicaOffset;
use crate::replica::{ConnectFactory, ReplicaReplicationHandler};
use crate::state::ReplicationState;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;

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

/// The ACK cadence is config-driven (`replication.ack-interval-ms`), not a
/// hardcoded constant. A fresh handler defaults to 1s; `set_ack_interval`
/// overrides it with the configured value, and a zero (which config validation
/// already rejects) is ignored so we never spin a zero-duration interval.
#[test]
fn ack_interval_reflects_injected_config_value() {
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = PathBuf::from("/tmp/frogdb-test");
    let state_path = data_dir.join("replication_state.json");
    let (mut handler, _rx) =
        ReplicaReplicationHandler::new(addr, 6380, state, state_path, data_dir);

    // Default when config supplies nothing: 1s.
    assert_eq!(handler.ack_interval(), Duration::from_secs(1));

    // A non-default configured value is adopted.
    handler.set_ack_interval(250);
    assert_eq!(handler.ack_interval(), Duration::from_millis(250));

    // Zero is ignored — the previous safe value survives.
    handler.set_ack_interval(0);
    assert_eq!(handler.ack_interval(), Duration::from_millis(250));
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

/// Demotion path: pins the intended offset sequence a primary->replica
/// transition publishes through the cluster-bus HealthProbe atomic.
///
/// When a node is demoted, a fresh `ReplicaReplicationHandler` is constructed
/// (its `live` offset seeded from the persisted `offset_at_save`, which is 0 for
/// a node that was never a replica) and then wired to the SAME HealthProbe
/// atomic the old primary role advertised into — an atomic currently holding the
/// old primary's high offset `N`.
///
/// `set_shared_offset` stamps the handler's own live offset (0 on a fresh
/// demotion) into that atomic BEFORE adopting it, so the failure detector
/// briefly reads 0 rather than the stale-high `N` the old primary left behind.
/// Round-10 lane-B judged the transient 0 an improvement over advertising a
/// stale-high value that no longer reflects this node's applied position; this
/// test pins that sequence and guards it against regressing back to stamping the
/// stale value.
///
/// Sequence asserted:
///   1. HealthProbe atomic reads `N` (node still advertising its old primary
///      offset).
///   2. Demotion wiring (`set_shared_offset`) -> atomic reads 0 (transient, NOT
///      the stale `N`).
///   3. FULLRESYNC (`ReplicaOffset::reset_to`, the method the connection's
///      `+FULLRESYNC` handler calls) -> atomic reflects the new stream offset.
#[tokio::test]
async fn test_demotion_stamps_zero_then_fullresync_offset_into_shared_probe() {
    const OLD_PRIMARY_OFFSET: u64 = 987_654; // N: what the old primary advertised.
    const FULLRESYNC_OFFSET: u64 = 42; // the fresh stream position from the new primary.

    // Stage 1: the shared HealthProbe atomic as the node's old primary role left
    // it — advertising the primary's high offset N.
    let health_probe = Arc::new(AtomicU64::new(OLD_PRIMARY_OFFSET));
    assert_eq!(
        health_probe.load(Ordering::Acquire),
        OLD_PRIMARY_OFFSET,
        "precondition: probe advertises the old primary offset N before demotion"
    );

    // Demotion: construct a fresh replica handler. `ReplicationState::new()` has
    // `offset_at_save == 0`, so the handler's live offset seeds to 0 — the node
    // was never a replica, it has no persisted replica progress.
    let addr: SocketAddr = "127.0.0.1:6379".parse().unwrap();
    let state = ReplicationState::new();
    let data_dir = std::env::temp_dir();
    let state_path = data_dir.join(format!(
        "frogdb-test-demotion-{}-{}.json",
        std::process::id(),
        line!()
    ));
    let (mut handler, _rx) =
        ReplicaReplicationHandler::new(addr, 6380, state, state_path, data_dir);

    // Stage 2: wire the fresh handler to the SAME probe atomic. It stamps its own
    // live offset (0) in before adopting — so the detector sees the transient 0,
    // never the stale-high N.
    handler.set_shared_offset(health_probe.clone());
    assert_eq!(
        health_probe.load(Ordering::Acquire),
        0,
        "demotion must stamp the fresh replica's live offset (0), not the stale-high old-primary N"
    );
    // The handler adopts the exact atomic the caller passed (single shared home).
    let adopted = handler
        .shared_offset()
        .expect("handler must vend the shared offset once wired");
    assert!(
        Arc::ptr_eq(&adopted, &health_probe),
        "handler must adopt the caller's atomic, not mint a second one"
    );

    // Stage 3: the FULLRESYNC path. The connection's `+FULLRESYNC` handler builds
    // a `ReplicaOffset` over the handler's (now shared) live atomic and calls
    // `reset_to(new_offset)`. Model that exact call against the adopted atomic.
    let offsets = ReplicaOffset::new(
        Arc::new(RwLock::new(handler.state().await)),
        adopted.clone(),
    );
    offsets.reset_to(FULLRESYNC_OFFSET);
    assert_eq!(
        health_probe.load(Ordering::Acquire),
        FULLRESYNC_OFFSET,
        "after FULLRESYNC the probe must reflect the new stream offset through the adopted atomic"
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
