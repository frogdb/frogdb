use crate::frame::ReplicationFrame;
use crate::identity::ReplicationIdentity;
use crate::net_bytes::NetByteCounters;
use crate::replica::connection::ConnectionState;
use crate::replica::offset::ReplicaOffset;
use crate::replica::{ConnectFactory, ReplicaReplicationHandler};
use crate::state::ReplicationState;
use bytes::Bytes;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
    let (handler, _rx) = ReplicaReplicationHandler::new(
        addr,
        6380,
        ReplicationIdentity::detached(state),
        state_path,
        data_dir,
    );
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
    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        addr,
        6380,
        ReplicationIdentity::detached(state),
        state_path,
        data_dir,
    );

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
    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        addr,
        6380,
        ReplicationIdentity::detached(state),
        state_path,
        data_dir,
    );
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
    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        addr,
        6380,
        ReplicationIdentity::detached(state),
        state_path,
        data_dir,
    );

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
        handler.applied_offset(),
    );
    assert!(offsets.reset_to(FULLRESYNC_OFFSET));
    assert_eq!(
        health_probe.load(Ordering::Acquire),
        FULLRESYNC_OFFSET,
        "after FULLRESYNC the probe must reflect the new stream offset through the adopted atomic"
    );
}

/// A handler over a primary that answers the handshake and grants a partial
/// resync, then goes quiet: the link reaches `Streaming` and stays there, which
/// is the only state in which `link_up()` is allowed to report `true`.
fn handler_over_a_quiet_primary(
    tmp: &std::path::Path,
) -> (
    Arc<ReplicaReplicationHandler>,
    Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>>,
) {
    let parked: Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>> = Arc::default();
    let keep = parked.clone();
    let factory: ConnectFactory = Arc::new(move |_addr| {
        let keep = keep.clone();
        Box::pin(async move {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            // Three `+OK`s for the REPLCONFs, then `+CONTINUE` — no dataset, so
            // no installer is needed to reach the streaming state.
            tokio::io::AsyncWriteExt::write_all(&mut client, b"+OK\r\n+OK\r\n+OK\r\n+CONTINUE\r\n")
                .await
                .unwrap();
            // Parked, not dropped: a closed primary half would end the stream
            // immediately and the link would come straight back down.
            keep.lock().unwrap().push(client);
            Ok(Box::new(server) as crate::BoxedStream)
        })
    });
    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        "127.0.0.1:6379".parse().unwrap(),
        6380,
        ReplicationIdentity::detached(ReplicationState::new()),
        tmp.join("state.json"),
        tmp.join("db"),
    );
    handler.set_connect_factory(factory);
    (Arc::new(handler), parked)
}

/// `link_up()` is what INFO renders as `master_link_status`, and an operator
/// deciding whether a replica is safe to fail over to reads exactly that. It
/// must report the live link, so it has to be able to say `true` — a reader
/// that is hardwired down would call a healthy, streaming replica disconnected.
#[tokio::test]
async fn link_up_reports_true_once_the_stream_is_running() {
    let tmp = tempfile::tempdir().unwrap();
    let (handler, _parked) = handler_over_a_quiet_primary(tmp.path());
    assert!(!handler.link_up(), "down before anything is dialled");

    let running = handler.clone();
    let task = tokio::spawn(async move { running.start().await });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !handler.link_up() {
        assert!(
            std::time::Instant::now() < deadline,
            "the link never came up over a primary that granted +CONTINUE"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    handler.stop();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
}

// FM-REPLICATION-063
/// `set_net_bytes_counters` is not a no-op that merely accepts an `Arc` and
/// forgets it: the exact counters it is handed are the ones the handler's own
/// connections write into. A mutant that replaces the whole method body with
/// `()` leaves the setter looking like it does something while every
/// connection this handler builds keeps counting into its private, unread
/// default — this pins that the caller's own copy of the `Arc` observes the
/// increment, which only holds if the setter actually stores it.
#[tokio::test]
async fn set_net_bytes_counters_wires_the_handlers_own_connections_to_it() {
    let tmp = tempfile::tempdir().unwrap();
    let parked: Arc<std::sync::Mutex<Vec<tokio::io::DuplexStream>>> = Arc::default();
    let keep = parked.clone();
    let frame = ReplicationFrame::new(0, Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"));
    let expected = frame.encoded_size() as u64;
    let encoded = frame.encode().unwrap();
    let factory: ConnectFactory = Arc::new(move |_addr| {
        let keep = keep.clone();
        let encoded = encoded.clone();
        Box::pin(async move {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            // Three `+OK`s for the REPLCONFs, then `+CONTINUE`, then one frame
            // — enough to reach Streaming and have `drain_frames` record real
            // input bytes without a full-resync installer.
            tokio::io::AsyncWriteExt::write_all(&mut client, b"+OK\r\n+OK\r\n+OK\r\n+CONTINUE\r\n")
                .await
                .unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut client, &encoded)
                .await
                .unwrap();
            // Parked, not dropped: a closed primary half would end the stream
            // immediately, racing the byte tally this test reads.
            keep.lock().unwrap().push(client);
            Ok(Box::new(server) as crate::BoxedStream)
        })
    });

    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        "127.0.0.1:6379".parse().unwrap(),
        6380,
        ReplicationIdentity::detached(ReplicationState::new()),
        tmp.path().join("state.json"),
        tmp.path().join("db"),
    );
    handler.set_connect_factory(factory);

    // The caller's own handle — never touched again except by reading it.
    let net_bytes = Arc::new(NetByteCounters::default());
    handler.set_net_bytes_counters(net_bytes.clone());

    let handler = Arc::new(handler);
    let running = handler.clone();
    let task = tokio::spawn(async move { running.start().await });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let seen = net_bytes.snapshot().input;
        if seen == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "input bytes on the caller's own counters never reached {expected}; \
             set_net_bytes_counters did not wire this handler's connections to it \
             (last read {seen})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    handler.stop();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
}

/// Saving is what makes a clean restart resume from where the replica got to
/// rather than rewinding to its boot value and forcing a full resync. A save
/// that writes nothing looks identical at runtime and only shows up as a
/// needless full resync after the next restart.
#[tokio::test]
async fn save_state_writes_the_applied_offset_to_the_state_file() {
    let tmp = tempfile::tempdir().unwrap();
    let state_path = tmp.path().join("replication_state.json");
    let identity = ReplicationIdentity::detached(ReplicationState::new());
    let (handler, _rx) = ReplicaReplicationHandler::new(
        "127.0.0.1:6379".parse().unwrap(),
        6380,
        identity.clone(),
        state_path.clone(),
        tmp.path().join("db"),
    );
    let expected_id = handler.state().await.replication_id;

    // The applier got to 4096 since boot; nothing has been persisted yet.
    identity.applied().advance_by(4096);
    assert!(!state_path.exists());

    handler.save_state().await.expect("the save must succeed");

    let reloaded = ReplicationState::load_or_create(&state_path).unwrap();
    assert_eq!(
        reloaded.offset_at_save, 4096,
        "the persisted save point must be the applied head"
    );
    assert_eq!(reloaded.replication_id, expected_id);
}

/// The port the handler announces with `REPLCONF listening-port` is the address
/// the primary renders as `slaveN:port=`; a reader that answers a constant would
/// hand every operator an address nobody can dial (FM-REPLICATION-049).
#[tokio::test]
async fn listening_port_reports_the_port_the_handler_was_built_with() {
    let tmp = tempfile::tempdir().unwrap();
    let (handler, _rx) = ReplicaReplicationHandler::new(
        "127.0.0.1:6379".parse().unwrap(),
        7137,
        ReplicationIdentity::detached(ReplicationState::new()),
        tmp.path().join("state.json"),
        tmp.path().join("db"),
    );
    assert_eq!(handler.listening_port(), 7137);
}

/// The handler does not own a private `ReplicationState`: it works on the
/// node's, so a replica reports the history the identity holds and a mutation
/// through the shared handle is visible to every reader (INFO, the frame
/// consumer) without a round trip.
#[tokio::test]
async fn state_and_shared_state_are_the_nodes_state_not_a_fresh_one() {
    let tmp = tempfile::tempdir().unwrap();
    let identity = ReplicationIdentity::detached(ReplicationState::new());
    let node_id = identity.state().read().replication_id.clone();
    let (handler, _rx) = ReplicaReplicationHandler::new(
        "127.0.0.1:6379".parse().unwrap(),
        6380,
        identity.clone(),
        tmp.path().join("state.json"),
        tmp.path().join("db"),
    );

    // A default-constructed state would carry a freshly minted id, and the
    // master host/port the constructor stamped in would be missing.
    let snapshot = handler.state().await;
    assert_eq!(snapshot.replication_id, node_id);
    assert_eq!(snapshot.master_port, Some(6379));

    let shared = handler.shared_state();
    assert!(
        Arc::ptr_eq(&shared, &identity.state()),
        "the handler must hand back the node's state handle, not a copy"
    );
    shared.write().replication_id = "shifted-by-a-holder".to_string();
    assert_eq!(handler.state().await.replication_id, "shifted-by-a-holder");
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
    let (mut handler, _rx) = ReplicaReplicationHandler::new(
        addr,
        6380,
        ReplicationIdentity::detached(state),
        state_path,
        data_dir,
    );
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
