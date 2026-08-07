//! Slot-scoped pause barrier: the pause dimension the slot-migration
//! finalization barrier arms (rework issue 02, sequencing step 2).
//!
//! A node-global `CLIENT PAUSE` quiesces the whole node — Redis semantics,
//! unchanged. A *slot-scoped* pause parks only the traffic that can reach one
//! hash slot, so a finalizing migration can freeze its own slot without
//! stalling the other 16383. The two dimensions arm, expire, and release
//! independently: `CLIENT UNPAUSE` cannot disarm a migration barrier, and a
//! barrier release cannot cancel an operator's pause.
//!
//! Tests drive the slot dimension through `DEBUG PAUSE-SLOT <slot> <ms>
//! [WRITE|ALL]` rather than a slot argument on `CLIENT PAUSE`: Redis has no
//! such argument and the parity break would be visible to every client library.
//! Production arms it from the migration path, not from the wire.
//!
//! The two hazards pinned here are self-deadlocks, not inconveniences. `MIGRATE`
//! is `WRITE`-flagged and runs over an ordinary client connection — it is how
//! the source finishes copying the slot the barrier is waiting on, so a barrier
//! that parks it waits forever on work only it could have released. `CLUSTER`
//! carries `CLUSTER SETSLOT <slot> STABLE`, the operator's escape hatch for a
//! migration that has gone wrong; a barrier that can swallow its own cancel
//! command is unrecoverable without a restart.
#![cfg(not(feature = "turmoil"))]

use frogdb_protocol::Response;
use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use frogdb_test_harness::cluster_helpers::{is_error, key_for_slot, slot_for_key};
use frogdb_test_harness::server::{TestClient, TestServer};
use std::time::Duration;

/// How long a probe waits before calling a command "parked".
///
/// Every unparked path in these tests is a local in-memory command that answers
/// in microseconds, so this only has to exceed scheduling noise, not any real
/// server work.
const PARK_PROBE: Duration = Duration::from_millis(400);

/// How long a released command gets to produce its reply.
const RELEASE_WAIT: Duration = Duration::from_secs(5);

/// The slot every test barriers. Any slot works; a fixed one keeps the
/// `DEBUG PAUSE-SLOT` arguments readable.
const BARRIERED_SLOT: u16 = 1000;

/// A second slot, deliberately not barriered.
const FREE_SLOT: u16 = 2000;

/// Two distinct keys that hash to the same slot as `base`, via a hash tag.
fn tagged(base: &str, suffix: &str) -> String {
    format!("{{{base}}}{suffix}")
}

/// Assert a command does not answer within [`PARK_PROBE`] — i.e. it is parked.
async fn assert_parked(client: &mut TestClient, args: &[&str], what: &str) {
    client.send_only(args).await;
    assert!(
        client.read_response(PARK_PROBE).await.is_none(),
        "{what} should be parked by the barrier, but answered immediately"
    );
}

/// Assert a previously parked command answers once the barrier is released.
async fn assert_released(client: &mut TestClient, what: &str) -> Response {
    client
        .read_response(RELEASE_WAIT)
        .await
        .unwrap_or_else(|| panic!("{what} never answered after the barrier released"))
}

// FM-CLUSTER-079
/// A slot-scoped pause parks writes to its slot and nothing else: another
/// slot's writes flow, and reads of the barriered slot flow (the node still
/// owns the data). A command that cannot be pinned to any slot is treated
/// fail-closed — `FLUSHALL` names no keys yet erases the barriered slot along
/// with everything else.
#[tokio::test]
async fn slot_scoped_pause_parks_only_its_slots_writes() {
    let server = TestServer::start_standalone().await;
    let barriered = key_for_slot(BARRIERED_SLOT);
    let free = key_for_slot(FREE_SLOT);

    let mut admin = server.connect().await;
    assert!(
        !is_error(
            &admin
                .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "5000"])
                .await
        ),
        "DEBUG PAUSE-SLOT should arm the barrier"
    );

    // The barriered slot's writes park.
    let mut writer = server.connect().await;
    assert_parked(
        &mut writer,
        &["SET", &barriered, "v"],
        "SET on the barriered slot",
    )
    .await;

    // Everything else keeps serving.
    let mut bystander = server.connect().await;
    assert_eq!(
        bystander.command(&["SET", &free, "v"]).await,
        Response::ok(),
        "a write to an unbarriered slot must not be parked"
    );
    assert!(
        !is_error(&bystander.command(&["GET", &barriered]).await),
        "a read of the barriered slot must not be parked by a WRITE-mode barrier"
    );

    // Unpinnable write: fail-closed.
    let mut wiper = server.connect().await;
    assert_parked(&mut wiper, &["FLUSHALL"], "FLUSHALL (pins to no slot)").await;

    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "0"])
            .await,
        Response::ok(),
        "0 ms should disarm the barrier"
    );

    assert_eq!(
        assert_released(&mut writer, "SET on the barriered slot").await,
        Response::ok()
    );
    assert_eq!(
        assert_released(&mut wiper, "FLUSHALL").await,
        Response::ok()
    );

    server.shutdown().await;
}

// FM-CLUSTER-080
/// Hazard 3: `MIGRATE` is `WRITE`-flagged and runs on a normal client
/// connection. It is the barrier's own catch-up copy, so a slot-scoped pause
/// must let it through — the barrier is waiting on exactly the work it would
/// otherwise park. Pinned end to end: a real `MIGRATE` moves a real key to a
/// second server while that key's slot is barriered on the source.
#[tokio::test]
async fn catch_up_migrate_runs_while_its_slot_is_barriered() {
    let source = TestServer::start_standalone().await;
    let target = TestServer::start_standalone().await;

    // Two keys in the barriered slot: one the MIGRATE carries, one that proves
    // the barrier really is armed rather than the MIGRATE succeeding vacuously.
    let base = key_for_slot(BARRIERED_SLOT);
    let moving = tagged(&base, ":moving");
    let staying = tagged(&base, ":staying");
    assert_eq!(slot_for_key(moving.as_bytes()), BARRIERED_SLOT);
    assert_eq!(slot_for_key(staying.as_bytes()), BARRIERED_SLOT);

    let mut admin = source.connect().await;
    assert_eq!(
        admin.command(&["SET", &moving, "payload"]).await,
        Response::ok()
    );
    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "5000"])
            .await,
        Response::ok()
    );

    // The barrier is armed: an ordinary write to the slot parks.
    let mut parked = source.connect().await;
    assert_parked(
        &mut parked,
        &["SET", &staying, "v"],
        "SET on the barriered slot",
    )
    .await;

    // ... and MIGRATE, on that same slot, is not.
    let mut migrator = source.connect().await;
    let port = target.port().to_string();
    let migrate = tokio::time::timeout(
        RELEASE_WAIT,
        migrator.command(&["MIGRATE", "127.0.0.1", &port, &moving, "0", "5000"]),
    )
    .await
    .expect("MIGRATE was parked behind the barrier for its own slot — hazard 3");
    assert_eq!(
        migrate,
        Response::ok(),
        "MIGRATE should have copied the key while the slot was barriered"
    );

    let mut on_target = target.connect().await;
    assert_eq!(
        on_target.command(&["GET", &moving]).await,
        Response::bulk("payload"),
        "the migrated key should be on the target"
    );

    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "0"])
            .await,
        Response::ok()
    );
    assert_eq!(
        assert_released(&mut parked, "SET on the barriered slot").await,
        Response::ok()
    );

    source.shutdown().await;
    target.shutdown().await;
}

// FM-CLUSTER-081
/// Hazard 4: the cluster control plane must never be parked by a slot-scoped
/// pause. `CLUSTER SETSLOT <slot> STABLE` is the escape hatch for cancelling a
/// migration, so a barrier that parks it is unrecoverable without a restart.
/// Forced under `ALL` — the strongest slot-scoped mode — because that is the
/// only mode under which the exemption is load-bearing.
#[tokio::test]
async fn cluster_control_plane_is_never_parked_by_a_slot_barrier() {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(3).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();

    let node_ids = harness.node_ids();
    let node = harness.node(node_ids[0]).unwrap();
    let slot = BARRIERED_SLOT.to_string();

    assert_eq!(
        node.send("DEBUG", &["PAUSE-SLOT", &slot, "5000", "ALL"])
            .await,
        Response::ok()
    );

    // The barrier is armed, and under ALL it parks even reads.
    let barriered_key = key_for_slot(BARRIERED_SLOT);
    assert!(
        tokio::time::timeout(PARK_PROBE, node.send("GET", &[&barriered_key]))
            .await
            .is_err(),
        "an ALL-mode slot barrier should park reads of its slot"
    );

    // CLUSTER is exempt regardless.
    let stable = tokio::time::timeout(
        RELEASE_WAIT,
        node.send("CLUSTER", &["SETSLOT", &slot, "STABLE"]),
    )
    .await
    .expect("CLUSTER SETSLOT STABLE was parked by the slot barrier — hazard 4");
    assert!(
        !is_error(&stable),
        "CLUSTER SETSLOT STABLE should succeed while the slot is barriered: {stable:?}"
    );

    assert_eq!(
        node.send("DEBUG", &["PAUSE-SLOT", &slot, "0"]).await,
        Response::ok()
    );

    harness.shutdown_all().await;
}

// FM-CLUSTER-082
/// The operator's node-global pause and a migration's slot barrier compose
/// without either one being able to release the other. `CLIENT UNPAUSE` leaves
/// the barrier armed; disarming the barrier leaves the operator's pause in
/// force.
#[tokio::test]
async fn node_pause_and_slot_barrier_release_independently() {
    let server = TestServer::start_standalone().await;
    let barriered = key_for_slot(BARRIERED_SLOT);
    let free = key_for_slot(FREE_SLOT);

    let mut admin = server.connect().await;
    assert_eq!(
        admin.command(&["CLIENT", "PAUSE", "10000", "WRITE"]).await,
        Response::ok()
    );
    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "10000"])
            .await,
        Response::ok()
    );

    // Both dimensions in force: every write parks, whatever its slot.
    let mut probe = server.connect().await;
    assert_parked(&mut probe, &["SET", &free, "v"], "SET under the node pause").await;

    // Releasing the operator's pause must not release the barrier.
    assert_eq!(admin.command(&["CLIENT", "UNPAUSE"]).await, Response::ok());
    assert_eq!(
        assert_released(&mut probe, "SET on the unbarriered slot").await,
        Response::ok()
    );

    let mut barrier_probe = server.connect().await;
    assert_parked(
        &mut barrier_probe,
        &["SET", &barriered, "v"],
        "SET on the barriered slot after CLIENT UNPAUSE",
    )
    .await;

    // And the reverse: re-arm the node pause, drop the barrier, and the node
    // pause is still in force.
    assert_eq!(
        admin.command(&["CLIENT", "PAUSE", "10000", "WRITE"]).await,
        Response::ok()
    );
    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "0"])
            .await,
        Response::ok()
    );
    let mut node_probe = server.connect().await;
    assert_parked(
        &mut node_probe,
        &["SET", &free, "v2"],
        "SET after the barrier released but the node pause stands",
    )
    .await;

    assert_eq!(admin.command(&["CLIENT", "UNPAUSE"]).await, Response::ok());
    assert_eq!(
        assert_released(&mut node_probe, "SET under the node pause").await,
        Response::ok()
    );
    assert_eq!(
        assert_released(&mut barrier_probe, "SET on the barriered slot").await,
        Response::ok()
    );

    server.shutdown().await;
}

// FM-CLUSTER-083
/// A write transaction parks at `EXEC`, not while queuing, and commits only
/// after the barrier releases — so the post-wait re-validation in
/// `txn/src/exec.rs` runs against the topology that exists *after* the
/// handover rather than the one that existed when the batch was queued. Phase 1
/// gates `EXEC` on any armed pause: the `TxnHost` seam hands `wait_if_paused` no
/// queue, so the batch's slot cannot be resolved, and over-parking a batch is
/// the safe direction.
#[tokio::test]
async fn write_exec_parks_on_a_slot_barrier_and_commits_after_release() {
    let server = TestServer::start_standalone().await;
    let barriered = key_for_slot(BARRIERED_SLOT);

    let mut admin = server.connect().await;
    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "10000"])
            .await,
        Response::ok()
    );

    // Queuing is never parked — only the commit is.
    let mut txn = server.connect().await;
    assert_eq!(txn.command(&["MULTI"]).await, Response::ok());
    assert_eq!(
        txn.command(&["SET", &barriered, "v"]).await,
        Response::queued()
    );

    assert_parked(&mut txn, &["EXEC"], "EXEC of a write batch").await;

    assert_eq!(
        admin
            .command(&["DEBUG", "PAUSE-SLOT", &BARRIERED_SLOT.to_string(), "0"])
            .await,
        Response::ok()
    );
    assert_eq!(
        assert_released(&mut txn, "EXEC of a write batch").await,
        Response::Array(vec![Response::ok()])
    );

    server.shutdown().await;
}
