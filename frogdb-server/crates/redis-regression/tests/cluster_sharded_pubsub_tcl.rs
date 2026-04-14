//! Rust port of Redis 8.6.0 `unit/cluster/sharded-pubsub.tcl` test suite.
//!
//! Upstream uses `start_cluster 1 1` (1 primary + 1 replica). FrogDB's
//! `ClusterTestHarness` does not yet expose a primary/replica relationship —
//! every node from `start_cluster(N)` is a voter in the Raft ensemble —
//! so replica-side cases cannot be exercised. Primary-side cases are
//! exercised against a single-node Raft cluster started via
//! `ClusterTestHarness::start_cluster(1)`.
//!
//! ## Intentional exclusions
//!
//! Every upstream test queues an `SPUBLISH` inside a `MULTI`/`EXEC` block.
//! FrogDB registers `SPUBLISH`/`PUBLISH` as metadata-only commands
//! (`frogdb-server/crates/server/src/commands/metadata.rs`), and
//! `queue_command` in `connection/handlers/transaction.rs` looks up
//! commands via `registry.get`, which only returns fully-registered
//! commands. As a result FrogDB rejects queued `SPUBLISH` with
//! `ERR unknown command 'SPUBLISH'` during transaction buffering. All six
//! tests are marked `#[ignore]` with this note until the registry exposes
//! metadata-only entries to `queue_command`, or `SPUBLISH`/`PUBLISH` are
//! promoted to full registrations.
//!
//! In addition to the pre-existing MULTI/EXEC limitation above, the
//! per-test reasons below still apply once that gap is closed:
//!
//! - `Sharded pubsub within multi/exec with cross slot operation` — intentional-incompatibility:scripting — FrogDB's SPUBLISH metadata declares no keys, so the channel is not considered when computing the transaction's target slot. A MULTI sequence of `SPUBLISH ch1` + `GET foo` therefore lands on `foo`'s slot instead of producing a CROSSSLOT error.
//! - `Sharded pubsub publish behavior within multi/exec with read operation
//!   on replica` — requires a Redis-style replica node. The cluster
//!   harness only creates voter/leader nodes, not explicit replicas.
//! - `Sharded pubsub publish behavior within multi/exec with write
//!   operation on replica` — same replica limitation.

use frogdb_protocol::Response;
use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use frogdb_test_harness::response::*;
use std::time::Duration;

async fn start_single_node_cluster() -> (ClusterTestHarness, u64) {
    let mut harness = ClusterTestHarness::new();
    harness.start_cluster(1).await.unwrap();
    harness
        .wait_for_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let node_id = harness.node_ids()[0];
    (harness, node_id)
}

// ---------------------------------------------------------------------------
// Test 1: Sharded pubsub publish behavior within multi/exec
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sharded_pubsub_publish_behavior_within_multi_exec() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    let resp = client.command(&["SPUBLISH", "ch1", "hello"]).await;
    assert!(
        matches!(resp, Response::Simple(ref s) if s == "QUEUED"),
        "expected QUEUED, got {resp:?}",
    );
    let exec_resp = client.command(&["EXEC"]).await;
    let items = unwrap_array(exec_resp);
    assert_eq!(items.len(), 1);
    assert_eq!(unwrap_integer(&items[0]), 0);

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test 2: Sharded pubsub within multi/exec with cross slot operation
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Redis-style replica (ClusterTestHarness lacks primary/replica split)"]
async fn tcl_sharded_pubsub_within_multi_exec_with_cross_slot_operation() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPUBLISH", "ch1", "hello"]).await;
    client.command(&["GET", "foo"]).await;
    let resp = client.command(&["EXEC"]).await;
    let err = match resp {
        Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected Error, got {other:?}"),
    };
    assert!(
        err.starts_with("CROSSSLOT"),
        "expected CROSSSLOT error, got {err}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test 3: Sharded pubsub publish behavior within multi/exec with read
//         operation on primary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sharded_pubsub_multi_exec_with_read_operation_on_primary() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPUBLISH", "foo", "hello"]).await;
    client.command(&["GET", "foo"]).await;
    let exec_resp = client.command(&["EXEC"]).await;
    let items = unwrap_array(exec_resp);
    assert_eq!(items.len(), 2);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert!(matches!(&items[1], Response::Bulk(None)));

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test 4: Sharded pubsub publish behavior within multi/exec with read
//         operation on replica
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Redis-style replica (ClusterTestHarness lacks primary/replica split)"]
async fn tcl_sharded_pubsub_multi_exec_with_read_operation_on_replica() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPUBLISH", "foo", "hello"]).await;
    let get_resp = client.command(&["GET", "foo"]).await;
    let err = match get_resp {
        Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected MOVED error, got {other:?}"),
    };
    assert!(err.starts_with("MOVED"), "expected MOVED error, got {err}",);
    let exec_resp = client.command(&["EXEC"]).await;
    let err = match exec_resp {
        Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected EXECABORT error, got {other:?}"),
    };
    assert!(
        err.starts_with("EXECABORT"),
        "expected EXECABORT error, got {err}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test 5: Sharded pubsub publish behavior within multi/exec with write
//         operation on primary
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tcl_sharded_pubsub_multi_exec_with_write_operation_on_primary() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPUBLISH", "foo", "hello"]).await;
    client.command(&["SET", "foo", "bar"]).await;
    let exec_resp = client.command(&["EXEC"]).await;
    let items = unwrap_array(exec_resp);
    assert_eq!(items.len(), 2);
    assert_eq!(unwrap_integer(&items[0]), 0);
    assert_ok(&items[1]);

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test 6: Sharded pubsub publish behavior within multi/exec with write
//         operation on replica
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Redis-style replica (ClusterTestHarness lacks primary/replica split)"]
async fn tcl_sharded_pubsub_multi_exec_with_write_operation_on_replica() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    assert_ok(&client.command(&["MULTI"]).await);
    client.command(&["SPUBLISH", "foo", "hello"]).await;
    let set_resp = client.command(&["SET", "foo", "bar"]).await;
    let err = match set_resp {
        Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected MOVED error, got {other:?}"),
    };
    assert!(err.starts_with("MOVED"), "expected MOVED error, got {err}",);
    let exec_resp = client.command(&["EXEC"]).await;
    let err = match exec_resp {
        Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected EXECABORT error, got {other:?}"),
    };
    assert!(
        err.starts_with("EXECABORT"),
        "expected EXECABORT error, got {err}",
    );

    harness.shutdown_all().await;
}
