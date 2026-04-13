//! Rust port of Redis 8.6.0 `unit/cluster/scripting.tcl` test suite.
//!
//! Upstream uses `start_cluster 1 0` (1 primary, 0 replicas). FrogDB's
//! `ClusterTestHarness::start_cluster(1)` spins up an equivalent single-node
//! Raft cluster.
//!
//! ## Intentional exclusions
//!
//! All six upstream tests depend on Redis-specific scripting behaviour that
//! FrogDB does not currently implement. Each test below is marked
//! `#[ignore]` with a note; the inclusion keeps the port entries visible to
//! the audit tracker and makes future parity work easy to spot.
//!
//! - `Eval scripts with shebangs and functions default to no cross slots` — intentional-incompatibility:scripting — FrogDB's EVAL shebang parser does not recognise `#!lua` / `flags=...`, and `redis.call('set', 'foo', ...)` from a script with `0` declared keys fails with FrogDB's strict `UndeclaredKey` error rather than the Redis `Script attempted to access keys that do not hash to the same slot` message. FUNCTION LOAD also trips strict-key-validation before the cross-slot check can fire.
//!
//! - `Cross slot commands are allowed by default for eval scripts and with
//!   allow-cross-slot-keys flag` — same strict-key-validation path. FrogDB
//!   requires `KEYS[...]` declarations for any key accessed via
//!   `redis.call`, so the old-style EVAL used by this test fails up-front.
//!
//! - `Cross slot commands are also blocked if they disagree with
//!   pre-declared keys` — depends on shebang `flags=allow-cross-slot-keys`
//!   parsing in EVAL (unimplemented) and on strict-key-validation
//!   differences.
//!
//! - `Cross slot commands are allowed by default if they disagree with
//!   pre-declared keys` — depends on `CLUSTER COUNTKEYSINSLOT`. Even if
//!   the script side is worked around, the upstream test asserts the key
//!   written via a non-declared name ends up in the correct slot, which
//!   currently requires the strict-validation diff above.
//!
//! - `Function no-cluster flag` — intentional-incompatibility:scripting — FrogDB parses the `no-cluster` flag in FUNCTION LOAD and stores it in `FunctionFlags::NO_CLUSTER`, but the flag is never consulted at FCALL time, so the upstream "Can not run script on cluster, 'no-cluster' flag is set" error is never produced.
//!
//! - `Script no-cluster flag` — intentional-incompatibility:scripting — FrogDB's EVAL handler does not parse `#!lua flags=no-cluster`, so the `no-cluster` flag has no effect on EVAL scripts at all.

use frogdb_test_harness::cluster_harness::ClusterTestHarness;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helper: start a single-node cluster and return the harness + node id.
// ---------------------------------------------------------------------------

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
// Test: Eval scripts with shebangs and functions default to no cross slots
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB strict key validation + EVAL does not parse shebang flags (see intentional exclusions)"]
async fn tcl_eval_scripts_with_shebangs_and_functions_default_to_no_cross_slots() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    // Upstream expects the shebang-ified script to fail with
    // "Script attempted to access keys that do not hash to the same slot".
    let script = r#"#!lua
        redis.call('set', 'foo', 'bar')
        redis.call('set', 'bar', 'foo')
        return 'OK'
    "#;
    let resp = client.command(&["EVAL", script, "0"]).await;
    let err = match resp {
        frogdb_protocol::Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("Script attempted to access keys that do not hash to the same slot"),
        "unexpected error: {err}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test: Cross slot commands are allowed by default for eval scripts and
//       with allow-cross-slot-keys flag
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB strict key validation rejects undeclared keys before cross-slot is considered"]
async fn tcl_cross_slot_commands_are_allowed_by_default_for_eval_scripts_and_with_allow_cross_slot_keys_flag()
 {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    // Old-style EVAL should accept cross-slot writes.
    let resp = client
        .command(&[
            "EVAL",
            "redis.call('set', 'foo', 'bar'); redis.call('set', 'bar', 'foo')",
            "0",
        ])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "expected success, got {resp:?}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test: Cross slot commands are also blocked if they disagree with
//       pre-declared keys
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB EVAL does not parse shebang `flags=`; strict key validation rejects first"]
async fn tcl_cross_slot_commands_are_also_blocked_if_they_disagree_with_pre_declared_keys() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let script = r#"#!lua
        redis.call('set', 'foo', 'bar')
        return 'OK'
    "#;
    let resp = client.command(&["EVAL", script, "1", "bar"]).await;
    let err = match resp {
        frogdb_protocol::Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("Script attempted to access keys that do not hash to the same slot"),
        "unexpected error: {err}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test: Cross slot commands are allowed by default if they disagree with
//       pre-declared keys
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB strict key validation rejects accessing `foo` when only `bar` is declared"]
async fn tcl_cross_slot_commands_are_allowed_by_default_if_they_disagree_with_pre_declared_keys() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    client.command(&["FLUSHALL"]).await;
    let resp = client
        .command(&["EVAL", "redis.call('set', 'foo', 'bar')", "1", "bar"])
        .await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "expected success, got {resp:?}",
    );

    // Upstream verifies the write landed in `foo`'s slot.
    let resp = client
        .command(&["CLUSTER", "COUNTKEYSINSLOT", "12182"])
        .await;
    assert_eq!(
        frogdb_test_harness::response::unwrap_integer(&resp),
        1,
        "key should have landed in foo's slot",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test: Function no-cluster flag
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB parses the `no-cluster` function flag but never enforces it at FCALL time"]
async fn tcl_function_no_cluster_flag() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let code = r#"#!lua name=test
        redis.register_function{function_name='f1', callback=function() return 'hello' end, flags={'no-cluster'}}
    "#;
    let resp = client.command(&["FUNCTION", "LOAD", code]).await;
    assert!(
        !matches!(resp, frogdb_protocol::Response::Error(_)),
        "FUNCTION LOAD should succeed, got {resp:?}",
    );

    let resp = client.command(&["FCALL", "f1", "0"]).await;
    let err = match resp {
        frogdb_protocol::Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("Can not run script on cluster, 'no-cluster' flag is set"),
        "unexpected error: {err}",
    );

    harness.shutdown_all().await;
}

// ---------------------------------------------------------------------------
// Test: Script no-cluster flag
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "FrogDB's EVAL does not parse shebang `flags=` so `no-cluster` has no effect"]
async fn tcl_script_no_cluster_flag() {
    let (mut harness, node_id) = start_single_node_cluster().await;
    let node = harness.node(node_id).unwrap();
    let mut client = node.connect().await;

    let script = r#"#!lua flags=no-cluster
        return 1
    "#;
    let resp = client.command(&["EVAL", script, "0"]).await;
    let err = match resp {
        frogdb_protocol::Response::Error(e) => String::from_utf8_lossy(&e).to_string(),
        other => panic!("expected error, got {other:?}"),
    };
    assert!(
        err.contains("Can not run script on cluster, 'no-cluster' flag is set"),
        "unexpected error: {err}",
    );

    harness.shutdown_all().await;
}
