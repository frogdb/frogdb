//! Integration tests for the DEBUG introspection commands (Phase 2 concurrency
//! quiescence probes): LOCKTABLE, WAITQUEUE, MEMORY-CHECK, EXPIRY-INDEX-CHECK.

use crate::common::replication_helpers::{start_primary_replica_pair, wait_for_replication};
use crate::common::test_server::{TestServer, TestServerConfig};
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
async fn debug_cluster_check_errors_outside_cluster_mode() {
    // A standalone node has no `ClusterState` to check against; the invariant
    // catalog is cluster-only, so DEBUG CLUSTER CHECK must say so rather than
    // silently reporting an empty (and therefore misleadingly "clean") array.
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "CLUSTER", "CHECK"]).await;
    match resp {
        Response::Error(e) => {
            assert!(String::from_utf8_lossy(&e).contains("cluster support disabled"));
        }
        other => panic!("expected error, got {other:?}"),
    }
    server.shutdown().await;
}

/// `DEBUG REPLICATION CHECK`'s reply, parsed back into `(id, detail)` pairs.
///
/// Each violation is a RESP map, which a RESP2 client sees as a flat
/// `[id, <id>, detail, <detail>]` array; RESP3 keeps it a map. Both shapes are
/// accepted so the helper does not silently pass on a protocol change.
fn parse_replication_check(resp: &Response) -> Vec<(String, String)> {
    fn bulk(v: &Response) -> String {
        match v {
            Response::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Response::Simple(s) => String::from_utf8_lossy(s).into_owned(),
            other => panic!("expected a string in a check entry, got {other:?}"),
        }
    }
    let entries = match resp {
        Response::Array(entries) => entries,
        other => panic!("DEBUG REPLICATION CHECK must reply with an array, got {other:?}"),
    };
    entries
        .iter()
        .map(|entry| {
            let fields: Vec<(String, String)> = match entry {
                Response::Map(pairs) => pairs.iter().map(|(k, v)| (bulk(k), bulk(v))).collect(),
                Response::Array(flat) => flat
                    .chunks(2)
                    .map(|kv| (bulk(&kv[0]), bulk(&kv[1])))
                    .collect(),
                other => panic!("expected a violation map, got {other:?}"),
            };
            let get = |name: &str| {
                fields
                    .iter()
                    .find(|(k, _)| k == name)
                    .unwrap_or_else(|| panic!("violation entry has no `{name}` field: {fields:?}"))
                    .1
                    .clone()
            };
            (get("id"), get("detail"))
        })
        .collect()
}

/// Standalone is a real answer, not an error: unlike its cluster twin, the
/// replication catalog is meaningful on every role — a node with no replicas
/// still has offsets, a backlog window and replication ids that can go wrong,
/// and a "not applicable" error there would hide exactly the states this
/// command exists to surface.
#[tokio::test]
async fn debug_replication_check_answers_on_a_standalone_node() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    for i in 0..5 {
        client.command(&["SET", &format!("k{i}"), "v"]).await;
    }

    let resp = client.command(&["DEBUG", "REPLICATION", "CHECK"]).await;
    let violations = parse_replication_check(&resp);
    assert!(
        violations.is_empty(),
        "a clean standalone node must report no violations, got: {violations:?}"
    );

    server.shutdown().await;
}

/// A primary with a live replica: the role that owns the backlog, the session
/// table and the fence, so this is where most of the catalog is actually
/// evaluated rather than skipped.
///
/// The writes are load-bearing, not decoration: the self-fence latches its
/// arming on the write path, so a primary that has never been written to
/// reports `INV-FENCE-1` while its replica streams (issue 19, pinned by
/// `debug_replication_check_renders_a_violating_states_id_and_detail`).
#[tokio::test]
async fn debug_replication_check_answers_on_a_primary() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let mut client = primary.connect().await;
    for i in 0..5 {
        client.command(&["SET", &format!("k{i}"), "v"]).await;
    }
    wait_for_replication(&primary, 5000).await;

    let resp = client.command(&["DEBUG", "REPLICATION", "CHECK"]).await;
    let violations = parse_replication_check(&resp);
    assert!(
        violations.is_empty(),
        "a healthy primary must report no violations, got: {violations:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// The replica half of the same pair. The apply gate and the applied/live
/// offset pair only exist on this role, so the command has to answer here too
/// for `INV-OFFSET-1` to ever be evaluated against a following node.
#[tokio::test]
async fn debug_replication_check_answers_on_a_replica() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let mut writer = primary.connect().await;
    for i in 0..5 {
        writer.command(&["SET", &format!("k{i}"), "v"]).await;
    }
    wait_for_replication(&primary, 5000).await;

    let mut client = replica.connect().await;
    let resp = client.command(&["DEBUG", "REPLICATION", "CHECK"]).await;
    let violations = parse_replication_check(&resp);
    assert!(
        violations.is_empty(),
        "a healthy replica must report no violations, got: {violations:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// The violating-state half of the contract, end to end: a real node in a real
/// state the catalog rejects renders that entry's id **and** its detail, and it
/// is a `Hard`-tier entry that only exists because this surface fills the fence
/// group (the in-crate hooks skip `INV-FENCE-1` for want of it) — so this
/// doubles as the proof that the assembled view is complete.
///
/// The state: a primary whose replica is streaming but which has served no
/// write yet. `ReplicationQuorumChecker` latches its arming lazily, inside
/// `has_quorum()` on the write path, so between the replica reaching Streaming
/// and the primary's next write the checker is unarmed while a session streams
/// — exactly what `INV-FENCE-1` calls the dead-detector shape.
///
/// This asserts today's behaviour deliberately; issue 19 of
/// `.scratch/replication-correctness/issues/` rules on the underlying arming
/// gap, and `debug_replication_check_is_clean_on_a_primary_before_its_first_write`
/// is the muzzled witness for the outcome. Flip the two when the ruling lands.
#[tokio::test]
async fn debug_replication_check_renders_a_violating_states_id_and_detail() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let mut client = primary.connect().await;

    // Poll rather than sleep: the entry only appears once the session actually
    // reaches Streaming, and no write may be issued in the meantime.
    let mut violations = Vec::new();
    for _ in 0..100 {
        violations =
            parse_replication_check(&client.command(&["DEBUG", "REPLICATION", "CHECK"]).await);
        if !violations.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(
        violations.len(),
        1,
        "expected exactly the unarmed-fence violation, got: {violations:?}"
    );
    assert_eq!(violations[0].0, "INV-FENCE-1");
    assert!(
        violations[0].1.contains("streaming")
            && violations[0].1.contains("self-fence checker is unarmed"),
        "the detail must describe the state, not just name the entry, got: {:?}",
        violations[0].1
    );

    // A single write arms the checker, and the same node then reports clean —
    // the empty-array half of the same assertion, on the same node.
    client.command(&["SET", "arming", "1"]).await;
    wait_for_replication(&primary, 5000).await;
    let after = parse_replication_check(&client.command(&["DEBUG", "REPLICATION", "CHECK"]).await);
    assert!(
        after.is_empty(),
        "the same primary must report clean once the fence has armed, got: {after:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

/// Muzzled witness for issue 19: arming is latched on the write path, so a
/// primary that has served no write since its replica began streaming has an
/// unarmed self-fence. It is not merely a reporting artifact — if that replica
/// is then lost, the next write's `arm_if_streaming` finds no streaming replica
/// to arm from and the fence never engages, which is the failure
/// FM-REPLICATION-041/062 exists to prevent.
///
/// Un-ignore (and drop the corresponding assertion from
/// `debug_replication_check_renders_a_violating_states_id_and_detail`) when the
/// ruling lands.
#[tokio::test]
#[ignore = "issue 19: the self-fence arms lazily on the write path, not when a session starts streaming"]
async fn debug_replication_check_is_clean_on_a_primary_before_its_first_write() {
    let (primary, replica) = start_primary_replica_pair(TestServerConfig::default()).await;
    let mut client = primary.connect().await;
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let violations =
        parse_replication_check(&client.command(&["DEBUG", "REPLICATION", "CHECK"]).await);
    assert!(
        violations.is_empty(),
        "a primary with a streaming replica must have an armed fence before it is written to, \
         got: {violations:?}"
    );

    replica.shutdown().await;
    primary.shutdown().await;
}

#[tokio::test]
async fn debug_replication_unknown_subcommand_errors() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;
    let resp = client.command(&["DEBUG", "REPLICATION", "NOPE"]).await;
    match resp {
        Response::Error(e) => {
            assert!(
                String::from_utf8_lossy(&e).contains("Unknown DEBUG REPLICATION subcommand"),
                "got: {}",
                String::from_utf8_lossy(&e)
            );
        }
        other => panic!("expected error, got {other:?}"),
    }
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

#[tokio::test]
async fn debug_expiry_index_check_consistent_after_expire() {
    let server = TestServer::start_standalone().await;
    let mut client = server.connect().await;

    // A persistent key and a key with a long TTL: the index must be consistent.
    assert!(matches!(
        client.command(&["SET", "persistent", "v"]).await,
        Response::Simple(_)
    ));
    assert!(matches!(
        client.command(&["SET", "ttl", "v", "EX", "3600"]).await,
        Response::Simple(_)
    ));

    let resp = client.command(&["DEBUG", "EXPIRY-INDEX-CHECK"]).await;
    match resp {
        Response::Bulk(Some(b)) => assert_eq!(&b[..], b"# expiry index is consistent"),
        other => panic!("expected consistent-sentinel bulk, got {other:?}"),
    }

    server.shutdown().await;
}
