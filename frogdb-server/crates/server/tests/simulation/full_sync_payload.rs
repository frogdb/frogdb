//! What a full-resync payload *contains*, checked from a second node
//! (replication-correctness issue
//! `.scratch/replication-correctness/issues/done/25-no-layer-sees-what-a-full-resync-payload-contains.md`).
//!
//! The defect this layer exists for is a checkpoint cut before the shard WALs
//! have drained: a write the primary already acked is then absent from the
//! payload a full-syncing replica loads, and the replica installs a keyspace
//! that is internally coherent and short of an acked write.
//!
//! No existing layer sees it. The replication view the catalog, the proptests
//! and the stateright models all share carries offsets and phases, never
//! keyspace, so none of them can state the violated fact. The seeded sweep in
//! [`super::replication_scheduler`] runs real servers and already compares
//! keys across nodes at quiesce, but it builds every node with the shipped
//! 10 ms batch-flush window, which under turmoil's virtual clock closes long
//! before any checkpoint is cut — the state the defect needs is never open, at
//! any seed. This module opens it on purpose: the batch window is set wider
//! than the whole simulation, so every write the primary acks is still sitting
//! in its shard's batch when the replica attaches, and the drain ahead of the
//! cut is the only thing that can put those writes in the payload.
//!
//! Deterministic and fault-free by construction: the claim is about what a
//! healthy full sync carries, so a fault would only add ways to fail.
//!
//! The same run also measures the transfer, which closes the second gap the
//! gate found (issue
//! `.scratch/replication-correctness/issues/done/27-nothing-but-its-own-tests-watches-the-replication-byte-counters.md`):
//! a node that has just shipped or installed a payload and reports zero
//! replication bytes is reporting a number nothing counts.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use turmoil::Builder;

use super::{RespConn, RespValue};
use crate::common::sim_helpers::{
    ReplicationNodeParams, SERVER_PORT, real_frogdb_replication_node,
};

/// The primary, which acks the whole keyspace before the replica exists.
const PRIMARY_HOST: &str = "payload-primary";

/// The replica, which attaches afterwards and so can only be served a
/// `PSYNC ? -1` full resync.
const REPLICA_HOST: &str = "payload-replica";

/// Batch-flush window wider than the simulation: nothing written during the run
/// reaches RocksDB on a timer, so "acked but not yet flushed" holds for the
/// whole run rather than for a few microseconds.
const NEVER_FLUSHES_MS: u64 = 600_000;

/// Shards the keyspace below spreads over. More than one because the drain is
/// per-shard: a checkpoint that drains only the shard it happens to ask first
/// is still wrong.
const NUM_SHARDS: usize = 4;

/// The writes the primary acks before the replica attaches.
const KEYS: [(&str, &str); 5] = [
    ("alpha", "1"),
    ("bravo", "2"),
    ("charlie", "3"),
    ("delta", "4"),
    ("echo", "5"),
];

/// Interval between polls, and the number of them before a wait is called a
/// failure.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_POLLS: u32 = 600;

/// A full resync must carry writes the primary acked while they were still
/// inside the shard batch window.
///
/// The primary boots with persistence on and a batch window wider than the run,
/// then acks five `SET`s. Only then does the replica boot, so its handshake is a
/// `PSYNC ? -1` over a keyspace that lives entirely in un-flushed batches. Once
/// the replica reports `master_link_status:up`, every acked value has to be
/// readable on it: reaching `Streaming` is the primary's claim that the payload
/// was complete.
#[test]
fn a_full_resync_carries_writes_still_sitting_in_the_batch_window() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(300))
        .build();

    // Nested one level inside each tempdir, never the tempdir itself: a full
    // sync stages its payload in a `checkpoint_ready` directory that is a
    // sibling of the data dir, so data dirs sitting directly in `$TMPDIR` share
    // one staging path across concurrently running tests.
    let primary_dir = tempfile::tempdir().expect("primary data dir");
    let replica_dir = tempfile::tempdir().expect("replica data dir");
    let primary_path = primary_dir.path().join("data");
    let replica_path = replica_dir.path().join("data");
    std::fs::create_dir_all(&primary_path).expect("primary data dir");
    std::fs::create_dir_all(&replica_path).expect("replica data dir");

    // Released by the driver once every write is acked; until then the replica
    // host is booted but has not started a server, so nothing on the primary can
    // be shipped incrementally.
    let acked = Arc::new(AtomicBool::new(false));

    sim.host(PRIMARY_HOST, move || {
        let path = primary_path.clone();
        async move {
            let params = ReplicationNodeParams {
                num_shards: NUM_SHARDS,
                persistence: true,
                batch_timeout_ms: NEVER_FLUSHES_MS,
                ..Default::default()
            };
            if let Err(e) = real_frogdb_replication_node(params, path).await {
                eprintln!("primary exited with error: {e}");
                return Err(e);
            }
            Ok(())
        }
    });

    let replica_gate = acked.clone();
    sim.host(REPLICA_HOST, move || {
        let path = replica_path.clone();
        let gate = replica_gate.clone();
        async move {
            while !gate.load(Ordering::SeqCst) {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            let params = ReplicationNodeParams {
                num_shards: NUM_SHARDS,
                primary_ip: Some(turmoil::lookup(PRIMARY_HOST)),
                persistence: true,
                batch_timeout_ms: NEVER_FLUSHES_MS,
                ..Default::default()
            };
            if let Err(e) = real_frogdb_replication_node(params, path).await {
                eprintln!("replica exited with error: {e}");
                return Err(e);
            }
            Ok(())
        }
    });

    sim.client("driver", async move {
        let mut primary = connect(PRIMARY_HOST).await;
        for (key, value) in KEYS {
            let reply = primary
                .cmd(&[b"SET", key.as_bytes(), value.as_bytes()])
                .await?;
            assert_eq!(
                reply,
                RespValue::Simple("OK".to_string()),
                "primary refused SET {key}, so there is no acked write to look for"
            );
        }

        acked.store(true, Ordering::SeqCst);
        await_link_up().await;

        assert!(
            stats_counter(&mut primary, "sync_full").await >= 1,
            "the replica attached without a full resync, so this run never \
             exercised the payload path"
        );

        let mut replica = connect(REPLICA_HOST).await;
        for (key, value) in KEYS {
            let reply = replica.cmd(&[b"GET", key.as_bytes()]).await?;
            let read = match &reply {
                RespValue::Bulk(Some(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
                _ => None,
            };
            assert_eq!(
                read.as_deref(),
                Some(value),
                "acked write {key}={value} is missing from the full-resync payload the \
                 replica loaded (got {reply:?}): the checkpoint was cut before the shard \
                 WALs holding it were drained"
            );
        }

        // The same transfer, measured (FM-REPLICATION-063). A payload and a
        // frame stream just crossed this link, so a node reporting zero bytes is
        // reporting a number nothing counts — the shape those counters exist to
        // rule out, and one no other layer looks at.
        let sent = stats_counter(&mut primary, "total_net_repl_output_bytes").await;
        assert!(
            sent > 0,
            "the primary shipped a full-resync payload and reports \
             total_net_repl_output_bytes=0"
        );
        let received = stats_counter(&mut replica, "total_net_repl_input_bytes").await;
        assert!(
            received > 0,
            "the replica installed a full-resync payload and reports \
             total_net_repl_input_bytes=0"
        );

        Ok(())
    });

    sim.run().unwrap();
    drop(primary_dir);
    drop(replica_dir);
}

/// Dial `host`, retrying while its listener is still coming up.
async fn connect(host: &str) -> RespConn {
    let addr = (turmoil::lookup(host), SERVER_PORT);
    for _ in 0..MAX_POLLS {
        if let Ok(conn) = RespConn::connect(addr).await {
            return conn;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!("{host} never accepted a client connection");
}

/// Block until the replica reports `master_link_status:up` — the point at which
/// it has loaded the payload and is streaming, and so the point at which the
/// payload's contents become a claim.
async fn await_link_up() {
    for _ in 0..MAX_POLLS {
        if link_status().await.as_deref() == Some("up") {
            return;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    panic!(
        "replica never reported master_link_status:up, last status {:?}",
        link_status().await
    );
}

/// One reading of the replica's `master_link_status`, on a fresh connection:
/// the replica is restarting its link while this polls, so a connection held
/// across attempts can die between them.
async fn link_status() -> Option<String> {
    let addr = (turmoil::lookup(REPLICA_HOST), SERVER_PORT);
    let mut conn = RespConn::connect(addr).await.ok()?;
    let RespValue::Bulk(Some(bytes)) = conn.cmd(&[b"INFO", b"replication"]).await.ok()? else {
        return None;
    };
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(field, _)| field.trim() == "master_link_status")
        .map(|(_, value)| value.trim().to_string())
}

/// One numeric field of a node's `INFO stats`, or `0` when the node did not
/// answer or does not report it.
async fn stats_counter(conn: &mut RespConn, field: &str) -> u64 {
    let Ok(RespValue::Bulk(Some(bytes))) = conn.cmd(&[b"INFO", b"stats"]).await else {
        return 0;
    };
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim() == field)
        .and_then(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0)
}
