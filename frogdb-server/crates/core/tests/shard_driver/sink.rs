//! Harness-local [`ShardSink`] over a `Vec<ShardSender>` (template
//! `server/src/vll_adapter.rs:61-161`), plus a failure-injecting wrapper. The
//! server's `ShardSenderSink` and vll's `TestSink` are both unreachable from
//! the integration-test crate, so the harness supplies its own.

#![allow(dead_code)] // FaultSink is wired in by later phase-4b scenario tasks

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use frogdb_vll::{LockMode, ShardReadyResult, ShardSink, ShardSinkError};
use tokio::sync::oneshot;

use frogdb_core::shard::types::PartialResult;
use frogdb_core::shard::{ScatterOp, ShardSender, VllMsg};

/// Plain sink: maps `ShardSink` calls onto `VllMsg::Vll*` sends into the
/// per-shard queues. The driver services those queues via `pump_one`.
pub struct ChannelSink {
    senders: Arc<Vec<ShardSender>>,
}

impl ChannelSink {
    pub fn new(senders: Arc<Vec<ShardSender>>) -> Self {
        Self { senders }
    }
}

impl ShardSink for ChannelSink {
    type Operation = ScatterOp;
    type Response = PartialResult;

    async fn send_lock_request(
        &self,
        shard_id: usize,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: Self::Operation,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(VllMsg::VllLockRequest {
                txid,
                keys,
                mode,
                operation,
                ready_tx,
            })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }

    async fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(VllMsg::VllExecute { txid, response_tx })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }

    async fn send_abort(&self, shard_id: usize, txid: u64) {
        let _ = self.senders[shard_id].send(VllMsg::VllAbort { txid }).await;
    }

    async fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> Result<(), ShardSinkError> {
        self.senders[shard_id]
            .send(VllMsg::VllContinuationLock {
                txid,
                conn_id,
                ready_tx,
                release_rx,
            })
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }
}

/// Failure-injecting wrapper: fail the lock-request or execute dispatch for a
/// chosen set of shards (phase-2 / phase-3 failure). Service-withholding —
/// forcing a `LockTimeout` — is driver-side (the driver simply does not
/// `pump_one` that shard), so it is not modeled here.
pub struct FaultSink {
    inner: ChannelSink,
    /// Shards whose `send_lock_request` returns an error (phase-2 dispatch
    /// failure → `ScatterError::ShardUnavailable`).
    fail_lock: HashSet<usize>,
    /// Shards whose `send_execute` returns an error (phase-3 dispatch failure).
    fail_execute: HashSet<usize>,
}

impl FaultSink {
    pub fn new(
        senders: Arc<Vec<ShardSender>>,
        fail_lock: HashSet<usize>,
        fail_execute: HashSet<usize>,
    ) -> Self {
        Self {
            inner: ChannelSink::new(senders),
            fail_lock,
            fail_execute,
        }
    }
}

impl ShardSink for FaultSink {
    type Operation = ScatterOp;
    type Response = PartialResult;

    async fn send_lock_request(
        &self,
        shard_id: usize,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: Self::Operation,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> Result<(), ShardSinkError> {
        if self.fail_lock.contains(&shard_id) {
            return Err(ShardSinkError {
                shard_id,
                reason: "injected lock dispatch failure",
            });
        }
        self.inner
            .send_lock_request(shard_id, txid, keys, mode, operation, ready_tx)
            .await
    }

    async fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> Result<(), ShardSinkError> {
        if self.fail_execute.contains(&shard_id) {
            return Err(ShardSinkError {
                shard_id,
                reason: "injected execute dispatch failure",
            });
        }
        self.inner.send_execute(shard_id, txid, response_tx).await
    }

    async fn send_abort(&self, shard_id: usize, txid: u64) {
        self.inner.send_abort(shard_id, txid).await;
    }

    async fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> Result<(), ShardSinkError> {
        self.inner
            .send_continuation_lock(shard_id, txid, conn_id, ready_tx, release_rx)
            .await
    }
}

mod sink_tests {
    use std::time::Duration;

    use bytes::Bytes;
    use frogdb_protocol::Response;
    use frogdb_vll::{
        LockMode, NoopMetricsSink, ScatterParticipant, ScatterRequest, VllCoordinator,
    };

    use super::super::harness::ShardDriver;
    use super::*;
    use frogdb_core::shard::ScatterOp;

    #[tokio::test]
    async fn real_coordinator_scatter_mset_over_two_pumped_shards() {
        // Two shards, real VllCoordinator, ChannelSink. The coordinator runs as
        // a spawned task; the driver services each shard's queue deterministically.
        let mut driver = ShardDriver::new(2);
        let senders = driver.senders();

        let coordinator = Arc::new(VllCoordinator::new(
            ChannelSink::new(senders),
            NoopMetricsSink,
        ));

        // MSET {shard0-key: a} on shard 0, {shard1-key: b} on shard 1.
        let request = ScatterRequest {
            txid: 1,
            mode: LockMode::Write,
            participants: vec![
                ScatterParticipant {
                    shard_id: 0,
                    keys: vec![Bytes::from_static(b"k0")],
                    operation: ScatterOp::MSet {
                        pairs: vec![(Bytes::from_static(b"k0"), Bytes::from_static(b"a"))],
                    },
                },
                ScatterParticipant {
                    shard_id: 1,
                    keys: vec![Bytes::from_static(b"k1")],
                    operation: ScatterOp::MSet {
                        pairs: vec![(Bytes::from_static(b"k1"), Bytes::from_static(b"b"))],
                    },
                },
            ],
            timeout: Duration::from_secs(5),
            command: "MSET",
        };

        let coord = coordinator.clone();
        let handle = tokio::spawn(async move { coord.scatter(request).await });

        // Phase 1: each shard gets a VllLockRequest → pump both so they reply Ready.
        // Loop until the coordinator finishes, servicing whatever is queued.
        loop {
            let a = driver.pump_one(0).await;
            let b = driver.pump_one(1).await;
            if handle.is_finished() && !a && !b {
                break;
            }
            tokio::task::yield_now().await;
        }

        let outcome = handle.await.unwrap().expect("scatter must succeed");
        assert_eq!(outcome.responses.len(), 2);

        // Both writes landed on their real shards.
        assert_eq!(
            driver.execute(0, "GET", &["k0"]).await,
            Response::Bulk(Some(Bytes::from_static(b"a")))
        );
        assert_eq!(
            driver.execute(1, "GET", &["k1"]).await,
            Response::Bulk(Some(Bytes::from_static(b"b")))
        );

        // Lock tables clean afterward.
        assert!(driver.lock_table_info(0).await.intents.is_empty());
        assert!(driver.lock_table_info(1).await.intents.is_empty());
    }
}
