//! Shard-driver harness core (Phase 4b), integration-test tree.
//!
//! Drives a real [`ShardWorker`] (and, in pumped mode, a real
//! `VllCoordinator`) directly with controlled `ShardMessage` ordering. Lives
//! under `crates/core/tests/shard_driver/` rather than as an in-crate
//! `#[cfg(test)]` module: the harness populates a real `CommandRegistry` via the
//! `frogdb-commands` dev-dependency, whose dep on `frogdb-core` forms a dev-dep
//! cycle that compiles `frogdb-core` twice — unit-test code touching both copies
//! trips E0308. Integration tests link the single normal build and reach the
//! seams through the feature-gated public `drive*` wrappers (`shard-driver`
//! feature, enabled by the self-dev-dep) (brief D1).
//!
//! Two driving styles (brief D3):
//! - **Direct**: [`ShardDriver::dispatch`] → `worker.drive` — used when the test
//!   is the only sender.
//! - **Pumped**: a real coordinator task talks through [`crate::sink::ChannelSink`];
//!   the driver chooses which shard's queue to service next via
//!   [`ShardDriver::pump_one`] (`worker.try_recv_queued()` + `worker.drive`).
//!   Under a current-thread runtime this makes cross-shard interleaving a
//!   deterministic function of the service schedule.

#![allow(dead_code)] // harness surface is used piecemeal across scenario modules

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use frogdb_core::shard::types::{
    ExpiryIndexCheckInfo, LockTableInfo, MemoryCheckInfo, TransactionResult, WaitQueueInfo,
};
use frogdb_core::shard::{
    Envelope, NewConnection, ShardMessage, ShardReceiver, ShardSender, ShardWorkerBuilder,
    WatchEntry,
};
use frogdb_core::types::BlockingOp;
use frogdb_core::{CommandRegistry, ShardWorker};

/// Owns N real shard workers plus the sender side of each shard's queue.
pub struct ShardDriver {
    workers: Vec<ShardWorker>,
    /// The length-N sender vector every worker shares (cross-shard routing).
    senders: Arc<Vec<ShardSender>>,
    /// Held open so shard queues never close under the workers.
    _conn_txs: Vec<mpsc::Sender<NewConnection>>,
}

impl ShardDriver {
    /// Build `n` shards with ids `0..n`, a shared registry, and a real
    /// length-`n` sender vector so cross-shard routing and keyspace-notify
    /// forwarding work.
    pub fn new(n: usize) -> Self {
        // Real data commands (Command-registry Global Constraint): the empty
        // registry has nothing to dispatch.
        let mut r = CommandRegistry::new();
        frogdb_commands::register_all(&mut r);
        let registry = Arc::new(r);

        // One (tx, rx) queue per shard; senders indexed by shard id.
        let mut msg_rxs: Vec<ShardReceiver> = Vec::with_capacity(n);
        let mut senders: Vec<ShardSender> = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = mpsc::channel::<Envelope>(1024);
            senders.push(ShardSender::new(tx));
            msg_rxs.push(ShardReceiver::new(rx));
        }
        let senders = Arc::new(senders);

        let mut workers = Vec::with_capacity(n);
        let mut conn_txs = Vec::with_capacity(n);
        for (shard_id, msg_rx) in msg_rxs.into_iter().enumerate() {
            let (conn_tx, conn_rx) = mpsc::channel::<NewConnection>(16);
            conn_txs.push(conn_tx);
            let worker = ShardWorkerBuilder::new(shard_id, n)
                .with_message_rx(msg_rx)
                .with_new_conn_rx(conn_rx)
                .with_shard_senders(senders.clone())
                .with_registry(registry.clone())
                .build();
            workers.push(worker);
        }

        Self {
            workers,
            senders,
            _conn_txs: conn_txs,
        }
    }

    /// Cloned handle to a shard's sender (for wiring a coordinator sink).
    pub fn senders(&self) -> Arc<Vec<ShardSender>> {
        self.senders.clone()
    }

    /// Mutable access to a worker's store for value-level asserts.
    pub fn worker(&mut self, shard: usize) -> &mut ShardWorker {
        &mut self.workers[shard]
    }

    // --- Direct dispatch (test is the sole sender) ----------------------

    /// Dispatch a raw message directly to a shard, bypassing its queue.
    pub async fn dispatch(&mut self, shard: usize, msg: ShardMessage) -> bool {
        self.workers[shard].drive(msg).await
    }

    /// Run one command and await its reply.
    pub async fn execute(&mut self, shard: usize, name: &str, args: &[&str]) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::Execute {
            command: Arc::new(cmd(name, args)),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("execute response")
    }

    /// Run a command attributed to a specific connection.
    pub async fn execute_conn(
        &mut self,
        shard: usize,
        conn_id: u64,
        name: &str,
        args: &[&str],
    ) -> Response {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::Execute {
            command: Arc::new(cmd(name, args)),
            conn_id,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("execute response")
    }

    /// Read a shard's WATCH version (pure probe: no keys, so no lazy purge).
    ///
    /// Discards the per-key liveness vector of the `GetVersion` reply — with no
    /// keys it is always empty. Use [`Self::watch_keys`] to also read liveness.
    pub async fn get_version(&mut self, shard: usize) -> u64 {
        let (tx, rx) = oneshot::channel();
        self.dispatch(
            shard,
            ShardMessage::GetVersion {
                keys: vec![],
                response_tx: tx,
            },
        )
        .await;
        rx.await.expect("version").0
    }

    /// Read a shard's WATCH version AND per-key liveness for the given keys.
    ///
    /// Mirrors [`Self::get_version`] but sends the watched keys, so the shard
    /// (a) reports each key's `live_at_watch` flag (present and unexpired at
    /// watch time, aligned with `keys`) and (b) runs the WATCH-time no-bump lazy
    /// purge for any already-expired key — the same seam a real WATCH drives.
    pub async fn watch_keys(&mut self, shard: usize, keys: &[&str]) -> (u64, Vec<bool>) {
        let (tx, rx) = oneshot::channel();
        self.dispatch(
            shard,
            ShardMessage::GetVersion {
                keys: keys
                    .iter()
                    .map(|k| Bytes::copy_from_slice(k.as_bytes()))
                    .collect(),
                response_tx: tx,
            },
        )
        .await;
        rx.await.expect("version + liveness")
    }

    /// Run a transaction with the given watches; returns the result.
    pub async fn exec_transaction(
        &mut self,
        shard: usize,
        conn_id: u64,
        commands: Vec<ParsedCommand>,
        watches: Vec<WatchEntry>,
    ) -> TransactionResult {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::ExecTransaction {
            commands,
            watches,
            conn_id,
            protocol_version: ProtocolVersion::Resp3,
            response_tx: tx,
        };
        self.dispatch(shard, msg).await;
        rx.await.expect("transaction result")
    }

    /// Register a blocking waiter; the returned receiver resolves when the
    /// waiter is satisfied, drained, or timed out by the shard.
    pub async fn block_wait(
        &mut self,
        shard: usize,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        deadline: Option<Instant>,
    ) -> oneshot::Receiver<Response> {
        let (tx, rx) = oneshot::channel();
        let msg = ShardMessage::BlockWait {
            conn_id,
            keys,
            op,
            response_tx: tx,
            deadline,
            protocol_version: ProtocolVersion::Resp3,
        };
        self.dispatch(shard, msg).await;
        rx
    }

    /// Fire-and-forget waiter cleanup (connection gave up).
    pub async fn unregister_wait(&mut self, shard: usize, conn_id: u64) {
        self.dispatch(shard, ShardMessage::UnregisterWait { conn_id })
            .await;
    }

    // --- Ticks (timer-only work; no ShardMessage exists) ----------------

    pub fn tick_expiry(&mut self, shard: usize) {
        self.workers[shard].drive_expiry_tick();
    }

    pub fn tick_waiter_timeout(&mut self, shard: usize) {
        self.workers[shard].drive_waiter_timeout_tick();
    }

    pub async fn pump_continuation_release(&mut self, shard: usize) {
        self.workers[shard].drive_continuation_release().await;
    }

    // --- Pumped mode ----------------------------------------------------

    /// Service one buffered message on a shard's queue (recv + dispatch).
    /// Returns `true` if a message was serviced. Non-blocking: does nothing
    /// if the queue is empty.
    pub async fn pump_one(&mut self, shard: usize) -> bool {
        if let Some(env) = self.workers[shard].try_recv_queued() {
            self.workers[shard].drive(env.message).await;
            true
        } else {
            false
        }
    }

    /// Drain every buffered message on a shard until its queue is empty.
    pub async fn drain(&mut self, shard: usize) {
        while self.pump_one(shard).await {}
    }

    // --- Probes (production DEBUG seam) ----------------------------------

    pub async fn wait_queue_info(&mut self, shard: usize) -> WaitQueueInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::GetWaitQueueInfo { response_tx: tx })
            .await;
        rx.await.expect("wait queue info")
    }

    pub async fn lock_table_info(&mut self, shard: usize) -> LockTableInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::GetLockTableInfo { response_tx: tx })
            .await;
        rx.await.expect("lock table info")
    }

    pub async fn memory_check(&mut self, shard: usize) -> MemoryCheckInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::MemoryCheck { response_tx: tx })
            .await;
        rx.await.expect("memory check")
    }

    pub async fn expiry_index_check(&mut self, shard: usize) -> ExpiryIndexCheckInfo {
        let (tx, rx) = oneshot::channel();
        self.dispatch(shard, ShardMessage::ExpiryIndexCheck { response_tx: tx })
            .await;
        rx.await.expect("expiry index check")
    }
}

/// Build a `ParsedCommand` from `&str`s.
pub fn cmd(name: &str, args: &[&str]) -> ParsedCommand {
    ParsedCommand::new(
        Bytes::from(name.to_string()),
        args.iter().map(|a| Bytes::from(a.to_string())).collect(),
    )
}

mod driver_tests {
    use super::*;

    #[tokio::test]
    async fn set_get_round_trip_and_empty_wait_queue() {
        let mut d = ShardDriver::new(1);
        assert!(matches!(
            d.execute(0, "SET", &["k", "v"]).await,
            Response::Simple(_)
        ));
        assert_eq!(
            d.execute(0, "GET", &["k"]).await,
            Response::Bulk(Some(Bytes::from_static(b"v")))
        );
        let wq = d.wait_queue_info(0).await;
        assert_eq!(wq.total_waiters, 0, "no waiters after a plain SET/GET");
        assert_eq!(wq.shard_id, 0);
    }

    #[tokio::test]
    async fn probes_report_clean_idle_shard() {
        let mut d = ShardDriver::new(2);
        let lt = d.lock_table_info(0).await;
        assert!(lt.intents.is_empty());
        assert!(lt.continuation_lock.is_none());
        let mem = d.memory_check(1).await;
        assert_eq!(mem.tracked_bytes, mem.recomputed_bytes);
        assert!(d.expiry_index_check(0).await.anomalies.is_empty());
    }
}
