//! Scatter-gather executor for VLL coordination.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ExecuteSignal, MetricsRecorder, PartialResult, ShardMessage, ShardReadyResult};

use crate::server::next_txid;
use frogdb_protocol::Response;
use tokio::sync::{mpsc, oneshot};
use tracing::{trace, warn};

use super::ScatterGatherStrategy;

/// Executor for scatter-gather operations using VLL coordination.
pub struct ScatterGatherExecutor {
    /// Shard message senders.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    /// Number of shards.
    num_shards: usize,
    /// Timeout for scatter-gather operations.
    timeout: Duration,
    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,
    /// Connection ID for logging.
    conn_id: u64,
}

impl ScatterGatherExecutor {
    /// Create a new scatter-gather executor.
    pub fn new(
        shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
        timeout: Duration,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        conn_id: u64,
    ) -> Self {
        let num_shards = shard_senders.len();
        Self {
            shard_senders,
            num_shards,
            timeout,
            metrics_recorder,
            conn_id,
        }
    }

    /// Execute a scatter-gather operation using a strategy.
    ///
    /// VLL (Very Lightweight Locking) provides atomic multi-shard operations:
    /// 1. Acquire global txid
    /// 2. Send VllLockRequest to each shard (sorted order prevents deadlocks)
    /// 3. Wait for all shards to report ready
    /// 4. Send execute signal to all shards
    /// 5. Gather and merge results
    pub async fn execute(&self, strategy: &dyn ScatterGatherStrategy, args: &[Bytes]) -> Response {
        let start = std::time::Instant::now();
        let command_name = strategy.name();

        // Partition keys across shards
        let partition = strategy.partition(args, self.num_shards);

        // Handle empty partition (no keys to process)
        if partition.shard_keys.is_empty() {
            return strategy.merge(&[], &HashMap::new());
        }

        let mode = strategy.lock_mode();
        let txid = next_txid();

        // Phase 1: Send VllLockRequest to each shard (in sorted order for deadlock prevention)
        let mut ready_receivers: Vec<(usize, oneshot::Receiver<ShardReadyResult>)> = Vec::new();
        let mut execute_senders: Vec<(usize, oneshot::Sender<ExecuteSignal>)> = Vec::new();

        for (&shard_id, shard_key_list) in &partition.shard_keys {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (execute_tx, execute_rx) = oneshot::channel();

            let shard_op = partition
                .shard_operations
                .get(&shard_id)
                .cloned()
                .unwrap_or_else(|| strategy.scatter_op());

            let msg = ShardMessage::VllLockRequest {
                txid,
                keys: shard_key_list.clone(),
                mode,
                operation: shard_op,
                ready_tx,
                execute_rx,
            };

            if self.shard_senders[shard_id].send(msg).await.is_err() {
                // Abort any shards that already received requests
                for &(abort_shard_id, _) in &ready_receivers {
                    let _ = self.shard_senders[abort_shard_id]
                        .send(ShardMessage::VllAbort { txid })
                        .await;
                }
                return Response::error("ERR shard unavailable");
            }
            ready_receivers.push((shard_id, ready_rx));
            execute_senders.push((shard_id, execute_tx));
        }

        // Phase 2: Wait for all shards to be ready
        let lock_timeout = Duration::from_millis(4000);
        let mut all_ready = true;

        for (shard_id, ready_rx) in &mut ready_receivers {
            match tokio::time::timeout(lock_timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {
                    trace!(shard_id, txid, "Shard ready for VLL operation");
                }
                Ok(Ok(ShardReadyResult::Failed(e))) => {
                    warn!(shard_id, txid, error = %e, "VLL lock acquisition failed");
                    all_ready = false;
                    break;
                }
                Ok(Err(_)) => {
                    warn!(shard_id, txid, "VLL ready channel dropped");
                    all_ready = false;
                    break;
                }
                Err(_) => {
                    warn!(shard_id, txid, "VLL lock acquisition timeout");
                    all_ready = false;
                    break;
                }
            }
        }

        // Phase 3: Send execute signal (proceed or abort)
        if !all_ready {
            // Abort all shards
            for (shard_id, execute_tx) in execute_senders {
                let _ = execute_tx.send(ExecuteSignal { proceed: false });
                let _ = self.shard_senders[shard_id]
                    .send(ShardMessage::VllAbort { txid })
                    .await;
            }
            return Response::error("ERR VLL lock acquisition failed");
        }

        // All ready - send execute signal and prepare to receive results
        let mut result_receivers: Vec<(usize, oneshot::Receiver<PartialResult>)> = Vec::new();

        for (shard_id, execute_tx) in execute_senders {
            // Send proceed signal
            if execute_tx.send(ExecuteSignal { proceed: true }).is_err() {
                // Abort remaining shards
                for (&abort_shard_id, _) in
                    partition.shard_keys.iter().skip(result_receivers.len() + 1)
                {
                    let _ = self.shard_senders[abort_shard_id]
                        .send(ShardMessage::VllAbort { txid })
                        .await;
                }
                return Response::error("ERR shard disconnected during VLL execute");
            }

            // Create result receiver
            let (response_tx, response_rx) = oneshot::channel();
            let msg = ShardMessage::VllExecute { txid, response_tx };
            if self.shard_senders[shard_id].send(msg).await.is_err() {
                return Response::error("ERR shard unavailable during VLL execute");
            }
            result_receivers.push((shard_id, response_rx));
        }

        // Phase 4: Gather results
        let mut shard_results: HashMap<usize, HashMap<Bytes, Response>> = HashMap::new();

        for (shard_id, rx) in result_receivers {
            match tokio::time::timeout(self.timeout, rx).await {
                Ok(Ok(partial)) => {
                    let results: HashMap<Bytes, Response> = partial.results.into_iter().collect();
                    shard_results.insert(shard_id, results);
                }
                Ok(Err(_)) => {
                    warn!(shard_id, txid, "Shard dropped VLL result");
                    self.metrics_recorder.increment_counter(
                        "frogdb_scatter_gather_total",
                        1,
                        &[("command", command_name), ("status", "error")],
                    );
                    return Response::error("ERR shard dropped VLL result");
                }
                Err(_) => {
                    warn!(
                        conn_id = self.conn_id,
                        timeout_ms = self.timeout.as_millis() as u64,
                        shard_id,
                        txid,
                        "Scatter-gather operation timed out"
                    );
                    self.metrics_recorder.increment_counter(
                        "frogdb_scatter_gather_total",
                        1,
                        &[("command", command_name), ("status", "timeout")],
                    );
                    return Response::error("ERR VLL execution timeout");
                }
            }
        }

        // Record successful scatter-gather metrics
        let num_shards = partition.shard_keys.len();
        self.metrics_recorder.increment_counter(
            "frogdb_scatter_gather_total",
            1,
            &[("command", command_name), ("status", "success")],
        );
        self.metrics_recorder.record_histogram(
            "frogdb_scatter_gather_duration_seconds",
            start.elapsed().as_secs_f64(),
            &[("command", command_name)],
        );
        self.metrics_recorder.record_histogram(
            "frogdb_scatter_gather_shards",
            num_shards as f64,
            &[("command", command_name)],
        );

        // Phase 5: Merge results
        strategy.merge(&partition.key_order, &shard_results)
    }
}
