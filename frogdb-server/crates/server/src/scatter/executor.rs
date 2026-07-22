//! Scatter-gather executor for VLL coordination.
//!
//! This module is the thin caller-facing wrapper around
//! [`frogdb_vll::VllCoordinator`]. The executor is responsible for:
//!
//! 1. Letting the strategy partition the keys.
//! 2. Building [`ScatterRequest`]s in shard-sorted order.
//! 3. Driving the coordinator via a host-side [`ShardSenderSink`].
//! 4. Translating coordinator outcomes back into protocol [`Response`]s.
//!
//! All cross-shard locking choreography (5-phase lock/execute/abort/gather)
//! lives inside the coordinator now — adding a new scatter command is just
//! adding a strategy.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{MetricsRecorder, ShardSender};
use frogdb_protocol::Response;
use frogdb_vll::{
    DEFAULT_LOCK_ACQUISITION_TIMEOUT, ScatterError, ScatterParticipant, ScatterRequest,
    VllCoordinator,
};
use tracing::warn;

use super::ScatterGatherStrategy;
use crate::server::next_txid;
use crate::vll_adapter::{MetricsRecorderSink, ShardSenderSink};

/// Executor for scatter-gather operations using VLL coordination.
pub struct ScatterGatherExecutor {
    /// Shard message senders.
    shard_senders: Arc<Vec<ShardSender>>,
    /// Number of shards.
    num_shards: usize,
    /// Timeout for the result-gather phase.
    timeout: Duration,
    /// Metrics recorder.
    metrics_recorder: Arc<dyn MetricsRecorder>,
    /// Connection ID for logging.
    conn_id: u64,
    /// Chaos testing configuration (turmoil simulation only).
    #[cfg(feature = "turmoil")]
    chaos_config: Arc<crate::config::ChaosConfig>,
}

impl ScatterGatherExecutor {
    /// Create a new scatter-gather executor.
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        timeout: Duration,
        metrics_recorder: Arc<dyn MetricsRecorder>,
        conn_id: u64,
        #[cfg(feature = "turmoil")] chaos_config: Arc<crate::config::ChaosConfig>,
    ) -> Self {
        let num_shards = shard_senders.len();
        Self {
            shard_senders,
            num_shards,
            timeout,
            metrics_recorder,
            conn_id,
            #[cfg(feature = "turmoil")]
            chaos_config,
        }
    }

    /// Execute a scatter-gather operation using a strategy.
    pub async fn execute(&self, strategy: &dyn ScatterGatherStrategy, args: &[Bytes]) -> Response {
        let start = std::time::Instant::now();
        let command_name = strategy.name();

        let partition = strategy.partition(args, self.num_shards);
        if partition.shard_keys.is_empty() {
            return strategy.merge(&[], &HashMap::new());
        }

        let txid = next_txid();
        frogdb_core::probes::fire_scatter_start(
            command_name,
            partition.shard_keys.len() as u64,
            txid,
        );

        let participants: Vec<ScatterParticipant<frogdb_core::ScatterOp>> = partition
            .shard_keys
            .iter()
            .map(|(&shard_id, keys)| ScatterParticipant {
                shard_id,
                keys: keys.clone(),
                operation: partition
                    .shard_operations
                    .get(&shard_id)
                    .cloned()
                    .unwrap_or_else(|| strategy.scatter_op()),
            })
            .collect();

        #[cfg(feature = "turmoil")]
        let sink = ShardSenderSink::with_chaos(
            Arc::clone(&self.shard_senders),
            Arc::clone(&self.chaos_config),
        );
        #[cfg(not(feature = "turmoil"))]
        let sink = ShardSenderSink::new(Arc::clone(&self.shard_senders));
        let metrics = MetricsRecorderSink(Arc::clone(&self.metrics_recorder));
        let coordinator = VllCoordinator::new(sink, metrics);

        let request = ScatterRequest {
            txid,
            mode: strategy.lock_mode(),
            participants,
            timeout: self.timeout.max(DEFAULT_LOCK_ACQUISITION_TIMEOUT),
            command: command_name,
        };

        let outcome = match coordinator.scatter(request).await {
            Ok(outcome) => outcome,
            Err(err) => return self.scatter_error_to_response(err, txid),
        };

        let mut shard_results: HashMap<usize, HashMap<Bytes, Response>> =
            HashMap::with_capacity(outcome.responses.len());
        for (shard_id, partial) in outcome.responses {
            shard_results.insert(shard_id, partial.into_keyed_results().into_iter().collect());
        }

        let num_shards = partition.shard_keys.len();
        frogdb_core::probes::fire_scatter_done(
            command_name,
            start.elapsed().as_micros() as u64,
            num_shards as u64,
        );

        strategy.merge(&partition.key_order, &shard_results)
    }

    fn scatter_error_to_response(&self, err: ScatterError, txid: u64) -> Response {
        match err {
            ScatterError::ShardUnavailable(e) => {
                warn!(
                    conn_id = self.conn_id,
                    txid,
                    shard_id = e.shard_id,
                    reason = e.reason,
                    "scatter shard unavailable"
                );
                Response::error("ERR shard unavailable")
            }
            ScatterError::LockFailed { shard_id, error } => {
                warn!(conn_id = self.conn_id, txid, shard_id, error = %error, "VLL lock acquisition failed");
                if matches!(error, frogdb_vll::VllError::ShardBusy) {
                    Response::error("BUSY shard busy with continuation lock; retry")
                } else {
                    Response::error("ERR VLL lock acquisition failed")
                }
            }
            ScatterError::LockChannelClosed { shard_id } => {
                warn!(
                    conn_id = self.conn_id,
                    txid, shard_id, "VLL ready channel dropped"
                );
                Response::error("ERR VLL lock acquisition failed")
            }
            ScatterError::LockTimeout { shard_id } => {
                warn!(
                    conn_id = self.conn_id,
                    txid, shard_id, "VLL lock acquisition timeout"
                );
                Response::error("ERR VLL lock acquisition failed")
            }
            ScatterError::ResultChannelClosed { shard_id } => {
                warn!(
                    conn_id = self.conn_id,
                    txid, shard_id, "shard dropped VLL result"
                );
                Response::error("ERR shard dropped VLL result")
            }
            ScatterError::ResultTimeout { shard_id } => {
                warn!(
                    conn_id = self.conn_id,
                    timeout_ms = self.timeout.as_millis() as u64,
                    txid,
                    shard_id,
                    "scatter-gather operation timed out"
                );
                Response::error("ERR VLL execution timeout")
            }
        }
    }
}
