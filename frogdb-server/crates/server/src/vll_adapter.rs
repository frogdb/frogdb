//! [`ShardSink`] adapter that wraps the host's [`ShardSender`] vector.
//!
//! This is where chaos hooks (turmoil) attach: the adapter inlines the
//! pre-existing chaos checks so the [`VllCoordinator`] stays free of
//! testing-only concerns.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use frogdb_core::{
    MetricsRecorder, PartialResult, ScatterOp, ShardMessage, ShardReadyResult, ShardSender,
};
use frogdb_vll::{LockMode, MetricsSink, ShardSink, ShardSinkError};
use tokio::sync::oneshot;

#[cfg(feature = "turmoil")]
use crate::config::{ChaosConfig, ChaosConfigExt};

/// Per-call adapter implementing [`ShardSink`] for VLL coordination.
///
/// Constructed fresh for each scatter or continuation operation so that
/// internal state (e.g. the `sent_any_lock_request` flag used for chaos
/// inter-send delay) does not leak between calls.
pub(crate) struct ShardSenderSink {
    senders: Arc<Vec<ShardSender>>,
    /// Tracks whether at least one lock-request has been dispatched. Used by
    /// turmoil chaos hooks to delay dispatches *between* shard sends without
    /// delaying the very first one.
    sent_any_lock_request: AtomicBool,
    /// Optional chaos config — `None` for callers that don't need chaos
    /// hooks (e.g. EVAL continuation acquisition).
    #[cfg(feature = "turmoil")]
    chaos: Option<Arc<ChaosConfig>>,
}

impl ShardSenderSink {
    /// Create a chaos-free sink. Used for paths (like EVAL) that don't
    /// participate in the scatter chaos injection.
    pub(crate) fn new(senders: Arc<Vec<ShardSender>>) -> Self {
        Self {
            senders,
            sent_any_lock_request: AtomicBool::new(false),
            #[cfg(feature = "turmoil")]
            chaos: None,
        }
    }

    /// Create a chaos-aware sink. Used by the scatter executor under
    /// turmoil to inject failures and delays before dispatch.
    #[cfg(feature = "turmoil")]
    pub(crate) fn with_chaos(senders: Arc<Vec<ShardSender>>, chaos: Arc<ChaosConfig>) -> Self {
        Self {
            senders,
            sent_any_lock_request: AtomicBool::new(false),
            chaos: Some(chaos),
        }
    }
}

impl ShardSink for ShardSenderSink {
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
        #[cfg(feature = "turmoil")]
        if let Some(chaos) = &self.chaos {
            if chaos.is_shard_unavailable(shard_id) {
                return Err(ShardSinkError {
                    shard_id,
                    reason: "shard unavailable",
                });
            }
            if let Some(err_msg) = chaos.get_shard_error(shard_id) {
                tracing::warn!(shard_id, error = err_msg, "chaos shard error injected");
                return Err(ShardSinkError {
                    shard_id,
                    reason: "shard error",
                });
            }
            if let Some(&delay_ms) = chaos.shard_delays_ms.get(&shard_id) {
                chaos.apply_delay(delay_ms).await;
            }
            if self.sent_any_lock_request.load(Ordering::Relaxed) {
                chaos.apply_delay(chaos.scatter_inter_send_delay_ms).await;
            }
        }

        let msg = ShardMessage::VllLockRequest {
            txid,
            keys,
            mode,
            operation,
            ready_tx,
        };

        match self.senders[shard_id].send(msg).await {
            Ok(()) => {
                self.sent_any_lock_request.store(true, Ordering::Relaxed);
                Ok(())
            }
            Err(_) => Err(ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            }),
        }
    }

    async fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> Result<(), ShardSinkError> {
        let msg = ShardMessage::VllExecute { txid, response_tx };
        self.senders[shard_id]
            .send(msg)
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }

    async fn send_abort(&self, shard_id: usize, txid: u64) {
        let _ = self.senders[shard_id]
            .send(ShardMessage::VllAbort { txid })
            .await;
    }

    async fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> Result<(), ShardSinkError> {
        let msg = ShardMessage::VllContinuationLock {
            txid,
            conn_id,
            ready_tx,
            release_rx,
        };
        self.senders[shard_id]
            .send(msg)
            .await
            .map_err(|_| ShardSinkError {
                shard_id,
                reason: "shard channel closed",
            })
    }
}

/// Newtype adapter that lets `Arc<dyn MetricsRecorder>` satisfy
/// [`MetricsSink`].
pub(crate) struct MetricsRecorderSink(pub Arc<dyn MetricsRecorder>);

impl MetricsSink for MetricsRecorderSink {
    fn increment_counter(&self, name: &'static str, value: u64, labels: &[(&str, &str)]) {
        self.0.increment_counter(name, value, labels);
    }

    fn record_histogram(&self, name: &'static str, value: f64, labels: &[(&str, &str)]) {
        self.0.record_histogram(name, value, labels);
    }
}
