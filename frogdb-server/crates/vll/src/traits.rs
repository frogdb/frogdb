//! Trait abstractions used by [`crate::coordinator`].
//!
//! These traits invert the dependency direction so the VLL crate can drive
//! the cross-shard protocol without knowing about the host's concrete
//! `ShardMessage` enum or metrics backend.
//!
//! - [`ShardSink`] — typed dispatch of VLL messages to a single shard.
//! - [`MetricsSink`] — narrow counters/histograms recorded by the coordinator.

use std::future::Future;

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::{LockMode, ShardReadyResult};

/// Error returned when a shard sink cannot deliver a message — typically
/// because the receiving shard channel has been closed.
#[derive(Debug, Clone)]
pub struct ShardSinkError {
    pub shard_id: usize,
    pub reason: &'static str,
}

impl std::fmt::Display for ShardSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard {} unavailable: {}", self.shard_id, self.reason)
    }
}

impl std::error::Error for ShardSinkError {}

/// Per-shard sink used by the VLL coordinator.
///
/// Each method dispatches one VLL message to a specific shard. The host's
/// adapter maps these calls onto its concrete `ShardMessage` enum and
/// delivery channels. Methods are async because real implementations send
/// over an `mpsc::Sender`.
pub trait ShardSink: Send + Sync {
    /// Operation payload attached to scatter lock requests.
    type Operation: Send + 'static;

    /// Response payload returned by [`Self::send_execute`].
    type Response: Send + 'static;

    /// Send a VLL lock request.
    fn send_lock_request(
        &self,
        shard_id: usize,
        txid: u64,
        keys: Vec<Bytes>,
        mode: LockMode,
        operation: Self::Operation,
        ready_tx: oneshot::Sender<ShardReadyResult>,
    ) -> impl Future<Output = Result<(), ShardSinkError>> + Send;

    /// Send a VLL execute request, with a oneshot channel for the response.
    fn send_execute(
        &self,
        shard_id: usize,
        txid: u64,
        response_tx: oneshot::Sender<Self::Response>,
    ) -> impl Future<Output = Result<(), ShardSinkError>> + Send;

    /// Send a VLL abort to clean up after a partial failure.
    fn send_abort(&self, shard_id: usize, txid: u64) -> impl Future<Output = ()> + Send;

    /// Send a VLL continuation-lock acquisition request.
    fn send_continuation_lock(
        &self,
        shard_id: usize,
        txid: u64,
        conn_id: u64,
        ready_tx: oneshot::Sender<ShardReadyResult>,
        release_rx: oneshot::Receiver<()>,
    ) -> impl Future<Output = Result<(), ShardSinkError>> + Send;
}

/// Narrow metrics surface used by the VLL coordinator. Hosts adapt this to
/// their own metrics backend (e.g. `frogdb-core`'s `MetricsRecorder`).
pub trait MetricsSink: Send + Sync {
    fn increment_counter(&self, name: &'static str, value: u64, labels: &[(&str, &str)]);
    fn record_histogram(&self, name: &'static str, value: f64, labels: &[(&str, &str)]);
}

/// No-op metrics sink for tests and configurations that disable metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetricsSink;

impl MetricsSink for NoopMetricsSink {
    fn increment_counter(&self, _: &'static str, _: u64, _: &[(&str, &str)]) {}
    fn record_histogram(&self, _: &'static str, _: f64, _: &[(&str, &str)]) {}
}
