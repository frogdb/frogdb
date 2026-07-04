//! Cross-shard VLL coordinator.
//!
//! [`VllCoordinator`] owns the cross-shard VLL protocol — both the SCA
//! scatter-gather flow used by multi-key commands (MGET, MSET, DEL, ...)
//! and the continuation-lock acquisition used by Lua / MULTI.
//!
//! Both flows share a common skeleton: send a VLL message to each
//! participating shard in sorted order (deadlock prevention), wait for all
//! shards to report ready, and then either proceed or abort. The
//! coordinator encapsulates that skeleton so callers express their command
//! at a higher level.
//!
//! See [`Self::scatter`] and [`Self::acquire_continuation`].
//!
//! # Drop semantics
//!
//! [`ContinuationGuard`] sends release signals on drop, so callers cannot
//! leak continuation locks even if they panic between acquisition and
//! cleanup.

use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::oneshot;

use crate::traits::{MetricsSink, ShardSink, ShardSinkError};
use crate::{LockMode, ShardReadyResult, VllError};

/// Default timeout used when the caller does not supply one explicitly.
pub const DEFAULT_LOCK_ACQUISITION_TIMEOUT: Duration = Duration::from_millis(4000);

/// One participant in a scatter operation: a shard plus its slice of the
/// keys (and optionally a per-shard payload override).
#[derive(Debug, Clone)]
pub struct ScatterParticipant<O> {
    pub shard_id: usize,
    pub keys: Vec<Bytes>,
    pub operation: O,
}

/// Inputs to [`VllCoordinator::scatter`].
#[derive(Debug)]
pub struct ScatterRequest<O> {
    /// Globally-unique transaction id.
    pub txid: u64,
    /// Lock mode requested on every participating shard.
    pub mode: LockMode,
    /// One entry per participating shard. The coordinator dispatches in
    /// the order given — callers should sort by shard id to match the
    /// rest of the system's deadlock-prevention convention.
    pub participants: Vec<ScatterParticipant<O>>,
    /// Per-phase timeout. Used independently for the lock-acquisition wait
    /// and the gather wait.
    pub timeout: Duration,
    /// Command name used for metrics labels (e.g. `"MGET"`).
    pub command: &'static str,
}

/// Outcome of a successful [`VllCoordinator::scatter`] — one entry per
/// participating shard, in dispatch order.
#[derive(Debug)]
pub struct ScatterOutcome<R> {
    pub responses: Vec<(usize, R)>,
}

/// Errors returned by [`VllCoordinator::scatter`].
#[derive(Debug)]
pub enum ScatterError {
    /// Failed to dispatch a message — channel closed.
    ShardUnavailable(ShardSinkError),
    /// At least one shard reported lock-acquisition failure.
    LockFailed { shard_id: usize, error: VllError },
    /// At least one shard's ready channel closed prematurely.
    LockChannelClosed { shard_id: usize },
    /// At least one shard's lock acquisition timed out.
    LockTimeout { shard_id: usize },
    /// At least one shard's result channel closed prematurely.
    ResultChannelClosed { shard_id: usize },
    /// At least one shard's result wait timed out.
    ResultTimeout { shard_id: usize },
}

impl std::fmt::Display for ScatterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScatterError::ShardUnavailable(e) => write!(f, "{e}"),
            ScatterError::LockFailed { shard_id, error } => {
                write!(f, "VLL lock failed on shard {shard_id}: {error}")
            }
            ScatterError::LockChannelClosed { shard_id } => {
                write!(f, "VLL ready channel dropped by shard {shard_id}")
            }
            ScatterError::LockTimeout { shard_id } => {
                write!(f, "VLL lock acquisition timeout on shard {shard_id}")
            }
            ScatterError::ResultChannelClosed { shard_id } => {
                write!(f, "shard {shard_id} dropped VLL result")
            }
            ScatterError::ResultTimeout { shard_id } => {
                write!(f, "VLL result timeout on shard {shard_id}")
            }
        }
    }
}

impl std::error::Error for ScatterError {}

/// RAII guard returned by [`VllCoordinator::acquire_continuation`].
///
/// While the guard is alive, every participating shard holds a continuation
/// lock that excludes other connections. Dropping the guard releases all
/// locks. The drop is non-blocking — release signals are delivered through
/// `oneshot::Sender::send`.
#[must_use = "dropping the guard releases the continuation lock; bind it to keep the lock held"]
#[derive(Debug)]
pub struct ContinuationGuard {
    release_txs: Vec<oneshot::Sender<()>>,
}

impl ContinuationGuard {
    /// Explicitly release all continuation locks. Equivalent to dropping.
    pub fn release(self) {
        // Drop runs and sends on each release_tx.
    }

    /// Number of shards still holding a lock through this guard.
    pub fn shard_count(&self) -> usize {
        self.release_txs.len()
    }
}

impl Drop for ContinuationGuard {
    fn drop(&mut self) {
        for tx in self.release_txs.drain(..) {
            let _ = tx.send(());
        }
    }
}

/// Errors returned by [`VllCoordinator::acquire_continuation`].
#[derive(Debug)]
pub enum ContinuationError {
    ShardUnavailable(ShardSinkError),
    LockFailed { shard_id: usize, error: VllError },
    LockChannelClosed { shard_id: usize },
    LockTimeout { shard_id: usize },
}

impl std::fmt::Display for ContinuationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContinuationError::ShardUnavailable(e) => write!(f, "{e}"),
            ContinuationError::LockFailed { shard_id, error } => {
                write!(f, "continuation lock failed on shard {shard_id}: {error}")
            }
            ContinuationError::LockChannelClosed { shard_id } => {
                write!(f, "continuation ready channel dropped by shard {shard_id}")
            }
            ContinuationError::LockTimeout { shard_id } => {
                write!(
                    f,
                    "continuation lock acquisition timeout on shard {shard_id}"
                )
            }
        }
    }
}

impl std::error::Error for ContinuationError {}

/// Cross-shard VLL coordinator.
pub struct VllCoordinator<S, M> {
    sink: S,
    metrics: M,
}

impl<S, M> VllCoordinator<S, M>
where
    S: ShardSink,
    M: MetricsSink,
{
    /// Construct a new coordinator from a sink + metrics adapter.
    pub fn new(sink: S, metrics: M) -> Self {
        Self { sink, metrics }
    }

    /// Borrow the underlying shard sink (useful for tests).
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Run a scatter operation through the 4-phase VLL choreography.
    ///
    /// Phases:
    /// 1. Send `VllLockRequest` to each participant in dispatch order.
    /// 2. Wait for every shard to signal `ShardReadyResult::Ready`.
    /// 3. Send `VllExecute` to each shard.
    /// 4. Gather `Response`s and return them in dispatch order.
    ///
    /// On any failure the coordinator aborts every shard still holding
    /// locks. `command` is used solely for metric labels.
    pub async fn scatter(
        &self,
        request: ScatterRequest<S::Operation>,
    ) -> Result<ScatterOutcome<S::Response>, ScatterError> {
        let start = Instant::now();
        let shard_count = request.participants.len();

        // Phase 1: Dispatch lock requests, tracking ready receivers.
        let mut ready_rxs: Vec<(usize, oneshot::Receiver<ShardReadyResult>)> =
            Vec::with_capacity(shard_count);

        for participant in request.participants {
            let (ready_tx, ready_rx) = oneshot::channel();

            if let Err(err) = self
                .sink
                .send_lock_request(
                    participant.shard_id,
                    request.txid,
                    participant.keys,
                    request.mode,
                    participant.operation,
                    ready_tx,
                )
                .await
            {
                self.abort_pending(&ready_rxs, request.txid).await;
                self.record_outcome(request.command, "error", start, shard_count);
                return Err(ScatterError::ShardUnavailable(err));
            }

            ready_rxs.push((participant.shard_id, ready_rx));
        }

        // Every participant received a lock request; from here on failures
        // abort by real shard id.
        let shard_ids: Vec<usize> = ready_rxs.iter().map(|(id, _)| *id).collect();

        // Phase 2: Wait for every shard to report ready.
        for (shard_id, ready_rx) in ready_rxs {
            match tokio::time::timeout(request.timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(error))) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::LockFailed { shard_id, error });
                }
                Ok(Err(_)) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::LockChannelClosed { shard_id });
                }
                Err(_) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "timeout", start, shard_count);
                    return Err(ScatterError::LockTimeout { shard_id });
                }
            }
        }

        // Phase 3: Dispatch VllExecute requests.
        //
        // On a partial failure, every participant that has not received
        // `VllExecute` still holds locks and must be aborted by its *real*
        // shard id — including the participant whose dispatch just failed.
        // Participants that already received `VllExecute` release their own
        // locks when execution completes, so they must not be aborted.
        let mut result_rxs: Vec<(usize, oneshot::Receiver<S::Response>)> =
            Vec::with_capacity(shard_count);

        for (idx, &shard_id) in shard_ids.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = self
                .sink
                .send_execute(shard_id, request.txid, response_tx)
                .await
            {
                self.abort_shards(&shard_ids[idx..], request.txid).await;
                self.record_outcome(request.command, "error", start, shard_count);
                return Err(ScatterError::ShardUnavailable(err));
            }
            result_rxs.push((shard_id, response_rx));
        }

        // Phase 4: Gather results.
        let mut responses: Vec<(usize, S::Response)> = Vec::with_capacity(shard_count);
        for (shard_id, rx) in result_rxs {
            match tokio::time::timeout(request.timeout, rx).await {
                Ok(Ok(response)) => responses.push((shard_id, response)),
                Ok(Err(_)) => {
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::ResultChannelClosed { shard_id });
                }
                Err(_) => {
                    self.record_outcome(request.command, "timeout", start, shard_count);
                    return Err(ScatterError::ResultTimeout { shard_id });
                }
            }
        }

        self.record_outcome(request.command, "success", start, shard_count);
        Ok(ScatterOutcome { responses })
    }

    /// Acquire a continuation lock on every participating shard.
    ///
    /// `shards` must be sorted in ascending order to prevent deadlocks.
    /// On success returns a [`ContinuationGuard`] which releases all locks
    /// when dropped. On any failure all already-acquired locks are
    /// released before the error is returned.
    pub async fn acquire_continuation(
        &self,
        txid: u64,
        conn_id: u64,
        shards: &[usize],
        timeout: Duration,
    ) -> Result<ContinuationGuard, ContinuationError> {
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut ready_rxs: Vec<(usize, oneshot::Receiver<ShardReadyResult>)> =
            Vec::with_capacity(shards.len());

        for &shard_id in shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();

            if let Err(err) = self
                .sink
                .send_continuation_lock(shard_id, txid, conn_id, ready_tx, release_rx)
                .await
            {
                // Dropping `release_txs` here would NOT release any locks
                // because we never got past send_continuation_lock for the
                // failing shard. The earlier shards that did receive the
                // request will have their release_rx receivers signaled
                // when their tx is dropped here.
                drop(release_txs);
                return Err(ContinuationError::ShardUnavailable(err));
            }

            release_txs.push(release_tx);
            ready_rxs.push((shard_id, ready_rx));
        }

        for (shard_id, ready_rx) in ready_rxs {
            match tokio::time::timeout(timeout, ready_rx).await {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(error))) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockFailed { shard_id, error });
                }
                Ok(Err(_)) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockChannelClosed { shard_id });
                }
                Err(_) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockTimeout { shard_id });
                }
            }
        }

        Ok(ContinuationGuard { release_txs })
    }

    async fn abort_pending(
        &self,
        ready_rxs: &[(usize, oneshot::Receiver<ShardReadyResult>)],
        txid: u64,
    ) {
        let shard_ids: Vec<usize> = ready_rxs.iter().map(|(id, _)| *id).collect();
        self.abort_shards(&shard_ids, txid).await;
    }

    /// Best-effort abort of the given shard ids. `send_abort` is fire-and-
    /// forget; an unreachable shard is already unable to hold its locks past
    /// its own lifetime.
    async fn abort_shards(&self, shard_ids: &[usize], txid: u64) {
        for &shard_id in shard_ids {
            self.sink.send_abort(shard_id, txid).await;
        }
    }

    fn record_outcome(
        &self,
        command: &'static str,
        status: &'static str,
        start: Instant,
        shards: usize,
    ) {
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics.increment_counter(
            "frogdb_scatter_gather_total",
            1,
            &[("command", command), ("status", status)],
        );
        if status == "success" {
            self.metrics.record_histogram(
                "frogdb_scatter_gather_duration_seconds",
                elapsed,
                &[("command", command)],
            );
            self.metrics.record_histogram(
                "frogdb_scatter_gather_shards",
                shards as f64,
                &[("command", command)],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::NoopMetricsSink;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    type ReadyCallback = Arc<Mutex<Box<dyn FnMut(usize, u64) -> ShardReadyResult + Send>>>;
    type ExecuteCallback =
        Arc<Mutex<Box<dyn FnMut(usize, u64) -> Result<u32, ShardSinkError> + Send>>>;

    /// Test sink that records every call and lets each test script the
    /// shard responses (ready / failed / dropped) and execute outcomes.
    struct TestSink {
        // Per-shard callbacks for lock-request: send Ready or Failed.
        on_lock: ReadyCallback,
        // Per-shard execute callback: produce the Response or a send error.
        on_execute: ExecuteCallback,
        // Shard ids that received a send_abort, in order.
        aborted_shards: Arc<Mutex<Vec<usize>>>,
        cont_outcomes: ReadyCallback,
    }

    impl TestSink {
        fn ok_sink() -> (Self, Arc<Mutex<Vec<usize>>>) {
            let aborts = Arc::new(Mutex::new(Vec::new()));
            (
                TestSink {
                    on_lock: Arc::new(Mutex::new(Box::new(|_, _| ShardReadyResult::Ready))),
                    on_execute: Arc::new(Mutex::new(Box::new(|s, _| Ok((s as u32) + 100)))),
                    aborted_shards: aborts.clone(),
                    cont_outcomes: Arc::new(Mutex::new(Box::new(|_, _| ShardReadyResult::Ready))),
                },
                aborts,
            )
        }
    }

    impl ShardSink for TestSink {
        type Operation = u64;
        type Response = u32;

        async fn send_lock_request(
            &self,
            shard_id: usize,
            txid: u64,
            _keys: Vec<Bytes>,
            _mode: LockMode,
            _operation: Self::Operation,
            ready_tx: oneshot::Sender<ShardReadyResult>,
        ) -> Result<(), ShardSinkError> {
            let mut cb = self.on_lock.lock().await;
            let result = cb(shard_id, txid);
            let _ = ready_tx.send(result);
            Ok(())
        }

        async fn send_execute(
            &self,
            shard_id: usize,
            txid: u64,
            response_tx: oneshot::Sender<Self::Response>,
        ) -> Result<(), ShardSinkError> {
            let mut cb = self.on_execute.lock().await;
            let result = cb(shard_id, txid)?;
            let _ = response_tx.send(result);
            Ok(())
        }

        async fn send_abort(&self, shard_id: usize, _txid: u64) {
            self.aborted_shards.lock().await.push(shard_id);
        }

        async fn send_continuation_lock(
            &self,
            shard_id: usize,
            txid: u64,
            _conn_id: u64,
            ready_tx: oneshot::Sender<ShardReadyResult>,
            _release_rx: oneshot::Receiver<()>,
        ) -> Result<(), ShardSinkError> {
            let mut cb = self.cont_outcomes.lock().await;
            let result = cb(shard_id, txid);
            let _ = ready_tx.send(result);
            Ok(())
        }
    }

    fn participant(shard_id: usize) -> ScatterParticipant<u64> {
        ScatterParticipant {
            shard_id,
            keys: vec![Bytes::from(format!("key{shard_id}"))],
            operation: shard_id as u64,
        }
    }

    #[tokio::test]
    async fn scatter_returns_responses_in_dispatch_order() {
        let (sink, aborts) = TestSink::ok_sink();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let outcome = coord
            .scatter(ScatterRequest {
                txid: 1,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect("scatter ok");
        assert_eq!(outcome.responses, vec![(0, 100), (1, 101), (2, 102)]);
        assert!(aborts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn scatter_aborts_when_shard_lock_fails() {
        let (sink, aborts) = TestSink::ok_sink();
        // Shard 1 fails lock acquisition.
        *sink.on_lock.lock().await = Box::new(|s, _| {
            if s == 1 {
                ShardReadyResult::Failed(VllError::QueueFull)
            } else {
                ShardReadyResult::Ready
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 7,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected lock failure");

        assert!(matches!(err, ScatterError::LockFailed { shard_id: 1, .. }));
        // 3 shards received lock requests; on failure all 3 are aborted.
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        assert_eq!(aborted, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn phase2_failure_aborts_real_shard_ids_for_sparse_participants() {
        let (sink, aborts) = TestSink::ok_sink();
        // Participants are a sparse shard subset — ids must not be
        // reconstructed from vector positions.
        *sink.on_lock.lock().await = Box::new(|s, _| {
            if s == 5 {
                ShardReadyResult::Failed(VllError::QueueFull)
            } else {
                ShardReadyResult::Ready
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 9,
                mode: LockMode::Write,
                participants: vec![participant(2), participant(5), participant(7)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected lock failure");

        assert!(matches!(err, ScatterError::LockFailed { shard_id: 5, .. }));
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        assert_eq!(aborted, vec![2, 5, 7]);
    }

    /// Regression test: a phase-3 dispatch failure must abort every shard
    /// that has not received (or could not receive) `VllExecute`, addressed
    /// by its *real* shard id — not an id reconstructed from its position in
    /// the participant vector.
    ///
    /// Participants are shards [2, 5, 7]. `send_execute` succeeds for shard
    /// 2 and fails for shard 5. Shard 2 has already received execute and
    /// releases its own locks; shards 5 and 7 still hold locks and must be
    /// aborted. The buggy position-based loop instead aborted "shard 2"
    /// (a foreign abort for a shard that is executing) and left the locks
    /// on shards 5 and 7 held forever — no GC reclaims them.
    #[tokio::test]
    async fn phase3_failure_aborts_remaining_holders_not_positions() {
        let (sink, aborts) = TestSink::ok_sink();
        *sink.on_execute.lock().await = Box::new(|s, _| {
            if s == 5 {
                Err(ShardSinkError {
                    shard_id: 5,
                    reason: "shard channel closed",
                })
            } else {
                Ok((s as u32) + 100)
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 11,
                mode: LockMode::Write,
                participants: vec![participant(2), participant(5), participant(7)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected dispatch failure");

        assert!(matches!(
            err,
            ScatterError::ShardUnavailable(ShardSinkError { shard_id: 5, .. })
        ));
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        // Shard 5 (the failed dispatch) and shard 7 (never dispatched) must
        // be aborted. Shard 2 already received VllExecute and must NOT be —
        // it releases its own locks when execution completes.
        assert_eq!(aborted, vec![5, 7]);
    }

    #[tokio::test]
    async fn acquire_continuation_returns_guard_that_releases_on_drop() {
        let (sink, _) = TestSink::ok_sink();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let guard = coord
            .acquire_continuation(42, 7, &[0, 1, 2], Duration::from_secs(1))
            .await
            .expect("continuation ok");
        assert_eq!(guard.shard_count(), 3);
        // Dropping guard sends release.
        drop(guard);
    }

    #[tokio::test]
    async fn acquire_continuation_releases_partially_acquired_on_failure() {
        let (sink, _) = TestSink::ok_sink();
        // Shard 2 reports busy.
        *sink.cont_outcomes.lock().await = Box::new(|s, _| {
            if s == 2 {
                ShardReadyResult::Failed(VllError::ShardBusy)
            } else {
                ShardReadyResult::Ready
            }
        });
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .acquire_continuation(42, 7, &[0, 1, 2], Duration::from_secs(1))
            .await
            .expect_err("expected busy");
        assert!(matches!(
            err,
            ContinuationError::LockFailed { shard_id: 2, .. }
        ));
    }
}
