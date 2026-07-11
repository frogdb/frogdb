//! WAL flush engine: batching, flush triggers, and durability outcome tracking.
//!
//! The flush thread drains [`WalCommand`]s into a [`FlushEngine`], which stages
//! entries into a [`WriteSink`] and commits them on explicit flush, size
//! threshold, or timeout. Every flush attempt — including background
//! (size/timeout/drain) flushes with no caller waiting on the result — records
//! its outcome into the shared [`FlushOutcomes`], so a failed flush can never
//! be silently swallowed: it is surfaced by the next durable-through
//! confirmation ([`FlushOutcomes::confirm_durable_through`]), by lag stats, and
//! by metrics.
use super::config::DurabilityMode;
use crate::rocks::RocksStore;
use bytes::Bytes;
use frogdb_types::metrics::definitions::{
    WalBytes, WalFlushDuration, WalFlushFailures, WalLostBytes, WalLostOps, WalWrites,
};
use rocksdb::{WriteBatch, WriteOptions};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, trace};

pub(super) enum WalEntry {
    Put {
        seq: u64,
        key: Bytes,
        value: Vec<u8>,
        size_estimate: usize,
    },
    Delete {
        seq: u64,
        key: Bytes,
        size_estimate: usize,
    },
    Merge {
        seq: u64,
        key: Bytes,
        operand: Vec<u8>,
        size_estimate: usize,
    },
    /// Full-shard clear: a full-range delete over the shard's primary CF. Keyless
    /// (it targets the whole CF). Handled specially by [`FlushEngine::apply`],
    /// which drains lower-seq entries first so the tombstone covers exactly the
    /// pre-clear keyspace.
    Clear { seq: u64, size_estimate: usize },
}

impl WalEntry {
    fn seq(&self) -> u64 {
        match self {
            WalEntry::Put { seq, .. }
            | WalEntry::Delete { seq, .. }
            | WalEntry::Merge { seq, .. }
            | WalEntry::Clear { seq, .. } => *seq,
        }
    }

    fn size_estimate(&self) -> usize {
        match self {
            WalEntry::Put { size_estimate, .. }
            | WalEntry::Delete { size_estimate, .. }
            | WalEntry::Merge { size_estimate, .. }
            | WalEntry::Clear { size_estimate, .. } => *size_estimate,
        }
    }
}

pub(super) enum WalCommand {
    Write(WalEntry),
    Flush {
        done_tx: flume::Sender<std::io::Result<()>>,
    },
}

pub(super) struct WalLagAtomics {
    pub(super) pending_ops: AtomicUsize,
    pub(super) pending_bytes: AtomicUsize,
    pub(super) last_flush_timestamp_ms: AtomicU64,
}

pub(super) fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================================
// WriteSink — the storage seam
// ============================================================================

/// Storage seam the flush engine writes through.
///
/// Entries are staged one at a time and committed as one atomic batch. The
/// staged set is consumed by [`WriteSink::commit`] **regardless of outcome**:
/// a failed commit drops the staged entries. The engine records that loss in
/// [`FlushOutcomes`] — dropped entries are permanently lost (see the module
/// docs for why the redo-of-current-state WAL does not retry).
///
/// The production implementation is [`RocksSink`]; tests substitute an
/// in-memory sink with failure injection so the batching/threshold/timeout/
/// error-propagation logic is unit-testable without RocksDB.
pub(super) trait WriteSink: Send {
    fn stage_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()>;
    fn stage_delete(&mut self, key: &[u8]) -> std::io::Result<()>;
    /// Stage a merge operand for `key`, folded by the CF's merge operator.
    fn stage_merge(&mut self, key: &[u8], operand: &[u8]) -> std::io::Result<()>;
    /// Stage a full-shard clear: a full-range delete over the shard's primary
    /// column family. The engine flushes lower-seq entries before calling this
    /// (see [`FlushEngine::apply_clear`]), so the range bound is computed from
    /// committed state that already includes every pre-clear write.
    fn stage_clear(&mut self) -> std::io::Result<()>;
    /// Atomically commit all staged entries, syncing to disk if `sync`.
    fn commit(&mut self, sync: bool) -> std::io::Result<()>;
    /// Number of currently staged (uncommitted) entries.
    fn staged_len(&self) -> usize;
}

/// [`WriteSink`] backed by a RocksDB [`WriteBatch`] over the shard's data CF.
pub(super) struct RocksSink {
    rocks: Arc<RocksStore>,
    shard_id: usize,
    batch: WriteBatch,
}

impl RocksSink {
    pub(super) fn new(rocks: Arc<RocksStore>, shard_id: usize) -> Self {
        Self {
            rocks,
            shard_id,
            batch: WriteBatch::default(),
        }
    }
}

impl WriteSink for RocksSink {
    fn stage_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()> {
        self.rocks
            .batch_put(&mut self.batch, self.shard_id, key, value)
            .map_err(std::io::Error::other)
    }

    fn stage_delete(&mut self, key: &[u8]) -> std::io::Result<()> {
        self.rocks
            .batch_delete(&mut self.batch, self.shard_id, key)
            .map_err(std::io::Error::other)
    }

    fn stage_merge(&mut self, key: &[u8], operand: &[u8]) -> std::io::Result<()> {
        self.rocks
            .batch_merge(&mut self.batch, self.shard_id, key, operand)
            .map_err(std::io::Error::other)
    }

    fn stage_clear(&mut self) -> std::io::Result<()> {
        self.rocks
            .batch_clear_shard(&mut self.batch, self.shard_id)
            .map_err(std::io::Error::other)
    }

    fn commit(&mut self, sync: bool) -> std::io::Result<()> {
        let batch = std::mem::take(&mut self.batch);
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(sync);
        self.rocks
            .write_batch_opt(batch, &write_opts)
            .map_err(std::io::Error::other)
    }

    fn staged_len(&self) -> usize {
        self.batch.len()
    }
}

// ============================================================================
// FlushOutcomes — shared record of every flush attempt
// ============================================================================

/// Shared durability state between the flush thread (writer of outcomes) and
/// the [`super::RocksWalWriter`] handle (reader / confirmer).
///
/// Invariants (single flush thread writes; sequence numbers are assigned
/// monotonically and batches are committed in sequence order):
/// - `durable_seq` is the highest sequence whose batch committed. Every entry
///   with a lower sequence was either committed or counted in `lost_ops`.
/// - `highest_failed_seq` is the highest sequence contained in any failed
///   (dropped) batch. A confirmation for entries written after sequence `S`
///   fails iff `highest_failed_seq > S` — batches are seq-ordered, so a failed
///   batch with max sequence above `S` must have contained at least one entry
///   assigned after `S`.
pub(super) struct FlushOutcomes {
    durable_seq: AtomicU64,
    highest_failed_seq: AtomicU64,
    flush_failures: AtomicU64,
    lost_ops: AtomicU64,
    lost_bytes: AtomicU64,
    /// Outcome of the most recent flush attempt (true when none yet).
    last_flush_ok: AtomicBool,
    /// Most recent failure message, for confirmation error text.
    last_error: Mutex<Option<String>>,
}

impl FlushOutcomes {
    pub(super) fn new() -> Self {
        Self {
            durable_seq: AtomicU64::new(0),
            highest_failed_seq: AtomicU64::new(0),
            flush_failures: AtomicU64::new(0),
            lost_ops: AtomicU64::new(0),
            lost_bytes: AtomicU64::new(0),
            last_flush_ok: AtomicBool::new(true),
            last_error: Mutex::new(None),
        }
    }

    fn record_success(&self, max_seq: u64) {
        self.durable_seq.store(max_seq, Ordering::Release);
        self.last_flush_ok.store(true, Ordering::Release);
    }

    fn record_failure(&self, max_seq: u64, lost_ops: usize, lost_bytes: usize, error: &str) {
        self.highest_failed_seq.fetch_max(max_seq, Ordering::AcqRel);
        self.flush_failures.fetch_add(1, Ordering::Release);
        self.lost_ops.fetch_add(lost_ops as u64, Ordering::Release);
        self.lost_bytes
            .fetch_add(lost_bytes as u64, Ordering::Release);
        self.last_flush_ok.store(false, Ordering::Release);
        *self.last_error.lock().unwrap() = Some(error.to_string());
    }

    pub(super) fn durable_sequence(&self) -> u64 {
        self.durable_seq.load(Ordering::Acquire)
    }

    pub(super) fn flush_failures(&self) -> u64 {
        self.flush_failures.load(Ordering::Acquire)
    }

    pub(super) fn lost_ops(&self) -> u64 {
        self.lost_ops.load(Ordering::Acquire)
    }

    pub(super) fn lost_bytes(&self) -> u64 {
        self.lost_bytes.load(Ordering::Acquire)
    }

    pub(super) fn last_flush_ok(&self) -> bool {
        self.last_flush_ok.load(Ordering::Acquire)
    }

    /// Confirm that every WAL entry assigned after `after_seq` (through
    /// `target_seq`) is durable.
    ///
    /// `flush_result` is the outcome of the explicit flush that drained the
    /// buffer. On top of it, this checks for failures of *background*
    /// (size-threshold / timeout) flushes that carried entries in the
    /// confirmed range — the failures that used to be silently swallowed.
    /// Failures entirely before `after_seq` (other commands' entries, already
    /// reported to their confirmers or counted as lost) do not fail this
    /// confirmation.
    pub(super) fn confirm_durable_through(
        &self,
        after_seq: u64,
        target_seq: u64,
        flush_result: std::io::Result<()>,
    ) -> std::io::Result<()> {
        flush_result?;
        if self.highest_failed_seq.load(Ordering::Acquire) > after_seq {
            let detail = self
                .last_error
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| "unknown flush error".to_string());
            return Err(std::io::Error::other(format!(
                "WAL flush failed for entries after sequence {after_seq} \
                 (durable through {}): {detail}",
                self.durable_sequence()
            )));
        }
        // Defensive: with an Ok flush and no overlapping failure, everything
        // through `target_seq` must have committed.
        let durable = self.durable_sequence();
        if durable < target_seq {
            return Err(std::io::Error::other(format!(
                "WAL durable sequence {durable} below confirmation target {target_seq}"
            )));
        }
        Ok(())
    }
}

// ============================================================================
// FlushEngine — batching + outcome recording
// ============================================================================

/// Owns the flush state machine: staging entries into the sink, tracking batch
/// size/ops/sequence bookkeeping, and recording every flush outcome (explicit,
/// size-threshold, timeout, and shutdown-drain alike) into [`FlushOutcomes`].
pub(super) struct FlushEngine<S: WriteSink> {
    sink: S,
    shard_id: usize,
    shard_label: String,
    is_sync: bool,
    batch_size: usize,
    batch_ops: usize,
    batch_max_seq: u64,
    last_flush: Instant,
    last_error_log: Option<Instant>,
    lag: Arc<WalLagAtomics>,
    outcomes: Arc<FlushOutcomes>,
    metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
}

/// Minimum interval between full-severity flush-failure log lines. Failures
/// can recur every batch timeout (10ms by default); without rate limiting a
/// dying disk would emit hundreds of error lines per second.
const FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(10);

impl<S: WriteSink> FlushEngine<S> {
    pub(super) fn new(
        sink: S,
        shard_id: usize,
        mode: &DurabilityMode,
        lag: Arc<WalLagAtomics>,
        outcomes: Arc<FlushOutcomes>,
        metrics: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Self {
        Self {
            sink,
            shard_id,
            shard_label: shard_id.to_string(),
            is_sync: matches!(mode, DurabilityMode::Sync),
            batch_size: 0,
            batch_ops: 0,
            batch_max_seq: 0,
            last_flush: Instant::now(),
            last_error_log: None,
            lag,
            outcomes,
            metrics,
        }
    }

    fn staged_size(&self) -> usize {
        self.batch_size
    }

    fn since_last_flush(&self) -> Duration {
        self.last_flush.elapsed()
    }

    /// Stage one entry. A staging failure (CF handle missing — effectively
    /// unreachable) is recorded as a lost entry rather than silently skipped.
    fn apply(&mut self, entry: WalEntry) {
        // A clear is a flush barrier, not a batchable entry: it drains the
        // current batch, then range-deletes the shard in its own committed
        // batch. Handled separately so the range bound sees every lower-seq
        // write on disk.
        if let WalEntry::Clear { seq, size_estimate } = entry {
            self.apply_clear(seq, size_estimate);
            return;
        }
        let seq = entry.seq();
        let size_estimate = entry.size_estimate();
        let staged = match &entry {
            WalEntry::Put { key, value, .. } => self.sink.stage_put(key, value),
            WalEntry::Delete { key, .. } => self.sink.stage_delete(key),
            WalEntry::Merge { key, operand, .. } => self.sink.stage_merge(key, operand),
            WalEntry::Clear { .. } => unreachable!("Clear handled above"),
        };
        if let Err(e) = staged {
            self.outcomes
                .record_failure(seq, 1, size_estimate, &e.to_string());
            self.log_failure(
                &e,
                1,
                size_estimate,
                "WAL entry staging failed; entry dropped",
            );
            return;
        }
        self.batch_size += size_estimate;
        self.batch_ops += 1;
        self.batch_max_seq = self.batch_max_seq.max(seq);
        self.lag.pending_ops.fetch_add(1, Ordering::Release);
        self.lag
            .pending_bytes
            .fetch_add(size_estimate, Ordering::Release);
        // Merges are effective writes (they add data), so they count toward
        // write throughput like puts; deletes do not.
        if matches!(entry, WalEntry::Put { .. } | WalEntry::Merge { .. }) {
            WalWrites::inc(&*self.metrics, &self.shard_label);
            WalBytes::inc_by(&*self.metrics, size_estimate as u64, &self.shard_label);
        }
    }

    /// Apply a full-shard clear as a flush barrier.
    ///
    /// The clear must range-delete exactly the keyspace that precedes it in
    /// sequence order. Same-batch lower-seq puts are still uncommitted, so a
    /// range bound computed from committed state alone would miss them. We
    /// therefore (1) flush the current batch, making every lower-seq entry
    /// durable, then (2) stage the range delete (its bound now sees them) and
    /// (3) commit it as its own batch. Higher-seq entries that follow are applied
    /// after this returns, into a fresh batch committed later — so post-flush
    /// writes correctly survive the tombstone. The outcome is recorded under the
    /// clear's `seq` even when the CF is empty (nothing staged), so the durable
    /// sequence still advances past it.
    fn apply_clear(&mut self, seq: u64, size_estimate: usize) {
        // (1) Barrier: drain all lower-seq entries to disk.
        let _ = self.flush();

        // (2) Stage the full-range delete over the (now fully-committed) CF.
        if let Err(e) = self.sink.stage_clear() {
            self.outcomes
                .record_failure(seq, 1, size_estimate, &e.to_string());
            self.log_failure(
                &e,
                1,
                size_estimate,
                "WAL clear staging failed; entry dropped",
            );
            return;
        }

        // A clear is an effective write (it mutates the CF), so it counts toward
        // write throughput like a put.
        WalWrites::inc(&*self.metrics, &self.shard_label);
        WalBytes::inc_by(&*self.metrics, size_estimate as u64, &self.shard_label);

        // (3) Commit the clear as its own batch, recording the outcome under
        // `seq`. `commit` of an empty batch (empty CF) is a durable no-op, which
        // still lets the durable sequence advance past the clear.
        let start = Instant::now();
        self.last_flush = Instant::now();
        match self.sink.commit(self.is_sync) {
            Ok(()) => {
                self.outcomes.record_success(seq);
                self.lag
                    .last_flush_timestamp_ms
                    .store(current_timestamp_ms(), Ordering::Release);
                WalFlushDuration::observe(
                    &*self.metrics,
                    start.elapsed().as_secs_f64(),
                    &self.shard_label,
                );
                trace!(shard_id = self.shard_id, seq, "WAL shard clear committed");
            }
            Err(e) => {
                self.outcomes
                    .record_failure(seq, 1, size_estimate, &e.to_string());
                WalFlushFailures::inc(&*self.metrics, &self.shard_label);
                WalLostOps::inc_by(&*self.metrics, 1, &self.shard_label);
                WalLostBytes::inc_by(&*self.metrics, size_estimate as u64, &self.shard_label);
                self.log_failure(
                    &e,
                    1,
                    size_estimate,
                    "WAL clear flush failed; batch dropped",
                );
            }
        }
    }

    /// Commit the staged batch and record the outcome — success advances the
    /// durable sequence; failure records the loss (counters, highest failed
    /// sequence, rate-limited log). Returns the commit result so an explicit
    /// flush can report it to its waiter.
    fn flush(&mut self) -> std::io::Result<()> {
        if self.sink.staged_len() == 0 {
            return Ok(());
        }
        let start = Instant::now();
        let flushed_bytes = self.batch_size;
        let flushed_ops = self.batch_ops;
        let max_seq = self.batch_max_seq;
        let batch_len = self.sink.staged_len();
        self.batch_size = 0;
        self.batch_ops = 0;
        self.batch_max_seq = 0;
        // The entries leave the buffer whether or not the commit succeeds
        // (a failed batch is dropped), so pending gauges drop either way.
        self.lag
            .pending_ops
            .fetch_sub(flushed_ops, Ordering::Release);
        self.lag
            .pending_bytes
            .fetch_sub(flushed_bytes, Ordering::Release);
        let result = self.sink.commit(self.is_sync);
        self.last_flush = Instant::now();
        match result {
            Ok(()) => {
                self.outcomes.record_success(max_seq);
                self.lag
                    .last_flush_timestamp_ms
                    .store(current_timestamp_ms(), Ordering::Release);
                let duration = start.elapsed();
                WalFlushDuration::observe(
                    &*self.metrics,
                    duration.as_secs_f64(),
                    &self.shard_label,
                );
                trace!(
                    shard_id = self.shard_id,
                    entries = batch_len,
                    bytes = flushed_bytes,
                    duration_ms = duration.as_millis() as u64,
                    "WAL batch flushed"
                );
                Ok(())
            }
            Err(e) => {
                self.outcomes
                    .record_failure(max_seq, flushed_ops, flushed_bytes, &e.to_string());
                WalFlushFailures::inc(&*self.metrics, &self.shard_label);
                WalLostOps::inc_by(&*self.metrics, flushed_ops as u64, &self.shard_label);
                WalLostBytes::inc_by(&*self.metrics, flushed_bytes as u64, &self.shard_label);
                self.log_failure(
                    &e,
                    flushed_ops,
                    flushed_bytes,
                    "WAL flush failed; batch dropped",
                );
                Err(e)
            }
        }
    }

    /// Flush with no waiter. The outcome is still recorded in
    /// [`FlushOutcomes`] — this replaces the old `let _ = do_flush(...)` that
    /// discarded size-threshold/timeout/drain flush errors entirely.
    fn flush_detached(&mut self) {
        let _ = self.flush();
    }

    fn log_failure(&mut self, e: &std::io::Error, ops: usize, bytes: usize, msg: &'static str) {
        let now = Instant::now();
        if self
            .last_error_log
            .is_none_or(|t| now.duration_since(t) >= FAILURE_LOG_INTERVAL)
        {
            self.last_error_log = Some(now);
            error!(
                shard_id = self.shard_id,
                error = %e,
                lost_ops = ops,
                lost_bytes = bytes,
                total_flush_failures = self.outcomes.flush_failures(),
                total_lost_ops = self.outcomes.lost_ops(),
                msg
            );
        } else {
            debug!(shard_id = self.shard_id, error = %e, lost_ops = ops, msg);
        }
    }
}

// ============================================================================
// Flush thread loop
// ============================================================================

pub(super) fn flush_thread_loop<S: WriteSink>(
    rx: flume::Receiver<WalCommand>,
    mut engine: FlushEngine<S>,
    batch_size_threshold: usize,
    batch_timeout: Duration,
) {
    loop {
        match rx.recv_timeout(batch_timeout) {
            Ok(cmd) => match cmd {
                WalCommand::Write(entry) => {
                    engine.apply(entry);
                    // Opportunistically drain queued commands into this batch.
                    while engine.staged_size() < batch_size_threshold {
                        match rx.try_recv() {
                            Ok(WalCommand::Write(e)) => engine.apply(e),
                            Ok(WalCommand::Flush { done_tx }) => {
                                let _ = done_tx.send(engine.flush());
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    if engine.staged_size() >= batch_size_threshold
                        || engine.since_last_flush() >= batch_timeout
                    {
                        engine.flush_detached();
                    }
                }
                WalCommand::Flush { done_tx } => {
                    let _ = done_tx.send(engine.flush());
                }
            },
            Err(flume::RecvTimeoutError::Timeout) => {
                engine.flush_detached();
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                while let Ok(cmd) = rx.try_recv() {
                    match cmd {
                        WalCommand::Write(e) => engine.apply(e),
                        WalCommand::Flush { done_tx } => {
                            let _ = done_tx.send(engine.flush());
                        }
                    }
                }
                engine.flush_detached();
                info!(shard_id = engine.shard_id, "WAL flush thread exiting");
                return;
            }
        }
    }
}

pub fn spawn_periodic_sync(
    rocks: Arc<RocksStore>,
    interval_ms: u64,
    monitor: Option<tokio_metrics::TaskMonitor>,
) -> tokio::task::JoinHandle<()> {
    info!(interval_ms, "Periodic WAL sync started");
    let interval = Duration::from_millis(interval_ms);
    let future = async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _span = tracing::info_span!("wal_sync").entered();
            if let Err(e) = rocks.flush() {
                tracing::warn!(error = %e, "Failed to sync WAL");
            }
        }
    };
    if let Some(monitor) = monitor {
        tokio::spawn(monitor.instrument(future))
    } else {
        tokio::spawn(future)
    }
}
