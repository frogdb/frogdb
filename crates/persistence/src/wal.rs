//! Write-Ahead Log (WAL) implementation using RocksDB.
//!
//! Supports three durability modes:
//! - Async: No fsync (fastest, least durable)
//! - Periodic: fsync at configurable intervals (default: 1s)
//! - Sync: fsync every write (slowest, most durable)
//!
//! WAL writes from the event loop are non-blocking sends into a bounded
//! `flume` channel. A dedicated background thread drains the channel,
//! batches entries, and performs RocksDB I/O.

use bytes::Bytes;
use rocksdb::{WriteBatch, WriteOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, trace};

use super::rocks::RocksStore;
use super::serialization::serialize;
use frogdb_types::traits::{WalOperation, WalWriter};
use frogdb_types::types::{KeyMetadata, Value};

/// Durability mode for WAL writes.
#[derive(Debug, Clone)]
pub enum DurabilityMode {
    /// No fsync - writes may be lost on crash.
    Async,

    /// Periodic fsync at the given interval.
    Periodic { interval_ms: u64 },

    /// Sync every write - safest but slowest.
    Sync,
}

impl Default for DurabilityMode {
    fn default() -> Self {
        DurabilityMode::Periodic { interval_ms: 1000 }
    }
}

/// Configuration for WAL behavior.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Durability mode (default: Periodic with 1s interval).
    pub mode: DurabilityMode,

    /// Batch size threshold in bytes (default: 4MB).
    pub batch_size_threshold: usize,

    /// Batch timeout in milliseconds (default: 10ms).
    pub batch_timeout_ms: u64,

    /// Channel capacity for WAL commands (default: 8192).
    pub channel_capacity: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::default(),
            batch_size_threshold: 4 * 1024 * 1024, // 4MB
            batch_timeout_ms: 10,
            channel_capacity: 8192,
        }
    }
}

/// Lag statistics for a WAL writer.
///
/// Tracks how far behind persistence is relative to in-memory state.
/// This is critical for understanding data durability guarantees,
/// especially in `Async` and `Periodic` durability modes.
#[derive(Debug, Clone)]
pub struct WalLagStats {
    /// Number of operations in batch not yet flushed.
    pub pending_ops: usize,
    /// Bytes in batch not yet flushed.
    pub pending_bytes: usize,
    /// Time since last batch flush in milliseconds.
    pub durability_lag_ms: u64,
    /// Time since last fsync in milliseconds (only relevant for Periodic mode).
    pub sync_lag_ms: Option<u64>,
    /// Current sequence number.
    pub sequence: u64,
    /// Shard ID this writer is associated with.
    pub shard_id: usize,
    /// Unix timestamp of last flush (milliseconds since epoch).
    pub last_flush_timestamp_ms: u64,
    /// Unix timestamp of last sync (milliseconds since epoch, if applicable).
    pub last_sync_timestamp_ms: Option<u64>,
}

/// Payload sent over the channel.
enum WalEntry {
    Put {
        key: Bytes,
        value: Vec<u8>,
        size_estimate: usize,
    },
    Delete {
        key: Bytes,
        size_estimate: usize,
    },
}

/// Messages to the flush thread.
enum WalCommand {
    Write(WalEntry),
    Flush {
        done_tx: flume::Sender<std::io::Result<()>>,
    },
}

/// Shared atomic counters for lock-free lag_stats reads.
struct WalLagAtomics {
    pending_ops: AtomicUsize,
    pending_bytes: AtomicUsize,
    last_flush_timestamp_ms: AtomicU64,
}

/// WAL writer that persists operations to RocksDB.
///
/// Writes are sent via a bounded channel to a dedicated flush thread,
/// keeping the shard event loop free from blocking RocksDB I/O.
pub struct RocksWalWriter {
    shard_id: usize,
    sequence: AtomicU64,
    config: WalConfig,
    cmd_tx: flume::Sender<WalCommand>,
    lag: Arc<WalLagAtomics>,
    /// Unix timestamp (ms) of last sync (for Periodic mode tracking).
    last_sync_timestamp_ms: AtomicU64,
    flush_thread: Option<std::thread::JoinHandle<()>>,
}

/// Get current unix timestamp in milliseconds.
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl RocksWalWriter {
    /// Create a new WAL writer for a shard.
    pub fn new(
        rocks: Arc<RocksStore>,
        shard_id: usize,
        config: WalConfig,
        metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Self {
        let durability_mode = match &config.mode {
            DurabilityMode::Async => "async".to_string(),
            DurabilityMode::Periodic { interval_ms } => format!("periodic_{}ms", interval_ms),
            DurabilityMode::Sync => "sync".to_string(),
        };
        debug!(shard_id, durability_mode = %durability_mode, "WAL writer created");

        let now_ms = current_timestamp_ms();

        let (cmd_tx, cmd_rx) = flume::bounded(config.channel_capacity);

        let lag = Arc::new(WalLagAtomics {
            pending_ops: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(now_ms),
        });

        let flush_thread = {
            let lag = Arc::clone(&lag);
            let batch_size_threshold = config.batch_size_threshold;
            let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
            let mode = config.mode.clone();

            std::thread::Builder::new()
                .name(format!("wal-flush-{shard_id}"))
                .spawn(move || {
                    flush_thread_loop(
                        cmd_rx,
                        rocks,
                        shard_id,
                        mode,
                        batch_size_threshold,
                        batch_timeout,
                        lag,
                        metrics_recorder,
                    );
                })
                .expect("failed to spawn WAL flush thread")
        };

        Self {
            shard_id,
            sequence: AtomicU64::new(0),
            config,
            cmd_tx,
            lag,
            last_sync_timestamp_ms: AtomicU64::new(now_ms),
            flush_thread: Some(flush_thread),
        }
    }

    /// Write a SET operation with value and metadata.
    pub async fn write_set(
        &self,
        key: &[u8],
        value: &Value,
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        let serialized = serialize(value, metadata);
        let size_estimate = key.len() + serialized.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Put {
                key: Bytes::copy_from_slice(key),
                value: serialized,
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_set)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;

        Ok(seq)
    }

    /// Write a DELETE operation.
    pub async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        let size_estimate = key.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Delete {
                key: Bytes::copy_from_slice(key),
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_delete)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;

        Ok(seq)
    }

    /// Force flush any pending writes.
    pub async fn flush_async(&self) -> std::io::Result<()> {
        let (done_tx, done_rx) = flume::bounded(1);
        self.cmd_tx
            .send_async(WalCommand::Flush { done_tx })
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (flush_async)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        done_rx.recv_async().await.map_err(|_| {
            error!(shard_id = self.shard_id, "WAL flush response lost");
            std::io::Error::other("WAL flush response channel disconnected")
        })?
    }

    /// Get lag statistics for this WAL writer.
    ///
    /// Pure atomic reads — no mutex, no channel send.
    pub fn lag_stats(&self) -> WalLagStats {
        let now_ms = current_timestamp_ms();

        let last_flush_ts = self.lag.last_flush_timestamp_ms.load(Ordering::Acquire);
        let durability_lag_ms = now_ms.saturating_sub(last_flush_ts);

        let (sync_lag_ms, last_sync_timestamp_ms) = match &self.config.mode {
            DurabilityMode::Periodic { .. } | DurabilityMode::Sync => {
                let last_sync = self.last_sync_timestamp_ms.load(Ordering::Acquire);
                (Some(now_ms.saturating_sub(last_sync)), Some(last_sync))
            }
            DurabilityMode::Async => (None, None),
        };

        WalLagStats {
            pending_ops: self.lag.pending_ops.load(Ordering::Acquire),
            pending_bytes: self.lag.pending_bytes.load(Ordering::Acquire),
            durability_lag_ms,
            sync_lag_ms,
            sequence: self.sequence.load(Ordering::SeqCst),
            shard_id: self.shard_id,
            last_flush_timestamp_ms: last_flush_ts,
            last_sync_timestamp_ms,
        }
    }

    /// Record that a sync operation occurred (called by periodic sync task).
    pub fn record_sync(&self) {
        self.last_sync_timestamp_ms
            .store(current_timestamp_ms(), Ordering::Release);
    }

    /// Get the current sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get the shard ID this writer is for.
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
}

impl Drop for RocksWalWriter {
    fn drop(&mut self) {
        // Drop the sender to disconnect the channel, signaling the flush thread to exit.
        // We replace cmd_tx with a disconnected sender so the thread sees Disconnected.
        let (dead_tx, _) = flume::bounded(0);
        let _ = std::mem::replace(&mut self.cmd_tx, dead_tx);

        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
    }
}

impl WalWriter for RocksWalWriter {
    fn append(&mut self, operation: &WalOperation) -> u64 {
        match operation {
            WalOperation::Set { key, value } => {
                let metadata = KeyMetadata::new(value.len());
                let serialized = serialize(&Value::string(value.clone()), &metadata);
                let size_estimate = key.len() + serialized.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Put {
                    key: key.clone(),
                    value: serialized,
                    size_estimate,
                }));

                seq
            }
            WalOperation::SetWithExpiry {
                key,
                value,
                expires_at,
            } => {
                let mut metadata = KeyMetadata::new(value.len());
                metadata.expires_at = Some(*expires_at);
                let serialized = serialize(&Value::string(value.clone()), &metadata);
                let size_estimate = key.len() + serialized.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Put {
                    key: key.clone(),
                    value: serialized,
                    size_estimate,
                }));

                seq
            }
            WalOperation::Delete { key } => {
                let size_estimate = key.len() + 32;
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                let _ = self.cmd_tx.send(WalCommand::Write(WalEntry::Delete {
                    key: key.clone(),
                    size_estimate,
                }));

                seq
            }
            WalOperation::Expire { key: _, at: _ } => {
                // Expiry updates are handled via SET with updated metadata
                self.sequence.fetch_add(1, Ordering::SeqCst) + 1
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let (done_tx, done_rx) = flume::bounded(1);
        self.cmd_tx
            .send(WalCommand::Flush { done_tx })
            .map_err(|_| std::io::Error::other("WAL flush thread disconnected"))?;
        done_rx
            .recv()
            .map_err(|_| std::io::Error::other("WAL flush response channel disconnected"))?
    }

    fn current_sequence(&self) -> u64 {
        self.sequence()
    }
}

/// Background flush thread loop.
#[allow(clippy::too_many_arguments)]
fn flush_thread_loop(
    rx: flume::Receiver<WalCommand>,
    rocks: Arc<RocksStore>,
    shard_id: usize,
    mode: DurabilityMode,
    batch_size_threshold: usize,
    batch_timeout: Duration,
    lag: Arc<WalLagAtomics>,
    metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
) {
    let mut batch = WriteBatch::default();
    let mut batch_size: usize = 0;
    let mut batch_ops: usize = 0;
    let mut last_flush = Instant::now();

    let is_sync = matches!(mode, DurabilityMode::Sync);
    let shard_label = shard_id.to_string();
    let metrics: &dyn frogdb_types::traits::MetricsRecorder = &*metrics_recorder;

    loop {
        // Block until an entry arrives or timeout
        match rx.recv_timeout(batch_timeout) {
            Ok(cmd) => {
                match cmd {
                    WalCommand::Write(entry) => {
                        apply_entry(
                            &rocks,
                            shard_id,
                            &mut batch,
                            &mut batch_size,
                            &mut batch_ops,
                            &lag,
                            metrics,
                            &shard_label,
                            entry,
                        );

                        // Drain remaining entries up to batch size threshold
                        while batch_size < batch_size_threshold {
                            match rx.try_recv() {
                                Ok(WalCommand::Write(entry)) => {
                                    apply_entry(
                                        &rocks,
                                        shard_id,
                                        &mut batch,
                                        &mut batch_size,
                                        &mut batch_ops,
                                        &lag,
                                        metrics,
                                        &shard_label,
                                        entry,
                                    );
                                }
                                Ok(WalCommand::Flush { done_tx }) => {
                                    let result = do_flush(
                                        &rocks,
                                        shard_id,
                                        is_sync,
                                        &mut batch,
                                        &mut batch_size,
                                        &mut batch_ops,
                                        &mut last_flush,
                                        &lag,
                                        metrics,
                                        &shard_label,
                                    );
                                    let _ = done_tx.send(result);
                                    break;
                                }
                                Err(_) => break, // empty
                            }
                        }

                        // Flush if size threshold reached or timeout elapsed
                        if batch_size >= batch_size_threshold
                            || last_flush.elapsed() >= batch_timeout
                        {
                            let _ = do_flush(
                                &rocks,
                                shard_id,
                                is_sync,
                                &mut batch,
                                &mut batch_size,
                                &mut batch_ops,
                                &mut last_flush,
                                &lag,
                                metrics,
                                &shard_label,
                            );
                        }
                    }
                    WalCommand::Flush { done_tx } => {
                        let result = do_flush(
                            &rocks,
                            shard_id,
                            is_sync,
                            &mut batch,
                            &mut batch_size,
                            &mut batch_ops,
                            &mut last_flush,
                            &lag,
                            metrics,
                            &shard_label,
                        );
                        let _ = done_tx.send(result);
                    }
                }
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                // Timeout — flush if there are pending entries
                if !batch.is_empty() {
                    let _ = do_flush(
                        &rocks,
                        shard_id,
                        is_sync,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &mut last_flush,
                        &lag,
                        metrics,
                        &shard_label,
                    );
                }
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                // Channel disconnected — flush remaining and exit
                // Drain any remaining entries
                while let Ok(cmd) = rx.try_recv() {
                    match cmd {
                        WalCommand::Write(entry) => {
                            apply_entry(
                                &rocks,
                                shard_id,
                                &mut batch,
                                &mut batch_size,
                                &mut batch_ops,
                                &lag,
                                metrics,
                                &shard_label,
                                entry,
                            );
                        }
                        WalCommand::Flush { done_tx } => {
                            let result = do_flush(
                                &rocks,
                                shard_id,
                                is_sync,
                                &mut batch,
                                &mut batch_size,
                                &mut batch_ops,
                                &mut last_flush,
                                &lag,
                                metrics,
                                &shard_label,
                            );
                            let _ = done_tx.send(result);
                        }
                    }
                }
                if !batch.is_empty() {
                    let _ = do_flush(
                        &rocks,
                        shard_id,
                        is_sync,
                        &mut batch,
                        &mut batch_size,
                        &mut batch_ops,
                        &mut last_flush,
                        &lag,
                        metrics,
                        &shard_label,
                    );
                }
                info!(shard_id, "WAL flush thread exiting");
                return;
            }
        }
    }
}

/// Apply a single WAL entry to the current batch.
#[allow(clippy::too_many_arguments)]
fn apply_entry(
    rocks: &RocksStore,
    shard_id: usize,
    batch: &mut WriteBatch,
    batch_size: &mut usize,
    batch_ops: &mut usize,
    lag: &WalLagAtomics,
    metrics_recorder: &dyn frogdb_types::traits::MetricsRecorder,
    shard_label: &str,
    entry: WalEntry,
) {
    match entry {
        WalEntry::Put {
            key,
            value,
            size_estimate,
        } => {
            if let Err(e) = rocks.batch_put(batch, shard_id, &key, &value) {
                error!(shard_id, error = %e, "WAL batch_put failed");
                return;
            }
            *batch_size += size_estimate;
            *batch_ops += 1;
            lag.pending_ops.fetch_add(1, Ordering::Release);
            lag.pending_bytes
                .fetch_add(size_estimate, Ordering::Release);

            metrics_recorder.increment_counter(
                "frogdb_wal_writes_total",
                1,
                &[("shard", shard_label)],
            );
            metrics_recorder.increment_counter(
                "frogdb_wal_bytes_total",
                size_estimate as u64,
                &[("shard", shard_label)],
            );
        }
        WalEntry::Delete { key, size_estimate } => {
            if let Err(e) = rocks.batch_delete(batch, shard_id, &key) {
                error!(shard_id, error = %e, "WAL batch_delete failed");
                return;
            }
            *batch_size += size_estimate;
            *batch_ops += 1;
            lag.pending_ops.fetch_add(1, Ordering::Release);
            lag.pending_bytes
                .fetch_add(size_estimate, Ordering::Release);
        }
    }
}

/// Flush the current batch to RocksDB.
#[allow(clippy::too_many_arguments)]
fn do_flush(
    rocks: &RocksStore,
    shard_id: usize,
    is_sync: bool,
    batch: &mut WriteBatch,
    batch_size: &mut usize,
    batch_ops: &mut usize,
    last_flush: &mut Instant,
    lag: &WalLagAtomics,
    metrics_recorder: &dyn frogdb_types::traits::MetricsRecorder,
    shard_label: &str,
) -> std::io::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let start = Instant::now();

    let flushed_batch = std::mem::take(batch);
    let flushed_bytes = *batch_size;
    let flushed_ops = *batch_ops;
    let batch_len = flushed_batch.len();
    *batch_size = 0;
    *batch_ops = 0;

    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(is_sync);

    rocks
        .write_batch_opt(flushed_batch, &write_opts)
        .map_err(|e| {
            error!(shard_id, error = %e, "WAL flush failed");
            std::io::Error::other(e)
        })?;

    // Update lag atomics
    lag.pending_ops.fetch_sub(flushed_ops, Ordering::Release);
    lag.pending_bytes
        .fetch_sub(flushed_bytes, Ordering::Release);
    lag.last_flush_timestamp_ms
        .store(current_timestamp_ms(), Ordering::Release);

    *last_flush = Instant::now();

    let duration = start.elapsed();
    metrics_recorder.record_histogram(
        "frogdb_wal_flush_duration_seconds",
        duration.as_secs_f64(),
        &[("shard", shard_label)],
    );

    trace!(
        shard_id,
        entries = batch_len,
        bytes = flushed_bytes,
        duration_ms = duration.as_millis() as u64,
        "WAL batch flushed"
    );

    Ok(())
}

/// Spawns a background task that periodically syncs the WAL for all shards.
///
/// Returns a handle that can be used to stop the background sync.
pub fn spawn_periodic_sync(
    rocks: Arc<RocksStore>,
    interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    info!(interval_ms, "Periodic WAL sync started");
    let interval = Duration::from_millis(interval_ms);

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            ticker.tick().await;

            if let Err(e) = rocks.flush() {
                tracing::warn!(error = %e, "Failed to sync WAL");
            }
        }
    })
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use frogdb_types::traits::NoopMetricsRecorder;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_write_and_flush() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 1000,
                ..Default::default()
            },
            metrics,
        );

        let value = Value::string("test_value");
        let metadata = KeyMetadata::new(10);

        let seq = wal.write_set(b"key1", &value, &metadata).await.unwrap();
        assert_eq!(seq, 1);

        wal.flush_async().await.unwrap();

        // Verify data was written
        let data = rocks.get(0, b"key1").unwrap();
        assert!(data.is_some());
    }

    #[tokio::test]
    async fn test_wal_delete() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );

        // First write some data
        let value = Value::string("test");
        let metadata = KeyMetadata::new(4);
        let serialized = serialize(&value, &metadata);
        rocks.put(0, b"key", &serialized).unwrap();

        // Now delete via WAL
        let metrics = Arc::new(NoopMetricsRecorder::new());
        let wal = RocksWalWriter::new(rocks.clone(), 0, WalConfig::default(), metrics);

        let seq = wal.write_delete(b"key").await.unwrap();
        assert_eq!(seq, 1);

        wal.flush_async().await.unwrap();

        // Verify data was deleted
        let data = rocks.get(0, b"key").unwrap();
        assert!(data.is_none());
    }

    #[tokio::test]
    async fn test_wal_batch_threshold() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 100, // Very small threshold
                batch_timeout_ms: 60000,   // Long timeout
                ..Default::default()
            },
            metrics,
        );

        // Write enough data to trigger threshold
        let value = Value::string("x".repeat(200)); // Large enough to trigger
        let metadata = KeyMetadata::new(200);

        wal.write_set(b"bigkey", &value, &metadata).await.unwrap();

        // Flush explicitly — the flush thread handles batching asynchronously
        wal.flush_async().await.unwrap();

        // Data should be visible after flush
        let data = rocks.get(0, b"bigkey").unwrap();
        assert!(data.is_some());
    }

    #[tokio::test]
    async fn test_wal_sequence() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), metrics);

        assert_eq!(wal.sequence(), 0);

        let value = Value::string("v");
        let metadata = KeyMetadata::new(1);

        let seq1 = wal.write_set(b"k1", &value, &metadata).await.unwrap();
        let seq2 = wal.write_set(b"k2", &value, &metadata).await.unwrap();
        let seq3 = wal.write_delete(b"k1").await.unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
        assert_eq!(wal.sequence(), 3);
    }

    #[tokio::test]
    async fn test_wal_drop_flushes_pending() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        {
            let wal = RocksWalWriter::new(
                rocks.clone(),
                0,
                WalConfig {
                    mode: DurabilityMode::Async,
                    batch_size_threshold: 1024 * 1024, // Large threshold — won't auto-flush
                    batch_timeout_ms: 60000,           // Long timeout
                    ..Default::default()
                },
                metrics,
            );

            let value = Value::string("drop_test");
            let metadata = KeyMetadata::new(9);
            wal.write_set(b"dropkey", &value, &metadata).await.unwrap();

            // Drop without explicit flush
        }

        // Data should be persisted by the Drop impl
        let data = rocks.get(0, b"dropkey").unwrap();
        assert!(data.is_some());
    }

    #[tokio::test]
    async fn test_wal_backpressure_no_data_loss() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 60000,
                channel_capacity: 1, // Minimal channel — tests backpressure
            },
            metrics,
        );

        let count = 50;
        let value = Value::string("bp");
        let metadata = KeyMetadata::new(2);

        for i in 0..count {
            let key = format!("bpkey{i}");
            wal.write_set(key.as_bytes(), &value, &metadata)
                .await
                .unwrap();
        }

        wal.flush_async().await.unwrap();

        for i in 0..count {
            let key = format!("bpkey{i}");
            let data = rocks.get(0, key.as_bytes()).unwrap();
            assert!(data.is_some(), "missing key {key}");
        }
    }

    #[tokio::test]
    async fn test_wal_lag_stats_sync() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );
        let metrics = Arc::new(NoopMetricsRecorder::new());

        let wal = RocksWalWriter::new(rocks, 0, WalConfig::default(), metrics);

        // lag_stats is sync — no .await
        let stats = wal.lag_stats();
        assert_eq!(stats.shard_id, 0);
        assert_eq!(stats.sequence, 0);
    }
}
