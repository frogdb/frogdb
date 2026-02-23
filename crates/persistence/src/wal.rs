//! Write-Ahead Log (WAL) implementation using RocksDB.
//!
//! Supports three durability modes:
//! - Async: No fsync (fastest, least durable)
//! - Periodic: fsync at configurable intervals (default: 1s)
//! - Sync: fsync every write (slowest, most durable)

use rocksdb::{WriteBatch, WriteOptions};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
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
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: DurabilityMode::default(),
            batch_size_threshold: 4 * 1024 * 1024, // 4MB
            batch_timeout_ms: 10,
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

/// WAL writer that persists operations to RocksDB.
///
/// Accumulates writes in a batch and flushes based on size/time thresholds.
pub struct RocksWalWriter {
    rocks: Arc<RocksStore>,
    shard_id: usize,
    pending_batch: Mutex<BatchState>,
    sequence: AtomicU64,
    config: WalConfig,
    metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    /// Unix timestamp (ms) of last sync (for Periodic mode tracking).
    last_sync_timestamp_ms: AtomicU64,
}

struct BatchState {
    batch: WriteBatch,
    /// Current batch size in bytes.
    size: usize,
    /// Number of operations in current batch.
    ops_count: usize,
    /// Instant of last flush (for batch timing).
    last_flush: Instant,
    /// Unix timestamp (ms) of last flush.
    last_flush_timestamp_ms: u64,
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

        Self {
            rocks,
            shard_id,
            pending_batch: Mutex::new(BatchState {
                batch: WriteBatch::default(),
                size: 0,
                ops_count: 0,
                last_flush: Instant::now(),
                last_flush_timestamp_ms: now_ms,
            }),
            sequence: AtomicU64::new(0),
            config,
            metrics_recorder,
            last_sync_timestamp_ms: AtomicU64::new(now_ms),
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
        self.write_raw(key, &serialized).await.map_err(|e| {
            error!(shard_id = self.shard_id, key_len = key.len(), error = %e, "WAL write_set failed");
            e
        })
    }

    /// Write a DELETE operation.
    pub async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let mut state = self.pending_batch.lock().await;
        self.rocks
            .batch_delete(&mut state.batch, self.shard_id, key)
            .map_err(|e| {
                error!(shard_id = self.shard_id, key_len = key.len(), error = %e, "WAL write_delete failed");
                std::io::Error::other(e)
            })?;

        // Estimate delete size (key + overhead)
        state.size += key.len() + 32;
        state.ops_count += 1;

        self.maybe_flush_locked(&mut state).await?;

        Ok(seq)
    }

    /// Write raw key-value data.
    async fn write_raw(&self, key: &[u8], value: &[u8]) -> std::io::Result<u64> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let mut state = self.pending_batch.lock().await;
        self.rocks
            .batch_put(&mut state.batch, self.shard_id, key, value)
            .map_err(std::io::Error::other)?;

        let bytes_written = key.len() + value.len() + 32; // Add overhead
        state.size += bytes_written;
        state.ops_count += 1;

        // Record WAL metrics
        let shard_label = self.shard_id.to_string();
        self.metrics_recorder.increment_counter(
            "frogdb_wal_writes_total",
            1,
            &[("shard", &shard_label)],
        );
        self.metrics_recorder.increment_counter(
            "frogdb_wal_bytes_total",
            bytes_written as u64,
            &[("shard", &shard_label)],
        );

        self.maybe_flush_locked(&mut state).await?;

        Ok(seq)
    }

    /// Check if we should flush based on size or time thresholds.
    async fn maybe_flush_locked(&self, state: &mut BatchState) -> std::io::Result<()> {
        let should_flush = state.size >= self.config.batch_size_threshold
            || state.last_flush.elapsed() >= Duration::from_millis(self.config.batch_timeout_ms);

        if should_flush {
            self.do_flush_locked(state).await?;
        }

        Ok(())
    }

    /// Actually perform the flush.
    async fn do_flush_locked(&self, state: &mut BatchState) -> std::io::Result<()> {
        if state.batch.is_empty() {
            return Ok(());
        }

        let start = Instant::now();

        let batch = std::mem::take(&mut state.batch);
        let batch_len = batch.len();
        let bytes = state.size;
        state.size = 0;
        state.ops_count = 0;
        state.last_flush = Instant::now();
        state.last_flush_timestamp_ms = current_timestamp_ms();

        let mut write_opts = WriteOptions::default();

        // Configure sync based on durability mode
        let is_sync = matches!(&self.config.mode, DurabilityMode::Sync);
        match &self.config.mode {
            DurabilityMode::Sync => {
                write_opts.set_sync(true);
            }
            DurabilityMode::Async | DurabilityMode::Periodic { .. } => {
                write_opts.set_sync(false);
            }
        }

        self.rocks
            .write_batch_opt(batch, &write_opts)
            .map_err(|e| {
                error!(shard_id = self.shard_id, error = %e, "WAL flush failed");
                std::io::Error::other(e)
            })?;

        // In Sync mode, flush is also a sync
        if is_sync {
            self.last_sync_timestamp_ms
                .store(current_timestamp_ms(), Ordering::Release);
        }

        // Record flush duration
        let duration = start.elapsed();
        let duration_secs = duration.as_secs_f64();
        let shard_label = self.shard_id.to_string();
        self.metrics_recorder.record_histogram(
            "frogdb_wal_flush_duration_seconds",
            duration_secs,
            &[("shard", &shard_label)],
        );

        trace!(
            shard_id = self.shard_id,
            entries = batch_len,
            bytes,
            duration_ms = duration.as_millis() as u64,
            "WAL batch flushed"
        );

        Ok(())
    }

    /// Force flush any pending writes.
    pub async fn flush_async(&self) -> std::io::Result<()> {
        let mut state = self.pending_batch.lock().await;
        self.do_flush_locked(&mut state).await
    }

    /// Get lag statistics for this WAL writer.
    ///
    /// Returns information about pending operations and durability lag.
    pub async fn lag_stats(&self) -> WalLagStats {
        let state = self.pending_batch.lock().await;
        let now_ms = current_timestamp_ms();

        // Calculate durability lag (time since last flush)
        let durability_lag_ms = now_ms.saturating_sub(state.last_flush_timestamp_ms);

        // Calculate sync lag (only relevant for Periodic mode)
        let (sync_lag_ms, last_sync_timestamp_ms) = match &self.config.mode {
            DurabilityMode::Periodic { .. } => {
                let last_sync = self.last_sync_timestamp_ms.load(Ordering::Acquire);
                (Some(now_ms.saturating_sub(last_sync)), Some(last_sync))
            }
            DurabilityMode::Sync => {
                // In sync mode, sync lag equals durability lag
                let last_sync = self.last_sync_timestamp_ms.load(Ordering::Acquire);
                (Some(now_ms.saturating_sub(last_sync)), Some(last_sync))
            }
            DurabilityMode::Async => {
                // No sync tracking in async mode
                (None, None)
            }
        };

        WalLagStats {
            pending_ops: state.ops_count,
            pending_bytes: state.size,
            durability_lag_ms,
            sync_lag_ms,
            sequence: self.sequence.load(Ordering::SeqCst),
            shard_id: self.shard_id,
            last_flush_timestamp_ms: state.last_flush_timestamp_ms,
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

impl WalWriter for RocksWalWriter {
    fn append(&mut self, operation: &WalOperation) -> u64 {
        // This is a synchronous interface, but our implementation is async.
        // We use block_on for the sync interface, though the async methods should be preferred.
        let rt = tokio::runtime::Handle::try_current();

        match operation {
            WalOperation::Set { key, value } => {
                let metadata = KeyMetadata::new(value.len());
                let serialized = serialize(&Value::string(value.clone()), &metadata);

                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                if let Ok(rt) = rt {
                    rt.block_on(async {
                        let mut state = self.pending_batch.lock().await;
                        let _ =
                            self.rocks
                                .batch_put(&mut state.batch, self.shard_id, key, &serialized);
                        state.size += key.len() + serialized.len() + 32;
                        state.ops_count += 1;
                        let _ = self.maybe_flush_locked(&mut state).await;
                    });
                }

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

                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                if let Ok(rt) = rt {
                    rt.block_on(async {
                        let mut state = self.pending_batch.lock().await;
                        let _ =
                            self.rocks
                                .batch_put(&mut state.batch, self.shard_id, key, &serialized);
                        state.size += key.len() + serialized.len() + 32;
                        state.ops_count += 1;
                        let _ = self.maybe_flush_locked(&mut state).await;
                    });
                }

                seq
            }
            WalOperation::Delete { key } => {
                let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

                if let Ok(rt) = rt {
                    rt.block_on(async {
                        let mut state = self.pending_batch.lock().await;
                        let _ = self
                            .rocks
                            .batch_delete(&mut state.batch, self.shard_id, key);
                        state.size += key.len() + 32;
                        state.ops_count += 1;
                        let _ = self.maybe_flush_locked(&mut state).await;
                    });
                }

                seq
            }
            WalOperation::Expire { key: _, at: _ } => {
                // Expiry updates are handled via SET with updated metadata
                // For now, just increment sequence
                self.sequence.fetch_add(1, Ordering::SeqCst) + 1
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.block_on(self.flush_async())
        } else {
            Ok(())
        }
    }

    fn current_sequence(&self) -> u64 {
        self.sequence()
    }
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
            },
            metrics,
        );

        // Write enough data to trigger threshold
        let value = Value::string("x".repeat(200)); // Large enough to trigger
        let metadata = KeyMetadata::new(200);

        wal.write_set(b"bigkey", &value, &metadata).await.unwrap();

        // Data should have been flushed due to size threshold
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
}
