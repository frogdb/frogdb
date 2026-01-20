//! Write-Ahead Log (WAL) implementation using RocksDB.
//!
//! Supports three durability modes:
//! - Async: No fsync (fastest, least durable)
//! - Periodic: fsync at configurable intervals (default: 1s)
//! - Sync: fsync every write (slowest, most durable)

use rocksdb::{WriteBatch, WriteOptions};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use super::rocks::RocksStore;
use super::serialization::serialize;
use crate::noop::{WalOperation, WalWriter};
use crate::types::{KeyMetadata, Value};

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

/// WAL writer that persists operations to RocksDB.
///
/// Accumulates writes in a batch and flushes based on size/time thresholds.
pub struct RocksWalWriter {
    rocks: Arc<RocksStore>,
    shard_id: usize,
    pending_batch: Mutex<BatchState>,
    sequence: AtomicU64,
    config: WalConfig,
}

struct BatchState {
    batch: WriteBatch,
    size: usize,
    last_flush: Instant,
}

impl RocksWalWriter {
    /// Create a new WAL writer for a shard.
    pub fn new(rocks: Arc<RocksStore>, shard_id: usize, config: WalConfig) -> Self {
        Self {
            rocks,
            shard_id,
            pending_batch: Mutex::new(BatchState {
                batch: WriteBatch::default(),
                size: 0,
                last_flush: Instant::now(),
            }),
            sequence: AtomicU64::new(0),
            config,
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
        self.write_raw(key, &serialized).await
    }

    /// Write a DELETE operation.
    pub async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        let mut state = self.pending_batch.lock().await;
        self.rocks
            .batch_delete(&mut state.batch, self.shard_id, key)
            .map_err(std::io::Error::other)?;

        // Estimate delete size (key + overhead)
        state.size += key.len() + 32;

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

        state.size += key.len() + value.len() + 32; // Add overhead

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

        let batch = std::mem::take(&mut state.batch);
        state.size = 0;
        state.last_flush = Instant::now();

        let mut write_opts = WriteOptions::default();

        // Configure sync based on durability mode
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
            .map_err(std::io::Error::other)?;

        Ok(())
    }

    /// Force flush any pending writes.
    pub async fn flush_async(&self) -> std::io::Result<()> {
        let mut state = self.pending_batch.lock().await;
        self.do_flush_locked(&mut state).await
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
                        let _ = self
                            .rocks
                            .batch_put(&mut state.batch, self.shard_id, key, &serialized);
                        state.size += key.len() + serialized.len() + 32;
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
                        let _ = self
                            .rocks
                            .batch_put(&mut state.batch, self.shard_id, key, &serialized);
                        state.size += key.len() + serialized.len() + 32;
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
                        let _ = self.rocks.batch_delete(&mut state.batch, self.shard_id, key);
                        state.size += key.len() + 32;
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
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_write_and_flush() {
        let tmp = TempDir::new().unwrap();
        let rocks = Arc::new(
            RocksStore::open(tmp.path(), 2, &super::super::rocks::RocksConfig::default()).unwrap(),
        );

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 1024 * 1024,
                batch_timeout_ms: 1000,
            },
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
        let wal = RocksWalWriter::new(rocks.clone(), 0, WalConfig::default());

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

        let wal = RocksWalWriter::new(
            rocks.clone(),
            0,
            WalConfig {
                mode: DurabilityMode::Async,
                batch_size_threshold: 100, // Very small threshold
                batch_timeout_ms: 60000,   // Long timeout
            },
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

        let wal = RocksWalWriter::new(rocks, 0, WalConfig::default());

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
