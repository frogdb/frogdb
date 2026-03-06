//! Test harness infrastructure for crash recovery tests.
//!
//! Provides utilities for simulating crashes, manipulating files,
//! and measuring recovery correctness.

#![allow(dead_code)] // Many helpers available for future tests

use bytes::Bytes;
use rocksdb::WriteOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

use super::recovery::{RecoveryStats, recover_all_shards};
use super::rocks::{RocksConfig, RocksStore};
use super::serialization::serialize;
use super::wal::{DurabilityMode, RocksWalWriter, WalConfig};
use crate::noop::{ExpiryIndex, NoopMetricsRecorder};
use crate::store::{HashMapStore, Store};
use crate::types::{KeyMetadata, Value};

/// Test harness for crash simulation.
///
/// Provides a controlled environment for testing crash recovery by:
/// - Creating isolated RocksDB instances in temp directories
/// - Allowing crash simulation via Drop without explicit close
/// - Supporting reopen and recovery verification
pub struct CrashTestHarness {
    /// Temporary directory (kept around for the lifetime of the test)
    pub dir: TempDir,
    /// RocksDB store (None after crash simulation)
    pub rocks: Option<Arc<RocksStore>>,
    /// WAL configuration
    pub wal_config: WalConfig,
    /// RocksDB configuration
    pub rocks_config: RocksConfig,
    /// Number of shards
    pub num_shards: usize,
    /// Metrics recorder for WAL writers
    pub metrics: Arc<NoopMetricsRecorder>,
}

impl CrashTestHarness {
    /// Create a new crash test harness with default configuration.
    pub fn new() -> Self {
        Self::with_config(WalConfig::default(), RocksConfig::default(), 4)
    }

    /// Create a new crash test harness with custom configuration.
    pub fn with_config(
        wal_config: WalConfig,
        rocks_config: RocksConfig,
        num_shards: usize,
    ) -> Self {
        let dir = TempDir::new().expect("Failed to create temp directory");
        let rocks = Arc::new(
            RocksStore::open(dir.path(), num_shards, &rocks_config)
                .expect("Failed to open RocksDB"),
        );

        Self {
            dir,
            rocks: Some(rocks),
            wal_config,
            rocks_config,
            num_shards,
            metrics: Arc::new(NoopMetricsRecorder::new()),
        }
    }

    /// Create a harness with sync durability mode.
    pub fn with_sync_mode() -> Self {
        let wal_config = WalConfig {
            mode: DurabilityMode::Sync,
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            ..Default::default()
        };
        Self::with_config(wal_config, RocksConfig::default(), 4)
    }

    /// Create a harness with periodic durability mode.
    pub fn with_periodic_mode(interval_ms: u64) -> Self {
        let wal_config = WalConfig {
            mode: DurabilityMode::Periodic { interval_ms },
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            ..Default::default()
        };
        Self::with_config(wal_config, RocksConfig::default(), 4)
    }

    /// Create a harness with async durability mode.
    pub fn with_async_mode() -> Self {
        let wal_config = WalConfig {
            mode: DurabilityMode::Async,
            batch_size_threshold: 4 * 1024 * 1024,
            batch_timeout_ms: 10,
            ..Default::default()
        };
        Self::with_config(wal_config, RocksConfig::default(), 4)
    }

    /// Get the RocksStore reference.
    ///
    /// Panics if called after crash().
    pub fn rocks(&self) -> &Arc<RocksStore> {
        self.rocks
            .as_ref()
            .expect("RocksStore not available after crash")
    }

    /// Get the database path.
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// Create a WAL writer for a shard.
    pub fn create_wal_writer(&self, shard_id: usize) -> RocksWalWriter {
        RocksWalWriter::new(
            self.rocks().clone(),
            shard_id,
            self.wal_config.clone(),
            self.metrics.clone(),
        )
    }

    /// Write a key-value pair directly to RocksDB (bypassing WAL batching).
    pub fn put_direct(&self, shard_id: usize, key: &[u8], value: &Value) {
        let metadata = KeyMetadata::new(value.memory_size());
        let serialized = serialize(value, &metadata);
        self.rocks()
            .put(shard_id, key, &serialized)
            .expect("Failed to put");
    }

    /// Write a key-value pair with explicit sync option.
    pub fn put_with_sync(&self, shard_id: usize, key: &[u8], value: &Value, sync: bool) {
        let metadata = KeyMetadata::new(value.memory_size());
        let serialized = serialize(value, &metadata);
        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(sync);
        self.rocks()
            .put_opt(shard_id, key, &serialized, &write_opts)
            .expect("Failed to put");
    }

    /// Write a key-value pair with expiry.
    pub fn put_with_expiry(&self, shard_id: usize, key: &[u8], value: &Value, expires_at: Instant) {
        let mut metadata = KeyMetadata::new(value.memory_size());
        metadata.expires_at = Some(expires_at);
        let serialized = serialize(value, &metadata);
        self.rocks()
            .put(shard_id, key, &serialized)
            .expect("Failed to put");
    }

    /// Write a key-value pair with LFU counter.
    pub fn put_with_lfu(&self, shard_id: usize, key: &[u8], value: &Value, lfu_counter: u8) {
        let mut metadata = KeyMetadata::new(value.memory_size());
        metadata.lfu_counter = lfu_counter;
        let serialized = serialize(value, &metadata);
        self.rocks()
            .put(shard_id, key, &serialized)
            .expect("Failed to put");
    }

    /// Flush all pending writes to disk.
    pub fn flush(&self) {
        self.rocks().flush().expect("Failed to flush");
    }

    /// Sync WAL to disk.
    pub fn sync_wal(&self) {
        self.rocks().sync_wal().expect("Failed to sync WAL");
    }

    /// Simulate a crash by dropping the RocksStore without explicit close.
    ///
    /// This mimics what happens during an unexpected process termination.
    /// Pending unflushed data may be lost depending on durability mode.
    pub fn crash(&mut self) {
        // Drop RocksStore without sync - simulates crash
        self.rocks = None;
    }

    /// Simulate a crash by dropping the RocksStore without flushing.
    /// This is an alias for `crash()`.
    pub fn simulate_crash(&mut self) {
        self.crash();
    }

    /// Reopen the database after a crash.
    ///
    /// Returns the reopened RocksStore.
    pub fn reopen(&mut self) -> Arc<RocksStore> {
        assert!(self.rocks.is_none(), "Must crash before reopening");
        let rocks = Arc::new(
            RocksStore::open(self.dir.path(), self.num_shards, &self.rocks_config)
                .expect("Failed to reopen RocksDB"),
        );
        self.rocks = Some(rocks.clone());
        rocks
    }

    /// Reopen and run full recovery, returning stores and statistics.
    pub fn recover(&mut self) -> (Vec<(HashMapStore, ExpiryIndex)>, RecoveryStats) {
        let rocks = self.reopen();
        recover_all_shards(&rocks).expect("Failed to recover")
    }

    /// Verify that a key exists and has the expected string value.
    pub fn verify_string_key(&self, shard_id: usize, key: &[u8], expected: &str) -> bool {
        if let Some(data) = self.rocks().get(shard_id, key).expect("Failed to get")
            && let Ok((value, _)) = super::serialization::deserialize(&data)
            && let Some(sv) = value.as_string()
        {
            return sv.as_bytes().as_ref() == expected.as_bytes();
        }
        false
    }

    /// Get the count of keys in a shard.
    pub fn count_keys(&self, shard_id: usize) -> usize {
        self.rocks()
            .iter_cf(shard_id)
            .expect("Failed to iterate")
            .count()
    }

    /// Get the total count of keys across all shards.
    pub fn total_key_count(&self) -> usize {
        (0..self.num_shards)
            .map(|shard_id| self.count_keys(shard_id))
            .sum()
    }
}

impl Default for CrashTestHarness {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// File Manipulation Helpers
// ============================================================================

/// Truncate a file to a specific length.
pub fn truncate_file(path: &Path, new_len: u64) -> std::io::Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(new_len)?;
    Ok(())
}

/// Corrupt a file by writing garbage bytes at a specific offset.
pub fn corrupt_file(path: &Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)?;
    Ok(())
}

/// Append garbage bytes to a file.
pub fn append_garbage(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

/// Get the size of a file.
pub fn file_size(path: &Path) -> std::io::Result<u64> {
    Ok(std::fs::metadata(path)?.len())
}

/// Find all files with a specific extension in a directory.
pub fn find_files_with_extension(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut results = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|e| e == ext) {
                results.push(path);
            } else if path.is_dir() {
                results.extend(find_files_with_extension(&path, ext));
            }
        }
    }
    results
}

/// Find all WAL files in a RocksDB directory.
pub fn find_wal_files(db_path: &Path) -> Vec<PathBuf> {
    find_files_with_extension(db_path, "log")
}

/// Find all SST files in a RocksDB directory.
pub fn find_sst_files(db_path: &Path) -> Vec<PathBuf> {
    find_files_with_extension(db_path, "sst")
}

// ============================================================================
// Snapshot Manipulation Helpers
// ============================================================================

/// Create a snapshot directory structure for testing.
pub fn create_test_snapshot_dir(base_dir: &Path, epoch: u64) -> PathBuf {
    let snapshot_dir = base_dir.join(format!("snapshot_{:05}", epoch));
    std::fs::create_dir_all(&snapshot_dir).expect("Failed to create snapshot dir");
    snapshot_dir
}

/// Write snapshot metadata to a file.
pub fn write_snapshot_metadata(
    snapshot_dir: &Path,
    epoch: u64,
    sequence_number: u64,
    num_shards: usize,
    complete: bool,
) {
    use super::snapshot::SnapshotMetadataFile;

    let mut metadata = SnapshotMetadataFile::new(epoch, sequence_number, num_shards);
    if complete {
        metadata.mark_complete(0, 0);
    }

    let json = serde_json::to_string_pretty(&metadata).expect("Failed to serialize metadata");
    let metadata_path = snapshot_dir.join("metadata.json");
    std::fs::write(&metadata_path, json).expect("Failed to write metadata");
}

/// Create a "latest" symlink pointing to a snapshot directory.
#[cfg(unix)]
pub fn create_latest_symlink(base_dir: &Path, target_name: &str) {
    let link_path = base_dir.join("latest");
    let _ = std::fs::remove_file(&link_path);
    std::os::unix::fs::symlink(target_name, &link_path).expect("Failed to create symlink");
}

#[cfg(not(unix))]
pub fn create_latest_symlink(base_dir: &Path, target_name: &str) {
    let link_path = base_dir.join("latest");
    std::fs::write(&link_path, target_name).expect("Failed to write latest file");
}

// ============================================================================
// Data Generation Helpers
// ============================================================================

/// Generate test data of various types.
pub struct TestDataGenerator;

impl TestDataGenerator {
    /// Generate a string value.
    pub fn string(content: impl Into<Bytes>) -> Value {
        Value::string(content)
    }

    /// Generate a large string value.
    pub fn large_string(size: usize) -> Value {
        Value::string("x".repeat(size))
    }

    /// Generate a sorted set with N members.
    pub fn sorted_set(count: usize) -> Value {
        use crate::types::SortedSetValue;
        let mut zset = SortedSetValue::new();
        for i in 0..count {
            zset.add(Bytes::from(format!("member_{:05}", i)), i as f64);
        }
        Value::SortedSet(zset)
    }

    /// Generate a hash with N fields.
    pub fn hash(count: usize) -> Value {
        use crate::types::HashValue;
        let mut hash = HashValue::new();
        for i in 0..count {
            hash.set(
                Bytes::from(format!("field_{:05}", i)),
                Bytes::from(format!("value_{:05}", i)),
            );
        }
        Value::Hash(hash)
    }

    /// Generate a list with N elements.
    pub fn list(count: usize) -> Value {
        use crate::types::ListValue;
        let mut list = ListValue::new();
        for i in 0..count {
            list.push_back(Bytes::from(format!("item_{:05}", i)));
        }
        Value::List(list)
    }

    /// Generate a set with N members.
    pub fn set(count: usize) -> Value {
        use crate::types::SetValue;
        let mut set = SetValue::new();
        for i in 0..count {
            set.add(Bytes::from(format!("member_{:05}", i)));
        }
        Value::Set(set)
    }

    /// Generate a stream with N entries.
    pub fn stream(count: usize) -> Value {
        use crate::types::{StreamId, StreamIdSpec, StreamValue};
        let mut stream = StreamValue::new();
        for i in 1..=count {
            stream
                .add(
                    StreamIdSpec::Explicit(StreamId::new(i as u64, 0)),
                    vec![(Bytes::from("field"), Bytes::from(format!("value_{}", i)))],
                )
                .expect("Failed to add stream entry");
        }
        Value::Stream(stream)
    }

    /// Generate a bloom filter with N items.
    pub fn bloom_filter(count: usize) -> Value {
        use crate::bloom::BloomFilterValue;
        let mut bloom = BloomFilterValue::new(count as u64 * 2, 0.01);
        for i in 0..count {
            bloom.add(format!("item_{}", i).as_bytes());
        }
        Value::BloomFilter(bloom)
    }

    /// Generate all data types in a vec with their keys.
    pub fn all_types() -> Vec<(&'static str, Value)> {
        vec![
            ("string_key", Self::string("hello world")),
            ("sorted_set_key", Self::sorted_set(10)),
            ("hash_key", Self::hash(10)),
            ("list_key", Self::list(10)),
            ("set_key", Self::set(10)),
            ("stream_key", Self::stream(10)),
            ("bloom_key", Self::bloom_filter(10)),
        ]
    }
}

// ============================================================================
// Verification Helpers
// ============================================================================

/// Verify that a recovered store matches expected data.
pub fn verify_store_contains(
    stores: &[(HashMapStore, ExpiryIndex)],
    shard_id: usize,
    key: &[u8],
) -> bool {
    if shard_id >= stores.len() {
        return false;
    }
    stores[shard_id].0.get(key).is_some()
}

/// Verify that a string value matches.
pub fn verify_string_value(
    stores: &[(HashMapStore, ExpiryIndex)],
    shard_id: usize,
    key: &[u8],
    expected: &str,
) -> bool {
    if shard_id >= stores.len() {
        return false;
    }
    if let Some(value) = stores[shard_id].0.get(key)
        && let Some(sv) = value.as_string()
    {
        return sv.as_bytes().as_ref() == expected.as_bytes();
    }
    false
}

/// Verify expiry index contains a key.
pub fn verify_expiry_index_contains(
    stores: &[(HashMapStore, ExpiryIndex)],
    shard_id: usize,
    key: &[u8],
) -> bool {
    if shard_id >= stores.len() {
        return false;
    }
    stores[shard_id].1.get(key).is_some()
}

/// Verify sorted set has expected members.
pub fn verify_sorted_set(
    stores: &[(HashMapStore, ExpiryIndex)],
    shard_id: usize,
    key: &[u8],
    expected_len: usize,
) -> bool {
    if shard_id >= stores.len() {
        return false;
    }
    if let Some(value) = stores[shard_id].0.get(key)
        && let Some(zset) = value.as_sorted_set()
    {
        return zset.len() == expected_len;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_harness_basic() {
        let harness = CrashTestHarness::new();
        assert!(harness.rocks.is_some());
        assert_eq!(harness.num_shards, 4);
    }

    #[test]
    fn test_harness_put_and_verify() {
        let harness = CrashTestHarness::new();
        let value = Value::string("test_value");
        harness.put_direct(0, b"test_key", &value);

        assert!(harness.verify_string_key(0, b"test_key", "test_value"));
        assert!(!harness.verify_string_key(0, b"test_key", "wrong_value"));
        assert!(!harness.verify_string_key(0, b"missing_key", "any"));
    }

    #[test]
    fn test_harness_crash_and_recover() {
        let mut harness = CrashTestHarness::new();

        // Write data
        harness.put_direct(0, b"key", &Value::string("value"));
        harness.flush();

        // Simulate crash
        harness.crash();
        assert!(harness.rocks.is_none());

        // Recover
        let (stores, stats) = harness.recover();
        assert_eq!(stats.keys_loaded, 1);
        assert!(verify_string_value(&stores, 0, b"key", "value"));
    }

    #[test]
    fn test_data_generator() {
        let zset = TestDataGenerator::sorted_set(5);
        assert!(zset.as_sorted_set().is_some());
        assert_eq!(zset.as_sorted_set().unwrap().len(), 5);

        let hash = TestDataGenerator::hash(3);
        assert!(hash.as_hash().is_some());
        assert_eq!(hash.as_hash().unwrap().len(), 3);
    }
}
