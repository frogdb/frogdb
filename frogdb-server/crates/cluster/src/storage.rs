//! Raft log storage backend using RocksDB.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, Vote};
use parking_lot::RwLock;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use serde::{Deserialize, Serialize};

use crate::types::{NodeId, TypeConfig};

/// Column family names for RocksDB.
const CF_LOGS: &str = "raft_logs";
const CF_META: &str = "raft_meta";

/// Key for storing the vote in metadata.
const KEY_VOTE: &[u8] = b"vote";
/// Key for storing the committed index.
const KEY_COMMITTED: &[u8] = b"committed";
/// Key for storing the last purged log ID.
const KEY_LAST_PURGED: &[u8] = b"last_purged";

/// RocksDB-backed Raft log storage.
pub struct ClusterStorage {
    db: Arc<DB>,
    /// Cache of recently accessed log entries.
    log_cache: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    /// Maximum number of entries to cache.
    cache_size: usize,
}

impl ClusterStorage {
    /// Open or create cluster storage at the given path.
    #[allow(clippy::result_large_err)]
    pub fn open(path: &Path) -> Result<Self, StorageError<NodeId>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(10);
        opts.set_keep_log_file_num(5);

        let cf_opts = Options::default();
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_LOGS, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_META, cf_opts),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::other(e),
            )
        })?;

        tracing::info!(path = %path.display(), "Opened cluster storage");

        Ok(Self {
            db: Arc::new(db),
            log_cache: RwLock::new(BTreeMap::new()),
            cache_size: 1000,
        })
    }

    /// Get the logs column family handle.
    fn cf_logs(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_LOGS).expect("logs CF must exist")
    }

    /// Get the metadata column family handle.
    fn cf_meta(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_META).expect("meta CF must exist")
    }

    /// Encode a log index as a key.
    fn encode_log_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Decode a log index from a key.
    fn decode_log_key(key: &[u8]) -> u64 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(key);
        u64::from_be_bytes(buf)
    }

    /// Get metadata value.
    #[allow(clippy::result_large_err)]
    fn get_meta<T: for<'a> Deserialize<'a>>(
        &self,
        key: &[u8],
    ) -> Result<Option<T>, StorageError<NodeId>> {
        let cf = self.cf_meta();
        let Some(data) = self
            .db
            .get_cf(&cf, key)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };

        let value = serde_json::from_slice(&data).map_err(|e| {
            self.io_error(
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        Ok(Some(value))
    }

    /// Set metadata value.
    #[allow(clippy::result_large_err)]
    fn set_meta<T: Serialize>(&self, key: &[u8], value: &T) -> Result<(), StorageError<NodeId>> {
        let data = serde_json::to_vec(value).map_err(|e| {
            self.io_error(
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        let cf = self.cf_meta();
        self.db
            .put_cf(&cf, key, data)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;

        Ok(())
    }

    /// Delete metadata value.
    #[allow(clippy::result_large_err)]
    fn delete_meta(&self, key: &[u8]) -> Result<(), StorageError<NodeId>> {
        let cf = self.cf_meta();
        self.db
            .delete_cf(&cf, key)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;
        Ok(())
    }

    /// Helper to create a storage IO error.
    fn io_error(
        &self,
        verb: openraft::ErrorVerb,
        e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> StorageError<NodeId> {
        StorageError::from_io_error(
            openraft::ErrorSubject::Store,
            verb,
            std::io::Error::other(e.into()),
        )
    }

    /// Add entry to cache, evicting old entries if necessary.
    fn cache_entry(&self, entry: Entry<TypeConfig>) {
        let mut cache = self.log_cache.write();
        cache.insert(entry.log_id.index, entry);

        // Evict old entries if cache is too large
        while cache.len() > self.cache_size {
            let oldest = *cache.keys().next().unwrap();
            cache.remove(&oldest);
        }
    }

    /// Get entry from cache.
    fn get_cached(&self, index: u64) -> Option<Entry<TypeConfig>> {
        self.log_cache.read().get(&index).cloned()
    }

    /// Invalidate cache entries in range.
    fn invalidate_cache_range(&self, start: u64, end: Option<u64>) {
        let mut cache = self.log_cache.write();
        let keys_to_remove: Vec<_> = cache
            .keys()
            .filter(|&&idx| idx >= start && end.is_none_or(|e| idx <= e))
            .copied()
            .collect();
        for key in keys_to_remove {
            cache.remove(&key);
        }
    }
}

impl RaftLogReader<TypeConfig> for ClusterStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => Some(n),
            std::ops::Bound::Excluded(&n) => n.checked_sub(1),
            std::ops::Bound::Unbounded => None,
        };

        let mut entries = Vec::new();
        let start_key = Self::encode_log_key(start);

        let cf = self.cf_logs();
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, value) = item.map_err(|e| self.io_error(openraft::ErrorVerb::Read, e))?;

            let index = Self::decode_log_key(&key);

            if let Some(end_index) = end
                && index > end_index
            {
                break;
            }

            // Try cache first
            if let Some(entry) = self.get_cached(index) {
                entries.push(entry);
            } else {
                let entry: Entry<TypeConfig> = serde_json::from_slice(&value).map_err(|e| {
                    self.io_error(
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    )
                })?;
                self.cache_entry(entry.clone());
                entries.push(entry);
            }
        }

        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for ClusterStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let last_purged: Option<LogId<NodeId>> = self.get_meta(KEY_LAST_PURGED)?;

        // Find the last log entry
        let last_log_id = {
            let cf = self.cf_logs();
            let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);

            if let Some(result) = iter.next() {
                let (_, value) = result.map_err(|e| self.io_error(openraft::ErrorVerb::Read, e))?;
                let entry: Entry<TypeConfig> = serde_json::from_slice(&value).map_err(|e| {
                    self.io_error(
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    )
                })?;
                Some(entry.log_id)
            } else {
                None
            }
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.get_meta(KEY_VOTE)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // Clone the storage for reading
        // This is safe because RocksDB handles concurrent access
        ClusterStorage {
            db: Arc::clone(&self.db),
            log_cache: RwLock::new(self.log_cache.read().clone()),
            cache_size: self.cache_size,
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.set_meta(KEY_VOTE, vote)?;
        self.db
            .flush()
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        if let Some(committed) = committed {
            self.set_meta(KEY_COMMITTED, &committed)?;
        } else {
            self.delete_meta(KEY_COMMITTED)?;
        }
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut batch = rocksdb::WriteBatch::default();
        let cf = self.cf_logs();

        for entry in entries {
            let key = Self::encode_log_key(entry.log_id.index);
            let value = serde_json::to_vec(&entry).map_err(|e| {
                self.io_error(
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                )
            })?;
            batch.put_cf(&cf, key, value);
            self.cache_entry(entry);
        }

        self.db
            .write(batch)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;

        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete all entries with index > log_id.index
        let start_key = Self::encode_log_key(log_id.index + 1);
        let mut batch = rocksdb::WriteBatch::default();

        let cf = self.cf_logs();
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item.map_err(|e| self.io_error(openraft::ErrorVerb::Read, e))?;
            batch.delete_cf(&cf, &key);
        }

        self.db
            .write(batch)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;

        self.invalidate_cache_range(log_id.index + 1, None);

        tracing::debug!(index = log_id.index, "Truncated log entries after index");

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Delete all entries with index <= log_id.index
        let _end_key = Self::encode_log_key(log_id.index);
        let mut batch = rocksdb::WriteBatch::default();

        let cf = self.cf_logs();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| self.io_error(openraft::ErrorVerb::Read, e))?;

            let index = Self::decode_log_key(&key);
            if index > log_id.index {
                break;
            }

            batch.delete_cf(&cf, &key);
        }

        // Store the last purged log ID
        self.set_meta(KEY_LAST_PURGED, &log_id)?;

        self.db
            .write(batch)
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;

        self.invalidate_cache_range(0, Some(log_id.index));

        tracing::debug!(index = log_id.index, "Purged log entries up to index");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_storage_open_and_close() {
        let dir = tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();
        drop(storage);

        // Should be able to reopen
        let _storage = ClusterStorage::open(dir.path()).unwrap();
    }

    #[tokio::test]
    async fn test_storage_vote() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();

        // Initially no vote
        assert!(storage.read_vote().await.unwrap().is_none());

        // Save a vote - Vote::new(committed_leader_id, voted_for_node_id)
        let vote = Vote::new_committed(1, 42);
        storage.save_vote(&vote).await.unwrap();

        // Should persist
        let loaded = storage.read_vote().await.unwrap().unwrap();
        assert_eq!(loaded.leader_id().voted_for(), Some(42));
    }

    #[tokio::test]
    async fn test_storage_metadata() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();

        // Test get_log_state when empty
        let log_state = storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());
        assert!(log_state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn test_storage_committed() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();

        // Create a committed log_id
        let leader_id = openraft::CommittedLeaderId::new(1, 1);
        let log_id = LogId::new(leader_id, 5);

        // Save committed
        storage.save_committed(Some(log_id)).await.unwrap();

        // Clear committed
        storage.save_committed(None).await.unwrap();
    }
}
