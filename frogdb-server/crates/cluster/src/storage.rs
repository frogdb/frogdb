//! Raft log storage backend using RocksDB.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, RaftLogReader, StorageError, Vote};
use parking_lot::{Mutex, RwLock};
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
/// Key for the latest cluster state-machine snapshot's openraft metadata.
const KEY_SNAPSHOT_META: &[u8] = b"state_machine_snapshot_meta";
/// Key for the latest cluster state-machine snapshot's payload.
///
/// Stored raw rather than inside the metadata record: `serde_json` renders a
/// `Vec<u8>` as a decimal integer array, which is roughly 4x the bytes.
const KEY_SNAPSHOT_DATA: &[u8] = b"state_machine_snapshot_data";

/// A cluster state-machine snapshot as it is stored on disk: openraft's
/// snapshot metadata plus the serialized `ClusterStateInner` it describes.
///
/// The two travel together because restoring either without the other is
/// meaningless — the data says *what* the state machine contains, the meta says
/// *how far into the log* that content is caught up, which is what openraft
/// reads back from `applied_state()` to decide which entries still need
/// applying.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredClusterSnapshot {
    /// openraft's snapshot metadata (last log id, membership, snapshot id).
    pub meta: openraft::SnapshotMeta<NodeId, openraft::BasicNode>,
    /// Serialized `ClusterStateInner`.
    pub data: Vec<u8>,
}

/// Durable home for the cluster state-machine snapshot.
///
/// The Raft **log** was always durable; the state machine built from it was
/// not, so a log purge (openraft purges entries covered by a snapshot it
/// believes exists) followed by a restart lost the purged prefix — nodes, slot
/// assignments and `config_epoch` — and the node had to re-derive everything
/// from a live leader. This store closes that gap by writing every snapshot the
/// state machine builds or installs to the same RocksDB instance as the log, so
/// a restart resumes from the snapshot and replays only the entries after it.
///
/// Obtained from [`ClusterStorage::snapshot_store`]; it shares the underlying
/// database handle, so it is cheap to clone and always consistent with the log
/// it accompanies.
#[derive(Clone)]
pub struct ClusterSnapshotStore {
    db: Arc<DB>,
    /// Serializes [`Self::save`]'s read-compare-write.
    ///
    /// openraft can call it from two places at once: `InstallFullSnapshot` runs
    /// inline in the state-machine worker while a `BuildSnapshot` task runs on
    /// its own thread. Without the lock two writers can both read the same
    /// "older" record and both write, so the loser's ordering check proves
    /// nothing.
    save_lock: Arc<Mutex<()>>,
}

impl ClusterSnapshotStore {
    fn cf_meta(&self) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        self.db.cf_handle(CF_META).expect("meta CF must exist")
    }

    fn io_error(
        verb: openraft::ErrorVerb,
        e: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> StorageError<NodeId> {
        StorageError::from_io_error(
            openraft::ErrorSubject::Store,
            verb,
            std::io::Error::other(e.into()),
        )
    }

    /// Persist a snapshot, unless a **newer** one is already stored.
    ///
    /// Two properties this has to hold, both of them the reason the store
    /// exists at all:
    ///
    /// *Durable.* The write is `sync`, so it is on the platter before this
    /// returns. A plain `flush()` would not do: it flushes the *default* column
    /// family, leaving this record in the `meta` memtable and an unsynced WAL —
    /// and a snapshot that evaporates in a crash is worse than no snapshot,
    /// because openraft purged log entries on the strength of it.
    ///
    /// *Monotonic.* openraft may build a snapshot concurrently with installing
    /// one from the leader, and the builder started from an older applied
    /// index. Letting the late-finishing builder win would move the stored
    /// snapshot *backwards*, past log entries purge already deleted — a restart
    /// would then ask for entries that no longer exist. A save that is not
    /// strictly newer than what is stored is a no-op.
    #[allow(clippy::result_large_err)]
    pub fn save(&self, snapshot: &StoredClusterSnapshot) -> Result<(), StorageError<NodeId>> {
        let _guard = self.save_lock.lock();

        let incoming = snapshot.meta.last_log_id.map(|log_id| log_id.index);
        if let Some(stored) = self.load_meta()? {
            let stored_index = stored.last_log_id.map(|log_id| log_id.index);
            if incoming <= stored_index {
                tracing::debug!(
                    ?incoming,
                    ?stored_index,
                    "Ignoring cluster snapshot that does not advance the stored one"
                );
                return Ok(());
            }
        }

        let encoded_meta = serde_json::to_vec(&snapshot.meta).map_err(|e| {
            Self::io_error(
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        let cf = self.cf_meta();
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(&cf, KEY_SNAPSHOT_META, encoded_meta);
        batch.put_cf(&cf, KEY_SNAPSHOT_DATA, &snapshot.data);

        let mut write_opts = rocksdb::WriteOptions::default();
        write_opts.set_sync(true);

        self.db
            .write_opt(batch, &write_opts)
            .map_err(|e| Self::io_error(openraft::ErrorVerb::Write, e))?;

        Ok(())
    }

    /// Load the persisted snapshot, or `None` when none was ever written.
    #[allow(clippy::result_large_err)]
    pub fn load(&self) -> Result<Option<StoredClusterSnapshot>, StorageError<NodeId>> {
        let Some(meta) = self.load_meta()? else {
            return Ok(None);
        };

        let cf = self.cf_meta();
        let Some(data) = self
            .db
            .get_cf(&cf, KEY_SNAPSHOT_DATA)
            .map_err(|e| Self::io_error(openraft::ErrorVerb::Read, e))?
        else {
            // Unreachable through `save` (both keys go in one atomic batch);
            // treated as "no snapshot" rather than trusted, because restoring
            // metadata without its payload would claim an applied index the
            // state machine does not actually hold.
            tracing::warn!("Cluster snapshot metadata present without payload; ignoring both");
            return Ok(None);
        };

        Ok(Some(StoredClusterSnapshot { meta, data }))
    }

    /// Read just the stored snapshot's metadata.
    #[allow(clippy::result_large_err)]
    fn load_meta(
        &self,
    ) -> Result<Option<openraft::SnapshotMeta<NodeId, openraft::BasicNode>>, StorageError<NodeId>>
    {
        let cf = self.cf_meta();
        let Some(raw) = self
            .db
            .get_cf(&cf, KEY_SNAPSHOT_META)
            .map_err(|e| Self::io_error(openraft::ErrorVerb::Read, e))?
        else {
            return Ok(None);
        };

        let meta = serde_json::from_slice(&raw).map_err(|e| {
            Self::io_error(
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        Ok(Some(meta))
    }
}

/// RocksDB-backed Raft log storage.
pub struct ClusterStorage {
    db: Arc<DB>,
    /// Cache of recently accessed log entries.
    log_cache: RwLock<BTreeMap<u64, Entry<TypeConfig>>>,
    /// Maximum number of entries to cache.
    cache_size: usize,
    /// Shared by every [`ClusterSnapshotStore`] handed out by
    /// [`Self::snapshot_store`], so concurrent saves serialize against each
    /// other however many handles exist.
    snapshot_save_lock: Arc<Mutex<()>>,
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
            snapshot_save_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Durable snapshot store backed by the same database as the Raft log.
    ///
    /// Sharing the handle is deliberate: a snapshot and the log it truncates
    /// must be crash-consistent with each other, which they only are when both
    /// live in the same RocksDB instance.
    pub fn snapshot_store(&self) -> ClusterSnapshotStore {
        ClusterSnapshotStore {
            db: Arc::clone(&self.db),
            save_lock: Arc::clone(&self.snapshot_save_lock),
        }
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
            // openraft's contract: with an empty log this is the last purged id,
            // not `None`. Reporting `None` after a purge tells it the log is
            // *behind* the state machine and it re-purges to heal the "hole"
            // on every restart.
            last_log_id: last_log_id.or(last_purged),
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
            snapshot_save_lock: Arc::clone(&self.snapshot_save_lock),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.set_meta(KEY_VOTE, vote)?;
        self.db
            .flush()
            .map_err(|e| self.io_error(openraft::ErrorVerb::Write, e))?;
        Ok(())
    }

    /// Persist the committed index.
    ///
    /// Deliberately write-only: `read_committed` is left at its default `None`
    /// so openraft re-derives the commit index from the leader on restart.
    /// [`Self::append`] uses default write options (no per-append fsync), so a
    /// crash can lose a log tail this key already counted as committed. Reading
    /// it back would then hand `StorageHelper::get_initial_state` a commit index
    /// naming entries the node no longer has: it only clamps `committed` *up* to
    /// `last_applied`, never down to `last_log_id`, so it goes on to re-apply
    /// `(last_applied, committed]` and fails with a `read_log_at_index` storage
    /// error when the entries are missing. Re-deriving from the leader costs one
    /// round trip and cannot be wrong. Revisit if `append` ever fsyncs.
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

    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_storage_open_and_close() {
        let dir = tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();
        drop(storage);

        // Should be able to reopen
        let _storage = ClusterStorage::open(dir.path()).unwrap();
    }

    // FM-CLUSTER-017
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

    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_storage_metadata() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();

        // Test get_log_state when empty
        let log_state = storage.get_log_state().await.unwrap();
        assert!(log_state.last_log_id.is_none());
        assert!(log_state.last_purged_log_id.is_none());
    }

    // FM-CLUSTER-017
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

    /// Config epoch round-trips through the persistence layer alone (issue 16).
    ///
    /// `test_cluster_epoch_persists` (tests/cluster_topology.rs) restarts a whole
    /// harness node, which also triggers a fresh Raft election. Historically
    /// `cluster_current_epoch` was reported as `max(config_epoch, raft_term)`,
    /// so the bumped term could mask a `config_epoch` that had been lost; the
    /// fold is gone, but this test still earns its place by isolating the
    /// storage layer from the election entirely: entries carrying
    /// `ClusterCommand::IncrementEpoch` are
    /// persisted directly to the RocksDB-backed log (the same column family
    /// and encoding `append` uses), the store is closed and reopened
    /// (simulating a process restart with no election involved), and the
    /// recovered entries are replayed into a brand-new state machine.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_config_epoch_round_trips_through_storage_restart() {
        use crate::state::ClusterStateMachine;
        use crate::types::ClusterCommand;
        use openraft::EntryPayload;
        use openraft::storage::RaftStateMachine;

        let dir = tempdir().unwrap();
        let leader_id = openraft::CommittedLeaderId::new(1, 1);

        // Persist 3 epoch-incrementing commands, then drop the storage
        // handle -- simulating the process shutting down.
        {
            let storage = ClusterStorage::open(dir.path()).unwrap();
            let cf = storage.cf_logs();
            let mut batch = rocksdb::WriteBatch::default();
            for index in 1..=3u64 {
                let entry: Entry<TypeConfig> = Entry {
                    log_id: LogId::new(leader_id, index),
                    payload: EntryPayload::Normal(ClusterCommand::IncrementEpoch),
                };
                let key = ClusterStorage::encode_log_key(index);
                let value = serde_json::to_vec(&entry).unwrap();
                batch.put_cf(&cf, key, value);
            }
            storage
                .db
                .write(batch)
                .expect("writing persisted log entries must succeed");
        }

        // Reopen (simulated restart) and read the log back via the public
        // `RaftLogReader` interface -- no Raft instance, no election, no term.
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        let recovered = storage.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(
            recovered.len(),
            3,
            "all persisted entries must survive the storage reopen"
        );

        // Replay the recovered entries into a fresh state machine: this is
        // exactly what a real restart does to rebuild `config_epoch`.
        let mut state_machine = ClusterStateMachine::new();
        state_machine
            .apply(recovered)
            .await
            .expect("replaying persisted entries must succeed");

        assert_eq!(
            state_machine.state().config_epoch(),
            3,
            "config_epoch must round-trip through storage persistence unchanged"
        );
    }

    /// State-machine snapshots survive a restart *without* the log (issue 16).
    ///
    /// The sibling test above recovers by replaying the log. That only works
    /// while the log still holds the entries: openraft purges entries it
    /// believes a snapshot covers, and before `ClusterSnapshotStore` the
    /// snapshot existed only in memory, so a purge plus a restart lost the
    /// purged prefix outright. This test reproduces exactly that shape -- a
    /// snapshot is built, the process "restarts", and no log entry is ever
    /// replayed -- and asserts the state comes back anyway.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_state_machine_snapshot_survives_restart_without_log_replay() {
        use crate::state::ClusterStateMachine;
        use crate::types::ClusterCommand;
        use openraft::EntryPayload;
        use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

        let dir = tempdir().unwrap();
        let leader_id = openraft::CommittedLeaderId::new(1, 1);
        let entries: Vec<Entry<TypeConfig>> = (1..=3u64)
            .map(|index| Entry {
                log_id: LogId::new(leader_id, index),
                payload: EntryPayload::Normal(ClusterCommand::IncrementEpoch),
            })
            .collect();

        {
            let mut storage = ClusterStorage::open(dir.path()).unwrap();
            let mut state_machine = ClusterStateMachine::new();
            state_machine
                .attach_snapshot_store(storage.snapshot_store())
                .expect("attaching an empty snapshot store must succeed");

            let cf = storage.cf_logs();
            let mut batch = rocksdb::WriteBatch::default();
            for entry in &entries {
                let key = ClusterStorage::encode_log_key(entry.log_id.index);
                batch.put_cf(&cf, key, serde_json::to_vec(entry).unwrap());
            }
            storage
                .db
                .write(batch)
                .expect("writing the log entries must succeed");
            drop(cf);

            state_machine.apply(entries).await.unwrap();
            state_machine
                .build_snapshot()
                .await
                .expect("building a snapshot must succeed");

            // The hazard this store exists for: openraft purges entries a
            // snapshot covers. After this the log holds nothing.
            storage
                .purge(LogId::new(leader_id, 3))
                .await
                .expect("purging the snapshotted prefix must succeed");
        }

        // Restart: a brand-new state machine over the same directory, with no
        // log entries left to replay into it.
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        assert!(
            storage.try_get_log_entries(1..=3).await.unwrap().is_empty(),
            "the purge must have survived the restart -- otherwise this test \
             would pass by replaying the log, which is the sibling test"
        );

        let mut state_machine = ClusterStateMachine::new();
        state_machine
            .attach_snapshot_store(storage.snapshot_store())
            .expect("attaching a populated snapshot store must succeed");

        assert_eq!(
            state_machine.state().config_epoch(),
            3,
            "config_epoch must be restored from the persisted snapshot alone"
        );

        let (last_applied, _membership) = state_machine.applied_state().await.unwrap();
        assert_eq!(
            last_applied.map(|log_id| log_id.index),
            Some(3),
            "the restored applied index tells openraft which entries still need replay"
        );

        let current = state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("the persisted snapshot must be visible to openraft after restart");
        assert_eq!(
            current.meta.last_log_id.map(|log_id| log_id.index),
            Some(3),
            "the durable snapshot, not a synthesized one, is what gets advertised"
        );

        // The real restart path: openraft assembles its initial state from the
        // log store and the state machine together. Before the snapshot store
        // this returned `last_applied: None` over a purged log -- state lost.
        let initial = openraft::storage::StorageHelper::new(&mut storage, &mut state_machine)
            .get_initial_state()
            .await
            .expect("openraft must be able to build initial state from a purged log + snapshot");
        assert_eq!(
            initial.committed.map(|log_id| log_id.index),
            Some(3),
            "openraft's restart path must see the snapshot's applied index \
             (it clamps the unread committed index up to last_applied)"
        );
        assert_eq!(
            initial.snapshot_meta.last_log_id.map(|log_id| log_id.index),
            Some(3),
            "and must adopt the persisted snapshot rather than rebuilding an empty one"
        );
    }

    /// A snapshot save that does not advance the stored one is ignored.
    ///
    /// openraft can have a `BuildSnapshot` task in flight while it installs a
    /// newer snapshot from the leader. The builder started earlier and finishes
    /// later, so last-writer-wins would move the durable snapshot *backwards*,
    /// behind log entries purge already deleted.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_snapshot_save_never_moves_backwards() {
        let dir = tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();
        let store = storage.snapshot_store();

        let stored_at = |index: u64, payload: &[u8]| StoredClusterSnapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), index)),
                last_membership: Default::default(),
                snapshot_id: format!("snapshot-{index}"),
            },
            data: payload.to_vec(),
        };

        store.save(&stored_at(10_000, b"installed")).unwrap();
        store.save(&stored_at(5_000, b"stale-builder")).unwrap();

        let loaded = store.load().unwrap().expect("a snapshot must be stored");
        assert_eq!(
            loaded.meta.last_log_id.map(|log_id| log_id.index),
            Some(10_000),
            "a stale builder must not overwrite an installed snapshot"
        );
        assert_eq!(
            loaded.data, b"installed",
            "the payload must match the metadata that survived"
        );

        // Equal index is a no-op too: nothing new to record, and re-writing
        // would let a same-index writer swap the payload.
        store.save(&stored_at(10_000, b"same-index")).unwrap();
        assert_eq!(store.load().unwrap().unwrap().data, b"installed");

        // Strictly newer wins.
        store.save(&stored_at(10_001, b"newer")).unwrap();
        assert_eq!(store.load().unwrap().unwrap().data, b"newer");
    }

    /// Without a store attached the state machine keeps its old purely
    /// in-memory behaviour: nothing is written, and `get_current_snapshot`
    /// synthesizes from live state.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn test_snapshot_store_is_opt_in() {
        use crate::state::ClusterStateMachine;
        use openraft::storage::RaftStateMachine;

        let dir = tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();

        let mut detached = ClusterStateMachine::new();
        assert!(
            detached.get_current_snapshot().await.unwrap().is_none(),
            "a state machine that has applied nothing has no snapshot to offer"
        );

        let mut attached = ClusterStateMachine::new();
        attached
            .attach_snapshot_store(storage.snapshot_store())
            .unwrap();
        assert!(
            attached.get_current_snapshot().await.unwrap().is_none(),
            "an attached store with nothing persisted must not synthesize a snapshot"
        );
    }

    // ---- Log key codec, cache, and the RaftLogStorage surface --------------

    /// Build a log entry at `index` under term 1.
    fn entry_at(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: openraft::EntryPayload::Normal(crate::types::ClusterCommand::IncrementEpoch),
        }
    }

    /// The key codec is a round trip, and the encoding is big-endian so RocksDB's
    /// byte order *is* index order — every range scan in this file (log reads,
    /// truncate, purge) depends on both halves.
    // FM-CLUSTER-017
    #[test]
    fn log_keys_round_trip_and_sort_in_index_order() {
        for index in [0u64, 1, 2, 42, 16_384, u64::MAX] {
            let key = ClusterStorage::encode_log_key(index);
            assert_eq!(
                ClusterStorage::decode_log_key(&key),
                index,
                "decode must invert encode for index {index}"
            );
        }

        assert!(
            ClusterStorage::encode_log_key(2) < ClusterStorage::encode_log_key(10),
            "byte order must match index order, or forward scans skip entries"
        );
    }

    /// `save_committed(None)` has to *delete* the key, not leave the previous
    /// value behind: a stale committed index outlives the log it names.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn save_committed_writes_then_deletes_the_persisted_key() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 5);

        storage.save_committed(Some(log_id)).await.unwrap();
        assert_eq!(
            storage
                .get_meta::<LogId<NodeId>>(KEY_COMMITTED)
                .unwrap()
                .expect("the committed index must be on disk"),
            log_id
        );

        storage.save_committed(None).await.unwrap();
        assert_eq!(
            storage.get_meta::<LogId<NodeId>>(KEY_COMMITTED).unwrap(),
            None,
            "clearing the committed index must remove the key, not keep the old one"
        );
    }

    /// The log cache holds up to `cache_size` entries and evicts the *oldest*
    /// index once it is over that bound — never at it, or the cache would run
    /// one entry short of its configured size forever.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn cache_evicts_the_oldest_only_once_over_the_bound() {
        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        storage.cache_size = 2;

        storage.cache_entry(entry_at(1));
        storage.cache_entry(entry_at(2));
        assert_eq!(
            storage.log_cache.read().len(),
            2,
            "a full cache is not over"
        );
        assert!(
            storage.get_cached(1).is_some(),
            "entry 1 must survive while the cache is merely full"
        );
        assert_eq!(storage.get_cached(2).unwrap().log_id.index, 2);

        storage.cache_entry(entry_at(3));
        assert_eq!(storage.log_cache.read().len(), 2);
        assert!(
            storage.get_cached(1).is_none(),
            "the oldest index is the one evicted"
        );
        assert!(storage.get_cached(2).is_some());
        assert!(storage.get_cached(3).is_some());
        assert!(storage.get_cached(4).is_none(), "never cached, never found");
    }

    /// Cache invalidation is bounded on *both* ends: `[start, end]` inclusive,
    /// with `None` meaning "to the tail". Widening it drops live entries;
    /// narrowing it leaves stale ones that a later read would serve in place of
    /// what is on disk.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn cache_invalidation_respects_both_range_ends() {
        let dir = tempdir().unwrap();
        let storage = ClusterStorage::open(dir.path()).unwrap();
        for index in 1..=5 {
            storage.cache_entry(entry_at(index));
        }

        storage.invalidate_cache_range(2, Some(4));
        assert!(storage.get_cached(1).is_some(), "below start, kept");
        assert!(storage.get_cached(2).is_none(), "start is inclusive");
        assert!(storage.get_cached(3).is_none());
        assert!(storage.get_cached(4).is_none(), "end is inclusive");
        assert!(storage.get_cached(5).is_some(), "above end, kept");

        storage.invalidate_cache_range(5, None);
        assert!(
            storage.get_cached(5).is_none(),
            "an open end reaches the tail"
        );
        assert!(storage.get_cached(1).is_some(), "still below start, kept");
    }

    /// An excluded start bound means "after this index" — off by one here and a
    /// follower re-reads an entry the leader already acknowledged.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn excluded_start_bound_skips_that_index() {
        use openraft::storage::RaftLogStorageExt;
        use std::ops::Bound;

        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        storage
            .blocking_append((1..=4).map(entry_at).collect::<Vec<_>>())
            .await
            .unwrap();

        let indexes = |entries: Vec<Entry<TypeConfig>>| {
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>()
        };

        let excluded = storage
            .try_get_log_entries((Bound::Excluded(2), Bound::Included(4)))
            .await
            .unwrap();
        assert_eq!(
            indexes(excluded),
            vec![3, 4],
            "an excluded start begins at start + 1"
        );

        let included = storage.try_get_log_entries(2..=4).await.unwrap();
        assert_eq!(indexes(included), vec![2, 3, 4]);
    }

    /// Appended entries reach RocksDB and `get_log_state` reports the real tail:
    /// a state that always looks empty makes openraft replay the whole log.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn append_persists_entries_and_log_state_reports_the_tail() {
        use openraft::storage::RaftLogStorageExt;

        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        storage
            .blocking_append((1..=3).map(entry_at).collect::<Vec<_>>())
            .await
            .unwrap();

        let state = storage.get_log_state().await.unwrap();
        assert_eq!(
            state.last_log_id.map(|id| id.index),
            Some(3),
            "the log state must name the last appended entry"
        );
        assert!(state.last_purged_log_id.is_none());

        // Reopen: the entries are on disk, not merely in the cache.
        drop(storage);
        let mut reopened = ClusterStorage::open(dir.path()).unwrap();
        let recovered = reopened.try_get_log_entries(1..=3).await.unwrap();
        assert_eq!(recovered.len(), 3);
        assert_eq!(recovered[2].log_id.index, 3);
    }

    /// Truncation drops everything *after* the kept index — in RocksDB and in
    /// the cache alike, and starting one past the kept index in both. A cache
    /// that keeps a truncated entry would serve a conflicting entry the leader
    /// has already overwritten.
    // FM-CLUSTER-017
    #[tokio::test]
    async fn truncate_drops_only_the_tail_after_the_kept_index() {
        use openraft::storage::RaftLogStorageExt;

        let dir = tempdir().unwrap();
        let mut storage = ClusterStorage::open(dir.path()).unwrap();
        storage
            .blocking_append((1..=5).map(entry_at).collect::<Vec<_>>())
            .await
            .unwrap();
        assert!(
            storage.get_cached(3).is_some(),
            "append populates the cache"
        );

        storage
            .truncate(LogId::new(openraft::CommittedLeaderId::new(1, 1), 3))
            .await
            .unwrap();

        // Inspect the cache *before* any read, which would refill it from disk.
        assert!(
            storage.get_cached(3).is_some(),
            "invalidation starts one past the kept index"
        );
        assert!(
            storage.get_cached(4).is_none(),
            "the truncated tail must leave the cache too"
        );
        assert!(storage.get_cached(5).is_none());

        let remaining = storage.try_get_log_entries(..).await.unwrap();
        assert_eq!(
            remaining.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the kept index and everything below it survive"
        );
    }
}
