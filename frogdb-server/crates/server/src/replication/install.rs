//! Installing a received full-resync checkpoint into the **live** keyspace.
//!
//! The replication crate stages a full-sync checkpoint on disk but owns no
//! store, so it delegates the install through the injected
//! [`CheckpointInstaller`] seam. This module is the server-side implementation:
//! it reads the staged RocksDB and replaces each shard's live keyspace with the
//! snapshot it holds, before the replica adopts the snapshot's offset and
//! resumes streaming.
//!
//! Without it a runtime `REPLICAOF <new-master>` left the demoted node serving
//! its own (possibly forked) keyspace until the next reboot — issue 61.
//!
//! **Why not swap the RocksDB handle.** The live keyspace is the per-shard
//! in-memory store owned by each shard task; RocksDB is the durability tier
//! behind it. Swapping the handle would leave every shard's in-memory map
//! untouched, which is precisely the bug. So the snapshot is read out of the
//! staged DB and pushed into the shards, which clear + restore and then persist
//! the restored keys through their own WAL — the same direction of travel as
//! boot recovery, just at runtime.
//!
//! **The staged dir survives the install** (the boot-time installer's contract
//! is untouched): a crash mid-install leaves the node re-installing the same
//! snapshot on the next boot, which is idempotent.

use std::io;
use std::path::{Path, PathBuf};

use crate::config::Config;
use bytes::Bytes;
use frogdb_core::persistence::{RocksConfig, RocksStore, deserialize, recover_shard_into};
use frogdb_core::sync::Arc;
use frogdb_core::{
    KeyMetadata, KeyType, ReplicationMsg, ShardSender, SnapshotEntry, Value,
    persistence::RestoreSink,
};
use frogdb_replication::replica::CheckpointInstaller;
use std::collections::HashSet;
use std::time::Instant;
use tokio::sync::oneshot;

/// Reads a staged full-resync checkpoint and installs it into the live shards.
pub struct LiveCheckpointInstaller {
    shard_senders: Arc<Vec<ShardSender>>,
    rocks_config: RocksConfig,
    warm_enabled: bool,
}

impl LiveCheckpointInstaller {
    pub fn new(
        shard_senders: Arc<Vec<ShardSender>>,
        rocks_config: RocksConfig,
        warm_enabled: bool,
    ) -> Self {
        Self {
            shard_senders,
            rocks_config,
            warm_enabled,
        }
    }

    /// The installer this node's config implies, erased into the seam the
    /// replica handler holds. Single construction site for both wiring points
    /// (boot-configured replica and runtime `REPLICAOF` demotion) so the two can
    /// not drift apart.
    pub fn for_config(
        config: &Config,
        shard_senders: Arc<Vec<ShardSender>>,
    ) -> CheckpointInstaller {
        Self::new(
            shard_senders,
            RocksConfig::from_persistence(&config.persistence),
            config.tiered_storage.enabled,
        )
        .into_installer()
    }

    /// Erase this into the injectable seam the replica handler holds.
    pub fn into_installer(self) -> CheckpointInstaller {
        let installer = Arc::new(self);
        Arc::new(move |staged_dir: PathBuf| {
            let installer = installer.clone();
            Box::pin(async move { installer.install(&staged_dir).await })
        })
    }

    /// Read `staged_dir` and replace every shard's live keyspace with it.
    ///
    /// **Consistency window: per shard, not across shards.** Each shard applies
    /// its clear + restore atomically inside its own task (see
    /// `ShardWorker::install_snapshot`), but the shards are walked in order, so
    /// a client reading during the install can see an already-installed shard
    /// and a not-yet-installed one in the same moment. That is the granularity
    /// every other cross-shard write already has, and it is chosen over
    /// quiescing all clients for the duration of a whole-dataset load: reads
    /// keep being served, from the pre-install snapshot, until their own shard
    /// swaps. The install completes before the replica adopts the snapshot's
    /// offset, so no *replicated* write can observe the half-installed state.
    async fn install(&self, staged_dir: &Path) -> io::Result<()> {
        let start = Instant::now();
        let num_shards = self.shard_senders.len();
        let dir = staged_dir.to_path_buf();
        let rocks_config = self.rocks_config.clone();
        let warm_enabled = self.warm_enabled;

        // RocksDB open + full scan is blocking work; keep it off the runtime.
        let per_shard = tokio::task::spawn_blocking(move || {
            read_snapshot(&dir, num_shards, &rocks_config, warm_enabled)
        })
        .await
        .map_err(|e| io::Error::other(format!("snapshot read task failed: {e}")))??;

        let total: usize = per_shard.iter().map(Vec::len).sum();
        for (shard_id, entries) in per_shard.into_iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            self.shard_senders[shard_id]
                .send(ReplicationMsg::InstallSnapshot {
                    entries,
                    response_tx,
                })
                .await
                .map_err(|_| io::Error::other(format!("shard {shard_id} is gone")))?;
            response_rx.await.map_err(|_| {
                io::Error::other(format!("shard {shard_id} dropped the install ack"))
            })?;
        }

        tracing::info!(
            keys = total,
            shards = num_shards,
            duration_ms = start.elapsed().as_millis() as u64,
            "Installed full-resync checkpoint into the live keyspace"
        );
        Ok(())
    }
}

/// Read every shard's entries out of the staged checkpoint DB.
///
/// Blocking: opens the staged RocksDB and scans it. The staged directory is a
/// complete database (the writer's commit point), so it is opened directly
/// rather than being renamed onto the live DB — the live DB converges through
/// the WAL writes each shard performs as it restores.
fn read_snapshot(
    staged_dir: &Path,
    num_shards: usize,
    rocks_config: &RocksConfig,
    warm_enabled: bool,
) -> io::Result<Vec<Vec<SnapshotEntry>>> {
    let rocks = RocksStore::open_with_warm(staged_dir, num_shards, rocks_config, warm_enabled)
        .map_err(|e| io::Error::other(format!("failed to open staged checkpoint: {e}")))?;

    let mut per_shard = Vec::with_capacity(num_shards);
    for shard_id in 0..num_shards {
        let mut sink = SnapshotSink::default();
        recover_shard_into(&rocks, shard_id, &mut sink)
            .map_err(|e| io::Error::other(format!("failed to read shard {shard_id}: {e}")))?;
        if warm_enabled {
            sink.absorb_warm(&rocks, shard_id)?;
        }
        per_shard.push(sink.entries);
    }
    Ok(per_shard)
}

/// Collects a shard's snapshot entries in iteration order.
#[derive(Default)]
struct SnapshotSink {
    entries: Vec<SnapshotEntry>,
    keys: HashSet<Bytes>,
}

impl SnapshotSink {
    /// Fold the shard's warm-tier keys in as ordinary (hot) entries.
    ///
    /// A warm entry's value lives in the *staged* database, which is thrown away
    /// after the install, so it cannot stay warm on the receiving side — it is
    /// materialized instead, exactly like any other key of the snapshot. The
    /// receiving node re-demotes it under its own tiering policy. Hot wins over
    /// warm for the same key, matching boot recovery.
    ///
    /// Unlike boot recovery this never prunes stale warm entries from the source
    /// DB: the staged checkpoint is read-only input, not this node's database.
    fn absorb_warm(&mut self, rocks: &RocksStore, shard_id: usize) -> io::Result<()> {
        let now = Instant::now();
        let iter = rocks
            .iter_warm_cf(shard_id)
            .map_err(|e| io::Error::other(format!("failed to read warm shard {shard_id}: {e}")))?;
        for (key, value) in iter {
            let Ok((val, metadata)) = deserialize(&value) else {
                tracing::warn!(
                    shard_id,
                    key = ?String::from_utf8_lossy(&key),
                    "Skipping undeserializable warm key in received checkpoint"
                );
                continue;
            };
            if metadata.expires_at.is_some_and(|at| at <= now) {
                continue;
            }
            let key = Bytes::copy_from_slice(&key);
            if self.contains(&key) {
                continue;
            }
            self.restore_entry(key, val, metadata);
        }
        Ok(())
    }
}

impl RestoreSink for SnapshotSink {
    fn restore_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata) {
        self.keys.insert(key.clone());
        self.entries.push(SnapshotEntry {
            key,
            value,
            metadata,
        });
    }

    /// Never called: warm entries are materialized as hot by
    /// [`SnapshotSink::absorb_warm`], which reads the value the warm recovery
    /// protocol does not hand to the sink.
    fn restore_warm_entry(&mut self, _key: Bytes, _metadata: KeyMetadata, _key_type: KeyType) {}

    fn contains(&self, key: &[u8]) -> bool {
        self.keys.contains(key)
    }
}
