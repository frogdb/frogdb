//! Replication-lifecycle dispatch: moving a full-resync snapshot in and out of
//! the live keyspace.
//!
//! A replica that takes a runtime `REPLICAOF <new-master>` (or boots into a
//! full resync) must adopt the master's dataset *live* — otherwise it keeps
//! serving its own, possibly forked, keyspace and only reconverges on restart
//! (issue 61). The snapshot is read off disk by the replication driver and
//! delivered here, one message per shard.
//!
//! The export direction is the same idea served from the primary's side: a
//! primary running with `persistence.enabled = false` has no RocksDB to
//! checkpoint, so the dataset a full resync owes its replica is serialized out
//! of RAM here instead (issue 67).

use super::message::{ReplicationMsg, SnapshotEntry};
use super::persistence::WalTarget;
use super::worker::ShardWorker;
use crate::shard::helpers::REPLICA_INTERNAL_CONN_ID;
use crate::store::Store;
use bytes::Bytes;

impl ShardWorker {
    /// Dispatch replication-lifecycle messages.
    pub(super) async fn dispatch_replication(&mut self, msg: ReplicationMsg) -> bool {
        match msg {
            ReplicationMsg::InstallSnapshot {
                entries,
                response_tx,
            } => {
                self.install_snapshot(entries).await;
                let _ = response_tx.send(());
            }
            ReplicationMsg::ExportSnapshot { response_tx } => {
                // The watermark is read in the same message as the export, with
                // no `.await` between: every write in this blob was broadcast at
                // or below it, and nothing above it has executed on this shard.
                let last_broadcast_offset = self.last_broadcast_offset();
                let _ = response_tx.send(
                    self.export_snapshot()
                        .map(|blob| (blob, last_broadcast_offset)),
                );
            }
        }
        false
    }

    /// Serialize this shard's live keyspace into one dataset blob.
    ///
    /// **Consistency window.** Like `install_snapshot`, this runs to completion
    /// without an `.await` inside the shard's own task, so the blob is one
    /// instant of *this* shard. Across shards the exports are sequential, which
    /// is the same granularity the checkpoint path has, and it is safe for the
    /// same reason: the offset granted to the replica is captured before the
    /// export starts, so the exported data can only run *ahead* of that offset,
    /// and the `(offset, current]` window is replayed from the backlog at the
    /// streaming handoff.
    ///
    /// **Expired keys are dropped**, not exported: they are already invisible to
    /// clients here, and shipping them would resurrect them on a replica whose
    /// clock disagrees. This matches the checkpoint path, where the flush engine
    /// has already tombstoned them.
    ///
    /// **A warm (spilled) value fails the export.** This path exists precisely
    /// because there is no RocksDB, so a value that is not hot has nowhere to be
    /// read back from; skipping it would hand the replica a subset of the
    /// keyspace while claiming a full sync. The caller turns the error into a
    /// failed sync, which the replica retries.
    fn export_snapshot(&self) -> Result<Vec<u8>, String> {
        let mut blob = Vec::new();
        let mut exported = 0usize;
        for key in self.store.all_keys() {
            let Some(metadata) = self.store.get_metadata(&key) else {
                continue;
            };
            if metadata.is_expired() {
                continue;
            }
            let Some(value) = self.store.get_hot(&key) else {
                return Err(format!(
                    "shard {} cannot export key of {} bytes: its value is not resident in memory",
                    self.shard_id(),
                    key.len()
                ));
            };
            frogdb_persistence::append_entry(&mut blob, &key, &value, &metadata);
            exported += 1;
        }
        tracing::debug!(
            shard_id = self.shard_id(),
            keys = exported,
            bytes = blob.len(),
            "Exported live keyspace for a full resync"
        );
        Ok(blob)
    }

    /// Replace this shard's live keyspace with `entries`.
    ///
    /// **Consistency window.** The clear + restore below runs to completion
    /// without an `.await`, inside the shard's own single-threaded task, so no
    /// other message can interleave: a client either sees the whole old
    /// keyspace or the whole new one *for this shard*. The install is therefore
    /// atomic per shard, not across shards — during a multi-shard install a
    /// client can see shard 0 already swapped while shard 1 has not been. That
    /// is the same granularity every other cross-shard write (FLUSHDB, MSET)
    /// already has, and it is why the alternative — blocking every client for
    /// the duration of a whole-dataset load — was not chosen: reads are served
    /// from the pre-install snapshot until their own shard swaps.
    ///
    /// **Effects.** The clear routes through the canonical write-effect
    /// pipeline as a synthetic `FLUSHDB`, so it inherits exactly the effects
    /// that command declares: WATCH version bump, flush-all client-tracking
    /// invalidation, the dirty counter, and a WAL `ClearShard` range tombstone —
    /// and, because `FLUSHDB` declares `EventSpec::Suppressed`, **no keyspace
    /// notifications**. That is deliberate: a boot-time snapshot load emits
    /// none either, and an install is the same event (adopting a dataset), not
    /// a stream of user writes. `REPLICA_INTERNAL_CONN_ID` suppresses the
    /// replication broadcast for the same reason a replica-applied write does
    /// not re-broadcast.
    ///
    /// The restored keys are then WAL-persisted individually so the live
    /// RocksDB converges on the snapshot too; the WAL sequence orders every
    /// `Put` after the range tombstone, so the flush engine cannot resurrect a
    /// cleared key.
    async fn install_snapshot(&mut self, entries: Vec<SnapshotEntry>) {
        let cleared = self.store.len();
        self.store.clear();
        let mut restored: Vec<Bytes> = Vec::with_capacity(entries.len());
        for entry in entries {
            restored.push(entry.key.clone());
            self.store
                .restore_entry(entry.key, entry.value, entry.metadata);
        }

        // A dirty delta of `-1` is the pipeline's "nothing changed" signal, which
        // suppresses the WATCH version bump. An install that neither cleared nor
        // restored anything really is a no-op; anything else must bump so
        // in-flight WATCHers abort.
        let changed = cleared + restored.len();
        let dirty_delta = if changed > 0 { changed as i64 } else { -1 };
        let handler = self.scatter_write_handler("FLUSHDB");
        self.run_scatter_effects(
            vec![(handler, Vec::new())],
            dirty_delta,
            REPLICA_INTERNAL_CONN_ID,
        )
        .await;

        for key in &restored {
            if let Err(e) = self.write_set(key).await {
                tracing::error!(error = %e, "Failed to persist restored snapshot key to WAL");
            }
        }

        tracing::info!(
            shard_id = self.shard_id(),
            cleared,
            restored = restored.len(),
            "Installed full-resync snapshot into the live keyspace"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopBroadcaster;
    use crate::shard::execution::scatter_effect_tests::scatter_worker;
    use crate::types::{KeyMetadata, Value};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn worker() -> ShardWorker {
        scatter_worker(Arc::new(NoopBroadcaster))
    }

    fn entry(key: &str, value: &str) -> SnapshotEntry {
        let value = Value::string(Bytes::from(value.to_string()));
        let metadata = KeyMetadata::new(value.memory_size());
        SnapshotEntry {
            key: Bytes::from(key.to_string()),
            value,
            metadata,
        }
    }

    /// The install is a replace, not a merge: pre-existing (forked) keys are
    /// gone and only the snapshot's keys remain.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn install_snapshot_replaces_the_live_keyspace() {
        let mut worker = worker();
        worker
            .store
            .set(Bytes::from("forked"), Value::string(Bytes::from("old")));

        worker.install_snapshot(vec![entry("fresh", "new")]).await;

        assert!(!worker.store.contains(b"forked"));
        let fresh = worker
            .store
            .get_hot(b"fresh")
            .expect("snapshot key is live");
        let Value::String(s) = fresh.as_ref() else {
            panic!("expected a string value")
        };
        assert_eq!(s.as_bytes(), Bytes::from("new"));
        assert_eq!(worker.store.len(), 1);
    }

    /// An install bumps the WATCH version so a transaction watching a key the
    /// snapshot replaced aborts instead of committing against stale state.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn install_snapshot_bumps_the_watch_version() {
        let mut worker = worker();
        worker
            .store
            .set(Bytes::from("k"), Value::string(Bytes::from("v")));
        let before = worker.get_key_version(b"k");

        worker.install_snapshot(vec![entry("k", "v2")]).await;

        assert_ne!(worker.get_key_version(b"k"), before);
    }

    /// An empty snapshot still clears: a demoted node whose new master has no
    /// keys must not keep serving its own.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn install_empty_snapshot_clears_the_shard() {
        let mut worker = worker();
        worker
            .store
            .set(Bytes::from("forked"), Value::string(Bytes::from("old")));

        worker.install_snapshot(Vec::new()).await;

        assert_eq!(worker.store.len(), 0);
    }

    /// Installing into an untouched shard changes nothing observable and must
    /// not bump the WATCH version (the pipeline's no-op rule).
    // FM-REPLICATION-002
    #[tokio::test]
    async fn install_empty_snapshot_into_empty_shard_is_a_no_op() {
        let mut worker = worker();
        let before = worker.get_key_version(b"k");

        worker.install_snapshot(Vec::new()).await;

        assert_eq!(worker.get_key_version(b"k"), before);
    }

    /// The export is the exact inverse of the install: what a persistence-less
    /// primary serializes out of RAM is what a replica installs.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn export_snapshot_round_trips_through_install() {
        let mut source = worker();
        source
            .store
            .set(Bytes::from("a"), Value::string(Bytes::from("1")));
        source
            .store
            .set(Bytes::from("b"), Value::string(Bytes::from("2")));

        let blob = source.export_snapshot().expect("hot keyspace exports");
        let entries: Vec<SnapshotEntry> = frogdb_persistence::read_entries(&blob)
            .expect("blob decodes")
            .into_iter()
            .map(|e| SnapshotEntry {
                key: e.key,
                value: e.value,
                metadata: e.metadata,
            })
            .collect();

        let mut target = worker();
        target
            .store
            .set(Bytes::from("forked"), Value::string(Bytes::from("old")));
        target.install_snapshot(entries).await;

        assert_eq!(target.store.len(), 2);
        assert!(!target.store.contains(b"forked"));
        let a = target.store.get_hot(b"a").expect("exported key is live");
        let Value::String(s) = a.as_ref() else {
            panic!("expected a string value")
        };
        assert_eq!(s.as_bytes(), Bytes::from("1"));
    }

    /// An empty shard exports an empty blob — which still clears the replica,
    /// because the install is a replace.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn export_of_an_empty_shard_is_an_empty_blob() {
        let worker = worker();
        assert!(worker.export_snapshot().expect("empty export").is_empty());
    }

    /// Keys past their TTL are already invisible here; exporting them would
    /// resurrect them on the replica.
    // FM-REPLICATION-002
    #[tokio::test]
    async fn export_snapshot_drops_expired_keys() {
        let mut worker = worker();
        worker
            .store
            .set(Bytes::from("live"), Value::string(Bytes::from("v")));
        worker
            .store
            .set(Bytes::from("dead"), Value::string(Bytes::from("v")));
        worker
            .store
            .set_expiry(b"dead", Instant::now() - Duration::from_secs(1));

        let blob = worker.export_snapshot().expect("export succeeds");
        let keys: Vec<Bytes> = frogdb_persistence::read_entries(&blob)
            .expect("blob decodes")
            .into_iter()
            .map(|e| e.key)
            .collect();

        assert_eq!(keys, vec![Bytes::from("live")]);
    }
}
