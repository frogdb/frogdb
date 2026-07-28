//! Replication-lifecycle dispatch: installing a full-resync snapshot into the
//! live keyspace.
//!
//! A replica that takes a runtime `REPLICAOF <new-master>` (or boots into a
//! full resync) must adopt the master's dataset *live* — otherwise it keeps
//! serving its own, possibly forked, keyspace and only reconverges on restart
//! (issue 61). The snapshot is read off disk by the replication driver and
//! delivered here, one message per shard.

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
        }
        false
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
    #[tokio::test]
    async fn install_empty_snapshot_into_empty_shard_is_a_no_op() {
        let mut worker = worker();
        let before = worker.get_key_version(b"k");

        worker.install_snapshot(Vec::new()).await;

        assert_eq!(worker.get_key_version(b"k"), before);
    }
}
