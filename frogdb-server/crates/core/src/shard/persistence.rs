use crate::command::{WalAction, WriteRecord};
use crate::store::Store;

use frogdb_types::metrics::definitions::WalMergeOperands;

use super::connection::NewConnection;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle a new connection assigned to this shard.
    pub(crate) async fn handle_new_connection(&self, new_conn: NewConnection) {
        tracing::debug!(
            shard_id = self.shard_id(),
            conn_id = new_conn.conn_id,
            addr = %new_conn.addr,
            "New connection assigned to shard"
        );

        // Connection handling is spawned as a separate task
        // The actual connection loop is implemented in the server crate
    }
}

// =============================================================================
// The shard persistence bridge
// =============================================================================
//
// One decision lives here: *given a command's resolved WAL actions, write them
// and (rollback only) confirm they are durable*. It is expressed as one entry
// point — [`ShardWorker::persist`] — parameterized by a [`Durability`] enum,
// over one narrow seam — [`WalTarget`] — so the store-existence probes
// [`execute_wal_action`] performs are unit-testable without a `ShardWorker` or
// RocksDB. See `todo/proposals/54-shard-persistence-bridge.md`.

/// Whether the persist bridge confirms durability after staging a command's
/// WAL actions. The single axis of variation that used to be three functions
/// (`persist_by_strategy`, `persist_and_confirm`, `persist_transaction_to_wal`),
/// expressed as data the way [`WalPhase`](super::post_execution) expresses its
/// own (proposal 03).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    /// Rollback mode: snapshot the sequence before the first write, stage every
    /// action, then `flush_through` the snapshot so the caller can propagate a
    /// flush failure. Confirmation fails if the flush fails *or* if a background
    /// (size-threshold/timeout) flush that already carried any of these entries
    /// failed — an acked write must never outrun a swallowed flush.
    Confirm,
    /// Effect (hot) path: stage each action and log on error; the flush pipeline
    /// owns durability asynchronously. Never calls `flush_through`.
    FireAndForget,
}

/// The store-view + WAL-write surface [`execute_wal_action`] needs, as a seam.
///
/// `execute_wal_action` needs exactly two capabilities from its environment:
/// **probe** (`does this key currently exist?`) and **write** (persist / delete
/// / merge / clear an entry). [`ShardWorker`] is the production adapter — probe
/// reads `self.store`, writes go through `self.persistence.wal_writer()`, and
/// `write_set`/`write_merge` own the store metadata read so the free function
/// stays pure over *set-this-key*, never *set key=value*. Tests supply an
/// in-memory adapter that answers `contains` from a set and records the write
/// calls in order, so the probe-relative ordering the integration suite guards
/// end-to-end becomes a three-line unit assertion. Mirrors the
/// [`WriteSink`](../../../persistence/wal/flush.rs) seam one layer down.
pub(crate) trait WalTarget {
    /// Whether `key` currently exists in the store view.
    fn contains(&self, key: &[u8]) -> bool;
    /// Persist `key`'s current in-store value to the WAL. A no-op if the key is
    /// absent or no WAL is configured; the adapter owns the value/metadata read.
    async fn write_set(&self, key: &[u8]) -> std::io::Result<()>;
    /// Persist a deletion of `key` to the WAL.
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<()>;
    /// Persist a HyperLogLog register-max delta for `key` as a `Merge` operand.
    async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)]) -> std::io::Result<()>;
    /// Persist a full-shard clear as a keyless range-tombstone entry.
    async fn write_clear(&self) -> std::io::Result<()>;
}

/// Resolve one [`WalAction`] against a target. Pure over the seam (no `self`),
/// so the probe-vs-write ordering is unit-testable directly.
///
/// This is the only place that maps a `WalAction` to a target call. Adding a new
/// action variant requires extending this match — and only this match.
async fn execute_wal_action(t: &impl WalTarget, action: &WalAction<'_>) -> std::io::Result<()> {
    match action {
        WalAction::Persist(key) => t.write_set(key).await,
        WalAction::DeleteIfMissing(key) => {
            if !t.contains(key) {
                t.write_delete(key).await
            } else {
                Ok(())
            }
        }
        WalAction::PersistOrDelete(key) => {
            if t.contains(key) {
                t.write_set(key).await
            } else {
                t.write_delete(key).await
            }
        }
        WalAction::PersistIfExists(key) => {
            if t.contains(key) {
                t.write_set(key).await
            } else {
                Ok(())
            }
        }
        WalAction::MergeHllDelta { key, pairs } => t.write_merge(key, pairs).await,
        WalAction::ClearShard => t.write_clear().await,
    }
}

/// Production [`WalTarget`]: probe reads the shard's store, writes go through the
/// shard's [`RocksWalWriter`]. Preserves the metadata-lookup framing the former
/// `persist_key_to_wal` / `merge_hll_delta_to_wal` helpers performed.
impl WalTarget for ShardWorker {
    fn contains(&self, key: &[u8]) -> bool {
        self.store.contains(key)
    }

    async fn write_set(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer()
            && let Some(value) = self.store.get_hot(key)
        {
            let metadata = self
                .store
                .get_metadata(key)
                .unwrap_or_else(|| crate::types::KeyMetadata::new(value.memory_size()));
            wal.write_set(key, &value, &metadata).await?;
        }
        Ok(())
    }

    async fn write_delete(&self, key: &[u8]) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer() {
            wal.write_delete(key).await?;
        }
        Ok(())
    }

    /// Reads the key's current metadata (the same store lookup [`write_set`]
    /// does) so the operand carries the size/TTL framing the merge operator
    /// needs, then enqueues the delta via [`RocksWalWriter::write_merge`].
    /// Increments [`WalMergeOperands`] on a successful enqueue so the delta path
    /// is observable.
    async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)]) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer() {
            let metadata = self.store.get_metadata(key).unwrap_or_else(|| {
                // Unreachable in practice: the key exists at deferred-persist time
                // on a single-threaded shard, so both `get_metadata` and `get_hot`
                // hit. Harmless even if reached -- the merge frame header serializes
                // only marker/expires/lfu, never `size`, so the `0` fallback is inert.
                let size = self
                    .store
                    .get_hot(key)
                    .map(|v| v.memory_size())
                    .unwrap_or(0);
                crate::types::KeyMetadata::new(size)
            });
            wal.write_merge(key, pairs, &metadata).await?;
            WalMergeOperands::inc(self.observability.metrics());
        }
        Ok(())
    }

    /// The flush thread applies the clear as a full-range delete of the shard's
    /// primary column family, seq-ordered with surrounding Put/Delete/Merge
    /// entries so a write accepted after the flush lands after the range
    /// tombstone (see [`RocksWalWriter::write_clear`]).
    async fn write_clear(&self) -> std::io::Result<()> {
        if let Some(wal) = self.persistence.wal_writer() {
            wal.write_clear().await?;
        }
        Ok(())
    }
}

impl ShardWorker {
    /// The one place a batch of [`WriteRecord`]s becomes WAL writes.
    ///
    /// Absorbs the former `persist_by_strategy` (fire-and-log effect path),
    /// `persist_and_confirm` (single-record rollback), and
    /// `persist_transaction_to_wal` (batch rollback); the single-record callers
    /// pass a one-element slice. The `durability` axis selects between staging +
    /// confirming (rollback: a flush failure propagates so an acked write can
    /// never outrun a swallowed flush) and staging + logging (hot path: the
    /// flush pipeline owns durability asynchronously).
    ///
    /// Delta-vs-full routing stays inside [`WriteRecord::wal_actions`], so both
    /// paths agree on whether a dense PFADD becomes a `Merge` or a `Put`.
    pub(crate) async fn persist(
        &self,
        records: &[WriteRecord<'_>],
        durability: Durability,
    ) -> std::io::Result<()> {
        let Some(wal) = self.persistence.wal_writer() else {
            return Ok(());
        };
        // Snapshot the sequence *before* the first write in Confirm mode, so the
        // single `flush_through` below confirms every entry this batch produced.
        let start_seq = matches!(durability, Durability::Confirm).then(|| wal.sequence());

        for record in records {
            for action in record.wal_actions() {
                match durability {
                    Durability::Confirm => execute_wal_action(self, &action).await?,
                    Durability::FireAndForget => {
                        let _ = execute_wal_action(self, &action)
                            .await
                            .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
                    }
                }
            }
        }

        match start_seq {
            Some(seq) => wal.flush_through(seq).await, // Confirm
            None => Ok(()),                            // FireAndForget
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::RefCell;
    use std::collections::HashSet;

    /// A recorded write against the [`TestTarget`]: the WAL-write surface as
    /// observed, in call order.
    #[derive(Debug, PartialEq, Eq)]
    enum Write {
        Set(Vec<u8>),
        Delete(Vec<u8>),
        Merge(Vec<u8>, Vec<(u16, u8)>),
        Clear,
    }

    /// In-memory [`WalTarget`]: answers `contains` from a set and records writes
    /// in call order — no RocksDB, no `ShardWorker`. `fail` makes every write
    /// return an error so the propagation the `Confirm` durability relies on
    /// (`execute_wal_action` surfaces the failure via `?`) is exercised directly.
    struct TestTarget {
        present: HashSet<Vec<u8>>,
        writes: RefCell<Vec<Write>>,
        fail: bool,
    }

    impl TestTarget {
        fn new(present: &[&[u8]]) -> Self {
            Self {
                present: present.iter().map(|k| k.to_vec()).collect(),
                writes: RefCell::new(Vec::new()),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::new(&[])
            }
        }

        fn recorded(&self) -> Vec<Write> {
            self.writes.take()
        }

        fn gate(&self) -> std::io::Result<()> {
            if self.fail {
                Err(std::io::Error::other("injected WAL failure"))
            } else {
                Ok(())
            }
        }
    }

    impl WalTarget for TestTarget {
        fn contains(&self, key: &[u8]) -> bool {
            self.present.contains(key)
        }
        async fn write_set(&self, key: &[u8]) -> std::io::Result<()> {
            self.gate()?;
            self.writes.borrow_mut().push(Write::Set(key.to_vec()));
            Ok(())
        }
        async fn write_delete(&self, key: &[u8]) -> std::io::Result<()> {
            self.gate()?;
            self.writes.borrow_mut().push(Write::Delete(key.to_vec()));
            Ok(())
        }
        async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)]) -> std::io::Result<()> {
            self.gate()?;
            self.writes
                .borrow_mut()
                .push(Write::Merge(key.to_vec(), pairs.to_vec()));
            Ok(())
        }
        async fn write_clear(&self) -> std::io::Result<()> {
            self.gate()?;
            self.writes.borrow_mut().push(Write::Clear);
            Ok(())
        }
    }

    // `Persist` always writes the current value, independent of the probe.
    #[tokio::test]
    async fn persist_always_writes_set() {
        let present = TestTarget::new(&[b"k"]);
        execute_wal_action(&present, &WalAction::Persist(b"k"))
            .await
            .unwrap();
        assert_eq!(present.recorded(), vec![Write::Set(b"k".to_vec())]);

        let absent = TestTarget::new(&[]);
        execute_wal_action(&absent, &WalAction::Persist(b"k"))
            .await
            .unwrap();
        assert_eq!(absent.recorded(), vec![Write::Set(b"k".to_vec())]);
    }

    // `PersistOrDelete` writes a `set` when the probe says present and a
    // `delete` when absent — the delete-on-empty semantics BITOP / SORT…STORE
    // rely on to survive a restart.
    #[tokio::test]
    async fn persist_or_delete_probes_store() {
        let present = TestTarget::new(&[b"dest"]);
        execute_wal_action(&present, &WalAction::PersistOrDelete(b"dest"))
            .await
            .unwrap();
        assert_eq!(present.recorded(), vec![Write::Set(b"dest".to_vec())]);

        let absent = TestTarget::new(&[]);
        execute_wal_action(&absent, &WalAction::PersistOrDelete(b"dest"))
            .await
            .unwrap();
        assert_eq!(absent.recorded(), vec![Write::Delete(b"dest".to_vec())]);
    }

    // `DeleteIfMissing` no-ops on a surviving key (the prior value stays
    // authoritative) and writes a `delete` on a gone key.
    #[tokio::test]
    async fn delete_if_missing_probes_store() {
        let survived = TestTarget::new(&[b"k"]);
        execute_wal_action(&survived, &WalAction::DeleteIfMissing(b"k"))
            .await
            .unwrap();
        assert!(survived.recorded().is_empty());

        let gone = TestTarget::new(&[]);
        execute_wal_action(&gone, &WalAction::DeleteIfMissing(b"k"))
            .await
            .unwrap();
        assert_eq!(gone.recorded(), vec![Write::Delete(b"k".to_vec())]);
    }

    // `PersistIfExists` writes a `set` for a surviving destination and no-ops
    // when absent (the former `PersistDestination` semantics).
    #[tokio::test]
    async fn persist_if_exists_probes_store() {
        let present = TestTarget::new(&[b"dest"]);
        execute_wal_action(&present, &WalAction::PersistIfExists(b"dest"))
            .await
            .unwrap();
        assert_eq!(present.recorded(), vec![Write::Set(b"dest".to_vec())]);

        let absent = TestTarget::new(&[]);
        execute_wal_action(&absent, &WalAction::PersistIfExists(b"dest"))
            .await
            .unwrap();
        assert!(absent.recorded().is_empty());
    }

    // `MergeHllDelta` routes to the merge surface carrying exactly its pairs.
    #[tokio::test]
    async fn merge_hll_delta_routes_to_merge() {
        let t = TestTarget::new(&[b"hll"]);
        let pairs: [(u16, u8); 2] = [(1, 5), (42, 3)];
        execute_wal_action(
            &t,
            &WalAction::MergeHllDelta {
                key: b"hll",
                pairs: &pairs,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            t.recorded(),
            vec![Write::Merge(b"hll".to_vec(), pairs.to_vec())]
        );
    }

    // `ClearShard` routes to the keyless clear surface.
    #[tokio::test]
    async fn clear_shard_routes_to_clear() {
        let t = TestTarget::new(&[]);
        execute_wal_action(&t, &WalAction::ClearShard)
            .await
            .unwrap();
        assert_eq!(t.recorded(), vec![Write::Clear]);
    }

    // A failing target surfaces the error — this is what a `Confirm` persist
    // propagates via `?` (and what `FireAndForget` swallows with a log).
    #[tokio::test]
    async fn write_failure_propagates() {
        let t = TestTarget::failing();
        assert!(
            execute_wal_action(&t, &WalAction::Persist(b"k"))
                .await
                .is_err()
        );
        // The probe still runs first; the write is what fails.
        assert!(
            execute_wal_action(&t, &WalAction::PersistOrDelete(b"k"))
                .await
                .is_err()
        );
    }
}
