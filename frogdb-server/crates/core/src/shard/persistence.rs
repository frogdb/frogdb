use crate::command::{WalAction, WriteRecord};
use crate::store::Store;

use smallvec::SmallVec;

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
// RocksDB.

/// Whether the persist bridge waits for the command's WAL actions to reach
/// storage before returning. The single axis of variation that used to be three
/// functions (`persist_by_strategy`, `persist_and_confirm`,
/// `persist_transaction_to_wal`), expressed as data the way
/// [`WalPhase`](super::post_execution) expresses its own (proposal 03).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Durability {
    /// Snapshot the sequence before the first write, stage every action, then
    /// `flush_through` the snapshot so the caller can propagate a flush failure.
    /// Confirmation fails if the flush fails *or* if a background
    /// (size-threshold/timeout) flush that already carried any of these entries
    /// failed — an acked write must never outrun a swallowed flush.
    ///
    /// Named for what it guarantees at every durability mode: the batch reached
    /// *storage*. Whether storage means the device is the durability mode's
    /// business — `sync` fsyncs the commit this waits on, `periodic`/`async`
    /// do not (FM-PERSISTENCE-043). The former name, `Confirm`, read as a
    /// durability guarantee at every call site and only was one under `sync`.
    Committed,
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
    /// Open a WAL write group: every entry written before the matching
    /// [`WalTarget::end_group`] must land in **one** committed storage batch.
    ///
    /// This is what makes a batch of [`WriteRecord`]s atomic against a
    /// checkpoint cut (BGSAVE) and against a crash. Without it the WAL's flush
    /// thread cuts batches on its own size/timeout schedule, so a shard task
    /// descheduled between two of a transaction's entries can leave a
    /// *committed prefix* of that transaction in storage.
    async fn begin_group(&self) -> std::io::Result<()>;
    /// Close the innermost open WAL write group.
    async fn end_group(&self) -> std::io::Result<()>;
    /// The WAL's current highest assigned sequence, or `None` when no WAL is
    /// configured. `Confirm` snapshots this *before* its first write so the lone
    /// [`WalTarget::flush_through`] below confirms every entry the batch
    /// produced; a `None` short-circuits the whole persist (no writes, no flush).
    fn wal_sequence(&self) -> Option<u64>;
    /// Confirm every entry assigned after `after_seq` is durable, propagating a
    /// flush failure (or a swallowed background flush that carried these entries)
    /// so an acked write can never outrun it. Only `Confirm` calls this.
    async fn flush_through(&self, after_seq: u64) -> std::io::Result<()>;
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

    async fn begin_group(&self) -> std::io::Result<()> {
        match self.persistence.wal_writer() {
            Some(wal) => wal.begin_group().await,
            None => Ok(()),
        }
    }

    async fn end_group(&self) -> std::io::Result<()> {
        match self.persistence.wal_writer() {
            Some(wal) => wal.end_group().await,
            None => Ok(()),
        }
    }

    fn wal_sequence(&self) -> Option<u64> {
        self.persistence.wal_writer().map(|wal| wal.sequence())
    }

    async fn flush_through(&self, after_seq: u64) -> std::io::Result<()> {
        match self.persistence.wal_writer() {
            Some(wal) => wal.flush_through(after_seq).await,
            // Unreachable via `persist_records` (it only flushes when
            // `wal_sequence` returned `Some`); inert if a future caller reaches it.
            None => Ok(()),
        }
    }
}

/// The one place a batch of [`WriteRecord`]s becomes WAL writes, expressed over
/// the [`WalTarget`] seam. Pure over the seam (no `self`, mirroring
/// [`execute_wal_action`]) so the confirm path's sequence-snapshot ordering and
/// `flush_through` failure injection are unit-testable without a [`ShardWorker`]
/// or RocksDB.
///
/// `Confirm` snapshots the sequence *before* the first write, stages every
/// action with `?` propagation, then `flush_through`s the snapshot exactly once.
/// `FireAndForget` logs each action error and continues, and never flushes. No
/// WAL configured ([`WalTarget::wal_sequence`] is `None`) short-circuits to
/// `Ok(())` with no writes.
///
/// Either way a batch of more than one action is bracketed by a
/// [`WalTarget::begin_group`] / [`WalTarget::end_group`] pair, so it commits to
/// storage as one unit: a concurrent checkpoint (BGSAVE) or a crash observes all
/// of the batch's writes or none of them, never a prefix. That is the atomicity
/// a `MULTI` / `EXEC` on one shard promises — durability (`Confirm`) is the
/// orthogonal axis. A one-action batch (the hot path: a single `SET`) is already
/// indivisible and skips the markers. Specified as the write-group row of
/// `specs/persistence.md`.
async fn persist_records(
    t: &impl WalTarget,
    records: &[WriteRecord<'_>],
    durability: Durability,
) -> std::io::Result<()> {
    // Snapshot the sequence *before* the first write, so the single
    // `flush_through` below confirms every entry this batch produced.
    let Some(start_seq) = t.wal_sequence() else {
        return Ok(());
    };

    // Resolve the batch's actions once. The count is what decides whether the
    // group markers are needed, and deriving it from the actions themselves
    // keeps [`WriteRecord::wal_actions`] the single authority on how many
    // entries a command produces — a second guess (per-strategy table, record
    // count) that drifted low would silently reopen the tear.
    let actions: SmallVec<[WalAction<'_>; 4]> =
        records.iter().flat_map(WriteRecord::wal_actions).collect();
    let grouped = actions.len() > 1;

    let opened = if grouped {
        t.begin_group().await
    } else {
        Ok(())
    };
    // A marker only fails when the WAL channel is gone, in which case every
    // write below would fail identically — skip them rather than log N times.
    let staged = if opened.is_ok() {
        stage_actions(t, &actions, durability).await
    } else {
        Ok(())
    };
    // Close what was opened on every path, including the staging error path: an
    // unclosed group suppresses this shard's background flushes until the next
    // explicit flush.
    let closed = if grouped && opened.is_ok() {
        t.end_group().await
    } else {
        Ok(())
    };

    match durability {
        Durability::Committed => {
            opened?;
            staged?;
            closed?;
            t.flush_through(start_seq).await
        }
        Durability::FireAndForget => {
            for e in [opened, closed].into_iter().filter_map(Result::err) {
                tracing::error!(error = %e, "WAL write group marker failed");
            }
            Ok(())
        }
    }
}

/// Stage every action of the batch, honoring `durability`'s error policy.
/// Split out of [`persist_records`] so the write-group brackets there are
/// unconditional — the group must close even when staging fails.
async fn stage_actions(
    t: &impl WalTarget,
    actions: &[WalAction<'_>],
    durability: Durability,
) -> std::io::Result<()> {
    for action in actions {
        match durability {
            Durability::Committed => execute_wal_action(t, action).await?,
            Durability::FireAndForget => {
                let _ = execute_wal_action(t, action)
                    .await
                    .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
            }
        }
    }
    Ok(())
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
        persist_records(self, records, durability).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;

    use bytes::Bytes;

    use crate::command::{
        Arity, Command, CommandFlags, ExecutionStrategy, WaiterWake, WalStrategy,
    };
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};

    /// A recorded write against the [`TestTarget`]: the WAL-write surface as
    /// observed, in call order.
    #[derive(Debug, PartialEq, Eq)]
    enum Write {
        Set(Vec<u8>),
        Delete(Vec<u8>),
        Merge(Vec<u8>, Vec<(u16, u8)>),
        Clear,
    }

    /// A recorded write-group marker.
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum Marker {
        Begin,
        End,
    }

    /// In-memory [`WalTarget`]: answers `contains` from a set and records writes
    /// in call order — no RocksDB, no `ShardWorker`. `fail` makes every write
    /// return an error so the propagation the `Confirm` durability relies on
    /// (`execute_wal_action` surfaces the failure via `?`) is exercised directly.
    struct TestTarget {
        present: HashSet<Vec<u8>>,
        writes: RefCell<Vec<Write>>,
        fail: bool,
        /// Injects a `flush_through` failure independently of the write `fail`
        /// gate, so the `Confirm` flush-failure path is exercisable on its own.
        flush_fail: bool,
        /// Monotonic WAL sequence: each successful write bumps it, so a test can
        /// assert the `Confirm` snapshot was taken *before* the first write
        /// advanced it.
        seq: Cell<u64>,
        /// The `after_seq` of each `flush_through` call, in order.
        flushes: RefCell<Vec<u64>>,
        /// Each write-group marker paired with the number of writes recorded
        /// before it — enough to assert the brackets enclose the whole batch
        /// without perturbing the `writes` log the other tests assert on.
        markers: RefCell<Vec<(Marker, usize)>>,
        /// Makes `begin_group` fail, modelling a dead WAL channel.
        group_fail: bool,
        /// Every write attempt, bumped *before* the fail gate — so a swallowed
        /// `FireAndForget` failure is still visible as an attempt that ran.
        attempts: Cell<u64>,
        /// Whether a WAL is configured — `false` models the no-WAL short-circuit.
        has_wal: bool,
    }

    impl TestTarget {
        fn new(present: &[&[u8]]) -> Self {
            Self {
                present: present.iter().map(|k| k.to_vec()).collect(),
                writes: RefCell::new(Vec::new()),
                fail: false,
                flush_fail: false,
                seq: Cell::new(0),
                flushes: RefCell::new(Vec::new()),
                markers: RefCell::new(Vec::new()),
                group_fail: false,
                attempts: Cell::new(0),
                has_wal: true,
            }
        }

        fn failing() -> Self {
            Self {
                fail: true,
                ..Self::new(&[])
            }
        }

        /// A target whose writes succeed but whose `flush_through` fails.
        fn flush_failing(present: &[&[u8]]) -> Self {
            Self {
                flush_fail: true,
                ..Self::new(present)
            }
        }

        /// A target with no WAL configured (the persist short-circuit).
        fn no_wal() -> Self {
            Self {
                has_wal: false,
                ..Self::new(&[])
            }
        }

        /// A target whose `begin_group` fails (dead WAL channel).
        fn group_failing() -> Self {
            Self {
                group_fail: true,
                ..Self::new(&[])
            }
        }

        /// Every write-group marker with the write count that preceded it.
        fn markers(&self) -> Vec<(Marker, usize)> {
            self.markers.take()
        }

        fn recorded(&self) -> Vec<Write> {
            self.writes.take()
        }

        /// The `after_seq` of each `flush_through` call, in order.
        fn flushed(&self) -> Vec<u64> {
            self.flushes.take()
        }

        /// Number of write attempts (counted before the fail gate).
        fn attempts(&self) -> u64 {
            self.attempts.get()
        }

        fn gate(&self) -> std::io::Result<()> {
            self.attempts.set(self.attempts.get() + 1);
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
            self.seq.set(self.seq.get() + 1);
            self.writes.borrow_mut().push(Write::Set(key.to_vec()));
            Ok(())
        }
        async fn write_delete(&self, key: &[u8]) -> std::io::Result<()> {
            self.gate()?;
            self.seq.set(self.seq.get() + 1);
            self.writes.borrow_mut().push(Write::Delete(key.to_vec()));
            Ok(())
        }
        async fn write_merge(&self, key: &[u8], pairs: &[(u16, u8)]) -> std::io::Result<()> {
            self.gate()?;
            self.seq.set(self.seq.get() + 1);
            self.writes
                .borrow_mut()
                .push(Write::Merge(key.to_vec(), pairs.to_vec()));
            Ok(())
        }
        async fn write_clear(&self) -> std::io::Result<()> {
            self.gate()?;
            self.seq.set(self.seq.get() + 1);
            self.writes.borrow_mut().push(Write::Clear);
            Ok(())
        }
        async fn begin_group(&self) -> std::io::Result<()> {
            if self.group_fail {
                return Err(std::io::Error::other("injected group failure"));
            }
            let writes = self.writes.borrow().len();
            self.markers.borrow_mut().push((Marker::Begin, writes));
            Ok(())
        }
        async fn end_group(&self) -> std::io::Result<()> {
            let writes = self.writes.borrow().len();
            self.markers.borrow_mut().push((Marker::End, writes));
            Ok(())
        }
        fn wal_sequence(&self) -> Option<u64> {
            self.has_wal.then(|| self.seq.get())
        }
        async fn flush_through(&self, after_seq: u64) -> std::io::Result<()> {
            if self.flush_fail {
                return Err(std::io::Error::other("injected flush failure"));
            }
            self.flushes.borrow_mut().push(after_seq);
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

    // FM-PERSISTENCE-014
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

    // FM-PERSISTENCE-012
    // `ClearShard` routes to the keyless clear surface.
    #[tokio::test]
    async fn clear_shard_routes_to_clear() {
        let t = TestTarget::new(&[]);
        execute_wal_action(&t, &WalAction::ClearShard)
            .await
            .unwrap();
        assert_eq!(t.recorded(), vec![Write::Clear]);
    }

    // FM-PERSISTENCE-008
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

    // A minimal `PersistFirstKey` command: one write record over args `[key]`
    // resolves to a single `WalAction::Persist(key)` -> `Write::Set(key)`, so the
    // `persist_records` flow can be exercised without a real command handler.
    struct MockPersistCommand;

    impl Command for MockPersistCommand {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                docs: crate::command_spec::CommandDocs {
                    summary: "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
                    since: "1.0.0",
                    group: "string",
                    complexity: Some("O(1)"),
                },
                arity: Arity::Fixed(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: ExecutionStrategy::Standard,
            };
            &SPEC
        }

        fn execute(
            &self,
            _ctx: &mut crate::command::CommandContext,
            _args: &[Bytes],
        ) -> Result<frogdb_protocol::Response, frogdb_types::CommandError> {
            Ok(frogdb_protocol::Response::ok())
        }
    }

    // Build a one-key `PersistFirstKey` write record over `key`.
    fn record_args(key: &[u8]) -> [Bytes; 1] {
        [Bytes::copy_from_slice(key)]
    }

    // FM-PERSISTENCE-002
    // Confirm snapshots the sequence *before* the first write and calls
    // `flush_through` exactly once, with that snapshot, after every write.
    #[tokio::test]
    async fn confirm_snapshots_sequence_then_flushes_once() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::new(&[]);
        persist_records(&t, &records, Durability::Committed)
            .await
            .unwrap();

        assert_eq!(
            t.recorded(),
            vec![Write::Set(b"a".to_vec()), Write::Set(b"b".to_vec())]
        );
        // Snapshot was 0 (before any write), and exactly one flush was issued
        // after both writes advanced the sequence to 2.
        assert_eq!(t.flushed(), vec![0]);
    }

    // FM-PERSISTENCE-005
    // FireAndForget never flushes, and a failing write is logged and does not
    // abort the writes that follow it (pins the effect-path swallow behavior).
    #[tokio::test]
    async fn fire_and_forget_never_flushes_and_continues_on_error() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        // Every write fails, yet the batch still returns Ok and attempts both.
        let t = TestTarget::failing();
        persist_records(&t, &records, Durability::FireAndForget)
            .await
            .unwrap();

        assert_eq!(
            t.attempts(),
            2,
            "both records attempted despite the first failing"
        );
        assert!(t.flushed().is_empty(), "FireAndForget must never flush");
    }

    // FM-PERSISTENCE-007
    // Confirm propagates an injected `flush_through` failure even though every
    // write succeeded.
    #[tokio::test]
    async fn confirm_propagates_flush_failure() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let records = [WriteRecord::new(&cmd, &a)];

        let t = TestTarget::flush_failing(&[]);
        let result = persist_records(&t, &records, Durability::Committed).await;

        assert!(result.is_err(), "flush failure must propagate");
        assert_eq!(t.recorded(), vec![Write::Set(b"a".to_vec())]);
    }

    // FM-PERSISTENCE-008
    // Confirm propagates a write failure via `?` and never reaches the flush.
    #[tokio::test]
    async fn confirm_write_failure_aborts_before_flush() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::failing();
        let result = persist_records(&t, &records, Durability::Committed).await;

        assert!(result.is_err(), "write failure must propagate");
        // First write failed -> `?` aborted before the second write and the flush.
        assert_eq!(t.attempts(), 1, "aborted after the first failing write");
        assert!(
            t.flushed().is_empty(),
            "no flush after a failed Confirm write"
        );
    }

    // FM-PERSISTENCE-009
    // No WAL configured -> no writes, no flush, Ok(()) — for both durabilities.
    #[tokio::test]
    async fn no_wal_short_circuits() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let records = [WriteRecord::new(&cmd, &a)];

        for durability in [Durability::Committed, Durability::FireAndForget] {
            let t = TestTarget::no_wal();
            persist_records(&t, &records, durability).await.unwrap();
            assert!(
                t.recorded().is_empty(),
                "{durability:?}: no writes without a WAL"
            );
            assert!(
                t.flushed().is_empty(),
                "{durability:?}: no flush without a WAL"
            );
            assert_eq!(t.attempts(), 0, "{durability:?}: no attempts without a WAL");
            assert!(
                t.markers().is_empty(),
                "{durability:?}: no write group without a WAL"
            );
        }
    }

    // A single-action batch skips the markers: one WAL entry is already
    // indivisible, so the hot path does not pay for a group it cannot need.
    // FM-PERSISTENCE-001
    #[tokio::test]
    async fn single_action_persist_skips_the_write_group() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let records = [WriteRecord::new(&cmd, &a)];

        for durability in [Durability::Committed, Durability::FireAndForget] {
            let t = TestTarget::new(&[]);
            persist_records(&t, &records, durability).await.unwrap();
            assert_eq!(t.recorded(), vec![Write::Set(b"a".to_vec())]);
            assert!(
                t.markers().is_empty(),
                "{durability:?}: no group around a lone entry"
            );
        }
    }

    // Every multi-action persist brackets its whole batch in exactly one write
    // group, under both durabilities: the group is the atomicity unit a
    // checkpoint cut and a crash observe, independent of whether the caller
    // waits for durability.
    // FM-PERSISTENCE-001
    #[tokio::test]
    async fn persist_brackets_the_batch_in_one_write_group() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let c = record_args(b"c");
        let records = [
            WriteRecord::new(&cmd, &a),
            WriteRecord::new(&cmd, &b),
            WriteRecord::new(&cmd, &c),
        ];

        for durability in [Durability::Committed, Durability::FireAndForget] {
            let t = TestTarget::new(&[]);
            persist_records(&t, &records, durability).await.unwrap();
            assert_eq!(
                t.markers(),
                vec![(Marker::Begin, 0), (Marker::End, 3)],
                "{durability:?}: one group opened before the first write and \
                 closed after the last"
            );
        }
    }

    // A staging failure still closes the group. An unclosed group would suppress
    // the shard's background flushes for the life of the process.
    // FM-PERSISTENCE-001
    #[tokio::test]
    async fn write_group_closes_on_staging_failure() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::failing();
        let result = persist_records(&t, &records, Durability::Committed).await;

        assert!(result.is_err(), "write failure must still propagate");
        assert_eq!(
            t.markers(),
            vec![(Marker::Begin, 0), (Marker::End, 0)],
            "the group is closed even though staging aborted"
        );
    }

    // FM-PERSISTENCE-008
    // If the group cannot be opened the WAL channel is gone: skip the writes
    // (they would all fail), do not emit an unmatched close, and propagate under
    // Confirm while FireAndForget logs and returns Ok.
    // FM-PERSISTENCE-001
    #[tokio::test]
    async fn failed_group_open_skips_writes() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::group_failing();
        assert!(
            persist_records(&t, &records, Durability::Committed)
                .await
                .is_err(),
            "Confirm propagates a dead WAL channel"
        );
        assert_eq!(t.attempts(), 0, "no writes attempted without a group");
        assert!(t.markers().is_empty(), "no unmatched close emitted");
        assert!(t.flushed().is_empty(), "no flush after a failed group open");

        let t = TestTarget::group_failing();
        persist_records(&t, &records, Durability::FireAndForget)
            .await
            .expect("FireAndForget logs rather than propagates");
        assert_eq!(t.attempts(), 0);
    }
}

/// End-to-end tests for `durability-mode = sync` under the **default**
/// `wal-failure-policy = continue`, driven through
/// [`ShardWorker::execute_command`] against a fake WAL sink.
///
/// The units above pin `persist_records`' behavior once a [`Durability`] value
/// has been chosen; these pin the layer that *chooses* it. That layer used to
/// read the failure policy alone, so `sync` durability under the shipped
/// default acknowledged a write with only a `FireAndForget` stage behind it —
/// the entry on the flush channel, the reply already on the wire, and the fsync
/// scheduled for whenever the flush thread got round to it (FM-PERSISTENCE-002,
/// spec-gaps issue 01).
#[cfg(test)]
mod sync_durability_ack_tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU8;

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use crate::command::{
        Arity, Command, CommandContext, CommandFlags, ExecutionStrategy, WaiterWake, WalStrategy,
    };
    use crate::command_spec::{
        AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec, ReindexSpec,
    };
    use crate::noop::NoopMetricsRecorder;
    use crate::persistence::{DurabilityMode, FakeWalLog, WalEffectKind};
    use crate::registry::CommandRegistry;
    use crate::shard::FakeWalRegistry;
    use crate::shard::builder::{ShardWorkerBuilder, WalMode};
    use crate::shard::message::{ShardReceiver, ShardSender};
    use crate::shard::worker::ShardWorker;
    use crate::store::{HashMapStore, Store};
    use crate::types::Value;
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};

    /// A `SET` that really writes to the store, so an acked write has something
    /// behind it for the crash model to keep or lose.
    struct MockSet;
    impl Command for MockSet {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                docs: crate::command_spec::CommandDocs {
                    summary: "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
                    since: "1.0.0",
                    group: "string",
                    complexity: Some("O(1)"),
                },
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::None,
                event: EventSpec::Suppressed,
                requires_same_slot: false,
                reindex: ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: ExecutionStrategy::Standard,
            };
            &SPEC
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            ctx.store
                .set(args[0].clone(), Value::string(args[1].clone()));
            Ok(Response::ok())
        }
    }

    /// A shard with a healthy fake WAL, the shipped default failure policy
    /// (`continue`, encoded 0) and the given durability mode.
    fn shard_with_mode(mode: DurabilityMode) -> (ShardWorker, FakeWalLog) {
        FakeWalRegistry::clear();
        let mut registry = CommandRegistry::new();
        registry.register(MockSet);
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_conn_tx, conn_rx) = mpsc::channel(16);
        let worker = ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(msg_rx))
            .with_new_conn_rx(conn_rx)
            .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
            .with_registry(Arc::new(registry))
            .with_metrics(Arc::new(NoopMetricsRecorder::new()))
            .with_store(HashMapStore::new())
            .with_wal_mode(WalMode::Fake)
            .with_durability_mode(mode)
            .with_wal_failure_policy(Arc::new(AtomicU8::new(0)))
            .build();
        let log = FakeWalRegistry::log(0).expect("fake sink log registered for shard 0");
        (worker, log)
    }

    fn set(key: &'static str, value: &'static str) -> ParsedCommand {
        ParsedCommand::new(
            Bytes::from_static(b"SET"),
            vec![
                Bytes::from_static(key.as_bytes()),
                Bytes::from_static(value.as_bytes()),
            ],
        )
    }

    // FM-PERSISTENCE-002
    // The ack is gated on the durability mode, not on the failure policy: with
    // `durability-mode = sync` and the default `wal-failure-policy = continue`,
    // the write's `flush_through` must have reached the sink *before*
    // `execute_command` produced the reply value.
    #[tokio::test]
    async fn sync_mode_under_continue_policy_flushes_before_the_reply() {
        let (mut worker, log) = shard_with_mode(DurabilityMode::Sync);
        assert!(
            !worker.persistence.should_rollback(),
            "policy 0 is `continue`, the shipped default"
        );

        let response = worker
            .execute_command(&set("k", "v"), 1, ProtocolVersion::Resp2, false)
            .await;

        assert!(
            matches!(response, Response::Simple(ref s) if s.as_ref() == b"OK"),
            "the write is acknowledged, got {response:?}"
        );

        // Everything the sink saw, it saw before this point: `execute_command`
        // has returned, so the effect log is complete as of the reply.
        let effects = log.effects();
        let write = effects
            .iter()
            .find(|e| e.kind == WalEffectKind::Set)
            .expect("the write reached the WAL");
        let flush = effects
            .iter()
            .find(|e| e.kind == WalEffectKind::FlushThrough)
            .expect(
                "sync durability must flush_through before the reply — \
                 an ack with only a FireAndForget stage behind it is the loss window",
            );
        assert!(
            flush.order > write.order,
            "the flush must follow the write it confirms: {flush:?} vs {write:?}"
        );
    }

    // FM-PERSISTENCE-002
    // The same run, read through the page-cache crash model: a crash the instant
    // after the reply must still find the acked write on the device. `periodic`
    // is the contrast — the same command, the same policy, and nothing durable.
    #[tokio::test]
    async fn sync_mode_acked_write_survives_a_crash_under_continue_policy() {
        let (mut worker, log) = shard_with_mode(DurabilityMode::Sync);
        let response = worker
            .execute_command(&set("k", "v"), 1, ProtocolVersion::Resp2, false)
            .await;
        assert!(matches!(response, Response::Simple(ref s) if s.as_ref() == b"OK"));
        assert!(worker.store.get(b"k").is_some(), "live in memory");

        // Crash here.
        let survivors = log.durable_writes();
        assert_eq!(
            survivors.len(),
            1,
            "the acked write must be past the crash, got {survivors:?}"
        );
        assert_eq!(survivors[0].key.as_deref(), Some(&b"k"[..]));

        // `periodic` makes no such promise, and pins that the assertion above is
        // about the mode rather than about the fake sink always flushing.
        let (mut worker, log) = shard_with_mode(DurabilityMode::Periodic { interval_ms: 1000 });
        let response = worker
            .execute_command(&set("k", "v"), 1, ProtocolVersion::Resp2, false)
            .await;
        assert!(matches!(response, Response::Simple(ref s) if s.as_ref() == b"OK"));
        assert!(
            log.durable_writes().is_empty(),
            "periodic durability acks without waiting — durability comes from the syncer"
        );
    }
}
