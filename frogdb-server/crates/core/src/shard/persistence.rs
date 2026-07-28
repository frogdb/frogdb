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
async fn persist_records(
    t: &impl WalTarget,
    records: &[WriteRecord<'_>],
    durability: Durability,
) -> std::io::Result<()> {
    let Some(current_seq) = t.wal_sequence() else {
        return Ok(());
    };
    // Snapshot the sequence *before* the first write in Confirm mode, so the
    // single `flush_through` below confirms every entry this batch produced.
    let start_seq = matches!(durability, Durability::Confirm).then_some(current_seq);

    for record in records {
        for action in record.wal_actions() {
            match durability {
                Durability::Confirm => execute_wal_action(t, &action).await?,
                Durability::FireAndForget => {
                    let _ = execute_wal_action(t, &action)
                        .await
                        .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
                }
            }
        }
    }

    match start_seq {
        Some(seq) => t.flush_through(seq).await, // Confirm
        None => Ok(()),                          // FireAndForget
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

    // A minimal `PersistFirstKey` command: one write record over args `[key]`
    // resolves to a single `WalAction::Persist(key)` -> `Write::Set(key)`, so the
    // `persist_records` flow can be exercised without a real command handler.
    struct MockPersistCommand;

    impl Command for MockPersistCommand {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
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

    // Confirm snapshots the sequence *before* the first write and calls
    // `flush_through` exactly once, with that snapshot, after every write.
    #[tokio::test]
    async fn confirm_snapshots_sequence_then_flushes_once() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::new(&[]);
        persist_records(&t, &records, Durability::Confirm)
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

    // Confirm propagates an injected `flush_through` failure even though every
    // write succeeded.
    #[tokio::test]
    async fn confirm_propagates_flush_failure() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let records = [WriteRecord::new(&cmd, &a)];

        let t = TestTarget::flush_failing(&[]);
        let result = persist_records(&t, &records, Durability::Confirm).await;

        assert!(result.is_err(), "flush failure must propagate");
        assert_eq!(t.recorded(), vec![Write::Set(b"a".to_vec())]);
    }

    // Confirm propagates a write failure via `?` and never reaches the flush.
    #[tokio::test]
    async fn confirm_write_failure_aborts_before_flush() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let b = record_args(b"b");
        let records = [WriteRecord::new(&cmd, &a), WriteRecord::new(&cmd, &b)];

        let t = TestTarget::failing();
        let result = persist_records(&t, &records, Durability::Confirm).await;

        assert!(result.is_err(), "write failure must propagate");
        // First write failed -> `?` aborted before the second write and the flush.
        assert_eq!(t.attempts(), 1, "aborted after the first failing write");
        assert!(
            t.flushed().is_empty(),
            "no flush after a failed Confirm write"
        );
    }

    // No WAL configured -> no writes, no flush, Ok(()) — for both durabilities.
    #[tokio::test]
    async fn no_wal_short_circuits() {
        let cmd = MockPersistCommand;
        let a = record_args(b"a");
        let records = [WriteRecord::new(&cmd, &a)];

        for durability in [Durability::Confirm, Durability::FireAndForget] {
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
        }
    }
}
