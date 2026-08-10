//! Storage-conformance layer for the RocksDB Raft log store.
//!
//! Every other validation layer in the cluster-correctness PRD sits *on or
//! above* the state machine: the invariant catalog is pure over
//! `ClusterStateInner`, the properties and the stateright models drive
//! `apply_command`, and the seeded schedules watch client-visible outcomes. A
//! defect produced entirely below the state machine — the log store lying to
//! openraft about what is on disk — is invisible to all of them
//! (`.scratch/cluster-correctness/issues/done/21-no-layer-sees-the-raft-log-store.md`).
//!
//! This module is that missing layer, in two parts:
//!
//! * **[`openraft_conformance_suite`]** runs `openraft::testing::Suite` — the
//!   log-store *and* state-machine conformance cases openraft ships for exactly
//!   this seam — against a temp-dir [`ClusterStorage`] plus the real
//!   [`ClusterStateMachine`]. It is the contract, written by the crate whose
//!   contract it is, instead of our reading of it.
//! * **[`openraft_conformance_suite_through_a_long_lived_reader`]** runs the
//!   same suite a second time with every *read* served by a
//!   [`RaftLogStorage::get_log_reader`] handle taken before the first write.
//!   openraft builds that reader once at startup and holds it for the node's
//!   lifetime, and the suite itself never constructs one, so without this pass
//!   the reader path has no conformance coverage at all (FM-CLUSTER-099).
//! * **[`a_reader_and_its_owner_never_disagree_with_the_column_family`]** is the
//!   coherence property the suite does not state: a generated
//!   append/truncate/purge/read sequence, executed against both the owning
//!   handle and a reader obtained *before* the sequence started, where every
//!   read through either handle must serve exactly the bytes the `raft_logs`
//!   column family holds.

use std::collections::BTreeSet;

use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::testing::{StoreBuilder, Suite};
use proptest::prelude::*;
use tempfile::TempDir;

use super::*;
use crate::state::ClusterStateMachine;
use crate::types::ClusterCommand;

// ---- the store under test --------------------------------------------------

/// Builds the real pair — a RocksDB log store in a fresh temp dir and a state
/// machine attached to that store's snapshot store — for openraft to drive.
///
/// The snapshot store is attached rather than left off so the suite's snapshot
/// cases (`snapshot_meta`, `transfer_snapshot`) exercise the durable path a
/// running node uses, not the in-memory fallback.
struct TempDirStoreBuilder;

impl StoreBuilder<TypeConfig, ClusterStorage, ClusterStateMachine, TempDir>
    for TempDirStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<(TempDir, ClusterStorage, ClusterStateMachine), StorageError<NodeId>> {
        let dir = tempfile::tempdir().expect("a temp dir for the log store");
        let storage = ClusterStorage::open(dir.path())?;
        let mut state_machine = ClusterStateMachine::new();
        state_machine.attach_snapshot_store(storage.snapshot_store())?;
        Ok((dir, storage, state_machine))
    }
}

/// The same store, with every read served by a log reader that was obtained
/// before any write happened.
///
/// This is the shape openraft actually runs in: `get_log_reader` is called once
/// while the node boots and the handle it returns answers every replication and
/// membership read for the rest of the process. A reader that carried its own
/// copy of the log cache would therefore be stale forever after the first
/// truncate — and openraft's own suite never builds a reader, so routing the
/// whole suite through one is what turns the reader into a covered path.
struct ReaderBackedStore {
    /// The handle that owns the writes.
    owner: ClusterStorage,
    /// A reader taken from `owner` before the first write.
    reader: ClusterStorage,
}

impl RaftLogReader<TypeConfig> for ReaderBackedStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        self.reader.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<TypeConfig> for ReaderBackedStore {
    type LogReader = ClusterStorage;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        self.reader.get_log_state().await
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        self.reader.read_vote().await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.owner.get_log_reader().await
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.owner.save_vote(vote).await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.owner.save_committed(committed).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        // Collected rather than forwarded: the trait's `I::IntoIter: Send`
        // bound is not one an impl may restate, and a `Vec`'s iterator is Send
        // unconditionally.
        let entries: Vec<_> = entries.into_iter().collect();
        self.owner.append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.owner.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.owner.purge(log_id).await
    }
}

struct ReaderBackedStoreBuilder;

impl StoreBuilder<TypeConfig, ReaderBackedStore, ClusterStateMachine, TempDir>
    for ReaderBackedStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<(TempDir, ReaderBackedStore, ClusterStateMachine), StorageError<NodeId>> {
        let (dir, mut owner, state_machine) = TempDirStoreBuilder.build().await?;
        let reader = owner.get_log_reader().await;
        Ok((dir, ReaderBackedStore { owner, reader }, state_machine))
    }
}

// ---- the conformance suites ------------------------------------------------

/// openraft's own storage conformance suite, against the real store.
// FM-CLUSTER-103
#[test]
fn openraft_conformance_suite() {
    Suite::test_all(TempDirStoreBuilder).expect("the log store must satisfy openraft's contract");
}

/// The same suite, with every read served by a pre-existing log reader.
// FM-CLUSTER-099
#[test]
fn openraft_conformance_suite_through_a_long_lived_reader() {
    Suite::test_all(ReaderBackedStoreBuilder)
        .expect("a log reader taken at startup must satisfy the same contract as its owner");
}

// ---- the coherence property ------------------------------------------------

/// Cases the coherence property draws by default; raised by `PROPTEST_CASES`
/// the same way [`crate::properties`] raises its own budget.
///
/// Each case opens a RocksDB instance in a fresh temp dir, so a case is
/// milliseconds rather than microseconds and the budget is sized against the
/// crate suite's under-a-minute target rather than against the generator.
const DEFAULT_CASES: u32 = 48;

/// The environment variable that raises [`DEFAULT_CASES`].
const CASES_ENV: &str = "PROPTEST_CASES";

/// Upper bound on operations per generated sequence.
const SEQUENCE_LEN: usize = 12;

fn config() -> ProptestConfig {
    let cases = std::env::var(CASES_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .filter(|cases| *cases > 0)
        .unwrap_or(DEFAULT_CASES);
    ProptestConfig {
        cases,
        ..ProptestConfig::default()
    }
}

/// One step of a generated log-store sequence.
///
/// The parameters are *relative* (how far back from the tail, how many entries)
/// rather than absolute indexes: the interpreter reads the real tail off disk
/// before each step, so a sequence stays meaningful whatever the preceding
/// steps did and the generator never has to model the store.
#[derive(Debug, Clone)]
enum LogOp {
    /// Append `count` fresh entries at the tail.
    Append { count: u64 },
    /// Truncate `back` entries from the tail, then append `count` entries under
    /// a *new* term.
    ///
    /// One op rather than two because this is the whole hazard: a leadership
    /// flap re-appends different content at indexes that were just removed, so
    /// a cache that survived the truncate now disagrees with the bytes on disk
    /// at exactly those indexes. Split into two independent ops the generator
    /// would have to get lucky to line them up.
    TruncateAndReappend { back: u64, count: u64 },
    /// Purge `count` entries from the head.
    Purge { count: u64 },
}

fn arb_op() -> impl Strategy<Value = LogOp> {
    prop_oneof![
        3 => (1u64..=4).prop_map(|count| LogOp::Append { count }),
        3 => (0u64..=3, 0u64..=3)
            .prop_map(|(back, count)| LogOp::TruncateAndReappend { back, count }),
        1 => (1u64..=3).prop_map(|count| LogOp::Purge { count }),
    ]
}

fn arb_ops() -> impl Strategy<Value = Vec<LogOp>> {
    prop::collection::vec(arb_op(), 1..=SEQUENCE_LEN)
}

/// Every log index the `raft_logs` column family currently holds, read straight
/// off RocksDB with the cache bypassed entirely — the oracle both handles are
/// judged against.
fn indexes_on_disk(storage: &ClusterStorage) -> Vec<u64> {
    let cf = storage.cf_logs();
    storage
        .db
        .iterator_cf(&cf, rocksdb::IteratorMode::Start)
        .map(|item| ClusterStorage::decode_log_key(&item.expect("iterating the log CF").0))
        .collect()
}

/// The raw bytes stored at `index`, or `None` when the column family has no
/// such key.
fn bytes_on_disk(storage: &ClusterStorage, index: u64) -> Option<Vec<u8>> {
    let cf = storage.cf_logs();
    storage
        .db
        .get_cf(&cf, ClusterStorage::encode_log_key(index))
        .expect("reading the log CF")
}

/// What a handle serves for a single index, in the same encoding the column
/// family stores — so a mismatch is a byte-for-byte statement about content,
/// not about a summary of it.
async fn served_bytes(handle: &mut ClusterStorage, index: u64) -> Option<Vec<u8>> {
    let entries = handle
        .try_get_log_entries(index..=index)
        .await
        .expect("reading a single index must not fail");
    assert!(
        entries.len() <= 1,
        "a single-index read returned {} entries",
        entries.len()
    );
    entries
        .first()
        .map(|entry| serde_json::to_vec(entry).expect("re-encoding a served entry"))
}

/// Render stored bytes for a failure message.
fn render(bytes: &Option<Vec<u8>>) -> String {
    match bytes {
        None => "nothing".to_string(),
        Some(raw) => String::from_utf8_lossy(raw).into_owned(),
    }
}

/// Both handles agree, index by index, with the column family.
async fn assert_coherent(
    after: &str,
    owner: &mut ClusterStorage,
    reader: &mut ClusterStorage,
    probe_upto: u64,
) {
    for index in 0..=probe_upto {
        let on_disk = bytes_on_disk(owner, index);
        let by_owner = served_bytes(owner, index).await;
        let by_reader = served_bytes(reader, index).await;

        assert_eq!(
            by_owner,
            on_disk,
            "after {after}: at log index {index} the owning handle served {} but the raft_logs \
             column family holds {}",
            render(&by_owner),
            render(&on_disk),
        );
        assert_eq!(
            by_reader,
            on_disk,
            "after {after}: at log index {index} the log reader served {} but the raft_logs \
             column family holds {} — a reader whose cache is not the owner's goes on serving an \
             overwritten term (FM-CLUSTER-099)",
            render(&by_reader),
            render(&on_disk),
        );
    }

    // The same judgment over a whole-log scan rather than point reads: a range
    // read must enumerate exactly the indexes that exist, through either handle.
    let on_disk = indexes_on_disk(owner);
    for (label, handle) in [
        ("the owning handle", &mut *owner),
        ("the log reader", &mut *reader),
    ] {
        let served: Vec<u64> = handle
            .try_get_log_entries(..)
            .await
            .expect("scanning the whole log must not fail")
            .iter()
            .map(|entry| entry.log_id.index)
            .collect();
        assert_eq!(
            served, on_disk,
            "after {after}: a whole-log scan through {label} enumerated {served:?} but the \
             raft_logs column family holds {on_disk:?}"
        );
    }

    // And the tail openraft asks for on every restart.
    let last_on_disk = on_disk.last().copied();
    if let Some(last) = last_on_disk {
        let state = owner
            .get_log_state()
            .await
            .expect("reading the log state must not fail");
        assert_eq!(
            state.last_log_id.map(|log_id| log_id.index),
            Some(last),
            "after {after}: get_log_state named a tail the column family does not end at"
        );
    }
}

/// Build an entry at `index` under `term`, with a payload that depends on both
/// so a re-append at the same index after a flap is distinguishable from the
/// entry it replaced.
fn entry_at_term(index: u64, term: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
        payload: openraft::EntryPayload::Normal(ClusterCommand::RemoveNode {
            node_id: term * 1000 + index,
        }),
    }
}

/// Run one generated sequence, checking coherence after every step.
async fn run_sequence(ops: &[LogOp]) {
    let dir = tempfile::tempdir().expect("a temp dir for the log store");
    let mut owner = ClusterStorage::open(dir.path()).expect("opening the log store");

    // The handle openraft takes once, at boot, before anything has been
    // written — and then keeps for the lifetime of the node.
    let mut reader = owner.get_log_reader().await;

    // Seed a log so the very first truncate has something to remove.
    owner
        .blocking_append(
            (1..=4)
                .map(|index| entry_at_term(index, 1))
                .collect::<Vec<_>>(),
        )
        .await
        .expect("seeding the log");

    let mut term = 1u64;
    let mut probe_upto = 6u64;
    assert_coherent("the seed append", &mut owner, &mut reader, probe_upto).await;

    for (step, op) in ops.iter().enumerate() {
        let present: BTreeSet<u64> = indexes_on_disk(&owner).into_iter().collect();
        let tail = present.last().copied();
        let head = present.first().copied();

        match *op {
            LogOp::Append { count } => {
                let from = tail.map_or(1, |index| index + 1);
                let entries: Vec<_> = (from..from + count)
                    .map(|index| entry_at_term(index, term))
                    .collect();
                owner.blocking_append(entries).await.expect("appending");
                probe_upto = probe_upto.max(from + count + 1);
            }
            LogOp::TruncateAndReappend { back, count } => {
                let Some(tail) = tail else { continue };
                let head = head.unwrap_or(tail);
                // Never truncate below the head: openraft only ever truncates a
                // suffix of what is present, and a `since` below the purged
                // watermark is a case the store is not asked to handle.
                let since = tail.saturating_sub(back).max(head);
                owner
                    .truncate(LogId::new(openraft::CommittedLeaderId::new(term, 1), since))
                    .await
                    .expect("truncating");

                // A new term, because that is what makes the re-appended
                // content differ from what the truncate removed.
                term += 1;
                let from = indexes_on_disk(&owner)
                    .last()
                    .map_or(since, |index| index + 1);
                let entries: Vec<_> = (from..from + count)
                    .map(|index| entry_at_term(index, term))
                    .collect();
                owner.blocking_append(entries).await.expect("re-appending");
                probe_upto = probe_upto.max(from + count + 1);
            }
            LogOp::Purge { count } => {
                let Some(head) = head else { continue };
                let Some(tail) = tail else { continue };
                let upto = (head + count - 1).min(tail);
                owner
                    .purge(LogId::new(openraft::CommittedLeaderId::new(term, 1), upto))
                    .await
                    .expect("purging");
            }
        }

        assert_coherent(
            &format!("step {step} ({op:?})"),
            &mut owner,
            &mut reader,
            probe_upto,
        )
        .await;
    }
}

proptest! {
    #![proptest_config(config())]

    /// A reader taken before the sequence started, and the handle that owns the
    /// writes, both agree with the `raft_logs` column family after every
    /// append, truncate and purge.
    ///
    /// This is the generated form of the two FM-CLUSTER-099 point witnesses.
    /// Those pin one hand-written flap; this quantifies over the sequences
    /// nobody wrote, and — because the oracle is the column family rather than
    /// another read through the same code — a divergence is reported as
    /// "the reader served X, the disk holds Y" rather than as whatever the
    /// state machine does with the wrong entry three layers up.
    // FM-CLUSTER-099
    #[test]
    fn a_reader_and_its_owner_never_disagree_with_the_column_family(ops in arb_ops()) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime");
        runtime.block_on(run_sequence(&ops));
    }
}
