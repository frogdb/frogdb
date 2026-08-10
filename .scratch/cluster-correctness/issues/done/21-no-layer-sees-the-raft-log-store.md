# 21 — No validation layer sees the Raft log store, so FM-CLUSTER-099 survives every generated check

Status: done

## Parent

[PRD](../../PRD.md) §6.1 — filed by the retro-validation gate (issue 13). FM-CLUSTER-099
(`get_log_reader` handed out a detached, never-invalidated log cache) is one of the two audit
defects that **no** layer catches when the fix is reverted. §3 W4 owns the closest thing to a
witness; the real owner is a layer that does not exist yet.

## What the experiment showed

Revert (inverse of `4cb1c4b4`, `cluster/src/storage.rs::get_log_reader`):

```rust
// fixed
log_cache: Arc::clone(&self.log_cache),
// reverted
log_cache: Arc::new(RwLock::new(self.log_cache.read().clone())),
```

Result with the two spec forcing tests (`a_log_reader_never_serves_an_entry_the_owner_truncated`,
`cache_invalidation_reaches_a_reader`) excluded from the judgment:

| layer | result |
|---|---|
| L1 invariant catalog + hooks (`just test frogdb-cluster`) | **miss** — 289/291, and the only two failures are the forcing tests |
| L2 properties P1–P4 | structurally out of reach — the properties drive `ClusterStateInner::apply_command`; the log store is below the state machine |
| L3 stateright | structurally out of reach — the models use `apply_command` as the transition function and treat Raft as an ordering oracle |
| L4 seeded schedules | **miss** — `just cluster-seeds 100` green (87.9 s); `just cluster-seeds 500` (`CLUSTER_SEEDS_JOBS=6`, 346.6 s) reproduced exactly the 36 known issue-20 `XNODE-SLOT-1` seeds and nothing else |
| seam gates (`just lint-gates`) | green |

So the defect class — Raft log divergence produced entirely below the state machine — is
invisible to the whole PRD. The 500-seed sweep is the strongest evidence: the leadership flap
that the FM row names as the trigger (append while leader → step down → truncate → re-append at
the same indexes → read through the long-lived reader) does happen in those schedules, but
nothing in the harness compares what a reader serves against what is on disk, so a divergent
read is only visible if it later detonates client-side inside the same run.

## Why the existing layers cannot be stretched to cover it

- W1's catalog is by design pure over `&ClusterStateInner`, with no I/O. A log-store invariant
  is not expressible there without breaking that property.
- W2/W3 sit on the same state machine and inherit the same boundary.
- W4 exercises the store for real but only observes it through client-visible outcomes.

## What to build

An **openraft storage conformance layer** for `ClusterLogStore` — openraft ships
`openraft::testing::Suite` (log-store + state-machine conformance) precisely for this seam, and
the FM-CLUSTER-099 note already records that "nothing outside openraft itself had ever exercised
the reader clone". Concretely:

1. Run openraft's storage test suite against a temp-dir `ClusterLogStore` in `frogdb-cluster`
   (so the mutation gate sees it), including the reader-handle cases.
2. Add the coherence properties the suite does not state, as a small proptest over an
   append/truncate/purge/read command sequence executed against *both* the owning handle and a
   reader obtained from `get_log_reader` before the sequence started: every read through either
   handle must agree with the on-disk column family. That is the generated form of the two
   point witnesses and it would have failed on the first shrink.
3. Feed the same shape into the vote/meta path so FM-CLUSTER-098's *behavioral* half (as
   opposed to the classification lint, see issue 22 sibling note) gets a generated witness too,
   once the campaign-2 crash harness can cut power between the ack and the restart.

## Acceptance criteria

- Reverting the FM-CLUSTER-099 fix makes at least one **non-forcing** test fail, and the
  failure names the divergence (reader index vs. disk content), not a downstream symptom.
- The new tests live in `frogdb-cluster` so `cargo mutants -p frogdb-cluster` scores them.
- `just test frogdb-cluster` stays green on a clean tree and the added runtime is under a
  minute.

## Resolution

Built, in `frogdb-server/crates/cluster/src/storage/conformance.rs` — a child module of `storage`
rather than a sibling, because the oracle it judges against is the `raft_logs` column family
itself and reaching it means reaching private state (`db`, `cf_logs`, the key codec).

**Item 1 — openraft's suite.** `openraft::testing::Suite::test_all` (openraft 0.9.21, ~32 cases
covering vote, append, truncate, purge, log state, snapshot and the state machine) runs against a
temp-dir `ClusterStorage` via a `StoreBuilder`. `Suite::run_fut` builds its own tokio runtime per
case, so these are plain `#[test]`s, not `#[tokio::test]`s.

**Item 1, reader half.** openraft's suite never calls `get_log_reader`, so the whole suite is run a
second time through a `ReaderBackedStore`: writes and `get_log_reader` go to the owning handle,
every *read* (`try_get_log_entries`, `get_log_state`, `read_vote`) goes to a reader obtained before
the first write. That is what the issue meant by "including the reader-handle cases" — the cases
exist, but only if something puts a reader under them.

**Item 2 — the coherence property.** `a_reader_and_its_owner_never_disagree_with_the_column_family`
generates sequences of up to twelve `Append` / `TruncateAndReappend` / `Purge` operations, runs
them against the owner while a reader taken before the first write watches, and after *every* step
compares, per index, what the owner serves and what the reader serves against the bytes in
`raft_logs` — plus a whole-log index scan through both handles and a `get_log_state` tail check.
Re-appends carry a bumped term and a payload derived from term and index, so an entry from an
overwritten term is distinguishable from its replacement rather than merely equal-shaped.

**Item 3 — out of scope, deliberately.** Feeding the same shape into the vote/meta path needs power
cut between the ack and the restart, which is campaign-2 crash-harness capability that does not
exist yet. FM-CLUSTER-098's behavioural half stays uncovered; its classification half is the lint
already in place. Not carried as a new issue — it is the sibling note in issue 22 and belongs to the
crash harness, not here.

### Acceptance: the revert experiment

`get_log_reader` reverted to `log_cache: Arc::new(RwLock::new(self.log_cache.read().clone()))`, and
the two spec forcing tests excluded from the judgment exactly as the original experiment did:

```
Summary [14.147s] 292 tests run: 291 passed, 1 failed, 7 skipped
  FAIL storage::conformance::a_reader_and_its_owner_never_disagree_with_the_column_family
```

shrunk to a single operation, naming the divergence rather than a downstream symptom:

```
after step 0 (TruncateAndReappend { back: 0, count: 1 }): at log index 4 the log reader served
{"log_id":{"leader_id":{"term":1,...},"index":4},"payload":{"Normal":{"RemoveNode":{"node_id":1004}}}}
but the raft_logs column family holds
{"log_id":{"leader_id":{"term":2,...},"index":4},"payload":{"Normal":{"RemoveNode":{"node_id":2004}}}}
 — a reader whose cache is not the owner's goes on serving an overwritten term (FM-CLUSTER-099)
```

Fix restored, tree verified clean, and the proptest regression seed the failing run wrote was
deleted rather than committed (it is a seed for a defect that no longer exists).

Worth recording which half did the catching: the **property** caught it, the reader-backed
**suite** did not. openraft's cases never read an index, overwrite it, and read it again through
the same handle, which is the interleaving a detached cache needs. So the reader-backed suite is
conformance coverage for a path that had none, and the property is the discriminating witness. Both
are on FM-CLUSTER-099's `Forced by`, with that distinction written into the row's prose.

### What the layer found on its first run

`Suite::delete_logs_since_0` failed immediately: `assertion left == right failed; left: 1,
right: 0`. openraft's `truncate(log_id)` is contracted as "truncate logs since `log_id`,
**inclusive**", and `ClusterStorage::truncate` deleted only `index > log_id.index`, in RocksDB and
in the cache alike. Every FrogDB test that had ever touched `truncate` was written against the
store's own exclusive reading, so the off-by-one was self-consistent below openraft and invisible
above it — which is the claim this issue was filed to make, arriving a good deal more concretely
than expected.

It matters because both shapes of `DeleteConflictLog` reach that path. When an `AppendEntries`
batch disagrees at `since`, the leader's entries are re-appended immediately and overwrite the
survivor. When `prev_log_id` does not match, `ensure_log_consecutive` truncates and then *rejects*
the append: nothing is written afterwards, the survivor stays, and the next open hands openraft a
`last_log_id` from a term the leader already ruled conflicting.

Filed as **FM-CLUSTER-103** and fixed in the same branch (one bound now drives both the key range
and the cache invalidation). `truncate_drops_only_the_tail_after_the_kept_index` encoded the old
reading; it is rewritten as `truncate_removes_the_named_index_and_everything_after_it`, moved off
FM-CLUSTER-017 onto the new row, and extended with a reopen. No caller of `truncate` exists outside
`storage.rs`, so the blast radius is the store.

### Gates

| gate | result |
|---|---|
| `just test frogdb-cluster` | 294/294 passed, 5 skipped, 14.4 s wall (was 291 tests). Added: two suite runs at ~13 s each and the property at ~5.6 s, all three in parallel with the rest — well under the one-minute budget. |
| `just lint-failure-modes` | OK — 279 failure modes, 1399 test references, 1399 tags |
| `just scratch-check` | OK |
| `just mutants-diff frogdb-cluster` | see the commit that closes this issue |
