# Proposal: VLL Lock-Table Consolidation

Status: implemented
Date: 2026-07-04

## Problem

The VLL crate's per-shard locking concept — *which transactions intend to touch a
key, and which of them currently hold it* — was one idea stored as two parallel
structures kept in lock-step by caller convention, plus a third vestigial
protocol channel threaded through every layer, plus a coordinator abort path
that confused vector positions with shard ids. Three of the four findings were
hygiene; the fourth was a permanent cross-shard lock leak.

### 1. Correctness bug: `abort_remaining` aborted positions, not shards

`frogdb-server/crates/vll/src/coordinator.rs:387-399` (pre-change):

```rust
async fn abort_remaining(
    &self,
    result_rxs: &[(usize, oneshot::Receiver<S::Response>)],
    txid: u64,
    total: usize,
) {
    // Abort shards we haven't yet sent VllExecute to.
    for shard_id in result_rxs.len() + 1..total {
        self.sink.send_abort(shard_id, txid).await;
    }
}
```

The loop treats *indices into the participant vector* as *shard ids*. That is
only correct when participants happen to be the contiguous set `0..n`. Scatter
participants are the sparse subset of shards the keys hash to — `[2, 5, 7]` is
typical. On a phase-3 partial failure (a shard channel closing mid-dispatch)
the loop:

- aborted **foreign shard ids** — e.g. participant list `[2, 5, 7]` failing at
  position 1 aborted "shard 2", which at that moment was *executing* the
  transaction (the abort is a no-op only because `VllShardState::abort` ignores
  unknown txids, `shard.rs:225`);
- **never aborted the actual lock holders** — shards 5 and 7 kept their
  granted locks and declared intents;
- had an off-by-one that skipped the participant whose dispatch just failed
  (`result_rxs.len() + 1`, not `result_rxs.len()`), so even in the contiguous
  case the failing shard's locks leaked.

**Severity.** Nothing reclaims a leaked op. `VllConfig::timeout_check_interval_ms`
exists in config (`frogdb-server/crates/config/src/vll.rs:23`) but no sweep is
wired to it anywhere in `core` or `vll` — the earlier survey's assumption that
leaked ops "age out" is false. A leaked write intent on a key blocks every later
conflicting transaction on that key *forever* (SCA in `can_proceed` treats any
lower-txid conflicting intent as a blocker), turning one dropped shard channel
into a permanently wedged key range.

### 2. Two parallel truths in the intent table

`intent_table.rs:28-34` (pre-change) held the same concept twice:

```rust
pub struct IntentTable {
    /// Intents indexed by key ... sorted by txid (via BTreeMap inside).
    intents_by_key: HashMap<Bytes, BTreeMap<u64, LockMode>>,
    /// Per-key lock states for actual lock acquisition.
    key_locks: HashMap<Bytes, KeyLockState>,
}
```

`intents_by_key` drove SCA ordering (`can_proceed`); `key_locks` drove
acquisition (`try_acquire_locks`/`release_locks`). Nothing tied them together —
callers had to pair the calls by hand at every transition:

| Transition | Required pairing | Where |
|------------|-----------------|-------|
| enqueue | `declare_intents` + `try_acquire_locks` | `shard.rs:127-139` |
| complete | `release_locks` + `remove_all_intents` | `shard.rs:211-216` |
| abort | `release_locks` *only if* `state == Ready`, `remove_all_intents` always | `shard.rs:228-233` |

The abort row is the tell: the *caller* had to know whether the op held locks
to decide which half of the pairing applied. One missed or misordered pairing
is a silent leak — exactly the class of bug finding 1 produced at the
coordinator level.

### 3. Atomics guarding single-threaded state

`lock_state.rs` (245 lines, deleted) implemented `KeyLockState` as an
`AtomicU8` with `compare_exchange_weak` retry loops, `MAX_READERS = 254`, and
acquire/release orderings — yet every mutation path runs through
`IntentTable::try_acquire_locks(&mut self)` and `VllShardState`'s `&mut self`
methods, owned by a single shard worker. No `Arc`, no `Mutex`, no sharing.
The atomics bought nothing except the implication (false) that this state is
concurrently accessed, and the retry loops could never retry.

### 4. Vestigial `ExecuteSignal` channel

The channel-driven execute protocol was replaced by `VllExecute`
`ShardMessage`s long ago, but its oneshot pair still threaded through six
layers: `ShardSink::send_lock_request` (traits.rs:48-57), the coordinator's
phase-1 channel creation and phase-3 `execute_tx.send(ExecuteSignal { proceed:
true })`, `ShardMessage::VllLockRequest { execute_rx }` (core message.rs:551),
the dispatch match, the shard handler, and `VllPendingOp.execute_rx` — which
queue.rs itself documented as "vestigial": the shard stored the receiver and
dropped it at dequeue without ever reading it. The `proceed` flag was never
consulted anywhere. The phase-3 send doubled as a liveness probe for a
condition (op vanished from the queue before execute) that only the same
coordinator could cause. Every `ShardSink` implementor and every test sink paid
a no-op channel per lock request for this.

## Proposed design

### One structure, three transitions

Replace `IntentTable` + `KeyLockState` with a single `LockTable`
(`frogdb-server/crates/vll/src/lock_table.rs`) in which a granted lock is a
*flag on the intent entry* rather than a separate map:

```rust
struct Intent {
    mode: LockMode,
    granted: bool,
}

pub struct LockTable {
    /// Intents per key, ordered by txid (BTreeMap gives SCA its ordering).
    keys: HashMap<Bytes, BTreeMap<u64, Intent>>,
}
```

The interface is exactly the three lifecycle transitions, and nothing else can
mutate the table:

- `declare(keys, txid, mode)` — register intents (SCA visibility).
- `try_grant(keys, txid) -> bool` — all-or-nothing grant. Checks both SCA (no
  conflicting lower-txid intent, pending or granted) *and* holder
  compatibility (no conflicting granted intent from another txid, regardless
  of order — a higher-txid read may already hold the key when a lower-txid
  write arrives). All keys are checked before any flag is set, so a failed
  grant leaves no partial state; the rollback loop the old
  `try_acquire_locks` needed is gone.
- `release(keys, txid)` — remove the intents. Because holder state is derived
  from `granted` flags, this **single transition serves completion and abort
  alike**: a granted transaction's locks vanish with its intents, and a
  never-granted transaction simply disappears from SCA ordering.

`can_proceed` (SCA) reads the same BTreeMap that `try_grant` writes, so the
two truths cannot diverge — there is no second structure to fall out of
lock-step. The lock-step *invariant* from finding 2 is not enforced better; it
is **deleted**: `VllShardState::abort` no longer inspects `PendingOpState::Ready`
to decide what to release (`shard.rs`), and `release_after_execution` dropped
its `mode` parameter because the table already knows.

Locks are plain data (`bool` on the entry, reader counts derived by iterating
the handful of granted entries per contended key); the `AtomicU8` machinery,
`MAX_READERS`, and `WRITE_LOCKED` sentinels are deleted with `lock_state.rs`.
The deletion test passes: no caller can distinguish plain counters from the
atomics, because no caller ever raced them.

### Coordinator: abort by identity, not position

Phase 3 now carries the participants' real shard ids and aborts the tail slice
on partial failure (`coordinator.rs`):

```rust
let shard_ids: Vec<usize> = ready_rxs.iter().map(|(id, _)| *id).collect();
// ...
for (idx, &shard_id) in shard_ids.iter().enumerate() {
    let (response_tx, response_rx) = oneshot::channel();
    if let Err(err) = self.sink.send_execute(shard_id, request.txid, response_tx).await {
        self.abort_shards(&shard_ids[idx..], request.txid).await;
        // ...
    }
    result_rxs.push((shard_id, response_rx));
}
```

`&shard_ids[idx..]` includes the participant whose dispatch just failed
(fixing the off-by-one) and excludes participants that already received
`VllExecute` (they release their own locks when execution completes). All
abort paths — phase-1 dispatch failure, phase-2 lock failure/timeout, phase-3
partial failure — now funnel through one `abort_shards(&[usize], txid)`
helper; ids are never reconstructed from positions anywhere.

### `ExecuteSignal` deleted

The type, the `VllPendingOp.execute_rx` slot, the `VllLockRequest.execute_rx`
field, the trait parameter, and the coordinator's phase-3 probe are gone;
`send_lock_request` narrowed by one parameter (which also let the
`#[allow(clippy::too_many_arguments)]` go). The scatter choreography is four
phases (lock → ready → execute → gather) instead of five. Losing the liveness
probe costs nothing observable: the only path it guarded now surfaces as
either a `send_execute` failure (shard channel closed → `ShardUnavailable` and
tail abort) or an empty `PartialResult` from `handle_vll_execute`'s unknown-txid
branch — and no actor other than the owning coordinator can remove an op from
the queue in the first place.

### Why this is the right depth

`LockTable` is deeper than `IntentTable` was: its interface shrank (three
transitions + two read-only views instead of eight mutators across two maps)
while its guarantee grew ("release is always safe and complete" vs. "release
is correct if you paired the right calls in the right order under the right
state check"). The queue (`TransactionQueue`) stays a separate module — it
orders *operations* (payload, ready channel, age), while the table orders *key
access*; conflating them would couple the coordinator-facing op lifecycle to
per-key bookkeeping. The seam between them is exactly
`VllShardState::try_acquire_for`, which is now three lines: look up the op,
`try_grant`, signal ready.

## Migration plan

Implemented in three commits, each green on `just fmt / check / lint / test`
for the touched crates:

1. **Bug fix first, with the repro test written before the fix**
   (`fix(vll): abort remaining shards by real id on phase-3 dispatch
   failure`). Test drives a sparse participant set `[2, 5, 7]` through a
   scripted `send_execute` failure on shard 5 and asserts exactly `{5, 7}`
   are aborted; against the old code it failed with `left: [2]`.
2. **Delete `ExecuteSignal`** (`refactor(vll): delete the vestigial
   ExecuteSignal channel`) — trait, coordinator, message enum, dispatch,
   handler, queue slot, adapters, and test sinks narrowed in one pass.
3. **Consolidate the table** (`refactor(vll): consolidate intent + lock maps
   into a single LockTable`) — new `lock_table.rs`; `intent_table.rs` and
   `lock_state.rs` deleted; `VllShardState` rewired; dead `mode` fields
   dropped from `VllPendingOp`/`DequeuedOp` and the
   `release_after_execution` signature.

Host-visible surface changes: `ShardSink::send_lock_request` and
`ShardMessage::VllLockRequest` lost `execute_rx`;
`VllShardState::release_after_execution(txid, keys)` lost `mode`;
`frogdb_core::ExecuteSignal` re-export removed. Diagnostics
(`intent_snapshots`, `VllKeyIntentInfo`, DEBUG VLL output) are unchanged in
shape — `lock_state_string` derives the same `write`/`read:N`/`unlocked`
strings from granted flags.

## Testing impact

- `coordinator.rs`: regression test
  `phase3_failure_aborts_remaining_holders_not_positions` (the leak repro,
  failed before the fix) and
  `phase2_failure_aborts_real_shard_ids_for_sparse_participants`; the test
  sink now records *which* shard ids were aborted instead of counting, so
  foreign aborts can no longer hide inside a correct-looking count.
- `lock_table.rs`: transition-level tests including the two cases the old
  split structure made hard to state — a lower-txid writer blocked by an
  already-granted higher-txid reader (holder check independent of SCA), and
  release-of-ungranted-intent unblocking SCA (the abort path with no state
  inspection). Failed grants are asserted to leave no partial state.
- `shard.rs`: existing acquire/release/abort tests unchanged in intent; new
  `abort_of_pending_op_removes_it_from_sca_ordering` covers aborting an op
  that holds intents but no locks.
- No loom/proptest existed for this crate before and none is added: the shard
  state is single-threaded by construction (finding 3), so the concurrency
  surface is the coordinator's channel choreography, which the scripted-sink
  tests exercise deterministically.

## Risks / open questions

- **No GC remains the status quo.** The fix closes the known coordinator-side
  leak, but any *future* caller that acquires locks and dies without sending
  `VllExecute` or `VllAbort` still leaks until process restart —
  `timeout_check_interval_ms` is still config-only fiction. Wiring an actual
  age-out sweep (and deciding its interaction with in-flight coordinators) is
  a separate proposal-sized decision; this change deliberately did not smuggle
  it in.
- **Reader counts are derived, not cached.** `try_grant` scans a key's intent
  list; per-key contention lists are bounded by queue depth (default 10k) but
  in practice are a handful of entries. If a pathological workload ever makes
  this hot, a cached per-key holder summary can be reintroduced *inside*
  `LockTable` without touching any caller — that locality is the point of the
  consolidation.
- **`MAX_READERS = 254` cap silently removed.** The old `AtomicU8` capped
  concurrent readers at 254 and rejected the 255th; readers are now unbounded
  (queue depth is the real limit). No test or caller depended on the cap.
