# Proposal 45 — VLL key ownership moves inside `VllShardState`; diagnostics collapse to one call

*Round 38. Candidates TX1 (phase 1) + TX13 (phase 2) of the txn+vll+scripting lane. One
proposal because phase 2 depends on phase 1: the ownership map changes what the diagnostics
can truthfully report.*

## Summary (2-3 sentences)

`VllShardState` hands a dequeued op's lock keys **out** to the host worker and then requires the
host to hand the same keys **back** (`release_after_execution(txid, &keys)`), tracking outstanding
ops as a bare `usize` that `saturating_sub`s on release — an interface whose central invariant
("release exactly once, with exactly the keys you were given") the module cannot check and the
compiler cannot enforce, with the whole failure mode written down in the spec instead
([FM-VLL-005](../../hardening/specs/vll-failure-modes.md#fm-vll-005--a-granted-op-panics-while-executing)).
Phase 1 moves that fact inside: `executing: HashMap<u64, Vec<Bytes>>` replaces the counter, `release(txid)`
looks the keys up, and the misuse becomes unrepresentable rather than merely tested-for. Phase 2 then
collapses six diagnostic entry points plus two `LockTable` diag-only methods into a single
`diagnostics()` call — which is only worth doing after phase 1, because phase 1 is what makes
`VllQueueInfo.executing_txid` reportable at all (**verified: it is `None` 100% of the time today —
a live `DEBUG VLL` under-report**).

## Files involved (verified paths + current line counts)

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/vll/src/shard.rs` | 1159 | **Owner of both phases.** `VllShardState` struct (L68–81, `executing_ops: usize` at L79); `dequeue_for_execution` (L226–238); `release_after_execution` (L250–257); `is_drained` (L334–336); the five public introspection methods (L415–463); `DequeuedOp` (L479–483) and the three pub-field snapshot structs (L486–508). |
| `frogdb-server/crates/vll/src/lock_table.rs` | 343 | `LockTable::declare`/`try_grant`/`release` (the intent store phase 1's map shadows), plus the two diag-only methods `iter_keys` (L150–154) and `lock_state_string` (L157–173). |
| `frogdb-server/crates/vll/src/queue.rs` | 299 | `VllPendingOp` (owns `keys: Vec<Bytes>`, L20–33) and `TransactionQueue::dequeue` (L122–124, `self.pending.remove(&txid)`) — the removal that strands the `Executing` state write. |
| `frogdb-server/crates/vll/src/lib.rs` | 28 | Re-exports `ContinuationLockSnapshot`, `IntentSnapshot`, `PendingOpSnapshot` (L22–25) — phase 2 shrinks this list. |
| `frogdb-server/crates/core/src/shard/vll.rs` | 290 | **The only production caller.** `handle_vll_execute` (L40–75): `dequeue_for_execution` at L45, the panic guard at L57–58, `release_after_execution(op.txid, &op.keys)` at L72. |
| `frogdb-server/crates/core/src/shard/diagnostics.rs` | 598 | **The only production diagnostics consumer.** `collect_vll_queue_info` (L167–204) and `collect_lock_table_info` (L207–231) — between them they make five separate calls into `VllShardState`. |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | `can_execute_during_lock` (L854–861) — the **non**-diagnostic reader of `continuation_lock_owner()`. See the discrepancy note below. |
| `frogdb-server/crates/core/src/shard/panic_guard.rs` | 564 | FM-VLL-005's only forcing test, `a_panicking_vll_op_releases_its_locks_and_the_shard_keeps_serving` (tag at L483, fn at L495), which asserts through `collect_lock_table_info()` (L534) — so it is a phase-2 consumer too. |
| `frogdb-server/crates/core/src/shard/types.rs` | 1498 | `VllQueueInfo.executing_txid: Option<u64>` (L987) — the always-`None` field. |
| `frogdb-server/crates/server/src/connection/debug_conn_command.rs` | 1445 | `format_vll_response` (L535–…) reads `info.executing_txid` at L550–551 — the dead render branch. |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED spec. Scope note (L20–23), FM-VLL-001 Invariant (L46), FM-VLL-003 NOT-observable (L69), FM-VLL-005 NOT-observable + Invariant (L105–106). All four need edits — enumerated below. |

## Problem (concrete verified evidence)

### 1. The key list is an out-parameter the caller must not lose

`dequeue_for_execution` moves the op's keys out of the queue and into a caller-owned struct
(`shard.rs:226-238`, `DequeuedOp` at `shard.rs:479-483`):

```rust
pub fn dequeue_for_execution(&mut self, txid: u64) -> Option<DequeuedOp<O>> {
    let tx_queue = self.tx_queue.as_mut()?;
    let mut op = tx_queue.dequeue(txid)?;          // removed from the queue outright
    op.state = PendingOpState::Executing;
    self.executing_ops += 1;                       // ...but only a COUNT stays behind
    Some(DequeuedOp { txid: op.txid, keys: op.keys, operation: op.operation })
}
```

and the release requires them back (`shard.rs:250-257`):

```rust
pub fn release_after_execution(&mut self, txid: u64, keys: &[Bytes]) {
    self.executing_ops = self.executing_ops.saturating_sub(1);
    if let Some(lock_table) = self.lock_table.as_mut() { lock_table.release(keys, txid); }
    self.try_advance_pending_locks();
    self.try_grant_pending_continuation();
}
```

Between those two calls the shard's most safety-critical fact — *which keys is txid still holding*
— lives **outside** the state machine, in a value the host can drop, truncate, reorder, or pass with
a mismatched `txid`. Nothing in the interface prevents any of it:

| Misuse | What the module does today | Consequence |
| --- | --- | --- |
| Never call `release_after_execution` | nothing | intents leak; `executing_ops` stays ≥1 so `is_drained()` (`shard.rs:334-336`) is false **forever** → every later continuation-lock request parks and times out (FM-VLL-003), every SCA request is refused (FM-VLL-001/004) |
| Call it twice | `saturating_sub` floors at 0; the second `lock_table.release` is a no-op | the count silently under-reports; a *different* op's outstanding-ness is erased → premature "drained", continuation lock granted over a live op |
| Call it with a short/edited key slice | releases only those keys | the omitted keys stay locked with no owner |
| Call it with the wrong `txid` | `LockTable::release` (`lock_table.rs:131-140`) is a total no-op for unknown `(key, txid)` | silent; nothing is released, nothing complains |

The doc comment states the contract in prose and the `saturating_sub` is explicitly a damage limiter
for its violation (`shard.rs:247-249`: *"Pairs 1:1 with `dequeue_for_execution`; `saturating_sub`
keeps an unpaired call from wrapping the outstanding-op count"*). A saturating operator defending
against your own caller is the tell: the interface is shallow at exactly the point where it should
be deep.

The spec agrees, at length. The scope section (`vll-failure-modes.md:20-23`) carves out an explicit
exception to the file's own scope rule for this one obligation:

> One caller-side obligation is in scope even though it lives outside those two files: the shard
> worker must pair every `dequeue_for_execution` with a `release_after_execution` … **The lock table
> cannot enforce this from the inside** — a caller that never returns leaves the entry held forever.

That sentence is a design defect written down as a specification. It is only true because the keys
left the module.

**Verified caller census.** `release_after_execution` has exactly **one** production caller
(`frogdb-server/crates/core/src/shard/vll.rs:72`) and **eight** test callers, all inside
`vll/src/shard.rs` (L546, 591, 666, 709, 792, 836, 888, 904). `dequeue_for_execution` mirrors it
exactly: one production caller (`core/src/shard/vll.rs:45`), eight tests (L545, 590, 665, 708, 779,
835, 887, 903). So the *cost* of narrowing the interface is one production edit — and the whole
`FM-VLL-005` structural argument exists to protect that single call site.

### 2. The one caller pays for the leak with a hand-written panic guard

Because the release is the host's obligation, the host must also make it unwind-safe itself
(`core/src/shard/vll.rs:49-58`, comment abridged):

> Panic isolation (c2-07). The release below is the whole reason this site needs its own guard
> rather than relying on the outer net: an unwind past it leaks the op's key locks *and* leaves
> `executing_ops` incremented, which permanently blocks every later request on those keys and any
> parked continuation lock.

A guard at the call site is the right fix *given* the interface. With the keys owned by the state
machine, a much stronger option opens up (a drop-guard or a `run_dequeued` closure inside
`frogdb-vll`) — see phase 1 variant (b).

### 3. `PendingOpState::Executing` is written to a value nobody can read → `DEBUG VLL` under-reports

This is a **live observability defect**, verified end to end:

- `TransactionQueue::dequeue` is `self.pending.remove(&txid)` (`queue.rs:122-124`) — the op is gone
  from the queue.
- `dequeue_for_execution` sets `op.state = PendingOpState::Executing` (`shard.rs:229`) on that
  already-removed value, which is then moved field-by-field into `DequeuedOp` — and `DequeuedOp`
  (`shard.rs:479-483`) **has no `state` field**. The write is dead on arrival.
- `iter_pending_ops` (`shard.rs:425-437`) iterates `self.tx_queue` only, so it can never yield a
  snapshot whose `state` is `Executing`.
- Therefore `diagnostics.rs:175`'s branch
  `if snap.state == crate::vll::PendingOpState::Executing { info.executing_txid = Some(snap.txid); }`
  is unreachable, `VllQueueInfo.executing_txid` (`types.rs:987`) is **always `None`**, and
  `debug_conn_command.rs:550-551`'s ` executing_txid:{}` suffix never renders.
- `grep executing_txid` finds **four** hits repo-wide (`types.rs:987`, `diagnostics.rs:176`,
  `debug_conn_command.rs:550,551`) and **zero** assertions. Nothing tests it, which is why 100% on
  the frogdb-vll mutation gate did not surface it: the field is only ever written by dead code, so
  there is no behaviour for a mutant to break.

An operator running `DEBUG VLL` on a shard that is stuck precisely because an op is executing sees
`queue_depth:0` and no executing txid — i.e. the display is *most* wrong exactly when it matters
most. Phase 1's `executing` map is what makes this field truthful, which is why the lane orders
TX13 after TX1.

### 4. Six diagnostic entry points + two `LockTable` methods, two production consumers

Verified surface (the lane's "7 introspection entry points"):

| # | Entry point | Defined | Production readers |
| --- | --- | --- | --- |
| 1 | `VllShardState::queue_depth()` | `shard.rs:420` | `diagnostics.rs:170` |
| 2 | `VllShardState::iter_pending_ops()` | `shard.rs:425` | `diagnostics.rs:174` |
| 3 | `VllShardState::continuation_lock_snapshot()` | `shard.rs:440` | `diagnostics.rs:187`, `:220` |
| 4 | `VllShardState::intent_snapshots()` | `shard.rs:451` | `diagnostics.rs:195`, `:210` |
| 5 | `VllShardState::continuation_lock_owner()` | `shard.rs:415` | `worker.rs:856` — **control path, not diagnostics** |
| 6 | `LockTable::iter_keys()` | `lock_table.rs:150` | `shard.rs:456` only |
| 7 | `LockTable::lock_state_string()` | `lock_table.rs:157` | `shard.rs:460` only |

plus three all-`pub`-field snapshot structs (`PendingOpSnapshot` L486, `ContinuationLockSnapshot`
L496, `IntentSnapshot` L504), all re-exported from `lib.rs:22-25`.

Two production functions consume the whole group, and both live in one file:
`collect_vll_queue_info` (`diagnostics.rs:167-204`, feeds `DEBUG VLL`) and `collect_lock_table_info`
(`diagnostics.rs:207-231`, feeds `DEBUG LOCKTABLE`). They overlap: `collect_lock_table_info` is a
strict subset of `collect_vll_queue_info` minus the queue half, and both re-walk
`intent_snapshots()` → `VllKeyIntentInfo` with **byte-identical** mapping code (L195-201 vs
L210-217). Meanwhile `iter_keys`/`lock_state_string` are `pub` on `LockTable` purely so `shard.rs`
two lines away can build `IntentSnapshot` — a crate-internal detail promoted to public API by
accident of module layout.

**Depth reading.** Each of these methods is a thin projection of one private field. A caller wanting
a coherent picture of the shard must learn six names, call them in an order nobody documents, and
know that they are not atomic with respect to each other (they are today, only because the caller
holds `&self` on a single-threaded worker — an invariant the interface never states). That is the
textbook shallow interface: nearly as much surface as implementation, and no leverage — the caller
does the assembly.

## Proposed change

### Phase 1 — key ownership moves inside `VllShardState`

Replace the counter with the map the counter was standing in for:

```rust
pub struct VllShardState<O: Debug> {
    lock_table: Option<LockTable>,
    tx_queue: Option<TransactionQueue<O>>,
    continuation_lock: Option<ContinuationLock>,
    pending_continuation_release: Option<oneshot::Receiver<()>>,
    pending_continuation: Option<PendingContinuation>,
    /// Ops handed to the host by `dequeue_for_execution` that have not yet
    /// reported back. The state machine keeps their keys: the host is not
    /// trusted to return them, and cannot.
    executing: HashMap<u64, Vec<Bytes>>,
    max_queue_depth: usize,
}
```

- `dequeue_for_execution(txid)` inserts `txid → keys` into `executing` before handing the op out.
- `release_after_execution(txid)` — **one argument** — takes the entry, releases exactly those keys
  through `LockTable::release`, then runs the existing `try_advance_pending_locks()` +
  `try_grant_pending_continuation()` drain points. An unknown `txid` removes nothing and releases
  nothing (unchanged observable behaviour), but is now *detectable*: the method can return
  `bool`/`Option<()>` and carry a `debug_assert!`.
- `is_drained()` becomes `self.executing.is_empty() && …` — `saturating_sub` and the entire
  wrap-around hazard it guards disappear, because there is no arithmetic left.

**The keys the host still needs.** `handle_vll_execute` genuinely uses the key slice twice, for
`execute_scatter_part(&op.keys, &op.operation, 0)` (`execution.rs:713`, takes `&mut self`) and for
`scatter_error_reply(&op.operation, &op.keys, err)` (`dispatch_core.rs:193`). Because
`execute_scatter_part` takes `&mut self`, the host cannot hold a borrow into `self.vll` across it,
so `DequeuedOp` keeps an owned `keys: Vec<Bytes>` — now a **clone** (one `Vec` allocation plus N
`Bytes` refcount bumps per VLL execute; `Bytes` clone is a refcount increment, not a copy). That
clone is the entire price of the fix, and it is paid once per scatter part on a path that already
does a store round-trip and a `PartialResult` allocation.

Two variants, both compatible:

- **(a) minimal** — signature becomes `release_after_execution(&mut self, txid: u64)`; the host's
  one call site loses its second argument. Smallest diff, ships the whole misuse-proofing.
- **(b) stronger, recommended if the host refactor is acceptable** — additionally expose
  `run_dequeued(txid, f)` (or return a drop-guard) so the release cannot be *forgotten* either, not
  just mis-argued. This is what would let `core/src/shard/vll.rs`'s bespoke panic-guard comment
  (L49-56) shrink to "the guard lives in frogdb-vll", and turns FM-VLL-005's invariant from a
  caller obligation into a module property. Note the interaction with sibling **52** below before
  choosing (b).

**Rejected alternative:** a `by_txid: HashMap<u64, Vec<Bytes>>` reverse index inside `LockTable`
instead. It also removes the out-parameter, but it does **not** replace `executing_ops` — the drain
predicate needs "dequeued but not yet released", which is not the same set as "holds intents"
(a queued-but-ungranted op holds intents too). The `VllShardState` map does both jobs with one
field, so it is the better shape.

### Phase 2 — one `diagnostics()` call (lands after phase 1)

```rust
pub struct VllShardDiagnostics<'a, O> {
    pub queue_depth: usize,
    pub executing_txids: Vec<u64>,                          // NEW — phase 1 makes this real
    pub pending_ops: Vec<PendingOpSnapshot<'a, O>>,
    pub continuation_lock: Option<ContinuationLockSnapshot>,
    pub intents: Vec<IntentSnapshot>,
}

impl<O: Debug> VllShardState<O> {
    /// The complete introspection surface, consistent as of one instant.
    pub fn diagnostics(&self) -> VllShardDiagnostics<'_, O> { … }
}
```

Consequences, all verified against the census above:

- `queue_depth`, `iter_pending_ops`, `continuation_lock_snapshot`, `intent_snapshots` become
  private (their only production readers are `diagnostics.rs`, which now makes one call).
- `LockTable::iter_keys` and `lock_state_string` drop to `pub(crate)` or private — their only
  non-test callers are `shard.rs:456` and `:460`, inside the same crate. Their unit tests
  (`lock_table.rs:190-341`) are in-module and keep working unchanged.
- `lib.rs:22-25` re-exports `VllShardDiagnostics` instead of the three loose snapshot structs
  (`PendingOpSnapshot` still needs export as a field type; `ContinuationLockSnapshot` and
  `IntentSnapshot` likewise — the win is one entry point, not fewer types).
- `diagnostics.rs`'s two collectors become one `let d = self.vll.diagnostics();` plus two
  projections, and the duplicated `IntentSnapshot → VllKeyIntentInfo` mapping (L195-201 ≡ L210-217)
  collapses to one helper.
- `executing_txid` is populated from `d.executing_txids` and the dead `state == Executing` branch
  (`diagnostics.rs:175`) is deleted. If `PendingOpState::Executing` then has no remaining reader
  beyond `format!("{:?}", …)`, delete the variant and the `shard.rs:229` write with it — or keep the
  variant and give it a real reader by having `diagnostics()` synthesize `Executing` entries from
  the `executing` map. **Recommend the latter**: `DEBUG VLL` showing the executing op alongside the
  queued ones is what an operator actually wants, and it is only possible after phase 1.

**`continuation_lock_owner()` deliberately stays.** It is not diagnostics: `worker.rs:856` uses it
as the `can_execute_during_lock` gate. Better still, phase 2 should push that comparison *in* —
`VllShardState::may_execute(conn_id) -> bool` — so the shard-exclusivity rule stops being
"read the owner, compare it yourself" at a call site outside the crate. That is a small extra depth
win in the same edit; the accessor can then go private too.

## Before / After

```rust
// BEFORE — core/src/shard/vll.rs:45..72
let Some(op) = self.vll.dequeue_for_execution(txid) else { … };
let outcome = panic_guard::caught(self.execute_scatter_part(&op.keys, &op.operation, 0)).await;
…
self.vll.release_after_execution(op.txid, &op.keys);   // ← the keys round-trip

// AFTER (variant a)
self.vll.release_after_execution(op.txid);             // keys never left the module

// AFTER (variant b)
let result = self.vll.run_dequeued(txid, |keys, operation| { … }).await;  // release is structural
```

```rust
// BEFORE — diagnostics.rs:167..231, five calls across two collectors
queue_depth: self.vll.queue_depth(),
for snap in self.vll.iter_pending_ops() { … }
if let Some(lock) = self.vll.continuation_lock_snapshot() { … }
for snap in self.vll.intent_snapshots() { … }
// … and again in collect_lock_table_info: intent_snapshots() + continuation_lock_snapshot()

// AFTER — one call, one instant, one mapping helper
let d = self.vll.diagnostics();
```

## Testability improvement

**Phase 1 removes a bug class instead of testing for it.** The four misuse rows in the table above
are not currently testable at all from `frogdb-vll` — they are *caller* mistakes in `frogdb-core`,
so a `frogdb-vll` test cannot construct them and a `frogdb-core` test would have to fake a wrong
call deliberately. After phase 1 there is no wrong call to make: `release_after_execution(txid)`
takes no key list, so "wrong keys" and "short keys" cease to exist as states, and "released twice"
becomes an observable no-op the module can assert on. The remaining risk — *never* releasing — is
either still caller-side (variant a) or structurally impossible (variant b).

**Phase 1 relocates FM-VLL-005's evidence into the crate that is gated.** Today FM-VLL-005's only
forcing test lives at `core/src/shard/panic_guard.rs:495` — i.e. in `frogdb-core`, which
`cargo mutants -p frogdb-vll` never runs (CLAUDE.md: *"Put the forcing test in the mutated crate"*).
So the row that describes the lock-leak failure mode contributes **nothing** to the 0.90 gate on the
crate that owns the lock table. With the keys inside `VllShardState`, a `frogdb-vll`-local test can
force it directly: dequeue, drop the `DequeuedOp` without releasing, assert `is_drained()` is false
and the intent is still held; then release by txid alone and assert both clear. Under variant (b),
force it with a panicking closure inside `run_dequeued`.

**Phase 1 makes the executing set assertable.** `executing_txids` gives the first test that can
distinguish "queue empty" from "shard drained" through the public interface — the very distinction
FM-VLL-003 turns on (`vll-failure-modes.md:69`, *"the lock granted … over a dequeued op still
executing"*). Today `continuation_lock_parks_while_a_dequeued_op_is_still_executing`
(`shard.rs:769-795`) proves it only indirectly, by observing that the parked request stays parked.

**Phase 2 makes diagnostics a single assertion.** `diagnostic_snapshots_reflect_state`
(`shard.rs:1143-1157`) currently makes five separate calls and asserts on each; it becomes one call
and one struct comparison, and gains a real assertion for `executing_txids`. The dead
`executing_txid` render path gets its first-ever test.

## Risks / scope boundaries vs siblings

**All four round-38 vll proposals touch `frogdb-vll/src/shard.rs`. File ownership, to keep the
merge honest:**

| Proposal | Owns (must not be edited by the others) | Overlap with 45 |
| --- | --- | --- |
| **45** (this) | `shard.rs` struct fields L68–81 + `dequeue_for_execution` L226–238 + `release_after_execution` L250–257 + `is_drained` L334–336 + introspection block L415–463 + `DequeuedOp` L479–483; `lock_table.rs` L150–173 visibility; `core/src/shard/diagnostics.rs` L167–231; `core/src/shard/vll.rs` L72 | — |
| **46** vll-acquire-error-unify | `vll/src/types.rs` (`VllError` → one `to_response`), `server/src/scatter/executor.rs:155-157`, `server/src/connection/scripting/eval.rs:268-281` | **None.** 45 touches no error type and no reply mapping. Safe to land in either order. |
| **51** txn-slot-vll-state-small | `txn/src/state.rs:74-115` (promotion lattice) **and** `vll/src/shard.rs:103-114` (`ensure_initialized`, dropping the `Option<LockTable>` / `Option<TransactionQueue>`) | **CONFLICT (textual, same struct).** 51 rewrites the same field block 45 edits (L68–81 / L92–100) and every `self.lock_table.as_mut()` / `self.tx_queue.as_ref()` site — including the ones inside `release_after_execution` and `is_drained`. Both changes are additive in intent (45 replaces one field, 51 unwraps two others) but the diffs will not auto-merge. **Land 51 first** (it is S and mechanical), then 45 on top; 45's map is unaffected by whether the neighbours are `Option`al. 51 also owns the FM-VLL-001 Invariant edit at `vll-failure-modes.md:46`, which names `ensure_initialized` — 45 must not touch that row. |
| **52** vll-unknown-txid-refusal | `core/src/shard/vll.rs:40-75` (`handle_vll_execute`'s `else` arm → typed refusal), new FM-VLL-006 row | **CONFLICT (same function, adjacent lines).** 52 rewrites the `dequeue_for_execution` → `None` branch at L45–48; 45 rewrites the release at L72 (variant a) or the whole body (variant b). **Land 52 first under variant (a)** — the two edits are then a few lines apart and merge cleanly. Under **variant (b)** the function is restructured around `run_dequeued`, which swallows 52's `else` arm entirely; in that case 45 and 52 must be a single change, or (b) deferred to a follow-up issue. This is the main reason variant (a) is the safe default. |

**Locked-area landing steps (frogdb-vll, gate 0.90):**

1. **Spec-first is not required for phase 1** — no observable behaviour changes (same locks
   released, same drain points, same `ShardReadyResult`s). But **four spec edits are required as
   part of the same change**, because the rows describe the mechanism by name:
   - `vll-failure-modes.md:20-23` (Scope) — the "caller-side obligation … the lock table cannot
     enforce this from the inside" carve-out becomes **false** and must be rewritten to say the
     state machine owns the keys and releases by txid (variant b: delete the carve-out entirely).
   - `:105` (FM-VLL-005 NOT observable) — `executing_ops` staying incremented → `executing` still
     holding the txid.
   - `:106` (FM-VLL-005 Invariant) — `release_after_execution` no longer takes keys; under variant
     (b), the pairing is enforced by the module, not by `handle_vll_execute`'s guard.
   - `:69` (FM-VLL-003 NOT observable) — names `continuation_lock_owner()` and
     `continuation_lock_snapshot()`; **phase 2** makes the latter private, so the row must name
     `diagnostics().continuation_lock` (and `may_execute()` if that lands). Phase 2 also implies a
     `Forced by` review for the rows whose tests assert through those accessors.
   - Do **not** touch `:46` (FM-VLL-001) — that row belongs to sibling 51.
2. `just lint-failure-modes` after every spec edit (it is part of `just lint`, and it verifies both
   directions: every `Forced by` name resolves to a real test, every `// FM-VLL-NNN` tag matches a
   row).
3. New forcing tests go in **`frogdb-vll`**, not `frogdb-core` — see the testability section; this
   is the change that finally gives FM-VLL-005 mutation weight in the crate that is gated.
4. `just mutants-diff frogdb-vll` before pushing (push discipline); full `just mutants frogdb-vll`
   + `just mutants-gate frogdb-vll 0.90` for the re-gate. Phase 2's method deletions *reduce* the
   mutable surface, so the score should hold or improve; phase 1 adds `HashMap` operations that need
   real assertions behind them.
5. **`frogdb-txn` is untouched** by both phases — no `frogdb-txn` re-gate needed, even though the
   locked *area* nominally spans both crates.

**Other risks:**

- **Clone cost.** One `Vec<Bytes>` clone per VLL execute (see phase 1). Real but small; call it out
  in the PR rather than hiding it. If it ever matters, the map can hold `Arc<[Bytes]>` shared with
  `DequeuedOp` — deliberately not proposed now (premature).
- **`abort(txid)` is not a release path for executing ops.** `abort` (`shard.rs:264-279`) dequeues
  from `tx_queue` and returns early if the txid is not there — so an op that is already *executing*
  cannot be aborted, today or after this change. Phase 1 does not change that, and should not: the
  host is mid-`execute_scatter_part` and the keys must stay held. Worth a one-line comment on the
  `executing` map so a future reader does not "fix" it.
- **`DEBUG VLL` output changes.** Once `executing_txid` starts being populated, any golden/snapshot
  test of `DEBUG VLL` output shifts. Verified there are **none** today (zero assertions on
  `executing_txid` repo-wide) — but this is a user-visible improvement to a debug command, so it
  belongs in the PR description and, if `DEBUG VLL` is documented on the website, in a docs pass.
- **Phase 2 is a `frogdb-vll` public-API break.** Pre-production, and the only external consumer is
  `frogdb-core` (verified: no other crate imports `PendingOpSnapshot`/`IntentSnapshot`/
  `ContinuationLockSnapshot`), so this is a two-file change, not a sweep.

## Effort estimate

- **Phase 1 (variant a): S–M.** One struct field, three method bodies (`dequeue_for_execution`,
  `release_after_execution`, `is_drained`), one production call site, eight in-crate test call sites
  (mechanical: drop the second argument), three spec-row edits, plus one new `frogdb-vll` forcing
  test. The mutation re-gate is the long pole, not the diff.
- **Phase 1 (variant b): M.** Adds a closure/guard API and restructures `handle_vll_execute` around
  it, which collides with sibling 52 — see the table. Recommend filing as a follow-up issue rather
  than bundling.
- **Phase 2: M.** One new struct + one method in `frogdb-vll`, six methods narrowed, two `LockTable`
  methods de-`pub`ed, the two `diagnostics.rs` collectors rewritten around one call with their
  duplicated mapping merged, `lib.rs` exports adjusted, the `executing_txid` path made live and
  given its first test, and the FM-VLL-003 row re-worded. Blocked on phase 1.

### Independently-landable hotfix

**`DEBUG VLL` never reports the executing txid** (problem §3) is a live, self-contained
under-report and a strict subset of phase 1:

1. `shard.rs:79` — `executing_ops: usize` → `executing_txids: BTreeSet<u64>`; increment/decrement
   become insert/remove; `is_drained()` reads `.is_empty()`.
2. `shard.rs` — add `pub fn executing_txids(&self) -> impl Iterator<Item = u64> + '_`.
3. `diagnostics.rs:174-177` — populate `info.executing_txid` from that iterator; delete the
   unreachable `state == Executing` branch.
4. One `frogdb-vll` test asserting `executing_txids` is non-empty between dequeue and release, and
   one `frogdb-core` test asserting `collect_vll_queue_info().executing_txid` is `Some`.

This keeps the `(txid, &keys)` release signature — so it does **not** conflict with siblings 51 or
52 — fixes the operator-visible bug on its own, and leaves phase 1 as "the set becomes a map".
It still touches a locked crate, so it carries the same `just mutants-gate frogdb-vll 0.90` step.
Its only spec edit is the `executing_ops` → `executing_txids` rename in FM-VLL-005's NOT-observable
row (`vll-failure-modes.md:105`); the Scope carve-out at L20-23 and the Invariant at L106 stay as
they are, because the `(txid, &keys)` release contract is unchanged. Those are phase 1's edits.
