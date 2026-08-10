# Proposal 51 — Two small state modules finish the consolidation they started

*Round 38. Candidates TX11 + TX12 of the txn+vll+scripting lane. One proposal because both are
the same shape at S size in two locked crates: a module that already owns a rule at its seam,
but still carries two implementations of it (TX11) or an internal representation choice that
leaked out into a locked spec row (TX12). They share nothing else — see the split note in
Effort.*

## Summary

`TxnSlotAccumulator` (`txn/src/state.rs:47-116`) documents itself as the "single owner of the
transaction co-location rule that once lived split across three ad-hoc spots" — and then
implements that rule twice, in `fold_shard` (L74-87) and `note_slot` (L93-115). The two copies
are not identical, and the differences are undocumented: one arm of `note_slot` is
**unreachable**, and another produces `Multi(vec![0, 0])` — a duplicate shard that both copies
carry explicit `contains` guards to prevent. The consolidation happened at the module's seam and
stopped at its front door. TX11 finishes it by moving the lattice onto `TransactionTarget`
itself, as two named transitions over one shard-set mutation.

`VllShardState` (`vll/src/shard.rs:68-114`) holds its lock table and transaction queue as
`Option`s initialized on first use, purely to avoid two allocations that
`HashMap::new()`/`BTreeMap::new()` do not perform. The cost is nine `as_ref()`/`as_mut()`
unwrapping sites, a redundant `max_queue_depth` field that exists only to feed the deferred
constructor, and — the part that matters — a **locked** spec row whose Invariant names
`ensure_initialized` by name (`vll-failure-modes.md:46`), so an allocation micro-optimization has
become a fact callers must know. TX12 initializes eagerly and deletes the ceremony.

Neither is a live defect. Both are re-gate-only work in locked crates, and **TX12 is on
proposal 45's critical path** — 45 explicitly requires this proposal to land first.

## Files involved

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/txn/src/state.rs` | 624 | **TX11 owner.** `TransactionTarget` (L16-25) + `resolve` (L32-38); `TxnSlotAccumulator` (L47-55) with `add_keys` (L60-70), `fold_shard` (L74-87), `note_slot` (L93-115). Callers `TransactionState::fold_keys` (L234-236), `fold_shard` (L240-242), `take`'s watch fold (L285-287). Tests L436-529. |
| `frogdb-server/crates/txn/src/exec.rs` | — | L279-280: the only production read of a resolved target; `Multi(_)` is `unreachable!()`. Establishes that `Multi`'s `Vec<usize>` payload is never consumed. **Not edited.** |
| `frogdb-server/crates/vll/src/shard.rs` | 1159 | **TX12 owner.** Struct fields L68-81 (`lock_table` L69, `tx_queue` L70, `max_queue_depth` L80); `Default` L83-87; `with_max_queue_depth` L91-101; `ensure_initialized` L103-114. Nine unwrap sites: L153, L204-206, L213-215, L227, L252-254, L265-267, L271-273, L335, L421, L426-429, L452-454. |
| `frogdb-server/crates/vll/src/queue.rs` | 299 | `TransactionQueue` (L71-77), `new` (L87-92), and the manual `Default` at **L79-83 with a hardcoded `10000`** — verified **zero callers**. Deleted by TX12. |
| `frogdb-server/crates/vll/src/lock_table.rs` | 343 | `#[derive(Debug, Default)] pub struct LockTable` (L38-42) over `HashMap<Bytes, BTreeMap<..>>` (L41); `new()` = `Self::default()` (L46-48). The evidence that eager construction allocates nothing. **Not edited.** |
| `frogdb-server/crates/core/src/shard/builder.rs` | — | L453 `vll: ShardVll::default()` — the **only** production construction of a `VllShardState`. **Not edited.** |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED. **L46 only** — FM-VLL-001's Invariant, which names `ensure_initialized`. Reserved to this proposal by sibling 45. |
| `.scratch/hardening/specs/txn-failure-modes.md` | 650 | LOCKED. **No edits.** FM-TXN-019 (L254-264), FM-TXN-020 (L266-276), FM-TXN-042 (L530-540) are the rows whose forcing tests reach this code; none names `fold_shard` or `note_slot`. Enumerated below. |

## Problem

### TX11.1 — the co-location rule has two implementations inside its own owner

`state.rs:41-46` states the module's purpose:

> Single owner of the transaction co-location rule that once lived split across three ad-hoc
> spots (`note_cluster_slot`, `add_transaction_shard`, and the WATCH loop).

Inside, the rule is written out twice. `fold_shard` (L74-87) and `note_slot`'s mismatch arm
(L100-110) each carry a full `None`/`Single`/`Multi` promotion match, and the last two arms are
character-for-character the same logic:

```rust
// fold_shard, L78-85                    // note_slot, L102-109
TransactionTarget::Single(s) =>          TransactionTarget::Single(s) =>
    Multi(vec![*s, shard_id]),               Multi(vec![*s, shard_id]),
TransactionTarget::Multi(shards) => {    TransactionTarget::Multi(shards) => {
    let mut shards = shards.clone();         let mut shards = shards.clone();
    if !shards.contains(&shard_id) {         if !shards.contains(&shard_id) {
        shards.push(shard_id);                   shards.push(shard_id);
    }                                        }
    Multi(shards)                            Multi(shards)
}                                        }
```

This is the shape the skill's vocabulary calls a **locality** failure that the seam hides: the
module's *interface* has one owner of the rule, its *implementation* has two, and a maintainer
fixing one has no signal that the other exists. The duplication is not free — it has already
produced two divergences that nothing in the code comments explains.

### TX11.2 — divergence 1: `note_slot`'s `None` arm is unreachable

`note_slot` L101 seeds `TransactionTarget::None → Multi(vec![shard_id])`, where `fold_shard` L76
seeds `None → Single(shard_id)`. To reach L101, `first_slot` must already be `Some` while
`target` is still `None`. Trace `add_keys` (L60-70), the only caller:

```rust
for key in keys {
    let shard = shard_for_key(key.as_ref(), num_shards);
    if is_cluster && self.note_slot(slot_for_key(key.as_ref()), shard) {
        continue;
    }
    self.fold_shard(shard);
}
```

`first_slot` is set only in `note_slot`'s `None` arm (L96), which returns `false` — so control
always falls through to `fold_shard`, which leaves `target` at `Single` or better. `begin()`
(L206) and `take()` (L288) reset both fields together, so there is no path that clears `target`
while retaining `first_slot`. **L101 cannot execute.** Confirmed against the tests: every
accumulator test (L473-529) calls `fold_shard` immediately after the first `note_slot`, because
that is the only way the production caller uses it.

Unreachable code in a crate held to a **0.90 mutation gate** is unverifiable code: no test can
cover it, so no test can kill a mutant in it.

### TX11.3 — divergence 2: `Single(s)` where `s == shard_id` builds `Multi(vec![s, s])`

`fold_shard` guards the equal case (L77, `Single(s) if *s == shard_id`). `note_slot` L102 does
not — deliberately, and correctly at the semantic level: two keys in *different slots* that
happen to land on the *same shard* are still cross-slot and must promote. But the promotion is
written as `Multi(vec![*s, shard_id])`, which with `s == shard_id` yields a shard list with the
same shard listed twice.

This is not hypothetical. It is what the FM-TXN-019 forcing test produces today:

```rust
// state.rs:419-431, FM-TXN-019
t.fold_keys(&[b"a".as_slice(), b"b".as_slice()], 1, true);  // num_shards = 1
```

Both keys map to shard 0 in different slots, so the target ends as `Multi(vec![0, 0])`. The test
asserts only `matches!(summary.target, Multi(_))`, so it passes.

Two `contains` guards exist in this file for the express purpose of keeping that list
duplicate-free — `state.rs:81` and `state.rs:105`, both with test assertions spelling out "must
not duplicate it" (L456, L503). The seed path bypasses both.

**Severity: latent, not live.** The `Vec<usize>` inside `Multi` is never read. `resolve()`
(L33-38) matches on the variant and discards the payload; `exec.rs:280` is
`unreachable!("resolve() maps Multi to Err")`. Verified by grep: no production site destructures
`Multi`. So the duplicate is dormant — which is exactly why it survived. It becomes a real bug
the day the payload gets a consumer, and `Multi` is documented at L23 as "prepared for future
multi-shard support".

### TX11.4 — the fold clones the shard list on every key

Both `Multi` arms match on `&self.target` and rebuild the vector via `shards.clone()`
(L80, L104). For a MULTI whose queue names N keys across a promoted target, that is N vector
allocations to produce a value nothing reads. Matching on `&mut self.target` (or taking it)
removes them. Minor, and not the reason to do this work — but it is free once the arms merge.

### TX12.1 — laziness that saves nothing, paid for in nine places

`VllShardState` (L68-81) stores:

```rust
lock_table: Option<LockTable>,
tx_queue: Option<TransactionQueue<O>>,
```

both `None` at construction (L93-94), materialized on first enqueue by `ensure_initialized`
(L103-114) with two `.unwrap()`s. What the laziness avoids:

- `LockTable::new()` → `Self::default()` (lock_table.rs:46-48) → `HashMap<Bytes, BTreeMap<..>>`
  (L41). `HashMap::new()` performs **no allocation** until first insert.
- `TransactionQueue::new(max_depth)` (queue.rs:87-92) → `BTreeMap::new()` + a `usize`.
  `BTreeMap::new()` performs **no allocation**.

So the deferred cost is two null-pointer-sized field writes, deferred once per shard, on a struct
constructed exactly once per shard at startup (`builder.rs:453`, the only production site). The
price is nine sites in `shard.rs` that must re-establish what the constructor could have
guaranteed:

| Site | Today | After |
| --- | --- | --- |
| L153 | `let (lock_table, tx_queue) = self.ensure_initialized();` | `let (lock_table, tx_queue) = (&mut self.lock_table, &mut self.tx_queue);` |
| L204-206 | `let Some(tx_queue) = self.tx_queue.as_ref() else { return; };` | direct field read |
| L213-215 | per-iteration `if let (Some(..), Some(..)) = (self.lock_table.as_mut(), self.tx_queue.as_mut())` **inside the loop** | one split borrow, hoisted |
| L227 | `let tx_queue = self.tx_queue.as_mut()?;` | direct |
| L252-254 | `if let Some(lock_table) = self.lock_table.as_mut() { .. }` | direct |
| L265-267, L271-273 | two more `let..else` / `if let` in `abort` | direct |
| L335 | `self.tx_queue.as_ref().is_none_or(\|q\| q.is_empty())` | `self.tx_queue.is_empty()` |
| L421 | `self.tx_queue.as_ref().map_or(0, \|q\| q.len())` | `self.tx_queue.len()` |
| L426-429 | `.as_ref().into_iter().flat_map(\|q\| q.iter())` | `self.tx_queue.iter()` |
| L452-454 | `let Some(lock_table) = .. else { return Vec::new(); };` | direct |

Every one of these encodes an "uninitialized" state that the state machine has no meaning for:
`None` and `Some(empty)` behave identically at every single site. Two representations of one
state, and the type asserts a distinction the implementation immediately erases.

L213-215 is the sharpest instance — a re-check of both `Option`s on **every iteration** of the
pending-txid loop, because the borrow checker cannot see across `try_acquire_for`. With plain
fields, `Self::try_acquire_for(&mut self.lock_table, &mut self.tx_queue, txid)` is two disjoint
field borrows and the dance disappears.

### TX12.2 — the laziness forces a duplicated config field

`max_queue_depth: usize` (L80) is stored on `VllShardState` for exactly one reason: to be
available later, at L108, when the queue is finally constructed. It is read nowhere else
(verified by grep: L99 write, L108 read, nothing more). `TransactionQueue` already owns
`max_depth` (queue.rs:76) and already answers `has_capacity()` from it (queue.rs:95-97). Eager
construction hands the value straight to the queue and **deletes the field** — the module stops
shadowing its own component's configuration.

The same laziness left `TransactionQueue`'s manual `Default` (queue.rs:79-83) with a hardcoded
`10000` duplicating `DEFAULT_MAX_QUEUE_DEPTH` (shard.rs:25). Verified **zero callers** — it is
dead, and nothing derives `Default` over a `TransactionQueue`.

### TX12.3 — an allocation detail is written into a LOCKED spec row

`vll-failure-modes.md:46`, FM-VLL-001's Invariant:

> `enqueue_lock_request` short-circuits on `continuation_held_or_pending()` before
> `ensure_initialized`, so no intent is declared and `queue_depth()` is unchanged — the rejection
> leaves nothing to clean up.

The row is stating the right property ("no intent declared, `queue_depth()` unchanged") but
citing an internal allocation-deferral helper as the mechanism. That is the leak: **Interface**,
in this skill's sense, is everything a caller must know — and a private lazy-init helper has
become part of it, in a document under `Status: LOCKED`. The forcing test
(`sca_lock_request_rejected_while_continuation_held`, shard.rs:596-618) already asserts the real
property and nothing about laziness: `outcome.enqueue_failed`, `Failed(ShardBusy)`, and
`assert_eq!(state.queue_depth(), 0)` (L617). The spec is describing a mechanism the test does
not check.

## Proposed change

### TX11 — the lattice moves onto `TransactionTarget`, as two named transitions

Add two private methods to the type that owns the states. `TransactionTarget` is `pub`; the
methods are not, so the interface does not grow.

```rust
impl TransactionTarget {
    /// Promote to `Multi`, adding `shard_id` to the shard set. The one place
    /// the shard set is mutated: `None` seeds an empty set, `Single` seeds a
    /// one-element set, and an already-listed shard is not duplicated.
    fn promote_to_multi(&mut self, shard_id: usize) {
        let mut shards = match std::mem::take(self) {
            TransactionTarget::None => Vec::new(),
            TransactionTarget::Single(s) => vec![s],
            TransactionTarget::Multi(shards) => shards,
        };
        if !shards.contains(&shard_id) {
            shards.push(shard_id);
        }
        *self = TransactionTarget::Multi(shards);
    }

    /// Fold a shard under the co-location rule: the first shard makes the
    /// target `Single`, a matching shard keeps it, a differing one promotes.
    fn fold(&mut self, shard_id: usize) {
        match self {
            TransactionTarget::None => *self = TransactionTarget::Single(shard_id),
            TransactionTarget::Single(s) if *s == shard_id => {}
            _ => self.promote_to_multi(shard_id),
        }
    }
}
```

`TxnSlotAccumulator` then holds no lattice logic at all — only the *policy* of which transition
each caller gets:

- `TxnSlotAccumulator::fold_shard` (L74-87) → `self.target.fold(shard_id)`.
- `note_slot`'s mismatch arm (L100-110) → `self.target.promote_to_multi(shard_id)` — the
  unconditional promotion the cluster rule requires, now named as such rather than inlined as a
  near-copy of `fold`.

What this fixes, concretely:

- **TX11.2 dissolves.** The `None` seed stops being a suspicious fifth arm reachable from one
  caller and becomes the identity element of the fold — one line, `Vec::new()`, correct for any
  future caller that promotes before folding. There is one lattice, and both entry points
  traverse it.
- **TX11.3 is fixed.** `Single(s)` seeds `vec![s]` and the shared `contains` guard runs, so
  `Multi(vec![0, 0])` becomes `Multi(vec![0])`. The two `contains` guards become one, and it
  covers the seed path they were both written to protect.
- **TX11.4 is fixed.** `mem::take` moves the vector out instead of cloning it.

`std::mem::take` requires `TransactionTarget: Default`, which it already derives (L16, `#[default]`
on `None` at L19).

The `Multi` payload being dead data (TX11.3) invites a larger move — drop the `Vec<usize>`
entirely, since `resolve()` and `exec.rs:280` are its only readers and neither looks inside.
**Deliberately not proposed here.** It changes a `pub` enum, deletes the dedup assertions that
are half of two existing tests, and discards scaffolding L23 documents as intentional
("prepared for future multi-shard support"). That is a design decision with an owner; this
proposal keeps the payload and makes it correct. File it as a follow-up if the multi-shard plan
is dead.

### TX12 — eager fields, no ceremony

```rust
pub struct VllShardState<O: Debug> {
    lock_table: LockTable,
    tx_queue: TransactionQueue<O>,
    continuation_lock: Option<ContinuationLock>,
    pending_continuation_release: Option<oneshot::Receiver<()>>,
    pending_continuation: Option<PendingContinuation>,
    executing_ops: usize,
}

pub fn with_max_queue_depth(max_queue_depth: usize) -> Self {
    Self {
        lock_table: LockTable::new(),
        tx_queue: TransactionQueue::new(max_queue_depth),
        continuation_lock: None,
        pending_continuation_release: None,
        pending_continuation: None,
        executing_ops: 0,
    }
}
```

The remaining three `Option`s stay: for those, absence is a *state* the machine reasons about
(`continuation_held_or_pending`, L329-331) rather than an initialization artifact. That is the
line this change draws — `Option` where absence means something, plain field where it does not.

Then: delete `ensure_initialized` (L103-114) and its two `.unwrap()`s; rewrite the nine sites per
the table in TX12.1; delete `max_queue_depth` (L80, L99); delete the dead
`impl Default for TransactionQueue` (queue.rs:79-83).

**Behaviour is preserved at every site**, because `None` and `Some(empty)` are already
indistinguishable at all nine:

| Site | `None` today | Eager empty |
| --- | --- | --- |
| `is_drained` L335 | `is_none_or(..)` → `true` | `is_empty()` → `true` |
| `queue_depth` L421 | `map_or(0, ..)` → `0` | `len()` → `0` |
| `iter_pending_ops` L426 | empty iterator | empty iterator |
| `intent_snapshots` L452 | `Vec::new()` | `iter_keys()` over empty map → `Vec::new()` |
| `dequeue_for_execution` L227 | `None` via `?` | `dequeue` misses → `None` |
| `abort` L265, L271 | early return | `dequeue` misses → early return |
| `try_advance_pending_locks` L204 | early return | empty txid list → empty loop |
| `release_after_execution` L252 | skip release | `release` on empty table → no-op |

### Spec edit (one line, TX12 only)

`vll-failure-modes.md:46` — replace the mechanism citation with the property the forcing test
actually asserts:

> `enqueue_lock_request` short-circuits on `continuation_held_or_pending()` **before it touches
> the lock table or the queue**, so no intent is declared and `queue_depth()` is unchanged — the
> rejection leaves nothing to clean up.

This is a strictly stronger statement: "declares no intent, enqueues nothing" is checkable at the
seam, where "runs before `ensure_initialized`" was checkable only by reading the body. Note that
`just lint-failure-modes` verifies `Forced by` names and `// FM-` tags in both directions — it
does **not** check Invariant prose, so this edit is discipline, not a gate. It still runs, because
the `Forced by` set is unchanged and must keep resolving.

## Testability improvement

**TX11 — one lattice, one test surface.** Today the accumulator's tests (L436-529) are white-box
by necessity: `note_slot` and `fold_shard` are private, and each needs its own coverage of the
same promotion rule. The result is duplicated assertions — `accumulator_shard_fold_none_single_multi`
(L436-471) and `accumulator_note_slot_dedupes_shards_once_already_multi` (L484-518) both assert
"re-folding an existing shard must not duplicate it" (L456 / L503) and "a new shard must be
appended" (L465 / L513), against two copies of the same eight lines. After the merge, the shard-set
rule is tested once against `promote_to_multi`, and the two accumulator tests shrink to what they
are actually about: *which* transition each caller selects.

**Mutation weight (frogdb-txn, gate 0.90).** Two effects, in opposite directions, both good:

- The mutable surface shrinks. `cargo mutants` generates a mutant set per function; one shard-set
  mutation instead of two means one killer test instead of two asserting the same property.
- The **unreachable** arm at L101 goes away as unreachable. Today it is dead weight in a gated
  crate — code no test can reach and therefore no test can defend. After the merge, both entry
  points traverse `promote_to_multi`, so every line of the lattice is exercised by the existing
  tests.

**New test TX11 needs** (one, in `frogdb-txn` — the gated crate, per the "put the forcing test in
the mutated crate" rule): the seed-dedup case that today produces `Multi(vec![0, 0])`.

```rust
#[test]
fn cluster_slot_mismatch_on_one_shard_does_not_duplicate_the_shard() {
    let mut acc = TxnSlotAccumulator::default();
    assert!(!acc.note_slot(100, 0));
    acc.fold_shard(0);                     // Single(0)
    assert!(acc.note_slot(200, 0));        // different slot, same shard
    match &acc.target {
        TransactionTarget::Multi(shards) => assert_eq!(shards, &vec![0]),
        other => panic!("expected Multi, got {other:?}"),
    }
}
```

**TX12 — nothing new to test, and that is the point.** No observable changes, so no new forcing
test is owed. What improves is the *reading* of the existing ones: FM-VLL-001's test asserts
`queue_depth() == 0` (shard.rs:617), and after the edit the spec row it forces says the same thing
in the same terms. The nine deleted `let..else`/`if let` branches are nine branches that were
structurally unreachable in production (`ensure_initialized` runs on the first enqueue, before
anything else can observe the fields) — removing them removes coverage holes rather than creating
them, which is why the `frogdb-vll` score should hold or rise.

## Risks / scope boundaries vs siblings

**TX11 and TX12 share no file.** They are in different crates and can land as two PRs; the only
reason they are one proposal is that both are S-sized re-gate-only work in the same locked area.

**Round-38 file ownership** (four proposals touch `frogdb-vll/src/shard.rs`):

| Proposal | Owns | Overlap with 51 |
| --- | --- | --- |
| **51** (this) | `txn/src/state.rs:16-115` (the lattice) + `vll/src/shard.rs:68-114` (fields, ctor, `ensure_initialized`) and the nine unwrap sites listed above + `vll/src/queue.rs:79-83` (dead `Default`) + `vll-failure-modes.md:46` | — |
| **45** vll-key-ownership-diagnostics | `shard.rs` `executing_ops` L79 → `executing` map, `dequeue_for_execution` L226-238, `release_after_execution` L250-257, `is_drained` L334-336, introspection L415-463, `DequeuedOp` L479-483; `lock_table.rs:150-173`; `core/shard/diagnostics.rs`; `core/shard/vll.rs:72`; `vll-failure-modes.md` L20-23, :69, :105-106 | **CONFLICT (textual, same struct).** 45 already records this and asks for **51 to land first** — restated and accepted from this side. 51 rewrites the field block (L68-81) and constructor (L92-100) that 45 also edits, plus every `self.lock_table.as_mut()` / `self.tx_queue.as_ref()` site including the ones inside `release_after_execution` and `is_drained`. The two changes are additive in intent (51 unwraps two fields, 45 replaces a third) but will not auto-merge. **Land 51 first; 45 rebases onto plain fields.** 45's design is unaffected by whether the neighbours are `Option`al. Spec split: **51 owns `vll-failure-modes.md:46` and nothing else** in that file; 45 owns L20-23, :69, :105-106 and must not touch :46. 45's independently-landable hotfix (`executing_ops` → `executing_txids`) keeps the `(txid, &keys)` signature and touches L79 only, so it does **not** conflict with 51 and may land in either order. |
| **46** vll-acquire-error-unify | `vll/src/coordinator.rs` (both error enums), `vll/src/types.rs`, `server/src/scatter/executor.rs:141-193`, `server/src/connection/scripting/eval.rs:268-281`, `vll-failure-modes.md` L14-18, L26-30, and FM-VLL-001-004 **`Observable` / `Outcome variant`** fields | **None.** 46 touches no field of `VllShardState` and no line of `shard.rs`. Within FM-VLL-001, 46 edits the `Observable` (L44) and `Outcome variant` (L47) fields; 51 edits the `Invariant` (L46). Adjacent lines in the same row — a trivial merge, but sequence them rather than landing simultaneously. |
| **52** vll-unknown-txid-refusal | `core/src/shard/vll.rs:40-75` (`handle_vll_execute`'s `None` arm → typed refusal), new FM-VLL-006 row | **None.** 52 lives entirely in `frogdb-core`; 51 changes no signature 52 calls (`dequeue_for_execution` still returns `Option<DequeuedOp<O>>` with identical semantics). Both need the `frogdb-vll` re-gate; independent otherwise. |
| **50** txn state.rs (`ConnectionState` → `TransactionState`) | `server/src/connection/state.rs:678-770, 797-826` (the 13-method pass-through, `asking` lifecycle) and `txn/src/state.rs`'s **public** `TransactionState` surface | **CONFLICT (same file, disjoint regions).** 50 moves `asking` into `TransactionState` and reshapes its public methods (L182-323); 51 touches only `TransactionTarget` (L16-38) and `TxnSlotAccumulator` (L47-116) and changes **no signature** — `fold_keys`, `fold_shard`, `take` keep their bodies verbatim apart from the two delegating call sites. Different regions of one file, so a rebase, not a redesign. 50 also carries FM-TXN-015 `Forced by` edits and an ADR-0002 sign-off; **51 edits no `txn-failure-modes.md` row at all**, so there is no spec collision. **Land 51 first** (S, mechanical, no sign-off needed) to keep 50's larger diff clean. |

**Locked-area landing steps:**

1. **Not spec-first — neither half changes an observable.** TX11's only behavioural delta is the
   contents of a `Vec` that no production reader destructures (verified: `resolve()` L33-38 and
   `exec.rs:279-280` are the only consumers). TX12's are all `None`-vs-`Some(empty)` equivalences
   enumerated above. So: change first, then the one spec-text edit, then re-gate.
2. **`txn-failure-modes.md`: no row edits.** Three rows have forcing tests that reach this code —
   enumerated so the reviewer can check the claim rather than take it:
   - **FM-TXN-019** (L254-264): `fold_keys_promotes_on_slot_mismatch_in_cluster_mode`
     (state.rs:419) and `transaction_target_resolve_maps_multi_to_crossslot` (state.rs:533) are
     in-crate; `cross_slot_when_the_queue_folded_to_more_than_one_shard`
     (txn/tests/exec_outcomes.rs:324) and `batch_spanning_two_slots_is_crossslot`
     (server/src/slot_migration/tests.rs:422) are outside. All assert `matches!(.., Multi(_))` or
     the `resolve()` reply — none inspects the shard list. Row Invariant names
     `TransactionTarget::resolve` and `redirect::CROSSSLOT_MSG`; unchanged.
   - **FM-TXN-020** (L266-276): `cross_shard_watch_set_folds_to_multi_at_take` (state.rs:357),
     `take_transaction_folds_cross_shard_watch_set_to_multi` (server/src/connection/state.rs:1398).
     Both go through `take`'s watch fold (L285-287) → `fold`. Row Invariant names `take`;
     unchanged.
   - **FM-TXN-042** (L530-540): `accumulator_shard_fold_none_single_multi` (state.rs:437) is a
     `Forced by` name **and** the test whose dedup assertions this change consolidates. **Keep the
     test name and its `// FM-TXN-042` tag (L435).** Renaming it silently breaks
     `just lint-failure-modes`. Row Invariant is about `None` resolving to `host.shard_id()`;
     unchanged.
3. `just lint-failure-modes` after the `vll-failure-modes.md:46` edit (also part of `just lint`).
4. **Both crates re-gate.** `just mutants-diff frogdb-txn` and `just mutants-diff frogdb-vll`
   before pushing (push discipline); full `just mutants <crate>` + `just mutants-gate <crate> 0.90`
   for each half that lands. TX11 alone → `frogdb-txn` only; TX12 alone → `frogdb-vll` only. Both
   reduce the mutable surface, so the scores should hold or improve.
5. **New tests go in the crate being mutated.** TX11's forcing test belongs in
   `frogdb-txn/src/state.rs`'s test module, next to the accumulator tests — not in
   `frogdb-server`, where it would contribute nothing to the 0.90 score.

**Other risks:**

- **`Multi`'s shard list changes contents.** `Multi(vec![0, 0])` → `Multi(vec![0])` in the
  same-shard-cross-slot case. Verified no production reader and no test asserts that specific
  list; `fold_keys_promotes_on_slot_mismatch_in_cluster_mode` (L426-430) matches on the variant
  only. Call it out in the PR anyway — it is the one place where "no observable change" is a claim
  about the *absence* of a reader rather than about equivalent values.
- **`TransactionQueue`'s `Default` deletion is a `pub` item removal** in `frogdb-vll`. Verified
  zero callers repo-wide and no `#[derive(Default)]` over a type containing one. Pre-production, so
  no compatibility concern; mentioned only so the reviewer does not have to re-derive it.
- **The `#[allow(clippy::result_large_err)]` on `resolve` (L32)** stays — unaffected.
- **No `frogdb-core` change.** `ShardVll` (`core/shard/types.rs:438`) is a type alias and
  `builder.rs:453` constructs via `Default`; both compile unchanged against eager fields.

## Effort estimate

- **TX11: S.** One file. Two new private methods (~25 lines), two call sites reduced to
  delegation, ~20 lines of duplicated match arms deleted, one new test, plus trimming the
  duplicated assertions out of the two existing accumulator tests. Zero spec edits. Zero signature
  changes. The `frogdb-txn` re-gate is the long pole, not the diff.
- **TX12: S.** One file plus five lines in another. Six field lines, one constructor, one deleted
  helper, nine mechanical unwrap-site edits, one dead `impl` deleted, one spec sentence. No test
  changes expected — if any `frogdb-vll` test fails, the "no observable change" claim is wrong and
  the change should stop.

### Independently-landable hotfix

**None, because there is no live defect** — TX11.3 is dormant behind an unread payload and TX12 is
pure ceremony. Claiming a hotfix here would be dressing up a cleanup as a fix.

The useful split is by crate rather than by severity, and it is **not** symmetric:

- **TX12 alone is the one with a schedule.** Sibling 45 is blocked on it and says so. It is the
  smaller, more mechanical half, it carries the single spec edit, and landing it first unblocks
  45's rebase. **Do TX12 first.**
- **TX11 alone** is the only half that touches `frogdb-txn`, needs no spec edit at all, and should
  precede sibling 50's larger `state.rs` restructuring for the same land-the-small-one-first
  reason.

If only one lands this round, land TX12.
