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
**unreached**, and another produces `Multi(vec![0, 0])` — a duplicate shard that both copies
carry explicit `contains` guards to prevent. The consolidation happened at the module's seam and
stopped at its front door.

The two copies are nevertheless **not interchangeable**, and that is the load-bearing fact: a
naive merge in *either* direction regresses a locked failure-mode row. Collapsing both onto
`fold_shard`'s semantics (note_slot delegates to `fold`) makes a same-shard slot mismatch stay
`Single` and breaks **FM-TXN-019**; collapsing both onto `note_slot`'s semantics (fold_shard
promotes unconditionally) makes a repeated single-shard fold produce `Multi` and breaks
**FM-TXN-013** — witnessed in-crate at `state.rs:444`. Both directions are worked in TX11.1a. The
merge that survives both proofs is not "pick one implementation" but a *factorization*: one shard-set
mutation, and two named transitions over it. TX11 moves that factorization onto `TransactionTarget`.

`VllShardState` (`vll/src/shard.rs:68-114`) holds its lock table and transaction queue as
`Option`s initialized on first use, purely to avoid two allocations that
`HashMap::new()`/`BTreeMap::new()` do not perform. The cost is **eleven** `as_ref()`/`as_mut()`
unwrapping sites, a redundant `max_queue_depth` field that exists only to feed the deferred
constructor, and — the part that matters — an allocation detail written into **two lines of a
LOCKED spec**: FM-VLL-001's Invariant names `ensure_initialized` outright
(`vll-failure-modes.md:46`), and FM-VLL-003's Observable opens with the parenthetical *"(queue
never used, or already drained)"* (`:68`) whose first branch exists only because the queue can be
absent. An allocation micro-optimization has become a fact callers must know, twice. TX12
initializes eagerly and deletes the ceremony.

Neither is a live defect. Both are re-gate-only work in locked crates, and **TX12 is on the
critical path of two siblings** — the agreed round order is **51 → 46 → 45**; 45 explicitly
requires this proposal to land first, and 46 recommends the same sequence from its own side.

## Files involved

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/txn/src/state.rs` | 624 | **TX11 owner.** `TransactionTarget` (L16-25) + `resolve` (L32-38); `TxnSlotAccumulator` (L47-55) with `add_keys` (L60-70), `fold_shard` (L74-87), `note_slot` (L93-115). Callers `TransactionState::fold_keys` (L234-236), `fold_shard` (L240-242), `take`'s watch fold (L285-287). Tests L436-529. |
| `frogdb-server/crates/txn/src/exec.rs` | — | L279-280: the only production read of a resolved target; `Multi(_)` is `unreachable!()`. Establishes that `Multi`'s `Vec<usize>` payload is never consumed. **Not edited.** |
| `frogdb-server/crates/vll/src/shard.rs` | 1159 | **TX12 owner.** Struct fields L68-81 (`lock_table` L69, `tx_queue` L70, `max_queue_depth` L80); `Default` L83-87; `with_max_queue_depth` L91-101; `ensure_initialized` L103-114. **Eleven** unwrap sites: L153, L204-206, L213-215, L227, L252-254, L265-267, L271-273, L335, L421, L426-429, L452-454 — ten rows in the table below, because L265-267 and L271-273 (both in `abort`) share one row. |
| `frogdb-server/crates/vll/src/queue.rs` | 299 | `TransactionQueue` (L71-77), `new` (L87-92), and the manual `Default` at **L79-83 with a hardcoded `10000`** — verified **zero callers**. Deleted by TX12; this is the single highest-value line of the whole proposal for the `frogdb-vll` gate (see *Mutation weight*). |
| `frogdb-server/crates/vll/src/lock_table.rs` | 343 | `#[derive(Debug, Default)] pub struct LockTable` (L38-42) over `HashMap<Bytes, BTreeMap<..>>` (L41); `new()` = `Self::default()` (L46-48). The evidence that eager construction allocates nothing. **Not edited.** |
| `frogdb-server/crates/core/src/shard/builder.rs` | — | L453 `vll: ShardVll::default()` — the **only** production construction of a `VllShardState`. **Not edited.** |
| `.scratch/hardening/specs/vll-failure-modes.md` | 109 | LOCKED. **Two lines.** **L46** — FM-VLL-001's Invariant, which names `ensure_initialized`; reserved to this proposal by sibling 45. **L68** — FM-VLL-003's Observable, whose opening parenthetical *"(queue never used, or already drained)"* encodes the same laziness; 51 deletes the four-word parenthetical only. Sibling 46 rewrites a *different sentence* of L68 (the error string) — see the ownership table. |
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

### TX11.1a — why the obvious merge is wrong, in *both* directions

The two copies are near-identical, so the first instinct is to delete one and call the other from
both sites. That instinct is a regression either way it is applied, and each way lands on a
different **locked** row. This is the argument for the factorization below, and it is checkable
against tests that exist today:

| Naive merge | What changes | Row it breaks | Witness that fails |
| --- | --- | --- | --- |
| Collapse onto **`fold_shard`** — `note_slot`'s mismatch arm delegates to `fold` | `Single(s)` with `s == shard_id` is *kept* instead of promoted, so two keys in different slots that land on the same shard stay `Single` | **FM-TXN-019** (`txn-failure-modes.md:254-264`) — "EXEC of a batch that folded to more than one shard" | `fold_keys_promotes_on_slot_mismatch_in_cluster_mode` (`state.rs:419-431`) runs with `num_shards = 1`, so `"a"` and `"b"` share shard 0; the target would end `Single(0)`, the assert at L426-430 fails, and cluster-mode CROSSSLOT silently disappears |
| Collapse onto **`note_slot`** — `fold_shard` delegates to the unconditional promotion | `Single(s)` with `s == shard_id` promotes, so a *single-shard* transaction that folds the same shard twice becomes `Multi` | **FM-TXN-013** (`:182-192`) — its NOT-observable is exactly *"after `WATCH {a}x` + `WATCH {b}y` + `UNWATCH`, a single-shard `EXEC` must commit rather than answer `CROSSSLOT`"* | `state.rs:444` (`assert!(matches!(acc.target, TransactionTarget::Single(1)))` after re-folding shard 1) — the in-crate witness, inside FM-TXN-042's forcing test |

So the difference between the copies is **semantic, not accidental**: `fold_shard` implements
*shard*-mismatch detection (equality is a no-op), `note_slot` implements *slot*-mismatch detection
(shard equality is irrelevant — the slots already differ). What is duplicated is not the policy but
the *shard-set mutation underneath it*. The proposed change factors exactly that out and leaves the
two policies distinct and named. Any reviewer tempted by a one-line "just call the other one" fix
should read this table first.

### TX11.2 — divergence 1: `note_slot`'s `None` arm is unreached

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
while retaining `first_slot`. **No caller can reach L101.** Confirmed against the tests: every
accumulator test (L473-529) calls `fold_shard` immediately after the first `note_slot`, because
that is the only way the production caller uses it.

**Precision — unreached, not type-unreachable.** `TxnSlotAccumulator` is private and derives
`Default`, and the in-file `mod tests` can construct it field-by-field, so an in-crate test *could*
build `{ first_slot: Some(_), target: None }` and execute L101. Nothing does, and no production
path can. That is the weaker and correct claim: the arm is defended by caller discipline, not by
the type system — which is precisely why it is unverifiable code that no reader can check without
tracing every caller.

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

### TX12.1 — laziness that saves nothing, paid for in eleven places

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
price is **eleven** sites in `shard.rs` that must re-establish what the constructor could have
guaranteed (ten rows below — the two `abort` sites share one):

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

### TX12.3 — an allocation detail is written into TWO LOCKED spec lines

Not one row. The laziness surfaced twice in `vll-failure-modes.md`, in two different rows, and both
citations lose their referent once the fields are eager.

**(a) `vll-failure-modes.md:46`, FM-VLL-001's Invariant:**

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

**(b) `vll-failure-modes.md:68`, FM-VLL-003's Observable**, opening clause:

> Drained shard **(queue never used, or already drained)**: the lock is granted synchronously.

The parenthetical enumerates two cases *because the queue can be absent*: "never used" is the
`tx_queue == None` state, "already drained" is `Some(empty)`. After TX12 there is exactly one
drained state — an empty queue — and the distinction has **no referent**. Nothing else in the row
depends on it: the Invariant at `:70` already defines drained as `is_drained()` (*"queue empty
**and** no dequeued op outstanding"*), which is a single predicate over a single representation.

**This is not a gate break.** `just lint-failure-modes` checks `Forced by` names and `// FM-` tags,
not prose, and the row does not become *false* — a never-used queue is still drained, it just stops
being a separate case. It is the same leak as (a): an internal allocation choice sitting in a
document under `Status: LOCKED`, where **Interface** is defined as everything a caller must know.
The fix is a four-word deletion (see *Spec edits*), and 51 takes it because 51 is the change that
removes the state the words name.

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

**Implementer note — borrowck on `fold`.** The sketch matches on `&mut self` while its `_` arm
calls `self.promote_to_multi(shard_id)`, and the `Single(s) if *s == shard_id` guard shared-borrows
through the same `&mut self`. NLL should accept this: no binding from the match is live in the `_`
arm, so the scrutinee borrow ends before the call. If a future borrowck (or a `#[deny]` in this
crate) rejects it, the fix is to match on a **copied discriminant** first —
`let seed = match self { None => …, Single(s) if *s == shard_id => …, _ => … }` reduced to a small
`Copy` decision value, then act on `self` afterwards. Do **not** "fix" it by reintroducing
`shards.clone()`; the clone removal (TX11.4) is one of the three things this change buys.

`TxnSlotAccumulator` then holds no lattice logic at all — only the *policy* of which transition
each caller gets:

- `TxnSlotAccumulator::fold_shard` (L74-87) → `self.target.fold(shard_id)`.
- `note_slot`'s mismatch arm (L100-110) → `self.target.promote_to_multi(shard_id)` — the
  unconditional promotion the cluster rule requires, now named as such rather than inlined as a
  near-copy of `fold`.

What this fixes, concretely:

- **TX11.2 dissolves.** The `None` seed stops being a suspicious arm that no caller reaches and
  becomes the identity element of the shard-set mutation — one line, `Vec::new()`, correct for any
  future caller that promotes before folding. Its correctness becomes readable from the function
  alone instead of from a survey of callers. (Worth zero mutation points either way — see *Mutation
  weight* (b).)
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

Then: delete `ensure_initialized` (L103-114) and its two `.unwrap()`s; rewrite the eleven sites per
the table in TX12.1; delete `max_queue_depth` (L80, L99); delete the dead
`impl Default for TransactionQueue` (queue.rs:79-83).

**Behaviour is preserved at every site**, because `None` and `Some(empty)` are already
indistinguishable at all eleven:

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

### Spec edits (two lines, TX12 only)

**1. `vll-failure-modes.md:46`** (FM-VLL-001, Invariant) — replace the mechanism citation with the
property the forcing test actually asserts:

> `enqueue_lock_request` short-circuits on `continuation_held_or_pending()` **before it touches
> the lock table or the queue**, so no intent is declared and `queue_depth()` is unchanged — the
> rejection leaves nothing to clean up.

This is a strictly stronger statement: "declares no intent, enqueues nothing" is checkable at the
seam, where "runs before `ensure_initialized`" was checkable only by reading the body.

**2. `vll-failure-modes.md:68`** (FM-VLL-003, Observable) — delete the four-word parenthetical
whose second case ceases to exist:

> ~~Drained shard (queue never used, or already drained):~~ → **Drained shard:** the lock is
> granted synchronously.

Nothing else on that line changes *from 51's side*. The rest of L68 — the parking description and
the `-ERR lock acquisition failed: VLL lock acquisition timeout` string — is untouched here and is
**sibling 46's** to rewrite; see the ownership table for the sequencing that makes both edits land.

**Neither edit is a gate break.** `just lint-failure-modes` verifies `Forced by` names and `// FM-`
tags in both directions — it does **not** check Invariant or Observable prose, so both edits are
discipline, not a gate. The lint still runs, because the `Forced by` sets are unchanged and must
keep resolving. Neither row becomes false: FM-VLL-001's short-circuit still short-circuits, and a
never-used queue is still a drained queue — it simply stops being a distinguishable case.

## Testability improvement

**TX11 — one lattice, one test surface.** Today the accumulator's tests (L436-529) are white-box
by necessity: `note_slot` and `fold_shard` are private, and each needs its own coverage of the
same promotion rule. The result is duplicated assertions — `accumulator_shard_fold_none_single_multi`
(L436-471) and `accumulator_note_slot_dedupes_shards_once_already_multi` (L484-518) both assert
"re-folding an existing shard must not duplicate it" (L456 / L503) and "a new shard must be
appended" (L465 / L513), against two copies of the same eight lines. After the merge, the shard-set
rule is tested once against `promote_to_multi`, and the two accumulator tests shrink to what they
are actually about: *which* transition each caller selects.

**Mutation weight — and the mechanism, stated correctly.** `cargo mutants` generates mutants per
*function* (replace the body with a default-shaped return value) and per *operator*. It does **not**
generate a mutant per match arm. Three consequences, one of them the best reason in this proposal to
do TX12 at all:

- **(a) `frogdb-txn`: the mutable surface GROWS.** Today `fold_shard` (`-> ()`) and `note_slot`
  (`-> bool`) contribute three fn-body mutants; afterwards `fold` and `promote_to_multi` add two
  more, for five. Operator mutants move the other way (the `*s == shard_id` guard plus the
  `!shards.contains(..)` check go from three sites to two), so the net is a small *increase*. The
  conclusion — the score holds — is unchanged, but the reason is not "fewer mutants", it is **every
  new mutant is killed by a test that already exists**: `fold`'s body → `()` makes `fold_shard` a
  no-op and fails `state.rs:441`; `promote_to_multi`'s body → `()` kills the cluster promotion and
  fails `state.rs:481`; `==` → `!=` on the guard fails `state.rs:444`; the dedup-guard mutant fails
  `state.rs:453-457`. "No new unkillable mutants" is the claim; "fewer mutants" was never true.
- **(b) `frogdb-txn`: deleting the L101 arm moves the score by ZERO.** L101 is a match arm inside
  `note_slot`, and `cargo mutants` emits nothing for a match arm — so it generates no mutants today
  and its removal removes none. `note_slot`'s own mutants (body → `true`, body → `false`) are
  already killed by `state.rs:477` and `state.rs:480`, and `state.rs:491` pins the shard list the
  arm's live sibling produces. The argument for deleting it is therefore **verifiability and reader
  confusion**, not the gate: it is the one line in the module whose correctness a reader cannot
  establish from a type or a test but only by tracing every caller of a private method, and it is
  what makes `note_slot` read as a fifth-arm variant of `fold_shard` instead of a different policy
  (TX11.1a).
- **(c) `frogdb-vll`: `impl Default for TransactionQueue` (`queue.rs:79-83`) is the single edit in
  this proposal that can move a score UP.** Verified zero callers — the only `default()` in
  `queue.rs` is the definition itself, `VllShardState` hand-writes its own `Default`
  (`shard.rs:83-87`) and always builds the queue through `TransactionQueue::new`, and nothing
  derives `Default` over a type containing one. A function no test can execute is a function no
  test can kill a mutant in: whatever `cargo mutants` emits there is a **`MissedMutant`** — a
  guaranteed survivor, landing directly in the denominator (`scripts/mutants-gate.py:46`,
  `denom = caught + missed`) — or, if the `Default::default()` body replacement self-recurses, a
  `Timeout`, excluded from the denominator but counted against the 5% timeout warning
  (`mutants-gate.py:58`). It is never `Caught`. Every other part of TX12 is score-*neutral* by
  construction (it deletes branches that were structurally unreachable in production, i.e. neither
  covered nor coverable); these five dead lines are the only strictly positive term. Lead with this
  in the PR, not with "the surface shrinks".

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
in the same terms, in both rows it touches. The eleven deleted `let..else`/`if let` branches are
eleven branches that were structurally unreachable in production (`ensure_initialized` runs on the
first enqueue, before anything else can observe the fields) — removing them removes coverage holes
rather than creating them, which is why the `frogdb-vll` score holds; the dead `Default` deletion
(c) is what makes it *rise*.

## Risks / scope boundaries vs siblings

**TX11 and TX12 share no file.** They are in different crates and can land as two PRs; the only
reason they are one proposal is that both are S-sized re-gate-only work in the same locked area.

**Round-38 file ownership** (four proposals touch `frogdb-vll/src/shard.rs`):

| Proposal | Owns | Overlap with 51 |
| --- | --- | --- |
| **51** (this) | `txn/src/state.rs` **L16-116** — both `impl TransactionTarget` (**L27-39**, which gains `fold` and `promote_to_multi`) and `TxnSlotAccumulator` (L47-116) + `vll/src/shard.rs:68-114` (fields, ctor, `ensure_initialized`) and the **eleven** unwrap sites listed above + `vll/src/queue.rs:79-83` (dead `Default`) + `vll-failure-modes.md` **:46 and the `:68` parenthetical** | — |
| **45** vll-key-ownership-diagnostics | `shard.rs` `executing_ops` L79 → `executing` map, `dequeue_for_execution` L226-238, `release_after_execution` L250-257, `is_drained` L334-336, introspection L415-463, `DequeuedOp` L479-483; `lock_table.rs:150-173`; `core/shard/diagnostics.rs`; `core/shard/vll.rs:72`; `vll-failure-modes.md` L20-23, :69, :105-106 | **CONFLICT (textual, same struct).** 45 already records this and asks for **51 to land first** — restated and accepted from this side. 51 rewrites the field block (L68-81) and constructor (L92-100) that 45 also edits, plus every `self.lock_table.as_mut()` / `self.tx_queue.as_ref()` site including the ones inside `release_after_execution` and `is_drained`. The two changes are additive in intent (51 unwraps two fields, 45 replaces a third) but will not auto-merge. **Land 51 first; 45 rebases onto plain fields.** 45's design is unaffected by whether the neighbours are `Option`al. **Spec split (revised):** 51 owns `:46` **and the `:68` opening parenthetical**; 45 owns L3, L20-23, `:69`, `:105-106`, `:108` and must not touch `:46`. `:68` and `:69` are adjacent lines of FM-VLL-003 — one line apart, so sequence rather than parallelize. **Requirement on 45:** its Files-involved table must not list `:46` among its edit sites. *Verified against the current on-disk 45* — its revised table (45:46) enumerates six sites, L3 / L20-23 / :69 / :105 / :106 / :108, with no `:46`, and 45:514 states "do **not** touch `:46` — that row belongs to sibling 51". The drift the review flagged is already fixed upstream; the requirement is restated here so the graph pass re-checks it rather than assuming. **Hotfix conflict — corrected in 45's favour.** This table previously said 45's independently-landable `executing_ops` → `executing_txid` hotfix touches L79 only and therefore does not conflict with 51. That was **wrong**: 45:585-589 correctly notes it also rewrites `shard.rs:98` (`executing_ops: 0` in the constructor), and *both* L79 and L98 sit inside 51's rewrite regions (fields L68-81, constructor L91-101). It is a two-adjacent-line git conflict inside hunks 51 rewrites wholesale, not a redesign — **land it after 51's TX12, or budget a five-minute rebase.** 51 accepts 45's correction. |
| **46** vll-acquire-error-unify | `vll/src/coordinator.rs` (both error enums), `vll/src/types.rs`, `vll/src/lib.rs:19`, `server/src/scatter/executor.rs:141-193`, `server/src/connection/scripting/eval.rs:268-281`; in the spec, the preamble (L11-12, L14-17, :29) and FM-VLL-001-004's **`Observable` / `Outcome variant`** fields | **No code overlap; spec overlap on TWO lines now.** 46 touches no field of `VllShardState` and no line of `shard.rs`. In **FM-VLL-001**, 46 edits the `Outcome variant` (`:47`) — per its revised text 46 no longer edits `:44`, which it records as already correct — one line below 51's `:46` Invariant. In **FM-VLL-003**, both proposals now edit **`:68`**: 51 deletes the opening *"(queue never used, or already drained)"* parenthetical, 46 rewrites the error string later on the same line (`-ERR lock acquisition failed: …` → `-ERR VLL lock acquisition failed: …`, 46:442-443). A markdown table row is one physical line, so these are a **guaranteed git conflict if landed in parallel** and a clean rebase if sequenced. **Ruling: 51 owns the `:68` parenthetical, 46 owns the `:68` error string, and the sequence is 51 → 46** — 46's own boundary section already recommends exactly `51 → 46 → 45` (46:587), so 46's `:68` edit simply rebases onto 51's shortened line. Accepted from this side. |
| **52** vll-unknown-txid-refusal | `core/src/shard/vll.rs:40-75` (`handle_vll_execute`'s `None` arm → typed refusal), new FM-VLL-006 row | **None.** 52 lives entirely in `frogdb-core`; 51 changes no signature 52 calls (`dequeue_for_execution` still returns `Option<DequeuedOp<O>>` with identical semantics). Both need the `frogdb-vll` re-gate; independent otherwise. |
| **50** txn state.rs (`ConnectionState` → `TransactionState`) | `server/src/connection/state.rs:678-770, 797-826` (the 13-method pass-through, `asking` lifecycle) and `txn/src/state.rs`'s **public** `TransactionState` surface | **CONFLICT (same file, disjoint regions).** 50 moves `asking` into `TransactionState` and reshapes its public methods (L182-323); 51 touches `TransactionTarget` — **including `impl TransactionTarget` at L27-39**, where the two new private methods go — and `TxnSlotAccumulator` (L47-116), and changes **no signature**: `fold_keys`, `fold_shard`, `take` keep their bodies verbatim apart from the two delegating call sites. Different regions of one file, so a rebase, not a redesign. 50 also carries FM-TXN-015 `Forced by` edits and an ADR-0002 sign-off; **51 edits no `txn-failure-modes.md` row at all**, so there is no spec collision. **Land 51 first** (S, mechanical, no sign-off needed) to keep 50's larger diff clean. **Two corrections owed to 50's cross-ref** (50:356), flagged for the graph pass — 50's reviser has been told: (i) it describes 51's region as `txn/src/state.rs` **L74-115**, which omits `impl TransactionTarget` L27-39; the correct region is **L16-116**. (ii) it asserts "51 also owns FM-TXN-019/020; 50 must not touch those rows" — 51 owns **no** `txn-failure-modes.md` row; FM-TXN-019/020 are rows 51 merely *reads* to prove it is not changing them. Neither correction disturbs 50's conclusions: the regions are still disjoint (50 owns L161-322), the shared `frogdb-txn` 0.90 re-gate still applies once on the second to land, and **51 first** still holds. |

**Locked-area landing steps:**

1. **Not spec-first — neither half changes an observable.** TX11's only behavioural delta is the
   contents of a `Vec` that no production reader destructures (verified: `resolve()` L33-38 and
   `exec.rs:279-280` are the only consumers). TX12's are all `None`-vs-`Some(empty)` equivalences
   enumerated above. So: change first, then the two spec-text edits, then re-gate.
2. **`txn-failure-modes.md`: no row edits.** Four rows have forcing tests that reach this code —
   enumerated so the reviewer can check the claim rather than take it:
   - **FM-TXN-019** (L254-264): its `Forced by` cell names **six** tests, not four. In-crate
     (`frogdb-txn`): `transaction_target_resolve_maps_multi_to_crossslot` (state.rs:533),
     `fold_keys_promotes_on_slot_mismatch_in_cluster_mode` (state.rs:419). Out-of-crate:
     `cross_slot_when_the_queue_folded_to_more_than_one_shard` (txn/tests/exec_outcomes.rs:324),
     `batch_spanning_two_slots_is_crossslot` (server/src/slot_migration/tests.rs:422),
     `test_multi_exec_two_single_key_commands_different_slots_defers_crossslot_to_exec`
     (server/tests/cluster_slots.rs:1377), and
     `test_multi_cross_shard_plain_keys_crossslot_default_config`
     (server/tests/integration_transactions.rs:1179). The two additions are **wire-level**: they
     drive a real MULTI/EXEC and assert the `-CROSSSLOT` reply frame, so they cannot see the shard
     list either. Conclusion unchanged — all six assert `matches!(.., Multi(_))`, the `resolve()`
     reply, or the wire error; **none inspects the shard list**, which is what makes TX11.3's
     `Multi(vec![0, 0])` → `Multi(vec![0])` unobservable. Row Invariant names
     `TransactionTarget::resolve` and `redirect::CROSSSLOT_MSG`; unchanged.
   - **FM-TXN-013** (L182-192): not edited, but named here because it is the row the *other*
     naive merge would break (TX11.1a). Its NOT-observable — a single-shard `EXEC` after `UNWATCH`
     must commit, not `CROSSSLOT` — is what forbids `fold_shard` from promoting on shard equality.
     The proposed `fold` keeps the `Single(s) if *s == shard_id => {}` arm precisely for this row.
   - **FM-TXN-020** (L266-276): `cross_shard_watch_set_folds_to_multi_at_take` (state.rs:357),
     `take_transaction_folds_cross_shard_watch_set_to_multi` (server/src/connection/state.rs:1398).
     Both go through `take`'s watch fold (L285-287) → `fold`. Row Invariant names `take`;
     unchanged.
   - **FM-TXN-042** (L530-540): `accumulator_shard_fold_none_single_multi` (state.rs:437) is a
     `Forced by` name **and** the test whose dedup assertions this change consolidates. Row
     Invariant is about `None` resolving to `host.shard_id()`; unchanged. **Untouchable keep list
     inside that test — three items, not one:**
     - the **test name** and its **`// FM-TXN-042` tag (L435)** — renaming either silently breaks
       `just lint-failure-modes`;
     - **`state.rs:439`, `assert!(matches!(acc.target, TransactionTarget::None));`** — this is the
       row's **only in-crate witness** that a fresh accumulator is `None`, i.e. the state the row's
       Invariant is *about*. `scripts/failure-modes.py` checks `Forced by` names and `// FM-` tags,
       **not assertion content**, so a trimming pass that deletes this line — plausible, since the
       revision's stated goal is to shrink these tests to "which transition each caller selects" —
       would gut the row's witness with the lint still green. Whoever trims must keep L439;
     - `state.rs:444` (`Single(1)` after a repeated fold), which TX11.1a shows is the witness that
       forbids the `note_slot`-direction collapse.
     The assertions that *may* be trimmed are the shard-list dedup/append blocks at L449-470, whose
     property moves to `promote_to_multi`'s own test.
3. `just lint-failure-modes` after **both** `vll-failure-modes.md` edits (`:46`, `:68`) — also part
   of `just lint`. It checks names and tags, not prose, so neither edit can turn it red; run it to
   confirm the `Forced by` sets still resolve.
4. **Both crates re-gate.** `just mutants-diff frogdb-txn` and `just mutants-diff frogdb-vll`
   before pushing (push discipline); full `just mutants <crate>` + `just mutants-gate <crate> 0.90`
   for each half that lands. TX11 alone → `frogdb-txn` only; TX12 alone → `frogdb-vll` only.
   Expected movement, per *Mutation weight*: `frogdb-txn` **holds** (two new mutants, both killed by
   existing tests — it does *not* shrink), `frogdb-vll` **rises** (one guaranteed survivor deleted
   with `TransactionQueue::default`). If `frogdb-txn` drops, the new mutant that survived names the
   arm the factorization got wrong — read it, do not raise the gate.
5. **New tests go in the crate being mutated.** TX11's forcing test belongs in
   `frogdb-txn/src/state.rs`'s test module, next to the accumulator tests — not in
   `frogdb-server`, where it would contribute nothing to the 0.90 score.

**Other risks:**

- **`Multi`'s shard list changes contents.** `Multi(vec![0, 0])` → `Multi(vec![0])` in the
  same-shard-cross-slot case. Verified no production reader and no test asserts that specific
  list; `fold_keys_promotes_on_slot_mismatch_in_cluster_mode` (L426-430) matches on the variant
  only. Call it out in the PR anyway — it is the one place where "no observable change" is a claim
  about the *absence* of a reader rather than about equivalent values.
- **`TransactionQueue`'s `Default` deletion is *not* a public-API removal** — downgraded from the
  first draft, which called it a "`pub` item removal". `mod queue` is **private**
  (`vll/src/lib.rs:13`) and `lib.rs:18-28` re-exports nothing from it, so `TransactionQueue` never
  leaves the crate. Its only in-crate appearances are a private field (`shard.rs:70`) and two
  private fn signatures (`ensure_initialized` L103, `try_acquire_for` L185) — no public signature
  mentions it. So the deletion is crate-internal dead-code removal, with **zero callers repo-wide**
  and nothing deriving `Default` over a containing type. Not a compatibility question at all, in
  either direction; the reason to mention it is the mutation-score win in *Mutation weight* (c),
  not risk.
- **The `#[allow(clippy::result_large_err)]` on `resolve` (L32)** stays — unaffected.
- **No `frogdb-core` change.** `ShardVll` (`core/shard/types.rs:438`) is a type alias and
  `builder.rs:453` constructs via `Default`; both compile unchanged against eager fields.

## Effort estimate

- **TX11: S.** One file. Two new private methods (~25 lines), two call sites reduced to
  delegation, ~20 lines of duplicated match arms deleted, one new test, plus trimming the
  duplicated assertions out of the two existing accumulator tests. Zero spec edits. Zero signature
  changes. The `frogdb-txn` re-gate is the long pole, not the diff.
- **TX12: S.** One file plus five lines in another. Six field lines, one constructor, one deleted
  helper, **eleven** mechanical unwrap-site edits, one dead `impl` deleted, two spec lines (`:46`
  rewritten, `:68`'s parenthetical deleted). No test changes expected — if any `frogdb-vll` test
  fails, the "no observable change" claim is wrong and the change should stop.

### Independently-landable hotfix

**None, because there is no live defect** — TX11.3 is dormant behind an unread payload and TX12 is
pure ceremony. Claiming a hotfix here would be dressing up a cleanup as a fix.

The useful split is by crate rather than by severity, and it is **not** symmetric:

- **TX12 alone is the one with a schedule.** Siblings 45 *and* 46 are sequenced behind it — the
  agreed round order is **51 → 46 → 45** (46:587, echoed by 45:484). It is the smaller, more
  mechanical half, it carries both spec edits (`:46`, `:68`) that 46 and 45 then rebase over, and it
  is the half with the positive gate effect. **Do TX12 first.**
- **TX11 alone** is the only half that touches `frogdb-txn`, needs no spec edit at all, and should
  precede sibling 50's larger `state.rs` restructuring for the same land-the-small-one-first
  reason.

If only one lands this round, land TX12.
