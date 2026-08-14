# 18: Slot drain orders by registration ordinal; per-key FIFO row restated honestly

Status: done

## Origin

Distsys-review MAJ-15 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **restate + ordinal drains** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

The `specs/blocking.md` state-space row for `next_seq`/`seq_by_slot` claims the
registration ordinal drives per-key FIFO order ("is this ordinal, **not** insertion
order into `waiters_by_key` alone") and slot-scoped ordering. Both claims are false:

- Writes: `frogdb-server/crates/core/src/shard/wait_queue.rs:162-202`. The **only**
  read is `dump()` (`:604`, `DEBUG WAITQUEUE`). No pop path (`:246`/`:430`/`:521`)
  consults it.
- Per-key FIFO order *is* the `VecDeque` push order.
- `drain_waiters_for_slot` (`:346`) iterates `self.waiters_by_key.keys()` — HashMap
  order, nondeterministic; the opposite of the claim.

Why it matters: this row is the load-bearing fairness citation for
TR-BLOCKING-001/013 and the turmoil `check_fifo_wake_order` checker. A mutant of
`seq_by_slot` is invisible to every pop path and survives the mutation gate with no
fairness consequence — the spec is actively misleading the gate.

## Ruled shape

Both halves, combined:

1. **Per-key FIFO**: restate the row — the per-key `VecDeque` push order is the FIFO
   authority. Making pop paths consult the ordinal would be churn for zero behavior
   change.
2. **Slot-scoped drain**: `drain_waiters_for_slot` orders by the registration ordinal
   — `seq_by_slot` gains the real reader the row claims, cross-key drain becomes fair
   (oldest registered waiter drained/replied first) and deterministic, and
   ordinal-mutants become killable.

## What to build (spec-first)

1. Restate the state-space row: `VecDeque` = per-key FIFO authority; `seq_by_slot` =
   slot-scoped drain order authority (its readers: `drain_waiters_for_slot`, `dump()`).
2. Code: `drain_waiters_for_slot` collects the slot's waiters and drains in ascending
   registration ordinal instead of HashMap key order.
3. Forcing tests:
   - Two keys in one slot, registrations interleaved across the keys → drain replies
     in global registration order (fails pre-fix under a seeded HashMap order, or via
     an ordinal-mutant check).
   - Mutant kill: corrupting `seq_by_slot` (e.g. reversing ordinals) flips the drain
     order and fails the test — the previously gate-invisible mutant dies.
4. Re-derive TR-BLOCKING-001/013 citations and the turmoil `check_fifo_wake_order`
   checker text against the restated row (per-key claims cite the deque; slot-drain
   claims cite the ordinal).

## Cross-references

- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md): the restructure moves
  wait-queue interaction into run-loop states; the drain-order authority must survive
  the move. Coordinate if in flight.
- [Issue 08](08-blocking-command-rows.md): blocking row family.

## What landed

- State-space rows restated: the `waiters_by_key` deque's push order is the per-key
  FIFO authority (no pop path reads the ordinal); `next_seq`/`seq_by_slot` is the
  cross-key drain-order authority, read by `drain_waiters_for_slot` and `dump()`.
- `drain_waiters_for_slot` sorts the collected slab indices by `seq_by_slot` before
  draining, so a slot migration answers the oldest registered waiter first across all
  the slot's keys instead of inheriting HashMap iteration order.
- TR-BLOCKING-018 postcondition + FM-BLOCKING-008 invariant/NOT-observable state the
  ordinal order; TR-BLOCKING-001's park postcondition and TR-BLOCKING-006's serve
  postcondition re-derived against the deque authority; the turmoil
  `check_fifo_wake_order_exact` note now says it judges *per-key* order.
- Forcing tests in `frogdb-core` (`shard::wait_queue::tests`):
  `slot_drain_answers_in_registration_order_across_keys` (interleaved registrations
  over two keys of one slot — any key-grouped drain gives `[1, 3, 2, 4]` or
  `[2, 4, 1, 3]`, so it fails pre-fix under *every* hash order; proven failing:
  `left: [1, 3, 2, 4]`) and `slot_drain_order_survives_hashmap_iteration_order`
  (32 single-waiter keys — the ordinal-mutant kill; proven failing pre-fix).
