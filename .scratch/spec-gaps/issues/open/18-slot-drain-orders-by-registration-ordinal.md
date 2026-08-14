# 18: Slot drain orders by registration ordinal; per-key FIFO row restated honestly

Status: ready-for-agent

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

## Acceptance criteria

- [ ] Row restated (deque = per-key authority; ordinal = slot-drain authority);
      `just lint-spec` green
- [ ] `drain_waiters_for_slot` drains in registration-ordinal order
- [ ] Forcing tests land; ordinal-mutant killable
- [ ] TR-BLOCKING-001/013 + `check_fifo_wake_order` citations re-derived

## Blocked by

None — can start immediately (coordinate with issue 13 if in flight).
