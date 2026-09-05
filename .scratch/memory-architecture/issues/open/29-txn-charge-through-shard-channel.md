# 29: TxnBuffering hand-off — carry the `Charge` through the shard channel

Status: needs-triage
Type: AFK
Origin: whole-branch review 2026-09-05
Area: frogdb-txn + frogdb-core + frogdb-replication + frogdb-memory
Phase: 6 — polish. Follows the lock; **locked-area work: spec-first discipline applies.**

## Why

Verbatim, from the whole-branch review's finding 3:

> **TxnBuffering hand-off window: batch bytes charged to nobody between EXEC and the shard's
> gate.** `txn/src/exec.rs` (`drop(charge)` before the shard round trip) →
> `core/src/shard/execution.rs::charge_transaction_buffer` (charged only after routing fence /
> rate limiter / watch check). Same shape in `replication/src/apply.rs` (`drop(txn.charge)`
> before `apply_group`). FM-TXN-054/FM-PERSISTENCE-062 document why (no double count on one
> budget) but the "hard cap" is not a cap on resident bytes: while batch A (up to 512 MiB)
> sits in the shard mpsc the budget is empty, so connection B can queue and EXEC another full
> budget; under a slow shard resident uncharged bytes scale with connection count. Fix without
> double counting: carry the `Charge` (it is `Send`) inside `CoreMsg::ExecTransaction` / the
> pending-group message and have `charge_transaction_buffer` `grow` that charge by the
> rollback-snapshot delta rather than opening a fresh one; spec drops the "released before the
> round trip" clause. Add a forcing test that `charged()` never reads zero while a batch is in
> the shard channel.

The release-before-round-trip was the [issue 21](../done/21-txn-budget-hard-cap.md) review's
fix for a real Critical — a genuine double charge on single-shard nodes, where the connection
charge and the shard charge landed on the same budget for the same refcount-shared bytes. That
fix is correct about the double count and wrong about the cap: it bought single-counting by
paying with a window in which the bytes are counted zero times. The bytes are resident the
whole time — they sit in the shard's mpsc queue — so the budget under-reports exactly when the
shard is slow, which is exactly when the cap matters.

## What to build

### 1. Move the charge, do not re-open it

The `Charge` is `Send`. Instead of dropping it at the hand-off and minting a fresh one on the
far side, move it into the message:

- `CoreMsg::ExecTransaction` carries the connection's `Charge` alongside the batch.
- The replicated pending-group message (`frogdb-replication`) does the same with
  `txn.charge`.

`charge_transaction_buffer` then takes the arriving charge and **grows** it by the
rollback-snapshot delta, rather than opening a new one against the budget. One charge, one
budget, one owner at a time, and no instant at which the bytes are unaccounted: ownership
transfers with the message.

Release stays where it is — the charge drops when the shard is done with the batch.

### 2. `Charge::grow_for(&[ParsedCommand])`

Three hand-rolled "sum `retained_bytes` then `grow`" loops exist today
(`txn/src/state.rs`, `replication/src/apply.rs`, `core/src/shard/execution.rs`). Fold them into
one helper on `Charge`. With the charge carried rather than re-opened, finding 3's fix becomes
a single `grow_for` call at the shard end, which is the point of the helper.

### 3. Spec rows, amended together

FM-TXN-054 and FM-PERSISTENCE-062 currently state the hand-off as a released-then-recharged
pair. Both drop the "released before the round trip" clause and instead state that the charge
travels with the batch, so the budget's `charged()` covers the bytes continuously from queue
time to apply. Amend them in one change — they describe two ends of one hand-off and must not
disagree.

Forcing test: **the budget's `charged()` never reads zero while a batch is in the shard
channel.** Drive an `EXEC` on a shard whose worker is parked, observe `charged()` from the
budget while the message is in flight, unpark, assert the charge released only after apply.
Put it in the mutated crate (`cargo mutants -p <crate>` runs only that package's own tests).

## Acceptance criteria

- [ ] `CoreMsg::ExecTransaction` and the replicated pending-group message carry the `Charge`.
- [ ] `charge_transaction_buffer` grows the arriving charge; it no longer opens a fresh one.
- [ ] `Charge::grow_for(&[ParsedCommand])` exists and replaces all three hand-rolled loops.
- [ ] Forcing test: `charged()` never reads zero while a batch is in the shard channel; proven
      to fail without the fix.
- [ ] The single-shard double-charge regression test from issue 21 still passes — this must not
      reintroduce the Critical it fixed.
- [ ] FM-TXN-054 and FM-PERSISTENCE-062 amended together, "released before the round trip"
      gone, new forcing test named on both rows; `just lint-spec` green.
- [ ] `just mutants-diff frogdb-txn` and `just mutants-diff frogdb-replication` before push.

## Files likely touched

- `frogdb-server/crates/txn/src/exec.rs`, `state.rs`
- `frogdb-server/crates/core/src/shard/execution.rs`
- `frogdb-server/crates/core/src/shard/` (the `CoreMsg` definition)
- `frogdb-server/crates/replication/src/apply.rs`
- `frogdb-server/crates/memory/src/budget.rs` (`Charge::grow_for`)
- `specs/txn.md` (FM-TXN-054), `specs/persistence.md` (FM-PERSISTENCE-062)

## Out of scope

The deferred-command window recorded in issue 21's Resolution (a `MULTI` whose commands are all
connection-level/server-wide makes no shard round trip). The budget refusal reply prefix —
that is [issue 31](31-budget-refusal-prefix-and-counting.md). Changing the 512 MiB default or
the per-core-vs-global limit shape. `retained_bytes` over-counting shared read-buffer slices.

## Depends on

Nothing.

## Blocks

Nothing.
