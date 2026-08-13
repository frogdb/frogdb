# Blocking commands — failure modes

Every way a blocking command (`BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`/`BLMPOP`/`BZPOPMIN`/
`BZPOPMAX`/`XREAD BLOCK`/`WAIT`) can be resolved on the connection side, one table per mode.
Same contract as [Transactions — failure modes](txn.md): a mutant that survives
is a row nothing forces.

Scope: the **connection-side** blocking-wait path — `handle_blocking_wait` /`register_wait` /
`cleanup_wait` / `reconcile_unregister` in
`frogdb-server/crates/server/src/connection/blocking.rs`, and the three-way race arbitration in
`frogdb-server/crates/server/src/connection/blocking/coordinator.rs`. Rows stop at what the
connection observes across the shard channel; the shard-side wait queue (registration, FIFO
`pop_oldest_*`, the acknowledged `UnregisterWait` handshake, restore-on-send-failure) lives in
`frogdb-core` and gets its own spec.

## How to read a row

Fields as in [txn-failure-modes.md](txn.md#how-to-read-a-row). `Outcome variant`
here names the `WaitOutcome` variant the coordinator settles on, the blocking-path analogue of the
txn spec's transaction outcome.

Test names are bare function names, resolved against
`cargo nextest list -p frogdb-txn -p frogdb-vll -p frogdb-server` (core command profile).
That listing profile does **not** build `frogdb-core`, `frogdb-shard-harness`, or anything behind
`--features turmoil`/`--features shuttle`, so the following genuinely load-bearing guards exist but
are deliberately not cited in `Forced by` rows:

- `frogdb-server/crates/core/tests/concurrency.rs`, module "Blocking-pop exactly-once conservation
  (serve-vs-timeout race)" — the shuttle model of the shard-side handshake, including two
  `#[should_panic]` sensitivity witnesses.
- `frogdb-server/crates/server/tests/concurrency_workload.rs` (turmoil) — the whole-history
  `check_exactly_once_delivery` / `check_fifo_wake_order` conservation checkers over generated
  seed sweeps, plus
  `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0`.

---

## FM-BLOCKING-001 — shard delivers a value

| Field | Value |
|---|---|
| Trigger | The shard pops an element for this parked waiter and sends it on the wait's `oneshot` response channel before the deadline elapses and before any `CLIENT UNBLOCK`. |
| Observable | The shard's `Response` verbatim — for `BLPOP` the 2-element `[key, element]` array, for `BLMOVE` the moved element. |
| NOT observable | A timeout reply while the value was in flight; the element also remaining in (or being restored to) the source key; the same element being handed to a second waiter. This is the exactly-once half: delivered XOR retained, never both, never neither. |
| Invariant | `wait_for_response` is `biased` with the response branch first, so a value that becomes ready in the same poll as an already-elapsed deadline still wins. The receiver is *borrowed*, not consumed, so a value the shard sends inside the pop→deliver window remains drainable by `cleanup_wait` (see FM-BLOCKING-005). |
| Outcome variant | `Response(_)` |
| Forced by | `response_wins`, `biased_response_beats_elapsed_deadline` |
| Bug refs | none |

## FM-BLOCKING-002 — deadline elapses with nothing delivered

| Field | Value |
|---|---|
| Trigger | A finite blocking timeout expires with the waiter still parked: no shard delivery, no `CLIENT UNBLOCK`. |
| Observable | The op-aware nil: a null **array** for array-returning ops (`BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`, `XREAD BLOCK`), a null **bulk** for single-value ops (`BLMOVE`, `BRPOPLPUSH`). |
| NOT observable | One nil shape for both op families (the wrong-shape bug `into_response` exists to fix); an element being consumed from any key; the waiter staying registered after the reply. |
| Invariant | `WaitOutcome::Timeout` carries no data, and `into_response` re-derives the shape from the surviving `BlockingOp`, so timing out is a pure no-op against the store. |
| Outcome variant | `Timeout` |
| Forced by | `timeout_wins_when_idle`, `timeout_reply_picks_nil_shape_per_op` |
| Bug refs | none |

## FM-BLOCKING-003 — CLIENT UNBLOCK targets the parked connection

| Field | Value |
|---|---|
| Trigger | `CLIENT UNBLOCK <id>` (or `… ERROR`) fires on a connection parked in a blocking wait, before any shard delivery or the deadline. |
| Observable | `ERROR` mode: `-UNBLOCKED client unblocked via CLIENT UNBLOCK`. `TIMEOUT` mode: exactly the FM-BLOCKING-002 reply, op-aware nil shape and all. |
| NOT observable | A delivered element (unblocking must never consume); a bare `Response::Null` standing in for the array-shaped nil; the unblock being swallowed while the wait had no deadline (an infinite `BLPOP 0` must still be unblockable). |
| Invariant | The unblock branch is a peer of the response and deadline branches in the same `select!`, so it resolves even when `deadline == None` (which parks the timeout branch on `pending()` forever). |
| Outcome variant | `Unblocked(UnblockMode::{Error,Timeout})` |
| Forced by | `unblock_wins_over_idle_wait`, `timeout_reply_picks_nil_shape_per_op` |
| Bug refs | none |

## FM-BLOCKING-004 — response channel closed without a value

| Field | Value |
|---|---|
| Trigger | The shard drops the wait's response sender without sending — shard shutdown, or the waiter being dropped by a GC/teardown path. |
| Observable | `Response::Null`. The connection is released, not wedged. |
| NOT observable | The wait hanging forever on a dead channel; a panic on `RecvError`; a fabricated element. |
| Invariant | `Err(_)` from the borrowed receiver maps to `WaitOutcome::Response(Response::Null)`, so channel closure is a terminal outcome of the race rather than an error path. |
| Outcome variant | `Response(Response::Null)` |
| Forced by | `channel_drop_yields_null_response` |
| Bug refs | none |

## FM-BLOCKING-005 — serve races the chosen timeout (pop→deliver window)

| Field | Value |
|---|---|
| Trigger | The coordinator has already chosen `Timeout`/`Unblocked` when the shard, concurrently, pops an element for this same waiter and sends it. `oneshot::send` returning `Ok` does **not** mean the client saw the value. |
| Observable | Whatever the shard's authoritative ack says: on `UnregisterAck::Unregistered` the timeout reply stands and nothing was consumed; on `UnregisterAck::AlreadyServed` the raced value is drained from the still-open receiver and delivered. |
| NOT observable | An element popped by the shard and dropped on the floor because the connection had moved on — the exact "neither delivered nor in final state" loss the conservation checkers report. |
| Invariant | The shard's serial mailbox is the single serialization point: `cleanup_wait` sends `BlockingMsg::UnregisterWait { ack }` and `reconcile_unregister` awaits the ack before deciding, and the coordinator borrows rather than consumes `response_rx` precisely so the `AlreadyServed` value is still there to drain. |
| Outcome variant | `Timeout` / `Unblocked(_)`, reconciled after the fact |
| Forced by | `already_served_drains_the_raced_value`, `unregistered_keeps_the_timeout_reply`, `closed_ack_channel_keeps_the_timeout_reply`, `already_served_with_closed_response_channel_falls_back` |
| Bug refs | `.scratch/testing-improvements/issues/07` |

> The cited four exercise `reconcile_ack` — the reconciliation decision extracted out of
> `reconcile_unregister` so it can be driven over in-memory channels without a live shard — across
> both `UnregisterAck` variants plus each channel-closure degradation. The end-to-end guards for
> this mode (the shuttle conservation model in `frogdb-server/crates/core/tests/concurrency.rs`
> with its `#[should_panic]` witnesses, and the turmoil sweep's `check_exactly_once_delivery`) live
> outside the listing profile above and so cannot be cited here.
