# WATCH is never slot-validated — CAS over a slot the node no longer owns

Status: done
Type: AFK
Origin: adversarial review of exec-slot-revalidation implementation (2026-07-28)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: cluster / transactions

Resolution — both halves of the fix direction landed, the second in a different shape than
proposed. (a) WATCH is slot-validated inside the `TransactionControl` stage itself, which is
where it structurally lands: `transaction_conn_command.rs::dispatch_transaction_command` calls
`PreDispatchView::validate_watch_slots` (`connection/guards.rs`) before the WATCH handler runs,
which folds the watched keys into a `BatchKeys` and routes them through the new
`slot_migration/routing.rs::route_watched_keys` — so a foreign slot gets `-MOVED`, an unassigned
slot `-CLUSTERDOWN`, and a two-slot watch `-CROSSSLOT`, all from `frogdb_core::redirect::*`.
The block-scoped ASKING flag is read with a new non-consuming `ConnectionState::is_asking()`
peek, so validating a WATCH cannot strand the MULTI that follows it. Spec: **FM-TXN-048**.
(b) EXEC re-checks the watch set, but *not* by folding it into `BatchKeys` as the fix direction
suggested: that would turn a watch set legitimately spanning two locally-owned slots into a
bogus `-CROSSSLOT` (`TxnSlotAccumulator::take()` folds watch *shards*, never watch slots, so
such a transaction commits fine today) and would emit a `-MOVED` naming a slot the queued batch
never touches. Instead `frogdb-txn`'s `exec.rs` gains a separate gate — `TxnHost::
watched_slots_still_local` → `PreDispatchView::watched_slots_still_local` →
`routing.rs::watch_slot_is_locally_served` — taken *after* the queue's own verdict (a redirect
is the more actionable answer) and yielding the ordinary nil watch abort
(`TransactionOutcome::WatchAborted`, metric label `watch_aborted`) when a watched slot has left
the node. Open migration windows are deliberately still serviceable. Spec: **FM-TXN-049**;
FM-TXN-020 and FM-TXN-033 `Bug refs` updated to point at both rows.
Tests: `watch_on_a_foreign_slot_is_refused_with_moved`, `watch_across_two_slots_is_crossslot`,
`watch_on_an_open_migration_is_accepted`, `watch_on_an_unassigned_slot_is_clusterdown`,
`watch_slot_locally_served_accepts_an_open_migration`,
`watch_slot_locally_served_rejects_a_slot_owned_elsewhere`
(`slot_migration/tests.rs`); `a_watched_slot_that_left_this_node_aborts_the_watch`,
`a_queue_redirect_outranks_the_watched_slot_abort` (`txn/tests/exec_outcomes.rs`);
`test_watch_on_a_slot_this_node_does_not_own_is_moved`,
`test_watch_then_slot_reassignment_then_keyless_exec_aborts_the_watch`
(`integration_cluster.rs`) — the last one reassigns the slot with a bare `CLUSTER SETSLOT …
NODE` over an open window rather than a real MIGRATE, because MIGRATE's delete bumps the
watched key's version and the ordinary CAS would then abort for the wrong reason.
No replication impact: the fix only *prevents* a batch from running, so nothing new reaches the
WAL or the replication stream.

## Problem

`WATCH_SPEC` is `KeySpec::All` + `ExecutionStrategy::ConnectionLevel(Transaction)`
(`transaction_conn_command.rs:236-244`). The `TransactionControl` dispatch stage
(position 5 of `PRE_DISPATCH_ORDER`) handles WATCH before `ClusterSlotValidation`
(position 14) is ever reached, so WATCH bypasses slot validation structurally — the
exec-slot-revalidation fix to `is_cluster_exempt` cannot cover it (un-exempting
`Transaction` is a no-op given the stage ordering).

Watch keys also never enter `BatchKeys` at EXEC time: `fold_queued_batch` folds the
*queued commands'* keys only.

## Failure scenario

Node A formerly owned slot S, now migrated away.

```
WATCH {S}k        -> +OK on A (no MOVED, wrong per Redis: watch on non-owner is MOVED)
MULTI
PING              -> queued (keyless body)
EXEC              -> executes on A after CAS-checking {S}k, a slot A no longer owns
```

The EXEC-entry validation sees an empty batch (PING is keyless) and serves locally.
The CAS decision is made against a stale, non-owned copy of `{S}k`. Narrow — requires
a keyless (or same-slot-exempt) transaction body — but a real correctness hole in the
WATCH contract.

## Fix direction

Slot-validate in the WATCH handler (or the `TransactionControl` stage): reply `-MOVED`
when a watched key's slot is not owned, matching Redis (`clusterRedirectClient` runs
for WATCH like any keyed command). Additionally consider folding watched keys into
`BatchKeys` at EXEC entry so a migration between WATCH and EXEC redirects the
transaction rather than CAS-ing a stale slot.
