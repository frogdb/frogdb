# Pin MULTI/EXEC-vs-slot-migration boundary semantics deliberately

Status: done
PRD: [replication-cluster-rework/exec-slot-revalidation.md](../../../replication-cluster-rework/exec-slot-revalidation.md)
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Transactions / Cluster

## Context

Cluster-mode MULTI/EXEC test coverage is limited to the READONLY-replica case
(`integration_cluster.rs:6655-6776`, `cluster_sharded_pubsub_tcl.rs:40-181`). No test commits a slot
migration between queue-time and EXEC-time for a transaction. Slot ownership is validated at queue
time (`server/src/connection/guards.rs:567`); the EXEC path
(`server/src/connection/transaction.rs:234-241`) resolves and executes queued commands without
re-running `MOVED`/`ASK`/`TRYAGAIN` checks. This leaves a narrow race window: a transaction that
validated against slot ownership at queue time could execute at EXEC time against a slot that has
since transitioned to `MIGRATING` or moved away entirely, with no re-validation.

Verdict CONFIRMED L1/C2 — deliberately low consequence given the narrow race window and cluster-only
scope, but the current behavior (no re-validation at EXEC) is unpinned and undocumented, so it's
worth locking down deliberately rather than leaving as an accidental property of the code.

## What to build

A test that deliberately queues a keyed write in `MULTI`, transitions the owning slot to `MIGRATING`
(or completes a migration away) from a second connection before `EXEC` runs, then documents and
asserts whatever the actual current behavior is.

## Acceptance criteria

- [ ] Integration test: `MULTI`; queue a keyed write for a slot owned by the current node; from a
      second connection, `SETSLOT MIGRATING` (or complete a migration away) for that slot; `EXEC`
      on the first connection.
- [ ] Test asserts and documents the actual current behavior (no re-validation at EXEC per
      `transaction.rs:234-241`) as the deliberate, accepted semantics — with an explicit code
      comment explaining the race window and why it's accepted.
- [ ] If, during triage, the team decides EXEC *should* re-run `MOVED`/`ASK`/`TRYAGAIN` validation,
      escalate that as a separate design/implementation follow-up rather than silently changing
      behavior as part of this test-only task.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/F-cluster.md #8 (`multi-exec-across-migration-boundary`)
- .scratch/testing-improvements/audit/verdicts-F.md #8 (CONFIRMED L1/C2)
- server/src/connection/guards.rs:567
- server/src/connection/transaction.rs:234-241
- frogdb-server/crates/server/tests/integration_cluster.rs:6655-6776

## Resolution

Behavior probed empirically, then pinned. Two integration tests added at the end of
`frogdb-server/crates/server/tests/integration_cluster.rs`, under a new section banner
`MULTI/EXEC across a slot-migration boundary (testing-gap issue 55)` that carries the contract
write-up in a code comment (as the acceptance criteria require):

- `test_multi_exec_after_completed_slot_migration_commits_on_former_owner`
- `test_multi_exec_during_in_flight_slot_migration_commits_on_source`

### Pinned contract

**Slot ownership is validated at queue time only; EXEC never re-validates.**

`guards.rs::try_queue_in_transaction` calls `validate_cluster_slots` for every queued command, so a
MOVED/ASK/CROSSSLOT at queue time aborts the transaction (EXEC then replies `EXECABORT`).
`transaction.rs::execute_transaction` only folds queued keys into an intra-node *shard* target via
`TransactionTarget::resolve()` (which knows about shards and CROSSSLOT, not node ownership) and
sends the batch to that local shard. No MOVED / ASK / TRYAGAIN check runs between `MULTI` and the
shard round-trip.

### Measured behavior (both cases in the acceptance criteria)

1. **Migration completed before EXEC.** Queue `SET`+`GET` on an owned slot (`+QUEUED` each),
   complete a full slot migration away, then `EXEC` → `*2\r\n+OK\r\n$2\r\nv1` — the batch commits
   on the node that just lost the slot, and the queued `GET` observes the queued `SET`, so it is a
   genuine local commit. Aftermath: the *same connection's* next ordinary `GET` returns
   `MOVED <slot> <new-owner>`, the former owner redirects all reads for that slot, and the new
   owner still holds the pre-migration value. **The transaction's write is invisible cluster-wide
   and leaves an orphan copy on the former owner.** Control assertion in the same test: re-queuing
   the identical write *after* the migration is rejected with `MOVED` at queue time and `EXEC`
   replies `EXECABORT` — proving the gap is EXEC-time-only.
2. **Migration in flight (MIGRATING/IMPORTING held open) at EXEC.** The harness can hold this
   state: `SETSLOT IMPORTING` + `SETSLOT MIGRATING` without the closing `SETSLOT NODE`
   (`cluster_state().is_slot_migrating(slot) == true`, owner still the source). With the key already
   `MIGRATE`d away — a plain read of it correctly `ASK`-redirects — `EXEC` returns `*1\r\n+OK` and
   **resurrects the key on the source**, so the same key then exists on both sides of the open slot
   with different values (source `v1`, importing target `v0`), and the source stops ASK-redirecting
   for it.

### Divergence from Redis — honest, and deliberate

Redis diverges in both cases. Verified against `redis/unstable` sources (Redis 8.x), not from
memory:

- `cluster.c::getNodeByQuery` explicitly special-cases EXEC and re-resolves the *entire* queued
  transaction: `if (cmd->proc == execCommand) { if (!(c->flags & CLIENT_MULTI)) return myself; ms =
  &c->mstate; }`, recomputing slot / owner / `migrating_slot` / `importing_slot` / `missing_keys`
  over every queued command.
- `server.c::processCommand` then does, when the resolved node is not `myself`:
  `if (c->cmd->proc == execCommand) discardTransaction(c); clusterRedirectClient(c, n, c->slot,
  error_code);`

So Redis answers the EXEC itself with `-MOVED` (case 1), `-ASK` (case 2: migrating slot,
`missing_keys` and no `existing_keys`), `-TRYAGAIN` (keys split across the boundary) or
`-CROSSSLOT`, **and discards the queue**. FrogDB does none of that. The tests assert FrogDB's actual
behavior and the comment states the Redis contract next to it, so the divergence is documented
rather than implied.

### Escalation (acceptance criterion 3)

Not silently changed. Closing the window means re-running the redirect seam over the queued batch
inside `execute_transaction` — a behavior change, out of scope for a test-only task. Per the third
acceptance criterion this is left as a follow-up design decision; the two tests are the tripwire
that will fail (loudly, with the contract comment right above them) if EXEC is ever taught to
re-validate.

### Verification

- `just test frogdb-server 'test_multi_exec_(after_completed|during_in_flight)'` — 2/2 pass.
- 8 consecutive runs with `--retries 0` (nextest's `integration_cluster` override normally grants 2
  retries, which would mask flakes): 16/16 test executions passed, **0 flakes**. Migration timing is
  driven by explicit Raft-committed SETSLOTs plus `cluster_state()` assertions rather than sleeps
  alone, so there is no timing race to lose.
- `cargo clippy -p frogdb-server --all-targets -- -D warnings` clean; `just fmt` applied.

## Resolution addendum — 2026-07-28: the tripwire fired, the contract changed

The escalation above was accepted. [`exec-slot-revalidation.md`](../../../replication-cluster-rework/exec-slot-revalidation.md)
(Status: implemented, pending review) closes the window, and the two tripwire tests were rewritten
to assert the *new* contract rather than FrogDB's old divergence.

What EXEC does now: at `EXEC` entry, before any queued command reaches a shard, the whole queue's
keys are folded to a slot set and routed against **one** `ClusterState::snapshot()`. If the batch no
longer belongs here, `EXEC` discards the queue and answers with a bare `-MOVED` / `-ASK` /
`-TRYAGAIN` / `-CROSSSLOT`, matching `cluster.c::getNodeByQuery`'s EXEC special-case plus
`server.c`'s `discardTransaction` before `clusterRedirectClient`. Validating the whole batch up
front (rather than per command, mid-loop) is load-bearing: survivors of a mid-batch abort would ship
to replicas as one `MULTI`…`EXEC` frame, replicating a partial transaction.

Tests in `integration_cluster.rs`, rewritten or added:

- `test_multi_exec_after_completed_slot_migration_redirects_with_moved` — `-MOVED`, queue discarded,
  **and** `DBSIZE` on the former owner is 0 (the no-orphan-writes assertion; the orphan is otherwise
  invisible, because a normal read of the key is answered by the very `MOVED` the former owner
  emits).
- `test_multi_exec_during_in_flight_slot_migration_asks_when_keys_migrated` — `-ASK`.
- `test_multi_exec_during_migration_with_split_keys_returns_tryagain` — `-TRYAGAIN`.
- `test_multi_exec_during_migration_serves_when_keys_still_local` — serve-through.
- `test_multi_exec_on_import_target_with_asking_serves_the_batch` — requires `ASKING` to be sticky
  for the whole MULTI block (it was one-shot; `take_asking` no longer clears inside a transaction).
- `test_keyless_multi_exec_is_never_redirected`, plus tightened `READONLY` batch-eligibility tests.

Jepsen: `slot_migration.clj` gained a `:queued-txn` / `:exec-queued-txn` pair on a pinned
single connection that straddles a migration, and a fifth gating checker property —
`no-orphaned-writes?`, computed from a `:read-orphans` op that reads every non-owner node directly
with `CLUSTER GETKEYSINSLOT`.

Flake bar re-met: 8 consecutive `--retries 0` runs of the boundary tests, 8/8 × 29 passed.

Residual: the Raft-apply-latency window (a node that has not yet applied the reassignment still
validates itself as owner) is unchanged and now documented in
`website/src/content/docs/architecture/clustering.md`; closing it is
[replication-cluster-rework/issues/02](../../../replication-cluster-rework/issues/).
