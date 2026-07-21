# 06 — Durability phase: cross-shard txn framing + abort-on-recovery

Status: needs-triage
Type: AFK
Origin: [issue 05](./05-vll-phase3-partial-commit-decision.md) decision (2026-07-21) — option 2b, deferred to the durability phase

## What to build

Cross-shard transaction framing so WAL recovery stops resurrecting the partial
commit that option 4 (4a supervise + 4c abort, `frogdb-server/crates/server/src/server/shard_supervisor.rs`,
merged `da1c0838`) fail-stops on but does not itself undo from disk:

- **Per-shard WAL txn framing**: tag records belonging to a cross-shard VLL
  transaction with its `txid` (or an equivalent grouping marker) so recovery
  can identify which appended records belong to the same cross-shard
  operation.
- **Coordinator intent record**: written before phase-3 dispatch
  (`VllCoordinator::scatter_gather`, `frogdb-server/crates/vll/src/coordinator.rs:266-288`),
  naming all participating shards for the `txid`. This is what lets recovery
  distinguish "every participant's portion made it to its WAL" from
  "the node fail-stopped mid-dispatch, so some participants never got there."
- **Abort-on-recovery policy**: on restart, recovery **discards** (does not
  replay) any cross-shard transaction whose intent record lacks a matching
  completion marker from every named participant — rather than attempting
  deterministic re-execution of the missing portion (the more expensive
  option 2 path in issue 05, not chosen).

  This is sound *specifically because* the node fail-stops on phase-3
  dispatch failure (4c abort, already implemented): the coordinator never
  acked EXEC to the client for an incomplete cross-shard transaction, and —
  because the node aborts the process rather than continuing to serve reads
  — no client observed the partial state before the crash either. Discarding
  the incomplete transaction at recovery is therefore consistent with what
  every observer (client and reader) was told. Without 4c's fail-stop, this
  policy would be unsound: a live node could have kept serving reads of the
  partial state and then reversed them out from under readers at recovery.

- **Pairs with** the already-deferred rollback-mode WAL partial-append work
  (a mid-transaction WAL *write* failure leaving earlier same-transaction
  records already appended while the store rolls back — a related
  recovery-replay inconsistency candidate for *single-shard* rollback-mode
  transactions, tracked in the phase-4b plan's Honest Scoping Notes,
  [`docs/superpowers/plans/2026-07-20-concurrency-phase4b-shard-driver.md`](../../../docs/superpowers/plans/2026-07-20-concurrency-phase4b-shard-driver.md)).
  Both are durability-phase, both are about a WAL that can contain a
  fragment of a transaction that never fully lands; the recovery scanner
  built for one should cover both shapes.

## Acceptance criteria

- [ ] Per-shard WAL record format carries cross-shard `txid`/grouping
      metadata (or documented equivalent) sufficient to identify all records
      belonging to one cross-shard transaction
- [ ] Coordinator writes an intent record before phase-3 `VllExecute`
      dispatch, naming every participating shard
- [ ] Recovery scanner identifies incomplete cross-shard transactions
      (intent record present, completion marker missing from ≥1 named
      shard) and discards their partial records instead of replaying them
- [ ] Recovery behavior is covered by a test that crashes mid-dispatch
      (or replays a captured WAL fixture with a deliberately incomplete
      cross-shard transaction) and asserts the partial writes are absent
      after recovery
- [ ] Design note or test explicitly ties this policy's soundness to 4c's
      fail-stop guarantee (no live reads served after a phase-3 dispatch
      failure) — if 4c's guard conditions ever change, this issue's
      soundness argument must be re-reviewed
- [ ] Combined with the rollback-mode WAL partial-append fix (single-shard
      case), the durability-phase recovery scanner handles both transaction
      shapes without duplicated scanning logic

## Blocked by

Durability phase (not started). No persistence real path (RocksDB/FrogDB
WAL) exists in the concurrency-testing harness yet — this issue depends on
that phase landing first (see the spec's ["Known blind spots"](../../../docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md#known-blind-spots-accepted)
and ["Future phases"](../../../docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md#phasing) sections).
