# 22 — a replica's ACK is credited past the primary's live offset

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3. Found by the seeded replication sweep
([issue 12](../done/12-replication-seeded-sweep.md)) at the 500-seed budget — seeds 225
and 340 of 1..=500.

## What was found

`INV-OFFSET-3` is a **Hard**-tier catalog invariant:

> no replica is credited past the live offset, and none is credited at all
> before it can have acked on the wire

The sweep reaches it. `OffsetCoordinator::ingest_replica_ack` records whatever
offset arrived in the replica's `REPLCONF ACK` and only *then* runs the catalog
hook, so the number the wire supplied is already in the tracker by the time the
invariant is evaluated:

```
thread '<unnamed>' panicked at frogdb-server/crates/replication/src/offset_coordinator.rs:263:
replication invariants violated after OffsetCoordinator::ingest_replica_ack:
INV-OFFSET-3: replica 2 acked 307 past live 278
```

The hook is `#[cfg(any(test, debug_assertions))]`, so this is a debug-build
assertion rather than a release crash — but in a release build nothing rejects
the value either, and a replica credited at 307 against a live head of 278 is
counted by `WAIT` and by `min-replicas-to-write` for bytes the primary never
produced. That is durability arithmetic on a number the primary cannot vouch
for.

Two ways the sweep can produce it, and the ruling has to say which one it is
fixing:

- **The lineage is stale.** A crash-restarted primary comes back on the id it
  headed before the crash with its offset at the bottom
  ([issue 21](./21-a-restart-keeps-the-replication-id-it-lost-the-history-for.md)),
  a replica that followed the previous incarnation reconnects and acks its own
  higher offset. Fixing 21 removes this path.
- **The head moved down under a live lineage.** A promotion settles the new
  primary at its *applied* offset (`settle_at_applied` discards received-but-
  unapplied frames and stores the lower value into `live`), while a replica that
  had received more from the old primary acks the higher number it holds. No
  restart is involved, so fixing 21 does not remove this one.

Either way the ingest path itself takes the wire's word for it — which is the
same shape as [issue 18](./18-unvalidated-wire-replication-id.md), one field
over: 18 is the unvalidated replication *id* on the wire, this is the
unvalidated *offset*.

## Precedent

Redis's `replconfCommand` stores `REPLCONF ACK` into `slave->repl_ack_off`
without bounding it against `master_repl_offset`, and its `WAIT` implementation
counts `replicationCountAcksByOffset` over those raw values — so Redis has the
same unbounded field. What Redis does *not* have is a head that moves
backwards under a live id: `changeReplicationId` runs on every promotion and on
every restart that did not load an RDB, so a replica presenting a stale offset
is on a different id and is full-resynced instead of counted. The bound is
enforced by the identity, not by the ack. FrogDB has the catalog invariant
written down as Hard, which is a stronger claim than Redis makes, and something
must uphold it.

## Ruling needed

- (a) **Clamp at ingest.** `ingest_replica_ack` records `min(acked, live)` and
  logs the discrepancy. Cheapest, keeps the invariant true by construction, and
  loses nothing real — a replica cannot have durably held bytes the primary
  never sent. Hides the lineage bug rather than fixing it.
- (b) **Reject and resync.** An ack above the live head means the replica is on
  a history this primary is not serving: drop the replica's stream and force a
  full resync. Matches what Redis gets for free through its identity discipline.
- (c) **Fix it upstream only** (issue 21 plus a promotion-side rule that a
  replica whose position is above the settled head cannot be granted
  `+CONTINUE`) and leave the ingest path unguarded, on the argument that a
  clamp at the bottom masks the next lineage defect.

## Acceptance criteria

- [ ] Ruling recorded here with its reasoning
- [ ] Behaviour implemented, with a forcing test in `frogdb-replication` itself
      (`cargo mutants -p <crate>` runs only that package's own tests)
- [ ] The promotion path covered specifically: a replica that received more than
      the promoted node applied is not credited above the new head
- [ ] Failure-mode row added to
      `.scratch/hardening/specs/replication-failure-modes.md` naming its forcing
      test
- [ ] `known_panic_gap` in the sweep deleted, so the arm witnesses the fix

## Witness

`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs` —
`known_panic_gap` matches this signature and only this one, pinned by
`test_known_panic_gap_matches_only_the_filed_signature`; seeds that hit it are
counted and named on stderr rather than silently skipped. Replay with:

```
REPLICATION_SEED_TRACE=1 REPLICATION_SEEDS_START=225 just replication-seeds 1
```
