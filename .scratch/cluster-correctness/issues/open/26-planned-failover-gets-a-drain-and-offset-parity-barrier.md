# 26 — Planned `CLUSTER FAILOVER` has no barrier, no drain and no offset-parity wait

Status: ready-for-agent

## Parent

[Adversarial design review — `specs/cluster.md` + the 2026-08-13 rulings](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-cluster.md),
finding **H2**. This is a spec-gap finding from the review, distinct from the amended rulings on
issues 14–20 and 25: those rulings each closed a defect the campaign had already filed and
adjudicated, while this finding is new — the review read FM-CLUSTER-041 and the operator handler
cold and found the graceful failover path was never given the same lossless-handoff treatment the
slot-migration path spent 14 FM rows building.

## What is wrong

FM-CLUSTER-041 (graceful failover) and the operator handler
(`frogdb-server/crates/server/src/commands/cluster/admin.rs:293-…`) validate roles and propose
`Failover` immediately: no client pause on the old primary, no wait for the successor's
replication offset to reach the primary's head, no fence. Consequences, on both the planned
(`CLUSTER FAILOVER`) and automatic paths:

- The old primary keeps admitting writes for its slots until *it* applies the demotion, while the
  successor starts serving as soon as *it* applies the promotion — two writers, bounded only by
  per-node apply lag. This is structurally identical to issue 17's stale-source finding, which the
  campaign ruled worth fixing for migration.
- Writes acked by the old primary above the successor's replication offset are discarded when the
  demoted node resyncs (the demotion/adopt-history rows in `specs/replication.md`). Silent loss of
  acknowledged writes on an operator-initiated, no-failure-present command.

Redis's manual `CLUSTER FAILOVER` is lossless by construction: the master pauses clients, the
replica waits until its offset matches, and only then does the swap happen (`FORCE`/`TAKEOVER` are
the documented lossy escapes). Raft leadership transfer (and CRDB's lease transfer) waits for the
target to catch up before handing over the same way. FrogDB's "graceful" failover today is a
demote, not a handover — and the review notes that the automatic path being lossy is defensible
(async PSYNC, Redis parity), but the spec never says so, and ruling 20 now routes the automatic
path through the same `force: false` shape as the planned path, which makes the two
indistinguishable in the rows as written.

## What to build

Spec-first: two FM rows, two different claims.

1. **Planned failover becomes lossless by construction**, reusing the migration machinery rather
   than inventing a parallel one:
   - arm slot pauses on the old primary (FM-CLUSTER-079 machinery);
   - drain in-flight writes (FM-CLUSTER-090/091);
   - wait for the successor's replication offset to reach parity with the old primary's head;
   - only then propose the swap.
   - `SlotFence` (FM-CLUSTER-095/100) carries the ownership token through the handoff exactly as
     it does for a slot migration, so the execute-seam fence half is free.
2. **A new, honest FM row for the automatic path**: automatic failover under async replication may
   lose the acked-but-unshipped tail. The divergence log records the window; `WAIT` is the
   per-write escape hatch an operator has today. This makes explicit what ruling 20's `force:
   false` shape left implicit.
3. **Cross-reference, don't re-solve**: lossless *unplanned* failover is a write-path property
   (`replication-durability = quorum`), not failover-time mechanics — that is a separate design
   question filed as replication-correctness issue 29 (the roadmap path). Cite it from the amended
   FM-CLUSTER-041 row rather than pulling its scope into this issue.

Amend FM-CLUSTER-041 (`specs/cluster.md`) to describe the drain-and-offset-parity sequence for the
planned path; add a new row (next `FM-CLUSTER-` id at merge time) for the automatic path's honest
lossy claim.

## Acceptance criteria

- [ ] FM-CLUSTER-041 amended to describe the pause/drain/offset-parity/propose sequence for planned
      failover, and a new FM row added stating the automatic path's async-lossy claim with `WAIT`
      as the escape hatch; `just lint-spec` green
- [ ] Forcing test in `frogdb-cluster`/`frogdb-cluster-runtime` reproducing H2's two-writers window
      (old primary still admits a write after the successor has already started serving) fails
      first against today's immediate-propose handler, then is fixed by the barrier
- [ ] A test asserting no acknowledged write is lost across a planned `CLUSTER FAILOVER` under
      concurrent load (mirroring FM-CLUSTER-095's `no_write_is_acknowledged_after_the_slot_is_
      handed_over_under_load` shape for migrations)
- [ ] `just mutants-diff` triaged on every touched locked crate (`frogdb-cluster`,
      `frogdb-cluster-runtime`, and `frogdb-replication`/`frogdb-replication-runtime` if the
      offset-parity wait touches replication-crate code)

## Blocked by

None. The migration machinery it reuses (FM-CLUSTER-079/090/091/095/097/100) is already shipped.

## Ruling (2026-08-13)

**Planned `CLUSTER FAILOVER` becomes lossless by construction: arm slot pauses (FM-CLUSTER-079 machinery), drain in-flight writes (FM-CLUSTER-090/091), wait for successor offset parity, then propose the swap — reusing the migration machinery, SlotFence carries the ownership token. The automatic path stays asynchronous-lossy: a new honest FM row states that automatic failover under async replication may lose the acked-but-unshipped tail (the divergence log records the window; `WAIT` is the per-write escape hatch). Lossless UNPLANNED failover is a write-path property, not failover-time mechanics: a separate design issue (replication-correctness issue 29, `replication-durability = quorum`) is filed as the roadmap path — cross-reference it. Precedent: Redis manual FAILOVER is lossless by construction (pause → offset parity → swap); Raft leadership transfer / CRDB lease transfer same shape.**
