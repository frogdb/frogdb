# 31: Slot migration redesign — source-authoritative-until-commit

Status: ready-for-human

> **STATUS** (2026-08-19): design complete (revision 35, adversarial-review loop closed) and the
> Quint rework is DONE and merged to main at `ce75ec0d`: phases Q1 (state/action rework), QR
> (modularize/DRY), Q1F (doc alignment), Q2 (37 invariants, 23 witnesses, 71+4 run tests),
> Q3 (73-row mutation battery), Q4 (battery gap closure incl. the ext-15/16 two-plane/lineage
> machinery; 4 honest residual misses documented in the Q4 report).
> Design doc: [2026-08-14-issue31-migration-design.md](../../2026-08-14-issue31-migration-design.md).
>
> **Remaining to build**: the spec amendment + implementation campaign per the design doc's
> `## Spec / impl blast radius — full verdicts` section (~40+ LOCKED-row rewrites/retires/adds in
> `specs/cluster.md`, each landing spec-first with its forcing tests, plus the Rust
> implementation).
>
> **All design-owner flags RULED and staging RULED** (2026-08-22 grill,
> [rulings ledger](../../2026-08-22-work-item-rulings.md)): the three semantics flags settled as
> R3 (adoption-time invariant replaces `inv_no_hold_during_staged_flip`), R4 (refusal class
> minted at verdict + carried in payload — arm 4b is reachable, **supersedes** the 2026-08-19
> "arm 4b deleted" note), R5 (stale-never-admits replaces
> `inv_no_record_outlives_its_registration`) — model work filed as
> [issue 43](43-quint-model-semantics-fixes-from-rulings.md); attribution corrections +
> accepted-limitation notes batched as
> [issue 44](44-design-doc-attribution-corrections-batch.md). Staging ruled R1: conflict-clustered
> sequential waves, one implementer per cluster, merge to main at wave boundaries; **wave-0
> decomposition dispatched 2026-08-22** — drafts land under
> `.scratch/cluster-correctness/campaign-31-decomposition/` for human review, with an approval
> gate before any implementation wave. Q6 promotion-retry boundedness folded into the
> decomposition brief as a candidate doc extension (R11).
> Do not start independent implementation work on the slot-migration area outside the campaign
> waves.

## Origin

Ruled by the user 2026-08-14 while triaging the independent distsys review's MAJ-5
(see [rulings](../../../formal-spec/2026-08-13-distsys-review-rulings.md)). MAJ-5 found the
issue-17 ruling left the source's finalization write pause with no release path when the
target dies mid-handoff (issue 17's amendment delegates liveness to an "issue 18 reconcile
abort" that issue 18 never defines). Root cause is structural: the Redis-style bulk phase
deletes keys from the source per `MIGRATE`, so mid-migration state is a split keyspace —
which is the only reason abort needs repatriation (issue 15 amendment) and the only reason
a dead target is hard to abort away from.

## What to build

Replace the Redis-style delete-as-you-copy bulk phase with the FoundationDB/CockroachDB
shape: **the source remains the sole authority for the slot until `CompleteSlotMigration`
applies.**

- Source retains every key and serves all reads and writes for the slot for the entire
  migration. No `ASK` redirects during the bulk phase; clients never observe a split slot.
- Target ingests a slot snapshot, then tails a **slot-scoped mutation stream** from the
  source (reuse the replication feed machinery — a migration target is a slot-filtered
  replica session) until its lag reaches parity.
- `PrepareSlotHandoff` is proposed only at parity; the write barrier covers the final
  in-flight drain only (the window the design already intends). `Complete` admission stays
  the CRIT-2 logical token: drain confirmation carries the drained `handoff_seq`; admitted
  iff it matches. No wall-clock anywhere (global ruling).
- **Abort is target-discard**: the target drops its partial slot state; the source was
  authoritative throughout, so abort is safe at any moment, including with a dead target.
  Issue 15's repatriation amendment is superseded and its machinery is not built.
- **Orphan-abort trigger** (the MAJ-5 gap, still required): the level-triggered reconcile
  pass (issue 18 machinery) proposes `AbortSlotMigration` for any open migration whose
  counterparty carries the FAIL flag — same no-clock criterion as failover selection.
  Under this design the abort is trivially safe, so no partial-transfer special case.
- Source deletes the slot's keys only after `Complete` applies. Transient cost: double
  storage for one migrating slot; state the bound in the spec.

## Spec/impl blast radius

- TR-CLUSTER-010..013 rewritten; FM-CLUSTER-026/027 (`ASK`/`RESTORE` importing gates)
  re-derived — importing-side acceptance shrinks to the cutover protocol or disappears;
  FM rows for bulk-phase key movement replaced by mutation-stream rows.
- Issue 15 (repatriation) superseded — close against this issue when it lands.
- Issue 17/18 cross-refs: barrier liveness = reconcile orphan-abort defined here.
- Task-2 quint model (`specs/quint/cluster_migration_failover.qnt`) encodes repatriation
  (`repatriating` phase, `inv_abort_repatriates`, `completeRepatriation`); rework to
  target-discard semantics after this design is settled. Phase-3 models encode the new
  semantics from the start.
- Redis deviation table: document divergence (Redis splits the slot during migration;
  FrogDB does not — deviation is an improvement, no `-ASK` during bulk phase).

## Acceptance criteria

- [ ] Design doc (brainstorm with user — HITL) covering: mutation-stream mechanics and
      backpressure, parity definition, cutover drain bound, snapshot+tail consistency,
      client-visible semantics (`MOVED` only after `Complete`), operator observability
- [ ] Spec rows rewritten spec-first (rows → forcing tests → impl)
- [ ] Quint model updated; abort-safety invariant (abort admissible in every migration
      state, target-discard leaves source authoritative) checked
- [ ] Reconcile orphan-abort implemented with FAIL-flag criterion, no wall-clock
- [ ] Issue 15 closed as superseded

## Blocked by

None — but design (HITL) must precede phase-3 replication/cluster model work that would
otherwise encode the old migration semantics.
