# Issue-31 Quint rework — phase Q3 report (mutation battery)

Base commit `c8ed899b` (Q2). Worktree `/Users/nathan/workspace/frogdb/.claude/worktrees/cluster-correctness-prd`.
Doc = `.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`, `## Quint rework`
(lines 10109–11358).

Model files: `specs/quint/cluster_migration_failover.qnt` (`F`),
`_logic.qnt` (`L`), `_machine.qnt` (`M`), `_types.qnt` (`T`).

Oracles used per row:
- **T** = `quint test specs/quint/cluster_migration_failover.qnt` (51 deterministic run tests, ~8s).
- **R** = `quint run … --max-samples=2500 --max-steps=40 --invariants <all 37>` (~4s), which
  names the violated invariant. Budget cap per the brief: 2500×40.

---

## Battery list (built from the doc before any mutation was run)

Numbering: `Mnn` is this report's id; the doc's own mutation numbers (1)–(18) are cited where
they exist. Rows marked *derived* have no explicit "mutation test:" sentence but are the
extension's acceptance bar ("reverting the design fix in the model must violate the named
property", doc:10121–10122) applied to a property the doc names.

| # | Doc cite | Mutation | Named detector |
|---|----------|----------|----------------|
| M01 | 10123–10128 (ext-1, V4-C2) | `Complete` admitted on a cross-tag (run-tag-blind) position comparison | `inv_no_acked_write_lost` |
| M02 | 10129–10130 (ext-2, v2-C2) *derived* | drop the fenced-source conjunct from `Complete` | `inv_complete_requires_fenced_source` |
| M03 | 10131–10132 (ext-3, v2-C3) *derived* | drop the `Draining`-phase conjunct from `Complete` | `inv_complete_requires_draining_phase` |
| M04 | 10141–10146 (ext-4, V7-C3) | revert `confirmDrained`'s observation-counter reset | `inv_draining_bound_counts_post_seal_observations` |
| M05 | 10133–10140 (ext-4, v2-C4) *derived* | drop `reconcileTick`'s progress-driven counter reset | `inv_progressing_migration_never_aborts` |
| M06 | 10158–10163 (ext-5, V8-C3) | revert the cross-run **replacement** of the target's reported position to a monotone max | `inv_no_acked_write_lost` |
| M07 | 10160–10163 (ext-5, V8-C3) | drop `Complete`'s attesting-run conjunct | `inv_no_acked_write_lost` |
| M08 | 10164–10174 (ext-5, V9-C2) | revert the **durable** guard on the target's ingest report | `inv_no_acked_write_lost` (with attesting-run conjunct intact) |
| M09 | 10175–10178 (ext-6, V4-M6) | full-sync payload to an attaching target replica does not carry the shadow | `inv_target_replicas_hold_committed_slot` |
| M10 | 10182–10187 (ext-7, V8-C1) | add a **second** member-keyspace deleter | `inv_node_keeps_slots_it_owns` |
| M11 | 10196–10199 (ext-8) | revert `discardShadow`'s residue guard | `inv_owner_serves_promoted_slot` |
| M12 | 10188–10195 (ext-8, V5-m7) *derived* | collapse the promotion split — serve at `relabelShadow` time | `inv_no_serve_before_attestation` |
| M13 | 10200–10204 (ext-9, V4-M1) *derived* | let a batch-level fault discard the shadow | `inv_open_migration_keeps_its_shadow` |
| M14 | 10208–10212 (ext-10, V7) | revert mark-don't-remove — membership prune **removes** the residue entry | `inv_begin_refuses_slot_with_residue` |
| M15 | 10213–10220 (ext-10, V8-C1) | revert `meetNode`'s non-empty-keyspace join gate | `inv_member_keyspace_is_tracked` |
| M16 | 10216–10220 (ext-10, V8-C1) | revert `resetCluster`'s non-empty-keyspace gate | `inv_member_keyspace_is_tracked` |
| M17 | 10223–10231 (ext-11, V8-M1) | revert the level rule — release once, let later arrivals hold | `inv_held_set_empty_while_latched` |
| M18 | 10231–10236 (ext-11, V4-M2) | revert the leader auto-`Complete` / widen the reset | `witnessDrainingWedgedWithCompletableToken` becomes **reachable** |
| M19 | 10250–10255 (ext-12, V5-C1) | revert the reaper's `promoted == true` gate | `inv_source_keeps_its_copy_until_promotion_attested` |
| M20 | 10251–10255 (ext-12, V5-C1) | revert `beginMigration`'s residue conjunct | `inv_source_keeps_its_copy_until_promotion_attested` |
| M21 | 10255–10261 (ext-12, V7-M6) | revert the target-departure unassign+mark rule | `inv_slot_copy_survives_until_owned_and_served` |
| M22 | 10277–10283 (ext-12, V8-C4) | remove `retargetSlotResidue` | `inv_residue_has_an_effective_remover` |
| M23 | 10281–10283 (ext-12, V8-C4) | revert the verb's own admission (`canRetargetSlotResidue`) | `inv_residue_has_an_effective_remover` |
| M24 | 10283–10288 (ext-12, V10-C4) | revert the **in-apply target re-home** on `demoteNode` | `inv_residue_has_an_effective_remover` |
| M25 | 10289–10295 (ext-12, V10-C3) | drop `Complete`'s target-role conjunct | `inv_slots_only_assigned_to_primaries` |
| M26 | 10296–10300 (ext-12, V8-C6) | revert the Demotion arm's same-apply slot re-home | `inv_slots_only_assigned_to_primaries` |
| M27 | 10299–10300 (ext-12, V8-C6) | revert `demoteNodeExternal`'s successor-less whole-refusal | `inv_slots_only_assigned_to_primaries` |
| M28 | 10301–10309 (ext-13, V5-M1) | restore v4's "(or `drained_pos` unset)" reset arm | `witnessDrainingWedgedBeforeConfirm` becomes **reachable** |
| M29 | 10310–10313 (ext-14, V5-M3) | revert the empty-counted-set-is-false rule | `inv_target_replicas_hold_committed_slot` |
| M30 | 10313–10320 (ext-14, V8-M2) | revert the replica-ack counter reset | `witnessKnobOnMigrationCompletes` unreachable at defaults |
| M31 | 10325–10332 (ext-15/17) | give an undeclared writer a parent-pointer write | `inv_role_written_only_by_declared_writers` |
| M32 | 10342–10347 (ext-15, V9-C1) | revert the Demotion arm's shard-relationship conjunct | `inv_slot_copy_survives_until_owned_and_served` / `inv_slots_only_assigned_to_primaries` |
| M33 | 10348–10353 (ext-15, V9-M4) | revert `clearSlotResidue`'s `promoted == true` conjunct | `inv_member_keyspace_is_tracked` |
| M34 | 10354–10357 (ext-15) | let a replayed/reordered boot report cancel migrations | `inv_no_spurious_cancel` |
| M35 | 10358–10363 (ext-15, V6-M2/M3) | revert the re-mint rule — `bootMintOf` mints `(0,0)` | `witnessNodeCanAlwaysReportIdentity` unreachable |
| M36 | 10363–10370 (ext-15, V8-C2) | revert the Demotion fence (`observed_role`/`observed_config_epoch`) | `inv_promotion_is_not_reverted_without_a_failover` |
| M37 | 10405–10409 (ext-15, V11-C2) | collapse stage and discard (discard at stage time) | `inv_no_acked_write_lost` / `inv_member_keyspace_is_tracked` |
| M38 | 10409–10412 (ext-15, V12-C1) | make `pendingTransition` volatile (cleared by `crashRestart`) | `inv_member_keyspace_is_tracked` |
| M39 | 10412–10414 (ext-15, V12-C1) | re-edge the adoption onto the observed admission apply | `inv_member_keyspace_is_tracked` |
| M40 | 10414–10419 (ext-15, V13-C1) = ext-16 (1) | weaken `completeAdoption` to the two-operand form | `inv_member_keyspace_is_tracked` / `inv_no_acked_write_lost` |
| M41 | 10419–10421 (ext-15, V13-M6) | drop the refusal partition (revert on a fence refusal) | `inv_no_acked_write_lost` |
| M42 | 10421–10424 (ext-15, V13-M3) | skip the boot re-derivation of the candidate | `inv_run_identity_never_regresses` |
| M43 | 10424–10427 (ext-15, V13-C1) | allow a record overwrite (second `stageReplicaof` toward another upstream) | `inv_member_keyspace_is_tracked` |
| M44 | 10427–10431 (ext-15, V12-M1) | revert the staged fence to *holding* | `inv_no_hold_during_staged_flip` — **N/A, Q2 flag 4** |
| M45 | 10459–10467 (ext-15, V12-C1) | stamp `kind` from bare plane disagreement | **N/A — superseded in ext-16**, no `Promotion` kind exists |
| M46 | 10468–10471 (ext-15, V12-M2) | remove `adoptReplicatedRole` | `witnessSplitPlanesConverge` unreachable |
| M47 | 10472–10474 (ext-15, V13-M1) | drop the lineage guard | `inv_no_acked_write_lost` |
| M48 | 10474–10479 (ext-15, V13-M1) | narrow the lineage guard (drop the removed-upstream disjunct) | `witnessSplitPlanesConverge` unreachable |
| M49 | 10486–10492 (ext-15, V15-C3) | drop the difference test's detached-matches-`None` equivalence | `witnessSplitPlanesConverge`'s silence falsified |
| M50 | 10505–10510 (ext-15, V11-C1) | drop the serving-primary guard from `abortHandoff` | `inv_no_execution_after_demotion` |
| M51 | 10505–10510 (ext-15, V11-C1) | drop the serving-primary guard from `cancelMigration` | `inv_no_execution_after_demotion` |
| M52 | 10577–10580 (ext-16 (2), V14-C2) | unspecific lineage disjunct (bare `u ∉ nodes`) | `inv_no_acked_write_lost` |
| M53 | 10581–10584 (ext-16 (3), V14-C3) | drop `synced(c) == true` from the failover candidate guard | `inv_no_acked_write_lost` |
| M54 | 10585–10590 (ext-16 (4), V14-M1) | re-mint `stageId` at boot | `witnessStagedFlipCompletesAcrossCrash` unreachable |
| M55 | 10591–10594 (ext-16 (5), V14-M7) | drop the stage-resolution precedence rule | `witnessStagedFlipCompletesAcrossCrash` unreachable |
| M56 | 10595–10600 (ext-16 (6), V14-M4) | `None`-writing demotion re-added | **N/A — deleted in ext-17** with its property |
| M57 | 10640–10645 (ext-17 (7), V15-C1) | sibling re-parent writes `primaryId` alone (companion stamps dropped) | `inv_no_acked_write_lost`; and on the **detach**, `inv_role_written_only_by_declared_writers` directly |
| M58 | 10646–10649 (ext-17 (8), V15-C4) | drop `attestReplicaSynced`'s observed-pair conjunct | `inv_no_acked_write_lost` |
| M59 | 10650–10654 (ext-17 (9), V15-M1) | stage-unbound refusal (drop `refusalPayload.stageId` conjunct) | `witnessStagedFlipCompletesAcrossCrash` unreachable |
| M60 | 10655–10659 (ext-17 (10), V15-C3) | **detach unmodelled** — remove the detach action | *negative control*: every property must still hold |
| M61 | 10660–10664 (ext-17 (11), V15-M4) | full-sync-only trigger for `attestReplicaSynced` | `witnessResyncedReplicaBecomesCandidate` unreachable |
| M62 | 10665–10676 (ext-17 (12), V16-C1) | parenting-token drop (`parent_seq` conjunct out of the apply guard) | `inv_no_acked_write_lost` |
| M63 | 10705–10718 (ext-17 (13), V18-C1) | registration-pairing drop (`observedRegistrationSeq` conjunct out) | `inv_no_acked_write_lost` |
| M64 | 10719–10733 (ext-17 (14), V18-C1) | same as (13), counter-loss schedule with a repeated `RunId` | `inv_no_acked_write_lost` |
| M65 | 10860–10867 (ext-18 (15), V19-M1) | delete `clearStaleRecord` | `inv_no_record_outlives_its_registration` (**not asserted**, Q2) + `witnessFenceClears` unreachable |
| M66 | 10868–10877 (ext-18 (16), V19-M1) | arm-4b deletion — `ordering`-against-absent-identity disposed as a no-op | `inv_every_terminal_refusal_has_a_disposition` |
| M67 | 10904–10911 (ext-18 (16'), V21-M2) | arm-5 variant — `membership` refusal disposed with **no client reply** | `inv_every_terminal_refusal_has_a_disposition` (`replyEmitted` conjunct) |
| M68 | 10912–10951 (ext-18 (17), V20-C1/V28-M6) | pairing drop — disposition guard weakened to `refusal.stageId == record.stageId` | `inv_no_live_record_disposed_by_a_foreign_refusal` |
| M69 | 10995–11003 (ext-18 (18), V21-M1) | counter-loss variant, intra-registration residue | **N/A — declared inexpressible** (structural limit) |
| M70 | 10113–10119 (ext-1/ext-10, V4 audit) *derived* | drop the reset-epoch key from `spent` (bare-seq key) | `inv_handoff_seq_never_reused` must **not** fire on a lawful post-reset re-mint |
| M71 | 10205–10210 (ext-10, N-M2/V4-M8) *derived* | drop `Begin`'s discard-then-ingest shadow re-mint | `inv_no_orphan_shadow_blocks_ingest` |
| M72 | 10148–10151 (ext-5, N-C2) *derived* | drop the per-position history tag / contiguity | `inv_stream_history_sound` |
| M73 | 10133–10140 (ext-4, V6-M4) *parameter check* | default sizing must make `witnessHealthyDrainAbortedByBound` unreachable | witness reachability |

---

## Results

Verdict taxonomy:

- **CAUGHT-P** — a named `inv_*` tripped in the invariant walk (`R`).
- **CAUGHT-T** — one or more deterministic run tests failed (`T`); run tests do **not**
  evaluate `inv_*`, so for anything the walk cannot reach they are the only oracle.
- **MISSED** — model still typechecks, all 51 tests pass, no invariant trips in 2500×40.
- **N/A** — not modelled in Q2, or superseded/declared inexpressible by the doc, or covered
  by a Q2 report flag.
- **CONTROL** — rows that assert a *non*-failure (M60, M73).

Every row ends with a verified restore: the next row's runner prints the pre-run diff stat,
and the final clean-tree re-run (`FINAL`) reproduces the Q2 baseline exactly — 51 passing,
no invariant violation, `git diff --stat -- specs/quint/` empty.

| # | Verdict | Evidence |
|---|---------|----------|
| M01 | MISSED | T 51 passing; R none |
| M02 | MISSED | T 51 passing; R none |
| M03 | MISSED | T 51 passing; R none |
| M04 | MISSED | T 51 passing; R none |
| M05 | CAUGHT-T | `drainingHeldForKTicksReachableTest`, `healthyDrainAbortedByBoundReachableTest` |
| M06 | MISSED | T 51 passing; R none |
| M07 | CAUGHT-T | `completeRefusesCrossRunTargetCopyTest` |
| M08 | MISSED | T 51 passing; R none |
| M09 | CAUGHT-T | `replicaAckKnobCompletesReachableTest` |
| M10 | CAUGHT-T | `promotionRollsBackToSourceTest` |
| M11 | CAUGHT-T | `discardShadowRefusedWithResidueTest` |
| M12 | CAUGHT-T | `happyMigrationTest`, `promotionRollsBackToSourceTest` |
| M13 | CAUGHT-P | R violation (walk) |
| M14 | CAUGHT-T | `failPromotionRefusedAfterSourceDepartedTest`, `reapDeferredWhileTargetGoneTest`, `orphanRehomeToSourceTest`, `orphanRehomeToAnotherPrimaryTest` |
| M15 | CAUGHT-P + T | R violation; `departedHolderWitnessesReachableTest` |
| M16 | CAUGHT-P + T | R violation; `resetRefusesNonEmptyKeyspaceTest`, `demotedHolderResetRefusedReachableTest` |
| M17 | CAUGHT-P + T | R violation; `feedHoldCapBreachReleasesFenceTest`, `selfFenceReleaseEmptiesHoldTest` |
| M18 | CAUGHT-T | `drainingHeldForKTicksReachableTest`, `healthyDrainAbortedByBoundReachableTest` |
| M19 | CAUGHT-T | `sourceKeepsCopyUntilAttestedTest` |
| M20 | CAUGHT-T | `residueBlocksNextMigrationTest`, `residueWitnessesReachableTest` |
| M21 | CAUGHT-P + T | R violation (a *different* invariant than the doc names — flag F13); `orphanRehomeToSourceTest`, `removeNodeForceEvictsLiveOwnerTest` |
| M22 | MISSED | T 51 passing; R none — flag F8 |
| M23 | MISSED | T 51 passing; R none — flag F8 |
| M24 | MISSED | T 51 passing; R none — flag F8 |
| M25 | MISSED | T 51 passing; R none |
| M26 | CAUGHT-P + T | R violation; `demotedHolderResetRefusedReachableTest` |
| M27 | CAUGHT-P | R violation |
| M28 | CAUGHT-T | `healthyDrainAbortedByBoundReachableTest` |
| M29 | MISSED | T 51 passing; R none |
| M30 | MISSED | T 51 passing; R none — flag F11 |
| M31 | CAUGHT-P | R violation |
| M32 | MISSED | T 51 passing; R none — structurally invisible (see analysis) |
| M33 | MISSED | T 51 passing; R none — flag F1 |
| M34 | MISSED | T 51 passing; R none — flag F2 |
| M35 | CAUGHT-T | `lostCounterStillReportsReachableTest` |
| M36 | CAUGHT-P | R violation |
| M37 | CAUGHT-P | R violation, but via a *different* invariant than the doc's named one — flag F1 |
| M38 | CAUGHT-T | `stagedFlipSurvivesCrashReachableTest` |
| M39 | CAUGHT-T | `stagedFlipSurvivesCrashReachableTest`, `fenceClearsWithinThreeStepsTest` |
| M40 | MISSED | T 51 passing; R none — flag F1 |
| M41 | CAUGHT-T | `terminalRefusalDisposesRecordTest` |
| M42 | CAUGHT-T | `sourceBootReportCancelsMigrationTest`, `stagedFlipSurvivesCrashReachableTest`, `fenceClearsWithinThreeStepsTest` — named detector is dead (flag F3) |
| M43 | MISSED | T 51 passing; R none — flag F1 |
| M44 | N/A | Q2 report flag 4 — `inv_no_hold_during_staged_flip` is false-of-model, comment-only at `cluster_migration_failover.qnt:888` |
| M45 | N/A | superseded in ext-16; no `Promotion` kind exists in the Q2 model |
| M46 | N/A | not modelled — `adoptReplicatedRole` / `witnessSplitPlanesConverge` absent (flag F4) |
| M47 | N/A | not modelled — no lineage guard (flag F4) |
| M48 | N/A | not modelled (flag F4) |
| M49 | N/A | not modelled (flag F4) |
| M50 | CAUGHT-P | R violation |
| M51 | CAUGHT-P | R violation |
| M52 | N/A | not modelled — lineage disjunct absent (flag F4) |
| M53 | CAUGHT-T | `forcedFailoverPromotesUnsyncedTest` |
| M54 | MISSED | T 51 passing; R none |
| M55 | MISSED | T 51 passing; R none (run `M55_M68`) |
| M56 | N/A | deleted in ext-17 together with its property |
| M57 | CAUGHT-P | R violation |
| M58 | MISSED | T 51 passing; R none — flag F5 |
| M59 | CAUGHT-T | `terminalRefusalDisposesRecordTest` — *not* the doc's named detector (flag F7) |
| M60 | CONTROL ✓ | detach removed from `step`: T 51 passing, R none — no property is falsified by the loss of reachability, as the doc requires |
| M61 | CAUGHT-T | `resyncedReplicaIsCandidateReachableTest` — exactly the doc's named witness |
| M62 | MISSED | T 51 passing; R none — flag F5 |
| M63 | MISSED | T 51 passing; R none — flag F5 |
| M64 | MISSED | same mutation as M63 (evidence shared); neither schedule is pinned by any test — flag F5 |
| M65 | CAUGHT-T | `recordOutlivesRegistrationTest`; `fenceClearsWithinThreeStepsTest` still **passes** → the doc's second half is wrong for Q2 (flag F6) |
| M66 | N/A | Q2 report flag 3 — `isRefusalTerminal` arm 4b is unreachable in the Q2 model |
| M67 | CAUGHT-P + T | R: `inv_every_terminal_refusal_has_a_disposition` violated (named, confirmed by single-invariant re-run); `terminalRefusalDisposesRecordTest` |
| M68 | MISSED | T 51 passing; R none (run `M55_M68`) |
| M69 | N/A | doc declares it inexpressible (structural limit) |
| M70 | CAUGHT-T | `resetMakesReMintDistinguishableTest` |
| M71 | CAUGHT-P | R: `inv_no_orphan_shadow_blocks_ingest` violated (named, confirmed by single-invariant re-run) |
| M72 | MISSED | restart-conditioned hole: T 51 passing, R none. Sub-row **M72b** (unconditional `pos+2`) → R: `inv_stream_history_sound` violated + `replicaAckKnobCompletesReachableTest` fails, so the invariant is live but the N-C2 hazard is unexercised — flag F10 |
| M73 | CONTROL ✓ | `PRECONFIRM_OBSERVATIONS` 4 → 30 (production default): `healthyDrainAbortedByBoundReachableTest` fails ⇒ the witness *is* unreachable at the default sizing, as the doc claims |

### Totals

| Verdict | Count |
|---------|-------|
| CAUGHT-P (invariant tripped in the walk) | 15 |
| CAUGHT-T (deterministic test only) | 21 |
| **CAUGHT total** | **36** |
| MISSED | 25 |
| N/A | 10 |
| CONTROL (both behaved as the doc requires) | 2 |
| **Total** | **73** |

---

## Finding 0 — walk power, and what it means for every "MISSED" below

At the brief's cap (2500 samples × 40 steps) the random walk **never reaches
`completeMigration`, any residue state, `witnessDrainingHeldForKTicks` or
`witnessHealthyDrainAbortedByBound`**. Every commit-family and residue-family invariant is
therefore *vacuous in the walk*: the 51 deterministic run tests are the real oracle for those
rows. This is not a budget complaint — enlarging the budget silently was explicitly out of
scope, and the structural point stands at any budget that keeps the walk unguided: the deep
migration prefix (begin → prepare → seal → confirm → coverage → complete) is not something a
uniform action choice assembles with useful probability.

Consequence for the design owner: **a doc mutation whose named detector is a commit-family
invariant is, in Q2, only as strong as the run test that constructs the trace.** Twelve of the
25 misses are exactly this shape.

---

## Per-miss analysis

Each miss is classified (a) *property gap* — the property is right, nothing forces the trace;
or (b) *wrong detector* — the property named by the doc cannot see the mutation even in
principle. (b) is a design-doc defect and is flagged.

### F1 — `inv_member_keyspace_is_tracked` is a gate-form invariant (wrong detector) — M33, M37, M38, M39, M40, M43

The invariant is `not(defects.untracked)`, and `defects.untracked` is recomputed **only inside
`meetNode` and `resetCluster`**. Every staged-flip / record / adoption mutation the doc routes
to this property is therefore undetectable by it: no admission step runs in the mutated trace.
M37 does trip the walk, but through a different invariant; M38/M39 are caught only by tests;
M33/M40/M43 are caught by nothing. **Wrong detector, six rows.** Either the doc should name the
per-step property it actually means, or Q2 should recompute the ghost at every keyspace write.

### F2 — `inv_no_spurious_cancel` is vacuous (wrong detector) — M34

`identityWritten` ≡ `admitted` in the Q2 report path, because admission already requires strict
identity domination, so `cancelled != Set() and ns.stored_identity == Some(identity)` is dead by
construction. Nothing can set the ghost. **Wrong detector.**

### F3 — `inv_run_identity_never_regresses` is vacuous (wrong detector) — M42

`refusalClassOf` returns `Whole` only when `identityOrderOk` already holds, so the
`admitted and not(identityOrderOk(...))` ghost is unreachable. M42 was caught by three tests, so
the mutation is not invisible — but the property the doc credits is dead. **Wrong detector.**

### F4 — the two-plane / lineage machinery is absent from Q2 — M46, M47, M48, M49, M52

`adoptReplicatedRole`, `reconcileIdentity`, `witnessSplitPlanesConverge` and the lineage guard do
not exist anywhere under `specs/quint/`. Five doc mutations have no model to mutate, so ext-15's
V12-M2 / V13-M1 and ext-16's V14-C2 claims are currently **unbacked by the model**. This is the
largest coverage hole in the rework, and it is a Q2 scope gap rather than a defect.

### F5 — the stale-attestation fence has no forcing trace — M58, M62, M63, M64 (property gap)

The four mutations that remove the attestation fence's operands (`parent`, `parent_seq`,
`registration_seq`, in the doc's combinations (8), (12), (13), (14)) all leave the model green.
The doc's own kills are *scheduled* traces (re-parent away-and-back; remove/meet;
remove/meet/report), and Q2 contains **no run test that builds any of them** — the walk does not
assemble them either. This is the single highest-value gap: V16-C1 and V18-C1 are the findings
ext-17 exists to answer, and the model as it stands would have gone green over both.

**Proposed strengthening (not implemented — needs new run tests, i.e. beyond the
invariant-only scope this phase was allowed):** three pinning tests, one per schedule, each
ending on `.expect(not(defects.ackLoss))` after promoting the attested node:

1. `staleAttestationRefusedAcrossReParentTest` — attach `n`, attest, `setRole` away and back,
   re-attest with the *old* `(parent_seq)` operand, fail the primary with `n` sole candidate.
2. `staleAttestationRefusedAcrossRejoinTest` — same, with `removeNode(n); meetNode(n)` between
   mint and apply (this is the schedule that returns `parent_seq` to `0`).
3. `staleAttestationRefusedWithRepeatedRunIdTest` — schedule 2 plus `reportRunIdentity(n)`
   carrying a `RunId` equal to a previously reported one.

Each must fail under the corresponding mutation and pass on the clean model.

### F6 — M65's second detector is wrong for Q2

Deleting `clearStaleRecord` does **not** make `witnessFenceClears` unreachable: `completeAdoption`
also drops `node_fenced`, so `fenceClearsWithinThreeStepsTest` still passes. The first half of the
doc's kill does land (`recordOutlivesRegistrationTest` fails). **Half-wrong detector.**

### F7 — M59's detector is misnamed

Stage-unbinding the refusal payload is caught immediately by
`terminalRefusalDisposesRecordTest`, not by `witnessStagedFlipCompletesAcrossCrash`, which stays
reachable. Caught either way; the doc's attribution is wrong.

### F8 — `inv_residue_has_an_effective_remover` is guard-level — M22, M23, M24

The property asserts that *some* remover is enabled, evaluated over guards. Deleting
`retargetSlotResidue` from `step` (M22) does not change any guard, and the run tests invoke
actions directly, so they are unaffected too. M23/M24 weaken admission paths the walk never
reaches (residue states are unreachable at this budget — Finding 0). **Property gap plus a
detector that cannot see an action-list deletion.**

### F9 — commit-family misses — M01, M02, M03, M06, M08, M25, M29, M32, M54, M55, M68

All eleven are Finding 0 in different clothes: `inv_no_acked_write_lost`,
`inv_complete_requires_*`, `inv_slots_only_assigned_to_primaries` and
`inv_no_live_record_disposed_by_a_foreign_refusal` are only ever evaluated in the walk, which
never commits a migration. Two deserve separate mention:

- **M32** is *structurally* invisible, not merely unexercised: `applyFailoverCommon`
  unconditionally unions `movedSlots` into the successor's `keys`, so a cross-shard successor and
  an in-shard one are indistinguishable in the post-state. No invariant over the Q2 state can see
  this mutation. **Wrong detector / model gap.**
- **M68** (the pairing drop, "the mutation this round exists for") is a plain property gap: the
  disposition tests use a matching pair, so weakening the guard from the `(stageId, regSeq)` pair
  to `stageId` alone changes no test outcome. **Proposed strengthening (not implemented, needs a
  test):** a `foreignRefusalDoesNotDisposeTest` that stages a record, re-registers to bump
  `staged_reg_seq`, then observes a terminal refusal carrying the *old* `regSeq` with the same
  `stageId`, expecting `record != None` and `not(defects.foreignDisposition)`.

### F10 — `inv_stream_history_sound` is live but its hazard is unexercised — M72

The as-specified mutation (hole only after a source restart) is missed; the unconditional variant
M72b trips the invariant in the walk. So the property has teeth, and what is missing is a trace
in which a source restarts *and then writes* inside one migration. **Property gap.** Note also
that Q2 deliberately narrows this invariant to position contiguity (declared in the Q2 report),
so the doc's "strengthened … via per-position history tags" is not what the model encodes.

### F11 — observation-counter resets are unobservable — M04, M30

`replicaAckKnobCompletesReachableTest` drives no `reconcileTick`, so neither `confirmDrained`'s
counter reset (M04) nor the replica-ack counter reset (M30) is load-bearing in any test, and the
walk never gets there. **Property gap**, fixable with one extra `reconcileTick` in that test plus
an `.expect` on `m.observations`.

### F12 — remaining single misses

- **M25** (`Complete`'s target-role conjunct): commit-family, Finding 0.
- **M29** (empty-counted-set-is-false): the knob test attaches a replica, so the empty case is
  never taken; a two-line variant of `replicaAckKnobCompletesReachableTest` with **no** attached
  replica would pin it. **Property gap.**
- **M54** (`stageId` re-mint at boot) and **M55** (stage-resolution precedence): both need a
  crash between stage and adoption *with a competing stage*; `stagedFlipSurvivesCrashReachableTest`
  crashes with a single stage outstanding, so precedence is never exercised. **Property gap.**

### F13 — M21's kill is real but differently attributed

The target-departure mutation trips the walk through an invariant other than the doc's named
`inv_slot_copy_survives_until_owned_and_served`, and two tests fail as well. Caught; attribution
noted for the design owner.

---

## Strengthenings implemented

**None.** Every gap identified above requires either a new deterministic run test or new model
state (an attested-triple field for F5, a per-write recomputation of `defects.untracked` for F1),
both of which are outside this phase's "small invariant-only addition" allowance. The proposals
are recorded above for phase Q4. The tree is therefore byte-identical to `c8ed899b` for
`specs/quint/`.

## Gates

| Gate | Result |
|------|--------|
| `git --no-optional-locks diff --stat -- specs/quint/` | empty |
| `git --no-optional-locks status --porcelain -- specs/quint/` | empty |
| Clean-tree re-run (`quint test`) | 51 passing, 0 failed — matches BASELINE |
| Clean-tree walk 2500×40, all 37 invariants | no violation — matches BASELINE |

No commit: the battery produced no model change, so this report is the artefact.

## Flags for the design owner (13)

| Flag | Kind | Rows |
|------|------|------|
| F1 | wrong detector — gate-form `inv_member_keyspace_is_tracked` | M33, M37–M40, M43 |
| F2 | wrong detector — `inv_no_spurious_cancel` vacuous | M34 |
| F3 | wrong detector — `inv_run_identity_never_regresses` vacuous | M42 |
| F4 | not modelled — two-plane / lineage machinery | M46–M49, M52 |
| F5 | property gap — stale-attestation fence unforced (highest value) | M58, M62–M64 |
| F6 | half-wrong detector — `witnessFenceClears` stays reachable | M65 |
| F7 | wrong detector attribution | M59 |
| F8 | guard-level detector cannot see an action-list deletion | M22–M24 |
| F9 | property gap — commit-family unreachable in the walk (M32 structurally invisible) | M01–M08, M25, M29, M32, M54, M55, M68 |
| F10 | property gap — restart-hole hazard unexercised | M72 |
| F11 | property gap — observation-counter resets unobservable | M04, M30 |
| F12 | property gaps — knob-empty case, stage precedence | M29, M54, M55 |
| F13 | attribution — kill via a different invariant | M21 |

