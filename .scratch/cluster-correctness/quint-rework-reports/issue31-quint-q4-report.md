# Q4 — closing the Q3 mutation-battery coverage gaps (issue-31 migration model)

Base: `c8ed899b` (37 invariants, 22 witnesses, 51 + 4 run tests).
Model files: `specs/quint/cluster_migration_failover{,_logic,_machine,_types}.qnt`.
Authority: `.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`.

## What was built (items A–H)

### A. F4 — ext-15/16 two-plane and lineage machinery (doc 10371–10490)

`NodeState` carries a node-local plane (`local_role`, `local_parent`) beside the replicated
cell. `writeParentPointer` follows the local plane only while the node is up
(`booted`), so a replicated write landing on a down node is the model's **only** producer of
a split. New pure defs `localUpstreamMatches` (detached-matches-`None` equivalence),
`identityFactsDiffer`, `lineageOk`, `canAdoptReplicatedRole`, `canReconcileIdentity`,
`reconcileKindOf`; new actions `adoptReplicatedRole` (local-plane-only writer) and
`reconcileIdentity` (level-triggered re-proposal), both wired into `step`; `restarted`
carries the local plane through a crash untouched (boot re-derives role from persisted
node-local state). New witness `witnessSplitPlanesConverge` (`coverage.planesConverged`)
with forcing tests `splitPlanesConvergeAfterCrashedPromotionTest`,
`foreignLineageAdoptionRefusedTest`, `detachedReplicaProposesNothingTest`.
Rows M46, M47, M48, M49, M52 → CAUGHT.

### B. F5 — the stale-attestation fence, one operand per trace (ext-17 (8)/(12)/(13)/(14))

Three traces built so that **exactly one** of the fence's three operands (observed primary,
parenting token `parent_seq`, `registration_seq`) refuses while the other two match at apply
time: `staleAttestationRefusedAcrossReParentTest`,
`staleAttestationRefusedAcrossReParentRoundTripTest`,
`staleAttestationRefusedAcrossRejoinTest`, plus the counter-loss/repeated-`RunId` schedule
`staleAttestationRefusedWithRepeatedRunIdTest`. Rows M58, M62, M63, M64 → CAUGHT, each
discriminating (dropping one operand fails exactly the trace built for it; siblings pass).

### C. F1 — `defects.untracked` recomputed at every keyspace-writing action

New logic: `trackedDirectly`, `keyIsTracked` (with the shard-primary indirection — a replica
of the shard that owns/sources/receives the slot is tracked through its primary),
`untrackedHoldersOf`, and `strandedBy`, whose pre-state reaper exclusion separates the lawful
reap-then-clear terminal state from V9-M4's short-circuit. The ghost is recomputed
**postcondition-derived** in `clearSlotResidue`, `reportPromoted`, `failPromotion`,
`retargetSlotResidue`, `rehomeOrphanSlot` and `completeMigration` (never guard-derived, so a
mutation of the admitting guard cannot make the antecedent a contradiction). Forcing tests
`unpromotedResidueNotClearableTest` (new, M33), `competingStageRefusedTest` (M43),
`adoptionRefusedWithoutItsOwnAdmissionTest` (M40). Rows M33, M40, M43 → CAUGHT; M38/M39 →
CAUGHT by a different detector (see flags); M37 stays MISSED (see analysis).

### D. F2/F3 — de-vacuified `inv_no_spurious_cancel` / `inv_run_identity_never_regresses`

`reportRunIdentity` now names its post-states (`postNodes`, `postMigs`, `postStored`,
`takenSlots`) and both ghosts compare **what the step wrote against what it overwrote**
(`identityRegress`), and **the records it took against whether it wrote at all**
(`spuriousCancel`). Q3's forms had antecedents that were contradictions of the admitting
guard. Rows M34, M42 → CAUGHT-P: both named detectors now fire under mutation, so the doc's
detector attribution for F2/F3 is **correct as written**.

### E. Finding 0 / F9 — a deterministic oracle for the commit and residue families

The random walk never reaches `completeMigration`, the residue states or the drain witnesses,
so run tests are the only oracle there. Added `completeRefusesCrossRunDrainTagTest` (M01),
`completeRefusesUnsealedSourceTest` (M02), `completeRefusesResealedRecordTest` (M03),
`completeRefusesDemotedTargetTest` (M25), `targetReportIsDrawnFromDurableTest` (M08).
M06 recorded as structurally invisible (analysis below); M32 carried as a design-owner flag —
the doc names no observable this model can distinguish, and none was invented.

### F. F8 — remover-effectiveness sensitive to action deletion

`demotedSourceResidueRetargetedTest` (level-triggered source arm, M23) and
`demotedTargetResidueRehomedInApplyTest` (in-apply target arm on `demoteNode`, M24) drive both
residue recovery arms to the end and assert `inv_residue_has_an_effective_remover` and the
post-state shape. M22's pure step-unwiring stays undetectable — structural limit, below.

### G. F10 — the restart-then-write window inside one record

New `restartedSourceCannotWriteWithinItsRecordTest` pins the closure: `sourceWrite` needs
`canAct` (booted); `sourceRestart` clears `booted`; the boot report that lifts the guard always
has `identityWritten == true` (the run id changed), so `cancelled = slotsSourcedBy(...)` takes
every record the node sources. A record therefore never sees two runs of its own source
writing into it, and per-record single-run history is **by construction**, not by an asserted
tag. Q2's declared narrowing stands: `inv_stream_history_sound` is position-contiguity only
(`p.pos >= 1` and a predecessor exists), deliberately not per-run — the per-run claim would be
false-by-construction and vacuous. The tag half is now pinned at the test level (the new test
asserts the exact `{run_id, pos}` pair, so blanking the tag fails it). Row M72 → CAUGHT
(both halves).

### H. Small test fixes

`replicaAckKnobCompletesReachableTest` amended with four pre-seal `reconcileTick`s (so the
pre-seal count is already at the bound), post-seal `observations == 0`,
`not(isAbortableByBound)`, `not(defects.sealCounter)`, one post-seal tick, and a non-empty
counted set at commit (M04, M30). `replicaAckKnobRefusesEmptyCountedSetTest` reworked so the
ack is **earned first** and the counted set emptied afterwards — with no ack ever recorded the
position conjunct refuses on its own and masked the rule under test (M29).
`competingStageRefusedTest` (M43), `foreignRefusalDoesNotDisposeTest` (M68, realized through
the `wipe`-driven stage-counter rewind so a new record collides on `stage_id` under a new
registration), and the `completeAdoption` admission-stamp conjunct
`ns.admitted_stage == Some(rec.stage_id)` (M54, M55 surface; doc ext-16 (4)/V14-M1).

## Re-verification battery

Every mutation applied as a single exact replacement, run, then restored byte-for-byte from a
backup; after each restore `git --no-optional-locks diff --stat -- specs/quint/` was confirmed
to be exactly the Q4 footprint. `git checkout -- specs/quint/` was never used.

| Row | Q3 | Q4 | Evidence |
|-----|----|----|----------|
| M01 | MISSED | CAUGHT-T | drop `d.run_id == m.source_log.run_id` → `completeRefusesCrossRunDrainTagTest` QNT511 |
| M02 | MISSED | CAUGHT-T | drop `m.fenced` → `completeRefusesUnsealedSourceTest` QNT511 |
| M03 | MISSED | CAUGHT-T | drop `m.phase == Draining` → `completeRefusesResealedRecordTest` QNT511 |
| M04 | MISSED | CAUGHT-T | drop `observations: 0, last_observation: None` from `applyConfirmDrained` → `replicaAckKnobCompletesReachableTest` QNT508 |
| M06 | MISSED | **MISSED** | cross-run replace → monotone max: 71/71 tests pass, 800×40 walk clean (structurally invisible, below) |
| M08 | MISSED | CAUGHT-T | report `target_applied` instead of `target_durable` → `targetReportIsDrawnFromDurableTest` QNT508 |
| M22 | MISSED | **MISSED** | unwire `retargetSlotResidue` from `step`: 71/71 pass, 1500×40 clean (structural limit, below) |
| M23 | MISSED | CAUGHT-T | `canRetargetSlotResidue` → `false` → `demotedSourceResidueRetargetedTest` QNT508 |
| M24 | MISSED | CAUGHT-T | drop the target arm of `retargetResidueOnDemotion` → `demotedTargetResidueRehomedInApplyTest` QNT508 |
| M25 | MISSED | CAUGHT-T | drop `isLivePrimary(allNodes, m.target)` → `completeRefusesDemotedTargetTest` QNT511 |
| M29 | MISSED | CAUGHT-T | drop `countedReplicasOf(...) != Set()` → `replicaAckKnobRefusesEmptyCountedSetTest` QNT508 |
| M30 | MISSED | CAUGHT-T | same edit as M04; the witness step is never reached → `replicaAckKnobCompletesReachableTest` QNT508 |
| M32 | MISSED | **MISSED** → **CAUGHT-P+T** † | drop `isInShardReplicaOf` from `canDemoteNode`: 2000×40 walk over both named invariants clean (design-owner flag) — **closed by the 2026-08-19 M32 ruling**: `inv_no_cross_shard_successor` violated at 500×20 and `demotionRefusesCrossShardSuccessorTest` fails |
| M33 | MISSED | CAUGHT-T | `canClearSlotResidue` → `Some(r) => true` → `unpromotedResidueNotClearableTest` QNT508 |
| M34 | MISSED | CAUGHT-P | cancel unbound from the field write → `inv_no_spurious_cancel` violated, 1500×40 |
| M37 | MISSED | **MISSED** → **CAUGHT-T / CAUGHT-P+T** † | the staged flip's dataset-discard leg is not modelled (below) — **closed by the 2026-08-19 M37 ruling**: hoisting the discard to stage time fails `adoptionDiscardsUnclaimedCopyTest`; discarding unexempted at adoption fails `assignSlotDuringStagedFlipKeepsCopyTest` **and** violates `inv_slot_copy_survives_until_owned_and_served` |
| M38 | MISSED | CAUGHT-T † | `restarted` clears `record` → `stagedFlipSurvivesCrashReachableTest` QNT508 (re-verified 2026-08-20 with the discard leg modelled; `inv_member_keyspace_is_tracked` still does not bite) |
| M39 | MISSED | CAUGHT-T † | adoption folded into the admission apply → `stagedFlipSurvivesCrashReachableTest` QNT508, plus `fenceClearsWithinThreeStepsTest`, `adoptionDiscardsUnclaimedCopyTest`, `assignSlotDuringStagedFlipKeepsCopyTest` (2026-08-20) |
| M40 | MISSED | CAUGHT-T | `completeAdoption` → bare pair (`record ∧ role == Replica`) → `adoptionRefusedWithoutItsOwnAdmissionTest` QNT511 |
| M42 | MISSED | CAUGHT-P | `Ordering` conjunct dropped from `refusalClassOf` → `inv_run_identity_never_regresses` violated, 1500×40 |
| M43 | MISSED | CAUGHT-T | drop `ns.record == None` from `stageFlip` → `competingStageRefusedTest` QNT511 |
| M46 | MISSED | CAUGHT-T | `canAdoptReplicatedRole` → `false` → `splitPlanesConvergeAfterCrashedPromotionTest` QNT508 |
| M47 | MISSED | CAUGHT-T | `lineageOk` → `true` → `foreignLineageAdoptionRefusedTest` QNT508 |
| M48 | MISSED | CAUGHT-T | drop the removed-upstream disjunct → `splitPlanesConvergeAfterCrashedPromotionTest` QNT508 |
| M49 | MISSED | CAUGHT-T | drop the detached-matches-`None` equivalence → `detachedReplicaProposesNothingTest` QNT508 |
| M52 | MISSED | CAUGHT-T | bare `u ∉ nodes` lineage disjunct → `foreignLineageAdoptionRefusedTest` QNT508 |
| M54 | MISSED | CAUGHT-T | `restarted` re-mints `stage_counter`/`record.stage_id` → `stagedFlipSurvivesCrashReachableTest` QNT508 |
| M55 | MISSED | **N/A** | structural: targets the `refused(ordering)` disposition, terminal only when `stored == None` (arm 4b), declared unreachable in Q2 (M66 N/A) |
| M58 | MISSED | CAUGHT-T | drop the observed-primary conjunct → `staleAttestationRefusedAcrossReParentTest` QNT511 (3 sibling traces pass) |
| M62 | MISSED | CAUGHT-T | drop `parent_seq == obsParentSeq` → `staleAttestationRefusedAcrossReParentRoundTripTest` QNT511 (3 pass) |
| M63 | MISSED | CAUGHT-T | drop `registration_seq == Some(obsRegSeq)` → `staleAttestationRefusedAcrossRejoinTest` QNT511 |
| M64 | MISSED | CAUGHT-T | same edit, counter-loss schedule → `staleAttestationRefusedWithRepeatedRunIdTest` QNT511 |
| M68 | MISSED | CAUGHT-T | `refusalBinds` weakened to the `stage_id` half → `foreignRefusalDoesNotDisposeTest` QNT508 |
| M72 | MISSED | CAUGHT-P+T | `pos + 2` → `inv_stream_history_sound` violated (800×40) **and** `restartedSourceCannotWriteWithinItsRecordTest` QNT508; blanked run tag → same test QNT508 |

**Totals: 34 rows re-verified — 29 CAUGHT (26 CAUGHT-T, 2 CAUGHT-P, 1 both), 4 still MISSED,
1 N/A.**

† Superseded by the **2026-08-20 re-run addendum** at the end of this report (the model
changed under these rows in `2eb66e35`). Totals after that re-run: **31 CAUGHT, 2 MISSED
(M06, M22), 1 N/A (M55)**.

## Rows still MISSED — per-row analysis (nothing weakened)

**M06 — cross-run replacement of the target's reported position, reverted to a monotone max.**
Structurally invisible *in isolation*. With M08's durable guard intact, `reportTargetIngest`
can only ever propose `m.target_durable`, and `target_durable` is monotone and always ≥ the
stored `target_copy.pos` for the current run; on a cross-run tag the stored position belongs to
a dead run and is ≤ the durable point that survived it. So `max2(stored, durable)` and
`durable` coincide in every reachable state. It becomes observable only as a **compound**
mutation with M08 (report the volatile applied position *and* max instead of replace), which is
outside the one-edit battery discipline. Not weakened, not papered over.

**M22 — remove `retargetSlotResidue`.** Two variants, and only one is expressible.
Deleting the *action* breaks compilation of `demotedSourceResidueRetargetedTest`, which calls
it directly — a real detection signal. Unwiring it from `step` alone is undetectable:
`inv_residue_has_an_effective_remover` is a **guard-level state predicate** (it asks whether
some verb is admissible here), so removing a verb's *schedulability* while leaving its guard
true cannot falsify it; run tests cannot steer `step`'s nondeterminism, and `quint run` has no
liveness or temporal checking at all. Detecting it needs a temporal operator this tool does not
have.

**M32 — the Demotion arm's shard-relationship conjunct.** *(Superseded 2026-08-20 — the
design owner ruled flag 4 in favour of a coverage ghost; `2eb66e35` added
`defects.crossShardSuccessor` and `inv_no_cross_shard_successor`, and the row is now
CAUGHT-P+T. The analysis below is why it was MISSED before that ruling.)* Dropping `isInShardReplicaOf` from
`canDemoteNode` (accepting a cross-shard successor) leaves **both** invariants the doc names
true, for structural reasons: the demoted source keeps its copy (demote-don't-remove), so
`inv_slot_copy_survives_until_owned_and_served` is satisfied by the *old* owner; and the
mis-homed successor is a live primary, so `inv_slots_only_assigned_to_primaries` holds.
Distinguishing the mis-home needs a claim the doc does not state — "the owner of a slot holds
its copy" — which is false in states the design declares lawful (the migration target owns
nothing until commit; the residue exists precisely because owner and holder diverge). No
semantics invented; carried as design-owner flag 4.

**M37 — collapse stage and discard (discard at stage time).** *(Superseded 2026-08-20 — flag 5
was ruled "model the discard leg"; `2eb66e35` added the `trackedDirectly` discard in
`completeAdoption` plus the `adoptionDiscardedKeys` coverage ghost, giving the mutation a
surface. The row is now CAUGHT-T (hoist variant) / CAUGHT-P+T (unexempted-discard variant) —
see the addendum. The analysis below is why it had no surface before that.)* The doc's
`completeAdoption`
effect is *discard + upstream adoption + record clear*; this model carries the adoption and the
record clear but **not** the dataset discard — no action writes `keys` on a staged flip. The
mutation therefore has no surface here. Fixing it means adding the discard leg to the model
(and its `assignSlots`-interleaved schedule), which is a modelling extension, not a test gap;
carried as design-owner flag 5.

## New counts

| | base `c8ed899b` | Q4 |
|---|---|---|
| `val inv_*` (auto-discovered) | 37 | 37 |
| `val witness*` | 22 | 23 |
| run tests, migration model | 51 | 71 |
| run tests, admission model | 4 | 4 |

20 new/reworked run tests; 1 new witness; 4 new pure defs for the tracking ghost, 6 for the
two-plane/lineage machinery; 2 new actions.

## Gate results

1. `just quint-check` — OK, 9 files type-check.
2. `quint test` — **71 passing, 0 failing** (migration model) + **4 passing** (admission model).
   No pre-existing expectation was changed except the two amended in item H, each justified above.
3. `just quint-run` — green; all 37 auto-discovered invariants pass on the sampled runs of both
   models. No new `inv_*` was added, so the discovered set is unchanged.
4. Random walk over the full 37-invariant conjunction, `--max-samples=4000 --max-steps=40` —
   **no violation** (also clean at 1500×40 and 2000×40 during the battery).
5. Witnesses — every reachable witness is asserted by its deterministic forcing test (the
   witness→test map in the model header; all 71 tests pass, `witnessSplitPlanesConverge`
   included). The three **declared-unreachable** witnesses
   (`witnessHealthyDrainAbortedByBound`, `witnessDrainingWedgedBeforeConfirm`,
   `witnessDrainingWedgedWithCompletableToken`) were pinned with a temporary conjunction
   invariant over a 4000×40 walk: **not reached**, as declared. The temporary invariant was
   reverted; the footprint check confirms it is not in the commit.

## Design-owner flags carried (no model change made)

**Disposition (2026-08-20).** All nine flags are now closed. Flags 4 and 5 were ruled in
favour of the model (ghost + discard leg, built in `2eb66e35`); flags 1, 2, 3, 6, 7 were
ruled in favour of the model's measured behaviour and the **design doc was corrected** in
this commit; flags 8 and 9 stand as honest-miss findings (M22, M06 remain MISSED). Per-flag
notes below.

1. **F6 / M65** — the doc's detector for deleting `clearStaleRecord` is half wrong:
   `inv_no_record_outlives_its_registration` is not asserted in this model (Q2), so only the
   `witnessFenceClears` half bites.
   → **CLOSED**: detector correction written into the design doc, item (15).
2. **F7 / M59** — stage-unbound refusal: the doc attributes the failure to
   `witnessStagedFlipCompletesAcrossCrash` becoming unreachable; the model's binding is in
   `refusalBinds`, and the observable is the disposition, not the witness.
   → **CLOSED**: detector correction written into the design doc, item (9)
   (`inv_no_live_record_disposed_by_a_foreign_refusal` / `foreignRefusalDoesNotDisposeTest`).
3. **F13 / M21** — target-departure unassign+mark: attribution to
   `inv_slot_copy_survives_until_owned_and_served` does not follow in this model's state shape
   (the departing target's copy is not the last one).
   → **CLOSED**: measured per-half attribution written into the design doc — the *unassign*
   half violates `inv_slot_owner_valid`, the *mark* half is caught only by run tests.
4. **M32** — no distinguishing observable exists for the cross-shard demotion; see above.
   → **CLOSED by ruling**: coverage ghost `defects.crossShardSuccessor` +
   `inv_no_cross_shard_successor` added in `2eb66e35`; row is CAUGHT-P+T.
5. **M37** — the staged flip's dataset-discard leg is unmodelled; see above.
   → **CLOSED by ruling**: discard leg modelled in `completeAdoption` (`trackedDirectly`) in
   `2eb66e35`; row is CAUGHT.
6. **M38 / M39 detector attribution** — both are caught, but by
   `stagedFlipSurvivesCrashReachableTest` (record survives the crash / adoption is not edged on
   the admission apply), **not** by `inv_member_keyspace_is_tracked` as the doc says. The
   doc's detector presumes the unmodelled discard leg of flag 5: with no discard, a lost record
   wedges the fence rather than stranding a keyspace copy.
   → **CLOSED**: re-measured *with* the discard leg present (2026-08-20). The reason survives
   — a lost record wedges the fence, so adoption never runs, so nothing is discarded and no
   copy is stranded; `inv_member_keyspace_is_tracked` stays green through 2000×40 on two seeds
   for each mutant. Corrected attribution written into the design doc.
7. **M34 mutation form** — binding the cancel to `admitted` instead of `identityWritten` is a
   **no-op**: admission requires strict domination of the stored pair, so `admitted` implies the
   proposed identity differs from the stored one, i.e. `admitted ⟺ identityWritten` for any
   admitted report. The doc's "replayed/reordered report cancels" only bites in the
   unconditional form (cancel on a *refused* report), which is what was run and caught.
   → **CLOSED**: mutation-form correction written into the design doc.
8. **M22** — undetectable step-unwiring; needs a temporal operator Quint's simulator lacks.
   → **CLOSED as honest miss**; still MISSED after the rework (detector unchanged).
9. **M06** — invisible except as a compound mutation with M08.
   → **CLOSED as honest miss**; still MISSED after the rework (detector unchanged).

**Correct as written** (raised in Q3 as suspect, confirmed accurate after the item-C/D fixes):
the doc's detectors for **F2 (M34 → `inv_no_spurious_cancel`)** and
**F3 (M42 → `inv_run_identity_never_regresses`)** both fire under mutation now that the ghosts
are postcondition-derived. F1's detector (M33 → `inv_member_keyspace_is_tracked`) is honest at
the ghost level, but the state is only reached deterministically — the random walk does not get
there, so `unpromotedResidueNotClearableTest` is the operative oracle.

---

## Addendum — 2026-08-20 re-run of the rows affected by `2eb66e35`

`2eb66e35` ("spec(quint): close mutation-battery coverage gaps for issue-31 rework") changed
the model underneath part of this battery, applying the four 2026-08-19 flag rulings:

- **M37 discard leg** — `completeAdoption` now discards the keys that are not
  `trackedDirectly(slots, migrations, residue, n, s)`, with a `coverage.adoptionDiscardedKeys`
  ghost.
- **hold × flip made effect-based** — `stageFlip` clears `held` for the flipping node and sets
  `coverage.stagedFlipAnsweredHolds`; `canLatchHold(ns) = ns.record == None` closes the
  latch/flip interleaving.
- **M32 ghost** — `defects.crossShardSuccessor` (postcondition-derived, set by `demoteNode`)
  plus `inv_no_cross_shard_successor`.
- **issue-33 tombstones** — `removeNode` writes the departure through `writeParentPointer`,
  clearing `member`/`registration_seq`/`epoch_own`.
- **anti-churn** — `churnFlag(n, failed)` / `churnEpoch` with `defects.churnEpochBump`.

Rows were selected by diffing every battery row's **mutation site** and **named detector**
against `2eb66e35`. Rows whose site text and detector are both untouched were not re-run; their
Q4 verdicts stand. Fourteen rows were affected (M17, M21, M31, M32, M37, M38, M39, M43, M44,
M62, M63, plus the new anti-churn row M74). Row ids follow the Q3 report's numbering; **M74 is
new here** — the anti-churn no-bump row has no Q3 id because the mechanism did not exist then.

### Re-run verdicts

| Row | Mutation (single exact replacement) | Q4 verdict | 2026-08-20 verdict | Evidence |
|---|---|---|---|---|
| M17 | drop the hold-region guard from the staged flip | CAUGHT-P+T | **CAUGHT-P+T** | control that `stageFlip`'s new `held' = held.set(n, Set())` does not mask the row: `inv_no_hold_during_staged_flip` violated at 2000×40 (2 seeds); forcing tests still fail |
| M21a | drop the **unassign** half of target-departure cleanup | CAUGHT-P+T | **CAUGHT-P+T** | `inv_slot_owner_valid` violated at 500×20; `orphanRehomeToSourceTest`, `removeNodeForceEvictsLiveOwnerTest` fail |
| M21b | drop the **mark** half of target-departure cleanup | CAUGHT-P+T | **CAUGHT-T** | no invariant trips (2000×40); 4 run tests fail (`reapDeferredWhileTargetGoneTest`, `failPromotionRefusedAfterSourceDepartedTest`, both orphan re-home tests) |
| M31 | re-armed at the **new** issue-33 tombstone site in `removeNode` | CAUGHT-P | **CAUGHT-P+T** | `inv_role_written_only_by_declared_writers` violated at 500×20; run tests also fail |
| M32 | drop `isInShardReplicaOf` from `canDemoteNode` | MISSED | **CAUGHT-P+T** | `inv_no_cross_shard_successor` violated at 500×20; `demotionRefusesCrossShardSuccessorTest` fails |
| M37a | hoist the discard into `stageFlip` | MISSED | **CAUGHT-T** | `adoptionDiscardsUnclaimedCopyTest` fails via `coverage.adoptionDiscardedKeys`; walk silent at 2000×40 |
| M37b | unexempted discard at adoption (`keys: Set()`) | MISSED | **CAUGHT-P+T** | `inv_slot_copy_survives_until_owned_and_served` violated at 500×20; `assignSlotDuringStagedFlipKeepsCopyTest` fails |
| M38 | make the staged-flip record volatile across crash | CAUGHT-T | **CAUGHT-T** | `stagedFlipSurvivesCrashReachableTest` fails; `inv_member_keyspace_is_tracked` green at 2000×40 (2 seeds) **even with the discard leg present** |
| M39 | edge adoption on the admission apply | CAUGHT-T | **CAUGHT-T** | 4 failing tests now (`stagedFlipSurvivesCrashReachableTest`, `fenceClearsWithinThreeStepsTest`, `adoptionDiscardsUnclaimedCopyTest`, `assignSlotDuringStagedFlipKeepsCopyTest`); walk silent |
| M43 | (site text rewritten by the rework; mutation re-expressed against the new text) | CAUGHT-T | **CAUGHT-T** | forcing tests unchanged |
| M44a | drop the hold-latch precondition `ns.record == None` | N/A | **CAUGHT-P+T** | was N/A because `canLatchHold` did not exist; `inv_no_hold_during_staged_flip` violated at 2000×40 (3 seeds) |
| M44b | latch a hold on a node with a live record | N/A | **CAUGHT-P+T** | `inv_held_set_empty_while_latched` violated at 500×20 |
| M62 | (tombstone-driven test amendment) | CAUGHT-T | **CAUGHT-T** | forcing tests fail |
| M63 | (tombstone-driven test amendment) | CAUGHT-T | **CAUGHT-T** | forcing tests fail |
| M74a | drop the epoch bump from `churnFlag` | *(new row)* | **CAUGHT-P+T** | `defects.churnEpochBump` observed; invariant violated at 500×20 and forcing test fails |
| M74b | bypass the ghost — write the churn flag without touching `churnEpoch` | *(new row)* | **CAUGHT-T** | run test only; the ghost is written on the same postcondition path the mutation removes, so the walk is blind to this form (see note below) |

**Verdict deltas.** M32 MISSED → CAUGHT-P+T; M37 MISSED → CAUGHT; M44 N/A → CAUGHT-P+T; M21b
CAUGHT-P+T → CAUGHT-T (a *measurement correction*, not a regression — Q3/Q4 recorded the
combined row's P from the unassign half); all other re-run rows unchanged. Battery totals after
the re-run: **31 CAUGHT, 2 MISSED (M06, M22), 1 N/A (M55)**.

**No masking.** M17, M43, M62, M63 were re-run specifically as controls that the new hold-clear
effect and the tombstone write do not mask an existing kill. None did.

**No counterexample-protocol trigger.** The unmutated model violated no detector at any budget
run here; `t2-blocked.md` was not needed.

### Two attribution findings recorded but *not* written into the design doc

Both are outside the ruled correction list for this task, so the doc was left alone:

1. **M37's doc-named detectors stay silent.** With the discard leg now modelled, the doc's own
   named detectors for M37 do not fire at 2000×40; the kills come from
   `adoptionDiscardsUnclaimedCopyTest` (variant a) and
   `inv_slot_copy_survives_until_owned_and_served` + `assignSlotDuringStagedFlipKeepsCopyTest`
   (variant b).
2. **M74b is ghost-blind by construction.** `defects.churnEpochBump` is computed on the same
   postcondition path the mutation deletes, so a mutant that bypasses the write is invisible to
   every invariant built on it. Deterministic tests catch it. A ghost derived from a *separate*
   observation of `ctl.epoch` would close the gap; that is a modelling decision for the design
   owner, not a test gap.

### Repro

```bash
eval "$(mise activate bash)"
quint test specs/quint/cluster_migration_failover.qnt
INVS=$(scripts/quint-invariants.sh specs/quint/cluster_migration_failover.qnt)
quint run specs/quint/cluster_migration_failover.qnt \
  --max-samples=500 --max-steps=20 --invariants $INVS
# escalation used before declaring any MISSED (2–3 seeds each):
quint run specs/quint/cluster_migration_failover.qnt \
  --max-samples=2000 --max-steps=40 --seed=<seed> --invariants $INVS
```

Runs are scoped to `cluster_migration_failover.qnt` only (not `just quint-run` /
`just quint-check`, which sweep the whole directory — other models were under concurrent
edit). Each mutation was a single exact temporary text replacement, applied and reverted
byte-for-byte from a scratch backup; `shasum` of all four migration `.qnt` files matches the
backups and `git diff --stat -- specs/quint/cluster_migration_failover*` is empty.

### Counts after `2eb66e35`

| | Q4 (`c8ed899b`) | 2026-08-20 (`2eb66e35`) |
|---|---|---|
| `val inv_*` (auto-discovered) | 37 | **40** |
| `val witness*` | 23 | **25** |
| run tests, migration model | 71 | **77** |

Final baseline, unmutated: `quint test` → **77 passing, 0 failing**; the 40-invariant walk at
500×20 → `[ok] No violation found`.

## Addendum — 2026-08-20 rows for the R1/R2 rulings

R1 (issue 41: demotion repoints dependants; `canRetargetSlotResidue` demands physical
holding) landed in `d1e15ab2` with two companion rules the counterexamples forced, and R2
(kind-scoped absent-operand ordering arm) landed in `91a3c6e0`. Eleven rows below: nine new,
plus the two M37 rows re-run against the fixed claim semantics as issue 41 asked. Row ids are
R1-*/R2-* because these mechanisms did not exist in the Q3 numbering.

Detectors per row: `quint test` (all run tests) and the 40-invariant walk at 500x20 on seeds
2 and 777, escalated to 2000x40 seed 3 whenever both were silent.

| Row | Mutation (single exact replacement) | Verdict | Evidence |
|---|---|---|---|
| R1-1a | drop the repoint from `setRole`'s demotion arm (`val base = nodes`) | **CAUGHT-T** | `retargetRefusedOntoNonHoldingPrimaryTest`, `residueFollowsDemotedSourceTest` fail; walk silent to 2000x40 |
| R1-1b | drop the repoint from the `reportRunIdentity` Demotion arm | **CAUGHT-T** | `adoptionRepointsDependantsTest` fails; walk silent to 2000x40 |
| R1-1c | drop the repoint from `attachTargetReplica` | **CAUGHT-T** | `attachTargetReplicaRepointsDependantsTest` fails; walk silent to 2000x40 |
| R1-2 | weaken `canRetargetSlotResidue` back to `p != r.source` (closure-edge holder allowed) | **CAUGHT-T** | `retargetRefusedOntoNonHoldingPrimaryTest` fails; walk silent to 2000x40 |
| R1-3 | delete the `isLivePrimary(nodes, rec.upstream)` conjunct from the staged-flip adoption guard | **CAUGHT-T** | `demotionRefusedOntoNonPrimaryUpstreamTest` fails; walk silent to 2000x40 |
| R1-4a | neuter `residueFollowsDemotion` (`if (true) res` — the claim never follows) | **CAUGHT-T** | `residueFollowsDemotedSourceTest` fails; walk silent to 2000x40 |
| R1-4b | drop both post-state conjuncts, moving the claim unconditionally (`if (r.source == n)`) | **CAUGHT-T** | `demotedSourceResidueRetargetedTest`, `retargetRefusedOntoNonHoldingPrimaryTest` fail; walk silent to 2000x40 |
| R2-1 | revert the absent-operand arm to `\| None => true` | **CAUGHT-T** | `demotionAgainstAbsentCellIsTerminalTest` fails; walk silent to 2000x40 |
| R2-2 | invert the kind scope (`\| None => kind != Boot`) | **CAUGHT-T** | same test fails on its `Boot` control; walk silent to 2000x40 |
| M37a | hoist the discard from `completeAdoption` into `stageFlip` (re-run) | **CAUGHT-T** (unchanged) | `adoptionDiscardsUnclaimedCopyTest` fails; walk silent — R1's claim-follows rule does not mask it |
| M37b | unexempted discard at adoption (`keys: Set()`) (re-run) | **CAUGHT-P+T** (unchanged) | invariant violated at 500x20 on both seeds; `assignSlotDuringStagedFlipKeepsCopyTest` fails |

**Two rows initially MISSED, and what was added.** R1-1b and R1-1c survived the first pass
with *no* failing test and a silent walk to 2000x40: the chained-replica topology those
deletions re-open is precisely what the flat walk does not sample, which is issue 41's own
finding, and no existing test looked at the dependants of a node demoted through the adoption
or attach path. `adoptionRepointsDependantsTest` and `attachTargetReplicaRepointsDependantsTest`
were added (`cc4a917b`) and both rows now die deterministically. No invariant was weakened.

**Walk blindness is the theme, not an accident.** Eight of the nine new rows are CAUGHT-T with
a silent walk. The states these mutations re-open (chained/cyclic replica topologies, a claim
resting on a closure edge) are the ones issue 41 showed the *unsteered* walk does not reach —
0/10 seeds at 2000x40 before steering, 8/8 under it. Until the steering question is settled
(see the issue-41 addendum in `t5-blocked.md`), deterministic tests are the honest detector
for this family, which is why every row above has one.

**No counterexample-protocol trigger from the battery itself.** The unmutated model violated
no detector at any budget run here.

### Repro and hygiene

Same commands as the previous addendum, scoped to `cluster_migration_failover.qnt`. Every
mutation was applied and reverted by an exact string replacement from a scratch backup;
`shasum` of the three migration `.qnt` files matches the backups after each row, and
`git status --short -- specs/quint/` is empty.

### Counts after `cc4a917b`

| | 2026-08-20 (`2eb66e35`) | 2026-08-20 (`cc4a917b`) |
|---|---|---|
| `val inv_*` (auto-discovered) | 40 | 40 |
| run tests, migration model | 77 | **84** |

Baseline, unmutated: `quint test` -> 84 passing, 0 failing; `just quint-check` green; the
40-invariant walk at 500x20 clean on seeds 2 and 777.
