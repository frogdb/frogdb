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
| M32 | MISSED | **MISSED** | drop `isInShardReplicaOf` from `canDemoteNode`: 2000×40 walk over both named invariants clean (design-owner flag) |
| M33 | MISSED | CAUGHT-T | `canClearSlotResidue` → `Some(r) => true` → `unpromotedResidueNotClearableTest` QNT508 |
| M34 | MISSED | CAUGHT-P | cancel unbound from the field write → `inv_no_spurious_cancel` violated, 1500×40 |
| M37 | MISSED | **MISSED** | the staged flip's dataset-discard leg is not modelled (below) |
| M38 | MISSED | CAUGHT-T | `restarted` clears `record` → `stagedFlipSurvivesCrashReachableTest` QNT508 |
| M39 | MISSED | CAUGHT-T | adoption folded into the admission apply → `stagedFlipSurvivesCrashReachableTest` QNT508 |
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

**M32 — the Demotion arm's shard-relationship conjunct.** Dropping `isInShardReplicaOf` from
`canDemoteNode` (accepting a cross-shard successor) leaves **both** invariants the doc names
true, for structural reasons: the demoted source keeps its copy (demote-don't-remove), so
`inv_slot_copy_survives_until_owned_and_served` is satisfied by the *old* owner; and the
mis-homed successor is a live primary, so `inv_slots_only_assigned_to_primaries` holds.
Distinguishing the mis-home needs a claim the doc does not state — "the owner of a slot holds
its copy" — which is false in states the design declares lawful (the migration target owns
nothing until commit; the residue exists precisely because owner and holder diverge). No
semantics invented; carried as design-owner flag 4.

**M37 — collapse stage and discard (discard at stage time).** The doc's `completeAdoption`
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

1. **F6 / M65** — the doc's detector for deleting `clearStaleRecord` is half wrong:
   `inv_no_record_outlives_its_registration` is not asserted in this model (Q2), so only the
   `witnessFenceClears` half bites.
2. **F7 / M59** — stage-unbound refusal: the doc attributes the failure to
   `witnessStagedFlipCompletesAcrossCrash` becoming unreachable; the model's binding is in
   `refusalBinds`, and the observable is the disposition, not the witness.
3. **F13 / M21** — target-departure unassign+mark: attribution to
   `inv_slot_copy_survives_until_owned_and_served` does not follow in this model's state shape
   (the departing target's copy is not the last one).
4. **M32** — no distinguishing observable exists for the cross-shard demotion; see above.
5. **M37** — the staged flip's dataset-discard leg is unmodelled; see above.
6. **M38 / M39 detector attribution** — both are caught, but by
   `stagedFlipSurvivesCrashReachableTest` (record survives the crash / adoption is not edged on
   the admission apply), **not** by `inv_member_keyspace_is_tracked` as the doc says. The
   doc's detector presumes the unmodelled discard leg of flag 5: with no discard, a lost record
   wedges the fence rather than stranding a keyspace copy.
7. **M34 mutation form** — binding the cancel to `admitted` instead of `identityWritten` is a
   **no-op**: admission requires strict domination of the stored pair, so `admitted` implies the
   proposed identity differs from the stored one, i.e. `admitted ⟺ identityWritten` for any
   admitted report. The doc's "replayed/reordered report cancels" only bites in the
   unconditional form (cancel on a *refused* report), which is what was run and caught.
8. **M22** — undetectable step-unwiring; needs a temporal operator Quint's simulator lacks.
9. **M06** — invisible except as a compound mutation with M08.

**Correct as written** (raised in Q3 as suspect, confirmed accurate after the item-C/D fixes):
the doc's detectors for **F2 (M34 → `inv_no_spurious_cancel`)** and
**F3 (M42 → `inv_run_identity_never_regresses`)** both fire under mutation now that the ghosts
are postcondition-derived. F1's detector (M33 → `inv_member_keyspace_is_tracked`) is honest at
the ghost level, but the state is only reached deterministically — the random walk does not get
there, so `unpromotedResidueNotClearableTest` is the operative oracle.
