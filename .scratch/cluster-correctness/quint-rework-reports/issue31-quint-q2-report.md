# Issue-31 Quint rework — phase Q2 report

**Status**: COMPLETE. Commit `c8ed899b8475d2f6311e84e7bb68ecc98a58713c` on `spec-gaps-impl`
(3 files, +1233 / -70, `specs/quint/` only). Not pushed.

Baseline at `4e1911a8`: 10 invariants, 8 witnesses, 32 migration tests, 4 admission tests.
Now: **37 invariants, 22 witnesses, 51 migration tests, 4 admission tests.**

---

## Per-extension table

Doc = `.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`, `## Quint rework`
(lines 10109–11359). "Reachability evidence" names the deterministic `run` test in
`specs/quint/cluster_migration_failover.qnt` unless stated otherwise.

| Ext | Doc property | Implemented name | Reachability evidence |
|-----|--------------|------------------|-----------------------|
| 1 | epoch never decreases across a step | `inv_epoch_never_decreases` | `ctl.prev_epoch` shadow exercised by every reset test; `resetClusterTest` (baseline) |
| 2 | commit requires a fenced source | `inv_complete_requires_fenced_source` | `completeRequiresFencedSourceTest` (baseline) + witness `witnessHandoffDrainedNotComplete` via `quietSlotWitnessesReachableTest` |
| 2 | commit requires the draining phase | `inv_complete_requires_draining_phase` | `quietSlotWitnessesReachableTest` (`witnessCompleteEnabledOnQuietSlot`) |
| 3 | target replicas hold the committed slot (ack knob) | `inv_target_replicas_hold_committed_slot` | `replicaAckKnobCompletesReachableTest` (`witnessKnobOnMigrationCompletes`) |
| 3 | slots are only ever assigned to primaries | `inv_slots_only_assigned_to_primaries` | forced by `defects.replicaSlot`; antecedent live in `replicaAckKnobCompletesReachableTest` |
| 4 | a progressing migration never aborts on the bound | `inv_progressing_migration_never_aborts` | `drainingHeldForKTicksReachableTest` (`witnessDrainingHeldForKTicks`) |
| 4 | the bound counts only post-seal observations | `inv_draining_bound_counts_post_seal_observations` **(coined)** | `healthyDrainAbortedByBoundReachableTest` (`witnessHealthyDrainAbortedByBound`) |
| 5 | held set is empty while latched | `inv_held_set_empty_while_latched` | `stagedFlipLeavesHoldRegionTest` pins the hold region; baseline barrier tests pin the latch |
| 5 | no execution after demotion | `inv_no_execution_after_demotion` | `demotedHolderResetRefusedReachableTest` (`witnessNonEmptyResetRefused`) |
| 6 | stream history is sound | `inv_stream_history_sound` | `quietSlotWitnessesReachableTest` asserts `stream_history.get(1) == Set()`; `witnessEmptyCoverageBatchIsANoOp` |
| 7 | no serve before attestation | `inv_no_serve_before_attestation` | `resyncedReplicaIsCandidateReachableTest` (`witnessResyncedReplicaBecomesCandidate`) |
| 8 | source keeps its copy until promotion is attested | `inv_source_keeps_its_copy_until_promotion_attested` | `residueWitnessesReachableTest` (`witnessResiduePending`) |
| 8 | a slot copy survives until owned and served | `inv_slot_copy_survives_until_owned_and_served` | `residueWitnessesReachableTest` |
| 8 | residue always has an effective remover | `inv_residue_has_an_effective_remover` **(renamed)** | `residueWitnessesReachableTest`; `canFailPromotion` arm covered by `retargetSlotResidue` |
| 9 | begin refuses a slot carrying residue | `inv_begin_refuses_slot_with_residue` | `residueWitnessesReachableTest` (`witnessBeginRefusedOverResidue`) |
| 10 | a node keeps the slots it owns (no over-delete) | `inv_node_keeps_slots_it_owns` | `departedHolderWitnessesReachableTest` (`witnessNodeRemoved`, keys survive removal) |
| 10 | the owner serves a promoted slot (no discard) | `inv_owner_serves_promoted_slot` | `gracefulPruneWitnessReachableTest` |
| 11 | member keyspace is tracked (reaper is sole deleter) | `inv_member_keyspace_is_tracked` | `departedHolderWitnessesReachableTest` + `demotedHolderResetRefusedReachableTest` |
| 12 | an orphan shadow never blocks ingest | `inv_no_orphan_shadow_blocks_ingest` | `failoverDuringOpenMigrationReachableTest` (`witnessFailoverDuringOpenMigration`) |
| 12 | an open migration keeps its shadow | `inv_open_migration_keeps_its_shadow` **(coined)** | `quietSlotWitnessesReachableTest` asserts the shadow survives `coverageBatch` |
| 13 | run identity never regresses | `inv_run_identity_never_regresses` | `lostCounterStillReportsReachableTest` (`witnessNodeCanAlwaysReportIdentity`) |
| 13 | registration generation is monotone | `inv_registration_gen_monotone` | `departedHolderWitnessesReachableTest` (`witnessNonEmptyJoinRefused`) |
| 14 | no spurious cancel | `inv_no_spurious_cancel` | `stagedFlipSurvivesCrashReachableTest` (`witnessStagedFlipCompletesAcrossCrash`) |
| 14 | promotion is not reverted without a failover | `inv_promotion_is_not_reverted_without_a_failover` | `forcedDemotionWitnessReachableTest` (`witnessForcedFailoverDemoted`) |
| 15 | every terminal refusal has a disposition | `inv_every_terminal_refusal_has_a_disposition` | `terminalRefusalDisposesRecordTest` |
| 15 | no live record disposed by a foreign refusal | `inv_no_live_record_disposed_by_a_foreign_refusal` | `terminalRefusalDisposesRecordTest` (antecedent `last_disposition != None` asserted live) |
| 16 | role written only by declared writers | `inv_role_written_only_by_declared_writers` | `forcedDemotionWitnessReachableTest`, `stagedFlipSurvivesCrashReachableTest` |
| 17 | fence clears within three steps | (witness only) `witnessFenceClears` | `fenceClearsWithinThreeStepsTest` |
| 17 | no replica without a primary pointer | **NOT ASSERTED** — withdrawn by ext-17 / V15-C3 | documented in the model |
| 18 | feed disconnect stops signaling | `witnessFeedDisconnected` + `inv_no_acked_write_lost` | `feedDisconnectedWitnessReachableTest` |
| — | abort repatriates | **NOT ASSERTED** — deleted with repatriation (MAJ-5) | documented in the model |
| — | no hold during a staged flip | **NOT ASSERTED** — false of this model | counter-state `stagedFlipLeavesHoldRegionTest` |
| — | no record outlives its registration | **NOT ASSERTED** — strict form unsatisfiable, guarded form tautological | counter-state `recordOutlivesRegistrationTest` |

Ownership/handoff (4), epoch (1 baseline), fencing/demotion (3) and `inv_complete_requires_drained`
/ `inv_no_acked_write_lost` are the pre-existing baseline invariants, carried forward unchanged.

Family counts (37 total, +27): ownership and handoff 4 (+0), epoch and reset 2 (+1),
fencing/barrier/demotion 3 (+0), commit admission 5 (+4), drain bounds 2 (+2), barrier and hold
2 (+2), stream and history 2 (+1), residue and reaper 10 (+10), identity and run 7 (+7).

---

## What of the dead agent's draft was kept / fixed / dropped

**Kept** (audited, correct as written): all 27 invariant bodies and their `defects`/`coverage`
ghost wiring; the 14 new witnesses; the `ctl.prev_parents` / `prev_keys` cross-step shadows; the
new pure helpers `parentsOf`, `keysOf`, `shardHoldsCopy`, `isUnclaimedSlot`, `ackedWritesCovered`,
`hasEffectiveRemover`, `replicaOwnsSlot`; the widened `defects` (20 fields) and `coverage`
(8 fields) records. Every ghost recomputes from its action's own postcondition — no guard-derived
ghosts found on audit.

**Fixed**:
1. **Zero run tests.** The draft added 27 invariants and 14 witnesses and *no* tests. Added 19
   deterministic `run` tests, pinning all 20 reachable witnesses. Three of these
   (`fenceClearsWithinThreeStepsTest`, `stagedFlipLeavesHoldRegionTest`,
   `recordOutlivesRegistrationTest`) were cited by the draft's own comments but never written.
2. **`witnessNodeCanAlwaysReportIdentity` parked as "declared unreachable".** It was unreachable
   only because `reportRunIdentity` minted inline and encoded the very mutation the doc forbids.
   Added `bootMintOf` to the logic module (a `Boot` mint below the replicated pair restarts the
   sequence above a strictly greater incarnation), wired `reportRunIdentity` to it, moved the
   witness out of the unreachable bucket, and added the anti-vacuity conjunct
   `ns.stored_identity != None`.
3. **`residueHasAnEffectiveRemover` unprefixed** → renamed `inv_residue_has_an_effective_remover`.
   `scripts/quint-invariants.sh` only greps `val inv_*`; unprefixed it would never have entered
   `just quint-run`.
4. **`inv_no_live_record_disposed_by_a_foreign_refusal` vacuous.** Its antecedent
   (`last_disposition != None`) was never reached. `terminalRefusalDisposesRecordTest` now drives
   a live `Membership`-class terminal refusal and asserts the antecedent holds.

**Dropped**: nothing from the draft was removed. The four doc properties left unasserted are
documented in the model with the reachable counter-state pinning each (table above); none of them
was in the draft as a live assertion.

**Rogue-agent claims confirmed FALSE against disk at `4e1911a8`**, as the brief stated: migrations
are PRUNED on source failover, not re-homed (`gracefulPruneWitnessReachableTest` asserts the
prune); the orphan re-home arm EXISTS; no `mint_epoch` field is needed. No off-doc instruction was
followed and no other agent was contacted.

---

## Coined / renamed names

| Kind | Name | Family |
|------|------|--------|
| Coined | `inv_draining_bound_counts_post_seal_observations` | drain bounds |
| Coined | `inv_open_migration_keeps_its_shadow` | residue and reaper |
| Renamed | `residueHasAnEffectiveRemover` → `inv_residue_has_an_effective_remover` | residue and reaper |

---

## Gate results — all green

| Gate | Result |
|------|--------|
| 1 `just quint-check` | exit 0, 9 files typechecked |
| 2 `quint test` | `cluster_migration_failover.qnt` **51 passing**; `cluster_admission.qnt` **4 passing** |
| 3 `just quint-run` | exit 0; **37** migration + **4** admission invariants auto-discovered, "No violation found" both models |
| 4 random walk (conjunction of all 37) | 4000×40 no violation, seed `0xd5ce4cd46f0429da`, trace length 41; 8000×60 no violation, seed `0x5fb39cc7f8cc5fd2`, trace length 61 |
| 5 witness reachability | 20/20 reachable witnesses pinned by deterministic runs; both declared-unreachable witnesses unreached at 6000×50 under `--invariant="not(<witness>)"` |

Expectation changes: **none**. All 32 pre-existing migration tests and all 4 admission tests pass
with their original assertions.

Helper scripts `.scratch/q2-walk.sh` and `.scratch/q2-unreach.sh` were used for gates 4/5 and left
uncommitted (path-scoped commit to `specs/quint/`). `website/src/data/frogctl-cli.json` remains
staged and untouched; `.claude/jobs/` untouched.

---

## Flags (4)

1. **`canFailPromotion` — verdict KEEP as written.** The doc does not spell out the
   departed-source rollback case, but prescribes `retargetSlotResidue` (V8-C4, doc:10277–10283)
   as the level-triggered arm for an entry whose `source` is no longer a primary — exactly the
   recovery the refusal defers to. The guard also prevents a real `inv_slot_owner_valid` violation
   that Q1F found by random walk. No change made.

2. **Epoch-keyed minting — verdict COMPLETE, no remainder.** `inv_handoff_owned`'s minting claim
   requires `t.epoch == ctl.reset_epoch`, so an old-epoch token cannot satisfy it; and
   `resetCluster` sets `migrations' = SLOTS.mapBy(_ => None)`, so no live record survives an epoch
   bump and the antecedent is vacuous immediately after a reset. Cross-epoch false witnesses in
   `inv_handoff_owned` are unreachable. Nothing left to close.

3. **`isRefusalTerminal`'s `Ordering ∧ stored == None` arm (design arm 4b) is unreachable in this
   model.** `identityOrderOk(None, …)` is vacuously true, so the class is never `Ordering` when
   `stored_identity == None`. Terminal refusals are therefore reachable only via the `Membership`
   class. Either the model's `identityOrderOk` is stricter than the design intends for a node with
   no stored identity, or arm 4b is dead in the design too. Not resolved in Q2 — a guard-semantics
   question for the design owner.

4. **`inv_no_hold_during_staged_flip` is false of this model** (counter-state:
   `stagedFlipLeavesHoldRegionTest` — `held.get(1) == Set(1)` coexists with a pending record).
   Making it true needs a new conjunct on `stageFlip` (or `prepareHandoff`), an action-semantics
   change phase Q2 does not own. Left unasserted and documented at the code.
