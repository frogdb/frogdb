# Issue-31 Quint rework — phase Q1 (state + actions) report

**Commits:** `5de6300e` (Q1 state + actions) and `b1a43cb6` (the six semantic rulings applied) on `spec-gaps-impl` — both pathspec-scoped to `specs/quint/`, `--no-verify`, not pushed. The rulings landed as a **follow-up commit rather than an amend**: another agent's commit `0eba0a4a` ("modularize and DRY cluster models (QR)") had already landed on top of `5de6300e`, so amending was no longer safe.
**Gate:** `just quint-check` PASSES (all 8 files: 4 admission + 4 migration). `just quint-run` PASSES (exit 0).
**Extra sanity (not required by the gate):** `quint test specs/quint/cluster_migration_failover.qnt` → **29/29 passing**; random-walk invariant runs over the conjunction of all 10 invariants at `--max-steps=60 --max-samples=8000` → no violation.
**Ambiguity flags:** 16 raised, **5 resolved by user ruling** (5, 9, 10, 14, 16), 11 still open.

Files (all four rewritten):

- `/Users/nathan/workspace/frogdb/.claude/worktrees/cluster-correctness-prd/specs/quint/cluster_migration_failover_types.qnt`
- `/Users/nathan/workspace/frogdb/.claude/worktrees/cluster-correctness-prd/specs/quint/cluster_migration_failover_logic.qnt`
- `/Users/nathan/workspace/frogdb/.claude/worktrees/cluster-correctness-prd/specs/quint/cluster_migration_failover_machine.qnt`
- `/Users/nathan/workspace/frogdb/.claude/worktrees/cluster-correctness-prd/specs/quint/cluster_migration_failover.qnt`

The four-file layering convention is preserved (types → logic → machine → main module importing the rest).

---

## Per-extension: what landed in Q1, what is deferred

### ext-1 — run-tagged positions
**Landed.** `type Pos = { run_id: RunId, pos: int }` (the field is `run_id`, not `run`: `run` is a Quint reserved word). `Migration.source_log`, `drained_pos: Option[Pos]` and `target_copy` are all run-tagged; `stream_history: SlotId -> Set[Pos]` records every position the slot's log ever produced with the run that minted it. `sourceWrite` advances the tagged high-water; `confirmDrained` draws its position from the same space using the source's *current* run.
**Deferred:** the forked-history invariant stated over `stream_history` (Q2). The variable exists and is populated.

### ext-2 — source fence separate from phase
**Landed.** `Migration.fenced` is tracked independently of `Migration.phase`. `prepareHandoff` seals; `localReleaseOnCapBreach` and `selfFenceRelease` drop the seal *locally* without touching the phase, and both empty the source's whole hold region (`held.get(source)`), not just the buffered bytes.
**Deferred:** `inv_held_set_empty_while_latched` (Q2).

### ext-3 — attempt stamping
**Landed.** `Migration.attempt` (0 = no prepared handoff). `prepareHandoff` mints from `ctl.handoff_seq`; `confirmDrained`, `completeMigration`, `abortHandoff` and `cancelMigration` all take an explicit `att: int` parameter so a **stale** stamp is proposable and refused (`prepareSupersedesLiveHandoffTest` proves it). `step` offers `att ∈ {handoff_seq, handoff_seq - 1, 0}`.

### ext-4 — observation counter + confirm-time reset
**Landed.** `observations`, `last_observation`, `preconfirm_bound` (4), `draining_bound` (3) on the record; `observedProgress`/`applyReconcileTick`/`abortableByBound` in the logic module implement the **narrowed** reset (only actual ingest progress since the last observation clears the counter). `confirmDrained` resets the counter so the post-seal bound is not consumed pre-seal (V7-C3). `boundAbort` is the bounded exit.

### ext-5 — durability tagging / target ingest
**Landed.** `target_applied` (volatile) and `target_durable` on the record; `reportTargetIngest` may only propose a *durable* position, and a cross-run report **replaces** (may regress) rather than maxing. `targetRestart` rolls the volatile point back to durable and re-mints the target's run. `canComplete` requires `m.target_copy.run_id == nodes.get(m.target).run_id` — the conjunct `completeRefusesCrossRunTargetCopyTest` exercises.

### ext-6 / ext-14 — target replicas
**Landed.** `NodeState.replica_copy`, `counted`, `install_incomplete`; actions `attachTargetReplica`, `detachTargetReplica`, `feedTargetReplica`, `reportReplicaAck`, `crashDuringShadowSection` (an install without its completion marker is discarded at boot by `restarted`).
**Amended by ruling Q5:** the requirement is a **captured parameter** `Migration.require_replica_ack`, stamped into the record by `newMigration` at Begin from the proposer's config (`beginMigration(s, source, target, requireReplicaAck)`, nondet in `step`) and never re-read. The global `ctl.count_replicas` field and the `setCountedReplicasKnob` action are **deleted**; `canCompleteMigration` lost its `countReplicas` parameter. See resolved flag 5.

### ext-7 — per-node keyspace + the reaper
**Landed.** `NodeState.keys: Set[SlotId]`. `reapSlots(n)` is the **only** deleter of a member's keyspace entry (V8-C1), gated on `residue.promoted` and deferred while `target_gone`. `assignSlot` also gives the node the copy (no separate "load the slot" step exists, and without it no residue entry would ever be reapable).

### ext-8 — first-class shadow + split promotion
**Landed.** `var shadow: SlotId -> Option[ShadowTag]` where `ShadowTag = { mig, epoch }` (epoch = the **reset** epoch it was minted under). Promotion is split: `relabelShadow(s)` is the local flip (clears the shadow only), `reportPromoted(s)` is the replicated attestation (sets `promoted` **and** adds the slot to the target's `keys`). `discardShadow` is guarded on no live record ∧ no residue entry. `failPromotion(s)` rolls ownership back to `r.source`, clears the shadow, and leaves the entry at `promoted == false`.

### ext-9 — coverage batch
**Landed.** `coverageBatch(s)` admits an **empty** payload as a no-op advance (`coverage.emptyBatch` ghost); `batchFault(s)` rolls the volatile applied point back to durable. Neither discards the shadow — that is the property.

### ext-10 — reset epoch / spent re-keying / refusal gates
**Landed.** `spent: Set[{epoch: int, seq: int}]`; `prepareHandoff` spends `{epoch: ctl.reset_epoch, seq: newSeq}` and recomputes `defects.seqReuse` against it. `resetCluster(n)` rewinds `ctl.handoff_seq` to 0 and bumps `ctl.reset_epoch`; both `meetNode` and `resetCluster` **refuse a non-empty keyspace** (V8-C1). `beginMigration` performs the Begin-time **discard-then-ingest** of any shadow the slot still carried. `resetMakesReMintDistinguishableTest` proves the re-mint-vs-reuse distinction.

### ext-11 / ext-13 — escape flags
**Landed.** `cannot_apply` (with `selfFenceRelease`), `cannot_drain` (blocks `confirmDrained`), `silent` (blocks the target's actions), plus the `setSourceCannotApply` / `setSourceCannotDrain` / `setTargetSilent` knob actions.

### ext-12 — residue
**Landed.** `var residue: SlotId -> Option[Residue]` with `Residue = { mig, source, target, promoted, source_gone, target_gone, promotion_failures }` (the counter added by ruling Q6; `failPromotion` increments it, `completeMigration` initializes it to 0). `completeMigration` is the sole writer and initializes **every** field. `removeNode(n)` **marks** (`source_gone` / `target_gone`) and never removes. `failPromotion` mutates it in place (entry stays, `promoted == false`). `retargetSlotResidue` is the **source arm only** (V10-C4), level-triggered on `shardPrimary`. `clearSlotResidue` requires `promoted`. Residue also blocks `beginMigration`, `assignSlot`, `removeSlot` and `discardShadow` on the slot.

### ext-15 / ext-16 / ext-17 — role, parent pointer, run identity, declared writers
**Landed.** `role` and `parent` (`primary_id`) are modelled variables written **only** through `writeParentPointer`, which always bumps `parent_seq` and rewrites `synced`/`admitted_stage`/`promoted_from` in the same step — so ext-17's declared-writer property holds *by construction* rather than by comment. `reportRunIdentity(n, kind, obsRole, obsEpoch)` is kind-stamped (`Boot | Demotion`), the Demotion arm is the one that writes role/parent (V7-C1), fenced on observed role/epoch (V8-C2), with the boot-ordering guard (`booted`) blocking every other proposal from that node. Refusals are classified (`Ordering | Fenced | Membership | Whole`) and `refusalIsTerminal` marks the terminal ones. `loseIncarnationCounter` produces the store-loss trace. `attestReplicaSynced` is fenced on the observed `(primary, parent_seq, registration_seq)` triple. `beltOk(..., requireSynced)` is the ext-16 candidacy gate — `failoverGraceful`/`failoverAuto`/`sourceFailover` require synced, `failoverForced` does not.
**Per the task's note, the struck-through Promotion arm is NOT modelled** (V14): there is no REPLICAOF NO ONE action; promotion rides the failover transitions.

### ext-18 — pending-transition record
**Landed.** `NodeState.record: Option[TransitionRecord]` (`kind`, `upstream`, `stage_id`, `staged_reg_seq`, `adopted`), `stage_counter`, `node_fenced`, `reply_emitted`, `last_disposition`, plus `var refusals: Set[Refusal]`. Actions: `stageFlip`, `observeRefusal`, `clearStaleRecord`, `completeAdoption`. The record survives a restart **field-for-field** (V14-M1) while `wipe` on `removeNode` drops the stage counter — which is what makes the cross-registration stage-id collision expressible.
**Deferred:** `inv_no_live_record_disposed_by_a_foreign_refusal` and the rest of the ext-18 invariant family (Q2). `last_disposition` is written by `observeRefusal` and read by nothing yet — that is intentional.

### Actions the section names — all present
`sourceFailover`, `sourceRestart`, `targetRestart`, `reapSlots`, `beginMigration` (residue guard + discard-then-ingest), `relabelShadow`, `reportPromoted`, `discardShadow`, `completeMigration` (full residue initializer + target-is-member-primary conjunct), `failPromotion`, `removeNode` (mark-don't-remove), `demoteNode`, `demoteNodeExternal` (whole refusal while it still owns slots), `retargetSlotResidue` (source arm), `clearSlotResidue` (promoted conjunct), `meetNode`/`resetCluster` (non-empty-keyspace refusals), `reportRunIdentity` (kind-stamped, Demotion-arm writes, boot-ordering guard), `loseIncarnationCounter`, `attachTargetReplica`/`detachTargetReplica`/`crashDuringShadowSection`, `coverageBatch`, `setSourceCannotApply`/`setSourceCannotDrain`/`setTargetSilent`, `reconcileTick` (narrowed reset), `resetCluster` with Begin-time discard-then-ingest at `beginMigration`. ~57 actions total; `step` wires all of them with nondeterministic `n1/n2/s/r/flag/kind/role/obsEpoch/obsRole/obsSeq/obsReg/att`.

---

## Invariants: dropped / ported / deleted

### Deleted (the redesign drops the claim)
| Invariant | Why |
|---|---|
| `inv_abort_repatriates` | Repatriation is superseded. Ownership does not move before commit, so there is no residue to stream back and no "resolved" state to assert. |
| `inv_repatriating_well_formed` | The `repatriating` phase no longer exists. |
| `inv_feed_hold_bounded` (`feed_bytes <= HOLD_BYTE_CAP`) | Under ext-2/ext-11 the cap is a **local release trigger**, not an atomic arrival-time bound: `feedBuffer` accepts and `localReleaseOnCapBreach` is a separately scheduled action, so a state above the cap awaiting release is lawful and reachable. Replacement lives in Q2 (`inv_held_set_empty_while_latched`) — a weakened version was deliberately not left in place. |

### Deleted witnesses
`witnessRepatriationPending` → replaced by `witnessResiduePending` (a residue entry exists with `promoted == false` — the post-commit state the redesign puts in repatriation's place).

### Ported (kept, adjusted to the new state)
| Invariant | Adjustment |
|---|---|
| `inv_slot_owner_valid` | unchanged |
| `inv_migration_endpoints_valid` | unchanged shape |
| `inv_handoff_owned` | **restated** — see flag 14. Now: every live `m.attempt >= 1` appears in `spent`, and `m.fenced implies m.attempt >= 1`. |
| `inv_handoff_seq_never_reused` | `not(defects.seqReuse)`; the ghost is now recomputed against the **epoch-keyed** spent set |
| `inv_epoch_monotone` | reads `ctl.epoch` and `NodeState.epoch_own` (the `node_epoch` parallel map is gone) |
| `inv_epoch_never_decreases` | reads `ctl.epoch`/`ctl.prev_epoch`; `keepCtl` carries `prev_epoch` forward on **every** action |
| `inv_last_failover_demoted` | ghost now recomputed via a shared `demotedTo(...)` postcondition predicate in **every** failover-shaped action (`failoverGraceful`, `failoverAuto`, `failoverForced`, `sourceFailover`, `demoteNode`) |
| `inv_last_failover_fenced` | ghost recomputed in all three failover paths — **including the forced one** (see flag 8) |
| `inv_graceful_failover_barriered` | ghost strengthened to the **subset** form (`movedSlotsOf(slots, old) ⊆ barriers.failover_fence`); the guard was strengthened to match after the random run found the mismatch (see flag 15) |
| `inv_complete_requires_drained` | ghost recomputed inside `completeMigration` from the record's own drained evidence, independent of `canComplete` |

### Ported witnesses
`witnessMigrationInFlight`, `witnessHandoffDrainedNotComplete` (now `phase == Draining`), `witnessGracefulFailoverPrunedMigration`, `witnessForcedFailoverDemoted` (now specifically "the promoted candidate was not synced" — the gate `force` actually drops), `witnessFeedDisconnected` (`barriers.disconnected`), `witnessNodeRemoved`, `witnessFailoverDuringOpenMigration`, `witnessResiduePending` (new name, replaces the repatriation one).

### NOT written (phase Q2, as instructed)
`inv_no_serve_before_attestation`, `inv_member_keyspace_is_tracked`, `inv_no_orphan_shadow_blocks_ingest`, `inv_held_set_empty_while_latched`, `inv_no_live_record_disposed_by_a_foreign_refusal`, `residueHasAnEffectiveRemover`, the forked-history property over `stream_history`, `inv_complete_requires_fenced_source`, and the bounded witnesses. **Every field they read already exists in the state.** No mutation tests (Q3).

### Tests
29 `run` tests, all passing. Added by the rulings: `completeRefusesAfterSourceRestartTest` (the seal breaks and the stale commit is refused), `sourceRestartHandoffCanBeReEarnedTest` (breaking the seal must not *wedge* the migration — a fresh Prepare/Confirm under the new run re-earns the commit), `sourceFailoverRehomesMigrationTest` (the re-home). `resetMakesReMintDistinguishableTest` was rewritten around TR-CLUSTER-035's corrected direction: the cluster has to be rebuilt (meet, boot, assign) before a second handoff can be minted at all. Deleted: `abortRepatriatesTest`, `cancelBeforeRepatriationTest`, `cancelRefusedWhileRepatriatingTest`, `cancelWithoutResidueAppliesImmediatelyTest`, `feedHoldOverflowDisconnectsTest`. Added, among others: `sourceKeepsCopyUntilAttestedTest` (the reaper is refused before attestation), `promotionRollsBackToSourceTest` (the rollback-wins-the-race trace between the split promotion halves), `reapDeferredWhileTargetGoneTest` (mark-don't-remove + the `target_gone` defer), `residueBlocksNextMigrationTest`, `discardShadowRefusedWithResidueTest`, `completeRefusesCrossRunTargetCopyTest`, `forcedFailoverPromotesUnsyncedTest`/`forcedFailoverAcceptsUnsyncedTest` (what `force` actually drops), `feedHoldCapBreachReleasesFenceTest`, `selfFenceReleaseEmptiesHoldTest`, `resetRefusesNonEmptyKeyspaceTest`, `resetMakesReMintDistinguishableTest`.

---

## Ambiguity flags (16)

1. **`residue` is keyed per-slot with `mig` inside the entry**, not by the `(SlotId, MigrationId)` pair the design text implies. At most one residue entry per slot is live at a time in this model (a slot with an entry refuses `beginMigration`), so the pair key is redundant — but if the design intends *concurrent* entries per slot, this is wrong and Q2 must re-key.
2. **`shadow`'s value is `{mig, epoch}`**, not a bare `MigrationId`. The reset epoch is carried so an orphan shadow from a pre-reset epoch is *distinguishable*, which ext-10's orphan property needs. The design text does not spell the tag's shape.
3. **Two distinct `fenced` notions.** ext-2's per-migration source seal (`Migration.fenced`) and ext-18's whole-node `-TRYAGAIN` fence (`NodeState.node_fenced`) are separate fields. The design uses "fenced" for both.
4. **`provisional_target` deleted rather than ported.** It was a ghost whose only job was making repatriation falsifiable; `shadow` is its first-class replacement. Nothing in the design asks for it to survive.
5. ~~**The counted-replica knob (`ctl.count_replicas`) is initialised OFF.**~~ **RESOLVED (ruling Q5).** The premise was wrong: it is not a knob. The design makes `require_target_replica_ack` a **captured parameter** — read from the *proposer's* config once, at proposal time, written into the `BeginSlotMigration` payload, and immutable thereafter; "every replicated predicate that consults a bound reads the record field, never the config". A config read at admission time is a real defect class: one applier admits what another refuses, which is permanent Raft divergence, and a knob flipped mid-migration silently voids the durability conjunct the operator believed was armed. The model now carries `Migration.require_replica_ack`; the mutable global and its setter are gone. (V8-m4's "reachable at stock defaults" refers to `draining_observations = 3`, not to this parameter defaulting on — so the default-off question dissolves.)
6. **The split-plane local-vs-replicated role reconciliation** (an `adoptReplicatedRole`/`reconcileIdentity` pair) is **not** modelled — it is not in the task's enumerated action list. The Demotion arm of `reportRunIdentity` is the only role-adopting path.
7. **`sourceRestart`/`targetRestart` double as ext-16's `crashRestart`.** They re-mint the run, clear the escape flags and drop held writes, while `record`/`keys`/`stage_counter`/`stored_identity` survive field-for-field.
8. **`failoverForced` keeps the per-object fence.** The design's "forced" language could be read as bypassing it. The reading taken: `force` drops ext-16's *sync* gate on the candidate, not the ownership fence (the pre-rework model had the same shape, and `failoverRefusesStaleFenceTest` depends on it). If the intent is fence bypass, `inv_last_failover_fenced` must become path-scoped.
9. **RESOLVED (ruling Q1 = option b): the migration survives, re-homed to the successor.** `rehomedMigrations(migs, old, succ, succRun)` re-homes every record the old primary sourced onto the successor — `source := succ`, a fresh run tag, `attempt: 0`, `fenced: false`, `phase: Streaming`, `drained_pos: None`, and all target-side evidence zeroed — while the target leg (and the degenerate case where the target *is* the successor) is still pruned by `applyFailoverCommon`. Original text: **`sourceFailover` prunes the migration** (it routes through `applyFailoverCommon`, which prunes every migration naming the old primary, per issue 15's ruling). That makes the "new source has a different run, so a pre-failover `Confirm` is refusable" scenario **unreachable via that action**. The cross-run refusal is instead exercised at the target (`targetRestart` + `completeRefusesCrossRunTargetCopyTest`). If ext-5 intends the source-side migration to *survive* a source failover, `sourceFailover` must stop pruning — a real semantic question, flagged rather than decided.
10. **RESOLVED (ruling Q2 = close the gap, both halves).** `canCompleteMigration` now requires `d.run_id == allNodes.get(m.source).run_id` — the drain's evidence must have been produced by the run the source is *still* on — and `sourceRestart` additionally **breaks the seal** (`attempt := 0`, `fenced := false`, `phase := Streaming`, `drained_pos := None`, barrier disarmed, feed reset), because the hold those fields described died with the process. The conjunct is stated against the *drained position* rather than the record's `source_log` tag: keying it on `source_log` would have wedged the migration permanently after a restart, since nothing re-tags `source_log` until the next `sourceWrite`. `sourceRestartHandoffCanBeReEarnedTest` pins the non-wedge half.
11. **`registration_seq` is `Option[int]`** (None ≡ deleted cell) rather than a partial map.
12. **`reset_epoch` / `handoff_seq` rewind is modelled globally** even though `resetCluster` applies to one node. A per-node counter would make the "re-mint vs reuse" question node-local; the design's INV-HANDOFF framing is cluster-wide, so the global reading was taken.
13. **Leader auto-`Complete` is modelled as `completeMigration` itself.** The model has no proposer identity, so "the leader proposes it" is unstated; `reconcileTick` only advances the observation counter and `boundAbort` is the bounded exit.
14. **RESOLVED (ruling Q4): the action was wrong, not the invariant.** The flag's own escape hatch was the right one — a reset *does* take all migrations. `resetCluster` was modelled backwards and has been rewritten (see the Q4 answer below); with the rewind now taking every record, `inv_handoff_owned` states all three halves again: the literal bound `m.attempt <= ctl.handoff_seq`, the stronger minting claim now **keyed by reset epoch** (`spent.exists(t => t.epoch == ctl.reset_epoch and t.seq == m.attempt)`), and the unminted-0 half verbatim.
15. **The graceful-failover barrier guard was strengthened** from `failover_fence != Set()` to `movedSlotsOf(slots, old) ⊆ failover_fence`. Found by the random run: with the weak guard, arming the fence for node A and then failing over node B was admitted while `defects.barrier` (which states the ruled property) flagged it. Issue 26's text says "armed over the moved slots", so the guard was brought up to the invariant rather than the invariant down to the guard.
16. **RESOLVED (ruling Q6 = option b).** `failPromotion` still never removes the residue entry — the single-deleter rules forbid it — but the entry now **counts its failures**: `Residue.promotion_failures`, incremented on every rollback. Without it a repeatedly-failing promotion is indistinguishable from one never attempted, and the operator arms have nothing to key an escalation on.

---

## The six rulings, and the two research answers they asked for

**Q1 — is (b) the straightforward option?** Yes, with one honest caveat. (b) is *semantically* the simpler story: the successor inherits the slot, so it inherits the intent to migrate it; the alternative (a) leaves the model asserting that a failover silently abandons an operator-issued migration, and it makes ext-5's own stated scenario — a position minted before the failover meeting a record minted after it — unreachable through the only action that produces it. The caveat is that (b) costs one extra helper (`rehomedMigrations`) and narrows issue 15's prune from "every migration naming the old primary" to "the target leg only", so issue 15's ruling text now needs the source leg read as re-homed rather than dropped. That is a spec-text follow-up, not a model problem.

**Q4 — when is `ResetCluster` used, and what does it actually do?** `CLUSTER RESET SOFT|HARD` (TR-CLUSTER-035, FM-CLUSTER-006, `frogdb-server/crates/cluster/src/commands.rs:816-863`) is the operator's "forget this cluster" button — used to recycle a node into a different cluster, or to unwedge a node whose replicated view is unusable. Researched behaviour, which the model had **backwards**:
  - clears **all** slot assignments (not just the resetting node's),
  - takes **all** migrations, paying their release events (not just those naming the node),
  - rewinds `handoff_seq` to 0 — the one lawful rewind in the system (FM-CLUSTER-086/100),
  - **reduces membership to just this node**: the resetting node's own `NodeInfo` is *retained*, `registration_seq` and `run_identity` carried unchanged, and every *other* member's cell is deleted. The design doc states it directly: "a reset ... deletes every **other** member's cell."
  - HARD additionally re-keys the node and zeroes both epochs — FM-CLUSTER-010's documented non-monotonicity.
`resetCluster(n)` now encodes the SOFT path exactly. HARD is **deliberately not modelled**, and the machine file says why: encoding the epoch rewind would falsify `inv_epoch_never_decreases` by design rather than by defect. Consequence: the literal INV-HANDOFF-1 bound is restorable (flag 14).

**Q5 — model-only, or real code implications? Is there a "more correct" solution?** Real code implications, and yes. `require_target_replica_ack` is a **captured parameter**: read from the proposer's local config exactly once, stamped into the `BeginSlotMigration` payload, and immutable thereafter — "every replicated predicate that consults a bound reads the **record field, never the config**". The mutable-global shape the model had is precisely the anti-pattern that produces (a) permanent Raft divergence when two appliers hold different config values, and (b) a durability conjunct silently voided mid-migration by an operator's config flip. The more correct solution is the per-record captured parameter, and that is what landed.

**Q2 / Q3 / Q6** are covered by resolved flags 10, 8 (unchanged — the fence is kept, which was already the encoded behaviour) and 16.

### Still-open item, flagged rather than actioned
The design (around line 10255) describes an **orphan re-home arm** for `promoted == true ∧ target_gone` — "re-assign to source removing the entry, or to another primary clearing the flag" — as the recovery action behind `residueHasAnEffectiveRemover`. The model has the *defer* (`reapSlots` waits while `target_gone`) but no recovery arm, so that residue class is currently terminal in the model. Q2's `residueHasAnEffectiveRemover` will not be statable without it.

### Coverage note for Q2/Q3
Random exploration (8000 samples × 40 steps) reaches `witnessHandoffDrainedNotComplete` and `witnessFailoverDuringOpenMigration` but **not** `witnessResiduePending` — the post-commit region needs `beginMigration → prepareHandoff → confirmDrained → reportTargetIngest → completeMigration` with a matching attempt stamp, which random walks essentially never hit. The `run` tests reach it deterministically. Q2's bounded witnesses will need `step` biasing (the pre-rework model already carried nondet biasing for the same reason).
