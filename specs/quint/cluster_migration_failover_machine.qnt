// cluster_migration_failover_machine — the state machine of the migration x failover
// composite model (formal-spec phase 2, task 2): the `var`s, `init`, every action, and
// `step`. All guards and updates live in cluster_migration_failover_logic; this module
// is the wiring — which variables each transition writes, and which nondeterministic
// choices `step` offers.
//
// Reworked for the issue-31 slot-migration redesign. Structural changes over the
// superseded model:
//   - **repatriation is gone** (`repatriating`, `completeRepatriation`, the `cancelling`
//     record field): the redesign is source-authoritative-until-commit, so the source
//     keeps its copy until the target's promotion is *attested*. `shadow` (ext-8),
//     `residue` (ext-12) and `reapSlots` (ext-7) replace it.
//   - **`provisional_target` is gone**: the ghost it existed to expose is now the
//     first-class `shadow` variable.
//   - **`spent` is re-keyed by reset epoch** (ext-10): `Set[{epoch, seq}]`, so a
//     `ResetCluster` that rewinds the handoff counter makes a *re-mint* distinguishable
//     from a *reuse*.
//   - per-node state that used to be a parallel map (`node_epoch`) is folded into
//     `NodeState`, and the small cluster-level counters into `ctl`, keeping the variable
//     count at 14 so every action can spell out its full frame.
//
// Frame discipline: Quint requires every action to assign every variable, so the
// "keeper" actions below (`keepGraph`, `keepMigAll`, ...) name the unchanged groups.
// Each action lists exactly the keepers for what it does not write; nothing is implicit.
// Two frames recur often enough to be named from the *written* side instead —
// `nodesOnly` and `migrationsOnly` — and two multi-variable postconditions likewise
// (`releaseHoldRegion`, `releaseLocalFence`); all four are defined with the keepers and
// expand to exactly the assignments they replace.
//
// Single responsibility: the wiring — which variables each transition writes, and which
// nondeterministic choices `step` offers.
//   Belongs here:     `var` declarations, `init`, frame keepers, one `action` per modeled
//                     transition, `stutter`, `step`, and the `step` candidate-set values.
//   Does NOT belong:  guard or update *logic* — an action here should read as "check
//                     `can*`, write `apply*`", with any new multi-conjunct condition given
//                     a name in cluster_migration_failover_logic.qnt rather than open-coded
//                     here; invariants, witnesses and `run` tests
//                     (cluster_migration_failover.qnt); types and constants (the two
//                     `*types` modules). This module must never declare a `val inv_*` —
//                     scripts/quint-models.sh would promote the file to a runnable main
//                     model, which it is not.
//
// Navigation — the sections below, in file order. Adding an action means picking the
// section its family already lives in rather than appending at the end:
//   State / Frame keepers / Shared barrier postconditions / Init / Shared derived values
//   Membership and identity      — meetNode, removeNode, resetCluster, setRole,
//                                  reportRunIdentity, loseIncarnationCounter,
//                                  attestReplicaSynced
//   Pending-transition record    — stageFlip, observeRefusal, clearStaleRecord,
//                                  completeAdoption
//   Slot map                     — assignSlot, removeSlot
//   Migration lifecycle          — beginMigration, prepareHandoff
//   Handoff barrier / hold       — feedBuffer, disconnectFeed, localReleaseOnCapBreach,
//                                  selfFenceRelease
//   Stream and target ingest     — sourceWrite, applyAtTarget, makeTargetDurable,
//                                  reportTargetIngest
//   Target-side replicas         — feedTargetReplica, reportReplicaAck,
//                                  attachTargetReplica, detachTargetReplica,
//                                  crashDuringShadowSection
//   Migration lifecycle (cont.)  — confirmDrained, coverageBatch, batchFault,
//                                  reconcileTick, boundAbort, completeMigration,
//                                  abortHandoff, cancelMigration
//   Shadow, promotion, rollback  — relabelShadow, reportPromoted, failPromotion,
//                                  discardShadow
//   Reaper and residue arms      — reapSlots, clearSlotResidue, retargetSlotResidue,
//                                  rehomeOrphanSlot
//   Roles, failover, demotion    — markNodeFailed, markNodeRecovered, armFailoverFence,
//                                  failoverGraceful, failoverAuto, failoverForced,
//                                  demoteNode, demoteNodeExternal
//   Restarts and run re-minting  — sourceRestart, targetRestart, sourceFailover
//   Escape flags and knobs       — setSourceCannotApply, setSourceCannotDrain,
//                                  setTargetSilent
//   step
//
// Rows wired by the actions above (guard/update logic lives in
// cluster_migration_failover_logic.qnt; this file only cites what it *dispatches* to):
// TR-CLUSTER-004 (setRole), TR-CLUSTER-008/009 (assignSlot/removeSlot), TR-CLUSTER-010
// (beginMigration), TR-CLUSTER-011 (prepareHandoff), TR-CLUSTER-012 (confirmDrained),
// TR-CLUSTER-013 (completeMigration), TR-CLUSTER-014 (abortHandoff), TR-CLUSTER-015
// (cancelMigration), TR-CLUSTER-016 (feedBuffer/disconnectFeed/localReleaseOnCapBreach/
// selfFenceRelease), TR-CLUSTER-017/018 (failoverGraceful), TR-CLUSTER-024/025
// (markNodeFailed/markNodeRecovered), TR-CLUSTER-033 (reconcileIdentity), TR-CLUSTER-034
// (stageFlip/observeRefusal/clearStaleRecord/completeAdoption), TR-CLUSTER-042
// (failoverForced).
module cluster_migration_failover_machine {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"
  import cluster_migration_failover_logic.* from "./cluster_migration_failover_logic"

  // ---------------------------------------------------------------------------
  // State.
  // ---------------------------------------------------------------------------

  var nodes: NodeId -> NodeState
  var slots: SlotId -> Option[NodeId]
  var migrations: SlotId -> Option[Migration]
  // ext-8: the target's staged copy of a slot, tagged with the migration and the reset
  // epoch it was minted under.
  var shadow: SlotId -> Option[ShadowTag]
  // ext-12: written by `completeMigration`, marked by `removeNode`, removed only by
  // `clearSlotResidue`. Keyed by slot with the migration id *inside* the entry — see the
  // main file's header for why the pair-keyed form of the design text is modelled this
  // way.
  var residue: SlotId -> Option[Residue]
  // ext-1/ext-5: every position the slot's log ever produced, carrying its minting run —
  // what a forked-history property is stated over.
  var stream_history: SlotId -> Set[Pos]
  // ext-11: writes held at a source behind an armed barrier. The level property
  // (`inv_held_set_empty_while_latched`) is stated over this.
  var held: NodeId -> Set[SlotId]
  var feed_bytes: SlotId -> int
  // ext-16/ext-18: refusals in flight, minted by a refused `reportRunIdentity` and
  // consumed by `observeRefusal`.
  var refusals: Set[Refusal]
  var barriers: { armed: Set[SlotId], failover_fence: Set[SlotId], disconnected: Set[SlotId] }
  // ext-10 (INV-HANDOFF-2, re-keyed): handoff sequence numbers already spent, per reset
  // epoch.
  var spent: Set[{ epoch: int, seq: int }]
  var ctl: {
    handoff_seq: int,
    epoch: int,
    prev_epoch: int,
    reset_epoch: int,
    next_mig: MigrationId,
    next_reg: int,
    next_run: RunId,
    // Q2 cross-step shadows (see `parentsOf`/`keysOf` in the logic module). A Quint
    // invariant reads one state, so the two properties that are about a *step* —
    // ext-17's "every write of the parent pointer moves the parenting token" and ext-8's
    // "an unpromoted residue entry never hands the target a copy" — read the previous
    // state's value here. Carried in `ctl` because `ctl` is the one record every action
    // already rewrites, so no action gains a frame entry.
    prev_parents: NodeId -> { p: Option[NodeId], seq: int },
    prev_keys: NodeId -> Set[SlotId],
  }
  // Ghost flags: set when the *unsafe* variant of a ruled precondition would have been
  // taken. Never read by a guard; only by invariants/witnesses. Each is recomputed inside
  // the action that could take the unsafe variant, from that action's own *outcome* or
  // from state the guard does not consult, so mutating the guard shows up here.
  var defects: {
    demotion: bool, fence: bool, barrier: bool, admission: bool, seqReuse: bool,
    // ext-1/ext-5 — an acked write not covered by the target's attested copy at commit.
    ackLoss: bool,
    // ext-2 — a commit admitted against an unfenced source.
    fencedAdmit: bool,
    // ext-3 — a commit admitted against a record still in `Streaming`.
    phaseAdmit: bool,
    // ext-6/ext-14 — a knob-on commit admitted without a counted replica at the position.
    replicaAck: bool,
    // ext-12 (V8-C6/V10-C3) — the slot map handed a slot to a Replica.
    replicaSlot: bool,
    // ext-4 — the observation counter reached its bound on a tick that saw progress.
    boundAbort: bool,
    // ext-4 (V7-C3) — the seal left the post-seal counter non-zero.
    sealCounter: bool,
    // ext-7 — the reaper deleted the last member-held copy of a slot.
    overDelete: bool,
    // ext-8 — a shadow discarded out from under a residue entry.
    discard: bool,
    // ext-10 — a node entered the membership holding copies.
    untracked: bool,
    // ext-15 (V11-C1) — a hold-region release scheduled after the source stopped serving.
    demotedExec: bool,
    // ext-15 — an admitted report that did not strictly dominate the stored identity.
    identityRegress: bool,
    // ext-15 — a report that cancelled records without writing a new identity.
    spuriousCancel: bool,
    // ext-15 (V8-C2) — a Demotion arm applied against a stale observed role/epoch.
    staleDemote: bool,
    // ext-18 (V21) — a terminal refusal observed against its own record that left the
    // record, the fence or the reply undisposed.
    disposition: bool,
    // ext-12 (M32, ruled 2026-08-19) — a demotion whose chosen successor was not a
    // replica of the demoted primary in the *pre*-state. The semantic observable (the
    // successor actually holds the shard's data) is phase-3 replication-model territory;
    // this ghost is the structural one the migration model can see.
    crossShardSuccessor: bool,
    // TR-CLUSTER-025 / FM-CLUSTER-014 (anti-churn) — a FAIL-flag transition that moved
    // the config epoch. Health churn must never bump it.
    churnEpochBump: bool,
  }
  // Coverage ghosts: set when a behaviour the witnesses care about actually happened.
  var coverage: {
    gracefulPruned: bool,
    forcedDemotedReplica: bool,
    failoverDuringOpenMigration: bool,
    promotionRolledBack: bool,
    reaped: bool,
    emptyBatch: bool,
    // ext-4 — a pre-`Confirm` migration whose target still had records to ingest was
    // aborted by the bound (the calibration obligation `preconfirm_observations` carries).
    healthyAborted: bool,
    // ext-14 — a `require_replica_ack` migration committed at the stock sizings.
    knobCompleted: bool,
    // ext-15 — a node whose two planes had split converged again, replicated -> local.
    // Written by `adoptReplicatedRole` from its own post-state (the split it was looking
    // at is gone in the node it wrote), never from its guard.
    planesConverged: bool,
    // V12-M1 (ruled 2026-08-19) — a `stageFlip` that found outstanding held writes at the
    // staging node and *answered* them in the same step. This is the coverage side of
    // `inv_no_hold_during_staged_flip`: without it the invariant is vacuously true
    // whenever the staging node never held anything.
    stagedFlipAnsweredHolds: bool,
    // M37 (ruled 2026-08-19) — a `completeAdoption` whose dataset-discard leg actually
    // dropped a key, i.e. the adopting node held a copy the replicated map gave it no
    // lawful claim to.
    adoptionDiscardedKeys: bool,
  }

  // ---------------------------------------------------------------------------
  // Frame keepers.
  // ---------------------------------------------------------------------------

  action keepNodes = nodes' = nodes
  action keepSlots = slots' = slots
  action keepGraph = all { keepNodes, keepSlots }
  action keepMigrations = migrations' = migrations
  action keepShadow = shadow' = shadow
  action keepResidue = residue' = residue
  action keepMigAll = all { keepMigrations, keepShadow, keepResidue }
  action keepBarriers = barriers' = barriers
  action keepHeld = held' = held
  action keepFeed = feed_bytes' = feed_bytes
  action keepBar = all { keepBarriers, keepHeld, keepFeed }
  action keepHistory = stream_history' = stream_history
  action keepRefusals = refusals' = refusals
  action keepSpent = spent' = spent
  action keepDefects = defects' = defects
  action keepCoverage = coverage' = coverage
  action keepAux = all { keepHistory, keepRefusals, keepSpent, keepDefects, keepCoverage }
  // `prev_epoch` shadows the epoch of the previous state in *every* step, which is what
  // `inv_epoch_never_decreases` compares against.
  action keepCtl = ctl' = { ...ctl, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) }
  // The disabled arm of a `match` whose other arm is a full transition: Quint's effect
  // checker requires both arms to update the same variables, so "this case is not
  // enabled" has to be spelled with the full frame attached.
  action nope = all { false, keepGraph, keepMigAll, keepBar, keepAux, keepCtl }

  // The two frames named from the *written* side rather than the kept side, because a
  // third of the actions below use one of them verbatim. `nodesOnly(m)` is exactly
  // "`nodes' = m` and nothing else changes"; `migrationsOnly(m)` likewise for the
  // migration map. Spelling them once keeps the frame discipline above intact (the
  // expansion is still explicit, just written down in one place) and makes "which
  // variables does this transition touch?" answerable from the action's first line.
  action nodesOnly(m: NodeId -> NodeState): bool = all {
    nodes' = m,
    keepSlots, keepMigAll, keepBar, keepAux, keepCtl,
  }

  // R1 (issue 41): `nodesOnly` plus the residue re-home a demotion owes (see
  // `residueFollowsDemotion`) — the demotion writers outside the failover family.
  action nodesAndResidue(m: NodeId -> NodeState, res: SlotId -> Option[Residue]): bool = all {
    nodes' = m,
    residue' = res,
    keepSlots, keepMigrations, keepShadow, keepBar, keepAux, keepCtl,
  }

  action migrationsOnly(m: SlotId -> Option[Migration]): bool = all {
    migrations' = m,
    keepGraph, keepShadow, keepResidue, keepBar, keepAux, keepCtl,
  }

  // ---------------------------------------------------------------------------
  // Shared postconditions over the barrier/hold variables.
  // ---------------------------------------------------------------------------

  // The full teardown of slot `s`'s hold region at source `src`: the barrier is disarmed,
  // the feed's disconnected mark cleared, the source's held writes for the slot released
  // and its buffered bytes dropped. Shared verbatim by every action that *resolves* the
  // handoff — `completeMigration`, `abortHandoff`, `cancelMigration`, `boundAbort`.
  action releaseHoldRegion(s: SlotId, src: NodeId): bool = all {
    barriers' = { ...barriers, armed: barriers.armed.exclude(Set(s)),
                  disconnected: barriers.disconnected.exclude(Set(s)) },
    held' = held.set(src, held.get(src).exclude(Set(s))),
    feed_bytes' = feed_bytes.set(s, 0),
  }

  // The *local* release (ext-2/ext-11): the source drops its own fence and empties the
  // whole hold region, but the feed's `disconnected` mark is deliberately NOT cleared —
  // nothing reconnected it. Shared by `localReleaseOnCapBreach` and `selfFenceRelease`.
  action releaseLocalFence(s: SlotId, src: NodeId): bool = all {
    barriers' = { ...barriers, armed: barriers.armed.exclude(Set(s)) },
    held' = held.set(src, held.get(src).exclude(Set(s))),
    feed_bytes' = feed_bytes.set(s, 0),
  }

  // ---------------------------------------------------------------------------
  // Init.
  // ---------------------------------------------------------------------------

  pure def mkNode(role: RoleTag, parent: Option[NodeId], ks: Set[SlotId], reg: int, rid: RunId): NodeState = {
    role: role,
    parent: parent,
    member: true,
    fail: false,
    epoch_own: 0,
    keys: ks,
    synced: true,
    admitted_stage: None,
    promoted_from: None,
    parent_seq: 0,
    // The two planes start agreeing — every split in this model is *produced* by a
    // replicated write landing at a node that is down.
    local_role: role,
    local_parent: parent,
    registration_seq: Some(reg),
    incarnation: 1,
    identity_seq: 1,
    stored_identity: Some({ inc: 1, seq: 1, run_id: rid }),
    booted: true,
    run_id: rid,
    record: None,
    stage_counter: 0,
    node_fenced: false,
    reply_emitted: false,
    last_disposition: None,
    cannot_apply: false,
    cannot_drain: false,
    silent: false,
    replica_copy: 0,
    counted: false,
    install_incomplete: false,
  }

  action init = all {
    // Node 1 is the primary of every slot and holds every copy; 2 and 3 are its
    // replicas; 4 is an unrelated, slotless primary (the migration target of choice).
    nodes' = Map(
      1 -> mkNode(Primary, None, SLOTS, 1, 1),
      2 -> mkNode(Replica, Some(1), Set(), 2, 2),
      3 -> mkNode(Replica, Some(1), Set(), 3, 3),
      4 -> mkNode(Primary, None, Set(), 4, 4),
    ),
    slots' = SLOTS.mapBy(_ => Some(1)),
    migrations' = SLOTS.mapBy(_ => None),
    shadow' = SLOTS.mapBy(_ => None),
    residue' = SLOTS.mapBy(_ => None),
    stream_history' = SLOTS.mapBy(_ => Set()),
    held' = NODES.mapBy(_ => Set()),
    feed_bytes' = SLOTS.mapBy(_ => 0),
    refusals' = Set(),
    barriers' = { armed: Set(), failover_fence: Set(), disconnected: Set() },
    spent' = Set(),
    ctl' = { handoff_seq: 0, epoch: 0, prev_epoch: 0, reset_epoch: 0, next_mig: 1,
             next_reg: 5, next_run: 5,
             // The shadows start at the initial nodes map, so the very first step is
             // compared against `init` rather than against a placeholder.
             prev_parents: Map(1 -> { p: None, seq: 0 }, 2 -> { p: Some(1), seq: 0 },
                               3 -> { p: Some(1), seq: 0 }, 4 -> { p: None, seq: 0 }),
             prev_keys: Map(1 -> SLOTS, 2 -> Set(), 3 -> Set(), 4 -> Set()) },
    defects' = { demotion: false, fence: false, barrier: false, admission: false, seqReuse: false,
                 ackLoss: false, fencedAdmit: false, phaseAdmit: false, replicaAck: false,
                 replicaSlot: false, boundAbort: false, sealCounter: false, overDelete: false,
                 discard: false, untracked: false, demotedExec: false, identityRegress: false,
                 spuriousCancel: false, staleDemote: false, disposition: false,
                 crossShardSuccessor: false, churnEpochBump: false },
    coverage' = { gracefulPruned: false, forcedDemotedReplica: false, failoverDuringOpenMigration: false,
                  promotionRolledBack: false, reaped: false, emptyBatch: false,
                  healthyAborted: false, knobCompleted: false, planesConverged: false,
                  stagedFlipAnsweredHolds: false, adoptionDiscardedKeys: false },
  }

  // ---------------------------------------------------------------------------
  // Shared derived values.
  // ---------------------------------------------------------------------------

  def barrierArmed(s: SlotId): bool = barriers.armed.contains(s)

  val currentView: ClusterView = {
    nodes: nodes, slots: slots, migrations: migrations, residue: residue,
    armed: barriers.armed, disconnected: barriers.disconnected,
    feed_bytes: feed_bytes, held: held, epoch: ctl.epoch,
  }

  // Writes back everything `applyFailoverCommon` (and the demotion transitions sharing
  // its postcondition) touches. The failover fence is single-shot: consuming it here is
  // what makes a *second* proposal carrying the same observation refusable.
  action applyView(v: ClusterView): bool = all {
    nodes' = v.nodes,
    slots' = v.slots,
    migrations' = v.migrations,
    residue' = v.residue,
    barriers' = { armed: v.armed, failover_fence: Set(), disconnected: v.disconnected },
    feed_bytes' = v.feed_bytes,
    held' = v.held,
    ctl' = { ...ctl, epoch: v.epoch, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
  }

  // ---------------------------------------------------------------------------
  // Membership and identity.
  // ---------------------------------------------------------------------------

  // ext-10 (V8-C1): a joiner whose main keyspace is non-empty is refused — the gate that
  // makes `inv_member_keyspace_is_tracked` hold once the self-cleanse is gone. The fresh
  // registration mints a new `registration_seq` (ext-17/V18-C1), which is what strands
  // any record staged under the previous one.
  action meetNode(n: NodeId): bool = {
    // Recomputed from the *outcome*: which slots does the node that just entered the
    // membership hold a copy of that no tracking object reaches it through? A fresh
    // registration names the joiner in nothing, so any copy of a still-claimed slot is
    // stranded the moment the join lands. Independent of `canMeetNode`'s phrasing, so
    // reverting the gate (or replacing it with a self-cleanse) surfaces here.
    val stranded = SLOTS.filter(s => and {
      nodes.get(n).keys.contains(s),
      not(isUnclaimedSlot(slots, migrations, residue, s)),
      slots.get(s) != Some(n),
    })
    all {
      canMeetNode(nodes, n),
      nodes' = nodes.set(n, {
        ...writeParentPointer(nodes.get(n), Primary, None, false, None, None),
        member: true, fail: false, booted: false,
        // A fresh registration is the node *presenting itself*: the two planes agree at
        // the join by construction, so a rejoin is never a split to be adopted over.
        local_role: Primary, local_parent: None,
        registration_seq: Some(ctl.next_reg),
      }),
      ctl' = { ...ctl, next_reg: ctl.next_reg + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
      defects' = { ...defects, untracked: defects.untracked or stranded != Set() },
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepCoverage,
    }
  }

  // TR-CLUSTER-003 + ext-12's mark-don't-remove + ext-17's detach. `wipe` additionally
  // drops the node-local *stage counter* (a store loss); the record itself is node-local
  // and crash-durable, so it deliberately survives (ext-18), which is what makes the
  // cross-registration stage-id collision expressible.
  action removeNode(n: NodeId, force: bool, wipe: bool): bool = {
    val prunedSet = slotsNaming(migrations, n)
    // ext-17's detach, which is `repointDependants` with no upstream to follow to: the
    // departing node takes no one with it and leaves no one chained off it (R1/issue 41).
    val detached = repointDependants(nodes, n, None)
    all {
      canRemoveNode(nodes, slots, n, force),
      // Issue 33: `ClusterState`'s `RemoveNode` *deletes* the node record, so there is no
      // live data anywhere to recover the departed node's last role/parent/epoch from.
      // The model keeps `nodes` a total map, so the tombstone has to carry the same
      // "nothing here" values the conformance harness's projection produces for an absent
      // record — `Replica`, no parent, epoch 0 — rather than the node's last live ones.
      // The pointer null goes through the ext-17 funnel like every other parent write, so
      // `inv_role_written_only_by_declared_writers` still sees the token move. The
      // keyspace is deliberately *not* cleared: ext-12's mark-don't-remove keeps a
      // departing node's copies (V8-C1).
      //
      // R10, ruled 2026-08-20: the **replicated run identity goes with the record**. It is
      // a field of the deleted `NodeInfo`, not a node-local durable like `stage_counter`,
      // so a removal that "deletes the node record" cannot leave the cell standing. Rust
      // has no field-level forget path at all — there is no run-identity cell in
      // `NodeInfo` yet (`frogdb-server/crates/cluster/src/types.rs`), and the two paths
      // that clear peer state clear the whole record: `ClusterCommand::RemoveNode` ->
      // `inner.nodes.remove(&node_id)` (commands.rs) for CLUSTER FORGET, and
      // `ResetCluster` -> `inner.nodes.clear()` for CLUSTER RESET. Both project onto
      // `stored_identity: None` **plus** the node leaving `member`, which is exactly this
      // arm and `resetCluster`'s below. That makes `identityOrderOk`'s absent-operand cell
      // walk-reachable (remove, then rejoin via `meetNode`, then report a Demotion), so
      // `isRefusalTerminal` arm 4b is forced by the walk rather than only by a constructed
      // view. A rejoin re-registers from nothing, which is what the empty cell says.
      nodes' = detached.set(n, {
        ...writeParentPointer(detached.get(n), Replica, None, false, None, None),
        member: false, registration_seq: None,
        epoch_own: 0,
        booted: false, counted: false,
        stored_identity: None,
        stage_counter: if (wipe) 0 else detached.get(n).stage_counter,
      }),
      // A target departure unassigns the slot (V7-M6) — as does any departure of an
      // owner reached through `force`.
      slots' = SLOTS.fold(slots, (acc, s) => if (slots.get(s) == Some(n)) acc.set(s, None) else acc),
      migrations' = prunedMigrations(migrations, n),
      residue' = markResidueOnDeparture(residue, n),
      keepShadow,
      barriers' = { ...barriers, armed: barriers.armed.exclude(prunedSet),
                    disconnected: barriers.disconnected.exclude(prunedSet) },
      held' = NODES.fold(held, (acc, m) => acc.set(m, acc.get(m).exclude(prunedSet))),
      feed_bytes' = SLOTS.fold(feed_bytes, (acc, s) => if (prunedSet.contains(s)) acc.set(s, 0) else acc),
      keepAux,
      keepCtl,
    }
  }

  // ext-10 + TR-CLUSTER-035: `CLUSTER RESET SOFT`, gated by the same non-empty-keyspace
  // refusal as `meetNode` (V8-C1, FM-CLUSTER-006).
  //
  // The direction matters and was previously modelled backwards. A reset does not evict
  // the *resetting* node — it keeps its own `NodeInfo` (registration sequence and stored
  // run identity carried unchanged) and deletes **every other member's cell**, leaving a
  // one-node cluster. It also drops *all* slot assignments and takes *all* migrations,
  // not only the ones naming this node.
  //
  // `handoff_seq` rewinding to 0 is the one lawful rewind in the model
  // (FM-CLUSTER-086/100): the reset-epoch bump is what keeps a re-minted sequence number
  // distinguishable from a reused one, which is why `spent` is keyed by reset epoch.
  //
  // Only SOFT is modelled. HARD additionally re-keys the node and zeroes both epochs;
  // that is FM-CLUSTER-010's documented non-monotonicity, and encoding it here would
  // falsify `inv_epoch_never_decreases` by design rather than by defect.
  action resetCluster(n: NodeId): bool = {
    // R10: the ejected members' records are *deleted* by the reset (Rust's
    // `inner.nodes.clear()`), so their replicated run identity goes with them — same
    // record-scoped forget as `removeNode` above, where the reasoning is written out.
    val cleared = NODES.fold(nodes, (acc, m) =>
      if (m == n) acc
      else acc.set(m, { ...writeParentPointer(acc.get(m), acc.get(m).role, None, false, None, None),
                        member: false, registration_seq: None, booted: false, counted: false,
                        stored_identity: None }))
    val departed = NODES.filter(m => m != n and nodes.get(m).member)
    all {
      canResetCluster(nodes, n),
      // The resetting node stays a member with its identity intact — it keeps its own
      // cell, so `booted` and `stored_identity` are untouched (a reset is not a restart).
      // It becomes a parentless Primary of an empty keyspace.
      nodes' = cleared.set(n, writeParentPointer(cleared.get(n), Primary, None, false, None, None)),
      slots' = SLOTS.mapBy(_ => None),
      migrations' = SLOTS.mapBy(_ => None),
      // ext-12's mark-don't-remove still applies to everyone the reset ejected.
      residue' = departed.fold(residue, (acc, m) => markResidueOnDeparture(acc, m)),
      // Shadows are deliberately NOT dropped: an orphan shadow minted under the previous
      // reset epoch must be reachable, and must not wedge the next migration
      // (`inv_no_orphan_shadow_blocks_ingest` — Begin's discard-then-ingest is what saves
      // it).
      keepShadow,
      barriers' = { armed: Set(), failover_fence: Set(), disconnected: Set() },
      held' = NODES.mapBy(_ => Set()),
      feed_bytes' = SLOTS.mapBy(_ => 0),
      ctl' = { ...ctl, handoff_seq: 0, reset_epoch: ctl.reset_epoch + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
      // The reset's half of `inv_member_keyspace_is_tracked`'s ghost, recomputed from the
      // outcome: the reset drops *every* slot assignment, so a resetting node that keeps
      // copies keeps them with nothing left to track them by.
      defects' = { ...defects, untracked: defects.untracked or nodes.get(n).keys != Set() },
      keepHistory, keepRefusals, keepSpent, keepCoverage,
    }
  }

  // TR-CLUSTER-004.
  //
  // R1 (issue 41): a role write that leaves `n` a **Replica** is a demotion, so `n`'s
  // dependants follow it to its new upstream (`repointDependants`) instead of being left
  // hanging off a node that is no longer a primary. A write to Primary keeps them.
  action setRole(n: NodeId, role: RoleTag, parent: Option[NodeId]): bool = {
    val base = if (role == Replica) repointDependants(nodes, n, parent) else nodes
    val post = base.set(n, writeParentPointer(base.get(n), role, parent, false, None, None))
    val postRes = if (role == Replica) residueFollowsDemotion(post, residue, n, parent) else residue
    all {
      canSetRole(nodes, n, role, parent),
      nodesAndResidue(post, postRes),
    }
  }

  // ext-15: the kind-stamped identity report. The Boot arm lifts the boot-ordering
  // guard; the Demotion arm is the one that *writes role and parent* (V7-C1) and is
  // fenced on the observed role/epoch (V8-C2). A refusal mints a `Refusal` carrying the
  // record pair, classified by the first failing conjunct in the declared order.
  //
  // `replay: true` is the reordered/replayed proposal — a report minted earlier in the
  // boot, carrying the `(incarnation, identity_seq)` pair the replicated cell already
  // holds. It fails the ordering conjunct and is refused.
  //
  // ext-15 (V10-M1), the replicated form of "a source restart aborts the migration":
  // applying a run-identity **field write** takes every open migration this node
  // *sources*, and pays the release events those records owed (barrier, held region,
  // buffered feed) exactly as `cancelMigration` would. The cancel binds to the field
  // write — `identityWritten`, an actual change of the stored triple — and never to mere
  // admission, so a topology-only re-proposal at equality (and any refused report, replay
  // included) cancels nothing. It is also what the physical `sourceRestart` deliberately
  // does **not** do: the record dies when the boot report applies, not when the process
  // dies.
  action reportRunIdentity(n: NodeId, kind: TransitionKind, obsRole: RoleTag, obsEpoch: int,
                           replay: bool): bool = {
    val ns = nodes.get(n)
    // ext-15's re-mint rule (see `bootMintOf`): a fresh boot mint is proposed *above* the
    // replicated pair when the node's own counter no longer dominates it, so a node that
    // lost its incarnation counter is never mute. Replays and `Demotion` re-proposals are
    // untouched, which is what keeps the `Ordering` refusal reachable.
    val mint = bootMintOf(ns, kind, replay)
    val inc = mint.inc
    val seq = mint.seq
    val cls = refusalClassOf(nodes, n, kind, inc, seq, obsRole, obsEpoch)
    val admitted = cls == Whole
    val identity = { inc: inc, seq: seq, run_id: ns.run_id }
    val identityWritten = admitted and ns.stored_identity != Some(identity)
    val cancelled = if (identityWritten) slotsSourcedBy(migrations, n) else Set()
    // `incarnation` follows the mint: in the ordinary case it is already `inc`, and after a
    // re-mint the node's own counter takes the value it just published rather than staying
    // at the lost `0`.
    val stamped = { ...ns, incarnation: inc, identity_seq: seq, stored_identity: Some(identity) }
    // The two post-states named once, so the ghosts below read what this step *did* rather
    // than re-deriving it from the same guard the step was admitted by.
    val postNodes =
      if (not(admitted)) nodes
      else if (kind == Boot) nodes.set(n, { ...stamped, booted: true })
      else match ns.record {
        // R1 (issue 41): the Demotion arm is the model's other Primary -> Replica writer,
        // so it repoints `n`'s dependants at the record's upstream in the same step.
        | Some(rec) => repointDependants(nodes, n, Some(rec.upstream)).set(n, {
            ...writeParentPointer(stamped, Replica, Some(rec.upstream), false, Some(rec.stage_id), None),
            record: Some({ ...rec, adopted: true }),
          })
        | None => nodes
      }
    // R1 (issue 41): the residue re-home this demotion owes, for the same reason the
    // failover family pays it in `applyFailoverCommon` — see `residueFollowsDemotion`.
    val postResidue =
      if (not(admitted) or kind == Boot) residue
      else match ns.record {
        | Some(rec) => residueFollowsDemotion(postNodes, residue, n, Some(rec.upstream))
        | None => residue
      }
    val postMigs = if (cancelled == Set()) migrations else cancelledBySource(migrations, n)
    val postStored = postNodes.get(n).stored_identity
    // The records this step actually took (an open migration that is gone in the
    // post-state) — the operand of the no-spurious-cancel claim.
    val takenSlots = cancelled.filter(s => migrations.get(s) != None)
    all {
      // A Demotion report only exists against a staged record.
      kind == Boot or (match ns.record { | Some(rec) => rec.kind == Demotion | None => false }),
      // R1 (issue 41), the second half of the repoint rule: this arm is the model's other
      // Primary -> Replica *parent* writer, and until now it was the only role writer that
      // did not obey TR-CLUSTER-004 (`canSetRole`: a Replica's parent is a live Primary).
      // A record staged against an upstream that is a Replica by the time it adopts parks
      // the demoted node in a headless shard — pre-fix as the `1 <-> 4` parent cycle
      // t5-blocked.md recorded, post-repoint as a parentless Replica — and either shape
      // empties `shardPrimary`, so an unpromoted residue entry sourced there loses both
      // `shardHoldsCopy` (`inv_source_keeps_its_copy_until_promotion_attested`) and its
      // level-triggered re-home (`inv_residue_has_an_effective_remover`). Blocked rather
      // than refused: this is the structural rule `setRole` is gated by, not one of
      // ext-16's four refusal classes. The record survives, and re-adopts once the
      // upstream is a live primary again (or is disposed of by a terminal refusal), so no
      // liveness obligation is dropped. Redis is the same rule: `CLUSTER REPLICATE`
      // refuses a non-master target ("I can only replicate a master, not a replica").
      kind == Boot or (match ns.record {
        | Some(rec) => isLivePrimary(nodes, rec.upstream)
        | None => false
      }),
      nodes' = postNodes,
      refusals' =
        if (admitted) refusals
        else match ns.record {
          | Some(rec) => refusals.union(Set({
              node: n, stage_id: rec.stage_id, reg_seq: rec.staged_reg_seq,
              class: cls, terminal: isRefusalTerminal(ns.stored_identity, cls) }))
          | None => refusals
        },
      migrations' = postMigs,
      barriers' = { ...barriers, armed: barriers.armed.exclude(cancelled),
                    disconnected: barriers.disconnected.exclude(cancelled) },
      held' = held.set(n, held.get(n).exclude(cancelled)),
      feed_bytes' = SLOTS.fold(feed_bytes, (acc, s) => if (cancelled.contains(s)) acc.set(s, 0) else acc),
      // ext-15's three ghosts, each recomputed here from **this step's post-state** rather
      // than from `refusalClassOf`'s verdict. Q3's mutation battery showed the earlier
      // forms (`admitted and not(identityOrderOk(...))`, `cancelled != Set() and stored ==
      // Some(identity)`) were vacuous: both antecedents were contradictions of the very
      // guard that admitted the step, so no mutation of that guard could reach them. The
      // forms below compare the cell the step *wrote* against the cell it *overwrote*, and
      // the records it *took* against whether it wrote at all — so weakening the ordering
      // conjunct (a replay applied over the live cell) or unbinding the cancel from the
      // field write (a refused report taking records) each trips its own named property.
      //   `identityRegress` — the identity the step left in the cell does not strictly
      //     dominate the one it replaced (`inv_run_identity_never_regresses`);
      //   `spuriousCancel` — records taken by a step that left the identity cell
      //     unchanged, i.e. a cancel bound to admission (or to nothing) rather than to the
      //     field write (`inv_no_spurious_cancel`);
      //   `staleDemote` — a Demotion arm that rewrote role and parent although the
      //     role/epoch it observed is not the pair the node holds now
      //     (`inv_promotion_is_not_reverted_without_a_failover`).
      defects' = { ...defects,
        identityRegress: defects.identityRegress
          or (postStored != ns.stored_identity and match postStored {
            | Some(st) => not(identityOrderOk(ns.stored_identity, kind, st.inc, st.seq))
            | None => true
          }),
        spuriousCancel: defects.spuriousCancel
          or (takenSlots != Set() and postStored == ns.stored_identity),
        staleDemote: defects.staleDemote
          or (admitted and kind == Demotion
              and (ns.role != obsRole or ns.epoch_own != obsEpoch)),
      },
      residue' = postResidue,
      keepSlots, keepShadow, keepHistory, keepSpent, keepCoverage, keepCtl,
    }
  }

  // ext-15: the node-local incarnation counter is lost (store loss). Every later report
  // is then refused with class `Ordering` against the surviving replicated cell — the
  // trace the `Ordering` disposition arms exist for.
  action loseIncarnationCounter(n: NodeId): bool = all {
    nodes.get(n).incarnation > 0,
    nodesOnly(nodes.set(n, { ...nodes.get(n), incarnation: 0, identity_seq: 0 })),
  }

  // ext-16: the replica's attestation that it holds the shard's data, fenced on the
  // (primary, parent_seq, registration_seq) triple it observed — a stale attestation
  // minted before a re-parent cannot land.
  action attestReplicaSynced(n: NodeId, obsPrimary: NodeId, obsParentSeq: int, obsRegSeq: int): bool = all {
    canAct(nodes, n),
    nodes.get(n).role == Replica,
    nodes.get(n).parent == Some(obsPrimary),
    nodes.get(n).parent_seq == obsParentSeq,
    nodes.get(n).registration_seq == Some(obsRegSeq),
    nodesOnly(nodes.set(n, { ...nodes.get(n), synced: true })),
  }

  // ---------------------------------------------------------------------------
  // ext-15/ext-16: the two planes.
  // ---------------------------------------------------------------------------

  // The replicated -> local convergence. It writes the **node-local plane only** — it is
  // deliberately not a `primary_id` writer, and `inv_role_written_only_by_declared_
  // writers` is keyed on the replicated cell it never touches. It runs *before* the boot
  // report proposes (it does not require `canAct`), which is the whole point: a node that
  // crashed before its failover promotion took local effect would otherwise boot as a
  // replica of a primary that no longer exists, phrase a `Demotion` report against a role
  // it no longer has, and be refused forever.
  action adoptReplicatedRole(n: NodeId): bool = {
    val ns = nodes.get(n)
    val post = { ...ns, local_role: ns.role, local_parent: ns.parent }
    all {
      canAdoptReplicatedRole(nodes, n),
      nodes' = nodes.set(n, post),
      // Recomputed from the post-state: a mutation that adopts into the wrong plane (or
      // adopts nothing) leaves the difference test still true and sets nothing.
      coverage' = { ...coverage, planesConverged: coverage.planesConverged
        or (identityFactsDiffer(nodes, n) and not(identityFactsDiffer(nodes.set(n, post), n))) },
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
    }
  }

  // ext-15 (V8-C6): the level-triggered re-proposal. Not a new apply path — it is
  // `reportRunIdentity` re-phrased from *current* replicated facts, which is what makes a
  // refused report recoverable without an operator. The kind is stamped from the record
  // (the V7-C1 arm-selector rule), never chosen by the proposer.
  action reconcileIdentity(n: NodeId): bool = all {
    canReconcileIdentity(nodes, n),
    reportRunIdentity(n, reconcileKindOf(nodes, n), nodes.get(n).role, nodes.get(n).epoch_own, false),
  }

  // ---------------------------------------------------------------------------
  // ext-18: the pending-transition record.
  // ---------------------------------------------------------------------------

  // V12-M1, ruled 2026-08-19 (effect-based closure of `inv_no_hold_during_staged_flip`).
  // Staging the flip **answers** whatever the node was holding — an about-to-demote
  // primary refuses or redirects the held writes rather than keeping them hostage — and
  // so empties its hold region in the same step. This is an *effect* of staging, not a
  // guard against it: refusing to stage while holds are outstanding would wire demotion
  // liveness to drain-release liveness (the issue-17 class), which the ruling rejected.
  // The complementary half — no *new* hold latches at a node with a pending record —
  // lives in `prepareHandoff` (`canLatchHold`). Together the two close the invariant.
  //
  // Modelling note: the answers themselves are not materialised. `refusals` is the
  // run-identity refusal channel (ext-16/ext-18), typed over `{node, stage_id, reg_seq,
  // class, terminal}`, and a client write refusal is not one of those; the coverage
  // ghost below is what keeps the invariant from being vacuous.
  action stageFlip(n: NodeId, upstream: NodeId): bool = {
    val ns = nodes.get(n)
    // A stage is minted **against a registration** (ext-17/V18-C1): with no live cell
    // there is nothing to pair the stage id with, so there is nothing to stage.
    val rs = match ns.registration_seq { | Some(x) => x | None => -1 }
    all {
      ns.member,
      ns.record == None,
      upstream != n,
      rs >= 0,
      nodes' = nodes.set(n, {
        ...ns, stage_counter: ns.stage_counter + 1, node_fenced: true, reply_emitted: false,
        record: Some({ kind: Demotion, upstream: upstream, stage_id: ns.stage_counter + 1,
                       staged_reg_seq: rs, adopted: false }),
      }),
      held' = held.set(n, Set()),
      coverage' = { ...coverage,
        stagedFlipAnsweredHolds: coverage.stagedFlipAnsweredHolds or held.get(n) != Set() },
      keepSlots, keepMigAll, keepBarriers, keepFeed,
      keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
    }
  }

  // ext-18 (V28-M6): observation and disposition are the *same* step, and the record is
  // only disposed of when both halves of the (stage_id, registration_seq) pair bind.
  // `last_disposition` is the auxiliary history the foreign-disposal invariant reads.
  action observeRefusal(n: NodeId, r: Refusal): bool = {
    val ns = nodes.get(n)
    // Does this observation *dispose* of the record? Only when both halves of the pair
    // bind and the class is one of the declared terminal ones.
    val disposes = match ns.record {
      | Some(rec) => refusalBinds(rec, r) and r.terminal
      | None => false
    }
    val disp = match ns.record {
      | Some(rec) => Some({ refusal_stage_id: r.stage_id, refusal_reg_seq: r.reg_seq,
                            record_stage_id: rec.stage_id, record_reg_seq: rec.staged_reg_seq })
      | None => None
    }
    // `last_disposition` is written on a step that disposes and on no other (ext-18's
    // stated writer rule). Q1 wrote it on every observation carrying a record, including
    // non-binding ones, which made `inv_no_live_record_disposed_by_a_foreign_refusal`
    // false of every observation of a foreign refusal — the very reading the invariant
    // exists to refute. Realigned to the doc here.
    val post =
      if (disposes) { ...ns, record: None, node_fenced: false, reply_emitted: true, last_disposition: disp }
      else ns
    all {
      refusals.contains(r),
      r.node == n,
      nodes' = nodes.set(n, post),
      refusals' = refusals.exclude(Set(r)),
      // ext-18 (V21): recomputed from the post-state, so turning any disposition arm into
      // a silent no-op leaves the record, the fence or the reply undisposed and trips.
      defects' = { ...defects, disposition: defects.disposition or (disposes and not(and {
        post.record == None,
        not(post.node_fenced),
        post.reply_emitted,
      })) },
      keepSlots, keepMigAll, keepBar, keepHistory, keepSpent, keepCoverage, keepCtl,
    }
  }

  // ext-18: the level-triggered clearing clause — a record whose staged registration no
  // longer matches the live cell is cleared, the fence dropped and the client paid,
  // without waiting for a reply that will never come.
  action clearStaleRecord(n: NodeId): bool = all {
    isRecordStale(nodes.get(n)),
    nodesOnly(nodes.set(n, { ...nodes.get(n), record: None, node_fenced: false, reply_emitted: true })),
  }

  // ext-18: the staged flip lands only after the replicated admission stamped it — and
  // the **stamp itself** is a conjunct (ext-16 (4), V14-M1), not just the record's
  // `adopted` flag. The two are separable exactly where the doc's mutation lives: a boot
  // that re-minted `stageId` the way it re-mints the candidate triple would leave the
  // record adopted while the stamp names the pre-crash stage, and the flip could never
  // land again (`witnessStagedFlipCompletesAcrossCrash` unreachable). Reading the stamp is
  // also what makes an admission *someone else* earned useless here (V13-C1).
  //
  // M37, ruled 2026-08-19 — the **dataset-discard leg**. Adopting a foreign upstream
  // discards the node's dataset (FM-REPLICATION-022), which is precisely why the flip is
  // staged: the discard lands here, at adoption, not at the gate check, so an
  // `AssignSlots` that binds the node in the window between the two still finds its copy
  // afterwards. Modelled as "drop every slot the replicated map gives this node no lawful
  // copy claim to", with the ruling's exemption list verbatim — it owns the slot, it is
  // the source or target of a live migration for it, or a residue entry names it
  // (`trackedDirectly`).
  action completeAdoption(n: NodeId): bool = {
    val ns = nodes.get(n)
    val discarded = ns.keys.filter(s => not(trackedDirectly(slots, migrations, residue, n, s)))
    all {
      match ns.record { | Some(rec) => rec.adopted and ns.admitted_stage == Some(rec.stage_id) | None => false },
      nodes' = nodes.set(n, { ...ns, record: None, node_fenced: false, reply_emitted: true,
                              keys: ns.keys.exclude(discarded) }),
      coverage' = { ...coverage,
        adoptionDiscardedKeys: coverage.adoptionDiscardedKeys or discarded != Set() },
      keepSlots, keepMigAll, keepBar,
      keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
    }
  }

  // ---------------------------------------------------------------------------
  // Slot map.
  // ---------------------------------------------------------------------------

  // Assignment also gives the node the copy — the model has no separate "load the slot"
  // step, and without it no residue entry would ever be reapable.
  action assignSlot(s: SlotId, n: NodeId): bool = all {
    canAssignSlot(nodes, migrations, residue, s, n),
    slots' = slots.set(s, Some(n)),
    nodes' = nodes.set(n, { ...nodes.get(n), keys: nodes.get(n).keys.union(Set(s)) }),
    keepMigAll, keepBar, keepAux, keepCtl,
  }

  action removeSlot(s: SlotId): bool = all {
    canRemoveSlot(migrations, residue, s),
    slots' = slots.set(s, None),
    keepNodes, keepMigAll, keepBar, keepAux, keepCtl,
  }

  // ---------------------------------------------------------------------------
  // Migration lifecycle.
  // ---------------------------------------------------------------------------

  // ext-10's Begin-time **discard-then-ingest**: whatever shadow the slot carried (an
  // orphan from a pre-reset epoch, say) is discarded and re-minted under the new
  // migration id and the current reset epoch — an orphan must never wedge the ingest.
  // ext-14 (V6-C2): `requireReplicaAck` stands for the proposer's local
  // `require_target_replica_ack` config at proposal time. It is nondet because two
  // proposers may hold different values — but it is *captured* into the record here and
  // never re-read, so the appliers cannot diverge over it.
  action beginMigration(s: SlotId, source: NodeId, target: NodeId, requireReplicaAck: bool): bool = all {
    canBeginMigration(nodes, slots, migrations, residue, s, source, target),
    canAct(nodes, source),
    canAct(nodes, target),
    migrations' = migrations.set(s, Some(newMigration(ctl.next_mig, source, target,
                                                      nodes.get(source).run_id, nodes.get(target).run_id,
                                                      requireReplicaAck))),
    shadow' = shadow.set(s, Some({ mig: ctl.next_mig, epoch: ctl.reset_epoch })),
    ctl' = { ...ctl, next_mig: ctl.next_mig + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
    keepGraph, keepResidue, keepBar, keepAux,
  }

  // TR-CLUSTER-011/034 + ext-2/ext-3. The sequence number is minted from the cluster
  // counter and spent under the current reset epoch (ext-10); the source starts holding
  // writes for the slot.
  action prepareHandoff(s: SlotId): bool = {
    val newSeq = ctl.handoff_seq + 1
    val token = { epoch: ctl.reset_epoch, seq: newSeq }
    match migrations.get(s) {
      | Some(m) => {
          val r = applyPrepareHandoff(migrations, barriers.armed, s, newSeq, nodes.get(m.source).run_id)
          all {
            canPrepareHandoff(migrations, s),
            canAct(nodes, m.source),
            canLatchHold(nodes.get(m.source)),
            migrations' = r.migs,
            barriers' = { ...barriers, armed: r.armed },
            held' = held.set(m.source, held.get(m.source).union(Set(s))),
            spent' = spent.union(Set(token)),
            defects' = { ...defects, seqReuse: defects.seqReuse or spent.contains(token) },
            ctl' = { ...ctl, handoff_seq: newSeq, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
            keepGraph, keepShadow, keepResidue, keepFeed, keepHistory, keepRefusals, keepCoverage,
          }
        }
      | None => nope
    }
  }

  // ---------------------------------------------------------------------------
  // Handoff barrier and hold buffer (ext-2/ext-11).
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-033: the hold buffer fills while the barrier is armed.
  action feedBuffer(s: SlotId): bool = all {
    barrierArmed(s),
    not(barriers.disconnected.contains(s)),
    feed_bytes' = feed_bytes.set(s, feed_bytes.get(s) + 1),
    keepGraph, keepMigAll, keepBarriers, keepHeld, keepAux, keepCtl,
  }

  action disconnectFeed(s: SlotId): bool = all {
    barrierArmed(s),
    not(barriers.disconnected.contains(s)),
    barriers' = { ...barriers, disconnected: barriers.disconnected.union(Set(s)) },
    keepGraph, keepMigAll, keepHeld, keepFeed, keepAux, keepCtl,
  }

  // TR-CLUSTER-034 + ext-2/ext-11: the cap breach releases the source's own fence
  // *locally* — the phase is untouched, and the **whole** hold region is released (the
  // held set for the slot empties, not just the writes already buffered).
  action localReleaseOnCapBreach(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          barrierArmed(s),
          feed_bytes.get(s) > HOLD_BYTE_CAP,
          isServingPrimary(nodes, slots, m, s),
          migrations' = migrations.set(s, Some({ ...m, fenced: false })),
          releaseLocalFence(s, m.source),
          keepGraph, keepShadow, keepResidue, keepAux, keepCtl,
        }
      | None => nope
    }

  // ext-11: the alive-but-unable-to-apply latch releases the fence too — the held set
  // must empty while the latch holds, anywhere in the hold region.
  action selfFenceRelease(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          barrierArmed(s),
          nodes.get(m.source).cannot_apply,
          migrations' = migrations.set(s, Some({ ...m, fenced: false })),
          releaseLocalFence(s, m.source),
          keepGraph, keepShadow, keepResidue, keepAux, keepCtl,
        }
      | None => nope
    }

  // ---------------------------------------------------------------------------
  // Stream and target ingest (ext-1/ext-5).
  // ---------------------------------------------------------------------------

  // ext-1: a write on the source advances its run-tagged log high-water and records the
  // position in the slot's history under the run that minted it.
  action sourceWrite(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => {
          val p = { run_id: nodes.get(m.source).run_id, pos: m.source_log.pos + 1 }
          all {
            not(m.fenced),
            canAct(nodes, m.source),
            isServingPrimary(nodes, slots, m, s),
            migrations' = migrations.set(s, Some({ ...m, source_log: p })),
            stream_history' = stream_history.set(s, stream_history.get(s).union(Set(p))),
            keepGraph, keepShadow, keepResidue, keepBar,
            keepRefusals, keepSpent, keepDefects, keepCoverage, keepCtl,
          }
        }
      | None => nope
    }

  // ext-5: the target applies one record (volatile).
  action applyAtTarget(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          canTargetApply(nodes, m.target),
          m.target_applied < m.source_log.pos,
          migrationsOnly(migrations.set(s, Some({ ...m, target_applied: m.target_applied + 1 }))),
        }
      | None => nope
    }

  // ext-5: node-local durability is all-or-nothing — the durable point catches up to the
  // applied point in one step, and a restart falls back to it.
  action makeTargetDurable(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          m.target_durable < m.target_applied,
          migrationsOnly(migrations.set(s, Some({ ...m, target_durable: m.target_applied }))),
        }
      | None => nope
    }

  // ext-5 (V8-C3/V9-C2): the target's replicated ingest report. It may only propose a
  // position drawn from the **durable** set (a volatile-only report would outlive a
  // power-loss restart it did not survive), it carries the attesting run, and a
  // **cross-run** report *replaces* the stored pair — it may legitimately regress.
  action reportTargetIngest(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => {
          val cur = nodes.get(m.target).run_id
          all {
            canTargetAct(nodes, m.target),
            migrationsOnly(migrations.set(s, Some({ ...m, target_copy:
              if (cur == m.target_copy.run_id) { run_id: cur, pos: max2(m.target_copy.pos, m.target_durable) }
              else { run_id: cur, pos: m.target_durable } }))),
          }
        }
      | None => nope
    }

  // ---------------------------------------------------------------------------
  // Target-side replicas (ext-6/ext-14).
  // ---------------------------------------------------------------------------

  // ext-6: a counted replica of the target ingests one more record of the shadow.
  action feedTargetReplica(n: NodeId): bool = {
    val ns = nodes.get(n)
    all {
      ns.member, ns.counted, not(ns.install_incomplete),
      match ns.parent {
        | Some(t) => SLOTS.exists(s => match migrations.get(s) {
            | Some(m) => m.target == t and ns.replica_copy < m.target_applied
            | None => false
          })
        | None => false
      },
      nodesOnly(nodes.set(n, { ...ns, replica_copy: ns.replica_copy + 1 })),
    }
  }

  // ext-6/ext-14: the counted replicas' ack, as read by `Complete`'s replica conjunct.
  action reportReplicaAck(s: SlotId, n: NodeId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          nodes.get(n).member, nodes.get(n).counted, nodes.get(n).parent == Some(m.target),
          migrationsOnly(migrations.set(s, Some({ ...m,
            target_replica_ack: max2(m.target_replica_ack, nodes.get(n).replica_copy) }))),
        }
      | None => nope
    }

  // ext-14: attaching a replica to the migration target and counting it toward the
  // `Complete` conjunct.
  // R1 (issue 41): attaching a *primary* as a replica of `t` demotes it, so its own
  // dependants follow it to `t` rather than chaining off it.
  action attachTargetReplica(n: NodeId, t: NodeId): bool = {
    val post = repointDependants(nodes, n, Some(t)).set(n,
      { ...writeParentPointer(nodes.get(n), Replica, Some(t), false, None, None),
        counted: true, replica_copy: 0, install_incomplete: false })
    all {
      n != t,
      nodes.get(n).member,
      isLivePrimary(nodes, t),
      not(nodes.get(n).counted),
      nodesAndResidue(post, residueFollowsDemotion(post, residue, n, Some(t))),
    }
  }

  // ext-14: detaching it. With the knob on and *zero* counted replicas, `Complete`'s
  // guard is false — not vacuously true (V5-M3).
  action detachTargetReplica(n: NodeId): bool = all {
    nodes.get(n).counted,
    nodesOnly(nodes.set(n, { ...nodes.get(n), counted: false })),
  }

  // ext-6 (V5-M4): a replica crash mid-section leaves an install without its completion
  // marker. It must be discarded and re-requested at boot, never promoted.
  action crashDuringShadowSection(n: NodeId): bool = all {
    nodes.get(n).counted,
    nodesOnly(nodes.set(n, { ...nodes.get(n), install_incomplete: true })),
  }

  // ---------------------------------------------------------------------------
  // Migration lifecycle, continued: seal, batches, commit and the two aborts.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-012. ext-3: the proposal carries its own attempt stamp, so a *stale*
  // confirm (one minted under an earlier `prepareHandoff`) is proposable and refused.
  action confirmDrained(s: SlotId, att: int): bool =
    match migrations.get(s) {
      | Some(m) => {
          val p = { run_id: nodes.get(m.source).run_id, pos: m.source_log.pos }
          val sealedMigs = applyConfirmDrained(migrations, s, p)
          all {
            canConfirmDrained(nodes, migrations, s, att, p),
            isServingPrimary(nodes, slots, m, s),
            migrations' = sealedMigs,
            // ext-4 (V7-C3), recomputed from the sealed record: the post-seal counter
            // starts from zero, so no pre-`Confirm` observation is ever spent against
            // `draining_observations` (`inv_draining_bound_counts_post_seal_observations`).
            defects' = { ...defects, sealCounter: defects.sealCounter or (match sealedMigs.get(s) {
              | Some(sm) => sm.observations != 0
              | None => true
            }) },
            keepGraph, keepShadow, keepResidue, keepBar,
            keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
          }
        }
      | None => nope
    }

  // ext-9: a coverage batch — the payload may be empty, and an empty batch is admitted
  // as a no-op advance. Neither this nor `batchFault` discards the shadow.
  action coverageBatch(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          canTargetApply(nodes, m.target),
          migrations' = migrations.set(s, Some({ ...m, target_applied: m.source_log.pos })),
          coverage' = { ...coverage, emptyBatch: coverage.emptyBatch or m.target_applied == m.source_log.pos },
          keepGraph, keepShadow, keepResidue, keepBar,
          keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
        }
      | None => nope
    }

  // ext-9: a batch-level fault rolls the volatile applied point back to the durable one.
  // The shadow is untouched — that is the property.
  action batchFault(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          m.target_applied > m.target_durable,
          migrationsOnly(migrations.set(s, Some({ ...m, target_applied: m.target_durable }))),
        }
      | None => nope
    }

  // ext-4: the reconciler's observation tick, with the **narrowed** reset — only actual
  // ingest progress since the last observation clears the counter.
  action reconcileTick(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => {
          val ticked = applyReconcileTick(m)
          all {
            migrations' = migrations.set(s, Some(ticked)),
            // ext-4's liveness pair (v2-C4/N-M6), recomputed from the tick's own outcome:
            // the counter must never *reach* its bound on a tick that saw ingest progress
            // since the last observation. Dropping the reset makes a steadily progressing
            // migration accrue to the bound and trips this
            // (`inv_progressing_migration_never_aborts`).
            defects' = { ...defects, boundAbort: defects.boundAbort
              or (hasObservedProgress(m) and isAbortableByBound(ticked)) },
            keepGraph, keepShadow, keepResidue, keepBar,
            keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
          }
        }
      | None => nope
    }

  // ext-4: the bounded exit. When the phase's observation bound is exhausted the leader
  // cancels — the model's stand-in for "the migration does not wedge forever".
  action boundAbort(s: SlotId): bool =
    match migrations.get(s) {
      | Some(m) => all {
          isAbortableByBound(m),
          migrations' = migrations.set(s, None),
          releaseHoldRegion(s, m.source),
          // ext-4: the calibration obligation `preconfirm_observations` carries — a
          // pre-`Confirm` migration whose target still had records to ingest was taken by
          // the bound (`witnessHealthyDrainAbortedByBound`).
          coverage' = { ...coverage, healthyAborted: coverage.healthyAborted
            or (m.phase == Streaming and m.target_applied < m.source_log.pos) },
          keepGraph, keepShadow, keepResidue,
          keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
        }
      | None => nope
    }

  // TR-CLUSTER-013 + ext-12: the commit. Ownership flips to the target and the residue
  // entry is written with **every** field initialized; the source keeps its copy (it is
  // still the only attested holder) until `reportPromoted` lets the reaper take it.
  action completeMigration(s: SlotId, att: int): bool =
    match migrations.get(s) {
      | Some(m) => all {
          canCompleteMigration(nodes, migrations, s, att),
          // `defects.admission` is recomputed here *independently* of
          // `canCompleteMigration`, from the record's own drained evidence — the
          // invariant is only mutation-sensitive
          // if the witness never reuses the guard it is supposed to police.
          defects' = { ...defects,
            admission: defects.admission or not(
              match m.drained_pos {
                | Some(d) => and {
                    m.phase == Draining,
                    d.run_id == m.source_log.run_id,
                    m.target_copy.pos >= d.pos,
                  }
                | None => false
              }),
            // ext-1/ext-5: every acked write of the record's run is covered by the copy
            // the target attested under its *current* run (`inv_no_acked_write_lost`).
            ackLoss: defects.ackLoss or not(ackedWritesCovered(nodes, stream_history.get(s), m)),
            // ext-2: the source was still sealed (`inv_complete_requires_fenced_source`).
            fencedAdmit: defects.fencedAdmit or not(m.fenced),
            // ext-3: the record had already sealed (`inv_complete_requires_draining_phase`).
            phaseAdmit: defects.phaseAdmit or m.phase != Draining,
            // ext-6/ext-14: with the knob on, a *non-empty* counted replica set held the
            // drained position — an empty set is false, not vacuously true (V5-M3)
            // (`inv_target_replicas_hold_committed_slot`).
            replicaAck: defects.replicaAck or (m.require_replica_ack and not(and {
              countedReplicasOf(nodes, m.target) != Set(),
              match m.drained_pos { | Some(d) => m.target_replica_ack >= d.pos | None => false },
            })),
            // ext-12 (V10-C3): the commit handed the slot to a Primary
            // (`inv_slots_only_assigned_to_primaries`).
            replicaSlot: defects.replicaSlot or nodes.get(m.target).role == Replica,
            // ext-10 per write: the commit retires the record and mints the entry that
            // replaces it as the tracking object for the source's copy. Recomputed from
            // the post-state maps, so a commit that hands ownership away without minting
            // the entry that keeps the loser's copy reachable is seen here.
            untracked: defects.untracked
              or strandedBy(nodes, migrations, residue, nodes, slots.set(s, Some(m.target)),
                            migrations.set(s, None),
                            residue.set(s, Some({ mig: m.id, source: m.source, target: m.target,
                                                  promoted: false, source_gone: false, target_gone: false })),
                            s) != Set(),
          },
          coverage' = { ...coverage, knobCompleted: coverage.knobCompleted or m.require_replica_ack },
          slots' = slots.set(s, Some(m.target)),
          nodes' = nodes.set(m.target, { ...nodes.get(m.target), epoch_own: ctl.epoch + 1 }),
          migrations' = migrations.set(s, None),
          residue' = residue.set(s, Some({ mig: m.id, source: m.source, target: m.target,
                                           promoted: false, source_gone: false, target_gone: false })),
          keepShadow,
          releaseHoldRegion(s, m.source),
          ctl' = { ...ctl, epoch: ctl.epoch + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
          keepHistory, keepRefusals, keepSpent,
        }
      | None => nope
    }

  // TR-CLUSTER-014: release the prepared handoff; the migration survives.
  action abortHandoff(s: SlotId, att: int): bool =
    match migrations.get(s) {
      | Some(m) => all {
          canAbortHandoff(nodes, slots, migrations, s, att),
          migrations' = migrations.set(s, Some({ ...m, attempt: 0, fenced: false, phase: Streaming,
                                                 drained_pos: None })),
          releaseHoldRegion(s, m.source),
          // ext-15 (V11-C1): a release of the hold region executes writes at the source,
          // so it is owed the serving-primary check. Recomputed here from the slot map and
          // the source's role (`inv_no_execution_after_demotion`).
          defects' = { ...defects, demotedExec: defects.demotedExec
            or not(and { slots.get(s) == Some(m.source), isLivePrimary(nodes, m.source) }) },
          keepGraph, keepShadow, keepResidue,
          keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
        }
      | None => nope
    }

  // TR-CLUSTER-015: cancel. With the source authoritative until commit there is nothing
  // to stream back — the record simply goes, and the target's shadow is discarded by the
  // separately schedulable `discardShadow`.
  action cancelMigration(s: SlotId, att: int): bool =
    match migrations.get(s) {
      | Some(m) => all {
          canCancelMigration(nodes, slots, migrations, s, att),
          migrations' = migrations.set(s, None),
          releaseHoldRegion(s, m.source),
          // ext-15 (V11-C1), as in `abortHandoff` above.
          defects' = { ...defects, demotedExec: defects.demotedExec
            or not(and { slots.get(s) == Some(m.source), isLivePrimary(nodes, m.source) }) },
          keepGraph, keepShadow, keepResidue,
          keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
        }
      | None => nope
    }

  // ---------------------------------------------------------------------------
  // ext-8: shadow, promotion (split in two) and rollback.
  // ---------------------------------------------------------------------------

  // The local flip: the staged copy stops being a shadow. It does **not** enter the
  // target's main keyspace yet — that is `reportPromoted`'s job, which is what makes
  // `inv_no_serve_before_attestation` a property about the gap between the two.
  action relabelShadow(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => all {
          shadow.get(s) != None,
          not(r.promoted),
          slots.get(s) == Some(r.target),
          shadow' = shadow.set(s, None),
          keepGraph, keepMigrations, keepResidue, keepBar, keepAux, keepCtl,
        }
      | None => nope
    }

  // The replicated attestation. Only now does the target hold the slot in its own
  // keyspace, and only now may the reaper take the source's copy.
  action reportPromoted(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => all {
          not(r.promoted),
          shadow.get(s) == None,
          slots.get(s) == Some(r.target),
          canAct(nodes, r.target),
          residue' = residue.set(s, Some({ ...r, promoted: true })),
          nodes' = nodes.set(r.target, { ...nodes.get(r.target), keys: nodes.get(r.target).keys.union(Set(s)) }),
          // ext-10 per write: the attestation hands the target a *copy*, so the write is
          // owed the tracking check like any other keyspace write.
          defects' = { ...defects, untracked: defects.untracked
            or strandedBy(nodes, migrations, residue,
                          nodes.set(r.target, { ...nodes.get(r.target), keys: nodes.get(r.target).keys.union(Set(s)) }),
                          slots, migrations, residue.set(s, Some({ ...r, promoted: true })), s) != Set() },
          keepSlots, keepMigrations, keepShadow, keepBar,
          keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
        }
      | None => nope
    }

  // ext-12: the promotion is attempted and fails. Ownership rolls back to the source
  // (which never stopped being authoritative) and the staged copy goes; the **entry
  // stays with `promoted == false`**. Scheduled between `relabelShadow` and
  // `reportPromoted` this is the rollback-wins-the-race trace: the re-label is reversed
  // and nothing was ever served. `canFailPromotion` is what keeps the rollback from
  // landing the slot on a source that has since departed or been demoted.
  action failPromotion(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => all {
          canFailPromotion(nodes, residue, s),
          slots' = slots.set(s, Some(r.source)),
          shadow' = shadow.set(s, None),
          coverage' = { ...coverage, promotionRolledBack: true },
          // ext-10 per write: the rollback moves ownership back under the entry, so any
          // copy it leaves unreachable is this step's doing.
          defects' = { ...defects, untracked: defects.untracked
            or strandedBy(nodes, migrations, residue, nodes, slots.set(s, Some(r.source)),
                          migrations, residue, s) != Set() },
          keepNodes, keepMigrations, keepResidue, keepBar,
          keepHistory, keepRefusals, keepSpent, keepCtl,
        }
      | None => nope
    }

  // ext-8: §5's stated predicate — no live record and no residue entry.
  action discardShadow(s: SlotId): bool = all {
    canDiscardShadow(migrations, residue, shadow, s),
    shadow' = shadow.set(s, None),
    // ext-8: the shadow is the copy the target will be promoted onto, so discarding one a
    // residue entry still claims means the slot the new owner ends up serving was thrown
    // away (`inv_owner_serves_promoted_slot`). Recomputed from the residue map, which the
    // relabel/discard pair alone cannot be told apart by afterwards: both leave
    // `shadow[s] == None`.
    defects' = { ...defects, discard: defects.discard or residue.get(s) != None },
    keepGraph, keepMigrations, keepResidue, keepBar,
    keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
  }

  // ---------------------------------------------------------------------------
  // ext-7/ext-12: the reaper and the residue operator arms.
  // ---------------------------------------------------------------------------

  // The model's **only** member-keyspace deleter, gated on the promotion attestation and
  // deferred while the target has departed (`target_gone`).
  action reapSlots(n: NodeId): bool = {
    val reap = reapableSlots(nodes, migrations, residue, n)
    all {
      reap != Set(),
      nodes' = nodes.set(n, { ...nodes.get(n), keys: nodes.get(n).keys.exclude(reap) }),
      coverage' = { ...coverage, reaped: true },
      // ext-7, recomputed from the *outcome* rather than from `reapableSlots`: no reaped
      // slot may be one no other member still holds — deleting a last copy is the
      // over-deletion `inv_node_keeps_slots_it_owns` forbids. Reverting the attestation
      // gate reaps ahead of `reportPromoted`, when the target holds nothing yet.
      defects' = { ...defects, overDelete: defects.overDelete or reap.exists(s =>
        not(NODES.exists(x => and { x != n, nodes.get(x).member, nodes.get(x).keys.contains(s) }))) },
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepCtl,
    }
  }

  action clearSlotResidue(s: SlotId): bool = all {
    canClearSlotResidue(nodes, slots, migrations, residue, s),
    residue' = residue.set(s, None),
    // ext-10 (V9-M4), recomputed per write from the *outcome*: dropping the entry retires
    // the last tracking object that reached the source's copy. With the attestation
    // conjunct intact the reaper was free to take that copy right up to this step, so the
    // ordinary terminal state is clean; clearing a `promoted == false` entry retires the
    // tracking of a copy the reaper was never allowed to take, and the rollback arm the
    // entry was still owed goes with it (`inv_member_keyspace_is_tracked`).
    defects' = { ...defects, untracked: defects.untracked
      or strandedBy(nodes, migrations, residue, nodes, slots, migrations, residue.set(s, None), s) != Set() },
    keepGraph, keepMigrations, keepShadow, keepBar,
    keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
  }

  // ext-12 (V8-C4, source arm only per V10-C4).
  action retargetSlotResidue(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => match shardPrimary(nodes, r.source) {
          | Some(p) => all {
              canRetargetSlotResidue(nodes, residue, s),
              residue' = residue.set(s, Some({ ...r, source: p, source_gone: false })),
              // ext-10 per write: the entry stops naming the old source. Its copy stays
              // reachable only through the shard closure (`keyIsTracked`) — the re-home
              // moves the entry to that node's own shard primary, which is the point.
              defects' = { ...defects, untracked: defects.untracked
                or strandedBy(nodes, migrations, residue, nodes, slots, migrations,
                              residue.set(s, Some({ ...r, source: p, source_gone: false })), s) != Set() },
              keepGraph, keepMigrations, keepShadow, keepBar,
              keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
            }
          | None => nope
        }
      | None => nope
    }

  // ext-12 (V7-M6): the orphan re-home arm — the recovery action for a `promoted` entry
  // whose target has departed. `dest` selects the disposition (see `canRehomeOrphanSlot`):
  // back to the source, which removes the entry outright, or on to another live primary,
  // which clears `target_gone` and hands the entry back to the ordinary reap-then-clear
  // path. The removing branch is the one place other than `clearSlotResidue` that deletes
  // an entry, and it is an *operator arm* deleting an entry the ordinary path provably
  // cannot reach (the reaper defers on `target_gone` forever), which is why ext-12's
  // single-remover rule is not weakened by it.
  action rehomeOrphanSlot(s: SlotId, dest: NodeId): bool =
    match residue.get(s) {
      | Some(r) => all {
          canRehomeOrphanSlot(nodes, residue, s, dest),
          slots' = slots.set(s, Some(dest)),
          residue' =
            if (dest == r.source) residue.set(s, None)
            else residue.set(s, Some({ ...r, target: dest, target_gone: false })),
          // The new target takes the copy with the slot: an entry that still says
          // `promoted` must name a target that physically holds what it serves, and the
          // source's copy is still there to be handed over (this arm never fires after
          // the reaper, which defers on `target_gone`). The transfer itself — the
          // operator re-seeding the new primary — is deliberately not modelled step by
          // step; only its outcome is. Nothing is *deleted* here, so the reaper stays
          // the model's only member-keyspace deleter.
          nodes' =
            if (dest == r.source) nodes
            else nodes.set(dest, { ...nodes.get(dest), keys: nodes.get(dest).keys.union(Set(s)) }),
          // ext-10 per write: the removing branch retires the entry outright and the
          // on-to-another-primary branch re-points it, so both are owed the check.
          defects' = { ...defects, untracked: defects.untracked
            or strandedBy(nodes, migrations, residue,
                          if (dest == r.source) nodes
                          else nodes.set(dest, { ...nodes.get(dest), keys: nodes.get(dest).keys.union(Set(s)) }),
                          slots.set(s, Some(dest)), migrations,
                          if (dest == r.source) residue.set(s, None)
                          else residue.set(s, Some({ ...r, target: dest, target_gone: false })), s) != Set() },
          keepMigrations, keepShadow, keepBar,
          keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
        }
      | None => nope
    }

  // ---------------------------------------------------------------------------
  // Roles, failover and demotion.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-024/025 + FM-CLUSTER-014 (anti-churn), ruled 2026-08-19. A FAIL-flag
  // transition is *health* news and must never move the config epoch — a flapping node
  // that bumped the epoch on every transition would churn the whole cluster's view.
  // `churnEpoch` names the epoch the step actually writes and the ghost compares it
  // against the pre-state's, so a mutant that bumps here moves the written value and the
  // ghost together and trips `inv_no_churn_epoch_bump`. (`nodesOnly` would fold the epoch
  // into `keepCtl` and make the property unobservable, which is why these two spell out
  // their `ctl` frame.)
  action churnFlag(n: NodeId, failed: bool): bool = {
    val churnEpoch = ctl.epoch
    all {
      nodes' = nodes.set(n, { ...nodes.get(n), fail: failed }),
      ctl' = { ...ctl, epoch: churnEpoch, prev_epoch: ctl.epoch,
               prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
      defects' = { ...defects, churnEpochBump: defects.churnEpochBump or churnEpoch != ctl.epoch },
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepCoverage,
    }
  }

  action markNodeFailed(n: NodeId): bool = all {
    nodes.get(n).member,
    not(nodes.get(n).fail),
    churnFlag(n, true),
  }

  action markNodeRecovered(n: NodeId): bool = all {
    nodes.get(n).fail,
    churnFlag(n, false),
  }

  // TR-CLUSTER-024/025's arming step: the fence is single-shot and is consumed by the
  // next failover apply.
  action armFailoverFence(n: NodeId): bool = all {
    canArmFailoverFence(nodes, slots, n),
    barriers' = { ...barriers, failover_fence: SLOTS.filter(s => slots.get(s) == Some(n)) },
    keepGraph, keepMigAll, keepHeld, keepFeed, keepAux, keepCtl,
  }

  // TR-CLUSTER-018: the planned path. The fence carries the observed epoch/role of the
  // old primary; the belt requires an in-shard, **synced** candidate.
  action failoverGraceful(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val v = applyFailoverCommon(currentView, old, succ)
    all {
      canApplyFailover(nodes, slots, old, succ, true, obsEpoch, obsRole),
      // Issue 26, ruled: armed over **every slot about to move**, not merely armed.
      barrierCoversMovedSlots(slots, barriers.failover_fence, old),
      applyView(v),
      keepShadow, keepHistory, keepRefusals, keepSpent,
      // Both ghosts are recomputed from the *outcome*, not from the guards above.
      defects' = { ...defects,
        demotion: defects.demotion or not(demotedTo(v.nodes, old, succ)),
        fence: defects.fence
          or not(fenceOk(nodes.get(old), nodes.get(old).epoch_own, obsEpoch, obsRole)),
        barrier: defects.barrier
          or not(movedSlotsOf(slots, old).subseteq(barriers.failover_fence)),
        replicaSlot: defects.replicaSlot
          or replicaOwnsSlot(v.nodes, v.slots, old) or replicaOwnsSlot(v.nodes, v.slots, succ) },
      coverage' = { ...coverage,
        gracefulPruned: coverage.gracefulPruned or slotsNaming(migrations, old) != Set(),
        failoverDuringOpenMigration:
          coverage.failoverDuringOpenMigration or migrationSourcesOf(migrations).contains(old) },
    }
  }

  // TR-CLUSTER-019: the automatic path — same postcondition, same per-object fence, but
  // driven by the FAIL flag rather than by an operator.
  action failoverAuto(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val v = applyFailoverCommon(currentView, old, succ)
    all {
      nodes.get(old).fail,
      canApplyFailover(nodes, slots, old, succ, true, obsEpoch, obsRole),
      applyView(v),
      keepShadow, keepHistory, keepRefusals, keepSpent,
      defects' = { ...defects,
        demotion: defects.demotion or not(demotedTo(v.nodes, old, succ)),
        fence: defects.fence
          or not(fenceOk(nodes.get(old), nodes.get(old).epoch_own, obsEpoch, obsRole)),
        replicaSlot: defects.replicaSlot
          or replicaOwnsSlot(v.nodes, v.slots, old) or replicaOwnsSlot(v.nodes, v.slots, succ) },
      coverage' = { ...coverage,
        failoverDuringOpenMigration:
          coverage.failoverDuringOpenMigration or migrationSourcesOf(migrations).contains(old) },
    }
  }

  // TR-CLUSTER-042: the forced path. The per-object fence **survives** (it is the
  // ownership fence, not a health gate) and so does the belt — the candidate must still
  // be an in-shard replica — but the sync gate does not. That is the deviation the
  // header documents, and `forcedDemotedReplica` is its coverage ghost.
  action failoverForced(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val v = applyFailoverCommon(currentView, old, succ)
    all {
      canApplyFailover(nodes, slots, old, succ, false, obsEpoch, obsRole),
      applyView(v),
      keepShadow, keepHistory, keepRefusals, keepSpent,
      defects' = { ...defects,
        demotion: defects.demotion or not(demotedTo(v.nodes, old, succ)),
        fence: defects.fence
          or not(fenceOk(nodes.get(old), nodes.get(old).epoch_own, obsEpoch, obsRole)),
        replicaSlot: defects.replicaSlot
          or replicaOwnsSlot(v.nodes, v.slots, old) or replicaOwnsSlot(v.nodes, v.slots, succ) },
      coverage' = { ...coverage,
        forcedDemotedReplica: coverage.forcedDemotedReplica or not(nodes.get(succ).synced),
        failoverDuringOpenMigration:
          coverage.failoverDuringOpenMigration or migrationSourcesOf(migrations).contains(old) },
    }
  }

  // ext-12 (V6-C3): the administrative demotion with a successor — the same
  // postcondition as a failover, including the residue re-target, but admitted on the
  // shard relationship rather than on a fence.
  action demoteNode(n: NodeId, succ: NodeId): bool = {
    val v = applyFailoverCommon(currentView, n, succ)
    all {
      canDemoteNode(nodes, n, succ),
      applyView(v),
      keepShadow, keepHistory, keepRefusals, keepSpent, keepCoverage,
      defects' = { ...defects,
        demotion: defects.demotion or not(demotedTo(v.nodes, n, succ)),
        // ext-12 (V8-C6): the demoted node must not still own slots afterwards, and the
        // successor it handed them to must be a Primary. Recomputed on the post-state
        // view, scoped to the two nodes this step wrote.
        replicaSlot: defects.replicaSlot
          or replicaOwnsSlot(v.nodes, v.slots, n) or replicaOwnsSlot(v.nodes, v.slots, succ),
        // M32 (V9-C1), ruled 2026-08-19: the successor's *shard relationship* is the one
        // admission conjunct with no post-state trace — `applyFailoverCommon` makes the
        // successor a Primary and re-parents the demoted node onto it, so the post-state
        // looks identical whether or not the successor was in the demoted node's shard.
        // Recomputed here from the **pre-state** `nodes`, independently of the guard, so
        // dropping `isInShardReplicaOf` from `canDemoteNode` surfaces as a violation
        // instead of as an unobservable widening. The *semantic* observable (the
        // successor actually holds the shard's data) is phase-3 replication-model
        // territory and is deliberately not claimed here.
        crossShardSuccessor: defects.crossShardSuccessor or not(isInShardReplicaOf(nodes, succ, n)) },
    }
  }

  // ext-12 (V7-m7/V8-C6): the successor-less demotion — no promotion in the step, and a
  // **whole refusal** while the node still owns slots.
  action demoteNodeExternal(n: NodeId): bool = {
    // R1 (issue 41): the same dependants-follow rule, with no successor to follow to —
    // the demoted node's replicas are detached rather than left chained off a Replica.
    val detached = repointDependants(nodes, n, None)
    val post = detached.set(n, writeParentPointer(detached.get(n), Replica, None, false, None, None))
    all {
      canDemoteNodeExternal(nodes, slots, n),
      // Residue entries naming the demoted node keep pointing at it and are recovered
      // level-triggered by `retargetSlotResidue` — there is no successor to re-home to.
      nodes' = post,
      // ext-12 (V8-C6): there is no successor here, so the whole refusal on a
      // slot-owning node is the *only* thing keeping the demoted node off the slot map.
      // Reverting it lands a slot on a Replica, and this ghost sees it.
      defects' = { ...defects, replicaSlot: defects.replicaSlot or replicaOwnsSlot(post, slots, n) },
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepCoverage, keepCtl,
    }
  }

  // ---------------------------------------------------------------------------
  // ext-5: restarts and run re-minting.
  // ---------------------------------------------------------------------------

  // The volatile half of a restart: a fresh run, the boot report owed again, the escape
  // flags cleared, held writes gone. `record`, `keys`, `stage_counter` and the replicated
  // identity survive **field-for-field** (ext-16/V14-M1).
  pure def restarted(ns: NodeState, newRun: RunId): NodeState = {
    ...ns, run_id: newRun, booted: false, cannot_apply: false, cannot_drain: false, silent: false,
    // ext-15: a boot **re-derives the node's role from its own persisted state** — which
    // is exactly the `local_role`/`local_parent` pair carried through here untouched, not
    // the replicated cell. Papering the split over at boot (re-deriving the local plane
    // from `role`/`parent` instead) would make `witnessSplitPlanesConverge` unreachable
    // and would hide V12-M2's mute node rather than fix it.
    // ext-6/V5-M4: an install without its completion marker is discarded at boot.
    replica_copy: if (ns.install_incomplete) 0 else ns.replica_copy,
    install_incomplete: false,
  }

  // The physical restart, and **only** that: the replicated migration record is
  // deliberately untouched here. "A source restart aborts the migration" is a *replicated*
  // rule in the design, and its trigger is the boot report's run-identity field write
  // (`reportRunIdentity`), not the process death — which is also why the window between
  // the two (physically restarted, boot report not yet applied) is a declared, reachable
  // state rather than one this action papers over. The node's own volatile hold region
  // goes with the process.
  action sourceRestart(n: NodeId): bool = all {
    nodes.get(n).member,
    nodes' = nodes.set(n, restarted(nodes.get(n), ctl.next_run)),
    held' = held.set(n, Set()),
    ctl' = { ...ctl, next_run: ctl.next_run + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
    keepSlots, keepMigAll, keepBarriers, keepFeed, keepAux,
  }

  // The target's restart additionally rolls its ingest back to the durable point — the
  // reason a report may only propose a durable position.
  action targetRestart(n: NodeId): bool = all {
    nodes.get(n).member,
    nodes' = nodes.set(n, restarted(nodes.get(n), ctl.next_run)),
    migrations' = SLOTS.fold(migrations, (acc, s) => match migrations.get(s) {
      | Some(m) => if (m.target == n) acc.set(s, Some({ ...m, target_applied: m.target_durable })) else acc
      | None => acc
    }),
    held' = held.set(n, Set()),
    ctl' = { ...ctl, next_run: ctl.next_run + 1, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes) },
    keepSlots, keepShadow, keepResidue, keepBarriers, keepFeed, keepAux,
  }

  // ext-5: a migration source that fails over mid-migration — the promotion
  // postcondition *and* a fresh run at the successor, so a position minted before the
  // failover and one minted after are distinguishable.
  //
  // The migration is **pruned**, by the same shared postcondition every other failover
  // path uses (TR-CLUSTER-018/FM-CLUSTER-036, release events paid per FM-CLUSTER-087).
  // A successor inherits the slot; it never inherits the migration and never inherits the
  // barrier — FM-CLUSTER-104 stays same-node-only. Re-migrating is a fresh `Begin`.
  action sourceFailover(old: NodeId, succ: NodeId): bool = {
    val v = applyFailoverCommon(currentView, old, succ)
    all {
      beltOk(nodes, slots, old, succ, true),
      migrationSourcesOf(migrations).contains(old),
      nodes' = v.nodes.set(succ, { ...v.nodes.get(succ), run_id: ctl.next_run }),
      slots' = v.slots,
      migrations' = v.migrations,
      residue' = v.residue,
      barriers' = { armed: v.armed, failover_fence: Set(), disconnected: v.disconnected },
      feed_bytes' = v.feed_bytes,
      held' = v.held,
      ctl' = { ...ctl, epoch: v.epoch, prev_epoch: ctl.epoch, prev_parents: parentsOf(nodes), prev_keys: keysOf(nodes), next_run: ctl.next_run + 1 },
      keepShadow, keepHistory, keepRefusals, keepSpent,
      defects' = { ...defects, demotion: defects.demotion or not(demotedTo(v.nodes, old, succ)) },
      coverage' = { ...coverage, failoverDuringOpenMigration: true },
    }
  }

  // ---------------------------------------------------------------------------
  // ext-11/ext-13/ext-14 escape flags and knobs.
  // ---------------------------------------------------------------------------

  action setSourceCannotApply(n: NodeId, v: bool): bool = all {
    nodes.get(n).cannot_apply != v,
    nodesOnly(nodes.set(n, { ...nodes.get(n), cannot_apply: v })),
  }

  action setSourceCannotDrain(n: NodeId, v: bool): bool = all {
    nodes.get(n).cannot_drain != v,
    nodesOnly(nodes.set(n, { ...nodes.get(n), cannot_drain: v })),
  }

  action setTargetSilent(n: NodeId, v: bool): bool = all {
    nodes.get(n).silent != v,
    nodesOnly(nodes.set(n, { ...nodes.get(n), silent: v })),
  }

  // ext-14 (V6-C2): there is deliberately **no** action that flips a global
  // `count_replicas` knob. The durability requirement is a captured parameter on the
  // migration record (`Migration.require_replica_ack`), stamped once at Begin from the
  // proposer's config. A mutable global consulted at admission time is the anti-pattern
  // the design forbids: one applier would admit what another refuses (Raft divergence),
  // and a mid-migration flip would silently void the conjunct the operator armed.

  action stutter = all { keepGraph, keepMigAll, keepBar, keepAux, keepCtl }

  // ---------------------------------------------------------------------------
  // The two step relations. Both are nondeterministic choices over every action, with the
  // candidate sets biased toward currently-interesting values (see the logic module's
  // note: biasing changes only what the random walk is likely to try, never what is
  // legal). They differ only in the *sampling distribution*:
  //
  //   - `step` — the flat arm list, the default the whole toolchain uses (`quint run`'s
  //     own default, `just quint-run`, the witness-floor gate, `quint verify`). Its budget
  //     is the PR lane's, and it is deterministic-green there.
  //   - `stepSteered` — the steered walk, R11 (issue 41, ruled 2026-08-20): an **opt-in**
  //     lane (`just quint-run-steered`, `quint run --step stepSteered`) that batteries,
  //     campaigns and the nightly CI job run at a deep budget. It reaches protocol states
  //     the flat walk samples in ~0% of traces — that is how issue 41's four-invariant
  //     family was found — at the cost of a run time the PR lane cannot carry.
  //
  // Neither relation is a subset of the other's *behaviour*: `stepSteered` offers the flat
  // arm list as one of its groups, so nothing it can do is unreachable from `step` and
  // vice versa (one deliberate widening, noted below).
  //
  // Walk steering (issue 03 [t2 N2], measured 2026-08-19). Everything in `stepSteered`
  // changes the *distribution* the simulator samples, never the transition relation: the
  // flat arm list at the bottom of it is unchanged and still offers every action over every
  // candidate domain it had, so every behaviour reachable before is reachable now. Two
  // mechanisms, both of them redrawn on every step:
  //
  //   1. A 1-in-4 coin (`churnCoin`) gates the two `removeNode` arms. `removeNode` was
  //      selected in ~39.8% of sampled steps and emptied the cluster before anything
  //      interesting happened; the coin makes it four times rarer without making any
  //      removal unreachable (`churnCoin == 0` is drawn on a quarter of all steps).
  //   2. `stepSteered`'s top level is an `any` over three *groups* rather than over ~65 flat
  //      arms. The simulator picks uniformly among the enabled arms of an `any`, so a
  //      group carries ~1/3 of the walk's weight whenever it has an enabled arm, instead
  //      of the (arms in group)/65 share the flat list gave it. The two focused groups
  //      are the commit chain (Begin -> Prepare -> Confirm -> Complete) and, nested one
  //      level deeper so its small sub-families are not outvoted, the support around it:
  //      ingest/observation, the counted-replica trio, and the record/staged-flip/
  //      adoption family — the deep protocol states the flat walk reached in ~0% of
  //      traces. The third group is the flat arm list itself, so no action loses its
  //      unbiased instance.
  //      Every focused arm is an action the flat list also offers; it differs only in
  //      *which* candidate value it is handed — the slot's own owner, the slot carrying
  //      a live record, that record's own attempt stamp and target, a live-primary
  //      target, a node that can still be counted against it — the values that keep the
  //      chain moving rather than an independent uniform pick that almost never matches.
  //      The flat arms keep the unbiased draws, so the stale-stamp and wrong-endpoint
  //      refusal behaviours they exercise are untouched.
  //
  // One deliberate *widening* comes with (2): `beginSlotCands` is not narrowed by an
  // open record elsewhere, so the focused Begin can open a second, concurrent migration
  // on another slot — a state the flat arm alone could not reach, because `slotCands`
  // collapses to the slots that already carry one. Steering may not take behaviour away;
  // handing some back is fine, and the full invariant set holds over it (2000 x 40).
  //
  // Measured over 2000 samples x 40 steps, seed 12345, before -> after: the commit
  // family goes from 0 traces to `witnessResiduePending` 131, `witnessKnobOnMigration-
  // Completes` 50, `witnessCompleteEnabledOnQuietSlot` 280, `witnessBeginRefusedOver-
  // Residue` 131, `witnessDrainingHeldForKTicks` 14, `witnessHealthyDrainAbortedByBound`
  // 1; `witnessNodeRemoved` drops 1984 -> 789, and no witness that was reached before
  // is reached in 0 traces after. The only two still at 0 are the pair this model
  // declares unreachable by design.
  // ---------------------------------------------------------------------------

  val nodeCands: Set[NodeId] = {
    val c = migrationSourcesOf(migrations).union(fencedOwners(barriers.failover_fence, slots))
    if (c == Set()) NODES else c
  }

  val slotCands: Set[SlotId] = {
    val c = openMigrationSlots(migrations, shadow, residue)
    if (c == Set()) SLOTS else c
  }

  // `oneOf` needs a non-empty set; the placeholder never satisfies `observeRefusal`'s
  // membership conjunct, so it is a no-op arm rather than an extra behaviour.
  val refusalCands: Set[Refusal] =
    if (refusals == Set()) Set({ node: 0, stage_id: 0, reg_seq: 0, class: Whole, terminal: false })
    else refusals

  // --- Steering candidates ---------------------------------------------------
  //
  // Each of these answers "which value would let the next step of the protocol fire?".
  // They feed the focused groups only; every one of them falls back to the full domain
  // so `oneOf` always has a non-empty set and the focused arm degrades into an ordinary
  // (possibly disabled) instance of the same action rather than into undefined behaviour.

  // Slots a `Begin` could open right now: owned, with neither a live record nor a
  // residue entry. `slotCands` deliberately points the other way — at slots that already
  // carry one — so the flat `beginMigration` arm can only propose over an *open* slot
  // once any migration exists.
  val beginSlotCands: Set[SlotId] = {
    val c = SLOTS.filter(s => and {
      slots.get(s) != None,
      migrations.get(s) == None,
      residue.get(s) == None,
    })
    if (c == Set()) SLOTS else c
  }

  // `canBeginMigration` requires the source to *be* the slot's owner.
  def ownerCands(s: SlotId): Set[NodeId] =
    match slots.get(s) {
      | Some(o) => Set(o)
      | None => NODES
    }

  // `canCompleteMigration` requires the target to be a live primary, and
  // `canBeginMigration` requires it to differ from the source: a target drawn uniformly
  // is the wrong value ~3 times in 4 and wedges the chain short of its commit.
  def primaryTargetCands(s: SlotId): Set[NodeId] = {
    val c = NODES.filter(n => and {
      isLivePrimary(nodes, n),
      canAct(nodes, n),
      slots.get(s) != Some(n),
    })
    if (c == Set()) NODES else c
  }

  // The attempt stamp the slot's own record is carrying — the one value `Confirm` and
  // `Complete` admit. The flat arms keep drawing from the stale/forged set.
  def liveAttemptOf(s: SlotId): int =
    match migrations.get(s) {
      | Some(m) => m.attempt
      | None => ctl.handoff_seq
    }

  // The open record's target — the node `attachTargetReplica`/`reportReplicaAck` have to
  // name for `Complete`'s counted-replica conjunct (ext-14) to be satisfiable.
  def migTargetCands(s: SlotId): Set[NodeId] =
    match migrations.get(s) {
      | Some(m) => Set(m.target)
      | None => NODES
    }

  // Nodes `attachTargetReplica` could newly count against target `t`, and nodes already
  // counted against it (what `feedTargetReplica`/`reportReplicaAck` need). Uniform `n2`
  // names the right node about a quarter of the time, which is what left
  // `witnessKnobOnMigrationCompletes` — a `require_replica_ack` migration that actually
  // commits — at the edge of visibility in a 200-trace run.
  def attachableCands(t: NodeId): Set[NodeId] = {
    val c = NODES.filter(n => and { n != t, nodes.get(n).member, not(nodes.get(n).counted) })
    if (c == Set()) NODES else c
  }

  def countedOfCands(t: NodeId): Set[NodeId] = {
    val c = countedReplicasOf(nodes, t)
    if (c == Set()) NODES else c
  }

  // Slots carrying a *live record*, which is narrower than `slotCands` (it also holds
  // shadow- and residue-only slots, where every lifecycle arm is a no-op). The focused
  // groups draw their slot here so a committed slot's leftover residue does not halve
  // the odds that the next chain step is proposed over the slot that can take it.
  val recordSlotCands: Set[SlotId] = {
    val c = SLOTS.filter(s => migrations.get(s) != None)
    if (c == Set()) slotCands else c
  }

  action stepSteered = {
    nondet n1 = nodeCands.oneOf()
    nondet n2 = NODES.oneOf()
    nondet s = slotCands.oneOf()
    nondet r = refusalCands.oneOf()
    nondet flag = Bool.oneOf()
    nondet kind = Set(Boot, Demotion).oneOf()
    nondet role = Set(Primary, Replica).oneOf()
    // The candidate's *observed* epoch and role: mostly the truth, sometimes stale — the
    // fence conjuncts only matter when a stale observation is reachable.
    nondet obsEpoch = Set(nodes.get(n1).epoch_own, nodes.get(n1).epoch_own - 1, ctl.epoch).oneOf()
    nondet obsRole = Set(Primary, Replica).oneOf()
    nondet obsSeq = Set(nodes.get(n2).parent_seq, nodes.get(n2).parent_seq - 1).oneOf()
    nondet obsReg = Set(0, 1, 2, 3, 4, 5, 6).oneOf()
    // ext-3: the attempt stamp a proposal carries — the live one, or a stale/forged one.
    nondet att = Set(ctl.handoff_seq, ctl.handoff_seq - 1, 0).oneOf()
    // Steering draws (see the header above). `bs`/`bsrc`/`btgt` are the Begin triple,
    // `ms` the slot carrying a live record and `rep` that record's target; `churnCoin`
    // is the `removeNode` gate.
    nondet bs = beginSlotCands.oneOf()
    nondet bsrc = ownerCands(bs).oneOf()
    nondet btgt = primaryTargetCands(bs).oneOf()
    nondet ms = recordSlotCands.oneOf()
    nondet rep = migTargetCands(ms).oneOf()
    nondet rAttach = attachableCands(rep).oneOf()
    nondet rCounted = countedOfCands(rep).oneOf()
    nondet churnCoin = Set(0, 1, 2, 3).oneOf()
    any {
      // --- focused group 1: the commit chain, ~1/3 of the walk's weight ---------
      any {
        beginMigration(bs, bsrc, btgt, flag),
        prepareHandoff(ms),
        confirmDrained(ms, liveAttemptOf(ms)),
        completeMigration(ms, liveAttemptOf(ms)),
      },
      // --- focused group 2: what the chain needs around it, ~1/3 ----------------
      // Three sub-groups so the small ones are not outvoted by the large one: the
      // counted-replica trio is three actions' worth of setup that `Complete`'s ext-14
      // durability conjunct needs (a *non-empty* counted set, V5-M3), and the record
      // family is the staged-flip/adoption region the flat walk also starved.
      any {
        // ingest and observation, on the slot carrying a live record
        any {
          sourceWrite(ms),
          applyAtTarget(ms),
          makeTargetDurable(ms),
          reportTargetIngest(ms),
          coverageBatch(ms),
          reconcileTick(ms),
          boundAbort(ms),
        },
        // the counted replicas of that record's target
        any {
          attachTargetReplica(rAttach, rep),
          feedTargetReplica(rCounted),
          reportReplicaAck(ms, rCounted),
        },
        // the record / staged-flip / adoption family
        any {
          stageFlip(n1, n2),
          observeRefusal(n1, r),
          completeAdoption(n1),
          adoptReplicatedRole(n1),
          reconcileIdentity(n1),
          clearStaleRecord(n1),
        },
      },
      // --- the unbiased arm list, unchanged, ~1/3 ------------------------------
      any {
        // membership / identity
        meetNode(n1),
        all { churnCoin == 0, removeNode(n1, flag, false) },
        all { churnCoin == 0, removeNode(n1, flag, true) },
        resetCluster(n1),
        setRole(n1, role, if (role == Primary) None else Some(n2)),
        reportRunIdentity(n1, kind, obsRole, obsEpoch, flag),
        loseIncarnationCounter(n1),
        attestReplicaSynced(n2, n1, obsSeq, obsReg),
        // ext-15 two-plane convergence
        adoptReplicatedRole(n1),
        reconcileIdentity(n1),
        // ext-18 record lifecycle
        stageFlip(n1, n2),
        observeRefusal(n1, r),
        clearStaleRecord(n1),
        completeAdoption(n1),
        // slot map
        assignSlot(s, n1),
        removeSlot(s),
        // migration lifecycle
        beginMigration(s, n1, n2, flag),
        prepareHandoff(s),
        feedBuffer(s),
        disconnectFeed(s),
        localReleaseOnCapBreach(s),
        selfFenceRelease(s),
        sourceWrite(s),
        applyAtTarget(s),
        makeTargetDurable(s),
        reportTargetIngest(s),
        confirmDrained(s, att),
        coverageBatch(s),
        batchFault(s),
        reconcileTick(s),
        boundAbort(s),
        completeMigration(s, att),
        abortHandoff(s, att),
        cancelMigration(s, att),
        // shadow / promotion / residue
        relabelShadow(s),
        reportPromoted(s),
        failPromotion(s),
        discardShadow(s),
        reapSlots(n1),
        clearSlotResidue(s),
        retargetSlotResidue(s),
        rehomeOrphanSlot(s, n2),
        // replicas
        attachTargetReplica(n2, n1),
        detachTargetReplica(n2),
        feedTargetReplica(n2),
        reportReplicaAck(s, n2),
        crashDuringShadowSection(n2),
        // roles, failover, restarts
        markNodeFailed(n1),
        markNodeRecovered(n1),
        armFailoverFence(n1),
        failoverGraceful(n1, n2, obsEpoch, obsRole),
        failoverAuto(n1, n2, obsEpoch, obsRole),
        failoverForced(n1, n2, obsEpoch, obsRole),
        sourceFailover(n1, n2),
        demoteNode(n1, n2),
        demoteNodeExternal(n1),
        sourceRestart(n1),
        targetRestart(n2),
        // knobs
        setSourceCannotApply(n1, flag),
        setSourceCannotDrain(n1, flag),
        setTargetSilent(n2, flag),
        stutter,
      },
    }
  }

  action step = {
    nondet n1 = nodeCands.oneOf()
    nondet n2 = NODES.oneOf()
    nondet s = slotCands.oneOf()
    nondet r = refusalCands.oneOf()
    nondet flag = Bool.oneOf()
    nondet kind = Set(Boot, Demotion).oneOf()
    nondet role = Set(Primary, Replica).oneOf()
    // The candidate's *observed* epoch and role: mostly the truth, sometimes stale — the
    // fence conjuncts only matter when a stale observation is reachable.
    nondet obsEpoch = Set(nodes.get(n1).epoch_own, nodes.get(n1).epoch_own - 1, ctl.epoch).oneOf()
    nondet obsRole = Set(Primary, Replica).oneOf()
    nondet obsSeq = Set(nodes.get(n2).parent_seq, nodes.get(n2).parent_seq - 1).oneOf()
    nondet obsReg = Set(0, 1, 2, 3, 4, 5, 6).oneOf()
    // ext-3: the attempt stamp a proposal carries — the live one, or a stale/forged one.
    nondet att = Set(ctl.handoff_seq, ctl.handoff_seq - 1, 0).oneOf()
    any {
      // membership / identity
      meetNode(n1),
      removeNode(n1, flag, false),
      removeNode(n1, flag, true),
      resetCluster(n1),
      setRole(n1, role, if (role == Primary) None else Some(n2)),
      reportRunIdentity(n1, kind, obsRole, obsEpoch, flag),
      loseIncarnationCounter(n1),
      attestReplicaSynced(n2, n1, obsSeq, obsReg),
      // ext-15 two-plane convergence
      adoptReplicatedRole(n1),
      reconcileIdentity(n1),
      // ext-18 record lifecycle
      stageFlip(n1, n2),
      observeRefusal(n1, r),
      clearStaleRecord(n1),
      completeAdoption(n1),
      // slot map
      assignSlot(s, n1),
      removeSlot(s),
      // migration lifecycle
      beginMigration(s, n1, n2, flag),
      prepareHandoff(s),
      feedBuffer(s),
      disconnectFeed(s),
      localReleaseOnCapBreach(s),
      selfFenceRelease(s),
      sourceWrite(s),
      applyAtTarget(s),
      makeTargetDurable(s),
      reportTargetIngest(s),
      confirmDrained(s, att),
      coverageBatch(s),
      batchFault(s),
      reconcileTick(s),
      boundAbort(s),
      completeMigration(s, att),
      abortHandoff(s, att),
      cancelMigration(s, att),
      // shadow / promotion / residue
      relabelShadow(s),
      reportPromoted(s),
      failPromotion(s),
      discardShadow(s),
      reapSlots(n1),
      clearSlotResidue(s),
      retargetSlotResidue(s),
      rehomeOrphanSlot(s, n2),
      // replicas
      attachTargetReplica(n2, n1),
      detachTargetReplica(n2),
      feedTargetReplica(n2),
      reportReplicaAck(s, n2),
      crashDuringShadowSection(n2),
      // roles, failover, restarts
      markNodeFailed(n1),
      markNodeRecovered(n1),
      armFailoverFence(n1),
      failoverGraceful(n1, n2, obsEpoch, obsRole),
      failoverAuto(n1, n2, obsEpoch, obsRole),
      failoverForced(n1, n2, obsEpoch, obsRole),
      sourceFailover(n1, n2),
      demoteNode(n1, n2),
      demoteNodeExternal(n1),
      sourceRestart(n1),
      targetRestart(n2),
      // knobs
      setSourceCannotApply(n1, flag),
      setSourceCannotDrain(n1, flag),
      setTargetSilent(n2, flag),
      stutter,
    }
  }
}
