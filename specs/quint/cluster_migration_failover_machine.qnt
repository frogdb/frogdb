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
//   Reaper and residue arms      — reapSlots, clearSlotResidue, retargetSlotResidue
//   Roles, failover, demotion    — markNodeFailed, markNodeRecovered, armFailoverFence,
//                                  failoverGraceful, failoverAuto, failoverForced,
//                                  demoteNode, demoteNodeExternal
//   Restarts and run re-minting  — sourceRestart, targetRestart, sourceFailover
//   Escape flags and knobs       — setSourceCannotApply, setSourceCannotDrain,
//                                  setTargetSilent
//   step
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
  }
  // Ghost flags: set when the *unsafe* variant of a ruled precondition would have been
  // taken. Never read by a guard; only by invariants/witnesses.
  var defects: { demotion: bool, fence: bool, barrier: bool, admission: bool, seqReuse: bool }
  // Coverage ghosts: set when a behaviour the witnesses care about actually happened.
  var coverage: {
    gracefulPruned: bool,
    forcedDemotedReplica: bool,
    failoverDuringOpenMigration: bool,
    promotionRolledBack: bool,
    reaped: bool,
    emptyBatch: bool,
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
  action keepCtl = ctl' = { ...ctl, prev_epoch: ctl.epoch }
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
             next_reg: 5, next_run: 5 },
    defects' = { demotion: false, fence: false, barrier: false, admission: false, seqReuse: false },
    coverage' = { gracefulPruned: false, forcedDemotedReplica: false, failoverDuringOpenMigration: false,
                  promotionRolledBack: false, reaped: false, emptyBatch: false },
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
    ctl' = { ...ctl, epoch: v.epoch, prev_epoch: ctl.epoch },
  }

  // ---------------------------------------------------------------------------
  // Membership and identity.
  // ---------------------------------------------------------------------------

  // ext-10 (V8-C1): a joiner whose main keyspace is non-empty is refused — the gate that
  // makes `inv_member_keyspace_is_tracked` hold once the self-cleanse is gone. The fresh
  // registration mints a new `registration_seq` (ext-17/V18-C1), which is what strands
  // any record staged under the previous one.
  action meetNode(n: NodeId): bool = all {
    canMeetNode(nodes, n),
    nodes' = nodes.set(n, {
      ...writeParentPointer(nodes.get(n), Primary, None, false, None, None),
      member: true, fail: false, booted: false,
      registration_seq: Some(ctl.next_reg),
    }),
    ctl' = { ...ctl, next_reg: ctl.next_reg + 1, prev_epoch: ctl.epoch },
    keepSlots, keepMigAll, keepBar, keepAux,
  }

  // TR-CLUSTER-003 + ext-12's mark-don't-remove + ext-17's detach. `wipe` additionally
  // drops the node-local *stage counter* (a store loss); the record itself is node-local
  // and crash-durable, so it deliberately survives (ext-18), which is what makes the
  // cross-registration stage-id collision expressible.
  action removeNode(n: NodeId, force: bool, wipe: bool): bool = {
    val prunedSet = slotsNaming(migrations, n)
    val detached = NODES.fold(nodes, (acc, m) =>
      if (m != n and acc.get(m).parent == Some(n))
        acc.set(m, writeParentPointer(acc.get(m), acc.get(m).role, None, false, None, None))
      else acc)
    all {
      canRemoveNode(nodes, slots, n, force),
      nodes' = detached.set(n, {
        ...detached.get(n), member: false, registration_seq: None,
        booted: false, counted: false,
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
    val cleared = NODES.fold(nodes, (acc, m) =>
      if (m == n) acc
      else acc.set(m, { ...writeParentPointer(acc.get(m), acc.get(m).role, None, false, None, None),
                        member: false, registration_seq: None, booted: false, counted: false }))
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
      ctl' = { ...ctl, handoff_seq: 0, reset_epoch: ctl.reset_epoch + 1, prev_epoch: ctl.epoch },
      keepAux,
    }
  }

  // TR-CLUSTER-004.
  action setRole(n: NodeId, role: RoleTag, parent: Option[NodeId]): bool = all {
    canSetRole(nodes, n, role, parent),
    nodesOnly(nodes.set(n, writeParentPointer(nodes.get(n), role, parent, false, None, None))),
  }

  // ext-15: the kind-stamped identity report. The Boot arm lifts the boot-ordering
  // guard; the Demotion arm is the one that *writes role and parent* (V7-C1) and is
  // fenced on the observed role/epoch (V8-C2). A refusal mints a `Refusal` carrying the
  // record pair, classified by the first failing conjunct in the declared order.
  action reportRunIdentity(n: NodeId, kind: TransitionKind, obsRole: RoleTag, obsEpoch: int): bool = {
    val ns = nodes.get(n)
    val inc = ns.incarnation
    val seq = ns.identity_seq + 1
    val cls = refusalClassOf(nodes, n, kind, inc, seq, obsRole, obsEpoch)
    val admitted = cls == Whole
    val stamped = { ...ns, identity_seq: seq,
                    stored_identity: Some({ inc: inc, seq: seq, run_id: ns.run_id }) }
    all {
      // A Demotion report only exists against a staged record.
      kind == Boot or (match ns.record { | Some(rec) => rec.kind == Demotion | None => false }),
      nodes' =
        if (not(admitted)) nodes
        else if (kind == Boot) nodes.set(n, { ...stamped, booted: true })
        else match ns.record {
          | Some(rec) => nodes.set(n, {
              ...writeParentPointer(stamped, Replica, Some(rec.upstream), false, Some(rec.stage_id), None),
              record: Some({ ...rec, adopted: true }),
            })
          | None => nodes
        },
      refusals' =
        if (admitted) refusals
        else match ns.record {
          | Some(rec) => refusals.union(Set({
              node: n, stage_id: rec.stage_id, reg_seq: rec.staged_reg_seq,
              class: cls, terminal: isRefusalTerminal(ns.stored_identity, cls) }))
          | None => refusals
        },
      keepSlots, keepMigAll, keepBar, keepHistory, keepSpent, keepDefects, keepCoverage, keepCtl,
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
  // ext-18: the pending-transition record.
  // ---------------------------------------------------------------------------

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
      nodesOnly(nodes.set(n, {
        ...ns, stage_counter: ns.stage_counter + 1, node_fenced: true, reply_emitted: false,
        record: Some({ kind: Demotion, upstream: upstream, stage_id: ns.stage_counter + 1,
                       staged_reg_seq: rs, adopted: false }),
      })),
    }
  }

  // ext-18 (V28-M6): observation and disposition are the *same* step, and the record is
  // only disposed of when both halves of the (stage_id, registration_seq) pair bind.
  // `last_disposition` is the auxiliary history the foreign-disposal invariant reads.
  action observeRefusal(n: NodeId, r: Refusal): bool = {
    val ns = nodes.get(n)
    all {
      refusals.contains(r),
      r.node == n,
      match ns.record {
        | Some(rec) => {
            val disp = Some({ refusal_stage_id: r.stage_id, refusal_reg_seq: r.reg_seq,
                              record_stage_id: rec.stage_id, record_reg_seq: rec.staged_reg_seq })
            nodes' = nodes.set(n,
              if (refusalBinds(rec, r) and r.terminal)
                { ...ns, record: None, node_fenced: false, reply_emitted: true, last_disposition: disp }
              else { ...ns, last_disposition: disp })
          }
        | None => nodes' = nodes
      },
      refusals' = refusals.exclude(Set(r)),
      keepSlots, keepMigAll, keepBar, keepHistory, keepSpent, keepDefects, keepCoverage, keepCtl,
    }
  }

  // ext-18: the level-triggered clearing clause — a record whose staged registration no
  // longer matches the live cell is cleared, the fence dropped and the client paid,
  // without waiting for a reply that will never come.
  action clearStaleRecord(n: NodeId): bool = all {
    isRecordStale(nodes.get(n)),
    nodesOnly(nodes.set(n, { ...nodes.get(n), record: None, node_fenced: false, reply_emitted: true })),
  }

  // ext-18: the staged flip lands only after the replicated admission stamped it.
  action completeAdoption(n: NodeId): bool = {
    val ns = nodes.get(n)
    all {
      match ns.record { | Some(rec) => rec.adopted | None => false },
      nodesOnly(nodes.set(n, { ...ns, record: None, node_fenced: false, reply_emitted: true })),
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
    ctl' = { ...ctl, next_mig: ctl.next_mig + 1, prev_epoch: ctl.epoch },
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
            migrations' = r.migs,
            barriers' = { ...barriers, armed: r.armed },
            held' = held.set(m.source, held.get(m.source).union(Set(s))),
            spent' = spent.union(Set(token)),
            defects' = { ...defects, seqReuse: defects.seqReuse or spent.contains(token) },
            ctl' = { ...ctl, handoff_seq: newSeq, prev_epoch: ctl.epoch },
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
  action attachTargetReplica(n: NodeId, t: NodeId): bool = all {
    n != t,
    nodes.get(n).member,
    isLivePrimary(nodes, t),
    not(nodes.get(n).counted),
    nodesOnly(nodes.set(n, { ...writeParentPointer(nodes.get(n), Replica, Some(t), false, None, None),
                            counted: true, replica_copy: 0, install_incomplete: false })),
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
          all {
            canConfirmDrained(nodes, migrations, s, att, p),
            isServingPrimary(nodes, slots, m, s),
            migrationsOnly(applyConfirmDrained(migrations, s, p)),
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
      | Some(m) => all {
          migrationsOnly(migrations.set(s, Some(applyReconcileTick(m)))),
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
          keepGraph, keepShadow, keepResidue, keepAux, keepCtl,
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
          defects' = { ...defects, admission: defects.admission or not(
            match m.drained_pos {
              | Some(d) => and {
                  m.phase == Draining,
                  d.run_id == m.source_log.run_id,
                  m.target_copy.pos >= d.pos,
                }
              | None => false
            }) },
          slots' = slots.set(s, Some(m.target)),
          nodes' = nodes.set(m.target, { ...nodes.get(m.target), epoch_own: ctl.epoch + 1 }),
          migrations' = migrations.set(s, None),
          residue' = residue.set(s, Some({ mig: m.id, source: m.source, target: m.target,
                                           promoted: false, source_gone: false, target_gone: false,
                                           promotion_failures: 0 })),
          keepShadow,
          releaseHoldRegion(s, m.source),
          ctl' = { ...ctl, epoch: ctl.epoch + 1, prev_epoch: ctl.epoch },
          keepHistory, keepRefusals, keepSpent, keepCoverage,
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
          keepGraph, keepShadow, keepResidue, keepAux, keepCtl,
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
          keepGraph, keepShadow, keepResidue, keepAux, keepCtl,
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
          keepSlots, keepMigrations, keepShadow, keepBar, keepAux, keepCtl,
        }
      | None => nope
    }

  // ext-12: the promotion is attempted and fails. Ownership rolls back to the source
  // (which never stopped being authoritative) and the staged copy goes; the **entry
  // stays with `promoted == false`**. Scheduled between `relabelShadow` and
  // `reportPromoted` this is the rollback-wins-the-race trace: the re-label is reversed
  // and nothing was ever served.
  action failPromotion(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => all {
          not(r.promoted),
          slots' = slots.set(s, Some(r.source)),
          shadow' = shadow.set(s, None),
          // ext-12 (ruled Q6): the entry counts its failed attempts. A rollback is not a
          // terminal state — `relabelShadow`/`reportPromoted` may be retried — so without
          // a counter on the entry itself a repeatedly-failing promotion is
          // indistinguishable from one that was never attempted, and the operator arms
          // have nothing to key an escalation on.
          residue' = residue.set(s, Some({ ...r, promotion_failures: r.promotion_failures + 1 })),
          coverage' = { ...coverage, promotionRolledBack: true },
          keepNodes, keepMigrations, keepBar,
          keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
        }
      | None => nope
    }

  // ext-8: §5's stated predicate — no live record and no residue entry.
  action discardShadow(s: SlotId): bool = all {
    canDiscardShadow(migrations, residue, shadow, s),
    shadow' = shadow.set(s, None),
    keepGraph, keepMigrations, keepResidue, keepBar, keepAux, keepCtl,
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
      keepSlots, keepMigAll, keepBar, keepHistory, keepRefusals, keepSpent, keepDefects, keepCtl,
    }
  }

  action clearSlotResidue(s: SlotId): bool = all {
    canClearSlotResidue(residue, s),
    residue' = residue.set(s, None),
    keepGraph, keepMigrations, keepShadow, keepBar, keepAux, keepCtl,
  }

  // ext-12 (V8-C4, source arm only per V10-C4).
  action retargetSlotResidue(s: SlotId): bool =
    match residue.get(s) {
      | Some(r) => match shardPrimary(nodes, r.source) {
          | Some(p) => all {
              canRetargetSlotResidue(nodes, residue, s),
              residue' = residue.set(s, Some({ ...r, source: p, source_gone: false })),
              keepGraph, keepMigrations, keepShadow, keepBar, keepAux, keepCtl,
            }
          | None => nope
        }
      | None => nope
    }

  // ---------------------------------------------------------------------------
  // Roles, failover and demotion.
  // ---------------------------------------------------------------------------

  action markNodeFailed(n: NodeId): bool = all {
    nodes.get(n).member,
    not(nodes.get(n).fail),
    nodesOnly(nodes.set(n, { ...nodes.get(n), fail: true })),
  }

  action markNodeRecovered(n: NodeId): bool = all {
    nodes.get(n).fail,
    nodesOnly(nodes.set(n, { ...nodes.get(n), fail: false })),
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
          or not(movedSlotsOf(slots, old).subseteq(barriers.failover_fence)) },
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
          or not(fenceOk(nodes.get(old), nodes.get(old).epoch_own, obsEpoch, obsRole)) },
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
          or not(fenceOk(nodes.get(old), nodes.get(old).epoch_own, obsEpoch, obsRole)) },
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
      defects' = { ...defects, demotion: defects.demotion or not(demotedTo(v.nodes, n, succ)) },
    }
  }

  // ext-12 (V7-m7/V8-C6): the successor-less demotion — no promotion in the step, and a
  // **whole refusal** while the node still owns slots.
  action demoteNodeExternal(n: NodeId): bool = {
    val detached = NODES.fold(nodes, (acc, m) =>
      if (m != n and acc.get(m).parent == Some(n))
        acc.set(m, writeParentPointer(acc.get(m), acc.get(m).role, None, false, None, None))
      else acc)
    all {
      canDemoteNodeExternal(nodes, slots, n),
      // Residue entries naming the demoted node keep pointing at it and are recovered
      // level-triggered by `retargetSlotResidue` — there is no successor to re-home to.
      nodesOnly(detached.set(n, writeParentPointer(detached.get(n), Replica, None, false, None, None))),
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
    // ext-6/V5-M4: an install without its completion marker is discarded at boot.
    replica_copy: if (ns.install_incomplete) 0 else ns.replica_copy,
    install_incomplete: false,
  }

  // Ruled Q2: the restart breaks the seal on every migration this node sources. The hold
  // is gone (`held`), so the barrier that stood for it is disarmed and the buffered feed
  // reset; the record's `fenced`/`phase`/`drained_pos`/`attempt` are rewound to match.
  // Without this the record kept advertising a hold that no longer existed, and a
  // completion could be admitted on evidence produced by a dead run.
  action sourceRestart(n: NodeId): bool = {
    val touched = slotsSourcedBy(migrations, n)
    all {
      nodes.get(n).member,
      nodes' = nodes.set(n, restarted(nodes.get(n), ctl.next_run)),
      migrations' = unsealedBySource(migrations, n),
      held' = held.set(n, Set()),
      barriers' = { ...barriers, armed: barriers.armed.exclude(touched) },
      feed_bytes' = SLOTS.mapBy(s => if (touched.contains(s)) 0 else feed_bytes.get(s)),
      ctl' = { ...ctl, next_run: ctl.next_run + 1, prev_epoch: ctl.epoch },
      keepSlots, keepShadow, keepResidue, keepAux,
    }
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
    ctl' = { ...ctl, next_run: ctl.next_run + 1, prev_epoch: ctl.epoch },
    keepSlots, keepShadow, keepResidue, keepBarriers, keepFeed, keepAux,
  }

  // ext-5: a migration source that fails over mid-migration — the promotion
  // postcondition *and* a fresh run at the successor, so a position minted before the
  // failover and one minted after are distinguishable.
  //
  // Ruled Q1(b): the open migration **survives**, re-homed onto the successor, instead of
  // being pruned with the old primary. The successor inherits the slot, so the migration
  // is still the cluster's intent; only the old primary's evidence dies with it. This is
  // what makes ext-5's own scenario reachable — a stale position minted before the
  // failover meeting a live record minted after it.
  action sourceFailover(old: NodeId, succ: NodeId): bool = {
    val v = applyFailoverCommon(currentView, old, succ)
    all {
      beltOk(nodes, slots, old, succ, true),
      migrationSourcesOf(migrations).contains(old),
      nodes' = v.nodes.set(succ, { ...v.nodes.get(succ), run_id: ctl.next_run }),
      slots' = v.slots,
      migrations' = rehomedMigrations(migrations, old, succ, ctl.next_run),
      residue' = v.residue,
      barriers' = { armed: v.armed, failover_fence: Set(), disconnected: v.disconnected },
      feed_bytes' = v.feed_bytes,
      held' = v.held,
      ctl' = { ...ctl, epoch: v.epoch, prev_epoch: ctl.epoch, next_run: ctl.next_run + 1 },
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
  // step — nondeterministic choice over every action, with the candidate sets biased
  // toward currently-interesting values (see the logic module's note: biasing changes
  // only what the random walk is likely to try, never what is legal).
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
      reportRunIdentity(n1, kind, obsRole, obsEpoch),
      loseIncarnationCounter(n1),
      attestReplicaSynced(n2, n1, obsSeq, obsReg),
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
