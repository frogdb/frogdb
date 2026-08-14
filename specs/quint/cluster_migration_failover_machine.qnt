// cluster_migration_failover_machine — the state machine of the migration x failover
// composite model (formal-spec phase 2, task 2): the `var` declarations (including the
// ghost/history fields the invariants are made falsifiable by) and one guarded action per
// modeled transition, plus `init` and the `step` relation the simulator samples.
//
// This is one Raft state machine: the actions are the *applied* sequence of committed
// commands, with no network/consensus modeling — exactly the `apply_command` boundary the
// quint-connect harness drives. Actions cover TR-CLUSTER-003/004/008/009/010/011/012/013/
// 014/015/016/017/018/019/024/025/034/042; guards and updates themselves live in
// cluster_migration_failover_logic.qnt, and the model-wide header (rows modeled,
// deliberately-not-modeled, deviations) lives in cluster_migration_failover.qnt.
module cluster_migration_failover_machine {
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"
  import cluster_migration_failover_logic.* from "./cluster_migration_failover_logic"

  var nodes: NodeId -> NodeState
  var slots: SlotId -> Option[NodeId]
  var migrations: SlotId -> Option[Migration]
  // "Handoff attempt counter" — the monotone mint.
  var handoff_seq: int
  // "Cluster config epoch".
  var epoch: int
  // "Node's own config epoch", per node.
  var node_epoch: NodeId -> int
  // "Slot-scoped write barrier" — see the header's TR-CLUSTER-034 note on why this is
  // a Set[SlotId] rather than a (NodeId, SlotId) -> Armed | Released map.
  var armed_barriers: Set[SlotId]
  // Ghost: "has data for this slot provisionally moved to a target while its handoff
  // was drained but not yet resolved?" — ConfirmSlotHandoffDrained sets it, and every
  // resolution path (Complete/Abort/Cancel/failover-pruning/RemoveNode-pruning) must
  // clear it. This is what makes `inv_abort_repatriates` falsifiable: slot ownership
  // itself never moves before CompleteSlotMigration in this model (matching
  // TR-CLUSTER-010's postcondition — the source keeps the ownership record the whole
  // time), so without this ghost field "the slot is owned by source after abort" would
  // be true by construction on every trace, real or broken.
  var provisional_target: SlotId -> Option[NodeId]
  // "Slots whose Cancel/Abort rollback is mid-repatriation" — added this round for I13:
  // TR-CLUSTER-015's amendment states a real ordering ("streams back to the source
  // *before* the Abort applies clean"), so `cancelMigration`/`abortHandoff` open this
  // phase (releasing the barrier immediately, but leaving the provisional-target/
  // feed-bytes residue — and for Cancel, the migration record itself — in place) rather
  // than resolving everything atomically in one step. `completeRepatriation` clears it.
  // `inv_repatriating_well_formed` states the phase's own structural invariant;
  // `inv_abort_repatriates` below is restated so the phase is a legal exception to its
  // "no residue once no live handoff" claim rather than a violation of it.
  var repatriating: Set[SlotId]
  // TR-CLUSTER-016 amendment: bytes buffered for a replica session held behind this
  // slot's armed barrier. No wall-clock field appears anywhere in this model — the
  // hold is bounded purely by this counter and HOLD_BYTE_CAP.
  var feed_bytes: SlotId -> int
  // Ghost/observability: which slots' held replica sessions have been disconnected for
  // breaching the byte cap (TR-CLUSTER-016 amendment's "breaching it disconnects that
  // replica session rather than buffering unboundedly").
  var disconnected_feed: Set[SlotId]
  // TR-CLUSTER-017/026's drain/parity barrier, explicit: the set of slots armed by
  // `armFailoverFence` and not yet consumed. Single-shot — every action other than
  // `armFailoverFence` clears it (see the header note).
  var failover_fence: Set[SlotId]
  // Every handoff seq ever minted, monotonically grown by `prepareHandoff`. Backs
  // `inv_handoff_seq_never_reused` (INV-HANDOFF's distinctness half).
  var spent: Set[int]
  // The cluster epoch's value one step ago — backs `inv_epoch_never_decreases`, a
  // cross-step claim the single-state INV-EPOCH-1/2 checks cannot state.
  var prev_epoch: int
  // History ghosts, independently recomputed at the point of each relevant action
  // (never gated on that action's own guard, so a mutant that weakens the guard cannot
  // also silence the ghost): has any failover ever left the old primary undemoted
  // (`demotion`, issue 20 / C1), failed its per-object fence (`fence`, issue 19),
  // or applied without the armed drain/parity barrier (`barrier`, issue 26,
  // `failoverGraceful` only); has any CompleteSlotMigration ever admitted without a
  // matching drained handoff (`admission`, issue 17); has any handoff seq ever been
  // minted twice (`seqReuse`, INV-HANDOFF distinctness). Each is monotone (sticky
  // once true) — a defect anywhere in the trace stays visible for the rest of it.
  var defects: { demotion: bool, fence: bool, barrier: bool, admission: bool, seqReuse: bool }
  // Coverage ghosts (added this round for I8/I9) — sticky like `defects`, but tracking
  // *non-defective, interesting-scenario* reach rather than bugs, so the witnesses built
  // on them cannot be satisfied by an unrelated cause the way
  // `witnessGracefulFailoverPrunedMigration`/`witnessForcedFailoverDemoted` originally
  // were (see the note at those witnesses in cluster_migration_failover.qnt):
  // `gracefulPruned` is set only inside
  // `failoverGraceful` at the moment it *actually* prunes a migration that was open on
  // one of the moved slots (checked independently of any guard, from the pre-state);
  // `forcedDemotedReplica` only when `failoverForced` demotes a primary that owned at
  // least one slot (so it cannot be satisfied by the two nodes that are already
  // Replicas at `init`); `failoverDuringOpenMigration` (the composite migration x
  // failover region I8/I9 asks to be genuinely explored) only when any of the three
  // failover actions fires while the old primary had an open migration.
  var coverage: { gracefulPruned: bool, forcedDemotedReplica: bool, failoverDuringOpenMigration: bool }

  // Impure (reads the `armed_barriers` state var directly) — kept out of
  // cluster_migration_failover_logic.qnt's pure helpers so the purity checker doesn't
  // reject it.
  def barrierArmed(s: SlotId): bool = armed_barriers.contains(s)

  // ---------------------------------------------------------------------------
  // Actions.
  // ---------------------------------------------------------------------------

  action init: bool = all {
    nodes' = Map(
      1 -> { role: Primary, parent: None, member: true, fail: false },
      2 -> { role: Primary, parent: None, member: true, fail: false },
      3 -> { role: Replica, parent: Some(1), member: true, fail: false },
      4 -> { role: Replica, parent: Some(2), member: true, fail: false },
    ),
    slots' = Map(1 -> Some(1), 2 -> Some(1), 3 -> Some(2), 4 -> Some(2)),
    migrations' = Map(1 -> None, 2 -> None, 3 -> None, 4 -> None),
    handoff_seq' = 0,
    epoch' = 0,
    node_epoch' = Map(1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0),
    armed_barriers' = Set(),
    provisional_target' = Map(1 -> None, 2 -> None, 3 -> None, 4 -> None),
    feed_bytes' = Map(1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0),
    disconnected_feed' = Set(),
    failover_fence' = Set(),
    spent' = Set(),
    prev_epoch' = 0,
    defects' = { demotion: false, fence: false, barrier: false, admission: false, seqReuse: false },
    repatriating' = Set(),
    coverage' = { gracefulPruned: false, forcedDemotedReplica: false, failoverDuringOpenMigration: false },
  }

  action setRole(n: NodeId, role: RoleTag, parent: Option[NodeId]): bool = all {
    canSetRole(nodes, n, role, parent),
    nodes' = nodes.set(n, applySetRole(nodes.get(n), role, parent)),
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  action removeNode(n: NodeId, force: bool): bool = {
    val prunedSlots = slotsNaming(migrations, n)
    all {
      canRemoveNode(nodes, slots, n, force),
      nodes' = NODES.fold(nodes, (acc, m) =>
        if (m == n) acc.set(m, { ...acc.get(m), member: false })
        else if (acc.get(m).parent == Some(n)) acc.set(m, { ...acc.get(m), parent: None })
        else acc
      ),
      slots' = SLOTS.fold(slots, (acc, s) => if (slots.get(s) == Some(n)) acc.set(s, None) else acc),
      migrations' = prunedMigrations(migrations, n),
      armed_barriers' = armed_barriers.filter(s => not(prunedSlots.contains(s))),
      provisional_target' = SLOTS.fold(provisional_target, (acc, s) => if (prunedSlots.contains(s)) acc.set(s, None) else acc),
      feed_bytes' = SLOTS.fold(feed_bytes, (acc, s) => if (prunedSlots.contains(s)) acc.set(s, 0) else acc),
      disconnected_feed' = disconnected_feed.filter(s => not(prunedSlots.contains(s))),
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      repatriating' = repatriating.filter(s => not(prunedSlots.contains(s))),
      coverage' = coverage,
    }
  }

  action assignSlot(s: SlotId, n: NodeId): bool = all {
    canAssignSlot(migrations, nodes.get(n), s),
    slots' = slots.set(s, Some(n)),
    nodes' = nodes,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  action removeSlot(s: SlotId): bool = all {
    canRemoveSlot(migrations, s),
    slots' = slots.set(s, None),
    nodes' = nodes,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  action beginMigration(s: SlotId, source: NodeId, target: NodeId): bool = all {
    canBeginMigration(slots, migrations, nodes.get(target), s, source, target),
    migrations' = applyBeginMigration(migrations, s, source, target),
    nodes' = nodes,
    slots' = slots,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  action prepareHandoff(s: SlotId): bool = {
    val newSeq = handoff_seq + 1
    val reused = spent.contains(newSeq)
    val result = applyPrepareHandoff(migrations, armed_barriers, s, newSeq)
    all {
      canPrepareHandoff(migrations, repatriating, s),
      migrations' = result.migs,
      armed_barriers' = result.armed,
      handoff_seq' = newSeq,
      nodes' = nodes,
      slots' = slots,
      epoch' = epoch,
      node_epoch' = node_epoch,
      provisional_target' = provisional_target,
      feed_bytes' = feed_bytes,
      disconnected_feed' = disconnected_feed,
      failover_fence' = Set(),
      spent' = spent.union(Set(newSeq)),
      prev_epoch' = epoch,
      defects' = { ...defects, seqReuse: defects.seqReuse or reused },
      repatriating' = repatriating,
      coverage' = coverage,
    }
  }

  action confirmDrained(s: SlotId, seq: int): bool = {
    val targetOpt = match migrations.get(s) {
      | Some(m) => Some(m.target)
      | None => None
    }
    all {
      canConfirmDrained(migrations, s, seq),
      migrations' = applyConfirmDrained(migrations, s),
      provisional_target' = provisional_target.set(s, targetOpt),
      nodes' = nodes,
      slots' = slots,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      armed_barriers' = armed_barriers,
      feed_bytes' = feed_bytes,
      disconnected_feed' = disconnected_feed,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      repatriating' = repatriating,
      coverage' = coverage,
    }
  }

  action completeMigration(s: SlotId, seq: int): bool = {
    val wasDrained = match migrations.get(s) {
      | Some(m) => match m.handoff { | Some(h) => h.seq == seq and h.drained | None => false }
      | None => false
    }
    all {
      canComplete(migrations, s, seq),
      slots' = slots.set(s, match migrations.get(s) { | Some(m) => Some(m.target) | None => None }),
      migrations' = migrations.set(s, None),
      armed_barriers' = armed_barriers.filter(x => x != s),
      provisional_target' = provisional_target.set(s, None),
      feed_bytes' = feed_bytes.set(s, 0),
      disconnected_feed' = disconnected_feed.filter(x => x != s),
      nodes' = nodes,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = { ...defects, admission: defects.admission or not(wasDrained) },
      repatriating' = repatriating,
      coverage' = coverage,
    }
  }

  // TR-CLUSTER-014: releases only the handoff; the migration survives. Restructured
  // this round for I13: if there is residue to repatriate (provisional-at-target,
  // buffered feed bytes, or a disconnected-feed mark), the barrier releases immediately
  // but the residue is left in place and the slot enters the `repatriating` phase —
  // `completeRepatriation` later clears it. If there is nothing to repatriate, the
  // (already-empty) residue fields are left as-is, which is observably identical to
  // clearing them, so the no-residue case is unaffected by this round's change.
  action abortHandoff(s: SlotId, seq: int): bool = {
    val hasResidue = provisional_target.get(s) != None or feed_bytes.get(s) > 0 or disconnected_feed.contains(s)
    all {
      canAbortHandoff(migrations, repatriating, s, seq),
      migrations' = match migrations.get(s) {
        | Some(m) => migrations.set(s, Some({ ...m, handoff: None }))
        | None => migrations
      },
      armed_barriers' = armed_barriers.filter(x => x != s),
      // Issue 15 amendment: the write barrier (governing new incoming writes) releases
      // immediately; the already-moved provisional data streams back separately, over
      // the `repatriating` phase, rather than in this same step (see the header note).
      provisional_target' = provisional_target,
      feed_bytes' = feed_bytes,
      disconnected_feed' = disconnected_feed,
      nodes' = nodes,
      slots' = slots,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      repatriating' = if (hasResidue) repatriating.union(Set(s)) else repatriating,
      coverage' = coverage,
    }
  }

  // TR-CLUSTER-015, amended (issue 15): Cancel opens the `repatriating` phase the same
  // way Abort does (added this round for I13) — if there is residue, the barrier
  // releases immediately and the migration record is marked `cancelling: true` (handoff
  // cleared) and kept, rather than removed, until `completeRepatriation` resolves the
  // stream and removes it. With nothing to repatriate, the migration record is removed
  // immediately, exactly as before this round.
  action cancelMigration(s: SlotId): bool = {
    val hasResidue = provisional_target.get(s) != None or feed_bytes.get(s) > 0 or disconnected_feed.contains(s)
    all {
      canCancel(migrations, repatriating, s),
      migrations' = if (hasResidue)
          match migrations.get(s) {
            | Some(m) => migrations.set(s, Some({ ...m, handoff: None, cancelling: true }))
            | None => migrations
          }
        else migrations.set(s, None),
      armed_barriers' = armed_barriers.filter(x => x != s),
      provisional_target' = provisional_target,
      feed_bytes' = feed_bytes,
      disconnected_feed' = disconnected_feed,
      nodes' = nodes,
      slots' = slots,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      repatriating' = if (hasResidue) repatriating.union(Set(s)) else repatriating,
      coverage' = coverage,
    }
  }

  // Added this round for I13: resolves a slot's `repatriating` phase once the stream
  // back to the source has finished. For a Cancel-originated phase (`cancelling ==
  // true`), this is where the migration record is finally removed — "the Abort applies
  // clean," in the amendment's words. For an Abort-originated phase (migration
  // survives, handoff already cleared), the migration record is left untouched.
  action completeRepatriation(s: SlotId): bool = {
    val wasCancelling = match migrations.get(s) { | Some(m) => m.cancelling | None => false }
    all {
      canCompleteRepatriation(repatriating, s),
      migrations' = if (wasCancelling) migrations.set(s, None) else migrations,
      provisional_target' = provisional_target.set(s, None),
      feed_bytes' = feed_bytes.set(s, 0),
      disconnected_feed' = disconnected_feed.filter(x => x != s),
      repatriating' = repatriating.filter(x => x != s),
      armed_barriers' = armed_barriers,
      nodes' = nodes,
      slots' = slots,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      coverage' = coverage,
    }
  }

  // TR-CLUSTER-016 amendment: buffer `n` bytes for the held session behind slot `s`'s
  // armed barrier. Modeled as a single atomic transition rather than two (arrival, then
  // a separate reactive disconnect) — a deliberate simplification (this model carries
  // no discrete inter-arrival time), disclosed here rather than left to look like the
  // cap silently refuses: an arrival that would breach the cap disconnects the session
  // in the same step instead of being admitted or refused. `inv_feed_hold_bounded`
  // holds unconditionally as a result, and is falsifiable by a mutant that skips the
  // breach branch (always takes the plain-buffer path).
  action feedBuffer(s: SlotId, n: int): bool = {
    val newBytes = feed_bytes.get(s) + n
    val breached = newBytes > HOLD_BYTE_CAP
    all {
      armed_barriers.contains(s),
      n > 0,
      feed_bytes' = if (breached) feed_bytes.set(s, 0) else feed_bytes.set(s, newBytes),
      disconnected_feed' = if (breached) disconnected_feed.union(Set(s)) else disconnected_feed,
      nodes' = nodes,
      slots' = slots,
      migrations' = migrations,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
      armed_barriers' = armed_barriers,
      provisional_target' = provisional_target,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = defects,
      repatriating' = repatriating,
      coverage' = coverage,
    }
  }

  action markNodeFailed(n: NodeId): bool = all {
    nodes.get(n).member,
    nodes' = nodes.set(n, { ...nodes.get(n), fail: true }),
    // TR-CLUSTER-024: unconditional bump on every mark.
    epoch' = epoch + 1,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  action markNodeRecovered(n: NodeId): bool = all {
    nodes.get(n).member,
    nodes' = nodes.set(n, { ...nodes.get(n), fail: false }),
    // TR-CLUSTER-025: no epoch bump (FM-CLUSTER-014 anti-churn) — disclaimer only,
    // see the header note; not asserted by any invariant in this model.
    epoch' = epoch,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    failover_fence' = Set(),
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  // TR-CLUSTER-017/026, ruled (issue 26): arm the drain/parity barrier over every slot
  // `old` currently owns. `failoverGraceful` requires every slot it is about to move to
  // be in this set; every other action clears it (single-shot — see the header note).
  action armFailoverFence(old: NodeId): bool = all {
    nodes.get(old).member,
    failover_fence' = SLOTS.filter(s => slots.get(s) == Some(old)),
    nodes' = nodes,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
    spent' = spent,
    prev_epoch' = epoch,
    defects' = defects,
    repatriating' = repatriating,
    coverage' = coverage,
  }

  // TR-CLUSTER-017/018: planned/graceful failover — old primary must currently be
  // healthy and its drain/parity barrier (`failover_fence`) armed over every slot it
  // owns; both the guard and the independently recomputed `defects.barrier` ghost check
  // the same fact, so a mutant that drops the guard is still caught by the ghost.
  action failoverGraceful(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed, repatriating: repatriating }
    val result = applyFailoverCommon(st, old, succ)
    val movedSlots = SLOTS.filter(s => slots.get(s) == Some(old))
    val barrierHeld = movedSlots.forall(s => failover_fence.contains(s))
    val fenced = obsEpoch == node_epoch.get(old) and obsRole == nodes.get(old).role
    val demoted = and {
      result.nodes.get(old).role == Replica,
      result.nodes.get(old).parent == Some(succ),
      result.nodes.get(old).member,
      result.nodes.get(succ).role == Primary,
    }
    // I8/I9 coverage ghosts: independently recomputed at the pre-state, before
    // `applyFailoverCommon` prunes anything, so they cannot be satisfied by an
    // unrelated cause the way the witnesses they back originally were (see the removal
    // note on the old witnesses below).
    val hadNamedMigration = slotsNaming(migrations, old) != Set()
    all {
      old != succ,
      beltOk(nodes, slots, old, succ),
      not(nodes.get(old).fail),
      fenceOk(nodes.get(old), node_epoch.get(old), obsEpoch, obsRole),
      barrierHeld,
      nodes' = result.nodes,
      slots' = result.slots,
      migrations' = result.migrations,
      epoch' = result.epoch,
      node_epoch' = result.node_epoch,
      armed_barriers' = result.armed_barriers,
      provisional_target' = result.provisional_target,
      feed_bytes' = result.feed_bytes,
      disconnected_feed' = result.disconnected_feed,
      repatriating' = result.repatriating,
      handoff_seq' = handoff_seq,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = { ...defects,
        demotion: defects.demotion or not(demoted),
        fence: defects.fence or not(fenced),
        barrier: defects.barrier or not(barrierHeld),
      },
      coverage' = { ...coverage,
        gracefulPruned: coverage.gracefulPruned or hadNamedMigration,
        failoverDuringOpenMigration: coverage.failoverDuringOpenMigration or hadNamedMigration,
      },
    }
  }

  // TR-CLUSTER-019: automatic failover — gated on the old primary's FAIL flag being
  // latched (the leader's reconcile pass has already marked it), async-lossy (no
  // drain/parity precondition, per issue 26's ruling that the automatic path stays
  // asynchronous — `failover_fence` is not required here).
  action failoverAuto(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed, repatriating: repatriating }
    val result = applyFailoverCommon(st, old, succ)
    val fenced = obsEpoch == node_epoch.get(old) and obsRole == nodes.get(old).role
    val demoted = and {
      result.nodes.get(old).role == Replica,
      result.nodes.get(old).parent == Some(succ),
      result.nodes.get(old).member,
      result.nodes.get(succ).role == Primary,
    }
    val hadNamedMigration = slotsNaming(migrations, old) != Set()
    all {
      old != succ,
      beltOk(nodes, slots, old, succ),
      nodes.get(old).fail,
      fenceOk(nodes.get(old), node_epoch.get(old), obsEpoch, obsRole),
      nodes' = result.nodes,
      slots' = result.slots,
      migrations' = result.migrations,
      epoch' = result.epoch,
      node_epoch' = result.node_epoch,
      armed_barriers' = result.armed_barriers,
      provisional_target' = result.provisional_target,
      feed_bytes' = result.feed_bytes,
      disconnected_feed' = result.disconnected_feed,
      repatriating' = result.repatriating,
      handoff_seq' = handoff_seq,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = { ...defects,
        demotion: defects.demotion or not(demoted),
        fence: defects.fence or not(fenced),
      },
      coverage' = { ...coverage,
        failoverDuringOpenMigration: coverage.failoverDuringOpenMigration or hadNamedMigration,
      },
    }
  }

  // TR-CLUSTER-042 (`force: true`): the belt+fence shape is identical to the graceful
  // path (issue 19's amendment applies to both), but neither the old primary's health
  // flag nor the drain/parity barrier gates it — force means force. Per the header's
  // deviation note, the postcondition still demotes rather than removes.
  action failoverForced(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed, repatriating: repatriating }
    val result = applyFailoverCommon(st, old, succ)
    val fenced = obsEpoch == node_epoch.get(old) and obsRole == nodes.get(old).role
    val demoted = and {
      result.nodes.get(old).role == Replica,
      result.nodes.get(old).parent == Some(succ),
      result.nodes.get(old).member,
      result.nodes.get(succ).role == Primary,
    }
    val hadNamedMigration = slotsNaming(migrations, old) != Set()
    all {
      old != succ,
      beltOk(nodes, slots, old, succ),
      fenceOk(nodes.get(old), node_epoch.get(old), obsEpoch, obsRole),
      nodes' = result.nodes,
      slots' = result.slots,
      migrations' = result.migrations,
      epoch' = result.epoch,
      node_epoch' = result.node_epoch,
      armed_barriers' = result.armed_barriers,
      provisional_target' = result.provisional_target,
      feed_bytes' = result.feed_bytes,
      disconnected_feed' = result.disconnected_feed,
      repatriating' = result.repatriating,
      handoff_seq' = handoff_seq,
      failover_fence' = Set(),
      spent' = spent,
      prev_epoch' = epoch,
      defects' = { ...defects,
        demotion: defects.demotion or not(demoted),
        fence: defects.fence or not(fenced),
      },
      // beltOk already requires `old` to be a Primary owning >=1 slot, so this ghost is
      // set on every successful firing — deliberately: the old witness
      // (`witnessForcedFailoverDemoted`) was vacuous because it scanned *current* state
      // for "any Replica exists" (satisfied by nodes 3/4's pre-existing role at init,
      // unrelated to forced failover ever firing); this ghost is set only *inside*
      // `failoverForced`'s own execution, so it discriminates "forced failover actually
      // fired" from "a replica happens to exist" — a state no other action can produce.
      coverage' = { ...coverage,
        forcedDemotedReplica: true,
        failoverDuringOpenMigration: coverage.failoverDuringOpenMigration or hadNamedMigration,
      },
    }
  }

  action stutter: bool = all {
    nodes' = nodes, slots' = slots, migrations' = migrations, handoff_seq' = handoff_seq,
    epoch' = epoch, node_epoch' = node_epoch, armed_barriers' = armed_barriers,
    provisional_target' = provisional_target, feed_bytes' = feed_bytes, disconnected_feed' = disconnected_feed,
    failover_fence' = Set(), spent' = spent, prev_epoch' = epoch, defects' = defects,
    repatriating' = repatriating, coverage' = coverage,
  }

  action step: bool = {
    // I8/I9/I10 reachability rework: `any{}` samples every `nondet` once per step, then
    // picks uniformly among the branches whose guard currently holds for those sampled
    // values — so a uniform pick over the full domain starves any transition whose guard
    // needs a *specific*, state-derived value (e.g. beginMigration's source must be the
    // slot's actual owner; a failover successor must be a replica of the chosen primary).
    // Below, `s`/`n1`/`n2` stay uniform (still used by actions that want a *fresh*, often
    // migration-free, slot/node — assignSlot/removeSlot/beginMigration's target, setRole),
    // while `handoffSlot`/`succGuess` are biased toward a state-derived "currently
    // interesting" candidate set when nonempty, falling back to the full domain when it
    // isn't (quint-modeling's "narrower value space with fallback" pattern — this changes
    // only which transitions the walk is *likely* to try, never which are legal: no guard
    // or postcondition reads these vals).
    val openMigSources = migrationSourcesOf(migrations)
    val openMigSlots = openMigrationSlots(migrations).union(repatriating)
    val fenceOwners = fencedOwners(failover_fence, slots)
    nondet s = SLOTS.oneOf()
    // Fence-owner bias takes priority over migration-source bias: a just-armed barrier is
    // single-shot and worth firing before it's clobbered, whereas an open migration stays
    // open across many steps and doesn't need the same urgency (see `fencedOwners` above).
    nondet n1 = (if (fenceOwners != Set()) fenceOwners else if (openMigSources != Set()) openMigSources else NODES).oneOf()
    nondet n2 = NODES.oneOf()
    nondet handoffSlot = (if (openMigSlots != Set()) openMigSlots else SLOTS).oneOf()
    val replicaCandidates = replicasOf(nodes, n1)
    nondet succGuess = (if (replicaCandidates != Set()) replicaCandidates else NODES).oneOf()
    nondet roleGuess = Set(Primary, Replica).oneOf()
    // Failover's `obsEpoch`/`obsRole` are the caller's *observation* of `n1`, which
    // `fenceOk`/`fenced` then check against the true current values — a different question
    // from `roleGuess`'s "what role should this node become" above, so splitting them from
    // a shared guess already removes an accidental ~50%/50% double penalty. `failoverGraceful`
    // additionally needs `barrierHeld` (a third, independent conjunct), so its natural hit
    // rate is far lower than auto/forced's: mostly-correct-with-fallback (narrower value
    // space, not a full guess) rather than the fully-deterministic derivation `beginSource`
    // uses above, since a mint-once-and-forget derivation would make `fenceOk`'s guard on
    // *this* action permanently unfalsifiable by `quint run` (an observation that is always
    // right by construction can never make the guard the deciding conjunct — confirmed by a
    // mutant that deleted `fenceOk` here surviving 20,000 samples before this fallback was
    // added). `obsCorrectCoin <= 3` (75%) keeps the reachability win; `> 3` (25%) still
    // exercises a wrong observation often enough for the guard to matter. Auto/forced keep
    // the original 2-way guess: they have no barrier conjunct, so a fully-deterministic
    // derivation overshot their band (58%, up from a healthy 22%) — confirmed by
    // measurement, not assumption. The stale-observation refusal path keeps dedicated
    // coverage either way (`failoverRefusesStaleFenceTest`), independent of this random walk.
    nondet obsCorrectCoin = Set(1, 2, 3, 4).oneOf()
    val obsEpochGraceful = if (obsCorrectCoin <= 3) node_epoch.get(n1) else node_epoch.get(n1) + 1
    val obsRoleGraceful = if (obsCorrectCoin <= 3) nodes.get(n1).role
      else (if (nodes.get(n1).role == Primary) Replica else Primary)
    nondet obsEpochGuess = Set(node_epoch.get(n1), node_epoch.get(n1) + 1).oneOf()
    nondet obsRoleGuess = Set(Primary, Replica).oneOf()
    nondet bytesGuess = Set(1, 2, 3, HOLD_BYTE_CAP).oneOf()
    // Sometimes replay a stale seq (one behind the current counter) instead of the
    // live one, so a supersede's now-stale handoff can actually be acted on and
    // refused (TR-CLUSTER-012's ruled claim, otherwise unreachable — see the header's
    // PrepareSlotHandoff note).
    nondet seqGuess = Set(handoff_seq, if (handoff_seq > 0) handoff_seq - 1 else 0).oneOf()
    nondet forceGuess = Set(true, false).oneOf()
    // beginMigration's source is derived from the slot's actual owner, not guessed —
    // `canBeginMigration`'s ownership conjunct is then always structurally satisfiable
    // (was ~25%, 1-in-4 nodes, under the old uniform n1 guess), with no semantic change:
    // this is exactly the value `canBeginMigration` requires source to equal.
    val beginSource = match slots.get(s) { | Some(o) => o | None => n1 }
    any {
      setRole(n1, roleGuess, if (roleGuess == Replica) Some(n2) else None),
      removeNode(n1, forceGuess),
      assignSlot(s, n1),
      removeSlot(s),
      beginMigration(s, beginSource, n2),
      prepareHandoff(handoffSlot),
      confirmDrained(handoffSlot, seqGuess),
      completeMigration(handoffSlot, seqGuess),
      abortHandoff(handoffSlot, seqGuess),
      cancelMigration(handoffSlot),
      completeRepatriation(handoffSlot),
      feedBuffer(handoffSlot, bytesGuess),
      markNodeFailed(n1),
      markNodeRecovered(n1),
      armFailoverFence(n1),
      failoverGraceful(n1, succGuess, obsEpochGraceful, obsRoleGraceful),
      failoverAuto(n1, succGuess, obsEpochGuess, obsRoleGuess),
      failoverForced(n1, succGuess, obsEpochGuess, obsRoleGuess),
      stutter,
    }
  }
}
