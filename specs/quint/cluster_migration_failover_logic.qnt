// cluster_migration_failover_logic — the pure logic of the migration x failover
// composite model (formal-spec phase 2, task 2): every guard (`can*`), every update
// (`apply*`), and the small helpers they share. Each takes the state it reads as an
// explicit argument, so this module holds no `var` and is unit-testable in isolation.
//
// Rows whose ruled semantics are encoded here: TR-CLUSTER-003 (RemoveNode fence),
// TR-CLUSTER-004 (SetRole), TR-CLUSTER-008/009 (Assign/RemoveSlots), TR-CLUSTER-010
// (BeginSlotMigration), TR-CLUSTER-011 (PrepareSlotHandoff), TR-CLUSTER-012
// (ConfirmSlotHandoffDrained), TR-CLUSTER-013 (CompleteSlotMigration), TR-CLUSTER-014
// (AbortSlotHandoff), TR-CLUSTER-015 (CancelSlotMigration + repatriation),
// TR-CLUSTER-017/018/019/042 (the shared failover postcondition, the per-object fence
// and the belt), TR-CLUSTER-021 (successor selection's structural precondition) and
// TR-CLUSTER-034 (handoff barrier arm/release). Per-definition comments below carry the
// full ruling/amendment rationale; the model-wide header lives in
// cluster_migration_failover.qnt.
module cluster_migration_failover_logic {
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"

  // ---------------------------------------------------------------------------
  // Small pure helpers.
  // ---------------------------------------------------------------------------

  pure def max2(a: int, b: int): int = if (a > b) a else b

  pure def ownsAnySlot(slotMap: SlotId -> Option[NodeId], n: NodeId): bool =
    SLOTS.exists(s => slotMap.get(s) == Some(n))

  // The set of slots whose open migration names `node` on either leg (source or
  // target) — TR-CLUSTER-018/042's "Open migrations naming old_primary_id pruned"
  // postcondition, generalized to every failover per issue 15's ruling ("keyed to the
  // failover transition itself, NOT node removal") and to TR-CLUSTER-003's own prune.
  pure def slotsNaming(migs: SlotId -> Option[Migration], node: NodeId): Set[SlotId] =
    SLOTS.filter(s => match migs.get(s) {
      | Some(m) => m.source == node or m.target == node
      | None => false
    })

  // Drops every migration entry naming `node` (issue 15's prune; also TR-CLUSTER-003's).
  pure def prunedMigrations(migs: SlotId -> Option[Migration], node: NodeId): SlotId -> Option[Migration] =
    SLOTS.fold(migs, (acc, s) =>
      match migs.get(s) {
        | Some(m) => if (m.source == node or m.target == node) acc.set(s, None) else acc
        | None => acc
      }
    )

  // ---------------------------------------------------------------------------
  // Nondet-biasing helpers (added this round for I8/I9/I10) — narrow the candidate set
  // `step` samples from to state-derived "currently interesting" values when that set is
  // nonempty, falling back to the full domain otherwise. This changes only which
  // transitions the randomized walk is *likely* to try, never which transitions are
  // legal — no guard or postcondition depends on these. See quint-modeling's "narrower
  // value space with fallback" pattern.
  // ---------------------------------------------------------------------------

  // The set of nodes currently the source of an open migration — biases `step`'s shared
  // `n1` toward a node a migration/handoff/failover action can actually act on, instead
  // of `beginMigration`'s old ~25% (1-in-4 nodes) hit rate against `canBeginMigration`'s
  // ownership conjunct.
  pure def migrationSourcesOf(migs: SlotId -> Option[Migration]): Set[NodeId] =
    SLOTS.fold(Set(), (acc, s) => match migs.get(s) {
      | Some(m) => acc.union(Set(m.source))
      | None => acc
    })

  // Slots with an open migration, or mid-repatriation — biases the handoff-lifecycle
  // nondet slot pick toward a slot those actions can actually act on.
  pure def openMigrationSlots(migs: SlotId -> Option[Migration]): Set[SlotId] =
    SLOTS.filter(s => migs.get(s) != None)

  // Current members who are a Replica of `n` — biases failover's successor pick toward
  // a value `beltOk` can actually accept, instead of an independent 1-in-4 uniform pick.
  pure def replicasOf(allNodes: NodeId -> NodeState, n: NodeId): Set[NodeId] =
    NODES.filter(m => allNodes.get(m).member and allNodes.get(m).role == Replica and allNodes.get(m).parent == Some(n))

  // Owners of the slots currently held in `failover_fence` — biases `step`'s shared `n1`
  // toward the node a just-armed barrier belongs to. `armFailoverFence` is single-shot
  // (every other action clears it — see its own comment), so firing `failoverGraceful`
  // for the armed node requires picking that *same* `n1` on the very next step; without
  // this bias `n1` is redrawn uniformly (or migration-source-biased) each step, making the
  // arm-then-fire pair a near-independent double draw. Narrower-domain-with-fallback, same
  // pattern as `migrationSourcesOf`/`replicasOf` above — legality is unaffected, since
  // `armFailoverFence`/`failoverGraceful`'s own guards are unchanged.
  pure def fencedOwners(fence: Set[SlotId], slotMap: SlotId -> Option[NodeId]): Set[NodeId] =
    fence.fold(Set(), (acc, s) => match slotMap.get(s) {
      | Some(n) => acc.union(Set(n))
      | None => acc
    })

  // ---------------------------------------------------------------------------
  // Functional logic (guards + updates), pure and unit-testable in isolation.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-004, ruled (issue 14): a Replica's parent must be a present Primary; a
  // Primary's own parent pointer must be empty (INV-REF-4's structural counterpart).
  // No slot-ownership or migration-target precondition — see the header note on why a
  // prior revision's extra guards here were removed.
  pure def canSetRole(allNodes: NodeId -> NodeState, n: NodeId, role: RoleTag, parent: Option[NodeId]): bool =
    allNodes.get(n).member and (
      if (role == Primary) parent == None
      else match parent {
        | Some(p) => p != n and allNodes.get(p).member and allNodes.get(p).role == Primary
        | None => true
      }
    )

  pure def applySetRole(ns: NodeState, role: RoleTag, parent: Option[NodeId]): NodeState =
    { ...ns, role: role, parent: parent }

  // TR-CLUSTER-003, ruled + amended (issue 20): refused unless the node is already
  // demoted off every slot it owns and out of the primary role, unless `force`
  // bypasses the fence.
  pure def canRemoveNode(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId], n: NodeId, force: bool): bool =
    allNodes.get(n).member and (force or (allNodes.get(n).role == Replica and not(ownsAnySlot(slotMap, n))))

  // TR-CLUSTER-008, ruled (issue 16): refused outright while a migration is open for
  // the slot — no mutation-while-migrating. No target-role precondition — see the
  // header note.
  pure def canAssignSlot(migs: SlotId -> Option[Migration], target: NodeState, s: SlotId): bool = and {
    migs.get(s) == None,
    target.member,
  }

  // TR-CLUSTER-009, same ruled precondition as TR-CLUSTER-008 (issue 16).
  pure def canRemoveSlot(migs: SlotId -> Option[Migration], s: SlotId): bool =
    migs.get(s) == None

  // TR-CLUSTER-010: `slot`'s current owner is `source`; no existing migration entry
  // for `slot` (the idempotent same-source/target re-issue is not separately modeled —
  // see "Deliberately not modeled"). No target-role precondition — see the header note.
  pure def canBeginMigration(slotMap: SlotId -> Option[NodeId], migs: SlotId -> Option[Migration],
                              targetNode: NodeState, s: SlotId, source: NodeId, target: NodeId): bool = and {
    slotMap.get(s) == Some(source),
    migs.get(s) == None,
    source != target,
    targetNode.member,
  }

  pure def applyBeginMigration(migs: SlotId -> Option[Migration], s: SlotId, source: NodeId, target: NodeId): SlotId -> Option[Migration] =
    migs.set(s, Some({ source: source, target: target, handoff: None, cancelling: false }))

  // TR-CLUSTER-011: migration exists (matching source/target is not separately
  // modeled — see "Deliberately not modeled"). No "no live handoff" precondition: the
  // ruled row text does not carry one, and requiring it made the supersede path
  // (FM-CLUSTER-085) and TR-CLUSTER-012's stale-seq refusal unreachable — see the
  // header note.
  // `not(rep.contains(s))` (added this round for I13): a slot mid-repatriation is
  // between Cancel/Abort and its resolution — re-preparing a handoff on it would
  // resurrect a migration that is being torn down, corrupting the phase.
  pure def canPrepareHandoff(migs: SlotId -> Option[Migration], rep: Set[SlotId], s: SlotId): bool =
    migs.get(s) != None and not(rep.contains(s))

  // TR-CLUSTER-011/034: mints seq from the bumped counter, arms the barrier at the
  // source (armed_barriers keyed by slot — see the header's TR-CLUSTER-034 note). A
  // re-prepare over a live handoff supersedes it wholesale (the old seq becomes stale
  // and is thereafter refused by Confirm/Complete/Abort's seq match).
  pure def applyPrepareHandoff(migs: SlotId -> Option[Migration], armed: Set[SlotId], s: SlotId, newSeq: int): { migs: SlotId -> Option[Migration], armed: Set[SlotId] } =
    match migs.get(s) {
      | Some(m) => { migs: migs.set(s, Some({ ...m, handoff: Some({ seq: newSeq, drained: false }) })), armed: armed.union(Set(s)) }
      | None => { migs: migs, armed: armed }
    }

  // TR-CLUSTER-012: the prepared handoff exists and its seq matches (stale acks from a
  // superseded attempt are refused).
  pure def canConfirmDrained(migs: SlotId -> Option[Migration], s: SlotId, seq: int): bool =
    match migs.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => h.seq == seq
          | None => false
        }
      | None => false
    }

  // TR-CLUSTER-012 postcondition, plus the ghost "data provisionally moved to target"
  // fact this model needs for `inv_abort_repatriates` to be non-vacuous.
  pure def applyConfirmDrained(migs: SlotId -> Option[Migration], s: SlotId): SlotId -> Option[Migration] =
    match migs.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => migs.set(s, Some({ ...m, handoff: Some({ ...h, drained: true }) }))
          | None => migs
        }
      | None => migs
    }

  // TR-CLUSTER-013, ruled (issue 17): admitted purely on matching-seq-and-drained — no
  // wall-clock `barrier_ms` window appears anywhere in this guard.
  pure def canComplete(migs: SlotId -> Option[Migration], s: SlotId, seq: int): bool =
    match migs.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => h.seq == seq and h.drained
          | None => false
        }
      | None => false
    }

  // TR-CLUSTER-014, ruled (issue 17): the prepared handoff exists with matching seq;
  // log-ordered admission, no wall-clock window. (The FM-CLUSTER-086 "already gone ->
  // no-op success" branch is not separately modeled — see "Deliberately not modeled".)
  // `not(rep.contains(s))` (added this round for I13, stated explicitly rather than
  // left implicit): a slot already mid-repatriation always has `handoff == None`
  // (both the Cancel and Abort entry points into the phase clear it), so the
  // `Some(handoff)` match below already excludes re-entry structurally — the explicit
  // conjunct is retained purely for readability/symmetry with `canCancel`.
  pure def canAbortHandoff(migs: SlotId -> Option[Migration], rep: Set[SlotId], s: SlotId, seq: int): bool =
    not(rep.contains(s)) and match migs.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => h.seq == seq
          | None => false
        }
      | None => false
    }

  // TR-CLUSTER-015: any open migration (with or without a live handoff) may be
  // cancelled. `not(rep.contains(s))` (added this round for I13): a slot already
  // mid-repatriation cannot be re-cancelled until `completeRepatriation` resolves it.
  pure def canCancel(migs: SlotId -> Option[Migration], rep: Set[SlotId], s: SlotId): bool =
    migs.get(s) != None and not(rep.contains(s))

  // Added this round for I13: `completeRepatriation` may resolve any slot mid-phase.
  pure def canCompleteRepatriation(rep: Set[SlotId], s: SlotId): bool =
    rep.contains(s)

  // ---------------------------------------------------------------------------
  // Failover logic — the postcondition shared by all three failover paths, plus the
  // per-object fence (issue 19) and the belt they are gated by.
  // ---------------------------------------------------------------------------

  // The single postcondition shared by the planned (TR-CLUSTER-018), automatic
  // (TR-CLUSTER-019) and forced (TR-CLUSTER-042, deviated per the header note) failover
  // paths: ownership transfer, role swap, sibling re-parenting, epoch bump, and issue
  // 15's migration pruning. All three differ only in their *preconditions* (see the
  // wrapper actions below) — never in this postcondition, since the demote-don't-remove
  // ruling now applies uniformly.
  pure def applyFailoverCommon(
    st: { nodes: NodeId -> NodeState, slots: SlotId -> Option[NodeId], migrations: SlotId -> Option[Migration],
          epoch: int, node_epoch: NodeId -> int, armed_barriers: Set[SlotId], provisional_target: SlotId -> Option[NodeId],
          feed_bytes: SlotId -> int, disconnected_feed: Set[SlotId], repatriating: Set[SlotId] },
    old: NodeId, succ: NodeId
  ): { nodes: NodeId -> NodeState, slots: SlotId -> Option[NodeId], migrations: SlotId -> Option[Migration],
       epoch: int, node_epoch: NodeId -> int, armed_barriers: Set[SlotId], provisional_target: SlotId -> Option[NodeId],
       feed_bytes: SlotId -> int, disconnected_feed: Set[SlotId], repatriating: Set[SlotId] } = {
    val newEpoch = st.epoch + 1
    val movedSlots = SLOTS.filter(s => st.slots.get(s) == Some(old))
    val newSlots = SLOTS.fold(st.slots, (acc, s) => if (movedSlots.contains(s)) acc.set(s, Some(succ)) else acc)
    val reparented = NODES.fold(st.nodes, (acc, n) =>
      if (n != old and n != succ and st.nodes.get(n).parent == Some(old))
        acc.set(n, { ...acc.get(n), parent: Some(succ) })
      else acc
    )
    val demoted = reparented.set(old, { ...reparented.get(old), role: Replica, parent: Some(succ) })
    val promoted = demoted.set(succ, { ...demoted.get(succ), role: Primary, parent: None })
    val prunedSet = slotsNaming(st.migrations, old)
    val newMigrations = prunedMigrations(st.migrations, old)
    val newArmed = st.armed_barriers.filter(s => not(prunedSet.contains(s)))
    val newProvisional = SLOTS.fold(st.provisional_target, (acc, s) => if (prunedSet.contains(s)) acc.set(s, None) else acc)
    val newFeedBytes = SLOTS.fold(st.feed_bytes, (acc, s) => if (prunedSet.contains(s)) acc.set(s, 0) else acc)
    val newDisconnected = st.disconnected_feed.filter(s => not(prunedSet.contains(s)))
    // Failover's prune is unconditional and does not route through the `repatriating`
    // phase (see the header's TR-CLUSTER-015 "Deliberately not modeled" note) — any
    // pending repatriation for a slot naming `old` is written off along with the
    // migration record itself, not left dangling.
    val newRepatriating = st.repatriating.filter(s => not(prunedSet.contains(s)))
    {
      nodes: promoted,
      slots: newSlots,
      migrations: newMigrations,
      epoch: newEpoch,
      node_epoch: st.node_epoch.set(succ, newEpoch),
      armed_barriers: newArmed,
      provisional_target: newProvisional,
      feed_bytes: newFeedBytes,
      disconnected_feed: newDisconnected,
      repatriating: newRepatriating,
    }
  }

  // TR-CLUSTER-018/019's shared per-object fence (issue 19, amended): the proposal
  // carries the old primary's *observed* epoch/role; apply refuses if that specific
  // node's current epoch/role has since diverged from what was observed. Not a global
  // epoch check (a global fence was rejected — see the header's TR-CLUSTER-019 note).
  pure def fenceOk(ns: NodeState, curEpoch: int, obsEpoch: int, obsRole: RoleTag): bool =
    obsEpoch == curEpoch and obsRole == ns.role

  // The belt (retained through issue 19's amendment): refuse the move outright if the
  // old primary is not a member and currently a Primary, owns no slots, or the new
  // primary is not a *current member* replica of it. `old`'s role being Primary is
  // TR-CLUSTER-017/019's own ruled precondition text ("old_primary_id ... with Node
  // role = Primary") — stated explicitly here rather than left to be implied by
  // `ownsAnySlot`, since this model no longer claims every slot owner is a Primary
  // (see the header's INV-SLOT-1 note). `succ.member` is likewise stated explicitly:
  // "new_primary_id is a replica of the failing primary" presupposes a *current*
  // replica — `removeNode` (TR-CLUSTER-003) deliberately leaves a removed node's own
  // stale role/parent bookkeeping untouched (only clearing *other* nodes' references
  // to it), so without this conjunct a node removed while still a replica could later
  // be "promoted" by a failover despite `member == false` — caught by `quint run`
  // (inv_slot_owner_valid, inv_migration_endpoints_valid) before this conjunct was
  // added.
  pure def beltOk(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId], old: NodeId, succ: NodeId): bool = and {
    allNodes.get(old).member,
    allNodes.get(old).role == Primary,
    ownsAnySlot(slotMap, old),
    allNodes.get(succ).member,
    allNodes.get(succ).role == Replica,
    allNodes.get(succ).parent == Some(old),
  }
}
