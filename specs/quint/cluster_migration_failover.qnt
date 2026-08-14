// cluster_migration_failover — the migration x failover composite model (formal-spec
// phase 2, task 2).
//
// Models the issue-15/16/17/19/20 defect class: slot migrations interleaved with
// failovers, and the ruled (target) semantics that close it. Modeled from
// specs/cluster.md's RULED rows and the issue rulings/amendments below — not today's
// code (the per-object epoch/role fence, the log-ordered handoff barrier, and the
// demote-not-remove postcondition for `force: true` are none of them implemented yet;
// `commands.rs:442` still removes `old_primary_id` outright under `force: true`).
//
// This is one Raft state machine: the model is the *applied* sequence of committed
// commands, with no network/consensus modeling — openraft's total order is assumed,
// exactly the `apply_command` boundary task 3's quint-connect harness drives.
//
// Rows modeled:
//   TR-CLUSTER-004 — SetRole: role/parent-pointer transition, gated by issue 14's
//     ruled precondition (a Replica's parent must name an existing Primary).
//   TR-CLUSTER-008/009 — AssignSlots/RemoveSlots, gated by issue 16's ruled
//     open-migration refusal (no mutation-while-migrating).
//   TR-CLUSTER-010 — BeginSlotMigration: opens a migration naming the slot's current
//     owner as source.
//   TR-CLUSTER-011 — PrepareSlotHandoff: mints a monotone `seq` from the handoff
//     counter, arms the write barrier at the source only (TR-CLUSTER-034).
//   TR-CLUSTER-012 — ConfirmSlotHandoffDrained: marks the prepared handoff drained;
//     does not itself move ownership.
//   TR-CLUSTER-013 — CompleteSlotMigration, ruled (issue 17): admitted purely on
//     matching-seq-and-drained, no wall-clock `barrier_ms` admission window.
//   TR-CLUSTER-014 — AbortSlotHandoff, ruled (issue 17): log-ordered admission, no
//     wall-clock window; the migration itself survives, only the handoff is released.
//   TR-CLUSTER-015 — CancelSlotMigration, ruled + amended (issue 15): abort is a real
//     rollback that repatriates — any data provisionally moved to the target while the
//     handoff was drained streams back to the source before the migration record is
//     removed.
//   TR-CLUSTER-016 — Replica-feed hold during an armed barrier, amended (issue 17):
//     bounded by a byte cap on buffered frames, not by the deleted wall-clock deadline;
//     breaching the cap disconnects the held session. Modeled minimally (a per-slot
//     byte counter and a cap-enforced buffer/disconnect pair of actions) to witness
//     that no wall-clock field exists anywhere the hold is decided.
//   TR-CLUSTER-017/018 — Planned (graceful) `CLUSTER FAILOVER`: TR-CLUSTER-018's
//     per-object epoch/role fence (issue 19, amended) plus the belt (refuse no-op
//     moves), applied only once TR-CLUSTER-017's ruled (issue 26) drain/parity
//     barrier — modeled as "no concurrently armed barrier survives the swap" rather
//     than a separate drain/parity sub-protocol, since this model does not carry
//     replication offsets (see "Deliberately not modeled" below).
//   TR-CLUSTER-019 — Automatic failover proposal: same fence/belt shape as
//     TR-CLUSTER-018, gated on the old primary's FAIL flag being latched, asynchronous
//     (no drain/parity wait — issue 26's ruled cost of async PSYNC).
//   TR-CLUSTER-020 — cited only as the reason this model has no
//     "primary-absorbs-primary" action: that mode is ruled removed outright, not
//     modeled as a disabled action.
//   TR-CLUSTER-021 — Failover successor selection: modeled as its structural
//     precondition only (`new_primary_id` is a replica of the failing primary); the
//     numeric priority/offset scoring formula is out of scope (see "Deliberately not
//     modeled").
//   TR-CLUSTER-024/025 — MarkNodeFailed/MarkNodeRecovered: FAIL-flag transitions,
//     including TR-CLUSTER-024's unconditional cluster-epoch bump (the reason issue
//     19's amendment moved the failover fence off the cluster-wide epoch).
//   TR-CLUSTER-034 — Handoff barrier arm/release, modeled at the applied-state level
//     as a `Set[SlotId]` rather than a `(NodeId, SlotId) -> Armed | Released` map: at
//     most one migration (hence one source) can be open per slot at a time in this
//     model's state shape, so the node coordinate is redundant with the slot key and
//     dropping it loses no information. This sidesteps modeling per-node log-apply
//     prefixes (contested area (a) in the task brief) by construction — the barrier is
//     tracked as a pure function of the replicated migration/handoff events applied so
//     far, not of any one node's local apply cursor, matching cluster.md's own
//     observation (state-space row 127) that the barrier is "reconstructible in
//     principle from the replicated migration/handoff state that drives it."
//   TR-CLUSTER-042 — Forced (`force: true`) failover. **Deviation from this row's
//     literal postcondition**: the row's text still says "Node membership loses
//     `old_primary_id` outright (removed...)" — that text predates issue 20's ruling.
//     This model applies issue 20's demote-don't-remove ruling to the forced path too,
//     per the task brief's explicit instruction, on the reasoning issue 20's amendment
//     itself supplies: removal is deliberately promoted to a separate, explicit,
//     fenced admin action (amendment item 1: `CLUSTER FORGET` of a Raft-reachable voter
//     is refused unless the node is demoted first) — never an automatic consequence of
//     a failover-shaped transition, forced or not. `inv_forced_failover_keeps_node`
//     encodes exactly this generalized claim, not TR-CLUSTER-042's stale literal text.
//     The belt-only branch for an *absent* old primary is not separately modeled: this
//     model has no action that ever un-members a node (see the deviation itself), so
//     that branch is permanently unreachable here — noted, not silently dropped.
//
// Invariant catalog `frogdb-server/crates/cluster/src/invariants.rs`:
//   INV-SLOT-1 (a slot's owner, if assigned, is a live Primary — `check_slot_1`'s
//     state-side backstop), INV-MIG-1 (`check_mig_1`: a migrating slot's owner equals
//     the migration's source), INV-HANDOFF-1/2 (`check_handoff_1/2`: the seq counter
//     dominates every live handoff's seq; a live handoff never carries the unminted
//     seq 0), INV-EPOCH-1/2 (`check_epoch_1/2`: the cluster epoch dominates every
//     node's own epoch; a nonzero node epoch is unique among Primaries).
//
// Issues (rulings + amendments this model encodes):
//   .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md
//   .scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md
//   .scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md
//   .scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md
//   .scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md
module cluster_migration_failover {
  type NodeId = int
  type SlotId = int

  // Local Option — see cluster_admission.qnt's header for why this isn't basicSpells'
  // (keeps this a single flat file per the task brief).
  type Option[a] = Some(a) | None

  type RoleTag = Primary | Replica

  type NodeState = {
    role: RoleTag,
    // "Node parent pointer" — Some(p) only meaningful when role == Replica
    // (TR-CLUSTER-004's ruled precondition, issue 14: p must exist and be a Primary).
    parent: Option[NodeId],
    // "Node membership" — false only ever reachable via a mutant of the demote path;
    // see the header's TR-CLUSTER-042 deviation note.
    member: bool,
    // "Node FAIL flag".
    fail: bool,
  }

  type Handoff = {
    // "A handoff's attempt number" — minted from the monotone handoff_seq counter.
    seq: int,
    drained: bool,
  }

  type Migration = {
    source: NodeId,
    target: NodeId,
    // "A migration's prepared handoff".
    handoff: Option[Handoff],
  }

  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)
  pure val SLOTS: Set[SlotId] = Set(1, 2, 3, 4)
  pure val HOLD_BYTE_CAP: int = 6

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
  // resolution path (Complete/Abort/Cancel/failover-pruning) must clear it. This is
  // what makes `inv_abort_repatriates` falsifiable: slot ownership itself never moves
  // before CompleteSlotMigration in this model (matching TR-CLUSTER-010's postcondition
  // — the source keeps the ownership record the whole time), so without this ghost
  // field "the slot is owned by source after abort" would be true by construction on
  // every trace, real or broken.
  var provisional_target: SlotId -> Option[NodeId]
  // TR-CLUSTER-016 amendment: bytes buffered for a replica session held behind this
  // slot's armed barrier. No wall-clock field appears anywhere in this model — the
  // hold is bounded purely by this counter and HOLD_BYTE_CAP.
  var feed_bytes: SlotId -> int
  // Ghost/observability: which slots' held replica sessions have been disconnected for
  // breaching the byte cap (TR-CLUSTER-016 amendment's "breaching it disconnects that
  // replica session rather than buffering unboundedly").
  var disconnected_feed: Set[SlotId]

  // ---------------------------------------------------------------------------
  // Deliberately not modeled.
  // ---------------------------------------------------------------------------
  //
  // - TR-CLUSTER-017/026's drain/parity sub-protocol: this model has no replication
  //   offsets, so "drained and at parity" is modeled as its outward effect only ("no
  //   concurrently armed barrier survives the swap" — `beltOk` on the failover
  //   actions), not as a separate offset-tracking state machine.
  // - TR-CLUSTER-021's numeric priority/offset successor-scoring formula: out of
  //   scope. Successor selection is modeled only by its structural precondition (the
  //   chosen `succ` must currently be a replica of the failing primary, checked by
  //   `beltOk`), not by the scoring arithmetic that picks one replica over another.
  // - TR-CLUSTER-014's FM-CLUSTER-086 "already gone -> no-op success" branch: not
  //   separately modeled. `canAbortHandoff` requires a live, seq-matching handoff;
  //   the already-resolved case (which real code accepts idempotently) is left
  //   unmodeled rather than encoded as a vacuous no-op action.
  // - `inv_no_stuck_handoff` (from any reachable state with an armed barrier, some
  //   enabled action sequence releases it): this is a temporal liveness property, not
  //   a state invariant `quint run --invariant` can check in 0.32.0. Rather than
  //   skip it silently, it is witnessed by trajectory instead: every `run` test that
  //   reaches an armed barrier (`gracefulFailoverArmedNotDrainedTest`,
  //   `gracefulFailoverDrainedNotCompleteTest`, `happyMigrationTest`,
  //   `abortRepatriatesTest`, `forcedFailoverDemotesTest`) continues on to a step
  //   that releases it — via `completeMigration`, `abortHandoff`, `cancelMigration`,
  //   or a pruning failover — and every one of those four release paths clears the
  //   slot from `armed_barriers` unconditionally (see the `armed_barriers'` update in
  //   `completeMigration`, `abortHandoff`, and `cancelMigration`, and
  //   `applyFailoverCommon`'s `newArmed` computation), so no action that resolves a
  //   migration's handoff ever leaves its barrier armed. This is a by-construction
  //   argument over the four release actions, not a model-checked temporal property —
  //   noted here rather than asserted as a checked invariant.

  // ---------------------------------------------------------------------------
  // Small pure helpers.
  // ---------------------------------------------------------------------------

  pure def max2(a: int, b: int): int = if (a > b) a else b

  pure def ownsAnySlot(slotMap: SlotId -> Option[NodeId], n: NodeId): bool =
    SLOTS.exists(s => slotMap.get(s) == Some(n))

  // The set of slots whose open migration names `node` on either leg (source or
  // target) — TR-CLUSTER-018/042's "Open migrations naming old_primary_id pruned"
  // postcondition, generalized to every failover per issue 15's ruling ("keyed to the
  // failover transition itself, NOT node removal").
  pure def slotsNaming(migs: SlotId -> Option[Migration], node: NodeId): Set[SlotId] =
    SLOTS.filter(s => match migs.get(s) {
      | Some(m) => m.source == node or m.target == node
      | None => false
    })

  // Whether `node` is currently the target leg of any open migration.
  pure def isMigrationTarget(migs: SlotId -> Option[Migration], node: NodeId): bool =
    SLOTS.exists(s => match migs.get(s) {
      | Some(m) => m.target == node
      | None => false
    })

  // Drops every migration entry naming `node` (issue 15's prune).
  pure def prunedMigrations(migs: SlotId -> Option[Migration], node: NodeId): SlotId -> Option[Migration] =
    SLOTS.fold(migs, (acc, s) =>
      match migs.get(s) {
        | Some(m) => if (m.source == node or m.target == node) acc.set(s, None) else acc
        | None => acc
      }
    )

  // Impure (reads the `armed_barriers` state var directly) — kept separate from the
  // pure helpers above so the purity checker doesn't reject it.
  def s1_in_armed(s: SlotId): bool = armed_barriers.contains(s)

  // ---------------------------------------------------------------------------
  // Functional logic (guards + updates), pure and unit-testable in isolation.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-004, ruled (issue 14): a Replica's parent must be a present Primary; a
  // Primary's own parent pointer must be empty (INV-REF-4's structural counterpart,
  // enforced as a guard here rather than as its own invariant since the brief scopes
  // the required invariant list to SLOT/MIG/HANDOFF/EPOCH plus the two named ruling
  // invariants). Additionally refuses demoting a node to Replica while it still owns
  // slots: TR-CLUSTER-004's row text does not spell this out, but INV-SLOT-1 is
  // Hard-tier in `invariants.rs` ("the state is unreachable by any correct
  // transition") and its self-check hook runs after every transition — a bare
  // `SetRole` that demotes a slot-owning Primary without also moving its slots (the
  // way `Failover`'s combined postcondition does) is exactly the kind of transition
  // that tier promises never to admit. Caught here rather than left to be an
  // unreachable-in-practice gap: `quint run` finds this shape within its first few
  // hundred samples, which is the counterexample-protocol signal that the guard, not
  // the invariant, was wrong.
  //
  // Same reasoning extends to a second shape `quint run` found after the first fix
  // (higher --max-samples): a node can be the *target* of an open migration without
  // yet owning any slot (ownership only moves at TR-CLUSTER-013's `Complete`), so
  // `ownsAnySlot` alone does not catch a demotion of a pending migration target.
  // `Complete` is admitted purely on matching-seq-and-drained (issue 17's ruling) and
  // does not re-check the target's live role, so a demote-then-complete sequence
  // hands the slot to a non-Primary target — the same INV-SLOT-1 breach as the first
  // shape. Refusing the demotion here (rather than adding a target-liveness re-check
  // to `canComplete`) keeps `Complete`'s guard exactly as ruled and keeps this file's
  // single slot-ownership-preserving discipline in one place (`SetRole`).
  pure def canSetRole(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                       migs: SlotId -> Option[Migration], n: NodeId, role: RoleTag, parent: Option[NodeId]): bool =
    allNodes.get(n).member and (
      if (role == Primary) parent == None
      else and {
        not(ownsAnySlot(slotMap, n)),
        not(isMigrationTarget(migs, n)),
        match parent {
          | Some(p) => p != n and allNodes.get(p).member and allNodes.get(p).role == Primary
          | None => true
        },
      }
    )

  pure def applySetRole(ns: NodeState, role: RoleTag, parent: Option[NodeId]): NodeState =
    { ...ns, role: role, parent: parent }

  // TR-CLUSTER-008, ruled (issue 16): refused outright while a migration is open for
  // the slot — no mutation-while-migrating.
  pure def canAssignSlot(migs: SlotId -> Option[Migration], target: NodeState, s: SlotId): bool = and {
    migs.get(s) == None,
    target.member,
    target.role == Primary,
  }

  // TR-CLUSTER-009, same ruled precondition as TR-CLUSTER-008 (issue 16).
  pure def canRemoveSlot(migs: SlotId -> Option[Migration], s: SlotId): bool =
    migs.get(s) == None

  // TR-CLUSTER-010: `slot`'s current owner is `source`; no existing migration entry
  // for `slot` (the idempotent same-source/target re-issue is not separately modeled —
  // see "Deliberately not modeled").
  pure def canBeginMigration(slotMap: SlotId -> Option[NodeId], migs: SlotId -> Option[Migration],
                              targetNode: NodeState, s: SlotId, source: NodeId, target: NodeId): bool = and {
    slotMap.get(s) == Some(source),
    migs.get(s) == None,
    source != target,
    targetNode.member,
    targetNode.role == Primary,
  }

  pure def applyBeginMigration(migs: SlotId -> Option[Migration], s: SlotId, source: NodeId, target: NodeId): SlotId -> Option[Migration] =
    migs.set(s, Some({ source: source, target: target, handoff: None }))

  // TR-CLUSTER-011: migration exists and has no live handoff (a re-prepare after
  // AbortSlotHandoff, which clears the handoff but keeps the migration, is legal).
  pure def canPrepareHandoff(migs: SlotId -> Option[Migration], s: SlotId): bool =
    match migs.get(s) {
      | Some(m) => m.handoff == None
      | None => false
    }

  // TR-CLUSTER-011/034: mints seq from the bumped counter, arms the barrier at the
  // source (armed_barriers keyed by slot — see the header's TR-CLUSTER-034 note).
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
  pure def canAbortHandoff(migs: SlotId -> Option[Migration], s: SlotId, seq: int): bool =
    match migs.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => h.seq == seq
          | None => false
        }
      | None => false
    }

  // TR-CLUSTER-015: any open migration (with or without a live handoff) may be
  // cancelled.
  pure def canCancel(migs: SlotId -> Option[Migration], s: SlotId): bool =
    migs.get(s) != None

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
  }

  action setRole(n: NodeId, role: RoleTag, parent: Option[NodeId]): bool = all {
    canSetRole(nodes, slots, migrations, n, role, parent),
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
  }

  action prepareHandoff(s: SlotId): bool = {
    val newSeq = handoff_seq + 1
    val result = applyPrepareHandoff(migrations, armed_barriers, s, newSeq)
    all {
      canPrepareHandoff(migrations, s),
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
    }
  }

  action completeMigration(s: SlotId, seq: int): bool = {
    val target = match migrations.get(s) {
      | Some(m) => m.target
      | None => 0
    }
    all {
      canComplete(migrations, s, seq),
      slots' = slots.set(s, Some(target)),
      migrations' = migrations.set(s, None),
      armed_barriers' = armed_barriers.filter(x => x != s),
      provisional_target' = provisional_target.set(s, None),
      feed_bytes' = feed_bytes.set(s, 0),
      disconnected_feed' = disconnected_feed.filter(x => x != s),
      nodes' = nodes,
      handoff_seq' = handoff_seq,
      epoch' = epoch,
      node_epoch' = node_epoch,
    }
  }

  // TR-CLUSTER-014: releases only the handoff; the migration survives.
  action abortHandoff(s: SlotId, seq: int): bool = all {
    canAbortHandoff(migrations, s, seq),
    migrations' = match migrations.get(s) {
      | Some(m) => migrations.set(s, Some({ ...m, handoff: None }))
      | None => migrations
    },
    armed_barriers' = armed_barriers.filter(x => x != s),
    // Issue 15 amendment: repatriation — any data provisionally at the target streams
    // back to the source before this applies clean.
    provisional_target' = provisional_target.set(s, None),
    feed_bytes' = feed_bytes.set(s, 0),
    disconnected_feed' = disconnected_feed.filter(x => x != s),
    nodes' = nodes,
    slots' = slots,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
  }

  // TR-CLUSTER-015, amended (issue 15): the whole migration (and any live handoff) is
  // removed; repatriation is paid the same way as abortHandoff.
  action cancelMigration(s: SlotId): bool = all {
    canCancel(migrations, s),
    migrations' = migrations.set(s, None),
    armed_barriers' = armed_barriers.filter(x => x != s),
    provisional_target' = provisional_target.set(s, None),
    feed_bytes' = feed_bytes.set(s, 0),
    disconnected_feed' = disconnected_feed.filter(x => x != s),
    nodes' = nodes,
    slots' = slots,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
  }

  // TR-CLUSTER-016 amendment: buffer `n` bytes for the held session behind slot `s`'s
  // armed barrier, refusing once the cap would be exceeded (the cap-enforced version of
  // the hold — no wall-clock deadline anywhere in this guard).
  action feedBuffer(s: SlotId, n: int): bool = all {
    armed_barriers.contains(s),
    feed_bytes.get(s) + n <= HOLD_BYTE_CAP,
    n > 0,
    feed_bytes' = feed_bytes.set(s, feed_bytes.get(s) + n),
    nodes' = nodes,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    disconnected_feed' = disconnected_feed,
  }

  // TR-CLUSTER-016 amendment: breaching the byte cap disconnects the held session
  // rather than buffering unboundedly.
  action feedOverflowDisconnect(s: SlotId): bool = all {
    feed_bytes.get(s) >= HOLD_BYTE_CAP,
    disconnected_feed' = disconnected_feed.union(Set(s)),
    feed_bytes' = feed_bytes.set(s, 0),
    nodes' = nodes,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    epoch' = epoch,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
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
  }

  action markNodeRecovered(n: NodeId): bool = all {
    nodes.get(n).member,
    nodes' = nodes.set(n, { ...nodes.get(n), fail: false }),
    // TR-CLUSTER-025: no epoch bump (FM-CLUSTER-014 anti-churn).
    epoch' = epoch,
    slots' = slots,
    migrations' = migrations,
    handoff_seq' = handoff_seq,
    node_epoch' = node_epoch,
    armed_barriers' = armed_barriers,
    provisional_target' = provisional_target,
    feed_bytes' = feed_bytes,
    disconnected_feed' = disconnected_feed,
  }

  // The single postcondition shared by the planned (TR-CLUSTER-018), automatic
  // (TR-CLUSTER-019) and forced (TR-CLUSTER-042, deviated per the header note) failover
  // paths: ownership transfer, role swap, sibling re-parenting, epoch bump, and issue
  // 15's migration pruning. All three differ only in their *preconditions* (see the
  // wrapper actions below) — never in this postcondition, since the demote-don't-remove
  // ruling now applies uniformly.
  pure def applyFailoverCommon(
    st: { nodes: NodeId -> NodeState, slots: SlotId -> Option[NodeId], migrations: SlotId -> Option[Migration],
          epoch: int, node_epoch: NodeId -> int, armed_barriers: Set[SlotId], provisional_target: SlotId -> Option[NodeId],
          feed_bytes: SlotId -> int, disconnected_feed: Set[SlotId] },
    old: NodeId, succ: NodeId
  ): { nodes: NodeId -> NodeState, slots: SlotId -> Option[NodeId], migrations: SlotId -> Option[Migration],
       epoch: int, node_epoch: NodeId -> int, armed_barriers: Set[SlotId], provisional_target: SlotId -> Option[NodeId],
       feed_bytes: SlotId -> int, disconnected_feed: Set[SlotId] } = {
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
    }
  }

  // TR-CLUSTER-018/019's shared per-object fence (issue 19, amended): the proposal
  // carries the old primary's *observed* epoch/role; apply refuses if that specific
  // node's current epoch/role has since diverged from what was observed. Not a global
  // epoch check (a global fence was rejected — see the header's TR-CLUSTER-019 note).
  pure def fenceOk(ns: NodeState, curEpoch: int, obsEpoch: int, obsRole: RoleTag): bool =
    obsEpoch == curEpoch and obsRole == ns.role

  // The belt (retained through issue 19's amendment): refuse the move outright if the
  // old primary is not a member, owns no slots, or the new primary is not its replica.
  pure def beltOk(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId], old: NodeId, succ: NodeId): bool = and {
    allNodes.get(old).member,
    ownsAnySlot(slotMap, old),
    allNodes.get(succ).role == Replica,
    allNodes.get(succ).parent == Some(old),
  }

  // TR-CLUSTER-017/018: planned/graceful failover — old primary must currently be
  // healthy (this is the "planned, old_primary healthy" precondition; TR-CLUSTER-017's
  // ruled drain/parity barrier is encoded as "no armed barrier survives the swap",
  // which `applyFailoverCommon`'s pruning already guarantees for migrations naming the
  // old primary — see the header's TR-CLUSTER-017 note for why a full replication-offset
  // parity wait is out of scope).
  action failoverGraceful(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed }
    val result = applyFailoverCommon(st, old, succ)
    all {
      old != succ,
      beltOk(nodes, slots, old, succ),
      not(nodes.get(old).fail),
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
      handoff_seq' = handoff_seq,
    }
  }

  // TR-CLUSTER-019: automatic failover — gated on the old primary's FAIL flag being
  // latched (the leader's reconcile pass has already marked it), async-lossy (no
  // drain/parity precondition, per issue 26's ruling that the automatic path stays
  // asynchronous).
  action failoverAuto(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed }
    val result = applyFailoverCommon(st, old, succ)
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
      handoff_seq' = handoff_seq,
    }
  }

  // TR-CLUSTER-042 (`force: true`): the belt+fence shape is identical to the graceful
  // path (issue 19's amendment applies to both), but neither the old primary's health
  // flag nor a drain/parity wait gates it — force means force. Per the header's
  // deviation note, the postcondition still demotes rather than removes.
  action failoverForced(old: NodeId, succ: NodeId, obsEpoch: int, obsRole: RoleTag): bool = {
    val st = { nodes: nodes, slots: slots, migrations: migrations, epoch: epoch, node_epoch: node_epoch,
               armed_barriers: armed_barriers, provisional_target: provisional_target,
               feed_bytes: feed_bytes, disconnected_feed: disconnected_feed }
    val result = applyFailoverCommon(st, old, succ)
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
      handoff_seq' = handoff_seq,
    }
  }

  action stutter: bool = all {
    nodes' = nodes, slots' = slots, migrations' = migrations, handoff_seq' = handoff_seq,
    epoch' = epoch, node_epoch' = node_epoch, armed_barriers' = armed_barriers,
    provisional_target' = provisional_target, feed_bytes' = feed_bytes, disconnected_feed' = disconnected_feed,
  }

  action step: bool = {
    nondet s = SLOTS.oneOf()
    nondet n1 = NODES.oneOf()
    nondet n2 = NODES.oneOf()
    nondet roleGuess = Set(Primary, Replica).oneOf()
    nondet epochGuess = Set(node_epoch.get(n1), node_epoch.get(n1) + 1).oneOf()
    nondet bytesGuess = Set(1, 2, 3).oneOf()
    any {
      setRole(n1, roleGuess, if (roleGuess == Replica) Some(n2) else None),
      assignSlot(s, n1),
      removeSlot(s),
      beginMigration(s, n1, n2),
      prepareHandoff(s),
      confirmDrained(s, handoff_seq),
      completeMigration(s, handoff_seq),
      abortHandoff(s, handoff_seq),
      cancelMigration(s),
      feedBuffer(s, bytesGuess),
      feedOverflowDisconnect(s),
      markNodeFailed(n1),
      markNodeRecovered(n1),
      failoverGraceful(n1, n2, epochGuess, roleGuess),
      failoverAuto(n1, n2, epochGuess, roleGuess),
      failoverForced(n1, n2, epochGuess, roleGuess),
      stutter,
    }
  }

  // ---------------------------------------------------------------------------
  // Witnesses.
  // ---------------------------------------------------------------------------

  val witnessMigrationInFlight: bool =
    SLOTS.exists(s => migrations.get(s) != None)

  val witnessHandoffDrainedNotComplete: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => match m.handoff { | Some(h) => h.drained | None => false }
      | None => false
    })

  val witnessGracefulFailoverPrunedMigration: bool =
    SLOTS.exists(s => provisional_target.get(s) == None) and epoch > 0

  val witnessForcedFailoverDemoted: bool =
    NODES.exists(n => nodes.get(n).role == Replica and nodes.get(n).member and epoch > 0)

  val witnessFeedDisconnected: bool =
    disconnected_feed.size() > 0

  // ---------------------------------------------------------------------------
  // Invariants.
  // ---------------------------------------------------------------------------

  // INV-SLOT-1: every assigned slot's owner is a live (member) Primary.
  val inv_slot_single_owner: bool =
    SLOTS.forall(s => match slots.get(s) {
      | Some(n) => nodes.get(n).member and nodes.get(n).role == Primary
      | None => true
    })

  // INV-MIG-1: an open migration's source currently owns the slot; source != target;
  // both endpoints are members.
  val inv_migration_endpoints_valid: bool =
    SLOTS.forall(s => match migrations.get(s) {
      | Some(m) => and {
          slots.get(s) == Some(m.source),
          m.source != m.target,
          nodes.get(m.source).member,
          nodes.get(m.target).member,
        }
      | None => true
    })

  // INV-HANDOFF-1/2: a live handoff's seq is minted (>= 1) and never exceeds the
  // counter (a seq above the counter would mean the counter was rewound behind a live
  // handoff).
  val inv_handoff_owned: bool =
    SLOTS.forall(s => match migrations.get(s) {
      | Some(m) => match m.handoff {
          | Some(h) => h.seq >= 1 and h.seq <= handoff_seq
          | None => true
        }
      | None => true
    })

  // INV-EPOCH-1/2: the cluster epoch dominates every node's own epoch; a nonzero node
  // epoch is unique among current Primaries.
  val inv_epoch_monotone: bool = and {
    epoch >= NODES.fold(0, (acc, n) => max2(acc, node_epoch.get(n))),
    NODES.forall(n1 => NODES.forall(n2 =>
      (n1 != n2 and nodes.get(n1).role == Primary and nodes.get(n2).role == Primary and node_epoch.get(n1) != 0)
        implies node_epoch.get(n1) != node_epoch.get(n2)
    )),
  }

  // Issue 15, ruled + amended: once a slot has no live handoff (its migration was
  // aborted/cancelled/completed, or never had one), no repatriation residue remains —
  // no provisional-at-target ghost claim, and no armed barrier still names that slot.
  // This is the "abort = rollback with repatriation" claim, made falsifiable by the
  // `provisional_target` ghost field (see its declaration comment for why a plain
  // "slot owned by source" check would be vacuous in this model's shape).
  val inv_abort_repatriates: bool =
    SLOTS.forall(s => {
      val liveHandoff = match migrations.get(s) {
        | Some(m) => m.handoff != None
        | None => false
      }
      not(liveHandoff) implies (provisional_target.get(s) == None and not(armed_barriers.contains(s)))
    })

  // Issue 20, ruled: no failover — planned, automatic, or forced — ever un-members the
  // old primary. Encodes the header's TR-CLUSTER-042 deviation as a checkable claim:
  // demote-don't-remove applies uniformly, not just to the graceful/automatic paths.
  val inv_forced_failover_keeps_node: bool =
    NODES.forall(n => nodes.get(n).member)

  // TR-CLUSTER-016 amendment: the byte-cap hold is honored — no slot's buffered bytes
  // ever exceed the cap (a byte-cap breach must flow through feedOverflowDisconnect,
  // which resets the counter, rather than accumulating past it).
  val inv_feed_hold_bounded: bool =
    SLOTS.forall(s => feed_bytes.get(s) <= HOLD_BYTE_CAP)

  // ---------------------------------------------------------------------------
  // Tests.
  // ---------------------------------------------------------------------------

  // (a) Full happy migration: begin -> prepare -> confirm drained -> complete.
  run happyMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .expect(migrations.get(1) == Some({ source: 1, target: 2, handoff: None }))
      .then(prepareHandoff(1))
      .expect(and {
        s1_in_armed(1),
        match migrations.get(1) { | Some(m) => match m.handoff { | Some(h) => h.seq == 1 and not(h.drained) | None => false } | None => false },
      })
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(completeMigration(1, 1))
      .expect(and {
        slots.get(1) == Some(2),
        migrations.get(1) == None,
        not(s1_in_armed(1)),
        provisional_target.get(1) == None,
      })
  }

  // (b1) Graceful failover before any handoff was ever prepared: migration open, no
  // barrier armed yet; failover of the source still prunes the migration.
  run gracefulFailoverBeforePrepareTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
        nodes.get(3).role == Primary,
      })
  }

  // (b2) Graceful failover while the handoff is armed but not drained: the barrier
  // must be released by the prune, not left orphaned.
  run gracefulFailoverArmedNotDrainedTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .expect(s1_in_armed(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        not(s1_in_armed(1)),
        nodes.get(1).role == Replica,
        nodes.get(1).member,
      })
  }

  // (b3) Graceful failover while the handoff is drained but not yet completed: the
  // provisional-at-target ghost claim must also be repatriated by the prune.
  run gracefulFailoverDrainedNotCompleteTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        not(s1_in_armed(1)),
        provisional_target.get(1) == None,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
      })
  }

  // (c) Abort mid-handoff -> repatriation, without any failover involved.
  run abortRepatriatesTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(abortHandoff(1, 1))
      .expect(and {
        migrations.get(1) != None,
        match migrations.get(1) { | Some(m) => m.handoff == None | None => false },
        provisional_target.get(1) == None,
        not(s1_in_armed(1)),
      })
  }

  // (d) Forced failover mid-migration -> demote path, not removal.
  run forcedFailoverDemotesTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(failoverForced(1, 3, 0, Primary))
      .expect(and {
        nodes.get(1).role == Replica,
        nodes.get(1).member,
        nodes.get(1).parent == Some(3),
        nodes.get(3).role == Primary,
        migrations.get(1) == None,
        not(s1_in_armed(1)),
      })
  }

  // The per-object fence (issue 19, amended) refuses a stale observation: node 3's
  // epoch has moved on (it was just promoted) since a proposal that still thinks it is
  // at epoch 0 observed it. After `failoverGraceful(1, 3, ...)`, node 1 becomes node
  // 3's replica (siblings re-parent to the new primary), so `failoverGraceful(3, 1, ...)`
  // passes the belt (1 is 3's replica, 3 owns slots) but must fail the fence.
  run failoverRefusesStaleFenceTest: bool = {
    init
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        node_epoch.get(3) == 1,
        nodes.get(1).role == Replica,
        nodes.get(1).parent == Some(3),
      })
      // A stale proposal that still thinks node 3 is at epoch 0 (its value before the
      // promotion above) must be refused by the fence, even though the belt passes.
      .then(failoverGraceful(3, 1, 0, Primary).fail())
  }

  // TR-CLUSTER-016 amendment: byte-cap hold overflow disconnects the session rather
  // than buffering unboundedly.
  run feedHoldOverflowDisconnectsTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(feedBuffer(1, 3))
      .then(feedBuffer(1, 3))
      .expect(feed_bytes.get(1) == 6)
      .then(feedOverflowDisconnect(1))
      .expect(and { feed_bytes.get(1) == 0, disconnected_feed.contains(1) })
  }

  // The byte cap itself is enforced by the guard — buffering past HOLD_BYTE_CAP is not
  // a legal step at all (`.fail()` is the terminal step of this trace).
  run feedHoldRefusesPastCapTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(feedBuffer(1, 3))
      .then(feedBuffer(1, 3))
      .expect(feed_bytes.get(1) == 6)
      .then(feedBuffer(1, 1).fail())
  }

}
