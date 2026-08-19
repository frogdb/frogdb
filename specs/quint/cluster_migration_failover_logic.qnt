// cluster_migration_failover_logic — the pure logic of the migration x failover
// composite model (formal-spec phase 2, task 2): every guard (`can*`), every update
// (`apply*`), and the small helpers they share. Each takes the state it reads as an
// explicit argument, so this module holds no `var` and is unit-testable in isolation.
//
// Reworked for the issue-31 redesign (design doc's "Quint rework" section). The
// superseded source-side *repatriation* logic (`canCompleteRepatriation`, the
// `cancelling` record field, the `repatriating` phase threaded through every helper) is
// gone: the redesign is source-authoritative-until-commit, so the source simply keeps
// its copy — and its authority — until the target's promotion is *attested*, at which
// point `reapSlots` (ext-7/ext-12) is the only thing that may delete it.
//
// Rows whose ruled semantics are encoded here: TR-CLUSTER-003 (RemoveNode fence),
// TR-CLUSTER-004 (SetRole), TR-CLUSTER-008/009 (Assign/RemoveSlots), TR-CLUSTER-010
// (BeginSlotMigration), TR-CLUSTER-011 (PrepareSlotHandoff), TR-CLUSTER-012
// (ConfirmSlotHandoffDrained), TR-CLUSTER-013 (CompleteSlotMigration), TR-CLUSTER-014
// (AbortSlotHandoff), TR-CLUSTER-015 (CancelSlotMigration), TR-CLUSTER-017/018/019/042
// (the shared failover postcondition, the per-object fence and the belt), TR-CLUSTER-021
// (successor selection's structural precondition), TR-CLUSTER-033/035 and TR-CLUSTER-034
// (handoff barrier arm/release). Per-definition comments carry the ruling/amendment or
// the redesign extension (`ext-N`) each encodes; the model-wide header lives in
// cluster_migration_failover.qnt.
//
// Single responsibility: the pure, state-argument-taking half of the model.
//   Belongs here:     `can<Transition>` admission guards, `apply<Transition>` postcondition
//                     updates, the `is*`/`has*`/`*Ok` predicates they are built from, and
//                     the small shared helpers — every one a `pure def` over explicitly
//                     passed state.
//   Does NOT belong:  `var` declarations, `action`s, frame keepers, `init`/`step`
//                     (cluster_migration_failover_machine.qnt); invariants, witnesses and
//                     `run` tests (cluster_migration_failover.qnt); type and constant
//                     declarations (the two `*types` modules).
//
// Naming convention (shared with cluster_admission_logic.qnt):
//   `can<Transition>` — the admission guard of the machine action of the same name. One
//                       per admissible transition; the action's job is then "check
//                       `can*`, write `apply*`".
//   `apply<X>`        — a pure postcondition update returning the new value(s).
//   `is*` / `has*`    — a standalone state predicate, reusable in several guards.
//   `*Ok`             — a named *conjunct* of a larger admission guard (`fenceOk`,
//                       `beltOk`, `identityOrderOk`, `demotionFenceOk`), never a guard in
//                       its own right.
//   `*Of` / `*sOf`    — a derived set or projection (`movedSlotsOf`, `replicasOf`, ...).
module cluster_migration_failover_logic {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"

  // The bundle `applyFailoverCommon` (and the demotion transitions that share its
  // postcondition) read and rewrite. Grouped as one record so the postcondition is a
  // single pure function over the whole affected footprint rather than eight parallel
  // out-parameters.
  type ClusterView = {
    nodes: NodeId -> NodeState,
    slots: SlotId -> Option[NodeId],
    migrations: SlotId -> Option[Migration],
    residue: SlotId -> Option[Residue],
    armed: Set[SlotId],
    disconnected: Set[SlotId],
    feed_bytes: SlotId -> int,
    held: NodeId -> Set[SlotId],
    epoch: int,
  }

  // ---------------------------------------------------------------------------
  // Small pure helpers.
  // ---------------------------------------------------------------------------

  pure def max2(a: int, b: int): int = if (a > b) a else b

  pure def ownsAnySlot(slotMap: SlotId -> Option[NodeId], n: NodeId): bool =
    SLOTS.exists(s => slotMap.get(s) == Some(n))

  // ---------------------------------------------------------------------------
  // Node-shape predicates — the three phrases nearly every guard below is built out of.
  // Named once so a guard reads as spec prose ("a live primary that still owns a slot")
  // rather than as an open-coded conjunction repeated a dozen times, and so a change to
  // what "live primary" means is one edit rather than a sweep.
  // ---------------------------------------------------------------------------

  // A current member holding the Primary role. The single most-repeated phrase in the
  // model: it is the shape `Complete` requires of its target (V6-C2/V10-C3), the shape
  // every release action requires of its acting source (V11-C1), the shape `beltOk` and
  // `canDemoteNode` require of the node being demoted, and the shape `shardPrimary` walks
  // for.
  pure def isLivePrimary(allNodes: NodeId -> NodeState, n: NodeId): bool =
    allNodes.get(n).member and allNodes.get(n).role == Primary

  // A live primary that still owns at least one slot — the shape a failover/demotion has
  // something to *move*, and the arming precondition of the failover fence.
  pure def isSlotOwningPrimary(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                               n: NodeId): bool =
    isLivePrimary(allNodes, n) and ownsAnySlot(slotMap, n)

  // `succ` is a current member, a Replica, and parented at `p` — the *shard relationship*
  // both `beltOk` and `canDemoteNode` require of a successor. Checking it directly is
  // V9-C1's rule: a cross-shard successor is refused at admission, never derived by a
  // post-write `shardPrimary` walk.
  pure def isInShardReplicaOf(allNodes: NodeId -> NodeState, succ: NodeId, p: NodeId): bool = and {
    allNodes.get(succ).member,
    allNodes.get(succ).role == Replica,
    allNodes.get(succ).parent == Some(p),
  }

  // The slots a failover/demotion of `old` moves — the set the graceful barrier must
  // have covered (TR-CLUSTER-018's "Ownership barrier armed over the moved slots").
  pure def movedSlotsOf(slotMap: SlotId -> Option[NodeId], old: NodeId): Set[SlotId] =
    SLOTS.filter(s => slotMap.get(s) == Some(old))

  // The demotion postcondition, stated *once* and read by the defect ghosts of every
  // failover/demotion action: the old primary is a replica of the successor, still a
  // member (issue 20: demote, don't remove), and the successor is a primary.
  pure def demotedTo(allNodes: NodeId -> NodeState, old: NodeId, succ: NodeId): bool = and {
    allNodes.get(old).role == Replica,
    allNodes.get(old).parent == Some(succ),
    allNodes.get(old).member,
    allNodes.get(succ).role == Primary,
  }

  // The primary of `n`'s shard: itself when it is a live primary, else its parent when
  // that is a live primary. `retargetSlotResidue` (ext-12) is level-triggered on this
  // being a live primary other than the entry's current `source`.
  pure def shardPrimary(allNodes: NodeId -> NodeState, n: NodeId): Option[NodeId] =
    if (isLivePrimary(allNodes, n)) Some(n)
    else match allNodes.get(n).parent {
      | Some(p) => if (isLivePrimary(allNodes, p)) Some(p) else None
      | None => None
    }

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

  // Slots whose migration names `node` as its **source** — the set ext-15's identity
  // field write cancels (see `cancelledBySource`), and the set whose hold region that
  // cancel has to release.
  pure def slotsSourcedBy(migs: SlotId -> Option[Migration], node: NodeId): Set[SlotId] =
    SLOTS.filter(s => match migs.get(s) {
      | Some(m) => m.source == node
      | None => false
    })

  // ext-15 (V10-M1), the replicated form of "a source restart aborts the migration":
  // applying a run-identity **field write** for a node that is the *source* of any open
  // migration cancels those migrations. Positions are denominated in the source's own
  // history, so its incarnation change invalidates them. The cancel binds to the field
  // write itself, never to mere admission — a re-proposal at equality changes no identity
  // and cancels nothing, which is why the caller computes the write/no-write distinction
  // rather than calling this on every admitted report.
  pure def cancelledBySource(migs: SlotId -> Option[Migration], node: NodeId): SlotId -> Option[Migration] =
    SLOTS.fold(migs, (acc, s) =>
      match migs.get(s) {
        | Some(m) => if (m.source == node) acc.set(s, None) else acc
        | None => acc
      }
    )

  // ext-16/ext-17, the single funnel every `primary_id` write in this model goes
  // through: the three companion stamps (`synced`, `admitted_stage`, `promoted_from`)
  // are written in the same step, and the parenting token `parent_seq` is incremented in
  // that same step (V16-C1). Keying the rule on the *parent pointer* rather than on
  // `role` is V15-C1's correction — the two LOCKED pointer-only writers (`removeNode`'s
  // detach and the failover sibling re-parent) escaped the role-keyed form, and both go
  // through this function here.
  pure def writeParentPointer(ns: NodeState, role: RoleTag, parent: Option[NodeId],
                              synced: bool, admitted: Option[int], promotedFrom: Option[NodeId]): NodeState =
    { ...ns, role: role, parent: parent, synced: synced, admitted_stage: admitted,
      promoted_from: promotedFrom, parent_seq: ns.parent_seq + 1 }

  // ---------------------------------------------------------------------------
  // Nondet-biasing helpers — narrow the candidate set `step` samples from to
  // state-derived "currently interesting" values when that set is nonempty, falling back
  // to the full domain otherwise. This changes only which transitions the randomized
  // walk is *likely* to try, never which transitions are legal — no guard or
  // postcondition depends on these. See quint-modeling's "narrower value space with
  // fallback" pattern.
  // ---------------------------------------------------------------------------

  pure def migrationSourcesOf(migs: SlotId -> Option[Migration]): Set[NodeId] =
    SLOTS.fold(Set(), (acc, s) => match migs.get(s) {
      | Some(m) => acc.union(Set(m.source))
      | None => acc
    })

  // Slots with an open migration, a shadow or a residue entry — the slots the
  // migration-lifecycle, promotion and reaper actions can act on.
  pure def openMigrationSlots(migs: SlotId -> Option[Migration], sh: SlotId -> Option[ShadowTag],
                              res: SlotId -> Option[Residue]): Set[SlotId] =
    SLOTS.filter(s => migs.get(s) != None or sh.get(s) != None or res.get(s) != None)

  // Current members who are a Replica of `n` — biases failover's successor pick toward
  // a value `beltOk` can actually accept, instead of an independent 1-in-4 uniform pick.
  pure def replicasOf(allNodes: NodeId -> NodeState, n: NodeId): Set[NodeId] =
    NODES.filter(m => allNodes.get(m).member and allNodes.get(m).role == Replica and allNodes.get(m).parent == Some(n))

  // Owners of the slots currently held in `failover_fence` — biases `step`'s shared `n1`
  // toward the node a just-armed barrier belongs to (the fence is single-shot).
  pure def fencedOwners(fence: Set[SlotId], slotMap: SlotId -> Option[NodeId]): Set[NodeId] =
    fence.fold(Set(), (acc, s) => match slotMap.get(s) {
      | Some(n) => acc.union(Set(n))
      | None => acc
    })

  // ---------------------------------------------------------------------------
  // Membership / identity logic.
  // ---------------------------------------------------------------------------

  // ext-15's boot-ordering rule as a guard: a node proposes no other action until its
  // boot report has been applied. (`reportRunIdentity` itself is the exception, and
  // membership-level actions are proposed *about* a node rather than by it.)
  pure def canAct(allNodes: NodeId -> NodeState, n: NodeId): bool =
    allNodes.get(n).member and allNodes.get(n).booted

  // ext-11's target-side escapes, named because every target-driven action repeats them:
  // `silent` is the alive-but-unable-to-*act* mirror (it may not propose a replicated
  // report at all), and `cannot_apply` additionally stops it ingesting locally. Two names
  // rather than one because `reportTargetIngest` is gated by only the first — a target that
  // cannot apply may still truthfully report what it already holds.
  pure def canTargetAct(allNodes: NodeId -> NodeState, t: NodeId): bool =
    canAct(allNodes, t) and not(allNodes.get(t).silent)

  pure def canTargetApply(allNodes: NodeId -> NodeState, t: NodeId): bool =
    canTargetAct(allNodes, t) and not(allNodes.get(t).cannot_apply)

  // ext-10 (V8-C1): the join gate. A node whose main keyspace is non-empty is refused —
  // with the self-cleanse gone, an untracked stale copy must never enter the membership.
  pure def canMeetNode(allNodes: NodeId -> NodeState, n: NodeId): bool = and {
    not(allNodes.get(n).member),
    allNodes.get(n).keys == Set(),
  }

  // ext-10 (V8-C1): the same non-empty refusal on the reset path.
  pure def canResetCluster(allNodes: NodeId -> NodeState, n: NodeId): bool = and {
    allNodes.get(n).member,
    allNodes.get(n).keys == Set(),
  }

  // TR-CLUSTER-004, ruled (issue 14): a Replica's parent must be a present Primary; a
  // Primary's own parent pointer must be empty (INV-REF-4's structural counterpart).
  pure def canSetRole(allNodes: NodeId -> NodeState, n: NodeId, role: RoleTag, parent: Option[NodeId]): bool =
    allNodes.get(n).member and (
      if (role == Primary) parent == None
      else match parent {
        | Some(p) => p != n and isLivePrimary(allNodes, p)
        | None => true
      }
    )

  // TR-CLUSTER-003, ruled + amended (issue 20): refused unless the node is already
  // demoted off every slot it owns and out of the primary role, unless `force`
  // bypasses the fence.
  pure def canRemoveNode(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId], n: NodeId, force: bool): bool =
    allNodes.get(n).member and (force or (allNodes.get(n).role == Replica and not(ownsAnySlot(slotMap, n))))

  // ext-15: `ReportRunIdentity`'s ordering conjunct — the report's (incarnation,
  // identity_seq) pair must strictly dominate the stored one. A replayed or reordered
  // report from earlier in the boot is refused, never a migration cancel.
  pure def identityOrderOk(stored: Option[RunIdentity], inc: int, seq: int): bool =
    match stored {
      | Some(st) => inc > st.inc or (inc == st.inc and seq > st.seq)
      | None => true
    }

  // ext-15 (V8-C2): the Demotion fence. The payload carries the role/epoch it observed;
  // apply refuses when either has moved since. `inv_promotion_is_not_reverted_without_a_
  // failover` is what this defends.
  pure def demotionFenceOk(ns: NodeState, obsRole: RoleTag, obsEpoch: int): bool =
    ns.role == obsRole and ns.epoch_own == obsEpoch

  // ext-16: the refusal class is the **first failing conjunct in the declared order** —
  // membership, then ordering, then the Demotion fence — as a committed apply outcome,
  // so the disposition arms are guarded on a value rather than on a paraphrase.
  pure def refusalClassOf(allNodes: NodeId -> NodeState, n: NodeId, kind: TransitionKind,
                          inc: int, seq: int, obsRole: RoleTag, obsEpoch: int): RefusalClass =
    if (not(allNodes.get(n).member)) Membership
    else if (not(identityOrderOk(allNodes.get(n).stored_identity, inc, seq))) Ordering
    else if (kind == Demotion and not(demotionFenceOk(allNodes.get(n), obsRole, obsEpoch))) Fenced
    else Whole

  // ext-18: an `Ordering` refusal against an **absent stored identity** (arm 4b) and a
  // `Membership` refusal (arm 5) are the design's declared *terminal* classes — the ones
  // that must clear the record, drop the fence and pay the client.
  pure def isRefusalTerminal(stored: Option[RunIdentity], class: RefusalClass): bool =
    class == Membership or (class == Ordering and stored == None)

  // ext-18: the paired record-binding conjunct. A refusal is disposed against the
  // record only when **both** halves of the pair match; a refusal minted for a dead
  // stage can therefore never bind a live record minted after a forget-and-rejoin.
  pure def refusalBinds(rec: TransitionRecord, r: Refusal): bool =
    r.stage_id == rec.stage_id and r.reg_seq == rec.staged_reg_seq

  // ext-18: the clearing clause, level-triggered — enabled whenever the record's staged
  // registration no longer matches the live cell (or the node has left the membership).
  pure def isRecordStale(ns: NodeState): bool =
    match ns.record {
      | Some(rec) => ns.registration_seq != Some(rec.staged_reg_seq) or not(ns.member)
      | None => false
    }

  // ---------------------------------------------------------------------------
  // Slot-map logic.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-008, ruled (issue 16): refused outright while a migration is open for
  // the slot — no mutation-while-migrating. Extended for ext-12: a slot carrying a
  // residue entry is likewise not re-assignable until the entry is cleared.
  pure def canAssignSlot(allNodes: NodeId -> NodeState, migs: SlotId -> Option[Migration],
                         res: SlotId -> Option[Residue], s: SlotId, n: NodeId): bool = and {
    migs.get(s) == None,
    res.get(s) == None,
    allNodes.get(n).member,
  }

  // TR-CLUSTER-009, same ruled precondition as TR-CLUSTER-008 (issue 16).
  pure def canRemoveSlot(migs: SlotId -> Option[Migration], res: SlotId -> Option[Residue], s: SlotId): bool =
    migs.get(s) == None and res.get(s) == None

  // ---------------------------------------------------------------------------
  // Migration lifecycle.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-010 + ext-12's residue guard: `slot`'s current owner is `source`, no
  // existing migration entry, and **no residue entry over the slot** — a marked
  // (`source_gone`/`target_gone`) entry blocks `Begin` exactly as an unmarked one does
  // (V7's kept-entry semantics; `witnessBeginRefusedOverResidue` is its witness).
  // No target-role precondition — see the main file's header note on why.
  pure def canBeginMigration(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                             migs: SlotId -> Option[Migration], res: SlotId -> Option[Residue],
                             s: SlotId, source: NodeId, target: NodeId): bool = and {
    slotMap.get(s) == Some(source),
    migs.get(s) == None,
    res.get(s) == None,
    source != target,
    allNodes.get(target).member,
  }

  // ext-14 (V6-C2): `requireReplicaAck` is read from the *proposer's* config exactly
  // once, here, and stamped into the record. Every later predicate reads the field.
  pure def newMigration(id: MigrationId, source: NodeId, target: NodeId,
                        sourceRun: RunId, targetRun: RunId, requireReplicaAck: bool): Migration = {
    id: id,
    source: source,
    target: target,
    attempt: 0,
    phase: Streaming,
    fenced: false,
    prepare_run: sourceRun,
    source_log: { run_id: sourceRun, pos: 0 },
    drained_pos: None,
    target_copy: { run_id: targetRun, pos: 0 },
    target_applied: 0,
    target_durable: 0,
    target_replica_ack: 0,
    observations: 0,
    last_observation: None,
    preconfirm_bound: PRECONFIRM_OBSERVATIONS,
    draining_bound: DRAINING_OBSERVATIONS,
    require_replica_ack: requireReplicaAck,
  }

  // TR-CLUSTER-011: migration exists. No "no live handoff" precondition: the ruled row
  // text does not carry one, and requiring it made the supersede path (FM-CLUSTER-085)
  // and TR-CLUSTER-012's stale-seq refusal unreachable — see the header note.
  pure def canPrepareHandoff(migs: SlotId -> Option[Migration], s: SlotId): bool =
    migs.get(s) != None

  // TR-CLUSTER-011/034 + ext-2/ext-3: mints the attempt stamp from the bumped counter,
  // arms the barrier at the source and **seals the source** (`fenced`), which is a
  // separate fact from `phase` — the seal is what `inv_complete_requires_fenced_source`
  // reads and what `localReleaseOnCapBreach` can drop without touching the phase.
  // ext-1 (V4-C2): the prepare carries the source's *current* run tag.
  pure def applyPrepareHandoff(migs: SlotId -> Option[Migration], armed: Set[SlotId],
                               s: SlotId, newSeq: int, sourceRun: RunId): { migs: SlotId -> Option[Migration], armed: Set[SlotId] } =
    match migs.get(s) {
      | Some(m) => {
          migs: migs.set(s, Some({ ...m, attempt: newSeq, fenced: true, phase: Streaming, prepare_run: sourceRun })),
          armed: armed.union(Set(s)),
        }
      | None => { migs: migs, armed: armed }
    }

  // TR-CLUSTER-012 + ext-1/ext-3/ext-13: the prepared attempt matches (stale acks from a
  // superseded attempt are refused), the source is sealed, the phase has not already
  // sealed, and the source is able to propose the drain at all (`cannot_drain` is
  // ext-13's alive-but-never-confirms escape). The proposed position is drawn from the
  // same tagged space as `source_log` and carries the proposer's current run.
  pure def canConfirmDrained(allNodes: NodeId -> NodeState, migs: SlotId -> Option[Migration],
                             s: SlotId, attempt: int, p: Pos): bool =
    match migs.get(s) {
      | Some(m) => and {
          m.attempt == attempt,
          m.attempt > 0,
          m.phase == Streaming,
          m.fenced,
          not(allNodes.get(m.source).cannot_drain),
          p.run_id == allNodes.get(m.source).run_id,
          p.pos == m.source_log.pos,
        }
      | None => false
    }

  // TR-CLUSTER-012 postcondition + ext-4 (V7-C3): the seal **resets the observation
  // counter** and clears `last_observation`, so the post-`Confirm` bound is not consumed
  // by pre-seal ticks (the 30->3 collapse at stock defaults is the failure trace of
  // reverting this).
  pure def applyConfirmDrained(migs: SlotId -> Option[Migration], s: SlotId, p: Pos): SlotId -> Option[Migration] =
    match migs.get(s) {
      | Some(m) => migs.set(s, Some({ ...m, phase: Draining, drained_pos: Some(p),
                                      observations: 0, last_observation: None }))
      | None => migs
    }

  // ext-14: the counted target replicas of `t`. The **empty counted set is false**, not
  // vacuously true (V5-M3) — with the knob on and nothing counted, `Complete` must be
  // refused and the migration must exit via the observation bound.
  pure def countedReplicasOf(allNodes: NodeId -> NodeState, t: NodeId): Set[NodeId] =
    NODES.filter(n => allNodes.get(n).member and allNodes.get(n).counted and allNodes.get(n).parent == Some(t))

  // `Complete`'s `record.run_id == nodes[record.source].run_identity` conjunct: the run
  // the record's own log tag was minted under must still be the source's **replicated**
  // identity. Deliberately read off `stored_identity` and not off the node's live
  // `run_id`: the window in which a source has physically restarted but its boot report
  // has not yet applied is a *declared* one — the replicated field still says the old run,
  // so `Complete` may still admit there. What closes the window is the field write itself
  // (`cancelledBySource`), which takes the record; this conjunct is belt-and-braces over
  // it, catching the record whose tag was minted under a run the replicated cell has since
  // moved off. A node with no stored identity has no run to match, so it is refused.
  pure def sourceRunIdentityOk(allNodes: NodeId -> NodeState, m: Migration): bool =
    match allNodes.get(m.source).stored_identity {
      | Some(st) => st.run_id == m.source_log.run_id
      | None => false
    }

  // TR-CLUSTER-013, ruled (issue 17) + ext-1/2/3/5/12/14. Admitted on:
  //   - the attempt stamp matching (ext-3),
  //   - the phase being Draining (ext-3, `inv_complete_requires_draining_phase`),
  //   - the source still being sealed (ext-2, `inv_complete_requires_fenced_source`),
  //   - the target holding the drained position, compared **within one tag space**
  //     (ext-1/V4-C2) and attested by the target's **current** run (ext-5/V8-C3),
  //   - the record's own source-log tag still being the source's replicated run identity
  //     (`sourceRunIdentityOk` — the design's `record.run_id ==
  //     nodes[record.source].run_identity`),
  //   - the counted target replicas holding it too when the knob is on (ext-14),
  //   - the target being a Primary (ext-12/V10-C3 — dropping this conjunct and demoting
  //     the target mid-Draining lands a slot on a Replica).
  // No wall-clock `barrier_ms` window appears anywhere in this guard.
  pure def canCompleteMigration(allNodes: NodeId -> NodeState, migs: SlotId -> Option[Migration],
                       s: SlotId, attempt: int): bool =
    match migs.get(s) {
      | Some(m) => and {
          m.attempt == attempt,
          m.attempt > 0,
          m.phase == Draining,
          m.fenced,
          isLivePrimary(allNodes, m.target),
          // The design's `record.run_id == nodes[record.source].run_identity`.
          sourceRunIdentityOk(allNodes, m),
          match m.drained_pos {
            | Some(d) => and {
                // ext-1 (V4-C2): the drain's position and the log it drained are compared
                // within **one** tag space — the record's own.
                d.run_id == m.source_log.run_id,
                m.target_copy.run_id == allNodes.get(m.target).run_id,
                m.target_copy.pos >= d.pos,
                // ext-14: the optional durability conjunct exists iff the migration's
                // own captured parameter says so — never a config read at admission.
                not(m.require_replica_ack) or and {
                  countedReplicasOf(allNodes, m.target) != Set(),
                  m.target_replica_ack >= d.pos,
                },
              }
            | None => false
          },
        }
      | None => false
    }

  // ext-15 (V11-C1)'s exit-row precedence: every held-set release action carries the
  // serving-primary guard — the acting source must still own the slot and still be a
  // Primary. Dropping it from either release action and scheduling it after a demotion
  // apply executes held writes on a demoted source (forked history).
  pure def isServingPrimary(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                          m: Migration, s: SlotId): bool = and {
    slotMap.get(s) == Some(m.source),
    isLivePrimary(allNodes, m.source),
  }

  // TR-CLUSTER-014, ruled (issue 17): the prepared attempt exists with matching stamp;
  // log-ordered admission, no wall-clock window. Releases the handoff only — the
  // migration itself survives.
  pure def canAbortHandoff(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                           migs: SlotId -> Option[Migration], s: SlotId, attempt: int): bool =
    match migs.get(s) {
      | Some(m) => and { m.attempt == attempt, m.attempt > 0, isServingPrimary(allNodes, slotMap, m, s) }
      | None => false
    }

  // TR-CLUSTER-015, reworked: with the source authoritative until commit there is
  // nothing to stream back — Cancel simply removes the record (the target's shadow is
  // discarded separately by `discardShadow`, whose guard is §5's stated predicate).
  // ext-3 stamps it with the attempt so a stale cancel is refused.
  pure def canCancelMigration(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                     migs: SlotId -> Option[Migration], s: SlotId, attempt: int): bool =
    match migs.get(s) {
      | Some(m) => m.attempt == attempt and isServingPrimary(allNodes, slotMap, m, s)
      | None => false
    }

  // ---------------------------------------------------------------------------
  // ext-4: the observation counter and its bound.
  // ---------------------------------------------------------------------------

  pure def observationBound(m: Migration): int =
    if (m.phase == Streaming) m.preconfirm_bound else m.draining_bound

  // The **narrowed** reset (V5-M1): only *actual* ingest progress resets the counter.
  // v4's "(or `drained_pos` unset)" arm is gone — it made a source that never proposes
  // `Confirm` (`sourceCannotDrain`) wedge the counter forever.
  pure def hasObservedProgress(m: Migration): bool =
    match m.last_observation {
      | Some(p) => m.target_copy.pos > p
      | None => true
    }

  pure def applyReconcileTick(m: Migration): Migration =
    if (hasObservedProgress(m)) { ...m, observations: 0, last_observation: Some(m.target_copy.pos) }
    else { ...m, observations: m.observations + 1 }

  pure def isAbortableByBound(m: Migration): bool = m.observations >= observationBound(m)

  // ---------------------------------------------------------------------------
  // ext-7/ext-8/ext-12: keyspace, shadow, residue.
  // ---------------------------------------------------------------------------

  // ext-7 + ext-12: the slots `reapSlots(n)` may delete from `n`'s keyspace — the slot
  // has no live record, and its residue entry names `n` as source, is **promoted**
  // (the attestation gate) and is **not** deferred by `target_gone` (V7-M6).
  pure def reapableSlots(allNodes: NodeId -> NodeState, migs: SlotId -> Option[Migration],
                         res: SlotId -> Option[Residue], n: NodeId): Set[SlotId] =
    SLOTS.filter(s => and {
      allNodes.get(n).keys.contains(s),
      migs.get(s) == None,
      match res.get(s) {
        | Some(r) => and { r.source == n, r.promoted, not(r.target_gone) }
        | None => false
      },
    })

  // ext-12: the rollback arm's own guard. A rollback hands the slot **back to the entry's
  // source**, so it is admissible only while that source is still a live primary: rolling
  // back onto a departed source lands an assigned slot on a non-member (INV-REF-1,
  // `inv_slot_owner_valid`), and onto a demoted one it lands a slot on a Replica. When the
  // source has left or been demoted the entry's recovery is `retargetSlotResidue`, which
  // re-homes it onto the shard's current primary first — which is exactly why the
  // level-triggered source arm exists.
  pure def canFailPromotion(allNodes: NodeId -> NodeState, res: SlotId -> Option[Residue],
                            s: SlotId): bool =
    match res.get(s) {
      | Some(r) => not(r.promoted) and isLivePrimary(allNodes, r.source)
      | None => false
    }

  // ext-8: §5's stated predicate — a shadow may be discarded only when no live record
  // and no residue entry claim it. Reverting the residue half is what makes
  // `inv_owner_serves_promoted_slot` fail on discard-before-promote.
  pure def canDiscardShadow(migs: SlotId -> Option[Migration], res: SlotId -> Option[Residue],
                            sh: SlotId -> Option[ShadowTag], s: SlotId): bool = and {
    sh.get(s) != None,
    migs.get(s) == None,
    res.get(s) == None,
  }

  // ext-15 (V9-M4): `ClearSlotResidue` gains the `promoted == true` conjunct. Clearing a
  // `promoted == false` entry drops the tracking of the source's still-authoritative
  // copy while the rollback arm that requires the entry is still owed.
  pure def canClearSlotResidue(res: SlotId -> Option[Residue], s: SlotId): bool =
    match res.get(s) { | Some(r) => r.promoted | None => false }

  // ext-12 (V8-C4, source arm only per V10-C4): level-triggered re-home of an entry
  // whose `source` is no longer a primary — admissible whenever that node's shard
  // primary is a live primary other than the entry's current source. The model carries
  // **no target arm**: target-side totality is in-apply at `demoteNode` (V10-C4), and
  // `residueHasAnEffectiveRemover` is what proves no level-triggered target re-home is
  // needed while that in-apply rule is intact.
  pure def canRetargetSlotResidue(allNodes: NodeId -> NodeState, res: SlotId -> Option[Residue], s: SlotId): bool =
    match res.get(s) {
      | Some(r) => match shardPrimary(allNodes, r.source) {
          | Some(p) => p != r.source
          | None => false
        }
      | None => false
    }

  // ext-12 (V7-M6): the **orphan re-home arm** — the recovery action of
  // `inv_slot_copy_survives_until_owned_and_served`. A `promoted` entry whose target has
  // departed (`target_gone`) names a slot whose owner is gone and which the reaper is
  // deliberately deferring on, so the entry is an orphan: nothing else in the model can
  // resolve it. Its two dispositions are the design's, and `dest` selects between them:
  //   - `dest == r.source` — re-assign the slot back to the source, which is refused unless
  //     the source is still a live primary **still physically holding the copy** (that
  //     copy is the whole point of the re-home; the reaper may have taken it before the
  //     target left). The entry then has nothing left to track and is removed.
  //   - any other live primary — re-assign the slot there and clear `target_gone`, which
  //     is what puts the entry back on the ordinary reap-then-clear path.
  pure def canRehomeOrphanSlot(allNodes: NodeId -> NodeState, res: SlotId -> Option[Residue],
                               s: SlotId, dest: NodeId): bool =
    match res.get(s) {
      | Some(r) => and {
          r.promoted,
          r.target_gone,
          isLivePrimary(allNodes, dest),
          dest == r.source implies allNodes.get(r.source).keys.contains(s),
        }
      | None => false
    }

  // ext-12 (V7-m3): `removeNode`'s mark-don't-remove rules — `source_gone` on a source
  // departure, `target_gone` (plus the slot-map unassign, applied by the caller) on a
  // target departure. The entry itself is **never** removed by a membership prune.
  pure def markResidueOnDeparture(res: SlotId -> Option[Residue], n: NodeId): SlotId -> Option[Residue] =
    SLOTS.fold(res, (acc, s) => match res.get(s) {
      | Some(r) =>
          if (r.source == n) acc.set(s, Some({ ...r, source_gone: true }))
          else if (r.target == n) acc.set(s, Some({ ...r, target_gone: true }))
          else acc
      | None => acc
    })

  // ext-12: the in-apply re-home a demotion (or a failover's demote arm) performs — the
  // source arm re-points at the successor and clears the mark; the **target** arm
  // re-points an *unpromoted* entry (V10-C4's in-apply target totality).
  pure def retargetResidueOnDemotion(res: SlotId -> Option[Residue], old: NodeId, succ: NodeId): SlotId -> Option[Residue] =
    SLOTS.fold(res, (acc, s) => match res.get(s) {
      | Some(r) =>
          if (r.source == old) acc.set(s, Some({ ...r, source: succ, source_gone: false }))
          else if (r.target == old and not(r.promoted)) acc.set(s, Some({ ...r, target: succ, target_gone: false }))
          else acc
      | None => acc
    })

  // ---------------------------------------------------------------------------
  // Failover / demotion logic — the postcondition shared by all three failover paths and
  // by `demoteNode`, plus the per-object fence (issue 19) and the belt they are gated by.
  // ---------------------------------------------------------------------------

  // The single postcondition shared by the planned (TR-CLUSTER-018), automatic
  // (TR-CLUSTER-019) and forced (TR-CLUSTER-042, deviated per the header note) failover
  // paths and by `demoteNode` (ext-12/V6-C3): ownership transfer, role swap with the
  // three companion stamps and the parenting token (ext-16/ext-17), sibling
  // re-parenting, epoch bump, issue 15's migration pruning, ext-12's residue re-target
  // (the promotion writer is where the residue-removal obligation rides since V14-C1
  // deleted the `Promotion` report arm), and the held-write release the exit rows owe.
  pure def applyFailoverCommon(st: ClusterView, old: NodeId, succ: NodeId): ClusterView = {
    val newEpoch = st.epoch + 1
    val movedSlots = SLOTS.filter(s => st.slots.get(s) == Some(old))
    val newSlots = SLOTS.fold(st.slots, (acc, s) => if (movedSlots.contains(s)) acc.set(s, Some(succ)) else acc)
    val reparented = NODES.fold(st.nodes, (acc, n) =>
      if (n != old and n != succ and st.nodes.get(n).parent == Some(old))
        acc.set(n, writeParentPointer(acc.get(n), acc.get(n).role, Some(succ), false, None, None))
      else acc
    )
    val demoted = reparented.set(old, writeParentPointer(reparented.get(old), Replica, Some(succ), false, None, None))
    val promotedNs = writeParentPointer(demoted.get(succ), Primary, None, true, None, Some(old))
    val promoted = demoted.set(succ, { ...promotedNs, epoch_own: newEpoch, keys: promotedNs.keys.union(movedSlots) })
    val prunedSet = slotsNaming(st.migrations, old)
    val newMigrations = prunedMigrations(st.migrations, old)
    val newArmed = st.armed.filter(s => not(prunedSet.contains(s)))
    val newFeedBytes = SLOTS.fold(st.feed_bytes, (acc, s) => if (prunedSet.contains(s)) acc.set(s, 0) else acc)
    val newDisconnected = st.disconnected.filter(s => not(prunedSet.contains(s)))
    val newHeld = NODES.fold(st.held, (acc, n) => acc.set(n, acc.get(n).exclude(prunedSet)))
    {
      nodes: promoted,
      slots: newSlots,
      migrations: newMigrations,
      residue: retargetResidueOnDemotion(st.residue, old, succ),
      armed: newArmed,
      disconnected: newDisconnected,
      feed_bytes: newFeedBytes,
      held: newHeld,
      epoch: newEpoch,
    }
  }

  // TR-CLUSTER-018/019's shared per-object fence (issue 19, amended): the proposal
  // carries the old primary's *observed* epoch/role; apply refuses if that specific
  // node's current epoch/role has since diverged from what was observed. Not a global
  // epoch check (a global fence was rejected — see the header's TR-CLUSTER-019 note).
  pure def fenceOk(ns: NodeState, curEpoch: int, obsEpoch: int, obsRole: RoleTag): bool =
    obsEpoch == curEpoch and obsRole == ns.role

  // The belt (retained through issue 19's amendment), plus ext-16's **candidacy gate**:
  // a candidate must be `synced` — a replica that has attested it holds the shard's data
  // — unless `force: true` bypasses it (V14-C3: without the gate, a whole shard can be
  // substituted by an empty candidate).
  pure def beltOk(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                  old: NodeId, succ: NodeId, requireSynced: bool): bool = and {
    isSlotOwningPrimary(allNodes, slotMap, old),
    isInShardReplicaOf(allNodes, succ, old),
    not(requireSynced) or allNodes.get(succ).synced,
  }

  // The fence-and-belt admission every failover path shares, named so the three failover
  // actions differ only by what they add: `failoverGraceful` adds the drain/parity barrier
  // (`barrierCoversMovedSlots`), `failoverAuto` the FAIL flag, `failoverForced` nothing but
  // `requireSynced: false`. The `defects.fence` ghost deliberately does NOT go through this
  // predicate — it recomputes `fenceOk` itself, so a mutant weakening the guard is still
  // caught (see `inv_last_failover_fenced`).
  pure def canApplyFailover(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                            old: NodeId, succ: NodeId, requireSynced: bool,
                            obsEpoch: int, obsRole: RoleTag): bool = and {
    beltOk(allNodes, slotMap, old, succ, requireSynced),
    fenceOk(allNodes.get(old), allNodes.get(old).epoch_own, obsEpoch, obsRole),
  }

  // Issue 26, ruled: the graceful path's extra conjunct — the drain/parity barrier must be
  // armed over **every slot about to move**, not merely armed. Both halves matter: an empty
  // fence would satisfy `subseteq` vacuously whenever the old primary owns no slots.
  pure def barrierCoversMovedSlots(slotMap: SlotId -> Option[NodeId], fence: Set[SlotId],
                                   old: NodeId): bool = and {
    fence != Set(),
    movedSlotsOf(slotMap, old).subseteq(fence),
  }

  // ext-12 (V6-C3/V9-C1): `demoteNode`'s own precondition. The successor is checked for
  // the **shard relationship** directly (a cross-shard `new_primary_id` is refused at
  // admission, never derived by a post-write `shard_primary` walk).
  pure def canDemoteNode(allNodes: NodeId -> NodeState, n: NodeId, succ: NodeId): bool = and {
    isLivePrimary(allNodes, n),
    n != succ,
    isInShardReplicaOf(allNodes, succ, n),
  }

  // ext-12 (V7-m7 + V8-C6): the successor-less demotion — role -> Replica, `primary_id`
  // -> None, no promotion in the step — with the **whole-refusal** when the node still
  // owns slots (there is no successor to re-home them to, and leaving them assigned to a
  // node about to become a Replica would violate `inv_slots_only_assigned_to_primaries`).
  pure def canDemoteNodeExternal(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                                 n: NodeId): bool = and {
    isLivePrimary(allNodes, n),
    not(ownsAnySlot(slotMap, n)),
  }

  // TR-CLUSTER-024/025's arming precondition, named for symmetry with the failover guards
  // it feeds: only a live primary that still owns slots has a drain/parity barrier to arm.
  pure def canArmFailoverFence(allNodes: NodeId -> NodeState, slotMap: SlotId -> Option[NodeId],
                               n: NodeId): bool =
    isSlotOwningPrimary(allNodes, slotMap, n)
}
