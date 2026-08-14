// cluster_migration_failover — the migration x failover composite model (formal-spec
// phase 2, task 2).
//
// Models the issue-15/16/17/19/20/26 defect class: slot migrations interleaved with
// failovers, and the ruled (target) semantics that close it, plus TR-CLUSTER-003's
// (issue 20 amendment) eviction fence. Modeled from specs/cluster.md's RULED rows and
// the issue rulings/amendments below — not today's code (the per-object epoch/role
// fence, the log-ordered handoff barrier, the drain/parity barrier and the
// demote-not-remove postcondition for `force: true` are none of them implemented yet;
// `commands.rs:442` still removes `old_primary_id` outright under `force: true`).
//
// This is one Raft state machine: the model is the *applied* sequence of committed
// commands, with no network/consensus modeling — openraft's total order is assumed,
// exactly the `apply_command` boundary task 3's quint-connect harness drives.
//
// Rows modeled:
//   TR-CLUSTER-003 — RemoveNode, ruled + amended (issue 20): refused unless the node is
//     already demoted off every slot it owns and out of the primary role, unless
//     `force` bypasses the fence; the postcondition prunes slot/migration entries naming
//     it and detaches its replicas' parent pointer either way. This is what makes "Node
//     membership" a genuinely two-way variable in this model (see the `defects` ghost
//     below for why that matters).
//   TR-CLUSTER-004 — SetRole: role/parent-pointer transition, gated by issue 14's ruled
//     precondition (a Replica's parent must name an existing Primary). The row's ruled
//     text carries no slot-ownership or migration-target precondition, and neither does
//     `commands.rs`'s `SetRole` arm (checked directly: it validates only node/parent
//     existence) — so this model does not add one either. A prior revision of this file
//     added `not(ownsAnySlot(...))`/`not(isMigrationTarget(...))` guards here, justified
//     by a misreading of INV-SLOT-1 (see the INV-SLOT-1 note below); removed per the
//     counterexample protocol once traced to no ruled or Hard-tier basis.
//   TR-CLUSTER-008/009 — AssignSlots/RemoveSlots, gated by issue 16's ruled
//     open-migration refusal (no mutation-while-migrating). Same note as SetRole: the
//     ruled text and `commands.rs`'s `AssignSlots` arm carry no target-role
//     precondition, so this model no longer invents one.
//   TR-CLUSTER-010 — BeginSlotMigration: opens a migration naming the slot's current
//     owner as source. Same note: no target-role precondition in the ruled text or
//     `commands.rs`'s `BeginSlotMigration` arm.
//   TR-CLUSTER-011 — PrepareSlotHandoff: mints a monotone `seq` from the handoff
//     counter, arms the write barrier at the source only (TR-CLUSTER-034). The ruled row
//     text's only precondition is that the migration exists with matching
//     source/target — it does not require the absence of a live handoff. A prior
//     revision added that requirement anyway, which made the supersede path
//     (FM-CLUSTER-085) and TR-CLUSTER-012's stale-seq refusal structurally unreachable;
//     removed, so re-preparing over a live handoff now mints a fresh seq and supersedes
//     it, and the old seq is thereafter correctly refused by Confirm/Complete/Abort.
//   TR-CLUSTER-012 — ConfirmSlotHandoffDrained: marks the prepared handoff drained;
//     does not itself move ownership. Stale-seq refusal is now reachable (see above).
//   TR-CLUSTER-013 — CompleteSlotMigration, ruled (issue 17): admitted purely on
//     matching-seq-and-drained, no wall-clock `barrier_ms` admission window. Falsifiable
//     independent of the guard via the `defects.admission` ghost (see below).
//   TR-CLUSTER-014 — AbortSlotHandoff, ruled (issue 17): log-ordered admission, no
//     wall-clock window; the migration itself survives, only the handoff is released.
//   TR-CLUSTER-015 — CancelSlotMigration, ruled + amended (issue 15): abort is a real
//     rollback that repatriates — any data provisionally moved to the target while the
//     handoff was drained streams back to the source before the migration record is
//     removed. The amendment's own wording ("streams ... back to the source *before*
//     the Abort applies clean") states a real ordering, not an atomicity claim, so this
//     model represents it as two separately-observable phases: `cancelMigration`/
//     `abortHandoff` open a `repatriating` phase (releasing the barrier immediately but
//     leaving the provisional-target/feed-bytes residue and, for Cancel, the migration
//     record itself in place) and a new `completeRepatriation` action later resolves it.
//     The amendment's closing CRDB analogy ("the keyspace never stays split") is about
//     *slot ownership* never splitting mid-move — already true here by construction
//     (ownership never moves before `completeMigration`; see `provisional_target`'s own
//     declaration comment) — not a claim that the rollback itself is instantaneous, so
//     giving the rollback an observable in-progress phase does not contradict it.
//   TR-CLUSTER-016 — Replica-feed hold during an armed barrier, amended (issue 17):
//     bounded by a byte cap on buffered frames, not by the deleted wall-clock deadline;
//     breaching the cap disconnects the held session. Modeled as a single atomic
//     transition (arrival-that-breaches immediately disconnects, in the same step,
//     rather than a separate reactive action) — see "Deliberately not modeled" for why.
//   TR-CLUSTER-017/018 — Planned (graceful) `CLUSTER FAILOVER`: TR-CLUSTER-018's
//     per-object epoch/role fence (issue 19, amended) plus the belt (refuse no-op
//     moves), applied only once TR-CLUSTER-017's ruled (issue 26) drain/parity barrier
//     is armed. The barrier is modeled explicitly: `failover_fence: Set[SlotId]`,
//     armed by `armFailoverFence(old)` over every slot `old` currently owns, and
//     required — as a real guard, plus an independently recomputed `defects.barrier`
//     ghost that does not depend on the guard existing — before `failoverGraceful` may
//     apply to any of those slots. The fence is single-shot: every action other than
//     `armFailoverFence` clears it, so arming and swapping must be back-to-back with
//     nothing else observed in between (a deliberate simplification of the real
//     drain/parity wait, which this model does not carry replication offsets to state
//     more precisely — see "Deliberately not modeled").
//   TR-CLUSTER-019 — Automatic failover proposal: same fence/belt shape as
//     TR-CLUSTER-018 (including the per-object fence — `commands.rs`'s guard on this
//     path is identical), gated on the old primary's FAIL flag being latched,
//     asynchronous (no drain/parity wait — issue 26's ruled cost of async PSYNC, so
//     `failoverAuto` does not require `failover_fence`).
//   TR-CLUSTER-021 — Failover successor selection: modeled as its structural
//     precondition only (`new_primary_id` is a replica of the failing primary, checked
//     by `beltOk`); the numeric priority/offset scoring formula is out of scope (see
//     "Deliberately not modeled").
//   TR-CLUSTER-024/025 — MarkNodeFailed/MarkNodeRecovered: FAIL-flag transitions,
//     including TR-CLUSTER-024's unconditional cluster-epoch bump and TR-CLUSTER-025's
//     no-bump anti-churn property (FM-CLUSTER-014) — the reason issue 19's amendment
//     moved the failover fence off the cluster-wide epoch.
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
//     itself supplies: removal is deliberately promoted to a separate, explicit, fenced
//     admin action (TR-CLUSTER-003 above) — never an automatic consequence of a
//     failover-shaped transition, forced or not. `inv_last_failover_demoted` encodes
//     exactly this generalized claim (checked against a genuinely two-way `member`
//     field now that TR-CLUSTER-003 is modeled), not TR-CLUSTER-042's stale literal
//     text.
//
// Invariant catalog `frogdb-server/crates/cluster/src/invariants.rs`:
//   INV-REF-1 (`check_ref_1`: every slot's owner exists as a current member) is what
//     `inv_slot_owner_valid` below actually encodes. A prior revision of this file
//     glossed INV-SLOT-1 as "a slot's owner, if assigned, is a live Primary" and built
//     guards to defend that claim; `check_slot_1`'s own doc comment says otherwise —
//     it is a key-range check ("every slot key is below CLUSTER_SLOTS"), unconditional
//     on `SLOTS`' bound in this model by construction, and no catalog entry (Hard or
//     documented-exception) claims a slot's owner must hold the Primary role. That
//     claim was invented, not ruled; see the SetRole/AssignSlots/BeginSlotMigration
//     notes above for the fix.
//   INV-MIG-1 (`check_mig_1`: a migrating slot's owner equals the migration's source,
//     *when the slot has a recorded owner at all* — an unassigned migrating slot is
//     exempt, per the module doc's own "What 'clean' does not claim" section).
//   INV-HANDOFF-1/2 (`check_handoff_1/2`: the seq counter dominates every live
//     handoff's seq; a live handoff never carries the unminted seq 0; a handoff never
//     repeats a seq already minted — `inv_handoff_seq_never_reused` below covers the
//     distinctness half the bare bound does not).
//   INV-EPOCH-1/2 (`check_epoch_1/2`: the cluster epoch dominates every node's own
//     epoch; a nonzero node epoch is unique among Primaries; plus this model's own
//     cross-step claim that the cluster epoch never decreases,
//     `inv_epoch_never_decreases`, which the catalog's single-state checks cannot
//     state).
//   inv_repatriating_well_formed (this model's own, not a catalog id): a slot mid the
//     new `repatriating` phase (added this round for I13) always still shows its
//     provisional-target residue and never a live handoff — the phase's own structural
//     integrity, independent of `inv_abort_repatriates`'s resolved-state claim below.
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
    // "Node membership" — a genuinely two-way field: TR-CLUSTER-003 (`removeNode`) can
    // clear it, gated by issue 20's amendment; no other action ever does.
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
    // "This migration is mid-Cancel, awaiting repatriation" — set by `cancelMigration`
    // when there is residue to stream back; distinguishes a cancelling-and-pending
    // migration record (which `completeRepatriation` must remove once the stream
    // finishes) from an abort-and-pending one (whose migration record survives
    // Abort per TR-CLUSTER-014, so `completeRepatriation` must leave it alone).
    cancelling: bool,
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
  // were (see their removal note below): `gracefulPruned` is set only inside
  // `failoverGraceful` at the moment it *actually* prunes a migration that was open on
  // one of the moved slots (checked independently of any guard, from the pre-state);
  // `forcedDemotedReplica` only when `failoverForced` demotes a primary that owned at
  // least one slot (so it cannot be satisfied by the two nodes that are already
  // Replicas at `init`); `failoverDuringOpenMigration` (the composite migration x
  // failover region I8/I9 asks to be genuinely explored) only when any of the three
  // failover actions fires while the old primary had an open migration.
  var coverage: { gracefulPruned: bool, forcedDemotedReplica: bool, failoverDuringOpenMigration: bool }

  // ---------------------------------------------------------------------------
  // Deliberately not modeled.
  // ---------------------------------------------------------------------------
  //
  // - TR-CLUSTER-017/026's drain/parity sub-protocol as a replication-offset state
  //   machine: this model has no replication offsets, so "drained and at parity" is
  //   modeled by its outward effect — an explicit, single-shot `failover_fence`
  //   (armed by `armFailoverFence`, required by `failoverGraceful`) — rather than a
  //   separate offset-tracking sub-protocol with its own progress states.
  // - TR-CLUSTER-020: no "primary-absorbs-primary" action exists in this model. That
  //   mode is ruled removed outright (issue 19), so it is omitted rather than modeled
  //   as a disabled action.
  // - TR-CLUSTER-021's numeric priority/offset successor-scoring formula: out of
  //   scope. Successor selection is modeled only by its structural precondition (the
  //   chosen `succ` must currently be a replica of the failing primary, checked by
  //   `beltOk`), not by the scoring arithmetic that picks one replica over another.
  // - TR-CLUSTER-014's FM-CLUSTER-086 "already gone -> no-op success" branch: not
  //   separately modeled. `canAbortHandoff` requires a live, seq-matching handoff;
  //   the already-resolved case (which real code accepts idempotently) is left
  //   unmodeled rather than encoded as a vacuous no-op action.
  // - Failover-triggered migration pruning (`applyFailoverCommon`, shared by
  //   `failoverGraceful`/`failoverAuto`/`failoverForced`) stays an unconditional,
  //   single-step prune — it is *not* routed through the new `repatriating` phase (see
  //   TR-CLUSTER-015's header note). The amendment's repatriation language is scoped to
  //   "the Abort protocol" (Cancel/AbortHandoff); a failover is a decisive, already-
  //   atomic cutover of primary role and slot ownership, and no ruling text extends the
  //   stream-back-before-applies-clean ordering to that path. Giving failover a wait-
  //   for-repatriation phase would be a materially larger structural change than what
  //   issue 15 actually rules on, so this model treats a failover's prune the same way
  //   it always has: whatever provisional residue the pruned migration was carrying is
  //   written off along with the migration record itself, not streamed back first.
  // - `inv_no_stuck_handoff` (from any reachable state with an armed barrier, some
  //   enabled action sequence releases it): this is a temporal liveness property, not
  //   a state invariant `quint run --invariant` can check in 0.32.0. Rather than
  //   skip it silently, it is witnessed by trajectory instead: every `run` test that
  //   reaches an armed barrier (`gracefulFailoverArmedNotDrainedTest`,
  //   `gracefulFailoverDrainedNotCompleteTest`, `happyMigrationTest`,
  //   `abortRepatriatesTest`, `forcedFailoverDemotesTest`) continues on to a step
  //   that releases it — via `completeMigration`, `abortHandoff`, `cancelMigration`,
  //   a pruning failover, or `removeNode` — and every one of those five release paths
  //   clears the slot from `armed_barriers` unconditionally, so no action that
  //   resolves a migration's handoff ever leaves its barrier armed. This is a
  //   by-construction argument over the five release actions, not a model-checked
  //   temporal property — noted here rather than asserted as a checked invariant.
  // - A slot's owner holding the Primary role: neither the ruled TR-CLUSTER-004/008/010
  //   rows nor `commands.rs`'s `SetRole`/`AssignSlots`/`BeginSlotMigration` arms
  //   enforce this (checked directly against the source — none of the three arms reads
  //   the target/parent node's role before applying), and no catalog entry (Hard or
  //   documented-exception) claims it either. This model does not add a guard the real
  //   system does not have: a demotion can leave a slot recorded to a now-Replica
  //   owner, and `AssignSlots`/`BeginSlotMigration` can name a Replica as a target. Not
  //   flagged as a bug here — it is a possible real gap in FrogDB's command layer, but
  //   fixing it is out of this model's scope (no ruling addresses it).
  // - The numeric priority/offset scoring formula behind successor selection (already
  //   noted above under TR-CLUSTER-021) is the only remaining out-of-scope corner of
  //   that row; `step`'s nondet biasing (added this round for I8/I9) does not touch it.

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

  // Impure (reads the `armed_barriers` state var directly) — kept separate from the
  // pure helpers above so the purity checker doesn't reject it.
  def barrierArmed(s: SlotId): bool = armed_barriers.contains(s)

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

  // Redesigned this round for I8/I9: the old predicate (`SLOTS.exists(s =>
  // provisional_target.get(s) == None) and epoch > 0`) was vacuous — the first half is
  // true for any never-migrated slot (true at `init`), and `epoch > 0` goes true after
  // *any* `markNodeFailed`, unrelated to graceful failover pruning anything. Backed now
  // by `coverage.gracefulPruned`, set only inside `failoverGraceful`'s own execution
  // when it actually prunes a migration open on one of the slots it moves.
  val witnessGracefulFailoverPrunedMigration: bool =
    coverage.gracefulPruned

  // Redesigned this round for I8/I9: the old predicate reduced to just `epoch > 0`,
  // since nodes 3/4 are already Replica+member at `init` — no failover needed to
  // satisfy it. Backed now by `coverage.forcedDemotedReplica`, set only inside
  // `failoverForced`'s own execution.
  val witnessForcedFailoverDemoted: bool =
    coverage.forcedDemotedReplica

  val witnessFeedDisconnected: bool =
    disconnected_feed.size() > 0

  val witnessNodeRemoved: bool =
    NODES.exists(n => not(nodes.get(n).member))

  // Added this round for I8/I9: the composite migration x failover region the reviewer
  // measured at 0.04-1.1% reach — any of the three failover actions firing while the
  // old primary had an open migration naming it.
  val witnessFailoverDuringOpenMigration: bool =
    coverage.failoverDuringOpenMigration

  // Added this round for I13: the new repatriating phase is reached (a Cancel/Abort
  // with residue has applied but the stream-back has not yet resolved).
  val witnessRepatriationPending: bool =
    repatriating.size() > 0

  // ---------------------------------------------------------------------------
  // Invariants.
  // ---------------------------------------------------------------------------

  // INV-REF-1: every assigned slot's owner exists as a current member. This model does
  // not claim the owner holds the Primary role — see the header's INV-SLOT-1 note for
  // why that claim was removed (it was never a ruled or Hard-tier property).
  val inv_slot_owner_valid: bool =
    SLOTS.forall(s => match slots.get(s) {
      | Some(n) => nodes.get(n).member
      | None => true
    })

  // INV-MIG-1: an open migration's source currently owns the slot, *or the slot is
  // currently unassigned* (check_mig_1's own `?`-early-return exempts a migrating slot
  // with no recorded owner — see the module doc's "What 'clean' does not claim");
  // source != target; both endpoints are members.
  val inv_migration_endpoints_valid: bool =
    SLOTS.forall(s => match migrations.get(s) {
      | Some(m) => and {
          slots.get(s) == Some(m.source) or slots.get(s) == None,
          m.source != m.target,
          nodes.get(m.source).member,
          nodes.get(m.target).member,
        }
      | None => true
    })

  // INV-HANDOFF-1: a live handoff's seq is minted (>= 1) and never exceeds the
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

  // INV-HANDOFF distinctness half: no handoff seq is ever minted twice.
  val inv_handoff_seq_never_reused: bool = not(defects.seqReuse)

  // INV-EPOCH-1/2: the cluster epoch dominates every node's own epoch; a nonzero node
  // epoch is unique among current Primaries.
  val inv_epoch_monotone: bool = and {
    epoch >= NODES.fold(0, (acc, n) => max2(acc, node_epoch.get(n))),
    NODES.forall(n1 => NODES.forall(n2 =>
      (n1 != n2 and nodes.get(n1).role == Primary and nodes.get(n2).role == Primary and node_epoch.get(n1) != 0)
        implies node_epoch.get(n1) != node_epoch.get(n2)
    )),
  }

  // Cross-step half INV-EPOCH-1/2 cannot state: the cluster epoch never decreases.
  val inv_epoch_never_decreases: bool = epoch >= prev_epoch

  // Issue 15, ruled + amended: once a slot has no live handoff *and* is not mid the
  // `repatriating` phase (its migration was aborted/cancelled/completed and any
  // repatriation resolved, or it never had one), no residue remains — no
  // provisional-at-target ghost claim, and no armed barrier still names that slot.
  // Restated this round for I13: the amendment's own ordering ("streams back to the
  // source *before* the Abort applies clean") makes a pending-repatriation state with
  // residue still visible a legal, ruled-for intermediate — not a violation of "abort =
  // rollback with repatriation" but the *in-progress* half of that same claim, which is
  // why `not(repatriating.contains(s))` is now part of the precondition rather than the
  // claim being dropped. This is still made falsifiable by the `provisional_target`
  // ghost field (see its declaration comment for why a plain "slot owned by source"
  // check would be vacuous in this model's shape).
  val inv_abort_repatriates: bool =
    SLOTS.forall(s => {
      val liveHandoff = match migrations.get(s) {
        | Some(m) => m.handoff != None
        | None => false
      }
      (not(liveHandoff) and not(repatriating.contains(s)))
        implies (provisional_target.get(s) == None and not(armed_barriers.contains(s)))
    })

  // Added this round for I13: the `repatriating` phase's own structural integrity — a
  // slot mid-phase always still shows the residue it is streaming back (so
  // `completeRepatriation` has something real to resolve), never a live handoff (both
  // entry points into the phase clear it, so it cannot be re-prepared/re-armed out from
  // under the phase), and — the `None` branch below — always still has a live
  // `Migration` record: both `cancelMigration` and `abortHandoff` only ever add a slot to
  // `repatriating` in the same step they keep (or write) that slot's migration as
  // `Some(...)`, so `None` while `repatriating.contains(s)` can only mean some other
  // action clobbered the record out from under an in-flight phase — caught here rather
  // than left as a silent gap the way a permissive `None => true` would leave it (found
  // by mutation testing: a mutant that made `cancelMigration` clear the record
  // unconditionally, ignoring residue, slipped past this invariant's original `None =>
  // true` arm and past `inv_abort_repatriates` alike, since neither one asserted the
  // record must survive while its repatriation is outstanding).
  val inv_repatriating_well_formed: bool =
    SLOTS.forall(s => repatriating.contains(s) implies and {
      provisional_target.get(s) != None or feed_bytes.get(s) > 0 or disconnected_feed.contains(s),
      match migrations.get(s) { | Some(m) => m.handoff == None | None => false },
    })

  // Issue 20, ruled: no failover — planned, automatic, or forced — ever leaves the old
  // primary undemoted (still a Primary), un-parented to its successor, or un-membered.
  // Checked via the `defects.demotion` ghost, which is recomputed independently at each
  // failover action from that action's own postcondition output — not from the guard —
  // so a mutant that stops demoting (while everything else about the transition stays
  // legal) is still caught even though no *guard* protects this postcondition.
  val inv_last_failover_demoted: bool = not(defects.demotion)

  // Issue 19, amended: the per-object epoch/role fence. Checked via `defects.fence`,
  // recomputed from the pre-action state independent of `fenceOk`'s own guard use.
  val inv_last_failover_fenced: bool = not(defects.fence)

  // Issue 26, ruled: `failoverGraceful` never applies without its drain/parity barrier
  // armed over every slot it is about to move. Checked via `defects.barrier`,
  // recomputed independent of the guard.
  val inv_graceful_failover_barriered: bool = not(defects.barrier)

  // Issue 17, ruled: CompleteSlotMigration never admits without a matching, drained
  // handoff. Checked via `defects.admission`, recomputed independent of `canComplete`.
  val inv_complete_requires_drained: bool = not(defects.admission)

  // TR-CLUSTER-016 amendment: the byte-cap hold is honored — no slot's buffered bytes
  // ever exceed the cap. Meaningful now that `feedBuffer` accepts every arrival
  // (folding the breach-disconnect branch into the same action, per the header note) —
  // a mutant that skips the breach branch violates this directly.
  val inv_feed_hold_bounded: bool =
    SLOTS.forall(s => feed_bytes.get(s) <= HOLD_BYTE_CAP)

  // ---------------------------------------------------------------------------
  // Tests.
  // ---------------------------------------------------------------------------

  // (a) Full happy migration: begin -> prepare -> confirm drained -> complete.
  run happyMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .expect(migrations.get(1) == Some({ source: 1, target: 2, handoff: None, cancelling: false }))
      .then(prepareHandoff(1))
      .expect(and {
        barrierArmed(1),
        match migrations.get(1) { | Some(m) => match m.handoff { | Some(h) => h.seq == 1 and not(h.drained) | None => false } | None => false },
      })
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(completeMigration(1, 1))
      .expect(and {
        slots.get(1) == Some(2),
        migrations.get(1) == None,
        not(barrierArmed(1)),
        provisional_target.get(1) == None,
      })
  }

  // (b1) Graceful failover before any handoff was ever prepared: migration open, no
  // barrier armed yet; failover of the source still prunes the migration. The barrier
  // (armFailoverFence) is armed immediately before the swap, per its single-shot
  // discipline.
  run gracefulFailoverBeforePrepareTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
        nodes.get(3).role == Primary,
      })
  }

  // (b1a) Without arming the barrier first, the same graceful failover is refused —
  // TR-CLUSTER-017's ruled drain/parity precondition, now a real guard.
  run gracefulFailoverRefusesWithoutBarrierTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(failoverGraceful(1, 3, 0, Primary).fail())
  }

  // (b2) Graceful failover while the handoff is armed but not drained: the barrier
  // must be released by the prune, not left orphaned.
  run gracefulFailoverArmedNotDrainedTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .expect(barrierArmed(1))
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        not(barrierArmed(1)),
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
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        not(barrierArmed(1)),
        provisional_target.get(1) == None,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
      })
  }

  // (c) Abort mid-handoff -> repatriation, without any failover involved. Restructured
  // this round for I13: `abortHandoff` no longer clears the residue in the same step —
  // it opens the `repatriating` phase (barrier released, migration survives, residue
  // still visible), and a separate `completeRepatriation` resolves it. This is exactly
  // the "cancel/abort-before-repatriation" intermediate state I13 asked to be made
  // expressible: without the intermediate `.expect()` below, this same test collapsed
  // Abort's rollback and its repatriation into a single, unobservable step.
  run abortRepatriatesTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(abortHandoff(1, 1))
      .expect(and {
        // The intermediate, previously-unobservable state: Abort has applied (handoff
        // released, barrier down) but repatriation has not yet caught up.
        migrations.get(1) != None,
        match migrations.get(1) { | Some(m) => m.handoff == None | None => false },
        provisional_target.get(1) == Some(2),
        not(barrierArmed(1)),
        repatriating.contains(1),
      })
      .then(completeRepatriation(1))
      .expect(and {
        migrations.get(1) != None,
        provisional_target.get(1) == None,
        not(repatriating.contains(1)),
      })
  }

  // Added this round for I13: Cancel (not just Abort) with residue also opens the
  // repatriating phase — cancel-before-repatriation is now a real, distinct
  // intermediate state (the migration record survives, marked `cancelling: true`, until
  // `completeRepatriation` actually removes it — "the Abort applies clean," in the
  // amendment's words).
  run cancelBeforeRepatriationTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(provisional_target.get(1) == Some(2))
      .then(cancelMigration(1))
      .expect(and {
        // Cancel has applied (barrier down) but the migration record is NOT yet
        // removed and the residue has NOT yet streamed back — the exact intermediate
        // state I13 asked to be expressible.
        match migrations.get(1) { | Some(m) => m.cancelling and m.handoff == None | None => false },
        provisional_target.get(1) == Some(2),
        not(barrierArmed(1)),
        repatriating.contains(1),
      })
      .then(completeRepatriation(1))
      .expect(and {
        migrations.get(1) == None,
        provisional_target.get(1) == None,
        not(repatriating.contains(1)),
      })
  }

  // While pending, the phase resists re-entry: a second Cancel is refused. (`.fail()`
  // is the terminal step of a run — see cluster_admission.qnt's tests for the same
  // discipline — so this is its own test rather than a mid-chain step.)
  run cancelRefusedWhileRepatriatingTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(cancelMigration(1))
      .expect(repatriating.contains(1))
      .then(cancelMigration(1).fail())
  }

  // The no-residue case is unaffected by this round's change: cancelling a migration
  // that never reached a drained handoff removes the record immediately, exactly as
  // before, with no repatriating phase opened at all.
  run cancelWithoutResidueAppliesImmediatelyTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(cancelMigration(1))
      .expect(and {
        migrations.get(1) == None,
        not(repatriating.contains(1)),
      })
  }

  // (c2) Re-preparing over a live handoff supersedes it (FM-CLUSTER-085): the old seq
  // becomes stale and is thereafter refused.
  run prepareSupersedesLiveHandoffTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .expect(match migrations.get(1) { | Some(m) => match m.handoff { | Some(h) => h.seq == 1 | None => false } | None => false })
      .then(prepareHandoff(1))
      .expect(match migrations.get(1) { | Some(m) => match m.handoff { | Some(h) => h.seq == 2 and not(h.drained) | None => false } | None => false })
      // The stale seq 1 is refused by every seq-checked action.
      .then(confirmDrained(1, 1).fail())
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
        not(barrierArmed(1)),
      })
  }

  // The per-object fence (issue 19, amended) refuses a stale observation: node 3's
  // epoch has moved on (it was just promoted) since a proposal that still thinks it is
  // at epoch 0 observed it. After `failoverForced(1, 3, ...)`, node 1 becomes node
  // 3's replica (siblings re-parent to the new primary), so `failoverForced(3, 1, ...)`
  // passes the belt (1 is 3's replica, 3 owns slots, 3 is Primary) but must fail the
  // fence. (Forced, not graceful, since this test does not arm the drain/parity
  // barrier — the fence check is what is under test here, not the barrier.)
  run failoverRefusesStaleFenceTest: bool = {
    init
      .then(failoverForced(1, 3, 0, Primary))
      .expect(and {
        node_epoch.get(3) == 1,
        nodes.get(1).role == Replica,
        nodes.get(1).parent == Some(3),
      })
      // A stale proposal that still thinks node 3 is at epoch 0 (its value before the
      // promotion above) must be refused by the fence, even though the belt passes.
      .then(failoverForced(3, 1, 0, Primary).fail())
  }

  // TR-CLUSTER-016 amendment: byte-cap hold overflow disconnects the session rather
  // than buffering unboundedly, in the same step as the breaching arrival.
  run feedHoldOverflowDisconnectsTest: bool = {
    init
      .then(beginMigration(1, 1, 2))
      .then(prepareHandoff(1))
      .then(feedBuffer(1, 3))
      .then(feedBuffer(1, 3))
      .expect(and { feed_bytes.get(1) == 6, not(disconnected_feed.contains(1)) })
      .then(feedBuffer(1, 1))
      .expect(and { feed_bytes.get(1) == 0, disconnected_feed.contains(1) })
  }

  // TR-CLUSTER-003, ruled + amended (issue 20): a member still owning a slot cannot be
  // removed without `force`. `.fail()` is the terminal step of a run (see
  // cluster_admission.qnt's tests for the same discipline) — the positive "succeeds
  // once demoted" continuation lives in removeNodeAfterDemotionSucceedsTest below.
  run removeNodeRefusesLiveOwnerTest: bool = {
    init
      .then(removeNode(1, false).fail())
  }

  // Once demoted off its slots by a graceful failover, the bare (non-forced) removal
  // succeeds and detaches its former replica's parent pointer.
  run removeNodeAfterDemotionSucceedsTest: bool = {
    init
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and { nodes.get(1).role == Replica, not(ownsAnySlot(slots, 1)) })
      .then(removeNode(1, false))
      .expect(and { not(nodes.get(1).member), nodes.get(4).parent == Some(2) })
  }

  // TR-CLUSTER-003's `force` escape: a live owning Primary can be forcibly removed;
  // the postcondition still prunes its slots (INV-REF-1 survives).
  run removeNodeForceEvictsLiveOwnerTest: bool = {
    init
      .then(removeNode(1, true))
      .expect(and {
        not(nodes.get(1).member),
        slots.get(1) == None,
        slots.get(2) == None,
      })
  }

}
