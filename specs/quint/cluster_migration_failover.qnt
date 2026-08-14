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
//     including TR-CLUSTER-024's unconditional cluster-epoch bump — the reason issue
//     19's amendment moved the failover fence off the cluster-wide epoch. Disclaimer:
//     TR-CLUSTER-025's no-bump anti-churn property (FM-CLUSTER-014) is cited for
//     context on `markNodeRecovered` below, not machine-checked — no invariant here
//     asserts it, so a mutant that adds a spurious epoch bump on recovery survives.
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
//
// Model layout (this model is split across four files under specs/quint/; the split is
// purely structural — same state space, same actions, same invariants, same tests as
// the single-file original):
//   cluster_migration_failover_types.qnt   — types + constants, no state
//   cluster_migration_failover_logic.qnt   — pure guards/updates/helpers over explicit
//                                            state arguments (unit-testable in isolation)
//   cluster_migration_failover_machine.qnt — `var` declarations + `init`/actions/`step`
//   cluster_migration_failover.qnt         — THIS FILE, the runnable main model:
//                                            witnesses, `val inv_*` invariants, `run` tests
// The satellite modules are imported with plain `import M.* from "./M"`, which shares one
// instance of the machine's state variables (it does not instantiate a copy the way a
// const-parameterised `import M(c = ...)` would). `just quint-check` typechecks every
// file; `just quint-run`/`quint-verify-*` only drive the main models (scripts/quint-models.sh
// derives that set from "declares at least one `val inv_*`").
module cluster_migration_failover {
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"
  import cluster_migration_failover_logic.* from "./cluster_migration_failover_logic"
  import cluster_migration_failover_machine.* from "./cluster_migration_failover_machine"

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
