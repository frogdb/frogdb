// cluster_migration_failover — the migration x failover composite model (formal-spec
// phase 2, task 2).
//
// Models the issue-15/16/17/19/20/26 defect class: slot migrations interleaved with
// failovers, and the ruled (target) semantics that close it, plus TR-CLUSTER-003's
// (issue 20 amendment) eviction fence. Modeled from specs/cluster.md's RULED rows, the
// issue rulings/amendments below, and — for everything about the migration protocol
// itself — the issue-31 redesign
// (`.scratch/cluster-correctness/2026-08-14-issue31-migration-design.md`), which is
// AUTHORITY over this model wherever the two disagree. Not modeled from today's code:
// the per-object epoch/role fence, the log-ordered handoff barrier, the drain/parity
// barrier, the demote-not-remove postcondition for `force: true` and the whole
// source-authoritative-until-commit protocol are none of them implemented yet.
//
// This is one Raft state machine: the model is the *applied* sequence of committed
// commands, with no network/consensus modeling — openraft's total order is assumed,
// exactly the `apply_command` boundary task 3's quint-connect harness drives.
//
// ---------------------------------------------------------------------------
// The issue-31 rework (this revision): what changed and why.
// ---------------------------------------------------------------------------
//
// The superseded model encoded issue 15's *repatriation* design: ownership moved to the
// target at `Complete`, and a Cancel/Abort after the handoff drained had to stream the
// provisionally-moved data back to the source before applying clean (`repatriating`,
// `completeRepatriation`, `provisional_target`, `inv_abort_repatriates`).
//
// The redesign supersedes that outright (MAJ-5): the migration is
// **source-authoritative-until-commit**. The target's copy is a *shadow* until its
// promotion is attested, the source keeps its own copy — and its authority — the whole
// time, and so there is never anything to stream back. Structurally:
//   - `repatriating` / `completeRepatriation` / `inv_abort_repatriates` /
//     `inv_repatriating_well_formed` / the `cancelling` record field: DELETED. Cancel
//     and Abort now simply resolve the record; the target's staged copy is cleaned up by
//     the separately schedulable `discardShadow`.
//   - `provisional_target` (a ghost that existed only to make repatriation falsifiable):
//     replaced by the first-class `shadow: SlotId -> Option[ShadowTag]` (ext-8).
//   - `residue: SlotId -> Option[Residue]` (ext-12): written by `completeMigration` with
//     **every** field initialized, marked (never removed) by `removeNode`, re-homed by
//     `retargetSlotResidue`, removed by `clearSlotResidue` — or, for the one state the
//     ordinary path provably cannot reach (a `promoted` entry whose target has departed,
//     which the reaper defers on forever), by the operator's orphan re-home arm
//     `rehomeOrphanSlot` (ext-12, V7-M6). It is what carries the "who still holds a copy
//     of this slot, and has the new owner attested yet?" question past the commit point.
//   - promotion is **split in two** (ext-8): `relabelShadow` is the target's local flip,
//     `reportPromoted` the replicated attestation. `failPromotion` rolls ownership back
//     to the source in between. The gap between the two is where the rollback-wins trace
//     lives.
//   - `reapSlots` (ext-7) is the model's **only** deleter of a member's keyspace entry
//     (V8-C1), gated on the promotion attestation and deferred while the target has
//     departed. Every node now carries its own `keys: Set[SlotId]`, distinct from the
//     slot *ownership* map.
//   - the handoff sequence is re-keyed by reset epoch (ext-10): `spent: Set[{epoch,
//     seq}]`, so a `ResetCluster` that rewinds the counter makes a re-mint
//     distinguishable from a reuse.
//   - the migration record carries the attempt stamp (ext-3), the run tags (ext-1), the
//     target's applied/durable/replica-ack positions (ext-5/ext-6/ext-14) and the
//     observation counter with its confirm-time reset (ext-4); `fenced` (the source's
//     own seal, ext-2) is tracked separately from `phase`.
//   - ext-14's durability requirement is a **captured parameter** on that record
//     (`require_replica_ack`), stamped once at Begin from the proposer's config, not a
//     mutable cluster knob read at admission time. A knob read at admission is the
//     anti-pattern the redesign forbids: two appliers holding different config values
//     would admit different commands off the same log (Raft divergence), and a flip
//     mid-migration would silently void the conjunct the operator believed was armed.
//   - TR-CLUSTER-035 is modelled in its real direction: a reset keeps the resetting
//     node's own cell and deletes every **other** member's, drops **all** slot
//     assignments and takes **all** migrations. Only SOFT is modelled — HARD's re-key and
//     epoch zeroing is FM-CLUSTER-010's documented non-monotonicity, and encoding it
//     would falsify `inv_epoch_never_decreases` by design rather than by defect.
//   - a source restart aborts its migrations through the **replicated** path, never
//     locally (ext-15, V10-M1): `sourceRestart` itself touches no record; the cancel
//     binds to the run-identity **field write**, so it is `reportRunIdentity` — and only
//     an arm of it that actually changes the stored identity — that takes every open
//     migration the reporting node sources (paying the same release events a prune does).
//     A replayed or equal-identity report is a refused no-op and cancels nothing, and a
//     topology-only re-proposal at equality changes no identity either.
//   - role/parent (`primary_id`) are modelled variables with a declared writer set
//     (ext-15/16/17): every writer goes through `writeParentPointer`, which bumps the
//     parenting token `parent_seq` and rewrites its three companions in the same step.
//   - the node-local, crash-durable pending-transition record (ext-18) and the refusal /
//     disposition machinery around it are modelled state.
//
// Q1 scope note: this revision reworks STATE and ACTIONS, ports the surviving invariants
// and rewrites the tests. The redesign's *new* invariants and witnesses (the
// `inv_no_serve_before_attestation` / `inv_member_keyspace_is_tracked` /
// `inv_no_orphan_shadow_blocks_ingest` family) are phase Q2 and are deliberately not
// written here yet — but every field they read already exists in the state.
//
// Rows modeled:
//   TR-CLUSTER-003 — RemoveNode, ruled + amended (issue 20): refused unless the node is
//     already demoted off every slot it owns and out of the primary role, unless
//     `force` bypasses the fence; the postcondition prunes slot/migration entries naming
//     it and detaches its replicas' parent pointer either way. Reworked per ext-12: a
//     departure **marks** residue entries naming the node (`source_gone`/`target_gone`)
//     rather than removing them — mark-don't-remove — and it never deletes the node's
//     keyspace.
//   TR-CLUSTER-004 — SetRole: role/parent-pointer transition, gated by issue 14's ruled
//     precondition (a Replica's parent must name an existing Primary). The row's ruled
//     text carries no slot-ownership or migration-target precondition, and neither does
//     `commands.rs`'s `SetRole` arm (checked directly: it validates only node/parent
//     existence) — so this model does not add one either.
//   TR-CLUSTER-008/009 — AssignSlots/RemoveSlots, gated by issue 16's ruled
//     open-migration refusal (no mutation-while-migrating), extended by ext-12 to refuse
//     while a residue entry is outstanding.
//   TR-CLUSTER-010 — BeginSlotMigration: opens a migration naming the slot's current
//     owner as source, and (ext-10) performs the Begin-time **discard-then-ingest** of
//     any shadow the slot still carried, so an orphan shadow from a previous reset epoch
//     can never wedge the next ingest.
//   TR-CLUSTER-011 — PrepareSlotHandoff: mints a monotone attempt stamp from the handoff
//     counter, spends it under the current reset epoch, seals the source (ext-2's
//     `fenced`) and arms the write barrier at the source only (TR-CLUSTER-034). Its only
//     precondition is that the migration exists — re-preparing over a live handoff
//     supersedes it (FM-CLUSTER-085), which is what makes the stale-attempt refusal
//     reachable.
//   TR-CLUSTER-012 — ConfirmSlotHandoffDrained: seals the drain, moving the record from
//     `Streaming` to `Draining` and recording the run-tagged position it drained at
//     (ext-1). It does not itself move ownership, and (ext-4, V7-C3) it resets the
//     observation counter so the post-seal bound is not consumed pre-seal.
//   TR-CLUSTER-013 — CompleteSlotMigration, ruled (issue 17): admitted on the log-ordered
//     conjuncts only — matching attempt, `Draining`, a sealed source, a same-run drained
//     position, a target copy that covers it and is attested under the target's *current*
//     run, the record's own minted run tag still matching the source's **replicated**
//     run identity, and — iff the migration's own captured `require_replica_ack`
//     parameter says so (ext-14) — a counted replica ack. No wall-clock `barrier_ms`
//     window. The replicated-identity conjunct is belt-and-braces over the field-write
//     cancel above: it deliberately reads the *stored* identity, so the design-accepted
//     window in which a source has physically restarted but its boot report has not yet
//     applied still admits `Complete`.
//     Falsifiable independent of the guard via the `defects.admission` ghost.
//   TR-CLUSTER-014 — AbortSlotHandoff, ruled (issue 17): log-ordered admission, no
//     wall-clock window; the migration itself survives, only the handoff is released.
//   TR-CLUSTER-015 — CancelSlotMigration, **reworked by the redesign**: with the source
//     authoritative until commit there is nothing to stream back, so Cancel removes the
//     record in one step. Issue 15's repatriation ordering is superseded (see above).
//   TR-CLUSTER-016 — Replica-feed hold during an armed barrier, amended (issue 17):
//     bounded by a byte cap on buffered frames, not by the deleted wall-clock deadline.
//     Reworked per ext-2/ext-11: the breach is a **separate, local** action
//     (`localReleaseOnCapBreach`) that drops the source's own fence and empties the whole
//     hold region — not an arrival that atomically disconnects itself. That is why the
//     old `inv_feed_hold_bounded` is gone (see the invariant notes below).
//   TR-CLUSTER-017/018 — Planned (graceful) `CLUSTER FAILOVER`: TR-CLUSTER-018's
//     per-object epoch/role fence (issue 19, amended) plus the belt (refuse no-op moves
//     and unsynced candidates — ext-16), applied only once TR-CLUSTER-017's ruled (issue
//     26) drain/parity barrier is armed over every slot about to move. The barrier is
//     single-shot: every action other than `armFailoverFence` clears it.
//   TR-CLUSTER-019 — Automatic failover proposal: same fence/belt shape as
//     TR-CLUSTER-018, gated on the old primary's FAIL flag, asynchronous (no drain/parity
//     wait — issue 26's ruled cost of async PSYNC).
//   TR-CLUSTER-021 — Failover successor selection: modeled as its structural
//     precondition only (`beltOk`); the numeric priority/offset scoring formula is out of
//     scope.
//   TR-CLUSTER-024/025 — MarkNodeFailed/MarkNodeRecovered: FAIL-flag transitions.
//     TR-CLUSTER-025's no-bump anti-churn property (FM-CLUSTER-014) **is** machine-checked
//     as of the 2026-08-19 ruling — `inv_no_churn_epoch_bump`, off the `churnEpochBump`
//     postcondition ghost both transitions write.
//   TR-CLUSTER-033/034 — Handoff barrier arm/release and the held-write region, modeled
//     at the applied-state level as `barriers.armed: Set[SlotId]` plus a per-node
//     `held: NodeId -> Set[SlotId]`: at most one migration (hence one source) can be open
//     per slot, so the node coordinate is redundant with the slot key in the barrier set,
//     while the held set is per-node because ext-11's release property is stated over the
//     *source's* whole hold region.
//   TR-CLUSTER-042 — Forced (`force: true`) failover. **Deviation from this row's
//     literal postcondition**: the row's text still says "Node membership loses
//     `old_primary_id` outright" — that text predates issue 20's ruling. This model
//     applies demote-don't-remove to the forced path too. What `force` actually drops is
//     ext-16's *sync* gate on the candidate, not the per-object fence and not the belt.
//     `inv_last_failover_demoted` encodes the generalized claim.
//
// Invariant catalog `frogdb-server/crates/cluster/src/invariants.rs`:
//   INV-REF-1 (`check_ref_1`: every slot's owner exists as a current member) is what
//     `inv_slot_owner_valid` below encodes. No catalog entry claims a slot's owner must
//     hold the Primary role, so this model does not guard for it.
//   INV-MIG-1 (`check_mig_1`: a migrating slot's owner equals the migration's source,
//     *when the slot has a recorded owner at all*).
//   INV-HANDOFF-1/2 (`check_handoff_1/2`: a live handoff never carries the unminted seq
//     0, and a seq is never minted twice). The "counter dominates every live seq" half is
//     strengthened to "every live attempt stamp was actually minted under the current
//     reset epoch" — see `inv_handoff_owned`'s note for how the bare bound survives
//     `ResetCluster`'s counter rewind.
//   INV-EPOCH-1/2 (`check_epoch_1/2`: the cluster epoch dominates every node's own
//     epoch; a nonzero node epoch is unique among Primaries; plus this model's own
//     cross-step claim `inv_epoch_never_decreases`).
//
// Issues (rulings + amendments this model encodes):
//   .scratch/cluster-correctness/issues/open/15-graceful-failover-leaves-migrations-sourced-at-the-old-primary.md
//   .scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md
//   .scratch/cluster-correctness/issues/open/19-a-forced-failover-promotes-a-node-that-inherits-nothing.md
//   .scratch/cluster-correctness/issues/open/20-force-failover-evicts-the-old-primary-from-raft-so-it-never-learns-it-lost-its-slots.md
//   .scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md
//   .scratch/cluster-correctness/2026-08-14-issue31-migration-design.md  (AUTHORITY)
//
// Model layout (this model is split across five files under specs/quint/; the split is
// purely structural — same state space, same actions, same invariants, same tests as a
// single-file original would have):
//   cluster_common_types.qnt               — `NodeId`/`Option`, shared with the admission
//                                            model; owned by neither
//   cluster_migration_failover_types.qnt   — this model's types + sizing constants, no state
//   cluster_migration_failover_logic.qnt   — pure guards/updates/helpers over explicit
//                                            state arguments (unit-testable in isolation)
//   cluster_migration_failover_machine.qnt — `var` declarations + `init`/actions/`step`
//   cluster_migration_failover.qnt         — THIS FILE, the runnable main model:
//                                            witnesses, `val inv_*` invariants, `run` tests
// The satellite modules are imported with plain `import M.* from "./M"`, which shares one
// instance of the machine's state variables (it does not instantiate a copy the way a
// const-parameterised `import M(c = ...)` would). Quint's `import M.*` is NOT transitive,
// so every file that names `NodeId`/`Option` imports cluster_common_types itself — which
// is also why only this file may declare an invariant: `just quint-check` typechecks every
// file, while `just quint-run`/`quint-verify-*` drive only the main models, and
// scripts/quint-models.sh derives that set from "declares at least one `val inv_*`" — a
// satellite that grew one would be promoted to a runnable model it cannot be.
//
// Single responsibility of THIS file: everything checkable — nothing that the checker
// reads as part of the system under check.
//   Belongs here:     witnesses (`witness*` reachability values), state invariants
//                     (`inv_*`), `run` tests, and the model-wide prose header above.
//   Does NOT belong:  `var`s, `init`, actions or `step` (…_machine.qnt); guards and updates
//                     (…_logic.qnt); types and sizing constants (…_types.qnt,
//                     cluster_common_types.qnt).
module cluster_migration_failover {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"
  import cluster_migration_failover_logic.* from "./cluster_migration_failover_logic"
  import cluster_migration_failover_machine.* from "./cluster_migration_failover_machine"

  // ---------------------------------------------------------------------------
  // Deliberately not modeled.
  // ---------------------------------------------------------------------------
  //
  // - TR-CLUSTER-017/026's drain/parity sub-protocol as a replication-offset state
  //   machine: modeled by its outward effect — an explicit, single-shot
  //   `failover_fence` — rather than a separate offset-tracking sub-protocol.
  // - TR-CLUSTER-020: no "primary-absorbs-primary" action exists in this model. That
  //   mode is ruled removed outright (issue 19), so it is omitted rather than modeled
  //   as a disabled action.
  // - TR-CLUSTER-021's numeric priority/offset successor-scoring formula.
  // - TR-CLUSTER-014's FM-CLUSTER-086 "already gone -> no-op success" branch:
  //   `canAbortHandoff` requires a live, attempt-matching handoff; the already-resolved
  //   case (which real code accepts idempotently) is left unmodeled rather than encoded
  //   as a vacuous no-op action.
  // - `RunId` freshness: the run tag is deliberately opaque (equality only). No fence
  //   here reads it for ordering — that job belongs to `registration_seq`/`parent_seq`.
  // - Raft proposer identity: the redesign's leader-driven auto-`Complete` is modeled as
  //   `completeMigration` itself; `reconcileTick` only advances the observation counter
  //   and `boundAbort` is the bounded exit. The model has no notion of *which* node
  //   proposed a command, so "the leader proposes it" is unstated rather than faked.
  // - `inv_no_stuck_handoff` (from any reachable state with an armed barrier, some
  //   enabled action sequence releases it): a temporal liveness property, not a state
  //   invariant `quint run --invariant` can check in 0.32.0. Witnessed by trajectory
  //   instead: every release path (`completeMigration`, `abortHandoff`,
  //   `cancelMigration`, `boundAbort`, `localReleaseOnCapBreach`, `selfFenceRelease`, a
  //   pruning failover, `removeNode`) clears the slot from `barriers.armed` and empties
  //   the source's held set unconditionally. A by-construction argument over those
  //   actions, not a model-checked property.
  // - A slot's owner holding the Primary role: neither the ruled TR-CLUSTER-004/008/010
  //   rows nor `commands.rs`'s corresponding arms enforce it, and no catalog entry claims
  //   it. `completeMigration` is the exception — the redesign *does* require the target
  //   to be a member primary at the commit point (V6-C2), and that conjunct is modeled.

  // ---------------------------------------------------------------------------
  // Witnesses — reachability values, in the same families as the invariants below.
  //   Migration lifecycle  — witnessMigrationInFlight, witnessHandoffDrainedNotComplete,
  //                          witnessDrainingHeldForKTicks, witnessHealthyDrainAbortedByBound,
  //                          witnessKnobOnMigrationCompletes, witnessCompleteEnabledOnQuietSlot,
  //                          witnessEmptyCoverageBatchIsANoOp
  //   Failover             — witnessGracefulFailoverPrunedMigration,
  //                          witnessForcedFailoverDemoted, witnessFailoverDuringOpenMigration,
  //                          witnessResyncedReplicaBecomesCandidate
  //   Barrier and hold     — witnessFeedDisconnected, witnessFenceClears
  //   Membership           — witnessNodeRemoved, witnessNonEmptyJoinRefused,
  //                          witnessNonEmptyResetRefused
  //   Residue and reaper   — witnessResiduePending, witnessBeginRefusedOverResidue
  //   Identity and run     — witnessStagedFlipCompletesAcrossCrash,
  //                          witnessNodeCanAlwaysReportIdentity
  //   Declared unreachable — witnessDrainingWedgedBeforeConfirm,
  //                          witnessDrainingWedgedWithCompletableToken
  // A witness backed by a `coverage.*` ghost is set inside the action's own execution, so
  // it cannot be satisfied by a state that merely looks like the behaviour happened.
  //
  // The last family is stated **because it is unreachable**: each of those two values is a
  // state a superseded revision of the design could reach and the adopted narrowing
  // forbids. Writing them down makes the narrowing checkable — reverting the narrowing
  // makes the witness reachable — instead of leaving it as a claim in a comment. Each
  // carries its own note saying which revert would reach it.
  //
  // Every other witness here is **reachable**, and each is pinned by a deterministic `run`
  // test in the witness-reachability block at the end of the test section.
  // ---------------------------------------------------------------------------

  val witnessMigrationInFlight: bool =
    SLOTS.exists(s => migrations.get(s) != None)

  // Reworked: "drained" is now the record's `Draining` phase (ext-4), reached by
  // `confirmDrained` sealing the drain, not a `handoff.drained` flag.
  val witnessHandoffDrainedNotComplete: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => m.phase == Draining
      | None => false
    })

  // Backed by `coverage.gracefulPruned`, set only inside `failoverGraceful`'s own
  // execution when it actually prunes a migration open on one of the slots it moves.
  val witnessGracefulFailoverPrunedMigration: bool =
    coverage.gracefulPruned

  // Backed by `coverage.forcedDemotedReplica`, set only inside `failoverForced`'s own
  // execution, and now specifically when the promoted candidate was *not* synced — the
  // exact gate `force` drops (ext-16).
  val witnessForcedFailoverDemoted: bool =
    coverage.forcedDemotedReplica

  val witnessFeedDisconnected: bool =
    barriers.disconnected.size() > 0

  val witnessNodeRemoved: bool =
    NODES.exists(n => not(nodes.get(n).member))

  // The composite migration x failover region the reviewer measured at 0.04-1.1% reach —
  // any failover-shaped action firing while the old primary had an open migration naming
  // it.
  val witnessFailoverDuringOpenMigration: bool =
    coverage.failoverDuringOpenMigration

  // Replaces `witnessRepatriationPending`: the post-commit state the redesign puts in
  // repatriation's place — ownership has moved but the source still holds its copy and
  // the promotion has not been attested.
  val witnessResiduePending: bool =
    SLOTS.exists(s => match residue.get(s) {
      | Some(r) => not(r.promoted)
      | None => false
    })

  // --- Q2 witnesses ----------------------------------------------------------

  // ext-4: the drain is *held* across observation ticks — sealed record, hold region
  // still armed, and the post-seal counter has accrued observations without reaching its
  // bound. At the stock `draining_observations = 3` that is K = 2: one more tick without
  // progress abandons the drain, so this is precisely the "still holding, not yet
  // abandoned" state the bound exists to bound.
  val witnessDrainingHeldForKTicks: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => and {
          m.phase == Draining,
          barrierArmed(s),
          m.observations >= DRAINING_OBSERVATIONS - 1,
          not(isAbortableByBound(m)),
        }
      | None => false
    })

  // ext-4, backed by `coverage.healthyAborted`: a *lawfully progressing-in-principle*
  // pre-`Confirm` migration — its target still had records to ingest — taken by the
  // pre-seal bound. The calibration obligation `preconfirm_observations` carries is a
  // reachable behaviour, not a hypothetical.
  val witnessHealthyDrainAbortedByBound: bool = coverage.healthyAborted

  // ext-14 (V8-m4), backed by `coverage.knobCompleted`: a `require_replica_ack`
  // migration that *commits* at the stock sizings. The knob has to be satisfiable, or
  // the durability conjunct is a liveness bug rather than a safety gate.
  val witnessKnobOnMigrationCompletes: bool = coverage.knobCompleted

  // ext-9 (N-C5): `Complete`'s guard holds on a slot that never took a write — the quiet
  // slot, where no traffic drives the drain and the coverage batch is the only advance.
  val witnessCompleteEnabledOnQuietSlot: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => canCompleteMigration(nodes, migrations, s, m.attempt)
                     and stream_history.get(s) == Set()
      | None => false
    })

  // ext-9 (V4-M1), backed by `coverage.emptyBatch`: a zero-advance coverage batch was
  // admitted as a no-op rather than refused.
  val witnessEmptyCoverageBatchIsANoOp: bool = coverage.emptyBatch

  // ext-10 (V8-C1): the join gate's refusal state — a non-member still physically
  // holding copies. `meetNode` is refused here, which is what keeps an untracked stale
  // copy out of the membership now that the self-cleanse is gone.
  val witnessNonEmptyJoinRefused: bool =
    NODES.exists(n => not(nodes.get(n).member) and nodes.get(n).keys != Set())

  // ext-10 (V8-C1): the same refusal on the reset path, in its non-trivial form — a
  // member holding copies of slots it does **not** own (the state a demotion leaves
  // behind). `canResetCluster` is false here; the initial state, where the holder is also
  // the owner, would satisfy a bare "member with a non-empty keyspace".
  val witnessNonEmptyResetRefused: bool =
    NODES.exists(n => and {
      nodes.get(n).member,
      nodes.get(n).keys != Set(),
      not(ownsAnySlot(slots, n)),
      not(canResetCluster(nodes, n)),
    })

  // ext-12: `Begin` refused over a slot carrying a residue entry — stated as "every other
  // Begin conjunct holds for some endpoint pair, and the proposal is still refused", so
  // it cannot be satisfied by a state where Begin was refused for an unrelated reason.
  val witnessBeginRefusedOverResidue: bool =
    SLOTS.exists(s => and {
      residue.get(s) != None,
      migrations.get(s) == None,
      NODES.exists(src => NODES.exists(t => and {
        slots.get(s) == Some(src),
        src != t,
        nodes.get(t).member,
        not(canBeginMigration(nodes, slots, migrations, residue, s, src, t)),
      })),
    })

  // ext-16/ext-18 (V14-M1): the staged flip whose *adoption* survived the crash. The
  // record is stamped `adopted` by the replicated admission and the node has since
  // restarted (its boot report is owed again), so `completeAdoption` still fires after
  // the crash. Mutation (4) — a restart that does not preserve the record field-for-field
  // — makes this state unreachable.
  val witnessStagedFlipCompletesAcrossCrash: bool =
    NODES.exists(n => and {
      not(nodes.get(n).booted),
      match nodes.get(n).record { | Some(rec) => rec.adopted | None => false },
    })

  // ext-17 (V15-M4): a replica that lost `synced` to a re-parent and re-earned it by
  // attestation is a promotable candidate again — `parent_seq > 0` is the token proving
  // the pointer really was rewritten, so this is not the initial state's free `synced`.
  val witnessResyncedReplicaBecomesCandidate: bool =
    NODES.exists(n => and {
      nodes.get(n).role == Replica,
      nodes.get(n).synced,
      nodes.get(n).parent_seq > 0,
      match nodes.get(n).parent { | Some(p) => isLivePrimary(nodes, p) | None => false },
    })

  // ext-18 (V28-m3): the fence *clears* — a node that a staged flip fenced is unfenced,
  // its record is gone and its client has been paid. The design's step **bound** on this
  // (three steps from the enabling state) is a property of a run, not of a state, so it
  // is pinned by `fenceClearsWithinThreeStepsTest` rather than stated here.
  val witnessFenceClears: bool =
    NODES.exists(n => and {
      not(nodes.get(n).node_fenced),
      nodes.get(n).reply_emitted,
      nodes.get(n).record == None,
    })

  // ext-15/ext-16 (V12-M2/V13-M1), the two-plane convergence claim: a node whose
  // replicated cell and node-local plane had split converged again, replicated -> local,
  // *before* its boot report proposed anything. The producing state is the one V12-M2
  // names — a node that crashed before its failover promotion took local effect, whose
  // old upstream the same forced failover then removed (TR-CLUSTER-042) — and the only
  // action in the model that can leave it is `adoptReplicatedRole`, so both of the doc's
  // mutations at it (deleting it, and narrowing the lineage guard by dropping the
  // removed-upstream disjunct) make this unreachable. Pinned by
  // `splitPlanesConvergeAfterCrashedPromotionTest`.
  //
  // The claim's *silence* half — a detached replica does not loop proposals, because the
  // difference test counts a locally detached link as matching replicated `None` — is a
  // claim about what is **not** enabled, so it is pinned by
  // `detachedReplicaProposesNothingTest` rather than stated as a state predicate here.
  val witnessSplitPlanesConverge: bool = coverage.planesConverged

  // ext-15's liveness claim, in the state the design says must never be mute: a member
  // that has *lost* its node-local incarnation counter (`incarnation == 0`, below every
  // pair the replicated cell can hold) still has an **admissible** next boot report. What
  // makes it admissible is the re-mint rule — `bootMintOf` proposes *above* the replicated
  // pair rather than at the lost `(0, 0)`. The witness reads that same function, so the
  // doc's mutation ("reverting the re-mint rule, minting `(0,0)`") is one edit in
  // `bootMintOf` and makes this state unreachable: every report the node can phrase is
  // then refused `Ordering` against the surviving cell, which is the mute-node defect.
  //
  // `stored_identity != None` is part of the claim, not scenery: with no cell at all
  // `identityOrderOk` is vacuously true and the witness would hold without the rule.
  val witnessNodeCanAlwaysReportIdentity: bool =
    NODES.exists(n => {
      val ns = nodes.get(n)
      val mint = bootMintOf(ns, Boot, false)
      and {
        ns.member,
        ns.incarnation == 0,
        ns.stored_identity != None,
        refusalClassOf(nodes, n, Boot, mint.inc, mint.seq, ns.role, ns.epoch_own) == Whole,
      }
    })

  // --- Declared-unreachable witnesses ----------------------------------------
  //
  // Two, and both are unreachable **because the doc says they must be**: each names a
  // state a superseded revision of the design could reach and the adopted narrowing
  // forbids. Each carries the single revert that makes it reachable again, which is the
  // mutation test the doc asks for. Nothing here is a model gap.

  // ext-13 (V5-M1): the wedged pre-`Confirm` drain the design says the **narrowed**
  // observation reset removes. A source that never proposes `Confirm` (`cannot_drain`)
  // sits in `Streaming` with `drained_pos == None` forever; under v4's reset — which had
  // an "(or `drained_pos` unset)" arm — every tick reset the counter, so the bound never
  // fired and the migration wedged. `hasObservedProgress` no longer reads `drained_pos`,
  // so the conjunction below (no progress observed, yet the tick would still reset the
  // counter) is false by construction. Restoring the v4 arm makes it reachable.
  val witnessDrainingWedgedBeforeConfirm: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => and {
          m.phase == Streaming,
          m.drained_pos == None,
          nodes.get(m.source).cannot_drain,
          not(hasObservedProgress(m)),
          applyReconcileTick(m).observations == 0,
        }
      | None => false
    })

  // ext-13's post-seal mirror, and the sharper of the two: a record whose `Complete` is
  // *already admissible* and whose hold region is still latched, with the target silent
  // — the shape where a counter that never advances would hold client writes forever.
  // Same contradiction, same single revert reaches it.
  val witnessDrainingWedgedWithCompletableToken: bool =
    SLOTS.exists(s => match migrations.get(s) {
      | Some(m) => and {
          canCompleteMigration(nodes, migrations, s, m.attempt),
          held.get(m.source) != Set(),
          nodes.get(m.target).silent,
          not(hasObservedProgress(m)),
          applyReconcileTick(m).observations == 0,
        }
      | None => false
    })

  // V12-M1, ruled 2026-08-19: the *non-vacuity* half of
  // `inv_no_hold_during_staged_flip`. The invariant alone is satisfied by a model in which
  // no staging node ever held anything, so the claim that matters — a staged flip found
  // outstanding held writes and **answered** them rather than waiting on a drain — is
  // stated here as a reachability obligation. Pinned by
  // `stagedFlipAnswersHeldWritesTest`.
  val witnessStagedFlipAnsweredHolds: bool = coverage.stagedFlipAnsweredHolds

  // M37, ruled 2026-08-19: the dataset-discard leg *fired* — some `completeAdoption`
  // found the adopting node holding a copy the replicated map gave it no claim to and
  // dropped it. Without this the discard leg could be deleted and every invariant would
  // stay green. Pinned by `adoptionDiscardsUnclaimedCopyTest`.
  val witnessAdoptionDiscardsUnclaimedCopy: bool = coverage.adoptionDiscardedKeys

  // ---------------------------------------------------------------------------
  // Invariants, grouped by family. A new check is one `inv_*` value added under the
  // family it belongs to — `just quint-run` picks it up automatically
  // (scripts/quint-invariants.sh scrapes the names out of this file).
  //
  //   Ownership and handoff  — who owns a slot, who may be an endpoint, and the
  //                            minting/distinctness of handoff stamps.
  //   Epoch and reset        — the cluster epoch's domination and monotonicity claims.
  //   Fencing and demotion   — the per-object fence and the drain/parity barrier every
  //                            failover-shaped transition must pass, plus the
  //                            demote-don't-remove postcondition.
  //   Commit admission       — what CompleteSlotMigration is allowed to admit.
  //   Drain bounds           — ext-4's observation counter: what it may abort and what it
  //                            is allowed to count.
  //   Barrier and hold       — ext-2/ext-11/ext-15: the hold region's latch and the
  //                            serving-primary rule every release of it owes.
  //   Stream and history     — ext-1/ext-5: what the slot's log may contain and what a
  //                            commit is allowed to lose.
  //   Residue and reaper     — ext-7/ext-8/ext-12: who may hold a copy, who may delete
  //                            one, what a residue entry guarantees, and what the shadow
  //                            owes the ingest.
  //   Identity and run       — ext-15/ext-17/ext-18: the replicated identity's ordering,
  //                            the parenting token, and the record's disposal rules.
  //
  // Every ghost-backed member of these families reads a `defects.*` flag that its action
  // recomputes from its own *outcome*, independent of the guard — see each note below.
  // ---------------------------------------------------------------------------

  // --- Ownership and handoff -------------------------------------------------

  // INV-REF-1: every assigned slot's owner exists as a current member. This model does
  // not claim the owner holds the Primary role — see the header for why.
  val inv_slot_owner_valid: bool =
    SLOTS.forall(s => match slots.get(s) {
      | Some(n) => nodes.get(n).member
      | None => true
    })

  // INV-MIG-1: an open migration's source currently owns the slot, *or the slot is
  // currently unassigned* (check_mig_1's own `?`-early-return exempts a migrating slot
  // with no recorded owner); source != target; both endpoints are members.
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

  // INV-HANDOFF-1, in all three halves. The catalog's bound ("the counter dominates every
  // live seq") *is* stateable as `m.attempt <= ctl.handoff_seq` even across ext-10's
  // rewind, because `ResetCluster` takes **every** migration when it rewinds the counter
  // (TR-CLUSTER-035) — it does not leave a live record behind carrying a stamp minted
  // under the old counter. Alongside the bound the model states the stronger *minting*
  // claim — every live attempt stamp is one this model actually spent under the current
  // reset epoch — which is what `spent` (re-keyed by reset epoch) records and what makes
  // the bound survive a rewind rather than merely coincide with it. The unminted-0 half
  // is verbatim: a sealed source always carries a real stamp.
  val inv_handoff_owned: bool =
    SLOTS.forall(s => match migrations.get(s) {
      | Some(m) => and {
          m.attempt <= ctl.handoff_seq,
          m.attempt >= 1 implies spent.exists(t => t.epoch == ctl.reset_epoch and t.seq == m.attempt),
          m.fenced implies m.attempt >= 1,
        }
      | None => true
    })

  // INV-HANDOFF-2's distinctness half: no handoff sequence number is ever spent twice
  // *under the same reset epoch*. Checked via the `defects.seqReuse` ghost, recomputed
  // inside `prepareHandoff` against the epoch-keyed spent set — which is precisely what
  // makes a post-`ResetCluster` re-mint of seq 1 lawful while a genuine reuse is not.
  val inv_handoff_seq_never_reused: bool = not(defects.seqReuse)

  // --- Epoch and reset -------------------------------------------------------

  // INV-EPOCH-1/2: the cluster epoch dominates every node's own epoch; a nonzero node
  // epoch is unique among current Primaries.
  val inv_epoch_monotone: bool = and {
    ctl.epoch >= NODES.fold(0, (acc, n) => max2(acc, nodes.get(n).epoch_own)),
    NODES.forall(n1 => NODES.forall(n2 =>
      (n1 != n2 and nodes.get(n1).role == Primary and nodes.get(n2).role == Primary
        and nodes.get(n1).epoch_own != 0)
        implies nodes.get(n1).epoch_own != nodes.get(n2).epoch_own
    )),
  }

  // Cross-step half INV-EPOCH-1/2 cannot state: the cluster epoch never decreases. Every
  // action carries `prev_epoch` forward from the state it fired in (see the machine's
  // `keepCtl`), so this compares consecutive states.
  val inv_epoch_never_decreases: bool = ctl.epoch >= ctl.prev_epoch

  // --- Fencing, barrier and demotion -----------------------------------------

  // Issue 20, ruled: no failover-shaped transition — planned, automatic, forced, the
  // source-failover variant, or the administrative `demoteNode` — ever leaves the old
  // primary undemoted (still a Primary), un-parented to its successor, or un-membered.
  // Checked via the `defects.demotion` ghost, which each of those actions recomputes
  // from its own postcondition output (`demotedTo`) rather than from its guard, so a
  // mutant that stops demoting is still caught even though no *guard* protects this.
  val inv_last_failover_demoted: bool = not(defects.demotion)

  // Issue 19, amended: the per-object epoch/role fence, on every failover path including
  // the forced one. Checked via `defects.fence`, recomputed from the pre-action state
  // independent of `fenceOk`'s own guard use.
  val inv_last_failover_fenced: bool = not(defects.fence)

  // Issue 26, ruled: `failoverGraceful` never applies without its drain/parity barrier
  // armed over **every slot it is about to move** (not merely non-empty). Checked via
  // `defects.barrier`, recomputed independent of the guard.
  val inv_graceful_failover_barriered: bool = not(defects.barrier)

  // --- Commit admission ------------------------------------------------------

  // Issue 17, ruled, reworked for the redesign: CompleteSlotMigration never admits
  // without a sealed drain — a `Draining` record whose drained position was minted by the
  // same run as the source log it drained, and a target copy that covers it. Checked via
  // `defects.admission`, recomputed inside `completeMigration` from the record's own
  // evidence rather than from `canCompleteMigration`.
  val inv_complete_requires_drained: bool = not(defects.admission)

  // DELETED with the rework — `inv_feed_hold_bounded` (`feed_bytes <= HOLD_BYTE_CAP`).
  // The cap is no longer an atomic arrival-time bound: ext-2/ext-11 make the breach a
  // separate, locally-scheduled release action, so a state in which the buffer sits above
  // the cap awaiting that release is lawful and reachable. The property that replaces it
  // is about the *held write region* emptying, not about the byte count — and it belongs
  // to the Q2 invariant set (`inv_held_set_empty_while_latched`), which is why nothing is
  // asserted about the cap here rather than a weakened version being left in place.

  // ext-2: the commit never admits against an **unfenced** source. Separate from the
  // phase conjunct below on purpose (ext-3): the seal and the phase are two facts, and
  // `localReleaseOnCapBreach`/`selfFenceRelease` drop the first without touching the
  // second. Ghost recomputed inside `completeMigration` from `m.fenced`.
  val inv_complete_requires_fenced_source: bool = not(defects.fencedAdmit)

  // ext-3: and never against a record still in `Streaming`. The stale-`Confirm` refusal
  // is what keeps the two apart; this is its safety consequence.
  val inv_complete_requires_draining_phase: bool = not(defects.phaseAdmit)

  // ext-6/ext-14 (V5-M3): with `require_replica_ack` captured on the record, the commit
  // admits only against a **non-empty** counted replica set that holds the drained
  // position. An empty set is false, not vacuously true — which is the whole mutation the
  // ghost polices (it is recomputed from `countedReplicasOf` and the drained position, not
  // from `canCompleteMigration`).
  val inv_target_replicas_hold_committed_slot: bool = not(defects.replicaAck)

  // ext-12 (V8-C6/V10-C3): the slot map never hands a slot to a node holding the Replica
  // role — checked at the redesign's own writers (the commit, the three failovers and
  // both demotions), each recomputing the ghost on its **post-state** view and scoped to
  // the nodes it wrote. Deliberately not a whole-state invariant: `canSetRole` lawfully
  // demotes a slot-owning node today (TR-CLUSTER-004 carries no such conjunct and no
  // catalog row claims one), so a state-wide form would fail on an operator action this
  // model is not reworking. Flagged as a declared scope narrowing in the Q2 report.
  val inv_slots_only_assigned_to_primaries: bool = not(defects.replicaSlot)

  // --- Drain bounds ----------------------------------------------------------

  // ext-4 (v2-C4/N-M6): a migration that keeps making ingest progress is never abandoned
  // by the observation bound. Checked via `defects.boundAbort`, recomputed inside
  // `reconcileTick` from the tick's own outcome — dropping the counter reset makes a
  // steadily progressing migration accrue to the bound and trips it.
  val inv_progressing_migration_never_aborts: bool = not(defects.boundAbort)

  // ext-4 (V7-C3). COINED (the doc states the property in prose at the `Confirm` reset
  // without naming it): the post-seal bound counts **post-seal** observations only —
  // `confirmDrained` zeroes the counter, so no pre-`Confirm` observation is ever spent
  // against `draining_observations`. Checked via `defects.sealCounter`, recomputed from
  // the sealed record rather than from the guard.
  val inv_draining_bound_counts_post_seal_observations: bool = not(defects.sealCounter)

  // --- Barrier and hold ------------------------------------------------------

  // ext-11, the *level* form of the design's "the held set empties while the latch
  // holds": a slot sits in some node's held region only while its barrier is still
  // armed. That is what makes a release action for it enabled — `selfFenceRelease`
  // (latched source) and `localReleaseOnCapBreach` (cap breach) are both guarded on
  // `barrierArmed`, and every action that disarms a barrier empties the held region for
  // that slot in the same step. The eventually-empties form is a liveness property Quint
  // invariants cannot state; this is the safety half of it, and the narrowing is
  // declared rather than silent (Q2 report).
  val inv_held_set_empty_while_latched: bool =
    NODES.forall(n => held.get(n).forall(s => barrierArmed(s)))

  // ext-15 (V11-C1): no release of a hold region executes writes at a source that has
  // stopped serving the slot. Checked via `defects.demotedExec`, recomputed inside
  // `abortHandoff`/`cancelMigration` from the slot map and the source's role, so dropping
  // `isServingPrimary` from either release path surfaces here as a forked history rather
  // than as a missing guard.
  val inv_no_execution_after_demotion: bool = not(defects.demotedExec)

  // --- Stream and history ----------------------------------------------------

  // ext-1/ext-5: no acked write is lost at the commit — every position of the record's
  // own run that the slot's log ever produced is covered by the copy the target attested
  // **under its current run**. Checked via `defects.ackLoss`, recomputed inside
  // `completeMigration` from `stream_history` and the record, independent of the guard:
  // collapsing the tagged comparison into a bare integer one (mutation (1)) shows up here
  // as a lost write instead of as a tautology.
  val inv_no_acked_write_lost: bool = not(defects.ackLoss)

  // ext-1: the slot's history is hole-free — every recorded position but the first has
  // its predecessor in the history, so no write vanished between two acked ones.
  //
  // Narrowed, deliberately, to the *position numbering* and not to the run tags: a second
  // migration over the same slot restarts `source_log` at 0, and a source that restarts
  // mid-record keeps counting from where the record was (`sourceWrite` stamps the source's
  // **current** run onto `m.source_log.pos + 1`). Both make a per-run hole-freedom claim —
  // and any "every entry is below the live record's high-water" claim — false by
  // construction rather than by defect. Declared in the Q2 report.
  val inv_stream_history_sound: bool =
    SLOTS.forall(s => stream_history.get(s).forall(p => and {
      p.pos >= 1,
      p.pos == 1 or stream_history.get(s).exists(q => q.pos == p.pos - 1),
    }))

  // --- Residue and reaper ----------------------------------------------------

  // ext-8, the redesign's central claim: the target never *serves* the slot before its
  // promotion is attested. `relabelShadow` is the local flip and writes nothing into the
  // target's main keyspace; only `reportPromoted` does, and it flips the entry to
  // `promoted` in the same step. Stated across the step (`ctl.prev_keys`) so that a copy
  // the target already held before the entry existed — lawful: nothing but the reaper
  // deletes a keyspace entry, so a former owner keeps its copy indefinitely — is exempt,
  // while a copy that *appeared* under an unpromoted entry is not.
  val inv_no_serve_before_attestation: bool =
    SLOTS.forall(s => match residue.get(s) {
      | Some(r) => or {
          r.promoted,
          not(nodes.get(r.target).keys.contains(s)),
          ctl.prev_keys.get(r.target).contains(s),
        }
      | None => true
    })

  // ext-12, source-authoritative-until-commit's other half: while an entry is
  // unpromoted, the source's shard still physically holds the copy — the reaper's
  // attestation gate is what guarantees it. The shard closure matters because a
  // demotion/failover re-points the entry at a shard primary without moving the copy off
  // the node that holds it.
  //
  // The `source_gone` disjunct is a declared narrowing (Q2 report): once the entry is
  // *marked*, ext-12 hands it to the operator arms, and a departure that also detaches
  // the holder from the shard leaves the copy tracked by nothing this model can walk to.
  val inv_source_keeps_its_copy_until_promotion_attested: bool =
    SLOTS.forall(s => match residue.get(s) {
      | Some(r) => r.promoted or r.source_gone or shardHoldsCopy(nodes, r.source, s)
      | None => true
    })

  // ext-7/ext-12: a slot any tracking object still claims — assigned, or carrying a
  // residue entry — is physically held by *someone*. The reaper is the model's only
  // deleter and it is gated on the attestation, so the last copy of a claimed slot can
  // never be taken.
  //
  // Narrowed to "some node", not "some member" (Q2 report): `removeNode` deliberately
  // leaves a departing node's keyspace intact (V8-C1 — the self-cleanse is gone), so the
  // holder of a still-claimed copy is lawfully a non-member for as long as the operator
  // arms take to resolve the entry.
  val inv_slot_copy_survives_until_owned_and_served: bool =
    SLOTS.forall(s =>
      (slots.get(s) != None or residue.get(s) != None)
        implies NODES.exists(q => nodes.get(q).keys.contains(s)))

  // ext-12 (V8-M6). RENAMED from the doc's `residueHasAnEffectiveRemover` to the `inv_`
  // prefix the run script scrapes (scripts/quint-invariants.sh) — an unprefixed name
  // would never enter `just quint-run`. Every residue entry has some verb that resolves
  // it admissible **in this very state**: an admissible-in-principle verb whose own guard
  // is false here is not a remover. See `hasEffectiveRemover` for the disjunct list and
  // for the two marked-entry terminal disjuncts, which are declared weakenings.
  val inv_residue_has_an_effective_remover: bool =
    SLOTS.forall(s => hasEffectiveRemover(nodes, slots, migrations, residue, s))

  // ext-12: no second migration is ever opened over a slot whose residue entry is still
  // outstanding — the state form of `canBeginMigration`'s residue conjunct, and what
  // keeps a stale entry from being silently re-used by the next record.
  val inv_begin_refuses_slot_with_residue: bool =
    SLOTS.forall(s => residue.get(s) != None implies migrations.get(s) == None)

  // ext-7: the reaper never deletes the last member-held copy of a slot. Checked via
  // `defects.overDelete`, recomputed inside `reapSlots` from the outcome rather than
  // from `reapableSlots` — reverting the attestation gate reaps ahead of
  // `reportPromoted`, when the target holds nothing yet, and this sees it.
  val inv_node_keeps_slots_it_owns: bool = not(defects.overDelete)

  // ext-8: a shadow is never discarded out from under a residue entry that still claims
  // it — the copy the new owner will serve. Checked via `defects.discard`, recomputed
  // from the residue map inside `discardShadow`, because the relabel/discard pair cannot
  // be told apart afterwards (both leave `shadow[s] == None`).
  val inv_owner_serves_promoted_slot: bool = not(defects.discard)

  // ext-10 (V8-C1): no member holds a copy that no tracking object reaches. Stated in its
  // **gate** form — the ghost is recomputed at the two entry points that could admit an
  // untracked copy (`meetNode`'s join, `resetCluster`'s wholesale unassign) from their own
  // outcomes. A state-wide form would be false by design: `removeSlot` (`CLUSTER
  // DELSLOTS`) lawfully leaves an operator holding an unclaimed copy, and neither it nor
  // the departing-node case is a migration writer. Declared in the Q2 report.
  val inv_member_keyspace_is_tracked: bool = not(defects.untracked)

  // ext-8/ext-10: an open record's shadow is the one **it** minted, under the current
  // reset epoch — Begin's discard-then-ingest re-mints whatever the slot carried, so an
  // orphan shadow from a pre-reset epoch can never wedge the next migration's ingest.
  val inv_no_orphan_shadow_blocks_ingest: bool =
    SLOTS.forall(s => match migrations.get(s) {
      | Some(m) => match shadow.get(s) {
          | Some(t) => t.mig == m.id and t.epoch == ctl.reset_epoch
          | None => true
        }
      | None => true
    })

  // ext-9. COINED (the doc states it as "neither the batch nor the fault discards the
  // shadow" without naming a property): an open record always has its shadow. A batch
  // fault rolls the volatile applied point back to the durable one and touches nothing
  // else, so no ingest-side fault can strand a record with nothing to promote.
  val inv_open_migration_keeps_its_shadow: bool =
    SLOTS.forall(s => migrations.get(s) != None implies shadow.get(s) != None)

  // --- Identity and run ------------------------------------------------------

  // ext-15: an *applied* run-identity report always strictly dominated the cell it
  // overwrote. Checked via `defects.identityRegress`, recomputed inside
  // `reportRunIdentity` from the report's own operands rather than from
  // `refusalClassOf`'s verdict.
  val inv_run_identity_never_regresses: bool = not(defects.identityRegress)

  // ext-15 (V10-M1): records are taken by the identity **field write**, never by mere
  // admission — a topology-only re-proposal at equality cancels nothing. Checked via
  // `defects.spuriousCancel`.
  val inv_no_spurious_cancel: bool = not(defects.spuriousCancel)

  // ext-15 (V8-C2): a `Demotion` arm never rewrites role and parent against an observed
  // role/epoch the node no longer holds — the fence that keeps a freshly promoted node
  // from being demoted back by a report minted before its promotion. Checked via
  // `defects.staleDemote`.
  val inv_promotion_is_not_reverted_without_a_failover: bool = not(defects.staleDemote)

  // ext-18 (V21): a terminal refusal observed against its **own** record disposes of all
  // three obligations in the same step — the record cleared, the fence dropped, the
  // client paid. Checked via `defects.disposition`, recomputed from the post-state, so
  // turning any one arm into a silent no-op trips it.
  val inv_every_terminal_refusal_has_a_disposition: bool = not(defects.disposition)

  // ext-18 (V28-M6): no live record is ever disposed of by a **foreign** refusal. Read
  // off `last_disposition`, the auxiliary history `observeRefusal` writes on a step that
  // disposes and on no other: both halves of the (stage_id, registration_seq) pair must
  // have matched. Dropping either half of `refusalBinds` lets a refusal minted for a dead
  // stage clear a record minted after a forget-and-rejoin, and that shows up here.
  val inv_no_live_record_disposed_by_a_foreign_refusal: bool =
    NODES.forall(n => match nodes.get(n).last_disposition {
      | Some(d) => d.refusal_stage_id == d.record_stage_id
                     and d.refusal_reg_seq == d.record_reg_seq
      | None => true
    })

  // ext-17 (V16-C1): the parent pointer is only ever written through the single funnel
  // that also moves the parenting token. Stated across the step against `ctl.prev_parents`:
  // a pointer that changed without `parent_seq` strictly increasing is a write that
  // bypassed `writeParentPointer` — i.e. a writer the design did not declare.
  val inv_role_written_only_by_declared_writers: bool =
    NODES.forall(n =>
      nodes.get(n).parent != ctl.prev_parents.get(n).p
        implies nodes.get(n).parent_seq > ctl.prev_parents.get(n).seq)

  // ext-17 (V18-C1): every live registration cell was minted from the cluster counter —
  // no cell carries a generation the cluster has not issued, which is what makes
  // `staged_reg_seq` a usable pairing partner for a stage id.
  val inv_registration_gen_monotone: bool =
    NODES.forall(n => match nodes.get(n).registration_seq {
      | Some(r) => r < ctl.next_reg
      | None => true
    })

  // ext-18 + V12-M1, ruled 2026-08-19: **the staged-fence level rule**. In every state
  // with a pending transition record, that node's held set is empty — a write scheduled
  // at a staged node is answered (`-TRYAGAIN`), never held, so no client ever waits out a
  // partition on the staged-flip fence and a demotion is never blockable by a wedged
  // drain. Closed by effect, not by guard (the ruling rejected the guard form for wiring
  // demotion liveness to drain-release liveness): `stageFlip` empties the hold region in
  // the same step and `prepareHandoff`'s `canLatchHold` refuses to latch a new one
  // underneath a pending record. Reverting either half and scheduling a hold makes this
  // false — see `stagedFlipAnswersHeldWritesTest` / `noHoldLatchesUnderPendingRecordTest`.
  val inv_no_hold_during_staged_flip: bool =
    NODES.forall(n => nodes.get(n).record != None implies held.get(n) == Set())

  // ext-12 (V9-C1), M32 ruled 2026-08-19: a demotion never hands a node's slots to a
  // successor outside its shard. Stated over the `crossShardSuccessor` ghost because the
  // fact has **no post-state trace** — `applyFailoverCommon` promotes the successor and
  // re-parents the demoted node onto it either way — so the pre-state recompute in
  // `demoteNode` is the only observable. Deliberately structural: whether the successor
  // actually holds the shard's data is phase-3 replication-model territory.
  val inv_no_cross_shard_successor: bool = not(defects.crossShardSuccessor)

  // TR-CLUSTER-025 / FM-CLUSTER-014, ruled 2026-08-19: the anti-churn rule. A FAIL-flag
  // transition carries health news and must never bump the config epoch — a flapping node
  // that bumped on every transition would churn every other member's view. Off the
  // postcondition ghost both `markNodeFailed` and `markNodeRecovered` write, so a mutant
  // that bumps the epoch there trips this rather than hiding inside `keepCtl`.
  val inv_no_churn_epoch_bump: bool = not(defects.churnEpochBump)

  // ---------------------------------------------------------------------------
  // Stated in the doc, NOT stated here — with the reachable counter-state each one is
  // pinned by, because a property that is false of the model must be a test, not a
  // weakened invariant.
  //
  // - `inv_no_replica_without_a_primary_pointer`: withdrawn by ext-17 (V15-C3) itself —
  //   `removeNode`'s detach writes `parent == None` at a Replica, a lawful state.
  // - `inv_abort_repatriates`: deleted with repatriation (MAJ-5).
  // - `inv_no_hold_during_staged_flip`: **no longer here** — closed by the 2026-08-19
  //   ruling (V12-M1) and asserted above. The action-semantics change this block declined
  //   to make is exactly the one that was ruled: `stageFlip` answers and empties the hold
  //   region it finds, and `prepareHandoff` will not latch a new one at a node carrying a
  //   pending record.
  // - `inv_no_record_outlives_its_registration`: its strict form is unsatisfiable by
  //   construction, and its guarded form is a tautology. The record is node-local and
  //   crash-durable, so it deliberately survives the loss of its registration cell
  //   (ext-18) — that gap is precisely what `clearStaleRecord` exists to close, and that
  //   clause is separately schedulable, so the gap state is reachable
  //   (`recordOutlivesRegistrationTest`). Adding "…or the clearing clause is enabled" as
  //   a disjunct restates `isRecordStale`, whose definition is exactly the negation of
  //   the pairing — a vacuous invariant, which the brief forbids.
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Tests.
  // ---------------------------------------------------------------------------

  // (a) The full happy path, end to end: begin -> prepare -> confirm -> complete ->
  // relabel -> attest -> reap. The point of the redesign is visible at the `complete`
  // step: ownership has moved to the target, but node 1 still holds its copy of the slot
  // and no one may take it until the promotion is attested.
  run happyMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .expect(and {
        match migrations.get(1) {
          | Some(m) => m.source == 1 and m.target == 4 and m.attempt == 0 and m.phase == Streaming
          | None => false
        },
        // ext-10: Begin ingests under the current reset epoch.
        shadow.get(1) == Some({ mig: 1, epoch: 0 }),
      })
      .then(prepareHandoff(1))
      .expect(and {
        barrierArmed(1),
        held.get(1) == Set(1),
        match migrations.get(1) { | Some(m) => m.attempt == 1 and m.fenced | None => false },
      })
      .then(confirmDrained(1, 1))
      .expect(match migrations.get(1) { | Some(m) => m.phase == Draining | None => false })
      .then(completeMigration(1, 1))
      .expect(and {
        slots.get(1) == Some(4),
        migrations.get(1) == None,
        not(barrierArmed(1)),
        held.get(1) == Set(),
        // The source is still a holder; the residue entry says so, un-attested.
        nodes.get(1).keys.contains(1),
        residue.get(1) == Some({ mig: 1, source: 1, target: 4, promoted: false,
                                 source_gone: false, target_gone: false }),
        // The target does NOT hold the slot in its own keyspace yet.
        not(nodes.get(4).keys.contains(1)),
      })
      .then(relabelShadow(1))
      .expect(and { shadow.get(1) == None, not(nodes.get(4).keys.contains(1)) })
      .then(reportPromoted(1))
      .expect(and {
        nodes.get(4).keys.contains(1),
        match residue.get(1) { | Some(r) => r.promoted | None => false },
      })
      .then(reapSlots(1))
      .expect(and { not(nodes.get(1).keys.contains(1)), nodes.get(4).keys.contains(1) })
      .then(clearSlotResidue(1))
      .expect(residue.get(1) == None)
  }

  // (a1) The core of source-authoritative-until-commit: after the commit but before the
  // attestation, the reaper is refused — the source's copy is the only attested one.
  run sourceKeepsCopyUntilAttestedTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .expect(nodes.get(1).keys.contains(1))
      .then(reapSlots(1).fail())
  }

  // (a2) The rollback-wins-the-race trace the promotion split exists for: the local flip
  // has happened, the attestation has not, and the promotion fails. Ownership returns to
  // the source (which never stopped being authoritative), the staged copy goes, and the
  // target never held the slot in its own keyspace at any point.
  run promotionRollsBackToSourceTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(relabelShadow(1))
      .then(failPromotion(1))
      .expect(and {
        slots.get(1) == Some(1),
        shadow.get(1) == None,
        nodes.get(1).keys.contains(1),
        not(nodes.get(4).keys.contains(1)),
        match residue.get(1) { | Some(r) => not(r.promoted) | None => false },
      })
  }

  // (a2b) Regression, found by the random walk at 8000x60 (it predates this revision):
  // the rollback hands the slot back to the entry's **source**, so it must be refused once
  // that source has departed — otherwise it lands an assigned slot on a non-member and
  // falsifies INV-REF-1. The entry's recovery in that state is `retargetSlotResidue`, not
  // a rollback.
  run failPromotionRefusedAfterSourceDepartedTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(relabelShadow(1))
      // Force is needed: node 1 still owns the other three slots.
      .then(removeNode(1, true, false))
      .expect(and {
        not(nodes.get(1).member),
        match residue.get(1) { | Some(r) => r.source_gone and not(r.promoted) | None => false },
      })
      .then(failPromotion(1).fail())
  }

  // (a3) An outstanding residue entry blocks the next migration over the same slot, and
  // blocks the shadow discard — the entry is the record of unfinished business.
  run residueBlocksNextMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .expect(residue.get(1) != None)
      .then(beginMigration(1, 4, 2, false).fail())
  }

  run discardShadowRefusedWithResidueTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(discardShadow(1).fail())
  }

  // (a4) ext-12's mark-don't-remove plus ext-7's `target_gone` defer: the attestation has
  // landed, so the promoted gate is satisfied — but the target has departed, and the
  // reaper must hold off rather than delete the last remaining copy.
  run reapDeferredWhileTargetGoneTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      .then(removeNode(4, true, false))
      .expect(and {
        match residue.get(1) { | Some(r) => r.promoted and r.target_gone | None => false },
        // Marked, never removed.
        residue.get(1) != None,
        nodes.get(1).keys.contains(1),
      })
      .then(reapSlots(1).fail())
  }

  // (b1) Graceful failover before any handoff was ever prepared: migration open, no
  // barrier armed yet; failover of the source still prunes the migration. The barrier is
  // armed immediately before the swap, per its single-shot discipline.
  run gracefulFailoverBeforePrepareTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
        nodes.get(1).parent == Some(3),
        nodes.get(3).role == Primary,
        coverage.gracefulPruned,
      })
  }

  // (b1a) Without arming the barrier first, the same graceful failover is refused —
  // TR-CLUSTER-017's ruled drain/parity precondition, a real guard.
  run gracefulFailoverRefusesWithoutBarrierTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(failoverGraceful(1, 3, 0, Primary).fail())
  }

  // (b2) Graceful failover while the handoff is armed but not drained: the barrier and
  // the source's held writes must be released by the prune, not left orphaned.
  run gracefulFailoverArmedNotDrainedTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .expect(and { barrierArmed(1), held.get(1) == Set(1) })
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        not(barrierArmed(1)),
        held.get(1) == Set(),
        nodes.get(1).role == Replica,
        nodes.get(1).member,
      })
  }

  // (b3) Graceful failover while the handoff is drained but not yet completed. Under the
  // redesign there is nothing to stream back — ownership never moved — so the prune is a
  // clean one-step cutover, and the target's now-orphaned shadow is cleaned up
  // separately by `discardShadow` (which is admissible precisely because the prune left
  // no record and no residue behind).
  run gracefulFailoverDrainedNotCompleteTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(match migrations.get(1) { | Some(m) => m.phase == Draining | None => false })
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and {
        migrations.get(1) == None,
        residue.get(1) == None,
        not(barrierArmed(1)),
        // Ownership never moved: the slot is the successor's, not the migration
        // target's.
        slots.get(1) == Some(3),
        shadow.get(1) != None,
      })
      .then(discardShadow(1))
      .expect(shadow.get(1) == None)
  }

  // (c) Cancel, reworked: with the source authoritative until commit, Cancel resolves the
  // record in a single step — no repatriating phase, no residue, and the slot is still
  // the source's.
  run cancelAppliesInOneStepTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(cancelMigration(1, 1))
      .expect(and {
        migrations.get(1) == None,
        residue.get(1) == None,
        slots.get(1) == Some(1),
        not(barrierArmed(1)),
        held.get(1) == Set(),
        // Only the target's staged copy survives, and it is discardable.
        shadow.get(1) != None,
      })
      .then(discardShadow(1))
      .expect(shadow.get(1) == None)
  }

  // (c1) Abort releases the handoff and the source's seal; the migration itself survives
  // and can be re-prepared.
  run abortHandoffKeepsMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(abortHandoff(1, 1))
      .expect(and {
        match migrations.get(1) {
          | Some(m) => m.attempt == 0 and not(m.fenced) and m.phase == Streaming and m.drained_pos == None
          | None => false
        },
        not(barrierArmed(1)),
        held.get(1) == Set(),
      })
      .then(prepareHandoff(1))
      .expect(match migrations.get(1) { | Some(m) => m.attempt == 2 and m.fenced | None => false })
  }

  // (c2) Re-preparing over a live handoff supersedes it (FM-CLUSTER-085): the old attempt
  // stamp becomes stale and is thereafter refused by every attempt-checked action
  // (ext-3).
  run prepareSupersedesLiveHandoffTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .expect(match migrations.get(1) { | Some(m) => m.attempt == 1 | None => false })
      .then(prepareHandoff(1))
      .expect(match migrations.get(1) { | Some(m) => m.attempt == 2 and m.phase == Streaming | None => false })
      .then(confirmDrained(1, 1).fail())
  }

  // (c3) ext-1/ext-5: the commit's evidence is run-tagged, and a restart at the target
  // re-mints its run — so the ingest report the record is carrying was attested by a run
  // the target no longer has, and `Complete` must refuse it rather than trust a position
  // from a forked history.
  run completeRefusesCrossRunTargetCopyTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(targetRestart(4))
      .expect(and {
        nodes.get(4).run_id == 5,
        match migrations.get(1) { | Some(m) => m.target_copy.run_id == 4 | None => false },
      })
      .then(completeMigration(1, 1).fail())
  }

  // ---------------------------------------------------------------------------
  // The commit's own conjuncts, one refusal per operand (Q3 Finding 0).
  //
  // The random walk does not reach `completeMigration` at all at the sizings the gates
  // use, so the commit family's invariants are vacuous under the walk and these
  // deterministic refusals are their only oracle. Each schedule is built so that
  // **exactly one** `canCompleteMigration` conjunct is false — every other one is
  // asserted true in the `.expect` immediately above the refusal — so dropping that one
  // conjunct turns exactly one of these `.fail()` steps into an enabled action.
  // ---------------------------------------------------------------------------

  // ext-1 (V4-C2): the drain tag. A source restart between `Prepare` and `Confirm` seals
  // the record with a position minted under the **new** run while the log the record is
  // draining is the old one — the replicated cell still says the old run (the boot report
  // has not applied), so every other conjunct, `sourceRunIdentityOk` included, still
  // holds. Comparing the two positions run-tag-blind admits a commit over a forked log.
  run completeRefusesCrossRunDrainTagTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(sourceRestart(1))
      .then(confirmDrained(1, 1))
      .expect(and {
        // Everything the commit reads except the tag comparison.
        isLivePrimary(nodes, 4),
        match migrations.get(1) { | Some(m) => sourceRunIdentityOk(nodes, m) | None => false },
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 1, m.fenced, m.phase == Draining,
                             m.target_copy.run_id == nodes.get(4).run_id,
                             m.drained_pos == Some({ run_id: 5, pos: 0 }),
                             // ... and the one operand that is false: the drain was
                             // minted under run 5, the log under run 1.
                             m.source_log.run_id == 1 }
          | None => false
        },
      })
      .then(completeMigration(1, 1).fail())
  }

  // ext-2 (v2-C2): the seal. The cap breach releases the source's own fence and leaves
  // the phase alone (TR-CLUSTER-034), which is the only state in the model where a
  // `Draining`, drained record is **unsealed** — writes may resume at the source while
  // the commit still thinks it is holding a drained log.
  run completeRefusesUnsealedSourceTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(localReleaseOnCapBreach(1))
      .expect(and {
        isLivePrimary(nodes, 4),
        match migrations.get(1) { | Some(m) => sourceRunIdentityOk(nodes, m) | None => false },
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 1, m.phase == Draining,
                             m.drained_pos == Some({ run_id: 1, pos: 0 }),
                             m.target_copy.run_id == nodes.get(4).run_id,
                             m.target_copy.pos >= 0,
                             // The one false operand.
                             not(m.fenced) }
          | None => false
        },
      })
      .then(completeMigration(1, 1).fail())
  }

  // ext-3 (v2-C3): the phase. Re-preparing over a drained record supersedes the attempt
  // and re-opens the stream — `drained_pos` survives the supersede (it is the evidence of
  // a drain that *did* happen), so this is the state where a phase-blind commit would
  // admit on stale drain evidence while the source is streaming again.
  run completeRefusesResealedRecordTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(prepareHandoff(1))
      .expect(and {
        isLivePrimary(nodes, 4),
        match migrations.get(1) { | Some(m) => sourceRunIdentityOk(nodes, m) | None => false },
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 2, m.fenced,
                             m.drained_pos == Some({ run_id: 1, pos: 0 }),
                             m.source_log.run_id == 1,
                             m.target_copy.run_id == nodes.get(4).run_id,
                             // The one false operand.
                             m.phase == Streaming }
          | None => false
        },
      })
      .then(completeMigration(1, 2).fail())
  }

  // ext-12 (V10-C3): the target's role. Demoting the target mid-`Draining` leaves a
  // record whose every other conjunct still holds; committing it would hand the slot to a
  // Replica (`inv_slots_only_assigned_to_primaries`, via `defects.replicaSlot`).
  run completeRefusesDemotedTargetTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      // Node 1 is the only other live primary at `init` (2 and 3 are its replicas), so it
      // is the upstream the demotion has to name; the migration is untouched by it.
      .then(setRole(4, Replica, Some(1)))
      .expect(and {
        nodes.get(4).role == Replica,
        nodes.get(4).member,
        not(nodes.get(4).fail),
        match migrations.get(1) { | Some(m) => sourceRunIdentityOk(nodes, m) | None => false },
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 1, m.fenced, m.phase == Draining,
                             m.drained_pos == Some({ run_id: 1, pos: 0 }),
                             m.target_copy.run_id == nodes.get(4).run_id,
                             m.target_copy.pos >= 0 }
          | None => false
        },
      })
      .then(completeMigration(1, 1).fail())
  }

  // ext-5 (V9-C2): the ingest report is drawn from the **durable** point, never from the
  // volatile applied point. Two applies, one durability step: the report must carry 1,
  // not 2 — the position a power-loss restart would keep. Reverting the guard reports the
  // volatile point, and the commit then admits over a position the target can lose.
  run targetReportIsDrawnFromDurableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(sourceWrite(1))
      .then(sourceWrite(1))
      .then(applyAtTarget(1))
      .then(makeTargetDurable(1))
      .then(applyAtTarget(1))
      .expect(match migrations.get(1) {
        | Some(m) => m.target_applied == 2 and m.target_durable == 1
        | None => false
      })
      .then(reportTargetIngest(1))
      .expect(match migrations.get(1) {
        | Some(m) => m.target_copy == { run_id: 4, pos: 1 }
        | None => false
      })
      // And the restart takes exactly the volatile half back.
      .then(targetRestart(4))
      .expect(match migrations.get(1) {
        | Some(m) => m.target_applied == 1 and m.target_durable == 1
        | None => false
      })
  }

  // ext-1/ext-5 (N-C2): the restart-then-write window *within one record* — the state a
  // run-conditioned history hole would need — is closed by two rules acting in series,
  // and this test is what says so. A physically restarted source is not booted, so
  // `sourceWrite` is refused (`canAct`); the only thing that lifts that guard is the boot
  // report, whose identity field write takes every record the node sources with it. So a
  // record never sees two runs of its own source writing into it, and `stream_history`
  // per record is single-run by construction rather than by an asserted tag.
  run restartedSourceCannotWriteWithinItsRecordTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(sourceWrite(1))
      .expect(and {
        stream_history.get(1) == Set({ run_id: 1, pos: 1 }),
        inv_stream_history_sound,
      })
      .then(sourceRestart(1))
      .expect(and {
        nodes.get(1).run_id == 5,
        not(nodes.get(1).booted),
        // The record is still there — the restart alone does not take it — but the source
        // cannot write into it.
        migrations.get(1) != None,
        not(canAct(nodes, 1)),
      })
      .then(reportRunIdentity(1, Boot, Primary, 0, false))
      .expect(and {
        nodes.get(1).booted,
        // ... and the field write that lifted the guard took the record.
        migrations.get(1) == None,
        stream_history.get(1) == Set({ run_id: 1, pos: 1 }),
        inv_stream_history_sound,
      })
      .then(sourceWrite(1).fail())
  }

  // ---------------------------------------------------------------------------
  // ext-12 (V8-C4/V10-C4): the residue's two recovery arms, driven to the end.
  // ---------------------------------------------------------------------------

  // The **level-triggered source arm**. A bare replicated demotion of the source is the
  // one demotion path that does not re-home the entry in-apply, so the entry is left
  // naming a node that is no longer a primary and `retargetSlotResidue` is the verb that
  // has to move it. (The other slots are unassigned first so the demotion does not leave
  // a Replica owning slots.)
  //
  // R1 (issue 41): the arm now re-homes **only onto a physical holder**, so the trace
  // attests the promotion first — `reportPromoted` is what puts the copy into the new
  // owner's own keyspace — and demotes the source under it afterwards. The refusal half
  // (a shard primary that holds nothing) is `retargetRefusedOntoNonHoldingPrimaryTest`.
  run demotedSourceResidueRetargetedTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(removeSlot(2))
      .then(removeSlot(3))
      .then(removeSlot(4))
      // The attestation: only now does node 4 hold slot 1 in its own keyspace.
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      // Node 1 is demoted under the new owner — the entry is left naming a node that is
      // no longer a primary, and the shard primary it would move to is a real holder.
      .then(setRole(1, Replica, Some(4)))
      .expect(and {
        shardPrimary(nodes, 1) == Some(4),
        nodes.get(4).keys.contains(1),
        canRetargetSlotResidue(nodes, residue, 1),
        inv_residue_has_an_effective_remover,
        match residue.get(1) { | Some(r) => r.source == 1 | None => false },
      })
      .then(retargetSlotResidue(1))
      .expect(and {
        match residue.get(1) {
          | Some(r) => and { r.source == 4, not(r.source_gone), r.target == 4 }
          | None => false
        },
        // The entry's new source holds the copy in its own keyspace — the re-home moved
        // the claim onto the data, not onto a closure edge.
        nodes.get(4).keys.contains(1),
        not(defects.untracked),
        inv_member_keyspace_is_tracked,
      })
  }

  // R1 (issue 41), the tightened guard's forcing test — and the trace t5-blocked.md's
  // counterexample used to run through. Node 1 is the residue's source and the only
  // physical holder of slot 1; it is demoted under node 3, which holds nothing. Before
  // the narrowing the entry re-homed onto node 3 and its whole copy claim came to rest on
  // the single closure edge `1 -> 3`, which the very next demotion cut. The re-home is
  // refused instead, the entry stays pinned to the holder, and the four-invariant family
  // survives the demotion that used to break it.
  run retargetRefusedOntoNonHoldingPrimaryTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(removeSlot(2))
      .then(removeSlot(3))
      .then(removeSlot(4))
      .then(setRole(3, Primary, None))
      .then(setRole(1, Replica, Some(3)))
      .expect(and {
        shardPrimary(nodes, 1) == Some(3),
        not(nodes.get(3).keys.contains(1)),
        not(canRetargetSlotResidue(nodes, residue, 1)),
        inv_residue_has_an_effective_remover,
      })
      // The chained demotion that used to cut the claim: node 3 goes under node 4. With
      // the dependants repointed, node 1 follows to node 4 — and because the entry never
      // left the physical holder, the claim does not depend on either edge.
      .then(setRole(3, Replica, Some(4)))
      .expect(and {
        match residue.get(1) { | Some(r) => r.source == 1 and not(r.promoted) | None => false },
        nodes.get(1).keys.contains(1),
        nodes.get(1).parent == Some(4),
        inv_source_keeps_its_copy_until_promotion_attested,
        inv_slot_copy_survives_until_owned_and_served,
        inv_no_serve_before_attestation,
        inv_residue_has_an_effective_remover,
      })
      .then(retargetSlotResidue(1).fail())
  }

  // R1 (issue 41), the staged-flip demotion's structural guard. A record staged against an
  // upstream that is a Replica cannot adopt: this arm writes `parent`, so it owes
  // TR-CLUSTER-004 the same rule `canSetRole` enforces (`isLivePrimary(upstream)`). Without
  // it the demoted node lands in a shard with no primary at all — the `1 <-> 4` cycle
  // t5-blocked.md recorded pre-repoint, a parentless Replica post-repoint — and the entry
  // sourced there loses `shardHoldsCopy` and every remover disjunct at once. The block is
  // not a refusal: no `Refusal` is minted, and the record adopts unchanged once the
  // upstream is a primary again.
  run demotionRefusedOntoNonPrimaryUpstreamTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      // Node 3 is a replica of node 1: staging against it is admissible, adopting is not.
      .then(stageFlip(1, 3))
      .expect(and {
        not(isLivePrimary(nodes, 3)),
        match nodes.get(1).record { | Some(rec) => rec.upstream == 3 | None => false },
        nodes.get(1).role == Primary,
        refusals == Set(),
        inv_source_keeps_its_copy_until_promotion_attested,
        inv_residue_has_an_effective_remover,
      })
      .then(reportRunIdentity(1, Demotion, Primary, 0, false).fail())
  }

  // The other half of the guard: the block is not a refusal and not a disposal — the very
  // same record adopts unchanged once its upstream is a live primary again.
  run demotionAdoptsOnceUpstreamIsPrimaryTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(stageFlip(1, 3))
      .then(setRole(3, Primary, None))
      .then(reportRunIdentity(1, Demotion, Primary, 0, false))
      .expect(and {
        demotedTo(nodes, 1, 3),
        refusals == Set(),
        inv_source_keeps_its_copy_until_promotion_attested,
        inv_residue_has_an_effective_remover,
      })
  }

  // R1 (issue 41), the repoint at the **staged-flip adoption** site. `setRole` is not the
  // only Primary -> Replica writer: the `reportRunIdentity` Demotion arm is the one the
  // protocol actually uses, and its dependants must follow the same way. Deleting the
  // `repointDependants` call there leaves nodes 2 and 3 parented at a Replica — the
  // `1 -> 4` chain the one-hop `shardPrimary` cannot walk, which is exactly issue 41's
  // second sub-root. The flat walk is blind to it (battery row R1-1b), so this is the
  // detector.
  run adoptionRepointsDependantsTest: bool = {
    init
      .expect(and { nodes.get(2).parent == Some(1), nodes.get(3).parent == Some(1) })
      .then(stageFlip(1, 4))
      .then(reportRunIdentity(1, Demotion, Primary, 0, false))
      .expect(and {
        demotedTo(nodes, 1, 4),
        // The dependants followed rather than chaining off a Replica.
        nodes.get(2).parent == Some(4),
        nodes.get(3).parent == Some(4),
        shardPrimary(nodes, 2) == Some(4),
        shardPrimary(nodes, 1) == Some(4),
      })
  }

  // R1 (issue 41), the repoint at the **replica-attach** site. `attachTargetReplica` turns
  // a primary into a replica of the migration target, so it owes the same rule; without it
  // nodes 2 and 3 hang off the freshly demoted node 1. Battery row R1-1c.
  run attachTargetReplicaRepointsDependantsTest: bool = {
    init
      .then(attachTargetReplica(1, 4))
      .expect(and {
        demotedTo(nodes, 1, 4),
        nodes.get(2).parent == Some(4),
        nodes.get(3).parent == Some(4),
        shardPrimary(nodes, 2) == Some(4),
      })
  }

  // R1 (issue 41), the companion of the repoint: the residue follows the demotion when —
  // and only when — the copy has moved out of the source's own closure. Both halves of that
  // rule are in one trace, and R9 (issue 41, 2026-08-20) is what re-cut the first half.
  //
  // Leg 1, the **pin** (R9's source arm, root 2). The failover moves node 1's own slots to
  // node 2, but not slot 1 — that one is already owned by the migration target — and node 1
  // keeps the only copy of it. `retargetResidueOnDemotion` used to move the entry's `source`
  // to node 2 anyway, leaving the copy claim resting on the single closure edge `1 -> 2`
  // that the next role change cuts. It now refuses: node 2 does not hold slot 1, so the
  // entry stays pinned to node 1, which does.
  //
  // Leg 2, the **follow** (R1-4a). Once the promotion is attested and node 1 has reaped its
  // copy, node 1 no longer holds slot 1 and node 4 does. Re-parenting node 1 under node 4
  // now moves the entry's `source` with the copy: `residueFollowsDemotion`'s two post-state
  // conjuncts are both satisfied. Pinning it to a node holding nothing would leave the entry
  // naming a node with no copy; moving it to a shard that does not reach the copy would be
  // the M37 escalation the ruling forbids, which is why the move is conditioned on the
  // post-state closure and not on the demotion alone.
  run residueFollowsDemotedSourceTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(demoteNode(1, 2))
      .expect(and {
        demotedTo(nodes, 1, 2),
        // R9: the claim stays on the physical holder, not on the successor.
        match residue.get(1) { | Some(r) => r.source == 1 | None => false },
        not(nodes.get(2).keys.contains(1)),
        nodes.get(1).keys.contains(1),
        inv_source_keeps_its_copy_until_promotion_attested,
        inv_residue_has_an_effective_remover,
      })
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      .then(reapSlots(1))
      .expect(and {
        match residue.get(1) { | Some(r) => r.promoted and r.source == 1 | None => false },
        not(nodes.get(1).keys.contains(1)),
        nodes.get(4).keys.contains(1),
      })
      .then(setRole(1, Replica, Some(4)))
      .expect(and {
        // The claim followed the copy into node 4's shard.
        match residue.get(1) { | Some(r) => r.source == 4 | None => false },
        shardHoldsCopy(nodes, 4, 1),
        inv_source_keeps_its_copy_until_promotion_attested,
        inv_slot_copy_survives_until_owned_and_served,
        inv_residue_has_an_effective_remover,
        not(defects.untracked),
      })
  }

  // ext-15 (V9-M4): `clearSlotResidue` is narrowed to a **promoted** entry, and this is
  // the state that says why. While the entry is unpromoted it is the only object naming
  // the source's copy, and the reaper is gated on `promoted` too — so clearing it here
  // would strand that copy with no verb left able to take it. The verb refuses, and the
  // keyspace stays tracked.
  run unpromotedResidueNotClearableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .expect(and {
        match residue.get(1) { | Some(r) => r.source == 1 and not(r.promoted) | None => false },
        nodes.get(1).keys.contains(1),
        not(canClearSlotResidue(nodes, slots, migrations, residue, 1)),
        // ... and the reap-then-clear terminal state is not available either.
        not(reapableSlots(nodes, migrations, residue, 1).contains(1)),
        inv_member_keyspace_is_tracked,
      })
      .then(clearSlotResidue(1).fail())
  }

  // R9 (issue 41), ruled 2026-08-20 — the **rollback-completed** clear arm, and the state
  // that says why it cannot be gated on a live primary. `failPromotion` hands slot 1 back
  // to the entry's source and keeps the entry unpromoted; the attestation path that would
  // set `promoted` is gone with the ownership, so V9-M4's `promoted` conjunct on its own
  // leaves the entry with nothing but `canFailPromotion` re-firing — an idempotent
  // re-assert that removes nothing. Demoting the source (which R1 correctly leaves holding
  // the copy and the claim) takes even that away: before this arm every disjunct of
  // `hasEffectiveRemover` was false and the entry was unreapable, which is root 3 of
  // issue 41. The clear writes no slot assignment, so the reason `canFailPromotion` is
  // role-gated does not apply to it.
  run rolledBackResidueClearableAfterSourceDemotionTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(failPromotion(1))
      .expect(and {
        // Ownership is home; the entry survives the rollback unpromoted.
        slots.get(1) == Some(1),
        match residue.get(1) { | Some(r) => not(r.promoted) and r.source == 1 | None => false },
        canClearSlotResidue(nodes, slots, migrations, residue, 1),
      })
      .then(attachTargetReplica(1, 4))
      .expect(and {
        demotedTo(nodes, 1, 4),
        // R1: the claim stays on the physical holder ...
        match residue.get(1) { | Some(r) => r.source == 1 | None => false },
        nodes.get(1).keys.contains(1),
        // ... and every live-primary-gated remover is now false ...
        not(canFailPromotion(nodes, residue, 1)),
        not(canRetargetSlotResidue(nodes, residue, 1)),
        not(reapableSlots(nodes, migrations, residue, 1).contains(1)),
        // ... leaving R9's arm as the entry's only exit.
        canClearSlotResidue(nodes, slots, migrations, residue, 1),
        inv_residue_has_an_effective_remover,
      })
      .then(clearSlotResidue(1))
      .expect(and {
        residue.get(1) == None,
        // The slot map is what tracks the copy now, which is why the clear strands nothing.
        nodes.get(1).keys.contains(1),
        slots.get(1) == Some(1),
        not(defects.untracked),
        inv_member_keyspace_is_tracked,
        inv_slot_copy_survives_until_owned_and_served,
      })
  }

  // The **in-apply target arm** (V10-C4). An unpromoted entry whose target is demoted is
  // re-homed onto the successor inside the demotion's own apply — there is no
  // level-triggered target arm to catch it afterwards, which is why the entry would
  // otherwise be left with no effective remover. The trace then runs the ordinary
  // attest-reap-clear path to the end on the *new* target, so the arm is shown to be
  // effective and not merely present.
  run demotedTargetResidueRehomedInApplyTest: bool = {
    init
      .then(attachTargetReplica(2, 4))
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(demoteNode(4, 2))
      .expect(and {
        slots.get(1) == Some(2),
        match residue.get(1) {
          | Some(r) => and { r.target == 2, not(r.target_gone), not(r.promoted), r.source == 1 }
          | None => false
        },
        // R8 (issue 41), ruled 2026-08-20: the successor inherits the *slot*, not the
        // *copy*. Node 4 had never attested for slot 1, so it held nothing to hand on;
        // `applyFailoverCommon`'s key union used to give node 2 a copy anyway, and the
        // re-pointed entry was still unpromoted — the new target served before it attested.
        // It now has to run the attestation path below against its new home first.
        not(nodes.get(2).keys.contains(1)),
        not(ctl.prev_keys.get(2).contains(1)),
        inv_no_serve_before_attestation,
        inv_residue_has_an_effective_remover,
        inv_slots_only_assigned_to_primaries,
      })
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      .then(reapSlots(1))
      .then(clearSlotResidue(1))
      .expect(and {
        residue.get(1) == None,
        nodes.get(2).keys.contains(1),
        not(nodes.get(1).keys.contains(1)),
        not(defects.untracked),
        not(defects.overDelete),
        inv_member_keyspace_is_tracked,
      })
  }

  // (c4) ext-15 (V10-M1), the mirror case at the **source**, in the two steps the design
  // separates. The physical restart leaves the replicated record alone — sealed, drained,
  // stamp intact, barrier still armed. What kills it is the **boot report's field write**:
  // the arriving identity differs from the stored one, so applying it takes every
  // migration this node sources and pays the release events they owed. `Complete` is then
  // unreachable for that record because there is no record.
  run sourceBootReportCancelsMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(reportTargetIngest(1))
      .then(sourceRestart(1))
      .expect(and {
        nodes.get(1).run_id == 5,
        not(nodes.get(1).booted),
        // Untouched by the restart itself: still the same sealed, drained record, still
        // behind an armed barrier.
        barrierArmed(1),
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 1, m.fenced, m.phase == Draining,
                             m.drained_pos == Some({ run_id: 1, pos: 0 }),
                             m.source_log.run_id == 1 }
          | None => false
        },
      })
      .then(reportRunIdentity(1, Boot, Primary, 0, false))
      .expect(and {
        // The field write landed: a new identity carrying the new run.
        nodes.get(1).stored_identity == Some({ inc: 1, seq: 2, run_id: 5 }),
        // ...and it took the record with it, releasing the barrier and the hold region.
        migrations.get(1) == None,
        not(barrierArmed(1)),
        held.get(1) == Set(),
        feed_bytes.get(1) == 0,
        // A cancel, not a commit: ownership never moved and no residue was written.
        slots.get(1) == Some(1),
        residue.get(1) == None,
      })
      .then(completeMigration(1, 1).fail())
  }

  // (c4b) The other side of the same ruling: the cancel binds to the **field write**, so a
  // replayed report from earlier in the same boot — carrying the `(incarnation,
  // identity_seq)` pair the cell already holds — is a refused no-op that cancels nothing.
  // The trace then walks straight through the design-accepted window: the source has
  // physically restarted, but its boot report has not applied, so the replicated identity
  // still names the run the record was minted under and `Complete` still admits. Closing
  // that window is deliberately NOT this model's behaviour.
  run replayedReportCancelsNothingTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(reportTargetIngest(1))
      .then(sourceRestart(1))
      .then(reportRunIdentity(1, Boot, Primary, 0, true))
      .expect(and {
        // Refused: the cell is untouched, so no identity was written...
        nodes.get(1).stored_identity == Some({ inc: 1, seq: 1, run_id: 1 }),
        nodes.get(1).identity_seq == 1,
        not(nodes.get(1).booted),
        // ...and nothing was cancelled.
        match migrations.get(1) {
          | Some(m) => and { m.attempt == 1, m.fenced, m.phase == Draining }
          | None => false
        },
        barrierArmed(1),
      })
      .then(completeMigration(1, 1))
      .expect(and { slots.get(1) == Some(4), residue.get(1) != None })
  }

  // (c5) TR-CLUSTER-018/FM-CLUSTER-036: a failover naming the **source** aborts the
  // migration. The successor inherits the slot; it never inherits the migration and never
  // inherits the barrier (FM-CLUSTER-104 stays same-node-only), and the release events the
  // record owed are paid by the prune (FM-CLUSTER-087). Re-migrating is a fresh `Begin`.
  run sourceFailoverPrunesMigrationTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(and { barrierArmed(1), held.get(1) == Set(1) })
      .then(sourceFailover(1, 2))
      .expect(and {
        slots.get(1) == Some(2),
        nodes.get(2).role == Primary,
        nodes.get(2).run_id == 5,
        nodes.get(1).role == Replica,
        nodes.get(1).member,
        // Pruned, with every release event paid.
        migrations.get(1) == None,
        not(barrierArmed(1)),
        held.get(1) == Set(),
        held.get(2) == Set(),
        feed_bytes.get(1) == 0,
        // Nothing the dead primary attested survives as a commit: no residue, and the
        // target's staged copy is left to `discardShadow`.
        residue.get(1) == None,
        shadow.get(1) != None,
        coverage.failoverDuringOpenMigration,
      })
      // The handoff cannot be resumed at the successor — there is no record to prepare,
      // so re-migrating means a fresh `Begin` (once `discardShadow` has taken the
      // orphaned staged copy, exactly as in `gracefulFailoverDrainedNotCompleteTest`).
      .then(prepareHandoff(1).fail())
  }

  // (c6) ext-12 (V7-M6): the orphan re-home arm, disposition one — back to the source.
  // The promotion was attested and the target then departed, so the reaper is deferring
  // forever on `target_gone` and nothing on the ordinary path can resolve the entry. The
  // operator re-assigns the slot to the source that still physically holds the copy; the
  // entry has nothing left to track and goes with the re-home.
  run orphanRehomeToSourceTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      .then(removeNode(4, true, false))
      .expect(and {
        slots.get(1) == None,
        match residue.get(1) { | Some(r) => r.promoted and r.target_gone | None => false },
        nodes.get(1).keys.contains(1),
      })
      .then(rehomeOrphanSlot(1, 1))
      .expect(and {
        slots.get(1) == Some(1),
        residue.get(1) == None,
        nodes.get(1).keys.contains(1),
      })
  }

  // (c7) ext-12 (V7-M6): the orphan re-home arm, disposition two — on to another live
  // primary. The entry survives with `target_gone` cleared and the new target named, which
  // is exactly what puts it back on the ordinary reap-then-clear path.
  run orphanRehomeToAnotherPrimaryTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .then(relabelShadow(1))
      .then(reportPromoted(1))
      .then(removeNode(4, true, false))
      // A second live primary to re-home onto (2 and 3 start as replicas of 1).
      .then(setRole(2, Primary, None))
      .then(rehomeOrphanSlot(1, 2))
      .expect(and {
        slots.get(1) == Some(2),
        nodes.get(2).keys.contains(1),
        match residue.get(1) {
          | Some(r) => and { r.source == 1, r.target == 2, r.promoted, not(r.target_gone) }
          | None => false
        },
        // The source still holds its copy — the re-home is not itself a reap.
        nodes.get(1).keys.contains(1),
      })
      // Back on the ordinary path: reap the source's copy, then clear the entry.
      .then(reapSlots(1))
      .expect(not(nodes.get(1).keys.contains(1)))
      .then(clearSlotResidue(1))
      .expect(residue.get(1) == None)
  }

  // (d) Forced failover mid-migration -> demote path, not removal.
  run forcedFailoverDemotesTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
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

  // (d1) What `force` actually drops is ext-16's *sync* gate on the candidate, not the
  // fence and not the belt: an unsynced in-shard replica is promotable by the forced path
  // and refused by the graceful one.
  run forcedFailoverPromotesUnsyncedTest: bool = {
    init
      .then(setRole(3, Replica, Some(1)))
      .expect(not(nodes.get(3).synced))
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary).fail())
  }

  run forcedFailoverAcceptsUnsyncedTest: bool = {
    init
      .then(setRole(3, Replica, Some(1)))
      .then(failoverForced(1, 3, 0, Primary))
      .expect(and { nodes.get(3).role == Primary, coverage.forcedDemotedReplica })
  }

  // The per-object fence (issue 19, amended) refuses a stale observation: node 3's epoch
  // has moved on (it was just promoted) since a proposal that still thinks it is at
  // epoch 0 observed it. After the first swap, node 1 is node 3's replica, so the reverse
  // proposal passes the belt but must fail the fence.
  run failoverRefusesStaleFenceTest: bool = {
    init
      .then(failoverForced(1, 3, 0, Primary))
      .expect(and {
        nodes.get(3).epoch_own == 1,
        nodes.get(1).role == Replica,
        nodes.get(1).parent == Some(3),
      })
      .then(failoverForced(3, 1, 0, Primary).fail())
  }

  // TR-CLUSTER-016/034 amendment + ext-2/ext-11: a hold that breaches the byte cap is
  // released *locally* — the source drops its own seal and the whole hold region empties,
  // while the migration record and its phase survive untouched.
  run feedHoldCapBreachReleasesTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .expect(and { feed_bytes.get(1) == 6, held.get(1) == Set(1) })
      // At the cap, nothing has breached yet.
      .then(localReleaseOnCapBreach(1).fail())
  }

  run feedHoldCapBreachReleasesFenceTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .then(feedBuffer(1))
      .expect(feed_bytes.get(1) == 7)
      .then(localReleaseOnCapBreach(1))
      .expect(and {
        feed_bytes.get(1) == 0,
        not(barrierArmed(1)),
        held.get(1) == Set(),
        // The seal drops; the phase does not.
        match migrations.get(1) { | Some(m) => not(m.fenced) and m.phase == Streaming | None => false },
      })
  }

  // ext-11: the alive-but-unable-to-apply latch releases the hold region too — the
  // held set must not stay latched behind a source that cannot drain it.
  run selfFenceReleaseEmptiesHoldTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(setSourceCannotApply(1, true))
      .then(selfFenceRelease(1))
      .expect(and { held.get(1) == Set(), not(barrierArmed(1)) })
  }

  // TR-CLUSTER-003, ruled + amended (issue 20): a member still owning a slot cannot be
  // removed without `force`.
  run removeNodeRefusesLiveOwnerTest: bool = {
    init
      .then(removeNode(1, false, false).fail())
  }

  // Once demoted off its slots by a graceful failover, the bare (non-forced) removal
  // succeeds. Note what it does NOT do: the departing node's keyspace is untouched —
  // `reapSlots` is the model's only keyspace deleter (V8-C1).
  run removeNodeAfterDemotionSucceedsTest: bool = {
    init
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(and { nodes.get(1).role == Replica, not(ownsAnySlot(slots, 1)) })
      .then(removeNode(1, false, false))
      .expect(and {
        not(nodes.get(1).member),
        nodes.get(1).registration_seq == None,
        nodes.get(1).keys == SLOTS,
        // Issue 33: the tombstone carries the "nothing here" values a deleted
        // `ClusterState` node record projects to — not the node's last live ones — on
        // both planes. Only the keyspace survives (ext-12's mark-don't-remove).
        nodes.get(1).role == Replica,
        nodes.get(1).parent == None,
        nodes.get(1).epoch_own == 0,
        nodes.get(1).local_role == Replica,
        nodes.get(1).local_parent == None,
      })
  }

  // TR-CLUSTER-003's `force` escape: a live owning Primary can be forcibly removed; the
  // postcondition still prunes its slot ownership (INV-REF-1 survives).
  run removeNodeForceEvictsLiveOwnerTest: bool = {
    init
      .then(removeNode(1, true, false))
      .expect(and {
        not(nodes.get(1).member),
        slots.get(1) == None,
        slots.get(2) == None,
        // Issue 33: the `force` path tombstones identically — a live owning Primary
        // leaves behind the same absent-record projection, role included.
        nodes.get(1).role == Replica,
        nodes.get(1).parent == None,
        nodes.get(1).epoch_own == 0,
      })
  }

  // ext-10: `ResetCluster` is refused while the node's keyspace is non-empty — the same
  // gate as `meetNode`, and the reason a rejoining node can never smuggle stale copies
  // back into the cluster.
  run resetRefusesNonEmptyKeyspaceTest: bool = {
    init
      .then(resetCluster(1).fail())
  }

  // ext-10, the point of re-keying `spent` by reset epoch: after a reset rewinds the
  // handoff counter, minting seq 1 again is a lawful *re-mint*, not a reuse — and the
  // model can tell the two apart.
  //
  // The trace also pins TR-CLUSTER-035's *direction*: node 4 resets, and it is every
  // **other** member that is ejected (node 1 among them) while node 4 keeps its own cell.
  // Every slot is unassigned and every migration taken, not only those naming node 4 —
  // which is why the cluster has to be rebuilt (meet, boot, assign) before a second
  // handoff can be minted at all.
  run resetMakesReMintDistinguishableTest: bool = {
    init
      .then(beginMigration(1, 1, 2, false))
      .then(prepareHandoff(1))
      .expect(spent == Set({ epoch: 0, seq: 1 }))
      .then(cancelMigration(1, 1))
      .then(resetCluster(4))
      .expect(and {
        ctl.reset_epoch == 1, ctl.handoff_seq == 0,
        nodes.get(4).member, nodes.get(4).registration_seq == Some(4),
        not(nodes.get(1).member), not(nodes.get(2).member), not(nodes.get(3).member),
        SLOTS.forall(s => slots.get(s) == None),
      })
      .then(meetNode(2))
      .then(reportRunIdentity(2, Boot, Primary, 0, false))
      .then(assignSlot(2, 4))
      .then(beginMigration(2, 4, 2, false))
      .then(prepareHandoff(2))
      .expect(and {
        match migrations.get(2) { | Some(m) => m.attempt == 1 | None => false },
        spent == Set({ epoch: 0, seq: 1 }, { epoch: 1, seq: 1 }),
        not(defects.seqReuse),
      })
  }

  // ---------------------------------------------------------------------------
  // Witness reachability.
  //
  // Every witness above except the two **declared-unreachable** ones is pinned here by a
  // deterministic trace that ends in a state satisfying it. A witness with no such trace
  // is a modelling gap, not a passing spec, so the mapping is one-to-one and exhaustive:
  //
  //   witnessMigrationInFlight                  -> migrationInFlightReachableTest
  //   witnessEmptyCoverageBatchIsANoOp          -> quietSlotWitnessesReachableTest
  //   witnessHandoffDrainedNotComplete          -> quietSlotWitnessesReachableTest
  //   witnessCompleteEnabledOnQuietSlot         -> quietSlotWitnessesReachableTest
  //   witnessResiduePending                     -> residueWitnessesReachableTest
  //   witnessBeginRefusedOverResidue            -> residueWitnessesReachableTest
  //   witnessGracefulFailoverPrunedMigration    -> gracefulPruneWitnessReachableTest
  //   witnessForcedFailoverDemoted              -> forcedDemotionWitnessReachableTest
  //   witnessFailoverDuringOpenMigration        -> failoverDuringOpenMigrationReachableTest
  //   witnessFeedDisconnected                   -> feedDisconnectedWitnessReachableTest
  //   witnessNodeRemoved                        -> departedHolderWitnessesReachableTest
  //   witnessNonEmptyJoinRefused                -> departedHolderWitnessesReachableTest
  //   witnessNonEmptyResetRefused               -> demotedHolderResetRefusedReachableTest
  //   witnessDrainingHeldForKTicks              -> drainingHeldForKTicksReachableTest
  //   witnessHealthyDrainAbortedByBound         -> healthyDrainAbortedByBoundReachableTest
  //   witnessKnobOnMigrationCompletes           -> replicaAckKnobCompletesReachableTest
  //   witnessResyncedReplicaBecomesCandidate    -> resyncedReplicaIsCandidateReachableTest
  //   witnessNodeCanAlwaysReportIdentity        -> lostCounterStillReportsReachableTest
  //   witnessStagedFlipCompletesAcrossCrash     -> stagedFlipSurvivesCrashReachableTest
  //   witnessFenceClears                        -> fenceClearsWithinThreeStepsTest
  //
  // The trailing three tests pin the states the "Stated in the doc, NOT stated here" block
  // cites, plus the antecedent of `inv_no_live_record_disposed_by_a_foreign_refusal` —
  // an invariant over `last_disposition` is vacuous until some step writes one.
  // ---------------------------------------------------------------------------

  run migrationInFlightReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .expect(witnessMigrationInFlight)
  }

  // ext-9 (N-C5/V4-M1): the quiet slot. No write is ever proposed, so the coverage batch
  // is the only thing that could advance the target — and it advances it by nothing. The
  // batch is admitted as a no-op (it does not discard the shadow), the drain still seals,
  // and `Complete` is enabled over an empty stream history.
  run quietSlotWitnessesReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(coverageBatch(1))
      .expect(and {
        witnessEmptyCoverageBatchIsANoOp,
        // The batch is a no-op *and* leaves the staged copy alone.
        shadow.get(1) != None,
        stream_history.get(1) == Set(),
      })
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(and {
        witnessHandoffDrainedNotComplete,
        witnessCompleteEnabledOnQuietSlot,
      })
  }

  // ext-12: the post-commit window. The entry is outstanding and unpromoted, and `Begin`
  // over the same slot is refused with every other conjunct satisfiable (node 4 owns the
  // slot now, and 1/2/3 are all members that could be a target).
  run residueWitnessesReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(completeMigration(1, 1))
      .expect(and { witnessResiduePending, witnessBeginRefusedOverResidue })
  }

  run gracefulPruneWitnessReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(armFailoverFence(1))
      .then(failoverGraceful(1, 3, 0, Primary))
      .expect(witnessGracefulFailoverPrunedMigration)
  }

  // ext-16: the candidate is *unsynced* (the re-parent in step one dropped the flag), so
  // this is the gate `force` drops rather than an incidental demotion.
  run forcedDemotionWitnessReachableTest: bool = {
    init
      .then(setRole(3, Replica, Some(1)))
      .expect(not(nodes.get(3).synced))
      .then(failoverForced(1, 3, 0, Primary))
      .expect(witnessForcedFailoverDemoted)
  }

  run failoverDuringOpenMigrationReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .then(sourceFailover(1, 2))
      .expect(witnessFailoverDuringOpenMigration)
  }

  run feedDisconnectedWitnessReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(disconnectFeed(1))
      .expect(witnessFeedDisconnected)
  }

  // ext-10 (V8-C1): `removeNode` leaves the departing node's keyspace intact — the
  // self-cleanse is gone — so the join gate is what keeps those copies out on the way back
  // in. Both witnesses name that one state.
  run departedHolderWitnessesReachableTest: bool = {
    init
      .then(removeNode(1, true, false))
      .expect(and {
        witnessNodeRemoved,
        witnessNonEmptyJoinRefused,
        not(canMeetNode(nodes, 1)),
      })
      .then(meetNode(1).fail())
  }

  // ext-10 (V8-C1), the non-trivial half: a **member** holding copies of slots it no
  // longer owns. An administrative demotion produces exactly that (the copies do not move
  // with the ownership), and `ResetCluster` is refused there.
  run demotedHolderResetRefusedReachableTest: bool = {
    init
      .then(demoteNode(1, 2))
      .expect(and {
        witnessNonEmptyResetRefused,
        nodes.get(1).member,
        nodes.get(1).keys == SLOTS,
        not(ownsAnySlot(slots, 1)),
      })
      .then(resetCluster(1).fail())
  }

  // M32 (V9-C1), ruled 2026-08-19: the demotion successor must be a replica of the node
  // being demoted, and the ghost is what makes that checkable. Node 4 is a live primary
  // of its own — a cross-shard successor — so the demotion is refused; node 2 is in
  // node 1's shard, so it is admitted with the ghost clear. Dropping
  // `isInShardReplicaOf` from `canDemoteNode` makes the refused step fire and trips
  // `inv_no_cross_shard_successor`, where before the change nothing observed it.
  run demotionSuccessorMustBeInShardTest: bool = {
    init
      .expect(isInShardReplicaOf(nodes, 2, 1))
      .then(demoteNode(1, 2))
      .expect(and {
        nodes.get(2).role == Primary,
        nodes.get(1).parent == Some(2),
        inv_no_cross_shard_successor,
      })
  }

  // The refusal half of the same rule, in its own run because a `.fail()` step produces no
  // frame for a later step to read. Node 4 is a live primary of its own shard, so it is
  // never a lawful successor for node 1.
  run demotionRefusesCrossShardSuccessorTest: bool = {
    init
      .expect(not(isInShardReplicaOf(nodes, 4, 1)))
      .then(demoteNode(1, 4).fail())
  }

  // TR-CLUSTER-025 / FM-CLUSTER-014, ruled 2026-08-19: the FAIL flag is health news, so
  // neither transition moves the config epoch. Both directions are exercised because both
  // write the ghost; bumping the epoch in either trips `inv_no_churn_epoch_bump`.
  run churnDoesNotBumpEpochTest: bool = {
    init
      .expect(ctl.epoch == 0)
      .then(markNodeFailed(2))
      .expect(and { nodes.get(2).fail, ctl.epoch == 0, inv_no_churn_epoch_bump })
      .then(markNodeRecovered(2))
      .expect(and { not(nodes.get(2).fail), ctl.epoch == 0, inv_no_churn_epoch_bump })
      .then(markNodeFailed(2))
      .expect(and { ctl.epoch == 0, inv_no_churn_epoch_bump })
  }

  // ext-4: the seal resets the counter (V7-C3), so the three ticks below are all
  // post-seal. The first sees `last_observation` unset and records the position; the next
  // two see no advance and accrue. At `draining_observations = 3` that leaves the drain
  // held one tick short of the bound — still holding, not yet abandoned.
  run drainingHeldForKTicksReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(match migrations.get(1) { | Some(m) => m.observations == 0 | None => false })
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .expect(and {
        witnessDrainingHeldForKTicks,
        match migrations.get(1) { | Some(m) => m.observations == DRAINING_OBSERVATIONS - 1 | None => false },
        not(defects.boundAbort),
      })
  }

  // ext-4's calibration obligation: the migration below is healthy — the source wrote, and
  // the target still has that record to ingest — and the pre-`Confirm` bound takes it
  // anyway because the target never applied. `preconfirm_observations` is the only thing
  // sizing how much of that is tolerated, so the state has to be reachable to be
  // calibrated against.
  run healthyDrainAbortedByBoundReachableTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(sourceWrite(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .expect(and {
        match migrations.get(1) {
          | Some(m) => and { m.phase == Streaming, isAbortableByBound(m), m.target_applied < m.source_log.pos }
          | None => false
        },
        not(defects.boundAbort),
      })
      .then(boundAbort(1))
      .expect(and { witnessHealthyDrainAbortedByBound, migrations.get(1) == None })
  }

  // ext-14 (V8-m4): the replica-ack knob is satisfiable at the stock sizings. Node 2 is
  // attached to the target and counted; it lags the target's applied point for a while
  // (the healthy, progressing case) and catches up before the commit, which is what the
  // `Complete` conjunct reads. Stamped `true` at `Begin` and never re-read from config.
  run replicaAckKnobCompletesReachableTest: bool = {
    init
      .then(attachTargetReplica(2, 4))
      .then(beginMigration(1, 1, 4, true))
      .expect(match migrations.get(1) { | Some(m) => m.require_replica_ack | None => false })
      .then(sourceWrite(1))
      .then(applyAtTarget(1))
      .then(makeTargetDurable(1))
      .then(reportTargetIngest(1))
      .expect(match migrations.get(1) {
        // Lagging but progressing: the counted replica is behind the target's own copy.
        | Some(m) => nodes.get(2).replica_copy < m.target_applied
        | None => false
      })
      // ext-4 (V7-C3) / ext-14 (V8-M2): the pre-seal wait is *spent*. Four ticks — the
      // first sees the ingest progress and resets, the next three accrue — leave the
      // counter at the `draining_observations` bound while the migration is still healthy
      // (the `preconfirm_observations` bound is larger, so nothing is abortable yet).
      // Without this the seal-time reset is invisible: the counter it zeroes is already
      // zero.
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .then(reconcileTick(1))
      .expect(match migrations.get(1) {
        | Some(m) => and { m.observations == DRAINING_OBSERVATIONS, not(isAbortableByBound(m)) }
        | None => false
      })
      .then(feedTargetReplica(2))
      .then(reportReplicaAck(1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      // The seal starts the post-`Confirm` bound from zero. Reverting the reset (or the
      // replica-ack counter reset it stands for) leaves the accrued pre-seal count in
      // place, which is already at the bound — the abort-every-lawful-drain defect, and
      // the reason the witness below is unreachable without it.
      .expect(match migrations.get(1) {
        | Some(m) => and { m.observations == 0, not(isAbortableByBound(m)), not(defects.sealCounter) }
        | None => false
      })
      // The drain waits one more observation for the ack it already has; with the counter
      // reset this is free, without it the record is abortable here.
      .then(reconcileTick(1))
      .expect(match migrations.get(1) { | Some(m) => not(isAbortableByBound(m)) | None => false })
      .then(completeMigration(1, 1))
      .expect(and {
        witnessKnobOnMigrationCompletes,
        slots.get(1) == Some(4),
        not(defects.replicaAck),
        // ext-14 (V5-M3): the counted set that carried the commit was *non-empty* — the
        // conjunct the no-replica variant below shows is not vacuous.
        countedReplicasOf(nodes, 4) != Set(),
      })
  }

  // ext-14 (V5-M3), the empty-counted-set half of the same rule: knob on, nothing counted.
  // `Complete` must be refused however drained the record is — an empty counted set is
  // false, not vacuously true.
  //
  // The ack is **earned first** and the counted set emptied afterwards, deliberately: with
  // no ack ever recorded the position conjunct (`target_replica_ack >= d.pos`) refuses on
  // its own and would mask the rule under test. Here the recorded ack covers the drained
  // position, so the emptiness of the counted set is the only thing left refusing — which
  // is what makes reverting the rule to "vacuously true" observable at this step.
  run replicaAckKnobRefusesEmptyCountedSetTest: bool = {
    init
      .then(beginMigration(1, 1, 4, true))
      .then(sourceWrite(1))
      .then(applyAtTarget(1))
      .then(makeTargetDurable(1))
      .then(reportTargetIngest(1))
      .then(attachTargetReplica(2, 4))
      .then(feedTargetReplica(2))
      .then(reportReplicaAck(1, 2))
      .then(prepareHandoff(1))
      .then(confirmDrained(1, 1))
      .expect(and {
        // Every `Complete` conjunct is satisfied here, the replica pair included.
        countedReplicasOf(nodes, 4) == Set(2),
        canCompleteMigration(nodes, migrations, 1, 1),
        match migrations.get(1) {
          | Some(m) => and { m.phase == Draining, m.require_replica_ack,
                             m.drained_pos == Some({ run_id: 1, pos: 1 }),
                             m.target_copy.run_id == nodes.get(4).run_id,
                             m.target_copy.pos >= 1,
                             m.target_replica_ack >= 1 }
          | None => false
        },
      })
      .then(detachTargetReplica(2))
      .expect(and {
        // Only the counted set changed — the recorded ack still covers the drained point.
        countedReplicasOf(nodes, 4) == Set(),
        match migrations.get(1) {
          | Some(m) => m.target_replica_ack >= 1
          | None => false
        },
        not(canCompleteMigration(nodes, migrations, 1, 1)),
      })
      .then(completeMigration(1, 1).fail())
  }

  // ext-17 (V15-M4): the re-parent drops `synced` and moves the parenting token; the
  // attestation earns it back. `parent_seq > 0` is what tells this apart from the free
  // `synced` every node starts with.
  run resyncedReplicaIsCandidateReachableTest: bool = {
    init
      .then(attachTargetReplica(2, 4))
      .expect(and {
        not(nodes.get(2).synced),
        nodes.get(2).parent_seq == 1,
        nodes.get(2).parent == Some(4),
      })
      .then(attestReplicaSynced(2, 4, 1, 2))
      .expect(witnessResyncedReplicaBecomesCandidate)
  }

  // ext-15: the node whose incarnation counter was lost is **not** mute. `bootMintOf`
  // proposes above the surviving replicated cell, the report is admitted `Whole`, and the
  // node's own counter takes the value it just published. Reverting the re-mint rule to a
  // literal `(0, 0)` makes the first expectation false — the mutation the doc names.
  run lostCounterStillReportsReachableTest: bool = {
    init
      .then(loseIncarnationCounter(4))
      .expect(and {
        witnessNodeCanAlwaysReportIdentity,
        nodes.get(4).incarnation == 0,
        nodes.get(4).stored_identity == Some({ inc: 1, seq: 1, run_id: 4 }),
      })
      .then(reportRunIdentity(4, Boot, Primary, 0, false))
      .expect(and {
        nodes.get(4).stored_identity == Some({ inc: 2, seq: 1, run_id: 4 }),
        nodes.get(4).incarnation == 2,
        nodes.get(4).booted,
        refusals == Set(),
        not(defects.identityRegress),
      })
  }

  // ext-16/ext-18 (V14-M1): the record survives the crash **field-for-field**, `adopted`
  // included, so the flip the replicated admission already stamped still lands after the
  // restart. A restart that dropped the flag would leave the node fenced with nothing that
  // can clear it.
  run stagedFlipSurvivesCrashReachableTest: bool = {
    init
      .then(stageFlip(3, 4))
      .expect(and { nodes.get(3).node_fenced, not(nodes.get(3).reply_emitted) })
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .expect(match nodes.get(3).record { | Some(rec) => rec.adopted | None => false })
      .then(sourceRestart(3))
      .expect(and { witnessStagedFlipCompletesAcrossCrash, not(nodes.get(3).booted) })
      .then(completeAdoption(3))
      .expect(and {
        nodes.get(3).record == None,
        not(nodes.get(3).node_fenced),
        nodes.get(3).reply_emitted,
      })
  }

  // ext-18 (V28-m3): the fence the staged flip raises is cleared inside a three-step
  // window — stage, admit, adopt — with the client paid in the same step that drops it.
  // The design's bound is a property of the run, which is why it is a test and not a
  // state invariant.
  run fenceClearsWithinThreeStepsTest: bool = {
    init
      .then(stageFlip(3, 4))
      .expect(and {
        nodes.get(3).node_fenced,
        nodes.get(3).record != None,
        not(nodes.get(3).reply_emitted),
      })
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .then(completeAdoption(3))
      .expect(and {
        witnessFenceClears,
        not(nodes.get(3).node_fenced),
        nodes.get(3).reply_emitted,
        nodes.get(3).record == None,
      })
  }

  // V12-M1, ruled 2026-08-19 — the *effect* half of `inv_no_hold_during_staged_flip`, and
  // the state that made the invariant false before the ruling. The prepare latches the
  // hold region on node 1; staging a flip at that same node **answers** the held writes
  // and empties the region in the same step, rather than refusing to stage until a drain
  // that may never finish releases them. Deleting the `held' = held.set(n, Set())` write
  // from `stageFlip` restores the old counter-state and trips the invariant here.
  run stagedFlipAnswersHeldWritesTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(prepareHandoff(1))
      .expect(and {
        held.get(1) == Set(1),
        not(witnessStagedFlipAnsweredHolds),
      })
      .then(stageFlip(1, 4))
      .expect(and {
        // The region is answered and empty, while the flip is pending and the node fenced.
        held.get(1) == Set(),
        nodes.get(1).record != None,
        nodes.get(1).node_fenced,
        witnessStagedFlipAnsweredHolds,
        inv_no_hold_during_staged_flip,
      })
  }

  // V12-M1, ruled 2026-08-19 — the *guard* half of the same closure. `prepareHandoff` is
  // the model's only action that adds to `held`, so once a record is pending at the source
  // it must not latch a new hold region underneath it: otherwise the invariant would be
  // restorable one step after `stageFlip` answered the region. The prepare is admissible
  // on this very migration before the flip is staged (`happyMigrationTest` runs it), so
  // what this pins is the record, not some other unmet precondition.
  run noHoldLatchesUnderPendingRecordTest: bool = {
    init
      .then(beginMigration(1, 1, 4, false))
      .then(stageFlip(1, 4))
      .expect(and { nodes.get(1).record != None, held.get(1) == Set() })
      .then(prepareHandoff(1).fail())
  }

  // M37, ruled 2026-08-19 — the dataset-discard leg fires. Node 3 is handed slot 1 and
  // its copy, the slot is then unassigned, and node 3 is left holding a copy the
  // replicated map gives it no claim to. Adopting a foreign upstream discards the node's
  // dataset (FM-REPLICATION-022), so the copy goes at `completeAdoption` — deleting the
  // discard leaves node 3 holding it and `witnessAdoptionDiscardsUnclaimedCopy`
  // unreachable.
  run adoptionDiscardsUnclaimedCopyTest: bool = {
    init
      .then(assignSlot(1, 3))
      .then(removeSlot(1))
      .expect(and {
        nodes.get(3).keys == Set(1),
        slots.get(1) == None,
        not(witnessAdoptionDiscardsUnclaimedCopy),
      })
      .then(stageFlip(3, 4))
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .then(completeAdoption(3))
      .expect(and {
        nodes.get(3).keys == Set(),
        witnessAdoptionDiscardsUnclaimedCopy,
        nodes.get(3).record == None,
      })
  }

  // M37's **`assignSlots`-interleaved schedule**, which is the reason the flip is staged
  // at all: the gate check (`stageFlip`) passes on a node bound to no slot, and an
  // `AssignSlots` then lands in the window before the report applies — "the race
  // interleaving staging exists for". Because the discard is at *adoption* and exempts
  // what the replicated map claims, the freshly assigned copy survives. Collapsing stage
  // and discard — discarding at stage time — loses it, which is the wiped-source leg of
  // `inv_no_acked_write_lost` and V11-C2's `inv_member_keyspace_is_tracked`.
  run assignSlotDuringStagedFlipKeepsCopyTest: bool = {
    init
      .then(assignSlot(1, 3))
      .then(removeSlot(1))
      .then(stageFlip(3, 4))
      // The interleaved apply: node 3 is bound to slot 1 *after* the gate check passed.
      .then(assignSlot(1, 3))
      .expect(and { slots.get(1) == Some(3), nodes.get(3).keys == Set(1) })
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .then(completeAdoption(3))
      .expect(and {
        // The copy the interleaved assign bound is still there — the discard exempted it.
        nodes.get(3).keys == Set(1),
        inv_slot_copy_survives_until_owned_and_served,
        inv_member_keyspace_is_tracked,
        inv_no_acked_write_lost,
      })
  }

  // The counter-state for `inv_no_record_outlives_its_registration`: the record is
  // node-local and crash-durable, so it deliberately outlives the registration cell it was
  // staged under. `clearStaleRecord` is the level-triggered clause that closes the gap,
  // and it is separately schedulable — which is exactly why the gap state is reachable.
  run recordOutlivesRegistrationTest: bool = {
    init
      .then(stageFlip(3, 4))
      .then(removeNode(3, false, false))
      .expect(and {
        nodes.get(3).record != None,
        nodes.get(3).registration_seq == None,
        isRecordStale(nodes.get(3)),
      })
      .then(clearStaleRecord(3))
      .expect(and {
        nodes.get(3).record == None,
        not(nodes.get(3).node_fenced),
        nodes.get(3).reply_emitted,
      })
  }

  // ext-18 (V18-M1/V19-M1), **arm 4b** of `isRefusalTerminal`: a `Demotion` report against
  // an **absent** stored identity is refused `Ordering`, and that refusal is *terminal*.
  // R2 (ruled 2026-08-20, `.scratch/formal-spec/t1-blocked.md`) is what makes the first half
  // true: `identityOrderOk`'s absent-operand arm is kind-scoped, so a Demotion — which has
  // no boot behind it and nothing to order itself against — fails the ordering conjunct
  // where a `Boot` report passes it vacuously. Both halves are asserted here, so reverting
  // the arm to `| None => true` flips the Ordering assertion and reverting the kind scope
  // (`| None => kind != Boot`, say) flips the Boot control.
  //
  // Stated here from a **constructed view**, which keeps the rule checkable in one place
  // and in both directions (Demotion vs Boot, absent vs present cell) without a five-action
  // prefix. The absent cell is *also* a reachable state since R10 gave the record-deleting
  // actions their identity-clearing arm — `demotionAgainstAbsentCellAfterForgetTest` below
  // walks to it, so this view is a statement of the rule, not a substitute for reachability.
  run demotionAgainstAbsentCellIsTerminalTest: bool = {
    init
      .expect({
        val ns = nodes.get(3)
        val absent = nodes.set(3, { ...ns, stored_identity: None })
        and {
          // The cell really is absent in the view, and present in the reachable state —
          // the difference the arm is about.
          absent.get(3).stored_identity == None,
          ns.stored_identity != None,
          // Arm 4b, both halves.
          refusalClassOf(absent, 3, Demotion, ns.incarnation, ns.identity_seq + 1,
                         ns.role, ns.epoch_own) == Ordering,
          isRefusalTerminal(absent.get(3).stored_identity, Ordering),
          // Kind scope: the same absent cell admits a `Boot` report — the ordinary first
          // report of a node whose cell was never written.
          refusalClassOf(absent, 3, Boot, ns.incarnation, ns.identity_seq + 1,
                         ns.role, ns.epoch_own) == Whole,
          // And against a *present* cell an `Ordering` refusal stays non-terminal, so 4b
          // is the absent-cell case alone rather than "every ordering refusal".
          not(isRefusalTerminal(ns.stored_identity, Ordering)),
        }
      })
  }

  // **R10** (ruled 2026-08-20), the reachability half of the arm above: the absent cell is
  // a state the walk can stand in, not only a view the test constructs. The replicated run
  // identity is a field of the node record, so deleting the record deletes it — `removeNode`
  // is CLUSTER FORGET's `inner.nodes.remove(&node_id)` — and a `meetNode` rejoin
  // re-registers against an empty cell. The node's staged record is node-local and
  // crash-durable, so it survives the round trip (ext-18) and its next `Demotion`
  // re-proposal has both a record to bind to and nothing to order itself against: refused
  // `Ordering`, terminally.
  //
  // The two neighbouring controls are the point of the sequence. Before the removal the
  // same report would be refused `Fenced`/admitted against a present cell; straight after
  // the removal it is refused `Membership` (the arm `terminalRefusalDisposesRecordTest`
  // walks), which is terminal for a different reason. Only the rejoin isolates arm 4b.
  // Reverting R10 (dropping `stored_identity: None` from `removeNode`) turns this refusal
  // back into `Fenced` and fails the test.
  run demotionAgainstAbsentCellAfterForgetTest: bool = {
    init
      .then(stageFlip(3, 4))
      .expect(nodes.get(3).stored_identity != None)
      .then(removeNode(3, false, false))
      .expect(and {
        // The forget cleared the cell along with the rest of the record.
        nodes.get(3).stored_identity == None,
        not(nodes.get(3).member),
        // The record is node-local and outlives the record deletion.
        nodes.get(3).record != None,
      })
      .then(meetNode(3))
      .expect(and {
        // A member again, still with no identity cell: the absent-operand state.
        nodes.get(3).member,
        nodes.get(3).stored_identity == None,
        // Membership is satisfied, so the classifier reaches the ordering conjunct.
        refusalClassOf(nodes, 3, Demotion, nodes.get(3).incarnation,
                       nodes.get(3).identity_seq + 1, nodes.get(3).role,
                       nodes.get(3).epoch_own) == Ordering,
      })
      .then(reportRunIdentity(3, Demotion, Primary, 0, false))
      .expect(and {
        // Arm 4b, walked to: refused `Ordering` and classified terminal.
        refusals.exists(r => r.node == 3 and r.class == Ordering and r.terminal),
        // Refused, so nothing was written and nothing was cancelled.
        nodes.get(3).stored_identity == None,
        nodes.get(3).role == Primary,
      })
  }

  // ext-18 (V21/V28-M6): the antecedent of `inv_no_live_record_disposed_by_a_foreign_
  // refusal`. `last_disposition` is written on a disposing step and on no other, so the
  // invariant is vacuous until some trace reaches a terminal refusal bound to its own
  // record. This is that trace: the node leaves the membership with a record still staged,
  // its next `Demotion` report is refused `Membership` (a declared terminal class), and
  // observing that refusal disposes of all three obligations at once.
  run terminalRefusalDisposesRecordTest: bool = {
    init
      .then(stageFlip(3, 4))
      .then(removeNode(3, false, false))
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .expect(refusals == Set({ node: 3, stage_id: 1, reg_seq: 3,
                                class: Membership, terminal: true }))
      .then(observeRefusal(3, { node: 3, stage_id: 1, reg_seq: 3,
                                class: Membership, terminal: true }))
      .expect(and {
        nodes.get(3).last_disposition != None,
        nodes.get(3).record == None,
        not(nodes.get(3).node_fenced),
        nodes.get(3).reply_emitted,
        not(defects.disposition),
        inv_no_live_record_disposed_by_a_foreign_refusal,
      })
  }

  // ext-18 (V20-C1/V28-M6), the refusing half of the same pair — the trace the invariant
  // above is *stated against*. The stage counter is a node-local store (`wipe`), so a
  // forget-and-rejoin can mint a **second stage carrying the same id** under a new
  // registration. The refusal still in flight names the dead stage's registration, so the
  // pair refuses and the live record survives; weakening the disposition guard to the
  // stage id alone disposes of a record the refusal was never minted for, and
  // `last_disposition` records the mismatched pair that says so.
  run foreignRefusalDoesNotDisposeTest: bool = {
    init
      .then(stageFlip(3, 4))
      .expect(match nodes.get(3).record {
        | Some(rec) => rec.stage_id == 1 and rec.staged_reg_seq == 3
        | None => false
      })
      // The store loss goes with the departure: the counter rewinds, the record does not
      // (ext-18 — that survival is what makes the collision expressible).
      .then(removeNode(3, false, true))
      .then(reportRunIdentity(3, Demotion, Replica, 0, false))
      .expect(refusals == Set({ node: 3, stage_id: 1, reg_seq: 3,
                                class: Membership, terminal: true }))
      .then(clearStaleRecord(3))
      .then(meetNode(3))
      .expect(nodes.get(3).registration_seq == Some(5))
      // Same stage id (the counter was wiped), different registration.
      .then(stageFlip(3, 4))
      .expect(match nodes.get(3).record {
        | Some(rec) => rec.stage_id == 1 and rec.staged_reg_seq == 5
        | None => false
      })
      .then(observeRefusal(3, { node: 3, stage_id: 1, reg_seq: 3,
                                class: Membership, terminal: true }))
      .expect(and {
        // Not disposed: the record, the fence and the unpaid client all survive, and no
        // disposition was recorded at all.
        nodes.get(3).record != None,
        nodes.get(3).node_fenced,
        not(nodes.get(3).reply_emitted),
        nodes.get(3).last_disposition == None,
        not(defects.disposition),
        inv_no_live_record_disposed_by_a_foreign_refusal,
      })
  }

  // ext-15 (V13-C1): a record is a *single* pending obligation. A second stage toward
  // another upstream while one is pending would let the adoption land toward B on an
  // admission earned for A.
  run competingStageRefusedTest: bool = {
    init
      .then(stageFlip(3, 4))
      .expect(match nodes.get(3).record {
        | Some(rec) => and { rec.upstream == 4, rec.stage_id == 1, not(rec.adopted) }
        | None => false
      })
      .then(stageFlip(3, 2).fail())
  }

  // ext-16 (1) / ext-15 (V13-C1): the adoption reads the admission **stamped for its own
  // stage**, not the bare pair (record ∧ role `Replica`). Here an unrelated replicated
  // write puts the node in exactly that bare state — a `Replica` carrying a pending
  // record — with no admission of its own: the flip must not land, or the keyspace would
  // move toward the record's upstream on someone else's admission.
  run adoptionRefusedWithoutItsOwnAdmissionTest: bool = {
    init
      .then(stageFlip(3, 4))
      .then(setRole(3, Replica, Some(1)))
      .expect(and {
        nodes.get(3).role == Replica,
        nodes.get(3).admitted_stage == None,
        match nodes.get(3).record {
          | Some(rec) => and { rec.upstream == 4, not(rec.adopted) }
          | None => false
        },
      })
      .then(completeAdoption(3).fail())
  }

  // ---------------------------------------------------------------------------
  // ext-17/V18-C1: the stale-attestation fence, one operand per trace.
  //
  // `attestReplicaSynced` is the only writer of the candidacy bit `beltOk` reads, so an
  // attestation minted before a topology change and applied after it is the model's
  // stale-evidence hazard: it re-arms candidacy over a dataset the node synced from
  // somewhere else. The fence is three operands wide (observed primary, parenting token,
  // registration), and each of the three traces below is built so that **exactly one** of
  // them refuses — the other two match at apply time. Dropping any one operand from the
  // guard therefore turns exactly one of these `.fail()` steps into an enabled action.
  // ---------------------------------------------------------------------------

  // Operand 1, the observed primary (ext-17 mutation (8), the attestation fence itself):
  // the attestation names the primary the replica was synced from, and it is applied after
  // a re-parent onto a different one.
  run staleAttestationRefusedAcrossReParentTest: bool = {
    init
      .expect(and { nodes.get(3).parent == Some(1), nodes.get(3).parent_seq == 0, nodes.get(3).synced })
      .then(setRole(3, Replica, Some(4)))
      .expect(and {
        nodes.get(3).parent == Some(4),
        // The other two operands match the in-flight attestation exactly.
        nodes.get(3).parent_seq == 1,
        nodes.get(3).registration_seq == Some(3),
        not(nodes.get(3).synced),
      })
      .then(attestReplicaSynced(3, 1, 1, 3).fail())
  }

  // Operand 2, the parenting token (ext-17 mutation (12), V16-C1) — the away-and-back
  // schedule the token exists for. At apply time the *observed primary* matches again and
  // the registration never changed, so nothing but `parent_seq` distinguishes an
  // attestation minted before the round trip from a fresh one. Without the token the
  // stale attestation re-arms candidacy over a dataset node 3 synced from node 4, and the
  // next graceful failover of node 1 promotes it.
  run staleAttestationRefusedAcrossReParentRoundTripTest: bool = {
    init
      .then(setRole(3, Replica, Some(4)))
      .then(setRole(3, Replica, Some(1)))
      .expect(and {
        // Indistinguishable from the mint state on every operand but the token.
        nodes.get(3).parent == Some(1),
        nodes.get(3).registration_seq == Some(3),
        nodes.get(3).parent_seq == 2,
        not(nodes.get(3).synced),
        // ...and while it is unsynced it is not a graceful-failover candidate.
        not(canApplyFailover(nodes, slots, 1, 3, true, 0, Primary)),
      })
      .then(attestReplicaSynced(3, 1, 0, 3).fail())
  }

  // Operand 3, the registration (ext-17/V18-C1 mutation (13)): the node leaves the
  // membership and rejoins between the mint and the apply. The parenting token is *not*
  // enough here — the rejoin re-derives it, and node 3's token happens to be back at a
  // value an in-flight attestation can carry — so the registration counter is what makes
  // the pre-wipe attestation refusable.
  //
  // The boot report is part of the schedule deliberately: without it the model's
  // boot-ordering guard (`canAct`) would refuse the attestation too and the refusal below
  // would be over-determined rather than attributable to the registration operand.
  run staleAttestationRefusedAcrossRejoinTest: bool = {
    init
      .then(removeNode(3, false, false))
      .expect(nodes.get(3).registration_seq == None)
      .then(meetNode(3))
      .expect(nodes.get(3).registration_seq == Some(5))
      .then(reportRunIdentity(3, Boot, Primary, 0, false))
      .then(setRole(3, Replica, Some(1)))
      .expect(and {
        // Every other operand of the in-flight attestation matches...
        nodes.get(3).parent == Some(1),
        // Three parent writes: issue 33's tombstone null at `removeNode`, the rejoin, and
        // the `setRole`. Each moves the token, which is the ext-17 funnel's whole point.
        nodes.get(3).parent_seq == 3,
        canAct(nodes, 3),
        // ...and the registration it was minted under is gone.
        nodes.get(3).registration_seq == Some(5),
      })
      .then(attestReplicaSynced(3, 1, 3, 3).fail())
  }

  // The same schedule with the run tag pinned (ext-17/V18-C1 mutation (14)): node 3 never
  // restarts, so it rejoins carrying the **same `RunId`** it left with and its re-minted
  // identity carries that same run. A freshness check keyed on the run tag would see no
  // change at all across the wipe — which is the point of `RunId` being opaque and of the
  // registration counter, not the run, being the pairing partner of the stage.
  run staleAttestationRefusedWithRepeatedRunIdTest: bool = {
    init
      .expect(nodes.get(3).stored_identity == Some({ inc: 1, seq: 1, run_id: 3 }))
      .then(removeNode(3, false, false))
      .then(meetNode(3))
      .then(reportRunIdentity(3, Boot, Primary, 0, false))
      .expect(and {
        // Same run, on both sides of the wipe: the tag distinguishes nothing.
        nodes.get(3).run_id == 3,
        nodes.get(3).stored_identity == Some({ inc: 1, seq: 2, run_id: 3 }),
        nodes.get(3).registration_seq == Some(5),
      })
      .then(setRole(3, Replica, Some(1)))
      .then(attestReplicaSynced(3, 1, 3, 3).fail())
  }

  // ---------------------------------------------------------------------------
  // ext-15/ext-16: the two planes and the lineage guard.
  // ---------------------------------------------------------------------------

  // V12-M2's mute node, and the action that saves it. Node 3 crashes; while it is down a
  // forced failover promotes it in the **replicated** plane and its old upstream is then
  // removed (TR-CLUSTER-042). Its node-local plane still says "replica of 1", so the two
  // planes are split and every `Demotion` report it could phrase is against a role it no
  // longer has. `adoptReplicatedRole` converges the split before the boot report proposes.
  //
  // Two of the doc's mutations are pinned here: deleting the action (or making it write
  // the wrong plane) leaves `witnessSplitPlanesConverge` unreachable, and narrowing the
  // lineage guard to `u == None or in_shard_parent(u, n)` — dropping the removed-upstream
  // disjunct — makes the adoption itself inadmissible in exactly this state.
  run splitPlanesConvergeAfterCrashedPromotionTest: bool = {
    init
      .then(sourceRestart(3))
      .expect(not(nodes.get(3).booted))
      .then(failoverForced(1, 3, 0, Primary))
      .expect(and {
        // The replicated cell moved...
        nodes.get(3).role == Primary,
        nodes.get(3).parent == None,
        nodes.get(3).promoted_from == Some(1),
        // ...and the node-local plane did not, because the node was down.
        nodes.get(3).local_role == Replica,
        nodes.get(3).local_parent == Some(1),
        identityFactsDiffer(nodes, 3),
        // Nothing has converged yet.
        not(witnessSplitPlanesConverge),
      })
      .then(removeNode(1, false, false))
      .expect(and {
        not(nodes.get(1).member),
        // The removed-upstream disjunct, and only it, admits the adoption here.
        lineageOk(nodes, 3),
        canAdoptReplicatedRole(nodes, 3),
      })
      .then(adoptReplicatedRole(3))
      .expect(and {
        nodes.get(3).local_role == Primary,
        nodes.get(3).local_parent == None,
        not(identityFactsDiffer(nodes, 3)),
        // Converged *before* the boot report proposed anything.
        not(nodes.get(3).booted),
        witnessSplitPlanesConverge,
      })
  }

  // ext-16 (V14-C2): the retightening itself. Node 2 is re-parented onto a foreign primary
  // while it is down, and its own old upstream then departs. The difference test fires —
  // the two planes really do disagree — but the local upstream is explicable by neither
  // an in-shard parent nor a promotion, so the adoption is refused rather than
  // substituting a foreign lineage for node 2's own. The bare `u not in nodes` disjunct
  // V13-M1 first carried admits it.
  run foreignLineageAdoptionRefusedTest: bool = {
    init
      .then(sourceRestart(2))
      .then(setRole(2, Replica, Some(4)))
      .expect(and {
        nodes.get(2).parent == Some(4),
        nodes.get(2).local_parent == Some(1),
      })
      // Force: node 1 is still a slot-owning primary.
      .then(removeNode(1, true, false))
      .expect(and {
        not(nodes.get(1).member),
        nodes.get(2).promoted_from == None,
        // The difference test is satisfied, so the refusal below is the lineage guard's
        // and nothing else's.
        identityFactsDiffer(nodes, 2),
        not(lineageOk(nodes, 2)),
        not(canAdoptReplicatedRole(nodes, 2)),
      })
      .then(adoptReplicatedRole(2).fail())
  }

  // `witnessSplitPlanesConverge`'s silence half (doc: "the difference test treats a
  // locally detached link as matching replicated `primary_id == None`"). Node 3 is down
  // when its primary is removed, so the detach writes `parent == None` into the
  // replicated cell while node 3 still names 1 locally. The equivalence makes that a
  // *match*: there is nothing to reconcile and nothing to adopt, and the node sits in its
  // declared state instead of re-proposing on every observation cycle. Dropping the
  // equivalence turns both refusals below into enabled actions — the permanent proposal
  // loop.
  //
  // Silence is not muteness: the node's ordinary boot report is admissible throughout.
  run detachedReplicaProposesNothingTest: bool = {
    init
      .then(sourceRestart(3))
      .then(removeNode(1, true, false))
      .expect(and {
        nodes.get(3).parent == None,
        nodes.get(3).local_parent == Some(1),
        not(nodes.get(1).member),
        localUpstreamMatches(nodes, 3),
        not(identityFactsDiffer(nodes, 3)),
        not(canReconcileIdentity(nodes, 3)),
        not(canAdoptReplicatedRole(nodes, 3)),
      })
      .then(reportRunIdentity(3, Boot, Replica, 0, false))
      .expect(and {
        nodes.get(3).booted,
        // Still silent on the far side of the boot report.
        not(identityFactsDiffer(nodes, 3)),
        not(canReconcileIdentity(nodes, 3)),
        not(canAdoptReplicatedRole(nodes, 3)),
      })
      .then(reconcileIdentity(3).fail())
  }
}
