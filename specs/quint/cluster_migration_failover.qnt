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
//     Disclaimer: TR-CLUSTER-025's no-bump anti-churn property (FM-CLUSTER-014) is not
//     machine-checked here.
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
  //   Migration lifecycle  — witnessMigrationInFlight, witnessHandoffDrainedNotComplete
  //   Failover             — witnessGracefulFailoverPrunedMigration,
  //                          witnessForcedFailoverDemoted, witnessFailoverDuringOpenMigration
  //   Barrier and hold     — witnessFeedDisconnected
  //   Membership           — witnessNodeRemoved
  //   Residue and reaper   — witnessResiduePending
  // A witness backed by a `coverage.*` ghost is set inside the action's own execution, so
  // it cannot be satisfied by a state that merely looks like the behaviour happened.
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
  //   Residue and reaper     — reserved: the ext-7/ext-12 properties land in Q2.
  //   Identity and run       — reserved: the ext-15/ext-18 properties land in Q2.
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
}
