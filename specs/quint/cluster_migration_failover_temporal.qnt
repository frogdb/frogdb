// cluster_migration_failover_temporal — the **liveness** (temporal) properties of the
// migration x failover composite model (quint-completeness campaign, T6).
//
// Models the spec rows TR-CLUSTER-011 (`PrepareSlotHandoff` — the action that *latches*
// the hold region), TR-CLUSTER-012/013/014/015 (`ConfirmSlotHandoffDrained` /
// `CompleteSlotMigration` / `AbortSlotHandoff` / `CancelSlotMigration` — the resolvers
// that release it), TR-CLUSTER-016 (the replica-feed hold during an armed barrier, and
// its local cap-breach/self-fence releases) and TR-CLUSTER-034 (the per-node barrier
// arm/release reaction). It adds no state, no action and no guard: it is a **sibling root
// module** over the same machine, declaring only `temporal` properties.
//
// ---------------------------------------------------------------------------
// Why this is a separate file.
// ---------------------------------------------------------------------------
//
// `cluster_migration_failover.qnt` (the safety main module) is byte-frozen for the
// conformance harness and for the concurrent issue-31 work; a `temporal` declaration
// there would also be invisible to `quint run` (which ignores temporal properties) while
// still perturbing a file many agents share. Keeping the properties here means:
//
//   - `scripts/quint-models.sh` selects "main models" by *declaring at least one
//     `val inv_*`*. This module declares none, so the PR-lane `just quint-run` gate and
//     the existing `just quint-verify-*` nightly sweeps do not pick it up. Nothing in the
//     dev loop or the commit gates changes.
//   - `just quint-check` still type-checks it (it globs every `.qnt`), so the properties
//     cannot rot silently.
//   - `just quint-verify-temporal` (nightly lane only) is the single sanctioned caller.
//
// This module imports the same satellites the safety main module imports, so it is an
// independent root over an identical state machine — not a second instance of the safety
// module's state.
//
// ---------------------------------------------------------------------------
// What these properties close.
// ---------------------------------------------------------------------------
//
// The safety module records two structural limits it cannot state as `val inv_*`:
//
//   - `inv_no_stuck_handoff` — "from any reachable state with an armed barrier, some
//     enabled action sequence releases it" — is listed under *Deliberately not modeled*
//     as "a temporal liveness property, not a state invariant `quint run --invariant` can
//     check in 0.32.0", left as a by-construction argument over the release actions.
//   - `inv_held_set_empty_while_latched` states only the **level** half of the design's
//     "the held set empties while the latch holds"; its docstring says outright that "the
//     eventually-empties form is a liveness property Quint invariants cannot state".
//
// Both halves are stated below as `temporal` properties, plus mutation-battery row M22
// (step-unwiring), which the campaign doc records as needing a temporal operator.
//
// **Accepted limitation — row M22** (cluster issue 44 / R2, 2026-08-23). Unwiring
// `retargetSlotResidue` from `step` while leaving its guard true is undetectable by the
// safety module: `inv_residue_has_an_effective_remover` is a guard-level state predicate
// (it asks whether *some* remover is admissible here), so removing a verb's
// *schedulability* cannot falsify it, run tests invoke the action directly and so are
// unaffected, and `quint run` has no liveness or temporal checking at all. Detecting it
// requires a temporal operator the invariant set deliberately excludes — which is exactly
// what this module's Apalache-only lane would supply if it converges at useful depth;
// revisit only if campaign work makes the observable cheap.
//
// ---------------------------------------------------------------------------
// FAIRNESS — stated explicitly, because a liveness property is only as meaningful as
// the fairness it is asserted under.
// ---------------------------------------------------------------------------
//
// Quint/TLA+ traces admit stuttering: without a fairness hypothesis every one of these
// properties is trivially false (the system simply stops taking steps). The hypothesis
// asserted here, per slot `s`, is:
//
//   (F1) **weak** fairness of `reconcileTick(s)` — the leader keeps reconciling an open
//        migration record. `reconcileTick` is enabled whenever the record exists, so WF
//        says: while a record is open, ticks keep happening. This is the model's stand-in
//        for "the Raft leader runs its reconcile loop"; a leaderless cluster is out of
//        scope of this model entirely (it has no consensus layer — see the safety
//        module's "one Raft state machine" note).
//
//   (F2) **strong** fairness of `boundAbort(s)` — the bounded exit fires if it is
//        *infinitely often* enabled. Weak fairness is deliberately not enough here:
//        `applyReconcileTick` **resets** `observations` to 0 whenever the tick saw ingest
//        progress (`hasObservedProgress`), which momentarily *disables* `boundAbort`. A
//        weakly-fair `boundAbort` could therefore be starved by an interleaving that
//        sneaks a progress tick in before each abort. Strong fairness is the honest
//        assumption: the leader does not skip the bounded exit every single time it comes
//        up.
//
// Under (F1)+(F2) the argument for `temporal_no_stuck_handoff` is: suppose a record stays
// open forever. By (F1) infinitely many ticks fire. Either infinitely many of them see
// ingest progress — impossible, because `target_copy.pos` is monotone and bounded in any
// bounded-depth Apalache run — or only finitely many do, after which `observations` grows
// past `observationBound` and stays there, so `boundAbort(s)` is enabled from then on and
// (F2) fires it, closing the record. Contradiction.
//
// NOT assumed, deliberately:
//   - No fairness on `completeMigration`/`confirmDrained`/`abortHandoff`/`cancelMigration`.
//     The properties must hold on the *bounded-exit* path alone; assuming the happy path
//     eventually fires would make them vacuous restatements of "the migration succeeds".
//   - No fairness on `sourceWrite`/`applyAtTarget`/`feedBuffer`. Client traffic is not
//     something the protocol may assume.
//   - No fairness on any failover or membership action. A failover that prunes a
//     migration *also* releases the barrier, so assuming it would weaken the properties;
//     they are asserted without it.
//
// `fairnessVars` is the frame the fairness conditions are taken over: `weakFair(a, v)` is
// `a.mustChange(v)`-based, so `v` must cover every variable a fairness-relevant step
// writes. `reconcileTick` writes `migrations`/`defects`; `boundAbort` writes
// `migrations`/`barriers`/`held`/`feed_bytes`/`coverage`. Quint's `weakFair`/`strongFair`
// take a single generic value, so the frame is spelled as one tuple of those variables
// rather than a (necessarily homogeneous) set.
//
// ---------------------------------------------------------------------------
// Checking status — see .scratch/formal-spec/t6-findings.md.
// ---------------------------------------------------------------------------
//
// These properties are checked ONLY by Apalache in the nightly lane
// (`just quint-verify-temporal`, wired by `.github/workflows/workflow_gen/.../
// quint_verify.py`). They are NOT checked by `quint run` (which ignores `temporal`
// declarations outright) and NOT by any commit gate or PR-lane job. The campaign ruling
// scopes `quint verify` to the nightly job only.

module cluster_migration_failover_temporal {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_migration_failover_types.* from "./cluster_migration_failover_types"
  import cluster_migration_failover_logic.* from "./cluster_migration_failover_logic"
  import cluster_migration_failover_machine.* from "./cluster_migration_failover_machine"

  // ---------------------------------------------------------------------------
  // State predicates the properties are phrased over.
  // ---------------------------------------------------------------------------

  // A live migration record exists on the slot — the state `beginMigration` enters and
  // every resolver (`completeMigration`, `abortHandoff`, `cancelMigration`, `boundAbort`,
  // a pruning failover) leaves.
  def migrationOpen(s: SlotId): bool = migrations.get(s) != None

  // The slot sits in `n`'s hold region: writes for `s` are latched at `n` rather than
  // answered. `prepareHandoff` is the only action that puts a slot here.
  def holdLatched(n: NodeId, s: SlotId): bool = held.get(n).contains(s)

  // Some node holds something. The aggregate form of the hold-region property below.
  val anyHoldLatched: bool = NODES.exists(n => held.get(n) != Set())

  // ---------------------------------------------------------------------------
  // Fairness frame and conditions (see the header for the full rationale).
  // ---------------------------------------------------------------------------

  // Every variable a fairness-relevant action writes. `mustChange` is evaluated over
  // this tuple, so an action that leaves all of them alone does not count as a step for
  // fairness purposes.
  val fairnessVars = (migrations, barriers, held, feed_bytes, defects, coverage)

  // (F1) The leader keeps reconciling open records.
  temporal reconcileFair: bool =
    SLOTS.forall(s => reconcileTick(s).weakFair(fairnessVars))

  // (F2) The bounded exit is not starved by progress-tick interleavings.
  temporal boundExitFair: bool =
    SLOTS.forall(s => boundAbort(s).strongFair(fairnessVars))

  temporal handoffFairness: bool = reconcileFair and boundExitFair

  // ---------------------------------------------------------------------------
  // Properties.
  // ---------------------------------------------------------------------------

  // P1 — `inv_no_stuck_handoff`, the eventually form. The safety module lists this under
  // *Deliberately not modeled* precisely because `quint run --invariant` cannot state it.
  // An open migration record does not stay open forever: it commits, is cancelled,
  // aborted, pruned by a failover, or taken by the observation bound.
  temporal temporal_no_stuck_handoff: bool =
    handoffFairness implies
      SLOTS.forall(s => migrationOpen(s) leadsTo not(migrationOpen(s)))

  // P2 — the barrier form of the same claim, one step closer to the operator-visible
  // symptom: an armed handoff barrier eventually disarms. Every resolver clears the slot
  // from `barriers.armed` in the same step it resolves the record (`releaseHoldRegion`),
  // and the two local releases (`localReleaseOnCapBreach`, `selfFenceRelease`) disarm it
  // without resolving the record at all (`releaseLocalFence`) — so this is implied by P1
  // but is stated separately because it is the property the *fence* has to satisfy, and
  // it stays true under mutations that break only the record lifecycle.
  temporal temporal_barrier_eventually_disarms: bool =
    handoffFairness implies
      SLOTS.forall(s => barrierArmed(s) leadsTo not(barrierArmed(s)))

  // P3 — "a latched hold does not hold forever": the eventually-empties half of
  // `inv_held_set_empty_while_latched`, whose docstring names this as the liveness
  // property a Quint invariant cannot state. Phrased per (node, slot) rather than as
  // "the node's whole held set empties": a node may legitimately latch a *second* slot
  // before the first releases, so the set-level form would be false of the model for a
  // reason that is not a defect. Each individual latch, though, must let go.
  temporal temporal_hold_eventually_releases: bool =
    handoffFairness implies
      NODES.forall(n => SLOTS.forall(s =>
        holdLatched(n, s) leadsTo not(holdLatched(n, s))))

  // P4 — the aggregate hold-region form, kept as a separate (weaker-looking but
  // genuinely different) property: the cluster is never *permanently* holding writes
  // somewhere. Unlike P3 this one is false if holds hand off from node to node forever,
  // which is exactly the pathology a per-latch property cannot see. Recorded as
  // **expected-to-be-harder**: under (F1)+(F2) alone nothing bounds how often
  // `prepareHandoff` may re-latch, so a counterexample here is a fairness-strength
  // finding, not necessarily a model defect — see t6-findings.md before acting on one.
  temporal temporal_hold_region_eventually_empties: bool =
    handoffFairness implies (anyHoldLatched leadsTo not(anyHoldLatched))
}
