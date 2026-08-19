// cluster_migration_failover_types — types and constants for the migration x failover
// composite model (formal-spec phase 2, task 2). The state-shape half of the model: the
// "Node role"/"Node parent pointer"/"Node membership"/"Node FAIL flag" state-space rows
// behind TR-CLUSTER-003/004, the handoff record behind TR-CLUSTER-011/012 (INV-HANDOFF-1,
// INV-HANDOFF-2), and the migration record behind TR-CLUSTER-010/015 (INV-MIG-1).
//
// Reworked for the issue-31 slot-migration redesign (`.scratch/cluster-correctness/
// 2026-08-14-issue31-migration-design.md`, "Quint rework" section). The superseded
// source-side *repatriation* design is gone from every file: the redesign is
// **source-authoritative-until-commit** — the source keeps its copy (and its
// authority) until the target's promotion is attested, so there is nothing to stream
// back and no `repatriating` phase. What replaces it is the shadow/residue pair
// (ext-8/ext-12) plus the reaper (ext-7).
//
// No state variables and no logic live here — see cluster_migration_failover.qnt (the
// runnable main model) for the full "Rows modeled" header, the deliberately-not-modeled
// list, and the layout note.
//
// Single responsibility: the *shape* of this model's state and the constants that bound
// it — nothing that reads or writes state.
//   Belongs here:     type declarations, sizing constants (`NODES`, `SLOTS`,
//                     `HOLD_BYTE_CAP`, the ext-4 observation bounds).
//   Does NOT belong:  `var`s, guards, updates, invariants, witnesses, tests. Guards and
//                     updates live in cluster_migration_failover_logic.qnt; the `var`s and
//                     the actions in cluster_migration_failover_machine.qnt; everything
//                     checkable in cluster_migration_failover.qnt.
//   Shared with the admission model: `NodeId` and `Option` come from
//                     cluster_common_types.qnt. Everything below is migration-specific by
//                     construction — the two models' `NodeState` records share a name and
//                     nothing else, and are deliberately not unified.
module cluster_migration_failover_types {
  import cluster_common_types.* from "./cluster_common_types"

  type SlotId = int
  // ext-8/ext-12: migrations are identified, because a shadow and a residue entry both
  // name the migration they belong to and must be distinguishable from a *later*
  // migration over the same slot (the orphan-shadow and stale-residue states).
  type MigrationId = int
  // ext-1/ext-5: an opaque run tag. Deliberately uninterpreted (equality only) — see the
  // rework section's "`RunId` is deliberately opaque" structural limit; no admission
  // fence in this model reads it for *freshness* (that job belongs to `registration_seq`
  // and `parent_seq`), only for same-run/cross-run discrimination.
  type RunId = int

  type RoleTag = Primary | Replica

  // ext-1: a stream position always carries the run that produced it, so a comparison
  // across runs is a *distinguishable* (and refusable) event rather than an arithmetic
  // accident. `confirmDrained` draws its position from this same tagged space.
  type Pos = { run_id: RunId, pos: int }

  // ext-3/ext-4: the migration's phase, tracked **separately** from the source fence
  // (ext-2's `fenced` field below) — the two are independent facts and the redesign's
  // `Complete` admission conjuncts check them separately.
  type Phase = Streaming | Draining

  // ext-15/ext-16: `ReportRunIdentity` is kind-stamped, and the kind is a declared
  // payload field (the V7-C1 arm-selector rule). The `Promotion` alternative is deleted
  // per V14-C1: cluster mode refuses `REPLICAOF` at dispatch, so no bare-promotion
  // command exists to model; promotion rides the failover transitions instead.
  type TransitionKind = Boot | Demotion

  // ext-16: the apply outcome of a refused `ReportRunIdentity`, as a committed value —
  // the disposition actions are guarded on *this*, never on a modeller's paraphrase.
  // `Ordering` = the identity-ordering conjunct failed; `Fenced` = the Demotion fence's
  // observed-role/observed-epoch conjunct failed; `Membership` = the proposer is not a
  // member; `Whole` = the refuse-whole class.
  type RefusalClass = Ordering | Fenced | Membership | Whole

  // ext-18: a refusal in flight, carrying the pair the record-binding conjunct reads.
  // `terminal` marks the classes the design declares terminal (arm 4b: `Ordering`
  // against an absent stored identity; arm 5: `Membership`).
  type Refusal = {
    node: NodeId,
    stage_id: int,
    reg_seq: int,
    class: RefusalClass,
    terminal: bool,
  }

  // ext-18 (V28-M6): the auxiliary history value `inv_no_live_record_disposed_by_a_
  // foreign_refusal` reads — written by `observeRefusal` on any step that disposes of a
  // record, and by nothing else.
  type Disposition = {
    refusal_stage_id: int,
    refusal_reg_seq: int,
    record_stage_id: int,
    record_reg_seq: int,
  }

  // ext-16/ext-18: the node-local, crash-durable pending-transition record. `stage_id`
  // and `adopted` are the fields a restart must preserve **field-for-field** (V14-M1);
  // `staged_reg_seq` is the registration the stage was minted under (V18-C1's pairing
  // partner), and it is what `clearStaleRecord` compares against the live cell.
  type TransitionRecord = {
    kind: TransitionKind,
    upstream: NodeId,
    stage_id: int,
    staged_reg_seq: int,
    adopted: bool,
  }

  // ext-15: the replicated run-identity triple. `inc`/`seq` order reports *within* a
  // registration; `run` is the opaque tag above.
  type RunIdentity = { inc: int, seq: int, run_id: RunId }

  type NodeState = {
    role: RoleTag,
    // "Node parent pointer" (`primary_id`) — Some(p) only meaningful when role ==
    // Replica (TR-CLUSTER-004's ruled precondition, issue 14: p must exist and be a
    // Primary). Note `parent == None` at a Replica is a *lawful, declared* state
    // (ext-17, V15-C3): `removeNode`'s detach writes exactly it.
    parent: Option[NodeId],
    // "Node membership" — a genuinely two-way field: TR-CLUSTER-003 (`removeNode`) can
    // clear it, gated by issue 20's amendment; `meetNode` sets it again.
    member: bool,
    // "Node FAIL flag".
    fail: bool,
    // "Node's own config epoch".
    epoch_own: int,
    // ext-7: the node's own keyspace — the slot copies it physically holds. `reapSlots`
    // is the model's **only** deleter of a member's entry here (V8-C1).
    keys: Set[SlotId],
    // --- ext-16/ext-17 companions of the parent pointer -----------------------------
    // Every writer of `primary_id` writes all three in the same step and increments
    // `parent_seq` (the parenting token, V16-C1) — see `writeParentPointer` in the logic
    // module, the single funnel that makes that rule structural rather than a comment.
    synced: bool,
    admitted_stage: Option[int],
    promoted_from: Option[NodeId],
    parent_seq: int,
    // ext-17/V18-C1: minted from the cluster-level counter at every *fresh*
    // registration, deleted with the cell. `None` == no cell.
    registration_seq: Option[int],
    // --- ext-15 run identity ---------------------------------------------------------
    // node-local counters (`loseIncarnationCounter` can drop them),
    incarnation: int,
    identity_seq: int,
    // the replicated cell they are admitted into,
    stored_identity: Option[RunIdentity],
    // and the boot-ordering guard: a node proposes no other action until its boot
    // report is applied.
    booted: bool,
    // the node's current run (re-minted by `sourceFailover`/`sourceRestart`/
    // `targetRestart`).
    run_id: RunId,
    // --- ext-18 pending-transition record --------------------------------------------
    record: Option[TransitionRecord],
    // node-local stage counter; the model's `wipe` composite (removeNode + rejoin)
    // resets it to 0, which is what makes the cross-registration id collision
    // expressible at all.
    stage_counter: int,
    // the whole-node `-TRYAGAIN` fence, modelled as *state* so a permanent fence is a
    // witnessable state rather than a prose claim.
    node_fenced: bool,
    reply_emitted: bool,
    last_disposition: Option[Disposition],
    // --- ext-11/ext-13 escape flags --------------------------------------------------
    // alive-but-unable-to-apply (the self-fence release must still empty the held set),
    cannot_apply: bool,
    // alive source that never proposes `Confirm`,
    cannot_drain: bool,
    // ext-11's mirror: alive-but-unable-to-act on the *target* side.
    silent: bool,
    // --- ext-6/ext-14 target-replica bookkeeping -------------------------------------
    // how much of the shadow this replica of a migration target holds,
    replica_copy: int,
    // whether it is attached and *counted* toward the `Complete` replica-ack conjunct,
    counted: bool,
    // ext-6/V5-M4: a shadow install interrupted mid-section (no completion marker) —
    // must be discarded and re-requested at boot, never promoted.
    install_incomplete: bool,
  }

  // ext-8: the target's shadow keyspace for a slot, tagged with the **reset epoch** it
  // was minted under (ext-10) so an orphan shadow from a pre-reset epoch is a
  // distinguishable reachable state rather than an indistinguishable one.
  type ShadowTag = { mig: MigrationId, epoch: int }

  // ext-12: the residue entry `completeMigration` writes and only `clearSlotResidue`
  // (or an operator arm) removes. `source_gone`/`target_gone` are the mark-don't-remove
  // marks `removeNode` writes.
  type Residue = {
    mig: MigrationId,
    source: NodeId,
    target: NodeId,
    promoted: bool,
    source_gone: bool,
    target_gone: bool,
  }

  type Migration = {
    id: MigrationId,
    source: NodeId,
    target: NodeId,
    // ext-3: the attempt stamp. 0 == no prepared handoff; every confirm/release/
    // complete/cancel carries it and is refused on a mismatch.
    attempt: int,
    // ext-4: Streaming until `Confirm` seals, Draining after.
    phase: Phase,
    // ext-2: the source's own fence, tracked separately from `phase`.
    fenced: bool,
    // ext-1 (V4-C2): the run tag `prepareHandoff` carried.
    prepare_run: RunId,
    // ext-1: the source's log high-water, run-tagged.
    source_log: Pos,
    // ext-1: the position `confirmDrained` carried, drawn from the same tagged space.
    drained_pos: Option[Pos],
    // ext-5 (V8-C3/V9-C2): the target's last *replicated* ingest report, carrying the
    // attesting run. A cross-run report replaces (may regress) rather than maxes.
    target_copy: Pos,
    // ext-5: the target's volatile applied position and the durable point a power-loss
    // restart falls back to. A report may only propose a position from the durable set.
    target_applied: int,
    target_durable: int,
    // ext-6/ext-14: how far the *counted* target replicas have acked.
    target_replica_ack: int,
    // ext-4: the replicated observation counter, its last observed progress point, and
    // the pre/post-`Confirm` bounds captured on the record at `Begin`. `confirmDrained`
    // resets the counter (V7-C3) so the post-seal bound is not consumed pre-seal.
    observations: int,
    last_observation: Option[int],
    preconfirm_bound: int,
    draining_bound: int,
    // ext-14 (V6-C2): a **captured parameter**, stamped into the record from the
    // *proposer's* local config at `Begin` and immutable thereafter. It is deliberately
    // NOT a global/config read at admission time: a replicated predicate that consults
    // node-local config admits on one applier what it refuses on another (permanent Raft
    // divergence), and a knob flipped mid-migration would silently void the durability
    // conjunct the operator believed was armed. Changing it means Cancel + a fresh
    // `Begin`. `canComplete`'s optional durability conjunct exists iff this is set.
    require_replica_ack: bool,
  }

  // This model's node/slot universes — bounded-checking sizing knobs, deliberately NOT
  // shared with the admission model even though the two agree on 4 nodes today (see
  // cluster_common_types.qnt's "does not belong here" note).
  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)
  pure val SLOTS: Set[SlotId] = Set(1, 2, 3, 4)
  pure val HOLD_BYTE_CAP: int = 6
  // ext-4 stock sizings: the pre-`Confirm` bound and the post-seal `draining_
  // observations` default (3 — the V8-m4 "stock defaults" the knob-on witness must be
  // reachable at).
  pure val PRECONFIRM_OBSERVATIONS: int = 4
  pure val DRAINING_OBSERVATIONS: int = 3
}
