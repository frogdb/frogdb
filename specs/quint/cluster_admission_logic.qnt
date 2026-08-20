// cluster_admission_logic — the pure logic of the admission-window model (formal-spec
// phase 2, task 1): every guard (`can*`) and every update (`apply*`), each taking the
// node state it reads as an explicit argument, so this module holds no `var` and is
// unit-testable in isolation.
//
// Rows whose ruled semantics are encoded here: TR-CLUSTER-028 (explicit bootstrap, with
// the persisted-intent amendment), TR-CLUSTER-029 (a joining node defers election),
// TR-CLUSTER-005 (the amended MEET join-safety gate) and TR-CLUSTER-035 (`CLUSTER RESET`
// as the residue hazard that gate exists to repel), plus the state-space row "This node's
// own id" (cluster.md:116), whose per-boot id derivation the identity helpers below encode.
// Per-definition comments carry the full ruling/amendment rationale; the model-wide header
// lives in cluster_admission.qnt.
//
// Single responsibility: the pure, state-argument-taking half of the model.
//   Belongs here:     `can<Transition>` admission guards, `apply<Transition>` postcondition
//                     updates, and the helpers they share — every one a `pure def` over
//                     explicitly-passed state.
//   Does NOT belong:  `var` declarations, `action`s, frame keepers, `init`/`step`
//                     (cluster_admission_machine.qnt); invariants, witnesses and `run`
//                     tests (cluster_admission.qnt); type/constant declarations
//                     (cluster_admission_types.qnt, cluster_common_types.qnt).
//
// Naming convention (shared with the migration model's logic module):
//   `can<Transition>`   — the admission guard of the action of the same name.
//   `apply<Transition>` — that action's pure postcondition update.
module cluster_admission_logic {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_admission_types.* from "./cluster_admission_types"

  // ---------------------------------------------------------------------------
  // Functional logic (guards + updates), pure and unit-testable in isolation.
  // ---------------------------------------------------------------------------

  // --- Identity (state-space row "This node's own id", cluster.md:116) --------

  // What the `cluster-node-id` knob says at this boot. `pin == false` is the default
  // (the knob left at 0): no configured id, so this boot mints a fresh one. `pin == true`
  // is the "explicitly configured non-zero" case; the value pinned is the id the node is
  // already running under, or — at a first boot, where there is none yet — its own durable
  // record's key. The *value* is not what the row is about (only its stability across boots
  // is), and pinning what the node already is keeps two nodes from ever pinning the same
  // number, which is what `inv_effective_ids_distinct` asserts.
  pure def configuredIdFor(ns: NodeState, boxId: NodeId, pin: bool): Option[NodeId] =
    if (not(pin)) None
    else match ns.effective_id { | Some(e) => Some(e) | None => Some(boxId) }

  // The id this boot runs under: the configured one unchanged if there is one, otherwise
  // the freshly minted one the machine supplies. This is `effective_node_id` itself.
  pure def idForBoot(cfg: Option[NodeId], fresh: NodeId): NodeId =
    match cfg { | Some(c) => c | None => fresh }

  // True when this boot consumes a fresh mint — i.e. the id churns. The machine advances
  // its mint source only then, so no two mints can produce the same id.
  pure def mintsFreshId(cfg: Option[NodeId]): bool = cfg == None

  // --- Boot / restart --------------------------------------------------------

  // TR-CLUSTER-028/029: a node only ever decides its mode once, at its first boot.
  pure def canBoot(ns: NodeState): bool = ns.intent == Undecided

  // TR-CLUSTER-028 (cb == true): explicitly configured to bootstrap a fresh
  // deployment — initializes its own Raft group immediately and becomes an
  // externally-routable leader of it; `member_of` is keyed on its own id (state-space
  // row "This node's own id"), since bootstrapping mints a fresh, independently-owned
  // cluster identity (TR-CLUSTER-030 seeds that cluster's slots next, out of scope
  // here). TR-CLUSTER-029 (cb == false): not configured to bootstrap — defers
  // raft.initialize/election entirely; no externally-observable leadership claim,
  // no cluster membership, until a real MEET folds it in.
  //
  // `selfId` is the id *this boot* runs under (`idForBoot` above), not a fixed property of
  // the node: a bootstrap mints its cluster identity out of whatever id it happens to be
  // running under at that moment, and `cfg` records the knob that decided it so a later
  // restart can tell "same id every boot" from "fresh id every boot".
  pure def applyBoot(ns: NodeState, selfId: NodeId, cfg: Option[NodeId], cb: bool): NodeState = {
    val identified = { ...ns, effective_id: Some(selfId), configured_id: cfg,
                       id_mints: ns.id_mints + (if (mintsFreshId(cfg)) 1 else 0) }
    if (cb)
      { ...identified, intent: Bootstrap, configured_bootstrap: cb, raft: Initialized(1),
        routable_leader: true, member_of: Some(selfId),
        decision_epoch: ns.decision_epoch + 1, raft_origin: Some(selfId) }
    else
      { ...identified, intent: Join, configured_bootstrap: cb, raft: Empty,
        routable_leader: false, member_of: None,
        decision_epoch: ns.decision_epoch + 1 }
        // raft_origin deliberately untouched — stays None, this node has never
        // independently originated Raft state.
  }

  // TR-CLUSTER-028/029 amendment (issue 25, "persist bootstrap intent"): once a mode
  // is decided, a restart never re-derives it — it only observes the current
  // (possibly since-flipped) config, not use it to redecide the mode.
  pure def canRestart(ns: NodeState): bool = ns.intent != Undecided

  //
  // A restart also re-runs `effective_node_id` (state-space row "This node's own id"), so
  // `effective_id`/`configured_id`/`id_mints` are the only cells it writes: unconfigured,
  // the node comes back up under a brand-new id. Everything the admission window turns on —
  // `intent`, `raft`, `routable_leader`, `member_of`, `raft_origin` — is keyed to the
  // durable node-local record, not to that id, so it persists across the churn untouched.
  // That is the determinism claim itself: nothing here reads `cb` or the new id to decide
  // anything. `defects.restartIntentWrite`/`restartIdentityRemint` in the machine recompute
  // whether this held, from the outcome rather than from the promise.
  pure def applyRestart(ns: NodeState, cb: bool, selfId: NodeId, cfg: Option[NodeId]): NodeState =
    { ...ns, configured_bootstrap: cb, effective_id: Some(selfId), configured_id: cfg,
      id_mints: ns.id_mints + (if (mintsFreshId(cfg)) 1 else 0) }

  // TR-CLUSTER-005, amended (issue 25): `n` folds `m` in only if `n` already belongs
  // to a real cluster (structural — `n` needs a cluster to fold `m` into), `m` is
  // physically there to be folded in, and `m`'s local Raft state is empty.
  //
  // Ruling (formal-spec issue 03, "[t1 re-review] `canMeet` lost its physicality
  // conjunct", ruled 2026-08-19): the physicality conjunct is **restored**, as
  // `mNode.effective_id != None`. It is not a second admission gate competing with the
  // ruled precondition at cluster.md:185 — that row still names exactly one gate, empty
  // Raft state, and this model still checks exactly that one. It is the *physical*
  // precondition of the transition existing at all: `CLUSTER MEET` is a handshake with a
  // running process at an address, and a node that has never booted has no process, no
  // listener, and (state-space row "This node's own id") not even an id to be registered
  // under. Without the conjunct the model admitted MEET against a node that had never
  // started — a transition the deployment has no way to take, so its absence in the spec
  // is not permission. Earlier drafts instead gated on `mNode.intent`/`mNode.member_of`,
  // which made `inv_meet_no_absorption` unfalsifiable (both were redundant with
  // `raft == Empty` on every path this model could otherwise reach, until `repurpose` made
  // them diverge); `effective_id` does not have that problem, because `repurpose` leaves a
  // running node running — the repurposed-node refusal below is still carried by
  // `raft == Empty` alone, and `meetRefusesRepurposedNodeTest` still forces it.
  pure def canMeet(nNode: NodeState, mNode: NodeState): bool = and {
    nNode.member_of != None,
    mNode.effective_id != None,
    mNode.raft == Empty,
  }

  // On acceptance `m` joins `n`'s cluster and gains real (non-empty) Raft state, but
  // does not itself become an externally-observable leader — it is a folded-in
  // member, not the group's leader (TR-CLUSTER-029's "no leadership claim" carries
  // through the fold). `raft_origin` is deliberately left untouched by the spread: a
  // legitimately-folded joiner always had `raft_origin == None` (guarded by
  // `canMeet`'s `raft == Empty`), so leaving it alone just keeps it `None`.
  pure def applyMeetJoiner(mNode: NodeState, nNode: NodeState): NodeState =
    { ...mNode, raft: nNode.raft, member_of: nNode.member_of }

  // TR-CLUSTER-035 (`CLUSTER RESET`): clears this node's membership/intent back to
  // "unclaimed", but — mirroring the real command's postcondition, which never
  // mentions wiping the node's own Raft log/vote state — leaves `raft` (and its
  // `raft_origin` provenance) exactly as they were. This is the hazard TR-CLUSTER-005's
  // amendment exists to repel: a node that looks unclaimed (`member_of == None`,
  // `intent == Join`) but still carries foreign/residual Raft state underneath.
  pure def canRepurpose(ns: NodeState): bool = and {
    ns.member_of != None,
    ns.raft != Empty,
  }

  //
  // Ruling (formal-spec issue 03, "[t1 re-review] `applyRepurpose` rewrites the write-once
  // `intent` field", ruled 2026-08-19): the rewrite **stands**. The write-once guarantee at
  // cluster.md:129 is scoped — "written once at first-boot mode decision; never rewritten
  // *by config changes*" — and `CLUSTER RESET` is an operator command, not a config change.
  // Rewriting the cell to `Join` is what leaves the node in the only state the reset's own
  // postcondition (TR-CLUSTER-035: membership reduced to just this node, slots and
  // migrations cleared) leaves it in: unclaimed, awaiting a MEET. The ruling is not taken on
  // trust: the scope of the guarantee it relies on — that no *config-driven* path rewrites
  // the cell — is the checkable half, carried by `defects.restartIntentWrite` (the machine's
  // `restart`, the one action that reads config, recomputes whether it changed `intent`) and
  // asserted by `inv_intent_write_once`.
  pure def applyRepurpose(ns: NodeState): NodeState =
    { ...ns, intent: Join, member_of: None, routable_leader: false }
    // raft, raft_origin, configured_bootstrap, decision_epoch, and the node's identity
    // (`effective_id`/`configured_id`) all persist — the residue this action models
    // leaving behind. TR-CLUSTER-035's HARD variant additionally renames the node
    // (`new_node_id`); that leg is not modeled here (see cluster_admission.qnt's header).
}
