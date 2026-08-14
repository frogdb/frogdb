// cluster_admission_logic — the pure logic of the admission-window model (formal-spec
// phase 2, task 1): every guard (`can*`) and every update (`apply*`), each taking the
// node state it reads as an explicit argument, so this module holds no `var` and is
// unit-testable in isolation.
//
// Rows whose ruled semantics are encoded here: TR-CLUSTER-028 (explicit bootstrap, with
// the persisted-intent amendment), TR-CLUSTER-029 (a joining node defers election),
// TR-CLUSTER-005 (the amended MEET join-safety gate) and TR-CLUSTER-035 (`CLUSTER RESET`
// as the residue hazard that gate exists to repel). Per-definition comments below carry
// the full ruling/amendment rationale; the model-wide header lives in
// cluster_admission.qnt.
module cluster_admission_logic {
  import cluster_admission_types.* from "./cluster_admission_types"

  // ---------------------------------------------------------------------------
  // Functional logic (guards + updates), pure and unit-testable in isolation.
  // ---------------------------------------------------------------------------

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
  pure def applyBoot(ns: NodeState, selfId: NodeId, cb: bool): NodeState =
    if (cb)
      { ...ns, intent: Bootstrap, configured_bootstrap: cb, raft: Initialized(1),
        routable_leader: true, member_of: Some(selfId),
        decision_epoch: ns.decision_epoch + 1, raft_origin: Some(selfId) }
    else
      { ...ns, intent: Join, configured_bootstrap: cb, raft: Empty,
        routable_leader: false, member_of: None,
        decision_epoch: ns.decision_epoch + 1 }
        // raft_origin deliberately untouched — stays None, this node has never
        // independently originated Raft state.

  // TR-CLUSTER-028/029 amendment (issue 25, "persist bootstrap intent"): once a mode
  // is decided, a restart never re-derives it — it only observes the current
  // (possibly since-flipped) config, not use it to redecide the mode.
  pure def canRestart(ns: NodeState): bool = ns.intent != Undecided

  pure def applyRestart(ns: NodeState, cb: bool): NodeState =
    { ...ns, configured_bootstrap: cb }
    // intent, raft, routable_leader, member_of all persist unchanged — this is the
    // determinism claim itself: nothing here reads `cb` to decide anything.

  // TR-CLUSTER-005, amended (issue 25): `n` folds `m` in only if `n` already belongs
  // to a real cluster (structural — `n` needs a cluster to fold `m` into) and `m`'s
  // local Raft state is empty. The ruled precondition at cluster.md:185 names exactly
  // one gate — empty Raft state — so that is the only per-`m` check here; earlier
  // drafts of this model also gated on `mNode.intent`/`mNode.member_of`, which made
  // `inv_meet_no_absorption` unfalsifiable (those conjuncts were redundant with
  // `raft == Empty` on every path this model could otherwise reach, until `repurpose`
  // below made them diverge) and additionally made this guard reject nothing that
  // `raft == Empty` alone didn't already reject.
  pure def canMeet(nNode: NodeState, mNode: NodeState): bool = and {
    nNode.member_of != None,
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

  pure def applyRepurpose(ns: NodeState): NodeState =
    { ...ns, intent: Join, member_of: None, routable_leader: false }
    // raft, raft_origin, configured_bootstrap, decision_epoch all persist — the
    // residue this action models leaving behind.
}
