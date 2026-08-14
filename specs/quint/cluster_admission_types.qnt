// cluster_admission_types — types and constants for the admission-window model
// (formal-spec phase 2, task 1). The state-shape half of the model: the persisted
// bootstrap-vs-join `Intent` behind TR-CLUSTER-028/029, the abstract `RaftState` whose
// emptiness gates TR-CLUSTER-005's amended MEET refusal, and the per-node record the
// TR-CLUSTER-035 reset hazard leaves residue in.
//
// No state variables and no logic live here — see cluster_admission.qnt (the runnable
// main model) for the full "Rows modeled" header and the four-file layout note.
module cluster_admission_types {
  type NodeId = int
  type ClusterId = int

  // Local Option — see the deviations note above for why this isn't basicSpells'.
  type Option[a] = Some(a) | None

  // The persisted, node-local bootstrap-vs-join decision (state-space row
  // "Bootstrap-vs-join intent"): written once at the first mode decision, never
  // rewritten by a later config change (TR-CLUSTER-028/029, amended by issue 25).
  type Intent = Undecided | Bootstrap | Join

  // Abstract Raft state: Empty means "never initialized, never folded into a group";
  // Initialized(term) means "has Raft log/vote state" — the exact fact TR-CLUSTER-005's
  // amendment gates MEET on. The term is opaque; only emptiness is load-bearing here.
  type RaftState = Empty | Initialized(int)

  type NodeState = {
    intent: Intent,
    configured_bootstrap: bool,
    raft: RaftState,
    routable_leader: bool,
    member_of: Option[ClusterId],
    // Ghost/history fields — see the header's "Hardened after review" note.
    decision_epoch: int,
    raft_origin: Option[ClusterId],
  }

  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)
}
