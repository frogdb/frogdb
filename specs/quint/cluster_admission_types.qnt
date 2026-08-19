// cluster_admission_types — types and constants for the admission-window model
// (formal-spec phase 2, task 1). The state-shape half of the model: the persisted
// bootstrap-vs-join `Intent` behind TR-CLUSTER-028/029, the abstract `RaftState` whose
// emptiness gates TR-CLUSTER-005's amended MEET refusal, and the per-node record the
// TR-CLUSTER-035 reset hazard leaves residue in.
//
// Single responsibility: the *shape* of this model's state and the constants that bound
// it — nothing that reads or writes state.
//   Belongs here:     type declarations, sizing constants (`NODES`).
//   Does NOT belong:  `var`s, guards, updates, invariants, witnesses, tests. Guards and
//                     updates live in cluster_admission_logic.qnt; the `var` and the
//                     actions in cluster_admission_machine.qnt; everything checkable in
//                     cluster_admission.qnt (the runnable main model, which also carries
//                     the full "Rows modeled" header and the layout note).
//   Shared with the migration model: `NodeId` and `Option` come from
//                     cluster_common_types.qnt. Anything below is admission-specific by
//                     construction — if a definition here ever becomes genuinely identical
//                     to a migration one, it moves to the common module rather than being
//                     copied.
module cluster_admission_types {
  import cluster_common_types.* from "./cluster_common_types"

  type ClusterId = int

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

  // This model's node universe — a bounded-checking sizing knob, deliberately NOT shared
  // with the migration model even though the two agree on 4 today (see
  // cluster_common_types.qnt's "does not belong here" note).
  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)
}
