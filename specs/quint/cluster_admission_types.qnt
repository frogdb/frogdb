// cluster_admission_types — types and constants for the admission-window model
// (formal-spec phase 2, task 1). The state-shape half of the model: the persisted
// bootstrap-vs-join `Intent` behind TR-CLUSTER-028/029, the abstract `RaftState` whose
// emptiness gates TR-CLUSTER-005's amended MEET refusal, the per-boot node identity of
// state-space row "This node's own id", and the per-node record the TR-CLUSTER-035 reset
// hazard leaves residue in.
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
    // Identity, per state-space row "This node's own id" (cluster.md:116) — the id a node
    // runs under is *per-boot*, not a fixed property of the node (see the node-id
    // instability section of cluster_admission.qnt's header).
    //   `effective_id` — what `ClusterConfigSection::effective_node_id` returned at this
    //     boot. `None` before the node has ever booted: an id exists only while a process
    //     does, which is also the physicality fact `canMeet` gates on.
    //   `configured_id` — the `cluster-node-id` knob. `Some(c)` is the "explicitly
    //     configured non-zero" case the row says survives every boot unchanged; `None` is
    //     the default (0), where a fresh id is minted per boot instead.
    effective_id: Option[NodeId],
    configured_id: Option[NodeId],
    // Ghost/history fields — see the header's "Hardened after review" note.
    decision_epoch: int,
    // How many times this node has minted a *fresh* id (i.e. booted unconfigured). Ghost:
    // no guard reads it. `>= 2` is the id-churn observable — the same durable node running
    // under a second, unrelated id.
    id_mints: int,
    raft_origin: Option[ClusterId],
  }

  // This model's node universe — a bounded-checking sizing knob, deliberately NOT shared
  // with the migration model even though the two agree on 4 today (see
  // cluster_common_types.qnt's "does not belong here" note).
  //
  // A member of `NODES` is the *durable node-local record* — the data directory a node
  // boots from, which is what the persisted cells (bootstrap-vs-join intent, the Raft log)
  // ride on. It is deliberately NOT the node's cluster-visible id: that lives in
  // `effective_id` above and may differ at every boot.
  pure val NODES: Set[NodeId] = Set(1, 2, 3, 4)

  // Where freshly-minted ids start. Above every member of `NODES` so a minted id can never
  // collide with the id an operator pins at a node's first boot, and monotone in the
  // machine (`next_id`) so two mints never collide either. That deliberately abstracts away
  // issue 35's *entropy* defect (16 random bits per millisecond, so simultaneous boots do
  // collide in the real minting function) — see cluster_admission.qnt's header for why that
  // defect is out of this model's scope.
  pure val FRESH_ID_BASE: NodeId = 101
}
