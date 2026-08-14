// cluster_admission_machine — the state machine of the admission-window model
// (formal-spec phase 2, task 1): the `nodes` state variable (including the
// `decision_epoch`/`raft_origin` ghost fields the invariants are made falsifiable by) and
// one guarded action per modeled transition — `boot` (TR-CLUSTER-028/029), `restart`
// (their persisted-intent amendment), `meet` (TR-CLUSTER-005) and `repurpose`
// (TR-CLUSTER-035) — plus `init` and the `step` relation the simulator samples.
//
// Guards and updates themselves live in cluster_admission_logic.qnt; the model-wide
// header (rows modeled, deviations, hardening notes) lives in cluster_admission.qnt.
module cluster_admission_machine {
  import cluster_admission_types.* from "./cluster_admission_types"
  import cluster_admission_logic.* from "./cluster_admission_logic"

  var nodes: NodeId -> NodeState

  // ---------------------------------------------------------------------------
  // State machine (guarded actions).
  // ---------------------------------------------------------------------------

  action init: bool = all {
    nodes' = NODES.mapBy(_ => {
      intent: Undecided, configured_bootstrap: false, raft: Empty,
      routable_leader: false, member_of: None,
      decision_epoch: 0, raft_origin: None,
    }),
  }

  // Deterministic variant used by tests: `cb` is given directly rather than chosen by
  // `boot`'s internal nondet pick, so a test can force a specific bootstrap-vs-join
  // outcome.
  action bootAs(n: NodeId, cb: bool): bool = {
    val ns = nodes.get(n)
    all {
      canBoot(ns),
      nodes' = nodes.set(n, applyBoot(ns, n, cb)),
    }
  }

  // TR-CLUSTER-028/029 — first boot: cb stands for "what this node's config says right
  // now," read exactly once, at the moment the persisted intent is decided.
  action boot(n: NodeId): bool = {
    nondet cb = Set(true, false).oneOf()
    bootAs(n, cb)
  }

  action restartAs(n: NodeId, cb: bool): bool = {
    val ns = nodes.get(n)
    all {
      canRestart(ns),
      nodes' = nodes.set(n, applyRestart(ns, cb)),
    }
  }

  // TR-CLUSTER-028/029 amendment — non-first boot: `cb` models a config value that may
  // have changed (an operator flip) since the persisted intent was written; the
  // model must allow `cb` to disagree with `intent` and have that disagreement be a
  // no-op for behavior (inv_restart_deterministic checks exactly this).
  action restart(n: NodeId): bool = {
    nondet cb = Set(true, false).oneOf()
    restartAs(n, cb)
  }

  // TR-CLUSTER-005 — `CLUSTER MEET` folding `m` into `n`'s cluster.
  action meet(n: NodeId, m: NodeId): bool = {
    val nNode = nodes.get(n)
    val mNode = nodes.get(m)
    all {
      n != m,
      canMeet(nNode, mNode),
      nodes' = nodes.set(m, applyMeetJoiner(mNode, nNode)),
    }
  }

  // TR-CLUSTER-035 — an operator resetting `n`'s membership/intent without wiping
  // its local Raft state (see `canRepurpose`/`applyRepurpose` above).
  action repurpose(n: NodeId): bool = {
    val ns = nodes.get(n)
    all {
      canRepurpose(ns),
      nodes' = nodes.set(n, applyRepurpose(ns)),
    }
  }

  action stutter: bool = all {
    nodes' = nodes,
  }

  action step: bool = {
    nondet n = NODES.oneOf()
    nondet m = NODES.oneOf()
    any {
      boot(n),
      restart(n),
      meet(n, m),
      repurpose(n),
      stutter,
    }
  }
}
