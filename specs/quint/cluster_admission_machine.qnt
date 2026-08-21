// cluster_admission_machine — the state machine of the admission-window model
// (formal-spec phase 2, task 1): the `nodes` state variable (including the
// `decision_epoch`/`id_mints`/`raft_origin` ghost fields the invariants are made
// falsifiable by), the `next_id` mint source behind state-space row "This node's own id",
// the `defects` ghost record, and one guarded action per modeled transition — `boot`
// (TR-CLUSTER-028/029), `restart` (their persisted-intent amendment, plus the per-boot id
// re-derivation), `meet` (TR-CLUSTER-005) and `repurpose` (TR-CLUSTER-035) — plus `init`
// and the `step` relation the simulator samples.
//
// Guards and updates themselves live in cluster_admission_logic.qnt; the model-wide
// header (rows modeled, deviations, hardening notes) lives in cluster_admission.qnt.
//
// Single responsibility: the wiring — which variables each transition writes, and which
// nondeterministic choices `step` offers.
//   Belongs here:     `var` declarations, `init`, one `action` per modeled transition,
//                     `stutter`, `step`, and the deterministic `*As` variants tests drive.
//   Does NOT belong:  guard/update logic (cluster_admission_logic.qnt) — an action here
//                     should read as "check `can*`, write `apply*`"; invariants, witnesses
//                     and `run` tests (cluster_admission.qnt); types and constants
//                     (cluster_admission_types.qnt, cluster_common_types.qnt).
module cluster_admission_machine {
  import cluster_common_types.* from "./cluster_common_types"
  import cluster_admission_types.* from "./cluster_admission_types"
  import cluster_admission_logic.* from "./cluster_admission_logic"
  // The shared helper library (formal-spec issue 01) — `import M.*` is not transitive, so
  // this module imports the ghost latch it uses for itself.
  import lib_monotone.* from "./lib_monotone"

  // Keyed by the *durable node-local record* (the data directory), not by the node's
  // cluster-visible id — that id is `NodeState.effective_id` and may be a different number
  // at every boot (state-space row "This node's own id"). Keying the map by what actually
  // persists is what lets the persisted cells (intent, Raft state) survive an id change.
  var nodes: NodeId -> NodeState

  // The mint source for fresh (unconfigured) ids: strictly increasing, so no two mints
  // collide and no mint collides with a pinned id. A model device, not a modeled cell —
  // the real `effective_node_id` mints `(timestamp << 16) | random_bits`, whose entropy
  // defect is deliberately out of scope (see cluster_admission.qnt's header).
  var next_id: NodeId

  // Ghost flags, set when an action took the *unsafe* variant of a ruled precondition.
  // Never read by a guard; only by invariants. Each is recomputed inside the action that
  // could take the unsafe variant, from that action's own outcome rather than from the
  // guard, so weakening the update shows up here. (Same convention as the migration
  // model's `defects` record.)
  //
  // Mutating a recompute itself (blanking it to `false`, or dropping one of its disjuncts)
  // is undetectable by construction, and deliberately so: these are *detectors*, and in the
  // ruled model neither ever trips, so removing a detector removes nothing observable. They
  // earn their keep by what they catch in the mutated model — rows A23 (config-driven
  // `intent` rewrite) and A24 (identity remint) of
  // .scratch/formal-spec/2026-08-19-admission-battery.md, where each fires as designed. The
  // battery records the corresponding self-mutations (A48, A49) as MISSED with this
  // analysis rather than papering over them; killing one needs a compound mutation
  // (detector + the defect it detects), which is outside the one-edit battery discipline.
  var defects: {
    // cluster.md:129 / issue-03 repurpose ruling — `restart`, the one action that reads
    // config (and, since the node-id extension, the one that can see a brand-new id),
    // wrote the write-once `intent` cell.
    restartIntentWrite: bool,
    // Issue 35 — `restart` re-pointed the node's persisted cluster identity
    // (`member_of`/`raft_origin`) at its new boot-time id instead of leaving the durable
    // record's own claim alone.
    restartIdentityRemint: bool,
  }

  // ---------------------------------------------------------------------------
  // State machine (guarded actions).
  // ---------------------------------------------------------------------------

  action keepIds: bool = next_id' = next_id
  action keepDefects: bool = defects' = defects

  action init: bool = all {
    nodes' = NODES.mapBy(_ => {
      intent: Undecided, configured_bootstrap: false, raft: Empty,
      routable_leader: false, member_of: None,
      // No id before the first boot: an id exists only while a process does.
      effective_id: None, configured_id: None,
      decision_epoch: 0, id_mints: 0, raft_origin: None,
    }),
    next_id' = FRESH_ID_BASE,
    defects' = { restartIntentWrite: false, restartIdentityRemint: false },
  }

  // Deterministic variant used by tests: `cb` and `pin` are given directly rather than
  // chosen by `boot`'s internal nondet picks, so a test can force a specific
  // bootstrap-vs-join outcome and a specific identity regime.
  action bootAs(n: NodeId, cb: bool, pin: bool): bool = {
    val ns = nodes.get(n)
    val cfg = configuredIdFor(ns, n, pin)
    all {
      canBoot(ns),
      nodes' = nodes.set(n, applyBoot(ns, idForBoot(cfg, next_id), cfg, cb)),
      next_id' = if (mintsFreshId(cfg)) next_id + 1 else next_id,
      keepDefects,
    }
  }

  // TR-CLUSTER-028/029 — first boot: cb stands for "what this node's config says right
  // now," read exactly once, at the moment the persisted intent is decided. `pin` is the
  // other config read of a boot — whether `cluster-node-id` is set (state-space row "This
  // node's own id"); left unset, this boot mints a fresh id.
  action boot(n: NodeId): bool = {
    nondet cb = Set(true, false).oneOf()
    nondet pin = Set(true, false).oneOf()
    bootAs(n, cb, pin)
  }

  action restartAs(n: NodeId, cb: bool, pin: bool): bool = {
    val ns = nodes.get(n)
    val cfg = configuredIdFor(ns, n, pin)
    val rebooted = applyRestart(ns, cb, idForBoot(cfg, next_id), cfg)
    all {
      canRestart(ns),
      nodes' = nodes.set(n, rebooted),
      next_id' = if (mintsFreshId(cfg)) next_id + 1 else next_id,
      defects' = { ...defects,
        restartIntentWrite: latch(defects.restartIntentWrite, rebooted.intent != ns.intent),
        restartIdentityRemint: latch(defects.restartIdentityRemint,
          rebooted.member_of != ns.member_of or rebooted.raft_origin != ns.raft_origin),
      },
    }
  }

  // TR-CLUSTER-028/029 amendment — non-first boot: `cb` models a config value that may
  // have changed (an operator flip) since the persisted intent was written; the
  // model must allow `cb` to disagree with `intent` and have that disagreement be a
  // no-op for behavior (inv_restart_deterministic checks exactly this). `pin` likewise
  // may have changed since the last boot — an operator adding the `cluster-node-id` knob
  // (pins this node's identity from here on) or removing it (fresh id every boot again).
  action restart(n: NodeId): bool = {
    nondet cb = Set(true, false).oneOf()
    nondet pin = Set(true, false).oneOf()
    restartAs(n, cb, pin)
  }

  // TR-CLUSTER-005 — `CLUSTER MEET` folding `m` into `n`'s cluster.
  action meet(n: NodeId, m: NodeId): bool = {
    val nNode = nodes.get(n)
    val mNode = nodes.get(m)
    all {
      n != m,
      canMeet(nNode, mNode),
      nodes' = nodes.set(m, applyMeetJoiner(mNode, nNode)),
      keepIds,
      keepDefects,
    }
  }

  // TR-CLUSTER-035 — an operator resetting `n`'s membership/intent without wiping
  // its local Raft state (see `canRepurpose`/`applyRepurpose` in
  // cluster_admission_logic.qnt).
  action repurpose(n: NodeId): bool = {
    val ns = nodes.get(n)
    all {
      canRepurpose(ns),
      nodes' = nodes.set(n, applyRepurpose(ns)),
      keepIds,
      keepDefects,
    }
  }

  action stutter: bool = all {
    nodes' = nodes,
    keepIds,
    keepDefects,
  }

  // Unwiring an action from `step` (rows A57/A58 of
  // .scratch/formal-spec/2026-08-19-admission-battery.md) is invisible to both automated
  // oracles: the `run` tests call the actions directly, so they stay green, and every
  // `inv_*` is a state predicate over reachable states — dropping a verb only *shrinks* the
  // reachable set, which no safety invariant can be falsified by. The observable is
  // reachability: `quint run --witnesses` drops the corresponding witness to 0 traces
  // (`witnessRepurposedHazard` for `repurpose`, `witnessJoinedViaMeet` for `meet`,
  // confirmed at 4000x40). That lane is run by hand today — `just quint-run` checks the
  // invariants only — so this is a documented detection hole, not a closed one; closing it
  // means either gating witness counts in the runner or a temporal operator the simulator
  // does not have (same structural limit as the migration model's row M22).
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
