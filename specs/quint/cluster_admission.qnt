// cluster_admission — the admission-window design model (formal-spec phase 2, task 1).
//
// Models the issue-25 defect class: a freshly-started node solo-bootstraps into an
// externally routable "leader" while it was actually meant to join, and the ruled
// (target) semantics that close it. Modeled from specs/cluster.md's RULED rows, not
// today's code (server/src/server/cluster_init.rs still computes `should_bootstrap`
// fresh every boot, which is exactly the pre-ruling behavior this model excludes).
//
// Rows modeled:
//   TR-CLUSTER-028 — explicit fresh-deployment bootstrap: a node only initializes its
//     own Raft group and becomes an externally-routable leader when explicitly
//     configured to bootstrap, and persists that decision so a restart mid-bootstrap
//     never re-decides it.
//   TR-CLUSTER-029 — a joining node defers raft.initialize/election entirely until
//     `CLUSTER MEET` folds it into a real cluster; it must never self-elect into an
//     externally routable "leader" of a solo one-node group.
//   TR-CLUSTER-030 — bootstrap slot seeding: the companion transition that gives a
//     freshly bootstrapped node's cluster real slot ownership immediately after
//     TR-CLUSTER-028. Slot assignment itself is outside this model's state (it does
//     not affect the admission-window invariants below); cited because it is part of
//     why a bootstrap node's `member_of` is a real, independently-owned cluster and
//     not just a leadership flag.
//   TR-CLUSTER-005 — `CLUSTER MEET` -> `AddNode`, amended (issue 25): a MEET is
//     refused unless the folded-in node's local Raft state is empty, mirroring
//     etcd's "wipe the data dir first" rule — a node that was ever a member of
//     another cluster cannot be absorbed. The ruled precondition (cluster.md:185)
//     names exactly one gate — empty Raft state — so `canMeet` below checks only
//     that (plus the structural fact that the inviting node already has a
//     cluster to fold into); it does not re-derive the gate from `intent`.
//   TR-CLUSTER-035 — `CLUSTER RESET SOFT/HARD`: clears "Node membership"/"Node
//     role"/"Node parent pointer" for this node but its postcondition never
//     mentions wiping this node's own Raft log/vote state. Modeled as
//     `repurpose`: the hazard TR-CLUSTER-005's amendment exists to repel — an
//     operator resets a node's membership/intent without wiping its Raft data
//     dir, so `member_of`/`intent` go back to "unclaimed" while `raft` (and its
//     provenance, `raft_origin`) stay behind as residue.
//   State-space rows "Bootstrap-vs-join intent" (the persisted, node-local,
//     write-once-at-first-decision record) and "This node's own id" (why
//     `member_of`/cluster identity for a bootstrap node is keyed on its own id).
//
// Issue: .scratch/cluster-correctness/issues/open/25-newly-started-node-briefly-usurps-leadership-via-solo-bootstrap.md
// (Ruling + Amendment sections — persisted intent, refuse-MEET-nonempty-state).
//
// Deliberately not modeled (out of scope for this invariant set, per "don't force it"):
// FM-CLUSTER-101 (voter-set removal) and INV-EPOCH-1/2 (config-epoch arbitration) —
// neither the voter-set claim nor epochs enter this model.
//
// Assumption: `NodeId` is a stable model parameter, so `member_of: Some(selfId)` and
// every other `member_of`/`raft_origin` comparison implicitly assumes a node's id is
// itself stable across `restart`. The state-space row "This node's own id"
// (cluster.md:116) says the opposite: id is re-derived at every boot
// (`ClusterConfigSection::effective_node_id`) and only stays fixed if
// `cluster-node-id` is explicitly configured non-zero — left at the default, a fresh
// id is minted on every boot. This model does not represent that instability; it
// assumes the configured-non-zero case.
//
// Deviations from the brief's literal field notation, both purely syntactic (Quint has
// no built-in Option/None; see quint-lang's Basic spells section):
//   - `Intent`'s "undecided" case is named `Undecided`, not the brief's bare `None` —
//     `None` is reserved below for `Option`'s own constructor, and Quint sum-type
//     constructors share one flat namespace.
//   - `member_of: Option[ClusterId]` is a small local `Option` sum type (`Some`/`None`)
//     defined by this model (in cluster_admission_types.qnt) rather than imported from
//     `basicSpells.qnt`, so the model depends on no external spells library.
//
// Hardened after adversarial (mutation-tested) review — two ghost/history fields were
// added, both beyond the brief's literal field list, because the four required
// invariants were unfalsifiable without them (see git history for the pre-hardening
// version and the review that forced this):
//   - `decision_epoch: int` — bumped by `applyBoot` alone (both branches), never by
//     `applyRestart`. `canBoot` only ever admits one legitimate call per node
//     (`intent == Undecided`, permanently false afterward), so in the ruled model
//     `decision_epoch` never exceeds 1; `inv_restart_deterministic` asserts this,
//     which is exactly the "mode is decided at most once, ever" claim TR-CLUSTER-028's
//     persisted-intent amendment makes. Catches a `restart` that re-derives the mode
//     by reusing `applyBoot`'s logic even when it also flips `intent` in the process
//     (a plain intent-based invariant can't see that: post-mutation the node just looks
//     like a node that decided Bootstrap on its first boot).
//   - `raft_origin: Option[ClusterId]` — set exactly once, by `applyBoot`'s Bootstrap
//     branch, to this node's own id; never written again by any other action
//     (`applyMeetJoiner` and `applyRestart` both leave it untouched via record spread).
//     It is the provenance fingerprint TR-CLUSTER-005's amendment is really protecting:
//     `inv_meet_no_absorption` requires that whenever a node claims membership in a
//     cluster, any raft-origin fingerprint it carries must name that same cluster —
//     catching a `meet` that (via a weakened guard) folds in a node whose Raft state
//     independently originated elsewhere, even after `repurpose` has erased the
//     `intent`/`member_of` evidence of that origin.
//
// Model layout (this model is split across four files under specs/quint/; the split is
// purely structural — same state space, same actions, same invariants, same tests as
// the single-file original):
//   cluster_admission_types.qnt   — types + constants, no state
//   cluster_admission_logic.qnt   — pure guards/updates over explicit state arguments
//                                   (unit-testable in isolation)
//   cluster_admission_machine.qnt — the `nodes` variable + `init`/actions/`step`
//   cluster_admission.qnt         — THIS FILE, the runnable main model: witnesses,
//                                   `val inv_*` invariants, `run` tests
// The satellite modules are imported with plain `import M.* from "./M"`, which shares one
// instance of the machine's state variables (it does not instantiate a copy the way a
// const-parameterised `import M(c = ...)` would). `just quint-check` typechecks every
// file; `just quint-run`/`quint-verify-*` only drive the main models (scripts/quint-models.sh
// derives that set from "declares at least one `val inv_*`").
module cluster_admission {
  import cluster_admission_types.* from "./cluster_admission_types"
  import cluster_admission_machine.* from "./cluster_admission_machine"

  // ---------------------------------------------------------------------------
  // Witnesses — is each shape of the admission window actually reached?
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-028: some node has explicitly bootstrapped and is a routable leader.
  val witnessBootstrapped: bool =
    NODES.exists(n => nodes.get(n).intent == Bootstrap and nodes.get(n).routable_leader)

  // TR-CLUSTER-005/029: some joining node has been folded into a real cluster.
  val witnessJoinedViaMeet: bool =
    NODES.exists(n => nodes.get(n).intent == Join and nodes.get(n).member_of != None)

  // TR-CLUSTER-035: some node has been repurposed (unclaimed on the surface, but
  // still carrying residual Raft state) — proves `meetRefusesRepurposedNodeTest`'s
  // scenario is reachable at all, not vacuous.
  val witnessRepurposedHazard: bool =
    NODES.exists(n => {
      val ns = nodes.get(n)
      ns.member_of == None and ns.raft != Empty
    })

  // ---------------------------------------------------------------------------
  // Invariants — the ruled admission-window safety properties.
  // ---------------------------------------------------------------------------

  // TR-CLUSTER-029: a joining node must never be an externally routable leader, full
  // stop — `applyMeetJoiner` deliberately never grants leadership to the folded-in
  // node either, so this holds even after a legitimate `meet`. (An earlier draft
  // additionally allowed the exemption "unless `member_of != None`", which made this
  // invariant unfalsifiable: a mutant that made a Join-intent node self-elect *and*
  // claim `member_of: Some(selfId)` — i.e. issue 25 itself — satisfied it vacuously.)
  val inv_no_usurper: bool =
    NODES.forall(n => {
      val ns = nodes.get(n)
      ns.intent == Join implies not(ns.routable_leader)
    })

  // TR-CLUSTER-028: two nodes routable-leader at once can only be leaders of distinct,
  // never-merged clusters (no split-brain leadership inside one deployment). Each
  // bootstrap mints a cluster keyed on its own id (state-space row "This node's own
  // id"), so two distinct bootstrap leaders are always distinct clusters by
  // construction; this invariant is the check that no action collapses that.
  val inv_single_routable_group: bool =
    NODES.forall(n1 => NODES.forall(n2 => {
      val a = nodes.get(n1)
      val b = nodes.get(n2)
      (n1 != n2 and a.routable_leader and b.routable_leader) implies a.member_of != b.member_of
    }))

  // TR-CLUSTER-028/029 amendment: after `restart`, a node's operative mode is exactly
  // its persisted `intent`, never re-derived from the current `configured_bootstrap` —
  // checked as the two behavioral consequences that a config-driven re-decision would
  // violate: a Bootstrap-intent node's Raft state, once initialized, never reverts to
  // empty; a Join-intent node not yet folded in never becomes a routable leader — in
  // every reachable state, regardless of how `configured_bootstrap` has since drifted.
  // The third conjunct, `decision_epoch <= 1`, is the direct formalization of "decided
  // at most once, ever": `applyBoot` is the only place `decision_epoch` is bumped, and
  // it is only ever legitimately reachable once per node (`canBoot` requires
  // `intent == Undecided`, which becomes permanently false after the first boot). A
  // `restart` that re-derives the mode by reusing `applyBoot`'s update — even one that
  // also updates `intent` in the process, which the other two conjuncts alone cannot
  // see through — pushes this past 1.
  val inv_restart_deterministic: bool =
    NODES.forall(n => {
      val ns = nodes.get(n)
      and {
        ns.intent == Bootstrap implies ns.raft != Empty,
        (ns.intent == Join and ns.member_of == None) implies not(ns.routable_leader),
        ns.decision_epoch <= 1,
      }
    })

  // TR-CLUSTER-005 amendment: `meet` never folds in a node whose Raft state carries a
  // foreign provenance — the foreign-state-absorption refusal. `raft_origin` is set
  // exactly once, at this node's own bootstrap, and is never reassigned by any other
  // action (see the header's "Hardened after review" note), so whenever a node claims
  // membership in a cluster while also carrying a raft-origin fingerprint, that
  // fingerprint must name the very cluster it claims — otherwise its Raft state
  // originated somewhere else and `meet` absorbed it in violation of the amendment.
  // (An earlier draft keyed this off `intent == Bootstrap` instead, which `repurpose`
  // makes unsound: `repurpose` clears `intent` back to `Join` while leaving `raft` and
  // `raft_origin` as residue, so an intent-based marker goes blind exactly where the
  // amendment needs to see.)
  val inv_meet_no_absorption: bool =
    NODES.forall(n => {
      val ns = nodes.get(n)
      (ns.member_of != None and ns.raft_origin != None) implies (ns.raft_origin == ns.member_of)
    })

  // ---------------------------------------------------------------------------
  // Tests.
  // ---------------------------------------------------------------------------

  // Happy trajectory: TR-CLUSTER-028 bootstraps n1, TR-CLUSTER-029 defers n2's
  // election, TR-CLUSTER-005 folds n2 into n1's cluster via MEET.
  run bootstrapThenMeetTest: bool = {
    init
      .then(bootAs(1, true))
      .expect(
        val n1 = nodes.get(1)
        all { n1.routable_leader, n1.raft != Empty, n1.member_of == Some(1) }
      )
      .then(bootAs(2, false))
      .expect(
        val n2 = nodes.get(2)
        all { not(n2.routable_leader), n2.raft == Empty, n2.member_of == None }
      )
      .then(meet(1, 2))
      .expect(
        val n1 = nodes.get(1)
        val n2 = nodes.get(2)
        all {
          n2.member_of == n1.member_of,
          not(n2.routable_leader),
          n2.raft != Empty,
          n2.intent == Join,
        }
      )
  }

  // The issue-25 usurpation, asserted unreachable under ruled semantics: a joining
  // node whose operator flips `configured_bootstrap` to true and restarts does not
  // solo-bootstrap (the pre-ruling bug — `should_bootstrap` recomputed every boot —
  // would have); nor can the node re-decide its mode at all once persisted, since
  // `boot`/`bootAs` is a first-boot-only action.
  run issue25UsurpationUnreachableTest: bool = {
    init
      .then(bootAs(2, false))
      .expect(nodes.get(2).intent == Join and not(nodes.get(2).routable_leader))
      .then(restartAs(2, true))
      .expect(
        val n2 = nodes.get(2)
        all { n2.intent == Join, not(n2.routable_leader), n2.raft == Empty }
      )
      .then(all {
        assert(inv_no_usurper),
        assert(inv_restart_deterministic),
        nodes' = nodes,
      })
      // `.fail()` is the terminal step of the trace (see tests.md): re-deciding the
      // mode once persisted is not a legal step at all.
      .then(bootAs(2, true).fail())
  }

  // TR-CLUSTER-005 amendment: MEET refuses a node that was ever a member of another
  // cluster — here, one that independently bootstrapped its own separate deployment —
  // rather than absorbing its foreign Raft state.
  run meetRefusesForeignRaftStateTest: bool = {
    init
      .then(bootAs(1, true))
      .then(bootAs(3, true))
      .then(all {
        assert(nodes.get(3).member_of != nodes.get(1).member_of),
        assert(inv_meet_no_absorption),
        nodes' = nodes,
      })
      // `.fail()` is the terminal step: MEET refuses n3, whose Raft state is
      // non-empty from its own independent bootstrap.
      .then(meet(1, 3).fail())
  }

  // TR-CLUSTER-005/035: MEET refuses a node an operator has repurposed (reset back to
  // "unclaimed", TR-CLUSTER-035) but whose Raft state was never wiped — the residue
  // hazard the amendment targets, reachable only once `intent`/`member_of` no longer
  // reflect the node's true Raft provenance.
  run meetRefusesRepurposedNodeTest: bool = {
    init
      .then(bootAs(1, true))
      .then(bootAs(3, true))
      .then(repurpose(3))
      .expect(
        val n3 = nodes.get(3)
        all { n3.intent == Join, n3.member_of == None, n3.raft != Empty }
      )
      .then(all {
        assert(witnessRepurposedHazard),
        assert(inv_meet_no_absorption),
        nodes' = nodes,
      })
      // `.fail()` is the terminal step: MEET still refuses n3 — `canMeet` gates on
      // `raft == Empty` alone, which n3 fails regardless of its reset `intent`.
      .then(meet(1, 3).fail())
  }
}
