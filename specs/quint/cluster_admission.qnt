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
// Rulings recorded on this model (formal-spec issue 03's admission items, ruled
// 2026-08-19; the rationale for each lives at the definition it governs, in
// cluster_admission_logic.qnt):
//   - `canMeet` physicality — RESTORED. A MEET is admissible only against a node that has
//     actually booted (`effective_id != None`), because `CLUSTER MEET` is a handshake with
//     a running process. This is the transition's physical precondition, not a second
//     admission gate: TR-CLUSTER-005's ruled precondition still names exactly one gate
//     (empty Raft state) and the model still checks exactly that one.
//     Forced by `meetRefusesUnbootedNodeTest`.
//   - `applyRepurpose` rewriting the write-once `intent` cell — STANDS. cluster.md:129
//     scopes the guarantee to "never rewritten *by config changes*", and `CLUSTER RESET`
//     (TR-CLUSTER-035) is an operator command, not a config change. The scope that ruling
//     leans on is itself checked rather than asserted: `inv_intent_write_once` fails if the
//     one config-reading action, `restart`, ever writes the cell.
//     Forced by `adminResetMayRewriteIntentTest`.
//
// Node-id instability (state-space row "This node's own id", cluster.md:116; issue 35
// class, admitted into this model by the 2026-08-19 Quint-completeness campaign ruling):
// an earlier version of this model *assumed* a node's id was stable across a restart, which
// is exactly what that row denies — the id is re-derived at every boot by
// `ClusterConfigSection::effective_node_id`, and only stays fixed when `cluster-node-id` is
// explicitly configured non-zero; left at the default, a fresh id is minted on every boot.
// The assumption is now gone, and the instability is modeled instead:
//   - `NODES` keys the state map by the **durable node-local record** (the data directory a
//     node boots from) rather than by the node's cluster-visible id. Everything the
//     admission window turns on — the persisted bootstrap-vs-join intent, the Raft
//     log/vote state, the membership claim and its `raft_origin` provenance — rides on that
//     record, which is what "keyed by what actually persists across the restart" means.
//   - `NodeState.effective_id` is the id this boot runs under (`None` before a node has
//     ever booted: an id exists only while a process does). `NodeState.configured_id` is
//     the `cluster-node-id` knob; unset, every boot draws a fresh id from the machine's
//     monotone `next_id` and bumps the `id_mints` ghost.
//   - The interaction that matters for THIS model's rows is that a fresh id must not
//     re-open a settled decision: pre-ruling, `should_bootstrap` is computed from the node
//     id (`initial_members.keys().next() == Some(node_id)`), so a re-minted id could flip
//     the bootstrap-vs-join answer at every boot — issue 25's usurpation, reached through
//     issue 35's churn. `inv_restart_deterministic` and `inv_intent_write_once` are the
//     checks; `restartMintsFreshIdTest` and `configuredIdStableAcrossRestartTest` are the
//     forcing traces.
// Deliberately still out of scope, both documented rather than modeled: issue 35's
// *entropy* defect (16 random bits per millisecond, so simultaneous boots can mint the
// same id) — the model's mint source is monotone, and `inv_effective_ids_distinct` asserts
// that device holds, because a colliding mint would falsify `inv_single_routable_group` by
// representation-level construction rather than by anything the admission window does; and
// TR-CLUSTER-035's HARD rename leg (`new_node_id`), an admin-driven identity change whose
// epoch-zeroing the same campaign ruling put out of scope.
//
// Deviations from the brief's literal field notation, both purely syntactic (Quint has
// no built-in Option/None; see quint-lang's Basic spells section):
//   - `Intent`'s "undecided" case is named `Undecided`, not the brief's bare `None` —
//     `None` is reserved below for `Option`'s own constructor, and Quint sum-type
//     constructors share one flat namespace.
//   - `member_of: Option[ClusterId]` is a small local `Option` sum type (`Some`/`None`)
//     declared in cluster_common_types.qnt — shared with the migration model, but still
//     model-local in the sense that matters: no dependency on `basicSpells.qnt` or any
//     other external spells library.
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
// Model layout (this model is split across four files under specs/quint/, over one shared
// vocabulary module; the split is purely structural — same state space, same actions, same
// invariants, same tests as the single-file original):
//   cluster_common_types.qnt      — `NodeId`/`Option`, shared with the migration model
//   cluster_admission_types.qnt   — this model's types + sizing constants, no state
//   cluster_admission_logic.qnt   — pure guards/updates over explicit state arguments
//                                   (unit-testable in isolation)
//   cluster_admission_machine.qnt — the `nodes` variable + `init`/actions/`step`
//   cluster_admission.qnt         — THIS FILE, the runnable main model: witnesses,
//                                   `val inv_*` invariants, `run` tests
// The satellite modules are imported with plain `import M.* from "./M"`, which shares one
// instance of the machine's state variables (it does not instantiate a copy the way a
// const-parameterised `import M(c = ...)` would). `import` is NOT transitive, so each file
// imports the vocabulary module it uses directly. `just quint-check` typechecks every
// file; `just quint-run`/`quint-verify-*` only drive the main models (scripts/quint-models.sh
// derives that set from "declares at least one `val inv_*`" — which is why the satellite
// modules must never declare one).
//
// Single responsibility of THIS file: everything a checker can *check*.
//   Belongs here:     witnesses (`val witness*`), invariants (`val inv_*`, grouped by
//                     family below), `run` tests, and the model-wide header above.
//   Does NOT belong:  `var`s or actions (cluster_admission_machine.qnt), guards/updates
//                     (cluster_admission_logic.qnt), types/constants (the two `*types`
//                     modules). Adding a new check should be one `val inv_*` under the
//                     matching family heading, and nothing else.
module cluster_admission {
  import cluster_common_types.* from "./cluster_common_types"
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

  // State-space row "This node's own id": some node has come back up under a second,
  // freshly-minted id — the unconfigured-`cluster-node-id` default. Backed by the
  // `id_mints` ghost, which only a fresh mint bumps, so this cannot be reached by a boot
  // that reused a configured id.
  val witnessFreshIdAfterRestart: bool =
    NODES.exists(n => nodes.get(n).id_mints >= 2)

  // The other identity regime: some booted node is running under an explicitly configured
  // id and has never minted one. Proves `configuredIdStableAcrossRestartTest`'s regime is
  // reachable in a sampled walk, not just by a scripted trace.
  val witnessConfiguredIdPinned: bool =
    NODES.exists(n => {
      val ns = nodes.get(n)
      ns.configured_id != None and ns.effective_id == ns.configured_id and ns.id_mints == 0
    })

  // Issue 35's orphan hazard, reachable: a node that minted its own cluster at bootstrap is
  // now running under a *different* id than the one that cluster is named after — the
  // "every restart joins as a stranger" state. Nothing in this model's invariant set forbids
  // it (it is a real consequence of the ruled-today state-space row); the checks are that it
  // does not let the node re-decide its mode, re-point its membership, or get re-absorbed.
  val witnessStrangerIdentity: bool =
    NODES.exists(n => {
      val ns = nodes.get(n)
      ns.raft_origin != None and ns.effective_id != ns.raft_origin
    })

  // ---------------------------------------------------------------------------
  // Invariants — the ruled admission-window safety properties, grouped by family so a new
  // check is one `val inv_*` under the heading it belongs to. `scripts/quint-invariants.sh`
  // discovers them by grepping this file for `val inv_*`, so the grouping is navigational
  // only — nothing enumerates the families.
  //
  //   Leadership / usurpation — who may claim to be an externally routable leader.
  //   Identity / restart      — what a restart may and may not re-derive, including the
  //                             per-boot node id of state-space row "This node's own id".
  //   Admission / absorption  — what a MEET may fold in.
  // ---------------------------------------------------------------------------

  // --- Leadership / usurpation -----------------------------------------------

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

  // --- Identity / restart ----------------------------------------------------

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

  // cluster.md:129 ("written once at first-boot mode decision; never rewritten by config
  // changes"), read with the issue-03 repurpose ruling in the header: the guarantee is
  // scoped to config-driven rewrites, so it is asserted where that scope bites — `restart`,
  // the one action that reads config (and, since the node-id extension, the one action that
  // can see a brand-new id). `defects.restartIntentWrite` is recomputed inside `restart`
  // from its own outcome, so a `restart` that re-derives the mode — from the since-flipped
  // `configured_bootstrap`, or from a freshly-minted id the way today's `should_bootstrap`
  // would — trips it however it is spelled. `applyRepurpose`'s admin-driven rewrite is
  // outside the scope by ruling, and does not trip it.
  val inv_intent_write_once: bool = not(defects.restartIntentWrite)

  // State-space row "This node's own id" x TR-CLUSTER-028: a node's persisted cluster
  // identity belongs to its durable record, not to the id it happens to be running under,
  // so a restart never re-points `member_of`/`raft_origin` at the new boot's id. Checked
  // via `defects.restartIdentityRemint`, recomputed inside `restart` from the outcome:
  // `inv_meet_no_absorption` catches a remint of one of the two cells (they stop agreeing),
  // but is blind to a restart that re-mints *both* in step — which is precisely the
  // "rejoin as a stranger, under a fresh cluster identity" shape issue 35 describes.
  val inv_identity_survives_id_churn: bool = not(defects.restartIdentityRemint)

  // State-space row "This node's own id", the configured half: `effective_node_id` returns
  // an explicitly-configured `cluster-node-id` unchanged at every boot. Falsified by a boot
  // that mints a fresh id anyway — the defect that would make the knob decorative.
  val inv_configured_id_stable: bool =
    NODES.forall(n => {
      val ns = nodes.get(n)
      match ns.configured_id { | Some(c) => ns.effective_id == Some(c) | None => true }
    })

  // The mint device's own soundness, asserted rather than assumed (see the header's
  // out-of-scope note): two distinct durable records never run under the same id at the
  // same time. This is what lets `inv_single_routable_group` keep meaning something once
  // ids churn — a cluster is named after the id that minted it, so id reuse would collapse
  // two independent clusters into one name for reasons that have nothing to do with the
  // admission window. Issue 35's real-world entropy defect lives outside this model; if a
  // later mint model admits collisions, this invariant is the thing that must move, not
  // quietly weaken.
  val inv_effective_ids_distinct: bool =
    NODES.forall(n1 => NODES.forall(n2 => {
      val a = nodes.get(n1)
      val b = nodes.get(n2)
      (n1 != n2 and a.effective_id != None) implies a.effective_id != b.effective_id
    }))

  // --- Admission / absorption ------------------------------------------------

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

  // A structural fact of this model, asserted rather than assumed: a node that claims
  // membership in a cluster always carries non-empty Raft state (`applyBoot`'s bootstrap
  // branch and `applyMeetJoiner` are the only two writers of `member_of != None`, and both
  // write a non-empty `raft` in the same step; `applyRepurpose` clears the claim without
  // touching `raft`).
  //
  // It is here as the tripwire for the mutation battery's two structurally-unobservable
  // rows (.scratch/formal-spec/2026-08-19-admission-battery.md, rows A40 and A50): dropping
  // `canRepurpose`'s `raft != Empty` conjunct, and dropping `meet`'s `n != m`, are both
  // undetectable *because* this holds — the first conjunct is implied by the one beside it,
  // and a self-MEET is disabled by `canMeet`'s surviving conjuncts contradicting each other
  // (`member_of != None` forces `raft != Empty`, which refuses `raft == Empty`). Asserting
  // the fact turns that honest-miss argument into a checked one: if a later change makes an
  // empty-Raft member reachable, this fails *before* the two redundant conjuncts silently
  // become load-bearing with nothing forcing them.
  val inv_member_carries_raft_state: bool =
    NODES.forall(n => {
      val ns = nodes.get(n)
      ns.member_of != None implies ns.raft != Empty
    })

  // ---------------------------------------------------------------------------
  // Tests.
  // ---------------------------------------------------------------------------

  // Happy trajectory: TR-CLUSTER-028 bootstraps n1, TR-CLUSTER-029 defers n2's
  // election, TR-CLUSTER-005 folds n2 into n1's cluster via MEET. Both nodes boot with
  // `cluster-node-id` unset (`pin == false`) — the default deployment shape — so each
  // cluster identity is named after a minted id rather than after the node's record key.
  run bootstrapThenMeetTest: bool = {
    init
      .then(bootAs(1, true, false))
      .expect(
        val n1 = nodes.get(1)
        all { n1.routable_leader, n1.raft != Empty, n1.member_of == n1.effective_id }
      )
      .then(bootAs(2, false, false))
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
          // Both shapes this trace exists to reach, asserted here so every witness in the
          // model has a deterministic forcing test (the other four are asserted below).
          witnessBootstrapped,
          witnessJoinedViaMeet,
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
      .then(bootAs(2, false, false))
      .expect(
        val n2 = nodes.get(2)
        // `decision_epoch == 1` pins the join branch's own bump (battery row A20): without
        // it, only the bootstrap branch's bump is forced by any test, and the "decided at
        // most once, ever" ghost would be half-dead — an upper-bound invariant cannot see a
        // branch that stops counting.
        all { n2.intent == Join, not(n2.routable_leader), n2.decision_epoch == 1 }
      )
      .then(restartAs(2, true, false))
      .expect(
        val n2 = nodes.get(2)
        // `configured_bootstrap` is the premise of this whole trace, asserted rather than
        // assumed (battery row A29): the flipped knob really did land in the node's state,
        // so `intent == Join` below is a *decision* that survived a contradicting config,
        // not a restart that quietly ignored the config read.
        all { n2.intent == Join, n2.configured_bootstrap, not(n2.routable_leader),
              n2.raft == Empty }
      )
      .then(all {
        assert(inv_no_usurper),
        assert(inv_restart_deterministic),
        assert(inv_intent_write_once),
        nodes' = nodes,
        keepIds,
        keepDefects,
      })
      // `.fail()` is the terminal step of the trace (see tests.md): re-deciding the
      // mode once persisted is not a legal step at all.
      .then(bootAs(2, true, false).fail())
  }

  // TR-CLUSTER-005 amendment: MEET refuses a node that was ever a member of another
  // cluster — here, one that independently bootstrapped its own separate deployment —
  // rather than absorbing its foreign Raft state.
  run meetRefusesForeignRaftStateTest: bool = {
    init
      .then(bootAs(1, true, false))
      .then(bootAs(3, true, false))
      .then(all {
        assert(nodes.get(3).member_of != nodes.get(1).member_of),
        assert(inv_meet_no_absorption),
        assert(inv_single_routable_group),
        assert(inv_effective_ids_distinct),
        nodes' = nodes,
        keepIds,
        keepDefects,
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
      .then(bootAs(1, true, false))
      .then(bootAs(3, true, false))
      .then(repurpose(3))
      .expect(
        val n3 = nodes.get(3)
        // The provenance fingerprint is part of the residue the reset leaves behind
        // (battery row A45): a reset that erased `raft_origin` while leaving `raft` would
        // leave `inv_meet_no_absorption` with nothing to recognize the foreign state by.
        all { n3.intent == Join, n3.member_of == None, n3.raft != Empty,
              n3.raft_origin == n3.effective_id }
      )
      .then(all {
        assert(witnessRepurposedHazard),
        assert(inv_meet_no_absorption),
        nodes' = nodes,
        keepIds,
        keepDefects,
      })
      // `.fail()` is the terminal step: MEET still refuses n3 — a repurposed node is
      // still a *running* node, so `canMeet`'s physicality conjunct is satisfied and the
      // refusal is carried by `raft == Empty` alone, regardless of its reset `intent`.
      .then(meet(1, 3).fail())
  }

  // TR-CLUSTER-005 physicality (issue-03 ruling, see the header): MEET is refused against
  // a node that has never booted, even though its Raft state is trivially empty — the one
  // gate the ruled precondition names would admit it, but there is no process to hand the
  // handshake to. This is the trace the restored conjunct exists for: without it, `meet`
  // folds a never-started node into the cluster.
  run meetRefusesUnbootedNodeTest: bool = {
    init
      .then(bootAs(1, true, false))
      .expect(
        val n4 = nodes.get(4)
        all { n4.effective_id == None, n4.raft == Empty, n4.intent == Undecided }
      )
      // `.fail()` is the terminal step: n4 has never booted.
      .then(meet(1, 4).fail())
  }

  // State-space row "This node's own id" (issue 35 class): a node left at the default
  // `cluster-node-id` comes back from a restart under a brand-new id, and none of the
  // persisted admission state moves with it — the decision it already made, the cluster it
  // already minted and that cluster's provenance all stay keyed to its durable record.
  run restartMintsFreshIdTest: bool = {
    init
      .then(bootAs(1, true, false))
      .expect(
        val n1 = nodes.get(1)
        all { n1.effective_id == Some(FRESH_ID_BASE), n1.id_mints == 1,
              n1.configured_id == None, n1.member_of == Some(FRESH_ID_BASE),
              n1.raft_origin == Some(FRESH_ID_BASE) }
      )
      // Restart with the bootstrap knob since flipped off *and* a fresh id: neither may
      // re-decide anything.
      .then(restartAs(1, false, false))
      .expect(
        val n1 = nodes.get(1)
        all {
          n1.effective_id == Some(FRESH_ID_BASE + 1),
          n1.id_mints == 2,
          n1.intent == Bootstrap,
          n1.decision_epoch == 1,
          n1.routable_leader,
          n1.member_of == Some(FRESH_ID_BASE),
          n1.raft_origin == Some(FRESH_ID_BASE),
        }
      )
      .then(all {
        assert(witnessFreshIdAfterRestart),
        assert(witnessStrangerIdentity),
        assert(inv_restart_deterministic),
        assert(inv_intent_write_once),
        assert(inv_identity_survives_id_churn),
        assert(inv_meet_no_absorption),
        assert(inv_effective_ids_distinct),
        nodes' = nodes,
        keepIds,
        keepDefects,
      })
  }

  // The configured half of the same row: with `cluster-node-id` set, every boot returns
  // that id unchanged and nothing is ever minted — then, once the knob is removed again,
  // minting resumes. The two regimes are one trace so the contrast is forced, not assumed.
  run configuredIdStableAcrossRestartTest: bool = {
    init
      .then(bootAs(2, false, true))
      .expect(
        val n2 = nodes.get(2)
        all { n2.configured_id == Some(2), n2.effective_id == Some(2), n2.id_mints == 0,
              n2.intent == Join }
      )
      .then(restartAs(2, true, true))
      .expect(
        val n2 = nodes.get(2)
        all { n2.effective_id == Some(2), n2.id_mints == 0, n2.intent == Join,
              not(n2.routable_leader) }
      )
      .then(all {
        assert(witnessConfiguredIdPinned),
        assert(inv_configured_id_stable),
        assert(inv_restart_deterministic),
        nodes' = nodes,
        keepIds,
        keepDefects,
      })
      // The knob removed again: this boot mints, and the mint source is untouched by the
      // two pinned boots before it.
      .then(restartAs(2, false, false))
      .expect(
        val n2 = nodes.get(2)
        all { n2.configured_id == None, n2.effective_id == Some(FRESH_ID_BASE),
              n2.id_mints == 1, n2.intent == Join }
      )
  }

  // The issue-03 `applyRepurpose` ruling, forced: the write-once `intent` cell is immune to
  // the config-driven paths the guarantee is scoped to (a flipped bootstrap knob, a
  // re-minted id — neither moves it), and an admin `CLUSTER RESET` rewrites it anyway,
  // which is lawful and leaves the scoped guarantee intact.
  run adminResetMayRewriteIntentTest: bool = {
    init
      .then(bootAs(3, true, false))
      .expect(nodes.get(3).intent == Bootstrap)
      .then(restartAs(3, false, false))
      .expect(
        val n3 = nodes.get(3)
        // `not(configured_bootstrap)` pins the premise (battery row A29): the knob really
        // is off in this node's state, so `intent == Bootstrap` beside it is the persisted
        // decision outliving a contradicting config read, not a restart that never read one.
        all { n3.intent == Bootstrap, not(n3.configured_bootstrap),
              n3.effective_id == Some(FRESH_ID_BASE + 1),
              inv_intent_write_once, inv_identity_survives_id_churn }
      )
      .then(repurpose(3))
      .expect(
        val n3 = nodes.get(3)
        all {
          n3.intent == Join,
          n3.member_of == None,
          n3.raft != Empty,
          // The reset does not un-boot the node: it is still a running process with an id,
          // which is why MEET's physicality conjunct is not what refuses it.
          n3.effective_id == Some(FRESH_ID_BASE + 1),
          inv_intent_write_once,
        }
      )
  }

  // ---------------------------------------------------------------------------
  // Refusal traces added by the mutation battery's gap-closure pass
  // (.scratch/formal-spec/2026-08-19-admission-battery.md, lifecycle step 5). Each one
  // forces exactly one guard conjunct that no earlier oracle could see, named in its
  // comment by the battery row it kills.
  // ---------------------------------------------------------------------------

  // Row A22 — `canRestart`'s `intent != Undecided`. A durable record that has never booted
  // has no process to restart: `restart` models a *re*boot, and admitting it before the
  // first boot would hand a never-started node an `effective_id` (and so satisfy MEET's
  // physicality conjunct) without ever passing through the one action that decides the
  // bootstrap-vs-join mode. Nothing downstream notices — the node just looks like a booted
  // Undecided node forever — so the refusal has to be pinned here.
  run restartRefusedBeforeFirstBootTest: bool = {
    init
      .then(bootAs(1, true, false))
      .expect(
        val n4 = nodes.get(4)
        all { n4.intent == Undecided, n4.effective_id == None }
      )
      // `.fail()` is the terminal step: n4 has never booted, so there is nothing to restart.
      .then(restartAs(4, true, false).fail())
  }

  // Row A32 — `canMeet`'s `nNode.member_of != None`. `CLUSTER MEET` is issued *at* a
  // cluster: a node that belongs to none has no cluster to fold the target into, and
  // admitting the transition anyway would write the inviter's own non-membership onto the
  // target. Both nodes here are lawful joiners (booted, empty Raft, physically present), so
  // the only conjunct that can refuse is the inviter's membership — the discrimination
  // `meetRefusesForeignRaftStateTest` (Raft state) and `meetRefusesUnbootedNodeTest`
  // (physicality) leave uncovered.
  run meetRefusedFromNonMemberTest: bool = {
    init
      .then(bootAs(1, false, false))
      .then(bootAs(2, false, false))
      .expect(
        val n1 = nodes.get(1)
        val n2 = nodes.get(2)
        all { n1.member_of == None, n2.effective_id != None, n2.raft == Empty }
      )
      // `.fail()` is the terminal step: n1 is in no cluster, so there is nothing to fold
      // n2 into — even though n2 satisfies both of the other two conjuncts.
      .then(meet(1, 2).fail())
  }

  // Row A39 — `canRepurpose`'s `member_of != None`. A `CLUSTER RESET` against a node that
  // is already unclaimed is not a transition this model has: the reset's postcondition is
  // "reduce membership to just this node", and there is no membership left to reduce. The
  // trace reaches the one state where that conjunct refuses *alone* — the residue state,
  // where `raft != Empty` still holds — so it discriminates the two conjuncts.
  run repurposeRefusedOnUnclaimedNodeTest: bool = {
    init
      .then(bootAs(3, true, false))
      .then(repurpose(3))
      .expect(
        val n3 = nodes.get(3)
        all { n3.member_of == None, n3.raft != Empty }
      )
      // `.fail()` is the terminal step: n3 is already unclaimed; only the membership
      // conjunct can refuse here, since the Raft-residue conjunct is satisfied.
      .then(repurpose(3).fail())
  }

  // Row A02 — `configuredIdFor`'s `Some(e) => Some(e)` arm. Pinning `cluster-node-id` at a
  // node that is already running fixes the id it is *running under*, not one re-derived
  // from its durable record: the knob's whole purpose (state-space row "This node's own
  // id") is to stop the id from moving, so a pin that itself renames the node would defeat
  // it. `configuredIdStableAcrossRestartTest` cannot see this — it pins at a first boot,
  // where the running id and the record key coincide by construction.
  run pinAdoptsRunningIdTest: bool = {
    init
      .then(bootAs(1, true, false))
      .expect(
        val n1 = nodes.get(1)
        all { n1.effective_id == Some(FRESH_ID_BASE), n1.configured_id == None }
      )
      // The operator adds the knob to an already-running node.
      .then(restartAs(1, true, true))
      .expect(
        val n1 = nodes.get(1)
        all { n1.configured_id == Some(FRESH_ID_BASE), n1.effective_id == Some(FRESH_ID_BASE),
              n1.id_mints == 1, inv_configured_id_stable }
      )
  }
}
