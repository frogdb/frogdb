# Phase 2 — Cluster Quint Models + quint-connect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the two phase-2 Quint design models (membership admission window,
migration×failover) plus the quint-connect conformance harness for the cluster state
machine, wired into lint and CI, per design doc §3/§4/§7-phase-2.

**Architecture:** Models are authored *from the spec* (`specs/cluster.md` State-space + TR
rows + amended rulings), never transcribed from code (design §8.5 transcription ban).
quint-connect replays model traces against the real `ClusterState` apply path in a
`frogdb-cluster` dev-dependency test target. Lint already counts `.qnt` header citations;
CI gains a typecheck+smoke lane and a nightly bounded-verify lane.

**Tech Stack:** Quint 0.32.0 (`npm:@informalsystems/quint` via mise, already in
`.mise.toml`), quint-connect 0.1.2 (crates.io, dev-dep), Apalache (auto-downloaded by
`quint verify`, needs JVM), existing `workflow_gen` Python package for CI.

## Global Constraints

- Build mode: **local**. All commands run on this machine. No testbox.
- Branch: `cluster-fixes-integration` only. Never push main. Commit per task. Commit
  trailer (exact): `Claude-Session: https://claude.ai/code/session_01RnVPzKvpMY2v27JFvUQXNf`.
  No Co-Authored-By lines.
- `frogdb-cluster` is a **locked crate** (mutation gate 0.80). Phase-2 adds only
  dev-dependency test code there — no production-code changes. FM rows in
  `specs/cluster.md` are **not edited** by any task in this plan.
- Spec text is authority: models encode the **ruled/amended** semantics stated in the TR
  rows (issue 19 per-object fence, issue 17 log-ordered fence/no-wall-clock, issue 15
  abort=rollback-with-repatriation, issue 20 demote-don't-remove, issue 25
  bootstrap-vs-join, issue 30 priority re-registration), not today's code where they
  differ. Where a TR row carries `Pending`, the model still encodes the ruled semantics.
- **Counterexample protocol (design §3):** a counterexample from `quint run`/`quint
  verify` is triaged like a defect — first check the model against the spec row; if the
  model is wrong, fix the model; if the *spec/ruling* is what breaks, STOP and report the
  counterexample verbatim to the orchestrator (it becomes a user ruling). Never silently
  weaken an invariant or add a guard to make checking pass.
- Every `.qnt` file's leading `//` comment block must cite the `TR-`/`FM-`/`INV-` ids it
  models (lint-enforced, ≥1 citation; dangling ids are errors). Helper modules, if any,
  also cite the rows they support (tripwire 3 resolution: no citation-free allowlist).
- Model files stay **flat** under `specs/quint/` (no subdirectories — the lint and
  `quint-check` globs are non-recursive, tripwire 2).
- After every task: `just lint-spec` green, `just quint-check` green, and the task's own
  test commands green, before commit.
- Implementer agents MUST load the project `quint-lang` and `quint-modeling` skills
  (in `.claude/skills/`) before writing any Quint.
- `quint` binary: via mise shims (`~/.local/share/mise/shims/quint`, v0.32.0 verified
  installed). If not on PATH in a fresh shell, `mise x -- quint ...` works.

## File Structure

- Create: `specs/quint/cluster_admission.qnt` — Task 1
- Create: `specs/quint/cluster_migration_failover.qnt` — Task 2
- Create: `frogdb-server/crates/cluster/tests/quint_conformance.rs` (+ dev-dep in
  `frogdb-server/crates/cluster/Cargo.toml`) — Task 3
- Modify: `Justfile` (`quint-run` smoke recipe; `lint` aggregation) — Task 4
- Modify: `.github/workflows/workflow_gen/src/workflow_gen/workflows/` (typecheck+smoke
  into the PR lane; new `quint-verify` nightly) + regenerate YAML — Task 4
- Modify: `specs/cluster.md` — add a short "Quint models" pointer subsection under the
  Transitions preamble naming the two models (Task 5, doc-only)

---

### Task 1: Admission-window model (`cluster_admission.qnt`)

The issue-25 defect class: a freshly-started node solo-bootstraps into an externally
routable "leader" while it was actually meant to join. Model the **ruled** semantics.

**Files:**
- Create: `specs/quint/cluster_admission.qnt`

**Spec inputs (read these, in this order):**
- `specs/cluster.md` TR-CLUSTER-028 (explicit bootstrap), TR-CLUSTER-029 (joining node
  defers election until MEET), TR-CLUSTER-005 (MEET join-safety gates, incl. the
  refuse-nonempty-raft-state amendment), TR-CLUSTER-030 (bootstrap slot seeding),
  State-space rows "Bootstrap-vs-join intent" and "This node's own id".
- `.scratch/cluster-correctness/issues/open/25-…solo-bootstrap.md` Ruling + Amendment
  sections (options 1+2, refuse-MEET-nonempty-state, persist-bootstrap-intent).

**Model shape (state variables and actions are the requirement — exact Quint syntax is
the implementer's, via the quint skills):**

State per node (small fixed node set, e.g. `Set(1, 2, 3, 4)`):
- `intent: None | Bootstrap | Join` — the *persisted* record (ruled semantics; written
  once at first mode decision, survives `restart`)
- `configured_bootstrap: bool` — what config says (may disagree with a stale intent after
  an operator config flip; the model must allow this divergence)
- `raft: Empty | Initialized(term)` — abstract; nonempty ⇒ node has ever initialized
- `routable_leader: bool` — externally-observable leadership claim
- `member_of: Option[ClusterId]` — which logical cluster the node belongs to

Actions (one per TR arm, cite them):
- `boot(n)` — first boot: decide mode from config, **persist intent**, bootstrap nodes
  initialize + become routable leader; join nodes do NOT initialize (TR-CLUSTER-028/029)
- `restart(n)` — non-first boot: mode comes from persisted intent, never re-derived
  (the issue-25 amendment's determinism claim)
- `meet(n, m)` — fold `m` into `n`'s cluster; **refused** if `m.raft != Empty` and `m`
  was ever a member of a different cluster (TR-CLUSTER-005 amendment)
- `stutter` — no-op step so the simulator can always progress

Invariants (each becomes a `val inv_…: bool` checked by `quint run --invariant` and named
tests):
- `inv_no_usurper`: a node with `intent == Join` is never `routable_leader` unless
  `member_of` matches a cluster that folded it in via `meet`
- `inv_single_routable_group`: two nodes routable-leader simultaneously ⇒ they are in
  different `ClusterId`s that were *never* merged (no split-brain inside one deployment)
- `inv_restart_deterministic`: after `restart(n)`, `n`'s mode equals its persisted
  intent regardless of `configured_bootstrap`
- `inv_meet_no_absorption`: `meet` never merges two clusters that both have nonempty
  raft state (the foreign-state-absorption refusal)

Header comment block cites (minimum): `TR-CLUSTER-005`, `TR-CLUSTER-028`,
`TR-CLUSTER-029`, `TR-CLUSTER-030`, `FM-CLUSTER-101` if the voter-set claim is modeled,
`INV-EPOCH-1` only if epochs enter the model (don't force it).

**Steps:**
- [ ] **Step 1:** Load `quint-lang` + `quint-modeling` skills; read the spec inputs.
- [ ] **Step 2:** Write the model with the state/actions/invariants above, plus at least
      two named `run` tests: one happy bootstrap-then-meet trajectory, one that would
      reproduce the issue-25 usurpation **and is asserted unreachable** under ruled
      semantics (i.e. the pre-ruling behavior is not expressible as a legal step).
- [ ] **Step 3:** `quint typecheck specs/quint/cluster_admission.qnt` — clean.
- [ ] **Step 4:** `quint test specs/quint/cluster_admission.qnt` and
      `quint run --invariant='inv_no_usurper and inv_single_routable_group and inv_restart_deterministic and inv_meet_no_absorption' --max-samples=1000 specs/quint/cluster_admission.qnt`
      (adjust flag syntax to what 0.32.0 accepts) — no violations. Any violation →
      counterexample protocol (Global Constraints).
- [ ] **Step 5:** `just quint-check && just lint-spec` — green; lint output line must now
      report `≥1 quint citations over 1 models`.
- [ ] **Step 6:** Commit (`spec(quint): admission-window model for the cluster join/bootstrap ruling`).

---

### Task 2: Migration×failover model (`cluster_migration_failover.qnt`)

The issue-15/16/17/20 defect class: slot migrations interleaved with failovers.

**Files:**
- Create: `specs/quint/cluster_migration_failover.qnt`

**Spec inputs:**
- `specs/cluster.md` State-space rows: Slot ownership, Open migrations, prepared handoff,
  handoff seq/attempt counter, config epoch, node roles/FAIL flag.
- TR rows: TR-CLUSTER-004 (SetRole), 008 (AssignSlots, incl. its ruled
  open-migration precondition), 009 (RemoveSlots), 010 (BeginSlotMigration),
  011 (PrepareSlotHandoff), 012 (ConfirmSlotHandoffDrained), 013
  (CompleteSlotMigration), 014 (AbortSlotHandoff), 015 (CancelSlotMigration —
  ruled: abort = rollback **with repatriation**, issue 15), 016 (replica-feed hold),
  017/018 (planned + applied graceful failover, incl. issue-17 byte-cap hold +
  log-ordered fence), 021 (successor selection), 024/025 (MarkNodeFailed/Recovered),
  042 (`force: true` failover; issue-20 ruled demote-don't-remove), 034 (barrier
  arm/release as per-node reaction).
- Invariant catalog `frogdb-server/crates/cluster/src/invariants.rs`: INV-SLOT-1,
  INV-MIG-1, INV-HANDOFF-1, INV-HANDOFF-2, INV-EPOCH-1, INV-EPOCH-2, INV-REF-1..4 —
  model the ones expressible over the model's variables (expected: SLOT, MIG, HANDOFF,
  EPOCH families; REF-family only if parent links are modeled).

**Model shape:**

Replicated state (this is one Raft state machine — model the *applied* sequence, no
network/consensus modeling; openraft's total order is assumed, which is exactly the
`apply_command` boundary quint-connect drives in Task 3):
- `slots: SlotId -> Option[NodeId]` (use a tiny slot space, e.g. 4 slots)
- `migrations: SlotId -> Option[{source, target, handoff: Option[{seq, drained}]}]`
- `handoff_seq: int` (monotone mint)
- `epoch: int`
- `nodes: NodeId -> {role: Primary | Replica(of: NodeId) | Removed, fail: bool}`

Node-local derived state (per-object fence, issue-19 amended):
- `barrier: (NodeId, SlotId) -> Armed | Released` — a pure function of the replicated
  handoff events each node has applied (TR-CLUSTER-034); model nodes applying the
  replicated log at different prefixes ONLY if cheap — otherwise model the barrier at the
  applied-state level and note the simplification in the header.

Actions: one per TR row listed above, with preconditions exactly as the TR states them
(ruled semantics). `Failover(force)` must implement: graceful = drain+parity barrier
before transfer (issue 26's planned-failover barrier is `Pending` — encode the ruled
barrier); forced = issue-20 ruled demotion (old primary demoted to replica, **not**
removed from topology or voter set); both prune/transfer migrations naming the demoted
node per TR-CLUSTER-018/042.

Invariants:
- `inv_slot_single_owner` (INV-SLOT-1): every assigned slot has exactly one owner, and
  the owner is a live Primary (`role == Primary`, not Removed)
- `inv_migration_endpoints_valid` (INV-MIG-1): every open migration's source owns the
  slot and source ≠ target; both endpoints exist and are not Removed
- `inv_handoff_owned` (INV-HANDOFF-1/2): a prepared handoff belongs to an open migration
  for that slot; `seq` values strictly increase with mint order and never repeat
- `inv_epoch_monotone` (INV-EPOCH-1/2): `epoch` never decreases across any action
- `inv_abort_repatriates` (issue-15 ruling): after `CancelSlotMigration`/abort, the slot
  is owned by the source and no barrier for it stays armed (no orphaned armed barrier —
  the per-object release is paid)
- `inv_forced_failover_keeps_node` (issue-20 ruling): `Failover(force)` never yields
  `role == Removed` for the old primary
- `inv_no_stuck_handoff`: from any reachable state with an armed barrier, some enabled
  action sequence releases it (temporal — encode as a named `run` test trajectory per
  reachable shape if a full temporal property is awkward in 0.32.0; do not skip silently,
  note what was checked in the header)

**Steps:**
- [ ] **Step 1:** Load quint skills; read spec inputs; enumerate the action↔TR map in the
      header comment before writing the body.
- [ ] **Step 2:** Write the model. Named `run` tests: (a) full happy migration
      begin→prepare→drain→complete; (b) graceful failover mid-migration at each handoff
      stage (at minimum: before prepare, armed-not-drained, drained-not-complete); (c)
      abort mid-handoff → repatriation; (d) forced failover mid-migration → demote path.
- [ ] **Step 3:** `quint typecheck` clean.
- [ ] **Step 4:** `quint test` + `quint run --invariant=<all invariants> --max-samples=2000`
      — no violations, else counterexample protocol.
- [ ] **Step 5:** Bounded verify locally once: `quint verify --max-steps=8` (Apalache
      auto-download; needs a JVM — if none is installed locally, note it in the report
      and rely on Step 4 + Task 4's nightly; do NOT install a JVM yourself).
- [ ] **Step 6:** `just quint-check && just lint-spec` green (`2 models` in lint line).
- [ ] **Step 7:** Commit (`spec(quint): migration×failover composite model`).

---

### Task 3: quint-connect conformance harness (`frogdb-cluster`)

Replay migration×failover model traces against the real `ClusterState` apply path.

**Files:**
- Modify: `frogdb-server/crates/cluster/Cargo.toml` — `[dev-dependencies] quint-connect = "0.1.2"`
- Create: `frogdb-server/crates/cluster/tests/quint_conformance.rs`

**Requirements:**
- Driver target is the synchronous deterministic apply boundary (design §4):
  `ClusterState`'s command application (`apply_command` path as exercised by the existing
  unit tests in `cluster/src/commands.rs` — find the test-visible constructor/apply
  helpers there; do not add pub items to production code for the harness unless a
  `#[cfg(test)]`/`pub(crate)`-shaped accessor already exists — if production API is
  genuinely insufficient, STOP and report NEEDS_CONTEXT rather than widening the API).
- Rust mirror types for the model's sum types use `#[serde(tag = "tag", content = "value")]`
  (design §4 serde note), contained in the test file.
- `State::from_driver` projection MUST cover every model variable a modeled TR
  postcondition touches: slots, migrations (incl. handoff seq/drained), handoff_seq,
  epoch, node roles + fail flags. Omitting one is the retro-gate failure mode the design
  names (projection blindness) — list the covered fields in a comment block and check it
  against the model's state declaration.
- Action mapping: model action name → `ClusterCommand` construction. Model steps that are
  node-local (barrier arm/release, TR-CLUSTER-034) are *derived* in the model from
  replicated events — the driver replays only replicated commands and projects derived
  state via the same pure function the model uses; if the production barrier planner
  (`handoff_barrier.rs::plan_handoff_action`) is reachable from `frogdb-cluster`'s test
  target, drive it; if it lives outside the crate, project only replicated state and
  note the exclusion in the test header.
- Traces: use `#[quint_test]`/`#[quint_run]` macros against
  `specs/quint/cluster_migration_failover.qnt`'s named runs and simulation. `QUINT_SEED`
  pinned in the test for reproducibility, plus one unpinned `#[quint_run]` sampling test.

**Steps:**
- [ ] **Step 1:** Read quint-connect 0.1.2 docs (docs.rs) + the model from Task 2 + the
      `ClusterState` apply API. Write the mirror types + Driver impl.
- [ ] **Step 2:** Write one failing-first smoke: replay the happy-migration named run;
      assert projection equality each step. Run
      `just test frogdb-cluster quint_conformance` — make it pass.
- [ ] **Step 3:** Add the failover-interleaving named runs + the seeded simulation test.
      A divergence between model and implementation here is EXPECTED for rows whose
      ruled semantics carry `Pending` (code lags ruling). Handle per row: if divergence
      matches a documented Pending issue, mark that trace's assertion with a
      `#[ignore = "pending issue NN — <one line>"]`-shaped skip (or the harness's
      equivalent) citing the issue; any divergence NOT matching a filed issue → STOP,
      report as a finding (either a model bug or an undocumented code defect).
- [ ] **Step 4:** `just test frogdb-cluster` — full crate green.
- [ ] **Step 5:** `just lint frogdb-cluster` green (clippy covers the test target).
- [ ] **Step 6:** Commit (`test(cluster): quint-connect conformance harness over ClusterState`).

---

### Task 4: CI wiring

**Files:**
- Modify: `Justfile` — add `quint-run` (smoke: `quint test` + bounded `quint run` per
  model, mirroring `quint-check`'s glob loop); add `quint-check` to the `lint` recipe's
  aggregation (check how `lint` composes sub-recipes first and match the pattern).
- Modify: `.github/workflows/workflow_gen/src/workflow_gen/workflows/` — (a) add
  quint typecheck+smoke to the PR/test lane (whichever module generates `test.yml` /
  the lint job; mise-managed install gives the runner `quint`); (b) new
  `quint_verify.py` nightly module patterned on the existing `cluster-model-nightly`
  module: `quint verify` (Apalache) over both models, JVM setup step
  (`actions/setup-java`, temurin LTS), change-gated like the other nightlies if the
  pattern supports it.
- Regenerate: run the workflow_gen regeneration recipe (find it in the Justfile —
  codegen rule: edit generators, never the YAML).

**Steps:**
- [ ] **Step 1:** Read `workflow_gen` package layout + one existing nightly module +
      the Justfile `lint` recipe.
- [ ] **Step 2:** Implement the Justfile additions; `just quint-run` green locally;
      `just lint` still green end-to-end.
- [ ] **Step 3:** Implement generator changes; regenerate; `git diff` the YAML and
      verify only the intended jobs appeared; YAML parses (the generator's own
      check/test recipe if one exists).
- [ ] **Step 4:** Commit (`ci: quint typecheck+smoke in the PR lane, bounded verify nightly`).

---

### Task 5: Spec pointer + phase bookkeeping (doc-only)

**Files:**
- Modify: `specs/cluster.md` — one short paragraph in the Transitions preamble: the two
  models exist, what each covers (named TR groups), where the conformance harness lives,
  and the counterexample-triage rule (one sentence each; link `specs/quint/*.qnt` paths).
- Modify: `.scratch/formal-spec/README.md` (or the design doc's §7 checklist if the
  README tracks phases) — record phase 2 complete with the delivered artifacts.
- Modify: `website/scripts/spec-gen.py` ONLY if spec-gen chokes on the new links (run
  `just spec-gen` to find out; tripwire 4 — the new paragraph must not paste `.qnt` code
  into spec prose, links only).

**Steps:**
- [ ] **Step 1:** Write both doc edits; `just lint-spec && just spec-gen && just scratch-check` green.
- [ ] **Step 2:** Commit (`docs(spec): register the phase-2 quint models and harness`).

---

## Self-review notes (writing-plans checklist)

- Spec coverage: design §7 phase 2 lists state-space + TR (landed previous arc), two
  Quint models (Tasks 1-2), quint-connect harness (Task 3). CI cadence from §3 (Task 4).
  Website/architecture-page rewrite for cluster is §6 work — deliberately OUT of this
  plan (its own later slice; the architecture-page rewrite is not gated by the models).
- The models' exact Quint code is intentionally specified at the state/action/invariant
  level, not transcribed here: design §8.5 mandates authoring from the finished spec
  sections via the quint-modeling skill, and the TR rows referenced in each task ARE the
  authoritative per-action semantics. Each task names every row so the implementer never
  reads the whole spec blind.
- Type consistency: Task 3's mirror types must match Task 2's model declarations —
  enforced by quint-connect's serde round-trip, checked at Step 2 of Task 3.
