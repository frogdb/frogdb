# 03: Phase-2 model hygiene sweep

Status: ready-for-agent

## Origin

Findings from the phase-2 cluster quint plan's task reviews (task-1/2/3/4/5-review.md) and its
final whole-branch review (`.superpowers/sdd/2026-08-13-phase2-cluster-quint-plan/final-review.md`,
"New issue to file"), deferred as cluster (not fix-before-merge): each needs either a ruling,
follow-up modeling work, or a small independent cleanup, none of it blocking the plan's merge.

## What to build

Work through each item; where marked "needs ruling", get a decision recorded here (or in the
citing model/spec comment) before doing the work, same as issues 01/02's ruled-items convention.

- [ ] [t1 m3] Joiner-local raft state is read atomically at the acceptor in
  `cluster_admission.qnt` — a modeling decision that needs an invariant added and re-validated
  under mutation testing, not just documented as an assumption.
- [ ] [t1 re-review] `canMeet` lost its physicality conjunct: as written, MEET can fold in a node
  that never booted. Needs a ruling on whether that's an intentional relaxation or a gap.
- [ ] [t1 re-review] `applyRepurpose` rewrites the write-once `intent` field. Likely consistent —
  `CLUSTER RESET` is an admin command and `specs/cluster.md:129` only forbids rewrite *by config
  changes* — but needs a ruling note recorded in the model header confirming that reading.
- [ ] [t2 N2] `removeNode` churn drowns simulation traces: measured 39.8% of sampled traces lose
  ≥3 nodes before anything interesting happens. Remedy measured and proposed: a 1-in-4 coin
  gating `removeNode`'s selection. Needs implementing in `cluster_migration_failover.qnt`'s
  action-selection weighting and re-measuring.
- [ ] [t2 N3] `canSetRole` carries two conjuncts beyond what TR-004 requires (non-load-bearing per
  the task-2 review). Either justify them in the header or drop them.
- [ ] [t3 F8] `new_driver()` in `quint_conformance.rs` is dead scaffolding — no caller. Delete it
  or wire it in, whichever the harness's next iteration needs.
- [ ] [t3 F11] The `#[ignore]` disclosure text is duplicated verbatim across several tests instead
  of factored into a shared `const`. De-duplicate.
- [ ] [t4 M6] `setup_java_step` (workflow_gen) is over-parameterized: two knobs nothing calls with
  a non-default value. Trim them.
- [ ] [t5] Phase 1 of the formal-spec design doc has no explicit "Complete" marker, unlike phase
  2 (a convention gap, not a defect). Decide whether phase 1 gets one retroactively.
- [ ] [reviewer #11] `itf = "0.4.0"` is a caret range in `frogdb-cluster/Cargo.toml`'s dev-deps
  even though the surrounding comment claims it's pinned; both `itf` and `quint-connect` bypass
  the workspace's `[workspace.dependencies]` table. Align the version spec with the comment (or
  fix the comment) and move both into `[workspace.dependencies]`.
- [ ] [reviewer #12] `binary(quint_conformance)` has no nextest slow-timeout override — nextest's
  default 15s hard-kill is tight against a cold `quint` CLI start (~2.6s measured locally; likely
  slower in CI). Add a per-binary override in `.config/nextest.toml`.
- [ ] [reviewer #13] `scripts/quint-invariants.sh`'s invariant enumeration is line-based grep and
  silently drops an invariant whose `val`/`inv_` tokens split across a newline. Add a post-check
  count assertion, or switch to quint-native enumeration.
- [ ] [reviewer #14] Model-hygiene residue: disclaimer-only citations inflate the admission
  count; `INV-SLOT-1` is cited three times only to be rejected, never asserted; slash-shorthand
  ids (`TR-CLUSTER-017/026`) are invisible to the citation linter and collide with the real,
  separate `TR-CLUSTER-026`; a stale "see their removal note below" cross-reference at
  `cluster_migration_failover.qnt:259-260` no longer points at anything. Clean up each.

## Un-ignore owner for the two simulation tests (I7 / minor 6)

`seeded_simulation_test` and `unpinned_simulation_test` in `quint_conformance.rs` are the only
callers of the `switch!`/nondet-decoding machinery (~45 lines, `quint_conformance.rs:553-598` at
final-review time) — with both `#[ignore]`d, that machinery has zero running coverage. This issue
is their owning condition: they stay `#[ignore]`d until quint-connect's simulation support
(`#[quint_run]` sampling arbitrary traces rather than a fixed named run) is validated end-to-end
against this crate — concretely, until [t2 N2]'s churn fix above lands (so a sampled trace has
useful odds of reaching interesting states before `removeNode` empties the cluster) *and* the
outstanding named-run divergences the simulation tests currently hit (issues 15, 17, 19, 20, and
the ghost-field ScriptedProj-only normalization tracked as issue 33) are closed, since simulation
traces use plain `ClusterDriver`/`ClusterProjection`, not `ScriptedProj`, and so get none of the
named-run harness's divergence workarounds. Un-ignore once all of the above hold; if the
`switch!` machinery is still uncovered by any other test at that point, that's this issue's
signal to look at it, not a reason to delete it before then.

## Acceptance criteria

- [ ] Every ruled item above has a recorded ruling (here or in the citing file) and, where the
  ruling calls for a change, the change is made
- [ ] `just lint-spec`, `just quint-check`, `just quint-run` stay green
- [ ] `seeded_simulation_test`/`unpinned_simulation_test` un-ignored once this issue's owning
  condition is met, or left `#[ignore]`d with an updated string still pointing here if not yet met
- [ ] `just scratch-check` green

## Blocked by

- None (independent cleanup; the un-ignore owner section above is self-contained about its own
  precondition rather than blocking on another issue number)
