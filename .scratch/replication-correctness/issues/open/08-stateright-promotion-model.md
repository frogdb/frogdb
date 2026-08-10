# 08 — Stateright model 1: promotion racing an active stream

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3 (model 1); dev-dependency, smoke/nightly split and budget inherited in
§8 D1; recipe shape ruled in §8 D8.

## What to build

`stateright = "0.31"` is already a workspace dependency and `frogdb-cluster` already uses it, so
adding it to `frogdb-replication` is one dev-dep line. The cost is the model, not the dependency.

Model a primary P feeding replica R with frames in flight and an ack lag, and land a promotion of
R at **every point** in the interleaving; then demote the ex-primary toward R and have it
re-`PSYNC`. The transition function is production code — issue 07's extracted decision functions
over a `ReplicationView`, the replication analogue of the cluster models' snapshot-reload trick
("the thing being checked is `commands.rs` itself"). No hand-translated transition function.

Safety properties:

- a granted `+CONTINUE` is never served over a hole in the promoted node's history;
- no two live replids claim the same offset range;
- a `+FULLRESYNC` adopts neither half of the granted pair before the payload installs
  (FM-REPLICATION-001's ordering clause).

Liveness: every reconnect eventually resolves to exactly one arm. A scope variant with two
would-be primaries covers the **two primaries feeding one replica** hunt — a surviving replica
must not accept a stream from a stale primary after reattaching to the promoted one. Every
explored state is additionally judged by the W1 catalog, and `sometimes` vacuity witnesses prove a
green run is not green because nothing happened.

Budgeting lesson carried over verbatim from the cluster campaign: **breadth and depth do not
compose.** Two exhaustive configs run side by side per model, not one product of them. Port the
same guardrails: `MIN_*_STATES` floors in the test file (cluster's `model/tests.rs:12-14`) so a
scope edit cannot quietly shrink the space, the `#[ignore]` split (bounded-depth smoke under 10 s
per commit, full configs nightly), and the `[test-groups.model-check] max-threads = 1` pin in
`.config/nextest.toml:131-150` because deep scopes hold multiple GB resident. Per §8 D8 the recipe
is a **sibling `just replication-model-check`**, not a widened `model-check` `PATTERN`
(Justfile:190), so the two nightly budgets stay independently tunable.

§6 criterion 7 makes a drop a legitimate close: if the state space defeats the small-scope
hypothesis, record the budget that was tried and how far the seam extraction got, here and in the
PRD, rather than leaving the criterion open.

## Acceptance criteria

- [ ] Transition function is production code (issue 07's pure decision functions); no
      hand-translated model transition
- [ ] All three safety clauses, the liveness property, `sometimes` vacuity witnesses, and
      `check_hard` over every explored state
- [ ] Two exhaustive configs run side by side with state-space sizes and wall times recorded in
      the model file header; `MIN_*_STATES` floors in the test file
- [ ] Smoke config under 10 s in the default suite; full configs `#[ignore]`d, run by
      `just replication-model-check`, pinned one-at-a-time via the `model-check` nextest group
- [ ] Nightly wired through the workflow generator (`just workflow-gen --check` green)
- [ ] Any counterexample checked in as a deterministic replay against real state (no model checker
      in the loop) and filed as its own issue

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — the model's transition function is the
  extracted promotion decision; without the split there is nothing pure to check.
