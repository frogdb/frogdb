# 08 — Stateright model 1: promotion racing an active stream

Status: done

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

- [x] Transition function is production code (issue 07's pure decision functions); no
      hand-translated model transition
- [x] All three safety clauses, the liveness property, `sometimes` vacuity witnesses, and
      `check_hard` over every explored state
- [x] Two exhaustive configs run side by side with state-space sizes and wall times recorded in
      the model file header; `MIN_*_STATES` floors in the test file
- [x] Smoke config under 10 s in the default suite; full configs `#[ignore]`d, run by
      `just replication-model-check`, pinned one-at-a-time via the `model-check` nextest group
- [x] Nightly wired through the workflow generator (`just workflow-gen --check` green)
- [x] Any counterexample checked in as a deterministic replay against real state (no model checker
      in the loop) and filed as its own issue

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — the model's transition function is the
  extracted promotion decision; without the split there is nothing pure to check.

## Resolution (2026-08-10)

Built as `frogdb-server/crates/replication/src/model/promotion/` (model, `tests.rs`, `replay.rs`),
with `stateright` added as a dev-dependency of `frogdb-replication`.

**Production code is the transition function.** Every deciding step calls what production calls:
`plan_primary_stint` (with the mint passed in exactly as `begin_primary_stint` passes
`generate_replication_id`), a real `PartialSyncReplay` rebuilt from the node's window and asked
`handle_partial_sync_request` — whose *granted frames* are what the model delivers, so a floor that
promises a range the backlog cannot cover surfaces as a hole rather than as an assumption — the
grant rendered as `replica_session` renders it and parsed back by the replica's `select_psync_arm`
over a line built by `psync_request_args`, and every state mutation through a `ReplicationState`
method (`shift_replication_id`, `apply_staged_metadata`). The model never assigns `replication_id`
or `secondary_offset` by hand.

**Properties.** Safety: `continue_is_never_served_over_a_hole`,
`no_two_replids_claim_one_offset_range`, `full_resync_adopts_nothing_early`,
`a_failed_promotion_restores_the_state_exactly`, `every_psync_resolves_to_exactly_one_arm`,
`a_live_link_never_forks_history`, and `replication_state_stays_valid` (production
`ReplicationState::validate` over every explored state). Liveness: `every_replica_ends_up_streaming`
where the scope admits it. Vacuity witnesses: `a_promotion_mints_and_freezes_a_window`,
`a_resume_is_granted`, `a_full_resync_is_granted`, `a_promotion_lands_with_frames_in_flight`,
`a_replica_streams_from_a_promoted_node`, plus the pinned exposure witness
`a_failed_promotion_strands_the_node`.

**Budget** (release, M-series laptop; floors in `tests.rs` sit just under each count):

| config | unique states | depth | wall | where |
|---|---|---|---|---|
| `smoke` | 124,265 | 23 | 0.2 s (0.85 s debug) | default suite |
| `strand` | 544 | 13 | <0.1 s | default suite |
| `full/deep` | 8,038,584 | 34 | 13.8 s | nightly / `just replication-model-check` |
| `full/two-primary` | 6,172,626 | 41 | 11.9 s | nightly / `just replication-model-check` |

Wiring: `just replication-model-check` (sibling recipe, per §8 D8), both full tests in the
`model-check` nextest group (`max-threads = 1`), the smoke config in the per-commit slow-timeout
group, and `.github/workflows/replication-model-nightly.yml` generated by
`workflow_gen/workflows/replication_model_nightly.py` (`just workflow-gen --check` green).

**Findings.**

1. *A real exposure, filed as [issue 16](../open/16-failed-promotion-strands-the-applied-gate.md) rather
   than fixed inline*: a promotion whose persist fails restores the `ReplicationState` bit for bit
   (the `StintPlan` half holds) but leaves the applied gate frozen by `settle_at_applied`, the
   inbound stream dropped and `primary_target` cleared. The node applies nothing (`Claim::Retired`)
   and cannot resync either (`reset_to` refused), recoverable only from outside. Pinned as the
   `sometimes("a_failed_promotion_strands_the_node")` witness and as the deterministic replay
   `model::promotion::replay::a_failed_promotion_leaves_the_node_unable_to_replicate`, which runs
   against a real `PrimaryReplicationHandler` / `AppliedOffset` / `ReplicaOffset` with no checker in
   the loop. FM-REPLICATION-020 gained a "deliberate non-guarantee" paragraph naming it.
2. *A model-fidelity bug the checker caught in the model itself*: the first full-scope run produced
   a `no_two_replids_claim_one_offset_range` counterexample in which a node advertised a failover
   window over an empty history. The cause was the model clearing the replica's dataset when the
   `+FULLRESYNC` was granted; production stages the payload and swaps it in only on install
   (`ReplicaConnection::install_payload`), keeping the previous keyspace meanwhile. Corrected, and
   `full_resync_adopts_nothing_early` now asserts the *state* clause only. No production defect.

**Gap carried forward.** The per-state judgment is production `ReplicationState::validate`, not the
W1 violation catalog — that catalog does not exist yet (issue 02 is in flight). Wiring
`check_hard` over every explored state is a one-line follow-up once issue 02 lands and belongs to
whoever closes it.
