# 11 — Stateright model 2: failover composite

Status: done

## Parent

[PRD](../../PRD.md) §3 W3.

## What to build

Second stateright model on the issue-10 infrastructure: detector verdicts +
`Failover { force }` racing `MarkNodeFailed`/`MarkNodeRecovered` and concurrent proposals
from two would-be leaders.

Safety: at most one Primary per slot after quiesce; epoch strictly grows across every
promotion. Same smoke/nightly split and budget-recording discipline as issue 10.

## Acceptance criteria

- [x] Both safety properties checked; state-space size recorded in the model header
- [x] Smoke config in default suite; full budget nightly
- [x] Counterexamples checked in as regression scenarios
- [x] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 10 (`.scratch/cluster-correctness/issues/`) — shares the model infrastructure.

## Resolution

Built as `frogdb-server/crates/cluster/src/model/failover/` (`mod.rs`, `tests.rs`,
`replay.rs`) on issue 10's infrastructure, with the same discipline: **the transition
function is production code**. Every step reloads a node's `ClusterSnapshot` into a real
`ClusterState` (`from_snapshot`) and calls `apply_command`, so what is checked is the
`Failover` / `MarkNodeFailed` / `MarkNodeRecovered` arms themselves; an edit to an arm
changes the model with no edit to the model.

The model layer adds only what Raft, the network and the *callers* add: one replicated log
appended by the current leader, per-node apply lag, leader changes (the new leader catches
up before it may append), the failure detector's control flow per would-be leader, and the
`CLUSTER FAILOVER` admin path. The detector is a transcription of `reconcile_topology` →
`mark_node_failed` → `trigger_auto_failover`, split into three model steps
(reconcile / select / propose) so everything can commit in the windows between them — which
is what admits the interleavings. Detectors are not gated on being the real leader, because
`is_leader()` reads openraft's `server_state()` and a deposed leader keeps answering
`Leader` while its `client_write` still commits by forwarding; `scope.detectors` is exactly
that window, and it is what makes "two would-be leaders" reachable.

### Properties

Both required safety properties **hold** in every checked scope:

- `single_primary_per_slot` — once the cluster quiesces (log at budget, no detector
  mid-proposal, every live detector's verdicts agreeing with the replicated flags), no slot
  has two nodes acting as its primary.
- `epoch_grows_across_promotions` — every promotion carries a strictly larger config epoch
  than every promotion before it.

Plus `invariant_catalog_clean` (issue 02's catalog judges every reachable state) and
`apply_is_deterministic` (keeps the state-identity trick honest: `NodeRt.view` is excluded
from `Hash`/`PartialEq` because it is a pure function of the applied prefix). Three vacuity
guards (`a_primary_is_flagged_failed`, `a_promotion_happens`, `a_slot_changes_hands`) fail
the run if a scope edit makes the interesting transitions unreachable.

### Budget

BFS to exhaustion; no config is depth- or time-truncated. Recorded in the model header and
floored by `MIN_*_STATES` in `model/failover/tests.rs`:

| config | scope | unique states | depth | wall | where |
|---|---|---|---|---|---|
| `smoke_scope()` | 2 leaders, 1 slot, 4 entries, 2 flips | 535 528 | 20 | 4.5 s (debug) | default suite |
| `stranded_scope()` | 1 detector, 3 entries (issue 18 witness) | 1 075 | 15 | < 0.1 s (debug) | default suite |
| `unjustified_promotion_scope()` | 1 detector, 3 entries, 1 takeover (issue 19 witness) | 1 368 | 14 | < 0.1 s (debug) | default suite |
| `two_leader_scope()` | 2 leaders, 5 entries, 3 flips, 2 leader changes | 23 090 707 | 26 | 70 s (release) | nightly, `just model-check` |
| `absorb_scope()` | 2 slots (peer primary), 5 entries, 2 leader changes | 1 704 886 | 25 | 4.6 s (release) | nightly, `just model-check` |

Model 1's finding repeats: **breadth and depth do not compose**. A second takeover on top of
the deep config (or a sixth entry) ran past four minutes and several GB resident without
terminating — one run was SIGKILLed by the OS memory manager — so the nightly runs depth and
breadth as two configs side by side. `just model-check`'s default pattern was widened to
`(handoff|failover)_model_full` and the `model-check` nextest test-group filter now pins
both models' full configs one-at-a-time; the nightly generator
(`workflows/cluster_model_nightly.py`) was updated and regenerated.

### Counterexamples

Two, and neither is covered by open issues 14/15/16/17 (14 is chained replicas, 15/16 are
migration-shaped, 17 is the handoff barrier; this model keeps `migrations` empty by
construction). Unlike model 1's, neither could be honestly framed as a withdrawn caller
assumption: three attempts to fence them off that way (a leader-view successor-health gate,
a caught-up-proposal gate, a fairness knob that retried an abandoned attempt) each moved the
counterexample instead of removing it, which is the tell that the exposure is in the system.
So both are pinned inside the checked configurations as `sometimes` properties, which go
unwitnessed — and therefore **red** — the day the exposure is closed.

- **Issue 18** (`18-a-missed-failover-is-never-retried.md`) — the automatic failover is
  edge-triggered on the `MarkNodeFailed` write, while `reconcile_topology` is level-triggered
  on the verdict-vs-flag disagreement alone. Any early return in `trigger_auto_failover`
  (stale proposer snapshot, no candidate, `MAX_ATTEMPTS` exhausted) therefore discards the
  failover permanently: the flag and the verdict now agree, so no later pass writes anything.
  The cluster comes to rest with a slot owned by a primary it has itself flagged FAIL, beside
  a healthy replica. Property `a_slot_strands_on_a_failed_primary`; witness
  `tests::a_slot_strands_on_a_primary_the_cluster_has_failed` (1 075 states); replayed
  against real `ClusterState`s by
  `replay::a_missed_failover_leaves_the_slot_on_a_failed_primary`.
- **Issue 19** (`19-a-forced-failover-promotes-a-node-that-inherits-nothing.md`) —
  `Failover { force: true }` waives the old-primary-exists check unconditionally, so it
  cannot tell "the old primary is gone because it died" from "gone because someone else
  already failed it over". A proposal built off a stale view therefore promotes a successor
  that inherits no slots: it is detached from the primary actually feeding it, that primary
  loses its only replica, and the cluster's highest config epoch lands on a slotless node.
  Reachable from both `trigger_auto_failover` and `CLUSTER FAILOVER FORCE` on a
  not-yet-reparented replica. Property `a_promotion_moves_nothing` (asserted only under
  `Topology::TwoReplicas` — structurally unwitnessable under `PeerPrimary`, where the
  failover absorbs and promotes nobody); witness `tests::a_promotion_can_move_nothing`
  (1 368 states); replayed by
  `replay::a_forced_failover_promotes_a_node_that_inherits_nothing`.

Both replays and both characterization tests assert the exposure is **still present**, so a
fix turns them red and they get flipped rather than passing dead. Each issue's acceptance
criteria name the flip.

### Evidence

- `just test frogdb-cluster` green — 291/291, smoke + both witnesses + both replays in the
  default suite, smoke 3.8 s.
- `just model-check` green: `full/two-leader` 23 090 707 states and `full/absorb`
  1 704 886 states, all properties passing.
- `just lint-failure-modes` green (278 failure modes, 1393 tags). No FM rows added: the
  change is model/test-only and adds no production behavior.
- `just scratch-check` green.
- `just workflow-gen` re-run; the generated `cluster-model-nightly.yml` matches its
  generator.
- `just mutants-diff frogdb-cluster`: "No mutants to filter" — the diff is `#[cfg(test)]`
  code only, so no product code is mutated by it.
