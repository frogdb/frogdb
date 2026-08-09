# 10 — Stateright model 1: two-phase slot handoff

Status: done

## Parent

[PRD](../../PRD.md) §3 W3; dev-dependency + nightly budget ruled in §8 D1.

## What to build

Add `stateright` as a dev-dependency of `frogdb-cluster`. Model the two-phase handoff:
coordinator + source + target + Raft-as-serializer, 3 nodes / 2 slots / bounded retries.
The transition function is production `apply_command` — the model layer contributes only
what Raft/network contribute in production: ordering, loss, duplication, leader changes.

Safety: no interleaving of Prepare/Confirm/Abort/Complete/leader-change admits writes on
two nodes for one slot; seqs never reused. Liveness: every prepared handoff reaches
Released or Completed.

Bounded-depth smoke config (<10 s) in the default suite; real exploration budget in the
nightly, recorded (states/minutes) in the model file header. If the state space defeats
the small-scope hypothesis, record the tried budget and the drop decision in this issue
and the PRD — that outcome is a legitimate close.

## Acceptance criteria

- [ ] Model embeds production `apply_command` (no hand-translated transition fn)
- [ ] Both safety properties + liveness checked; state-space size recorded
- [ ] Smoke config in default suite; full budget nightly
- [ ] Any counterexample checked in as a regression scenario replayed against the real
      state machine
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — model states are judged by the
  catalog.

## Resolution

Shipped. `frogdb-server/crates/cluster/src/model/` — a `stateright` model whose transition
function is production `ClusterState::apply_command`: every step reloads a node's
`ClusterSnapshot` through `ClusterState::from_snapshot`, applies the real command, and keeps
the resulting snapshot, so `commands.rs` (and the restore seam, FM-CLUSTER-100) is what is
being checked. The model layer supplies only Raft/network effects: one replicated log,
per-node apply lag, leader changes, duplicated drain confirmations, abandoned coordinator
polls, a common discrete clock. Coordinator control flow transcribes
`SlotMigrationCoordinator::complete`; the source barrier transcribes `plan_handoff_action`.

Properties: `single_writer_per_slot` (safety a), `handoff_seqs_never_reused` (safety b),
`invariant_catalog_clean` (every explored state judged by the catalog),
`apply_is_deterministic`, `every_prepared_handoff_settles` (liveness: Released or
Completed), plus `sometimes` witnesses (`a_handoff_completes`, `a_handoff_aborts`,
`a_second_attempt_runs` where the scope allows a retry) so a green run cannot be vacuous.

### State space (BFS to exhaustion; no config truncated)

| config | scope | unique states | depth | wall | where |
|---|---|---|---|---|---|
| `smoke_scope()` | 1 slot, 2 attempts, 1 dup ack, 1 leader change | 31 324 | 31 | 0.8 s (debug) | default suite |
| `unbounded_lag_scope()` | 1 slot, unbounded apply lag | 1 156 | 14 | 0.1 s (debug) | default suite |
| `cross_slot_scope()` | 2 slots, 1 attempt, 1 dup ack, 1 leader change | 1 306 692 | 30 | 8 s (release) | nightly, `just model-check` |
| `deep_scope()` | 1 slot, 3 attempts, 2 dup acks, 2 leader changes | 12 186 542 | 54 | 65 s (release) | nightly, `just model-check` |

The small-scope hypothesis **bends, not breaks**: breadth and depth do not compose. Two
slots *with* retries ran past 25 s and 7 GB resident without terminating, so the full budget
is the two exhaustive configs above run side by side rather than one product of them; both
are `#[ignore]`d, run by `just model-check` and the generated `cluster-model-nightly`
workflow (90-minute job budget, change-gated), and pinned one-at-a-time via the
`model-check` nextest test-group (`deep_scope()` holds ~3 GB). `MIN_*_STATES` floors in
`model/tests.rs` fail the build if a scope edit quietly shrinks the space.

### Counterexample

One, and it maps to neither issue 14 nor issue 15: **issue 17** — a source whose apply of
`CompleteSlotMigration` lags its apply of `PrepareSlotHandoff` by more than `barrier_ms`
re-admits writes for a slot the target already owns, because the barrier is a local
wall-clock window rather than a fence on ownership. Trace:
`[Tick, Coord(0), Apply(1), Apply(2), Coord(0), Drain(0), Apply(1), Apply(2), Coord(0), Apply(2), Tick, Tick]`.
Filed as `.scratch/cluster-correctness/issues/open/17-stale-source-outlives-its-write-barrier.md`
with three candidate rulings. Encoded twice: `model::tests::stale_source_admits_writes_after_ownership_moves`
(characterization — asserts the discovery is still there, so a fix fails it loudly) and
`model::replay::stale_source_keeps_serving_after_the_target_takes_the_slot`, a deterministic
replay of the trace against real `ClusterState`s with no model checker in the loop.

### Evidence

- `just test frogdb-cluster` green, smoke + replay + unbounded-lag in the default suite.
- `just model-check` green: both full configs pass all properties at the numbers above.
- `just lint-failure-modes` green; spec rows FM-CLUSTER-084/085/086/088/100 name the model
  tests. The lint now lists with `--run-ignored all` — a nightly-budget test still forces
  the failure mode its row names.
- `just workflow-gen --check` green (nightly workflow is generated, not hand-written).
- `just mutants-diff frogdb-cluster`: the crate diff is `#[cfg(test)]`-only, so no product
  code is mutated by it.
