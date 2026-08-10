# 09 — Stateright model 2: feed gate racing promotion

Status: done

## Parent

[PRD](../../PRD.md) §3 W3 (model 2); budget and smoke/nightly split inherited in §8 D1; recipe
shape ruled in §8 D8.

## What to build

Model a deadline-carrying feed hold (`ReplicaFeedGate::publish`, `feed_gate.rs:75`, called from
`core/src/client_registry/mod.rs:979`) racing a barrier release, a barrier re-arm, and a role
change. As in model 1, the transition function is issue 07's extracted pure decision function over
a `ReplicationView` — production code, not a hand-translated transcription.

Safety properties:

- the feed is never held past its deadline;
- a release belonging to an ended barrier never opens a feed that a later barrier holds;
- a role change never leaves a hold with no owner.

Liveness: every hold is eventually released. Every explored state is judged by the W1 catalog, and
`sometimes` witnesses keep a green run non-vacuous.

This models `8d55cc4f` / FM-CLUSTER-097 — retro-validation revert (d) — directly, and is that
defect's level-3 catcher, paired with INV-GATE-1 at level 1.

Same discipline as issue 08 and for the same reasons: two exhaustive configs side by side rather
than one product, `MIN_*_STATES` floors in the test file so a scope edit cannot quietly shrink the
space, the `#[ignore]` split (bounded-depth smoke under 10 s per commit, full configs nightly),
the `[test-groups.model-check] max-threads = 1` pin in `.config/nextest.toml:131-150`, and the
sibling `just replication-model-check` recipe rather than a widened `model-check` `PATTERN`.

§6 criterion 7 applies here too: a written drop decision naming the budget tried and how far the
seam extraction got is a legitimate close.

## Acceptance criteria

- [x] Transition function is production code (issue 07's extracted feed-gate decision)
- [x] All three safety clauses, the liveness property, `sometimes` vacuity witnesses, and
      `check_hard` over every explored state
- [x] A model run against a tree with `8d55cc4f` reverted produces a counterexample (recorded as
      revert (d)'s level-3 evidence for issue 15)
- [x] Two exhaustive configs with recorded state-space sizes and wall times; `MIN_*_STATES` floors
      in the test file
- [x] Smoke config under 10 s in the default suite; full configs `#[ignore]`d and run by
      `just replication-model-check`, nightly wired through the workflow generator
- [x] Any counterexample checked in as a deterministic replay against real state and filed as its
      own issue

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — the feed-gate transition must be a pure
  function before it can be model-checked.

## Resolution (2026-08-10)

`frogdb-server/crates/replication/src/model/feed_gate/` — model, checker runs, deterministic
replays. Sibling of `frogdb-cluster`'s models, same discipline, own budget.

**The transitions are production code.** Every transition that touches the hold calls
issue 07's decisions and applies what they answer: `decide_publish` (store-and-wake iff the
value changes), `decide_hold` (in force strictly before the deadline), and
`decide_feed_hold_until` — extracted in this issue's first commit, because the "a release
belonging to an ended barrier never opens a feed a later barrier holds" clause is a property
of the *derivation* (latest deadline across armed entries), which lived in `frogdb-core`
where `frogdb-replication` cannot reach it. `PauseState::feed_hold_until` now calls it, so
the model checks that rule rather than a transcription of it. What the model layer adds is
only what the callers add — the pause entry fold and its republish, `released()`'s
register-then-recheck loop, the write task's buffer-while-held, `notify_waiters`' wake-
everyone-registered — each documented against the call site it transcribes.

**Properties.** Safety: `no_frame_ships_while_a_barrier_is_armed` (judged against the pause
entries, not the gate, so the two halves cannot agree by construction);
`the_gate_agrees_with_the_pause_state` (the ended-barrier clause, and the role-change clause
— the gate belongs to the client registry and outlives every session, and the model replaces
all sessions on a role change so the ones that read the hold afterwards are never the ones
that slept against it); `a_hold_in_force_is_future_and_bounded`; `no_publish_is_lost`;
`a_publish_never_drops_a_later_deadline`. Liveness: `every_hold_is_eventually_released`.
Nine `sometimes` witnesses keep a green run non-vacuous (overlapping barriers, a hold lapsing
with nobody clearing it, a publish waking a sleeper, a role change inside a window, ...).

Issue 02's catalog does not exist yet, so `a_hold_in_force_is_future_and_bounded` stands in
for INV-GATE-1 in the shape the catalog will state it; the module doc names it as the thing
to replace with a `check_hard` call when W1 lands.

**Revert (d) evidence without a reverted tree.** `Scope::honour_the_gate` withdraws the
assumption that sessions consult the gate — that *is* the tree before `8d55cc4f`. The model
falsifies `no_frame_ships_while_a_barrier_is_armed` in 44 states:
`[Arm(0, 2), Write, Ship(0), ...]`. `replay.rs` re-runs it against the real
`ReplicaFeedGate` with no `stateright` in the loop, and runs the same sequence through the
shipped write task to show the gate refusing it. Both assertions are in the default suite, so
this is level-3 evidence for issue 15 that cannot rot.

**Teeth.** Three edits to the production decisions, each caught by the smoke config in ~0.1 s:
`decide_feed_hold_until`'s `max` as `min` falsifies the ship clause; `decide_hold`'s `<` as
`<=` falsifies the bounded-hold clause; a `decide_publish` that treats a shortened deadline as
no change falsifies the agreement clause. The last two are the surviving-mutant shapes
`cargo mutants` proposes for this file.

**Budgets** (exhaustive BFS, no depth or time truncation; floors asserted by `MIN_*_STATES`):

| config | scope | states | depth | wall |
|---|---|---|---|---|
| smoke (default suite) | 2 barriers, 2 sessions, horizon 7 | 20,438 | 20 | 0.1s |
| unheld-feed (default suite) | pre-`8d55cc4f`, 1 barrier, 1 session | 44 | 9 | <0.1s |
| full/overlapping (nightly) | 3 barriers, 3 sessions, horizon 12 | 3,942,370 | ~42 | 12.9s |
| full/churn (nightly) | 2 barriers, 3 sessions, 2 role changes, horizon 15 | 2,649,370 | ~45 | 6.9s |

State counts are exact — a property of the scope, not of the run. Depth is not: parallel BFS
moves it by a tick or two, so only the counts are floored.

**Wiring.** Full configs are `#[ignore]`d, run by `just replication-model-check` (a sibling
recipe per §8 D8, not a widened `model-check` pattern) and by the generated
`replication-model-nightly.yml` workflow (`workflow_gen/workflows/replication_model_nightly.py`,
04:47 UTC, change-gated like its cluster twin). They share the `[test-groups.model-check]`
`max-threads = 1` pin so two multi-million-state runs cannot land on one machine at once.

**No new defect.** Every property holds at every explored state of every honest scope; the
only counterexample is the withdrawn-assumption one, which is the point of that scope. No
issue filed.

Commits: `29772149` (extract `decide_feed_hold_until`), `d75d2b0e` (model + replays),
plus the recipe/workflow/spec-row commit.
