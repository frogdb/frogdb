# 09 — Stateright model 2: feed gate racing promotion

Status: needs-triage

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

- [ ] Transition function is production code (issue 07's extracted feed-gate decision)
- [ ] All three safety clauses, the liveness property, `sometimes` vacuity witnesses, and
      `check_hard` over every explored state
- [ ] A model run against a tree with `8d55cc4f` reverted produces a counterexample (recorded as
      revert (d)'s level-3 evidence for issue 15)
- [ ] Two exhaustive configs with recorded state-space sizes and wall times; `MIN_*_STATES` floors
      in the test file
- [ ] Smoke config under 10 s in the default suite; full configs `#[ignore]`d and run by
      `just replication-model-check`, nightly wired through the workflow generator
- [ ] Any counterexample checked in as a deterministic replay against real state and filed as its
      own issue

## Blocked by

- Issue 07 (`.scratch/replication-correctness/issues/`) — the feed-gate transition must be a pure
  function before it can be model-checked.
