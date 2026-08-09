# 03 — Proptest command-sequence generator + P1 (invariants always hold)

Status: done

## Parent

[PRD](../../PRD.md) §3 W2; dev-dependency + nightly ruled in §8 D1.

## What to build

Add `proptest` as a dev-dependency of `frogdb-cluster`. Build
`arb_command_sequence(len)`: a weighted stateful strategy over all 18 `ClusterCommand`
variants that tracks live node ids / assigned slots / open migrations and biases ~80/20
toward commands valid in context — garbage retained deliberately, because a *rejected*
command must also preserve every invariant and the rejection path is where
validate-then-mutate bugs live.

P1: apply each generated sequence via `apply_local`, assert the invariant catalog clean
after every step. Moderate case count in the normal suite; `PROPTEST_CASES`-boosted pass
wired into the nightly.

## Acceptance criteria

- [ ] Generator produces stateful, biased sequences over all 18 variants
- [ ] P1 runs in the default suite at moderate cases; failure shrinks to a minimal
      sequence
- [ ] Nightly boosted pass wired (same test, env-raised cases)
- [ ] `just mutants-diff frogdb-cluster` triaged before push

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — P1 asserts the catalog.

## Resolution

Shipped 2026-08-09 on `worktree-agent-a52c303bb4857d209`.

### What landed

`frogdb-server/crates/cluster/src/properties.rs` — the generator and P1.

`arb_command_sequence(len)` is **stateful without a shadow model**: it folds the sequence it is
building through the real transition function (`ClusterState::apply_to`, widened to
`pub(crate)` for this), so every command is drawn against the state its predecessors actually
produced — live node ids, assigned slots, open migrations, and each migration's handoff `seq`.
It applies through `apply_to` rather than `apply_local` on purpose: `apply_local` runs the
issue-02 assertion hook, and a panic raised *inside* a proptest strategy escapes the runner's
`catch_unwind` and aborts the run without shrinking. Generation stays panic-free; the property
does the asserting.

All 18 `ClusterCommand` variants are covered by a weight table that a test proves exhaustive
and non-overlapping (`the_weight_table_covers_every_command_exactly_once`,
`every_weighted_draw_selects_a_variant`), and a second test proves the generator actually
*emits* each of the 18 (`the_generator_emits_every_command_variant`). `IN_CONTEXT_BIAS = 0.8`
aims 80% of draws at the reached state; the remaining 20% is deliberate garbage (unknown node
ids, mismatched migration parameters, stale handoff `seq`s), because a rejected command must
leave the state as clean as an accepted one and validate-then-mutate bugs live on the
rejection path. `the_generator_mixes_accepted_and_rejected_commands` pins that both outcomes
occur in quantity; the accepted-share band is deliberately wide (0.25..=0.9) because an
in-context draw is an *aim*, not a guarantee — the state can move under it within the same
sequence.

Reaching the *interesting* states took tuning, all of it recorded in-code:

- `SEQUENCE_LEN` 24 → 48 (lengths drawn uniformly from `1..=SEQUENCE_LEN`). The migration
  lifecycle is four commands deep *after* a cluster with assigned slots exists.
- `CLOCK_STEPS` shrunk to `[0, 0, 1, 25, 25, HANDOFF_BARRIER_MS + 1]`. The original pool
  contained `HANDOFF_LEASE_MS + 1`, giving a ~1688 ms mean step that blew the 100 ms barrier on
  every sequence, so **no migration ever completed** — diagnosed by instrumenting the sampler
  (`steps=4484 mig=1088 handoff=428 drained=105 completed=0`).
- Barrier/lease pools widened to 4 entries so the already-elapsed `0` is 1-in-4, not 1-in-2.
- In-context draws now chain the lifecycle: `migration_ref_where` / `prepared_migration_ref` /
  `drained_migration_ref` pick the migration that is at the stage the command wants,
  `AssignSlots` picks a run of slots that are neither owned nor migrating, and
  `BeginSlotMigration` prefers an assigned slot with no open migration.

`the_generator_reaches_prepared_drained_and_completed_handoffs` is the coverage guard for all
of the above: it fails if the generator regresses to never preparing, never draining, or never
completing a handoff.

**P1** (`p1_every_apply_leaves_the_catalog_clean`) applies each sequence through `apply_local`
and asserts `invariants::check_hard` is empty after **every** step — an explicit assert on top
of the debug hook, so the property still holds in a build where `debug_assertions` is off.

### Case budget

- Default suite: `DEFAULT_CASES = 96` (well under a second; a case is up to 48 applies).
- Boosted: `PROPTEST_CASES`, read by `cases_from`, which treats unset/unparseable/zero as "use
  the default" so a typo in a CI invocation cannot silently reduce the property to nothing
  (`the_case_budget_defaults_and_is_raised_by_the_environment`).
- `just cluster-proptest [CASES]` (default **200000**) is the laptop-runnable boosted pass, per
  PRD §8 D4 — one place owns the budget.
- Nightly: `.github/workflows/cluster-nightly.yml`, **generated** from
  `.github/workflows/workflow_gen/src/workflow_gen/workflows/cluster_nightly.py` (registered in
  `render.py::WORKFLOWS`) — cron `47 3 * * *`, change-gated like the other nightlies, a
  `workflow_dispatch` `cases` input, and a body that is exactly `just cluster-proptest`, so the
  workflow never duplicates the budget.
- `.config/nextest.toml` gained a `package(frogdb-cluster) & test(properties::)` override at
  30 s slow / 4× terminate. The default 5 s/15 s kill terminated the 200k run at 15.007 s. The
  hard kill is kept (unlike the nightly seed sweep, this test also runs in the default suite,
  so a genuine hang must still be caught).

### Counterexamples and their disposition

P1 found a counterexample **on its first run**, shrinking to four commands:

```
INV-MIG-1: slot 0 is migrating from 1 but is owned by 5
```

A third defect class, distinct from the two already filed — no role transition (issue 14) and
no failover (issue 15) is involved. `AssignSlots` validates membership and current ownership
but never consults `inner.migrations`, while `BeginSlotMigration` deliberately accepts a slot
with no recorded owner (the follower-seed allowance the catalog documents). The two allowances
compose: a migrating-but-unassigned slot can be handed to a node that is neither source nor
target. Filed as
[issue 16](../open/16-assign-slots-ignores-open-migrations.md) with the verbatim shrunk repro, the
`RemoveSlots`-then-`AssignSlots` variant that reaches the same state, the downstream
consequences (ASK/MOVED naming a node without the data; `CompleteSlotMigration` overwriting the
third node's claim; a barrier armed at a non-owner) and two candidate rulings. Fixing it is out
of scope here (PRD §7 keeps defect fixes out of the harness issues).

No other counterexample appeared, at 96 cases or at 200 000.

### Keeping P1 green without weakening it

The generator is **not** constrained away from any of the three defects — constraining it would
hide the neighbourhood the defect lives in. Instead a single `known_defect(state, command)`
function muzzles exactly three pre-state/command shapes, one per issue (14, 15, 16), each
citing its issue by number in the returned string. Each shape is pinned by a
`#[should_panic(expected = "INV-…")]` witness that replays the shrunk repro through
`apply_local` — `pinned_issue_14_add_node_admits_a_dangling_parent`,
`pinned_issue_15_graceful_failover_strands_a_migration`,
`pinned_issue_16_assign_slots_hands_a_migrating_slot_to_a_third_node` — so when a defect is
fixed its witness goes red and points at the muzzle entry to delete.
`the_muzzle_only_covers_the_pinned_shapes` holds the muzzle narrow: it asserts `known_defect`
returns `None` for the near-miss shapes (well-formed parent, non-graceful failover, assignment
to the migration's own source, assignment of a slot with no migration).

### Verification

- `just test frogdb-cluster` — **276 passed, 1 skipped**
- `just cluster-proptest` (200 000 cases/property) — 11 tests passed, P1 in **26.756 s**, no
  further counterexamples
- `just check frogdb-cluster`, `cargo clippy -p frogdb-cluster --all-targets -- -D warnings` — clean
- `just lint-failure-modes` — OK: 278 failure modes, 1382 test references, 1382 tags
- `just scratch-check` — OK: 10 feature dirs, tracker consistent
- `just workflow-gen --check` / `just generate-check` — all generated files up to date
- `just mutants-diff frogdb-cluster` — **"No mutants to filter"**: the diff is `#[cfg(test)]`
  code plus one visibility widening, so cargo-mutants produced zero mutants. Nothing to triage.
  The harness's own mutation coverage comes from the meta-tests listed above (weight-table
  exhaustiveness, variant emission, accept/reject mix, lifecycle reach, budget parsing, muzzle
  narrowness) rather than from mutating product code that this issue did not touch.
