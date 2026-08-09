# 09 — Seed-driven fault scheduler for the turmoil cluster sims

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W4; seed budget ruled in §8 D4 (500/night, locally runnable).

## What to build

Generalize the five scripted turmoil cluster sims into one seed-driven scheduler: a seed
derives the full schedule (which links partition when, which nodes SIGKILL/restart when,
message delay distributions, barrier/lease timer skew). Same seed → same run.

Invariant checking at quiesce via `DEBUG CLUSTER CHECK` on every surviving node, plus the
existing client-visible assertions, plus cross-node checks a single-state catalog cannot
express (pairwise epoch monotonicity as observed by clients, single-writer-per-slot over
the whole history).

Sweep recipe: one `just` invocation, budget in one Justfile variable (default 500),
runnable on a laptop; nightly CI calls the same recipe. A failing seed is committed to a
regression list replayed forever after.

Durability faults (lose-unsynced-writes on kill) explicitly ride campaign 2's crash
harness, not turmoil — no disk model here.

## Acceptance criteria

- [ ] The five scripted sims are subsumed (each scripted scenario expressible as a seed
      or kept as a named regression seed)
- [ ] Same seed reproduces the same run bit-for-bit
- [ ] `just <recipe> [SEEDS=n]` works locally; nightly wired to the same recipe
- [ ] Regression-seed list exists and replays in CI
- [ ] Cross-node invariant checks (epoch monotonicity, single-writer-per-slot) active at
      quiesce

## Blocked by

- Issue 06 (`.scratch/cluster-correctness/issues/`) — quiesce checks consume the
  command's catalog surface (in-process equivalent acceptable under turmoil).
