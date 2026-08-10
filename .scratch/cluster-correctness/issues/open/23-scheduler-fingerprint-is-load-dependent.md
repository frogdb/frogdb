# 23 — The scheduler's same-seed fingerprint changes under host load

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W4 — follow-up to issue 09 (`../done/09-seeded-fault-scheduler.md`),
observed during issue 13's retro-validation runs (`../done/13-retro-validation-gate.md`).

## What is wrong

`simulation::scheduler::test_cluster_scheduler_same_seed_same_run` runs seed 1 twice and
compares `RunOutcome::fingerprint` (the event-trace hash). During issue 13, on a **clean
tree**, the two runs diverged — one trace recorded a write as `commit-pending` where the
other recorded `committed` — while a concurrent `cargo-mutants` run was saturating the
machine (~4x timing inflation). On an idle machine the same test passes repeatedly
(re-verified on d5681872).

A turmoil sim is supposed to be closed over simulated time, so host load must not be able
to reach the trace. The observed divergence means something inside the sim still consults
real time or real scheduling. Suspects, in rough order:

- a real-clock read (`Instant::now`/`SystemTime`) somewhere on the turmoil path that the
  cfg(turmoil) seams don't cover — the `commit-pending`/`committed` split smells like a
  client giving up on an ack wait that is bounded by something other than simulated time;
- tokio internals that turmoil does not virtualize (e.g. anything spawned onto a runtime
  outside the sim, channel `try_recv` loops with `yield_now` counting on wall progress);
- the sweep's `CLUSTER_SEEDS_JOBS` worker split leaking OS-thread interleaving into a
  structure that feeds the fingerprint (the same-seed test uses one seed, so only if
  shared state crosses runs).

## Why it matters

The determinism claim is what makes a failing seed replayable and a muzzled seed
meaningful. A load-dependent fingerprint means a nightly failure may not reproduce
locally, and a muzzled `EXPECTED-FAILURE` seed may flap green/red on a busy CI host —
exactly the class of noise the regression list exists to prevent.

## What to build

Find the leak and close it at a seam (the same pattern as issue 09's collapse of
openraft's election jitter under `cfg(feature = "turmoil")`). Then make the determinism
test hostile: run the pair of seed-1 runs while the harness itself burns CPU (spawn
busy-loop threads for the duration) so the test fails on an idle laptop too, not only
under a concurrent mutants run.

## Acceptance criteria

- [ ] Root cause identified and closed at a seam; the divergent trace event named
- [ ] `same_seed_same_run` hardened to run under synthetic CPU pressure and still pass
- [ ] `just mutants-diff` triaged on any touched locked crate

## Blocked by

None.
