# 23 — The scheduler's same-seed fingerprint changes under host load

Status: done

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

- [x] Root cause identified and closed at a seam; the divergent trace event named
- [x] `same_seed_same_run` hardened to run under synthetic CPU pressure and still pass
- [x] `just mutants-diff` triaged on any touched locked crate

## Blocked by

None.

## Resolution (2026-08-10)

### Root cause

`Instant::elapsed()` is `std::time::Instant::now() - self`. It reads the **OS** clock no
matter which clock produced the anchor. Every site that correctly took its anchor from the
seam — `frogdb_types::clock::now()`, which is `tokio::time::Instant::now()` and therefore
virtual under a paused runtime — and then measured that anchor's age with `.elapsed()`
silently put the measurement back on the real clock. Under turmoil the two clocks run at
different speeds (virtual time fast-forwards past idle periods, the OS clock crawls), so the
result was a duration proportional to how loaded the host was.

`scripts/clock-seam.py` could not see it: the gate grepped for `::now`, and an anchor read
through the seam looks compliant to every predicate it had.

The first site the bisect landed on is the one the trace exposed:
`frogdb-server/crates/server/src/connection.rs:640`, `self.state.created_at.elapsed()`,
rendered as `session_duration_ms` in the connection log. On seed 7 the same connection
reported `197` in the fast run and `2405` in the slow one. The **behavioural** site on the
same page is `frogdb-server/crates/server/src/connection/lifecycle.rs:221`, whose
`>= STATS_SYNC_INTERVAL_MS` comparison decides whether a connection syncs its stats this
iteration — a real-clock-gated branch inside the simulation. The clearest illustration of the
pattern is `frogdb-server/crates/acl/src/ratelimit.rs:27`: a `BASELINE` initialised with
`get_or_init(clock::now)` under a comment explaining that it is seamed, then un-seamed one
line later by `.elapsed()`.

46 such sites existed across 12 crates, four of them locked (replication,
replication-runtime, persistence, txn).

The divergent trace event asked for by this issue: seed 7, `op[58] migrate slot=1769`,
`committed` in the fast run against `commit-pending` in the slow one. Seed 1 shows the same
shape, which is what the issue originally observed.

### Fix

`c62da703` — `frogdb_types::clock::elapsed(since)`, defined as
`now().saturating_duration_since(since)`, is the compliant way to age an anchor. All 46
in-scope sites now call it. Left deliberately on the OS clock, with the reason recorded at the
code: `server/src/latency_test.rs` (a busy loop that must observe real time or hang forever),
`tokio-coz/src/hooks.rs` (a profiler measuring the machine), and the `vll` readings whose
anchors are already `tokio::time::Instant`.

The gate now rejects `.elapsed()` in the same scope, exempting files whose `Instant` is
tokio's by the same import test the bare-`Instant::now` rule already used. Without that rule
the anchor read looks compliant and the un-seaming stays invisible — which is exactly how
this survived the earlier passes.

### Hardening

`48a6188a` — the determinism test's replay now runs through `run_seed_stretched`, which
sleeps 500us of *real* time after every `sim.step()`. That inverts the real-to-simulated time
ratio (a step simulates 1ms and normally costs ~270us) without touching the simulated clock,
so any duration read off the OS clock comes out several times larger in the replay and the
fingerprints diverge.

This is a deterministic substitute for the "burn CPU in the harness" the issue asked for, and
a stronger one: it does not depend on the machine's core count or on how much of the load the
scheduler actually delivers to the sim thread, and it reproduces the failure on an idle
laptop. The test was additionally verified 10/10 under eight competing busy loops.

### Not fixed here

Seed 9 still diverges under a 2000us-per-step stretch (13 of 14 seeds are clean). That
residue is [issue 24](../open/), filed with the full bisect and the hypotheses that survive
it.
