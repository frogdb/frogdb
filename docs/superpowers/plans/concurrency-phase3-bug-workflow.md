# Concurrency Phase 3 — Bug Workflow

How a failing seed from the generated-workload sweep becomes a fixed bug with a
permanent regression guard. Referenced from the Phase-3 plan
([`2026-07-19-concurrency-phase3-turmoil-workload-harness.md`](2026-07-19-concurrency-phase3-turmoil-workload-harness.md))
and the concurrency-invariant-testing design's "Bug workflow" section.

## The loop

1. **Failing seed.** `just concurrency` runs `seed_sweep_short_workloads`
   (`frogdb-server/crates/server/tests/concurrency_workload.rs`). On a violation
   the sweep panics and auto-writes a repro file to
   `target/concurrency-repros/<seed>.json` capturing the exact
   `(seed, profile, num_clients, ops_per_client, num_shards)`.

2. **Reproduce in isolation.** Replay just that case:

   ```bash
   just concurrency-repro target/concurrency-repros/<seed>.json
   ```

   This runs the `#[ignore]`d `replay_repro` test with `REPRO_FILE` set. The run
   is fully deterministic (turmoil seed + fake persistence), so it reproduces
   every time.

3. **Triage: harness gap vs server bug.**
   - **Harness/model gap** (e.g. a reply the recorder mis-encodes, a vocabulary
     item the model can't route) → fix the harness (`sim_harness.rs`
     canonicalization, `workload.rs` generation, `invariants.rs` pipeline).
     Never weaken a checker to make the sweep pass.
   - **Genuine server bug** → continue.

4. **Debug.** Use `superpowers:systematic-debugging`. The repro is small and
   deterministic; shrink it further by hand (fewer ops/clients) if needed.

5. **Fix + pin.** Land the server fix together with a **named** pinned
   regression test in the `regressions` module of `concurrency_workload.rs`,
   with the failing `(seed, profile, config)` **hardcoded**. Copy the
   `regression_template_seed_0` template, rename it for the bug, and confirm it
   **FAILS before the fix and PASSES after**. It can then never silently
   regress.

6. **Keep the repro in history, not the tree.** Reference the repro JSON in the
   commit message; do not commit `target/concurrency-repros/` (it is build
   output). The pinned test carries everything needed to reconstruct the case.

## Day-1 candidate bugs

The design flags these as likely first finds; if the sweep trips one, it is
fixed in-plan with a pinned regression: lost-element race on concurrent
list pop/push, XREADGROUP NOGROUP-on-expiry (out of Phase-3 vocabulary),
cross-shard keyspace delivery ordering.
