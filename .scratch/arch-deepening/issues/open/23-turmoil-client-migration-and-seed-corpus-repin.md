# 23 — Turmoil runners keep their hand-rolled clients because no guard protects the seed corpus from a read-cadence change

Status: needs-triage

## What to build

Proposal 78 unifies the tree's seven hand-expanded RESP clients behind one generic
`RespClient<S, C>`, but had to **exclude the three turmoil-side runners** (`RespConn` in
`frogdb-server/crates/server/tests/simulation.rs`, `workload_runner.rs`, `pubsub_runner.rs`,
plus `sim_helpers.rs` and the six inline `round_trip` bodies) to land safely. This issue carries
the excluded half and the gap that forced the exclusion.

The mechanism: turmoil's scheduler is deterministic **per execution trace**, not per seed alone.
Moving those runners from raw `read()` calls onto `Framed` changes the number and ordering of
`poll_read` wakeups by construction, which reorders the whole simulated run for a fixed seed. The
repo carries seed-addressed reproducers whose value depends on a seed replaying the same scenario:
`frogdb-server/crates/server/tests/simulation/cluster-regression-seeds.txt` holds **12 pinned
seeds, 10 of them carrying `EXPECTED-FAILURE:issue-20`** (verified at HEAD — seeds 3, 13, 17, 21,
24, 25, 39, 50, 72, 99; the two plain ones are `2 healthy` and `5 leader-isolation`), swept by
`just cluster-seeds` (`Justfile:211-212`). `FROGDB_CONCURRENCY_SEEDS` (`Justfile:164`) and
`just concurrency-repro` (`Justfile:137`) carry the same dependency for the concurrency sweep.

**The existing guard does not cover this.** The seed file's family column is checked against
`Schedule::from_seed(seed).family` by `test_scheduler_regression_seed_file_parses` — that watches
the *seed→schedule derivation*, so it catches a changed draw order. It does not watch the
*schedule→execution interleaving*. A read-cadence change leaves `Schedule::from_seed` untouched
and walks straight past it. Worse, `simulation/scheduler.rs:69` **imports `RespConn`** and uses it
at `:1457, 1559, 1565, 1671, 1691, 1798, 1807` — the seed sweep's own client layer is one of the
sites the migration would rewrite. The 10 `EXPECTED-FAILURE` seeds would at least break loudly if
quieted, but the two plain seeds would flip to a different scenario **silently**, taking their
regression coverage with them.

Two things belong in this work item, and they must land in one commit. First, migrate the turmoil
runners onto the shared client and, in the same change, **re-pin the whole seed corpus**:
re-derive `cluster-regression-seeds.txt` by re-running `just cluster-seeds` at the pre-change
sweep budget, confirm each `EXPECTED-FAILURE` seed still fails for the *same* reason (issue-20's
`XNODE-SLOT-1` signature, not merely "fails"), re-run the concurrency sweep, and regenerate any
repro files. Second, fold in the ~109 single-shot `read()` sites in `simulation.rs` that
`simulation.rs:4338-4340` already documents as able to mis-frame under turmoil chunking — they
are latent-correctness, not tidiness, and they are out of scope in 78 for exactly the same
cadence reason. Worth considering alongside: a guard that actually detects an interleaving
change, so the next cadence edit is not protected only by whoever remembers to re-run the sweep.

## Acceptance criteria

- [ ] The three turmoil-side runners (`RespConn`, `workload_runner`, `pubsub_runner`) and the
      `simulation.rs` single-read sites use the shared framed client; no hand-rolled RESP parser
      or single-shot `read()` remains in the simulation tests
- [ ] `cluster-regression-seeds.txt` is re-derived in the same commit, and each of the 10
      `EXPECTED-FAILURE:issue-20` seeds is confirmed to still fail with the `XNODE-SLOT-1`
      signature — not merely to fail; the two plain seeds still exercise their named scenarios
- [ ] The concurrency sweep is re-run and any `target/concurrency-repros/` files regenerated
- [ ] A guard exists (or the absence is explicitly ruled) that detects a schedule→execution
      interleaving change, closing the gap that
      `test_scheduler_regression_seed_file_parses` leaves open
- [ ] `just test frogdb-server test_cluster_scheduler_regression_seeds` green, and
      `just cluster-seeds` reproduces the re-pinned corpus

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 78
(`.scratch/arch-deepening/proposals/78-test-harness-resp-client.md`), blocking item **B1** —
resolved as "option (a), exclusion", with option (b) (migrate + re-pin, method written out)
recorded as this follow-up (proposal `:389-441`, `:812`, `:866-872`; review ledger `:883`).

## Comments
