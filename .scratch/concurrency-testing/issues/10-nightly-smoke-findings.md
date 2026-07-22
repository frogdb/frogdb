# 10 — Nightly-tier smoke testing surfaced real concurrency invariant violations

Status: needs-triage
Type: bug
Origin: Phase 5 (CI wiring) plumbing validation, 2026-07-22 — testbox smoke runs of the new
`seed_sweep_nightly` harness (`just concurrency-nightly`), not the 1000-seed sweep itself.

## What happened

While validating the new nightly concurrency harness (bumping seed count and, separately,
`ops_per_client` via the new `FROGDB_CONCURRENCY_SEEDS`/`FROGDB_CONCURRENCY_OPS_PER_CLIENT` env
overrides — see issue context in the phase-5 CI-wiring work), several **real, reproducible
invariant violations** turned up that the existing per-PR tier (`just concurrency`, `ops_per_client
= 30` effectively, ≤20 seeds) never exercises. These are pre-existing bugs in the product, not bugs
in the new harness/CI plumbing — surfaced *because* phase 5 finally wires up longer
histories/more seeds, which is exactly what the nightly tier is for.

### Finding A — MultiWaiter "exactly-once delivery" data loss at `ops_per_client >= ~90` (severe, near-deterministic)

Empirical bisection (testbox `tbx_01ky55m7z93fdxn6bry62mkc95`, `frogdb-server` turmoil harness):

| `ops_per_client` | seeds tried | result |
|---|---|---|
| 30 | 5 | pass |
| 75 | 5 | pass |
| 90 | 5 | 4/5 seeds FAIL |
| 110 | 5 | 5/5 seeds FAIL |
| 150 (harness default at time of writing) | 3 | 3/3 seeds FAIL |

Failure signature (`MultiWaiter` profile only):

```
seed 0 (MultiWaiter) FAILED: ["exactly-once delivery: element [105, 109, 48, 98] (pushed by op 12491) was neither delivered nor in final state"]
```

An element pushed to a blocking-list key is neither delivered to any blocked waiter nor present in
the key's final state — it is silently lost. This reproduces on effectively every seed once
`ops_per_client` crosses roughly 90, which points at a real bug (likely a race in the blocking-pop
wake/consume path under sustained load) rather than a rare interleaving. **This is the most
actionable finding here** and is why the nightly job's default `ops_per_client` was capped at 75
(see "Resolution" below) rather than something larger — running nightly at 150 would just
re-report the same known bug every night instead of surfacing new ones.

### Finding B — Other, rarer violations at `ops_per_client = 60`, `seeds = 20` (3 of 80 seed×profile runs)

```
seed 3 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 24458 committed
  though op 24405 wrote watched key [123, 116, 49, 125, 107, 118, 49] after watch op 24402"]
seed 5 (MultiWaiter) FAILED: ["FIFO wake order: FIFO wake order violated on key [123, 116, 52, 125,
  108, 115, 48]: op 34464 (later waiter) served before op 34462"]
seed 19 (MultiWaiter) FAILED: ["key {t13}zs1 (ZSet) not linearizable (problematic ops [...])",
  "key {t12}zs0 (ZSet) not linearizable (problematic ops [...])"]
```

The TxHeavy WATCH false-negative is a **different bug class** than the already-fixed/pinned
`regression_crossshard_watch_false_negative_seed_8` in
`frogdb-server/crates/server/tests/concurrency_workload.rs` (`mod regressions`) — same invariant,
different trigger — so it is not simply a re-manifestation of a known, already-fixed issue.

### Finding C — ZSet non-linearizability under MultiWaiter reproduces even at `ops_per_client = 30` (the per-PR-vetted value) given enough seeds

Control run at `ops_per_client = 30`, `seeds = 20` (isolating seed-count from ops-per-client,
run on the same testbox):

```
seed 1 (MultiWaiter) FAILED: ["key {t12}zs0 (ZSet) not linearizable (problematic ops [16458,
  16472, 16477, 16483, 16499, 16505, 16512, 16514, 16524, 16528, 16536, 16539, 16540, 16544,
  16547, 16548, 16550, 16551])"]
```

This confirms Finding B's ZSet-linearizability failure is **not** an artifact of raising
`ops_per_client` — it is a rare bug reachable at the same per-op-count scale the per-PR tier
already uses, just not sampled by the per-PR tier's low seed count (≤20 seeds across profiles,
vs. nightly's 1000+). This is the clearest evidence that the nightly tier's value proposition
(more seeds, not just longer histories) is real: per-PR tuning alone cannot make this class of
finding go away, because it is a genuine sampling effect, not a scale effect. (Note: this
particular control run's log was contaminated by an operator error — two `tb-run` invocations were
launched against the same testbox almost simultaneously, violating the "one `tb-run` at a time per
worktree" rule — so a second reported seed (16) from that run is not trusted; only the clean,
unambiguous seed 1 failure above is cited.)

## Why this is filed as a bug, not closed as "working as intended"

Findings B and C are exactly the kind of rare-interleaving bug the nightly tier exists to catch —
that's not a problem, that's the design working. Finding A is different in kind: it reproduces on
*nearly every seed* once `ops_per_client` crosses a threshold, which means it isn't "rare" at all —
it's a load-dependent bug that per-PR's low `ops_per_client` simply doesn't reach. All three should
be triaged as real product bugs.

## Acceptance criteria

- [ ] Root-cause Finding A (MultiWaiter exactly-once-delivery loss under sustained load) — likely in
      the blocking-pop wake/consume path (`frogdb-server/crates/core` blocking-list waiter
      machinery). Given how reliably it reproduces (`FROGDB_CONCURRENCY_OPS_PER_CLIENT=110
      FROGDB_CONCURRENCY_SEEDS=5 just concurrency-nightly`, or replay any of the repro files
      generated during the bisection — not preserved past the testbox session, so a fresh repro run
      is needed), this should be one of the cheaper items to reproduce and fix.
- [ ] Investigate the new TxHeavy WATCH false-negative (Finding B, seed 3 at
      `ops_per_client=60`) against the existing WATCH false-negative fix that pinned
      `regression_crossshard_watch_false_negative_seed_8` — determine whether it's the same root
      cause reachable via a different path, or a distinct gap.
      Repro: `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20
      just concurrency-nightly` (TxHeavy profile, seed 3).
- [ ] Investigate the MultiWaiter FIFO wake-order violation (Finding B, seed 5 at same params).
- [ ] Investigate MultiWaiter ZSet non-linearizability (Findings B/C — reproduces at both
      `ops_per_client=30` seed 1 and `ops_per_client=60` seed 19, and `ops_per_client=150` regime
      too). Given it reproduces at the per-PR-vetted `ops_per_client=30`, this is reachable with a
      comparatively short/cheap repro and should be prioritized for root-causing.
- [ ] Once Finding A is fixed, revisit whether `concurrency-nightly`'s default `ops_per_client`
      (see "Resolution" below) can be safely raised back toward the harness's original coded
      default, to get longer per-history coverage in the nightly tier.

## Resolution shipped in phase 5 (CI wiring)

To avoid shipping a nightly job that is *guaranteed* red every run (which trains reviewers to
ignore it, defeating the point), `ops_per_client = 75` — not 150 — is baked in as the default in
both places that matter: the `env_override("FROGDB_CONCURRENCY_OPS_PER_CLIENT", 75usize)` call in
`seed_sweep_nightly` (`frogdb-server/crates/server/tests/concurrency_workload.rs`), and the `OPS`
parameter default on the `just concurrency-nightly` recipe (`Justfile`). 75 is the highest value
that was empirically clean across the bisection above (the ~90 threshold above which Finding A
reproduces on nearly every seed is documented in both places). `concurrency-nightly.yml` doesn't
override `OPS`, so it inherits 75 from the Justfile recipe.

The nightly job is safe to run as shipped: it will not go permanently red on Finding A. It may
still occasionally surface Findings B/C (rare, real bugs — that's the tier's purpose), which is
expected and should be triaged against this issue rather than treated as a workflow defect.
Raising `ops_per_client` past 75, or raising `seeds` enough to meaningfully change the sampling
rate of Findings B/C, is gated on root-causing the relevant finding below first.

## Blocked by

None for triage/investigation. Fixing Finding A is likely blocking-list/waiter code; Findings B/C
are TxHeavy WATCH and MultiWaiter/ZSet code respectively — see acceptance criteria for pointers.

## References

- `frogdb-server/crates/server/tests/concurrency_workload.rs` — `seed_sweep_nightly`,
  `env_override`, `mod regressions` (existing pinned WATCH false-negative regression test, for
  comparison against Finding B's new one).
- `frogdb-server/crates/server/tests/common/invariants.rs` — `check_all`/`check_all_with`
  (exactly-once delivery, FIFO wake order, WATCH false-negative, bounded WGL linearizability
  checks referenced above).
- `frogdb-server/crates/testing/src/workload.rs` — `Profile` enum (`TxHeavy`, `BlockingHeavy`,
  `MultiWaiter`, `Mixed`).
- `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md` — phase 5 (CI
  wiring) entry, which references this issue.
