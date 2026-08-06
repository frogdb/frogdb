# 14 — Generated-workload turmoil runs are not reproducible: same seed, different history

Status: ready-for-agent
Type: bug
Origin: post-harness-fix re-verification of issue 11 Findings B/C (2026-08-02) — discovered while
trying to pin the surviving seeds.

## What happened

The concurrency harness is documented and used as if it were deterministic ("same seed → same
trace"): failures are reported per seed, repro files are written per seed
(`target/concurrency-repros/<seed>.json`), and regressions are pinned by seed
(`mod regressions` in `frogdb-server/crates/server/tests/concurrency_workload.rs`).

**It is not deterministic.** Re-running the *same* `Workload` (same seed, profile, client count, op
count, shard count) produces a different operation history every time — different interleavings and
different pass/fail verdicts — both across processes and within a single process.

## Evidence

Instrumented `run_workload_capturing` + `check_all_with` (temporary diagnostic, not committed) that
prints the op count, a `DefaultHasher` digest of the history ordered by `(invoke_time, id)` over
`(id, client_id, function, args, result)`, and the violation count.

Seed 10 / `MultiWaiter` / OPS=30 / 4 clients / 2 shards, **four separate processes**:

```
DIAG violations: []   DIAG digest: ops=120 order_hash=f4d6d01a4fbbb36f
DIAG violations: []   DIAG digest: ops=120 order_hash=b9df24da679aa669
DIAG violations: []   DIAG digest: ops=120 order_hash=acdb506f2b3b8f45
DIAG violations: ["key {t12}zs0 (ZSet) not linearizable (problematic ops [122, 123, …, 240])"]
                      DIAG digest: ops=120 order_hash=55282e7463b5eefd
```

Same seed, **four repeats inside one process** (so this is not a per-process seed effect):

```
DIAG repeat 0: ops=120 order_hash=c220f6aa14a6e211 violations=0
DIAG repeat 1: ops=120 order_hash=e50ab54e50090300 violations=1
DIAG repeat 2: ops=120 order_hash=3a9115f8ea9f424c violations=0
DIAG repeat 3: ops=120 order_hash=78ef5ee0f1f16457 violations=0
```

Seed 3 / `TxHeavy` / OPS=60, five repeats in one process — five distinct histories (the WATCH
false-negative of issue 12 happens to survive all five, which is why *that* class is trustworthy):

```
DIAG repeat 0..4: order_hash = a1315627738a772a, 8976f3651969ba1a, 3546b14171fac851,
                               d5fffdf298cb10bb, 7909b049e7dfac2a   (violations=1 each)
```

The op-count is stable (120 / 230); only the ordering, the results and the verdicts move. The
verdict flapping (`violations` 0/1/0/0 on an identical input) is the load-bearing part — the digest
alone could be dismissed as op-id assignment noise.

### What it is *not*

turmoil itself is seeded correctly: `build_sim` (`frogdb-server/crates/server/tests/common/
sim_harness.rs:72`) calls `.rng_seed(config.seed)` and `run_workload_capturing` passes
`seed: workload.seed`.

Rebuilding the whole workspace with `RUSTFLAGS="--cfg tokio_unstable"` — which is what lets turmoil
seed the **tokio runtime's** RNG (`turmoil-0.7.1/src/rt.rs:241-245`, the block is `#[cfg(tokio_
unstable)]`, so today `tokio::select!` branch selection is entropy-seeded) — does **not** fix it:
three runs, three digests (`843ba37d1bbc395b`, `b10a4906f3d544d3`, `6e2179bbc779a0fb`). Setting the
cfg is still probably necessary; it is not sufficient.

The server spawns no OS threads (`grep thread::spawn frogdb-server/crates/server/src` is empty) and
builds no multi-thread runtime outside `main.rs`, so this is not real parallelism.

### Remaining suspects (for whoever picks this up)

1. **`HashMap`/`HashSet` iteration order.** `std`'s `RandomState` is randomly keyed *per instance*,
   so iteration order differs between two runs even in one process — matching the in-process
   evidence exactly. Anywhere the server iterates a hash container to make an ordering decision is
   a candidate: the keyspace in `HashMapStore`, active-expiry sampling, the client registry, waiter
   registries, pubsub. Bisect by swapping the suspect containers to a deterministic hasher (or
   `BTreeMap`) and re-running the digest test.
2. **Real-clock leakage into sim time.** `std::time::Instant::now()` is *not* virtualized by
   turmoil. Uses that feed a decision, not just a metric, will vary run to run — e.g. the
   active-expiry cycle's time budget (`frogdb-server/crates/core/src/shard/active_expiry.rs:123`)
   and `envelope.enqueued_at.elapsed()` in the shard event loop
   (`frogdb-server/crates/core/src/shard/event_loop.rs:43`).
3. **Unseeded tokio `select!` branch order**, as above — necessary but not sufficient.

## Why it matters

- **Seeds cannot be pinned.** Every `regression_*_seed_N` test in `mod regressions` is a
  probabilistic guard, not a deterministic one: it may pass on a broken build and fail on a good
  one. The two existing pins survive today only because their bugs are near-deterministic.
- **Repro files do not reproduce.** `target/concurrency-repros/<seed>.json` records the *workload*,
  and replaying it re-rolls the interleaving, so a captured failure often will not reappear.
- **Sweep results flap.** Across otherwise identical sweeps the ZSet class moved between seeds 1,
  10, 13 and 19, and the FIFO wake-order report moved between seeds 5 and 6 — the seed numbers in
  issue 11 are sampling noise, not bug identities.
- **Nightly triage is misleading.** A "new" seed in a nightly report may be an old bug re-sampled.

## Related harness defect (same theme, cheap fix)

Repro files are keyed by seed alone — `repro_path(seed)` →
`target/concurrency-repros/<seed>.json` (`frogdb-server/crates/server/tests/common/repro.rs`) — but
`seed_sweep_nightly` runs every profile per seed, so two profiles failing on the same seed overwrite
each other's repro file and the survivor is silently mislabelled. Include the profile and op count
in the filename.

## Acceptance criteria

- [x] Two runs of the same `Workload` (same seed/profile/clients/ops/shards) produce byte-identical
      histories, in-process and across processes. Pin it with a test that runs one workload twice
      and asserts the histories are equal — this is the harness's core contract and nothing else
      currently checks it.
      **Test: `concurrency_workload::determinism::run_is_reproducible_*`. Holds for
      seed 10 / MultiWaiter — in-process and across three separate processes. The two
      stream-generating cases stay `#[ignore]`d: `XADD *` mints its ID from `SystemTime::now()`
      (audit A15) and nothing virtualizes wall-clock time; that is a separate piece of work.**
- [x] The nondeterminism source(s) are identified and named in this issue (hash iteration order /
      real clock / tokio select), not just papered over.
      **All three suspects confirmed, plus two the issue did not list: unseeded chaos injection and
      unseeded skiplist level heights (observable through `memory_size()`). Enumerated as 59 sites
      in `.scratch/concurrency-testing/audit/determinism-audit-2026-08-02.md`.**
- [x] `--cfg tokio_unstable` is set for turmoil test builds (via `.cargo/config.toml` or the `just`
      recipes), so turmoil can seed tokio's RNG and forward host panics as test failures.
      **`.cargo/config.toml`, `[target.'cfg(all())']` so it joins rather than replaces any
      per-target rustflags.**
- [x] Repro files are keyed by `(seed, profile, ops)`.
- [x] Once deterministic: re-verify issue 11's findings and re-pin the surviving classes by seed.
      **Unblocked by issue 17 (A15) and done 2026-08-05 — see "Criterion 5 discharged" below.
      Nothing survives to re-pin: every issue-11 class is gone at the configs that produced it.**

## References

- `frogdb-server/crates/server/tests/common/workload_runner.rs` — `run_workload_capturing`.
- `frogdb-server/crates/server/tests/common/sim_harness.rs` — `build_sim` (seeding is correct).
- `frogdb-server/crates/server/tests/common/repro.rs` — `repro_path`.
- `frogdb-server/crates/server/tests/concurrency_workload.rs` — `seed_sweep_nightly`,
  `mod regressions`.
- Issue 11 — the findings whose seed pinning this invalidates.
- Issues 12, 13, 15 — the three classes re-verified under this constraint.
- [`../../audit/determinism-audit-2026-08-02.md`](../../audit/determinism-audit-2026-08-02.md) —
  exhaustive product-code determinism audit: 59 class-A sites, all three suspects above confirmed
  plus unseeded chaos injection and hash-order in client-visible replies, with a cheapest-first
  remediation order (R0–R12) mapped onto the acceptance criteria.

## Progress (2026-08-02)

Audit: `.scratch/concurrency-testing/audit/determinism-audit-2026-08-02.md` — 59 nondeterminism
sites, classified, with a remediation order R0–R12. Steps R0–R4 have landed; §6.1 of the audit
carries the measured before/after digest table.

| Commit | Step | What |
|---|---|---|
| `12cca673` | R0, R1, R2 | The run-twice assertion + digest helper; `repro_path(seed, profile, ops)`; `--cfg tokio_unstable`; seeded chaos injector |
| `b48fd3a7` | R3 | Blocking deadlines moved to the timer's clock end to end (`server/connection/blocking*`, `core/shard/{blocking,wait_queue,message}`) |
| `00dfb0ab` | R4 | One `frogdb_types::clock::now()` seam for the whole expiry domain; the active-expiry budget stops measuring real CPU time; `expiry.rs` takes a single paired `Instant`/`SystemTime` sample |
| `faaf7f2d` | (not in the plan) | Seeded skiplist level heights — the audit had filed this class B, but the height is observable through `memory_size()`, and it was the last divergence in the MultiWaiter digest |

Not started: R5–R11 (clock-seam lint + mechanical sweep, `biased;` select loops, seeded command
RNG, hash-order determinism, `spawn_blocking`/thread inlining, `frogdb-net` bypasses).

**The remaining blocker for criterion 5 is A15**, filed as issue 17: `XADD *` mints stream IDs from
`SystemTime::now()`, and no virtual wall clock exists in the codebase, so every profile that
generates streams (Mixed, TxHeavy) still produces a different history per run.
*(Resolved — issue 17 landed as `8b62120f`; see below.)*

## Criterion 5 discharged (2026-08-05)

A15/issue 17 landed (`8b62120f`, `frogdb_types::clock::system_now()`), which un-ignored the two
stream-generating determinism pins. Re-verified on the current tree, local mode, no product
changes.

### The determinism contract holds for all three pinned profiles

`cargo nextest run -p frogdb-server --features turmoil -E 'test(/determinism/)'` — 3/3 pass:

```
PASS [0.280s] concurrency_workload::determinism::run_is_reproducible_mixed_seed_0
PASS [0.468s] concurrency_workload::determinism::run_is_reproducible_txheavy_seed_3
PASS [1.067s] concurrency_workload::determinism::run_is_reproducible_multiwaiter_seed_10
```

Mixed and TxHeavy are the two that were `#[ignore]`d for A15, so the run-twice digest now covers
every profile the pins name.

### Re-verification of issue 11's findings: nothing survives to re-pin

`FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly`
— the exact configuration that produced issue 11's Findings B and C — is now **clean**, 80/80
seed×profile runs:

```
seed_sweep_nightly: 80 seed(s), 0/964 WGL-eligible key(s) downgraded to conservation-only
  (downgrade ratio 0.0000)
seed_sweep_nightly: exact FIFO attributed 86/88 journaled registration(s) (ratio 0.9773),
  judged 45 of 398 served blocking pop(s), 17 pair(s) compared,
  0 run(s) with a truncated registration journal
```

The same command previously reported 5 failures of 80 (three TxHeavy WATCH false-negatives, two
MultiWaiter ZSet non-linearizability reports). All five are accounted for by fixes that have since
landed — issue 12 (product), issue 13 (checker), issue 15 (model) — so the clean sweep is the
expected result, not a sampling fluke. **There is no surviving issue-11 class to pin by seed**,
which is why this criterion closes without adding a `regression_*_seed_N` test.

Note the WGL downgrade ratio is 0.0000: every eligible key got a real linearizability check, so
the clean pass is not the silent-degradation failure mode issue 41 guards against.

### Residual gap this surfaced (not a blocker for criterion 5)

The FIFO coverage line is healthy in aggregate but shows two things worth tracking as a follow-up
to issue 16 rather than to this issue:

- **Attribution loss of 2/88.** Two runs logged `FIFO coverage gap: 0/1 journaled registration(s)
  attributed … journal complete=true`. The journal was *not* truncated, so this is the
  attribution path — a client's pop count on a key disagreeing with its registration count there —
  not capture loss. Small, but it means the exact checker still silently declines to judge a key
  now and then for a reason nobody has traced.
- **Only 45 of 398 served blocking pops are judged, over 17 compared pairs in 80 runs.** Most
  served pops never parked (they found data present), so they legitimately carry no wake-order
  information — but it does mean the FIFO invariant rests on a thin sample per sweep. Worth
  confirming the generator produces enough genuinely-contended waiters to make the check
  load-bearing.

### What keeps this issue open

Criterion 5 was the last checkbox, but the audit's remediation plan is not finished: **R5–R11 are
still not started** (clock-seam lint + mechanical sweep, `biased;` select loops, seeded command
RNG, hash-order determinism, `spawn_blocking`/thread inlining, `frogdb-net` bypasses). The three
determinism pins passing is evidence for three specific configurations
(`(seed, profile, clients, ops, shards)` = Mixed/0/4/30/2, MultiWaiter/10/4/30/2,
TxHeavy/3/4/60/2), in-process, not a general guarantee:

- **`BlockingHeavy` has no determinism pin at all** — the one profile of the four that is never
  checked for reproducibility, and the profile that produced the OPS=150 `seed 7` failure in issue
  11's post-fix table.
- The pins are **run-twice-in-one-process**. Cross-process reproducibility was verified by hand
  when issue 17 landed but nothing asserts it on an ongoing basis.
- Determinism at 30–60 ops does not imply determinism at the nightly default of 150.

Recommend this issue stay open for R5–R11 and the `BlockingHeavy` pin, with the "findings cannot
be pinned by seed" warning in issue 11 now downgraded: seed pinning is sound for the pinned
configurations, and should be treated as unproven elsewhere until the above closes.
