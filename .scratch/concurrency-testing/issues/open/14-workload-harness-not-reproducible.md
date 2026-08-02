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

- [ ] Two runs of the same `Workload` (same seed/profile/clients/ops/shards) produce byte-identical
      histories, in-process and across processes. Pin it with a test that runs one workload twice
      and asserts the histories are equal — this is the harness's core contract and nothing else
      currently checks it.
- [ ] The nondeterminism source(s) are identified and named in this issue (hash iteration order /
      real clock / tokio select), not just papered over.
- [ ] `--cfg tokio_unstable` is set for turmoil test builds (via `.cargo/config.toml` or the `just`
      recipes), so turmoil can seed tokio's RNG and forward host panics as test failures.
- [ ] Repro files are keyed by `(seed, profile, ops)`.
- [ ] Once deterministic: re-verify issue 11's findings and re-pin the surviving classes by seed.

## References

- `frogdb-server/crates/server/tests/common/workload_runner.rs` — `run_workload_capturing`.
- `frogdb-server/crates/server/tests/common/sim_harness.rs` — `build_sim` (seeding is correct).
- `frogdb-server/crates/server/tests/common/repro.rs` — `repro_path`.
- `frogdb-server/crates/server/tests/concurrency_workload.rs` — `seed_sweep_nightly`,
  `mod regressions`.
- Issue 11 — the findings whose seed pinning this invalidates.
- Issues 12, 13, 15 — the three classes re-verified under this constraint.
