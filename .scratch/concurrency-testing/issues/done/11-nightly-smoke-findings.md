# 11 — Nightly-tier smoke testing surfaced real concurrency invariant violations

Status: done
Landed: findings split into issues 12–16, 2026-08-02
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

- [x] Root-cause Finding A (MultiWaiter exactly-once-delivery loss under sustained load) — likely in
      the blocking-pop wake/consume path (`frogdb-server/crates/core` blocking-list waiter
      machinery). Given how reliably it reproduces (`FROGDB_CONCURRENCY_OPS_PER_CLIENT=110
      FROGDB_CONCURRENCY_SEEDS=5 just concurrency-nightly`, or replay any of the repro files
      generated during the bisection — not preserved past the testbox session, so a fresh repro run
      is needed), this should be one of the cheaper items to reproduce and fix.
- [x] Investigate the new TxHeavy WATCH false-negative (Finding B, seed 3 at
      `ops_per_client=60`) against the existing WATCH false-negative fix that pinned
      `regression_crossshard_watch_false_negative_seed_8` — determine whether it's the same root
      cause reachable via a different path, or a distinct gap.
      Repro: `FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20
      just concurrency-nightly` (TxHeavy profile, seed 3).
      **Done 2026-08-02** — a distinct gap, filed as issue 12 (re-WATCH resets the version
      snapshot). One of the three reported seeds (14) turned out to be a *checker* false positive,
      filed as issue 13. See the re-verification section below.
- [x] Investigate the MultiWaiter FIFO wake-order violation (Finding B, seed 5 at same params).
      **Done 2026-08-02 — not a product bug.** The exact wake-order checker had no registration
      ordinals for the flagged waiters and fell back to its invoke-order proxy, which flags nested
      invoke intervals; filed as issue 16. See the re-verification section below.
- [x] Investigate MultiWaiter ZSet non-linearizability (Findings B/C — reproduces at both
      `ops_per_client=30` seed 1 and `ops_per_client=60` seed 19, and `ops_per_client=150` regime
      too). Given it reproduces at the per-PR-vetted `ops_per_client=30`, this is reachable with a
      comparatively short/cheap repro and should be prioritized for root-causing.
      **Done 2026-08-02 — not a product bug.** The `ZSetModel` BZPOPMAX tie rule is inverted;
      filed as issue 15. See the re-verification section below.
- [x] Once Finding A is fixed, revisit whether `concurrency-nightly`'s default `ops_per_client`
      (see "Resolution" below) can be safely raised back toward the harness's original coded
      default, to get longer per-history coverage in the nightly tier. **Done** — raised 75 → 150
      (the harness's coded default) in both places, 2026-07-31.

## Resolution shipped in phase 5 (CI wiring) — SUPERSEDED 2026-07-31

> Superseded: the `ops_per_client = 75` cap described below was a workaround for what turned out to
> be a harness defect, not a product bug. Both defaults are back at 150. See
> "Finding A — root cause found and fixed" at the end of this file.


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

## Finding A — root-cause update (2026-07-23, testing-improvements issue 07)

Investigated while building the shuttle exactly-once guard (issue 07). **Finding A is NOT the
serve-vs-timeout deferred-delivery race** it was hypothesized to be, and remains **open**.

### What was checked

A separate, real hypothesis was pursued and a fix shipped for it: the *serve-vs-timeout race* on
the deferred blocking-serve path — where the shard pops an element and `oneshot::send()`s it to a
waiter whose server-side coordinator already timed out and dropped the receiver, losing the popped
element (`send`-`Ok` does not imply delivery). That race is real; the fix (acknowledged
`UnregisterWait` handshake making the shard the single serialization point) shipped on branch
`worktree-agent-a3a766999e8e4f424` and is guarded deterministically by the new shuttle model
(`frogdb-server/crates/core/tests/concurrency.rs`, "Blocking-pop exactly-once conservation").

**But that fix does not clear Finding A.** With the handshake in place, MultiWaiter seeds 1–5 at
OPS=100 still fail the exactly-once check identically.

### Evidence that Finding A is a different mechanism

Deterministic local repro (MultiWaiter seed 1, 4 clients, OPS=100, 2 shards, via
`run_workload_capturing`), instrumented with debug prints on every deferred-serve delivery,
restore, and deadline-skip:

- The lost element (`[100,116]="dt"`, RPUSH'd to `{t4}ls0` at op 400, list length 78 at push time)
  produced **zero** deferred-serve events — no `SERVE_OK`, no restore, no deadline-skip. The
  deferred blocking-serve path the handshake fixes was **never involved** in this loss.
- `"dt"` appears in **no operation's recorded result** anywhere in the history, and is **absent from
  the final store**. The only ops touching `{t4}ls0` by first-arg are RPUSHes plus one immediate
  BLPOP that returned a *different* element.
- Final list length was **nondeterministic across identical-seed runs** (observed 69 vs 70),
  i.e. ~8–9 pushed elements left the list via a path recorded in **no** operation result.

This points at a lower-level loss in the blocking-list consume/record path under sustained
concurrency (candidates: multi-key LMPOP/BLMPOP pop-and-record, or an internal list-element drop),
**distinct** from the deferred serve-vs-timeout race. It was not fully root-caused; the mechanism
that removes `"dt"` without recording a delivery is still unidentified.

### Status

- Finding A: **still open.** Not fixed. The serve-vs-timeout handshake shipped alongside is a
  genuine correctness improvement for a *different* (real) race, not a fix for this.
- Nightly `ops_per_client` cap stays at **75** — do not raise it; Finding A still reproduces above
  ~90.
- Next step for whoever picks this up: instrument the actual element-removal path for `{t4}ls0`
  (every store mutation, not just deferred serves) in the seed-1/OPS=100 repro to catch the op (or
  internal path) that removes an element without recording a delivery. The nondeterministic final
  length is the strongest lead — confirm whether it is a genuine loss or a drainer-readback timing
  artifact (`DRAIN_SETTLE_MS`) at high op counts before assuming a product bug.

## Finding A — root cause found and fixed (2026-07-31)

**Finding A is not a product bug. It is a defect in the turmoil workload harness's final-state
readback.** Fixed; nightly `ops_per_client` restored to 150 in both places.

### Root cause

`frogdb-server/crates/server/tests/common/workload_runner.rs` — the `"drainer"` sim client slept a
fixed `DRAIN_SETTLE_MS = 30_000` sim-ms **from sim start** and then `LRANGE`d every key, treating
the result as the workload's *final* state. The `WAITQUEUE` prober was on the same wall-clock
budget.

A `MultiWaiter` client script spends `producer_think(rng) = 120..400` sim-ms before each producer
push and ~45% of its ops are producers, so a script's duration grows roughly **0.26 sim-s per op**.
Past `ops_per_client ≈ 90` (`90 × 0.26 ≈ 23s` plus consumer blocking waits) the scripts run past
the 30 s mark, so the readback happened **mid-workload**. Every element RPUSH'd after the readback
was therefore neither delivered (never popped) nor present in the captured "final state" — which
`check_exactly_once_delivery` correctly reported as `LostElement`.

This explains every previously-unexplained observation in the 2026-07-23 update:

- the sharp threshold at ~90 ops and the near-determinism above it — it is a duration threshold,
  not an interleaving one, and the bisection table (30 pass, 75 pass, 90 → 4/5, 110 → 5/5, 150 →
  3/3) is exactly the shape of "scripts crossing a fixed wall-clock";
- the lost element traversing **zero** deferred-serve events — the deferred path was never involved
  because nothing had consumed the element yet;
- the **nondeterministic final list length across identical-seed runs** — the `LRANGE` raced
  in-flight pushes.

Direct proof, seed 0 / OPS=110, instrumented: `DBG_DRAIN_READ t=30000ms` followed by ~40 further
`DBG_LATE_OP ... rpush` lines, with `DBG_CLIENT_DONE` at t=32672 ms, t=33401 ms and later.

### Fix

`workload_runner.rs`: the drainer and the `WAITQUEUE` prober now key off an `AtomicUsize`
client-completion latch instead of a wall-clock deadline. The readback is taken
`DRAIN_SETTLE_MS = 500` sim-ms after the **last client script finishes**; a `DRAIN_DEADLINE_MS =
240_000` ceiling makes a wedged run fail loudly with an actionable assert instead of silently
capturing early. `SimConfig` gained a `simulation_duration` field so the workload runner can raise
turmoil's virtual-time ceiling to 300 s (virtual time, so an unused ceiling costs no wall-clock).

Side effects: runs no longer pad out to 30 sim-seconds (the prober stops when clients do), and the
final-state capture is now deterministic across identical-seed runs.

### Regression guard

`regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0` in
`frogdb-server/crates/server/tests/concurrency_workload.rs` — seed 0, `MultiWaiter`, 4 clients,
`ops_per_client = 110` (load-bearing; at the per-PR tier's 30 the bug is invisible). Verified to
**fail** with the harness fix stashed:
`"exactly-once delivery: element [118] (pushed by op 234) was neither delivered nor in final state"`.

### Sweep results after the fix (local)

| config | exactly-once violations | remaining failures |
|---|---|---|
| OPS=110, SEEDS=25 (100 runs) | **0** | seed 3 + seed 10 TxHeavy WATCH false-negative, seed 6 MultiWaiter FIFO wake order (Finding B) |
| OPS=150, SEEDS=25 (100 runs) | **0** | seed 7 BlockingHeavy, seed 3 + seed 13 MultiWaiter ZSet non-linearizable (Finding C); WGL downgrade ratio 0.0178 |
| OPS=75, SEEDS=25 (baseline, the old shipped cap) | 0 | seed 3 TxHeavy WATCH false-negative (Finding B) |

The OPS=75 baseline is the important control: the nightly tier was **already** red on Finding B at
the capped value, so the cap was buying nothing.

### Consequences

- `seed_sweep_nightly`'s `env_override("FROGDB_CONCURRENCY_OPS_PER_CLIENT", …)` and the `just
  concurrency-nightly` `OPS` default are both back at **150**; the workaround comments are
  condensed to a one-liner pointing at this fix and the pinned regression.
- Findings B and C remain open and are expected to keep surfacing in nightly — that is the tier
  working as designed.
- Blocking-path failure modes are now specced in
  `specs/blocking.md` (FM-BLOCKING-001..005).

## Findings B and C — post-harness-fix re-verification (2026-08-02)

Re-ran every seed/profile/OPS combo cited in Findings B and C on the fixed harness (local mode, no
product changes). **Only one of the three violation classes is a product bug.** Three new issues
came out of it: 12 (product), 13 (checker), 15 (model) — plus 14, which invalidates the way this
issue reports failures by seed.

### The seed numbers in Findings B and C are not stable

The harness is **not reproducible**: re-running the same `Workload` (same seed, profile, clients,
ops, shards) yields a different history and a different verdict every time, in-process and across
processes. Filed as **issue 14**, with digests. Consequences for this issue:

- a per-combo verdict is a *sample*, not a fact — every verdict below is annotated with how many
  repeats it survived;
- the seed labels in Findings B/C identify sampling draws, not bugs. The same ZSet class appeared at
  seeds 1, 10, 13 and 19 in different sweeps, and the FIFO class at seeds 5 and 6;
- `target/concurrency-repros/<seed>.json` does not replay the failure it was written for.

### Per-combo verdicts

| combo (seed / profile / OPS) | cited as | verdict on the fixed harness |
|---|---|---|
| 3 / TxHeavy / 60 | Finding B (i) | **FAIL** — WATCH false-negative, 5 of 5 repeats. Real product bug → issue 12 |
| 5 / MultiWaiter / 60 | Finding B (ii) | **FAIL, rare** — FIFO wake order in 1 of 6 repeats; 0 of 40 further repeats. Checker false positive → issue 16 |
| 19 / MultiWaiter / 60 | Finding B (iii) | **FAIL** — ZSet non-linearizable (2 keys), first attempt. Model artifact → issue 15 |
| 1 / MultiWaiter / 30 | Finding C | **PASS** — the class re-sampled at seed 10 in the same sweep; same model artifact → issue 15 |

Full sweeps behind those verdicts, verbatim:

`FROGDB_CONCURRENCY_OPS_PER_CLIENT=60 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly`
— 5 of 80 seed×profile runs:

```
seed 3 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 24459 committed
  though op 24406 wrote watched key [123, 116, 49, 125, 107, 118, 49] after watch op 24403"]
seed 14 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 29575 committed
  though op 29569 wrote watched key [123, 116, 48, 125, 107, 118, 48] after watch op 29564"]
seed 16 (TxHeavy) FAILED: ["WATCH false-negative: watch false-negative: exec op 30468 committed
  though op 30464 wrote watched key [123, 116, 48, 125, 107, 118, 48] after watch op 30459"]
seed 13 (MultiWaiter) FAILED: ["key {t12}zs0 (ZSet) not linearizable (problematic ops [38539, …])"]
seed 19 (MultiWaiter) FAILED: ["key {t13}zs1 (ZSet) not linearizable (problematic ops [41412, …])",
  "key {t12}zs0 (ZSet) not linearizable (problematic ops [41416, …])"]
```

`FROGDB_CONCURRENCY_OPS_PER_CLIENT=30 FROGDB_CONCURRENCY_SEEDS=20 just concurrency-nightly`
— 1 of 80:

```
seed 10 (MultiWaiter) FAILED: ["key {t12}zs0 (ZSet) not linearizable (problematic ops [18605, …])"]
```

No `exactly-once delivery` violation appeared in either sweep — Finding A stays fixed.

### Class-by-class outcome

**(i) TxHeavy WATCH false-negative — REAL, product bug → issue 12.** A second `WATCH k` on a
connection already watching `k` overwrites the version snapshot taken by the first, so writes
between the two WATCHes are forgotten and EXEC commits. Redis' `watchForKey()` no-ops on a
re-watch; `TransactionState::watch_key` does a `HashMap::insert`. Distinct from the pinned
cross-shard `regression_crossshard_watch_false_negative_seed_8`. Trace in issue 12.

**(i-b) Seed 14's WATCH false-negative — NOT a product bug → issue 13.** The flagged "writer" is a
`DEL` that returned `0`; `written_keys_of`'s default branch counts a DEL as a write regardless of
result, and the same hole exists for DELs inside another client's committed EXEC. The
no-false-positive claim this checker rests on is what breaks.

**(ii) MultiWaiter FIFO wake order — reproduces, but NOT a product bug → issue 16.** Seed 5 / OPS=60
reproduced `FIFO wake order violated on key [123,116,52,125,108,115,48]` (= `{t4}ls0`) in 1 of 6
repeats, seed 6 / OPS=110 in 1 of 6, seed 3 / OPS=110 in 1 of 4; every hit flagged adjacent op ids
(31 vs 29; 1776 vs 1775; 2698 vs 2697), i.e. near-simultaneous waiters. Instrumenting
`WaiterRegistrationOrder::get` on the seed-3 hit settled which path fired: three of the four served
waiters on the key had **no** registration ordinal (`reg_order_len=6` for 12 distinct
`(key, client)` blocking pairs), so `check_fifo_wake_order_exact`'s `all_known` gate was false and
the key fell to the invoke-order proxy — the path its own doc comment calls a possible false
positive. The flagged pair's invoke intervals are nested (`[112,118]` vs `[113,115]`), and
`invoke_time` is a shared record-order counter rather than a clock, so the proxy's premise does not
hold for concurrent waiters at all. The prober's 50 sim-ms cadence versus a few sim-ms of park time
is why ordinals are missing. Filed as issue 16 (checker/harness), not as product.

**(iii) ZSet non-linearizability (Findings B *and* C) — NOT a product bug → issue 15.** The
`ZSetModel` pops the lexicographically *smallest* member on a `bzpopmax` score tie; Redis and FrogDB
pop the *greatest*. The generator emits scores `0..100` over members `m0..m4`, so ties are routine
(the model's "workloads avoid ties" comment is stale). Every reported ZSet violation examined
traces to a tied-score BZPOPMAX where the server answered correctly. Finding C's "genuine sampling
effect" conclusion still holds mechanically — the tier did surface something at `ops_per_client=30`
— but what it surfaced is a model bug.

### What this leaves for issue 11

Findings A and C are resolved (harness defect and model defect respectively); Finding B splits into
issue 12 (product), issue 13 (checker) and issue 16 (checker). Every investigation item is closed
out, so this issue is closed; the follow-up work lives in issues 12–16
(`.scratch/concurrency-testing/issues/`). The overarching lesson is issue 14: until the
harness is deterministic, no
finding here can be pinned by seed, and this issue's own seed-indexed history should be read as
"these classes exist", never "this seed reproduces it".

## Re-confirmation of closure (2026-08-05)

Re-opened for investigation on the stale premise that "Findings B and C remain open". **They do
not** — this issue was closed on 2026-08-02 and nothing here needs further root-causing. Recorded
so the next reader does not re-litigate it. Each fix was verified present in the tree, not merely
asserted by a status header:

| Finding | Verdict | Fix, verified in code |
|---|---|---|
| A — exactly-once loss ≳90 ops | harness defect | `workload_runner.rs` readback latched to client completion; pinned by `regressions::regression_drain_capture_race_multiwaiter_ops_110_seed_0` |
| B(i) — TxHeavy WATCH false-negative | **product bug** | `frogdb-server/crates/txn/src/state.rs` `watch_key` is now `entry().or_insert()` (first-watch-wins), matching Redis `watchForKey()` — issue 12 |
| B(i-b) — seed-14 WATCH report | checker false positive | `written_keys_of` is result-aware and splits EXEC sub-results — issue 13 |
| B(ii) — MultiWaiter FIFO wake order | checker false positive | `check_fifo_wake_order_exact` reports `FifoCoverage` and has no invoke-order fallback — issue 16 |
| C — MultiWaiter ZSet non-linearizable | model defect | `ZSetModel::step` BZPOPMAX tie-break is now lexicographically greatest — issue 15 |

So exactly one of the five classes this issue reported was a product bug (B(i)); three were
harness/checker/model defects and one was a load-dependent readback artifact.

The one live descendant is **issue 14**, whose last open criterion is "once deterministic,
re-verify issue 11's findings and re-pin the surviving classes by seed". Its blocker (audit item
A15, `XADD *` reading real wall-clock time) closed as issue 17 (`8b62120f`), so that criterion is
now actionable — progress recorded in issue 14, not here.
