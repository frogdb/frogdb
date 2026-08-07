# 16 — exact FIFO checker silently degrades to an unsound invoke-order proxy (checker false positive)

Status: done
Type: bug
Origin: post-harness-fix re-verification of issue 11 Finding B (2026-08-02) — the surviving
MultiWaiter "FIFO wake order violated" reports are **not** a product bug.

## What happened

`check_fifo_wake_order_exact` (`frogdb-server/crates/testing/src/conservation.rs:346`) judges a key
by real registration ordinals only when *every* served waiter on that key has one:

```rust
let all_known = served
    .iter()
    .all(|(_, _, client_id, _)| order.get(&key, *client_id).is_some());
if all_known { /* exact: ascending registration_seq */ } else { /* invoke-order proxy */ }
```

Ordinals come from a mid-run `DEBUG WAITQUEUE` prober that samples every
`PROBE_INTERVAL_MS = 50` sim-ms
(`frogdb-server/crates/server/tests/common/workload_runner.rs:60`). Generated
`MultiWaiter` waiters are parked for only a few sim-ms — the producer push that serves them is
already in flight — so the prober observes almost none of them. Every FIFO verdict therefore comes
from the fallback proxy, which is documented (`conservation.rs:238`) as being able to false-positive.

The proxy is worse than "a narrow race window": it compares `Operation::invoke_time`, which is not a
clock at all. `OperationHistory::next_timestamp`
(`frogdb-server/crates/server/tests/common/sim_harness.rs:151`) is a counter
bumped under the shared history mutex at record time, so `invoke_time` is *recorder-arrival order*.
Two clients invoking `BLPOP` at the same sim instant get consecutive ordinals in whatever order the
sim happened to poll their tasks — carrying no information about the order the server registered
them in. The proxy then demands that serve order match that arbitrary order.

## Evidence

Seed 3 / `MultiWaiter` / `ops_per_client = 110`, one repeat in six (this workload is not
reproducible — see issue 14):

```
FIFO wake order: FIFO wake order violated on key [123, 116, 52, 125, 108, 115, 48]:
  op 2698 (later waiter) served before op 2697
```

`[123,116,52,125,108,115,48]` = `{t4}ls0`. Instrumenting `WaiterRegistrationOrder::get` and dumping
every served blocking pop on that key:

```
DIAG reg_order_len=6
DIAG   seq=Some(1) served_key=Some(b"{t4}ls0")
DIAG blk op   2642 c3 [      1,     10] brpop ["{t4}ls0", "5"] -> Some("{t4}ls0|r1ov")
DIAG   seq=None    served_key=Some(b"{t4}ls0")
DIAG blk op   2697 c2 [    112,    118] blpop ["{t4}ls0", "5"] -> Some("{t4}ls0|02")
DIAG   seq=None    served_key=Some(b"{t4}ls0")
DIAG blk op   2698 c0 [    113,    115] brpop ["{t4}ls0", "5"] -> Some("{t4}ls0|wb8")
DIAG   seq=None    served_key=Some(b"{t4}ls0")
DIAG blk op   2701 c1 [    120,    124] blpop ["{t4}ls0", "5"] -> Some("{t4}ls0|p1p")
```

Three of the four waiters on the key have no ordinal, so `all_known` is false and the key fell to
the proxy. The flagged pair's intervals are *nested* — op 2698 `[113, 115]` sits inside op 2697
`[112, 118]` — i.e. the two invokes are adjacent recorder ordinals with no established ordering, the
exact shape the proxy's own doc comment calls a false positive. Nothing in this history shows the
server serving a later registrant first.

`reg_order_len = 6` against 12 distinct `(key, client)` blocking pairs in the same run quantifies the
coverage gap: the existing "map is non-empty" smoke assert passes while ~everything still runs on the
proxy.

## Suggested fix

Two independent parts; the first is what makes the check trustworthy.

1. **Get ordinals for every waiter.** Options, cheapest first:
   - Drop `PROBE_INTERVAL_MS` far below the park duration — fragile, and a shorter probe interval
     perturbs the very schedule under test.
   - Have the server hand the registration ordinal back to the *waiter* (e.g. a testing-only field on
     the blocking reply, or a `DEBUG` counter read the client can join on), removing the sampling
     race entirely. This is the durable fix and also removes the
     `WaiterRegistrationOrder` "at most one blocking pop per key per client" soundness constraint,
     since the ordinal is then per-operation rather than per-`(key, client)`.
2. **Do not fall back to the proxy for concurrent invokes.** With `invoke_time` being recorder
   order, the fallback can only be sound for waiters whose intervals are strictly disjoint
   (`w[0].return_time < w[1].invoke_time`, i.e. `Operation::precedes`). Restrict the residual path to
   disjoint pairs and skip overlapping ones (report them as *unchecked*, not as violations), or drop
   the fallback and count ordinal-incomplete keys as coverage loss.

Either way, surface coverage: assert (or at least report) the fraction of served blocking pops
judged on the exact path, so a regression to ~0% fails loudly instead of quietly turning the
checker into a random-verdict generator.

## Acceptance criteria

- [ ] Registration ordinals are available for every served blocking pop in a `MultiWaiter` run
      (exact-path coverage reported, and asserted to be 100% or an explicit documented threshold).
- [ ] The invoke-order fallback either no longer exists, or is applied only to pairs whose
      `[invoke, return]` intervals are disjoint.
- [ ] Unit tests in `frogdb-server/crates/testing` pin the false positive: a history with two
      overlapping served waiters and no ordinals is **not** flagged; a history with ordinals showing
      a genuine out-of-order serve **is**.
- [ ] Repeated `MultiWaiter` runs (seed 3 / `ops_per_client = 110`, seeds 5–6 / 60–110) no longer
      report FIFO violations.

## References

- `frogdb-server/crates/testing/src/conservation.rs` — `check_fifo_wake_order` (proxy, with its own
  false-positive note), `check_fifo_wake_order_exact`, `WaiterRegistrationOrder`.
- `frogdb-server/crates/server/tests/common/workload_runner.rs` — `PROBE_INTERVAL_MS`, the
  `DEBUG WAITQUEUE` prober and its CLIENT ID join.
- `frogdb-server/crates/server/tests/common/sim_harness.rs` — `OperationHistory::next_timestamp`,
  the counter behind `invoke_time`.
- Issue 11 — Finding B (ii), where this class was first reported as a possible product bug.
- Issue 14 — the reproducibility defect that makes this class appear and vanish between runs.

## Resolution

Fixed 2026-08-02 (checker-repair batch; finished by session coordinator after agent stalls).
Registration ordinals are now captured event-driven at registration time: `ShardWaitQueue`
keeps an append-only registration journal behind the test-only `wait-queue-log` cargo feature,
read via `DEBUG WAITQUEUE-LOG` (with an explicit truncation marker so a reader can tell
"no more registrations" from "log stopped recording"). The polling probe that missed
short-lived waiters is no longer the source of truth. The checker never silently proxies:
`check_fifo_wake_order_exact` reports `FifoCoverage` in the `InvariantReport` — incomplete
ordinals mean "coverage collapsed: this run proves nothing about wake order", never an
arrival-order verdict in either direction. Self-test pins that incomplete ordinals produce no
FIFO verdict.

## Follow-ups discharged (2026-08-06)

Issue 14's "residual gap" section recorded two open questions about the repaired checker. Both are
now answered. Local mode, no product changes; the checker, the aggregate reporting and the workload
generator changed.

### 1. The ~2% attribution loss is benign-direction only — checker behaviour, not a defect

**Verdict: no product bug, no checker bug. The loss is the checker correctly declining to judge.**

Attribution joins two counts per `(watched_key, client_id)`: journaled park count vs. that client's
completed `BLPOP`/`BRPOP` count on the key. A mismatch means the pair is not judged at all. Reading
the join shows the mismatch has two directions, and only one of them can occur by design:

- **`pops > parks` (extra-pop, benign).** A blocking pop that finds an element already present
  returns immediately and never registers, so the pop is counted on the history side with no park to
  match it. Note timed-out pops are *not* a source of loss: they park (counted on the journal side)
  and complete (counted on the history side), and never appear in the served-order comparison.
- **`parks > pops` (missing-pop, defect).** A `BLPOP` registers at most once and only when it parks,
  so more journaled parks than blocking pops means the journal and the history disagree about
  reality — a wake-accounting bug or a capture/join bug. Nothing in the design produces it.

`FifoCoverage` now carries the two directions as separate counters
(`unattributed_extra_pops`, `unattributed_missing_pops`), including the previously invisible residual
where a `(key, client)` pair journaled parks but recorded no blocking pops at all. The nightly
aggregate reports both, and `SweepSummary::check_fifo_coverage` **hard-fails on any missing-pop
loss** — before the existing attributed-ratio threshold, so the defect direction can never be
absorbed as ordinary coverage loss.

Measured, 10 seeds × 5 profiles at OPS=150 / 4 clients / 2 shards: every unattributed registration
is extra-pop (`unattributed 2 extra-pop / 0 missing-pop`; the 2 are both `BlockingHeavy`). The
per-run `FIFO coverage gap` diagnostic now prints the same split, so a future gap says which
direction it is on sight.

Unit tests in `frogdb-server/crates/testing/src/conservation.rs` pin all three cases
(`fifo_attribution_loss_is_classified_by_direction`,
`fifo_attribution_loss_flags_parks_without_pops`,
`fifo_truncated_journal_reports_no_attribution_split`), plus two in `sweep_summary.rs` for the
hard-fail.

### 2. Judged fraction: 8.1% → 80.9% via a `HighContention` profile

The exact check only judges pops that actually parked, and none of the four existing profiles is
built to force parking, so the judged fraction was falling as ops rose. Added
`Profile::HighContention`: role-split on a **single** hot list key, one producer and
`num_clients - 1` consumers, pushes deliberately under-supplied (~3 pops per push) with spread
producer delays (150–350 ms) and a 1 s consumer timeout. Design follows directly from the join
above — surplus *pops* are harmless (they park, time out, and stay attributable), surplus *pushes*
are what poisons a pair by leaving data sitting for the next pop to find without parking.

Its single key exceeds the per-key WGL op cap and downgrades to conservation-only, which is
accepted: the profile exists for wake-order coverage, and the other four profiles carry
linearizability.

Measured over the same 10-seed sweep (`FROGDB_CONCURRENCY_SEEDS=10`,
`FROGDB_CONCURRENCY_OPS_PER_CLIENT=150`):

| Profile | attributed / registrations | judged / served | ratio | pairs |
|---|---|---|---|---|
| Mixed | 0/0 | 0/0 | — | 0 |
| BlockingHeavy | 0/2 | 0/306 | 0.0000 | 0 |
| TxHeavy | 0/0 | 0/0 | — | 0 |
| MultiWaiter | 44/44 | 32/88 | 0.3636 | 17 |
| **HighContention (new)** | **4500/4500** | **1500/1500** | **1.0000** | **1490** |
| **Pooled (after)** | 4544/4546 | **1532/1894** | **0.8089** | 1507 |
| Pooled (before, 4 profiles) | 44/46 | **32/394** | **0.0812** | 17 |

Every served blocking pop in `HighContention` parks and is judged, and compared pairs go from 17 to
1507 — the FIFO invariant is no longer resting on a thin sample. The profile is pinned into
`seed_sweep_nightly` (250 × 5 = 1250 runs), and the sweep now prints a **per-profile** FIFO line in
addition to the pooled one, because pooled coverage hides which profile stopped contributing.

A per-PR smoke test, `concurrency_workload::high_contention_exact_fifo_judges_most_served_pops`
(seed 0, 4 clients, 20 ops, 2 shards), asserts the journal is complete, attribution is total,
missing-pop loss is zero, and at least half of served pops are judged — so a generator change that
quietly stops producing contention fails in CI rather than in the nightly.
