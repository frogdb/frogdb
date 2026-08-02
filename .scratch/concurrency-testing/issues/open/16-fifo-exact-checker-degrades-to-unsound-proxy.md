# 16 — exact FIFO checker silently degrades to an unsound invoke-order proxy (checker false positive)

Status: ready-for-agent
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
