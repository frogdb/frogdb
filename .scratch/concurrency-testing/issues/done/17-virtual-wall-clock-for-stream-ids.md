# 17 — `XADD *` stream IDs come from the real wall clock, so any stream profile is unreproducible

Status: needs-triage
Type: bug
Origin: determinism remediation R0–R4 (2026-08-02) — the one divergence class left in the
run-twice digest after every in-scope step landed.

## What happens

`XADD key *` mints its ID from `SystemTime::now()` (`types/src/types/stream.rs:126,496`,
`types/src/types/mod.rs:502` — audit item **A15**). The ID is the reply *and* the sort key, so a
real millisecond lands directly in the recorded history. Two runs of the same workload therefore
differ at the first `XADD`:

```
A[6] op[6] t=6 kind=Return client=2 fn=xadd args=[{t17}st1,*,f0,jbp] result=1785711185292-0
B[6] op[6] t=6 kind=Return client=2 fn=xadd args=[{t17}st1,*,f0,jbp] result=1785711185393-0
```

Every other class that reached the digest has been closed: blocking deadlines and the whole expiry
domain now read the timer's clock (issue 14, commits `b48fd3a7` / `00dfb0ab`), the chaos injector
and skiplist level heights are seeded. `seed 10 / MultiWaiter` is byte-identical in-process and
across processes. `Mixed` and `TxHeavy` are not, and generating streams is the only reason.

## Why it matters

- `concurrency_workload::determinism::run_is_reproducible_mixed_seed_0` and
  `..._txheavy_seed_3` are `#[ignore]`d on this alone.
- These are the two broadest profiles — Mixed covers all six type families and TxHeavy covers
  WATCH/EXEC — so issue 14's criterion 5 ("re-verify issue 11's findings and re-pin the surviving
  classes by seed") cannot be satisfied for the profiles that actually carry the findings.

## Shape of a fix

Nothing in the codebase virtualizes wall-clock time; the R4 seam (`frogdb_types::clock::now()`)
covers `Instant` only, because tokio's paused clock has no `SystemTime` counterpart. Options, in
rough order of cost:

1. A `frogdb_types::clock::system_now()` seam alongside `now()`, defaulting to `SystemTime::now()`
   and, under a paused runtime, deriving the value from a fixed epoch plus the virtual elapsed time
   (`tokio::time::Instant::now()` minus a recorded runtime-start reading). Same shape as R4, same
   "identical in production" property, and it would also cover A17's Lua `os.time`/`os.clock` and
   the absolute-`EXPIRETIME` replies.
2. Restrict the fix to stream ID generation by injecting the ID source into the stream type. Cheaper
   but leaves every other `SystemTime` reader nondeterministic.

Whichever is chosen, R5's proposed `just lint-clock-seam` grep gate should be extended to
`SystemTime::now` so the next reader has to opt out deliberately.

## Acceptance criteria

- [ ] `run_is_reproducible_mixed_seed_0` and `run_is_reproducible_txheavy_seed_3` pass un-ignored,
      in-process and across processes.
- [ ] Production behaviour is unchanged: outside a paused runtime the value is
      `SystemTime::now()`, so `XADD *` IDs, `EXPIRETIME`, and `TIME` still report real time.
- [ ] The seam is enforced by a lint, not by convention.

## References

- `.scratch/concurrency-testing/audit/determinism-audit-2026-08-02.md` — A15 (and A17, the Lua
  time builtins, which the same seam would close).
- Issue 14 — the parent determinism issue; criterion 5 is blocked on this.
- `frogdb-server/crates/types/src/clock.rs` — the `Instant`-side seam this would mirror.
