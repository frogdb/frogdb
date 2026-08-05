# 17 — `XADD *` stream IDs come from the real wall clock, so any stream profile is unreproducible

Status: done
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

- [x] `run_is_reproducible_mixed_seed_0` and `run_is_reproducible_txheavy_seed_3` pass un-ignored,
      in-process and across processes.
- [x] Production behaviour is unchanged: outside a paused runtime the value is
      `SystemTime::now()`, so `XADD *` IDs, `EXPIRETIME`, and `TIME` still report real time.
- [ ] The seam is enforced by a lint, not by convention. **Deferred to R5** (see Resolution) —
      not blocking this issue's close.

## References

- `.scratch/concurrency-testing/audit/determinism-audit-2026-08-02.md` — A15 (and A17, the Lua
  time builtins, which the same seam would close).
- Issue 14 — the parent determinism issue; criterion 5 is blocked on this.
- `frogdb-server/crates/types/src/clock.rs` — the `Instant`-side seam this would mirror.

## Resolution

Took option 1 from "Shape of a fix": `frogdb_types::clock::system_now()`
(`frogdb-server/crates/types/src/clock.rs`) sits alongside `clock::now()`. It latches an
`(Instant, SystemTime)` pair the first time it's called and answers every later call as
`latched_system + (clock::now() - latched_instant)` — real time in production (`clock::now()`
is real time too), but under a paused tokio runtime it only advances when the runtime's timer
does, same as `now()`. A test-only `reset_system_epoch(SystemTime)` re-latches the pair; the
turmoil harness calls it once per simulated server (`sim_helpers.rs`'s
`real_frogdb_server_fake_persistence`, mirroring the existing `FakeWalRegistry::clear()` /
`ChaosConfig::with_seed(seed)` per-run resets) so that the determinism harness's run-twice
digest doesn't inherit the first run's real-time reading — without the reset, the *second*
in-process run would drift from the first by whatever real wall-clock time separated them.

All three A15 call sites now read the seam:
- `types/src/types/stream.rs` `StreamId::generate` (`XADD *` auto-ID) — `SystemTime::now()` →
  `crate::clock::system_now()`.
- `types/src/types/stream.rs` `ClaimClock::sample` (`XCLAIM`/`XAUTOCLAIM` idle-time math) — same
  swap. Also fixed a paired bug found while here: `sample()`'s monotonic half read raw
  `Instant::now()` instead of `clock::now()`, which would have desynced from the now-virtualized
  wall-clock half under a paused runtime — exactly the mismatched-clock-domain bug this module's
  doc comment warns against.
- `types/src/types/mod.rs` `Expiry::to_instant` (absolute-`EXPIRETIME`-style replies) — same swap.

Added unit tests under `#[tokio::test(start_paused = true)]` + `tokio::time::advance(..)`:
`clock.rs`'s `tests` module (`system_now_tracks_the_paused_clock`,
`system_now_reproduces_across_resets`) and `stream.rs`'s `id_generation_tests` module
(`generate_tracks_the_paused_clock`, `generate_reproduces_across_resets`,
`generate_bumps_sequence_within_same_millisecond`) — the latter drives `StreamId::generate`
itself through a stepped clock, closing determinism issue 14 criterion 5 for the stream family.

`run_is_reproducible_mixed_seed_0` and `run_is_reproducible_txheavy_seed_3`
(`frogdb-server/crates/server/tests/concurrency_workload.rs`) are un-ignored and verified passing
both in-process (the test's own run-twice digest) and across two separate
`cargo nextest run -p frogdb-server --features turmoil` process invocations.

**Acceptance criteria 1 and 2: met and verified.** Criterion 3 ("the seam is enforced by a lint,
not by convention") is **not** done here: the audit's R5 step (`just lint-clock-seam`, "not
started" as of the audit) is a separate, much larger sweep — dozens of `SystemTime::now()` call
sites across ~15 crates (ACL logging, snapshot metadata, telemetry, WAL flush timestamps, etc.),
most of which are legitimately real wall-clock reads outside any deterministic-test path and
were never part of A15's three cited call sites. Writing a `SystemTime::now()`-forbidding gate
scoped only to `frogdb-types` would be trivial but wouldn't catch a new stream/expiry-path
regression added to `frogdb-commands` or `frogdb-server`, which is the case that matters; a gate
with real teeth needs R5's full allowlist work. Left as a follow-up under R5, not blocking this
issue's close — the reproducibility bug A15 exists to fix (issue 14 criterion 5, the two
un-ignored tests) is resolved and verified.

Verification: `just check frogdb-types`, `just test frogdb-types` (373/373), `just lint
frogdb-types` (clean, `-D warnings`), `just fmt frogdb-types` / `just fmt frogdb-server` (no
diff), `just check frogdb-server`, `cargo clippy -p frogdb-server --features turmoil --tests --
-D warnings` (clean), `cargo nextest run -p frogdb-server --features turmoil -E
'test(/determinism/)'` (3/3, run twice as separate processes), `cargo nextest run -p
frogdb-server --features turmoil -E 'test(/stream/) or test(/xadd/) or test(/claim/)'`
(16/16).
