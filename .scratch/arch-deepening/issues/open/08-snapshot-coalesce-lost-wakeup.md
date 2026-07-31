# 08 — Close the SnapshotScheduler coalesce lost-wakeup window

Status: ready-for-human

## What to build

Pre-existing design race, preserved deliberately in round-8 (P06 kept exact `SeqCst` semantics):
between a `request()` observing `in_progress == true` and its `scheduled.store(true)`, the
running Snapshot can finish and consume nothing — a `Coalesced` BGSAVE SCHEDULE occasionally
arms no follow-up Snapshot. The contract should be: after a successful
`request_snapshot()`, a Snapshot that *begins after the request* is guaranteed (either the
started one, or a rescheduled run).

The scheduler is now a pure type with storm tests (`scheduler.rs`), so the fix and its proof
are self-contained: e.g., re-check `in_progress` after `scheduled.store` and claim the slot if
the runner exited (CAS loop), or fold request/finish into one CAS-ordered protocol. The storm
test currently *avoids* asserting `!is_scheduled()` at quiescence because of this window — the
fixed version should assert the guarantee instead.

## Acceptance criteria

- [x] `request()` guarantees a post-request Snapshot begins (no lost wakeup)
- [x] Storm test extended to assert the guarantee at quiescence (currently deliberately weakened)
- [x] Interleaving unit test pinning the exact former lost-wakeup window now passes
- [x] No behavior change to `try_begin`/`finish_and_maybe_rebegin` callers; coordinator tests green

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report; `.scratch/arch-deepening/proposals/06-snapshot-scheduler.md`.

## Comments

### 2026-07-20 — implemented

Closed the window in `SnapshotScheduler::request` (`crates/persistence/src/snapshot/scheduler.rs`)
with a **double-check protocol**. The naive body was `if in_progress { scheduled.store(true) }`;
the race is that between the `in_progress` load and the `scheduled.store`, the observed runner can
reach `finish_and_maybe_rebegin` and run its `scheduled.swap(false)` *before* our store lands,
consuming nothing, then exit — stranding `scheduled == true` with no runner.

New protocol (all atomics `SeqCst`, so there is a single total order `S`):

1. `scheduled.store(true)` — arm the follow-up (call it P1).
2. Re-load `in_progress` (P2).
   - **If P2 sees `true`:** the runner active at P2 has not yet executed its finish-store
     (`finish` does `in_progress.store(false)` *before* `scheduled.swap`). So that finish-swap is
     ordered after P2, hence after P1, and is guaranteed to observe our flag — *unless* a peer
     already consumed it, in which case that peer owns the run. Either way a post-request run is
     guaranteed → `Coalesced`.
   - **If P2 sees `false`:** the runner exited and may have missed our flag. We take ownership:
     `scheduled.swap(false)`. If it returns `true` we reclaimed our own flag and *must* run, so
     `try_begin`; if `try_begin` fails a fresh save already claimed the slot *after* our request
     (post-request run exists) → `Coalesced`. If the swap returns `false`, a peer already consumed
     our flag and owns the post-request run → `Coalesced`.

**Ordering argument for why the window is closed:** every `scheduled = true` is consumed by exactly
one `swap` that returns `true`, and every such consumer either reschedules (finish path) or begins
(request path) a run — no armed flag is ever dropped. A `Coalesced` return is only produced when
some run is provably ordered after P1 in `S`. The former lost wakeup (arm lands after the observed
runner's swap and its exit) is now caught by the P2==false branch, which reclaims and runs.

The tail (`arm_follow_up`) is split into a `pub(super)` method purely so a deterministic test can
enter it with the runner *already exited* — the exact interleaving `request`'s black-box re-load
would otherwise hide.

**Tests** (`crates/persistence/src/snapshot/tests.rs`, all green — `just test frogdb-persistence
test_scheduler`, 11 passed):
- `test_scheduler_arm_follow_up_after_runner_exit_starts` — new deterministic pin of the former
  window; asserts the delayed arm `Started`s the run and leaves no dangling schedule flag.
- `test_scheduler_concurrent_request_storm` — strengthened: 500 × 6-thread randomized interleavings
  now assert `!is_scheduled()` at quiescence (the guarantee), in addition to slot-release, epoch
  uniqueness/contiguity, and single-runner invariants.
- Existing coordinator/handshake tests unchanged and green (`just test frogdb-persistence snapshot`,
  35 passed) — `try_begin`/`finish_and_maybe_rebegin` behaviour and callers untouched.
