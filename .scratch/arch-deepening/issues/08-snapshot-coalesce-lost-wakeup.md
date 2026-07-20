# 08 — Close the SnapshotScheduler coalesce lost-wakeup window

Status: needs-triage

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

- [ ] `request()` guarantees a post-request Snapshot begins (no lost wakeup)
- [ ] Storm test extended to assert the guarantee at quiescence (currently deliberately weakened)
- [ ] Interleaving unit test pinning the exact former lost-wakeup window now passes
- [ ] No behavior change to `try_begin`/`finish_and_maybe_rebegin` callers; coordinator tests green

## Blocked by

None - can start immediately

## Source

Round-8 P06 agent report; `.scratch/arch-deepening/proposals/06-snapshot-scheduler.md`.
