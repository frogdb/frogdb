# Raft `append` acknowledges log durability without an fsync

Status: ready-for-human
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/11 F10 · MASTER.md §3 (durability)
Score: severity 5 · likelihood 2 · effort 4 · priority 15
Area: frogdb-cluster / Raft log storage

> Re-triaged during filing from `ready-for-agent`. The proposal records `Boundary: n/a`; the
> deliverable is a durability-vs-latency trade-off plus a benchmark, not a test. That is a
> human call, not AFK work.

## Context

openraft treats `log_io_completed(Ok(()))` as "this entry is durable". FrogDB signals it
immediately after a RocksDB write with default `WriteOptions`, i.e. without an fsync. A leader
can therefore commit and act on a topology decision — slot transfer, failover, epoch bump —
that is lost on power failure. That is a consensus-safety violation, not just data loss. The
asymmetry with `save_vote`, which *does* flush, is the tell that this is an oversight rather
than a considered trade-off.

**This is a suspected live defect found by reading, not by test failure — the proposed change
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`crates/cluster/src/storage.rs:333-337` — `self.db.write(batch)` with default `WriteOptions`
(`sync = false`), then `callback.log_io_completed(Ok(()))` immediately. Contrast `save_vote` at
`:290-296`, which *does* `self.db.flush()`. Same asymmetry noted in
`epoch-fold-redesign.md` §2 ("appends use default `WriteOptions` (no per-append fsync)").
Round 1 issue 12 covered the *data-plane* fsync boundary; the Raft log was not in its scope.

The proposal is explicit that test value here is low relative to effort: proving loss needs
filesystem-level fault injection, and a white-box assertion on `WriteOptions` tests the
implementation rather than the behaviour. The honest deliverable is a **source fix plus a
measured benchmark**, not a test.

## What to fix

1. Make `append` durable before signalling the callback — `WriteOptions::set_sync(true)`, or
   batch-and-flush before `log_io_completed`.
2. Measure the cost: benchmark sync-vs-async append and record the numbers in the issue.
3. Expose the trade-off as a config knob if the measured cost justifies one, with the safe
   value as the default.
4. Keep `save_vote` and `append` symmetric so the next reader cannot mistake one for the other.

## Acceptance criteria

- [ ] `append` does not call `log_io_completed(Ok(()))` until the batch is fsynced (asserted at
      minimum by a white-box test on the `WriteOptions`/flush call, which fails today).
- [ ] A benchmark of sync-vs-async Raft append is recorded, with numbers, in this issue.
- [ ] If a knob is added, its default is the durable setting and the non-durable value is
      documented as a consensus-safety trade-off.
- [ ] `storage.rs:333-337` and `:290-296` are consistent in their durability handling.

## Test boundary

The proposal records `Boundary: n/a` — it recommends filing this as a durability *issue*, not as
test work. The closest testable level is 2 (a white-box assertion on the storage call), which is
deliberately weak: a true behavioural proof needs level-5 filesystem-level fault injection, and
the audit judged that effort unjustified relative to simply making the write durable.

## Depends on

nothing to make the fix. A behavioural loss-proof would need a crash/filesystem-fault primitive
— adjacent to issue 02 (I2 — subprocess-SIGKILL crash primitive),
`.scratch/testing-improvements-round2/issues/` — which this proposal did **not** request.
