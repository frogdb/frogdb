# VLL continuation package: wound-wait, SCRIPT KILL, unwind/gather rows, panic restart

Status: ready-for-agent

## Parent

[spec-review-txn-vll-blocking.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
— Finding H1 ("no-wait refusal without priority ⇒ mutual-abort livelock"), Finding H2 ("the
continuation lock has no bounded hold time and no rowed kill path"), Finding H3 ("the cross-shard
atomicity core is explicitly unrowed"), Finding H4 ("FM-VLL-005: panic isolation without a
rollback or invariant-recovery story").

## What is wrong

**H1 — livelock.** FM-VLL-002 refuses a second continuation request outright (`-BUSY … retry`) and
forbids queueing it; FM-VLL-001/004 refuse SCA work the same way. Nothing bounds retry, orders
contenders, or asserts progress. `acquire_continuation`
(`frogdb-server/crates/vll/src/coordinator.rs:371-392`) dispatches the lock request to every shard
before awaiting any ready reply. Two cross-shard scripts over overlapping shard sets can each win a
different shard, each receive `ShardBusy` from the other's, release everything, and retry into the
same interleaving indefinitely. The coordinator's doc comment ("`shards` must be sorted in
ascending order to prevent deadlocks", `coordinator.rs:329,354`) is also misleading: callers do
sort (`connection/scripting/eval.rs:78`), but because acquisition is pipelined rather than
serialized, sorted order is not what buys deadlock freedom — the no-wait refusal is. The
liveness hazard sorted-but-pipelined acquisition trades into is livelock, and it is unrowed.

**H2 — unbounded hold, no kill row.** `acquire_continuation_and_run` bounds acquisition with
`DEFAULT_LOCK_ACQUISITION_TIMEOUT` (4s) but leaves `run` unbounded
(`coordinator.rs:341-349`). A looping cross-shard script holds shard-exclusive locks on every
participant for as long as it runs, and FM-VLL-001/004 refuse every SCA request on those shards for
the whole duration — a node-wide availability event with no rowed escape. `SCRIPT KILL` exists
(`connection/scripting/script.rs:106`) but no VLL row describes its interaction with a held
continuation lock, and the kill path is also an atomicity question: a cross-shard script killed
mid-flight has already applied sub-command writes on sibling shards, and neither `vll.md` nor
`txn.md` states whether those are rolled back. FM-VLL-005 explicitly declines the question
("Outcome variant: n/a — the failure is in execution, not acquisition").

**H3 — atomicity core unrowed.** The `vll.md` scope note (lines 25-30) defers "the scatter phases
themselves (lock-request dispatch failure, phase-2/3 partial failure unwind, gather timeouts)" as
internal, with a generic `-ERR VLL lock acquisition failed` observable. Phase-2/3 unwind *is* the
atomicity contract: an op whose execute succeeded on shard A and failed/timed out on shard B. A
gather timeout resolves an op's outcome (abandons a granted, already-executing op) rather than
merely bounding a wait — wall-clock-as-correctness, the banned anti-pattern. `-ERR VLL lock
acquisition failed` is returned for a possibly-applied op, collapsing the distinction FM-TXN-032
correctly keeps (`-ERR shard unavailable` = never accepted, vs `-ERR shard dropped request` =
accepted, fate unknown). The three existing tests
(`phase1_dispatch_failure_aborts_shards_already_holding_intents`, etc.) already exist and need rows,
not new code.

**H4 — panic isolation, no rollback story.** FM-VLL-005 pins that locks are released and the shard
"answers the next message normally" after an op panics mid-`execute_scatter_part`, but never states
what happened to writes already applied on this shard or sibling shards of a cross-shard op. The
client gets an error-shaped `PartialResult` while some effects may stand, and continuing to serve
means continuing against data structures whose invariants may be broken mid-mutation.

## What to build

1. **Wound-wait (H1).** Add `FM-VLL-006 — two continuation requests collide`, pinning a
   starvation-free rule on continuation-lock collision: the lowest `txid` wins, the higher-`txid`
   holder aborts and retries (progress guaranteed; CockroachDB txn-priority shape). Forcing test:
   two colliding continuations over the same shard set — both terminate, one commits. Correct the
   misleading "sorted shards prevent deadlocks" comment at `coordinator.rs:371-392` (dispatch is
   concurrent, not serialized).
2. **SCRIPT KILL row (H2).** Add a row for `SCRIPT KILL`/`FUNCTION KILL` vs. a held continuation
   lock: killing a cross-shard script revokes its continuation locks; the row states the observable
   state of writes already applied on some shards, bounded (FDB 5s-cap / CRDB heartbeat-lock
   precedent for bounding holds).
3. **Phase 2/3 rows (H3).** Row the unwind and gather-timeout phases. A gather timeout yields an
   AMBIGUOUS outcome (mirroring FM-TXN-032's model: possibly-applied vs. definitely-not), never
   resolved to commit/abort by clock. Wire the three existing tests
   (`phase1_dispatch_failure_aborts_shards_already_holding_intents`, etc.) to the new rows.
4. **Panic restart (H4).** On a panic in a VLL write path, restart the shard from WAL (clean state)
   rather than trusting the `catch_unwind` boundary. Extend FM-VLL-005's NOT observable with
   "partial writes surviving a panic."

## Acceptance criteria

- [ ] `FM-VLL-006` added with wound-wait rule and forcing test (two colliding continuations, both
      terminate, one commits); `just lint-spec` green
- [ ] `coordinator.rs:371-392`/`:329,354` doc comments corrected to state the real deadlock-freedom
      mechanism
- [ ] `SCRIPT KILL`/`FUNCTION KILL` vs. held-continuation-lock row added, with a bounded-hold
      mechanism and forcing test
- [ ] Phase-2 unwind and phase-3 gather-timeout rows added; AMBIGUOUS outcome for gather timeout
      mirroring FM-TXN-032's vocabulary; existing three phase tests wired as forcing tests
- [ ] Panic-in-VLL-write-path restarts the shard from WAL; FM-VLL-005 NOT observable extended;
      forcing test panics mid-`execute_scatter_part` and asserts no partial writes survive
- [ ] `just mutants-diff frogdb-vll` triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

Full package with WAL-restart panic story. (1) FM-VLL-006 wound-wait: on continuation-lock
collision the lowest txid wins, the younger aborts and retries (progress guaranteed; CRDB
txn-priority shape); forcing test: two colliding continuations — both terminate, one commits. The
misleading "sorted shards" comment (coordinator.rs:371-392 dispatch is concurrent) is corrected.
(2) SCRIPT KILL row: killing a cross-shard script revokes its continuation locks; the row states
the observable state of writes already applied on some shards (FDB 5s-cap / CRDB heartbeat-lock
precedent for bounding holds). (3) Row phases 2/3: unwind and gather timeouts get rows; a gather
timeout yields an AMBIGUOUS outcome (FM-TXN-032 model — possibly-applied vs definitely-not), never
resolves commit/abort by clock (wall-clock-as-correctness is the banned anti-pattern). (4) Panic in
a VLL write path → restart the shard from WAL (clean state) rather than trusting the catch_unwind
boundary; extend the NOT-observable with "partial writes surviving a panic".
