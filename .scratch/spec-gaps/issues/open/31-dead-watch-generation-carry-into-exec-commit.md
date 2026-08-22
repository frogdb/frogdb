# Dead-watch off-target round-trip: close the WATCH false-negative window via generation-carry

Status: ready-for-agent

Size: M

> **Ruling (2026-08-22, R10 in
> [work-item rulings](../../../cluster-correctness/2026-08-22-work-item-rulings.md)):**
> file and fix with the [issue 09](done/09-routing-epoch-carry-and-watch-recheck.md)
> generation-carry treatment.

## Parent

[TR-TXN-028](../../../specs/txn.md#tr-txn-028--a-dead-watch-off-the-target-shard-gets-its-own-round-trip)
— the row itself states the defect: "The off-target check is not atomic with the target's commit:
a write to an off-target watched slot landing between the two is not seen (a bounded WATCH
false-negative window that only dead watches can enter)."

## What is wrong

At EXEC, watches on shards other than the target (reachable only for `live_at_watch = false`
entries) get their own watch-only round-trips *before* the batch runs
(`frogdb-server/crates/txn/src/exec.rs:313-318`). If all report clean, the target's batch then
commits — but nothing re-checks the off-target keys at commit time. A write to an off-target
watched key that lands between its pre-batch check and the target's commit is invisible: the EXEC
commits even though a watched key changed after WATCH. That violates the WATCH contract's spirit
(optimistic CAS over the whole watch set as of commit) in a narrow but real window.

## What to build

Issue-09 treatment, applied to watch generations: the pre-batch off-target round-trip returns each
key's **generation** (version) rather than a bare clean/dirty verdict; the target shard's commit
step carries those generations and the off-target shards re-verify at commit… or, simpler and
matching issue 09's shape:

1. The off-target watch-only round-trip captures the observed generation per watched key.
2. The target's atomic commit step includes those captured generations in its CAS input — the
   commit aborts (`WatchAborted`, nil reply) if any carried generation no longer matches when the
   off-target shard confirms at commit time.
3. Exact protocol shape (second confirmation round vs. carrying generations into a single
   commit-time verification) follows the issue-09 precedent (routing-epoch carry + watch recheck);
   reuse its machinery where it exists.
4. Spec-first: TR-TXN-028's Postcondition rewritten — the false-negative-window sentence is
   deleted and replaced by the carry protocol; forcing test injects a write to the off-target
   watched key between the pre-batch check and the commit (concurrency-harness hook) and asserts
   `WatchAborted`.

## Acceptance criteria

- [ ] TR-TXN-028 rewritten: no false-negative window stated; carry/verify protocol rowed;
      `just lint-spec` green
- [ ] Forcing test: interleaved off-target write between pre-check and commit → EXEC aborts nil
- [ ] Existing dead-watch tests (issue-25 family) stay green
- [ ] `just mutants-diff frogdb-txn` triaged on touched code

## Blocked by

None - can start immediately.
