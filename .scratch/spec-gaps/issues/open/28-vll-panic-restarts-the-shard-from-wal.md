# A panic in a VLL write path restarts the shard from its WAL

Status: needs-triage

## Parent

[issue 07](07-vll-continuation-package.md) — the fourth item of its ruling
("Panic in a VLL write path → restart the shard from WAL (clean state) rather than trusting the
catch_unwind boundary; extend the NOT-observable with 'partial writes surviving a panic'"), carved
out because it is the one item of that package with no mechanism to build on. Issue 07's other
three items shipped; this one did not.

## What is wrong

[FM-VLL-005](../../../specs/vll.md#fm-vll-005--a-granted-op-panics-while-executing) pins that a
panic inside `execute_scatter_part` is caught at the shard boundary, that the dequeue/release
pairing survives the unwind, and that the shard answers the next message normally. What it does
*not* say is what happened to the data the panicking op was halfway through mutating. Continuing to
serve after a panic means continuing against structures whose invariants may have been broken
mid-mutation, and for a cross-shard op it means sibling shards may hold writes this shard never
completed.

The ruled fix is to stop trusting `catch_unwind` alone: a panic in a VLL *write* path should
restart the shard from its WAL, which is a known-clean state, and FM-VLL-005's NOT observable
should then be able to say "partial writes surviving a panic".

## Why it was not built with the rest of issue 07

There is no shard-restart mechanism to hang this on. Nothing in the tree reloads a single shard's
state from the WAL while the process keeps running — recovery is a process-startup path, and the
shard supervisor's only response to a dead worker is `AbortFailStop`. Building the restart is a
design task in its own right (what the shard does with in-flight messages and granted locks across
the reload, how the WAL replay boundary is chosen, whether the restart is observable to clients as
`-LOADING` or as an error), not an increment on the wound-wait/kill/gather-row work issue 07 landed.

## What to build

1. A single-shard restart-from-WAL path: quiesce the worker, drop the in-memory state, replay the
   shard's WAL to a known boundary, resume serving. Decide and row what clients see during it.
2. Route a panic caught in a VLL *write* path (`handle_vll_execute` over a write op) into that path
   instead of resuming on the existing state. Read paths keep today's isolate-and-continue
   behavior — there is nothing half-mutated to clean up.
3. Extend FM-VLL-005's NOT observable with "partial writes surviving a panic", and add the forcing
   test: panic mid-`execute_scatter_part` on a write op, then assert no partial write is readable
   afterwards.

## Acceptance criteria

- [ ] Single-shard WAL restart exists and is rowed (its own TR/FM rows, `just lint-spec` green)
- [ ] A panic in a VLL write path triggers it; a panic in a read path does not
- [ ] FM-VLL-005 NOT observable extended with "partial writes surviving a panic", forced by a test
      that panics mid-write and asserts no partial effect is readable
- [ ] TR-VLL-020's `Pending` field removed
- [ ] `just mutants-diff frogdb-vll` triaged on touched code

## Blocked by

None, but it is a design task: the restart mechanism should be brainstormed before implementation.
