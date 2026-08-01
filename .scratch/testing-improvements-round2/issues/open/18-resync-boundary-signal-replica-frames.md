# Resync-boundary signal on the replica frame channel — the applier cannot tell pre- from post-resync

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I18
LOE: 1–2 days (estimated)
Tier: B
Area: frogdb-replication / replica frame channel
Asked by: 14 (shared-infra item 1, its top priority). **Dropped from `MASTER.md` §6.**

## Context

This is the one infrastructure item that is not test-only. The replica applier currently has
no way to distinguish a frame produced before a resync from one produced after it, which is
the direct cause of two replication findings — it is not merely what makes them hard to test.
Adding the boundary signal fixes the defects and creates the observation point a test needs,
in the same change.

## Evidence

- **Distinguishing feature**: this is required to **fix** 14/F3 and 14/F5, not only to test
  them. Today the applier cannot tell a pre-resync frame from a post-resync one. A generation
  counter on `ReplicationFrame`, or an explicit `Barrier` message, is simultaneously the fix
  and the test hook.
- **Sequencing**: therefore belongs with the replication bug work, not with the test work.

## What to build

1. Choose one of the two shapes named above — a generation counter on `ReplicationFrame`, or
   an explicit `Barrier` message on the channel — and implement it.
2. Make the applier reject or discard frames from a superseded generation, which is the fix
   for 14/F3 and 14/F5.
3. Expose the boundary as an observable signal a test can await, so resync-ordering
   assertions do not have to infer the boundary from timing.
4. Schedule this with the replication bug work, not with the test-infrastructure work.

## Acceptance criteria

- [ ] `ReplicationFrame` (or the channel) carries an explicit resync boundary; the choice
      between generation counter and `Barrier` is recorded with its reasoning.
- [ ] The applier drops or rejects frames from a superseded generation, and a test asserts a
      pre-resync frame delivered after a resync is not applied.
- [ ] A test can await the boundary signal directly, with no sleep and no timing inference.
- [ ] 14/F3 and 14/F5 each have a test that fails against today's code and passes after this
      lands.

## Test boundary

Level 5 — a resync boundary only exists between a real primary and a real replica, so the
regression tests need the multi-node/turmoil harness; the generation-comparison logic itself
is additionally unit-testable at level 1.

## Depends on

Nothing.
