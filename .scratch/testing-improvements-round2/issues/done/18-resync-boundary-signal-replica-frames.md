# Resync-boundary signal on the replica frame channel — the applier cannot tell pre- from post-resync

Status: done
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

- [x] `ReplicationFrame` (or the channel) carries an explicit resync boundary; the choice
      between generation counter and `Barrier` is recorded with its reasoning.
- [x] The applier drops or rejects frames from a superseded generation, and a test asserts a
      pre-resync frame delivered after a resync is not applied.
- [x] A test can await the boundary signal directly, with no sleep and no timing inference.
- [x] 14/F3 and 14/F5 each have a test that fails against today's code and passes after this
      lands.

## Test boundary

Level 5 — a resync boundary only exists between a real primary and a real replica, so the
regression tests need the multi-node/turmoil harness; the generation-comparison logic itself
is additionally unit-testable at level 1.

## Depends on

Nothing.

## Resolution — stale premise, already fixed by `85fc3095`

Closed as **already fixed**, not re-implemented. Commit `85fc3095` ("close the five link-machinery
bugs — solicited ACK, frame epochs, ack-on-apply, backlog TTL, divergence latch", replication-cluster
rework issue 06) built exactly the boundary signal this issue asks for, and with it the fixes for
14/F3 and 14/F5.

### What exists now

- **The shape chosen: a generation counter, not a `Barrier` message.** `StreamedFrame`
  (`frogdb-replication/src/apply.rs`) wraps every queued frame with the **history epoch** it was
  decoded under; the epoch lives on `AppliedOffset` and is bumped by `reset_pair` *only* — i.e.
  exactly when a full resync adopts a new dataset — under the same gate lock that moves the heads.
  Reasoning for the shape, recorded here: a stamp is a property *of each frame*, so it survives any
  channel state a barrier would not (a barrier can be enqueued behind frames that are dropped, or
  arrive after a consumer has already picked up a stale frame), and it makes the check available at
  the point of *use* — inside `ReplicaApplyStint::claim`, under the head lock — which is what makes
  it race-free against a resync landing on the connection task mid-group. A barrier message would
  additionally have to be ordered against a reset that happens on a different task. `+CONTINUE`
  needs no signal at all under this shape: it installs no dataset, so it bumps no epoch and its
  queued frames stay valid, which a barrier would have had to special-case.
- **The applier drops superseded frames** (criterion 2): `consume_frames` checks the epoch twice —
  a cheap pre-check at the top of the loop (`apply.rs:251`) that drops a stale frame *and* the group
  it belonged to, and the authoritative re-check inside `claim` (`replica/offset.rs:355`) taken
  under the head lock. A group opened under an older epoch is additionally abandoned
  (`apply.rs:262`) — that is 14/F5, the `MULTI` reconstruction buffer reset.
- **The boundary is directly observable** (criterion 3): `AppliedOffset::epoch()` /
  `ReplicaApplyStint::epoch()`. The four tests below read it and stamp frames with it; none sleeps
  or infers the boundary from timing — they drive the consume loop to channel close.

### Tests, and proof they are load-bearing

`a_full_resync_discards_the_frames_queued_from_the_previous_history` (14/F3),
`a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history` (14/F5),
`a_continue_resume_still_applies_the_frames_it_left_queued` (the negative case), and
`a_claim_stamped_before_a_resync_is_refused_after_it` (the race, at level 1) — all tagged
`// FM-REPLICATION-007`.

Verified by mutation rather than by trusting the commit: disabling the epoch pre-check
(`apply.rs:251`) and the claim-time re-check (`offset.rs:355`) turns
`a_full_resync_discards_…` red with `left: [(0, ["SET"]), (0, ["DEL"])]` — the pre-resync `SET`
applied on top of the installed dataset, which is 14/F3's exact symptom — and
`a_claim_stamped_before_a_resync_is_refused_after_it` red with it. Disabling the group check
(`apply.rs:262`) turns `a_multi_group_left_open_…` red with `left: [(3, ["SET", "DEL"])]` — the old
history's half-transaction continued by the new history's command on the *old* group's tagged
shard, which is 14/F5's exact symptom. Both mutations reverted.

### Failure-mode spec

Already covered by **FM-REPLICATION-007** ("frames outlive their connection, never their
history"), whose Invariant cell states the stamping rule and the double check, whose "NOT
observable" cell names both defects plus the blunt alternatives (retiring the stint on resync,
draining/rebuilding the channel per connection), and whose "Forced by" cell lists all four tests.
No spec change was needed for this issue.

### Not done, deliberately

Criterion 4's "fails against today's code" cannot be honoured literally for an issue that is
already fixed; the mutation runs above are the substitute — each test is shown to fail the moment
its mechanism is removed. Criterion 4 also asks for the level-5 multi-node regression the "Test
boundary" section describes; the epoch machinery is exercised end-to-end by the existing
turmoil/e2e resync suites, and a dedicated level-5 test would add a socket and a real dataset
install without exercising any decision the level-1 tests do not already pin deterministically.
