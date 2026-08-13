# 06 — the replica frame channel (and its open MULTI group) survives a reconnect/FULLRESYNC

Status: done

## Resolution — history epochs on the frame channel (2026-08-02)

Neither of the two directions in "Fix direction" as written. Bumping the stint at the reconnect
seam retires the *consumer*, not just the leftovers — the long-lived `consume_frames` task stops at
its next refused claim and never restarts, so the replica silently applies nothing after its first
reconnect; a channel per stint costs a task per attempt and re-opens the "never abort mid
`apply_group`" question. Both also decide a frame's fate by *when it is claimed*, which is a race
against the connection task that is doing the reset.

Instead the frames themselves carry their history. `AppliedOffset` gained a **history epoch**,
bumped by `reset_pair` only — i.e. exactly when a full resync adopts a new dataset — under the same
gate lock that moves the heads. The decode loop stamps every frame it queues with the epoch it read
at stream start (`StreamedFrame`), and the consumer checks it twice: a cheap pre-check at the top of
the loop that drops a stale frame and the group it belonged to, and the authoritative re-check
inside `ReplicaApplyStint::claim` under the gate lock, which is what makes it race-free
(`Claim::Stale`, a third verdict next to `Granted`/`Retired`). A group is additionally abandoned
when the frame in hand is current but the group was opened under an older epoch, which is the
"wrong-shard stale group" case: without it a bare command from the new history is buffered into the
old group and applied on the *old* group's tagged shard at the next `EXEC`
(`left: [(3, ["SET", "DEL"])]` vs `right: [(1, ["DEL"])]` before the fix).

`+CONTINUE` bumps nothing, so leftovers from a resumed link legitimately still apply, including a
`MULTI` group split across the reconnect — the fourth acceptance criterion, pinned by its own test
(which passes before the fix too, and is there to stop the fix from over-reaching).

Spec: FM-REPLICATION-007 (`specs/replication.md`). Tests:
`a_full_resync_discards_the_frames_queued_from_the_previous_history`,
`a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history`,
`a_continue_resume_still_applies_the_frames_it_left_queued` (apply.rs),
`a_claim_stamped_before_a_resync_is_refused_after_it` (replica/offset.rs).

Ordering note: the epoch this adds is the mechanism issue 08's divergence handling reuses — a
divergence that forces a full resync gets its "everything queued is void" for free from this row.

## What to build

A replica's decoded-frame channel and its frame consumer both outlive the connection that filled
them. `ReplicaReplicationHandler::start()` reconnects in a loop, and every stint pushes into the
*same* `mpsc::Sender<ReplicationFrame>` handed to a single long-lived `consume_frames` task
(`Server::start_subsystems`, and `RealReplicaStreamer::start` for runtime demotions). Nothing
drains that channel, and nothing resets the consumer's `pending: Option<PendingTxn>`, when the
link drops or when the new stint comes back `+FULLRESYNC` and installs a fresh checkpoint.

Two failure modes follow:

1. **Wrong-shard stale group.** The link drops between `MULTI` and `EXEC`. The consumer is holding
   a `PendingTxn` tagged with the old stint's shard id and its buffered commands. The next stint
   full-resyncs and streams from a new offset; its first `EXEC` closes the *stale* group, applying
   the old primary's buffered commands (possibly onto a shard chosen by the old tag) on top of a
   keyspace that was just replaced wholesale by the checkpoint.
2. **`applied` above the live head.** Frames still queued from the previous stint are applied after
   the resync reset the offsets (`reset_to` / checkpoint install), so the applied counter can be
   credited for bytes that belong to a stream position the node no longer occupies — breaking the
   `applied <= live` invariant the promotion boundary depends on, and crediting the new history for
   data from the old one.

Both are latent on `main` today: they need a link drop plus a FULLRESYNC, and the common path is a
`+CONTINUE` resume where the leftover frames are (accidentally) still correct.

## Fix direction

Either:

- **Drain + clear at the seam.** On FULLRESYNC / checkpoint install (and, conservatively, on any
  reconnect), drain the frame channel and clear the consumer's pending group before the new stint
  pushes its first frame. Needs a signal from the connection task to the consumer — the applying
  stint token added in `promotion-replid-psync.md` §13 is the natural carrier: bump the stint and
  hand the connection a fresh one, which makes the consumer refuse every leftover claim and drop
  its pending group.
- **Channel per stint.** Give each connection attempt its own channel and its own consumer task,
  so leftover frames die with the stint that decoded them. Cleaner invariant, more task churn, and
  it must keep the "never abort a consumer mid-`apply_group`" property established in §13
  (retire by refusing claims, let the task end as its channel closes).

## Acceptance criteria

- [x] A MULTI group left open by a dropped link is never closed by a later stint's `EXEC`
- [x] Frames decoded before a FULLRESYNC are not applied after the checkpoint is installed
- [x] The applied offset never exceeds the live/received offset across a reconnect
- [x] Test covers both a `+CONTINUE` resume (leftovers legitimately still apply, or are re-sent)
      and a `+FULLRESYNC` (leftovers discarded)

## Note (2026-07-28)

The stint-bump option is now nearly free. `AppliedOffset`/`ReplicaApplyStint` already exist for the
promotion boundary, and `ReplicaOffset` already captures a stint number so a retired connection
cannot reset the heads. Bumping the stint at the reconnect seam (rather than only per *stream*)
would refuse every leftover claim from the previous stint, which drops the queued frames' effect
without touching the consume loop's structure — the remaining work is clearing the pending `MULTI`
group and deciding whether the consumer keeps running under a fresh token or is replaced.

## Source

Adversarial review of `promotion-replid-psync.md`, finding NEW-2 (2026-07-28). Filed rather than
fixed: it is pre-existing, orthogonal to the promotion boundary, and the fix touches the reconnect
seam rather than the promotion path.
