# 06 — the replica frame channel (and its open MULTI group) survives a reconnect/FULLRESYNC

Status: ready-for-agent

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

- [ ] A MULTI group left open by a dropped link is never closed by a later stint's `EXEC`
- [ ] Frames decoded before a FULLRESYNC are not applied after the checkpoint is installed
- [ ] The applied offset never exceeds the live/received offset across a reconnect
- [ ] Test covers both a `+CONTINUE` resume (leftovers legitimately still apply, or are re-sent)
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
