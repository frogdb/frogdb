# Does a slot barrier stall the primary's replica feed? Policy undecided

Status: done
Type: decision (product/policy)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Cluster / Replication interaction

## Problem

Brief §8 row FM-013/014 territory, left undecided through issue 02's build (phases 1-2b): when a
slot-scoped write barrier arms on a node that is also a replication primary, nothing today decides
whether the replication stream should pause. Redis 8.4 ASM and Valkey 9.0 ASM both include
`PAUSE_ACTION_REPLICA` in their migration pause; FrogDB's barrier pauses client acknowledgements
only, and replication ships whatever the shards apply.

The barrier's fencing model makes this narrower than it sounds: a fenced write may APPLY locally
(the fence refuses the acknowledgement, not the work — FM-CLUSTER-095's caveat), and an applied
write replicates. A replica of the losing node can therefore hold a write the client was told to
retry elsewhere. That is at-least-once, not loss, but it is state the primary's own clients never
saw acknowledged.

## Decision needed

1. Match Redis/Valkey (`PAUSE_ACTION_REPLICA`-equivalent: barrier also holds the replica feed for
   the barriered slot's shard), or
2. Document the current behavior as intended (replicas may briefly hold unacknowledged-but-applied
   writes around a handoff; a subsequent resync reconciles).

## Forcing test

Whichever way it goes: a witness with a replica attached to the source across a loaded handoff,
asserting either the feed stalls (option 1) or the replica's extra writes are exactly the fenced
unacknowledged set and reconcile on resync (option 2). New FM-CLUSTER row either way.

## Resolution (2026-08-08)

**Ruling — option 1.** The barrier holds the replica feed. Matching Redis 8.4 / Valkey 9.0
`PAUSE_ACTION_REPLICA`: while the atomic-slot-migration pause is up, the primary stops flushing its
replica output buffers, so a replica cannot run ahead of a primary that is fencing its own clients.

**Scope of the hold — node-wide for the barrier window, not per-shard.** The issue proposed
holding "the barriered slot's shard", and the frames do carry a shard id, so it looks reachable.
It is not: replication is a single global stream with one monotonic offset
(`OffsetCoordinator::advance`, the one advance gate), and a replica dedups and acks against that
single sequence. Shipping shard B's frames while shard A's are held would put offsets on the wire
out of order — a worse bug than the anomaly being closed. So the hold is node-wide, which is also
what Redis and Valkey do (their pause is node-wide too). The window is the barrier's own: ~100 ms
in production (`HANDOFF_BARRIER_MS`), backstopped by the handoff lease.

A node-global `CLIENT PAUSE` deliberately does *not* hold the feed. It stops the writes
themselves, so there is nothing new to ship and stalling a replica behind it would be pure lag —
Redis likewise reserves `PAUSE_ACTION_REPLICA` for the migration pause.

**Seam — derived from the pause state, not a second flag.** `PauseState::feed_hold_until()`
(`core/src/client_registry/mod.rs`) returns the latest deadline across armed *slot* pauses.
`ClientRegistry::publish_pause_derived_state` — the renamed `publish_expiry_paused`, already
called on every pause mutation — republishes it into a `ReplicaFeedGate`
(`replication/src/feed_gate.rs`) alongside the existing `expiry_paused` flag. The write barrier and
the feed hold are therefore two renderings of one fact and cannot disagree.
`PrimaryReplicationHandler` is constructed with the registry's gate
(`server/src/server/replication_init.rs`), and `ReplicaSession::start_streaming`
(`replication/src/replica_session.rs`) consults it.

**Release paths — all of them, including the ones nobody runs.** The gate stores the barrier's
*deadline*, never a latch:

- normal completion (`SlotMigrationCompleted` → `unpause_slot`) and abort both republish `None`;
- a barrier nobody released — finalizer died, lease lapsed, no sweep ran — reads as released the
  moment `clock::now()` passes the published deadline, with nothing to clear;
- `released()` sleeps until that deadline, so a session waiting out a hold on an otherwise idle
  node always wakes.

A wedged feed would be strictly worse than the anomaly, so the design admits no state in which one
is possible.

**Ordering (requirement 4).** A held session keeps draining the broadcast receiver into a
per-session `VecDeque<ReplicationFrame>` (so a hold cannot get it `Lagged` off the channel) and
flushes in offset order on release. A session that PSYNCs *into* a barrier waits on the gate after
subscribing but before replaying the backlog tail, so the held writes do not leak out of the replay
lane either.

**Fenced-but-applied writes (requirement 5) — they still replicate after release.** The fence
refuses the acknowledgement, not the work (FM-CLUSTER-095), so such a write is legitimate local
state on the source; no rollback machinery was built or is wanted. What the hold buys is that it
does not ship *while the client is being told to retry elsewhere*: the replica never observes,
during the handover window, state the primary's own clients were denied. After release the source
is just a node with committed data, and it ships it.

**Forcing test** — `the_barrier_holds_the_replica_feed_until_the_handoff_releases_it`
(`frogdb-server/crates/server/tests/cluster_handoff_barrier.rs`): a replica attached to the source
across a loaded handoff. A control write proves the link ships promptly; a write on an unbarriered
slot then applies on the source under the armed barrier and must not reach the replica for the
probe window; the handoff finalization releases the barrier and the replica converges. The
unbarriered slot is deliberate — the hold is node-wide, so a write that does not park at all is
the sharpest probe of it.

Seam-level witnesses: `slot_barrier_publishes_and_clears_the_replica_feed_hold`,
`a_node_pause_does_not_hold_the_replica_feed`,
`a_lapsed_barrier_frees_the_feed_with_nobody_clearing_it`,
`overlapping_barriers_hold_the_feed_to_the_later_deadline` (`frogdb-core`), plus six
`feed_gate::tests` cases in `frogdb-replication`.

**FM row** — FM-CLUSTER-097, in `.scratch/hardening/specs/cluster-failure-modes.md`.
