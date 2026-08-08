# Does a slot barrier stall the primary's replica feed? Policy undecided

Status: needs-triage
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
