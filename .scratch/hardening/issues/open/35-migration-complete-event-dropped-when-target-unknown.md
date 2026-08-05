# A migration-complete event whose target is not in cluster state is dropped, leaving clients blocked forever

Status: needs-triage
Type: bug (liveness)
Severity: likelihood 2/3 (the event arrives from the Raft apply path, and this node's cluster
snapshot is read at the *same* instant — a target added in a recent entry can legitimately be
missing on a lagging node), consequence 3/3 (blocked clients on a slot this node no longer owns are
never woken; nothing will ever write that key here again) — score 5
Area: cluster / slot migration / blocked clients

## Problem

`frogdb-server/crates/server/src/slot_migration/events.rs:24-46` is the whole dispatcher:

```rust
while let Some(event) = migration_rx.recv().await {
    let target_addr = match cluster_state.get_node(event.target_node) {
        Some(node_info) => node_info.addr,
        None => { tracing::warn!(..); continue; }        // (1)
    };
    let target_shard = event.slot as usize % num_shards;
    if let Some(sender) = shard_senders.get(target_shard) {
        let _ = sender.send(ClusterMsg::SlotMigrated { .. }).await;   // (2)
    }
}
```

Two swallowed failures, both fatal to the only thing this loop exists to do — wake blocked clients
with the right `MOVED`:

1. **Unknown target ⇒ dropped.** The event is discarded with a `warn!`. Clients blocked on
   `BLPOP`/`BRPOP`/`BLMOVE`/`WAIT`-adjacent keys in that slot stay blocked until their own timeout,
   which for a zero timeout is *forever*. The slot has moved away, so no local write can ever wake
   them.
2. **Shard send failure ⇒ dropped.** `let _ = sender.send(..)` discards the result. A closed shard
   channel is indistinguishable from a delivered notification.

The `continue` is also strictly worse than the alternative available right here: `event` carries
`slot`, `source_node`, and `target_node`. A `SlotMigrated` notification needs an address only to
*format* the redirect; waking the clients with a plain `CLUSTERDOWN` (or re-routing them through
the normal routing path, which already handles "owner known, address unknown" as FM-CLUSTER-023)
strictly dominates leaving them parked.

There are no unit tests of any kind for this file.

## Fix sketch

- Retry the address lookup, or fall back to waking clients without an address (a `MOVED` this node
  cannot format is the FM-CLUSTER-023 case, which routing already renders as `CLUSTERDOWN`). Either
  way, **do not drop the event.**
- Surface the shard-send failure — at minimum `warn!`, since a closed shard channel means the shard
  is gone and its blocked clients are unreachable.
- Extract the per-event body into a pure-ish function taking `(event, &ClusterState, &[ShardSender],
  num_shards)` and returning the messages it wants sent, so it can be unit-tested without a live
  cluster. That seam is what FM-CLUSTER-038 is waiting on.

## Tests that should exist

- `migration_event_with_a_known_target_notifies_the_owning_shard`
- `migration_event_with_an_unknown_target_still_wakes_blocked_clients`
- `migration_event_routes_to_slot_modulo_num_shards`
- `migration_event_reports_a_closed_shard_channel`

## Spec impact

FM-CLUSTER-038 is currently `Forced by | MISSING` citing this issue. It becomes forceable once the
seam exists.
