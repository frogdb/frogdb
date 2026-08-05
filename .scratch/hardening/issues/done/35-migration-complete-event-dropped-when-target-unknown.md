# A migration-complete event whose target is not in cluster state is dropped, leaving clients blocked forever

Status: done
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

## Resolution

The dispatcher moved out of `frogdb-server` into the mutation-gated `frogdb-cluster-runtime`
(`frogdb-server/crates/cluster-runtime/src/migration_events.rs`); `slot_migration/events.rs` is
deleted and `SlotMigrationCoordinator::spawn_event_dispatcher` now spawns
`frogdb_cluster_runtime::run_slot_migration_event_dispatcher`. All of the crate's inputs were
already `frogdb-core` types, so nothing had to be re-plumbed.

Both swallowed failures are handled:

* **Unknown target no longer drops the event.** `ClusterMsg::SlotMigrated.target_addr` became
  `Option<SocketAddr>` (`core/src/shard/message.rs:751`). The pure `plan_migration_notice` records
  `None` when the node is absent from the snapshot, the loop `warn!`s with `slot`/`target_node`, and
  delivery proceeds. `handle_slot_migrated` (`core/src/shard/blocking.rs:118`) renders
  `MOVED <slot> <addr>` for `Some` and `CLUSTERDOWN Hash slot <n> not served` for `None` — the same
  reply routing already produces for "owner known, address unknown" (FM-CLUSTER-023). The waiter is
  drained either way, so no client is left parked.
* **Delivery failure is loud.** `deliver_migration_notice` `error!`s — with `slot`, `shard`, and
  either `num_shards` or the send error — for both a shard index the sender list does not have and a
  closed shard channel, and returns `false` rather than a silent `let _ =`.

Unknown-target *is* reachable in a healthy cluster (the apply path reads this node's snapshot at the
same instant the entry lands), so it is a `warn!`, not an `error!`; the two delivery failures are
`error!` because they mean a shard is gone.

`BlockedMigrationMoved` is incremented only on the `MOVED` path, so the counter keeps meaning
exactly what its name says rather than absorbing `CLUSTERDOWN` wake-ups.

Tests: the four named above, plus `slot_migrated_without_a_known_target_replies_clusterdown` in
`core/src/shard/blocking.rs` pinning the shard-side rendering. FM-CLUSTER-038 rewritten (no longer
`MISSING`); GAPS entry 5 removed and the list renumbered.
