# `SPUBLISH` cannot distinguish "I own this slot" from "nobody owns this slot"

Status: needs-triage
Type: bug (correctness, silent)
Severity: likelihood 1/3 (needs an unassigned slot — bootstrap, post-`FORGET`, mid-reset),
consequence 2/3 (a shard message is delivered on a node that does not own the shard channel's slot,
and no signal distinguishes it from correct delivery) — score 3
Area: cluster-runtime / pub/sub

## Problem

`frogdb-server/crates/cluster-runtime/src/pubsub.rs:196-203`:

```rust
let slot = slot_for_key(shard_channel.as_bytes());
let owner = snapshot.get_slot_owner(slot)?;          // (a) unassigned  -> None
if owner == self_node_id { return None; }            // (b) self-owned  -> None
let addr = /* registry lookup */?;                   // (c) no address  -> None
```

All three return `None`, and the caller reads `None` as "deliver locally". For (b) that is exactly
right. For (a) and (c) it is a fallback: better than dropping the message, but indistinguishable
from correct ownership.

The practical effect of (a): on a cluster with an unassigned slot, `SPUBLISH` to a shard channel in
that slot is delivered to whichever node's client happened to connect, on *every* such node
independently. Subscribers on different nodes see different subsets of the messages, and nothing —
no error, no log, no counter — says the shard channel was never routed.

`SUBSCRIBE`/`SSUBSCRIBE` do not slot-route at all, so a subscriber is not pinned to the owner
either; that is a separate (and larger) gap, noted here because it is why the conflation is
survivable today.

## Redis comparison

Redis routes `SPUBLISH` and `SSUBSCRIBE` through the same slot check as keyed commands and answers
`MOVED`/`CLUSTERDOWN` when the slot is not served locally. FrogDB's shard pub/sub has no such
refusal — it always delivers locally rather than redirecting.

## Fix sketch

Give the three cases distinct types, at minimum internally:

```rust
enum ShardRoute { Local, Remote(SocketAddr), Unowned, OwnerUnaddressable(NodeId) }
```

`Local` delivers locally; `Remote` forwards; `Unowned` and `OwnerUnaddressable` deliver locally
*and* log, so the fallback is visible. Whether the two fallback cases should instead be
`CLUSTERDOWN` is a client-visible decision and belongs with the broader "should shard pub/sub
slot-route at all" question, not with this cleanup.

## Tests that should exist

- `cluster_forward_distinguishes_an_unowned_slot_from_local_ownership`
- `cluster_forward_distinguishes_an_unaddressable_owner_from_local_ownership`

`cluster_forward_returns_none_when_this_node_owns_the_slot`,
`cluster_forward_falls_back_to_local_for_an_unowned_slot`, and
`cluster_forward_falls_back_to_local_when_the_owner_has_no_address` (added by FM-CLUSTER-070) pin
today's three-way collapse.

## Spec impact

FM-CLUSTER-070's `Outcome variant` gains the discriminated type; its `NOT observable` gains
"a fallback delivery reported identically to a correct one".
