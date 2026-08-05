# `SPUBLISH` cannot distinguish "I own this slot" from "nobody owns this slot"

Status: done
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

## Resolution

Implemented as sketched. `cluster-runtime/src/pubsub.rs` grew two types:

```rust
pub enum ShardRoute { Local, Remote { owner, addr }, Unowned { slot }, OwnerUnaddressable { owner, slot } }
pub enum SpublishOutcome { Forwarded(usize), Local(ShardRoute) }
```

`route_shard_channel_in` is the pure decision (slot map + address registry, no network), exposed as
`ClusterPubSubForwarder::route_shard_channel`. `forward_spublish` matches it exhaustively, so a
future unresolvable case cannot be added as another silent `None`. Both fallbacks warn-log naming
the slot (and the owner, for the unaddressable case), which is the "no signal at all" half of the
bug.

Client-visible behavior is deliberately unchanged. `SpublishOutcome::remote_count() -> Option<usize>`
is exactly the shape the connection layer already consumed, so `handle_spublish` is a one-line
change and the RESP reply is byte-identical. Redis would answer `MOVED`/`CLUSTERDOWN` for a shard
channel it does not serve, but FrogDB's `SSUBSCRIBE` does not slot-route subscribers, so refusing
here would drop a message no other node would deliver. Adopting Redis' refusal is gated on routing
subscribers first; the deviation is now recorded in the spec's Redis-deviations table rather than
implied.

Forcing tests (`frogdb-cluster-runtime`, `pubsub.rs`):
`cluster_forward_distinguishes_an_unowned_slot_from_local_ownership` and
`cluster_forward_distinguishes_an_unaddressable_owner_from_local_ownership` — each asserts the exact
variant *and* `assert_ne!` against the correct-ownership outcome, which is the property the bug
violated. Added `cluster_route_names_a_reachable_remote_owner` for the fourth arm (previously only
reachable through a real RPC). The three tests that pinned the collapse were rewritten in place.

Spec: FM-CLUSTER-070 rewritten (Observable, NOT observable, Invariant, Outcome variant, Forced by),
plus a new Redis-deviations row for shard pub/sub slot routing.
