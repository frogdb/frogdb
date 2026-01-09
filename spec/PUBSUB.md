# FrogDB Pub/Sub

This document details FrogDB's publish/subscribe messaging system, including broadcast and sharded modes.

## Overview

FrogDB supports two pub/sub modes with a unified architecture:

| Mode | Commands | Routing | Use Case |
|------|----------|---------|----------|
| **Broadcast** | SUBSCRIBE, PUBLISH, PSUBSCRIBE | All shards | General pub/sub, patterns |
| **Sharded** | SSUBSCRIBE, SPUBLISH | Hash to owner shard | High-throughput, Redis 7.0+ |

---

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                   Per-Shard PubSubHandler                  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  ShardSubscriptions (shared by both modes)          │  │
│  │  • channel_subs: HashMap<Channel, Set<ConnId>>      │  │
│  │  • pattern_subs: Vec<(Pattern, ConnId)>             │  │
│  └─────────────────────────────────────────────────────┘  │
│                          │                                 │
│  ┌───────────────────────┴───────────────────────────┐    │
│  │                  Routing Decision                  │    │
│  │  Broadcast: fan-out to all shards                 │    │
│  │  Sharded: route to hash(channel) % num_shards     │    │
│  └────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

1. **Unified data structure**: `ShardSubscriptions` used by both modes
2. **Routing is the only difference**: Broadcast fans out, Sharded routes to owner
3. **~90% code reuse**: Only routing logic differs between modes
4. **Pattern subscriptions**: Broadcast mode only (PSUBSCRIBE)

---

## Mode Comparison

| Aspect | Broadcast | Sharded |
|--------|-----------|---------|
| Publish cost | O(shards) | O(1) |
| Subscribe cost | O(1) local | O(1) or forward |
| Pattern support | Yes | No |
| Use case | General, low-volume | High-throughput |

---

## Commands

### Broadcast Mode

| Command | Description |
|---------|-------------|
| SUBSCRIBE channel [channel...] | Subscribe to channel(s) |
| UNSUBSCRIBE [channel...] | Unsubscribe from channel(s) |
| PSUBSCRIBE pattern [pattern...] | Subscribe to pattern(s) |
| PUNSUBSCRIBE [pattern...] | Unsubscribe from pattern(s) |
| PUBLISH channel message | Publish message to all shards |

### Sharded Mode (Redis 7.0+)

| Command | Description |
|---------|-------------|
| SSUBSCRIBE channel [channel...] | Subscribe to sharded channel(s) |
| SUNSUBSCRIBE [channel...] | Unsubscribe from sharded channel(s) |
| SPUBLISH channel message | Publish to owner shard only |

### Introspection

| Command | Description |
|---------|-------------|
| PUBSUB CHANNELS [pattern] | List active channels |
| PUBSUB NUMSUB [channel...] | Get subscriber counts |
| PUBSUB NUMPAT | Get pattern subscription count |

---

## Message Format

Published messages are delivered to subscribers in this format:

```
*3\r\n
$7\r\nmessage\r\n
$<channel_len>\r\n<channel>\r\n
$<message_len>\r\n<message>\r\n
```

Pattern matches include the pattern:

```
*4\r\n
$8\r\npmessage\r\n
$<pattern_len>\r\n<pattern>\r\n
$<channel_len>\r\n<channel>\r\n
$<message_len>\r\n<message>\r\n
```

---

## Cluster Mode

In cluster mode, pub/sub behavior extends beyond internal shards to coordinate across cluster nodes.

### Cluster Pub/Sub Abstraction

```rust
/// Trait for cluster-wide pub/sub message forwarding.
/// Single-node deployments use a no-op implementation.
pub trait ClusterPubSubForwarder: Send + Sync {
    /// Forward PUBLISH message to all other cluster nodes (global pub/sub).
    /// Called after local delivery completes.
    fn broadcast_to_cluster(&self, channel: &[u8], message: &[u8]);

    /// Forward SPUBLISH to the node owning this channel's slot.
    /// Returns true if forwarded (remote owner), false if local.
    fn forward_to_slot_owner(&self, channel: &[u8], message: &[u8]) -> bool;

    /// Check if this node owns the slot for a sharded channel.
    fn is_local_slot(&self, channel: &[u8]) -> bool;
}

/// No-op implementation for single-node deployments.
pub struct LocalOnlyForwarder;

impl ClusterPubSubForwarder for LocalOnlyForwarder {
    fn broadcast_to_cluster(&self, _channel: &[u8], _message: &[u8]) {
        // No-op: single node, no cluster to broadcast to
    }

    fn forward_to_slot_owner(&self, _channel: &[u8], _message: &[u8]) -> bool {
        false // Always local in single-node mode
    }

    fn is_local_slot(&self, _channel: &[u8]) -> bool {
        true // All slots are "local" in single-node mode
    }
}
```

### Cluster Message Flow

**Global Pub/Sub (PUBLISH):**
```
Client: PUBLISH channel message
         │
         ▼
    Local Node
         │
         ├── 1. Deliver to local subscribers (all internal shards)
         │
         └── 2. ClusterPubSubForwarder::broadcast_to_cluster()
                  │
                  ├── Node B: Deliver to local subscribers
                  ├── Node C: Deliver to local subscribers
                  └── Node N: Deliver to local subscribers
```

**Sharded Pub/Sub (SPUBLISH):**
```
Client: SPUBLISH channel message
         │
         ▼
    Receiving Node
         │
         ├── if is_local_slot(channel):
         │       └── Deliver to local subscribers (owner shard only)
         │
         └── else:
                 └── forward_to_slot_owner(channel, message)
                          │
                          ▼
                     Slot Owner Node
                          └── Deliver to local subscribers
```

### Cluster Routing Summary

| Command | Single-Node Routing | Cluster Routing |
|---------|---------------------|-----------------|
| PUBLISH | Fan-out to all internal shards | Fan-out to all nodes → each fans out internally |
| SPUBLISH | Route to `hash(channel) % num_shards` | Route to slot owner node → owner shard |
| SUBSCRIBE | Store on all internal shards | Store locally (global: any node receives) |
| SSUBSCRIBE | Store on owner shard | Store on slot owner node's owner shard |

### Slot Migration (Sharded Channels) {#cluster-integration}

During slot migration, sharded pub/sub subscriptions require special handling.

**Server-Side Behavior (matches Redis 7.0+):**

1. **Pre-migration:** Source node owns the slot, handles SPUBLISH/SSUBSCRIBE
2. **Migration in progress:** Source continues handling subscriptions
3. **Migration complete:** Source **actively unsubscribes** all clients from migrated channels
4. **Notification:** Server sends `-MOVED slot target:port` to each affected subscriber
5. **State cleanup:** Subscription removed from source node's local state

```
Source Node                              Target Node
     │                                        │
     │ ◀── SSUBSCRIBE {channel}:foo ──────── Client
     │     (subscription stored locally)      │
     │                                        │
     │  [Slot migration to Target starts]     │
     │  [Slot migration completes]            │
     │                                        │
     │  Server iterates sharded subscriptions │
     │  for migrated slot                     │
     │                                        │
     │  Server removes subscription ────────▶ │
     │  -MOVED 12345 target:6379 ───────────▶ Client
     │                                        │
     │                              Client ── SSUBSCRIBE {channel}:foo ──▶│
     │                                        │  (subscription on new owner)
```

**Why Server-Side Unsubscription?**
- Clients may not actively poll for messages
- Passive `-MOVED` on next publish could leave clients waiting indefinitely
- Proactive notification ensures timely reconnection

**Message Loss Window:**
Messages published after migration completes but before client resubscribes on target are lost.
This is inherent to pub/sub's at-most-once delivery model.

**No State Transfer:**
Pub/sub subscriptions are connection-bound and not persisted. The source node doesn't transfer
subscription state to the target; clients must resubscribe.

### ACL Permissions During Migration

Channel ACL permissions are stored in the cluster-wide ACL configuration, not per-slot:

| ACL Aspect | Migration Behavior |
|------------|-------------------|
| **Channel permissions** | Unchanged - ACL rules apply cluster-wide |
| **User authentication** | Connection remains authenticated |
| **Subscription re-auth** | NOT required when resubscribing to target |
| **Permission check** | Target node validates permissions on SSUBSCRIBE |

**Scenario: User with limited channel access**

```
User ACL: +@pubsub ~{orders}:*  # Can only subscribe to {orders}:* channels
```

1. Client subscribes on Source: `SSUBSCRIBE {orders}:events` → OK
2. Slot migrates to Target
3. Client receives `-MOVED`
4. Client resubscribes on Target: `SSUBSCRIBE {orders}:events`
5. Target validates ACL → OK (same ACL applies)

**Important:** If ACL rules are updated cluster-wide during migration:
- New rules take effect immediately on all nodes
- Resubscription may fail if permissions were revoked
- Client receives `-NOPERM` on resubscribe attempt

**ACL Propagation:**

| Method | Behavior |
|--------|----------|
| `ACL SETUSER` on single node | Change NOT propagated (node-local) |
| `ACL SAVE` + deployment | Manual propagation |
| Config file + restart | All nodes reload ACL |
| Orchestrator-managed ACL | Pushed to all nodes |

**Recommendation:** Use orchestrator or config management to ensure ACL consistency across cluster nodes before migrations.

### Scalability Considerations

| Aspect | Global Pub/Sub | Sharded Pub/Sub |
|--------|----------------|-----------------|
| Cluster bus traffic | O(nodes × messages) | O(1) per message |
| Scaling behavior | Degrades with cluster size | Scales horizontally |
| Recommended for | Low-volume cluster-wide events | High-throughput messaging |

**Warning:** Global pub/sub (PUBLISH) broadcasts to all cluster nodes. For high-throughput
workloads, use sharded pub/sub (SPUBLISH) to confine traffic to slot owners.

### Mixed Mode: PUBLISH vs SPUBLISH on Same Channel

Global and sharded pub/sub operate as **separate namespaces**. Using the same channel name with both modes does NOT cause conflicts:

| Scenario | Behavior |
|----------|----------|
| `SUBSCRIBE mychannel` + `PUBLISH mychannel msg` | Subscriber receives `msg` |
| `SSUBSCRIBE mychannel` + `SPUBLISH mychannel msg` | Subscriber receives `msg` |
| `SUBSCRIBE mychannel` + `SPUBLISH mychannel msg` | **No delivery** (different namespace) |
| `SSUBSCRIBE mychannel` + `PUBLISH mychannel msg` | **No delivery** (different namespace) |

**Namespace Isolation:**

```rust
// Global subscriptions (SUBSCRIBE)
global_subscriptions: HashMap<ChannelName, Set<ConnectionId>>

// Sharded subscriptions (SSUBSCRIBE) - per shard
sharded_subscriptions: HashMap<ChannelName, Set<ConnectionId>>

// PUBLISH routes to global_subscriptions only
// SPUBLISH routes to sharded_subscriptions only
```

**Client Responsibility:**
- Use consistent mode per logical channel
- `SUBSCRIBE` + `SSUBSCRIBE` to same name = two independent channels
- No warning issued (this is valid Redis behavior)

**Pattern Subscriptions:**
- `PSUBSCRIBE` pattern matches only `PUBLISH` messages
- No `PSSUBSCRIBE` equivalent exists (sharded channels don't support patterns)

---

## Delivery Guarantees

- **At-most-once delivery**: Messages may be lost on disconnect
- **Per-channel FIFO**: Order preserved within a channel
- **No persistence**: Pub/sub messages are not persisted
- **No acknowledgment**: Fire-and-forget delivery model

---

## Connection State

Once a connection enters pub/sub mode, it can only execute:
- SUBSCRIBE / UNSUBSCRIBE
- PSUBSCRIBE / PUNSUBSCRIBE
- SSUBSCRIBE / SUNSUBSCRIBE
- PING
- QUIT

All other commands return an error until the connection unsubscribes from all channels.

```rust
struct ConnectionState {
    /// Set of subscribed channels
    subscriptions: HashSet<Bytes>,
    /// Set of subscribed patterns
    patterns: HashSet<Bytes>,
    /// True if in pub/sub mode
    pubsub_mode: bool,
}
```
