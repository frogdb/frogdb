# FrogDB Pub/Sub

This document details FrogDB's publish/subscribe messaging system, including broadcast and sharded modes.

## Overview

FrogDB supports two pub/sub modes with a unified architecture:

| Mode | Commands | Routing | Use Case |
|------|----------|---------|----------|
| **Broadcast** | SUBSCRIBE, PUBLISH, PSUBSCRIBE | Shard 0 (coordinator) | General pub/sub, patterns |
| **Sharded** | SSUBSCRIBE, SPUBLISH | Hash to owner shard | High-throughput, Redis 7.0+ |

**Message Ordering Guarantee:**
- Per-subscriber FIFO: Messages are delivered in order to each individual subscriber
- Cross-subscriber: NO ordering guarantee across different subscribers on different shards
- Two subscribers to the same channel may see messages in different order during fan-out

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
│  │  Broadcast: route to shard 0 (coordinator)        │    │
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
| Publish cost | O(1) via shard-0 coordinator | O(1) |
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
| PUBLISH channel message | Publish message via shard-0 coordinator |

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

> **[Not Yet Implemented]** The entire Cluster Mode section below describes a future design for cross-node pub/sub forwarding. None of the types or functions described (ClusterPubSubForwarder, LocalOnlyForwarder, broadcast_to_cluster, forward_to_slot_owner) are currently implemented. Current pub/sub operates within a single node only.

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
| PUBLISH | Route to shard 0 (coordinator) | Route to shard 0 on each node via cluster bus |
| SPUBLISH | Route to `hash(channel) % num_shards` | Route to slot owner node → owner shard |
| SUBSCRIBE | Register on shard 0 (coordinator) | Register on shard 0 (local node) |
| SSUBSCRIBE | Store on owner shard | Store on slot owner node's owner shard |

### Message Ordering Guarantees in Cluster Mode

This section clarifies exactly what ordering guarantees pub/sub provides in cluster deployments.

**Ordering Scope:**

| Scope | Guarantee | Notes |
|-------|-----------|-------|
| Single subscriber | FIFO | Messages arrive in publication order |
| Multiple subscribers, same node | FIFO per subscriber | Each sees FIFO, but not synchronized across subscribers |
| Multiple subscribers, different nodes | **No guarantee** | Network timing determines order |
| Multiple publishers, same channel | **No guarantee** | Messages interleave arbitrarily |

**Why No Cross-Node Ordering:**

```
Publisher                Node A                 Node B
    │                       │                      │
    │── PUBLISH chan m1 ──▶│                      │
    │                       │── forward m1 ──────▶│
    │                       │                      │
    │── PUBLISH chan m2 ──▶│                      │
    │                       │── forward m2 ──────▶│
    │                       │                      │

If m2 arrives at Node B before m1 (network reordering):
- Subscriber on Node A sees: m1, m2 ✓ (local, FIFO)
- Subscriber on Node B sees: m2, m1 ✗ (network reordering)
```

**Global Pub/Sub (PUBLISH) Ordering:**

```
┌─────────────────────────────────────────────────────────────────┐
│                 PUBLISH Ordering Guarantees                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Guaranteed:                                                     │
│  • Per-subscriber FIFO for messages from a SINGLE publisher      │
│    (if messages go through the same node)                       │
│                                                                   │
│  NOT Guaranteed:                                                 │
│  • Cross-publisher ordering (multiple publishers = arbitrary)    │
│  • Cross-node ordering for subscribers on different nodes        │
│  • Total ordering across all subscribers                         │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Sharded Pub/Sub (SPUBLISH) Ordering:**

Sharded pub/sub has STRONGER ordering guarantees because messages only go to one node:

```
┌─────────────────────────────────────────────────────────────────┐
│                 SPUBLISH Ordering Guarantees                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Guaranteed:                                                     │
│  • Per-channel FIFO for all subscribers to that channel          │
│    (single slot owner = single point of serialization)          │
│                                                                   │
│  NOT Guaranteed:                                                 │
│  • Cross-channel ordering (different channels may be on          │
│    different nodes)                                             │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Per-Publisher FIFO:**

Messages from the same publisher to the same channel are delivered in order:

```
Publisher P (connected to Node A)
    │
    ├── PUBLISH chan m1
    ├── PUBLISH chan m2
    └── PUBLISH chan m3

All subscribers see: m1, m2, m3 (in order)

Why: Single TCP connection serializes messages.
```

**Cross-Publisher Non-Ordering:**

Messages from different publishers may interleave in any order:

```
Publisher P1 (Node A)        Publisher P2 (Node B)
    │                             │
    ├── PUBLISH chan p1_m1        ├── PUBLISH chan p2_m1
    ├── PUBLISH chan p1_m2        ├── PUBLISH chan p2_m2

Subscriber might see:
- p1_m1, p2_m1, p1_m2, p2_m2
- p2_m1, p1_m1, p2_m2, p1_m2
- p1_m1, p1_m2, p2_m1, p2_m2  (if P1's messages arrive first)
- Any other interleaving
```

**Client Recommendations for Ordering-Sensitive Workloads:**

| Requirement | Solution |
|-------------|----------|
| Total ordering across all messages | Use a single publisher |
| Per-topic ordering | Use sharded pub/sub (SPUBLISH) |
| Causally-related message ordering | Include sequence numbers in messages |
| Strong ordering | Use Redis Streams instead of pub/sub |

**Configuration for Network Timing:**

```toml
[cluster]
# Cluster bus message timeout affects forwarding latency
cluster_bus_timeout_ms = 5000

# No configuration for message ordering - it's inherent to distributed systems
```

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_pubsub_messages_forwarded_total` | Messages forwarded between nodes |
| `frogdb_pubsub_forward_latency_ms` | Latency of cross-node message delivery |
| `frogdb_pubsub_out_of_order_total` | Detected out-of-order deliveries (if sequence tracked) |

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

### Node Failover Behavior

When a node fails, pub/sub subscriptions on that node are lost. This section specifies exact behavior and message loss bounds.

**Message Loss Window:**

```
Maximum message loss window = failover_detection_time + topology_propagation_time

Where:
  failover_detection_time = health_check_interval × health_check_failures
                          = 5s × 3 = 15s (default)
  topology_propagation_time = cluster_bus_broadcast_latency
                            ≈ 50-100ms

Total: ~15 seconds worst case (default configuration)
```

**Failover Timeline for Sharded Pub/Sub:**

```
T=0s:     Primary node (owns slot S) fails
          - SPUBLISH to slot S: buffered or lost (node unreachable)
          - SSUBSCRIBE to slot S: rejected with -CLUSTERDOWN

T=0-15s:  Failure detection period
          - Health checks fail repeatedly
          - No automatic failover yet
          - Messages to slot S are lost

T=15s:    Orchestrator detects failure
          - Selects replica for promotion
          - Sends ROLE PRIMARY to replica

T=15.1s:  Replica promotes to primary
          - Inherits slot ownership
          - Ready to accept SPUBLISH/SSUBSCRIBE

T=15.2s:  Topology broadcast begins
          - TOPOLOGY_UPDATE sent to all nodes
          - Clients may still route to failed node

T=15.3s:  Nodes update routing tables
          - New routes take effect
          - -MOVED errors stop
          - Normal operation resumes

T=15.3s+: Messages deliverable again
          - New subscriptions on promoted node
          - Old subscriptions on failed node = LOST
```

**Subscription State After Failover:**

| Subscription Type | On Failed Node | On Promoted Replica | Client Action Required |
|-------------------|----------------|---------------------|----------------------|
| SUBSCRIBE (global) | Lost | Not transferred | Resubscribe |
| SSUBSCRIBE (sharded, owns slot) | Lost | Not transferred | Resubscribe |
| SSUBSCRIBE (sharded, remote slot) | Unaffected | N/A | None |

**Key Insight:** Pub/sub subscriptions are **connection-bound**, not replicated. Replica promotion does NOT transfer subscription state from failed primary.

**Client Behavior During Failover:**

```
Client connected to failed node:
  1. Connection drops (TCP reset or timeout)
  2. Client receives connection error
  3. Client should reconnect to any cluster node
  4. Client should resubscribe to all channels

Client connected to surviving node, subscribed to slot on failed node:
  1. Messages to that channel: lost during failover window
  2. After failover: messages to new owner deliverable
  3. Client subscription still valid (sharded sub routed to new owner)
```

**Broadcast vs Sharded Failover Impact:**

| Mode | Impact of Single Node Failure |
|------|-------------------------------|
| Global (PUBLISH) | Subscribers on failed node miss messages; others unaffected |
| Sharded (SPUBLISH) | All subscribers to channels owned by failed node miss messages |

**Message Loss Mitigation:**

Pub/sub provides **at-most-once delivery** by design. For applications requiring stronger guarantees:

| Requirement | Solution |
|-------------|----------|
| At-least-once delivery | Use Redis Streams (XADD/XREAD) |
| Exactly-once delivery | Use Streams + idempotent consumers |
| Message durability | Use Streams with persistence |
| Delivery confirmation | Use Streams with consumer groups |

**Configuration for Faster Failover:**

```toml
[cluster]
# Reduce detection time (trade-off: more false positives)
health_check_interval_ms = 2000    # Default: 5000
health_check_failures = 2          # Default: 3

# Faster detection = smaller message loss window
# 2s × 2 = 4s worst case (vs 15s default)
```

**Metrics for Failover Monitoring:**

| Metric | Description |
|--------|-------------|
| `frogdb_pubsub_failover_message_loss_window_ms` | Estimated message loss duration |
| `frogdb_pubsub_subscriptions_lost_total` | Subscriptions lost due to node failure |
| `frogdb_pubsub_resubscribe_after_failover_total` | Clients that resubscribed after failover |

**Client Library Recommendations:**

```python
class PubSubClient:
    def __init__(self, cluster):
        self.cluster = cluster
        self.subscriptions = set()  # Track subscriptions locally

    def subscribe(self, channel):
        self.cluster.subscribe(channel)
        self.subscriptions.add(channel)

    def on_disconnect(self):
        # Reconnect and resubscribe
        self.cluster.reconnect()
        for channel in self.subscriptions:
            self.cluster.subscribe(channel)

    def on_moved(self, channel, new_node):
        # For sharded pub/sub: resubscribe to new owner
        self.cluster.ssubscribe(channel)
```

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

### Subscription Limits

FrogDB enforces limits on subscriptions to prevent resource exhaustion from misbehaving or malicious clients.

**Per-Connection Limits:**

| Limit | Default | Description |
|-------|---------|-------------|
| `max_subscriptions_per_connection` | 10000 | Maximum channel subscriptions per connection |
| `max_pattern_subscriptions_per_connection` | 1000 | Maximum pattern subscriptions per connection |
| `max_sharded_subscriptions_per_connection` | 10000 | Maximum sharded channel subscriptions |

**Per-Shard Limits:**

| Limit | Default | Description |
|-------|---------|-------------|
| `max_total_subscriptions_per_shard` | 1000000 | Total subscriptions across all connections |
| `max_unique_channels_per_shard` | 100000 | Unique channel names per shard |
| `max_unique_patterns_per_shard` | 10000 | Unique patterns per shard |

**Configuration:**

```toml
[pubsub]
# Per-connection limits
max_subscriptions_per_connection = 10000
max_pattern_subscriptions_per_connection = 1000
max_sharded_subscriptions_per_connection = 10000

# Per-shard limits (0 = unlimited)
max_total_subscriptions_per_shard = 1000000
max_unique_channels_per_shard = 100000
max_unique_patterns_per_shard = 10000
```

**Behavior When Limits Exceeded:**

| Limit Exceeded | Error Response | Existing Subscriptions |
|----------------|----------------|------------------------|
| Per-connection channel | `-ERR max subscriptions reached for this connection` | Unchanged |
| Per-connection pattern | `-ERR max pattern subscriptions reached for this connection` | Unchanged |
| Per-shard total | `-ERR shard subscription limit reached` | Unchanged |
| Per-shard unique channels | `-ERR too many unique channels on this shard` | Unchanged |

**Limit Enforcement:**

```rust
fn handle_subscribe(&mut self, channels: Vec<Bytes>) -> Result<Response, Error> {
    let current_count = self.connection.subscriptions.len();
    let new_count = current_count + channels.len();

    // Per-connection limit
    if new_count > self.config.max_subscriptions_per_connection {
        return Err(Error::MaxSubscriptionsReached);
    }

    // Per-shard limit
    let shard_total = self.shard.subscription_count();
    if shard_total + channels.len() > self.config.max_total_subscriptions_per_shard {
        return Err(Error::ShardSubscriptionLimitReached);
    }

    // All limits passed - subscribe
    for channel in channels {
        self.shard.add_subscription(&channel, self.connection.id);
        self.connection.subscriptions.insert(channel);
    }

    Ok(Response::Subscribed(new_count))
}
```

**Memory Impact Per Subscription:**

| Component | Approximate Size |
|-----------|------------------|
| Channel name in HashMap key | `len(channel) + 24 bytes` (Bytes overhead) |
| Connection ID in HashSet | 8 bytes |
| Pattern (compiled regex) | ~200-500 bytes |
| Per-connection tracking | 8 bytes per subscription |

**Example Memory Calculation:**

```
10,000 connections × 100 subscriptions each = 1,000,000 total subscriptions

Per subscription: ~50 bytes (channel) + 8 bytes (conn ID) + 8 bytes (tracking) = ~66 bytes
Total: 1,000,000 × 66 bytes ≈ 63 MB for subscription metadata

Pattern subscriptions are more expensive:
1,000 patterns × 300 bytes (compiled) = 300 KB
```

**Why Pattern Limits Are Lower:**

1. **CPU cost**: Pattern matching requires regex evaluation for every PUBLISH
2. **Memory cost**: Compiled regex patterns consume more memory than simple strings
3. **Complexity**: Many patterns increase matching time significantly
4. **Attack surface**: Pathological regex patterns can cause ReDoS

**Subscription Count Reporting:**

```
PUBSUB NUMSUB channel1 channel2
*4
$8
channel1
:150       # 150 subscribers
$8
channel2
:25        # 25 subscribers
```

**DEBUG Command for Limits:** **[Not Yet Implemented]**

```
DEBUG PUBSUB LIMITS

# Sample output:
connection_subscriptions: 523/10000
connection_patterns: 5/1000
shard_total_subscriptions: 45230/1000000
shard_unique_channels: 8421/100000
shard_unique_patterns: 234/10000
```

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_pubsub_subscriptions_total` | Total active subscriptions |
| `frogdb_pubsub_subscriptions_per_connection` | Histogram of subscriptions per connection |
| `frogdb_pubsub_patterns_total` | Total active pattern subscriptions |
| `frogdb_pubsub_subscription_limit_errors_total` | Times limit was hit |
| `frogdb_pubsub_unique_channels_total` | Unique channel names with subscribers |

**Graceful Degradation:** **[Not Yet Implemented]** (80%/90% warning thresholds are not yet implemented; 100% limit enforcement is implemented)

When approaching limits:

| Threshold | Behavior |
|-----------|----------|
| 80% of per-connection limit | Log warning |
| 90% of per-shard limit | Log warning, emit metric |
| 100% limit hit | Reject new subscriptions, log error |

**Client Guidance:**

1. **Use sharded pub/sub** for high-subscription-count workloads
2. **Reuse channels** instead of creating unique channel per entity
3. **Use wildcards** (PSUBSCRIBE) sparingly - prefer explicit subscriptions
4. **Unsubscribe** when no longer needed to free resources
5. **Monitor** subscription counts via `PUBSUB NUMSUB` and metrics
