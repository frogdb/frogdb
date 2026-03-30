---
title: "Pub/Sub"
description: "FrogDB supports publish/subscribe messaging with two modes: broadcast (global) and sharded."
sidebar:
  order: 6
---
FrogDB supports publish/subscribe messaging with two modes: broadcast (global) and sharded.

## Modes

| Mode | Commands | Use Case |
|------|----------|----------|
| **Broadcast** | SUBSCRIBE, PUBLISH, PSUBSCRIBE | General pub/sub, pattern matching |
| **Sharded** | SSUBSCRIBE, SPUBLISH | High-throughput, per-channel ordering (Redis 7.0+) |

## Broadcast Mode

Broadcast pub/sub delivers messages to all subscribers across the server.

| Command | Description |
|---------|-------------|
| SUBSCRIBE channel [channel...] | Subscribe to channel(s) |
| UNSUBSCRIBE [channel...] | Unsubscribe from channel(s) |
| PSUBSCRIBE pattern [pattern...] | Subscribe to glob-style pattern(s) |
| PUNSUBSCRIBE [pattern...] | Unsubscribe from pattern(s) |
| PUBLISH channel message | Publish a message |

### Example

```
# Terminal 1 (subscriber)
> SUBSCRIBE news
Reading messages...
1) "subscribe"
2) "news"
3) (integer) 1

# Terminal 2 (publisher)
> PUBLISH news "breaking story"
(integer) 1

# Terminal 1 receives:
1) "message"
2) "news"
3) "breaking story"
```

### Pattern Subscriptions

```
> PSUBSCRIBE news.*
```

Pattern matches deliver messages with the matched pattern included:
- Message type: `pmessage`
- Fields: pattern, channel, message

## Sharded Mode (Redis 7.0+)

Sharded pub/sub routes messages to the shard that owns the channel, providing better throughput and stronger per-channel ordering.

| Command | Description |
|---------|-------------|
| SSUBSCRIBE channel [channel...] | Subscribe to sharded channel(s) |
| SUNSUBSCRIBE [channel...] | Unsubscribe from sharded channel(s) |
| SPUBLISH channel message | Publish to sharded channel |

### When to Use Sharded Pub/Sub

- High message volume on specific channels
- Per-channel FIFO ordering is important
- In cluster mode, to avoid broadcasting to all nodes

## Introspection

| Command | Description |
|---------|-------------|
| PUBSUB CHANNELS [pattern] | List active channels matching pattern |
| PUBSUB NUMSUB [channel...] | Get subscriber counts for channels |
| PUBSUB NUMPAT | Get total number of pattern subscriptions |

## Delivery Guarantees

- **At-most-once delivery**: Messages may be lost on disconnect
- **No persistence**: Pub/sub messages are not stored
- **No acknowledgment**: Fire-and-forget delivery model
- **Per-subscriber FIFO**: Order preserved within a single subscriber connection

For stronger delivery guarantees (at-least-once, durability), use Redis Streams (XADD/XREAD) instead of pub/sub.

## Message Ordering

**Broadcast mode:**
- Per-subscriber FIFO from a single publisher is guaranteed
- Cross-publisher ordering is not guaranteed (messages from different publishers may interleave)
- In cluster mode, subscribers on different nodes may see messages in different order

**Sharded mode:**
- Per-channel FIFO for all subscribers (stronger than broadcast)
- Cross-channel ordering is not guaranteed

**Recommendations for ordering-sensitive workloads:**

| Requirement | Solution |
|-------------|----------|
| Total ordering | Use a single publisher |
| Per-topic ordering | Use sharded pub/sub (SPUBLISH) |
| Causally-related messages | Include sequence numbers in messages |
| Strong ordering guarantees | Use Redis Streams instead of pub/sub |

## Namespace Isolation

Broadcast and sharded pub/sub are separate namespaces. Using the same channel name with both modes does not cause conflicts:

| Scenario | Result |
|----------|--------|
| `SUBSCRIBE chan` + `PUBLISH chan msg` | Delivered |
| `SSUBSCRIBE chan` + `SPUBLISH chan msg` | Delivered |
| `SUBSCRIBE chan` + `SPUBLISH chan msg` | **Not delivered** |
| `SSUBSCRIBE chan` + `PUBLISH chan msg` | **Not delivered** |

Pattern subscriptions (PSUBSCRIBE) match only broadcast PUBLISH messages. There is no pattern equivalent for sharded pub/sub.

## Connection State

Once a connection enters pub/sub mode, it can only execute:
- SUBSCRIBE / UNSUBSCRIBE
- PSUBSCRIBE / PUNSUBSCRIBE
- SSUBSCRIBE / SUNSUBSCRIBE
- PING
- QUIT

All other commands return an error until the connection unsubscribes from all channels.

## Subscription Limits

FrogDB enforces limits to prevent resource exhaustion:

**Per-Connection:**

| Limit | Default |
|-------|---------|
| Channel subscriptions | 10,000 |
| Pattern subscriptions | 1,000 |
| Sharded channel subscriptions | 10,000 |

**Per-Shard:**

| Limit | Default |
|-------|---------|
| Total subscriptions | 1,000,000 |
| Unique channels | 100,000 |
| Unique patterns | 10,000 |

These limits are configurable via `[pubsub]` settings. See [Configuration](/operations/configuration/).

When a limit is exceeded, the subscription is rejected with an error and existing subscriptions remain unaffected.

## Cluster Behavior

**Broadcast pub/sub (PUBLISH):** Messages are forwarded to all cluster nodes via the cluster bus. Subscribers on any node receive the message.

**Sharded pub/sub (SPUBLISH):** Messages are routed only to the node owning the channel's slot. No cluster-wide broadcast occurs, making it more efficient at scale.

| Aspect | Broadcast | Sharded |
|--------|-----------|---------|
| Cluster bus traffic | O(nodes x messages) | O(1) per message |
| Scaling behavior | Degrades with cluster size | Scales horizontally |

### Failover

Pub/sub subscriptions are connection-bound and not replicated. When a node fails:
- Subscriptions on the failed node are lost
- Clients must reconnect and resubscribe
- Messages published during the failover window are lost

For applications requiring message durability across failovers, use Redis Streams with consumer groups.

### Slot Migration (Sharded Channels)

During slot migration, the source node actively unsubscribes clients from migrated sharded channels and sends a `-MOVED` redirect. Clients must resubscribe on the new owner node. Messages published between migration completion and client resubscription are lost (inherent to at-most-once delivery).

## Best Practices

1. **Use sharded pub/sub** for high-throughput workloads -- it avoids cluster-wide broadcasting
2. **Reuse channels** instead of creating a unique channel per entity
3. **Use patterns (PSUBSCRIBE) sparingly** -- each pattern requires regex evaluation on every PUBLISH
4. **Unsubscribe** when no longer needed to free resources
5. **Track subscriptions client-side** for automatic resubscription on reconnect
