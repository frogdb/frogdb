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
