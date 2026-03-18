# FrogDB Stream Commands

Redis Streams implementation for append-only log data structures with consumer groups.

**Status:** Implemented (basic commands: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XDEL, XSETID; consumer groups and XREADGROUP also available)

## Overview

Streams are append-only log data structures ideal for:
- Event sourcing
- Message queues
- Activity feeds
- Time-series data

Each stream entry has:
- **ID**: Timestamp-based unique identifier (`<millisecondsTime>-<sequenceNumber>`)
- **Fields**: Key-value pairs (like a hash)

---

## Basic Commands

### XADD

Append entry to stream:

```
XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `key` | Stream key |
| `NOMKSTREAM` | Don't create stream if doesn't exist |
| `MAXLEN ~ N` | Trim to approximately N entries |
| `MINID ~ id` | Trim entries older than ID |
| `*` | Auto-generate ID (recommended) |
| `id` | Explicit ID (must be greater than last) |

**Returns:** Entry ID (e.g., `"1704825600000-0"`)

**Example:**
```
XADD mystream * sensor-id 1234 temperature 23.5
"1704825600000-0"

XADD mystream MAXLEN ~ 1000 * event click user-id 42
"1704825600001-0"
```

### XLEN

Get stream length:

```
XLEN key
```

**Returns:** Integer count of entries

### XRANGE / XREVRANGE

Read entries in ID range:

```
XRANGE key start end [COUNT count]
XREVRANGE key end start [COUNT count]
```

**Special IDs:**
- `-` : Minimum ID
- `+` : Maximum ID
- `<id>` : Specific ID (inclusive)

**Example:**
```
XRANGE mystream - + COUNT 10
XRANGE mystream 1704825600000-0 1704825700000-0
XREVRANGE mystream + - COUNT 5
```

### XREAD

Read from one or more streams:

```
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `COUNT` | Max entries per stream |
| `BLOCK` | Block until data available (0 = forever) |
| `STREAMS` | Required keyword |
| `id` | Read entries after this ID; `$` = only new entries |

**Example:**
```
# Read new entries, block up to 5 seconds
XREAD BLOCK 5000 STREAMS mystream $

# Read from multiple streams
XREAD COUNT 10 STREAMS stream1 stream2 0 0
```

### XTRIM

Trim stream to size:

```
XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
```

- `=` : Exact trimming
- `~` : Approximate (more efficient)

### XDEL

Delete specific entries:

```
XDEL key id [id ...]
```

**Returns:** Count of deleted entries

### XDELEX

Extended delete with fine-grained control over consumer group PEL references:

```
XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
```

**Strategies:**
| Strategy | Behavior |
|----------|----------|
| `KEEPREF` | (default) Delete entry, preserve PEL references — same as XDEL |
| `DELREF` | Delete entry AND remove all PEL references across all groups |
| `ACKED` | Only delete entries acknowledged by ALL consumer groups |

**Returns:** Array of per-ID results:
- `-1` — entry not found
- `1` — entry deleted
- `2` — entry not deleted (ACKED mode only, still has pending refs)

**IDS block:** The `numids` count must match the actual number of IDs provided.

---

## Consumer Groups

Consumer groups enable parallel processing with at-least-once delivery semantics.

### XGROUP CREATE

Create consumer group:

```
XGROUP CREATE key group id|$ [MKSTREAM] [ENTRIESREAD entries-read]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `key` | Stream key |
| `group` | Group name |
| `id` | Start reading from this ID |
| `$` | Only new messages |
| `0` | All existing messages |
| `MKSTREAM` | Create stream if doesn't exist |

**Example:**
```
XGROUP CREATE mystream mygroup $ MKSTREAM
```

### XGROUP DESTROY

Delete consumer group:

```
XGROUP DESTROY key group
```

### XGROUP CREATECONSUMER

Explicitly create consumer:

```
XGROUP CREATECONSUMER key group consumer
```

### XGROUP DELCONSUMER

Remove consumer from group:

```
XGROUP DELCONSUMER key group consumer
```

**Returns:** Number of pending entries that were released

### XGROUP SETID

Change group's last-delivered ID:

```
XGROUP SETID key group id|$ [ENTRIESREAD entries-read]
```

---

## Reading with Consumer Groups

### XREADGROUP

Read as consumer in group:

```
XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
```

**Special ID `>`:** Read only new (undelivered) messages

**Example:**
```
# Consumer "alice" reads new messages from group "workers"
XREADGROUP GROUP workers alice COUNT 10 BLOCK 5000 STREAMS tasks >

# Re-read pending messages (for retry)
XREADGROUP GROUP workers alice STREAMS tasks 0
```

### XACK

Acknowledge message processing:

```
XACK key group id [id ...]
```

**Returns:** Count of successfully acknowledged entries

**Example:**
```
XACK mystream mygroup 1704825600000-0 1704825600001-0
```

### XACKDEL

Atomic acknowledge + conditional delete:

```
XACKDEL key group [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
```

First acknowledges each entry in the specified group, then deletes based on strategy.
Uses the same KEEPREF/DELREF/ACKED strategies as XDELEX.

**Returns:** Array of per-ID results:
- `-1` — entry not found
- `1` — acknowledged and deleted
- `2` — acknowledged but not deleted (ACKED mode, other groups still pending)

**Errors:** Returns `-NOGROUP` if the consumer group does not exist.

### XCLAIM

Claim pending entries from another consumer:

```
XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME unix-time-ms] [RETRYCOUNT count] [FORCE] [JUSTID]
```

**Use case:** Reassign stuck messages when a consumer dies

**Example:**
```
# Claim messages idle > 60 seconds
XCLAIM mystream mygroup alice 60000 1704825600000-0
```

### XAUTOCLAIM

Automatically claim idle pending entries:

```
XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
```

**Returns:** New scan cursor and claimed entries

### XPENDING

View pending entries (delivered but not acknowledged):

```
XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
```

**Summary mode:**
```
XPENDING mystream mygroup
1) (integer) 10               # Total pending
2) "1704825600000-0"          # First pending ID
3) "1704825600100-0"          # Last pending ID
4) 1) 1) "alice"              # Per-consumer counts
      2) "5"
   2) 1) "bob"
      2) "5"
```

---

## Stream Information

### XINFO STREAM

Get stream metadata:

```
XINFO STREAM key [FULL [COUNT count]]
```

### XINFO GROUPS

List consumer groups:

```
XINFO GROUPS key
```

### XINFO CONSUMERS

List consumers in group:

```
XINFO CONSUMERS key group
```

---

## Sharding Considerations

### Single-Shard Requirement

Consumer groups require all operations on the **same shard**. Use hash tags:

```
# All stream operations use same hash tag
XADD {order-events}:stream * ...
XGROUP CREATE {order-events}:stream workers $
XREADGROUP GROUP workers consumer1 STREAMS {order-events}:stream >
```

### Cross-Shard Streams

Reading from multiple streams across shards works via scatter-gather:

```
# Each stream can be on different shard
XREAD STREAMS {shard1}:events {shard2}:events 0 0
```

**Limitation:** Consumer groups cannot span multiple shards.

---

## Data Model

### Stream Entry

```rust
pub struct StreamEntry {
    /// Unique ID: milliseconds-sequence
    pub id: StreamId,
    /// Field-value pairs
    pub fields: Vec<(Bytes, Bytes)>,
}

pub struct StreamId {
    pub ms: u64,       // Unix timestamp milliseconds
    pub seq: u64,      // Sequence within millisecond
}
```

### Consumer Group

```rust
pub struct ConsumerGroup {
    /// Group name
    pub name: Bytes,
    /// Last delivered ID
    pub last_delivered_id: StreamId,
    /// Pending entries list (PEL)
    pub pending: BTreeMap<StreamId, PendingEntry>,
    /// Active consumers
    pub consumers: HashMap<Bytes, Consumer>,
}

pub struct PendingEntry {
    pub consumer: Bytes,
    pub delivery_time: Instant,
    pub delivery_count: u32,
}

pub struct Consumer {
    pub name: Bytes,
    pub pending_count: usize,
    pub last_seen: Instant,
}
```

### Storage

Streams are stored in RocksDB with sorted keys:

```
stream:{key}:entries:{id} -> serialized entry
stream:{key}:meta -> length, first_id, last_id
stream:{key}:groups:{group} -> group metadata
stream:{key}:groups:{group}:pel:{id} -> pending entry
stream:{key}:groups:{group}:consumers:{name} -> consumer metadata
```

---

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `stream_max_entries` | 0 | Global max entries per stream (0 = unlimited) |
| `stream_node_max_bytes` | 4096 | Internal radix tree node size |

---

## Error Responses

| Error | Cause |
|-------|-------|
| `-WRONGTYPE` | Key exists but is not a stream |
| `-NOGROUP` | Consumer group doesn't exist |
| `-BUSYGROUP` | Consumer group already exists |
| `-ERR Invalid stream ID` | Malformed ID format |
| `-ERR CROSSSLOT` | Consumer group operation spans shards |

---

## References

- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [COMMANDS.md](../COMMANDS.md) - Command reference index
- [CONCURRENCY.md](../CONCURRENCY.md) - Sharding and hash tags
