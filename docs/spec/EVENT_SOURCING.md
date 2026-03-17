# Event Sourcing (ES.*)

**FrogDB-proprietary extension.** Not available in Redis, Valkey, or DragonflyDB.

Event Sourcing primitives built on top of Redis Streams. Provides optimistic concurrency
control (OCC), version-based reads, snapshot-accelerated replay, idempotent writes, and
a global `$all` stream.

## Commands

### ES.APPEND

Append an event with optimistic concurrency control.

```
ES.APPEND key expected_version event_type data [field value ...] [IF_NOT_EXISTS idem_key]
```

- `expected_version`: must equal the stream's current version (0 for first append)
- `event_type`: stored as the `event_type` field in the stream entry
- `data`: stored as the `data` field
- Additional `field value` pairs are stored alongside event_type and data
- `IF_NOT_EXISTS idem_key`: idempotency deduplication (bounded FIFO, 10K keys)

**Returns:** `[new_version, stream_id]` on success.

**Errors:**
- `VERSIONMISMATCH expected N actual M` if version check fails
- Idempotent duplicate returns `[existing_version, null]` (not an error)

**Execution:** Standard, WRITE, WAL: PersistFirstKey, wakes Stream waiters.

### ES.READ

Read events by version range.

```
ES.READ key start_version [end_version] [COUNT n]
```

- `start_version`: 1-based inclusive start
- `end_version`: inclusive end (default: latest)
- `COUNT n`: limit number of returned events

**Returns:** Array of `[version, stream_id, [field, value, ...]]` tuples.

**Execution:** Standard, READONLY.

### ES.REPLAY

Replay all events, optionally starting from a snapshot.

```
ES.REPLAY key [SNAPSHOT snapshot_key]
```

- `SNAPSHOT snapshot_key`: read snapshot from this key, replay only events after its version
- Both keys must hash to the same slot (use hash tags like `{order}:events` / `{order}:snapshot`)

**Returns:** `[snapshot_state_or_null, [events...]]`

**Execution:** Standard, READONLY, requires_same_slot.

### ES.INFO

Get event stream metadata.

```
ES.INFO key
```

**Returns:** Flat array of key-value pairs:
- `version` (i64): total events appended (monotonic)
- `entries` (i64): current entry count (may differ from version if XDEL was used)
- `first-id`: first stream entry ID or null
- `last-id`: last stream entry ID or null
- `idempotency-keys` (i64): number of tracked idempotency keys

**Execution:** Standard, READONLY.

### ES.SNAPSHOT

Store a snapshot at a given version.

```
ES.SNAPSHOT snapshot_key version state
```

- `snapshot_key`: destination key (stored as a string value)
- `version`: the event version this snapshot represents
- `state`: opaque state data (e.g., JSON)

The snapshot is stored as `"version:state"` for ES.REPLAY to parse.

**Execution:** Standard, WRITE, WAL: PersistFirstKey.

### ES.ALL

Read the global event stream across all shards.

```
ES.ALL [COUNT n] [AFTER stream_id]
```

- `COUNT n`: limit total entries returned
- `AFTER stream_id`: return entries after this stream ID

**Returns:** Array of `[stream_id, [field, value, ...]]` tuples, sorted by StreamId.

The `$all` stream is per-shard (`__frogdb:es:all`), non-durable, and merged via
scatter-gather. Ordering is approximate (by StreamId timestamp, may interleave within
the same millisecond across shards).

**Execution:** ServerWide(EsAll), READONLY.

## Design Decisions

### Version = `total_appended`, not `len()`

A monotonic counter that increments on every append and never decrements. Using `len()`
would break if XDEL were called on the stream.

### Snapshot key is explicit

The user passes both keys explicitly. Colocation is the user's responsibility via hash
tags, same as any Redis multi-key pattern.

### $all stream is non-durable

Lost on restart. Bounded to 100,000 entries per shard (trimmed on each ES.APPEND).
Reconstructable from individual event streams via background scan (future enhancement).

### OCC is race-free in FrogDB

FrogDB's thread-per-core model executes commands on the same shard serially. Concurrent
ES.APPEND with the same expected_version: exactly one wins, the other gets VERSIONMISMATCH.

### Idempotency eviction

After 10,000 subsequent events, old idempotency keys are evicted (FIFO). Late retries
after eviction will result in duplicate appends.

## Limitations

- Mixing XADD and ES.APPEND on the same stream breaks version semantics (XADD doesn't
  increment `total_appended`). This is undefined behavior for v1.
- XDEL on ES streams: versions are permanent but entries are removed. `range_by_version()`
  handles gaps. Prefer not to XDEL on ES streams.
- ES.ALL on replicas returns empty (the `$all` stream is not WAL'd or replicated).
- ES.SNAPSHOT version is not validated against the event stream (trust-the-client).
- `range_by_version()` is O(N) scan — acceptable for v1.
