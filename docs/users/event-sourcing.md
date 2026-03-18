# Event Sourcing (ES.*)

FrogDB provides first-class event sourcing primitives built on top of Redis Streams. These commands are a FrogDB extension and are not available in Redis, Valkey, or DragonflyDB.

The ES.* commands provide optimistic concurrency control (OCC), version-based reads, snapshot-accelerated replay, idempotent writes, and a global `$all` stream.

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

### ES.READ

Read events by version range.

```
ES.READ key start_version [end_version] [COUNT n]
```

- `start_version`: 1-based inclusive start
- `end_version`: inclusive end (default: latest)
- `COUNT n`: limit number of returned events

**Returns:** Array of `[version, stream_id, [field, value, ...]]` tuples.

### ES.REPLAY

Replay all events, optionally starting from a snapshot.

```
ES.REPLAY key [SNAPSHOT snapshot_key]
```

- `SNAPSHOT snapshot_key`: read snapshot from this key, replay only events after its version
- Both keys must hash to the same slot (use hash tags like `{order}:events` / `{order}:snapshot`)

**Returns:** `[snapshot_state_or_null, [events...]]`

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

### ES.SNAPSHOT

Store a snapshot at a given version.

```
ES.SNAPSHOT snapshot_key version state
```

- `snapshot_key`: destination key (stored as a string value)
- `version`: the event version this snapshot represents
- `state`: opaque state data (e.g., JSON)

The snapshot is stored as `"version:state"` for ES.REPLAY to parse.

### ES.ALL

Read the global event stream across all shards.

```
ES.ALL [COUNT n] [AFTER stream_id]
```

- `COUNT n`: limit total entries returned
- `AFTER stream_id`: return entries after this stream ID

**Returns:** Array of `[stream_id, [field, value, ...]]` tuples, sorted by StreamId.

The `$all` stream is non-durable (lost on restart) and merged via scatter-gather across shards. Ordering is approximate -- entries within the same millisecond across shards may interleave.

## Usage Notes

**Version semantics.** The version counter is monotonically increasing and increments on every ES.APPEND. It tracks total appends rather than current entry count, so it remains accurate even if XDEL is used on the underlying stream.

**Snapshot key colocation.** When using ES.REPLAY with SNAPSHOT, both the event stream key and the snapshot key must hash to the same slot. Use hash tags to guarantee this:

```
ES.APPEND {order}:events 0 OrderCreated '{"id":1}'
ES.SNAPSHOT {order}:snapshot 1 '{"id":1,"status":"created"}'
ES.REPLAY {order}:events SNAPSHOT {order}:snapshot
```

**Idempotency.** The `IF_NOT_EXISTS` option on ES.APPEND enables idempotent writes. Idempotency keys are stored in a bounded FIFO buffer (10,000 keys). After 10,000 subsequent events, old idempotency keys are evicted and late retries will result in duplicate appends.

**Concurrency.** FrogDB's thread-per-core model executes commands on the same shard serially, making OCC race-free. Concurrent ES.APPEND calls with the same expected_version: exactly one succeeds, the other gets a VERSIONMISMATCH error.

## Limitations

- Mixing XADD and ES.APPEND on the same stream breaks version semantics (XADD does not increment the version counter). Avoid this.
- XDEL on ES streams removes entries but versions are permanent. `ES.READ` handles gaps correctly, but prefer not to XDEL on ES streams.
- ES.ALL returns empty on replicas (the `$all` stream is not replicated).
- ES.SNAPSHOT version is not validated against the event stream (trust-the-client).
- The `$all` stream is bounded to 100,000 entries per shard, trimmed on each ES.APPEND.
