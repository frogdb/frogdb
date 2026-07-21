---
title: "Event Sourcing (ES.*)"
description: "Canonical reference for FrogDB's ES.* event sourcing commands: syntax, arguments, return shapes, errors, and cross-cutting semantics."
sidebar:
  order: 2
---

Event Sourcing (`ES.*`) is a FrogDB-original command family built on top of
Redis Streams. It is not available in Redis, Valkey, or DragonflyDB, so no
upstream documentation exists — this page is the canonical reference. See the
[Extensions overview](/extensions/overview/) for how it fits alongside the
other extension families.

The family adds five capabilities on top of a stream:

- **Optimistic concurrency control (OCC)** — appends carry an expected version
  and fail if another writer advanced the stream first.
- **Version-based reads** — read events by an inclusive version range rather
  than by stream ID.
- **Snapshot-accelerated replay** — replay only the events after a stored
  snapshot version.
- **Idempotent writes** — deduplicate retried appends with a caller-supplied
  key.
- **A global `$all` stream** — a per-shard index of every appended event,
  readable server-wide with `ES.ALL`.

ES.* commands are built on Redis Streams but do not restate stream behavior.
For the semantics of the underlying stream commands (`XADD`, `XREAD`, `XDEL`,
and so on) see [redis.io](https://redis.io/docs/latest/develop/data-types/streams/).
This page documents only the event-sourcing layer.

## Commands

### ES.APPEND

Append an event with optimistic concurrency control.

```
ES.APPEND key expected_version event_type data [field value ...] [IF_NOT_EXISTS idem_key]
```

- `expected_version` — must equal the stream's current version. Use `0` for the
  first append. The version is the count of appends so far (see
  [Version semantics](#version-semantics)).
- `event_type` — stored as the `event_type` field of the stream entry.
- `data` — stored as the `data` field.
- `field value ...` — optional additional field/value pairs stored alongside
  `event_type` and `data`.
- `IF_NOT_EXISTS idem_key` — idempotency key. If this key was seen recently on
  this stream, the append is skipped (see [Idempotency](#idempotency)).

If the key does not exist it is created. The command wakes any blocked stream
readers waiting on the key.

**Returns:** on success, a two-element array `[new_version, stream_id]`, where
`new_version` is an integer and `stream_id` is the underlying stream entry ID.
On an idempotent no-op, `[existing_version, nil]` — the version is returned and
the second element is nil, indicating no new entry was written.

**Errors:**

- `VERSIONMISMATCH expected N actual M` — `expected_version` did not equal the
  stream's current version `M`. No event is appended.
- `WRONGTYPE Operation against a key holding the wrong kind of value` — `key`
  holds a value that is not a stream.
- `ERR value is not an integer or out of range` — `expected_version` is not a
  non-negative integer.
- `ERR syntax error` — a trailing field without a value, or `IF_NOT_EXISTS`
  without a key.

### ES.READ

Read events by version range.

```
ES.READ key start_version [end_version] [COUNT n]
```

- `start_version` — 1-based inclusive start. Version `1` is the first event ever
  appended. A `start_version` of `0`, or one past the last version, returns an
  empty array.
- `end_version` — inclusive end. Defaults to the latest version.
- `COUNT n` — cap the number of events returned.

**Returns:** an array of events, each shaped `[version, stream_id, [field, value, ...]]`.
The fields array always begins with `event_type` and `data`, followed by any
extra field/value pairs supplied at append time.

**Errors:**

- `WRONGTYPE Operation against a key holding the wrong kind of value` — `key`
  holds a value that is not a stream. A missing key is not an error; it returns
  an empty array.
- `ERR value is not an integer or out of range` — `start_version`,
  `end_version`, or the `COUNT` argument is not a valid integer.

### ES.REPLAY

Replay events, optionally continuing from a stored snapshot.

```
ES.REPLAY key [SNAPSHOT snapshot_key]
```

- `SNAPSHOT snapshot_key` — read a snapshot (written by
  [`ES.SNAPSHOT`](#essnapshot)) from `snapshot_key` and replay only the events
  after the snapshot's version. Without `SNAPSHOT`, all events are replayed.
- When `SNAPSHOT` is used, `key` and `snapshot_key` must hash to the same
  cluster slot. Use a [hash tag](#snapshot-key-colocation) to guarantee this.

**Returns:** a two-element array `[snapshot_state, [events...]]`. `snapshot_state`
is the stored snapshot payload, or nil when no `SNAPSHOT` was given or the
snapshot key does not exist. `events` is an array of
`[version, stream_id, [field, value, ...]]` entries, in the same shape as
[`ES.READ`](#esread). A missing event stream returns `[nil, []]`.

**Errors:**

- `WRONGTYPE Operation against a key holding the wrong kind of value` — `key` is
  not a stream, or `snapshot_key` is not a string.
- `ERR invalid snapshot format` / `ERR invalid snapshot version` — the snapshot
  value is not in the `version:state` form written by `ES.SNAPSHOT`.
- `ERR syntax error` — an argument other than `SNAPSHOT` was supplied.

### ES.INFO

Return metadata for an event stream.

```
ES.INFO key
```

**Returns:** a flat array of label/value pairs:

- `version` — total events ever appended (monotonic; see
  [Version semantics](#version-semantics)).
- `entries` — current number of entries in the underlying stream. This can be
  lower than `version` if `XDEL` was used on the stream.
- `first-id` — the first stream entry ID, or nil if empty.
- `last-id` — the last stream entry ID, or nil if empty.
- `idempotency-keys` — the number of idempotency keys currently retained.

A missing key returns nil.

**Errors:**

- `WRONGTYPE Operation against a key holding the wrong kind of value` — `key`
  holds a value that is not a stream.

### ES.SNAPSHOT

Store a snapshot of aggregated state at a given version.

```
ES.SNAPSHOT snapshot_key version state
```

- `snapshot_key` — destination key. The snapshot is stored as an ordinary string
  value in the form `version:state`, which [`ES.REPLAY`](#esreplay) parses back.
- `version` — the event version this snapshot represents.
- `state` — opaque snapshot payload (for example a serialized aggregate as JSON).

The `version` you pass is **not** validated against the event stream — the
command trusts the caller (see [Limitations](#limitations)). Storing a snapshot
overwrites any existing value at `snapshot_key`.

**Returns:** `OK`.

**Errors:**

- `ERR value is not an integer or out of range` — `version` is not a
  non-negative integer.

### ES.ALL

Read the global event index across all shards.

```
ES.ALL [COUNT n] [AFTER stream_id]
```

- `COUNT n` — cap the total number of entries returned, applied after merging
  results from all shards.
- `AFTER stream_id` — return only entries after this stream ID.

Every `ES.APPEND` also writes a small reference entry (source key, source stream
ID, and version) into a per-shard `$all` index. `ES.ALL` scatters to all shards,
gathers those reference entries, and merges them sorted by stream ID.

**Returns:** an array of `[stream_id, [field, value, ...]]` tuples sorted by
stream ID. Each entry's fields are the reference fields `key`, `id`, and
`version`.

**Errors:**

- `ERR syntax error` — an unrecognized argument.
- `ERR value is not an integer or out of range` — the `COUNT` argument is not an
  integer.
- `ERR Invalid stream ID specified` — the `AFTER` argument is not a valid stream
  ID.

The `$all` index is non-durable and node-local; on a replica `ES.ALL` returns an
empty array (see [Limitations](#limitations)).

## Usage notes

### Version semantics

The version counter increments on every successful `ES.APPEND` and never
decreases. It counts appends, not the current number of stream entries, so it
stays accurate even if `XDEL` removes entries from the underlying stream —
`ES.INFO` reports the append total as `version` and the surviving entry count as
`entries`.

`ES.READ` and `ES.REPLAY`, however, number events by their position among the
entries that are still present. As long as you never delete entries, that
position equals the append version and the two views agree. Deleting entries
breaks that correspondence (see [Limitations](#limitations)).

### Snapshot key colocation

`ES.REPLAY ... SNAPSHOT` reads two keys — the event stream and the snapshot — in
a single call, so both must live on the same cluster slot. Use a
[hash tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags)
(the `{...}` portion of a key) to force colocation:

```
ES.APPEND {order:42}:events 0 OrderCreated '{"id":42}'
ES.SNAPSHOT {order:42}:snapshot 1 '{"id":42,"status":"created"}'
ES.REPLAY {order:42}:events SNAPSHOT {order:42}:snapshot
```

Because both keys share the hash tag `{order:42}`, they always map to the same
slot and `ES.REPLAY` can read them together.

### Idempotency

`IF_NOT_EXISTS idem_key` makes an append idempotent: if the same `idem_key` was
seen recently on that stream, the append is a no-op and returns
`[existing_version, nil]` instead of writing a duplicate event.

Idempotency keys are retained in a bounded per-stream FIFO buffer. Once the
buffer is full the oldest keys are evicted, so a retry that arrives after enough
subsequent appends will no longer be recognized and will append again. Treat
idempotency as a short retry window, not a permanent dedup log.

### Optimistic concurrency

FrogDB routes each key to a single shard and executes that shard's commands
serially (see [Architecture → Concurrency model](/architecture/concurrency/)).
Two `ES.APPEND` calls with the same `expected_version` therefore cannot both
succeed: one appends and advances the version, the other observes the new
version and fails with `VERSIONMISMATCH`. The losing caller can re-read and
retry.

## Limitations

State these plainly:

- **Do not mix `XADD` with `ES.APPEND` on the same stream.** `XADD` writes an
  entry without advancing the event-sourcing version counter, which
  desynchronizes the version from the entries and breaks OCC.
- **Avoid `XDEL` on ES streams.** `ES.READ` and `ES.REPLAY` number events by the
  position of surviving entries, so deleting an entry renumbers every later
  event and pulls those version numbers out of step with the monotonic append
  counter reported by `ES.INFO`.
- **`ES.ALL` is node-local and non-durable.** The `$all` index is not written to
  the write-ahead log and is not replicated, so it is lost on restart and
  `ES.ALL` returns an empty array on replicas.
- **`ES.ALL` ordering is approximate across shards.** Entries are sorted by
  stream ID, but IDs are generated per shard; entries created within the same
  millisecond on different shards may interleave.
- **`ES.SNAPSHOT` trusts the caller.** The `version` recorded in a snapshot is
  not checked against the event stream. A wrong version causes `ES.REPLAY` to
  replay from the wrong point.
- **The `$all` index is bounded per shard** and trimmed on every `ES.APPEND`, so
  `ES.ALL` reflects only recent activity rather than complete history.
