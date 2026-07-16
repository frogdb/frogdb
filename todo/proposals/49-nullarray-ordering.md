# 49 — RESP2 NullArray replies desynchronize pipelined batches

**Status:** proposed (2026-07-16, round 6)
**Severity:** 🔴 Correctness (wire-protocol reply reordering, verified by code read)
**Found:** round-6 deepening review fan-out over the connection layer.

## Problem

`frame_io.rs` has two write paths with different buffering disciplines that can interleave
out of order on RESP2:

- Normal frames go through the `Framed` codec: `feed_wire_response`
  (`frogdb-server/crates/server/src/connection/frame_io.rs:132-168`) calls `self.framed.feed(frame)`,
  which appends encoded bytes to the codec's **write buffer** without flushing (deliberate write
  coalescing for pipelined batches; `flush_responses` drains it at batch end).
- `WireResponse::NullArray` bypasses the codec: the `redis-protocol` RESP2 encoder cannot produce
  `*-1\r\n` (its `Null` is always `$-1\r\n`), so `write_null_array` (`frame_io.rs:53-77`) writes the
  raw literal with `self.framed.get_mut().write_all(NULL_ARRAY_BYTES)` — **straight to the
  underlying socket**, skipping the codec's write buffer entirely.

A pipelined RESP2 batch whose replies are e.g. `[Bulk, NullArray, Bulk]` therefore hits the wire
as `*-1\r\n` **first**, then the two buffered bulk frames at flush time — the client attributes
replies to the wrong commands from that point on. Any RESP2 pipeline containing a null-array
reply (`EXEC` on a failed transaction, `BLPOP`/`BLMPOP` timeout shapes routed through non-blocking
paths, `LPOP key count` on a missing key, `GEOPOS` edge shapes, …) desynchronizes the connection.

The `send` path is unaffected in practice (it flushes after every response, so ordering degenerates
to write order), and the RESP3 branches of both paths write every frame via `get_mut().write_all`
in feed order — consistently unbuffered, so no reordering.

Why tests missed it: there is no encoder-level byte-stream test; the behavior is only reachable
through full-server socket tests, which mostly issue one command per round-trip.

## Design

Keep the upstream `redis-protocol` codec (no fork). Add a single "queue raw wire bytes in-order"
primitive: tokio-util 0.7's `Framed` exposes `write_buffer_mut()`, so the RESP2 NullArray branch
becomes

```rust
self.framed.write_buffer_mut().extend_from_slice(NULL_ARRAY_BYTES);
```

which lands the literal **in the codec's write buffer, in feed order** with the surrounding
frames. Concretely:

- `write_null_array` stops doing socket I/O on the RESP2 arm; it queues into the write buffer.
  The `*-1\r\n` literal and the protocol branch stay here and nowhere else (proposal 26's seam).
- `feed_wire_response`'s NullArray early-return keeps its no-flush policy — now actually true.
- `send_wire_response`'s NullArray arm keeps its flush-after policy (queue + flush).
- RESP3 arm unchanged (already write-order-consistent).

## Tests

1. **Encoder-level byte-stream test (new seam):** drive the connection encoder with
   `[Bulk("a"), NullArray, Bulk("b")]` via the feed path on RESP2 over an in-memory duplex, then
   flush; assert the exact byte sequence `$1\r\na\r\n*-1\r\n$1\r\nb\r\n`. Red before, green after.
2. Same-shape RESP3 test pinning `_\r\n` ordering (already correct — pins it).
3. Pipelined integration regression: one batch `GET k1; LPOP missing 2; GET k2`, assert replies
   arrive in command order with correct values.

## Verify

`just test frogdb-server` + the new regression red-before/green-after.
