---
title: "Protocol Integration"
description: "RESP2/RESP3 frame processing, the FrogDbResp2 codec, and wire protocol details for contributors."
sidebar:
  order: 12
---
How FrogDB parses and serializes the Redis wire protocol, for contributors.

## Overview

FrogDB implements RESP2 and RESP3 and targets compatibility with Redis 8.x
commands. On the inbound path, wire bytes are decoded into a `ParsedCommand`; on
the outbound path, a command handler's `Response` is encoded as RESP2 or RESP3
frames depending on the version negotiated for that connection. Parsing is built
on the `redis-protocol` crate (the wire codec) wrapped by a FrogDB-specific
codec that tolerates the protocol edge cases real Redis clients emit. The
`redis-protocol` version is pinned in the workspace `Cargo.toml`; it is not
restated here because it is a drift liability (it has already changed once).

After parsing, command dispatch and execution are covered by
[request-flows.md](/architecture/request-flows/) and
[execution.md](/architecture/execution/). This page covers the codec and type
system only, not per-command RESP shapes (those are documented by redis.io and
the command matrix).

## Decoding: the FrogDbResp2 codec

A command decodes into an internal representation
(`frogdb-server/crates/protocol/src/command.rs`):

```rust
pub struct ParsedCommand {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}
```

The connection does not drive the upstream codec directly. It uses `FrogDbResp2`
(`frogdb-server/crates/server/src/connection/codec.rs`), a wrapper that holds the
upstream `redis_protocol::codec::Resp2` in an `inner` field and pre-processes the
raw `BytesMut` buffer before handing it to the strict upstream decoder. The
upstream decoder rejects inputs that Redis itself tolerates; `FrogDbResp2`
handles those cases so behavior matches Redis:

- **Empty lines** (`\r\n` with no type prefix) are silently consumed.
- **Negative multibulk counts** (`*-N\r\n` with `N < -1`) are silently consumed;
  `*-1\r\n` is the RESP2 null array.
- **Oversized multibulk counts** (`*N` with `N > PROTO_MAX_MULTIBULK_LEN`) are
  rejected with a protocol error; the offending line is consumed so the
  connection can continue.
- **Oversized bulk lengths** (`$N` with `N > PROTO_MAX_BULK_LEN`), including bulk
  elements nested inside a multibulk, are rejected the same way — the buffer is
  pre-scanned so the upstream decoder never waits for `N` bytes that will never
  be valid.
- **Inline (telnet-style) commands** — any top-level line whose first byte is not
  a RESP type prefix (`* $ + - :`) is parsed with Redis `sdssplitargs` semantics
  into a multibulk request, bounded by `PROTO_INLINE_MAX_SIZE` (64 KiB). This
  inline handling is scoped to client connections; replication apply and slot
  migration drive strict decoders that never see this wrapper.

Decoding produces a RESP2 `BytesFrame`, which `ParsedCommand::try_from` converts.
RESP3 output is produced separately on the encode path (see
[the response type system](#the-response-type-system)). Parse failures are
reported as `ProtocolError`
(`frogdb-server/crates/protocol/src/error.rs`):

| Variant | Meaning |
|---------|---------|
| `EmptyCommand` | Command array had no command name |
| `ExpectedArray` | Top-level frame was not an array |
| `InvalidFrame` | Frame was malformed |
| `Incomplete` | Need more bytes; the codec buffers and waits |
| `FrameTooLarge` | Frame exceeded a protocol limit |

## The response type system

Command handlers return a `Response`
(`frogdb-server/crates/protocol/src/response.rs`). The wire-serializable subset
is modeled by `WireResponse`, whose `to_resp2_frame()` / `to_resp3_frame()`
methods cannot panic over the frames they encode. `Response` additionally carries
internal, non-wire control-flow variants (`BlockingNeeded`, `RaftNeeded`,
`MigrateNeeded`, `SlotMigrationNeeded`) that dispatch intercepts before
serialization — they never reach the wire.

`WireResponse` covers the RESP2 types (`Simple`, `Error`, `Integer`, `Bulk`,
`Array`) and the RESP3 types below, plus `NullArray`:

| Type | Wire Format | Use Case |
|------|-------------|----------|
| Null | `_\r\n` | Explicit null (RESP3) |
| Double | `,3.14\r\n` | Floating point (scores, etc.) |
| Boolean | `#t\r\n` / `#f\r\n` | True/false values |
| Map | `%<n>\r\n...` | Key-value pairs (HGETALL, etc.) |
| Set | `~<n>\r\n...` | Unordered unique elements |
| Push | `><n>\r\n...` | Out-of-band pub/sub messages |
| BigNumber | `(<num>\r\n` | Arbitrary precision integers |
| VerbatimString | `=<n>\r\n<fmt>:...` | Formatted text (markdown, etc.) |
| Attribute | `\|<n>\r\n...` | Metadata without breaking clients |

### RESP2 has two distinct null shapes

RESP2 distinguishes a **null bulk** (`$-1\r\n`) from a **null array**
(`*-1\r\n`), and RESP3 adds a third, explicit null (`_\r\n`):

- `WireResponse::Bulk(None)` encodes `$-1\r\n`.
- `WireResponse::NullArray` encodes `*-1\r\n` (used, e.g., by `LPOP`/`RPOP` with
  a count against a missing key). The upstream crate cannot produce `*-1\r\n`
  (its null frame is always `$-1\r\n`), so a top-level `NullArray` is diverted to
  the codec's `Resp2Outbound::NullArray` path; a *nested* `NullArray` encodes as
  the nested null `$-1\r\n`.
- `WireResponse::Null` encodes the RESP3 `_\r\n`.

### Benefits of RESP3

- **Type-rich responses**: maps, sets, and booleans reduce client parsing
  ambiguity.
- **Out-of-band push**: cleaner pub/sub without inline message interleaving.
- **Explicit nulls**: `_\r\n` instead of RESP2's overloaded `$-1\r\n` / `*-1\r\n`.
- **Native doubles**: no string conversion for `ZSCORE` and similar.

## Protocol negotiation (HELLO)

New connections start in RESP2. A client sends `HELLO 3` to switch to RESP3 (or
`HELLO 2` to switch back). `handle_hello`
(`frogdb-server/crates/server/src/connection/auth_conn_command.rs`) validates the
requested version before mutating anything:

| HELLO argument | Behavior |
|----------------|----------|
| `HELLO` (no version) | Return connection info in the current protocol |
| `HELLO 2` | Set the connection to RESP2, return info in RESP2 |
| `HELLO 3` | Set the connection to RESP3, return info in RESP3 |
| version outside `2..=3` (including 0, 1, 4+) | `-NOPROTO sorry, this protocol version is not supported` |

The protocol version is set unconditionally once validated, so **downgrade is
allowed**: a connection that did `HELLO 3` may `HELLO 2` back to RESP2 within the
same session. Clients that never send `HELLO` behave exactly as with Redis
(default RESP2). A rejected version leaves the connection's protocol untouched.

## Client tracking (server-assisted caching)

FrogDB supports server-assisted client-side caching over RESP3 push
invalidation. When a client reads a key with tracking enabled, the server records
the association; when the key is later modified, the server sends an invalidation
push frame so the client can evict its cache.

Tracking has one mode enum plus separate flags — the seven `CLIENT TRACKING`
options are not all "modes"
(`frogdb-server/crates/server/src/connection/state.rs`):

- `TrackingMode` enum values: `Default`, `OptIn`, `OptOut`, `Broadcast`.
- `NOLOOP` is a boolean (do not invalidate keys this connection itself modified).
- `PREFIX` populates a list of registered prefixes (BCAST mode).
- `REDIRECT` is a target connection id (`0` = no redirect).

Validation rules (`TrackingEnableError`) mirror Redis:

- `PREFIX` requires `BCAST`.
- `OPTIN` and `OPTOUT` are mutually exclusive, and neither is compatible with
  `BCAST`.
- You cannot switch `BCAST` on/off, or switch between `OPTIN`/`OPTOUT`, without
  first disabling tracking.
- Prefixes registered on one connection must not overlap.

## Protocol limits

The frame limits are compile-time constants in the codec, not `CONFIG`-tunable
fields (`frogdb-server/crates/server/src/connection/codec.rs`):

| Constant | Value | Meaning |
|----------|-------|---------|
| `PROTO_MAX_BULK_LEN` | 512 MiB (`536870912`) | Maximum single bulk-string length |
| `PROTO_MAX_MULTIBULK_LEN` | `1048576` (1024×1024) | Maximum elements in a multibulk request |
| `PROTO_INLINE_MAX_SIZE` | 64 KiB | Maximum inline (telnet-style) line length |

Exceeding a bulk or multibulk limit yields a `redis-protocol` decode error whose
offending line is consumed so the connection can continue (see below). There is
no `frame_timeout_ms`, `max_frame_size`, or `max_array_elements` config field.

## Error handling and framing recovery

Command-level errors (wrong type, wrong arity, unknown command) are returned as
`-ERR ...` replies and never close the connection, matching Redis.

Framing errors are also recoverable in FrogDB. When the codec returns a decode
error (empty command, non-array top level, oversized bulk/multibulk length), the
connection replies with `-ERR Protocol error: ...` — using the error's `details()`
so the wire form omits the upstream `Decode Error:` kind prefix — and **continues
reading** rather than closing. The malformed leading line has already been
consumed by `FrogDbResp2`, so the stream can resynchronize. The connection closes
on client disconnect or an unrecoverable write/flush failure, not on ordinary
protocol errors.
