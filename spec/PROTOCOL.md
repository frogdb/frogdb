# FrogDB Protocol Integration

FrogDB uses the [`redis-protocol`](https://crates.io/crates/redis-protocol) crate for RESP2/RESP3 parsing and encoding.

## Crate Configuration

```toml
[dependencies]
redis-protocol = { version = "5", features = ["bytes", "codec"] }
```

**Features:**
- `bytes` - Zero-copy parsing using `Bytes` type
- `codec` - Tokio codec for async streaming

## Frame Processing

### Inbound: Frame → ParsedCommand

The `redis-protocol` crate decodes wire data into `BytesFrame`. We convert to our internal `ParsedCommand`:

```rust
use redis_protocol::resp2::types::BytesFrame;

pub struct ParsedCommand {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

impl TryFrom<BytesFrame> for ParsedCommand {
    type Error = ProtocolError;

    fn try_from(frame: BytesFrame) -> Result<Self, Self::Error> {
        match frame {
            BytesFrame::Array(frames) => {
                let mut iter = frames.into_iter();
                let name = iter.next()
                    .and_then(|f| f.as_bytes())
                    .ok_or(ProtocolError::EmptyCommand)?;
                let args = iter
                    .filter_map(|f| f.as_bytes())
                    .collect();
                Ok(ParsedCommand { name, args })
            }
            _ => Err(ProtocolError::ExpectedArray),
        }
    }
}
```

### Outbound: Response → Frame

Our `Response` type encodes back to `BytesFrame`. The enum includes both RESP2 types (implemented)
and RESP3 types (defined for future use):

```rust
pub enum Response {
    // === RESP2 Types (Implemented) ===
    Simple(Bytes),              // +OK\r\n
    Error(Bytes),               // -ERR message\r\n
    Integer(i64),               // :1000\r\n
    Bulk(Option<Bytes>),        // $5\r\nhello\r\n or $-1\r\n (null)
    Array(Vec<Response>),       // *2\r\n...

    // === RESP3 Types (Defined, Not Yet Serialized) ===
    Null,                       // _\r\n
    Double(f64),                // ,3.14159\r\n
    Boolean(bool),              // #t\r\n or #f\r\n
    BlobError(Bytes),           // !<len>\r\n<bytes>\r\n
    VerbatimString {            // =<len>\r\n<fmt>:<data>\r\n
        format: [u8; 3],        // e.g., "txt", "mkd"
        data: Bytes,
    },
    Map(Vec<(Response, Response)>),  // %<count>\r\n<key><value>...
    Set(Vec<Response>),              // ~<count>\r\n<elements>...
    Attribute(Box<Response>),        // |<count>\r\n<attr-map><data>
    Push(Vec<Response>),             // ><count>\r\n<elements>...
    BigNumber(Bytes),                // (<big-integer>\r\n
}

/// Protocol version negotiated per connection
pub enum ProtocolVersion {
    Resp2,  // Default
    Resp3,  // Negotiated via HELLO command
}

impl From<Response> for BytesFrame {
    fn from(response: Response) -> Self {
        // Note: RESP3 variants will panic until RESP3 encoding is implemented
        match response {
            Response::Simple(s) => BytesFrame::SimpleString(s),
            Response::Error(e) => BytesFrame::SimpleError(e),
            Response::Integer(i) => BytesFrame::Number(i),
            Response::Bulk(Some(b)) => BytesFrame::BulkString(b),
            Response::Bulk(None) => BytesFrame::Null,
            Response::Array(items) => BytesFrame::Array(
                items.into_iter().map(Into::into).collect()
            ),
            // RESP3 types - placeholder until implemented
            _ => unimplemented!("RESP3 encoding not yet implemented"),
        }
    }
}
```

### RESP3 Implementation Status

#### Industry Comparison

| Feature | Redis 6+ | DragonflyDB | FrogDB |
|---------|----------|-------------|--------|
| RESP3 basic types | Full | Full | **Planned** |
| HELLO command | Yes | Yes | **Planned** |
| Client tracking | Yes | Yes | **Deferred** |
| Push notifications | Yes | Yes | **Planned** (pub/sub) |
| Streaming types | Experimental | No | **Deferred** |

#### FrogDB RESP3 Roadmap

| Priority | Feature | Status | Notes |
|----------|---------|--------|-------|
| **Phase 1** | Basic types (Null, Double, Boolean, Map, Set) | Planned | Required for modern client libraries |
| **Phase 1** | HELLO command | Planned | Protocol negotiation |
| **Phase 2** | Push type for pub/sub | Planned | Cleaner than RESP2 inline messages |
| **Phase 3** | Client tracking / invalidation | Deferred | Complex, requires per-key tracking |
| **Deferred** | Streaming strings/aggregates | Not planned | Limited use cases |
| **Deferred** | Attributes | Not planned | Metadata extensions |

#### RESP3 Type Reference

| Type | Wire Format | Use Case |
|------|-------------|----------|
| Null | `_\r\n` | Explicit null (vs RESP2's overloaded `$-1`) |
| Double | `,3.14\r\n` | Floating point (scores, etc.) |
| Boolean | `#t\r\n` / `#f\r\n` | True/false values |
| Map | `%<n>\r\n...` | Key-value pairs (HGETALL, etc.) |
| Set | `~<n>\r\n...` | Unordered unique elements |
| Push | `><n>\r\n...` | Out-of-band pub/sub messages |
| BigNumber | `(<num>\r\n` | Arbitrary precision integers |
| VerbatimString | `=<n>\r\n<fmt>:...` | Formatted text (markdown, etc.) |
| Attribute | `\|<n>\r\n...` | Metadata without breaking clients |

#### Benefits of RESP3

- **Type-rich responses**: Maps, sets, booleans reduce client parsing ambiguity
- **Out-of-band push**: Cleaner pub/sub without inline message interleaving
- **Explicit nulls**: `_\r\n` vs RESP2's overloaded `$-1\r\n`
- **Native doubles**: No string conversion needed for ZSCORE, etc.

#### HELLO Command (Planned)

```
HELLO [protover [AUTH username password] [SETNAME clientname]]
```

**Response (RESP3 map):**
```
%7
$6 server
$6 frogdb
$7 version
$5 0.1.0
$5 proto
:3
$2 id
:42
$4 mode
$10 standalone
$4 role
$6 master
$7 modules
*0
```

**Negotiation:** Clients send `HELLO 3` to upgrade; server responds with connection info.
The `ProtocolVersion` is stored per-connection and determines encoding behavior.

#### Client Tracking (Deferred)

Client-side caching via invalidation messages is a powerful RESP3 feature but requires:
- Per-key tracking of which clients have cached which keys
- Invalidation broadcast on key modification
- Significant memory overhead

This is deferred until core functionality is stable. See [Redis CLIENT TRACKING docs](https://redis.io/docs/latest/commands/client-tracking/) for the full feature specification.

## Tokio Codec

Use the built-in codec for connection handling:

```rust
use redis_protocol::resp2::codec::Resp2;
use tokio_util::codec::Framed;

let framed = Framed::new(socket, Resp2::default());
```

## Error Handling

| Error Type | Handling |
|------------|----------|
| Incomplete frame | Codec buffers, waits for more data |
| Malformed frame | Return `-ERR` response, continue |
| Invalid command | Return `-ERR unknown command`, continue |

Connections are not closed on protocol errors (matches Redis behavior).

## References

- [RESP Protocol Spec](https://redis.io/docs/reference/protocol-spec/)
- [redis-protocol crate docs](https://docs.rs/redis-protocol)
- [DESIGN.md](INDEX.md#protocol)
