# FrogDB Protocol Integration

RESP2/RESP3 frame processing, zero-copy design, and wire protocol details for contributors.

## Crate Configuration

```toml
[dependencies]
redis-protocol = { version = "5", features = ["bytes", "codec"] }
```

**Features:**
- `bytes` - Zero-copy parsing using `Bytes` type
- `codec` - Tokio codec for async streaming

## Frame Processing

### Inbound: Frame -> ParsedCommand

The `redis-protocol` crate decodes wire data into `BytesFrame`. We convert to our internal `ParsedCommand`:

```rust
use redis_protocol::resp2::types::BytesFrame;

pub struct ParsedCommand {
    pub name: Bytes,
    pub args: Vec<Bytes>,
}

pub enum ProtocolError {
    EmptyCommand,
    ExpectedArray,
    InvalidFrame,
    Incomplete,
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

### Outbound: Response -> Frame

The `Response` enum includes both RESP2 types and RESP3 types:

```rust
pub enum Response {
    // === RESP2 Types ===
    Simple(Bytes),              // +OK\r\n
    Error(Bytes),               // -ERR message\r\n
    Integer(i64),               // :1000\r\n
    Bulk(Option<Bytes>),        // $5\r\nhello\r\n or $-1\r\n (null)
    Array(Vec<Response>),       // *2\r\n...

    // === RESP3 Types ===
    Null,                       // _\r\n
    Double(f64),                // ,3.14159\r\n
    Boolean(bool),              // #t\r\n or #f\r\n
    BlobError(Bytes),           // !<len>\r\n<bytes>\r\n
    VerbatimString {            // =<len>\r\n<fmt>:<data>\r\n
        format: [u8; 3],
        data: Bytes,
    },
    Map(Vec<(Response, Response)>),  // %<count>\r\n<key><value>...
    Set(Vec<Response>),              // ~<count>\r\n<elements>...
    Attribute(Box<Response>),        // |<count>\r\n<attr-map><data>
    Push(Vec<Response>),             // ><count>\r\n<elements>...
    BigNumber(Bytes),                // (<big-integer>\r\n
}

pub enum ProtocolVersion {
    Resp2,  // Default
    Resp3,  // Negotiated via HELLO command
}
```

---

## RESP3 Type Reference

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
| Attribute | `|<n>\r\n...` | Metadata without breaking clients |

### Benefits of RESP3

- **Type-rich responses**: Maps, sets, booleans reduce client parsing ambiguity
- **Out-of-band push**: Cleaner pub/sub without inline message interleaving
- **Explicit nulls**: `_\r\n` vs RESP2's overloaded `$-1\r\n`
- **Native doubles**: No string conversion needed for ZSCORE, etc.

---

## Protocol Negotiation

All new connections start in RESP2 mode. Clients send `HELLO 3` to upgrade to RESP3.

| HELLO Version | Behavior |
|---------------|----------|
| `HELLO` (no version) | Return connection info in current protocol |
| `HELLO 2` | Downgrade to RESP2 (if in RESP3), return info in RESP2 |
| `HELLO 3` | Upgrade to RESP3, return info in RESP3 |
| `HELLO 4+` | Return `-NOPROTO unsupported protocol version` |

Once a connection has upgraded to RESP3, it cannot downgrade within the same session. Clients that never send HELLO operate exactly as they would with Redis (default RESP2).

### Client Tracking

Server-assisted client-side caching via RESP3 Push invalidation messages. When a client reads a key with tracking enabled, the server records the association. When that key is later modified, the server sends a `>invalidate [keys]` Push frame so the client can evict its local cache.

**Supported modes:** Default, OPTIN, OPTOUT, NOLOOP, BCAST, PREFIX, REDIRECT.

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

### Framing Error Recovery

**Recoverable Errors (Send -ERR, Continue):**

| Error | Response | Continue? |
|-------|----------|-----------|
| Empty array | `-ERR empty command` | Yes |
| Non-array top-level | `-ERR commands must be arrays` | Yes |
| Non-bulk-string args | `-ERR arguments must be bulk strings` | Yes |
| Command too long | `-ERR command too long` | Yes |

**Unrecoverable Errors (Close Connection):**

| Error | Close Reason |
|-------|--------------|
| Invalid type byte | Protocol corruption |
| Negative bulk length | Protocol corruption |
| Overflow length | DoS protection |
| Invalid integer parse | Protocol corruption |
| Missing CRLF terminator | Protocol corruption |

### Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `frame_timeout_ms` | 30000 | Timeout for incomplete frames |
| `max_frame_size` | 536870912 | Maximum frame size (512MB, matches Redis) |
| `max_bulk_string_length` | 536870912 | Maximum bulk string length |
| `max_array_elements` | 1000000 | Maximum array elements |
