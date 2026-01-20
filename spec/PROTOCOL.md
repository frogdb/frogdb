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

/// Errors during RESP parsing
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
            // RESP3 types - implement in Phase 12
            _ => todo!("RESP3 encoding - implement in Phase 12"),
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

#### FrogDB RESP3 Features

| Priority | Feature | Notes |
|----------|---------|-------|
| **Phase 1** | Basic types (Null, Double, Boolean, Map, Set) | Required for modern client libraries |
| **Phase 1** | HELLO command | Protocol negotiation |
| **Phase 2** | Push type for pub/sub | Cleaner than RESP2 inline messages |
| **Phase 3** | Client tracking / invalidation | Complex, requires per-key tracking |

See [ROADMAP.md](ROADMAP.md) for implementation status.

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

#### Protocol Negotiation and Fallback

This section specifies exact behavior for protocol version negotiation and fallback scenarios.

**Default Protocol:**

All new connections start in RESP2 mode. RESP2 is the default for backward compatibility with existing Redis clients.

**HELLO Command Protocol:**

```
┌─────────────────────────────────────────────────────────────────┐
│                    HELLO Negotiation Flow                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Client connects (default: RESP2)                                │
│         │                                                         │
│         ▼                                                         │
│  Client: HELLO 3 [AUTH user pass]                                │
│         │                                                         │
│         ├── Version 3 supported?                                  │
│         │   │                                                     │
│         │   ├── Yes: Switch to RESP3, return connection info     │
│         │   │                                                     │
│         │   └── No: Return error (RESP2), stay in RESP2          │
│         │                                                         │
│         └── Auth provided?                                        │
│             │                                                     │
│             ├── Yes: Validate credentials                        │
│             │   │                                                 │
│             │   ├── Valid: Proceed with upgrade                  │
│             │   │                                                 │
│             │   └── Invalid: Return -WRONGPASS, stay in RESP2    │
│             │                                                     │
│             └── No: Proceed without auth                          │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**HELLO Version Handling:**

| HELLO Version | Behavior |
|---------------|----------|
| `HELLO` (no version) | Return connection info in current protocol |
| `HELLO 2` | Downgrade to RESP2 (if in RESP3), return info in RESP2 |
| `HELLO 3` | Upgrade to RESP3 (Phase 12+), return info in RESP3 |
| `HELLO 4+` | Return `-NOPROTO unsupported protocol version` |

**Fallback Scenarios:**

| Scenario | Behavior | Rationale |
|----------|----------|-----------|
| Client sends RESP2 after `HELLO 3` | Continue in RESP3 | Server doesn't downgrade mid-session |
| HELLO fails (wrong auth) | Stay in current protocol | Don't change protocol on error |
| Client never sends HELLO | Stay in RESP2 | Backward compatibility |
| Server restarts during session | Client reconnects, renegotiates | Stateless design |

**No RESP3 → RESP2 Downgrade Mid-Session:**

Once a connection has upgraded to RESP3, it cannot downgrade to RESP2 within the same session:

```
Client: HELLO 3
Server: (RESP3 map with connection info)
       [Connection is now RESP3]

Client: HELLO 2
Server: -NOPROTO cannot downgrade to RESP2, reconnect to change protocol

Client must: Close connection, reconnect, use RESP2
```

**Rationale:** Allowing protocol downgrade mid-session complicates state management and could cause client confusion if responses suddenly change format.

**Legacy Client Compatibility:**

Clients that never send HELLO operate exactly as they would with Redis:
- Default RESP2 encoding for all responses
- No push messages
- Standard RESP2 null handling (`$-1\r\n`)

**Per-Connection Protocol State:**

```rust
pub struct ConnectionProtocol {
    /// Current protocol version
    pub version: ProtocolVersion,

    /// True if HELLO has been sent
    pub hello_received: bool,

    /// Timestamp of last HELLO (for debugging)
    pub hello_at: Option<Instant>,
}

impl ConnectionProtocol {
    pub fn new() -> Self {
        Self {
            version: ProtocolVersion::Resp2, // Default
            hello_received: false,
            hello_at: None,
        }
    }

    pub fn upgrade_to_resp3(&mut self) {
        self.version = ProtocolVersion::Resp3;
    }
}
```

**HELLO Error Responses:**

| Error | Condition | Response |
|-------|-----------|----------|
| `-NOPROTO` | Unsupported version requested | `-NOPROTO unsupported protocol version` |
| `-WRONGPASS` | Invalid AUTH credentials | `-WRONGPASS invalid username-password pair` |
| `-NOAUTH` | AUTH required but not provided | `-NOAUTH authentication required` |

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

### Framing Error Recovery

This section specifies exact behavior for recovering from protocol framing errors.

**Types of Framing Errors:**

| Error Type | Example | Recoverable? | Action |
|------------|---------|--------------|--------|
| Incomplete frame | Buffer ends mid-frame | Yes | Buffer until complete |
| Invalid type marker | `?` (unknown) | No | Close connection |
| Negative length | `$-2\r\n` | No | Close connection |
| Length mismatch | `$5\r\nabc\r\n` (only 3 bytes) | Yes* | Wait for more data or timeout |
| Invalid integer | `:abc\r\n` | No | Close connection |
| Missing CRLF | `+OK\n` (only LF) | No | Close connection |
| Excessively large length | `$999999999\r\n` | No | Close connection |

*Length mismatch may be incomplete frame or corruption - timeout determines outcome.

**Recovery Strategy:**

```
┌─────────────────────────────────────────────────────────────────┐
│                  Framing Error Recovery Flow                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Read data from socket                                           │
│         │                                                         │
│         ▼                                                         │
│  Parse frame with redis-protocol codec                           │
│         │                                                         │
│    ┌────┴────────────────────────┐                               │
│    │                             │                                │
│    ▼                             ▼                                │
│  Success                    Frame Error                          │
│    │                             │                                │
│    ▼                             ├── Incomplete?                  │
│  Process                         │   │                            │
│  command                         │   ├── Yes: Buffer, wait for    │
│    │                             │   │       more data or timeout │
│    │                             │   │                            │
│    │                             │   └── No: Structural error     │
│    │                             │           │                    │
│    │                             │           ▼                    │
│    │                             │       Recoverable?             │
│    │                             │           │                    │
│    │                             │       ┌───┴───┐                │
│    │                             │       │       │                │
│    │                             │       ▼       ▼                │
│    │                             │   Send -ERR  Close             │
│    │                             │   continue   connection        │
│    │                             │                                │
│    ▼                             ▼                                │
│  Send response              Return error                         │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

**Recoverable Errors (Send -ERR, Continue):**

These errors indicate a malformed command but not protocol corruption:

| Error | Response | Continue? |
|-------|----------|-----------|
| Empty array | `-ERR empty command` | Yes |
| Non-array top-level | `-ERR commands must be arrays` | Yes |
| Non-bulk-string args | `-ERR arguments must be bulk strings` | Yes |
| Command too long | `-ERR command too long` | Yes |

**Unrecoverable Errors (Close Connection):**

These errors indicate protocol stream corruption - recovery is not possible:

| Error | Log Message | Close Reason |
|-------|-------------|--------------|
| Invalid type byte | `Invalid RESP type marker: 0x{:02x}` | Protocol corruption |
| Negative bulk length | `Invalid bulk string length: {}` | Protocol corruption |
| Overflow length | `Bulk string length exceeds maximum` | DoS protection |
| Invalid integer parse | `Failed to parse integer from RESP` | Protocol corruption |
| Missing CRLF terminator | `Expected CRLF, found 0x{:02x}` | Protocol corruption |

**Timeout Handling for Incomplete Frames:**

```rust
/// Timeout for incomplete frames before treating as error
const FRAME_COMPLETION_TIMEOUT_MS: u64 = 30000; // 30 seconds

async fn read_frame(&mut self) -> Result<BytesFrame, FrameError> {
    let timeout = Duration::from_millis(FRAME_COMPLETION_TIMEOUT_MS);

    loop {
        // Try to decode a frame from the buffer
        match self.codec.decode(&mut self.buffer) {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => {
                // Incomplete frame - read more data
                let read_result = tokio::time::timeout(
                    timeout,
                    self.socket.read_buf(&mut self.buffer)
                ).await;

                match read_result {
                    Ok(Ok(0)) => return Err(FrameError::ConnectionClosed),
                    Ok(Ok(_)) => continue, // More data received
                    Ok(Err(e)) => return Err(FrameError::Io(e)),
                    Err(_) => {
                        // Timeout - incomplete frame
                        return Err(FrameError::IncompleteTimeout);
                    }
                }
            }
            Err(e) => return Err(FrameError::Protocol(e)),
        }
    }
}
```

**Configuration:**

```toml
[protocol]
# Timeout for incomplete frames (ms)
frame_timeout_ms = 30000

# Maximum frame size (bytes, for DoS protection)
max_frame_size = 536870912  # 512MB (matches Redis)

# Maximum bulk string length
max_bulk_string_length = 536870912  # 512MB

# Maximum array elements
max_array_elements = 1000000  # 1M elements
```

**Behavior on Timeout:**

| Condition | Action |
|-----------|--------|
| Incomplete frame, timeout | Close connection |
| Buffer has unparsed garbage | Close connection |
| Valid partial frame, timeout | Close connection |

**Distinction from Command Errors:**

| Error Type | Example | Connection Closed? |
|------------|---------|-------------------|
| Protocol framing | Invalid type byte | Yes |
| Command syntax | `SET` (missing args) | No |
| Command semantics | `GET nonexistent` | No |
| Execution error | WRONGTYPE | No |

**Metrics:**

| Metric | Description |
|--------|-------------|
| `frogdb_protocol_framing_errors_total` | Total framing errors |
| `frogdb_protocol_incomplete_timeouts_total` | Frames that timed out incomplete |
| `frogdb_protocol_connections_closed_error_total` | Connections closed due to protocol errors |
| `frogdb_protocol_recovered_errors_total` | Errors that were recoverable |

## References

- [RESP Protocol Spec](https://redis.io/docs/reference/protocol-spec/)
- [redis-protocol crate docs](https://docs.rs/redis-protocol)
- [DESIGN.md](INDEX.md#protocol)
