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

Our `Response` type encodes back to `BytesFrame`:

```rust
pub enum Response {
    Simple(Bytes),        // +OK\r\n
    Error(Bytes),         // -ERR message\r\n
    Integer(i64),         // :123\r\n
    Bulk(Option<Bytes>),  // $5\r\nhello\r\n or $-1\r\n
    Array(Vec<Response>), // *2\r\n...
}

impl From<Response> for BytesFrame {
    fn from(response: Response) -> Self {
        match response {
            Response::Simple(s) => BytesFrame::SimpleString(s),
            Response::Error(e) => BytesFrame::SimpleError(e),
            Response::Integer(i) => BytesFrame::Number(i),
            Response::Bulk(Some(b)) => BytesFrame::BulkString(b),
            Response::Bulk(None) => BytesFrame::Null,
            Response::Array(items) => BytesFrame::Array(
                items.into_iter().map(Into::into).collect()
            ),
        }
    }
}
```

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
- [DESIGN.md](../DESIGN.md#protocol)
