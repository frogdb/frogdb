#![allow(clippy::collapsible_if)]
//! Custom RESP2 codec wrapper with Redis-compatible protocol edge case handling.
//!
//! The upstream `redis-protocol` crate's `Resp2` codec strictly rejects certain
//! protocol inputs that Redis itself tolerates. This wrapper intercepts the raw
//! byte buffer before decoding to handle:
//!
//! - **Empty lines** (`\r\n` alone) — silently consumed, like Redis does.
//! - **Negative multibulk counts** (`*-N\r\n` where N > 1) — silently consumed.
//! - **Excessively large multibulk counts** (`*N\r\n` where N > 1048576) — returns error.
//! - **Excessively large bulk lengths** (`$N\r\n` where N > 512MB) — returns error.

use bytes::BytesMut;
use redis_protocol::{
    codec::Resp2,
    error::{RedisProtocolError, RedisProtocolErrorKind},
    resp2::types::{BorrowedFrame, BytesFrame},
};
use tokio_util::codec::{Decoder, Encoder};

/// Maximum number of elements in a multibulk (array) request.
/// Redis uses 1048576 (1024 * 1024).
const PROTO_MAX_MULTIBULK_LEN: i64 = 1_048_576;

/// Maximum length of a single bulk string (512 MB).
/// Redis uses `512ll * 1024 * 1024`.
const PROTO_MAX_BULK_LEN: i64 = 512 * 1024 * 1024;

/// The RESP2 array-null literal (`*-1\r\n`).
///
/// The `redis-protocol` crate cannot produce it — its `Null` frame is always
/// `$-1\r\n` — so the codec owns these raw bytes. This is the single home of the
/// RESP2 array-null encoding, alongside the rest of the RESP2 wire encoding.
pub const RESP2_NULL_ARRAY: &[u8] = b"*-1\r\n";

/// An item the RESP2 codec can encode on the outbound path.
///
/// Most responses narrow to an ordinary [`BytesFrame`]. The one exception is
/// [`WireResponse::NullArray`](frogdb_protocol::WireResponse::NullArray), which
/// `redis-protocol` cannot represent (its `Null` is `$-1\r\n`, never `*-1\r\n`);
/// it rides this enum so the `*-1\r\n` bytes stay in feed order with surrounding
/// frames without the connection layer poking the write buffer directly.
#[derive(Clone, Debug)]
pub enum Resp2Outbound {
    /// An ordinary RESP2 frame, encoded by the upstream codec.
    Frame(BytesFrame),
    /// The RESP2 array-null (`*-1\r\n`), encoded by this codec.
    NullArray,
}

/// Redis-compatible RESP2 codec that handles protocol edge cases.
#[derive(Clone, Debug, Default)]
pub struct FrogDbResp2 {
    inner: Resp2,
}

impl Encoder<BytesFrame> for FrogDbResp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: BytesFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

impl Encoder<Resp2Outbound> for FrogDbResp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: Resp2Outbound, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Resp2Outbound::Frame(frame) => self.inner.encode(frame, dst),
            Resp2Outbound::NullArray => {
                dst.extend_from_slice(RESP2_NULL_ARRAY);
                Ok(())
            }
        }
    }
}

impl Encoder<BorrowedFrame<'_>> for FrogDbResp2 {
    type Error = RedisProtocolError;

    fn encode(&mut self, item: BorrowedFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.inner.encode(item, dst)
    }
}

impl Decoder for FrogDbResp2 {
    type Error = RedisProtocolError;
    type Item = BytesFrame;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Pre-process the buffer to handle edge cases before the upstream decoder sees it.
        loop {
            if src.is_empty() {
                return Ok(None);
            }

            // 1. Strip leading empty lines (\r\n with no prefix byte).
            //    Redis silently ignores these — common with telnet-style clients.
            if src.len() >= 2 && src[0] == b'\r' && src[1] == b'\n' {
                let _ = src.split_to(2);
                continue;
            }

            // 2. Handle multibulk (array) prefix: *N\r\n
            if src[0] == b'*' {
                if let Some(crlf_pos) = find_crlf(src) {
                    let line = &src[1..crlf_pos];
                    if let Ok(s) = std::str::from_utf8(line) {
                        if let Ok(count) = s.parse::<i64>() {
                            // Negative counts other than -1 (null array): silently ignore
                            if count < -1 {
                                let _ = src.split_to(crlf_pos + 2);
                                continue;
                            }
                            // Out-of-range positive count
                            if count > PROTO_MAX_MULTIBULK_LEN {
                                // Consume the bad line so the connection can continue
                                let _ = src.split_to(crlf_pos + 2);
                                return Err(RedisProtocolError::new(
                                    RedisProtocolErrorKind::DecodeError,
                                    format!("Protocol error: invalid multibulk length ({count})"),
                                ));
                            }
                        }
                    }
                }
                // Fall through to normal decode
            }

            // 3. Handle bulk string prefix inside a multibulk: $N\r\n
            //    We only validate top-level $ prefixes here; the inner codec handles
            //    $ inside arrays. But if a top-level $ appears with a huge length,
            //    we reject it early. For in-array bulk strings, we scan the entire
            //    buffer for any $N\r\n where N is too large.
            if src[0] == b'$' {
                if let Some(crlf_pos) = find_crlf(src) {
                    let line = &src[1..crlf_pos];
                    if let Ok(s) = std::str::from_utf8(line) {
                        if let Ok(len) = s.parse::<i64>() {
                            if len > PROTO_MAX_BULK_LEN {
                                let _ = src.split_to(crlf_pos + 2);
                                return Err(RedisProtocolError::new(
                                    RedisProtocolErrorKind::DecodeError,
                                    format!("Protocol error: invalid bulk length ({len})"),
                                ));
                            }
                        }
                    }
                }
            }

            // 4. Scan for out-of-range bulk lengths within a multibulk frame.
            //    The multibulk header `*N\r\n` may be valid, but one of its bulk
            //    string elements `$M\r\n` might have M > PROTO_MAX_BULK_LEN.
            //    We pre-scan the buffer to catch this before the upstream decoder
            //    tries to wait for M bytes of data (which would hang).
            if src[0] == b'*' {
                if let Some(bad_bulk) = scan_for_oversized_bulk(src) {
                    // Discard everything up to and including the bad $N\r\n line
                    let _ = src.split_to(bad_bulk);
                    return Err(RedisProtocolError::new(
                        RedisProtocolErrorKind::DecodeError,
                        "Protocol error: invalid bulk length",
                    ));
                }
            }

            break;
        }

        self.inner.decode(src)
    }
}

/// Find the position of the first `\r\n` in the buffer.
fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

/// Scan the buffer starting from a `*N\r\n` multibulk header for any `$M\r\n`
/// bulk string prefix where M exceeds the protocol maximum. Returns the byte
/// offset just past the offending `$M\r\n` if found.
fn scan_for_oversized_bulk(buf: &[u8]) -> Option<usize> {
    let mut pos = 0;
    while pos < buf.len() {
        if buf[pos] == b'$' {
            if let Some(crlf_rel) = find_crlf(&buf[pos..]) {
                let line = &buf[pos + 1..pos + crlf_rel];
                if let Ok(s) = std::str::from_utf8(line) {
                    if let Ok(len) = s.parse::<i64>() {
                        if len > PROTO_MAX_BULK_LEN {
                            return Some(pos + crlf_rel + 2);
                        }
                    }
                }
                pos += crlf_rel + 2;
                continue;
            }
        }
        pos += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The codec owns the RESP2 array-null encoding: feeding
    /// [`Resp2Outbound::NullArray`] emits exactly `*-1\r\n` — the shape the
    /// upstream `redis-protocol` `Null` frame cannot produce.
    #[test]
    fn encode_null_array_emits_star_minus_one() {
        let mut codec = FrogDbResp2::default();
        let mut dst = BytesMut::new();
        codec
            .encode(Resp2Outbound::NullArray, &mut dst)
            .expect("encoding a null array cannot fail");
        assert_eq!(&dst[..], b"*-1\r\n");
    }

    /// The `Frame` arm delegates to the upstream RESP2 encoder unchanged.
    #[test]
    fn encode_frame_delegates_to_inner() {
        let mut codec = FrogDbResp2::default();
        let mut dst = BytesMut::new();
        codec
            .encode(
                Resp2Outbound::Frame(BytesFrame::BulkString(bytes::Bytes::from_static(b"a"))),
                &mut dst,
            )
            .expect("encoding a bulk string cannot fail");
        assert_eq!(&dst[..], b"$1\r\na\r\n");
    }
}
