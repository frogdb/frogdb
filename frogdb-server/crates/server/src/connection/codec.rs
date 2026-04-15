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
