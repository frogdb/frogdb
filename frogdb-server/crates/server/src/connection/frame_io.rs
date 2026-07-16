//! Frame reading/writing and backpressure for client connections.

use bytes::Bytes;
use frogdb_core::InvalidationMessage;
use frogdb_protocol::{ProtocolVersion, Response, WireResponse};
use futures::SinkExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Framed;

use super::codec::FrogDbResp2;
use super::{ConnectionHandler, estimate_resp2_frame_size};

/// The RESP2 array-null literal.
///
/// The `redis-protocol` crate cannot produce it — its `Null` is always
/// `$-1\r\n` — so we manage these raw bytes ourselves. This literal lives here
/// and nowhere else (proposal 26's seam).
const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";

/// Queue the RESP2 array-null literal into the codec's write buffer, in feed
/// order with the surrounding frames.
///
/// This MUST NOT write straight to the socket. Normal RESP2 frames are buffered
/// in the codec's write buffer by [`Framed::feed`] and only reach the wire at
/// flush time; a direct socket write here would jump *ahead* of those buffered
/// replies and desynchronize a pipelined client (proposal 49). Writing into the
/// same codec write buffer keeps the `*-1\r\n` interleaved in the correct order.
fn queue_resp2_null_array<S>(framed: &mut Framed<S, FrogDbResp2>)
where
    S: AsyncWrite + Unpin,
{
    framed
        .write_buffer_mut()
        .extend_from_slice(NULL_ARRAY_BYTES);
}

impl ConnectionHandler {
    /// Narrow a [`Response`] to its wire form at the encoder boundary.
    ///
    /// Internal control-flow actions are resolved upstream (see
    /// `handle_internal_action`) before a response is handed to the encoder, so
    /// this narrowing is total in practice. Should a stray action ever reach
    /// here it indicates a routing bug: we log and degrade to an error response
    /// rather than panicking — and, crucially, the encoder itself
    /// ([`Self::send_response`] / [`Self::feed_response`]) only ever accepts a
    /// [`WireResponse`], so an internal action is structurally unrepresentable
    /// past this point.
    pub(super) fn narrow_to_wire(response: Response) -> WireResponse {
        match response.into_wire() {
            Ok(wire) => wire,
            Err(action) => {
                tracing::error!(
                    ?action,
                    "internal action reached the encoder boundary; this is a \
                     routing bug — degrading to an error response"
                );
                WireResponse::error("ERR internal action reached response encoder")
            }
        }
    }

    /// Send a wire response to the client, using appropriate encoding based on
    /// protocol version.
    ///
    /// For RESP2 connections, uses the standard Framed codec.
    /// For RESP3 connections, manually encodes and writes to the socket.
    ///
    /// This accepts a [`WireResponse`], which has no internal-action variants,
    /// so it can never encode a control-flow signal and cannot panic on one.
    pub(super) async fn send_response(&mut self, response: WireResponse) -> std::io::Result<()> {
        self.send_wire_response(response).await
    }

    /// Queue the protocol-correct array-null bytes in feed order, without
    /// flushing. RESP2 queues the raw `*-1\r\n` (which the `redis-protocol`
    /// crate cannot produce — its `Null` is always `$-1\r\n`) into the codec's
    /// write buffer via [`queue_resp2_null_array`], so it stays interleaved with
    /// surrounding buffered frames; RESP3 writes `_\r\n`. The protocol branch
    /// lives here and nowhere else; each caller keeps its own flush/return policy.
    async fn write_null_array(&mut self) -> std::io::Result<()> {
        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                self.state
                    .local_stats
                    .add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
                queue_resp2_null_array(&mut self.framed);
                Ok(())
            }
            ProtocolVersion::Resp3 => {
                let frame = frogdb_protocol::WireResponse::NullArray.to_resp3_frame();
                self.resp3_buf.clear();
                redis_protocol::resp3::encode::complete::extend_encode(
                    &mut self.resp3_buf,
                    &frame,
                    false,
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                self.state
                    .local_stats
                    .add_bytes_sent(self.resp3_buf.len() as u64);
                self.framed.get_mut().write_all(&self.resp3_buf).await
            }
        }
    }

    /// Send a wire response to the client.
    ///
    /// This is the type-safe version that only accepts wire-serializable responses.
    /// Use this when you have already extracted a WireResponse from a Response.
    async fn send_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        // NullArray needs the protocol-specific array-null shape; the one owner
        // of that rule is write_null_array. `send` flushes after writing. On
        // RESP2 the null-array bytes are now queued in the codec write buffer,
        // so the flush must drain that buffer as well as the stream — use the
        // two-level flush from flush_responses.
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            self.write_null_array().await?;
            return self.flush_responses().await;
        }

        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Use RESP2 encoding via the Framed codec
                let frame = response.to_resp2_frame();
                // Estimate frame size for stats tracking
                let frame_size = estimate_resp2_frame_size(&frame);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed.send(frame).await.map_err(std::io::Error::other)
            }
            ProtocolVersion::Resp3 => {
                // Manually encode RESP3 and write to socket
                let frame = response.to_resp3_frame();
                self.resp3_buf.clear();
                redis_protocol::resp3::encode::complete::extend_encode(
                    &mut self.resp3_buf,
                    &frame,
                    false,
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                // Track actual encoded size
                self.state
                    .local_stats
                    .add_bytes_sent(self.resp3_buf.len() as u64);
                self.framed.get_mut().write_all(&self.resp3_buf).await?;
                self.framed.get_mut().flush().await
            }
        }
    }

    /// Buffer a wire response without flushing (for write coalescing).
    ///
    /// Accepts a [`WireResponse`], so an internal control-flow action can never
    /// reach this buffering path.
    pub(super) async fn feed_response(&mut self, response: WireResponse) -> std::io::Result<()> {
        self.feed_wire_response(response).await
    }

    /// Buffer a wire response without flushing.
    async fn feed_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        // NullArray needs the protocol-specific array-null shape; the one owner
        // of that rule is write_null_array. `feed` buffers without flushing and
        // resets the RESP3 accumulator for subsequent feeds.
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            self.write_null_array().await?;
            self.resp3_buf.clear();
            return Ok(());
        }

        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                let frame = response.to_resp2_frame();
                let frame_size = estimate_resp2_frame_size(&frame);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed.feed(frame).await.map_err(std::io::Error::other)
            }
            ProtocolVersion::Resp3 => {
                let frame = response.to_resp3_frame();
                // Don't clear resp3_buf here — accumulate across multiple feeds
                redis_protocol::resp3::encode::complete::extend_encode(
                    &mut self.resp3_buf,
                    &frame,
                    false,
                )
                .map_err(|e| std::io::Error::other(e.to_string()))?;
                let encoded_len = self.resp3_buf.len() as u64;
                self.state.local_stats.add_bytes_sent(encoded_len);
                self.framed.get_mut().write_all(&self.resp3_buf).await?;
                self.resp3_buf.clear();
                Ok(())
            }
        }
    }

    /// Flush all buffered responses to the client.
    pub(super) async fn flush_responses(&mut self) -> std::io::Result<()> {
        // Flush the RESP2 codec buffer and then the underlying stream.
        // Disambiguate: Resp2 now implements Encoder for both BytesFrame and BorrowedFrame.
        SinkExt::<redis_protocol::resp2::types::BytesFrame>::flush(&mut self.framed)
            .await
            .map_err(std::io::Error::other)?;
        self.framed.get_mut().flush().await
    }

    /// Try to decode the next frame from the codec's internal read buffer
    /// without issuing a read syscall. Returns `None` if no complete frame
    /// is buffered.
    pub(super) fn try_next_frame(
        &mut self,
    ) -> Option<
        Result<
            redis_protocol::resp2::types::BytesFrame,
            <super::codec::FrogDbResp2 as tokio_util::codec::Decoder>::Error,
        >,
    > {
        use futures::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Pin::new(&mut self.framed).poll_next(&mut cx) {
            Poll::Ready(item) => item,
            Poll::Pending => None,
        }
    }

    /// Convert an invalidation message to a RESP3 Push response.
    pub(super) fn invalidation_to_response(msg: &InvalidationMessage) -> WireResponse {
        match msg {
            InvalidationMessage::Keys(keys) => WireResponse::Push(vec![
                WireResponse::bulk(Bytes::from_static(b"invalidate")),
                WireResponse::Array(keys.iter().map(|k| WireResponse::bulk(k.clone())).collect()),
            ]),
            InvalidationMessage::FlushAll => WireResponse::Push(vec![
                WireResponse::bulk(Bytes::from_static(b"invalidate")),
                WireResponse::Null,
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
    use tokio::io::AsyncReadExt;

    /// RESP2 pipelined ordering (proposal 49): a null-array reply queued between
    /// two buffered bulk frames must reach the wire *between* them, not ahead of
    /// them. Drives the codec feed path over an in-memory duplex and asserts the
    /// exact byte stream.
    ///
    /// Red before the fix (`write_null_array` wrote the raw `*-1\r\n` straight to
    /// the socket via `get_mut().write_all`, jumping ahead of the two frames
    /// buffered in the codec write buffer → `*-1\r\n$1\r\na\r\n$1\r\nb\r\n`);
    /// green after (the literal is queued into the same write buffer in feed
    /// order via [`queue_resp2_null_array`]).
    #[tokio::test]
    async fn resp2_null_array_feed_order_is_preserved() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut framed = Framed::new(server, FrogDbResp2::default());

        // Feed [Bulk("a"), NullArray, Bulk("b")] in order.
        framed
            .feed(Resp2Frame::BulkString(Bytes::from_static(b"a")))
            .await
            .unwrap();
        queue_resp2_null_array(&mut framed);
        framed
            .feed(Resp2Frame::BulkString(Bytes::from_static(b"b")))
            .await
            .unwrap();

        // Two-level flush: drain the codec write buffer, then the stream.
        SinkExt::<Resp2Frame>::flush(&mut framed).await.unwrap();
        framed.get_mut().flush().await.unwrap();

        let expected: &[u8] = b"$1\r\na\r\n*-1\r\n$1\r\nb\r\n";
        let mut got = vec![0u8; expected.len()];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(
            got, expected,
            "RESP2 null-array must stay in feed order between the bulk frames",
        );
    }

    /// RESP3 sibling of the above (proposal 49, test 2): every RESP3 frame is
    /// written to the socket in feed order, so the `_\r\n` array-null lands
    /// between the two bulk strings. Pins the already-correct behavior — passes
    /// before and after the fix.
    #[tokio::test]
    async fn resp3_null_array_feed_order_is_preserved() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        // Mirror feed_wire_response's RESP3 arm: encode each frame and write it
        // to the socket in feed order.
        let mut buf = bytes::BytesMut::new();
        for frame in [
            WireResponse::bulk(Bytes::from_static(b"a")).to_resp3_frame(),
            WireResponse::NullArray.to_resp3_frame(),
            WireResponse::bulk(Bytes::from_static(b"b")).to_resp3_frame(),
        ] {
            buf.clear();
            redis_protocol::resp3::encode::complete::extend_encode(&mut buf, &frame, false)
                .unwrap();
            server.write_all(&buf).await.unwrap();
        }
        server.flush().await.unwrap();

        let expected: &[u8] = b"$1\r\na\r\n_\r\n$1\r\nb\r\n";
        let mut got = vec![0u8; expected.len()];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(
            got, expected,
            "RESP3 null-array (_\\r\\n) must stay in feed order between the bulk frames",
        );
    }
}
