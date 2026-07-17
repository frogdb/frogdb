//! Frame reading/writing and backpressure for client connections.

use bytes::Bytes;
use frogdb_core::InvalidationMessage;
use frogdb_protocol::{ProtocolVersion, Response, WireResponse};
use futures::SinkExt;
use tokio::io::AsyncWriteExt;

use super::codec::{RESP2_NULL_ARRAY, Resp2Outbound};
use super::{ConnectionHandler, estimate_resp2_frame_size};

/// Narrow a [`WireResponse`] to the RESP2 codec's outbound item, returning the
/// item and its estimated encoded size for stats tracking.
///
/// [`WireResponse::NullArray`] becomes [`Resp2Outbound::NullArray`] so the
/// `*-1\r\n` bytes (which `redis-protocol` cannot produce — its `Null` is always
/// `$-1\r\n`) are emitted by the codec in feed order with surrounding frames.
/// Every other variant is encoded to an ordinary [`BytesFrame`] up front.
fn narrow_to_resp2_outbound(response: WireResponse) -> (Resp2Outbound, usize) {
    match response {
        WireResponse::NullArray => (Resp2Outbound::NullArray, RESP2_NULL_ARRAY.len()),
        other => {
            let frame = other.to_resp2_frame();
            let size = estimate_resp2_frame_size(&frame);
            (Resp2Outbound::Frame(frame), size)
        }
    }
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

    /// Send a wire response to the client.
    ///
    /// This is the type-safe version that only accepts wire-serializable responses.
    /// Use this when you have already extracted a WireResponse from a Response.
    async fn send_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Narrow to the codec's outbound item and send it through the
                // Framed codec. NullArray rides `Resp2Outbound::NullArray` so its
                // `*-1\r\n` bytes are queued in the codec write buffer in feed
                // order; `send` flushes after buffering.
                let (outbound, frame_size) = narrow_to_resp2_outbound(response);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed
                    .send(outbound)
                    .await
                    .map_err(std::io::Error::other)
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
        match self.state.protocol_version {
            ProtocolVersion::Resp2 => {
                // Narrow to the codec's outbound item and buffer it without
                // flushing. NullArray rides `Resp2Outbound::NullArray` so its
                // `*-1\r\n` bytes stay interleaved with surrounding buffered
                // frames in the codec write buffer.
                let (outbound, frame_size) = narrow_to_resp2_outbound(response);
                self.state.local_stats.add_bytes_sent(frame_size as u64);
                self.framed
                    .feed(outbound)
                    .await
                    .map_err(std::io::Error::other)
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
    use super::super::codec::FrogDbResp2;
    use super::*;
    use redis_protocol::resp2::types::BytesFrame as Resp2Frame;
    use tokio::io::AsyncReadExt;
    use tokio_util::codec::Framed;

    /// RESP2 pipelined ordering (proposal 49): a null-array reply fed between
    /// two buffered bulk frames must reach the wire *between* them, not ahead of
    /// them. Drives the codec feed path over an in-memory duplex and asserts the
    /// exact byte stream.
    ///
    /// The array-null now rides the codec's own outbound item
    /// ([`Resp2Outbound::NullArray`], proposal 62-A) rather than a hand-poked
    /// write buffer, so it is buffered by [`Framed::feed`] in feed order with the
    /// surrounding frames. Red before proposal 49 (the raw `*-1\r\n` was written
    /// straight to the socket, jumping ahead of the buffered frames →
    /// `*-1\r\n$1\r\na\r\n$1\r\nb\r\n`); green now.
    #[tokio::test]
    async fn resp2_null_array_feed_order_is_preserved() {
        let (mut client, server) = tokio::io::duplex(1024);
        let mut framed = Framed::new(server, FrogDbResp2::default());

        // Feed [Bulk("a"), NullArray, Bulk("b")] in order, all through the codec.
        framed
            .feed(Resp2Outbound::Frame(Resp2Frame::BulkString(
                Bytes::from_static(b"a"),
            )))
            .await
            .unwrap();
        framed.feed(Resp2Outbound::NullArray).await.unwrap();
        framed
            .feed(Resp2Outbound::Frame(Resp2Frame::BulkString(
                Bytes::from_static(b"b"),
            )))
            .await
            .unwrap();

        // Two-level flush: drain the codec write buffer, then the stream.
        SinkExt::<Resp2Outbound>::flush(&mut framed).await.unwrap();
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
