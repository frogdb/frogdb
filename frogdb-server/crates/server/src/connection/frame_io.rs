//! Frame reading/writing and backpressure for client connections.

use bytes::Bytes;
use frogdb_core::InvalidationMessage;
use frogdb_protocol::{ProtocolVersion, Response};
use futures::SinkExt;
use tokio::io::AsyncWriteExt;

use super::{ConnectionHandler, estimate_resp2_frame_size};

impl ConnectionHandler {
    /// Send a response to the client, using appropriate encoding based on protocol version.
    ///
    /// For RESP2 connections, uses the standard Framed codec.
    /// For RESP3 connections, manually encodes and writes to the socket.
    ///
    /// # Panics
    ///
    /// Panics if the response is an internal action type (BlockingNeeded, RaftNeeded,
    /// MigrateNeeded). These should be intercepted by `route_and_execute_with_transaction`
    /// before reaching this method.
    pub(super) async fn send_response(&mut self, response: Response) -> std::io::Result<()> {
        // Convert to WireResponse, panicking if it's an internal action
        // (which would indicate a bug in the command routing logic)
        let wire_response = response.into_wire().expect(
            "Internal action reached send_response - should be intercepted by route_and_execute_with_transaction"
        );
        self.send_wire_response(wire_response).await
    }

    /// Send a wire response to the client.
    ///
    /// This is the type-safe version that only accepts wire-serializable responses.
    /// Use this when you have already extracted a WireResponse from a Response.
    async fn send_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        // NullArray requires raw bytes (*-1\r\n) in RESP2 which the redis-protocol
        // crate cannot produce (its Null is always $-1\r\n). In RESP3, null is just _\r\n.
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            match self.state.protocol_version {
                ProtocolVersion::Resp2 => {
                    const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
                    self.state
                        .local_stats
                        .add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
                    self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await?;
                    return self.framed.get_mut().flush().await;
                }
                ProtocolVersion::Resp3 => {
                    let frame = response.to_resp3_frame();
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
                    self.framed.get_mut().write_all(&self.resp3_buf).await?;
                    return self.framed.get_mut().flush().await;
                }
            }
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

    /// Buffer a response without flushing (for write coalescing).
    pub(super) async fn feed_response(&mut self, response: Response) -> std::io::Result<()> {
        let wire_response = response.into_wire().expect(
            "Internal action reached feed_response - should be intercepted by route_and_execute_with_transaction"
        );
        self.feed_wire_response(wire_response).await
    }

    /// Buffer a wire response without flushing.
    async fn feed_wire_response(
        &mut self,
        response: frogdb_protocol::WireResponse,
    ) -> std::io::Result<()> {
        if matches!(response, frogdb_protocol::WireResponse::NullArray) {
            match self.state.protocol_version {
                ProtocolVersion::Resp2 => {
                    const NULL_ARRAY_BYTES: &[u8] = b"*-1\r\n";
                    self.state
                        .local_stats
                        .add_bytes_sent(NULL_ARRAY_BYTES.len() as u64);
                    return self.framed.get_mut().write_all(NULL_ARRAY_BYTES).await;
                }
                ProtocolVersion::Resp3 => {
                    let frame = response.to_resp3_frame();
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
                    self.framed.get_mut().write_all(&self.resp3_buf).await?;
                    self.resp3_buf.clear();
                    return Ok(());
                }
            }
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
            <redis_protocol::codec::Resp2 as tokio_util::codec::Decoder>::Error,
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
    pub(super) fn invalidation_to_response(msg: &InvalidationMessage) -> Response {
        match msg {
            InvalidationMessage::Keys(keys) => Response::Push(vec![
                Response::bulk(Bytes::from_static(b"invalidate")),
                Response::Array(keys.iter().map(|k| Response::bulk(k.clone())).collect()),
            ]),
            InvalidationMessage::FlushAll => Response::Push(vec![
                Response::bulk(Bytes::from_static(b"invalidate")),
                Response::Null,
            ]),
        }
    }
}
