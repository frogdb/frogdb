//! WAL frame streaming and ACK handling for the replica side.

use super::connection::ReplicaConnection;
use crate::frame::{ReplicationFrame, ReplicationFrameCodec};
use crate::offset_coordinator::OffsetCoordinator;
use bytes::BytesMut;
use std::io;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

const ACK_INTERVAL: Duration = Duration::from_secs(1);

impl ReplicaConnection {
    pub(crate) async fn stream_replication(
        &mut self,
        frame_tx: &mpsc::Sender<ReplicationFrame>,
    ) -> io::Result<()> {
        tracing::info!("Starting replication stream");
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let mut ack_interval = tokio::time::interval(ACK_INTERVAL);
        loop {
            tokio::select! {
                result = self.stream.read_buf(&mut buf) => {
                    match result {
                        Ok(0) => { tracing::info!("Primary connection closed"); return Ok(()); }
                        Ok(_) => {
                            while let Some(frame) = codec.decode(&mut buf)? {
                                let mut state = self.state.write().await;
                                // Advance by the RESP payload only — the 18-byte frame header is
                                // transport, not part of the replication offset. This is the same
                                // unit the primary advances by, so the replica's ACK is directly
                                // comparable to the primary's live offset (see OffsetCoordinator).
                                state.increment_offset(OffsetCoordinator::frame_advance(&frame));
                                let offset = state.replication_offset;
                                drop(state);
                                if let Some(ref shared) = self.shared_offset { shared.store(offset, Ordering::Release); }
                                tracing::trace!(sequence = frame.sequence, offset = offset, "Received replication frame");
                                if frame_tx.send(frame).await.is_err() { tracing::warn!("Frame channel closed"); return Ok(()); }
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ = ack_interval.tick() => {
                    let state = self.state.read().await;
                    let offset = state.replication_offset;
                    drop(state);
                    self.send_ack(offset).await?;
                }
            }
        }
    }

    async fn send_ack(&mut self, offset: u64) -> io::Result<()> {
        let offset_str = offset.to_string();
        let cmd = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
            offset_str.len(),
            offset_str
        );
        self.stream.write_all(cmd.as_bytes()).await?;
        tracing::trace!(offset = offset, "Sent ACK to primary");
        Ok(())
    }
}
