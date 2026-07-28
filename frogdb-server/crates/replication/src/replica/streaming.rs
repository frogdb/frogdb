//! WAL frame streaming and ACK handling for the replica side.

use super::connection::ReplicaConnection;
use crate::frame::{ReplconfCodec, ReplicationFrame, ReplicationFrameCodec};
use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::codec::Decoder;

impl ReplicaConnection {
    pub(crate) async fn stream_replication(
        &mut self,
        frame_tx: &mpsc::Sender<ReplicationFrame>,
    ) -> io::Result<()> {
        tracing::info!("Starting replication stream");
        let mut codec = ReplicationFrameCodec::new();
        let mut buf = BytesMut::with_capacity(64 * 1024);
        // The spontaneous ACK cadence comes from config (`replication.ack-interval-ms`,
        // Redis `repl-ping-replica-period`), threaded in at connection construction.
        let mut ack_interval = tokio::time::interval(self.ack_interval);
        loop {
            tokio::select! {
                result = self.stream.read_buf(&mut buf) => {
                    match result {
                        Ok(0) => { tracing::info!("Primary connection closed"); return Ok(()); }
                        Ok(_) => {
                            while let Some(frame) = codec.decode(&mut buf)? {
                                // Advance the live applied offset (also the cluster-bus handle).
                                // The advance unit is the RESP payload only (see
                                // `ReplicationFrame::stream_advance`), the same unit the primary
                                // advances by, so the replica's ACK is directly comparable.
                                let offset = self.offsets.frame_advance(&frame);
                                tracing::trace!(sequence = frame.sequence, offset = offset, "Received replication frame");
                                // The primary's GETACK is an ack solicitation (sent by WAIT):
                                // answer immediately instead of waiting for the next 1-second
                                // spontaneous ACK tick, matching Redis replicas. The offset
                                // already includes the GETACK frame itself, as in Redis.
                                let solicited = ReplconfCodec::is_getack(&frame.payload);
                                if frame_tx.send(frame).await.is_err() { tracing::warn!("Frame channel closed"); return Ok(()); }
                                if solicited { self.send_ack(offset).await?; }
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ = ack_interval.tick() => {
                    let offset = self.offsets.current();
                    self.send_ack(offset).await?;
                }
            }
        }
    }

    async fn send_ack(&mut self, offset: u64) -> io::Result<()> {
        self.stream
            .write_all(&ReplconfCodec::encode_ack(offset))
            .await?;
        tracing::trace!(offset = offset, "Sent ACK to primary");
        Ok(())
    }
}
