//! WAL frame streaming and ACK handling for the replica side.
//!
//! What the replica ACKs is the **applied** offset, not the received head. WAIT
//! on the primary counts these ACKs, so an ACK is a durability claim: it must
//! describe data this node would still hold if it were promoted a moment later.
//! The received head does not — a promotion settles the node at its applied
//! offset and discards the frames still queued behind it, so ACKing the
//! received head would let `WAIT 1` report success for a write the promoted
//! node then threw away. Redis has the same rule for the same reason: its
//! replicas ack after executing the command stream, not on receipt.

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
                                let received = self.offsets.frame_advance(&frame);
                                tracing::trace!(sequence = frame.sequence, offset = received, "Received replication frame");
                                // The primary's GETACK is an ack solicitation (sent by WAIT):
                                // answer immediately instead of waiting for the next 1-second
                                // spontaneous ACK tick, matching Redis replicas. The solicited
                                // offset covers the GETACK frame itself, as in Redis.
                                let solicited = ReplconfCodec::is_getack(&frame.payload);
                                if frame_tx.send(frame).await.is_err() { tracing::warn!("Frame channel closed"); return Ok(()); }
                                if solicited {
                                    // Everything up to `received` is already decoded and
                                    // queued, so it only has to drain the frame channel;
                                    // Redis replicas likewise ack after execution. Bounded
                                    // by the spontaneous cadence: a stalled applier gets a
                                    // truthful low ACK now rather than a silent gap.
                                    let applied = self.offsets.applied()
                                        .wait_until_applied(received, self.ack_interval)
                                        .await;
                                    self.send_ack(applied).await?;
                                }
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                _ = ack_interval.tick() => {
                    let offset = self.offsets.applied().current();
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
