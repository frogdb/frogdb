//! WAL frame streaming and ACK handling for the replica side.

use super::connection::ReplicaConnection;
use crate::frame::{ReplicationFrame, ReplicationFrameCodec};
use bytes::BytesMut;
use std::io;
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
                                let solicited = is_getack_frame(&frame.payload);
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

/// True iff a replication frame carries the primary's `REPLCONF GETACK *`
/// ack solicitation (`*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n...`).
///
/// Matched structurally (case-insensitive tokens) rather than by full RESP
/// decode: this runs once per ingested frame on the replica hot path, and the
/// solicitation is the only REPLCONF the primary puts on the stream.
fn is_getack_frame(payload: &[u8]) -> bool {
    let Some(rest) = payload.strip_prefix(b"*3\r\n$8\r\n") else {
        return false;
    };
    if rest.len() < 8 + 6 + 6 {
        return false;
    }
    let (name, rest) = rest.split_at(8);
    if !name.eq_ignore_ascii_case(b"REPLCONF") {
        return false;
    }
    let Some(rest) = rest.strip_prefix(b"\r\n$6\r\n") else {
        return false;
    };
    rest.len() >= 6 && rest[..6].eq_ignore_ascii_case(b"GETACK")
}

#[cfg(test)]
mod tests {
    use super::is_getack_frame;
    use crate::frame::serialize_command_to_resp;
    use bytes::Bytes;

    #[test]
    fn recognizes_the_primary_getack_wire_form() {
        // Exactly what `PrimaryReplicationHandler::request_acks` broadcasts.
        let payload = serialize_command_to_resp(
            "REPLCONF",
            &[Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")],
        );
        assert!(is_getack_frame(&payload));
    }

    #[test]
    fn matches_case_insensitively() {
        assert!(is_getack_frame(
            b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"
        ));
    }

    #[test]
    fn rejects_other_commands_and_other_replconf_subcommands() {
        let set =
            serialize_command_to_resp("SET", &[Bytes::from_static(b"k"), Bytes::from_static(b"v")]);
        assert!(!is_getack_frame(&set));
        // REPLCONF ACK has a different arg shape and must not trigger an ack.
        assert!(!is_getack_frame(
            b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$3\r\n100\r\n"
        ));
        assert!(!is_getack_frame(b""));
        assert!(!is_getack_frame(b"*3\r\n$8\r\nREPLCONF\r\n"));
    }
}
