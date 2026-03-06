//! Replica command executor for applying replicated commands.
//!
//! This module provides the infrastructure for executing commands received via
//! replication on a replica server. Commands are parsed from RESP format,
//! routed to the appropriate shard, and executed with a special connection ID
//! to prevent re-broadcast.

use bytes::BytesMut;
use frogdb_core::{REPLICA_INTERNAL_CONN_ID, ReplicationFrame, ShardMessage, shard_for_key};
use frogdb_protocol::{ParsedCommand, ProtocolVersion};
use redis_protocol::resp2::decode::decode_bytes_mut;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tracing;

/// Error type for replication executor operations.
#[derive(Debug, thiserror::Error)]
pub enum ReplicationError {
    #[error("Failed to parse RESP frame: {0}")]
    ParseError(String),

    #[error("Empty command")]
    EmptyCommand,

    #[error("Shard channel closed")]
    ShardUnavailable,

    #[error("Invalid command format")]
    InvalidCommand,
}

/// Executor for applying replicated commands to shards.
///
/// This component sits between the ReplicaReplicationHandler (which receives
/// frames from the primary) and the shard workers. It:
/// 1. Parses RESP-encoded commands from replication frames
/// 2. Routes commands to the appropriate shard based on key hashing
/// 3. Executes commands with REPLICA_INTERNAL_CONN_ID to prevent re-broadcast
pub struct ReplicaCommandExecutor {
    /// Shard message senders for routing commands.
    shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>,
    /// Number of shards for key routing.
    num_shards: usize,
}

impl ReplicaCommandExecutor {
    /// Create a new replica command executor.
    ///
    /// # Arguments
    /// * `shard_senders` - Channel senders for each shard worker
    /// * `num_shards` - Total number of shards for key routing
    pub fn new(shard_senders: Arc<Vec<mpsc::Sender<ShardMessage>>>, num_shards: usize) -> Self {
        Self {
            shard_senders,
            num_shards,
        }
    }

    /// Execute a replicated command by routing to the appropriate shard.
    ///
    /// The command is executed with `REPLICA_INTERNAL_CONN_ID` to prevent
    /// it from being re-broadcast to other replicas.
    pub async fn execute(&self, cmd: ParsedCommand) -> Result<(), ReplicationError> {
        // Get first key for shard routing (most commands have key as first arg)
        let shard_id = if !cmd.args.is_empty() {
            let key = &cmd.args[0];
            shard_for_key(key, self.num_shards)
        } else {
            // Commands without keys go to shard 0 (e.g., FLUSHDB)
            0
        };

        // Create response channel (we'll discard the response for performance)
        let (response_tx, _response_rx) = oneshot::channel();

        let msg = ShardMessage::Execute {
            command: std::sync::Arc::new(cmd),
            conn_id: REPLICA_INTERNAL_CONN_ID,
            txid: None,
            protocol_version: ProtocolVersion::Resp2,
            response_tx,
        };

        self.shard_senders
            .get(shard_id)
            .ok_or(ReplicationError::ShardUnavailable)?
            .send(msg)
            .await
            .map_err(|_| ReplicationError::ShardUnavailable)?;

        Ok(())
    }
}

/// Parse RESP-encoded command from replication frame payload.
///
/// Replication frames contain RESP2-encoded commands that need to be
/// parsed back into ParsedCommand format for execution.
pub fn parse_frame_payload(payload: &[u8]) -> Result<ParsedCommand, ReplicationError> {
    let mut buf = BytesMut::from(payload);

    // Use redis-protocol's RESP2 decoder that returns BytesFrame
    // Returns (frame, bytes_consumed, remaining_bytes)
    match decode_bytes_mut(&mut buf) {
        Ok(Some((frame, _, _))) => {
            // Convert BytesFrame to ParsedCommand
            ParsedCommand::try_from(frame)
                .map_err(|e| ReplicationError::ParseError(format!("{:?}", e)))
        }
        Ok(None) => Err(ReplicationError::ParseError("Incomplete frame".to_string())),
        Err(e) => Err(ReplicationError::ParseError(format!("{}", e))),
    }
}

/// Consume replication frames and execute commands on shards.
///
/// This is the main loop that processes incoming replication frames:
/// 1. Receives frames from the frame channel
/// 2. Parses the RESP payload
/// 3. Routes to the appropriate shard for execution
///
/// The loop runs until the frame channel is closed (primary disconnect or shutdown).
pub async fn consume_frames(
    mut frame_rx: mpsc::Receiver<ReplicationFrame>,
    executor: ReplicaCommandExecutor,
) {
    tracing::info!("Replica frame consumer started");

    let mut frames_processed: u64 = 0;
    let mut errors: u64 = 0;

    while let Some(frame) = frame_rx.recv().await {
        match parse_frame_payload(&frame.payload) {
            Ok(cmd) => {
                let cmd_name = String::from_utf8_lossy(&cmd.name).to_uppercase();

                // Skip REPLCONF GETACK - this is a control message, not a data command
                if cmd_name == "REPLCONF" {
                    continue;
                }

                tracing::trace!(
                    sequence = frame.sequence,
                    command = %cmd_name,
                    "Applying replicated command"
                );

                if let Err(e) = executor.execute(cmd).await {
                    tracing::warn!(
                        error = %e,
                        sequence = frame.sequence,
                        command = %cmd_name,
                        "Failed to execute replicated command"
                    );
                    errors += 1;
                } else {
                    frames_processed += 1;
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    sequence = frame.sequence,
                    payload_len = frame.payload.len(),
                    "Failed to parse replication frame"
                );
                errors += 1;
            }
        }
    }

    tracing::info!(
        frames_processed = frames_processed,
        errors = errors,
        "Replica frame consumer shutting down"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_command() {
        // RESP encoding of: SET mykey myvalue
        // *3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n
        let payload = b"*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n";

        let cmd = parse_frame_payload(payload).unwrap();
        assert_eq!(cmd.name.as_ref(), b"SET");
        assert_eq!(cmd.args.len(), 2);
        assert_eq!(cmd.args[0].as_ref(), b"mykey");
        assert_eq!(cmd.args[1].as_ref(), b"myvalue");
    }

    #[test]
    fn test_parse_get_command() {
        // RESP encoding of: GET mykey
        let payload = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n";

        let cmd = parse_frame_payload(payload).unwrap();
        assert_eq!(cmd.name.as_ref(), b"GET");
        assert_eq!(cmd.args.len(), 1);
        assert_eq!(cmd.args[0].as_ref(), b"mykey");
    }

    #[test]
    fn test_parse_del_command() {
        // RESP encoding of: DEL key1 key2 key3
        let payload = b"*4\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";

        let cmd = parse_frame_payload(payload).unwrap();
        assert_eq!(cmd.name.as_ref(), b"DEL");
        assert_eq!(cmd.args.len(), 3);
    }

    #[test]
    fn test_parse_invalid_frame() {
        let payload = b"not valid resp";
        assert!(parse_frame_payload(payload).is_err());
    }
}
