//! Checkpoint streaming for full synchronization (FULLRESYNC).
//!
//! Handles creating and streaming RocksDB checkpoints to replicas,
//! including fallback to minimal RDB for empty databases.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use sha2::Digest;
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::BoxedStream;
use crate::fullsync::{
    FullSyncMetadata, FullSyncState, calculate_file_checksum, stream_file_to_writer,
};
use crate::primary::PrimaryReplicationHandler;
use crate::tracker::ReplicaState;

// ============================================================================
// RDB Format Constants
// ============================================================================

/// RDB auxiliary field opcode (key-value metadata).
const RDB_OPCODE_AUX: u8 = 0xFA;

/// RDB database selector opcode.
const RDB_OPCODE_SELECTDB: u8 = 0xFE;

/// RDB resize database opcode (hash table size hints).
const RDB_OPCODE_RESIZEDB: u8 = 0xFB;

/// RDB end-of-file opcode.
const RDB_OPCODE_EOF: u8 = 0xFF;

impl PrimaryReplicationHandler {
    /// Handle full synchronization (FULLRESYNC).
    pub(crate) async fn handle_full_sync(
        &self,
        mut stream: BoxedStream,
        addr: SocketAddr,
    ) -> io::Result<()> {
        let state = self.state.read().await;

        // Send FULLRESYNC response
        let response = format!(
            "+FULLRESYNC {} {}\r\n",
            state.replication_id, state.replication_offset
        );
        stream.write_all(response.as_bytes()).await?;

        let current_offset = state.replication_offset;
        let replication_id = state.replication_id.clone();
        drop(state);

        // Register replica in syncing state
        let replica_id = self.tracker.register_replica(addr);
        self.tracker.set_state(replica_id, ReplicaState::Syncing);

        // Stream database snapshot to replica
        if let Some(ref rocks) = self.rocks_store {
            // Create checkpoint for FULLRESYNC
            let checkpoint_path = self.data_dir.join(format!("fullsync_{}", replica_id));

            // Create checkpoint in blocking task (Checkpoint is !Send)
            let rocks_clone = rocks.clone();
            let path_clone = checkpoint_path.clone();
            let checkpoint_result =
                tokio::task::spawn_blocking(move || rocks_clone.create_checkpoint(&path_clone))
                    .await
                    .map_err(io::Error::other)?;

            if let Err(e) = checkpoint_result {
                tracing::error!(error = %e, "Failed to create checkpoint for FULLRESYNC");
                // Fall back to minimal RDB
                self.send_minimal_rdb(&mut stream).await?;
            } else {
                // Stream checkpoint files to replica
                let stream_result = self
                    .stream_checkpoint(
                        &mut stream,
                        &checkpoint_path,
                        replica_id,
                        &replication_id,
                        current_offset,
                    )
                    .await;

                // Clean up checkpoint directory regardless of success/failure
                if let Err(e) = fs::remove_dir_all(&checkpoint_path).await {
                    tracing::warn!(
                        checkpoint_path = %checkpoint_path.display(),
                        error = %e,
                        "Failed to clean up checkpoint directory"
                    );
                }

                // Propagate streaming errors
                if let Err(e) = stream_result {
                    tracing::error!(error = %e, "Failed to stream checkpoint for FULLRESYNC");
                    return Err(e);
                }
            }
        } else {
            // No persistence - send minimal RDB
            self.send_minimal_rdb(&mut stream).await?;
        }

        tracing::info!(
            replica_id = replica_id,
            addr = %addr,
            offset = current_offset,
            "Completed FULLRESYNC"
        );

        // Update state to streaming
        self.tracker.set_state(replica_id, ReplicaState::Streaming);

        // Start streaming WAL updates
        self.start_streaming(stream, addr, replica_id).await
    }

    /// Stream checkpoint files to replica.
    ///
    /// Protocol:
    /// 1. Send file count as $<count>\r\n
    /// 2. For each file:
    ///    a. Send filename as bulk string
    ///    b. Send file size as bulk string
    ///    c. Stream file contents
    /// 3. Send metadata (replication_id:offset:checksum)
    pub(crate) async fn stream_checkpoint(
        &self,
        stream: &mut BoxedStream,
        checkpoint_path: &Path,
        replica_id: u64,
        replication_id: &str,
        replication_offset: u64,
    ) -> io::Result<()> {
        // Collect all files in checkpoint directory
        let mut files: Vec<(String, u64, PathBuf)> = Vec::new();
        let mut total_size = 0u64;

        let mut dir = fs::read_dir(checkpoint_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.is_file() {
                let metadata = fs::metadata(&path).await?;
                let file_name = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let file_size = metadata.len();
                total_size += file_size;
                files.push((file_name, file_size, path));
            }
        }

        // Sort files for deterministic ordering
        files.sort_by(|a, b| a.0.cmp(&b.0));

        tracing::info!(
            replica_id = replica_id,
            file_count = files.len(),
            total_size = total_size,
            "Streaming checkpoint to replica"
        );

        // Create sync state for progress tracking
        let sync_state = FullSyncState::new(replica_id, replica_id, total_size);

        // Send FrogDB checkpoint header (distinguishes from minimal RDB)
        // Format: $FROGDB_CHECKPOINT\r\n<file_count>\r\n
        stream.write_all(b"$FROGDB_CHECKPOINT\r\n").await?;
        stream
            .write_all(format!("{}\r\n", files.len()).as_bytes())
            .await?;

        // Stream each file
        for (file_name, file_size, file_path) in &files {
            // Send filename
            stream
                .write_all(format!("${}\r\n{}\r\n", file_name.len(), file_name).as_bytes())
                .await?;

            // Send file size
            stream
                .write_all(format!("${}\r\n", file_size).as_bytes())
                .await?;

            // Stream file contents
            let bytes_written = stream_file_to_writer(file_path, stream, Some(&sync_state)).await?;

            tracing::debug!(
                file = %file_name,
                size = bytes_written,
                progress = format!("{:.1}%", sync_state.progress_percent()),
                "Streamed checkpoint file"
            );
        }

        // Calculate overall checksum from checkpoint files
        // We use a simple approach: hash the concatenation of all file hashes
        let mut combined_hash = sha2::Sha256::new();
        for (file_name, _, file_path) in &files {
            let file_hash = calculate_file_checksum(file_path).await?;
            combined_hash.update(file_name.as_bytes());
            combined_hash.update(file_hash);
        }
        let final_hash = Digest::finalize(combined_hash);
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&final_hash);

        // Send metadata
        let metadata = FullSyncMetadata {
            rdb_size: total_size,
            checksum,
            replication_id: replication_id.to_string(),
            replication_offset,
        };
        let metadata_bytes = metadata.to_bytes();
        stream
            .write_all(format!("${}\r\n", metadata_bytes.len()).as_bytes())
            .await?;
        stream.write_all(&metadata_bytes).await?;
        stream.write_all(b"\r\n").await?;

        let elapsed = sync_state.elapsed();
        let rate_mbps = (total_size as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
        tracing::info!(
            replica_id = replica_id,
            files = files.len(),
            total_bytes = total_size,
            elapsed_ms = elapsed.as_millis() as u64,
            rate_mbps = format!("{:.2}", rate_mbps),
            "Checkpoint streaming complete"
        );

        Ok(())
    }

    /// Send a minimal RDB for empty database or fallback.
    pub(crate) async fn send_minimal_rdb(&self, stream: &mut BoxedStream) -> io::Result<()> {
        let empty_rdb = create_minimal_rdb();
        let rdb_header = format!("${}\r\n", empty_rdb.len());
        stream.write_all(rdb_header.as_bytes()).await?;
        stream.write_all(&empty_rdb).await?;
        Ok(())
    }
}

/// Create a minimal valid RDB file.
///
/// This is used for empty databases during FULLRESYNC.
pub(crate) fn create_minimal_rdb() -> Vec<u8> {
    let mut rdb = Vec::new();

    // Magic string
    rdb.extend_from_slice(b"REDIS");

    // RDB version (e.g., 0011 for version 11)
    rdb.extend_from_slice(b"0011");

    // Auxiliary fields (optional, but good for compatibility)
    rdb.push(RDB_OPCODE_AUX);
    // redis-ver
    rdb.extend_from_slice(b"\x09redis-ver");
    rdb.extend_from_slice(b"\x057.2.0");

    // Database selector
    rdb.push(RDB_OPCODE_SELECTDB);
    rdb.push(0x00); // DB 0

    // Resize database (hash table sizes)
    rdb.push(RDB_OPCODE_RESIZEDB);
    rdb.push(0x00); // DB hash table size
    rdb.push(0x00); // Expires hash table size

    // End of file
    rdb.push(RDB_OPCODE_EOF);

    // 8-byte checksum (CRC64)
    // For simplicity, we'll use zeros. A real implementation would calculate CRC64.
    rdb.extend_from_slice(&[0u8; 8]);

    rdb
}

// Tests for RDB creation are in primary.rs tests module
