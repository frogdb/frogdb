//! Full synchronization (FULLRESYNC) protocol handling.
//!
//! This module handles the full resynchronization process when a replica
//! cannot do partial sync (e.g., new replica or offset too old).
//!
//! # Protocol
//!
//! 1. Primary creates RocksDB checkpoint
//! 2. Primary sends checkpoint metadata (size, checksum)
//! 3. Primary streams checkpoint files
//! 4. Replica receives and validates (SHA256)
//! 5. Replica loads checkpoint
//! 6. Switch to incremental streaming

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

/// Maximum memory for full sync buffering (default: 512 MB)
pub const DEFAULT_FULLSYNC_MAX_MEMORY_MB: usize = 512;

/// Chunk size for streaming RDB data
const CHUNK_SIZE: usize = 64 * 1024; // 64 KB

/// State of a full sync operation on the primary side.
pub struct FullSyncState {
    /// Unique ID for this sync operation
    pub sync_id: u64,

    /// Replica ID
    pub replica_id: u64,

    /// Started at timestamp
    pub started_at: Instant,

    /// Total bytes to transfer
    pub total_bytes: u64,

    /// Bytes transferred so far
    pub bytes_transferred: AtomicU64,

    /// Checkpoint path (if using RocksDB checkpoint)
    pub checkpoint_path: Option<PathBuf>,

    /// State of the sync
    pub state: FullSyncPhase,
}

/// Phase of full sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullSyncPhase {
    /// Preparing checkpoint
    Preparing,
    /// Transferring data
    Transferring,
    /// Waiting for replica confirmation
    Confirming,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
}

impl std::fmt::Display for FullSyncPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FullSyncPhase::Preparing => write!(f, "preparing"),
            FullSyncPhase::Transferring => write!(f, "transferring"),
            FullSyncPhase::Confirming => write!(f, "confirming"),
            FullSyncPhase::Completed => write!(f, "completed"),
            FullSyncPhase::Failed => write!(f, "failed"),
        }
    }
}

impl FullSyncState {
    /// Create a new full sync state.
    pub fn new(sync_id: u64, replica_id: u64, total_bytes: u64) -> Self {
        Self {
            sync_id,
            replica_id,
            started_at: Instant::now(),
            total_bytes,
            bytes_transferred: AtomicU64::new(0),
            checkpoint_path: None,
            state: FullSyncPhase::Preparing,
        }
    }

    /// Get progress as a percentage (0-100).
    pub fn progress_percent(&self) -> f64 {
        if self.total_bytes == 0 {
            return 100.0;
        }
        let transferred = self.bytes_transferred.load(Ordering::Relaxed);
        (transferred as f64 / self.total_bytes as f64) * 100.0
    }

    /// Get bytes transferred.
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred.load(Ordering::Relaxed)
    }

    /// Add to bytes transferred.
    pub fn add_transferred(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get elapsed time.
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }

    /// Calculate transfer rate in bytes per second.
    pub fn transfer_rate(&self) -> f64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed < 0.001 {
            return 0.0;
        }
        self.bytes_transferred() as f64 / elapsed
    }
}

/// Metadata sent before RDB transfer.
#[derive(Debug, Clone)]
pub struct FullSyncMetadata {
    /// Total size of the RDB data
    pub rdb_size: u64,

    /// SHA256 checksum of the RDB data
    pub checksum: [u8; 32],

    /// Replication ID
    pub replication_id: String,

    /// Replication offset at time of checkpoint
    pub replication_offset: u64,
}

impl FullSyncMetadata {
    /// Serialize metadata to bytes.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);

        // Format: size:checksum_hex:repl_id:offset
        let checksum_hex = hex::encode(self.checksum);
        let data = format!(
            "{}:{}:{}:{}",
            self.rdb_size, checksum_hex, self.replication_id, self.replication_offset
        );

        buf.extend_from_slice(data.as_bytes());
        buf.freeze()
    }

    /// Parse metadata from bytes.
    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let s = std::str::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 in metadata"))?;

        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "malformed metadata",
            ));
        }

        let rdb_size: u64 = parts[0]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid rdb_size"))?;

        let checksum_bytes = hex::decode(parts[1])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid checksum hex"))?;

        if checksum_bytes.len() != 32 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum must be 32 bytes",
            ));
        }

        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&checksum_bytes);

        let replication_id = parts[2].to_string();

        let replication_offset: u64 = parts[3].parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid replication_offset")
        })?;

        Ok(Self {
            rdb_size,
            checksum,
            replication_id,
            replication_offset,
        })
    }
}

/// Calculate SHA256 checksum of a file.
pub async fn calculate_file_checksum(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK_SIZE];

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let result = hasher.finalize();
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&result);
    Ok(checksum)
}

/// Calculate SHA256 checksum of bytes.
pub fn calculate_bytes_checksum(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&result);
    checksum
}

/// Verify checksum of received data.
pub fn verify_checksum(data: &[u8], expected: &[u8; 32]) -> bool {
    let actual = calculate_bytes_checksum(data);
    actual == *expected
}

/// Stream file contents to a writer with progress tracking.
pub async fn stream_file_to_writer<W: AsyncWriteExt + Unpin>(
    path: &Path,
    writer: &mut W,
    progress: Option<&FullSyncState>,
) -> io::Result<u64> {
    let mut file = File::open(path).await?;
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut total_written = 0u64;

    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        writer.write_all(&buf[..n]).await?;
        total_written += n as u64;

        if let Some(state) = progress {
            state.add_transferred(n as u64);
        }
    }

    Ok(total_written)
}

/// Receive file contents from a reader with progress tracking.
pub async fn receive_to_file<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    path: &Path,
    expected_size: u64,
    progress: Option<&FullSyncState>,
) -> io::Result<[u8; 32]> {
    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let file = File::create(path).await?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut total_read = 0u64;

    while total_read < expected_size {
        let to_read = std::cmp::min(CHUNK_SIZE as u64, expected_size - total_read) as usize;
        let n = reader.read(&mut buf[..to_read]).await?;

        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed during file transfer",
            ));
        }

        writer.write_all(&buf[..n]).await?;
        hasher.update(&buf[..n]);
        total_read += n as u64;

        if let Some(state) = progress {
            state.add_transferred(n as u64);
        }
    }

    writer.flush().await?;

    let result = hasher.finalize();
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&result);
    Ok(checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_full_sync_state_progress() {
        let state = FullSyncState::new(1, 1, 1000);
        assert_eq!(state.progress_percent(), 0.0);

        state.add_transferred(500);
        assert_eq!(state.progress_percent(), 50.0);

        state.add_transferred(500);
        assert_eq!(state.progress_percent(), 100.0);
    }

    #[test]
    fn test_full_sync_state_zero_total() {
        let state = FullSyncState::new(1, 1, 0);
        assert_eq!(state.progress_percent(), 100.0);
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = FullSyncMetadata {
            rdb_size: 1000,
            checksum: [0xAB; 32],
            replication_id: "abc123".to_string(),
            replication_offset: 500,
        };

        let bytes = metadata.to_bytes();
        let parsed = FullSyncMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.rdb_size, 1000);
        assert_eq!(parsed.checksum, [0xAB; 32]);
        assert_eq!(parsed.replication_id, "abc123");
        assert_eq!(parsed.replication_offset, 500);
    }

    #[test]
    fn test_checksum_calculation() {
        let data = b"hello world";
        let checksum = calculate_bytes_checksum(data);

        // SHA256 of "hello world"
        let expected =
            hex::decode("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
                .unwrap();

        assert_eq!(checksum.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_verify_checksum() {
        let data = b"test data";
        let checksum = calculate_bytes_checksum(data);

        assert!(verify_checksum(data, &checksum));
        assert!(!verify_checksum(b"different data", &checksum));
    }

    #[tokio::test]
    async fn test_calculate_file_checksum() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        tokio::fs::write(&path, b"hello world").await.unwrap();

        let checksum = calculate_file_checksum(&path).await.unwrap();

        // SHA256 of "hello world"
        let expected =
            hex::decode("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
                .unwrap();

        assert_eq!(checksum.as_slice(), expected.as_slice());
    }

    #[test]
    fn test_full_sync_phase_display() {
        assert_eq!(format!("{}", FullSyncPhase::Preparing), "preparing");
        assert_eq!(format!("{}", FullSyncPhase::Transferring), "transferring");
        assert_eq!(format!("{}", FullSyncPhase::Completed), "completed");
    }
}
