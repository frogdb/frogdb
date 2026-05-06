//! Full-synchronization protocol primitives.
//!
//! This module is now a thin layer of stateless utilities used by both the
//! primary's [`crate::replica_session::ReplicaSession`] and the replica's
//! [`crate::replica::connection`] code path:
//!
//! - [`FullSyncMetadata`] — the per-snapshot metadata frame on the wire
//! - File I/O helpers ([`stream_file_to_writer`], [`receive_to_file`])
//! - Checksum helpers
//!
//! Per-replica progress and lifecycle live on `ReplicaSession`; this file
//! has no per-session state of its own.

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};

/// Maximum memory for full sync buffering (default: 512 MB).
pub const DEFAULT_FULLSYNC_MAX_MEMORY_MB: usize = 512;

/// Chunk size for streaming RDB data.
const CHUNK_SIZE: usize = 64 * 1024;

/// Metadata frame sent at the end of a checkpoint stream.
///
/// Wire format: `<rdb_size>:<checksum_hex>:<replication_id>:<replication_offset>`
#[derive(Debug, Clone)]
pub struct FullSyncMetadata {
    /// Total byte size of the RDB / checkpoint payload.
    pub rdb_size: u64,
    /// SHA256 of the payload.
    pub checksum: [u8; 32],
    /// Replication id at time of checkpoint.
    pub replication_id: String,
    /// Replication offset at time of checkpoint.
    pub replication_offset: u64,
}

impl FullSyncMetadata {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);
        let checksum_hex = hex::encode(self.checksum);
        let data = format!(
            "{}:{}:{}:{}",
            self.rdb_size, checksum_hex, self.replication_id, self.replication_offset
        );
        buf.extend_from_slice(data.as_bytes());
        buf.freeze()
    }

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

/// SHA256 checksum of a file's contents.
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

/// SHA256 checksum of a byte slice.
pub fn calculate_bytes_checksum(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut checksum = [0u8; 32];
    checksum.copy_from_slice(&result);
    checksum
}

/// Verify that `data`'s checksum matches `expected`.
pub fn verify_checksum(data: &[u8], expected: &[u8; 32]) -> bool {
    let actual = calculate_bytes_checksum(data);
    actual == *expected
}

/// Stream a file's contents into `writer`, optionally accumulating progress
/// into a shared atomic counter (used by the session for in-flight reporting).
pub async fn stream_file_to_writer<W: AsyncWriteExt + Unpin>(
    path: &Path,
    writer: &mut W,
    progress: Option<&AtomicU64>,
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
        if let Some(counter) = progress {
            counter.fetch_add(n as u64, Ordering::Relaxed);
        }
    }
    Ok(total_written)
}

/// Receive `expected_size` bytes from `reader` into a file at `path`,
/// computing a SHA256 checksum and optionally accumulating progress.
pub async fn receive_to_file<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    path: &Path,
    expected_size: u64,
    progress: Option<&AtomicU64>,
) -> io::Result<[u8; 32]> {
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
        if let Some(counter) = progress {
            counter.fetch_add(n as u64, Ordering::Relaxed);
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
        let expected =
            hex::decode("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
                .unwrap();
        assert_eq!(checksum.as_slice(), expected.as_slice());
    }

    #[tokio::test]
    async fn test_stream_and_receive_progress() {
        // Round-trip: write a file, then stream it through an in-memory pipe,
        // and verify the progress counter and received bytes match.
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.bin");
        let payload: Vec<u8> = (0u8..=255).cycle().take(150_000).collect();
        tokio::fs::write(&src, &payload).await.unwrap();

        let send_progress = AtomicU64::new(0);
        let recv_progress = AtomicU64::new(0);
        let (mut writer, mut reader) = tokio::io::duplex(1 << 16);

        let send_ref = &send_progress;
        let recv_ref = &recv_progress;
        let send = async {
            let result = stream_file_to_writer(&src, &mut writer, Some(send_ref)).await;
            // Drop the writer so the reader sees EOF (not strictly needed here
            // because receive_to_file uses an explicit byte count, but it
            // cleans up the duplex half deterministically).
            drop(writer);
            result
        };

        let dst = dir.path().join("dst.bin");
        let recv = async {
            receive_to_file(&mut reader, &dst, payload.len() as u64, Some(recv_ref)).await
        };

        let (sent, received) = tokio::join!(send, recv);
        assert_eq!(sent.unwrap(), payload.len() as u64);
        assert_eq!(send_progress.load(Ordering::Relaxed), payload.len() as u64);
        assert_eq!(recv_progress.load(Ordering::Relaxed), payload.len() as u64);
        let received_checksum = received.unwrap();
        let expected_checksum = calculate_bytes_checksum(&payload);
        assert_eq!(received_checksum, expected_checksum);
        let on_disk = tokio::fs::read(&dst).await.unwrap();
        assert_eq!(on_disk, payload);
    }
}
