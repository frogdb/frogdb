//! The checkpoint transport loop: drive the [`CheckpointStreamCodec`] over the
//! framed per-file headers/payloads into a scratch dir, folding the combined
//! checksum, then read the trailing metadata.
//!
//! Socket-agnostic by design: the loop takes any [`AsyncBufRead`] (the live
//! `BufReader<&mut BoxedStream>` in production, an in-memory duplex or
//! [`std::io::Cursor`] in tests), so the transport can be exercised without a
//! socket. It never sees `ReplicationState`; adopting the returned offset and
//! flipping to `Streaming` is the connection driver's job.

use crate::fullsync::{
    CheckpointChecksum, CheckpointStreamCodec, FullSyncMetadata, receive_to_file,
};
use std::io;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncBufRead;

/// Drive [`CheckpointStreamCodec`] over `file_count` framed files into
/// `incoming`, folding each file into a [`CheckpointChecksum`] in wire order,
/// then read the trailing [`FullSyncMetadata`]. Returns the metadata plus the
/// receiver's *computed* combined checksum so the stager can verify without
/// re-reading the files.
///
/// Creates `incoming` unconditionally up front (`fs::create_dir_all`). This is
/// load-bearing for `file_count == 0`: [`receive_to_file`] self-creates only
/// each file's *parent* ([`fullsync::receive_to_file`]), so with zero files no
/// dir would otherwise exist and the stager's `fs::rename(incoming ->
/// staged.dir())` would fail. Do NOT rely on per-file parent creation to make
/// the scratch dir.
///
/// [`fullsync::receive_to_file`]: crate::fullsync::receive_to_file
pub async fn receive_checkpoint_files<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    incoming: &Path,
    file_count: usize,
) -> io::Result<(FullSyncMetadata, [u8; 32])> {
    fs::create_dir_all(incoming).await?;
    let mut total_bytes = 0u64;
    // The combined checksum's coverage is owned by `CheckpointChecksum`: fold
    // each file in as it lands, in the same order the codec framed it.
    let mut combined = CheckpointChecksum::new();
    for i in 0..file_count {
        // Framing (per-file header) via the codec; the payload bytes still flow
        // through `receive_to_file`, which computes the per-file hash.
        let header = CheckpointStreamCodec::read_file_header(reader).await?;
        let file_path = incoming.join(&header.name);
        let checksum = receive_to_file(reader, &file_path, header.size, None).await?;
        total_bytes += header.size;
        combined.update_file(&header.name, &checksum);
        tracing::debug!(file = i + 1, filename = %header.name, size = header.size, checksum = %hex::encode(&checksum[..8]), "Received checkpoint file");
    }
    let metadata = CheckpointStreamCodec::read_metadata(reader).await?;
    tracing::info!(total_bytes = total_bytes, files = file_count, replication_id = %metadata.replication_id, offset = metadata.replication_offset, "Checkpoint received successfully");
    Ok((metadata, combined.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fullsync::{CheckpointFileHeader, calculate_bytes_checksum};
    use tempfile::tempdir;
    use tokio::io::AsyncWriteExt;

    /// Encode a whole checkpoint envelope (files + trailing metadata) into a
    /// byte buffer, folding the combined checksum the way the sender does.
    async fn encode_envelope(files: &[(String, Vec<u8>)]) -> (Vec<u8>, [u8; 32]) {
        let mut buf: Vec<u8> = Vec::new();
        let mut combined = CheckpointChecksum::new();
        for (name, payload) in files {
            CheckpointStreamCodec::write_file_header(
                &mut buf,
                &CheckpointFileHeader {
                    name: name.clone(),
                    size: payload.len() as u64,
                },
            )
            .await
            .unwrap();
            buf.write_all(payload).await.unwrap();
            combined.update_file(name, &calculate_bytes_checksum(payload));
        }
        let checksum = combined.finalize();
        let metadata = FullSyncMetadata {
            rdb_size: files.iter().map(|(_, p)| p.len() as u64).sum(),
            checksum,
            replication_id: "repl-receiver".to_string(),
            replication_offset: 123,
        };
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();
        (buf, checksum)
    }

    #[tokio::test]
    async fn receiver_reads_framed_files_and_metadata() {
        let files = vec![
            ("CURRENT".to_string(), b"MANIFEST-000005\n".to_vec()),
            ("000042.sst".to_string(), (0u8..=200).collect()),
            ("MANIFEST-000005".to_string(), Vec::new()),
        ];
        let (buf, expected_checksum) = encode_envelope(&files).await;

        let dir = tempdir().unwrap();
        let incoming = dir.path().join("checkpoint_incoming");
        let mut cursor = std::io::Cursor::new(buf);

        let (metadata, computed) = receive_checkpoint_files(&mut cursor, &incoming, files.len())
            .await
            .unwrap();

        // The metadata frame round-trips and the folded checksum equals the
        // one the sender put on the wire.
        assert_eq!(metadata.replication_id, "repl-receiver");
        assert_eq!(metadata.replication_offset, 123);
        assert_eq!(computed, expected_checksum);
        assert_eq!(computed, metadata.checksum);

        // Every file landed in the scratch dir with the right bytes.
        for (name, payload) in &files {
            let on_disk = tokio::fs::read(incoming.join(name)).await.unwrap();
            assert_eq!(&on_disk, payload, "file {name} mismatch");
        }
    }

    #[tokio::test]
    async fn receiver_creates_incoming_for_zero_files() {
        // With no files, only the unconditional create_dir_all makes the scratch
        // dir exist — the load-bearing guarantee the stager's rename relies on.
        let (buf, _) = encode_envelope(&[]).await;
        let dir = tempdir().unwrap();
        let incoming = dir.path().join("checkpoint_incoming");
        let mut cursor = std::io::Cursor::new(buf);

        let (metadata, _computed) = receive_checkpoint_files(&mut cursor, &incoming, 0)
            .await
            .unwrap();
        assert_eq!(metadata.replication_offset, 123);
        assert!(incoming.is_dir(), "incoming scratch dir must exist");
    }

    #[tokio::test]
    async fn receiver_truncated_stream_yields_unexpected_eof() {
        // A single file declared but its payload truncated: receive_to_file must
        // report UnexpectedEof.
        let mut buf: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_file_header(
            &mut buf,
            &CheckpointFileHeader {
                name: "CURRENT".to_string(),
                size: 16,
            },
        )
        .await
        .unwrap();
        buf.write_all(b"only-8b").await.unwrap(); // fewer than 16 bytes

        let dir = tempdir().unwrap();
        let incoming = dir.path().join("checkpoint_incoming");
        let mut cursor = std::io::Cursor::new(buf);

        let err = receive_checkpoint_files(&mut cursor, &incoming, 1)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
