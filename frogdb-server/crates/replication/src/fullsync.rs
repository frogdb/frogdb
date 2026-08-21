//! Full-synchronization protocol primitives.
//!
//! This module is now a thin layer of stateless utilities used by both the
//! primary's [`crate::replica_session::ReplicaSession`] and the replica's
//! [`crate::replica::connection`] code path:
//!
//! - [`FullSyncMetadata`] — the per-snapshot metadata frame on the wire
//! - [`CheckpointStreamCodec`] — the symmetric checkpoint envelope codec that
//!   owns the on-wire grammar both the primary and the replica used to
//!   hand-roll (marker + count prelude, per-file headers, trailing metadata)
//! - [`CheckpointChecksum`] — the combined-SHA256 accumulator that owns *what
//!   bytes the checkpoint checksum covers*, so the sender and receiver cannot
//!   drift on coverage
//! - File I/O helpers ([`stream_file_to_writer`], [`receive_to_file`])
//! - Checksum helpers
//!
//! Per-replica progress and lifecycle live on `ReplicaSession`; this file
//! has no per-session state of its own.

pub mod receiver;
pub mod stager;

pub use receiver::receive_checkpoint_files;
pub use stager::{CheckpointStager, StagedOutcome};

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{self, File};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufWriter};

/// Chunk size for streaming RDB data.
///
/// A buffering knob, not a protocol constant: every read/write loop that uses it
/// bounds each pass by `min(CHUNK_SIZE, bytes_remaining)`, so any positive value
/// transfers the same bytes and computes the same checksums — only the number of
/// syscalls changes. Documented equivalent mutant: no test can observe the
/// arithmetic here.
const CHUNK_SIZE: usize = 64 * 1024;

/// Per-shard coverage watermarks for one full-sync payload — the `Y_s` vector.
///
/// `Y_s` is the offset of the **last write shard `s` broadcast** at the moment
/// that shard's contribution to the payload was captured: its drain-ack on the
/// checkpoint path, its export message on the live-dataset path. Both capture
/// points are messages the shard task itself processes, and a shard task is
/// single-threaded with `ReplicationBroadcast` as the terminal write effect
/// (`core/src/shard/post_execution.rs`), so every write in shard `s`'s half of
/// the payload was broadcast at or below `Y_s` and none above it was.
///
/// That is the fact the single trailer offset cannot express. `replication_offset`
/// is captured before *any* shard is touched, so it is only a lower bound on what
/// the payload holds (FM-REPLICATION-004's `offset <= data` direction); the
/// replica installing it therefore replays `(replication_offset, current]` over a
/// keyspace that already holds part of that range. The vector is the exact upper
/// bound, per shard, that makes the replay dedupable — see FM-REPLICATION-066.
///
/// Indexed by the **sender's** shard id, which is what every replication frame
/// carries (`ReplicationFrame::shard_id`), so a replica with a different shard
/// count still reads the right watermark. An index past the end has *no*
/// watermark rather than a zero one: absent means "apply it", never "skip it".
///
/// Serialized transparently as the bare list, because it rides in the staged
/// `replication_metadata.json` (ruling R15) and in the persisted replication
/// state, where a wrapper object would buy nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct ShardCoverage(Vec<u64>);

impl ShardCoverage {
    /// The vector for a payload whose shard `s` last broadcast at `watermarks[s]`.
    pub fn from_watermarks(watermarks: Vec<u64>) -> Self {
        Self(watermarks)
    }

    /// No watermarks at all — a payload that carries no coverage claim, so the
    /// replica installs no floors and behaves exactly as it did before
    /// FM-REPLICATION-066.
    ///
    /// Mutating this to `Default::default()` is an equivalent mutant: the
    /// derived `Default` *is* the empty vector, so no observation can separate
    /// the two. The name is kept because "no claim" is the meaning at every
    /// call site, and the mutant is excluded by name in `.cargo/mutants.toml`.
    pub fn none() -> Self {
        Self(Vec::new())
    }

    /// Whether this vector makes no claim at all. Not the same question as
    /// "are all its watermarks zero": a vector of zeros claims that each of
    /// those shards contributed nothing above 0, which is a floor.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[u64] {
        &self.0
    }

    /// Shard `shard`'s watermark, or `None` when this vector does not describe
    /// that shard (a control-shard frame, or a sender with fewer shards).
    pub fn watermark(&self, shard: u16) -> Option<u64> {
        self.0.get(usize::from(shard)).copied()
    }

    /// The highest watermark in the vector, or `0` when it is empty — the point
    /// at or above which the replica's applied head has caught up with
    /// everything the payload covered, and the floors stop mattering.
    pub fn max(&self) -> u64 {
        self.0.iter().copied().max().unwrap_or(0)
    }

    /// The wire/JSON rendering: watermarks in shard order, comma-separated. An
    /// empty vector renders as the empty string, which is why the trailer's
    /// field count does not change with the shard count.
    pub fn render(&self) -> String {
        let mut out = String::new();
        for (i, w) in self.0.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            use std::fmt::Write;
            let _ = write!(out, "{w}");
        }
        out
    }

    /// Parse [`Self::render`]'s output. A malformed watermark is an error rather
    /// than a dropped field: a floor vector that silently lost a shard would let
    /// that shard's half of the payload be replayed a second time.
    pub fn parse(s: &str) -> io::Result<Self> {
        if s.is_empty() {
            return Ok(Self::none());
        }
        let mut out = Vec::new();
        for part in s.split(',') {
            out.push(part.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid shard coverage watermark",
                )
            })?);
        }
        Ok(Self(out))
    }
}

/// Metadata frame sent at the end of a checkpoint stream.
///
/// Wire format:
/// `<rdb_size>:<checksum_hex>:<replication_id>:<replication_offset>:<coverage>`
///
/// The trailing `coverage` field is [`ShardCoverage::render`] — comma-separated
/// per-shard watermarks, possibly empty. It is a fifth `:`-delimited field
/// rather than an optional suffix, so a sender that ships no coverage and one
/// whose message was truncated are different bytes, not the same ones.
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
    /// Per-shard coverage watermarks for this payload (FM-REPLICATION-066).
    pub coverage: ShardCoverage,
}

impl FullSyncMetadata {
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);
        let checksum_hex = hex::encode(self.checksum);
        let data = format!(
            "{}:{}:{}:{}:{}",
            self.rdb_size,
            checksum_hex,
            self.replication_id,
            self.replication_offset,
            self.coverage.render()
        );
        buf.extend_from_slice(data.as_bytes());
        buf.freeze()
    }

    pub fn from_bytes(data: &[u8]) -> io::Result<Self> {
        let s = std::str::from_utf8(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 in metadata"))?;

        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 5 {
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
        let coverage = ShardCoverage::parse(parts[4])?;
        Ok(Self {
            rdb_size,
            checksum,
            replication_id,
            replication_offset,
            coverage,
        })
    }
}

/// Wire marker identifying a FrogDB checkpoint stream (as opposed to a plain
/// RDB payload). Sent as `$FROGDB_CHECKPOINT\r\n` at the head of the envelope.
pub const CHECKPOINT_MARKER: &str = "FROGDB_CHECKPOINT";

/// Wire marker identifying a FrogDB *live dataset* stream: the same envelope,
/// but the bodies are per-shard dataset blobs serialized straight out of the
/// primary's memory rather than checkpoint files.
///
/// A primary with `persistence.enabled = false` has no RocksDB to checkpoint,
/// yet it still owes a full resync the whole dataset (Redis's diskless
/// replication ships the RDB over the socket for exactly this reason — no
/// persistence configured never means no dataset on the wire). Marking the two
/// payload kinds apart keeps the replica's handling honest: files are staged to
/// disk and installed from there, blobs are installed directly.
pub const SNAPSHOT_MARKER: &str = "FROGDB_SNAPSHOT";

/// Upper bounds on the length-prefixed counts in the envelope. These reject a
/// malformed or hostile length *before* it drives an allocation, so a corrupt
/// stream yields a clean [`io::ErrorKind::InvalidData`] instead of a capacity
/// panic or a multi-gigabyte reservation. Legitimate checkpoints sit far below
/// these bounds (dozens of short-named SST files, a ~150-byte metadata frame).
const MAX_CHECKPOINT_FILE_COUNT: usize = 1_000_000;
const MAX_CHECKPOINT_NAME_LEN: usize = 64 * 1024;
const MAX_CHECKPOINT_METADATA_LEN: usize = 64 * 1024;

/// One checkpoint file on the wire: `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`.
///
/// The codec owns the *header* (name + size); the `size` payload bytes still
/// flow through [`stream_file_to_writer`] / [`receive_to_file`] (which also
/// compute the per-file SHA256).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointFileHeader {
    pub name: String,
    pub size: u64,
}

/// The full-sync checkpoint envelope, as a symmetric codec.
///
/// This is the single definition of the on-wire grammar that the doc comment in
/// `ReplicaSession::stream_checkpoint` used to describe only in prose and that
/// was realized three times by hand (the sender, the PSYNC-reply marker/count
/// detector, and the receiver body parser). The sender and both receiver stages
/// now call these methods instead of re-encoding the bytes.
///
/// Framing only — the *bytes* of each file flow through the existing
/// [`stream_file_to_writer`] / [`receive_to_file`] helpers. The codec owns the
/// delimiters; the helpers own the payload. The combined checksum policy and the
/// `StagedCheckpoint` landing contract stay in the caller by design.
///
/// The grammar, top to bottom:
/// 1. Prelude: `$FROGDB_CHECKPOINT\r\n<count>\r\n`
/// 2. For each of `count` files: `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`
/// 3. Trailing metadata: `$<len>\r\n<FullSyncMetadata bytes>\r\n`
pub struct CheckpointStreamCodec;

impl CheckpointStreamCodec {
    // --- encode ---

    /// Write `$FROGDB_CHECKPOINT\r\n<count>\r\n`, ahead of the file bodies.
    pub async fn write_prelude<W: AsyncWriteExt + Unpin>(
        w: &mut W,
        file_count: usize,
    ) -> io::Result<()> {
        Self::write_marked_prelude(w, CHECKPOINT_MARKER, file_count).await
    }

    /// Write `$FROGDB_SNAPSHOT\r\n<count>\r\n`, ahead of the dataset blobs.
    ///
    /// Same grammar as [`write_prelude`](Self::write_prelude) — only the marker
    /// differs, so the replica can tell a staged-to-disk checkpoint from an
    /// installed-directly live dataset before it reads a single body.
    pub async fn write_snapshot_prelude<W: AsyncWriteExt + Unpin>(
        w: &mut W,
        blob_count: usize,
    ) -> io::Result<()> {
        Self::write_marked_prelude(w, SNAPSHOT_MARKER, blob_count).await
    }

    async fn write_marked_prelude<W: AsyncWriteExt + Unpin>(
        w: &mut W,
        marker: &str,
        count: usize,
    ) -> io::Result<()> {
        w.write_all(format!("${marker}\r\n").as_bytes()).await?;
        w.write_all(format!("{count}\r\n").as_bytes()).await?;
        Ok(())
    }

    /// Write one per-file header: `$<name_len>\r\n<name>\r\n$<size>\r\n`. The
    /// caller then streams `size` payload bytes via [`stream_file_to_writer`].
    pub async fn write_file_header<W: AsyncWriteExt + Unpin>(
        w: &mut W,
        h: &CheckpointFileHeader,
    ) -> io::Result<()> {
        w.write_all(format!("${}\r\n{}\r\n", h.name.len(), h.name).as_bytes())
            .await?;
        w.write_all(format!("${}\r\n", h.size).as_bytes()).await?;
        Ok(())
    }

    /// Write the trailing metadata frame: `$<len>\r\n<FullSyncMetadata bytes>\r\n`.
    pub async fn write_metadata<W: AsyncWriteExt + Unpin>(
        w: &mut W,
        m: &FullSyncMetadata,
    ) -> io::Result<()> {
        let bytes = m.to_bytes();
        w.write_all(format!("${}\r\n", bytes.len()).as_bytes())
            .await?;
        w.write_all(&bytes).await?;
        w.write_all(b"\r\n").await?;
        Ok(())
    }

    // --- inverses ---

    /// Parse the file-count line (`<count>\r\n`) of the prelude.
    ///
    /// The marker line itself is detected one layer up, in the PSYNC reply
    /// reader, because that reader must first distinguish `$FROGDB_CHECKPOINT`
    /// from a plain `$<rdb_size>` RDB. Once it decides the stream is a
    /// checkpoint it routes the count through this method so the parse rule
    /// (including the sanity bound) lives with the rest of the grammar.
    pub fn parse_file_count(line: &str) -> io::Result<usize> {
        let count: usize = line.trim().parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid file count in checkpoint",
            )
        })?;
        if count > MAX_CHECKPOINT_FILE_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint file count exceeds maximum",
            ));
        }
        Ok(count)
    }

    /// Read the whole prelude — marker line then count line — returning the file
    /// count. This is the symmetric inverse of [`write_prelude`](Self::write_prelude)
    /// used by round-trip tests; the live receiver splits the marker detection
    /// out (see [`parse_file_count`](Self::parse_file_count)) so the RDB-vs-checkpoint
    /// decision is not entangled with the envelope.
    pub async fn read_prelude<R: AsyncBufRead + Unpin>(r: &mut R) -> io::Result<usize> {
        let mut marker_line = String::new();
        r.read_line(&mut marker_line).await?;
        let marker = marker_line.trim().strip_prefix('$').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "expected checkpoint marker prefix",
            )
        })?;
        if marker != CHECKPOINT_MARKER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected checkpoint marker",
            ));
        }
        let mut count_line = String::new();
        r.read_line(&mut count_line).await?;
        Self::parse_file_count(&count_line)
    }

    /// Read one `$<name_len>\r\n<name>\r\n$<size>\r\n` header. The caller then
    /// consumes `size` payload bytes via [`receive_to_file`].
    pub async fn read_file_header<R: AsyncBufRead + Unpin>(
        r: &mut R,
    ) -> io::Result<CheckpointFileHeader> {
        let mut line = String::new();
        r.read_line(&mut line).await?;
        let name_len = parse_dollar_len(&line, "invalid filename length", MAX_CHECKPOINT_NAME_LEN)?;
        // `name_len + 2` also consumes the trailing `\r\n`.
        let mut name_buf = vec![0u8; name_len + 2];
        r.read_exact(&mut name_buf).await?;
        // Decoded strictly, not lossily: `from_utf8_lossy` maps every invalid
        // byte to U+FFFD, so two distinct wire names would decode to the same
        // `String` — same staging path, different bytes folded into the
        // combined checksum.
        let name = std::str::from_utf8(&name_buf[..name_len]).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "checkpoint file name is not valid UTF-8",
            )
        })?;
        let name = checkpoint_file_name(name)?.to_string();

        let mut size_line = String::new();
        r.read_line(&mut size_line).await?;
        let size: u64 = size_line
            .trim()
            .trim_start_matches('$')
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid file size"))?;
        Ok(CheckpointFileHeader { name, size })
    }

    /// Read the trailing `$<len>\r\n<bytes>\r\n` and parse a [`FullSyncMetadata`].
    pub async fn read_metadata<R: AsyncBufRead + Unpin>(r: &mut R) -> io::Result<FullSyncMetadata> {
        let mut line = String::new();
        r.read_line(&mut line).await?;
        let metadata_len = parse_dollar_len(
            &line,
            "invalid metadata length",
            MAX_CHECKPOINT_METADATA_LEN,
        )?;
        // `metadata_len + 2` also consumes the trailing `\r\n`.
        let mut buf = vec![0u8; metadata_len + 2];
        r.read_exact(&mut buf).await?;
        FullSyncMetadata::from_bytes(&buf[..metadata_len])
    }
}

/// Accumulator for the full-sync checkpoint's *combined* checksum — the single
/// SHA256 carried by [`FullSyncMetadata::checksum`] and verified by the receiver
/// after every file lands.
///
/// The coverage is part of the checkpoint codec's contract, so it lives here
/// beside the framing rather than being hand-rolled on each side. Both the
/// primary and the replica fold every checkpoint file through
/// [`update_file`](Self::update_file) — in the same order the codec frames them
/// on the wire — then call [`finalize`](Self::finalize). A future coverage
/// change (an added field, a reordering) is a single edit here instead of two
/// independent `Sha256::new()` loops that must be kept byte-identical by
/// inspection.
///
/// Coverage, byte-for-byte:
/// `SHA256( name_0 || hash_0 || name_1 || hash_1 || … )`, where `name_i` is the
/// UTF-8 filename bytes and `hash_i` is that file's raw 32-byte SHA256, in wire
/// order, with no delimiters, length prefixes, or metadata mixed in.
#[derive(Debug, Default)]
pub struct CheckpointChecksum {
    hasher: Sha256,
}

impl CheckpointChecksum {
    /// A fresh accumulator covering no files yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one checkpoint file into the combined checksum: the filename bytes
    /// followed by that file's raw 32-byte SHA256. Call once per file, in the
    /// order the files appear on the wire.
    pub fn update_file(&mut self, name: &str, file_hash: &[u8; 32]) {
        self.hasher.update(name.as_bytes());
        self.hasher.update(file_hash);
    }

    /// Finalize into the 32-byte combined checksum carried by
    /// [`FullSyncMetadata::checksum`].
    pub fn finalize(self) -> [u8; 32] {
        let mut checksum = [0u8; 32];
        checksum.copy_from_slice(&self.hasher.finalize());
        checksum
    }
}

/// Validate a wire-supplied checkpoint file *name*, returning it unchanged.
///
/// The name arrives from the primary and is joined onto the replica's staging
/// directory before a single checksum has been verified, so on the replica it is
/// remote input on the *write* path: [`Path::join`] discards the staging
/// directory entirely for `/etc/authorized_keys` and climbs out of it for
/// `../../frogdb.conf`. A checkpoint file name is a name, never a path, so the
/// rule is exact — it must decompose to one [`std::path::Component::Normal`] and
/// re-encode to itself.
///
/// The round-trip half of that rule carries as much weight as the component
/// count: `components()` normalizes, so `CURRENT`, `CURRENT/` and `CURRENT/.`
/// all yield the single component `CURRENT`. Accepting them would let three
/// distinct wire names land on one staged file while folding three *different*
/// byte strings into the combined checksum, which
/// [`CheckpointChecksum::update_file`] computes over the name as sent.
fn checkpoint_file_name(name: &str) -> io::Result<&str> {
    let mut components = Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(only)), None) if only == std::ffi::OsStr::new(name) => {
            Ok(name)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "checkpoint file name must be a single path component",
        )),
    }
}

/// Parse a `$<n>\r\n` length line, rejecting a non-numeric value or one that
/// exceeds `max` with a clean [`io::ErrorKind::InvalidData`].
fn parse_dollar_len(line: &str, context: &'static str, max: usize) -> io::Result<usize> {
    let n: usize = line
        .trim()
        .trim_start_matches('$')
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, context))?;
    if n > max {
        return Err(io::Error::new(io::ErrorKind::InvalidData, context));
    }
    Ok(n)
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

/// Receive `expected_size` bytes from `reader` into `dir`/`name`, computing a
/// SHA256 checksum and optionally accumulating progress.
///
/// Takes the directory and the file name separately, and re-validates the name
/// through [`checkpoint_file_name`], so a received file cannot land outside
/// `dir` no matter how the caller obtained the name. The codec already rejects
/// an escaping name at the wire boundary; keeping the rule here too makes
/// containment a property of this function instead of a property every caller
/// has to preserve while building a path.
pub async fn receive_to_file<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    dir: &Path,
    name: &str,
    expected_size: u64,
    progress: Option<&AtomicU64>,
) -> io::Result<[u8; 32]> {
    let path = dir.join(checkpoint_file_name(name)?);
    fs::create_dir_all(dir).await?;
    let file = File::create(&path).await?;
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
    use proptest::prelude::Strategy;
    use tempfile::tempdir;

    #[test]
    fn test_metadata_serialization() {
        let metadata = FullSyncMetadata {
            rdb_size: 1000,
            checksum: [0xAB; 32],
            replication_id: "abc123".to_string(),
            replication_offset: 500,
            coverage: crate::fullsync::ShardCoverage::none(),
        };
        let bytes = metadata.to_bytes();
        let parsed = FullSyncMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.rdb_size, 1000);
        assert_eq!(parsed.checksum, [0xAB; 32]);
        assert_eq!(parsed.replication_id, "abc123");
        assert_eq!(parsed.replication_offset, 500);
        assert!(parsed.coverage.is_empty());
    }

    // FM-REPLICATION-066
    #[test]
    fn a_coverage_vector_round_trips_through_the_trailer() {
        let metadata = FullSyncMetadata {
            rdb_size: 1000,
            checksum: [0xAB; 32],
            replication_id: "abc123".to_string(),
            replication_offset: 500,
            coverage: ShardCoverage::from_watermarks(vec![700, 0, u64::MAX]),
        };
        let parsed = FullSyncMetadata::from_bytes(&metadata.to_bytes()).unwrap();
        assert_eq!(parsed.coverage.as_slice(), &[700, 0, u64::MAX]);
    }

    // FM-REPLICATION-066
    #[test]
    fn a_trailer_without_the_coverage_field_is_refused() {
        // Four fields is the pre-FM-REPLICATION-066 trailer. It is refused
        // rather than read as an empty vector: an empty vector is a positive
        // claim ("this payload covers nothing"), and inferring it from a
        // missing field would let a truncated message install no floors and
        // silently double-apply the overshipped range.
        let four_fields = format!("1000:{}:abc123:500", hex::encode([0xABu8; 32]));
        assert!(FullSyncMetadata::from_bytes(four_fields.as_bytes()).is_err());
    }

    // FM-REPLICATION-066
    #[test]
    fn a_malformed_watermark_fails_the_whole_vector() {
        assert!(ShardCoverage::parse("10,,30").is_err());
        assert!(ShardCoverage::parse("10,nope").is_err());
        assert!(ShardCoverage::parse("-1").is_err());
    }

    // FM-REPLICATION-066
    #[test]
    fn an_empty_coverage_field_parses_as_no_watermarks() {
        let parsed = ShardCoverage::parse("").unwrap();
        assert!(parsed.is_empty());
        assert_eq!(parsed.render(), "");
        assert_eq!(parsed.max(), 0);
        assert_eq!(parsed.watermark(0), None);

        // A vector of zeros is not the same answer: it claims two shards
        // contributed nothing above 0, and that claim is a pair of floors.
        let zeros = ShardCoverage::parse("0,0").unwrap();
        assert!(
            !zeros.is_empty(),
            "an all-zero vector still makes a claim, so it is not the empty one"
        );
        assert_eq!(zeros.max(), 0);
        assert_eq!(zeros.watermark(1), Some(0));
    }

    // FM-REPLICATION-066
    #[test]
    fn a_watermark_past_the_end_of_the_vector_is_absent_not_zero() {
        let coverage = ShardCoverage::from_watermarks(vec![7]);
        assert_eq!(coverage.watermark(0), Some(7));
        assert_eq!(coverage.watermark(1), None);
        assert_eq!(coverage.watermark(u16::MAX), None);
        assert_eq!(coverage.max(), 7);
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
            receive_to_file(
                &mut reader,
                dir.path(),
                "dst.bin",
                payload.len() as u64,
                Some(recv_ref),
            )
            .await
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

    /// A file bigger than one chunk is the case where the per-pass bound has to
    /// shrink: the last pass may read only the remainder, or it eats into
    /// whatever follows on the stream. On a real socket that "whatever" is the
    /// next file's header (or the trailing metadata), so an over-long final read
    /// silently corrupts the rest of the envelope.
    #[tokio::test]
    async fn receive_to_file_stops_at_the_declared_size_across_chunks() {
        let dir = tempdir().unwrap();
        // Just past one chunk, so the transfer needs a short final pass.
        let payload: Vec<u8> = (0u8..=255).cycle().take(CHUNK_SIZE + 10).collect();
        let trailer = b"$7\r\nnextfile\r\n".to_vec();
        let mut wire = payload.clone();
        wire.extend_from_slice(&trailer);
        let mut cursor = std::io::Cursor::new(wire);

        let checksum = receive_to_file(
            &mut cursor,
            dir.path(),
            "dst.bin",
            payload.len() as u64,
            None,
        )
        .await
        .unwrap();

        assert_eq!(checksum, calculate_bytes_checksum(&payload));
        let on_disk = tokio::fs::read(dir.path().join("dst.bin")).await.unwrap();
        assert_eq!(
            on_disk.len(),
            payload.len(),
            "no byte past the declared size"
        );
        assert_eq!(on_disk, payload);
        // The bytes that followed the payload are still on the stream for the
        // next reader.
        let consumed = cursor.position() as usize;
        assert_eq!(consumed, payload.len());
        assert_eq!(&cursor.into_inner()[consumed..], &trailer[..]);
    }

    // ---- CheckpointStreamCodec ----

    fn sample_metadata() -> FullSyncMetadata {
        FullSyncMetadata {
            rdb_size: 42,
            checksum: [0xAB; 32],
            replication_id: "repl-1".to_string(),
            replication_offset: 99,
            coverage: crate::fullsync::ShardCoverage::none(),
        }
    }

    // FM-REPLICATION-035
    /// Encode a whole envelope (prelude + interleaved headers/payloads +
    /// metadata) into a buffer, then decode it back and assert every field
    /// survives. The checkpoint-stream analogue of `frame.rs`'s round-trip test
    /// and the drift guard between the send and receive paths.
    #[tokio::test]
    async fn test_checkpoint_codec_round_trip() {
        let files: Vec<(CheckpointFileHeader, Vec<u8>)> = vec![
            (
                CheckpointFileHeader {
                    name: "CURRENT".to_string(),
                    size: 4,
                },
                b"abcd".to_vec(),
            ),
            (
                CheckpointFileHeader {
                    name: "000042.sst".to_string(),
                    size: 0,
                },
                Vec::new(),
            ),
            (
                CheckpointFileHeader {
                    name: "MANIFEST-000005".to_string(),
                    size: 5,
                },
                b"hello".to_vec(),
            ),
        ];
        let metadata = sample_metadata();

        let mut buf: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_prelude(&mut buf, files.len())
            .await
            .unwrap();
        for (header, payload) in &files {
            CheckpointStreamCodec::write_file_header(&mut buf, header)
                .await
                .unwrap();
            buf.write_all(payload).await.unwrap();
        }
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let count = CheckpointStreamCodec::read_prelude(&mut cursor)
            .await
            .unwrap();
        assert_eq!(count, files.len());
        for (expected_header, expected_payload) in &files {
            let header = CheckpointStreamCodec::read_file_header(&mut cursor)
                .await
                .unwrap();
            assert_eq!(&header, expected_header);
            let mut payload = vec![0u8; header.size as usize];
            cursor.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, expected_payload);
        }
        let decoded = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap();
        assert_eq!(decoded.rdb_size, metadata.rdb_size);
        assert_eq!(decoded.checksum, metadata.checksum);
        assert_eq!(decoded.replication_id, metadata.replication_id);
        assert_eq!(decoded.replication_offset, metadata.replication_offset);
    }

    /// End-to-end checksum agreement: drive the *whole* checkpoint the way the
    /// sender and receiver do — frame each file, fold it into a
    /// [`CheckpointChecksum`], carry the finalized hash in the trailing
    /// [`FullSyncMetadata`], then on the read side re-fold each decoded file and
    /// assert the recomputed combined checksum matches the one on the wire. This
    /// is the drift guard for checksum *coverage*: the two sides can only agree
    /// because they share one accumulator definition.
    #[tokio::test]
    async fn test_checkpoint_checksum_agreement() {
        let files: Vec<(String, Vec<u8>)> = vec![
            ("CURRENT".to_string(), b"MANIFEST-000005\n".to_vec()),
            ("000042.sst".to_string(), (0u8..=200).collect()),
            ("MANIFEST-000005".to_string(), Vec::new()),
        ];

        // --- sender side ---
        let mut buf: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_prelude(&mut buf, files.len())
            .await
            .unwrap();
        let mut send_checksum = CheckpointChecksum::new();
        for (name, payload) in &files {
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
            send_checksum.update_file(name, &calculate_bytes_checksum(payload));
        }
        let metadata = FullSyncMetadata {
            rdb_size: files.iter().map(|(_, p)| p.len() as u64).sum(),
            checksum: send_checksum.finalize(),
            replication_id: "repl-agreement".to_string(),
            replication_offset: 7,
            coverage: crate::fullsync::ShardCoverage::none(),
        };
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();

        // --- receiver side ---
        let mut cursor = std::io::Cursor::new(buf);
        let count = CheckpointStreamCodec::read_prelude(&mut cursor)
            .await
            .unwrap();
        assert_eq!(count, files.len());
        let mut recv_checksum = CheckpointChecksum::new();
        for _ in 0..count {
            let header = CheckpointStreamCodec::read_file_header(&mut cursor)
                .await
                .unwrap();
            let mut payload = vec![0u8; header.size as usize];
            cursor.read_exact(&mut payload).await.unwrap();
            recv_checksum.update_file(&header.name, &calculate_bytes_checksum(&payload));
        }
        let decoded = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap();
        let computed = recv_checksum.finalize();
        assert_eq!(computed, decoded.checksum, "combined checksum must agree");
    }

    /// A single flipped payload byte must change the combined checksum, so the
    /// receiver's recomputed hash no longer matches the one the primary framed.
    #[tokio::test]
    async fn test_checkpoint_checksum_tamper_detected() {
        let mut sender = CheckpointChecksum::new();
        sender.update_file("CURRENT", &calculate_bytes_checksum(b"MANIFEST-000005\n"));
        sender.update_file("000042.sst", &calculate_bytes_checksum(b"payload-bytes"));
        let wire_checksum = sender.finalize();

        // Receiver re-hashes, but one file arrived corrupted.
        let mut receiver = CheckpointChecksum::new();
        receiver.update_file("CURRENT", &calculate_bytes_checksum(b"MANIFEST-000005\n"));
        receiver.update_file("000042.sst", &calculate_bytes_checksum(b"payload-bytesX"));
        assert_ne!(
            receiver.finalize(),
            wire_checksum,
            "a tampered file must break the combined checksum"
        );

        // The filename is part of the coverage too: a renamed file diverges.
        let mut renamed = CheckpointChecksum::new();
        renamed.update_file("CURRENT", &calculate_bytes_checksum(b"MANIFEST-000005\n"));
        renamed.update_file("000043.sst", &calculate_bytes_checksum(b"payload-bytes"));
        assert_ne!(renamed.finalize(), wire_checksum);
    }

    // FM-REPLICATION-035
    /// Golden-bytes assertion: for a fixed input the codec must emit exactly the
    /// bytes today's hand-rolled sender emits (`$FROGDB_CHECKPOINT\r\n`, decimal
    /// count, `$<name_len>\r\n<name>\r\n$<size>\r\n`, `$<meta_len>\r\n<bytes>\r\n`).
    /// A round-trip test proves the two sides agree *with each other*; this
    /// proves they still agree with the *old* wire, catching a reordering that
    /// would otherwise pass by agreeing with itself.
    #[tokio::test]
    async fn test_checkpoint_codec_golden_bytes() {
        let metadata = sample_metadata();
        let meta_str = format!("42:{}:repl-1:99:", hex::encode([0xAB; 32]));

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(b"$FROGDB_CHECKPOINT\r\n");
        expected.extend_from_slice(b"1\r\n");
        expected.extend_from_slice(b"$7\r\nCURRENT\r\n");
        expected.extend_from_slice(b"$1\r\n");
        expected.extend_from_slice(b"x");
        expected.extend_from_slice(format!("${}\r\n", meta_str.len()).as_bytes());
        expected.extend_from_slice(meta_str.as_bytes());
        expected.extend_from_slice(b"\r\n");

        let mut actual: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_prelude(&mut actual, 1)
            .await
            .unwrap();
        CheckpointStreamCodec::write_file_header(
            &mut actual,
            &CheckpointFileHeader {
                name: "CURRENT".to_string(),
                size: 1,
            },
        )
        .await
        .unwrap();
        actual.write_all(b"x").await.unwrap();
        CheckpointStreamCodec::write_metadata(&mut actual, &metadata)
            .await
            .unwrap();

        assert_eq!(actual, expected);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_checkpoint_codec_zero_file_prelude() {
        let mut buf: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_prelude(&mut buf, 0)
            .await
            .unwrap();
        let metadata = sample_metadata();
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let count = CheckpointStreamCodec::read_prelude(&mut cursor)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let decoded = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap();
        assert_eq!(decoded.replication_offset, metadata.replication_offset);
    }

    // FM-REPLICATION-035
    #[test]
    fn test_parse_file_count_rejects_garbage() {
        // Non-numeric.
        let err = CheckpointStreamCodec::parse_file_count("not-a-number\r\n").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Oversized.
        let err = CheckpointStreamCodec::parse_file_count("2000000\r\n").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Valid.
        assert_eq!(CheckpointStreamCodec::parse_file_count("7\r\n").unwrap(), 7);
    }

    /// The bounds exist to refuse a hostile length *before* it drives an
    /// allocation, so where each one falls is the whole contract: one short and
    /// a legitimate checkpoint is refused, one long and the allocation it was
    /// meant to cap goes through. Pinned at the exact value and one past it.
    #[test]
    fn parse_file_count_admits_the_bound_and_refuses_one_past_it() {
        const BOUND: usize = 1_000_000;
        assert_eq!(
            CheckpointStreamCodec::parse_file_count(&format!("{BOUND}\r\n")).unwrap(),
            BOUND,
            "the bound itself is a legal count"
        );
        let err =
            CheckpointStreamCodec::parse_file_count(&format!("{}\r\n", BOUND + 1)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_file_header_admits_a_name_at_the_bound_and_refuses_one_past_it() {
        // Spelled out rather than read off the constant: a test that sizes its
        // name from the bound moves with the bound and can never say where the
        // bound is, which is the only thing worth pinning here.
        const BOUND: usize = 65_536;
        let name = "a".repeat(BOUND);
        let wire = format!("${}\r\n{}\r\n$0\r\n", name.len(), name).into_bytes();
        let mut cursor = std::io::Cursor::new(wire);
        let header = CheckpointStreamCodec::read_file_header(&mut cursor)
            .await
            .expect("a name of exactly the bound is legal");
        assert_eq!(header.name.len(), BOUND);
        assert_eq!(header.size, 0);

        // One past it is refused on the length line alone — before the name
        // buffer is allocated, so nothing here depends on supplying a body.
        let mut cursor = std::io::Cursor::new(format!("${}\r\n", BOUND + 1).into_bytes());
        let err = CheckpointStreamCodec::read_file_header(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_metadata_admits_a_body_far_above_a_short_bound() {
        // A replication id long enough that a bound computed as `64 + 1024`
        // instead of `64 * 1024` would refuse this legitimate frame.
        let metadata = FullSyncMetadata {
            replication_id: "z".repeat(4096),
            ..sample_metadata()
        };
        let mut buf: Vec<u8> = Vec::new();
        CheckpointStreamCodec::write_metadata(&mut buf, &metadata)
            .await
            .unwrap();
        assert!(buf.len() > 4096 && buf.len() <= 65_536);

        let mut cursor = std::io::Cursor::new(buf);
        let got = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .expect("a body under the bound round-trips");
        assert_eq!(got.replication_id, metadata.replication_id);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_prelude_rejects_bad_marker() {
        let mut cursor = std::io::Cursor::new(b"$NOT_A_CHECKPOINT\r\n1\r\n".to_vec());
        let err = CheckpointStreamCodec::read_prelude(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // Missing `$` prefix.
        let mut cursor = std::io::Cursor::new(b"FROGDB_CHECKPOINT\r\n1\r\n".to_vec());
        let err = CheckpointStreamCodec::read_prelude(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_file_header_non_numeric_len() {
        let mut cursor = std::io::Cursor::new(b"$xyz\r\nname\r\n$0\r\n".to_vec());
        let err = CheckpointStreamCodec::read_file_header(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_file_header_oversized_name_len() {
        // Larger than MAX_CHECKPOINT_NAME_LEN — rejected before allocation.
        let mut cursor = std::io::Cursor::new(b"$999999\r\n".to_vec());
        let err = CheckpointStreamCodec::read_file_header(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_file_header_truncated_name() {
        // Declares an 8-byte name but only supplies part of it: the length
        // overruns the buffer, so read_exact must report UnexpectedEof.
        let mut cursor = std::io::Cursor::new(b"$8\r\nabc".to_vec());
        let err = CheckpointStreamCodec::read_file_header(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// Frame one file header with an arbitrary (possibly non-UTF-8) name, the
    /// way a hostile primary could. `write_file_header` cannot express these —
    /// its name is a `String` — so the bytes are laid out by hand.
    fn raw_file_header(name: &[u8], size: u64) -> Vec<u8> {
        let mut buf = format!("${}\r\n", name.len()).into_bytes();
        buf.extend_from_slice(name);
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(format!("${size}\r\n").as_bytes());
        buf
    }

    // FM-REPLICATION-044
    /// Every name shape that is not exactly one normal path component is refused
    /// at the codec boundary, before the receiver ever joins it onto the staging
    /// directory. Each shape is checked on its own so a partial fix cannot pass:
    /// absolute paths and `..` escape the staging dir, nested names put files in
    /// unexpected subtrees, and the empty / normalizing / non-UTF-8 forms alias
    /// two distinct wire names onto one staged path while folding different
    /// bytes into the combined checksum.
    #[tokio::test]
    async fn read_file_header_refuses_names_that_are_not_one_component() {
        let hostile: &[(&str, &[u8])] = &[
            ("absolute", b"/etc/authorized_keys"),
            ("parent traversal", b"../../frogdb.conf"),
            ("embedded traversal", b"a/../../b"),
            ("nested", b"a/b"),
            ("empty", b""),
            ("current dir", b"."),
            ("bare parent", b".."),
            ("trailing slash", b"CURRENT/"),
            ("normalizing suffix", b"CURRENT/."),
            ("invalid utf-8", &[b'C', 0xff, 0xfe, b'T']),
        ];
        for (label, name) in hostile {
            let mut cursor = std::io::Cursor::new(raw_file_header(name, 4));
            let err = match CheckpointStreamCodec::read_file_header(&mut cursor).await {
                Ok(header) => panic!("{label} name must be refused, decoded {header:?}"),
                Err(err) => err,
            };
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidData,
                "{label} name must be InvalidData"
            );
        }

        // The shapes that *are* legal still decode unchanged.
        for name in ["CURRENT", "000042.sst", "MANIFEST-000005", "..sneaky"] {
            let mut cursor = std::io::Cursor::new(raw_file_header(name.as_bytes(), 4));
            let header = CheckpointStreamCodec::read_file_header(&mut cursor)
                .await
                .unwrap_or_else(|e| panic!("{name} must be accepted: {e}"));
            assert_eq!(header.name, name);
            assert_eq!(header.size, 4);
        }
    }

    // FM-REPLICATION-044
    /// The containment rule is `receive_to_file`'s own, not something the caller
    /// has to preserve: handed an escaping name directly it refuses before
    /// creating anything, so no bytes land outside the directory it was given.
    #[tokio::test]
    async fn receive_to_file_refuses_a_name_that_escapes_its_directory() {
        let dir = tempdir().unwrap();
        let incoming = dir.path().join("incoming");
        let outside = dir.path().join("escaped.txt");

        for name in ["../escaped.txt", "/tmp/escaped.txt", "sub/escaped.txt", ""] {
            let mut reader = std::io::Cursor::new(b"payload".to_vec());
            let err = receive_to_file(&mut reader, &incoming, name, 7, None)
                .await
                .expect_err("escaping name must be refused");
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "name {name:?}");
        }

        assert!(!outside.exists(), "no file may land outside the target dir");
        assert!(
            !incoming.exists(),
            "a refused name must not even create the target dir"
        );
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_metadata_wrong_field_count() {
        // Payload with too few `:`-separated fields must be InvalidData.
        let mut cursor = std::io::Cursor::new(b"$3\r\nabc\r\n".to_vec());
        let err = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_metadata_oversized_len() {
        let mut cursor = std::io::Cursor::new(b"$99999999\r\n".to_vec());
        let err = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    // FM-REPLICATION-035
    #[tokio::test]
    async fn test_read_metadata_truncated_body() {
        // Declares a 71-byte metadata frame but supplies far fewer bytes.
        let mut cursor = std::io::Cursor::new(b"$71\r\n1:0:x".to_vec());
        let err = CheckpointStreamCodec::read_metadata(&mut cursor)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    proptest::proptest! {
        // FM-REPLICATION-035
        /// For an arbitrary list of file headers, encoding then decoding the
        /// header sequence yields the same list — pinning the per-file framing
        /// against future format edits (proposal 56's property test).
        #[test]
        fn prop_file_header_sequence_round_trips(
            // Names are single path components by contract, so the strategy
            // generates only those: non-empty, and never leading with `.` so it
            // cannot emit `.` or `..`.
            headers in proptest::collection::vec(
                ("[a-zA-Z0-9_-][a-zA-Z0-9._-]{0,63}", proptest::prelude::any::<u64>()).prop_map(
                    |(name, size)| CheckpointFileHeader { name, size },
                ),
                0..8usize,
            )
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
            rt.block_on(async {
                let mut buf: Vec<u8> = Vec::new();
                for header in &headers {
                    CheckpointStreamCodec::write_file_header(&mut buf, header)
                        .await
                        .unwrap();
                }
                let mut cursor = std::io::Cursor::new(buf);
                for expected in &headers {
                    let got = CheckpointStreamCodec::read_file_header(&mut cursor)
                        .await
                        .unwrap();
                    proptest::prop_assert_eq!(&got, expected);
                }
                Ok(())
            })?;
        }
    }
}
