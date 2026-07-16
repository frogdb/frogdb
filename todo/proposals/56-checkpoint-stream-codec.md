# Proposal: Checkpoint Stream Codec

Status: proposed
Date: 2026-07-16

## Problem

Full-sync checkpoint transfer has one wire format — `$FROGDB_CHECKPOINT`, a file count, a
per-file `$len\r\nname\r\n$size\r\n<bytes>` run, a trailing metadata frame, and a combined
SHA256 — but that format has no module. It exists only as prose comments and as three separate
hand-rolled byte sequences that must agree byte-for-byte, spread across the send path and the
receive path. The metadata *frame* (`FullSyncMetadata`) and the raw byte-copy helpers already got
factored into a shared module (`fullsync.rs`); the **envelope around them did not**, so the highest
-risk part of the format — where files begin and end — is still mirrored by hand.

| Symptom | Where |
|---------|-------|
| Send: envelope hand-written — marker, count, per-file `$len`/`$size` headers, metadata length-prefix | `replication/src/replica_session.rs:508-554` (`stream_checkpoint`, decl `:471`) |
| Receive-detect: marker + file-count re-parsed in the PSYNC reply reader | `replication/src/replica/connection.rs:161-183` (`FullSyncCheckpoint { file_count }`) |
| Receive-body: per-file `$len`/`name`/`$size`, metadata length-prefix, checksum re-parsed | `replication/src/replica/connection.rs:240-278` (`receive_checkpoint`, decl `:227`) |
| Minimal-RDB fallback path (empty db / checkpoint-cut failure) | `replica_session.rs:591` (`send_minimal_rdb`), `:804` (`create_minimal_rdb`) |
| The metadata frame — the *one* piece that is already a shared type | `replication/src/fullsync.rs:31-91` (`FullSyncMetadata::to_bytes` / `from_bytes`) |
| Contrast — the WAL live stream *does* have a symmetric codec with round-trip tests | `frame.rs:132-268` (`ReplicationFrame` + `encode`/`decode`), codec `:273-392`, tests `:428+` |

Three consequences, in increasing severity:

1. **The format is an interface with no module behind it.** The envelope grammar lives in a doc
   comment (`replica_session.rs:464-470`) and is realized three times: once as writes in
   `stream_checkpoint`, once as the marker/count branch in the PSYNC reply reader, and once as the
   file loop in `receive_checkpoint`. Changing the format — adding a per-file checksum, a format
   version, a compression flag — means editing three call sites in lockstep with nothing to catch a
   drift. The WAL stream, the *other* half of replication, does not work this way: `ReplicationFrame`
   owns `encode`/`decode` as an inverse pair and `ReplicationFrameCodec` implements tokio's
   `Encoder`/`Decoder`, pinned by round-trip tests (`frame.rs:428+`). The checkpoint stream is the
   only replication wire format still defined by prose.

2. **The receive side has zero unit tests.** `receive_checkpoint` (`connection.rs:227-330`) parses
   attacker-adjacent, length-prefixed input straight from a socket — the classic place for an
   off-by-one or a trusting `parse()` — yet it has no unit coverage: `connection.rs` has no `mod
   tests` at all, and the only exercise is the two-server end-to-end suite
   (`server/tests/integration_replication.rs`, ~38 checkpoint references). A corrupt-length or
   truncated-stream case can only be provoked by standing up two servers and cannot be provoked at
   all for the paths a healthy primary never emits. This is the highest-risk untested surface in
   replication, and it is untested *because* the parser is welded to a live `BufReader<TcpStream>`
   with no seam a test can feed bytes to.

3. **Duplicated "package a rocks checkpoint" knowledge — but only partly.** Two sites wrap
   `rocks.create_checkpoint` and then build their own metadata: `SnapshotStager` (proposal 25,
   `persistence/src/snapshot/stager.rs:105-143`) produces an on-disk `snapshot_NNNNN/` with a
   `SnapshotMetadataFile`, while the replica session cuts `fullsync_<id>/` and streams it with a
   `FullSyncMetadata`. The shared kernel — *cut a checkpoint at a sequence* — is already shared
   (both call `create_checkpoint`). What is **not** shared, and is the subject of this proposal, is
   the *wire envelope*. The landing contract on the receive side **is** correctly shared already —
   `StagedCheckpoint` / `STAGED_REPLICATION_METADATA_FILE` (`persistence/src/rocks/staged.rs:33-89`)
   is the one seam both the boot installer and `receive_checkpoint` land through, and this proposal
   keeps it untouched.

**Correcting the intake framing.** Two claims in the brief drifted from the tree and the design
below reflects the corrected picture: (a) the two hand-rolled sites are in **one** crate
(`replication`), not two, and they are **not** "no shared type" — `FullSyncMetadata` and the
byte-copy helpers already live in `fullsync.rs`; the gap is specifically the *envelope*. (b) The
envelope is realized in **three** places, not two, because the receiver splits marker/count
detection (`connection.rs:161-183`) from body parsing (`receive_checkpoint`).

## Design

Promote the envelope into a symmetric codec that lives beside the metadata frame it already wraps,
in `fullsync.rs` — the module that exists precisely to hold "full-sync protocol primitives used by
both the primary and the replica" (`fullsync.rs:1-12`). This is the same shape `frame.rs` gives the
WAL stream: one type owns `encode`/`decode` as inverses, and both sides call it instead of
open-coding bytes.

### The seam — `CheckpointStreamCodec` in `fullsync.rs`

```rust
/// One checkpoint file on the wire: `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`.
pub struct CheckpointFileHeader {
    pub name: String,
    pub size: u64,
}

/// The full-sync checkpoint envelope, as a symmetric codec. This is the single
/// definition of the on-wire grammar the doc comment in `stream_checkpoint`
/// used to describe in prose; the sender and both receiver stages call it
/// instead of re-encoding the bytes three times.
///
/// Framing only — the *bytes* of each file still flow through the existing
/// `stream_file_to_writer` / `receive_to_file` helpers (which already compute
/// the per-file SHA256). The codec owns the delimiters; the helpers own the payload.
pub struct CheckpointStreamCodec;

impl CheckpointStreamCodec {
    /// `$FROGDB_CHECKPOINT\r\n<count>\r\n`. Written once, ahead of the file bodies.
    pub async fn write_prelude<W: AsyncWriteExt + Unpin>(w: &mut W, file_count: usize)
        -> io::Result<()>;

    /// Per-file header: `$<name_len>\r\n<name>\r\n$<size>\r\n`. The caller then
    /// streams `size` payload bytes via `stream_file_to_writer`.
    pub async fn write_file_header<W: AsyncWriteExt + Unpin>(w: &mut W, h: &CheckpointFileHeader)
        -> io::Result<()>;

    /// Trailing metadata frame: `$<len>\r\n<FullSyncMetadata bytes>\r\n`.
    pub async fn write_metadata<W: AsyncWriteExt + Unpin>(w: &mut W, m: &FullSyncMetadata)
        -> io::Result<()>;

    // --- inverses ---

    /// Read one `$<name_len>\r\n<name>\r\n$<size>\r\n` header. The caller then
    /// consumes `size` payload bytes via `receive_to_file`.
    pub async fn read_file_header<R: AsyncBufRead + Unpin>(r: &mut R)
        -> io::Result<CheckpointFileHeader>;

    /// Read the trailing `$<len>\r\n<bytes>\r\n` and parse a `FullSyncMetadata`.
    pub async fn read_metadata<R: AsyncBufRead + Unpin>(r: &mut R)
        -> io::Result<FullSyncMetadata>;
}
```

The prelude's marker+count is *detected* one layer up, in the PSYNC reply reader
(`connection.rs:161-183`), because that reader must first distinguish `$FROGDB_CHECKPOINT` from a
plain `$<rdb_size>` RDB. That branch keeps its `is_checkpoint` decision but delegates the count read
to `CheckpointStreamCodec`; it returns `SyncType::FullSyncCheckpoint { file_count }` exactly as
today, so the dispatch in `replica/mod.rs:142` is unchanged.

### Both sides become calls, not byte-writes

`stream_checkpoint` (`replica_session.rs:508-554`) collapses from three hand-written `write_all`
blocks to:

```rust
CheckpointStreamCodec::write_prelude(stream, files.len()).await?;
for (name, size, path) in &files {
    CheckpointStreamCodec::write_file_header(stream, &CheckpointFileHeader { name: name.clone(), size: *size }).await?;
    stream_file_to_writer(path, stream, Some(&self.sync_bytes_transferred)).await?; // unchanged
}
CheckpointStreamCodec::write_metadata(stream, &metadata).await?; // metadata built as today
```

`receive_checkpoint` (`connection.rs:240-278`) collapses its file loop and metadata parse to the
inverse calls, keeping everything downstream of the parse — the combined-checksum verification
(`connection.rs:273-286`) and the `StagedCheckpoint` landing (`connection.rs:288-320`) — byte-for-byte
identical. The codec replaces only the *parsing*; the checksum policy and the staged-landing
contract stay exactly where they are.

### Where the codec lives, and why not persistence

The codec goes in `replication`, not `persistence`, and the dependency direction settles it:
`replication` depends on `frogdb-persistence` (`replication/Cargo.toml:16`) and **not** the reverse
(`persistence/Cargo.toml` names no replication dep). The envelope is a PSYNC wire concern — where a
file starts and stops on a socket — which is replication's domain; `persistence` is the durability
layer and has no business owning a transfer grammar. Placing the codec in `fullsync.rs` (a) keeps it
next to `FullSyncMetadata`, its own trailing frame, (b) mirrors `frame.rs`'s ownership of the WAL
codec one directory over, and (c) leaves `persistence` free of a dependency it should never grow.
The `create_checkpoint` call and the `StagedCheckpoint` landing already provide the correct
persistence seams on either end; the codec fills the gap *between* them without moving either.

### Deliberately **not** merged with `SnapshotStager`

Proposal 25's `SnapshotStager` and this codec share the sentence "package a rocks checkpoint," but
inspection shows the overlap is one already-shared call (`create_checkpoint`) and nothing more. The
two metadata types are not the same knowledge wearing two hats: `SnapshotMetadataFile` is a
durability descriptor (epoch, sequence, shard count, size, `is_complete`) that lives on disk to be
read at boot; `FullSyncMetadata` is a transfer descriptor (payload size, checksum, replication id +
offset) that lives on the wire to validate a stream. Fusing them would couple the snapshot on-disk
format to the PSYNC wire format — two things that must be free to change independently. So this
proposal **deepens `fullsync.rs`** and **leaves `stager.rs` alone**: no fork, and no forced merge.
The one thing worth folding in later (out of scope here) is that both call `create_checkpoint`
inside a `spawn_blocking` and could share a tiny "cut a checkpoint at seq N into dir D" helper — but
that is a persistence-side refactor for proposal 25 to own, not a wire concern.

## Why this is the right depth

- **Locality.** The envelope grammar becomes one type with one pair of inverse operations, beside
  the metadata frame it wraps. "What does the checkpoint stream look like?" is answered by reading
  `CheckpointStreamCodec`, not by cross-referencing a doc comment against three call sites in two
  files. A format change (per-file checksum, version byte, compression flag) is an edit to the codec
  plus its round-trip test — the send and receive sites inherit it because they no longer spell the
  bytes themselves.
- **Leverage.** The codec is the seam that makes the untested receive path testable *without a
  socket*: round-trip and corrupt-input tests feed `&[u8]` buffers through `read_file_header` /
  `read_metadata`, exactly as `frame.rs` tests feed `BytesMut` through `ReplicationFrameCodec`. The
  whole class of "the two sides drifted" bug is replaced by one property test asserting
  `decode(encode(x)) == x`; the whole class of "malformed length wedges the parser" bug becomes a
  table of corrupt-input cases the integration suite could never enumerate.
- **Deletion test.** Delete `CheckpointStreamCodec` and the envelope grammar scatters back into
  three hand-rolled byte sequences across `stream_checkpoint` and the two receive stages — precisely
  today's shape. That it *cannot* be deleted without regressing the structure is the signal the seam
  belongs where this puts it. Conversely, the codec adds no layer the WAL stream doesn't already
  have: it is the checkpoint stream's overdue `frame.rs`.

## Testing impact

The receive side gains the unit coverage it has never had, and both sides gain a drift guard:

- **Round-trip (the drift guard).** For a synthetic file list + `FullSyncMetadata`: write the full
  envelope into an in-memory buffer via the `write_*` methods, then read it back via the `read_*`
  methods and assert every field (names, sizes, metadata) survives. This is the checkpoint-stream
  analogue of `frame.rs`'s `test_frame_shard_id_round_trips`.
- **Corrupt / hostile input (new, impossible before).** Feed `read_file_header` / `read_metadata`
  buffers with a non-numeric `$len`, a length that overruns the buffer, a truncated name, a metadata
  frame with the wrong field count, and a zero-file prelude — each must return a clean
  `InvalidData`/`UnexpectedEof` error, never panic or hang. These are the paths a healthy primary
  never emits, so the integration suite cannot reach them.
- **Property test on the file loop.** For an arbitrary `Vec<CheckpointFileHeader>` (proptest),
  encoding then decoding the header sequence yields the same list — pinning the per-file framing
  against future format edits.
- **Metadata parity retained.** `fullsync.rs`'s existing `test_metadata_serialization`
  (`fullsync.rs:196`) stays; the codec's metadata methods wrap `FullSyncMetadata::to_bytes` /
  `from_bytes` rather than re-implementing them.
- **Integration unchanged.** `server/tests/integration_replication.rs` full-sync scenarios must pass
  untouched — the wire bytes are identical by construction, which is the point of extracting the
  codec rather than redesigning the format.

## Risks / open questions

- **Byte-for-byte compatibility is the whole contract.** The extraction must emit and accept exactly
  today's bytes (`$FROGDB_CHECKPOINT\r\n`, decimal count, `$<name_len>\r\n<name>\r\n$<size>\r\n`,
  `$<meta_len>\r\n<bytes>\r\n`). A round-trip test proves send/receive agree with *each other* but
  not with the *old* bytes; add one golden-bytes assertion (a checked-in expected buffer for a fixed
  input) so a subtle reordering can't pass by agreeing with itself. Because FrogDB is pre-production
  a primary and replica always run matched builds, so no cross-version wire skew is in play — but
  the golden test is cheap insurance.
- **`AsyncBufRead` bound vs. the live `BufReader<&mut TcpStream>`.** `receive_checkpoint` currently
  reads through a `BufReader` wrapping the stream; the codec's `read_*` methods take
  `R: AsyncBufRead + Unpin` so the same reader passes straight through. The marker/count detection in
  the PSYNC reply reader uses `read_resp_line` on the raw stream *before* the `BufReader` is built —
  keep that split (detection upstream, body parse in the codec) rather than forcing one reader type
  across both, which would entangle the RDB-vs-checkpoint decision with the envelope.
- **Should the codec own the combined checksum too?** Today the SHA256-of-(name,filehash) pairs is
  computed inline on both sides (`replica_session.rs:532-541`, `connection.rs:273-278`). It is a
  natural codec responsibility, but it depends on per-file hashes produced *during* streaming, not
  purely on the envelope. Leaving checksum verification in the caller (this proposal's choice) keeps
  the codec a pure framing seam; folding it in later is a clean follow-up once the framing lands.
- **Minimal-RDB fallback is a different format and stays out.** `send_minimal_rdb` /
  `create_minimal_rdb` (`replica_session.rs:591`, `:804`) emit a plain `$<len>\r\n<RDB>` — the
  RDB-not-checkpoint branch the receiver already distinguishes at `connection.rs:178-182`. The codec
  covers only the `$FROGDB_CHECKPOINT` envelope; the RDB path is untouched.
- **Diskless-transfer grounding (informational).** Redis's diskless replication
  (`repl-diskless-sync`) cannot length-prefix a forked RDB it streams without buffering, so it uses
  an EOF-delimited transfer: `$EOF:<40-byte random delimiter>\r\n`, the RDB bytes, then the
  delimiter repeated (`readSyncBulkPayload` in `replication.c`). DragonflyDB full sync is
  flow-based — each shard streams its own journal-serialized snapshot over a separate `DFLY FLOW`
  socket, sharing one serializer between save and load. FrogDB's format is neither: it is a
  length-prefixed multi-file checkpoint (RocksDB SST set) with a trailing metadata+checksum frame.
  The relevant lesson from both is architectural, not on-the-wire: each keeps a *single* serializer
  shared by both directions — which is exactly the gap `CheckpointStreamCodec` closes.
