# Proposal 30 — Extract a CheckpointStager seam: split full-sync transport from stage/verify/commit, and make streaming-before-install an explicit contract

## Summary

`ReplicaConnection::receive_checkpoint`
(`frogdb-server/crates/replication/src/replica/connection.rs:239-323`, 85 lines) is a single async
method that bundles five responsibilities: (a) the transport loop that drives `CheckpointStreamCodec`
over the per-file headers/payloads while folding the combined checksum, (b) the combined-checksum
verify plus scratch-dir cleanup on mismatch, (c) the `StagedCheckpoint` remove-old-then-rename commit,
(d) the `StagedReplicationMetadata` JSON stamp, and (e) the live `ReplicationState` +
`shared_offset` mutation followed by the `ConnectionState::Streaming` transition. The load-bearing
invariant — the checkpoint is *staged*, not installed (install is deferred to the next boot at
`RocksStore::load_staged_checkpoint`), yet the connection immediately proceeds to consume live FrogDB
WAL frames on top of a snapshot that is not yet on disk as the live DB — exists only as a prose
comment (`connection.rs:317-320`). Because all five concerns live inside one method on a struct that
owns a `BoxedStream` and an `Arc<RwLock<ReplicationState>>`, none of the persistence-side steps
(checksum-mismatch cleanup, stale-staged removal, metadata stamp, offset adoption) has a unit test;
their only coverage is a two-server booted integration test.

This proposal extracts a `CheckpointStager` (owning verify → commit → metadata) and a
`CheckpointReceiver` (owning the transport loop), leaves `receive_checkpoint` as a thin driver that
adopts the returned offset and flips to `Streaming`, and turns the streaming-before-install invariant
into an explicit return-type contract rather than a comment.

**Recommendation: proceed with caveats.** The core premise holds, but two candidate claims were
wrong and are corrected below: the seam name `CheckpointInstaller` collides with the codebase's
established "Installer" party, and the staging path does **not** touch `RocksStore` at all.

## Evidence (verified file:line)

All paths are under `frogdb-server/crates/` (the candidate omitted this prefix).

**The five bundled responsibilities in `receive_checkpoint`
(`replication/src/replica/connection.rs:239-323`):**

| # | Responsibility | Lines |
| --- | --- | --- |
| a | Transport loop: per-file `CheckpointStreamCodec::read_file_header` + `receive_to_file`, folding `CheckpointChecksum::update_file`; then `read_metadata` | 250-265 |
| b | Verify `combined.finalize()` vs `metadata.checksum`; on mismatch `remove_dir_all(checkpoint_dir)` + error | 266-274 |
| c | Commit: `StagedCheckpoint::in_parent`, remove pre-existing staged dir, `fs::rename(incoming → staged.dir())` (with rollback cleanup on failure) | 279-292 |
| d | Metadata: build `StagedReplicationMetadata`, `serde_json::to_string`, `fs::write(staged.replication_metadata_path())` | 293-307 |
| e | Adopt offset into live `ReplicationState` + `shared_offset`, then `set_state(Streaming)` | 309-321 |

**Verified supporting facts:**

- **Single call site.** `receive_checkpoint` is called from exactly one place,
  `connect_and_sync` (`replication/src/replica/mod.rs:208`), inside the
  `SyncType::FullSyncCheckpoint` arm. After it returns, the same async block calls
  `conn.stream_replication(&self.frame_tx)` (`mod.rs:212`) — this is where the live WAL frames are
  actually consumed, one layer up from the `set_state(Streaming)` at `connection.rs:321`.
- **The transport primitives are already extracted** (prior work, proposal 56 — the property test at
  `fullsync.rs:844` names it): `CheckpointStreamCodec` owns the framing
  (`read_file_header`/`read_metadata`/`parse_file_count`, `fullsync.rs:233-267`), `receive_to_file`
  owns per-file payload + hash (`fullsync.rs:389-424`), `CheckpointChecksum` owns combined-checksum
  coverage (`fullsync.rs:287-313`). What is *not* extracted is the orchestration that stitches them
  to the staging commit — that is the gap this proposal closes.
- **`StagedCheckpoint` already owns the on-disk staging contract**
  (`persistence/src/rocks/staged.rs:51-91`): `in_parent`, `dir()`, `exists()`,
  `replication_metadata_path()`, and the doc comment that names three parties — **Writer** (the
  replica full-sync code, i.e. `receive_checkpoint`), **Installer**
  (`RocksStore::load_staged_checkpoint`, boot-time, `rocks/checkpoint.rs:28`), **Orchestrator**
  (`server/src/recovery/`). The rename *to* the staged dir is "the writer's commit point"
  (`staged.rs:27`); the rename *from* it is the installer's.
- **`StagedReplicationMetadata`** is defined in `replication/src/state.rs:35-43` and its file name is a
  re-export of `frogdb_persistence::rocks::staged::STAGED_REPLICATION_METADATA_FILE`
  (`state.rs:25-26`) — the staging contract is already shared, not re-literal'd.
- **Streaming-before-install is prose only.** The invariant lives entirely in the comment at
  `connection.rs:317-320` ("The checkpoint itself needs a restart to load, but the connection now
  moves straight into live WAL streaming … so the link is up from here") and the corresponding
  `mod.rs:60-68` doc on `link_up`. Nothing in a type or signature enforces "commit stages but does
  not install; the caller must adopt the offset and then may stream."
- **No unit coverage of (b)–(e).** `replication/src/replica/tests.rs` tests only
  `ConnectionState` display, handler creation, and the stop/reconnect loop — it never drives
  `receive_checkpoint`. The `fullsync.rs` tests cover the codec/checksum in isolation. The only
  end-to-end pin is `test_full_sync_stages_live_primary_offset`
  (`server/tests/integration_replication.rs:4166-4238`), which boots **two** live servers over real
  TCP with real `RocksStore` on both sides to assert the staged metadata offset. The
  **checksum-mismatch cleanup** (`connection.rs:266-274`) and the **stale-staged removal**
  (`connection.rs:280-284`) have **zero** coverage of any kind.

### Corrected candidate claims (see `evidence_discrepancies`)

1. **`receive_checkpoint` does not touch `RocksStore`.** The candidate's test-win note says the code
   "today needs live socket + real RocksStore." The staging path is pure `tokio::fs` plus
   `StagedCheckpoint` path math; the actual database swap is the boot-time **Installer**
   (`RocksStore::load_staged_checkpoint`) in the `persistence` crate, a different party entirely. What
   blocks unit testing is the `ReplicaConnection` struct coupling (a `BoxedStream` +
   `Arc<RwLock<ReplicationState>>` + `link_up` atomic), not `RocksStore`.
2. **Naming collision.** The candidate names the seam `CheckpointInstaller`. "Installer" is already a
   named party in `staged.rs:12` — the boot-time `RocksStore::load_staged_checkpoint`. The replica
   side is the **Writer**. This proposal uses `CheckpointStager` (the commit unit) and
   `CheckpointReceiver` (the transport unit) to stay inside the module's own vocabulary.
3. **`:321 → stream_replication` is imprecise.** Line 321 sets `Streaming`; the live-frame
   consumption happens in the caller `connect_and_sync` (`mod.rs:212`), not at `:321`.

## Proposed design (Rust interface sketch — signatures/types only)

Two seams in the `replication` crate. `CheckpointStager` owns the persistence-side commit and never
sees the socket; `CheckpointReceiver` owns the transport loop and never sees `ReplicationState`.
`receive_checkpoint` shrinks to a driver.

```rust
// replication/src/fullsync/stager.rs  (or replica/checkpoint_stager.rs)

use frogdb_persistence::rocks::staged::StagedCheckpoint;
use crate::fullsync::FullSyncMetadata;
use std::path::{Path, PathBuf};

/// What a committed stage yields the caller: the replication identity + offset
/// the just-staged snapshot corresponds to. Adopting these is the caller's job,
/// deliberately kept *out* of the stager so the mutation of live
/// `ReplicationState` stays on the connection that owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedOutcome {
    pub replication_id: String,
    pub replication_offset: u64,
    // NOTE: no `checksum` field. An earlier draft carried the raw `[u8;32]` "for
    // callers that want it", but the only caller (the connection driver) adopts
    // only `replication_id` + `replication_offset`, so a checksum field would be
    // dead on arrival and lint-flagged. Add it back only when a real consumer
    // appears; the hex encoding still happens inside `commit` for the on-disk
    // metadata (see the checksum-type risk below).
}

/// Owns verify -> commit -> metadata for one full-sync checkpoint, against the
/// parent of the live db dir. Pure filesystem + `StagedCheckpoint`; touches no
/// socket and no `RocksStore` (the DB swap is the boot-time Installer,
/// `RocksStore::load_staged_checkpoint`).
///
/// Contract: `commit` *stages* the checkpoint — it does NOT install it. The
/// returned `StagedOutcome` is the caller's licence to adopt the offset and then
/// proceed to live streaming; the on-disk DB is unchanged until the next boot.
/// This is the streaming-before-install invariant, now expressed as a type.
pub struct CheckpointStager {
    parent_dir: PathBuf,
}

impl CheckpointStager {
    pub fn new(parent_dir: &Path) -> Self;

    /// Scratch dir (`checkpoint_incoming`) that received files land in before the
    /// commit rename. The `CheckpointReceiver` writes here.
    pub fn incoming_dir(&self) -> PathBuf;

    /// Verify the combined checksum against `meta`, remove any stale staged dir,
    /// rename `incoming` onto the staged dir (the writer's commit point), and
    /// stamp `replication_metadata.json`.
    ///
    /// Failure asymmetry must be reproduced *exactly* — the original method
    /// (`connection.rs`) treats only two steps as fatal and two as non-fatal:
    /// - **Fatal (return `Err`):** checksum mismatch → remove `incoming`, return
    ///   `InvalidData` (`:266-274`); rename failure → remove `incoming`, return
    ///   `io::Error::other` (`:285-292`).
    /// - **Non-fatal (`warn!` and continue, still returning `Ok`):** failure to
    ///   remove a stale staged dir before the rename (`:280-284`), and any
    ///   serialize/write failure stamping `replication_metadata.json`
    ///   (`:298-307`). These MUST NOT be promoted to `Err` — the offset is still
    ///   adopted and the stage is still considered successful. The new unit tests
    ///   pin this (a metadata-write failure returns `Ok(StagedOutcome)`).
    pub async fn commit(
        &self,
        incoming: PathBuf,
        computed: [u8; 32],
        meta: &FullSyncMetadata,
    ) -> io::Result<StagedOutcome>;
}
```

```rust
// replication/src/fullsync/receiver.rs  (or a free fn in fullsync)

use tokio::io::AsyncBufRead;

/// Drives `CheckpointStreamCodec` over `file_count` framed files into `incoming`,
/// folding each into a `CheckpointChecksum`, then reads the trailing metadata.
/// Returns the metadata plus the receiver's *computed* combined checksum so the
/// stager can verify without re-reading the files. Socket-agnostic: any
/// `AsyncBufRead` (the live `BufReader<&mut BoxedStream>` in production, an
/// in-memory duplex or `Cursor` in tests).
///
/// Creates `incoming` unconditionally up front (`fs::create_dir_all`) — this
/// replaces the current `connection.rs:248` unconditional create. It is load-
/// bearing for `file_count == 0`: `receive_to_file` self-creates only the parent
/// of each file it writes (`fullsync.rs:395`), so with zero files no dir would
/// otherwise exist and the stager's `fs::rename(incoming -> staged.dir())` would
/// fail. Do NOT rely on per-file parent creation to make the scratch dir.
pub async fn receive_checkpoint_files<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    incoming: &Path,
    file_count: usize,
) -> io::Result<(FullSyncMetadata, [u8; 32])>;
```

`ReplicaConnection::receive_checkpoint` becomes the driver that keeps only what belongs to the
connection — the transport source and the live-state adoption + `Streaming` transition:

```rust
pub(crate) async fn receive_checkpoint(&mut self, file_count: usize) -> io::Result<()> {
    let parent = self.data_dir.parent().ok_or_else(/* ... */)?;
    let stager = CheckpointStager::new(parent);
    let incoming = stager.incoming_dir();

    let mut reader = BufReader::new(&mut self.stream);
    let (meta, computed) =
        receive_checkpoint_files(&mut reader, &incoming, file_count).await?;

    let outcome = stager.commit(incoming, computed, &meta).await?;

    // Adopt the staged offset into live state — the one step that must stay on
    // the connection, because it mutates `ReplicationState` + `shared_offset`.
    {
        let mut state = self.state.write().await;
        state.replication_id = outcome.replication_id.clone();
        state.replication_offset = outcome.replication_offset;
    }
    if let Some(ref shared) = self.shared_offset {
        shared.store(outcome.replication_offset, Ordering::Release);
    }
    // Staged, not installed: the connection now proceeds to live WAL streaming
    // (see `connect_and_sync`), so the link is up from here.
    self.set_state(ConnectionState::Streaming);
    Ok(())
}
```

The `StagedOutcome`-returning `commit` is the type-level expression of the invariant: you cannot get
an offset to adopt without having *staged* (not installed), and the doc contract states the caller
then streams. The "prose-only" invariant now has a signature carrying it.

## Migration plan (ordered steps)

1. **Add `CheckpointReceiver`** (`receive_checkpoint_files`) as a free function or small struct in
   `fullsync` (or `replica`), moving lines `connection.rs:250-265` verbatim behind the generic
   `AsyncBufRead` bound. **Also move the unconditional `fs::create_dir_all(&checkpoint_dir)` at
   `connection.rs:248` into the receiver** (as the first thing it does) — it is currently the *only*
   thing that guarantees the scratch dir exists, and with `file_count == 0` no per-file parent
   creation would ever run, so dropping it would make the stager's rename fail. No behavior change;
   `BufReader<&mut BoxedStream>` still satisfies the bound.
2. **Add `CheckpointStager`** with `new`/`incoming_dir`/`commit`, moving the verify (266-274), commit
   (279-292), and metadata (293-307) blocks into `commit`. Preserve exact error kinds and the
   remove-on-mismatch / remove-on-rename-failure cleanup semantics — **and preserve the two non-fatal
   `warn!` branches** (stale-staged removal failure at `:280-284`, metadata serialize/write failure at
   `:298-307`): these log and continue, returning `Ok`. Do not accidentally propagate them as errors
   when hoisting the blocks behind `?`.
3. **Rewrite `receive_checkpoint`** as the driver above; it retains only the parent-dir resolution,
   the reader construction, offset adoption (309-316), and `set_state(Streaming)` (321).
4. **Add the unit tests** (see Test plan) against a tempdir + in-memory duplex — these are new
   coverage, so land them in the same change to prove the extraction is faithful.
5. **Run the existing integration pin** (`test_full_sync_stages_live_primary_offset`) plus the
   boot-time `test_replica_recovers_offset_from_staged_metadata` to confirm end-to-end parity.
6. **Optional follow-up:** the primary sender `ReplicaSession::stream_checkpoint`
   (`replica_session.rs:498-570`) is the symmetric encoder. It is already thin (delegates to the
   codec/checksum), so no change is required, but a doc cross-link from `CheckpointStager` to it keeps
   the send/receive symmetry discoverable. Do not fold them together — they live on opposite sides of
   the wire.

No crate-dependency change: `replication` already depends on `frogdb-persistence`
(`replication/Cargo.toml`), and `connection.rs:279` already references
`frogdb_persistence::rocks::staged::StagedCheckpoint`. The new seams depend on nothing `receive_checkpoint`
did not already depend on, and touch no `core` internals.

## Test plan

New, socket-free unit tests the current shape cannot express (all against `tempfile::tempdir` +
`tokio::io::duplex`/`Cursor`):

- **`stager_commit_stages_and_stamps_metadata`** — pre-populate an `incoming` dir, feed a matching
  `FullSyncMetadata`, assert the staged dir exists, `CURRENT`-style contents moved, and
  `replication_metadata.json` carries the right replid/offset. (Covers d + the happy path of c.)
- **`stager_commit_checksum_mismatch_cleans_up`** — feed a `computed` that differs from `meta.checksum`;
  assert `InvalidData`, and that the `incoming` scratch dir was removed and no staged dir was created.
  **This path has zero coverage today.**
- **`stager_commit_removes_stale_staged`** — with a pre-existing staged dir present, assert `commit`
  removes it before the rename and the new content wins. **Zero coverage today.**
- **`stager_commit_metadata_write_failure_is_non_fatal`** — force the `replication_metadata.json`
  write to fail (e.g. pre-create the metadata path as a directory so `fs::write` errors), assert
  `commit` still returns `Ok(StagedOutcome)` with the right replid/offset and the staged dir is intact.
  Pins the deliberate non-fatal asymmetry (`connection.rs:298-307`) so a refactor that hoists the
  block behind `?` is caught. **Zero coverage today.**
- **`receiver_reads_framed_files_and_metadata`** — drive `receive_checkpoint_files` over a duplex
  written by `CheckpointStreamCodec` (reuse the `fullsync.rs` round-trip fixtures), assert the returned
  metadata and that the computed checksum equals the folded value; a truncated stream yields
  `UnexpectedEof`.
- **`receive_checkpoint_adopts_offset_and_streams`** — a thin integration-style test on
  `ReplicaConnection` with an in-memory stream, asserting `ReplicationState.replication_offset` and
  `link_up` are set after a good checkpoint (the one behavior that must stay on the connection).

Retain unchanged: `test_full_sync_stages_live_primary_offset` and
`test_replica_recovers_offset_from_staged_metadata` (the end-to-end pins), and all `fullsync.rs`
codec/checksum tests.

## Risks & alternatives

- **Error-semantics parity is the whole point — guard it.** The candidate value is that mismatch/stale
  cleanup becomes testable; the migration must preserve the exact `io::ErrorKind` values and the
  "remove scratch dir on any failure" discipline (`connection.rs:269, 287`). Land the mismatch/stale
  unit tests *first* so a regression is caught synchronously rather than only in a two-server boot.
- **`FullSyncMetadata.checksum` is `[u8;32]`; `StagedReplicationMetadata.checksum` is
  `Option<String>` (hex).** The existing code hex-encodes at the boundary (`connection.rs:296`). Keep
  that conversion inside `commit` so the wire type and the on-disk type stay decoupled exactly as
  today. `StagedOutcome` deliberately does **not** re-expose the checksum: the driver adopts only the
  replid + offset, so a raw-`[u8;32]` field would be dead on arrival (see the type comment above). If
  a future caller genuinely needs it, add the field then rather than shipping an unused one now.
- **`AsyncBufRead` bound vs today's `BufReader<&mut self.stream>`.** Verified the current code already
  wraps the stream in a `BufReader` (`connection.rs:250`) and the codec's read methods are already
  `AsyncBufRead`-generic (`fullsync.rs:233, 255`), so the bound change is free.
- **Alternative: one combined `CheckpointInstaller` doing loop+commit.** Rejected — it would re-fuse
  transport and persistence (the two things this splits) and re-collide with the "Installer" party
  name. The two-seam split matches the module's existing decomposition (codec = framing, helpers =
  payload, stager = commit) and keeps each unit testable in isolation.
- **Alternative: leave it as prose, add only tests.** You cannot unit-test (b)–(e) without a
  `ReplicaConnection`, which needs a `BoxedStream` and a shared `ReplicationState` — so "just add
  tests" forces either a mock connection or the very extraction proposed here. The extraction is the
  cheaper path to the coverage.
- **ADR interaction: none.** This is a data-path (full-sync) refactor; it does not touch the Raft
  Metadata Plane. ADR-0001 (`frogdb-server/docs/adr/0001-raft-cluster-metadata.md`, "the data path
  never goes through Raft") is respected — checkpoint transfer is replication data, not metadata
  consensus. ADR-0002 (single database) is likewise untouched.

## Effort

**M.** One crate (`replication`), no cross-crate signature churn, compiler-guided (the single call site
at `mod.rs:208` is unaffected — `receive_checkpoint`'s signature is unchanged). The bulk is mechanical
block-moves into two new units plus five new unit tests; the care is in preserving the exact cleanup
error semantics, which the new tests pin. Not S because it introduces two new types + a generic bound
and must prove parity against the only existing (booted) pin; not L because there is no wire-format,
dependency, or cross-crate change and the primary sender is left alone.

## Related

- **`CheckpointStreamCodec` (P56)** — the prior extraction that pulled the on-wire framing grammar out
  of both the hand-rolled sender and receiver (`fullsync.rs:140-268`; property test at
  `fullsync.rs:844`). This proposal is the natural next layer: P56 owns *the bytes*, this owns *the
  orchestration + commit* that P56 explicitly left in the caller ("The combined checksum policy and
  the `StagedCheckpoint` landing contract stay in the caller by design," `fullsync.rs:133-134`).
- **Proposal 06 / 12 (snapshot scheduler + coordinator surface)** — sibling pattern of pushing a
  concern down into a pure, unit-testable module and trimming the shallow surface left above it.
- **`StagedCheckpoint` (`persistence/src/rocks/staged.rs`)** — the shared Writer/Installer/Orchestrator
  contract this seam plugs the replica-side Writer into.

## Adversarial review

**Verdict: CONFIRMED.** The reviewer opened every cited `file:line` and confirmed the core premise
(five bundled concerns in `receive_checkpoint`, prose-only streaming-before-install invariant, zero
unit coverage of the persistence-side steps) against `connection.rs:239-323`, `replica/tests.rs`, and
`integration_replication.rs`. Both self-corrections were verified accurate (the "Installer" name is
taken by the boot-time party at `staged.rs:12`; `receive_checkpoint` touches no `RocksStore`). The
design was judged feasible — the `AsyncBufRead + Unpin` bound satisfies both the codec's read methods
and `receive_to_file`, no borrow-checker conflict, no crate-dependency change — and correctly scoped
away from the Raft metadata plane. Three **minor** precision/migration-fidelity issues were raised;
none required a design change. All three are resolved:

1. **Dropped `create_dir_all` in the migration (minor).** The original unconditionally creates the
   scratch dir at `connection.rs:248`; `receive_to_file` only self-creates each file's *parent*
   (`fullsync.rs:395`), so with `file_count == 0` no dir would exist and the stager's
   `fs::rename(incoming -> staged.dir())` would fail. **Resolved:** the receiver now documents that it
   creates `incoming` unconditionally as its first action (replacing `:248`), and migration step 1
   explicitly calls out moving that `create_dir_all` into the receiver and why `file_count == 0`
   makes it load-bearing.

2. **`commit` must reproduce the fatal/non-fatal asymmetry (minor).** Two moved steps — stale-staged
   removal failure (`:280-284`) and metadata serialize/write failure (`:298-307`) — are deliberate
   non-fatal `warn!`s that still return success and adopt the offset; only checksum mismatch and
   rename failure are fatal. The original sketch prose did not spell this out. **Resolved:** the
   `commit` doc comment now enumerates the fatal vs non-fatal branches explicitly, migration step 2
   warns against propagating the non-fatal branches behind `?`, and a new test
   (`stager_commit_metadata_write_failure_is_non_fatal`) pins that a metadata-write failure returns
   `Ok(StagedOutcome)`.

3. **Self-acknowledged dead `StagedOutcome.checksum` field (minor).** No consumer exists; the driver
   adopts only replid + offset. **Resolved:** the field is removed from the sketch with a comment
   explaining why, and the checksum-type risk bullet updated to match (hex encoding still happens
   inside `commit` for the on-disk metadata; the field can be re-added if a real consumer appears).

No claim was disputed; all cited evidence was re-read during this revision and matches
(`connection.rs:248, 266-274, 280-284, 285-292, 298-307`).
