# Proposal 53 — `FullSyncEmitter`: one emit envelope for both full-sync payload kinds

Covers exploration-lane candidate **RC1** (`stream_checkpoint` / `stream_live_dataset` are
structural twins). The candidate as filed said the two functions duplicate "the same
progress/checksum/accounting envelope around different payload sources". Verified against
`f31e5911`: **the claim holds, but is narrower and sharper than filed** in one important way
and *wider* in another, and both corrections are load-bearing for how the extraction is
scoped.

- **Narrower.** The *wire grammar* is already centralized — `CheckpointStreamCodec`
  (`fullsync.rs:161-318`) owns the marker/count prelude, the per-file header and the trailing
  metadata frame, and both twins call it. Round 10's proposal 30 deepened the receive side
  (`CheckpointStager` + socket-free receiver) and this codec came with it. So this proposal
  is **not** "extract the wire format" — that is done, and it must not be re-litigated.
  What is duplicated is the layer *above* the codec: the emit envelope that drives it.
- **Wider.** That emit envelope is realized **five** times in the tree, not two: twice in
  production (`replica_session.rs:888-988`, `:1018-1104`) and three more times as test
  fixtures on the *receive* side — `encode_checkpoint_body` (`replica/connection.rs:852-882`),
  `encode_dataset_body` (`replica/connection.rs:1076-1119`) and `encode_envelope`
  (`fullsync/receiver.rs:74-101`) — two of which carry a comment that says out loud what the
  missing module costs: *"folding the combined checksum **the sender way**"*. There is no
  sender to reuse, so every test that needs sender bytes writes another sender.

  `encode_dataset_body` is the sharpest instance, and it is the one the first draft of this
  proposal missed. It does not merely re-fold a checksum: it re-derives the live path's
  **naming and ordering rule**, `format!("shard-{i}.dataset")` over `blobs.iter().enumerate()`
  (`connection.rs:1082-1086`), which is character-for-character `stream_live_dataset:1056-1057`.
  That rule is exactly what this proposal claims `PayloadSource` should own, and it is already
  written twice on opposite sides of the wire with nothing binding them.

**This proposal claims no live bug.** Two latent fragilities and one stale doc-comment are
documented below with the reason each is unreachable today; none of them justifies a hotfix
ahead of the refactor, and the proposal is honest that its case is consolidation plus test
surface, which is the only case a LOCKED area should accept.

`frogdb-replication` is **LOCKED** (gate 0.85, ADR 0003). Wire bytes, checksum bytes,
`record_output` accounting and write granularity must all be preserved exactly — hard
acceptance criteria below, with a golden pin that lands **before** the extraction so the
extraction commit shows it passing unchanged.

## Files involved (verified paths + line counts at `f31e5911`)

All paths under `frogdb-server/crates/` unless noted.

| File | Lines | Role |
| --- | --- | --- |
| `replication/src/replica_session.rs` | 4574 | `handle_full` (`:744-881`) — offset capture, `+FULLRESYNC`, branch on `rocks_store`, checkpoint cut, phase/clock bookkeeping; `stream_checkpoint` (`:888-988`) — emitter #1; `progress_percent` (`:991-998`); `stream_live_dataset` (`:1018-1104`) — emitter #2; `transfer_rate_mbps` (`:1429`) |
| `replication/src/fullsync.rs` | 1164 | The shared, already-deep layer this proposal builds on and **does not change**: `FullSyncMetadata` (`:47-106`), `CHECKPOINT_MARKER`/`SNAPSHOT_MARKER` (`:110`/`:122`), `CheckpointStreamCodec` (`:161-318`), `CheckpointChecksum` (`:338-363`), `calculate_file_checksum` (`:409`), `calculate_bytes_checksum` (`:427`), `stream_file_to_writer` (`:444-464`), `receive_to_file` (`:475-510`); golden test `test_checkpoint_codec_golden_bytes` (`:831-864`) |
| `replication/src/fullsync/receiver.rs` | 219 | `receive_checkpoint_files` (`:38-63`) — the *land*-side twin of emitter #1, already extracted (proposal 30); test fixture `encode_envelope` (`:74-101`) — hand-rolled emitter #3 |
| `replication/src/replica/connection.rs` | 1884 | `receive_snapshot` (`:368-419`) and `receive_checkpoint` (`:452-498`) — **proposal 55's territory**; test fixtures `encode_checkpoint_body` (`:852-882`) and `encode_dataset_body` (`:1076-1119`) — hand-rolled emitters #4 and #5, called from `checkpoint_fixture_with_tail` (`:922`), `dataset_fixture_with_tail` (`:1142`), `:1527` and `:1761` |
| `replication/src/primary/mod.rs` | 865 | `LiveSnapshotSource` type (`:67-71`), `live_snapshot_source()` accessor (`:343`), `PreCheckpointHook` (`:55-56`) — the two injected seams the two payload kinds hang off |
| `replication-runtime/src/export.rs` | 143 | `live_snapshot_source` (`:22`) — the shard-side implementation; FM-REPLICATION-001 forcing tests `every_shard_contributes_its_blob_in_shard_order` (`:69`), `a_shard_that_cannot_export_fails_the_whole_sync` (`:96`) |
| `server/src/info/mod.rs` | — | `ReplicaState::from_phase` (`:300-307`) — the **only** external reader of `Phase`; `Connecting` and `PreparingCheckpoint` both render `wait_bgsave` (`:304`), `StreamingCheckpoint` renders `send_bulk` (`:305`) |
| `.scratch/hardening/specs/replication-failure-modes.md` | 1571 | FM-REPLICATION-001 (`:95-122`, Invariant `:102`), -004 (`:148-159`), -013 (`:322-332`, `Forced by` `:331`), -035/-044 (codec, `:763`/`:934`), -060 (`:1392-`), -063 (`:1472-1492`, Invariant `:1479`) |

## Problem (verified evidence)

### The envelope, step by step, twice

Both functions execute the same eleven-step envelope. Verified line by line:

| # | step | `stream_checkpoint` (`:888-988`) | `stream_live_dataset` (`:1018-1104`) |
| --- | --- | --- | --- |
| 0 | preflight | none — the caller already produced the directory | `live_snapshot_source()` or error (`:1027-1032`) |
| 1 | produce payload | **caller**: `spawn_blocking(create_checkpoint)` (`:824`) | **inline**: `source().await?` (`:1035`) |
| 2 | enumerate + total | `read_dir` loop + `fs::metadata().len()`, then `sort_by(name)` (`:899-913`) | `blobs.iter().map(len).sum()` (`:1037`), positional order |
| 3 | arm progress | `sync_total_bytes = total` (`:915`); `sync_bytes_transferred.store(0, Release)` (`:916`) | **verbatim identical** (`:1038-1039`) |
| 4 | phase + clock | **caller**: `set_phase(StreamingCheckpoint)` (`:849`), `sync_started_at = now()` (`:850`) | **inline** (`:1040-1041`) |
| 5 | start log | `info!(replica_id, file_count, total_size, "Streaming checkpoint to replica")` (`:918-923`) | same shape, `blob_count`, different message (`:1043-1048`) |
| 6 | prelude | `write_prelude` → `$FROGDB_CHECKPOINT` (`:926`) | `write_snapshot_prelude` → `$FROGDB_SNAPSHOT` (`:1050`) |
| 7 | body loop | `write_file_header{name,size}`; `stream_file_to_writer(path, stream, Some(&counter))`; `calculate_file_checksum(path)` — a **second read of the file**; `combined.update_file`; per-file `debug!` with `progress_percent()` (`:932-952`) | `write_file_header{name: "shard-{i}.dataset", size}`; `write_all(blob)`; `counter.fetch_add(len, Relaxed)`; `calculate_bytes_checksum(blob)`; `combined.update_file`; **no per-body log** (`:1056-1070`) |
| 8 | trailer | `combined.finalize()` let-bound (`:953`), then `FullSyncMetadata { rdb_size: total_size, checksum, replication_id, replication_offset }` let-bound (`:955-960`) + `write_metadata` (`:961`) | **the same four fields**, constructed inline inside the `write_metadata` call (`:1072-1081`) |
| 9 | net-bytes | `tracker().net_bytes_handle().record_output(total_size)` (`:966-969`) | **verbatim identical** (`:1086-1089`) |
| 10 | completion log | elapsed from `sync_started_at`, `transfer_rate_mbps`, `info!(files, total_bytes, elapsed_ms, rate_mbps)` (`:971-985`) | elapsed computed inline, **no rate**, `info!(blobs, total_bytes, elapsed_ms)` (`:1091-1102`) |

Classification of all eleven, so none is glossed:

- **Steps 3 and 9 are verbatim duplicates** — identical statements, identical order.
- **Step 8 is the same four field values spelled two ways**: the checkpoint path let-binds
  `checksum` (`:953`) and `metadata` (`:955-960`) and passes a reference; the live path inlines
  the whole struct into the `write_metadata` argument (`:1072-1081`). Same bytes, not the same
  text — so "verbatim" would have been wrong.
- **Steps 5, 6 and 7 are the same shape with one substitution each** — log field name and
  message; marker; body source.
- **Steps 2, 7-details and 10 differ in implementation** (enumeration rule; per-body progress
  cadence and logging; presence of the rate field).
- **Steps 0, 1 and 4 differ only in *placement*** — the checkpoint path's caller does them.

The twin claim therefore survives verification, with one honest correction: `stream_checkpoint`
is **pure emit**, while `stream_live_dataset` is **prepare + emit** — it owns its own payload
production (steps 0-1) where the checkpoint path's production lives in `handle_full`. That
asymmetry is the single most important fact for scoping the extraction, and it is what makes
a naive "wrap both bodies in one function" reshape wrong.

### The differences that are load-bearing, stated so they are not glossed

1. **Phase placement is observable — but the specific moves this proposal needs are not.**
   `Phase` feeds FM-REPLICATION-060: `PreparingCheckpoint` renders `state=wait_bgsave` and
   `StreamingCheckpoint` renders `state=send_bulk` in `INFO replication`. So moving a
   `set_phase` call is in principle a behavior change. The saving facts, both verified:
   - `ReplicaState::from_phase` maps `Connecting | PreparingCheckpoint => Some(WaitBgsave)`
     (`info/mod.rs:304`), and `info/mod.rs:343` is the **sole** call site of `from_phase` and
     the sole external read of `ReplicaInfo::phase` anywhere outside `frogdb-replication`. So
     `Connecting → PreparingCheckpoint` is invisible to every external surface.
   - It is invisible *in-crate* too, which is stronger than the first draft claimed. Grepping
     every `Phase` discrimination inside `frogdb-replication` returns only `Phase::Streaming`
     matches — `tracker.rs:299`, `:319`, `:599` and `replica_session.rs:317`, `:491`. Nothing
     in the crate branches on `PreparingCheckpoint` or `StreamingCheckpoint` at all; they
     appear only in the `Display` arms (`:69-70`) and in `set_phase` calls. So the only
     consumer that can distinguish any of these phases is `INFO`, and it distinguishes only
     `StreamingCheckpoint` from the other two.

   The `PreparingCheckpoint → StreamingCheckpoint` transition, by contrast, *is* observable
   through `INFO` and must not move relative to the first payload byte.
2. **The checkpoint path reads every payload byte twice.** `stream_file_to_writer`
   (`fullsync.rs:444-464`) streams and counts but does **not** hash; `stream_checkpoint` then
   calls `calculate_file_checksum(file_path)` (`:944`), re-opening and re-reading the same
   file. The receive side does it in one pass — `receive_to_file` (`fullsync.rs:475-510`)
   writes and hashes from the same buffer. This is a real asymmetry with a real cost (a
   full-sync reads the whole dataset off disk twice) and it is *not* a bug today.
3. **The declared size and the sent size are two different numbers.** The per-file header and
   `rdb_size` come from `fs::metadata().len()` captured during enumeration (`:903-908`);
   `stream_file_to_writer` returns `bytes_written`, which is bound at `:941` and used only in
   a `debug!` (`:948`). Nothing reconciles them, and `stream_file_to_writer` streams the file
   to EOF rather than to the declared size — so a file that changed length between
   enumeration and send would desynchronize the receiver's framing (`receive_to_file` reads
   exactly `header.size` bytes). **Unreachable today**: the directory is
   `data_dir/fullsync_{session_id}`, private to this session, populated by
   `rocks.create_checkpoint` with hardlinks to immutable SSTs, and nothing else writes there.
   Recorded because "the checkpoint files do not change" is an argument held outside the
   function, not a property of it.
4. **The progress counter advances at two different cadences.** The checkpoint path counts
   through `stream_file_to_writer`, which `fetch_add`s once per 64 KiB chunk
   (`fullsync.rs:459-461`, `CHUNK_SIZE = 64 * 1024` at `:41`) — so `sync_bytes_transferred`
   moves *during* a large file. The live path `fetch_add`s once per whole blob, after
   `write_all` returns (`:1067-1068`) — so a 200 MB blob shows zero progress until it is
   entirely written. Neither counter is externally rendered (`sync_total_bytes` and
   `sync_bytes_transferred` are private fields read only by `progress_percent`, which feeds
   one `debug!`), so this is not a defect. It is called out because a unified body loop that
   "simplifies" to one cadence would change the checkpoint path's in-flight reporting, and
   because it becomes *observable in the logs* once the live path gains the per-body `debug!`
   line (see change #3 below): the percentage printed after body *n* must be the fraction
   *including* body *n*, exactly as the checkpoint path prints it today.
5. **A stale doc-comment, and two stale cross-references.** `fullsync.rs:136-137` states that
   the payload bytes "flow through `stream_file_to_writer` / `receive_to_file` (which also
   compute the per-file SHA256)". Only `receive_to_file` computes a SHA256. The parenthetical
   reads as a shared property of a symmetric pair, and the asymmetry in fact 2 is exactly what
   it hides. (The module header at `:14` names the same two helpers but makes no SHA256 claim —
   it is accurate as written and needs no edit; the first draft of this proposal cited it
   wrongly.) Separately, `fullsync.rs:147` and `fullsync/stager.rs:21-23` both name
   `ReplicaSession::stream_checkpoint` as "the symmetric encoder on the primary side"; both go
   stale the moment that function is deleted.

### Why this is shallow (architecture vocabulary)

- **`CheckpointStreamCodec` is a deep module and is not the problem.** Behind four small
  methods sit the marker strings, the `$`-length grammar, the count bounds, the name-validation
  rule (FM-REPLICATION-044) and the metadata framing. It is doing its job. Everything below
  is about the layer that *calls* it.
- **The emit envelope is a seam that was never cut.** Between "the codec knows the grammar"
  and "the session knows its progress counters" sits a rule nobody owns: *enumerate the
  bodies, fix their wire order, name them, fold the combined checksum in that same order,
  count the bytes, stamp the trailer with the total, record the transfer once, log the rate.*
  That rule is implemented five times. `CheckpointChecksum` (`fullsync.rs:338-363`) owns the
  checksum's **coverage** — its doc-comment is explicit that this exists so "the sender and
  receiver cannot drift on coverage" — but the **order** in which files are folded, and the
  **names** they are folded under, are left to each caller. Coverage was extracted; ordering
  and naming were not, and between them they are half the invariant.
- **Test fixtures are the measurement.** `encode_checkpoint_body` (`connection.rs:852`),
  `encode_dataset_body` (`connection.rs:1076`) and `encode_envelope` (`receiver.rs:74`) exist
  because there is nothing to call. Two of the three say "folding the combined checksum the
  way the sender does" — a comment that names a missing interface — and the third copies the
  sender's `shard-<n>.dataset` naming rule outright. A fourth site, at
  `replica_session.rs:2611-2618`, re-folds the checksum *and* re-derives the same names inside
  an assertion that checks the emitter's own output, which means the test and the code under
  test contain the same loop. Agreement between sender and receiver is currently a property
  maintained **by inspection across five sites**, and the live path's naming rule is written
  **three** times.
- **`FullSyncEmitter` would be deep, and passes the deletion test.** Delete it and eleven
  envelope steps reappear twice in a 4574-line file, plus three test fixtures return, plus the
  wire-order and naming rules go back to being unstated. That is the definition of leverage: a
  three-argument call standing in front of enumeration, ordering, naming, checksum folding,
  progress accounting, marker selection, trailer construction and completion logging.
  **Locality**: a change to the envelope — a new field in the trailer, a different progress
  cadence, single-pass hashing — becomes one edit in one module instead of two edits plus
  three fixture edits plus a review that has to notice the fixtures exist.
- **A shallow version of this exists and must be rejected.** `emit(stream, marker, items)`
  with the caller building `items` leaves the two rules that actually differ — sort-by-name
  vs positional-shard-index, and file-path vs in-memory body — in the callers, which is where
  they are now. The depth is in `PayloadSource` owning enumeration and naming, because that
  is precisely what the checksum's wire order depends on.

## Proposed change

**One new module, `frogdb-replication/src/fullsync/emitter.rs`**, sitting above the codec and
below the session. `fullsync.rs` gains one `pub mod emitter;` line next to the existing
`receiver` and `stager` (`:20-21`) — the emit side finally has the module the land side got
from proposal 30.

### The payload source, shaped so the fixtures can use it

The obvious two-variant enum (`CheckpointDir(PathBuf)` / `LiveDataset(Vec<Vec<u8>>)`) was the
first draft's design and it **cannot serve a single one of the three fixtures**, which is
disqualifying given that collapsing them is half this proposal's case. Each failure is
specific:

- All three fixtures hand in **named in-memory bodies**. `CheckpointDir` would force every one
  of them through a `tempdir` round-trip, and would then **re-sort** them: the checkpoint
  fixtures' wire order is `CURRENT`, `000042.sst` (`connection.rs:918-919`;
  `receiver.rs:106-110` adds `MANIFEST-000005`), and a name sort puts `000042.sst` first —
  reversing the byte order those tests were written against and changing the combined checksum.
- `LiveDataset(Vec<Vec<u8>>)` would impose `shard-<n>.dataset` names on bodies that must keep
  their real names: `receiver_reads_framed_files_and_metadata` asserts the *on-disk* filenames
  after landing (`receiver.rs:129-132`, `incoming.join(name)` per fixture name).
- `encode_dataset_body` mutates one payload byte **after** the fold and **before** the trailer
  (`connection.rs:1105-1108`), which no single `emit` call can express.

So the source carries names, and the live path's naming rule becomes a constructor on it —
which keeps the rule inside the module (that was the depth claim) while making the fixture case
expressible:

```rust
pub(crate) enum PayloadMarker { Checkpoint, Snapshot }

/// What a full-sync payload is made of, and how it is enumerated onto the wire.
/// Owning *both* here is the point: the combined checksum is folded in wire
/// order under wire names, so the ordering rule, the naming rule and the
/// payload kind cannot be separated.
pub(crate) enum PayloadSource {
    /// A cut RocksDB checkpoint directory. Bodies are files, named by their file
    /// name, ordered by that name. Marker: `$FROGDB_CHECKPOINT`.
    CheckpointDir(PathBuf),
    /// Already-named in-memory bodies, emitted in vec order under `marker`.
    Bodies { marker: PayloadMarker, bodies: Vec<(String, Vec<u8>)> },
}

impl PayloadSource {
    /// The live-dataset rule, written once instead of three times: positional
    /// `shard-<n>.dataset` names in shard order, under `$FROGDB_SNAPSHOT`.
    pub(crate) fn live_dataset(blobs: Vec<Vec<u8>>) -> Self { /* Bodies { Snapshot, .. } */ }
}
```

### The emitter, in three layers

The fixtures also **deliberately omit the prelude** — `psync` consumes the marker and count
before dispatching, so `receive_checkpoint`/`receive_snapshot` take `file_count`/`blob_count`
as a parameter and start reading at the first body frame. Both fixture doc comments say so
(`connection.rs:850-851`, `:1074-1075`). A single `emit` that always writes the prelude is
therefore **not** drop-in for them. Three entry points, each one line on top of the next:

```rust
pub(crate) struct EmitSummary { pub payload_bytes: u64, pub checksum: [u8; 32] }

pub(crate) struct FullSyncEmitter<'a> {
    pub source: PayloadSource,
    pub replication_id: &'a str,
    pub replication_offset: u64,
}

impl FullSyncEmitter<'_> {
    /// Prelude + bodies + trailer. What production calls. Returns the payload
    /// size announced in the trailer — the number the caller records via
    /// `record_output`.
    pub(crate) async fn emit<W: AsyncWrite + Unpin>(
        self, w: &mut W, obs: &dyn EmitObserver,
    ) -> io::Result<u64>;

    /// Bodies + trailer, no prelude — the shape the receive side's fixtures
    /// need, because `psync` already consumed the prelude.
    pub(crate) async fn emit_body<W: AsyncWrite + Unpin>(
        self, w: &mut W, obs: &dyn EmitObserver,
    ) -> io::Result<EmitSummary>;

    /// Bodies only; the caller writes its own trailer. Exists for the one
    /// fixture that mutates a payload byte between the fold and the trailer.
    pub(crate) async fn emit_bodies<W: AsyncWrite + Unpin>(
        self, w: &mut W, obs: &dyn EmitObserver,
    ) -> io::Result<EmitSummary>;
}
```

`emit` = `write_prelude` + `emit_body`; `emit_body` = `emit_bodies` + `write_metadata`. Nothing
in the layering is test-only — `emit_bodies` is the honest decomposition point of an envelope
whose prelude is consumed by a different function on the other side.

### The observer

```rust
/// Where the emitter reports payload size and progress. `ReplicaSession` is the
/// production adapter; a test recorder is another, in three lines.
pub(crate) trait EmitObserver {
    fn payload_sized(&self, total_bytes: u64);  // arms sync_total_bytes, zeroes the counter
    fn byte_sink(&self) -> &AtomicU64;          // the in-flight progress counter
    fn progress_percent(&self) -> f64;          // for the per-body debug line
}

/// The adapter for callers with no session: a private counter and a live
/// percentage. What the three fixtures use.
pub(crate) struct DetachedProgress { /* total: Cell<u64>, sink: AtomicU64 */ }
```

`EmitObserver` is the **interface**; `ReplicaSession` and the test recorder are its
**adapters**. (The first draft called the trait itself "an adapter" — that was a vocabulary
slip; the point it was making is the one restated here.) `sync_total_bytes`,
`sync_bytes_transferred`, `sync_started_at` and `Phase` stay owned by `ReplicaSession` — they
are session lifecycle, and `progress_percent_is_the_transferred_fraction_of_the_expected_total`
(`:3196-3210`) reaches into those fields directly and must keep working unedited. The emitter
*borrows* the sink; the session keeps the state.

**`progress_percent` name collision, resolved.** `ReplicaSession::progress_percent` is today a
**private inherent** fn (`:991-998`), and `:3196` calls it as an inherent method. Adding a
trait method of the same name does not break that: Rust resolves inherent methods before trait
methods, so `session.progress_percent()` at `:3196` binds to exactly the fn it binds to today
and the test stays unedited. The `impl EmitObserver for ReplicaSession` body for that method is
a one-line forward to the inherent fn — no formula is duplicated. The alternative (compute the
percentage inside the emitter from its own total and sink) was rejected: it restates the
`total == 0 → 100.0` guard in a second place, which is the duplication this proposal exists to
remove.

**`transfer_starting` is deliberately *not* on the trait** — see change #2 below.

### Call-site rewiring

`handle_full` (`:793-868`) becomes symmetric — **prepare, mark, emit** — in both branches:

```rust
let source = if let Some(rocks) = handler.rocks_store.as_ref().cloned() {
    self.set_phase(Phase::PreparingCheckpoint);
    // unchanged: pre-checkpoint drain (:807-821), create_checkpoint (:824-826),
    // the two failure arms (:829-845), sync_checkpoint_path (:848)
    PayloadSource::CheckpointDir(checkpoint_path)
} else {
    self.set_phase(Phase::PreparingCheckpoint);
    let Some(src) = handler.live_snapshot_source() else { return Err(/* unchanged text */) };
    PayloadSource::live_dataset(src().await?)
};

// The one INFO-visible transition, kept in the session layer (see #2).
self.set_phase(Phase::StreamingCheckpoint);
self.inner.write().sync_started_at = Some(clock::now());

let total = FullSyncEmitter { source, replication_id: &replication_id,
                              replication_offset: snapshot_offset }
    .emit(&mut stream, self.as_ref()).await?;
handler.tracker().net_bytes_handle().record_output(total);
```

`record_output` stays at the **call site** rather than moving inside, so FM-REPLICATION-063's
"recorded once, after `write_metadata` succeeds" remains a statement about a line the caller
can see.

Three deliberate consequences:

1. **The live path's `set_phase(PreparingCheckpoint)` moves** from `:1034` (after the
   source-presence check) to before it, matching the checkpoint branch. Provably unobservable:
   the prior phase is `Connecting`; `ReplicaState::from_phase` maps `Connecting` and
   `PreparingCheckpoint` to the same `WaitBgsave` (`info/mod.rs:304`); `INFO` is the only
   external reader; and nothing in `frogdb-replication` discriminates either variant
   (difference #1 above).
2. **The `StreamingCheckpoint` transition stays in the session layer, in `handle_full`.** The
   first draft moved it *into* the emitter, which is wrong on D2 grounds: the ruled
   restructure makes phase transitions the **output** of a pure `step(view, event) -> (phase,
   effects)`, so pushing one down into an effect executor is exactly the direction D2(iii)
   will have to undo. Keeping it in `handle_full` also keeps the emitter's interface to three
   methods and removes the need for a `transfer_starting` callback at all.

   What this costs, stated precisely: today the checkpoint path sets the phase (`:849-850`)
   *before* `sync_total_bytes` is armed (`:915-916`), while the live path arms the counters
   (`:1038-1039`) *before* setting the phase (`:1040-1041`). Hoisting the transition into
   `handle_full` gives **both** paths the checkpoint path's order — phase first, then
   `payload_sized` inside `emit`. So the live path's ordering flips. That is unobservable:
   `sync_total_bytes` and `sync_bytes_transferred` are private fields of `ReplicaSession`'s
   inner state (`:374`, `:401`), read only by `progress_percent` — which feeds one `debug!`
   line — and by tests (`:3204`, `:3353`). No `INFO` field, no metric and no wire byte is
   derived from either, so a concurrent reader cannot land in the reordered window and see
   anything. Both orderings keep the whole bookkeeping between "payload exists" and "first
   payload byte", which is the property `INFO` actually depends on.
3. **Two logging deltas are resolved by unification, not preserved.** The live path gains the
   per-body `debug!` and the `rate_mbps` field it lacks today. Log fields are not a pinned
   surface (no test and no FM row asserts on them; `transfer_rate_mbps` has its own unit test
   at `:3216`), and the alternative — carrying two log shapes through a parameter — would
   re-import the duplication the module exists to remove. The unified body loop must keep
   *both* progress cadences from difference #4: `fetch_add` per 64 KiB chunk for file bodies
   (inherited free, because the loop still calls `stream_file_to_writer`) and per whole body
   for in-memory ones, with the `debug!` emitted after the body's bytes are counted so the
   printed percentage includes the body just sent.

**Explicitly out of scope for the extraction commit: single-pass hashing.** Fact 2 above makes
it tempting to have the emitter hash while it streams, deleting the second file read and
closing fact 3 structurally. It is the right end state and the emitter makes it a one-line
future change — but it moves the checksum's meaning from *"hash of the file as it is after
sending"* to *"hash of the bytes actually sent"*. Those are identical today and the second is
strictly stronger, which is exactly why it does not belong in a commit whose entire claim is
"nothing changed": strengthening a locked-area invariant is a spec-first change, not a
refactor side effect. It is proposed as a separate, independently reviewable follow-up below.

### Hard acceptance criteria (LOCKED crate)

1. **Golden pin lands first, in its own commit, against today's code — in the two shapes the
   type system actually allows.** The first draft asked for one test driven through
   `stream_checkpoint` / `stream_live_dataset` that "passes unedited" after the extraction.
   That is self-contradictory twice over: those functions take `stream: &mut BoxedStream`
   (`lib.rs:93`, `Box<dyn AsyncReadWrite + Unpin + Send>`), so a golden cannot write into a
   `Vec<u8>` at all — `Vec<u8>` is `AsyncWrite` but not `AsyncRead`; and step 2 **deletes**
   both functions, so a test that names them cannot survive unedited by construction. Split it:

   - **`full_sync_snapshot_golden_bytes` — driven through `handle_full`, survives verbatim.**
     `handle_full` takes `replication_id: String` as a parameter (`:747`) and reads
     `snapshot_offset` from `handler.offsets.current()` (`:776`), so a fresh tracker plus a
     literal replid makes the entire byte stream deterministic: `+FULLRESYNC <id> 0\r\n`, then
     `$FROGDB_SNAPSHOT\r\n2\r\n`, then two `shard-<n>.dataset` frames from a stub
     `live_snapshot_source`, then the trailer — asserted as one byte literal including the
     32-byte checksum's hex. `handle_full` survives the extraction, so this test must appear
     **unedited** in the extraction diff. Precedent for the driving style is already in the
     file: `:2707`, `:2812` and `:2908` call `handle_full` directly.
   - **`full_sync_checkpoint_golden_bytes` — driven through a one-line helper.** The checkpoint
     path cannot go through `handle_full` for a *byte* golden, because its payload comes from a
     real `RocksStore::create_checkpoint` and RocksDB's SST names and contents are not
     reproducible across runs. It needs a hand-made directory, and today the only thing that
     accepts one is `stream_checkpoint`. So the test's driving call is isolated in a helper —
     `async fn emit_checkpoint_dir_for_golden(dir, id, offset) -> Vec<u8>`, which today writes
     through a `tokio::io::duplex(1 << 20)` and drains the client half. The extraction commit
     edits **that helper's body only** (from `stream_checkpoint(...)` to
     `FullSyncEmitter { .. }.emit(..)`). The `expected` byte-literal block and the checksum hex
     literal are the pinned region: **if any line inside them appears in the extraction diff,
     the refactor changed the wire and review stops there.**

   Both layer on `test_checkpoint_codec_golden_bytes` (`fullsync.rs:831`, FM-REPLICATION-035),
   which pins the *grammar* but not the *composition* — file order, marker selection, checksum
   coverage order, and the trailer's `rdb_size`.
2. **Checksum bytes identical, not just "verified".** Both goldens pin the trailer's 32-byte
   combined checksum as a hex literal, so a reordering that both sides would still agree on is
   still caught.
3. **Write granularity preserved — no buffering.** `full_sync_replays_writes_made_during_handoff`
   (`:2133`, FM-REPLICATION-004's *only* forcing test) and
   `fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` (`:2812`) both stall the
   session mid-payload using a `tokio::io::duplex(32)` (`:2144`) / `duplex(64)` (`:2861`) pair —
   they depend on the emitter blocking inside the body loop rather than assembling the envelope
   in memory. Wrapping the stream in a `BufWriter` would deadlock or trivialize them. The
   emitter must write through to the caller's stream, exactly as today.
4. **Forcing tests pass unedited.** Every FM-tagged test that touches these paths:

   | test | line | row | drives | what it pins |
   | --- | --- | --- | --- | --- |
   | `run_full_sync_without_rocks_streams_the_live_dataset` | `:2562` | -001 | `run` (`:2584`) | `$FROGDB_SNAPSHOT` envelope, blob decode, combined checksum, `rdb_size`, trailer id/offset |
   | `full_sync_without_a_live_snapshot_source_fails_the_sync` | `:2707` | -001 | `handle_full` (`:2720`) | the preflight refusal — must stay before any payload byte |
   | `full_sync_replays_writes_made_during_handoff` | `:2133` | -004 | `run` (`:2153`) | the handoff window, stalled inside the live body loop |
   | `repl_output_bytes_include_the_full_sync_payload` | `:2650` | -063 | `run` (`:2672`) | `record_output(total_size)`, once, after the trailer |
   | `fullresync_offset_and_metadata_come_from_live_tracker` | `:3253` | **-013** (tag `:3247`, listed in `Forced by` at spec `:331`) | `run` (`:3288`) | trailer id/offset from the live tracker **and** `sync_total_bytes == streamed_bytes` (`:3352-3356`) |
   | `fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` | `:2812` | — | `handle_full` (`:2866`) | drain-before-cut ordering, checkpoint stream stalled |
   | `fullresync_fails_when_the_pre_checkpoint_drain_fails` | `:2908` | — | `handle_full` (`:2940`) | drain failure errors the sync before any payload |
   | `run_cleans_up_checkpoint_dir_on_mid_fullsync_drop` | `:2742` | — | `run` (`:2771`) | `sync_checkpoint_path` cleanup |
   | `progress_percent_is_the_transferred_fraction_of_the_expected_total` | `:3196` | — | **nothing** — no stream, no session run; calls the inherent fn directly | `sync_total_bytes` / `sync_bytes_transferred` reached **directly** — the reason `ReplicaSession` keeps owning them |

   Eight of the nine drive the session end-to-end over a duplex pair (five through `run`, three
   through `handle_full`) and are insulated from the extraction by construction; the ninth
   touches no I/O at all. So the fourth governing row is **FM-REPLICATION-013**, not "—": the
   first draft's dash was wrong.

   `every_shard_contributes_its_blob_in_shard_order` and `a_shard_that_cannot_export_fails_the_whole_sync`
   (`replication-runtime/src/export.rs:69`, `:96`) live in the sibling crate and are untouched —
   they force the `LiveSnapshotSource` implementation, not its consumer.
5. **Forcing tests stay in the mutated crate.** All of the above are inline `#[cfg(test)]` tests
   in `frogdb-replication`, so `cargo mutants -p frogdb-replication` sees them; the new emitter
   module and both goldens live in the same crate. No forcing test moves to `frogdb-server`.
6. **Spec prose edited in the same commit — one edit is *not* citation-only, and the lint will
   catch neither.** Two Invariant cells name the two functions:
   - FM-REPLICATION-001 (`:102`): *"without it `stream_live_dataset` serializes the live
     keyspace"* — **citation-only**. The sentence's meaning (a primary without RocksDB
     serializes its live keyspace rather than sending nothing) is unchanged; only the name of
     the code that does it moves. Rename to the emitter's live-dataset path.
   - FM-REPLICATION-063 (`:1479`): *"**Four call sites record output**, all on the primary:
     `stream_checkpoint` and `stream_live_dataset` each record `total_size` … once, after
     `write_metadata` succeeds"* — **not citation-only**. Collapsing the two emit sites into one
     `record_output` in `handle_full` makes it **three** call sites (`handle_full`, the
     live-forward path in `start_streaming`'s write task, the backlog-replay tail loop). That is
     an **arity change in the row's own inventory**, so the first draft's "meaning is unchanged"
     was wrong and must not be repeated in the PR description. It remains in-bounds under the D2
     discipline — the *invariant* (every full-sync payload is counted exactly once, after the
     trailer lands) is untouched, and the inventory is a mechanism description — but it must be
     declared as a counted-inventory edit and reviewed as one.

   `just lint-failure-modes` checks only the `Forced by` test-name agreement, and **no test is
   renamed by this proposal**, so the lint stays green over both stale sentences. This is a
   human-review item. No row's Observable, NOT-observable or `Forced by` cell changes.
7. **Mutation re-gate.** `just mutants-diff frogdb-replication` before pushing; then full
   `just mutants frogdb-replication` + `just mutants-gate frogdb-replication 0.85`. A full run,
   not just the diff, because the extraction moves mutation targets between functions and the
   diff view would score the new module against nothing.

## Testability improvement

**The interface is the test surface.** Today the emit envelope has no interface, so its test
surface is a socket: every assertion about what the primary puts on the wire has to spawn
`session.run()` over a `tokio::io::duplex` pair, speak enough of the handshake to get to
`+FULLRESYNC`, and then hand-decode the envelope (`drain_live_dataset`, `:1936-1973`, is a
test-side decoder written for exactly this). Four concrete consequences, each verified:

1. **Sender/receiver agreement is maintained by inspection across five sites.** The receive
   side's tests cannot use the sender, so they built three: `encode_checkpoint_body`
   (`connection.rs:852-882`), `encode_dataset_body` (`connection.rs:1076-1119`) and
   `encode_envelope` (`receiver.rs:74-101`). Two are documented as folding the checksum *"the
   way the sender does"*; the third re-derives the sender's `shard-<n>.dataset` naming rule.
   Nothing checks that any of them still match. After the extraction all three are **deleted**
   and their bodies become one `FullSyncEmitter { .. }.emit_body(&mut buf, &DetachedProgress)`
   call (`emit_bodies` plus a hand-written trailer for the `corrupt` variant) — at which point
   "the receiver parses what the sender emits" stops being a claim two comments make and
   becomes the only way the tests can be written. That is three test helpers passing the
   deletion test as a side effect, and the live path's naming rule dropping from three copies
   to one.
2. **The phase transition nobody pins becomes assertable.** Searching the tree, every test that
   mentions `Phase::StreamingCheckpoint` *sets* it rather than observing the emitter advance
   it: `session_accessors_report_the_announced_identity_and_live_phase` (`:3143`, forces the
   phase at `:3174` to assert `is_streaming()` is false) and `info/sections.rs:1135`, `:1145`,
   `:1184`, `:1204`, which pin the `Phase → wait_bgsave/send_bulk` mapping but not when the
   transition happens. (The first draft cited `force_phase_for_test_drives_phase` `:3014` and
   `tracker.rs:943` here; both use *other* variants — `Phase::Streaming` and
   `Phase::PreparingCheckpoint` respectively. The conclusion is unchanged.) So today **nothing
   fails** if `set_phase(StreamingCheckpoint)` moves after the first payload byte, which would
   render a replica as `wait_bgsave` while its payload is already on the wire — precisely the
   shape FM-REPLICATION-060 exists to forbid. Once `emit` is callable against a `Vec<u8>` with
   a recording observer, a test can assert *the phase is `StreamingCheckpoint` before the first
   byte reaches the writer* by having the observer's `payload_sized` read
   `session.phase()` — a socket-free unit assertion. This is a **new** pin the extraction
   enables, filed as a test the extraction commit adds — not a claim that today is broken.
3. **Emitter-level properties become expressible at all.** With `emit` writing into a
   `Vec<u8>`, these are direct assertions instead of socket choreography: file ordering is
   name-sorted regardless of `read_dir` order (today an untested dependency on `sort_by` at
   `:913`); an empty checkpoint directory emits a well-formed zero-count envelope (the codec has
   `test_checkpoint_codec_zero_file_prelude` at `:868`, the *emitter* has nothing); the
   announced `rdb_size` equals the bytes actually written; the progress counter ends at exactly
   `sync_total_bytes`. A proptest over `PayloadSource::live_dataset(blobs)` asserting `emit`
   output round-trips through `receive_snapshot`'s parse becomes a ten-line test rather than a
   two-node integration fixture.
4. **The mutation gate gets a better-shaped target.** Two ~50-line async I/O functions
   reachable only through a socket are hard territory for `cargo mutants`; one module whose
   every step is directly asserted by criteria 1-3 is the shape that scores well. Expect the
   crate's score to improve rather than merely hold.

## Risks / scope boundaries vs sibling proposals

### Boundary vs proposal 55 (adopt-full-sync-landing, RC3) — emit side / land side

The two proposals are the two halves of one wire protocol. The line is drawn by **direction of
travel**:

| unit | owner |
| --- | --- |
| `stream_checkpoint` (`replica_session.rs:888-988`) | **53** |
| `stream_live_dataset` (`replica_session.rs:1018-1104`) | **53** |
| `handle_full`'s payload-preparation branch (`:793-868`) | **53** |
| new `fullsync/emitter.rs` | **53** |
| the three receive-side sender fixtures (`connection.rs:852-882`, `:1076-1119`; `receiver.rs:74-101`) | **53** |
| the landing tails of `receive_snapshot` (`connection.rs:404-418`) and `receive_checkpoint` (`:475-497`) | **55** |
| `fullsync/receiver.rs` production code, `fullsync/stager.rs` | **neither — 55 lists both as untouched** |
| `CheckpointStreamCodec`, `CheckpointChecksum`, `FullSyncMetadata`, `calculate_*`, `stream_file_to_writer`, `receive_to_file`, both markers (`fullsync.rs`) | **neither — shared, unchanged by both** |

The first draft's ownership table and its "three conflict edges" were substantially wrong about
55 and are replaced here. Corrections, then the real edges:

- **Edge 1 was fabricated.** It claimed both proposals add a `pub mod` line to `fullsync.rs`.
  55 adds no module to `fullsync.rs` at all — its whole change is one method, one struct and one
  two-arm adapter *inside* `replica/connection.rs`, and its Files table explicitly lists
  `fullsync/receiver.rs` as an untouched seam. There is no textual edge in `fullsync.rs`.
- **Edge 3 was fabricated.** It claimed 55 "will want to delete `encode_checkpoint_body` and
  `encode_envelope`". 55 deletes neither; its Effort section states **"No test edits required"**.
  The three fixtures are 53's to delete, and the ownership table above is corrected accordingly
  (the first draft wrongly assigned `receiver.rs` and `stager.rs` to 55).
- **Edge 2 stands.** Both are `frogdb-replication` changes needing the 0.85 gate; run the full
  gate once at the end of the chain, not once per proposal.

The two real edges:

1. **Rebase cost, and it is what dictates order.** 53's step 3 rewrites
   `connection.rs:849-964` and `:1074-1119` — deleting two fixtures totalling ~90 lines and
   rewriting their two call sites. Every line citation in 55 below `:852` shifts, including the
   three tests its independently-landable hotfix tags (`:971`, `:995`, `:1044`) and all six of
   its landing-test citations (`:1188`-`:1521`). **Land 53 first**; 55 then rebases onto stable
   numbers. The reverse order costs 55 a full re-verification pass over citations it already
   did once. This is the adjudicated ruling carried over from 55's own review.
2. **A genuine spec merge conflict, on one sentence.** Both proposals edit
   FM-REPLICATION-063's Invariant cell — **the same cell, `:1479`**. 53 edits its *output* half
   ("Four call sites record output … `stream_checkpoint` and `stream_live_dataset` each record
   `total_size`"); 55 edits its *input* half ("`receive_snapshot` and `receive_checkpoint` each
   record `metadata.rdb_size`", which after 55 both record *through* `adopt_full_sync`). Whoever
   lands second must re-read the cell and re-state **both** counts, not just its own. Neither
   proposal can pre-write the merged sentence.

Neither proposal touches the install-ordering invariant FM-REPLICATION-001 pins on the land
side ("`install_payload` runs before either half of the granted identity is adopted") — that is
55's territory and it is a behavior contract, not a duplication.

### Boundary vs proposal 54 (replica-connection-wiring, RC2 + RC10) — a real collision the first draft did not analyse

54 replaces all **eleven** `ReplicaConnection` struct literals with a
`ReplicaConnection::new(stream, ReplicaLink)` / `for_test` call. Eight of those literals are in
`replica/connection.rs` (`:623`, `:680`, `:802`, `:934`, `:1154`, `:1314`, `:1458`, `:1545`),
and **two of them sit inside functions 53 rewrites**:

- `:934` is the literal inside `checkpoint_fixture_with_tail` (`:912-971`), whose
  `encode_checkpoint_body` call at `:922` is a 53 step-3 target.
- `:1154` is the literal inside `dataset_fixture_with_tail` (`:1134-1180`), whose
  `encode_dataset_body` call at `:1142` is the other one — the fifth copy the first draft
  missed entirely, which is why this edge went unnoticed.

Both proposals therefore edit the same two function bodies. 54's own conflict table calls its
55 edge a "hard conflict" for the same structural reason and rules **55 before 54**, because
54 rewrites the type's construction shape last. The same logic puts 53 ahead of both: 53's edits
are to the *fixture-encoding* lines, 54's to the *literal* lines a few lines below them, and
rebasing 54 (a mechanical eleven-site rewrite) onto 53 is cheaper than re-deriving 53's fixture
collapse against a changed constructor.

**Resulting chain, all four items in `frogdb-replication`:**

> 55's tag-only hotfix (add `// FM-REPLICATION-001` to the three untagged checkpoint landing
> tests) → **53** → **55** → **54** (54's `_primary_addr` deletion may go first or anywhere;
> it is independent). One `just mutants frogdb-replication` + `just mutants-gate
> frogdb-replication 0.85` at the end of the chain, plus `just mutants-diff` per landing.

The hotfix goes first because it is a spec-file-and-tags-only edit that touches no line 53, 54
or 55 moves, and it gives the rest of the chain a spec-forced target on the land side.

### Relationship to the replication-correctness campaign's D2 (authorized `replica_session.rs` restructure)

`.scratch/replication-correctness/PRD.md:533-554` records D2 as **ruled 2026-08-10: full
restructure (iii) authorized** — `replica_session.rs` becomes an explicit phase state machine
with a pure `step(view, event) -> (phase, effects)` and the async loop as an interpreter, with
(i) promotion/feed-gate transition extraction and (ii) the replica-side PSYNC arm selection
landing first as stepping stones, and the full 0.85 gate re-run after (iii).

This proposal is a **subset in file, disjoint in kind, and complementary in shape**:

- D2(iii) restructures the *decision* layer — which phase, on which event, producing which
  effects. Proposal 53 extracts an *effect executor* — the I/O that one of those effects
  performs. They occupy the same 4574-line file but not the same lines: 53 takes `:888-1104`
  plus the branch body of `:793-868`; D2(iii) takes `run` (`:642`), the phase machine and the
  session loop.
- **Phase transitions stay on D2's side of the line.** This is the correction to the first
  draft, and it is why change #2 above keeps `set_phase(StreamingCheckpoint)` in `handle_full`.
  D2(iii) makes phase transitions the *output* of `step`; an emitter that flipped a phase from
  inside an effect would be a transition D2 has to pull back out. The emitter therefore needs
  only `payload_sized`, `byte_sink` and `progress_percent` — no lifecycle callback — and every
  `set_phase` call in the full-sync path stays in the session layer where `step` will eventually
  own it.
- D2(i) (promotion / feed-gate transitions) and D2(ii) (replica-side PSYNC arm selection,
  `replica/connection.rs:224`) are entirely disjoint from 53.
- **Recommendation: land 53 before D2(iii) begins.** It removes ~190 lines of pure I/O from the
  file D2(iii) must restructure, and `FullSyncEmitter` + `EmitObserver` is exactly the shape an
  interpreter wants on the far side of `step`: a self-contained effect executor with an injected
  observer and no phase authority of its own. Doing it first makes D2(iii) smaller; doing it
  after means doing it inside D2(iii).
- **Do not run them concurrently.** Two changes restructuring one locked 4574-line file at the
  same time is the shared-tree trap in the memory ledger, and D2's own execution discipline
  ("land (i) and (ii) first as stepping stones, then (iii) as its own issue chain") assumes a
  serial chain. If D2(iii) has already started when this proposal is triaged, **fold 53 into
  the D2 chain as its first stepping stone** rather than landing it in parallel.
- 53 needs no `frogdb-replication` public-surface change (`FullSyncEmitter`, `PayloadSource`,
  `PayloadMarker`, `EmitSummary` and `EmitObserver` are all `pub(crate)`), so it cannot
  constrain D2's API choices.

### Risk — the extraction's whole claim is "nothing changed", and only the goldens prove it

The extraction has no behavioral acceptance test of its own; it borrows the nine tests in
criterion 4 plus the two new goldens. That is why criterion 1 insists they land **in a
separate, earlier commit against the unmodified functions**. A golden written at the same time
as the refactor pins whatever the refactor produced, which is worth nothing. Note the asymmetry
criterion 1 forces: the snapshot golden survives *verbatim*, the checkpoint golden survives
*except for one helper body*, and only the second one requires a reviewer to check that the
edited region is exactly the driving call.

### Risk — `PayloadSource::Bodies` holds the whole dataset in memory

It already does: `stream_live_dataset` binds `blobs` at `:1035` and holds it for the duration
of the transfer. Moving the binding into an enum variant changes nothing about peak residency —
`live_dataset()` maps `Vec<Vec<u8>>` into `Vec<(String, Vec<u8>)>`, which moves the blobs rather
than copying them and adds one short `String` per shard. Recorded so a reviewer does not read
the enum as *introducing* the buffering. (Making the live path streaming rather than buffered is
a genuine improvement and a genuinely separate change — it would alter the `LiveSnapshotSource`
type in `primary/mod.rs:67-71` and the shard export protocol in `replication-runtime`, both
outside this proposal.)

## Effort

**M**, in three commits with one shared re-gate. The mutation gate, not the code, is the long
pole.

| Step | Scope | Size |
| --- | --- | --- |
| **1 — golden pins** | `full_sync_snapshot_golden_bytes` (through `handle_full`, literal bytes + literal checksum hex, survives verbatim) and `full_sync_checkpoint_golden_bytes` (through a one-line `emit_checkpoint_dir_for_golden` helper over a hand-made directory). No production change. Independently landable and independently valuable: they are the composition-level pin the codec golden never provided, and they are worth having whether or not the extraction is approved. | **S** — ~130 test lines |
| **2 — extraction** | New `fullsync/emitter.rs` (`PayloadSource` + `live_dataset()`, `PayloadMarker`, `EmitObserver`, `DetachedProgress`, `emit`/`emit_body`/`emit_bodies`); `stream_checkpoint` / `stream_live_dataset` deleted; `handle_full` branch rewired to prepare-mark-emit; `ReplicaSession` implements `EmitObserver` (`progress_percent` forwards to the inherent fn); FM-REPLICATION-001 `:102` re-cited and FM-REPLICATION-063 `:1479` re-counted; stale doc text fixed at `fullsync.rs:136-137` (the SHA256 parenthetical), `fullsync.rs:147` and `fullsync/stager.rs:21-23` (both name `stream_checkpoint` as the symmetric encoder). | **M** — ~230 new (half tests), ~190 deleted |
| **3 — fixture collapse** | Delete `encode_checkpoint_body` (`connection.rs:852-882`), `encode_dataset_body` (`:1076-1119`) and `encode_envelope` (`receiver.rs:74-101`); their four call sites (`connection.rs:922`, `:1142`, `:1527`, `:1761`) and `receiver.rs`'s tests construct a `FullSyncEmitter` over a `Vec<u8>` — `emit_body` for the three plain cases, `emit_bodies` + a hand-written trailer for the `corrupt` variant. Optionally collapse the assertion-side re-fold at `replica_session.rs:2611-2618`. Coordinate with proposals 54 and 55 — see the boundaries above. | **S** — net negative |
| **Re-gate** | `mutants-diff` per landing, then full `mutants` + `mutants-gate frogdb-replication 0.85` at the end of the 53 → 55 → 54 chain. Testbox-class workload. | — |
| *Follow-up (separate proposal or issue, not this one)* | Single-pass hashing: `stream_file_to_writer` returns `(bytes, [u8; 32])` and streams exactly the declared size, mirroring `receive_to_file`. Halves full-sync disk reads and closes fact 3 structurally. Spec-first, because it strengthens what FM-REPLICATION-001's checksum covers. | **S** |

**Independently landable ahead of the refactor:** step 1 (the golden pins) and the three
doc-text corrections (`fullsync.rs:136-137`, `fullsync.rs:147`, `fullsync/stager.rs:21-23`).
**No hotfix is proposed**, because no defect was found — facts 2, 3, 4 and 5 in the Problem
section are a cost, a fragility held safe by an argument outside the function, an unobservable
cadence difference, and stale comments, respectively, and none of them is reachable as a bug on
the current tree.
