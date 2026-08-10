# Proposal 53 — `FullSyncEmitter`: one emit envelope for both full-sync payload kinds

Covers exploration-lane candidate **RC1** (`stream_checkpoint` / `stream_live_dataset` are
structural twins). The candidate as filed said the two functions duplicate "the same
progress/checksum/accounting envelope around different payload sources". Verified against
`f31e5911`: **the claim holds, but is narrower and sharper than filed** in one important way
and *wider* in another, and both corrections are load-bearing for how the extraction is
scoped.

- **Narrower.** The *wire grammar* is already centralized — `CheckpointStreamCodec`
  (`fullsync.rs:161-260`) owns the marker/count prelude, the per-file header and the trailing
  metadata frame, and both twins call it. Round 10's proposal 30 deepened the receive side
  (`CheckpointStager` + socket-free receiver) and this codec came with it. So this proposal
  is **not** "extract the wire format" — that is done, and it must not be re-litigated.
  What is duplicated is the layer *above* the codec: the emit envelope that drives it.
- **Wider.** That emit envelope is realized **four** times in the tree, not two: twice in
  production (`replica_session.rs:888-988`, `:1018-1104`) and twice more as test fixtures on
  the *receive* side — `encode_checkpoint_body` (`replica/connection.rs:852-881`) and
  `encode_envelope` (`fullsync/receiver.rs:73-101`) — each carrying a comment that says out
  loud what the missing module costs: *"folding the combined checksum **the sender way**"*.
  There is no sender to reuse, so every test that needs sender bytes writes a fourth sender.

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
| `replication/src/fullsync.rs` | 1164 | The shared, already-deep layer this proposal builds on and **does not change**: `FullSyncMetadata` (`:47-106`), `CHECKPOINT_MARKER`/`SNAPSHOT_MARKER` (`:110`/`:122`), `CheckpointStreamCodec` (`:161-330`), `CheckpointChecksum` (`:338-363`), `calculate_file_checksum` (`:409`), `calculate_bytes_checksum` (`:427`), `stream_file_to_writer` (`:444-464`), `receive_to_file` (`:475-511`); golden test `test_checkpoint_codec_golden_bytes` (`:831-864`) |
| `replication/src/fullsync/receiver.rs` | 219 | `receive_checkpoint_files` (`:38-63`) — the *land*-side twin of emitter #1, already extracted (proposal 30); test fixture `encode_envelope` (`:73-101`) — hand-rolled emitter #3 |
| `replication/src/replica/connection.rs` | 1884 | `receive_snapshot` (`:368-451`) and `receive_checkpoint` (`:452-…`) — **proposal 55's territory**; test fixture `encode_checkpoint_body` (`:852-881`) — hand-rolled emitter #4 |
| `replication/src/primary/mod.rs` | 865 | `LiveSnapshotSource` type (`:66-72`), `live_snapshot_source()` accessor (`:343`), `PreCheckpointHook` (`:55-56`) — the two injected seams the two payload kinds hang off |
| `replication-runtime/src/export.rs` | 143 | `live_snapshot_source` (`:22`) — the shard-side implementation; FM-REPLICATION-001 forcing tests `every_shard_contributes_its_blob_in_shard_order` (`:69`), `a_shard_that_cannot_export_fails_the_whole_sync` (`:96`) |
| `server/src/info/mod.rs` | — | `ReplicaState::from_phase` (`:304-305`) — the **only** external reader of `Phase`; `Connecting` and `PreparingCheckpoint` both render `wait_bgsave`, `StreamingCheckpoint` renders `send_bulk` |
| `.scratch/hardening/specs/replication-failure-modes.md` | — | FM-REPLICATION-001 (`:95-122`), -004 (`:148-159`), -035/-044 (codec, `:934-951`), -060 (`:1392-`), -063 (`:1472-1492`) |

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
| 8 | trailer | `FullSyncMetadata { rdb_size: total_size, checksum, replication_id, replication_offset }` + `write_metadata` (`:955-961`) | **the same four fields**, constructed inline (`:1072-1081`) |
| 9 | net-bytes | `tracker().net_bytes_handle().record_output(total_size)` (`:966-969`) | **verbatim identical** (`:1086-1089`) |
| 10 | completion log | elapsed from `sync_started_at`, `transfer_rate_mbps`, `info!(files, total_bytes, elapsed_ms, rate_mbps)` (`:971-985`) | elapsed computed inline, **no rate**, `info!(blobs, total_bytes, elapsed_ms)` (`:1091-1102`) |

Steps **3, 8 and 9 are verbatim duplicates**. Steps **6 and 7** are the same shape with one
substitution each (marker; body source). Steps **2, 7-details and 10** differ in
implementation. Steps **0, 1 and 4** differ only in *placement*.

The twin claim therefore survives verification, with one honest correction: `stream_checkpoint`
is **pure emit**, while `stream_live_dataset` is **prepare + emit** — it owns its own payload
production (steps 0-1) where the checkpoint path's production lives in `handle_full`. That
asymmetry is the single most important fact for scoping the extraction, and it is what makes
a naive "wrap both bodies in one function" reshape wrong.

### The differences that are load-bearing, stated so they are not glossed

1. **Phase placement is observable — but the specific move this proposal needs is not.**
   `Phase` feeds FM-REPLICATION-060: `PreparingCheckpoint` renders `state=wait_bgsave` and
   `StreamingCheckpoint` renders `state=send_bulk` in `INFO replication`. So moving a
   `set_phase` call is in principle a behavior change. The saving fact, verified at
   `server/src/info/mod.rs:304`: `Connecting | PreparingCheckpoint => Some(WaitBgsave)` — the
   `Connecting → PreparingCheckpoint` transition is **invisible to every external surface**,
   because `INFO` is the only reader of `Phase` outside the replication crate
   (`info/mod.rs:343` is the sole call site of `ReplicaState::from_phase`) and it collapses
   the two. The `PreparingCheckpoint → StreamingCheckpoint` transition, by contrast, is fully
   observable and must not move relative to the first payload byte.
2. **The checkpoint path reads every payload byte twice.** `stream_file_to_writer`
   (`fullsync.rs:444-464`) streams and counts but does **not** hash; `stream_checkpoint` then
   calls `calculate_file_checksum(file_path)` (`:944`), re-opening and re-reading the same
   file. The receive side does it in one pass — `receive_to_file` (`fullsync.rs:475-511`)
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
4. **A stale doc-comment.** `fullsync.rs:14` and `:133-137` both state that the payload bytes
   "flow through `stream_file_to_writer` / `receive_to_file` (which also compute the per-file
   SHA256)". Only `receive_to_file` computes a SHA256. The parenthetical reads as a shared
   property of a symmetric pair, and the asymmetry in fact 2 is exactly what it hides.

### Why this is shallow (architecture vocabulary)

- **`CheckpointStreamCodec` is a deep module and is not the problem.** Behind four small
  methods sit the marker strings, the `$`-length grammar, the count bounds, the name-validation
  rule (FM-REPLICATION-044) and the metadata framing. It is doing its job. Everything below
  is about the layer that *calls* it.
- **The emit envelope is a seam that was never cut.** Between "the codec knows the grammar"
  and "the session knows its progress counters" sits a rule nobody owns: *enumerate the
  bodies, fix their wire order, name them, fold the combined checksum in that same order,
  count the bytes, stamp the trailer with the total, record the transfer once, log the rate.*
  That rule is implemented four times. `CheckpointChecksum` (`fullsync.rs:338-363`) owns the
  checksum's **coverage** — its doc-comment is explicit that this exists so "the sender and
  receiver cannot drift on coverage" — but the **order** in which files are folded is left to
  each caller. Coverage was extracted; ordering was not, and ordering is half the invariant.
- **Test fixtures are the measurement.** `encode_checkpoint_body` (`connection.rs:852`) and
  `encode_envelope` (`receiver.rs:73`) exist because there is nothing to call. Both say
  "folding the combined checksum the way the sender does" — a comment that names a missing
  interface. A third, at `replica_session.rs:2611-2618`, re-folds the checksum inside an
  assertion to check the emitter's output, which means the test and the code under test
  contain the same loop. Agreement between sender and receiver is currently a property
  maintained **by inspection across four sites**.
- **`FullSyncEmitter` would be deep, and passes the deletion test.** Delete it and eleven
  envelope steps reappear twice in a 4574-line file, plus two test fixtures return, plus the
  wire-order rule goes back to being unstated. That is the definition of leverage: a
  three-argument call standing in front of enumeration, ordering, naming, checksum folding,
  progress accounting, marker selection, trailer construction, transfer accounting and
  completion logging. **Locality**: a change to the envelope — a new field in the trailer, a
  different progress cadence, single-pass hashing — becomes one edit in one module instead of
  two edits plus two fixture edits plus a review that has to notice the fixtures exist.
- **A shallow version of this exists and must be rejected.** `emit(stream, marker, items)`
  with the caller building `items` leaves the two rules that actually differ — sort-by-name
  vs positional-shard-index, and file-path vs in-memory body — in the callers, which is where
  they are now. The depth is in `PayloadSource` owning enumeration and naming, because that
  is precisely what the checksum's wire order depends on.

## Proposed change

**One new module, `frogdb-replication/src/fullsync/emitter.rs`**, sitting above the codec and
below the session. `fullsync.rs` gains one `pub mod emitter;` line next to the existing
`receiver` and `stager` (`:20-24`) — the emit side finally has the module the land side got
from proposal 30.

```rust
/// What a full-sync payload is made of, and how it is enumerated onto the wire.
/// Owning *both* here is the point: the combined checksum is folded in wire
/// order, so the ordering rule and the payload kind cannot be separated.
pub(crate) enum PayloadSource {
    /// A cut RocksDB checkpoint directory. Bodies are files, named by their file
    /// name, ordered by that name.
    CheckpointDir(PathBuf),
    /// Per-shard dataset blobs from a primary with no RocksDB. Bodies are bytes,
    /// named positionally (`shard-<n>.dataset`), ordered by shard index.
    LiveDataset(Vec<Vec<u8>>),
}

/// Where the emitter reports progress and lifecycle. The session implements this;
/// a test implements it in three lines.
pub(crate) trait EmitObserver {
    fn payload_sized(&self, total_bytes: u64);   // arms sync_total_bytes, zeroes the counter
    fn transfer_starting(&self);                  // Phase::StreamingCheckpoint + sync_started_at
    fn byte_sink(&self) -> &AtomicU64;            // the in-flight progress counter
    fn progress_percent(&self) -> f64;            // for the per-body debug line
}

pub(crate) struct FullSyncEmitter<'a> {
    pub source: PayloadSource,
    pub replication_id: &'a str,
    pub replication_offset: u64,
}

impl FullSyncEmitter<'_> {
    /// Write the whole envelope and return the payload size that was announced
    /// in the trailer — the number the caller records via `record_output`.
    pub(crate) async fn emit<W: AsyncWrite + Unpin>(
        self, w: &mut W, obs: &dyn EmitObserver,
    ) -> io::Result<u64>;
}
```

`emit` is the eleven-step envelope, once: enumerate (kind-specific, inside `PayloadSource`),
size, `payload_sized`, `transfer_starting`, start log, prelude (marker chosen by the source's
kind), body loop, trailer, completion log. `record_output` stays at the **call site** rather
than moving inside, so FM-REPLICATION-063's "recorded once, after `write_metadata` succeeds"
remains a statement about a line the caller can see.

`EmitObserver` is deliberately an **adapter**, not a relocation. `sync_total_bytes`,
`sync_bytes_transferred`, `sync_started_at` and `Phase` stay owned by `ReplicaSession` — they
are session lifecycle, and `progress_percent_is_the_transferred_fraction_of_the_expected_total`
(`:3196-3210`) reaches into those fields directly and must keep working unedited. The
emitter *borrows* the sink; the session keeps the state.

Call-site rewiring in `handle_full` (`:793-868`), which becomes symmetric — **prepare, then
emit** — in both branches:

```rust
let (source, total) = if let Some(rocks) = handler.rocks_store.as_ref().cloned() {
    self.set_phase(Phase::PreparingCheckpoint);
    // unchanged: pre-checkpoint drain (:807-821), create_checkpoint (:824-826),
    // the two failure arms (:829-845), sync_checkpoint_path (:848)
    PayloadSource::CheckpointDir(checkpoint_path)
} else {
    self.set_phase(Phase::PreparingCheckpoint);
    let Some(src) = handler.live_snapshot_source() else { return Err(/* unchanged text */) };
    PayloadSource::LiveDataset(src().await?)
};
let total = FullSyncEmitter { source, replication_id: &replication_id,
                              replication_offset: snapshot_offset }
    .emit(&mut stream, self.as_ref()).await?;
handler.tracker().net_bytes_handle().record_output(total);
```

Three deliberate consequences:

1. **The live path's `set_phase(PreparingCheckpoint)` moves** from `:1034` (after the
   source-presence check) to before it, matching the checkpoint branch. Provably unobservable:
   the prior phase is `Connecting`, and `ReplicaState::from_phase` maps `Connecting` and
   `PreparingCheckpoint` to the same `WaitBgsave` (`info/mod.rs:304`), with `INFO` the only
   external reader.
2. **The `StreamingCheckpoint` transition moves *into* the emitter** for both paths, at
   exactly the position it occupies today: after the payload exists and the total is known,
   before the prelude is written. For the checkpoint path this is a pure relocation of
   `:849-850`; for the live path a pure relocation of `:1040-1041`. This is the one phase
   transition `INFO` can see, and it lands on the same byte boundary as before.
3. **Two logging deltas are resolved by unification, not preserved.** The live path gains the
   per-body `debug!` and the `rate_mbps` field it lacks today. Log fields are not a pinned
   surface (no test and no FM row asserts on them; `transfer_rate_mbps` has its own unit test
   at `:3216`), and the alternative — carrying two log shapes through a parameter — would
   re-import the duplication the module exists to remove.

**Explicitly out of scope for the extraction commit: single-pass hashing.** Fact 2 above makes
it tempting to have the emitter hash while it streams, deleting the second file read and
closing fact 3 structurally. It is the right end state and the emitter makes it a one-line
future change — but it moves the checksum's meaning from *"hash of the file as it is after
sending"* to *"hash of the bytes actually sent"*. Those are identical today and the second is
strictly stronger, which is exactly why it does not belong in a commit whose entire claim is
"nothing changed": strengthening a locked-area invariant is a spec-first change, not a
refactor side effect. It is proposed as a separate, independently reviewable follow-up below.

### Hard acceptance criteria (LOCKED crate)

1. **Golden pin lands first, in its own commit, against today's code.**
   `full_sync_emitter_golden_bytes` in `frogdb-replication`, asserting literal byte arrays for
   both markers — a two-file checkpoint (exercising the name sort) and a two-blob live dataset
   (exercising positional naming) — driven through the *existing* `stream_checkpoint` /
   `stream_live_dataset`. The extraction commit must then show this test passing **unedited**.
   If its diff appears in the extraction PR, the refactor changed the wire and review stops
   there. This layers on `test_checkpoint_codec_golden_bytes` (`fullsync.rs:831`,
   FM-REPLICATION-035), which pins the *grammar* but not the *composition* — file order,
   marker selection, checksum coverage order, and the trailer's `rdb_size`.
2. **Checksum bytes identical, not just "verified".** The golden pins the trailer's 32-byte
   combined checksum as a hex literal, so a reordering that both sides would still agree on is
   still caught.
3. **Write granularity preserved — no buffering.** `full_sync_replays_writes_made_during_handoff`
   (`:2133`, FM-REPLICATION-004's *only* forcing test) and
   `fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` (`:2812`) both stall the
   session mid-payload using a `tokio::io::duplex(32)` / `duplex(64)` pair — they depend on the
   emitter blocking inside the body loop rather than assembling the envelope in memory. Wrapping
   the stream in a `BufWriter` would deadlock or trivialize them. The emitter must write through
   to the caller's stream, exactly as today.
4. **Forcing tests pass unedited.** Every FM-tagged test that touches these paths, all of which
   drive `session.run()` over a duplex pair rather than calling the emitter directly, and are
   therefore insulated from the extraction by construction:

   | test | line | row | what it pins |
   | --- | --- | --- | --- |
   | `run_full_sync_without_rocks_streams_the_live_dataset` | `:2562` | -001 | `$FROGDB_SNAPSHOT` envelope, blob decode, combined checksum, `rdb_size`, trailer id/offset |
   | `full_sync_without_a_live_snapshot_source_fails_the_sync` | `:2707` | -001 | the preflight refusal — must stay before any payload byte |
   | `full_sync_replays_writes_made_during_handoff` | `:2133` | -004 | the handoff window, stalled inside the live body loop |
   | `repl_output_bytes_include_the_full_sync_payload` | `:2650` | -063 | `record_output(total_size)`, once, after the trailer |
   | `fullresync_offset_and_metadata_come_from_live_tracker` | `:3253` | — | trailer id/offset from the live tracker **and** `sync_total_bytes == streamed_bytes` (`:3352-3356`) |
   | `fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` | `:2812` | — | drain-before-cut ordering, checkpoint stream stalled |
   | `fullresync_fails_when_the_pre_checkpoint_drain_fails` | `:2908` | — | drain failure errors the sync before any payload |
   | `run_cleans_up_checkpoint_dir_on_mid_fullsync_drop` | `:2742` | — | `sync_checkpoint_path` cleanup |
   | `progress_percent_is_the_transferred_fraction_of_the_expected_total` | `:3196` | — | reaches `sync_total_bytes` / `sync_bytes_transferred` **directly** — the reason `EmitObserver` is an adapter and not a relocation |

   `every_shard_contributes_its_blob_in_shard_order` and `a_shard_that_cannot_export_fails_the_whole_sync`
   (`replication-runtime/src/export.rs:69`, `:96`) live in the sibling crate and are untouched —
   they force the `LiveSnapshotSource` implementation, not its consumer.
5. **Forcing tests stay in the mutated crate.** All of the above are inline `#[cfg(test)]` tests
   in `frogdb-replication`, so `cargo mutants -p frogdb-replication` sees them; the new emitter
   module and its golden live in the same crate. No forcing test moves to `frogdb-server`.
6. **Spec prose edited in the same commit — and the lint will *not* catch it.** Two Invariant
   cells name the two functions by name:
   - FM-REPLICATION-001 (`:102`): *"without it `stream_live_dataset` serializes the live keyspace"*
   - FM-REPLICATION-063 (`:1479`): *"`stream_checkpoint` and `stream_live_dataset` each record
     `total_size` … once, after `write_metadata` succeeds"*

   Both become false as written once one emitter serves both. `just lint-failure-modes` checks
   only the `Forced by` test-name agreement, and **no test is renamed by this proposal**, so the
   lint stays green over stale prose. This is a human-review item, and the D2 ruling's phrasing
   is the governing precedent: *rows may move file:line citations but not meaning.* Meaning is
   unchanged; the citations must follow the code. No row's Observable, NOT-observable or
   `Forced by` cell changes.
7. **Mutation re-gate.** `just mutants-diff frogdb-replication` before pushing; then full
   `just mutants frogdb-replication` + `just mutants-gate frogdb-replication 0.85`. A full run,
   not just the diff, because the extraction moves mutation targets between functions and the
   diff view would score the new module against nothing.

## Testability improvement

**The interface is the test surface.** Today the emit envelope has no interface, so its test
surface is a socket: every assertion about what the primary puts on the wire has to spawn
`session.run()` over a `tokio::io::duplex` pair, speak enough of the handshake to get to
`+FULLRESYNC`, and then hand-decode the envelope (`drain_live_dataset`, `:1936-1974`, is a
test-side decoder written for exactly this). Four concrete consequences, each verified:

1. **Sender/receiver agreement is maintained by inspection across four sites.** The receive
   side's tests cannot use the sender, so they built two: `encode_checkpoint_body`
   (`connection.rs:852`) and `encode_envelope` (`receiver.rs:73`), each explicitly documented
   as folding the checksum *"the way the sender does"*. Nothing checks that they still do.
   After the extraction both are **deleted** and their callers construct a `FullSyncEmitter`
   over an in-memory `Vec<u8>` — at which point "the receiver parses what the sender emits"
   stops being a claim two comments make and becomes the only way the tests can be written.
   That is two test helpers passing the deletion test as a side effect.
2. **The phase transition nobody pins becomes assertable.** Searching the tree, the only tests
   that touch `Phase::StreamingCheckpoint` are `force_phase_for_test_drives_phase` (`:3014`)
   and `tracker.rs:943`, both of which *set* the phase rather than observing the emitter
   advance it; `server/src/info/sections.rs:1135-1145` pins the `Phase → wait_bgsave/send_bulk`
   mapping but not when the transition happens. So today **nothing fails** if
   `set_phase(StreamingCheckpoint)` moves after the first payload byte, which would render a
   replica as `wait_bgsave` while its payload is already on the wire — precisely the shape
   FM-REPLICATION-060 exists to forbid. With `EmitObserver`, a three-line recording observer
   makes it a unit assertion: *`transfer_starting` is called after `payload_sized` and before
   the first byte reaches the writer.* This is a **new** pin the extraction enables, filed as a
   test the extraction commit adds — not a claim that today is broken.
3. **Emitter-level properties become expressible at all.** With `emit` writing into a
   `Vec<u8>`, these are direct assertions instead of socket choreography: file ordering is
   name-sorted regardless of `read_dir` order (today an untested dependency on
   `sort_by` at `:913`); an empty checkpoint directory emits a well-formed zero-count envelope
   (the codec has `test_checkpoint_codec_zero_file_prelude` at `:868`, the *emitter* has
   nothing); the announced `rdb_size` equals the bytes actually written; the progress counter
   ends at exactly `sync_total_bytes`. A proptest over `PayloadSource::LiveDataset(blobs)`
   asserting `emit` output round-trips through `receive_snapshot`'s parse becomes a
   ten-line test rather than a two-node integration fixture.
4. **The mutation gate gets a better-shaped target.** Two ~50-line async I/O functions
   reachable only through a socket are hard territory for `cargo mutants`; one module whose
   every step is directly asserted by criteria 1-3 is the shape that scores well. Expect the
   crate's score to improve rather than merely hold.

## Risks / scope boundaries vs sibling proposals

### Boundary vs proposal 55 (adopt-full-sync-landing, RC3) — emit side / land side

The two proposals are the two halves of one wire protocol and they must not both claim the
middle. The line is drawn by **direction of travel**:

| unit | owner |
| --- | --- |
| `stream_checkpoint` (`replica_session.rs:888-988`) | **53** |
| `stream_live_dataset` (`replica_session.rs:1018-1104`) | **53** |
| `handle_full`'s payload-preparation branch (`:793-868`) | **53** |
| new `fullsync/emitter.rs` | **53** |
| `receive_snapshot` (`replica/connection.rs:368-451`) | **55** |
| `receive_checkpoint` (`replica/connection.rs:452-…`) | **55** |
| `fullsync/receiver.rs`, `fullsync/stager.rs` | **55** |
| `CheckpointStreamCodec`, `CheckpointChecksum`, `FullSyncMetadata`, `calculate_*`, `stream_file_to_writer`, `receive_to_file`, both markers (`fullsync.rs`) | **neither — shared, unchanged by both** |

Three conflict edges, in ascending order of seriousness:

1. **Textual, trivial.** Both add a `pub mod` line to `fullsync.rs:20-24`. Whichever lands
   second rebases; five-line region.
2. **Shared re-gate.** Both are `frogdb-replication` changes needing the 0.85 gate. Land them
   as a PR chain and run the full gate once at the end of the chain, not twice.
3. **Real, and it dictates order.** 55 will want to delete `encode_checkpoint_body`
   (`connection.rs:852`) and `encode_envelope` (`receiver.rs:73`) — the two hand-rolled senders
   its own tests depend on. Their correct replacement is 53's `FullSyncEmitter`. **Land 53
   first**, and 55's tests consume the real emitter as their fixture. If 55 lands first it must
   *not* invent a third shared sender fixture to unblock itself; it should leave the two
   existing fixtures alone and let 53 delete them.

Neither proposal touches the install-ordering invariant FM-REPLICATION-001 pins on the land
side ("`install_payload` runs before either half of the granted identity is adopted") — that is
55's territory and it is a behavior contract, not a duplication.

### Relationship to the replication-correctness campaign's D2 (authorized `replica_session.rs` restructure)

`.scratch/replication-correctness/PRD.md:533-551` records D2 as **ruled 2026-08-10: full
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
- D2(i) (promotion / feed-gate transitions) and D2(ii) (replica-side PSYNC arm selection,
  `replica/connection.rs:224`) are entirely disjoint from 53.
- **Recommendation: land 53 before D2(iii) begins.** It removes ~190 lines of pure I/O from the
  file D2(iii) must restructure, and `FullSyncEmitter` + `EmitObserver` is already shaped as an
  effect executor with an injected observer — which is the exact interface `step(view, event)
  -> (phase, effects)` needs on the other side of its interpreter. Doing it first makes D2(iii)
  smaller; doing it after means doing it inside D2(iii).
- **Do not run them concurrently.** Two changes restructuring one locked 4574-line file at the
  same time is the shared-tree trap in the memory ledger, and D2's own execution discipline
  ("land (i) and (ii) first as stepping stones, then (iii) as its own issue chain") assumes a
  serial chain. If D2(iii) has already started when this proposal is triaged, **fold 53 into
  the D2 chain as its first stepping stone** rather than landing it in parallel.
- 53 needs no `frogdb-replication` public-surface change (`FullSyncEmitter`, `PayloadSource`
  and `EmitObserver` are all `pub(crate)`), so it cannot constrain D2's API choices.

### Risk — the extraction's whole claim is "nothing changed", and only the golden proves it

The extraction has no behavioral acceptance test of its own; it borrows the nine tests in
criterion 4 plus the new golden. That is why criterion 1 insists the golden lands **in a
separate, earlier commit against the unmodified functions**. A golden written at the same time
as the refactor pins whatever the refactor produced, which is worth nothing.

### Risk — `PayloadSource::LiveDataset(Vec<Vec<u8>>)` holds the whole dataset in memory

It already does: `stream_live_dataset` binds `blobs` at `:1035` and holds it for the duration
of the transfer. Moving the binding into an enum variant changes nothing about peak residency.
Recorded so a reviewer does not read the enum as *introducing* the buffering. (Making the live
path streaming rather than buffered is a genuine improvement and a genuinely separate change —
it would alter the `LiveSnapshotSource` type in `primary/mod.rs:66` and the shard export
protocol in `replication-runtime`, both outside this proposal.)

## Effort

**M**, in three commits with one shared re-gate. The mutation gate, not the code, is the long
pole.

| Step | Scope | Size |
| --- | --- | --- |
| **1 — golden pin** | `full_sync_emitter_golden_bytes` (both markers, literal bytes + literal checksum hex) against today's `stream_checkpoint` / `stream_live_dataset`. No production change. Independently landable and independently valuable: it is the composition-level pin the codec golden never provided, and it is worth having whether or not the extraction is approved. | **S** — ~90 test lines |
| **2 — extraction** | New `fullsync/emitter.rs` (`PayloadSource`, `EmitObserver`, `emit`); `stream_checkpoint` / `stream_live_dataset` deleted; `handle_full` branch rewired to prepare-then-emit; `ReplicaSession` implements `EmitObserver`; the two spec Invariant cells re-cited; the stale `fullsync.rs:14`/`:133-137` SHA256 parenthetical corrected. | **M** — ~200 new (half tests), ~190 deleted |
| **3 — fixture collapse** | Delete `encode_checkpoint_body` (`connection.rs:852-881`) and `encode_envelope` (`receiver.rs:73-101`); both call sites construct a `FullSyncEmitter` over a `Vec<u8>`. Coordinate with proposal 55 — see the boundary above. | **S** — net negative |
| **Re-gate** | `mutants-diff`, then full `mutants` + `mutants-gate frogdb-replication 0.85`. Testbox-class workload. | — |
| *Follow-up (separate proposal or issue, not this one)* | Single-pass hashing: `stream_file_to_writer` returns `(bytes, [u8; 32])` and streams exactly the declared size, mirroring `receive_to_file`. Halves full-sync disk reads and closes fact 3 structurally. Spec-first, because it strengthens what FM-REPLICATION-001's checksum covers. | **S** |

**Independently landable ahead of the refactor:** step 1 (the golden pin) and the doc-comment
correction at `fullsync.rs:14`/`:133-137`. **No hotfix is proposed**, because no defect was
found — facts 2, 3 and 4 in the Problem section are a cost, a fragility held safe by an
argument outside the function, and a stale comment, respectively, and none of them is
reachable as a bug on the current tree.
