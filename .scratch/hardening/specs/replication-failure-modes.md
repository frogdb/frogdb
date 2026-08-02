# Replication — failure modes

Every way FrogDB's full-resync path can fail, refuse, or succeed, one table per mode. This is the
reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope: the full-sync payload path — what a primary puts on the wire when it grants a
`+FULLRESYNC` (`frogdb-replication/src/replica_session.rs`), the envelope and its markers
(`frogdb-replication/src/fullsync.rs`), what the replica accepts, verifies and installs
(`frogdb-replication/src/replica/connection.rs`), the shard-level export/install seam
(`frogdb-core/src/shard/dispatch_replication.rs`), the dataset framing they share
(`frogdb-persistence/src/serialization/dataset.rs`), and the server-side wiring that connects them
(`frogdb-server/src/replication/{export,install}.rs`). Rows stop at what a *client of the replica*
can observe once the sync reports done.

Adjacent specs: the boot-time half of a full sync — installing a staged checkpoint and adopting the
replication id/offset it carries — lives in
[Persistence — failure modes](persistence-failure-modes.md) (FM-PERSISTENCE-027, -038, -039).

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the link in this mode. |
| Observable | What a client of the replica sees: its keyspace, its `INFO replication`, the outcome of the sync. |
| NOT observable | What must never appear in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Outcome variant | The enum variant / metric label the mode reports through, or `n/a` for wire-level invariants with no client-visible outcome. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-REPLICATION-NNN` tag at its definition site; `just lint-failure-modes` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in
`scripts/failure-modes.py` (`NEXTEST_CRATES`).

---

## FM-REPLICATION-001 — a granted full resync always carries the primary's dataset

| Field | Value |
|---|---|
| Trigger | A replica sends `PSYNC ? -1` (first attach, or a reconnect whose offset the primary cannot serve from the backlog) and the primary grants `+FULLRESYNC`. The mode that matters is the primary running with `persistence.enabled = false`: there is no RocksDB to checkpoint, which is the configuration the turmoil sims and every embedded/cache deployment use. Sharpened by a replica that already holds a *divergent* keyspace — its own forked keys, or stale values for keys the primary has since changed. |
| Observable | The replica's keyspace after the sync is exactly the primary's: the primary's keys and values, its TTLs, and **none** of the replica's own. `DBSIZE` matches, keys that exist only on the replica are gone, and post-sync writes stream on top. On the wire the payload is a `$FROGDB_SNAPSHOT` envelope — one `shard-<n>.dataset` blob per primary shard, then the `FullSyncMetadata` trailer — the same shape as the `$FROGDB_CHECKPOINT` envelope, differing only in the marker and in what the blobs contain. |
| NOT observable | **A replica reporting `master_link_status:up` with a stale keyspace** — the whole bug (issue 67): the old minimal-RDB branch sent an empty envelope carrying no dataset, and the replica adopted the new replid/offset, flipped to `Streaming`, and kept serving its pre-sync keys forever. Nor any of its near misses: a payload marker other than `FROGDB_CHECKPOINT`/`FROGDB_SNAPSHOT` being accepted; a corrupted or truncated dataset being installed; a dataset arriving with no installer wired and the sync still reporting success; the granted offset being adopted before the dataset is installed. |
| Invariant | The primary has exactly two honest payloads and no third: with RocksDB it checkpoints, without it `stream_live_dataset` serializes the *live* keyspace (Redis' `repl-diskless-sync` parity — "no persistence configured" never means "no dataset on the wire"). Both are refused rather than faked when they cannot be produced: a failed checkpoint cut errors the sync, and an unwired `live_snapshot_source` errors it too, because dropping the connection costs one reconnect backoff while a data-less payload is silently permanent. The replica enforces the same rule from its side — `psync` rejects any marker it cannot install, so an old primary's minimal RDB fails the sync instead of being mistaken for one. Ordering: `psync` rewinds the offset to 0 on `+FULLRESYNC` instead of adopting the granted one, `install_payload` runs before the offset is adopted, and an install failure rewinds to 0 again — so every failure lands on "ask for a full resync again", never on "stream deltas onto a keyspace that never took the base snapshot". The trailer's combined checksum is folded blob-by-blob in wire order under positional names (`shard-<n>.dataset`), so a reordered, dropped, truncated or corrupted blob fails verification before it reaches the installer. |
| Outcome variant | `SyncType::{FullSyncCheckpoint, FullSyncSnapshot}`; `INFO replication` `master_link_status` |
| Forced by | `run_full_sync_without_rocks_streams_the_live_dataset`, `full_sync_without_a_live_snapshot_source_fails_the_sync`, `receive_snapshot_installs_the_dataset_before_adopting_offset`, `receive_snapshot_without_an_installer_fails_the_sync`, `receive_snapshot_rejects_a_corrupted_dataset`, `psync_rejects_a_payload_that_carries_no_dataset`, `test_full_resync_from_a_persistence_disabled_primary_transfers_the_dataset` |
| Bug refs | `.scratch/testing-improvements/issues/67` (fixed — this row is its outcome); `.scratch/testing-improvements/issues/61` (the live-install seam this reuses) |

Deliberate non-guarantees, so a future reader does not mistake them for gaps:

* **The live dataset is read shard-by-shard, not from one instant.** Each shard serializes its own
  keyspace when its `ExportSnapshot` message reaches it, so the blobs are not a single atomic cut
  across shards. This does not lose writes: `snapshot_offset` is captured *before* the export
  (FM-REPLICATION-004), so anything written during the export is either already in a blob or is
  replayed from the backlog at the streaming handoff. Cross-shard *visibility* during the export is
  the same granularity every other cross-shard operation has.
* **The shipped offset is durable only on the replica's next state save.** A crash between install
  and that save leaves the state file naming the previous offset; the replica reconnects from there
  and re-applies a tail it already holds, which is idempotent for the replicated write stream. Same
  window the checkpoint path has between install and metadata consumption
  (FM-PERSISTENCE-039).
* **A live-dataset sync is not resumable.** There is nothing staged on disk, so a connection that
  dies mid-payload starts over. The checkpoint path is no different in practice — the staged dir is
  discarded unless it committed.

## FM-REPLICATION-002 — a shard exports its live keyspace, and an install replaces it wholesale

| Field | Value |
|---|---|
| Trigger | `ReplicationMsg::ExportSnapshot` on a shard serving a live-dataset full resync, and `ReplicationMsg::InstallSnapshot` on the receiving side — including the edge shapes: an empty shard, a shard holding logically-expired-but-not-yet-reaped keys, and an install whose routed slice for some shard is empty. |
| Observable | The blob round-trips: installing a shard's export into another shard reproduces its keys, values and TTLs. An empty shard exports an empty blob. Installing into a shard clears whatever it held first, so keys absent from the payload are gone; the `WATCH` version bumps so in-flight watchers abort. |
| NOT observable | Expired keys resurfacing on the replica as live data — a logically-dead key that the reaper has not reached is not part of the keyspace and must not be shipped. A partial export presented as complete: a value that has been spilled out of memory has nowhere to be read back from on a node with no RocksDB, so it errors the export rather than being skipped. An install that merges into the existing keyspace instead of replacing it (which would leave the replica's forked keys behind — the FM-REPLICATION-001 bug arriving by another route). A no-op install that still bumps the watch version. |
| Invariant | `export_snapshot` walks `all_keys()` and drops anything whose metadata is missing or `is_expired()`, and returns `Err` — never a short blob — when a key's value is not hot. `install_snapshot` clears then restores inside the shard's own task with no `.await` between, so the swap is atomic *per shard*: a client sees the whole old keyspace or the whole new one for that shard, never a mixture. The clear routes through the canonical write-effect pipeline as a synthetic `FLUSHDB`, inheriting its WATCH bump, tracking invalidation and WAL range tombstone while staying silent on keyspace notifications (adopting a dataset is not a stream of user writes). A dirty delta of `-1` is reserved for the genuinely empty install so it does not bump watchers. |
| Outcome variant | `ReplicationMsg::{ExportSnapshot, InstallSnapshot}` |
| Forced by | `export_snapshot_round_trips_through_install`, `export_of_an_empty_shard_is_an_empty_blob`, `export_snapshot_drops_expired_keys`, `install_snapshot_replaces_the_live_keyspace`, `install_snapshot_bumps_the_watch_version`, `install_empty_snapshot_clears_the_shard`, `install_empty_snapshot_into_empty_shard_is_a_no_op` |
| Bug refs | `.scratch/testing-improvements/issues/67`, `.scratch/testing-improvements/issues/61` |

## FM-REPLICATION-003 — a dataset blob decodes to exactly what was encoded, or fails whole

| Field | Value |
|---|---|
| Trigger | Decoding a `shard-<n>.dataset` blob on the receiving node: a well-formed blob, an empty one, a blob truncated by a short read, and a blob whose entry payload is corrupt but whose length prefixes still parse. |
| Observable | Round-trip equality for keys, values and expiry, including a zero-length key. An empty blob decodes to no entries — the legitimate encoding of an empty shard, not an error. Anything malformed is a `SerializationError`. |
| NOT observable | A truncated blob decoding to the entries that happened to fit — a short read would silently drop the tail of a shard's keyspace, and the replica would install a subset while reporting a completed sync. A corrupt entry being skipped so the rest of the blob still installs: partial success here is indistinguishable from data loss. |
| Invariant | The framing is `[key_len u32 LE][key][entry_len u32 LE][entry]` repeated to the end of the blob, with no entry count to disagree with the bytes — the blob's own length is the terminator, which is why an empty blob is unambiguous. `take_chunk` treats a length prefix that overruns the remaining bytes as `Truncated`, and `read_entries` propagates the first error rather than accumulating what parsed, so a blob is all-or-nothing. Verification runs earlier still: the envelope's combined checksum (FM-REPLICATION-001) rejects a damaged blob before decoding is attempted, making this layer the second line rather than the only one. |
| Outcome variant | `SerializationError::{Truncated, …}` |
| Forced by | `blob_round_trips_keys_values_and_expiry`, `empty_blob_decodes_to_no_entries`, `truncated_blob_is_an_error`, `corrupt_entry_payload_fails_the_blob` |
| Bug refs | `.scratch/testing-improvements/issues/67` |

## FM-REPLICATION-004 — the granted offset never runs ahead of the payload

| Field | Value |
|---|---|
| Trigger | Writes landing on the primary while a full sync is being prepared and streamed — the window between granting `+FULLRESYNC <replid> <offset>` and the replica switching to the live tail. |
| Observable | The replica ends the sync holding the payload's dataset *and* every write made during the handoff: the writes in `(snapshot_offset, current]` are replayed from the backlog before the live tail. |
| NOT observable | An acknowledged write that is in neither the payload nor the replayed range — the shape that appears when the offset is captured *after* the cut (`offset > data`), so the replica believes it holds writes that were never shipped and no reconnect will ever ask for them again. |
| Invariant | `snapshot_offset` is read from the tracker **before** the checkpoint is cut or the live dataset exported, and the same value is used for both the FULLRESYNC reply and the trailer, so the safe direction is structural: writes landing after the capture only *add* data, giving `offset <= data`. On the checkpoint path the pre-checkpoint hook drains the shard WALs into RocksDB first, so an acknowledged-but-unflushed write cannot be missed; the live-dataset path needs no drain because the shards *are* the source it reads. `start_streaming` subscribes to the broadcast before replaying `(snapshot_offset, current]`, so the replayed range and the live tail cannot leave a gap between them. Redis parity: the FULLRESYNC offset is the `master_repl_offset` captured at fork time. |
| Outcome variant | n/a (wire-level invariant; surfaces as a missing write on the replica) |
| Forced by | `full_sync_replays_writes_made_during_handoff` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (the WAL-drain half — a full-resync checkpoint cut before the shard WALs were drained lost acknowledged writes) |

## FM-REPLICATION-005 — the receive→stream handoff loses no byte of the live tail

| Field | Value |
|---|---|
| Trigger | The primary streaming live WAL frames while the replica is still reading the full-sync payload — the normal case, because `start_streaming` begins the moment the payload is written, so the trailer and the first frames routinely arrive in the same TCP segment. Sharpened by load: the slower the transfer (a large checkpoint, a contended host), the more frames are already queued behind the trailer. Both payload shapes are exposed, checkpoint and live dataset. |
| Observable | Streaming resumes on the byte after the trailer: every frame the primary sent during the transfer is decoded, applied and ACKed, so the replica's offset converges on the primary's and `WAIT` is satisfiable without a reconnect. A sync whose payload was followed by nothing on the wire starts streaming from an empty decoder. |
| NOT observable | **A replica whose offset is permanently short of the primary's stream position while it reports `master_link_status:up`** — the whole bug (issue 01): the frames that shared the trailer's read were dropped with the payload reader, so they were never decoded, never applied and never ACKed. Nor its consequences: `WAIT` structurally unsatisfiable against a replica the primary considers online; a fixed byte-count divergence that only a reconnect (which re-syncs from the replica's short offset) can heal; frames re-decoded or double-counted because the residual was handed over more than once. |
| Invariant | The payload paths never wrap the socket themselves: [`PayloadReader`] owns the `BufReader`, and its `Drop` moves whatever the buffering read past the payload into the connection's `pending_stream_bytes` — on every exit path, including the `?` returns of a failed sync. `stream_replication` *seeds* its decode buffer with `take_pending_stream_bytes()` instead of starting empty, and drains the seeded bytes **before** its first socket read, because they can already hold whole frames the primary is waiting to see ACKed. The hand-back is take-once (`mem::take`), so a residual cannot be replayed. A third payload shape inherits all of this by construction: `payload_reader()` is the only sanctioned way to buffer this socket (`stream` says so at its declaration), and it carries the hand-back with it. |
| Outcome variant | n/a (wire-level invariant; surfaces as a stalled replica offset and an unsatisfiable `WAIT`) |
| Forced by | `receive_checkpoint_streams_the_frames_that_trailed_the_payload`, `receive_snapshot_streams_the_frames_that_trailed_the_payload`, `a_payload_with_no_trailing_frames_leaves_the_stream_empty`, `dropping_the_reader_hands_back_what_it_read_past_the_payload`, `a_fully_consumed_reader_leaves_no_residual` |
| Bug refs | `.scratch/hardening/issues` (issue 01 — surfaced as a load-dependent `test_broadcast_lag_disconnect_and_resync` flake; the seed write was acked by neither replica because the ACK never arrived) |

[`PayloadReader`]: ../../../frogdb-server/crates/replication/src/replica/payload_reader.rs

---

## Redis deviations

Deliberate or known differences from Redis 8.x replication semantics on this path. Each is pinned
by the tests named above, so a change here is a visible spec edit rather than a silent drift.

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| FM-REPLICATION-001 | The full-sync payload is a FrogDB envelope (`$FROGDB_CHECKPOINT` / `$FROGDB_SNAPSHOT`, per-file headers, a `FullSyncMetadata` trailer), never an RDB | `$<length>\r\n<RDB payload>`, or the diskless `$EOF:<40-byte delimiter>` framing | FrogDB has no RDB writer, and its on-disk form is RocksDB. The trailer carries the replid/offset that Redis puts in RDB aux fields, which is what couples offset durability to snapshot durability (FM-PERSISTENCE-039). The cost is that only FrogDB replicas can sync from a FrogDB primary — `redis-cli --rdb` and a real Redis replica cannot. |
| FM-REPLICATION-001 | A primary that cannot produce its dataset (failed checkpoint cut, no live-snapshot source) drops the connection and lets the replica retry | Redis retries the RDB save and, in diskless mode, will kill the transfer, but a `bgsave` failure still leaves the replica waiting rather than the link failing outright | Failing loudly is the point of this row: the alternative FrogDB actually shipped — putting *something* on the wire — is issue 67. One reconnect backoff is cheap; a silently stale replica is not. |
| FM-REPLICATION-001 | A persistence-disabled primary serializes its live keyspace from the shard tasks, with no fork and no child process | `repl-diskless-sync` forks and serializes the fork's memory to the socket | Same guarantee, different mechanism: FrogDB's shards are the source of truth and are single-threaded, so an export is a message, not a memory snapshot. The absence of a fork is why the export is per-shard rather than one instant (see the non-guarantees above). |
| FM-REPLICATION-002 | Installing a dataset emits no keyspace notifications | Redis' replica-side `FLUSHALL` before loading an RDB is likewise silent | Matched deliberately; noted because the install *does* route through the `FLUSHDB` effect pipeline, and the suppression is that command's `EventSpec`, not a special case. |
