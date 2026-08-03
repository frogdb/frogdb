# Replication — failure modes

Every way FrogDB's replication link can fail, refuse, or succeed, one table per mode. This is the
reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope, part one — the full-sync payload path (FM-REPLICATION-001..005): what a primary puts on the
wire when it grants a `+FULLRESYNC` (`frogdb-replication/src/replica_session.rs`), the envelope and
its markers (`frogdb-replication/src/fullsync.rs`), what the replica accepts, verifies and installs
(`frogdb-replication/src/replica/connection.rs`), the shard-level export/install seam
(`frogdb-core/src/shard/dispatch_replication.rs`), the dataset framing they share
(`frogdb-persistence/src/serialization/dataset.rs`), and the server-side wiring that connects them
(`frogdb-replication-runtime/src/{export,install}.rs`). Rows stop at what a *client of the replica*
can observe once the sync reports done.

Scope, part two — the steady-state link (FM-REPLICATION-006..): what the replica does with the
frames a sync hands it. The decode loop and the ACK it answers with
(`frogdb-replication/src/replica/streaming.rs`), the received/applied offset pair and the gate that
guards it (`frogdb-replication/src/replica/offset.rs`), the frame consumer that applies them
(`frogdb-replication/src/apply.rs`) through the executor seam
(`frogdb-replication-runtime/src/executor.rs`), and the primary-side backlog the link resumes from
(`frogdb-replication/src/primary/{ring_buffer,replay}.rs`). Rows stop at what a client of *either*
node can observe: the replica's keyspace and `INFO replication`, and the primary's `WAIT`.

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
| NOT observable | **A replica reporting `master_link_status:up` with a stale keyspace** — the whole bug (issue 67): the old minimal-RDB branch sent an empty envelope carrying no dataset, and the replica adopted the new replid/offset, flipped to `Streaming`, and kept serving its pre-sync keys forever. Nor any of its near misses: a payload marker other than `FROGDB_CHECKPOINT`/`FROGDB_SNAPSHOT` being accepted; a corrupted or truncated dataset being installed; a dataset arriving with no installer wired and the sync still reporting success; the granted *identity* — replid or offset — being adopted before the dataset is installed, which would leave a node that never received the payload advertising a history it cannot serve and having discarded the failover window (`secondary_id`/`secondary_offset`) describing the keyspace it is still holding. |
| Invariant | The primary has exactly two honest payloads and no third: with RocksDB it checkpoints, without it `stream_live_dataset` serializes the *live* keyspace (Redis' `repl-diskless-sync` parity — "no persistence configured" never means "no dataset on the wire"). Both are refused rather than faked when they cannot be produced: a failed checkpoint cut errors the sync, and an unwired `live_snapshot_source` errors it too, because dropping the connection costs one reconnect backoff while a data-less payload is silently permanent. The replica enforces the same rule from its side — `psync` rejects any marker it cannot install, so an old primary's minimal RDB fails the sync instead of being mistaken for one. Ordering: on `+FULLRESYNC` `psync` adopts *neither* half of the granted pair — it rewinds the offset to 0 and leaves the replication id and failover window alone, so a promise that goes unkept costs nothing; the id is taken from the payload's own trailer, `install_payload` runs before either is adopted, and an install failure rewinds to 0 again — so every failure lands on "ask for a full resync again", never on "stream deltas onto a keyspace that never took the base snapshot". The trailer's combined checksum is folded blob-by-blob in wire order under positional names (`shard-<n>.dataset`), so a reordered, dropped, truncated or corrupted blob fails verification before it reaches the installer. |
| Outcome variant | `SyncType::{FullSyncCheckpoint, FullSyncSnapshot}`; `INFO replication` `master_link_status` |
| Forced by | `run_full_sync_without_rocks_streams_the_live_dataset`, `full_sync_without_a_live_snapshot_source_fails_the_sync`, `receive_snapshot_installs_the_dataset_before_adopting_offset`, `receive_snapshot_without_an_installer_fails_the_sync`, `receive_snapshot_rejects_a_corrupted_dataset`, `psync_rejects_a_payload_that_carries_no_dataset`, `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone`, `a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone`, `test_full_resync_from_a_persistence_disabled_primary_transfers_the_dataset` |
| Bug refs | `.scratch/testing-improvements/issues/67` (fixed — this row is its outcome); `.scratch/testing-improvements/issues/61` (the live-install seam this reuses); `.scratch/testing-improvements-round2/issues/51` (the granted identity adopted before the payload — fixed) |

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

## FM-REPLICATION-006 — owing an ACK never stops the replica reading its link

| Field | Value |
|---|---|
| Trigger | A `REPLCONF GETACK` (what `WAIT` sends) reaching a replica whose applier is behind the received head — frames still queued on the 10k frame channel, a shard slow to apply, or a consumer that has stopped for good because a promotion froze its stint. Sharpened by the primary continuing to stream behind the GETACK, which is the normal case: a solicitation is not a barrier, and the frames after it are already in flight. |
| Observable | The frames that arrive behind the GETACK are decoded and queued while the answer is still owed, with no wait for the applier: the received head keeps advancing at socket speed. The spontaneous ACK tick keeps firing on cadence throughout, so a primary counting ACKs keeps seeing this replica's true applied head. The solicited ACK goes out the moment the applier reaches the solicited offset — which covers the GETACK frame itself, as in Redis. |
| NOT observable | **A replica that stops reading its socket because it owes an ACK.** Its consequences: TCP backpressure to the primary, which stalls the shared broadcast and slows *other* replicas' streams; the spontaneous ACK cadence skipping a tick, so the link goes quiet exactly when `WAIT` made it interesting; the solicited answer itself arriving a whole cadence late because the loop that must notice the applier caught up was the loop that was blocked. Nor the opposite escape: an ACK reporting the *received* head to answer without waiting (FM-REPLICATION-008's rule — an ACK is a durability claim). |
| Invariant | The wait is a `select!` branch, not an inline `await`. `drain_frames` only *records* the solicitation (`pending_ack: Option<u64>`, the offset it covers) and returns to the loop, so the socket-read branch and the ACK-tick branch stay pollable for exactly as long as the answer is owed. A GETACK arriving while one is already pending raises the target to the newer, higher offset rather than queueing a second answer: ACKs are cumulative, so one ACK at the newer target answers both. An unanswerable solicitation (the applier stopped for good — a frozen gate, a retired stint) needs no timeout to stay harmless: it parks one branch of the loop and nothing else, while the spontaneous cadence keeps reporting the same applied head the timeout would have made it report. |
| Outcome variant | n/a (wire-level invariant; surfaces as a stalled replica read and a hiccuping ACK cadence) |
| Forced by | `a_solicited_ack_does_not_stall_the_decode_loop`, `a_solicited_ack_is_sent_as_soon_as_the_applier_catches_up`, `a_second_getack_raises_the_target_to_the_newer_offset`, `the_ack_cadence_survives_a_solicitation_that_can_never_be_answered`, `wait_until_applied_returns_as_soon_as_the_applier_catches_up`, `wait_until_applied_parks_when_the_applier_can_no_longer_advance` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 09) |

---

## FM-REPLICATION-007 — frames outlive their connection, never their history

| Field | Value |
|---|---|
| Trigger | A replica link that drops mid-stream, possibly mid-`MULTI`, with frames already decoded onto the 10k-deep frame channel — which, with its consumer, outlives the connection (`ReplicaReplicationHandler::start` reconnects in a loop into the same channel). The retry is then granted `+FULLRESYNC`: a dataset is installed and both heads are reset to the payload's offset. The leftovers now describe a keyspace that no longer exists. |
| Observable | The queued frames from the replaced history are dropped without reaching a shard and without claiming a byte, counted on the consumer's `discarded` shutdown field; an open group among them is abandoned with them. The applied head after the resync is the payload's offset plus exactly the new history's frames. A `+CONTINUE` resume is the opposite case and is left alone: it installs no dataset and resets no head, so its leftovers — including a `MULTI` group split across the reconnect — still apply, and the group closes normally on the `EXEC` that arrives after it. |
| NOT observable | **A frame from a replaced history applied to the installed dataset**, or its bytes credited to the new history's offset (which would make this node ACK — and, once promoted, vouch for — an offset covering data it never held, the FM-REPLICATION-004 hazard arriving from the replica side). **A `MULTI` group straddling a resync**: an old history's half-transaction continued by the new history's commands, or closed by an `EXEC` from the other side of the install, applied on the *old* group's tagged shard. Nor the blunt fixes for either: retiring the stint on resync (which stops the long-lived consumer for good, so the replica silently applies nothing after its first reconnect) or draining/rebuilding the channel per connection. |
| Invariant | Every frame the decode loop queues is stamped with the **history epoch** it was decoded under (`StreamedFrame`), and the epoch is bumped by `reset_pair` only — i.e. exactly when a full resync adopts a new dataset — under the same gate lock that moves the heads. The consumer checks it twice: a cheap pre-check at the top of the loop that drops a stale frame and the group it belonged to, and the authoritative re-check inside `ReplicaApplyStint::claim`, taken under that same lock, which is what makes the check race-free (`Claim::Stale`) against a resync landing on the connection task mid-group. A group is additionally abandoned when the frame in hand is current but the group was opened under an older epoch. Because both the decode loop and the reset run on the connection task, a channel never interleaves epochs: all of one history's frames precede all of the next's. |
| Outcome variant | n/a (internal invariant; surfaces as a diverged replica keyspace and an over-claimed applied offset) |
| Forced by | `a_full_resync_discards_the_frames_queued_from_the_previous_history`, `a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history`, `a_continue_resume_still_applies_the_frames_it_left_queued`, `a_claim_stamped_before_a_resync_is_refused_after_it` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 06) |

---

## FM-REPLICATION-008 — an ACK is a durability claim, not a receipt

| Field | Value |
|---|---|
| Trigger | `WAIT N t` on the primary, which sends `REPLCONF GETACK` and counts the ACKs that come back, against a replica whose applier is behind its socket: frames queued on the 10k-deep frame channel, or one group in flight between the applier's claim and the shard's reply. Sharpened by killing the replica the instant `WAIT` returns, which is the whole point of the primitive. |
| Observable | An acked offset implies **every frame at or below it has been applied to its shard**. The replica ACKs its *landed* head — moved as each `apply_group` returns, and immediately for frames that reach no shard (`REPLCONF`, `FROGDB.FINALIZE`, an unparseable payload) — on both branches: the spontaneous cadence tick and the solicited answer. A group in flight to a shard is inside the promotion boundary (claimed) and outside the ACK (not landed), which is the one-group gap between the two heads. A full resync levels all three heads at the adopted offset: the installed dataset is applied. |
| NOT observable | **`WAIT` satisfied by a replica that has not applied the write** — neither one still holding it in the frame channel (the received head, which a promotion discards down to the applied offset) nor one that has merely claimed it (the claimed head, which is what the promotion boundary needs and is a byte count, not an apply). A `WAIT 1` that returns 1 and is then followed by killing and restarting that replica must not lose the key. Nor the reverse escape: a landed head that runs ahead of the claimed head, or that keeps reporting a previous history's offset after a resync adopted a lower one. |
| Invariant | Three heads, one direction: `landed <= claimed <= received`. `claimed` is `AppliedOffset::current` — taken before the group is dispatched, under the gate, which is what makes the promotion boundary exact (FM-REPLICATION-007's claim path). `landed` is `AppliedOffset::landed`, advanced by `ReplicaApplyStint::land` at every point where nothing is in flight; because the consume loop applies one group at a time and awaits each, "nothing in flight" means `landed` may simply be re-read from `claimed` (a `fetch_max`, so a resync that stored a lower offset in between wins). Only `landed` is read by `send_ack` and by `wait_until_applied`, the two ACK branches. The primary's own writes (`advance_by`) move both, because that path applies first and counts after. |
| Outcome variant | n/a (wire-level invariant; surfaces as `WAIT` returning a count that overstates durability) |
| Forced by | `an_ack_reports_the_landed_head_not_the_claimed_one`, `a_claim_alone_does_not_move_the_offset_the_replica_acks`, `a_group_in_flight_to_its_shard_is_claimed_but_not_yet_ackable`, `a_frame_that_touches_no_shard_lands_as_it_is_claimed`, `a_full_resync_levels_the_landed_head_with_the_adopted_offset`, `test_spop_replication_convergence_random_workload` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 76) |

**Latency.** Moving the ACK behind the apply does not add a round trip, but it does mean a `WAIT`
is answered on the *applier's* schedule rather than the decoder's: the solicited answer is parked as
a `select!` branch and fires the moment the landed head reaches the solicited offset
(FM-REPLICATION-006), while the fallback is the spontaneous cadence
(`replication.ack-interval-ms`, Redis `repl-ping-replica-period`). A replica whose applier is
wedged therefore keeps ACKing its true, lower landed head on cadence instead of a higher receipt —
`WAIT` times out rather than lying. This is the Redis behaviour and the reason its replicas ack
after executing the command stream.

**Not covered here.** ACK means applied, not *persisted*: `ReplicationState::save()` is
`write`+`rename` with no fsync, so a power cut can still lose a landed offset that was acked. That
is a separate contract (issue 76 item 2) and is not claimed by this row.

---

## FM-REPLICATION-009 — the backlog outlives its last replica by a bounded time

| Field | Value |
|---|---|
| Trigger | A primary whose backlog window is armed — by boot recovery at a non-zero offset, or by `begin_primary_stint` at a promotion boundary — and whose replica count is zero and stays zero: a standalone node restarted after it once had a replica, or a primary whose only replica went away for good. Every write is still stamped and buffered, for resume history nobody is waiting for. |
| Observable | After `repl-backlog-ttl` seconds with zero connected replicas the buffer is emptied and the window closed, so the next `PSYNC` is answered `+FULLRESYNC` (`FullResyncReason::BacklogEvicted`). A replica that reconnects *before* the TTL elapses still gets its `+CONTINUE`, and a replica connected the whole time never lets the clock start — a reconnect restarts the window rather than resuming it. `repl-backlog-ttl 0` parks the timer entirely (Redis's disable value), and the knob is live: a `CONFIG SET` retunes an idle window that is already running. |
| NOT observable | **A `+CONTINUE` over history that was freed** — the floor is disarmed in the same call that empties the buffer, so no resume can be granted over the hole. **Any offset or identity movement**: freeing the backlog is not a stint change, so `master_replid`, `master_replid2`, `second_repl_offset`, `master_repl_offset` and the applied head are all exactly what they were; `INFO` reports the same history, only without a resume window. Nor the timer firing repeatedly (once per tick for the rest of the process) once the window has already been freed, nor starting at all on a node that has no window armed. |
| Invariant | The TTL is an idle clock, not a countdown from arming: `BacklogTtl::due` clears its start whenever the replica count is non-zero or the TTL is `0`, sets it on the first tick that finds zero replicas, and fires — clearing itself in the same breath — on the first tick at or past the deadline. `PartialSyncReplay::expire_backlog_if_idle` refuses to start the clock while nothing is armed and, when the clock fires, calls exactly `reset_backlog()`: the ring buffer's `reset` empties the entries and returns `start` to `UNARMED`, which is the same disarm both ends of a primary stint use. The tick itself is the server's 1 Hz maintenance task (Redis frees the backlog from `replicationCron` on the same cadence and for the same reason). |
| Outcome variant | `FullResyncReason::BacklogEvicted` on the next `PSYNC` after a free |
| Forced by | `an_idle_backlog_is_freed_once_its_ttl_elapses`, `a_replica_reconnecting_before_the_ttl_still_resumes`, `a_connected_replica_never_starts_the_ttl_clock`, `a_ttl_of_zero_never_frees_the_backlog`, `a_freed_backlog_full_resyncs_the_next_psync`, `freeing_the_backlog_moves_no_offset_and_no_replication_id`, `an_unarmed_backlog_never_starts_the_ttl_clock`, `the_ttl_fires_once_per_idle_window_not_once_per_tick` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 07) |

---

## FM-REPLICATION-010 — an admitted divergence ends the history it happened on

| Field | Value |
|---|---|
| Trigger | `apply_group` returning `Err` on a replica: a command the primary accepted that this node's shard refuses (a version skew, a shard-side resource failure, a bug), for a bare command or for a `MULTI/EXEC` group. The claim is already taken and cannot be given back without desynchronising every later frame's stream position, so the node now holds a keyspace that does not match the offset it counts. |
| Observable | The failure is **latched on the history it happened on**: every further `claim` on that epoch returns `Claim::Stale`, so later frames are dropped without reaching a shard and the applied head stops moving. `land()` is never reached either, so the ACK keeps reporting the last truly-applied offset and `WAIT` times out rather than lying (FM-REPLICATION-008). The connection task wakes on the latch, logs at `error`, rewinds the received head to 0 and drops the link with an `Err`, so the reconnect asks `PSYNC ? -1` and is answered `+FULLRESYNC` — on the exponential-backoff path, not a 100 ms hot loop. A divergence outstanding when a link is *established* abandons that link before it decodes a byte. Installing the resync payload (`reset_pair`) clears the latch in the same critical section that bumps the epoch, and the applier resumes on the new history. |
| NOT observable | **A diverged replica quietly continuing to apply** the rest of the stream onto a keyspace it has already broken, or ACKing offsets it never applied, or being handed `+CONTINUE` over the hole after a promotion. **Retiring the long-lived frame consumer** — FM-REPLICATION-007 forbids it: the consumer outlives connections, so retiring it makes the replica silently apply nothing for the rest of the process. Nor the reverse escapes: a latch surviving the full resync that replaced the keyspace (which would force a second, pointless resync), a latch admitted against an epoch a resync has *already* replaced, or an unparseable payload being treated as a divergence (it reaches no shard, breaks no keyspace, and is counted and skipped as before). |
| Invariant | The latch is an epoch-keyed cell on `AppliedOffset` (`diverged: AtomicU64`, sentinel `u64::MAX`) plus a `Notify`. `ReplicaApplyStint::admit_divergence(epoch)` takes the gate, compares `epoch` to the live epoch and stores it only if they match — so a resync that landed between the claim and the `Err` wins and the doomed history is simply forgotten. `claim` checks the cell under the same gate, immediately after its epoch check, and returns `Claim::Stale` (not `Retired`: the consumer must stay alive for the next history). `reset_pair` stores the sentinel back under that gate, so the latch clears if and only if a fresh dataset was installed — it survives reconnects, `+CONTINUE`s and promotion/demotion round trips. `AppliedOffset::divergence()` is a re-checking `Notify` wait, safe against a latch that fires before the waiter parks, consumed as a `select!` branch in `stream_replication` and re-checked once before the pre-loop drain. |
| Outcome variant | n/a (internal invariant; surfaces as a dropped link, a forced `+FULLRESYNC`, and a stalled ACK head) |
| Forced by | `a_failed_apply_stops_the_history_it_happened_on`, `a_diverged_applier_resumes_on_the_history_a_resync_installs`, `a_diverged_history_is_refused_until_a_resync_replaces_it`, `a_divergence_on_a_history_a_resync_already_replaced_is_ignored`, `the_divergence_wait_resolves_however_it_races_the_latch`, `an_admitted_divergence_drops_the_link_and_rewinds_for_a_full_resync`, `a_divergence_outstanding_at_connect_abandons_the_new_link_at_once` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 08) |

**Reads during the window.** A diverged replica keeps serving reads until the resync payload lands.
This matches Redis's `replica-serve-stale-data yes` default, and the window here is much tighter than
Redis's: the link is dropped the instant the latch is seen, so the node is already in a forced full
resync rather than waiting out a link timeout. Refusing reads would need a `-MASTERDOWN`-style gate
on the read path and a knob to go with it; that is deliberately not in this row.

**Not covered here.** The remaining over-claim window is a **crash between a claim and its shard
write**, which leaves the persisted offset one group ahead of the data with no `Err` to latch. This
row only covers failures the applier is alive to see. Closing the crash window means persisting the
applied offset from the shard's own write path, and it stays tracked in
`.scratch/replication-cluster-rework/issues` (issue 08 follow-up).

---

## FM-REPLICATION-011 — the link carries every command the connection layer accepted

| Field | Value |
|---|---|
| Trigger | A write whose replicated encoding is larger than one internal frame's old ceiling: a `SET` of a bulk value between 64 MB and 512 MB — inside the limit FrogDB advertises to clients (`proto-max-bulk-len`) and therefore committed on the primary before any replication code sees it. The link is healthy; the only thing wrong is the size. |
| Observable | The write replicates: the replica serves the whole value (not a prefix), `WAIT 1` acks it, and both nodes still report the link — `master_link_status:up` on the replica, `connected_slaves:1` on the primary — after the frame has crossed. |
| NOT observable | **A committed write that cannot be replicated** — the whole bug (issue 69): `encode` cast the payload length with an unchecked `as u32` while `MAX_FRAME_SIZE` (64 MB) was checked on **decode only**, so the primary emitted a frame its replica refused, the link dropped, the backlog re-sent the identical frame on reconnect, and it never recovered. Nor its near misses: a payload silently **truncated** by the length cast and installed as a short value; a frame that encodes but cannot decode (the two ceilings disagreeing in either direction); a ceiling raised past `u32::MAX`, which would make the length field itself lossy and the decode-side sanity check meaningless. |
| Invariant | One derivation, not three coincidences. `frogdb_protocol::PROTO_MAX_BULK_LEN` is the ceiling on user data; `MAX_INTERNAL_FRAME_LEN = 2 * PROTO_MAX_BULK_LEN` is what every internal transport must carry (one accepted command: a maximal bulk plus its command name, key and framing, with one further maximal bulk of allowance — 1 GiB, the number Redis caps a client's accumulated request at with `PROTO_MAX_QUERYBUF_LEN`). Both `frogdb_replication::frame::MAX_FRAME_SIZE` and `frogdb_cluster::network::MAX_FRAME_SIZE` are *defined as* that constant rather than copied, and the cluster bus is in scope because it carries user bytes too (`BusRpc::PubSubBroadcast` / `PubSubForward`). Enforcement is symmetric: `ReplicationFrame::encode` and the tokio `Encoder` both refuse over-sized payloads with `FrameEncodeError::PayloadTooLarge` **before** reserving or casting, so the bound is checked on the side that can still report it, and the decoder's check becomes a sanity check on a peer rather than the only guard. A frame that cannot be encoded fails the replica link loudly (logged at `error`, link dropped) instead of going out short. |
| Outcome variant | `FrameEncodeError::PayloadTooLarge` (surfaced as `io::ErrorKind::InvalidInput`) |
| Forced by | `the_frame_ceiling_is_derived_from_the_resp_bulk_ceiling`, `encode_refuses_a_payload_larger_than_the_frame_ceiling`, `a_payload_over_the_old_ceiling_round_trips_across_the_link`, `a_value_over_the_old_frame_ceiling_replicates_without_wedging_the_link` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 69 — this row is its outcome) |

Deliberate non-guarantees:

* **A replicated encoding above 1 GiB still fails, but loudly.** The ceiling bounds one *frame*, not
  one user value, so a command whose replicated form exceeds it — an `MSET` of several maximal
  bulks, or a synthesized effect such as a `SORT ... STORE` result — is refused at encode instead of
  being truncated. That is a link failure, not silent divergence, and it is the deliberate trade:
  fragmenting frames would put reassembly state on both sides of a path whose whole job is to be
  hard to desynchronise. Redis has the same shape of limit (`PROTO_MAX_QUERYBUF_LEN`) and the same
  answer to exceeding it — kill the connection.
* **The boundary is asserted arithmetically, not by allocating it.** `payload_fits` is checked at
  `0`, `MAX - 1`, `MAX` and `MAX + 1`, and a real encode→decode round trip is done at 64 MiB + 1 —
  above the *old* ceiling, which is the regression that matters. Round-tripping a literal 1 GiB
  payload would allocate several GiB in a test process for no additional coverage of the branch.
* **The connection layer is not tightened to match.** Issue 69's third option — refuse at the client
  so the write is never accepted — was rejected: it would drop FrogDB below the `proto-max-bulk-len`
  it advertises, and Redis parity on what a client may store is the stronger contract. The internal
  transports move up to meet it instead.

---

## FM-REPLICATION-012 — a replay is contiguous from the resume point or it does not happen

| Field | Value |
|---|---|
| Trigger | The backlog window closing between a resume being *granted* and the same resume being *streamed*. The two are far apart: `+CONTINUE` (or `+FULLRESYNC <offset>`) is written first, and for a full sync the entire checkpoint cut and file transfer sits in between — on a busy primary with the default 10 000-entry backlog that is routinely long enough to evict the whole window. Also reached by the backlog TTL firing (FM-REPLICATION-009) or a stint boundary resetting the buffer inside that gap. |
| Observable | The resume is abandoned, not shortened: the link drops, and the replica's reconnect is answered `+FULLRESYNC` (`FullResyncReason::BacklogEvicted`) because the same floor check fails at grant time too. A full sync whose window closed mid-transfer therefore costs a second full sync rather than a silently incomplete keyspace. |
| NOT observable | **A replica streaming from a hole** — the whole bug (issue 52): the eviction check ran only at grant time, `extract_backlog` returned a shorter vector, the session seeded `resume_offset` from the last frame it *did* send, and the live tail deduped against that — so the replica was permanently missing the evicted range while its offset looked contiguous, `master_link_status` stayed `up`, and `WAIT` converged. Nor the near misses: a truncated range reported as an empty tail (indistinguishable from a caught-up replica); a `+CONTINUE` granted from the window as it was *before* the extraction; the ordering invariant being the only thing asserted about the returned tail. |
| Invariant | The window check lives on the extraction, not just on the grant. `ReplicationRingBuffer::extract_backlog` re-reads the floor **under the entries lock** — the same lock `push` holds while it evicts and raises that floor — so the window it checks is the window whose contents it returns, and it answers `Err(BacklogTruncated { requested, floor })` rather than a short `Vec`. Both callers must then decide rather than assume: `handle_partial_sync_request` degrades a grant to `FullResyncReason::BacklogEvicted`, and `ReplicaSession::start_streaming` fails the link with `io::ErrorKind::InvalidData` before writing a single replayed frame. Two ranges need no history and are served regardless: an empty range (`start >= end` — a caught-up replica, and the fresh-primary full sync whose snapshot offset is the head), and a backlog disabled by config, where every reconnect already full-resyncs. |
| Outcome variant | `BacklogTruncated`; `FullResyncReason::BacklogEvicted` on the next `PSYNC` |
| Forced by | `an_evicted_resume_point_is_refused_not_truncated`, `a_closed_window_refuses_every_extraction`, `a_resume_evicted_after_the_grant_is_abandoned_not_truncated`, `a_full_sync_whose_handoff_window_is_evicted_abandons_the_link` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 52 — this row is its outcome) |

**Not covered here.** This row makes a truncated replay *fail*; it does not make the window wider.
A primary whose backlog cannot span its own checkpoint transfer will now full-resync twice instead
of once, and the fix for that is capacity (`repl-backlog-size`), not the streamer. Nor does the row
claim byte-level contiguity *within* the retained range: the floor is the authority, because
eviction raises it to exactly the end offset of the entry it dropped, so "floor `<=` resume point"
is equivalent to "no entry covering the replayed range was dropped".

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
