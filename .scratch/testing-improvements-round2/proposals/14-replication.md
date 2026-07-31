# Replication — testing gap audit (round 2)

## Scope

Audited (read-only, no builds run):

| path | src LOC | notes |
|---|---|---|
| `frogdb-server/crates/replication/` | ~9.6k | 17 inline `#[cfg(test)]` modules, **no `tests/` dir** |
| `frogdb-server/crates/server/src/replication/` | 363 | `executor.rs` (135), `install.rs` (228) |
| `frogdb-server/crates/server/src/replication_quorum.rs` | 373 | 8 inline tests |
| `frogdb-server/crates/server/src/recovery/` | ~440 + 415 test LOC | |

Line coverage for crate `replication`: **4853/5200 = 93.3%** (regions 8543/9133 = 93.5%).

Depth classes across the audited surface, deduplicated by `(file, line_start)` keeping the
monomorphization with the highest `test_count` and dropping `::<_>` placeholders — **593 real
functions**:

| class | count |
|---|---|
| `well-covered` (≥5 tests, ≥2 suites) | 218 |
| `single-test` | 238 |
| `monoculture` (>1 test, 1 suite) | 73 |
| `untested` | 52 |
| `covered` | 12 |

`untested` is **not** the signal here — 40 of the 52 are accessors, `Default` impls and
`Display`. The signal is that `single-test` + `monoculture` = **311 of 593 (52%)**, and the
concentration is on the failure paths: `replica_session.rs` (38 `single-test`, 10
`monoculture`, 9 `untested`), `replica/connection.rs` (9 `single-test`, 5 `untested` — all
five are error closures), `fullsync.rs` (27 `single-test`, 5 `untested` — all five are
`FullSyncMetadata::from_bytes` parse-error arms), `server/src/replication/executor.rs`
(5 `untested`, 52.9% lines).

**Coverage-data caveat (important).** The 2026-07-28 snapshot **predates** issue 61's
`server/src/replication/install.rs` and the exec-slot-revalidation landing:
`install.rs` appears in neither `depth.json` nor `lcov.info`, and the test names
`test_runtime_full_resync_installs_snapshot_into_live_store`,
`test_promoted_node_via_replicaof_no_one_rejects_downstream_psync` and
`test_self_fence_multi_partial_queue_aborts_whole_transaction` are absent from `tests.json`
while their same-file siblings are present. Treat "no depth data for `install.rs`" as
staleness, not as zero coverage. Findings below never rest on `install.rs` depth data.

## Summary

The replication crate is *broadly* well covered and round 1 fixed the loud bugs (handoff
gaps, reconnect loops, sibling-task leaks, lag disconnects). What is left is a specific,
consistent shape: **the happy path of every state transition has a test; the abort of that
same transition does not.** A replica adopts the primary's replication id and offset the
instant `+FULLRESYNC` is *parsed*, before a single byte of the snapshot has arrived, and
only one of four failure modes rewinds it. A partial-sync grant checks the backlog floor at
decision time and re-extracts the tail later with no re-check, so eviction in between
silently truncates the replayed range under a `+CONTINUE` that has already been promised.
The replica-side frame channel, the `MULTI` reconstruction buffer, and the applied-offset
atomic all survive a reconnect with no barrier, so pre-resync frames can be applied on top of
a freshly installed snapshot. The bug that escapes today is therefore not a crash — it is a
replica that reports `master_link_status:up`, acks an offset the primary believes, satisfies
`WAIT`, and is missing writes. Two integration tests assert nothing at all and a third
(`test_writes_during_full_sync_are_not_lost`) asserts only that the *offset* converged, which
is exactly the signal these bugs preserve.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| inline unit, `primary/replay.rs` + `primary/tests.rs` (9 + 21 tests) | grant/deny matrix: `InitialSync`, `ReplidMismatch`, `OffsetAhead`, `BacklogEvicted`, `req == oldest`, `req == current`, disabled | the decision function is genuinely well covered | nothing covers the *interval* between the decision and the replay; no `max_entries` bound test |
| inline unit, `replica_session.rs` (~1000 test LOC) | handoff window via a tiny duplex buffer that blocks the session inside `send_minimal_rdb` (`:1083-1165`); phase transitions; lag policy | the one real concurrency test in the crate | only the **minimal-RDB** path is blocked-and-raced; `stream_checkpoint` (`:496`) has no equivalent, and its closure at `:514` is `untested` |
| inline unit, `replica/connection.rs` (`:365-600`) | `psync_request_args`, checkpoint adopt-after-install, installer-failure rewind | the injected-installer seam is exercised | transfer failure, checksum mismatch, malformed `+FULLRESYNC`, `-ERR` reply, non-numeric offset — every one leaves the adopted identity in place and none is tested (`:182`, `:207`, `:303` all `untested`) |
| inline unit, `apply.rs` (3 tests, `MockApplier`) | txn grouping, `REPLCONF` skip, error surfacing | asserts on a mock, so it is fast and deterministic | `monoculture` (1 suite); never crosses a reconnect; never reaches the real executor |
| inline unit, `offset.rs` (6), `wait_coordinator.rs` (8), `offset_coordinator.rs` (~10), `replication_quorum.rs` (8), `frame.rs` (16) | offset arithmetic, WAIT quorum/solicit/timeout, fence arming, frame codec | genuinely good; `offset.rs` is 100% and includes the reconnect-PSYNC hazard | each function is typically reached by exactly one of these, so a refactor that breaks two invariants at once fails one test |
| `server/tests/integration_replication.rs` (7210 LOC, 119 tests, ~101 fixed sleeps) | end-to-end PSYNC, WAIT, fencing, NOREPLICAS, promotion, issue-32 expiry contract | breadth is excellent; round-1 tests are sharply asserted | 2 tests with **zero** assertions, several that compute a verification counter and only `eprintln!` it; sleep-based synchronisation throughout |
| `server/tests/simulation.rs` (turmoil, `real_frogdb_primary`/`real_frogdb_replica`) | SPOP deterministic propagation convergence; `test_replication_failover_wgl_linearizable` | real multi-node determinism with a WGL verdict; the harness for a primary+replica pair already exists | pinned to the durable subset (issue 66); failback is forced toward the boot primary because a promoted node cannot serve PSYNC (blocked on the promotion PRD); `:4556` notes in a comment that "WAIT acks cover the offset, not the replica's apply loop" and works around it by polling |
| `crates/testing/` | `fault_injection` (history-level: drop/duplicate/reorder), `partition`, `history`/`checker`, models | good for post-hoc history mangling | fault primitives operate on recorded histories, **not** on a live replication link; no primitive evicts a backlog, stalls a checkpoint transfer, or skews a clock |
| `testing/jepsen/` | failover-durability register workload | the only harness that catches a window computed one byte too wide | nightly tier, not per-PR |

## Findings

### F1: A replica adopts the primary's replid + offset before any snapshot byte arrives, and only one of four failure modes rewinds it

- **Severity** 5 — the replica advertises an identity and offset for a dataset it never
  received. The next reconnect is granted `+CONTINUE` and streams deltas onto the *old*
  keyspace: permanent, silent divergence with `master_link_status:up`.
- **Likelihood** 4 — any interrupted full resync. Checkpoint transfers are the long pole of
  replication; a network blip, a primary restart mid-transfer, or a disk-full on the staging
  dir all land here.
- **Effort** 2 — the crate already has a scripted fake stream and an injectable installer;
  this is a table of scripted responses.
- **Priority** 21
- **Evidence**: `frogdb-server/crates/replication/src/replica/connection.rs:191-192` —
  `self.state.write().await.replication_id = new_repl_id.clone(); self.offsets.reset_to(new_offset);`
  runs immediately after parsing `+FULLRESYNC`, *before* `receive_rdb` / `receive_checkpoint`.
  The only rewind is `:329` (`self.offsets.reset_to(0)`) inside `install_staged_checkpoint`,
  reached only when the injected installer returns `Err`. A failure in
  `receive_checkpoint_files` (`:294`, transport) or `stager.commit` (`:298`, checksum
  mismatch) returns `Err` with the adopted pair intact, and a missing installer (`:319-326`)
  returns `Ok(())` and then adopts the staged offset over an untouched keyspace. The
  doc-comment at `:270-280` asserts the opposite ("The offset is adopted only after the
  install succeeds") — it describes `receive_checkpoint`'s second adopt and is blind to the
  first. `reset_to` writes the handler-owned shared atomic (`replica/offset.rs:53-55`), so
  the adoption survives the reconnect. Coverage: `connection.rs:182` and `:207` (the two
  `psync` error closures) are `untested`; the three existing checkpoint tests
  (`:522`, `:546`, `:588`) all take the success or installer-failure path.
- **Proposed test**: table-driven — for each of {EOF mid-file-transfer, checksum mismatch,
  malformed `+FULLRESYNC` (2 fields), non-numeric offset, `-ERR` PSYNC reply} assert that
  after the failed attempt `ReplicaOffset::current()` and `ReplicationState::replication_id`
  are **unchanged from before the attempt**, so `psync_request_args` produces `PSYNC ? -1`
  (or the pre-attempt pair) on the next connect — never the primary's freshly minted pair.
- **Boundary**: 2 (crate-level API) — the behaviour is entirely inside `ReplicaConnection`
  against an injected stream + installer; a server would add a socket and a real RocksDB
  checkpoint without exercising anything extra.

### F2: Backlog eviction between the PSYNC grant and the tail re-extraction silently truncates the replayed range

- **Severity** 5 — the primary has already written `+CONTINUE` (or `+FULLRESYNC <offset>`)
  when it discovers nothing. `extract_backlog` just returns a shorter vector, the session
  seeds `resume_offset` from the last frame it *did* send, and the live tail dedups against
  that. The replica is permanently missing the evicted range, its offset is contiguous, and
  `WAIT` converges.
- **Likelihood** 4 — the window in `handle_full` spans the whole RocksDB checkpoint creation
  (`spawn_blocking`) *and* the file transfer. At the default 10 000-entry backlog, a
  multi-gigabyte checkpoint on a busy primary evicts the entire window routinely.
- **Effort** 2 — drivable at the crate level with a small backlog and a blocking stream, the
  same technique `replica_session.rs:1083-1165` already uses.
- **Priority** 21
- **Evidence**: the lower-bound check lives in `primary/replay.rs:198-212`
  (`Some(oldest) if req_offset >= oldest => Ok(())`), executed at grant time.
  `replica_session.rs:652` then calls `handler.replay.extract_backlog(replay_from, current)`
  with **no lower-bound re-check**; for the full-sync path `replay_from` is the
  `snapshot_offset` captured at `:438`, and `start_streaming` is only reached at `:488`.
  `ring_buffer.rs:113-117` `debug_assert!`s that the returned tail is offset-*ordered* but
  never that it starts contiguously from `start`, so the truncation is invisible even in
  debug builds. `ring_buffer.rs:106-112` filters `offset > start && offset <= end` — a
  wholly-evicted range yields an empty vector, indistinguishable from "nothing to replay".
- **Proposed test**: (a) unit — `extract_backlog(start, end)` on a buffer whose
  `oldest_offset() > start` must not return a silently short tail; assert it either errors or
  the caller re-checks (a `debug_assert!(tail.first().is_none_or(|f| f.0 > start))` paired
  with `oldest_offset() <= start`). (b) session-level — grant a partial sync at offset X,
  push enough entries to evict past X while the session is blocked in the checkpoint stream,
  then let `start_streaming` run; assert the replica receives every offset in `(X, current]`
  or the session aborts the resume (forcing a fresh full sync) rather than streaming a hole.
- **Boundary**: 2 — the race is between two crate-internal calls; a socket adds nothing.
- **OPTIONS**:
  - *Level 1* (`ReplicationRingBuffer` alone): cheapest, fully deterministic, but only pins
    the invariant on the data structure — it cannot prove `ReplicaSession` honours it.
  - *Level 2* (`ReplicaSession` + fake stream + tiny backlog): reproduces the actual
    ordering, still deterministic. **Recommended**, together with the level-1 assertion.
  - *Level 4* (server integration with `replication_split_brain_buffer_size: Some(8)` and a
    large dataset): closest to production but timing-dependent and slow. Worth one case
    later; not the primary vehicle.

### F3: Frames queued from the pre-resync connection are applied on top of a freshly installed full-resync snapshot

- **Severity** 5 — stale writes at offsets *below* the snapshot offset land after the
  install, resurrecting deleted keys and reverting newer values. Nothing detects it: the
  frames are well-formed and the offset accounting is already reset.
- **Likelihood** 3 — needs a reconnect that escalates to a full resync while frames are still
  queued. The channel is 10 000 deep, and the apply loop is the slow side (it round-trips
  through shard workers), so a non-empty queue at disconnect is the common case, not the rare
  one.
- **Effort** 2 — crate-level with the existing fake connect factory and a `MockApplier`.
- **Priority** 19
- **Evidence**: `replica/mod.rs:123` — `let (frame_tx, frame_rx) = mpsc::channel(10000);` is
  created **once per handler**; `consume_frames(frame_rx, ...)` is spawned once
  (`server/src/role_manager.rs:505`, `server/src/server/replication_init.rs:223`). The
  reconnect loop reuses `self.frame_tx` on every attempt (`replica/mod.rs:327`), and
  `connect_and_sync` (`:295-331`) performs `receive_checkpoint` — which installs into the
  live shards — with the consumer running concurrently. There is no drain, no barrier, and
  no generation/epoch stamp on `ReplicationFrame` (`frame.rs:252-277`: version, flags,
  shard_id, sequence, payload only), so the consumer cannot tell a pre-resync frame from a
  post-resync one.
- **Proposed test**: drive `ReplicaReplicationHandler` with a scripted stream: deliver frames
  at offsets 100..200, force a disconnect, answer the reconnect with `+FULLRESYNC <replid>
  500` and a checkpoint, and assert **no frame with `sequence <= 500` is passed to
  `apply_group` after the installer closure has been invoked**.
- **Boundary**: 2 — a mock applier can record the install/apply interleaving exactly; a real
  server can only observe the resulting keyspace, which is flakier and less diagnostic.

### F4: RESP payloads between 64 MB and 512 MB are accepted by the connection layer but cannot cross the replication link

- **Severity** 5 — the primary commits the write and emits a frame the replica's decoder
  rejects. The stream drops, the replica reconnects, the same frame is re-sent from the
  backlog, and the link never recovers — a write-accepted / never-replicated wedge.
- **Likelihood** 3 — values in that band are unusual but explicitly inside the documented
  limit FrogDB advertises (Redis parity: 512 MB); blob and serialized-document workloads hit
  it.
- **Effort** 2 — the unit half is trivial; the end-to-end half is one integration test.
- **Priority** 19
- **Evidence**: `server/src/connection/codec.rs:36` — `const PROTO_MAX_BULK_LEN: i64 = 512 *
  1024 * 1024;` enforced at `:176` and `:277`. `replication/src/frame.rs:186` — `pub const
  MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;`, referenced **only** at `:315` (`decode`) and
  `:434` (`Decoder::decode`), i.e. exclusively on the receiving side. `frame.rs:287` encodes
  the length as `buf.put_u32(self.payload.len() as u32)` with no bound check and no
  saturating guard. No test in `frame.rs:499-780` uses a payload anywhere near the limit; the
  largest integration case, `test_large_value_replication`
  (`server/tests/integration_replication.rs:937`), carries a single assertion and a modest
  value.
- **Proposed test**: (a) unit — `ReplicationFrame::new(0, payload_of(MAX_FRAME_SIZE + 1))`
  must be rejected at encode time (or the codec must fragment), and round-trip
  `encode`→`decode` must hold at exactly `MAX_FRAME_SIZE` and `MAX_FRAME_SIZE - 1`. (b)
  integration — primary + replica, `SET k <70 MB value>`, assert the replica's `GET k` length
  matches and `master_link_status` is still `up` 2 s later.
- **Boundary**: 1 for the encode/decode symmetry (pure codec arithmetic — a socket would only
  make it slow), 4 for the end-to-end wedge, which genuinely needs two processes and a real
  link.
- **OPTIONS**:
  - *Level 1 only*: catches the asymmetry, misses the reconnect-storm consequence. Fast.
  - *Level 1 + one level-4 case*: **recommended** — the unit test is the regression guard, the
    integration test is the one-time proof the wedge exists.
  - *Property test over payload size*: overkill; the interesting behaviour is a single
    threshold, not a distribution.

### F5: The replica's `MULTI` reconstruction buffer is never reset across a reconnect or resync

- **Severity** 5 — a disconnect between a `MULTI` frame and its `EXEC` leaves `pending =
  Some(..)`. Every command that arrives after the resync is absorbed into that stale group
  and carries the *stale* `shard_id` captured at the old `MULTI`, so at the next `EXEC` an
  arbitrary batch of unrelated commands is applied atomically on the wrong shard — or is
  discarded outright when the next `MULTI` resets the group.
- **Likelihood** 3 — needs a disconnect inside a transaction group. Multi-key writes framed
  as `MULTI…EXEC` are the normal shape for cross-key commands, so on a busy primary the
  window is small but constantly re-entered.
- **Effort** 1 — the existing `MockApplier` harness is enough; no new infrastructure.
- **Priority** 19
- **Evidence**: `replication/src/apply.rs:111` — `let mut pending: Option<PendingTxn> = None;`
  is scoped to the whole `consume_frames` task, which outlives every connection (see F3's
  lifetime evidence). The only mutations are `"MULTI"` (`:164`, which warns "Nested MULTI in
  replication stream; resetting group" and *replaces* the group) and `"EXEC"` (`:176`,
  `pending.take()`). There is no reset hook, and nothing in the frame protocol signals a
  resync boundary. Coverage: the three tests are `monoculture` (1 suite) and all feed a
  single uninterrupted stream (`:299-302`).
- **Proposed test**: feed `MULTI`(shard 3), `SET a`, `SET b` — no `EXEC` — then the frames a
  post-resync stream would deliver (`SET c` on shard 1, `SET d` on shard 5); assert each is
  delivered to `apply_group` as its own single-command group on its own tagged shard, and
  that nothing is ever applied on shard 3. **This test fails today** — it documents a real
  bug whose fix is a reset signal on the channel, not just a missing assertion.
- **Boundary**: 1 — `consume_frames` is a pure loop over a channel with an injected applier;
  every higher level would only obscure which group went to which shard.

### F6: `ReplicaCommandExecutor::apply_transaction` is entirely untested

- **Severity** 4 — this is the only production path that applies a replicated
  `MULTI…EXEC` group. If `TransactionResult::WatchAborted` or `::Error` is mishandled, a
  transaction is silently half-applied or reported clean while it diverged.
- **Likelihood** 4 — every replicated multi-key write goes through it on every replica.
- **Effort** 2 — the shard-worker seam already exists; this is a crate/server-level test that
  hands a real group to the real executor.
- **Priority** 18
- **Evidence**: `depth.json` — `server/src/replication/executor.rs:86`
  (`apply_transaction`) and its closures at `:90` (**26 regions**), `:102` and `:106` are all
  class `untested` with `test_count: 0` in every monomorphization; `:70`
  (`apply_single::{closure#0}`) is likewise `untested`. File line coverage 52.9%. The three
  `apply.rs` tests exercise grouping against a `MockApplier` and never reach this type, so the
  `CoreMsg::ExecTransaction` dispatch and its `TransactionResult` mapping have no test at all.
- **Proposed test**: through the real executor against real shard workers: (a) a 3-command
  group applies all three and returns `Ok`; (b) a group the shard answers with
  `TransactionResult::WatchAborted` returns `Err(ApplyError)` and leaves the keyspace
  untouched; (c) `TransactionResult::Error` likewise. Assert on the keyspace, not on a mock.
- **Boundary**: 3 (`shard_driver` harness) — real dispatch and a real shard worker are
  exactly what is untested; a socket and the connection layer add nothing, and a mock would
  re-test what `apply.rs` already covers.

### F7: Replicas ACK on frame receipt, not on apply, so `WAIT` overstates durability by up to 10 000 commands

- **Severity** 4 — `WAIT N t` returning `N` is the primary's durability primitive. It
  currently means "N replicas have the bytes in a queue", not "N replicas have applied them"
  and certainly not "N replicas have persisted them" (`state.rs::save()` is
  `fs::write(tmp)` + `fs::rename` with **no fsync**). A replica killed right after a
  successful `WAIT` loses everything still queued.
- **Likelihood** 4 — this is the default configuration; any crash of an acking replica
  exposes it.
- **Effort** 3 — needs a two-node scenario with a kill at a controlled point; turmoil already
  hosts `real_frogdb_primary`/`real_frogdb_replica`.
- **Priority** 17
- **Evidence**: `replication/src/replica/streaming.rs:33` advances the offset *then* queues
  the frame *then* ACKs (`let offset = self.offsets.frame_advance(&frame); ... frame_tx.send(frame) ...; if solicited { self.send_ack(offset) }`),
  with a spontaneous tick branch ACKing `self.offsets.current()` unconditionally. The queue is
  `mpsc::channel(10000)` (`replica/mod.rs:123`). `server/tests/simulation.rs:4556` already
  documents the consequence in a comment — "WAIT acks cover the offset, not the replica's
  apply loop, so poll" — and works around it rather than asserting it. No test asserts what
  `WAIT` means.
- **Proposed test**: pin the contract explicitly, whichever way it is decided. Either (a)
  after `WAIT 1 t` returns 1, kill the replica process, restart it, and assert the
  WAIT-confirmed key is readable — which asserts real durability and fails today; or (b) a
  test that asserts and documents the weaker receipt-only contract plus an INFO field
  exposing apply-lag, so operators are not misled.
- **Boundary**: 4 (turmoil) — the property is about a crash at a precise point in a two-node
  stream; the deterministic simulator is the only place that is not flaky.
- **OPTIONS**:
  - *Pin the current contract* (level 3, server integration): cheap, honest, no behaviour
    change; leaves `WAIT` weaker than Redis.
  - *Assert apply-durability* (level 4, turmoil): the correct contract, fails today, forces
    the ACK to move behind the apply loop. **Recommended** as the target, with the level-3
    pin landing first so the current behaviour is at least documented.
  - *Jepsen register workload with kills* (level 5): highest fidelity, nightly tier only.

### F8: Two zero-assertion replication tests and three that compute a verification counter and only print it

- **Severity** 3 — no production consequence directly, but these tests advertise coverage of
  exactly the properties F2/F3/F7 break. A total replication failure passes them.
- **Likelihood** 4 — they run on every CI cycle and have presumably been green through every
  bug round 1 found.
- **Effort** 1 — turn existing computed counters into assertions; delete one duplicate.
- **Priority** 16
- **Evidence** (all `server/tests/integration_replication.rs`):
  - `:2400 test_replica_lag_behavior` — 47 lines, writes 1000 keys, calls `WAIT 1 5000`,
    samples 5 keys into `replicated_count`, and ends with
    `eprintln!("Replicated {} of {} sampled keys…")`. **Zero assertions.**
  - `:1013 test_replica_read_only` — 35 lines, `if is_error(&response) { eprintln!("Replica
    correctly rejected write") } else { eprintln!("Note: Replica accepted write (read-only
    mode may not be enforced yet)") }`. **Zero assertions**, and superseded by
    `test_replica_readonly_error` (`:3460`).
  - `:2984 test_fullresync_interrupted_resume` — ~130 lines; computes `initial_verified` and
    `additional_verified` over sampled keys and `eprintln!`s both. The only assertion is
    `PING == PONG`. It also drops the replica and starts a **fresh** one (`replica2`,
    `:3030`), so despite its name it never exercises same-replica resume.
  - `:5591 test_writes_during_full_sync_are_not_lost` — asserts that `WAIT 1 2000` eventually
    returns ≥1 and that one *post*-sync key (`after_sync`) replicates. It never reads a single
    `during{i}` key off the replica. Offset convergence is precisely the signal F2 and F7
    preserve while data is missing, so this test cannot detect the loss it is named for.
- **Proposed test**: assert `replicated_count == sample_keys.len()`; assert
  `initial_verified == sample.len() && additional_verified == sample.len()`; add a
  same-replica resume arm (restart the *same* replica data dir rather than spawning a new
  one) to `test_fullresync_interrupted_resume`; in `test_writes_during_full_sync_are_not_lost`
  read back **every** `during{i}` key on the replica and assert value equality; delete
  `test_replica_read_only`.
- **Boundary**: 4 — these are already at the right level; only the assertions are missing.

### F9: The full-sync handoff window is raced only on the minimal-RDB path, never on the checkpoint path

- **Severity** 4 — the checkpoint path is the *production* full-sync path (persistence
  enabled); the minimal-RDB path is the degenerate one. A write broadcast during a checkpoint
  transfer that is neither in the snapshot nor in the replayed tail is silently lost.
- **Likelihood** 3 — the window is wide (checkpoint creation + multi-file transfer) and open
  on every new replica attach.
- **Effort** 2 — clone the existing technique onto the other branch.
- **Priority** 16
- **Evidence**: `replication/src/replica_session.rs:1083-1165` — the one test that broadcasts
  *during* the handoff does so "while the session is blocked in `send_minimal_rdb`" using a
  tiny duplex buffer. The checkpoint branch, `stream_checkpoint` (`:496`, invoked at `:465`),
  has no equivalent test and its inner closure at `:514` is class `untested`. Both branches
  converge on `start_streaming(stream, handler, snapshot_offset)` at `:488`, so the window is
  identical in shape and strictly longer in duration.
- **Proposed test**: same shape as `:1083`, blocking inside `stream_checkpoint`: broadcast
  N commands while the transfer is stalled, release, and assert the replica's received frames
  cover `(snapshot_offset, current]` **exactly once each and in offset order** — no gap
  (F2) and no duplicate (the `frame.sequence <= resume_offset` dedup at `:710`).
- **Boundary**: 2 — the crate already owns the blocking-stream trick; a real RocksDB
  checkpoint would add minutes of runtime and no new coverage.

### F10: Replicas expire independently; a `PERSIST` or TTL extension issued after the replica has already reaped diverges permanently

- **Severity** 4 — the primary holds the key, the replica does not, and no subsequent
  replicated command re-creates it (`PERSIST` on a missing key is a no-op on the replica).
  This survives until the next full resync, and is baked in as data loss if the replica is
  promoted.
- **Likelihood** 3 — needs the replica to reap first (replication lag, backlog replay, or
  wall-clock skew) and then a TTL-lengthening command. Session/lock refresh patterns
  (`SET k v EX n` re-issued, `PERSIST`) do exactly this.
- **Effort** 3 — needs a controlled lag window between primary and replica.
- **Priority** 15
- **Evidence**: `core/src/shard/event_loop.rs:133` — `run_active_expiry` is gated only on
  `expiry_paused` (CLIENT PAUSE) and `debug_active_expire_disabled`; there is **no
  `is_replica` check**. `rg is_replica core/src` returns only `builder.rs`, `worker.rs`,
  `types.rs`, `command.rs` (context field) and `scripting/gate.rs` — the flag never reaches
  the expiry path, and `active_expiry.rs` names only `Store` + a clock. Round 1's issue-32
  test (`integration_replication.rs:3603
  test_replica_expires_independently_not_via_del`) deliberately *pins* this divergence, but
  only for the case where the key dies on both sides: it asserts a bounded stale-read window
  and eventual convergence, and explicitly tolerates either outcome mid-window (`:3684-3699`).
  The asymmetric case — replica reaps, primary then extends — is not covered.
- **Proposed test**: `SET k v PX 1500` on the primary; stall the link (or hold the replica's
  apply loop) so the replica's own clock reaps `k` while the primary still holds it;
  `PERSIST k` on the primary; heal; assert `GET k` on the replica returns `v` (and that
  `INFO keyspace` agrees on both nodes). Second arm: promote the replica after the drift and
  assert no WAIT-confirmed key was lost.
- **Boundary**: 4 — requires two nodes with a controllable link; the divergence is a
  cross-node property and cannot be observed at level 3.

### F11: `split_brain_buffer_size = 0` spins forever inside `ReplicationRingBuffer::push` while holding the buffer mutex

- **Severity** 4 — an unkillable spin on the broadcasting path with a `parking_lot::Mutex`
  held. Not corruption, but a hard hang on the first replicated write.
- **Likelihood** 2 — requires an operator to set the value to 0, which reads like a natural
  way to disable the buffer (the correct switch is `split_brain_log_enabled = false`).
- **Effort** 1 — one unit test plus one config-validation test.
- **Priority** 15
- **Evidence**: `replication/src/primary/ring_buffer.rs:58-66` —
  `while entries.len() >= self.max_entries || (…) { if let Some(evicted) = entries.pop_front() { … } }`.
  With `max_entries == 0` the guard is `0 >= 0` (true), `pop_front()` returns `None`, the body
  is a no-op, and the loop never terminates. `config/src/replication.rs:251-300`
  (`ReplicationConfig::validate`) bounds `ack_interval_ms`, `connect_timeout_ms`,
  `handshake_timeout_ms`, `reconnect_backoff_initial_ms` and `replica_freshness_timeout_ms`
  but **never** `split_brain_buffer_size`. The two existing `max_entries: 0` tests
  (`primary/tests.rs:359`, `:407`) both set `enabled: false`, and `PartialSyncReplay::record`
  (`primary/replay.rs:111-114`) gates the push on `self.enabled`, so neither reaches `push`.
  `ring_buffer.rs:19` (`SplitBrainBufferConfig::default`) is itself `untested`.
- **Proposed test**: `ReplicationRingBuffer::new(0, 1024).push(1, 0, b"x".into())` must
  terminate (a `#[test]` with an explicit timeout, or an assertion that the buffer is empty
  after the call); and `ReplicationConfig { split_brain_buffer_size: 0, split_brain_log_enabled: true, .. }.validate()` must
  return `Err`.
- **Boundary**: 1 — a pure data-structure bound and a pure config predicate.

### F12: A `MULTI` queued while replicas are healthy and `EXEC`'d after they drop bypasses both the self-fence and the `min-replicas` gate

- **Severity** 4 — writes commit on a primary that has lost its replicas, which is exactly
  the state both gates exist to prevent. On a subsequent promotion elsewhere they are lost.
- **Likelihood** 3 — a replica dropping between `MULTI` and `EXEC` is an ordinary failover /
  network-blip event, and transaction bodies are not instantaneous.
- **Effort** 3 — pair, queue, kill, EXEC.
- **Priority** 15
- **Evidence**: `server/src/connection/guards.rs:316-322` names the gap in a comment — "the
  narrow window where a MULTI is queued while replicas are healthy and then EXEC'd after they
  drop are NOT gated here — a bound shared with self-fence, tracked as a follow-up". Round 1
  covered the *other* halves: `test_min_replicas_to_write_multi_and_lua_paths`
  (`integration_replication.rs:6350`) starts from **zero** replicas, so it only exercises
  queue-time rejection + `EXECABORT`, and `test_self_fence_does_not_gate_lua_writes` pins the
  Lua half. The healthy-then-dropped ordering is untested on both gates.
- **Proposed test**: pair with `min-replicas-to-write 1`; `MULTI`, queue `SET k v` (accepted,
  replica healthy), kill the replica, `EXEC`; assert the pinned outcome — currently the write
  commits, which should be asserted explicitly so any fix is a deliberate, visible change.
  Repeat with `self_fence_on_replica_loss = true`.
- **Boundary**: 4 — needs a real replica whose disappearance the primary observes; the gate
  reads the live tracker.

### F13: PSYNC and full-sync-metadata error paths are uniformly untested

- **Severity** 3 — malformed or hostile primary responses are mishandled quietly; combined
  with F1 several of them leave adopted identity behind. Individually these are wrong-error /
  wrong-state bugs rather than data loss.
- **Likelihood** 3 — rolling upgrades across protocol versions, a truncated response from a
  primary that is shutting down, and a corrupt staged metadata file are all ordinary events.
- **Effort** 1 — table-driven scripted responses through the existing fake stream.
- **Priority** 14
- **Evidence** (`depth.json`, class `untested`, `test_count: 0`):
  `replication/src/replica/connection.rs:182` and `:207` (the `parse` error closures inside
  `psync`), `:303` (`read_ok_response`), `:47` (`read_resp_line` monomorphization);
  `replication/src/fullsync.rs:66`, `:78`, `:80`, `:90` (**all four** error arms of
  `FullSyncMetadata::from_bytes`) and `:253` (`CheckpointStreamCodec::read_file_header`);
  `replication/src/frame.rs:370` (`From<io::Error> for FrameDecodeError`). The only negative
  transport test that exists is `fullsync/receiver.rs:144`
  (`receiver_truncated_stream_yields_unexpected_eof`).
- **Proposed test**: one table each. For `psync`: `{"+FULLRESYNC abc", "+FULLRESYNC abc xyz",
  "-ERR loading", "garbage", "+CONTINUE"}` → assert the exact `io::ErrorKind` and (tying to
  F1) that replid/offset are unchanged. For `FullSyncMetadata::from_bytes`: truncated,
  wrong-field-count, non-numeric offset, bad checksum length → assert each distinct error.
- **Boundary**: 2 — pure parsing plus one injected stream; these are the cheapest
  high-value tests in this proposal.

### F14: Wall-clock skew between primary and replica can silently drop the working set at full-resync install time

- **Severity** 4 — a replica whose clock runs ahead discards every key whose absolute expiry
  falls inside the skew, immediately after adopting the snapshot offset. The replica reports
  a healthy link and a matching offset with a truncated dataset.
- **Likelihood** 2 — needs meaningful clock skew (NTP outage, VM resume, container clock
  drift) plus short TTLs.
- **Effort** 3 — needs a clock-injection seam that does not exist on the install path today.
- **Priority** 13
- **Evidence**: `persistence/src/serialization/mod.rs:93` persists expiry as an absolute Unix
  millisecond stamp (`metadata.expires_at.map(instant_to_unix_ms).unwrap_or(-1)`), which is
  correct across processes but is interpreted against the *receiver's* clock.
  `server/src/replication/install.rs:197` drops warm-tier entries whose `expires_at <= now` at
  read time, and the shard restore path applies the same predicate. There is no test on the
  replication side that installs a checkpoint containing near-now TTLs, and no clock seam in
  `LiveCheckpointInstaller` or `read_snapshot`.
- **Proposed test**: install a staged checkpoint whose entries expire at `now + 50 ms` under
  an injected clock offset by +5 s; assert either that the keys survive (with the skew
  reported) or that the drop is counted and surfaced in INFO — silently dropping them is the
  behaviour to prevent.
- **Boundary**: 3 — the install is a server-side component with an injectable installer seam;
  a full two-node scenario cannot control the clocks any better and costs far more.
- **OPTIONS**:
  - *Level 1* on the expiry predicate alone: cheapest, but the predicate is already trivially
    correct — the bug is that nothing bounds the skew at install time.
  - *Level 3* with a clock seam added to `LiveCheckpointInstaller`: **recommended**;
    the seam is small and pays for itself in the persistence area too.
  - *Level 4/5* turmoil with a skewed host clock: turmoil controls time but not per-host wall
    clock offsets today; would need a new primitive. Not worth it for this.

## Deprioritised

- **Promoted node cannot serve PSYNC to a surviving sibling.** Real and important, but
  **blocked on pending rework**: `.scratch/replication-cluster-rework/promotion-replid-psync.md`
  §4.4–4.5 and §7 already specify the full test plan (backlog floor, `clear_secondary_window`,
  replid rotation, the chain test, the turmoil variant). `new_replication_id`
  (`replication/src/state.rs:248`) still has **no production caller** — confirmed by `rg`:
  only tests at `replay.rs:327`, `state.rs:434/440/469/472`, `offset_coordinator.rs:345` and
  two doc comments. Any test asserting replid rotation on `REPLICAOF NO ONE` cannot pass
  today. `test_promoted_node_via_replicaof_no_one_serves_downstream_psync`
  (`integration_replication.rs:1875`) accepts `FULLRESYNC || CONTINUE` for exactly this
  reason; tightening it is part of that PRD, not of this round.
- **Minimal-RDB full sync carries no dataset** — issue 66 is open with two candidate
  resolutions; a test asserting either would prejudge the decision. The turmoil failover WGL
  suite is already pinned to the durable subset because of it.
- **Cluster-mode data replication is inert** — `.scratch/replication-cluster-rework/wait-cluster-mode.md`.
  Out of my scope (cluster agent) and blocked.
- **`oldest_offset()` returns the front entry's *end* offset, not a first-byte floor.**
  Divergent from Redis's `repl_backlog_off` but conservative in the safe direction (it can
  only force an unnecessary full resync, never grant a `+CONTINUE` over missing data), and
  the promotion PRD §4.5 folds this arm away entirely. Not worth pinning behaviour that is
  scheduled to change.
- **`min_replicas_timeout_ms == 0` counts every streaming replica as "good"**
  (`tracker.rs:157-162`, `max_lag.is_zero() || …`). Matches Redis's `min-slaves-max-lag 0`
  semantics; a test would pin parity, not catch a bug.
- **`ReplicationState::save()` has no fsync** (`state.rs`). Real durability gap, but it
  belongs to the persistence agent's crash-consistency story, and testing it needs the
  crash-injection harness that already lives there. Noted in Cross-area.
- **Non-deterministic write propagation** (`SPOP`, served blocking pops, expiry). Genuinely
  well covered: `core/src/shard/post_execution.rs` has a `debug_assert` that *panics* if a
  non-deterministic write ships without a `ReplicationOverride` (`:103`), plus rewrite /
  suppression / in-MULTI-substitution tests (`:1197-1420`), and issues 01/02 added turmoil
  convergence runs. No residue found.
- **`install.rs` cross-shard atomicity** — documented as per-shard, not cross-shard
  (`install.rs:86-97`), with the argument that the install completes before the offset is
  adopted so no *replicated* write can observe the half-installed state. The reasoning holds;
  the observable gap is a client reading the replica mid-install, which the doc names
  explicitly as an accepted trade-off.
- **Sleep-based synchronisation in `integration_replication.rs`** (~101 fixed sleeps across
  7210 lines). Flakiness risk and slow, but converting them to polling is mechanical churn
  across 119 tests; better handled as a single dedicated cleanup than as a testing-gap
  finding.

## Cross-area notes

- **Core engine agent** — F10 lands in `core/src/shard/event_loop.rs:133` /
  `core/src/shard/active_expiry.rs`: active expiry is entirely role-agnostic and `is_replica`
  never reaches it. Whether replicas should stop reaping on their own clock is a core
  decision with a replication-visible consequence; the fix (if any) is theirs, the test is
  cross-node and belongs here.
- **Persistence agent** — `ReplicationState::save()` writes tmp + rename with **no fsync**
  (`replication/src/state.rs`). Combined with F7 (ACK on receipt) this means a
  `WAIT`-confirmed offset can be lost on a replica power-cut. Their crash-injection harness is
  the right place to prove it. Also relevant to F14: `instant_to_unix_ms` round-tripping and
  the "drop entries whose `expires_at <= now`" predicate are shared with boot recovery.
- **Server net/connection agent** — F4 is a boundary mismatch between two crates:
  `PROTO_MAX_BULK_LEN` (512 MB, `server/src/connection/codec.rs:36`) and `MAX_FRAME_SIZE`
  (64 MB, `replication/src/frame.rs:186`). Whichever side is wrong, the constants should be
  related explicitly rather than coincidentally. Note `cluster/src/network.rs:77` defines its
  own independent 64 MB copy — three unrelated ceilings for the same byte stream.
- **Cluster agent** — `replication_quorum.rs::reset_arming` is called from
  `role_manager.rs::demote()`; the arming-latch lifecycle across promote/demote cycles is
  jointly owned. I did not test it here.
- **Shared infrastructure needed** (in rough priority order):
  1. A **resync-boundary signal** on the replica frame channel — required to test F3 and F5,
     and required to *fix* them. Today the applier cannot distinguish a pre-resync frame from
     a post-resync one; a generation counter on `ReplicationFrame` or an explicit
     `Barrier` message would give both the fix and the test hook.
  2. A **live-link fault primitive** in `crates/testing/`. The current `fault_injection`
     module mangles recorded histories after the fact; nothing can stall a checkpoint
     transfer, evict a backlog, or delay an ACK on a running link. F2, F7, F9 and F10 all
     want one, and the turmoil hosts (`real_frogdb_primary` / `real_frogdb_replica`) are the
     natural place to hang it.
  3. A **clock seam** on the checkpoint install path (F14), shared with persistence.
