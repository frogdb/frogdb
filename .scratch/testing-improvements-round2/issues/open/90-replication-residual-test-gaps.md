# replication — residual test gaps (9 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 — residual findings after promotion to issues 19–76
Score: 9 findings, priority range 13–19
Area: `frogdb-server/crates/replication/` + `server/src/replication/` (`executor.rs`, `install.rs`), `server/src/replication_quorum.rs`, `server/src/recovery/`

## Context

This area is the primary/replica link: PSYNC grant and backlog replay, the replica connection
and its full-resync install path, the frame codec, offset accounting, the apply loop, and the
WAIT/fencing quorum machinery. Crate `replication` is **4853/5200 = 93.3%** line coverage
(regions 8543/9133 = 93.5%) across 17 inline `#[cfg(test)]` modules and **no `tests/` dir**;
depth over 593 deduplicated functions gives 218 `well-covered`, 238 `single-test`, 73
`monoculture`, 52 `untested`, 12 `covered` — and `untested` is not the signal, since 40 of the
52 are accessors, `Default` impls and `Display`. The signal is `single-test` + `monoculture` =
**311 of 593 (52%)**, concentrated on failure paths (`replica_session.rs`,
`replica/connection.rs`, `fullsync.rs`, `server/src/replication/executor.rs` at 52.9% lines).
The proposal's verdict on the shape of that coverage: "**the happy path of every state
transition has a test; the abort of that same transition does not**" — so the bug that escapes
is "not a crash — it is a replica that reports `master_link_status:up`, acks an offset the
primary believes, satisfies `WAIT`, and is missing writes."

**Coverage-data caveat carried from the proposal**: the 2026-07-28 snapshot predates issue 61's
`server/src/replication/install.rs` and the exec-slot-revalidation landing, so `install.rs`
appears in neither `depth.json` nor `lcov.info`. Treat "no depth data for `install.rs`" as
staleness, not zero coverage; no finding here rests on `install.rs` depth data.

The proposal's `## Deprioritised` section carries no F-numbers. One of its entries is
nonetheless carried below in `## Residual findings` because `MASTER.md` §8 names it as blocked
rather than proposed. The rest are recorded as deliberately not filed: minimal-RDB full sync
carrying no dataset (round-1 issue 66, `.scratch/testing-improvements/issues/`, still open with
two candidate resolutions — a test would prejudge the decision, and `MASTER.md` §8 notes it may
share a root cause with 09/F5, filed as issue 48,
`.scratch/testing-improvements-round2/issues/`); cluster-mode data replication being inert
(`.scratch/replication-cluster-rework/wait-cluster-mode.md`, cluster agent's scope and blocked);
`oldest_offset()` returning the front entry's *end* offset (conservative in the safe direction
and scheduled to change under the promotion PRD); `min_replicas_timeout_ms == 0` counting every
streaming replica as good (Redis `min-slaves-max-lag 0` parity); `ReplicationState::save()`
having no fsync (routed to persistence, and recorded in this proposal's cross-area notes);
non-deterministic write propagation (genuinely well covered, no residue); `install.rs`
cross-shard atomicity (documented and argued as an accepted trade-off); and the ~101 fixed
sleeps in `integration_replication.rs` (mechanical churn, better handled as one cleanup).

## Promoted elsewhere

- F1 → issue 51, `.scratch/testing-improvements-round2/issues/` (a replica adopts the primary's replid + offset before any snapshot byte arrives, and only one of four failure modes rewinds it).
- F2 → issue 52, `.scratch/testing-improvements-round2/issues/` (backlog eviction between the PSYNC grant and the tail re-extraction silently truncates the replayed range).
- F4 → issue 69, `.scratch/testing-improvements-round2/issues/` (RESP payloads between 64 MB and 512 MB are accepted by the connection layer but cannot cross the replication link — the frame-size mismatch wedges it).
- F7 → issue 76, `.scratch/testing-improvements-round2/issues/` (replicas ACK on frame receipt, not on apply, so `WAIT` overstates durability by up to 10 000 commands).
- F10 → issue 22, `.scratch/testing-improvements-round2/issues/` (theme T4 — replicas expire independently; a `PERSIST` or TTL extension issued after the replica has already reaped diverges permanently).

## Residual findings

### F3 — Frames queued from the pre-resync connection are applied on top of a freshly installed full-resync snapshot

- **Severity** 5 — stale writes at offsets *below* the snapshot offset land after the install, resurrecting deleted keys and reverting newer values. Nothing detects it: the frames are well-formed and the offset accounting is already reset.
- **Likelihood** 3 — needs a reconnect that escalates to a full resync while frames are still queued. The channel is 10 000 deep, and the apply loop is the slow side (it round-trips through shard workers), so a non-empty queue at disconnect is the common case, not the rare one.
- **Effort** 2 — crate-level with the existing fake connect factory and a `MockApplier`.
- **Priority** 19
- **Evidence**: `replica/mod.rs:123` — `let (frame_tx, frame_rx) = mpsc::channel(10000);` is created **once per handler**; `consume_frames(frame_rx, ...)` is spawned once (`server/src/role_manager.rs:505`, `server/src/server/replication_init.rs:223`). The reconnect loop reuses `self.frame_tx` on every attempt (`replica/mod.rs:327`), and `connect_and_sync` (`:295-331`) performs `receive_checkpoint` — which installs into the live shards — with the consumer running concurrently. There is no drain, no barrier, and no generation/epoch stamp on `ReplicationFrame` (`frame.rs:252-277`: version, flags, shard_id, sequence, payload only), so the consumer cannot tell a pre-resync frame from a post-resync one.
- **Proposed test**: drive `ReplicaReplicationHandler` with a scripted stream: deliver frames at offsets 100..200, force a disconnect, answer the reconnect with `+FULLRESYNC <replid> 500` and a checkpoint, and assert **no frame with `sequence <= 500` is passed to `apply_group` after the installer closure has been invoked**.
- **Boundary**: 2 — a mock applier can record the install/apply interleaving exactly; a real server can only observe the resulting keyspace, which is flakier and less diagnostic.

### F5 — The replica's `MULTI` reconstruction buffer is never reset across a reconnect or resync

- **Severity** 5 — a disconnect between a `MULTI` frame and its `EXEC` leaves `pending = Some(..)`. Every command that arrives after the resync is absorbed into that stale group and carries the *stale* `shard_id` captured at the old `MULTI`, so at the next `EXEC` an arbitrary batch of unrelated commands is applied atomically on the wrong shard — or is discarded outright when the next `MULTI` resets the group.
- **Likelihood** 3 — needs a disconnect inside a transaction group. Multi-key writes framed as `MULTI…EXEC` are the normal shape for cross-key commands, so on a busy primary the window is small but constantly re-entered.
- **Effort** 1 — the existing `MockApplier` harness is enough; no new infrastructure.
- **Priority** 19
- **Evidence**: `replication/src/apply.rs:111` — `let mut pending: Option<PendingTxn> = None;` is scoped to the whole `consume_frames` task, which outlives every connection (see F3's lifetime evidence). The only mutations are `"MULTI"` (`:164`, which warns "Nested MULTI in replication stream; resetting group" and *replaces* the group) and `"EXEC"` (`:176`, `pending.take()`). There is no reset hook, and nothing in the frame protocol signals a resync boundary. Coverage: the three tests are `monoculture` (1 suite) and all feed a single uninterrupted stream (`:299-302`).
- **Proposed test**: feed `MULTI`(shard 3), `SET a`, `SET b` — no `EXEC` — then the frames a post-resync stream would deliver (`SET c` on shard 1, `SET d` on shard 5); assert each is delivered to `apply_group` as its own single-command group on its own tagged shard, and that nothing is ever applied on shard 3. **This test fails today** — it documents a real bug whose fix is a reset signal on the channel, not just a missing assertion.
- **Boundary**: 1 — `consume_frames` is a pure loop over a channel with an injected applier; every higher level would only obscure which group went to which shard.

### F6 — `ReplicaCommandExecutor::apply_transaction` is entirely untested

- **Severity** 4 — this is the only production path that applies a replicated `MULTI…EXEC` group. If `TransactionResult::WatchAborted` or `::Error` is mishandled, a transaction is silently half-applied or reported clean while it diverged.
- **Likelihood** 4 — every replicated multi-key write goes through it on every replica.
- **Effort** 2 — the shard-worker seam already exists; this is a crate/server-level test that hands a real group to the real executor.
- **Priority** 18
- **Evidence**: `depth.json` — `server/src/replication/executor.rs:86` (`apply_transaction`) and its closures at `:90` (**26 regions**), `:102` and `:106` are all class `untested` with `test_count: 0` in every monomorphization; `:70` (`apply_single::{closure#0}`) is likewise `untested`. File line coverage 52.9%. The three `apply.rs` tests exercise grouping against a `MockApplier` and never reach this type, so the `CoreMsg::ExecTransaction` dispatch and its `TransactionResult` mapping have no test at all.
- **Proposed test**: through the real executor against real shard workers: (a) a 3-command group applies all three and returns `Ok`; (b) a group the shard answers with `TransactionResult::WatchAborted` returns `Err(ApplyError)` and leaves the keyspace untouched; (c) `TransactionResult::Error` likewise. Assert on the keyspace, not on a mock.
- **Boundary**: 3 (`shard_driver` harness) — real dispatch and a real shard worker are exactly what is untested; a socket and the connection layer add nothing, and a mock would re-test what `apply.rs` already covers.

### F8 — Two zero-assertion replication tests and three that compute a verification counter and only print it

- **Severity** 3 — no production consequence directly, but these tests advertise coverage of exactly the properties F2/F3/F7 break. A total replication failure passes them.
- **Likelihood** 4 — they run on every CI cycle and have presumably been green through every bug round 1 found.
- **Effort** 1 — turn existing computed counters into assertions; delete one duplicate.
- **Priority** 16
- **Evidence** (all `server/tests/integration_replication.rs`):
  - `:2400 test_replica_lag_behavior` — 47 lines, writes 1000 keys, calls `WAIT 1 5000`, samples 5 keys into `replicated_count`, and ends with `eprintln!("Replicated {} of {} sampled keys…")`. **Zero assertions.**
  - `:1013 test_replica_read_only` — 35 lines, `if is_error(&response) { eprintln!("Replica correctly rejected write") } else { eprintln!("Note: Replica accepted write (read-only mode may not be enforced yet)") }`. **Zero assertions**, and superseded by `test_replica_readonly_error` (`:3460`).
  - `:2984 test_fullresync_interrupted_resume` — ~130 lines; computes `initial_verified` and `additional_verified` over sampled keys and `eprintln!`s both. The only assertion is `PING == PONG`. It also drops the replica and starts a **fresh** one (`replica2`, `:3030`), so despite its name it never exercises same-replica resume.
  - `:5591 test_writes_during_full_sync_are_not_lost` — asserts that `WAIT 1 2000` eventually returns ≥1 and that one *post*-sync key (`after_sync`) replicates. It never reads a single `during{i}` key off the replica. Offset convergence is precisely the signal F2 and F7 preserve while data is missing, so this test cannot detect the loss it is named for.
- **Proposed test**: assert `replicated_count == sample_keys.len()`; assert `initial_verified == sample.len() && additional_verified == sample.len()`; add a same-replica resume arm (restart the *same* replica data dir rather than spawning a new one) to `test_fullresync_interrupted_resume`; in `test_writes_during_full_sync_are_not_lost` read back **every** `during{i}` key on the replica and assert value equality; delete `test_replica_read_only`.
- **Boundary**: 4 — these are already at the right level; only the assertions are missing.

Note: these five tests are replication's instance of `MASTER.md` §4. The §4 sweep is issue 33,
`.scratch/testing-improvements-round2/issues/`, but its bullet list cites 04/F3, 12/F4, 13/F11,
13/F18, 11/F14, 07/F15, 08/F10, 03/F2, 04/F2, 15/F13 and 12 — **not** 14/F8 — so this finding
stays here and is not double-owned. Coordinate if the sweep is widened.

### F9 — The full-sync handoff window is raced only on the minimal-RDB path, never on the checkpoint path

- **Severity** 4 — the checkpoint path is the *production* full-sync path (persistence enabled); the minimal-RDB path is the degenerate one. A write broadcast during a checkpoint transfer that is neither in the snapshot nor in the replayed tail is silently lost.
- **Likelihood** 3 — the window is wide (checkpoint creation + multi-file transfer) and open on every new replica attach.
- **Effort** 2 — clone the existing technique onto the other branch.
- **Priority** 16
- **Evidence**: `replication/src/replica_session.rs:1083-1165` — the one test that broadcasts *during* the handoff does so "while the session is blocked in `send_minimal_rdb`" using a tiny duplex buffer. The checkpoint branch, `stream_checkpoint` (`:496`, invoked at `:465`), has no equivalent test and its inner closure at `:514` is class `untested`. Both branches converge on `start_streaming(stream, handler, snapshot_offset)` at `:488`, so the window is identical in shape and strictly longer in duration.
- **Proposed test**: same shape as `:1083`, blocking inside `stream_checkpoint`: broadcast N commands while the transfer is stalled, release, and assert the replica's received frames cover `(snapshot_offset, current]` **exactly once each and in offset order** — no gap (F2) and no duplicate (the `frame.sequence <= resume_offset` dedup at `:710`).
- **Boundary**: 2 — the crate already owns the blocking-stream trick; a real RocksDB checkpoint would add minutes of runtime and no new coverage.

### F11 — `split_brain_buffer_size = 0` spins forever inside `ReplicationRingBuffer::push` while holding the buffer mutex

- **Severity** 4 — an unkillable spin on the broadcasting path with a `parking_lot::Mutex` held. Not corruption, but a hard hang on the first replicated write.
- **Likelihood** 2 — requires an operator to set the value to 0, which reads like a natural way to disable the buffer (the correct switch is `split_brain_log_enabled = false`).
- **Effort** 1 — one unit test plus one config-validation test.
- **Priority** 15
- **Evidence**: `replication/src/primary/ring_buffer.rs:58-66` — `while entries.len() >= self.max_entries || (…) { if let Some(evicted) = entries.pop_front() { … } }`. With `max_entries == 0` the guard is `0 >= 0` (true), `pop_front()` returns `None`, the body is a no-op, and the loop never terminates. `config/src/replication.rs:251-300` (`ReplicationConfig::validate`) bounds `ack_interval_ms`, `connect_timeout_ms`, `handshake_timeout_ms`, `reconnect_backoff_initial_ms` and `replica_freshness_timeout_ms` but **never** `split_brain_buffer_size`. The two existing `max_entries: 0` tests (`primary/tests.rs:359`, `:407`) both set `enabled: false`, and `PartialSyncReplay::record` (`primary/replay.rs:111-114`) gates the push on `self.enabled`, so neither reaches `push`. `ring_buffer.rs:19` (`SplitBrainBufferConfig::default`) is itself `untested`.
- **Proposed test**: `ReplicationRingBuffer::new(0, 1024).push(1, 0, b"x".into())` must terminate (a `#[test]` with an explicit timeout, or an assertion that the buffer is empty after the call); and `ReplicationConfig { split_brain_buffer_size: 0, split_brain_log_enabled: true, .. }.validate()` must return `Err`.
- **Boundary**: 1 — a pure data-structure bound and a pure config predicate.

### F12 — A `MULTI` queued while replicas are healthy and `EXEC`'d after they drop bypasses both the self-fence and the `min-replicas` gate

- **Severity** 4 — writes commit on a primary that has lost its replicas, which is exactly the state both gates exist to prevent. On a subsequent promotion elsewhere they are lost.
- **Likelihood** 3 — a replica dropping between `MULTI` and `EXEC` is an ordinary failover / network-blip event, and transaction bodies are not instantaneous.
- **Effort** 3 — pair, queue, kill, EXEC.
- **Priority** 15
- **Evidence**: `server/src/connection/guards.rs:316-322` names the gap in a comment — "the narrow window where a MULTI is queued while replicas are healthy and then EXEC'd after they drop are NOT gated here — a bound shared with self-fence, tracked as a follow-up". Round 1 covered the *other* halves: `test_min_replicas_to_write_multi_and_lua_paths` (`integration_replication.rs:6350`) starts from **zero** replicas, so it only exercises queue-time rejection + `EXECABORT`, and `test_self_fence_does_not_gate_lua_writes` pins the Lua half. The healthy-then-dropped ordering is untested on both gates.
- **Proposed test**: pair with `min-replicas-to-write 1`; `MULTI`, queue `SET k v` (accepted, replica healthy), kill the replica, `EXEC`; assert the pinned outcome — currently the write commits, which should be asserted explicitly so any fix is a deliberate, visible change. Repeat with `self_fence_on_replica_loss = true`.
- **Boundary**: 4 — needs a real replica whose disappearance the primary observes; the gate reads the live tracker.

### F13 — PSYNC and full-sync-metadata error paths are uniformly untested

- **Severity** 3 — malformed or hostile primary responses are mishandled quietly; combined with F1 several of them leave adopted identity behind. Individually these are wrong-error / wrong-state bugs rather than data loss.
- **Likelihood** 3 — rolling upgrades across protocol versions, a truncated response from a primary that is shutting down, and a corrupt staged metadata file are all ordinary events.
- **Effort** 1 — table-driven scripted responses through the existing fake stream.
- **Priority** 14
- **Evidence** (`depth.json`, class `untested`, `test_count: 0`): `replication/src/replica/connection.rs:182` and `:207` (the `parse` error closures inside `psync`), `:303` (`read_ok_response`), `:47` (`read_resp_line` monomorphization); `replication/src/fullsync.rs:66`, `:78`, `:80`, `:90` (**all four** error arms of `FullSyncMetadata::from_bytes`) and `:253` (`CheckpointStreamCodec::read_file_header`); `replication/src/frame.rs:370` (`From<io::Error> for FrameDecodeError`). The only negative transport test that exists is `fullsync/receiver.rs:144` (`receiver_truncated_stream_yields_unexpected_eof`).
- **Proposed test**: one table each. For `psync`: `{"+FULLRESYNC abc", "+FULLRESYNC abc xyz", "-ERR loading", "garbage", "+CONTINUE"}` → assert the exact `io::ErrorKind` and (tying to F1) that replid/offset are unchanged. For `FullSyncMetadata::from_bytes`: truncated, wrong-field-count, non-numeric offset, bad checksum length → assert each distinct error.
- **Boundary**: 2 — pure parsing plus one injected stream; these are the cheapest high-value tests in this proposal.

Note: the "replid/offset are unchanged" half of this table is the same assertion issue 51,
`.scratch/testing-improvements-round2/issues/` (14/F1) owns. Write the table once; F13's
residue is the *error taxonomy* — the exact `io::ErrorKind` and the distinct
`FullSyncMetadata` errors.

### F14 — Wall-clock skew between primary and replica can silently drop the working set at full-resync install time

- **Severity** 4 — a replica whose clock runs ahead discards every key whose absolute expiry falls inside the skew, immediately after adopting the snapshot offset. The replica reports a healthy link and a matching offset with a truncated dataset.
- **Likelihood** 2 — needs meaningful clock skew (NTP outage, VM resume, container clock drift) plus short TTLs.
- **Effort** 3 — needs a clock-injection seam that does not exist on the install path today.
- **Priority** 13
- **Evidence**: `persistence/src/serialization/mod.rs:93` persists expiry as an absolute Unix millisecond stamp (`metadata.expires_at.map(instant_to_unix_ms).unwrap_or(-1)`), which is correct across processes but is interpreted against the *receiver's* clock. `server/src/replication/install.rs:197` drops warm-tier entries whose `expires_at <= now` at read time, and the shard restore path applies the same predicate. There is no test on the replication side that installs a checkpoint containing near-now TTLs, and no clock seam in `LiveCheckpointInstaller` or `read_snapshot`.
- **Proposed test**: install a staged checkpoint whose entries expire at `now + 50 ms` under an injected clock offset by +5 s; assert either that the keys survive (with the skew reported) or that the drop is counted and surfaced in INFO — silently dropping them is the behaviour to prevent.
- **Boundary**: 3 — the install is a server-side component with an injectable installer seam; a full two-node scenario cannot control the clocks any better and costs far more.
- **OPTIONS**:
  - *Level 1* on the expiry predicate alone: cheapest, but the predicate is already trivially correct — the bug is that nothing bounds the skew at install time.
  - *Level 3* with a clock seam added to `LiveCheckpointInstaller`: **recommended**; the seam is small and pays for itself in the persistence area too.
  - *Level 4/5* turmoil with a skewed host clock: turmoil controls time but not per-host wall clock offsets today; would need a new primitive. Not worth it for this.

### Promoted node cannot serve PSYNC to a surviving sibling (carried from `## Deprioritised` — not F-numbered)

**BLOCKED on `.scratch/replication-cluster-rework/`** — `promotion-replid-psync.md` §4.4–4.5
and §7 already specify the full test plan, and `new_replication_id` has no production caller,
so any test asserting replid rotation on `REPLICAOF NO ONE` cannot pass until that PRD is
reviewed and implemented.

Carried here because `MASTER.md` §8 names it — "promoted-node PSYNC / replid rotation *(14)*"
— as reported blocked rather than proposed. It carries no F-number and no
severity/likelihood/effort/priority line in the proposal, so it is **not** counted in this
issue's finding total.

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`) —
`MASTER.md` §5 lists `new_replication_id` (no production caller). §5 cites no finding numbers,
so it claims nothing on its own; here the absence of a caller is the *symptom* of the blocked
work, not dead code to delete.

- **Reason for deprioritising (verbatim from the proposal)**: "Real and important, but **blocked on pending rework**: `.scratch/replication-cluster-rework/promotion-replid-psync.md` §4.4–4.5 and §7 already specify the full test plan (backlog floor, `clear_secondary_window`, replid rotation, the chain test, the turmoil variant). `new_replication_id` (`replication/src/state.rs:248`) still has **no production caller** — confirmed by `rg`: only tests at `replay.rs:327`, `state.rs:434/440/469/472`, `offset_coordinator.rs:345` and two doc comments. Any test asserting replid rotation on `REPLICAOF NO ONE` cannot pass today. `test_promoted_node_via_replicaof_no_one_serves_downstream_psync` (`integration_replication.rs:1875`) accepts `FULLRESYNC || CONTINUE` for exactly this reason; tightening it is part of that PRD, not of this round."

## Acceptance criteria

- [ ] F3: a test drives `ReplicaReplicationHandler` with frames at offsets 100..200, a forced disconnect, and a reconnect answered with `+FULLRESYNC <replid> 500` plus a checkpoint, and asserts no frame with `sequence <= 500` is passed to `apply_group` after the installer closure has been invoked.
- [ ] F5: a test feeds `MULTI`(shard 3), `SET a`, `SET b` with no `EXEC`, then post-resync frames `SET c` (shard 1) and `SET d` (shard 5), and asserts each is delivered to `apply_group` as its own single-command group on its own tagged shard and that nothing is ever applied on shard 3.
- [ ] F6: tests drive real shard workers through `ReplicaCommandExecutor::apply_transaction` and assert (a) a 3-command group applies all three and returns `Ok`; (b) a `TransactionResult::WatchAborted` group returns `Err(ApplyError)` and leaves the keyspace untouched; (c) `TransactionResult::Error` likewise — asserting on the keyspace, not on a mock.
- [ ] F8: `test_replica_lag_behavior` asserts `replicated_count == sample_keys.len()`; `test_fullresync_interrupted_resume` asserts `initial_verified == sample.len() && additional_verified == sample.len()` and has a same-replica resume arm that restarts the same replica data dir; `test_writes_during_full_sync_are_not_lost` reads back every `during{i}` key on the replica and asserts value equality; `test_replica_read_only` is deleted.
- [ ] F9: a test blocks inside `stream_checkpoint`, broadcasts N commands while the transfer is stalled, releases, and asserts the replica's received frames cover `(snapshot_offset, current]` exactly once each and in offset order.
- [ ] F11: a test asserts `ReplicationRingBuffer::new(0, 1024).push(1, 0, b"x".into())` terminates, and a second asserts `ReplicationConfig { split_brain_buffer_size: 0, split_brain_log_enabled: true, .. }.validate()` returns `Err`.
- [ ] F12: a test with `min-replicas-to-write 1` queues `SET k v` inside a `MULTI` while a replica is healthy, kills the replica, `EXEC`s, and asserts the pinned outcome explicitly; the same test exists with `self_fence_on_replica_loss = true`.
- [ ] F13: a table over `{"+FULLRESYNC abc", "+FULLRESYNC abc xyz", "-ERR loading", "garbage", "+CONTINUE"}` asserts the exact `io::ErrorKind` returned by `psync` for each, and a second table asserts a distinct error for each of truncated / wrong-field-count / non-numeric-offset / bad-checksum-length input to `FullSyncMetadata::from_bytes`.
- [ ] F14: a test installs a staged checkpoint whose entries expire at `now + 50 ms` under an injected clock offset by +5 s and asserts either that the keys survive with the skew reported, or that the drop is counted and surfaced in INFO — a silent drop fails the test.

## Depends on

- issue 18, `.scratch/testing-improvements-round2/issues/` (I18 — resync-boundary signal on the replica frame channel. This area's **top** shared-infrastructure ask, and it is required to *fix* F3 and F5, not only to test them: today the applier cannot distinguish a pre-resync frame from a post-resync one, so a generation counter on `ReplicationFrame` or an explicit `Barrier` message is simultaneously the fix and the test hook).
- issue 06, `.scratch/testing-improvements-round2/issues/` (I6 — live-link fault primitive; the current `fault_injection` module mangles only *recorded histories*, and nothing can stall a checkpoint transfer on a running link, which is what F9's blocking-stream case needs at the session level and what F12 wants for a controlled replica loss).
- issue 03, `.scratch/testing-improvements-round2/issues/` (I3 — injectable clock seam; this area's shared-infra item 3, named explicitly as "a clock seam on the checkpoint install path (F14), shared with persistence". F14 is unwritable without one — there is no clock seam in `LiveCheckpointInstaller` or `read_snapshot` today).
- issue 02, `.scratch/testing-improvements-round2/issues/` (I2 — subprocess-SIGKILL crash primitive. **Weaker claim than the three above**: this area's own cross-area notes do not list it, and the finding that asked for it — F7, "kill the replica process, restart it" — is promoted to issue 76, `.scratch/testing-improvements-round2/issues/`. What remains for F12 is I2's "naming bug to fix regardless": `ClusterNode::kill()` (`cluster_harness.rs:912`) is a **graceful** shutdown, so "kill the replica between MULTI and EXEC" cannot today be expressed as an abrupt loss the primary's tracker observes).
