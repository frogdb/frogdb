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

## Re-triage 2026-08-06

**Verdict: partially-fixed**

Five of the ten entries are discharged; four-and-a-half remain. Per-finding verdicts below;
`Area:` line is stale throughout — `server/src/replication/{executor,install}.rs` and
`server/src/replication_quorum.rs` are now
`frogdb-server/crates/replication-runtime/src/{executor,install,quorum}.rs`, and the crate
`frogdb-server/crates/replication/` kept its own path.

| F | verdict | evidence |
|---|---|---|
| F3 | **fixed** | Commit `85fc3095` (issue 18, now in `issues/done/`) added the epoch stamp the finding said did not exist: `StreamedFrame { epoch, frame }` (`replication/src/apply.rs:52`) wraps every queued frame, the epoch lives on `AppliedOffset` and is bumped only by `reset_pair` (i.e. only when a full resync installs a dataset). `consume_frames` drops a superseded frame at the top of the loop (`apply.rs:393`) and `ReplicaApplyStint::claim` re-checks under the head lock (`replica/offset.rs`). FM-REPLICATION-007. Forcing test `a_full_resync_discards_the_frames_queued_from_the_previous_history` (`apply.rs:1399`) — **run in this triage: PASS**. Stale refs: `replica/mod.rs:123` → `:237`; `apply.rs:111` → `:374`; `frame.rs:252-277` is still stamp-free on the *wire*, deliberately — the epoch is node-local. |
| F5 | **fixed** | Same commit. The finding's central claim — "no reset hook" — is false today: `consume_frames` sets `pending = None` on an epoch mismatch (`apply.rs:393-395`) and again when the open group's own epoch is superseded (`apply.rs:404-406`), and `PendingTxn` now carries `epoch` (`apply.rs:260`). Forcing test `a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history` (`apply.rs:1453`), with the negative case `a_continue_resume_still_applies_the_frames_it_left_queued` (`:1574`) and the level-1 race `a_claim_stamped_before_a_resync_is_refused_after_it` (`replica/offset.rs:831`). **All four run in this triage: PASS.** The issue's "**This test fails today**" is no longer true. |
| F6 | **fixed** (with a recorded deviation) | `ReplicaCommandExecutor::apply_transaction` moved `server/src/replication/executor.rs:86` → `replication-runtime/src/executor.rs:101` and is no longer untested: `a_reconstructed_transaction_is_one_atomic_shard_message` (`:233`) pins the single-`ExecTransaction`-message shape, and `a_failed_transaction_is_reported_as_a_divergence` (`:330`) drives **both** mapped failures — `Reply::Error("EXECABORT")` and `Reply::WatchAborted` — asserting `Err(ApplyError::Rejected { shard, detail })` each time. Also `an_empty_group_reaches_no_shard` (`:280`), `an_origin_shard_tag_outside_the_shard_count_is_refused_before_any_send` (`:368`, FM-REPLICATION-051), `a_shard_that_is_gone_or_silent_is_reported_as_unavailable` (`:408`). **Deviation from the acceptance criterion**: these drive the `test_shards::fake_shards` message seam and assert on the shard message + the returned error, not on a real shard worker's keyspace. That is the seam the hardening specs chose; the "entirely untested" defect the finding names is gone. |
| F8 | **partially-fixed** (3 of 5) | Done: `test_replica_lag_behavior` (`integration_replication.rs:2400` → **`:2890`**) now `assert_batch_replicated`s the last write *and* all five sampled keys — no `eprintln!` left. `test_replica_read_only` (`:1013` → **`:1106`**) was **strengthened rather than deleted**: it now asserts the error starts with `READONLY` and that no value was left behind, and carries `// FM-REPLICATION-028` (as does the near-duplicate `test_replica_readonly_error`, `:3460` → **`:3923`**); the duplication remains but neither test is decorative. `test_fullresync_interrupted_resume` (`:2984` → **`:3296`**) now asserts `wait_for_acks == 1`, `role:slave`, and eight sampled `initial_key_*` / `while_down_key_*` values via `assert_batch_replicated`. **Remaining**: (a) it still drops the replica and starts a *fresh* `replica2` (`:3337`) — the same-replica-data-dir resume arm was not added, so the test still does not do what its name says; (b) `test_writes_during_full_sync_are_not_lost` (`:5591` → **`:6293`**) still reads back only `after_sync` and never a single `during{i}` key. |
| F9 | **still-valid** | FM-REPLICATION-004's `Forced by` cell still names exactly one test, `full_sync_replays_writes_made_during_handoff` (`replica_session.rs:1083` → **`:2073`**), and it still stalls on the **live-dataset** branch (`tokio::io::duplex(32)` + `with_live_dataset`). No test broadcasts during `stream_checkpoint` (`:496` → **`:887`**, invoked at `:465` → **`:850`**; `start_streaming` `:488` → **`:878`/`:1127`**). **Cheaper than filed, though**: `fullresync_cuts_the_checkpoint_after_the_pre_checkpoint_hook` (`:2752`) already stalls the *checkpoint* stream with a `duplex(64)` against a real `RocksStore`, so the blocking harness F9 asked for now exists on the right branch — what is missing is the broadcast-during-stall plus the `(snapshot_offset, current]` exactly-once/in-order assertion. |
| F11 | **fixed** | Both halves, under **FM-REPLICATION-047**, plus two bugs the finding did not see. The spin is closed at the loop, not only at validation: `ring_buffer.rs:58-66` → **`:191-203`**, now `while !entries.is_empty() && (entries.len() >= self.max_entries \|\| …)` — the guard is outside the disjunction, so an empty deque exits whatever the caps say. Forced by `ring_buffer_push_terminates_under_a_degenerate_cap` (`primary/tests.rs:306`). The config keys were **renamed**: `split_brain_buffer_size` no longer exists; the backlog owns `backlog_enabled` / `backlog_size` / `backlog_max_mb` / `backlog_ttl_secs`, and `ReplicationConfigSection::validate` (`config/src/replication.rs:413-425`) rejects `0` on both caps with an error naming `backlog_enabled`, forced by `zero_backlog_caps_are_rejected_and_the_mb_conversion_is_checked` (`config/src/replication.rs:518`). Bonus fixes in the same row: the backlog had been wired to `split-brain-log-enabled` (turning off audit logging silently disabled partial resync), and `mb * 1024 * 1024` wrapped on `usize`. |
| F12 | **still-valid** | Unchanged and now *doubly* documented rather than fixed. The comment is verbatim at `server/src/connection/guards.rs:316-322` → **`:319-325`** ("the narrow window where a MULTI is queued while replicas are healthy and then EXEC'd after they drop are NOT gated here"), and `.scratch/hardening/specs/replication-failure-modes.md:889-893` restates it as a pinned bound of FM-REPLICATION-041/042 ("…is not re-checked"). No test pins the healthy-then-dropped ordering on either gate: `test_min_replicas_to_write_multi_and_lua_paths` (`:6350` → **`:7071`**) and `test_self_fence_multi_rejected_at_queue_time` (**`:6910`**) both start from an already-unhealthy primary and only exercise queue-time rejection → `EXECABORT`. **Note for whoever writes it**: `94a248a1` gave the fence its own `SELFFENCE` error code and a **clean-departure disarm**, so the test must drop the replica *abruptly* (a graceful shutdown disarms the fence — see `test_self_fence_disarms_after_a_clean_replica_departure`, `:6871`), which is exactly the I2 gap this issue's `Depends on` already names. |
| F13 | **partially-fixed** | The PSYNC table landed: `psync_rejects_a_payload_that_carries_no_dataset` (`replica/connection.rs:1306`) drives six scripted replies — including the finding's `"+FULLRESYNC abc"` (no offset), `"+FULLRESYNC abc xyz"` (non-numeric offset) and `"-ERR loading"` rows — and asserts a message fingerprint **plus** the replid/offset-unchanged half (issue 51's, now in `issues/done/`) and the next reconnect's request args. The `psync_against` harness (`:667`) covers the `+CONTINUE` arms (bare / carrying an id / echoing the current id). `FullSyncMetadata` is reached through `read_metadata`: `test_read_metadata_wrong_field_count` / `_oversized_len` / `_truncated_body` (`fullsync.rs:1098` / `:1109` / `:1119`) each assert an exact `io::ErrorKind`, all tagged FM-REPLICATION-035, and `read_file_header`'s error arms (`:983`, `:993`, `:1004`, `:1034`) are covered too. **Remaining — precisely the "error taxonomy" residue this finding was reduced to**: (a) the psync rows assert message text, never `err.kind()`; (b) the `"garbage"` row is absent — `unexpected PSYNC response` (`connection.rs:337`) has no case; (c) `FullSyncMetadata::from_bytes` (`fullsync.rs:70-98`) still has four undistinguished arms — `invalid rdb_size`, `invalid checksum hex`, `checksum must be 32 bytes`, `invalid replication_offset` — only the field-count arm is forced; (d) `read_ok_response`'s non-`+OK` arm (`:303` → **`:559-572`**) and `read_resp_line`'s UTF-8 arm (`:47` → **`:51`**) are still only driven on the happy path (`the_handshake_announces_the_port_it_was_given`, `:793`). |
| F14 | **still-valid, but unblocked** | The predicate moved `server/src/replication/install.rs:197` → **`replication-runtime/src/install.rs:332`** (`SnapshotSink::absorb_warm`: `if metadata.expires_at.is_some_and(\|at\| at <= now) { continue }`, `now = clock::now()` at `:319`) and the hazard is real on today's code: `unix_ms_to_instant` (`persistence/src/serialization/mod.rs:232-249`) re-anchors the absolute stamp against the **receiver's** `clock::system_now()`, so a receiver +5 s ahead maps a `now+50 ms` deadline into the past and the entry is dropped with no counter and no log line. **The finding's stated blocker is discharged**: `2fb1051c` / `0fe2dd0a` landed `frogdb-server/crates/types/src/clock.rs` (`now`, `system_now`, and the test-only `reset_system_epoch(SystemTime)`) plus a lint that gates OS-clock reads, and both the install path and the serializer read through it — a test can now pin the receiver's wall clock without a new seam, so this is level-1/2 work, not the "Effort 3" the finding priced. **Scope narrows**: the dataset/minimal-RDB branch is now safe by construction — expired keys are dropped at the *exporting* shard (`install.rs:212-213`, echoed in the FM-REPLICATION spec's `install_dataset` invariant), so only the **checkpoint / warm-tier** branch is exposed. Round-2 issue 03 (the clock-seam dependency) can be struck from `Depends on`. |
| *promoted node cannot serve PSYNC* (blocked, un-numbered) | **fixed / unblocked** | `new_replication_id` **has a production caller now**: `PrimaryReplicationHandler::begin_primary_stint` (`replication/src/primary/mod.rs:385`) mints the id, freezes the failover window at the settled applied offset, disarms any staged checkpoint and arms the backlog, all under one write lock with rollback on a failed persist. The PRD it was blocked on, `.scratch/replication-cluster-rework/promotion-replid-psync.md`, reads `Status: implemented (review rounds 1-3 applied)`. `test_promoted_node_via_replicaof_no_one_serves_downstream_psync` (`integration_replication.rs:1875` → **`:2225`**) no longer accepts `FULLRESYNC \|\| CONTINUE`: it asserts `+CONTINUE` exactly at the boundary, `+FULLRESYNC` one byte past it, and `+FULLRESYNC` for an unknown replid. The dead-code overlap with issue 34 is moot. |

**Issue-level:** stays in `open/`. Acceptance criteria F3, F5, F6, F11 are met and the blocked
entry is resolved. What is left, in cost order: F13 (a)-(d) and F11-style table work (cheapest);
F8's two remaining assertions — read back every `during{i}` key, add the same-replica resume arm;
F14 (now writable at level 1-2 on the clock seam, checkpoint branch only); F9 (the
checkpoint-stall harness exists, only the broadcast + ordering assertion is missing); F12 (still
the most expensive, and still gated on an abrupt-loss primitive per I2).

**Bearing of the F7 family on the residue** (asked explicitly): issue 76 made the acked offset the
**landed** (applied) head — `landed <= claimed <= received`, `replica/offset.rs` — and `90fefaf7`
(hardening 28) made `acked_offset` wire-only, split from `resume_offset`, so a replica the primary
merely *sent* bytes to no longer counts toward `WAIT`. Together they mean `WAIT 1` returning `≥1`
now implies "one replica decoded, applied and acknowledged from the wire every frame at or below
the target". That materially strengthens `test_writes_during_full_sync_are_not_lost` without
touching it — its convergence assertion is no longer the vacuous offset check F8 described — and it
makes F9's proposed assertion cheaper, because the offsets the replica reports are now apply-backed.
Neither discharges the residual asks: F8 still never compares a `during{i}` *value*, and F9 still
has no checkpoint-branch ordering test.
