# Independent distributed-systems review of FrogDB specs

## Verdict

FrogDB's six locked specs are, structurally, at or above the bar set by CockroachDB's RFCs and
FoundationDB's design notes: a state-space table that names the writer and the persistence vehicle
for every piece of mutable state, transition rows with explicit pre/postconditions and code
citations, and a failure-mode table whose every row is bidirectionally linked to a forcing test by a
lint. That discipline is rare and it works — the majority of load-bearing claims I spot-checked
against source held up exactly as written, including several subtle ones (fsync bracketing of
publishing renames, `PointInTime` recovery mode, the write-effect ordering that stops a served pop
from resurrecting, the fence-then-role-check ordering in WAIT). The defects found are therefore not
sloppiness; they are the residue that only an adversarial code-vs-spec pass finds, and they cluster
in one recognizable place: **state that lives only in memory and is driven only by a live event
stream, whose spec row quietly says it is "reconstructible in principle" without any code that
reconstructs it.** Three of the six Criticals are that exact shape. A second recurring shape is
**overloading one signal (a dropped sender, an absent marker, a post-hoc global sequence read) to
mean one thing when three different producers use it**, which is where the rest of the Criticals
live. A third, narrower to `vll`, is **a safety argument imported from a paper whose enabling
precondition the implementation does not provide** (CRIT-7). Findings across all six specs:
**7 Critical, 23 Major, 14 Minor.** Four of these are cases where a *filed ruling is itself
unsound* rather than merely un-implemented — MAJ-2 (dedup key that does not exist on the wire),
MAJ-5 (issue 17 removes a bound and replaces nothing), MAJ-11 (issue 08's H5 converts ordinary
timeouts into false `-ERR shard unavailable`), and CRIT-2 (proposer-minted deadlines relocate trust
rather than removing it).

## Findings

### CRIT-1: A source node that restarts or snapshot-catches-up mid-handoff never re-arms its slot write barrier

- **Where**: `specs/cluster.md` — State-space row for `PauseState.slots`; TR-CLUSTER-013;
  FM-CLUSTER-089/095. Code: `frogdb-server/crates/cluster/src/state.rs:607` (`slot_handoff_tx`),
  `:257-268` (`restore_from_snapshot`), `:1065-1102` (`install_snapshot`),
  `frogdb-server/crates/server/src/server/cluster_init.rs:190-300`,
  `frogdb-server/crates/cluster-runtime/src/handoff_barrier.rs:177`.
- **Claim/gap**: The slot write barrier is the entire safety argument for two-phase slot handoff —
  it is what makes "the source has quiesced" true before ownership moves. The State-space row admits
  the barrier is in-memory and claims only that it is "reconstructible in principle" from the
  replicated `migrations` map. Nothing reconstructs it. `run_slot_handoff_barrier` is
  `while let Some(event) = handoff_rx.recv().await` — it arms and releases *exclusively* from the
  live `SlotHandoffEvent` stream. `restore_from_snapshot` does `*guard = restored` plus a debug
  invariant check and emits nothing. `install_snapshot` contains explicit reconciliation logic for
  the *role* view (`emit_self_role_change(role_after)`) and has no handoff analogue. And in
  `cluster_init.rs` the ordering is decisive: `attach_snapshot_store(...)` (which restores the
  durable snapshot) runs at `:~215`, `enable_slot_handoff_notification()` only at `:240` — so even
  the boot-time snapshot restore is *guaranteed* to predate the existence of the channel.
- **Why it matters**: Node A owns slot 42 and is the source of a handoff to B.
  `PrepareSlotHandoff` commits; A arms its barrier and stops admitting writes to slot 42. A crashes
  and restarts (or is partitioned long enough to be caught up by snapshot rather than log replay).
  On rejoin A's replicated state still says slot 42 is `Migrating` and A is still the owner, so A
  still serves slot 42 — but its barrier is not armed, so it **admits writes to slot 42 while the
  cluster believes it has quiesced**. B then completes the handoff on the strength of A's earlier
  drain confirmation, and the writes A admitted post-restart are silently lost (they were never
  drained to B, and A ceases to own the slot). This is a lost-write window, not merely an
  availability bug. FM-CLUSTER-095's `SlotFence` does not close it: the fence stamp and the fence
  verdict both read the same unchanged `handoff_seq`, so a post-restart write stamps and validates
  consistently. There is no FM row of the form "the source restarts mid-handoff".
- **Mature-system comparison**: This is the canonical reason CockroachDB derives leaseholder and
  latch state from the replicated range state on every store restart rather than from an event
  stream, and why `Replica.loadRaftMuLockedReplicaMuLocked` reconstructs everything the range needs
  from durable state at start-of-day. etcd/raft's design guidance is the same: an in-memory
  derivative of replicated state must be rebuilt on both the log-replay path and the snapshot path,
  and snapshot install is the path people forget. FrogDB already learned this lesson once for the
  role view — issue 37 added `reconcile_self_role` with a comment naming "a role folded into a
  snapshot produced no log entry to replay". The barrier is the identical bug, unfixed.
- **Suggested resolution**: Add `reconcile_slot_handoff(...)` next to `reconcile_self_role` in
  `cluster_init.rs`, driven from the restored `migrations` map, and make `install_snapshot` emit
  `Prepared`/`Released` on any handoff-state delta the way it emits role changes. Add an FM row
  ("source restarts with a handoff prepared → barrier is armed before the node admits any client
  write") and a forcing test that restarts a source mid-handoff and asserts a slot-42 write is
  refused. Restate the State-space row: it currently launders an unimplemented reconstruction as a
  property.

### CRIT-2: Handoff admission is gated on timestamps minted by the party seeking admission

- **Where**: `specs/cluster.md` — preamble on proposer-minted deadlines; FM-CLUSTER-089;
  TR-CLUSTER-013. Code: `frogdb-server/crates/cluster/src/commands.rs:~586` (`PrepareSlotHandoff`
  → `migration.live_handoff_at(proposed_at_ms)`), `:~718` (`CompleteSlotMigration` →
  `h.admits_complete_at(proposed_at_ms)`), `frogdb-server/crates/cluster/src/types.rs:690-796`.
- **Claim/gap**: The spec correctly forbids a clock read at apply time (that would be
  non-deterministic across replicas and would fork the state machine). Its remedy is to have the
  proposer mint `prepared_at_ms` / `barrier_ms` / `lease_ms` and carry them as replicated data. That
  fixes determinism but the spec never notices that it has *relocated trust to the interested
  party*: `admits_complete_at(proposed_at_ms)` and `live_handoff_at(proposed_at_ms)` are both
  evaluated against a number supplied by the node that wants the transition to be admitted. There is
  no stated maximum-clock-offset assumption, no cross-check of the proposed timestamp against the
  applying node's own clock or against the Raft leader's, and no self-termination monitor.
- **Why it matters**: The target node of a handoff, whose clock has drifted forward by more than the
  barrier window (NTP step, VM live-migration, a hypervisor pause), proposes `CompleteSlotMigration`
  with `proposed_at_ms` already past the source's `barrier_ms`. Every replica deterministically
  agrees the barrier has expired and admits the completion — even though in real time the source has
  not finished draining. Deterministic and wrong is still wrong; the state machine cannot fork, but
  it can uniformly admit an unsafe transition. A malicious or merely broken node needs no special
  privilege to do this.
- **Mature-system comparison**: CockroachDB's HLC makes the max-offset assumption explicit
  (`--max-offset`) *and* enforces it with a self-termination monitor: a node that observes a remote
  clock beyond the offset kills itself rather than continue participating. Spanner's TrueTime does
  the same thing with hardware backing and `commit-wait`. The general rule these systems encode is
  that a time-derived safety property needs either (a) a bounded-offset assumption plus enforcement,
  or (b) a logical replacement. FrogDB has neither.
- **Suggested resolution**: Prefer (b) — the campaign has already ruled in this direction elsewhere
  (issue 17's "log-ordered fence, no wall-clock"). Replace the timestamp comparison with a logical
  admission token: the source's drain confirmation carries the `handoff_seq` it drained, and
  `CompleteSlotMigration` is admitted iff that seq matches the current one. If timestamps are kept,
  the spec must state the max-offset assumption as a named precondition, the applying node must
  sanity-bound `proposed_at_ms` against the leader's clock, and an FM row must cover
  "proposer's clock is skewed forward past the barrier".

### CRIT-3: `wal_watermark` can lead the durable point; "never a false alarm" is unearned

- **Where**: `specs/persistence.md` — FM-PERSISTENCE-035, TR-PERSISTENCE-010. Code:
  `frogdb-server/crates/persistence/src/rocks/mod.rs:345` (`record_wal_watermark()` after
  `publish_synced_through`), `:582-588` (`flush()` = sequential per-CF `flush_cf` loop), `:70`
  (`sync_targets: Mutex<Vec<Weak<dyn DurableSyncTarget>>>`); `record_wal_watermark` body does
  `let seq = self.db.latest_sequence_number();` — a post-hoc *global* read, not the sequence the
  fsync actually covered.
- **Claim/gap**: The watermark is supposed to be a lower bound on what is durable, and
  FM-PERSISTENCE-035 declares "a false alarm" (recovery reporting lost acknowledged writes that were
  never acknowledged as durable) impossible. Two reachable paths make the watermark *lead* the
  durable point. (a) `periodic` mode: `durable_sync` calls `flush()`, then `publish_synced_through`,
  then `record_wal_watermark()`; `flush()` is a sequential loop over column families, so a commit
  landing in shard 0 *after* shard 0's `flush_cf` returned is WAL-only, yet the subsequent global
  `latest_sequence_number()` includes it. (b) `sync` mode, multi-shard: `record_wal_watermark()`
  runs after `write_batch_opt` on one of N shard threads that share one `RocksStore`; a concurrent
  shard's write, or a cluster-metadata write
  (`frogdb-server/crates/cluster/src/storage.rs:393` `put_cf_opt`), bumps the global sequence between
  the fsync and the read.
- **Why it matters**: On power loss or kernel panic, `detect_and_reset` compares the recovered
  sequence to the watermark, increments `frogdb_wal_recovery_dropped_records_total`, and logs
  `WARN … sequence numbers of acknowledged writes were lost` — for writes that `periodic` mode never
  acknowledged as durable. That is exactly the outcome FM-PERSISTENCE-035 says is impossible, and it
  is the worst kind of monitoring defect: it trains operators to ignore the one alarm that means real
  data loss. Note the failure fires only on power loss / kernel panic, never on `SIGKILL`, so no
  process-crash test in the suite can catch it. Worse, the named forcing test
  `only_a_synced_rocks_sink_commit_records_the_durable_watermark`
  (`persistence/src/wal/tests.rs:1655-1686`) asserts `watermark == latest_sequence_number()` — it
  *encodes* the buggy semantics, so the lint's spec↔test link is satisfied by a test that pins the
  bug.
- **Mature-system comparison**: RocksDB deliberately separates `FlushWAL(sync)` from
  `GetLatestSequenceNumber()` and exposes `WriteBatchInternal::Sequence(batch)` precisely so callers
  record the sequence *the batch carried*, not whatever the DB has reached since. etcd's
  `raftStorage` and CRDB's `LogEngine` never derive a durable index from a post-hoc global read; the
  index is a value carried through the write path. FrogDB's own `publish_synced_through` already does
  it correctly — `record_wal_watermark` is the one place that doesn't.
- **Suggested resolution**: Pass the covered sequence as an argument to `record_wal_watermark`, taken
  from the same snapshot `publish_synced_through` uses, and make the store a `fetch_max` rather than
  a blind store. Add a two-shard forcing test (write on shard 1, sync on shard 0, assert the
  watermark did not advance past shard 0's covered sequence). Rewrite the forcing test that currently
  asserts equality with `latest_sequence_number()`.

### CRIT-4: Disconnect-while-parked is undetectable; a later push is consumed and delivered to nobody

- **Where**: `specs/blocking.md` — TR-BLOCKING-013 (Source `connection/lifecycle.rs:181-216`);
  FM-BLOCKING-005. Code: `frogdb-server/crates/server/src/connection.rs:609-700` (run-loop
  `select!`; `self.process_one_command(frame).await` at `:649` runs to completion *inside* the frame
  branch), `connection/blocking.rs:63-75` and `:141` (`end_block()`), `connection/lifecycle.rs:206-215`.
- **Claim/gap**: TR-BLOCKING-013 states that a parked connection which closes drives
  `notify_connection_closed` → `BlockingMsg::UnregisterWait`. Neither half is reachable. (a)
  `handle_blocking_wait` is awaited *inline inside* the frame branch of the run-loop `select!`, so
  the socket is not polled for the entire duration of the wait — EOF cannot be observed while parked.
  (b) `cleanup_wait` calls `state.end_block()` *before* returning the reply, so by the time the loop
  re-enters `select!` and can see EOF, `blocked_shard()` is already `None` and the
  `if let Some(shard_id) = self.state.blocked_shard()` guard at `lifecycle.rs:206` is false. The
  blocking branch of `notify_connection_closed` is dead code on this path.
- **Why it matters**: A client issues `BLPOP jobs 0` and its process dies. The `WaitEntry` stays
  registered forever; the connection task stays parked forever (leaked task + FD); the per-key and
  global waiter budgets are permanently consumed. When a producer later runs `LPUSH jobs x`,
  `drive_satisfaction_body` pops the element and `response_tx.send(reply)` **succeeds** — the
  receiver is alive inside the orphaned task — so the `Err(_)` restore arm never fires. The element
  is removed from the store and written to a dead socket: a silently lost write. This is precisely
  the loss FM-BLOCKING-005 declares NOT observable. The conservation checkers miss it because no
  generator models a mid-wait disconnect.
- **Mature-system comparison**: In Redis a blocked client's fd stays in the event loop;
  `readQueryFromClient` sees EOF and `freeClient` → `unblockClient` removes it from
  `server.blocked_clients` and the per-key `blocking_keys` dict in the same tick, so the later
  `LPUSH` leaves the element in the list. Valkey and Dragonfly are the same — a blocked client is a
  *state on a still-readable connection*, never a suspended read loop. FrogDB's inline-await
  structure is the root architectural divergence.
- **Suggested resolution**: Add a fourth branch to `BlockingWaitCoordinator::wait_for_response` that
  resolves on peer-close, or (better) restructure so the wait is a run-loop state rather than an
  inline await. Forcing test: connect, `BLPOP k 0`, close the socket, `LPUSH k v` from another
  connection, assert `LLEN k == 1` and `DEBUG WAITQUEUE` empty. Until fixed, TR-BLOCKING-013's
  postcondition must be marked `(ruled)` with a `(current code)` cell stating the leak.

### CRIT-5: `CLIENT KILL` cannot terminate a parked client

- **Where**: `specs/blocking.md` — no row exists. Code: `connection.rs:611-614`
  (`self.client_handle.killed()` is select branch 1, not polled during the wait),
  `connection/blocking/coordinator.rs:93-110` (three-branch select: response / unblock / deadline —
  no kill branch), `frogdb-server/crates/core/src/client_registry/mod.rs:709-717` (`kill_by_id`
  sends only `kill_tx`, never touches `unblock_tx`).
- **Claim/gap**: The spec's entire "is every blocked state leavable" argument rests on the CLIENT
  UNBLOCK signal (state-space row plus TR-BLOCKING-010/012). There is no row for CLIENT KILL, and no
  code path by which a kill reaches a parked waiter.
- **Why it matters**: An operator draining a node runs `CLIENT KILL ID <n>` against a client parked
  in `BLPOP k 0`. The command reports success and does nothing, forever. Combined with CRIT-4 —
  which makes leaked parked connections arbitrarily common — the operator has *no* mechanism to
  reclaim them except `CLIENT UNBLOCK`, which requires knowing each id and does not close the
  connection. A node cannot be cleanly drained.
- **Mature-system comparison**: Redis `CLIENT KILL` calls `freeClient`, which unblocks the client
  first; the documentation explicitly covers killing blocked clients, and `CLIENT KILL ... LADDR` is
  the documented node-drain primitive.
- **Suggested resolution**: Have `kill_by_id`/`kill_by_filter` also publish an unblock signal — the
  existing `UnblockSignal` seam then does the work — or add a `killed()` branch to
  `wait_for_response`. Add a TR row and forcing test.

### CRIT-6: `replication_state.json` has one fixed temp path shared by two unsynchronized writers, and no fsync

- **Where**: `specs/replication.md` — TR-REPLICATION-031; FM-REPLICATION-020. Code:
  `frogdb-server/crates/replication/src/state.rs:279-297`:
  ```rust
  let temp_path = path.with_extension("tmp");
  let contents = serde_json::to_string_pretty(self).map_err(io::Error::other)?;
  fs::write(&temp_path, contents)?;
  fs::rename(temp_path, path)?;
  ```
- **Claim/gap**: Two independent writers reach `save()` — `save_state` (periodic snapshot hook and
  the shutdown hook) and `save_snapshot` (promotion) — with no mutual exclusion, and they share one
  fixed temp path. `fs::write` truncates then writes, so writer B truncating A's temp file
  mid-flight, or the two renames interleaving, yields either a torn file or a file whose fields come
  from two different states. Neither the file nor its parent directory is ever fsynced, so even a
  well-ordered rename is not durable. TR-REPLICATION-031 notes the temp-path collision as an
  observation; no FM row forbids it and no filed issue tracks it.
- **Why it matters**: The scenario that matters is exactly the one the file exists for. A node is
  promoted; `save_snapshot` writes the new `master_replid`/`replid2`/offset; the periodic
  `save_state` fires concurrently and clobbers the shared temp. Outcome (i): the file is torn,
  `validate()` fails on next boot, the node mints a *fresh* replication identity — every downstream
  replica full-resyncs and the PSYNC2 failover window is destroyed. Outcome (ii): the file parses but
  mixes a pre-promotion `replid` with a post-promotion offset, which is a correctness fault, not just
  a performance one. Both reach FM-REPLICATION-020's forbidden "half-promoted node" — an in-memory
  `master_replid` no disk knows about — by a route that row never considers: it reasons only about
  `save_snapshot` returning `Err`, and here `save_snapshot` returns `Ok`.
- **Mature-system comparison**: TigerBeetle and FoundationDB both treat "fsync before you publish"
  as non-negotiable, and FrogDB knows this — `persistence`'s `stamp_with` does the full
  write → `sync_file` → rename → `sync_dir` dance and has a `RecordingFs` trace test asserting the
  order (FM-PERSISTENCE-049). The replication-identity file, which is *more* safety-critical than
  most of what that machinery protects, skips all of it. etcd serializes all member-state writes
  through a single goroutine for exactly the concurrent-writer reason.
- **Suggested resolution**: Use a unique temp name (pid + nonce), fsync the temp file before the
  rename and the parent directory after, and serialize the two writers behind one mutex or one
  owning task. Add an FM row: "a promotion snapshot concurrent with a periodic state save leaves a
  valid, promotion-complete file on disk". Reuse persistence's `stamp_with` rather than
  reimplementing.

### CRIT-7: Cross-shard SCA admits hold-and-wait cycles — the VLL deadlock-freedom argument does not hold

- **Where**: `specs/vll.md:45` (state-space row `lock_table.intents`: "BTreeMap ordering realizes SCA
  priority (lower txid = higher priority)"), TR-VLL-002, TR-VLL-014. Code:
  `frogdb-server/crates/vll/src/lock_table.rs:90-123`,
  `frogdb-server/crates/vll/src/coordinator.rs:219-266`.
- **Claim/gap**: The entire SCA liveness story rests on a total order by `txid` — a transaction only
  ever waits on *lower* txids, so the wait-for graph is acyclic. `try_grant` breaks that order. It
  applies two rules: (1) SCA — blocked by conflicting *lower*-txid intents; (2) holder — blocked by
  any conflicting **granted** intent from another txid **regardless of order** (`lock_table.rs:107-114`,
  pinned by the crate's own test `lower_txid_writer_blocked_by_granted_higher_reader:284`). Rule 2
  creates lower→higher wait edges. Because `scatter` declares on each shard via independent async
  messages (`coordinator.rs:219-240`) and then waits (`:247-266`) *while holding grants already
  obtained elsewhere*, per-shard grant order can differ across shards.
- **Why it matters**: Two clients issue `MSET kA kB` (kA on shard A, kB on shard B), txids 3 and 5.
  Shard A processes txn5 first → granted; txn3 declares, SCA passes (nothing lower), rule 2 blocks on
  txn5's grant → parked. Shard B processes txn3 first → granted; txn5 declares, SCA blocks on txn3 →
  parked. txn3 holds B waiting on A; txn5 holds A waiting on B. A cycle. Neither releases until its
  whole batch is granted, so the only exit is the phase-2 timeout — and then *both* abort, both
  clients retry, and the same interleaving is equally likely: mutual-abort livelock under sustained
  contention. This is an ordinary two-key cross-shard write, no Lua involved. `vll.md` has no `FM-VLL`
  row for it; TR-VLL-014's wound-wait ruling is explicitly scoped to the **continuation** lock
  (`ShardBusy` refusal), not the SCA lock table, and issue 07's H1 has the same continuation-only
  scope. Compounded by MAJ-20: resolution latency is `participants × timeout`, not `timeout`.
- **Mature-system comparison**: Calvin/VLL is deadlock-free *only* because the sequencer inserts a
  transaction's entire lock request into all lock tables in one globally-ordered step before
  execution; waits then only run lower→higher. FrogDB has the global txid but not the atomic
  multi-shard declaration, so it loses the invariant that makes VLL VLL. CockroachDB handles the
  analogous case with wound-wait plus an explicit txn-wait-queue and deadlock detection; FoundationDB
  sidesteps it by not holding locks at all (OCC over conflict ranges).
- **Suggested resolution**: Add an `FM-VLL` row for the cross-shard cycle with an honest Invariant
  (today: broken only by `acquisition.timeout`), and extend TR-VLL-014's wound-wait ruling to the SCA
  path — on a rule-2 conflict, wound the higher-txid *holder* rather than blocking the lower-txid
  requester, which restores acyclicity by construction. Forcing test belongs in `frogdb-vll` (two
  coordinators, two shards, opposite arrival order).

### MAJ-1: Split-brain audit floor collapses to 0 in exactly the case that triggers it, contradicting FM-REPLICATION-024's own NOT-observable cell

- **Where**: `specs/replication.md` — FM-REPLICATION-024. Code:
  `frogdb-server/crates/replication/src/primary/mod.rs:838-849`
  (`let start = self.offsets.min_acked().unwrap_or(0);`),
  `offset_coordinator.rs:288-290` (`min_acked` → `tracker.min_acked_offset()`),
  `tracker.rs:398-403` (`self.get_streaming_replicas().iter().map(|r| r.acked_offset).min()`),
  test `min_acked_offset_ignores_a_resume_seed` at `tracker.rs:827+`.
- **Claim/gap**: The audit's floor is meant to be "the acked offset, not `0`" —
  FM-REPLICATION-024's NOT-observable cell says so explicitly ("Acknowledged writes in the discard
  audit"). But `min_acked_offset` is the minimum over *currently streaming* sessions. In a network
  partition — the only realistic split-brain trigger — the diverged primary has lost its replicas, so
  the set is empty, `min_acked()` is `None`, and `unwrap_or(0)` yields a floor of 0. A freshly
  re-attached replica gives the same answer: its `acked_offset` is 0 and
  `min_acked_offset_ignores_a_resume_seed` deliberately pins `Some(0)`. The row's own `Forced by`
  list names `divergence_record_no_streaming_replicas_uses_zero_floor` — a test that pins the
  behavior the row forbids.
- **Why it matters**: The audit then lists *every* write still in the backlog as discarded,
  including writes the new primary already holds. `ops_discarded` is inflated by orders of magnitude,
  destroying the metric's usefulness for judging partition severity. Operationally worse: the audit
  is meant to be replayable, and replaying it re-applies non-idempotent writes (`INCR`, `LPUSH`,
  `APPEND`) that the successor already applied — turning a diagnostic into a corruption tool.
- **Mature-system comparison**: The correct floor is the successor's match index — what CRDB and any
  Raft implementation use to decide which suffix of a deposed leader's log is genuinely
  uncommitted. FrogDB already has this value: the `DemotionEvent` names the successor's acked offset.
- **Suggested resolution**: Take the floor from the `DemotionEvent`'s successor offset; when it is
  unavailable, emit the audit as `unknown-floor` rather than silently substituting 0. Delete or
  invert `divergence_record_no_streaming_replicas_uses_zero_floor`.

### MAJ-2: TR-REPLICATION-022's AMENDED dedup ruling is unimplementable as written, and contradicts a locked invariant

- **Where**: `specs/replication.md` — TR-REPLICATION-022 (AMENDED ruling), FM-REPLICATION-049,
  INV-SESSION-2 (locked catalog).
- **Claim/gap**: The amended ruling requires session dedup to key on "node/replica id" and
  explicitly rejects "(peer IP, announced port)". But FM-REPLICATION-049 establishes that the
  announced identity on the wire is exactly `listening-port` + `capa` + `frogdb-version` — in
  standalone mode there is **no replica node id or run id transmitted at all**. The ruling names a
  key that does not exist. Meanwhile INV-SESSION-2, which is locked, already asserts "at most one
  live session per announced identity" — i.e. it keys on precisely the identity the amendment
  rejects. This is a filed ruling that is itself unsound, not a code-catch-up lag.
- **Why it matters**: An implementer following the ruling has three bad options: invent a wire
  extension (a protocol change the ruling does not authorize or describe), silently fall back to the
  rejected key (leaving the spec lying), or block. Meanwhile the locked invariant it contradicts
  cannot both hold and be superseded. Concretely, two replicas behind one NAT announcing the same
  `listening-port` are indistinguishable today; the ruling was presumably filed to fix that, but
  without a wire-level id there is no fix.
- **Mature-system comparison**: Redis transmits `replconf` with a run-id-bearing handshake precisely
  so the primary can distinguish sessions; etcd assigns and persists a member id at join time for
  the same reason. Identity must be *minted and transmitted*, not inferred from transport.
- **Suggested resolution**: Either (a) extend the handshake with a replica run-id (a `REPLCONF
  replica-id <id>` capability), amend FM-REPLICATION-049, and then the ruling becomes
  implementable; or (b) withdraw the amendment and keep the announced-identity key, documenting the
  NAT limitation. Until one is chosen, mark INV-SESSION-2 as contested — a locked invariant and a
  ruling must not disagree.

### MAJ-3: FM-REPLICATION-019 contradicts the State-space table on offset rewind

- **Where**: `specs/replication.md` — FM-REPLICATION-019 Observable cell vs. the `LiveOffset`
  State-space row. Code: `offset_coordinator.rs:223-240`:
  ```rust
  let applied = self.applied.freeze();
  let received = self.current();
  if received > applied {
      tracing::warn!(received, applied, discarded_bytes = received - applied,
          "Promotion discarding replication frames received but never applied");
      self.live.store(applied, Ordering::Release);
  }
  ```
- **Claim/gap**: FM-REPLICATION-019 says `master_repl_offset` "is continuous across the identity
  change (it does not reset, jump, or rewind)". The State-space row for `LiveOffset` describes
  `settle_at_applied_inner` as "(promotion boundary, `store(applied)` — a rewind)". The code does
  rewind, and warns when it does. `master_repl_offset` renders from `tracker.current_offset()`,
  which is `LiveOffset`. Two locked rows state opposite facts about the same observable.
- **Why it matters**: The rewind happens in the *normal* promotion case — any frames received but
  not yet applied. A monitoring system or an operator's failover runbook built on FM-019's guarantee
  (offsets are monotone across failover) will misread a healthy promotion as a corruption event, or
  worse, a tool comparing offsets to decide "has the new primary caught up" will conclude wrongly.
  No FM row states the true behavior, so nothing forces it either way.
- **Mature-system comparison**: Redis's PSYNC2 keeps `master_repl_offset` monotone at promotion
  precisely because downstreams use it to compute a partial-resync point; the rewind here means a
  downstream that had received the un-applied bytes now has a *higher* offset than its new primary,
  which is the classic "replica ahead of master" condition Redis handles by forcing a full resync.
- **Suggested resolution**: Decide which is true and make the other row cite it. If the rewind stays
  (it is the safe choice — you must not claim bytes you did not apply), amend FM-019's Observable
  cell to "may rewind by the un-applied byte count; never advances past applied", and add an FM row
  for the downstream-ahead-of-new-primary case with a forcing test.

### MAJ-4: The preamble's directional clock-skew argument is backwards

- **Where**: `specs/cluster.md` — preamble justifying proposer-minted deadlines
  ("a clock skewed forward admits a transition no quorum agreed the source had quiesced for").
- **Claim/gap**: For the *applying* node the implication runs the other way. The guard is
  `!barrier_expired`, so a forward-skewed clock at apply makes the barrier look *more* expired and
  therefore **refuses** more, not fewer, transitions. The dangerous direction at apply is backward
  skew. The stated rationale is the one place a reader checks whether the design reasoning is sound,
  and it argues for the right conclusion from the wrong direction — which is how a later change
  reintroduces the hazard while believing it is safe.
- **Why it matters**: Combined with CRIT-2 (where forward skew at the *proposer* genuinely is the
  hazard), the spec conflates two roles with opposite sensitivities. Anyone reasoning about a clock
  fix from this paragraph will protect the wrong side.
- **Suggested resolution**: Split the paragraph by role: proposer-forward-skew is the admission
  hazard (CRIT-2); applier-skew is irrelevant once the value is replicated data. State both.

### MAJ-5: Issue 17's ruled deletion of the `barrier_ms` admission window leaves the source's local pause deadline unfenced

- **Where**: `specs/cluster.md` — TR-CLUSTER-013 (ruled), FM-CLUSTER-084/085.
- **Claim/gap**: The ruling deletes the `barrier_ms` admission window in favor of a log-ordered
  fence — correct in itself, and I am not re-litigating the ruling's direction. But `barrier_ms` had
  a second job: it bounded how long the *source* holds its local write pause. Nothing in the spec
  replaces that. With the admission window gone, a source that armed its barrier and then lost
  contact with the leader holds slot writes paused indefinitely.
- **Why it matters**: A handoff that stalls (target crashes after `PrepareSlotHandoff`) now takes
  the source's slots offline for writes permanently, with no timeout and no documented operator
  escape. That converts a target-node failure into an unbounded availability outage on a healthy
  node — the "stuck state" category the review lens calls out.
- **Mature-system comparison**: CRDB's replica-level latches and merge/split barriers are always
  paired with a lease-expiry backstop; the barrier is never held on the strength of a peer that may
  never return.
- **Suggested resolution**: Add a bounded local hold (a byte-cap or apply-index cap consistent with
  issue 17's "no wall-clock" direction) plus an explicit abort path, and an FM row: "target never
  completes → source's barrier releases and the migration aborts within a bounded number of applied
  entries". FM-CLUSTER-084/085's LOCKED "NOT observable" cells also contradict TR-CLUSTER-013's
  ruling and must be re-derived.

### MAJ-6: `CLUSTER RESET` is replicated and therefore cluster-wide destructive

- **Where**: `specs/cluster.md` — Redis deviations table (absent). Code:
  `frogdb-server/crates/cluster/src/commands.rs:~815-863` (`ResetCluster` arm).
- **Claim/gap**: `ResetCluster` is a *replicated* state-machine command: applying it clears
  `slot_assignment`, `migrations`, `handoff_seq`, and `nodes` on **every** node. Redis's `CLUSTER
  RESET` is node-local — it resets the node issuing it and nothing else. The deviation is not in the
  deviations table.
- **Why it matters**: An operator following Redis muscle memory — "this one node is confused, reset
  it and let it rejoin" — destroys the entire cluster's topology and slot map in one command. There
  is no confirmation, no `HARD`/`SOFT` distinction that limits blast radius, and the operation is
  replicated so it cannot be undone by reconnecting.
- **Mature-system comparison**: etcd deliberately has no cluster-wide reset; `etcdctl member remove`
  is per-member and the documented recovery is "stop the member, wipe its data dir, re-add". The
  destructive-scope-equals-invocation-scope principle is what makes that safe.
- **Suggested resolution**: Either make it node-local (matching Redis) or, if cluster-wide reset is
  intentionally offered, rename it, gate it behind an explicit confirmation token, and add both a
  deviations-table row and a TR row stating the blast radius.

### MAJ-7: Node ids are 16 bits of entropy per millisecond, regenerated every boot, never persisted

- **Where**: `specs/cluster.md` — State-space row for node identity. Code:
  `frogdb-server/crates/config/src/cluster.rs:231-243`:
  ```rust
  let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
  let random_bits = rand::random::<u16>() as u64;
  (timestamp << 16) | random_bits
  ```
  `node_id` is `#[param(skip)]` with the comment "Redis keeps node id in nodes.conf, not CONFIG" —
  but FrogDB has no nodes.conf equivalent, so an unconfigured node mints a new id on every boot.
- **Claim/gap**: Collision probability is birthday-bounded on 16 bits *within the same millisecond*,
  and `AddNode` is an upsert, so a collision does not error — it silently merges two distinct nodes
  into one entry. Independently, a node restarting without a configured id joins as a *stranger*,
  and the old id's entry (holding its slot assignments) is orphaned.
- **Why it matters**: Two nodes started simultaneously by an orchestrator (the common case — a
  StatefulSet rollout, a `docker compose up`) collide with non-trivial probability, and the merge is
  silent: one node's slot ownership is attributed to the other. The restart case is worse because it
  is deterministic, not probabilistic: every restart of an unconfigured node orphans its slots and
  the cluster has no way to know the newcomer is the same machine.
- **Mature-system comparison**: Redis persists a 160-bit node id in nodes.conf specifically so
  identity survives restart; etcd persists a member id and refuses to start a member whose id does
  not match its data dir. Both treat "identity outlives the process" as foundational.
- **Suggested resolution**: Persist the generated id in the data directory (alongside
  `database_id`, which persistence already does correctly) and read it back on boot; widen the
  random component; make `AddNode` reject rather than upsert a conflicting id. Add a TR row for
  "node restarts without a configured id" and an FM row for id collision.

### MAJ-8: No stated inequality between the self-fence window and the minimum time to promote a successor

- **Where**: `specs/cluster.md` — self-fence rows; `frogdb-server/crates/config/src/cluster.rs:182-228`
  (`validate()` enforces only `heartbeat_interval_ms < election_timeout_ms`; `fail_threshold`
  defaults to 5 and is unconstrained relative to either).
- **Claim/gap**: A fencing design is only safe if the deposed primary stops accepting writes
  *before* the successor starts. The spec states neither the required inequality nor any enforcement
  of it. `fail_threshold * heartbeat_interval` (detection) and `election_timeout` (promotion) are
  independent knobs with no cross-constraint in `validate()`.
- **Why it matters**: With defaults (5 × 250ms = 1250ms to detect, 1000ms election timeout), the
  successor's election can complete before the old primary's fence engages — both nodes admit writes
  simultaneously. This is a split-brain window created by the *default* configuration, and nothing in
  the spec or the config validator flags it.
- **Mature-system comparison**: Raft's safety argument requires the election timeout to exceed the
  broadcast time by an order of magnitude, and etcd's docs state the inequality explicitly and warn
  when it is violated. CRDB's lease design makes the stale-lease interval strictly precede the new
  lease start.
- **Suggested resolution**: State the inequality as a named precondition in the spec, enforce it in
  `ClusterConfigSection::validate()`, and add an FM row: "self-fence engages before any successor
  can be promoted, for all admissible configurations".

### MAJ-9: No replica-validity bound on automatic promotion; an offset-unknown candidate stays eligible

- **Where**: `specs/cluster.md` — FM-CLUSTER-056; promotion scoring
  `priority * 100_000 + (max_offset - replica_offset) * 1_000`.
- **Claim/gap**: There is no staleness bound: a replica that has been disconnected for an arbitrary
  period is still a promotion candidate. FM-CLUSTER-056 goes further and *explicitly* keeps a
  candidate whose offset could not be determined in the candidate set.
- **Why it matters**: A replica that was partitioned for an hour, or one whose offset probe timed
  out, can win the election and be promoted — discarding an hour of writes that a healthier replica
  holds. The scoring formula cannot rescue this: with an undetermined offset there is no meaningful
  term to score.
- **Mature-system comparison**: Redis has `cluster-replica-validity-factor` for exactly this,
  disqualifying replicas whose disconnection exceeds a multiple of the node timeout. Raft's voting
  restriction (§5.4.1) is the general form: a candidate whose log is not at least as up-to-date as
  the voter's cannot win.
- **Suggested resolution**: Exclude offset-unknown candidates from the candidate set (or rank them
  strictly last and require no known-offset candidate to exist), add a validity bound, and add an FM
  row: "a replica whose offset is unknown is never promoted while a known-offset candidate exists".

### MAJ-10: `config_epoch` arbitrates nothing

- **Where**: `specs/cluster.md` — FM-CLUSTER-010/011/012. Code: `config_epoch` has no consumer
  outside `types.rs`/`wire.rs` rendering and tests.
- **Claim/gap**: Three locked failure-mode rows protect the monotonicity and collision behavior of a
  value that no routing, ownership, or admission path reads. The rows are true but vacuous — they
  guard an unused field, which is exactly the "unjustified assertion" category: the invariant is
  real, its *significance* is not.
- **Why it matters**: Two costs. First, a reader (and the mutation gate) is given false confidence
  that ownership conflicts are epoch-arbitrated when they are not — there is no tiebreak at all for
  a genuine ownership disagreement. Second, the collision-resolution rule renumbers the *newcomer*
  upward, which if ever wired up would let a late-arriving node win against an established owner —
  the opposite of what a fencing epoch should do.
- **Mature-system comparison**: Redis's `clusterHandleConfigEpochCollision` bumps the node with the
  *lexicographically smaller id*, deterministically and symmetrically, and the epoch genuinely
  arbitrates slot ownership in `clusterUpdateSlotsConfigWith`. An epoch that nothing compares is not
  a fencing token.
- **Suggested resolution**: Either wire `config_epoch` into ownership arbitration (and fix the
  renumber direction) or state plainly in the spec that FrogDB derives ownership from Raft consensus
  and `config_epoch` exists only for wire compatibility — in which case the three FM rows should say
  so, and the deviations table should record it.

### MAJ-11: `TR-BLOCKING-007`'s "the server's reply normally wins" is false, and it makes issue 08's H5 ruling unsound

- **Where**: `specs/blocking.md` — TR-BLOCKING-007 postcondition; FM-BLOCKING-002. Code:
  `frogdb-server/crates/core/src/shard/blocking.rs:320-322` (deadline fast-path drops the sender),
  `connection/blocking/coordinator.rs:93-102` (`biased;` select with `response_rx` **first**,
  `Err(_)` → `Response::Null`), `protocol/src/response.rs:285` (`Null` → `$-1`) vs `:323`
  (`NullArray` → `*-1`).
- **Claim/gap**: The row asserts the dropped sender's `Err(_)`→`Response::Null` collapse is raced by
  the server's own deadline-elapsed reply, which "normally wins first". It cannot win: the select is
  `biased;` with `response_rx` first, so once the shard drops `response_tx` the coordinator
  deterministically takes the `Err(_)` arm and `op.timeout_reply()` is never consulted — regardless
  of whether the deadline branch is also ready.
- **Why it matters**: `BLPOP q 1`; at t≈1s a producer runs `LPUSH q x`; the satisfaction pass pops
  the waiter, the fast-path sees `deadline <= now` and drops the sender. The client receives `$-1`
  where the protocol requires `*-1` — the wrong-shape bug FM-BLOCKING-002 exists to forbid, on an
  ordinary interleaving (window = one timer tick). Worse, **issue 08's H5 rules the coordinator's
  `Err(_)` arm to `-ERR shard unavailable`**. Applied at that site, an ordinary timeout becomes a
  false `-ERR shard unavailable`, and clients will retry or fail over away from a healthy node. The
  ruling assumes `Err(_)` means shard death, but three live paths signal by dropping the sender:
  admission refusal (`shard/blocking.rs:49-57`), the deadline fast-path (`:320-322`), and
  `Satisfaction::Retry` (`:325`). This is a filed ruling that is itself unsound.
- **Mature-system comparison**: Redis never signals a blocked client by tearing down its reply
  channel; `unblockClientOnTimeout` writes the op-specific null (`addReplyNullArray` for BLPOP,
  `addReplyNull` for BLMOVE). The general rule: a condition-variable teardown must not be overloaded
  to mean three different outcomes.
- **Suggested resolution**: At `blocking.rs:320-322` send `entry.op.timeout_reply()` (and increment
  `BlockedTimeoutTotal`) instead of dropping; likewise for the `Retry` arm and the admission refusal
  (H7 already requires the latter). Only then does `Err(_)` mean "channel died" and H5 become
  implementable. Amend TR-BLOCKING-007 and annotate issue 08's H5 with the ordering dependency.

### MAJ-12: TR-BLOCKING-019 misstates the drain scope, leaving a genuinely unleavable blocked state

- **Where**: `specs/blocking.md` — TR-BLOCKING-019. Code:
  `frogdb-server/crates/core/src/shard/blocking.rs:493-505` and `:512-519` — both loop on
  `pop_oldest_xreadgroup_waiter`, i.e. **only** `BlockingOp::XReadGroup`.
- **Claim/gap**: The row says "Every waiter on that key sharing the failing condition is drained and
  replied to … rather than left parked to time out". False for the `DrainWrongType` arm and for
  `DrainNoGroup`: plain `XREAD` waiters are deliberately left parked, and the code comments at
  `:489-492` and `:510-511` say so explicitly. Row and code contradict each other.
- **Why it matters**: `XREAD BLOCK 0 STREAMS s $` followed by `SET s foo` leaves that client parked
  with no deadline and nothing that will ever re-signal the key as a stream — an unleavable blocked
  state, and (with CRIT-4/CRIT-5) unrecoverable except via `CLIENT UNBLOCK`. The spec's false claim
  that it is drained is precisely what stops a reviewer noticing.
- **Mature-system comparison**: Redis 7 replies `-UNBLOCKED the stream key no longer exists`
  (`unblockDeletedStreamReadgroupClients`) to affected clients. FrogDB replies `NOGROUP …` /
  `WRONGTYPE …`; a repo-wide grep finds no occurrence of the Redis string and no note recording the
  divergence, against the project rule that deviations are documented.
- **Suggested resolution**: Split the row — one postcondition for the XREADGROUP drain with error
  texts pinned, one explicitly stating plain XREAD waiters stay parked — each with its own forcing
  test (`DrainWrongType` currently has none). Record or fix the `-UNBLOCKED` divergence.

### MAJ-13: Blocking inside MULTI/EXEC returns the wrong nil shape, and deny-blocking contexts are unrowed

- **Where**: `specs/blocking.md` — no row exists; FM-BLOCKING-002. Code:
  `frogdb-server/crates/core/src/shard/execution.rs:623-630`:
  ```rust
  let response = if matches!(&response, Response::BlockingNeeded { .. }) {
      Response::Null
  } else { response };
  ```
- **Claim/gap**: The spec has no row for a blocking command executed where it cannot block
  (MULTI/EXEC, Lua), despite that being a standard interleaving and despite
  `test_wait_inside_multi_nonzero_timeout_does_not_block` being cited for WAIT. The conversion
  collapses every op to `Response::Null` → `$-1`, discarding the `op` sitting in the matched variant.
- **Why it matters**: `MULTI; BLPOP k 1; EXEC` on an empty list returns `$-1` where Redis returns
  `*-1` — a third instance of the wrong-shape family FM-BLOCKING-002 polices, on a path the spec
  never mentions, so nothing forces it and `into_response`'s fix does not reach it. (The Lua path at
  `scripting/bindings.rs:184` returning `Boolean(false)` is correct — Lua flattens both nils.)
- **Mature-system comparison**: Redis's `blpopCommand`/`brpopCommand`/`bzpopmin`/`XREAD BLOCK` under
  `CLIENT_DENY_BLOCKING` call `addReplyNullArray`; only `BLMOVE`/`BRPOPLPUSH` call `addReplyNull`.
  Op-awareness is preserved exactly as FM-BLOCKING-002 requires.
- **Suggested resolution**: One-line fix — `op.timeout_reply()` at the conversion site. Add a TR
  row: "a blocking command in a deny-blocking context resolves immediately with the op-aware nil,
  registers no `WaitEntry`, sets no blocked flag", with a forcing test per op family.

### MAJ-14: CLIENT PAUSE interaction is unrowed and diverges from Redis

- **Where**: `specs/blocking.md` — absent from state-space, TR, and FM tables. Code:
  `frogdb-server/crates/server/src/connection/dispatch.rs:568` (`wait_if_paused` runs before
  execution, so a blocking command never reaches `handle_blocking_wait` while paused); the
  divergence is documented only in `redis-regression/tests/list_tcl.rs:2910-2953`.
- **Claim/gap**: FrogDB's own regression test carries a "FrogDB note" recording that CLIENT PAUSE
  blocks at dispatch before the BLPOP enters the wait, so `blocked_client_count` stays 0 — but the
  spec has no row for it.
- **Why it matters**: `CLIENT PAUSE 60000 WRITE` then `BLPOP k 1` waits 61s in FrogDB and 1s in
  Redis. `INFO clients:blocked_clients` and `CLIENT LIST` disagree with Redis for the whole pause
  window, so an operator using pause during a failover cannot see how many clients are parked. The
  regression assertion is `resp.is_none() || matches!(Bulk(None))` — it passes whether or not a reply
  arrives, so it forces nothing.
- **Mature-system comparison**: Redis's upstream test is named "Blocking timeout following PAUSE
  should honor the timeout": the client enters the blocked state and its deadline runs during pause.
- **Suggested resolution**: Add a TR row pinning the chosen semantics; adopt Redis's behavior or
  record the divergence in the deviations doc. Tighten the regression assertion to require a reply.

### MAJ-15: The "Registration ordinal" state-space row asserts a FIFO mechanism the code does not use

- **Where**: `specs/blocking.md` — state-space row for `next_seq`/`seq_by_slot`. Code:
  `frogdb-server/crates/core/src/shard/wait_queue.rs:162-202` (writes), `:604` (the **only** read —
  `dump()`), `:246`/`:430`/`:521` (the pop paths, none of which consult it), `:346`
  (`drain_waiters_for_slot` iterates `self.waiters_by_key.keys()` — HashMap order).
- **Claim/gap**: The row says per-key FIFO order "is this ordinal, **not** insertion order into
  `waiters_by_key` alone", and that the ordinal is read by the diagnostic "and by slot-scoped
  ordering". Both halves are false: the sole reader is `DEBUG WAITQUEUE`; per-key order *is* the
  `VecDeque` push order; and slot-scoped drain order is HashMap-nondeterministic, the opposite of
  the claim.
- **Why it matters**: This is the load-bearing fairness citation for TR-BLOCKING-001/013 and for the
  turmoil `check_fifo_wake_order` checker. An engineer told the ordinal is the tie-break concludes
  that reordering the deque is safe (it is not) and that mutating `seq_by_slot` breaks fairness (it
  does not — such a mutant is invisible to every pop path and survives `mutants-gate` with no
  fairness consequence). The spec is actively misleading the mutation gate.
- **Suggested resolution**: Restate the row — the per-key `VecDeque` is the FIFO authority,
  `seq_by_slot` is diagnostic-only with `dump()` as its sole reader — or make the pop paths actually
  consult the ordinal. Separately, make slot drain order deterministic.

### MAJ-16: `Satisfaction::Retry` silently drops a live waiter and has no row

- **Where**: `specs/blocking.md` — no row. Code:
  `frogdb-server/crates/core/src/shard/blocking.rs:325` (`Satisfaction::Retry => continue` — `entry`
  is dropped with its `response_tx`), `:571-573` (doc: "drop the waiter and re-loop"), `:884-891` and
  `:1041-1048` (`_ =>` arms returning `Retry` after `debug_assert!(false)`).
- **Claim/gap**: The preamble promises "every way a blocking command can be resolved". `Retry` is a
  resolution — the waiter leaves the queue without timing out and gets `Err(_)` → bare
  `Response::Null` — and it is unrowed.
- **Why it matters**: In a release build the two `debug_assert!(false)` arms become a silent nil for
  a client that should have stayed parked. The documented cause ("it lost a race to an earlier waiter
  that emptied the key") is not reachable as written: `check_key` runs immediately before every
  `satisfy` on the same serial thread, so `Yes` followed by a `None` pop cannot happen for the same
  key. The mismatch between the documented cause and the actual guard is the suspicious part.
- **Evidence that would settle it**: a `cargo mutants -p frogdb-core` run over `blocking.rs` showing
  whether any `Retry` return is forced by a test, or a targeted attempt to construct a reachable
  `Retry`.
- **Suggested resolution**: If genuinely dead, make the arms `unreachable!()`. If reachable,
  re-register the entry (Redis re-checks and keeps the client blocked) or send `op.timeout_reply()`.
  Row the outcome either way.

### MAJ-17: A crash between the two install renames re-mints `database_id` and can permanently wedge the boot

- **Where**: `specs/persistence.md` — FM-PERSISTENCE-025, FM-PERSISTENCE-049. Code:
  `frogdb-server/crates/persistence/src/rocks/checkpoint.rs` (`install_staged`: rename `<db>` →
  `<db>_backup_<ts>`, then rename staged → `<db>`),
  `frogdb-server/crates/persistence/src/data_dir.rs:25-30` (Phase 0 runs *before* the install),
  `:113-122` (`require-existing-data` bail), `:133` (`database_id` mint).
- **Claim/gap**: Rename 1 moves the `frogdb_data_dir` marker aside with the old DB. Power loss
  before rename 2 leaves no marker and no files at `<db>`. The next boot's Phase 0 — which runs
  *before* `install_staged` — therefore (a) mints a **new** `database_id`, contradicting
  FM-PERSISTENCE-049, and (b) under `require-existing-data = true` **bails** with "data directory is
  empty" while a complete `checkpoint_ready` sits in the parent. The install code never runs.
- **Why it matters**: FM-PERSISTENCE-025 claims "the next boot finishes the install cleanly". It does
  the opposite: the node is unbootable except via `--force-fresh-data-dir`, which discards the very
  data the staged checkpoint was there to install. And the re-minted `database_id` means that even a
  successful later boot presents as a *different database*, which is the identity that guards against
  cross-cluster data mixing.
- **Mature-system comparison**: CRDB's `StoreIdent` is written first and read before anything else,
  and a store whose ident is missing but whose directory is non-empty is a hard error, never a
  re-mint. The general rule: identity establishment must be the last thing you re-derive, not the
  first.
- **Suggested resolution**: Make Phase 0 mid-install-aware — probe `StagedCheckpoint::for_db_dir` and
  `<db>_backup_*` before concluding "empty", carry `database_id` forward from the backup, and run the
  install before the `require-existing-data` gate. Add an FM row for "crash between the two install
  renames".

### MAJ-18: `checkpoint_ready` and `<db>_backup_*` live in the *parent* of the data dir — an undeclared deployment constraint

- **Where**: `specs/persistence.md` — staged-checkpoint rows. Code:
  `frogdb-server/crates/persistence/src/rocks/staged.rs:58-67`:
  ```rust
  pub fn for_db_dir(db_dir: &Path) -> Option<Self> {
      db_dir.parent().map(Self::in_parent)
  }
  ```
  (verified; `STAGED_CHECKPOINT_DIR = "checkpoint_ready"` at `:28`, `backup_dir_name` at `:94`.)
- **Claim/gap**: The whole staged-install mechanism requires write access to, and rename atomicity
  within, the **parent** of the configured data dir. The spec never states this as a precondition.
- **Why it matters**: In the standard Kubernetes layout the PVC is mounted *at*
  `persistence.data-dir`. Then `rename("/data", "/data_backup_T")` fails `EBUSY` — you cannot rename
  a mount point — so **full resync is permanently impossible on that node**, and the staged download
  writes a full copy of the database into the container's ephemeral root filesystem, where it can
  fill the node's disk. Both failures appear only under the exact conditions (a resync) where you
  most need them not to.
- **Mature-system comparison**: etcd and CRDB confine all snapshot staging *inside* the data
  directory precisely so the only filesystem requirement is the one the operator already provisioned.
- **Suggested resolution**: Stage inside the data dir (e.g. `<db>/../` → `<db>/.staged/`), or at
  minimum add a boot-time precondition probe (create-and-rename a sentinel in the parent, fail fast
  with a clear message) plus an explicit spec row naming the constraint.

### MAJ-19: The full-sync writer's commit rename into `checkpoint_ready` is never fsynced

- **Where**: `specs/persistence.md` — FM-PERSISTENCE-023 (global "every publishing rename is
  bracketed" rule). Code: `frogdb-server/crates/replication/src/fullsync/stager.rs:~110-135`
  (`remove_dir_all` then `fs::rename`, with no `sync_file` on the payload and no `sync_dir` on the
  parent) — contrast `persistence/src/rocks/checkpoint.rs:110-118`, which brackets both install
  renames correctly.
- **Claim/gap**: FM-PERSISTENCE-023 states the bracketing rule globally; this publishing rename
  breaks it. The staged checkpoint becomes *visible* before its bytes are durable.
- **Why it matters**: Power loss after the rename's metadata is durable but before the SST and
  CURRENT bytes are → the next boot's `is_complete_db()` check passes (the names are all there) and
  the install proceeds, writing a **partial database over the live one**. This is distinct from filed
  issue 04 (install-side verification): the point is that directory metadata can be durable while
  file contents are not, so no amount of install-side name checking detects it.
- **Mature-system comparison**: This is the fsync-before-publish discipline TigerBeetle documents at
  length and FoundationDB enforces in its file-backed stores. FrogDB's own persistence crate already
  implements it correctly — the replication crate reimplemented the pattern and dropped the syncs.
- **Suggested resolution**: Use the same bracketed helper as `rocks/checkpoint.rs` (fsync every file,
  fsync the source dir, rename, fsync the parent) — ideally by extracting it to one shared function
  so the two crates cannot drift again. Add a `RecordingFs` trace test on the full-sync path
  mirroring the one that already guards `stamp_with`.

### MAJ-20: Phase-2 lock acquisition has the `participants × timeout` accumulation the spec documents only for phase 4

- **Where**: `specs/vll.md:55` (`acquisition.timeout` row: "phase 4 applies it **per receiver** in a
  sequential loop … so the worst-case phase-4 wall clock is `participants × timeout`"), TR-VLL-017.
  Code: `frogdb-server/crates/vll/src/coordinator.rs:247-266` (phase 2), `:394-409`
  (`acquire_continuation`).
- **Claim/gap**: Phase 2 has the identical shape —
  `for (shard_id, ready_rx) in ready_rxs { timeout(request.timeout, ready_rx) }` — a fresh *relative*
  timeout per receiver, started only when the loop reaches that receiver. The spec calls the
  accumulation out for phase 4 and is silent for phase 2, so a reader takes TR-VLL-017's bound to be
  one `timeout`. `acquire_continuation`'s ready loop has it too and is documented nowhere.
- **Why it matters**: With 16 shards at the 4 s default, a contended scatter can burn 64 s in phase 2
  before aborting, and another 64 s in phase 4 — a request that hangs over a minute against a 4 s
  configured timeout. Directly compounds CRIT-7.
- **Mature-system comparison**: gRPC and CockroachDB's DistSender compute a single *absolute*
  deadline once at request entry and pass it down, so per-hop waits cannot sum. This is the standard
  fix and the reason it is standard.
- **Suggested resolution**: One `Instant` deadline computed at `scatter` entry; `timeout_at` on every
  receiver in phases 2 and 4 and in `acquire_continuation`. Restate the `acquisition.timeout` row as a
  total bound rather than a phase-4-only quirk.

### MAJ-21: WATCH is slot-granular with a shard-wide epoch; the spec describes it as key-granular, and the epoch admits unbounded WATCH starvation

- **Where**: `specs/txn.md:707-710` (FM-TXN-033: Trigger "Any watched key's version moved… another
  client's write"), Redis deviations table `:905-909` (four rows, none about this). Code:
  `frogdb-server/crates/core/src/shard/worker.rs:56-103` (`SlotVersions::version_for` = per-slot stamp
  + `global_epoch`), `:633` (`get_key_version` = `version_for(slot_for_key(key))`), `:648-663`
  (`check_watches`), `shard/event_loop.rs:365`
  (`if result.fields_expired > 0 { self.bump_version_global(); }`).
- **Claim/gap**: Two divergences from the spec's per-key language, neither rowed nor listed as a
  deviation. (1) **Slot aliasing** — a write to *any* key sharing the watched key's hash slot aborts
  the CAS; Redis sets `CLIENT_DIRTY_CAS` per watched key. (2) **Global epoch** — every active-expiry
  cycle that reaps at least one hash *field* bumps the shard-wide epoch, invalidating **every**
  outstanding WATCH on the shard. The code comment calls this a "safe over-abort": safe for the
  safety property (no false negatives), but nothing bounds the liveness cost.
- **Why it matters**: A tenant using `HEXPIRE` field TTLs on a continuously-expiring hash bumps
  `global_epoch` every expiry cycle. Any *other* connection on that shard running a WATCH/MULTI/EXEC
  CAS loop gets a nil abort on every attempt, forever — no forward progress, no error, and no metric
  that explains why. Slot aliasing is the milder version: on a shard owning ~1300 slots under 100k
  writes/s, roughly 70 unrelated writes/second land in any given watched slot.
- **Mature-system comparison**: Redis/Valkey key-granular dirty-CAS never aborts on unrelated
  activity. FoundationDB uses precise read-conflict ranges; a coarse fallback there would be a
  *documented* conflict-range widening, never silent.
- **Suggested resolution**: Add both to the Redis-deviations table with FM-TXN-033 cross-references,
  and give the epoch bump a bounded story — enumerate the field-expiry victim keys so the bump can be
  per-slot (the lazy path already does exactly this at `worker.rs:768-776`, bumping `shrunk` keys'
  slots rather than the epoch) — or at minimum emit `watch_aborted{reason="epoch"}` so starvation is
  diagnosable.

### MAJ-22: `WATCH k1 k2` across shards is refused with `-CROSSSLOT`, which FM-TXN-049 declares NOT observable

- **Where**: `specs/txn.md:~897` (FM-TXN-049 NOT-observable: "**not** observable: a `-CROSSSLOT` for a
  watch set legitimately spanning two slots this node owns (only the *queue* is
  co-location-constrained, FM-TXN-019 — watch sets are not)"), FM-TXN-020. Code:
  `frogdb-server/crates/server/src/connection/transaction_conn_command.rs:285-293`
  (`SlotValidator::same_shard(args, num_shards)` runs before anything else),
  `frogdb-server/crates/server/src/slot_migration/validator.rs:31-44` (returns `redirect::crossslot()`).
- **Claim/gap**: `handle_watch` rejects a multi-key WATCH whose keys land on different *internal
  shards*, in every mode, cluster or standalone, before any cluster snapshot is consulted. Two slots
  the node legitimately owns land on different shards in the normal case, so the exact reply FM-TXN-049
  says is unobservable is trivially reachable.
- **Why it matters**: `WATCH {a}x {b}y` → `-CROSSSLOT`, but `WATCH {a}x` then `WATCH {b}y` builds the
  same watch set successfully and reaches EXEC (that is FM-TXN-020's own scenario). Semantically
  identical inputs, different answers, decided by argument packing — a client library that batches its
  watch list fails where a one-key-per-call library succeeds. Also an unlisted Redis deviation; the
  existing FM-TXN-019/021 row covers the *queued batch*, not WATCH.
- **Suggested resolution**: Decide which side is right — either fan the `GetVersion` round-trip out
  per shard (matching the two-call path and FM-TXN-049's text), or accept the refusal and fix
  FM-TXN-049's NOT-observable, FM-TXN-020's premise, and the deviations table. Existing coverage
  misses it: `slot_migration/tests.rs:598` (`watch_across_two_slots_is_crossslot`) exercises only the
  cluster `route_watched_keys` path, not `same_shard`.

### MAJ-23: `take` folds every watched shard, not every *live* one — the spec's qualifier is unimplemented

- **Where**: `specs/txn.md:30` (state-space `txn.slots.target`: "**also** `fold_shard` on every *live*
  watched shard … `state.rs:285-287`") and FM-TXN-020 Invariant (`:554`: "`take` folds every *live*
  watched shard into the target"). Code: `frogdb-server/crates/txn/src/state.rs:285-287`:
  ```rust
  for &(shard_id, _, _) in self.watches.values() {
      self.slots.fold_shard(shard_id);
  }
  ```
  The `live_at_watch` component is destructured away; a grep across `crates/txn/src` finds no
  `live_at_watch` filter on any `fold_shard` call site.
- **Why it matters**: The canonical create-if-absent CAS breaks. `WATCH counter` where `counter` does
  not exist (recorded `live_at_watch = false`, on shard B), then `MULTI; SET other 1; EXEC` where
  `other` is on shard A. The spec says the dead watch contributes nothing, so this commits; the code
  folds shard B anyway → target `Multi` → `-CROSSSLOT` at EXEC. Client-visible, and it hits standalone
  too since FrogDB shards standalone.
- **Suggested resolution**: Pick a side and pin it. Filtering on `live_at_watch` is the behavior the
  spec describes and is safe (a dead watch that stays dead never aborts — FM-TXN-033's gap-4 clause —
  so folding its shard buys nothing). Either way FM-TXN-020 needs a forcing test distinguishing a live
  from a dead cross-shard watch: today's two tests
  (`cross_shard_watch_set_folds_to_multi_at_take`, `take_transaction_folds_cross_shard_watch_set_to_multi`)
  cannot tell the two implementations apart, so no mutant on the filter would ever be killed.

### MIN-1: FM-CLUSTER-037's "bounded by Raft apply latency" is unsupported

- **Where**: `specs/cluster.md` — FM-CLUSTER-037.
- **Claim/gap**: Neither cited test measures or bounds the interval, and the claim ignores VLL
  queueing, blocking-command parking, and pause delays that sit between the snapshot read and command
  execution. The true bound is apply latency *plus* the execution pipeline's queueing delay.
- **Suggested resolution**: Either weaken to "eventually, after the apply and the execution queue
  drain" or add a test that actually measures the interval under load.

### MIN-2: TR-CLUSTER-036 and FM-CLUSTER-008 disagree on whether FinalizeUpgrade's version check is state-machine-checked

- **Where**: `specs/cluster.md` — TR-CLUSTER-036 ("not state-machine-checked") vs FM-CLUSTER-008
  (says it is).
- **Claim/gap**: Direct contradiction between two locked rows. Separately, an irreversible finalize
  is accepted on empty membership, reachable via TR-CLUSTER-035's else branch.
- **Suggested resolution**: Resolve the contradiction against the code; add a precondition that
  membership is non-empty before an irreversible finalize.

### MIN-3: TR-CLUSTER-005's ruled MEET precondition names state the deciding node cannot observe

- **Where**: `specs/cluster.md` — TR-CLUSTER-005 (ruled).
- **Claim/gap**: The precondition refers to local Raft state of the *joining* node, which the
  deciding node has no access to. As written it is unenforceable.
- **Mature-system comparison**: etcd enforces the equivalent check at the joining node
  (`--initial-cluster-state existing` plus a data-dir emptiness check), not at the acceptor.
- **Suggested resolution**: Move the check to the joining node, or restate it in terms of something
  the acceptor can observe.

### MIN-4: FM-CLUSTER-073 counts unknown-owner slots as `ok`

- **Where**: `specs/cluster.md` — FM-CLUSTER-073, in tension with FM-CLUSTER-033's reasoning.
- **Claim/gap**: Fail-open observability: a slot whose owner cannot be determined is reported
  healthy, so the health metric is at its most optimistic exactly when the cluster is most confused.
- **Suggested resolution**: Report unknown-owner slots in a third state (`unknown`) rather than
  folding them into `ok`.

### MIN-5: No read-consistency contract is stated anywhere

- **Where**: `specs/cluster.md` — absent.
- **Claim/gap**: The self-fence covers writes only. No row documents what a read may return on a
  fenced or partitioned node, and there is no stated stale-read non-guarantee.
- **Mature-system comparison**: Redis exposes `replica-serve-stale-data` as an explicit knob with
  documented semantics; CRDB documents follower reads and their staleness bound precisely.
- **Suggested resolution**: Add a row stating the read contract (most likely "reads may be stale on a
  fenced node; no bound is offered") so applications can reason about it.

### MIN-6: `wal_watermark::write`'s no-torn-body reasoning is filesystem-dependent and the dependency is unnamed

- **Where**: `specs/persistence.md` — `wal_watermark` rows.
- **Claim/gap**: The argument rests on ext4's `auto_da_alloc` behavior; it does not hold on XFS,
  btrfs, or ext4 mounted `data=writeback`. The dependency is never stated.
- **Why it matters**: Degrades safely (a torn body parses as absent), so this is minor — but an
  unstated filesystem assumption in a durability argument is exactly the thing that becomes load-
  bearing after the next refactor.
- **Suggested resolution**: Name the assumption in the row, or add a checksum so the argument is
  filesystem-independent.

### MIN-7: Backup directories are wall-clock-named with `unwrap_or(0)` pruning and retention 1

- **Where**: `specs/persistence.md` — backup/retention rows. Code: `backup_dir_name(db_name,
  unix_secs)` at `staged.rs:94`; `BACKUP_RETENTION = 1`.
- **Claim/gap**: An NTP step-back makes the newest backup sort oldest, so `prune_backups` deletes the
  only rollback copy. A same-second reinstall fails `ENOTEMPTY`. A malformed suffix parses via
  `unwrap_or(0)`, sorts first, and is deleted.
- **Why it matters**: Same class the campaign already ruled against elsewhere (issue 17's
  "log-ordered fence, no wall-clock"), applied to the last-resort rollback copy.
- **Suggested resolution**: Use a monotonically increasing counter persisted alongside the data dir;
  refuse to prune anything whose name does not parse.

### MIN-8: TR-BLOCKING-003's current-code cell misdescribes the client-visible shape

- **Where**: `specs/blocking.md` — TR-BLOCKING-003. Code: `coordinator.rs:40` vs `:101`,
  `response.rs:285`.
- **Claim/gap**: The cell says the dropped sender resolves to `Response::Null` "exactly as an
  ordinary timeout would (TR-BLOCKING-009)". TR-BLOCKING-009 yields `op.timeout_reply()` — `*-1` for
  BLPOP; the drop yields `$-1`. This is the sentence that makes the admission-limit bug look benign,
  and it repeats the confusion issue 08's H5 was filed to remove (see MAJ-11).
- **Suggested resolution**: Correct the cell to state the shape difference.

### MIN-9: TR-BLOCKING-020 omits the `timer_sweeps` gate

- **Where**: `specs/blocking.md` — TR-BLOCKING-020. Code:
  `frogdb-server/crates/core/src/shard/event_loop.rs:83` —
  `_ = waiter_timeout_interval.tick(), if timer_sweeps =>`.
- **Claim/gap**: The GC backstop is suppressed in driven/deterministic runs (it arrives as a
  `DriveTick` instead — `ShardWorker::set_driven_ticks`). The row cites the interval unconditionally,
  so a reader concludes the backstop is always armed. It also explains part of the row's
  `Forced by | MISSING`: a turmoil test must drive the tick explicitly.
- **Suggested resolution**: Add the gate to the precondition and note the driven-tick path.

### MIN-10: `CLIENT UNBLOCK` is a silent no-op during the registration window

- **Where**: `specs/blocking.md` — state-space "Client-registry blocked mirror" row (ordering not
  mentioned). Code: `connection/blocking.rs:99-117` (`register_wait` sends `BlockWait` to the shard
  and only *then* calls `update_blocked_state(id, true)`),
  `core/src/client_registry/mod.rs:729` (`unblock` returns `false` unless `ClientFlags::BLOCKED`).
- **Claim/gap**: A `CLIENT UNBLOCK` landing after the shard registers the entry but before the mirror
  is set replies `0` and does nothing, though the client is genuinely parked.
- **Suggested resolution**: Set the mirror before the send (the shard-side entry is what
  `UnregisterWait` cleans up either way); state the ordering in the row.

### MIN-11 (suspicious): TR-PERSISTENCE-010(b)'s group-commit assumption is untested

- **Where**: `specs/persistence.md` — TR-PERSISTENCE-010(b): "a successful `sync` commit has fsync'd
  the WAL through this batch's sequence".
- **Claim/gap**: This depends on RocksDB's group-commit behavior for *mismatched* `sync` flags — can
  a `sync=true` follower join a `sync=false` leader's write group and return `Ok` without an fsync?
  Every forcing test on this row is single-writer, so the multi-writer case is unexercised.
- **Evidence that would settle it**: cite RocksDB's `write_thread` rule for mismatched sync flags in
  the row, or add a concurrent mixed-flag `PageCacheSink` test asserting the sync count.
- **Suggested resolution**: Do one of the two. If RocksDB does allow the join, this is a Critical, not
  a suspicion — it would mean acknowledged `sync` writes are not durable.

### MIN-12: FM-TXN-013's Invariant names a mechanism that does not exist

- **Where**: `specs/txn.md:470` (FM-TXN-013 Invariant: "`unwatch_all` clears the watch set *and* the
  watch-derived half of the slot accumulator, so `take` re-folds from the queue alone"). Code:
  `frogdb-server/crates/txn/src/state.rs:265-267` — `pub fn unwatch_all(&mut self) { self.watches.clear(); }`.
- **Claim/gap**: There is no watch-derived half of the accumulator to clear, because the watch fold is
  deferred to `take` (MAJ-23). The NOT-observable holds, but for a different reason than the Invariant
  states.
- **Why it matters**: In a LOCKED, spec-first area the Invariant column is what a future change is
  checked against. Anyone moving the fold to WATCH time — an obvious optimization that removes the
  per-EXEC loop — would read this Invariant as already satisfied and ship the stale-fold bug the five
  forcing tests exist to prevent.
- **Suggested resolution**: Reword to the real mechanism ("the fold is taken at `take`, from the
  current watch set, so a cleared set contributes nothing; there is no separately-cleared accumulator
  half") and keep the FM-TXN-020 cross-reference.

### MIN-13: A parked continuation keeps the drain barrier up after its requester has gone

- **Where**: `specs/vll.md` — FM-VLL-003 / FM-VLL-004 (Triggers cover a live requester only),
  TR-VLL-016. Code: `frogdb-server/crates/vll/src/shard.rs:383-405` (`next_continuation_event` selects
  on the release receiver or `sleep_until(deadline)` — never on `ready_tx.closed()`), `:346-357`.
- **Claim/gap**: `grant_continuation` correctly declines to install a lock for an abandoned requester
  (`shard.rs:344-350`, documented by TR-VLL-012). The *parked* state has no such cancellation path:
  once the coordinator times out or the connection dies, the park persists until the shard happens to
  drain fully or the 2 s deadline fires, and for that whole window every SCA request on the shard is
  refused with `-BUSY shard busy with continuation lock; retry`.
- **Why it matters**: A dead transaction degrades a shard for every other connection, and under a
  retrying script client the window re-arms on each attempt. FM-VLL-004's Invariant argues the barrier
  terminates because "the queue can only shrink" — true, and silent about the case where the thing the
  barrier exists to serve no longer exists.
- **Suggested resolution**: Add a `ready_tx.closed()` arm to `next_continuation_event` so an abandoned
  park drops immediately; extend FM-VLL-003's NOT-observable with "the barrier outliving its
  requester". The `parked_continuation_deadline_survives_cancellation` fixture already exists.

### MIN-14: Rate-limit tokens and the pause wait are spent before the CROSSSLOT gate

- **Where**: `specs/txn.md` — TR-TXN-004 / FM-TXN-019 ordering. Code:
  `frogdb-server/crates/txn/src/exec.rs:150` (`try_acquire_batch`), `:203-207`
  (`queue_has_writes` / `wait_if_paused`), `:278-283` (`target.resolve()` → CROSSSLOT).
- **Claim/gap**: A transaction that can never execute (cross-shard target) still charges the user's
  rate limiter for every queued command and byte, and still blocks on the CLIENT PAUSE barrier, before
  being told so. Only the `exec_abort` latch short-circuits earlier (`:140`) — and FM-TXN-016's
  Postcondition explicitly asserts no charge there, which does hold.
- **Why it matters**: A client in a CROSSSLOT retry loop burns its own quota at full rate and sees
  rate-limit errors attributed to the wrong cause. Minor because the gate order is otherwise sound and
  the spec makes no contrary promise — but FM-TXN-016 promises exactly this property for the abort
  latch, so the asymmetry deserves a stated rationale.
- **Suggested resolution**: Resolve the target before the limiter charge, or add a sentence to
  TR-TXN-004's Postcondition making the charge-then-reject order deliberate.

## Things checked and found sound

- **FM-PERSISTENCE-034 / TR-PERSISTENCE-049** — `DBRecoveryMode::PointInTime` is explicitly set, not
  relied on as a default.
- **FM-PERSISTENCE-032** — `list_cf` errors genuinely propagate rather than being swallowed into an
  empty-CF-set fresh start.
- **FM-PERSISTENCE-049** — `stamp_with` really does write → `sync_file` → rename → `sync_dir`, and a
  `RecordingFs` trace test asserts the exact order. This is the pattern MAJ-19 and CRIT-6 should copy.
- **FM-PERSISTENCE-050** — `read` maps only `NotFound` to `Ok(None)`; other IO errors propagate, so a
  permissions fault cannot masquerade as a fresh data dir.
- **FM-PERSISTENCE-048** — `contains_files` recurses and treats symlinks as content (no
  empty-dir false negative).
- **FM-PERSISTENCE-024** — `is_complete_db()` gates *before* the first rename, so an incomplete
  staged dir cannot displace a live one.
- **FM-PERSISTENCE-023** — the install and snapshot halves are correctly bracketed (the full-sync
  half is MAJ-19).
- **FM-PERSISTENCE-001/002** — `group_depth` suppression and `set_sync(is_sync)` behave as rowed.
- **FM-PERSISTENCE-043** — `durable_sync` snapshots `committed_sequence()` *before* `flush()`, which
  is exactly the correct pattern `record_wal_watermark` fails to follow (CRIT-3).
- **TR-PERSISTENCE-007** — `confirm_durable_through` is genuinely non-waiting and fails on
  `highest_failed_seq > after_seq`.
- The `apply_clear` barrier sequence is correct as specified.
- **Blocking single-shard key invariant** — `handle_blocking_wait` routing on
  `shard_for_key(&keys[0])` is sound because every blocking spec sets `requires_same_slot: true` and
  `routing.rs:106-121` returns `-CROSSSLOT` for multi-shard key sets in cluster **and** standalone
  multi-shard mode. I specifically hunted for the keys-2..n-register-on-the-wrong-shard lost wakeup;
  it is closed.
- **FM-BLOCKING-005 serialization** — `cleanup_wait` / `reconcile_unregister` / `reconcile_ack` and
  `handle_unregister_wait` really do serialize on one mailbox (`ShardMessage` over one
  `message_rx`), the receiver is borrowed not consumed, and all four cited tests exist and force both
  `UnregisterAck` variants plus both channel-closure degradations.
- **TR-BLOCKING-008 restore** — the `Err(_)` arm calls `apply_restore`, which restores order
  correctly (reverse re-insert at the same end), recreates a key the wake had deleted, and undoes both
  ends of a BLMOVE including `src == dest`. `propagate` is pushed only in the `Ok(())` arm, so a
  restored serve ships nothing to replicas.
- **Serve-propagation buffer cannot leak** — `pending_serve_propagations` is drained with
  `std::mem::take` unconditionally in the terminal `ReplicationBroadcast` effect, and
  `try_satisfy_*_waiters` has exactly one production caller. The `WaiterSatisfaction`-before-
  `WalPersistence` position in `WRITE_EFFECT_ORDER` is what stops a served pop resurrecting after a
  restart — a genuinely well-designed ordering.
- **WAIT fence ordering and tie-break** — fence-then-role-check ordering and the `biased;`
  role-change-beats-CLIENT-UNBLOCK arbitration are real, with the cited unit tests present.
- **Blocking admission-limit and GC-tick citations** — config defaults (10000/50000, both
  `#[param(skip)]`) and the 100ms interval match the state-space row (subject to MIN-9).
- **No promotion-while-blocked row needed for non-WAIT ops** — blocking commands carry
  `CommandFlags::WRITE` and a replica answers `-READONLY`, so a non-WAIT waiter reaches a replica only
  via demotion. TR-BLOCKING-016's framing is right.
- **XREAD BLOCK unit conversion** — the ms `BLOCK` argument is correctly divided by 1000 before
  reaching the seconds-based `handle_blocking_wait` (checked specifically because a 1000× error there
  would be invisible in the spec).
- **Replication three-head discipline** — `landed <= claimed <= received` holds at every cited site;
  the `ApplyGate{frozen, stint}` and `DivergedEpoch` latch are consistently used.
- **PSYNC2 failover window** — the inclusive-boundary deviation from Redis's `+1` is deliberate,
  correct, and documented in the deviations table.
- **`FRPL` frame header exclusion from the offset** — consistent across the writer and reader paths.
- **Full-resync WATCH invalidation** — I specifically hunted for a snapshot install replacing a
  shard's whole keyspace without moving any WATCH version (which would let a pre-resync CAS commit
  against a wholesale-replaced dataset). It is closed: `dispatch_replication.rs:118-140` runs the
  install through the `FLUSHDB` scatter-effect handler with an empty key set, reaching `bump_keys([])`
  → `bump_global()`, invalidating every watch on the shard — and the `dirty_delta = -1` guard
  correctly suppresses the bump only for a genuinely empty install.
- **`version_for` monotonicity** — `per_slot + global_epoch` is ABA-free: both summands are
  increment-only, so the sum strictly increases whenever either moves, and each key is compared
  against its own slot's sum.
- **FM-TXN-050 first-wins re-WATCH** — `watch_key` uses `entry().or_insert(...)`, so `live_at_watch`
  cannot be laundered by a re-WATCH; matches Redis' `watchForKey()` early return.
- **TR-VLL-012 abandoned-requester grant** — `grant_continuation` checks
  `ready_tx.send(...).is_err()` *before* installing either the lock or the release receiver, so a
  timed-out coordinator cannot leave an unreleasable shard-exclusive lock behind. The Postcondition's
  "unless that `ready_tx` is already closed" clause is exact.
- **`try_grant` all-or-nothing** — every key is checked before any `granted` flag is set; a failed
  grant leaves zero partial state, as TR-VLL-002 asserts.
- **`LockTable::release` covers abort and completion identically**, including never-granted intents;
  `release_of_ungranted_intent_unblocks_sca` pins it.
- **Duplicate keys in a lock request are idempotent** (`BTreeMap` insert by txid), so `WATCH k k` /
  `MSET k v1 k v2` shapes need no dedup at the lock layer.
- **Scatter phase-3 partial-failure unwind** aborts by real shard id and correctly excludes
  participants that already received `VllExecute` — TR-VLL-018's all-or-nothing `result_rxs`
  reasoning checks out.
- **`purge_expired_watches` runs before `check_watches`** at the EXEC seam, and the WATCH-time purge
  correctly withholds the version bump (`apply_lazy_purge_effects_no_version_bump`) — the F3/gap-4
  story in TR-TXN-025 and FM-TXN-033 is implemented as written.
- **`take_transaction` consumes queue and watches atomically**, so no EXEC exit path can leak a stale
  watch set — FM-TXN-047's "the queue is consumed either way" holds on every branch traced.
