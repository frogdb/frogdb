# Cross-shard `MULTI`/`EXEC` in standalone — stage-1 design

Status: DRAFT — pending HITL design review (gate for stage 2 implementation)
Issue: [spec-gaps 29](issues/open/29-cross-shard-transaction-per-shard-undo.md)
Ruling: [R8, 2026-08-22 work-item rulings](../cluster-correctness/2026-08-22-work-item-rulings.md)
Author: agent (stage 1, design only — no code, no spec edits)

---

## 0. Scope, and what the ruling changed

Issue 29 was filed as "per-shard undo". Ruling R8 corrects the framing:

> Redis `EXEC` is **run-all**: a command that fails at runtime puts its error in the reply array
> and the rest of the batch still executes. There is no rollback to build. The missing piece is
> **crash-atomicity across per-shard WALs** — transaction markers / a commit record, the
> AOF-wrapping equivalent.

So this design deliberately contains **no undo log, no before-images, no runtime rollback**. It
answers one question: *when a shard-spanning `EXEC` is accepted in standalone mode, how does the
batch become durable as one unit, and how is it observed and replicated as one unit?*

Binding constraints carried from the rulings:

- **No wall clock in correctness decisions.** Timeouts may report; they may not decide commit.
- **Fail closed over available.** An undecidable commit is a refusal or a fail-stop, never a guess.
- **Run-all semantics.** Per-command errors are data in the reply array, not control flow.
- **Cluster mode keeps `-CROSSSLOT`** (§5.5).

Out of scope for stage 2 unless the reviewer says otherwise: cross-shard EVAL (§7 O-6), cross-shard
`MSET`/scatter atomicity (§7 O-5), cluster-mode multi-slot transactions (never).

---

## 1. The load-bearing facts reconnaissance turned up

These five change the shape of the answer, so they come first.

**F1 — "per-shard WALs" is a logical statement, not a physical one.** There is one RocksDB, one
column family per shard, and **one shared WAL file**
(`frogdb-server/crates/persistence/src/wal/flush.rs:186-196` says so explicitly). A single
`rocksdb::WriteBatch` spanning several CFs is already crash-atomic, and the repo already proves it:
the test-only seam `RocksStore::commit_raw_batch` / `RawBatchOp { shard, .. }`
(`frogdb-server/crates/persistence/src/rocks/mod.rs:676-719`) is exercised by
`test_cross_shard_batch` (`frogdb-server/crates/core/src/persistence/crash_recovery_tests.rs:507-557`),
which asserts `found_count == 0 || found_count == 4` across two shards after a crash.

**F2 — the obstacle is architectural, not physical.** Each shard owns a private `RocksWalWriter`,
a private flush thread (`wal-flush-{shard_id}`), a private `RocksSink` with a private `WriteBatch`,
and a private `group_depth`. `begin_group`/`end_group`
(`frogdb-server/crates/core/src/shard/persistence.rs:93-95`) bracket **one shard's** batch. Two
shards cannot contribute to one storage batch today, and no WAL layer carries a transaction id.

**F3 — no code path in FrogDB is cross-shard atomic today, including the one that looks like it
is.** A continuation-locked cross-shard Lua script runs each remote sub-command through
`run_script_write_effects` on the *owning* shard
(`frogdb-server/crates/core/src/shard/scripting.rs:294-302` →
`post_execution.rs:648-674`), so it emits N independent WAL groups and N independent replication
units. VLL buys isolation only. Whatever protocol `EXEC` gets should be reusable by `EVAL` (§7 O-6).

**F4 — the replication framing has a hard single-shard premise, in two places.**
`ReplicationBroadcaster::broadcast_transaction_on_shard`
(`frogdb-server/crates/replication/src/lib.rs:141-154`) tags a whole `MULTI…EXEC` group with one
origin shard and holds **no lock across its N+2 `OffsetCoordinator::advance` calls**. On the replica,
`consume_frames` (`frogdb-server/crates/replication/src/apply.rs:377`) keeps exactly **one node-wide**
`pending: Option<PendingTxn>` and its default arm pushes any frame into the open group **ignoring
`frame.shard_id`**. `specs/replication.md` FM-REPLICATION-034 states the premise as a fact.
Consequence recorded as a present-tense hazard in §4.4.

**F5 — `TransactionOutcome` has no ambiguous variant, and the spec currently forbids one.**
FM-TXN-032 (`specs/txn.md:699-709`) rules that both `Unavailable` ("never accepted") and `Dropped`
("accepted, fate unknown") map to the `Error` outcome and "no path invents a `Committed`". The
possibly-applied vocabulary lives only on the VLL side as
`ScatterError::ResultAmbiguous { shard_id, applied, unknown }`
(`frogdb-server/crates/vll/src/coordinator.rs:115`). Adding an ambiguous transaction outcome is a
spec delta, not just a code change (§5.1).

**F6 — the refusal has exactly one choke point, and the flag never reaches it.**
`TransactionTarget::resolve` (`frogdb-server/crates/txn/src/state.rs:33`) maps `Multi(_)` to
`Err(redirect::crossslot())`; `frogdb-txn/src/exec.rs:177-182` consumes it, second in the gate order
— above the rate limiter, `validate_queued_batch`, and the pause barrier.
`allow_cross_slot_standalone` is read once
(`frogdb-server/crates/server/src/server/subsystems.rs:573`) and reaches routing and EVAL, never
`frogdb-txn`. Defaults today: `allow_cross_slot_standalone = false`, `num_shards = 1` — so the
default deployment cannot produce a shard-spanning batch at all. The deviation only bites operators
who set `num_shards > 1` *and* turned the flag on.

---

## 2. Design point 1 — the cross-WAL crash-atomicity protocol

### 2.1 Prior art

**Redis AOF (web-verified).** Redis wraps a replicated/persisted transaction by propagating an
explicit `MULTI`, the effective commands, and an `EXEC` into one AOF write. On load, a partial
trailing transaction is not applied: the loader tracks the offset of the last position before the
open `MULTI` (`valid_before_multi`) and, under `aof-load-truncated yes`, truncates the tail back to
that offset; a partial `MULTI` with `aof-load-truncated no` is a fatal load error. So Redis's
crash-atomicity for a multi-command batch is **tail truncation to a marker**, not undo. The
structural lesson: the *marker* only has to be honored by a recovery pass that is already willing to
discard a suffix.

**DragonflyDB (mix of fetched and training knowledge).** Fetched from `main` today:
`src/server/journal/types.h` has `enum class Op : uint8_t { SELECT, EXPIRED, COMMAND, PING, LSN }`
and `struct EntryBase { TxId txid; Op opcode; DbIndex dbid; uint32_t slot; LSN lsn; }` — every
journal entry carries a **txid**. `src/server/journal/tx_executor.h` has `MultiShardExecution` with a
per-`TxId` `TxExecutionSync { barrier, counter, BlockingCounter block }`: the replica groups entries
by txid across shard flows and executes the multi-shard transaction only once every participating
shard flow has arrived. *Training knowledge, marked as such and not verified against current
sources:* earlier Dragonfly journal versions carried explicit multi-command opcodes and a
`shard_cnt` field so the replica knew how many flows to wait for. The lesson we take is the
**txid-keyed rendezvous on the apply side**, which §4 reuses.

**Elsewhere (training knowledge).** MySQL's binlog/InnoDB XA and PostgreSQL's `PREPARE
TRANSACTION` both use the classic shape our fallback (§2.5) copies: participants stage, one
designated log owns the commit record, recovery re-drives staged-and-committed and discards
staged-and-uncommitted. FoundationDB and CockroachDB instead make the commit a single atomic write
to one place (a resolver/txn record), which is closer to what §2.2 chooses.

### 2.2 Chosen protocol: one storage batch is the commit unit ("single-batch commit")

> **Commit-decision owner: the shared RocksDB WAL.** The commit point is one
> `write_batch_opt(batch, sync = mode is Sync)` call carrying every participating shard's entries.
> Nothing before it is durable as part of the transaction; nothing after it can be partial.

This is the structural analogue of Redis's AOF block: one indivisible unit that a truncating
recovery either has whole or does not have. RocksDB already gives us the truncation rule for free —
`DBRecoveryMode::PointInTime` (`rocks/mod.rs:193`) truncates at record boundaries and a `WriteBatch`
is one record group, so a torn tail drops the whole batch.

Sequence, under one continuation lock over the sorted participant set (dispatch shape in §3):

1. **Coordinator** allocates `txid` (the same id used for the VLL lock, so wound-wait ordering and
   the commit protocol agree) and a `CrossShardCommit` rendezvous handle:
   `{ txid, participants: BTreeSet<usize>, arrived, parts: Vec<(shard, Vec<WalEntry>)>, outcome }`.
2. **Each participant shard worker** runs its slice of the queued commands (run-all), applies them
   in memory as today, collects `WriteRecord`s, and instead of `persist_records`'s local
   `begin_group`/`end_group`/`flush_through` calls
   `WalTarget::join_cross_shard_commit(txid, actions, handle)`. That enqueues one
   `WalCommand::CrossShardCommit { txid, entries, handle }` on **its own** FIFO
   (`RocksWalWriter::cmd_tx`), so it is ordered behind every earlier entry from that shard.
3. **Each participant flush thread**, on dequeuing it:
   a. commits its currently-staged (pre-`EXEC`) batch first — this preserves per-shard write order,
      and is safe because those entries are not part of the transaction;
   b. registers `entries` with the rendezvous;
   c. if it is not the last arriver, blocks on the rendezvous.
4. **The last arriver is the committer.** It stages every participant's entries into **one**
   `WriteBatch`, ordered by `(shard_id, per-shard enqueue order)` — the CF-addressed staging that
   `RawBatchOp { shard, .. }` already proves — appends the commit record (§2.3), and issues the
   single `write_batch_opt`. It publishes `Ok(rocks_seq)` or `Err(e)` on the rendezvous.
5. **Every participant wakes**, adopts the shared outcome into its own `committed_seq` /
   `synced_seq` / lag stats / poison latch, and returns.
6. **Coordinator** gathers per-shard reply slices, merges them by original queue index, and — for
   `Durability::Committed` — awaits the *single* rendezvous outcome rather than N `flush_through`s.

Why the rendezvous instead of a coordinator-assembled batch: entries already queued on a
participant's FIFO must commit before the transaction's entries, or a crash could expose the
transaction's value while losing an earlier write to the same key. Routing through each shard's own
FIFO gets that ordering for free (rejected alternative A3, §8).

**No wall clock anywhere in this path.** The rendezvous is resolved only by structural events:
all participants arrived, a participant reported a staging failure, or a participant's channel
disconnected (which is already node-fatal — `FM-PERSISTENCE-010` territory / `AbortFailStop`). There
is no rendezvous timeout. An observability-only watchdog may log and increment a metric when a
rendezvous stays open a long time; it never decides.

**Group-commit interaction.** TR-PERSISTENCE-010 leans on RocksDB's `WriteThread` refusing to admit
a follower whose sync flag differs. Our merged batch carries **one** sync flag, so there is no
downgrade hazard and no need for a new rule.

**Durability modes.** Atomicity is unconditional; durability follows the configured mode, exactly
as for a single-shard `EXEC` today. `Sync`: the batch write is both commit point and durable point,
and the client is acked after it. `Periodic`/`Async`: a crash can lose the whole transaction, never
part of it.

**Truncation / checkpointing.** FrogDB does not truncate the WAL; RocksDB does. A checkpoint is a
whole-DB `atomic_flush` (`rocks/mod.rs:217`), i.e. a consistent `rocks_seq` cut, and a cut cannot
split a single `WriteBatch`. So this protocol also closes — for `EXEC` only — the deliberate
non-guarantee recorded under FM-PERSISTENCE-001 ("**Cross-shard atomicity is not provided**",
`specs/persistence.md:610`, pinned by
`test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave`). Cross-shard `MSET`/scatter
keeps the old, honest limitation unless §7 O-5 rules otherwise.

### 2.3 The commit record

Layout (one extra `Put` inside the same batch, so it exists **iff** the batch committed):

| field | purpose |
|---|---|
| `version: u8` | forward compat |
| `txid: u64` | the VLL txid; matches the replication frame tag (§4) |
| `participants: Vec<u16>` | sorted shard ids |
| `entry_counts: Vec<u32>` | per participant, same order |
| `wal_seq: Vec<u64>` | per participant, that shard's post-append sequence |

Key: `txid` big-endian, in a dedicated `txn_commit` column family (recommended) — see §7 O-3 for
the alternative of a reserved `\xff` key inside the lowest participant's CF, which is rejected here
because `\xff`-prefixed user keys are legal in Redis.

**Be honest about what it is for.** Under this protocol the record is **not** the atomicity
mechanism — the batch is. The record buys: (i) forensics after an incident (which transactions
committed last, with participant sets); (ii) a replication/backup anchor keyed by the same `txid`
the stream carries; (iii) the migration path to fallback protocol B (§2.5) if the store is ever
split into genuinely independent logs; (iv) a debug-assertable invariant in the crash tests. It
costs one small `Put` in a batch that is being written anyway.

### 2.4 Recovery replay rules

1. **Partial batches**: nothing to do. RocksDB never applies a fraction of a `WriteBatch`, and
   `PointInTime` truncation drops a torn tail record whole. The existing watermark detector still
   reports the truncation (`frogdb_wal_recovery_dropped_records_total`, TR-PERSISTENCE-049 /
   FM-PERSISTENCE-034/035) — a dropped cross-shard batch is counted like any other dropped suffix.
2. **Commit records**: recovery scans the `txn_commit` CF, adds `cross_shard_commits_recovered` and
   `last_cross_shard_txid` to `RecoveryStats`, logs at info, then clears the CF. Cleared, not kept:
   it is evidence, not state.
3. **What recovery deliberately does not do**: verify that a recorded transaction's values are
   present. It cannot — a later write may legitimately have overwritten them, and carrying key
   hashes would make the record expensive. Any real check lives in the crash tests, which control
   the write set. Stating this explicitly is better than shipping a check that only looks like one.
4. **A commit record for a shard id outside the configured shard count** is a corruption /
   shard-count-change signal: refuse to start, matching FM-PERSISTENCE-030's shape (fail closed).
5. Retention: the CF is pruned by the same batch under a bounded ring (last K records, K a constant,
   not a duration — no clock), so it cannot grow without bound and a checkpoint that captures stale
   records is harmless.

### 2.5 Fallback protocol B — staging + commit record (documented, not built)

If the store is ever split so that shards own genuinely independent logs, the single-batch commit
stops being available. The replacement, written down now so the commit record's fields are already
right:

- Each participant appends its entries under a `TxnStage { txid, participants, entry_count }`
  marker into its own log and flushes to the configured durability level.
- The **lowest-numbered participant** is the designated **owner log** (deterministic, no clock, no
  election). After every participant reports staged, the owner appends `TxnCommit { txid }` and
  flushes. That append is the commit point.
- Recovery, per log: collect staged transactions; for each, consult the owner log for `TxnCommit`.
  Committed → re-apply the staged entries (they must be idempotent at the storage layer, which
  `Put`/`Delete`/`Merge`-with-recorded-result are, since we stage *effects*, not commands).
  Not committed → discard the staged entries.
- Owner log lost entirely → fail closed: refuse to start, since the commit decision is unknowable.

This is strictly more machinery for the same guarantee, which is why it is the fallback (§8 A2).

---

## 3. Design point 2 — dispatch shape

### 3.1 Admission

`TransactionTarget::resolve` stops being the sole verdict. The gate at `exec.rs:177-182` becomes a
three-way branch, still second in the gate order (above the rate limiter and the pause barrier):

| mode / flag | `Multi(shards)` |
|---|---|
| cluster mode | `-CROSSSLOT` (unchanged, §5.5) |
| standalone, `allow_cross_slot_standalone = false` | `-CROSSSLOT` (unchanged) |
| standalone, `allow_cross_slot_standalone = true` | cross-shard path |

Honoring the flag rather than accepting unconditionally makes FM-TXN-021 a **rewrite** ("the flag
now relaxes transactions too") rather than a retirement, keeps one knob for "this deployment
tolerates shard-spanning work", and leaves an operator escape hatch. Whether the flag's *default*
should flip to `true` for standalone-Redis parity is §7 O-1. Plumbing: `is_cluster` and the flag
must reach `frogdb-txn` — carried on `TxnSummary` or exposed as `TxnHost` accessors (F6).

`Multi` arises from two sources and both must work: keys folded by `TxnSlotAccumulator::fold_shard`,
and **live watches** folded at `TransactionState::take` (`state.rs:309-313`). A cross-shard *watch*
set alone promotes the target to `Multi` today and is refused; under this design it takes the same
path with an empty write slice on the watch-only shards.

### 3.2 Layering

`frogdb-txn` must not gain a dependency on `frogdb-vll` (ADR-0002; `exec.rs` has zero `vll`
occurrences today and TR-TXN-019's structural claim rests on that). The continuation acquisition
therefore lives behind a new `TxnHost` method:

```text
TxnHost::execute_cross_shard_transaction(
    shards: &[usize], commands, watches, asking, routing_fence
) -> CrossShardTransactionResult
```

implemented in `frogdb-server/crates/server/src/connection/transaction.rs`, mirroring
`execute_cross_shard_script` (`connection/scripting/eval.rs:121-193`) — same `VllCoordinator`, same
`ShardSenderSink`, same `MAX_WOUND_RETRIES` loop **reusing one `txid`** across retries.

### 3.3 The run, under the continuation lock

1. `acquire_continuation_and_run(txid, conn_id, sorted_shards, DEFAULT_LOCK_ACQUISITION_TIMEOUT, ..)`.
2. **Watch validation first, everywhere.** Each participant version-checks its own watched keys
   *inside* the lock, before any command runs. If any is dirty, no command runs on any shard and the
   reply is nil-`WatchAborted`. This is strictly stronger than today's off-target watch round trips
   (`exec.rs:335-342`, `partition_watches` at `:428`), which read watches outside any lock.
3. **Slice dispatch.** One message per participant carrying that shard's commands *with their
   original queue indices*, the watch slice, `conn_id`, `protocol_version`, `admission`,
   `routing_fence`, and the `CrossShardCommit` handle. Shape: a new `CoreMsg::ExecTransactionPart`
   alongside `CoreMsg::ExecTransaction`. Dispatch every message before awaiting any reply (the
   pattern `acquire_continuation` already uses at `coordinator.rs:612-614`).
4. **Per-shard execution** is `execution.rs:594-815` minus the fence/watch preamble (done in 2) and
   with step 6's commit replaced by the rendezvous join (§2.2). Run-all is preserved: a per-command
   error becomes that index's reply and the slice continues.
5. **Commit** happens once, in the rendezvous.
6. **Gather** by original queue index into one array. Deferred connection-level and server-wide
   commands keep running after the batch, outside atomicity, exactly as today (`exec.rs:298-313`,
   `:378-403`).

### 3.4 The hold cap, and why the batch is not revocable mid-flight

`CONTINUATION_MAX_HOLD` (5s) revoking a *transaction* mid-batch would leave shard A applied in
memory and shard B never run — an in-memory partial visible the moment the lock releases. That is
the one place this design could break atomicity, so:

- The hold cap is enforced as an **admission bound** — checked before the first command runs — and
  never mid-batch. Once dispatch begins, the batch runs to completion.
- The batch is bounded **structurally, not temporally**: a cap on queued command count and total
  argument bytes for a cross-shard `EXEC` (constants, no clock), so "runs to completion" is bounded
  work. Unlike Lua, a `MULTI` queue cannot loop.
- `SCRIPT KILL` (`shard/scripting.rs:324`, `revoke_held_continuation`) targets scripts and must not
  reach a transaction continuation. `FM-VLL-007`'s not-observable list gains that clause.

Consequence: the AMBIGUOUS window shrinks to genuinely structural failures (§4).

---

## 4. Design point 3 — failure and timeout observability

### 4.1 Client-visible outcomes

| outcome | wire | applied? | decided where |
|---|---|---|---|
| `Committed` | reply array, queue order | yes, all shards | rendezvous returned `Ok` |
| `WatchAborted` | nil | **no**, definitely | before any command ran (§3.3 step 2) |
| `ExecAbort` | `-EXECABORT` | no | queue-time errors, unchanged |
| `RateLimited` | `-ERR` | no | unchanged, gate order after admission |
| `CrossSlot` | `-CROSSSLOT` | no | cluster mode, or flag off |
| `Error` (lock lost) | `-ERR lock acquisition failed` / `-BUSY` | **no**, definitely | continuation acquire failed or wound past `MAX_WOUND_RETRIES`; no slice was dispatched |
| `Error` (topology) | `-TRYAGAIN` / redirect | no | routing fence, cluster only |
| **`Ambiguous` (new)** | `-ERR cross-shard EXEC outcome unknown` | **unknown** | coordinator lost the rendezvous outcome after dispatch |
| node-fatal | no reply, process aborts | unknown | participant shard worker gone (existing `AbortFailStop`) |

Everything above the `Ambiguous` row is a **definitely-not-applied** answer, which is the point of
doing acquisition and watch validation before dispatch: the large majority of failures land in the
clean half of the FM-TXN-032 split.

### 4.2 The AMBIGUOUS family

Modelled directly on `ScatterError::ResultAmbiguous { shard_id, applied, unknown }`
(`vll/src/coordinator.rs:115`, TR-VLL-019). A new `TransactionOutcome::Ambiguous` with metric label
`"ambiguous"` (the label set is pinned by `outcome_metric_labels_are_stable`, `exec.rs:558`, so this
is a deliberate, tested addition). It is reachable only when:

- the coordinator's task is cancelled after dispatch but before the rendezvous outcome arrives, or
- a participant's reply channel drops after the commit point.

It is **not** reachable from a lock failure, a watch abort, or a per-command error. The structured
log carries `txid`, `participants`, `applied`, `unknown`; the wire reply does not (it would leak
internal shard ids). This requires amending FM-TXN-032, which today forbids any ambiguous outcome
(F5, §5.1).

### 4.3 Metrics

- `frogdb_transactions_total{outcome="ambiguous"}` — new label value on the existing counter.
- `frogdb_transactions_cross_shard_total{outcome}` — cross-shard batches by outcome.
- `frogdb_txn_cross_shard_participants` — histogram of participant counts.
- `frogdb_txn_cross_shard_commit_wait_seconds` — rendezvous wait, observability only; it must not
  feed any decision.
- `frogdb_txn_cross_shard_rendezvous_open` — gauge, so a stuck rendezvous is visible even though
  nothing times it out.
- `frogdb_wal_recovery_cross_shard_commits_total` — from §2.4 rule 2.

### 4.4 WAL-failure policy — the one genuinely unresolved interaction

FrogDB applies in memory and then persists. Per shard, `wal-failure-policy` reconciles the two:
`continue` acks anyway, `rollback` restores the key snapshots in reverse and replaces every reply
with `-EXECABORT transaction aborted due to WAL failure` (`execution.rs:778`), `readonly`
fail-stops. Across shards, `rollback` cannot be honored atomically — one participant would restore
and another would not, which is precisely the partial state the design exists to prevent.

Recommendation: under `rollback`, a cross-shard commit failure **escalates to fail-stop** (poison
every participant and abort the node), because a fail-stop is recoverable from durable state while a
half-rolled-back transaction is not. The cheaper alternative — refuse cross-shard `EXEC` while
`wal-failure-policy = rollback` — is a config-gated re-introduction of the deviation. See §7 O-2.

---

## 5. Design point 4 — replication

### 5.1 The batch must reach the replica as one unit

Today's framing (F4) tags a whole group with one origin shard, and the replica's `apply_group`
contract is single-shard. Proposal:

- New broadcaster method `broadcast_transaction_across_shards(txid, parts: &[(u16, &[(&str, &[Bytes])])])`
  emitting `MULTI` (tagged `CONTROL_SHARD`), then each command **tagged with its own origin shard**,
  then `EXEC`. Called once by the coordinator **after** the commit point, not by each shard worker
  — a new `EffectScope::CrossShardTransactionPart` suppresses the per-shard broadcast arm at
  `post_execution.rs:413-503` so a participant does not emit its slice independently.
- Because the frames are shard-tagged individually, the replica can route each command to its owning
  shard while still treating the group as one commit unit.

### 5.2 Group contiguity — a present-tense hazard this design must fix

`broadcast_transaction_on_shard` issues N+2 unlocked `OffsetCoordinator::advance` calls
(`offset_coordinator.rs:88-121`). Concurrent shard workers can therefore interleave frames, and the
replica's single node-wide `pending` group swallows any frame that arrives mid-group **regardless of
its shard tag** (`apply.rs:688-735`). Today this is masked because a group only ever comes from one
single-threaded shard worker — but nothing at the broadcaster enforces it, and two shards emitting
groups concurrently would already mis-assemble.

Stage 2 must make group emission atomic against the offset coordinator: a broadcaster-level mutex
held across the whole `MULTI…EXEC` emission (or an `advance_group` that reserves the byte range in
one `fetch_add` and appends the frames under that reservation). This benefits the existing
single-shard path too, and deserves its own FM row (§6).

### 5.3 Replica-side apply

- `PendingTxn` gains a per-command shard tag (or the group carries a participant set) and the
  applier gains `apply_group_multi(parts)`, which drives the **same** cross-shard commit path
  (§2.2) on the replica. Precedent: Dragonfly's txid-keyed `MultiShardExecution` /
  `TxExecutionSync` barrier (§2.1).
- `ReplicaTxnBound` (`DEFAULT_REPLICA_TXN_MAX_COMMANDS = 1_000_000`,
  `DEFAULT_REPLICA_TXN_MAX_BYTES = 1 GiB`) already bounds an unterminated group and forces
  `+FULLRESYNC` on breach; its accounting must be re-checked to sum across participants rather than
  per shard (FM-REPLICATION-045).
- Bytes stay claimed at `EXEC` (`apply.rs:408`), so the applied offset advances exactly at the
  commit unit.

### 5.4 Replica crash mid-apply

Because the replica applies the group through the same single-batch commit, a crash leaves **the
whole group or none of it** in RocksDB — a partial group is not representable. What a crash can
still do is lose the *offset* advance while keeping the data (or the reverse), so the group may be
re-delivered and re-applied after resume. That is the pre-existing, already-ruled-but-unbuilt
pairing of the persisted offset with the write it names (TR-REPLICATION-024, FM-PERSISTENCE-019);
cross-shard `EXEC` neither improves nor worsens it, and this design must not claim otherwise. A
failed apply still latches divergence and forces `+FULLRESYNC`.

---

## 6. Design point 5 — enumerated spec deltas (not applied in stage 1)

Next free ids at time of writing: `TR-TXN-029`, `FM-TXN-054`, `TR-VLL-023`, `FM-VLL-012`,
`TR-PERSISTENCE-053`, `FM-PERSISTENCE-060`, `TR-REPLICATION-035`, `FM-REPLICATION-067`. Every new FM
row needs its forcing test named, or `just lint-spec` fails.

### 6.1 `specs/txn.md`

| row | change |
|---|---|
| TR-TXN-018 | amend: `Multi(_)` resolves to `-CROSSSLOT` **in cluster mode or with the flag off**; otherwise to the cross-shard path |
| TR-TXN-019 | narrow to cluster mode; keep the "`frogdb-txn` does not depend on `frogdb-vll`" structural claim, reworded to name the `TxnHost` seam |
| FM-TXN-019 | narrow to cluster mode; delete the "per-shard undo is the missing piece" sentence |
| FM-TXN-020 | narrow: the live-watch fold refuses in cluster mode / flag off only |
| FM-TXN-021 | **rewrite**: the flag now relaxes transactions; refusal with the flag off is the observable |
| FM-TXN-032 | **amend**: add the `Ambiguous` outcome and state precisely when it is reachable; keep "no path invents a `Committed`" |
| TR-TXN-029 (new) | a cross-shard `EXEC` takes one continuation lock over the sorted participant set under one `txid` |
| TR-TXN-030 (new) | watch validation for every participant happens under the lock, before any command runs |
| TR-TXN-031 (new) | per-shard slice dispatch; replies merged by original queue index |
| TR-TXN-032 (new) | the cross-shard commit rendezvous is the single commit point |
| FM-TXN-054 (new) | a cross-shard `EXEC` is all-or-nothing across shards across a crash |
| FM-TXN-055 (new) | run-all holds across shards: a per-command error does not stop siblings on any shard |
| FM-TXN-056 (new) | a lost continuation lock is definitely-not-applied, never ambiguous |
| FM-TXN-057 (new) | the batch is not revocable mid-flight; the hold cap is an admission bound |
| deviations table (`:980`) | rewrite the FM-TXN-019/021 row: refusal is cluster-mode-only (plus flag-off standalone), rationale = Redis-cluster parity + migration safety |

### 6.2 `specs/vll.md`

| row | change |
|---|---|
| scope paragraph | delete "a cross-shard `MULTI` never reaches it" |
| FM-VLL-002 | fix the now-stale trigger for the same reason |
| FM-VLL-007 | add to not-observable: a transaction batch cut mid-flight by the hold cap |
| TR-VLL-023 (new) | a continuation grant taken for a transaction is non-revocable; the cap is checked at admission and the batch is bounded by count/bytes |

### 6.3 `specs/persistence.md`

| row | change |
|---|---|
| FM-PERSISTENCE-001 | narrow the non-guarantee at `:610`: cross-shard atomicity **is** provided for `EXEC` via the single-batch commit; still not provided for scatter/`MSET` |
| TR-PERSISTENCE-053 (new) | the cross-shard rendezvous: participants stage into one batch, the last arriver commits once |
| TR-PERSISTENCE-054 (new) | the commit-record layout and its bounded retention |
| FM-PERSISTENCE-060 (new) | a cross-shard write batch is never torn across shards on crash |
| FM-PERSISTENCE-061 (new) | a participant that cannot stage fails the whole commit closed (and the `rollback`-policy escalation of §4.4) |
| FM-PERSISTENCE-062 (new) | recovery reports and clears commit records; an out-of-range participant refuses startup |
| note | `commit_raw_batch` / `RawBatchOp` graduate out of `#[cfg(test)]` |

### 6.4 `specs/replication.md`

| row | change |
|---|---|
| FM-REPLICATION-034 | **rewrite**: drop the "runs entirely on one shard" premise; a group is one commit unit whose frames may carry different shard tags |
| FM-REPLICATION-045 | `ReplicaTxnBound` accounting sums across participants |
| TR-REPLICATION-035 (new) | cross-shard framing: `MULTI` on `CONTROL_SHARD`, per-command shard tags, `EXEC` closes |
| FM-REPLICATION-067 (new) | a `MULTI…EXEC` group is emitted contiguously in the stream; no other frame interleaves (fixes §5.2, applies to the single-shard path too) |
| FM-REPLICATION-068 (new) | a replicated cross-shard group applies atomically on the replica or not at all |

### 6.5 Why cluster mode keeps `-CROSSSLOT`

Not a limitation we are deferring — a decision:

1. **Redis-cluster parity.** Real Redis Cluster answers `-CROSSSLOT`. Clients, libraries, and test
   suites depend on it, and hash tags are the documented way to opt in. Accepting a cross-slot batch
   where Redis refuses is a compatibility trap, not an improvement.
2. **Migration safety.** A slot can be `MIGRATING`/`IMPORTING`, or owned by a different **node**. A
   cross-slot batch would need distributed 2PC across nodes — a different problem from the
   single-process, single-RocksDB, single-WAL case this design solves. Nothing here generalizes.
3. **No single fence.** The routing-generation / `SlotFence` machinery (TR-TXN-020) is per-slot and
   per-owner; a multi-slot batch spanning owners has no coherent fence to stamp.
4. **Issue 31's migration redesign** (source-authoritative-until-commit) assumes single-slot commit
   units; widening the commit unit in cluster mode would reopen it.

---

## 7. Open questions for the design owner

- **O-1 — flag or unconditional, and what default?** Recommendation: gate on
  `allow_cross_slot_standalone`, leave the default `false`. Standalone Redis has no slots at all, so
  strict parity argues for unconditional acceptance; against it, the flag is the existing "this
  deployment tolerates shard-spanning work" switch and `num_shards` defaults to 1 anyway, so the
  flag costs nothing in practice. Flipping the default is a separate, later call.
- **O-2 — `wal-failure-policy = rollback` (§4.4).** Recommendation: fail-stop the node. Alternative:
  refuse cross-shard `EXEC` while that policy is set. This is the one place the design cannot pick
  the answer on its own, because both options are defensible readings of fail-closed.
- **O-3 — commit-record placement.** Recommendation: a dedicated `txn_commit` CF. It is a data-dir
  layout change that interacts with CF enumeration and the shard-count-change refusal
  (FM-PERSISTENCE-030/032). Alternative: a reserved `\xff`-prefixed key in the lowest participant's
  CF — cheaper, but `\xff` keys are legal Redis user keys.
- **O-4 — write the commit record at all?** Under the single-batch protocol it is not load-bearing
  (§2.3). Recommendation: write it, for forensics and for the protocol-B migration path. A reviewer
  who wants the minimum viable change can drop it and lose only observability.
- **O-5 — should cross-shard `MSET`/scatter adopt the same commit unit?** Recommendation: not in
  stage 2. Scatter has no all-or-nothing contract to the client today, the win is smaller, and a
  rendezvous barrier sits in the hottest cross-shard path. It would, however, let
  FM-PERSISTENCE-001's non-guarantee be deleted outright rather than narrowed.
- **O-6 — should cross-shard `EVAL` adopt it (F3/H4)?** Recommendation: yes, as a follow-up issue,
  not in stage 2. Leaving two continuation-lock paths with different crash semantics is a trap; but
  Lua's sub-commands arrive one at a time and synchronously, so batching them into one commit unit
  is a larger change than `EXEC`'s.
- **O-7 — batch bounds (§3.4).** What are the count and byte caps for a cross-shard `EXEC`, and is a
  breach `-EXECABORT` at queue time or a refusal at `EXEC`? Queue time is friendlier and matches
  `validate_queued_batch`.
- **O-8 — is the group-contiguity fix (§5.2) in scope for stage 2, or split into its own issue?** It
  is a pre-existing latent defect that this work makes reachable; it may deserve a separate issue
  with its own forcing test so the cross-shard work is not gated on it.
- **O-9 — Quint model?** A small `txn_cross_shard.qnt` covering "no participant is applied unless all
  are" and the rendezvous state space would fit the existing model battery. Not required; cheap
  insurance if the reviewer wants it.

---

## 8. Alternatives considered, and why they were rejected

- **A1 — per-shard undo log (the issue's original framing).** Rejected by ruling R8 and on merit:
  run-all means there is no runtime rollback contract to honor, so the log would exist purely for
  crash recovery — which the single-batch commit provides without before-images, without the memory
  cost, and without the write amplification.
- **A2 — 2PC with per-shard prepare records and a designated owner log.** Rejected as the primary
  design: more moving parts, a recovery-time scan, and an idempotent re-apply requirement, all to
  buy a guarantee the shared RocksDB WAL already gives. Kept as documented fallback §2.5 for the day
  the store is split, which is why the commit record's fields already match it.
- **A3 — coordinator assembles the batch and writes it directly, bypassing the shard flush
  threads.** Rejected: it reorders the transaction ahead of entries already queued on participant
  FIFOs (a crash could then show the transaction's value while losing an earlier write to the same
  key), and avoiding that needs a drain — i.e. a barrier — anyway. It would also duplicate the
  poison-latch, sequence, and lag accounting that lives in the flush threads.
- **A4 — route the whole batch to one shard for the duration.** Rejected: unbounded data movement
  and it violates the sharding model; nothing else in the system relocates keys for a request.
- **A5 — tell operators to run `num_shards = 1` in standalone.** Rejected: that is the deviation
  written as a workaround, and it discards the reason standalone is sharded.
- **A6 — bound the rendezvous with a wall-clock timeout.** Rejected by ruling: no wall clock in
  correctness decisions. The rendezvous resolves on structural events only; a missing participant is
  already a node-fatal condition, so there is nothing for a timeout to rescue.
- **A7 — allow a partial commit and report which shards applied (the scatter path's answer).**
  Rejected: a transaction's whole contract is all-or-nothing. Reporting partial application is the
  right answer for `MSET`, which never promised otherwise, and the wrong answer for `EXEC`.
- **A8 — keep the refusal and document it harder.** Rejected by ruling R8: the refusal is a real
  Redis-compat gap in standalone, and the substrate to close it already exists (F1).

---

## 9. Stage-2 implementation sketch

### 9.1 Crates and seams

| crate | work |
|---|---|
| `frogdb-persistence` | `CrossShardCommit` rendezvous; `WalCommand::CrossShardCommit`; committer path in `flush.rs`; commit-record encode/decode; graduate `commit_raw_batch`/`RawBatchOp` out of `#[cfg(test)]` |
| `frogdb-recovery` | read/report/clear the `txn_commit` CF; `RecoveryStats` fields; out-of-range participant refusal |
| `frogdb-core` | `WalTarget::join_cross_shard_commit`; `CoreMsg::ExecTransactionPart` + shard handler; `EffectScope::CrossShardTransactionPart` suppressing the per-shard broadcast |
| `frogdb-vll` | non-revocable continuation grant for transactions (admission-time hold check) |
| `frogdb-txn` | three-way gate at `exec.rs:177-182`; `is_cluster` + flag on `TxnSummary`; `TxnHost::execute_cross_shard_transaction`; `TransactionOutcome::Ambiguous` + metric label |
| `frogdb-server` | host impl mirroring `execute_cross_shard_script`; wound-retry loop; reply merge by queue index |
| `frogdb-replication` | `broadcast_transaction_across_shards`; group-contiguity lock; per-command shard tags in `PendingTxn`; `apply_group_multi`; `ReplicaTxnBound` accounting |
| `frogdb-replication-runtime` | executor path for a multi-shard group |
| docs / website | transaction docs, the deviations table, config reference for the flag's new reach |

Order: persistence rendezvous (with its own unit + crash tests, green in isolation) → core wiring →
txn gate → replication → docs. The persistence layer is independently testable, so it lands first.

### 9.2 Tests

- **`frogdb-persistence` unit** — the rendezvous commits one batch across CFs; a participant
  reporting failure aborts the commit for everyone; a disconnected participant resolves the
  rendezvous structurally (no hang, no timer).
- **Crash forcing test (`frogdb-core`, `CrashTestHarness`)** — the acceptance criterion.
  `cross_shard_exec_is_all_or_nothing_across_a_crash`: two shards, four keys; crash *before* the
  commit → zero keys present; crash *after* the sync'd commit → four present; never one to three.
  Mirrors `test_cross_shard_batch`'s `found_count == 0 || found_count == 4` assertion and uses
  `crash()`'s drop-the-unsynced-write-set model.
- **Kill-mid-`EXEC`/recover integration test** — SIGKILL a live server mid-`EXEC` in `Sync` mode,
  reopen, assert all-or-none across two shards' keys, and assert the recovery stats counted the
  transaction (or did not) consistently with the observed data.
- **`frogdb-server` integration** (`integration_transactions.rs`) — commits in standalone with the
  flag on; run-all error semantics across shards; cross-shard watch abort runs nothing anywhere;
  `-BUSY`/lock-failure is definitely-not-applied; cluster mode still `-CROSSSLOT`; flag off still
  `-CROSSSLOT` (the three existing tests at `:1428-1600` get rewritten, not deleted).
- **Replication** — the primary emits one contiguous group with per-frame shard tags; an unrelated
  concurrent write does not interleave into the group; the replica applies atomically; a replica
  crash mid-group leaves none of it.
- **Ambiguity** — a cancelled coordinator after dispatch yields `Ambiguous`, and no lock failure or
  watch abort ever does.
- **Gates** — `just lint-spec` after the rows land (every new FM row names its forcing test);
  `just mutants-diff frogdb-txn frogdb-vll frogdb-persistence frogdb-replication` before pushing;
  a seam-lint check if the new commit path bypasses the durable-ack `flush_through` chokepoint.

### 9.3 Rollout

Behind the existing flag, which defaults off, in a mode (`num_shards > 1` standalone) that is
already non-default. No migration: the `txn_commit` CF is created on open like the other tiers, and
an old data directory simply has none.
