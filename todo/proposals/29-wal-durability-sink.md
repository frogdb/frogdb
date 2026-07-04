# Proposal: WAL Durability Sink

Status: implemented
Date: 2026-07-04

## Problem

FrogDB's per-shard "WAL" is a redo-of-current-state buffer: the shard serializes a key's current
state, a dedicated flush thread accumulates entries into a RocksDB `WriteBatch` against the main
data CF, and recovery iterates that CF. RocksDB's own WAL is the actual crash mechanism. The flush
thread commits on three triggers — explicit `Flush` command, size threshold, and batch timeout —
plus a drain on shutdown.

Only the explicit `Flush` trigger had a result channel. **Every other flush discarded its outcome**
(`wal/flush.rs`, pre-change): the size-threshold and timeout paths were literally
`let _ = do_flush(...)`, and the shutdown drain the same. `write_set`/`write_delete` return
`Ok(seq)` the moment the entry lands on the channel — before any flush. The consequences, by
failure policy:

1. **`WalFailurePolicy::Continue` (default): acked write, failed persist, total silence.** A
   `write_batch_opt` failure (disk full, I/O error) dropped the batch, logged once per failure,
   and told no one: no counter, no INFO field, no status-JSON field, nothing a health check could
   see. The client's write was acked; its durability silently evaporated.
2. **`WalFailurePolicy::Rollback`: the confirmation had a hole.** `persist_and_confirm` flushed
   and returned the result of *that* flush. If a command's entries had already been swept out by a
   failed size-threshold flush (a large value crossing `batch_size_threshold` mid-accumulation),
   the explicit flush found an empty buffer, returned `Ok`, and the command was acked — the exact
   guarantee rollback mode exists to provide, broken by the batching it sits on top of.

Two adjacent structural problems compounded this:

3. **`sync_lag_ms` was dead-by-construction and lied.** `record_sync()` had zero callers in the
   entire tree (grep: only its definition). `last_sync_timestamp_ms` was seeded once at
   construction and never updated, so the derived `sync_lag_ms` climbed monotonically forever. It
   was nevertheless surfaced as truth in INFO persistence (`wal_sync_lag_ms`,
   `wal_last_sync_time`), status JSON (`max_sync_lag_ms`, per-shard `sync_lag_ms`), and a
   Prometheus gauge (`frogdb_wal_sync_lag_ms`). A monitoring rule on it would fire always or
   never.
4. **The flush state machine was untestable.** `flush_thread_loop` was reachable only through
   `RocksWalWriter::new`, which spawns a real OS thread and requires an on-disk RocksDB — so none
   of the swallowed-error paths had (or could have) unit tests, which is how flags 1 and 2
   survived.
5. **A dead trait impl duplicated the serialization path.** `impl WalWriter for RocksWalWriter`
   re-implemented entry construction already owned by `write_set`/`write_delete` — and no code
   called `RocksWalWriter` through the trait at all (the only `append` caller in the tree is
   `NoopWalWriter`'s own unit test). Its `WalOperation::Expire` arm incremented the sequence and
   wrote nothing. A second, drift-prone serialization site guarding zero behaviour.

## Prior art: how Redis handles AOF write errors

Redis (`aof.c`, `flushAppendOnlyFile`) treats an AOF write error as first-class state:

- With `appendfsync always`, a failed/short write is unrecoverable by policy and Redis **exits**.
- Otherwise it sets `server.aof_last_write_status = err`, **keeps the unwritten buffer** and
  retries on the next cron; while the status is `err`, write commands are refused with
  `-MISCONF Errors writing to the AOF file`. On a successful retry the status returns to `ok`.
- The error log is rate-limited (once per 30s), and INFO exposes `aof_last_write_status`.

The shape to copy is *status as state + refusal where durability is promised + visibility
everywhere*. The mechanism does **not** transplant directly, for two reasons rooted in what
FrogDB's WAL is:

- Redis's AOF is an append-only command log — completeness and order are the whole story, so the
  failed buffer must be retried. FrogDB's WAL entries are full current-state images going into the
  data CF; a later write of the same key self-heals it, and RocksDB's own WAL carries crash
  consistency for committed batches.
- Retrying a dropped batch is actively wrong in rollback mode: the failed batch may contain the
  pre-rollback state of a command that was subsequently rolled back in memory (persist runs before
  rollback). A retry that later succeeds would resurrect the rolled-back value on disk.

## Implemented design

One principle: **every flush outcome is recorded; nothing is confirmable except through the
record.** Three pieces, all in `persistence/src/wal/flush.rs`:

### `WriteSink` — the storage seam

```rust
pub(super) trait WriteSink: Send {
    fn stage_put(&mut self, key: &[u8], value: &[u8]) -> std::io::Result<()>;
    fn stage_delete(&mut self, key: &[u8]) -> std::io::Result<()>;
    fn commit(&mut self, sync: bool) -> std::io::Result<()>; // consumes staged set either way
    fn staged_len(&self) -> usize;
}
```

`RocksSink` (a `WriteBatch` + `Arc<RocksStore>` + shard id) is the production impl. The seam exists
for testability-depth: the flush thread loop is now generic over `S: WriteSink`, so the entire
batching/threshold/timeout/drain/error machinery runs in unit tests against an in-memory sink with
commit- and stage-failure injection — no RocksDB, no tempdir, milliseconds per test.

### `FlushOutcomes` — the shared durability record

WAL entries now carry their assigned sequence (`WalEntry::{Put,Delete}` gained `seq`). The engine
tracks the max sequence per batch, and after **every** flush attempt writes into a shared
`FlushOutcomes` (one `Arc` held by the flush thread, the writer handle, and thus `lag_stats()`):

- `durable_seq` — highest sequence whose batch committed (the durable high-water anchor; this is
  what earns `sequence` its keep).
- `highest_failed_seq` — highest sequence contained in any failed batch (`fetch_max`).
- `flush_failures`, `lost_ops`, `lost_bytes` — permanent counters; a later success never
  un-counts a loss.
- `last_flush_ok` + `last_error` — status of the most recent attempt and its message.

Failure logging is rate-limited (full `error!` at most once per 10s, `debug!` otherwise) — with a
10ms batch timeout, a dying disk would otherwise emit ~100 error lines/second.

### Durable-through confirmation

Sequence numbers make confirmation precise. Batches are committed in sequence order, so *a failed
batch whose max sequence exceeds `S` must have contained an entry assigned after `S`*. Hence:

```rust
// RocksWalWriter
pub async fn flush_through(&self, after_seq: u64) -> std::io::Result<()>
```

flushes, then fails iff that flush failed **or** `highest_failed_seq > after_seq` (with a defensive
`durable_seq >= target` check). Rollback-mode callers (`persist_and_confirm`,
`persist_transaction_to_wal` in `core/src/shard/persistence.rs`) capture `after_seq =
wal.sequence()` *before* writing their entries and confirm through it. Two properties fall out:

- **An acked write cannot outrun a swallowed failure** (flag 2 closed): the failed size-threshold
  flush that carried the command's entries has `highest_failed_seq > after_seq`, so the
  confirmation fails and the command rolls back with `IOERR`, even though the final explicit flush
  of the emptied buffer succeeded.
- **Attribution is exact**: a background failure that covered only *earlier* sequences (another
  command's already-reported loss) does not fail an innocent later command — which matters because
  failing it would roll back a write whose entries *did* commit, creating a memory/disk divergence
  in the other direction.

`flush_async()` keeps its old meaning — "drain the buffer now, report this attempt" — for the
shutdown paths that want exactly that.

### Decision: failed batches are dropped, visibly — not retried, not refused

- **Rollback mode** already has the right refusal semantics: the client gets an error and memory
  is rolled back, per-command. That is FrogDB's analog of Redis's MISCONF refusal, with finer
  granularity. Dropping the failed batch is *correct* here — retrying could persist rolled-back
  state (see prior art section).
- **Continue mode** is an explicit availability-over-durability opt-in (like running Redis with
  AOF errors tolerated). Refusing writes would contradict the policy's contract; silently losing
  them contradicts honesty. The implemented semantics: the loss is permanent and permanently
  *visible* — `wal_last_flush_status:err` + `wal_flush_failures` + `wal_lost_ops` in INFO,
  `flush_failures_total`/`lost_ops_total`/`last_flush_ok` in status JSON,
  `frogdb_wal_flush_failures_total`/`frogdb_wal_lost_ops_total`/`frogdb_wal_lost_bytes_total`
  counters and a `frogdb_wal_last_flush_ok` gauge for alerting, plus the rate-limited log.
  `last_flush_ok` returning to `true` after a subsequent successful flush is honest *as labeled*
  (it describes the last attempt); `lost_ops > 0` is the permanent evidence that data was dropped.
  A server-wide MISCONF-style write refusal for Continue mode remains possible on top of
  `FlushOutcomes` if ever wanted — the state now exists to implement it in one place.

### Decision: `sync_lag_ms` deleted, not wired

Per the project's observability rule (misleading data is not ok; honest absence beats a lying
value), and because wiring it truthfully is structurally awkward: the actual fsync happens either
per-flush (`Sync` mode sets `WriteOptions::set_sync(true)`, so sync lag ≡ `durability_lag_ms`
there) or in `spawn_periodic_sync`, a **global** task holding only the shared `RocksStore` with no
handle to any per-shard writer — per-shard "sync lag" is the wrong shape for a global sync.
Deleted: `record_sync`, `last_sync_timestamp_ms`, `sync_lag_ms`/`last_sync_timestamp_ms` fields in
`WalLagStats`, the `Option` plumbing in `WalAggregate`/INFO (`wal_sync_lag_ms`,
`wal_last_sync_time`), status JSON (`max_sync_lag_ms`, per-shard `sync_lag_ms`), the
`frogdb_wal_sync_lag_ms`/`frogdb_wal_last_sync_timestamp` gauges, and their telemetry
definitions/name constants. If periodic-sync observability is wanted later, the truthful home is a
single global gauge emitted by `spawn_periodic_sync` itself after each successful `rocks.flush()`.

### Decision: dead `WalWriter` impl deleted

`impl WalWriter for RocksWalWriter` (and with it the duplicate serialization path and the
write-nothing `WalOperation::Expire` arm) is deleted from `wal/writer.rs`. Nothing in the tree
called it: no `dyn WalWriter` holds a `RocksWalWriter`, no generic is bounded by the trait. Entry
construction now has exactly one site (`write_set`/`write_delete`). The trait itself, its
`WalOperation` enum, and `NoopWalWriter` live in `frogdb-types` and are now implementor-dead
except for the noop — deleting them is a follow-up (below) since `frogdb-types` is outside this
change's scope.

### Why this is the right depth

- **Locality.** "What happened to my flush?" has one answer surface (`FlushOutcomes`) written by
  one component (`FlushEngine::flush`) that every trigger path must go through — there is no
  longer a `do_flush` return value a call site can forget to check; the recording is inside the
  seam, not at the seams' callers.
- **Leverage.** The seam deletes the whole class of "which flush trigger forgot to propagate its
  error?" bugs (three live instances) and makes the previously-untestable state machine the most
  tested code in the module. The durable-sequence anchor also gives future work (WAIT-style
  durability barriers, a MISCONF-style gate) a primitive instead of a redesign.
- **Not a new layer.** `FlushEngine` is the old `do_flush`/`apply_entry` free functions given a
  name and their shared mutable state made explicit; `RocksSink` is the old inline
  `WriteBatch`/`write_batch_opt` calls. The thread loop's trigger logic is unchanged in shape —
  only the outcome handling moved behind the seam.
- **Deletion test.** Removing `FlushOutcomes` scatters outcome tracking back across four trigger
  sites and reopens flags 1–2; removing `WriteSink` re-welds the state machine to RocksDB and
  deletes the test suite's foundation. Neither can be removed without regressing the structure.

## Phases / commits

1. **Durability sink + confirmation** — `WriteSink`/`RocksSink`, `FlushOutcomes`, `FlushEngine`,
   generic `flush_thread_loop`, sequence-stamped entries, `flush_through`, rollback-path
   conversion, 7 sink-level unit tests. Landed in `17cb35fc` (folded into a concurrent commit by
   an unrelated in-flight change; the WAL work is the
   `wal/flush.rs`/`wal/writer.rs`/`wal/tests.rs`/`shard/persistence.rs` hunks).
2. **Truthful surfacing + sync-lag deletion** — `WalLagStats` reshaped (adds `durable_sequence`,
   `flush_failures`, `lost_ops`, `lost_bytes`, `last_flush_ok`; drops the two dead sync fields),
   `WalAggregate`/INFO persistence section (`wal_last_flush_status`, `wal_flush_failures`,
   `wal_lost_ops`), status JSON, `frogdb_wal_last_flush_ok` gauge, telemetry definitions, and
   integration-test updates. Commit `4a555927`.
3. **Dead code deletion** — `impl WalWriter for RocksWalWriter` removed. Commit `fc715ad8`.

## Testing impact

The flush state machine went from zero testable paths to a loop-level suite driving the real
`flush_thread_loop` + `FlushEngine` against an injectable in-memory sink
(`persistence/src/wal/tests.rs`):

- `test_sink_happy_path_batches_and_advances_durable_sequence` — ordered batching, durable
  high-water, clean gauges.
- `test_sink_size_threshold_triggers_commit_without_explicit_flush` — threshold trigger.
- `test_sink_size_threshold_flush_error_surfaces_on_confirm` — **the regression pin**: background
  size flush fails, explicit flush of the emptied buffer returns `Ok`, durable-through
  confirmation must fail. This is the exact shape that used to ack lost writes in rollback mode.
- `test_sink_timeout_flush_error_surfaces_on_confirm` — timeout-trigger failure surfaced.
- `test_sink_disconnect_drain_error_recorded` — shutdown-drain failure recorded.
- `test_sink_failure_attribution_and_recovery` — a failure covering only earlier sequences does
  not fail a later command's confirmation; `last_flush_ok` recovers truthfully while `lost_ops`
  never un-counts.
- `test_sink_stage_failure_recorded_and_surfaced` — staging errors count as losses and fail
  confirmation instead of being log-and-skipped.

Existing RocksDB-backed WAL tests, core shard tests (634), telemetry tests (167), and the INFO
integration tests were kept green; INFO tests now pin the new `wal_last_flush_status` /
`wal_flush_failures` / `wal_lost_ops` fields and the honest absence of all WAL fields when
persistence is off.

## Follow-ups (out of scope here)

- **Warm-tier writes bypass the durability mode.** `demote_key` persists via the rocks
  `put_warm` path (`rocks/columns.rs`) directly, not through the WAL writer — so warm-tier writes
  ignore `DurabilityMode` and the new outcome tracking entirely. Unifying tiered writes behind the
  same sink is deferred (the owning `store/hashmap.rs` is under concurrent rework).
- **Delete the `WalWriter` trait from `frogdb-types`.** After phase 3 the trait has no non-noop
  implementor and no production caller; `WalOperation` (including the never-implemented `Expire`)
  goes with it.
- **Optional MISCONF-style gate for Continue mode**, if operators want refusal rather than
  visible loss: one check of `FlushOutcomes::last_flush_ok`/`lost_ops` at write dispatch.
- **Periodic-sync observability**, if wanted: a global last-sync gauge emitted by
  `spawn_periodic_sync` itself — not per-shard writer state.

## Correctness flags (from the survey, all confirmed against code)

1. **Swallowed background flush errors — CONFIRMED, fixed.** `let _ = do_flush(...)` on the
   size-threshold, timeout, and disconnect-drain paths; only explicit `Flush` had a result
   channel. Fixed by recording every outcome in `FlushOutcomes` (phase 1).
2. **Rollback confirmation blind to prior batch failures — CONFIRMED, fixed.**
   `persist_and_confirm` returned only the final flush's result. Fixed by sequence-anchored
   `flush_through` (phase 1); pinned by the regression test.
3. **`sync_lag_ms` dead-by-construction — CONFIRMED, deleted.** `record_sync` had zero callers;
   the metric climbed forever and was surfaced in INFO, status, and Prometheus. Deleted end to
   end (phase 2).
4. **Dual serialization in dead trait impl — CONFIRMED (stronger than surveyed), deleted.** Not
   merely drift-prone duplication: the impl had no callers at all. Deleted (phase 3); trait
   removal from `frogdb-types` noted as follow-up.
