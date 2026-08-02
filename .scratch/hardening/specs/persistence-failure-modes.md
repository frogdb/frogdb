# Persistence — failure modes

Every way FrogDB's durable-write path can fail, refuse, or succeed, one table per mode. This is
the reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope: the shard-to-storage path — `frogdb-core/src/shard/persistence.rs` (the `WalTarget` seam
and `persist_records`), `frogdb-persistence/src/wal/` (the per-shard flush engine, its batching
triggers and durability confirmation), and the snapshot/checkpoint machinery in
`frogdb-persistence/src/snapshot/` plus the pre-snapshot quiesce in
`frogdb-server/src/server/checkpoint_quiesce.rs`. Rows stop at what a *reader of storage* can
observe — a restored checkpoint or a crash-recovery replay. Client-visible transaction semantics
live in [Transactions — failure modes](txn-failure-modes.md).

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the shard in this mode. |
| Observable | What a reader of storage sees: the restored checkpoint's contents, the recovered keyspace. |
| NOT observable | What must never appear in storage in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Outcome variant | The enum variant / metric label the mode reports through, or `n/a` for storage-layer invariants with no client-visible outcome. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-PERSISTENCE-NNN` tag at its definition site; `just lint-failure-modes` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in
`scripts/failure-modes.py` (`NEXTEST_CRATES`).

---

## FM-PERSISTENCE-001 — a shard's write batch is never torn across storage batches

| Field | Value |
|---|---|
| Trigger | One shard-side `persist` call carrying more than one WAL entry — a single-shard `MULTI`/`EXEC`, a multi-key `MSET`, a `RENAME` (write + delete) — while the WAL flush engine's own batch triggers (size threshold, batch timeout) are live, and a `BGSAVE` checkpoint or a crash lands in the middle of the sequence. Load makes it likely: a descheduled shard task leaves the flush thread with an empty channel and an expired timeout. |
| Observable | A restored checkpoint, or a crash-recovered keyspace, shows **all** of the batch's writes or **none** of them. For the transaction test: every hash-tag group is all-absent or all-present at a single generation. |
| NOT observable | A committed *prefix* — some of a transaction's keys at the new generation and the rest at the old one (the shape issue 65 caught: `[Some("470"), Some("458"), Some("458"), Some("458"), Some("458")]`). Equally: a group that never commits because its trigger stayed suppressed, or entries lost because the producer died mid-group. |
| Invariant | `persist_records` resolves the batch's `WalAction`s once and brackets them in `WalTarget::begin_group` / `end_group` whenever there is more than one, closing the group on every path including the staging-error path. A one-action batch skips the markers — a single WAL entry is already indivisible, and the hot path should not pay for a group it cannot need; the count comes from `WriteRecord::wal_actions` itself, so no second guess at how many entries a command produces can drift low. The markers travel the same FIFO channel as the entries, so grouping is decided by *enqueue* order, not by timing. `FlushEngine::should_flush` returns false while `group_depth > 0`, suppressing both the size and the timeout trigger, so the group commits as one `WriteBatch`. Two deliberate exceptions: an explicit `WalCommand::Flush` is honored mid-group (a durability request must not deadlock behind a group), and a channel disconnect drains and commits an unclosed group (the producer is gone; losing the writes would be strictly worse). |
| Outcome variant | n/a (storage-layer invariant; a failure surfaces as a torn restore, not as a reply) |
| Forced by | `test_sink_ungrouped_entries_tear_across_batches`, `test_sink_write_group_survives_the_batch_timeout`, `test_sink_write_group_survives_the_size_threshold`, `persist_brackets_the_batch_in_one_write_group`, `single_action_persist_skips_the_write_group`, `write_group_closes_on_staging_failure`, `failed_group_open_skips_writes`, `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave` |
| Bug refs | `.scratch/testing-improvements/issues/65` (the flake that exposed it), `.scratch/testing-improvements/issues/43` (asserted the guarantee on reasoning that did not hold) |

Deliberate non-guarantees, so a future reader does not mistake them for gaps:

* **Cross-shard atomicity is not provided.** A cross-shard `MSET` is several independent
  `persist` calls on several shards, each its own group. The cut can capture one shard's half and
  not another's — FrogDB's documented model (execution atomicity via locking, not durability
  atomicity). `test_checkpoint_cross_shard_mset_contract_under_concurrent_bgsave` pins the
  contract as written, including its limits.
* **A group is not size-capped.** Atomicity and size-capping are mutually exclusive; an oversized
  transaction buys its atomicity with one oversized `WriteBatch`. The flush thread keeps draining
  the channel while a group is open, so backpressure cannot deadlock — the memory moves from the
  channel into the staged batch.
* **`WalEntry::Clear` remains a hard flush barrier** and cuts a group, because a range tombstone's
  bound is only correct against committed state. Unreachable in practice: core's
  `WalStrategy::ClearShard` yields exactly one action, so a clear is always a group of its own.

---

# Write path — durability modes and append failures

## FM-PERSISTENCE-002 — `sync` durability loses no acknowledged write

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = sync` (Redis `appendfsync always`); a write command is acknowledged; the process dies without a clean shutdown. |
| Observable | Every write the client saw a reply for is in the recovered keyspace. `INFO persistence` reports `durability_mode:sync`. |
| NOT observable | An acknowledged write missing after recovery. A commit that reaches the sink with `sync = false` while the mode is `sync` — the batch would be in the OS page cache, not on the device, and a power loss would take it. |
| Invariant | `DurabilityMode::Sync` makes every `WriteSink::commit` pass `sync = true`, and `persist_records` under `Durability::Confirm` snapshots the WAL sequence before the first entry and `flush_through(start_seq)` before returning — so the reply cannot outrun the fsync. The page-cache crash model in `wal/tests.rs` (`PageCacheSink`) makes the distinction real: entries committed without `sync` live in a cache the simulated crash discards. |
| Outcome variant | n/a (storage-layer invariant; `INFO persistence` `durability_mode`, `wal_durability_lag_ms`) |
| Forced by | `test_sync_mode_zero_acked_write_loss`, `test_fsync_seam_sync_flag_matches_mode`, `test_sync_mode_crash_recovery`, `confirm_snapshots_sequence_then_flushes_once` |
| Bug refs | `.scratch/testing-improvements/issues/12-durability-fsync-boundary.md` (done — these tests are its outcome) |

## FM-PERSISTENCE-003 — `periodic` durability bounds loss by the flush interval

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = periodic` (the default, `interval_ms = 1000`; Redis `appendfsync everysec`); a crash lands between two periodic syncs. |
| Observable | Writes acknowledged more than one interval before the crash are recovered. Writes acknowledged inside the current window may be absent — that is the mode's contract, not a defect. |
| NOT observable | Loss of a write older than the interval, i.e. an unbounded window: the periodic sync thread silently stopping would turn `periodic` into `async` while `INFO` still reports `durability_mode:periodic`. Also not observable: a *gap* — a recovered keyspace holding a later write while a strictly earlier one from the same shard is missing (the WAL commits in enqueue order, so loss is always a suffix). |
| Invariant | `spawn_periodic_sync` drives a `WalCommand::Flush` every `interval_ms`, and that flush commits with `sync = true`; between flushes `FlushEngine::should_flush` may still commit on the size threshold, which only ever shortens the window. |
| Outcome variant | n/a (`INFO persistence` `durability_mode:periodic`, `wal_durability_lag_ms`) |
| Forced by | `test_periodic_mode_loss_bounded_by_flush_interval`, `test_periodic_mode_after_interval`, `test_periodic_mode_within_window` |
| Bug refs | none |

## FM-PERSISTENCE-004 — `async` durability is unbounded until something forces a sync

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = async` (Redis `appendfsync no`); a crash with no explicit flush since the last one. |
| Observable | Only writes covered by an *explicit* durability request survive — a `Durability::Confirm` persist, an operator `sync_wal()`, or shutdown's drain. Everything after the last such point is gone. |
| NOT observable | A silently *bounded* window that makes `async` behave like `periodic` — an operator who chose `async` must not be able to mistake page-cache residency for durability. Equally: an explicit flush that does **not** sync, which would leave the "durable point" undurable. |
| Invariant | `DurabilityMode::Async` commits with `sync = false` on every batch trigger and spawns no periodic syncer; only `WalCommand::Flush` (from `flush_through` / `flush_async` / drop) passes `sync = true`. Durability is therefore an event, not a clock. |
| Outcome variant | n/a (`INFO persistence` `durability_mode:async`) |
| Forced by | `test_async_mode_unbounded_loss_without_fsync`, `test_async_mode_window_bounded_only_by_external_fsync`, `test_async_mode_explicit_flush`, `test_explicit_sync_wal` |
| Bug refs | none |

## FM-PERSISTENCE-005 — a WAL failure under the default `continue` policy acknowledges a lost write

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = continue` (**the default**) and the WAL sink fails — a full or read-only device, a RocksDB write error, a dead flush channel — while a write command is executing. |
| Observable | `+OK`. The key is in the in-memory keyspace and is served to readers, but it is not on disk and does not come back after a restart. The only signal is `INFO persistence` (`wal_last_flush_status:err`, `wal_flush_failures`, `wal_lost_ops`, `wal_pending_ops`), the `frogdb_wal_*` counters, and a rate-limited `ERROR` log (`FAILURE_LOG_INTERVAL`, one line per 100 failures, so a sustained outage does not drown the log). |
| NOT observable | An error reply (that is FM-PERSISTENCE-006's mode, and it needs the non-default policy). A shard that stops accepting commands, panics, or drops the remaining entries of the same batch — `stage_actions` under `FireAndForget` logs each failure and continues to the next action. Counters must not reset on a later success: `FlushOutcomes` accumulates `flush_failures`/`lost_ops`/`lost_bytes` for the life of the process, so an operator sampling `INFO` after recovery still sees that data was lost. |
| Invariant | `Durability::FireAndForget` never returns `Err`; the failure is recorded in `FlushOutcomes` and surfaced through `INFO`/metrics rather than the reply. This is a deliberate availability-over-durability default and the main deviation from Redis, which stops accepting writes entirely (`MISCONF`) once a background save fails — see [Redis deviations](#redis-deviations). |
| Outcome variant | `frogdb_wal_lost_ops` / `frogdb_wal_flush_failures` (`INFO persistence` `wal_lost_ops`, `wal_flush_failures`, `wal_last_flush_status`) |
| Forced by | `wal_failure_in_continue_mode_acks_the_write_and_keeps_it_in_memory`, `test_sink_stage_failure_recorded_and_surfaced`, `test_sink_failure_attribution_and_recovery`, `fire_and_forget_never_flushes_and_continues_on_error`, `test_continue_mode_default`, `should_rollback_follows_shared_flag` |
| Bug refs | none |

## FM-PERSISTENCE-006 — a WAL failure under `rollback` refuses the write and restores the key

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = rollback` (live-settable) and the WAL fails while a single write command is executing. |
| Observable | `-IOERR WAL persistence failed: <cause>`. The key is back at its pre-command value — restored if it existed, deleted if it did not, with its previous expiry and no leftover TTL. `frogdb_wal_rollbacks_total` increments. |
| NOT observable | A committed in-memory value with an error reply — the pair a client cannot reconcile, and the exact reason this policy exists. Also not observable: rollback for a *read*, for a command with no WAL strategy, or when no WAL is configured (`has_wal()` gates the whole path, so an in-memory-only server never pays the snapshot cost); a rollback that resurrects a key the command did not touch; replication or keyspace-notification effects for the refused write, which are skipped because `run_write_effects` is never reached on the error arm. |
| Invariant | `execute_command` decides `rollback_mode = is_write && has_wal() && should_rollback()` *before* execution and captures a `WriteSnapshot` of exactly the keys the handler's `WalStrategy` will persist (`ClearShard` is filtered out — a full clear has no per-key undo, so on failure the in-memory clear stands). The policy lives in a shared `AtomicU8` so `CONFIG SET wal-failure-policy` retunes every shard without a restart. `warn_if_rollback_without_sync` logs at startup that `rollback` with a non-`sync` durability mode still leaves a loss window: rollback only covers failures the shard *observes*, and a background flush failure is observed at the next `Confirm` (FM-PERSISTENCE-007), not at the write that was lost. |
| Outcome variant | `frogdb_wal_rollbacks_total`; reply prefix `-IOERR` |
| Forced by | `wal_failure_in_rollback_mode_replies_ioerr_and_restores_the_key`, `test_rollback_existing_key`, `test_rollback_missing_key`, `test_rollback_del_restores_key`, `test_rollback_rename`, `test_rollback_preserves_expiry`, `test_rollback_clears_added_expiry`, `test_rollback_mode_flag_toggle`, `test_snapshot_arc_efficiency` |
| Bug refs | none |

## FM-PERSISTENCE-007 — a swallowed background flush failure surfaces at the next durability confirmation

| Field | Value |
|---|---|
| Trigger | A batch commits in the background — fired by the size threshold or the batch timeout, with no caller waiting — and fails. A later write then asks for `Durability::Confirm`. |
| Observable | The confirming caller gets the error, attributed to the failed sequence range, even though it was a *different* write that failed. In rollback mode that becomes the confirming write's `-IOERR` (FM-PERSISTENCE-006). |
| NOT observable | A `Confirm` returning `Ok` while `highest_failed_seq >= target_seq` — an acknowledged write that outran a swallowed failure, which is the whole hazard. Equally: the failure staying latched forever after the sink recovers — once a later commit succeeds past the failed range, `confirm_durable_through` stops reporting it, so a transient error does not poison every subsequent write. |
| Invariant | `FlushOutcomes` records `highest_failed_seq` and `last_error` alongside `durable_seq`; `confirm_durable_through(after_seq, target_seq, flush_result)` compares the confirmed range against both, so a background failure inside the range the caller is confirming is re-raised rather than dropped on the floor of a thread nobody joins. |
| Outcome variant | `frogdb_wal_flush_failures` (`INFO persistence` `wal_last_flush_status:err`, `wal_flush_failures`) |
| Forced by | `test_sink_size_threshold_flush_error_surfaces_on_confirm`, `test_sink_timeout_flush_error_surfaces_on_confirm`, `confirm_propagates_flush_failure` |
| Bug refs | none |

## FM-PERSISTENCE-008 — a failed `Confirm` batch is never flushed as a partial batch

| Field | Value |
|---|---|
| Trigger | Under `Durability::Confirm`, one of the batch's stage calls fails (or the group could not be opened) — a dead channel, a full device. |
| Observable | The caller gets the staging error. Nothing from that batch is force-flushed; the shard's durable sequence does not advance on its account. |
| NOT observable | A `flush_through` for a batch that failed to stage — it would confirm a *prefix* of the batch as durable and let the caller ack it. Also not observable: N identical error logs for a batch whose group open failed; the entries are skipped wholesale, since a marker only fails when the channel is gone and every write below would fail identically. |
| Invariant | The `Confirm` arm checks `opened? staged? closed?` in that order before reaching `flush_through`, so any earlier failure short-circuits. `stage_actions` uses `?` per action under `Confirm` (stop at the first failure) and `inspect_err`-and-continue under `FireAndForget`. |
| Outcome variant | n/a (propagates as the persist error the caller maps — `-IOERR` in rollback mode, a counter in continue mode) |
| Forced by | `confirm_write_failure_aborts_before_flush`, `write_failure_propagates`, `failed_group_open_skips_writes` |
| Bug refs | none |

## FM-PERSISTENCE-009 — with no WAL configured the persist path does nothing at all

| Field | Value |
|---|---|
| Trigger | Persistence disabled (or a shard built without a WAL sink) and a write command executes. |
| Observable | The write applies in memory and replies normally. Nothing is staged, no group is opened, no flush is attempted, and no error is logged. |
| NOT observable | A begin/end-group pair with no entries between them; a `flush_through` on a sink that does not exist; a spurious persist error on an in-memory-only server. Recovery must likewise leave the filesystem untouched — a disabled-persistence boot does not create the data directory. |
| Invariant | `persist_records` short-circuits on `wal_sequence() == None` before any marker or write, so "no WAL" is one branch at the top of the function rather than an `Option` check smeared across nine `WalTarget` methods (each of which is *also* individually a no-op without a writer). |
| Outcome variant | n/a |
| Forced by | `no_wal_short_circuits`, `persistence_disabled_touches_nothing` |
| Bug refs | none |

## FM-PERSISTENCE-010 — a producer dying mid-group commits the group rather than discarding it

| Field | Value |
|---|---|
| Trigger | The shard (and with it the WAL command sender) is dropped while a write group is still open — a panicking shard task, or an ordinary shutdown that races an in-flight batch. |
| Observable | The staged entries are committed and recoverable. `RocksWalWriter::drop` replaces the sender and joins the flush thread, so a clean drop also drains everything pending. |
| NOT observable | Entries silently discarded because their group never closed. A flush thread that exits with a non-empty staged batch. A drain error that goes unrecorded — the disconnect path still attributes its failure into `FlushOutcomes`, so a shutdown that could not persist is visible rather than assumed clean. |
| Invariant | `flush_thread_loop` treats channel disconnect as "drain and commit": the producer is gone, so an unclosed group can never be closed, and losing the writes is strictly worse than committing a group whose atomicity nobody can still observe. This is one of the two deliberate exceptions to group-suppressed flushing named in FM-PERSISTENCE-001. |
| Outcome variant | `frogdb_wal_flush_failures` when the drain commit itself fails |
| Forced by | `test_sink_unclosed_group_commits_on_disconnect`, `test_sink_disconnect_drain_error_recorded`, `test_wal_drop_flushes_pending` |
| Bug refs | none |

## FM-PERSISTENCE-011 — an explicit flush inside a group is honoured immediately

| Field | Value |
|---|---|
| Trigger | A `Durability::Confirm` write (or an operator `sync_wal`) issues `WalCommand::Flush` while another batch has a group open on the same shard. |
| Observable | The flush commits the staged batch and returns; the caller is not parked until the group closes. Nested groups still collapse into a single commit — depth, not the first `end_group`, decides when suppression lifts. |
| NOT observable | A durability request deadlocking behind an open group. A nested group committing as two batches, which would reopen the tear FM-PERSISTENCE-001 closes. |
| Invariant | `should_flush` is suppressed while `group_depth > 0`, but an explicit `WalCommand::Flush` bypasses `should_flush` entirely; `begin_group`/`end_group` maintain `group_depth` so only the outermost close re-enables the automatic triggers. |
| Outcome variant | n/a |
| Forced by | `test_sink_explicit_flush_inside_a_group_is_honoured`, `test_sink_nested_write_groups_commit_as_one_batch` |
| Bug refs | none |

## FM-PERSISTENCE-012 — a shard clear is a hard flush barrier

| Field | Value |
|---|---|
| Trigger | `FLUSHDB` / `FLUSHALL` reaches a shard's WAL as `WalEntry::Clear`, with writes staged before it and more writes arriving after. |
| Observable | The pre-clear batch commits, the clear commits, and post-clear writes land in a batch of their own — and survive. The durable sequence advances across the clear, so a `Confirm` immediately after a clear is answerable. |
| NOT observable | A post-clear write swallowed by the clear's range tombstone, or a pre-clear write surviving it. Space reclamation resurrecting cleared data at the next compaction. |
| Invariant | `apply_clear` commits what is staged, applies the range delete, and commits again — the bound of a range tombstone is only correct against committed state, so it cannot be batched with either side. Reclamation (`rocks/reclaim.rs`, `delete_file_in_range_cf` + a forced bottommost compaction on a dedicated thread) runs *after* the clear commits and is coalesced per `(tier, shard)`, so it never races a concurrent write into resurrection. |
| Outcome variant | n/a |
| Forced by | `test_sink_clear_is_a_flush_barrier_between_batches`, `test_sink_clear_advances_durable_sequence`, `test_wal_clear_reclamation_end_to_end`, `clear_reclamation_keeps_data_gone_after_reopen`, `clear_reclamation_preserves_post_clear_writes`, `clear_shard_routes_to_clear` |
| Bug refs | none |

## FM-PERSISTENCE-013 — WAL backpressure blocks the writer, it never drops entries

| Field | Value |
|---|---|
| Trigger | Writes arrive faster than the flush thread commits them and the WAL command channel (capacity 8192) fills. |
| Observable | The producing shard blocks on the send until the flush thread makes room; every entry is eventually committed and recoverable. Retuning `wal-batch-size-threshold` at runtime takes effect on the next batch without a restart. |
| NOT observable | An entry dropped, reordered, or silently coalesced away under pressure; a `try_send` failure path that acknowledges the write anyway. A retune that only reaches shards created after the `CONFIG SET`. |
| Invariant | The channel is bounded and the send is awaited, so backpressure propagates to the shard rather than into a lossy buffer. The size threshold is a shared handle read per batch, not a value copied into each engine at construction. |
| Outcome variant | `INFO persistence` `wal_pending_ops` / `wal_pending_bytes` / `wal_durability_lag_ms` (`WalLagStats`) |
| Forced by | `test_wal_backpressure_no_data_loss`, `test_wal_batch_threshold`, `test_wal_batch_threshold_live_retune`, `test_wal_adopts_shared_batch_size_threshold_handle`, `test_wal_lag_stats` |
| Bug refs | none |

## FM-PERSISTENCE-014 — an HLL delta is replay-idempotent

| Field | Value |
|---|---|
| Trigger | `PFADD` persists a register delta rather than the whole sketch; the same merge operand is replayed after a crash (RocksDB re-applies WAL records that were not yet in an SST). |
| Observable | The recovered sketch equals the one a single application would produce; cardinality does not drift upward on repeated replay. |
| NOT observable | A doubled or reordered fold changing the estimate. A delta applied against a base that was never written — the base `put` and its operands go into the WAL in order, and the merge operator folds whatever prefix survives. |
| Invariant | `WalAction::MergeHllDelta` becomes a RocksDB merge operand folded by `full_value_merge` → `merge_hll_serialized`, which takes the per-register maximum. Register-max folding is idempotent and order-independent, so replay is safe by construction rather than by a de-duplication table. |
| Outcome variant | `frogdb_wal_merge_operands` |
| Forced by | `hll_merge_operand_folds_and_survives_reopen`, `hll_batch_merge_folds_operand`, `test_sink_stages_merge_operand_in_order`, `merge_hll_delta_routes_to_merge` |
| Bug refs | none |

---

# Checkpoint — BGSAVE lifecycle, atomicity, retention

FrogDB's `BGSAVE` is a RocksDB checkpoint of the single multi-column-family database, staged in a
dot-prefixed temp directory and published by one rename. There is no fork, no COW page accounting,
and no synchronous `SAVE` — the command is an explicit `-ERR ... not supported` stub pointing at
`BGSAVE`. Snapshot artifacts are **not** loaded at boot; see FM-PERSISTENCE-021.

## FM-PERSISTENCE-015 — a second BGSAVE never starts a second save

| Field | Value |
|---|---|
| Trigger | `BGSAVE` (or `BGSAVE SCHEDULE`, or a periodic tick) arrives while a save is already running. |
| Observable | Plain `BGSAVE` is refused with `+Background save already in progress` and queues nothing. `BGSAVE SCHEDULE` replies `+Background saving scheduled` and arms exactly one follow-up, however many times it is issued. A periodic tick during a save is dropped, never queued. |
| NOT observable | Two concurrent stagers, two runs at the same epoch, or an epoch skipped/reused. A `SCHEDULE` storm turning into a queue of saves. An epoch minted by a `try_begin` that lost the CAS. |
| Invariant | `SnapshotScheduler` is a pure atomics state machine: `try_begin` claims `in_progress` with a `compare_exchange` and only then mints `epoch.fetch_add(1) + 1`, so the epoch and the slot are taken together. `request_mode` maps the loser of that CAS to `AlreadyRunning` (`Immediate`) or `Coalesced` (`Schedule`); the follow-up is a single `AtomicBool`, not a counter. |
| Outcome variant | `SnapshotRequest::{Started(epoch), Coalesced, AlreadyRunning}`; `SnapshotError::AlreadyInProgress` on the `start_snapshot` path |
| Forced by | `test_scheduler_request_mode_immediate_no_queue_vs_schedule_arms`, `test_scheduler_begin_while_running_rejected`, `test_scheduler_request_while_running_coalesces`, `test_scheduler_begin_while_idle`, `test_scheduler_resumes_epoch`, `test_scheduler_schedule_only_while_running`, `test_schedule_true`, `test_schedule_false`, `bgsave_overlap_observes_already_running_on_real_coordinator`, `bgsave_starts_a_snapshot`, `bgsave_schedule_starts_when_idle`, `test_bgsave_concurrent_returns_already_in_progress` |
| Bug refs | `.scratch/testing-improvements/issues/45-persistence-cmd-replies.md` (done — the `+`-vs-`-` reply shape, see [Redis deviations](#redis-deviations)) |

## FM-PERSISTENCE-016 — a coalesced follow-up is never stranded

| Field | Value |
|---|---|
| Trigger | `BGSAVE SCHEDULE` arms the follow-up flag in the window where the running save is finishing — after it cleared `in_progress`, before (or while) it checks `scheduled`. |
| Observable | The follow-up runs, either as the finishing runner's re-begin or as a fresh run started by the arming caller. Epochs stay unique, contiguous, and monotonic across the whole cycle. |
| NOT observable | A `scheduled` flag left set with no runner — a save the operator was told was scheduled that never happens. Two runners racing because both the finisher and the armer decided to start. An epoch double-run. |
| Invariant | Both sides re-check the other's flag after setting their own. `finish_and_maybe_rebegin` clears `in_progress`, then consumes `scheduled` with a `swap`, then re-claims `in_progress` with a CAS — returning `None` if that CAS fails, so an epoch is never double-run. `arm_follow_up` symmetrically re-loads `in_progress` after arming and, if the runner has already exited, reclaims the flag and starts the run itself. |
| Outcome variant | `SnapshotRequest::{Coalesced, Started(epoch)}` |
| Forced by | `test_scheduler_arm_follow_up_after_runner_exit_starts`, `test_scheduler_finish_with_pending_reschedule_reruns_once`, `test_scheduler_finish_without_schedule_idles`, `test_scheduler_request_after_finish_starts`, `test_scheduler_epoch_monotonic_across_cycle`, `test_scheduler_concurrent_request_storm`, `test_noop_concurrent_request_storm_settles_idle`, `test_noop_start_advances_epoch_monotonically`, `test_noop_request_always_starts_when_quiescent`, `test_noop_coordinator` |
| Bug refs | `.scratch/arch-deepening/issues/08-snapshot-coalesce-lost-wakeup.md` (the window this row closes) |

## FM-PERSISTENCE-017 — a partial snapshot is never visible as a snapshot

| Field | Value |
|---|---|
| Trigger | The process dies, or the stager fails (unwritable directory, ENOTEMPTY on promotion, IO error mid-checkpoint), part-way through building a snapshot. |
| Observable | No `snapshot_NNNNN` directory for the failed epoch. At most a dot-prefixed `.snapshot_NNNNN.tmp`, which every scanner skips and which the next run at that epoch removes before starting. A snapshot directory that *does* exist always holds both `checkpoint/` and a `metadata.json` carrying `FROGDB_SNAPSHOT_COMPLETE_v1`. |
| NOT observable | A `snapshot_NNNNN` without its completion marker being adopted as the latest snapshot; a leaked temp directory (a full extra copy of the database) after a failure; a metadata file that is half-written. A crashed epoch permanently wedging that epoch number. |
| Invariant | Everything is built under `.snapshot_NNNNN.tmp` and published by a single `rename` — the commit point. Every `?` in `SnapshotStager::run` drops a `TmpDirGuard` whose `Drop` removes the staging directory unless `commit()` was called, so cleanup covers *all* failure paths rather than the one somebody remembered. `metadata.json` is itself written `.tmp` + rename inside the staging dir, and `mark_complete` is the only writer of the completion marker; `load_latest_metadata` refuses to adopt an unmarked snapshot (it resumes the epoch counter but reports no last save). |
| Outcome variant | `SnapshotError::{Io, Internal}`, `frogdb_persistence_errors{type="snapshot"}` |
| Forced by | `test_stager_happy_path`, `test_stager_checkpoint_failure_aborts_cleanly`, `test_stager_promote_rename_failure_leaves_no_leak`, `test_stager_reclaims_stale_tmp`, `test_stager_excludes_search_sidecar`, `test_metadata_file_new`, `test_metadata_file_complete`, `test_metadata_serialization`, `test_metadata_to_metadata`, `test_metadata_deserializes_legacy_num_keys_field`, `test_handle_is_bare_epoch_carrier`, `test_incomplete_snapshot_skipped`, `test_corrupted_snapshot_metadata`, `test_truncated_metadata_recovery`, `test_missing_completion_marker` |
| Bug refs | none |

## FM-PERSISTENCE-018 — publishing steps after the commit point cannot fail a durable snapshot

| Field | Value |
|---|---|
| Trigger | The promotion rename succeeded, and then repointing `latest` fails (an occupied path) or retention cannot delete an old snapshot (permissions, a busy directory). |
| Observable | The snapshot stays installed and the save is reported as successful; the failure is a `WARN`. Retention keeps the newest `max-snapshots` (default 5, `0` = unlimited) and evicts the oldest, retrying the over-count on the next successful save. |
| NOT observable | A durable, already-published snapshot being rolled back or deleted because a cosmetic post-step failed. Retention deleting a *newer* snapshot, or counting `.snapshot_*.tmp` / non-directory entries as snapshots. |
| Invariant | `install()` is the commit point; everything after it is wrapped in warn-only `if let Err`. `cleanup_old_snapshots` filters to directories named `snapshot_<u64>`, sorts by parsed epoch, and deletes only the oldest overflow; a delete failure warns and the loop continues. |
| Outcome variant | n/a (warn-only; the save still reports success) |
| Forced by | `test_stager_symlink_failure_is_nonfatal`, `test_stager_retention_across_epochs`, `test_cleanup_old_snapshots`, `test_cleanup_unlimited`, `test_config_default` |
| Bug refs | `.scratch/hardening/issues/03` (the failure surface these warnings do *not* reach) |

## FM-PERSISTENCE-019 — every write acknowledged before the cut is in the cut

| Field | Value |
|---|---|
| Trigger | A checkpoint (BGSAVE, or the replication FULLRESYNC path) is taken while clients are writing under the default non-`sync` durability, where a write is acknowledged as soon as it is staged in its shard's flush engine. |
| Observable | The restored checkpoint contains every write the server had already replied to when the cut began, including search-index state. Writes landing *during* the drain may or may not be captured — the safe direction. |
| NOT observable | A restored checkpoint missing an acknowledged write because it was still sitting in a flush engine at cut time. A drain that blocks the command pipeline, stalls writers, or costs one flush latency *per shard* instead of one overall. |
| Invariant | `quiesce_shards_for_checkpoint` is a **drain, not a freeze**: two fan-out waves of ack-carrying messages (flush search indexes, then flush WAL) down the same FIFO channel the writes travelled. The `WalPersistence` effect enqueues a write's WAL entry before `ReplicationBroadcast` acknowledges it, so the drain message is behind every acknowledged write by construction — the argument is about *ordering*, not timing, which is why no lock is needed. Both waves send to all shards first and then await, so the cost is one flush latency, not `num_shards` of them. |
| Outcome variant | n/a (storage-layer invariant) |
| Forced by | `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave`, `test_bgsave_snapshot_survives_restart`, `test_concurrent_bgsave_stress_restores_cleanly`, `test_ft_search_bgsave_flushes_search` |
| Bug refs | `.scratch/testing-improvements/issues/43-checkpoint-cross-shard-cut.md`; commit `ebdf7d9e` (the full-resync half of the same drain) |

## FM-PERSISTENCE-020 — a wedged shard drops out of the drain silently

| Field | Value |
|---|---|
| Trigger | One shard cannot service the quiesce message — its task has panicked, its channel is closed, or it is blocked long enough that the checkpoint proceeds without it. |
| Observable | Current behavior: the checkpoint is cut anyway and reported as successful, with that shard's staged-but-uncommitted writes absent from the artifact. There is **no log line, no metric, and no counter** from the quiesce itself; the drain returns `()` and has no channel to report a partial result. |
| NOT observable | The checkpoint aborting or reporting the partial drain — a gap, not a guarantee, and worse on the FULLRESYNC path, where the uncaptured writes are missing from the replica permanently rather than just from one artifact. |
| Invariant | `fan_out` uses `let _ =` on both the send and the receive, deliberately matching pre-existing behavior so a wedged shard cannot hang a checkpoint. Forcing this needs a shard harness that can wedge one shard mid-checkpoint and inspect the artifact; the campaign has not built it. |
| Outcome variant | n/a (no outcome is reported — that is the gap) |
| Forced by | MISSING ([gap: 05-quiesce-partial-drain-is-silent.md](../issues/open/05-quiesce-partial-drain-is-silent.md)) |
| Bug refs | `.scratch/hardening/issues/05` |

## FM-PERSISTENCE-021 — a snapshot is restored by an explicit operator install, never at boot

| Field | Value |
|---|---|
| Trigger | An operator wants to restore a `BGSAVE` artifact: they copy `snapshot_NNNNN/checkpoint/.` into a `checkpoint_ready` sibling of a fresh data directory and start the server. |
| Observable | Startup installs the staged checkpoint and the whole keyspace comes back, every type intact; a key written *after* the cut is absent. The restored server then behaves normally — later writes are WAL-durable and replay across a further restart, layered on the installed checkpoint. |
| NOT observable | A snapshot being loaded automatically into an existing data directory. Restarting a server against its own data directory rolling it back to the last `BGSAVE` — restart recovery is RocksDB + WAL, not the snapshot; the snapshot directory is read at startup only to resume the epoch counter and seed `last_save_time`. |
| Invariant | Recovery consumes exactly one staged artifact, `checkpoint_ready`, and nothing under `snapshot_dir`. This is a deliberate divergence from Redis, which loads its RDB at startup — see [Redis deviations](#redis-deviations). |
| Outcome variant | n/a |
| Forced by | `test_bgsave_snapshot_restores_into_fresh_data_dir`, `test_restored_snapshot_accepts_and_replays_new_writes`, `test_bgsave_snapshot_survives_restart` |
| Bug refs | none |

## FM-PERSISTENCE-022 — a failed BGSAVE is invisible to every client-facing surface

| Field | Value |
|---|---|
| Trigger | The stager fails (disk full, unwritable directory, IO error) after `BGSAVE` has already replied. |
| Observable | Current behavior: `LASTSAVE` and `rdb_last_save_time` correctly do **not** advance — a failed save never stamps `last_save_time`. But `INFO persistence` still reports `rdb_last_bgsave_status:ok`, because that field is a hardcoded literal with no failure state behind it. The only real signals are a `tracing::error!` and `frogdb_persistence_errors{type="snapshot"}`. |
| NOT observable | A failed save advancing `LASTSAVE` (that *is* pinned). What an operator polling `INFO` cannot see: that every save since startup has failed while `rdb_last_bgsave_status` reads `ok` next to a stale-but-plausible `rdb_last_save_time`. Redis reports `err` here and additionally refuses writes (`MISCONF`) — FrogDB does neither; see [Redis deviations](#redis-deviations). |
| Invariant | `SnapshotRun::record` only stamps `last_save_time`/`last_metadata` on `Ok`, which is why `LASTSAVE` is honest. `PersistenceSnapshot` carries no failure field, so there is nothing for `PersistenceSection::render` to report even if it wanted to — the fix is a plumbing change, not a formatting one. |
| Outcome variant | `frogdb_persistence_errors{type="snapshot"}` (the only truthful surface) |
| Forced by | `lastsave_tracks_real_bgsave_and_ignores_failed_saves`, `lastsave_returns_zero_when_never_saved`, `lastsave_returns_timestamp_after_save`, `bgsave_starts_each_time_under_instant_completion`, `test_lastsave_basic`, `test_bgsave_then_lastsave`, `test_bgsave_basic` |
| Bug refs | `.scratch/hardening/issues/03`, `.scratch/hardening/issues/07` |

## FM-PERSISTENCE-023 — the renames that publish a checkpoint are not fsynced

| Field | Value |
|---|---|
| Trigger | Power loss (not a process crash) shortly after a "Snapshot completed" log line, or shortly after a staged checkpoint is installed over the live database. |
| Observable | Current behavior: unverified. RocksDB fsyncs the checkpoint *contents*, but neither the promotion rename, the `latest` repoint, the `metadata.json` rename, nor the two `checkpoint_ready` install renames is followed by a directory fsync — there is no `sync_all`/`sync_data` anywhere in the persistence or replication production paths. The directory entry that publishes a durable payload may therefore be lost while the in-memory coordinator, `LASTSAVE`, and `rdb_last_save_time` all report success. |
| NOT observable | Any claim that a completed `BGSAVE` is power-loss durable. Note the WAL watermark side-file is deliberately *not* fsynced for the opposite reason (FM-PERSISTENCE-035): it may only lag, so an unsynced write can under-report but never false-alarm. |
| Invariant | Rename atomicity is relied on and tested (crash-window tests inject the process-crash cases); rename *durability* is neither argued nor tested. Forcing it needs a power-loss/barrier harness — filesystem fault injection the campaign has not built. |
| Outcome variant | n/a |
| Forced by | MISSING ([gap: 04-checkpoint-publish-renames-not-fsynced.md](../issues/open/04-checkpoint-publish-renames-not-fsynced.md)) |
| Bug refs | `.scratch/hardening/issues/04` |

---

# Staged-checkpoint install — the boot-time filesystem protocol

## FM-PERSISTENCE-024 — an incomplete staged checkpoint is refused without touching the live database

| Field | Value |
|---|---|
| Trigger | `checkpoint_ready/` exists but is not a complete RocksDB directory — a full sync that died mid-download, a partial operator copy, a directory holding stray `.sst` files and no `CURRENT`. |
| Observable | Startup fails with `RecoveryError { phase: InstallStagedCheckpoint }` naming the incomplete directory. The live database is exactly as it was: still in place, `CURRENT` intact, and no `<db>_backup_<ts>` created. |
| NOT observable | The live database being renamed aside for a checkpoint that cannot replace it — which would leave RocksDB to open a fresh empty database in its place: silent, total data loss dressed as a clean boot. A partial install being retried into a worse state. |
| Invariant | `is_complete_db()` (presence of RocksDB's own `CURRENT` manifest pointer — the same marker `RocksStore::open` trusts) is checked **before** the first rename, so the refusal happens while nothing has moved. |
| Outcome variant | `RecoveryPhase::InstallStagedCheckpoint` |
| Forced by | `test_load_staged_checkpoint_incomplete_dir_refuses_and_preserves_data`, `incomplete_staged_checkpoint_is_refused_without_touching_live_db` |
| Bug refs | none |

## FM-PERSISTENCE-025 — the install is idempotent across both of its crash windows

| Field | Value |
|---|---|
| Trigger | A crash between the two renames of an install (`<db>` → `<db>_backup_<ts>`, then `checkpoint_ready` → `<db>`), or after the second, followed by a reboot. |
| Observable | Crash in the middle: the next boot finishes the install cleanly and the previous data is still recoverable from the backup directory. Crash after: the marker is gone, so the next boot is a no-op with no spurious backup. Absent marker, or a db directory with no parent, are both quiet no-ops rather than errors. |
| NOT observable | A double install; a backup made for a no-op boot; a boot that hangs or errors because a previous install was interrupted. |
| Invariant | The staged directory's *name* is the state: rename 2 consumes it, so completion is self-evident and the operation is naturally idempotent. Rename 1 preserves rather than deletes, so the pre-install database survives a bad checkpoint. |
| Outcome variant | `RecoveredState::installed_staged_checkpoint` |
| Forced by | `test_load_staged_checkpoint_crash_after_backup_recovers`, `test_load_staged_checkpoint_idempotent_after_success`, `test_load_staged_checkpoint_installs_and_backs_up_old_db`, `test_load_staged_checkpoint_first_sync_no_existing_db`, `test_load_staged_checkpoint_absent_marker_is_noop`, `test_load_staged_checkpoint_no_parent_is_noop`, `staged_checkpoint_is_installed` |
| Bug refs | none |

## FM-PERSISTENCE-026 — exactly one previous-database backup survives an install

| Field | Value |
|---|---|
| Trigger | Repeated staged-checkpoint installs (every replica full sync is one), each of which renames the live database aside. |
| Observable | After a successful install, exactly one `<db>_backup_<ts>` remains — the newest. Older ones are deleted. A leftover backup from an interrupted install is kept rather than pruned away, and never blocks a new install. |
| NOT observable | Backups accumulating without bound (before retention existed, every full sync leaked a complete copy of the database, forever). `_backup_2` outranking `_backup_10` — the suffix is compared numerically, not lexically. Files that merely share the prefix being deleted; only directories are backups. A pruning failure failing the install — retention is hygiene. |
| Invariant | `BACKUP_RETENTION = 1`: one generation covers the story the backup exists for (recover the immediately-previous database if a sync installed bad data); every additional generation is a full database copy with no recovery story attached. |
| Outcome variant | n/a (warn-only on failure) |
| Forced by | `test_load_staged_checkpoint_prunes_older_backups`, `test_load_staged_checkpoint_crash_recovery_keeps_lone_backup`, `test_prune_backups_orders_numerically_not_lexically`, `test_prune_backups_noop_within_retention` |
| Bug refs | none |

---

# Recovery — startup ordering, fallbacks, and refusals

`frogdb-recovery::recover` is one synchronous seam over six ordered phases. Every `RecoveryError`
is fatal: there is a single `?` at the server's `init_infrastructure`, no phase-level catch, no
retry, and no degraded mode. What is *survivable* is therefore decided inside each phase, and the
rows below are that decision, phase by phase.

## FM-PERSISTENCE-027 — the recovery phase order is a data dependency, not a layout accident

| Field | Value |
|---|---|
| Trigger | Any boot. The order is: install staged checkpoint → open RocksDB → restore shard stores → restore functions → restore replication state → open cluster storage. |
| Observable | A staged checkpoint's dataset is what gets opened and restored, and the replication offset the node adopts is the one that shipped *inside* that checkpoint. Errors name their phase (`recovery failed during OpenRocks: …`) rather than arriving as a bare cause chain. |
| NOT observable | An install attempted while RocksDB holds the directory open (it would be refused by the lock, or corrupt an open database). A replication offset restored *before* the install, which would pair the old offset with the new dataset — a replica silently resuming from a position its data does not match. Cluster storage opened early enough to matter: it is deliberately last and consumes nothing from the earlier phases. |
| Invariant | The order is the body of `recover`, readable top to bottom, with the two real dependencies stated in the phase modules: the install is filesystem rename surgery that must precede the open, and it is what carries `replication_metadata.json` into the data dir for phase 5 to find. Phases 1-4 are gated on RocksDB-backed persistence; phase 5 is role-gated and phase 6 cluster-gated, both independent of persistence. |
| Outcome variant | `RecoveryPhase::{InstallStagedCheckpoint, OpenRocks, RestoreShards, RestoreFunctions, RestoreReplicationState, OpenClusterStorage}` |
| Forced by | `staged_checkpoint_is_installed`, `staged_replication_metadata_is_adopted_and_consumed`, `restart_with_data_restores_keys`, `fresh_boot_creates_empty_shards`, `shard_count_mismatch_is_a_recovery_error` |
| Bug refs | none |

## FM-PERSISTENCE-028 — disabled (or fake) persistence touches no disk

| Field | Value |
|---|---|
| Trigger | `persistence.enabled = false`, or `persistence.mode = "fake"` (the in-process recording sink used by simulation tests). |
| Observable | Recovery returns `rocks: None` and exactly `num_shards` empty stores. The data directory is **not created**. A standalone node writes no replication state file. |
| NOT observable | A data directory, RocksDB lock, or state file materialising for a server the operator configured as in-memory — an empty directory tree left behind by a `--no-persistence` run is a support question waiting to happen. `fake` mode opening RocksDB: it is enabled-but-RocksDB-less and must take the fresh-stores path. |
| Invariant | `rocks_backed = persistence.enabled && mode != "fake"` gates phases 1-4 as a set, so there is one condition rather than four. Phase 5 is gated on the replication *role*, so a persistence-less replica still restores its identity. |
| Outcome variant | n/a |
| Forced by | `persistence_disabled_touches_nothing`, `standalone_does_not_persist_replication_state` |
| Bug refs | none |

## FM-PERSISTENCE-029 — an empty data directory boots fresh instead of failing

| Field | Value |
|---|---|
| Trigger | First boot; or a boot against a directory whose shard column families hold no keys. |
| Observable | `INFO` logs "No existing data found, starting fresh", recovery returns `num_shards` empty stores and zeroed `RecoveryStats`, and the server accepts writes. |
| NOT observable | A hard failure on first boot. A *partially* restored keyspace presented as fresh. |
| Invariant | `has_data()` is a first-key-exists probe across the shard column families, run *after* the open — so RocksDB has already replayed its WAL and any recovered rows are visible to the probe. There is no separate FrogDB-level WAL replay path to get this wrong. |
| Outcome variant | n/a |
| Forced by | `fresh_boot_creates_empty_shards`, `test_has_data`, `test_empty_database_recovery`, `test_empty_shard_recovery`, `test_recover_empty_shard`, `test_open_and_write`, `test_reopen` |
| Bug refs | `.scratch/hardening/issues/06` (a mistyped `data-dir` is indistinguishable from a first boot) |

## FM-PERSISTENCE-030 — a shard-count change refuses to start

| Field | Value |
|---|---|
| Trigger | Reopening a data directory with a different `num_shards` than the one that wrote it — in either direction. |
| Observable | Startup fails during `OpenRocks` with `RocksError::ShardCountMismatch` carrying the persisted count, the configured count, and the remediation ("restart with num_shards = N, or migrate the data directory"). |
| NOT observable | The server starting with keys misrouted under a new hash space — recovery is *positional* (shard `n`'s column family becomes store `n`; keys are never re-hashed), so a count change silently drops or misroutes every key. Warm-tier column families being counted as shards, which would make the check disagree with itself when tiering is on. |
| Invariant | `ColumnFamilyManifest::reconcile` compares the persisted CF layout against the configured one before any store is built; `count_persisted_shards` filters to `shard_<n>` names only. The orchestrator re-states the guarantee at the seam (FM-PERSISTENCE-041) so a future restore path that breaks it fails loudly instead of silently. |
| Outcome variant | `RecoveryPhase::OpenRocks` / `RocksError::ShardCountMismatch` |
| Forced by | `shard_count_mismatch_is_a_recovery_error`, `test_reopen_with_more_shards_fails`, `test_reopen_with_fewer_shards_fails`, `test_reopen_with_matching_shards_succeeds`, `test_shard_count_validation_ignores_warm_cfs`, `mismatched_shard_count_is_hard_error`, `count_persisted_shards_ignores_other_cfs`, `existing_matching_shards_includes_default_first`, `test_invalid_shard`, `test_warm_cf_invalid_shard` |
| Bug refs | none |

## FM-PERSISTENCE-031 — disabling the warm tier on a tiered database refuses to start

| Field | Value |
|---|---|
| Trigger | A data directory carrying warm-tier column families is reopened with `tiered-storage.enabled = false`. The reverse (enabling on a database without them) is allowed. |
| Observable | Startup fails during `OpenRocks` with `RocksError::WarmTierMismatch`. Enabling the tier for the first time succeeds and creates the warm column families. |
| NOT observable | Warm-tier keys becoming unreachable-but-present because the column families holding them were not opened — data that exists, is paid for, and cannot be read or evicted. |
| Invariant | Reconcile treats the persisted layout as authoritative for what *exists* and the config as authoritative for what to *create*: adding column families is safe, dropping them from the open set is not. A fresh directory has no persisted layout, so config wins outright. |
| Outcome variant | `RecoveryPhase::OpenRocks` / `RocksError::WarmTierMismatch` |
| Forced by | `test_warm_toggle_on_then_off_fails`, `test_warm_toggle_off_then_on_succeeds`, `persisted_warm_with_warm_off_is_hard_error`, `persisted_warm_with_warm_on_keeps_warm`, `no_persisted_warm_with_warm_on_creates_warm`, `fresh_dir_warm_on_includes_warm`, `fresh_dir_warm_off_stamps_config_layout`, `test_warm_cf_disabled`, `test_warm_cf_reopen` |
| Bug refs | none |

## FM-PERSISTENCE-032 — a failed column-family enumeration is never coerced into "fresh database"

| Field | Value |
|---|---|
| Trigger | `DB::list_cf` fails on an existing data directory — a transient IO error, a permissions problem, a damaged manifest. |
| Observable | The error propagates verbatim and startup fails. The on-disk data is untouched. |
| NOT observable | An empty CF list standing in for a failed enumeration. That is the dangerous shape: an empty list reads as "fresh database", which takes reconcile's early-return path and **skips both the shard-count and warm-tier invariants**, so a transient error would open a fresh database over a real one with every guard bypassed. |
| Invariant | The enumeration result is propagated, not `unwrap_or_default()`ed. "Fresh" is decided by a *successful* enumeration returning nothing, never by a failure. |
| Outcome variant | `RecoveryPhase::OpenRocks` / `RocksError::RocksDb` |
| Forced by | `test_cf_enumeration_failure_propagates_and_preserves_data` |
| Bug refs | none |

## FM-PERSISTENCE-033 — an undecodable key is skipped and counted, never fatal — and nothing checks the count

| Field | Value |
|---|---|
| Trigger | A stored value fails to deserialize during restore: a truncated payload, an invalid header, an unknown type byte from a future version. |
| Observable | The key is skipped, `stats.keys_failed` increments, a `WARN` names the key, and recovery continues with every other key intact. Ten good keys and one corrupt one recover as ten keys loaded, one failed. |
| NOT observable | One bad value aborting recovery and taking the whole keyspace down with it. Silent skipping with no counter. **Also not observable — and this is the gap:** any refusal, however extreme the count. `keys_failed` is summed, returned in `RecoveryStats`, and consulted by nobody, so a database whose every value fails to decode recovers "successfully" with an empty keyspace, a clean startup, and only WARN lines to say otherwise. |
| Invariant | Per-key `SerializationError` is caught inside `recover_shard_into` and converted to a counter; only a `RocksError` from the iteration itself propagates. The `Serialization` variant on the recovery error enum therefore exists but is never returned by the recovery path. |
| Outcome variant | `RecoveryStats::keys_failed` |
| Forced by | `test_partial_recovery_on_corruption`, `test_recover_with_data`, `test_recover_all_shards`, `test_recover_sorted_set`, `test_harness_crash_and_recover` |
| Bug refs | `.scratch/hardening/issues/06` |

## FM-PERSISTENCE-034 — WAL replay is point-in-time: a torn tail truncates, it does not refuse

| Field | Value |
|---|---|
| Trigger | An unclean shutdown leaves a partially written WAL record, or a bit-flip corrupts a record mid-log. |
| Observable | The database opens. Records up to the first inconsistency are replayed; everything from there on is discarded. A torn tail therefore costs only the tail; a mid-log corruption also costs the valid suffix behind it. Acknowledged-and-synced writes from before the tear all come back. |
| NOT observable | A refusal to open, which would turn an ordinary unclean shutdown into a hard startup failure. Records *after* a corruption being replayed anyway, which can resurrect stale values and reorder history. |
| Invariant | `DBRecoveryMode::PointInTime` is set explicitly at open — pinned by a test, not inherited from the library default, so a rocksdb upgrade cannot change FrogDB's crash semantics silently. This mirrors Redis' `aof-load-truncated` / point-in-time AOF loading: replay to the first inconsistency, then stop. `AbsoluteConsistency` and `SkipAnyCorruptedRecord` were both considered and rejected for the reasons above. |
| Outcome variant | n/a (see FM-PERSISTENCE-035 for the signal) |
| Forced by | `wal_truncation_recovers_prefix_and_signals`, `wal_mid_log_bitflip_drops_suffix_and_signals`, `wal_recovery_mode_is_pinned_to_point_in_time`, `test_unclean_shutdown_recovery`, `test_wal_recovery_despite_snapshot_issues`, `test_repeated_crash_cycles` |
| Bug refs | `.scratch/testing-improvements/issues/14-wal-recovery-mode-pin.md` (done — the pin test is its outcome) |

## FM-PERSISTENCE-035 — silently dropped WAL records raise a counter, and the detector never false-alarms

| Field | Value |
|---|---|
| Trigger | Point-in-time recovery discards records (FM-PERSISTENCE-034) that a durable sync had already covered. |
| Observable | `frogdb_wal_recovery_dropped_records_total` increases by the number of dropped sequence numbers and a `WARN` names the count. A clean reopen raises nothing. The watermark is then re-baselined, so the same loss is not re-reported on every subsequent boot. |
| NOT observable | A false alarm. The watermark is written best-effort (temp + rename, never fsynced) and can therefore only *lag* the true durable sequence, so the detector under-reports rather than over-reports: a torn or garbage watermark file parses as absent and reports nothing. Telling an operator they lost data when they did not is the worse failure, and this asymmetry is chosen deliberately. |
| Invariant | `frogdb_wal_watermark` holds the durable-sync high-watermark as a decimal `u64`; `detect_and_reset` compares it against `latest_sequence_number()` at open. It is advanced only by successful *sync* commits — never by a checkpoint, which does not truncate or reclaim the WAL. |
| Outcome variant | `frogdb_wal_recovery_dropped_records_total` |
| Forced by | `wal_clean_reopen_recovers_all_without_signal`, `test_wal_sequence_persistence`, `test_wal_sequence`, `test_async_wal_writer_recovery` |
| Bug refs | none |

## FM-PERSISTENCE-036 — an expired key never comes back from disk

| Field | Value |
|---|---|
| Trigger | Restart with keys whose `expires_at` has already passed, including one expiring exactly at recovery time. |
| Observable | Expired keys are not restored, `stats.keys_expired_skipped` counts them, and the rebuilt `ExpiryIndex` holds only the live ones. Expired warm-tier entries are additionally deleted from disk. |
| NOT observable | A key resurrected past its TTL because recovery bypassed the expiry check — a lazily-expired key is logically gone the moment its deadline passes, and a restart must not undo that. A live key dropped by an off-by-one at the boundary. An `ExpiryIndex` that disagrees with the restored store. |
| Invariant | The comparison uses a single `now` snapshotted once per pass, so a long recovery cannot classify two keys with the same deadline differently. The restore sink mirrors each surviving expiry into the standalone index the shard worker receives, in the same pass that populates the store. |
| Outcome variant | `RecoveryStats::keys_expired_skipped` |
| Forced by | `test_expiry_filtering_on_recovery`, `test_immediate_expiry_recovery`, `test_expiry_index_rebuilt`, `test_recover_skips_expired`, `test_recover_with_expiry`, `test_expiry_roundtrip` |
| Bug refs | none |

## FM-PERSISTENCE-037 — a corrupt function library does not block startup

| Field | Value |
|---|---|
| Trigger | `functions.fdb` is missing, empty, truncated, or garbage; or a library in it parses but fails to compile at registration time. |
| Observable | The server starts. Unreadable file → no functions and a `WARN`. Readable file with one bad library → the others still load, per-library, with a `WARN` naming the failed one. |
| NOT observable | A database refusing to start because of a stored script — the keyspace is the valuable thing and a function library is recoverable by re-uploading it. A partial parse silently registering a truncated library body. |
| Invariant | Phase 4 cannot fail: every read/parse error is downgraded to `Ok(Vec::new())`. Compilation is separate, happens in the wiring layer, and is isolated per library. The known cost is a silently smaller `FUNCTION LIST` with no counter or metric to notice it by. |
| Outcome variant | n/a (warn-only) |
| Forced by | `corrupt_functions_file_is_tolerated` |
| Bug refs | none |

## FM-PERSISTENCE-038 — a missing or corrupt replication state file is regenerated, forcing a full resync

| Field | Value |
|---|---|
| Trigger | A primary or replica boots with no replication state file, or with one that is unparseable or fails validation (the replication id must be 40 ASCII hex characters). |
| Observable | A fresh 40-hex replication id is minted, `offset_at_save` is 0, and the regenerated state is written back over the bad file. Offset 0 means the next sync is a full one (`PSYNC ? -1`). A standalone node does none of this and writes no file. |
| NOT observable | A crash or refusal on a corrupt state file. A garbage replid being trusted and offered to a peer, which is strictly worse than a full resync: it invites a *partial* resync from a position neither side can honour. Only a genuine non-`NotFound` IO error is fatal. |
| Invariant | The bad file is replaced rather than left in place, so the next boot is clean too. This matches Redis, where a mismatched replid forces a full sync rather than an error. |
| Outcome variant | `RecoveryPhase::RestoreReplicationState` (fatal only for a real IO error) |
| Forced by | `corrupt_replication_state_is_regenerated`, `primary_loads_and_persists_replication_state`, `standalone_does_not_persist_replication_state` |
| Bug refs | none |

## FM-PERSISTENCE-039 — staged full-sync metadata outranks the state file and is consumed only once it is durable

| Field | Value |
|---|---|
| Trigger | A boot that installs a staged full-sync checkpoint whose `replication_metadata.json` disagrees with the node's own persisted replication state — including the sub-case where writing the reconciled state back fails (a full disk, a permissions change, a transient IO error). |
| Observable | The staged replid and offset win unconditionally — no comparison, no `max()`, no "only if newer" — the reconciled state is written back, and the staging file is removed **after** that write succeeds, so a later boot uses the state file. Adoption is wholesale: the primary's history replaces this node's, dropping any secondary id / failover window. When the write back fails the boot still proceeds on the correct in-memory state, and the staging file is *left in place* so the next boot re-adopts it. |
| NOT observable | The node keeping its old offset for a dataset that was wholly replaced — the offset would name a position in a history the data no longer belongs to. Corrupt staged metadata being trusted: it is treated as absent, falling back to a full resync rather than crashing or adopting garbage. A failed save that still consumes the staging file, which would destroy the only durable copy of the post-full-sync offset and leave the next boot resuming from a *plausible but wrong* position (issue 08). |
| Invariant | Offset durability is coupled to snapshot durability by shipping the metadata *inside* the checkpoint (compare Redis' RDB aux fields), which is exactly why the install must precede this phase (FM-PERSISTENCE-027). Consumption is ordered after a successful `ReplicationState::save`: the staging file is the only durable carrier of the snapshot's offset until the state file holds it, so it is dropped only once that hand-off is complete. Re-adoption on a later boot is idempotent — the metadata describes a dataset that is already installed — so "consumed exactly once" is a liveness property (it does get consumed, on the first boot that can persist it), not a correctness one. |
| Outcome variant | `RecoveredState::{replication, installed_staged_checkpoint}` |
| Forced by | `staged_replication_metadata_is_adopted_and_consumed`, `staged_metadata_survives_a_failed_state_save` |
| Bug refs | `.scratch/hardening/issues/08` (fixed — the consume was unconditional while the save was warn-only); `.scratch/testing-improvements/issues/67` (fixed — the inverse case: a persistence-disabled primary handed over a replid and offset with no dataset behind them, so there was no staged metadata to reconcile and the replica kept a stale keyspace; that primary now ships its live dataset, see [FM-REPLICATION-001](replication-failure-modes.md)) |

## FM-PERSISTENCE-040 — cluster storage that will not open refuses to start

| Field | Value |
|---|---|
| Trigger | Cluster mode enabled and the Raft store at `<data_dir>/raft` cannot be opened — a file where the directory should be, a damaged store, a permissions failure. |
| Observable | Startup fails with `RecoveryError { phase: OpenClusterStorage }` and the process exits. A *missing* `raft/` directory is not a failure: it is created. |
| NOT observable | A degraded standalone fallback — a node that was configured as a cluster member coming up without its Raft log would serve a keyspace with no consensus behind it and could diverge from the rest of the cluster. A silent re-bootstrap over an unreadable log. |
| Invariant | The phase is gated on cluster mode, not on persistence: the Raft store is independent of the RocksDB data store and is opened last. Raft instance construction, log replay, and bootstrap stay outside the seam, so a corrupt Raft *log* (as opposed to an unopenable store) fails later, in the cluster wiring. |
| Outcome variant | `RecoveryPhase::OpenClusterStorage` |
| Forced by | `cluster_storage_open_failure_is_a_recovery_error`, `cluster_mode_opens_raft_storage` |
| Bug refs | none |

## FM-PERSISTENCE-041 — recovery returns exactly one store per configured shard

| Field | Value |
|---|---|
| Trigger | Any boot, on either path — restored from disk or freshly created. |
| Observable | `RecoveredState.shards` is exactly `num_shards` long, in shard order. A mismatch is a `RestoreShards` error, not a silent default. |
| NOT observable | A short vector being padded with empty stores, or a long one truncated — either would misroute or drop a whole shard's keyspace while the server came up looking healthy. |
| Invariant | The check is re-stated at the seam even though `RocksStore::open` already aborts on a layout mismatch and `spawn_shard_workers` guards the handoff: it makes the doc contract on `shards` hold by construction, so a future restore path that breaks it fails here rather than downstream. |
| Outcome variant | `RecoveryPhase::RestoreShards` |
| Forced by | `fresh_boot_creates_empty_shards`, `restart_with_data_restores_keys`, `test_multi_shard_recovery`, `test_recover_all_shards` |
| Bug refs | none |

---

## Redis deviations

Deliberate or known differences from Redis 8.x persistence semantics. Each is pinned by the tests
named above, so a change here is a visible spec edit rather than a silent drift.

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| FM-PERSISTENCE-005 | A WAL failure under the default `continue` policy acknowledges the write; the server keeps accepting writes indefinitely | The first failed background save flips `stop-writes-on-bgsave-error`, and every subsequent write is refused with `-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk` | FrogDB has no MISCONF equivalent at all. The only refusal is per-write `-IOERR` under the non-default `wal-failure-policy = rollback` (FM-PERSISTENCE-006), which covers failures the shard observes at write time — not a persistently broken disk. **Flagged as a spec gap, not a settled decision:** an operator migrating from Redis will expect writes to stop, and instead gets acknowledged writes that are not on disk. Tracked by `.scratch/hardening/issues/03`. |
| FM-PERSISTENCE-022 | `INFO persistence` reports `rdb_last_bgsave_status:ok` unconditionally | `rdb_last_bgsave_status:err` after a failed `BGSAVE` | A hardcoded literal with no failure state plumbed behind it. `LASTSAVE` *is* honest (it does not advance on failure), which makes the `INFO` field the only lying surface. Real bug, filed as issue 03; row states current behavior. |
| FM-PERSISTENCE-021 | A `BGSAVE` artifact is never loaded at boot; restoring it is an explicit operator install into a fresh data dir via `checkpoint_ready` | Redis loads `dump.rdb` at startup | FrogDB restarts recover from RocksDB + its WAL, which is strictly more current than the last snapshot. A snapshot is a *backup artifact*, not the boot path; auto-loading one would roll the keyspace back to the last cut. |
| FM-PERSISTENCE-015 | `BGSAVE` while a save is running replies with the `+` simple string `Background save already in progress` | `-ERR Background save already in progress` | A RESP client that treats only `-` replies as errors reads a rejected `BGSAVE` as success. Pinned as-is rather than fixed, because the wire shape is observable by raw-protocol clients; changing it is a separate, testable change (issue 45). |
| — | `SAVE`, `BGREWRITEAOF`, and `SYNC` are explicit `-ERR ... not supported` stubs | All three are supported | There is no AOF and no synchronous save path: durability is continuous WAL persistence, and `BGSAVE` is the only snapshot verb. The stub text points at `BGSAVE` rather than failing opaquely. |
| — | Snapshot cadence is a single `snapshot-interval-secs` timer, live-retunable, with no dirty-key threshold | `save <seconds> <changes>` save points, several of them, each a (time, change-count) pair | FrogDB's WAL already bounds durability loss (FM-PERSISTENCE-002/003/004), so the snapshot's job is periodic backup artifacts, not the durability mechanism Redis' save points implement. A tick during a running save is dropped, never queued. |
| FM-PERSISTENCE-034 | Point-in-time WAL replay: truncate at the first inconsistency and open | `aof-load-truncated yes` (default) truncates a torn AOF tail and starts | Matched deliberately. The divergence is what happens *behind* a mid-log corruption: FrogDB discards the valid suffix (RocksDB's point-in-time mode), where Redis with `aof-load-truncated no` would refuse to start. FrogDB has no equivalent strict knob. |
| FM-PERSISTENCE-023 | Checkpoint publication relies on rename atomicity; no directory fsync anywhere on the path | Redis fsyncs the RDB file and renames it into place | A real durability gap under power loss (as opposed to process crash), filed as issue 04. |
