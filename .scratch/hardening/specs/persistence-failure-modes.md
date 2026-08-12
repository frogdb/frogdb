# Persistence — failure modes

Status: LOCKED (2026-08-02) — Phase 2 mutation gate passed (frogdb-persistence 99.1%,
frogdb-recovery 100%, vs an 85% gate). Behavior changes to this area are spec-first: edit the
row, update the forcing test, then the code. See CLAUDE.md "Locked core areas".

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
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-PERSISTENCE-NNN` tag at its definition site; `just lint-spec` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in
`scripts/spec-lint.py` (`NEXTEST_CRATES`).

---

## FM-PERSISTENCE-001 — a shard's write batch is never torn across storage batches

| Field | Value |
|---|---|
| Trigger | One shard-side `persist` call carrying more than one WAL entry — a single-shard `MULTI`/`EXEC`, a multi-key `MSET`, a `RENAME` (write + delete) — while the WAL flush engine's own batch triggers (size threshold, batch timeout) are live, and a `BGSAVE` checkpoint or a crash lands in the middle of the sequence. Load makes it likely: a descheduled shard task leaves the flush thread with an empty channel and an expired timeout. |
| Observable | A restored checkpoint, or a crash-recovered keyspace, shows **all** of the batch's writes or **none** of them. For the transaction test: every hash-tag group is all-absent or all-present at a single generation. |
| NOT observable | A committed *prefix* — some of a transaction's keys at the new generation and the rest at the old one (the shape issue 65 caught: `[Some("470"), Some("458"), Some("458"), Some("458"), Some("458")]`). Equally: a group that never commits because its trigger stayed suppressed, or entries lost because the producer died mid-group. |
| Invariant | `persist_records` resolves the batch's `WalAction`s once and brackets them in `WalTarget::begin_group` / `end_group` whenever there is more than one, closing the group on every path including the staging-error path. A one-action batch skips the markers — a single WAL entry is already indivisible, and the hot path should not pay for a group it cannot need; the count comes from `WriteRecord::wal_actions` itself, so no second guess at how many entries a command produces can drift low. The markers travel the same FIFO channel as the entries, so grouping is decided by *enqueue* order, not by timing. `FlushEngine::should_flush` returns false while `group_depth > 0`, suppressing both the size and the timeout trigger, so the group commits as one `WriteBatch`. Two deliberate exceptions: an explicit `WalCommand::Flush` is honored mid-group (a durability request must not deadlock behind a group), and a channel disconnect drains and commits an unclosed group (the producer is gone; losing the writes would be strictly worse). |
| Outcome variant | n/a (storage-layer invariant; a failure surfaces as a torn restore, not as a reply) |
| Forced by | `test_sink_ungrouped_entries_tear_across_batches`, `test_sink_write_group_survives_the_batch_timeout`, `test_sink_write_group_survives_the_size_threshold`, `a_writer_group_suppresses_the_size_trigger_until_it_closes`, `the_drain_loop_stops_packing_exactly_at_the_size_threshold`, `an_oversized_entry_closes_its_batch_instead_of_pulling_the_queue_in`, `the_trait_objects_group_markers_reach_the_writer`, `persist_brackets_the_batch_in_one_write_group`, `single_action_persist_skips_the_write_group`, `write_group_closes_on_staging_failure`, `failed_group_open_skips_writes`, `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave` |
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
| Forced by | `test_sync_mode_zero_acked_write_loss`, `test_fsync_seam_sync_flag_matches_mode`, `test_sync_mode_crash_recovery`, `test_durability_mode_gates_crash_loss`, `confirm_snapshots_sequence_then_flushes_once` |
| Bug refs | `.scratch/testing-improvements/issues/12-durability-fsync-boundary.md` (done — these tests are its outcome) |

## FM-PERSISTENCE-003 — `periodic` durability bounds loss by the flush interval

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = periodic` (the default, `interval_ms = 1000`; Redis `appendfsync everysec`); a crash lands between two periodic syncs. |
| Observable | Writes acknowledged more than one interval before the crash are recovered. Writes acknowledged inside the current window may be absent — that is the mode's contract, not a defect. |
| NOT observable | Loss of a write older than the interval, i.e. an unbounded window: the periodic sync thread silently stopping would turn `periodic` into `async` while `INFO` still reports `durability_mode:periodic`. Also not observable: a *gap* — a recovered keyspace holding a later write while a strictly earlier one from the same shard is missing (the WAL commits in enqueue order, so loss is always a suffix). |
| Invariant | The commit seam never fsyncs in this mode (`is_sync` is false, so every `WriteSink::commit` passes `sync = false`); durability comes entirely from out of band. `spawn_periodic_sync` calls `RocksStore::durable_sync` every `interval_ms` — flush every shard's memtable, then publish the sequence that flush covered to each registered WAL writer's durable watermark and re-record the on-disk WAL watermark. Between ticks `FlushEngine::should_flush` may still *commit* on the size threshold, which shortens the window of buffered (not yet even committed) entries but does not make them durable. |
| Outcome variant | n/a (`INFO persistence` `durability_mode:periodic`, `wal_durability_lag_ms`) |
| Forced by | `test_periodic_mode_loss_bounded_by_flush_interval`, `test_periodic_mode_after_interval`, `test_periodic_mode_within_window` |
| Bug refs | `.scratch/hardening-2/issues/open/11-no-seam-to-stop-the-periodic-syncer.md` (open — the "syncer silently stopped" NOT-observable is unwitnessed: `spawn_periodic_sync` returns no handle, so no test can stop the syncer) |

## FM-PERSISTENCE-004 — `async` durability is unbounded until something forces a sync

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = async` (Redis `appendfsync no`); a crash with no explicit flush since the last one. |
| Observable | Only writes covered by an *out-of-band fsync* survive — an operator `sync_wal()`, a `RocksStore::durable_sync()`, or a RocksDB-internal memtable flush. Everything after the last such point is gone. |
| NOT observable | A silently *bounded* window that makes `async` behave like `periodic` — an operator who chose `async` must not be able to mistake page-cache residency for durability. Equally: a *reported* durable point that no fsync backs (FM-PERSISTENCE-043) — `flush_through` in this mode confirms that the batch reached RocksDB, not that it reached the device, and the durable sequence must not claim otherwise. |
| Invariant | `DurabilityMode::Async` commits with `sync = false` on **every** trigger — size threshold, batch timeout, an explicit `WalCommand::Flush` from `flush_through` / `flush_async`, and the shutdown drain alike — and spawns no periodic syncer. `WalCommand::Flush` empties the buffer into RocksDB; it does not fsync, because the mode's whole contract is that fsync is somebody else's event. Durability is therefore an event, not a clock, and the event is external to the WAL writer. |
| Outcome variant | n/a (`INFO persistence` `durability_mode:async`) |
| Forced by | `test_async_mode_unbounded_loss_without_fsync`, `test_async_mode_window_bounded_only_by_external_fsync`, `test_async_mode_explicit_flush`, `test_explicit_sync_wal`, `test_durability_mode_gates_crash_loss` |
| Bug refs | none |

## FM-PERSISTENCE-043 — the reported durable sequence counts only fsynced commits

| Field | Value |
|---|---|
| Trigger | `periodic` or `async` durability (FM-PERSISTENCE-003/004): the flush engine commits batches with `sync = false`, and something reads the WAL writer's durable sequence before any out-of-band fsync has landed. |
| Observable | `RocksWalWriter::durable_sequence` advances only for a commit that actually fsynced (`sync` mode), or when `RocksStore::durable_sync` publishes the sequence its flush covered. What merely reached RocksDB is reported separately as `WalLagStats::committed_sequence`, so the buffered-vs-committed gap an operator watches for stuck flushes stays visible. |
| NOT observable | A durable sequence above the last fsynced sequence — "handed to RocksDB" reported as "on the device", which is a durability claim the mode never made. Equally: a durable sequence above the *committed* sequence (an out-of-band sync claiming writes committed after its flush began); a `Durability::Confirm` that starts failing in `periodic`/`async` because it was re-pointed at a watermark those modes never advance; an empty-stage `flush()` moving either sequence. |
| Invariant | `FlushOutcomes` keeps two sequences instead of conflating them. `committed_seq` is stored by `record_success` after every successful commit and is what `confirm_durable_through` checks — `Durability::Confirm` means "the batch reached storage", and the durability mode alone decides whether storage means the device (FM-PERSISTENCE-002/003/004). `synced_seq` is `fetch_max`'d only when that commit passed `sync = true`, or by `publish_synced_through` from `RocksStore::durable_sync`, which snapshots each registered writer's `committed_seq` *before* calling `flush()` so it can never over-claim a write committed during the flush. Writers register a `Weak<dyn DurableSyncTarget>` with the store at construction, so the out-of-band syncer reaches every shard's watermark without the flush thread being on the path. `FlushEngine::flush` returns early on an empty stage without touching either sequence. |
| Outcome variant | n/a (storage-layer invariant; `WalLagStats`) |
| Forced by | `durable_sequence_tracks_only_fsynced_commits_in_periodic_mode`, `durable_sequence_tracks_only_fsynced_commits_in_async_mode`, `sync_mode_durable_sequence_equals_committed_sequence`, `empty_flush_advances_neither_sequence`, `durable_sync_publishes_the_flushed_sequence_to_every_registered_writer` |
| Bug refs | `.scratch/testing-improvements-round2/issues/done/71-durable-sequence-advances-on-non-synced-commits.md` (fixed) |

## FM-PERSISTENCE-005 — a WAL failure under the default `continue` policy acknowledges a lost write

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = continue` (**the default**) and the WAL sink fails — a full or read-only device, a RocksDB write error, a dead flush channel — while a write command is executing. |
| Observable | `+OK`. The key is in the in-memory keyspace and is served to readers, but it is not on disk and does not come back after a restart. The only signal is `INFO persistence` (`wal_last_flush_status:err`, `wal_flush_failures`, `wal_lost_ops`, `wal_pending_ops`), the `frogdb_wal_*` counters, and a rate-limited `ERROR` log (`FAILURE_LOG_INTERVAL`, one line per 100 failures, so a sustained outage does not drown the log). |
| NOT observable | An error reply (that is FM-PERSISTENCE-006's mode, and it needs the non-default policy). A shard that stops accepting commands, panics, or drops the remaining entries of the same batch — `stage_actions` under `FireAndForget` logs each failure and continues to the next action. Counters must not reset on a later success: `FlushOutcomes` accumulates `flush_failures`/`lost_ops`/`lost_bytes` for the life of the process, so an operator sampling `INFO` after recovery still sees that data was lost. |
| Invariant | `Durability::FireAndForget` never returns `Err`; the failure is recorded in `FlushOutcomes` and surfaced through `INFO`/metrics rather than the reply. This is a deliberate availability-over-durability default and the main deviation from Redis, which stops accepting writes entirely (`MISCONF`) once a background save fails — FrogDB offers that refusal as the opt-in [FM-PERSISTENCE-046](#fm-persistence-046--a-failed-save-refuses-client-writes-only-when-the-operator-asked-for-it), off by default; see [Redis deviations](#redis-deviations). |
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
| Forced by | `the_atomic_encoding_round_trips_and_distinguishes_the_policies`, `an_unknown_encoding_decodes_to_continue`, `config_strings_round_trip_and_only_rollback_selects_rollback`, `wal_failure_in_rollback_mode_replies_ioerr_and_restores_the_key`, `test_rollback_existing_key`, `test_rollback_missing_key`, `test_rollback_del_restores_key`, `test_rollback_rename`, `test_rollback_preserves_expiry`, `test_rollback_clears_added_expiry`, `test_rollback_mode_flag_toggle`, `test_snapshot_arc_efficiency` |
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
| Forced by | `test_sink_clear_is_a_flush_barrier_between_batches`, `test_sink_clear_advances_committed_sequence`, `test_wal_clear_reclamation_end_to_end`, `clear_reclamation_keeps_data_gone_after_reopen`, `clear_reclamation_preserves_post_clear_writes`, `clear_shard_routes_to_clear` |
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
| Invariant | `WalAction::MergeHllDelta` becomes a RocksDB merge operand folded by `full_value_merge` → `merge_hll_serialized`, which takes the per-register maximum. Register-max folding is idempotent and order-independent, so replay is safe by construction rather than by a de-duplication table. The same register-max fold is registered as the *partial* merge operator (`partial_value_merge` → `partial_merge_hll_deltas`), so operands flushed to an SST ahead of their base are pre-combined into one operand rather than accumulating unboundedly — and because the fold is the same one, pre-combining cannot change the answer. |
| Outcome variant | `frogdb_wal_merge_operands` |
| Forced by | `hll_merge_operand_folds_and_survives_reopen`, `hll_batch_merge_folds_operand`, `hll_operands_flushed_without_their_base_still_read_back_merged`, `test_sink_stages_merge_operand_in_order`, `merge_hll_delta_routes_to_merge` |
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
| Invariant | Everything is built under `.snapshot_NNNNN.tmp` and published by a single `rename` — the commit point. Every `?` in `SnapshotStager::run` drops a `TmpDirGuard` whose `Drop` removes the staging directory unless `commit()` was called, so cleanup covers *all* failure paths rather than the one somebody remembered. `metadata.json` is itself written `.tmp` + rename inside the staging dir, and `mark_complete` is the only writer of the completion marker; `load_latest_metadata` refuses to adopt an unmarked snapshot (it resumes the epoch counter but reports no last save). An *unreadable* `latest`/`metadata.json` (absent, zero-length, garbage) is not allowed to collapse the epoch to 0 either: the boot seed takes the max of the metadata's epoch and the highest `snapshot_NNNNN` directory on disk, and logs the parse failure, so a damaged pointer costs the last-save time but never makes the next `BGSAVE` reuse a live epoch or reap existing snapshots. |
| Outcome variant | `SnapshotError::{Io, Internal}`, `frogdb_persistence_errors{type="snapshot"}` |
| Forced by | `test_stager_happy_path`, `test_stager_checkpoint_failure_aborts_cleanly`, `test_stager_promote_rename_failure_leaves_no_leak`, `test_stager_reclaims_stale_tmp`, `test_stager_excludes_search_sidecar`, `test_metadata_file_new`, `test_metadata_file_complete`, `test_metadata_serialization`, `test_metadata_to_metadata`, `test_metadata_deserializes_legacy_num_keys_field`, `test_handle_is_bare_epoch_carrier`, `test_incomplete_snapshot_skipped`, `test_corrupted_snapshot_metadata`, `test_truncated_metadata_recovery`, `test_missing_completion_marker`, `coordinator_boot_keeps_the_epoch_when_latest_metadata_is_unreadable` |
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
| Invariant | `quiesce_shards_for_checkpoint` is a **drain, not a freeze**: two fan-out waves of ack-carrying messages (flush search indexes, then flush WAL) down the same FIFO channel the writes travelled. The `WalPersistence` effect enqueues a write's WAL entry before `ReplicationBroadcast` acknowledges it, so the drain message is behind every acknowledged write by construction — the argument is about *ordering*, not timing, which is why no lock is needed. Both waves send to all shards first and then await, so the cost is one flush latency, not `num_shards` of them. A shard that cannot complete a wave fails the cut rather than dropping out of it — [FM-PERSISTENCE-020](#fm-persistence-020--a-shard-that-cannot-drain-fails-the-checkpoint). |
| Outcome variant | n/a (storage-layer invariant) |
| Forced by | `test_checkpoint_preserves_single_shard_multi_atomicity_under_concurrent_bgsave`, `test_bgsave_snapshot_survives_restart`, `test_concurrent_bgsave_stress_restores_cleanly`, `test_ft_search_bgsave_flushes_search` |
| Bug refs | `.scratch/testing-improvements/issues/43-checkpoint-cross-shard-cut.md`; commit `ebdf7d9e` (the full-resync half of the same drain) |

## FM-PERSISTENCE-020 — a shard that cannot drain fails the checkpoint

| Field | Value |
|---|---|
| Trigger | One shard cannot complete a wave of the pre-checkpoint drain ([FM-PERSISTENCE-019](#fm-persistence-019--every-write-acknowledged-before-the-cut-is-in-the-cut)): its channel is closed (task ended or panicked) or the ack responder is dropped without answering. |
| Observable | No artifact is cut. `quiesce_shards_for_checkpoint` returns `QuiesceIncomplete` naming the wave (`search-index flush` / `WAL drain`), the failing shard indices, and the shard count, and logs it at `ERROR`. `BGSAVE`/periodic saves turn it into `SnapshotError::PreSnapshot`, which records a failure through the normal path — `rdb_last_bgsave_status:err`, `rdb_last_bgsave_error` carrying the cause, `rdb_bgsave_failures` incremented, `rdb_saves` and `LASTSAVE` unchanged, and the previous snapshot left as the newest on disk. `FULLRESYNC` fails the handshake before writing the checkpoint, so the replica retries on its reconnect backoff. |
| NOT observable | A checkpoint reported as successful, or shipped to a replica, while missing acknowledged writes still staged in a shard's flush engine. A *slow* shard failing the drain: there is no timeout, so a slow shard still blocks and is waited for. A partially-written checkpoint directory or a `sync_checkpoint_path` left set after the failed FULLRESYNC. |
| Invariant | `fan_out` collects every failure instead of returning at the first, so the error names the full blast radius; `send` errors and `rx.await` errors are treated identically (both mean the shard task is gone). Because there is no timeout, a failure can only mean "this shard is not running" — never "this shard is busy" — which is what makes failing the cut the right response rather than a liveness hazard. The failure is logged once, in `fan_out`, the only place that knows *which* shards; both callers only translate it into their own error type. No new metric: the BGSAVE path already flows into `frogdb_persistence_errors{type="snapshot"}` via `record_failure`. |
| Outcome variant | `BGSAVE`: save reported failed, previous snapshot retained. `FULLRESYNC`: handshake dropped with `pre-checkpoint drain failed for FULLRESYNC: …`, no checkpoint on disk. |
| Forced by | `quiesce_succeeds_when_every_shard_acks`, `quiesce_fails_when_a_shard_channel_is_closed`, `quiesce_fails_when_a_shard_drops_the_ack`, `quiesce_reports_every_undrained_shard`, `test_failed_pre_snapshot_hook_fails_the_save_and_cuts_nothing`, `fullresync_fails_when_the_pre_checkpoint_drain_fails` |
| Bug refs | `.scratch/hardening/issues/05` (fixed) |

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

## FM-PERSISTENCE-022 — a failed BGSAVE is reported as a failure, and a later success does not erase it

| Field | Value |
|---|---|
| Trigger | The stager fails (disk full, unwritable directory, IO error) after `BGSAVE` has already replied. |
| Observable | `INFO persistence` reports `rdb_last_bgsave_status:err` with the cause in `rdb_last_bgsave_error` (CR/LF folded to spaces so one field stays one line) and the attempt counted in `rdb_bgsave_failures`. `LASTSAVE` and `rdb_last_save_time` do **not** advance — a failed save never stamps `last_save_time` — and `rdb_saves` does not advance either. The next *successful* save flips the status back to `ok`, drops the error field, and advances `rdb_saves`, while `rdb_bgsave_failures` stays at its cumulative count so an operator sampling after a recovery still sees that saves were failing. `tracing::error!` and `frogdb_persistence_errors{type="snapshot"}` remain. The two duration fields are real: `rdb_last_bgsave_time_sec` is the last *successful* save's measured wall time (Redis' `-1` only until one completes in this process), and `rdb_current_bgsave_time_sec` is the in-flight save's elapsed time, live from the moment `BGSAVE` claims the scheduler slot until the outcome is recorded (`-1` when idle). |
| NOT observable | `rdb_last_bgsave_status:ok` while the last attempt failed — the field is derived from the coordinator's retained cause, not a literal. A failed save advancing `LASTSAVE`, `rdb_last_save_time`, or `rdb_saves`. A success zeroing `rdb_bgsave_failures`. A multi-line error message tearing the INFO section into fake fields. A failed attempt supplying a `rdb_last_bgsave_time_sec` (it answers "how long does a save take here"; a save that died on `mkdir` is no evidence about that) or leaving `rdb_current_bgsave_time_sec` stuck at a positive number, which would read as a save running forever. A running save reporting `-1` in the window between claiming the slot and the runtime polling its task — the start is stamped synchronously with the claim. A frozen `rdb_current_bgsave_time_sec`: it is computed at read time, so a hung save shows a rising number. Not observable *by default*: a write refused because saves are failing — that is the opt-in `snapshot.stop-writes-on-save-error`, [FM-PERSISTENCE-046](#fm-persistence-046--a-failed-save-refuses-client-writes-only-when-the-operator-asked-for-it). The shard-local INFO the scripting path renders (`commands/info.rs`) now agrees with the client-facing one for this row's fields — both call `persistence_snapshot_fields` off the same `SnapshotStats` — so `rdb_last_bgsave_status:ok` while a script's own shard just failed a save is no longer observable either (issue 10). |
| Invariant | One `SnapshotStats` value behind every surface: `SnapshotRun::record` calls `record_success` (stamps the artifact's own completion time, counts, clears the cause) or `record_failure` (counts, retains the cause, leaves the time alone), and `SnapshotCoordinator::stats` publishes it in a single lock acquisition — so `LASTSAVE`, the status, and the counters can never describe different moments. `PersistenceSection::render` derives `rdb_last_bgsave_status` from `last_bgsave_error.is_some()`, so "there is a retained cause" and "the status says err" are the same fact. Durations use a monotonic `Instant` (a wall-clock step cannot invent a negative or hour-long save) stamped by `record_start` inside `spawn_run` — the same instant `run_loop` measures against, so the number published as in-flight and the number recorded as final describe one window — and both duration fields are `Option`s that render as `-1`, so "absent" and Redis' sentinel are one decision at the edge rather than a magic number carried through the plumbing. |
| Outcome variant | `rdb_last_bgsave_status:err` / `frogdb_persistence_errors{type="snapshot"}` |
| Forced by | `bgsave_failure_is_visible_in_info_persistence`, `test_coordinator_records_failed_then_recovered_save`, `persistence_renders_the_real_bgsave_outcome`, `test_coordinator_reports_last_and_current_save_duration`, `test_failed_save_leaves_no_duration_and_nothing_in_flight`, `persistence_renders_save_durations_with_redis_sentinels`, `lastsave_tracks_real_bgsave_and_ignores_failed_saves`, `lastsave_returns_zero_when_never_saved`, `lastsave_returns_timestamp_after_save`, `bgsave_starts_each_time_under_instant_completion`, `test_lastsave_basic`, `test_bgsave_then_lastsave`, `test_bgsave_basic`, `script_info_persistence_reports_the_real_bgsave_outcome` |
| Bug refs | `.scratch/hardening/issues/03` (fixed), `.scratch/hardening/issues/09` (the MISCONF decision, settled as FM-PERSISTENCE-046), `.scratch/hardening/issues/10` (the scripting path's placeholder save-outcome INFO, fixed), `.scratch/hardening/issues/42` (the scripting path's `rdb_last_load_keys_*` fields, fixed — see [FM-PERSISTENCE-033](#fm-persistence-033--an-undecodable-key-is-skipped-counted-and-surfaced--never-fatal-on-its-own)) |

## FM-PERSISTENCE-046 — a failed save refuses client writes only when the operator asked for it

The opt-in half of [FM-PERSISTENCE-022](#fm-persistence-022--a-failed-bgsave-is-reported-as-a-failure-and-a-later-success-does-not-erase-it):
that row is what an operator can *see* after a failed save, this one is what the server *does*
about it.

| Field | Value |
|---|---|
| Trigger | `snapshot.stop-writes-on-save-error = true` (default `false`, live-settable as `stop-writes-on-save-error`) **and** the most recent background save failed — exactly the `rdb_last_bgsave_status:err` condition of FM-PERSISTENCE-022. |
| Observable | Every WRITE-flagged command from a client connection is refused at the `PreChecks` stage with `-MISCONF Errors writing snapshots. Refusing writes because snapshot.stop-writes-on-save-error is on. Acknowledged writes are still durable in the WAL; backups are not. Check rdb_last_bgsave_error in INFO persistence and disk space, then BGSAVE.` — one line, `MISCONF` prefix, counted as a *rejected* call like every other pre-dispatch refusal. A refusal inside an open `MULTI` poisons the transaction, so the later `EXEC` is `EXECABORT`. Reads are unaffected. The next successful save clears the condition and writes are admitted again with no restart and no `CONFIG SET`. With the flag off — the default — a failed save changes nothing about the write path. |
| NOT observable | A refusal with the flag off: FrogDB's durability is the WAL, so a failed snapshot means backups are stale, not that acknowledged writes are at risk, and turning that into a write outage is the operator's call to make. A refusal that outlives the failure (a latch that a successful save cannot clear), or one that survives `CONFIG SET stop-writes-on-save-error no`. Replicated writes refused: the replica apply path goes straight to the shard channels with `REPLICA_INTERNAL_CONN_ID` and never enters the gauntlet, so a primary that cannot snapshot still cannot stall its replicas' apply loops. Reads, `INFO`, `CONFIG`, and `BGSAVE` itself refused — the remediation must stay reachable from the same connection that just got the error. A per-write lock acquisition or `SnapshotStats` clone on the default-off path. |
| Invariant | The condition is `stop-writes-on-save-error` **and** `SnapshotCoordinator::last_save_failed()`, evaluated in that order by `ConfigManager::refuse_writes_on_save_error` — so the default-off path costs one relaxed atomic load and never reaches the coordinator. `last_save_failed()` is not a second copy of the truth: `SaveHistory` owns both the `SnapshotStats` lock and the `AtomicBool`, and the only two methods that can set the bool are the same two that write `last_error` (`record_success` clears both, `record_failure` sets both), which is what makes `last_save_failed() == stats().last_error.is_some()` structural rather than a convention. Placement mirrors Redis `processCommand`: after the read-only-replica check and before everything that could accept the write, gated on the same `CommandFlags::WRITE` predicate as the `CLUSTERDOWN` and `NOREPLICAS` gates beside it — and inheriting the same two documented bounds as those gates (a write issued from inside Lua, and a `MULTI` queued healthy then `EXEC`ed unhealthy, are not gated). |
| Outcome variant | `-MISCONF` (pre-dispatch rejection; `errorstat_MISCONF`) |
| Forced by | `failed_save_refuses_writes_when_stop_writes_on_save_error_is_on`, `snapshot_failure_does_not_refuse_writes_under_the_default`, `save_error_latch_mirrors_the_retained_cause`, `noop_coordinator_never_latches_a_save_error`, `refuse_writes_on_save_error_requires_both_the_flag_and_a_failed_save` |
| Bug refs | `.scratch/hardening/issues/done/09-no-misconf-write-refusal-on-failed-saves.md` (decided: opt-in, default off) |

## FM-PERSISTENCE-023 — the renames that publish a checkpoint are fsynced on both sides

| Field | Value |
|---|---|
| Trigger | Power loss (not a process crash) shortly after a "Snapshot completed" log line, or shortly after a staged checkpoint is installed over the live database. |
| Observable | A snapshot the coordinator reported complete is still there after the reboot, holding a `metadata.json` with the completion marker, and `latest` resolves to it. Every publishing rename is bracketed: what it publishes is fsynced *before* the rename, and the directory that gains the new name is fsynced *after* it. |
| NOT observable | `snapshot_NNNNN` present with an absent, zero-length or truncated `metadata.json`; `latest` pointing at a directory that is not there; a completed `BGSAVE` erased by a reboot while `LASTSAVE` and `rdb_last_save_time` still report it. On the install side: a backup rename that reached disk paired with an install rename that did not. Note the WAL watermark side-file remains deliberately *not* fsynced for the opposite reason (FM-PERSISTENCE-035): it may only lag, so an unsynced write can under-report but never false-alarm. |
| Invariant | The stager writes through a `SnapshotFs` seam (`sync_file`, `sync_dir`, `rename`, `symlink`, `write`) rather than calling `std::fs` directly, so the order of syncs and renames is a testable sequence and not a claim about syscalls. Order: `finalize_metadata` fsyncs `metadata.json.tmp` and then the staging directory, so the commit rename cannot become durable ahead of the metadata it publishes; `install()` fsyncs `snapshot_dir` after the promotion rename; `update_latest_symlink` fsyncs `snapshot_dir` again after the repoint. RocksDB's `create_checkpoint` already fsyncs the checkpoint's own file contents and directory, so the stager syncs only what it wrote itself. `load_staged_checkpoint` applies the same rule to the two `checkpoint_ready` install renames, fsyncing the parent of the live data dir after each. Directory durability is `File::open(dir)?.sync_all()`, the portable directory-entry barrier; a platform whose directories cannot be opened for sync (Windows) degrades to a no-op rather than failing a save. |
| Outcome variant | n/a (a failure to sync is an `SnapshotError::Io` on the save, not a silent success) |
| Forced by | `stager_fsyncs_metadata_and_staging_dir_before_install`, `stager_fsyncs_snapshot_dir_after_install_and_after_latest_repoint`, `staged_checkpoint_install_fsyncs_the_data_dir_parent` |
| Bug refs | `.scratch/hardening/issues/done/04-checkpoint-publish-renames-not-fsynced.md` (fixed), `.scratch/testing-improvements-round2/issues/done/74-snapshot-stager-fsyncs-nothing-before-promoting-latest.md` (the same hole, filed twice; fixed together) |

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
| Forced by | `test_load_staged_checkpoint_incomplete_dir_refuses_and_preserves_data`, `incomplete_staged_checkpoint_is_refused_without_touching_live_db`, `is_complete_db_requires_the_rocksdb_manifest_pointer` |
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

`frogdb-recovery::recover` is one synchronous seam over a gate (phase 0, the data-directory
verdict) and six ordered phases. Every `RecoveryError`
is fatal: there is a single `?` at the server's `init_infrastructure`, no phase-level catch, no
retry, and no degraded mode. What is *survivable* is therefore decided inside each phase, and the
rows below are that decision, phase by phase.

## FM-PERSISTENCE-027 — the recovery phase order is a data dependency, not a layout accident

| Field | Value |
|---|---|
| Trigger | Any boot. The order is: verify the data directory → install staged checkpoint → open RocksDB → restore shard stores → restore functions → restore replication state → open cluster storage. |
| Observable | A staged checkpoint's dataset is what gets opened and restored, and the replication offset the node adopts is the one that shipped *inside* that checkpoint. Errors name their phase (`recovery failed during OpenRocks: …`) rather than arriving as a bare cause chain. |
| NOT observable | An install attempted while RocksDB holds the directory open (it would be refused by the lock, or corrupt an open database). A replication offset restored *before* the install, which would pair the old offset with the new dataset — a replica silently resuming from a position its data does not match. Cluster storage opened early enough to matter: it is deliberately last and consumes nothing from the earlier phases. The data-directory verdict arriving after something has already written to the directory — phase 0 is a gate, not a step, and it precedes the install for the reason in [FM-PERSISTENCE-051](#fm-persistence-051--a-populated-data-directory-with-no-marker-refuses-the-boot). |
| Invariant | The order is the body of `recover`, readable top to bottom, with the three real dependencies stated in the phase modules: the marker verdict must precede everything that writes; the install is filesystem rename surgery that must precede the open; and the install is what carries `replication_metadata.json` into the data dir for phase 5 to find. Phase 0 is split around the install — decide before, stamp after — because the install renames the marker away with the directory it replaces ([FM-PERSISTENCE-049](#fm-persistence-049--the-data-directory-carries-an-identity-marker-published-atomically)). Phases 0-4 are gated on RocksDB-backed persistence; phase 5 is role-gated and phase 6 cluster-gated, both independent of persistence. |
| Outcome variant | `RecoveryPhase::{VerifyDataDir, InstallStagedCheckpoint, OpenRocks, RestoreShards, RestoreFunctions, RestoreReplicationState, OpenClusterStorage}` |
| Forced by | `staged_checkpoint_is_installed`, `staged_replication_metadata_is_adopted_and_consumed`, `restart_with_data_restores_keys`, `fresh_boot_creates_empty_shards`, `shard_count_mismatch_is_a_recovery_error`, `the_data_dir_guard_runs_before_a_staged_checkpoint_can_install` |
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
| Trigger | First boot; or a boot against a directory whose shard column families hold no keys. Reaching this point means the data directory has already been *identified* as FrogDB's — either it carries a marker or it was empty enough to initialize ([FM-PERSISTENCE-048](#fm-persistence-048--a-genuinely-empty-data-directory-is-a-first-boot-and-files-are-what-empty-means)). |
| Observable | `INFO` logs "No existing data found, starting fresh", recovery returns `num_shards` empty stores and zeroed `RecoveryStats`, and the server accepts writes. |
| NOT observable | A hard failure on first boot. A *partially* restored keyspace presented as fresh. This row standing in for "I am not looking where you think I am": that observation is now separated out and refused by [FM-PERSISTENCE-051](#fm-persistence-051--a-populated-data-directory-with-no-marker-refuses-the-boot) before this row's probe ever runs, so an empty keyspace here means an empty *FrogDB* keyspace. |
| Invariant | `has_data()` is a first-key-exists probe across the shard column families, run *after* the open — so RocksDB has already replayed its WAL and any recovered rows are visible to the probe. There is no separate FrogDB-level WAL replay path to get this wrong. What the probe cannot answer — whether this is the right directory at all — is answered earlier, by the marker, because a first-key probe has no way to tell an empty database from a foreign one. |
| Outcome variant | n/a |
| Forced by | `fresh_boot_creates_empty_shards`, `test_has_data`, `test_empty_database_recovery`, `test_empty_shard_recovery`, `test_recover_empty_shard`, `test_open_and_write`, `test_reopen` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` (fixed by the marker guard, FM-PERSISTENCE-048..052; split out of issue 06, whose decode-failure halves are [FM-PERSISTENCE-033](#fm-persistence-033--an-undecodable-key-is-skipped-counted-and-surfaced--never-fatal-on-its-own) / [FM-PERSISTENCE-045](#fm-persistence-045--a-wholly-undecodable-database-refuses-to-start)) |

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
| Forced by | `test_warm_toggle_on_then_off_fails`, `test_warm_toggle_off_then_on_succeeds`, `persisted_warm_with_warm_off_is_hard_error`, `persisted_warm_with_warm_on_keeps_warm`, `no_persisted_warm_with_warm_on_creates_warm`, `fresh_dir_warm_on_includes_warm`, `fresh_dir_warm_off_stamps_config_layout`, `test_warm_cf_disabled`, `test_warm_cf_reopen`, `warm_detection_needs_a_numeric_suffix_and_default_is_not_invented` |
| Bug refs | none |

## FM-PERSISTENCE-032 — a failed column-family enumeration is never coerced into "fresh database"

| Field | Value |
|---|---|
| Trigger | `DB::list_cf` fails on an existing data directory — a transient IO error, a permissions problem, a damaged manifest. |
| Observable | The error propagates verbatim and startup fails. The on-disk data is untouched. |
| NOT observable | An empty CF list standing in for a failed enumeration. That is the dangerous shape: an empty list reads as "fresh database", which takes reconcile's early-return path and **skips both the shard-count and warm-tier invariants**, so a transient error would open a fresh database over a real one with every guard bypassed. |
| Invariant | The enumeration result is propagated, not `unwrap_or_default()`ed. "Fresh" is decided by a *successful* enumeration returning nothing, never by a failure. The complementary hole — a directory that was never FrogDB's, and so has no persisted layout for reconcile to disagree with — is closed one phase earlier by [FM-PERSISTENCE-051](#fm-persistence-051--a-populated-data-directory-with-no-marker-refuses-the-boot). |
| Outcome variant | `RecoveryPhase::OpenRocks` / `RocksError::RocksDb` |
| Forced by | `test_cf_enumeration_failure_propagates_and_preserves_data` |
| Bug refs | none |

## FM-PERSISTENCE-033 — an undecodable key is skipped, counted, and surfaced — never fatal on its own

| Field | Value |
|---|---|
| Trigger | A stored value fails to deserialize during restore: a truncated payload, an invalid header, an unknown type byte from a future version. |
| Observable | The key is skipped, `stats.keys_failed` increments, a `WARN` names the key, and recovery continues with every other key intact. Ten good keys and one corrupt one recover as ten keys loaded, one failed. A boot that skipped anything additionally raises one `ERROR` naming the totals and the data dir, increments `frogdb_recovery_keys_failed_total` by the count, and reports it for the life of the process as `rdb_last_load_keys_failed` in `INFO persistence` — next to `rdb_last_load_keys_loaded` / `rdb_last_load_keys_expired`, which report the same boot's real numbers rather than Redis-shaped zeros. The shard-local INFO the scripting path renders (`commands/info.rs`) now agrees with the client-facing one for these three fields — both read the same node-wide `RecoveryStats`, `Arc`'d once per shard at construction — so a script's `redis.call('INFO', 'persistence')` reporting static zeros (and previously omitting `rdb_last_load_keys_failed` entirely) while the connection-level surface showed real counts is no longer observable (issue 42). |
| NOT observable | One bad value aborting recovery and taking the whole keyspace down with it *under the default* `recovery.on-decode-failure = continue` (the extreme case is [FM-PERSISTENCE-045](#fm-persistence-045--a-wholly-undecodable-database-refuses-to-start), which is about the *whole* database, not one key; the opt-in that does abort on one key is [FM-PERSISTENCE-047](#fm-persistence-047--recoveryon-decode-failure--refuse-turns-any-undecodable-key-into-a-refused-boot)). Silent skipping with no counter, or a counter that reaches nobody: a shrunken keyspace on its own cannot say whether keys were lost, so the count is the only positive signal. The load fields moving after startup — they describe the boot, so they are constants for the life of the process, like Redis'. |
| Invariant | Per-key `SerializationError` is caught inside `recover_shard_into` and converted to a counter; the warm scan (`recover_warm_shard_into`) folds its own decode failures into the *same* counter, so a tiered database reports one total rather than a hot-tier total that quietly ignores warm bit-rot. `stats.bytes_loaded` accumulates before the decode attempt, which is what distinguishes "scanned unreadable data" from "scanned nothing". Only a `RocksError` from the iteration itself propagates. The `Serialization` variant on the recovery error enum therefore exists but is never returned by the recovery path. The surfacing happens once, at the orchestrator seam (`frogdb_recovery::shards::restore`), against the aggregate — not per shard — so one boot produces one ERROR and one metric increment. `RecoveryStats` reaches INFO by being carried on `AdminDeps`, the same way the snapshot coordinator's stats do. |
| Outcome variant | `RecoveryStats::keys_failed` → `frogdb_recovery_keys_failed_total` + `rdb_last_load_keys_failed` |
| Forced by | `test_partial_recovery_on_corruption`, `test_recover_with_data`, `test_recover_all_shards`, `test_recover_sorted_set`, `test_harness_crash_and_recover`, `partial_decode_failure_is_counted_and_metered`, `expired_keys_count_as_decoded_so_one_bad_key_does_not_refuse`, `persistence_renders_the_real_load_stats`, `undecodable_values_are_counted_on_both_tiers`, `script_info_persistence_reports_the_real_load_stats` |
| Bug refs | `.scratch/hardening/issues/done/06-recovery-has-no-corruption-or-emptiness-threshold.md` (the surfacing half is fixed here; the policy half is [FM-PERSISTENCE-047](#fm-persistence-047--recoveryon-decode-failure--refuse-turns-any-undecodable-key-into-a-refused-boot), and the wrong-data-dir marker is issue 11) |

## FM-PERSISTENCE-045 — a wholly undecodable database refuses to start

| Field | Value |
|---|---|
| Trigger | Recovery finds data in the directory ([FM-PERSISTENCE-029](#fm-persistence-029--an-empty-data-directory-boots-fresh-instead-of-failing) is the no-data case) and **every** value in it fails to deserialize: a format change, a truncated or bit-rotted column family, a directory written by something that is not FrogDB. |
| Observable | Startup fails during `RestoreShards` with the data dir, the failed-key count, and the remediation in the message ("restore from a snapshot, or point `data-dir` at the right directory"). Nothing is written: the refusal happens before any shard worker, WAL, or snapshot cadence exists. |
| NOT observable | The catastrophic shape this replaces: a clean boot with an empty keyspace, `LOADING` cleared, writes accepted, and the WAL/snapshot cadence starting to overwrite the undecodable-but-present data with the new empty state — the operator's only signal being WARN lines that scrolled past. |
| Invariant | The predicate is *nothing decoded at all*: `keys_failed > 0` **and** `keys_loaded + keys_expired_skipped + warm_keys_loaded + warm_keys_stale == 0`. It needs no configuration because it has no judgement in it — a database that yielded not one decodable value is broken by any threshold anyone would pick. A single decoded key (including one that decoded and was then dropped as expired, and including a warm-tier key) takes the boot back to the skip-and-count path of FM-PERSISTENCE-033. All four terms are load-bearing and separately forced: a tiered deployment can have a hot CF this build decodes nothing from while its data sits warm (`warm_keys_loaded`), and a warm entry shadowed by a hot copy still *decoded* before being pruned as stale (`warm_keys_stale`). A partial-corruption *fraction* — "refuse above N%" — is still deliberately not implemented: it needs a force-boot override and a replica policy. What issue 06 settled instead is the binary [FM-PERSISTENCE-047](#fm-persistence-047--recoveryon-decode-failure--refuse-turns-any-undecodable-key-into-a-refused-boot); this row is the refusal that needs no configuration and therefore holds under *either* policy — under `refuse` the boot has already failed on the first key, with 047's message. |
| Outcome variant | `RecoveryPhase::RestoreShards` |
| Forced by | `wholly_undecodable_database_refuses_to_start`, `expired_keys_count_as_decoded_so_one_bad_key_does_not_refuse`, `warm_tier_only_decode_does_not_refuse_start`, `stale_warm_entry_counts_as_decoded` |
| Bug refs | `.scratch/hardening/issues/done/06-recovery-has-no-corruption-or-emptiness-threshold.md` (the unambiguous half of its candidate fix 1; the rest is FM-PERSISTENCE-047) |

## FM-PERSISTENCE-047 — `recovery.on-decode-failure = refuse` turns any undecodable key into a refused boot

The policy knob over [FM-PERSISTENCE-033](#fm-persistence-033--an-undecodable-key-is-skipped-counted-and-surfaced--never-fatal-on-its-own)
and [FM-PERSISTENCE-045](#fm-persistence-045--a-wholly-undecodable-database-refuses-to-start): 033
is what the default `continue` does with one bad key, 045 is the refusal `continue` makes anyway,
and this row is the operator's option to refuse on the first one.

| Field | Value |
|---|---|
| Trigger | `recovery.on-decode-failure = refuse` (default `continue`, boot-time only — there is no seam to change a policy that has already run) and **at least one** stored value fails to deserialize during restore, on either tier. |
| Observable | Startup fails during `RestoreShards` naming the policy that caused it, the failed-key count, the data dir, the *first* failing key's context — shard id, tier, the key (printable-lossy, truncated past 128 bytes with the real length reported), and the deserialization error — and the three remediations (restore from a snapshot, point `data-dir` at the right directory, or set the policy back to `continue` to boot without the undecodable keys). `frogdb_recovery_keys_failed_total` is incremented before the refusal, so the count is observable even though the process is about to exit. Nothing is written: the refusal happens before any shard worker, WAL, or snapshot cadence exists. |
| NOT observable | A refusal under the default. `continue` is the default because one bit-rotted value must not cost the whole keyspace, and because the unambiguous case — nothing at all decoded — already refuses without a knob (FM-PERSISTENCE-045). A refusal whose message names no key: "1 key failed to deserialize" with nothing to look at is not actionable, which is the whole reason the first failure's context is carried out of the format layer. A *percentage* threshold — the decision was binary on purpose: a fraction needs a force-boot override and an answer for what a replica does when it trips, and neither is settled. `first_failed_key` being overwritten by a later failure (it is first-wins across shards and tiers, in recovery order) or surviving into `INFO` (it is a boot-refusal diagnostic, not a live field). |
| Invariant | The policy is parsed once, in `RecoveryConfig::decode_failure_policy`, from the same `ON_DECODE_FAILURE_POLICIES` list `RecoveryConfig::validate` accepts — so a value that boots is a value the policy branch understands, and there is no second string comparison to drift. The branch lives in `frogdb_recovery::shards::report_decode_failures`, at the same aggregate seam as 033/045 and after the metric increment, so one boot produces one signal whichever policy is set. `refuse` is checked before the nothing-decoded predicate: both would refuse, and the one naming the operator's own setting is the more actionable message. The context itself is captured in the format layer (`recover_shard_into` / `recover_warm_shard_into`) at the only place the key, the tier, and the `SerializationError` are all in scope, stored first-wins so the cost is bounded at one key regardless of how many fail, and folded first-wins again in `recover_all_shards` where per-shard stats meet the total. |
| Outcome variant | `RecoveryPhase::RestoreShards` |
| Forced by | `refuse_policy_fails_the_boot_on_a_single_decode_failure`, `continue_policy_is_the_default_and_boots_past_a_decode_failure`, `refuse_policy_covers_warm_tier_decode_failures`, `decode_failure_context_is_first_wins`, `warm_decode_failure_context_records_the_warm_tier`, `the_first_decode_failure_is_captured_with_its_tier_and_key`, `recording_a_later_failure_does_not_even_build_it`, `a_failing_key_is_previewed_whole_up_to_the_limit_and_marked_when_cut` |
| Bug refs | `.scratch/hardening/issues/done/06-recovery-has-no-corruption-or-emptiness-threshold.md` (its remaining candidate-1 scope, decided binary rather than fractional; the wrong-data-dir marker of candidate 3 shipped separately as `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md`, FM-PERSISTENCE-048..052) |

## FM-PERSISTENCE-048 — a genuinely empty data directory is a first boot, and *files* are what "empty" means

| Field | Value |
|---|---|
| Trigger | Boot against a data directory with no marker that holds no regular files: one that does not exist yet, an empty one, a freshly formatted volume carrying only `lost+found`, or one whose subdirectories (the cluster storage path, a pre-created mount point) were made by orchestration before FrogDB ever ran. |
| Observable | The boot succeeds silently, initializes the database, and stamps the marker ([FM-PERSISTENCE-049](#fm-persistence-049--the-data-directory-carries-an-identity-marker-published-atomically)) so the next boot needs no override. |
| NOT observable | A refusal on a first boot — that would make [FM-PERSISTENCE-051](#fm-persistence-051--a-populated-data-directory-with-no-marker-refuses-the-boot) unshippable, because the shapes it must let through are the ones every container deployment produces. An entry *count* standing in for emptiness: a `lost+found` on an ext4 PVC and a pre-created `cluster/` are not evidence of a wrong directory, and refusing them would refuse the most common production first boot there is. A symlink being followed and its target's emptiness answering for it. |
| Invariant | `frogdb_persistence::data_dir::contains_files` walks the tree and answers on the first *file* at any depth; directories recurse, a missing directory contains nothing, and symlinks count as content without being resolved. `NotFound` is the only listing failure that answers "no files": every other one (a `data-dir` that is a file, a permissions error, an IO error) propagates, because a directory whose contents are unknown must not read as empty. It is a first-file-exists probe, so the cost is bounded by the answer rather than by the tree. The rule is deliberately about what *this process did not write* rather than an allowlist of tolerated names, which would need an entry per orchestrator. |
| Outcome variant | n/a (the `RecoveryPhase::VerifyDataDir` success path) |
| Forced by | `contains_files_counts_files_at_any_depth_and_ignores_empty_directories`, `contains_files_treats_a_symlink_as_content`, `contains_files_propagates_a_listing_failure_that_is_not_absence`, `a_fresh_data_dir_boots_and_stamps_the_marker`, `pre_created_empty_subdirectories_are_still_a_first_boot`, `fresh_boot_creates_empty_shards` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

## FM-PERSISTENCE-049 — the data directory carries an identity marker, published atomically

| Field | Value |
|---|---|
| Trigger | Any RocksDB-backed boot: the marker is (re)written after the staged-checkpoint install, in `RecoveryPhase::VerifyDataDir`'s second half. |
| Observable | `frogdb_data_dir` in the data directory, holding a generated 128-bit `database_id`, `created_at_unix_ms`, and `layout_version`. The id is stable across restarts **and** across a full resync that replaces the whole database: it names the directory, not its contents. An `INFO` line reports the id on every verified boot. |
| NOT observable | A marker that survives a crash mid-write as half a file (it is written to `frogdb_data_dir.tmp`, fsynced, renamed, and the directory fsynced — the same rule the checkpoint publishers follow, [FM-PERSISTENCE-023](#fm-persistence-023--the-renames-that-publish-a-checkpoint-are-fsynced-on-both-sides)). A `.tmp` left behind by a failed publish, which would itself be *content in an unmarked directory* and so refuse the next boot under FM-PERSISTENCE-051. A directory that comes out of a full resync unmarked: the install renames the live directory aside — marker and all — and moves in a staged RocksDB checkpoint that carries none, so the stamp is unconditional and runs *after* the install. Two directories sharing an id. |
| Invariant | `DataDirMarker::mint` is not a `Default`: every call yields a different value. `verify` decides which marker the directory ends up with (an existing one, or a freshly minted one) and `stamp` writes that decision, so the id an installed checkpoint inherits is the *directory's* prior id rather than a new one. The id is recorded for a future cross-check ("this node came up on a directory that is not the one it had last time"); nothing compares it across boots yet, because that needs out-of-band expected-id state this round did not add. |
| Outcome variant | `RecoveryPhase::VerifyDataDir` / `DataDirMarkerError::Write` |
| Forced by | `marker_round_trips_through_an_atomic_publish`, `minted_markers_are_unique_and_carry_the_current_layout`, `a_missing_marker_is_absent_not_an_error`, `a_failed_stamp_leaves_no_temporary_file`, `an_installed_checkpoint_leaves_the_data_dir_marked` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

## FM-PERSISTENCE-050 — an unreadable or future-layout marker refuses the boot

| Field | Value |
|---|---|
| Trigger | The marker file exists but cannot be turned into a `DataDirMarker`: an IO or permissions error, bytes that will not parse, or a `layout_version` newer than this build understands. |
| Observable | Startup fails during `VerifyDataDir` naming the resolved absolute path, the underlying cause, and `--force-fresh-data-dir`, which adopts the directory as it is and re-stamps the marker. A marker from an *older* layout reads normally — reading an older layout is the migration path. |
| NOT observable | "Unreadable" collapsing into "absent". That collapse is the fail-open bug in miniature: the absent branch is the one that initializes a fresh database, so a single corrupt byte in the marker would license overwriting a real database. A directory left permanently unbootable — the override re-stamps rather than requiring the flag forever. A future layout being opened under this build's rules and hoping. |
| Invariant | `DataDirMarker::read` returns `Ok(None)` for `ErrorKind::NotFound` and nothing else; every other outcome is a `DataDirMarkerError` variant, and the phase treats all of them as refusals. Unknown *fields* are deliberately tolerated (forward compatibility is `layout_version`'s job, not `deny_unknown_fields`'). Under `--force-fresh-data-dir` the read error is downgraded to a `WARN` and the marker re-minted. |
| Outcome variant | `RecoveryPhase::VerifyDataDir` / `DataDirMarkerError::{Unreadable, Malformed, FutureLayout}` |
| Forced by | `an_io_error_other_than_not_found_is_an_error_not_an_absent_marker`, `a_malformed_marker_is_an_error_not_an_absent_marker`, `a_marker_from_a_future_layout_is_refused`, `a_corrupt_marker_refuses_the_boot`, `force_fresh_data_dir_re_stamps_a_corrupt_marker` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

## FM-PERSISTENCE-051 — a populated data directory with no marker refuses the boot

| Field | Value |
|---|---|
| Trigger | Boot against a directory that holds files but carries no FrogDB marker: a mistyped `persistence.data-dir`, a container that lost its bind mount, a volume mounted somewhere else, a directory that belongs to another program — or a FrogDB database written before markers existed. |
| Observable | Startup fails during `VerifyDataDir` before anything is written, naming the *resolved absolute* path (a relative `data-dir` is resolved against a working directory the operator may not know — which is the confusion this row exists to end), the marker file it looked for, the three shapes this failure usually is, and `--force-fresh-data-dir` as the way to adopt the directory. Nothing is created, nothing is renamed, nothing is stamped. |
| NOT observable | The original bug: "nothing decoded here" and "I am not looking where you think I am" reading as the same observation, so a wrong `data-dir` came up as a healthy empty keyspace and the WAL/snapshot cadence began persisting over the real state ([FM-PERSISTENCE-029](#fm-persistence-029--an-empty-data-directory-boots-fresh-instead-of-failing) is the *genuinely* empty case; the column-family guards of [FM-PERSISTENCE-030](#fm-persistence-030--a-shard-count-change-refuses-to-start)/[031](#fm-persistence-031--disabling-the-warm-tier-on-a-tiered-database-refuses-to-start)/[032](#fm-persistence-032--a-failed-column-family-enumeration-is-never-coerced-into-fresh-database) cannot help, because they compare a *persisted layout* against the configured one and a directory that was never FrogDB's has no layout to disagree with). A *replica* refusing only after a full resync has already repopulated the directory — that is not a refusal, it is the operator's data replaced with the primary's. The override deleting anything: it adopts and re-stamps, and the existing data recovers normally. A persisted `force-fresh-data-dir = true` disabling the guard forever — the override is a CLI flag with no config-file or environment spelling. |
| Invariant | The phase runs *first* in `recover`, ahead of the staged-checkpoint install, ahead of the RocksDB open, and — because recovery completes before `init_replication` dials anyone — ahead of any resync. `force_fresh_data_dir` is `#[serde(skip)]` on `PersistenceConfig` and set only from `main.rs`, so `deny_unknown_fields` makes a config-file attempt a loud error rather than a silent, permanent disarm. |
| Outcome variant | `RecoveryPhase::VerifyDataDir` |
| Forced by | `an_unrelated_file_in_the_data_dir_refuses_the_boot`, `the_data_dir_guard_runs_before_a_staged_checkpoint_can_install`, `force_fresh_data_dir_adopts_an_unmarked_directory`, `force_fresh_data_dir_is_not_settable_from_the_config_file` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

## FM-PERSISTENCE-052 — `persistence.require-existing-data` turns an empty directory into a refusal

| Field | Value |
|---|---|
| Trigger | `persistence.require-existing-data = true` (default `false`) and a data directory with no marker and no files. |
| Observable | Startup fails during `VerifyDataDir` naming the resolved path, the setting, and `--force-fresh-data-dir` for the genuine first boot. With the setting off, the same directory is an ordinary first boot (FM-PERSISTENCE-048); with the setting on and the directory marked, the boot is ordinary too. |
| NOT observable | A default that refuses first boots. The knob exists because of an honest limit: an unmounted volume presents as an empty directory and is **indistinguishable from a first boot by inspection** — no marker can be read from a filesystem that is not there. Only the deployment knows it is past its first boot, so only the deployment can say so. A `require-existing-data` node that can never be provisioned — the override is the provisioning path, and stamping the marker retires it after one boot. |
| Invariant | The empty-directory refusal is checked after the marker read and after `contains_files`, so a marked directory satisfies it without consulting the setting at all; `--force-fresh-data-dir` short-circuits both this refusal and FM-PERSISTENCE-051's. The setting is a plain config param (settable from file and environment); the override is not, for the reason in FM-PERSISTENCE-051's invariant. |
| Outcome variant | `RecoveryPhase::VerifyDataDir` |
| Forced by | `require_existing_data_refuses_an_empty_data_dir`, `force_fresh_data_dir_overrides_require_existing_data` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

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
| Invariant | `frogdb_wal_watermark` holds the durable-sync high-watermark as a decimal `u64`; `detect_and_reset` compares it against `latest_sequence_number()` at open. It is advanced only after something durable — a successful *sync* commit, an out-of-band `durable_sync`, or the open-time (re-)baseline — never by a checkpoint, which does not truncate or reclaim the WAL. |
| Outcome variant | `frogdb_wal_recovery_dropped_records_total` |
| Forced by | `wal_clean_reopen_recovers_all_without_signal`, `test_wal_sequence_persistence`, `test_wal_sequence`, `test_async_wal_writer_recovery`, `the_watermark_file_round_trips_and_degrades_to_none`, `a_recovery_that_reaches_the_watermark_reports_nothing`, `a_short_recovery_reports_the_gap_once_and_re_baselines`, `only_a_synced_rocks_sink_commit_records_the_durable_watermark` |
| Bug refs | none |

## FM-PERSISTENCE-036 — an expired key never comes back from disk

| Field | Value |
|---|---|
| Trigger | Restart with keys whose `expires_at` has already passed, including one expiring exactly at recovery time. |
| Observable | Expired keys are not restored, `stats.keys_expired_skipped` counts them, and the rebuilt `ExpiryIndex` holds only the live ones. Expired warm-tier entries are additionally deleted from disk. |
| NOT observable | A key resurrected past its TTL because recovery bypassed the expiry check — a lazily-expired key is logically gone the moment its deadline passes, and a restart must not undo that. A live key dropped by an off-by-one at the boundary. An `ExpiryIndex` that disagrees with the restored store. |
| Invariant | The comparison uses a single `now` snapshotted once per pass, so a long recovery cannot classify two keys with the same deadline differently. On disk the absence of a deadline is the sentinel `-1`, not `0`: every non-negative stamp — the epoch included — decodes back to a real `expires_at`, so the boundary between "no TTL" and "a TTL long past" cannot blur. The restore sink mirrors each surviving expiry into the standalone index the shard worker receives, in the same pass that populates the store. |
| Outcome variant | `RecoveryStats::keys_expired_skipped` |
| Forced by | `test_expiry_filtering_on_recovery`, `test_immediate_expiry_recovery`, `test_expiry_index_rebuilt`, `test_recover_skips_expired`, `test_recover_with_expiry`, `test_expiry_roundtrip`, `the_epoch_decodes_as_a_passed_deadline_not_as_never_expires` |
| Bug refs | none |

## FM-PERSISTENCE-044 — a key past its deadline is never resurrected by a command that reads through the expiry window

The live-read counterpart of FM-PERSISTENCE-036: that row keeps a dead key from coming back across
a *restart*, this one keeps it from coming back while the process is still running.

| Field | Value |
|---|---|
| Trigger | Active expiry is sampled, so a key whose `expires_at` has already passed can still be physically present in the store. A command reaches it before the sweeper does — the ordinary `SET … EX` then `PERSIST` cache-pinning pattern is enough. |
| Observable | Every command reports the key as gone: `PERSIST` → `0`, `RENAME` → no-such-key error, `RENAMENX`/`EXISTS` → `0`, `TYPE` → `none`, `EXPIRETIME`/`PEXPIRETIME` → `-2`. A later expiry tick finds nothing left to do and `EXISTS` is still `0`. |
| NOT observable | A key made **permanently immortal** because `PERSIST` cleared `expires_at` and dropped the `ExpiryIndex` entry after the deadline had passed — the value would then outlive its TTL forever, and diverge from a replica that expired it independently. A value read back through `RENAME`/`RENAMENX`. An `ExpiryIndex` orphan (`by_time` without `by_key`, or the reverse) left behind by any of these paths. A non-destructive probe (`TYPE`, `EXISTS`) reporting a lazy purge, which would turn an observation into a mutation. |
| Invariant | `HashMapStore::persist` opens with `check_and_delete_expired`, the same guard its sibling `touch` uses, so the logically-dead key is deleted rather than pinned. Value-returning paths (`RENAME`, `RENAMENX`) go through `get_with_expiry_check`, matching `UNLINK`; non-mutating probes (`TYPE`, `EXISTS`, including the scatter-gather `EXISTS` branch) go through `exists_unexpired`, which answers correctly without purging. `EXPIRETIME`/`PEXPIRETIME` apply the same `expires_at <= now → -2` guard as `TTL`/`PTTL`. Every deletion runs through the store's own delete path, so `by_time` and `by_key` stay in step. |
| Outcome variant | n/a (per-command replies; `ExpiryIndex` consistency) |
| Forced by | `persist_on_expired_key_deletes_instead_of_immortalizing`, `persist_on_expired_key_leaves_no_expiry_index_orphan`, `nondestructive_probes_do_not_see_a_past_deadline_key` |
| Bug refs | `.scratch/testing-improvements-round2/issues/done/57-persist-past-deadline-key-permanently-immortal.md` (fixed) |

## FM-PERSISTENCE-037 — a corrupt function library does not block startup

| Field | Value |
|---|---|
| Trigger | `functions.fdb` is missing, empty, truncated, or garbage; or a library in it parses but fails to compile at registration time. |
| Observable | The server starts. Unreadable file → no functions and a `WARN`. Readable file with one bad library → the others still load, per-library, with a `WARN` naming the failed one. |
| NOT observable | A database refusing to start because of a stored script — the keyspace is the valuable thing and a function library is recoverable by re-uploading it. A partial parse silently registering a truncated library body. And the mirror image of the tolerance: a *well-formed* `functions.fdb` coming back empty, which would make a valid library disappear exactly as silently as a corrupt one. |
| Invariant | Phase 4 cannot fail: every read/parse error is downgraded to `Ok(Vec::new())`. Compilation is separate, happens in the wiring layer, and is isolated per library. The downgrade is on the *error* arm only — a successful read returns the stored `(name, source)` pairs verbatim for the wiring layer to register. The known cost is a silently smaller `FUNCTION LIST` with no counter or metric to notice it by. |
| Outcome variant | n/a (warn-only) |
| Forced by | `corrupt_functions_file_is_tolerated`, `persisted_functions_are_restored` |
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

## FM-PERSISTENCE-042 — the reported save time is the snapshot's own, across restarts

| Field | Value |
|---|---|
| Trigger | A restart with snapshot artifacts already in `snapshot_dir` — the normal case for any server that has ever run `BGSAVE`. The newest artifact may be seconds or months old. |
| Observable | `LASTSAVE` and `rdb_last_save_time` report the newest **complete** snapshot's own `completed_at_ms`, read out of its `metadata.json` at boot, so a month-old artifact reports as a month old. `rdb_saves` stays 0 after such a boot: a restart is not a save. With no snapshot at all — or one that is incomplete, or complete but carrying no completion timestamp — the time is absent and `LASTSAVE` answers `0` ("never saved"). A save completed by *this* process reports the same value it will report after the next restart, because both read the same field. The *duration* fields do not survive the restart: `rdb_last_bgsave_time_sec` is back to `-1` after a boot even though `rdb_last_save_time` is set, because the artifact records when it finished, not how long it took (see FM-PERSISTENCE-022). |
| NOT observable | The boot time standing in for a save that happened before the process started (the original bug: `last_save = Instant::now()` whenever any checkpoint metadata existed, which made a stale snapshot look fresh to every monitor and every operator). An unknown completion time being reported as "now". An incomplete snapshot stamping a time at all. `LASTSAVE` and `rdb_last_save_time` disagreeing — one derives from the other. A save *duration* reconstructed across the restart from the artifact's timestamps and reported as if this process had measured it. |
| Invariant | `SnapshotStats::last_save_time` is a wall-clock `SystemTime`, not an `Instant`: the value it must carry may predate this process, which a process-relative monotonic clock cannot represent. The truth source is `SnapshotMetadataFile::completed_at()` (`completed_at_ms`, written by `mark_complete` at finalize time) rather than the checkpoint directory's mtime — it lives *inside* the durable artifact, survives a copy that rewrites mtimes, and is the same field the completing process stamps into `SnapshotStats`, so "reported now" and "reported after a restart" are the same bytes. `RocksSnapshotCoordinator::load_latest_metadata` already discards a snapshot with no completion marker, and the boot seed maps a missing `completed_at_ms` to `None`; `SnapshotCoordinator::last_save_time` is a provided method over `stats()`, so no surface can read a second copy. |
| Outcome variant | n/a (`LASTSAVE` integer reply / `rdb_last_save_time`) |
| Forced by | `lastsave_reports_the_snapshot_time_after_restart`, `test_coordinator_seeds_last_save_from_snapshot_metadata`, `test_coordinator_seeds_none_without_a_usable_completion_time` |
| Bug refs | `.scratch/hardening/issues/07` (fixed) |

---

## Redis deviations

Deliberate or known differences from Redis 8.x persistence semantics. Each is pinned by the tests
named above, so a change here is a visible spec edit rather than a silent drift.

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| FM-PERSISTENCE-005 | A WAL failure under the default `continue` policy acknowledges the write; the server keeps accepting writes indefinitely | The first failed background save flips `stop-writes-on-bgsave-error`, and every subsequent write is refused with `-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk` | Unchanged for the *WAL* failure this row is about: there is no snapshot involved, so no snapshot policy can gate it, and the only refusal remains per-write `-IOERR` under the non-default `wal-failure-policy = rollback` (FM-PERSISTENCE-006). The adjacent gap — a persistently broken disk observed by the snapshot task — is now closed as an opt-in (FM-PERSISTENCE-046). |
| FM-PERSISTENCE-022 / FM-PERSISTENCE-046 | `INFO persistence` reports the real save outcome (`rdb_last_bgsave_status`, `rdb_last_bgsave_error`, `rdb_saves`, `rdb_bgsave_failures`); a failing save refuses writes with `-MISCONF` only under `snapshot.stop-writes-on-save-error`, **default off** | `rdb_last_bgsave_status:err` *and*, with the default `stop-writes-on-bgsave-error yes`, every subsequent write refused with `-MISCONF` | The status field matched Redis in phase 2c (issue 03); the refusal itself matched in this round, but the *default* is deliberately inverted (issue 09). In Redis the RDB/AOF is the durability mechanism, so a failed save means writes are not durable and refusing is conservative. In FrogDB the WAL is the durability mechanism (FM-PERSISTENCE-002/003/004) and a snapshot is a backup artifact (FM-PERSISTENCE-021), so a failed save means backups are stale — refusing writes for that is a bigger hammer than it is in Redis, and is offered as a parity checkbox rather than imposed. `rdb_last_bgsave_error` and `rdb_bgsave_failures` are FrogDB additions — Redis exposes only the status, which tells an operator that saves broke but not why. |
| FM-PERSISTENCE-047 | `recovery.on-decode-failure` (`continue` default / `refuse`) decides whether a single undecodable value refuses the boot | No equivalent: a value Redis cannot load is fatal — a corrupt RDB aborts the load outright, and `aof-load-truncated no` is the closest knob, which is about a torn *tail* rather than about individual values | FrogDB decodes per key out of RocksDB rather than replaying one file, so "skip the bad value and keep the other ten million" is a real option Redis does not have; `continue` takes it. `refuse` exists for operators who would rather not serve a silently smaller keyspace. Binary on purpose — a percentage threshold needs a force-boot override and a replica policy, neither of which is settled. |
| FM-PERSISTENCE-048..052 | The data directory carries an identity marker; one holding files FrogDB did not write refuses to boot, and `persistence.require-existing-data` can extend the refusal to an empty directory | Redis starts against any `dir`: it loads `dump.rdb`/the AOF if they are there and otherwise comes up with an empty keyspace, so a mistyped `dir`, a failed mount, or a lost bind mount is a silent fresh start that then begins saving over nothing | A FrogDB data directory is a live RocksDB the server writes to continuously, not a file it loads once at boot — the window in which a wrong directory quietly becomes the real one is every second the process runs, rather than the moment of a save. The marker is the evidence Redis' layout does not need and FrogDB's does. The empty-directory case stays a silent success by default because container orchestration pre-creates mount points; `require-existing-data` exists because an unmounted volume is indistinguishable from a first boot by inspection, and only the deployment knows which it is. |
| FM-PERSISTENCE-022 | The shard-local `INFO persistence` the *scripting* path renders (`redis.call('INFO')`) still emits static `rdb_last_load_keys_expired`/`rdb_last_load_keys_loaded` placeholders (recovery is aggregated node-wide, not per-shard) | One INFO implementation for every caller | Fixed for the save-outcome block in issue 10: both renderers now read `persistence_snapshot_fields(&SnapshotStats, bgsave_in_progress)` off the same `SnapshotCoordinator` every shard shares, so `rdb_last_bgsave_status`/`rdb_saves`/etc can no longer disagree. The two load-count fields remain a narrower, known gap — the script path runs inside a shard worker with no per-shard `RecoveryStats` handle — tracked by issue 42. |
| FM-PERSISTENCE-021 | A `BGSAVE` artifact is never loaded at boot; restoring it is an explicit operator install into a fresh data dir via `checkpoint_ready` | Redis loads `dump.rdb` at startup | FrogDB restarts recover from RocksDB + its WAL, which is strictly more current than the last snapshot. A snapshot is a *backup artifact*, not the boot path; auto-loading one would roll the keyspace back to the last cut. |
| FM-PERSISTENCE-015 | `BGSAVE` while a save is running replies with the `+` simple string `Background save already in progress` | `-ERR Background save already in progress` | A RESP client that treats only `-` replies as errors reads a rejected `BGSAVE` as success. Pinned as-is rather than fixed, because the wire shape is observable by raw-protocol clients; changing it is a separate, testable change (issue 45). |
| — | `SAVE`, `BGREWRITEAOF`, and `SYNC` are explicit `-ERR ... not supported` stubs | All three are supported | There is no AOF and no synchronous save path: durability is continuous WAL persistence, and `BGSAVE` is the only snapshot verb. The stub text points at `BGSAVE` rather than failing opaquely. |
| — | Snapshot cadence is a single `snapshot-interval-secs` timer, live-retunable, with no dirty-key threshold | `save <seconds> <changes>` save points, several of them, each a (time, change-count) pair | FrogDB's WAL already bounds durability loss (FM-PERSISTENCE-002/003/004), so the snapshot's job is periodic backup artifacts, not the durability mechanism Redis' save points implement. A tick during a running save is dropped, never queued. |
| FM-PERSISTENCE-034 | Point-in-time WAL replay: truncate at the first inconsistency and open | `aof-load-truncated yes` (default) truncates a torn AOF tail and starts | Matched deliberately. The divergence is what happens *behind* a mid-log corruption: FrogDB discards the valid suffix (RocksDB's point-in-time mode), where Redis with `aof-load-truncated no` would refuse to start. FrogDB has no equivalent strict knob. |
| FM-PERSISTENCE-023 | Checkpoint publication fsyncs the staged payload before each publishing rename and the containing directory after it | Redis fsyncs the RDB file and renames it into place | Matched deliberately, and extended: Redis has one file to sync, FrogDB publishes a directory tree, so the directory entry — not just the payload — is the thing that must survive. Was a real power-loss gap (issue 04 / issue 74) until phase 2c. |
