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
`frogdb-server/crates/server/src/server/checkpoint_quiesce.rs`. Rows stop at what a *reader of storage* can
observe — a restored checkpoint or a crash-recovery replay. Client-visible transaction semantics
live in [Transactions — failure modes](txn.md).

## State space

The variables the transitions below read and write. "Survives restart" means the *value* outlives
a process restart, not just that some field with the name exists after boot — an in-memory counter
that gets recomputed from scratch on every boot is marked No even though a same-named field exists
post-boot.

| Variable | Authoritative field | Writer(s) | Persistence | Survives restart |
| --- | --- | --- | --- | --- |
| `wal_seq` | `RocksWalWriter.sequence` (`AtomicU64`) | `RocksWalWriter::new` seeds it to 0 (`frogdb-persistence/src/wal/writer.rs:84`); `write_set` and its variants `fetch_add` it (`writer.rs:113,144,173,188`) | In-memory only | No — a per-shard, per-process counter seeded at 0 on every `RocksWalWriter::new` and never reseeded from RocksDB; distinct from `rocks_seq` below (two different counters that both get called "sequence" in code, never conflate them) |
| `rocks_seq` | RocksDB's own sequence number (`DB::latest_sequence_number`) | RocksDB itself, on every write; read via `RocksStore::latest_sequence_number` (`frogdb-persistence/src/rocks/checkpoint.rs:20-22`) | On disk (RocksDB's own MANIFEST) | Yes — this, not `wal_seq`, is what a checkpoint's `snapshot_metadata.sequence_number` records (`stage_checkpoint`, `snapshot/stager.rs:151`) and what `wal_watermark` persists (`checkpoint.rs:29-31`) |
| `committed_seq` | `FlushOutcomes.committed_seq` (`AtomicU64`) | `FlushOutcomes::record_success` (`frogdb-persistence/src/wal/flush.rs:271-280`) — a plain `store`, not `fetch_max`: the caller already holds the batch's max sequence, so there is one writer per commit and nothing to race | In-memory only | No |
| `synced_seq` | `FlushOutcomes.synced_seq` (`AtomicU64`) | `record_success` (`flush.rs:277`, `fetch_max`, only when the commit passed `synced = true`) and `publish_synced_through` (`flush.rs:375-377`, `fetch_max`), the latter driven by `RocksStore::durable_sync`'s out-of-band tick (`rocks/mod.rs:328-347`) | In-memory only | No |
| `highest_failed_seq` | `FlushOutcomes.highest_failed_seq` (`AtomicU64`) | `FlushOutcomes::record_failure` (`flush.rs:282-290`, `fetch_max`) | In-memory only | No |
| `flush_failures` / `lost_ops` / `lost_bytes` / `last_error` | `FlushOutcomes`' remaining fields (`flush.rs:244-250`) | `record_failure` (`flush.rs:282-290`, `fetch_add` on the counters, the message replaces `last_error` under a `Mutex`) | In-memory only | No — reset with the process; surfaced live via `WalLagStats` (`writer.rs:283-300`) |
| `last_flush_ok` | `FlushOutcomes.last_flush_ok` (`AtomicBool`, `frogdb-persistence/src/wal/flush.rs:248`) | both `record_success` (`frogdb-persistence/src/wal/flush.rs:279`, sets `true`) and `record_failure` (`:288`, sets `false`) — the only field either branch shares | In-memory only | No — surfaced as the `frogdb_wal_last_flush_ok` gauge (`WalLastFlushOk`, `frogdb-server/crates/types/src/metrics/definitions.rs:209`) via `WalLagStats` (`frogdb-persistence/src/wal/writer.rs:296`) → `frogdb-core/src/shard/diagnostics.rs:454-456` → aggregated (AND across shards) in `frogdb-core/src/observability/wal.rs:114` |
| `poisoned` / `poisoned_seq` | `FlushOutcomes.poisoned_seq` (`AtomicU64`, `0` = healthy) alongside `highest_failed_seq`, with `poison_was_sync: AtomicBool` recording which kind of failure latched it | `FlushOutcomes::record_failure` latches it once, at the **first** failed sequence (`compare_exchange` from `0`, not `fetch_max` — the latch names where the persisted stream was truncated, so it must be the earliest loss); cleared only by `clear_poison` | In-memory only | No — `clear_poison` (the operator reset, reached through `WalSink::clear_poison`) and process restart are its only clears; a later successful commit never clears it ([FM-PERSISTENCE-053](#fm-persistence-053--a-durability-failure-poisons-the-shard-until-an-operator-clears-it)) |
| `group_depth` | per-shard write-group nesting counter | `WalTarget::begin_group`/`end_group` (`frogdb-core/src/shard/persistence.rs:87,89,199,206`); the counter itself lives in `FlushEngine` (`flush.rs:400,467,474`) | In-memory only | No |
| `wal_failure_policy` | shared `Arc<AtomicU8>` decoded via `WalFailurePolicy::from_u8` (`frogdb-persistence/src/wal/config.rs`): `Continue` (0, default), `Rollback` (1), `Readonly` (2, [FM-PERSISTENCE-055](#fm-persistence-055--wal-failure-policy--readonly-fail-stops-the-shard)) | `CONFIG SET wal-failure-policy` / shard construction (`frogdb-core/src/shard/types.rs:420-431`) | Config only, not written as data | N/A — reloaded from config at boot |
| `durability_mode` | `WalConfig.mode: DurabilityMode` (`Async` / `Periodic{interval_ms}` / `Sync`, `wal/config.rs:62-67`), mirrored shard-side as `ShardPersistence.sync_durability: bool` (`frogdb-core/src/shard/types.rs`) — the only distinction the ack path draws is `Sync` vs not (FM-PERSISTENCE-002), so the mirror is a bool, not a second copy of the mode | server config (`persistence.durability-mode`), `frogdb-persistence/src/wal/config.rs`; the shard-side mirror is set once by `ShardWorkerBuilder` from the `WalConfig` (or `with_durability_mode` in tests) | Config only | N/A — boot-time; unlike `wal_failure_policy` this is *not* live-retunable (`CONFIG SET durability-mode` updates the reported value and nothing else, `Propagation::None`), so the mirror needs no shared atomic |
| `wal_watermark` | decimal `u64` in the `frogdb_wal_watermark` side-file (`rocks/wal_watermark.rs:43-52`) | `RocksStore::record_wal_watermark`, called from two independent sites (TR-PERSISTENCE-010): the `Periodic`-mode out-of-band `durable_sync` tick (`rocks/mod.rs:345`) and a `Sync`-mode commit's direct in-line call from the flush thread (`wal/flush.rs:206-208`); also on fresh open (`rocks/mod.rs:295`), and re-baselined by `wal_watermark::detect_and_reset` on an existing open (`rocks/mod.rs:294`, `wal_watermark.rs:87-126`) | On disk, best-effort (never itself fsynced — see `wal_watermark.rs:66-68`) | Yes, but may lag the true durable point — by construction it never leads it |
| `wal_dropped_records_total` | `WalRecoveryDroppedRecords` counter (`frogdb_wal_recovery_dropped_records_total`) | `wal_watermark::detect_and_reset`, when the recovered sequence lands below the watermark (`wal_watermark.rs:92-107`) | Process metric only | No — resets with the process |
| `WalRollbacks` | `frogdb_wal_rollbacks_total` counter (`frogdb_types::metrics::definitions::WalRollbacks`) | `WalRollbacks::inc`, on both the single-command and transaction-batch rollback paths (`frogdb-core/src/shard/execution.rs:489,664`) | Process metric only | No |
| `snapshot_epoch` | `SnapshotScheduler.epoch` (`AtomicU64`) | `SnapshotScheduler::with_epoch`/`try_begin` (`frogdb-persistence/src/snapshot/scheduler.rs:17,34,57`) | In-memory only | No — reseeded at construction to `max(latest_metadata.epoch, highest_snapshot_epoch_on_disk)` (`RocksSnapshotCoordinator::new`) |
| `snapshot_in_progress` / `snapshot_scheduled` | `SnapshotScheduler`'s two `AtomicBool`s | `try_begin` / `finish_and_maybe_rebegin` (`scheduler.rs:15,16,57,73`) | In-memory only | No |
| `snapshot_metadata` | `SnapshotMetadataFile` — `epoch`, `sequence_number`, `started_at_ms`, `completed_at_ms`, `num_shards`, `size_bytes`, a `FROGDB_SNAPSHOT_COMPLETE_v1` completion marker | snapshot stager's `finalize_metadata`, `mark_complete` (`frogdb-persistence/src/snapshot/metadata.rs:52`; `snapshot/stager.rs:170-185`) | On disk, one `metadata.json` per snapshot directory, fsynced before the publish rename | Yes |
| `max_snapshots` | `SnapshotConfig.max_snapshots: usize` (`snapshot/metadata.rs:86`, default 5) | server config (`persistence.max-snapshots` or equivalent), flows into `RocksSnapshotCoordinator`/`SnapshotStager` (`snapshot/rocks_coordinator.rs:41,94,177,230,255,265`) | Config only | N/A |
| `save_history` | `SnapshotStats` — `last_save_time`, `last_duration`, `current_started_at`, `saves`, `failures`, `last_error` (`snapshot/mod.rs:76-105`) | `SnapshotStats::record_success`/`record_failure` (`snapshot/mod.rs:119-134`), wrapped by `SaveHistory` (below) | In-memory; `last_save_time` alone is reseeded at boot from the newest complete `snapshot_metadata` | Partial — only `last_save_time` (via `snapshot_metadata`); the counters reset |
| `SaveHistory.failed` (the `-MISCONF` latch) | `SaveHistory.failed: AtomicBool`, mirroring `stats.last_error.is_some()` (`snapshot/mod.rs:157-164`) | the same two calls that write `save_history`: `SaveHistory::record_success` clears it, `SaveHistory::record_failure` sets it (`snapshot/mod.rs:190-203`) — one writer for both fields, so drift is structurally impossible | In-memory only | No — consumed by `ConfigManager::refuse_writes_on_save_error` (`server/src/runtime_config.rs`, ~:1088-1092), the gate [FM-PERSISTENCE-046](#fm-persistence-046--a-failed-save-refuses-client-writes-only-when-the-operator-asked-for-it) already specs |
| `data_dir_layout` | the four paths `DataDirLayout` owns, all *inside* `persistence.data-dir`: the root itself, `<data-dir>/db` (the live RocksDB directory), `<data-dir>/staging` (plus its `staging.incoming` / `staging.discarded` scratch siblings), `<data-dir>/backup` | `DataDirLayout` (`frogdb-persistence/src/data_dir.rs`), the single owner every party resolves paths through | On disk | Yes — the layout *is* the on-disk contract |
| `staged_checkpoint` | presence and `is_complete_db()` verdict of `<data-dir>/staging` | writer side: operator copy or replica full-sync download (outside this crate); reader side: `StagedCheckpoint::is_complete_db` (`frogdb-persistence/src/rocks/staged.rs`) | On disk (filesystem) | Yes, until an install consumes it |
| `staged_repl_metadata` | `replication_metadata.json` inside `<data-dir>/staging` | replica full-sync writer (outside this crate) | On disk, inside the staged dir — so install rename 2 carries it to `<data-dir>/db` | Yes, until `recovery::replication::restore_state` consumes it — a role-gated call site (`frogdb-recovery/src/replication.rs:42`; the early role-gate is at `:30-32`) |
| `backup_dir` | `<data-dir>/backup/<db>_backup_<unix_secs>` directory(ies) | rename 1 of the install (`frogdb-persistence/src/rocks/checkpoint.rs`), named via `backup_dir_name` (`staged.rs`, wall-clock keyed today) | On disk | Yes, until `prune_backups` removes it (`staged.rs`, `BACKUP_RETENTION = 1`) |
| `database_id` | `DataDirMarker{database_id, created_at_unix_ms, layout_version}` in the `frogdb_data_dir` file | `DataDirMarker::mint` (`frogdb-persistence/src/data_dir.rs:125`); read/written by `recovery::data_dir::verify`/`stamp` (`frogdb-recovery/src/data_dir.rs:56,151`) | On disk, atomic temp-file-then-rename publish | Yes — stable across restarts and across a full resync (the marker names the directory, not its contents) |
| `replication_state_file` | `replication_state.json` — `ReplicationState`'s persisted fields: `replication_id`, `secondary_id` (the replid2 slot), `offset_at_save`, `secondary_offset`, `active_version` (`frogdb-server/crates/replication/src/state.rs:173-210`) | `recovery::replication::restore_state` / `ReplicationState::save` (`frogdb-recovery/src/replication.rs:27`) | On disk | Yes |
| `decode_failure_policy` | `OnDecodeFailure` (`Refuse` / `Continue`) | server config (`recovery.on-decode-failure`), read via `inputs.recovery.decode_failure_policy()` (`frogdb-recovery/src/shards.rs:106`) | Config only | N/A |
| `recovery_stats` | `RecoveryStats` — `keys_loaded`, `keys_expired_skipped`, `keys_failed`, `functions_failed`, `warm_keys_loaded`, `warm_keys_stale`, `bytes_loaded`, `duration_ms`, `first_failure` | `recovery::shards::restore` (`frogdb-recovery/src/shards.rs:44`); `functions_failed` is finished by the wiring layer (`server/init.rs`, per-library parse/register failures) | In-memory only, recomputed every boot; `keys_failed` also feeds the process metric `frogdb_recovery_keys_failed_total` (`RecoveryKeysFailed`) | No — recomputed, not carried forward |
| `recovery_functions_failed` | `RecoveryStats::functions_failed` → `frogdb_recovery_functions_failed_total` (`RecoveryFunctionsFailed`), mirroring `RecoveryKeysFailed`'s pattern | `recovery::functions::restore` on a read/parse failure (`frogdb-recovery/src/functions.rs`), plus each library the wiring layer cannot parse or register (`server/init.rs`) | Process metric + `INFO persistence` `functions_last_load_failed` | No — recomputed, not carried forward |
| `num_shards` | configured shard count | server config (`server.num-shards` or equivalent); compared against `recovery_stats`' recovered shard count by the orchestrator (`frogdb-recovery/src/lib.rs:182`) | Config only | N/A |
| `cf_layout` | the persisted column-family set (`DB::list_cf`), reconciled by `ColumnFamilyManifest::reconcile` | RocksDB itself, at CF creation | On disk (RocksDB's own manifest) | Yes |
| `atomic_flush_enabled` | RocksDB `Options::set_atomic_flush` | options builder, `frogdb-persistence/src/rocks/mod.rs` (set explicitly next to `set_wal_recovery_mode`, `:212`) | N/A — a build-time option, not persisted data | Pinned to `true` (spec-gaps issue 03, FM-PERSISTENCE-034), paired with `RocksStore::flush()` naming every CF it manages — main, warm (when enabled), search-meta — in one `flush_cfs_opt` call rather than a per-CF loop: a manual or automatic-background flush now commits every CF to the MANIFEST at the same consistent sequence point, so FrogDB's own flush surface can no longer leave one CF durably ahead of a sibling that is still WAL-only |
| `rocksdb_paranoid_checks` | RocksDB `Options::set_paranoid_checks` | options builder, `rocks/mod.rs` (set explicitly next to `set_wal_recovery_mode`) | N/A | Pinned to `true` — the value RocksDB defaults to, now stated at the call site so a library default change cannot move it silently. **Auto-resume is deliberately absent**: `max_bgerror_resume_count` has no binding in `rocksdb 0.24`, so a background error stays a background error and reaches FrogDB as a failed commit — which is exactly what the poison latch ([FM-PERSISTENCE-053](#fm-persistence-053--a-durability-failure-poisons-the-shard-until-an-operator-clears-it)) wants: nothing behind our back retries a write we have already declared lost. The rest of the corruption story is `DBRecoveryMode::PointInTime` replay stopping at the first bad record plus the `wal_watermark` drop detector (TR-PERSISTENCE-049) noticing when that stop lands short |
| `wal_recovery_mode` | RocksDB `Options::set_wal_recovery_mode` | options builder, `rocks/mod.rs:188` | N/A | Pinned to `DBRecoveryMode::PointInTime`, pinned by test |

## Transitions

One row per action source that changes State-space variables, numbered from `TR-PERSISTENCE-001`.
Rows encode **ruled** semantics — where the ruling in an open issue changes what the code does
today, the row states the ruled behavior as its postcondition, with current-code behavior kept only
as an annotation (never a row of its own) and a `Pending` field pointing at the issue that has not
landed yet. Ruling authority for this section: [issue 01](../.scratch/spec-gaps/issues/done/01-sync-durability-must-gate-the-ack.md),
[issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md),
[issue 03](../.scratch/spec-gaps/issues/open/03-enable-rocksdb-atomic-flush.md),
[issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md),
[issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md), and the persistence
addendum (R4-R6) of [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md).

*WAL append, by durability grade.*

## TR-PERSISTENCE-001 — WAL append, FireAndForget durability

| Field | Value |
| --- | --- |
| Precondition | a write action is staged; `confirm_mode = is_write && has_wal() && should_confirm()` is false — i.e. `wal_failure_policy = Continue` **and** `durability_mode != Sync`, or the shard has no WAL configured |
| Postcondition | `wal_seq` advances by one; `committed_seq`/`synced_seq` are not waited on before the caller proceeds |
| Source | `frogdb-core/src/shard/execution.rs` (`confirm_mode` gate, false branch); `frogdb-core/src/shard/persistence.rs` (`Durability::FireAndForget` branch of `persist_records`) |
| Rulings | — |

## TR-PERSISTENCE-002 — WAL append, Committed under rollback policy

| Field | Value |
| --- | --- |
| Precondition | a write action is staged; `rollback_mode = is_write && has_wal() && should_rollback()` is true — i.e. `wal_failure_policy = Rollback` (which also makes `confirm_mode` true) |
| Postcondition | the caller awaits `committed_seq` reaching this action's `wal_seq` (via `flush_through`, TR-PERSISTENCE-007) before the write is acknowledged; a failure in that range rolls the in-memory write back |
| Source | `frogdb-core/src/shard/execution.rs` (`rollback_mode` / `confirm_mode` gates, the single-command and transaction-batch `Durability::Committed` calls); `frogdb-core/src/shard/persistence.rs` (`Durability::Committed` branch of `persist_records`) |
| Rulings | [issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md) (the variant's rename to `Committed`, no semantic change to this row) |

## TR-PERSISTENCE-003 — WAL append, Sync durability forces Committed regardless of policy

| Field | Value |
| --- | --- |
| Precondition | `durability_mode = Sync` |
| Postcondition | `Durability::Committed` is used whenever `should_confirm() = should_rollback() \|\| sync_durability()`, i.e. a `Sync`-mode write is staged with `Committed` (TR-PERSISTENCE-002's wait) even under the default `Continue` policy. Landed with [issue 01](../.scratch/spec-gaps/issues/done/01-sync-durability-must-gate-the-ack.md). The two knobs are orthogonal: the mode decides *whether the ack waits*, the policy decides *what a failed wait does* — under `Continue` a failed `Committed` is logged and accounted (FM-PERSISTENCE-005) and the write is still acked; only `Rollback` undoes it and replies `-IOERR` (FM-PERSISTENCE-006). This row rules only which `Durability` value is chosen — `Committed`'s own wait target is `committed_seq` (TR-PERSISTENCE-002/007), and whether a `Sync`-mode write should additionally require `synced_seq` before acknowledgment is settled by TR-PERSISTENCE-005 |
| Source | `frogdb-core/src/shard/types.rs` (`should_confirm`/`sync_durability`); `frogdb-core/src/shard/execution.rs` (`confirm_mode`); `frogdb-core/src/shard/persistence.rs` (`persist_records`) |
| Rulings | [issue 01](../.scratch/spec-gaps/issues/done/01-sync-durability-must-gate-the-ack.md) |

## TR-PERSISTENCE-004 — WAL append, Async durability

| Field | Value |
| --- | --- |
| Precondition | `durability_mode = Async`; `wal_failure_policy = Continue` |
| Postcondition | wire behavior is identical to TR-PERSISTENCE-001 (same `FireAndForget` path, no wait). `Async` commits with `is_sync = false`, as `Periodic` does; unlike `Periodic`, no `durable_sync` driver is spawned for `Async` (`spawn_wal_sync_if_periodic` starts one only when `durability_mode == "periodic"`), so `synced_seq` has no driver and never advances under `Async` |
| Source | `frogdb-persistence/src/wal/config.rs:62-67` (`DurabilityMode::Async`); `frogdb-core/src/shard/persistence.rs:295-300`; `wal/flush.rs:424` (`is_sync: matches!(mode, DurabilityMode::Sync)`); `frogdb-server/crates/server/src/server/startup.rs:88-108` (`spawn_wal_sync_if_periodic`, periodic-only gate at `:95`) |
| Rulings | — |

## TR-PERSISTENCE-005 — Durability::Committed names the wait target; nothing binds synced_seq

| Field | Value |
| --- | --- |
| Precondition | any row above that names `Durability::Committed` |
| Postcondition | The variant is `Durability::Committed`, renamed from `Confirm` with [issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md). `Committed` over `Synced` because the wait target is `committed_seq` (TR-PERSISTENCE-007) at *every* durability mode: under `sync` the commit it waits on is the fsync, under `periodic`/`async` it is a RocksDB write. `Synced` would have been true only in `sync` mode, and the old `Confirm` said nothing at all — it read as a durability guarantee at every call site and was one only under `sync` (FM-PERSISTENCE-043). Second half of the ruling: **no client-visible acknowledgment primitive binds to `synced_seq` today.** `WAITAOF`, which in Redis counts fsynced copies specifically, is a stub that replies `-ERR ... not implemented`; `WAIT` counts replica acknowledgments of the *replication* offset, a stream position with no fsync in it. `synced_seq` is not even in `WalLagStats` — the operator-visible watermark is `committed_sequence`, deliberately (`INFO persistence` publishes the buffered-vs-committed gap, never a durable one) — and `RocksWalWriter::durable_sequence` is read only inside the persistence crate. The moment a `WAITAOF` (or any other on-device acknowledgment) lands, it binds to `synced_seq` and not to `committed_seq`, and this row records the binding |
| Source | `frogdb-core/src/shard/persistence.rs` (`enum Durability`, both variants); `flush.rs` (`committed_sequence`/`durable_sequence` accessors, the two candidates this binding chooses between); `frogdb-server/src/commands/stub.rs` (`WAITAOF`) |
| Rulings | [issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md) |

*Flush and durable-sync advance.*

## TR-PERSISTENCE-006 — flush commit outcome recorded

| Field | Value |
| --- | --- |
| Precondition | the flush thread completes a commit attempt covering sequences up to `S` |
| Postcondition | on success: `committed_seq := S` (a plain store — the flush thread is the sole writer of a successful commit's max sequence, so nothing to `fetch_max` against); if the commit passed `synced = true`, `synced_seq := max(synced_seq, S)`; `last_flush_ok := true`. On failure: `highest_failed_seq := max(highest_failed_seq, S)`, `flush_failures`/`lost_ops`/`lost_bytes` increment by the batch's counts (`fetch_add`), `last_error` is replaced, `last_flush_ok := false` |
| Source | `frogdb-persistence/src/wal/flush.rs:271-280` (`FlushOutcomes::record_success`), `:282-290` (`FlushOutcomes::record_failure`) |
| Rulings | — |

## TR-PERSISTENCE-007 — flush_through: the actual wait, then a synchronous check

| Field | Value |
| --- | --- |
| Precondition | a caller (TR-PERSISTENCE-002's `Committed` path) awaits durability for entries assigned after `after_seq` |
| Postcondition | `RocksWalWriter::flush_through(after_seq)` captures `target_seq = wal_seq` (as of the call), sends an explicit flush to the flush thread and awaits its reply — this round trip, not `confirm_durable_through` itself, is where the caller actually waits. Once the reply arrives, `confirm_durable_through(after_seq, target_seq, flush_result)` runs as a synchronous, non-waiting check over already-published atomics: (1) propagate `flush_result` if it was `Err`; (2) fail if `highest_failed_seq > after_seq`; (3) fail if `committed_sequence() < target_seq`. All three checks read state that TR-PERSISTENCE-006 already published — no polling, no additional wait |
| Source | `frogdb-persistence/src/wal/writer.rs:259-264` (`flush_through`, the round trip via `send_flush`/`done_rx.recv_async` at `writer.rs:266-282`); `frogdb-persistence/src/wal/flush.rs:338-367` (`FlushOutcomes::confirm_durable_through`) |
| Rulings | — |

## TR-PERSISTENCE-008 — out-of-band durable-sync tick (Periodic mode only)

| Field | Value |
| --- | --- |
| Precondition | the periodic sync task's interval elapses (`durability_mode = Periodic`) and `RocksStore::durable_sync`'s internal `flush()` succeeds — this driver exists only in `Periodic` mode: `spawn_wal_sync_if_periodic` starts it iff `config.durability_mode.to_lowercase() == "periodic"` (`startup.rs:95`), and outside tests the only non-test caller of `durable_sync()` in the tree is this task's loop (`wal/flush.rs:817`) — there is no `Async`-mode driver and no operator-triggered call site today |
| Postcondition | `synced_seq` advances via `publish_synced_through` to each registered shard's `committed_sequence()` *as snapshotted before the flush ran* — never to a sequence that committed during the flush itself — and `wal_watermark` is `fetch_max`-advanced (TR-PERSISTENCE-010 below) to `latest_sequence_number()` taken from that *same* pre-flush snapshot, never a fresh read after `flush()` returns |
| Source | `frogdb-persistence/src/rocks/mod.rs:344-377` (`durable_sync`; snapshot of both `pending` and `covered_seq` at `:353-371`, flush at `:372`, publish loop at `:373-375`, watermark record at `:376`); `wal/flush.rs:375-377` (`publish_synced_through` impl, `fetch_max`); `wal/flush.rs:987-1004` (`spawn_periodic_sync`, its `durable_sync()` call at `:1004`); `frogdb-server/crates/server/src/server/startup.rs:88-108` (`spawn_wal_sync_if_periodic`, the periodic-only gate at `:95`) |
| Rulings | — |

## TR-PERSISTENCE-009 — durable_sync failure short-circuit

| Field | Value |
| --- | --- |
| Precondition | `RocksStore::durable_sync` runs; its internal `self.flush()` fails |
| Postcondition | `durable_sync` returns `Err` immediately — neither `publish_synced_through` (so `synced_seq` does not advance) nor `record_wal_watermark` (so `wal_watermark` does not advance) runs for this tick. Both variables simply stay at their prior values until a later successful tick, matching the documented invariant that `wal_watermark` "may only lag, so an unsynced write can under-report but never false-alarm" |
| Source | `frogdb-persistence/src/rocks/mod.rs:372` (`self.flush()?`, the short-circuit) |
| Rulings | — |

## TR-PERSISTENCE-010 — wal_watermark write (durable-sync tick or Sync-mode commit)

This watermark has two independent writers: `Periodic` mode's out-of-band tick
(TR-PERSISTENCE-008), and a `Sync`-mode commit's direct write from the flush thread itself — the
latter is not itself a "durable sync" (no `durable_sync` call is involved) and is the *only*
watermark writer that exists when `durability_mode = Sync`, since no periodic task runs in that
mode either (TR-PERSISTENCE-008's precondition).

| Field | Value |
| --- | --- |
| Precondition | (a) `RocksStore::durable_sync` (TR-PERSISTENCE-008) completes its flush and publish loop successfully; **or** (b) a flush-thread commit with `sync = true` (`Sync`-mode) commits successfully |
| Postcondition | `wal_watermark` is `fetch_max`-advanced to `covered_seq`, a value the caller snapshotted (`RocksStore::latest_sequence_number()`) *before* starting the write or flush it now reports complete — (a) the pre-flush snapshot inside `durable_sync`, (b) the pre-write snapshot at the top of `RocksSink::commit` — never a fresh read taken after the fact, via an atomic temp-write-then-rename that is deliberately never fsynced. `fetch_max` rather than a blind store: independent concurrent callers (each shard's own sync commit, plus the periodic tick) report out of order, and a lower snapshot must not regress a higher one. (a) and (b) both terminate at the same write function; (b) is the direct in-line call the comment at `flush.rs:186-195` explains: "any concurrent shard's still-unsynced write can land in the gap between our fsync finishing and that read, inflating the claimed durable point past what is actually on disk" |
| Source | `frogdb-persistence/src/rocks/checkpoint.rs:23-43` (`record_wal_watermark`, taking `covered_seq: u64`); (a) `rocks/mod.rs:353-371` (the snapshot inside `durable_sync`, taken alongside `pending`); (b) `wal/flush.rs:186-217` (`commit`; pre-write snapshot at `:196`, rationale comment `:186-195`, the recording call at `:217`); `wal_watermark.rs:69-77` (`write`, the temp-then-rename), `:79-97` (`fetch_max`) |
| Rulings | — |

*Write-group atomicity.*

## TR-PERSISTENCE-011 — write-group open

| Field | Value |
| --- | --- |
| Precondition | a multi-action command (e.g. a transaction) begins persisting |
| Postcondition | `group_depth` increments by one; subsequent WAL appends in this group commit as one storage batch |
| Source | `frogdb-core/src/shard/persistence.rs:87,199` (`WalTarget::begin_group`); `flush.rs:400,467` |
| Rulings | — |

## TR-PERSISTENCE-012 — write-group close

| Field | Value |
| --- | --- |
| Precondition | `group_depth > 0` and the command's actions are all staged |
| Postcondition | `group_depth` decrements by one (saturating); at zero, the batch is eligible for the next flush |
| Source | `frogdb-core/src/shard/persistence.rs:89,206` (`WalTarget::end_group`); `flush.rs:400,474,480` |
| Rulings | — |

*Failure handling under each `wal_failure_policy` value.*

## TR-PERSISTENCE-013 — WAL failure under Continue (stage-time append, or flush-thread commit)

| Field | Value |
| --- | --- |
| Precondition | `wal_failure_policy = Continue`, and either (a) a `stage_actions` call's own WAL append fails (the flush-thread channel is gone), or (b) a flush-thread stage or commit attempt covering sequences through `F` fails |
| Postcondition | for case (b): the failing entries are dropped and accounted (TR-PERSISTENCE-006), the shard latches `poisoned_seq := F` (TR-PERSISTENCE-015), and every subsequent entry the flush engine receives is discarded instead of committed — the persisted stream is prefix-truncated at `F`, so recovery loss is again a suffix ([FM-PERSISTENCE-054](#fm-persistence-054--a-wal-failure-truncates-the-persisted-stream-instead-of-leaving-a-hole)). The client still gets its `+OK`: `Continue` acknowledges the loss by design (FM-PERSISTENCE-005). Case (a) is unchanged: a dead flush thread never reaches `record_failure`, so there is no `F` to poison at (see the annotation) |
| Postcondition (case (a) detail) | the WAL writer's `write_set`/`write_merge`/`write_clear`/`write_delete` fail only when the flush thread's channel is gone (`std::io::Error::other("WAL flush thread disconnected")`, `wal/writer.rs`) — the entry never reached the flush thread. Under `Continue` this is logged and the entry is dropped (`frogdb-core/src/shard/persistence.rs`, the `Durability::FireAndForget` branch) with no counter movement, since `record_failure` is flush-thread-only code |
| Source | `frogdb-core/src/shard/persistence.rs` (`stage_actions`); `frogdb-persistence/src/wal/writer.rs` (`write_set`/`write_merge`/`write_clear`/`write_delete`, the append-failure branch each shares); `flush.rs` (`FlushOutcomes::record_failure`, `FlushEngine::discard_poisoned` and its `apply`/`apply_clear`/`flush` guards) |
| Rulings | [issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md) (H2 / "Suffix restoration") |

## TR-PERSISTENCE-014 — stage failure under Rollback

| Field | Value |
| --- | --- |
| Precondition | `wal_failure_policy = Rollback` (or `Readonly`, which rolls back too); a `Committed`-gated write's commit fails |
| Postcondition | `rollback_snapshot` restores the in-memory state to before the write (`frogdb-core/src/shard/rollback.rs`), `WalRollbacks` increments, the reply is `-IOERR` — and because the failure also latched the poison (TR-PERSISTENCE-015), every *subsequent* write on this shard is refused up front with `-MISCONF` ([FM-PERSISTENCE-055](#fm-persistence-055--wal-failure-policy--readonly-fail-stops-the-shard)) rather than silently resuming acks. Reads keep being served |
| Source | `frogdb-core/src/shard/execution.rs` (rollback call sites, `WalRollbacks::inc`, and the `write_refused()` gate at the head of the write path); `frogdb-core/src/shard/rollback.rs` (`rollback_snapshot`) |
| Rulings | [issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md) |

## TR-PERSISTENCE-015 — poison latch set

| Field | Value |
| --- | --- |
| Precondition | a WAL entry is lost: a stage failure, or a commit failure — and in `sync` durability that commit failure *is* an fsync failure, the fsyncgate case the latch exists for |
| Postcondition | `poisoned_seq` latches at the first lost sequence `F` (`compare_exchange` from `0`, so a second failure never moves it) and `poison_was_sync` records whether the failing commit was fsyncing; `FlushOutcomes::poisoned()` is `true` from here on. For a lost *batch* `F` is the batch's lowest sequence (`FlushEngine::batch_min_seq`), not its highest: the whole batch is gone, so the truncation point is its first entry, while `highest_failed_seq` keeps the highest for the confirmation range |
| Source | `frogdb-persistence/src/wal/flush.rs` (`FlushOutcomes::record_failure` → `latch_poison`) |
| Rulings | [issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md) (H1 / fsyncgate) |

## TR-PERSISTENCE-016 — poison latch cleared only by restart or RESET

| Field | Value |
| --- | --- |
| Precondition | the shard is `poisoned = true` |
| Postcondition | `poisoned = false` **only** on process restart or an explicit operator reset (`WalSink::clear_poison`); a later successful commit past the failed range never clears it, and `confirm_durable_through` fails on the latch *before* it looks at the confirmed range, so the un-latching that the range check used to produce (a `Committed` above `F` returning `Ok` once `committed_seq > F`) is no longer reachable |
| Source | `frogdb-persistence/src/wal/flush.rs` (`confirm_durable_through`'s poison check ahead of the range check; `FlushOutcomes::clear_poison`) |
| Rulings | [issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md) |

## TR-PERSISTENCE-017 — wal-failure-policy = readonly

| Field | Value |
| --- | --- |
| Precondition | `wal_failure_policy = Readonly` and the shard is poisoned (TR-PERSISTENCE-015) |
| Postcondition | every write is refused with `-MISCONF` before it executes (reads still served): no ack, no loss, no silent continuation. The write that *caused* the poison is rolled back exactly as under `Rollback` — `readonly` is `rollback` plus the refusal that stops the next write from finding out the hard way |
| Source | `frogdb-persistence/src/wal/config.rs` (`WalFailurePolicy::Readonly`, `rolls_back`, `refuses_writes_when_poisoned`); `frogdb-core/src/shard/types.rs` (`ShardPersistence::write_refused`); `frogdb-core/src/shard/execution.rs` (the gate at the head of the write path) |
| Rulings | [issue 02](../.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md) (A2) |

*Checkpoint and snapshot lifecycle.*

## TR-PERSISTENCE-018 — snapshot scheduler: try_begin

| Field | Value |
| --- | --- |
| Precondition | `snapshot_in_progress = false` |
| Postcondition | `snapshot_in_progress = true`; caller receives `Some(snapshot_epoch)` and proceeds to cut a checkpoint; a concurrent caller finding `snapshot_in_progress = true` receives `None` |
| Source | `frogdb-persistence/src/snapshot/scheduler.rs:57` (`try_begin`) |
| Rulings | — |

## TR-PERSISTENCE-019 — snapshot scheduler: finish_and_maybe_rebegin

| Field | Value |
| --- | --- |
| Precondition | a snapshot completes; `snapshot_scheduled` may be `true` if a request arrived mid-run |
| Postcondition | `snapshot_in_progress = false`; if `snapshot_scheduled` was `true`, it is cleared and a fresh run begins immediately (coalesced follow-up), otherwise the scheduler goes idle |
| Source | `scheduler.rs:73` (`finish_and_maybe_rebegin`) |
| Rulings | — |

## TR-PERSISTENCE-020 — pre-checkpoint quiesce drain

| Field | Value |
| --- | --- |
| Precondition | a checkpoint cut is about to begin |
| Postcondition | every shard's WAL flush engine and search index are drained (two waves) before `create_checkpoint` runs, so the cut sees no in-flight writes |
| Source | `frogdb-server/crates/server/src/server/checkpoint_quiesce.rs`; see FM-PERSISTENCE-019 |
| Rulings | — |

## TR-PERSISTENCE-021 — checkpoint cut

| Field | Value |
| --- | --- |
| Precondition | quiesce (TR-PERSISTENCE-020) has completed |
| Postcondition | a RocksDB checkpoint is created at the target path; `rocks_seq` at cut time (`stage_checkpoint`'s call to `latest_sequence_number`) becomes the checkpoint's `snapshot_metadata.sequence_number` — **not** `wal_seq`, which is a separate, unrelated per-shard counter |
| Source | `frogdb-persistence/src/rocks/checkpoint.rs:7-19` (`create_checkpoint`), `:20-22` (`latest_sequence_number`); `snapshot/stager.rs:148-156` (`stage_checkpoint`, the call site at `:151`) |
| Rulings | — |

## TR-PERSISTENCE-022 — snapshot publish (stager finalize + install)

| Field | Value |
| --- | --- |
| Precondition | the checkpoint directory is fully written (TR-PERSISTENCE-021) |
| Postcondition | `finalize_metadata` writes `snapshot_metadata` with `mark_complete` (the `FROGDB_SNAPSHOT_COMPLETE_v1` marker), fsyncs it, and renames it into place; `install` then renames the staged directory to its published name — this rename is the commit point |
| Source | `frogdb-persistence/src/snapshot/metadata.rs:52` (`mark_complete`); `snapshot/stager.rs:170-185` (`finalize_metadata`); `stager.rs:193-197` (`install`, commit rename at `:194`); `stager.rs:89-140` (`run`, the orchestrating sequence) |
| Rulings | — |

## TR-PERSISTENCE-023 — "latest" symlink update

| Field | Value |
| --- | --- |
| Precondition | install (TR-PERSISTENCE-022) has committed |
| Postcondition | the `latest` symlink is repointed at the newly-published snapshot directory — best-effort, run after the commit rename, so a failure here does not un-publish an already-committed snapshot |
| Source | `frogdb-persistence/src/snapshot/stager.rs:261-283` (`update_latest_symlink`); `stager.rs:133` (the call site inside `run`, after `install`) |
| Rulings | — |

## TR-PERSISTENCE-024 — snapshot retention prune

| Field | Value |
| --- | --- |
| Precondition | a new snapshot publishes and the retained count exceeds `max_snapshots` |
| Postcondition | the oldest snapshot directory(ies) beyond `max_snapshots` are removed, best-effort — run after the commit rename and the symlink update (TR-PERSISTENCE-023), so a prune failure does not affect the just-published snapshot's availability |
| Source | `frogdb-persistence/src/snapshot/stager.rs:216-248` (`cleanup_old_snapshots`); `stager.rs:136` (the call site); `snapshot/metadata.rs:86` (`max_snapshots` field) |
| Rulings | — |

## TR-PERSISTENCE-025 — failed BGSAVE recorded

| Field | Value |
| --- | --- |
| Precondition | a checkpoint cut or publish fails |
| Postcondition | `save_history.last_error` is set and `save_history.failures` increments (`SnapshotStats::record_failure`, `snapshot/mod.rs:130-134`); `snapshot_in_progress` still clears (TR-PERSISTENCE-019 runs regardless of outcome) |
| Source | `snapshot/mod.rs:130-134` (`SnapshotStats::record_failure`), `:198-203` (`SaveHistory::record_failure`, which also sets the `-MISCONF` latch — TR-PERSISTENCE-026) |
| Rulings | — |

## TR-PERSISTENCE-026 — save-outcome latch set/clear (the `-MISCONF` mirror)

| Field | Value |
| --- | --- |
| Precondition | a background save attempt completes, successfully or not |
| Postcondition | `SaveHistory::record_success` clears `SaveHistory.failed`; `SaveHistory::record_failure` sets it — the same two calls that write `save_history.last_error`, so `SaveHistory.failed == save_history.last_error.is_some()` is structural rather than a convention that could drift |
| Source | `frogdb-persistence/src/snapshot/mod.rs:190-195` (`SaveHistory::record_success`), `:198-203` (`SaveHistory::record_failure`) |
| Rulings | — |

*Staged-checkpoint install.*

## TR-PERSISTENCE-027 — is_complete_db check

| Field | Value |
| --- | --- |
| Precondition | an install of `<data-dir>/staging` is requested |
| Postcondition (ruled) | `CURRENT` is read, the MANIFEST it names is parsed, and every SST/blob file it references is confirmed present with the expected size before the staged directory is treated as complete |
| Postcondition (current code) | the staged directory is treated as complete iff a file literally named `CURRENT` exists in it (`frogdb-persistence/src/rocks/staged.rs:83`) — `CURRENT`'s contents (the MANIFEST it names) and the SSTs that MANIFEST lists are never read |
| Source | `frogdb-persistence/src/rocks/staged.rs:83` (`is_complete_db`) |
| Rulings | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) |
| Pending | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) |

## TR-PERSISTENCE-028 — payload manifest written and verified at install (ruled, new)

| Field | Value |
| --- | --- |
| Precondition | a snapshot publishes (TR-PERSISTENCE-022) |
| Postcondition (ruled) | the stager writes a payload manifest (relative path, size, ideally checksum per file) into `metadata.json`; install verifies the staged tree against it when present, on top of TR-PERSISTENCE-027's MANIFEST check — proving the payload is intact, not just that the stager reported completion |
| Source | not yet implemented |
| Rulings | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) (item 2) |
| Pending | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) (item 2) |

## TR-PERSISTENCE-029 — install rename 1: existing db backed up

| Field | Value |
| --- | --- |
| Precondition | `<data-dir>/staging` verifies (TR-PERSISTENCE-027/028); a live `<data-dir>/db` directory exists |
| Postcondition | `<data-dir>/backup` is created (and the root synced, publishing its name), then `<data-dir>/db` is renamed into it as `backup_dir_name(db_name, unix_secs)` (current code: wall-clock-second keyed), then the backup root and the data-dir root are synced. Both operands are inside the data dir, so the mount point is never a rename operand ([FM-PERSISTENCE-057](#fm-persistence-057--staging-and-backup-live-inside-the-data-directory-the-mount-point-is-never-renamed)) |
| Source | `frogdb-persistence/src/rocks/checkpoint.rs` (rename-1 guard, `fs.rename`, `fs.sync_dir`); `rocks/staged.rs` (`backup_dir_name`) |
| Rulings | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) — ruled to become a monotone counter seeded from the highest existing suffix, not wall-clock seconds (an NTP step back, or two installs inside one second, currently corrupt the retention ordering) |
| Pending | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) — ruled to become a monotone counter, not wall-clock seconds |

## TR-PERSISTENCE-030 — install rename 2: commit point

| Field | Value |
| --- | --- |
| Precondition | rename 1 (TR-PERSISTENCE-029) has completed and the data-dir root has been synced |
| Postcondition | `<data-dir>/staging` is renamed to `<data-dir>/db` and the data-dir root is synced; the staged dir no longer exists; this rename is the install's commit point |
| Source | `frogdb-persistence/src/rocks/checkpoint.rs` (`fs.rename`, `fs.sync_dir`; `load_staged_checkpoint_with`, the full sequenced install) |
| Rulings | — |

## TR-PERSISTENCE-031 — backup prune

| Field | Value |
| --- | --- |
| Precondition | install (TR-PERSISTENCE-030) has committed |
| Postcondition | `prune_backups` keeps at most `BACKUP_RETENTION = 1` backup directory under `<data-dir>/backup`, best-effort (a prune failure is non-fatal) |
| Source | `rocks/staged.rs` (`prune_backups`); `checkpoint.rs` (call site) |
| Rulings | — |

## TR-PERSISTENCE-032 — post-install open failure: rollback or trial-open

| Field | Value |
| --- | --- |
| Precondition | install (TR-PERSISTENCE-030) committed; the subsequent `OpenRocks` phase fails against the newly-installed `<db>` |
| Postcondition (ruled) | either the staged directory is trial-opened before the old `<db>` is destroyed, or a failed post-install `OpenRocks` restores `backup_dir` over `<db>` and the boot refuses with a message naming what happened |
| Postcondition (current code) | no rollback occurs; `<data-dir>/staging` has already been consumed; `backup_dir` sits unrestored under `<data-dir>/backup` while `<data-dir>/db` is broken; every subsequent boot fails identically |
| Source | `frogdb-recovery/src/lib.rs:182` (`recover`, phase 1 then phase 2 in sequence, no error path back to the backup) |
| Rulings | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) |
| Pending | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) |

## TR-PERSISTENCE-033 — frogctl checkpoint-verify (ruled, new)

| Field | Value |
| --- | --- |
| Precondition | an operator runs `frogctl checkpoint-verify` against a backup or staged checkpoint, on demand |
| Postcondition (ruled) | the same payload-manifest check TR-PERSISTENCE-028 runs at install (CURRENT/MANIFEST parse plus per-file manifest verification) runs against the named directory and reports pass/fail, without installing it |
| Source | not yet implemented |
| Rulings | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) (item 6, Finding A4) |
| Pending | [issue 04](../.scratch/spec-gaps/issues/open/04-staged-checkpoint-install-verification.md) (item 6, Finding A4) |

## TR-PERSISTENCE-034 — atomic_flush cross-CF consistency

| Field | Value |
| --- | --- |
| Precondition | a crash occurs between two column families that jointly hold one logical key (e.g. a tiering demotion's warm-put and hot-delete) |
| Postcondition (ruled) | `atomic_flush_enabled = true`; all column families flush at the same sequence point, so `PointInTime` recovery cannot leave two CFs disagreeing about one key |
| Postcondition (current code) | `atomic_flush_enabled = false` (RocksDB's own default — no call site overrides it, confirmed by the checked-in `OPTIONS-000007:40` fixture); each CF flushes independently and `DBRecoveryMode::PointInTime` stops replay at the first CF-local inconsistency, so a recovered database can hold divergent state across CFs for the same key |
| Source | `frogdb-persistence/src/rocks/mod.rs:188` (recovery mode set, `atomic_flush` not) |
| Rulings | [issue 03](../.scratch/spec-gaps/issues/open/03-enable-rocksdb-atomic-flush.md) |
| Pending | [issue 03](../.scratch/spec-gaps/issues/open/03-enable-rocksdb-atomic-flush.md) |

*Recovery, in phase order.*

## TR-PERSISTENCE-035 — Phase 0: VerifyDataDir

| Field | Value |
| --- | --- |
| Precondition | boot begins; persistence is Rocks-backed |
| Postcondition | four outcomes from `verify`: (1) marker present and readable → boot continues with the existing `database_id` (`data_dir.rs:83-91`); (2) no marker, no files → fresh init mints a new `database_id` (`:133-139`); (3) no marker, files present → refuses unless `--force-fresh-data-dir` (`:100-111`); (4) no marker, no files, but `persistence.require-existing-data` is set → refuses unless `--force-fresh-data-dir`, even though the directory is empty (`:113-122`) — an empty directory is what an unmounted volume and a genuine first boot both look like, and this fourth branch is what tells them apart when the operator has declared the deployment past its first boot |
| Source | `frogdb-recovery/src/data_dir.rs:56-140` (`verify`) |
| Rulings | — |

## TR-PERSISTENCE-036 — Phase 0.5: stamp (unconditional marker rewrite)

| Field | Value |
| --- | --- |
| Precondition | verify (TR-PERSISTENCE-035) and install (TR-PERSISTENCE-037) have both run |
| Postcondition | `database_id`'s marker file is (re)written unconditionally — install may have renamed the marker away along with the directory it replaced, so this is not write-if-absent |
| Source | `frogdb-recovery/src/data_dir.rs:151-153` (`stamp`) |
| Rulings | — |

## TR-PERSISTENCE-037 — Phase 1: InstallStagedCheckpoint

| Field | Value |
| --- | --- |
| Precondition | `<data-dir>/staging` may or may not be present |
| Postcondition | if present and complete (TR-PERSISTENCE-027/028), installs it (TR-PERSISTENCE-029-031); if absent, this phase is a no-op |
| Source | `frogdb-recovery/src/checkpoint.rs:22` (`install_staged`) |
| Rulings | — |

## TR-PERSISTENCE-038 — Phase 2: OpenRocks

| Field | Value |
| --- | --- |
| Precondition | phase 1 has completed (successfully or as a no-op) |
| Postcondition | `cf_layout` is opened and reconciled against the expected shard/warm/search-meta set; `rocks_seq` becomes readable via `latest_sequence_number()` from this point on |
| Source | `frogdb-recovery/src/shards.rs:20` (`open_rocks`) |
| Rulings | — |

## TR-PERSISTENCE-039 — Phase 3: RestoreShards, decode-failure skip-and-count

| Field | Value |
| --- | --- |
| Precondition | `cf_layout` is open; some keys fail to decode; `decode_failure_policy != Refuse`; at least one key decoded successfully |
| Postcondition | `recovery_stats.keys_failed` increments per failure and is logged at ERROR; recovery continues |
| Source | `frogdb-recovery/src/shards.rs:44` (`restore`), `:95-144` (`report_decode_failures`; the ERROR log this row asserts is at `:133`) |
| Rulings | — |

## TR-PERSISTENCE-040 — Phase 3: RestoreShards, wholly-undecodable refusal

| Field | Value |
| --- | --- |
| Precondition | `decode_failure_policy = Refuse`, **or** nothing at all decoded (`keys_loaded + keys_expired_skipped + warm_keys_loaded + warm_keys_stale == 0`) |
| Postcondition | boot is refused rather than starting from an empty-looking store that may actually be entirely undecodable |
| Source | `frogdb-recovery/src/shards.rs:106` (`decode_failure_policy() == OnDecodeFailure::Refuse` bail); the zero-decoded bail alongside it |
| Rulings | — |

## TR-PERSISTENCE-041 — Phase 4: RestoreFunctions

| Field | Value |
| --- | --- |
| Precondition | phase 3 has completed; `functions.fdb` is unreadable or fails to parse |
| Postcondition | the failure is still downgraded to an empty library set with a `warn!` — the boot proceeds — but it is counted: `RecoveryStats::functions_failed` gains `1`, `frogdb_recovery_functions_failed_total` increments, and `INFO persistence` reports `functions_last_load_failed` alongside `rdb_last_load_keys_failed` (mirrors TR-PERSISTENCE-039's `keys_failed` pattern). A library the wiring layer then fails to parse or register adds `1` to the same two. See [FM-PERSISTENCE-037](#fm-persistence-037--a-corrupt-function-library-does-not-block-startup) |
| Source | `frogdb-recovery/src/functions.rs` (`restore`); `frogdb-server/src/server/init.rs` (per-library registration) |
| Rulings | [issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md) |

## TR-PERSISTENCE-042 — Phase 5: RestoreReplicationState

| Field | Value |
| --- | --- |
| Precondition | boot completes phase 4 and the node is primary- or replica-role (this phase is role-gated: `restore_state` early-returns fresh state without touching disk for every other role, `frogdb-recovery/src/replication.rs:30-32`) |
| Postcondition (ruled) | a boot that did not recover a dataset intact (`recovery_stats.keys_failed > 0`, or `wal_dropped_records_total > 0`, or persistence disabled) mints a fresh replication id rather than adopting `replication_state_file`; a boot that did recover an intact dataset shifts any prior id into `secondary_id` at a frozen boundary and mints a fresh primary id (an unclean-restart case, not covered by phase 5 alone — see TR-PERSISTENCE-044's identity-write constraint) |
| Postcondition (current code) | `replication_state_file` loads (or is freshly created) via `ReplicationState::load_or_create` for every primary/replica boot, independent of whether phase 3 recovered the dataset intact — a boot that recovered no dataset (persistence disabled, or an empty store) still adopts whatever replication id the file names |
| Source | `frogdb-recovery/src/replication.rs:27-36` (`restore_state`, disk read at `:34-36`); `frogdb-recovery/src/lib.rs:182` (`recover`, phase 5 comment: "role-gated, not persistence-gated") |
| Rulings | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Ruling + Amendment) |
| Pending | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Ruling + Amendment) |

## TR-PERSISTENCE-043 — Phase 5: staged replication metadata adoption

| Field | Value |
| --- | --- |
| Precondition | `staged_repl_metadata` is present (written by a replica full-sync); the role gate above (TR-PERSISTENCE-042) already passed |
| Postcondition | its identity is adopted unconditionally over `replication_state_file`'s own contents, saved, and the staging file is consumed only if the save succeeds (left in place otherwise, so the next boot re-adopts) |
| Source | `frogdb-recovery/src/replication.rs:42` (`read_staged_replication_metadata`, a call site reached only after the role gate at `:30-32`) |
| Rulings | — |

## TR-PERSISTENCE-044 — identity write location, ruled (moved inside the quiesce window)

| Field | Value |
| --- | --- |
| Precondition | a checkpoint cut (TR-PERSISTENCE-020/021) is in progress |
| Postcondition (ruled) | replication identity and `offset_at_save` are written inside FM-PERSISTENCE-019's quiesce window, in the same atomic unit as the write that names the offset — not as a separate post-cut sidecar write that can tear the pair |
| Source | not yet implemented — today identity lives in the independently-timed `replication_state_file` |
| Rulings | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Amendment, point 4) |
| Pending | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Amendment, point 4) |

## TR-PERSISTENCE-045 — replication_state.json demotion, ruled

| Field | Value |
| --- | --- |
| Precondition | the dataset-authoritative identity scheme (TR-PERSISTENCE-042/044) is in place |
| Postcondition (ruled) | `replication_state_file` is either retired outright or demoted to a cache keyed on `database_id` plus the recovered sequence number, ignored whenever either mismatches the opened database |
| Source | not yet implemented |
| Rulings | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R4) |
| Pending | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R4) |

## TR-PERSISTENCE-046 — offset validated against snapshot sequence anchor, ruled

| Field | Value |
| --- | --- |
| Precondition | a restored `offset_at_save` is being adopted at boot |
| Postcondition (ruled) | it is validated against `snapshot_metadata.sequence_number` from the same cut/window; a mismatch is treated as corruption, not silently adopted |
| Source | not yet implemented |
| Rulings | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R6) |
| Pending | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R6) |

## TR-PERSISTENCE-047 — Phase 6: OpenClusterStorage

| Field | Value |
| --- | --- |
| Precondition | `inputs.cluster.enabled` |
| Postcondition | `<data_dir>/raft` is opened as `ClusterStorage`; if cluster mode is disabled this phase is a no-op |
| Source | `frogdb-recovery/src/cluster.rs:18` (`open_storage`) |
| Rulings | — |

## TR-PERSISTENCE-048 — orchestrator: shard-count invariant

| Field | Value |
| --- | --- |
| Precondition | phase 3 has restored `recovery_stats` for some number of shards |
| Postcondition | the recovered shard count must equal the configured `num_shards`, or boot refuses |
| Source | `frogdb-recovery/src/lib.rs:182` (`recover`, shard-count mismatch check between phases 3 and 5) |
| Rulings | — |

## TR-PERSISTENCE-049 — WAL point-in-time replay and dropped-records detection

| Field | Value |
| --- | --- |
| Precondition | `OpenRocks` (TR-PERSISTENCE-038) replays the WAL under `wal_recovery_mode = PointInTime` |
| Postcondition | replay stops at the first corrupted/incomplete record, yielding a recovered `rocks_seq`; `wal_watermark::detect_and_reset` compares that recovered sequence against the persisted `wal_watermark` — if the recovered sequence is below the watermark, `wal_dropped_records_total` increments and a WARN is logged; `wal_watermark` is always re-baselined to the recovered sequence afterward, via the same never-fsynced write TR-PERSISTENCE-010 uses |
| Source | `frogdb-persistence/src/rocks/wal_watermark.rs:87-126` (`detect_and_reset`, metric increment at `:95`, re-baseline write at `:122-124`); `rocks/mod.rs:293-297` (open-time call sites: `detect_and_reset` on an existing db, `wal_watermark::write` to seed on a fresh one); `mod.rs:188` (recovery mode) |
| Rulings | — |

## TR-PERSISTENCE-050 — database_id mint (fresh data directory)

| Field | Value |
| --- | --- |
| Precondition | no marker file and no existing files (TR-PERSISTENCE-035's fresh-init branch) |
| Postcondition | `database_id` is minted fresh (`DataDirMarker::mint`) and stamped (TR-PERSISTENCE-036) |
| Source | `frogdb-persistence/src/data_dir.rs:125` (`mint`) |
| Rulings | — |

## TR-PERSISTENCE-051 — database_id validate (existing data directory)

| Field | Value |
| --- | --- |
| Precondition | a marker file is present at boot |
| Postcondition | it is read and accepted if `layout_version <= DATA_DIR_LAYOUT_VERSION`; a version from the future refuses boot; **any** marker-read error — unreadable, malformed, or a future layout version alike — refuses unless `--force-fresh-data-dir`, which bypasses all of them and adopts the directory as-is |
| Source | `frogdb-persistence/src/data_dir.rs:76,144` (`DataDirMarkerError`, `read`); `frogdb-recovery/src/data_dir.rs:60-70` (the force-bypasses-every-read-error branch) |
| Rulings | — |

## TR-PERSISTENCE-052 — database_id first real consumer, ruled

| Field | Value |
| --- | --- |
| Precondition | `database_id` is minted or validated (TR-PERSISTENCE-050/051) |
| Postcondition (ruled) | `database_id` is compared against an expected value somewhere it currently is not — the R4 identity-cache key (TR-PERSISTENCE-045) is the first concrete consumer named by the current rulings |
| Source | not yet implemented — today nothing compares `database_id` across boots (confirmed: no comparison call site exists in `frogdb-recovery/src/` or `frogdb-persistence/src/`) |
| Rulings | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R4) — sole owner: [issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md) deferred the consumer to it rather than inventing an expected-id knob with no reader |
| Pending | [replication issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) (Addendum R4) |

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
| NOT observable | An acknowledged write missing after recovery. A commit that reaches the sink with `sync = false` while the mode is `sync` — the batch would be in the OS page cache, not on the device, and a power loss would take it. Equally: **a `sync`-mode write acknowledged with only a `FireAndForget` stage behind it** — the entry enqueued on the flush channel, the reply produced, and the commit left to the flush thread's own size/timeout schedule. The durability *mode* gates the ack; the failure policy does not get a vote on whether the ack waits. |
| Invariant | Two orthogonal conditions, and the row needs both. **(1) The commit fsyncs**: `DurabilityMode::Sync` makes every `WriteSink::commit` pass `sync = true`. **(2) The ack waits for that commit**: `execute_command` / `execute_transaction` select `Durability::Committed` iff `should_confirm() = should_rollback() \|\| sync_durability()` — the failure policy (`rollback`) *or* the durability mode (`sync`), not the policy alone — and `persist_records` under `Committed` snapshots the WAL sequence before the first entry and `flush_through(start_seq)` before returning, so the reply cannot outrun the fsync. The two knobs stay orthogonal: `sync` decides *whether the ack waits*, the policy decides *what a failed wait does* (`rollback` refuses and undoes, FM-PERSISTENCE-006; `continue` accounts and acks, FM-PERSISTENCE-005). Scope: this row is about the crash window, not about a WAL that is failing — a write lost to an *observed* WAL failure under `continue` belongs to FM-PERSISTENCE-005. The page-cache crash model in `wal/tests.rs` (`PageCacheSink`) makes the distinction real: entries committed without `sync` live in a cache the simulated crash discards; the `FakeWalLog::durable_writes` log projection is the same model at the shard seam, where only a `flush_through` promotes a staged entry past the crash. |
| Outcome variant | n/a (storage-layer invariant; `INFO persistence` `durability_mode`, `wal_durability_lag_ms`) |
| Forced by | `test_sync_mode_zero_acked_write_loss`, `test_fsync_seam_sync_flag_matches_mode`, `test_sync_mode_crash_recovery`, `test_durability_mode_gates_crash_loss`, `confirm_snapshots_sequence_then_flushes_once`, `sync_mode_under_continue_policy_flushes_before_the_reply`, `sync_mode_acked_write_survives_a_crash_under_continue_policy`, `sync_durability_selects_confirm_independently_of_the_failure_policy` |
| Bug refs | `.scratch/testing-improvements/issues/12-durability-fsync-boundary.md` (done — these tests are its outcome) |

## FM-PERSISTENCE-003 — `periodic` durability bounds loss by the flush interval

| Field | Value |
|---|---|
| Trigger | `persistence.durability-mode = periodic` (the default, `interval_ms = 1000`; Redis `appendfsync everysec`); a crash lands between two periodic syncs. |
| Observable | Writes acknowledged more than one interval before the crash are recovered. Writes acknowledged inside the current window may be absent — that is the mode's contract, not a defect. |
| NOT observable | Loss of a write older than the interval, i.e. an unbounded window: the periodic sync thread silently stopping would turn `periodic` into `async` while `INFO` still reports `durability_mode:periodic`. Also not observable: a *gap* — a recovered keyspace holding a later write while a strictly earlier one from the same shard is missing. Crash loss is a suffix because the WAL commits in enqueue order; *failure* loss is a suffix because the first lost entry poisons the shard and truncates everything behind it ([FM-PERSISTENCE-054](#fm-persistence-054--a-wal-failure-truncates-the-persisted-stream-instead-of-leaving-a-hole)) — without that second half this NOT-observable was simply false. |
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
| NOT observable | A durable sequence above the last fsynced sequence — "handed to RocksDB" reported as "on the device", which is a durability claim the mode never made. Equally: a durable sequence above the *committed* sequence (an out-of-band sync claiming writes committed after its flush began); a `Durability::Committed` that starts failing in `periodic`/`async` because it was re-pointed at a watermark those modes never advance; an empty-stage `flush()` moving either sequence. |
| Invariant | `FlushOutcomes` keeps two sequences instead of conflating them. `committed_seq` is stored by `record_success` after every successful commit and is what `confirm_durable_through` checks — `Durability::Committed` means "the batch reached storage", and the durability mode alone decides whether storage means the device (FM-PERSISTENCE-002/003/004). `synced_seq` is `fetch_max`'d only when that commit passed `sync = true`, or by `publish_synced_through` from `RocksStore::durable_sync`, which snapshots each registered writer's `committed_seq` *before* calling `flush()` so it can never over-claim a write committed during the flush. Writers register a `Weak<dyn DurableSyncTarget>` with the store at construction, so the out-of-band syncer reaches every shard's watermark without the flush thread being on the path. `FlushEngine::flush` returns early on an empty stage without touching either sequence. The split is also in the *names*: the wait variant is `Durability::Committed` (renamed from `Confirm`, [TR-PERSISTENCE-005](#tr-persistence-005--durabilitycommitted-names-the-wait-target-nothing-binds-synced_seq)), so a `periodic`/`async` call site cannot read an fsync guarantee into the variant it selected. And the binding this row exists to prevent an unearned claim about is stated rather than left silent: **no client-visible acknowledgment primitive binds to `synced_seq`** — `WAITAOF` is an unimplemented stub, `WAIT` acknowledges a replication offset, and `INFO persistence` reports `committed_sequence`-derived lag only. The first primitive that claims on-device durability to a client must wait on `synced_seq`. |
| Outcome variant | n/a (storage-layer invariant; `WalLagStats`) |
| Forced by | `durable_sequence_tracks_only_fsynced_commits_in_periodic_mode`, `durable_sequence_tracks_only_fsynced_commits_in_async_mode`, `sync_mode_durable_sequence_equals_committed_sequence`, `empty_flush_advances_neither_sequence`, `durable_sync_publishes_the_flushed_sequence_to_every_registered_writer` |
| Bug refs | `.scratch/testing-improvements-round2/issues/done/71-durable-sequence-advances-on-non-synced-commits.md` (fixed) |

## FM-PERSISTENCE-005 — a WAL failure under the default `continue` policy acknowledges a lost write

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = continue` (**the default**) and the WAL sink fails — a full or read-only device, a RocksDB write error, a dead flush channel — while a write command is executing. |
| Observable | `+OK`. The key is in the in-memory keyspace and is served to readers, but it is not on disk and does not come back after a restart. The only signal is `INFO persistence` (`wal_last_flush_status:err`, `wal_flush_failures`, `wal_lost_ops`, `wal_pending_ops`), the `frogdb_wal_*` counters, and a rate-limited `ERROR` log (`FAILURE_LOG_INTERVAL`, one line per 100 failures, so a sustained outage does not drown the log). |
| NOT observable | An error reply (that is FM-PERSISTENCE-006's mode, and it needs the non-default policy). A shard that stops accepting commands or panics — `stage_actions` under `FireAndForget` logs each failure and continues to the next action, and the shard keeps replying `+OK`. Counters must not reset on a later success: `FlushOutcomes` accumulates `flush_failures`/`lost_ops`/`lost_bytes` for the life of the process, so an operator sampling `INFO` after recovery still sees that data was lost. Equally not observable: the *persisted stream* resuming behind the operator's back — from the first loss on, this shard's entries are discarded rather than committed ([FM-PERSISTENCE-054](#fm-persistence-054--a-wal-failure-truncates-the-persisted-stream-instead-of-leaving-a-hole)), so `continue` acknowledges a *suffix* of lost writes, never a hole with good data on both sides. |
| Invariant | `Durability::FireAndForget` never returns `Err`; the failure is recorded in `FlushOutcomes` and surfaced through `INFO`/metrics rather than the reply. This is a deliberate availability-over-durability default and the main deviation from Redis, which stops accepting writes entirely (`MISCONF`) once a background save fails — FrogDB offers that refusal two ways, both opt-in: [FM-PERSISTENCE-046](#fm-persistence-046--a-failed-save-refuses-client-writes-only-when-the-operator-asked-for-it) for a failing *save*, and `wal-failure-policy = readonly` ([FM-PERSISTENCE-055](#fm-persistence-055--wal-failure-policy--readonly-fail-stops-the-shard)) for a failing *WAL*, which is the fail-stop escalation `continue` deliberately does not perform; see [Redis deviations](#redis-deviations). |
| Outcome variant | `frogdb_wal_lost_ops` / `frogdb_wal_flush_failures` (`INFO persistence` `wal_lost_ops`, `wal_flush_failures`, `wal_last_flush_status`) |
| Forced by | `wal_failure_in_continue_mode_acks_the_write_and_keeps_it_in_memory`, `test_sink_stage_failure_recorded_and_surfaced`, `fire_and_forget_never_flushes_and_continues_on_error`, `test_continue_mode_default`, `should_rollback_follows_shared_flag` |
| Bug refs | none |

## FM-PERSISTENCE-006 — a WAL failure under `rollback` refuses the write and restores the key

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = rollback` (live-settable) and the WAL fails while a single write command is executing. |
| Observable | `-IOERR WAL persistence failed: <cause>`. The key is back at its pre-command value — restored if it existed, deleted if it did not, with its previous expiry and no leftover TTL. `frogdb_wal_rollbacks_total` increments. |
| NOT observable | A committed in-memory value with an error reply — the pair a client cannot reconcile, and the exact reason this policy exists. Also not observable: rollback for a *read*, for a command with no WAL strategy, or when no WAL is configured (`has_wal()` gates the whole path, so an in-memory-only server never pays the snapshot cost); a rollback that resurrects a key the command did not touch; replication or keyspace-notification effects for the refused write, which are skipped because `run_write_effects` is never reached on the error arm. |
| Invariant | `execute_command` decides `rollback_mode = is_write && has_wal() && should_rollback()` *before* execution and captures a `WriteSnapshot` of exactly the keys the handler's `WalStrategy` will persist (`ClearShard` is filtered out — a full clear has no per-key undo, so on failure the in-memory clear stands). The policy lives in a shared `AtomicU8` so `CONFIG SET wal-failure-policy` retunes every shard without a restart. `warn_if_rollback_without_sync` logs at startup that `rollback` with a non-`sync` durability mode still leaves a loss window: rollback only covers failures the shard *observes*, and a background flush failure is observed at the next `Committed` (FM-PERSISTENCE-007), not at the write that was lost. |
| Outcome variant | `frogdb_wal_rollbacks_total`; reply prefix `-IOERR` |
| Forced by | `the_atomic_encoding_round_trips_and_distinguishes_the_policies`, `an_unknown_encoding_decodes_to_continue`, `config_strings_round_trip_and_only_rollback_selects_rollback`, `wal_failure_in_rollback_mode_replies_ioerr_and_restores_the_key`, `test_rollback_existing_key`, `test_rollback_missing_key`, `test_rollback_del_restores_key`, `test_rollback_rename`, `test_rollback_preserves_expiry`, `test_rollback_clears_added_expiry`, `test_rollback_mode_flag_toggle`, `test_snapshot_arc_efficiency` |
| Bug refs | none |

## FM-PERSISTENCE-007 — a swallowed background flush failure surfaces at the next durability confirmation

| Field | Value |
|---|---|
| Trigger | A batch commits in the background — fired by the size threshold or the batch timeout, with no caller waiting — and fails. A later write then asks for `Durability::Committed`. |
| Observable | The confirming caller gets the error, attributed to the failed sequence range, even though it was a *different* write that failed. In rollback mode that becomes the confirming write's `-IOERR` (FM-PERSISTENCE-006). |
| NOT observable | A `Committed` returning `Ok` while `highest_failed_seq >= target_seq` — an acknowledged write that outran a swallowed failure, which is the whole hazard. Equally, and **inverted from this row's original text**: a `Committed` for a range *above* the failure returning `Ok` because a later commit succeeded. "A later commit succeeded" is not evidence the earlier range reached the device — a Linux writeback error marks the page clean-but-failed and reports it to exactly one `fsync` caller, so the next `fsync` returns `0` with the data gone (the 2018 Postgres fsyncgate). The failure therefore stays latched ([FM-PERSISTENCE-053](#fm-persistence-053--a-durability-failure-poisons-the-shard-until-an-operator-clears-it)) until an operator clears it. |
| Invariant | `FlushOutcomes` records `highest_failed_seq` and `last_error` alongside `durable_seq`; `confirm_durable_through(after_seq, target_seq, flush_result)` checks the poison latch first and only then compares the confirmed range, so a background failure inside the range the caller is confirming is re-raised rather than dropped on the floor of a thread nobody joins, and one outside it still fails the confirmation because the shard as a whole is no longer trustworthy. |
| Outcome variant | `frogdb_wal_flush_failures` (`INFO persistence` `wal_last_flush_status:err`, `wal_flush_failures`) |
| Forced by | `test_sink_size_threshold_flush_error_surfaces_on_confirm`, `test_sink_timeout_flush_error_surfaces_on_confirm`, `confirm_propagates_flush_failure`, `test_sink_failure_latches_and_confirmations_stay_failed` |
| Bug refs | none |

## FM-PERSISTENCE-053 — a durability failure poisons the shard until an operator clears it

| Field | Value |
|---|---|
| Trigger | The flush thread loses a WAL entry: a stage failure, or a commit failure — which in `sync` durability is an *fsync* failure. The sink then recovers on its own (a full device gets space back, a transient IO error stops recurring) and later batches commit successfully. |
| Observable | `FlushOutcomes::poisoned()` stays `true` and every subsequent `Durability::Committed` fails with the latched error, naming the poisoned sequence — including confirmations for ranges *entirely above* the failure. `wal_lost_ops` / `wal_flush_failures` keep their totals. Only `clear_poison` (the operator reset) or a process restart returns the shard to healthy. |
| NOT observable | A `Committed` that succeeds because a later commit succeeded — the un-latching this row's predecessor (FM-PERSISTENCE-007) used to require. A second failure moving the latch forward: `poisoned_seq` names the *first* lost sequence, because that is where the persisted stream was truncated (FM-PERSISTENCE-054). A successful commit clearing the latch, an `INFO` sample reporting the shard healthy while it is poisoned, or the latch surviving a restart (it is in-memory: a restart is a real recovery event, and recovery re-reads what is actually on disk). |
| Invariant | `FlushOutcomes::record_failure` calls `latch_poison(seq)`, a `compare_exchange` from `0` so only the first loss wins, and records `poison_was_sync` from the commit's `sync` flag for the operator-facing message. `confirm_durable_through` checks the latch **before** the `highest_failed_seq > after_seq` range check, so the range check can no longer un-latch a failure by outrunning it. This is the fsyncgate lesson: a Linux writeback error marks the failed page clean and reports it to exactly one `fsync` caller, so a later `fsync` returning `0` says nothing about the earlier data — Postgres answers with a PANIC, RocksDB latches its background error, and FrogDB latches here. |
| Outcome variant | `frogdb_wal_flush_failures` / `frogdb_wal_lost_ops` (`INFO persistence` `wal_last_flush_status:err`); `-IOERR` at the confirming write under `rollback`, `-MISCONF` at the next write under `rollback`/`readonly` (FM-PERSISTENCE-055) |
| Forced by | `a_lost_batch_latches_the_poison_at_the_first_lost_sequence`, `a_later_success_never_unlatches_the_poison`, `clear_poison_is_the_only_way_back`, `test_sink_failure_latches_and_confirmations_stay_failed` |
| Bug refs | `.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md` (H1) |

## FM-PERSISTENCE-054 — a WAL failure truncates the persisted stream instead of leaving a hole

| Field | Value |
|---|---|
| Trigger | An entry at sequence `F` is lost (stage or commit failure) while later entries from the same shard keep arriving — the same batch's remaining actions, and every batch after it. |
| Observable | Nothing at or after `F` is committed: the flush engine discards later entries, counting them in `lost_ops`/`lost_bytes` exactly as if their own commit had failed. What recovery finds is the shard's history up to `F`, and nothing else — loss is a suffix. |
| NOT observable | A *hole*: a committed entry above `F` with `F` itself missing. That is what made FM-PERSISTENCE-003's "loss is always a suffix" false for failure loss, and it breaks the prefix consistency replication offsets are arithmetic over. Also not observable: the truncation ending on its own — it lasts as long as the poison latch does (FM-PERSISTENCE-053), so an operator, not a lucky retry, decides when the shard persists again. Nor a truncation that hides itself: every discarded entry is counted and logged. |
| Invariant | `FlushEngine::apply` and `apply_clear` both begin by consulting `FlushOutcomes::poisoned()`; when it is set they route the entry through `discard_poisoned`, which accounts the loss and returns without touching the sink. `FlushEngine::flush` still commits whatever is already staged — those entries were assigned *below* the truncation point and belong to the surviving prefix — but reports `Err` while the shard is poisoned, so no flush waiter mistakes someone else's batch for its own durability. The latch is set by the failure itself, inside `record_failure`, so there is no window between "the first entry was lost" and "the engine stops committing" for a later entry to slip through. |
| Outcome variant | `frogdb_wal_lost_ops` / `frogdb_wal_lost_bytes` (`INFO persistence` `wal_lost_ops`) |
| Forced by | `a_poisoned_engine_discards_later_entries_instead_of_committing_them`, `test_sink_stage_failure_recorded_and_surfaced` |
| Bug refs | `.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md` (H2) |

## FM-PERSISTENCE-055 — `wal-failure-policy = readonly` fail-stops the shard

| Field | Value |
|---|---|
| Trigger | `persistence.wal-failure-policy = readonly` (or `rollback`), the WAL has failed at least once, and a client sends another write. |
| Observable | `-MISCONF FrogDB is unable to persist writes: the WAL failed and this shard is fenced. Fix storage, then restart the node.` Reads are served normally, and the failing write that latched the poison was itself rolled back with `-IOERR` (FM-PERSISTENCE-006). `frogdb_wal_lost_ops` stops growing, because nothing is accepted to lose. |
| NOT observable | Under `readonly`/`rollback`: a write acknowledged after the WAL failed, i.e. the "ack every write, lose every write, refuse nothing" posture that made `continue` worse than Redis' `MISCONF` default. Under `continue` (the default): any refusal at all — the fail-stop is opt-in, and an operator who chose availability keeps it. Also not observable: reads refused (this is not a global freeze), or a refusal on a shard whose own WAL never failed — the latch is per-shard, like the WAL it guards. |
| Invariant | `WalFailurePolicy` gains a third value, `Readonly` (encoding `2`, config string `readonly`); `rolls_back()` covers `Rollback \| Readonly`, so `readonly` keeps `rollback`'s undo semantics and adds the refusal. `ShardPersistence::write_refused()` = `has_wal() && policy.refuses_writes_when_poisoned() && wal_poisoned()`, checked in `execute_command_inner` as soon as the command is known to be a write — before it executes, and before the memory check, since a fenced shard is not taking the write on any grounds. Deliberate deviation from this row's ruling, which named FM-PERSISTENCE-046's connection-level PreChecks gate: the check lives at the shard's single dispatch seam instead, because that is where the poisoned WAL is, and because that seam also covers the two paths the PreChecks gate documents as blind spots — writes issued from inside a Lua script, and a `MULTI` queued healthy and `EXEC`'d after the failure. The cross-shard scatter path (`execute_scatter_part`) does not route through this seam and is not yet fenced; a scattered `MSET` on a poisoned shard still follows the `Committed`/rollback path of FM-PERSISTENCE-006. |
| Outcome variant | reply prefix `-MISCONF`; `frogdb_wal_flush_failures` |
| Forced by | `readonly_is_a_distinct_policy_that_rolls_back_and_refuses`, `a_poisoned_shard_refuses_writes_only_under_a_refusing_policy`, `a_poisoned_shard_under_readonly_refuses_every_later_write_and_still_reads` |
| Bug refs | `.scratch/spec-gaps/issues/done/02-durability-failure-taxonomy.md` (A2) |

## FM-PERSISTENCE-008 — a failed `Committed` batch is never flushed as a partial batch

| Field | Value |
|---|---|
| Trigger | Under `Durability::Committed`, one of the batch's stage calls fails (or the group could not be opened) — a dead channel, a full device. |
| Observable | The caller gets the staging error. Nothing from that batch is force-flushed; the shard's durable sequence does not advance on its account. |
| NOT observable | A `flush_through` for a batch that failed to stage — it would confirm a *prefix* of the batch as durable and let the caller ack it. Also not observable: N identical error logs for a batch whose group open failed; the entries are skipped wholesale, since a marker only fails when the channel is gone and every write below would fail identically. |
| Invariant | The `Committed` arm checks `opened? staged? closed?` in that order before reaching `flush_through`, so any earlier failure short-circuits. `stage_actions` uses `?` per action under `Committed` (stop at the first failure) and `inspect_err`-and-continue under `FireAndForget`. |
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
| Trigger | A `Durability::Committed` write (or an operator `sync_wal`) issues `WalCommand::Flush` while another batch has a group open on the same shard. |
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
| Observable | The pre-clear batch commits, the clear commits, and post-clear writes land in a batch of their own — and survive. The durable sequence advances across the clear, so a `Committed` immediately after a clear is answerable. |
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
| Trigger | An operator wants to restore a `BGSAVE` artifact: they copy `snapshot_NNNNN/checkpoint/.` into `<data-dir>/staging` inside a fresh data directory and start the server. |
| Observable | Startup installs the staged checkpoint and the whole keyspace comes back, every type intact; a key written *after* the cut is absent. The restored server then behaves normally — later writes are WAL-durable and replay across a further restart, layered on the installed checkpoint. |
| NOT observable | A snapshot being loaded automatically into an existing data directory. Restarting a server against its own data directory rolling it back to the last `BGSAVE` — restart recovery is RocksDB + WAL, not the snapshot; the snapshot directory is read at startup only to resume the epoch counter and seed `last_save_time`. |
| Invariant | Recovery consumes exactly one staged artifact, `<data-dir>/staging`, and nothing under `snapshot_dir`. This is a deliberate divergence from Redis, which loads its RDB at startup — see [Redis deviations](#redis-deviations). |
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
| Invariant | The stager writes through a `SnapshotFs` seam (`sync_file`, `sync_dir`, `rename`, `symlink`, `write`) rather than calling `std::fs` directly, so the order of syncs and renames is a testable sequence and not a claim about syscalls. Order: `finalize_metadata` fsyncs `metadata.json.tmp` and then the staging directory, so the commit rename cannot become durable ahead of the metadata it publishes; `install()` fsyncs `snapshot_dir` after the promotion rename; `update_latest_symlink` fsyncs `snapshot_dir` again after the repoint. RocksDB's `create_checkpoint` already fsyncs the checkpoint's own file contents and directory, so the stager syncs only what it wrote itself. `load_staged_checkpoint` applies the same rule to the two install renames, fsyncing every directory whose entries changed: the data-dir root before rename 1 (which publishes the `backup/` name the rename targets), then `backup/` and the root after rename 1, then the root again after rename 2. Directory durability is `File::open(dir)?.sync_all()`, the portable directory-entry barrier; a platform whose directories cannot be opened for sync (Windows) degrades to a no-op rather than failing a save. |
| Outcome variant | n/a (a failure to sync is an `SnapshotError::Io` on the save, not a silent success) |
| Forced by | `stager_fsyncs_metadata_and_staging_dir_before_install`, `stager_fsyncs_snapshot_dir_after_install_and_after_latest_repoint`, `staged_checkpoint_install_fsyncs_every_directory_it_changes` |
| Bug refs | `.scratch/hardening/issues/done/04-checkpoint-publish-renames-not-fsynced.md` (fixed), `.scratch/testing-improvements-round2/issues/done/74-snapshot-stager-fsyncs-nothing-before-promoting-latest.md` (the same hole, filed twice; fixed together) |

---

# Staged-checkpoint install — the boot-time filesystem protocol

## FM-PERSISTENCE-024 — an incomplete staged checkpoint is refused without touching the live database

| Field | Value |
|---|---|
| Trigger | `<data-dir>/staging` exists but is not a database this node can open — a full sync that died mid-download, a partial operator copy (FM-PERSISTENCE-021), a directory holding stray `.sst` files and no `CURRENT`, a `CURRENT` naming a MANIFEST that is absent, truncated, or corrupt, or a payload whose SSTs did not all arrive. |
| Observable | Startup fails with `RecoveryError { phase: InstallStagedCheckpoint }` naming the staged directory and which of the three checks rejected it. The live database is exactly as it was: still in place at `<data-dir>/db`, `CURRENT` intact, and no backup created under `<data-dir>/backup`. The staged directory is left on disk for the operator to inspect or re-copy. |
| NOT observable | The live database being renamed aside for a checkpoint that cannot replace it — which would leave RocksDB to open a fresh empty database in its place: silent, total data loss dressed as a clean boot. Worse, it is not recoverable by retrying: the staged dir has been consumed, so FM-PERSISTENCE-025's idempotence makes every later boot fail identically, and `BACKUP_RETENTION = 1` (FM-PERSISTENCE-026) means the next full sync overwrites the last good copy. Also not observable: a checkpoint refused for damage a *hand-rolled* MANIFEST reader thought it saw — the verdict on the format belongs to the party that owns it. A silent fallback to the live database: the operator staged this checkpoint deliberately, so a refusal is fatal to the boot rather than a quiet "restored server that isn't restored". |
| Invariant | The whole verdict is reached **before the first rename**, while nothing has moved, in three stages of increasing strength: (1) structure — `CURRENT` is read (not `stat`ed) and must name a `MANIFEST-NNNNNN` that exists; (2) payload manifest — every file `frogdb_payload.json` lists is present at its recorded size, when the payload carries one (FM-PERSISTENCE-056); (3) trial open — `RocksStore::trial_open_payload` opens the staged directory read-only with `paranoid_checks` and `max_open_files = -1`, which forces a table reader for every file the current version references, so a missing or truncated SST fails here and not on a later read. Stage 3 is what resolves the MANIFEST to the file set it references, and it is deliberately not reimplemented: the MANIFEST is a version-coupled `VersionEdit` log, and a parser's mis-reads would refuse *good* checkpoints. The read-only open takes no `LOCK` and mutates nothing but RocksDB's own `LOG`, so a refused payload is left as it was found. This is the "trial-open before destroying the old one" arm of spec-gaps issue 04's ruling; there is therefore no post-install rollback row, because the install never starts against a payload that cannot open. |
| Outcome variant | `RecoveryPhase::InstallStagedCheckpoint` |
| Forced by | `test_load_staged_checkpoint_incomplete_dir_refuses_and_preserves_data`, `incomplete_staged_checkpoint_is_refused_without_touching_live_db`, `verify_requires_a_current_pointer_that_resolves_to_a_manifest`, `a_current_pointer_that_resolves_to_nothing_is_not_a_payload`, `test_load_staged_checkpoint_refuses_a_payload_rocksdb_cannot_open` |
| Bug refs | `.scratch/spec-gaps/issues/done/04-staged-checkpoint-install-verification.md` (H4: the guard was one `CURRENT.exists()` and a failed install had no way back) |

## FM-PERSISTENCE-025 — the install is idempotent across both of its crash windows

| Field | Value |
|---|---|
| Trigger | A crash between the two renames of an install (`<data-dir>/db` → `<data-dir>/backup/<db>_backup_<ts>`, then `<data-dir>/staging` → `<data-dir>/db`), or after the second, followed by a reboot. |
| Observable | Crash in the middle: the next boot finishes the install cleanly and the previous data is still recoverable from the backup directory. Crash after: the staged name is gone, so the next boot is a no-op with no spurious backup. An absent staged directory is a quiet no-op rather than an error. |
| NOT observable | A double install; a backup made for a no-op boot; a boot that hangs or errors because a previous install was interrupted. |
| Invariant | The staged directory's *name* is the state: rename 2 consumes it, so completion is self-evident and the operation is naturally idempotent. Rename 1 preserves rather than deletes, so the pre-install database survives a bad checkpoint. |
| Outcome variant | `RecoveredState::installed_staged_checkpoint` |
| Forced by | `test_load_staged_checkpoint_crash_after_backup_recovers`, `test_load_staged_checkpoint_idempotent_after_success`, `test_load_staged_checkpoint_installs_and_backs_up_old_db`, `test_load_staged_checkpoint_first_sync_no_existing_db`, `test_load_staged_checkpoint_absent_marker_is_noop`, `staged_checkpoint_is_installed` |
| Bug refs | none |

## FM-PERSISTENCE-026 — exactly one previous-database backup survives an install

| Field | Value |
|---|---|
| Trigger | Repeated staged-checkpoint installs (every replica full sync is one), each of which renames the live database aside. |
| Observable | After a successful install, exactly one `<db>_backup_<ts>` remains under `<data-dir>/backup` — the newest. Older ones are deleted. A leftover backup from an interrupted install is kept rather than pruned away, and never blocks a new install. |
| NOT observable | Backups accumulating without bound (before retention existed, every full sync leaked a complete copy of the database, forever). `_backup_2` outranking `_backup_10` — the suffix is compared numerically, not lexically. Files that merely share the prefix being deleted; only directories are backups. A pruning failure failing the install — retention is hygiene. |
| Invariant | `BACKUP_RETENTION = 1`: one generation covers the story the backup exists for (recover the immediately-previous database if a sync installed bad data); every additional generation is a full database copy with no recovery story attached. Retention that thin only holds because the install never consumes a payload it cannot open ([FM-PERSISTENCE-024](#fm-persistence-024--an-incomplete-staged-checkpoint-is-refused-without-touching-the-live-database)), so the single generation is a rollback for *bad data*, not for a broken copy. **Known gap**: the ordering key is still `unix_secs`, so a clock stepped backwards between two installs inverts the order and retention deletes the generation it should keep, and two installs inside one second collide on the name. Replacing it with a persisted monotone counter — and making prune refuse rather than delete a name it cannot parse — is [spec-gaps issue 27](../.scratch/spec-gaps/issues/open/27-backup-naming-monotonic-counter-prune-refuses-unparseable.md), which owns that change by ruling. |
| Outcome variant | n/a (warn-only on failure) |
| Forced by | `test_load_staged_checkpoint_prunes_older_backups`, `test_load_staged_checkpoint_crash_recovery_keeps_lone_backup`, `test_prune_backups_orders_numerically_not_lexically`, `test_prune_backups_noop_within_retention` |
| Bug refs | none |

## FM-PERSISTENCE-056 — a checkpoint payload carries a manifest of its own files

| Field | Value |
|---|---|
| Trigger | A checkpoint payload arrives somewhere that must decide whether to trust it: a snapshot restored by an operator copy (FM-PERSISTENCE-021) staged as `<data-dir>/staging`, an off-host backup that has been sitting on bit-rotting media, or an operator asking `frogctl backup checkpoint-verify` before an outage rather than during one. The damage is the kind a directory copy leaves: a file that stopped partway, a file that never arrived, a byte that flipped. |
| Observable | The snapshot stager writes `frogdb_payload.json` **inside** `checkpoint/` — every file in the payload by path relative to the payload root, with its size and xxh3-64 checksum — fsynced, and its directory entry fsynced, before the promotion rename publishes the snapshot. At install, every listed file must be present at its recorded size or the staged directory is refused before any rename (FM-PERSISTENCE-024), with the error naming the file that disagrees. `frogctl backup checkpoint-verify <dir>` runs the same check on demand and additionally re-reads and re-hashes every file, exiting non-zero when the payload does not verify and reporting whether a manifest was present at all. |
| NOT observable | A verification that reports success while quietly having checked nothing: a payload with no manifest is reported as *structure-checked only*, never as verified. A same-size corruption passing the checksum pass (that is the case sizes alone cannot see, and the reason the checksums exist). A manifest that describes a different directory than the one it sits in — paths are relative to the payload root, so a restore that moves the tree does not invalidate it. The manifest listing itself. A damaged manifest being ignored as if absent: absent is a legitimate state (an older backup, or a full-sync payload, which is checksummed on the wire instead), but unparseable is a refusal. Unlisted *extra* files failing verification — RocksDB writes its own `LOG` into a directory it opens, including the trial open, so a payload that gained a file is not thereby corrupt. Checksums on the install path: it verifies sizes, because a boot must not re-read the entire database, and the trial open is the stronger check it runs instead. |
| Invariant | The manifest is written by the snapshot stager only. Full-sync payloads deliberately do not carry one — that path already checksums each file on the wire — and `create_checkpoint` stays the cheap hard-link operation it is. Placement is inside the payload, not in `metadata.json`, precisely because the restore path that most needs it copies `snapshot_NNNNN/checkpoint/.` and nothing else; a manifest in `metadata.json` would be left behind by the trip it exists to protect. Verification is one implementation (`rocks::payload::verify_payload`) shared by the install and by `frogctl`, so the operator's pre-restore check and the node's boot-time check cannot drift apart. |
| Outcome variant | `PayloadError::{MissingFile, SizeMismatch, ChecksumMismatch, MalformedPayloadManifest, UnsupportedPayloadManifest}` → `io::ErrorKind::InvalidData`; `frogctl backup checkpoint-verify` exit code `1` |
| Forced by | `stager_writes_a_payload_manifest_inside_the_published_checkpoint`, `a_payload_manifest_catches_a_missing_or_truncated_file`, `only_the_checksum_pass_catches_same_size_corruption`, `an_absent_manifest_verifies_but_a_damaged_one_refuses`, `the_manifest_names_every_file_relative_to_the_payload_root`, `test_load_staged_checkpoint_refuses_a_payload_that_disagrees_with_its_manifest` |
| Bug refs | `.scratch/spec-gaps/issues/done/04-staged-checkpoint-install-verification.md` (A4: the backup path's integrity story stopped at "a marker file exists") |

## FM-PERSISTENCE-057 — staging and backup live inside the data directory; the mount point is never renamed

| Field | Value |
|---|---|
| Trigger | A full sync or an operator restore on a deployment where `persistence.data-dir` *is* a mount point — the standard Kubernetes layout, where the PVC is mounted at the configured path. The install and the staged download both run against a directory that cannot be renamed and whose parent is somebody else's filesystem (a container's ephemeral root). |
| Observable | The whole staging and install protocol reads and writes inside `<data-dir>` only: the download lands in `<data-dir>/staging` (scratch in `<data-dir>/staging.incoming`, a promotion-disarmed copy in `<data-dir>/staging.discarded`), rename 1 moves `<data-dir>/db` into `<data-dir>/backup`, rename 2 moves `<data-dir>/staging` onto `<data-dir>/db`. Both renames have both operands inside the mount, so both are same-filesystem and atomic. An install completes normally even when the data directory itself refuses every rename. |
| NOT observable | `<data-dir>` appearing as either operand of a rename — on a mount point that is `EBUSY`, which would make full resync permanently impossible on that node. Any path outside `<data-dir>` being created, written, renamed or synced by the staging or install path: a staged download into the parent writes a complete second copy of the database onto whatever filesystem happens to be mounted there (in a container, the node's ephemeral root, which it can fill). A precondition that the *parent* of the data dir is writable, or that a rename across it is atomic — the only filesystem requirement is the one the operator already provisioned. |
| Invariant | `DataDirLayout` (`frogdb-persistence/src/data_dir.rs`) is the single owner of the four paths; nothing else joins a layout name onto a path, and no party derives a location from `data_dir.parent()`. The identity marker stays at `<data-dir>/frogdb_data_dir`, a sibling of `db/` rather than a file inside it, so an install never moves it — see [FM-PERSISTENCE-049](#fm-persistence-049--the-data-directory-carries-an-identity-marker-published-atomically). The emptiness question phase 0 asks ([FM-PERSISTENCE-048](#fm-persistence-048--a-genuinely-empty-data-directory-is-a-first-boot-and-files-are-what-empty-means)) skips the install scratch names (`staging*`, `backup`) and *only* those, so an operator restore staged into a fresh data directory is still a first boot while an unmarked directory holding a real `db/` still refuses ([FM-PERSISTENCE-051](#fm-persistence-051--a-populated-data-directory-with-no-marker-refuses-the-boot)). Pre-alpha: there is no compatibility shim for the old flat layout and no migration — the campaign ruled that out explicitly. etcd and CockroachDB confine snapshot staging inside the store directory for the same reason. |
| Outcome variant | n/a (a layout violation is a test failure, not a runtime outcome) |
| Forced by | `staged_install_touches_no_path_outside_the_data_dir`, `install_succeeds_when_the_data_dir_itself_cannot_be_renamed`, `the_layout_puts_db_staging_and_backup_inside_the_data_dir`, `staged_paths_are_the_cross_crate_contract`, `foreign_files_ignores_the_install_scratch_but_not_the_database`, `staged_checkpoint_install_fsyncs_every_directory_it_changes`, `the_stager_stages_inside_the_data_dir` |
| Bug refs | `.scratch/spec-gaps/issues/done/21-staging-and-backup-live-inside-the-data-dir.md` (distsys-review MAJ-18) |

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
| Invariant | The order is the body of `recover`, readable top to bottom, with the three real dependencies stated in the phase modules: the marker verdict must precede everything that writes; the install is filesystem rename surgery that must precede the open; and the install is what carries `replication_metadata.json` into the data dir for phase 5 to find. Phase 0 is split around the install — decide before, stamp after — because the data directory itself may not exist until the install creates it, and a marker cannot be published into a directory that is not there ([FM-PERSISTENCE-049](#fm-persistence-049--the-data-directory-carries-an-identity-marker-published-atomically)). Phases 0-4 are gated on RocksDB-backed persistence; phase 5 is role-gated and phase 6 cluster-gated, both independent of persistence. |
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
| Invariant | `frogdb_persistence::data_dir::contains_foreign_files` walks the tree and answers on the first *file* at any depth; directories recurse, a missing directory contains nothing, and symlinks count as content without being resolved. It skips exactly the top-level install scratch names the layout owns — `backup`, `staging`, and `staging.*` ([FM-PERSISTENCE-057](#fm-persistence-057--staging-and-backup-live-inside-the-data-directory-the-mount-point-is-never-renamed)) — and nothing else: a payload an operator staged into a fresh data directory is not evidence of a wrong directory, while `db/` is still scanned in full so an unmarked *database* refuses. `NotFound` is the only listing failure that answers "no files": every other one (a `data-dir` that is a file, a permissions error, an IO error) propagates, because a directory whose contents are unknown must not read as empty. It is a first-file-exists probe, so the cost is bounded by the answer rather than by the tree. The rule is deliberately about what *this process did not write* rather than an allowlist of tolerated names, which would need an entry per orchestrator. |
| Outcome variant | n/a (the `RecoveryPhase::VerifyDataDir` success path) |
| Forced by | `contains_files_counts_files_at_any_depth_and_ignores_empty_directories`, `contains_files_treats_a_symlink_as_content`, `contains_files_propagates_a_listing_failure_that_is_not_absence`, `foreign_files_ignores_the_install_scratch_but_not_the_database`, `a_fresh_data_dir_boots_and_stamps_the_marker`, `pre_created_empty_subdirectories_are_still_a_first_boot`, `fresh_boot_creates_empty_shards` |
| Bug refs | `.scratch/hardening/issues/done/11-wrong-data-dir-boots-as-a-fresh-database.md` |

## FM-PERSISTENCE-049 — the data directory carries an identity marker, published atomically

| Field | Value |
|---|---|
| Trigger | Any RocksDB-backed boot: the marker is (re)written after the staged-checkpoint install, in `RecoveryPhase::VerifyDataDir`'s second half. |
| Observable | `frogdb_data_dir` in the data directory, holding a generated 128-bit `database_id`, `created_at_unix_ms`, and `layout_version`. The id is stable across restarts **and** across a full resync that replaces the whole database: it names the directory, not its contents. An `INFO` line reports the id on every verified boot. |
| NOT observable | A marker that survives a crash mid-write as half a file (it is written to `frogdb_data_dir.tmp`, fsynced, renamed, and the directory fsynced — the same rule the checkpoint publishers follow, [FM-PERSISTENCE-023](#fm-persistence-023--the-renames-that-publish-a-checkpoint-are-fsynced-on-both-sides)). A `.tmp` left behind by a failed publish, which would itself be *content in an unmarked directory* and so refuse the next boot under FM-PERSISTENCE-051. A directory that comes out of a full resync unmarked, or one that comes out of it carrying a *different* id: the marker is a sibling of `db/`, so the install's renames move the database and never the identity ([FM-PERSISTENCE-057](#fm-persistence-057--staging-and-backup-live-inside-the-data-directory-the-mount-point-is-never-renamed)); the stamp is unconditional and runs *after* the install only because the directory may not exist until the install creates it. Two directories sharing an id. |
| Invariant | `DataDirMarker::mint` is not a `Default`: every call yields a different value. `verify` decides which marker the directory ends up with (an existing one, or a freshly minted one) and `stamp` writes that decision, so the id an installed checkpoint inherits is the *directory's* prior id rather than a new one. The id is recorded for a future cross-check ("this node came up on a directory that is not the one it had last time"); nothing compares it across boots yet, because that needs out-of-band expected-id state this round did not add. The first consumer is *named*, not open-ended: [replication-correctness issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md), addendum R4 — demote `replication_state.json` from identity authority to a cache keyed on `database_id` plus the recovered sequence, ignored whenever either mismatches the opened database. That issue owns the comparison because it owns the expected-id state; adding a comparison here first would mean inventing a config knob with no reader ([issue 05](../.scratch/spec-gaps/issues/done/05-persistence-advisory-sweep.md) deferred it deliberately). [Cluster issue 35](../.scratch/cluster-correctness/issues/open/35-node-identity-outlives-the-process.md) reuses this mint-once-and-stamp *shape* for node identity but is not a consumer of this id. |
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
| NOT observable | Records *after* a corruption being replayed anyway, which can resurrect stale values and reorder history. A recovered database in which the hot and warm column families of the same shard silently disagree about a key because FrogDB's own flush surface left one durably ahead of the other (spec-gaps issue 03): `RocksStore::flush()` — the only production path that flushes proactively, driven by `durable_sync` — now names every CF it manages (main, warm, search-meta) in one grouped `flush_cfs_opt` call under `atomic_flush` ([state-space](#state-space)), so a flush initiated through that surface cannot leave one CF durably ahead of a sibling still relying on WAL replay; a tiering demotion's `(warm put, hot delete)` pair is therefore always captured as one unit. This guarantee is scoped to that surface: a hand-rolled single-CF flush issued outside it is not reachable from production code (the only in-tree caller of a per-CF flush is the test-only `sync_wal` harness helper, gated `cfg(any(test, feature = "test-support"))`, used to force a durable point deliberately in crash-recovery test setup) — and was confirmed empirically to still be able to produce the same cross-CF divergence, with `rocksdb_paranoid_checks` ([state-space](#state-space)) refusing the reopen on some corruption shapes but not others (a clean truncation at the divergence boundary was observed to open and resurrect silently). That residual gap is RocksDB's own cross-CF detection acting on a precondition unreachable from FrogDB's shipped code paths, not one this issue closes or that an in-tree test can responsibly pin as guaranteed behavior. |
| Invariant | `DBRecoveryMode::PointInTime` is set explicitly at open — pinned by a test, not inherited from the library default, so a rocksdb upgrade cannot change FrogDB's crash semantics silently. This mirrors Redis' `aof-load-truncated` / point-in-time AOF loading: replay to the first inconsistency, then stop. `AbsoluteConsistency` and `SkipAnyCorruptedRecord` were both considered and rejected for the reasons above. Point-in-time truncation is a single cut across the *whole* WAL, but a database is multiple column families (`shard_<n>`, `warm_<n>` when tiering is on, search-meta). Two changes (spec-gaps issue 03) close the gap through which FrogDB's own operation could leave one CF durably ahead of a sibling still WAL-only: `atomic_flush` (`db_opts.set_atomic_flush(true)`, pinned alongside the recovery mode) guarantees every CF named in one flush call — and, per RocksDB's own documented contract, every automatic background flush — commits at the same consistent sequence point; and `RocksStore::flush()` now names every CF it manages (main, warm, search-meta) in that one call, where it previously covered only the main tier, leaving a demotion's warm copy WAL-dependent no matter how many `durable_sync` ticks ran. Together they mean a tiering demotion's `(warm put, hot delete)` pair is captured as one unit by FrogDB's own flush, so the divergent SST-ahead-of-WAL precondition cannot arise from ordinary operation; see NOT observable for the scope of that guarantee and the residual gap outside it. |
| Outcome variant | n/a (see FM-PERSISTENCE-035 for the signal) |
| Forced by | `wal_truncation_recovers_prefix_and_signals`, `wal_mid_log_bitflip_drops_suffix_and_signals`, `wal_recovery_mode_is_pinned_to_point_in_time`, `atomic_flush_is_pinned_on`, `flush_covers_the_warm_tier_not_just_main`, `test_unclean_shutdown_recovery`, `test_wal_recovery_despite_snapshot_issues`, `test_repeated_crash_cycles` |
| Bug refs | `.scratch/testing-improvements/issues/14-wal-recovery-mode-pin.md` (done — the pin test is its outcome); `.scratch/spec-gaps/issues/done/03-enable-rocksdb-atomic-flush.md` (done — `atomic_flush` + full-tier `flush()` coverage close the cross-CF divergence window for FrogDB's production flush surface; the residual single-CF-bypass gap outside that surface is RocksDB-internal and unreachable from shipped code, see NOT observable) |

## FM-PERSISTENCE-035 — silently dropped WAL records raise a counter, and the detector never false-alarms

| Field | Value |
|---|---|
| Trigger | Point-in-time recovery discards records (FM-PERSISTENCE-034) that a durable sync had already covered. |
| Observable | `frogdb_wal_recovery_dropped_records_total` increases by the number of dropped sequence numbers and a `WARN` names the count. A clean reopen raises nothing. The watermark is then re-baselined, so the same loss is not re-reported on every subsequent boot. |
| NOT observable | A false alarm. The watermark is written best-effort (temp + rename, never fsynced) and can therefore only *lag* the true durable sequence, so the detector under-reports rather than over-reports: a torn or garbage watermark file parses as absent and reports nothing. Telling an operator they lost data when they did not is the worse failure, and this asymmetry is chosen deliberately. This also rules out a *racy over-claim*: `record_wal_watermark` takes the covered sequence as an argument, snapshotted by the caller *before* the write or flush it reports as complete, never a fresh `latest_sequence_number()` read taken after the fact — a concurrent shard's still-unsynced write landing in the gap between another shard's fsync and a post-hoc read can no longer inflate the claimed durable point past what is actually on disk. |
| Invariant | `frogdb_wal_watermark` holds the durable-sync high-watermark as a decimal `u64`; `detect_and_reset` compares it against `latest_sequence_number()` at open. It is advanced, via `fetch_max` against whatever is already recorded, only after something durable — a successful *sync* commit (the pre-write snapshot `RocksSink::commit` took, per the "before, not after" rule above), an out-of-band `durable_sync` (the same pre-flush snapshot `publish_synced_through` uses), or the open-time (re-)baseline — never by a checkpoint, which does not truncate or reclaim the WAL. The `fetch_max` matters because independent concurrent callers (each shard's own sync commit, plus the periodic tick) report out of order; a lower snapshot arriving after a higher one must not regress the mark. |
| Outcome variant | `frogdb_wal_recovery_dropped_records_total` |
| Forced by | `wal_clean_reopen_recovers_all_without_signal`, `test_wal_sequence_persistence`, `test_wal_sequence`, `test_async_wal_writer_recovery`, `the_watermark_file_round_trips_and_degrades_to_none`, `a_recovery_that_reaches_the_watermark_reports_nothing`, `a_short_recovery_reports_the_gap_once_and_re_baselines`, `only_a_synced_rocks_sink_commit_records_the_durable_watermark`, `a_racing_shard_write_after_the_fsync_does_not_inflate_the_watermark` |
| Bug refs | `spec-gaps/issues/done/12-wal-watermark-carries-covered-sequence.md` — `record_wal_watermark` read `latest_sequence_number()` after the fact instead of carrying a pre-write/pre-flush covered sequence, letting a concurrent shard's unsynced write racing in between another shard's fsync and that post-hoc read inflate the watermark past what was actually durable |

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
| Observable | The server starts. Unreadable file → no functions and a `WARN`. Readable file with one bad library → the others still load, per-library, with a `WARN` naming the failed one. Either way the loss is *counted*: `frogdb_recovery_functions_failed_total` increments and `INFO persistence` reports `functions_last_load_failed` for the life of the process, next to `rdb_last_load_keys_failed` (the same argument as [FM-PERSISTENCE-033](#fm-persistence-033--an-undecodable-key-is-skipped-counted-and-surfaced--never-fatal-on-its-own), one layer up: a `FUNCTION LIST` that came back smaller cannot say by itself whether it shrank). |
| NOT observable | A database refusing to start because of a stored script — the keyspace is the valuable thing and a function library is recoverable by re-uploading it. A partial parse silently registering a truncated library body. And the mirror image of the tolerance: a *well-formed* `functions.fdb` coming back empty, which would make a valid library disappear exactly as silently as a corrupt one. Equally not observable now: a *silent* loss — a boot that dropped a library reporting the same `functions_last_load_failed:0` as a boot that never had one. The inverse also holds: a missing `functions.fdb` is the ordinary fresh boot and counts nothing. |
| Invariant | Phase 4 cannot fail: every read/parse error is downgraded to `Ok((Vec::new(), 1))`. The count is `1` per downgraded *file*, not per library it held — the file did not parse, so its library count is exactly what is unknown. Compilation is separate, happens in the wiring layer, and is isolated per library; each library that fails to parse or to register there adds `1` to the same `RecoveryStats::functions_failed` and to the same counter, so one number covers both halves of a phase that is split across two crates. The downgrade is on the *error* arm only — a successful read returns the stored `(name, source)` pairs verbatim for the wiring layer to register. |
| Outcome variant | `RecoveryStats::functions_failed` → `frogdb_recovery_functions_failed_total` + `functions_last_load_failed` |
| Forced by | `corrupt_functions_file_is_tolerated`, `persisted_functions_are_restored`, `a_corrupt_functions_file_is_counted_and_exported`, `a_boot_with_no_functions_file_counts_no_failures` |
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
| Bug refs | `.scratch/hardening/issues/08` (fixed — the consume was unconditional while the save was warn-only); `.scratch/testing-improvements/issues/67` (fixed — the inverse case: a persistence-disabled primary handed over a replid and offset with no dataset behind them, so there was no staged metadata to reconcile and the replica kept a stale keyspace; that primary now ships its live dataset, see [FM-REPLICATION-001](replication.md)) |

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
| FM-PERSISTENCE-005 | A WAL failure under the default `continue` policy acknowledges the write; the server keeps accepting writes indefinitely | The first failed background save flips `stop-writes-on-bgsave-error`, and every subsequent write is refused with `-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk` | Unchanged for the *WAL* failure this row is about: there is no snapshot involved, so no snapshot policy can gate it, and the only refusal remains per-write `-IOERR` under the non-default `wal-failure-policy = rollback` (FM-PERSISTENCE-006). The adjacent gap — a persistently broken disk observed by the snapshot task — is now closed as an opt-in (FM-PERSISTENCE-046). Note the scope after spec-gaps issue 01: `durability-mode = sync` now *waits* for the commit under every policy (FM-PERSISTENCE-002), so this row is no longer a silent loss window for `sync` + `continue` on a **crash** — only on an observed WAL **failure**, where `continue` acknowledges the failed wait by design. That remaining ack-the-loss default is now bounded rather than open-ended (spec-gaps issue 02): the first loss poisons the shard (FM-PERSISTENCE-053) and truncates its persisted stream (FM-PERSISTENCE-054), so `continue` acks a *suffix* of lost writes instead of an unbounded scatter, and an operator who wants Redis' refusal has `wal-failure-policy = readonly` (FM-PERSISTENCE-055) — a fail-stop for a failing *WAL*, which Redis has no equivalent of because its `MISCONF` is keyed on the RDB/AOF save. |
| FM-PERSISTENCE-022 / FM-PERSISTENCE-046 | `INFO persistence` reports the real save outcome (`rdb_last_bgsave_status`, `rdb_last_bgsave_error`, `rdb_saves`, `rdb_bgsave_failures`); a failing save refuses writes with `-MISCONF` only under `snapshot.stop-writes-on-save-error`, **default off** | `rdb_last_bgsave_status:err` *and*, with the default `stop-writes-on-bgsave-error yes`, every subsequent write refused with `-MISCONF` | The status field matched Redis in phase 2c (issue 03); the refusal itself matched in this round, but the *default* is deliberately inverted (issue 09). In Redis the RDB/AOF is the durability mechanism, so a failed save means writes are not durable and refusing is conservative. In FrogDB the WAL is the durability mechanism (FM-PERSISTENCE-002/003/004) and a snapshot is a backup artifact (FM-PERSISTENCE-021), so a failed save means backups are stale — refusing writes for that is a bigger hammer than it is in Redis, and is offered as a parity checkbox rather than imposed. `rdb_last_bgsave_error` and `rdb_bgsave_failures` are FrogDB additions — Redis exposes only the status, which tells an operator that saves broke but not why. |
| FM-PERSISTENCE-047 | `recovery.on-decode-failure` (`continue` default / `refuse`) decides whether a single undecodable value refuses the boot | No equivalent: a value Redis cannot load is fatal — a corrupt RDB aborts the load outright, and `aof-load-truncated no` is the closest knob, which is about a torn *tail* rather than about individual values | FrogDB decodes per key out of RocksDB rather than replaying one file, so "skip the bad value and keep the other ten million" is a real option Redis does not have; `continue` takes it. `refuse` exists for operators who would rather not serve a silently smaller keyspace. Binary on purpose — a percentage threshold needs a force-boot override and a replica policy, neither of which is settled. |
| FM-PERSISTENCE-048..052 | The data directory carries an identity marker; one holding files FrogDB did not write refuses to boot, and `persistence.require-existing-data` can extend the refusal to an empty directory | Redis starts against any `dir`: it loads `dump.rdb`/the AOF if they are there and otherwise comes up with an empty keyspace, so a mistyped `dir`, a failed mount, or a lost bind mount is a silent fresh start that then begins saving over nothing | A FrogDB data directory is a live RocksDB the server writes to continuously, not a file it loads once at boot — the window in which a wrong directory quietly becomes the real one is every second the process runs, rather than the moment of a save. The marker is the evidence Redis' layout does not need and FrogDB's does. The empty-directory case stays a silent success by default because container orchestration pre-creates mount points; `require-existing-data` exists because an unmounted volume is indistinguishable from a first boot by inspection, and only the deployment knows which it is. |
| FM-PERSISTENCE-022 | The shard-local `INFO persistence` the *scripting* path renders (`redis.call('INFO')`) still emits static `rdb_last_load_keys_expired`/`rdb_last_load_keys_loaded` placeholders (recovery is aggregated node-wide, not per-shard) | One INFO implementation for every caller | Fixed for the save-outcome block in issue 10: both renderers now read `persistence_snapshot_fields(&SnapshotStats, bgsave_in_progress)` off the same `SnapshotCoordinator` every shard shares, so `rdb_last_bgsave_status`/`rdb_saves`/etc can no longer disagree. The two load-count fields remain a narrower, known gap — the script path runs inside a shard worker with no per-shard `RecoveryStats` handle — tracked by issue 42. |
| FM-PERSISTENCE-021 | A `BGSAVE` artifact is never loaded at boot; restoring it is an explicit operator install into a fresh data dir via `<data-dir>/staging` | Redis loads `dump.rdb` at startup | FrogDB restarts recover from RocksDB + its WAL, which is strictly more current than the last snapshot. A snapshot is a *backup artifact*, not the boot path; auto-loading one would roll the keyspace back to the last cut. |
| FM-PERSISTENCE-015 | `BGSAVE` while a save is running replies with the `+` simple string `Background save already in progress` | `-ERR Background save already in progress` | A RESP client that treats only `-` replies as errors reads a rejected `BGSAVE` as success. Pinned as-is rather than fixed, because the wire shape is observable by raw-protocol clients; changing it is a separate, testable change (issue 45). |
| — | `SAVE`, `BGREWRITEAOF`, and `SYNC` are explicit `-ERR ... not supported` stubs | All three are supported | There is no AOF and no synchronous save path: durability is continuous WAL persistence, and `BGSAVE` is the only snapshot verb. The stub text points at `BGSAVE` rather than failing opaquely. |
| — | Snapshot cadence is a single `snapshot-interval-secs` timer, live-retunable, with no dirty-key threshold | `save <seconds> <changes>` save points, several of them, each a (time, change-count) pair | FrogDB's WAL already bounds durability loss (FM-PERSISTENCE-002/003/004), so the snapshot's job is periodic backup artifacts, not the durability mechanism Redis' save points implement. A tick during a running save is dropped, never queued. |
| FM-PERSISTENCE-034 | Point-in-time WAL replay: truncate at the first inconsistency and open | `aof-load-truncated yes` (default) truncates a torn AOF tail and starts | Matched deliberately. The divergence is what happens *behind* a mid-log corruption: FrogDB discards the valid suffix (RocksDB's point-in-time mode), where Redis with `aof-load-truncated no` would refuse to start. FrogDB has no equivalent strict knob. |
| FM-PERSISTENCE-023 | Checkpoint publication fsyncs the staged payload before each publishing rename and the containing directory after it | Redis fsyncs the RDB file and renames it into place | Matched deliberately, and extended: Redis has one file to sync, FrogDB publishes a directory tree, so the directory entry — not just the payload — is the thing that must survive. Was a real power-loss gap (issue 04 / issue 74) until phase 2c. |
