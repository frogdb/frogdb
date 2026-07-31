# 01 — Wire telemetry `/status` stub fields from their real sources

Status: ready-for-human

## What to build

Several fields in the telemetry `/status` JSON are hardcoded rather than sourced, while INFO
already reports the real values. End-to-end: an operator polling the Metrics/Admin `/status`
endpoint sees live numbers that agree with INFO.

- `WalStatus.total_writes` / `total_bytes` — hardcoded 0; INFO already sources
  `wal_writes_total`/`wal_bytes_total` from the `PrometheusRecorder` that `StatusCollector`
  holds (its `_recorder: Arc<PrometheusRecorder>` field is currently unused).
- `KeyspaceStatus.expired_keys_total` — hardcoded 0; source from the same recorder.
- `CommandsStatus.total_processed` / `ops_per_sec` — hardcoded 0; source from the recorder.
- `SnapshotStatus.in_progress` / `last_timestamp` — hardcoded; the pure `SnapshotScheduler`
  (round-8 P06) now exposes `in_progress()` and epoch/last-run state — thread it (or the
  coordinator's view of it) into `StatusCollector`.

Accuracy rule: values must be correct, not merely present — cross-check each against the INFO
renderer's source before wiring (see feedback: misleading observability data is not acceptable).

## Acceptance criteria

- [x] `/status` `wal.total_writes`/`total_bytes` match INFO's `wal_writes_total`/`wal_bytes_total` for the same workload
- [x] `expired_keys_total` and command counters report live values (test: run traffic, assert nonzero and consistent)
- [x] `SnapshotStatus.in_progress` flips true during a BGSAVE and back; `last_timestamp` reflects the last completed Snapshot
- [x] `StatusCollector._recorder` is either used or removed — no dead field
- [x] Serde round-trip tests updated; `cargo clippy -p frogdb-telemetry --all-targets -- -D warnings` clean

## Blocked by

None - can start immediately

## Source

Round-8 P02 agent report; `.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (step 2).

## Comments

### 2026-07-20 — implemented (ready-for-human)

Wired every listed `StatusCollector` field to its real source via the `_recorder`
it already held (renamed `_recorder` → `recorder`; no more dead field):

| `/status` field | source implemented |
| --- | --- |
| `wal.total_writes` | `WalWrites` counter (`recorder.get_counter_value`) — same counter INFO's `wal_writes_total` reads |
| `wal.total_bytes` | `WalBytes` counter — same as INFO's `wal_bytes_total` |
| `commands.total_processed` | `CommandsTotal` counter (summed across the `command` label) |
| `snapshot.in_progress` | `SnapshotInProgress` gauge (`> 0.5`) — the coordinator emits this from the round-8 `SnapshotScheduler` |
| `snapshot.last_timestamp` | `SnapshotLastTimestamp` gauge (`Some` iff `> 0`) |
| `keyspace.expired_keys_total` | sum of per-shard `ShardMemoryStats.expired_keys` |

Accuracy deviations from the issue's literal suggestion (both to satisfy the
"must agree with INFO / no misleading data" rule):

- **`expired_keys_total` is sourced from the per-shard accumulator, NOT the
  `frogdb_keys_expired_total` recorder counter.** That counter is only
  incremented on the *active* expiry cycle (`event_loop.rs`); lazy (on-access)
  expiry bumps only the store's `expired_keys` (`hashmap.rs::check_and_delete_expired`).
  Using the counter would under-count lazy expirations and disagree with INFO's
  `expired_keys` (which aggregates the same per-shard accumulator). `/status` now
  uses that same accumulator, so the two agree exactly.
- **`commands.ops_per_sec` is now `Option<f64>` and is omitted (`None`), not
  faked.** FrogDB has no server-wide instantaneous-rate sampler on the status
  path (per-shard windowed rates exist only in the debug hot-shard report).
  Reporting a lifetime average under an "instantaneous" name would be misleading,
  so the field is dropped from the JSON. This is the one criterion I could not
  fully satisfy with a live value — flagged rather than faked, per the accuracy rule.

Additional change for INFO agreement: INFO's client-facing
`total_commands_processed` was itself a hardcoded `0`. Wired it to the same
`CommandsTotal` counter (`InfoSources::total_commands_processed`) so INFO,
`/metrics`, and `/status` cannot disagree. INFO's `instantaneous_ops_per_sec`
stays a Redis-compat stub `0` (same missing-sampler reason as `/status`).

Also exported `frogdb_core::Envelope` so external crates can build mock shards
in tests.

Tests added (all pass):
- `status::tests::test_wal_and_command_counters_sourced_from_recorder` — emits
  `WalWrites`/`WalBytes`/`CommandsTotal`, asserts `/status` reflects the sums and
  equals `get_counter_value` (the INFO source).
- `status::tests::test_snapshot_status_sourced_from_recorder` — `in_progress`
  flips with the gauge; `last_timestamp` reflects the gauge / is `None` when unset.
- `status::tests::test_expired_keys_total_sums_shard_stats` — mock shards return
  `expired_keys` 12 + 8 → `/status` reports 20.
- `status::tests::test_ops_per_sec_omitted_not_faked` +
  `test_commands_status_serde_round_trip_omits_ops_per_sec` — field absent from JSON.
- `info::sections::tests::stats_total_commands_processed_reflects_the_shared_counter`
  — INFO reports 0 with metrics disabled, 7 after emitting to `CommandsTotal`.

Verification: `cargo clippy -p frogdb-telemetry --all-targets -- -D warnings` clean;
full `just check` (workspace, all targets) green; pre-commit lefthook fmt + clippy
+ info/metrics seam checks green.

Out of scope (noted): `connection/observability_conn_command.rs` has a *separate*,
hand-rolled `STATUS JSON` Redis-command implementation that does NOT use
`StatusCollector` and still hardcodes these same fields (and `persistence.enabled:false`).
It has no recorder in its command context. Worth a follow-up to route it through
`StatusCollector` (or delete the duplicate).
