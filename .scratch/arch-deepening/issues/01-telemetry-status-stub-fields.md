# 01 — Wire telemetry `/status` stub fields from their real sources

Status: needs-triage

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

- [ ] `/status` `wal.total_writes`/`total_bytes` match INFO's `wal_writes_total`/`wal_bytes_total` for the same workload
- [ ] `expired_keys_total` and command counters report live values (test: run traffic, assert nonzero and consistent)
- [ ] `SnapshotStatus.in_progress` flips true during a BGSAVE and back; `last_timestamp` reflects the last completed Snapshot
- [ ] `StatusCollector._recorder` is either used or removed — no dead field
- [ ] Serde round-trip tests updated; `cargo clippy -p frogdb-telemetry --all-targets -- -D warnings` clean

## Blocked by

None - can start immediately

## Source

Round-8 P02 agent report; `.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (step 2).
