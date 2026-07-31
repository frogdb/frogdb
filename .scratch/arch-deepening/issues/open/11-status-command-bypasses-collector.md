# 11 — `STATUS JSON` Redis command bypasses StatusCollector; hardcodes fields

Status: ready-for-human

## What to build

The `STATUS JSON` connection command (`connection/observability_conn_command.rs::status_json`)
is a hand-rolled duplicate of the telemetry `/status` endpoint that predates issues 01/03. It
does its own `gather_memory_stats` scatter and hardcodes what it cannot reach:
`frogdb.uptime_secs: 0`, `cluster.mode: "standalone"` (even in cluster mode),
`health.status: "healthy"` unconditionally, `memory.limit_bytes: 0`,
`memory.fragmentation_ratio: 1.0`, `persistence.enabled: false` (even with persistence on),
`keyspace.expired_keys_total: 0`, `commands.total_processed: 0`, `commands.ops_per_sec: 0.0`.

Issue 01 fixed exactly these stubs in the HTTP `/status` path (`StatusCollector`), and issue 03
put a single `NodeStateSnapshot` collect() behind INFO + `/status` + debug UI — this command is
now the last observability surface that can disagree with the others, violating the accuracy
rule (misleading data is not acceptable).

End-to-end: `redis-cli STATUS JSON` and the HTTP `/status` endpoint report the same live values.
Preferred shape: render the command from `StatusCollector`/`NodeStateSnapshot` rather than
re-sourcing fields ad hoc. The command's `ConnCtx` has no recorder/collector today — thread one
in (provider trait on `ConnCtx`, or construct the collector at registration like the HTTP
endpoint does). Fields that genuinely cannot be sourced accurately at this seam are omitted or
`null`, not faked (precedent: issue 01's `ops_per_sec`). Wire-format changes to the JSON are
acceptable (unreleased software) — accuracy wins over shape stability; note removals in the
issue comment.

## Acceptance criteria

- [x] `STATUS JSON` output agrees with telemetry `/status` for every shared field (test: bump a
      counter / enable persistence / run in cluster mode, assert both surfaces agree)
- [x] No hardcoded field values remain in `status_json` (each field sourced or omitted/`null`
      with a rationale comment)
- [x] One rendering source: the command consumes `StatusCollector`/`NodeStateSnapshot`; no
      second hand-assembled scatter path remains
- [x] `just test frogdb-server` observability tests green

## Blocked by

None - can start immediately (issues 01 + 03 already merged)

## Comments

### Resolution (2026-07-20)

`STATUS JSON` now renders from the **same** `frogdb_telemetry::StatusCollector` the HTTP
`/status` endpoint uses. The hand-rolled `gather_memory_stats` scatter + `serde_json::json!`
field assembly in `status_json` is gone; the executor is one line: `ctx.status.status_json().await`.

**Seam.** Added a core `StatusProvider` trait (`conn_command.rs`) mirroring `InfoProvider` — the
whole gather-and-render runs server-side behind one method. `ConnCtx` gains a `status` field
(no-op default `NoopStatusProvider`, live one layered via `ConnCtx::with_status` on the read-only
dispatch path). `ConnectionHandler` implements `StatusProvider` in a new `connection/status_handler.rs`,
delegating to a single `render_status_json(collector)` = `collector.collect()` + `to_json`.

**One collector, all configs.** `StatusCollector` now holds `Arc<dyn MetricsRecorder>` instead of
the concrete `Arc<PrometheusRecorder>`, so it is built **unconditionally** in `start_subsystems`
(previously only inside the `http.enabled` block) and shared into both the HTTP server
(`with_status_collector`) and `ObservabilityDeps.status_collector`. Added `gauge_value` to the
`MetricsRecorder` trait (default `None`; implemented on `PrometheusRecorder`) to complete the
counter/gauge readback pair (`counter_value` already existed for INFO). When metrics are disabled
the no-op recorder reports absent counters as `0`/`None` — accurate, never faked.

**Removed the nine hardcoded stubs.** `frogdb.uptime_secs` (real elapsed), `cluster.mode` (real
role), `health.status` (real issue detection), `memory.limit_bytes` (real maxmemory),
`memory.fragmentation_ratio` (RSS-derived), `persistence.enabled`/`durability_mode`/`snapshot`/`wal`
(real config + counters), `keyspace.expired_keys_total` (summed shard stats),
`commands.total_processed` (recorder counter) all now flow from the collector.

**Wire-format changes (unreleased; accuracy over shape stability):**
- `commands.ops_per_sec` — was always `0.0`; now **omitted** when unknown (no instantaneous-rate
  source on the status path), matching issue 01's `/status` precedent (`Option<f64>`).
- `memory.rss_bytes` — new optional field (present on Linux when RSS is readable).
- `persistence` — now a rich object (`durability_mode`, `snapshot`, `wal` with lag) when
  persistence is enabled, instead of the bare `{ "enabled": false }`.
- `frogdb.uptime_secs`, `cluster.mode`, `health.status`, `memory.limit_bytes`,
  `memory.fragmentation_ratio`, `keyspace.expired_keys_total`, `commands.total_processed` — same
  keys, now live values.

Also deleted the orphaned `connection/util.rs::format_timestamp_iso` (its only caller was the old
`status_json`; telemetry has its own `format_iso8601`).

**Tests (all green):**
- `frogdb-telemetry status` — 25 passed (verifies `dyn MetricsRecorder` swap).
- `frogdb-server status_json|observability|conn_command|status_` — 115 passed, incl. new
  `status_json_renders_from_shared_collector_and_agrees_with_http` (bumps `CommandsTotal`, enables
  persistence, sets mode `cluster`; asserts live values and section-for-section agreement with the
  collector's `/status` render).
- `frogdb-core conn_command` — 1 passed (`ConnCtx` default/override placeholders).
- `just check` / `just lint` clean on core, types, telemetry, server.
