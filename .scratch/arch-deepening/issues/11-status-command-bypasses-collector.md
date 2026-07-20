# 11 — `STATUS JSON` Redis command bypasses StatusCollector; hardcodes fields

Status: needs-triage

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

- [ ] `STATUS JSON` output agrees with telemetry `/status` for every shared field (test: bump a
      counter / enable persistence / run in cluster mode, assert both surfaces agree)
- [ ] No hardcoded field values remain in `status_json` (each field sourced or omitted/`null`
      with a rationale comment)
- [ ] One rendering source: the command consumes `StatusCollector`/`NodeStateSnapshot`; no
      second hand-assembled scatter path remains
- [ ] `just test frogdb-server` observability tests green

## Blocked by

None - can start immediately (issues 01 + 03 already merged)
