# `CONFIG RESETSTAT` does not reset `sync_full` / `sync_partial_ok` / `sync_partial_err`

Status: needs-triage
Type: gap (Redis parity)
Severity: likelihood 2/3 (any operator who resets before observing a window), consequence 1/3
(the counters are monotone and honest; they just cannot be zeroed) — score 2
Area: replication / INFO stats

## Problem

Redis zeroes these three in `resetServerStats`, alongside the command stats, error stats and
keyspace counters. FrogDB's `config_resetstat`
(`frogdb-server/crates/server/src/connection/conn_command.rs:336-347`) resets the shard stats, the
per-client call counts, `error_stats`, the latency histograms and the keyspace stats — and leaves
the sync counters alone, because it cannot reach them: `ConnCtx` (`core/src/conn_command.rs:629`)
carries no handle to the replication tracker, which is where `SyncCounters` lives
(deliberately — it is the one object both `INFO` renderers can reach in either role,
FM-REPLICATION-050).

Consequence: an operator who resets stats and then watches a maintenance window still reads
lifetime resync totals, and has to diff them by hand against a reading taken before. Nothing is
misreported — the numbers are true — so this is a parity gap, not the misleading-data class.

`SyncCounters` deliberately has **no** `reset()` method today: adding one without a caller would be
dead code claiming a capability the server does not have, which is the shape of issues 16 and 22.
The comment in `sync_counters.rs` points here.

## Suggested remedy

1. Give `ConnCtx` a replication handle. It needs a *narrow* one — a `&dyn` provider with a `reset`
   verb, in the shape of the existing `ConfigProvider` / `StatusProvider` seams, not the tracker
   itself; `ConnCtx` is a `core` type and must not depend on the replication crate.
2. Call it from `config_resetstat` alongside the other four resets, and add `SyncCounters::reset()`
   as the implementation.
3. While there: audit whether any other replication-owned `INFO stats` field is in the same
   position.

## Tests that should exist

- `config_resetstat_zeroes_the_sync_counters` — record one of each outcome, RESETSTAT, assert all
  three read `0` through both `INFO` renderers.
- `config_resetstat_with_no_replication_tracker_is_ok` — the provider is absent on a node that
  never installed replication; RESETSTAT must still reply `+OK` (the twin of
  `config_resetstat_with_no_shards_is_ok`).

## Spec impact

FM-REPLICATION-050's Observable says the counters are lifetime totals. Closing this issue adds the
reset as a second way they move, and the "a primary that has served no `PSYNC` reports all three as
`0`" sentence needs a companion for "and a primary that was reset reports `0` again".
