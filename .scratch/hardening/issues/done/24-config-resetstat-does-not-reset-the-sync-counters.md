# `CONFIG RESETSTAT` does not reset `sync_full` / `sync_partial_ok` / `sync_partial_err`

Status: done
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

## Resolution

Fixed. `CONFIG RESETSTAT` now zeroes all three counters. New spec row **FM-REPLICATION-058**;
FM-REPLICATION-050's Observable gained the "since the last RESETSTAT" companion sentence.

- `frogdb-server/crates/replication/src/sync_counters.rs` — `SyncCounters::reset()` (the "no
  `reset()` yet" comment that pointed here is gone).
- `frogdb-server/crates/replication/src/tracker.rs` — `ReplicationTrackerImpl::reset_sync_counters()`,
  deliberately narrower than "reset the tracker": the replica registry, the offset and the
  lag-disconnect history are live state, not statistics.
- `frogdb-server/crates/core/src/conn_command.rs` — `ConnCtx::replication_tracker:
  Option<&ReplicationTrackerImpl>` + `with_replication_tracker`.
- `frogdb-server/crates/server/src/connection/conn_command.rs` — `base_ctx` layers it from
  `ClusterDeps`; `config_resetstat` resets the counters alongside the other five resets.

**Divergence from the suggested remedy (loud).** Remedy step 1 asked for a narrow `&dyn` provider
because "`ConnCtx` is a `core` type and must not depend on the replication crate". That premise is
false in the current tree: `frogdb-core`'s `Cargo.toml` already depends on `frogdb-replication`,
`lib.rs` re-exports `ReplicationTrackerImpl` from the crate root, and `CommandContext`
(`core/src/command.rs:1247`) already carries `Option<&Arc<ReplicationTrackerImpl>>`. A provider trait
would have added trait + no-op + layering method to express a dependency that already exists, and
would have made `ConnCtx` and `CommandContext` reach the same object two different ways. The
concrete handle was used instead.

**Remedy step 3 (audit).** `sync_counter_fields` is the only live replication data in either `INFO
stats` renderer, so nothing else was in this issue's position. The neighbouring
`total_net_repl_input_bytes` / `total_net_repl_output_bytes` are the literal `0` in both renderers
with no source at all — that is issue 20's class (a hardcoded literal shaped like data), not a
missing reset, and is recorded under FM-REPLICATION-058's audit note.

### Tests

Red-first: all six failed before the fix (the three counter-level ones verified red against a no-op
`reset()` body, not merely absent-method compile errors).

- `reset_zeroes_all_three_counters`, `counters_keep_counting_after_a_reset` (`sync_counters.rs`)
- `resetting_the_sync_counters_leaves_the_replica_registry_and_offset_alone` (`tracker.rs`)
- `config_resetstat_zeroes_the_sync_counters`,
  `config_resetstat_without_a_replication_tracker_is_ok` (`connection/conn_command.rs`) — the second
  is the issue's `config_resetstat_with_no_replication_tracker_is_ok`, renamed to match the
  `without_a_` phrasing of its `config_resetstat_with_no_shards_is_ok` twin
- `both_info_renderers_report_zeros_after_the_counters_are_reset` (`info/sections.rs`) — the issue's
  "assert all three read `0` through both `INFO` renderers", asserted through a real tracker rather
  than a hand-built snapshot

**Fixture bug found while wiring this**: `conn_command.rs`'s test `Fixture::ctx()` hand-copies
`ConnCtx::new` instead of calling `base_ctx`, so a field layered by `base_ctx` is invisible to every
test in that module. The new field is layered in both places and the divergence is now commented,
but the fixture remains a second spelling of the production builder.
