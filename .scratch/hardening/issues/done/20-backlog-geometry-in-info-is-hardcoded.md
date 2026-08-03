# Backlog geometry in `INFO replication` is hardcoded

Status: done
Type: bug (observability accuracy)
Severity: likelihood 3/3 (every `INFO replication` on every node reports these), consequence 2/3
(the two fields an operator sizes the backlog with are constants — one contradicts any tuned
`backlog-size`, the other contradicts the armed eviction floor) — score 6
Area: replication / INFO

## Problem

`repl_backlog_size` and `repl_backlog_first_byte_offset` are literals in **both** INFO renderers:

- `frogdb-server/crates/server/src/info/sections.rs:439-440` (primary branch) and `:463-464`
  (replica branch)
- `frogdb-server/crates/server/src/commands/info.rs:435-436` and `:477-478`

```rust
.field("repl_backlog_size", 1048576)
.field("repl_backlog_first_byte_offset", 0)
```

Two separate wrongnesses:

1. **`repl_backlog_size:1048576`** is the *default*, printed regardless of what the operator set.
   A node running with a tuned `replication.backlog-size` reports the default back at them, so the
   field cannot be used to confirm a config change landed — which is the only reason to read it.

2. **`repl_backlog_first_byte_offset:0`** is not merely stale, it contradicts a specced invariant.
   FM-REPLICATION-014 is entirely about the backlog's armed eviction floor: once entries have been
   evicted, offsets below the floor are refused a partial resync. `0` says the backlog can serve
   from the beginning of history, which is exactly the claim the floor exists to deny. An operator
   diagnosing "why is every reconnect full-resyncing" reads this field, sees `0`, and concludes the
   backlog is not the cause.

This is the same class as issues 16 (`listening-port` parsed and discarded) and 17 (`sync_*`
counters hardcoded to zero), both now fixed: a real value exists inside the process, and the render
prints a constant beside honest fields, with no marker distinguishing the two. Per
`feedback_observability_accuracy`, misleading data is worse than missing data.

`repl_backlog_active` and `repl_backlog_histlen` are *not* part of this — both are projected from
real state already.

## Why it was left

Closing issues 16/17 collapsed the `slaveN:` literals (`state`, `lag`) into a per-replica
projection, which is what FM-REPLICATION-049 covers. The backlog geometry has no plumbing at all —
neither renderer's snapshot type carries the configured size or the floor — so it is a separate
change, not a leftover of that one. It is now the only remaining literal in the replication INFO
surface, and is recorded as such in FM-REPLICATION-043's tail note and in the Redis-deviations
table.

## Suggested remedy

1. Carry both values on the replication snapshot that `info_handler` builds:
   `backlog_size` from the live `replication.backlog-size` config (live-tunable — read at render
   time, not cached), and `backlog_first_byte_offset` from the same floor `PartialSyncReplay`
   consults, so INFO and the partial-resync decision cannot drift (the pattern FM-REPLICATION-049
   used for `lag`).
2. A disabled backlog should report `repl_backlog_active:0` with a size of the configured value,
   matching Redis, rather than a size that implies an active buffer.
3. Render through one field list both `StatsSection`-style renderers consume, so the pair cannot be
   fixed in one renderer and forgotten in the other — the failure mode that produced issue 17.

## Tests that should exist

- `the_backlog_size_reported_is_the_configured_one` — render with a non-default `backlog-size` and
  assert the field matches, in both renderers.
- `the_first_byte_offset_reported_is_the_armed_floor` — arm the floor by evicting entries, assert
  the rendered offset equals the floor `can_replay` would refuse below (the FM-REPLICATION-014
  boundary).
- `both_info_renderers_report_the_same_backlog_geometry` — the twin of
  `both_info_renderers_report_the_same_sync_counters`.

## Spec impact

FM-REPLICATION-043's `Observable`/tail note and the FM-REPLICATION-043 row in the Redis-deviations
table both currently record these as known-hardcoded; closing this issue means rewriting both to
describe the projection, and likely adds a `Forced by` entry naming the three tests above.

## Resolution

Fixed. Both `INFO replication` renderers now project all four `repl_backlog_*` fields off the live
ring buffer through one shared field list; specced as **FM-REPLICATION-059**.

**Root cause.** The geometry had no plumbing at all: `PartialSyncReplay` owned the
`ReplicationRingBuffer` by value inside `PrimaryReplicationHandler`, and neither renderer's snapshot
type could reach it. Both printed constants instead.

**Fix.**

1. `ReplicationRingBuffer::geometry(current_offset) -> BacklogGeometry` (`primary/ring_buffer.rs`) is
   the single producer of `{active, size_bytes, first_byte_offset, histlen}`. `active` and
   `first_byte_offset` come from `start_offset()` — the armed floor `can_replay` consults, so INFO
   and the `PSYNC` decision read one value (FM-REPLICATION-014). `histlen` is a `saturating_sub`.
2. `PartialSyncReplay` holds the buffer as an `Arc` and exposes `backlog_handle()`;
   `PrimaryReplicationHandler::new` publishes it into `ReplicationTrackerImpl::publish_backlog`.
   The tracker is the one object both renderers hold in either role — the same seam
   FM-REPLICATION-050 used for the sync counters — so no per-renderer plumbing was needed.
3. `info::backlog_geometry_fields(geometry)` is the shared `[(name, value); 4]`. `sections.rs` writes
   it through the `StatsSection` writer; `commands/info.rs` formats the same list via
   `build_backlog_info`. Both role branches call it (remedy 3).
4. `TestServerConfig::replication_backlog_max_mb` so an integration test can configure a cap that is
   neither FrogDB's default (64 MiB) nor Redis's (1 MiB).

A disabled backlog reports `active:0` beside its configured size (remedy 2): writes are gated on
`enabled`, so the buffer never arms and `size_bytes` is still the configured cap.

## Divergences from the issue as written — flagged loudly

**Two fields the issue excluded were also wrong, and are fixed here.** The issue says
`repl_backlog_active` and `repl_backlog_histlen` are "projected from real state already". They were
projected from the *wrong* real state:

- `active` was `!replicas.is_empty()` — replicas attached, not a window armed. Projecting only the
  floor would have let INFO print `repl_backlog_active:0` beside `repl_backlog_first_byte_offset:9000`.
- `histlen` was `repl_offset`, i.e. the whole history rather than the retained window, overstating
  what a reconnecting replica can resume from by everything already evicted.

Both now derive from the same `BacklogGeometry`, so `floor + histlen <= head` holds by construction.
The `active` change is Redis-visible (Redis's flag means "a backlog exists") and is recorded in the
deviations table.

**A third literal, in the neighbouring branch.** The shard-local replica branch printed
`master_repl_offset:0` — the literal FM-REPLICATION-043's NOT-observable clause already forbade,
still present in the renderer that clause did not name. Replaced with the node's real offset.

**A wider bug the acceptance test exposed.** With both renderers fixed, the end-to-end assertion
through `redis.call('INFO','replication')` still read `repl_backlog_size:0`. `ScriptInvoker`
(`frogdb-core/src/scripting/gate.rs`) carried the store, shard routing and replica identity across
the script seam but **not** `replication_tracker`, so the sub-command context it builds had `None`
and every tracker-backed reader inside a script rendered the "replication is not configured"
defaults: `connected_slaves:0`, `master_repl_offset:0`, zeroed sync counters (FM-REPLICATION-050) and
an all-zero backlog on a primary with live streaming replicas — plus a scripted `WAIT` returning `0`
acks and a scripted `ROLE` reporting no replicas. Fixed by propagating the tracker, with
`run_local_propagates_the_replication_tracker` / `run_local_without_a_tracker_sees_none` at the seam.
Fixed rather than filed because the issue's own acceptance test cannot pass without it.

`ctx.node_id` is dropped by the same seam and was deliberately **not** propagated: it gates the
cluster admin subcommands (`ok_or(ClusterDisabled)`), so propagating it would widen what a script may
do — a separate decision.

**Not reached: `total_net_repl_input_bytes` / `total_net_repl_output_bytes`.** FM-REPLICATION-058's
audit put these "in issue 20's position". They differ in kind — no counter exists anywhere to project
from — so they are refiled as
`.scratch/hardening/issues/open/29-net-repl-byte-counters-are-hardcoded-zero.md`.

## Tests

All tagged `// FM-REPLICATION-059`.

- `frogdb-server/crates/replication/src/primary/tests.rs`:
  `the_handler_publishes_its_backlog_to_the_tracker`,
  `backlog_geometry_reports_the_configured_cap_and_the_armed_floor`,
  `backlog_geometry_first_byte_offset_follows_eviction`,
  `backlog_geometry_after_a_reset_claims_no_window`,
  `backlog_geometry_histlen_never_underflows`
- `frogdb-server/crates/server/src/info/sections.rs`:
  `the_backlog_size_reported_is_the_configured_one` (issue's test 1),
  `the_first_byte_offset_reported_is_the_armed_floor` (test 2),
  `both_info_renderers_report_the_same_backlog_geometry` (test 3),
  `a_replica_reports_its_configured_capacity_with_no_window`
- `frogdb-server/crates/server/tests/integration_replication.rs`:
  `info_reports_the_configured_backlog_geometry_through_both_renderers`
- `frogdb-server/crates/core/src/scripting/gate.rs`:
  `run_local_propagates_the_replication_tracker`, `run_local_without_a_tracker_sees_none`

Red proofs: restoring the four literals in `sections.rs` failed the four render tests; disabling
`publish_backlog` failed `the_handler_publishes_its_backlog_to_the_tracker`; disabling the tracker
propagation in `run_local` failed `run_local_propagates_the_replication_tracker`.

## Spec

FM-REPLICATION-059 added. FM-REPLICATION-043's tail note rewritten (no field in either replication
renderer is a constant now). The FM-REPLICATION-043 backlog row in the Redis-deviations table is
replaced by an FM-REPLICATION-059 row recording the `repl_backlog_active` sharpening.
