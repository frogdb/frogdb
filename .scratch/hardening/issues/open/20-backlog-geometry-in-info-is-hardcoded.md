# Backlog geometry in `INFO replication` is hardcoded

Status: needs-triage
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
