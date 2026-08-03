# sync_full and sync_partial_ok/err in INFO are hardcoded to zero

Status: done
Type: bug (observability)
Severity: likelihood 3/3 (every `INFO replication` on a primary), consequence 2/3 (the three fields
an operator uses to tell "replicas are resyncing constantly" from "replication is healthy" always
read `0`) — score 6
Area: replication / INFO

## Problem

Both `INFO replication` renderers emit the sync counters as literals:

- `frogdb-server/crates/server/src/info/sections.rs:348-350` — `.field("sync_full", 0)`,
  `.field("sync_partial_ok", 0)`, `.field("sync_partial_err", 0)`
- `frogdb-server/crates/server/src/commands/info.rs:350-352` — the same three baked into a format
  string.

In Redis these are the primary diagnostic for resync churn: `sync_full` climbing means replicas
keep falling out of the backlog and paying for full checkpoint transfers; `sync_partial_err`
climbing means partial resync is being *attempted* and refused. Reading `0` from all three is
indistinguishable from a perfectly healthy link, so the single most common replication pathology is
invisible from `INFO`.

This matters more than usual right now because issue 14 (the backlog wired to the split-brain
config keys) has exactly this signature: `split-brain-log-enabled = false` silently turns every
partial resync into a full one, and `sync_full` is the field that would have shown it.

The primary already decides these outcomes — the `+FULLRESYNC` / `+CONTINUE` fork and its
`FullResyncReason` are the increment sites, and spec row FM-REPLICATION-011 pins that fork's
honesty. There is simply no counter behind them.

## Candidate fix

Add three counters to the primary replication handler (or to `frogdb-telemetry` alongside the other
replication metrics, if a Prometheus surface is wanted too) and increment them at the PSYNC
decision point: `sync_full` on every `+FULLRESYNC`, `sync_partial_ok` on every `+CONTINUE`,
`sync_partial_err` on a partial resync that was requested and refused — noting that in Redis a
refused partial *also* increments `sync_full`, since it falls through to a full resync, and FrogDB
should match that or state the deviation in the spec's Redis-deviations table. Then read them in
both renderers.

The two renderers duplicating the same fields is itself a hazard — a fix applied to one and not the
other is silently half-done. Worth collapsing them, or at minimum asserting they agree in a test.

## Forcing tests

An integration test that attaches a replica (asserting `sync_full` goes 0 -> 1), disconnects and
reconnects it within the backlog window (asserting `sync_partial_ok` goes 0 -> 1 and `sync_full`
stays), then reconnects it after overrunning the backlog (asserting `sync_partial_err` and
`sync_full` both advance). A unit test asserting both renderers produce the same three values for
the same state.

## Resolution

Fixed. The counters are real, they live on the replication tracker, and **Redis's rule that a
refused partial also increments `sync_full` is matched exactly** — no deviation row was needed for
it.

- New `frogdb-server/crates/replication/src/sync_counters.rs`. `SyncOutcome::classify(requested_id,
  granted_partial)` is the whole classification rule and is pure: a grant is `PartialOk` whatever id
  was sent; otherwise the literal `"?"` sentinel is `FullResyncRequested`; any other id is
  `PartialRefused`.
- It deliberately does **not** key on `FullResyncReason`, which would have been the obvious wiring.
  `PartialSyncReplay::can_replay` returns `Disabled` *before* it inspects the `?` sentinel, so a
  `PSYNC ? -1` against a primary with the backlog switched off would have been charged to
  `sync_partial_err` — a healthy cold start reported as a backlog miss. Classifying on the requested
  replid is also literally what Redis does (`if (master_replid[0] != '?') server.stat_sync_partial_err++;`).
- `SyncCounters::record`'s `PartialRefused` arm increments `partial_err` **and** `full`, which is
  the one place Redis's fall-through is expressed. `handle_psync` records the outcome immediately
  after the decision and before registration, so the counter moves on the same decision the wire
  reply is minted from.
- `ReplicationTrackerImpl` owns the counters and exposes `sync_counters() -> SyncCountersSnapshot`.
  `info_handler` reads it role-independently into `ReplicationSnapshot.sync`.

The two renderers were **not** collapsed into one: `build_stats_info` serves the shard-local `INFO`
a script sees and cannot reach the connection's `InfoSources`. The *fields* were collapsed instead —
`info::sync_counter_fields(snapshot) -> [(&'static str, u64); 3]` is the only place the three names
are spelled, both renderers consume it, and `both_info_renderers_report_the_same_sync_counters`
asserts they agree for one state. `build_stats_info` was made a pure function of
`SyncCountersSnapshot` so that test needs no `CommandContext`.

Forcing tests: `each_psync_fork_moves_the_counter_it_is_named_after` (drives all three arms through
the real `handle_psync`, not the classifier alone), plus the five `sync_counters.rs` unit tests
including `a_refused_partial_advances_both_partial_err_and_full`, plus
`both_info_renderers_report_the_same_sync_counters` and `an_untouched_primary_reports_zero_sync_counters`
in `info/sections.rs`. The issue asked for an *integration* test attaching and reconnecting a real
replica; the crate-level equivalent was written instead, because `cargo mutants -p` never runs the
integration suite and the three arms are reachable directly through `handle_psync`.

Spec row **FM-REPLICATION-050**. FM-REPLICATION-013's first non-guarantee bullet and its
Redis-deviations row both claim the counters are hardcoded `0` and need amending — replacement text
is in the fragment this change shipped with.

**The issue's FM reference is stale:** it names FM-REPLICATION-011 for the PSYNC fork; that row is
now FM-REPLICATION-013.
