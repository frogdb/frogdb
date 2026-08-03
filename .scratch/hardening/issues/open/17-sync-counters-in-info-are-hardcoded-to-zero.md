# sync_full and sync_partial_ok/err in INFO are hardcoded to zero

Status: needs-triage
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
