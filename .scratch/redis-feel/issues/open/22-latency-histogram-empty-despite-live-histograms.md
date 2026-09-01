# `LATENCY HISTOGRAM` returns an empty array while the histograms it needs are already live

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: observability / latency

## Problem

`LATENCY HISTOGRAM [command ...]` unconditionally replies with an empty array:

```rust
// frogdb-server/crates/server/src/connection/observability_conn_command.rs:730
async fn latency_histogram(_args: &[Bytes]) -> Response {
    // This would require command-level latency tracking which is not yet implemented
    // Return an empty response for now
    Response::Array(vec![])
}
```

The comment is stale. Per-command latency histograms exist and already render:

- `frogdb-server/crates/server/src/info/sections.rs:707-730` — `LatencystatsSection` reads
  `src.latency().histograms`, calls `all_commands()` and `percentiles_for(&cmd, &percentiles)`,
  and emits `latencystats_<cmd>:p50=…,p99=…`.

Every other `LATENCY` subcommand is real (`DOCTOR` :633, `GRAPH` :669, `HISTORY` :737,
`LATEST` :768, `RESET` :790). `HISTOGRAM` is the only fabricated one, and it is the subcommand
`redis-cli --latency`-style tooling and `redis_exporter` reach for.

## Ruling

Wire `LATENCY HISTOGRAM` to the same `histograms` source `latencystats` uses. Redis's reply shape
is a RESP map of `command -> {calls, histogram_usec -> {bucket -> count}}`; emit real bucket
counts from the existing histogram, filtered to the command names given as arguments (all
commands when no argument is given). An unknown/never-called command name contributes no entry —
that is Redis's behavior and it is truthful here.

If the underlying histogram cannot expose raw buckets (only percentiles), say so in `## Comments`
and emit the buckets the histogram implementation does hold rather than interpolating fake ones.
An honest coarser histogram is acceptable; invented bucket counts are not.

## Acceptance criteria

- [ ] `LATENCY HISTOGRAM` after N `GET`s reports a `get` entry whose `calls` matches the
      `cmdstat_get:calls` value `INFO commandstats` reports at the same moment
- [ ] `LATENCY HISTOGRAM get set` filters to those two commands; unknown names are absent, not an
      error
- [ ] `CONFIG RESETSTAT` clears what `LATENCY HISTOGRAM` reports (same reset seam as
      `commandstats`/`latencystats`)
- [ ] Regression test asserts non-empty output after traffic — the empty-array stub must fail it

Size: S-M
