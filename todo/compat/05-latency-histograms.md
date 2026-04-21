# 5. Latency Histograms for INFO (6 tests — `info_tcl.rs`)

**Status**: Per-client latency exists; per-command latency histograms in INFO do not
**Scope**: Implement `latencystats` section in INFO with configurable percentile reporting.

## Work

- Implement per-command latency histogram collection (HDR histogram or similar)
- Add `latency-tracking-info-percentiles` config parameter
- Support `CONFIG SET` for runtime percentile reconfiguration
- Include subcommand-level granularity
- Add enable/disable toggle
- Expose in `INFO latencystats` section

**Key files to modify**: `commands/info.rs`, command dispatch/timing, config module

## Tests

- `latencystats: disable/enable`
- `latencystats: configure percentiles`
- `latencystats: bad configure percentiles`
- `latencystats: blocking commands`
- `latencystats: subcommands`
- `latencystats: measure latency`
