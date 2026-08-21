# Default `INFO` output is missing sections Redis 8.6 emits by default

Status: ready-for-agent
Type: bug (introspection accuracy)
Area: info

## Problem

`INFO` with no arguments omits several sections Redis 8.6 includes by default. Two are already
implemented but not wired into the default list; four don't exist at all.

## Already implemented, just not defaulted

`Errorstats` and `Keysizes` exist in `frogdb-server/crates/server/src/info/sections.rs`
(`all_sections` :20-37; `Commandstats` :537-559; `Errorstats` :565-579; `Latencystats` :585-613)
but aren't included in `frogdb-server/crates/server/src/commands/info.rs`'s
`DEFAULT_SECTIONS`/`EXTRA_SECTIONS` lists (:44-60).

## Don't exist at all

`Cluster`, `Modules`, `Threads`, `Hotkeys`.

## Ruling

Align the default section list with Redis 8.6's defaults:

- Add `Errorstats` and `Keysizes` to `DEFAULT_SECTIONS` (they're already correct, just gated
  off).
- Add `# Cluster` — `cluster_enabled:0`/`1` sourced from the same cluster-mode flag `redis_mode`
  already reads.
- Add an empty `# Modules` section (FrogDB has no module system; an empty section is the
  truthful answer, matching Redis's own output when no modules are loaded).
- Add `# Threads` reporting real shard-worker facts (thread/worker count, not fabricated).
- Add `# Hotkeys` — map truthfully from the `hotshards` feature if it's compiled in, or emit an
  empty section if it isn't. No fabricated hot-key data either way.

## Acceptance criteria

- [ ] `INFO` (default, no args) section-header diff vs Redis 8.6's default `INFO` output is
      empty
- [ ] `Errorstats`/`Keysizes` values are unchanged from their existing (already-correct)
      implementation — this issue only changes whether they're included by default
- [ ] `Cluster` section's `cluster_enabled` matches the value `redis_mode` already reports
- [ ] `Threads`/`Hotkeys` never emit fabricated data — empty section is acceptable, wrong data is
      not

Size: S-M
