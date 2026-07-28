# 17 — Hot-shard collector: build the feature or delete the collector

Status: needs-triage

## Context

Issue 14 triage (2026-07-22) deleted the dead `hotshards.*` config section (3 fields) and its
dead plumb chain (`to_collector_config()` → `frogdb_debug::HotShardConfig` → `deps.hotshards_config`
→ unused `_hotshards_config` connection field). Per user decision, the collector implementation
itself was KEPT: `frogdb-server/crates/debug/src/hotshards.rs` (~300 lines, `HotShardCollector`,
implements `frogdb_core::HotShardDetector`, has unit tests) — but it has ZERO production callers:
`HotShardCollector::new` is never called, the `hot_shard_detector()` observability hook returns
`None` by default (`core/src/metrics.rs:104`) with no production override, and nothing calls
`collect()` or renders `HotShardReport`.

## What to build

Wire the collector end-to-end, or decide to delete it after all:

1. Reconcile the config-struct duplication: `debug/src/config.rs` `HotShardConfig` (the old plumb
   type) vs the collector's own local `HotShardConfig` in `debug/src/hotshards.rs:18` — one struct.
2. Re-introduce a `hotshards.*` config section wired directly to the collector (the old fields were
   deleted precisely because they fed the wrong struct and nothing ran).
3. Construct the collector at startup and install it via an `Observability` impl so
   `hot_shard_detector()` returns `Some`.
4. Add a consumer surface: a status field / debug-UI panel / command that calls `collect()` and
   renders `HotShardReport` (relates to the status/debug work in issues 02/11/12).
5. Propagation-truth gate per 13-01 before any field is promoted to CONFIG GET/SET.

## Source

Issue 14 dead-config triage; user chose "keep collector, file feature issue" over deletion.
