# 03 — `NodeStateSnapshot`: one collect() scatter behind all observability surfaces

Status: ready-for-human

## What to build

The endgame of proposal 02: a unified node-state snapshot type assembled by a single
Scatter-Gather `collect()` pass, which INFO, telemetry `/status`, and the debug UI all render
from. Today assembling per-shard state costs up to 4 scatter round-trips and each surface
carries its own parallel structs (`MemoryStats`, `WalLagStats` scatters, `InfoSnapshot`).

Round-8 deliberately stopped at the `WalLagAggregate` facet: a half-populated god-type would
have added a fourth parallel model. With issues 01 and 02 done, every facet has a single
accurate source — folding them into one snapshot type then removes models instead of adding
one.

Home: `frogdb-telemetry` is the natural owner per the proposal (audit layering; core is
acceptable fallback precedent from `WalLagAggregate`).

## Acceptance criteria

- [x] One `collect()` produces the snapshot in a single scatter round-trip (test pins the message count)
- [x] INFO renders from it with byte-identical wire output (existing INFO pin tests unchanged)
- [x] Telemetry `/status` renders from it (serde round-trip tests unchanged)
- [x] Debug UI renders from it
- [x] Redundant parallel structs deleted (deletion test: `MemoryStats`/`InfoSnapshot`-style duplicates gone or reduced to renderers)
- [x] `lint-info-seam` gate still passes

## Blocked by

- `01-telemetry-status-stub-fields.md`
- `02-debug-ui-unstub-shard-stats-latency.md`

## Source

`.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (steps 1, 4, 5).

## Comments

### 2026-07-20 — implemented (ready-for-human)

Endgame of proposal 02 landed: a single `frogdb_telemetry::NodeStateSnapshot`,
folded by one `collect()` (one `InfoSnapshot` fleet scatter), is now the source
of truth for INFO, telemetry `/status`, and the debug UI.

**Home / layering.** `frogdb-telemetry` (dep chain `core -> telemetry -> debug`,
and `server` depends on all three). Telemetry is downstream of `frogdb-core`
(shard types) and upstream of both `debug` and `server`, so all three surfaces
depend on the type with no cycle — matches the proposal; no types had to be
relocated. The WAL fold reuses the round-8 `frogdb_core::observability::WalLagAggregate`.

**Scatter round-trips (per observability request).** Before: INFO 1
(`InfoSnapshot`), telemetry `/status` **2** (`MemoryStats` + `WalLagStats`),
debug UI 1 (bespoke `InfoSnapshot` fold) = up to **4** distinct gather families
for overlapping data. After: **1** (`NodeStateSnapshot::collect`, one
`InfoSnapshot` message per shard) shared by all three. `/status` dropped from 2
scatters to 1; the `ShardMessage::WalLagStats` protocol variant is now orphaned
and was removed.

**Parallel data-model count.** Before: **3** independent per-shard/aggregate
models — `ShardInfoSnapshot` (server INFO), `ShardStatus` (telemetry),
`ShardStats` (debug), each with its own fold. After: **1** canonical
(`NodeStateSnapshot` + `ShardState` row); `ShardStatus` and `ShardStats` are
reduced to `From<&ShardState>` render views (per the "reduced to renderers"
allowance). Deleted: `ShardInfoSnapshot` struct + its `absorb`/WAL fold;
telemetry `gather_shard_stats` + `gather_wal_lag_stats`; debug
`gather_info_snapshots`; the dead `ShardMessage::WalLagStats` variant.

**Accuracy.** All three surfaces now read the *same* aggregate, so memory /
keyspace / WAL views can no longer disagree (the drift the proposal called out:
telemetry surfaced avg-lag + per-shard rows that INFO's separate loop lacked).
INFO wire output is byte-identical; `/status` JSON shape unchanged.

**Tests (targeted).**
- `frogdb-telemetry node_state::tests` — `collect_sends_exactly_one_message_per_shard`
  (pins one message/shard), `absorb_sums_aggregate_and_records_per_shard_rows`,
  `absorb_folds_wal_lag_through_shared_aggregate`, `collect_errors_when_a_shard_drops_the_request`.
- `frogdb-telemetry status::tests` — `status_shards_and_totals_render_from_node_state_snapshot`
  (new; /status rows + sums come off the snapshot) + 24 existing serde/round-trip
  tests unchanged → 25 passed.
- `frogdb-debug web_ui::state::tests` — `shard_views_agree_across_surfaces` (new;
  /status `ShardStatus` and debug `ShardStats` render identical id/keys/memory
  from one `ShardState`) + existing panel tests → all passed.
- `frogdb-server info::` — 43 passed incl. gather wrapper tests and the byte-format
  integration pins (`info_default_renders_sections_in_canonical_order`,
  `info_persistence_enabled_reports_live_wal_values`, `info_memory_reports_fleet_wide_usage`).
- `just lint-info-seam` clean; full `just check` (workspace, all-targets) green;
  `just lint` clippy green on touched crates.

**Commits.** `31d08e81` (type), `b6c0a258` (/status), `85822aa1` (debug),
`ada021c2` (INFO), `8193bcc7` (cross-surface tests), `d0c1f855` (orphaned message removal).
