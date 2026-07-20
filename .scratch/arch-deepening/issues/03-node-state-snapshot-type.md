# 03 — `NodeStateSnapshot`: one collect() scatter behind all observability surfaces

Status: needs-triage

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

- [ ] One `collect()` produces the snapshot in a single scatter round-trip (test pins the message count)
- [ ] INFO renders from it with byte-identical wire output (existing INFO pin tests unchanged)
- [ ] Telemetry `/status` renders from it (serde round-trip tests unchanged)
- [ ] Debug UI renders from it
- [ ] Redundant parallel structs deleted (deletion test: `MemoryStats`/`InfoSnapshot`-style duplicates gone or reduced to renderers)
- [ ] `lint-info-seam` gate still passes

## Blocked by

- `01-telemetry-status-stub-fields.md`
- `02-debug-ui-unstub-shard-stats-latency.md`

## Source

`.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (steps 1, 4, 5).
