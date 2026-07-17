# Rename warm-tier demote/promote to spill/unspill

Status: ready-for-agent

The warm-tier vocabulary collides with cluster role demotion. Canonical terms per
`frogdb-server/CONTEXT.md`: **spill** (hot→warm) and **unspill** (warm→hot).

Rename (mechanical, sed-friendly; frogdb-server/crates/core):

- `demote_key` → `spill_key`; `DemotionError` → `SpillError` (variant `AlreadyWarm` etc. review)
- Promotion path: `promote`/`promotion` identifiers in `store/warm_tier.rs` and
  `store/hashmap.rs` → `unspill` equivalents
- Counters: `total_demotions` → `total_spills`, `total_promotions` → `total_unspills`,
  `expired_on_promote` → `expired_on_unspill`
- Any Prometheus metric names and debug-UI fields derived from these (check
  `frogdb-telemetry`, `frogdb-debug`)
- Doc comments and website storage docs mentioning demotion/promotion for tiering

Cluster-side `DemotionEvent`/`handle_demotion` (frogdb-cluster) are role demotion and must NOT
be renamed.
