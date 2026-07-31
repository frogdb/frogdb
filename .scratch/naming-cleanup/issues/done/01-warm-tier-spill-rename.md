# Rename warm-tier demote/promote to spill/unspill

Status: done

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

## Comments

Renamed the warm-tier demote/promote vocabulary to spill/unspill, scoped to
`frogdb-server/crates/core` plus the identifiers/docs that reach outward from it:

- `store/hashmap.rs`: `demote_key` → `spill_key`, `promote_key` → `unspill_key`,
  `DemotionError` → `SpillError` (variants `AlreadyWarm`/`NoWarmStore`/`KeyNotFound`/`Rocks`
  kept as-is — none referenced "demotion").
- `store/warm_tier.rs`: `record_demotion`/`record_promotion`/`record_expired_on_promote` →
  `record_spill`/`record_unspill`/`record_expired_on_unspill`; `total_demotions`/
  `total_promotions`/`expired_on_promote` fields and `demotions()`/`promotions()`/
  `expired_on_promote()` accessors → `total_spills`/`total_unspills`/`expired_on_unspill` and
  `spills()`/`unspills()`/`expired_on_unspill()`. Fixed one sed collateral hit where a substring
  match mangled a test fn name (`demotion_and_promotion_counters_move_in_lockstep` →
  `spill_and_unspill_counters_move_in_lockstep`, not `demotion_and_unspill_counters_...`).
- `shard/eviction.rs`: `demote_with_ranker`/`demote_for_eviction` → `spill_with_ranker`/
  `spill_for_eviction`; also reworded the two tracing log messages ("Demoted key to warm
  tier"/"Failed to demote key" → "Spilled key.../Failed to spill key") since they're
  warm-tier observability text, same family as the doc comments the issue calls out.
- `store/mod.rs`: `pub use hashmap::{DemotionError, HashMapStore}` → `{HashMapStore,
  SpillError}` (re-alphabetized).
- `shard/types.rs`, `shard/diagnostics.rs`, `shard/rollback.rs`, `shard/blocking.rs`,
  `eviction/policy.rs`, `command.rs`(none — see below): doc-comment prose updated
  (demote/demoted/demotion → spill/spilled/spill; promote/promoted/promotion → unspill/
  unspilled/unspill) everywhere it referred to warm-tier movement.
- `config/tiered.rs`: doc comment ("demote values to a RocksDB warm tier" → "spill values...").
- `frogdb-types/src/metrics/definitions.rs`: `TieredDemotions`("frogdb_tiered_demotions_total")
  → `TieredSpills`("frogdb_tiered_spills_total"); `TieredBytesDemoted`
  ("frogdb_tiered_bytes_demoted_total") → `TieredBytesSpilled`
  ("frogdb_tiered_bytes_spilled_total"). No frogdb-telemetry- or frogdb-debug-crate references
  to these names existed (checked both crates plus the debug UI's JS/HTML assets — none render
  tiered_* fields).
- `frogdb-server/src/info/{mod.rs,sections.rs}` and `commands/info.rs`: `TieredCounts` fields
  and the `INFO` wire fields `tiered_promotions`/`tiered_demotions`/`tiered_expired_on_promote`
  → `tiered_unspills`/`tiered_spills`/`tiered_expired_on_unspill`.
- `frogdb-server/crates/core/tests/tiered_storage.rs`: all `demote_key` call sites →
  `spill_key`; test fn names and inline comments renamed (e.g. `test_demote_and_promote_cycle`
  → `test_spill_and_unspill_cycle`, `test_demote_errors` → `test_spill_errors`).
- Docs: `frogdb-server/CONTEXT.md` Spill/Unspill glossary entry and the "Flagged ambiguities"
  note updated to say the code rename is done (was "legacy naming... pending, issue 01").
  `website/src/content/docs/reference/configuration.mdx` (eviction-policy table + Tiered
  Storage section) and `website/src/content/docs/compatibility/redis-differences.mdx` (Tiered
  Storage section) reworded to spill/unspill.

Deliberately left as demote/promote (all out of scope — confirmed each by reading context,
not just grepping):
- `frogdb-cluster`'s `DemotionEvent`/`handle_demotion` and every cluster role
  demotion/promotion reference in `frogdb-server/crates/{cluster,config,server,replication}`
  and the website's replication/clustering/glossary/consistency docs (role changes, not tiering).
- `command.rs`'s `ListpackConfig` doc ("control when to promote from listpack...") and the
  listpack/HLL sparse→dense "promotion" language in `frogdb-types` (`hyperloglog.rs`,
  `types/set.rs`, `types/hash.rs`, `types/mod.rs`) and `frogdb-persistence`'s serialization —
  a different, unrelated "promotion" (small-encoding → big-encoding), not warm-tier.
- `frogdb-persistence/src/snapshot/stager.rs` and its tests: "promote"/"promoted" there means
  atomically renaming a staged snapshot dir into place — an unrelated domain concept.
- `todo/proposals/{20-eviction-generic-ranker,29-wal-durability-sink,30-store-entry-
  reconciliation,35-typed-metric-chokepoint}.md`: historical design-analysis docs pinned to
  specific pre-rename code snapshots/line numbers; the issue's scope (doc comments + website
  docs) doesn't cover `todo/proposals`, so left as-is rather than risk misdescribing history.

## Verification

- `just check frogdb-core`, `just check frogdb-config`, `just check frogdb-types`,
  `just check frogdb-server` — all pass.
- `just lint frogdb-core`, `just lint frogdb-config`, `just lint frogdb-types`,
  `just lint frogdb-server` (clippy `-D warnings`) — all pass.
- `just fmt frogdb-core`, `just fmt frogdb-config`, `just fmt frogdb-types`,
  `just fmt frogdb-server` — no-op (already formatted).
- `just test frogdb-core tiered` — 17/17 pass (covers every renamed spill/unspill/warm-key
  test, including the fixed `test_spill_and_unspill_cycle`,
  `test_get_hot_does_not_unspill`, `test_get_mut_unspills_warm_key`,
  `test_expired_warm_key_cleaned_on_unspill`, `test_spill_errors`,
  `test_spill_no_warm_store`).
- `just test frogdb-core` (full crate) — 706/706 pass.
- `just test frogdb-server tiered` and `just test frogdb-server info` — 1/1 and 78/78 pass
  (covers `tiered_renders_summed_counters` and the broader INFO section suite, which exercises
  the renamed `tiered_spills`/`tiered_unspills`/`tiered_expired_on_unspill` wire fields).
