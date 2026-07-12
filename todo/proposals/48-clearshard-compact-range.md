# 48 — Space reclamation after ClearShard (compact_range follow-up)

**Status:** implemented (2026-07-12)
**Severity:** Minor (disk-usage, not correctness)
**Found:** deferred from [43-flushdb-persistence-clearing.md](43-flushdb-persistence-clearing.md)
open decision 2 (implementer + final review both filed it as a follow-up, 2026-07-12).

## Problem

Proposal 43's FLUSHDB/FLUSHALL clear lands as a RocksDB range tombstone (DeleteRange). The
tombstone hides the data immediately and correctly, but the underlying SST bytes are reclaimed
only when compaction happens to cover the range. A large keyspace flushed on a quiet instance
can hold its disk footprint indefinitely.

## Prior art

Kvrocks follows FLUSHDB's DeleteRange with an explicit `CompactRange` over the same span to
reclaim space eagerly. RocksDB also offers `DeleteFilesInRange` (drops whole SSTs entirely
within the range before compacting the rest) as the cheap first pass — the recommended combo for
bulk deletion is `DeleteFilesInRange` then `CompactRange`.

## Design sketch

After `stage_clear`'s tombstone batch commits (the correctness point — do not move it),
trigger reclamation asynchronously so the flush thread is not blocked:

1. `DeleteFilesInRange(cf, full-range)` — near-free, drops fully-covered SSTs.
2. `CompactRange(cf, full-range)` with `bottommost_level_compaction` set to force — reclaims the
   remainder and drops the tombstone itself.

Constraints: must not run on the flush thread (compaction can take seconds—minutes); a
background task or RocksDB's own thread pool via the async compact API. Both primary and warm
CFs. Coalesce repeated FLUSHDBs (a second clear while compaction runs should not queue a
duplicate). Expose observability: a counter or log line when post-clear compaction starts and
finishes (`frogdb_wal_merge_operands_total` from proposal 42 is the metric-shape precedent).

## Tests

1. Functional: FLUSHDB with reclamation enabled → data still gone after restart (compaction must
   not resurrect anything; exercises tombstone+compaction interaction).
2. Disk assertion (best-effort, may be flaky-prone — consider `#[ignore]` + manual): write N MB,
   FLUSHDB, wait for the reclamation hook, assert the DB directory shrank materially.
3. Concurrency: FLUSHDB → immediate writes → reclamation completes → new writes intact.
4. Double-flush coalescing unit test on whatever queue/flag guards duplicate compactions.

## Open decisions

1. Config knob (`flush-compact-range yes/no`, default on?) vs unconditional.
   **Resolved: default-on knob.** `persistence.flush-compact-range` (TOML, default `true`),
   registered as an immutable CONFIG GET param — the plumbing through the proposal-16 registry
   was one serde field + one `ConfigParamInfo` entry + one read-only `ParamMeta`, so the knob
   was cheap. Kvrocks runs its post-FLUSHDB `CompactRange` unconditionally; the knob exists as
   an escape hatch for operators who would rather trade lingering disk for zero compaction I/O
   after a flush.
2. Whether eviction/warm-tier demotion churn justifies a broader periodic-compaction story —
   out of scope here, but note if evidence appears while implementing. No new evidence
   surfaced during implementation; still open as a future consideration.

## Implementation notes (2026-07-12)

- `frogdb-server/crates/persistence/src/rocks/reclaim.rs`: `ReclaimGuard` (per-`(tier, shard)`
  coalescing set) + `spawn_clear_reclamation` / `run_reclamation`
  (`delete_file_in_range_cf` over the cleared range, then `compact_range_cf_opt` with
  `BottommostLevelCompaction::Force`).
- **Async mechanism:** a dedicated `std::thread` per pass (`clear-reclaim-<shard>`), never the
  WAL flush thread. The rocksdb 0.24 Rust binding exposes only the *blocking*
  `compact_range_cf_opt` (no async/callback compact API and no
  `CompactRangeOptions::exclusive_manual_compaction`-style completion handle surfaced for
  scheduling on RocksDB's own pool), so a spawned thread that parks on the blocking call is the
  idiomatic option; the actual compaction work still executes on RocksDB's background job pool.
- **Trigger points (correctness point unmoved):**
  - Primary CF: `RocksSink::stage_clear` captures the staged tombstone's upper bound
    (`batch_clear_shard` now returns it); the `commit` that lands the tombstone batch spawns
    the pass on success and drops the bound on failure.
  - Warm CF: `RocksStore::clear_tier_shard` spawns the same pass after its direct tombstone
    write commits.
  - An empty CF stages no tombstone and triggers no pass (nothing to reclaim).
- **Coalescing:** a second FLUSHDB while a pass is in flight for the same `(tier, shard)` is
  dropped, not queued — a pass started after the newer tombstone commits would only repeat the
  work. Unit-tested in `reclaim.rs`.
- **Observability:** `frogdb_flush_compact_started_total` / `frogdb_flush_compact_completed_total`
  counters (labelled by shard, following the proposal-42 metric-shape precedent) plus
  start/finish `info!` log lines with duration. Counters only increment for passes that actually
  run — coalesced or knob-disabled requests do not count. The recorder reaches the store via
  `RocksStore::set_metrics_recorder`, installed at server startup before shard workers spawn.
- **Tests** (all four proposal tests landed; the disk assertion needed no `#[ignore]` because it
  measures the `rocksdb.total-sst-files-size` CF property instead of directory size, avoiding
  WAL/memtable noise): `rocks/tests.rs` (`clear_reclamation_*`,
  `warm_clear_tier_shard_reclaims_and_counts`, `reclamation_disabled_by_config_knob`),
  `rocks/reclaim.rs` (guard unit tests), `wal/tests.rs`
  (`test_wal_clear_reclamation_end_to_end`).
