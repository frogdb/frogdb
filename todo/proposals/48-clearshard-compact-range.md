# 48 — Space reclamation after ClearShard (compact_range follow-up)

**Status:** proposed
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
2. Whether eviction/warm-tier demotion churn justifies a broader periodic-compaction story —
   out of scope here, but note if evidence appears while implementing.
