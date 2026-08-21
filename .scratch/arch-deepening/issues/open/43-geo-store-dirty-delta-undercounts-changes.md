# 43 — GEO `STORE` credits 1 dirty change regardless of how many members it stored, starving change-based save points

Status: needs-triage

## What to build

Nothing in `frogdb-server/crates/commands/src/geo.rs` touches `dirty` — a `grep -n dirty
geo.rs` returns nothing — so all three GEO store branches leave `ctx.effects.dirty_delta` at
its default `0`. The three sites are `GEOSEARCHSTORE` at `geo.rs:439-456`, `GEORADIUS … STORE`
at `:543-557`, and `GEORADIUSBYMEMBER … STORE` at `:708-722`; each builds a fresh
`SortedSetValue`, calls `ctx.store.set(dest, …)` and returns the member count.

`update_dirty_counter` (`core/src/shard/post_execution.rs:690-699`) maps a `0` delta to exactly
**one**:

```rust
let dirty_amount = if dirty_delta > 0 { dirty_delta as u64 }
                   else if dirty_delta < 0 { 0 }
                   else { 1 // Default: most write commands count as 1 dirty change
                   };
```

Redis's store branch does `server.dirty += returned_items` — the size of the result array it
just wrote. So a `GEOSEARCHSTORE` that stores 10,000 members credits **1** change where Redis
credits **10,000**: an under-count of 9,999 per call, flowing straight into
`rdb_changes_since_last_save`. That counter is what `save <seconds> <changes>` thresholds and
every "is a snapshot due" heuristic read, so a workload whose only writes are GEO stores can
run essentially forever without tripping a change-based save point — a durability-window
regression that is invisible until a crash. This is **LIVE on main today**. The direction was
re-verified during review: `georadiusGeneric` builds the destination zset from
`array[0..returned_items]` and replies that count, never the zset's length, so
`results.len()` in FrogDB *is* `returned_items` by construction.

Fix direction: one line per store branch (one line total once proposal 96 folds the three into
a shared `store_geo_results` helper) —

```rust
ctx.effects.dirty_delta = results.len() as i64;
```

Note that `results.len()` is the correct source, not `dest_zset.len()`: the two spellings
already disagree in-tree (`geo.rs:450` uses `results.len()`, `:552` and `:717` use
`dest_zset.len()`), and `results.len()` is the one that matches Redis. The clear branches are a
separate defect in the opposite direction — see issue 39 — and both are one-line fixes in the
same helper, so they are natural to land together.

## Acceptance criteria

- [ ] A GEO `STORE`/`STOREDIST` that stores N members advances `rdb_changes_since_last_save`
      by N, not by 1, for all three commands (`GEOSEARCHSTORE`, `GEORADIUS … STORE`,
      `GEORADIUSBYMEMBER … STORE`).
- [ ] Reply values are unchanged at all three sites.
- [ ] Regression test `geo_store_dirty_delta_matches_stored_count` in
      `crates/redis-regression/tests/geo_regression.rs`: reads
      `rdb_changes_since_last_save` from `INFO persistence`, runs a multi-member
      `GEOSEARCHSTORE`, re-reads, asserts the delta equals the stored member count.
      **Fails at HEAD** (delta is 1).
- [ ] `just test frogdb-redis-regression geo_store_dirty` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 96
(`.scratch/arch-deepening/proposals/96-geo-store-unification.md`), §Problem 4b (added by the
review as a MINOR addition to §Problem 4).

## Comments
