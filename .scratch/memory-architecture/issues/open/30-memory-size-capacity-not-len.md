# 30: one `memory_size` figure — capacity, not len

Status: ready-for-agent
Type: AFK
Origin: whole-branch review 2026-09-05
Area: frogdb-types + frogdb-core + frogdb-memory
Phase: 6 — polish. Follows the lock; **locked-area work: spec-first discipline applies.**

## Why

`memory_size` runs on two philosophies at once, and the spec could only describe one of them.

- **Len-based.** `Listpack::byte_len` is `self.buf.len()`
  (`frogdb-server/crates/types/src/listpack.rs:161`); `QuickList::memory_size` counts each
  block's encoded bytes plus a fixed per-block overhead, "never the buffers' spare capacity"
  (`types/src/types/list.rs:719`).
- **Capacity-based.** `BlockStore::allocated_bytes` sums `buf.capacity()` across blocks plus
  the `blocks` and `free` vectors' capacities (`types/src/blockstore.rs:126`);
  `SortedSet::memory_size` adds the entry table, free list and index table *capacities*
  (`types/src/types/sorted_set.rs:718`).

Both are deterministic functions of the op history, so the run-stability rule holds under
either and does not pick between them. But they answer different questions, and the shard's
`maxmemory` verdict compares one number to one limit. A keyspace of listpack-backed hashes
under-reports its true footprint by up to about 2× for freshly doubled buffers; a keyspace of
block-backed values over-reports its slack. The same `maxmemory` therefore means two different
things depending on what the keys happen to hold.

Capacity is the honest figure: it is what the allocator is actually holding, and the budget
exists to bound what the allocator holds.

A second symptom, from the same review: `Value::re_encode`'s doc claims it reclaims "a listpack
buffer that grew and shrank". It does reclaim it — but with len-based sizing that reclaim was
never counted, so `DEBUG RE-ENCODE`'s before/after figures cannot move because of it. The
operator-visible tool for compaction cannot show the compaction it advertises. Moving listpacks
to capacity makes that reclaim visible, which is the behaviour an operator running
`DEBUG RE-ENCODE` expects.

## What to build

**Ruled capacity (D9).** Move listpack-backed forms to capacity-based sizing; the hash and set
listpack forms (`types/hash.rs`, `types/set.rs` `memory_size`) follow the same accessor. One
philosophy, stated once in `specs/memory.md`.

Under the capacity ruling:

1. `Listpack::byte_len` keeps its meaning (it is the encoded length, and callers use it for
   threshold decisions); sizing switches to a separate capacity-reporting accessor, so a
   threshold check and a memory charge stop sharing one number.
2. `QuickList::memory_size` counts block buffer capacity plus the per-block overhead.
3. The FM-MEMORY-004 NOT-observable sentence added by the whole-branch fix round — which
   currently enumerates both philosophies and names this issue — collapses back to one
   philosophy.
4. `Value::re_encode`'s doc drops the caveat this issue adds and goes back to claiming the
   listpack reclaim, because it is then true of the accounting too.
5. `DEBUG RE-ENCODE` before/after shows listpack slack reclaim. Worth a forcing test: grow a
   listpack-backed hash, shrink it, assert `memory_size` is unchanged, `DEBUG RE-ENCODE`, assert
   it dropped.

**The constants will move.** The txn-cap tests and the eviction-verdict tests bind to concrete
`memory_size` figures; every one of them will need its constant re-derived, not merely nudged
until green. Budget the work for that — it is the bulk of the change, and a constant re-derived
by fitting to the new output is a test that has stopped forcing anything.

## Acceptance criteria

- [ ] One sizing philosophy across `listpack.rs`, `list.rs`, `blockstore.rs`, `sorted_set.rs`,
      stated once in `specs/memory.md`.
- [ ] FM-MEMORY-004's NOT-observable no longer enumerates two philosophies; the issue-30
      cross-reference is gone.
- [ ] `Value::re_encode`'s doc matches what the accounting does.
- [ ] Forcing test: `DEBUG RE-ENCODE` on a churned listpack-backed value moves `memory_size`.
- [ ] Every txn-cap and eviction-verdict constant re-derived from the new definition, with the
      derivation recorded at the test.
- [ ] `just lint-spec` green; `just test frogdb-server` green.

## Files likely touched

- `frogdb-server/crates/types/src/listpack.rs`
- `frogdb-server/crates/types/src/types/list.rs`, `sorted_set.rs`, `mod.rs` (`re_encode` doc)
- `frogdb-server/crates/types/src/blockstore.rs`
- `specs/memory.md` (FM-MEMORY-004)
- the txn-cap and eviction-verdict tests that bind to `memory_size` constants

## Out of scope

The per-shard arena sample and its relationship to accounted contents (FM-MEMORY-008 —
a different figure answering a different question). RSS and the allocator's process-wide
gauges. Changing the `maxmemory` verdict's shape or the exactness of its boundary.

## Depends on

Nothing.

## Blocks

Nothing.

## Decisions

D7, D9

## Conflicts with

[issue 31](31-budget-refusal-prefix-and-counting.md) (both edit `specs/memory.md`) — 31 lands first.
