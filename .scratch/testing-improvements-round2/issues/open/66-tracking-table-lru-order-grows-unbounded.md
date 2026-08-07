# `TrackingTable::lru_order` grows without bound; only the 1M-key eviction path compacts it

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/02 F2 · MASTER.md §3 (availability / resource)
Score: severity 4 · likelihood 4 · effort 1 · priority 19
Area: frogdb-core / client-side caching (tracking)

## Context

One RESP3 tracking client on a churning keyspace is enough: every read of a *new* key pushes an
entry onto `lru_order` and nothing pops it. Removal from `key_to_clients` on invalidation or
disconnect deliberately leaves the `lru_order` entry behind, and the only compaction lives
inside `evict_lru`, which runs only once `key_to_clients` exceeds `max_keys` (1M by default).
A read-then-write workload keeps `key_to_clients` small forever, so compaction never runs and
`lru_order` grows monotonically with the number of *distinct keys ever read* — unbounded RSS
growth outside `maxmemory` accounting, ending in an OOM-kill / crash-loop.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`crates/core/src/tracking.rs:103-177`. `record_read` pushes to `lru_order` on
every first-sight key. `invalidate_keys` removes from `key_to_clients` with the comment
"*we don't remove from lru_order here — stale entries are cleaned lazily during eviction*",
and `remove_connection` (`:191-204`) has the same "*Stale lru_order entries cleaned lazily
during eviction*" note. Compaction lives **only** inside `evict_lru` (`:207`), which is
called only from `while self.key_to_clients.len() > self.max_keys`.

Why nothing catches it: no test asserts `lru_order.len()`; `evict_lru` is `single-test`.

## What to fix

1. Bound `lru_order` independently of `key_to_clients` — compact on a size/staleness trigger,
   or remove the entry eagerly in `invalidate_keys` / `remove_connection`.
2. Include the tracking table's real footprint in the memory accounting the operator can see,
   so the growth is not invisible to `maxmemory`.
3. Expose a `#[cfg(test)]`-visible `lru_len()` (or make compaction observable via
   `memory_usage()`) so the invariant is assertable without reaching into internals.

## Acceptance criteria

- [ ] A unit test on `TrackingTable` registers one tracked conn and loops 10_000×
      {`record_read(key_i)`, `invalidate_keys(&[key_i])`} with `max_keys` at its 1M default,
      then asserts the exposed length/capacity accessor stays O(live keys), not O(iterations).
      Fails today.
- [ ] The same assertion holds after `remove_connection` rather than `invalidate_keys`.
- [ ] `evict_lru` gains at least one further test so it is no longer `single-test`.

## Test boundary

Level 1 (pure unit) — a pure data-structure invariant with no engine involvement; driving 10k
reads through a socket would be strictly worse. Not level 2 or above: nothing about the shard,
the connection or RESP3 push framing participates in the growth.

## Depends on

nothing — the accessor named in `## What to fix` step 3 is part of this work, not separate
infrastructure

## Re-triage 2026-08-06

**Verdict: still-valid**

Unchanged code. In `frogdb-server/crates/core/src/tracking.rs`: `record_read` (now `:103-133`)
pushes to `lru_order` at `:126` on every first-sight key; `invalidate_keys` (now `:141-177`) still
carries the "*we don't remove from lru_order here — stale entries are cleaned lazily during
eviction*" comment at `:174-175`; `remove_connection` (`:191-204`) repeats it at `:199`; and the
only compaction is inside `evict_lru` (`:207-232`), reachable solely from the
`while self.key_to_clients.len() > self.max_keys` loop at `:129-131`. `lru_order` is only ever
fully drained by `flush_all` (`:187`). No `lru_len()`/capacity accessor exists and no test asserts
`lru_order.len()` (the single reference at `:499` asserts `is_empty()` after a flush). Tracking is
outside the four locked areas, so no FM row covers it.
