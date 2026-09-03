# 15: zset member bytes into block storage

Status: done
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R7
Area: frogdb-types (sorted_set.rs, skiplist.rs)
Phase: 4 — value representation

## Resolution

Landed on `mem-arch-integration` (four commits: block conversion, SET..GET old-value
boxing, review fix round, `Value` size const-assert).

- Large-form zset members now live in per-value `BlockStore` blocks. The member table
  holds `(handle, score)` rows addressed by stable `u32` slots; the member→score index
  is a `HashTable<u32>` keyed by member bytes resolved through the store; the skiplist
  stores slots and resolves member bytes via a borrow-based resolver passed into
  insert/remove — the structure stays a plain owned value. Compaction patches handles
  in the table (slots never move), so slot lists collected before a batch removal stay
  valid throughout it.
- **BTree score-index backend retired.** Nothing selected it in production config; a
  second backend doubled the conversion surface. `SCORE_INDEX_BACKEND` and the
  `sorted-set-index` config key are gone. **Breaking change:** the config struct is
  `deny_unknown_fields`, so a config file still carrying `sorted-set-index` is now
  rejected at startup. Acceptable pre-production; docs regenerated (`just docs-gen`).
- **No zset small form existed** (verified: hash/set have listpack forms, zset never
  did), so "small form unchanged" is vacuous — nothing converted or preserved.
- `Value::SortedSet` is boxed: the block-backed value is 280 bytes inline vs. 200 for
  the next-largest variant; boxing keeps `Value` at 208 bytes, enforced by a
  `const_assert` at the enum. `SetResult::OkWithOldValue` stays boxed (208 > clippy's
  200-byte threshold; payload exists only on SET ... GET).
- Range removals (`remove_range_by_rank/score/lex`) are slot-based — member bytes are
  resolved by borrow for the skiplist search, never materialized as `Bytes`.
- Verified: full zset+geo regression suite 185/185 (geo under `cmd-full`, no geo code
  changes needed); frogdb-types 458/458 incl. model-based proptest vs `BTreeMap` model
  with compaction and a deterministic churn test proving compaction + handle patching;
  `memory_size()` determinism preserved (run-stable seed untouched); full
  `frogdb-server` suite 2350/2350; workspace clippy `-D warnings` clean;
  `just docs-gen-check` green.

## Why

The zset score index is already in good shape: `types/src/skiplist.rs` is an index-based
skiplist modeled on Redis's zskiplist — safe Rust, `Vec<Option<Node>>` with a free list and
`u32` indices instead of raw pointers, span-based O(log n) rank queries, deterministic level
seed so `memory_size()` is run-stable. Nodes live contiguously in one growable vec. **Keep it.**
(PRD shorthand calls it the "arena skiplist"; the accurate description is index-based node
storage in a `Vec` — no unsafe, no raw-pointer arena. Wording in code/docs should follow the
code.)

What is *not* in good shape is member storage: every member is a refcounted `Bytes`, held once
in the skiplist node and once in the member→score map. Large zsets pay the same per-entry
allocation scatter as large hashes. `ScoreIndex` also has a selectable `BTree` backend
(`sorted_set.rs`, `SCORE_INDEX_BACKEND: AtomicU8`, skiplist default at line 149) — the member
storage change must work under both or the BTree backend must be retired first (recommended:
retire it in this issue if nothing selects it in production config; a second backend doubles
the conversion surface for no user-visible benefit).

## What to build

### 1. Member bytes into blocks

Reuse the block storage from [issue 14](../): members
live in per-value blocks; the skiplist node and the member→score index hold a compact handle
`(block, offset, len)` instead of a `Bytes`. One copy of each member's bytes total (today:
bytes shared via `Bytes` refcount but two handle+header sets). Compaction as in issue 14.

### 2. Comparator plumbing

Skiplist ordering is (score, member-lex). Nodes comparing members must resolve handles through
the block store — the skiplist gains a borrow of the block store at operation time (pass
`&BlockStore` into insert/delete/rank calls) rather than storing a reference, keeping the
structure a plain owned value that moves between shards (SMOVE/COPY/MIGRATE paths).

### 3. Small form unchanged

The listpack small form for zsets (if present — verify; hash/set have one, zset small form
follows the same `ListpackThresholds` pattern) stays as-is; only the large form converts.

### 4. Range surfaces

ZRANGEBYLEX/ZRANGEBYSCORE/ZRANK iterate members: return paths still need `Bytes` for the wire.
Materialize response `Bytes` by copying from blocks at reply time — replies are transient and
this matches R7's principle that long-lived storage is packed and wire buffers are transient
(the zero-copy *input* story is [issue 19](../); output copies here
are fine and already exist today as refcount bumps).

## Acceptance criteria

- [ ] Large-form zset members stored in blocks; skiplist and score map hold handles, not
      `Bytes`.
- [ ] `SCORE_INDEX_BACKEND` either retired or both backends converted (decide early, note in
      the resolution).
- [ ] Full zset regression suite green: ZADD GT/LT/NX/XX, ZRANGEBYLEX edge encodings,
      ZRANDMEMBER, ZPOPMIN/MAX, blocking variants, ZRANGESTORE.
- [ ] `memory_size()` run-stable (deterministic seed behavior preserved — MEMORY USAGE and the
      memory-conservation checker read it).
- [ ] Model-based fuzz: block-backed zset vs `BTreeMap`-based model under random ops including
      compaction.

## Test boundary

Level 1 property/fuzz vs model. Level 2 via existing zset regression suite. The
memory-conservation checker is the sentinel for `memory_size()` drift.

## Spec rows at R15

None new; generic eviction/OOM rows cover zsets through `memory_size()`.

## Out of scope

Skiplist algorithm changes, score storage format (f64 stays inline in nodes), small-form
changes, GEO family (sits on zset and inherits the conversion; `geo` is a non-default cargo
feature — run its suite under `full` once, but its code should not need changes).

## Depends on

[Issue 14](../) — block store with compaction and its
handle conventions.
