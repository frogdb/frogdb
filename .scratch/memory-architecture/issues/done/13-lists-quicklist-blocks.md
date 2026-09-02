# 13: lists as quicklist chains of listpack blocks

Status: done
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R7
Area: frogdb-types (list.rs) + commands (list family)
Phase: 4 — value representation, first mover

## Why

`ListValue` is `VecDeque<Bytes>` (`frogdb-server/crates/types/src/types/list.rs:11`): one
heap-allocated refcounted `Bytes` per element, plus the deque's pointer array. A list of a
million 10-byte elements pays ~48 bytes of `Bytes` overhead per element — 5x the payload —
scattered across the allocator, which is precisely the fragmentation and cache-miss profile R7
exists to kill. (The doc comment on `ListValue` claims "doubly-linked list of values"; it has
never been one. Fix the comment while in there.)

Redis solved this in 3.2 with **quicklist**: a doubly-linked chain of listpack blocks, each
block holding many small elements contiguously, with per-block size limits
(`list-max-listpack-size`, default 128 entries / 8KB per block). Valkey inherits it unchanged;
Dragonfly reimplemented the same shape (`QList`). It is the proven design for exactly this
workload, and FrogDB already has a hand-rolled listpack encoding for small hash/set
(`types/src/types/hash.rs` `HashEncoding::Listpack`) to draw on.

Lists go first among the R7 conversions because they are the worst offender (every element is a
separate allocation regardless of size — hash/set at least have a small-form listpack already)
and because the quicklist block machinery (block sizing, split/merge, cursor iteration) is the
template issues 14–16 reuse.

## What to build

### 1. Listpack block encoding for list elements

Extract the listpack buffer building/iteration helpers from `hash.rs`/`set.rs` into a shared
module (`types/src/listpack.rs`) rather than hand-rolling a third copy. Lists need the
sequence flavor (elements only, no field/value pairing): append, prepend, insert-at, remove-at,
get-at, iterate forward/backward over a contiguous `Vec<u8>`/`BytesMut` buffer.

### 2. Quicklist chain

`ListValue` becomes a chain of blocks:

- Each block: one listpack buffer + cached entry count. Cap per block at the Redis defaults
  (128 entries / 8KB, whichever first) as constants next to `ListpackThresholds`
  (`types/src/types/mod.rs:637`); config plumbing can follow later, deviation-documented if we
  skip it.
- The chain itself can be a `VecDeque<Block>` — FrogDB does not need Redis's intrusive
  linked-list nodes; a deque of blocks gives O(1) both-end push/pop and keeps the code safe
  Rust. Middle insertion (`LINSERT`) splits a block; deletion merges adjacent underfull blocks
  (Redis merges when combined fill < limit).
- Elements larger than the block value cap get a solo "plain" block (Redis's `PLAIN` node),
  so one huge element does not force every neighbor into its own allocation.

### 3. Command-surface preservation

The list command family (LPUSH/RPUSH/LPOP/RPOP/LRANGE/LINDEX/LSET/LINSERT/LREM/LMOVE/LPOS,
blocking variants, and the `memory_size()` used by MEMORY USAGE and the memory-conservation
checker) must be behavior-identical. Rank/index operations walk blocks by cached counts —
O(blocks + in-block scan), same complexity class as Redis. `memory_size()` must remain
run-stable (see the deterministic-seed precedent in `types/src/skiplist.rs`).

### 4. No compression tier

Redis optionally LZF-compresses interior quicklist nodes (`list-compress-depth`). Skip it:
document as a deviation (depth 0 = off is the Redis default anyway). Revisit only with a
workload that wants it.

## Acceptance criteria

- [ ] `ListValue` stores elements in listpack blocks; no per-element `Bytes` for elements under
      the block value cap.
- [ ] Shared listpack module used by lists (hash/set migration to it may land here or in
      issue 14, but no third copy of the encoding exists).
- [ ] Full list-family regression suite green, including blocking commands and LMOVE/LMPOP.
- [ ] MEMORY USAGE on a converted list reports the block-based size, run-stable.
- [ ] A memory-shape test asserts a 100k-small-element list allocates O(blocks), not
      O(elements) (count via allocation-free comparison of `memory_size()` before/after, or the
      arena-sampled figure from issue 03's machinery).
- [ ] Fuzz target for the listpack sequence encoding (split/merge/insert-at under random ops
      vs a `VecDeque<Bytes>` model).

## Test boundary

Level 1 model-based fuzz/property tests for the block encoding (random op sequence vs naive
model). Level 2 for the command family via the existing regression suite. No new socket-level
tests needed.

## Spec rows at R15

None specific to lists — value representation is below the `specs/memory.md` seam. Eviction
and OOM-verdict rows (issue 22) apply to converted values automatically via `memory_size()`.

## Out of scope

Hash/set/zset/stream conversions (issues 14–16), listpack node compression, config knobs for
block limits beyond constants, arena/block *storage* placement (values still allocate from the
shard's bound arena per issue 03 — that is already the right arena).

## Depends on

Nothing hard. Lands after the phase-3 table work ([issues 10–12 reserved](../)) only by
sequencing convention — the block encoding itself touches neither the Dashtable nor eviction.

## Resolution

Landed 2026-09-01 on `mem-arch-integration` (commits `2cd246b6`..`358b19c0`, six
cherry-picks from the implementation worktree).

- New shared `frogdb-server/crates/types/src/listpack.rs`: contiguous buffer, entry format
  `[len: LEB128][payload][backlen: reverse varint]` — O(1) tail ops and reverse iteration.
  Documented deviations from Redis listpack: no integer encoding, no header, no EOF byte.
- `ListValue` is now `VecDeque<Block>` with `Block::{Packed(Listpack), Plain(Bytes)}`;
  Redis-matching caps (128 entries / 8192 bytes, plain blocks for oversized elements),
  split on full-block LINSERT, merge of underfull neighbours on deletion. `memory_size()`
  is a pure function of contents + partitioning (run-stable).
- Model-based fuzz target `listpack_ops` (vs `VecDeque<Bytes>` model): 777k execs clean,
  values straddling the plain-block threshold.
- Review round 1 findings (fuzz model trim clamp, fuzz run evidence, oversized mid-chain
  coverage, dead limits field, `replace` fast path) all fixed and re-review-verified.
- Follow-ups spun out: [issue 14](../) is now a re-encode (migrate hash/set small forms
  onto the shared module — three codecs coexist until then); `serialize_list` `to_vec`
  materialisation noted for the zero-copy work.
