# 14: block-packed large forms for hash and set

Status: done
Type: AFK
Origin: memory-architecture PRD phase filing, 2026-09-01 — [PRD.md](../../PRD.md) R7
Area: frogdb-types (hash.rs, set.rs)
Phase: 4 — value representation

## Why

Small hashes and sets already have a compact form: `HashEncoding::Listpack`
(`types/src/types/hash.rs:168`) and `SetEncoding::Listpack` (`types/src/types/set.rs:68`),
promoted at the Redis-default thresholds (`ListpackThresholds`, 128 entries / 64-byte values,
`types/src/types/mod.rs:637`). But past the threshold both fall off a cliff into
`HashMap<Bytes, Bytes>` / `HashSet<Bytes>`: every field and value becomes its own refcounted
heap allocation plus hashbrown table overhead. A 10k-field hash of short fields pays more in
`Bytes` headers than in payload, spread across the allocator — the R7 fragmentation profile.

Redis's large form (dict of sds strings) has the same per-entry allocation cost, so here we are
not mirroring Redis — we are beating it, which the project goals explicitly invite when the
deviation is an improvement. Dragonfly's DenseSet (for sets) packs entries to cut per-entry
overhead for the same reason. The design: keep the hash-table *index* for O(1) lookup, but move
the *bytes* into shared blocks so entries stop being individual allocations.

## What to build

### 1. Block-backed byte storage

Reuse the block machinery from [issue 13](../) (shared
`types/src/listpack.rs` module): entry bytes live contiguously in append-only blocks; the
hash index maps `hash(field) → (block, offset)` or stores small handles. Deleted entries leave
dead space; a block whose live ratio drops below a threshold (~50%) is compacted by rewriting
live entries into the current tail block. This is a per-value mini-arena with compaction, not a
general allocator — keep it dumb.

**Amendment (2026-09-01, from issue 13's review):** issue 13 built `types/src/listpack.rs`
new (LEB128 len + reverse-varint backlen) rather than extracting the existing hash/set
small-form codecs, so three encodings now coexist: `set.rs` (`[mlen:u16][member]`),
`hash.rs` (`lp_hash_*`), and the new module. This issue is therefore a **re-encode**, not a
lift: migrate the hash and set *small forms* onto the shared module too (killing their
hand-rolled codecs), and check for persisted-form implications while doing it — the small
forms serialize through the same persistence path as everything else.

### 2. Large-hash form

Replace `HashEncoding::HashMap(HashMap<Bytes, Bytes>)` (`hash.rs:171`) with an index over
block-stored field+value records. Constraints from the existing code:

- Field expiries (`field_expiries: Option<HashMap<Bytes, Instant>>`, `hash.rs:187`, HEXPIRE
  family) key off field bytes — the expiry map can index by the same block handle instead of
  cloning the field, or stay keyed by `Bytes` copies as today (expiring fields are rare; do not
  let this constraint drive the main design).
- `HRANDFIELD` needs uniform random access — index handles in a dense vec give it for free.
- Promotion from listpack at the threshold now writes into blocks, not a `HashMap` collect
  (current path at `hash.rs` `set()` ~line 278).

### 3. Large-set form

Same treatment for `SetEncoding::HashSet` (`set.rs:71`): index over block-stored members.
`SRANDMEMBER`/`SPOP` random access via the dense handle vec. Note: FrogDB has no intset
encoding for integer sets; that remains a documented deviation, unchanged by this issue.

### 4. Iteration and SCAN cursor stability

HSCAN/SSCAN cursor semantics must survive: the current large forms iterate hashbrown tables
with the Redis-compatible cursor contract. The block-backed index must preserve whatever
guarantee the current implementation gives (check the existing scan tests before designing the
index — if cursors are reverse-bit over buckets, the index needs bucket-shaped iteration).

## Acceptance criteria

- [ ] Large hash and set forms store entry bytes in blocks; no per-entry `Bytes` allocations.
- [ ] Compaction bounds dead space (test: heavy insert/delete churn keeps `memory_size()`
      within a constant factor of live payload).
- [ ] Full hash/set regression suites green, including HEXPIRE family, HRANDFIELD/SRANDMEMBER
      distribution tests, and SCAN guarantees.
- [ ] MEMORY USAGE run-stable on converted forms.
- [ ] Model-based fuzz: random op sequence on block-backed forms vs `HashMap`/`HashSet` model,
      including compaction triggers.

## Test boundary

Level 1 property/fuzz vs naive model. Level 2 regression via existing suites. SCAN cursor
tests are the highest-risk existing surface — run them first.

## Spec rows at R15

None new — same position as [issue 13](../): below the memory-spec
seam, covered by the generic eviction/OOM rows via `memory_size()`.

## Out of scope

Small-form listpack encodings (already exist, unchanged), zset (issue 15), intset, changing
promotion thresholds, per-value compression.

## Depends on

[Issue 13](../) — the shared listpack/block module and its fuzz
harness. The index-over-blocks design here does not depend on the phase-3 Dashtable work
([issues 10–12 reserved](../)); the keyspace table and per-value byte storage are
separate structures.

## Resolution

Landed 2026-09-02 on `mem-arch-integration` (picks `3e7fe83b`..`b603871a`, 3 commits).

What shipped: `BlockStore` (`types/src/blockstore.rs`) — append-only block arena with
per-block compaction: handles are `(block, offset, len)`, block capacities ramp 512 B →
16 KiB (oversized records get exactly-sized dedicated blocks), `remove` marks a fully-dead
or mostly-dead (≥4 KiB and ≥half dead, non-tail) block as the compaction candidate, and
`compact` moves at most one block's live bytes (≤8 KiB for ratio victims; zero for
fully-dead ones), recycling freed slots so churn never grows the block table. `BlockHash`
and `BlockSet` replace `HashMap<Bytes,Bytes>`/`HashSet<Bytes>` as the large forms: dense
entry vec + hashbrown index of handles, swap-remove with index repointing, same-length
updates overwritten in place. Set-op results (`SUNION`/`SINTER`/`SDIFF`) build the block
form from sorted members so MEMORY USAGE is run-stable. HRANDFIELD/SRANDMEMBER read
positionally — no full-value materialization. Small forms re-encoded onto the shared
`Listpack` module per the issue-13 amendment, retiring both bespoke u16 codecs.

Review round 1 (6 Important, 5 Minor) fixed; re-review clean (all findings addressed, no
new Critical/Important). Gates: frogdb-types 440/440, frogdb-server 2152/2154 (2
replication load flakes, both pass isolated — no replication code touched), workspace
clippy `-D warnings` 0, lint-spec + quint-check, seam gates.

**Flagged for human decision (review finding 6):** small-form reads lost zero-copy. The
old bespoke codecs returned refcounted `Bytes` slices; the shared `Listpack` stores
`Vec<u8>`, so every HGET/HGETALL/SMEMBERS element on the (default) listpack encoding now
pays `Bytes::copy_from_slice` — a heap allocation per element on the common path. The
consolidation was mandated by the issue-13 amendment and is documented at both `Listpack`
encoding variants; accept the cost, or file a follow-up to give the shared listpack a
`Bytes`-backed buffer.
