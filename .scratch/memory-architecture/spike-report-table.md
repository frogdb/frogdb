# R5 slot layout — segmented-table SPIKE report

Status: informational (spike, not a ruling)
Date: 2026-09-01
Prototype: [`spike-table/`](spike-table/) — throwaway crate, not a workspace member
Rules under test: [PRD.md](PRD.md) R5 (segmented Dashtable-style keyspace, tagged
8-byte slots, SCAN cursors stable across splits), R6 (inline small values), and the
*space reservation* half of R9 (segment-integrated 2Q eviction).
Named consumers: **issue 11** (the production table crate) and **issue 12** (2Q
eviction policy).

## Headline verdicts

| Question | Verdict | One-line reason |
| --- | --- | --- |
| **R5** — segmented extendible-hash table replaces `griddle::HashMap<Bytes, Entry>` | **GO on memory, GO WITH CONDITIONS on speed** | 3.3–6.4× less live memory per entry; lookups are **2.0× slower on `redis-feel`, 2.4× on `sessions`, 3.2× on `counters`** with the hasher matched and the instrumentation off. The residual is the scalar fingerprint loop plus a 4-cache-line bucket; SIMD is *expected* to remove the first and cannot remove the second. [Gate for issue 11](#verdict-2). |
| **R6** — inline small values | **GO, threshold = 7 bytes** | An 8-byte tagged word inlines 59 % of realistic values and 100 % of counter values at **zero** structural cost. |
| Wider slot (16 B word, 15-byte inline) | **NO-GO** | Buys +16 pp of inline values and costs +33.5 B/entry of structure — a net **+22.8 B/entry loss** on `redis-feel`. |
| Inline **keys** | **NO-GO at 8 B, not worth it at 16 B** | Only 0.1 % of realistic keys fit in 7 bytes; the 19.5 % that fit in 15 do not pay for the wider slot. |
| Extendible-hash directory ≤ ~1 B/entry | **GO, by two orders of magnitude** | **0.008 B/entry** measured at 1 M keys (`u32` segment indices, not pointers). |
| SCAN cursor stable across splits | **GO — proven executably** | Reverse-binary counter advanced at the *scanned segment's* local depth; every pre-existing key seen exactly once across 16 mid-scan splits, where the obvious directory walk duplicates **3 500 of 8 000** keys. |
| **R9** space reservation in the segment header | **GO** | 22 B of eviction state + 24 B spare inside the 64-byte (one cache line) header — **0.045 B/entry**, against 4–16 B/entry for any per-key scheme. (Not against Redis's LRU field, which rides free in `robj`'s header word — see [section 4](#4-r9-eviction-space-in-the-segment-header--reservation-only).) |
| Worst single-operation split stall | **CONCERN** | A split rehashes all **808** entries in the segment and moves **404** of them: **~44 µs median**, ~110 ns per moved entry, hit by 0.2 % of inserts. Visible at p99.9. Issue 11 needs the mitigation in [Follow-ups](#follow-ups). |

Machine: Apple M1-class, 10 physical cores, macOS 26.5.1, rustc 1.92.0, jemalloc 5.3.0
via `tikv-jemalloc-sys 0.6.1` (`--enable-stats`), release + debug symbols, single-threaded
(per R2/R3 the table is never shared).

**How the timings were taken, and how far to trust them.** This box was building four
other worktrees throughout; 1-minute load ranged from 13 to 45 across the runs and a
single timed pass measures the scheduler as much as the layout. So: every read-side
number is the **best of 5 passes** over the built table inside one process, and then the
**minimum across three whole-process runs**, each of which records its own load average.
Contention can only make a pass slower, so the minimum is the best available estimate of
the uncontended cost; treat every latency here as an **upper bound with a wide error
bar**, and the prototype-vs-baseline *ratio* as the number that carries meaning. Insert
timings and the split-stall distribution are single-shot by construction (the table is
built once) and are the most contention-exposed figures in the report — they are
explicitly flagged where they appear. Memory numbers are load-independent and agreed to
within 0.3 B/entry across every run.

---

## What was built

`spike-table/` is one table implementation parameterised over the slot word, so every
variant below runs the *same* segment, directory, split, probe and cursor code and the
only difference is the word type:

| Variant | Key word | Value word | Slot B | Slots/bucket | Segment B | Segment slots | What it isolates |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `ptr8` | 8 B, no inlining | 8 B, no inlining | 16 | 14 | 15 424 | 840 | The no-inline control |
| `int8` | 8 B, no inlining | 8 B, ints inline (61 bit) | 16 | 14 | 15 424 | 840 | Small-int inlining alone |
| `str7` | 8 B, ≤ 7 B inline | 8 B, ≤ 7 B inline | 16 | 14 | 15 424 | 840 | Short-string inlining |
| `str15w` | 16 B, ≤ 15 B inline | 16 B, ≤ 15 B inline | 32 | 7 | 15 424 | 420 | A wider slot at halved bucket capacity |
| `hybrid` | 8 B, ≤ 7 B inline | 16 B, ≤ 15 B inline | 24 | 9 | 14 944 | 540 | Narrow keys, wide values |

Three workload shapes, generated from a fixed seed (`spike-table/src/workload.rs`):

| Shape | Keys | Values |
| --- | --- | --- |
| `counters` | `cnt:{i}`, mean 9.9 B | 100 % integers |
| `sessions` | `sess:{16 hex}:{i}`, mean 27.9 B | 43-byte tokens, 0 % inlinable |
| `redis-feel` | 8–48 B, mean 28.0 B (19.5 % ≤ 15 B) | 45 % integers, 30 % ≤ 15 B, 20 % 16–64 B, 5 % 65–512 B |

Reproduce with `cargo run --release --bin sweep` from inside the crate.

---

## (1) Slot word format and the inline threshold — R5, R6

### Method

Byte 0 of a word carries a 3-bit tag in its low bits (`TAG_PTR = 0b000`,
`TAG_INT = 0b001`, `TAG_STR = 0b010`) and the inline length in its high bits. Payloads
are 8-aligned, so a pointer word needs **no masking on the hot path** — the tag bits of
a live pointer are already zero and the word *is* the pointer. Integers are stored
shifted left by 3, keeping 61 significant bits in an 8-byte word (`fits_int` falls back
to an out-of-line 8-byte LE payload for the rest); short strings use bytes 1..8 (or
1..16 in the wide word).

Live bytes are jemalloc `stats.allocated` after an `epoch` advance, delta across the
build loop — the same ground truth the phase-1 spike used. `struct B/e` is directory +
segments only; the difference between the two columns is out-of-line key/value payload.

### Results — bytes per entry, 1 M entries

| Shape | griddle (live) | `ptr8` | `int8` | `str7` | `str15w` | `hybrid` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `counters` | 416.1 | 81.3 | 65.3 | **65.2** | 67.0 | 74.4 |
| `sessions` | 480.3 | 145.6 | 145.3 | **145.4** | 179.0 | 154.9 |
| `redis-feel` | 463.6 | 119.0 | 111.6 | **109.2** | 132.0 | 112.7 |

Inline share, and the structure that buys it:

| Shape | variant | inline keys | inline values | struct B/e | dir B/e | occupancy | mean bucket fill | stash share |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `counters` | `str7` | 0.1 % | 100 % | 33.6 | 0.008 | 0.581 | 8.47/14 | 2.88 % |
| `counters` | `str15w` | 100 % | 100 % | 67.1 | 0.016 | 0.581 | 4.23/7 | 2.95 % |
| `redis-feel` | `int8` | 0.0 % | 45.0 % | 33.6 | 0.008 | 0.581 | 8.47/14 | 2.84 % |
| `redis-feel` | `str7` | 0.0 % | 59.0 % | 33.6 | 0.008 | 0.581 | 8.47/14 | 2.84 % |
| `redis-feel` | `str15w` | 19.5 % | 75.0 % | 67.1 | 0.016 | 0.581 | 4.23/7 | 2.95 % |
| `redis-feel` | `hybrid` | 0.0 % | 75.0 % | 41.9 | 0.016 | 0.724 | 6.65/9 | 4.75 % |

`struct B/e` is charged at the **allocated size class**, not `size_of`: a 15 424 B
segment is served from jemalloc's 16 384 B class, so the 8-byte-word layouts cost
16 384 / 488 live entries = 33.6 B/entry rather than the 31.6 that `size_of` suggests.
The 6.2 % of rounding is real memory and [follow-up 5](#follow-ups) reclaims it.

Probe lengths are identical across the 8-byte variants and slightly better for the wide
one (fewer slots per bucket ⇒ fewer collisions per bucket):

| variant | buckets touched on hit | on miss | max |
| --- | ---: | ---: | ---: |
| `ptr8` / `int8` / `str7` | 1.67 | 2.32 | 6 |
| `str15w` | 1.59 | 2.16 | 6 |
| `hybrid` | 1.62 | 2.35 | 6 |

### Verdict — the inline threshold is **7 bytes**, in an 8-byte word

- **Small-int inlining is free money.** `ptr8 → int8` on `counters` is 81.3 → 65.3
  B/entry (−19.7 %) for no structural change: the 8-byte heap record and its 8-byte
  header disappear entirely.
- **Short-string inlining is also free**, and strictly dominates int-only inlining:
  `int8 → str7` on `redis-feel` lifts the inline share 45 % → 59 % and saves another
  2.4 B/entry at *identical* slot width. There is no reason to inline integers but not
  7-byte strings — the tag scheme already has the room.
- **A wider slot does not pay for its halved bucket capacity.** `str15w` inlines 75 %
  of `redis-feel` values against `str7`'s 59 %, but its segment holds 420 slots instead
  of 840, so structure doubles from 33.6 to 67.1 B/entry. Net **+22.8 B/entry** — the
  wide word loses on every shape, including `counters`, where it inlines *everything*
  (67.0 vs 65.2). The hybrid 24-byte slot is a smaller version of the same loss
  (+3.5 B/entry on `redis-feel`).
- **Keys stay out of line.** Only 0.1 % of realistic keys fit in 7 bytes, so key
  inlining contributes nothing at the winning width. The tag scheme is still symmetric
  across key and value words because that symmetry costs zero bytes and keeps one
  encode/decode path.

**Ruling for issue 11: one 8-byte tagged word per key and per value, 16-byte slot,
14 slots per 256-byte bucket. Inline byte strings up to 7 bytes and integers up to 61
significant bits; everything else is an 8-aligned pointer to a heap record with an
8-byte `{len: u32, rc: u32}` header** (the `rc` is R6's non-atomic same-core refcount —
allocated and sized here, not exercised).

---

## (2) Segment and directory layout — R5

### Method

`spike-table/src/segment.rs` and `src/table.rs`. Dashtable's shape: 56 regular + 4 stash
buckets per segment; an item lives in its home bucket, the next bucket, or a stash;
growth adds exactly one segment per split. The split repoints only the directory entries
that alias the segment being split, by **striding** over them — first index
`old_key | 2^depth`, step `2^(depth+1)` — instead of testing all `2^global_depth`
entries. `tests/table.rs::every_directory_entry_routes_to_its_own_segment` pins the
invariant the stride relies on (every directory index routes to a segment whose
`segment_key` is that index masked to the segment's local depth), because an off-by-one
stride loses keys silently. The directory is `Vec<u32>` of segment indices. Split stalls
are measured by timing **every** insert individually and bucketing them by what they did;
the split path also counts the slots it rehashes, the slots it actually moves, and the
directory words it writes.

### Results

Layout, asserted by `tests/table.rs::bucket_is_four_cache_lines_and_header_is_one`:

```
bucket   = 32 B metadata + 14 × 16 B slots = 256 B  (four cache lines, no tail padding)
metadata = fp[14] | occupied: u16 | stash_map: u8 | stash_count: u8 | pad[14]
segment  = 64 B header + 60 × 256 B buckets = 15 424 B, 840 slots
```

Directory growth at 1 M `redis-feel` entries: **2 048 entries, 2 047 splits, 11
doublings, 8 KB total = 0.008 B/entry**, two orders of magnitude inside the ~1 B/entry
target. The reasoning is simply that a directory entry is 4 bytes and (with no aliasing,
the worst case) there is one per segment, so the overhead is `4 / entries-per-segment`
B/entry: it stays under 1 B/entry as long as a segment holds more than **4** live
entries, and ours holds ~488 (840 slots at 0.581 occupancy). The directory is free by
construction, not by luck.
`tests/table.rs::directory_overhead_stays_under_one_byte_per_entry` pins this at 500 K
keys.

Split stalls, `redis-feel`, 1 M inserts, nanoseconds. `plain p50` and `split p50` are the
**minimum across the three runs** of a per-run median; the tail columns are from the
quietest run and are the single most contention-exposed numbers in this report — a
million individually timed inserts on a box at load 40 produce multi-millisecond
"splits" that are pure scheduler. Read the p50s; treat the p99 as an upper bound and the
max as noise. `scanned` / `moved` / `dir writes` are counts, not times, and are exact:

| variant | plain p50 | split p50 | split p99 | scanned/split | moved/split | ns per moved entry | dir writes/split | splits | doublings |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ptr8` | 250 | 43 792 | 113 459 | 808 | 404 | 108 | 1.0 | 2 047 | 11 |
| `int8` | 208 | 43 292 | 89 958 | 808 | 404 | 107 | 1.0 | 2 047 | 11 |
| `str7` | 209 | **44 375** | 101 250 | 808 | 404 | **110** | 1.0 | 2 047 | 11 |
| `str15w` | 209 | 22 875 | 55 541 | 382 | 191 | 120 | 1.0 | 4 095 | 12 |
| `hybrid` | 208 | 27 500 | 61 458 | 501 | 250 | 110 | 1.0 | 2 556 | 12 |

### Verdict

**Incremental splits work and the directory is free — but a split is a ~44 µs stall on
0.2 % of inserts, and that is the one number in this spike that is not good enough.**

- **A split rehashes every entry in the segment and moves about half of them.** The
  counts are exact: 808 slots scanned and rehashed, 404 actually relocated to the new
  segment (the rest are rehashed only to discover that bit `local_depth` is 0 and they
  stay). That ratio is a direct consequence of storing 8 fingerprint bits and nothing
  else — there is no way to know which half moves without recomputing the hash.
- **~110 ns per moved entry** (44 375 ns / 404), or ~55 ns per *rehashed* entry, which is
  the cost of one hash over an out-of-line key plus a 16-byte slot copy. Follow-up 2
  attacks the rehash, not the copy: keeping more hash bits per slot removes the 808
  rehashes and leaves 404 copies, so the expected floor is roughly a quarter of today's
  stall. (Earlier this report said "≈160 ns per moved entry, ~420 moved". Nothing had
  counted moved entries; the count is now instrumented and both numbers were wrong.)
- The stall is **linear in entries moved**, not in table size: `str15w` moves half as
  many entries per split and pays half the time (23 µs). So growth cost does not degrade
  as the keyspace grows — but each individual split still lands at p99.9.
- **The directory rewrite is not part of the cost**: 1.0 directory words written per
  split, because the stride visits only the entries that alias the split segment and at
  the global depth there is exactly one. An earlier version of this code scanned all
  `2^global_depth` entries (2 048 iterations per split at 1 M keys) and the report's
  prose claimed otherwise; the code now matches the prose and
  `every_directory_entry_routes_to_its_own_segment` pins the invariant that makes the
  stride correct.
- **Occupancy settles at 0.581**, mean bucket fill 8.47/14, with stash buckets holding
  2.9 % of entries and only 0.1 % of regular buckets full. Dash reports ~0.9; the gap
  is our one real functional deviation (no displacement on insert, below), and closing
  it would take structure from 33.6 to ~21.7 B/entry.

---

## (3) The SCAN cursor — R5, and the hard deliverable

### The scheme

A **reverse-binary counter over segment keys**, advanced at the *scanned segment's local
depth*:

```rust
let idx = (cursor & ((1 << global_depth) - 1)) as usize;   // directory index
let seg = &self.segs[self.dir[idx]];
emit_every_key_in(seg);

let mask = (1 << seg.header.local_depth) - 1;              // the SEGMENT's depth
let mut v = cursor | !mask;
v = v.reverse_bits();
v = v.wrapping_add(1);
v.reverse_bits()                                           // 0 == scan complete
```

This is Redis `dictScan`'s reverse-binary increment, with one change: the mask comes from
the segment that was just scanned, not from the global depth. That single change does
both jobs at once.

- **Grow-only exactly-once.** Visiting directory indices in reverse-binary order means
  the bits the directory *gains* when it doubles are the high bits of the cursor, which
  the counter has not reached yet. A segment split mid-scan therefore moves keys only
  into directory entries the cursor has not yet visited, or leaves them where they are —
  never behind the cursor.
- **Aliases are skipped in one step.** When a segment's local depth is below the global
  depth, several directory entries point at it. Masking at the *local* depth makes the
  increment jump past all of them, so each segment is emitted exactly once no matter how
  many directory entries alias it.
- **Cursors map to segment ids.** After the advance the cursor's only non-zero bits are
  the low `local_depth` bits of the hash that route to the next segment — literally the
  next segment's key. This is what R5 asks for, and it is why the cursor survives the
  table changing shape underneath a paused client.

### The proof — `tests/cursor.rs`

Every test inserts a pre-existing set, then inserts fresh keys **between every SCAN
step**, so splits happen mid-iteration:

| Test | What it asserts | Result |
| --- | --- | --- |
| `reverse_binary_cursor_sees_every_preexisting_key_exactly_once` | 8 000 pre-existing keys, 400 fresh per step, ≥ 4 mid-scan splits (16 observed): every `pre:{i}` seen exactly once | **pass** |
| `linear_directory_cursor_breaks_under_mid_scan_splits` | the counter-example: walking directory indices in order loses the guarantee | **pass** — reports `0 missing, 3500 duplicated` of 8 000 |
| `scan_without_mutation_is_exactly_once` | 20 000 keys, no churn: emitted count == `len()`, no duplicates | **pass** |
| `aliased_directory_entries_are_visited_once` | with an aliased directory, the scan takes exactly `segments()` steps, not `directory_entries()` | **pass** |
| `cursor_guarantee_holds_for_the_hybrid_layout` | the guarantee is a property of the cursor, not of the slot width | **pass** |

RED → GREEN evidence: `aliased_directory_entries_are_visited_once` first failed
(`test needs an aliasing directory: 64 entries, 64 segments` — with a uniform hash the
sampled table happened to be perfectly even) and was fixed by inserting until the
directory is genuinely aliased, which is what the test is about. The counter-example
test is the RED case made permanent: it *asserts* the naive cursor's failure, so the
report's claim has a test behind it rather than an argument.

### Verdict

**GO.** The cursor is 6 lines, needs no per-scan snapshot, no epoch, and no bookkeeping
in the table, and it upgrades today's behaviour: `frogdb-server/crates/core/src/store/
hashmap.rs` currently derives SCAN cursors from a 48-bit content hash of the key, which
is stable but not ordered; this scheme gives Redis's actual guarantee with the actual
Redis cursor semantics.

---

## (4) R9 eviction space in the segment header — reservation only

`SegmentHeader` is exactly 64 bytes, one cache line, asserted by
`tests/table.rs::segment_capacity_and_r9_reservation`:

| Field | Bytes | Role |
| --- | ---: | --- |
| `local_depth` | 1 | extendible hashing |
| `q_state` | 1 | **R9** — 2Q membership: none / A1in / A1out / Am, upper bits spare |
| `victim_cursor` | 1 | **R9** — rotating bucket index, so victim selection inside a segment is O(1) |
| `flags` | 1 | reserved: split-in-progress, pinned, tiering |
| `entries` | 2 | live entries |
| `inline_values` | 2 | spike instrumentation (a production build can reuse these 2 B) |
| `segment_key` | 8 | the SCAN cursor identity |
| `q_prev`, `q_next` | 4 + 4 | **R9** — intrusive 2Q queue links, **segment indices, not pointers** |
| `hits`, `misses` | 4 + 4 | **R9** — counters driving A1in → Am promotion |
| `last_touch` | 4 | **R9** — coarse clock tick |
| `bytes_charged` | 4 | **R8** — bytes this segment has charged to the shard budget |
| `reserved` | 24 | headroom for issue 12 |
| **total** | **64** | one cache line |

R9 therefore holds **22 B reserved plus 24 B of headroom**, and at the measured
occupancy that is **22 / (840 × 0.581) = 0.045 B/entry**.

The comparison to make it against is *not* Redis's LRU field. Redis's 24-bit `lru`
lives in the bit-packed header word `robj` already has (`type:4 | encoding:4 | lru:24 |
refcount`), so its marginal cost is **~0 B/entry**, not 3. The honest statements are:
this design's eviction state costs 0.045 B/entry, which is as good as free; and it is
much cheaper than the obvious alternative for a table with no spare header bits — a
per-key intrusive LRU list at 16 B/entry, or a per-key timestamp at 4–8 B/entry. The
reservation stands on that, and on the fact that our slot is two bare words with **no**
header word to hide bits in.

Queue links are `u32` segment indices deliberately: the header stays 64 bytes on any
target, and the segment vector can reallocate without fixing up links.

No eviction *logic* is implemented — that is issue 12. What this spike establishes is
that issue 12 will not have to grow the header or add a per-key field to get 2Q.

---

## (5) Baseline comparison — the shipped `griddle::HashMap<Bytes, Entry>`

### Method and fidelity

`spike-table/src/baseline.rs` replicates the shipped types rather than taking a path
dependency on `frogdb-types` (which would drag the whole workspace, `usearch` included,
into a throwaway crate). The replication is honest in a specific, checkable way:

- `size_of::<Entry>()` is **exact**: `ValueLocation::Hot` holds an `Arc<Value>`, a
  pointer, so `Entry`'s size depends only on `KeyMetadata` and `KeyType`, both copied
  field-for-field. Measured: `Bytes` 32 B, `Entry` 64 B, `(Bytes, Entry)` 96 B.
- `size_of::<Value>()` is a **lower bound**: the real enum has 15 variants, the replica
  has the String arm. Every baseline number is therefore *generous to the incumbent*.
- **Both sides hash with the same `BuildHasher`**, and it is griddle's own default:
  `griddle::hash_map::DefaultHashBuilder` = `core::hash::BuildHasherDefault<ahash::AHasher>`.
  The sweep prints the type name it instantiated, so this is checked at run time rather
  than asserted. (The first version of this report compared a std `RandomState`
  prototype against an ahash baseline and the difference was a large part of what it
  called a lookup gap — see [fix round 1](#fix-round-1-what-changed-and-why).)

### Results

| Shape | griddle live B/e | griddle table B/e | `str7` live B/e | **saving** |
| --- | ---: | ---: | ---: | ---: |
| `counters` | 416.1 | 203.4 | 65.2 | **6.4×** |
| `sessions` | 480.3 | 203.4 | 145.4 | **3.3×** |
| `redis-feel` | 463.6 | 203.4 | 109.2 | **4.2×** |

The 203.4 B/entry table term is the **bucket** array, not the usable-capacity array:
hashbrown keeps one group free, so `capacity()` is 7/8 of the bucket count and the
bucket count is a power of two — at 1 M live entries that is 2 097 152 buckets, each
holding one `(Bytes, Entry)` pair (96 B) plus one control byte, = 203 423 744 B. (An
earlier version of this report charged `capacity × 97` and reported 178 B/entry, which
understates the incumbent's own array by an eighth.)

The gap between 203.4 and the 416–480 B/entry measured live figure is (a) the per-entry
heap allocations the incumbent needs and this design does not (a `Bytes` record for the
key, an `Arc<Value>` for the value) and (b) whatever griddle is still holding of the
*old* table, since its selling point is incremental resize. The largest credit (b) can
possibly be worth is one complete old table at half the bucket count —
1 048 576 × 97 = 101.7 B/entry. Subtracting all of it anyway leaves the incumbent at
**314 / 379 / 362 B/entry** on `counters` / `sessions` / `redis-feel`, still **2.5–4.8×**
this design. The memory verdict does not depend on the resize question either way.

Operation costs, nanoseconds per op, 1 M entries. Each cell is the **minimum over three
runs of the best-of-5 in-process passes** (see [How the timings were taken](#headline-verdicts));
both sides use the same `BuildHasher` and the prototype's timed read path carries no
instrumentation:

| Shape | op | griddle | `ptr8` | `int8` | `str7` | `str15w` | `hybrid` |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `redis-feel` | lookup hit | **54** | 154 | 116 | 108 | 139 | 137 |
| `redis-feel` | lookup miss | **45** | 138 | 122 | 117 | 104 | 140 |
| `redis-feel` | iterate | 4.8 | 4.7 | 4.8 | 5.2 | 3.0 | 5.1 |
| `counters` | lookup hit | **35** | 105 | 107 | 113 | 102 | 146 |
| `sessions` | lookup hit | **56** | 138 | 130 | 133 | 125 | 142 |
| `redis-feel` | insert (noisy) | 464 | 337 | 283 | 251 | 323 | 268 |

### Where the lookup gap actually comes from

The first version of this report called this gap "3–4×" and blamed the scalar
fingerprint loop. That number was wrong and the attribution was undecomposed. Taking the
`redis-feel` hit column as the reference case:

| Step | `str7` hit ns | ratio vs griddle | What it removes |
| --- | ---: | ---: | --- |
| as first reported | 167 | 4.5× | — |
| **same `BuildHasher` on both sides** | ~108 | 2.0× | The prototype hashed with std `RandomState` (SipHash-1-3) while griddle's default is `BuildHasherDefault<ahash::AHasher>`. On a 28-byte key that is most of a lookup. **This was roughly 45 % of the reported gap and it was an artefact of the harness, not of the design.** |
| **counters compiled out of the timed path** | ~108 | 2.0× | `probe` is now generic over `const COUNT: bool`; the timed path takes `&self` and updates nothing. Measured cost of the counters, over the same minima: `str7` +16 ns (`redis-feel`), +8 (`counters`), +17 (`sessions`) — but `ptr8` on `redis-feel` comes out 27 ns *faster* with the counters on. The instrumentation is therefore **at or below this box's noise floor**: real, small, and not the explanation. |
| **residual** | 108 vs 54 | **2.0×** | Attributed below. |

The residual, in the order the probe pays it, is:

1. **A scalar fingerprint match.** `Bucket::find` walks the occupancy bitmap one bit at
   a time and compares one fingerprint byte per iteration — a mean 8.47 occupied slots
   per bucket, over 1.67 buckets per hit, so ~14 dependent iterations. hashbrown matches
   16 control bytes in one SIMD instruction and gets a match mask.
2. **A bucket spans four cache lines, a hashbrown group spans one.** Our probe reads the
   fingerprint block (line 0 of the bucket), then the matching slot, which is 0–3 lines
   further in, then chases the key pointer for the final `memcmp` — and does that for
   1.67 buckets. hashbrown reads one 16-byte control group and one 96-byte pair, then
   chases its own key pointer.

Contributor 1 is what SIMD removes; contributor 2 is a layout property that SIMD does
*not* remove — a 256 B bucket is the price of holding 14 inline slots. **This report does
not measure how the residual splits between them**, and issue 11 should not assume the
whole 2.0× is recoverable.

### Verdict

**Memory: decisive GO. Iteration: parity. Insert: same order of magnitude (single-shot
and contention-exposed — do not read it finer than that). Lookup: 2.0× slower on
`redis-feel`, 2.4× on `sessions`, 3.2× on `counters` — CONDITIONAL, not the 3–4× NO-GO
this report first claimed.**

**Gate for issue 11**, restated from the decomposed numbers:

- Land SIMD fingerprint matching (follow-up 1) and re-measure hit and miss on an idle
  Linux box, both sides on the same hasher — the measurement conditions here are not
  good enough to set a target from.
- **Ship gate: lookup hit within 1.25× of `griddle::HashMap` on `redis-feel` and
  `sessions`.** That is the load-bearing path; a 3–6× memory saving does not buy a
  25 %-plus regression on every GET.
- The `counters` shape (mean 9.9 B key) is the worst case at 3.2×, because a short key
  makes ahash almost free for the baseline while our fixed per-bucket scan cost does not
  shrink. Measure it separately; if SIMD does not bring it inside 1.5×, the fingerprint
  block wants re-sizing (fewer, wider slots) before the swap-out.
- If SIMD lands and the gap holds above the gate, the honest options are to keep griddle
  for the hot table and use segments only where memory dominates, or to revisit the
  256 B bucket. Both are issue 11's call and both are cheaper than shipping a slower GET.

---

## Deviations from Dragonfly's Dashtable

Studied before building; each deviation is deliberate and stated with its reason.

1. **A bucket is four cache lines (256 B), not one.** Dashtable's bucket is
   cache-line-sized because its slots are pointer pairs; ours hold the key and value
   words *inline*, so the bucket is sized to hold 14 16-byte slots plus a half-cache-line
   metadata block. The PRD's "cache-line-sized buckets" wording describes the reference's
   *metadata*, and that is what we kept at one line's worth (32 B).
2. **Fingerprints are matched by a scalar bit-loop, not SIMD.** Spike shortcut, and
   *one* of the two identified contributors to the residual lookup gap in section 5 —
   the other being the 4-cache-line bucket, which SIMD does not address.
3. **Directory entries are `u32` segment indices, not 8-byte pointers.** Halves the
   directory and lets the segment vector move; the indirection is one L1 hit.
4. **No displacement on insert.** Dash relocates an item from a full home bucket into
   *its* neighbour to make room; we only balance between home and neighbour, then stash,
   then split. This is why occupancy settles at 0.581 rather than Dash's ~0.9 — the
   single largest remaining memory win, and it is issue 11's.
5. **No directory shrink or segment merge on delete.** Deletes free the payload and clear
   the slot; the directory never contracts. Out of scope for a spike with no delete-heavy
   workload, and Redis's own dict does not shrink eagerly either.
6. **The per-bucket stash bitmap is cleared only when the last spill leaves** (the count
   is exact, the bitmap is not). A production build keeps a per-stash counter; the effect
   here is a slightly pessimistic probe count after deletes, which the sweep does not
   exercise.
7. **No bucket version counters.** Dashtable carries them for optimistic concurrent
   reads; under R2/R3 the table is owned by one shard thread and never shared, so they
   would be pure overhead.
8. **A split rehashes keys.** Same as the reference — with 8 fingerprint bits there is
   nothing else to route on. Unlike the reference, we counted it: 808 rehashes to move
   404 entries, ~44 µs, and we propose keeping more hash bits (follow-ups).

---

## Caveats

- **Single-threaded throughout**, per R2/R3. No concurrency, no cross-core anything.
- **macOS laptop, shared with four other agents, and it never went quiet.** Load ranged
  13–45 across the three reported runs; individual runs disagree by up to 2× on the same
  cell (`str7` `redis-feel` hit: 108 / 176 / 693 ns across runs at load 20 / 14 / 45).
  The mitigation is the best-of-5-passes, minimum-across-3-runs rule described at the
  top, and the minima *are* stable — they changed by at most one cell between two and
  three runs. Memory agreed to within 0.3 B/entry across every run, so the bytes/entry
  verdicts do not depend on machine quiet. **Every nanosecond figure here should be
  treated as suspect in absolute terms** and re-measured on an idle Linux box before
  issue 11 commits to a performance target; the single-shot columns (insert, split tail
  percentiles, split max) are the ones to distrust most, and are flagged in place.
- **Values are byte strings and integers only.** No lists, hashes, sets, zsets, streams —
  R7's block encodings are a separate question and out of scope here.
- **R6's other half is untouched**: the heap record carries a sized `rc` field but there
  is no refcounting, no COW, and no snapshot interaction.
- **No TTL, no expiry, no eviction, no persistence hooks.** `KeyMetadata` is replicated
  in the baseline for sizing only; the prototype's slots carry no metadata at all, which
  is a real gap — see follow-up 4.
- **The insert comparison is not like-for-like, and it favours neither side cleanly.**
  The baseline insert builds a full `Entry` — including an `Instant::now()` for
  `last_access` — while the prototype's insert stores only two words. That inflates
  griddle's insert number by a clock read; it also means the prototype is not yet doing
  the work follow-up 4 will give it. Read the insert column as "same order of
  magnitude", nothing finer.
- **`stats.allocated` includes size-class rounding**, which is the honest number for a
  budget (that memory is unavailable to anything else) but slightly flatters designs
  that allocate fewer, larger objects — i.e. it flatters ours over griddle's. The
  `struct B/e` column is charged the *same* way: it is `nallocx`'s size class for a
  segment (16 384 B for a 15 424 B struct, 6.2 % of rounding that is real live memory),
  not `size_of`. An earlier version of this report used `size_of` there and understated
  our own structure by 6.2 %; the verdicts are unchanged and the numbers are now
  consistent between the two columns.
- **The prototype is `unsafe` by construction** (raw payload pointers, transmuted words,
  `alloc_zeroed` segments) and has had **no miri and no fuzzing** — explicitly out of
  scope per the brief, explicitly *in* scope for issue 11 per R5.

---

## Follow-ups (for issues 11 and 12)

1. **SIMD fingerprint matching** — the blocking item. 14 fingerprint bytes fit a NEON
   `uint8x16_t` / SSE2 `__m128i`; match-and-mask reduces `Bucket::find` to one compare
   plus a bit-loop over *matching* slots only. This is an **expectation, not a
   measurement**: it removes the ~14 dependent scalar iterations per hit, but not the
   extra cache line a 256 B bucket costs, so it closes an unknown fraction of the
   measured 2.0×. Re-measure against the [gate](#verdict-2) before scheduling the
   swap-out.
2. **Stop rehashing on split.** Store 16 fingerprint bits (or the top bits of the hash)
   per slot so the split bit can be read out of bucket metadata without touching the key.
   Today a split pays 808 rehashes to move 404 entries at ~110 ns per moved entry; with
   the split bit in metadata it should fall to the 404 slot copies, so expect the ~44 µs
   stall to land near 10–15 µs.
3. **Displacement on insert**, as Dash does: occupancy 0.581 → ~0.9 takes structure from
   33.6 to ~21.7 B/entry, a further 11 % off total live bytes on `redis-feel`.
4. **Where does `KeyMetadata` live?** The prototype's slot is key word + value word and
   nothing else, but the shipped `Entry` carries `KeyMetadata` and `KeyType`. Either they
   join the heap record's header, or they get a third word per slot (+8 B/entry, which
   would still leave the design 3–5× ahead), or expiry moves to a per-segment structure.
   This is issue 11's first design question and this spike does not answer it.
5. **Size the segment to an allocator size class.** A 15 424 B segment is served from
   jemalloc's 16 384 B class: 960 B — **6.2 %** of our whole structural cost — is bought
   and never used. The hybrid layout is worse (14 944 B → 16 384 B, 9.6 %). Growing the
   segment to fill the class is free memory: at 16 384 B a 16-byte-slot segment holds
   ~63 buckets instead of 60, +5 % capacity for zero extra bytes. Alternatively drop to
   the next class down. Either way this is arithmetic, not research, and it should be
   done before issue 11 measures anything.
6. **Directory shrink / segment merge** on delete-heavy workloads.
7. **Issue 12 starts from the header in section 4** — `q_state`, `q_prev`, `q_next`,
   `hits`, `misses`, `victim_cursor` are allocated and named; the 24 reserved bytes are
   its budget, and it should not need them.

---

## Contradictions with the PRD as written

1. **"Cache-line-sized buckets" (R5) is the wrong unit.** With key and value words
   inline, the bucket that makes sense is four cache lines with a half-line of metadata.
   The PRD sentence should read "cache-line-sized bucket *metadata*".
2. **R5's "tagged 8-byte slots" needs one clarification, and it is the right call.** The
   *slot* is 16 bytes: an 8-byte key word plus an 8-byte value word. This spike confirms
   8 bytes as the word width and rules out 16.
3. **R6 says "inline small values"; the threshold the PRD left open is 7 bytes** — not a
   round number, but what a 3-bit tag plus a length nibble leaves in an 8-byte word, and
   the sweep says paying for more is a net loss.
4. **The PRD assumes the new table is at least as fast as the incumbent.** As prototyped
   it is 2.0–3.2× slower on lookup depending on key length, with one cause identified as
   fixable (scalar fingerprint match) and one that is inherent to the layout (a 256 B
   bucket touches more cache lines than a 16-byte hashbrown group). The PRD should carry
   an explicit performance gate for the swap-out rather than treating speed as given.
5. **R9's "no per-key LRU field" is affordable.** The whole eviction state is 22 B *per
   segment* — 0.045 B/entry. The PRD should not, however, sell this as beating Redis's
   3 B/entry LRU field: those 24 bits ride in `robj`'s existing header word and cost
   Redis ~nothing. The claim to make is that a design whose slot is two bare words —
   with no header to hide bits in — still gets 2Q for 0.045 B/entry, versus 4–16 B/entry
   for any per-key scheme it would otherwise need.

---

## Fix round 1: what changed and why

Review of the first version of this report found the memory ruling and the cursor proof
sound and the performance half unsound. Everything below is what changed; the R5/R6
memory rulings, the inline threshold, and the SCAN cursor scheme are unchanged.

**Measurement defects fixed in the prototype** (all of them changed numbers in this
report):

1. **Hasher mismatch.** The prototype used std `RandomState` (SipHash-1-3) while the
   griddle baseline used its default `BuildHasherDefault<ahash::AHasher>`. On 28-byte
   keys that difference was a large part of what the report called a lookup gap.
   `Table` is now generic over `S: BuildHasher` with griddle's own default as the
   default, the sweep prints the instantiated type name, and the cursor tests pin a
   fixed `ahash::RandomState` seed so a failing scan reproduces.
2. **Instrumented read path.** The timed lookup called `contains(&mut self)`, which
   bumped four `Stats` counters including a max, against `map.contains_key` which pays
   nothing. `probe` is now generic over `const COUNT: bool`: `contains` takes `&self` and
   counts nothing, `contains_counted` supplies the probe-length statistics, and the sweep
   reports both so the instrumentation's cost is measured rather than asserted away.
3. **`size_of` instead of the allocated size class.** `structural_bytes()` charged
   `size_of::<Segment>() × count`; jemalloc rounds 15 424 B to the 16 384 B class, so
   structure was understated 6.2 % (9.6 % for `hybrid`). It now charges `nallocx`, and
   "size the segment to a size class" is [follow-up 5](#follow-ups).
4. **Split accounting was invented.** "≈160 ns per moved entry, ~420 moved" was derived,
   not measured. The split path now counts slots scanned, slots moved and directory words
   written: 808 / 404 / 1.0 per split, ~110 ns per moved entry.
5. **The directory rewrite contradicted its own prose.** Section 2 claimed a split
   "rewrites only the directory slice that points at it" while `split()` scanned all
   `2^global_depth` entries. The code now strides (`old_key | 2^depth`, step
   `2^(depth+1)`), a new test pins the invariant that makes the stride legal, and the
   stall was re-measured after the change.
6. **Contention.** Timings are now best-of-5 in-process passes, minimised across three
   runs, with the load average recorded in the sweep output — see
   [How the timings were taken](#headline-verdicts). The box never went quiet (load
   13–45); the minima are stable but the absolutes remain suspect and are labelled so.

**Claims corrected in the report text:**

- The lookup verdict is **2.0× / 2.4× / 3.2×** (redis-feel / sessions / counters), not a
  flat "3–4× NO-GO", and it is decomposed into hasher artefact (gone), instrumentation
  (below the noise floor), scalar fingerprint match (SIMD's target) and the 4-cache-line
  bucket (not SIMD's target). Issue 11's gate is restated from those numbers.
- The griddle table term is **203.4 B/entry** (bucket count × 97), not 178 — `capacity()`
  is 7/8 of the bucket count. The unreconstructable "240–300 B/entry settled" range is
  replaced with an explicit upper-bound credit for the old table.
- Section 2's directory argument was inverted: 1 B/entry needs ~**4** entries per
  segment, not 8 K. Conclusion unchanged.
- Section 4 no longer claims to beat Redis's 3 B/entry LRU field: those bits are free in
  `robj`'s existing header word. The 0.045 B/entry reservation stands on its own.
- `struct B/e` is no longer described as "exact and rounding-free".
