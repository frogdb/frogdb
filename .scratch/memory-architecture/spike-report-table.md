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
| **R5** — segmented extendible-hash table replaces `griddle::HashMap<Bytes, Entry>` | **GO on memory, GO WITH CONDITIONS on speed** | 3.3–6.4× less live memory per entry; lookups are still **3–4× slower** than griddle because the fingerprint scan is a bit-loop, not a SIMD group match. Issue 11 must close that before the swap-out. |
| **R6** — inline small values | **GO, threshold = 7 bytes** | An 8-byte tagged word inlines 59 % of realistic values and 100 % of counter values at **zero** structural cost. |
| Wider slot (16 B word, 15-byte inline) | **NO-GO** | Buys +16 pp of inline values and costs +31.6 B/entry of structure — a net **+22.9 B/entry loss** on `redis-feel`. |
| Inline **keys** | **NO-GO at 8 B, not worth it at 16 B** | Only 0.1 % of realistic keys fit in 7 bytes; the 19.5 % that fit in 15 do not pay for the wider slot. |
| Extendible-hash directory ≤ ~1 B/entry | **GO, by two orders of magnitude** | **0.008 B/entry** measured at 1 M keys (`u32` segment indices, not pointers). |
| SCAN cursor stable across splits | **GO — proven executably** | Reverse-binary counter advanced at the *scanned segment's* local depth; every pre-existing key seen exactly once across 16 mid-scan splits, where the obvious directory walk duplicates **3 500 of 8 000** keys. |
| **R9** space reservation in the segment header | **GO** | 22 B of eviction state + 24 B spare inside the 64-byte (one cache line) header — **0.045 B/entry**, against Redis's 3 B/entry per-key LRU field. |
| Worst single-operation split stall | **CONCERN** | A split moves ~420 entries and rehashes each: **67 µs median, 126 µs max**, hit by 0.2 % of inserts. Visible at p99.9. Issue 11 needs the mitigation in [Follow-ups](#follow-ups). |

Machine: Apple M1-class, 10 physical cores, macOS 26.5.1, rustc 1.92.0, jemalloc 5.3.0
via `tikv-jemalloc-sys 0.6.1` (`--enable-stats`), release + debug symbols, single-threaded
(per R2/R3 the table is never shared). Two full sweeps were taken; the first ran while
other agents were compiling and its latency column is 4–8× worse across *every* variant
including the baseline. The numbers below are the quiet run; see [Caveats](#caveats).

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
| `counters` | 416.1 | 81.2 | 65.3 | **65.3** | 67.0 | 74.2 |
| `sessions` | 480.3 | 145.6 | 145.4 | **145.3** | 178.9 | 154.4 |
| `redis-feel` | 463.6 | 118.9 | 111.5 | **109.3** | 132.2 | 113.5 |

Inline share, and the structure that buys it:

| Shape | variant | inline keys | inline values | struct B/e | dir B/e | occupancy | mean bucket fill | stash share |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `counters` | `str7` | 0.1 % | 100 % | 31.6 | 0.008 | 0.581 | 8.47/14 | 2.87 % |
| `counters` | `str15w` | 100 % | 100 % | 63.2 | 0.016 | 0.581 | 4.23/7 | 2.96 % |
| `redis-feel` | `int8` | 0.0 % | 45.0 % | 31.6 | 0.008 | 0.581 | 8.47/14 | 2.88 % |
| `redis-feel` | `str7` | 0.0 % | 59.0 % | 31.6 | 0.008 | 0.581 | 8.47/14 | 2.84 % |
| `redis-feel` | `str15w` | 19.5 % | 75.0 % | 63.2 | 0.016 | 0.581 | 4.23/7 | 2.94 % |
| `redis-feel` | `hybrid` | 0.0 % | 75.0 % | 38.8 | 0.016 | 0.713 | 6.55/9 | 4.69 % |

Probe lengths are identical across the 8-byte variants and slightly better for the wide
one (fewer slots per bucket ⇒ fewer collisions per bucket):

| variant | buckets touched on hit | on miss | max |
| --- | ---: | ---: | ---: |
| `ptr8` / `int8` / `str7` | 1.67 | 2.32 | 6 |
| `str15w` | 1.59 | 2.16 | 6 |
| `hybrid` | 1.62 | 2.35 | 6 |

### Verdict — the inline threshold is **7 bytes**, in an 8-byte word

- **Small-int inlining is free money.** `ptr8 → int8` on `counters` is 81.2 → 65.3
  B/entry (−19.6 %) for no structural change: the 8-byte heap record and its 8-byte
  header disappear entirely.
- **Short-string inlining is also free**, and strictly dominates int-only inlining:
  `int8 → str7` on `redis-feel` lifts the inline share 45 % → 59 % and saves another
  2.2 B/entry at *identical* slot width. There is no reason to inline integers but not
  7-byte strings — the tag scheme already has the room.
- **A wider slot does not pay for its halved bucket capacity.** `str15w` inlines 75 %
  of `redis-feel` values against `str7`'s 59 %, but its segment holds 420 slots instead
  of 840, so structure doubles from 31.6 to 63.2 B/entry. Net **+22.9 B/entry** — the
  wide word loses on every shape, including `counters`, where it inlines *everything*
  (67.0 vs 65.3). The hybrid 24-byte slot is a smaller version of the same loss
  (+4.2 B/entry on `redis-feel`).
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
growth adds exactly one segment per split and rewrites only the directory slice that
points at it. The directory is `Vec<u32>` of segment indices. Split stalls are measured
by timing **every** insert individually and bucketing them by what they did.

### Results

Layout, asserted by `tests/table.rs::bucket_is_four_cache_lines_and_header_is_one`:

```
bucket   = 32 B metadata + 14 × 16 B slots = 256 B  (four cache lines, no tail padding)
metadata = fp[14] | occupied: u16 | stash_map: u8 | stash_count: u8 | pad[14]
segment  = 64 B header + 60 × 256 B buckets = 15 424 B, 840 slots
```

Directory growth at 1 M `redis-feel` entries: **2 048 entries, 2 047 splits, 11
doublings, 8 KB total = 0.008 B/entry**, two orders of magnitude inside the ~1 B/entry
target. The directory only reaches 1 B/entry at ~8 K entries per segment, which cannot
happen: a segment holds 840. `tests/table.rs::directory_overhead_stays_under_one_byte_per_entry`
pins this at 500 K keys.

Split stalls, `redis-feel`, 1 M inserts, nanoseconds:

| variant | plain p50 | plain p99.9 | split p50 | split p99 | split max | doubling p50 | doubling max | splits | doublings |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `ptr8` | 208 | 2 042 | 71 125 | 120 084 | 166 333 | 45 750 | 85 666 | 2 047 | 11 |
| `int8` | 209 | 1 959 | 69 625 | 121 750 | 201 792 | 62 750 | 109 625 | 2 047 | 11 |
| `str7` | 208 | 1 500 | **67 292** | 103 375 | **126 292** | 56 042 | 107 375 | 2 047 | 11 |
| `str15w` | 208 | 1 416 | 32 375 | 47 375 | 86 208 | 21 333 | 43 125 | 4 095 | 12 |
| `hybrid` | 167 | 1 083 | 41 417 | 61 417 | 84 875 | 28 792 | 48 458 | 2 582 | 12 |

### Verdict

**Incremental splits work and the directory is free — but a split is a 67 µs stall,
and that is the one number in this spike that is not good enough.**

- The stall is **linear in entries moved**, not in table size: `str15w` moves half as
  many entries per split and pays half the time (32 µs), and the directory doubling
  itself is cheaper than the entry movement it accompanies. So the table's growth cost
  does not degrade as the keyspace grows — but each individual split is a 67 µs
  hiccup on 0.2 % of inserts, which lands squarely at p99.9.
- 67 µs / ~420 moved entries ≈ 160 ns per entry, the same as a *lookup* — because a
  split rehashes every key (only 8 fingerprint bits are stored) and each rehash chases
  the key's out-of-line pointer. The fix is in [Follow-ups](#follow-ups) and it is
  cheap: keep more hash bits.
- **Occupancy settles at 0.581**, mean bucket fill 8.47/14, with stash buckets holding
  2.9 % of entries and only 0.1 % of regular buckets full. Dash reports ~0.9; the gap
  is our one real functional deviation (no displacement on insert, below), and closing
  it would take structure from 31.6 to ~20 B/entry.

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
occupancy that is **22 / (840 × 0.581) = 0.045 B/entry** — against Redis's 24-bit
per-key LRU field (3 B/entry) and against a per-key intrusive LRU list (16 B/entry).
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
- Both sides use `RandomState` (SipHash-1-3), so no hasher bias.

### Results

| Shape | griddle live B/e | griddle table B/e | `str7` live B/e | **saving** |
| --- | ---: | ---: | ---: | ---: |
| `counters` | 416.1 | 178.0 | 65.3 | **6.4×** |
| `sessions` | 480.3 | 178.0 | 145.3 | **3.3×** |
| `redis-feel` | 463.6 | 178.0 | 109.3 | **4.2×** |

The 178 B/entry table term is `capacity × (96 + 1)` — griddle's own array at 1 M live
entries. The gap between it and the 416–480 B/entry measured live figure is (a) the
per-entry heap allocations the incumbent needs and this design does not (a `Bytes`
record for the key, an `Arc<Value>` for the value) and (b) **the old table griddle is
still holding at 1 M entries**, since its selling point is incremental resize. Even
crediting the incumbent a fully-settled table (halving the 178), it lands near
240–300 B/entry, still 2.5–4× this design.

Operation costs, nanoseconds per op, 1 M entries:

| Shape | op | griddle | `ptr8` | `int8` | `str7` | `str15w` | `hybrid` |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `redis-feel` | insert | 217 | 353 | 406 | 438 | 373 | 344 |
| `redis-feel` | lookup hit | **37** | 148 | 171 | 167 | 148 | 139 |
| `redis-feel` | lookup miss | **36** | 133 | 173 | 170 | 125 | 123 |
| `redis-feel` | iterate | 5.3 | 5.0 | 5.8 | 5.1 | 5.8 | 5.1 |
| `counters` | lookup hit | **54** | 173 | 168 | 172 | 150 | 172 |
| `sessions` | lookup hit | **50** | 158 | 140 | 133 | 151 | 130 |

### Verdict

**Memory: decisive GO. Iteration: parity. Insert: parity (within 1.5–2×, and the
incumbent is not paying for splits). Lookup: NO-GO as prototyped — 3–4× slower.**

The lookup gap is not a property of the design; it is the one shortcut the spike took
(see deviation 2): `Bucket::find` walks the occupancy bitmap one bit at a time and
compares fingerprint bytes in a scalar loop, where hashbrown/griddle matches 16 control
bytes in a single SIMD instruction. The same trick applies directly to a 14-byte
fingerprint array. **Issue 11 must land SIMD fingerprint matching and re-measure before
the griddle swap-out is scheduled**; a design that is 4× slower on the hottest path in
the database is not shippable no matter what it saves in bytes.

---

## Deviations from Dragonfly's Dashtable

Studied before building; each deviation is deliberate and stated with its reason.

1. **A bucket is four cache lines (256 B), not one.** Dashtable's bucket is
   cache-line-sized because its slots are pointer pairs; ours hold the key and value
   words *inline*, so the bucket is sized to hold 14 16-byte slots plus a half-cache-line
   metadata block. The PRD's "cache-line-sized buckets" wording describes the reference's
   *metadata*, and that is what we kept at one line's worth (32 B).
2. **Fingerprints are matched by a scalar bit-loop, not SIMD.** Spike shortcut, and the
   sole cause of the lookup gap in section 5.
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
   nothing else to route on. Unlike the reference, we measured what it costs (67 µs) and
   propose keeping more hash bits (follow-ups).

---

## Caveats

- **Single-threaded throughout**, per R2/R3. No concurrency, no cross-core anything.
- **macOS laptop, shared with other agents.** The first full sweep ran under compile
  load and every latency was 4–8× worse *including griddle's*; the reported run is
  quiet. Memory numbers were identical across both runs to within 0.2 B/entry, so the
  bytes/entry verdicts do not depend on machine quiet. Treat the nanosecond columns as
  ratios, not absolutes, and re-run on the Linux box before issue 11 commits to a
  performance target.
- **Values are byte strings and integers only.** No lists, hashes, sets, zsets, streams —
  R7's block encodings are a separate question and out of scope here.
- **R6's other half is untouched**: the heap record carries a sized `rc` field but there
  is no refcounting, no COW, and no snapshot interaction.
- **No TTL, no expiry, no eviction, no persistence hooks.** `KeyMetadata` is replicated
  in the baseline for sizing only; the prototype's slots carry no metadata at all, which
  is a real gap — see follow-up 4.
- **`stats.allocated` includes size-class rounding**, which is the honest number for a
  budget (that memory is unavailable to anything else) but slightly flatters designs
  that allocate fewer, larger objects — i.e. it flatters ours over griddle's. The
  `struct B/e` column is exact and rounding-free, and the verdicts hold on it alone.
- **The prototype is `unsafe` by construction** (raw payload pointers, transmuted words,
  `alloc_zeroed` segments) and has had **no miri and no fuzzing** — explicitly out of
  scope per the brief, explicitly *in* scope for issue 11 per R5.

---

## Follow-ups (for issues 11 and 12)

1. **SIMD fingerprint matching** — the blocking item. 14 fingerprint bytes fit a NEON
   `uint8x16_t` / SSE2 `__m128i`; match-and-mask reduces `Bucket::find` to one compare
   plus a bit-loop over *matching* slots only. Expect the 3–4× lookup gap to close.
   Until it does, the griddle swap-out should not be scheduled.
2. **Stop rehashing on split.** Store 16 fingerprint bits (or the top bits of the hash)
   per slot so the split bit can be read out of bucket metadata without touching the key.
   This is the 67 µs stall; it should fall to the cost of moving words.
3. **Displacement on insert**, as Dash does: occupancy 0.581 → ~0.9 takes structure from
   31.6 to ~20 B/entry, a further 12 % off total live bytes on `redis-feel`.
4. **Where does `KeyMetadata` live?** The prototype's slot is key word + value word and
   nothing else, but the shipped `Entry` carries `KeyMetadata` and `KeyType`. Either they
   join the heap record's header, or they get a third word per slot (+8 B/entry, which
   would still leave the design 3–5× ahead), or expiry moves to a per-segment structure.
   This is issue 11's first design question and this spike does not answer it.
5. **Directory shrink / segment merge** on delete-heavy workloads.
6. **Issue 12 starts from the header in section 4** — `q_state`, `q_prev`, `q_next`,
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
   it is 3–4× slower on lookup. The cause is identified and fixable, but the PRD should
   carry an explicit performance gate for the swap-out rather than treating speed as
   given.
5. **R9's "no per-key LRU field" is affordable, and cheaper than the PRD claims.** The
   whole eviction state is 22 B *per segment* — 0.045 B/entry, ~66× cheaper than Redis's
   3 B/entry LRU field, not merely "no worse".
