# Spec: architecture/storage.md
Status: update
Audiences: A2, A3, A5

Goal: A reader walks away understanding how a single FrogDB shard stores data in
memory and *why* the design is shaped that way: each shard owns one `Store`
(default backed by `griddle::HashMap` to avoid the resize-spike problem of a
standard hash table), values are variants of a `Value` enum with per-key
metadata (expiry, LRU access time, LFU counter, memory size), memory is
accounted per-key and per-shard *including* forkless-snapshot COW buffers so
that maxmemory enforcement stays honest, expiry is hybrid lazy+active, and
eviction is driven by a configurable policy (including the tiered variants). The
reader also learns the ownership choice behind `Store::get()` returning owned
values and how the two clock sources (monotonic vs wall-clock) are used.

Not in scope: On-disk format and recovery (persistence.md owns the RocksDB KV
schema and snapshot algorithm); cluster slot distribution (glossary/clustering);
command dispatch (execution.md). Link, don't restate.

Sources of truth (author must read and reconcile every claim):
- `frogdb-server/crates/core/src/store/mod.rs` — `Store` trait (`pub trait
  Store: Send`), its method set (get/set/delete/contains/key_type/len/
  memory_used/scan, and any others). Verify the listed method names.
- `frogdb-server/crates/core/src/store/hashmap.rs` — default store on
  `griddle::HashMap`; confirm the griddle usage and per-key metadata layout.
- `frogdb-server/crates/types/src/types/mod.rs` — the `Value` enum
  (**15 variants** at time of writing: String, SortedSet, Hash, List, Set,
  Stream, BloomFilter, HyperLogLog, TimeSeries, Json, CuckooFilter, TopK,
  TDigest, CountMinSketch, VectorSet). Bitmap and Geo are **not** separate
  variants — they are layered on String and SortedSet respectively.
- `frogdb-server/crates/core/src/eviction/policy.rs` — `EvictionPolicy` enum:
  NoEviction, VolatileLru, AllkeysLru, VolatileLfu, AllkeysLfu, VolatileRandom,
  AllkeysRandom, VolatileTtl, **TieredLru, TieredLfu**.
- `frogdb-server/crates/core/src/eviction/lfu.rs`, `.../mod.rs`, `.../pool.rs` —
  LFU logarithmic counter, sampled-pool eviction, config defaults
  (`lfu-log-factor`, `lfu-decay-time`).
- `frogdb-server/crates/types/src/` — value implementations to confirm the
  "Implementation Notes" column (e.g. VectorSet → HNSW via `usearch`).

Existing content: `website/src/content/docs/architecture/storage.md`. Strong
rationale sections; contains one wrong hash claim, an unbacked latency table,
and gaps (eviction policy list, tiered policies).

Structure (keep existing H2s, corrected + one addition):
- ## Terminology Note — internal shard vs hash slot. **Fix** the
  `hash(key) % num_shards` framing to reflect CRC16-derived routing (see Drift
  guards) and cross-link concurrency.md as the canonical routing home.
- ## Store Trait + Ownership Semantics — verify method list; keep the owned-value
  rationale.
- ## Value Types — the data-type table. Clarify that the table lists *logical
  data types* (17 rows) which map onto **15 `Value` enum variants**; call out
  that Bitmap operates on String and Geospatial is stored as a SortedSet with
  geohash scores. Do not assert a "number of value types" that contradicts the
  enum.
- ## Default Implementation + Key Metadata — griddle store, KeyMetadata fields,
  clock sources, LFU counter (verify `lfu_log_incr`, start value, defaults).
- ## Memory Accounting — per-key/per-shard; the griddle rationale; COW-inclusive
  total memory.
- ## Eviction (expand) — document the full `EvictionPolicy` set including
  `TieredLru`/`TieredLfu` and what "tiered" means here; sampled-pool approach.
  This is currently under-covered and must be added.
- ## Expiry Index + Strategy + Atomicity — BTreeMap index, lazy+active, at-entry
  expiry check.
- ## Hash Algorithms — **must be corrected** (see Drift guards).
- ## CROSSSLOT Validation — keep.
- ## Persistence Integration — brief; link persistence.md.

Generated data: None. Value-type and eviction-policy lists could later be
generated, but for now must be hand-verified against the enums above.

Drift guards:
- **Remove the unbacked griddle latency table (known issue).** The "HashMap
  Implementation Choice" table asserts `std::HashMap` max insert `~38ms` vs
  `griddle::HashMap` `~1.8ms`, and mean `~94ns`/`~126ns`. These are performance
  numbers with no benchmark citation and violate PLAN §6. Either (a) delete the
  numeric columns and keep the qualitative explanation (standard tables resize
  in one O(n) step causing a latency spike; griddle amortizes resize work across
  inserts, trading a slightly slower read during active resize), or (b) cite a
  reproducible, published FrogDB benchmark. Default to (a). The same applies to
  the "~2x memory during resize / latency spikes of 30-40ms" prose — keep the
  mechanism, drop the specific figures unless cited.
- **Hash algorithm claim is WRONG.** The "Hash Algorithms" table lists internal
  shard routing as `xxhash64`. Real routing is CRC16 (see
  `shard/helpers.rs::shard_for_key`, `CRC16 % 16384 % num_shards`). Correct this
  table and the Terminology Note to match concurrency.md; xxhash64 is used for
  probabilistic structures, not shard routing.
- **Value variant count.** Do not state a value-type count that conflicts with
  the 15-variant `Value` enum. If a number is used, generate it or omit it
  (PLAN §6); prefer describing the logical-type-vs-enum-variant mapping in prose.
- **Eviction policy list drift.** The policy enumeration must match
  `EvictionPolicy` exactly, including the tiered variants; if a policy is added
  or renamed, the page must follow.
- **Config defaults.** `lfu-log-factor`, `lfu-decay-time`, and any eviction
  sampling config must match the config crate defaults; verify rather than
  trusting the current page.
- **S7 code-path check.** Any `crates/...` path named must exist; include this
  page in the S7 scan.
