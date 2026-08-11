# Proposal 89 — one `BloomChunkCodec` / `CuckooChunkCodec` in `frogdb-types`: the filter wire format has two decoders and only one of them was hardened

Round 38 · lane: commands + types · candidate **CT1** (merged-list H1) · effort **M** ·
**edits a LOCKED crate** (`frogdb-persistence`, gate 0.85) — but **zero `FM-` tags exist in any
edited region** and **no persistence-observable behavior changes** (§Locked-area clearance)

**Verified at HEAD `eb8760e9`** (worktree `arch-round-38-99`, branch `worktree-arch-round-38-99`).
Every file:line below was re-derived at this SHA; nothing is inherited from the lane brief.
Concurrent authors hold proposals 81/82/83/84 in the working tree; **no code file in this set is
dirty**.

**The headline is not the one the brief expected.** The two copies of the filter format do not
merely differ in hardening — they *disagree about what a valid filter is*, and the command-side
copy can persist a value the storage-side copy refuses to decode on recovery. That is a
durability defect on the ordinary command path, reachable with a 46-byte well-formed payload and
no hostile intent, and it is the load-bearing case for the refactor. The allocation guards are a
second, separately-classified matter held under the standing security policy (§Security
classification — flagged for the user).

## Corrections to the lane brief

| Brief claim | Correction at `eb8760e9` |
|---|---|
| "`frogdb-types` … claimed to be the only shared dep — VERIFY" | **True in effect, false as stated.** `frogdb-commands/Cargo.toml` declares **no `frogdb-types` dependency at all** — only `frogdb-core` and `frogdb-protocol` (`commands/Cargo.toml:55-56`). It reaches every types item through `frogdb-core/src/lib.rs:7` (`pub use frogdb_types::*;`) and already does so for exactly these types (`commands/src/bloom.rs:6-10` imports `BloomFilterValue`/`BloomLayer` from `frogdb_core`; `cuckoo.rs:728` writes `frogdb_core::CuckooLayer::from_raw`). `frogdb-persistence/Cargo.toml:15` declares `frogdb-types` directly. **Net: the home works and needs no `Cargo.toml` edit on either side** — one `pub mod codec;` line in `types/src/lib.rs`. |
| "consider whether `frogdb-protocol` is a better home" | **Impossible, and the reason is one line.** `protocol/Cargo.toml` (20 lines) declares **zero `frogdb-*` dependencies** — it is the graph leaf (the same fact proposal 84 used to move `BlockingOp` *toward* protocol). A chunk codec must name `BloomFilterValue`/`CuckooLayer`, which live in `frogdb-types` and carry `bitvec` + `murmur3`/`xxhash` machinery. Protocol cannot see them and must not grow to. **Home = `frogdb-types`.** |
| "cuckoo.rs:709 wraps in release … a potential OOB-read bypass" | **Line is `:710`; the wrap is real, the OOB-read is not.** `[profile.release]` (root `Cargo.toml:202-204`) sets `lto`/`codegen-units` and **no `overflow-checks`**, so `num_buckets * layer_bucket_size as usize * 2` (`cuckoo.rs:710`) wraps in release. But the read that follows is `data[offset..offset + 2]` (`:721`) — a **bounds-checked slice index**, which panics rather than reading out of bounds. Rust has no OOB read here. The consequences are an allocation and a panic; both are classified precisely in §Security classification. |
| "bloom.rs:622-625, cuckoo.rs:690, :709, :717" | Sites confirmed, one renumbered: `bloom.rs:622` (`num_layers` read), `:625` (`Vec::with_capacity(num_layers)`); `cuckoo.rs:690` (`Vec::with_capacity(num_layers)`), **`:710`** (the unchecked mul), `:717` (`Vec::with_capacity(num_buckets)`). One more the brief missed: **`cuckoo.rs:719`** `Vec::with_capacity(layer_bucket_size as usize)` — bounded by `u8::MAX`, harmless, but it is the fourth unguarded capacity in the same loop. |
| "persistence/serialization/probabilistic.rs:264-355" | `deserialize_bloom_filter` is **`:264-305`**, `deserialize_cuckoo_filter` **`:308-387`**. `:355` lands mid-`deserialize_cuckoo_filter`, not at its end. The `remaining()/28` guard is `:276-281`, `remaining()/25` is `:321-326`, `checked_mul` is `:344-351`, and there is a **fifth** guard the brief does not name: the zero-bucket-size rejection at **`:337-341`**, which is the one that produces the round-trip divergence below. |
| Implied: "the drift is only about hardening" | **The drift changes the accepted language.** `:337-341` rejects a layer with `num_buckets > 0 && bucket_size == 0`; `commands/src/cuckoo.rs` has no equivalent. That is not a resource guard, it is a validity rule, and the two sides disagreeing on it is §Problem 3 — a **LIVE** durability defect. |
| Effort **M** | **Confirmed M**, and the size is now known: ~150 lines deleted from `frogdb-commands`, ~95 relocated out of `frogdb-persistence`, ~155 relocated (`FrameReader` + its tests) plus ~260 lines of guard tests moved crate-to-crate. The cost is not the code, it is the mutation-gate bookkeeping (§Locked-area clearance). |

## Summary

The scalable-filter wire format is written and read in **two crates that cannot see each
other**. `BF.SCANDUMP` (`commands/src/bloom.rs:532-548`) and `serialize_bloom_filter`
(`persistence/src/serialization/probabilistic.rs:25-68`) emit **byte-identical** payloads —
`error_rate f64`, `expansion u32`, `non_scaling u8`, `num_layers u32`, then per layer `k u32`,
`count u64`, `capacity u64`, `bits_len u64`, packed bits. `CF.SCANDUMP`
(`commands/src/cuckoo.rs:591-611`) and `serialize_cuckoo_filter` (`probabilistic.rs:84-118`) are
likewise byte-identical. **It is one format, not two**: `BF.LOADCHUNK`'s argument and a bloom
value's persisted payload are the same bytes, and the only difference is that persistence wraps
them in the 24-byte frame header (`mod.rs:87-118`) while the command does not.

The decoders have since diverged. `frogdb-persistence`'s pair reads through the bounds-checked
`FrameReader` (`frame.rs:20-89`), bounds every pre-allocation with `safe_capacity`
(`mod.rs:46-55`), refuses a layer count the remaining bytes cannot cover (`:276-281`, `:321-326`),
uses `checked_mul` for the fingerprint region (`:344-351`), rejects a bucket-bearing layer with a
zero bucket size (`:337-341`), carries ~260 lines of two-sided guard tests (`:859-1119`), and is
fuzzed (`testing/fuzz/fuzz_targets/deserialize.rs`). `frogdb-commands`' pair hand-slices the
buffer with a running `offset`, pre-allocates straight from the wire counts (`bloom.rs:625`,
`cuckoo.rs:690`, `:717`), multiplies unchecked (`cuckoo.rs:710`), has **no** validity rule, has
**zero** tests of any kind (the only test module in either file is `flag_value_pin_tests`,
`bloom.rs:671` / `cuckoo.rs:751`, about `BF.RESERVE` flag parsing), and has **no** fuzz target.

The consequence is not symmetric hardening debt. Because the two decoders accept different
languages, `CF.LOADCHUNK` will happily store a filter whose re-serialized payload
`deserialize_cuckoo_filter` **refuses** — so the key survives in memory, is written to the WAL
(`WalStrategy::PersistFirstKey`, `cuckoo.rs:641`), and then fails to decode at recovery: skipped
and counted under the default policy ([FM-PERSISTENCE-033](../../hardening/specs/persistence-failure-modes.md), spec `:489`), or a **refused
boot** under `recovery.on-decode-failure = refuse` (FM-PERSISTENCE-047, spec `:513`) — and on a
node whose only key is that one, a refused boot with no knob at all (FM-PERSISTENCE-045, spec
`:501`). A client command can write a value the storage layer cannot read back.

The fix is one **deep module** with a two-function interface. `frogdb-types` gains
`codec::{BloomChunkCodec, CuckooChunkCodec}` — `encode(&FilterValue) -> Vec<u8>` and
`decode(&[u8]) -> Result<FilterValue, SerializationError>` — holding the hardened bodies
verbatim, together with the three primitives they need (`FrameReader`, `safe_capacity`,
`SerializationError`, all relocated unchanged so persistence's error *text* and *shape* stay
byte-identical for the RESTORE path that surfaces them to clients). `frogdb-persistence`'s four
functions are **deleted** — its existing `TypeCodec` table (`registry.rs:295-299`, `:315-319`)
calls the shared codec directly. `frogdb-commands`' four hand-rolled halves are **deleted** —
~150 lines become four one-line calls, and `bitvec` (used at exactly one line in the whole crate,
`bloom.rs:593`) leaves `commands/Cargo.toml`.

**Deletion test applied, and it bites twice.** The persistence wrappers exist only to pair a
payload with a `TypeMarker`, which the codec table already knows — deleted, not moved. The
command-side `data.len() < 17` / `< 19` pre-checks (`bloom.rs:612`, `cuckoo.rs:672`) exist only
because a hand-rolled parser cannot report its own shortfall — deleted; `FrameReader` reports
`Truncated { expected, actual }` with the arithmetic. What cannot be deleted is the commands
themselves; what can be deleted from the commands crate is *all format knowledge*.

## Files involved

Line counts at `eb8760e9`. All paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `types/src/codec/mod.rs` | *new (~70)* | **The change.** Relocated `safe_capacity` (from `persistence/src/serialization/mod.rs:46-55`) + `SerializationError` (from `:61-74`), verbatim. `pub mod probabilistic; pub mod reader;`. |
| `types/src/codec/reader.rs` | *new (~197)* | **Relocated verbatim.** `FrameReader` and its seven tests, moved from `persistence/src/serialization/frame.rs` with `pub(crate)` → `pub`. Only dependency is `bytes` + the error enum; `frogdb-types` already declares `bytes` and `thiserror` (`types/Cargo.toml:16,19`). |
| `types/src/codec/probabilistic.rs` | *new (~350)* | **The interface.** `BloomChunkCodec::{encode,decode}` and `CuckooChunkCodec::{encode,decode}`, bodies moved verbatim from `probabilistic.rs:25-68`, `:84-118`, `:264-305`, `:308-387`. The bloom/cuckoo half of `alloc_guard_tests` (`probabilistic.rs:859-1119`) moves with them. |
| `types/src/lib.rs` | 76 | **Primary, one line.** `pub mod codec;` alongside `pub mod bloom;` `:8` / `pub mod cuckoo;` `:11`. **No root `pub use` of `SerializationError`** — see §Risk 3 (glob collision with `core/src/lib.rs:118`). |
| `persistence/src/serialization/probabilistic.rs` | 1311 | **Primary, LOCKED crate.** `serialize_bloom_filter :25-68`, `serialize_cuckoo_filter :84-118`, `deserialize_bloom_filter :264-305`, `deserialize_cuckoo_filter :308-387` — **deleted** (≈195 lines). The bloom/cuckoo guard tests `:911-1043` relocate. **Untouched: the entire HLL delta/merge region `:464-624` and its tests `:717-851`, `frame_boundary_tests :1125-1198`, and the t-digest/TopK/CMS codecs — those have one caller each and fail the move test.** **Zero `FM-` tags in this file.** |
| `persistence/src/serialization/mod.rs` | 732 | **Primary, LOCKED crate.** `safe_capacity :46-55` and `SerializationError :61-74` **move**; both are replaced by `pub use frogdb_types::codec::{SerializationError, safe_capacity};` so every one of the 11 files in the module that names them (`grep -c`: `registry.rs` 19, `frame.rs` 14, `search.rs` 12, `collections.rs` 5, `marker.rs` 5, …) compiles unchanged. `pub(crate) use frame::FrameReader;` `:35` → `pub(crate) use frogdb_types::codec::reader::FrameReader;`. **The one `FM-` tag in this file (`:688`, FM-PERSISTENCE-036) is 600+ lines away and untouched.** |
| `persistence/src/serialization/frame.rs` | 197 | **Deleted** (relocated whole, including tests). |
| `persistence/src/serialization/registry.rs` | 711 | **Primary, small.** `encode_bloom :120-125` / `decode_bloom_value :217-219` / the cuckoo pair `:233-235` become one-liners over the shared codec; `BLOOM_CODEC :295-299` and `CUCKOO_CODEC :315-319` are unchanged in shape — the table *is* the persistence-side seam and it survives. |
| `commands/src/bloom.rs` | 758 | **Primary.** `BfScandump::execute` `:517-564` — the 17-line hand encoder `:532-548` → `BloomChunkCodec::encode(bf)`. `BfLoadchunk::execute` `:592-667` — **56 lines deleted** (`:612-663`), including the `use bitvec::prelude::*;` at `:593`. **Zero `FM-` tags.** |
| `commands/src/cuckoo.rs` | 839 | **Primary.** `CfScandump::execute` `:577-625` — hand encoder `:591-611` → `CuckooChunkCodec::encode(cf)`. `CfLoadchunk::execute` `:653-747` — **77 lines deleted** (`:665-744`), including the unchecked mul `:710`. **Zero `FM-` tags.** |
| `commands/Cargo.toml` | — | **Primary, one line.** `bitvec.workspace = true` deleted: `bloom.rs:593` is the **only** `bitvec` mention in the entire crate (verified by `grep -rn bitvec commands/src`). |
| `types/src/cuckoo.rs` | 600 | **Read-only evidence.** `CuckooLayer` `:27-39`; `from_raw` `:59-73` stores `num_buckets` **as given**, not `buckets.len()` — which is why a `bucket_size == 0` layer round-trips its bogus count into the persisted payload. `primary_index` `:83-86` is `hash % self.num_buckets` — §Adjacent finding. |
| `types/src/bloom.rs` | 361 | **Read-only evidence.** `BloomLayer` `:16-25` = `BitVec` + `u32` + 2×`u64`; `from_raw` `:47-55`. |
| `core/src/lib.rs` | 166 | **Read-only, load-bearing.** `pub use frogdb_types::*;` `:7` — the reach that makes `frogdb-types` the home without a `Cargo.toml` edit. `pub use persistence::{… SerializationError …}` `:116-122` — must keep resolving; §Risk 3. No `codec` module exists in this crate (module list `:14-40`), so the glob re-export introduces no collision. |
| `core/src/persistence/mod.rs` | — | **Read-only evidence.** `pub use frogdb_persistence::*;` `:13` and `pub use frogdb_persistence::{…, serialization, …};` `:15` — the commands crate has had a *compile path* to the hardened module all along; **`pub(super)` visibility on the four functions is the only reason it was re-implemented instead of called.** That is the whole defect in one sentence. |
| `commands/Cargo.toml` / `persistence/Cargo.toml` / `protocol/Cargo.toml` | 78 / 60 / 20 | **Read-only evidence** for the home decision: commands declares `frogdb-core` + `frogdb-protocol` only (`:55-56`); persistence declares `frogdb-types` (`:15`); protocol declares **no** `frogdb-*` dependency. |
| `redis-regression/tests/bloom_regression.rs` | 715 | **Read-only regression pin.** `bf_scandump_loadchunk_roundtrip :246-283`, `cf_scandump_loadchunk_roundtrip :487-…` — the **only** end-to-end coverage of either path, both happy-path only. Requires the `full`/`cmd-full` feature. The byte-shape oracle for the refactor. |
| `testing/fuzz/fuzz_targets/deserialize.rs` (repo root) | 39 | **Read-only evidence of the asymmetry.** Fuzzes `frogdb_persistence::deserialize` across all 17 markers — so the *persistence* bloom/cuckoo decoders are fuzzed and the *command* ones are not. There is no `bf_loadchunk` target in the 34-target list. |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | **Read-only.** `Status: LOCKED` `:3-5`; scope `:10-17`; **no row mentions `chunk`, `bloom`, `cuckoo`, or `probabilistic`** (verified by grep). The rows this change must not disturb are FM-PERSISTENCE-033 `:489`, 045 `:501`, 047 `:513`. |
| `website/src/content/docs/compatibility/overview.mdx` | 154 | **Hotfix H1 target.** "Other differences" `:105`, "Not supported" `:124`. |
| `website/src/content/docs/compatibility/command-matrix.mdx` | 34 | **Read-only — generated, must not be hand-edited** (`:3`, `:11`, `:25-27`). It reports BF/CF `SCANDUMP`+`LOADCHUNK` as plain **Supported** because status is derived from *regression-test exclusions* and `website/src/data/compat-exclusions.json` has **no** bloom or cuckoo entry. §Problem 4. |

## Problem

### 1. One format, two decoders, and the seam that already exists is closed by a visibility keyword

The formats are not merely similar. Field for field, in order:

| Region | `BF.SCANDUMP` (`bloom.rs:535-547`) | `serialize_bloom_filter` (`probabilistic.rs:36-54`) |
|---|---|---|
| header | `error_rate f64`, `expansion u32`, `non_scaling u8`, `num_layers u32` | identical, identical order |
| per layer | `k u32`, `count u64`, `capacity u64`, `size_bits u64`, packed bits | identical |

and for cuckoo (`cuckoo.rs:594-610` vs `probabilistic.rs:94-109`): `bucket_size u8`,
`max_iterations u16`, `expansion u32`, `delete_count u64`, `num_layers u32`; then per layer
`num_buckets u64`, `bucket_size u8`, `count u64`, `capacity u64`, `bucket_size` × `u16`
fingerprints. Identical, in identical order, little-endian throughout.

`frogdb-core` **already re-exports the whole persistence serialization module**
(`core/src/persistence/mod.rs:13`, `:15`), and `frogdb-commands` already depends on
`frogdb-core`. The reason `BF.LOADCHUNK` re-implements the decoder rather than calling it is a
single visibility keyword: `deserialize_bloom_filter` is `pub(super)` (`probabilistic.rs:264`).
The seam is *there*; it is shut. This is the cheapest possible kind of duplication to have
created and the most expensive kind to keep, because nothing in the build fails when the two
copies drift.

### 2. They drifted. Every difference runs the same direction

| Guard | `frogdb-persistence` | `frogdb-commands` |
|---|---|---|
| bounds-checked cursor | `FrameReader`, never panics (`frame.rs:8-11`, `:52-65`) | hand-rolled `offset`, `try_into().unwrap()` ×8 |
| layer-count pre-check | `num_layers > remaining()/28` → `Truncated` (`:276-281`); `/25` for cuckoo (`:321-326`) | **none** |
| bounded pre-allocation | `safe_capacity(count, min_elem, remaining)` at `:283`, `:328`, `:358`, `:360` | `Vec::with_capacity(num_layers)` `bloom.rs:625`, `cuckoo.rs:690`; `Vec::with_capacity(num_buckets)` `cuckoo.rs:717` |
| fingerprint-region arithmetic | `checked_mul` ×2 → `InvalidPayload` on overflow (`:344-351`) | `num_buckets * bucket_size * 2` **unchecked** (`cuckoo.rs:710`) |
| validity rule: buckets with zero bucket size | rejected (`:337-341`) | **none** |
| error reporting | `Truncated { expected, actual }` — states the arithmetic | four fixed strings ("Data too short", "Data truncated", …) |
| unit tests | ~260 lines, two-sided (`:859-1119`) | **zero** |
| fuzz target | `testing/fuzz/fuzz_targets/deserialize.rs` | **none** |

Eight rows, all the same direction. That is what "hardening exists only on one side" looks like
when the two sides are the same format, and it is not a thing anyone decided — it is the
signature of a copy that was made once and then only one copy got attention.

### 3. LIVE: `CF.LOADCHUNK` can persist a value recovery refuses to decode

This is the finding the brief did not anticipate, and it needs no hostile payload — only a
`bucket_size` of 0 on a layer that claims one bucket.

1. `CfLoadchunk::execute` (`cuckoo.rs:653-747`) has no validity rule. With
   `num_buckets = 1, bucket_size = 0`, the fingerprint loop `:720-724` runs zero times, so
   `buckets = [vec![]]`, and `CuckooLayer::from_raw` (`types/src/cuckoo.rs:59-73`) stores
   `num_buckets = 1` **as given** alongside `bucket_size = 0`.
2. `CF.LOADCHUNK` is `WalStrategy::PersistFirstKey` (`cuckoo.rs:641`), so the value is
   serialized and written. `serialize_cuckoo_filter` (`probabilistic.rs:100-110`) faithfully
   re-emits `num_buckets = 1`, `bucket_size = 0`, and zero fingerprint bytes.
3. On recovery, `deserialize_cuckoo_filter` hits `:337-341` —
   `layer_bucket_size == 0 && num_buckets > 0` → `InvalidPayload("Cuckoo filter layer has
   buckets but zero bucket size")`. **The key does not decode.**

Where that lands is entirely a matter of the persistence policy the operator picked, and all
three destinations are specced:

- default `recovery.on-decode-failure = continue` → **FM-PERSISTENCE-033** (spec `:489`): the
  key is silently dropped from the keyspace, counted, and surfaced in the load stats. The client
  that wrote it got `+OK`.
- `refuse` → **FM-PERSISTENCE-047** (spec `:513`): **the node will not boot**, naming that key.
- a node whose only decodable content was that key → **FM-PERSISTENCE-045** (spec `:501`): a
  refused boot with no knob to override it.

The persistence side is *correct* here and its spec rows are *satisfied* — an undecodable value
is exactly what they describe. The defect is that the command path is allowed to manufacture
one. Note the asymmetry this creates for the fix: hardening the storage decoder further would
make this **worse**, not better; the only correct direction is for the two decoders to be one
decoder, which is this proposal.

The bloom pair has no equivalent divergence — its guard set is a strict superset with no
validity rule, so every payload `BF.LOADCHUNK` accepts, `deserialize_bloom_filter` also accepts.
Cuckoo is the one that disagrees, and it disagrees because of the one guard that is about
*meaning* rather than about *size*.

### 4. The compatibility matrix reports both commands as plain "Supported", and the dump protocol is not Redis's

`BF.SCANDUMP` returns `Response::Array([Integer(0), bulk(data)])` on the **first** call
(`bloom.rs:551-554`) — iterator 0, whole filter, one chunk. The regression test's own comment
concedes the deviation: *"FrogDB uses single-chunk dump: SCANDUMP with iterator 0 returns [0,
data]"* (`bloom_regression.rs:258`, and again at `:497` for cuckoo).

Upstream RedisBloom's protocol is the opposite: the first call with iterator 0 returns
**`(1, data)`**, and a client loops until the returned iterator is 0. That is visible in
RedisBloom's own source — `BF.LOADCHUNK`'s initial-load branch is guarded by `iter == 1` and
`CF.LOADCHUNK`'s by `pos == 1` — and it is what the documented client loop
(`while iter != 0: save(chunk)`) depends on. Against FrogDB, that loop reads `iterator == 0` on
the very first response, **breaks before saving anything**, and produces an empty dump. FrogDB's
`LOADCHUNK` then hard-rejects any non-zero iterator (`bloom.rs:606-609`,
`cuckoo.rs:665-669`), so a RedisBloom dump cannot be loaded either. The formats are FrogDB's own
and interchange in neither direction.

None of that is documented. `command-matrix.mdx` is generated from the registry joined against
regression-test *exclusions* (`:11`, `:25-27`), and `compat-exclusions.json` carries no bloom or
cuckoo entry — so both commands render as unqualified **Supported**. Per the project's own rule
("Deviations should be improvements", and deltas belong on the overview page), a deviation this
size needs a line. That is hotfix **H1**, and it is independently landable today.

## Proposed change

### A. `frogdb_types::codec` — the module the format should have had

Three files, no new dependency in any crate:

```
types/src/codec/mod.rs           SerializationError, safe_capacity   (moved verbatim)
types/src/codec/reader.rs        FrameReader + its 7 tests           (moved verbatim)
types/src/codec/probabilistic.rs BloomChunkCodec, CuckooChunkCodec   (moved verbatim)
```

The **interface** is four functions:

```
BloomChunkCodec::encode(&BloomFilterValue) -> Vec<u8>
BloomChunkCodec::decode(&[u8]) -> Result<BloomFilterValue, SerializationError>
CuckooChunkCodec::encode(&CuckooFilterValue) -> Vec<u8>
CuckooChunkCodec::decode(&[u8]) -> Result<CuckooFilterValue, SerializationError>
```

The **implementation** behind it is nine header fields, two nested count-driven regions, five
allocation guards, one validity rule, and the `bits_len`-vs-`bits_bytes` rounding rule. That
ratio is the definition of a deep module: both call sites shrink to one line each, and every
fact about the format sits behind them. `TypeMarker` stays out — it is a *storage* concern
(`persistence/src/serialization/marker.rs`), and the codec table already supplies it
(`registry.rs:295-299`, `:315-319`). A `LOADCHUNK` payload carries no marker and no frame
header, which is precisely why the marker does not belong in the codec's signature.

`SerializationError` **moves rather than being wrapped**. Wrapping would change persistence's
error shape, and that shape is client-visible: `RESTORE` surfaces its `Display` text. Moving the
enum verbatim and re-exporting it from `persistence/src/serialization/mod.rs` keeps every
variant, every message string, and every `matches!` in the existing tests byte-identical, and
keeps `frogdb_core::SerializationError` (`core/src/lib.rs:118`) resolving to the same type.

### B. `frogdb-persistence` — delete the wrappers, keep the table

`serialize_bloom_filter` / `serialize_cuckoo_filter` / `deserialize_bloom_filter` /
`deserialize_cuckoo_filter` are **deleted**. Their sole remaining job — pairing a payload with a
`TypeMarker` — is what `TypeCodec` already does, so `registry.rs`'s four adapters call the shared
codec directly. The `debug_assert_eq!` pre-size contracts (`probabilistic.rs:62-66`, `:112-116`)
move with the encoders; they are the only thing that makes a wrong capacity formula observable
and the corpus already treats them as part of the contract.

This is the **adapter** direction that matters: persistence adapts a *shared* codec to *its*
framing, instead of owning the codec and hiding it. Nothing else in the serialization module
changes — the HLL delta/merge machinery (`:464-624`), t-digest, TopK and CMS keep their existing
`pub(super)` bodies because each has exactly one caller and moving them would be motion without
leverage.

### C. `frogdb-commands` — delete all format knowledge

`BfScandump::execute` `:532-548` → `BloomChunkCodec::encode(bf)`. `BfLoadchunk::execute`
`:612-663` → `BloomChunkCodec::decode(data).map_err(|e| CommandError::InvalidArgument { message:
e.to_string() })?`. Same shape for the cuckoo pair. The `data.len() < 17` / `< 19` pre-checks go
with them: `FrameReader` already answers "how many bytes did you need, how many did you get".

Consequences worth naming: `bitvec` leaves `commands/Cargo.toml` (one use site in the crate,
`bloom.rs:593`); `frogdb_core::{BloomLayer, CuckooLayer}` leave the import lists (`bloom.rs:6-10`,
`cuckoo.rs:728`) because raw layer construction is no longer a command's business; and the four
bespoke error strings collapse to the two `SerializationError` variants, which the LOADCHUNK path
renders through `CommandError::InvalidArgument` exactly as it renders its current strings.

**Locality** is the payoff to state plainly: after this, changing the filter format means editing
one file, and both the client-facing dump path and the on-disk path change together **by
construction** rather than by whoever remembers.

### D. Optional, cheap, and it fits the existing family: a codec chokepoint lint

`agents/seam-lints.md:27` already carries `lint-format-float` — *"exactly one `format_float`
definition exists, in `frogdb-protocol`; every other renderer re-exports it, so the reply path and
the WAL/replication store path can never disagree on a float's spelling."* That is this problem,
solved once, for floats. A `lint-chunk-codec` rule in the same shape — no `to_le_bytes()` on a
filter field, and no `BloomLayer::from_raw` / `CuckooLayer::from_raw` call, outside
`types/src/codec/probabilistic.rs` — costs one `rg` pattern, joins the compile-free `lint-gates`
subset, and makes the *third* copy un-mergeable. Recommended, not required; §Scope boundaries
notes the argument for deferring it.

## Security classification — flagged for the user

**Per the standing policy, this is filed and classified here, not fixed as a standalone hotfix,
and the proposal is not framed as a security change.** The remediation arrives as a side effect
of C above, because the command path stops having a decoder of its own.

**Class: remotely-triggerable resource exhaustion / process abort on an authenticated command
path.** `BF.LOADCHUNK` and `CF.LOADCHUNK` allocate from attacker-controlled wire counts with no
bound:

| Site | Wire input | Element | Worst-case request | Outcome |
|---|---|---|---|---|
| `bloom.rs:625` | `num_layers: u32` (`:622`) | `BloomLayer` = `BitVec` + `u32` + 2×`u64` (`types/src/bloom.rs:16-25`), 48 B | ~206 GB from a **17-byte** payload | allocator failure → `handle_alloc_error` → **`abort()`** |
| `cuckoo.rs:690` | `num_layers: u32` (`:687`) | `CuckooLayer`, 56 B | ~240 GB from a **19-byte** payload | as above |
| `cuckoo.rs:717` | `num_buckets: u64 as usize` (`:701`) | `Vec<u16>`, 24 B | up to `capacity overflow` | panic, or abort at intermediate sizes |
| `cuckoo.rs:710` | `num_buckets * bucket_size * 2` | — | wraps (no `overflow-checks` in `[profile.release]`, root `Cargo.toml:202-204`) | **defeats the `:711` truncation guard**, feeding `:717` |

Two severity notes, both verified, both cutting in opposite directions:

- **Panics are contained; allocation failure is not.** `core/src/shard/panic_guard.rs:1-74`
  isolates a panic raised while executing a client command (`PanicSite::Command`, `:88`) — the
  shard survives, the client gets `-ERR internal error`, `ShardPanicsIsolated` increments. So the
  `capacity overflow` branch is an availability nick, not an outage. **`Vec::with_capacity` on an
  allocation the allocator refuses does not unwind** — Rust calls `handle_alloc_error`, which
  aborts. `catch_unwind` cannot see it. The `bloom.rs:625` and `cuckoo.rs:690` rows above are
  therefore a **whole-process kill from a 17-byte argument**, and the panic guard's own doc
  (`:8-11`, on round-2 issue 63) describes exactly this class as the thing it exists to prevent.
- **The `:710` wrap is not an out-of-bounds read.** The brief's "potential OOB-read bypass" is
  not reachable: the read at `:721` is `data[offset..offset + 2]`, bounds-checked by the
  compiler. The wrap's actual effect is to smuggle a large `num_buckets` past the `:711` guard
  into the `:717` allocation — a reclassification from memory-safety to resource exhaustion.

**Adjacent, related, not claimed by this proposal:** `CuckooLayer::primary_index`
(`types/src/cuckoo.rs:83-86`) is `hash % self.num_buckets`, and a layer with `num_buckets == 0` is
accepted by **both** decoders — persistence explicitly blesses it
(`probabilistic.rs:1004-1013`, *"an empty layer is not a hostile one"*). Any subsequent `CF.ADD`
on such a filter divides by zero. Caught by the panic guard, so bounded, but it is a live
`-ERR internal error` reachable from a stored value. The fix belongs in `frogdb-types`' filter
implementation, not in the codec — recommend a separate issue.

**Recommended handling:** the `commands`-side adoption of the persistence guards (change C) closes
every row in the table above with no code written specifically for it. No CVE-shaped issue, no
standalone patch, no security framing in the commit message. **User decision requested** on one
boundary: hotfix **H2** below is the `bucket_size == 0` validity rule, which I have classified as
a *durability* fix (§Problem 3) rather than a hostile-input fix — if you would rather that also
wait for the refactor, drop H2 and the proposal loses nothing but a week.

## Testability improvement

Today the format has **zero** unit-level tests on the command side and **~260 lines** of
two-sided guard tests on the storage side (`probabilistic.rs:859-1119`) that the command side
cannot benefit from. After the move, one test file covers both callers. Concretely:

**What the relocation buys immediately.** The existing bloom/cuckoo guard tests
(`:911-1043`) become tests of `BF.LOADCHUNK`'s decoder too, without a line being written. Their
style is already the right one — each guard is driven from *both* sides (a hostile count refused
with the arithmetic it reports, **and** a payload that fits exactly still decoding), which is what
keeps a guard from being tightened into a corruption bug.

**Red-first forcing tests the codec needs**, all in `frogdb-types` (the crate that will own the
mutants):

1. **The round-trip property** — the test that would have caught §Problem 3, and the one the
   corpus is missing entirely: for any `CuckooFilterValue`/`BloomFilterValue`,
   `decode(encode(v))` succeeds and `encode(decode(encode(v))) == encode(v)`. `frogdb-types`
   already has `proptest` in `[dev-dependencies]` (`types/Cargo.toml:69`), so this is a
   `proptest!` block, not a harness.
2. **The closure property, stated as an invariant** — *every payload `decode` accepts
   re-encodes to a payload `decode` accepts*. This is the property whose violation is the whole
   of §Problem 3, and it is the reason the durability defect is a *test-design* gap and not just
   a coding slip: nothing anywhere asserts that the codec's accepted language is closed under
   round-trip.
3. **Wrapping-multiply payload** — `num_buckets = 2^63`, `bucket_size = 2`, so
   `num_buckets * bucket_size * 2` wraps to 0. Must be `InvalidPayload("… overflow")`. Red today
   against `commands`, green today against `persistence` — the single cleanest demonstration
   that the two decoders are two decoders.
4. **Oversize layer count** — `num_layers = u32::MAX` with a 17-byte payload. Must be
   `Truncated { expected: 17 + u32::MAX as usize * 28, actual: 17 }`, allocating nothing.
5. **Zero bucket size with a positive bucket count** — the §Problem 3 payload, asserted at the
   codec rather than only at the storage decoder.
6. **Malformed header** — a 3-byte payload; `Truncated`, never a panic.

**A fuzz target is now free.** `testing/fuzz/fuzz_targets/deserialize.rs` fuzzes the persistence
decoders across all 17 markers; there is no `loadchunk` target among the 34. A
`chunk_codec.rs` target over `BloomChunkCodec::decode` / `CuckooChunkCodec::decode` is ~15 lines
and covers *both* entry points at once — which is only possible after they are one function.

**Command-level integration coverage** stays where it is (`bloom_regression.rs:246`, `:487`) and
gains a negative sibling: `BF.LOADCHUNK`/`CF.LOADCHUNK` with a malformed chunk must answer with
an error and leave the key absent, rather than storing a filter that recovery will reject.

## Locked-area clearance and mutation-gate implications

`frogdb-persistence` is **LOCKED** (spec `.scratch/hardening/specs/persistence-failure-modes.md`,
`Status: LOCKED` `:3-5`, gate 0.85). Four findings, all verified:

1. **No `FM-PERSISTENCE` row covers the chunk decode path.** Grepping the spec for `chunk`,
   `bloom`, `cuckoo` and `probabilistic` returns nothing. The spec's scope paragraph (`:10-17`)
   is the shard-to-storage path: `WalTarget`/`persist_records`, `wal/`, `snapshot/`, quiesce.
   Value-payload codecs are not enumerated. **So this is not a spec-first change** — there is no
   row to edit, because the change is behavior-preserving on the persistence side.
2. **Nothing persistence-observable changes.** Same bytes out (encoders moved verbatim), same
   bytes accepted (decoders moved verbatim), same error variants and same `Display` text
   (`SerializationError` relocated, not wrapped). The three rows in reach —
   FM-PERSISTENCE-033 `:489`, 045 `:501`, 047 `:513` — describe what happens *to* an undecodable
   value; the set of undecodable values is unchanged. **Hard acceptance criterion: not one
   existing persistence test may be edited to accommodate the refactor** (relocated verbatim is
   fine; edited is not).
3. **`just lint-failure-modes` is unaffected.** `probabilistic.rs` and `frame.rs` carry **zero**
   `FM-` tags; `mod.rs`'s single tag (`:688`, FM-PERSISTENCE-036) is 600 lines from any edit;
   `commands/src/bloom.rs` and `cuckoo.rs` carry zero. No tagged test moves crates, so neither
   direction of the lint's spec↔test agreement check can trip.
4. **The gate scope genuinely shrinks, and that must be paid for.** This is the one real cost.
   `cargo mutants -p <crate>` runs only that package's own tests (`.cargo/mutants.toml`,
   `Justfile:266-279`). Moving ~195 lines of decoder plus `safe_capacity` plus `FrameReader` into
   `frogdb-types` removes those functions from `frogdb-persistence`'s denominator — and
   `frogdb-types` **is not a locked crate and has no gate**. A guard that is at 99.1% mutation
   coverage today would land in a crate where nothing measures it. Three ways to pay, in
   preference order:
   - **(a) Move the tests with the code and gate the module explicitly.** The relocated
     `alloc_guard_tests` are already two-sided, so `cargo mutants -p frogdb-types --file
     src/codec/probabilistic.rs --file src/codec/reader.rs --file src/codec/mod.rs` should score
     at or above 0.85 on arrival. Record that command in the PR and in the persistence spec's
     scope paragraph as a pointer ("the value-payload codecs for bloom/cuckoo now live in
     `frogdb-types::codec`; their gate is …"). **Recommended.**
   - (b) Promote `frogdb-types` to a fifth gated area. Correct in the long run, far too large for
     this proposal, and it would swallow 20+ unrelated modules.
   - (c) Leave the codec in `frogdb-persistence` and make it `pub`, with commands calling
     `frogdb_core::persistence::serialization::…`. **Rejected**: it points the command layer at
     the storage engine, and it means every filter-format change is a change to a locked crate
     forever. The whole leverage of this proposal is that the format stops being storage's
     private business.
5. **`just mutants-diff frogdb-persistence` is required before push** — it is the project's push
   discipline for any change touching a locked crate, and this one deletes ~200 lines from it.
   Expect a near-empty diff report (deletions produce no mutants); the meaningful run is
   (a) above. Pair it with `just mutants-diff frogdb-types --file src/codec/**` so the moved
   guards are measured on arrival rather than on someone's memory.

**Seam-lint clearance.** `lint-no-typed-unwrap` (`agents/seam-lints.md:30`) governs
`crates/commands/src`: the rewritten `execute` bodies keep using `ctx.store.get_bloom(key)?` /
`get_cuckoo(key)?` (`core/src/store/typed.rs:276-277`) and add no check-then-unwrap and no
hand-rolled `WrongType`. No other rule in the family names any file in this set.

## Risks / scope boundaries

**Risk 1 — a "verbatim" move that is not verbatim.** The whole safety argument rests on the
bodies being unchanged. Mitigation: land the move as a mechanical commit with **zero** edits
inside the moved functions (visibility keywords and `use` lines only), then any behavior change
as a separate commit on top. A reviewer should be able to `git diff -M` the move and see pure
renames.

**Risk 2 — the commands path becomes stricter, and that is a behavior change.** Payloads
`BF.LOADCHUNK`/`CF.LOADCHUNK` accept today and reject tomorrow: (i) an oversize `num_layers`,
(ii) a wrapping fingerprint region, (iii) a bucket-bearing layer with zero bucket size. Every one
of those is a payload no `SCANDUMP` ever emits, so no legitimate FrogDB dump is affected — the
round-trip regression tests (`bloom_regression.rs:246`, `:487`) are the proof and must stay green
unedited. Against Redis compatibility this moves **toward** upstream, not away: RedisBloom
validates a loaded structure (`CuckooFilter_Init` requires `numBuckets` to be a power of two and
non-zero; `CuckooFilter_ValidateIntegrity` re-checks structural constraints after load) and
`CF.LOADCHUNK` rejects a header of the wrong size outright with `"Invalid header"`. FrogDB
accepting *less* than it does today is FrogDB behaving more like the thing it is compatible with.
Document it as an improvement in the changelog; no config knob, no opt-out.

**Risk 3 — a glob-re-export collision on `SerializationError`.** `core/src/lib.rs:7` is
`pub use frogdb_types::*;` and `:118` explicitly re-exports `SerializationError` from
`persistence`. If `types/src/lib.rs` also re-exported the enum at its **root**, the two would name
the same item by two paths. Rust resolves this (an explicit `use` shadows a glob), but it is
gratuitous: **keep the enum at `frogdb_types::codec::SerializationError` with no root `pub use`.**
Verified safe: `frogdb-core` has no `codec` module (`core/src/lib.rs:14-40`), so the glob
introduces `frogdb_core::codec` cleanly.

**Risk 4 — feature gating.** `commands/src/bloom.rs` and `cuckoo.rs` are behind the `bloom` /
`cuckoo` features (`commands/src/lib.rs:27-32`), which are **not** in `core-profile`
(`commands/Cargo.toml:19-46`). `frogdb-types::codec` must be unconditional — `frogdb-persistence`
needs it in every build, including `cmd-core`. Concretely: the codec compiles always, its command
callers compile only under `full`/`cmd-full`, and `just test frogdb-server` (core profile) will
**not** exercise the LOADCHUNK integration tests. That is the status quo, but it means the
red-first tests of §Testability must live in `frogdb-types` — where they run unconditionally —
and not only in `redis-regression`.

**Scope boundaries — what this proposal does not touch:**

- **The other four probabilistic codecs.** t-digest, TopK, CMS and the HLL delta/merge machinery
  (`probabilistic.rs:464-624`, `:626-715`) have exactly one caller each. Moving them is motion
  without leverage and fails the same test this proposal applies to itself.
- **The frame header.** `build_frame`/`deserialize`'s 24-byte header is proposal 40's subject and
  stays in `frogdb-persistence` — a `LOADCHUNK` payload has no header, which is the cleanest
  possible demarcation between the two proposals.
- **The SCANDUMP iterator protocol.** §Problem 4 establishes that FrogDB's single-chunk
  `(0, data)` first response breaks the upstream client loop. Fixing it changes a reply shape and
  requires editing `bloom_regression.rs:246`/`:487`, which puts it outside a refactor whose
  acceptance criterion is "no existing test edited". **Recommend a separate issue**; H1
  documents the deviation in the meantime.
- **`CuckooLayer::primary_index`'s division by zero** (`types/src/cuckoo.rs:83-86`) — a filter
  implementation bug, not a codec bug, and reachable from a value both decoders currently bless.
  Separate issue.
- **The chokepoint lint (D)** is optional. Argument for deferring: `lint-gates` runs on every
  commit and the family is at fifteen rules; adding a sixteenth for a two-call-site invariant may
  not earn its place until a third caller is plausible. Argument for landing it: the third caller
  is exactly what nobody will notice.

**Sibling edges (file-level, with preferred ordering):**

| Sibling | Shared file | Overlap | Ordering |
|---|---|---|---|
| **40** frame-codec-unify | `persistence/src/serialization/probabilistic.rs`; `frame.rs` | **Disjoint hunks, same files.** 40 owns `framed_payload :512-522` and `reframe_with_header :528-544` (the HLL merge helpers) and builds `FrameHeader` *on* `FrameReader`; 89 owns `:25-118` + `:264-387` and **relocates** `frame.rs` wholesale. | **40 first.** 40's acceptance criteria forbid editing existing tests, and 89 relocates test modules across crates; rebasing 89 onto 40 is a path rename, rebasing 40 onto 89 is not. Either way 40's `FrameHeader` picks up the relocated reader through persistence's `pub(crate) use` with **zero** import edits. |
| **41** persistence-small-dedups | — | **None.** 41's own edge table (`41:602`) records it touching `wal/writer.rs`, `wal/flush.rs`, `wal/sink.rs` and no frame encode/decode. | Independent. |
| **39** recovery-replay-driver | — | **None** in the serialization module (`39:323` cites `persistence/Cargo.toml`'s dependency list as read-only evidence only). | Independent. |
| **84** blocking-op-dedupe | `types/src/` | **None.** 84 deletes `types/src/types/mod.rs:322-426` and does **not** edit `types/src/lib.rs` (`84:111` lists it read-only). 89 adds `types/src/codec/**` (new) and one line to `types/src/lib.rs`. | Independent. 84's verified fact that `frogdb-protocol` has no `frogdb-*` dependency is re-verified here and used for the opposite conclusion (protocol is too shallow for a codec that names value types). |
| **85** frogdb-macros-fate | `types/src/types/mod.rs` | **None.** 85 owns `:71-128` and `:298-320`; 89 touches neither that file nor those regions. | Independent. |
| **90** CommandSpec sweep (not yet in tree) | `commands/src/bloom.rs`, `cuckoo.rs` | **Same files, adjacent hunks.** 90 rewrites the `static SPEC: CommandSpec` literals (`bloom.rs:499-513`, `:574-588`; `cuckoo.rs:559-573`, `:635-649`); 89 rewrites only the `execute` bodies below them. | **90 solo, first or last, per its own brief.** If 90 lands first, 89 rebases mechanically. If 89 lands first, 90's sweep is unaffected — 89 removes no `CommandSpec` and adds none. |

## Effort

**M.** One mechanical relocation, two call-site rewrites, and a mutation-gate bookkeeping
decision. Sizing:

| Step | Size |
|---|---|
| 1. Move `FrameReader` + `safe_capacity` + `SerializationError` → `types/src/codec/`, re-export from `persistence/src/serialization/mod.rs` | ~250 lines relocated, 3 lines added; **no behavior** |
| 2. Move the four bloom/cuckoo functions + their guard tests → `types/src/codec/probabilistic.rs`; wire `registry.rs`'s four adapters | ~195 + ~135 lines relocated; **no behavior** |
| 3. Rewrite the four command bodies; drop `bitvec` from `commands/Cargo.toml` | **−150 lines**; behavior change per §Risk 2 |
| 4. Red-first tests 1–6 + the round-trip proptest, in `frogdb-types` | ~120 lines new |
| 5. `just mutants -p frogdb-types --file src/codec/**` + `just mutants-diff frogdb-persistence` | gate bookkeeping, §Locked-area (4) |

Steps 1 and 2 are independently reviewable and behavior-free; step 3 is the only one that changes
what the server accepts. **Recommend landing 1+2 as one commit and 3+4 as a second**, so the
locked-crate diff a reviewer must scrutinize contains no behavior at all.

### Independently-landable hotfixes

| # | What | Where | Class | Independent of the refactor? |
|---|---|---|---|---|
| **H1** | Document the probabilistic dump deviation: FrogDB's `BF`/`CF` `SCANDUMP` returns the whole filter as one chunk with iterator **0** on the first call (upstream returns **1** and loops), the payload is FrogDB's own layout, and dumps do not interchange with RedisBloom in either direction. One entry under "Other differences" (`compatibility/overview.mdx:105`). **The generated matrix (`command-matrix.mdx`) must not be hand-edited.** | `website/src/content/docs/compatibility/overview.mdx` | **LIVE** (documentation states, by omission, a compatibility that does not exist) | **Yes** — docs only, lands today. |
| **H2** | Reject a cuckoo layer with `num_buckets > 0 && bucket_size == 0` in `CfLoadchunk::execute` — the §Problem 3 divergence. ~5 lines, mirroring `probabilistic.rs:337-341`, plus a regression test asserting the stored value re-decodes. | `commands/src/cuckoo.rs:692-735` | **LIVE** (durability: a client command persists a value recovery refuses) | **Yes**, and **flagged for your decision** — it is the one guard whose justification is round-trip closure rather than hostile input. If you prefer zero pre-landing behavior change on the command path, drop it and it arrives with step 3. |
| — | Drop `bitvec` from `commands/Cargo.toml` | `commands/Cargo.toml` | LATENT (dead dependency) | **No** — `bloom.rs:593` is the one use site and only step 3 removes it. Listed for completeness, lands *with* the refactor. |

The hotfix yield here is deliberately thin, and that is the honest report: the two substantial
defects this proposal found both route through the shared codec, and one of them
(§Security classification) is parked by standing policy. Manufacturing a third would be padding.
