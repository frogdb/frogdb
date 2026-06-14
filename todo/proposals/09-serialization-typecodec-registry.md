# Proposal: Serialization TypeCodec Registry

Status: proposed
Date: 2026-06-13

## Problem

A data type's on-disk encode and decode live in different places, joined only by a hand-kept `u8`
type marker. The seam between a `Value` variant and its bytes is crossed twice — once going out
(`serialize_value`, `frogdb-server/crates/persistence/src/serialization/mod.rs:197-221`) and once
coming back (`deserialize_value`, `mod.rs:224-303`) — and the two halves are kept in agreement
entirely by hand. Adding a type means editing 4+ sites; the module has no single place that owns the
encode/decode pair for one type. The interface is shallow: a bare integer marker stands in for a
contract ("byte 11 means a hash with per-field expiry, laid out *this* way") that the compiler
cannot see.

The dangerous property is the **deletion test**: remove one decode arm from `deserialize_value` and
the crate still compiles and every other round-trip test still passes. The encode side keeps
emitting that marker; nothing in the type system or the test suite notices the decode half is gone.
The break surfaces only later, in production, when a snapshot or replication stream containing that
marker is loaded — `deserialize` returns `SerializationError::UnknownType` (the catch-all at
`mod.rs:301`) and the key is dropped. This is **silent data loss on snapshot load / replica
full-sync / WAL replay**, not a test failure. This is the same exhaustiveness class the CommandSpec
work closed for the dispatch table last round, but here it sits on the persistence wire, where the
failure mode is lost user data rather than a wrong reply.

### The 4+ edit sites per new type

All paths relative to `frogdb-server/crates/`. Adding one type touches every row:

| Edit site | File:line | What it is |
|-----------|-----------|------------|
| Type-marker constant | `persistence/src/serialization/mod.rs:63-95` | one of 17 hand-numbered `const TYPE_*: u8` |
| Encode dispatch arm | `persistence/src/serialization/mod.rs:197-221` | `serialize_value` match over `Value` |
| Decode dispatch arm | `persistence/src/serialization/mod.rs:224-303` | `deserialize_value` match over `u8` — **forgettable, still compiles** |
| Per-type encode/decode fns | `persistence/src/serialization/{string,collections,probabilistic,search,stream,timeseries}.rs` | the actual byte layout |
| `Value` enum variant | `types/src/types/mod.rs:36-67` | the in-memory type |

Of these, only the encode arm and the `Value` variant are compiler-enforced (a missing `Value`
match arm fails `serialize_value` to build). The **decode arm is not**: `deserialize_value` ends in
`_ => Err(SerializationError::UnknownType(type_byte))` (`mod.rs:301`), so a marker that has an encode
path but no decode path is a clean compile and a latent loss.

### The symmetry is untested for 9 of 17 markers

The round-trip tests that *would* catch a broken encode/decode pair do not enumerate the markers —
they are a hand-picked subset. Combining `persistence/src/serialization/mod.rs` unit tests with
`core/tests/proptest_serialization.rs`, the markers with **any** round-trip coverage are:

| Marker | Value | Round-trip tested? |
|--------|-------|--------------------|
| `TYPE_STRING_RAW` (0) | String (raw) | yes (`proptest_serialization.rs:41`, `mod.rs:365`) |
| `TYPE_STRING_INT` (1) | String (int) | yes (`proptest_serialization.rs:59`, `mod.rs:381`) |
| `TYPE_SORTED_SET` (2) | SortedSet | yes (`proptest_serialization.rs:73`) |
| `TYPE_HASH` (3) | Hash | yes (`proptest_serialization.rs:104`) |
| `TYPE_LIST` (4) | List | yes (`proptest_serialization.rs:131`) |
| `TYPE_SET` (5) | Set | yes (`proptest_serialization.rs:155`) |
| `TYPE_STREAM` (6) | Stream | yes (`proptest_serialization.rs:179`) |
| `TYPE_BLOOM` (7) | BloomFilter | **no** |
| `TYPE_HYPERLOGLOG` (8) | HyperLogLog | yes (`mod.rs:493/513/541`) |
| `TYPE_TIMESERIES` (9) | TimeSeries | **no** |
| `TYPE_JSON` (10) | Json | **no** |
| `TYPE_HASH_WITH_FIELD_EXPIRY` (11) | Hash (w/ field TTL) | **no** |
| `TYPE_CUCKOO` (12) | CuckooFilter | **no** |
| `TYPE_TOPK` (13) | TopK | **no** |
| `TYPE_TDIGEST` (14) | TDigest | **no** |
| `TYPE_CMS` (15) | CountMinSketch | **no** |
| `TYPE_VECTORSET` (16) | VectorSet | **no** |

Nine of seventeen markers — including every probabilistic type, JSON, time series, vector sets, and
the per-field-expiry hash encoding — have **zero** test that takes a value through `serialize` and
back through `deserialize`. For those nine, the deletion test passes trivially: there is no test to
delete. `TYPE_HASH_WITH_FIELD_EXPIRY` is the sharpest case — it is a *second* marker for the same
`Value::Hash` variant, reached only when `hash.has_field_expiries()` is true (`mod.rs:202`), and the
one hash round-trip test (`proptest_serialization.rs:104`) never sets a field expiry, so that encode
branch and its decode partner (`collections.rs:60` / `collections.rs:247`) are never exercised
together.

Locality is the underlying defect: the marker, the encode, and the decode for one type are three
declarations in three locations with no structural link. Leverage is low and risk is high — the
cheapest possible mistake (forget one arm) has the most expensive possible consequence (silent loss
of persisted data).

## Current state

### The markers — 17 hand-numbered constants (`mod.rs:62-95`)

```rust
/// Marker for raw string type.
const TYPE_STRING_RAW: u8 = 0;
/// Marker for integer-encoded string type.
const TYPE_STRING_INT: u8 = 1;
/// Marker for sorted set type.
const TYPE_SORTED_SET: u8 = 2;
const TYPE_HASH: u8 = 3;
const TYPE_LIST: u8 = 4;
const TYPE_SET: u8 = 5;
const TYPE_STREAM: u8 = 6;
const TYPE_BLOOM: u8 = 7;
const TYPE_HYPERLOGLOG: u8 = 8;
const TYPE_TIMESERIES: u8 = 9;
const TYPE_JSON: u8 = 10;
const TYPE_HASH_WITH_FIELD_EXPIRY: u8 = 11;
const TYPE_CUCKOO: u8 = 12;
const TYPE_TOPK: u8 = 13;
const TYPE_TDIGEST: u8 = 14;
const TYPE_CMS: u8 = 15;
const TYPE_VECTORSET: u8 = 16;
```

Nothing ties a constant to its encode or decode function; collisions and gaps are prevented only by
reading the list.

### Encode dispatch (`serialize_value`, `mod.rs:197-221`)

```rust
fn serialize_value(value: &Value) -> (u8, Vec<u8>) {
    match value {
        Value::String(sv) => serialize_string(sv),
        Value::SortedSet(zset) => serialize_sorted_set(zset),
        Value::Hash(hash) => {
            if hash.has_field_expiries() {
                serialize_hash_with_field_expiry(hash)
            } else {
                serialize_hash(hash)
            }
        }
        Value::List(list) => serialize_list(list),
        Value::Set(set) => serialize_set(set),
        Value::Stream(stream) => serialize_stream(stream),
        Value::BloomFilter(bf) => serialize_bloom_filter(bf),
        Value::HyperLogLog(hll) => serialize_hyperloglog(hll),
        Value::TimeSeries(ts) => serialize_timeseries(ts),
        Value::Json(json) => serialize_json(json),
        Value::CuckooFilter(cf) => serialize_cuckoo_filter(cf),
        Value::TopK(tk) => serialize_topk(tk),
        Value::TDigest(td) => serialize_tdigest(td),
        Value::CountMinSketch(cms) => serialize_cms(cms),
        Value::VectorSet(vs) => serialize_vectorset(vs),
    }
}
```

This match *is* compiler-enforced exhaustive over `Value` — add a variant and this fails to build.
That is the half that already works. Note the 1-to-N cases: `Value::String` produces marker 0 or 1
(decided inside `serialize_string`, `string.rs:4-20`); `Value::Hash` produces marker 3 or 11. The
marker is not a function of the `Value` variant alone, which is why a naive "one marker per variant"
table is wrong.

### Decode dispatch (`deserialize_value`, `mod.rs:224-303`)

```rust
fn deserialize_value(type_byte: u8, payload: &[u8]) -> Result<Value, SerializationError> {
    match type_byte {
        TYPE_STRING_RAW => {
            let sv = StringValue::new(Bytes::copy_from_slice(payload));
            Ok(Value::String(sv))
        }
        TYPE_STRING_INT => { /* ... */ }
        TYPE_SORTED_SET => Ok(Value::SortedSet(deserialize_sorted_set(payload)?)),
        TYPE_HASH => Ok(Value::Hash(deserialize_hash(payload)?)),
        TYPE_HASH_WITH_FIELD_EXPIRY => Ok(Value::Hash(deserialize_hash_with_field_expiry(payload)?)),
        TYPE_LIST => Ok(Value::List(deserialize_list(payload)?)),
        TYPE_SET => Ok(Value::Set(deserialize_set(payload)?)),
        TYPE_STREAM => Ok(Value::Stream(deserialize_stream(payload)?)),
        TYPE_BLOOM => Ok(Value::BloomFilter(deserialize_bloom_filter(payload)?)),
        TYPE_HYPERLOGLOG => Ok(Value::HyperLogLog(deserialize_hyperloglog(payload)?)),
        TYPE_TIMESERIES => Ok(Value::TimeSeries(deserialize_timeseries(payload)?)),
        TYPE_JSON => Ok(Value::Json(deserialize_json(payload)?)),
        TYPE_CUCKOO => Ok(Value::CuckooFilter(deserialize_cuckoo_filter(payload)?)),
        TYPE_TOPK => Ok(Value::TopK(deserialize_topk(payload)?)),
        TYPE_TDIGEST => Ok(Value::TDigest(deserialize_tdigest(payload)?)),
        TYPE_CMS => Ok(Value::CountMinSketch(deserialize_cms(payload)?)),
        TYPE_VECTORSET => Ok(Value::VectorSet(Box::new(deserialize_vectorset(payload)?))),
        _ => Err(SerializationError::UnknownType(type_byte)),  // mod.rs:301
    }
}
```

This match is over `u8`, **not** an enum, so it is exhaustive only because of the `_` arm. That
wildcard is exactly what makes the decode side forgettable: drop `TYPE_TOPK =>` and the code still
compiles — `serialize_topk` (`probabilistic.rs:191`) keeps writing marker 13, and `deserialize`
silently routes it into `Err(UnknownType(13))`. The encode and decode halves of one type sit ~70
lines apart in this file and in a separate per-type module (`probabilistic.rs`), with no compiler
link between them.

The per-type halves are themselves split across files with no shared anchor: `serialize_topk` lives
at `probabilistic.rs:191`, its partner `deserialize_topk` at `probabilistic.rs:580`; `serialize_json`
at `search.rs:9`, `deserialize_json` at `search.rs:15`. Each pair is a contract enforced only by the
matching length-prefixes and endianness the two functions happen to agree on by hand.

## Proposed design

Give each marker one home that owns its encode, its decode, and its byte value, and make the set of
markers a closed enum the compiler can reason about. Then dispatch through that registry so the two
halves can no longer drift, and add one registry-iterating test so a marker that lacks a working
round-trip is a test failure (and a missing decode arm is a *compile* error).

The design has two cooperating mechanisms:

1. **A closed `TypeMarker` enum** for compile-time decode exhaustiveness (replaces the 17 loose
   `const`s and the `u8` match).
2. **A `TypeCodec`-per-marker registry** for test-time symmetry enforcement and locality (collects
   encode + decode + a representative sample in one place per marker).

### 1. The closed marker enum (`serialization/marker.rs`)

```rust
/// Every persisted type marker. The byte values are the on-disk wire format and MUST NOT change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum TypeMarker {
    StringRaw = 0,
    StringInt = 1,
    SortedSet = 2,
    Hash = 3,
    List = 4,
    Set = 5,
    Stream = 6,
    Bloom = 7,
    HyperLogLog = 8,
    TimeSeries = 9,
    Json = 10,
    HashWithFieldExpiry = 11,
    Cuckoo = 12,
    TopK = 13,
    TDigest = 14,
    Cms = 15,
    VectorSet = 16,
}

impl TypeMarker {
    /// Single source of truth for "what markers exist". Used by the symmetry test.
    pub(crate) const ALL: &'static [TypeMarker] = &[
        Self::StringRaw, Self::StringInt, Self::SortedSet, Self::Hash, Self::List,
        Self::Set, Self::Stream, Self::Bloom, Self::HyperLogLog, Self::TimeSeries,
        Self::Json, Self::HashWithFieldExpiry, Self::Cuckoo, Self::TopK, Self::TDigest,
        Self::Cms, Self::VectorSet,
    ];

    fn from_byte(b: u8) -> Result<Self, SerializationError> {
        // The ONE place unknown markers become an error.
        Self::ALL
            .iter()
            .copied()
            .find(|m| *m as u8 == b)
            .ok_or(SerializationError::UnknownType(b))
    }
}
```

`#[repr(u8)]` with explicit discriminants pins the wire bytes; the enum is the single declaration of
the marker set. (`TypeMarker::ALL` is asserted complete by a tiny test that
`ALL.len() == <variant count>` and that discriminants are unique — or generated by a small derive
such as `strum::EnumIter` if we want to drop the hand-written slice.)

### 2. The per-marker codec and registry (`serialization/registry.rs`)

A codec is one marker's full contract: which byte it writes, how it turns a `Value` into a payload
(or declines, for the 1-to-N variants), how it turns a payload back into a `Value`, and a sample
value that is *guaranteed* to encode to this marker (so the symmetry test can reach every branch,
including `HashWithFieldExpiry`).

```rust
pub(crate) struct TypeCodec {
    pub(crate) marker: TypeMarker,

    /// Encode if this codec applies to `value`, else `None`.
    /// `Some`/`None` resolves the 1-to-N cases: the StringInt codec returns `None`
    /// for non-integer strings; the HashWithFieldExpiry codec returns `None` when
    /// the hash has no field expiries. Encode dispatch tries codecs in registry order.
    pub(crate) encode: fn(&Value) -> Option<Vec<u8>>,

    /// Decode a payload into a `Value`.
    pub(crate) decode: fn(&[u8]) -> Result<Value, SerializationError>,

    /// At least one value that `encode` returns `Some` for, used by the round-trip
    /// symmetry test. More than one is allowed (e.g. empty + populated).
    pub(crate) samples: fn() -> Vec<Value>,
}

/// The whole persistence type system, in declaration order. Order matters only for the
/// 1-to-N encode cases: more specific codecs (StringInt, HashWithFieldExpiry) precede
/// their fallbacks (StringRaw, Hash).
pub(crate) const REGISTRY: &[TypeCodec] = &[
    string::INT_CODEC,        // marker 1, must precede RAW
    string::RAW_CODEC,        // marker 0
    collections::SORTED_SET_CODEC,
    collections::HASH_FIELD_EXPIRY_CODEC, // marker 11, must precede HASH
    collections::HASH_CODEC,              // marker 3
    collections::LIST_CODEC,
    collections::SET_CODEC,
    stream::STREAM_CODEC,
    probabilistic::BLOOM_CODEC,
    probabilistic::HLL_CODEC,
    timeseries::TIMESERIES_CODEC,
    search::JSON_CODEC,
    probabilistic::CUCKOO_CODEC,
    probabilistic::TOPK_CODEC,
    probabilistic::TDIGEST_CODEC,
    probabilistic::CMS_CODEC,
    search::VECTORSET_CODEC,
];
```

Each per-type file declares its codec next to its encode/decode functions — locality the current
layout lacks. Example for the trickiest pair, the per-field-expiry hash
(`collections.rs`, today the `serialize_hash_with_field_expiry`/`deserialize_hash_with_field_expiry`
pair has no round-trip test at all):

```rust
pub(super) const HASH_FIELD_EXPIRY_CODEC: TypeCodec = TypeCodec {
    marker: TypeMarker::HashWithFieldExpiry,
    encode: |v| match v {
        Value::Hash(h) if h.has_field_expiries() => Some(serialize_hash_with_field_expiry(h).1),
        _ => None,
    },
    decode: |p| Ok(Value::Hash(deserialize_hash_with_field_expiry(p)?)),
    samples: || vec![sample_hash_with_field_expiry()], // forces marker 11 through the test
};
```

(The existing `serialize_*` functions keep returning `(u8, Vec<u8>)` internally so their byte layout
is untouched; the codec's `encode` simply drops the now-redundant marker byte, which the dispatcher
supplies from `codec.marker`.)

### Dispatch through the registry

```rust
fn serialize_value(value: &Value) -> (TypeMarker, Vec<u8>) {
    for codec in REGISTRY {
        if let Some(payload) = (codec.encode)(value) {
            return (codec.marker, payload);
        }
    }
    unreachable!("Value variant has no codec: {value:?}") // see compile-time backstop below
}

fn deserialize_value(type_byte: u8, payload: &[u8]) -> Result<Value, SerializationError> {
    let marker = TypeMarker::from_byte(type_byte)?;
    decode_for(marker)(payload)
}
```

`decode_for` is where compile-time enforcement lives — a `match marker { ... }` with **no wildcard**:

```rust
fn decode_for(marker: TypeMarker) -> fn(&[u8]) -> Result<Value, SerializationError> {
    match marker {
        TypeMarker::StringRaw => string::RAW_CODEC.decode,
        TypeMarker::StringInt => string::INT_CODEC.decode,
        // ... one arm per marker ...
        TypeMarker::VectorSet => search::VECTORSET_CODEC.decode,
    }
    // No `_ =>` arm: adding a TypeMarker variant without a decode here is a COMPILE ERROR.
}
```

This is the key move: `decode_for` matches the *enum*, so the compiler now demands an arm for every
marker — the exact guarantee `deserialize_value`'s old `u8` match could not give. The wildcard that
swallowed forgotten markers (`mod.rs:301`) is gone; unknown *bytes* still error, but unknown
*markers* cannot exist.

### Before / after: adding a hypothetical `GeoHash` type

Today (4 disconnected edits, one of them silently optional):

1. `const TYPE_GEOHASH: u8 = 17;` in `mod.rs`.
2. `Value::GeoHash(g) => serialize_geohash(g),` in `serialize_value`.
3. `TYPE_GEOHASH => Ok(Value::GeoHash(deserialize_geohash(payload)?)),` in `deserialize_value`
   — **omit this and it compiles**; the type round-trips in dev (encode works, you test encode), and
   breaks the first time a replica or restart reads it.
4. Write `serialize_geohash` / `deserialize_geohash`.

After (the marker, encode, decode, and sample are one declaration; the compiler and the test refuse
anything incomplete):

1. Add `GeoHash = 17` to `TypeMarker` and to `TypeMarker::ALL`.
2. Add the `GEOHASH_CODEC` (marker + encode + decode + sample) to its per-type file and to
   `REGISTRY`.
3. The `decode_for` match **fails to compile** until a `TypeMarker::GeoHash => ...` arm exists.
4. The registry round-trip test **fails** until `samples()` returns something that survives
   `decode(encode(..))`.

There is no way to ship an encode without a decode. The `Value` enum variant is still required, and
`serialize_value`'s fallthrough is covered by a compile-time backstop (below).

### Compile-time vs test-time enforcement

The two halves of symmetry get the two strongest enforcement levels available:

- **Decode existence — compile time.** `decode_for` matches the closed `TypeMarker` enum with no
  wildcard. A marker with no decode is a build failure, not a runtime `UnknownType`. This is strictly
  stronger than today and strictly stronger than a test (a test you can forget to run; the compiler
  you cannot).
- **Encode existence — compile time.** `serialize_value`'s `for` loop ends in `unreachable!`, which
  is weaker than a match. To keep the encode side compiler-enforced, pair it with the existing
  exhaustive `match value { ... }` shape: a thin `fn marker_of(value: &Value) -> TypeMarker` that
  matches every `Value` variant (no wildcard) and is asserted in a test to agree with the registry's
  `encode` results. Adding a `Value` variant then fails `marker_of` to build. (Alternatively, keep
  `serialize_value` as the exhaustive `match` it is today and treat the registry as decode-side +
  test-side only — a smaller change that still closes the silent-decode-loss hole, which is the whole
  point. Open question below.)
- **Round-trip correctness — test time.** Existence is necessary but not sufficient: the decode must
  actually invert the encode. That can only be checked by running bytes through both halves, so it is
  a test (below). The registry makes that test *total over markers* instead of a hand-picked subset.

### Why this is the right depth

- **Locality.** One marker's byte value, encode, decode, and sample become one declaration in one
  file. Today they are spread across `mod.rs` (constant + two dispatch arms) and a per-type module
  (two functions) — five sites, zero structural link. Future changes to a type's wire format are a
  one-codec edit.
- **Leverage.** The change is mostly mechanical (move existing functions behind a struct) but it
  converts the worst failure mode in the module — silent loss of persisted data from a forgotten
  decode arm — from "compiles, ships, loses data" into "does not compile." One enum + one match
  rewrite removes an entire class of latent bug for all 17 current and all future types.
- **Deletion test.** After the change, deleting a decode arm fails to compile (the `decode_for`
  match is non-exhaustive); deleting a codec's `samples` or breaking its decode fails the round-trip
  test. The defect that motivated this proposal — "remove one decode arm, everything still passes" —
  is no longer expressible.
- **No new adapter, no format change.** This is not a wrapper layer over serialization that callers
  may bypass; it *is* `serialize`/`deserialize`, restructured. The `#[repr(u8)]` discriminants are
  the existing byte values, so not a single persisted byte changes. The seam stays where it is; it
  just stops being hand-maintained.

## Migration plan

The wire format is on-disk **and** on the replication stream. Existing snapshots, WAL segments
(`wal/writer.rs` calls `serialize`), and replica full-syncs were written with markers 0-16 and the
exact payload layouts in the per-type files. **The bytes MUST stay identical.** The migration is a
pure internal restructure; every phase is verified to be byte-for-byte compatible.

1. **Phase 0 — introduce `TypeMarker`, keep behavior.** Add `serialization/marker.rs` with the
   `#[repr(u8)]` enum whose discriminants equal the current constants. Replace the 17 `const TYPE_*`
   with `TypeMarker` (the per-type `serialize_*` functions return `TypeMarker` instead of `u8`; the
   header writer at `mod.rs:122` casts `marker as u8`). `deserialize_value` becomes
   `TypeMarker::from_byte(type_byte)?` then the existing arms keyed on enum variants. No bytes change.
   Add a test asserting each `TypeMarker as u8` equals the historic constant (0..=16) so the wire
   pinning is itself tested. `just check frogdb-persistence && just test frogdb-persistence`.
2. **Phase 1 — make decode exhaustive.** Convert `deserialize_value` to dispatch through
   `decode_for(marker)` with the wildcard removed. This is the moment the silent-decode hole closes.
   Confirm the catch-all for unknown *bytes* still lives in `from_byte` (so `test_deserialize_unknown_type`,
   `mod.rs:451`, still passes).
3. **Phase 2 — collect codecs + registry.** Add `serialization/registry.rs` and a `*_CODEC` per type
   in its per-type file, wiring `encode`/`decode` to the existing functions. Route
   `serialize_value`/`deserialize_value` through `REGISTRY`/`decode_for`. The per-type byte-layout
   functions are unchanged; only the dispatch indirection is new.
4. **Phase 3 — the symmetry test (below).** Land the registry-iterating round-trip test. It will
   immediately exercise the 9 currently-untested markers; fix anything it surfaces (see Correctness
   flags) before the test can go green.
5. **Phase 4 — optional encode-side compile enforcement.** Add `marker_of(&Value)` (exhaustive
   match) and assert it agrees with the registry, so adding a `Value` variant without a codec fails
   to build. Decide per "open questions" whether this is worth the second match.
6. **Verification gate.** A golden-bytes test: check in a small fixture of pre-change serialized
   blobs (one per marker, produced by the current code) and assert the new code deserializes them and
   re-serializes to identical bytes. Plus the existing crash-recovery tests
   (`core/src/persistence/crash_recovery_tests.rs`) and `recovery.rs` paths act as the end-to-end
   "old snapshot still loads" check.

## Testing impact

- **One test enumerates the registry.** The headline addition:

  ```rust
  #[test]
  fn every_marker_round_trips() {
      for codec in REGISTRY {
          for value in (codec.samples)() {
              // 1. encode produces this codec's marker
              let (marker, payload) = serialize_value(&value);
              assert_eq!(marker, codec.marker, "{:?} encoded to wrong marker", codec.marker);
              // 2. decode inverts encode
              let back = (codec.decode)(&payload).expect("decode failed");
              assert_eq!(value.key_type(), back.key_type());
              // 3. full serialize/deserialize round-trip with the header
              let bytes = serialize(&value, &KeyMetadata::new(value.memory_size()));
              let (back2, _) = deserialize(&bytes).expect("deserialize failed");
              assert_eq!(value.key_type(), back2.key_type());
          }
      }
  }

  #[test]
  fn registry_covers_every_marker() {
      let covered: HashSet<u8> = REGISTRY.iter().map(|c| c.marker as u8).collect();
      for m in TypeMarker::ALL {
          assert!(covered.contains(&(*m as u8)), "no codec for {m:?}");
      }
  }
  ```

  A new type with a missing or broken decode is now a **test failure here**, not a production
  incident on the next replica sync. `registry_covers_every_marker` plus the non-exhaustive
  `decode_for` match means a marker cannot exist without both an encode sample and a decode.
- **Closes the 9-marker gap immediately.** Bloom, TimeSeries, JSON, HashWithFieldExpiry, Cuckoo,
  TopK, TDigest, CMS, and VectorSet get round-trip coverage the first time the registry test runs,
  because each codec must supply a `samples()`. `HashWithFieldExpiry` specifically gets its first-ever
  round-trip via a sample hash that sets a field expiry.
- **Wire-format pinning is tested, not assumed.** The Phase-0 discriminant test and the Phase-6
  golden-bytes fixture make "the bytes did not change" a CI assertion rather than a code-review hope.
- **Existing tests are a safety net, not replaced.** `proptest_serialization.rs` (random-bytes
  no-panic, truncation, corruption) stays as-is; it covers adversarial inputs the sample-based
  registry test does not. The two are complementary: proptest fuzzes the decoders, the registry test
  guarantees coverage breadth.

## Risks / open questions

- **Wire compatibility is non-negotiable.** Markers 0-16 and every payload layout must stay
  byte-identical because old snapshots, WAL segments, and replica streams already contain them. The
  `#[repr(u8)]` discriminants enforce the marker bytes; the per-type functions are moved, not
  rewritten, so payloads are unchanged. The golden-bytes fixture (Phase 6) is the guard. Any future
  format change is a *separate* versioned decision, out of scope here.
- **1-to-N markers (String, Hash).** Two markers map to one `Value` variant, decided by content
  (`serialize_string`, `string.rs:6`; `has_field_expiries()`, `mod.rs:202`). The `encode:
  fn(&Value) -> Option<Vec<u8>>` + registry-order design handles this (specific codec first, fallback
  second), but it means encode dispatch is an ordered scan, not a direct index. Order is load-bearing
  and must be asserted (a test that a hash-with-expiry never matches `HASH_CODEC` first). An
  alternative is an explicit `marker_of(&Value)` that encodes the selection logic in one match;
  decide which is clearer.
- **Legacy / version markers.** There is currently no format-version byte (the header `flags` byte at
  `mod.rs:124` is reserved and unused). The Stream encoder already does *in-payload* versioning: it
  appends `total_appended` + idempotency keys after the entries and the decoder detects old vs new
  format by remaining-byte length (`stream.rs:60-72`, `stream.rs:160-195`). The registry must not
  disturb this — `STREAM_CODEC` wraps the existing functions verbatim. If a real format version is
  ever needed, the reserved `flags` byte is the place, and the codec abstraction should grow a
  `decode(version, payload)` rather than minting new markers.
- **Asymmetric-by-design encodes.** Some encoders intentionally drop state, so decode cannot
  reproduce the in-memory value exactly: stream consumer groups are deliberately not persisted
  ("ephemeral state", `stream.rs:23`). The round-trip test must compare on what is *meant* to
  survive (entries, last-id, total_appended), not full structural equality — hence the test asserts
  `key_type()` and per-type semantic checks, not `Value == Value`. Each codec's sample/assertion has
  to encode that type's contract.
- **Encode-time compaction / normalization.** HyperLogLog has three encode branches (sparse / dense
  / empty-fallback) all under one marker with an internal encoding byte (`probabilistic.rs:152-186`);
  TimeSeries compresses chunks. The registry samples must cover each internal branch (the HLL codec's
  `samples()` should return a sparse, a dense, and an empty value), or the round-trip test gives false
  confidence by only exercising one branch.
- **Sub-encodings inside payloads (search / stream / vectorset).** JSON delegates to
  `JsonValue::parse`/`to_bytes` (`search.rs:9-17`); VectorSet has nested per-element length prefixes
  and a metric/quantization sub-enum (`search.rs:34-72`); these are their own little codecs. The
  top-level registry guarantees the *marker* round-trips but not that every nested branch is covered —
  those keep needing their own targeted tests; the registry test is a floor, not a ceiling.
- **Encode-side compile enforcement is optional.** Closing the *decode* hole (the silent-loss bug) is
  the must-have and needs only the enum + `decode_for`. The extra `marker_of(&Value)` exhaustive
  match (Phase 5) closes the much smaller "added a `Value` variant, forgot to serialize it" hole,
  which already fails to compile today via `serialize_value`'s match. If we keep `serialize_value` as
  a direct exhaustive match and use the registry only for decode + testing, we get most of the value
  with less indirection. Recommendation: do the enum + decode-side registry first (Phases 0-4), then
  evaluate whether the encode-side registry earns its keep.

## Correctness flags

- **`frogdb-server/crates/persistence/src/serialization/search.rs:117-134` — VectorSet encode
  accepts parameters that decode rejects (silent data loss on restart/replica).**
  `deserialize_vectorset` rejects `dim`/`original_dim` > 65536, `m` > 512, `ef_construction` > 4096.
  But there is no matching cap on the encode/creation side: `VADD ... M <m> EF <ef>` parses `m` and
  `ef` via unbounded `parse_usize` (`commands/src/vectorset/vadd.rs:135`, `:161`), defaults aside
  (`vadd.rs:212-213`), and `VectorSetValue::new_inner` passes them straight to usearch without a
  range check (`types/src/vectorset.rs:132-162`). A vector set created with e.g. `M 1000` or
  `EF 5000` serializes fine (`serialize_vectorset`, `search.rs:34`) but, on snapshot load / WAL
  replay / replica full-sync, `deserialize_vectorset` returns `InvalidPayload("VectorSet
  connectivity too large …")` and the key is dropped. The encode and decode disagree on the valid
  range — exactly the asymmetry this proposal targets, and currently untested (marker 16 has no
  round-trip test). Fix: enforce the same bounds at creation (reject oversized `M`/`EF`/`dim` in
  `VADD`/`new_inner`) so encode and decode share one definition of "valid", or relax the decode caps
  to match what creation allows. Confidence is high on the asymmetry; the realized impact depends on
  usearch accepting the oversized index at creation time (it applies no obvious upper clamp), so this
  should be reproduced with a `VADD key … M 1000` followed by a save/load before fixing.

- **`frogdb-server/crates/persistence/src/serialization/timeseries.rs:96-104` — TimeSeries decode
  silently coerces an unknown `DuplicatePolicy` byte to `Last` instead of erroring.** Encode writes
  0-5 (`timeseries.rs:33-40`); decode maps 0-5 and `_ => DuplicatePolicy::Last` (`:103`). For
  self-produced data this round-trips (only 0-5 are ever written), so it is not a loss bug today, but
  it is an asymmetry: a corrupted or future-version policy byte is accepted as `Last` rather than
  rejected, unlike every other enum decode in the module (e.g. VectorSet metric/quantization error on
  unknown, `search.rs:89`/`:100`). Lower severity; flagged for consistency. Recommend erroring with
  `InvalidPayload` to match the rest of the module.

- **Coverage gap (not a live bug, but the mechanism that hides them):
  `TYPE_HASH_WITH_FIELD_EXPIRY` (marker 11) is never round-trip tested.** It is a second marker for
  `Value::Hash`, reached only via `has_field_expiries()` (`mod.rs:202`); the sole hash round-trip
  test (`core/tests/proptest_serialization.rs:104`) never sets a field expiry, so the
  `serialize_hash_with_field_expiry`/`deserialize_hash_with_field_expiry` pair
  (`collections.rs:60`/`:247`) — including the `instant_to_unix_ms`/`unix_ms_to_instant` time
  conversion for per-field TTLs — has no encode↔decode test. Eight other markers (Bloom, TimeSeries,
  JSON, Cuckoo, TopK, TDigest, CMS, VectorSet) are in the same position. No decode arm is currently
  *missing* (cross-checked: all 17 markers appear in both `serialize_value`/per-type encoders and
  `deserialize_value`), but nothing prevents a future omission from going unnoticed. This is the gap
  the registry round-trip test closes.
</content>
</invoke>
