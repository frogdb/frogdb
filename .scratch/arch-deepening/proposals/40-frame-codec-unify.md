# Proposal 40 — `FrameHeader` as the single 24-byte frame codec; delete `take_chunk`

Covers lane-persistence candidates **PE3** (FrameHeader writer-no-reader) and **PE4**
(`take_chunk` re-implements `FrameReader`). Both are one problem seen twice: the persisted
frame format has a centralized *encoder* and no centralized *decoder*, so every reader
re-derives the layout by hand — including a reader that re-derives a framing helper that
already exists eight files away.

## Summary

`build_frame` (`serialization/mod.rs:87`) is the only place the 24-byte persisted header is
written, but it is decoded **four** independent ways in non-test source — `deserialize`
hand-slices it (`mod.rs:132-148`), `framed_payload` re-reads only bytes `16..24`
(`probabilistic.rs:512-522`), `reframe_with_header` treats bytes `0..16` as an opaque
prefix it copies (`probabilistic.rs:528-544`), and the MIGRATE handler in a *different
crate* reads the expiry field out of `data[2..10]` behind a `data.len() >= 10` guard
(`server/src/connection/persistence_handler.rs:276-292`) — where it also gets the `-1`
no-expiry sentinel wrong. Four more sites re-declare `HEADER_SIZE = 24` or its field
offsets locally in tests and fuzz targets. Separately, `dataset.rs`'s `take_chunk`
(`dataset.rs:84-106`) is a 23-line re-implementation of `FrameReader::read_u32_len_prefixed`
(`frame.rs:80-83`) with the same semantics and the same error variants.

The fix is one deep module: a `FrameHeader` value type in `frogdb-persistence` with an
`encode_into` / `decode` pair as the *only* code that knows the offsets, the endianness and
the `-1` sentinel — built on the existing `FrameReader` so the header stops being the one
part of the format still read by hand. `take_chunk` is deleted outright; it fails the
deletion test.

`frogdb-persistence` is **LOCKED** (gate 0.85). The wire format must stay byte-identical and
the error shapes must stay identical — both are stated as hard acceptance criteria below,
with golden pins.

## Files involved (verified paths + current line counts)

All paths under `frogdb-server/crates/` unless noted. Line numbers verified against
`08c143d6`.

| File | Lines | Role in the fragmentation |
| --- | --- | --- |
| `persistence/src/serialization/mod.rs` | 732 | `HEADER_SIZE` decl (`:42`); the layout doc-comment (`:3-16`); `build_frame` — the **only** encoder (`:87-118`); `deserialize` — decoder #1, hand-sliced (`:123-168`) |
| `persistence/src/serialization/frame.rs` | 197 | `FrameReader` — the bounds-checked payload cursor every *payload* decoder uses, which the header decoders do not (`:20-89`) |
| `persistence/src/serialization/probabilistic.rs` | 1311 | `framed_payload` — decoder #2 (`:512-522`); `reframe_with_header` — decoder #3 **and** partial encoder #2 (`:528-544`); both on the RocksDB HLL merge path (`:570-624`) |
| `persistence/src/serialization/dataset.rs` | 192 | `take_chunk` (`:84-106`) duplicating `FrameReader::read_u32_len_prefixed`; called twice at `:71-72` |
| `server/src/connection/persistence_handler.rs` | 339 | MIGRATE: decoder #4, **outside the persistence crate**, wrong sentinel handling (`:276-292`) |
| `server/src/commands/persistence.rs` | — | RESTORE surfaces `SerializationError`'s `Display` text to the client (`:121-124`) — decode error *shape* is wire-observable |
| `persistence/src/lib.rs` | — | re-exports `HEADER_SIZE` (`:30`) |
| `core/src/lib.rs` | — | re-exports `HEADER_SIZE` again (`:117`) |
| `core/tests/proptest_serialization.rs` | 398 | builds a header by hand from `HEADER_SIZE` (`:288-292`) |
| `server/tests/integration_dump_restore.rs` | 814 | local `DUMP_HEADER_SIZE`/`TYPE_BYTE_OFFSET`/`LEN_FIELD_OFFSET` copies (`:433-437`) |
| `testing/fuzz/fuzz_targets/deserialize.rs` (repo root) | 39 | local `HEADER_SIZE = 24` (`:9`), hand-written header (`:22-33`) |
| `testing/fuzz/fuzz_targets/restore_payload.rs` (repo root) | 192 | local `HEADER_SIZE = 24` (`:37`), `constructed_frame` (`:118-125`) |

## Problem (verified evidence)

### The format, stated once, in a doc-comment nothing enforces

`mod.rs:3-6` documents the layout in prose:

```text
[type:u8][flags:u8][expires_at_ms:i64][lfu:u8][padding:5][payload_len:u64]
```

Written out as offsets that every reader must independently know:

| offset | field | notes |
| --- | --- | --- |
| `0` | type marker `u8` | `TypeMarker::from_byte` |
| `1` | flags `u8` | reserved; **read and discarded**, never validated |
| `2..10` | `expires_at_ms` `i64` LE | **`-1` means "no expiry"**; `0` is the epoch, a real passed deadline (FM-PERSISTENCE-036) |
| `10` | `lfu_counter` `u8` | |
| `11..16` | padding | 5 bytes, never validated |
| `16..24` | `payload_len` `u64` LE | |

Nothing in the type system connects the prose to the code. `HEADER_SIZE` is exported
(`mod.rs:42`, re-exported twice more) but it publishes exactly one fact — the number `24`.
A caller still has to know the field order, the offsets, the endianness, the sentinel, and
(for `reframe_with_header`) that the header splits at byte 16. That is a constant wearing
an interface's clothes.

### PE3 — one writer, four readers, none of them the same code

**Encoder (1).** `build_frame` (`mod.rs:87-118`) pushes the fields in order and
`debug_assert_eq!`s the total length.

**Partial encoder (2).** `reframe_with_header` (`probabilistic.rs:528-544`) does *not* call
`build_frame`. It copies `header_src[..16]` verbatim and writes a fresh `payload_len` at
`16..24`, with a comment restating the layout:

```rust
// Bytes 0..16 carry marker(1) + flags(1) + expires(8) + lfu(1) + pad(5); only
// the trailing payload length (16..24) changes.
if header_src.len() < 16 {
```

Note the guard is `< 16`, not `< HEADER_SIZE`.

**Decoder 1** — `deserialize` (`mod.rs:132-148`), hand-sliced, in a module whose sibling
`FrameReader` exists precisely to stop this pattern:

```rust
let type_byte = data[0];
let _flags = data[1];
let expires_ms = i64::from_le_bytes(data[2..10].try_into().unwrap());
let lfu_counter = data[10];
// Skip padding (5 bytes)
let payload_len = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
```

`frame.rs:1-11` opens by describing this exact anti-pattern ("`payload[o..o + N].try_into().unwrap()`
guarded by an open-coded bounds check … where one wrong `+= N` silently shifts every field
after it") — and then the header, the one field group in the whole format with fixed
offsets, is the part that never got converted.

**Decoder 2** — `framed_payload` (`probabilistic.rs:512-522`) reads only `frame[16..24]`,
returns `Option` rather than `Result`, and knows nothing about the marker or expiry.

**Decoder 3** — `reframe_with_header`'s `header_src[..16]` copy is a decode by omission: it
asserts the shape of bytes `0..16` without parsing them.

**Decoder 4 — a different crate, and it is wrong.**
`server/src/connection/persistence_handler.rs:276-292`, on the MIGRATE path:

```rust
// Extract TTL from serialized data (stored in header bytes 2-10 as i64 milliseconds)
let ttl = if data.len() >= 10 {
    let expires_ms = i64::from_le_bytes(data[2..10].try_into().unwrap_or([0; 8]));
    if expires_ms > 0 {
        // ... remaining = expires_ms - now_ms; if remaining > 0 { remaining } else { 0 }
    } else {
        0 // No expiry
    }
} else { 0 };
```

Three separate defects, all downstream of the format being re-derived rather than reused:

1. **The `0` epoch is decoded as "no expiry."** `FM-PERSISTENCE-036` and its forcing test
   `the_epoch_decodes_as_a_passed_deadline_not_as_never_expires` (`mod.rs:688-714`) pin the
   opposite for `deserialize`: the no-expiry sentinel is `-1`, so `0` is a deadline that has
   already passed. `instant_to_unix_ms` (`mod.rs:222`) returns `0` for any deadline too old
   to represent. MIGRATE's decoder maps that to `ttl = 0`, and `RESTORE` with `ttl = 0` means
   *no expiry* — the key lands on the target immortal.
2. **An already-passed positive deadline lands immortal too.** `remaining <= 0 → ttl = 0`
   → the same "no expiry" on the target. Same failure mode as FM-PERSISTENCE-036, on the
   wire path instead of the disk path.
3. **The bounds guard is `>= 10`, not `>= HEADER_SIZE`**, and the failure mode of the
   `try_into` is `unwrap_or([0; 8])` — which lands on the *same* wrong default as (1).

The exposure is narrow in practice (the payload comes from a local DUMP, and lazy expiry
usually removes a dead key before DUMP sees it) but the decode is unambiguously wrong
against the row the persistence codec is locked to. Treat (1) and (3) as a decode bug — the
hotfix below. Treat (2)'s *remedy* as a spec question (see Risks).

### PE4 — `take_chunk` fails the deletion test

`dataset.rs:84-106`:

```rust
fn take_chunk<'a>(blob: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SerializationError> {
    let header_end = pos.checked_add(4).ok_or_else(...)?;
    if header_end > blob.len() { return Err(Truncated { expected: header_end, actual: blob.len() }); }
    let len = u32::from_le_bytes(blob[*pos..header_end].try_into().unwrap()) as usize;
    let end = header_end.checked_add(len).ok_or_else(...)?;
    if end > blob.len() { return Err(Truncated { expected: end, actual: blob.len() }); }
    *pos = end;
    Ok(&blob[header_end..end])
}
```

`frame.rs:80-83`:

```rust
pub(crate) fn read_u32_len_prefixed(&mut self) -> Result<&'a [u8], SerializationError> {
    let len = self.read_le_u32()? as usize;
    self.take(len)
}
```

They are the same function. Behavioral comparison, field by field:

| case | `take_chunk` | `FrameReader::read_u32_len_prefixed` |
| --- | --- | --- |
| prefix truncated | `Truncated { expected: pos+4, actual: blob.len() }` | `Truncated { expected: pos+4, actual: data.len() }` — identical |
| run truncated | `Truncated { expected: header_end+len, actual: blob.len() }` | `Truncated { expected: pos+len, actual: data.len() }` — identical |
| `usize` overflow | `InvalidPayload("dataset blob offset overflow" / "dataset blob chunk length overflow")` | `InvalidPayload("length prefix overflows usize")` — same variant, different text |
| cursor on failure | not advanced | not advanced |

Delete `take_chunk` and no complexity reappears at either call site — the replacement is
`FrameReader::new(blob)` plus `while reader.remaining() > 0`. It is a pass-through, and
having two adapters for one framing rule that vary in nothing is an accidental seam, not a
real one.

The four `FM-REPLICATION-003` tests in the same file (`dataset.rs:124,150,158,181`) survive
unchanged. `truncated_blob_is_an_error` (`:160-177`) asserts `actual == cut` and
`expected > actual` — both hold under `FrameReader` by the table above.

### Why it is shallow (architecture vocabulary)

- **`HEADER_SIZE` is a shallow seam.** The interface is one integer; the implementation —
  offsets, endianness, sentinel, split point — is entirely in the callers' heads. Six
  declarations of the number `24` across the repo (one real, five copies) are the
  measurement.
- **`build_frame` is a writer with no reader.** Encoding is centralized; decoding is not.
  The interface is one-directional, so nothing forces the halves to agree: the compiler
  cannot see that `framed_payload`'s `[16..24]` names the field `build_frame` wrote there.
  Every new reader is a fresh opportunity to disagree — and one already does, in a
  different crate.
- **`FrameHeader` would be deep.** Behind a two-call interface (`encode_into` / `decode`)
  sit the offsets, endianness, sentinel semantics, bounds checks and truncation error
  shapes. *Leverage*: four decode sites stop knowing offsets, and a fifth (MIGRATE) can
  stop reaching across a crate boundary for them. *Locality*: an offset change becomes one
  edit with one golden test guarding it.
- **`take_chunk` fails the deletion test**, as shown above.
- **Naming note worth fixing while here.** `serialization/frame.rs`'s `FrameReader` reads
  *payloads*, not frames — the header is the one thing it never reads, despite the name.
  Building `FrameHeader::decode` on top of it makes the name true.

## Proposed change

**One new private module, `serialization/header.rs`**, owning the format:

```rust
/// Every fact about the 24-byte persisted frame header, in one place.
pub(crate) struct FrameHeader {
    pub marker_byte: u8,
    pub flags: u8,
    pub expires_at_ms: i64,   // -1 == no expiry
    pub lfu_counter: u8,
    pub padding: [u8; 5],
    pub payload_len: u64,
}

impl FrameHeader {
    pub const SIZE: usize = 24;

    /// Decode the header prefix of `data`. Never panics; never validates `flags`
    /// or `padding` (a future writer's bits must survive a read).
    pub fn decode(data: &[u8]) -> Result<Self, SerializationError>;

    /// Append the header to `out`. Exactly `SIZE` bytes, always.
    pub fn encode_into(&self, out: &mut Vec<u8>);

    /// `expires_at_ms` as an option, applying the `-1` sentinel — the one place
    /// that rule lives (FM-PERSISTENCE-036).
    pub fn expiry_deadline_ms(&self) -> Option<i64>;
}
```

`decode` performs one up-front `data.len() < SIZE` check (preserving today's
`Truncated { expected: HEADER_SIZE, actual }` shape for short input), then reads fields
through a `FrameReader` so the offsets are expressed as a read *sequence*, not as literals.

Call-site rewiring, in landing order:

1. **`build_frame`** (`mod.rs:87`) constructs a `FrameHeader` and calls `encode_into`, then
   appends the payload. `HEADER_SIZE` becomes `pub const HEADER_SIZE: usize = FrameHeader::SIZE;`
   so both public re-exports (`persistence/src/lib.rs:30`, `core/src/lib.rs:117`) keep
   working unchanged.
2. **`deserialize`** (`mod.rs:123`) calls `FrameHeader::decode`, then takes the payload via
   the same `FrameReader` the header was read from. The hand-slices at `:132-148` go away.
   Expiry goes through `expiry_deadline_ms()`.
3. **`framed_payload`** (`probabilistic.rs:512`) becomes `FrameHeader::decode(frame).ok()`
   plus a bounds check, keeping its `Option` return (its callers are RocksDB merge
   operators; `None` is "merge failure").
4. **`reframe_with_header`** (`probabilistic.rs:528`) decodes `header_src`, replaces
   `payload_len`, and re-encodes — **preserving `flags` and `padding` byte-for-byte**
   through the struct (see Risks: this is the subtlest constraint in the proposal).
5. **`take_chunk`** (`dataset.rs:84-106`) is deleted; `read_entries` uses
   `FrameReader::read_u32_len_prefixed` twice inside `while reader.remaining() > 0`.
6. **MIGRATE** (`persistence_handler.rs:276-292`) stops slicing. It calls
   `frogdb_persistence::frame_expiry_ms(data)` — a thin `pub fn` wrapping
   `FrameHeader::decode(..).expiry_deadline_ms()` — so the format stops leaking across the
   crate seam. (Landable ahead of the rest; see Effort.)
7. **Optional follow-up, matching the codebase's chokepoint idiom.** A
   `lint-frame-header-seam` gate in the [seam-lints family](../../../agents/seam-lints.md):
   outside `serialization/header.rs`, no source file may contain a slice literal into the
   header region (`[2..10]`, `[16..24]`, `[..16]`) or a bare `24` bound to a
   `*HEADER_SIZE*` name. Tests and fuzz targets that deliberately corrupt headers stay
   allowlisted — they are *adversaries* of the format, not readers of it — but the four
   local `HEADER_SIZE = 24` copies should import the real constant. This is what stops the
   fan-out from regrowing; without it, decoder #5 arrives the same way #4 did.

The public surface of `frogdb-persistence` does not change: `HEADER_SIZE`, `serialize`,
`deserialize`, `SerializationError`, `merge_hll_serialized`, `partial_merge_hll_deltas`,
`serialize_hll_delta` all keep their signatures. `FrameHeader` itself is `pub(crate)`; only
the `frame_expiry_ms` helper for step 6 is added to the public surface.

### Hard acceptance criteria (LOCKED crate)

1. **Byte-identical wire.** A new golden test in `serialization/header.rs` asserts a
   hardcoded 24-byte array against `encode_into` for a fixed
   `(marker, flags, expires_ms, lfu, padding, payload_len)` tuple — the literal bytes are in
   the test, so a format change is a test diff and never a silent break. Plus a proptest:
   `decode(encode(h)) == h` over arbitrary field values, and
   `encode(decode(bytes)) == bytes[..24]` over arbitrary 24-byte inputs (the *round-trip
   in both directions*, which is what pins padding and flags preservation).
2. **Offsets pinned independently of the implementation.** A test asserts each field's
   position by slicing a freshly built frame with literal offsets (`[0]`, `[1]`, `[2..10]`,
   `[10]`, `[16..24]`). This is the test that fails if someone reorders `encode_into`, and
   it must not be written in terms of `FrameHeader`'s own constants.
3. **Error shapes preserved.** `SerializationError` variants *and their `Display` text* are
   client-visible through RESTORE (`server/src/commands/persistence.rs:121-124`), so the
   existing assertions in `deserialize_refuses_a_payload_len_past_the_end_of_the_buffer`
   (`mod.rs:719-731`, exact `expected`/`actual`) and `test_deserialize_truncated`
   (`mod.rs:534-539`) must pass unmodified.
4. **Existing suites unmodified.** `core/tests/proptest_serialization.rs` (1000-case
   round-trips + corruption sweeps), both fuzz targets, and
   `server/tests/integration_dump_restore.rs`'s corruption matrix pass without edits — they
   are the differential harness for this change.
5. **FM tags intact.** `just lint-failure-modes` green; `FM-PERSISTENCE-036` (`mod.rs:688`)
   and the four `FM-REPLICATION-003` tests (`dataset.rs`) keep their tags and their bodies.
6. **Mutation re-gate.** `just mutants-diff frogdb-persistence` before pushing; full
   `just mutants frogdb-persistence` + `just mutants-gate frogdb-persistence 0.85`.
   Expect the score to *improve*: five hand-rolled arithmetic sites collapse into one whose
   every field is directly asserted by criteria 1-2, which is exactly the shape mutants
   scores well.

## Testability improvement

Today the frame header has **no test surface of its own**. It can only be exercised
transitively, through `serialize`/`deserialize` of a whole `Value`, which means:

- **Fields are tested by proxy, or not at all.** `flags` and `padding` have *zero*
  assertions anywhere in the repo — nothing checks they survive a read, and
  `reframe_with_header` silently depends on that. The only direct header assertion in the
  tree is a single `i64::from_le_bytes(data[2..10])` inside FM-PERSISTENCE-036's body
  (`mod.rs:697`).
- **The four decoders cannot be compared.** There is no test that `framed_payload` and
  `deserialize` agree about where `payload_len` lives; they agree today by coincidence of
  two people typing `16..24`. MIGRATE's decoder disagrees, and no test caught it — because
  there is no seam at which "the decoders agree" is a statement you could write.
- **Header-shaped property tests are impossible to express.** `proptest_serialization.rs`
  generates *values* and round-trips them, so the header space it explores is whatever
  `KeyMetadata` happens to produce. `expires_at_ms = i64::MIN`, `payload_len = u64::MAX`,
  non-zero `flags`, non-zero `padding` are unreachable from that generator.

After the change the codec is a **pure, allocation-free, `Value`-free function pair over a
6-field struct** — the ideal proptest target:

```rust
proptest! {
    #[test]
    fn header_round_trips(marker: u8, flags: u8, expires: i64, lfu: u8,
                          padding: [u8; 5], payload_len: u64) {
        let h = FrameHeader { marker_byte: marker, flags, expires_at_ms: expires,
                              lfu_counter: lfu, padding, payload_len };
        let mut buf = Vec::new();
        h.encode_into(&mut buf);
        prop_assert_eq!(buf.len(), FrameHeader::SIZE);
        prop_assert_eq!(FrameHeader::decode(&buf).unwrap(), h);
    }

    /// The direction that pins flags/padding preservation — what the HLL
    /// merge path's re-framing actually depends on.
    #[test]
    fn arbitrary_header_bytes_survive_a_decode_encode(bytes: [u8; 24]) {
        let mut out = Vec::new();
        FrameHeader::decode(&bytes).unwrap().encode_into(&mut out);
        prop_assert_eq!(&out[..], &bytes[..]);
    }

    /// Never panics, whatever the length.
    #[test]
    fn decode_never_panics(data: Vec<u8>) { let _ = FrameHeader::decode(&data); }
}
```

Three concrete gains beyond coverage:

1. **The sentinel becomes assertable in one line.** `expiry_deadline_ms()` is a total
   function over `i64` — `-1 → None`, everything else `→ Some` — testable exhaustively at
   the boundaries (`-1`, `0`, `1`, `i64::MIN`, `i64::MAX`) instead of only through a
   `Value` with a wall-clock deadline attached. FM-PERSISTENCE-036 gains a *unit-level*
   forcing test in the mutated crate, which (per `CLAUDE.md`) is where a forcing test has
   to live to count toward `frogdb-persistence`'s score.
2. **"The decoders agree" becomes unwriteable-as-false.** There is one decoder, so
   agreement is structural rather than a test you have to remember to write.
3. **The proposed seam lint makes the invariant mechanical**, in the same family as
   `lint-clock-seam` and `lint-metrics-chokepoint`: the gate, not a reviewer, is what
   notices decoder #5.

## Risks / scope boundaries vs siblings

### Risk 1 (highest) — `reframe_with_header` must preserve `flags` and `padding` verbatim

This is on the RocksDB **merge-operator** path (`merge_hll_serialized:570`,
`partial_merge_hll_deltas:611`), which FM-PERSISTENCE-014 pins for replay idempotence. Today
those bytes are copied as an opaque `header_src[..16]` blob. If the rewrite decodes into a
struct and re-encodes, any non-zero `flags` or `padding` written by a future or foreign
writer would be **normalized to zero** — a silent byte change on already-persisted data.
Mitigation: `FrameHeader` carries `flags` and `padding` as real fields (not dropped as
`_flags` the way `mod.rs:133` drops them today), and acceptance criterion 1's
`encode(decode(bytes)) == bytes` proptest is precisely the pin for it. Land this file last
and run the HLL merge tests (`probabilistic.rs:740-860`) before and after.

### Risk 2 — `reframe_with_header`'s guard narrows from `< 16` to `< 24`

A strict `FrameHeader::decode` requires 24 bytes; the current guard accepts 16. Reachability
audit: `header_src` is `operands.last()` or `base`; every operand has already passed
`framed_payload` (which requires `>= HEADER_SIZE`), and `base` has already passed
`deserialize` (same requirement). So no reachable input is in the 16..23 window. Still a
narrowing on an unreachable path — pin it with a direct unit test asserting `None` for a
20-byte `header_src`, and state the narrowing in the PR body rather than letting a mutant
find it.

### Risk 3 — short-input error shape

Today `deserialize` returns `Truncated { expected: HEADER_SIZE, actual }` for anything
under 24 bytes. A naive field-by-field `FrameReader` decode would instead report the *first
failing field's* expectation (e.g. `expected: 10`). Since that text reaches clients through
RESTORE, `FrameHeader::decode` must do the explicit `data.len() < SIZE` check **before** any
field read. Criterion 3 covers it.

### Risk 4 — MIGRATE policy vs MIGRATE decode

Defects (1) and (3) of decoder #4 are decode bugs with one right answer and belong in the
hotfix. Defect (2) — *what TTL should MIGRATE send for a deadline that has already passed?* —
is a **policy** question (send `1`ms? refuse the key? drop it from the batch?) that is
client-visible and touches the FM-PERSISTENCE-036 neighbourhood. Do not decide it inside a
refactor: file it spec-first (failure-mode row → failing test → fix) per the locked-area
rule. The hotfix should make the decode correct and leave the policy where it is, or the
change stops being reviewable as a no-op.

### Boundary vs proposal 38 (SnapshotLayout / SnapshotFs)

No overlap. 38 is about *file and directory naming* under the snapshot root
(`snapshot_{epoch}`, `latest`) and the read half of `SnapshotFs`. 40 never opens a file, and
`serialization/` has no filesystem dependency at all.

### Boundary vs proposal 42 (RocksSink)

No overlap in code. 42 owns `rocks/flush.rs` — what `commit` does with a staged batch and
what effects it returns. 40 owns the bytes *inside* the values that batch carries. The one
adjacency: both touch the HLL merge feature. 42's territory is the sink's commit path;
40's is `probabilistic.rs`'s `framed_payload` / `reframe_with_header`. They meet at the
merge-operator registration and neither changes it. **Sequence 42 after 40** if both land in
the same round — 40 is the smaller diff and 42 is the higher-blast-radius one.

### Boundary vs proposal 89 (bloom/cuckoo CHUNK codec) — different wire format, stated explicitly

Both proposals name `probabilistic.rs`, so draw the line at function granularity:

| function | owner |
| --- | --- |
| `build_frame`, `deserialize`'s header parse (`mod.rs`) | **40** |
| `framed_payload` (`probabilistic.rs:512`) | **40** |
| `reframe_with_header` (`probabilistic.rs:528`) | **40** |
| `take_chunk` (`dataset.rs:84`) | **40** |
| `serialize_bloom_filter` / `deserialize_bloom_filter` (`probabilistic.rs:25` / `:264`) | **89** |
| `serialize_cuckoo_filter` / `deserialize_cuckoo_filter` (`probabilistic.rs:84` / `:308`) | **89** |
| `BfScandump`/`BfLoadchunk` chunk bytes (`commands/src/bloom.rs:517-564`, `:570+`) | **89** |
| `CfScandump`/`CfLoadchunk` chunk bytes (`commands/src/cuckoo.rs:552+`, `:631+`) | **89** |

These are two genuinely different wire formats. 40 owns the **frame envelope** — the fixed
24-byte header that wraps *every* persisted value regardless of type — and never reads a
payload byte. 89 owns the **SCANDUMP/LOADCHUNK payload format** for two specific filter
types, which today is hand-rolled inside the command handlers
(`commands/src/bloom.rs:532-548` builds `error_rate`/`expansion`/`non_scaling`/`num_layers`
plus per-layer runs by hand) and is *not* framed by a `FrameHeader` at all — it travels as a
bare RESP bulk string. Verified: no `chunk`/`SCANDUMP` symbol exists in
`frogdb-server/crates/types/src/bloom.rs` or `cuckoo.rs`; the codec lives in the commands
crate. The two changes can land in either order and touch disjoint functions. If 89
proposes routing the chunk codec through `probabilistic.rs`'s payload serializers, that is
still disjoint from 40 — 40 does not touch payload codecs.

## Effort

**S–M overall**, split into three independently reviewable commits. The mutation re-gate,
not the code, is the long pole.

| Step | Scope | Size |
| --- | --- | --- |
| **Hotfix (independently landable, first)** | MIGRATE decode: replace `persistence_handler.rs:276-292`'s hand-slice with `frame_expiry_ms(data)` honouring the `-1` sentinel and the full `HEADER_SIZE` bound; regression test that a frame with `expires_at_ms = 0` is not migrated as "no expiry". Touches `server/` + one `pub fn` in `persistence/`; needs **no** structural change to `FrameHeader` (the helper can wrap today's `deserialize`). Ships the correctness fix without waiting on the locked-crate re-gate. | **S** — ~30 lines + test |
| **PE4** | Delete `take_chunk`, rewire `read_entries` onto `FrameReader`. Self-contained in `dataset.rs`; 4 FM-tagged tests unchanged. | **S** — net −20 lines |
| **PE3** | New `header.rs` + golden/proptest suite; rewire `build_frame`, `deserialize`, `framed_payload`, `reframe_with_header`. | **M** — ~150 lines new (mostly tests), ~60 deleted |
| **Seam lint (optional follow-up)** | `lint-frame-header-seam` + wiring into `lint-gates`; import the real `HEADER_SIZE` into the four local copies. | **S** |
| **Re-gate** | `just mutants-diff frogdb-persistence`, then full `mutants` + `mutants-gate … 0.85`. Testbox-class workload. | — |

Ordering: hotfix → PE4 → PE3 (`reframe_with_header` last within PE3, per Risk 1) → lint.
Each step is green on its own; nothing needs a flag day.
