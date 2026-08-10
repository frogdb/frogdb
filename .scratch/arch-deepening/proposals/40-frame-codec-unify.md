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
(`server/src/connection/persistence_handler.rs:276-292`) — to recompute, off the source's
clock, a deadline the payload it is forwarding already carries. Three more sites re-declare
`HEADER_SIZE = 24` and two re-derive its field offsets, in tests and fuzz targets.
Separately, `dataset.rs`'s `take_chunk`
(`dataset.rs:84-106`) is a 23-line re-implementation of `FrameReader::read_u32_len_prefixed`
(`frame.rs:80-83`) with the same semantics and the same error variants.

The fix is one deep module: a `FrameHeader` value type in `frogdb-persistence` with an
`encode_into` / `decode` pair as the *only* code that knows the offsets, the endianness and
the negative-means-no-expiry sentinel — built on the existing `FrameReader` so the header
stops being the one part of the format still read by hand. `take_chunk` is deleted outright;
it fails the deletion test. MIGRATE's decoder is deleted too, not ported: `RESTORE`'s
`ttl = 0` already means "honour the expiry in the payload", so the whole computation is
redundant.

**This proposal claims no live bug.** An earlier draft argued the MIGRATE decoder mishandled
the no-expiry sentinel; review refuted it and the claim is gone. What remains is a pure
consolidation with two comment corrections — which is the honest case for it, and the case a
LOCKED crate deserves.

`frogdb-persistence` is **LOCKED** (gate 0.85). The wire format must stay byte-identical, the
error shapes must stay identical, and no existing test may be edited to accommodate the
refactor — all three are hard acceptance criteria below, with golden pins.

## Files involved (verified paths + current line counts)

All paths under `frogdb-server/crates/` unless noted. Line numbers verified against
`08c143d6`.

| File | Lines | Role in the fragmentation |
| --- | --- | --- |
| `persistence/src/serialization/mod.rs` | 732 | `HEADER_SIZE` decl (`:42`); the layout doc-comment (`:3-16`); `build_frame` — the **only** encoder (`:87-118`); `deserialize` — decoder #1, hand-sliced (`:123-168`) |
| `persistence/src/serialization/frame.rs` | 197 | `FrameReader` — the bounds-checked payload cursor every *payload* decoder uses, which the header decoders do not (`:20-89`) |
| `persistence/src/serialization/probabilistic.rs` | 1311 | `framed_payload` — decoder #2 (`:512-522`); `reframe_with_header` — decoder #3 **and** partial encoder #2 (`:528-544`); both on the RocksDB HLL merge path (`:570-624`) |
| `persistence/src/serialization/dataset.rs` | 192 | `take_chunk` (`:84-106`) duplicating `FrameReader::read_u32_len_prefixed`; called twice at `:71-72` |
| `server/src/connection/persistence_handler.rs` | 339 | MIGRATE: decoder #4, **outside the persistence crate**; a 17-line TTL recomputation that duplicates what the forwarded payload already says (`:276-292`) |
| `server/src/migrate.rs` | — | `MigrateClient::restore`'s doc-comment states the wrong `ttl` contract (`:325`) |
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
| `2..10` | `expires_at_ms` `i64` LE | **any negative value means "no expiry"** — `deserialize` tests `expires_ms < 0` (`mod.rs:154-158`), and `build_frame` writes `-1` (`mod.rs:97`); `0` is the epoch, a real passed deadline (FM-PERSISTENCE-036) |
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

**Decoder 4 — a different crate, decoding a field it does not need.**
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

**This block is redundant, not incorrect** — it was reviewed as a live bug and the claim was
refuted. The three facts that make it correct today:

1. **`ttl = 0` in `RESTORE` means "keep the payload's embedded expiry", not "no expiry."**
   `commands/persistence.rs:137-139` is explicit: `ttl_ms == 0` falls through with the
   `expires_at` that `deserialize` already put in `metadata`. Only `ttl_ms < 0` clears it
   (`:140-142`). So the `0` epoch, an already-passed positive deadline, and a short frame all
   land on the target with **the deadline the frame carries**, which is the right answer in
   every case.
2. **An expired key never reaches this loop.** MIGRATE's DUMP scatter goes through
   `serialize_key_for_transport`, whose fetch is `store.get_with_expiry_check`
   (`core/src/shard/execution.rs:397`); a key past its deadline reads as absent, the shard
   replies `Response::null()`, and the gather at `persistence_handler.rs:241` drops it. The
   behavior is documented at the call site (`execution.rs:388-391`).
3. **The `expiry_in_header` design says the header is the channel.** `execution.rs:843-846`:
   DUMP for MIGRATE "embeds the expiry in the frame header (the format RESTORE reads back),
   so it requests `expiry_in_header` and ignores the returned out-of-band TTL."

So the out-of-band `ttl` argument re-derives, off the *source's* wall clock, information the
payload already carries — and converts an absolute deadline into a relative one, re-anchoring
it to the *target's* clock. That is a skew window this path does not need to have. What is
actually wrong here is the **prose**: the `0 // No expiry` comment at `:288` and
`MigrateClient::restore`'s doc `The ttl is in milliseconds (0 = no expiry)`
(`server/src/migrate.rs:325`) both state the inverse of what `RESTORE` implements.

The fix is a deletion plus two comment corrections, not a behavior change: drop `:276-292`
entirely, pass `ttl = 0` unconditionally (removing the clock-skew conversion with it), and
make the two comments say *preserve the expiry embedded in the payload's frame header*.
Decoder #4 then stops existing rather than being rewritten — the strongest possible outcome
for a cross-crate format leak. There is no independently landable hotfix here, because there
is nothing broken to land ahead of the refactor.

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
| run truncated | `Truncated { expected: pos+4+len, actual: blob.len() }` | `Truncated { expected: pos+4+len, actual: data.len() }` — identical |
| `usize` overflow | `InvalidPayload("dataset blob offset overflow" / "dataset blob chunk length overflow")` | `InvalidPayload("length prefix overflows usize")` — same variant, different text |
| cursor on failure | never advanced | advanced by 4 in the run-truncated case — `read_le_u32` consumes the prefix through `take` before `take(len)` fails (`frame.rs:80-83`) |

The one asymmetry is the last row, and it is unobservable: both call sites propagate with
`?`, so the reader is dropped on the error path and `pos` is never read again. It is stated
here rather than glossed because "the cursor is untouched on failure" would otherwise read as
a preserved property, and it is not one.

Delete `take_chunk` and no complexity reappears at either call site — the replacement is
`FrameReader::new(blob)` plus `while reader.remaining() > 0`. It is a pass-through, and
having two adapters for one framing rule that vary in nothing is an accidental seam, not a
real one.

The four `FM-REPLICATION-003` tests in the same file (`dataset.rs:124,150,158,181`) survive
unchanged. `truncated_blob_is_an_error` (`:160-177`) asserts `actual == cut` and
`expected > actual` — both hold under `FrameReader` by the table above.

### Why it is shallow (architecture vocabulary)

- **`HEADER_SIZE` is a shallow seam.** The interface is one integer; the implementation —
  offsets, endianness, sentinel, split point — is entirely in the callers' heads. Four
  declarations of the number `24` across the repo (one real at `mod.rs:42`; three copies at
  `integration_dump_restore.rs:433`, `fuzz_targets/restore_payload.rs:37`,
  `fuzz_targets/deserialize.rs:9`) are the measurement — plus two sites that re-derive the
  *field offsets* by hand (`integration_dump_restore.rs:434-437`,
  `proptest_serialization.rs:288-292`), which is the part a single exported integer cannot
  help with. The two `pub use` re-exports and `proptest_serialization.rs`'s import of the
  real constant are the correct pattern, not part of the count.
- **`build_frame` is a writer with no reader.** Encoding is centralized; decoding is not.
  The interface is one-directional, so nothing forces the halves to agree: the compiler
  cannot see that `framed_payload`'s `[16..24]` names the field `build_frame` wrote there.
  Every new reader is a fresh opportunity to disagree, and the count only grows — one of
  them already lives in a different crate, where the persistence crate's own tests cannot
  reach it even in principle.
- **`FrameHeader` would be deep.** Behind a small interface (`encode_into` / `decode`, plus
  one prefix constructor for the merge path) sit the offsets, endianness, sentinel
  semantics, bounds checks and truncation error shapes. *Leverage*: four decode sites stop
  knowing offsets, and a fifth (MIGRATE) stops existing. *Locality*: an offset change
  becomes one edit with one golden test guarding it.
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
    pub expires_at_ms: i64,   // any negative value == no expiry
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

    /// `expires_at_ms` as an option, applying the sentinel — the one place that
    /// rule lives (FM-PERSISTENCE-036). **`< 0 → None`**, not `-1 → None`.
    pub fn expiry_deadline_ms(&self) -> Option<i64>;
}
```

**The sentinel is `< 0`, not `== -1`, and the difference is a panic.** `build_frame` only
ever *writes* `-1` (`mod.rs:97`), but `deserialize` *reads* `expires_ms < 0`
(`mod.rs:154-158`), and FM-PERSISTENCE-036's Invariant is stated over that read: "every
non-negative stamp — the epoch included — decodes back to a real `expires_at`." A
`-1`-only sentinel would send `-2` down the `Some` arm into `unix_ms_to_instant`, whose
`UNIX_EPOCH + Duration::from_millis(unix_ms as u64)` (`mod.rs:236`) sign-casts to ~5.8e11
years and panics on `SystemTime` overflow. That input is client-reachable: `RESTORE` hands
arbitrary attacker bytes to `deserialize` (`commands/persistence.rs:121-125`). So
`expiry_deadline_ms` must be exactly today's `< 0` test, and its boundary tests must include
`-2` and `i64::MIN` — the two values that a plausible-looking `== -1` implementation gets
wrong — alongside `-1`, `0`, `1` and `i64::MAX`.

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
   through the struct, and **keeping today's 16-byte acceptance**. It must not be rewired to
   the strict 24-byte `decode`: the existing test at `probabilistic.rs:1189-1192` asserts
   `reframe_with_header(&f[..16], ..).is_some()` ("sixteen bytes is exactly the metadata
   half, so it is enough"), and narrowing to 24 would require editing a test in a LOCKED
   crate to accommodate a refactor — exactly backwards. Give `FrameHeader` a second
   constructor for this one caller, `decode_metadata_prefix(&[u8]) -> Option<Self>`: it reads
   bytes `0..16` through the same `FrameReader` and *synthesizes* `payload_len`, which
   `reframe_with_header` overwrites on the next line anyway. Behavior is then preserved
   exactly — same acceptance boundary, same `None` at 15 bytes — while the offsets still live
   in one module. (`decode` and `decode_metadata_prefix` share one field-read sequence; only
   the length check and the `payload_len` read differ.)
5. **`take_chunk`** (`dataset.rs:84-106`) is deleted; `read_entries` uses
   `FrameReader::read_u32_len_prefixed` twice inside `while reader.remaining() > 0`.
6. **MIGRATE** (`persistence_handler.rs:276-292`) stops slicing — by stopping decoding at
   all. The 17 lines are deleted and `client.restore(key, 0, data, parsed.replace)` is called
   directly, because `ttl = 0` already means "honour the expiry in the payload"
   (`commands/persistence.rs:137-139`). Fix the two comments that say otherwise
   (`persistence_handler.rs:288`, `migrate.rs:325`). **No new public function is needed**:
   the crate seam is closed by removing the reader, not by exporting a decoder, so
   `frogdb-persistence`'s LOCKED public surface does not grow. This step touches only
   `server/` and is pure deletion plus comment repair.
7. **Optional follow-up, matching the codebase's chokepoint idiom.** A
   `lint-frame-header-seam` gate in the [seam-lints family](../../../agents/seam-lints.md):
   outside `serialization/header.rs`, no source file may contain a slice literal into the
   header region (`[2..10]`, `[16..24]`, `[..16]`) or a bare `24` bound to a
   `*HEADER_SIZE*` name. Tests and fuzz targets that deliberately corrupt headers stay
   allowlisted — they are *adversaries* of the format, not readers of it — but the three
   local `HEADER_SIZE = 24` copies should import the real constant. This is what stops the
   fan-out from regrowing; without it, decoder #5 arrives the same way #4 did.

The public surface of `frogdb-persistence` **does not change at all**: `HEADER_SIZE`,
`serialize`, `deserialize`, `SerializationError`, `merge_hll_serialized`,
`partial_merge_hll_deltas`, `serialize_hll_delta` all keep their signatures, nothing is
added, and `FrameHeader` is `pub(crate)`. A LOCKED crate growing no new exported API is a
property worth having: the whole change is internal, and the mutation re-gate is the only
external cost.

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
4. **Existing suites unmodified — including the acceptance boundaries.**
   `core/tests/proptest_serialization.rs` (1000-case round-trips + corruption sweeps), both
   fuzz targets, and `server/tests/integration_dump_restore.rs`'s corruption matrix pass
   without edits — they are the differential harness for this change. Called out
   specifically because it is the one a rewrite is most likely to "fix":
   `re_framing_keeps_the_metadata_and_rewrites_only_the_length`
   (`probabilistic.rs:1178-1197`) must pass **unedited**, both its `is_some()` at 16 bytes
   and its `is_none()` at 15. If any diff to this test appears in the PR, the refactor
   changed behavior and the review should stop there. Same rule for
   `dataset.rs`'s four `FM-REPLICATION-003` tests.
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
  two people typing `16..24`. MIGRATE's copy of `2..10` sits in another crate with no test
  of its own at all — because there is no seam at which "the decoders agree" is a statement
  you could write.
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
   function over `i64` — `< 0 → None`, `>= 0 → Some` — testable exhaustively at the
   boundaries (`i64::MIN`, `-2`, `-1`, `0`, `1`, `i64::MAX`) instead of only through a
   `Value` with a wall-clock deadline attached. `-2` and `i64::MIN` earn their place: they
   are the values that separate the real contract from the `== -1` misreading, and the
   misreading routes them into a `SystemTime` overflow panic (see the sentinel note above).
   FM-PERSISTENCE-036 already has an in-crate forcing test
   (`the_epoch_decodes_as_a_passed_deadline_not_as_never_expires`, `mod.rs:695`), so these
   deepen an existing pin rather than filling a hole — and if any of them takes the FM tag,
   the spec row's `Forced by` list is edited with it.
2. **"The decoders agree" becomes unwriteable-as-false.** There is one decoder, so
   agreement is structural rather than a test you have to remember to write.
3. **The proposed seam lint makes the invariant mechanical**, in the same family as
   `lint-clock-seam` and `lint-metrics-chokepoint`: the gate, not a reviewer, is what
   notices decoder #5.

## Risks / scope boundaries vs siblings

### Risk 1 (theoretical, kept as a forward-compat pin) — `flags` / `padding` normalization

On the RocksDB **merge-operator** path (`merge_hll_serialized:570`,
`partial_merge_hll_deltas:611`, which FM-PERSISTENCE-014 pins for replay idempotence)
`reframe_with_header` copies bytes `0..16` as an opaque blob.
Decoding into a struct and re-encoding would normalize any non-zero `flags` or `padding` to
zero. **Today that is provably a no-op**, and the proposal should not pretend otherwise:
`build_frame` is the only encoder in the tree, and it writes `flags = 0` (`mod.rs:94`) and
`padding = [0; 5]` (`mod.rs:104`) unconditionally — there is no code path that produces a
non-zero value in either field. Client-supplied bytes are not a counterexample: `RESTORE`
deserializes to a `Value` and stores *that* (`commands/persistence.rs:146`), so anything
persisted later is re-serialized through `build_frame`. So no byte on disk can differ.

The mitigation stays anyway, as a **forward-compatibility pin rather than a bug guard**:
`FrameHeader` carries `flags` and `padding` as real fields (not dropped as `_flags`, the way
`mod.rs:133` drops them today), and criterion 1's `encode(decode(bytes)) == bytes` proptest
locks the round-trip so that the *first* writer to start using those reserved bits does not
silently lose them on the merge path. That is cheap insurance on a reserved field. It is not
a reason to sequence the commits in a particular order or to treat this file as hazardous;
run the HLL merge tests (`probabilistic.rs:740-860`) as normal.

### Risk 2 — no acceptance-boundary change, by construction

An earlier draft narrowed `reframe_with_header`'s guard from `< 16` to `< 24` on the grounds
that the 16..23 window is unreachable (`header_src` is `operands.last()` or `base`; every
operand has passed `framed_payload` and `base` has passed `deserialize`, both requiring
`>= HEADER_SIZE`). The reachability argument holds, but the narrowing is still rejected: it
breaks `probabilistic.rs:1189-1192`, and editing a LOCKED crate's test to fit a refactor
inverts the spec-first rule. `decode_metadata_prefix` (step 4) keeps the boundary at 16
exactly, so this risk is designed out rather than argued away, and the PR carries no
behavioral delta to disclose.

### Risk 3 — short-input error shape

Today `deserialize` returns `Truncated { expected: HEADER_SIZE, actual }` for anything
under 24 bytes. A naive field-by-field `FrameReader` decode would instead report the *first
failing field's* expectation (e.g. `expected: 10`). Since that text reaches clients through
RESTORE, `FrameHeader::decode` must do the explicit `data.len() < SIZE` check **before** any
field read. Criterion 3 covers it.

### Risk 4 — MIGRATE's `ttl` argument is a policy surface, and this change must not move it

Deleting `persistence_handler.rs:276-292` is behavior-preserving **only** because `ttl = 0`
and the computed relative TTL express the same deadline (modulo the source/target clock skew
the deletion removes). If someone later wants MIGRATE to do something *different* with an
already-passed deadline — refuse the key, drop it from the batch, send an explicit `1`ms —
that is a policy decision, and it does not belong in this refactor.

Two notes on how such a change would be governed, since "locked crate" invites the wrong
reflex here:

- **It needs no persistence failure-mode row.** The persistence spec's Scope is the
  shard-to-storage path (`shard/persistence.rs`, `frogdb-persistence/src/wal/`,
  `snapshot/`, `checkpoint_quiesce.rs`) and its rows stop at "what a reader of storage can
  observe". MIGRATE's wire path is outside that boundary; FM-PERSISTENCE-036 governs the
  *decode*, which after this change MIGRATE no longer performs.
- **FM-PERSISTENCE-036 already has its in-crate forcing test.**
  `the_epoch_decodes_as_a_passed_deadline_not_as_never_expires` lives at
  `frogdb-server/crates/persistence/src/serialization/mod.rs:695` — inside the mutated
  crate, where `cargo mutants -p frogdb-persistence` can see it. The new
  `expiry_deadline_ms` boundary tests are therefore *additional* coverage, not a missing
  forcing test being supplied. If any of them is tagged `FM-PERSISTENCE-036`, the row's
  `Forced by` list must be edited in the same commit or `just lint-failure-modes` fails in
  both directions.

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
| **MIGRATE cleanup** | Delete `persistence_handler.rs:276-292`, pass `ttl = 0` to `client.restore`, correct the two comments (`:288`, `migrate.rs:325`). Touches `server/` only — no `persistence/` change, no new public API, no locked-crate re-gate. Regression test: a migrated key whose frame carries an already-passed deadline arrives on the target still expired (asserts the preserved semantics rather than the deleted arithmetic). | **S** — net −17 lines + test |
| **PE4** | Delete `take_chunk`, rewire `read_entries` onto `FrameReader`. Self-contained in `dataset.rs`; 4 FM-tagged tests unchanged. | **S** — net −20 lines |
| **PE3** | New `header.rs` (`decode`, `decode_metadata_prefix`, `encode_into`, `expiry_deadline_ms`) + golden/proptest suite; rewire `build_frame`, `deserialize`, `framed_payload`, `reframe_with_header`. | **M** — ~150 lines new (mostly tests), ~60 deleted |
| **Seam lint (optional follow-up)** | `lint-frame-header-seam` + wiring into `lint-gates`; import the real `HEADER_SIZE` into the three local copies. | **S** |
| **Re-gate** | `just mutants-diff frogdb-persistence`, then full `mutants` + `mutants-gate … 0.85`. Testbox-class workload. | — |

Ordering: MIGRATE cleanup → PE4 → PE3 → lint. The first step is a `server/`-only change that
is independent of the rest and can land in any order relative to them; PE4 and PE3 are both
inside `frogdb-persistence` and share one re-gate. Each step is green on its own; nothing
needs a flag day, and no step within PE3 needs special sequencing (see Risk 1).

## Out of scope — a separate suspected defect found while verifying this proposal

`RESTORE key 9223372036854775807 <payload>` looks client-triggerable into a panic:
`parse_i64` (`commands/persistence.rs:92`) accepts the full `i64` range, and the relative-TTL
branch computes `clock::now() + Duration::from_millis(ttl_ms as u64)`
(`commands/persistence.rs:134`) with no bound, which overflows the `Instant` addition. This is
the same *family* as the sentinel issue above — unvalidated `i64` from the wire reaching an
unchecked time addition — but it is a distinct site on a distinct path, and fixing it is an
input-validation change with client-visible error semantics, not a codec refactor. The
orchestrator has filed it separately. **Proposal 40 does not touch it**, and a reviewer
should not expect it to appear in this diff.
