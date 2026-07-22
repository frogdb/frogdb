# Proposal 27 — A `ReplconfCodec` seam for the REPLCONF ACK/GETACK control grammar

## Summary

The REPLCONF ACK/GETACK control-message wire grammar — the offset-solicitation half of the
replication protocol — has **no single owner**. It is realized across four sites in two files with
**three different mechanisms**: the GETACK solicitation is encoded through the shared
`serialize_command_to_resp` helper (`primary/mod.rs`), the replica's ACK is encoded by a
hand-written `format!` literal (`replica/streaming.rs`), the primary parses the ACK with a real
`redis_protocol` RESP2 decoder (`primary/mod.rs`), and the replica recognizes the GETACK with a
bespoke structural `strip_prefix` walk (`replica/streaming.rs`). The two producer/parser pairs are
kept consistent by hand across two files; change one side's byte shape and the ends silently stop
understanding each other, with **no golden round-trip test** binding an encoder to its decoder on the
ACK side at all. This proposal introduces a symmetric `ReplconfCodec` — modeled directly on the
existing `CheckpointStreamCodec` (round 7 P56) — that owns `encode_ack` / `parse_ack` /
`encode_getack` / `is_getack`, giving the grammar one home and one round-trip test suite.

**Verification note (see Evidence):** the candidate's framing of "hand-realized in four places"
overstates the current state. Only **two** of the four sites hand-realize raw byte shapes
(`send_ack`'s `format!` literal and `is_getack_frame`'s structural parse); the other two already
lean on shared machinery (`serialize_command_to_resp` and `redis_protocol::resp2::decode`). The
scatter and the drift seam are real — four sites, three mechanisms, no owner, and an untested ACK
round-trip — but the sharpest justification is narrower than "four hand-rolled copies."

## Problem

The replication control protocol has two solicitation messages: the primary broadcasts
`REPLCONF GETACK *` into the offset-stamped command stream (its ack-solicitation, sent by WAIT),
and the replica answers with `REPLCONF ACK <offset>`. This is one small grammar with two directions.
It is currently expressed four times, no two the same way, and split across the Primary and Replica
modules:

1. **GETACK encode** (primary) — `serialize_command_to_resp("REPLCONF", [GETACK, *])`, the shared
   serializer.
2. **ACK decode** (primary) — `redis_protocol::resp2::decode` + token comparison.
3. **ACK encode** (replica) — a hand-written `format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", …)`
   literal, the **only** REPLCONF in the crate not routed through `serialize_command_to_resp`.
4. **GETACK decode** (replica) — a bespoke structural `strip_prefix(b"*3\r\n$8\r\n")` /
   `strip_prefix(b"\r\n$6\r\n")` walk, deliberately avoiding a full RESP decode for hot-path speed.

The couplings that must stay consistent are the two **producer/parser pairs across files**:

- **ACK pair:** replica `send_ack` (encode, `streaming.rs`) → primary `parse_replconf_ack`
  (decode, `primary/mod.rs`). The encoder is a hand-rolled literal; if its byte shape drifts (a
  spacing change, a different arg order), the decoder — though it uses a real RESP2 decoder that
  tolerates some variation — still depends on `parts[0]==REPLCONF`, `parts[1]==ACK`,
  `parts[2]==offset`. **No test feeds `send_ack`'s output to `parse_replconf_ack`** (`send_ack`
  needs a live stream; `parse_replconf_ack` is tested only against hand-written literals), so this
  producer/parser link is entirely unpinned.
- **GETACK pair:** primary `request_acks` (encode) → replica `is_getack_frame` (decode). The
  encoder uses the generic serializer; the decoder hardcodes `*3\r\n$8\r\n…$6\r\nGETACK`. If
  `serialize_command_to_resp`'s framing ever changed, `is_getack_frame` would silently stop
  matching. This pair *is* partially pinned — `recognizes_the_primary_getack_wire_form`
  (`streaming.rs`) feeds the serializer's output to `is_getack_frame` — but the pin lives on the
  replica side only, disconnected from the primary encoder.

The grammar is a genuine **seam** between the Primary and Replica modules, but it has no explicit
interface object: it exists only as an informal agreement replicated across four call sites. This is
exactly the shape the round-7 `CheckpointStreamCodec` was extracted to cure for the full-sync
envelope — whose own doc comment records that its grammar "was realized three times by hand" before
the codec collapsed it to one definition.

### Why it is shallow/fragmented (architecture vocabulary)

- **No owning module; poor locality.** The ACK/GETACK grammar is a coherent unit of knowledge
  smeared across two files and three mechanisms. To answer "what does a REPLCONF ACK look like on
  the wire, and who agrees on it?" you must read a `format!` literal in `streaming.rs`, a
  `redis_protocol` decode in `primary/mod.rs`, a serializer call in `primary/mod.rs`, and a
  `strip_prefix` walk in `streaming.rs`, then hold all four in your head to confirm they match.
- **A leaky seam with no adapter.** The Primary↔Replica control-message boundary is real, but there
  is no object that *is* the boundary. Each side re-derives its half. A codec is the missing
  **adapter**: one deep module whose small interface (four functions) hides the byte-shape
  implementation and makes the producer/parser contract a compile-time surface rather than a
  hand-maintained convention.
- **Inconsistent leverage.** `send_ack` reinvents what `serialize_command_to_resp` already does for
  every other REPLCONF (handshake `listening-port` / `capa` / `frogdb-version`, and GETACK itself).
  That one hand-rolled encoder is pure duplication with negative leverage — more bytes, more drift
  surface, zero capability the shared serializer lacks.
- **Deletion test.** The per-site structural tests (`test_parse_replconf_ack*`,
  `rejects_other_commands_and_other_replconf_subcommands`) pin each *parser* against literals, but
  deleting the encoder tomorrow and rewriting it differently would break nothing they check — they
  never exercise the encoder→decoder identity. The invariant that actually matters (parse∘encode =
  id) has no test that fails when it is violated.

## Evidence (verified file:line)

All paths under `frogdb-server/crates/replication/src/`. Line numbers verified against the tree.

| Site | Location | Mechanism | Verified |
| --- | --- | --- | --- |
| GETACK encode | `primary/mod.rs:259-273` (`request_acks`) | `serialize_command_to_resp("REPLCONF", [GETACK, *])` then `offsets.advance` + `replay.record(CONTROL_SHARD)` + `broadcast_frame` | ✓ uses shared serializer, **not** a hand literal |
| ACK decode | `primary/mod.rs:322-349` (`parse_replconf_ack`) | `redis_protocol::resp2::decode` + case-insensitive token match on `parts[0..3]`; returns `Some((offset, consumed))` | ✓ real RESP2 decoder, **not** structural byte inspection |
| ACK encode | `replica/streaming.rs:64-74` (`send_ack`) | `format!("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n", len, offset)` → `write_all` | ✓ hand-rolled literal; only REPLCONF not via serializer |
| GETACK decode | `replica/streaming.rs:83-98` (`is_getack_frame`) | structural `strip_prefix(b"*3\r\n$8\r\n")` / `b"\r\n$6\r\n"` + `eq_ignore_ascii_case` | ✓ bespoke fast-path, no full decode |

Call sites and couplings (all verified):

- `request_acks` is called by the WAIT solicitor: `wait_coordinator.rs:49`.
- `parse_replconf_ack` is `pub(crate)`; production caller is the primary's per-replica read loop
  `replica_session.rs:681` (`while let Some((ack_offset, consumed)) = parse_replconf_ack(&buf)` →
  `ingest_replica_ack` + `buf.advance(consumed)`), plus tests in `primary/tests.rs:1,15-62`. The
  `consumed` return is load-bearing for streaming reassembly.
- `is_getack_frame` is module-private; sole caller `streaming.rs:46`
  (`let solicited = is_getack_frame(&frame.payload)`), tests at `streaming.rs:100-134`.
- `send_ack` is called at `streaming.rs:48` (solicited answer) and `streaming.rs:58` (1s spontaneous
  tick).
- Behavior pin already present: `primary/tests.rs:209-270`
  (`wait_with_lagging_replica_broadcasts_a_stamped_getack`) asserts the broadcast frame
  `starts_with(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n")` — a wire-form assertion on the primary
  encoder output that the codec must keep green.
- The `$8`/`$3`/`$6` length prefixes are correct discriminators: `REPLCONF`=8, `ACK`=3,
  `GETACK`=6 bytes.

Shared machinery (unchanged, the encode side composes it): `serialize_command_to_resp`
(`frame.rs:48-72`) emits `*<n>\r\n$<len>\r\n<data>\r\n…`; `CONTROL_SHARD = u16::MAX`
(`frame.rs:89`) tags GETACK as a non-routed control frame; `ReplicationFrameCodec`
(`frame.rs:273-376`) owns the transport frame header (separate concern — the 20-byte envelope
per `FRAME_HEADER_SIZE = 20` at `frame.rs:84`, not the RESP payload).

Precedent verified: `CheckpointStreamCodec` (`fullsync.rs:140-…`), a `pub struct` with
`write_prelude` / `write_file_header` / `write_metadata` encoders and `parse_file_count` inverses;
its doc comment (`fullsync.rs:123-139`) states it is "the single definition of the on-wire grammar …
that was realized three times by hand (the sender, the PSYNC-reply marker/count detector, and the
receiver body parser)." This proposal applies the identical pattern to the ACK/GETACK grammar.

ADR alignment: `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` — "the data path never goes
through Raft." REPLCONF ACK/GETACK is pure replication data-path control (offset solicitation +
acknowledgement between Primary and Replica); it is untouched by the Raft Metadata Plane, so this
refactor is orthogonal to ADR-0001. No ADR is affected.

## Proposed design (Rust interface sketch)

A symmetric codec beside `CheckpointStreamCodec`, living in the `replication` crate (in `frame.rs`
next to `serialize_command_to_resp`, or a small sibling `replconf.rs` module). No new crate
dependency, no change to crate dependency direction — every current call site is already inside the
`replication` crate. Visibility `pub(crate)` (matching `parse_replconf_ack`; the codec never leaves
the crate).

```rust
/// The REPLCONF ACK / GETACK control-message grammar, as one symmetric codec.
///
/// Single definition of the wire shapes previously scattered across
/// `request_acks` (GETACK encode), `send_ack` (ACK encode), `parse_replconf_ack`
/// (ACK decode) and `is_getack_frame` (GETACK decode). The encode side composes
/// the crate's `serialize_command_to_resp`; the decode side owns the parsers.
/// Framing only — offset stamping, frame headers, and backlog recording stay in
/// the callers (`OffsetCoordinator` / `ReplicationFrameCodec` / `replay`).
pub(crate) struct ReplconfCodec;

impl ReplconfCodec {
    // --- encode (delegates to serialize_command_to_resp) ---

    /// `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n`
    pub(crate) fn encode_ack(offset: u64) -> Bytes;

    /// `*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n`
    pub(crate) fn encode_getack() -> Bytes;

    // --- decode (inverses) ---

    /// Parse a leading REPLCONF ACK frame from a (possibly streaming) buffer.
    /// `Some((offset, consumed))` on a complete frame; `None` if incomplete or
    /// not an ACK. Preserves the `consumed` contract the primary read loop uses
    /// to advance its buffer (`replica_session.rs:681`).
    pub(crate) fn parse_ack(buf: &[u8]) -> Option<(u64, usize)>;

    /// Structural fast-path: true iff `payload` is a `REPLCONF GETACK *`
    /// solicitation. Case-insensitive; intentionally *no* full RESP decode —
    /// this runs once per ingested frame on the replica hot path.
    pub(crate) fn is_getack(payload: &[u8]) -> bool;
}
```

Design notes:

- **`encode_ack` / `encode_getack` compose the existing serializer** — the bodies become
  `serialize_command_to_resp("REPLCONF", &[Bytes::from_static(b"ACK"), offset_bytes])` and
  `serialize_command_to_resp("REPLCONF", &[Bytes::from_static(b"GETACK"), Bytes::from_static(b"*")])`.
  This *removes* the hand-rolled `format!` in `send_ack` (byte-identical output; verified the
  serializer emits the same `$<len>\r\n<data>\r\n` per element) and makes GETACK encode a named verb
  instead of an inline serializer call.
- **`parse_ack` is a verbatim move** of `parse_replconf_ack`'s body (the `redis_protocol` decode +
  token match + `(offset, consumed)` return). Its `consumed` streaming semantics are preserved
  exactly — the read loop is untouched.
- **`is_getack` is a verbatim move** of `is_getack_frame`'s structural walk. It stays structural on
  purpose (documented hot-path constraint); the codec does not "upgrade" it to a full decode.
- The transport `ReplicationFrameCodec` (frame header) and `OffsetCoordinator` (advance/stamp) are
  **not** absorbed — the codec owns the RESP grammar of the two control messages only, mirroring how
  `CheckpointStreamCodec` owns delimiters while `stream_file_to_writer` owns payload bytes.

## Migration plan (ordered steps)

1. Add `ReplconfCodec` with the four methods; `encode_*` delegate to `serialize_command_to_resp`,
   `parse_ack` / `is_getack` receive the moved bodies of `parse_replconf_ack` / `is_getack_frame`.
2. Add the golden round-trip + discriminator tests (see Test plan) alongside the codec.
3. Rewire `request_acks` (`primary/mod.rs:260-263`): replace the inline
   `serialize_command_to_resp(…GETACK…)` with `ReplconfCodec::encode_getack()`. Offset advance +
   `replay.record` + `broadcast_frame` unchanged.
4. Rewire `send_ack` (`streaming.rs:64-74`): replace the `format!` literal with
   `self.stream.write_all(&ReplconfCodec::encode_ack(offset)).await?`. Delete the hand literal.
5. Rewire `is_getack_frame` caller (`streaming.rs:46`) to `ReplconfCodec::is_getack(&frame.payload)`;
   delete the standalone `fn is_getack_frame` and migrate its three tests into the codec suite.
   While touching `streaming.rs`, correct the stale `streaming.rs:33` comment that says "the 18-byte
   frame header" — the header is now 20 bytes (`FRAME_HEADER_SIZE = 20`, `frame.rs:84`; v2 added the
   2-byte origin-shard tag). The advance logic is unaffected (it counts RESP payload only), but the
   comment misstates the envelope size.
6. Rewire `parse_replconf_ack` callers (`replica_session.rs:681`, and the `use` at
   `replica_session.rs:43`; `primary/tests.rs:1`) to `ReplconfCodec::parse_ack`; delete the standalone
   `fn parse_replconf_ack` and migrate its four tests into the codec suite.
7. Keep `wait_with_lagging_replica_broadcasts_a_stamped_getack` (`primary/tests.rs:209`) as-is — it
   still asserts the stamped GETACK wire form end-to-end and now transitively validates
   `encode_getack`.
8. `just check replication` + `just test replication` (single-crate; per the remote-execution
   policy this is a sub-minute local loop, no testbox needed).

Each step is compiler-guided: deleting a `fn` breaks its callers until they point at the codec.

## Test plan

The win is a **golden-bytes round-trip suite** co-located with the grammar, replacing per-site
structural tests that can only pin one direction:

- `parse_ack(encode_ack(x)) == Some((x, encode_ack(x).len()))` for representative offsets asserted
  explicitly as `0`, `1`, and `u64::MAX` — **the currently-missing ACK producer/parser pin**, the one
  that would catch `send_ack`↔`parse_replconf_ack` drift. This case also pins that the offset is
  emitted in **decimal ASCII** (`offset.to_string()`, as `serialize_command_to_resp` does), so the
  20-digit `u64::MAX` round-trips correctly through the `$<len>\r\n<offset>\r\n` bulk-string framing —
  the boundary a hand-rolled `format!` most easily gets wrong.
- `is_getack(encode_getack())` is `true` — GETACK producer/parser pin, now anchored to the real
  encoder rather than a re-typed literal.
- **Cross-discriminator rejection:** `!is_getack(encode_ack(x))` and `parse_ack(encode_getack())`
  is `None` — proves ACK and GETACK cannot be confused (subsumes
  `rejects_other_commands_and_other_replconf_subcommands`).
- **Streaming/partial invariants preserved** (ported from `test_parse_replconf_ack_incomplete` /
  `_with_trailing_data`): incomplete buffer → `None`; two concatenated ACK frames → first parsed,
  `consumed == frame1.len()`, remainder re-parses.
- **Case-insensitivity** (ported from `matches_case_insensitively`): lowercase `replconf`/`getack`
  still recognized by `is_getack`.
- **Wrong-command rejection** (ported from `test_parse_replconf_ack_wrong_command`): a `SET` array
  parses to `None`.

Net: the four `test_parse_replconf_ack*` cases and the three `is_getack_frame` cases fold into one
codec suite that additionally covers both round-trips — strictly more coverage, one location.

## Risks & alternatives

- **Byte-identical encode is the load-bearing check.** `encode_ack` via `serialize_command_to_resp`
  must produce exactly `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n` (verified the
  serializer does; the round-trip test enforces it). Low risk, but it is the one place a wrong
  assumption would silently change the wire.
- **Do not regress the GETACK hot path.** `is_getack` must remain the structural `strip_prefix`
  fast-path, not a full RESP decode — it runs once per ingested frame. The proposal moves the body
  verbatim; a reviewer should reject any "cleanup" that routes it through `redis_protocol`.
- **`consumed` semantics.** `parse_ack` must keep returning `consumed` for the primary's streaming
  read loop; a signature that dropped it would break `buf.advance(consumed)`. Preserved by moving the
  body unchanged.
- **Alternative — do nothing.** The grammar is small and two of four sites already use shared
  machinery, so the blast radius today is modest. Rejected because the ACK producer/parser link is
  genuinely untested and `send_ack`'s hand literal is an outlier that invites exactly the drift a
  codec forecloses; the fix is S-sized and matches an established precedent.
- **Alternative — a full `Replconf` message enum covering every subcommand** (listening-port, capa,
  frogdb-version, ACK, GETACK) with unified encode/decode. Rejected for this pass: the handshake
  subcommands are encoded via `serialize_command_to_resp` in `replica/connection.rs` and parsed on
  the *server* command-dispatch side (`server/src/commands/replication.rs`), which would pull the
  refactor across the crate boundary into `server` and mix connection-setup config with the
  offset-solicitation control loop. Keeping the codec scoped to the ACK/GETACK data-path control pair
  is the deeper, tighter module; a broader message type can be a later proposal if it earns itself.
- **Alternative — absorb the frame header too.** Rejected: transport framing already has an owner
  (`ReplicationFrameCodec`); conflating RESP grammar with the wire envelope would widen the seam, not
  deepen it.

## Effort

**S.** Confined to the `replication` crate: add one 4-method codec, delete two free functions, rewire
four call sites, and consolidate seven existing tests into one round-trip suite. Compiler-guided, no
async/ordering changes, no cross-crate signature churn, no new dependency. The only substantive
verification is byte-identical encode, which the golden round-trip test pins directly.

## Related

- **Round 7 P56 — `CheckpointStreamCodec`** (`fullsync.rs:140`): the direct precedent. Same problem
  (a wire grammar realized N times by hand, kept consistent by inspection), same cure (one symmetric
  codec owning encode + inverse, framing-only, payload/stamping left in the callers). This proposal
  is the ACK/GETACK analogue.
- `serialize_command_to_resp` (`frame.rs:48`) — the shared RESP serializer the encode side composes.
- `ReplicationFrameCodec` (`frame.rs:273`) — the sibling transport-framing codec; deliberately left
  distinct (envelope vs. RESP grammar).
- ADR-0001 (Raft for cluster metadata): orthogonal — REPLCONF ACK/GETACK is data-path control, never
  routed through the Raft Metadata Plane.

## Adversarial review

**Verdict: CONFIRMED.** The reviewer's attempted refutation failed on every axis: all file:line
claims check out against the tree (`request_acks` primary/mod.rs:259-273; `parse_replconf_ack`
:322-349; `send_ack` streaming.rs:64-74; `is_getack_frame` :83-98; `serialize_command_to_resp`
frame.rs:48-72; `CONTROL_SHARD` :89; callers wait_coordinator.rs:49, replica_session.rs:681,
streaming.rs:46/48/58; wire-form pin primary/tests.rs:209-270; `CheckpointStreamCodec` precedent
fullsync.rs:140/123-139). Byte-identical encode was verified by hand for both ACK and GETACK. The
core claims — "`send_ack` is the only REPLCONF not via the serializer" and "the ACK producer/parser
link is entirely unpinned at the unit level" — were both confirmed. No borrow-checker, async,
crate-dependency-direction, or wire-compat problems; orthogonal to ADR-0001; a faithful analogue of
the round-7 `CheckpointStreamCodec` pattern. Premise sound, design feasible, effort S accurate.
Proceed as written. Two minor (optional-polish) items were raised; both fixed in place since they
were quick:

1. **Stale "18-byte frame header" comment / "18/20" hedge (minor, not a proposal defect).** The
   reviewer noted `streaming.rs:33` still comments "the 18-byte frame header" while
   `FRAME_HEADER_SIZE` is now 20 (v2 added the 2-byte origin-shard tag), and that the proposal hedged
   with "18/20". Verified `FRAME_HEADER_SIZE = 20` at `frame.rs:84`. **Resolved:** replaced the
   "18/20-byte envelope" hedge in Evidence with a definite "20-byte envelope per
   `FRAME_HEADER_SIZE = 20`", and added migration step 5 an instruction to correct the stale
   `streaming.rs:33` comment while that file is already being touched. Harmless to the refactor
   (`frame_advance` counts RESP payload only), so it stays scoped as opportunistic cleanup.

2. **Pin decimal-ASCII offset encoding explicitly in the round-trip test (minor).** The reviewer
   asked that the round-trip case assert the offset is emitted in decimal ASCII (`offset.to_string()`)
   so `u64::MAX` round-trips through the `$<len>` bulk-string length, with `0`, `1`, `u64::MAX` as
   asserted cases. **Resolved:** the Test plan's first bullet now names `0`, `1`, and `u64::MAX`
   explicitly and calls out the decimal-ASCII / 20-digit `$<len>` framing as the pinned invariant.
