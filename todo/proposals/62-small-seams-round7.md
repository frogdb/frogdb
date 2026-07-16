# Proposal: Small Seams, Round 7

Status: proposed
Date: 2026-07-16

## Problem

Round-7 deepening left another residue of small findings that share the round-40 shape: a
protocol-encoding rule implemented in the wrong module, a deep module with zero unit tests of its
own, an ordering invariant whose checks are a hand-picked subset of the real constraints, a
command path reaching past two established seams, and one flat-out scope gap that wants a decision.
Each is individually below the bar for its own proposal; together they are a batch of single-owner
seams. The five items are **independently implementable** — no item depends on another, and each is
one focused change with its own deletion test.

Vocabulary used throughout: *module / interface / depth / seam / adapter / leverage / locality /
deletion test*.

---

## Item A — the RESP2 null-array encoding belongs to the encoder, not the connection layer

**Problem.** `WireResponse::NullArray` is the one wire response the encoder cannot produce.
`to_resp2_frame` (`frogdb-server/crates/protocol/src/response.rs:260-264`) has a best-effort branch
that "falls back to `Null` (`$-1\r\n`)" — a silently-wrong output inside a method whose doc claims
it is total and cannot panic. The *real* `*-1\r\n` encoding lives one crate away, in the connection
layer, by poking the codec write buffer: `NULL_ARRAY_BYTES`
(`frogdb-server/crates/server/src/connection/frame_io.rs:18`), `queue_resp2_null_array`
(`:28-35`), and `write_null_array` (`:80-104`), plus a `matches!(…, NullArray)` special case in
both `send_wire_response` (`:119-122`) and `feed_wire_response` (`:169-173`).

| Evidence | Location |
|---|---|
| Encoder best-effort wrong-output branch | `protocol/src/response.rs:260-264` |
| Raw `*-1\r\n` literal + protocol branch | `server/…/frame_io.rs:18, 80-104` |
| Special-case in send + feed paths | `server/…/frame_io.rs:119-122, 169-173` |

This is a leaky seam: understanding the null-array wire path requires bouncing between the
`protocol` encoder and the `server` connection layer, and a protocol-encoding concern lives in the
connection module. Proposal 26 already collapsed the *two* duplicated blocks into one
`write_null_array` (its phase 3 landed even though its header still reads `proposed`), and proposal
49 (`d2d3fdca`) fixed the ordering by queueing into the codec write buffer instead of writing
straight to the socket. This item is the **remaining** leak: the bytes and the branch still live in
the connection layer, and the encoder still lies about being total. It does not contradict 26 or 49
— it generalizes 49 (the manual `write_buffer_mut()` poke becomes an ordinary codec feed) and
finishes 26's direction (one owner, moved down to where the other RESP2 encoding lives).

**Change.** Give the codec a small outbound type it can encode, so `NullArray` rides the normal
`feed`/`send` codec path and stays in feed order for free:

```rust
// server/…/connection/codec.rs
pub enum Resp2Outbound { Frame(BytesFrame), NullArray }

impl Encoder<Resp2Outbound> for FrogDbResp2 {
    fn encode(&mut self, item: Resp2Outbound, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Resp2Outbound::Frame(f) => self.inner.encode(f, dst),
            Resp2Outbound::NullArray => { dst.extend_from_slice(b"*-1\r\n"); Ok(()) }
        }
    }
}
```

`feed_response`/`send_response` narrow `WireResponse` to `Resp2Outbound` and feed it — the
`matches!(…, NullArray)` branches, `queue_resp2_null_array`, and the module-level `NULL_ARRAY_BYTES`
all delete. `NullArray` is handled *before* `to_resp2_frame`, so `response.rs` drops its
best-effort branch and its doc becomes honest (`to_resp2_frame` is total over the frames it is now
asked to encode). The `*-1\r\n` literal ends up beside the rest of the RESP2 encoding, in the
codec.

**Tests.** Keep proposal 49's `resp2_null_array_feed_order_is_preserved` (now driving the
`Resp2Outbound::NullArray` path — same asserted byte stream `$1\r\na\r\n*-1\r\n$1\r\nb\r\n`); add a
direct codec unit test that `encode(Resp2Outbound::NullArray)` emits exactly `*-1\r\n`. Deletion
test: the move removes whole artifacts (the literal, two `matches!` branches, the queue helper, the
encoder fallback) — net deletion, the signature of relocating a rule to its right module.

---

## Item B — RESP2 edge-case codec has zero unit tests of its own

**Problem.** `FrogDbResp2::decode` (`frogdb-server/crates/server/src/connection/codec.rs:55-137`)
is a genuinely deep module: it pre-processes the Redis protocol edge cases the upstream
`redis_protocol::codec::Resp2` rejects — empty lines (`:64-67`), negative multibulk `*-N` (`:76-79`),
oversized multibulk (`:81-88`) and oversized bulk (`:100-131`, via `scan_for_oversized_bulk`
`:148`) — with *consume-on-error-and-continue* semantics, then delegates. It earns its keep (the
deletion test screams: drop it and protocol-incompatibility bugs reappear across every client). But
it has **zero** unit tests in its own file; it is covered only indirectly through full-server
integration tests, so the subtle invariant — *exactly how much of the buffer is consumed on each
error path* — is asserted nowhere.

| Evidence | Location |
|---|---|
| Edge-case decode with consume-on-error | `server/…/codec.rs:55-137` |
| O(n) full-buffer scan per `*` decode | `scan_for_oversized_bulk` `codec.rs:148-168` |
| No `#[cfg(test)]` module in the file | `codec.rs` (ends at line 168) |

**Change.** Add a table-driven unit-test module over `BytesMut` inputs, asserting **both** the
returned frame *and* the residual buffer state after each call — the consume-on-error contract is
the load-bearing invariant a client depends on to resynchronize:

```rust
// (input bytes, expected result, expected residual buffer after decode)
let cases = [
    (b"\r\n*1\r\n$4\r\nPING\r\n".as_ref(), Ok(Some(ping_frame())), b"".as_ref()), // empty line skipped
    (b"*-3\r\n*1\r\n$4\r\nPING\r\n",        Ok(Some(ping_frame())), b""),          // *-N consumed, continues
    (b"*1048577\r\n",                       Err(oversized_multibulk),  b""),        // consumed, connection survives
    (b"$536870913\r\n",                     Err(oversized_bulk),       b""),        // top-level oversized bulk
    (b"*2\r\n$536870913\r\nx\r\n",          Err(oversized_bulk),       /* residual */),// in-array scan path
];
```

Also flag `scan_for_oversized_bulk` as an O(n) full-buffer scan performed on *every* `*`-prefixed
decode: a measure-before-optimizing note, not a change. Tests first — they pin the current behavior
so any later bound (e.g. cap the scan at the first `$` header) is provably equivalence-preserving.

**Depth justification.** The module is deep and adversarial-input-facing; unit tests are the
leverage that lets it be hardened or bounded later without a full-server round-trip to prove each
edge case. Deletion test: these tests document a contract that has no other written home.

---

## Item C — `WRITE_EFFECT_ORDER` must-precede constraints as a checked relation

**Problem.** `WRITE_EFFECT_ORDER` (`frogdb-server/crates/core/src/shard/post_execution.rs:156-166`)
is the single canonical 9-step post-execution order, guarded by `safety_ordering_invariants`
(`:522-552`) with **hand-picked position asserts**. The encoded constraints are a *subset* of the
real ones: a reorder can compile and pass the position tests while violating an unencoded
constraint. The module docs (`:40-56`, `:148-155`) and the effect implementations justify more
must-precede pairs than the test checks — most conspicuously **WAL persistence must precede
replication broadcast** (a write must be durable locally before replicas observe it), which today
is only *implicitly* guarded by the "broadcast is terminal" assert, not stated as its own relation.

| Constraint (from docs + impl) | Encoded in `safety_ordering_invariants`? |
|---|---|
| `VersionIncrement` is first (before all observers) | yes (`position == 0`) |
| `TrackingInvalidation` → `ReplicationBroadcast` | yes |
| `DirtyCounter` → `WaiterSatisfaction` | yes |
| `KeysizesFlush` == `WaiterSatisfaction` + 1 (adjacency) | yes |
| `ReplicationBroadcast` is terminal | yes |
| `WalPersistence` → `ReplicationBroadcast` (durability before replicas) | **no** (only implied by terminal) |
| `WalPersistence` → `SearchIndex` (index reflects persisted state) | **no** |
| `KeyspaceNotifications` → `WaiterSatisfaction` (notify before wake) | **no** |

**Change.** Declare the must-precede relation as data — a small const of `(before, after)` pairs
plus the one adjacency — and have the test validate the whole array against **every** pair:

```rust
/// Each pair (A, B) asserts A runs strictly before B. Adding a WriteEffectKind
/// forces declaring its constraints here or the exhaustiveness check below fails.
const MUST_PRECEDE: &[(WriteEffectKind, WriteEffectKind)] = &[
    (VersionIncrement, TrackingInvalidation), (VersionIncrement, ReplicationBroadcast),
    (TrackingInvalidation, ReplicationBroadcast),
    (DirtyCounter, WaiterSatisfaction),
    (KeyspaceNotifications, WaiterSatisfaction),
    (WalPersistence, SearchIndex), (WalPersistence, ReplicationBroadcast),
    // …
];
const MUST_BE_ADJACENT: &[(WriteEffectKind, WriteEffectKind)] =
    &[(WaiterSatisfaction, KeysizesFlush)];

#[test]
fn order_satisfies_all_declared_constraints() {
    for &(a, b) in MUST_PRECEDE { assert!(position(a) < position(b), "{a:?} must precede {b:?}"); }
    for &(a, b) in MUST_BE_ADJACENT { assert_eq!(position(a) + 1, position(b)); }
}
```

The exact pair set is enumerated by reading the ordering rationale (`:148-155`) and each effect's
body; the table above is the starting enumeration. `VersionIncrement`-first collapses to "precedes
every other kind"; the point is that a new effect can no longer be added without declaring how it
orders against the rest.

**Depth justification.** Locality — the *rule* (which effect precedes which) becomes data next to
the order it constrains, instead of being smeared across the docs and partially mirrored in ad-hoc
asserts. Deletion test: the position asserts collapse into one relation-driven check; the WAL→
broadcast durability constraint gains a written home it never had.

---

## Item D — scatter path reaches past the transport-codec and keyspace-accounting seams

**Problem.** `execute_scatter_part` (`frogdb-server/crates/core/src/shard/execution.rs:450-693`)
reaches past two established seams:

1. **Transport serialization.** The COPY, DUMP, and CopySet arms call
   `crate::persistence::serialize`/`deserialize` directly and hand-roll expiry extraction, so the
   self-describing transport frame has three producers, not one. COPY serializes at
   `execution.rs:555` and extracts expiry inline at `:545-548`; DUMP serializes at `:612` and sets
   `metadata.expires_at` inline at `:607-609`; the RESTORE side deserializes at `:867`. This is the
   same self-describing frame the WAL/RDB path uses — a command-execution module reaching into the
   persistence codec is a leaky seam, and expiry-vs-header handling is duplicated across the arms.

2. **Keyspace hit/miss accounting.** The `LookupSpec` seam (`execution.rs:88-113`) owns the
   Redis-parity rule (proposal 24: *existence == hit*, derived from key existence, never reply
   shape). Yet three scatter arms re-implement it inline with their own `hits`/`misses`
   accumulators: `Exists` (`:467-479`), `Touch` (`:483-496`), and `scatter_mget` (`:701-720`).
   Four copies of one rule, three of them bypassing the seam that exists precisely to own it.

| Evidence | Location |
|---|---|
| COPY: direct `serialize` + inline expiry | `execution.rs:545-558` |
| DUMP: direct `serialize` + inline `expires_at` | `execution.rs:600-613` |
| CopySet: direct `deserialize` | `execution.rs:867` |
| Inline hit/miss (Exists / Touch / MGET) | `execution.rs:467-479, 483-496, 701-720` |
| The seams reached past | `LookupSpec` `:88-113`; `persistence::{serialize,deserialize}` |

**Change (two independent sub-changes; land separately).**

- **D1 — one transport-serialize helper.** A `serialize_key_for_transport(store, key) ->
  Option<(Bytes, Option<i64>)>` (bytes + relative-expiry-ms) shared by DUMP/RESTORE/COPY (and RDB
  where the frame shape matches), so the self-describing frame has one producer and expiry
  extraction lives once. The three arms shrink to a call plus their arm-specific reply shaping.
- **D2 — route accounting through one helper.** A `record_lookup_existence(keys)` (or reuse the
  seam's existence-snapshot logic) called by the three scatter arms, so the *existence == hit* rule
  lives once. Note the arms differ in what they *return* (Exists → 0/1, Touch → touched, MGET →
  value), but the *accounting* is identical and is the only duplicated part.

**Depth justification.** Deletion test: each helper deletes three inline reimplementations that
today can drift independently (an expiry-handling bug in COPY that DUMP doesn't share; a hit-count
rule the seam already fixed once). Leverage: the transport frame and the parity rule each get a
single owner, matching proposals 09 (typecodec) and 24 (keyspace accounting).

---

## Item E — inline (telnet-style) command support: a flagged scope gap, decision needed

**Problem.** FrogDB has no inline-command path. `FrogDbResp2::decode`
(`frogdb-server/crates/server/src/connection/codec.rs:55-137`) pre-processes only `*`/`$`/empty-line
prefixes; any other first byte falls through to the upstream RESP2 decoder, which rejects it as a
protocol decode error — a bare `PING\r\n` never reaches `ParsedCommand`. (A *valid* but non-array
RESP frame, e.g. `+PING\r\n`, is what actually reaches `ParsedCommand::try_from` and yields
`ProtocolError::ExpectedArray` at `protocol/src/command.rs:69`; the bare inline line errors one
layer earlier, in the codec. Either way it is an error, never inline execution.) Real Redis accepts
inline commands (`processInlineBuffer`), and the compat suite **explicitly excludes** them:
`redis-regression/tests/protocol_tcl.rs:6` lists "Inline command tests (require special inline
parsing)" among intentional exclusions, and `tcl_unbalanced_number_of_quotes`
(`protocol_tcl.rs:229-252`) sends the inline `set """test-key""" test-value\r\n` and passes today
only *incidentally* — it asserts any `-ERR`, and the codec's decode error happens to be one, not
because unbalanced-quote inline parsing is implemented.

| Evidence | Location |
|---|---|
| No inline path; non-`*`/`$` → decode error | `server/…/codec.rs:55-137` |
| Non-array RESP frame → `ExpectedArray` | `protocol/src/command.rs:48-72` |
| Compat suite excludes inline tests explicitly | `redis-regression/…/protocol_tcl.rs:6` |
| Inline test passes only incidentally | `protocol_tcl.rs:229-252` |

**Decision (pick one — do not leave it implicit).**

- **Implement.** In `FrogDbResp2::decode`, add a branch: when the first byte is not a recognized
  RESP prefix (`*`/`$`/`+`/`-`/`:`) and a full `\r\n`-terminated line is buffered, parse it as an
  inline command — whitespace-split with Redis's quote/escape rules (`sdssplitargs`), erroring on
  unbalanced quotes — and synthesize an `Array` `BytesFrame`. The rest of the pipeline is unchanged;
  this is a thin *adapter* at the one seam that already owns edge-case pre-processing, and it lets
  the excluded `protocol_tcl` inline tests be ported (real coverage, not incidental).
- **Document non-support as a divergence.** If inline commands are deemed out of scope
  (RESP-only clients are the target), record it in `todo/compat/` alongside the other intentional
  incompatibilities, and tighten `tcl_unbalanced_number_of_quotes` so it is not silently satisfied
  by an unrelated decode error.

**Tests.** If implemented: unit tests in the codec (Item B's table extends naturally) for
`PING\r\n` → `["PING"]`, quoted args, escape sequences, and unbalanced-quote → error; plus porting
the excluded `protocol_tcl` inline cases. If documented as divergence: a compat entry and the
tightened incidental test.

**Depth justification.** The inline rule, if implemented, is data-shaped pre-processing that belongs
at the same seam as the other RESP2 edge cases (Item B) — one module, not a second parser. Deletion
test cuts the other way here: there is nothing to delete because the path does not exist; this item
exists to force the *decision* rather than leave a silent scope gap that a passing-by-accident test
disguises.

---

## Verification

Per touched crate: `just fmt` / `just check` / `just lint` + crate tests. Items A, B, C, D1, D2, and
E are independently implementable and independently testable — none shares a change surface with
another.
