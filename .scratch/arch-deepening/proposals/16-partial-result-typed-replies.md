# Proposal 16 — Give `PartialResult` typed reply variants instead of smuggling control data through fabricated keys

## Summary (2-3 sentences)

`PartialResult.results: Vec<(Bytes, Response)>` is the shard→coordinator reply for *keyed*
Scatter-Gather, but SCAN, DBSIZE, RANDOMKEY, FLUSHDB, and COPY overload it with non-keyed control
data: SCAN packs the next cursor under a magic `b"__cursor__"` key that the coordinator recovers by
**string comparison**; DBSIZE/RANDOMKEY/FLUSHDB invent throwaway keys (`__dbsize__`, `__randomkey__`,
`__flushdb__`) that exist only to satisfy the `(Bytes, Response)` shape and are then discarded; COPY
packs `[value, expiry]` as a positional two-element `Response::Array`. FrogDB already has the right
pattern one field over — `PartialResult.ft: Option<FtShardReply>` carries FT.* replies as a *typed*
sum. This proposal extends that precedent to the rest of the broadcast ops: replace the overloaded
keyed vector with typed reply variants (`Scan { next_cursor, keys }`, `Count`, `RandomKey`,
`Flushed`, `Copy`) so the cursor-walk merge and the COPY handshake become compiler-checked and
socket-free-testable, and a rename can no longer silently break the wire.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current design |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/types.rs` | 1285 | `PartialResult` struct (L726-732: `results` + `ft`), `from_results`/`from_ft` (L734-747) |
| `frogdb-server/crates/core/src/shard/execution.rs` | 1733 | `execute_scatter_part` (L583-787); the five overloads: DbSize L633-640, Scan L642-663, Copy L664-686, FlushDb→`scatter_flushdb` L941, RandomKey L698-704 |
| `frogdb-server/crates/core/src/shard/message.rs` | 870 | `ScatterOp` enum (L732) — the request side these replies answer |
| `frogdb-server/crates/server/src/connection/scatter.rs` | 348 | `handle_scan` (L43) string-matches `b"__cursor__"` (L165); `handle_dbsize` (L225), `handle_randomkey` (L241), `handle_flushdb` (L321) |
| `frogdb-server/crates/server/src/connection/routing.rs` | 362 | `execute_cross_shard_copy` (L195) positional COPY destructure (L258-280); `dispatch_scatter(op: ScatterGatherOp)` (L153) |
| `frogdb-server/crates/server/src/scatter/broadcast.rs` | 923 | Merge strategies; `IntegerTotal for PartialResult` (L194-204), `SortedUnion` (L253-274), `AllOk` (L482-505), `ShardZeroProjection for PartialResult` (L386-402) |
| `frogdb-server/crates/server/src/scatter/executor.rs` | — | VLL-locked keyed path (`ScatterGatherStrategy`, MGET/MSET/DEL/EXISTS/TOUCH/UNLINK): `partial.results.into_iter().collect()` into a `HashMap<Bytes, Response>` (L127). A genuinely-keyed consumer → `Keyed(pairs)`; caught at compile time. |
| `frogdb-server/crates/search/src/wire.rs` | 755 | `FtShardReply` enum (L500-505) — the typed-reply precedent |
| `frogdb-server/crates/server/src/connection/search/merge.rs` | 1022 | Consumes `reply.ft` by typed match (L345, L445, L528) — the model consumer |
| `frogdb-server/crates/core/src/command.rs` | — | `ScatterGatherOp` enum (L160), the round-8 P01 typed op the reply should mirror |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | 16 | Data path never goes through Raft; SCAN/DBSIZE/COPY are pure data-plane Scatter-Gather. Untouched. |

## Problem (concrete verified evidence)

`PartialResult` models exactly one reply shape well — a list of `(key, response)` pairs, one per key
the coordinator asked about (MGET, DEL, EXISTS, TOUCH, UNLINK, KEYS, DUMP, CopySet). Five broadcast
ops do not fit that shape, so each fakes it in a different way.

**1. SCAN — genuine magic-string coupling.** The shard packs the next cursor under a sentinel key
(`execution.rs:654-658`):

```rust
results.push((
    Bytes::from_static(b"__cursor__"),
    Response::Integer(next_cursor as i64),
));
for key in found_keys { results.push((key.clone(), Response::bulk(key))); }
```

and the coordinator recovers it by comparing the key byte-for-byte (`scatter.rs:163-172`):

```rust
for (key, response) in partial.results {
    if key.as_ref() == b"__cursor__" {
        if let Response::Integer(c) = response { shard_next_cursor = c as u64; }
    } else {
        all_keys.push(key);
    }
}
```

This is the load-bearing coupling: the string `"__cursor__"` is written in one crate (`core`) and
matched in another (`server`) with **no shared constant and no compiler link**. Rename either side
and the cursor silently reads as `0` — SCAN reports "done" after the first shard, dropping every key
on shards 1..N, with no compile error and no panic.

**2. DBSIZE / RANDOMKEY / FLUSHDB — fabricated filler keys the consumer *ignores*.** These are not
string-matched at all; the sentinel key is pure junk that exists only because the reply type demands
a `Bytes` for every `Response`:

- **DBSIZE** returns `(b"__dbsize__", Integer(count))` (`execution.rs:633-640`). The consumer is
  `SumIntegers<PartialResult>` (`scatter.rs:228`), whose `IntegerTotal::integer_total`
  (`broadcast.rs:194-203`) sums every `Response::Integer` **regardless of key** — the key is
  `filter_map`'d away. `__dbsize__` is never read.
- **RANDOMKEY** returns `(b"__randomkey__", …)` (`execution.rs:698-704`); the consumer takes
  `partial.results.into_iter().next().map(|(_, response)| response)` (`scatter.rs:310-315`) — key
  discarded with `_`.
- **FLUSHDB** returns `(b"__flushdb__", ok())` (`execution.rs:941`); the consumer is `AllOk`, whose
  `absorb` is `{}` and `finish` returns `Response::ok()` unconditionally (`broadcast.rs:496-504`) —
  the entire reply, key and value, is thrown away.

So three of the five overloads invent a meaningless key purely to inhabit `Vec<(Bytes, Response)>`.
The reply shape is wrong enough that the data has to be *dressed up as keyed* and then *undressed*.

**3. COPY — positional array smuggling.** The read phase packs value + expiry as a two-element
`Response::Array` under the real source key (`execution.rs:664-686`):

```rust
vec![(source_key.clone(), Response::Array(vec![Response::bulk(serialized), expiry_resp]))]
```

and the coordinator destructures it positionally, ignoring the key (`routing.rs:258-274`):

```rust
let source_data = source_result.results.into_iter().next();
let (value_data, expiry_ms) = match source_data {
    Some((_, Response::Array(arr))) if arr.len() == 2 => {
        let data_bytes = match &arr[0] { Response::Bulk(Some(b)) => b.clone(), _ => return err };
        let expiry    = match &arr[1] { Response::Integer(ms) => Some(*ms), Response::Null => None, _ => return err };
        (data_bytes, expiry)
    }
    …
};
```

`arr[0]`/`arr[1]` and `arr.len() == 2` are a hand-rolled tuple with no name. Swap the two slots, or
have the shard emit a one- or three-element array, and this decodes wrong at runtime only.

**The typed-reply precedent already exists, one field over.** `PartialResult.ft:
Option<FtShardReply>` (`types.rs:729-731`) carries the FT.* fan-out replies as a *typed sum*
(`wire.rs:500-505`):

```rust
pub enum FtShardReply {
    Search(Result<ShardSearchReply, String>),
    Aggregate(Result<PartialAggregate, String>),
}
```

and its consumers destructure by variant, not by string (`merge.rs:345-365`):

```rust
match reply.ft {
    Some(FtShardReply::Search(Ok(shard_reply))) => { self.total += shard_reply.total; … }
    Some(FtShardReply::Search(Err(msg)))        => { self.error = Some(Response::error(msg)); }
    _ => {}
}
```

FT.* proves the codebase already knows the right shape for "a shard reply that is *not* a keyed
vector." SCAN/DBSIZE/RANDOMKEY/FLUSHDB/COPY are the same kind of reply, left in the old smuggled
form.

## Why it is shallow (architecture vocabulary)

**The Interface is narrower than the data.** `PartialResult` presents one **Interface** —
"key→response pairs" — over five payloads that are *not* key→response pairs. That is a **shallow**
carrier: the type's shape (`Vec<(Bytes, Response)>`) is thinner than the information it must move, so
each caller re-widens it by hand — a sentinel key here, a positional array there, a discarded filler
elsewhere. The FT.* variant already widened correctly; the broadcast ops did not.

**Locality is broken across a crate Seam.** The fact "SCAN's cursor rides under `__cursor__`" is
knowledge split between `core/src/shard/execution.rs` (writer) and
`server/src/connection/scatter.rs` (reader), held consistent only by a byte string that appears in
both files and nowhere else. To understand one COPY reply you must read the producer arm in `core`
*and* the positional destructure in `server` and mentally align slot 0 with slot 0. A typed variant
moves that agreement onto the compiler.

**Fails the deletion / rename test — the failure class Proposal 01 named.** Round-8 P01
(`01-scatter-gather-declarative-seam.md`) made the *request* side compiler-checked: adding a scatter
command is one `ScatterGatherOp` declaration and a missing arm is a compile error. The *reply* side
was left un-deepened. Rename `__cursor__`, reorder the COPY array, or add a sixth broadcast op and
forget the sentinel wiring, and nothing fails to compile — exactly the silent-break the P01 op enum
was built to prevent, reappearing on the return path.

**Low Leverage.** Three ops pay the cost of manufacturing a `Bytes` key they never use. The type
carries more shape (a key per response) than four of the five consumers read (they read a scalar, an
option, or nothing), and the fifth (SCAN) reads the key only to throw half of them away after a
string test.

## Proposed design (Rust interface sketch — signatures/types only)

Turn `PartialResult` from a struct-with-optional-payloads into a **sum type**, one variant per reply
shape, folding today's `results` + `ft` fields into variants. Each variant mirrors the `ScatterOp` /
`ScatterGatherOp` that produces it (round-8 P01), so request and reply are typed in lockstep.

```rust
// core/src/shard/types.rs
#[derive(Debug)]
pub enum PartialResult {
    /// Keyed (key, response) pairs: MGET/DEL/EXISTS/TOUCH/UNLINK/KEYS/DUMP/CopySet,
    /// and the shard-error fallback path (dispatch_core.rs).
    Keyed(Vec<(Bytes, Response)>),

    /// SCAN: this shard's next cursor (0 = exhausted) and the keys found this step.
    Scan { next_cursor: u64, keys: Vec<Bytes> },

    /// DBSIZE: this shard's key count.
    Count(i64),

    /// RANDOMKEY: a random key from this shard, or `None` if empty.
    RandomKey(Option<Bytes>),

    /// FLUSHDB acknowledgement (no payload).
    Flushed,

    /// COPY read phase: the source value + out-of-band expiry, or `None` if absent.
    Copy(Option<CopyPayload>),

    /// FT.* typed reply (absorbs today's `ft` field).
    Ft(frogdb_search::FtShardReply),
}

#[derive(Debug)]
pub struct CopyPayload {
    /// Self-describing persistence frame (no separate type tag).
    pub value: Bytes,
    /// Expiry in ms; `None` = no expiry. Shipped out-of-band, not in the frame header.
    pub expiry_ms: Option<i64>,
}

impl PartialResult {
    pub fn keyed(results: Vec<(Bytes, Response)>) -> Self; // replaces from_results
    pub fn ft(reply: frogdb_search::FtShardReply) -> Self; // replaces from_ft
}
```

The merge strategies destructure by variant instead of scanning keys. Sketch of the shape change (no
bodies):

```rust
// broadcast.rs
impl IntegerTotal for PartialResult {           // DBSIZE
    fn integer_total(&self) -> i64;             // matches Count(n) => n, else 0
}
impl MergeStrategy for SortedUnion {            // KEYS
    fn absorb(&mut self, _: usize, reply: PartialResult); // matches Keyed(pairs)
}
// SCAN keeps its bespoke cursor walk but reads typed fields:
//   let PartialResult::Scan { next_cursor, keys } = partial else { … };
// COPY read phase:
//   let PartialResult::Copy(payload) = source_result else { … };
```

`SmallVec`/`Vec` allocation is unchanged; the enum is no larger than today's struct (a `Vec` +
`Option` collapse into a single tagged union). Keyed remains a `Vec<(Bytes, Response)>` for the ops
that genuinely are keyed — this is not "enum everything," it is "name the five shapes that were
lying about being keyed."

**`Default` must be hand-written.** `PartialResult` today `#[derive(Default)]` (`types.rs:725`), and
`PartialResult::default()` is a live call site — the empty reply on a VLL dequeue-miss
(`core/src/shard/vll.rs:46`). The derive does not survive the struct→enum conversion: `#[default]`
only attaches to a *unit* variant, not the data-carrying `Keyed(Vec<…>)` that must be the default. So
the enum design requires a hand-written `impl Default for PartialResult { fn default() -> Self {
PartialResult::Keyed(Vec::new()) } }` (matching today's empty-`results` default). The staged
`control: Option<ScatterControl>` alternative keeps the struct and its `#[derive(Default)]` intact,
so it needs no such shim — one more reason to stage if the blast radius must be minimized.

### Alternative (smaller, exact `ft` mirror)

Keep `PartialResult` a struct and add one typed field beside `ft`:

```rust
pub struct PartialResult {
    pub results: Vec<(Bytes, Response)>,
    pub ft: Option<frogdb_search::FtShardReply>,
    pub control: Option<ScatterControl>,   // NEW
}
pub enum ScatterControl { Scan { next_cursor: u64, keys: Vec<Bytes> }, Count(i64),
                          RandomKey(Option<Bytes>), Flushed, Copy(Option<CopyPayload>) }
```

This is the minimal move that matches the existing `ft` precedent exactly and touches the fewest
consumers, but it leaves a struct with three mutually-exclusive payload fields — itself a union
pretending to be a product. Recommended only if the enum's blast radius must be staged; see Risks.

## Migration plan (ordered steps)

1. **Add `CopyPayload` and the `PartialResult` enum** (or `ScatterControl` for the staged
   alternative) in `core/src/shard/types.rs`, with `keyed`/`ft` constructors. Keep the old
   `from_results`/`from_ft` as thin `#[deprecated]` shims for one commit to shrink the diff, or
   rename outright (pre-production; breaking is fine). Replace the `#[derive(Default)]` with a
   hand-written `impl Default` returning `Keyed(Vec::new())` — the derive cannot pick a
   data-carrying default variant — so the `PartialResult::default()` call at `vll.rs:46`
   (dequeue-miss empty reply) keeps compiling. (The staged `control`-field alternative keeps the
   struct's derive and skips this step.)
2. **Producers in `execution.rs`** — rewrite the five arms to build typed variants: DbSize →
   `Count`, Scan → `Scan { next_cursor, keys }`, Copy → `Copy(Some(CopyPayload{..}))` / `Copy(None)`,
   RandomKey → `RandomKey(opt)`, `scatter_flushdb` → `Flushed`. The keyed arms (MGET/DEL/…/DUMP,
   `CopySet`, the `dispatch_core.rs:53` error path) become `PartialResult::keyed(...)`.
3. **Consumers, compiler-driven** — every `partial.results` access is now a match arm:
   - `handle_scan` (`scatter.rs:163-172`) → `let PartialResult::Scan { next_cursor, keys } = partial`
     and drop the `b"__cursor__"` comparison entirely.
   - `execute_cross_shard_copy` (`routing.rs:258-280`) → match `PartialResult::Copy(payload)`; delete
     the `arr.len() == 2` / `arr[0]` / `arr[1]` positional decode.
   - `IntegerTotal` (DBSIZE, `broadcast.rs:194`), `SortedUnion` (KEYS), `SortedByKey`,
     `ShardZeroProjection` (`broadcast.rs:386`), `AllOk`, and `handle_randomkey` → match their
     variant.
   - The VLL-locked keyed path (`scatter/executor.rs:127`, `ScatterGatherStrategy` for
     MGET/MSET/DEL/EXISTS/TOUCH/UNLINK) does `partial.results.into_iter().collect()` — a distinct
     consumer from the broadcast merges and the two bespoke walks. It is genuinely keyed, so it
     maps cleanly to `Keyed(pairs)` and is caught at compile time.
   - `reply.ft` matches in `merge.rs:345/445/528` → `PartialResult::Ft(...)` (enum design only).
4. **Delete the sentinel constants** — grep proves the only `__cursor__`/`__dbsize__`/`__randomkey__`
   /`__flushdb__` occurrences are the producer arms and the one SCAN consumer; all vanish.
5. **`cargo check` the workspace** — the enum makes every un-updated `.results` a type error, so the
   compiler enumerates the remaining call sites; there is no silent-miss path.
6. **Run the SCAN/DBSIZE/COPY/RANDOMKEY/FLUSHDB integration + unit suites** unchanged as the
   behavioral gate, then add the new typed unit tests (Test plan).

## Test plan

- **New: cursor-walk merge without a live shard.** Today asserting "SCAN stitches cursors across
  shards and drops none" needs a booted multi-shard server. With `PartialResult::Scan { next_cursor,
  keys }`, feed the `handle_scan` merge synthetic per-shard `Scan` replies and assert the encoded
  final cursor and the concatenated key set — a plain unit test, no Tokio, no shards. This directly
  pins the bug the magic string can cause (cursor mis-read → keys dropped on shards 1..N).
- **New: COPY handshake on typed fields.** Assert the read phase yields `Copy(Some(CopyPayload {
  value, expiry_ms }))` for a present key and `Copy(None)` for an absent one, and that
  `execute_cross_shard_copy` forwards `value`/`expiry_ms` into the `CopySet` request — replacing the
  `Array(arr) if arr.len() == 2` positional assertion that silently tolerates a reordered array.
- **New: DBSIZE/RANDOMKEY/FLUSHDB shape tests** — `Count(n)` sums, `RandomKey(None)` on empty shard,
  `Flushed` ack; each a synchronous assertion that no longer depends on a fabricated key surviving
  the aggregator.
- **Regression guard:** keep the existing end-to-end SCAN/COPY/DBSIZE tests green through the
  migration; the typed rewrite must be behavior-preserving on the wire (client-visible RESP is
  unchanged — only the internal shard→coordinator representation changes).
- **Grep guard:** a test or CI grep asserting `__cursor__` and friends no longer appear in the tree,
  so the sentinel form cannot be reintroduced.

## Risks & alternatives

- **Blast radius is wider than the FT precedent's.** `ft` was additive; this rewrites the
  `results`-shaped consumers in `broadcast.rs` (`IntegerTotal`, `SortedUnion`, `SortedByKey`,
  `ShardZeroProjection`, `AllOk`) plus the two bespoke walks (`handle_scan`, `execute_cross_shard_copy`).
  All are compiler-driven — an enum turns every stale `.results` into a type error — so there is no
  silent-miss risk, but it is more files than a one-field add. Mitigation: the **staged alternative**
  (add `control: Option<ScatterControl>` beside `ft`) lands the typed replies without touching the
  keyed consumers, then a follow-up collapses `results`/`ft`/`control` into the full enum.
- **Keyed ops must stay keyed.** MGET/DEL/EXISTS/TOUCH/UNLINK/KEYS/DUMP/CopySet and the
  `dispatch_core.rs:53` shard-error fallback genuinely need `(key, response)` pairs (order/dedup by
  key). The `Keyed` variant preserves that exactly; this proposal narrows only the five ops that were
  never keyed. Verify the error-fallback path (`from_results(error_results)`) maps to `Keyed`.
- **`ShardZeroProjection`/`ShardZeroReply` interplay.** `ShardZeroProjection for PartialResult`
  (`broadcast.rs:386`) currently pulls `results.next()`; confirm which ops route through the
  shard-zero reply path and give each the right variant (most are `Keyed`; none of the five overloads
  use it today — verify before deleting the `results` field).
- **Folding `ft` in vs leaving it.** The enum design absorbs `ft` into an `Ft` variant, removing the
  "struct with two optional payloads" smell but touching `merge.rs`. If minimizing search-crate churn
  matters this pass, keep `ft` as-is and add only the non-FT variants; the `ft` fold is a clean
  follow-up. Either way the FT.* path is the template, not the target.
- **Wire/serialization.** `PartialResult` is an in-process type sent over an in-memory
  `oneshot`/`ShardMessage` channel between the coordinator and Internal Shards — it is **not** a
  persisted or network-serialized frame, so there is no on-disk/replication format migration. (COPY's
  `value` is a self-describing persistence frame *inside* the reply, unchanged.) Confirmed: no
  `Serialize`/`Deserialize` derive on `PartialResult`.
- **ADR-0001 untouched.** SCAN/DBSIZE/RANDOMKEY/FLUSHDB/COPY are pure data-plane Scatter-Gather; none
  traverse the Raft Metadata Plane. This is an internal reply-type reshape with no cluster-metadata
  or Config-Epoch interaction.
- **Crate dependency direction preserved.** `CopyPayload` and the variants live in `core` (where
  `PartialResult` already is); `core` already depends on `frogdb_search` for the `ft` field, so the
  `Ft` variant adds no new edge. `persistence`/`replication`/`cluster` are not touched.

## Effort

**M** (staged alternative) to **M–L** (full enum). The producers are five localized arm rewrites in
one `core` function; the consumers are compiler-enumerated. The M–L upper bound is the enum's
consumer breadth in `broadcast.rs` + `merge.rs` and the care needed to keep the keyed ops and the
shard-error fallback on the `Keyed` variant. No async, disk, ordering, or wire-format changes; the
client-visible RESP output is byte-identical — only the internal shard→coordinator representation is
retyped. Landing the staged `control` field first de-risks the collapse.

## Related

- **`FtShardReply` precedent (`PartialResult.ft`)** — `types.rs:729-731`, `wire.rs:500-505`,
  consumed by typed match at `merge.rs:345/445/528`. The exact pattern this proposal generalizes: a
  shard reply that is not a keyed vector, modeled as a typed sum rather than smuggled through
  `results`.
- **Round-8 P01 `ScatterGatherOp`** — `01-scatter-gather-declarative-seam.md`; `ScatterGatherOp`
  (`core/src/command.rs:160`) made the *request* side compiler-checked. This proposal is the
  symmetric move on the *reply* side, so each op variant pairs with a reply variant end to end.
- **Proposal 07 (Scatter-Gather fanout helpers)** and **Proposal 11 (shard-message category enums)**
  — adjacent work tightening the Scatter-Gather seam; the typed reply composes with the `MergeStrategy`
  fanout helpers those introduce.

## Adversarial review

**Verdict: CONFIRMED** — all load-bearing claims verified accurate against the cited file:lines; two
minor precision/completeness nits, both fixed. Design premise (shallow keyed-vector carrier smuggling
non-keyed control data across a crate seam) and the FT-mirror fix are sound. Recommend proceed.

- **Issue 1 (minor) — incomplete consumer inventory.** The `.results` consumer inventory (Files
  table, migration step 3) omitted the VLL-locked keyed path `scatter/executor.rs:127`, which does
  `partial.results.into_iter().collect()` into a `HashMap<Bytes, Response>` for
  MGET/MSET/DEL/EXISTS/TOUCH/UNLINK via `ScatterGatherStrategy` — a distinct consumer from the
  broadcast merges and the two bespoke walks. It is genuinely keyed, so it maps cleanly to
  `Keyed(pairs)` and IS caught at compile time (the design's safety claim holds); the "verified paths"
  table simply under-counted. **Resolved:** added a Files-table row for `scatter/executor.rs:127` and
  an explicit bullet in migration step 3.
- **Issue 2 (minor) — `Default` derive breaks under struct→enum.** `PartialResult` currently
  `#[derive(Default)]` (`types.rs:725`) and `PartialResult::default()` is a live call site — the empty
  reply on VLL dequeue-miss (`vll.rs:46`). `#[default]` only attaches to unit variants, not the
  data-carrying `Keyed(Vec<…>)` that must be the default, so a hand-written `impl Default` (returning
  `Keyed(Vec::new())`) is required for the enum design; the staged `control: Option<ScatterControl>`
  alternative keeps the struct's derive and avoids it. The original size/collapse framing and
  migration steps omitted this. **Resolved:** documented the required hand-written `impl Default` in
  the design section and migration step 1, and noted the staged alternative sidesteps it.
