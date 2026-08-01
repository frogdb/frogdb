# protocol — residual test gaps (8 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/08 — residual findings after promotion to issues 19–76
Score: 8 findings, priority range 7–16
Area: `frogdb-server/crates/protocol/` (`response.rs`, `command.rs`, `reply.rs`, `error/version/lib.rs`, `tests/proptest_protocol.rs`) **plus** the real byte-level RESP decoder `FrogDbResp2` in `frogdb-server/crates/server/src/connection/{codec,frame_io,util}.rs`, audited here by deliberate scope extension

## Context

The protocol crate holds the frame types and the RESP2/RESP3 encoders; the actual decoder that
sits on every inbound byte lives in the server crate and was pulled into scope because no other
round-2 agent owns it by name. Crate coverage is **lines 780/911 = 85.6%, regions 1270/1499 =
84.7%**, with depth classes over 173 functions of **44 `untested`**, **71 `single-test`**, **25
`monoculture`**, 33 `well-covered`, 0 hot-but-shallow — but the proposal is explicit that
percentage is not the signal: the covered lines are covered by *shape* assertions (`is_ok`,
"does not panic", "parses back within 1e-10") rather than byte assertions, and the
adversarial-input surface sits in a crate those numbers do not even include. The proposal's
verdict on the shape of that coverage: this area *"is small, pure, total-function-shaped, and
sits on 100% of traffic — which makes it the single best fit in FrogDB for property and fuzz
testing, and the worst fit for the example-test-per-case style it currently uses."* Nine named
invariants P1–P9 are enumerated in `proposals/08-protocol.md`; each residual finding below cites
the ones it would add.

**Structural note carried from the proposal's cross-area note 1 and `MASTER.md` §6 /
`INFRASTRUCTURE.md`:** the real RESP decoder is not in the protocol crate. Relocating
`FrogDbResp2` into `protocol` would drop **F2 and F5 from effort 2 to effort 1**, make the
decoder fuzzable as one public surface without a server dependency, and make "protocol coverage"
a meaningful number for the first time. That relocation is filed as issue 92,
`.scratch/testing-improvements-round2/issues/`. If the *server net / connection* agent also
claims F2/F5 (and F4, F6, F8, which also land in `connection/`), they should be merged as this
area's contribution, not duplicated.

## Promoted elsewhere

- F1 → issue 55, `.scratch/testing-improvements-round2/issues/` (`INCRBYFLOAT` stores a different float rendering than it replies) **and** issue 26, `.scratch/testing-improvements-round2/issues/` (theme T8 — five independent `format_float` implementations)
- F3 → issue 38, `.scratch/testing-improvements-round2/issues/` (RESP frame injection via unsanitised, client-controlled command name in error messages)
- F4 → issue 70, `.scratch/testing-improvements-round2/issues/` (unbounded allocations — unbounded RESP nesting depth, stack overflow → abort)
- F10 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — double-comparison tests use `< 1e-10` everywhere)

## Residual findings

### F2 — No partial-read / streaming-resumption property: a frame split at an arbitrary byte offset is never tested

- **Severity** 4 — a resumption bug in the hand-rolled pre-scan corrupts or desyncs the
  command stream: the wrong command executes, or the connection wedges. Silent wrong-write
  is in scope.
- **Likelihood** 3 — real TCP segments split large frames constantly (`MSET` with big
  values, `RESTORE`, pipelines crossing the 1460-byte MSS); the code path runs on every
  partially-arrived frame.
- **Effort** 2 — crate-level proptest over a `FrogDbResp2` and a `BytesMut`; no I/O needed.
  (Drops to **1** if `FrogDbResp2` is relocated into the protocol crate — issue 92,
  `.scratch/testing-improvements-round2/issues/`.)
- **Priority** 16
- **Evidence**: `crates/server/src/connection/codec.rs:104-206` runs *four* hand-rolled
  pre-scan stages against a possibly-partial `src` on every `decode` call — leading-CRLF
  strip (`:114`), inline detection on `src[0]` (`:132`), `*N` header parse via
  `find_crlf(src)` over the whole buffer (`:141-160`), `$N` header parse (`:170-184`), and
  `scan_for_oversized_bulk` (`:193-201`). Each has its own partial-buffer bail-out
  (`find_crlf(...)?`, `pos >= buf.len()`), and none of them is exercised with a truncated
  buffer by any test: the table at `codec.rs:562` feeds every case whole. The only
  resumption evidence in the area is implicit (`Framed` retrying), never asserted.
- **Proposed test**: proptest — generate an arbitrary valid command frame `f` (arbitrary
  arity, arbitrary binary payloads including embedded `\r\n`, `$0`, `$-1`); for a generated
  split index `i`, feed `bytes[..i]` (assert `Ok(None)` unless `i == len`), then
  `bytes[i..]`, and assert the decoded frame equals the whole-buffer decode **and** the
  residual buffer is empty. Extend to N-way splits and to a pipeline of frames split
  mid-frame. This is invariant P1.
- **Boundary**: 2 — `FrogDbResp2` is `pub(crate)` to the server crate today, so this is an
  inline `#[cfg(test)]` proptest in `codec.rs` rather than a `tests/` file. It directly
  exercises the behaviour with zero extra layers; a socket-level version would only add
  nondeterminism about where splits land.

### F5 — `FrogDbResp2` has no fuzz target: the RESP fuzz targets fuzz the dependency, not FrogDB (issue-40 residue)

- **Severity** 4 — this is the parser on every inbound byte; the class of bug fuzzing finds
  here is panic/hang/over-allocation, i.e. availability.
- **Likelihood** 2 — the specific escapes are adversarial, but the *coverage* claim
  ("we fuzz the protocol") is currently false, which is what makes this worth filing.
- **Effort** 2 — a new `libFuzzer` target is ~20 lines; the CI/corpus half is the work.
  (Drops to **1** if `FrogDbResp2` is relocated into the protocol crate — issue 92,
  `.scratch/testing-improvements-round2/issues/`.)
- **Priority** 14
- **Evidence**: `testing/fuzz/fuzz_targets/resp_parse.rs` and
  `testing/fuzz/fuzz_targets/resp_pipeline.rs` both call
  `redis_protocol::resp2::decode::decode_bytes` directly, so none of the pre-scan logic in
  `crates/server/src/connection/codec.rs:104-206` (inline commands, `sdssplitargs`,
  the count/length caps, `scan_for_oversized_bulk`) is ever fuzzed — every FrogDB-authored
  line in the decode path is outside the fuzzer's reach. Round-1 issue 40
  (`.scratch/testing-improvements/issues/40`) built the
  scaffolding, and its own generator documents the residue —
  `.github/workflows/workflow_gen/src/workflow_gen/workflows/fuzz.py`: *"The campaign's
  nightly cron is deliberately off — a 34-target, 180s-per-target run is ~100min of pure
  fuzzing every night. It stays available via workflow_dispatch."* The PR gate is
  `corpus-replay` with `-runs=0` and a restore-only cache, so it degrades to a no-op on a
  cold cache and the corpus only grows when a human manually dispatches. Local corpora are
  dated Mar 18 (811 files for `resp_parse`); `artifacts/` is empty for both RESP targets.
- **Proposed test**: a `fuzz_frogdb_codec` target that drives `FrogDbResp2::decode` over a
  `BytesMut`, feeding the fuzz input in **arbitrary-sized chunks** (derive the chunk
  schedule from the input itself) so it exercises resumption too, and asserting invariant
  P3 — terminates, no panic, and after any `Err` a subsequent well-formed frame still
  decodes. Seed the corpus from the existing `codec.rs` table cases. Separately: either
  re-enable a low-frequency campaign cron for the handful of security-relevant targets, or
  make `corpus-replay` fail loudly on a cold cache instead of silently passing.
- **Boundary**: 1/2 — the fuzz harness is the natural home; the CI half is shared
  infrastructure (see *Cross-area notes*).

### F6 — No inbound query-buffer limit: one connection can grow memory without bound

- **Severity** 4 — OOM kill of the whole process; also an eviction-pressure amplifier
  before that.
- **Likelihood** 2 — needs a client that declares a large frame and drips it, but that is
  also the shape of an ordinary stuck/slow client behind a proxy.
- **Effort** 3 — server integration test with a raw socket and timing control.
- **Priority** 13
- **Evidence**: `rg -in "query.?buffer|client.output.buffer" crates/config/src crates/server/src`
  returns only `pubsub_output_buffer_hard_limit` (`crates/config/src/server.rs:94`) and
  `maxmemory_clients` (`crates/config/src/memory.rs:70`, resolved at
  `crates/server/src/runtime_config.rs:3349` and used only for client **eviction**
  accounting, `runtime_config.rs:4457`). There is no equivalent of Redis's
  `client-query-buffer-limit` (default 1GB, enforced in `readQueryFromClient` →
  `closeClientOnOutputBufferLimitReached`'s query-buffer sibling). The per-element and
  per-count caps at `codec.rs:36` and `:31` bound a *single* declared value, not the sum:
  `*1048576\r\n` followed by a million `$1048576\r\n…` elements is entirely within both
  caps.
- **Proposed test**: open a raw socket to a `TestServer`, send `*1048576\r\n` then stream
  bulk elements, and assert the server closes the connection (or replies an error) once the
  accumulated query buffer crosses a configured limit — and that other connections stay
  healthy throughout.
- **Boundary**: 4 — the behaviour is "server drops a misbehaving connection under memory
  pressure", which only exists at the connection/lifecycle level.

### F7 — No encode→decode round-trip property for `WireResponse` in either protocol

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`).
`MASTER.md` §5 names `Response::Attribute` (no producer anywhere) among the dead code to delete,
and `Attribute` is one of the 16 `WireResponse` variants this property would generate and
round-trip; §5 cites no finding numbers, so it claims nothing on its own. If the variant is
deleted, the generator drops it — but the property itself stands over whatever variants remain.

- **Severity** 3 — a mis-encoded reply is a wrong answer the client cannot detect; the
  RESP2 downgrade table (16 variants) is the most refactor-fragile code in the crate.
- **Likelihood** 3 — any change to the downgrade table or a `redis-protocol` bump; both are
  ordinary maintenance events.
- **Effort** 2 — crate-level proptest, no server.
- **Priority** 13
- **Evidence**: the crate's encoders are covered only by per-variant example tests
  (`crates/protocol/src/response.rs:909-1594`), each constructing one frame and matching on
  its shape. Nothing feeds encoder output back through a decoder, so nothing catches a
  frame that encodes to bytes no decoder accepts — and the RESP2 arms do real, lossy work
  that deserves a stability property: `Map` flattens
  (`response.rs:252-261`), `Set`/`Push` collapse to `Array` (`:263`, `:272`), `VerbatimString`
  drops its format prefix (`:249`), `BigNumber` becomes a bulk string (`:276`), `Attribute`
  is stripped (`:266`), and nested `NullArray` becomes `$-1` (`:280`). `test_from_wire`
  (`response.rs:1540`) round-trips exactly two hand-built values.
- **Proposed test**: proptest over a recursive `WireResponse` generator (bounded depth,
  binary payloads, all 16 variants); assert (P2) RESP2 and RESP3 encodings both decode
  successfully and re-encode to identical bytes, and (P6)
  `Response::from_wire(r.into_wire().unwrap()) == r` for the non-action variants.
- **Boundary**: 2 — the crate's public API is exactly the right surface; a server-level
  version would only reach the variants some command happens to produce.

### F8 — `estimate_resp2_frame_size` is a hand-rolled duplicate of the encoder's length maths, never compared against it

- **Severity** 2 — wrong `INFO` `total_net_output_bytes`, which operators use for capacity
  planning and which feeds client-eviction accounting.
- **Likelihood** 4 — every RESP2 reply on default config goes through it.
- **Effort** 1 — pure unit/proptest.
- **Priority** 13
- **Evidence**: `crates/server/src/connection/util.rs:130-146` recomputes RESP2 encoded
  length independently of `redis-protocol`'s encoder and is the sole input to
  `add_bytes_sent` on both send paths —
  `crates/server/src/connection/frame_io.rs:82` and `:128`
  (`self.state.local_stats.add_bytes_sent(frame_size as u64)`). The RESP3 path, by
  contrast, measures the real buffer (`frame_io.rs:101`, `:143`), so RESP2 and RESP3
  connections report on different bases. No test compares the estimate to
  `extend_encode` output.
- **Proposed test**: proptest (P5) — for a generated `WireResponse`, assert
  `estimate_resp2_frame_size(f) == extend_encode(f).len()`. Include `NullArray` at top
  level, where `frame_io.rs:21` substitutes `RESP2_NULL_ARRAY.len()` instead.
- **Boundary**: 1 — arithmetic on a frame; no server involved.

### F9 — `ParsedCommand::try_from` silently *drops* non-bulk elements instead of rejecting

- **Severity** 3 — argument smuggling and parity divergence: FrogDB accepts and executes a
  request that Redis rejects with `Protocol error: expected '$', got ':'` **and closes the
  connection**. An intermediary (proxy, audit log, ACL pre-filter) that parses the same
  bytes correctly sees a different command than the one FrogDB runs.
- **Likelihood** 2 — a correct client never emits this; a buggy or hostile one does.
- **Effort** 1 — pure unit test.
- **Priority** 12
- **Evidence**: `crates/protocol/src/command.rs:57-67`:
  ```rust
  let args: Vec<Bytes> = iter
      .filter_map(|f| f.as_bytes().map(Bytes::copy_from_slice))
      .collect();
  ```
  `filter_map` discards `Integer`, `Null`, nested `Array`, and `SimpleString` elements
  without error, so arity checks downstream see a shorter argument list than the client
  sent. Worse, the property suite **pins this as intended**:
  `crates/protocol/tests/proptest_protocol.rs` — `null_elements_filtered` (*"Null elements
  in array should be filtered out"*, asserts `cmd.args.len() == 2` for a 3-element input)
  and `only_null_args` (asserts `cmd.args.is_empty()`). And
  `crates/redis-regression/tests/protocol_tcl.rs` (683 lines of protocol parity) has **no**
  case where a multibulk element is a valid non-bulk frame — it covers bad *lengths*
  (`$-10`, `$2000000000`) but never a well-formed `:5` in argument position.
- **Proposed test**: table of arrays whose elements include `Integer`, `Null`,
  `SimpleString`, `Error`, and a nested `Array` in both name and argument position; assert
  every one returns `Err(ProtocolError::…)`. Invariant P9: `1 + args.len()` equals the
  array arity or it is an error. Delete/invert the two proptests above as part of the fix.
- **Boundary**: see OPTIONS.
- **OPTIONS**:
  1. *Protocol-crate unit test on `ParsedCommand::try_from`* — cheapest, directly on the
     function that decides. But it pins the rejection at a layer the wire contract does not
     name, and says nothing about whether the connection is closed.
  2. *Server codec/dispatch unit test* — asserts the resulting `-ERR Protocol error` and
     connection teardown, which is the actual observable contract, without a socket.
  3. *`protocol_tcl.rs` parity case* — highest fidelity (a real Redis comparison exists
     there for every neighbouring case) and it is precisely the file whose gap this is.
  Recommendation: **(1) + (3)**. (1) is the regression pin for the parser contract, (3) is
  the parity oracle that keeps it honest and costs one test in an existing file.  Skip (2);
  it duplicates (3) at lower fidelity.

### F11 — Documented "cannot panic" contract is false: four `.expect()`s in the encoders

- **Severity** 3 — an unwinding panic in the connection task drops that client's
  connection (no `panic = "abort"` in the workspace manifests), and the surrounding docs
  assert this is impossible, so the invariant will be relied on.
- **Likelihood** 1 — requires non-UTF8 bytes inside a `WireResponse::Error`.
- **Effort** 1
- **Priority** 10
- **Evidence**: `crates/protocol/src/response.rs:217` says *"This method is total — and
  cannot panic — over every variant"* and `:295` says *"This method CANNOT panic"*, yet
  `:229` (`Str::from_inner(e).expect("error messages must be valid UTF-8")`), `:246`, `:303`,
  and `:904` (`.expect("cannot convert internal action to BytesFrame")`) can. `:246` is in
  fact safe (it feeds `String::from_utf8_lossy` output) but `:229`/`:303` take
  `WireResponse::Error(Bytes)` verbatim — and error text is built from client-controlled
  bytes at the six sites listed in F3. All four are `untested` regions.
- **Proposed test**: assert `WireResponse::Error(Bytes::from_static(b"ERR \xff\xfe"))`
  encodes to a valid frame in both protocols (lossy-converting like the `BlobError` arm
  already does) rather than panicking; and correct the two doc claims either way.
- **Boundary**: 1.

### F12 — RESP2 `Double` downgrade renders 17 significant digits — dead today, one `is_resp3` removal from live

- **Severity** 2 — wrong-looking but round-trippable values on a data path.
- **Likelihood** 1 — currently unreachable: every producer gates on RESP3
  (`crates/commands/src/utils.rs:835-840` `score_response`,
  `crates/commands/src/string.rs:768`, `crates/commands/src/sorted_set/basic.rs:411`,
  `crates/core/src/scripting/executor.rs:386-391`, `crates/core/src/shard/blocking.rs:1183`).
- **Effort** 1
- **Priority** 7
- **Evidence**: `crates/protocol/src/response.rs:239` —
  `WireResponse::Double(d) => Resp2BytesFrame::BulkString(Bytes::from(format_float(d)))`
  — using the crate-local `format_float` at `response.rs:876` (`{:.17}` + trim), which is
  the same algorithm as the F1 store-side bug. Reachable only if any producer stops
  gating.
- **Proposed test**: a unit test pinning the RESP2 downgrade bytes for `3.14`, `0.1`,
  `-0.0`, `inf` — which will immediately document the divergence and force the decision to
  either delete the arm or route it through the one true `format_float`.
- **Boundary**: 1.

### Deprioritised in the proposal (unnumbered — not counted as findings)

Carried verbatim so nothing in `proposals/08-protocol.md` is lost. The proposal gave these no
`F<n>` numbers, so they are outside the 12-finding arithmetic and carry no acceptance criterion.

- **`set_frame_attributes` (`protocol/src/response.rs:386`, 0/19 regions, 0 tests) and
  `Response::Attribute` / `WireResponse::Attribute`.** `rg` finds no producer anywhere in
  the workspace — no command constructs an attribute reply. Recommend **deleting the
  variant and the helper** rather than writing tests for dead code; if attributes are
  wanted later they come back with their producer and its tests. (`redis-regression`
  already declares RESP3 attributes an `intentional-incompatibility:protocol`.) This is
  the `Response::Attribute` row of `MASTER.md` §5 — see the overlap note on F7 above.
- **`Direction::parse` (`response.rs:434`, 0/11 regions).** Untested but a trivial two-arm
  string match with no adversarial input; a unit test is nearly free but would score
  Priority ~5. Fold into any future edit of that file rather than filing it.
- **`MapReply::with_capacity` (`reply.rs`) untested.** Pure `Vec::with_capacity`
  delegation; no behaviour to assert.
- **`ParsedCommand::name_uppercase` / `name_uppercase_string` (`command.rs:36`) untested,
  plus a stale SAFETY comment describing an unchecked-UTF8 implementation that no longer
  exists** (the code uses checked `String::from_utf8` with a lossy fallback). Fix the
  comment; the functions are exercised transitively by every dispatch test, so a dedicated
  test adds little. Worth noting only because a stale unsafety comment invites someone to
  "restore" the unchecked version.
- **Quadratic re-scan of one large multibulk under drip-fed reads.**
  `scan_for_oversized_bulk` (`codec.rs:252`) re-walks all *already-arrived* elements of the
  first frame on every `decode` call. Its doc comment (`codec.rs:225-250`) proves linearity
  across *pipelined frames*, which is true, but the argument does not cover repeated partial
  reads of a single 1M-element frame, where total work is quadratic in element count. Real
  (CPU-burn from one connection) but: Severity 3, Likelihood 2, Effort 3 → Priority 9, and
  a wall-clock-ratio test would be flaky. The honest fix is a memoized resume offset in the
  decoder plus a criterion bench; deprioritised until the F5 fuzz target or a bench shows
  it mattering in practice.
- **`*-1\r\n` inbound from a client.** FrogDB lets it through to the upstream decoder
  (`codec.rs:150-155` only special-cases `count < -1`), producing a `Null` frame that
  `ParsedCommand::try_from` rejects with `ExpectedArray` → an error reply, where Redis
  silently ignores it with no reply. A one-line parity divergence;
  `protocol_tcl.rs` covers the neighbouring negative-count cases, so add it there
  opportunistically rather than as its own finding.

## Acceptance criteria

- [ ] F2: a proptest asserts invariant P1 — for an arbitrary valid command frame and an arbitrary split index `i`, feeding `bytes[..i]` yields `Ok(None)` (unless `i == len`) and the subsequent `bytes[i..]` decodes to the same frame as the whole-buffer decode with an empty residual buffer; extended to N-way splits and to a pipeline split mid-frame.
- [ ] F5: a `fuzz_frogdb_codec` libFuzzer target exists that drives `FrogDbResp2::decode` over a `BytesMut` in input-derived arbitrary-sized chunks and asserts invariant P3 (terminates, no panic, and after any `Err` a subsequent well-formed frame still decodes), seeded from the `codec.rs` table cases; and `corpus-replay` either runs a real campaign or fails loudly on a cold cache instead of silently passing.
- [ ] F6: a test asserts that a raw socket declaring `*1048576\r\n` and streaming bulk elements is closed (or errored) once the accumulated query buffer crosses a configured limit, and that other connections stay healthy throughout.
- [ ] F7: a proptest over a recursive `WireResponse` generator (bounded depth, binary payloads, all variants) asserts P2 — RESP2 and RESP3 encodings both decode and re-encode to identical bytes — and P6 — `Response::from_wire(r.into_wire().unwrap()) == r` for the non-action variants.
- [ ] F8: a proptest asserts invariant P5 — `estimate_resp2_frame_size(f) == extend_encode(f).len()` for a generated `WireResponse` — including top-level `NullArray`, where `frame_io.rs:21` substitutes `RESP2_NULL_ARRAY.len()`.
- [ ] F9: a table asserts `ParsedCommand::try_from` returns `Err(ProtocolError::…)` for arrays containing `Integer`, `Null`, `SimpleString`, `Error` and nested `Array` elements in both name and argument position (invariant P9: `1 + args.len()` equals the array arity or it errors); the `null_elements_filtered` and `only_null_args` proptests are deleted or inverted; and `protocol_tcl.rs` gains the corresponding parity case for a well-formed `:5` in argument position.
- [ ] F11: a test asserts `WireResponse::Error(Bytes::from_static(b"ERR \xff\xfe"))` encodes to a valid frame in both RESP2 and RESP3 rather than panicking, and the "cannot panic" doc claims at `response.rs:217` and `:295` are corrected to match whichever behaviour is chosen.
- [ ] F12: a unit test pins the exact RESP2 `Double` downgrade bytes for `3.14`, `0.1`, `-0.0` and `inf`, and a decision is recorded to either delete the arm or route it through the one true `format_float`.

## Depends on

- issue 10, `.scratch/testing-improvements-round2/issues/` (I10 — fuzz CI; F5's second half is exactly this: the nightly cron was deliberately removed and the PR `corpus-replay` gate is `-runs=0` restore-only, so it silently no-ops on a cold cache across all 34 targets. The decision the proposal frames — weekly campaign, per-PR time-boxed security subset, or accept manual dispatch **and remove the "continuous" framing from the docs** — must be settled there, not here)
