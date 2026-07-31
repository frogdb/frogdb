# frogdb-protocol — testing gap audit (round 2)

## Scope

Audited:

| path | LOC | note |
|---|---|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1594 (908 src + 686 inline test) | `Response`/`WireResponse`, RESP2+RESP3 encoders, `format_float` |
| `frogdb-server/crates/protocol/src/command.rs` | 104 (73 src + 31 test) | `ParsedCommand::try_from(BytesFrame)` |
| `frogdb-server/crates/protocol/src/reply.rs` | 168 (83 src + 85 test) | `MapReply` RESP2-flatten/RESP3-map seam |
| `frogdb-server/crates/protocol/src/{error,version,lib}.rs` | 69 | |
| `frogdb-server/crates/protocol/tests/proptest_protocol.rs` | 323 | the crate's only `tests/` file |

**Extended scope (deliberate).** The protocol *crate* contains only frame types and
encoders. The actual byte-level RESP **decoder** — `FrogDbResp2`, the thing that sits on
every inbound byte — lives in `frogdb-server/crates/server/src/connection/codec.rs` (1022
lines, 532 of them inline tests) with its outbound half in
`.../connection/frame_io.rs` (283). No other round-2 agent owns it by name and it is
inseparable from "the protocol area", so it is audited here. See *Cross-area notes*.

Coverage (`target/llvm-cov/depth/depth.json`, crate `protocol`):

- lines 780/911 = **85.6%**; regions 1270/1499 = 84.7%
- depth classes over 173 functions: **44 untested**, **71 single-test**, **25
  monoculture**, **33 well-covered**, 0 hot-but-shallow

Percentage is not the signal here. The signal is that the covered lines are covered by
*shape* assertions (`is_ok`, "does not panic", "parses back within 1e-10") rather than
byte assertions, and that the adversarial-input surface is concentrated in a crate the
coverage numbers above do not even include.

## Summary

This area is unusual in the workspace: it is small, pure, total-function-shaped, and sits
on 100% of traffic — which makes it the single best fit in FrogDB for property and fuzz
testing, and the worst fit for the example-test-per-case style it currently uses. The
bugs that escape today are of three kinds. (1) **Rendering divergences** that are invisible
to "assert within epsilon" tests: there are *five* independent `format_float`
implementations in this workspace and `INCRBYFLOAT` demonstrably **stores one rendering and
replies with another** (`GET` after `INCRBYFLOAT k 0.1` returns
`0.10000000000000001` where the command replied `0.1`). (2) **Adversarial-input handling**:
the decoder caps multibulk count and bulk length exactly like Redis but caps *nothing*
about nesting depth, total query-buffer size, or CRLF content of error text — so
`*1\r\n*1\r\n*1\r\n…` recurses unboundedly into `redis-protocol`'s nom parser, a command
name containing `\r\n` is echoed verbatim into `-ERR unknown command '…'` and splits the
reply stream, and a drip-fed frame can grow one connection's read buffer without limit.
(3) **Streaming resumption**: there is no test anywhere that a frame split at an arbitrary
byte offset reassembles identically, despite `FrogDbResp2::decode` doing its own
hand-rolled pre-scan of a possibly-partial buffer on every call.

The existing inline codec tests are genuinely good (table-driven, they pin the
consume-on-error residual-buffer contract byte-for-byte) — the gap is not diligence, it is
that example tests cannot enumerate this input space. Round 1's issue 40 built the fuzz
CI scaffolding but the campaign cron was removed and neither RESP fuzz target exercises
`FrogDbResp2`; they call `redis_protocol::resp2::decode::decode_bytes` directly, i.e. they
fuzz the dependency, not FrogDB.

### Is property/fuzz the right primary tool here? (dispatch question)

**Yes — for the decoder and the encoders, property and fuzz testing should be the primary
tool, with example tests demoted to regression pins for specific historical bugs.**
Justification: the input domain is `&[u8]` (unbounded, adversary-chosen), the output
domain is a small closed enum, and every desirable behaviour is expressible as a
*universally quantified* statement rather than a table row. That is the textbook
precondition. Example tests should remain only where a specific wire-byte string is the
contract (Redis parity bytes, round-1's issue-54 non-finite double pins) — those are
*pins*, not coverage.

Proposed invariant set (each is cited by the finding that would add it):

| # | invariant | finding |
|---|---|---|
| P1 | For all frames `f` and all split points `0 ≤ i ≤ len`: feeding `bytes(f)[..i]` then `bytes(f)[i..]` to one `FrogDbResp2` yields exactly the same `Some(f)` (and the same residual buffer) as feeding it whole. | F2 |
| P2 | For all `WireResponse` `r`: `decode(encode_resp2(r))` and `decode(encode_resp3(r))` succeed and re-encode to identical bytes (round-trip stability, not structural equality — RESP2 downgrades are lossy by design). | F7 |
| P3 | For all `&[u8]` inputs: `FrogDbResp2::decode` terminates, never panics, never allocates more than `O(bytes supplied)`, and on `Err` leaves the buffer in a state where a subsequent valid frame decodes (the consume-on-error contract). | F5 |
| P4 | For all inputs: recursion depth of decode and of `to_resp2_frame`/`to_resp3_frame` is bounded by a constant independent of input length. | F4 |
| P5 | For all `WireResponse` `r`: `estimate_resp2_frame_size(r.to_resp2_frame()) == encoded_len(r.to_resp2_frame())`. | F8 |
| P6 | For all `Response` `r` with no internal action: `Response::from_wire(r.clone().into_wire().unwrap()) == r`. | F7 |
| P7 | For all `f64` `x` that FrogDB can store: the string FrogDB *stores* for `x` equals the string FrogDB *replies* for `x`, and both round-trip back to `x`. | F1 |
| P8 | For all error `Response`s: the encoded bytes contain exactly one `\r\n`, at the end. | F3 |
| P9 | For all decoded `BytesFrame`s: `ParsedCommand::try_from` either returns a command whose `1 + args.len()` equals the array arity, or returns `Err`. Never silently fewer. | F9 |

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `protocol/tests/proptest_protocol.rs` (323 L, 1000 cases each) | `ParsedCommand::try_from` over generated `BytesFrame`s | runs real generated input; catches panics | almost every assertion is `is_ok()` / `.len() ==` shape. No byte-level generation, no round-trip, no split-resumption. Two tests (`null_elements_filtered:~180`, `only_null_args:~200`) actively **cement** the F9 divergence as intended behaviour |
| `protocol/src/response.rs` inline (39 tests) | per-variant RESP2/RESP3 frame shapes, `into_wire`/`from_wire`, issue-54 non-finite double **wire bytes** (`:1470-1540`) | the issue-54 block is exemplary: asserts exact bytes and documents *why* the path is a passthrough | `set_frame_attributes` 0/19 regions; `Direction::parse` 0/11; `test_double_to_resp2_string:1039` parses the string back and asserts `< 1e-10` — cannot detect an 18-significant-digit rendering change |
| `protocol/src/command.rs` inline (3 tests) | empty array → `EmptyCommand`, non-array → `ExpectedArray` | correct negatives for the two rejections that *do* exist | nothing on non-bulk *elements*; `name_uppercase`/`name_uppercase_string` untested (0 regions) |
| `protocol/src/reply.rs` inline (4 tests) | `MapReply` flatten vs map | both protocol arms asserted | `with_capacity` untested; `field_if` monomorphisations from `connection::hotkeys` untested |
| `server/src/connection/codec.rs` inline (532 L, `mod tests:490-1022`) | **the strongest suite in the area**: table-driven `decode_edge_cases_table:562` over negative/oversized counts, oversized bulk, inline commands, quote handling, and the residual-buffer state after each error | pins the consume-on-error contract; faithful `sdssplitargs` port at `:391` with its own tests | no nesting depth case; no partial-read/resumption case; no adversarial timing/quadratic case; no fuzz target reaches this type |
| `server/src/connection/frame_io.rs` inline (2 tests) | RESP2 + RESP3 null-array **feed order** byte streams | exact-bytes assertions over a real duplex | `estimate_resp2_frame_size` never compared against actual output |
| `server/tests/resp3.rs` (926 L) | HELLO negotiation, per-protocol map/set/double/push shapes, null-array bytes | end-to-end protocol-version matrix | `:385` and `:412` assert doubles `.abs() < 1e-10` — the two places a rendering regression would be caught, and they are epsilon-blind |
| `redis-regression/tests/protocol_tcl.rs` (683 L) | socket-level Redis parity: empty query, negative/out-of-range multibulk, bad bulk length, unbalanced quotes, inline forms, big number, verbatim | real parity oracle at the right boundary | **no case where a multibulk element is a valid non-bulk frame** (`:5`, `+x`, `$-1`, nested `*`) — exactly F7's hole |
| `testing/fuzz/` (34 targets) | `resp_parse.rs`, `resp_pipeline.rs` | targets exist and build | both call `redis_protocol::resp2::decode::decode_bytes` — **`FrogDbResp2` is never fuzzed**. Corpus gitignored; local copies last written Mar 18 |

## Findings

### F1: `INCRBYFLOAT` stores a different float rendering than it replies (5 divergent `format_float`s)

- **Severity** 3 — a subsequent `GET` returns a different string than the command that wrote it returned, and the ugly string is what persists to RDB/AOF and replicates. Compounds across repeated increments.
- **Likelihood** 4 — default config, no special setup: any `INCRBYFLOAT` whose result is not exactly representable (`0.1`, `3.14`, `1.1`) on an **existing** key.
- **Effort** 1 — a unit assertion on `StringValue::increment_float`'s stored bytes.
- **Priority** 16
- **Evidence**: the reply uses the ryu-based `commands::utils::format_float`
  (`crates/commands/src/utils.rs:31-62`, minimal round-trip representation) —
  `crates/commands/src/string.rs:767-771`:
  ```rust
  let new_val = sv.increment_float(delta)?;
  if is_resp3 { Ok(Response::Double(new_val)) }
  else { Ok(Response::bulk(Bytes::from(format_float(new_val)))) }   // commands::utils
  ```
  but the *store* inside `increment_float` uses a different function —
  `crates/types/src/types/string_value.rs:211` writing
  `format_float` from `crates/types/src/types/string_value.rs:338-354`:
  ```rust
  let s = format!("{:.17}", f);
  let s = s.trim_end_matches('0');
  ```
  So `SET k 0; INCRBYFLOAT k 0.1` replies `"0.1"` and stores `"0.10000000000000001"`.
  Redis renders both with `d2string`/`fpconv_dtoa` (shortest round-trip) and they agree.
  Three further variants exist: `crates/protocol/src/response.rs:876`,
  `crates/core/src/shard/timeseries_execution.rs:352`,
  `crates/commands/src/timeseries.rs:1370`. The existing regression tests miss this because
  they only use exactly-representable values —
  `crates/redis-regression/tests/incr_tcl.rs:169-240` uses `1`, `0.25`, `1.5`,
  `17179869184`; `crates/server/tests/property_tests.rs:210` asserts "within epsilon".
- **Proposed test**: (a) unit: for a table of `f64`s including `0.1`, `3.14`, `1e-7`,
  `-0.0`, `1e17`, `1e-320`, assert `StringValue::increment_float` stores exactly
  `commands::utils::format_float(new_val)` (invariant P7); (b) `shard_driver`:
  `SET k 0` → `INCRBYFLOAT k 0.1` → assert reply bytes == `GET k` bytes.
  Then a follow-up test asserting there is exactly one `format_float` in the workspace.
- **Boundary**: 1 for (a) — pure rendering, no engine needed; 3 for (b) — needs real
  command dispatch + store, but no socket.
- **Cross-area**: the fix lands in `types`/`commands`; surfaced here because it is a float
  *rendering* bug and the protocol crate owns one of the five copies.

### F2: No partial-read / streaming-resumption property — a frame split at an arbitrary byte offset is never tested

- **Severity** 4 — a resumption bug in the hand-rolled pre-scan corrupts or desyncs the
  command stream: the wrong command executes, or the connection wedges. Silent wrong-write
  is in scope.
- **Likelihood** 3 — real TCP segments split large frames constantly (`MSET` with big
  values, `RESTORE`, pipelines crossing the 1460-byte MSS); the code path runs on every
  partially-arrived frame.
- **Effort** 2 — crate-level proptest over a `FrogDbResp2` and a `BytesMut`; no I/O needed.
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

### F3: Error messages are not CRLF-sanitized — client-controlled bytes split the reply stream

- **Severity** 4 — RESP response splitting. The client's *next* reply read consumes
  attacker-authored frames, so a pooled client library can attribute a forged `+OK` to a
  later command on the same connection. This is a protocol-level confused-deputy, not just
  a cosmetic formatting issue.
- **Likelihood** 2 — requires a deliberately malformed command name/argument; not hit by
  well-behaved clients, but reachable pre-auth on any exposed port.
- **Effort** 1 — a unit test on the error encoder plus one socket-level parity test.
- **Priority** 15
- **Evidence**: the command name is client-controlled binary and is interpolated raw:
  `crates/server/src/connection/guards.rs:485` — `"ERR unknown command '{cmd_name}', with
  args beginning with:"` — and identically at `guards.rs:540`,
  `crates/server/src/connection/routing.rs:44-46`,
  `crates/core/src/shard/execution.rs:134`, `crates/core/src/shard/scripting.rs:193`,
  `crates/core/src/scripting/gate.rs:423`. The encoder writes the string verbatim —
  `redis-protocol-6.0.0/src/resp2/encode.rs`:
  ```rust
  fn gen_error(x, data: &str) { do_gen!(x, gen_be_u8!(Error.to_byte()) >> gen_slice!(data.as_bytes()) >> gen_slice!(CRLF)) }
  ```
  Redis maps CR/LF to spaces before replying (`sdsmapchars(s, "\r\n", "  ", 2)` in
  `addReplyErrorFormatInternal`). `rg` finds no equivalent anywhere in this workspace.
  So `*1\r\n$10\r\nAB\r\n+OK\r\nX\r\n` yields
  `-ERR unknown command 'AB\r\n+OK\r\nX', with args beginning with:\r\n` — three frames on
  the wire where the client expects one.
- **Proposed test**: (a) unit at the protocol boundary: for a table of error payloads
  containing `\r`, `\n`, `\r\n`, and a trailing newline, assert the encoded RESP2 and RESP3
  bytes contain exactly one terminating `\r\n` and no interior CR/LF (invariant P8);
  (b) socket: send a command name containing `\r\n+OK\r\n`, then `PING`, and assert the
  reply stream is exactly one error frame followed by exactly one `+PONG`.
- **Boundary**: 1 for (a) — pure encoding. 4 for (b) — the desync only manifests as a
  *stream* property across two commands on one connection, which needs a real socket; it
  belongs in `redis-regression/tests/protocol_tcl.rs` next to the existing parity cases.

### F4: Unbounded RESP nesting depth — `*1\r\n*1\r\n…` recurses until the process segfaults

- **Severity** 4 — stack overflow is `SIGSEGV`/abort, not an unwind: it kills the whole
  server process and every other connection with it. No `panic = "abort"` is configured, so
  ordinary panics are survivable — this one is not.
- **Likelihood** 2 — adversarial, but pre-auth reachable, needs only a few hundred KB of
  input from one connection, and no configuration makes it safe.
- **Effort** 2 — a unit test, but it must run on a thread with a deliberately small stack
- **Priority** 14
  so the failure is observable rather than fatal (see OPTIONS).
- **Evidence**: the codec's `*` handling validates only the element *count* and then falls
  through — `crates/server/src/connection/codec.rs:141-163` — and
  `scan_for_oversized_bulk` explicitly bails out on a non-`$` element
  (`codec.rs:262`: `if pos >= buf.len() || buf[pos] != b'$' { return None; }`), so nested
  `*` elements go straight to `self.inner.decode(src)` at `codec.rs:207`. Upstream is
  plainly recursive with no depth parameter —
  `redis-protocol-6.0.0/src/resp2/decode.rs`:
  ```rust
  fn d_parse_array_frames(input, len) -> DResult<Vec<RangeFrame>> { nom_count(d_parse_frame, len)(input) }
  fn d_parse_frame(...) { match kind { FrameKind::Array => d_parse_array(...), ... } }
  ```
  The **encode** direction has the same shape: `crates/protocol/src/response.rs:234` and
  `:255-262` recurse through `Array`/`Map`/`Set` with no depth bound, reachable from a Lua
  script returning a deeply nested table (`crates/core/src/scripting/executor.rs:393`,
  itself recursive). Redis bounds this implicitly — `processMultibulkBuffer` is iterative
  and rejects a non-`$` element outright, so nesting is *unrepresentable* in a client
  request.
- **Proposed test**: spawn a `std::thread::Builder::new().stack_size(256 * 1024)` thread,
  feed `"*1\r\n".repeat(N)` for N well past any legitimate depth, and assert
  `decode` returns `Err(DecodeError)` rather than overflowing. Mirror it for
  `to_resp2_frame`/`to_resp3_frame` on a synthetic deep `WireResponse`. Invariant P4. Note
  this test is **red today** and stays red until a depth cap exists — file it as a bug with
  the test attached, not as a coverage gap.
- **Boundary**: 1 — pure decoder/encoder behaviour; a socket-level version would risk
  killing the test harness process.
- **OPTIONS**:
  1. *Bounded-stack thread unit test* (recommended). Deterministic, fast, no process risk.
     Cost: the 256 KB figure is a magic number that must be documented, and it proves
     "bounded" only relative to that stack.
  2. *`#[should_panic]`/abort-capturing integration test.* Honest about the real failure
     mode but a stack overflow aborts rather than panics, so it needs a subprocess
     harness — new infrastructure for one assertion.
  3. *Fuzz-only* — let the `FrogDbResp2` fuzz target from F5 find it. Zero incremental
     cost, but non-deterministic and gives no regression pin.
  Recommendation: (1) as the pin, with (3) as the ongoing net.

### F5: `FrogDbResp2` has no fuzz target — the RESP fuzz targets fuzz the dependency, not FrogDB (issue-40 residue)

- **Severity** 4 — this is the parser on every inbound byte; the class of bug fuzzing finds
  here is panic/hang/over-allocation, i.e. availability.
- **Likelihood** 2 — the specific escapes are adversarial, but the *coverage* claim
  ("we fuzz the protocol") is currently false, which is what makes this worth filing.
- **Effort** 2 — a new `libFuzzer` target is ~20 lines; the CI/corpus half is the work.
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

### F6: No inbound query-buffer limit — one connection can grow memory without bound

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

### F7: No encode→decode round-trip property for `WireResponse` in either protocol

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

### F8: `estimate_resp2_frame_size` is a hand-rolled duplicate of the encoder's length maths, never compared against it

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

### F9: `ParsedCommand::try_from` silently *drops* non-bulk elements instead of rejecting

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
  the parity oracle that keeps it honest and costs one test in an existing file. Skip (2);
  it duplicates (3) at lower fidelity.

### F10: Assertion-weak double tests cannot see a rendering change (`< 1e-10` everywhere)

- **Severity** 2 — on its own this is a test-quality defect; its consequence is that F1 and
  any future float-rendering regression ship silently.
- **Likelihood** 3 — the tests run on every CI job and give false assurance now.
- **Effort** 1 — rewrite existing assertions to compare bytes.
- **Priority** 11
- **Evidence**: `crates/protocol/src/response.rs:1039` (`test_double_to_resp2_string`)
  parses the encoded string back to `f64` and asserts a `1e-10` epsilon — it passes whether
  the encoder emits `3.14` or `3.14000000000000012`. Same pattern at
  `crates/server/tests/resp3.rs:385` (`(data - 3.14159).abs() < 1e-10`) and `:412`
  (`(data - 3.14).abs() < 1e-10`), and `crates/server/tests/property_tests.rs:210`
  (`test_incrbyfloat_precision`, "within epsilon"). Contrast with the issue-54 block at
  `response.rs:1470-1540`, which asserts exact bytes (`b",inf\r\n"`, `b",3\r\n"`) and is
  the model to follow.
- **Proposed test**: convert each of the above to exact-byte assertions against a table of
  `f64` values, and add `-0.0` (Redis preserves the sign; both FrogDB variants return
  `"0"`), `1e-320` (subnormal — `{:.17}` collapses it to `"0"`), and `1e300` (`{:.17}`
  expands to 300 digits where Redis emits `1e+300`).
- **Boundary**: 1 — these are rendering assertions; the `resp3.rs` ones stay at 4 because
  they also assert the protocol-version dispatch, but their float comparison becomes exact.

### F11: Documented "cannot panic" contract is false — four `.expect()`s in the encoders

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

### F12: RESP2 `Double` downgrade renders 17 significant digits — dead today, one `is_resp3` removal from live

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

## Deprioritised

- **`set_frame_attributes` (`protocol/src/response.rs:386`, 0/19 regions, 0 tests) and
  `Response::Attribute` / `WireResponse::Attribute`.** `rg` finds no producer anywhere in
  the workspace — no command constructs an attribute reply. Recommend **deleting the
  variant and the helper** rather than writing tests for dead code; if attributes are
  wanted later they come back with their producer and its tests. (`redis-regression`
  already declares RESP3 attributes an `intentional-incompatibility:protocol`.)
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

## Cross-area notes

1. **`FrogDbResp2` ownership.** `crates/server/src/connection/codec.rs` +
   `frame_io.rs` + `util.rs:130` are the real protocol implementation but live in the
   server crate. F2, F4, F5, F6, F8 all land there. If the *server net/connection* agent
   also claims them, F2/F4/F5 should be treated as the protocol area's contribution and
   merged, not duplicated. A cleaner long-term move is to relocate `FrogDbResp2` into the
   `protocol` crate so the decoder and encoders are testable (and fuzzable) as one public
   surface without a server dependency — that alone would drop F2/F5 from Effort 2 to
   Effort 1 and make the coverage numbers for "the protocol" meaningful.
2. **Float rendering is a workspace-wide invariant, not a protocol one.** Five
   `format_float` implementations exist: `commands/src/utils.rs:31` (ryu — the correct
   one), `types/src/types/string_value.rs:338`, `protocol/src/response.rs:876`,
   `core/src/shard/timeseries_execution.rs:352`, `commands/src/timeseries.rs:1370`. F1 is
   the proven live bug; the *types*, *core-type commands*, and *extended-type commands*
   agents will likely each see a shard of it. Recommend one shared decision: collapse to a
   single `format_float` in a low-level crate and add a workspace test that asserts no
   second definition exists.
3. **Fuzz CI is shared infrastructure.** F5's second half (campaign cadence, corpus
   persistence that does not silently no-op on a cold cache) is the residue of round-1
   issue 40 and affects all 34 targets, not just RESP. Whoever owns CI should decide
   between a weekly campaign, a per-PR time-boxed run for a security-critical subset, or
   accepting manual dispatch and removing the "continuous" framing from the docs.
4. **`protocol_tcl.rs` is the right home for parity findings.** F3(b) and F9(3) both add
   cases to `crates/redis-regression/tests/protocol_tcl.rs`; the redis-parity agent should
   be told these are incoming so the file is not restructured underneath them.
5. **No source or test file was modified during this audit**, per the brief.
