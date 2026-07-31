# Pin RESP3 non-finite double wire format with raw-byte regression tests

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Protocol / Basic Commands

## Context

The original audit gap claimed RESP3 non-finite doubles (`inf`/`-inf`/`nan`) were untested and
potentially broken: RESP3 passes a raw `f64` into `Resp3BytesFrame::Double`
(`protocol/src/response.rs:320-323`) via the external `redis-protocol` crate's encoder, bypassing
FrogDB's own `format_float` special-casing (`response.rs:239,876-885`) that RESP2 uses for
`inf`/`-inf`/`nan`. All existing RESP3 Double tests use only finite values (`resp3.rs:370,401`;
RESP3 zset tests finite-only), and the ZSET test-exclusion header at `zset_tcl.rs:21` drops ±inf
tests under an "internal-encoding" label.

**Verified against the actual encoder — the headline is refuted, not confirmed.**
`redis-protocol` 6.0.0's RESP3 encoder (`resp3/encode.rs:108-120`, `f64_to_redis_string`) already
emits `,inf\r\n` / `,-inf\r\n` / `,nan\r\n` correctly, with its own upstream tests covering exactly
this (`encode.rs:1484-1506`). FrogDB is a pure passthrough at `response.rs:320-323` — there is no
live bug (verdict ADJUSTED L1/C2). The residual, real risk is purely regression exposure: if FrogDB
ever stops passing through the raw `f64` (adds its own RESP3 formatting layer, or the
`redis-protocol` dependency is upgraded/swapped and changes behavior), nothing today would catch a
silent break, because zero RESP3 tests touch a non-finite double.

## What to build

Regression-pin tests only — no encoder or dispatch code changes. Raw-byte wire assertions for RESP3
non-finite doubles across the commands that can emit them.

## Acceptance criteria

- [x] `HELLO 3; ZADD k inf m; ZSCORE k m` → raw-byte read of the wire response asserts the exact
      bytes `,inf\r\n`; repeat for a `-inf` member asserting `,-inf\r\n`; add `nan` coverage if
      reachable through any command path.
      (Note: ZSCORE/ZMSCORE do NOT actually take the Double path for non-finite scores in the
      current implementation — see Resolution below. Pinned as `$3\r\ninf\r\n` /
      `$4\r\n-inf\r\n` bulk strings instead, with the naively-expected Double assertion documented
      in the test comment. NaN is unreachable via any command path — pinned directly at the
      encoder in `response.rs` unit tests, plus a command-level test confirming NaN results are
      rejected as errors before they can reach the wire.)
- [x] Extend coverage to `ZINCRBY` producing ±inf and `ZADD ... INCR` paths.
      (ZINCRBY genuinely emits RESP3 Double `,inf\r\n`/`,-inf\r\n`/`,3\r\n` — pinned as expected.
      ZADD INCR is NOT RESP3-aware at all in the current implementation and never emits Double —
      pinned as current/bulk-string behavior, flagged as a follow-up gap.)
- [x] Test comments document this is a regression guard, not a bug fix, citing `redis-protocol`
      6.0.0 `resp3/encode.rs:108-120` as the correct-by-construction encoder and
      `response.rs:320-323` as FrogDB's passthrough call site.
- [x] The "internal-encoding" exclusion label at `zset_tcl.rs:21` reviewed and corrected if it drops
      ±inf coverage from ported upstream tests — wire format correctness is portable protocol
      behavior, not internal encoding, and should not be excluded on that basis.
      (Corrected: the exclusion covered ZUNIONSTORE/ZINTERSTORE SUM-aggregate ±inf folding, which
      is portable protocol behavior, not internal encoding. Un-excluded and ported.)

## Resolution

**Verdict confirmed, no live encoder bug.** Direct inspection of `redis-protocol` 6.0.0
(`resp3/utils.rs:595-607`, `f64_to_redis_string`) confirms it already special-cases NaN → `"nan"`,
±infinity → `"inf"`/`"-inf"`, and otherwise uses `data.to_string()` (which already omits a
trailing `.0` for integer-valued floats, e.g. `3.0.to_string() == "3"`). FrogDB's passthrough at
`response.rs:320-323` is correct by construction. This task adds regression-pin tests only; no
encoder or dispatch code was changed.

**Tests added:**

1. `frogdb-server/crates/protocol/src/response.rs` — 5 new unit tests directly encoding
   `WireResponse::Double` via `redis_protocol::resp3::encode::complete::extend_encode` and
   asserting raw bytes: `test_resp3_double_positive_infinity_wire_bytes` (`,inf\r\n`),
   `test_resp3_double_negative_infinity_wire_bytes` (`,-inf\r\n`),
   `test_resp3_double_nan_wire_bytes` (`,nan\r\n` — pinned directly at the encoder since NaN is
   unreachable through any live command), `test_resp3_double_integer_valued_wire_bytes`
   (`,3\r\n`, `,-3\r\n`, `,0\r\n` — confirms no trailing `.0`), and
   `test_resp3_double_finite_fractional_wire_bytes` (`,3.125\r\n`).

2. `frogdb-server/crates/server/tests/resp3.rs` — 9 new `#[tokio::test]` raw-TCP tests (new
   `encode_resp_command`/`connect_resp3_raw`/`send_raw_command` helpers, bypassing the
   `redis_protocol` codec client-side to assert exact wire bytes) covering `ZINCRBY` (+inf, -inf,
   NaN-result rejection, integer-valued), `ZSCORE` (finite, +inf, -inf), and `ZADD ... INCR`
   (finite, +inf).

3. `frogdb-server/crates/redis-regression/tests/zset_tcl.rs` — removed the
   `$cmd with +inf/-inf scores - $encoding` line from the "internal-encoding" exclusion list (it
   was bucketed there purely by its `- $encoding` name suffix, but its actual assertions test
   ZUNIONSTORE/ZINTERSTORE SUM-aggregate ±inf folding — portable protocol behavior). Ported the
   real upstream test (`redis/redis tests/unit/type/zset.tcl`, verified against the actual
   GitHub source) as `tcl_zunionstore_zinterstore_with_inf_scores`, plus a doc-comment note
   explaining the correction. FrogDB's existing `apply_aggregate` (`set_ops.rs:115-122`) already
   implements the Redis-specific inf+(-inf)→0 special case correctly; the test passes with no
   implementation changes.

**Two pre-existing dispatch-consistency gaps discovered and documented (NOT fixed — out of
scope, "no encoder or dispatch code changes" per task brief):**

- `ZSCORE`/`ZMSCORE` route non-finite scores through `commands/src/utils.rs::score_response`,
  which gates `Response::Double` to `is_resp3 && score.is_finite()` — i.e. even under RESP3, a
  non-finite score falls back to a RESP2-style bulk string (`$3\r\ninf\r\n`), never `,inf\r\n`.
  This diverges from real Redis, whose `addReplyDouble` emits RESP3 Double for ZSCORE of inf
  unconditionally. The acceptance criteria's assumed `,inf\r\n` result for ZSCORE is therefore
  incorrect for FrogDB's current behavior; tests pin the actual (bulk-string) behavior and
  document the discrepancy inline.
- `ZADD ... INCR` (`commands/src/sorted_set/basic.rs`) unconditionally returns
  `Response::bulk(format_float(new_score))` regardless of `ctx.protocol_version` — it is not
  RESP3-aware at all, unlike `ZINCRBY` which correctly branches on `is_resp3()`. Pinned as
  current (bulk-string) behavior.

Recommend filing a follow-up issue to make `score_response` and `ZADD ... INCR` RESP3-consistent
with `ZINCRBY` (emit `Response::Double` under RESP3 regardless of finiteness, matching real
Redis). Not done here per this task's explicit scope restriction.

**Verification:** `just fmt` (no diff) and `cargo clippy --tests -D warnings` clean on
`frogdb-protocol`, `frogdb-server` (lib + tests), and `frogdb-redis-regression` (lib + tests).
Each of the three new/modified test suites run 3 consecutive times, all green:
- `just test frogdb-protocol test_resp3_double` → `5 tests run: 5 passed, 63 skipped` (x3)
- `just test frogdb-server 'resp3::test_zincrby|resp3::test_zscore_resp3|resp3::test_zadd_incr|resp3::test_null_array|resp3::test_pipelined'` → `13 tests run: 13 passed, 1926 skipped` (x3)
- `just test frogdb-redis-regression tcl_zunionstore_zinterstore_with_inf_scores` → `1 test run: 1 passed, 2290 skipped` (x3)

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/A-basic-commands.md #3 (`resp3-double-non-finite-wire-format-untested`)
- .scratch/testing-improvements/audit/verdicts-A.md #3 (ADJUSTED L1/C2 — encoder correct upstream)
- protocol/src/response.rs:239,320-323,876-885
- frogdb-server/crates/redis-regression/tests/resp3.rs:370,401
- frogdb-server/crates/redis-regression/tests/zset_tcl.rs:21
