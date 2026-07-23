# Pin RESP3 non-finite double wire format with raw-byte regression tests

Status: needs-triage
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

- [ ] `HELLO 3; ZADD k inf m; ZSCORE k m` → raw-byte read of the wire response asserts the exact
      bytes `,inf\r\n`; repeat for a `-inf` member asserting `,-inf\r\n`; add `nan` coverage if
      reachable through any command path.
- [ ] Extend coverage to `ZINCRBY` producing ±inf and `ZADD ... INCR` paths.
- [ ] Test comments document this is a regression guard, not a bug fix, citing `redis-protocol`
      6.0.0 `resp3/encode.rs:108-120` as the correct-by-construction encoder and
      `response.rs:320-323` as FrogDB's passthrough call site.
- [ ] The "internal-encoding" exclusion label at `zset_tcl.rs:21` reviewed and corrected if it drops
      ±inf coverage from ported upstream tests — wire format correctness is portable protocol
      behavior, not internal encoding, and should not be excluded on that basis.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/A-basic-commands.md #3 (`resp3-double-non-finite-wire-format-untested`)
- .scratch/testing-improvements/audit/verdicts-A.md #3 (ADJUSTED L1/C2 — encoder correct upstream)
- protocol/src/response.rs:239,320-323,876-885
- frogdb-server/crates/redis-regression/tests/resp3.rs:370,401
- frogdb-server/crates/redis-regression/tests/zset_tcl.rs:21
