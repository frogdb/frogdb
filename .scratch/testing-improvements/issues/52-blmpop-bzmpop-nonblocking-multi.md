# Extend MULTI and Lua non-blocking regressions to cover BLMPOP/BZMPOP (and XREAD BLOCK in Lua)

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Transactions / Scripting

## Context

`tcl_blocking_commands_ignore_timeout_in_multi`
(`frogdb-server/crates/redis-regression/tests/multi_tcl.rs:516-578`) queues and EXEC-verifies a nil/
null-array result for BLPOP, BRPOP, BRPOPLPUSH, BLMOVE, BZPOPMIN, BZPOPMAX, `XREAD BLOCK`, and
XREADGROUP — proving these blocking commands run non-blocking with timeout ignored inside MULTI. It
omits `BLMPOP` and `BZMPOP`, both of which exist and are registered blocking commands
(`frogdb-server/crates/commands/src/blocking.rs`; registered in `register.rs`;
non-blocking-in-MULTI code paths at `blocking.rs:307-318,614-625`).

The Lua sibling suite (`scripting_tcl.rs:462-576`,
`tcl_eval_scripts_do_not_block_on_*`) has the same gap: BLMPOP/BZMPOP/`XREAD BLOCK` are missing from
the set of blocking commands verified non-blocking when called from a script.

Verdict CONFIRMED L1/C2: likely shares the generic deny-blocking code path already exercised by
sibling commands, so probably not a live bug — but if either command slipped the shared path, EXEC
(or a script) would hang forever on an empty key, which is a real availability risk worth a cheap
extension to close.

## What to build

Extend both existing regression suites with the missing commands.

## Acceptance criteria

- [ ] `multi_tcl.rs` `tcl_blocking_commands_ignore_timeout_in_multi` extended with
      `BLMPOP 0 1 empty LEFT` and `BZMPOP 0 1 emptyz MIN` against empty keys, asserting a
      null-array result at EXEC.
- [ ] `scripting_tcl.rs` Lua non-blocking suite extended with BLMPOP, BZMPOP, and `XREAD BLOCK`,
      asserting non-blocking behavior when called from a script.
- [ ] Both extended tests run within a tight timeout so a hang is caught as a test failure, not just
      correctness of the returned value.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #5 (`blmpop-bzmpop-nonblocking-in-multi-omitted`)
- .scratch/testing-improvements/audit/verdicts-B.md #5 (CONFIRMED L1/C2)
- frogdb-server/crates/redis-regression/tests/multi_tcl.rs:516-578
- frogdb-server/crates/redis-regression/tests/scripting_tcl.rs:462-576
- frogdb-server/crates/commands/src/blocking.rs:307-318,614-625
