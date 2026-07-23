# Align sharded pub/sub-in-MULTI rejection with verified Redis behavior; add missing test coverage

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 1/3 (score 1)
Area: Transactions / Pub/Sub

## Context

**The original audit's headline claim is REFUTED — do not re-assert it.** The original gap
(`subscribe-family-in-multi-diverges-and-untested`) claimed Redis rejects all SUBSCRIBE-family
commands inside `MULTI` with `EXECABORT`, and that FrogDB's queuing/execution of them was a
divergence. Verified directly against Redis source (`pubsub.c`, `reset.json`, `subscribe.json`): the
subscribe guard in real Redis is `(DENY_BLOCKING && !CLIENT_MULTI)` — i.e. `SUBSCRIBE`/`PSUBSCRIBE`
are explicitly **MULTI-exempt** in Redis and genuinely execute at EXEC time, not reject.
`UNSUBSCRIBE`/`PUNSUBSCRIBE` carry no `NO_MULTI` guard at all, and neither does `RESET`. FrogDB's
plain-family behavior — `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE` actually execute at
EXEC via `exec_pubsub_in_transaction` (`pubsub_conn_command.rs:958-978`), and `ResetIntercept`
handling — **matches Redis exactly**. Do not implement queue-time `NO_MULTI` rejection or
`EXECABORT` semantics for these commands — doing so would be a regression against real Redis
behavior, not a fix.

**The real residue** (verdict ADJUSTED L1/C1, confirmed real): FrogDB rejects
`SSUBSCRIBE`/`SUNSUBSCRIBE`/`PUBSUB` inside `MULTI` with a bespoke, non-Redis error string
`"ERR command not supported inside MULTI"` (`pubsub_conn_command.rs:958-978`), where Redis allows at
least `SUNSUBSCRIBE`/`PUBSUB` inside MULTI (the exact Redis-verified behavior for `SSUBSCRIBE`
specifically should be re-confirmed, not assumed, before writing the fix). There is also **zero test
coverage** of any subscribe-family command inside `MULTI` in either direction — a grep of
`multi_regression.rs`/`multi_tcl.rs`/`integration_transactions.rs` finds none.

## What to build

1. Verify against Redis source/docs exactly which of `SSUBSCRIBE`/`SUNSUBSCRIBE`/`PUBSUB` are
   rejected vs. allowed inside `MULTI` in real Redis — confirm before writing the fix, do not assume.
2. Align FrogDB's rejected set with the verified Redis behavior; where FrogDB genuinely should
   reject, replace the bespoke `"ERR command not supported inside MULTI"` string with Redis's actual
   error text/behavior for that case.
3. Add integration tests covering the full subscribe family (`SUBSCRIBE`, `UNSUBSCRIBE`,
   `PSUBSCRIBE`, `PUNSUBSCRIBE`, `SSUBSCRIBE`, `SUNSUBSCRIBE`, `PUBSUB`) plus `RESET` inside `MULTI`,
   one test per command, asserting the Redis-matching behavior for each.

## Acceptance criteria

- [ ] Do NOT implement or assert queue-time `EXECABORT` for `SUBSCRIBE`/`PSUBSCRIBE`/`UNSUBSCRIBE`/
      `PUNSUBSCRIBE`/`RESET` — these are confirmed Redis-compatible as-is (MULTI-exempt, execute at
      EXEC).
- [ ] Redis-verified (source-checked, not assumed) determination of which of `SSUBSCRIBE`/
      `SUNSUBSCRIBE`/`PUBSUB` are actually rejected inside MULTI upstream.
- [ ] FrogDB's behavior/error text for the genuinely-rejected subset matches Redis exactly; bespoke
      non-Redis string replaced if it diverges.
- [ ] Integration test suite covers all seven subscribe-family commands + RESET inside MULTI, one
      test per command, asserting Redis-parity behavior for each.
- [ ] `pubsub_conn_command.rs:958-978` comment updated to record the verified Redis-parity policy,
      to prevent this refuted claim from being re-litigated in a future audit.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/B-transactions.md #1 (`subscribe-family-in-multi-diverges-and-untested`)
- .scratch/testing-improvements/audit/verdicts-B.md #1 (ADJUSTED L1/C1 — HEADLINE REFUTED vs Redis source)
- server/src/connection/pubsub_conn_command.rs:958-978
- server/src/connection/guards.rs:470-544,524
