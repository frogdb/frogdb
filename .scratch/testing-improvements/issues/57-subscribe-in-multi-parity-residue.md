# Align sharded pub/sub-in-MULTI rejection with verified Redis behavior; add missing test coverage

Status: done
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

## Resolution

**Redis-verified allowed/rejected set** (checked directly against Redis 8.6.4 source via
`gh api repos/redis/redis/contents/...?ref=8.6.4`, not assumed):

- `src/commands/{ssubscribe,sunsubscribe,pubsub}.json` — none of the three carry a `NO_MULTI`-style
  command flag (only `PUBSUB`/`NOSCRIPT`/`LOADING`/`STALE`/`DENYOOM`, none gate on transaction
  state).
- `src/multi.c` (`execCommand`): during the EXEC loop, `CLIENT_DENY_BLOCKING` is set (line ~164) and
  `CLIENT_MULTI` remains set throughout (cleared only afterward in `discardTransaction`) — so a
  queued command's runtime guard sees both flags simultaneously.
- `src/pubsub.c` runtime guards, read directly:
  - `subscribeCommand`/`psubscribeCommand`: `if ((c->flags & CLIENT_DENY_BLOCKING) &&
    !(c->flags & CLIENT_MULTI))` — the `!CLIENT_MULTI` carve-out means these are **MULTI-exempt**
    and genuinely execute at EXEC (already FrogDB's behavior — unchanged).
  - `unsubscribeCommand`/`punsubscribeCommand`/`sunsubscribeCommand`: **no** `DENY_BLOCKING` check
    at all — unconditionally execute in any context, including MULTI.
  - `pubsubCommand`: no `DENY_BLOCKING` check either — executes unconditionally, including MULTI.
  - `ssubscribeCommand`: `if (c->flags & CLIENT_DENY_BLOCKING)` — **no** `!CLIENT_MULTI` carve-out.
    This is the one genuine exception: SSUBSCRIBE really is rejected inside MULTI in real Redis,
    with the wire text `"SSUBSCRIBE isn't allowed for a DENY BLOCKING client"` (Redis auto-prepends
    `-ERR `).

**Net verified policy**: SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, SUNSUBSCRIBE, and PUBSUB
all execute normally inside MULTI/EXEC. SSUBSCRIBE alone is rejected, with Redis's exact error text.
RESET is never queued at all (intercepted before the transaction queue in both Redis and FrogDB) and
always executes directly, aborting any open transaction.

**Changes made**:
- `server/src/connection/pubsub_conn_command.rs` (`exec_pubsub_in_transaction`): moved `PUBSUB` into
  the single-response branch alongside `PUBLISH`/`SPUBLISH`, moved `SUNSUBSCRIBE` into the
  multi-confirmation branch alongside `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`, and gave
  `SSUBSCRIBE` its own dedicated arm returning Redis's exact error text (`"ERR SSUBSCRIBE isn't
  allowed for a DENY BLOCKING client"`) instead of the bespoke `"ERR command not supported inside
  MULTI"`. Updated the doc comment to record the verified Redis-parity policy (with source
  citations) so this refuted claim isn't re-litigated by a future audit. The old catch-all `_` arm is
  now unreachable for the 9 registered `ConnMutation::PubSub` commands (all covered by name) but kept
  as a defensive fallback.
- `server/src/connection/transaction.rs`: updated the adjacent comment describing the dispatch to
  match.
- No changes to `guards.rs::queue_command` — queuing behavior for all of these commands was already
  correct (none carry a queue-time `NO_MULTI` gate in Redis, matching FrogDB's existing unconditional
  queuing).

**Tests added** (`server/tests/integration_pubsub.rs`), one per subscribe-family command:
- `test_subscribe_inside_multi_executes` — SUBSCRIBE executes, confirmation shape pinned.
- `test_psubscribe_inside_multi_executes` — PSUBSCRIBE executes, confirmation shape pinned.
- `test_unsubscribe_inside_multi_executes` — UNSUBSCRIBE executes (null-channel/count-0 shape, no
  active subscription).
- `test_punsubscribe_inside_multi_executes` — PUNSUBSCRIBE executes (same null-pattern shape).
- `test_sunsubscribe_inside_multi_executes` — SUNSUBSCRIBE executes (was rejected before this fix);
  uses a never-subscribed connection since a RESP2 client actually in pub/sub mode can't issue MULTI
  at all in either Redis or FrogDB (`is_allowed_in_pubsub_mode`) — that restriction is orthogonal to
  this issue.
- `test_pubsub_inside_multi_executes` — PUBSUB executes (was rejected before this fix), single
  response folded into the EXEC slot.
- `test_ssubscribe_inside_multi_rejected` — tightened from a bare `starts_with(b"ERR")` prefix check
  to the exact Redis wire text `"ERR SSUBSCRIBE isn't allowed for a DENY BLOCKING client"`.
- RESET inside MULTI: already covered by the pre-existing `test_reset_aborts_transaction`
  (`server/tests/integration_client.rs`), which pins that RESET drops the queued command and returns
  `+RESET` — matches the verified Redis behavior, no changes needed there.

**Test evidence** (local, macOS, `just test frogdb-server <pattern>`):
- `just test frogdb-server "inside_multi"` — 11/11 passed, run 3 times consecutively, consistent.
- `just test frogdb-server integration_pubsub` — 112/112 passed (full file, no regressions).
- `just test frogdb-server integration_transactions` — 31/31 passed (no regressions).
- `just test frogdb-server integration_client` — 61/61 passed (no regressions, incl. existing RESET
  tests).
- `just test frogdb-redis-regression multi` — 152/152 passed (incl. `pubsub_tcl` MULTI-adjacent
  cases, no regressions).
- `just lint frogdb-server` (clippy `-D warnings`) — clean.
- `just fmt frogdb-server` — no diff beyond the intended edits.

**Explicitly not done** (per task scope / verdict): did not implement or assert queue-time
`NO_MULTI`/`EXECABORT` rejection for SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE/
PUBSUB/RESET — verified none of them carry that flag upstream; doing so would be a regression
against real Redis, not a fix.
