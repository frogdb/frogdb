# RESP2/RESP3 subscribed-mode command gate underspecified by tests

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: pubsub

## Context

`is_allowed_in_pubsub_mode` (`frogdb-server/crates/server/src/connection/guards.rs:196-213`) has two branches: for RESP3, every command is allowed while subscribed (":197-199 — `if self.state.protocol_version.is_resp3() { return true; }`", correctly matching Redis's RESP3 push-based model), and for RESP2, only a fixed allow-set (strategy `PubSub`/`ConnectionState` commands plus `PING`/`QUIT`) is permitted. The only existing test, `test_pubsub_mode_restrictions` (`frogdb-server/crates/server/tests/integration_pubsub.rs:768`), asserts just two data points: RESP2 `GET` is rejected, and `PING` works. It never exercises:
- the RESP3 "allow everything" branch at all (no test issues a normal data command like `SET`/`GET` while subscribed under RESP3 and asserts it succeeds with a normal reply alongside push messages),
- the RESP2 allow-set boundary (which of the ~9 Redis-documented subscribed-mode-allowed commands are actually permitted, one by one),
- `RESET` while subscribed — the error text at `guards.rs:322` advertises RESET as an escape hatch, but no test confirms issuing RESET while subscribed actually exits pubsub mode.

## What to build

- RESP3 test: HELLO 3, SUBSCRIBE a channel, issue a normal data command (e.g. `SET`/`GET`) while subscribed, assert it returns a normal (non-push) reply, and that push messages continue to arrive independently.
- RESP2 boundary test: enumerate the 9 Redis-documented subscribed-mode-allowed commands (SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING, QUIT, RESET — confirm the exact set against `guards.rs:196-213`) and assert each is allowed, plus assert a representative disallowed data command (e.g. `GET`) is rejected with the documented error.
- RESET-in-pubsub-mode test: subscribe, issue RESET, assert the connection exits pubsub mode (subsequent data commands succeed) and RESET's own reply is correct.

## Acceptance criteria

- [ ] RESP3 test confirms a normal command succeeds with a normal reply while subscribed (not just that push delivery works).
- [ ] RESP2 test enumerates and asserts the full allow-set boundary (all permitted commands succeed; a disallowed command is rejected with the correct error).
- [ ] RESET-while-subscribed test asserts pubsub mode is exited and subsequent commands are no longer gated.
- [ ] `is_allowed_in_pubsub_mode` gains direct unit-test coverage (not just integration-level), matching the verdict's finding that no unit test exists today.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/src/connection/guards.rs:196-213` (`is_allowed_in_pubsub_mode`, RESP3 allow-all branch at :197-199)
- `frogdb-server/crates/server/src/connection/guards.rs:322` (RESET error text advertising exit-from-pubsub-mode)
- `frogdb-server/crates/server/tests/integration_pubsub.rs:768` (`test_pubsub_mode_restrictions`, only asserts GET-rejected + PING-works)
