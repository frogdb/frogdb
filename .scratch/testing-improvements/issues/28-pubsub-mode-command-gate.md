# RESP2/RESP3 subscribed-mode command gate underspecified by tests

Status: done
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

- [x] RESP3 test confirms a normal command succeeds with a normal reply while subscribed (not just that push delivery works).
- [x] RESP2 test enumerates and asserts the full allow-set boundary (all permitted commands succeed; a disallowed command is rejected with the correct error).
- [x] RESET-while-subscribed test asserts pubsub mode is exited and subsequent commands are no longer gated.
- [x] `is_allowed_in_pubsub_mode` gains direct unit-test coverage (not just integration-level), matching the verdict's finding that no unit test exists today.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/server/src/connection/guards.rs:196-213` (`is_allowed_in_pubsub_mode`, RESP3 allow-all branch at :197-199)
- `frogdb-server/crates/server/src/connection/guards.rs:322` (RESET error text advertising exit-from-pubsub-mode)
- `frogdb-server/crates/server/tests/integration_pubsub.rs:768` (`test_pubsub_mode_restrictions`, only asserts GET-rejected + PING-works)

## Resolution

Verified the exact allow-set against Redis 8.6 `processCommand`'s subscribe-context
gate (`server.c`): exactly **9 commands** — SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE,
PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING, QUIT, RESET — are allowed while
subscribed under RESP2; RESP3 lifts the restriction entirely (`guards.rs:202-204`
short-circuits to `true` before consulting the registry at all, so even an unknown
command reaches its own handler rather than the gate's error text).

**Direct unit coverage added** (`frogdb-server/crates/server/src/connection/guards.rs`,
`mod tests`) — none existed before this task:
- `test_is_allowed_in_pubsub_mode_resp2_allow_set_boundary` — all 9 allowed
  commands return `true`; representative data commands (GET/SET/DEL) return `false`.
- `test_is_allowed_in_pubsub_mode_resp3_allows_everything` — GET/SET/DEL/SUBSCRIBE/
  PING/an unknown command name all return `true` under RESP3.
- `test_is_allowed_in_pubsub_mode_resp2_connection_state_siblings_diverge_from_redis`
  — see divergence below.

**Integration coverage added** (`frogdb-server/crates/server/tests/integration_pubsub.rs`,
new "Subscribe-mode command gate (issue 28)" section):
- `test_pubsub_mode_resp2_allowed_commands_succeed` — enumerates SUBSCRIBE,
  UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING against a
  live connection held in pub/sub mode via an anchor subscription throughout, then
  re-confirms GET is still rejected (proving the connection never fell out of
  pub/sub mode mid-test).
- `test_pubsub_mode_resp2_quit_allowed` — QUIT is allowed while subscribed and
  closes the connection (own test: QUIT is terminal).
- `test_pubsub_mode_resp2_reset_exits_pubsub_mode` — RESET is allowed while
  subscribed, replies `+RESET`, and a subsequent SET/GET succeed immediately
  afterward (pub/sub mode is genuinely exited — the acceptance criterion this
  issue called out specifically).
- `test_pubsub_mode_resp2_disallowed_commands_exact_error_text` — GET/SET/DEL
  rejected with the exact Redis wire text: `ERR Can't execute '<CMD>': only
  (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this
  context` (matches `guards.rs:360-363` byte for byte).
- `test_pubsub_mode_resp3_normal_command_succeeds_while_subscribed` — HELLO 3,
  SUBSCRIBE, then SET/GET return normal (non-push) replies while subscribed, and
  a subsequently published message still arrives as an independent Push frame.
- `test_pubsub_mode_resp3_gate_is_unconditional_allow` — an unknown command while
  subscribed under RESP3 fails as "unknown command", never with the subscribe-mode
  gate's error text, pinning that the RESP3 branch is unconditional (not merely
  "known commands are all allowed").

**Divergence from Redis, pinned honestly (not silently "fixed" by this task):**
`is_allowed_in_pubsub_mode`'s RESP2 branch allows any command sharing RESET's
`ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)` execution
strategy, not RESET specifically. ASKING, READONLY, and READWRITE
(`connection_state_conn_command.rs`) share that same strategy tag with RESET, so
FrogDB's RESP2 gate — contrary to Redis's exact 9-command allow-set — also lets
ASKING/READONLY/READWRITE execute while subscribed. Confirmed live: on a
standalone (non-cluster) server, issuing ASKING/READONLY/READWRITE while
subscribed under RESP2 returns each command's own normal reply
(`ERR This instance has cluster support disabled`), never the subscribe-mode
gate's rejection — proving the commands reach their own handlers rather than
being blocked. Pinned by
`test_is_allowed_in_pubsub_mode_resp2_connection_state_siblings_diverge_from_redis`
(unit) and `test_pubsub_mode_resp2_connection_state_siblings_bypass_gate`
(integration), both documented as a known, deliberate-to-leave divergence rather
than filed as a new bug — low practical impact (these three commands are
inert/no-ops for a subscribed client in every real deployment shape: standalone
returns the cluster-disabled error above, and a cluster-mode client that issues
them while subscribed only flips connection-local routing flags, matching no
Redis-observable side effect a subscribed client could exploit). Left as-is per
this issue's scope (test the gate as it exists); a follow-up to split
`ConnectionState` into a RESET-only strategy tag (or add an explicit pubsub-gate
allowlist independent of `ExecutionStrategy`) would close it if ever desired.

Verification: `just check frogdb-server` and `just fmt frogdb-server` clean;
`just lint frogdb-server` clean (`-D warnings`, both the plain and
`--features turmoil --tests` clippy passes). `just test frogdb-server
test_pubsub_mode` (9 tests: 3 new unit + 6 new/existing integration) run 3x,
all green every time. `just test frogdb-server integration_pubsub` (full file,
120 tests) run 3x, all green every time (no other test in the file
regressed).
