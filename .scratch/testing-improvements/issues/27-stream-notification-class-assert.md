# Stream-class keyspace-notification events emitted but never asserted end-to-end

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: streams

## Context

Stream-class (`t`) keyspace-notification events are genuinely emitted by production code — `frogdb-server/crates/commands/src/stream/basic.rs:28-30` (xadd), `:271`, `:320`, and `frogdb-server/crates/commands/src/stream/consumer_groups.rs:29` all call the notification path. But `integration_pubsub.rs`'s keyspace-notification coverage only exercises the `$` (generic), `g` (generic/general), `l` (list), `s` (set), `h` (hash), `z` (zset), and `x` (expired) classes — stream events are never subscribed to and asserted in an end-to-end test. `pubsub_tcl.rs:22` excludes the upstream stream-events test as an intentional incompatibility (config-related), which is a separate, deliberate exclusion — but it means FrogDB's own port has zero equivalent coverage to fall back on. A regression that silently stopped XADD (or XTRIM/XSETID/XGROUP CREATE, etc.) from emitting its keyspace event would go unnoticed.

## What to build

- Integration test: subscribe to `__keyspace@0__:*` and `__keyevent@0__:*` with `notify-keyspace-events` set to include the stream class (`t`, plus `K`/`E` as needed for keyspace/keyevent channels and `A`/`g` as appropriate).
- Run each stream mutator command (XADD, XTRIM, XSETID, XGROUP CREATE, and any other emission sites found in `basic.rs`/`consumer_groups.rs`) and assert the corresponding keyspace/keyevent notification fires with the correct event name (`xadd`, `xtrim`, `xsetid`, `xgroup-create`, etc.).
- Where feasible, port the excluded upstream stream-events test's assertions (not the excluded test itself, which stays excluded for its documented config-incompatibility reason) into a FrogDB-native equivalent.

## Acceptance criteria

- [ ] New integration test asserts XADD emits a keyspace/keyevent notification with event name `xadd`.
- [ ] Test covers at least XTRIM, XSETID, and XGROUP CREATE emission sites (matching the call sites identified in `basic.rs`/`consumer_groups.rs`), asserting correct event names.
- [ ] Test would fail if any of the emission call sites were removed (verify by temporarily commenting one out, confirming test failure, then reverting).

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/commands/src/stream/basic.rs:28-30,271,320` (xadd and other stream event emission sites)
- `frogdb-server/crates/commands/src/stream/consumer_groups.rs:29` (consumer-group event emission)
- `frogdb-server/crates/server/tests/integration_pubsub.rs` (existing keyspace-notification coverage — $/g/l/s/h/z/x classes only, no stream class)
- `frogdb-server/crates/redis-regression/tests/pubsub_tcl.rs:22` (excluded upstream stream-events test, intentional-incompatibility:config)
