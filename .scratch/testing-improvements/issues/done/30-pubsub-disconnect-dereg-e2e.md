# Subscriber disconnect cross-shard deregistration untested end-to-end

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: pubsub

## Context

`ShardSubscriptions::remove_connection` is unit-tested against a single in-memory map (`frogdb-server/crates/core/src/pubsub.rs:517`, test at `:911`), but a real client disconnect must deregister the connection from the broadcast subscription map (shard 0) *and* from every sharded-channel subscription map on each shard it had subscribed against — a fan-out. The real ungraceful-close path broadcasts `ConnectionClosed` to all shards (`frogdb-server/crates/server/src/connection/lifecycle.rs:191-193`), but there is no integration test that: subscribes a client to channels spanning at least two shards, disconnects it (both gracefully via `CLIENT KILL` and ungracefully via a raw socket close), and then verifies `PUBSUB CHANNELS`/`PUBSUB NUMSUB`/`PUBSUB SHARDNUMSUB` reflect zero subscriptions across all shards, and that subsequent `PUBLISH` calls no longer count the disconnected client as a receiver.

## What to build

- Integration test: subscribe one client to a broadcast channel, a pattern, and a sharded channel that lands on a different shard than the broadcast subscription (so the deregistration fan-out is actually exercised across ≥2 shards).
- Variant A: disconnect via `CLIENT KILL` (graceful path) — assert `PUBSUB CHANNELS`, `PUBSUB NUMSUB`, `PUBSUB NUMPAT`, and `PUBSUB SHARDNUMSUB` all reflect zero for the disconnected client's subscriptions on every shard.
- Variant B: disconnect via raw ungraceful socket close (drop the TCP connection) — same assertions, confirming the `ConnectionClosed` broadcast fan-out (`lifecycle.rs:191-193`) actually deregisters cross-shard state, not just the shard the connection's owning worker lives on.
- Assert `PUBLISH` to the previously-subscribed channels no longer counts the disconnected client as a receiver (return value / receiver count drops).

## Acceptance criteria

- [ ] Test subscribes across ≥2 shards (broadcast + sharded/pattern spanning a different shard) before disconnecting.
- [ ] Graceful (`CLIENT KILL`) variant asserts zero subscription counts across all shards post-disconnect.
- [ ] Ungraceful (raw close) variant asserts the same, specifically exercising the `ConnectionClosed` fan-out path.
- [ ] `PUBLISH` receiver count drops to exclude the disconnected client after either disconnect path.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/core/src/pubsub.rs:517` (`ShardSubscriptions::remove_connection`), unit test at `:911` (single-map only)
- `frogdb-server/crates/server/src/connection/lifecycle.rs:191-193` (`ConnectionClosed` broadcast fan-out to all shards on real disconnect)

## Resolution

Done 2026-07-23. Added two e2e integration tests to
`frogdb-server/crates/server/tests/integration_pubsub.rs`, plus a set of PUBSUB
introspection + deadline-polling helpers:

- `test_subscriber_dereg_on_client_kill_cross_shard` (Variant A, graceful
  `CLIENT KILL ID`).
- `test_subscriber_dereg_on_raw_close_cross_shard` (Variant B, ungraceful raw
  TCP `drop`, exercising the `ConnectionClosed` fan-out in
  `connection/lifecycle.rs` `notify_connection_closed`).

Both share `run_disconnect_dereg_test`, which on a 4-shard standalone server
subscribes ONE connection to a broadcast channel + a pattern (both register on
broadcast shard 0) and a sharded channel deliberately owned by a non-zero shard
(via the existing `key_off_shard_zero` helper asserting `shard_for_key != 0`),
so deregistration must fan out across >= 2 shards. The test:

1. captures `CLIENT ID` before entering pub/sub mode (RESP2 pub/sub mode rejects
   CLIENT commands);
2. polls (`wait_for_full_reg`, 5s deadline) until `PUBSUB NUMSUB` /
   `NUMPAT` / `SHARDNUMSUB` all read 1, and asserts `PUBLISH`/`SPUBLISH` each
   return receiver count 1, so the post-disconnect zero is a real drop;
3. disconnects via the selected mode;
4. polls (`wait_for_full_dereg`, 5s deadline, 20ms interval — no fixed sleeps)
   until `PUBSUB CHANNELS` is empty and `NUMSUB` / `NUMPAT` / `SHARDNUMSUB` all
   read 0 across all shards;
5. asserts `PUBLISH` and `SPUBLISH` to the previously-subscribed channels now
   return receiver count 0.

All four acceptance criteria are met.

### Findings

No leak. Deregistration is correct on both paths — the `ConnectionClosed`
broadcast fan-out (`lifecycle.rs:191-193`) does deregister the sharded
subscription on its non-zero owning shard as well as the broadcast/pattern state
on shard 0. Counts drop to zero within the poll deadline (observed
sub-second; the whole test runs in ~0.15s). No production code change was
needed.

### Test discipline

- `just test frogdb-server subscriber_dereg`: 2 passed.
- Flake check: 15 consecutive `cargo nextest` runs (30 test executions total),
  all passed — 0% flake rate.
- `just fmt frogdb-server`, `cargo clippy -p frogdb-server --tests -- -D warnings`,
  and the full `just lint frogdb-server` (incl. `lint-turmoil`) all clean.
