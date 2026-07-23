# Subscriber disconnect cross-shard deregistration untested end-to-end

Status: needs-triage
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
