# 10 — Roll the shard-driver notification-capture checker out beyond S8

Status: ready-for-agent
Type: AFK
Origin: Phase 4c (pub/sub oracle) — deliverable B landed the capture seam + S8 checker; broader
rollout deferred here.

## Context

Phase 4c added a keyspace-notification capture seam to the shard-driver harness and an
order-consistency checker, closing the deferred S8 half ("keyspace notifications consistent with
the chosen order"):

- `ShardWorker::drive_capture_keyspace(patterns, conn_id, flags)`
  (`frogdb-server/crates/core/src/shard/event_loop.rs`) — feature-gated (`shard-driver`) seam that
  enables a keyspace-event mask and registers a capture PSUBSCRIBE on the worker's own subscription
  table. Single-shard drivers run the `Local` topology, so emits land synchronously in the returned
  receiver, in emission order.
- `frogdb-server/crates/core/tests/shard_driver/notify_capture.rs` — `NotificationCapture`
  (`drain` / `drain_keyevents`), `CapturedNotification` keyevent/keyspace projection, and
  `assert_keyevents_consistent(observed, expected)` (exact order; detects missing, extra, and
  reordered notifications).
- `ShardDriver::capture_keyspace(shard, conn_id, patterns, flags)`
  (`frogdb-server/crates/core/tests/shard_driver/harness.rs`).
- Wired into `scenario_s8.rs`: the deterministic pin now asserts `expired e` precedes `set a`,
  `set b`; `s8_notifications_consistent_with_serialization_order` runs both serialization orders
  (sweep→EXEC and EXEC→sweep) and asserts each keyevent stream matches its order.

## What to build

Extend the capture checker to the other shard-driver scenarios where keyspace-notification ordering
is a meaningful invariant — the seam is generic (any pattern, any mask), so these are additive:

- **S2 (WATCH vs expiry vs unrelated write, `scenario_s2.rs`)** and the **F3 lazy-expiry arms**: a
  lazy read that purges an expired key emits its `expired`/`del` keyevent through the same
  `apply_expiry_effects` / lazy-purge drain seam. Assert that the keyevent for the purged key
  appears iff the chosen schedule actually purged it (and does not appear when the key was still
  live), and that its position is consistent with the read that triggered it.
- Any other scenario driving expiry, eviction, or multi-key writes where the *set* of emitted
  notifications, not just the final state, is load-bearing.

## Acceptance criteria

- [ ] At least the S2 and lazy-expiry (F3) scenarios capture notifications and assert
      consistency-with-chosen-order via `assert_keyevents_consistent` (or a documented reason a
      given scenario's notifications are not order-meaningful).
- [ ] The checker asserts *iff* semantics — a notification that should NOT fire under the chosen
      order (e.g. a key that stayed live) is asserted absent, not merely un-checked.
- [ ] Existing shard-driver scenarios stay green.

## Blocked by

None — the seam and checker already exist (Phase 4c).

## References

- `frogdb-server/crates/core/tests/shard_driver/notify_capture.rs` (seam consumer + checker)
- `frogdb-server/crates/core/tests/shard_driver/scenario_s8.rs` (worked example)
- `frogdb-server/crates/core/src/shard/event_loop.rs` (`drive_capture_keyspace`,
  `apply_expiry_effects`)
- Design doc S8 note: `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`
