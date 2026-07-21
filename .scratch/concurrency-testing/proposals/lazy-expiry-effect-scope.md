# Proposal: Lazy-expiry effect-scope parity (notifications, tracking, search indexes)

Status: needs-triage
Origin: design ruling 3 of the lazy-expiry-parity fix-stage plan (`docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`, 2026-07-21)

## Problem

The fix-stage plan closes lazy-expiry parity for exactly two externally observable effects — the shard-version bump (WATCH invalidation) and the XREADGROUP-waiter drain — via the `Store::take_lazily_purged` worker seam (Task 1). It deliberately does **not** extend lazy purge to the rest of the effect set active expiry (`apply_expiry_effects`, `frogdb-server/crates/core/src/shard/event_loop.rs:154`) fires for a key that dies via the active sweep. Verified by grep (2026-07-21) against `apply_expiry_effects`, all of the following are real, **implemented** FrogDB features today — not future-facing — that active expiry fires and lazy purge does not:

- **Keyspace notifications.** `self.emit_keyspace_notification(key, "expired", KeyspaceEventFlags::EXPIRED)` for whole-key TTL death, `"del"` for a key whose last hash field expired (`event_loop.rs:166`, `:186`; implementation in `frogdb-server/crates/core/src/shard/keyspace_notify.rs:28`).
- **Client-side-caching / tracking invalidation.** `self.tracking.invalidate_keys(&[key.as_ref()], 0)`, gated on `self.tracking.has_tracking_clients()` (`event_loop.rs:158-160`, `:180-182`; implementation `frogdb-server/crates/core/src/tracking.rs:141`, wired via `frogdb-server/crates/core/src/shard/types.rs:479`).
- **Search-index maintenance.** `self.delete_from_search_indexes(key)` (`event_loop.rs:163`, `:184`; implementation `frogdb-server/crates/core/src/shard/search_hook.rs:153`) — removes the expired key from any search index it participates in.
- **USDT key-expired probe.** `crate::probes::fire_key_expired(...)` (`event_loop.rs:169-172`, `:188-191`) — an observability probe, not a client-visible protocol effect; included here for completeness but out of scope for client-facing acceptance criteria below.

All four are exercised today by the active-expiry sweep and by explicit `DEL`/write paths; none of them fire when a key instead dies through the lazy-purge path (`check_and_delete_expired` → `uninstall`, `frogdb-server/crates/core/src/store/hashmap.rs:421`). This is a real, present-tense parity gap, not a speculative one — a subscriber to `__keyevent@0__:expired`, a client-tracking consumer, or a search index will silently miss the event/update whenever the key instead happens to die via a lazy read.

## Direction

Reuse the `Store::take_lazily_purged` worker seam introduced by the fix-stage plan (Task 1) as the **single drain point** for these effects too, so they fire from exactly one place regardless of which command's lazy purge removed the key — mirroring why Mechanism 1 anchors at the physical-removal site rather than a per-command predicate. Concretely: extend the drain helper that currently applies the version bump + XREADGROUP drain (`ShardWorker::apply_lazy_purge_effects`, `frogdb-server/crates/core/src/shard/worker.rs`) to also call `emit_keyspace_notification`, `tracking.invalidate_keys`, and `delete_from_search_indexes` for each lazily-purged key — the same per-key effect set `apply_expiry_effects` already applies for `deleted_keys`/`emptied_keys`. Unlike the version bump and the XREADGROUP drain, these three effects are **not** obviously idempotent-safe across the multiple seams a single lazy removal can be reported through (a command's own purge, `StreamSatisfaction::check_key`'s purge, the EXEC watch purge) — a naive port risks double notifications, double invalidations, or a double index-delete attempt. Each needs its own idempotency argument or a guard before it is safe to add to the shared drain point; do not port them mechanically.

Note the phase-4c pub/sub oracle (`docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`, "4c. Pub/sub oracle" and the deferred S8 item: "the 'keyspace notifications consistent with the chosen order' half of this goal is not yet pinned: the shard-driver harness has no notification-capture seam; deferred to a future phase (candidate: phase 4c pubsub oracle work)") may supply the notification-capture invariant this proposal needs to actually pin the keyspace-notification gap with a real checker, rather than an ad hoc assertion. Sequencing: this proposal's keyspace-notification acceptance criterion is naturally gated on phase 4c landing a capture seam, even if the fix itself lands earlier.

D8 discipline carries over from the fix-stage plan: each effect-class gap needs a real-path repro (or a reasoned-unreachable note) before its fix lands.

## Acceptance criteria

- [ ] Keyspace-notification gap repro'd (real-path, pending a 4c capture seam) or argued unreachable
- [ ] Client-tracking-invalidation gap repro'd or argued unreachable
- [ ] Search-index-maintenance gap repro'd or argued unreachable
- [ ] Lazy purge fires the same effects as active expiry for each implemented class above (keyspace notification, tracking invalidation, search-index deletion)
- [ ] Single-drain-point discipline preserved — no per-call-site effect logic; all three effects route through the `take_lazily_purged` seam alongside the version bump / XREADGROUP drain
- [ ] Idempotency argument (or a guard) recorded for each effect given multiple lazy-purge report seams per removal

## References

- Fix-stage plan (introduces the `take_lazily_purged` seam this proposal reuses, and Honest Scoping item 1 which first named this gap): `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- Parity proposal (the four version/WATCH + XREADGROUP-drain gaps this proposal's scope is deliberately split from): `.scratch/concurrency-testing/proposals/lazy-expiry-parity.md`
- Deferred S8 keyspace-notification capture seam / phase-4c pub/sub oracle: `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`
