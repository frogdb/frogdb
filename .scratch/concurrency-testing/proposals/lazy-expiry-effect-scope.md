# Proposal: Lazy-expiry effect-scope parity (notifications, tracking, search indexes)

Status: done (2026-07-22) — all three whole-key effect classes closed; hash-field-death `"del"` variant resolved as a documented follow-up (see [Resolution](#resolution-2026-07-22))
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

- [x] Keyspace-notification gap repro'd (real-path) — `regression_lazy_expiry_emits_expired_keyevent` (`frogdb-server/crates/server/tests/integration_pubsub.rs`). Used a real-path **integration** repro (SUBSCRIBE + `read_message`) rather than a turmoil sim: the integration harness already reads pub/sub push frames first-class and there was no SUBSCRIBE/push precedent in `simulation.rs` to mirror; both are single-shard real-path, so the integration pin gives the same coverage the 4c capture seam would, with no ad-hoc frame parsing. The 4c pub/sub-oracle CHECKER criterion (order-consistency across all seams) remains a separate, still-open item; this pin is the direct fail→pass real-path repro.
- [x] Client-tracking-invalidation gap repro'd — `regression_lazy_expiry_invalidates_tracked_key` (`frogdb-server/crates/server/tests/integration_client.rs`); RESP3 `CLIENT TRACKING` consumer observes the invalidation push. Chose the integration harness (real RESP3 push plumbing) over a shard-level test seam — no new `pub` test seam needed and it exercises the full client-visible path.
- [x] Search-index-maintenance gap repro'd — `regression_lazy_expiry_removes_key_from_search_index` (`frogdb-server/crates/server/tests/search.rs`); `FT.SEARCH` stops surfacing the dead key (search hits come straight from the tantivy index, without store hydration, so a stale entry is directly observable).
- [x] Lazy purge fires the same effects as active expiry for each implemented class — `ShardWorker::drain_lazy_purge_effects` (`frogdb-server/crates/core/src/shard/worker.rs`) mirrors `apply_expiry_effects`' `deleted_keys` branch: tracking invalidation → search-index deletion → `expired` keyspace notification → USDT probe → XREADGROUP drain, then the version bump.
- [x] Single-drain-point discipline preserved — all effects route through the one `apply_lazy_purge_effects` / `apply_lazy_purge_effects_no_version_bump` seam (both delegate to `drain_lazy_purge_effects`); no per-call-site effect logic.
- [x] Idempotency argument recorded for each effect — see [Resolution](#resolution-2026-07-22) (no guard required).

### Fix stage (2026-07-22)

| Effect class | Repro pin (fail→pass) | File |
| --- | --- | --- |
| `expired` keyspace notification | `regression_lazy_expiry_emits_expired_keyevent` | `integration_pubsub.rs` |
| Client-tracking invalidation | `regression_lazy_expiry_invalidates_tracked_key` | `integration_client.rs` |
| Search-index deletion | `regression_lazy_expiry_removes_key_from_search_index` | `search.rs` |

Commits: repros (ignored) `6ca2bcf1`; fix + un-ignore `5529768b`.

## References

- Fix-stage plan (introduces the `take_lazily_purged` seam this proposal reuses, and Honest Scoping item 1 which first named this gap): `docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`
- Parity proposal (the four version/WATCH + XREADGROUP-drain gaps this proposal's scope is deliberately split from): `.scratch/concurrency-testing/proposals/lazy-expiry-parity.md`
- Deferred S8 keyspace-notification capture seam / phase-4c pub/sub oracle: `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`

## Resolution (2026-07-22)

The fix lands in `ShardWorker::drain_lazy_purge_effects` (`frogdb-server/crates/core/src/shard/worker.rs`), reached from the two public seams `apply_lazy_purge_effects` (bumps the version) and `apply_lazy_purge_effects_no_version_bump` (WATCH-time). For each key the store reports via `take_lazily_purged`, it applies the exact `apply_expiry_effects` `deleted_keys` effect set.

### GetVersion (WATCH-time) discard seam — split, not drop

**Decision: split the drain.** The `GetVersion` handler's WATCH-time purge (`dispatch_core.rs`) physically removes an already-expired watched key. Before this change it called `discard_lazy_purges`, dropping the whole report — which would have silently persisted the effect gap on that seam (a WATCH could purge a key and no subscriber / tracking client / search index would learn of it). Redis fires the `expired` notification on lazy expiry regardless of which command triggered it. So the WATCH seam now calls `apply_lazy_purge_effects_no_version_bump`: it fires the physical-removal effects (tracking / search / `expired` notification / probe / XREADGROUP drain) but **withholds only the shard-version bump**. Withholding the bump is required by F3 — a WATCH on an already-expired key must record a "nonexistent" watch and must not over-abort unrelated watchers on the shard (bumping at WATCH would make WATCH a mutating op). The EXEC-time watched-key purge (`purge_expired_watches`) continues to use the full `apply_lazy_purge_effects` (bump included): a watched key that genuinely died during the WATCH window must both abort the watcher (gap 3) and notify observers.

### Idempotency — no guard required

Each physical removal is pushed into the store's `lazily_purged` buffer **exactly once**: only `check_and_delete_expired`'s actual-removal branch pushes (the `expiry_suppressed` early return does not), and once a key is `uninstall`ed it is absent, so any later `check_and_delete_expired` on the same key finds nothing and pushes nothing. The buffer is drained **exactly once** per seam via `std::mem::take`. No key can therefore be reported through two seams for one removal — the first physical removal makes it absent for every later purge attempt. Given exactly-once push + exactly-once drain:

- **`expired` keyspace notification** — fires once per removal. Safe.
- **Client-tracking invalidation** — fires once; additionally idempotent by construction (`ShardTracking::invalidate_keys` → `tracking.rs` removes the key from `key_to_clients` after sending, so a hypothetical second call finds no clients and sends nothing).
- **Search-index deletion** — fires once; additionally idempotent (`Index::delete_document` on an already-deleted key is a no-op `delete_term`).
- **XREADGROUP drain** — idempotent (a second drain finds no waiters).
- **Version bump** — monotonic; an extra bump only widens the accepted coarse over-abort envelope. Applied at every seam except the WATCH-time one.

No guard was added.

### Hash-field death (`"del"`) — reachable via a distinct seam, scoped as a follow-up

Active expiry emits a generic `"del"` (not `"expired"`) for a key whose **last hash field** expired and emptied it (`apply_expiry_effects`' `emptied_keys` branch). The lazy path *does* have an equivalent: hash command handlers call `Store::purge_expired_hash_fields` on read (`frogdb-server/crates/commands/src/hash.rs`, ~13 call sites), and when the last field goes it empties the key via `self.delete(key)` (`hashmap.rs:1375`). But that removal travels a **different seam** than the whole-key one this proposal fixes: `delete` (not `check_and_delete_expired` → `uninstall`), so it never lands in `lazily_purged` and this fix does not fire `"del"` for it. This is correct as far as it goes — a lazily-emptied hash key must not emit `"expired"` — but it means the `"del"` variant of lazy hash-field death remains a parity gap.

Closing it cleanly is **not** a mechanical copy: `purge_expired_hash_fields` is shared by the active sweep (`active_expiry.rs:193,575`), which already handles its emptied keys via `ExpiryResult::emptied_keys`. A naive companion `lazily_emptied` buffer pushed from `purge_expired_hash_fields` would be populated by the sweep too and re-fire `"del"` on the next command drain — a double-notification. A correct implementation needs a companion buffer plus an explicit drain-and-discard of it on the active-expiry path (so the sweep owns its `emptied_keys` reporting and the buffer only fires for lazy reads), plus its own D8 repro. Given (a) the proposal's acceptance criteria enumerate the three whole-key effect classes, (b) the fix-stage plan deferred the whole-key effects the same way, and (c) the shared-function double-fire coordination this needs, the lazy hash-field-death `"del"` variant is scoped as a tracked follow-up rather than folded into this stage. It is a genuine (small) gap, not unreachable.

### Ordering

`drain_lazy_purge_effects` matches `apply_expiry_effects`' order for expiry-driven removals (per-key effects, then the batch version bump at the end) — deliberately the active-expiry order, not the write-path `WRITE_EFFECT_ORDER` (which bumps first). For a pub/sub subscriber or tracking client the notification and the version bump are independent, so lazy and active removals are indistinguishable, which is the parity goal.

Follow-up tracking the hash-field-death `"del"` gap above: [09-hash-field-del-keyevent-lazy-empty.md](../issues/09-hash-field-del-keyevent-lazy-empty.md).
