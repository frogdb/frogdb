# Proposal: Active Expiry Coordinator

Status: proposed
Date: 2026-06-13

## Problem

`run_active_expiry` (`frogdb-server/crates/core/src/shard/event_loop.rs:121-238`) is ~116 lines of
imperative glue wired directly into the shard event loop's `select!` arm
(`event_loop.rs:57-65`). It is load-bearing — it is the *only* active-expiry path; nothing else
deletes keys on the TTL timer — yet it is a shallow, locality-free module: it has no interface, no
name a test can call, and no seam between "decide what expired" and "apply the consequences." It is
behaviour with no boundary.

The method tangles ~6 distinct concerns in one function body:

| # | Concern | Evidence (`event_loop.rs`) |
|---|---------|----------------------------|
| 1 | Read pause/disable gates + sync lazy-expiry suppression | `:122-136` (`set_expiry_suppressed`, `expiry_paused`, `debug_active_expire_disabled`) |
| 2 | Get-expired-keys + field sweep from the store | `:143` `get_expired_keys`, `:182` `get_expired_fields`, `:204` `purge_expired_hash_fields` |
| 3 | Delete under a time budget | `:138-153`, `:147-150` (budget), `:153` `store.delete` |
| 4 | Invalidate client tracking | `:157-159`, `:209-211` (`tracking.invalidate_keys`) |
| 5 | Delete from search indexes | `:162`, `:213` (`delete_from_search_indexes`) |
| 6 | Emit keyspace notifications + fire probes + bump metrics/version | `:165` (`emit_keyspace_notification`), `:168-171` (`fire_key_expired`), `:218-237` (counters + `increment_version`) |

The depth is inverted: the *decision* logic (which keys are due, the dedup of keys-with-expired-
fields at `:185-192`, the budget arithmetic) is trivial, while the *side effects* — the part the
shard genuinely owns — are interleaved line-by-line with that decision logic. Adding a post-expiry
effect (say, replicating the implicit DEL, or a new "active-expired" metric) means editing
`event_loop.rs`. Changing the decision logic (adaptive sampling, a smarter budget) means editing the
same 116 lines and risking the side effects. The two axes that should move independently are welded
together.

**Deletion test.** This is the diagnostic the review applies: *could a unit test exercise this
behaviour by deleting nothing else?* Today, no. Testing "the budget is honored," "the metric counts
each expired key once," or "no key is double-deleted" requires standing up a `ShardWorker` and its
event loop — channels, registry, persistence, observability, the lot. There is no smaller artifact
to call. A behaviour this load-bearing with zero reachable unit test is exactly the shape this
deepening targets.

## Current state

### `run_active_expiry`, the tangle (`frogdb-server/crates/core/src/shard/event_loop.rs:121-238`)

```rust
fn run_active_expiry(&mut self) {
    // Sync the expiry_paused flag to the store for passive expiry suppression.
    let paused = self
        .expiry_paused
        .load(std::sync::atomic::Ordering::Relaxed);
    self.store.set_expiry_suppressed(paused);

    // Skip active expiry during CLIENT PAUSE to prevent master/replica divergence.
    if paused {
        return;
    }

    // Skip active expiry when disabled via DEBUG SET-ACTIVE-EXPIRE 0.
    if self.debug_active_expire_disabled {
        return;
    }

    let budget = Duration::from_millis(25);
    let start = Instant::now();
    let now = Instant::now();

    // Get expired keys using the cleaner abstraction
    let expired = self.store.get_expired_keys(now);
    let mut deleted_count = 0u64;

    for key in expired {
        if start.elapsed() > budget {
            tracing::trace!(shard_id = self.shard_id(), "Active expiry budget exhausted");
            break;
        }

        // Delete the key
        if self.store.delete(&key) {
            deleted_count += 1;

            // Invalidate tracked clients for expired key
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }

            // Remove from search indexes
            self.delete_from_search_indexes(&key);

            // Emit expired keyspace notification
            self.emit_keyspace_notification(&key, "expired", KeyspaceEventFlags::EXPIRED);

            // Fire USDT probe: key-expired
            crate::probes::fire_key_expired(
                std::str::from_utf8(&key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );

            tracing::trace!(/* ... */);
        }
    }

    // Field-level expiry sweep
    let expired_fields = self.store.get_expired_fields(now);
    let mut field_deleted_count = 0u64;

    // Collect unique keys that have expired fields
    let mut keys_with_expired_fields: Vec<Bytes> = Vec::new();
    let mut seen_keys: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
    for (key, _field) in expired_fields {
        if seen_keys.insert(key.clone()) {
            keys_with_expired_fields.push(key);
        }
    }

    for key in keys_with_expired_fields {
        if start.elapsed() > budget {
            tracing::trace!(/* ... */);
            break;
        }

        let key_existed_before = self.store.get(&key).is_some();
        let purged = self.store.purge_expired_hash_fields(&key) as u64;
        field_deleted_count += purged;

        // If the key existed before but is gone now, the hash was emptied and deleted
        if key_existed_before && self.store.get(&key).is_none() {
            if self.tracking.has_tracking_clients() {
                self.tracking.invalidate_keys(&[key.as_ref()], 0);
            }

            self.delete_from_search_indexes(&key);
        }
    }

    // Record expired keys metric and increment version
    let total_expired = deleted_count + field_deleted_count;
    if total_expired > 0 {
        let shard_label = self.shard_id().to_string();
        if deleted_count > 0 {
            self.store.add_expired_keys(deleted_count);
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keys_expired_total",
                deleted_count,
                &[("shard", &shard_label)],
            );
        }
        if field_deleted_count > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_fields_expired_total",
                field_deleted_count,
                &[("shard", &shard_label)],
            );
        }
        self.increment_version();
    }
}
```

### Reachable only by spinning the event loop

The only call sites are the `expiry_interval.tick()` arm (`event_loop.rs:57-65`), driven by a
`tokio::time::interval(Duration::from_millis(100))` set up at `event_loop.rs:17`. There is no other
caller. To assert anything about this code — budget honored, metric counted once, the field-emptied
branch firing the right effects — a test must construct a full `ShardWorker` (`shard/worker.rs:33-100`)
and run `ShardWorker::run`. There is no unit test for active expiry anywhere in `shard/`; the
behaviour is verified, if at all, only end-to-end through the Redis-compat suite.

The store side it leans on is already a clean, testable seam — `get_expired_keys`
(`store/traits.rs:139`, impl `store/hashmap.rs:1018` → `ExpiryIndex::get_expired`,
`noop.rs:99-112`), `get_expired_fields` (`store/hashmap.rs:1046` → `FieldExpiryIndex::get_expired`,
`noop.rs:223-233`), `purge_expired_hash_fields` (`store/hashmap.rs:1050-1088`), `delete`
(`store/hashmap.rs:629-657`), `add_expired_keys` (`store/hashmap.rs:303-305`). The decision half is
*already* deep and unit-testable in isolation. What is missing is an object that orchestrates them
and hands back a result the shard can apply — instead the orchestration is dissolved into the loop.

## Proposed design

Introduce a small **module** with one **interface** method and a data **seam**:

```text
ActiveExpiryCoordinator::run_cycle(store: &mut dyn Store, now: Instant) -> ExpiryResult
```

The coordinator owns concern #2 (read expired sets) and #3 (delete under budget) — everything that
touches *only* the store. It returns an `ExpiryResult` describing what it deleted. The shard, past
the seam, owns concerns #4–#6 (tracking, search, notifications, probes, metrics, version) by reading
the result and applying effects. The pause/disable gates (concern #1) stay in the thin shard wrapper
because they read shard-owned atomics.

### What crosses the seam vs. what does not

This is the crux. The side effects do **not** move into the coordinator — they stay with the shard,
*past* the seam, because they depend on shard-owned state the store has no business seeing:
`self.tracking`, `self.search`, `self.subscriptions` (via `emit_keyspace_notification`),
`self.observability.metrics_recorder`, `self.shard_version`, and the USDT probes.

| Crosses the seam (in `ExpiryResult`) | Stays shard-side, applied past the seam |
|--------------------------------------|------------------------------------------|
| `deleted_keys: Vec<Bytes>` — keys whose own TTL elapsed | tracking invalidation (`tracking.invalidate_keys`) |
| `emptied_keys: Vec<Bytes>` — keys deleted because their last field expired | search-index delete (`delete_from_search_indexes`) |
| `fields_expired: u64` — total hash fields purged | keyspace notification (`emit_keyspace_notification`) |
| `budget_exhausted: bool` — whether the cycle stopped early | `fire_key_expired` probe, metrics counters, `increment_version` |

The coordinator never names `tracking`, `search`, `subscriptions`, `observability`, or
`shard_version`. It needs `&mut dyn Store` and a clock, nothing more. That is what makes it
unit-testable against a bare `HashMapStore` with no worker.

### New module: `frogdb-server/crates/core/src/shard/active_expiry.rs`

```rust
use std::time::{Duration, Instant};

use bytes::Bytes;

use crate::store::Store;

/// What one active-expiry cycle deleted. The seam between the coordinator
/// (decides + deletes) and the shard (applies tracking/search/notify/metrics
/// effects). Nothing in here references shard-only state, so the coordinator
/// is testable against a bare `Store`.
#[derive(Debug, Default, Clone)]
pub struct ExpiryResult {
    /// Keys removed because their own TTL elapsed. The shard fires the full
    /// effect set for each (tracking, search, `expired` notification, probe).
    pub deleted_keys: Vec<Bytes>,
    /// Keys removed because their last live hash field expired (hash emptied).
    /// The shard invalidates tracking + search for these. NOTE: today no
    /// notification/probe is fired for these — see `## Correctness flags`.
    pub emptied_keys: Vec<Bytes>,
    /// Total hash fields purged across all keys (drives `frogdb_fields_expired_total`).
    pub fields_expired: u64,
    /// True if the time budget stopped the cycle before draining all due keys.
    pub budget_exhausted: bool,
}

impl ExpiryResult {
    /// Keys expired at the key level (drives `add_expired_keys` + `frogdb_keys_expired_total`).
    pub fn keys_expired(&self) -> u64 {
        self.deleted_keys.len() as u64
    }

    /// Whether anything happened (gate for `increment_version`).
    pub fn is_empty(&self) -> bool {
        self.deleted_keys.is_empty() && self.emptied_keys.is_empty() && self.fields_expired == 0
    }
}

/// Owns the active-expiry decision + deletion. Holds the cycle's tunables so the
/// budget/sampling policy can evolve without touching `event_loop.rs`.
pub struct ActiveExpiryCoordinator {
    /// Per-cycle wall-clock budget (currently 25ms; see `event_loop.rs:138`).
    budget: Duration,
}

impl Default for ActiveExpiryCoordinator {
    fn default() -> Self {
        Self { budget: Duration::from_millis(25) }
    }
}

impl ActiveExpiryCoordinator {
    /// Run one active-expiry cycle: delete TTL-expired keys and purge expired
    /// hash fields, under the time budget. Returns what was deleted so the
    /// caller can apply side effects past the seam. Touches only the store.
    pub fn run_cycle(&mut self, store: &mut dyn Store, now: Instant) -> ExpiryResult {
        let mut result = ExpiryResult::default();
        let start = Instant::now();

        // --- Key-level expiry ---
        for key in store.get_expired_keys(now) {
            if start.elapsed() > self.budget {
                result.budget_exhausted = true;
                return result;
            }
            if store.delete(&key) {
                result.deleted_keys.push(key);
            }
        }

        // --- Field-level (hash field TTL) expiry ---
        let mut seen = std::collections::HashSet::new();
        let keys_with_expired_fields: Vec<Bytes> = store
            .get_expired_fields(now)
            .into_iter()
            .filter_map(|(key, _field)| seen.insert(key.clone()).then_some(key))
            .collect();

        for key in keys_with_expired_fields {
            if start.elapsed() > self.budget {
                result.budget_exhausted = true;
                break;
            }
            let existed_before = store.get(&key).is_some();
            result.fields_expired += store.purge_expired_hash_fields(&key) as u64;
            if existed_before && store.get(&key).is_none() {
                result.emptied_keys.push(key);
            }
        }

        result
    }
}
```

### The shard side: a thin wrapper + an effect applier

`ShardWorker` gains one field, `expiry: ActiveExpiryCoordinator` (`shard/worker.rs:33-100`,
constructed at the two `ShardWorker` builders), and `run_active_expiry` shrinks to the gates plus two
calls:

```rust
fn run_active_expiry(&mut self) {
    let paused = self.expiry_paused.load(std::sync::atomic::Ordering::Relaxed);
    self.store.set_expiry_suppressed(paused);
    if paused || self.debug_active_expire_disabled {
        return;
    }

    // Disjoint-field borrow: `self.expiry` and `self.store` are distinct fields.
    let result = self.expiry.run_cycle(&mut self.store, Instant::now());
    self.apply_expiry_effects(result);
}

/// Apply the side effects of an expiry cycle. Lives shard-side: it owns
/// tracking, search, notifications, probes, metrics, and the version counter.
fn apply_expiry_effects(&mut self, result: ExpiryResult) {
    for key in &result.deleted_keys {
        if self.tracking.has_tracking_clients() {
            self.tracking.invalidate_keys(&[key.as_ref()], 0);
        }
        self.delete_from_search_indexes(key);
        self.emit_keyspace_notification(key, "expired", KeyspaceEventFlags::EXPIRED);
        crate::probes::fire_key_expired(
            std::str::from_utf8(key).unwrap_or("<binary>"),
            self.shard_id() as u64,
        );
    }

    for key in &result.emptied_keys {
        if self.tracking.has_tracking_clients() {
            self.tracking.invalidate_keys(&[key.as_ref()], 0);
        }
        self.delete_from_search_indexes(key);
        // Behavior-preserving: today no notification/probe here (see Correctness flags).
    }

    if result.is_empty() {
        return;
    }
    let shard_label = self.shard_id().to_string();
    let keys_expired = result.keys_expired();
    if keys_expired > 0 {
        self.store.add_expired_keys(keys_expired);
        self.observability.metrics_recorder.increment_counter(
            "frogdb_keys_expired_total", keys_expired, &[("shard", &shard_label)],
        );
    }
    if result.fields_expired > 0 {
        self.observability.metrics_recorder.increment_counter(
            "frogdb_fields_expired_total", result.fields_expired, &[("shard", &shard_label)],
        );
    }
    self.increment_version();
}
```

### Before / after: the event-loop arm

The `select!` arm itself does not change (`event_loop.rs:57-65`); it still calls
`self.run_active_expiry()`. What changes is what that method *is*: a 116-line tangle becomes a
gate + `let result = self.expiry.run_cycle(...); self.apply_expiry_effects(result);`. The 6 concerns
split cleanly across the seam — decision/deletion on one side, effects on the other — and each side
is now nameable, callable, and testable on its own.

### Why this is the right depth

- **Locality.** Budget logic and the keys-with-expired-fields dedup live in `active_expiry.rs` and
  nowhere else; the effect ordering lives in `apply_expiry_effects` and nowhere else. A change to the
  expiry *policy* (adaptive sampling, a larger budget) is a one-module edit that cannot perturb the
  effects; a change to the *effects* (replicate the DEL, add a metric) cannot perturb the policy.
  Today both are one 116-line function where any edit risks the other.
- **Leverage.** The interface is one method returning one plain struct, yet it unlocks the entire
  test matrix below against a bare `HashMapStore` — no worker, no channels, no `tokio` runtime. Every
  future expiry change gets that test harness for free.
- **Deletion test, now passing.** `run_cycle` can be exercised by constructing a `HashMapStore`,
  setting TTLs, and asserting on `ExpiryResult` — deleting nothing else from the system. The
  behaviour that was reachable only by spinning the event loop becomes a 10-line unit test.
- **Not an adapter.** `ExpiryResult` is not a wrapper the shard may or may not consult — it is the
  only output of the cycle, and the side effects genuinely cannot move behind it (they need
  shard-owned state). The seam is drawn exactly where the dependency boundary already is: store on
  one side, shard services on the other.

## Migration plan

Each phase compiles and passes `just test frogdb-core`; each is behavior-preserving.

1. **Phase 0 — add the module, no call-site change.** Create `shard/active_expiry.rs` with
   `ExpiryResult` + `ActiveExpiryCoordinator::run_cycle`, exported from `shard/mod.rs`. Add the unit
   tests (below). `run_active_expiry` is untouched. `just check frogdb-core && just test frogdb-core`.
2. **Phase 1 — route the cycle through the coordinator.** Add the `expiry: ActiveExpiryCoordinator`
   field to `ShardWorker` (both builders, `shard/worker.rs`). Replace the decision/deletion half of
   `run_active_expiry` with `self.expiry.run_cycle(...)`, keeping the side effects *inline* for one
   commit so the diff is reviewable as "extract decision, no behaviour change." Verify the
   Redis-compat expiry tests are green.
3. **Phase 2 — extract `apply_expiry_effects`.** Move the inline effects from Phase 1 into
   `apply_expiry_effects(result)`, driven by `deleted_keys`/`emptied_keys`/`fields_expired`. This is
   the commit that finalizes the seam. `run_active_expiry` is now the gate + two calls. Re-run the
   compat suite; the effect order and counts must be byte-identical to `git show HEAD~2`.
4. **Phase 3 (optional, gated on a decision) — fix the flagged gaps.** Each `## Correctness flags`
   item becomes a one-line change in `apply_expiry_effects` (emit notification/probe for
   `emptied_keys`; count `emptied_keys` in `add_expired_keys`) or in `run_cycle` (bound the scan by
   the budget). Each lands as its own commit with a regression test, *after* the behavior-preserving
   refactor is in, so the fix is a visible, reviewable behaviour change rather than a silent rider.

## Testing impact

All of these are impossible today (no reachable unit), and trivial once `run_cycle` exists — each
constructs a `HashMapStore`, sets TTLs, calls `run_cycle`, and asserts on `ExpiryResult`:

- **Budget honored.** With more due keys than fit in the budget, assert `budget_exhausted == true`
  and `deleted_keys.len() < total_due`. Use an injected/short budget so the test is deterministic
  (see the borrow/clock note in Risks).
- **Metric counted once, no double-delete.** `keys_expired()` equals the number of keys actually
  removed; a key already gone (e.g. lazily purged) is not counted. Pins that `delete` returning
  `false` does not increment the count — guards against the double-count class.
- **`add_expired_keys` not double-applied.** A direct test that `Store::delete` (`hashmap.rs:629`)
  does *not* bump the `expired_keys` counter and `add_expired_keys` (`hashmap.rs:303`) does — so the
  shard's `add_expired_keys(keys_expired())` is the single source. (See Correctness flag #2.)
- **Field sweep classification.** A hash with one expiring field → `fields_expired == 1`,
  `emptied_keys` empty; a hash whose last field expires → key in `emptied_keys`, and the underlying
  key is gone from the store.
- **Dedup.** Multiple expired fields on one key produce exactly one `purge_expired_hash_fields` call
  (one entry considered), matching `event_loop.rs:185-192`.
- **Effects applier, separately.** `apply_expiry_effects` becomes testable against a worker with a
  hand-built `ExpiryResult` (no timer, no store TTLs) — assert notifications/probes/metrics fire per
  `deleted_keys` and per `emptied_keys`, isolating the effect logic from the decision logic.

The Redis-compat and Jepsen suites remain the end-to-end guarantee that no externally observable
reply or event changed across Phases 1–2.

## Risks / open questions

- **Time budget vs. unbounded scan (real today).** The 25ms budget gates only the delete/purge
  loops. `get_expired_keys(now)` (`event_loop.rs:143`) and `get_expired_fields(now)`
  (`event_loop.rs:182`) each walk the time-ordered index and **clone every due entry into a `Vec`
  before the budget loop runs** (`ExpiryIndex::get_expired`, `noop.rs:99-112`;
  `FieldExpiryIndex::get_expired`, `noop.rs:223-233`). Under an expiry avalanche the loop can block
  well past 25ms in the scan+clone alone. The coordinator is the right home for the fix: switch to a
  bounded `sample(n)` (`ExpiryIndex::sample`, `noop.rs:118-130`) or pass a cap into `get_expired_keys`
  so the scan respects the budget too. This is the adaptive-sampling design space Redis occupies
  (`activeExpireCycle` samples `ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP` per db and re-runs if >25% are
  still expiring). Listed as Correctness flag #3.
- **Adaptive sampling.** Once `run_cycle` returns `budget_exhausted`, the shard could reschedule the
  next tick sooner (Redis's "keep going while >25% expired"). The `ActiveExpiryCoordinator` struct is
  the place to carry that cursor/ratio state across cycles without touching `event_loop.rs`. Out of
  scope for the behavior-preserving migration; the field gives it a home.
- **Ordering of effects (delete before/after notify).** Current order is delete → tracking → search →
  notify → probe (`event_loop.rs:153-171`). The seam preserves this: `run_cycle` deletes first and
  the shard applies effects after, so subscribers/trackers are notified only for keys that were truly
  deleted (`store.delete` returned `true`). The proposal keeps that invariant; any reordering is a
  deliberate, separate change. Note this *differs* from the post-execution pipeline's canonical
  `WRITE_EFFECT_ORDER` (proposal 03) — a follow-up could ask whether active expiry should route
  through that same ordering rather than a parallel one.
- **Borrow shape.** `self.expiry.run_cycle(&mut self.store, ...)` borrows two disjoint fields of
  `self`, which the compiler accepts (split borrows). `apply_expiry_effects` then takes the owned
  `ExpiryResult`, so no borrow of `self` overlaps the cycle. The coordinator needs `&mut dyn Store`
  only — it must *not* close over `&mut self`, or the seam collapses. For deterministic budget tests,
  `run_cycle` should take the budget from `self.budget` (settable) rather than hardcoding it; injecting
  `now` already lets tests control the clock.
- **Interaction with lazy expiry.** Active expiry shares state with the lazy/passive path:
  `set_expiry_suppressed` (`event_loop.rs:126`) toggles a flag that makes `check_and_delete_expired`
  (`hashmap.rs:264-295`) a no-op during CLIENT PAUSE, and `Store::get` (`&mut self`) can lazily purge
  a key mid-cycle — note `store.get(&key)` at `event_loop.rs:203,208` can itself trigger lazy
  deletion. Keeping the pause/disable gates in the shard wrapper (not the coordinator) preserves this
  coupling exactly. The `expiry_suppressed` semantics (`hashmap.rs:312-315`, `:264-271`) are
  deliberately untouched.

## Correctness flags

Found while tracing `run_active_expiry`; each is separable from the refactor and would land in
Phase 3 with a regression test. The seam makes all three *visible and testable* (today they are
buried in the loop).

- **Field-emptied key deletion emits no notification and fires no probe.**
  `event_loop.rs:207-214` — when a hash is emptied by `purge_expired_hash_fields` and the key is
  deleted, the code invalidates tracking and removes from search indexes but does **not**
  `emit_keyspace_notification` and does **not** `fire_key_expired`, unlike the key-level path
  (`event_loop.rs:165-171`). A key that vanishes because its last field expired is invisible to
  keyspace-notification subscribers and USDT probes. (Redis emits a generic `del` when a hash empties
  via field TTL, plus `hexpired` per field — FrogDB emits neither in active expiry.)
- **Field-emptied key deletion is not counted in the `expired_keys` stat.**
  `event_loop.rs:218-237` — only `deleted_count` (key-level) feeds `add_expired_keys`
  (`hashmap.rs:303`) and `frogdb_keys_expired_total`. A whole key removed via the field sweep bumps
  only `frogdb_fields_expired_total`, and `Store::delete` (`hashmap.rs:629-657`) does not touch the
  counter (only `check_and_delete_expired` at `hashmap.rs:291` does). So INFO `expired_keys`
  (`diagnostics.rs:50`) under-counts genuine key expirations.
- **Time budget does not cover the expired-set scan.** `event_loop.rs:143,182` — `get_expired_keys`
  and `get_expired_fields` clone every due entry into a `Vec` before the budgeted loop begins
  (`noop.rs:99-112`, `:223-233`), so the 25ms budget (`event_loop.rs:138`) bounds deletions but not
  the scan+allocation. A large TTL avalanche can stall the shard event loop past the budget. (Also in
  Risks; the coordinator is where a bounded-sample fix belongs.)
</content>
</invoke>
