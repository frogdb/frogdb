# Proposal 14 — Route internal key removals (active-expiry, eviction) through `WRITE_EFFECT_ORDER`

## Summary

**Four** internal-removal paths delete keys from the **Store** *outside* the canonical
post-execution pipeline the whole codebase was refactored onto. This proposal migrates
**two** of them — active expiry (`apply_expiry_effects`, `event_loop.rs:154-230`) and
eviction-delete (`delete_for_eviction`, `eviction.rs:266-307`) — onto the seam, and
**explicitly defers** a third, **lazy expiry** (`Store::check_and_delete_expired`,
`store/hashmap.rs:421-435`), for a structural reason spelled out below; the fourth
"path", `spill_for_eviction`, is shown *not to be a removal* at all. So the accurate
framing is **"reduce the write-effect invariant from four homes to two,"** not "close
the fragmentation" — see [Scope and the lazy-expiry gap](#scope-and-the-lazy-expiry-gap).

Active expiry and eviction-delete each hand-roll a *different, partial*
subset of the nine effects in [`WRITE_EFFECT_ORDER`](../../../frogdb-server/crates/core/src/shard/post_execution.rs)
— the exact anti-pattern that module's own docs (`post_execution.rs:20-31`) say the
pipeline was built to eliminate, and the exact anti-pattern the scatter path was
already migrated off (`scatter_del`, `execution.rs:867-909`). Expiry runs
tracking-invalidation + search-delete + notification + stream-waiter-drain + version
but **skips FrogDB WAL persist, replication broadcast, and the dirty counter**.
Eviction-delete runs version + `evicted` notification + metrics but **skips tracking
invalidation, search-index delete, waiter drain, WAL, replication, and the dirty
counter** — so an evicted key stays live in the search index and in RESP3 tracking
caches, and (verified below) resurrects on restart.

The deepening: reconstruct each internal removal as a synthetic `DEL`/`UNLINK` write
and drive it through `run_write_effects` under a new `EffectScope::InternalRemoval`
that carries (a) the *removal reason* selecting the keyspace-event class
(`expired` / `del` / `evicted`) and (b) a *propagation policy* governing WAL and
replication — exactly the seam `scatter_del` already uses, extended with the one
dimension internal removals need that a user `DEL` does not.

Two candidate claims are **corrected** by the investigation (see Evidence): the
"restart resurrects" hazard is **real for eviction but not for expiry** (recovery
filters expired keys at load — `recovery.rs:100-105`), and `spill_for_eviction` is
**not a removal** at all (the value moves to the **Warm Tier** and stays logically
present) — so it must be excluded from the removal pipeline, and its own version
bump + `evicted` notification are a separate, orthogonal defect.

## Files involved (verified paths + current line counts)

| File | Lines | Role in the current fragmented design |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | 823 | Canonical pipeline: `WRITE_EFFECT_ORDER` (L156-166), `run_write_effects` (L179-297), `run_scatter_effects` (L315-342), `EffectScope` (L97-116), `invalidate_written_keys` (L396-413) |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 706 | `apply_expiry_effects` (L154-230) — the hand-rolled active-expiry effect subset |
| `frogdb-server/crates/core/src/shard/eviction.rs` | 308 | `delete_for_eviction` (L266-307), `spill_for_eviction` (L214-263) — hand-rolled eviction effect subsets |
| `frogdb-server/crates/core/src/shard/execution.rs` | 1733 | `scatter_del` (L867-909) + `scatter_write_handler` (L834-838) — the pattern to copy |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 673 | `ActiveExpiryCoordinator::run_cycle` (L112-…) returns `ExpiryResult` (blind to effects) |
| `frogdb-server/crates/core/src/store/hashmap.rs` | — | `check_and_delete_expired` (L421-435) → `uninstall` (L366-408) — **lazy expiry**, the deferred fourth path (emits zero canonical effects; deep inside a borrowed `&mut store`) |
| `frogdb-server/crates/core/src/shard/blocking.rs` | — | `drain_stream_waiters_with_error` (L319) — the stream NOGROUP drain expiry hand-rolls |
| `frogdb-server/crates/persistence/src/recovery.rs` | 491 | `recover_shard_into` (L87-…) skips already-expired keys at load (L100-105, L156-160) |
| `frogdb-server/crates/commands/src/basic.rs` | 964 | `DelCommand` spec (L765-785): `WaiterWake::All` (L777), `event del/GENERIC` (L779), `WalStrategy::DeleteKeys` |
| `frogdb-server/docs/adr/0001-raft-cluster-metadata.md` | — | Data path never goes through Raft; internal removals are pure data-path — untouched here |

## Problem

`WRITE_EFFECT_ORDER` (`post_execution.rs:156-166`) is the codebase's single source of
truth for *which* side effects a write produces and *in what order*: version bump →
tracking invalidation → keyspace notifications → dirty counter → waiter satisfaction
→ keysizes flush → **WAL persist** → search index → **replication broadcast**. The
module docs are explicit that this exists to kill hand-rolled subsets:

> "The cross-shard scatter write path … used to hand-roll its *own* subset of these
> effects inline — each in a slightly different combination, silently skipping
> keyspace notifications, replication broadcast, waiter satisfaction, the dirty
> counter, and search-index upkeep." (`post_execution.rs:22-26`)

`scatter_del` was migrated onto the pipeline exactly this way — it deletes from the
Store, then reconstructs the removed keys as a synthetic `DEL`/`UNLINK` and calls
`run_scatter_effects` (`execution.rs:894-907`), inheriting every effect in order.

But two **internal** removal paths never made that migration and remain hand-rolled:

**Active expiry** (`apply_expiry_effects`, `event_loop.rs:154-230`) runs, per removed
key: tracking invalidation (L158-160), search-index delete (L163), a reason-specific
keyspace notification — `expired` for TTL-elapsed keys (L166), `del` for
hash-field-emptied keys (L186) — a USDT probe, XREADGROUP stream-waiter drain
(L204-206), aggregate metrics, and one version bump per cycle (L229). It **skips**
FrogDB WAL persist, replication broadcast, and the dirty counter.

**Eviction delete** (`delete_for_eviction`, `eviction.rs:266-307`) runs: version bump
(L279), `record_evicted` (L280), an `evicted` keyspace notification (L283), and
eviction metrics. It **skips** tracking invalidation, search-index delete, waiter
drain, FrogDB WAL persist, replication broadcast, and the dirty counter.

So the invariant "a key leaving the Store produces *these* effects in *this* order"
is stated in **four** places — the canonical order plus **three** divergent partial
hand-rolls (active expiry, eviction-delete, and lazy expiry) — maintained by hand,
differing on which effects they include. That is precisely the fragmentation
`WRITE_EFFECT_ORDER` was built to remove; internal removals are simply write paths that
never got onto the seam. This proposal moves the **two** batch, effect-scoped paths
(active expiry, eviction) onto the seam — reducing four homes to two — and documents
why lazy expiry (a per-key removal fired mid-command from inside a borrowed `&mut store`)
cannot cleanly join them without a separate change (below).

### Scope and the lazy-expiry gap

There is a **fourth** internal-removal path this proposal does **not** migrate, and it
must be named honestly because it changes the headline claim. **Lazy expiry**
(`Store::get_with_expiry_check` → `check_and_delete_expired` → `uninstall`,
`store/hashmap.rs:421-435`, `366-408`) physically removes an expired key from the
in-memory Store on *any read that touches it*, and emits **none** of the canonical
effects: no keyspace notification (the only `"expired"` emit in the whole core crate is
active expiry at `event_loop.rs:166` — verified), no tracking invalidation, no
search-index delete, no WAL persist, no replication, no stream-waiter drain, no dirty
counter. It fires from many high-frequency call sites (every read path through
`get_with_expiry_check` — e.g. `execution.rs:310`, `798`, `880`, and the blocking/typed
read paths), i.e. **far more often** than active expiry's 100ms cycle. So lazy expiry
carries the **same** stale-RESP3-tracking and search-index-leak bugs this proposal
attributes to eviction, at higher frequency.

Lazy expiry is **deferred, not folded in**, for a concrete structural reason: it happens
**deep inside `&mut self` Store methods invoked mid-command while the Store is already
mutably borrowed** by the executing command. `run_internal_removal_effects` /
`run_write_effects` is `async` and needs `&mut self` on the **`ShardWorker`** (to reach
tracking, search, WAL, the broadcaster), which is not available from inside a
`Store::check_and_delete_expired` call frame. Routing its effects through the pipeline
therefore requires either (a) deferring lazy-removal effects to a post-command drain
(collect expired-key ids in the Store, replay them through `run_internal_removal_effects`
after the command returns and the Store borrow is released), or (b) restructuring the
read path so expiry is detected *before* the command borrows the Store. Both are
materially larger than the two batch paths, which already sit at the `ShardWorker` level
with the Store un-borrowed. That work is **out of scope here** and tracked as a follow-up;
the shared `RemovalReason::Expired` seam this proposal builds is exactly what a future
lazy-expiry drain would reuse, so this proposal is a prerequisite, not a dead end.

Consequently the deliverable is **"reduce the invariant from four homes to two"** — a
genuine and compiler-checkable reduction that also fixes the eviction correctness bugs —
**not** "single source of truth achieved." The title's `(active-expiry, eviction)` scope
is delivered in full; lazy expiry is named, its identical bug class acknowledged, and its
deferral justified rather than glossed.

## Evidence (verified file:line)

### The canonical pipeline and its own anti-pattern verdict — CONFIRMED

`WRITE_EFFECT_ORDER` is a `[WriteEffectKind; 9]` (`post_execution.rs:156-166`) iterated
by `run_write_effects` (`post_execution.rs:194-296`). `EffectScope::ScatterPart`
(`post_execution.rs:103-115`) is the existing mechanism for driving reconstructed
writes through it; `run_scatter_effects` (`post_execution.rs:315-342`) is the entry
point. `scatter_del` reconstructs removed keys as a `DEL`/`UNLINK` handler resolved via
`scatter_write_handler` (`execution.rs:834-838`, panics on a missing registration
rather than silently dropping effects) and calls `run_scatter_effects`
(`execution.rs:903-906`). The module docs at `post_execution.rs:20-31` describe the
hand-rolled-subset anti-pattern verbatim. **All confirmed.**

### Expiry's hand-rolled subset — CONFIRMED (with one refinement)

`apply_expiry_effects` (`event_loop.rs:154-230`): tracking invalidation
(L158-160, L180-182), `delete_from_search_indexes` (L163, L184), reason-specific
notification (L166 `expired`, L186 `del`), `drain_stream_waiters_with_error`
(L204-206), version bump (L229). No `persist(...)` call, no
`replication_broadcaster` call, no `update_dirty_counter`/`increment_dirty` call
anywhere in the function. **Skips WAL, replication, dirty — confirmed.**

*Refinement of the candidate's "waiter-drain" claim:* expiry drains **only**
XREADGROUP stream waiters (`drain_stream_waiters_with_error`, `blocking.rs:319-333`,
sending NOGROUP), not general waiters. This is correct behavior for a removal and,
notably, is **exactly what a synthetic `DEL` through the pipeline already produces**:
`DelCommand` declares `WaiterWake::All` (`basic.rs:777`) whose own spec comment reads
"Stream waiters (XREADGROUP) need NOGROUP when their stream disappears; list/zset
waiters gracefully find no data and stay blocked" (`basic.rs:773-776`). The pipeline's
`WaiterSatisfaction` effect (`post_execution.rs:221-225` → `satisfy_waiters_for_command`
→ stream check → `DrainNoGroup` → `drain_stream_waiters_with_error`, `blocking.rs:232`)
subsumes the F1 hand-rolled drain. Routing expiry through the pipeline **removes** the
special-case drain rather than duplicating it.

### Eviction's hand-rolled subset — CONFIRMED

`delete_for_eviction` (`eviction.rs:266-307`): `increment_version` (L279),
`record_evicted` (L280), `emit_keyspace_notification(..., "evicted", EVICTED)` (L283),
metrics (L288-293). No `invalidate_written_keys`/`tracking`, no
`delete_from_search_indexes`, no waiter drain, no `persist`, no
`replication_broadcaster`, no dirty counter. **All skips confirmed.** Consequence, as
the candidate states: an evicted key remains addressable via any search index built
over it, and RESP3 client-side-caching (tracking) clients are never invalidated, so
they serve a stale cached value for a key the server no longer holds — a direct
violation of the observability/correctness-accuracy rule.

### Lazy expiry's hand-rolled subset — CONFIRMED (the deferred fourth path)

`check_and_delete_expired` (`store/hashmap.rs:421-435`): when the entry
`is_expired()` and expiry is not suppressed, it calls `self.uninstall(key)`
(`store/hashmap.rs:430` → `366-408`, the physical hashmap removal) and bumps
`expired_keys` (L431). That is the **entire** side-effect set — a pure in-memory
delete plus a counter. No keyspace notification, no tracking invalidation, no
search-delete, no waiter drain, no `persist`, no `replication_broadcaster`, no dirty
counter. Verified: the sole `"expired"` keyspace emit in the whole core crate is active
expiry (`event_loop.rs:166`); lazy expiry emits nothing. It runs from every
`get_with_expiry_check` call site (`execution.rs:310`, `798`, `880`, plus blocking/typed
read paths), so it is the **highest-frequency** removal path and carries the identical
stale-tracking + search-leak bug class as eviction. It is **not migrated** by this
proposal because it removes from inside a borrowed `&mut Store` mid-command, where the
`ShardWorker`-level async effect pipeline is unreachable — see
[Scope and the lazy-expiry gap](#scope-and-the-lazy-expiry-gap) for the deferral
rationale and the follow-up shape.

### Restart resurrection — CORRECTED: real for eviction, NOT for expiry

The candidate's "check whether skipping WAL means a restart resurrects
expired/evicted keys" resolves **asymmetrically**:

- The **FrogDB WAL** is "a redo-of-current-state buffer" that "serializes a key's
  *current state* into RocksDB write batches" (`CONTEXT.md:47-51`). A removal that
  skips WAL persist never writes the delete/tombstone, so RocksDB retains the key's
  last-persisted value.
- **Expiry: no resurrection.** The recovery load path `recover_shard_into`
  (`recovery.rs:87-129`) deserializes each key and, *at load time*, skips any key with
  `expires_at <= now` (`recovery.rs:100-105`; warm-tier equivalent `recovery.rs:156-160`,
  which also cleans up the stale entry). An expired key that was never WAL-deleted is
  filtered out on restart. The recovery test suite pins this (`recovery.rs:262-411`,
  `hot_expired`/`warm_expired`). So the WAL skip on expiry is largely *benign for
  correctness* — its cost is stale entries lingering in RocksDB until recovery or
  compaction, plus the divergences below.
- **Eviction: genuine resurrection.** Eviction removes non-expired keys (e.g.
  `allkeys-lru`/`allkeys-random`/`allkeys-lfu` evict keys with no TTL at all). Such a
  key has `expires_at == None`, so the recovery filter does not skip it — RocksDB
  still holds it, and restart reloads it, re-inflating memory the eviction was meant
  to reclaim. **This is a real correctness bug**, not merely cosmetic: eviction is not
  durable.

### Replica divergence — CONFIRMED, and it is the deliberate-vs-accidental crux

`run_active_expiry` (`event_loop.rs:126-146`) gates only on `expiry_paused`
(CLIENT PAUSE) and `debug_active_expire_disabled` — **not** on Primary/Replica role.
A **Replica** therefore runs active expiry on its own monotonic clock, independent of
its Primary. This confirms the candidate's suspicion and marks a deliberate divergence
from Redis, where a Replica never expires independently and instead applies an explicit
`DEL`/`UNLINK` the Primary propagates on both lazy and active expiry (see Redis
`expireIfNeeded` → `propagateDeletion`, `activeExpireCycle`). Consequences:

- Expiry's skipped replication broadcast is *semi-deliberate*: because each node
  expires independently, no propagation is strictly required for eventual convergence.
  But independent clocks mean transient Primary/Replica divergence (a key gone on one,
  live on the other), and lazy expiry on the Primary (`get_with_expiry_check`,
  invoked e.g. at `execution.rs:880`) similarly does not propagate. Whether to keep
  independent expiry or adopt Redis-style propagation is a **policy decision** the
  unified seam should make *explicit* rather than leave as an accidental omission.
- Eviction's skipped replication broadcast is **not** cushioned the same way. Redis
  drives eviction from the Primary and propagates the `DEL` so Replicas evict the
  *same* keys; a Replica sampling its own memory would pick *different* keys, diverging
  the datasets. FrogDB's skip means a Replica retains a key the Primary evicted (until
  the Replica independently hits its own limit and samples differently). This is a
  divergence hazard, not a deliberate design.

### `spill_for_eviction` is NOT a removal — CORRECTED (exclude from scope)

`spill_for_eviction` (`eviction.rs:214-263`) calls `self.store.spill_key(key)`
(`store/hashmap.rs`), which moves the value hot→warm into the **Warm Tier** RocksDB
column family (`CONTEXT.md:78-84`); the key stays logically present and **unspills**
on next access. So its *omission* of tracking invalidation and search-index delete is
**correct** — the key still exists and its value is unchanged; invalidating tracking
caches or deleting the search entry would be wrong. `spill_for_eviction` therefore
must be **excluded** from the removal pipeline. Two of its current side effects are,
however, independently questionable and worth a separate note:

- It calls `increment_version` (`eviction.rs:221`). A spill changes no logical value,
  so bumping the per-shard WATCH version can spuriously abort an in-flight
  `WATCH`/`MULTI` on an untouched key.
- It emits an `evicted` keyspace notification (`eviction.rs:224`) for a key that was
  *not* evicted (still readable) — a notification-accuracy issue.

These are flagged as follow-ups, not fixed by this proposal (which is about the
removal seam).

### Path-prefix discrepancy in the candidate — CORRECTED

Every file:line in the candidate is written as `core/src/shard/…`; the actual path is
`frogdb-server/crates/core/src/shard/…`. Line numbers are accurate; only the crate
prefix was missing.

## Why it is shallow/fragmented (architecture vocabulary)

**The write-effect invariant has four homes.** `WRITE_EFFECT_ORDER` is a deep
**Module**: one small **Interface** (`run_write_effects(summary, wal, scope)`) over the
substantial, order-sensitive implementation of nine effects. Internal removals bypass
that interface and re-implement partial copies inline — reintroducing exactly the
fragmentation the module docs claim to have closed. The **seam** that `scatter_del`
reaches through (`run_scatter_effects` + `EffectScope::ScatterPart`) already exists and
already handles "reconstruct removed keys as a synthetic `DEL` and inherit every
effect"; the two batch internal paths (active expiry, eviction) simply never adopted it.
Poor **Locality**: to know what happens when a key leaves the Store you must read four
functions in four files and diff their effect sets by hand. This proposal migrates the
two batch paths, taking the count from four homes to two; the fourth (lazy expiry) is
deferred for the structural reason above but reuses the same `RemovalReason::Expired`
seam once a post-command drain exists.

**Deletion test.** Delete the effect ordering from `apply_expiry_effects` /
`delete_for_eviction` and route them through the pipeline: no behavior that *should*
exist is lost — instead the *missing* effects (WAL for eviction, search-delete +
tracking for eviction, dirty counter for both) are gained, and the duplicated
stream-drain / notification code is subsumed. Conversely, add a tenth effect to
`WRITE_EFFECT_ORDER` today and the two internal paths silently do not get it — the
compiler cannot hold them to completeness, the same failure class the scatter
migration fixed.

**Leverage.** The pipeline already carries the per-scope variation (Command /
Transaction / ScatterPart) as *data* (`EffectScope`), not control flow. Internal
removals add one genuinely new axis — *why* the key left (expired / emptied / evicted)
and *whether to propagate* — which belongs as one more data dimension on the same seam,
not as two bespoke functions.

## Proposed design (Rust interface sketch)

Add an `InternalRemoval` scope that carries the two dimensions internal removals need
beyond a user `DEL`: the notification reason and the propagation policy. Signatures /
types only.

```rust
// post_execution.rs

/// Why an internal (non-command) removal happened. Selects the keyspace-event
/// class the pipeline emits, replacing DelCommand's fixed `del`/GENERIC event.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RemovalReason {
    /// Whole-key TTL elapsed (active or lazy). Emits `expired` / EXPIRED.
    Expired,
    /// Key removed because its last live hash field expired. Emits `del` / GENERIC.
    FieldEmptied,
    /// Key removed to reclaim memory under maxmemory. Emits `evicted` / EVICTED.
    Evicted,
}

/// Propagation policy for an internal removal — makes the WAL/replication decision
/// explicit data instead of an accidental omission (see Evidence: replica divergence).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct RemovalPropagation {
    /// Persist the delete to the FrogDB WAL (durability). Recommended `true` for
    /// eviction (fixes restart resurrection); policy choice for expiry.
    pub wal: bool,
    /// Broadcast the synthetic DEL/UNLINK to Replicas. Policy choice — see Risks.
    pub replicate: bool,
}

/// Extend the existing scope enum with the internal-removal case.
pub(crate) enum EffectScope {
    Command,
    Transaction,
    ScatterPart,
    /// A key removed by the engine itself (expiry/eviction), not by a client
    /// command. Carries the removal reason (notification class) and propagation
    /// policy (WAL/replication). Version bump follows the same one-per-batch,
    /// dirty-delta rule as ScatterPart.
    InternalRemoval { reason: RemovalReason, propagation: RemovalPropagation },
}

impl ShardWorker {
    /// Drive a batch of engine-initiated removals through WRITE_EFFECT_ORDER.
    /// Reconstructs each removed key as a synthetic DEL (or UNLINK) — mirroring
    /// `run_scatter_effects` — and runs the canonical effect set. The `reason`
    /// overrides the synthetic handler's keyspace-event class; `propagation`
    /// gates the WAL and ReplicationBroadcast effects.
    pub(crate) async fn run_internal_removal_effects(
        &mut self,
        removed_keys: Vec<Bytes>,
        reason: RemovalReason,
        propagation: RemovalPropagation,
        conn_id: u64,             // typically 0 (engine); never REPLICA_INTERNAL_CONN_ID
    );
}
```

The keyspace-notification effect learns to read the reason from the scope:

```rust
// In run_write_effects, WriteEffectKind::KeyspaceNotifications arm:
//   EffectScope::InternalRemoval { reason, .. } => emit `reason`'s class per key
//   otherwise                                    => existing per-command notification
// And WalPersistence / ReplicationBroadcast arms consult `propagation` when the
// scope is InternalRemoval, leaving Command/Transaction/ScatterPart unchanged.
```

Call-site shape (the hand-rolled bodies collapse to a store mutation + one call):

```rust
// eviction.rs — delete_for_eviction becomes:
//   forget_key; capture memory_freed; if store.delete(key) {
//       record metrics + probe (eviction-specific, stay here);
//       run_internal_removal_effects(vec![key], Evicted,
//           RemovalPropagation { wal: true, replicate: <policy> }, 0).await; }
// version bump, tracking, search-delete, dirty, waiter-drain now come from the pipeline.

// event_loop.rs — apply_expiry_effects becomes: partition deleted_keys (Expired) and
//   emptied_keys (FieldEmptied); record metrics/probes (expiry-specific, stay here);
//   run_internal_removal_effects for each reason group. The manual tracking/search/
//   notification/stream-drain/version blocks (L156-206, L229) are deleted.
```

Note `delete_for_eviction` currently returns `bool` and is called from a *synchronous*
`evict_one` chain (`eviction.rs:117-130`) that is itself invoked synchronously from
`check_memory_for_write` (`eviction.rs:74-95`), whereas `run_write_effects` is `async`
(its only suspension point is WAL persist, `post_execution.rs:229-240`). Threading
`async` up the eviction call chain — or giving internal removals a synchronous WAL
enqueue — is the one non-mechanical part of the migration (see Migration plan step 4).

`spill_for_eviction` is **not** modified to use this seam (it is not a removal); its
version-bump / notification questions are tracked separately.

## Migration plan (ordered steps)

1. **Land the eviction-durability fix as the headline correctness change.** Add
   `EffectScope::InternalRemoval`, `RemovalReason`, `RemovalPropagation`, and
   `run_internal_removal_effects` to `post_execution.rs`; teach the
   `KeyspaceNotifications`, `WalPersistence`, and `ReplicationBroadcast` arms to read
   the new scope. Extend the pipeline's exhaustiveness tests
   (`post_execution.rs:454-585`) to cover the new scope.
2. **Migrate `delete_for_eviction`** (`eviction.rs:266-307`) to
   `run_internal_removal_effects(reason = Evicted, wal = true)`. Keep the
   eviction-specific metrics/probe (`record_evicted`, `EvictionKeysTotal`,
   `EvictionBytesTotal`, `fire_key_evicted`) at the call site — they are not pipeline
   effects, mirroring how `scatter_del` keeps lazyfree accounting local
   (`execution.rs:895-901`). This alone closes the search-index-leak, stale-tracking,
   and restart-resurrection bugs.
3. **Migrate `apply_expiry_effects`** (`event_loop.rs:154-230`): partition
   `deleted_keys` → `Expired`, `emptied_keys` → `FieldEmptied`; call
   `run_internal_removal_effects` per group; delete the hand-rolled tracking /
   search-delete / notification / stream-drain / version blocks. Retain the aggregate
   expiry metrics and USDT probes (L208-228) at the call site. Decide expiry's
   `RemovalPropagation` (see Risks) — recommend `wal = true` (drop stale entries at
   source) and start with `replicate = false` to preserve the current independent-expiry
   model, flipping later only with a deliberate ADR.
   - **Version-bump semantics change (call out explicitly).** Active expiry today
     increments the WATCH version **exactly once per cycle** when the result is non-empty
     (`event_loop.rs:229`). Two `run_internal_removal_effects` calls (Expired group,
     FieldEmptied group) each run `VersionIncrement` gated on `dirty_delta >= 0`, so a
     cycle that removes *both* whole-key-expired **and** field-emptied keys would produce
     **two** version bumps instead of one. This is **benign** — an extra WATCH-version
     bump only causes conservative, correct MULTI aborts — but it is a behavior change.
     Preferred resolution: **coalesce** by giving `run_internal_removal_effects` (or a
     thin wrapper) the ability to take both reason groups and emit a **single**
     `VersionIncrement` for the whole cycle while still selecting the per-key
     notification class from each key's reason. If coalescing proves awkward, accept the
     two bumps and pin the semantics with a test. Do **not** ship this silently.
4. **Resolve the sync→async seam** for eviction — and note it is more than "thread
   `async`". `check_memory_for_write` (`eviction.rs:74-95`) is a **pre-flight** that runs
   **before the triggering command executes**, looping up to **10** eviction attempts
   (`eviction.rs:73-96`), each currently calling the synchronous `delete_for_eviction`.
   After migration, each attempt runs a **complete, WAL-awaiting** synthetic-DEL pipeline
   — so a single write can drive up to 10 full awaited effect pipelines *before* the
   command's own effects. This is **Redis-consistent** (Redis propagates eviction DELs
   ahead of the command that triggered them) and there is **no re-entrancy/recursion
   risk**: the synthetic DEL is routed through `run_write_effects`, **not**
   `execute_command`, so it never re-enters `check_memory_for_write` (the eviction path
   is not itself a "write" that trips the memory check). The ordering is therefore
   sound; the cost is that the memory check is no longer a cheap synchronous probe.
   Options, in preference order: (a) make the eviction pre-write check `async` and
   `.await` it at its call site (`execution.rs:120`; it already runs on the shard task);
   (b) give `InternalRemoval` a synchronous WAL-enqueue variant so no suspension is
   needed (WAL persist is the sole `await` in the pipeline). Pick during implementation;
   (a) is simpler and keeps one WAL path.
5. **Verify no crate-dependency inversion.** Everything is inside `frogdb-core`
   (`shard/*`); `persistence`/`replication` are consumed through the same
   `self.persist(...)` and `self.replication_broadcaster` seams the pipeline already
   uses. No new cross-crate edges (respects ADR crate-direction and the
   data-path-never-through-Raft rule — internal removals are pure data-path).
6. **Follow-up (separate change): `spill_for_eviction`** — evaluate dropping its
   `increment_version` (spill changes no value) and correcting its `evicted`
   notification. Out of scope here.
7. **Follow-up (separate change): lazy expiry drain.** Route
   `check_and_delete_expired` (`store/hashmap.rs:421-435`) through the same
   `RemovalReason::Expired` seam. Because it removes from inside a borrowed `&mut Store`
   mid-command, it needs a **post-command drain**: have the Store record the ids it
   lazily uninstalled (e.g. a small `Vec<Bytes>` cleared each command), then replay them
   through `run_internal_removal_effects(Expired, …)` at the `ShardWorker` level once the
   command returns and the Store borrow is released. This closes the highest-frequency
   removal path's identical stale-tracking + search-leak bugs. Out of scope here; this
   proposal's seam is its prerequisite.

## Test plan

- **Pipeline unit tests (extend `post_execution.rs` tests, socket-free).** Reuse the
  `RecordingBroadcaster` harness (`post_execution.rs:612-708`): assert that
  `run_internal_removal_effects` under `Evicted` runs tracking invalidation +
  search-delete + WAL persist + (policy-gated) broadcast in `WRITE_EFFECT_ORDER`, and
  that `reason` selects the notification class (`expired`/`del`/`evicted`).
- **Eviction correctness (new).** After evicting a key that was indexed: a search over
  the index no longer returns it (closes the search-leak bug). With a tracking client
  registered on the key: an invalidation push is emitted (closes the stale-cache bug).
- **Eviction durability (new, the headline).** Evict a non-TTL key, restart via the
  `recover_shard_into` path (`recovery.rs`), assert the key is **absent** — the current
  code resurrects it. This is the regression guard for the correctness fix.
- **Expiry parity (port existing).** `event_loop.rs:482-545` (`effect_tests`) already
  pins notifications-for-both-paths, expired-key stat counting, and empty-result. Keep
  them green through the migration; add a WAL-persist assertion and a dirty-counter
  assertion (both currently skipped).
- **Waiter-drain subsumption.** Assert an XREADGROUP waiter on an expired/evicted
  stream key receives NOGROUP via the pipeline's `WaiterSatisfaction` (DEL's
  `WaiterWake::All`), and a blocked BLPOP on an expired list key stays blocked — i.e.
  the hand-rolled drain was correctly replaced, not lost.
- **Replication policy.** With `replicate = true`, assert the synthetic DEL/UNLINK
  reaches the broadcaster tagged with the origin shard; with `replicate = false`,
  assert nothing is broadcast (documents the chosen policy as a test, not a comment).
- **Version-bump coalescing (new).** Run an active-expiry cycle that removes *both* a
  whole-key-expired key and a field-emptied key; assert the WATCH version advances by the
  intended amount (once if coalesced, as recommended) so the semantics change vs. the
  current one-bump-per-cycle behavior is pinned rather than silent.

## Risks & alternatives

- **The replication-propagation policy is a real semantic decision, not a mechanical
  fix.** FrogDB Replicas currently expire independently (verified: `run_active_expiry`
  is role-agnostic). Turning on expiry propagation (`replicate = true`) toward
  Redis-style Primary-drives-expiry is a behavior change touching convergence
  semantics and should be its own ADR; this proposal defaults expiry to
  `replicate = false` (status quo) and only makes the choice *explicit and testable*.
  Eviction propagation is the stronger case — without it a Replica keeps keys the
  Primary evicted and samples different victims under its own pressure — but even there,
  enabling it is a deliberate step, so it ships behind the policy flag with a default
  chosen during review.
- **Sync→async eviction seam** (Migration step 4) is the one non-mechanical edit; if
  threading `async` proves invasive, the synchronous-WAL-enqueue variant keeps the
  change local.
- **Lazy expiry remains un-migrated after this proposal ships.** This is the honest
  scope limit: the highest-frequency removal path still emits zero canonical effects, so
  the stale-tracking + search-leak bug class is *not* fully closed until the step-7
  follow-up lands. This proposal deliberately does **not** claim "single source of truth
  achieved" — only "four homes reduced to two" — and builds the exact seam the lazy-expiry
  drain will reuse. Deferred for a structural reason (borrowed `&mut Store` mid-command),
  not overlooked.
- **Notification-class override adds a branch to the hot `KeyspaceNotifications`
  arm.** Kept to a single `match scope` already present for other effects; no new
  per-effect scaffolding.
- **Alternative — reuse the real `DelCommand` handler unchanged (like `scatter_del`
  does).** Rejected: `DelCommand`'s `EventSpec` emits `del`/GENERIC (`basic.rs:779`),
  but expiry needs `expired`/EXPIRED and eviction needs `evicted`/EVICTED. A pure
  synthetic-`DEL` reuse would emit the wrong keyspace event. The `RemovalReason`
  override is the minimal extension that keeps notification parity while inheriting
  every other effect.
- **Alternative — leave active expiry alone, fix only eviction.** Tempting (active
  expiry's skips are mostly benign per the recovery finding), but it re-splits the seam:
  active expiry would keep its bespoke tracking/search/notification/drain block while
  eviction goes through the pipeline, so the invariant would live in *three* places
  instead of two. Migrating both **batch** paths is what takes the count from four homes
  to two; the shared `run_internal_removal_effects` makes it one change. (The remaining
  two homes are the canonical pipeline and lazy expiry — the latter deferred for the
  structural reason in [Scope and the lazy-expiry gap](#scope-and-the-lazy-expiry-gap),
  not left un-migrated by oversight.)

## Effort

**M.** Confined to `frogdb-core` (`shard/*`). The pipeline extension (new scope +
reason/propagation data + three effect-arm branches) and the two call-site migrations
are compiler-guided and mirror the already-shipped `scatter_del` pattern. The two parts
that earn the M rather than S: the sync→async eviction seam (step 4), and the
propagation-policy decision (which is design, not code). No cross-crate signature
churn; no ADR is violated (data-path-only; crate direction preserved). The
eviction-durability fix (steps 1-2) is independently shippable ahead of the expiry
migration.

## Related

- **[Proposal 03 / Round 3 — `WRITE_EFFECT_ORDER` extraction]** and the scatter-path
  migration onto `EffectScope::ScatterPart` — this proposal extends that same seam with
  the internal-removal axis; `scatter_del` (`execution.rs:867-909`) is the reference
  pattern.
- **[Round 7] pipeline hardening** — the exhaustiveness tests
  (`post_execution.rs:454-585`, `MUST_PRECEDE`/`MUST_BE_ADJACENT`) that any new effect
  branch must satisfy; the new scope's assertions slot into that framework.
- **[Proposal 10](10-cluster-apply-owns-events.md)** — same underlying shape (a partial
  hand-rolled subset re-derived beside its canonical owner); different subsystem.
- **[Proposal 12](12-snapshot-coordinator-surface-trim.md)** — the Snapshot/Checkpoint
  recovery path this proposal leans on (`recover_shard_into`) for the resurrection
  analysis.

## Adversarial review

**Verdict: AMEND** — premise sound, eviction correctness bugs real; amended for scope
honesty around a fourth internal-removal path. All four issues resolved in place.

- **[major] Fourth internal-removal path (lazy expiry) unmigrated and under-acknowledged
  — RESOLVED.** Confirmed against code: `Store::check_and_delete_expired`
  (`store/hashmap.rs:421-435`) physically `uninstall`s an expired key on any read and
  emits **zero** canonical effects; the only `"expired"` keyspace emit in the core crate
  is active expiry (`event_loop.rs:166`), so lazy expiry fires far more often (every read
  of an expired key) carrying the identical stale-tracking + search-leak bug class. Fixes
  applied: (1) reframed the headline throughout from "closes the fragmentation / single
  source of truth achieved" to **"reduce four homes to two"** (Summary, Problem, Why-it's-
  shallow, both closing alternatives); (2) added a dedicated **Scope and the lazy-expiry
  gap** subsection and an Evidence subsection naming lazy expiry, its bug class, and the
  **structural reason it is deferred** (it removes from inside a borrowed `&mut Store`
  mid-command where the `ShardWorker`-level async effect pipeline is unreachable — needs a
  post-command drain); (3) added it to the Files table; (4) added migration **step 7** (a
  lazy-expiry drain reusing the same `RemovalReason::Expired` seam) and a Risks bullet
  flagging the residual gap. The proposal now delivers its title scope
  `(active-expiry, eviction)` in full and defers lazy expiry with rationale.
- **[minor] Version-bump semantics change for active expiry — RESOLVED.** Confirmed:
  active expiry does one unconditional `increment_version()` per non-empty cycle
  (`event_loop.rs:229`); two `run_internal_removal_effects` calls (Expired + FieldEmptied)
  would produce two bumps. Added an explicit call-out in migration step 3 (benign — extra
  WATCH bumps only cause conservative correct MULTI aborts — but a behavior change),
  recommended **coalescing** to a single `VersionIncrement`, and added a test-plan item
  pinning the semantics. Not shipped silently.
- **[minor] Sync→async seam is more than "thread async" — RESOLVED.** Confirmed the
  pre-flight `check_memory_for_write` loops up to 10 attempts (`eviction.rs:73-96`) before
  the triggering command runs. Expanded migration step 4: after migration each attempt
  runs a full WAL-awaiting synthetic-DEL pipeline (up to 10×) *ahead of* the command's own
  effects; noted this is Redis-consistent and carries **no re-entrancy risk** (synthetic
  DEL routes through `run_write_effects`, not `execute_command`, so it never re-enters the
  memory check).
- **[minor] `basic.rs` line drift — RESOLVED.** `EventSpec::Emits{del/GENERIC}` is at
  `basic.rs:779` (not 783); `WaiterWake::All` at 777 confirmed. Corrected in the Files
  table and the closing alternative.
- Also removed stray `</content>`/`</invoke>` XML artifacts that were trailing the
  document.
