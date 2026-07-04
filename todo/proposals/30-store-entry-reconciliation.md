# Proposal: Store Entry-Reconciliation Seam

Status: implemented
Date: 2026-07-04

## Problem

`HashMapStore` keys carry five derived side structures: the key-level `expiry_index`, the
`field_expiry_index`, the TimeSeries `label_index`, the keysize histograms, and the running
`memory_used` counter. Before this change there was no single place where an insert/overwrite
reconciled them — every write method hand-maintained its own subset, and the subsets disagreed:

| Write path (`store/hashmap.rs`, pre-change) | expiry_index | field_expiry_index | label_index | histograms | memory_used |
|---|---|---|---|---|---|
| `set` | **never touched** | **never touched** | yes | yes | yes |
| `set_with_options` | yes | **never touched** | **never touched** | yes | yes |
| `restore_entry` | add-only (no overwrite retire) | add-only | add-only | add-only | add-only |
| `delete` / `get_and_delete` | yes | `delete` only | yes | yes | yes (live size) |
| in-place mutation via `get_mut` | n/a | n/a | n/a | deferred flush | **never touched** |

Two of those gaps were live correctness bugs, one of them data loss:

**Bug 1 — `set` overwrite leaves the expiry index stale (data loss).**
`SET k v EX 100` puts `k` in the expiry index. `MSET k v2` routes through `Store::set`
(`shard/execution.rs:715`, `scatter_mset`), which correctly created fresh metadata with
`expires_at = None` (MSET clears the TTL, matching Redis) — but never removed the key from
`expiry_index`. The active-expiry cycle trusts the index unconditionally: `run_cycle` did
`if store.delete(&key)` (`shard/active_expiry.rs:142`) with no re-check of the entry's own
deadline. At the old deadline, active expiry deleted a key the user had made persistent.
Collateral of the same stale entry: `keys_with_expiry_count()` (INFO `expires`) over-reported,
and `sample_volatile_keys()` could hand a no-TTL key to volatile-* eviction. The field-TTL
variant: overwriting a hash left permanent ghosts in `field_expiry_index`.

**Bug 2 — in-place mutation is invisible to memory accounting.**
`get_mut` (the RPUSH/SADD/HSET/ZADD/APPEND path) returned `Arc::make_mut` and pushed a
deferred-refresh snapshot, but `flush_keysizes_refreshes` migrated *histogram bins only* — it
never applied the memory delta. A list grown to megabytes still counted as its creation size, so
`is_over_memory_limit` (`shard/eviction.rs:14-18`) fired late or never, and INFO `used_memory` /
the `frogdb_memory_used_bytes` gauge drifted low. Worse, the increment/decrement pair was
asymmetric: inserts charged the creation-time size, but `delete` refunded the recomputed *live*
size (`entry.memory_size(key)`), so a grown-then-deleted key refunded more than it charged. The
`warn!("Memory accounting underflow during delete")` in the old `delete` was this asymmetry
observing itself in production logs. Three notions of "the entry's size" coexisted: the
creation-time `metadata.memory_size` (write-only except for eviction's `memory_freed` metric),
the running `memory_used`, and the live `value.memory_size()`.

**Root cause shape.** ~6 write methods each hand-reconciled 5 side structures. That is a
locality failure, not a typo: nothing *forced* a new or edited write path to remember all five,
so `set` forgot two, `set_with_options` forgot two others, and the deferred-refresh path forgot
the most load-bearing one. The fix is not to patch the missing lines into each method — it is to
make forgetting impossible.

## Proposed design

Two seams, both inside `store/hashmap.rs`, plus one guard past the store boundary.

### 1. `replace_entry` — the single insert/overwrite path

```rust
fn replace_entry(&mut self, key: Bytes, value: Value, metadata: KeyMetadata)
    -> Option<Arc<Value>>
```

Private to `HashMapStore`. `set`, `set_with_options`, and `restore_entry` are now thin wrappers
around it; it is the only place `self.data.insert` happens for hot entries. On every call it:

- retires the old incarnation, if any: refunds the *accounted* size from `memory_used`,
  decrements histogram bins, cleans the warm CF, drops the old incarnation's
  `field_expiry_index` entries, and discards any pending deferred-refresh snapshot;
- reconciles `label_index` from the new value (TimeSeries adds, everything else removes);
- re-derives `field_expiry_index` from the new value itself — a `Value::Hash` carrying field
  TTLs (COPY/RESTORE of a hash) gets them indexed, which the old `set` silently dropped;
- reconciles the key-level `expiry_index` from `metadata.expires_at`: `Some` → indexed,
  `None` → removed. The metadata and the index cannot disagree, closing Bug 1;
- charges `memory_used`, increments histograms, and stamps `metadata.memory_size` with the
  computed entry size, then inserts.

The caller's only degrees of freedom are the metadata fields that legitimately vary (recovery
passes its restored `last_access`/`lfu_counter`/`expires_at`; SET passes the computed expiry).
Everything derived is owned by the seam. A new write method routed through `replace_entry`
*cannot* forget an index because it never sees one.

`restore_entry` routing through the seam also fixed a latent recovery leak: replaying the same
key twice (snapshot + WAL) used to double-charge `memory_used` and histograms because the old
code never retired the first incarnation.

### 2. The accounted-size invariant — `metadata.memory_size`

Bug 2's three notions of size collapse to one rule:

> `memory_used` is exactly the sum of `metadata.memory_size` over all entries, and every charge
> or refund moves both in lockstep.

- `replace_entry` stamps `metadata.memory_size` at insert (charge).
- `flush_keysizes_refreshes` now routes each pending snapshot through `apply_size_refresh`,
  which migrates histogram bins **and** applies `new_live − old_accounted` to `memory_used`,
  then re-stamps `metadata.memory_size = new_live`. In-place growth is visible to eviction/OOM
  the moment the post-execution flush runs (`shard/execution.rs:162`,
  `shard/post_execution.rs:196`). This also makes eviction's `memory_freed` metric
  (`shard/eviction.rs`, `get_metadata(key).memory_size`) reflect reality instead of the
  creation-time size.
- Every removal path (`delete`, `get_and_delete`, `check_and_delete_expired`, the
  expired-on-promote cleanup, `replace_entry`'s retire step) refunds `metadata.memory_size`,
  never the recomputed live size. Increment and refund are now the same number by construction,
  so the delete-underflow class is closed structurally; `delete` carries a `debug_assert` (plus
  the release-mode `warn!`) so any future violation of the invariant fails loudly in tests.
- `demote_key`/`promote_key`/`restore_warm_entry` move `metadata.memory_size` in lockstep with
  the value bytes they add/remove from RAM, so warm entries obey the same invariant.
- `histogram_snapshot` (the decrement side) reads the accounted size too, so the key-memory
  histogram decrements the same bin the increment (or last refresh) populated.

Two hazards around the deferred snapshot needed explicit handling:

- **Replay against a new incarnation.** If a key is overwritten or deleted after `get_mut` but
  before the flush, the stale snapshot must not be applied to the key's next incarnation.
  `discard_pending_refresh(key)` runs in every path that settles the key's accounting itself
  (the retire step of `replace_entry` and the whole delete family). The flush's key-missing arm
  stays as a defensive no-op.
- **Double snapshot in one command.** Two `get_mut` calls before one flush used to push two
  snapshots; replaying both double-applied the delta between them (this was a live histogram
  bug too). The push now dedups: the first snapshot — the last-flushed state — wins.

Also folded in: the pending snapshot is no longer gated on `keysize_type()`. Types outside the
keysize histograms (JSON, TimeSeries, bloom, …) previously got *no* snapshot at all, so their
in-place growth would have stayed invisible even with the flush fixed. The snapshot now carries
`Option<KeysizeType>`; histogram migration is conditional, the memory delta is not. And
`purge_expired_hash_fields`, which shrinks a hash in place without going through `get_mut`,
reconciles inline via the same `apply_size_refresh` (skipped when a deferred snapshot is
already pending — the end-of-command flush then observes the post-purge state once).

### 3. Belt and suspenders: the active-expiry guard

The store-side seam guarantees the index is never stale *going forward*. Defense in depth for
the class: `ActiveExpiryCoordinator::run_cycle` (`shard/active_expiry.rs`) no longer trusts the
index alone — before deleting it re-checks the entry's own deadline via
`store.get_expiry(&key)`, and skips entries whose metadata says "no TTL" or "not yet due". A
stale index entry can therefore never delete a live persistent key, whatever future bug
produces it. A skipped entry does not count as progress, so the existing `!progressed` batch
guard prevents the cycle from spinning its budget on dead entries.

### What crosses the seam vs. what does not

`replace_entry` is deliberately *private*: the seam is an internal discipline of
`HashMapStore`, not new `Store`-trait surface. The trait's contract is unchanged (`set` clears
the TTL, `set_with_options` honors `SetOptions`, `restore_entry` preserves recovered metadata);
what changed is that the contract is now enforced in one place. The coordinator guard names
only `Store::get_expiry`, which already existed — no trait widening anywhere.

### Why this is the right depth

- **Locality.** "What must be reconciled when a key is written" now has exactly one home. The
  bug matrix at the top of this document is impossible to recreate: there is no second insert
  path to diverge. Same for size: "what an entry costs" is written once (`replace_entry`) and
  refreshed once (`apply_size_refresh`); every other site only copies `metadata.memory_size`.
- **Leverage.** One private method closes five index-divergence bugs (key-expiry ghost,
  field-TTL ghost, `set_with_options` label/field gaps, recovery double-charge) and the whole
  memory-drift family (invisible growth, delete underflow, stale eviction `memory_freed`), and
  every future write path inherits the reconciliation for free.
- **Deletion test.** Every behaviour here is exercisable against a bare `HashMapStore` — no
  shard, no channels, no runtime. The regression tests below construct a store, write, and
  assert on `keys_with_expiry_count` / `get_expired_keys` / `memory_used`; the coordinator
  guard tests reuse the existing `active_expiry.rs` harness.

## Migration plan (as landed)

1. **Phase 1 — `replace_entry` seam.** Add the seam; route `set`, `set_with_options`,
   `restore_entry` through it. Fixes Bug 1 and the field-TTL/label/recovery gaps. Store-level
   regression tests for each closed gap.
2. **Phase 2 — accounted-size invariant.** `apply_size_refresh` +
   `discard_pending_refresh`; accounted refunds in the delete family; lockstep updates in
   demote/promote/warm-restore; snapshot widening + dedup; inline reconcile in
   `purge_expired_hash_fields`; underflow `debug_assert`. Memory-accounting tests.
3. **Phase 3 — coordinator guard.** Deadline re-check in `run_cycle` + orphaned-index tests.
4. **Phase 4 — this document.**

## Testing impact

Store-level (all new, `store/hashmap.rs` tests):

- `set_overwrite_clears_stale_key_expiry_index` — the Bug 1 repro at store level: SET+EX then
  plain set → `keys_with_expiry_count() == 0`, `get_expired_keys(far_future)` empty, key alive.
- `set_overwrite_clears_stale_field_expiry_index`, `set_with_options_overwrite_clears_stale_field_expiry_index`
  — field-TTL ghosts.
- `set_indexes_field_expiries_carried_by_the_value` — COPY/RESTORE of a hash with field TTLs.
- `restore_entry_overwrite_retires_previous_incarnation` — recovery replay does not leak.
- `flush_applies_memory_delta_after_in_place_growth` — grow via `get_mut`, flush, and
  `memory_used`/`metadata.memory_size` equal a fresh store holding the same value.
- `grow_flush_delete_returns_to_zero`, `grow_then_delete_without_flush_does_not_underflow` —
  the underflow class, with and without an intervening flush.
- `shrink_then_flush_reduces_memory_used`, `repeated_get_mut_in_one_command_does_not_double_count`,
  `overwrite_after_unflushed_growth_settles_cleanly`, `purge_expired_hash_fields_reconciles_memory`.

Coordinator-level (`shard/active_expiry.rs` tests):

- `orphaned_index_entry_does_not_delete_live_key` — an injected stale index entry never deletes
  a live persistent key.
- `orphaned_index_entry_does_not_block_genuine_expirations` — the guard skips orphans without
  starving genuinely due keys or spinning the budget.

## Risks / open questions

- **Skipped orphans are not healed.** The coordinator guard skips a stale index entry but has
  no trait-level way to remove it (only `delete` and the deprecated `expiry_index_mut` touch
  the index from outside). With the store seam in place orphans should not arise at all, and a
  skipped orphan costs one `get_expiry` per cycle; if a healing path is ever wanted, it belongs
  in the store (e.g. `get_expired_keys_limited` cross-checking metadata), not the coordinator.
- **`metadata.memory_size` semantics changed.** It now means "accounted entry size" (key +
  value + metadata + entry overhead; value bytes excluded while warm) and is kept current by
  the refresh, where it previously froze the creation-time size. Its only external reader —
  eviction's `memory_freed` metric — wanted exactly this. Persistence is unaffected: the
  serializer recomputes `memory_size` from the value at write time.
- **Warm-tier histogram bins were already leaky** (demotion does not migrate the key-memory
  bin, and a warm delete's snapshot is `None`), and logical-size bins can still drift if a
  value is mutated outside `get_mut`. Pre-existing, unchanged by this work, and bounded to
  INFO keysizes reporting — flagged here rather than silently inherited.
- **`get_and_delete` and expired-on-promote now clear `field_expiry_index`.** These removal
  paths previously left field-TTL ghosts like the old `set` did; they were aligned with
  `delete` as part of the same reconciliation pass.
