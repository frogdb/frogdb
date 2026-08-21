# 41 — A spilled (warm) hash with due field TTLs wedges the shard-wide field-expiry sweep

Status: needs-triage

## What to build

`HashMapStore::spill_key` (`frogdb-server/crates/core/src/store/hashmap.rs:747-786`) flips a hot
entry to `ValueLocation::Warm` at `:781` and reconciles through `resize` at `:782`. It **never
touches `field_expiry_index`** — correctly, since the value still exists in the warm CF and its
field TTLs are still real. `unspill_key` likewise restores through `resize`, not `install`. So
a hash spilled while holding field TTLs keeps every one of its index entries, and those entries
are *not* stale.

But `purge_expired_hash_fields` cannot act on them. Its very first move is to take the entry's
location, and the warm arm bails out unconditionally:

```rust
// core/src/store/hashmap.rs:1413-1419
let value = match self.data.get_mut(key) {
    Some(entry) => match &mut entry.location {
        ValueLocation::Hot(arc) => Arc::make_mut(arc),
        ValueLocation::Warm => return 0,
    },
    None => return 0,
};
```

The consequence at `core/src/shard/active_expiry.rs:194-237` is a shard-wide stall. The sweep
pulls an oldest-first batch from the index (`:203`,
`store.get_expired_fields_limited(now, self.batch_size)`, `DEFAULT_BATCH_SIZE = 1024` at
`:34`), calls `purge_expired_hash_fields` on each key (`:224`), and only sets `purged_any` when
a purge returned a non-zero count (`:225-228`). Every due `(key, field)` pair belonging to a
spilled hash returns `0`, so if such pairs occupy the head of `by_time`, `purged_any` stays
`false` and `:234` breaks the loop for the whole cycle — and every subsequent cycle, because
the entries can never leave the index. Field TTLs stop being reaped for **every key on the
shard**, including hot ones that would purge fine.

This is **LIVE on main today** for any deployment running tiered storage with hash field TTLs,
and it is byte-for-byte the same failure shape as the ghost-driven starvation in proposal 93
§Problem 5 — but with **no ghost anywhere in the system**. That matters for scheduling: these
are correct index entries for a value the purge path structurally refuses to touch, so neither
proposal 93's fold nor either of its hotfixes repairs them. It is a pre-existing, independent
bug in the tiered-storage area. Secondary observation from the same section: the
`FieldExpiryIndex`'s own memory is counted nowhere — `hot_entry_memory_size`
(`hashmap.rs:300-305`) covers key + value + `KeyMetadata` + `Entry`, and
`HashValue::memory_size` counts the *value* book's expiries — so unbounded index growth is
invisible to `INFO memory`, `maxmemory` and eviction.

Fix direction is a design call in the tiered-storage area, and the two plausible shapes trade
off differently: (a) unspill the key before purging, which is correct but lets a field TTL
drive an unbounded stream of RocksDB reads; or (b) make the sweep skip-and-continue rather than
break on a non-purgeable key, which keeps the rest of the shard healthy but leaves the warm
key's fields unreaped until it is next read. A third option is to have `spill_key` drop the
key's index entries and have `unspill_key` re-derive them through `install`, at the cost of
warm keys never being actively reaped. Whichever is chosen, the sweep must stop treating "this
key could not be purged" as "the index is drained".

## Acceptance criteria

- [ ] One active-expiry tick over an index whose oldest entries belong to a **spilled** hash
      still reaps due fields belonging to other, hot keys on the same shard — the sweep no
      longer breaks shard-wide.
- [ ] A spilled hash's due field TTLs are eventually reaped (or the chosen policy for
      deferring them is documented at `hashmap.rs:1416` and asserted by a test).
- [ ] Regression test `warm_spilled_hash_does_not_starve_field_sweep` in the core store /
      active-expiry test suite: spill a hash holding due field TTLs, seed a second hot hash
      with a due field, run one expiry tick, assert the hot key's field was reaped. **Fails at
      HEAD** (`purged_any` false → `active_expiry.rs:234` breaks before reaching it).
- [ ] Regression test asserting `purge_expired_hash_fields` on a warm key is distinguishable
      from "nothing was due" at the sweep's seam (a returned count of 0 for a non-purgeable
      key no longer terminates the cycle).
- [ ] `just test frogdb-core field_expiry` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 93
(`.scratch/arch-deepening/proposals/93-hash-field-expiry-store-api.md`), §5a "The same
starvation exists with zero ghosts, via the warm tier — a separate, pre-existing bug"
(explicitly flagged for the orchestrator to file; not fixed by 93 or either of its hotfixes).

## Comments
