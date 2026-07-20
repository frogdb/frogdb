# Proposal 04 — One entry-lifecycle seam inside HashMapStore (install / uninstall / resize)

## Summary

`HashMapStore` keeps six derived side-structures (`memory_used`, the `keysizes`
histograms, `expiry_index`, `field_expiry_index`, `ts_labels`,
`warm_tier.warm_keys`) plus a deferred `pending_keysizes_refreshes` queue in sync
with the primary `data` map. The invariant *"an entry's derived structures move
atomically with the entry"* is not owned by any one module boundary — it is
re-stated, in slightly different combinations, across roughly nine mutation
methods. Each site re-derives which subset of the six-plus-one it must poke, and
the correctness of the whole store depends on every one of those hand-written
combinations being complete. The proposal is to concentrate that invariant behind
a private three-operation lifecycle seam — `install`, `uninstall`, `resize` — so
the mutation methods stop touching the indexes directly and the reconciliation
lives in exactly one place.

## Files involved (verified path + line count)

- `frogdb-server/crates/core/src/store/hashmap.rs` — **1978 lines** (verified with
  `wc -l`; the task brief's "~1977L" matches). All line references below are from
  this file as read on 2026-07-20.

Supporting types referenced by the seam (not modified by this proposal, only
called through it):

- `crate::histogram::KeysizeHistograms` / `KeysizeType` — per-type keysize and
  key-memory histograms (`keysizes` field, `hashmap.rs:126`).
- `crate::noop::{ExpiryIndex, FieldExpiryIndex}` — key-level and field-level
  expiry indexes (`hashmap.rs:103-104`).
- `super::timeseries_labels::TimeSeriesLabels` — `ts_labels` (`hashmap.rs:105`).
- `super::warm_tier::WarmTier` — owns `warm_keys` and the spill/unspill counters
  (`hashmap.rs:113`).

## Problem

### The six side-structures + the refresh queue

Declared on the struct at `hashmap.rs:101-137`:

| # | Structure | Field | Kept-in-sync meaning |
|---|-----------|-------|----------------------|
| 1 | `memory_used` | `memory_used: usize` (107) | running byte count; must equal `recompute_memory_used()` |
| 2 | keysize histograms | `keysizes: KeysizeHistograms` (126) | per-type logical-size bins + key-memory bins |
| 3 | key expiry index | `expiry_index: ExpiryIndex` (103) | one entry per volatile key |
| 4 | field expiry index | `field_expiry_index: FieldExpiryIndex` (104) | per-field TTLs of hash values |
| 5 | ts labels | `ts_labels: TimeSeriesLabels` (105) | label→key reverse index for time series |
| 6 | warm-key count | `warm_tier.warm_keys` (113) | count of spilled (Warm) entries |
| Q | refresh queue | `pending_keysizes_refreshes: Vec<…>` (131) | deferred `get_mut` size snapshots |

### The ~9 mutation sites that each re-state the reconciliation

Each row is a method that mutates `data`; each column marks whether that method
hand-reconciles the corresponding structure. The near-duplication — and the
subtle *gaps* — are the point.

| Mutation method (line) | 1 mem | 2 histo | 3 expiry | 4 field-exp | 5 ts_labels | 6 warm_keys | Q refresh queue |
|---|---|---|---|---|---|---|---|
| `replace_entry` (257) | ✓ sub+add | ✓ dec+inc | ✓ set/remove | ✓ remove+re-derive | ✓ reconcile | ✓ remove_warm | ✓ discard |
| `delete` (705) | ✓ refund | ✓ dec | ✓ remove | ✓ remove_key | ✓ remove | ✓ remove_warm | ✓ discard |
| `get_and_delete` (1034) | ✓ refund | ✓ dec | ✓ remove | ✓ remove_key | ✓ remove | ✓ decrement_warm_keys | ✓ discard |
| `check_and_delete_expired` (331) | ✓ refund | ✓ dec | ✓ remove | ✓ remove_key | ✓ remove | ✓ remove_warm | ✓ discard |
| `spill_key` (556) | ✓ sub value | — (gap) | — | — | — | ✓ record_spill | ✓ flush first |
| `unspill_key` (598) | ✓ add/sub | — (gap) | ✓ remove* | ✓ remove_key* | ✓ remove* | ✓ record_unspill / expired | ✓ discard* |
| `purge_expired_hash_fields` (1166) | ✓ via resize | ✓ via resize | — (unless empty→delete) | ✓ remove per field | — (unless delete) | — (unless delete) | ✓ skip-if-pending |
| `apply_size_refresh` (458) | ✓ delta | ✓ migrate | — | — | — | — | consumed from Q |
| `clear` (885) | ✓ =0 | ✓ clear | ✓ new | ✓ new | ✓ clear | ✓ reset_keys | ✓ clear |

`*` = only on `unspill_key`'s expired-key cleanup branch (606-620); the normal
rehydrate branch (645-661) touches only `memory_used` and `warm_keys`.

Observations that a single seam would erase:

- **The delete family (`delete`, `get_and_delete`, `check_and_delete_expired`,
  and `unspill_key`'s expired branch) is four copies of the same eight-line
  ritual** — snapshot histogram, refund accounted memory, `remove_warm`,
  `expiry_index.remove`, `ts_labels.remove`, `field_expiry_index.remove_key`,
  `discard_pending_refresh`. They differ only in how the warm side is settled
  (`remove_warm` vs `decrement_warm_keys`+`take` vs `record_expired_on_unspill`)
  and in the `debug_assert!` that only `delete` bothers to carry.
- **`spill_key` and `unspill_key` silently skip the keysize histograms** (column 2
  gap) even though they move value bytes in and out of RAM. That is arguably a
  latent key-memory-histogram drift; whether intentional or not, it is invisible
  because there is no one place that says "resizing an entry updates memory *and*
  histograms together."

### Evidence the incremental accounting is not trusted

**1. The `replace_entry` doc-comment records a past silent-data-loss bug** — the
scar tissue that motivated routing all inserts through one seam
(`hashmap.rs:236-256`, quoted verbatim):

```rust
/// Insert or overwrite an entry, reconciling **every** side structure.
///
/// This is the single insert/overwrite seam: `set`, `set_with_options`,
/// and `restore_entry` all route through it, so no write path can forget
/// one of the derived structures (`expiry_index`, `field_expiry_index`,
/// `label_index`, keysize histograms, `memory_used`). Before this seam
/// existed, `set` skipped the expiry indexes — an overwritten key kept its
/// stale index entry and active expiry would later delete the (now
/// persistent) key: silent data loss.
```

That paragraph is a design admission: the *insert* side was already consolidated
into one seam **because** a hand-reconciled write path forgot a structure and
corrupted data. The *delete/resize* side has never been given the same treatment —
it is still the nine-site hand-reconciliation above.

**2. Two DEBUG-only cross-checks exist purely to catch the counters drifting**,
wired to `DEBUG MEMORY-CHECK` / `DEBUG EXPIRY-INDEX-CHECK`:

- `recompute_memory_used` (`hashmap.rs:771-776`) re-sums `Entry::memory_size` over
  every entry — "the ground truth the running counter tracks." Its existence
  concedes that `memory_used` is a hand-maintained approximation that must be
  auditable against ground truth.
- `audit_expiry_index` (`hashmap.rs:778-816`) walks the index in **both
  directions** (index→data and data→index) looking for `KeyMissing`,
  `KeyPersistent`, `DeadlineMismatch`, and `IndexMissing` anomalies — i.e. exactly
  the classes of bug a forgotten `expiry_index.remove`/`.set` would produce.

**3. `delete` carries a `debug_assert!` plus a runtime `warn!` on underflow**
(`hashmap.rs:715-727`): it does not trust that the accounted size it is refunding
is `<= memory_used`, and logs in production if the invariant it is manually
maintaining has already broken. A seam that owns the accounting makes this a
class-level invariant rather than a per-call guard.

## Why it is shallow / fragmented (architecture vocabulary)

- **Information leakage.** The single invariant — "the six derived structures move
  atomically with an entry" — is knowledge that leaks into nine call sites. Each
  mutation method must *know* the full set of side-structures and the correct verb
  for each (`remove` vs `remove_key` vs `remove_warm` vs `reset_keys`). There is
  no interface hiding that knowledge; every method re-implements it. This is the
  textbook shallow-module smell: the "interface" (nine public/private mutators) is
  as wide as the implementation, giving no leverage.
- **Poor locality → the deletion test fails.** Adding a *seventh* side-structure
  (say a secondary type index, or a per-key CRC) means editing all nine sites and
  hoping none is missed — precisely the failure mode the `replace_entry`
  doc-comment memorializes for the *insert* path. Conversely, deleting a
  side-structure requires touching nine methods to prove it is fully gone. A deep
  seam would make both a one-line change.
- **The DEBUG audits are a load-bearing safety net, not a luxury.**
  `recompute_memory_used` and `audit_expiry_index` exist *because* the incremental
  path is fragmented and therefore untrusted. In a design where reconciliation is
  concentrated behind one seam, those audits demote from "the thing that catches
  the bug in production" to "belt-and-suspenders assertions in tests." Their
  current prominence (wired to live `DEBUG` subcommands) is a measure of how
  shallow the mutation layer is.
- **No adapter seam for the warm tier's three settle-verbs.** `remove_warm`,
  `decrement_warm_keys`+`take`, and `record_expired_on_unspill` are three
  different ways to retire a warm entry, chosen ad hoc at each delete site. The
  choice is real (some paths need the value back, some do not), but it is
  currently made by the caller instead of encapsulated behind an `uninstall` that
  returns the removed `Entry`.

## Proposed change

Introduce a **private entry-lifecycle seam** on `HashMapStore` — three methods
that are the *only* code permitted to touch the six side-structures + refresh
queue. Every mutation method delegates to them.

```rust
/// The single point that reconciles ALL derived side-structures with an
/// insert. Owns memory_used, histograms, both expiry indexes, ts_labels,
/// the warm-key count, and the pending-refresh queue. `replace_entry` is
/// its only caller.
fn install(&mut self, key: Bytes, value: Value, metadata: KeyMetadata)
    -> Option<Arc<Value>>;   // returns prior hot value, as replace_entry does today

/// The single point that reconciles ALL derived side-structures with a
/// removal. Snapshots + decrements histograms, refunds the accounted
/// memory, retires both expiry indexes + ts_labels, settles the warm tier
/// via the entry's own location, and discards any pending refresh.
/// Returns the removed Entry so callers that need the value (get_and_delete)
/// or the warm bytes can finish their own work.
fn uninstall(&mut self, key: &[u8]) -> Option<Entry>;

/// The single point that reconciles memory + histograms for an in-place
/// size change of an existing entry, given the previously-accounted size.
/// Owns the migrate/delta logic currently in apply_size_refresh, and is
/// what spill/unspill's byte-accounting becomes.
fn resize(&mut self, key: &[u8], old_accounted: usize);
```

Mapping of the current nine sites onto the seam:

| Current site | Becomes |
|---|---|
| `replace_entry` | body moves *into* `install`; `replace_entry` becomes a thin wrapper (or is renamed) |
| `delete` | `let e = self.uninstall(key)?;` then return `true`, keeping only its `debug_assert!` if desired |
| `get_and_delete` | `let e = self.uninstall(key)?;` then extract value / warm bytes from the returned `Entry` |
| `check_and_delete_expired` | `self.uninstall(key)` + `self.expired_keys += 1` |
| `unspill_key` (expired branch) | `self.uninstall(key)` (its `record_expired_on_unspill` counter handled inside, keyed off `Entry.location`) |
| `spill_key` | `self.resize(key, old_accounted)` after flipping `location` to `Warm` |
| `unspill_key` (rehydrate branch) | `self.resize(key, old_accounted)` after flipping `location` to `Hot` |
| `apply_size_refresh` | *is* `resize` — folded in, called by `flush_keysizes_refreshes` |
| `purge_expired_hash_fields` | `self.resize(...)` for the shrink, `self.uninstall(...)` when the hash empties |
| `clear` | stays whole (it is a bulk reset, not a per-entry op) — the one legitimate non-seam mutator |

The seam **also closes the column-2 gap**: because `resize` owns both the memory
delta *and* the histogram migrate, `spill_key`/`unspill_key` reconciling their
byte movement automatically keeps the key-memory histogram correct, which they do
not do today.

`install`/`uninstall`/`resize` are private (`fn`, not part of the `Store` trait),
so this is an internal refactor of `HashMapStore` with no trait-surface change.

## Before / After (real current code)

### Before — `delete` (`hashmap.rs:705-740`, verbatim)

```rust
    fn delete(&mut self, key: &[u8]) -> bool {
        if let Some(entry) = self.data.remove(key) {
            self.discard_pending_refresh(key);
            // Decrement keysize histograms
            let snap = Self::histogram_snapshot(&entry);
            self.histogram_decrement_snapshot(snap);
            // Refund the accounted size — exactly what `memory_used` was
            // charged at insert/refresh time — never the recomputed live
            // size, which can exceed it after unflushed in-place growth.
            let size = entry.metadata.memory_size;
            debug_assert!(
                size <= self.memory_used,
                "memory accounting underflow during delete: accounted {} > memory_used {}",
                size,
                self.memory_used
            );
            if size > self.memory_used {
                warn!(
                    key_len = key.len(),
                    reported_size = size,
                    "Memory accounting underflow during delete"
                );
            }
            self.memory_used = self.memory_used.saturating_sub(size);
            // Clean up warm CF if this was a warm key
            if !entry.is_hot() {
                self.warm_tier.remove_warm(key);
            }
            self.expiry_index.remove(key);
            self.ts_labels.remove(key);
            self.field_expiry_index.remove_key(key);
            true
        } else {
            false
        }
    }
```

### Before — `spill_key` (`hashmap.rs:556-593`, verbatim)

```rust
    pub fn spill_key(&mut self, key: &[u8]) -> Result<usize, SpillError> {
        // Settle any deferred size reconciliation first so the accounted size
        // reflects the live value about to be spilled (spill runs from the
        // event loop, so this is normally a no-op).
        self.flush_keysizes_refreshes();

        if !self.warm_tier.is_configured() {
            return Err(SpillError::NoWarmStore);
        }

        let entry = self.data.get(key).ok_or(SpillError::KeyNotFound)?;
        let value = entry.hot_value().ok_or(SpillError::AlreadyWarm)?;

        // Serialize using existing persistence format
        let serialized = serialize(value, &entry.metadata);
        let value_bytes = value.memory_size();

        // Write to warm CF. `try_put` returns `None` only when the tier is
        // unconfigured, already handled above.
        match self.warm_tier.try_put(key, &serialized) {
            Some(Ok(())) => {}
            Some(Err(e)) => return Err(SpillError::Rocks(e)),
            None => return Err(SpillError::NoWarmStore),
        }

        // Replace location with Warm
        let entry = self.data.get_mut(key).unwrap();
        entry.location = ValueLocation::Warm;

        // Update memory accounting — value bytes freed, metadata stays in RAM.
        // The accounted size moves in lockstep so a later delete refunds
        // exactly what remains charged.
        entry.metadata.memory_size = entry.metadata.memory_size.saturating_sub(value_bytes);
        self.memory_used = self.memory_used.saturating_sub(value_bytes);
        self.warm_tier.record_spill();

        Ok(value_bytes)
    }
```

Note `spill_key` here hand-adjusts `entry.metadata.memory_size` and
`self.memory_used` but never migrates the key-memory histogram bin — the column-2
gap.

### After (sketch) — the reconciliation lives once, inside the seam

```rust
    // --- the one place that owns removal reconciliation ---
    fn uninstall(&mut self, key: &[u8]) -> Option<Entry> {
        let entry = self.data.remove(key)?;
        self.discard_pending_refresh(key);

        let snap = Self::histogram_snapshot(&entry);
        self.histogram_decrement_snapshot(snap);

        let size = entry.metadata.memory_size;
        debug_assert!(size <= self.memory_used, "accounting underflow: {size} > {}", self.memory_used);
        self.memory_used = self.memory_used.saturating_sub(size);

        // Warm side settled from the entry's own location — no caller choice.
        if !entry.is_hot() {
            self.warm_tier.remove_warm(key);
        }
        self.expiry_index.remove(key);
        self.ts_labels.remove(key);
        self.field_expiry_index.remove_key(key);
        Some(entry)
    }

    // --- the one place that owns in-place resize reconciliation ---
    fn resize(&mut self, key: &[u8], old_accounted: usize) {
        // migrate histogram bins + apply (new_live − old_accounted) to
        // memory_used + rewrite entry.metadata.memory_size.
        // (this is today's apply_size_refresh, generalized so spill/unspill
        //  byte movement flows through it too, closing the histogram gap.)
        // ...
    }

    // --- now the mutation methods are thin ---
    fn delete(&mut self, key: &[u8]) -> bool {
        self.uninstall(key).is_some()
    }

    fn get_and_delete(&mut self, key: &[u8]) -> Option<Value> {
        if self.check_and_delete_expired(key) { return None; }
        let entry = self.uninstall(key)?;           // all six structures settled here
        match entry.location {
            ValueLocation::Hot(arc) => Some(Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone())),
            ValueLocation::Warm => self.warm_tier.take(key)
                .and_then(|d| deserialize(&d).ok()).map(|(v, _)| v),
        }
    }

    pub fn spill_key(&mut self, key: &[u8]) -> Result<usize, SpillError> {
        self.flush_keysizes_refreshes();
        // ... write to warm CF, flip location to Warm ...
        let old_accounted = /* size before flip */;
        self.resize(key, old_accounted);            // memory + histogram in one place
        self.warm_tier.record_spill();
        Ok(value_bytes)
    }
```

The eight-line delete ritual now exists exactly once; `delete`,
`get_and_delete`, `check_and_delete_expired`, and `unspill_key`'s expired branch
all become one `uninstall` call plus their own residual (a counter bump, a value
extraction).

## Testability improvement

Concentrating the invariant makes it directly unit-testable in isolation, instead
of being provable only by exercising every command path:

- **Round-trip to zero.** `install(k, v, meta)` immediately followed by
  `uninstall(k)` must return `memory_used` to its pre-install value and leave all
  six structures empty for `k`. Today this can only be asserted end-to-end via
  `set` + `delete`; with a seam it is a two-line property test over `install` /
  `uninstall` for every `Value` variant.
- **Resize symmetry.** `resize` after a grow, then `resize` back after an
  equivalent shrink, must be `memory_used`-neutral and histogram-neutral. The
  existing `recompute_reveals_unflushed_inplace_growth` test (`hashmap.rs:1370`)
  becomes a direct test of `resize` rather than of the whole `get_mut` →
  `flush_keysizes_refreshes` pipeline.
- **The DEBUG audits demote from safety-net to belt-and-suspenders.**
  `recompute_memory_used` and `audit_expiry_index` currently backstop nine
  hand-written reconciliations in production (`DEBUG MEMORY-CHECK` /
  `DEBUG EXPIRY-INDEX-CHECK`). Once reconciliation is proven correct for the three
  seam operations in isolation, those audits are covering a single implementation,
  not nine — so they can stay as cheap invariant checks in tests without being the
  primary line of defense against a forgotten `.remove()`.

## Risks / open questions

- **Does any site legitimately need a *partial* reconciliation the seam would
  over-do?** `set_expiry` (989), `persist` (1009), and `set_field_expiry` (1138)
  touch only the expiry indexes without removing the entry — these are *not*
  install/uninstall/resize events and should stay as-is; the seam is for
  entry-lifecycle transitions, not metadata edits. Confirm the boundary is
  "entry appears / disappears / changes size," and that field-expiry edits are
  deliberately outside it.
- **`uninstall` returning `Entry` vs. the three warm-settle verbs.** Today
  `delete` uses `remove_warm`, `get_and_delete` uses `decrement_warm_keys`+`take`,
  and `unspill_key`'s expired branch uses `record_expired_on_unspill` (which also
  bumps the expired-on-unspill counter). If `uninstall` always calls `remove_warm`,
  the two paths that need the warm *bytes* (`get_and_delete`) or a *different
  counter* (`unspill` expired) must be handled by the caller after the entry is
  returned — verify no double-decrement of `warm_keys` results. This is the
  trickiest part of the mapping and deserves its own test.
- **`expired_keys` counter placement.** `check_and_delete_expired` increments
  `expired_keys`; `uninstall` must NOT (it is also called by non-expiry deletes).
  Keep the counter bump in the caller.
- **Closing the histogram gap changes observable INFO output.** Making
  `spill_key`/`unspill_key` migrate the key-memory histogram (which they skip
  today) will shift `keysizes` values under tiered storage. That is arguably a
  *fix*, but it is a behavior change to verify against expected INFO/DEBUG output,
  not a pure refactor.
- **`clear` intentionally stays outside the seam** — it is a bulk reset, and
  routing it through per-entry `uninstall` would be O(n) for no benefit. Confirm
  that is acceptable (it already re-`new()`s the indexes directly).

## Effort estimate

**M.** The three seam methods are almost entirely *extraction* of code that
already exists and is already correct (`replace_entry`'s body → `install`, the
delete ritual → `uninstall`, `apply_size_refresh` → `resize`), so the logic risk
is low. The effort concentrates in three places: (1) making `uninstall` return the
`Entry` and rewiring the four delete-family callers to extract what they need
without double-settling the warm tier; (2) generalizing `resize` so spill/unspill
byte-accounting flows through it, which also closes the histogram gap and thus
needs new assertions on INFO/keysize output; (3) confirming the metadata-only
paths (`set_expiry`/`persist`/field-expiry) stay outside the seam. It is more than
S because of the warm-tier three-verb subtlety and the observable histogram
change; it is not L because the trait surface is untouched and the extracted logic
is proven by the existing `recompute`/`audit` tests.
