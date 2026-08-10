# Proposal 83 — Lazy expiry: `ExpiryReport` and one removal authority

Status: draft (arch-deepening round 38, lane protocol/net/core, candidate PN5)
Verified at HEAD `49a21b18f9c583c3a58405b32a37fdc2d462846e` (2026-08-10), local mode.
**LOCKED-ADJACENT.** Touches the persistence contract (`frogdb-persistence` +
`frogdb-recovery`, mutation gate 0.85; boundary ADR `adr/0003`). Every behaviour change
below is **spec-first**: new `FM-PERSISTENCE` row → failing test → fix. Nothing here is a
one-liner hotfix. See [Spec / LOCKED impact](#spec--locked-impact).

## Corrections to the lane brief

The brief is stale; each of its PN5 claims was re-derived at HEAD.

| Brief claim | Verdict at HEAD | Evidence |
| --- | --- | --- |
| "lazy expiry hand-rolls the effect set instead of reusing the pipeline" | **Confirmed** | `worker.rs:738-851` mirrors five of nine `WRITE_EFFECT_ORDER` steps by hand |
| "WAL delete MISSING on the lazy path" | **Confirmed** | no `WalPersistence` effect anywhere in `drain_lazy_purge_effects`; contrast `event_loop.rs:338-349` |
| "dirty counter MISSING on the lazy path" | **Confirmed** | `check_and_delete_expired` (`hashmap.rs:480-498`) bumps `expired_keys`, never `dirty` |
| "resurrection suspect" (whole-key TTL) | **REFUTED** | recovery filters on absolute `expires_at` (`recovery.rs:150-158`); covered by `FM-PERSISTENCE-036` |
| — (not in brief) | **NEW, REAL, LIVE**: hash-field-emptied keys *do* resurrect across restart | `metadata.expires_at` is `None` for a field-TTL-only hash, so the recovery filter does not apply; `collections.rs:203-228` restores expired fields |
| — (not in brief) | **NEW, REAL, LIVE**: permanently stale hot-CF row | no WAL delete + key leaves the expiry index + recovery *skips without deleting* + no compaction filter / CF TTL exists |

The missing WAL delete is therefore real, but its consequence is *not* the one the brief
guessed. Detail in [Problem](#problem) §3–§5.

## Files involved

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | **The second authority.** `drain_lazy_purge_effects` `:738-851` (~114 lines) hand-mirrors the pipeline |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | 1907 | **The interface.** `WRITE_EFFECT_ORDER` `:282`, `run_internal_removal_effects` `:561`, `EffectScope::InternalRemoval` `:241` |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **The compliant caller.** `apply_expiry_effects` `:303-366` routes active expiry through the pipeline; also holds the hand-discard of lazy buffers `:255-281` |
| `frogdb-server/crates/core/src/store/hashmap.rs` | 2977 | Report buffers `:149`/`:161`/`:170`/`:183`; `check_and_delete_expired` `:480-498`; `purge_expired_hash_fields` `:1392`; `lazy_purge_buffers_empty` `:511` |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | `Store` trait: four defaulted drain methods `take_lazily_purged` `:522`, `take_lazily_emptied` `:540`, `take_lazily_expired_fields` `:555`, `take_lazily_shrunk` `:573` |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 704 | `ExpiryResult { deleted_keys, emptied_keys, fields_expired, budget_exhausted }` `:42-55` — the shape `ExpiryReport` converges on |
| `frogdb-server/crates/persistence/src/recovery.rs` | 664 | Hot-tier expiry filter `:150-158` (skip, no delete) vs warm-tier `:214-219` (skip **and** delete) |
| `frogdb-server/crates/persistence/src/serialization/collections.rs` | 258 | `deserialize_hash_with_field_expiry` `:203-228` restores every field, expired or not |
| `frogdb-server/crates/core/src/persistence/store_recovery.rs` | 309 | `StoreRestoreSink` `:29`; `FM-PERSISTENCE-036`-tagged tests `:187`, `:210` |
| `frogdb-server/crates/core/src/persistence/test_harness.rs` | 624 | `CrashTestHarness` `:29` — RocksDB/WAL-level crash harness |
| `frogdb-server/crates/shard-harness/tests/eviction_spill_failure.rs` | 303 | Level-3 precedent driving the *sibling* internal-removal authority; the shape the crash test copies |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | `FM-PERSISTENCE-036` `:614`, `FM-PERSISTENCE-044` `:626` — neither covers lazy expiry |

## Problem

### 1. Two removal authorities, one of which is a hand copy

FrogDB has one canonical post-write pipeline. `WRITE_EFFECT_ORDER`
(`post_execution.rs:282-292`) is a nine-element array, and the module doc at `:12` states
the invariant plainly: "here the ordering lives in exactly one place … and nowhere else."
`run_write_effects` `:305` iterates it for commands; `run_scatter_effects` `:512` for
scatter parts; `run_internal_removal_effects` `:561-603` for engine-initiated removals,
under `EffectScope::InternalRemoval { propagation }`.

Active expiry is a compliant caller. `apply_expiry_effects`
(`event_loop.rs:303-366`) hands its `ExpiryResult` straight to the pipeline at `:338-349`
with `RemovalPropagation { wal: true, replicate: false }` and `ENGINE_INTERNAL_CONN_ID`.
One call; the ordering is not restated.

Lazy expiry is not. `ShardWorker::drain_lazy_purge_effects`
(`worker.rs:738-851`) drains four store buffers and then re-implements the effect
sequence by hand: tracking invalidate → `delete_from_search_indexes` →
`emit_keyspace_notification` (`"expired"`/`EXPIRED` for purged keys `:788`, `"del"`/
`GENERIC` for emptied keys `:811`) → `fire_key_expired` probe →
`drain_stream_waiters_with_error` → `add_expired_keys` + `KeysExpired::inc_by` `:827-835`
→ `bump_versions_for` `:843-849`.

This is a **module boundary that leaks**: the pipeline's *interface* (an effect scope plus
a removal reason) is bypassed, and the caller re-derives the *implementation*. The
duplication is not abstract — it is lossy. Measured against `WRITE_EFFECT_ORDER`, the hand
copy is missing four of nine effects:

| `WriteEffectKind` | Active expiry (pipeline) | Lazy expiry (hand copy) |
| --- | --- | --- |
| Tracking invalidation | yes | yes |
| Search index | yes | yes |
| Keyspace notification | yes | yes |
| USDT probe | yes | yes |
| Version bump | yes | yes (or withheld at WATCH) |
| **DirtyCounter** | yes | **no** |
| **WalPersistence** | yes (`wal: true`) | **no** |
| **ReplicationBroadcast** | n/a (`replicate: false`) | n/a |
| **WaiterSatisfaction** | yes | **partial** — stream waiters only |
| **KeysizesFlush** | yes | **no** |

The five that agree agree because a previous round hand-ported them
(`.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md`, done 2026-07-22).
That proposal closed the notification/tracking/search gaps by *widening the hand copy*
rather than by deleting it. This proposal finishes the job the other way: delete the copy,
route through the authority. The evidence that widening does not hold is that four effects
were still missed by the same edit.

The seam is already load-bearing enough that `event_loop.rs` has to defend against it. At
`:249-253` the active sweep `debug_assert!`s `self.store.lazy_purge_buffers_empty()`, and
at `:255-281` it hand-discards three of the four buffers with a comment explaining why the
active path must not double-report what it already reported. That defensive code exists
solely because two authorities write into one set of buffers.

### 2. The report is four buffers and four trait methods, not one value

`hashmap.rs` carries `lazily_purged: Vec<Bytes>` `:149`, `lazily_emptied: Vec<Bytes>`
`:161`, `lazily_expired_fields: u64` `:170`, `lazily_shrunk: Vec<Bytes>` `:183`. The
`Store` trait exposes each through its own defaulted drain method (`mod.rs:522`, `:540`,
`:555`, `:573`). Every consumer must know all four exist and drain all four; forgetting one
is silent. `event_loop.rs:255-281` forgets one *on purpose* and needs a comment to say so.

The active sweep already has the value type this wants: `ExpiryResult { deleted_keys,
emptied_keys, fields_expired, budget_exhausted }` (`active_expiry.rs:42-55`). Two paths
describe the same event — "the store removed things because they expired" — in two
vocabularies.

### 3. Missing WAL delete → a permanently stale hot-CF row (REAL, LIVE)

`check_and_delete_expired` (`hashmap.rs:480-498`) calls `self.uninstall(key)`, bumps
`expired_keys`, pushes onto `lazily_purged`. `uninstall` removes the key from the
in-memory map, the **expiry index**, the ts-label index and the field-expiry index. No WAL
effect is produced anywhere downstream, because `drain_lazy_purge_effects` has no
`WalPersistence` step.

Consequences, in order:

1. The RocksDB hot-CF row for that key is never deleted.
2. The key is gone from the expiry index, so the active sweep — which is index-driven
   (`active_expiry.rs:152-230` drains the index; it is not Redis-style sampling) — will
   never revisit it and never issue the delete either.
3. Recovery *skips* the row instead of deleting it:

   ```rust
   // frogdb-server/crates/persistence/src/recovery.rs:150-158  (hot tier)
   Ok((val, metadata)) => {
       // Skip keys whose expiry has already passed.
       if let Some(expires_at) = metadata.expires_at
           && expires_at <= now
       {
           stats.keys_expired_skipped += 1;
           continue;                      // <- skipped, NOT deleted
       }
   ```

   The warm tier at `:214-219` does the opposite — it calls
   `let _ = rocks.delete_warm(shard_id, &key);`. The asymmetry is undocumented.
4. No RocksDB compaction filter and no column-family TTL exists anywhere under
   `frogdb-server/crates/persistence/src/` (verified by grep at HEAD), so nothing reclaims
   it out of band.

The row therefore survives every restart, forever. On a read-through TTL workload (cache
keys touched once after expiry, never again) the hot CF accumulates dead rows without
bound: disk growth, compaction cost, and a recovery scan that pays for keys it will only
skip. This is a **live leak**, not a correctness break — but it is the direct, provable
consequence of the missing WAL effect the brief flagged.

### 4. Whole-key TTL resurrection: REFUTED

The obvious fear — "no tombstone means the key comes back after a crash" — does not hold,
and the proposal must say so rather than inherit the brief's suspicion.

The serialization header stores `expires_at_ms` as an **absolute** unix timestamp, and
`recovery.rs:150-158` compares it against `now`. Any key whose deadline has passed is
skipped on the way in. The EXPIRE family writes with `WalStrategy::PersistFirstKey`, so
the durable header always carries the current deadline; there is no window in which the
durable copy claims a later expiry than the live one. `KeyMetadata::is_expired()`
(`types/src/types/mod.rs:456-460`) reads through `crate::clock::now()`, so a test can move
the deadline deterministically.

This is exactly the contract of **`FM-PERSISTENCE-036`**
(`.scratch/hardening/specs/persistence-failure-modes.md:614`, "an expired key never comes
back from disk"), forced by `test_expiry_filtering_on_recovery`,
`test_immediate_expiry_recovery`, `test_expiry_index_rebuilt`, `test_recover_skips_expired`
(`store_recovery.rs:210`), `test_recover_with_expiry` (`store_recovery.rs:187`),
`test_expiry_roundtrip`, and
`the_epoch_decodes_as_a_passed_deadline_not_as_never_expires`. **Verdict: refuted, already
covered, no new work.**

### 5. Hash-field-emptied resurrection: REAL and LIVE (transient)

The refutation in §4 depends entirely on `metadata.expires_at` being `Some`. For a key
emptied by *field* expiry it is `None`.

Chain, each link verified at HEAD:

1. A hash with per-field TTLs but no key-level TTL has `expires_at = None` in its header.
2. A lazy read calls `purge_expired_hash_fields` (`hashmap.rs:1392`, clock read via
   `crate::clock::now()` at `:1394`). When the last field dies it calls `self.delete(key)`
   and pushes onto `lazily_emptied`.
3. `drain_lazy_purge_effects` fires the `"del"` notification (`worker.rs:811`) and the
   version bump — but no WAL delete. The durable row still holds the full hash.
4. Crash / restart. `recovery.rs:150-158` sees `expires_at == None`; the filter does not
   apply; the row is restored.
5. `deserialize_hash_with_field_expiry` (`collections.rs:203-228`) rebuilds **every**
   field including already-past ones, and `HashValue::from_entries_with_expiries`
   (`types/src/types/hash.rs:495-515`) does not filter either.
6. Post-restart, `EXISTS` / `DBSIZE` / `TYPE` / `SCAN` observe a key that was deleted — and
   `del`-notified to subscribers — before the crash.

Self-healing: the next active-expiry tick (`expiry_interval` is
`Duration::from_millis(100)`, `event_loop.rs:24`) or any hash read re-kills it. So the
window is **live but transient (~100 ms + budget)** — long enough to be observed by
`DBSIZE`, by a `SCAN` cursor, by a keyspace subscriber that now sees no second `del`, and
by any client that took the first `del` as final.

The active sweep does **not** have this defect, because it passes `wal: true` and the
pipeline issues the delete. The bug is precisely "which authority happened to kill the
key", which is the architectural smell in §1 made observable.

**No FM row covers this.** `FM-PERSISTENCE-036` is about `expires_at`-carrying keys;
`FM-PERSISTENCE-044` (`:626`, "a key past its deadline is never resurrected by a command
that reads through the expiry window", forced by
`persist_on_expired_key_deletes_instead_of_immortalizing`,
`persist_on_expired_key_leaves_no_expiry_index_orphan`,
`nondestructive_probes_do_not_see_a_past_deadline_key`) is about in-process reads, not
restart. Neither mentions hash-field expiry or the lazy WAL tombstone.

### 6. Missing dirty counter

`check_and_delete_expired` increments `expired_keys` (`hashmap.rs:497`) but never `dirty`.
`increment_dirty` (`hashmap.rs:1594`) is called only from the pipeline's `DirtyCounter`
effect, which the lazy path skips. So a lazy expiry does not advance `rdb_changes_since_
last_save`, does not contribute to save triggers, and diverges from Redis, which does
`server.dirty++` on lazy expire. The active sweep is correct here for the same
one-authority reason.

## Proposed change

Collapse the second authority into the first. One value type, one drain seam, one caller
of the existing pipeline.

### A. `ExpiryReport` — one value replaces four buffers

Introduce in `frogdb-server/crates/core/src/store/`:

```rust
/// Everything the store removed for expiry reasons since the last drain.
#[derive(Debug, Default)]
pub struct ExpiryReport {
    pub purged: Vec<Bytes>,          // whole keys that died on a key-level TTL
    pub emptied: Vec<Bytes>,         // keys whose last hash field expired
    pub shrunk: Vec<Bytes>,          // keys that lost fields but survive
    pub expired_fields: u64,         // field-level counter for stats
}

impl ExpiryReport {
    pub fn is_empty(&self) -> bool { /* ... */ }
}
```

`Store` gains **one** method, `take_expiry_report(&mut self) -> ExpiryReport`, replacing
the four defaulted `take_lazily_*` methods (`mod.rs:522`, `:540`, `:555`, `:573`) and
`lazy_purge_buffers_empty` (`hashmap.rs:511`). The struct fields stay private to
`HashMapStore`; only the report crosses the trait.

This is the **adapter** between store-side bookkeeping and shard-side effects. It shares
its shape with `ExpiryResult` (`active_expiry.rs:42-55`) deliberately: after this change
both expiry authorities speak one vocabulary, and a follow-up can make `ExpiryResult`
*be* an `ExpiryReport` plus `budget_exhausted`. That fold is **out of scope here** —
`ExpiryResult` is produced by a budgeted sweep and carries a control-flow field; merging
the two is a separate, larger change with its own risk.

### B. Route the report through `run_internal_removal_effects`

`drain_lazy_purge_effects` (`worker.rs:738-851`, ~114 lines) is replaced by a thin
adapter — take the report, and for each removed key call the existing pipeline exactly the
way `apply_expiry_effects` already does at `event_loop.rs:338-349`:

- purged keys → `RemovalReason::Expired`, `RemovalPropagation { wal: true, replicate: false }`
- emptied keys → `RemovalReason::FieldEmptied`, same propagation
- shrunk keys → version bump + notification only; **no** removal (the key still exists),
  so these do not enter the removal pipeline. They keep their current handling.

`replicate: false` is preserved verbatim from the active path: expiry is a local decision
on a primary and replicas expire independently; only the WAL tombstone is required. Do
**not** flip this — it is a replication-semantics change with its own locked area.

The version-bump split established by the earlier proposal must survive. `WATCH`-time
purge (`apply_lazy_purge_effects_no_version_bump`, `worker.rs:729`) fires the removal
effects but withholds the shard-version bump, because bumping at `WATCH` would make
`WATCH` a mutating op and over-abort unrelated watchers. Expressed against the pipeline
this becomes a caller-side flag on the adapter, not a second effect ordering: the adapter
calls `run_internal_removal_effects` and then conditionally bumps, mirroring
`event_loop.rs:363-365`'s `bump_version_global()`.

### C. Delete what the authority makes redundant

| Deleted | Why it can go |
| --- | --- |
| `worker.rs:738-851` `drain_lazy_purge_effects` body | the pipeline is the ordering |
| four `take_lazily_*` trait methods (`mod.rs:522`-`:573`) | one `take_expiry_report` |
| `lazy_purge_buffers_empty` (`hashmap.rs:511`) | `ExpiryReport::is_empty` |
| `event_loop.rs:249-253` `debug_assert!` | one authority ⇒ nothing to assert |
| `event_loop.rs:255-281` three-buffer hand-discard + comment | the sweep drains one report |

Net: roughly −150 lines of shard code, +40 for the report type, and one fewer concept
("lazy purge buffers") in the store's public surface.

### D. Depth and locality

`run_internal_removal_effects` is already a **deep module**: a two-argument interface
(keys + propagation) over nine ordered effects, WAL strategy selection, replication
framing, and waiter satisfaction. The lazy path currently reaches around it. Adding a
fifth caller costs nothing in interface width and buys every effect the pipeline gains in
future — the leverage is that the next effect added to `WRITE_EFFECT_ORDER` reaches lazy
expiry for free instead of needing a second hand-port.

Locality improves in the other direction too: today "what happens when a key expires" is
answered by reading `post_execution.rs` **and** `worker.rs` **and** the discard comment in
`event_loop.rs`. After, it is answered by `WRITE_EFFECT_ORDER` plus which
`RemovalPropagation` the caller passes.

### E. Deletion test, applied honestly

*If `ExpiryReport` did not exist, what would be worse?* The four buffers would still be
four buffers, but nothing would break — the type is ergonomics and a forcing function for
the single drain seam. **The report alone is a B-grade change.** It earns its place only
because it makes the one-authority routing (§B) natural rather than bolted on.

*If the routing (§B) did not happen?* The WAL tombstone, dirty counter, keysizes flush and
non-stream waiter satisfaction stay missing on the lazy path, and every future effect must
be hand-ported a third time. Two of those gaps are observable defects (§3, §5). **The
routing is the load-bearing half; §A is scaffolding for it.**

If the review wants the smallest honest version: §B without §A (route the four existing
buffers through the pipeline) delivers all the correctness value. §A is the tidy-up that
makes the result readable. They are separable and should be separable commits.

## Testability improvement

Today the lazy path is testable only by asserting on its externally visible effects one at
a time — which is how four effects went missing. After the change, lazy expiry is covered
by the *existing* `WRITE_EFFECT_ORDER` order-validation tests
(`post_execution.rs:736-866`) and the internal-removal tests (`:1436-1720`), because it is
the same code path. That is the leverage: one authority ⇒ one test surface.

### Crash-test design for the resurrection suspect (§5)

Two tiers. Both are new tests; both must be written **failing first** against the new FM
row (see [Spec / LOCKED impact](#spec--locked-impact)).

**Tier 1 — level-3, WAL-effect pin (the primary forcing test).**
Harness: `frogdb-shard-harness`, modelled directly on
`frogdb-server/crates/shard-harness/tests/eviction_spill_failure.rs` (303 lines), which
already drives a real `ShardWorker` + `RocksStore` + `NotificationCapture` +
`RecordingBroadcaster` against the *sibling* internal-removal authority — its header
explicitly names `run_internal_removal_effects` ("WAL delete, replicated `DEL`, `evicted`
keyspace notification"). Swap `RecordingBroadcaster` for `FakeWalSink`
(`frogdb-persistence/src/wal/fake.rs`, re-exported at `wal/mod.rs:10`; wired through
`core/src/shard/fake_wal_registry.rs` and `core/src/shard/builder.rs:392`; precedents at
`shard-harness/tests/scenario_s6.rs:175` and `server/tests/common/invariants.rs:592-602`).

Shape:

1. `HSET k f v` + `HEXPIRE k 1 FIELDS 1 f` (field TTL only — key-level TTL must stay unset,
   that is the whole point).
2. Disable the active sweep for the window (`DEBUG SET-ACTIVE-EXPIRE 0` at level 4, or
   simply do not tick `expiry_interval` in the shard harness) so the *lazy* path is the one
   that kills the key. Without this the test proves nothing about lazy expiry.
3. Advance the clock past the field deadline through the clock seam.
4. Issue a read that triggers `purge_expired_hash_fields` → key emptied → `del`
   notification observed via `NotificationCapture`.
5. **Assert a `RecordedWalEffect { kind: WalEffectKind::Delete, key: "k", .. }` is present.**
   Today: absent → test fails. This is the exact, minimal pin on the missing tombstone, and
   it is deterministic (no restart, no timing).

The same fixture with a key-level TTL pins the whole-key case (§3's stale row) — same
assertion, and it fails today for the same reason even though §4 says it does not
resurrect.

**Tier 2 — restart round-trip (the user-visible pin).**
Harness: `frogdb-server/crates/server/tests/integration_persistence.rs`, whose Test 3
`test_deleted_keys_stay_deleted_after_restart` `:139` is already the exact shape (write →
shutdown → restart → read), with `persistence_config(data_dir)` `:12` supplying the
durable setup. Steps 1–4 as above over TCP with `DEBUG SET-ACTIVE-EXPIRE 0`, then
`DEBUG RELOAD`-free hard restart, then assert `EXISTS k` → `0` **before** the first active
tick can heal it. Today: returns `1`.

Deliberately **not** `CrashTestHarness` (`core/src/persistence/test_harness.rs:29`,
consumer `crash_recovery_tests.rs`, 1800 lines): it operates at the RocksDB/WAL level
(`put_with_expiry` `:173`, `crash()` `:209`, `recover()` `:239`) and has no `ShardWorker`,
so it cannot exercise the lazy-purge path at all. It stays the right harness for
`FM-PERSISTENCE-036`-style durable-layer questions; it is the wrong one here. Naming it and
rejecting it is part of the design.

**Tier 2 is inherently racy against the 100 ms sweep** — it must gate the sweep off, not
race it. A test that merely reads fast enough is a flake generator and must not land.

### Mutation-gate consequence

`frogdb-persistence` sits at gate 0.85. The forcing test must live **in the crate whose
code changes**: the WAL-effect assertion is in `frogdb-shard-harness` (which exercises
`frogdb-core`), so it contributes nothing to `frogdb-persistence`'s score. If the fix
touches `recovery.rs` (see H2 below), that change needs its own in-crate test in
`frogdb-persistence`. `just mutants-diff frogdb-persistence` before pushing.

## Spec / LOCKED impact

**This is the gating step. No code lands before it.**

1. **New row `FM-PERSISTENCE-0NN`** in `.scratch/hardening/specs/persistence-failure-modes.md`
   (currently 52 rows; `Status: LOCKED`). Proposed statement: *"a key removed by lazy
   expiry does not survive a crash, and its durable row is reclaimed."* This covers both
   §3 (stale row) and §5 (field-emptied resurrection); if review prefers, split into two
   rows — the field-emptied case is a correctness break, the stale row is a leak, and they
   have different severities.
2. The row must name its forcing tests. `just lint-failure-modes` (`Justfile:293`, run
   inside `just lint` `:319`) enforces spec↔test agreement in both directions: every
   `FM-<AREA>-NNN` row names forcing tests, every tagged test matches a row. A row without
   tests fails the lint; a test tagged with a nonexistent row fails it too.
3. Order is **row → failing test → fix**, per `adr/0003` and the locked-area contract in
   `CLAUDE.md`. **The fix is explicitly not an independently-landable one-liner.** Adding
   `wal: true` to a lazy removal is a two-token diff, and that is exactly why it must not
   be committed as a drive-by: it changes durable behaviour in a locked area with a
   mutation gate, and the spec row is the contract that makes the change reviewable and
   the regression permanent.
4. `frogdb-txn` / `frogdb-vll` (gate 0.90) are **not** touched. Lazy expiry runs inside the
   shard worker, below the transaction layer; `EffectScope::Transaction` is untouched.

### Seam-lint clearance

- **`scripts/clock-seam.py`** (`just lint-clock-seam`, `Justfile:1284`; part of
  `just lint-gates` `:329`, which lefthook runs unconditionally). It bans
  `std::time::Instant::now()` / `SystemTime::now()` in `frogdb-server/crates/*/src`
  non-test code; compliant reads go through `clock::now()` / `clock::system_now()` /
  `tokio::time::Instant::now()`. Every expiry clock read on the touched paths is **already
  compliant**: `purge_expired_hash_fields` uses `crate::clock::now()`
  (`hashmap.rs:1394`), `KeyMetadata::is_expired()` uses `crate::clock::now()`
  (`types/src/types/mod.rs:456-460`), and the sweep cadence is a
  `tokio::time::interval` (`event_loop.rs:24`). The count-pinned `ALLOWLIST`
  (`clock-seam.py:75`) contains **no** entry for `worker.rs`, `event_loop.rs`,
  `hashmap.rs` or `active_expiry.rs`, so this change must neither add nor decrement a
  pinned count. Constraint on the implementation: the new adapter reads no clock of its own
  — it consumes an `ExpiryReport` the store already produced. Recovery's `now`
  (`recovery.rs:153`) likewise stays as-is.
- **`scripts/durable-ack.py`** (`just lint-durable-ack`, `Justfile:1290`): a hand-crafted
  single-file pin on the openraft storage impl
  `frogdb-server/crates/cluster/src/storage.rs`, scoped to `save` / `save_vote` / `append`.
  Nothing in this proposal touches that file or those functions. **Unaffected.**
- The remaining `lint-gates` members (redirect replies, metrics emission, …) are not on
  these paths. Adding a WAL effect *through the pipeline* is the compliant direction for
  every chokepoint gate in the family — the gates exist to force exactly this routing.

### Behaviour changes

| Change | Visible as | Risk |
| --- | --- | --- |
| WAL delete on lazy whole-key expiry | stale hot-CF rows reclaimed; recovery scan shrinks | low — matches active sweep exactly |
| WAL delete on lazy field-emptied key | key stays absent across restart | **medium — this is the correctness fix; needs the FM row** |
| Dirty counter bumped on lazy expiry | `rdb_changes_since_last_save` advances; save triggers fire sooner | low — matches Redis and the active sweep |
| Keysizes flush on lazy expiry | `INFO keysizes` histograms stop drifting | low |
| Full waiter satisfaction (not just stream waiters) | blocked clients woken on lazy removal | **medium — overlaps proposal 88; see below** |

## Risks / scope boundaries vs siblings

**Proposal 88 (PN12, blocking-serve wake effects) — declared order-or-conflict edge.**
Both proposals converge on the same seam. 83 adds a **fifth caller** of the existing
`run_internal_removal_effects` / `EffectScope::InternalRemoval`; 88 adds a **new variant**
(`EffectScope::ServedWake` or equivalent) to `EffectScope` and a new arm to the
`WRITE_EFFECT_ORDER` dispatch match. These do not conflict semantically, but they conflict
textually in `post_execution.rs` (the `EffectScope` enum `:241`, the per-effect
`match scope` arms `:330`/`:358`/`:392`/`:426`/`:454`, and the order-validation tests
`:736-866`).

**Ruling: land 83 first.** 83 only *uses* the interface; 88 *widens* it. Landing the widen
first forces 83 to rebase across a changed enum for no benefit. If they must overlap, 88's
new variant must be added by a separate commit that touches only `post_execution.rs`, and
83's adapter must not be written against a pre-88 exhaustive match it will have to
re-touch. The `WaiterSatisfaction` row in the table above is the specific overlap: 83
gains full waiter satisfaction for free by routing through the pipeline, and 88 changes
what "satisfaction" does. If 88 lands first, 83 inherits 88's semantics silently — another
reason for the stated order.

**Boundary vs `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md`
(done 2026-07-22).** That proposal established the single-drain-point discipline and the
`WATCH`-time bump split, and recorded a per-effect idempotency argument (exactly-once push
in `check_and_delete_expired`'s removal branch, exactly-once drain via `std::mem::take`).
**That argument is a precondition of this proposal and must be preserved** — a WAL delete
emitted twice is harmless (`DeleteIfMissing` is idempotent) but a doubled `expired`
notification is not, and the existing argument is what rules it out. This proposal does not
re-litigate it; it changes *where* the effects run, not *how many times*.

**Boundary vs the `ExpiryResult`/`ExpiryReport` fold.** Explicitly deferred (§A). Merging
the budgeted sweep's result type with the lazy report is a bigger change touching
`active_expiry.rs`'s control flow.

**Boundary vs eviction.** `RemovalReason::Evicted` and the warm-tier spill path
(`eviction_spill_failure.rs`) are untouched. They already go through the pipeline; this
proposal makes lazy expiry look like them, not the reverse.

**Risk: replication semantics.** `replicate: false` must be carried over verbatim.
Flipping it would propagate expiry decisions to replicas and is a `frogdb-replication`
(gate 0.85) change, out of scope.

**Risk: WAL volume.** Every lazy expiry now writes a tombstone. On a workload that reads
many expired keys this is new write traffic. Mitigation: it is exactly the traffic the
active sweep already generates for the same keys — this removes an accidental
*under*-write, it does not add a new class of write. Worth a benchmark note, not a blocker.

**Risk: `debug_assert` removal.** Deleting `event_loop.rs:249-253` removes a real invariant
check. It is safe only *because* there is now one authority; if the implementation keeps
any second drain site, the assert must stay. Reviewer should treat a surviving second drain
site as a failed implementation of this proposal.

## Effort

| Piece | Size | Note |
| --- | --- | --- |
| New `FM-PERSISTENCE` row(s) + `just lint-failure-modes` green | S | gating; must be first |
| Tier-1 failing test (shard-harness + `FakeWalSink`) | S | copy `eviction_spill_failure.rs` |
| Tier-2 restart test (`integration_persistence.rs`) | S | copy Test 3 `:139` |
| §B routing (four buffers → `run_internal_removal_effects`) | M | the load-bearing change; `WATCH` bump split must survive |
| §A `ExpiryReport` + trait collapse (5 methods → 1) | S–M | touches every `Store` impl |
| §C deletions (assert, hand-discard, hand copy) | S | falls out of §B |
| `just mutants-diff` on touched locked crates | S | push discipline |

**Overall: M.** Not L — the pipeline already exists and already has a compliant caller to
copy; the work is deletion plus one adapter. Not S — it is spec-first in a locked area with
two new test tiers and a cross-crate trait change.

### Independently-landable hotfixes

- **H1 — Tier-1 WAL-effect test, `#[ignore]`d.** Landing the *failing* test first, ignored,
  with the FM row, is the spec-first opening move and is independently landable. Nothing
  else here is.
- **H2 — recovery hot/warm asymmetry note.** `recovery.rs:150-158` skips where `:214-219`
  deletes. A comment recording *why* (or a `delete_hot` to match) is a genuine standalone
  improvement, but the `delete_hot` variant is a **durable-behaviour change in a locked
  crate** and therefore inherits the spec-first rule. Only the comment is truly free.
- **H3 — NOT a hotfix: adding `wal: true`.** Stated explicitly because it looks like one.
  Two tokens, locked area, no spec row, no regression test. **Do not land it alone.**

**Security:** no security-relevant findings in this candidate. Per standing policy,
security items are **filed but parked — record only**; nothing to record here.

## References

- `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md` — prior round;
  established the drain seam and `WATCH` bump split this proposal preserves
- `.scratch/testing-improvements-round2/issues/open/54-bcast-trackers-never-invalidated-on-lazy-expiry.md`
- `.scratch/testing-improvements-round2/issues/open/22-expiry-not-checked-before-reads.md`
- `adr/0003` — persistence boundary
- Proposal 88 (PN12, blocking-serve wake effects) — shared `WRITE_EFFECT_ORDER` seam
