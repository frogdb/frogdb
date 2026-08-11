# Proposal 83 — Lazy expiry: `ExpiryReport` and one removal authority

Status: draft, **revision 2** (arch-deepening round 38, lane protocol/net/core, candidate PN5)
Authored at HEAD `49a21b18`; **revised at `eb8760e9`** (2026-08-11), local mode. Every file:line
below was re-derived a second time against the working tree during this revision.
**LOCKED-ADJACENT.** Touches the persistence contract (`frogdb-persistence` +
`frogdb-recovery`, mutation gate 0.85; boundary ADR `adr/0003`). Every behaviour change
below is **spec-first**: new `FM-PERSISTENCE` row → failing test → fix. Nothing here is a
one-liner hotfix. See [Spec / LOCKED impact](#spec--locked-impact).

> **Revision 2** (post adversarial review; verdict AMEND). The core diagnosis survived — two
> removal authorities, a real WAL-tombstone gap, a real cross-restart resurrection — but rev 1
> was **structurally unbuildable** in §B/§C and shipped two tests that would have passed against
> unfixed code. Four blocking findings applied:
>
> - **The effect pipeline would have become reentrant into itself.** `run_internal_removal_effects`
>   is `async`; the lazy drain is reached from *inside* the pipeline's own `WaiterSatisfaction`
>   stage through a **synchronous, recursive** driver. §B now carries an explicit reentrancy
>   design ([§B.2](#b2-the-reentrancy-problem-and-the-chosen-design)).
> - **The `WATCH` no-bump seam has no pipeline equivalent.** `VersionIncrement` is
>   unconditionally first in `WRITE_EFFECT_ORDER` and `EffectScope::InternalRemoval` carries no
>   bump-suppression axis. Two options presented, orchestrator ruling requested
>   ([§B.3](#b3-the-watch-no-bump-axis--orchestrator-ruling-requested)).
> - **H1's FM row would have broken `just lint`** (`frogdb-shard-harness` is not in
>   `scripts/failure-modes.py`'s `NEXTEST_CRATES`). H1 amended.
> - **The Tier-2 restart test was a false green** (the boot sweep reclaims the row before the
>   client's first command). Replaced.
>
> §3 narrowed, §5 rewritten (the latency framing was unsupported; the durable/duplicate-effect
> defect is the real residue), §C narrowed, three sibling overlaps declared. Full accounting in
> [§Revision ledger](#revision-ledger).

## Corrections to the lane brief

The brief is stale; each of its PN5 claims was re-derived at HEAD.

| Brief claim | Verdict at HEAD | Evidence |
| --- | --- | --- |
| "lazy expiry hand-rolls the effect set instead of reusing the pipeline" | **Confirmed** | `worker.rs:738-851` mirrors five of nine `WRITE_EFFECT_ORDER` steps by hand |
| "WAL delete MISSING on the lazy path" | **Confirmed** | no `WalPersistence` effect anywhere in `drain_lazy_purge_effects`; contrast `event_loop.rs:338-349` |
| "dirty counter MISSING on the lazy path" | **Confirmed** | `check_and_delete_expired` (`hashmap.rs:480-498`) bumps `expired_keys`, never `dirty` |
| "resurrection suspect" (whole-key TTL) | **REFUTED** | recovery filters on absolute `expires_at` (`recovery.rs:150-158`); covered by `FM-PERSISTENCE-036` |
| — (not in brief) | **NEW, REAL, LIVE**: a lazily field-emptied key's removal is *not durable*, and its effects fire *twice* across a restart | `metadata.expires_at` is `None` for a field-TTL-only hash, so the recovery filter does not apply; `collections.rs:203-228` restores expired fields; the boot sweep then re-fires `del` + re-counts `expired_keys`. **Rev 2: the "client-observable resurrection window" framing is refuted — see §5** |
| — (not in brief) | **NEW, REAL, LIVE**: permanently stale hot-CF row (read-triggered purges only) | no WAL delete on the *drain* + `uninstall` drops the expiry-index entry + recovery *skips without deleting* + no compaction filter / CF TTL exists. **Rev 2: narrowed — a purge inside a `DeleteKeys`/`PersistOrDeleteFirstKey` write still gets an incidental tombstone; see §3** |

The missing WAL delete is therefore real, but its consequence is *not* the one the brief
guessed. Detail in [Problem](#problem) §3–§5.

## Files involved

| File | Lines | Role |
| --- | --- | --- |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | **The second authority.** `drain_lazy_purge_effects` `:738-851` (~114 lines) hand-mirrors the pipeline |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | 1907 | **The interface.** `WRITE_EFFECT_ORDER` `:282`, `run_internal_removal_effects` `:561`, `EffectScope::InternalRemoval` `:241` |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **The compliant caller.** `apply_expiry_effects` `:303-366` routes active expiry through the pipeline; also holds the hand-discard of lazy buffers `:255-281`. Also: the biased `select!` `:57` and the 100 ms `expiry_interval` `:24` that make the rev-1 Tier-2 test a false green |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **The reentrancy site (rev 2, new).** `drive_satisfaction` `:255-258` (sync wrapper: body + drain), `drive_satisfaction_body` `:271` with the BLMove wake-cascade recursion at `:374` and the depth cap `:277`; `StreamSatisfaction::check_key` `:1077-1085` (`purge_if_expired` `:1079` → `KeyReady::DrainNoGroup` `:1083`); sibling `purge_if_expired` reads at `:671`, `:914` |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | Two drain seams: `execute_command_inner` `:113`, `execute_scatter_part` `:733` (both `async`); `execute_transaction` `:554` calls `purge_expired_watches` |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | 535 | `CoreMsg::GetVersion` arm: `apply_lazy_purge_effects_no_version_bump` `:131`, with the F3 rationale at `:113-130`; enclosing `dispatch_core` is `async` `:10` |
| `frogdb-server/crates/core/src/store/hashmap.rs` | 2977 | Report buffers `:149`/`:161`/`:170`/`:183`; `check_and_delete_expired` `:480-498`; `purge_expired_hash_fields` `:1392`; `lazy_purge_buffers_empty` `:511`; the entry-lifecycle seam `install` `:367` / `uninstall` `:437` (`install` `:389-396` re-derives the field-expiry index — the §5 refutation) ; `restore_entry` `:266` → `replace_entry` `:328` → `install` |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | `Store` trait: four defaulted drain methods `take_lazily_purged` `:522`, `take_lazily_emptied` `:540`, `take_lazily_expired_fields` `:555`, `take_lazily_shrunk` `:573` |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 704 | `ExpiryResult { deleted_keys, emptied_keys, fields_expired, budget_exhausted }` `:42-55` — the shape `ExpiryReport` converges on |
| `frogdb-server/crates/persistence/src/recovery.rs` | 664 | Hot-tier expiry filter `:150-158` (skip, no delete) vs warm-tier `:214-219` (skip **and** delete) |
| `frogdb-server/crates/persistence/src/serialization/collections.rs` | 257 | `deserialize_hash_with_field_expiry` `:203-228` restores every field, expired or not |
| `frogdb-server/crates/core/src/shard/persistence.rs` | 908 | `execute_wal_action` `:105-133` — the only `WalAction` → target mapping. `DeleteIfMissing` `:108-115` and `PersistOrDelete` `:115-122` **do** emit a tombstone for a key a command lazily purged; `Persist` → `write_set` `:143-154` silently `Ok(())`s when `get_hot` is `None` (the §3 narrowing) |
| `frogdb-server/crates/core/src/command.rs` | — | `WalStrategy` `:360-390`; `WalAction::DeleteIfMissing` `:444`, mapped from `DeleteKeys` at `:664` and `RenameKeys` at `:673` |
| `scripts/failure-modes.py` | — | `NEXTEST_CRATES` `:64-77` — **does not contain `frogdb-shard-harness`**; `check`'s spec→test direction `:474-481` hard-errors on an unresolvable test name. Blocks H1 as rev 1 wrote it |
| `frogdb-server/crates/persistence/src/rocks/columns.rs` | 147 | `delete_warm` `:127` — the *production* tier-delete precedent an H2 `delete_hot` would mirror (`RocksStore::delete`, `rocks/mod.rs:422`, is `#[cfg(any(test, feature = "test-support"))]` with no production caller) |
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

`check_and_delete_expired` (`hashmap.rs:480-498`) calls `self.uninstall(key)` (`:437`), bumps
`expired_keys`, pushes onto `lazily_purged`. `uninstall` removes the key from the
in-memory map, the **expiry index**, the ts-label index and the field-expiry index. No WAL
effect is produced anywhere downstream, because `drain_lazy_purge_effects` has no
`WalPersistence` step.

**Narrowing (rev 2).** "No WAL delete on the lazy path" is too broad as stated, and the
over-broad version invites a reviewer to dismiss the whole section on one counterexample. The
precise claim: *the lazy drain never emits a tombstone of its own*. A lazy purge that happens
**during a write command** can still get one incidentally, from the *command's* WAL step —
`execute_wal_action` (`shard/persistence.rs:105-133`) is the single `WalAction` → target mapping,
and two of its arms probe the store before writing:

```rust
// shard/persistence.rs:108-122
WalAction::DeleteIfMissing(key) => {
    if !t.contains(key) { t.write_delete(key).await } else { Ok(()) }
}
WalAction::PersistOrDelete(key) => {
    if t.contains(key) { t.write_set(key).await } else { t.write_delete(key).await }
}
```

If a `DEL`/`UNLINK`/`GETDEL` (`WalStrategy::DeleteKeys` → `DeleteIfMissing`, `command.rs:664`) or
an `LPOP`-class command (`PersistOrDeleteFirstKey` → `PersistOrDelete`) lazily purged its own key
on the way in, `contains` is already false and the tombstone lands. The leak is therefore
**precise, not universal**. It covers exactly:

1. **Read-triggered purges** — `GET`, `TYPE`, `HGET`, `EXISTS`-via-`exists_unexpired`, the
   blocking wake path. A read command runs no `WalAction` at all, so there is no incidental
   tombstone. **This is the read-through-TTL cache workload, which is the whole point.**
2. **`PersistFirstKey` writers** — `WalAction::Persist` → `write_set` (`shard/persistence.rs:143-154`),
   which is `if let Some(wal) = … && let Some(value) = self.store.get_hot(key)` and otherwise
   returns `Ok(())`. A write command that lazily purged a *different* key (or purged its own key
   and then wrote nothing) silently produces no record.

The permanent-stale-row conclusion below **survives the narrowing unchanged**, because it rests on
the sweep being index-driven and the index entry being gone, not on the tombstone being universally
absent.

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

The row therefore survives every restart, forever. The unreachability is structural, not a
timing accident: `uninstall` (`hashmap.rs:437`) drops the key's `ExpiryIndex` entry, recovery
`continue`s past the row without restoring it (so it is never re-indexed either — contrast the
field-TTL case in §5, where `install` *does* re-derive the index), and no compaction filter or CF
TTL exists to reclaim it out of band. There is no code path left that can ever name that key
again.

On a read-through TTL workload (cache keys touched once after expiry, never again) the hot CF
accumulates dead rows without bound: disk growth, compaction cost, and a recovery scan that pays
for keys it will only skip. This is a **live leak**, not a correctness break — but it is the
direct, provable consequence of the missing WAL effect the brief flagged, and per the narrowing
above it is *precisely* the read-triggered purge that leaks.

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

### 5. Hash-field-emptied key: a durable/duplicate-effect defect (REAL and LIVE) — rewritten in rev 2

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
6. The restored row is re-indexed on the way in: `StoreRestoreSink::restore_entry`
   (`store_recovery.rs:41-49`) → `HashMapStore::restore_entry` (`hashmap.rs:266`) →
   `replace_entry` (`:328`) → `install` (`:367`), and `install` `:389-396` re-derives the
   field-expiry index from the restored value:

   ```rust
   // hashmap.rs:389-396
   if let Value::Hash(ref hash) = value
       && let Some(expiries) = hash.field_expiries()
   {
       for (field, &expires_at) in expiries {
           self.field_expiry_index.set(key.clone(), field.clone(), expires_at);
       }
   }
   ```

7. The active sweep is index-driven over that index (`active_expiry.rs:152` `run_cycle`, field
   reap at `:226`), so it finds the restored hash and re-empties it.

**Two things rev 1 claimed here are refuted (rev 2).**

- **REFUTED: "escalation to a permanent leak."** Unlike §3's whole-key case, the field-TTL row
  *is* restored and *is* re-indexed (step 6), so the sweep reclaims it. It is not a permanent
  disk leak; it is a transient live-state defect with a durable tail.
- **REFUTED: "a ~100 ms client-observable window."** `expiry_interval` is a
  `tokio::time::interval` (`event_loop.rs:24`) whose **first tick completes immediately**, and the
  shard's `select!` is `biased;` (`:57`) with the expiry arm at position 3 and `message_rx`
  deliberately **last** (`:41-56` states this explicitly: "`message_rx` is deliberately last: it
  is the one arm that can be perpetually ready, so nothing may be placed after it"). On a fresh
  worker the boot sweep therefore runs on the *first* loop iteration, before any client command is
  dequeued. A post-restart `EXISTS`/`DBSIZE`/`SCAN` cannot observe the resurrected key by racing
  the tick — there is nothing to race. The "~100 ms + budget" figure was arithmetic on a cadence
  that does not gate the first cycle.

**What is actually wrong, and it is worth fixing.** Strip the latency framing and the residue is a
clean durable-effect defect, in two parts:

1. **A removal acked to the client is not durable.** Before the crash the server told the client
   the key is gone (`HDEL`-equivalent semantics: `EXISTS` → `0`) and told every keyspace
   subscriber `del`. The durable image still holds the full hash. That is an acknowledged
   observation the storage layer did not commit — the same class of defect the persistence spec
   exists to exclude, differing from `FM-PERSISTENCE-036` only in that the guard `036` relies on
   (`metadata.expires_at`) is `None` here.
2. **The effect fires twice, and the counters double-count.** After restart the boot sweep
   re-empties the key and `apply_expiry_effects` fires a **second** `del` notification for a key
   that was already `del`-notified pre-crash, and `add_expired_keys` + `KeysExpired::inc_by` count
   the same logical expiry a second time. A subscriber holding a keyspace-notification stream
   across a restart sees a duplicate; `INFO expired_keys` and `frogdb_keys_expired_total` overcount
   by one per affected key. This is the *inverse* of the exactly-once argument the prior round
   established for the in-process case (`worker.rs:701-711`): that argument is about two drains
   within one process, and says nothing across a restart boundary.

The active sweep does **not** have either half, because it passes `wal: true` and the
pipeline issues the delete. The bug is precisely "which authority happened to kill the
key", which is the architectural smell in §1 made observable.

**Consequence for the test design.** Because the boot sweep heals the live state before the first
client command, a restart test that asserts on post-restart `EXISTS` is a **false green** — it
passes against unfixed code. The forcing assertion must be on the durable artifact (the WAL
effect / the hot-CF row) or on the duplicate notification, not on post-restart visibility. See
[Crash-test design](#crash-test-design-for-the-durable-effect-defect-5).

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

#### B.1 The routing itself

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

#### B.2 The reentrancy problem, and the chosen design

**Rev 1 got this wrong and the review caught it.** `run_internal_removal_effects` is
`async` (`post_execution.rs:561`). The lazy drain has **five production call sites**, all
synchronous today:

| Site | Enclosing fn | `async`? | Reached from inside the pipeline? |
|---|---|---|---|
| `execution.rs:113` | `execute_command_inner` | yes | no — runs *before* the pipeline |
| `execution.rs:733` | `execute_scatter_part` | yes | no |
| `worker.rs:686` (`purge_expired_watches`) | ← `execution.rs:554`, `execute_transaction` | yes | no |
| `dispatch_core.rs:131` | `dispatch_core` `:10` | yes | no |
| **`blocking.rs:257`** (`drive_satisfaction`) | — | **no** | **YES** |

The last row is fatal to the naive design. `drive_satisfaction` is sync, its body
`drive_satisfaction_body` is **recursive** (BLMove wake cascade, `:374`, depth-capped at `:277`),
and — critically — it is reached **from inside the pipeline's own `WaiterSatisfaction` stage**:

```
run_write_effects (async)
  └─ WRITE_EFFECT_ORDER[4] = WaiterSatisfaction        post_execution.rs:377
      └─ satisfy_waiters_for_command  (sync)                          :701
          └─ satisfy_waiters          (sync)                          :717
              └─ try_satisfy_{list,zset,stream}_waiters (sync)   :721-723
                  └─ drive_satisfaction (sync)                 blocking.rs:255
                      ├─ drive_satisfaction_body → check_key
                      │      └─ store.purge_if_expired(key)      blocking.rs:1079
                      │             ⇒ fills `lazily_purged`
                      └─ apply_lazy_purge_effects()             blocking.rs:257
```

Today that trailing drain is a **sync leaf**, and the whole thing is safe. Turning it into
`run_internal_removal_effects` — as rev 1's §B implied — would (a) make the pipeline reentrant
into itself, (b) force `drive_satisfaction_body`'s recursion to become an async recursion
requiring `Box::pin` at `:374`, and (c) infect `satisfy_waiters`/`satisfy_waiters_for_command`
and every `try_satisfy_*` with `async`. Rev 1 never mentioned `blocking.rs`.

**A decisive structural fact makes a clean design available.** `try_satisfy_list_waiters` /
`try_satisfy_zset_waiters` / `try_satisfy_stream_waiters` have **exactly one non-test caller in
the whole workspace**: `post_execution.rs:721-723`, the pipeline's own `WaiterSatisfaction` stage.
Every other reference is a unit test inside `blocking.rs` (`:1644`, `:1683`, `:1737`, `:1783`,
`:1809`, `:1833`, `:1865`, `:1886`, `:1922`, `:1976`). In production, a purge triggered by waiter
satisfaction is therefore *by construction* already inside a pipeline run.

**Chosen design — sync collection, async drain-to-fixpoint at the outermost pipeline exit, with
a one-bool reentrancy barrier.** Four parts:

1. **Store-side collection stays synchronous and untouched.** `check_and_delete_expired`,
   `purge_if_expired` and `purge_expired_hash_fields` keep pushing into the report exactly as
   today. Nothing in `Store`, in `drive_satisfaction_body`, in the BLMove cascade, or in
   `satisfy_waiters` becomes `async`. **No `Box::pin`, no async recursion, no signature churn in
   `blocking.rs`.** This is the whole point of the design and the direct answer to the finding.
2. **`drive_satisfaction`'s trailing drain (`blocking.rs:257`) is deleted**, and its wrapper
   collapses into `drive_satisfaction_body`. Safe *because* of the single-caller fact above:
   anything it purges is inside a pipeline run and is picked up by that run's exit drain. This
   preserves the property the issue-08 fix bought (the report never survives into the *next*
   message, `blocking.rs:241-254`) — the drain simply moves a few frames outward, from inside the
   `WaiterSatisfaction` stage to the end of the enclosing run. The ten `blocking.rs` unit tests
   that call `try_satisfy_*` directly must call the drain explicitly (or through a test helper);
   that is a mechanical, in-file change and is part of the estimate.
3. **`run_write_effects` grows a tail drain guarded by a barrier.** A single
   `ShardWorker.in_effect_run: bool` is set on entry and cleared on exit. After the
   `WRITE_EFFECT_ORDER` loop:

   ```rust
   // sketch — post_execution.rs, end of run_write_effects
   if !was_reentrant {
       self.drain_expiry_report_to_fixpoint(version_policy).await;
   }
   ```

   A nested `run_internal_removal_effects` (reachable only *from* that drain loop) sees the flag
   set and skips its own tail. **Recursion becomes iteration**: the fixpoint loop re-takes the
   report each pass and calls `run_internal_removal_effects` until it is empty, so an expiry
   chain costs loop iterations, not stack frames.
4. **The four outside-the-pipeline sites keep an explicit drain**, now calling the same
   `async fn drain_expiry_report_to_fixpoint`. All four already sit in `async` fns (table above);
   the only signature change required is `purge_expired_watches` (`worker.rs:679`) becoming
   `async fn`, awaited at `execution.rs:554` inside the already-`async` `execute_transaction`.

**Proof obligation the implementation must discharge (not an assertion here).** The fixpoint loop
terminates because each pass physically removes at least one key that was present, and a
physically removed key cannot be re-reported (the exactly-once argument recorded at
`worker.rs:701-711`: exactly-once push in `check_and_delete_expired`'s removal branch, exactly-once
drain via `std::mem::take`; a second purge of an absent key finds nothing). The loop is therefore
bounded by the number of live keys on the shard. **This must ship with a forcing test** — a
chained expiry where removing key A wakes a `BLMOVE` whose destination key B is itself past its
deadline — not with a comment. If that test cannot be written, the barrier must instead be a hard
depth cap with a `tracing::warn!`, mirroring `MAX_BLMOVE_FANOUT_DEPTH` (`blocking.rs:20`, `:277`).

**Rejected alternatives, and why.**

- *Make everything async.* Costs `Box::pin` on the BLMove cascade, an `async` infection through
  `satisfy_waiters` into `post_execution.rs`, and leaves the pipeline genuinely reentrant — the
  nested run would re-enter `VersionIncrement` and `DirtyCounter` mid-outer-run, with no story for
  what a nested `WalPersistence` means relative to the outer `WalPhase`. Strictly worse.
- *Explicit barrier at the blocking seam only* (keep `blocking.rs:257`, guard it with the flag).
  Cannot work: `drive_satisfaction` is sync, so the guarded branch still cannot `await`. The
  barrier has to live where an `await` is legal, which is the pipeline exit.
- *Scope §C down and keep the hand copy for the blocking seam.* Viable fallback if the fixpoint
  test in the proof obligation cannot be made deterministic, but it re-establishes exactly the
  second authority this proposal exists to remove. Present as the retreat position, not the plan.

#### B.3 The `WATCH` no-bump axis — orchestrator ruling requested

The version-bump split established by the earlier proposal must survive, and **rev 1 waved at it**
(one mention, never as an obstacle). It is an obstacle.

`apply_lazy_purge_effects_no_version_bump` (`worker.rs:729`, sole caller `dispatch_core.rs:131`)
exists for a stated reason (`dispatch_core.rs:113-130`): a `WATCH` on an already-expired key must
record a *nonexistent* watch, so the purge it triggers must not bump the version and over-abort
unrelated watchers (F3). Against the pipeline there is no way to express that today:

- `WriteEffectKind::VersionIncrement` is **unconditionally first** in `WRITE_EFFECT_ORDER`
  (`post_execution.rs:283`).
- `EffectScope::InternalRemoval { propagation }` carries **only** `RemovalPropagation { wal,
  replicate }` — no bump-suppression axis (`:241`).
- The one suppression that exists is `summary.dirty_delta >= 0` (`:327-332`), and an internal
  removal's `dirty_delta` is `groups.iter().map(|(_, keys)| keys.len()).sum()` (`:583`), i.e.
  always positive for a non-empty batch. It never fires here.

So routing `dispatch_core.rs:131` through the pipeline as-is **regresses F3**; not routing it
leaves the hand copy alive and **kills §C**. Two options, both buildable, presented for ruling:

| | **Option 1 — widen the scope** | **Option 2 — explicit carve-out** |
|---|---|---|
| Shape | `EffectScope::InternalRemoval { propagation, version: VersionPolicy }` with `VersionPolicy::{Bump, Withhold}`; the `VersionIncrement` arm at `:328-332` gains a third disjunct | `dispatch_core.rs:131` keeps a non-pipeline path: purge, fire only the *non-version* effects by hand, no bump |
| Precedent | Strong — `VersionIncrement` is **already** scope-conditioned (`:324-332` documents "Scope-dependent (intentional — see module docs)"), so this is a new value on an existing axis, not a new kind of knob | None; it is the status quo re-scoped |
| Cost | `EffectScope` grows a field. **Textual conflict with proposal 88**, which also widens `EffectScope` (`:241`) | §C's deletion list shrinks to the non-`WATCH` drain; the "one authority" claim becomes "one authority plus a documented `WATCH` exception" |
| Risk | Puts caller-specific policy into the canonical order — the exact thing `WRITE_EFFECT_ORDER` exists to prevent, mitigated by it being *data on the scope*, not a branch in the caller | The seam this proposal is trying to close stays half-open, and the next effect added to `WRITE_EFFECT_ORDER` still needs a hand-port for `WATCH` |

**Presented, not decided.** Option 1 is the one this proposal would build, on the strength of the
existing scope-conditioned precedent at `:324-332`; Option 2 is the honest fallback and must be
paired with a scoped-down §C. Either way the `VersionPolicy` is a **parameter of the drain call**,
so a fixpoint pass initiated at the `WATCH` seam withholds bumps for the *whole* chain it drains —
matching today's single `bump_version=false` call.

Note the interaction with `bump_versions_for`: today's lazy bump (`worker.rs:836-850`, plus an
earlier shrunk-only bump at `:773-774`) is slot-granular over `purged ∪ emptied ∪ shrunk`, and the
pipeline's `VersionIncrement` (`post_execution.rs:343-348`) is slot-granular over
`record.handler.keys(record.args)` — the same
key set for the removal groups. `shrunk` keys are **not** removals and never enter the pipeline, so
their bump stays where it is (§B.1). That asymmetry must be stated in the adapter's doc comment or
it will read as an omission.

### C. Delete what the authority makes redundant

**Rev 2: narrowed.** Rev 1's list over-claimed — two entries do not follow from §B and one is
weaker than stated. Corrected:

| Deleted / changed | Why it can go | Status |
| --- | --- | --- |
| `worker.rs:738-851` `drain_lazy_purge_effects` body | the pipeline is the ordering | **deleted** (contingent on §B.3 Option 1; under Option 2 the `WATCH` slice survives) |
| four `take_lazily_*` trait methods (`mod.rs:522`-`:573`) | one `take_expiry_report` | **deleted** |
| `blocking.rs:255-258` `drive_satisfaction` wrapper | the drain moves to the pipeline exit (§B.2) | **deleted** — new in rev 2 |
| `lazy_purge_buffers_empty` (`hashmap.rs:511`) | `ExpiryReport::is_empty` | **kept as a rename**, not deleted — see below |
| `event_loop.rs:249-253` `debug_assert!` | — | **WITHDRAWN. Keep it.** |
| `event_loop.rs:255-281` three-buffer hand-discard + comment | — | **simplified, not deleted** |

Three corrections:

- **The `debug_assert!` stays (withdrawn).** Rev 1 argued "one authority ⇒ nothing to assert".
  That is wrong twice over. The assert's own comment (`:240-247`) states what it guards: *no
  `.await` between a command's drain and the sweep arm*. §B.2 adds a drain point at the pipeline
  exit — still inside the same message handling, still before the loop returns to `select!` — so
  the invariant is preserved, but the assert is now guarding a **stronger** claim across **more**
  code. Deleting the only thing that would fail loudly if a future refactor introduced a yield
  point, in the same change that moves the drain, is exactly backwards. It is re-expressed against
  `ExpiryReport::is_empty()` and kept.
- **The sweep's discard does not disappear.** `run_cycle` reaps last-hash-field deaths through the
  *same* `purge_expired_hash_fields` seam a lazy read uses, so it still fills the report while
  `ExpiryResult` separately owns `emptied_keys`/`fields_expired`. The sweep therefore still has to
  take the report and use only `shrunk` from it. Rev 1's "the sweep drains one report" is true;
  "the hand-discard goes away" is not. It becomes one `take_expiry_report()` plus a comment
  explaining ownership, instead of three `take_lazily_*` calls plus a comment — a simplification
  of maybe 15 lines, not a 27-line deletion.
- **`lazy_purge_buffers_empty` becomes `ExpiryReport::is_empty` at the call site**, but the
  *store-side* predicate is still needed by the assert above (the report has not been taken yet at
  that point). It is a rename, not a removal.

Net, honestly recounted: roughly **−110 lines** of shard code (not −150), **+40** for the report
type, **+25** for the fixpoint drain and barrier, and one fewer concept ("lazy purge buffers") in
the store's public surface. The line count is not the case for this change; the single authority
is.

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

### Crash-test design for the durable-effect defect (§5)

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
5. **Assert a `RecordedWalEffect { kind: WalEffectKind::Delete, key: Some(b"k".to_vec()), .. }`
   is present.** Today: absent → test fails. This is the exact, minimal pin on the missing
   tombstone, and it is deterministic (no restart, no timing).

The same fixture with a key-level TTL pins the whole-key case (§3's stale row) — same
assertion, and it fails today for the same reason even though §4 says it does not
resurrect.

**Mechanics the implementation must not rediscover (rev 2):**

- `frogdb-shard-harness` sets `autotests = false` with a single `[[test]] name = "main", path =
  "tests/main.rs"`. **A new test file is invisible until it is registered in `tests/main.rs`** —
  a silent no-op failure mode, not a compile error.
- `RecordedWalEffect.key` is `Option<Vec<u8>>` (`wal/fake.rs:22`), not `Vec<u8>`; the assertion
  must match through the `Option`. Fields are `order`, `kind`, `key`, `seq` (`:18-25`);
  `WalEffectKind` is `{ Set, Merge, Delete, Clear }` (`:29-33`).
- `FakeWalSink` wiring precedents, in ascending order of directness:
  `core/src/shard/builder.rs:392`, `shard-harness/tests/scenario_s6.rs:175`,
  `server/tests/common/invariants.rs:592-602`.
- The harness crate already compiles `frogdb-commands` with `features = ["full"]`
  (`shard-harness/Cargo.toml`), so `HEXPIRE` is available without a feature-flag negotiation.

**Tier 2 — restart round-trip (the user-visible pin).**
Harness: `frogdb-server/crates/server/tests/integration_persistence.rs`, whose Test 3
`test_deleted_keys_stay_deleted_after_restart` `:139` is already the exact shape (write →
shutdown → restart → read), with `persistence_config(data_dir)` `:12` supplying the
durable setup.

**Rev 1's version of this test was a FALSE GREEN and is withdrawn.** It proposed: steps 1–4 over
TCP with `DEBUG SET-ACTIVE-EXPIRE 0`, hard restart, then `EXISTS k` → `0` "**before** the first
active tick can heal it". Three verified facts make that assertion unable to fail:

1. **`DEBUG SET-ACTIVE-EXPIRE 0` does not survive the restart.** It sets
   `ShardWorker.debug_active_expire_disabled` (`worker.rs:188`, written at
   `dispatch_observability.rs:99`, read at `event_loop.rs:236-239`), and the builder initialises
   it to `false` (`builder.rs:471`). It is **process state with no persisted or boot-time
   equivalent** — `grep` over `frogdb-server/crates/config/src` finds no active-expiry knob at all.
   The post-restart server has the sweep **on**.
2. **The first sweep tick fires immediately.** `expiry_interval` is
   `tokio::time::interval(Duration::from_millis(100))` (`event_loop.rs:24`), and a tokio interval's
   first tick completes without delay. There is no 100 ms grace period on a fresh worker.
3. **The sweep arm is ordered ahead of client traffic.** The loop is `tokio::select! { biased; … }`
   (`:57`) with the expiry arm at position 3 and `message_rx` **deliberately last** — the
   fairness comment at `:41-56` says so in as many words.

So the boot sweep re-empties the restored hash and issues the delete *durably* before the client's
first command is ever dequeued. `EXISTS k` returns `0` **against unfixed code**. The test passes
today, proves nothing, and would have shipped as a permanent green.

**Replacement — two options, both of which fail today.** Pick one; do not ship the `EXISTS` form.

- **Tier 2a (preferred, no new production surface): assert on the durable artifact, not on
  post-restart visibility.** After step 4 and *before* any restart, assert the hot-CF row for `k`
  is gone (or, equivalently, that no `Delete` reached the WAL — which is Tier 1's assertion at a
  different level). This is Tier 1's pin promoted to the integration harness and is deterministic.
  The restart is then only a *confirmation* leg, not the assertion.
- **Tier 2b (needs a declared new knob): a boot-time active-expiry disable.** A config parameter
  (`active-expiry-enabled`, default `true`) read by the builder instead of the hardcoded `false`
  at `builder.rs:471`, so the post-restart process starts with the sweep off and the resurrected
  key is genuinely observable. This makes the *user-visible* assertion (`EXISTS k` → `0`) real.
  **It is new production surface and must be declared as such** — a new config parameter goes
  through the config registry (proposal 13's territory) and the live-mutability golden count, and
  is not free. It also has independent operational value (`DEBUG SET-ACTIVE-EXPIRE` exists for
  exactly this need at runtime; there is no boot-time counterpart today).

**Presented, not decided.** Tier 2a is what this proposal would build, because it needs no new
production surface and the duplicate-`del`/double-count residue of §5 is separately assertable in
Tier 1. Tier 2b is the option if the orchestrator wants the user-visible restart assertion.

Deliberately **not** `CrashTestHarness` (`core/src/persistence/test_harness.rs:29`,
consumer `crash_recovery_tests.rs`, 1800 lines): it operates at the RocksDB/WAL level
(`put_with_expiry` `:173`, `crash()` `:209`, `recover()` `:239`) and has no `ShardWorker`,
so it cannot exercise the lazy-purge path at all. It stays the right harness for
`FM-PERSISTENCE-036`-style durable-layer questions; it is the wrong one here. Naming it and
rejecting it is part of the design.

**Standing rule for anything in this area:** a test whose assertion is "the key is absent after a
restart" is testing the boot sweep, not the WAL. Assert on the artifact (WAL effect, CF row) or on
the *duplicate* effect (a second `del` notification for a key already `del`-notified), both of
which the sweep cannot manufacture.

### Mutation-gate consequence

**Rev 2 calibration:** `frogdb-core` is **not** a locked crate. The four locked areas are txn
(`frogdb-txn` + `frogdb-vll`, 0.90), persistence (`frogdb-persistence` + `frogdb-recovery`, 0.85),
replication (0.85) and cluster (0.80). **§A + §B + §C on their own — all of which live in
`frogdb-core` and `frogdb-shard-harness` — incur no mutation gate at all.** The 0.85 gate bites
only if the change reaches `frogdb-persistence/src/recovery.rs`, i.e. only under H2.

That said, the forcing test must still live **in the crate whose code changes** (`cargo mutants -p
<crate>` runs only that package's own tests): the WAL-effect assertion is in
`frogdb-shard-harness`, which exercises `frogdb-core`, so it would contribute nothing to
`frogdb-persistence`'s score if H2 were folded in. If H2 lands, it needs its own in-crate test in
`frogdb-persistence`, and `just mutants-diff frogdb-persistence` before pushing. Without H2, no
`mutants-diff` obligation applies — say so explicitly rather than paying a gate this change does
not owe.

## Spec / LOCKED impact

**This is the gating step. No code lands before it.**

1. **New row `FM-PERSISTENCE-0NN`** in `.scratch/hardening/specs/persistence-failure-modes.md`
   (currently 52 rows; `Status: LOCKED`). Proposed statement: *"a key removed by lazy
   expiry does not survive a crash, and its durable row is reclaimed."* This covers both
   §3 (stale row) and §5 (non-durable removal + duplicate effect); if review prefers, split into
   two rows — the field-emptied case is a correctness break, the stale row is a leak, and they
   have different severities. Note per §5 that the *observable* for the field-emptied case is the
   durable artifact and the duplicate `del`/`expired_keys` count, **not** post-restart `EXISTS` —
   the row's `Observable` and `NOT observable` fields must say so, or the row will license the
   false-green test rev 1 wrote.
2. **Correction to an existing row, same spec-first edit: `FM-PERSISTENCE-044`'s `Trigger`
   (`persistence-failure-modes.md:633`) is factually wrong.** It reads *"Active expiry is
   **sampled**, so a key whose `expires_at` has already passed can still be physically present
   in the store."* FrogDB's active expiry is **not** Redis-style sampling — `ExpiryCycle::run_cycle`
   (`active_expiry.rs:152-233`) drains the `ExpiryIndex` in bounded batches under a time budget.
   The row's *conclusion* survives (a key can still be physically present when the budget is
   exhausted, or between ticks), only its stated mechanism is wrong; the fix is one clause. This
   proposal **plans** the edit and does not make it — the file is `Status: LOCKED` and this is a
   proposal, not an implementation. Folding it into the same spec-first commit is cheap and keeps
   the spec from teaching the next reader the wrong model of the sweep — which is precisely the
   misconception that produced rev 1's Tier-2 test.
3. The row must name its forcing tests. `just lint-failure-modes` (`Justfile:293`, run
   inside `just lint` `:319`) enforces spec↔test agreement in both directions: every
   `FM-<AREA>-NNN` row names forcing tests, every tagged test matches a row. A row without
   tests fails the lint; a test tagged with a nonexistent row fails it too.
4. **The forcing test's *crate* is a hard constraint, not a preference (rev 2).**
   `scripts/failure-modes.py` resolves every name a row cites against a `cargo nextest list`
   over `NEXTEST_CRATES` (`:64-77`), and **`frogdb-shard-harness` is not in that list.** A row
   naming a shard-harness test hits the spec→test branch at `:474-481` and hard-errors:
   *"…names `X`, which no test in <crate list> matches"*. Two consequences:
   - Any commit that lands the row **must extend `NEXTEST_CRATES` with `frogdb-shard-harness` in
     the same commit**, or place the forcing test in a crate already on the list
     (`frogdb-core` and `frogdb-server` both are).
   - This surfaces **only at `just lint` / CI**, never on commit: `lint-gates` (`Justfile:329`)
     does *not* include `lint-failure-modes`. A local `git commit` will look clean.
   The `#[ignore]` axis is *not* a problem — `load_test_paths` runs `cargo nextest list
   --run-ignored all` (`:367`, `:379`) precisely so a nightly-budget test still resolves.
5. Order is **row → failing test → fix**, per `adr/0003` and the locked-area contract in
   `CLAUDE.md`. **The fix is explicitly not an independently-landable one-liner** — and rev 2
   corrects *why*: it is not that `wal: true` is a risky two-token diff, it is that
   **there is no `wal:` field on the lazy path to flip.** The hand copy
   (`worker.rs:738-851`) has no `RemovalPropagation` at all, because it never calls the pipeline.
   Adding the tombstone means either routing through `run_internal_removal_effects` (this
   proposal) or hand-writing a WAL call into the drain (a third authority). There is no
   two-token version. See [H3](#independently-landable-hotfixes).
6. `frogdb-txn` / `frogdb-vll` (gate 0.90) are **not** touched. Lazy expiry runs inside the
   shard worker, below the transaction layer; `EffectScope::Transaction` is untouched. Note that
   §B.3 Option 1 widens `EffectScope`, which the transaction scope inhabits — a compile-visible
   change to a shared enum, but no behaviour change to `EffectScope::Transaction`.

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
| WAL delete on lazy field-emptied key | the removal becomes durable; no duplicate `del` and no double-counted `expired_keys` after a restart | **medium — this is the correctness fix; needs the FM row** |
| Dirty counter bumped on lazy expiry | `rdb_changes_since_last_save` advances; save triggers fire sooner | low — matches Redis and the active sweep |
| Keysizes flush on lazy expiry | `INFO keysizes` histograms stop drifting | low |
| Full waiter satisfaction (not just stream waiters) | list/zset waiters also consulted on lazy removal | **low — strict superset, no loss; see the parity note below** |
| Drain moves from the blocking seam to the pipeline exit (§B.2) | none externally — same effects, same message | **medium — the reentrancy design; needs the fixpoint test** |
| `EffectScope::InternalRemoval` gains a version axis (§B.3 Option 1 only) | none | low — data on an already scope-conditioned effect; textual conflict with 88 |

## Risks / scope boundaries vs siblings

**Proposal 88 (PN12, blocking-serve wake effects) — COMMITTED, not hypothetical (rev 2).**
Rev 1 cited 88 as a proposal that might exist. It does:
`.scratch/arch-deepening/proposals/88-served-wake-effects.md`, committed `eb8760e9`
("arch-deepening: author proposal 88 (served-wake effects, PN12)"). It has been read.

88 documents the **fourth** write-effect authority — the blocking-serve path in `blocking.rs`,
which hand-applies a three-effect subset of `WRITE_EFFECT_ORDER` — and its worst finding is
acknowledged-write loss on `BRPOPLPUSH` (a crash after a served wake loses the element from both
source and destination, because `dest` is not in the waking write's declared key set). Its design
is `EffectScope::ServedWake` plus `run_served_wake_effects`, walking `WRITE_EFFECT_ORDER` and
deciding **per effect** one of three dispositions: *apply now*, *union into the outer run's pending
key set*, or *skip with a stated reason*.

**The ordering ruling is already recorded on both sides.** 88's header states "83 lands first
(orchestrator ruling)" and its §"Sibling 83" (`:455-478`) says it is "written assuming 83's
`ExpiryReport` routing is already merged". 83 only *uses* the interface; 88 *widens* it.

Textual conflict surface in `post_execution.rs`, agreed by both documents: the `EffectScope` enum
`:211-242` (its `InternalRemoval` variant at `:241`), the per-effect `match scope` arms
`:327`/`:354-358`/`:391-392`/`:425-426`/`:451-454`, and the
order-validation tests `:736-866`. **Rev 2 adds one 88 did not know about:** if §B.3 **Option 1**
is ruled, 83 *also* widens `EffectScope` (adding a `version` field to `InternalRemoval`), so the
two changes touch the same enum in the same commit range rather than 83 merely using it. Under
Option 2, 83 does not touch the enum and the original clean layering holds. **This is a reason the
§B.3 ruling matters beyond 83.**

**Design interplay worth exploiting.** 88's three-way per-effect disposition (`ApplyNow` /
`UnionIntoOuter` / `Skip`) and §B.2's barrier-plus-fixpoint are two solutions to *the same
problem*: an inner mutation discovered while the outer pipeline is mid-run. §B.2's
"union into the outer run" is exactly 88's `UnionIntoOuter`, reached by a different mechanism
(a shared report the outer loop re-drains, rather than a threaded key set). Whoever lands second
should consider collapsing the two into one mechanism rather than shipping both; that is a
follow-up, not a precondition.

**The shared test.** 88 names `expiry_triggered_wake_of_blmove_stages_destination_write` as the
test that sits on both seams — a lazily-expired key whose removal wakes a `BLMOVE` that stages a
destination write. It does **not** exist in the tree yet (grep: the only occurrence is 88's own
text at `:473`). 88's ruling stands: **whoever lands second owns it.** For 83 specifically, this is
very close to the fixpoint-termination forcing test §B.2 already requires — if 83 lands first, 83
writes it, and 88 inherits it.

**Three further file-level overlaps, declared (rev 2). Rev 1 declared none of them.**

| Sibling | Status | Overlapping region | Interaction |
|---|---|---|---|
| **81 — core dead seams** (`81-core-dead-seams.md`, committed `f73bdd8f`) | proposal, unimplemented | **`event_loop.rs`** (deletes the `NewConnection` `select!` arm `:119-124`, edits the fairness comment `:48`, touches test scaffolding `:632`/`:637`/`:1169`/`:1176`) and **`worker.rs`** (`use` `:24`, field `:117`, all four constructors `:385`/`:402`/`:434`/`:463`, test `:988`) | **Two of 83's three primary files.** The regions are disjoint in *lines* — 81 works in the constructors and the connection arm, 83 in `drain_lazy_purge_effects` `:738-851` and the expiry arm — but both edit the `select!` block and both edit `worker.rs`'s method set, so **merge conflicts are near-certain if they land concurrently**. 88 already records "81 H1 lands first (one-way)". 83 should state the same: **81 first, or explicitly serialize.** 83 additionally *cites* the fairness comment `:41-56` as load-bearing evidence for the Tier-2 false green, and 81 rewrites it — whoever lands second must re-verify that citation. |
| **66 — shard-worker builder** (`66-shard-worker-builder.md`) | proposal, unimplemented | **`worker.rs:385-493`** — the four convenience constructors, plus the 25 `pub` setters at `:231`–`:596` | 83 does not touch `:385-493`. But 83 *does* change `purge_expired_watches` (`:679`) to `async fn` and deletes `:738-851`, both inside the same file 66 restructures wholesale. **Disjoint by line, colliding by file.** 66 is the larger change; if 66 is scheduled, 83 should land first (it is a narrower edit) or rebase. |
| **39 — recovery replay driver** (`39-recovery-replay-driver.md`) | proposal, unimplemented | **`persistence/src/recovery.rs`**, explicitly including `recover_shard_into` `:138-194` | **This is the exact region of 83's §3 evidence (`:150-158`) *and* of hotfix H2.** 39 moves the whole-database driver into this module and re-exports it from `lib.rs`. 83's §3 only *reads* `:150-158` — no conflict. **H2, which would edit it, is a direct conflict** and is the reason H2 is deferred rather than approved (see [Hotfixes](#independently-landable-hotfixes)). |

None of the three changes 83's *analysis*; all three change its *merge order*. Declared here so
the orchestrator sequences rather than discovers.

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

**Parity note: the `WaiterSatisfaction` row is a strict superset, and NOGROUP is not lost
(rev 2).** The `WRITE_EFFECT_ORDER` table in §1 marks the hand copy "**partial** — stream waiters
only", which invites the reading that routing through the pipeline *changes* stream-waiter
behaviour. It does not. Today `drain_lazy_purge_effects` calls `drain_stream_waiters_with_error`
directly. Through the pipeline, the synthetic `DEL` handler's spec carries `WaiterWake::All`
(documented at `post_execution.rs:578-579`: "waiter-wake (`All` → stream NOGROUP drain)"), so
`satisfy_waiters_for_command` `:701` fans out to List, SortedSet
**and** Stream; the Stream leg reaches `StreamSatisfaction::check_key` `:1077`, which returns
`KeyReady::DrainNoGroup` for a missing key (`blocking.rs:1083`) and the driver then calls
`drain_stream_waiters_with_error` (`:295-296`) — **identical outcome, reached through the
authority.** The delta is purely additive: list and zset waiters on a lazily-removed key are now
consulted too, which is what the active sweep already does. This is **parity hygiene, not a
defect**, and the proposal should not be read as claiming blocked list clients are currently
mis-served on lazy expiry.

**Risk: `debug_assert` removal — WITHDRAWN (rev 2).** Rev 1 proposed deleting
`event_loop.rs:249-253` on the grounds that one authority leaves nothing to assert. That is
withdrawn; see §C. The assert guards "no `.await` between a command's drain and the sweep arm",
which §B.2 makes *more* important, not less, because the drain point moves. It is kept and
re-expressed against `ExpiryReport::is_empty()`. The reviewer's underlying instinct still applies
in the other direction: **a surviving second drain site is a failed implementation of this
proposal** — but the assert is the thing that would catch it, so it stays.

**Risk: the reentrancy design is the real risk in this change, not the WAL delete.** The tombstone
is a one-line consequence of routing; the routing requires moving a drain point that four seams
and one recursive driver depend on. If the fixpoint-termination test of §B.2 cannot be made
deterministic, the honest outcome is the scoped-down variant (keep the blocking-seam hand copy),
not a shipped fixpoint loop with a comment where the proof should be.

## Effort

| Piece | Size | Note |
| --- | --- | --- |
| New `FM-PERSISTENCE` row(s) + `FM-PERSISTENCE-044` `Trigger` fix + `NEXTEST_CRATES` amendment + `just lint-failure-modes` green | S | gating; must be first. **The `NEXTEST_CRATES` line is part of this commit, not a follow-up** |
| Tier-1 failing test (shard-harness + `FakeWalSink`, registered in `tests/main.rs`) | S | copy `eviction_spill_failure.rs` |
| Tier-2a durable-artifact test (`integration_persistence.rs`) | S | shape from Test 3 `:139`; **not** the `EXISTS`-after-restart form |
| Tier-2b boot-time active-expiry config knob (only if ruled) | S–M | new production config surface; registry + golden count |
| **§B.2 reentrancy design** (barrier + fixpoint drain, delete `blocking.rs:255-258`, `purge_expired_watches` → `async`, fix ~10 in-file unit tests) | **M** | **new in rev 2; the actual hard part** |
| §B.2 fixpoint-termination forcing test (chained expiry through a `BLMOVE` wake) | S–M | shared with 88; whoever lands second inherits it |
| §B.1 routing (report → `run_internal_removal_effects`) | S–M | mechanical once §B.2 exists |
| §B.3 `WATCH` no-bump axis (Option 1: widen `EffectScope`) | S | plus a golden/order-validation test touch; Option 2 is smaller but keeps the hand copy |
| §A `ExpiryReport` + trait collapse (5 methods → 1) | S–M | touches every `Store` impl |
| §C deletions (hand copy, trait methods, blocking wrapper) | S | falls out of §B — **narrower than rev 1 claimed** |
| `just mutants-diff` | — | **not owed** unless H2 folds in (`frogdb-core` is unlocked) |

**Overall: M, at the upper end** (rev 1 said M on a scope that did not include the reentrancy
design). Not L — the pipeline exists, has a compliant caller to copy, and the single-caller fact
about `try_satisfy_*` keeps the blast radius inside three files. Not S — it is spec-first in a
locked-adjacent area, it moves a drain point four seams depend on, it needs a termination proof
with a test behind it, and it carries a cross-crate trait change plus an orchestrator ruling
(§B.3) that changes the shape of the result.

### Independently-landable hotfixes

Rulings from the adversarial review are recorded inline.

- **H1 — Tier-1 WAL-effect test, `#[ignore]`d. APPROVED, with a mandatory amendment.**
  Landing the *failing* test first, ignored, with the FM row, is the spec-first opening move and
  is independently landable. Nothing else here is.

  **Mandatory amendment (rev 2): the same commit must add `"frogdb-shard-harness"` to
  `NEXTEST_CRATES` (`scripts/failure-modes.py:64-77`)** — or place the forcing test in
  `frogdb-core`/`frogdb-server` instead. Without it, `check`'s spec→test branch (`:474-481`)
  cannot resolve the test name and `just lint-failure-modes` hard-fails. This does **not** show up
  on commit (`lint-gates`, `Justfile:329`, excludes `lint-failure-modes`), only at `just lint` and
  in CI — i.e. H1 as rev 1 wrote it would have passed review, passed the pre-commit hook, and
  broken CI. The `#[ignore]` itself is fine: `load_test_paths` lists with `--run-ignored all`
  (`:367`, `:379`).

  Adding a crate to `NEXTEST_CRATES` costs one `cargo nextest list` compile of the shard-harness
  test binary on every `lint-failure-modes` run; the crate is small and already in the graph.
  State that cost in the commit message rather than letting the next person discover it.

- **H2 — recovery hot/warm asymmetry. DEFERRED, not rejected (rev 2 changes this verdict).**
  `recovery.rs:150-158` skips where `:214-219` deletes; the asymmetry is real and undocumented,
  and a `delete_hot` to match the warm tier is a **grounded** fix, not a speculative one.

  Two things defer it:
  1. **It edits proposal 39's region.** 39 (`39-recovery-replay-driver.md`) explicitly owns
     `recover_shard_into` `:138-194` — the exact function. Landing a durable-behaviour change
     inside a function another proposal is restructuring is the wrong order.
  2. **It pulls the `frogdb-persistence` 0.85 mutation gate into a change that otherwise owes
     nothing** (see [Mutation-gate consequence](#mutation-gate-consequence)). §A/§B/§C are entirely
     in unlocked crates; H2 alone changes that, and it needs its own in-crate forcing test to
     contribute to the score.

  One correction to the grounding: the API precedent is **`delete_warm` (`rocks/columns.rs:127`)**,
  a production method. `RocksStore::delete` (`rocks/mod.rs:422`) is
  `#[cfg(any(test, feature = "test-support"))]` with **no production caller** ("production deletes
  flow through the WAL batch path"), so H2 would need a new production `delete_hot` mirroring
  `delete_warm` — a slightly larger change than "call the existing method".

  **Two options for the orchestrator, presented not decided:** *(a)* sequence H2 behind 39 and
  keep 83 gate-free; *(b)* take the 0.85 gate cost explicitly now, with an in-crate
  `frogdb-persistence` test and `just mutants-diff frogdb-persistence` before push, and accept the
  merge conflict with 39. The comment-only variant (record *why* the tiers differ, change no
  behaviour) remains free under either option and should land regardless.

- **H3 — NOT a hotfix: adding the WAL delete. Rejection stands; the premise was wrong (rev 2).**
  Rev 1 called it "a two-token diff" (`wal: true`) and rejected it for being a locked-area
  drive-by. The rejection is right, the reason is not: **there is no `wal:` field on the lazy path
  to flip.** `RemovalPropagation` is a *pipeline* concept; `drain_lazy_purge_effects`
  (`worker.rs:738-851`) never calls the pipeline and carries no propagation at all. Getting a
  tombstone onto the lazy path means either routing through `run_internal_removal_effects` — which
  is this entire proposal, §B.2 reentrancy design included — or hand-writing a WAL call into the
  drain, which creates a *third* authority. **It is H1-then-the-proposal, or nothing.** Stating it
  as a two-token diff made the change look temptingly small; it is not small, and the corrected
  premise is a stronger argument against the drive-by than the original one.

**Security:** no security-relevant findings in this candidate. Per standing policy,
security items are **filed but parked — record only**; nothing to record here.

## Revision ledger

Revision 2, against the adversarial review of revision 1 (verdict **AMEND**: 4 blocking, 8
non-blocking, 3 hotfix rulings). Every correction below was re-derived against the working tree at
`eb8760e9` before being applied; the two that did not verify are recorded as refuted with evidence.

### Applied

| # | Finding | What changed | Verified against |
|---|---|---|---|
| **B1** | §B/§C made the effect pipeline reentrant into itself via sync recursive code | **§B split into B.1/B.2/B.3.** New §B.2 documents the full reentrancy path, enumerates all five production drain sites with their `async`-ness, and commits to a design: **sync collection + async drain-to-fixpoint at the outermost pipeline exit + a one-bool barrier**, with `blocking.rs:255-258` deleted rather than made async. Rejected alternatives stated. Termination made an explicit proof obligation with a forcing test | `post_execution.rs:561` (`async fn`), `:377`→`:701`→`:717`→`:721-723`; `blocking.rs:255-258`, `:271-277`, `:374`, `:1079-1083`; the five call sites `execution.rs:113`/`:733`, `worker.rs:686`→`execution.rs:554`, `dispatch_core.rs:131`, `blocking.rs:257`. **New evidence found during verification:** `try_satisfy_*` has exactly **one** non-test caller (`post_execution.rs:721-723`) — this is what makes the chosen design work and rev 1 did not know it |
| **B2** | `no_version_bump` has no pipeline equivalent | **New §B.3.** Two options tabled (widen `EffectScope` with a `VersionPolicy` axis / explicit carve-out with a scoped-down §C), orchestrator ruling requested, `present-don't-decide` | `dispatch_core.rs:113-131`; `WRITE_EFFECT_ORDER[0]` `post_execution.rs:283`; `EffectScope::InternalRemoval` `:241` (enum `:211-242`); the `dirty_delta >= 0` suppression `:327-332`; `dirty_delta = Σ keys.len()` `:583`. **Strengthening found:** `VersionIncrement` is *already* scope-conditioned and documents itself as such (`:323-332`), so Option 1 is a new value on an existing axis, not a new kind of knob |
| **B3** | H1's FM row breaks `just lint` | H1 amended with a **mandatory same-commit `NEXTEST_CRATES` extension**; new item 4 in [Spec / LOCKED impact](#spec--locked-impact) explaining the failure mode and why it escapes the pre-commit hook | `scripts/failure-modes.py:64-77` (no `frogdb-shard-harness`), `check` spec→test `:474-481`, `load_test_paths --run-ignored all` `:367`/`:379`; `Justfile:329` (`lint-gates` excludes it), `:319` (`lint` includes it) |
| **B4** | Tier-2 restart test is a false green | **Tier 2 rewritten.** Rev 1's `EXISTS`-after-restart form withdrawn with a three-point proof that it cannot fail; replaced by **Tier 2a** (assert the durable artifact) preferred, **Tier 2b** (declare a boot-time active-expiry config knob) presented as the option if the user-visible assertion is wanted. Standing rule added | `debug_active_expire_disabled` is process state (`worker.rs:188`, `builder.rs:471`, `dispatch_observability.rs:99`, `event_loop.rs:236-239`); no config knob (grep over `config/src`); `tokio::time::interval` first-tick-immediate (`event_loop.rs:24`); `biased;` with the expiry arm at position 3 and `message_rx` last (`:41-57`) |
| **N1** | §3's "no WAL delete on the lazy path" too broad | §3 narrowed with the exact mechanism: `DeleteIfMissing`/`PersistOrDelete` **do** emit an incidental tombstone; the leak is precisely read-triggered purges + `PersistFirstKey` writers | `shard/persistence.rs:105-133`, `write_set` `:143-154`; `command.rs:360-390`, `:444`, `:664` |
| **N3** | Proposal 88 exists and is committed | 88 section rewritten: cited as committed `eb8760e9`, read, its ruling and conflict surface quoted; **new conflict identified** (under §B.3 Option 1, 83 also widens `EffectScope`); design interplay noted (`UnionIntoOuter` ≈ §B.2's fixpoint union); shared test ownership recorded | `88-served-wake-effects.md:7`, `:199-202`, `:260-261`, `:455-478`, `:473`; commit `eb8760e9` |
| **N4** | Three undeclared overlaps | New overlap table declaring **81** (`f73bdd8f`, edits `event_loop.rs` + `worker.rs`), **66** (`worker.rs:385-493`), **39** (`recovery.rs` `recover_shard_into:138-194`) with the interaction for each | `81-core-dead-seams.md:65`/`:67`/`:143`; `66-shard-worker-builder.md:73`; `39-recovery-replay-driver.md:44`; `recovery.rs:138-194` |
| **N5** | `WaiterSatisfaction` parity | New parity note: §B is a **strict superset**, NOGROUP is not lost, the delta is additive list/zset consultation — hygiene, not a defect | `post_execution.rs:578-579` (`WaiterWake::All`), `:701`, `:717`; `blocking.rs:1077-1083`, `:295-296` |
| **N6** | Mutation-gate over-claim | Section recalibrated: `frogdb-core` is **not** locked; §A+§B+§C owe **no** gate; the 0.85 gate applies only under H2 | `CLAUDE.md` locked-area list; `Justfile:278-279` |
| **N7** | `FM-PERSISTENCE-044` `Trigger` is wrong | Added as item 2 of the planned spec-first edit — *planned, not made* (the spec is `Status: LOCKED` and this is a proposal) | `persistence-failure-modes.md:633` ("Active expiry is sampled") vs `active_expiry.rs:152-233` (index-driven, budgeted) |
| **N9** | Tier-1 mechanics | Added: `tests/main.rs` registration (crate is `autotests = false`), `RecordedWalEffect.key: Option<Vec<u8>>`, the three `FakeWalSink` precedents, `frogdb-commands = ["full"]` already on | `shard-harness/Cargo.toml`; `wal/fake.rs:18-33`; `builder.rs:392`, `scenario_s6.rs:175`, `invariants.rs:592-602` |
| **cite** | `collections.rs` line count | 258 → **257** | `wc -l` |
| **H1** | Ruling | Recorded **APPROVE with mandatory amendment** | — |
| **H2** | Ruling | Recorded **DEFER, not reject**, with both orchestrator options (sequence behind 39 / take the gate cost) and a correction to the grounding | `rocks/mod.rs:415-422` is `#[cfg(any(test, feature = "test-support"))]`, no production caller; the production precedent is `delete_warm` (`rocks/columns.rs:127`) |
| **H3** | Ruling | Rejection stands; **premise corrected** — there is no `wal:` field on the lazy path, so it is not a two-token diff at all | `worker.rs:738-851` carries no `RemovalPropagation` |

### Refuted

| # | Claim (rev 1's, or the review's) | Evidence |
|---|---|---|
| **R1** | Rev 1 §5: "the window is live but transient (~100 ms + budget)", observable by `DBSIZE`/`SCAN`/a subscriber | **Refuted.** The boot sweep runs on the shard's **first** loop iteration — `tokio::time::interval`'s first tick completes immediately (`event_loop.rs:24`) and the `select!` is `biased;` with the expiry arm ahead of `message_rx`, which the fairness comment (`:41-56`) states is deliberately last. No client command can be dequeued before the heal. The 100 ms figure was arithmetic on a cadence that does not gate the first cycle. §5 rewritten around the durable/duplicate-effect defect, which *is* real |
| **R2** | Rev 1 §5 (implied) + review's phrasing: the field-emptied key "escalates to a permanent leak" like §3 | **Refuted.** Recovery restores the field-TTL row (`expires_at` is `None`, so the `:150-158` filter does not apply) and `install` (`hashmap.rs:389-396`) **re-derives the field-expiry index** from the restored value, so the index-driven sweep (`active_expiry.rs:152-233`) reclaims it. Path verified end to end: `store_recovery.rs:41-49` → `hashmap.rs:266` → `:328` → `:367`. The §3 whole-key case *is* permanent, for the opposite reason: recovery `continue`s past it, so it is never restored and never re-indexed |
| **R3** | Review: H2 is grounded in `RocksStore::delete` (`rocks/mod.rs:422`) | **Partially refuted.** That method is `#[cfg(any(test, feature = "test-support"))]` and its own doc says "no production caller; production deletes flow through the WAL batch path". H2 needs a *new* production `delete_hot` mirroring `delete_warm` (`rocks/columns.rs:127`). H2 remains grounded and deferred — it is just slightly larger than the citation suggests |

### Withdrawn

| # | Withdrawn | Why |
|---|---|---|
| **W1** | §C's deletion of `event_loop.rs:249-253` (`debug_assert!` on empty lazy buffers) | The assert guards "no `.await` between a command's drain and the sweep arm" (`:240-247`). §B.2 **moves the drain point**, which makes the invariant *more* fragile, not less. Deleting the only loud failure for a second drain site, in the same change that relocates the drain, is backwards. Kept, re-expressed against `ExpiryReport::is_empty()` |
| **W2** | §C's deletion of `event_loop.rs:255-281` (three-buffer hand-discard) | Downgraded from "deleted" to "simplified". `run_cycle` still fills the report through the shared `purge_expired_hash_fields` seam while `ExpiryResult` separately owns `emptied_keys`/`fields_expired`, so the sweep still takes the report and uses only `shrunk`. Three `take_lazily_*` calls become one `take_expiry_report()` — ~15 lines, not 27 |
| **W3** | §C's deletion of `lazy_purge_buffers_empty` (`hashmap.rs:511`) | Downgraded to a rename. The store-side predicate is still needed by W1's assert, which runs before the report is taken |
| **W4** | Rev 1's "−150 lines" net and its **M** effort estimate at that scope | Recounted to ~−110 net, and the estimate re-stated as **M at the upper end** now that §B.2's reentrancy work, its forcing test, and the `blocking.rs` unit-test fixups are inside the scope |
| **W5** | Rev 1's Tier-2 test (`EXISTS k` → `0` after restart) | See **B4**. It passes against unfixed code |
| **W6** | Rev 1's framing of H3 as "a two-token diff" | See **H3**. There is no such diff; the framing made the change look landable-in-passing |

### Not drifted

Every line number rev 1 cited in `worker.rs`, `post_execution.rs`, `event_loop.rs`, `hashmap.rs`,
`active_expiry.rs`, `recovery.rs`, `store_recovery.rs` and the FM spec was re-checked and is still
correct at `eb8760e9`, with the single exception of `collections.rs`'s file length (258 → 257).
`EffectScope`'s citation is refined from `:241` (the `InternalRemoval` variant, correct) to
`:211-242` where the whole enum is meant.

## References

- `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md` — prior round;
  established the drain seam and `WATCH` bump split this proposal preserves
- `.scratch/testing-improvements-round2/issues/open/54-bcast-trackers-never-invalidated-on-lazy-expiry.md`
- `.scratch/testing-improvements-round2/issues/open/22-expiry-not-checked-before-reads.md`
- `adr/0003` — persistence boundary
- `.scratch/arch-deepening/proposals/88-served-wake-effects.md` (PN12, committed `eb8760e9`) —
  the fourth write-effect authority; shares the `WRITE_EFFECT_ORDER` seam, records "83 lands
  first", and its `UnionIntoOuter` disposition is the same idea as §B.2's fixpoint union
- `.scratch/arch-deepening/proposals/81-core-dead-seams.md` (committed `f73bdd8f`) — edits
  `event_loop.rs` and `worker.rs`; merge-order overlap
- `.scratch/arch-deepening/proposals/66-shard-worker-builder.md` — restructures `worker.rs`
- `.scratch/arch-deepening/proposals/39-recovery-replay-driver.md` — owns
  `recovery.rs:recover_shard_into`; the reason H2 is deferred
