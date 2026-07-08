# PFADD / HyperLogLog Write-Amplification — Tier 1 + Tier 2 Plan

## Problem (measured)

PFADD persists the full HLL value to RocksDB on **every** call via `WalStrategy::PersistFirstKey`.
For a **dense** HLL that's **12 288 bytes per PFADD** regardless of how few registers changed.

Measured (debug, in-process harness — mirrors the flaky `tcl_pfcount_multiple_keys_*` test):

| | per-cmd |
|---|---|
| PING (round-trip only) | 0.09 ms |
| SET (small value + WAL) | 0.25 ms |
| **PFADD (100 elems, dense key)** | **27 ms** |
| PFADD pipelined | 26.5 ms (no help → server-side cost) |

Confirmed **not** VLL (PFADD/PFCOUNT are `Standard`, keys same-shard) and **not** the network
(replication is command-based — `broadcast_command` sends `PFADD key elem`, not the value). The cost
is **WAL/disk value persistence of a 12KB dense HLL**.

Already-good: HLL has **sparse encoding** (`hyperloglog.rs`, `SPARSE_TO_DENSE_THRESHOLD = 3000`), so
low-cardinality keys already persist small. The amplification is specific to **dense (>3000-card)
keys**.

Two independent fixes. Tier 1 is small + also a correctness alignment with Redis. Tier 2 removes the
amplification for genuine high-distinct-rate dense keys (needs a persistence-format change — OK,
pre-production).

---

## Tier 1 — Suppress write effects on a no-op PFADD (small; also Redis-parity)

### Rationale
PFADD already computes whether it changed anything (`HyperLogLogValue::add` returns `changed`,
aggregated in `commands/src/hyperloglog.rs` as `any_changed`) — then **throws it away**. The value is
persisted (12KB), broadcast, version-bumped, and keyspace-notified even when **nothing changed**. In
the classic HLL workload (counting uniques in a high-cardinality stream) the steady state is mostly
**duplicate** adds → `changed == false` → today every one pays 12KB for nothing.

Redis **skips propagation/notification/dirty** when PFADD doesn't modify the HLL. So suppressing
effects on a no-op is **both** the perf fix **and** a correctness alignment (a no-op PFADD must not
trip WATCH, emit a keyspace event, or replicate).

### Design
Add a per-command "this write changed nothing → skip write effects" signal, and gate
`run_write_effects` on it. Generic (other commands — SETNX-existing, SADD-dup, SREM-missing,
HSETNX-existing — can opt in later), but scope the first cut to PFADD (+ PFMERGE).

**`CommandContext` gains a flag** (`core/src/command.rs`):
```rust
pub struct CommandContext<'a> {
    // ...
    /// A write command sets this `true` to declare it made NO change, so the
    /// shard skips the entire write-effect pipeline (WAL persist, replication
    /// broadcast, version bump, keyspace notify, tracking invalidation).
    /// Default `false`. Matches Redis, which does not propagate a no-op write.
    pub write_was_noop: bool,
}
```

**PFADD sets it** (`commands/src/hyperloglog.rs`):
```rust
// after computing `changed`:
if !changed {
    ctx.write_was_noop = true;          // NEW: no registers moved → suppress effects
}
Ok(Response::Integer(if changed { 1 } else { 0 }))
```

**The shard gates the pipeline** (`core/src/shard/execution.rs`, where `is_write` currently drives
`run_write_effects`):
```rust
// BEFORE
if is_write {
    self.run_write_effects(summary, wal_phase, EffectScope::Command).await;
}
// AFTER
if is_write && !ctx.write_was_noop {
    self.run_write_effects(summary, wal_phase, EffectScope::Command).await;
}
```
(Reset `write_was_noop` per command; it lives on the per-command `CommandContext`.)

### Files
- `core/src/command.rs` (field + reset), `core/src/shard/execution.rs` (the two/three
  `run_write_effects` call sites — command + the blocking/timeseries paths that reuse it),
  `commands/src/hyperloglog.rs` (PFADD, and PFMERGE when the destination is unchanged).

### Correctness / tests
- A no-op PFADD (all-duplicate elems) → returns `0`, **no** WAL write, **no** replication frame, **no**
  `pfadd` keyspace event, **no** version bump (WATCH not tripped). Add tests for each (assert replica
  receives nothing; assert `WATCH k; <dup PFADD>; MULTI; …; EXEC` still succeeds).
- A changing PFADD is unaffected (full effects as today).
- Regression: existing HLL + keyspace-notification + replication suites.

### Estimate
**~1–2 days.** Small, contained, correctness-positive. Does **not** speed up all-*distinct* workloads
(the flaky test's worst case) — so the test still needs its own fix (larger PFADD batches + a longer
per-test nextest timeout), tracked separately.

---

## Tier 2 — Delta persistence for dense HLL via a RocksDB merge operator (medium; format change)

### Rationale
Tier 1 removes no-op writes. A genuine PFADD that sets a few new registers on a **dense** key still
persists 12KB. Fix: **persist only the changed registers as a delta**, and let RocksDB combine them.
Register update is `max` per register — **associative + commutative** — the textbook fit for a
**RocksDB merge operator**. This keeps FrogDB's **value-restore recovery model** (recovery reads the
merged value; no command replay needed).

### The persistence-format change (allowed — pre-production)
Today `persist_by_strategy` issues a RocksDB **`Put(serialize(value))`** — full 12KB for HLL. New:
- HLL registers are stored/merged via a **merge operator** registered on the store CF.
- PFADD persists a **`Merge(hll_delta_operand)`** carrying only the `(index, new_value)` pairs that
  increased — typically tens of bytes.
- RocksDB folds operands into the base on read/compaction (`base_registers[i] = max(base, operand)`).
- Recovery / PFCOUNT read the fully-merged value and `deserialize` it as today.

**Operand format** (new; extends the existing `encoding_byte || data` scheme in
`types/src/hyperloglog.rs:314`):
```
encoding_byte:
  0 = sparse full value        (existing)
  1 = dense full value         (existing)
  2 = DELTA operand (NEW): num_pairs(u32) || (index u16 || value u8)*   // register maxes to apply
```
The merge operator: start from the base (sparse or dense full value, byte 0/1), apply each `2`-delta
operand as register-wise max, emit the merged full value. (Promotes sparse→dense past the threshold,
same rule as `HyperLogLogValue`.)

### Type-side support
`add()` must report *which* registers changed so PFADD can build the delta. Add a variant that
collects changes:
```rust
// BEFORE
pub fn add(&mut self, element: &[u8]) -> bool { /* returns changed */ }
// AFTER (add alongside; keep `add` as a thin wrapper)
/// Returns the (index, new_value) if this element raised a register, else None.
pub fn add_tracked(&mut self, element: &[u8]) -> Option<(u16, u8)> { /* ... */ }
```
PFADD collects the `Some(..)` pairs across its elements → that's the delta operand.

### Persist-path routing
`persist_by_strategy` (or a new `WalStrategy::MergeDelta` / a per-value hook) must, for an HLL write,
emit a `Merge` with the delta operand instead of `Put(full)`. Cleanest: a `WalAction::Merge(key,
operand)` variant + a `WalStrategy` that PFADD/PFMERGE use, resolved in `Command::wal_actions`.
The RocksStore write path (`persistence` crate) issues `db.merge_cf(cf, key, operand)`; the CF is
created with the HLL merge operator.

### Files
- `types/src/hyperloglog.rs` — `add_tracked`, delta-operand serialize/deserialize (`encoding_byte 2`),
  a `merge(base, operands) -> merged` free fn used by the operator.
- `persistence/src/` (RocksStore setup) — register the HLL merge operator on the CF; `merge_cf` write
  path; recovery unchanged (reads merged value).
- `core/src/command.rs` — `WalAction::Merge` + a `WalStrategy` for HLL (or a dynamic `wal_actions`
  hook on PFADD/PFMERGE).
- `core/src/shard/post_execution.rs` — `WalPersistence` effect handles the `Merge` action.
- `commands/src/hyperloglog.rs` — PFADD/PFMERGE build + attach the delta.

### Open decisions
1. **Merge operator scope:** dedicated HLL column family vs a type-tagged merge operator on the main
   CF (dispatch by the operand's `encoding_byte`; full-value Puts still replace). Dedicated CF is
   cleaner but means HLL keys live in their own CF (routing + recovery iterate it). Recommend the
   **type-tagged operator on the existing value CF** to avoid a CF split, since only HLL uses merge.
2. **PFMERGE / PFCOUNT-with-cache:** PFMERGE writes a full dense result → a `Put(full)` (byte 1) is
   correct (no delta). PFCOUNT is read-only. GETDEL/DEL of an HLL → normal delete (merge operands for
   a deleted key are dropped by a delete tombstone — verify RocksDB delete-vs-merge ordering).
3. **Warm-tier / snapshot:** the epoch snapshot + warm-tier demotion read the merged value (fine).
   Confirm snapshot serialization writes the full value (byte 1), not operands.
4. **Compaction bound:** operands accumulate between compactions; a hot key could stack many operands
   before RocksDB compacts. Acceptable (read-time merge is cheap register-max), but a periodic
   full-value rewrite (e.g. on encoding promotion or every N operands) bounds read cost — optional.

### Correctness / tests
- Round-trip through `persistence` recovery: base + N delta operands → merged HLL equals the
  in-memory HLL (extend the existing `recovery.rs` round-trip test with merge operands).
- Crash-recovery: kill mid-stream, recover, PFCOUNT within error bound.
- Delete then re-add; PFMERGE over delta-persisted sources; sparse→dense promotion via operands.
- Cardinality accuracy unchanged (the merge is exact register-max — lossless).

### Estimate
**~1 week.** Medium. The RocksDB merge-operator plumbing + delta operand format + persist routing +
crash/recovery tests are the bulk. Fully removes the amplification (a few-register PFADD persists
~tens of bytes instead of 12KB) for all dense-key workloads.

---

## Sequencing

1. **Tier 1 first** (independent, ~1–2 days) — ships the correctness fix + kills amplification for the
   common duplicate-heavy workload.
2. **Test fix for item 6** alongside Tier 1 (bigger PFADD batches + nextest timeout) — de-flakes the
   regression test (all-distinct worst case, which neither tier fully speeds up in debug).
3. **Tier 2** (~1 week) — for high-distinct-rate dense-key durability. Composes cleanly on Tier 1
   (Tier 1 already elided the no-op writes; Tier 2 shrinks the remaining real ones).

They compose: Tier 1 removes writes that shouldn't happen; Tier 2 shrinks the writes that should.
