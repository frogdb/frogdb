# Failure of a derived structure is reported as success — no conservation invariant exists anywhere

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T2
Score: aggregate of 6 findings
Area: frogdb-core / store + eviction · frogdb-search · frogdb-persistence · crates/testing

## Context

Six findings across four areas share one shape: a *derived* structure (warm tier, search index,
shard contents, encoded sketch) fails to be written or read, and the code path returns `Ok` /
`Recovered` / a `nil` that is indistinguishable from absence. Nothing anywhere in the suite asserts
that a derived structure still agrees with its source.

This is **one piece of work, not six**. The search proposal names the general fix: a single
**conservation invariant** — `index_docs ≡ {store keys matching prefix, of matching type, not
expired}` — asserted at the quiescent points of workloads that already exist, living in
`crates/testing/` beside the existing conservation checkers. The same shape generalises to
store↔expiry-index and store↔DBSIZE, which is what closes the non-search instances. Per
`INFRASTRUCTURE.md` I4, `testing/src/conservation.rs` already hosts six checkers of exactly this
shape (`check_exactly_once_delivery:121`, `check_fifo_wake_order:246`,
`check_tx_sum_conservation:431`, `check_watch_no_false_negative:621`, `check_pel_conservation:682`)
— this is a seventh, not a new pattern.

## Evidence

- **Failed RocksDB spill → real delete + replicated `DEL` + `evicted` notification.** *(01/F2)*
  `core/src/shard/eviction.rs:269-277` — `line_counts` 0 for the
  `Err(e) => { … self.delete_for_eviction(key).await }` arm, which routes a *real* removal through
  `run_internal_removal_effects`. The doc comment at `eviction.rs:240-254` concedes the fallback is
  destructive. `core/src/store/hashmap.rs:771-772` (the `SpillError::Rocks` return) is likewise
  0-covered.
- **Failed warm-tier read → key reads absent but stays in `data`/`expiry_index`/DBSIZE.** *(01/F3)*
  `core/src/store/hashmap.rs:813-827` — the `Ok(None)`, `Err(e)` and deserialize-`Err` arms of
  `unspill_key` all `return None` and take no repair action; `line_counts` 0 for all three. So
  `GET k` → nil while `EXISTS k` → 1 and DBSIZE still counts it. Contrast `hashmap.rs:800-806`, the
  *expired*-warm-key arm, which correctly calls `uninstall` + `note_expired_on_unspill` and **is**
  covered (`core/tests/tiered_storage.rs:159`).
- **Mid-iteration RocksDB error indistinguishable from end-of-CF → shard silently truncated.**
  *(13/F8)* `persistence/src/rocks/columns.rs:41-45` — `self.inner.next().and_then(|r| r.ok())`;
  `Item` is `(Box<[u8]>, Box<[u8]>)`, so the error is structurally unrepresentable. `recovery.rs`
  consumes this iterator directly and returns `Ok(stats)` with an under-reported `keys_loaded`.
  `RocksStore::has_data` (`rocks/mod.rs:531-539`) has the same shape.
- **Search index absent from snapshot and full-sync → an empty index reported as success.**
  *(10/F3, 10/F4)* `persistence/src/snapshot/stager.rs:9-11` and `:100-101` state that neither a
  snapshot nor a replication full sync ships `<data_dir>/search`. The RocksDB checkpoint *does*
  carry `search_meta`, so `IndexLifecycleManager::recover`
  (`core/src/shard/search/lifecycle.rs:357-431`) finds the definition, `ShardSearchIndex::open`
  (`search/src/index.rs:252`) calls `Index::open_or_create` (`index.rs:258`) against an empty dir,
  and `recover` records `RecoveryOutcome::Recovered { num_docs: 0 }` (`lifecycle.rs:381-383`) — the
  *success* variant. Nothing compares `num_docs` to the shard's key count. Separately the tantivy
  commit point and the WAL recovery point are independent (`core/src/shard/event_loop.rs:31`,
  fired at `:88-95`, vs `core/src/shard/post_execution.rs:385-405`), so an unclean shutdown leaves
  the two permanently at different points.
- **HLL encode failure → persists an empty sketch, returns `Ok`.** *(13/F9)*
  `persistence/src/serialization/probabilistic.rs:181-183` — the `else` arm writes
  `(TypeMarker::HyperLogLog, vec![0, 0, 0, 0, 0])` with the comment "Shouldn't happen, but fallback
  to empty sparse". No log, no metric, no error, no test reaches the arm.

## What to fix

1. Add a seventh checker to `testing/src/conservation.rs` implementing
   `index_docs ≡ {store keys matching prefix, of matching type, unexpired}`, following the shape of
   the five existing checkers.
2. Generalise it to two sibling invariants over the same seam: store ↔ `expiry_index`, and
   store ↔ DBSIZE. The `shard_driver` harness already exposes `memory_check` and
   `expiry_index_check`.
3. Wire all three into the quiescent points of the **existing** fault-injection, restart and
   workload runners — no new workload.
4. Add the cheap direct detector the search proposal names: `FT.INFO`'s `num_docs` must equal the
   number of prefix-matching keys, reusable as a helper.
5. Make each failure arm above assert a *decided* contract rather than the current silent success.
   The failed-spill policy specifically is a semantics call — see issue 30.

## Acceptance criteria

- [ ] `testing/src/conservation.rs` gains `check_index_conservation` (or equivalent), asserted at
      every quiescent point of at least one existing fault-injection workload and one restart
      workload.
- [ ] A test corrupts a spilled key's warm-CF record out of band and asserts
      `GET`/`EXISTS`/`DBSIZE`/`TYPE`/`SCAN` agree with one another — it fails today.
- [ ] A test seeds a CF with keys `a`, `m`, `z`, makes `m`'s merge operand undecodable, calls
      `recover_shard_into`, and asserts it does **not** return `Ok` with `keys_loaded == 1`; and
      that `has_data()` does not report an unreadable CF as empty.
- [ ] A test asserts the HLL encoder is total — for every constructible `HyperLogLogValue`,
      `as_sparse()` or `as_dense()` is `Some`, so `probabilistic.rs:181-183` is provably dead.
- [ ] A restore/full-sync test asserts `FT.INFO idx` reports `num_docs` equal to the count of
      prefix-matching keys on the receiving node — it fails today.

## Test boundary

**Level 3 for the invariant, level 5 for the workloads it runs inside.** The checker itself is
crate-level code in `crates/testing/`; it is asserted from the `shard_driver` harness for the
store↔expiry-index and store↔DBSIZE forms, and from the existing multi-node/crash workloads for
the index form (10/F3 and 10/F4 are genuinely full-sync and crash-restart properties and cannot be
observed lower). The HLL encoder totality assertion is **level 1** (pure encoder) and the RocksDB
iterator assertion is **level 2** (real `RocksStore` with the merge operator, no server).

## Depends on

Issue 04, `.scratch/testing-improvements-round2/issues/` (conservation checker in
`crates/testing/`) is this issue's core deliverable. Issue 01,
`.scratch/testing-improvements-round2/issues/` (`shard_driver` harness extension —
`with_eviction`, optional warm/persistent store) is required for the spill and warm-read arms. The
failed-spill contract is gated on the semantics call in issue 30,
`.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: partially-fixed**

One of five findings is fixed; the core deliverable (a seventh conservation checker) does not
exist — `crates/testing/src/conservation.rs` still has exactly the five checkers named in the body
(`check_exactly_once_delivery:121`, `check_fifo_wake_order_exact:417`,
`check_tx_sum_conservation:539`, `check_watch_no_false_negative:818`, `check_pel_conservation:879`)
and no `check_index_conservation`. Per-claim:

- **Failed RocksDB spill → real delete + replicated `DEL` — FIXED.** Commit `0d727d05`
  (2026-08-03, "keep keys on failed spill"), tracked as round-2 issue 41 (now in `issues/done/`).
  `crates/core/src/shard/eviction.rs:299-309` now splits `Err(e @ SpillError::Rocks(_))` (log +
  return `false` ⇒ caller replies `-OOM`, key survives) from structural errors, which still fall
  back to `delete_for_eviction` at :320. Forced by `crates/shard-harness/tests/eviction_spill_failure.rs`.
  Line refs old → new: `eviction.rs:269-277` → `:262-323`; the doc rationale moved to `:236-261`.
- **Failed warm-tier read → still valid.** `crates/core/src/store/hashmap.rs:812-831` (old ref
  `:813-827`) — the `Ok(None)`, `Err(e)` and deserialize-`Err` arms of `unspill_key` all still
  `return None` with only a `warn!`, taking no repair action, while the expired-warm-key arm at
  `:799-808` still correctly `uninstall`s.
- **Mid-iteration RocksDB error → still valid.** `crates/persistence/src/rocks/columns.rs:41-45`
  is byte-identical (`self.inner.next().and_then(|r| r.ok())`, `Item = (Box<[u8]>, Box<[u8]>)`).
  Owned separately by still-open issue 42.
- **Search index absent from snapshot/full-sync → still valid, and now deliberate.**
  `crates/persistence/src/snapshot/stager.rs:9-11,98-124` documents the exclusion as a decided
  design ("proposal 23 — search-sidecar layout, DELETE branch"), so the remaining gap is purely
  the *unasserted* `num_docs` reconciliation. Owned separately by still-open issue 46.
- **HLL encode failure → still valid.** `crates/persistence/src/serialization/probabilistic.rs:211-214`
  (old ref `:181-183`) still has the `else` arm writing `(TypeMarker::HyperLogLog, vec![0;5])` with
  the "Shouldn't happen" comment.

Relationship to **issue 61**: 61 was re-triaged as *superseded by issue 23*, not by this issue —
61/23 are the scatter-merge family (T5), this issue is the derived-structure family (T2). They do
not overlap in evidence; keep both.
