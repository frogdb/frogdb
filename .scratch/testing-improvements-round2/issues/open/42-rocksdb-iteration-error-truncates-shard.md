# `RocksIterator::next` maps a mid-iteration error to `None` — recovery silently truncates a shard

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/13 F8 · MASTER.md §3
Score: severity 5 · likelihood 3 · effort 2 · priority 19
Area: frogdb-persistence / RocksDB columns + recovery

## Context

Recovery's `for (key, value) in rocks.iter_cf(shard_id)?` cannot distinguish "error" from "end of
column family". One unreadable block — a bad SST checksum, or a merge-operator failure — drops
**every remaining key in that shard** and still returns `Ok(stats)`, with an under-reported
`keys_loaded`, no error, no metric and no log. The truncated state then becomes the new truth at
the next snapshot. The merge path makes this reachable from data alone, not only from disk
corruption.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `persistence/src/rocks/columns.rs:41-45` —
  ```rust
  fn next(&mut self) -> Option<Self::Item> {
      self.inner.next().and_then(|r| r.ok())
  }
  ```
  The `Item` type is `(Box<[u8]>, Box<[u8]>)`, so the error is structurally unrepresentable.
  `recovery.rs` consumes this iterator directly.
- `RocksStore::has_data` (`rocks/mod.rs:531-539`) has the same shape and would report "empty" for
  an unreadable CF.
- Reachability from data alone: `full_value_merge`/`partial_value_merge`
  (`persistence/src/rocks/mod.rs:607-631`) return `Option<Vec<u8>>`, and `None` becomes a RocksDB
  Corruption status — reachable from a single undecodable HLL delta operand.

## What to fix

1. Change `RocksIterator::Item` to `Result<(Box<[u8]>, Box<[u8]>), Error>` (or have the iterator
   latch the first error and expose it), so a mid-iteration failure is representable.
2. Make `recover_shard_into` propagate that error — fail loudly, or skip exactly the bad key and
   report it in `stats` plus a log line and a metric. Silently returning `Ok` with a truncated
   shard must become impossible.
3. Apply the same treatment to `RocksStore::has_data`, which today reports an unreadable CF as
   empty and would let a node come up believing it has no data.

## Acceptance criteria

- [ ] New crate-level test seeds a CF with keys `a`, `m`, `z`, makes `m`'s merge operand
      undecodable so the merge operator returns `None`, calls `recover_shard_into`, and asserts it
      does **not** return `Ok` with `keys_loaded == 1` — the shard must either fail loudly or skip
      only `m`. **Fails today.**
- [ ] The same test asserts `has_data()` does not report an unreadable CF as empty.
- [ ] The test is deterministic: put a good key, merge a garbage HLL operand under a middle key,
      put another good key, then iterate — no timing, no fault-injection harness.

## Test boundary

**2** — needs a real `RocksStore` with the merge operator installed, but no server. Not level 3+:
the defect is entirely inside the persistence crate's iterator contract, and a shard worker would
only re-expose the same `Ok` return.

## Depends on

Theme T2 (failure of a derived structure reported as success) — issue 20,
`.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: still-valid**

The swallow is byte-for-byte intact: `persistence/src/rocks/columns.rs:41-44` is still
`type Item = (Box<[u8]>, Box<[u8]>); fn next(&mut self) { self.inner.next().and_then(|r| r.ok()) }`
(last touched by 3a135004, March), and `recover_shard_into` still drives it as
`for (key, value) in rocks.iter_cf(shard_id)?` at `persistence/src/recovery.rs:147` — the `?` covers
only CF-handle resolution, so a mid-iteration `Err` ends the loop and returns `Ok(stats)` with a
short `keys_loaded`. Reachability is unchanged: `full_value_merge`/`partial_value_merge`
(`persistence/src/rocks/mod.rs:671-692`) still return `Option<Vec<u8>>` and `merge_hll_serialized`
(`persistence/src/serialization/probabilistic.rs:570-601`) still returns `None` on an undecodable
operand. Phase 2 locked persistence but added no FM row for iteration errors — worse,
[FM-PERSISTENCE-033](../../../../specs/persistence.md)'s Invariant cell asserts
*"Only a `RocksError` from the iteration itself propagates"*, which the code does **not** honour;
that sentence should be corrected as part of the fix. **Correction to the body**: the `has_data`
sub-claim (old `rocks/mod.rs:531-539`, now `rocks/mod.rs:591-600`) is wrong and always was — it
iterates the raw `rocksdb` iterator and tests `iter.next().is_some()`, and an error item is
`Some(Err(_))`, so an unreadable CF reports *has data*, not empty. Acceptance criterion 2 should be
restated (or dropped) accordingly; criteria 1 and 3 stand.
