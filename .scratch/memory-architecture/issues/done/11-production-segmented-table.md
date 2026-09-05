# 11: production segmented keyspace table (Dashtable, 8-byte tagged words)

Status: done
Type: AFK
Origin: drafted 2026-09-01 from the landed
[spike report](../../spike-report-table.md) — [PRD.md](../../PRD.md) R5/R6, D5
Area: new crate (working name `frogdb-table`) + frogdb-core store seam
Phase: 3 — keyspace table

## Why

The R5 spike ruled GO: a segmented extendible-hash table with 8-byte tagged slot words
holds 33.6 struct B/entry vs griddle's 203.4 table term — 3.3–6.4× less live memory per
entry on realistic shapes — with SCAN's exactly-once guarantee proven executably across
mid-scan splits. Lookup is CONDITIONAL: 2.0×/2.4×/3.2× slower than griddle at hasher
parity, with the scalar fingerprint loop identified as SIMD's target and the 4-cache-line
bucket as layout cost SIMD cannot remove. This issue builds the production crate and
takes the swap-out decision against the spike's ship gate.

Everything below inherits the spike's rulings verbatim (do not relitigate): 8-byte word /
16-byte slot / 14 slots per 256 B bucket / 64 B header, inline threshold 7 bytes for
strings and 61 bits for ints, keys never inline, `u32` directory, reverse-binary cursor
advanced at the scanned segment's local depth.

## What to build

### 1. The crate

Production-quality port of `spike-table/` (`.scratch/memory-architecture/spike-table/` —
read it; the layout, split stride, and cursor code carry over) with the spike's
follow-ups 1, 2, 3, 5 built in from the start, in this order:

- **Size the segment to a jemalloc size class first** (follow-up 5) — arithmetic, not
  research; do it before measuring anything. Target the 16 384 B class (~63 buckets).
- **SIMD fingerprint match** (follow-up 1): 14 fp bytes in a NEON `uint8x16_t` / SSE2
  `__m128i`, match-and-mask, scalar fallback behind `cfg`. The blocking perf item.
- **16 fingerprint bits per slot** (follow-up 2): the split bit readable from bucket
  metadata, so a split does 404 slot copies instead of 808 rehashes (~44 µs → ~10–15 µs
  expected). Re-measure the stall with the spike's scanned/moved/dir-write counters kept
  as (test-only or feature-gated) instrumentation.
- **Displacement on insert** (follow-up 3): Dash-style relocate-from-home-bucket;
  occupancy 0.581 → ~0.9, structure 33.6 → ~21.7 B/entry.
- **`KeyMetadata`/`KeyType` placement** (follow-up 4) is this issue's first design
  question, unanswered by the spike. Decide with measurement: heap-record header vs third
  slot word (+8 B/e) vs per-segment expiry structure. Record the decision and its
  numbers in the report.

### 2. R6 heap records: refcount + COW (D5 ruling)

The out-of-line record is `{len: u32, rc: u32}` + payload, `rc` a **non-atomic,
same-core** refcount (the table is shard-owned per R2/R3, never shared). Implement the
COW half: cloning a value for snapshot/replication bumps `rc`; a write to a record with
`rc > 1` copies first. Forcing test: mutate-under-snapshot sees the old bytes in the
snapshot and the new bytes in the table.

### 3. Hasher and HashDoS (turmoil ruling, D5)

Production: keyed ahash, per-shard seed drawn from process randomness at startup
(HashDoS stance no weaker than the incumbent griddle default). Sim/turmoil builds: the
seed comes through the deterministic sim entropy so runs replay bit-identically —
**pinning fidelity is a seed question, not a table question**; the SCAN guarantee and all
invariant tests must hold under any seed (pin one seed in tests for reproduction, as the
spike's cursor tests do).

### 4. txn/VLL: no coupling (D5 ruling)

VLL continuation locks and cross-slot transactions operate above the store seam, keyed on
key bytes and shard dispatch — nothing in txn admission reads table internals. The table
swap must not change any txn-visible behavior. Forcing evidence: the existing
`frogdb-txn`/`frogdb-vll` suites and the turmoil txn sims run green against the swapped
store with **zero** test edits. Any needed edit = a design smell to raise, not patch.

### 5. Safety

The design is `unsafe` by construction (tagged words, raw payload pointers,
`alloc_zeroed` segments). Required: miri clean on the crate's test suite (or a documented
minimal miri-excluded surface with reasons), model-based fuzz target (ops vs a
`HashMap` model, including SCAN-under-churn exactly-once), and the spike's cursor
invariant tests ported.

### 6. Store integration + the swap gate

Wire the crate behind the store seam (`frogdb-server/crates/core/src/store/hashmap.rs`
SCAN derives cursors from a content hash today — this replaces that with real Redis
cursor semantics). The swap-out itself is **gated**, per the spike report:

- Re-measure lookup hit/miss on an idle Linux box (testbox), both sides same hasher,
  SIMD landed. **Ship gate: within 1.25× of griddle on `redis-feel` and `sessions`.**
- `counters` (short keys) measured separately; above 1.5× → resize the fingerprint
  block before swapping.
- Gate failed → the table ships crate-complete but the default store stays griddle;
  report the numbers and stop. Keeping griddle hot + segments where memory dominates is
  an acceptable outcome (the memory verdict does not depend on the swap).

## Acceptance criteria

- [ ] Crate green: unit + invariant + cursor tests, miri (or documented exclusions),
      fuzz target compiled and run.
- [ ] Split stall re-measured with 16 fp bits; scanned == moved (no rehash), stall
      p50 materially below the spike's 44 µs.
- [ ] Occupancy ≥ ~0.85 with displacement; struct B/e reported at allocated class.
- [ ] COW forcing test green; rc non-atomic and same-core by construction.
- [ ] Existing txn/VLL suites + turmoil sims green with zero test edits.
- [ ] Lookup gate measured on the testbox and the swap decision recorded with numbers
      either way.
- [ ] `MEMORY USAGE`/budget integration run-stable; budget-growth gate satisfied for the
      new crate's buffers.

## Test boundary

Level 1: property/fuzz vs model, miri, invariant tests in-crate. Level 2: full
`frogdb-core` + regression suites against the swapped store (if swapped). Perf: testbox
(idle Linux) only — macOS numbers are upper bounds, not gates.

## Spec rows at R15

The OOM-verdict and eviction rows ([issue 22](../)) bind above this crate via
`memory_size()`; the table itself contributes the SCAN exactly-once guarantee row and
the sampled-upper-bound sizing row if issue 22's audit wants them mechanical.

## Out of scope

Eviction logic (issue 12 — the 64 B header's R9 fields are reserved and named, do not
grow the header), value block encodings (issues 13–17), directory shrink/segment merge
on delete (spike follow-up 6, file separately if it bites), changing the inline
threshold or slot width (ruled).

## Depends on

[Issue 10](../) (landed — the spike report is the contract). Not blocked by the value
representation issues; the table stores words and heap records regardless of what the
payload encodes.

## Resolution

Landed 2026-09-05 on `mem-arch-integration` (picks `0612ab94a`..`6988ef192`, 15 commits).
**Default store backend unchanged (griddle)** — the swap is gated on a testbox lookup
measurement that could not be taken in local build mode (see below).

What shipped: new crate `frogdb-table` — segment geometry sized to the 16 KiB jemalloc
class, tagged 8-byte `KeyWord`/`ValueWord` (inline ≤ 7 B, else an R6 refcounted/COW heap
record), SIMD fingerprint match, 16 route bits in the slot so a split never rehashes, Dash
displacement + stash, 13-slot × 256 B buckets (the 64 B `KeyMetadata` rides in the entry:
layout (a) measured 27.1 vs 42.0 structural B/entry against a third slot word),
keyed-ahash hasher with sim-seed plumbing, directory + incremental one-segment split, and a
split-stable reverse-binary SCAN cursor (Redis's actual cursor — one segment per step at its
local depth, replacing the incumbent's whole-shard sort per SCAN step). Safety: words carry
`PhantomData<*mut u8>` (not `Send`/`Sync`); the one `unsafe impl` in the crate is
`Send for Table<V: Send, N>` (no `Sync`), with `KeyWord` deliberately not `Clone` and
`ValueWord::duplicate` `#[cfg(test)] pub(crate)`, so no second handle to a record can leave
the table from safe code; the SAFETY comment enumerates every public route; six
`compile_fail` doctests pin the attacks. `with_record_mut` is panic-safe via a `LentRecord`
drop guard. `frogdb-core` gains a `Keyspace` seam (`store/keyspace.rs`, `KeyRef`,
visitor-style iteration, reservoir `random_key`/`sample_keys`): `GriddleKeyspace` is the
incumbent moved not rewritten and stays `Selected`; `TableKeyspace` (`Table<Box<Entry>>`)
sits behind the `table-keyspace` cargo feature. `HashMapStore`'s public API is
byte-identical. Also: `tests/layout_cost.rs` (layout decision + occupancy cycle),
`benches/{lookup,split}.rs` with `just bench-table` / `just bench-table-split`, fuzz target
`table_ops` (model check vs `HashMap` incl. SCAN under mid-scan splits, values compared).

Numbers, honest: occupancy oscillates in rounds (all segments fill together, then a burst of
splits) — peak 0.913 / 21.9 B/entry, trough 0.515 / 38.9, cycle mean 0.716 / 28.7 at 200 k
and 0.685 / 30.1 at 1 M, settled at 1 M 0.596 / 33.6 = the spike's settled figure. The test
asserts peak ≥ 0.85, trough ≥ 0.50, cycle mean < 33.6 B/entry and > 0.581. Split p50
11.2 µs vs the spike's 44.4 µs (4.0×), 28 ns per moved entry, `rehashed/split = 0.000`.
End to end 132.7 B/key table vs 179.9 griddle (26 %), half of the table figure being the
boxed 64 B `Entry`.

Review: round 0 (1 Critical, 3 Important, 4 Minor) — words were auto-`Send`/`Sync`; growth
past 2^16 segments asserted instead of degrading; occupancy claim not like-for-like; fuzz
oracle compared presence not values. Fix r1 (6 commits) addressed all eight; re-review r1
found the `Send` impl defeatable from safe code via `iter()` + public `KeyWord: Clone`. Fix
r2 (1 commit) removed the capability; re-review r2 independently enumerated the public
surface and verified the doctest fails for the stated reason: all addressed, no new
Critical/Important. Gates: crate 68/68, doctests 6/6, miri 46/0 (documented skip set),
`cargo check`/clippy `frogdb-core --features table-keyspace --all-targets` clean, clippy
`frogdb-table` clean, lint-spec, lint-gates, `just fuzz table_ops 900` 36 966 runs clean,
full `just test frogdb-server` 2144/2145 (253 s; the one failure, `integration_persistence::lastsave_reports_the_snapshot_time_after_restart`, died on a test-harness RocksDB `LOCK: No locks available` restart race while another session compiled — 6/6 green re-run in isolation, no persistence code in this diff; one cluster flake passed on retry) before landing.

Deviations from the brief, for human sign-off:

- **Lookup gate not measured; swap not made.** Laptop numbers (`just bench-table`) were
  unusable — 17× spread on griddle between two statistically identical workloads while
  three other agents compiled. Decision recorded: default stays griddle. To decide:
  `just tb-warmup; just tb-run "just bench-table"`, read
  `lookup-hit/{redis-feel,sessions}/{griddle,table}`, gate is within 1.25×. Flip is one
  `cfg` swap on `Selected` in `frogdb-server/crates/core/src/store/keyspace.rs`.
- **"scanned == moved" read as "no rehash".** A split must examine every occupied slot
  (797 scanned) and moves half (399); the operative property `rehashed/split = 0` holds
  and is asserted.
- **`scripts/budget-growth.py` is outside the file boundary**: one path
  (`frogdb-server/crates/table/src/`) added to `KEYSPACE_MODULES`, the script's own scope
  boundary (keyspace bytes are attributed by the per-shard arena under ADR-0006 §3), not the
  `ALLOWLIST` ratchet.
- **13-slot geometry** falls out of the 16 B slot in a 256 B bucket; the brief's ~0.9 /
  ~21.7 B/entry prediction is met at the cycle peak only.

Follow-ups to file: (1) collapse `Entry` into tagged words — the boxed 64 B entry is a
per-key allocation griddle does not pay and is the single most valuable next step for the
PRD's memory target; (2) a `.config/nextest.toml` override for
`store::scan_stress::scan_present_throughout_is_subset_of_returned` (15 s ×3), the same
starvation its sibling already has an override for — passes in 11–12 s idle, 0.5 s on the
table backend.

Known gaps carried: `Slot.key` is `pub`, so a future public `&mut Slot`/`&mut Segment`
accessor on `Table` would reopen the `Send` escape (SAFETY comment names `take`/`remove`
specifically); `ValueWord::with_record` is a public clone-a-record-out-of-a-borrow
primitive, harmless while no `Send` value type holds a `ValueWord`; `benches/split.rs`
`split_rehashed == 0` is asserted by measurement not construction, `end_occupancy`/`end_bpe`
printed only.
