# 11: production segmented keyspace table (Dashtable, 8-byte tagged words)

Status: ready-for-agent
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
