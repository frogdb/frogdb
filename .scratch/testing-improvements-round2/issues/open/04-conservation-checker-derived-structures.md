# Conservation checker for derived structures — collapse four search findings into one invariant

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I4
LOE: 2–3 days (estimated)
Tier: B
Area: crates/testing / conservation checkers (search index ↔ store)
Asked by: 10 — "the single highest-leverage item in the audit"
Unblocks: 10/F3, F4, F5, F9 collapse into **one invariant** rather than four example tests

## Context

The search audit found four separate ways the tantivy index can diverge from the store
(restore, attach, `FT.ALTER`, and more). Written as four example tests they cover four known
paths and nothing else. Written as one conservation invariant asserted at every quiescent
point of the existing workloads, they cover the class. `crates/testing` already hosts six
checkers of exactly this shape, so this is a seventh instance of an established pattern, not
new machinery.

## Evidence

- **Invariant**: `index_docs ≡ {store keys matching prefix, of matching type, not expired}`,
  asserted at every quiescent point of the existing fault-injection and restart workloads.
- `testing/src/conservation.rs` already hosts six checkers of exactly this shape
  (`check_exactly_once_delivery:121`, `check_fifo_wake_order:246`,
  `check_tx_sum_conservation:431`, `check_watch_no_false_negative:621`,
  `check_pel_conservation:682`). This is a seventh, not a new pattern.
- **Generalises to**: store↔expiry-index and store↔DBSIZE (theme T2 in `MASTER.md`).

## What to build

1. A seventh checker in `testing/src/conservation.rs`, following the shape of the five cited
   above, asserting `index_docs ≡ {store keys matching prefix, of matching type, not
   expired}`.
2. Wire it into the quiescent points of the existing fault-injection and restart workloads,
   so it runs without each new search test having to opt in.
3. Keep the key-set predicate parameterised so the same checker can later express
   store↔expiry-index and store↔DBSIZE (theme T2) without a rewrite.

## Acceptance criteria

- [ ] A `check_*` function in `testing/src/conservation.rs` asserts the invariant above and
      follows the signature/reporting shape of the existing six checkers.
- [ ] The checker runs at every quiescent point of the existing fault-injection and restart
      workloads, with no per-test opt-in.
- [ ] A deliberately corrupted index (one doc removed, one extra) makes the checker fail with
      a diff naming the offending keys.
- [ ] The predicate is parameterised over (prefix, type, expiry) rather than hardcoded to the
      search index.

## Test boundary

Level 4 — the invariant has to be checked against a live store plus a live index at workload
quiescence, which is where the existing fault-injection and restart workloads run; the
checker function itself is level-1 testable in isolation.

## Depends on

Nothing. **Needs coordination with** whoever owns `crates/testing/`.
