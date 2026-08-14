# 19: `Satisfaction::Retry` — proven dead (`unreachable!()`) or re-registers; never a silent nil

Status: ready-for-agent

## Origin

Distsys-review MAJ-16 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **accept, investigate-first** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

`specs/blocking.md`'s preamble promises "every way a blocking command can be
resolved", but `Satisfaction::Retry` is an unrowed resolution:

- `frogdb-server/crates/core/src/shard/blocking.rs:325`:
  `Satisfaction::Retry => continue` — the `entry` is dropped **with its
  `response_tx`**; the client's wait ends in `Err(_)` → bare `Response::Null`.
- `:884-891`, `:1041-1048`: the producing arms are `_ =>` fallbacks returning `Retry`
  after `debug_assert!(false)` — in a release build, a silent nil for a client that
  should have stayed parked.
- The documented cause (`:571-573`: "it lost a race to an earlier waiter that emptied
  the key") is not reachable as written: `check_key` runs immediately before every
  `satisfy` on the same serial thread, so `Yes` followed by a `None` pop cannot
  happen for the same key. Documented cause and actual guard disagree — the
  suspicious part.

## Ruled shape

Investigation first, then one of two fixes; either way the silent-nil path dies:

1. **Step 1 — settle reachability**: targeted attempt to construct a reachable
   `Retry` (walk every producer arm's guard against the serial-thread argument), plus
   mutants evidence (`cargo mutants -p frogdb-core` over `blocking.rs`: is any `Retry`
   return forced by a test?). Write the verdict into the issue/commit.
2. **If dead**: replace the `debug_assert!(false)` + `Retry` arms with
   `unreachable!()` — fail-stop over a silent wrong answer. Row: the resolution
   taxonomy (satisfy / timeout / drain / kill / disconnect) is total; `Retry` does
   not exist as an outcome.
3. **If reachable**: re-register the entry (Redis shape — re-check and keep the
   client blocked, deadline intact), never a silent nil. Row the outcome ("a waiter
   that loses a satisfaction race remains parked with its original deadline") +
   forcing test constructing the race.

## What to build

1. Reachability verdict recorded (step 1 above).
2. Code per the branch taken; the `Err(_) → bare Response::Null` reply path for
   dropped waiters is removed in both branches (any remaining channel-drop is a
   fail-stop diagnostic, not a nil).
3. Spec row per the branch taken; `just lint-spec` green.
4. Forcing test: reachable branch — the constructed race; dead branch — mutants
   evidence that the `unreachable!()` arms are not weakening coverage (no
   previously-forced behavior lost).

## Cross-references

- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md): same machinery — the
  producer arms may move or vanish in the run-loop restructure. Coordinate; if 13
  lands first, re-derive the producer inventory before investigating.
- [Issue 16](16-deny-blocking-context-returns-op-aware-nil.md): sibling wrong-shape
  family (bare `Null` where an op-aware reply belongs).

## Acceptance criteria

- [ ] Reachability verdict documented with evidence
- [ ] Dead → `unreachable!()`; reachable → re-register with original deadline
- [ ] Silent-nil path removed in both branches
- [ ] Row landed; `just lint-spec` green; forcing test per branch

## Blocked by

None — can start immediately (coordinate with issue 13 if in flight).
