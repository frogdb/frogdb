# 19: `Satisfaction::Retry` — proven dead (`unreachable!()`) or re-registers; never a silent nil

Status: done

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

## Reachability verdict: REACHABLE (via `StreamSatisfaction`), by construction

Re-derived per producer arm after issue 13 landed:

- `StreamSatisfaction::check_key` answers *does the key exist and hold a stream*;
  `satisfy` asks *does it hold entries this waiter has not read*. The gap is real and
  reachable two ways, both now covered by tests:
  - a plain `XREAD BLOCK 0 STREAMS s <id>` parked on an id beyond the stream's tail —
    any later XADD below that id makes `check_key` say `Yes` and `read_after` return
    empty (`a_stream_waiter_the_new_entry_does_not_reach_stays_parked`);
  - an `XREADGROUP` whose single new entry an earlier waiter in the *same* satisfaction
    pass already consumed — `check_key` still says `Yes` because the stream is non-empty
    (the `_ => Satisfaction::Retry` after `read_group_entries`).
- The list/zset `None`/`is_empty` arms would need `check_key`'s non-emptiness answer to
  be stale between the check and the pop, which the shard's serial thread forbids. They
  are left returning `Retry` — under the new semantics that is a safe re-park, not a
  wrong answer — rather than converted to a panic on an argument about scheduling.
- The three per-family `_ =>` kind-mismatch arms are provably dead:
  `pop_oldest_waiter_of_kind(kind)` filters through `entry_matches_kind`, which is
  exactly the op set each strategy matches. These are now `unreachable!()`.

Because the verdict is *reachable*, the ruled shape is branch 3: re-register, never a
reply.

## What landed

- TR-BLOCKING-023 + FM-BLOCKING-013: a popped-but-unsatisfiable waiter stays parked
  with its original deadline, registration ordinal and deque position; nothing is sent
  and the channel is not dropped.
- `drive_satisfaction_body` collects retried waiters and requeues them at a single exit,
  *before* any stream drain the pass triggers (a drain-first order would strand a
  retried waiter on a key the pass just declared unusable). The drain arms became
  `break`s carrying the `KeyReady` so there is one exit path.
- `ShardWaitQueue::requeue_retry(entry, seq)` + `pop_oldest_waiter_of_kind_with_seq`:
  head-insertion with the original ordinal, and deliberately not journalled, so the
  exact-FIFO checker does not see one client as two. `register`/`requeue_retry` share a
  private `insert(entry, seq, Placement)`.
- The interim `Satisfaction::Retry => timeout_reply()` (a timeout the client never asked
  for, and before that a dropped channel = shard death) is gone.
- Forcing tests in `frogdb-core` (`shard::blocking::tests`), all four proven failing
  against the interim silent-nil arm:
  `a_stream_waiter_the_new_entry_does_not_reach_stays_parked`,
  `a_retried_stream_waiter_is_served_by_a_later_write`,
  `a_retried_waiter_keeps_its_deque_position_and_ordinal`,
  `a_retried_waiter_is_still_covered_by_a_wrong_type_drain`.
