# EXEC's empty-queue fast path ignores the watch set — silent WATCH false negative

Status: done

## Parent

Found during the 2026-08-13 constructive-spec drafting review of `specs/txn.md` (opus review of
the TR-row draft, finding C2). The LOCKED row `FM-TXN-034` already names this exact shape in its
NOT observable; the drafting gap pass surfaced that the code violates it and no forcing test
covers the violating arm.

## What is wrong

`frogdb-server/crates/txn/src/exec.rs:159` early-returns on a bare

```rust
if queue.is_empty() { return (TransactionOutcome::CommittedEmpty, vec![Response::Array(vec![])]); }
```

with no reference to the connection's watch set. The watch-only shard round-trip
(`exec.rs:286-295`) and the watched-slot gate (`exec.rs:234`) both sit below this early return and
are unreachable when the queue is empty. So `WATCH k` → key `k` modified by another client →
`MULTI; EXEC` (genuinely empty queue) takes the fast path and replies `*0` — a committed
transaction whose CAS precondition was already broken.

This is precisely `FM-TXN-034`'s NOT observable: *"An empty-queue fast path that skips the shard
round-trip and commits a transaction whose CAS precondition was already broken — a silent WATCH
false negative."* Its Trigger explicitly includes the *"either genuinely empty"* arm. But every
forcing test on the row scopes to the other arm (all-deferred or connection-level-only queues,
never a genuinely empty one): `an_all_deferred_queue_with_watches_still_takes_the_shard_round_trip`,
`test_watch_with_only_connection_level_commands_abort/_success`.

Redis precedent: `execCommand` checks `CLIENT_DIRTY_CAS` *before* queue length and answers a null
array — a watched-and-dirtied EXEC aborts even when the queue is empty.

## What to build

Spec-first, on the existing LOCKED row (no new row needed — FM-TXN-034 already states the
contract):

1. Forcing test for the genuinely-empty-plus-watched arm: `WATCH k` on conn A, write `k` from
   conn B, `MULTI; EXEC` on conn A → must reply null array (aborted), not `*0`. Tag it
   `FM-TXN-034` and add it to the row's `Forced by`. Must fail against today's code.
2. Fix: the empty-queue fast path consults the watch state before committing — either check the
   dirty-CAS/watch condition ahead of the `queue.is_empty()` return (Redis shape), or route
   watched empty queues through the existing watch-only shard round-trip (`exec.rs:286-295`).
   Empty queue with *no* watches keeps the fast path.
3. `specs/txn.md`'s TR-TXN-014 (empty-queue EXEC transition): drop its `Pending` link to this
   issue once fixed, and state the guard as built.

## Acceptance criteria

- [ ] New forcing test (genuinely empty queue + broken watch → null array) tagged `FM-TXN-034`,
      shown failing pre-fix, passing post-fix
- [ ] `exec.rs` empty-queue path checks watches; unwatched empty EXEC still fast-paths
- [ ] FM-TXN-034 `Forced by` updated; TR-TXN-014 `Pending` resolved; `just lint-spec` green
- [ ] `just mutants-diff frogdb-txn` triaged on the touched guard

## Blocked by

None.
