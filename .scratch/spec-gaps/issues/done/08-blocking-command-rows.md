# Blocking-command rows: shard-death error, topology change, disconnect, admission limits

Status: done

## Parent

[spec-review-txn-vll-blocking.md](../../../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
— Finding H5 ("FM-BLOCKING-004 contradicts FM-BLOCKING-002 and FM-BLOCKING-003"), Finding H6
("no rows for slot migration, promotion, or client disconnect"), Finding H7 ("the admission limit
is enforced in code and rowed nowhere").

## What is wrong

**H5 — three-row contradiction.** FM-BLOCKING-004 specifies `Response::Null` when the shard drops
the response channel, confirmed by its forcing test
(`connection/blocking/coordinator.rs:173-180`, `channel_drop_yields_null_response`). But
FM-BLOCKING-002 lists as NOT observable "one nil shape for both op families (the wrong-shape bug
`into_response` exists to fix)", and FM-BLOCKING-003 lists as NOT observable "a bare
`Response::Null` standing in for the array-shaped nil". So a `BLPOP` client gets `$-1` where every
other nil path on the same command gives `*-1`. Worse: shard death is reported as a timeout — the
client cannot distinguish "no data arrived within your timeout" from "the shard serving your key is
gone," inviting a silent retry loop against a dead shard. `txn.md` FM-TXN-032 takes the opposite,
correct position for the same underlying event. As written, no implementation can satisfy all of
002, 003, and 004 simultaneously.

**H6 — no cluster/topology rows.** `txn.md` devotes eight rows to cluster interaction
(FM-TXN-022…030, FM-TXN-048/049); `blocking.md` has none, despite blocking waits being the
longest-lived connection state in the system. Three gaps:
1. **Demotion**: a waiter parked on a primary that is demoted must not be served from replicated
   data (would be a replica-side write, diverging the replica's store from its own replication
   stream). `WAIT` already handles the analogous case
   (`connection/blocking.rs:330-335`, `WAIT_ROLE_CHANGED_ERR`, cross-referenced to
   FM-REPLICATION-040) — and `WAIT` is in `blocking.md`'s own scope line yet has no row at all.
2. **Slot migration**: a waiter parked on a key whose slot departs must be resolved (`-MOVED` or
   unblock), never served locally from a slot this node no longer owns — the blocking analogue of
   FM-TXN-049.
3. **Client disconnect**: `cleanup_wait` is explicitly in scope, but no row covers a disconnect
   while parked, so nothing forces the shard-side unregistration that prevents a leaked wait entry.

**H7 — unrowed admission limit.** `ShardWaitQueue` enforces real bounds — 10,000 waiters per key,
50,000 blocked connections total (`frogdb-server/crates/core/src/shard/wait_queue.rs:117-155`) —
returning `-ERR max blocked connections limit reached` / `-ERR max waiters per key limit reached`.
`register_wait` is named in `blocking.md`'s scope, but no row covers refusal-at-registration:
unrowed means unforced, so a mutant removing the bound (turning a DoS-resistant queue into an
unbounded one) survives the gate.

## What to build

1. **H5**: Reply `-ERR shard unavailable` on channel closure (matching FM-TXN-032's vocabulary),
   reconciling FM-BLOCKING-002/003/004 into one consistent contract (rows edited so all three agree).
2. **H6**: Add three rows:
   - Demotion → unblock all parked waiters on the demoted-to-replica node (Redis
     `disconnectAllBlockedClients` precedent), never serve from replicated data.
   - Slot migrated away while parked → `-MOVED` or unblock, never serve a departed slot.
   - Client-disconnect-while-parked → forced cleanup row exercising the `cleanup_wait` path.
   - Plus a `WAIT`-role-change row pinning `WAIT_ROLE_CHANGED_ERR`
     (`connection/blocking.rs:330-335`) so the scope line's promised coverage is delivered.
3. **H7**: Add `FM-BLOCKING-006 — registration refused at the waiter limit`, pinning both error
   texts (10k/key, 50k/shard) and, as NOT observable, "a refused registration that nonetheless
   leaves an entry in the wait queue."

## Acceptance criteria

- [ ] FM-BLOCKING-002/003/004 reconciled to a single consistent contract (`-ERR shard unavailable`
      on channel drop, or equivalent agreed vocabulary); forcing test updated/added; `just
      lint-spec` green
- [ ] Demotion row: parked waiter on demoted node unblocks, never served from replicated data;
      forcing test
- [ ] Slot-migration row: parked waiter on departed slot resolves via `-MOVED`/unblock; forcing test
- [ ] Client-disconnect row: `cleanup_wait` path forced by a disconnect-while-parked test
- [ ] `WAIT` role-change row added, pinning `WAIT_ROLE_CHANGED_ERR`
- [ ] `FM-BLOCKING-006` added with both admission-limit error texts pinned and NOT observable for
      "refused but still registered"; forcing test
- [ ] `just mutants-diff frogdb-core` (blocking/wait_queue paths) triaged on touched code

## Blocked by

None.

## Ruling (2026-08-13)

All rows, error on shard death. Shard death while parked → `-ERR shard unavailable` (distinct from
timeout nil; resolves the FM-BLOCKING-004 vs 002/003 three-row contradiction). Demotion → unblock
all parked waiters (Redis disconnectAllBlockedClients precedent). Slot migrated away while parked
→ -MOVED/unblock, never serve a departed slot. Client-disconnect-while-parked → forced cleanup row
(cleanup_wait path). WAIT role-change row (WAIT_ROLE_CHANGED_ERR exists at
connection/blocking.rs:330-335, unrowed). FM-BLOCKING-006 admission-limits row pinning 10k/key +
50k/shard (wait_queue.rs:117-155) + exact error texts + NOT-observable.

## Amendment (2026-08-14, distsys-review MAJ-11)

**H5 has an ordering dependency.** The review proved "the server's reply normally wins"
(TR-BLOCKING-007) false: the coordinator select is `biased;` with `response_rx` first, so a
dropped sender deterministically takes the `Err(_)` arm. Today THREE live paths signal by
dropping the sender — admission refusal (`shard/blocking.rs:49-57`), the deadline fast-path
(`:320-322`), and `Satisfaction::Retry` (`:325`) — so ruling `Err(_)` → `-ERR shard
unavailable` as-is turns ordinary timeouts into false shard-unavailable errors.

Ruled: land the drop-elimination first, then H5 as written.

1. Deadline fast-path sends `entry.op.timeout_reply()` (and increments
   `BlockedTimeoutTotal`) instead of dropping the sender.
2. `Satisfaction::Retry` likewise sends a real reply, never drops (see also MAJ-16).
3. Admission refusal sends its refusal reply (H7 already requires this).
4. Only then does `Err(_)` uniquely mean channel death, and the H5 `-ERR shard
   unavailable` ruling is sound. Amend TR-BLOCKING-007 to delete the race fiction.
