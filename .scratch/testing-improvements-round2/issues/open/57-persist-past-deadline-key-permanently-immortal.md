# `PERSIST` on a past-deadline key makes it permanently immortal (and `RENAME`/`TYPE`/`EXPIRETIME` resurrect too)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/06 F1 · MASTER.md §3 (consistency violations), §2 T4
Score: severity 5 · likelihood 4 · effort 2 · priority 21
Area: frogdb-commands / expiry + generic; frogdb-core store

## Context

Active expiry is sampled, so a key whose deadline has passed can still be physically present
until the sweeper reaches it. Several commands read *through* that window with no logical-expiry
check. `PERSIST` is the worst: it clears `expires_at` **and removes the key from the expiry
index**, so a key that was already logically dead becomes permanently immortal. It also
diverges primary from replica, because the replica expires independently — a consistency
violation on default config, from an ordinary cache-pinning pattern (`SET … EX` then `PERSIST`).

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly.

## Evidence

`commands/src/expiry.rs:697-700` — `PersistCommand::execute` is three lines,
`ctx.store.persist(key)` with no expiry check. `core/src/store/hashmap.rs:1239-1247` — `persist`
matches only on `expires_at.is_some()`, sets it to `None` and calls `self.expiry_index.remove`.
Sibling `touch` at `:1249` is commented "Check and delete if expired first", proving the guard is
the house style and its absence here is an oversight. Same class: `generic.rs:95` (`RENAME` uses
`store.get`), `:182` (`RENAMENX` uses `store.contains`), `:48` (`TYPE` uses `store.key_type`) —
while `generic.rs:293` (`UNLINK`) correctly uses `get_with_expiry_check` *with a comment saying
why*. `expiry.rs:738-744` / `:782-788` (`EXPIRETIME`/`PEXPIRETIME`) lack the already-expired `-2`
guard that `TTL`/`PTTL` have at `:603` / `:656`.

Cross-area note from proposal 06: `core/src/store/hashmap.rs:1239` (`persist` without an expiry
check) belongs to the core agent, while the reachability property belongs to the commands surface.

## What to fix

1. Add the logical-expiry check to `HashMapStore::persist` (mirror `touch` at
   `core/src/store/hashmap.rs:1249`): an already-expired key must be deleted, not persisted.
2. Route `RENAME` (`generic.rs:95`), `RENAMENX` (`:182`) and `TYPE` (`:48`) through
   `get_with_expiry_check`, matching `UNLINK` at `:293`.
3. Add the already-expired `-2` guard to `EXPIRETIME`/`PEXPIRETIME`
   (`expiry.rs:738-744` / `:782-788`), matching `TTL`/`PTTL` at `:603`/`:656`.
4. Assert no orphan is left in `expiry_index` after any of these paths deletes a key.

## Acceptance criteria

- [ ] A `core/tests/shard_driver/` scenario sets a key with a 1 ms TTL, sleeps past it, issues
      each of PERSIST/RENAME/RENAMENX/TYPE/EXISTS/EXPIRETIME **without** `tick_expiry`, and
      asserts the command observes the key as gone (`0`/`none`/`-2`/error). Fails today.
- [ ] After `PERSIST` on an expired key, `tick_expiry` runs and `EXISTS == 0`.
- [ ] `expiry_index_check()` reports no orphan after `PERSIST` on an expired key.
- [ ] Each of the five commands has its own case, so a partial fix cannot pass.

## Test boundary

Level 3 (`core/tests/shard_driver/`) — needs the real store's expiry index and the shard's
expiry tick; a leaked `HashMapStore` has neither. Not level 4: no socket, connection or
routing behaviour is involved, so a server integration test would only add latency.

## Depends on

issue 22 (theme T4 — expiry not consistently checked before reads),
`.scratch/testing-improvements-round2/issues/`; issue 29 (decision D1 — home for
command-semantics tests), `.scratch/testing-improvements-round2/issues/`
