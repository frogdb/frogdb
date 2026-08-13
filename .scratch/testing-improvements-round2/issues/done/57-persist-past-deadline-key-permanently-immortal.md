# `PERSIST` on a past-deadline key makes it permanently immortal (and `RENAME`/`TYPE`/`EXPIRETIME` resurrect too)

Status: done
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

- [x] A shard-driver scenario enters the expiry window and issues each of
      PERSIST/RENAME/RENAMENX/TYPE/EXISTS/EXPIRETIME **without** `tick_expiry`, asserting the
      command observes the key as gone (`0`/`none`/`-2`/error). All six failed before the fix.
      Landed as `frogdb-server/crates/shard-harness/tests/scenario_expiry_window.rs` — the
      harness moved out of `core/tests/shard_driver/` into the `frogdb-shard-harness` crate.
      The window is entered through the deterministic `DEBUG EXPIRE-BACKDATE` seam
      (`ShardDriver::backdate_expiry`, added here) instead of a 1 ms TTL + sleep: no wall-clock
      race, no flake.
- [x] After `PERSIST` on an expired key, `tick_expiry` runs and `EXISTS == 0`
      (`persist_on_expired_key_does_not_immortalize`).
- [x] `expiry_index_check()` reports no orphan after `PERSIST` on an expired key. Noted in the
      scenario as a **regression guard, not the failing signal**: the pre-fix state was
      self-consistent (PERSIST cleared the deadline *and* the index row), so the probe reported
      zero anomalies both before and after. It guards a future fix that deletes the entry but
      leaks its index row.
- [x] Each command has its own `#[test]`, so a partial fix cannot pass.

## Test boundary

Level 3 (`core/tests/shard_driver/`) — needs the real store's expiry index and the shard's
expiry tick; a leaked `HashMapStore` has neither. Not level 4: no socket, connection or
routing behaviour is involved, so a server integration test would only add latency.

## Depends on

issue 22 (theme T4 — expiry not consistently checked before reads),
`.scratch/testing-improvements-round2/issues/`; issue 29 (decision D1 — home for
command-semantics tests), `.scratch/testing-improvements-round2/issues/`

## Resolution

Confirmed as a live defect and fixed. Failure mode **FM-PERSISTENCE-044**
(`specs/persistence.md`).

### Root cause

Active expiry is sampled, so a key past its deadline stays physically present until the sweeper
reaches it. Five read paths crossed that window with no logical-expiry check. The auditing
agent's file:line evidence was accurate; the confirmed sites (and the one it missed) are:

| Site | Before |
| --- | --- |
| `frogdb-server/crates/core/src/store/hashmap.rs:1238` `HashMapStore::persist` | matched only on `expires_at.is_some()`, cleared the deadline **and** the expiry-index row — the key lost the only thing that could ever kill it, so it became *permanently* immortal (and diverged from a replica that expires independently) |
| `frogdb-server/crates/commands/src/generic.rs:97` `RENAME` | `store.get(old_key)` — no expiry check, resurrected a dead value under the new name |
| `frogdb-server/crates/commands/src/generic.rs:182,190` `RENAMENX` | `store.contains(new_key)` (spurious `0` against a dead destination) and `store.get(old_key)` |
| `frogdb-server/crates/commands/src/generic.rs:48` `TYPE` | `store.key_type(key)`, which is expiry-blind |
| `frogdb-server/crates/commands/src/basic.rs:881` `EXISTS` | `store.contains(key)` |
| `frogdb-server/crates/commands/src/expiry.rs:738,782` `EXPIRETIME`/`PEXPIRETIME` | no `expires_at <= now => -2` guard, so they replied with an absolute timestamp already in the past |
| `frogdb-server/crates/core/src/shard/execution.rs:738-741` scatter `EXISTS` | **not in the issue** — counted the keyspace hit with `exists_unexpired` but built the *reply* from `contains`, so the cross-shard EXISTS contradicted both its own accounting and the single-shard one |

`generic.rs:114,196` (`RENAME`/`RENAMENX` `get_expiry`) needed no change: they run after the
expiry-checking read, so the key is already known live.

### Fix

Smallest correct change at each site, respecting the existing destructive/non-destructive split:

- `HashMapStore::persist` opens with `check_and_delete_expired`, mirroring the sibling `touch`
  exactly. The key is retired through `uninstall`, so the expiry index settles and the lazy
  removal is reported to the worker like every other purge.
- `RENAME`/`RENAMENX` route both the source read and the destination existence probe through
  `get_with_expiry_check` — the purging variant, matching `UNLINK` at `generic.rs:293`.
- `TYPE`/`EXISTS` (and the scatter `EXISTS` arm) gate on the **non-mutating** `exists_unexpired`.
  Deliberately *not* `get_with_expiry_check`: metadata probes must not physically purge or report
  a lazy removal — pinned by the pre-existing
  `lazy_value_read_reports_purge_but_nondestructive_probes_do_not`, which stays green.
- `EXPIRETIME`/`PEXPIRETIME` gained the `expires_at <= now => -2` guard that `TTL`/`PTTL` carry.

### Tests

Core unit tests tagged `// FM-PERSISTENCE-044`, in `store::hashmap::tests`
(`frogdb-server/crates/core/src/store/hashmap.rs`):

- `persist_on_expired_key_deletes_instead_of_immortalizing` — failed pre-fix
- `persist_on_expired_key_leaves_no_expiry_index_orphan` — failed pre-fix
- `nondestructive_probes_do_not_see_a_past_deadline_key` — regression guard on the
  `exists_unexpired` seam TYPE/EXISTS now read (already correct; passed pre-fix)

Untagged: `shard::execution::scatter_effect_tests::scatter_exists_reports_hot_expired_key_as_absent`
(failed pre-fix) covers the scatter arm, which the shard driver's single-shard path cannot reach.

Level 3, `frogdb-server/crates/shard-harness/tests/scenario_expiry_window.rs` — all six failed
pre-fix: `persist_on_expired_key_does_not_immortalize`,
`rename_of_expired_source_is_no_such_key`,
`renamenx_sees_expired_source_and_destination_as_gone`, `type_of_expired_key_is_none`,
`exists_does_not_count_expired_key`, `expiretime_of_expired_key_is_minus_two`.
