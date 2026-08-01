# Expiry is not consistently checked before reads — dead keys are resurrected and served

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T4
Score: aggregate of 3 findings
Area: frogdb-commands / expiry + generic · frogdb-core / store · frogdb-search · frogdb-replication

## Context

Active expiry in a Redis-style engine is sampled, so a window between logical expiry and physical
reap always exists. Some read and mutate paths check it, some do not, and the ones that do not
either resurrect a dead key permanently or serve its stale content. The engine's own code shows
this is an oversight rather than a design: `UNLINK` does it correctly *with an explanatory
comment*, and `HashMapStore::persist`'s sibling `touch` has the guard while `persist` does not.

This is **one invariant, not N command fixes**: *no read or mutation may observe a key past its
logical deadline, regardless of which structure it consults*. The deliverable is a table-driven
assertion of that invariant over the paths that read the store directly, plus the two cross-surface
instances (search index, replica).

## Evidence

- **`PERSIST` / `RENAME` / `RENAMENX` / `TYPE` / `EXPIRETIME` read past logical expiry.** *(06/F1)*
  `commands/src/expiry.rs:697-700` — `PersistCommand::execute` is three lines,
  `ctx.store.persist(key)`, with no expiry check. `core/src/store/hashmap.rs:1239-1247` — `persist`
  matches only on `expires_at.is_some()`, sets it to `None` and calls `self.expiry_index.remove`,
  making the key **permanently immortal**. Sibling `touch` at `hashmap.rs:1249` is commented "Check
  and delete if expired first", proving the guard is the house style. Same class:
  `commands/src/generic.rs:95` (`RENAME` uses `store.get`), `:182` (`RENAMENX` uses
  `store.contains`), `:48` (`TYPE` uses `store.key_type`) — while `generic.rs:293` (`UNLINK`)
  correctly uses `get_with_expiry_check` *with a comment saying why*. `expiry.rs:738-744` and
  `:782-788` (`EXPIRETIME`/`PEXPIRETIME`) lack the already-expired `-2` guard that `TTL`/`PTTL`
  have at `:603`/`:656`.
- **Replica-side independent expiry plus a primary-side TTL extension diverges permanently.**
  *(14/F10)* `core/src/shard/event_loop.rs:133` — `run_active_expiry` is gated only on
  `expiry_paused` (CLIENT PAUSE) and `debug_active_expire_disabled`; there is **no `is_replica`
  check**. `rg is_replica core/src` returns only `builder.rs`, `worker.rs`, `types.rs`,
  `command.rs` and `scripting/gate.rs` — the flag never reaches the expiry path, and
  `active_expiry.rs` names only `Store` + a clock. Round-1 issue 32's test
  (`integration_replication.rs:3603 test_replica_expires_independently_not_via_del`) deliberately
  pins the *symmetric* case and explicitly tolerates either outcome mid-window (`:3684-3699`); the
  asymmetric case — replica reaps, primary then extends via `PERSIST` or a re-issued `SET k v EX n`
  — is uncovered, and no replicated command re-creates the key.
- **`FT.SEARCH` returns expired-unreaped keys with stale content.** *(10/F9)*
  `search/src/index.rs:709` — `let (key, fields) = self.extract_hit_fields(&doc);` reads from the
  tantivy document; `search()` never consults the store. De-indexing happens only on the removal
  paths (`core/src/shard/worker.rs:736` lazy purge, `:761` hash emptied, and
  `run_internal_removal_effects` for the sweep and eviction) — all *after* physical removal. So
  `GET k` returns nil while `FT.SEARCH` returns `k` with full stale field values in the same
  instant. Existing tests (`server/tests/search.rs:509-560`, `:1219-…`) test the *post-reap* state
  correctly and never assert the state between expiry and reap.

## What to fix

1. Write one table over the commands that read the store directly — PERSIST, RENAME, RENAMENX,
   TYPE, EXISTS, EXPIRETIME, PEXPIRETIME — driven at `shard_driver` with a short TTL, no
   `tick_expiry`, and an assertion that each observes the key as gone.
2. Fix the read paths to use `get_with_expiry_check` (or add the `-2` guard), matching `UNLINK`'s
   documented pattern.
3. Add the structural assertion that `expiry_index_check()` reports **no orphan** after `PERSIST`
   on an already-expired key — this is what makes the resurrection permanent.
4. Add the replica arm: primary `SET k v PX …`, stall the link so the replica reaps on its own
   clock, `PERSIST k` on the primary, heal, assert the replica returns `v` and `INFO keyspace`
   agrees on both nodes.
5. Add the search arm using the two existing DEBUG verbs (`DEBUG SET-ACTIVE-EXPIRE 0`,
   `DEBUG EXPIRE-BACKDATE`): force an index commit without reading the key and assert `FT.SEARCH`
   does not return it — or pin the divergence explicitly if it is accepted.

## Acceptance criteria

- [ ] A table test covers PERSIST/RENAME/RENAMENX/TYPE/EXISTS/EXPIRETIME/PEXPIRETIME; for each,
      the command issued after the deadline and before `tick_expiry` returns `0`/`none`/`-2`/error.
      Fails today for at least PERSIST, RENAME, RENAMENX, TYPE, EXPIRETIME.
- [ ] After `PERSIST` on a past-deadline key, `expiry_index_check()` reports no orphan and a
      subsequent `tick_expiry` yields `EXISTS == 0`.
- [ ] A two-node test asserts that a primary-side TTL extension issued after the replica has
      already reaped restores the key on the replica; both nodes agree in `INFO keyspace`.
- [ ] A search test asserts `FT.SEARCH idx *` does not return a logically-expired, unreaped key —
      or pins the accepted divergence in a named test whose comment cites this issue.

## Test boundary

**Level 3** for the command table — it needs the real store's expiry index and the shard's expiry
tick, neither of which a leaked `HashMapStore` with `num_shards = 1` has, and a socket adds
nothing. **Level 4** for the search arm, because `DEBUG SET-ACTIVE-EXPIRE` and
`DEBUG EXPIRE-BACKDATE` are connection-level verbs (it drops to level 3 if `shard_driver` gains
FT.\* support). **Level 4/5** for the replica arm — the divergence is a cross-node property and
cannot be observed at level 3.

## Depends on

Issue 01, `.scratch/testing-improvements-round2/issues/` (`shard_driver` harness extension) for the
command table. Issue 03, `.scratch/testing-improvements-round2/issues/` (injectable clock seam)
would make the TTL windows deterministic rather than sleep-based; `INFRASTRUCTURE.md` records the
expiry-scoped slice as ~30–40 of the 313 call sites and names theme T4 as its main beneficiary —
but the table above can be written without it using short TTLs plus explicit ticks. Issue 06,
`.scratch/testing-improvements-round2/issues/` (live-link fault primitive) for the controlled lag
window in the replica arm.
