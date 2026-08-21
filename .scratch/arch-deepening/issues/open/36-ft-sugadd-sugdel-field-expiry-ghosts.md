# 36 — FT.SUGADD / FT.SUGDEL mutate hash values without touching the field-expiry index

Status: needs-triage

## What to build

Hash field TTLs live in two books: the value-side `HashValue.field_expiries` (the only book
persistence serializes) and the store-side `FieldExpiryIndex` (derived, rebuilt by
`install`). Every handler that reaches `HashValue::set` or `HashValue::remove` must clear the
store-side index too, because both of those methods clear the value-side TTL as their first
statement — `types/src/types/hash.rs:262` (`set` → `self.remove_field_expiry(&field)`) and
`:321` (`remove`). `HSET` does the pairing (`commands/src/hash.rs:85-88`), `HDEL` does
(`:234-237`), `HGETDEL` and `HSETEX` do. The two FT suggestion commands do not.

`FtSugaddCommand::execute` takes the hash through the generic accessor at
`frogdb-server/crates/server/src/commands/search.rs:906`
(`ctx.get_or_create::<HashValue>(key)`) and then calls `hash.set(...)` twice — the suggestion
itself at `:919-923` and the optional `__P__`-prefixed payload at `:929-933`. Neither call is
paired with a `Store::remove_field_expiry`. `FtSugdelCommand::execute` is worse: it reaches
the value through a **raw** `ctx.store.get_mut(key)` at `:1136` plus `as_hash_mut()` at
`:1137` — bypassing every typed accessor in the tree — and then calls `hash.remove(...)`
twice (`:1144`, `:1150`), again with no index maintenance. Because neither command purges
elapsed fields on the way in either, an already-elapsed field is also counted in SUGADD's
`Integer(count)` reply (`:936-940`) and reported as removable by SUGDEL.

This is the same class as the HMSET ghost documented in proposal 93 §Problem 2, and it is
**LIVE on main today**, scoped to keys used as FT suggestion dictionaries. A user who sets a
field TTL on such a key (`HEXPIRE dict 100 FIELDS 1 term`) and then runs `FT.SUGADD dict term
1.0` leaves an index entry with a deadline the value no longer backs. That ghost is immortal:
`purge_expired_hash_fields` removes index entries only for fields the **value** reported
expired (`core/src/store/hashmap.rs:1437-1440`), so nothing reaps it. Once enough ghosts
accumulate at the head of `by_time`, the active field sweep breaks shard-wide
(`core/src/shard/active_expiry.rs:224-236`: `purged` stays 0 → `purged_any` false → `:234`
breaks the loop), stopping field-TTL reaping for **every** key on the shard. Redis's
`hashTypeSet` without `HASH_SET_KEEP_TTL` discards the field TTL in the one and only book it
keeps, so it has no divergence to have.

Fix direction: pair each value-side write with the index clear that `HSET` already performs,
and route `FT.SUGDEL` through a typed hash accessor so it also inherits lazy purge-on-read.
The structural fix (deleting the index's public write API and reconciling from the value) is
proposal 93's job; this issue is the two-command repair that must hold until then, and the
`FT.SUGDEL` reroute is a prerequisite for 93's design (c) landing correctly — 93 §(c) notes
SUGDEL is the one hash mutator that bypasses *both* typed layers. Related but not
overlapping: `.scratch/testing-improvements-round2/issues/open/82-commands-core-types-residual-test-gaps.md`
§F3 tracks seven *core hash* commands that skip the purge; it does not name the FT pair.

## Acceptance criteria

- [ ] `HSET d f v` / `HEXPIRE d 100 FIELDS 1 f` / `FT.SUGADD d f 1.0` / `HTTL d FIELDS 1 f`
      answers `-1`, not `100` — the store-side index no longer holds a deadline the value
      does not back. Same for the `FT.SUGDEL` path via `HashValue::remove`.
- [ ] `FT.SUGADD` and `FT.SUGDEL` observe an elapsed field as absent: after a field's
      deadline passes, `FT.SUGADD`'s returned suggestion count excludes it and `FT.SUGDEL`
      of that suggestion returns `0`.
- [ ] `FT.SUGDEL` reaches its hash through a typed accessor rather than raw
      `ctx.store.get_mut` (`search.rs:1136`), so purge-on-read applies to it.
- [ ] Regression test `ft_sug_field_expiry_index_stays_consistent` in the search test suite:
      seeds a suggestion-dictionary key with a field TTL, drives SUGADD and SUGDEL, and
      asserts `HTTL` agrees with the value book both times. Fails at HEAD.
- [ ] Regression test `ft_sugadd_ghost_does_not_starve_field_sweep`: after the SUGADD path,
      one active-expiry tick still reaps a *different* key's due field (proves `purged_any`
      is no longer wedged false).
- [ ] `just test frogdb-server ft_sug` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 93
(`.scratch/arch-deepening/proposals/93-hash-field-expiry-store-api.md`), §Problem 2 table
rows FT.SUGADD / FT.SUGDEL ("same class" residue flagged for separate filing).

## Comments
