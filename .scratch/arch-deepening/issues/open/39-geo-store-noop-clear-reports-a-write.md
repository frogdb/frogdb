# 39 — GEO `STORE` with an empty result reports a write even when nothing was deleted (WATCH abort, propagation, dirty++)

Status: needs-triage

## What to build

The GEO `STORE` "empty result ⇒ clear the destination" rule is spelled five times in
`frogdb-server/crates/commands/src/geo.rs`, and all five copies make the same mistake. Each
calls `ctx.store.delete(dest)` and correctly gates the `del` **keyspace notification** on the
returned `bool` — but gates nothing else on it. The sites: `GEOSEARCHSTORE` missing-source at
`:421-425`, `GEOSEARCHSTORE` empty-result at `:433-435`, `GEORADIUS … STORE` empty-result at
`:538-540`, `GEORADIUSBYMEMBER … STORE` missing-source at `:670-672`, and
`GEORADIUSBYMEMBER … STORE` empty-result at `:703-705`.

In the sub-case where `delete` returned `false` — the destination never existed, so **nothing
at all happened** — the command still produces a full `WriteCommandMeta`, because nothing in
`geo.rs` sets `ctx.effects.write_was_noop` (a `grep -n dirty geo.rs` finds nothing either, so
`dirty_delta` is `0` at every site). The consequences all key off that meta:
`WriteEffectKind::VersionIncrement` is `warranted` whenever `summary.dirty_delta >= 0`
(`core/src/shard/post_execution.rs:327-332`), so the destination's slot version bumps;
client-tracking invalidation fires (`:686`); `update_dirty_counter` maps the `0` delta to
exactly **one** change (`:690-699`, the `1 // Default: most write commands count as 1 dirty
change` arm); and the command is propagated to replicas and the AOF verbatim. Redis does the
opposite at every step: no `dbDelete` means no `signalModifiedKey`, no `server.dirty++`, and no
propagation.

The observable is a spurious transaction abort: `WATCH dest` … (another client runs
`GEOSEARCHSTORE dest src FROMLONLAT … BYRADIUS …` that matches nothing, with `dest` absent) …
`EXEC` returns a nil-abort in FrogDB and succeeds in Redis. Secondary observables are an
inflated `rdb_changes_since_last_save` and needless replication traffic for a command that
changed nothing. This is **LIVE on main today** for all five sites; when `delete` returns
`true` FrogDB and Redis agree, and the three *store* sites are correct on this axis (a store
always is a change — though they under-report its magnitude; see issue 43).

Fix direction: one line, in the shared clear path — `ctx.effects.write_was_noop = true` when
`delete` returned `false`. `into_write_meta` already owns the rule that a no-op write produces
no meta, and `execution.rs:299-305` documents it as the single place covering the
single-command, rollback and MULTI/EXEC paths. The pattern is already in this very file:
`GeoaddCommand` sets `write_was_noop` at `geo.rs:155-157` for exactly the same reason.
Proposal 96 folds all five clear sites into one helper, which is what makes this a one-line
fix instead of a five-line one; landing it before or after that unification is equally fine,
but a lone fix must touch all five sites.

## Acceptance criteria

- [ ] A GEO `STORE`/`STOREDIST` command whose result is empty and whose destination did not
      exist declares itself a no-op: no slot-version bump, no tracking invalidation, no
      keyspace notification, no dirty increment, no propagation.
- [ ] The `delete` returned `true` case is unchanged — the `del` notification still fires and
      the write still propagates.
- [ ] Regression test `geo_store_noop_clear_does_not_abort_watch` in
      `crates/redis-regression/tests/geo_regression.rs`: client A `WATCH dest` / `MULTI`;
      client B runs an empty-result `GEOSEARCHSTORE` into the absent `dest`; client A `EXEC`
      succeeds. **Fails at HEAD** (nil-abort today).
- [ ] The same assertion parameterised over all five sites (`GEOSEARCHSTORE` missing-source and
      empty-result, `GEORADIUS … STORE` empty-result, `GEORADIUSBYMEMBER … STORE`
      missing-source and empty-result).
- [ ] A test asserts `rdb_changes_since_last_save` from `INFO persistence` does not advance
      across a no-op clear.
- [ ] `just test frogdb-redis-regression geo_store_noop` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 96
(`.scratch/arch-deepening/proposals/96-geo-store-unification.md`), §Problem 4a (narrowed by
the review from "all eight sites wrong the same way" to the five clear sites, `delete() ==
false` sub-case only).

## Comments
