# 42 — `GEORADIUS_RO … STORE dest` writes and DELETES `dest` — unlogged, unreplicated, accepted on replicas

Status: needs-triage

## What to build

`GeoradiusRoCommand::execute` is a two-line adapter that forwards to the write command's body
verbatim (`frogdb-server/crates/commands/src/geo.rs:789-792`: `GeoradiusCommand.execute(ctx,
args)`); `GeoradiusbymemberRoCommand::execute` is the same shape at `:821-824`. The delegate
parses its options with `parse_georadius_options`, whose `STORE` and `STOREDIST` arms are
**unconditional** — `geo.rs:1063-1067` — while the modern grammar has had exactly the right
gate for years (`parse_geosearch_options(… allow_storedist: bool)` at `:870`, enforced at
`:987-991`). Nothing else on the path stops it: the only STORE-related validation is the
WITHCOORD/WITHDIST/WITHHASH incompatibility check at `:502-508`, which a bare `STORE dest`
passes. So `GEORADIUS_RO src lon lat r unit STORE dest` reaches `ctx.store.set(dest, …)` at
`geo.rs:553` and really writes `dest`.

**The destructive half is the sharp edge.** The `STORE` tail is not "write or do nothing" — it
is "write, *or delete the destination*". When the search matches nothing, `geo.rs:538` calls
`ctx.store.delete(&dest)` and returns `Integer(0)`; `GEORADIUSBYMEMBER_RO` has the identical
arm at `:703`, plus a third at `:670` for the missing-source case. Therefore

```
GEORADIUS_RO {g}src 0 0 1 m STORE {g}victim     # matches nothing
```

**destroys `{g}victim`** from a command whose spec (`geo.rs:771-785`) declares
`CommandFlags::READONLY`, `KeySpec::First`, `AccessSpec::Uniform`, `WalStrategy::NoOp`,
`EventSpec::NotApplicable`. Redis rejects the command outright: `georadiusroCommand` passes
`RADIUS_NOSTORE` into `georadiusGeneric`, and the `store`/`storedist` arms are guarded by
`!(flags & RADIUS_NOSTORE)`, so the token falls through to the syntax-error arm.

Blast radius, each consequence flowing from the same `READONLY` spec — and applying to the
delete exactly as it applies to the write. **The effect is never persisted**:
`core/src/shard/execution.rs:304-305` builds a `WriteCommandMeta` only when the handler carries
`CommandFlags::WRITE`, so there is no WAL record — a stored destination vanishes on restart,
and *a deleted destination comes back*. **The effect is never replicated**: the replication
broadcast is driven off that same write record (`post_execution.rs:73-110`), so the primary
loses `victim` while every replica keeps it — divergence in the direction that survives both a
failover and a restart. **A replica accepts the command**: the `-READONLY` refusal keys off
`flags.contains(CommandFlags::WRITE)` (`guards.rs:262`), so a client can delete from a
read-only replica's keyspace; `-MISCONF`, the self-fence and `min-replicas-to-write` (`:293`,
`:306`, `:328`) and the shard-side OOM check (`execution.rs:156`) are bypassed identically.
**The destination is unrouted and unauthorized**: `KeySpec::First` yields only the source, and
the `_RO` commands do not override `dynamic_keys` the way `GeoradiusCommand` does
(`geo.rs:563-575`), so ACL key permissions, cluster slot validation and `COMMAND GETKEYS` all
see one key — in cluster mode the write lands on the **source's** owner, where no client will
ever look for it. **WATCH and client-side caching miss it**: the slot version bump and
`invalidate_keys_all_modes` both hang off the write record (`post_execution.rs:322-340`,
`:686`), so a `WATCH dest` / `MULTI` / `EXEC` survives a concurrent destruction of `dest`.
**The keyspace event is dropped**: `ctx.notify_event` only deposits into `CommandEffects`, and
the deposits are discarded when no write meta is built. A lost write is a missing key; a lost
delete is a *resurrected* one, which is why the delete branch is worse than the write branch.

This is **LIVE on main today** — the chain was re-traced link by link during review with no
link failing, and `git diff` over `geo.rs` since the proposal's base is empty. The coverage gap
that let it through: `redis-regression/tests/geo_tcl.rs` ports two `_RO` tests
(`tcl_georadius_ro_simple_sorted` at `:752-770`, `tcl_georadiusbymember_ro_simple_sorted` at
`:1229-1248`) and between them exercise a single option token, the sort order `asc` at `:765`;
neither passes `STORE`, `STOREDIST`, `COUNT` or any `WITH*`. The registry gates cannot catch
this class either — `every_write_command_declares_event`
(`server/src/server/register.rs:311-323`), `every_write_command_declares_wal` (`:328-340`) and
`no_read_command_declares_reindex` (`:908-921`) check spec facts against spec facts, and here
the **spec is correct**: `GEORADIUS_RO` genuinely is a read command. The defect is that its
body is an adapter over a write command's body with no gate in between.

Fix direction (the modern parser already demonstrates it): make "may this caller use `STORE`?"
a parameter of the legacy grammar — `parse_georadius_options(args, allow_store: bool)`
mirroring `allow_storedist`, threaded through a `georadius_exec(ctx, args, allow_store)` shared
body, so the `_RO` variants get their `-ERR syntax error` for free and the write variants are
unchanged. Roughly 15 production lines; independently landable ahead of proposal 96's larger
unification. **Related, separate defect:** the same hole exists for `EVAL_RO`/`FCALL_RO`
because the script gate consults a hand-written name list rather than `CommandFlags`
(`core/src/scripting/gate.rs:236-239` → `core/src/scripting/bindings.rs:44-73`, whose entire
geo arm is `"GEOADD" | "GEOSEARCHSTORE"` at `:65-66`). That half is already tracked as
`.scratch/testing-improvements-round2/issues/open/19-parallel-command-tables-drift-from-commandspec.md`
and is **not** closed by this fix for the non-`_RO` spellings; the `allow_store` gate here does
close the `_RO` spellings on both the direct and the scripted path.

## Acceptance criteria

- [ ] `GEORADIUS_RO key lon lat r unit STORE dest`, `… STOREDIST dest`, and the same two
      options on `GEORADIUSBYMEMBER_RO`, all return `-ERR syntax error` and leave `dest`
      untouched — matching Redis's `RADIUS_NOSTORE` behavior.
- [ ] `GEORADIUS` and `GEORADIUSBYMEMBER` (the write variants) keep accepting `STORE` /
      `STOREDIST` with byte-identical replies and effects.
- [ ] Regression test `georadius_ro_rejects_store` in
      `crates/redis-regression/tests/geo_regression.rs` (which has zero STORE coverage today):
      asserts the syntax error for all four `_RO` × `STORE`/`STOREDIST` combinations.
- [ ] Regression test `georadius_ro_store_does_not_destroy_destination`: seeds `victim` with a
      known value, runs `GEORADIUS_RO src 0 0 1 m STORE victim` against a source that matches
      nothing, asserts `EXISTS victim == 1` and the value is unchanged. **Fails at HEAD** —
      the key is deleted today.
- [ ] Regression test asserting the same for the missing-source arm (`geo.rs:670`) of
      `GEORADIUSBYMEMBER_RO`.
- [ ] `just test frogdb-redis-regression georadius_ro` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 96
(`.scratch/arch-deepening/proposals/96-geo-store-unification.md`), §Problem 3 (headline,
escalated from the review's A1 MAJOR); hotfix H1.

## Comments
