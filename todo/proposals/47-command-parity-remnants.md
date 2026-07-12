# 47 — Command parity remnants: BITOP empty-result, MSETEX reply shape

**Status:** implemented (2026-07-12 — item 1 fixed; item 2 verified correct-as-is + pinned)
**Severity:** Minor (behavioral Redis divergences, no data loss)
**Found:** during proposal 44 phase 2 (BITOP, 2026-07-11) and the regression-test wave (MSETEX,
2026-07-12). Two independent items.

## 1. BITOP with an empty result stores `""` instead of deleting the destination

Redis bitops.c `bitopCommand`: when the computed result is zero-length, it deletes the
destination key (if it existed) and emits `del`; a destination that never existed stays absent.
FrogDB stores an empty string unconditionally (`frogdb-server/crates/commands/src/bitmap.rs` —
`ctx.store.set(destkey, "")`), so:

- `EXISTS dest` diverges (1 vs 0) after e.g. `BITOP AND dest missing1 missing2`.
- `TYPE dest` returns string for a key Redis would not have.
- The keyspace event diverges: proposal 44 deliberately pinned BITOP to `EmitsAt`/`set` because
  that matches FrogDB's actual store-empty behavior (documented at the spec site and in
  [44-keyspace-event-key-accuracy.md](44-keyspace-event-key-accuracy.md)); once the data behavior
  changes, the event must become set-or-del — flip BITOP to `EventSpec::Dynamic` with deposits,
  exactly the GEOSEARCHSTORE pattern from commit `84804282`.

Fix: delete-on-empty (gated on `store.delete()`'s bool for the `del` event), keep `set` +
store on non-empty. Update the WAL declaration if needed (delete must persist —
check how DEL-like strategies express it; `WalStrategy::PersistOrDelete`-family precedent).
Tests: empty-result deletes existing dest + emits `del`; empty-result on never-existed dest
stays absent + silent; non-empty stores + emits `set`; restart-survival of the deletion
(extends the BITOP test added by the regression wave in `integration_persistence.rs`).

**Resolution (2026-07-12):** implemented, verified against redis/unstable bitops.c
(`bitopCommand`: non-empty → `setKey` + NOTIFY_STRING `set`; empty → `dbDelete` +
NOTIFY_GENERIC `del` iff the dest existed). `execute()` now deletes on empty (the `del`
deposit gated on `store.delete()`'s bool) and stores + deposits `set` on non-empty. The spec
flipped to `EventSpec::Dynamic` (GEOSEARCHSTORE pattern, commit `84804282`); the registry
exhaustiveness test moved BITOP from the EmitsAt table to the Dynamic table. WAL: a new
`WalStrategy::PersistOrDeleteDestination(idx)` variant (resolving to
`WalAction::PersistOrDelete` on args\[idx\]) replaces `PersistDestination(1)` so the
delete-on-empty is written to the WAL instead of leaving the stale prior value authoritative
on disk. Tests: `test_bitop_empty_result_deletes_dest_emits_del`,
`test_bitop_empty_missing_dest_silent` (new), `test_bitop_notifies_destination_only`
(pre-existing, non-empty `set` path) in `integration_pubsub.rs`;
`test_bitop_empty_result_deletion_survives_restart` (new) in `integration_persistence.rs`;
unit test `wal_strategy_persist_or_delete_destination` in core.

## 2. MSETEX replies `Integer(1)`/`Integer(0)`, likely divergent

Found while writing the MSETEX restart test: FrogDB's MSETEX returns an integer where SET-family
commands reply `+OK`. MSETEX is not a canonical Redis command (verify: if it's a FrogDB
extension, the integer reply may be the intended contract — check the command's origin, docs,
and any client expectations before changing anything; if it mirrors a Redis-stack or KeyDB
command, match that implementation's reply). Resolution is: verify origin → either document the
integer contract at the spec site or flip to the verified upstream shape, with a regression test
either way. Do not change behavior without the verification step (accuracy over assumed parity).

**Resolution (2026-07-12): verified — no divergence; the integer reply IS the upstream
contract. No behavior change.** Verdict: MSETEX is a canonical Redis 8.4 command, not a FrogDB
extension. Evidence:

- FrogDB origin: commit `c16316b7` ("implement Redis 8.4 string commands: DIGEST, DELEX,
  MSETEX", 2026-03-18) introduced it explicitly as a Redis 8.4 port, removing the three
  commands from `docs/todo/COMMANDS.md`'s gap-analysis table ("Lower: DELEX/DIGEST/MSETEX
  (Redis 8.4, very new)").
- Upstream reply shape (redis/unstable): `t_string.c msetexCommand` replies `shared.cone`
  (Integer 1) after setting all pairs and `shared.czero` (Integer 0) when the NX/XX condition
  fails — never `shared.ok`. `src/commands/msetex.json`'s reply_schema documents exactly
  `0` ("No key was set (at least one key failed).") | `1` ("All the keys were set.").

FrogDB's `Integer(1)`/`Integer(0)` already matches, so the proposal's "likely divergent"
suspicion was wrong: unlike the rest of the SET family, upstream MSETEX itself replies an
integer. Documented at the spec site (`string.rs` `MsetexCommand::spec` doc comment) and
pinned by the dedicated regression test `test_msetex_reply_shape_is_integer_not_ok` in
`integration_strings.rs` (asserts Integer(1) on success — explicitly not `+OK` — and
Integer(0) on NX failure), alongside the pre-existing NX/XX tests that assert the same values
incidentally.
