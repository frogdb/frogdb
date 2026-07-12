# 47 — Command parity remnants: BITOP empty-result, MSETEX reply shape

**Status:** proposed
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

## 2. MSETEX replies `Integer(1)`/`Integer(0)`, likely divergent

Found while writing the MSETEX restart test: FrogDB's MSETEX returns an integer where SET-family
commands reply `+OK`. MSETEX is not a canonical Redis command (verify: if it's a FrogDB
extension, the integer reply may be the intended contract — check the command's origin, docs,
and any client expectations before changing anything; if it mirrors a Redis-stack or KeyDB
command, match that implementation's reply). Resolution is: verify origin → either document the
integer contract at the spec site or flip to the verified upstream shape, with a regression test
either way. Do not change behavior without the verification step (accuracy over assumed parity).
