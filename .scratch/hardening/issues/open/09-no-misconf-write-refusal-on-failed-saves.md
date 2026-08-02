# No MISCONF: a server that cannot persist keeps acknowledging writes

Status: ready-for-human
Type: decision (Redis parity / durability posture)
Severity: likelihood 2/3 (any persistent snapshot failure — full disk, permissions, IO error),
consequence 2/3 (writes are acknowledged against a server whose backups are broken; the operator
now *can* see it in `INFO`, but nothing stops the writes) — score 4
Area: persistence / snapshot coordinator, write path, config

## Problem

Split out of `.scratch/hardening/issues/03`, which fixed the observability half (`INFO persistence`
now reports the real save outcome) and deliberately left this half alone: it changes when a server
refuses writes, which is a product decision rather than an accuracy fix.

Redis' `stop-writes-on-bgsave-error` (default `yes`) refuses every write with
`-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk`
once a background save has failed, and clears the latch on the next successful save. FrogDB has no
equivalent: writes keep being acknowledged indefinitely against a server that cannot snapshot. The
only refusal FrogDB offers is per-write `-IOERR` under the non-default `wal-failure-policy =
rollback`, which covers failures the *shard* observes at write time — not a persistently broken
disk observed by the snapshot task.

The two systems weigh this differently, and the difference is real rather than accidental:

- In Redis the RDB/AOF *is* the durability mechanism, so a failing save means writes are not
  durable and refusing them is the conservative choice.
- In FrogDB the WAL is the durability mechanism (FM-PERSISTENCE-002/003/004) and a snapshot is a
  *backup artifact* (FM-PERSISTENCE-021). A failing `BGSAVE` therefore does not imply that
  acknowledged writes are at risk — it implies backups are broken. Refusing writes for a broken
  backup is a much bigger hammer than it is in Redis.

Recorded as a deviation in the Redis-deviations table of
`.scratch/hardening/specs/persistence-failure-modes.md` (FM-PERSISTENCE-005 and
FM-PERSISTENCE-022 rows).

## The decision to make

Whether an operator migrating from Redis should be able to get Redis' behavior, and if so what the
default is. Three shapes:

1. **Leave it.** Document the deviation (done) and rely on `rdb_last_bgsave_status:err` +
   `rdb_bgsave_failures` + the `frogdb_persistence_errors{type="snapshot"}` metric for alerting.
   Zero code. The gap is that monitoring must exist for the signal to matter.
2. **Opt-in latch, default off.** A `stop-writes-on-snapshot-error` config that latches on a
   snapshot failure and makes the write path reply `-MISCONF ...`, cleared by the next successful
   save. Preserves current behavior unless an operator asks for parity. The mechanism already
   exists in the right shape — the write path reads `WalFailurePolicy` from an `AtomicU8` on every
   write, so a second latch is the same pattern and the same cost.
3. **Opt-in latch, default on (full Redis parity).** Least surprising for a migrating operator,
   most surprising for an existing FrogDB one: a broken backup disk turns into a write outage on a
   server whose WAL is fine.

Option 2 is the recommendation, but the default is a product call, not an agent call — hence
`ready-for-human`.

## Forcing test (once decided)

An e2e test that makes the snapshot directory unwritable, runs `BGSAVE`, waits for
`rdb_last_bgsave_status:err`, and asserts the next `SET` is refused with `-MISCONF` (or accepted,
under the off default) — then that a successful save re-admits writes. The
`bgsave_failure_is_visible_in_info_persistence` test built for issue 03 already has the fixture
that breaks a save on demand.
