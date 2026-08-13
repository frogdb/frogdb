# No MISCONF: a server that cannot persist keeps acknowledging writes

Status: done
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
`specs/persistence.md` (FM-PERSISTENCE-005 and
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

## Resolution: option 2 — opt-in latch, default off

Decided by the maintainer, on the reasoning the issue itself lays out: FrogDB's durability is the
WAL, so a failed `BGSAVE` risks backup staleness and not acknowledged writes. Refusal is therefore
a parity checkbox for operators arriving with Redis-conditioned monitoring and runbooks, not a
safety need — which makes off the only defensible default.

### The knob

`snapshot.stop-writes-on-save-error` (`CONFIG` name `stop-writes-on-save-error`), boolean, default
`false`, **live-mutable**. Redis' spirit, FrogDB's vocabulary: the artifact here is a *snapshot*,
not an RDB, and the failure it latches on is any failed save rather than specifically a background
one. Live-mutable because an operator diagnosing a broken backup disk should be able to arm the
refusal — or disarm it, mid-incident, without a restart — and because the check is a live read of
the coordinator rather than a decision frozen when the save failed.

### The refusal

Armed only by *both* halves: the flag on, and the snapshot coordinator's last save failed. The
write path then answers

```
-MISCONF Errors writing snapshots. Refusing writes because snapshot.stop-writes-on-save-error is
on. Acknowledged writes are still durable in the WAL; backups are not. Check rdb_last_bgsave_error
in INFO persistence and disk space, then BGSAVE.
```

Redis' `-MISCONF` prefix, so existing client-side matching fires; the rest is honest about FrogDB —
telling an operator their acknowledged writes are gone when the WAL still has them would be its own
bug. The next successful save clears the latch.

`SnapshotCoordinator::last_save_failed` reads a `SaveHistory` that owns both the failure bool and
the `SnapshotStats` behind it, so `last_save_failed()` and `stats().last_error.is_some()` cannot
drift: the same two methods write both. The bool is an `AtomicBool`, keeping the write path off the
stats lock.

### Where it sits in the gauntlet

`run_pre_checks` (`connection/guards.rs`), between the READONLY replica check and CLUSTERDOWN, on
commands carrying `CommandFlags::WRITE`. That is Redis' relative order (MISCONF before NOREPLICAS)
with one deliberate difference: FrogDB answers `-READONLY` on a replica first, because a replica
that cannot snapshot is still a replica and the client's next move is to find the primary. Reads
never reach the check. Neither does replication traffic — the replica apply path never builds a
`PreDispatchView`, which is FrogDB's structural equivalent of Redis' "unless the write came from
our master" clause.

Spec: FM-PERSISTENCE-046 (new), with the FM-005 / FM-022 deviation rows rewritten from "FrogDB has
no equivalent" to "opt-in, default off". Forcing tests:
`failed_save_refuses_writes_when_stop_writes_on_save_error_is_on` and
`snapshot_failure_does_not_refuse_writes_under_the_default` (e2e, both on issue 03's
unwritable-directory fixture), `refuse_writes_on_save_error_requires_both_the_flag_and_a_failed_save`
(the two-halves truth table), `save_error_latch_mirrors_the_retained_cause` and
`noop_coordinator_never_latches_a_save_error` (the coordinator seam).
