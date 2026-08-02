# A failed BGSAVE is invisible to every client-facing surface

Status: needs-triage
Type: bug (observability / Redis parity)
Severity: likelihood 2/3 (any snapshot failure — full disk, permissions, IO error), consequence 3/3
(an operator believes backups are running when none have succeeded for weeks) — score 6
Area: persistence / snapshot coordinator, INFO

## Problem

`BGSAVE` is fire-and-forget: the command replies `+Background saving started` the moment the
scheduler claims the epoch, and the run happens on a background task. Nothing that fails in that
task reaches a client.

1. **`INFO persistence` lies.** `rdb_last_bgsave_status` is the string literal `"ok"`
   (`frogdb-server/crates/server/src/info/sections.rs:256`) with no failure state plumbed behind
   it. There is no code path that can make it report `err`. Redis flips this field to `err` after
   a failed background save, and monitoring built against Redis reads it. The legacy handler has
   the same literal (`frogdb-server/crates/server/src/commands/info.rs:298`).
2. **No MISCONF.** Redis' `stop-writes-on-bgsave-error` (default `yes`) refuses every write with
   `-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on
   disk` once a background save has failed. FrogDB has no equivalent: writes keep being
   acknowledged indefinitely against a server that cannot persist. The only refusal FrogDB offers
   is per-write `-IOERR` under the non-default `wal-failure-policy = rollback`, which covers
   failures the shard observes at write time — not a persistently broken disk observed by the
   snapshot task.
3. **No counters.** There is a `SnapshotFailures`-style log line, but no failure counter or
   timestamp reaches `INFO`, so even a careful operator has only the process log.

What *is* honest: `LASTSAVE` does not advance on a failed save (pinned by
`lastsave_tracks_real_bgsave_and_ignores_failed_saves`). So the only accurate signal is a
*negative* one — a timestamp that stops moving — while the positive signal next to it says `ok`.

Current behavior pinned by FM-PERSISTENCE-022 in
`.scratch/hardening/specs/persistence-failure-modes.md`.

## Candidate fixes

1. **Minimum: stop lying.** Track the last snapshot outcome in the coordinator (it already owns
   `last_save_time`), surface it through the existing `PersistenceSnapshot` struct the INFO handler
   builds, and report `ok`/`err`. Add `rdb_saves` and a failure counter while the plumbing is open.
   Small, no client-visible behavior change beyond a field telling the truth.
2. **Redis parity: a MISCONF gate.** A `stop-writes-on-snapshot-error` config (default off, to keep
   the current behavior until an operator opts in) that latches on a snapshot failure and makes the
   write path reply `-MISCONF ...`, clearing on the next successful save. Bigger: it needs a
   shared latch the write path reads on every write, which is exactly the shape of the existing
   `WalFailurePolicy` `AtomicU8`, so the mechanism exists.

Fix 1 is worth doing regardless of whether 2 is ever wanted.
