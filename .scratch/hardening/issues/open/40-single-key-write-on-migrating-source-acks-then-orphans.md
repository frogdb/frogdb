# Single-key write on a migrating source acks +OK locally, then SETSLOT orphans it (acked-write loss, fault-free)

Status: ready-for-agent
Type: AFK
Origin: issue-31 jepsen forensics, 2026-08-05 ("Bug X" — slot-migration-partition suite: writes
90..120 acked and read back, then every post-finalization read returns 89; loss occurred 1.9s
BEFORE the nemesis partition started, i.e. in a healthy cluster)
Severity: likelihood 3/3 (every single-key write to a migrating slot whose key was already
migrated), consequence 3/3 (acked writes silently destroyed at handoff)
Area: Cluster / migration routing — Phase 4 scope

## Mechanism (traced in code)

- `route_with_snapshot` (`frogdb-server/crates/server/src/slot_migration/routing.rs:139-146`)
  returns `LocalServeMigrating` from ownership alone — never consults key presence.
- `check_migrating_multikey` (`connection/guards.rs:871-873`) bails on `keys.len() < 2`, so
  single-key commands never get the migrating-source presence probe.
- `migrating_ask_for_nil` (`guards.rs:181-183`) fires only on a **nil reply** — `GET` of a
  migrated key gets `-ASK`, but `SET` replies `+OK` and executes locally, recreating the key
  on the source.
- `CLUSTER SETSLOT NODE` hands the slot away; the source's recreated copy is the orphan the
  jepsen `read-orphans` checker reports.

Redis checks key presence pre-execution for ALL arities on the migrating side
(`cluster.c:1436-1461`: all-present → serve, none-present → -ASK, partial → -TRYAGAIN).
FrogDB's EXEC/batch path is already correct (`ProbeMigratingSource`); only the single-key
path lacks the probe.

## Fix direction

Drop the `keys.len() < 2` gate so single-key commands on a migrating source take the same
presence probe (absent → -ASK before execution); retire the reply-side `migrating_ask_for_nil`
hack once the pre-execution probe covers reads too. **FM-CLUSTER-028 currently documents the
buggy behavior and self-contradicts between its Observable and NOT-observable cells — rewrite
the row first (spec-first), then failing test, then fix.** The failing test shape: migrate a
key to the target, then SET it via the source; expect -ASK, not +OK; then finalize and prove
the write is not orphaned.

## Related

- Issue 31 (root-cause analysis, evidence, and the harness-side follow-ups)
- NOT the FM-CLUSTER-037 commit→apply window; NOT fixed by rework 02's barrier (writes were
  accepted seconds before finalization).
