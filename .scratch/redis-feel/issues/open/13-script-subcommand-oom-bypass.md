# Scripted sub-commands bypass the OOM admission check

Status: needs-triage

## Origin

Found during the DENYOOM gate rewire (`aabc6632`, wave C of the redis-feel residue
round). Not a regression from that change — the bypass predates it and is unchanged
by it.

## What is wrong

The shard OOM check (`check_memory_for_write`, gated on `CommandFlags::DENYOOM` in
`frogdb-core/src/shard/execution.rs`) sits in `execute_command_body`, which plain
dispatch and EXEC-queued commands share. But scripted sub-commands invoked through
`core/src/scripting/gate.rs` call `handler.execute(...)` directly and never pass
through that seam — so a Lua script can run DENYOOM commands (SET, APPEND, ...)
unbounded while the server is over `maxmemory` with `noeviction`.

Redis's behavior: when over the memory limit, scripts that may write are rejected
up front with OOM before the script body runs (unless the script/function declares
allow-oom). Our functions path already models this (`shard/functions.rs` ALLOW_OOM
check); the EVAL/script gate path does not.

## Candidate direction

Either route scripted sub-commands through the same admission seam (preferred if it
doesn't break the continuation-lock model), or add a script-level pre-admission OOM
check mirroring `shard/functions.rs`. Also worth a seam lint if the fix creates a
chokepoint ("every command execution reaches the OOM gate").

## Acceptance

- Over limit + noeviction: EVAL invoking SET is rejected with the OOM error;
  EVAL invoking only reads/DEL succeeds (or matches Redis's script-level policy).
- Regression test in the owning crate.
