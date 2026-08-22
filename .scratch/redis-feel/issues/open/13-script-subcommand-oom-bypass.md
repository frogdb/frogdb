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

## Ruling (2026-08-21)

**RULING: Chokepoint refactor — both halves:**

1. **Script-start OOM pre-admission for EVAL/EVALSHA, matching Redis semantics:** when over
   `maxmemory` with noeviction, a script that may write is rejected up front with the OOM error
   before the body runs. "May write" and the escape hatches follow Redis: shebang scripts
   (`#!lua flags=...`) honor `allow-oom` and `no-writes`; shebang-less scripts are presumed
   may-write. Mirror the existing FUNCTION path (`shard/functions.rs` ALLOW_OOM check) — same
   semantics, same error text. First check whether our EVAL path parses shebang flags at all; if
   not, implement parsing (Redis 7.0 format) as part of this. Scripts declaring `no-writes` that
   then call a write command must error (Redis parity — verify exact error).
2. **Shared admission chokepoint:** refactor `core/src/scripting/gate.rs` so every scripted
   sub-command flows through a shared admission function rather than calling `handler.execute()`
   around the seam in `core/src/shard/execution.rs` (~line 180). Behavior: no mid-script OOM
   rejection (Redis doesn't re-check per call) — the chokepoint exists so admission policy has one
   home; design its signature to carry an execution-context (e.g. FromScript) so issue 17's
   noscript gate can slot in later WITHOUT implementing noscript now. Preserve the
   continuation-lock model — if routing through the seam genuinely breaks it, implement the
   chokepoint as a shared admission fn called from both paths and say so.
3. **Seam lint**: new compile-free gate pinning "every command execution path reaches the
   admission chokepoint" (or the nearest enforceable phrasing — e.g. no direct `handler.execute`
   calls outside the blessed sites). Follow existing gates' pattern (`scripts/`, wired into
   `just lint-gates`, row in `agents/seam-lints.md`). Prove pass + induced-fail.
