# Scripted sub-commands bypass the OOM admission check

Status: done

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

## Resolution (2026-08-21)

Fixed in `1e8a08dc`.

### Shebang parsing

`parse_eval_shebang` **already existed** (`core/src/scripting/executor.rs`), handling
the Redis 7.0 `#!lua flags=...` form. Nothing to implement; the admission path
consumes it.

### The chokepoint

`core/src/command_admission.rs` (new) holds the one admission policy:

```rust
pub fn admit_command(request: &AdmissionRequest<'_>) -> Admission
```

`AdmissionRequest { name, flags, origin }`, where `flags` is `Command::flags_for(args)`
(issue 15's per-subcommand flags) and `origin` is
`ExecOrigin::Direct | ExecOrigin::FromScript(ScriptOomState)`. The verdict is
`Admission::Run { memory_gate } | Admission::Refused(CommandError)` — `memory_gate`
tells the caller it must still clear `check_memory_for_write`, because that is
`async` and may evict, so only the shard worker can perform it. The *decision* is
what must not be duplicated, and it lives in one place.

Three call sites reach it: `ShardWorker::execute_command_body`
(`shard/execution.rs`), `ScriptCommandGate::dispatch` (`scripting/gate.rs`), and
`ShardWorker::execute_script_sub_command` (`shard/scripting.rs`, the cross-shard
continuation, which passes `ScriptOomState::already_admitted()` because the VM's
shard already ruled).

**How issue 17 slots in**: `noscript` is an origin-keyed refusal —
`matches!(request.origin, ExecOrigin::FromScript(_)) && flags.contains(NOSCRIPT)`
→ `Admission::Refused`. It is a branch inside `admit_command` and touches no call
site. A marker comment names it at that spot.

### Continuation-lock impact: none

Routing scripted sub-commands through the seam in `execute_command_body` is not
possible without breaking the continuation-lock model. `ScriptInvoker::run_local`
runs synchronously while holding a `RefCell` `&mut` borrow of the store and has no
`&mut ShardWorker` to call the async path with. Per the ruling's escape clause the
chokepoint is therefore a **shared pure admission function** called from all three
paths rather than a single physical seam. `handle_script_kill`,
`vll.revoke_held_continuation()` and the `block_in_place` remote dispatch are
untouched.

### Script-start pre-admission

`ShardWorker::run_script` samples the memory state once
(`sample_oom_state()`, a non-counting probe so EVAL does not inflate
`frogdb_eviction_oom_total`) and consults the script's declared policy
(`ScriptOomPolicy::reject_at_start`). A shebang script that may write and declares
neither `allow-oom` nor `no-writes` is refused before its body runs, mirroring
`shard/functions.rs`.

### Two deviations from the ruling's parentheticals — both toward real Redis

The ruling's primary directive is "match Redis policy exactly"; two of its
parenthetical descriptions of that policy do not match `script.c`, and the
implementation follows Redis:

1. **Shebang-less scripts are not rejected at start.** Redis's
   `SCRIPT_FLAG_EVAL_COMPAT_MODE` has no start-time rejection — the server cannot
   know whether a shebang-less body writes. `scriptVerifyOOM` gates each
   `redis.call` on `CMD_DENYOOM` against `server.pre_command_oom_state`. Redis's
   own `scripting.tcl` "allow-oom shebang flag" test proves a shebang-less script
   calling `get` **succeeds** over the limit while one calling `set` fails. So the
   ruling's "shebang-less scripts are presumed may-write [and rejected up front]"
   and acceptance item (c) ("rejected even if they only DEL") would both have been
   Redis *incompatibilities*.
2. **There is therefore a per-call gate for the compat path** — contradicting the
   ruling's "no mid-script OOM rejection (Redis doesn't re-check per call)". It is
   bounded exactly as Redis bounds it: the state is sampled once at script start
   (never re-sampled mid-run) and stops applying at the script's first write
   (`ScriptOomState::write_dirty`), because Redis will not halt a script half-way.
   Shebang scripts and functions are `exempt` and never re-checked, which is the
   case the ruling was describing.

**Error text**: the start-time refusal reuses `CommandError::OutOfMemory`
("OOM command not allowed when used memory > 'maxmemory'.") per the ruling's
"same error text" as the FUNCTION path, rather than Redis's more specific
"OOM allow-oom flag is not set on the script, can not run it when used memory > 'maxmemory'".
Both carry the `OOM` prefix clients match on. Worth revisiting if verbatim
script-error parity is ever pursued.

### Seam lint

`scripts/command-admission.py`, wired into `just lint-gates`, with a row in
`agents/seam-lints.md`. Five checks: pinned `admit_command(` callers; pinned
`handler.execute(` sites; admission ordered before execution (and, in the gate,
before `mark_write` — a refused command must not buy the unkillable-script
exemption); exactly one production reader of `.contains(CommandFlags::DENYOOM)`;
and `run_script` still performing the script-start pre-admission. Stale pins are
errors too, so the pinned set stays the live set.

Proven pass plus four induced failures (a stray `handler.execute(` in another
crate; a second `DENYOOM` read; `mark_write(` hoisted above admission; removing
`sample_oom_state(` from `run_script`), each producing its intended message, with
restoration returning to OK.

### Tests

- `command_admission::tests` — 9 unit tests over the policy matrix.
- `shard::execution::oom_gate_tests` — 9 new end-to-end EVAL tests beside the
  existing direct-dispatch ones: scripted SET refused over the limit;
  `flags=allow-oom` SET succeeds; a may-write shebang refused at start;
  `no-writes` runs over the limit; `no-writes` calling SET errors with
  "Write commands are not allowed from read-only scripts"; shebang-less DEL and
  GET both survive the limit; a write-dirty script is not stopped mid-way; a
  scripted write runs under the limit.

Runs (local mode): `just check frogdb-core` clean;
`just test frogdb-core 'oom_gate|command_admission|scripted'` 26/26;
`just test frogdb-core scripting` 81/81;
`just test frogdb-core 'function|eviction|eval|maxmemory'` 60/60;
`just test frogdb-server integration_scripting` 19/19; `just lint-gates` green.
