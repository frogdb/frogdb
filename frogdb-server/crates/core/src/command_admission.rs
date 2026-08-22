//! The admission chokepoint every command execution passes through.
//!
//! Before issue 13 there were two unrelated pre-execution gauntlets. Plain
//! dispatch and EXEC-queued commands went through `execute_command_body`
//! (`shard/execution.rs`), which owns the `maxmemory` gate; a script's
//! `redis.call` went through [`crate::scripting::gate::ScriptCommandGate`],
//! which had no OOM policy at all — so a Lua script could run `SET`/`APPEND`
//! unbounded while the instance sat over `maxmemory` under `noeviction`.
//!
//! This module is the one home for that admission policy. It is deliberately
//! **pure**: it reads the command's per-invocation flags plus the origin's
//! state and returns a verdict. Actually *performing* a gate — running
//! evictions, consulting the WAL — stays with the caller, because only the
//! shard worker can do those and only it can `await`. What must not be
//! duplicated is the *decision*, and that lives here.
//!
//! # The OOM policy, and why the two origins differ
//!
//! FrogDB follows Redis (`script.c`, `scriptPrepareForRun` / `scriptVerifyOOM`):
//!
//! - **Direct** dispatch: every `DENYOOM` command clears the `maxmemory` gate
//!   before it runs. The gate keys off `DENYOOM`, not `WRITE`: a write that can
//!   only free memory (DEL, LPOP, the expiry family) must stay available exactly
//!   when the instance is over its limit.
//! - **From a script with a shebang** (`#!lua flags=...`): the whole invocation
//!   is admitted once, at script start, by the shard worker — rejected up front
//!   with OOM unless the script declares `allow-oom` or `no-writes`. Having paid
//!   that toll, its sub-commands are [`ScriptOomState::exempt`]: Redis sets
//!   `SCRIPT_ALLOW_OOM` on the run context for exactly these scripts, so no
//!   per-call re-check happens.
//! - **From a shebang-less script** (Redis's `SCRIPT_FLAG_EVAL_COMPAT_MODE`):
//!   there is no script-start rejection — the server cannot know whether the
//!   body writes. Instead each `DENYOOM` sub-command is gated, using the OOM
//!   state sampled *at script start* (Redis's `server.pre_command_oom_state`)
//!   and only until the script's first write: once a script has written, Redis
//!   will not stop it half-way, so [`ScriptOomState::write_dirty`] releases the
//!   gate. This is why a shebang-less script that only reads or only calls DEL
//!   still succeeds while over the limit.
//!
//! # Extending it
//!
//! [`AdmissionRequest`] carries the execution origin precisely so an
//! origin-specific gate can be added without touching a single call site. The
//! `noscript` gate (redis-feel issue 17) is the next one: refuse a command
//! carrying `CommandFlags::NOSCRIPT` when the origin is
//! [`ExecOrigin::FromScript`]. It is not implemented here yet.

use crate::command::CommandFlags;
use crate::error::CommandError;

/// The script-execution state the admission decision reads for a `redis.call`.
///
/// Built once per script invocation by the shard worker (the only party that
/// can sample the memory state and parse the shebang) and carried by the
/// running [`crate::scripting::gate::ScriptCommandGate`], which owns the
/// write-dirty flag.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScriptOomState {
    /// The invocation already cleared the `maxmemory` gate as a whole, so its
    /// sub-commands are not gated individually.
    ///
    /// True for every shebang script (its start-time admission is the toll) and
    /// for every function (FUNCTION LOAD requires a shebang). False only in
    /// Redis's backwards-compatibility mode: a shebang-less EVAL/EVALSHA body.
    pub exempt: bool,
    /// Whether the instance was over `maxmemory` when the script started —
    /// Redis's `server.pre_command_oom_state`. Sampled once, after any
    /// eviction pass, so a long script's verdict does not flap mid-run.
    pub over_limit_at_start: bool,
    /// Whether the script has already performed a write. Redis refuses to stop
    /// a script in the middle, so the OOM gate stops applying once the first
    /// write lands.
    pub write_dirty: bool,
}

impl ScriptOomState {
    /// A sub-command that some other shard already admitted.
    ///
    /// The cross-shard leg (`ShardWorker::execute_script_sub_command`) is a
    /// *continuation* of a `redis.call` that [`admit_command`] already ruled on,
    /// at the shard running the Lua VM. Re-deciding here would judge it against
    /// a different shard's memory, and could refuse a call the script was
    /// already told would run. It still passes through the chokepoint so the
    /// seam lint's "every execution path reaches admission" invariant holds
    /// literally.
    pub fn already_admitted() -> Self {
        Self {
            exempt: true,
            over_limit_at_start: false,
            write_dirty: false,
        }
    }
}

/// Where the command being admitted came from.
///
/// The dimension an origin-specific gate keys off. Adding a variant is how a
/// new execution path announces itself to the policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecOrigin {
    /// Plain connection dispatch, or an EXEC-queued command.
    Direct,
    /// A `redis.call` / `redis.pcall` issued from inside a running script or
    /// function.
    FromScript(ScriptOomState),
}

/// One command's admission inputs.
///
/// A struct rather than positional arguments so a new policy dimension can be
/// added without rewriting every call site.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionRequest<'a> {
    /// The command's name, uppercased. Not read by the current policy; it is
    /// the identity an origin-specific gate names in its refusal (issue 17's
    /// `noscript`), and it keeps the request self-describing at call sites.
    pub name: &'a str,
    /// The flags governing *this* invocation — `Command::flags_for(args)`, so a
    /// container command is judged by its matched subcommand rather than by the
    /// union of everything the container can do.
    pub flags: CommandFlags,
    /// Where the execution came from.
    pub origin: ExecOrigin,
}

/// The chokepoint's verdict.
#[derive(Debug, Clone)]
pub enum Admission {
    /// The command may run.
    Run {
        /// The caller must still clear the `maxmemory` gate for this command
        /// (`ShardWorker::check_memory_for_write`, which may evict and is
        /// `async`, so it cannot happen here). Only ever set for
        /// [`ExecOrigin::Direct`]: a scripted call's memory verdict is already
        /// decided by the time this returns.
        memory_gate: bool,
    },
    /// Refuse before the command runs.
    Refused(CommandError),
}

/// Decide whether a command may run, and which gates the caller must still
/// clear for it.
///
/// This is the only admission policy in the tree; `scripts/command-admission.py`
/// pins the call sites that reach it.
pub fn admit_command(request: &AdmissionRequest<'_>) -> Admission {
    // Issue 17's `noscript` gate slots in here: refuse when
    // `matches!(request.origin, ExecOrigin::FromScript(_))` and the flags carry
    // `NOSCRIPT`. Deliberately not implemented yet.

    // The `maxmemory` gate keys off `DENYOOM`, not `WRITE` (Redis's
    // `CMD_DENYOOM` in `processCommand`): a write that can only free memory
    // must stay available precisely when the operator needs it to recover.
    let denies_oom = request.flags.contains(CommandFlags::DENYOOM);

    match request.origin {
        ExecOrigin::Direct => Admission::Run {
            memory_gate: denies_oom,
        },
        ExecOrigin::FromScript(state) => {
            if denies_oom && !state.exempt && !state.write_dirty && state.over_limit_at_start {
                Admission::Refused(CommandError::OutOfMemory)
            } else {
                Admission::Run { memory_gate: false }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Collapse a verdict to something comparable: `Ok(memory_gate)` when the
    /// command may run, `Err(message)` when it is refused. `Admission` cannot
    /// derive `PartialEq` because `CommandError` does not.
    fn verdict(flags: CommandFlags, origin: ExecOrigin) -> Result<bool, String> {
        let request = AdmissionRequest {
            name: "SET",
            flags,
            origin,
        };
        match admit_command(&request) {
            Admission::Run { memory_gate } => Ok(memory_gate),
            Admission::Refused(err) => Err(err.to_string()),
        }
    }

    const OOM: &str = "OOM command not allowed when used memory > 'maxmemory'.";

    fn compat(over_limit: bool, write_dirty: bool) -> ExecOrigin {
        ExecOrigin::FromScript(ScriptOomState {
            exempt: false,
            over_limit_at_start: over_limit,
            write_dirty,
        })
    }

    #[test]
    fn direct_denyoom_command_defers_the_memory_gate_to_the_caller() {
        assert_eq!(
            verdict(
                CommandFlags::WRITE | CommandFlags::DENYOOM,
                ExecOrigin::Direct
            ),
            Ok(true)
        );
    }

    #[test]
    fn direct_memory_freeing_write_is_not_memory_gated() {
        // DEL is WRITE but not DENYOOM: it must stay available over the limit.
        assert_eq!(verdict(CommandFlags::WRITE, ExecOrigin::Direct), Ok(false));
    }

    #[test]
    fn shebang_less_script_write_is_refused_when_over_limit_at_start() {
        assert_eq!(
            verdict(
                CommandFlags::WRITE | CommandFlags::DENYOOM,
                compat(true, false)
            ),
            Err(OOM.to_string())
        );
    }

    #[test]
    fn shebang_less_script_read_survives_the_limit() {
        assert_eq!(
            verdict(CommandFlags::READONLY, compat(true, false)),
            Ok(false)
        );
    }

    #[test]
    fn shebang_less_script_memory_freeing_write_survives_the_limit() {
        // Matches Redis: a compat-mode script calling only DEL runs fine.
        assert_eq!(verdict(CommandFlags::WRITE, compat(true, false)), Ok(false));
    }

    #[test]
    fn a_script_that_already_wrote_is_never_stopped_mid_way() {
        assert_eq!(
            verdict(
                CommandFlags::WRITE | CommandFlags::DENYOOM,
                compat(true, true)
            ),
            Ok(false)
        );
    }

    #[test]
    fn shebang_script_sub_commands_are_exempt() {
        let origin = ExecOrigin::FromScript(ScriptOomState {
            exempt: true,
            over_limit_at_start: true,
            write_dirty: false,
        });
        assert_eq!(
            verdict(CommandFlags::WRITE | CommandFlags::DENYOOM, origin),
            Ok(false)
        );
    }

    #[test]
    fn a_cross_shard_continuation_is_not_re_judged() {
        let origin = ExecOrigin::FromScript(ScriptOomState::already_admitted());
        assert_eq!(
            verdict(CommandFlags::WRITE | CommandFlags::DENYOOM, origin),
            Ok(false)
        );
    }

    #[test]
    fn under_the_limit_a_scripted_write_runs() {
        assert_eq!(
            verdict(
                CommandFlags::WRITE | CommandFlags::DENYOOM,
                compat(false, false)
            ),
            Ok(false)
        );
    }
}
