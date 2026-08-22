use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

use frogdb_types::metrics::definitions::{
    LuaScriptsCacheHits, LuaScriptsCacheMisses, LuaScriptsDuration, LuaScriptsErrors,
    LuaScriptsTotal,
};
use frogdb_types::metrics::labels::{ScriptError as ScriptErrorLabel, ScriptKind};

use crate::command::CommandContext;
use crate::registry::CommandRegistry;
use crate::scripting::{CacheDisposition, ScriptExecutor, ScriptOomPolicy, ScriptOutcome};
use crate::write_seam::WriteAdmission;

use super::worker::ShardWorker;
use crate::clock;

impl ShardWorker {
    /// Handle EVAL / EVAL_RO - execute a Lua script.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_eval_script(
        &mut self,
        script_source: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
        admission: WriteAdmission,
    ) -> Response {
        let is_cluster_mode = self.cluster.is_cluster_mode();
        self.run_script(
            ScriptKind::Eval,
            conn_id,
            protocol_version,
            admission,
            |executor| executor.eval_oom_policy(script_source, read_only),
            |executor, ctx, registry, oom_at_start| {
                executor.eval(
                    script_source,
                    keys,
                    argv,
                    ctx,
                    registry,
                    read_only,
                    is_cluster_mode,
                    oom_at_start,
                )
            },
        )
        .await
    }

    /// Handle EVALSHA / EVALSHA_RO - execute a cached Lua script by SHA.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_evalsha(
        &mut self,
        script_sha: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
        admission: WriteAdmission,
    ) -> Response {
        let is_cluster_mode = self.cluster.is_cluster_mode();
        self.run_script(
            ScriptKind::Evalsha,
            conn_id,
            protocol_version,
            admission,
            |executor| executor.evalsha_oom_policy(script_sha, read_only),
            |executor, ctx, registry, oom_at_start| {
                executor.evalsha(
                    script_sha,
                    keys,
                    argv,
                    ctx,
                    registry,
                    read_only,
                    is_cluster_mode,
                    oom_at_start,
                )
            },
        )
        .await
    }

    /// Shared EVAL/EVALSHA execution path.
    ///
    /// Builds the connection context, invokes the executor via `invoke`, and
    /// records the `frogdb_lua_scripts_*` metrics from the executor's typed
    /// [`ScriptOutcome`] — cache hit/miss comes from `outcome.disposition`
    /// and the error label from `ScriptError::metric_label()`, never from
    /// matching the formatted error string. This is the single place that
    /// owns cache-disposition + metric emission for both handlers above, so
    /// they only need to build the ctx and call into the executor.
    #[allow(clippy::too_many_arguments)]
    async fn run_script(
        &mut self,
        kind: ScriptKind,
        conn_id: u64,
        protocol_version: ProtocolVersion,
        admission: WriteAdmission,
        oom_policy: impl FnOnce(&ScriptExecutor) -> ScriptOomPolicy,
        invoke: impl FnOnce(
            &mut ScriptExecutor,
            &mut CommandContext,
            &CommandRegistry,
            bool,
        ) -> ScriptOutcome,
    ) -> Response {
        let shard_label = self.identity.shard_id().to_string();

        if !self.scripting.has_executor() {
            LuaScriptsErrors::inc(
                self.observability.metrics(),
                &shard_label,
                ScriptErrorLabel::NotAvailable,
            );
            return Response::error("ERR scripting not available");
        }

        let start = crate::clock::now();
        // Clone the registry Arc and move the executor out so that `self` is free
        // for the `command_context` builder (which borrows `&mut self`).
        let registry = std::sync::Arc::clone(&self.registry);
        let mut executor = self
            .scripting
            .take_executor()
            .expect("executor presence checked above");

        // Script-start OOM admission (redis-feel issue 13, Redis's
        // `scriptPrepareForRun`). The memory state is sampled ONCE here, after
        // any eviction pass, and drives both halves of the policy: a shebang
        // script that may write and does not declare `allow-oom` is refused
        // outright, and a shebang-less script carries the sampled state into
        // its per-sub-command gate (`crate::command_admission`). Sampling
        // rather than `check_memory_for_write` so an EVAL we do not reject does
        // not count an OOM rejection.
        let policy = oom_policy(&executor);
        let oom_at_start = self.sample_oom_state().await;
        if policy.reject_at_start && oom_at_start {
            self.scripting.set_executor(executor);
            LuaScriptsErrors::inc(
                self.observability.metrics(),
                &shard_label,
                ScriptErrorLabel::Execution,
            );
            return crate::error::CommandError::OutOfMemory.to_response();
        }

        // The shard write seam every `redis.call` this script issues is admitted
        // through (`specs/txn.md` FM-TXN-051).
        let write_seam = self.write_seam(admission);
        let (outcome, script_writes) = {
            let mut ctx = self.command_context(conn_id, protocol_version);
            ctx.write_seam = Some(write_seam);
            let outcome = invoke(&mut executor, &mut ctx, &registry, oom_at_start);
            // Drain the effective writes the script's `redis.call`s recorded
            // before the context drops; the pipeline below consumes them.
            (outcome, std::mem::take(&mut ctx.effects.script_writes))
        };
        self.scripting.set_executor(executor);

        // Route the script's effective writes through the canonical
        // write-effect pipeline (keyspace notifications, WATCH bump, tracking
        // invalidation, waiter wake, WAL, replication) — a scripted write has
        // exactly the side effects of a direct one. Runs even when the script
        // itself errored mid-way: the sub-commands that DID complete really
        // wrote (scripts are not transactions; Redis propagates completed
        // effects of a failed script the same way).
        //
        // The only abort that can leave writes behind is a *script-raised*
        // error (`redis.call` failed, Lua threw), and Redis behaves
        // identically. A *server-imposed* abort cannot: since issue 60
        // (option A) neither `lua-time-limit` nor `SCRIPT KILL` terminates a
        // script that has already written — the instruction hook stops
        // enforcing the deadline once the write-dirty flag is set
        // (`frogdb_scripting::sandbox::deadline_aborts`) and
        // `LuaVm::request_kill` returns `Unkillable`. A timeout abort is
        // therefore only ever reachable for a read-only script, which by
        // construction has no writes to drain, so "partial writes survive a
        // server-imposed abort" is a dead class rather than a policy.
        // Independently of the abort's origin: the applied set and the
        // propagated batch are the same `script_writes` vec, and
        // `run_script_write_effects` frames >1 write as one MULTI/EXEC
        // transaction, so primary and replica never disagree about what
        // landed.
        self.run_script_write_effects(script_writes, conn_id).await;

        let elapsed = clock::elapsed(start).as_secs_f64();

        match outcome.disposition {
            CacheDisposition::Hit => {
                LuaScriptsCacheHits::inc(self.observability.metrics(), &shard_label)
            }
            CacheDisposition::Miss => {
                LuaScriptsCacheMisses::inc(self.observability.metrics(), &shard_label)
            }
        }
        LuaScriptsTotal::inc(self.observability.metrics(), &shard_label, kind);
        LuaScriptsDuration::observe(self.observability.metrics(), elapsed, &shard_label, kind);

        match outcome.result {
            Ok(response) => response,
            Err(e) => {
                LuaScriptsErrors::inc(self.observability.metrics(), &shard_label, e.metric_label());
                Response::error(e.to_string())
            }
        }
    }

    /// Handle SCRIPT LOAD - load a script into the cache.
    pub(crate) fn handle_script_load(&mut self, script_source: &Bytes) -> String {
        match self.scripting.executor_mut() {
            Some(executor) => executor.load_script(script_source.clone()),
            None => String::new(),
        }
    }

    /// Handle SCRIPT EXISTS - check if scripts are cached.
    pub(crate) fn handle_script_exists(&self, shas: &[Bytes]) -> Vec<bool> {
        match self.scripting.executor() {
            Some(executor) => {
                let sha_refs: Vec<&[u8]> = shas.iter().map(|s| s.as_ref()).collect();
                executor.scripts_exist(&sha_refs)
            }
            None => vec![false; shas.len()],
        }
    }

    /// Handle SCRIPT FLUSH - clear the script cache.
    pub(crate) fn handle_script_flush(&mut self) {
        if let Some(executor) = self.scripting.executor_mut() {
            executor.flush_scripts();
        }
    }

    /// Execute a sub-command dispatched from a Lua script running on another shard.
    pub(crate) async fn execute_script_sub_command(
        &mut self,
        parts: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
    ) -> Response {
        if parts.is_empty() {
            return Response::error("ERR wrong number of arguments for redis command");
        }
        let cmd_name = String::from_utf8_lossy(&parts[0]).to_uppercase();
        let handler = match self.registry.get(&cmd_name) {
            Some(h) => h,
            None => return Response::error(format!("ERR unknown command '{}'", cmd_name)),
        };
        let args = &parts[1..];
        if let Err(msg) = crate::command_spec::check_arity(handler.name(), handler.arity(), args) {
            return Response::error(msg);
        }
        // THE admission chokepoint. This leg is a *continuation*: the shard
        // running the Lua VM already ruled on this `redis.call` (see
        // `ScriptCommandGate::dispatch`), so re-deciding here would judge it
        // against a different shard's memory and could refuse a call the script
        // was already told would run. It still passes through the chokepoint so
        // "every execution path reaches admission" holds literally, and so a
        // future origin-specific gate lands here too.
        let request = crate::command_admission::AdmissionRequest {
            name: &cmd_name,
            flags: handler.flags_for(args),
            origin: crate::command_admission::ExecOrigin::FromScript(
                crate::command_admission::ScriptOomState::already_admitted(),
            ),
        };
        if let crate::command_admission::Admission::Refused(err) =
            crate::command_admission::admit_command(&request)
        {
            return err.to_response();
        }
        // Route through the shared builder so a cross-shard script sub-command
        // sees the same cluster + replica identity as any other command on this
        // shard (previously it used the bare `new` constructor). The deposits
        // are drained as one `CommandEffects` value.
        let (result, effects) = {
            let mut ctx = self.command_context(conn_id, protocol_version);
            let result = handler.execute(&mut ctx, args);
            (result, std::mem::take(&mut ctx.effects))
        };

        // A cross-shard scripted write runs its effects on THIS shard — the
        // one that owns the key — through the same canonical pipeline as a
        // direct command (the local-shard analogue is the record accumulated
        // by `ScriptInvoker::run_local` and drained after the script).
        // `into_script_record` owns the no-op suppression rule: a
        // `write_was_noop` sub-command records nothing.
        if result.is_ok()
            && handler
                .flags()
                .contains(crate::command::CommandFlags::WRITE)
            && let Some(record) =
                effects.into_script_record(std::sync::Arc::clone(&handler), args.to_vec())
        {
            self.run_script_write_effects(vec![record], conn_id).await;
        }

        match result {
            Ok(response) => response,
            Err(err) => Response::error(err.to_string()),
        }
    }

    /// Handle SCRIPT KILL - kill the running script.
    ///
    /// A *cross-shard* script holds this shard's continuation lock, which takes
    /// the shard exclusively: killing the script without taking that lock back
    /// leaves every other connection refused until a future nobody is waiting
    /// on happens to finish. So the kill revokes the lock first, and a revoked
    /// holder counts as a killed script even on a shard whose own executor is
    /// idle — on every shard but the primary that is exactly the state a
    /// cross-shard script leaves behind.
    ///
    /// Revoking on one shard is enough to free them all: the notice goes to the
    /// script's coordinator, which abandons its work and drops the guard that
    /// holds every participant's lock. That is what lets the caller's
    /// `find_first` scatter stop at the first shard with something to say.
    pub(crate) fn handle_script_kill(&mut self) -> Result<(), String> {
        let revoked = self.vll.revoke_held_continuation();
        match self.scripting.executor() {
            Some(executor) => {
                if !executor.is_running() {
                    if revoked {
                        return Ok(());
                    }
                    return Err("NOTBUSY No scripts in execution right now.".to_string());
                }
                executor.kill_script().map_err(|e| e.to_string())
            }
            None if revoked => Ok(()),
            None => Err("ERR scripting not available".to_string()),
        }
    }
}
