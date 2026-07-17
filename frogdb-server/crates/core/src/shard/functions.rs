use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_scripting::FunctionFlags;

use crate::sync::RwLockExt;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle FCALL/FCALL_RO - execute a function.
    pub(crate) async fn handle_function_call(
        &mut self,
        function_name: &Bytes,
        keys: &[Bytes],
        argv: &[Bytes],
        conn_id: u64,
        protocol_version: ProtocolVersion,
        read_only: bool,
    ) -> Response {
        // Get function and library code from registry in a single scope
        // to release the immutable borrow before the OOM check.
        let func_name = String::from_utf8_lossy(function_name);
        let (function, library_code) = {
            let registry = match self.scripting.function_registry() {
                Some(r) => r,
                None => {
                    return Response::error("ERR Functions not available");
                }
            };
            let registry_guard = match registry.try_read_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };

            // Look up the function
            let (func, lib_name) = match registry_guard.get_function(&func_name) {
                Some(pair) => (pair.0.clone(), pair.1.to_string()),
                None => {
                    return Response::error(format!("ERR Function not found: {}", func_name));
                }
            };

            // Look up the library code
            let code = match registry_guard.get_library(&lib_name) {
                Some(lib) => lib.code.clone(),
                None => {
                    return Response::error(format!("ERR Library not found: {}", lib_name));
                }
            };

            (func, code)
        };

        // Enforce no-cluster flag
        if function.flags.contains(FunctionFlags::NO_CLUSTER) && self.cluster.is_cluster_mode() {
            return Response::error("ERR Can not run script on cluster, 'no-cluster' flag is set");
        }

        // Enforce read-only for FCALL_RO
        if read_only && !function.is_read_only() {
            return Response::error(format!(
                "ERR Can't execute a function with write flag using FCALL_RO: {}",
                func_name
            ));
        }

        // If the function has no-writes flag, treat execution as read-only
        // even when called via FCALL (not just FCALL_RO)
        let effective_read_only = read_only || function.is_read_only();

        // OOM enforcement: reject write functions when over memory limit,
        // unless the function has the ALLOW_OOM flag.
        if !effective_read_only
            && !function.flags.contains(FunctionFlags::ALLOW_OOM)
            && let Err(err) = self.check_memory_for_write()
        {
            return err.to_response();
        }

        // Execute the function using the script executor.
        if !self.scripting.has_executor() {
            return Response::error("ERR Scripting not available");
        }

        // Clone the registry Arc and move the executor out so that `self` is free
        // for the `command_context` builder (which borrows `&mut self`). Routing
        // through the builder means the function now observes the correct cluster
        // + replica identity (previously it saw none).
        let registry = std::sync::Arc::clone(&self.registry);
        let mut executor = self
            .scripting
            .take_executor()
            .expect("executor presence checked above");
        let (result, script_writes) = {
            let mut ctx = self.command_context(conn_id, protocol_version);
            let result = executor.execute_function(
                &func_name,
                &library_code,
                keys,
                argv,
                &mut ctx,
                &registry,
                effective_read_only,
            );
            (result, std::mem::take(&mut ctx.effects.script_writes))
        };
        self.scripting.set_executor(executor);

        // Same contract as EVAL/EVALSHA: the function's effective writes run
        // through the canonical write-effect pipeline after execution.
        self.run_script_write_effects(script_writes, conn_id).await;

        match result {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }
}
