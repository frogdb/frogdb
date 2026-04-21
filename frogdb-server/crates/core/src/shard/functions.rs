use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};
use frogdb_scripting::FunctionFlags;

use crate::command::CommandContext;
use crate::store::Store;
use crate::sync::RwLockExt;

use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle FCALL/FCALL_RO - execute a function.
    pub(crate) fn handle_function_call(
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
            let registry = match &self.scripting.function_registry {
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
        if function.flags.contains(FunctionFlags::NO_CLUSTER)
            && self.cluster.cluster_state.is_some()
        {
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

        // Execute the function using the script executor
        let executor = match &mut self.scripting.executor {
            Some(e) => e,
            None => {
                return Response::error("ERR Scripting not available");
            }
        };

        let store = &mut self.store as &mut dyn Store;
        let mut ctx = CommandContext::with_cluster(
            store,
            &self.shard_senders,
            self.identity.shard_id,
            self.identity.num_shards,
            conn_id,
            protocol_version,
            None,
            self.cluster.cluster_state.as_ref(),
            self.cluster.node_id,
            self.cluster.raft.as_ref(),
            self.cluster.network_factory.as_ref(),
            self.cluster.quorum_checker.as_ref().map(|q| q.as_ref()),
        );

        match executor.execute_function(
            &func_name,
            &library_code,
            keys,
            argv,
            &mut ctx,
            &self.registry,
            effective_read_only,
        ) {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }
}
