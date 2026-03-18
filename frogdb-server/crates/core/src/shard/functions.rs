use bytes::Bytes;
use frogdb_protocol::{ProtocolVersion, Response};

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
        // Get function registry
        let registry = match &self.scripting.function_registry {
            Some(r) => r,
            None => {
                return Response::error("ERR Functions not available");
            }
        };

        // Get function from registry
        let func_name = String::from_utf8_lossy(function_name);
        let (function, library_name) = {
            let registry_guard = match registry.try_read_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry_guard.get_function(&func_name) {
                Some((func, lib_name)) => (func.clone(), lib_name.to_string()),
                None => {
                    return Response::error(format!("ERR Function not found: {}", func_name));
                }
            }
        };

        // Enforce read-only for FCALL_RO
        if read_only && !function.is_read_only() {
            return Response::error(format!(
                "ERR Can't execute a function with write flag using FCALL_RO: {}",
                func_name
            ));
        }

        // Get the library code
        let library_code = {
            let registry_guard = match registry.try_read_err() {
                Ok(r) => r,
                Err(_) => return Response::error("ERR internal lock contention"),
            };
            match registry_guard.get_library(&library_name) {
                Some(lib) => lib.code.clone(),
                None => {
                    return Response::error(format!("ERR Library not found: {}", library_name));
                }
            }
        };

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
            read_only,
        ) {
            Ok(response) => response,
            Err(e) => Response::error(e.to_string()),
        }
    }
}
