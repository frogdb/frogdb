//! Shard routing logic for command execution.
//!
//! This module handles routing commands to the appropriate shard(s):
//! - `route_and_execute` - Main routing logic with ACL key checks and scatter-gather
//! - `execute_cross_shard_copy` - Two-phase cross-shard COPY operation
//! - `execute_on_shard` - Send a command to a specific shard

use std::sync::Arc;

#[cfg(feature = "turmoil")]
use crate::config::ChaosConfigExt;

use bytes::Bytes;
use frogdb_core::{ScatterOp, ShardMessage, shard_for_key};
use frogdb_protocol::{ParsedCommand, Response};
use tokio::sync::oneshot;
use tracing::Instrument;

use crate::connection::ConnectionHandler;
use crate::connection::util::{extract_subcommand, key_access_type_for_flags};
use crate::scatter::{ScatterGatherExecutor, strategy_for_op};
use crate::server::next_txid;

impl ConnectionHandler {
    /// Route command to appropriate shard and execute.
    ///
    /// `cmd_name` is the precomputed uppercase command name to avoid redundant allocations.
    pub(crate) async fn route_and_execute(
        &self,
        cmd: &Arc<ParsedCommand>,
        cmd_name: &str,
    ) -> Response {
        // Lookup command
        let handler = match self.core.registry.get(cmd_name) {
            Some(h) => h,
            None => {
                return Response::error(format!(
                    "ERR unknown command '{}', with args beginning with:",
                    cmd_name
                ));
            }
        };

        // Validate arity
        if !handler.arity().check(cmd.args.len()) {
            return Response::error(format!(
                "ERR wrong number of arguments for '{}' command",
                handler.name().to_ascii_lowercase()
            ));
        }

        // Extract keys for routing
        let keys = handler.keys(&cmd.args);

        // Check key permissions with command context
        if let Some(user) = self.state.auth.user()
            && !keys.is_empty()
        {
            let access_type = key_access_type_for_flags(handler.flags());
            let subcommand = extract_subcommand(cmd_name, &cmd.args);
            for key in &keys {
                if !user.check_command_with_key(cmd_name, subcommand.as_deref(), key, access_type) {
                    let client_info =
                        format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                    let key_str = String::from_utf8_lossy(key);
                    self.core.acl_manager.log().log_key_denied(
                        &user.username,
                        &client_info,
                        &key_str,
                    );
                    return Response::error(
                        "NOPERM this user has no permissions to access one of the keys used as arguments",
                    );
                }
            }
        }

        // Keyless commands: execute on local shard
        if keys.is_empty() {
            return self.execute_on_shard(self.shard_id, Arc::clone(cmd)).await;
        }

        // Single-key command: route to owner shard
        if keys.len() == 1 {
            let target_shard = shard_for_key(keys[0], self.num_shards);

            // Chaos injection: check for shard unavailability or errors on single-key commands.
            #[cfg(feature = "turmoil")]
            {
                if self.chaos_config.is_shard_unavailable(target_shard) {
                    return Response::error("ERR shard unavailable");
                }
                if let Some(err_msg) = self.chaos_config.get_shard_error(target_shard) {
                    return Response::error(err_msg.to_string());
                }
                self.chaos_config
                    .apply_delay(self.chaos_config.single_shard_delay_ms)
                    .await;
            }

            return self.execute_on_shard(target_shard, Arc::clone(cmd)).await;
        }

        // Multi-key command: check if all keys are on the same shard
        let first_shard = shard_for_key(keys[0], self.num_shards);
        let all_same_shard = keys[1..]
            .iter()
            .all(|key| shard_for_key(key, self.num_shards) == first_shard);

        if all_same_shard {
            // All keys on same shard - execute directly
            return self.execute_on_shard(first_shard, Arc::clone(cmd)).await;
        }

        // Keys span multiple shards
        // Check if command requires same slot (like MSETNX)
        if handler.requires_same_slot() {
            return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
        }

        // Check if cross-slot is allowed
        if !self.allow_cross_slot {
            return Response::error("CROSSSLOT Keys in request don't hash to the same slot");
        }

        // Special handling for COPY - it's a two-phase operation (read + write)
        if cmd_name == "COPY" {
            return self.execute_cross_shard_copy(&cmd.args).await;
        }

        // Determine the scatter operation based on command name
        let scatter_op = match cmd_name {
            "MGET" => Some(ScatterOp::MGet),
            "MSET" => {
                // Build pairs from args
                let pairs: Vec<(Bytes, Bytes)> = cmd
                    .args
                    .chunks(2)
                    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
                    .collect();
                Some(ScatterOp::MSet { pairs })
            }
            "DEL" => Some(ScatterOp::Del),
            "EXISTS" => Some(ScatterOp::Exists),
            "TOUCH" => Some(ScatterOp::Touch),
            "UNLINK" => Some(ScatterOp::Unlink),
            _ => None,
        };

        match scatter_op.and_then(|op| strategy_for_op(&op)) {
            Some(strategy) => {
                let executor = ScatterGatherExecutor::new(
                    self.core.shard_senders.clone(),
                    self.scatter_gather_timeout,
                    self.observability.metrics_recorder.clone(),
                    self.state.id,
                    #[cfg(feature = "turmoil")]
                    self.chaos_config.clone(),
                );
                if self
                    .per_request_spans
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    executor
                        .execute(strategy.as_ref(), &cmd.args)
                        .instrument(tracing::info_span!("scatter_gather"))
                        .await
                } else {
                    executor.execute(strategy.as_ref(), &cmd.args).await
                }
            }
            None => {
                // Command doesn't support scatter-gather
                Response::error("CROSSSLOT Keys in request don't hash to the same slot")
            }
        }
    }

    /// Execute a cross-shard COPY operation.
    /// This is a two-phase operation: read from source shard, write to destination shard.
    pub(crate) async fn execute_cross_shard_copy(&self, args: &[Bytes]) -> Response {
        if args.len() < 2 {
            return Response::error("ERR wrong number of arguments for 'copy' command");
        }

        let source = &args[0];
        let dest = &args[1];

        // Parse optional arguments
        let mut replace = false;
        let mut i = 2;
        while i < args.len() {
            let arg = args[i].to_ascii_uppercase();
            match arg.as_slice() {
                b"REPLACE" => {
                    replace = true;
                    i += 1;
                }
                b"DB" => {
                    return Response::error(
                        "ERR COPY is not supported with DB. FrogDB uses a single database per instance.",
                    );
                }
                _ => {
                    return Response::error(format!(
                        "ERR Unknown option: {}",
                        String::from_utf8_lossy(&arg)
                    ));
                }
            }
        }

        let source_shard = shard_for_key(source, self.num_shards);
        let dest_shard = shard_for_key(dest, self.num_shards);

        // Phase 1: Read from source shard using ScatterOp::Copy
        let (tx1, rx1) = oneshot::channel();
        let copy_request = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![source.clone()],
            operation: ScatterOp::Copy {
                source_key: source.clone(),
            },
            conn_id: self.state.id,
            response_tx: tx1,
        };

        if self.core.shard_senders[source_shard]
            .send(copy_request)
            .await
            .is_err()
        {
            return Response::error("ERR source shard unavailable");
        }

        // Await response from source shard
        let source_result = match tokio::time::timeout(self.scatter_gather_timeout, rx1).await {
            Ok(Ok(partial)) => partial,
            Ok(Err(_)) => return Response::error("ERR source shard dropped request"),
            Err(_) => return Response::error("ERR scatter-gather timeout"),
        };

        // Parse the source shard response
        let source_data = source_result.results.into_iter().next();
        let (value_type, value_data, expiry_ms) = match source_data {
            Some((_, Response::Array(arr))) if arr.len() == 3 => {
                // Extract type, data, and expiry from the response
                let type_bytes = match &arr[0] {
                    Response::Bulk(Some(b)) => b.clone(),
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                let data_bytes = match &arr[1] {
                    Response::Bulk(Some(b)) => b.clone(),
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                let expiry = match &arr[2] {
                    Response::Integer(ms) => Some(*ms),
                    Response::Null | Response::Bulk(None) => None,
                    _ => return Response::error("ERR invalid response from source shard"),
                };
                (type_bytes, data_bytes, expiry)
            }
            Some((_, Response::Null)) | Some((_, Response::Bulk(None))) => {
                // Source key doesn't exist
                return Response::Integer(0);
            }
            _ => return Response::error("ERR invalid response from source shard"),
        };

        // Phase 2: Write to destination shard using ScatterOp::CopySet
        let (tx2, rx2) = oneshot::channel();
        let copy_set_request = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: vec![dest.clone()],
            operation: ScatterOp::CopySet {
                dest_key: dest.clone(),
                value_type,
                value_data,
                expiry_ms,
                replace,
            },
            conn_id: self.state.id,
            response_tx: tx2,
        };

        if self.core.shard_senders[dest_shard]
            .send(copy_set_request)
            .await
            .is_err()
        {
            return Response::error("ERR destination shard unavailable");
        }

        // Await response from destination shard
        let dest_result = match tokio::time::timeout(self.scatter_gather_timeout, rx2).await {
            Ok(Ok(partial)) => partial,
            Ok(Err(_)) => return Response::error("ERR destination shard dropped request"),
            Err(_) => return Response::error("ERR scatter-gather timeout"),
        };

        // Return the response from the destination shard
        match dest_result.results.into_iter().next() {
            Some((_, response)) => response,
            None => Response::error("ERR no response from destination shard"),
        }
    }

    /// Execute command on a specific shard.
    pub(crate) async fn execute_on_shard(
        &self,
        shard_id: usize,
        cmd: Arc<ParsedCommand>,
    ) -> Response {
        if self
            .per_request_spans
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            self.execute_on_shard_inner(shard_id, cmd)
                .instrument(tracing::info_span!("shard_roundtrip", shard_id))
                .await
        } else {
            self.execute_on_shard_inner(shard_id, cmd).await
        }
    }

    /// Inner implementation of shard execution (channel send + response wait).
    async fn execute_on_shard_inner(&self, shard_id: usize, cmd: Arc<ParsedCommand>) -> Response {
        let (response_tx, response_rx) = oneshot::channel();

        let msg = ShardMessage::Execute {
            command: cmd,
            conn_id: self.state.id,
            txid: None, // Single-shard operations don't need txid
            protocol_version: self.state.protocol_version,
            track_reads: self.pending_track_reads,
            response_tx,
        };

        // Send to shard
        if self.core.shard_senders[shard_id].send(msg).await.is_err() {
            return Response::error("ERR shard unavailable");
        }

        // Await response
        match response_rx.await {
            Ok(response) => response,
            Err(_) => Response::error("ERR shard dropped request"),
        }
    }
}
