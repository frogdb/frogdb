//! Pre-execution checks and validation guards.
//!
//! This module handles command pre-checks before routing:
//! - `is_allowed_in_pubsub_mode` - Check if command is allowed in pub/sub mode
//! - `is_auth_exempt` - Check if command is exempt from authentication
//! - `validate_channel_access` - ACL channel permission check
//! - `run_pre_checks` - Combined pre-execution validation
//! - `is_cluster_exempt` - Check if command bypasses cluster slot validation
//! - `validate_cluster_slots` - Slot ownership validation for cluster mode

use bytes::Bytes;
use frogdb_core::{
    CommandFlags, ConnectionLevelOp, ExecutionStrategy, ScatterOp, ShardMessage, shard_for_key,
    slot_for_key,
};
use frogdb_protocol::{ParsedCommand, Response};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tracing::warn;

use crate::connection::ConnectionHandler;
use crate::connection::next_txid;
use crate::connection::util::extract_subcommand;

impl ConnectionHandler {
    /// Check if a command is allowed in pub/sub mode.
    pub(crate) fn is_allowed_in_pubsub_mode(&self, cmd_name: &str) -> bool {
        // PING and QUIT are special cases - always allowed
        if matches!(cmd_name, "PING" | "QUIT") {
            return true;
        }
        // Commands with PubSub or ConnectionState strategy are allowed
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::PubSub)
                    | ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::ConnectionState)
            )
        })
    }

    /// Check if a command is exempt from authentication requirements.
    pub(crate) fn is_auth_exempt(&self, cmd_name: &str) -> bool {
        // PING and QUIT are always allowed
        if matches!(cmd_name, "PING" | "QUIT") {
            return true;
        }
        // Commands with Auth strategy are exempt (they handle their own auth)
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(ConnectionLevelOp::Auth)
            )
        })
    }

    /// Validate that the current user has permission to access all specified channels.
    ///
    /// Returns `Ok(())` if access is granted, or `Err(Response)` with NOPERM error
    /// if any channel is denied.
    #[allow(clippy::result_large_err)]
    pub(crate) fn validate_channel_access(&self, channels: &[Bytes]) -> Result<(), Response> {
        let Some(user) = self.state.auth.user() else {
            return Ok(());
        };

        for channel in channels {
            if !user.check_channel_access(channel) {
                let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                let channel_str = String::from_utf8_lossy(channel);
                self.acl_manager.log().log_channel_denied(
                    &user.username,
                    &client_info,
                    &channel_str,
                );
                return Err(Response::error(
                    "NOPERM this user has no permissions to access one of the channels used as arguments",
                ));
            }
        }
        Ok(())
    }

    /// Run pre-execution checks for a command.
    ///
    /// This validates:
    /// - Authentication (if required)
    /// - Admin port restrictions
    /// - ACL command permissions
    /// - Pub/sub mode restrictions
    ///
    /// Returns `Some(Response)` with an error if the command should be rejected,
    /// or `None` if the command can proceed.
    pub(crate) fn run_pre_checks(&self, cmd_name: &str, args: &[Bytes]) -> Option<Response> {
        // Check authentication
        if !self.state.auth.is_authenticated() && !self.is_auth_exempt(cmd_name) {
            return Some(Response::error("NOAUTH Authentication required."));
        }

        // Block write commands on replicas
        if self.is_replica.load(std::sync::atomic::Ordering::Relaxed)
            && let Some(cmd_impl) = self.registry.get(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
        {
            return Some(Response::error(
                "READONLY You can't write against a read only replica.",
            ));
        }

        // Self-fence: reject writes when quorum is lost in cluster mode
        if let Some(ref qc) = self.quorum_checker
            && let Some(cmd_impl) = self.registry.get(cmd_name)
            && cmd_impl.flags().contains(CommandFlags::WRITE)
            && !qc.has_quorum()
        {
            return Some(Response::error(
                "CLUSTERDOWN The cluster is down (quorum lost, writes rejected)",
            ));
        }

        // Block admin commands on regular port when admin port is enabled
        if self.admin_enabled
            && !self.is_admin
            && let Some(cmd_info) = self.registry.get(cmd_name)
            && cmd_info.flags().contains(CommandFlags::ADMIN)
        {
            return Some(Response::error(
                "NOADMIN Admin commands are disabled on this port. Use the admin port.",
            ));
        }

        // Check command ACL permission
        // Note: ACL command is exempt (users need ACL WHOAMI to check their identity)
        if let Some(user) = self.state.auth.user() {
            let subcommand = extract_subcommand(cmd_name, args);
            if cmd_name != "ACL" && !user.check_command(cmd_name, subcommand.as_deref()) {
                let client_info = format!("{}:{}", self.state.addr.ip(), self.state.addr.port());
                // Log with subcommand if present
                let log_cmd = if let Some(ref sub) = subcommand {
                    format!("{}|{}", cmd_name.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name.to_lowercase()
                };
                self.acl_manager
                    .log()
                    .log_command_denied(&user.username, &client_info, &log_cmd);
                warn!(
                    conn_id = self.state.id,
                    username = %user.username,
                    command = %log_cmd,
                    "ACL denied command"
                );
                // Return error with subcommand in message if present
                let err_cmd = if let Some(ref sub) = subcommand {
                    format!("{} {}", cmd_name.to_lowercase(), sub.to_lowercase())
                } else {
                    cmd_name.to_lowercase()
                };
                return Some(Response::error(format!(
                    "NOPERM this user has no permissions to run the '{}' command",
                    err_cmd
                )));
            }
        }

        // Check pub/sub mode restrictions
        if self.state.pubsub.in_pubsub_mode() && !self.is_allowed_in_pubsub_mode(cmd_name) {
            return Some(Response::error(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd_name
            )));
        }

        None
    }

    /// Check if a command is exempt from slot validation in cluster mode.
    /// Connection-level and admin commands that don't operate on specific keys are exempt.
    pub(crate) fn is_cluster_exempt(&self, cmd_name: &str) -> bool {
        // Certain commands are always exempt
        if matches!(cmd_name, "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG") {
            return true;
        }
        // Connection-level, scatter-gather, and server-wide commands are exempt
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(_)
                    | ExecutionStrategy::ScatterGather { .. }
                    | ExecutionStrategy::ServerWide(_)
            )
        })
    }

    /// Validate slot ownership for keys in cluster mode.
    /// Returns Some(Response) if validation fails, None if OK.
    pub(crate) fn validate_cluster_slots(&mut self, cmd: &ParsedCommand) -> Option<Response> {
        // Only validate if cluster mode is enabled
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;

        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);

        // Skip cluster-exempt commands (using execution strategy for type-safe check)
        if self.is_cluster_exempt(&cmd_name) {
            return None;
        }

        // Get keys from command using the registry
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None; // Unknown command, let execute handle it
        };

        // No keys = no slot validation needed
        if keys.is_empty() {
            return None;
        }

        // Calculate slot for first key
        let first_slot = slot_for_key(keys[0]);

        // CROSSSLOT check - all keys must hash to same slot
        for key in &keys[1..] {
            let slot = slot_for_key(key);
            if slot != first_slot {
                return Some(Response::error(
                    "CROSSSLOT Keys in request don't hash to the same slot",
                ));
            }
        }

        // Check slot ownership
        let snapshot = cluster_state.snapshot();

        match snapshot.slot_assignment.get(&first_slot) {
            Some(&owner) if owner == node_id => {
                // We own this slot. During MIGRATING state (we are the source),
                // Redis serves keys that still exist locally and only returns ASK
                // for keys that have been migrated away. We let the command through
                // here and convert nil responses to ASK in the dispatch layer.
                if snapshot.migrations.contains_key(&first_slot) && self.state.asking {
                    self.state.asking = false;
                }
                None // We own it — execute locally
            }
            Some(&owner) => {
                // Another node owns this slot — but check if we're the
                // IMPORTING target for an active migration. The importing
                // node accepts commands when ASKING is set (regular clients)
                // or unconditionally for RESTORE (used by MIGRATE internally).
                if let Some(migration) = snapshot.migrations.get(&first_slot)
                    && migration.target_node == node_id
                {
                    // We are importing this slot
                    if self.state.asking || cmd_name.as_ref() == "RESTORE" {
                        self.state.asking = false;
                        return None; // Allow the command
                    }
                }
                if self.state.asking {
                    self.state.asking = false;
                }

                // READONLY mode: allow read-only commands to execute locally
                // even though we don't own the slot (replica reads).
                if self.state.readonly {
                    let is_read_cmd = self
                        .registry
                        .get(&cmd_name)
                        .is_some_and(|c| c.flags().contains(CommandFlags::READONLY));
                    if is_read_cmd {
                        return None; // Serve read locally
                    }
                }

                if let Some(owner_node) = snapshot.nodes.get(&owner) {
                    Some(Response::error(format!(
                        "MOVED {} {}:{}",
                        first_slot,
                        owner_node.addr.ip(),
                        owner_node.addr.port()
                    )))
                } else {
                    Some(Response::error(format!(
                        "CLUSTERDOWN Hash slot {} not served",
                        first_slot
                    )))
                }
            }
            None => {
                // Slot not assigned locally — check if we're importing it
                if let Some(migration) = snapshot.migrations.get(&first_slot)
                    && migration.target_node == node_id
                    && (self.state.asking || cmd_name.as_ref() == "RESTORE")
                {
                    self.state.asking = false;
                    return None; // Allow: importing node accepts during migration
                }
                if self.state.asking {
                    self.state.asking = false;
                }

                Some(Response::error(format!(
                    "CLUSTERDOWN Hash slot {} not served",
                    first_slot
                )))
            }
        }
    }

    /// For multi-key commands targeting a MIGRATING slot, check key presence
    /// and return TRYAGAIN if keys are split between source and target.
    ///
    /// Redis semantics:
    /// - All keys present locally → serve locally (None)
    /// - All keys absent → ASK redirect
    /// - Mixed presence → TRYAGAIN
    pub(crate) async fn check_migrating_multikey(&self, cmd: &ParsedCommand) -> Option<Response> {
        // Only relevant in cluster mode
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;

        // Get keys from command
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None;
        };

        // Single-key commands are handled by existing migrating_ask_for_nil
        if keys.len() < 2 {
            return None;
        }

        let slot = slot_for_key(keys[0]);

        // Check if we own the slot and it's in MIGRATING state
        let snapshot = cluster_state.snapshot();
        let owner = snapshot.slot_assignment.get(&slot)?;
        if *owner != node_id {
            return None;
        }
        let migration = snapshot.migrations.get(&slot)?;
        let target_addr = snapshot.nodes.get(&migration.target_node)?.addr;

        // Send EXISTS scatter to the owning shard to check key presence
        let shard_id = shard_for_key(keys[0], self.num_shards);
        let keys_bytes: Vec<Bytes> = keys.iter().map(|k| Bytes::copy_from_slice(k)).collect();

        let (response_tx, response_rx) = oneshot::channel();
        let msg = ShardMessage::ScatterRequest {
            request_id: next_txid(),
            keys: keys_bytes,
            operation: ScatterOp::Exists,
            conn_id: self.state.id,
            response_tx,
        };

        if self.shard_senders[shard_id].send(msg).await.is_err() {
            return Some(Response::error("ERR shard unavailable"));
        }

        let partial = match tokio::time::timeout(self.scatter_gather_timeout, response_rx).await {
            Ok(Ok(partial)) => partial,
            _ => return Some(Response::error("ERR shard unavailable")),
        };

        let mut any_present = false;
        let mut any_absent = false;
        for (_, response) in &partial.results {
            match response {
                Response::Integer(1) => any_present = true,
                Response::Integer(0) => any_absent = true,
                _ => {}
            }
            if any_present && any_absent {
                return Some(Response::error(
                    "TRYAGAIN Multiple keys request during rehashing of slot",
                ));
            }
        }

        if any_absent && !any_present {
            // All keys migrated away — ASK redirect
            return Some(Self::ask_response(slot, target_addr));
        }

        // All keys present locally — serve normally
        None
    }

    /// For a MIGRATING slot (we are the source), check if the command's response
    /// indicates the key doesn't exist locally. If so, return an ASK redirect
    /// so the client retries on the importing target.
    ///
    /// Redis behavior: during MIGRATING, existing keys are served locally;
    /// missing keys (already migrated away) get `-ASK` redirect.
    pub(crate) fn migrating_ask_for_nil(
        &self,
        cmd: &ParsedCommand,
        response: &Response,
    ) -> Option<Response> {
        // Only relevant in cluster mode
        let cluster_state = self.cluster_state.as_ref()?;
        let node_id = self.node_id?;

        // Check if response indicates "key not found"
        if !Self::is_nil_response(response) {
            return None;
        }

        // Get the first key's slot
        let cmd_name_bytes = cmd.name_uppercase();
        let cmd_name = String::from_utf8_lossy(&cmd_name_bytes);
        let keys = if let Some(cmd_impl) = self.registry.get(&cmd_name) {
            cmd_impl.keys(&cmd.args)
        } else {
            return None;
        };
        if keys.is_empty() {
            return None;
        }
        let slot = slot_for_key(keys[0]);

        // Check if we own this slot and it's in MIGRATING state
        let snapshot = cluster_state.snapshot();
        if let Some(&owner) = snapshot.slot_assignment.get(&slot)
            && owner == node_id
            && let Some(migration) = snapshot.migrations.get(&slot)
            && let Some(target_node) = snapshot.nodes.get(&migration.target_node)
        {
            return Some(Self::ask_response(slot, target_node.addr));
        }

        None
    }

    fn is_nil_response(response: &Response) -> bool {
        matches!(response, Response::Null | Response::Bulk(None))
    }

    fn ask_response(slot: u16, addr: SocketAddr) -> Response {
        Response::error(format!("ASK {} {}:{}", slot, addr.ip(), addr.port()))
    }
}

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_core::command::QuorumChecker;
    use std::sync::Arc;

    struct MockQuorumChecker {
        has_quorum: bool,
    }

    impl QuorumChecker for MockQuorumChecker {
        fn has_quorum(&self) -> bool {
            self.has_quorum
        }
        fn count_reachable_nodes(&self) -> usize {
            if self.has_quorum { 2 } else { 1 }
        }
    }

    /// Helper to create a ConnectionHandler for testing pre-checks.
    /// Uses a loopback TCP connection and minimal deps.
    async fn make_test_handler(
        quorum_checker: Option<Arc<dyn QuorumChecker>>,
    ) -> ConnectionHandler {
        use crate::connection::deps::*;
        use frogdb_core::{ClientRegistry, CommandRegistry, NoopMetricsRecorder, ShardMessage};
        use tokio::sync::mpsc;

        // Create a loopback TCP pair
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect_fut = tokio::net::TcpStream::connect(addr);
        let (stream, _) = tokio::join!(async { listener.accept().await.unwrap() }, connect_fut);
        let tcp_stream: crate::net::TcpStream = stream.0;

        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);
        let registry = Arc::new(registry);
        let (tx, _rx) = mpsc::channel::<ShardMessage>(1);
        let shard_senders = Arc::new(vec![tx]);
        let acl_manager = frogdb_core::AclManager::new(Default::default());
        let client_registry = Arc::new(ClientRegistry::new());
        let config_manager = Arc::new(crate::runtime_config::ConfigManager::new(
            &crate::config::Config::default(),
        ));
        let snapshot_coordinator: Arc<dyn frogdb_core::persistence::SnapshotCoordinator> =
            Arc::new(frogdb_core::NoopSnapshotCoordinator::new());
        let function_registry = frogdb_core::SharedFunctionRegistry::default();

        let core = CoreDeps {
            registry,
            shard_senders,
            metrics_recorder: Arc::new(NoopMetricsRecorder::new()),
            acl_manager,
        };
        let admin = AdminDeps {
            client_registry: client_registry.clone(),
            config_manager,
            snapshot_coordinator,
            function_registry,
            cursor_store: Arc::new(crate::cursor_store::AggregateCursorStore::new()),
        };
        let cluster = ClusterDeps {
            quorum_checker,
            ..ClusterDeps::default()
        };
        let config = ConnectionConfig::default_for_testing(1);
        let observability = ObservabilityDeps::default();

        let client_handle = client_registry.register(1, "127.0.0.1:9999".parse().unwrap(), None);

        ConnectionHandler::from_deps(
            tcp_stream,
            "127.0.0.1:9999".parse().unwrap(),
            1,
            0,
            client_handle,
            core,
            admin,
            cluster,
            config,
            observability,
        )
    }

    #[tokio::test]
    async fn test_self_fence_write_rejected_when_quorum_lost() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: false });
        let handler = make_test_handler(Some(qc)).await;

        let result = handler.run_pre_checks("SET", &[]);
        assert!(result.is_some());
        let resp = result.unwrap();
        match resp {
            Response::Error(msg) => {
                assert!(
                    msg.starts_with(b"CLUSTERDOWN"),
                    "expected CLUSTERDOWN error, got: {}",
                    String::from_utf8_lossy(&msg)
                );
            }
            other => panic!("expected Error response, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_self_fence_read_allowed_when_quorum_lost() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: false });
        let handler = make_test_handler(Some(qc)).await;

        let result = handler.run_pre_checks("GET", &[]);
        assert!(
            result.is_none(),
            "GET should be allowed when quorum is lost"
        );
    }

    #[tokio::test]
    async fn test_self_fence_write_allowed_when_quorum_present() {
        let qc = Arc::new(MockQuorumChecker { has_quorum: true });
        let handler = make_test_handler(Some(qc)).await;

        let result = handler.run_pre_checks("SET", &[]);
        assert!(
            result.is_none(),
            "SET should be allowed when quorum is present"
        );
    }

    #[tokio::test]
    async fn test_self_fence_no_quorum_checker_standalone() {
        let handler = make_test_handler(None).await;

        let result = handler.run_pre_checks("SET", &[]);
        assert!(
            result.is_none(),
            "SET should be allowed in standalone mode (no quorum checker)"
        );
    }
}
