//! Pre-execution checks and validation guards.
//!
//! This module handles command pre-checks before routing:
//! - `is_blocking_command` - Check if command uses blocking execution
//! - `is_allowed_in_pubsub_mode` - Check if command is allowed in pub/sub mode
//! - `is_auth_exempt` - Check if command is exempt from authentication
//! - `validate_channel_access` - ACL channel permission check
//! - `run_pre_checks` - Combined pre-execution validation
//! - `is_cluster_exempt` - Check if command bypasses cluster slot validation
//! - `validate_cluster_slots` - Slot ownership validation for cluster mode

use bytes::Bytes;
use frogdb_core::{slot_for_key, CommandFlags, ConnectionLevelOp, ExecutionStrategy};
use frogdb_protocol::{ParsedCommand, Response};
use tracing::warn;

use crate::connection::util::extract_subcommand;
use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Check if a command is a blocking command.
    pub(crate) fn is_blocking_command(&self, cmd_name: &str) -> bool {
        self.registry
            .get_entry(cmd_name)
            .is_some_and(|entry| matches!(entry.execution_strategy(), ExecutionStrategy::Blocking { .. }))
    }

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
                    "NOPERM this user has no permissions to access one of the channels used as arguments"
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

        // Block admin commands on regular port when admin port is enabled
        if self.admin_enabled && !self.is_admin {
            if let Some(cmd_info) = self.registry.get(cmd_name) {
                if cmd_info.flags().contains(CommandFlags::ADMIN) {
                    return Some(Response::error(
                        "NOADMIN Admin commands are disabled on this port. Use the admin port."
                    ));
                }
            }
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
                self.acl_manager.log().log_command_denied(&user.username, &client_info, &log_cmd);
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
        if matches!(
            cmd_name,
            "CLUSTER" | "PING" | "COMMAND" | "TIME" | "DEBUG"
        ) {
            return true;
        }
        // Connection-level commands and scatter-gather commands are exempt
        self.registry.get_entry(cmd_name).is_some_and(|entry| {
            matches!(
                entry.execution_strategy(),
                ExecutionStrategy::ConnectionLevel(_) | ExecutionStrategy::ScatterGather { .. }
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
                // We own this slot - check for migration
                if let Some(migration) = snapshot.migrations.get(&first_slot) {
                    // Slot is being migrated
                    if !self.state.asking {
                        // Client needs to follow the migration
                        if let Some(target_node) = snapshot.nodes.get(&migration.target_node) {
                            return Some(Response::error(format!(
                                "ASK {} {}:{}",
                                first_slot,
                                target_node.addr.ip(),
                                target_node.addr.port()
                            )));
                        }
                    }
                    // Clear ASKING flag after use
                    self.state.asking = false;
                }
                None // We own it and no migration issues
            }
            Some(&owner) => {
                // Another node owns this slot
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
                // Slot not assigned
                Some(Response::error(format!(
                    "CLUSTERDOWN Hash slot {} not served",
                    first_slot
                )))
            }
        }
    }
}
