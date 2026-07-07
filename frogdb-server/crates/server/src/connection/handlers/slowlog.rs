//! Slow query logging.
//!
//! This module provides the `maybe_log_slow_query` helper used by the run()
//! loop to record slow commands. The SLOWLOG command itself (GET/LEN/RESET/HELP)
//! is migrated behind the [`ConnectionCommand`] seam — see
//! [`crate::connection::observability_conn_command`].
//!
//! This helper is implemented as an extension method on `ConnectionHandler`.

use frogdb_core::{CommandFlags, ShardMessage, SlowLog};
use frogdb_protocol::ParsedCommand;

use crate::connection::ConnectionHandler;

impl ConnectionHandler {
    /// Log a slow query to the appropriate shard if threshold is exceeded.
    pub(crate) async fn maybe_log_slow_query(&self, cmd: &ParsedCommand, elapsed_us: u64) {
        // Check threshold setting
        let threshold = self.admin.config_manager.slowlog_log_slower_than();

        // -1 means disabled
        if threshold < 0 {
            return;
        }

        // Check if elapsed time exceeds threshold (0 means log all)
        if threshold > 0 && elapsed_us < threshold as u64 {
            return;
        }

        // Check if command has SKIP_SLOWLOG flag
        let cmd_name = cmd.name_uppercase();
        let cmd_name_str = String::from_utf8_lossy(&cmd_name);
        if let Some(handler) = self.core.registry.get(&cmd_name_str)
            && handler.flags().contains(CommandFlags::SKIP_SLOWLOG)
        {
            return;
        }

        // Prepare command args for logging (including command name)
        let mut command_args = vec![cmd.name.clone()];
        command_args.extend(cmd.args.iter().cloned());

        // Truncate args according to max_arg_len setting
        let max_arg_len = self.admin.config_manager.slowlog_max_arg_len();
        let truncated_args = SlowLog::truncate_args(&command_args, max_arg_len);

        // Get current max_len to propagate to shard
        let max_len = self.admin.config_manager.slowlog_max_len();

        // Get client info
        let client_addr = self.state.addr.to_string();
        let client_name = self
            .state
            .name
            .as_ref()
            .map(|n| String::from_utf8_lossy(n).to_string())
            .unwrap_or_default();

        // Send to shard 0 (or we could distribute based on some logic)
        // Using shard 0 is simplest and matches Redis behavior
        if let Some(sender) = self.core.shard_senders.first() {
            let _ = sender
                .send(ShardMessage::SlowlogAdd {
                    duration_us: elapsed_us,
                    command: truncated_args,
                    client_addr,
                    client_name,
                    max_len,
                })
                .await;
        }
    }
}
