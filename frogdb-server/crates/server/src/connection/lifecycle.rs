//! Connection setup, teardown, and utility methods.

use std::time::Duration;

use frogdb_core::{CommandFlags, InvalidationSender, PauseMode, PubSubSender, ShardMessage};
use frogdb_protocol::Response;
use tokio::sync::mpsc;

use super::ConnectionHandler;
use super::state::{STATS_SYNC_INTERVAL_COMMANDS, STATS_SYNC_INTERVAL_MS};

impl ConnectionHandler {
    /// Ensure the pub/sub channel is initialized, returning a clone of the sender.
    /// Called lazily on the first pub/sub command to avoid allocating channels
    /// for the ~99% of connections that never use pub/sub.
    pub(crate) fn ensure_pubsub_channel(&mut self) -> PubSubSender {
        if let Some(ref tx) = self.pubsub_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.pubsub_tx = Some(tx.clone());
        self.pubsub_rx = Some(rx);
        tx
    }

    /// Ensure the invalidation channel is initialized, returning a clone of the sender.
    /// Called lazily on CLIENT TRACKING ON.
    pub(crate) fn ensure_invalidation_channel(&mut self) -> InvalidationSender {
        if let Some(ref tx) = self.invalidation_tx {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        self.invalidation_tx = Some(tx.clone());
        self.invalidation_rx = Some(rx);
        tx
    }

    /// Notify all shards that this connection is closed.
    pub(super) async fn notify_connection_closed(&mut self) {
        // Drop MONITOR subscription (auto-decrements broadcast receiver count)
        self.monitor_rx = None;

        // Abort redirect forwarding task if any
        if let Some(task) = self.redirect_task.take() {
            task.abort();
        }

        // Final stats sync before closing
        self.sync_stats_to_registry();

        // Notify shards if we had subscriptions or tracking enabled
        if self.state.pubsub.in_pubsub_mode() || self.state.tracking.enabled {
            for sender in self.core.shard_senders.iter() {
                let _ = sender
                    .send(ShardMessage::ConnectionClosed {
                        conn_id: self.state.id,
                    })
                    .await;
            }
        }

        // Unregister any blocking waits
        if let Some(ref blocked) = self.state.blocked
            && let Some(sender) = self.core.shard_senders.get(blocked.shard_id)
        {
            let _ = sender
                .send(ShardMessage::UnregisterWait {
                    conn_id: self.state.id,
                })
                .await;
        }
    }

    /// Extract PSYNC_HANDOFF parameters from responses.
    ///
    /// The PSYNC command returns a special response array to signal that
    /// the connection should be handed off to the replication handler:
    /// `[PSYNC_HANDOFF, replication_id, offset]`
    ///
    /// Returns `Some((replication_id, offset))` if handoff is needed.
    pub(super) fn extract_psync_handoff(responses: &[Response]) -> Option<(String, i64)> {
        if responses.len() != 1 {
            return None;
        }

        if let Response::Array(items) = &responses[0]
            && items.len() >= 3
        {
            // Check for PSYNC_HANDOFF marker
            if let Response::Simple(marker) = &items[0]
                && marker.as_ref() == b"PSYNC_HANDOFF"
            {
                // Extract replication_id
                let replication_id = match &items[1] {
                    Response::Bulk(Some(b)) => String::from_utf8_lossy(b).to_string(),
                    _ => return None,
                };

                // Extract offset
                let offset = match &items[2] {
                    Response::Bulk(Some(b)) => String::from_utf8_lossy(b).parse::<i64>().ok()?,
                    _ => return None,
                };

                return Some((replication_id, offset));
            }
        }

        None
    }

    /// Periodically sync local stats to the registry.
    /// Syncs every STATS_SYNC_INTERVAL_COMMANDS commands or STATS_SYNC_INTERVAL_MS milliseconds.
    pub(super) fn maybe_sync_stats(&mut self) {
        let should_sync = self.state.local_stats.commands_total >= STATS_SYNC_INTERVAL_COMMANDS
            || self.state.last_stats_sync.elapsed().as_millis() as u64 >= STATS_SYNC_INTERVAL_MS;

        if should_sync && self.state.local_stats.has_data() {
            self.sync_stats_to_registry();
        }
    }

    /// Force sync local stats to the registry.
    pub(crate) fn sync_stats_to_registry(&mut self) {
        if self.state.local_stats.has_data() {
            let delta = self.state.local_stats.to_delta();
            self.admin
                .client_registry
                .update_stats(self.state.id, &delta);
            self.state.local_stats.clear();
            self.state.last_stats_sync = std::time::Instant::now();
        }
    }

    /// Check whether a command should be blocked by the current pause state.
    ///
    /// Returns `true` if the command must wait, `false` if it's exempt or no
    /// pause is active.
    pub(crate) fn should_pause_command(&self, cmd_name: &str) -> bool {
        // Certain commands are always exempt from pause
        let is_exempt = matches!(
            cmd_name,
            "CLIENT" | "PING" | "QUIT" | "RESET" | "INFO" | "CONFIG" | "DEBUG" | "SLOWLOG"
        );

        if is_exempt {
            return false;
        }

        match self.admin.client_registry.check_pause() {
            Some(PauseMode::All) => true,
            Some(PauseMode::Write) => {
                // Get command flags to determine if this is a write/script command
                let flags = self
                    .core
                    .registry
                    .get(cmd_name)
                    .map(|h| h.flags())
                    .unwrap_or(CommandFlags::empty());

                let is_write_command = flags.contains(CommandFlags::WRITE);
                let is_script_command = flags.contains(CommandFlags::SCRIPT);
                let is_readonly_script =
                    is_script_command && flags.contains(CommandFlags::READONLY);

                // Block writes, scripts (conservatively), and special commands
                // that replicate or have write side-effects. Read-only script
                // variants (EVAL_RO, EVALSHA_RO, FCALL_RO) are exempt.
                is_write_command
                    || (is_script_command && !is_readonly_script)
                    || matches!(cmd_name, "PFCOUNT" | "PUBLISH" | "SPUBLISH")
            }
            None => false,
        }
    }

    /// Wait if the server is paused (CLIENT PAUSE).
    /// This queues commands (not drops them) by blocking until pause ends.
    /// Marks the client as PAUSED so `wait_for_blocked_clients` can observe it
    /// and CLIENT UNBLOCK correctly rejects unblocking.
    ///
    /// Called from `route_and_execute_with_transaction` after transaction-control
    /// dispatch and transaction queuing, so it only blocks commands outside MULTI.
    pub(crate) async fn wait_if_paused(&self, cmd_name: &str) {
        if !self.should_pause_command(cmd_name) {
            return;
        }

        // Mark client as paused/blocked
        self.admin
            .client_registry
            .update_paused_state(self.state.id, true);

        // Wait until pause ends or this command is no longer affected
        loop {
            if !self.should_pause_command(cmd_name) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clear paused state
        self.admin
            .client_registry
            .update_paused_state(self.state.id, false);
    }

    /// Wait if the server is paused, for a write-containing transaction (EXEC).
    /// Both PAUSE ALL and PAUSE WRITE block write transactions.
    pub(crate) async fn wait_if_paused_for_transaction(&self) {
        if self.admin.client_registry.check_pause().is_none() {
            return;
        }

        // Mark client as paused/blocked
        self.admin
            .client_registry
            .update_paused_state(self.state.id, true);

        // Wait until pause ends
        loop {
            if self.admin.client_registry.check_pause().is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Clear paused state
        self.admin
            .client_registry
            .update_paused_state(self.state.id, false);
    }

    /// Check whether a MULTI/EXEC transaction contains write commands that
    /// should be blocked by PAUSE WRITE.
    pub(crate) fn transaction_has_writes(&self, queue: &[frogdb_protocol::ParsedCommand]) -> bool {
        for cmd in queue {
            let name = cmd.name_uppercase();
            let name_str = std::str::from_utf8(&name).unwrap_or("");
            let flags = self
                .core
                .registry
                .get(name_str)
                .map(|h| h.flags())
                .unwrap_or(CommandFlags::empty());

            let is_write = flags.contains(CommandFlags::WRITE);
            let is_script = flags.contains(CommandFlags::SCRIPT);
            let is_readonly_script = is_script && flags.contains(CommandFlags::READONLY);

            if is_write
                || (is_script && !is_readonly_script)
                || matches!(name_str, "PFCOUNT" | "PUBLISH" | "SPUBLISH")
            {
                return true;
            }
        }
        false
    }
}
