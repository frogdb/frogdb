//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ReplicationTracker, ShardMessage, shard_for_key};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::connection::state::BlockedState;
use crate::connection::util::convert_blocking_op;

impl ConnectionHandler {
    /// Handle a blocking command wait.
    ///
    /// This sends a BlockWait message to the shard and waits for a response.
    /// For WAIT command, uses the replication tracker instead of shard routing.
    pub(crate) async fn handle_blocking_wait(
        &mut self,
        keys: Vec<Bytes>,
        timeout: f64,
        proto_op: frogdb_protocol::BlockingOp,
    ) -> Response {
        use std::time::Instant;
        use tokio::sync::oneshot;

        // Handle WAIT command specially - it uses the replication tracker, not shard routing
        if let frogdb_protocol::BlockingOp::Wait {
            num_replicas,
            timeout_ms,
        } = proto_op
        {
            return self.handle_wait_command(num_replicas, timeout_ms).await;
        }

        // Convert protocol BlockingOp to core BlockingOp
        let op = convert_blocking_op(proto_op);

        // Determine the target shard - all keys must be on the same shard
        // (this was already validated in the command execute method)
        if keys.is_empty() {
            return Response::error("ERR No keys provided for blocking command");
        }

        let target_shard = shard_for_key(&keys[0], self.num_shards);

        // Calculate deadline
        let deadline = if timeout > 0.0 {
            Some(Instant::now() + Duration::from_secs_f64(timeout))
        } else {
            None // Block forever (until data or disconnect)
        };

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Send BlockWait message to shard
        let sender = match self.shard_senders.get(target_shard) {
            Some(s) => s,
            None => return Response::error("ERR Internal error: invalid shard"),
        };

        if sender
            .send(ShardMessage::BlockWait {
                conn_id: self.state.id,
                keys: keys.clone(),
                op,
                response_tx,
                deadline,
            })
            .await
            .is_err()
        {
            return Response::error("ERR Internal error: shard unreachable");
        }

        // Update blocked state
        self.state.blocked = Some(BlockedState {
            shard_id: target_shard,
            keys: keys.clone(),
        });
        self.client_registry
            .update_blocked_state(self.state.id, true);

        // Wait for response with timeout
        let result = if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            tokio::time::timeout(remaining, response_rx).await
        } else {
            // No timeout - wait indefinitely
            // Use a very long timeout to avoid blocking forever
            tokio::time::timeout(Duration::from_secs(86400), response_rx).await
        };

        // Clear blocked state
        self.state.blocked = None;
        self.client_registry
            .update_blocked_state(self.state.id, false);

        match result {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => {
                // Channel was dropped (shard shutdown or error)
                Response::Null
            }
            Err(_) => {
                // Timeout - send unregister and return null
                if let Some(sender) = self.shard_senders.get(target_shard) {
                    let _ = sender
                        .send(ShardMessage::UnregisterWait {
                            conn_id: self.state.id,
                        })
                        .await;
                }
                Response::Null
            }
        }
    }

    /// Handle WAIT command using the replication tracker.
    ///
    /// WAIT blocks until the specified number of replicas have acknowledged
    /// all writes up to this point, or until the timeout expires.
    pub(crate) async fn handle_wait_command(&self, num_replicas: u32, timeout_ms: u64) -> Response {
        // If no replication tracker is available, return 0 replicas
        let tracker = match &self.replication_tracker {
            Some(t) => t,
            None => {
                // No replication configured - return 0 replicas immediately
                return Response::Integer(0);
            }
        };

        // Get the current replication offset that replicas need to acknowledge
        let current_offset = tracker.current_offset();

        // If num_replicas is 0 or timeout_ms is 0, return immediately with current replica count
        // Redis behavior: timeout=0 means "check and return immediately, don't wait"
        if num_replicas == 0 || timeout_ms == 0 {
            let count = tracker.count_acked(current_offset);
            return Response::Integer(count as i64);
        }

        // Wait for replicas with timeout
        let timeout_duration = Duration::from_millis(timeout_ms);

        let wait_future = tracker.wait_for_acks(current_offset, num_replicas);
        let result = tokio::time::timeout(timeout_duration, wait_future).await;

        match result {
            Ok(count) => Response::Integer(count as i64),
            Err(_) => {
                // Timeout - return the count of replicas that did ACK
                let count = tracker.count_acked(current_offset);
                Response::Integer(count as i64)
            }
        }
    }
}
