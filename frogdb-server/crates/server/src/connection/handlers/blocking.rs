//! Blocking command handlers.
//!
//! This module handles blocking commands:
//! - BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, XREAD, XREADGROUP
//! - WAIT - Wait for replication acknowledgment

use std::time::Duration;

use bytes::Bytes;
use frogdb_core::{ReplicationTracker, ShardMessage, UnblockMode, shard_for_key};
use frogdb_protocol::Response;

use crate::connection::ConnectionHandler;
use crate::connection::state::BlockedState;
use crate::connection::util::convert_blocking_op;

/// Outcome of a `handle_blocking_wait` select over the response, unblock, and
/// timeout futures.
enum WaitOutcome {
    /// The shard delivered a response (or the channel was dropped).
    Response(Response),
    /// The deadline elapsed.
    Timeout,
    /// CLIENT UNBLOCK signalled this connection.
    Unblocked(UnblockMode),
}

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

        // Defensively clear any stale CLIENT UNBLOCK signal from a previous
        // blocking command so the new wait starts fresh.
        self.admin.client_registry.reset_unblock(self.state.id);

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Send BlockWait message to shard
        let sender = match self.core.shard_senders.get(target_shard) {
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
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, true);

        // Wait for either a shard response, a CLIENT UNBLOCK signal, or the
        // timeout deadline.
        let outcome: WaitOutcome = {
            // Build a deadline future that never resolves if there is no
            // deadline. Using pending() keeps the select! branch alive
            // without artificially timing out.
            let timeout_fut = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d.into()).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::pin!(timeout_fut);

            tokio::select! {
                biased;
                // 1. Shard delivered a value (or the channel closed).
                recv = response_rx => match recv {
                    Ok(resp) => WaitOutcome::Response(resp),
                    Err(_) => WaitOutcome::Response(Response::Null),
                },
                // 2. CLIENT UNBLOCK signal fired.
                mode = self.client_handle.unblocked() => match mode {
                    Some(m) => WaitOutcome::Unblocked(m),
                    None => WaitOutcome::Response(Response::Null),
                },
                // 3. Deadline elapsed.
                _ = &mut timeout_fut => WaitOutcome::Timeout,
            }
        };

        // Clear blocked state.
        self.state.blocked = None;
        self.admin
            .client_registry
            .update_blocked_state(self.state.id, false);

        // Always clean up the unblock signal so the next BLPOP starts fresh.
        self.admin.client_registry.reset_unblock(self.state.id);

        match outcome {
            WaitOutcome::Response(resp) => resp,
            WaitOutcome::Timeout => {
                // Send unregister so the shard doesn't fire later.
                if let Some(sender) = self.core.shard_senders.get(target_shard) {
                    let _ = sender
                        .send(ShardMessage::UnregisterWait {
                            conn_id: self.state.id,
                        })
                        .await;
                }
                Response::Null
            }
            WaitOutcome::Unblocked(mode) => {
                // Cancel the shard-side waiter so it doesn't deliver a value
                // after CLIENT UNBLOCK already returned.
                if let Some(sender) = self.core.shard_senders.get(target_shard) {
                    let _ = sender
                        .send(ShardMessage::UnregisterWait {
                            conn_id: self.state.id,
                        })
                        .await;
                }
                match mode {
                    UnblockMode::Timeout => Response::Null,
                    UnblockMode::Error => {
                        Response::error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                    }
                }
            }
        }
    }

    /// Handle WAIT command using the replication tracker.
    ///
    /// WAIT blocks until the specified number of replicas have acknowledged
    /// all writes up to this point, or until the timeout expires.
    pub(crate) async fn handle_wait_command(&self, num_replicas: u32, timeout_ms: u64) -> Response {
        // If no replication tracker is available, return 0 replicas
        let tracker = match &self.cluster.replication_tracker {
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
