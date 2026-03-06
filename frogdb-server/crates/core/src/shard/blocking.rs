use std::time::Instant;

use bytes::Bytes;
use frogdb_protocol::Response;
use tokio::sync::oneshot;

use crate::store::Store;
use crate::types::{BlockingOp, Direction};

use super::helpers::format_xread_response;
use super::wait_queue::WaitEntry;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Handle a blocking wait request.
    pub(crate) fn handle_block_wait(
        &mut self,
        conn_id: u64,
        keys: Vec<Bytes>,
        op: BlockingOp,
        response_tx: oneshot::Sender<Response>,
        deadline: Option<Instant>,
    ) {
        let keys_count = keys.len();
        let entry = WaitEntry {
            conn_id,
            keys,
            op,
            response_tx,
            deadline,
        };

        if let Err(e) = self.wait_queue.register(entry) {
            tracing::warn!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                error = %e,
                "Failed to register blocking wait"
            );
            // The response_tx was moved into entry, so we can't send an error back here.
            // The client will timeout.
        } else {
            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id,
                keys_count,
                "Client blocked on keys"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Handle unregistering a blocking wait (disconnect or explicit cancel).
    pub(crate) fn handle_unregister_wait(&mut self, conn_id: u64) {
        let removed = self.wait_queue.unregister(conn_id);
        if !removed.is_empty() {
            tracing::trace!(
                shard_id = self.shard_id(),
                conn_id = conn_id,
                count = removed.len(),
                "Unregistered blocking waits on disconnect"
            );

            // Update blocked clients metric
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Check for expired blocking waits and send nil responses.
    pub(crate) fn check_waiter_timeouts(&mut self) {
        let now = Instant::now();
        let expired = self.wait_queue.collect_expired(now);

        if !expired.is_empty() {
            let shard_label = self.shard_id().to_string();

            for entry in expired {
                tracing::trace!(
                    shard_id = self.shard_id(),
                    conn_id = entry.conn_id,
                    "Blocking wait timed out"
                );

                // Send nil response for timeout
                let _ = entry.response_tx.send(Response::Null);

                // Increment timeout counter
                self.observability.metrics_recorder.increment_counter(
                    "frogdb_blocked_timeout_total",
                    1,
                    &[("shard", &shard_label)],
                );
            }

            // Update blocked clients gauge
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Check if a list key has non-empty data.
    fn list_is_non_empty(&self, key: &Bytes) -> bool {
        if let Some(value) = self.store.get(key)
            && let Some(list) = value.as_list()
        {
            return !list.is_empty();
        }
        false
    }

    /// Check if a sorted set key has non-empty data.
    fn zset_is_non_empty(&self, key: &Bytes) -> bool {
        if let Some(value) = self.store.get(key)
            && let Some(zset) = value.as_sorted_set()
        {
            return !zset.is_empty();
        }
        false
    }

    /// Clean up an empty list key.
    fn cleanup_empty_list(&mut self, key: &Bytes) {
        if let Some(value) = self.store.get(key)
            && let Some(list) = value.as_list()
            && list.is_empty()
        {
            self.store.delete(key);
        }
    }

    /// Clean up an empty sorted set key.
    fn cleanup_empty_zset(&mut self, key: &Bytes) {
        if let Some(value) = self.store.get(key)
            && let Some(zset) = value.as_sorted_set()
            && zset.is_empty()
        {
            self.store.delete(key);
        }
    }

    /// Try to satisfy list waiters after a list write operation.
    ///
    /// Called after LPUSH, RPUSH, LPUSHX, RPUSHX operations.
    pub fn try_satisfy_list_waiters(&mut self, key: &Bytes) {
        while self.wait_queue.has_waiters(key) {
            // Check if the list has data
            let has_data = self.list_is_non_empty(key);

            if !has_data {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::BLPop => {
                    // Pop from left and return [key, value]
                    if let Some(value) = self
                        .store
                        .get_mut(key)
                        .and_then(|v| v.as_list_mut())
                        .and_then(|l| l.pop_front())
                    {
                        // Clean up empty list
                        self.cleanup_empty_list(key);
                        self.increment_version();
                        Response::Array(vec![Response::bulk(key.clone()), Response::bulk(value)])
                    } else {
                        continue; // List became empty, try next waiter
                    }
                }
                BlockingOp::BRPop => {
                    // Pop from right and return [key, value]
                    if let Some(value) = self
                        .store
                        .get_mut(key)
                        .and_then(|v| v.as_list_mut())
                        .and_then(|l| l.pop_back())
                    {
                        // Clean up empty list
                        self.cleanup_empty_list(key);
                        self.increment_version();
                        Response::Array(vec![Response::bulk(key.clone()), Response::bulk(value)])
                    } else {
                        continue;
                    }
                }
                BlockingOp::BLMove {
                    dest,
                    src_dir,
                    dest_dir,
                } => {
                    // Pop from source direction
                    let value = match src_dir {
                        Direction::Left => self
                            .store
                            .get_mut(key)
                            .and_then(|v| v.as_list_mut())
                            .and_then(|l| l.pop_front()),
                        Direction::Right => self
                            .store
                            .get_mut(key)
                            .and_then(|v| v.as_list_mut())
                            .and_then(|l| l.pop_back()),
                    };

                    if let Some(value) = value {
                        // Clean up empty source list
                        self.cleanup_empty_list(key);

                        // Push to destination
                        // Get or create dest list
                        if self.store.get(dest).is_none() {
                            self.store.set(dest.clone(), crate::types::Value::list());
                        }

                        if let Some(dest_list) =
                            self.store.get_mut(dest).and_then(|v| v.as_list_mut())
                        {
                            match dest_dir {
                                Direction::Left => dest_list.push_front(value.clone()),
                                Direction::Right => dest_list.push_back(value.clone()),
                            }
                        }

                        self.increment_version();
                        Response::bulk(value)
                    } else {
                        continue;
                    }
                }
                BlockingOp::BLMPop { direction, count } => {
                    let mut elements = Vec::new();
                    if let Some(list) = self.store.get_mut(key).and_then(|v| v.as_list_mut()) {
                        for _ in 0..*count {
                            let elem = match direction {
                                Direction::Left => list.pop_front(),
                                Direction::Right => list.pop_back(),
                            };
                            match elem {
                                Some(e) => elements.push(Response::bulk(e)),
                                None => break,
                            }
                        }
                    }

                    if elements.is_empty() {
                        continue;
                    }

                    // Clean up empty list
                    self.cleanup_empty_list(key);

                    self.increment_version();
                    Response::Array(vec![Response::bulk(key.clone()), Response::Array(elements)])
                }
                _ => continue, // Not a list operation
            };

            // Calculate wait duration (approximate since we don't track start time)
            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Try to satisfy sorted set waiters after a sorted set write operation.
    ///
    /// Called after ZADD operations.
    pub fn try_satisfy_zset_waiters(&mut self, key: &Bytes) {
        while self.wait_queue.has_waiters(key) {
            // Check if the zset has data
            let has_data = self.zset_is_non_empty(key);

            if !has_data {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::BZPopMin => {
                    // Pop minimum element
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                    {
                        let popped = zset.pop_min(1);
                        let is_empty = zset.is_empty();
                        if let Some((member, score)) = popped.into_iter().next() {
                            // Clean up empty zset
                            if is_empty {
                                self.store.delete(key);
                            }
                            self.increment_version();
                            Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ])
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                BlockingOp::BZPopMax => {
                    // Pop maximum element
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                    {
                        let popped = zset.pop_max(1);
                        let is_empty = zset.is_empty();
                        if let Some((member, score)) = popped.into_iter().next() {
                            // Clean up empty zset
                            if is_empty {
                                self.store.delete(key);
                            }
                            self.increment_version();
                            Response::Array(vec![
                                Response::bulk(key.clone()),
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ])
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }
                BlockingOp::BZMPop { min, count } => {
                    let mut elements = Vec::new();
                    if let Some(zset) = self.store.get_mut(key).and_then(|v| v.as_sorted_set_mut())
                    {
                        let popped = if *min {
                            zset.pop_min(*count)
                        } else {
                            zset.pop_max(*count)
                        };
                        for (member, score) in popped {
                            elements.push(Response::Array(vec![
                                Response::bulk(member),
                                Response::bulk(Bytes::from(score.to_string())),
                            ]));
                        }
                    }

                    if elements.is_empty() {
                        continue;
                    }

                    // Clean up empty zset
                    self.cleanup_empty_zset(key);

                    self.increment_version();
                    Response::Array(vec![Response::bulk(key.clone()), Response::Array(elements)])
                }
                _ => continue, // Not a zset operation
            };

            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Try to satisfy stream waiters after a stream write operation.
    ///
    /// Called after XADD operations.
    pub fn try_satisfy_stream_waiters(&mut self, key: &Bytes) {
        while self.wait_queue.has_waiters(key) {
            // Check if the stream exists
            let stream_exists = self
                .store
                .get(key)
                .map(|v| v.as_stream().is_some())
                .unwrap_or(false);
            if !stream_exists {
                break;
            }

            // Pop the oldest waiter
            let entry = match self.wait_queue.pop_oldest_waiter(key) {
                Some(e) => e,
                None => break,
            };

            // Execute the blocking operation
            let response = match &entry.op {
                BlockingOp::XRead { after_ids, count } => {
                    // Find key index and read after that ID
                    let key_idx = entry.keys.iter().position(|k| k == key).unwrap_or(0);
                    let after_id = &after_ids[key_idx];

                    // Read entries from stream
                    let entries: Vec<crate::types::StreamEntry> = match self.store.get(key) {
                        Some(value) => match value.as_stream() {
                            Some(stream) => stream.read_after(after_id, *count),
                            None => Vec::new(),
                        },
                        None => Vec::new(),
                    };

                    if entries.is_empty() {
                        // No new entries yet, continue to next waiter
                        continue;
                    }

                    // Format: [[key, [[id, [field, value, ...]], ...]]]
                    format_xread_response(key, &entries)
                }

                BlockingOp::XReadGroup {
                    group,
                    consumer,
                    noack,
                    count,
                } => {
                    // Read new entries and update PEL
                    let result: Option<Vec<crate::types::StreamEntry>> =
                        self.read_group_entries(key, group, consumer, *noack, *count);
                    match result {
                        Some(entries) if !entries.is_empty() => {
                            format_xread_response(key, &entries)
                        }
                        _ => continue,
                    }
                }

                _ => continue, // Not a stream operation
            };

            tracing::debug!(
                shard_id = self.shard_id(),
                conn_id = entry.conn_id,
                "Blocked client unblocked"
            );

            // Send response
            let _ = entry.response_tx.send(response);

            // Increment satisfied counter
            let shard_label = self.shard_id().to_string();
            self.observability.metrics_recorder.increment_counter(
                "frogdb_blocked_satisfied_total",
                1,
                &[("shard", &shard_label)],
            );

            // Update blocked clients gauge
            self.observability.metrics_recorder.record_gauge(
                "frogdb_blocked_clients",
                self.wait_queue.waiter_count() as f64,
                &[("shard", &shard_label)],
            );
        }
    }

    /// Read entries for XREADGROUP and update group state.
    fn read_group_entries(
        &mut self,
        key: &Bytes,
        group_name: &Bytes,
        consumer_name: &Bytes,
        noack: bool,
        count: Option<usize>,
    ) -> Option<Vec<crate::types::StreamEntry>> {
        let stream = self.store.get_mut(key)?.as_stream_mut()?;
        let group = stream.get_group_mut(group_name)?;

        let last_delivered = group.last_delivered_id;
        let new_entries = stream.read_after(&last_delivered, count);

        if new_entries.is_empty() {
            return None;
        }

        // Update last_delivered_id and add to PEL
        if let Some(last) = new_entries.last() {
            let group = stream.get_group_mut(group_name)?;
            group.last_delivered_id = last.id;

            if !noack {
                for entry in &new_entries {
                    group.add_pending(entry.id, consumer_name.clone());
                }
            }
        }

        Some(new_entries)
    }
}
