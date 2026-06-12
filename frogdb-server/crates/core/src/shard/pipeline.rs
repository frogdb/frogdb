use bytes::Bytes;

use crate::command::{Command, CommandFlags, WaiterKind, WaiterWake};
use crate::store::Store;

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Run the full post-execution pipeline after a write command.
    ///
    /// Keyspace hit/miss stats are recorded earlier, at lookup level, inside
    /// `execute_command_inner`; this pipeline only handles write side effects.
    ///
    /// Pipeline order (preserves original behavior):
    /// 1. Version increment
    /// 2. Dirty counter
    /// 3. Blocking waiter satisfaction
    /// 4. WAL persistence
    /// 5. Replication broadcast
    pub(crate) async fn run_post_execution(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
        dirty_delta: i64,
        conn_id: u64,
    ) {
        let flags = handler.flags();

        // Steps below only apply to write commands
        if !flags.contains(CommandFlags::WRITE) {
            return;
        }

        // 2. Increment version — skip when dirty_delta < 0 (command signalled
        //    that it did not actually modify any data, e.g. DEL on an
        //    already-expired key).  This matches Redis behaviour where
        //    no-op writes do not dirty WATCH state.
        if dirty_delta >= 0 {
            self.increment_version();
        }

        // 2.5. Client tracking: invalidate written keys
        if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
            let keys = handler.keys(args);
            if !keys.is_empty() {
                if self.tracking.has_tracking_clients() {
                    self.tracking.invalidate_keys(&keys, conn_id);
                }
                if !self.tracking.broadcast_table.is_empty() {
                    self.tracking.broadcast_table.invalidate_matching(
                        &keys,
                        conn_id,
                        &self.tracking.invalidation_registry,
                    );
                }
            }
        }

        // 2.7. Keyspace notifications
        self.emit_keyspace_notifications_for_command(handler, args);

        // 3. Update dirty counter
        self.update_dirty_counter(dirty_delta);

        // 4. Satisfy blocking waiters (may trigger further get_mut mutations)
        self.satisfy_waiters_for_command(handler, args);

        // 4.5. Flush keysizes histogram updates from blocking waiter mutations
        self.store.flush_keysizes_refreshes();

        // 5. WAL persistence
        self.persist_by_strategy(handler, args).await;

        // 5.5. Update search indexes
        if !self.search.indexes.is_empty() {
            self.update_search_indexes(handler.name(), args);
        }

        // 6. Replication broadcast (skip if from replica to avoid loops)
        if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
            self.replication_broadcaster
                .broadcast_command(handler.name(), args);
        }
    }

    /// Post-execution pipeline for rollback mode (WAL already persisted and confirmed).
    ///
    /// Same as `run_post_execution()` but skips the `persist_by_strategy()` step
    /// because WAL persistence was already done by `persist_and_confirm()`.
    pub(crate) async fn run_post_execution_after_wal(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
        dirty_delta: i64,
        conn_id: u64,
    ) {
        let flags = handler.flags();

        if !flags.contains(CommandFlags::WRITE) {
            return;
        }

        // 2. Increment version — skip when dirty_delta < 0 (command signalled
        //    that it did not actually modify any data, e.g. DEL on an
        //    already-expired key).  This matches Redis behaviour where
        //    no-op writes do not dirty WATCH state.
        if dirty_delta >= 0 {
            self.increment_version();
        }

        // 2.5. Client tracking: invalidate written keys
        if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
            let keys = handler.keys(args);
            if !keys.is_empty() {
                if self.tracking.has_tracking_clients() {
                    self.tracking.invalidate_keys(&keys, conn_id);
                }
                if !self.tracking.broadcast_table.is_empty() {
                    self.tracking.broadcast_table.invalidate_matching(
                        &keys,
                        conn_id,
                        &self.tracking.invalidation_registry,
                    );
                }
            }
        }

        // 2.7. Keyspace notifications
        self.emit_keyspace_notifications_for_command(handler, args);

        // 3. Update dirty counter
        self.update_dirty_counter(dirty_delta);

        // 4. Satisfy blocking waiters (may trigger further get_mut mutations)
        self.satisfy_waiters_for_command(handler, args);

        // 4.5. Flush keysizes histogram updates from blocking waiter mutations
        self.store.flush_keysizes_refreshes();

        // 5. WAL persistence — SKIPPED (already done by persist_and_confirm)

        // 5.5. Update search indexes
        if !self.search.indexes.is_empty() {
            self.update_search_indexes(handler.name(), args);
        }

        // 6. Replication broadcast (skip if from replica to avoid loops)
        if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
            self.replication_broadcaster
                .broadcast_command(handler.name(), args);
        }
    }

    /// Emit keyspace hit/miss counters from lookup-level accounting.
    ///
    /// `hits`/`misses` are tallied by the executing command from actual key
    /// existence (see [`crate::command::CommandContext::record_keyspace_lookup`]),
    /// matching Redis's `lookupKeyReadWithFlags` semantics. This deliberately
    /// does not infer hit/miss from the reply shape: a nil bulk reply (e.g. GET
    /// on a missing key vs. HGET on a missing field) is ambiguous and would
    /// misclassify lookups.
    pub(super) fn record_keyspace_lookups(&self, hits: u64, misses: u64) {
        if hits > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_hits_total",
                hits,
                &[],
            );
        }
        if misses > 0 {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_misses_total",
                misses,
                &[],
            );
        }
    }

    fn update_dirty_counter(&mut self, dirty_delta: i64) {
        let dirty_amount = if dirty_delta > 0 {
            dirty_delta as u64
        } else if dirty_delta < 0 {
            0
        } else {
            1 // Default: most write commands count as 1 dirty change
        };
        self.store.increment_dirty(dirty_amount);
    }

    fn satisfy_waiters_for_command(&mut self, handler: &dyn Command, args: &[Bytes]) {
        match handler.wakes_waiters() {
            WaiterWake::None => {}
            WaiterWake::Kind(kind) => {
                let keys = handler.keys(args);
                self.satisfy_waiters(kind, &keys);
            }
            WaiterWake::All => {
                let keys = handler.keys(args);
                for kind in [WaiterKind::List, WaiterKind::SortedSet, WaiterKind::Stream] {
                    self.satisfy_waiters(kind, &keys);
                }
            }
        }
    }

    fn satisfy_waiters(&mut self, kind: WaiterKind, keys: &[&[u8]]) {
        for key in keys {
            let key_bytes = Bytes::copy_from_slice(key);
            match kind {
                WaiterKind::List => self.try_satisfy_list_waiters(&key_bytes),
                WaiterKind::SortedSet => self.try_satisfy_zset_waiters(&key_bytes),
                WaiterKind::Stream => self.try_satisfy_stream_waiters(&key_bytes),
            }
        }
    }

    /// Batched post-execution pipeline for a MULTI/EXEC transaction.
    ///
    /// Unlike `run_post_execution` (called per-command), this method processes all
    /// write commands from a transaction as a single atomic unit:
    /// - Single version increment (not one per write command)
    /// - Single dirty counter update (accumulated total)
    /// - Batched client tracking invalidation
    /// - Batched waiter satisfaction
    /// - Per-command WAL persistence (still sequential, but deferred to here)
    /// - Batched replication broadcast (wrapped in MULTI/EXEC framing)
    /// - Batched search index updates
    pub(crate) async fn run_transaction_post_execution(
        &mut self,
        write_infos: &[(&dyn Command, &[Bytes])],
        total_dirty: i64,
        conn_id: u64,
    ) {
        if write_infos.is_empty() {
            return;
        }

        // 1. Single version increment for the entire transaction
        self.increment_version();

        // 2. Client tracking: invalidate all written keys at once
        //    FLUSHDB/FLUSHALL have no keys (keyless broadcast), so they require
        //    flush_all_tracking() instead of key-based invalidation.
        if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
            let has_flush = write_infos
                .iter()
                .any(|&(handler, _)| matches!(handler.name(), "FLUSHDB" | "FLUSHALL"));
            if has_flush && self.tracking.has_tracking_clients() {
                self.tracking.flush_all_tracking();
            } else {
                let mut all_keys: Vec<&[u8]> = Vec::new();
                for &(handler, args) in write_infos {
                    all_keys.extend(handler.keys(args));
                }
                if !all_keys.is_empty() {
                    if self.tracking.has_tracking_clients() {
                        self.tracking.invalidate_keys(&all_keys, conn_id);
                    }
                    if !self.tracking.broadcast_table.is_empty() {
                        self.tracking.broadcast_table.invalidate_matching(
                            &all_keys,
                            conn_id,
                            &self.tracking.invalidation_registry,
                        );
                    }
                }
            }
        }

        // 2.7. Keyspace notifications for each write command
        for &(handler, args) in write_infos {
            self.emit_keyspace_notifications_for_command(handler, args);
        }

        // 3. Update dirty counter (accumulated total)
        self.update_dirty_counter(total_dirty);

        // 4. Satisfy blocking waiters for all relevant keys
        for &(handler, args) in write_infos {
            self.satisfy_waiters_for_command(handler, args);
        }

        // 4.5. Flush keysizes histogram updates from blocking waiter mutations
        self.store.flush_keysizes_refreshes();

        // 5. WAL persistence for each write command
        for &(handler, args) in write_infos {
            self.persist_by_strategy(handler, args).await;
        }

        // 5.5. Update search indexes for each write command
        if !self.search.indexes.is_empty() {
            for &(handler, args) in write_infos {
                self.update_search_indexes(handler.name(), args);
            }
        }

        // 6. Atomic replication broadcast (MULTI/EXEC wrapping)
        if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
            let commands: Vec<(&str, &[Bytes])> = write_infos
                .iter()
                .map(|&(handler, args)| (handler.name(), args))
                .collect();
            self.replication_broadcaster
                .broadcast_transaction(&commands);
        }
    }

    /// Batched post-execution for rollback mode (WAL already persisted and confirmed).
    ///
    /// Same as `run_transaction_post_execution` but skips WAL persistence.
    pub(crate) async fn run_transaction_post_execution_after_wal(
        &mut self,
        write_infos: &[(&dyn Command, &[Bytes])],
        total_dirty: i64,
        conn_id: u64,
    ) {
        if write_infos.is_empty() {
            return;
        }

        // 1. Single version increment
        self.increment_version();

        // 2. Client tracking invalidation (same flush-aware logic as non-WAL path)
        if self.tracking.has_tracking_clients() || !self.tracking.broadcast_table.is_empty() {
            let has_flush = write_infos
                .iter()
                .any(|&(handler, _)| matches!(handler.name(), "FLUSHDB" | "FLUSHALL"));
            if has_flush && self.tracking.has_tracking_clients() {
                self.tracking.flush_all_tracking();
            } else {
                let mut all_keys: Vec<&[u8]> = Vec::new();
                for &(handler, args) in write_infos {
                    all_keys.extend(handler.keys(args));
                }
                if !all_keys.is_empty() {
                    if self.tracking.has_tracking_clients() {
                        self.tracking.invalidate_keys(&all_keys, conn_id);
                    }
                    if !self.tracking.broadcast_table.is_empty() {
                        self.tracking.broadcast_table.invalidate_matching(
                            &all_keys,
                            conn_id,
                            &self.tracking.invalidation_registry,
                        );
                    }
                }
            }
        }

        // 2.7. Keyspace notifications for each write command
        for &(handler, args) in write_infos {
            self.emit_keyspace_notifications_for_command(handler, args);
        }

        // 3. Dirty counter
        self.update_dirty_counter(total_dirty);

        // 4. Waiter satisfaction
        for &(handler, args) in write_infos {
            self.satisfy_waiters_for_command(handler, args);
        }

        // 4.5. Flush keysizes histogram updates from blocking waiter mutations
        self.store.flush_keysizes_refreshes();

        // 5. WAL — SKIPPED (already done by persist_transaction_to_wal)

        // 5.5. Search indexes
        if !self.search.indexes.is_empty() {
            for &(handler, args) in write_infos {
                self.update_search_indexes(handler.name(), args);
            }
        }

        // 6. Atomic replication broadcast
        if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
            let commands: Vec<(&str, &[Bytes])> = write_infos
                .iter()
                .map(|&(handler, args)| (handler.name(), args))
                .collect();
            self.replication_broadcaster
                .broadcast_transaction(&commands);
        }
    }

    async fn persist_by_strategy(&self, handler: &dyn Command, args: &[Bytes]) {
        if self.persistence.wal_writer.is_none() {
            return;
        }

        for action in handler.wal_strategy().actions(args) {
            let _ = self
                .execute_wal_action(&action)
                .await
                .inspect_err(|e| tracing::error!(error = %e, "WAL persist failed"));
        }
    }
}
