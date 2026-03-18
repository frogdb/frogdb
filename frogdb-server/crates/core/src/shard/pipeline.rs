use bytes::Bytes;
use frogdb_protocol::Response;

use crate::command::{Command, CommandFlags, WaiterKind, WalStrategy};
use crate::store::Store;

use super::helpers::REPLICA_INTERNAL_CONN_ID;
use super::worker::ShardWorker;

impl ShardWorker {
    /// Run the full post-execution pipeline after a command.
    ///
    /// Pipeline order (preserves original behavior):
    /// 1. Keyspace hit/miss metrics
    /// 2. Version increment (writes only)
    /// 3. Dirty counter (writes only)
    /// 4. Blocking waiter satisfaction (writes only)
    /// 5. WAL persistence (writes only)
    /// 6. Replication broadcast (writes only)
    pub(crate) async fn run_post_execution(
        &mut self,
        handler: &dyn Command,
        args: &[Bytes],
        response: &Response,
        dirty_delta: i64,
        conn_id: u64,
    ) {
        let flags = handler.flags();

        // 1. Track keyspace hits/misses
        if flags.contains(CommandFlags::TRACKS_KEYSPACE) {
            self.track_keyspace_metrics(response);
        }

        // Steps 2-6 only apply to write commands
        if !flags.contains(CommandFlags::WRITE) {
            return;
        }

        // 2. Increment version
        self.increment_version();

        // 2.5. Client tracking: invalidate written keys
        if !self.invalidation_registry.is_empty() || !self.broadcast_table.is_empty() {
            let keys = handler.keys(args);
            if !keys.is_empty() {
                if !self.invalidation_registry.is_empty() {
                    self.tracking_table.invalidate_keys(
                        &keys,
                        conn_id,
                        &self.invalidation_registry,
                    );
                }
                if !self.broadcast_table.is_empty() {
                    self.broadcast_table.invalidate_matching(
                        &keys,
                        conn_id,
                        &self.invalidation_registry,
                    );
                }
            }
        }

        // 3. Update dirty counter
        self.update_dirty_counter(dirty_delta);

        // 4. Satisfy blocking waiters
        if let Some(kind) = handler.wakes_waiters() {
            let keys = handler.keys(args);
            self.satisfy_waiters(kind, &keys);
        }

        // 5. WAL persistence
        self.persist_by_strategy(handler, args).await;

        // 5.5. Update search indexes
        if !self.search_indexes.is_empty() {
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
        response: &Response,
        dirty_delta: i64,
        conn_id: u64,
    ) {
        let flags = handler.flags();

        // 1. Track keyspace hits/misses
        if flags.contains(CommandFlags::TRACKS_KEYSPACE) {
            self.track_keyspace_metrics(response);
        }

        if !flags.contains(CommandFlags::WRITE) {
            return;
        }

        // 2. Increment version
        self.increment_version();

        // 2.5. Client tracking: invalidate written keys
        if !self.invalidation_registry.is_empty() || !self.broadcast_table.is_empty() {
            let keys = handler.keys(args);
            if !keys.is_empty() {
                if !self.invalidation_registry.is_empty() {
                    self.tracking_table.invalidate_keys(
                        &keys,
                        conn_id,
                        &self.invalidation_registry,
                    );
                }
                if !self.broadcast_table.is_empty() {
                    self.broadcast_table.invalidate_matching(
                        &keys,
                        conn_id,
                        &self.invalidation_registry,
                    );
                }
            }
        }

        // 3. Update dirty counter
        self.update_dirty_counter(dirty_delta);

        // 4. Satisfy blocking waiters
        if let Some(kind) = handler.wakes_waiters() {
            let keys = handler.keys(args);
            self.satisfy_waiters(kind, &keys);
        }

        // 5. WAL persistence — SKIPPED (already done by persist_and_confirm)

        // 5.5. Update search indexes
        if !self.search_indexes.is_empty() {
            self.update_search_indexes(handler.name(), args);
        }

        // 6. Replication broadcast (skip if from replica to avoid loops)
        if conn_id != REPLICA_INTERNAL_CONN_ID && self.replication_broadcaster.is_active() {
            self.replication_broadcaster
                .broadcast_command(handler.name(), args);
        }
    }

    fn track_keyspace_metrics(&self, response: &Response) {
        if matches!(response, Response::Null) {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_misses_total",
                1,
                &[],
            );
        } else {
            self.observability.metrics_recorder.increment_counter(
                "frogdb_keyspace_hits_total",
                1,
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

    async fn persist_by_strategy(&self, handler: &dyn Command, args: &[Bytes]) {
        if self.persistence.wal_writer.is_none() {
            return;
        }

        match handler.wal_strategy() {
            WalStrategy::PersistFirstKey => {
                if !args.is_empty() {
                    self.persist_key_to_wal(&args[0]).await;
                }
            }
            WalStrategy::DeleteKeys => {
                for arg in args {
                    if !self.store.contains(arg) {
                        self.persist_delete_to_wal(arg).await;
                    }
                }
            }
            WalStrategy::PersistOrDeleteFirstKey => {
                if !args.is_empty() {
                    let key = &args[0];
                    if self.store.contains(key) {
                        self.persist_key_to_wal(key).await;
                    } else {
                        self.persist_delete_to_wal(key).await;
                    }
                }
            }
            WalStrategy::RenameKeys => {
                if args.len() >= 2 {
                    let old_key = &args[0];
                    let new_key = &args[1];
                    if !self.store.contains(old_key) {
                        self.persist_delete_to_wal(old_key).await;
                    }
                    self.persist_key_to_wal(new_key).await;
                }
            }
            WalStrategy::MoveKeys => {
                if args.len() >= 2 {
                    let source = &args[0];
                    let dest = &args[1];
                    if self.store.contains(source) {
                        self.persist_key_to_wal(source).await;
                    } else {
                        self.persist_delete_to_wal(source).await;
                    }
                    self.persist_key_to_wal(dest).await;
                }
            }
            WalStrategy::PersistDestination(idx) => {
                if let Some(dest) = args.get(idx)
                    && self.store.contains(dest)
                {
                    self.persist_key_to_wal(dest).await;
                }
            }
            WalStrategy::NoOp => {}
            WalStrategy::Infer => {
                self.persist_command_to_wal_legacy(handler.name(), args)
                    .await;
            }
        }
    }
}
